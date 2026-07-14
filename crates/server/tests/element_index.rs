//! The per-room element-context index — id → path, resolving each element to its
//! zone as ops commit.
//!
//! The index projects the room's authoritative document to `id → core::path` and
//! resolves each path's zone by the schema's longest-prefix rule. A node created,
//! a child inserted, a node moved, or a node deleted each shows through because
//! the projection reads the live document. These tests drive real ops emitted by
//! a `Document` (so the ids are the ones a client mints) into a `Hub` and assert
//! the resolution.

use crdtsync_core::xml::XmlFragment;
use crdtsync_core::{ClientId, Document, ElementId, Op, Schema};
use crdtsync_server::{index, Hub};

const ROOM: &[u8] = b"room-1";

/// Root map with two fragment slots, each its own zone; `loose` is unzoned.
const SCHEMA: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Frag", "notes": "Frag", "loose": "Frag" } },
        "Frag": { "kind": "fragment", "children": { "a": {} } },
        "a": { "kind": "xml", "tag": "a", "children": {} }
    },
    "zones": { "za": "/board", "zb": "/notes" }
}"#;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn schema() -> Schema {
    Schema::parse(SCHEMA).expect("schema parses")
}

fn hub() -> Hub {
    Hub::new(cid(0xFF))
}

fn ingest(h: &mut Hub, ops: Vec<Op>) {
    h.ingest(ROOM, ops, None).expect("in-memory ingest");
}

/// The fragment node id at root slot `key`.
fn frag_id(d: &Document, key: &[u8]) -> ElementId {
    XmlFragment::node_id(d.root_id(), key)
}

// --- create resolves path + zone ---

#[test]
fn a_created_fragment_resolves_its_path_and_zone() {
    let mut d = Document::new(cid(1));
    let ops = d.transact(|tx| {
        tx.xml_fragment(b"board");
        tx.xml_fragment(b"notes");
        tx.xml_fragment(b"loose");
    });
    let mut h = hub();
    ingest(&mut h, ops);
    let paths = h.element_paths(ROOM);
    let s = schema();

    // A zoned slot resolves to its zone, at its one-segment path.
    assert_eq!(
        paths.get(&frag_id(&d, b"board")),
        Some(&vec![b"board".to_vec()])
    );
    assert_eq!(
        index::zone_of(&paths, &s, frag_id(&d, b"board")),
        Some("za")
    );
    assert_eq!(
        index::zone_of(&paths, &s, frag_id(&d, b"notes")),
        Some("zb")
    );
    // An unzoned slot is the default region.
    assert_eq!(index::zone_of(&paths, &s, frag_id(&d, b"loose")), None);
    // The Hub convenience resolves the same.
    assert_eq!(h.element_zone(ROOM, &s, frag_id(&d, b"board")), Some("za"));
}

// --- inserted child resolves under its holding fragment's zone ---

#[test]
fn an_inserted_child_inherits_its_holding_fragments_zone() {
    let mut d = Document::new(cid(1));
    let mut child = ElementId::from_bytes([0u8; 16]);
    let ops = d.transact(|tx| {
        let mut board = tx.xml_fragment(b"board");
        child = board.children().insert_element(0, b"a").id();
    });
    let mut h = hub();
    ingest(&mut h, ops);

    // The positional child hangs under the board fragment and inherits its zone.
    assert_eq!(h.element_zone(ROOM, &schema(), child), Some("za"));
}

// --- move updates zone ---

#[test]
fn a_move_updates_the_movers_zone() {
    let mut d = Document::new(cid(1));
    let mut child = ElementId::from_bytes([0u8; 16]);
    let setup = d.transact(|tx| {
        let mut board = tx.xml_fragment(b"board");
        child = board.children().insert_element(0, b"a").id();
        tx.xml_fragment(b"notes");
    });
    let mut h = hub();
    ingest(&mut h, setup);
    assert_eq!(h.element_zone(ROOM, &schema(), child), Some("za"));

    // Move the child from the board zone into the notes zone.
    let notes = frag_id(&d, b"notes");
    let mv = d.transact(|tx| tx.move_xml(child, notes, 0));
    assert!(!mv.is_empty(), "the move emits an op");
    ingest(&mut h, mv);

    assert_eq!(h.element_zone(ROOM, &schema(), child), Some("zb"));
}

// --- delete removes ---

#[test]
fn a_deleted_child_leaves_the_projection() {
    let mut d = Document::new(cid(1));
    let mut child = ElementId::from_bytes([0u8; 16]);
    let setup = d.transact(|tx| {
        let mut board = tx.xml_fragment(b"board");
        child = board.children().insert_element(0, b"a").id();
    });
    let mut h = hub();
    ingest(&mut h, setup);
    assert!(h.element_paths(ROOM).contains_key(&child));

    // Tombstone the child at index 0 of the board fragment.
    let board_path = crdtsync_core::path::encode_path(&[b"board"]);
    let del = crdtsync_core::path::xml_child_delete(&mut d, &board_path, 0);
    assert!(!del.is_empty(), "the delete emits an op");
    ingest(&mut h, del);

    assert!(!h.element_paths(ROOM).contains_key(&child));
}

// --- out-of-order (buffered) delivery still resolves ---

#[test]
fn a_child_delivered_before_its_parent_resolves_once_the_parent_arrives() {
    // The child insert arrives before the fragment that holds it. The document
    // buffers it and drains it once the create lands, so the projection — read
    // from that document — resolves the child correctly regardless of arrival
    // order. (A separately-maintained arrival-order index would have dropped it.)
    let mut d = Document::new(cid(1));
    let create = d.transact(|tx| {
        tx.xml_fragment(b"board");
    });
    let mut child = ElementId::from_bytes([0u8; 16]);
    let insert = d.transact(|tx| {
        child = tx
            .xml_fragment(b"board")
            .children()
            .insert_element(0, b"a")
            .id();
    });

    let mut h = hub();
    ingest(&mut h, insert); // arrives before its holding fragment exists
    ingest(&mut h, create); // makes the child reachable
    assert_eq!(h.element_zone(ROOM, &schema(), child), Some("za"));
}

// --- order-independence where ops commute ---

#[test]
fn independent_creates_resolve_the_same_regardless_of_order() {
    // The two fragment creates target distinct root slots, so they commute; a hub
    // that ingests them either way resolves the identical path and zone.
    let mut d = Document::new(cid(1));
    let board = d.transact(|tx| {
        tx.xml_fragment(b"board");
    });
    let notes = d.transact(|tx| {
        tx.xml_fragment(b"notes");
    });

    let mut forward = hub();
    ingest(&mut forward, board.clone());
    ingest(&mut forward, notes.clone());

    let mut reverse = hub();
    ingest(&mut reverse, notes);
    ingest(&mut reverse, board);

    let s = schema();
    let f = forward.element_paths(ROOM);
    let r = reverse.element_paths(ROOM);
    for id in [frag_id(&d, b"board"), frag_id(&d, b"notes")] {
        assert_eq!(f.get(&id), r.get(&id));
        assert_eq!(index::zone_of(&f, &s, id), index::zone_of(&r, &s, id));
    }
}
