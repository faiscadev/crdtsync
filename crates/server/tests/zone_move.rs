//! Cross-zone tree move enforcement (Zones Unit 1b-ii-b) — the op-submit half of
//! the cross-zone rule.
//!
//! The per-zone lamport clocks never order across zones, so a tree move whose
//! mover leaves one zone for another is inadmissible. Unlike a cross-zone anchor,
//! it is not detectable from the post-move tree — the moved node simply renders
//! under its new parent — so it is caught at the op against the room's pre-move
//! element index. A crossing move is refused recoverably (`OpsRejected` /
//! `Forbidden`), exactly as a doc-ACL write denial is: the author keeps its ops
//! and the op never enters the log, so every replica converges on its absence. A
//! same-zone (or fully-unzoned) move commits unchanged; a schema with no zones
//! never refuses.

use std::sync::Mutex;

use crdtsync_core::protocol::Channel;
use crdtsync_core::xml::XmlFragment;
use crdtsync_core::{ClientId, Document, ElementId, ErrorCode, Message, Op, Schema};
use crdtsync_server::auth::AllowAll;
use crdtsync_server::{step, Hub, PermitAll, SchemaRegistry, Session, Store};

const ROOM: &[u8] = b"room-1";
const CH: Channel = Channel(0);

/// Two zoned fragment slots and one unzoned slot.
const ZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Frag", "notes": "Frag", "loose": "Frag" } },
        "Frag": { "kind": "fragment", "children": { "a": {} } },
        "a": { "kind": "xml", "tag": "a", "children": {} }
    },
    "zones": { "za": "/board", "zb": "/notes" }
}"#;

/// The same slot layout with no `zones` block — every location is unzoned.
const UNZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Frag", "notes": "Frag", "loose": "Frag" } },
        "Frag": { "kind": "fragment", "children": { "a": {} } },
        "a": { "kind": "xml", "tag": "a", "children": {} }
    }
}"#;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn zoned() -> Schema {
    Schema::parse(ZONED).expect("schema parses")
}

fn frag_id(d: &Document, key: &[u8]) -> ElementId {
    XmlFragment::node_id(d.root_id(), key)
}

/// A doc with `board`, `notes`, `loose` fragments and one child `a` in the board
/// fragment; returns the setup ops and the child id.
fn doc_with_child_in_board() -> (Document, Vec<Op>, ElementId) {
    let mut d = Document::new(cid(1));
    let mut child = ElementId::from_bytes([0u8; 16]);
    let ops = d.transact(|tx| {
        let mut board = tx.xml_fragment(b"board");
        child = board.children().insert_element(0, b"a").id();
        tx.xml_fragment(b"notes");
        tx.xml_fragment(b"loose");
    });
    (d, ops, child)
}

fn ingest(h: &mut Hub, ops: Vec<Op>) {
    h.ingest(ROOM, ops, None).expect("in-memory ingest");
}

// --- the index-level detection ---

#[test]
fn a_same_zone_move_does_not_cross() {
    // Move the child within the board fragment (a reorder) — same zone, allowed.
    let (mut d, setup, child) = doc_with_child_in_board();
    let mut h = Hub::new(cid(0xFF));
    // A second child so a reorder has somewhere to go.
    let more = d.transact(|tx| {
        tx.xml_fragment(b"board").children().insert_element(1, b"a");
    });
    ingest(&mut h, setup);
    ingest(&mut h, more);

    let board = frag_id(&d, b"board");
    let mv = d.transact(|tx| tx.move_xml(child, board, 1));
    assert!(!mv.is_empty(), "the reorder emits a move op");
    assert!(!h.batch_crosses_zone(ROOM, &mv, &zoned()));
}

#[test]
fn a_move_across_two_zones_crosses() {
    let (mut d, setup, child) = doc_with_child_in_board();
    let mut h = Hub::new(cid(0xFF));
    ingest(&mut h, setup);

    let notes = frag_id(&d, b"notes");
    let mv = d.transact(|tx| tx.move_xml(child, notes, 0));
    assert!(h.batch_crosses_zone(ROOM, &mv, &zoned()));
}

#[test]
fn a_zoned_to_unzoned_move_crosses() {
    // The unzoned region is distinct from every zone, so board → loose crosses.
    let (mut d, setup, child) = doc_with_child_in_board();
    let mut h = Hub::new(cid(0xFF));
    ingest(&mut h, setup);

    let loose = frag_id(&d, b"loose");
    let mv = d.transact(|tx| tx.move_xml(child, loose, 0));
    assert!(h.batch_crosses_zone(ROOM, &mv, &zoned()));
}

#[test]
fn an_unzoned_to_zoned_move_crosses() {
    // Build the child in the unzoned `loose` slot, then move it into `board`.
    let mut d = Document::new(cid(1));
    let mut child = ElementId::from_bytes([0u8; 16]);
    let setup = d.transact(|tx| {
        let mut loose = tx.xml_fragment(b"loose");
        child = loose.children().insert_element(0, b"a").id();
        tx.xml_fragment(b"board");
    });
    let mut h = Hub::new(cid(0xFF));
    ingest(&mut h, setup);

    let board = frag_id(&d, b"board");
    let mv = d.transact(|tx| tx.move_xml(child, board, 0));
    assert!(h.batch_crosses_zone(ROOM, &mv, &zoned()));
}

#[test]
fn a_move_into_a_same_batch_created_destination_crosses() {
    // The destination fragment is created in the *same* batch as the move, so it
    // is absent from the committed document; only simulating the batch
    // materializes it, and the crossing is still caught.
    let (mut d, setup, child) = doc_with_child_in_board();
    let mut h = Hub::new(cid(0xFF));
    ingest(&mut h, setup);

    // One transaction: create a fresh zoned fragment `later` and move the board
    // child into it.
    let later = XmlFragment::node_id(d.root_id(), b"later");
    let batch = d.transact(|tx| {
        tx.xml_fragment(b"later");
        tx.move_xml(child, later, 0);
    });
    // A schema that zones the freshly-created `/later` slot.
    let schema = Schema::parse(
        r#"{ "schema": "z", "version": 1, "root": "Doc",
             "types": {
                 "Doc": { "kind": "map", "children": {
                     "board": "Frag", "later": "Frag" } },
                 "Frag": { "kind": "fragment", "children": { "a": {} } },
                 "a": { "kind": "xml", "tag": "a", "children": {} } },
             "zones": { "za": "/board", "zc": "/later" } }"#,
    )
    .expect("schema parses");
    assert!(h.batch_crosses_zone(ROOM, &batch, &schema));
}

#[test]
fn a_create_then_reorder_within_a_zone_does_not_cross() {
    // One batch inserts a child into the board zone and reorders it within board.
    // The mover is created this batch (absent before), so it holds no committed
    // position that could cross — the batch must not be refused.
    let (mut d, setup, _child) = doc_with_child_in_board();
    let mut h = Hub::new(cid(0xFF));
    ingest(&mut h, setup);

    let board = frag_id(&d, b"board");
    let batch = d.transact(|tx| {
        let fresh;
        {
            let mut board_cur = tx.xml_fragment(b"board");
            fresh = board_cur.children().insert_element(1, b"a").id();
        }
        tx.move_xml(fresh, board, 0);
    });
    assert!(!h.batch_crosses_zone(ROOM, &batch, &zoned()));
}

#[test]
fn a_move_then_delete_within_a_zone_does_not_cross() {
    // One batch moves a board child within board and then deletes it. The mover is
    // gone after the batch (absent after), so there is no persistent cross-zone
    // edge — the batch must not be refused.
    let (mut d, setup, child) = doc_with_child_in_board();
    let mut h = Hub::new(cid(0xFF));
    // A second board child so the delete addresses a specific one.
    let more = d.transact(|tx| {
        tx.xml_fragment(b"board").children().insert_element(1, b"a");
    });
    ingest(&mut h, setup);
    ingest(&mut h, more);

    let board = frag_id(&d, b"board");
    let batch = d.transact(|tx| {
        tx.move_xml(child, board, 1);
    });
    // Append the delete of the moved child to the same batch.
    let mut batch = batch;
    batch.extend(crdtsync_core::path::xml_child_delete(
        &mut d,
        &crdtsync_core::path::encode_path(&[b"board"]),
        1,
    ));
    assert!(!h.batch_crosses_zone(ROOM, &batch, &zoned()));
}

#[test]
fn a_schema_with_no_zones_never_crosses() {
    let (mut d, setup, child) = doc_with_child_in_board();
    let mut h = Hub::new(cid(0xFF));
    ingest(&mut h, setup);

    let notes = frag_id(&d, b"notes");
    let mv = d.transact(|tx| tx.move_xml(child, notes, 0));
    // The same move that crosses under a zoned schema is clean with no zones.
    let unzoned = Schema::parse(UNZONED).expect("schema parses");
    assert!(!h.batch_crosses_zone(ROOM, &mv, &unzoned));
}

#[test]
fn a_move_of_an_unindexed_node_never_crosses() {
    // A room the hub does not know, and a mover it never indexed, are not refused
    // on a guess — the move is left to apply, which drops an unresolvable one.
    let (mut d, _setup, child) = doc_with_child_in_board();
    let h = Hub::new(cid(0xFF));
    let notes = frag_id(&d, b"notes");
    let mv = d.transact(|tx| tx.move_xml(child, notes, 0));
    assert!(!h.batch_crosses_zone(ROOM, &mv, &zoned()));
}

// --- durability: the refusal survives a store reopen ---

#[test]
#[cfg_attr(miri, ignore)] // touches the filesystem store
fn the_refusal_survives_a_store_replay_reopen() {
    let dir = std::env::temp_dir().join(format!("cs-zone-move-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let (mut d, setup, child) = doc_with_child_in_board();
    {
        let store = Store::open(&dir).expect("store opens");
        let mut h = Hub::from_rooms(cid(0xFF), Vec::new()).expect("empty hub");
        h.attach_store(store);
        ingest(&mut h, setup.clone());
    }

    // Reopen from the store: the tail replays through the shared commit path, so
    // the index is rebuilt and the same cross-zone move is still detected.
    let store = Store::open(&dir).expect("store reopens");
    let rooms = store.load().expect("store loads");
    let h = Hub::from_rooms(cid(0xFF), rooms).expect("hub rebuilt");
    let notes = frag_id(&d, b"notes");
    let mv = d.transact(|tx| tx.move_xml(child, notes, 0));
    assert!(h.batch_crosses_zone(ROOM, &mv, &zoned()));

    let _ = std::fs::remove_dir_all(&dir);
}

// --- the wire refusal through the session ---

/// Drive one message with the dev verifier, permit-all deployment authorizer, and
/// the given schema — so the schema/zone tier is the only gate a write meets.
fn st(h: &mut Hub, s: &mut Session, schema: &Schema, msg: Message) -> crdtsync_server::Response {
    step(
        h,
        s,
        &AllowAll,
        &PermitAll,
        Some(schema),
        &Mutex::new(SchemaRegistry::new()),
        None,
        None,
        0,
        None,
        msg,
    )
}

fn handshake(h: &mut Hub, s: &mut Session, schema: &Schema) {
    st(
        h,
        s,
        schema,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
        },
    );
    st(
        h,
        s,
        schema,
        Message::Auth {
            credential: b"cred".to_vec(),
        },
    );
    let r = st(
        h,
        s,
        schema,
        Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq: 0,
        },
    );
    assert!(!r.close, "subscribe establishes the channel");
}

fn ops_msg(ops: Vec<Op>) -> Message {
    Message::Ops { channel: CH, ops }
}

fn is_forbidden(r: &crdtsync_server::Response) -> bool {
    r.replies.iter().any(|m| {
        matches!(
            m,
            Message::OpsRejected {
                reason: ErrorCode::Forbidden,
                ..
            }
        )
    })
}

#[test]
fn a_cross_zone_move_is_refused_forbidden_and_the_mover_stays_put() {
    let schema = zoned();
    let (mut d, setup, child) = doc_with_child_in_board();
    let mut h = Hub::new(cid(0xFF));
    let mut s = Session::new();
    handshake(&mut h, &mut s, &schema);

    // The setup ops are not moves, so they commit and seed the index.
    let r = st(&mut h, &mut s, &schema, ops_msg(setup));
    assert!(!is_forbidden(&r), "setup writes are accepted");
    let seq_after_setup = h.seq(ROOM);

    // A cross-zone move: board → notes.
    let notes = frag_id(&d, b"notes");
    let mv = d.transact(|tx| tx.move_xml(child, notes, 0));
    let r = st(&mut h, &mut s, &schema, ops_msg(mv));
    assert!(is_forbidden(&r), "the cross-zone move is refused Forbidden");
    // The op never entered the log — the mover stays put.
    assert_eq!(h.seq(ROOM), seq_after_setup, "no op was logged");
}

#[test]
fn a_same_zone_move_is_accepted_through_the_session() {
    let schema = zoned();
    let mut d = Document::new(cid(1));
    let mut child = ElementId::from_bytes([0u8; 16]);
    let setup = d.transact(|tx| {
        let mut board = tx.xml_fragment(b"board");
        let mut kids = board.children();
        child = kids.insert_element(0, b"a").id();
        kids.insert_element(1, b"a");
    });
    let mut h = Hub::new(cid(0xFF));
    let mut s = Session::new();
    handshake(&mut h, &mut s, &schema);
    st(&mut h, &mut s, &schema, ops_msg(setup));
    let before = h.seq(ROOM);

    // A reorder within the board zone commits.
    let board = frag_id(&d, b"board");
    let mv = d.transact(|tx| tx.move_xml(child, board, 1));
    let r = st(&mut h, &mut s, &schema, ops_msg(mv));
    assert!(!is_forbidden(&r), "a same-zone move is accepted");
    assert_eq!(h.seq(ROOM), before + 1, "the move was logged");
}
