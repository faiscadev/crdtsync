//! Migration translation on the cold-start snapshot seam.
//!
//! A joiner below the room's compaction floor is served a whole-replica snapshot
//! (the merged state at the room's governing version) rather than an op delta.
//! When the joiner's schema version differs from the room's, the snapshot is
//! migrated the same way the op seam translates a delta — down (a field added
//! above the joiner is dropped, a container carried verbatim) or up (a renamed
//! field re-keyed, a removed field dropped) — so a snapshot-served joiner and a
//! peer served the same history as a translated op delta converge.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Element, Message, Op, Scalar};
use crdtsync_server::translate::{translate_ops, translate_snapshot};
use crdtsync_server::{ConnId, ManualClock, Registry, SchemaRegistry};

const ROOM: &[u8] = b"room-a";
/// v1→v2 adds two back-compatible fields: `note` (a scalar) and `body` (a text
/// container). Down to v1, `note`'s set op is dropped but `body`'s create is
/// carried verbatim — the split the snapshot projection must mirror.
const APP: &[u8] = b"down";

const MAP_V1: &str = r#"{ "schema": "s", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } } }"#;
const MAP_V2: &str = r#"{ "schema": "s", "version": 2, "root": "R",
    "types": { "R": { "kind": "map" } } }"#;
const EDGE_V2: &[u8] = br#"{ "from": 1, "to": 2, "steps": [
    { "kind": "addField", "type": "R", "field": "note", "fieldType": "int" },
    { "kind": "addField", "type": "R", "field": "body", "fieldType": "text" } ] }"#;
/// A second app whose v1→v2 renames `title`→`heading` — a breaking edge an older
/// snapshot must be up-migrated across for a newer below-floor joiner.
const UP: &[u8] = b"up";
const UP_EDGE_V2: &[u8] = br#"{ "from": 1, "to": 2, "steps": [
    { "kind": "renameField", "type": "R", "from": "title", "to": "heading" } ] }"#;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn schema_registry() -> SchemaRegistry {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, MAP_V1.as_bytes(), b"").unwrap();
    sr.register(APP, 2, MAP_V2.as_bytes(), EDGE_V2).unwrap();
    sr.register(UP, 1, MAP_V1.as_bytes(), b"").unwrap();
    sr.register(UP, 2, MAP_V2.as_bytes(), UP_EDGE_V2).unwrap();
    sr
}

/// The whole history, authored at v2: a v1 field, an added scalar field, and an
/// added text container. Returns the ops and the governing (v2) snapshot bytes.
fn v2_history() -> (Vec<Op>, Vec<u8>) {
    let mut w = Document::new(cid(1));
    let ops = w.transact(|tx| {
        tx.register(b"title", Scalar::Int(1));
        tx.register(b"note", Scalar::Int(2));
        tx.text(b"body").insert(0, "hi");
    });
    (ops, w.encode_state())
}

/// The Int behind a register slot, or `None` if absent.
fn int_at(d: &Document, key: &[u8]) -> Option<i64> {
    match d.get(key) {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => Some(*n),
            _ => panic!("expected an Int register"),
        },
        None => None,
        _ => panic!("expected a register or nothing"),
    }
}

/// The observable `(title, note, body)` reading of a document.
fn observe(d: &Document) -> (Option<i64>, Option<i64>, Option<String>) {
    let body = match d.get(b"body") {
        Some(Element::Text(t)) => Some(t.borrow().as_string()),
        None => None,
        _ => panic!("expected a text or nothing"),
    };
    (int_at(d, b"title"), int_at(d, b"note"), body)
}

// --- the projection itself ---

#[test]
fn a_snapshot_drops_a_scalar_field_added_above_the_recipient() {
    let reg = schema_registry();
    let (_, snapshot) = v2_history();
    let projected = translate_snapshot(&reg, APP, &snapshot, 2, 1);
    let d = Document::decode_state(&projected).unwrap();
    let (title, note, body) = observe(&d);
    assert_eq!(title, Some(1), "a v1 field survives");
    assert_eq!(note, None, "an added scalar field is dropped");
    assert_eq!(body.as_deref(), Some("hi"), "an added container is kept");
}

#[test]
fn a_same_or_newer_recipient_gets_the_snapshot_verbatim() {
    let reg = schema_registry();
    let (_, snapshot) = v2_history();
    assert_eq!(
        translate_snapshot(&reg, APP, &snapshot, 2, 2),
        snapshot,
        "a same-version recipient is served verbatim"
    );
    assert_eq!(
        translate_snapshot(&reg, APP, &snapshot, 1, 2),
        snapshot,
        "a newer recipient is served verbatim"
    );
}

#[test]
fn an_unknown_app_snapshot_is_served_verbatim() {
    let reg = schema_registry();
    let (_, snapshot) = v2_history();
    // No chain to resolve for a foreign app: fail-safe verbatim (a foreign or
    // relay joiner is a different version space, never projected).
    assert_eq!(
        translate_snapshot(&reg, b"other", &snapshot, 2, 1),
        snapshot
    );
}

// --- the convergence guarantee ---

#[test]
fn a_snapshot_joiner_converges_with_an_op_delta_joiner() {
    let reg = schema_registry();
    let (ops, snapshot) = v2_history();

    // A v1 joiner below the floor: the governing snapshot, down-projected.
    let projected = translate_snapshot(&reg, APP, &snapshot, 2, 1);
    let via_snapshot = Document::decode_state(&projected).unwrap();

    // A v1 joiner above the floor: the same history as a down-translated delta.
    let translated = translate_ops(&reg, APP, &ops, 2, 1);
    let mut via_delta = Document::new(cid(2));
    for op in &translated {
        via_delta.apply(op);
    }

    assert_eq!(
        observe(&via_snapshot),
        observe(&via_delta),
        "the snapshot joiner and the op-delta joiner converge"
    );
    // And it is the correct down-projection: the added scalar gone, the v1 field
    // and the carried container present.
    assert_eq!(
        observe(&via_snapshot),
        (Some(1), None, Some("hi".to_string()))
    );
}

#[test]
fn a_snapshot_up_migrates_a_renamed_field_and_converges() {
    let reg = schema_registry();
    // The whole history authored at v1: a single `title` field.
    let mut w = Document::new(cid(1));
    let ops = w.transact(|tx| tx.register(b"title", Scalar::Int(1)));
    let snapshot = w.encode_state();

    // A v2 joiner below the floor: the v1 snapshot up-migrated to v2 (title→heading).
    let projected = translate_snapshot(&reg, UP, &snapshot, 1, 2);
    let via_snapshot = Document::decode_state(&projected).unwrap();

    // A v2 joiner above the floor: the same history up-translated as an op delta.
    let translated = translate_ops(&reg, UP, &ops, 1, 2);
    let mut via_delta = Document::new(cid(2));
    for op in &translated {
        via_delta.apply(op);
    }

    let read = |d: &Document| (int_at(d, b"title"), int_at(d, b"heading"));
    assert_eq!(
        read(&via_snapshot),
        read(&via_delta),
        "the up-migrated snapshot converges with the op delta"
    );
    assert_eq!(
        read(&via_snapshot),
        (None, Some(1)),
        "title is re-keyed to heading"
    );
}

// --- end to end through the server ---

fn registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(schema_registry())));
    r.set_clock(Arc::new(ManualClock::new(0)));
    // Compact after two ops so a from-zero joiner falls below the floor.
    r.set_compaction_threshold(2);
    r
}

fn hello(r: &mut Registry, client: u8, version: u32) -> ConnId {
    hello_app(r, client, APP, version)
}

fn hello_app(r: &mut Registry, client: u8, app: &[u8], version: u32) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: app.to_vec(),
            schema_version: version,
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: format!("actor-{client}").into_bytes(),
        }
    ));
    r.take_outbox(id);
    id
}

fn subscribe(r: &mut Registry, id: ConnId) {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        }
    ));
}

#[test]
fn a_below_floor_v1_joiner_is_served_a_down_projected_snapshot() {
    let mut r = registry();
    // A v2 writer binds the room and authors the whole v2 history; two ops hit
    // the compaction threshold, folding the log into a snapshot.
    let writer = hello(&mut r, 1, 2);
    subscribe(&mut r, writer);
    r.take_outbox(writer);
    let (ops, _) = v2_history();
    assert!(r.deliver(
        writer,
        Message::Ops {
            channel: Channel(0),
            ops,
        }
    ));
    r.take_outbox(writer);

    // A v1 joiner catches up from zero — below the floor, so a snapshot — and it
    // is down-projected: no added `note`, but the v1 field and the carried
    // container are present.
    let joiner = hello(&mut r, 2, 1);
    subscribe(&mut r, joiner);
    let state = r
        .take_outbox(joiner)
        .into_iter()
        .find_map(|m| match m {
            Message::Snapshot { state, .. } => Some(state),
            _ => None,
        })
        .expect("a below-floor joiner is served a snapshot");
    let d = Document::decode_state(&state).unwrap();
    assert_eq!(observe(&d), (Some(1), None, Some("hi".to_string())));
}

#[test]
fn a_below_floor_snapshot_is_sourced_from_the_content_version_not_the_lifted_floor() {
    let mut r = registry();
    // A v1 writer authors the whole UP history — a single `title` field, plus a
    // filler to reach the compaction threshold — folding it into a v1 snapshot.
    let writer = hello_app(&mut r, 1, UP, 1);
    subscribe(&mut r, writer);
    r.take_outbox(writer);
    let mut w = Document::new(cid(1));
    let ops = w.transact(|tx| {
        tx.register(b"title", Scalar::Int(1));
        tx.register(b"filler", Scalar::Int(9));
    });
    assert!(r.deliver(
        writer,
        Message::Ops {
            channel: Channel(0),
            ops,
        }
    ));
    r.take_outbox(writer);

    // A transient v2 peer lifts the room's governing floor to v2, then leaves; the
    // snapshot still embodies only v1 content.
    let transient = hello_app(&mut r, 2, UP, 2);
    subscribe(&mut r, transient);
    r.take_outbox(transient);
    r.disconnect(transient);

    // A v2 joiner below the floor is served the snapshot up-migrated from the v1
    // content — `title` re-keyed to `heading` — not the v1 state left verbatim
    // under a mistaken v2 source drawn from the lifted floor.
    let joiner = hello_app(&mut r, 3, UP, 2);
    subscribe(&mut r, joiner);
    let state = r
        .take_outbox(joiner)
        .into_iter()
        .find_map(|m| match m {
            Message::Snapshot { state, .. } => Some(state),
            _ => None,
        })
        .expect("a below-floor joiner is served a snapshot");
    let d = Document::decode_state(&state).unwrap();
    assert_eq!(int_at(&d, b"title"), None, "the v1 field is re-keyed");
    assert_eq!(
        int_at(&d, b"heading"),
        Some(1),
        "the snapshot is up-migrated from its v1 content"
    );
}
