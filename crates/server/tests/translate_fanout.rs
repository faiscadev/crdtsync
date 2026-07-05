//! Per-recipient migration translation on the live fan-out seam.
//!
//! An enforcing room can hold a mixed-version fleet: the server rewrites each
//! broadcast op from the writer's schema version to every recipient's own
//! version. A forward (up) edge rewrites a write to a newer recipient; a
//! back-compatible addition is dropped for an older recipient that has no such
//! field; a same-version or relay recipient receives the ops verbatim.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Op, OpKind, Scalar};
use crdtsync_server::{ConnId, ManualClock, Registry, SchemaRegistry};

const ROOM: &[u8] = b"room-a";

/// An app whose v1→v2 edge renames the `age` field to `years` (a forward edge).
const UP: &[u8] = b"up";
/// An app whose v1→v2 edge adds a `note` field (a back-compatible edge).
const DOWN: &[u8] = b"down";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const MAP_V1: &str = r#"{ "schema": "s", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } } }"#;
const MAP_V2: &str = r#"{ "schema": "s", "version": 2, "root": "R",
    "types": { "R": { "kind": "map" } } }"#;

fn registry() -> Registry {
    let mut sr = SchemaRegistry::new();
    // The "up" chain: v1, then v2 renaming age -> years.
    sr.register(UP, 1, MAP_V1.as_bytes(), b"").unwrap();
    sr.register(
        UP,
        2,
        MAP_V2.as_bytes(),
        br#"{ "from": 1, "to": 2, "steps": [ { "kind": "renameField", "type": "R", "from": "age", "to": "years" } ] }"#,
    )
    .unwrap();
    // The "down" chain: v1, then v2 adding a back-compatible note field.
    sr.register(DOWN, 1, MAP_V1.as_bytes(), b"").unwrap();
    sr.register(
        DOWN,
        2,
        MAP_V2.as_bytes(),
        br#"{ "from": 1, "to": 2, "steps": [ { "kind": "addField", "type": "R", "field": "note", "fieldType": "text" } ] }"#,
    )
    .unwrap();

    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    // A fixed clock: the default SystemClock is unreadable under Miri isolation.
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

/// Hello + Auth a connection declaring `{app, version}` (empty app + version 0
/// is a relay connection).
fn hello(r: &mut Registry, client: u8, app: &[u8], version: u32) -> ConnId {
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
    r.take_outbox(id);
}

/// A single RegisterSet op on `key` from `client`.
fn set(client: u8, key: &[u8]) -> Vec<Op> {
    Document::new(cid(client)).transact(|tx| tx.register(key, Scalar::Int(1)))
}

fn write(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: Channel(0),
            ops,
        }
    ));
    r.take_outbox(id);
}

/// The register keys delivered to `id`'s outbox, across every `Ops` message.
fn delivered_keys(r: &mut Registry, id: ConnId) -> Vec<Vec<u8>> {
    r.take_outbox(id)
        .into_iter()
        .filter_map(|m| match m {
            Message::Ops { ops, .. } => Some(ops),
            _ => None,
        })
        .flatten()
        .map(|op| match op.kind {
            OpKind::RegisterSet { key, .. } => key,
            other => panic!("expected a RegisterSet, got {other:?}"),
        })
        .collect()
}

#[test]
fn a_forward_write_is_translated_up_for_a_newer_recipient() {
    let mut r = registry();
    let writer = hello(&mut r, 1, UP, 1);
    let newer = hello(&mut r, 2, UP, 2);
    let peer_v1 = hello(&mut r, 3, UP, 1);
    for id in [writer, newer, peer_v1] {
        subscribe(&mut r, id);
    }
    // The v1 writer sets "age"; the v2 recipient sees it renamed to "years", the
    // v1 recipient sees it unchanged.
    write(&mut r, writer, set(1, b"age"));
    assert_eq!(delivered_keys(&mut r, newer), vec![b"years".to_vec()]);
    assert_eq!(delivered_keys(&mut r, peer_v1), vec![b"age".to_vec()]);
}

#[test]
fn a_back_compatible_addition_is_dropped_for_an_older_recipient() {
    let mut r = registry();
    let writer = hello(&mut r, 1, DOWN, 2);
    let older = hello(&mut r, 2, DOWN, 1);
    let peer_v2 = hello(&mut r, 3, DOWN, 2);
    for id in [writer, older, peer_v2] {
        subscribe(&mut r, id);
    }
    // One writer doc across both writes, so the two ops carry distinct ids
    // rather than colliding on a fresh doc's first sequence.
    let mut wdoc = Document::new(cid(1));

    // A write to the v2-only "note" field has no image at v1: the v1 recipient
    // receives nothing, the v2 recipient receives it.
    write(
        &mut r,
        writer,
        wdoc.transact(|tx| tx.register(b"note", Scalar::Int(1))),
    );
    assert!(delivered_keys(&mut r, older).is_empty());
    assert_eq!(delivered_keys(&mut r, peer_v2), vec![b"note".to_vec()]);

    // A write to a field both versions share survives down to the v1 recipient.
    write(
        &mut r,
        writer,
        wdoc.transact(|tx| tx.register(b"title", Scalar::Int(1))),
    );
    assert_eq!(delivered_keys(&mut r, older), vec![b"title".to_vec()]);
    assert_eq!(delivered_keys(&mut r, peer_v2), vec![b"title".to_vec()]);
}

#[test]
fn a_partly_translatable_transaction_is_dropped_whole_for_an_older_recipient() {
    let mut r = registry();
    let writer = hello(&mut r, 1, DOWN, 2);
    let older = hello(&mut r, 2, DOWN, 1);
    let peer_v2 = hello(&mut r, 3, DOWN, 2);
    for id in [writer, older, peer_v2] {
        subscribe(&mut r, id);
    }
    // One atomic transaction touching a shared field and a v2-only field. At v1
    // the "note" member has no image, so the whole group is dropped — "title"
    // too — rather than stranding a partial transaction the recipient could
    // never complete. The v2 recipient receives both members intact.
    let mut wdoc = Document::new(cid(1));
    let tx = wdoc.atomic_transact(|c| {
        c.register(b"title", Scalar::Int(1));
        c.register(b"note", Scalar::Int(2));
    });
    write(&mut r, writer, tx);
    assert!(delivered_keys(&mut r, older).is_empty());
    assert_eq!(
        delivered_keys(&mut r, peer_v2),
        vec![b"title".to_vec(), b"note".to_vec()]
    );
}

#[test]
fn a_foreign_app_write_is_not_translated_along_the_rooms_chain() {
    let mut r = registry();
    // App UP at v1 subscribes first, binding the room to UP.
    let up_v1 = hello(&mut r, 2, UP, 1);
    subscribe(&mut r, up_v1);
    // A connection of a different app (DOWN, v2) writes into the same room. Its
    // version number lives in DOWN's space, not UP's, so it must never drive
    // UP's age<->years edge.
    let foreign = hello(&mut r, 1, DOWN, 2);
    subscribe(&mut r, foreign);
    r.take_outbox(up_v1);
    write(&mut r, foreign, set(1, b"years"));
    // The write is delivered verbatim — down-translating along UP's chain would
    // have corrupted "years" into "age".
    assert_eq!(delivered_keys(&mut r, up_v1), vec![b"years".to_vec()]);
}

#[test]
fn a_same_version_fleet_is_untranslated() {
    let mut r = registry();
    let writer = hello(&mut r, 1, UP, 2);
    let peer = hello(&mut r, 2, UP, 2);
    for id in [writer, peer] {
        subscribe(&mut r, id);
    }
    // Both at v2: the op passes through verbatim, no rewrite.
    write(&mut r, writer, set(1, b"age"));
    assert_eq!(delivered_keys(&mut r, peer), vec![b"age".to_vec()]);
}

#[test]
fn a_relay_room_is_untranslated() {
    let mut r = registry();
    // Relay connections: empty app, version 0. The room binds no app, so the
    // fan-out never translates.
    let writer = hello(&mut r, 1, b"", 0);
    let peer = hello(&mut r, 2, b"", 0);
    for id in [writer, peer] {
        subscribe(&mut r, id);
    }
    write(&mut r, writer, set(1, b"age"));
    assert_eq!(delivered_keys(&mut r, peer), vec![b"age".to_vec()]);
}
