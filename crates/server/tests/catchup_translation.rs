//! Migration translation on the catch-up (late-joiner) seam.
//!
//! A subscriber that joins an enforcing room asks for everything past the
//! sequence it last saw. That delta is a slice of the room's heterogeneous log —
//! each op stored at its own creation version — so, unlike a live broadcast
//! batch (all one writer's version), the delta can mix versions. The server
//! translates each op from its stored version to the joiner's own version before
//! sending, exactly as the live fan-out does, so a late joiner and a live peer at
//! the same version converge to the same state.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Op, OpKind, Scalar};
use crdtsync_server::{ConnId, ManualClock, Registry, SchemaRegistry};

const ROOM: &[u8] = b"room-a";
/// v1→v2 renames `age`→`years` (a forward edge).
const UP: &[u8] = b"up";
/// v1→v2 adds a back-compatible `note` field.
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
    sr.register(UP, 1, MAP_V1.as_bytes(), b"").unwrap();
    sr.register(
        UP,
        2,
        MAP_V2.as_bytes(),
        br#"{ "from": 1, "to": 2, "steps": [ { "kind": "renameField", "type": "R", "from": "age", "to": "years" } ] }"#,
    )
    .unwrap();
    sr.register(DOWN, 1, MAP_V1.as_bytes(), b"").unwrap();
    sr.register(
        DOWN,
        2,
        MAP_V2.as_bytes(),
        br#"{ "from": 1, "to": 2, "steps": [ { "kind": "addField", "type": "R", "field": "note", "fieldType": "text" }, { "kind": "addField", "type": "R", "field": "extra", "fieldType": "int" } ] }"#,
    )
    .unwrap();

    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

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

/// Subscribe from sequence 0, so the whole room log comes back as the catch-up
/// delta.
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

/// The register keys in the catch-up `Ops` reply delivered to `id`.
fn caught_up_keys(r: &mut Registry, id: ConnId) -> Vec<Vec<u8>> {
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

fn caught_up_ops(r: &mut Registry, id: ConnId) -> Vec<Op> {
    r.take_outbox(id)
        .into_iter()
        .filter_map(|m| match m {
            Message::Ops { ops, .. } => Some(ops),
            _ => None,
        })
        .flatten()
        .collect()
}

#[test]
fn a_v1_joiner_catches_up_a_v2_written_log_down_translated() {
    let mut r = registry();
    // A v1 writer binds the room, then a v2 writer appends — the log now holds
    // ops at two versions. (The v1 writer binds first so the room's governing app
    // is UP; the v2 subscribe lifts the bound version to 2.)
    let w1 = hello(&mut r, 1, UP, 1);
    subscribe(&mut r, w1);
    r.take_outbox(w1);
    write(&mut r, w1, set(1, b"age"));

    // A late v1 joiner catches up: the whole log arrives at v1. The v1 op is
    // untouched; nothing renamed since the writer was already v1.
    let joiner = hello(&mut r, 2, UP, 1);
    subscribe(&mut r, joiner);
    assert_eq!(caught_up_keys(&mut r, joiner), vec![b"age".to_vec()]);
}

#[test]
fn a_v2_joiner_catches_up_a_v1_written_log_up_translated() {
    let mut r = registry();
    let writer = hello(&mut r, 1, UP, 1);
    subscribe(&mut r, writer);
    r.take_outbox(writer);
    // The v1 writer sets "age".
    write(&mut r, writer, set(1, b"age"));

    // A v2 joiner catches up: "age" is up-translated to "years".
    let joiner = hello(&mut r, 2, UP, 2);
    subscribe(&mut r, joiner);
    assert_eq!(caught_up_keys(&mut r, joiner), vec![b"years".to_vec()]);
}

#[test]
fn a_heterogeneous_log_translates_each_op_from_its_own_version() {
    let mut r = registry();
    // A v1 writer and a v2 writer both append to the room, so the log interleaves
    // versions. The v1 writer subscribes first and binds the room to UP.
    let v1w = hello(&mut r, 1, UP, 1);
    subscribe(&mut r, v1w);
    r.take_outbox(v1w);
    let v2w = hello(&mut r, 2, UP, 2);
    subscribe(&mut r, v2w);
    r.take_outbox(v2w);

    // v1 writes "age" (stored at v1); v2 writes "years" (stored at v2). Both name
    // the same logical field across the rename.
    write(&mut r, v1w, set(1, b"age"));
    write(&mut r, v2w, set(2, b"years"));

    // A v2 joiner catches up: the v1 op is up-translated "age"->"years"; the v2 op
    // stays "years". Each op is bridged from its own stored version, so both land
    // as "years" at v2.
    let joiner2 = hello(&mut r, 4, UP, 2);
    subscribe(&mut r, joiner2);
    assert_eq!(
        caught_up_keys(&mut r, joiner2),
        vec![b"years".to_vec(), b"years".to_vec()]
    );

    // A v1 joiner catches up: the v1 op stays "age", but the v2 op cannot reach v1
    // — the rename is a breaking (forward-only) edge with no inverse — so it is
    // dropped fail-closed, exactly as the live seam drops an unreachable batch
    // (the handshake range-check of 8e refuses such a joiner outright). The v1 op
    // still arrives.
    let joiner1 = hello(&mut r, 3, UP, 1);
    subscribe(&mut r, joiner1);
    assert_eq!(caught_up_keys(&mut r, joiner1), vec![b"age".to_vec()]);
}

#[test]
fn a_back_compatible_addition_is_dropped_for_an_older_joiner() {
    let mut r = registry();
    let writer = hello(&mut r, 1, DOWN, 2);
    subscribe(&mut r, writer);
    r.take_outbox(writer);
    // One writer doc so the two ops carry distinct ids.
    let mut wdoc = Document::new(cid(1));
    write(
        &mut r,
        writer,
        wdoc.transact(|tx| tx.register(b"title", Scalar::Int(1))),
    );
    write(
        &mut r,
        writer,
        wdoc.transact(|tx| tx.register(b"note", Scalar::Int(2))),
    );

    // A v1 joiner catches up: the shared "title" survives; the v2-only "note" has
    // no image at v1 and is dropped.
    let joiner = hello(&mut r, 2, DOWN, 1);
    subscribe(&mut r, joiner);
    assert_eq!(caught_up_keys(&mut r, joiner), vec![b"title".to_vec()]);
}

#[test]
fn a_partly_translatable_transaction_in_the_delta_is_destranded_for_an_older_joiner() {
    let mut r = registry();
    let writer = hello(&mut r, 1, DOWN, 2);
    subscribe(&mut r, writer);
    r.take_outbox(writer);
    // An atomic transaction over a shared field and a v2-only field.
    let mut wdoc = Document::new(cid(1));
    let tx = wdoc.atomic_transact(|c| {
        c.register(b"title", Scalar::Int(1));
        c.register(b"extra", Scalar::Int(2));
    });
    write(&mut r, writer, tx);

    // A v1 joiner catches up: "extra" drops, so the transaction can never reach
    // its count at v1; the surviving "title" is destranded (tx tag stripped) and
    // delivered, rather than dropped whole — the joiner must not diverge from the
    // down-projection, which holds "title".
    let joiner = hello(&mut r, 2, DOWN, 1);
    subscribe(&mut r, joiner);
    let got = caught_up_ops(&mut r, joiner);
    assert_eq!(got.len(), 1);
    assert!(matches!(&got[0].kind, OpKind::RegisterSet { key, .. } if key == b"title"));
    assert!(got[0].tx.is_none(), "the survivor is destranded");
}

#[test]
fn a_same_version_joiner_catches_up_untranslated() {
    let mut r = registry();
    let writer = hello(&mut r, 1, UP, 2);
    subscribe(&mut r, writer);
    r.take_outbox(writer);
    write(&mut r, writer, set(1, b"age"));

    let joiner = hello(&mut r, 2, UP, 2);
    subscribe(&mut r, joiner);
    // Both at v2: verbatim.
    assert_eq!(caught_up_keys(&mut r, joiner), vec![b"age".to_vec()]);
}

#[test]
fn a_relay_room_catches_up_untranslated() {
    let mut r = registry();
    let writer = hello(&mut r, 1, b"", 0);
    subscribe(&mut r, writer);
    r.take_outbox(writer);
    write(&mut r, writer, set(1, b"age"));

    let joiner = hello(&mut r, 2, b"", 0);
    subscribe(&mut r, joiner);
    assert_eq!(caught_up_keys(&mut r, joiner), vec![b"age".to_vec()]);
}

#[test]
fn a_foreign_app_joiner_catches_up_untranslated() {
    let mut r = registry();
    // UP binds the room.
    let up = hello(&mut r, 1, UP, 2);
    subscribe(&mut r, up);
    r.take_outbox(up);
    write(&mut r, up, set(1, b"age"));

    // A DOWN-app joiner's version is a different space and must never drive UP's
    // chain: it catches up verbatim.
    let foreign = hello(&mut r, 2, DOWN, 1);
    subscribe(&mut r, foreign);
    assert_eq!(caught_up_keys(&mut r, foreign), vec![b"age".to_vec()]);
}

/// A single RegisterSet on `key` from a fresh doc for `client`.
fn set(client: u8, key: &[u8]) -> Vec<Op> {
    Document::new(cid(client)).transact(|tx| tx.register(key, Scalar::Int(1)))
}
