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
        br#"{ "from": 1, "to": 2, "steps": [ { "kind": "addField", "type": "R", "field": "note", "fieldType": "text" }, { "kind": "addField", "type": "R", "field": "extra", "fieldType": "int" } ] }"#,
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
            branch: Vec::new(),
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

/// Every op delivered to `id`'s outbox, across every `Ops` message, in order.
fn delivered_ops(r: &mut Registry, id: ConnId) -> Vec<Op> {
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
fn a_forward_write_is_translated_up_for_a_newer_recipient() {
    let mut r = registry();
    let writer = hello(&mut r, 1, UP, 1);
    let newer = hello(&mut r, 2, UP, 2);
    for id in [writer, newer] {
        subscribe(&mut r, id);
    }
    // The v2 subscriber lifts the room's governing version to 2; a v1 recipient
    // could no longer reach it across the breaking rename, so 8e refuses such a
    // joiner outright (covered in `subscribe_range_check`). The v1 writer sets
    // "age"; the v2 recipient sees it up-translated to "years".
    write(&mut r, writer, set(1, b"age"));
    assert_eq!(delivered_keys(&mut r, newer), vec![b"years".to_vec()]);
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
fn a_partly_translatable_transaction_delivers_its_representable_members_destranded() {
    let mut r = registry();
    let writer = hello(&mut r, 1, DOWN, 2);
    let older = hello(&mut r, 2, DOWN, 1);
    let peer_v2 = hello(&mut r, 3, DOWN, 2);
    for id in [writer, older, peer_v2] {
        subscribe(&mut r, id);
    }
    // One atomic transaction touching a shared field and a v2-only field. At v1
    // the "note" member has no image and drops; the group can never reach its
    // count there, so the surviving "title" is destranded — delivered without its
    // tx tag so it applies standalone rather than buffering forever. Dropping it
    // with the group would diverge the v1 recipient from the down-projection of
    // the writer's state, which does contain "title". The v2 recipient receives
    // both members intact, still atomic.
    let mut wdoc = Document::new(cid(1));
    let tx = wdoc.atomic_transact(|c| {
        c.register(b"title", Scalar::Int(1));
        c.register(b"note", Scalar::Int(2));
    });
    write(&mut r, writer, tx);

    let older_got = delivered_ops(&mut r, older);
    assert_eq!(older_got.len(), 1, "only the representable member crosses");
    assert!(
        matches!(&older_got[0].kind, OpKind::RegisterSet { key, .. } if key == b"title"),
        "the shared member survives"
    );
    assert!(
        older_got[0].tx.is_none(),
        "it is destranded, so it applies without buffering for the dropped member"
    );

    let peer_got = delivered_ops(&mut r, peer_v2);
    assert_eq!(
        peer_got.len(),
        2,
        "the same-version recipient receives both members"
    );
    assert!(
        peer_got.iter().all(|op| op.tx.is_some()),
        "and they stay atomic — the group crosses whole"
    );
}

#[test]
fn a_container_create_and_its_subtree_survive_verbatim_to_an_older_recipient() {
    let mut r = registry();
    let writer = hello(&mut r, 1, DOWN, 2);
    let older = hello(&mut r, 2, DOWN, 1);
    for id in [writer, older] {
        subscribe(&mut r, id);
    }
    // The v2 writer creates the v2-only "note" text field and inserts into it —
    // a container-create followed by an insert whose target is the container's
    // element id, carrying no field key. Down at v1 the chain would drop the
    // create (the field is v2-only) but cannot see the keyless insert, so a naive
    // rewrite would strand the insert against a container that never arrives.
    // The whole subtree is carried verbatim instead: the v1 recipient receives
    // both ops, internally consistent.
    let mut wdoc = Document::new(cid(1));
    let note = crdtsync_core::path::encode_path(&[b"note"]);
    let ops = crdtsync_core::path::text_insert(&mut wdoc, &note, 0, "hi");
    assert!(
        matches!(
            ops.first().map(|op| &op.kind),
            Some(OpKind::TextCreate { .. })
        ),
        "the write should open with a TextCreate"
    );
    let sent = ops.len();
    write(&mut r, writer, ops);

    let got = delivered_ops(&mut r, older);
    assert_eq!(
        got.len(),
        sent,
        "the older recipient receives the whole subtree"
    );
    assert!(matches!(got[0].kind, OpKind::TextCreate { .. }));
    assert!(got[1..]
        .iter()
        .all(|op| matches!(op.kind, OpKind::TextInsert { .. })));
}

#[test]
fn a_poisoned_transactions_container_subtree_is_destranded_whole_not_left_empty() {
    let mut r = registry();
    let writer = hello(&mut r, 1, DOWN, 2);
    let older = hello(&mut r, 2, DOWN, 1);
    for id in [writer, older] {
        subscribe(&mut r, id);
    }
    // One atomic transaction creates the v2-only "note" text container, types into
    // it, and sets a v2-only scalar — the create, its keyless insert, and the
    // scalar all share one tx. Down at v1 the scalar drops, poisoning the group.
    // Dropping the survivors would leave the container empty (the insert lost,
    // though v1 can represent it); tagging them would strand them against a count
    // that never completes. Both survivors are destranded instead, so the v1
    // recipient receives the container with its content intact.
    let mut wdoc = Document::new(cid(1));
    let batch = wdoc.atomic_transact(|c| {
        c.text(b"note").insert(0, "hi");
        c.register(b"extra", Scalar::Int(1));
    });
    write(&mut r, writer, batch);

    let got = delivered_ops(&mut r, older);
    assert!(
        got.iter()
            .any(|op| matches!(op.kind, OpKind::TextCreate { .. })),
        "the container-create survives the poisoned transaction"
    );
    assert!(
        got.iter()
            .any(|op| matches!(op.kind, OpKind::TextInsert { .. })),
        "its in-tx descendant is delivered too — the container is not left empty"
    );
    assert!(
        got.iter().all(|op| op.tx.is_none()),
        "every survivor is destranded, so it applies without buffering"
    );
    assert!(
        !got.iter()
            .any(|op| matches!(&op.kind, OpKind::RegisterSet { key, .. } if key == b"extra")),
        "the v2-only scalar member does not cross"
    );
}

#[test]
fn an_untransacted_insert_beside_a_poisoned_create_is_delivered_as_is() {
    let mut r = registry();
    let writer = hello(&mut r, 1, DOWN, 2);
    let older = hello(&mut r, 2, DOWN, 1);
    for id in [writer, older] {
        subscribe(&mut r, id);
    }
    // A poisoned transaction (the v2-only "note" create + a v2-only scalar) sits in
    // the same fan-out batch as a separate un-transacted insert into that
    // container. Down at v1 the scalar poisons the transaction, so its create is
    // destranded; the insert, never in the transaction, is not poisoned and passes
    // through as-is — the survivor branch that carries an already-standalone op.
    // The container still arrives with its content, from two different code paths.
    let mut wdoc = Document::new(cid(1));
    let note = crdtsync_core::path::encode_path(&[b"note"]);
    let mut batch = wdoc.atomic_transact(|c| {
        c.text(b"note");
        c.register(b"extra", Scalar::Int(1));
    });
    batch.extend(crdtsync_core::path::text_insert(&mut wdoc, &note, 0, "hi"));
    write(&mut r, writer, batch);

    let got = delivered_ops(&mut r, older);
    assert!(
        got.iter()
            .any(|op| matches!(op.kind, OpKind::TextCreate { .. })),
        "the destranded create survives"
    );
    assert!(
        got.iter()
            .any(|op| matches!(op.kind, OpKind::TextInsert { .. })),
        "the un-transacted insert is delivered, not stranded against a missing container"
    );
    assert!(
        got.iter().all(|op| op.tx.is_none()),
        "the create is destranded and the insert was already standalone"
    );
    assert!(
        !got.iter()
            .any(|op| matches!(&op.kind, OpKind::RegisterSet { key, .. } if key == b"extra")),
        "the v2-only scalar member does not cross"
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
