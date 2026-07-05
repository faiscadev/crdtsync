//! The handshake range-check: a subscriber that cannot reach the room's
//! governing version across a back-compatible path is refused with
//! `onUpdateRequired` before it becomes a subscriber, so down-translation at
//! fan-out only ever traverses invertible edges. Forward is always reachable; a
//! back-compatible gap never rejects; a foreign-app client (a different version
//! space) is not range-checked.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, ErrorCode, Message, Scalar};
use crdtsync_core::{Document, Op};
use crdtsync_server::{ConnId, ManualClock, Registry, SchemaRegistry};

const ROOM: &[u8] = b"room-a";
/// v1→v2 renames `age`→`years`: a breaking (forward-only) edge.
const UP: &[u8] = b"up";
/// v1→v2 adds a `note` field: a back-compatible edge.
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
        br#"{ "from": 1, "to": 2, "steps": [ { "kind": "addField", "type": "R", "field": "note", "fieldType": "text" } ] }"#,
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

/// Deliver a Subscribe from `id` and return its reply messages.
fn subscribe_reply(r: &mut Registry, id: ConnId) -> Vec<Message> {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        }
    ));
    r.take_outbox(id)
}

/// Whether `replies` carry an `UpdateRequired` error.
fn is_update_required(replies: &[Message]) -> bool {
    replies.iter().any(|m| {
        matches!(
            m,
            Message::Error {
                code: ErrorCode::UpdateRequired,
                ..
            }
        )
    })
}

/// Whether `replies` carry a catch-up (a successful subscribe).
fn is_subscribed(replies: &[Message]) -> bool {
    replies
        .iter()
        .any(|m| matches!(m, Message::Ops { .. } | Message::Snapshot { .. }))
}

fn set(client: u8, key: &[u8]) -> Vec<Op> {
    Document::new(cid(client)).transact(|tx| tx.register(key, Scalar::Int(1)))
}

/// Bind `ROOM` to `app` at `version` by subscribing an enforcing client there,
/// then clear its outbox. Returns the binder so the caller can keep it live.
fn bind_room(r: &mut Registry, client: u8, app: &[u8], version: u32) -> ConnId {
    let id = hello(r, client, app, version);
    let replies = subscribe_reply(r, id);
    assert!(is_subscribed(&replies), "the binder itself must subscribe");
    id
}

#[test]
fn a_client_below_a_breaking_gap_is_refused_with_update_required() {
    let mut r = registry();
    // The room is governed by UP at v2; v1→v2 is a breaking rename, so v1 cannot
    // be reached from v2.
    bind_room(&mut r, 1, UP, 2);

    let old = hello(&mut r, 2, UP, 1);
    let replies = subscribe_reply(&mut r, old);
    assert!(
        is_update_required(&replies),
        "a v1 client below the breaking rename is refused"
    );
    assert!(
        !is_subscribed(&replies),
        "and it never becomes a subscriber"
    );
}

#[test]
fn a_refused_client_receives_no_further_fan_out() {
    let mut r = registry();
    let writer = bind_room(&mut r, 1, UP, 2);

    let old = hello(&mut r, 2, UP, 1);
    subscribe_reply(&mut r, old);

    // A later write to the room must not reach the refused client — it never
    // joined, so it is not in the fan-out set.
    assert!(r.deliver(
        writer,
        Message::Ops {
            channel: Channel(0),
            ops: set(1, b"years"),
        }
    ));
    let late = r.take_outbox(old);
    assert!(
        !late.iter().any(|m| matches!(m, Message::Ops { .. })),
        "a refused client receives no ops"
    );
}

#[test]
fn a_reachable_older_client_over_a_back_compatible_gap_subscribes() {
    let mut r = registry();
    // DOWN's v1→v2 adds a field: back-compatible, so v1 is reachable from v2.
    bind_room(&mut r, 1, DOWN, 2);

    let old = hello(&mut r, 2, DOWN, 1);
    let replies = subscribe_reply(&mut r, old);
    assert!(
        !is_update_required(&replies),
        "a back-compat gap never rejects"
    );
    assert!(is_subscribed(&replies), "the older client joins");
}

#[test]
fn a_newer_client_is_never_refused() {
    let mut r = registry();
    // Room governed at v1; a v2 client joins — forward is always reachable.
    bind_room(&mut r, 1, UP, 1);

    let newer = hello(&mut r, 2, UP, 2);
    let replies = subscribe_reply(&mut r, newer);
    assert!(!is_update_required(&replies));
    assert!(is_subscribed(&replies));
}

#[test]
fn a_same_version_client_is_never_refused() {
    let mut r = registry();
    bind_room(&mut r, 1, UP, 2);

    let peer = hello(&mut r, 2, UP, 2);
    let replies = subscribe_reply(&mut r, peer);
    assert!(!is_update_required(&replies));
    assert!(is_subscribed(&replies));
}

#[test]
fn a_foreign_app_client_is_not_range_checked() {
    let mut r = registry();
    // UP governs the room at v2. A DOWN-app v1 client's version is a different
    // space, so the UP rename gap must not refuse it — it subscribes and is
    // served verbatim.
    bind_room(&mut r, 1, UP, 2);

    let foreign = hello(&mut r, 2, DOWN, 1);
    let replies = subscribe_reply(&mut r, foreign);
    assert!(
        !is_update_required(&replies),
        "a foreign app is not range-checked"
    );
    assert!(is_subscribed(&replies));
}

#[test]
fn a_relay_client_is_not_range_checked() {
    let mut r = registry();
    let writer = hello(&mut r, 1, b"", 0);
    let replies = subscribe_reply(&mut r, writer);
    assert!(is_subscribed(&replies));

    let peer = hello(&mut r, 2, b"", 0);
    let replies = subscribe_reply(&mut r, peer);
    assert!(!is_update_required(&replies));
    assert!(is_subscribed(&replies));
}
