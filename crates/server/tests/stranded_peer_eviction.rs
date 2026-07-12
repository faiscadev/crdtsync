//! Stranded-peer eviction on a version lift: a write that raises a room's
//! op-version high-water past a joined enforcing peer's back-compat reach evicts
//! that peer with `onUpdateRequired` and drops it from the room, rather than
//! silently down-dropping every later write to it. Admission and eviction share
//! the same reachability predicate — only a breaking gap the new high-water
//! opens past the peer's version strands it; a back-compatible lift, the writer
//! itself, and a relay / foreign-app / versionless peer are untouched.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Scalar};
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

fn schema_registry() -> Arc<Mutex<SchemaRegistry>> {
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
    Arc::new(Mutex::new(sr))
}

fn registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(schema_registry());
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

/// An enforcing connection plus the local document its writes flow from — one
/// document per writer, so successive writes carry distinct op ids rather than
/// colliding on a fresh document's reset sequence.
struct Peer {
    id: ConnId,
    doc: Document,
}

fn hello(r: &mut Registry, client: u8, app: &[u8], version: u32) -> Peer {
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
    Peer {
        id,
        doc: Document::new(cid(client)),
    }
}

/// Subscribe `peer` to `ROOM`, asserting it joins, then clear its outbox.
fn subscribe(r: &mut Registry, peer: &Peer) {
    assert!(r.deliver(
        peer.id,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            last_seen_seq: 0,
            branch: Vec::new(),
        }
    ));
    let replies = r.take_outbox(peer.id);
    assert!(is_subscribed(&replies), "the peer must subscribe");
}

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

fn is_subscribed(replies: &[Message]) -> bool {
    replies
        .iter()
        .any(|m| matches!(m, Message::Ops { .. } | Message::Snapshot { .. }))
}

fn has_ops(replies: &[Message]) -> bool {
    replies.iter().any(|m| matches!(m, Message::Ops { .. }))
}

/// Write a fresh `key` from `peer` and assert the hub accepted it, so the op
/// lands tagged at the writer's version — the write that can lift the room's
/// op-version high-water. Returns the writer's own reply frames.
fn write(r: &mut Registry, peer: &mut Peer, key: &[u8]) -> Vec<Message> {
    let ops = peer.doc.transact(|tx| tx.register(key, Scalar::Int(1)));
    assert!(r.deliver(
        peer.id,
        Message::Ops {
            channel: Channel(0),
            ops,
        }
    ));
    let replies = r.take_outbox(peer.id);
    assert!(
        replies
            .iter()
            .any(|m| matches!(m, Message::Accepted { .. })),
        "the write must be accepted and logged"
    );
    replies
}

#[test]
fn a_lift_across_a_breaking_gap_evicts_the_stranded_peer() {
    let mut r = registry();
    // A v1 peer joins the empty room and binds it to UP v1 — admitted, since the
    // room holds no versioned op to reach.
    let v1 = hello(&mut r, 1, UP, 1);
    subscribe(&mut r, &v1);
    // A v2 peer joins — forward is always reachable — and lifts the binding to v2.
    let mut v2 = hello(&mut r, 2, UP, 2);
    subscribe(&mut r, &v2);
    // A second v2 peer stays reachable — the positive control that later fan-out
    // still flows to the room's remaining subscribers.
    let control = hello(&mut r, 3, UP, 2);
    subscribe(&mut r, &control);

    // The v2 peer writes a v2 op across the breaking rename, raising the room's
    // high-water past the v1 peer's reach.
    write(&mut r, &mut v2, b"years");

    // The stranded v1 peer is evicted with UpdateRequired.
    let evicted = r.take_outbox(v1.id);
    assert!(
        is_update_required(&evicted),
        "the v1 peer stranded by the lift is evicted"
    );
    r.take_outbox(control.id);

    // And removed from the room: a subsequent write reaches the control peer but
    // never the evicted one.
    write(&mut r, &mut v2, b"decades");
    assert!(
        !has_ops(&r.take_outbox(v1.id)),
        "the evicted peer is dropped from the room's fan-out"
    );
    assert!(
        has_ops(&r.take_outbox(control.id)),
        "a still-joined peer keeps receiving fan-out"
    );
}

#[test]
fn a_lift_across_a_back_compatible_gap_does_not_evict() {
    let mut r = registry();
    // DOWN's v1→v2 adds a field: back-compatible, so the v1 peer down-reaches v2.
    let v1 = hello(&mut r, 1, DOWN, 1);
    subscribe(&mut r, &v1);
    let mut v2 = hello(&mut r, 2, DOWN, 2);
    subscribe(&mut r, &v2);

    // The v2 op writes `age` — present in both versions, so its down-translation
    // survives — while its v2 tag still lifts the high-water None→v2.
    write(&mut r, &mut v2, b"age");

    let out = r.take_outbox(v1.id);
    assert!(
        !is_update_required(&out),
        "a back-compat lift does not strand the v1 peer"
    );
    assert!(
        has_ops(&out),
        "and it keeps receiving the translated fan-out"
    );
}

#[test]
fn the_writer_is_not_evicted_by_its_own_lift() {
    let mut r = registry();
    // The sole subscriber binds the room at v2 and then writes the v2 op that
    // lifts the high-water from nothing to v2 — the predicate must not evict the
    // author of its own lift.
    let mut v2 = hello(&mut r, 1, UP, 2);
    subscribe(&mut r, &v2);

    let out = write(&mut r, &mut v2, b"years");
    assert!(
        !is_update_required(&out),
        "the writer that lifted the high-water is not evicted"
    );
}

#[test]
fn a_relay_or_foreign_app_peer_is_never_evicted_on_a_lift() {
    let mut r = registry();
    // A v1 UP peer binds the room; a relay and a foreign-app peer both join it.
    let up1 = hello(&mut r, 1, UP, 1);
    subscribe(&mut r, &up1);
    let relay = hello(&mut r, 2, b"", 0);
    subscribe(&mut r, &relay);
    let foreign = hello(&mut r, 3, DOWN, 1);
    subscribe(&mut r, &foreign);

    // A v2 UP peer joins and writes a v2 op across the breaking rename, lifting
    // the high-water to v2.
    let mut v2 = hello(&mut r, 4, UP, 2);
    subscribe(&mut r, &v2);
    write(&mut r, &mut v2, b"years");

    assert!(
        !is_update_required(&r.take_outbox(relay.id)),
        "a relay peer is a different version space and is never evicted"
    );
    assert!(
        !is_update_required(&r.take_outbox(foreign.id)),
        "a foreign-app peer is never evicted"
    );
    // The v1 UP peer, the same app across the breaking gap, is the one stranded.
    assert!(
        is_update_required(&r.take_outbox(up1.id)),
        "the enforcing same-app v1 peer is evicted"
    );
}

#[test]
fn a_write_that_does_not_raise_the_high_water_evicts_nobody() {
    let mut r = registry();
    // A room floored at v2 with a reachable v2 peer subscribed.
    let mut writer = hello(&mut r, 1, UP, 2);
    subscribe(&mut r, &writer);
    write(&mut r, &mut writer, b"years");
    let peer = hello(&mut r, 2, UP, 2);
    subscribe(&mut r, &peer);

    // Another v2 write leaves the high-water at v2 — no lift, no re-check.
    write(&mut r, &mut writer, b"decades");
    let out = r.take_outbox(peer.id);
    assert!(
        !is_update_required(&out),
        "a non-raising write evicts nobody"
    );
    assert!(has_ops(&out), "the reachable peer keeps receiving fan-out");
}
