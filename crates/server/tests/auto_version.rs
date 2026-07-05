//! Declarative auto-versioning — the first built-in engine-event sink.
//!
//! A room's governing schema may declare `autoVersion` triggers: on a matching
//! lifecycle event ([`EngineEvent`](crdtsync_server::EngineEvent)), the engine
//! captures a named version of that room, expanding the name template
//! (`${timestamp}`, `${event}`) at fire time. Only room-bearing events drive a
//! capture (subscribe, version create/rename/delete, compaction); a relay room
//! with no governing schema never auto-versions; an `every:` schedule trigger is
//! the scheduler's job, not an event's. The `keep` retention window is a follow-on
//! unit — it parses but is inert here. A [`ManualClock`] drives `${timestamp}`
//! deterministically.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Op, Scalar};
use crdtsync_server::{ConnId, ManualClock, Registry, SchemaRegistry};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

const ROOM: &[u8] = b"room-a";
const APP: &[u8] = b"collab";
const CH: Channel = Channel(0);

fn actor_of(client: u8) -> Vec<u8> {
    format!("actor-{client}").into_bytes()
}

/// A schema of `APP` version 1 declaring `body` as its `autoVersion` array.
fn schema(body: &str) -> String {
    format!(
        r#"{{ "schema": "collab", "version": 1, "root": "R",
            "types": {{ "R": {{ "kind": "map" }} }},
            "autoVersion": {body} }}"#
    )
}

/// A registry whose shared schema registry holds `APP` version 1 with the given
/// `autoVersion` body, driven by a manual clock starting at 0.
fn registry_with(auto_version: &str) -> (Registry, Arc<ManualClock>) {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, schema(auto_version).as_bytes(), b"")
        .unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    let clock = Arc::new(ManualClock::new(0));
    r.set_clock(clock.clone());
    (r, clock)
}

/// Hello + Auth a connection declaring `{APP, version}` — enforcing for a
/// registered app, relay for an empty id.
fn hello_auth(r: &mut Registry, client: u8, app_id: &[u8], version: u32) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: app_id.to_vec(),
            schema_version: version,
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: actor_of(client),
        }
    ));
    r.take_outbox(id);
    id
}

fn subscribe(r: &mut Registry, id: ConnId, room: &[u8]) {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: CH,
            room: room.to_vec(),
            last_seen_seq: 0,
        }
    ));
    r.take_outbox(id);
}

/// Ingest a register write, so the room exists with state to capture.
fn write(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(id, Message::Ops { channel: CH, ops }));
    r.take_outbox(id);
}

fn a_write() -> Vec<Op> {
    doc(1).transact(|tx| tx.register(b"k", Scalar::Int(1)))
}

fn version_names(r: &Registry) -> Vec<Vec<u8>> {
    r.hub().version_names(ROOM)
}

/// A 20-digit zero-padded millis stamp — how a name template's `${timestamp}`
/// renders, so the names sort chronologically.
fn stamp(millis: u64) -> String {
    format!("{millis:020}")
}

/// Bring `ROOM` into existence bound to `APP`: an enforcing subscribe (empty room,
/// captures nothing) then a write, so later subscribes have state to version.
fn seed_room(r: &mut Registry) -> ConnId {
    let a = hello_auth(r, 1, APP, 1);
    subscribe(r, a, ROOM);
    write(r, a, a_write());
    a
}

#[test]
fn a_subscribe_trigger_captures_a_version_on_join() {
    let (mut r, clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}" }]"#);
    seed_room(&mut r);

    clock.advance(1000);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM);

    assert_eq!(
        version_names(&r),
        vec![format!("auto/join/{}", stamp(1000)).into_bytes()],
        "the join captures exactly one version, named at the clock",
    );
}

#[test]
fn the_event_token_expands_to_the_kebab_event_name() {
    let (mut r, _clock) = registry_with(r#"[{ "on": "subscribe", "name": "auto/${event}" }]"#);
    seed_room(&mut r);

    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM);

    assert_eq!(version_names(&r), vec![b"auto/subscribe".to_vec()]);
}

#[test]
fn a_capture_on_an_empty_room_makes_no_version() {
    let (mut r, _clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}" }]"#);
    // The first subscriber joins a room with no state — the trigger fires but has
    // nothing to capture.
    let a = hello_auth(&mut r, 1, APP, 1);
    subscribe(&mut r, a, ROOM);
    assert!(version_names(&r).is_empty());
}

#[test]
fn the_first_enforcing_subscriber_to_a_populated_room_captures() {
    // A relay client populates the room (no schema governs it yet), then the first
    // enforcing subscriber joins. Recording arms as its subscribe is authorized —
    // before its `Subscribed` fires — so that very first join captures, not only
    // the second. (A latch armed after the event would miss it.)
    let (mut r, clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}" }]"#);
    let relay = hello_auth(&mut r, 1, b"", 0);
    subscribe(&mut r, relay, ROOM);
    write(&mut r, relay, a_write());

    clock.advance(500);
    let enforcing = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, enforcing, ROOM);

    assert_eq!(
        version_names(&r),
        vec![format!("auto/join/{}", stamp(500)).into_bytes()],
    );
}

#[test]
fn a_relay_room_never_auto_versions() {
    let (mut r, _clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}" }]"#);
    // Both connect under an empty app id — no schema governs the room, so its
    // autoVersion triggers are never resolved.
    let a = hello_auth(&mut r, 1, b"", 0);
    subscribe(&mut r, a, ROOM);
    write(&mut r, a, a_write());
    let b = hello_auth(&mut r, 2, b"", 0);
    subscribe(&mut r, b, ROOM);
    assert!(version_names(&r).is_empty());
}

#[test]
fn a_non_matching_event_does_not_fire() {
    let (mut r, _clock) =
        registry_with(r#"[{ "on": "compaction", "name": "auto/c/${timestamp}" }]"#);
    // Only compaction is declared; a subscribe must not capture.
    seed_room(&mut r);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM);
    assert!(version_names(&r).is_empty());
}

#[test]
fn a_compaction_event_captures_a_version() {
    let (mut r, clock) =
        registry_with(r#"[{ "on": "compaction", "name": "auto/c/${timestamp}" }]"#);
    r.set_compaction_threshold(1);
    let a = hello_auth(&mut r, 1, APP, 1);
    subscribe(&mut r, a, ROOM);

    clock.advance(500);
    // The write's ingest crosses the threshold and compacts, emitting Compacted.
    write(&mut r, a, a_write());

    assert_eq!(
        version_names(&r),
        vec![format!("auto/c/{}", stamp(500)).into_bytes()],
    );
}

#[test]
fn an_every_schedule_trigger_does_not_fire_on_an_event() {
    let (mut r, _clock) =
        registry_with(r#"[{ "every": "1h", "name": "auto/hourly/${timestamp}" }]"#);
    // A schedule trigger is the scheduler's concern; no lifecycle event fires it.
    seed_room(&mut r);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM);
    assert!(version_names(&r).is_empty());
}

#[test]
fn two_triggers_on_the_same_event_both_fire() {
    let (mut r, clock) = registry_with(
        r#"[{ "on": "subscribe", "name": "a/${timestamp}" },
             { "on": "subscribe", "name": "b/${timestamp}" }]"#,
    );
    seed_room(&mut r);

    clock.advance(7);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM);

    assert_eq!(
        version_names(&r),
        vec![
            format!("a/{}", stamp(7)).into_bytes(),
            format!("b/{}", stamp(7)).into_bytes(),
        ],
    );
}

#[test]
fn keep_is_parsed_but_not_yet_enforced() {
    // Retention (bounding a trigger's captures to its `keep` window) needs durable
    // per-version provenance and is a follow-on unit; until then `keep` parses but
    // is inert, so every capture is retained.
    let (mut r, clock) =
        registry_with(r#"[{ "on": "subscribe", "name": "auto/join/${timestamp}", "keep": 2 }]"#);
    seed_room(&mut r);

    for client in [2u8, 3, 4] {
        clock.advance(1000);
        let c = hello_auth(&mut r, client, APP, 1);
        subscribe(&mut r, c, ROOM);
    }

    assert_eq!(
        version_names(&r),
        vec![
            format!("auto/join/{}", stamp(1000)).into_bytes(),
            format!("auto/join/{}", stamp(2000)).into_bytes(),
            format!("auto/join/{}", stamp(3000)).into_bytes(),
        ],
        "keep is inert for now — all three captures retained despite keep:2",
    );
}

#[test]
fn an_auto_created_version_does_not_cascade() {
    // A subscribe trigger captures a version, whose VersionCreated event a
    // version-created trigger would in turn capture — an unbounded cascade. The
    // engine suppresses recording while it drains, so the auto-created version
    // never re-fires: exactly the subscribe capture, not a version-created one.
    let (mut r, _clock) = registry_with(
        r#"[{ "on": "subscribe", "name": "auto/join/x" },
             { "on": "version-created", "name": "auto/vc/x" }]"#,
    );
    seed_room(&mut r);

    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, b, ROOM);

    assert_eq!(
        version_names(&r),
        vec![b"auto/join/x".to_vec()],
        "the join capture does not cascade into a version-created capture",
    );
}
