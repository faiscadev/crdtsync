//! Server-side throttle / coalesce of high-frequency awareness entries.
//!
//! A kind whose schema declares a `throttle` is rate-limited on the way out: the
//! first update fans out at once, and so does the first update once the window has
//! elapsed, but an update arriving inside the window is coalesced — recorded (its
//! last-seen time refreshes) but neither stored as the room's value nor fanned
//! out. The server caps the outbound rate; the client SDK's debounce owns
//! delivering the trailing value on its next past-window send. So the stored value
//! is always what the room was last sent, and a joiner replays exactly what
//! existing peers already hold. A kind with no declared throttle fans every update
//! out immediately. A [`ManualClock`] drives the window deterministically; the
//! per-kind throttle is supplied by the room's schema.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Message};
use crdtsync_server::{ConnId, ManualClock, Registry, SchemaRegistry};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM_A: &[u8] = b"room-a";
const APP: &[u8] = b"collab";
const THROTTLE: u64 = 100;

/// `cursor` is throttled at 100ms and `name` is not, so the two coalescing modes
/// are exercised side by side.
const SCHEMA: &str = r#"{ "schema": "collab", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "awareness": { "cursor": { "throttle": 100 }, "name": {} } }"#;

fn actor_of(client: u8) -> Vec<u8> {
    format!("actor-{client}").into_bytes()
}

fn registry_schema(src: &str) -> (Registry, Arc<ManualClock>) {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, src.as_bytes(), b"").unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    let clock = Arc::new(ManualClock::new(0));
    r.set_clock(clock.clone());
    (r, clock)
}

fn registry() -> (Registry, Arc<ManualClock>) {
    registry_schema(SCHEMA)
}

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
            channel: Channel(0),
            room: room.to_vec(),
            last_seen_seq: 0,
            branch: Vec::new(),
        }
    ));
    r.take_outbox(id);
}

fn set_awareness(r: &mut Registry, id: ConnId, key: &[u8], value: Vec<u8>) {
    assert!(r.deliver(
        id,
        Message::AwarenessSet {
            channel: Channel(0),
            key: key.to_vec(),
            value,
        }
    ));
}

/// The `(key, value)` of every awareness update in a batch of outbound messages.
fn updates(msgs: Vec<Message>) -> Vec<(Vec<u8>, Vec<u8>)> {
    msgs.into_iter()
        .filter_map(|m| match m {
            Message::AwarenessUpdate { key, value, .. } => Some((key, value)),
            _ => None,
        })
        .collect()
}

#[test]
fn the_first_update_fans_out_but_a_burst_within_the_window_is_coalesced() {
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1, APP, 1);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, a, ROOM_A);
    subscribe(&mut r, b, ROOM_A);

    // Three cursor updates inside one throttle window: only the first reaches the
    // peer; the rest are coalesced (recorded but not sent).
    set_awareness(&mut r, a, b"cursor", vec![1]);
    clock.advance(20);
    set_awareness(&mut r, a, b"cursor", vec![2]);
    clock.advance(20);
    set_awareness(&mut r, a, b"cursor", vec![3]);

    assert_eq!(
        updates(r.take_outbox(b)),
        vec![(b"cursor".to_vec(), vec![1])],
        "only the first update in the window fans out",
    );
}

#[test]
fn a_coalesced_update_is_never_fanned_out_by_a_sweep() {
    // The server does not run a trailing-edge timer: a coalesced value is not
    // flushed by the periodic sweep. Peers keep the last broadcast until the
    // client's next past-window send (which the SDK debounce produces).
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1, APP, 1);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, a, ROOM_A);
    subscribe(&mut r, b, ROOM_A);

    set_awareness(&mut r, a, b"cursor", vec![1]); // fans out
    clock.advance(20);
    set_awareness(&mut r, a, b"cursor", vec![2]); // coalesced
    r.take_outbox(b);

    // Sweeping well past the window flushes nothing — no trailing delivery.
    clock.advance(THROTTLE * 5);
    r.sweep();
    assert!(
        updates(r.take_outbox(b)).is_empty(),
        "no sweep delivers a coalesced value",
    );
}

#[test]
fn an_update_after_the_window_fans_out_immediately_again() {
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1, APP, 1);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, a, ROOM_A);
    subscribe(&mut r, b, ROOM_A);

    set_awareness(&mut r, a, b"cursor", vec![1]);
    r.take_outbox(b);

    // A second update spaced past the throttle window fans out at once — no
    // coalescing.
    clock.advance(THROTTLE);
    set_awareness(&mut r, a, b"cursor", vec![2]);
    assert_eq!(
        updates(r.take_outbox(b)),
        vec![(b"cursor".to_vec(), vec![2])]
    );
}

#[test]
fn a_kind_without_a_throttle_fans_out_every_update() {
    let (mut r, _clock) = registry();
    let a = hello_auth(&mut r, 1, APP, 1);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, a, ROOM_A);
    subscribe(&mut r, b, ROOM_A);

    // `name` declares no throttle, so a rapid burst is not coalesced.
    set_awareness(&mut r, a, b"name", vec![1]);
    set_awareness(&mut r, a, b"name", vec![2]);
    set_awareness(&mut r, a, b"name", vec![3]);
    assert_eq!(
        updates(r.take_outbox(b)),
        vec![
            (b"name".to_vec(), vec![1]),
            (b"name".to_vec(), vec![2]),
            (b"name".to_vec(), vec![3]),
        ],
    );
}

#[test]
fn the_originating_connection_is_not_echoed_its_own_update() {
    // Exclusion is by originating connection: the setter is not sent its own
    // update, but a genuine peer is.
    let (mut r, _clock) = registry();
    let a = hello_auth(&mut r, 1, APP, 1);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, a, ROOM_A);
    subscribe(&mut r, b, ROOM_A);

    set_awareness(&mut r, a, b"cursor", vec![1]);
    assert!(
        updates(r.take_outbox(a)).is_empty(),
        "the setter is not echoed its own update",
    );
    assert_eq!(
        updates(r.take_outbox(b)),
        vec![(b"cursor".to_vec(), vec![1])]
    );
}

#[test]
fn a_second_connection_of_the_same_client_still_receives_updates() {
    // Presence is coalesced by owning client, but fan-out excludes only the
    // originating connection — a client's other connection (another tab) is a
    // recipient and keeps receiving, on both the immediate and later windows.
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1, APP, 1);
    let a2 = hello_auth(&mut r, 1, APP, 1); // same client id, second connection
    subscribe(&mut r, a, ROOM_A);
    subscribe(&mut r, a2, ROOM_A);

    set_awareness(&mut r, a, b"cursor", vec![1]);
    assert_eq!(
        updates(r.take_outbox(a2)),
        vec![(b"cursor".to_vec(), vec![1])],
        "the sibling connection receives the first update",
    );

    clock.advance(THROTTLE);
    set_awareness(&mut r, a, b"cursor", vec![2]);
    assert_eq!(
        updates(r.take_outbox(a2)),
        vec![(b"cursor".to_vec(), vec![2])],
        "and keeps receiving past-window updates",
    );
}

#[test]
fn a_joiner_replays_the_last_broadcast_value_not_a_coalesced_one() {
    // A joiner is replayed the current presence — the last value the room was
    // sent, which is exactly what existing peers hold. A coalesced value that was
    // never fanned out is not surfaced, so a joiner and existing peers never
    // disagree about a peer's presence.
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1, APP, 1);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, a, ROOM_A);
    subscribe(&mut r, b, ROOM_A);

    set_awareness(&mut r, a, b"cursor", vec![1]); // fans out to b
    clock.advance(20);
    set_awareness(&mut r, a, b"cursor", vec![2]); // coalesced, never sent
    assert_eq!(
        updates(r.take_outbox(b)),
        vec![(b"cursor".to_vec(), vec![1])],
        "the peer holds the broadcast value",
    );

    let joiner = hello_auth(&mut r, 3, APP, 1);
    assert!(r.deliver(
        joiner,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
            branch: Vec::new(),
        }
    ));
    assert_eq!(
        updates(r.take_outbox(joiner)),
        vec![(b"cursor".to_vec(), vec![1])],
        "the joiner replays the same broadcast value, not the coalesced one",
    );
}

/// `cursor` carries a short TTL alongside its throttle, so a coalesced update's
/// effect on the TTL clock can be observed.
const SCHEMA_TTL: &str = r#"{ "schema": "collab", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "awareness": { "cursor": { "throttle": 100, "ttl": 250 } } }"#;

#[test]
fn a_coalesced_update_refreshes_the_ttl_so_a_streaming_entry_does_not_expire() {
    // A coalesced (not fanned out) update still counts as activity: it refreshes
    // the entry's last-seen time, so a client streaming faster than its throttle
    // is not timed out mid-stream despite most of its updates being suppressed.
    let (mut r, clock) = registry_schema(SCHEMA_TTL);
    let a = hello_auth(&mut r, 1, APP, 1);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, a, ROOM_A);
    subscribe(&mut r, b, ROOM_A);

    set_awareness(&mut r, a, b"cursor", vec![1]); // fans out, last-seen = 0
    r.take_outbox(b);

    // Keep setting inside the throttle window (each coalesced) past the TTL from
    // the first set, refreshing last-seen each time.
    for step in 1..=6 {
        clock.advance(50);
        set_awareness(&mut r, a, b"cursor", vec![step + 1]);
        r.sweep();
    }
    // 300ms have elapsed (> 250ms TTL) but the last set was only 50ms ago, so the
    // entry is alive — no clear was fanned out.
    let msgs = r.take_outbox(b);
    assert!(
        !msgs
            .iter()
            .any(|m| matches!(m, Message::AwarenessClearKey { .. })),
        "a streaming entry kept alive by coalesced sets is not expired",
    );

    // Once it does fall silent past the TTL, the sweep expires it.
    clock.advance(300);
    r.sweep();
    assert!(
        r.take_outbox(b)
            .iter()
            .any(|m| matches!(m, Message::AwarenessClearKey { .. })),
        "a genuinely silent entry still expires",
    );
}
