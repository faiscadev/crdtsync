//! Schema-driven timed-TTL expiry end to end.
//!
//! With no injected policy, the sweep resolves each room's governing schema from
//! the shared registry — the `{app_id, version}` its enforcing subscribers
//! declared at the handshake — parses it, and reads each awareness kind's
//! declared TTL. A kind with a `ttl` expires once silent past it; a kind with
//! none, and a relay room with no governing schema at all, are session-lifetime.
//! A [`ManualClock`] drives the silence window so the expiry is deterministic.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Message};
use crdtsync_server::{
    Action, Authorizer, ConnId, Identity, ManualClock, NoTimedTtl, Registry, Resource,
    SchemaRegistry,
};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM_A: &[u8] = b"room-a";
const APP: &[u8] = b"collab";
const TTL: u64 = 5000;

/// A schema declaring a timed `cursor` and a session-lifetime `presence`.
const SCHEMA: &str = r#"{ "schema": "collab", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "awareness": { "cursor": { "ttl": 5000 }, "presence": {} } }"#;

/// Version 2 of the same app, widening `cursor`'s TTL — a rolling upgrade.
const SCHEMA_V2: &str = r#"{ "schema": "collab", "version": 2, "root": "R",
    "types": { "R": { "kind": "map" } },
    "awareness": { "cursor": { "ttl": 30000 }, "presence": {} } }"#;
const TTL_V2: u64 = 30000;

/// A second app whose id sorts *after* `APP`, with a shorter cursor TTL — a
/// foreign app that must not re-govern a room `APP` already bound.
const APP2: &[u8] = b"zzz";
const SCHEMA_APP2: &str = r#"{ "schema": "zzz", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "awareness": { "cursor": { "ttl": 1000 } } }"#;

fn actor_of(client: u8) -> Vec<u8> {
    format!("actor-{client}").into_bytes()
}

/// A registry whose shared schema registry holds `SCHEMA` at version 1 of `APP`,
/// driven by a manual clock and left on the default (schema-resolving) policy.
fn registry() -> (Registry, Arc<ManualClock>) {
    registry_versions(1)
}

/// As [`registry`], registering versions `1..=up_to` of `APP` — version 2
/// widens the `cursor` TTL, standing in for a rolling schema upgrade.
fn registry_versions(up_to: u32) -> (Registry, Arc<ManualClock>) {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, SCHEMA.as_bytes(), b"").unwrap();
    if up_to >= 2 {
        sr.register(
            APP,
            2,
            SCHEMA_V2.as_bytes(),
            br#"{ "from": 1, "to": 2, "steps": [] }"#,
        )
        .unwrap();
    }
    from_registry(sr)
}

/// A registry holding both `APP` (5s cursor) and `APP2` (1s cursor), for the
/// foreign-app governance test.
fn registry_two_apps() -> (Registry, Arc<ManualClock>) {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, SCHEMA.as_bytes(), b"").unwrap();
    sr.register(APP2, 1, SCHEMA_APP2.as_bytes(), b"").unwrap();
    from_registry(sr)
}

/// A manual-clock registry over `sr`, left on the default schema-resolving policy.
fn from_registry(sr: SchemaRegistry) -> (Registry, Arc<ManualClock>) {
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    let clock = Arc::new(ManualClock::new(0));
    r.set_clock(clock.clone());
    (r, clock)
}

/// An authorizer denying `actor` the read of any room, permitting all else — so
/// its subscribe is refused while others' succeed.
fn deny_read(actor: &'static [u8]) -> Box<dyn Authorizer> {
    Box::new(move |id: &Identity, action: Action, _res: &Resource| {
        !(action == Action::Read && id.actor() == actor)
    })
}

/// Hello + Auth a connection declaring `{app_id, version}`, so it resolves to the
/// tier the registry assigns — enforcing for a registered app, relay otherwise.
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

fn clear_keys(msgs: Vec<Message>) -> Vec<(Vec<u8>, Vec<u8>)> {
    msgs.into_iter()
        .filter_map(|m| match m {
            Message::AwarenessClearKey { actor, key, .. } => Some((actor, key)),
            _ => None,
        })
        .collect()
}

/// The presence kinds a joining subscriber is replayed — the room's live entries.
fn replayed_keys(r: &mut Registry, room: &[u8]) -> Vec<Vec<u8>> {
    let observer = hello_auth(r, 0x99, APP, 1);
    assert!(r.deliver(
        observer,
        Message::Subscribe {
            channel: Channel(0),
            room: room.to_vec(),
            last_seen_seq: 0,
        }
    ));
    r.take_outbox(observer)
        .into_iter()
        .filter_map(|m| match m {
            Message::AwarenessUpdate { key, .. } => Some(key),
            _ => None,
        })
        .collect()
}

#[test]
fn a_schema_ttl_expires_a_silent_cursor_but_not_a_session_lifetime_kind() {
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1, APP, 1);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, a, ROOM_A);
    subscribe(&mut r, b, ROOM_A);

    // `cursor` carries the schema's 5s TTL; `presence` declares none.
    set_awareness(&mut r, a, b"cursor", vec![1]);
    set_awareness(&mut r, a, b"presence", vec![2]);
    r.take_outbox(b);

    clock.advance(TTL + 1);
    r.sweep();

    assert_eq!(
        clear_keys(r.take_outbox(b)),
        vec![(actor_of(1), b"cursor".to_vec())],
        "only the TTL'd kind is cleared, and its key is named",
    );
    let live = replayed_keys(&mut r, ROOM_A);
    assert!(!live.contains(&b"cursor".to_vec()));
    assert!(live.contains(&b"presence".to_vec()));
}

#[test]
fn a_cursor_at_exactly_its_schema_ttl_has_not_yet_expired() {
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1, APP, 1);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, a, ROOM_A);
    subscribe(&mut r, b, ROOM_A);
    set_awareness(&mut r, a, b"cursor", vec![1]);
    r.take_outbox(b);

    clock.advance(TTL);
    r.sweep();
    assert!(clear_keys(r.take_outbox(b)).is_empty());
    assert!(replayed_keys(&mut r, ROOM_A).contains(&b"cursor".to_vec()));
}

#[test]
fn a_relay_room_with_no_governing_schema_never_times_out() {
    let (mut r, clock) = registry();
    // Both connect under an empty app id — no schema governs the room.
    let a = hello_auth(&mut r, 1, b"", 0);
    let b = hello_auth(&mut r, 2, b"", 0);
    subscribe(&mut r, a, ROOM_A);
    subscribe(&mut r, b, ROOM_A);
    set_awareness(&mut r, a, b"cursor", vec![1]);
    r.take_outbox(b);

    clock.advance(1_000_000);
    r.sweep();
    assert!(clear_keys(r.take_outbox(b)).is_empty());
}

#[test]
fn an_enforcing_subscriber_governs_the_ttl_for_a_relay_peer_in_the_room() {
    let (mut r, clock) = registry();
    // The publisher is enforcing; the observer joined as a relay. The room's TTL
    // is resolved from the enforcing subscriber's schema all the same.
    let publisher = hello_auth(&mut r, 1, APP, 1);
    let observer = hello_auth(&mut r, 2, b"", 0);
    subscribe(&mut r, publisher, ROOM_A);
    subscribe(&mut r, observer, ROOM_A);
    set_awareness(&mut r, publisher, b"cursor", vec![1]);
    r.take_outbox(observer);

    clock.advance(TTL + 1);
    r.sweep();
    assert_eq!(
        clear_keys(r.take_outbox(observer)),
        vec![(actor_of(1), b"cursor".to_vec())],
    );
}

#[test]
fn a_bound_room_keeps_timing_out_after_its_last_enforcing_client_leaves() {
    let (mut r, clock) = registry();
    // An enforcing client binds the room's schema, then leaves; a relay client
    // holding presence stays. The binding persists, so the relay's cursor still
    // times out — it does not become a permanent ghost.
    let enforcing = hello_auth(&mut r, 1, APP, 1);
    let relay = hello_auth(&mut r, 2, b"", 0);
    let watcher = hello_auth(&mut r, 3, b"", 0);
    subscribe(&mut r, enforcing, ROOM_A);
    subscribe(&mut r, relay, ROOM_A);
    subscribe(&mut r, watcher, ROOM_A);

    set_awareness(&mut r, relay, b"cursor", vec![1]);
    r.take_outbox(watcher);

    // The only enforcing subscriber disconnects (it held no presence, so no
    // grace clear); the schema binding it established remains.
    r.disconnect(enforcing);

    clock.advance(TTL + 1);
    r.sweep();
    assert_eq!(
        clear_keys(r.take_outbox(watcher)),
        vec![(actor_of(2), b"cursor".to_vec())],
        "the relay's cursor expires under the room's persisted schema binding",
    );
}

#[test]
fn a_newer_schema_version_governs_the_room_over_an_older_one() {
    let (mut r, clock) = registry_versions(2);
    // Two clients of the same app on different versions share a room (a rolling
    // upgrade). The higher version governs, so its wider cursor TTL applies.
    let v1 = hello_auth(&mut r, 1, APP, 1);
    let v2 = hello_auth(&mut r, 2, APP, 2);
    let watcher = hello_auth(&mut r, 3, b"", 0);
    subscribe(&mut r, v1, ROOM_A);
    subscribe(&mut r, v2, ROOM_A);
    subscribe(&mut r, watcher, ROOM_A);

    set_awareness(&mut r, v2, b"cursor", vec![1]);
    r.take_outbox(watcher);

    // Past v1's 5s TTL but within v2's 30s: the newer version governs, so the
    // cursor survives.
    clock.advance(TTL + 1);
    r.sweep();
    assert!(
        clear_keys(r.take_outbox(watcher)).is_empty(),
        "the newer version's wider TTL governs, not the older 5s one",
    );

    // Past v2's TTL, it finally expires.
    clock.advance(TTL_V2);
    r.sweep();
    assert_eq!(
        clear_keys(r.take_outbox(watcher)),
        vec![(actor_of(2), b"cursor".to_vec())],
    );
}

#[test]
fn a_read_denied_subscribe_does_not_bind_the_rooms_schema() {
    let (mut r, clock) = registry();
    // The one enforcing client is denied read on the room, so its subscribe is
    // rejected — it must not schema-govern the room and force presence to expire.
    r.set_authorizer(deny_read(b"actor-1"));
    let enforcing = hello_auth(&mut r, 1, APP, 1);
    let relay = hello_auth(&mut r, 2, b"", 0);
    let watcher = hello_auth(&mut r, 3, b"", 0);

    // A rejected subscribe: the connection stays open but binds nothing.
    assert!(r.deliver(
        enforcing,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
        }
    ));
    r.take_outbox(enforcing);
    subscribe(&mut r, relay, ROOM_A);
    subscribe(&mut r, watcher, ROOM_A);
    set_awareness(&mut r, relay, b"cursor", vec![1]);
    r.take_outbox(watcher);

    clock.advance(TTL + 1);
    r.sweep();
    assert!(
        clear_keys(r.take_outbox(watcher)).is_empty(),
        "a rejected subscribe must not schema-govern the room",
    );
}

#[test]
fn a_foreign_app_subscribe_does_not_override_the_governing_schema() {
    let (mut r, clock) = registry_two_apps();
    // `APP` (5s cursor) binds the room first; a client of `APP2` (1s cursor, an
    // app id sorting after `APP`) then joins. The first-binding app governs — the
    // foreign app never re-governs the room to its shorter TTL.
    let collab = hello_auth(&mut r, 1, APP, 1);
    subscribe(&mut r, collab, ROOM_A);
    let foreign = hello_auth(&mut r, 2, APP2, 1);
    subscribe(&mut r, foreign, ROOM_A);
    let watcher = hello_auth(&mut r, 3, b"", 0);
    subscribe(&mut r, watcher, ROOM_A);

    set_awareness(&mut r, collab, b"cursor", vec![1]);
    r.take_outbox(watcher);

    // Past the foreign 1s TTL but within `APP`'s 5s: the cursor survives.
    clock.advance(1001);
    r.sweep();
    assert!(
        clear_keys(r.take_outbox(watcher)).is_empty(),
        "the first-binding app governs, not a foreign app's shorter TTL",
    );

    // Past `APP`'s own TTL, it expires.
    clock.advance(TTL);
    r.sweep();
    assert_eq!(
        clear_keys(r.take_outbox(watcher)),
        vec![(actor_of(1), b"cursor".to_vec())],
    );
}

#[test]
fn a_foreign_app_cannot_seize_a_room_from_an_incumbent_in_its_grace_window() {
    let (mut r, clock) = registry_two_apps();
    // `APP` (5s cursor) binds the room and publishes a cursor, then its socket
    // blips — presence retained for the grace window. A `APP2` client (1s cursor)
    // subscribes during the gap. It must not seize governance: the incumbent's
    // presence survives its own 5s TTL, not the foreign 1s one.
    let incumbent = hello_auth(&mut r, 1, APP, 1);
    subscribe(&mut r, incumbent, ROOM_A);
    let watcher = hello_auth(&mut r, 3, b"", 0);
    subscribe(&mut r, watcher, ROOM_A);
    set_awareness(&mut r, incumbent, b"cursor", vec![1]);
    r.take_outbox(watcher);

    r.disconnect(incumbent); // into the grace window; presence retained
    let foreign = hello_auth(&mut r, 2, APP2, 1);
    subscribe(&mut r, foreign, ROOM_A);

    // Past the foreign 1s TTL but within both `APP`'s 5s TTL and the grace
    // window: the incumbent still governs, so its cursor is not expired.
    clock.advance(1001);
    r.sweep();
    assert!(
        clear_keys(r.take_outbox(watcher)).is_empty(),
        "a foreign app must not grief-expire the incumbent's grace-held presence",
    );
}

#[test]
fn a_departed_newer_versions_grace_presence_is_not_expired_under_an_older_peers_ttl() {
    let (mut r, clock) = registry_versions(2);
    r.set_grace_millis(100_000); // long grace, so the TTL — not grace — is under test
                                 // A v1 client (5s cursor) and a v2 client (30s cursor) share the room during a
                                 // rolling upgrade. The v2 client publishes a cursor, then blips into its grace
                                 // window; the v1 client stays. The room must stay governed at v2, so the
                                 // grace-held cursor keeps its 30s TTL rather than the peer's 5s one.
    let v1 = hello_auth(&mut r, 1, APP, 1);
    let v2 = hello_auth(&mut r, 2, APP, 2);
    let watcher = hello_auth(&mut r, 3, b"", 0);
    subscribe(&mut r, v1, ROOM_A);
    subscribe(&mut r, v2, ROOM_A);
    subscribe(&mut r, watcher, ROOM_A);

    set_awareness(&mut r, v2, b"cursor", vec![1]);
    r.take_outbox(watcher);
    r.disconnect(v2); // into the (long) grace window

    // Past v1's 5s TTL but within v2's 30s: the room stays at v2, cursor survives.
    clock.advance(TTL + 1);
    r.sweep();
    assert!(
        clear_keys(r.take_outbox(watcher)).is_empty(),
        "the governing version must not downgrade and expire grace-held v2 presence early",
    );
}

#[test]
fn an_injected_policy_is_authoritative_and_can_suppress_schema_expiry() {
    let (mut r, clock) = registry();
    // An injected policy governs alone: `NoTimedTtl` keeps every entry session-
    // lifetime even though the room's schema declares a 5s cursor TTL.
    r.set_awareness_policy(Arc::new(NoTimedTtl));
    let a = hello_auth(&mut r, 1, APP, 1);
    let b = hello_auth(&mut r, 2, APP, 1);
    subscribe(&mut r, a, ROOM_A);
    subscribe(&mut r, b, ROOM_A);
    set_awareness(&mut r, a, b"cursor", vec![1]);
    r.take_outbox(b);

    clock.advance(TTL + 1);
    r.sweep();
    assert!(
        clear_keys(r.take_outbox(b)).is_empty(),
        "an injected no-TTL policy overrides the schema's TTL",
    );
}

#[test]
fn a_dormant_rooms_binding_is_dropped_and_a_later_relay_is_session_lifetime() {
    let (mut r, clock) = registry();
    // An enforcing client binds the room, then leaves with the room fully dormant
    // — no presence, no subscribers. A sweep prunes the binding.
    let enforcing = hello_auth(&mut r, 1, APP, 1);
    subscribe(&mut r, enforcing, ROOM_A);
    r.disconnect(enforcing);
    r.sweep();

    // A relay-only session later holds presence; with the binding pruned the room
    // is session-lifetime again — no enforcing client ever governed this session.
    let relay = hello_auth(&mut r, 2, b"", 0);
    let watcher = hello_auth(&mut r, 3, b"", 0);
    subscribe(&mut r, relay, ROOM_A);
    subscribe(&mut r, watcher, ROOM_A);
    set_awareness(&mut r, relay, b"cursor", vec![1]);
    r.take_outbox(watcher);

    clock.advance(TTL + 1);
    r.sweep();
    assert!(
        clear_keys(r.take_outbox(watcher)).is_empty(),
        "a pruned binding leaves a later relay room session-lifetime",
    );
}

#[test]
fn a_version_0_dynamic_client_adopts_the_head_schema_and_its_ttl() {
    let (mut r, clock) = registry();
    // Version 0 declares a dynamic client: it adopts the chain head (version 1),
    // so the head's `cursor` TTL governs its presence.
    let a = hello_auth(&mut r, 1, APP, 0);
    let b = hello_auth(&mut r, 2, APP, 0);
    subscribe(&mut r, a, ROOM_A);
    subscribe(&mut r, b, ROOM_A);
    set_awareness(&mut r, a, b"cursor", vec![1]);
    r.take_outbox(b);

    clock.advance(TTL + 1);
    r.sweep();
    assert_eq!(
        clear_keys(r.take_outbox(b)),
        vec![(actor_of(1), b"cursor".to_vec())],
    );
}
