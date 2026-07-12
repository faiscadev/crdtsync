//! Timed-TTL expiry for ephemeral presence.
//!
//! An awareness entry whose kind carries a timed TTL is dropped once it has gone
//! silent longer than that TTL — the periodic [`sweep`](crdtsync_server::Registry::sweep)
//! removes it and tells the room's subscribers with an
//! [`AwarenessClearKey`](crdtsync_core::Message::AwarenessClearKey) naming just
//! that entry, so the actor's other entries (and its connection) live on. A kind
//! with no declared TTL is session-lifetime and never expires this way. A
//! [`ManualClock`] drives the silence window; the per-kind TTL is supplied by an
//! injected [`AwarenessPolicy`], so the enforcement is exercised without a schema.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Message};
use crdtsync_server::{
    Action, Authorizer, AwarenessPolicy, ConnId, Identity, ManualClock, Registry, Resource,
};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM_A: &[u8] = b"room-a";
const TTL: u64 = 5000;

fn actor_of(client: u8) -> Vec<u8> {
    format!("actor-{client}").into_bytes()
}

/// A per-kind TTL policy backed by a fixed map — a kind absent from the map is
/// session-lifetime (no timed TTL), standing in for the schema-derived policy.
struct TtlMap(HashMap<Vec<u8>, u64>);

impl AwarenessPolicy for TtlMap {
    fn ttl(&self, _room: &[u8], key: &[u8]) -> Option<u64> {
        self.0.get(key).copied()
    }
}

/// A registry driven by a shared manual clock, timing `cursor` out after `TTL`
/// and leaving every other kind session-lifetime.
fn registry() -> (Registry, Arc<ManualClock>) {
    let mut r = Registry::new(cid(0xFF));
    let clock = Arc::new(ManualClock::new(0));
    r.set_clock(clock.clone());
    let mut ttls = HashMap::new();
    ttls.insert(b"cursor".to_vec(), TTL);
    r.set_awareness_policy(Arc::new(TtlMap(ttls)));
    (r, clock)
}

fn hello_auth(r: &mut Registry, client: u8) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: Vec::new(),
            schema_version: 0,
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

/// Hello + Auth a connection under an explicit credential, so two distinct
/// clients can resolve to the same actor (the dev verifier echoes the credential).
fn hello_auth_as(r: &mut Registry, client: u8, credential: &[u8]) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: Vec::new(),
            schema_version: 0,
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: credential.to_vec(),
        }
    ));
    r.take_outbox(id);
    id
}

/// An authorizer that denies `actor-2` reads once its flag is set, permitting
/// everything else — a peer can subscribe while allowed, then lose the room.
fn revocable() -> (Arc<AtomicBool>, Box<dyn Authorizer>) {
    let revoked = Arc::new(AtomicBool::new(false));
    let flag = revoked.clone();
    let authorizer: Box<dyn Authorizer> =
        Box::new(move |id: &Identity, action: Action, _res: &Resource| {
            !(action == Action::Read && id.actor() == b"actor-2" && flag.load(Ordering::SeqCst))
        });
    (revoked, authorizer)
}

fn subscribe(r: &mut Registry, id: ConnId, channel: u32, room: &[u8]) {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(channel),
            room: room.to_vec(),
            last_seen_seq: 0,
            branch: Vec::new(),
        }
    ));
    r.take_outbox(id);
}

fn set_awareness(r: &mut Registry, id: ConnId, channel: u32, key: &[u8], value: Vec<u8>) {
    assert!(r.deliver(
        id,
        Message::AwarenessSet {
            channel: Channel(channel),
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
    let observer = hello_auth(r, 0x99);
    assert!(r.deliver(
        observer,
        Message::Subscribe {
            channel: Channel(0),
            room: room.to_vec(),
            last_seen_seq: 0,
            branch: Vec::new(),
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
fn a_silent_entry_past_its_ttl_is_swept_and_cleared_per_key() {
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1);
    let b = hello_auth(&mut r, 2);
    subscribe(&mut r, a, 0, ROOM_A);
    subscribe(&mut r, b, 0, ROOM_A);

    set_awareness(&mut r, a, 0, b"cursor", vec![1]);
    r.take_outbox(b); // drain the fan-out AwarenessUpdate

    // Silence just past the TTL: the sweep drops the entry and tells the peer.
    clock.advance(TTL + 1);
    r.sweep();

    assert_eq!(
        clear_keys(r.take_outbox(b)),
        vec![(actor_of(1), b"cursor".to_vec())],
        "the peer is told exactly which entry expired",
    );
    assert!(
        !replayed_keys(&mut r, ROOM_A).contains(&b"cursor".to_vec()),
        "the expired entry is gone from the room",
    );
}

#[test]
fn an_entry_at_exactly_its_ttl_has_not_yet_expired() {
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1);
    let b = hello_auth(&mut r, 2);
    subscribe(&mut r, a, 0, ROOM_A);
    subscribe(&mut r, b, 0, ROOM_A);
    set_awareness(&mut r, a, 0, b"cursor", vec![1]);
    r.take_outbox(b);

    // Silence of exactly the TTL is not yet past it.
    clock.advance(TTL);
    r.sweep();
    assert!(clear_keys(r.take_outbox(b)).is_empty());
    assert!(replayed_keys(&mut r, ROOM_A).contains(&b"cursor".to_vec()));
}

#[test]
fn an_entry_without_a_ttl_is_session_lifetime_and_survives() {
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1);
    let b = hello_auth(&mut r, 2);
    subscribe(&mut r, a, 0, ROOM_A);
    subscribe(&mut r, b, 0, ROOM_A);

    // `cursor` has a TTL; `typing` does not.
    set_awareness(&mut r, a, 0, b"cursor", vec![1]);
    set_awareness(&mut r, a, 0, b"typing", vec![2]);
    r.take_outbox(b);

    clock.advance(TTL + 1);
    r.sweep();

    // Only the TTL'd kind is cleared; the session-lifetime one stays.
    assert_eq!(
        clear_keys(r.take_outbox(b)),
        vec![(actor_of(1), b"cursor".to_vec())],
    );
    let live = replayed_keys(&mut r, ROOM_A);
    assert!(!live.contains(&b"cursor".to_vec()));
    assert!(live.contains(&b"typing".to_vec()));
}

#[test]
fn refreshing_an_entry_resets_its_ttl() {
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1);
    let b = hello_auth(&mut r, 2);
    subscribe(&mut r, a, 0, ROOM_A);
    subscribe(&mut r, b, 0, ROOM_A);

    set_awareness(&mut r, a, 0, b"cursor", vec![1]);

    // A refresh before the TTL elapses re-stamps the entry's last-seen time.
    clock.advance(3000);
    set_awareness(&mut r, a, 0, b"cursor", vec![2]);
    r.take_outbox(b);

    // 3000ms after the refresh is still within the TTL — no expiry.
    clock.advance(3000);
    r.sweep();
    assert!(clear_keys(r.take_outbox(b)).is_empty());
    assert!(replayed_keys(&mut r, ROOM_A).contains(&b"cursor".to_vec()));

    // Past the TTL measured from the refresh, it finally expires.
    clock.advance(TTL);
    r.sweep();
    assert_eq!(
        clear_keys(r.take_outbox(b)),
        vec![(actor_of(1), b"cursor".to_vec())],
    );
}

#[test]
fn one_clients_expiry_keeps_a_sibling_of_the_same_actor_present() {
    let (mut r, clock) = registry();
    // Two connections, distinct clients, one actor (same credential) — two tabs.
    let a1 = hello_auth_as(&mut r, 1, b"user");
    let a2 = hello_auth_as(&mut r, 2, b"user");
    let observer = hello_auth(&mut r, 3);
    subscribe(&mut r, a1, 0, ROOM_A);
    subscribe(&mut r, a2, 0, ROOM_A);
    subscribe(&mut r, observer, 0, ROOM_A);

    set_awareness(&mut r, a1, 0, b"cursor", vec![1]);
    set_awareness(&mut r, a2, 0, b"cursor", vec![2]);

    // a1 refreshes within the window; a2 goes silent past the TTL.
    clock.advance(4000);
    set_awareness(&mut r, a1, 0, b"cursor", vec![3]);
    r.take_outbox(observer);
    clock.advance(2000);
    r.sweep();

    // a2's entry expired, but the actor still holds cursor via a1 — so no clear
    // is sent and the presence survives for everyone watching.
    assert!(
        clear_keys(r.take_outbox(observer)).is_empty(),
        "a sibling's live presence must not be cleared",
    );
    assert!(replayed_keys(&mut r, ROOM_A).contains(&b"cursor".to_vec()));
}

#[test]
fn a_read_revoked_peer_is_not_told_of_an_expiry() {
    let (mut r, clock) = registry();
    let (revoked, authorizer) = revocable();
    r.set_authorizer(authorizer);
    let a = hello_auth(&mut r, 1); // actor-1, publisher
    let b = hello_auth(&mut r, 2); // actor-2, revoked mid-session
    subscribe(&mut r, a, 0, ROOM_A);
    subscribe(&mut r, b, 0, ROOM_A);
    set_awareness(&mut r, a, 0, b"cursor", vec![1]);
    r.take_outbox(b);

    // b loses read on the room, then a's entry times out.
    revoked.store(true, Ordering::SeqCst);
    clock.advance(TTL + 1);
    r.sweep();

    // The expiry clear is gated like every other read: b learns nothing.
    assert!(clear_keys(r.take_outbox(b)).is_empty());
}

#[test]
fn with_the_default_policy_nothing_times_out() {
    // A registry left on the default NoTimedTtl policy never expires an entry,
    // however long it stays silent — presence is cleared only on disconnect.
    let mut r = Registry::new(cid(0xFF));
    let clock = Arc::new(ManualClock::new(0));
    r.set_clock(clock.clone());
    let a = hello_auth(&mut r, 1);
    let b = hello_auth(&mut r, 2);
    subscribe(&mut r, a, 0, ROOM_A);
    subscribe(&mut r, b, 0, ROOM_A);
    set_awareness(&mut r, a, 0, b"cursor", vec![1]);
    r.take_outbox(b);

    clock.advance(1_000_000);
    r.sweep();
    assert!(clear_keys(r.take_outbox(b)).is_empty());
    assert!(replayed_keys(&mut r, ROOM_A).contains(&b"cursor".to_vec()));
}
