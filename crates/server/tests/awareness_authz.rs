//! Awareness fan-out is gated on the recipient's read authority.
//!
//! Presence (cursors, selections, typing) fans out to a room's peers, and
//! seeing a peer's presence is a read of the room — so every awareness delivery
//! passes the same per-recipient read gate the op-redaction fan-out uses. A peer
//! that may not read the room receives no presence for it: it cannot subscribe
//! (so it is never a fan-out recipient and replays nothing on join), and a peer
//! whose read is revoked mid-session stops receiving further sets and clears at
//! once. The gate is symmetric — an unauthorized peer neither learns others'
//! presence nor is a recipient its own would leak to. Room-level today; the same
//! hook narrows to zone/branch scope once that enforcement seam lands.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, ErrorCode, Message};
use crdtsync_server::acl::{Acl, ResourceMatch, Subject};
use crdtsync_server::{
    Action, Authorizer, AwarenessPolicy, ConnId, Identity, ManualClock, Registry, Resource,
};

const ROOM_A: &[u8] = b"room-a";
const TTL: u64 = 5000;
const GRACE: u64 = 5000;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// The dev verifier adopts the credential as the actor, so client `n` acts as
/// `actor-n`.
fn actor_of(client: u8) -> Vec<u8> {
    format!("actor-{client}").into_bytes()
}

fn registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    // An awareness set stamps last-seen off the clock, unreadable under Miri's
    // isolation — drive a fixed manual clock.
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
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

fn subscribe(r: &mut Registry, id: ConnId, channel: u32, room: &[u8]) -> Vec<Message> {
    r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(channel),
            room: room.to_vec(),
            last_seen_seq: 0,
            branch: Vec::new(),
        },
    );
    r.take_outbox(id)
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

fn is_forbidden(m: &Message) -> bool {
    matches!(
        m,
        Message::Error {
            code: ErrorCode::Forbidden,
            ..
        }
    )
}

fn awareness_updates(msgs: Vec<Message>) -> Vec<Message> {
    msgs.into_iter()
        .filter(|m| matches!(m, Message::AwarenessUpdate { .. }))
        .collect()
}

/// An ACL granting full authority to the listed actors on every room and
/// nothing to anyone else — an unlisted actor is default-denied read.
fn allow_only(actors: &[u8]) -> Box<dyn Authorizer> {
    let mut acl = Acl::new();
    for &client in actors {
        acl = acl.allow(
            Subject::Actor(actor_of(client)),
            None,
            ResourceMatch::AnyRoom,
        );
    }
    Box::new(acl)
}

/// A per-kind TTL policy backed by a fixed map — a kind absent from the map is
/// session-lifetime (no timed TTL).
struct TtlMap(HashMap<Vec<u8>, u64>);

impl AwarenessPolicy for TtlMap {
    fn ttl(&self, _room: &[u8], key: &[u8]) -> Option<u64> {
        self.0.get(key).copied()
    }
}

fn cursor_ttl_policy() -> Arc<dyn AwarenessPolicy> {
    let mut ttls = HashMap::new();
    ttls.insert(b"cursor".to_vec(), TTL);
    Arc::new(TtlMap(ttls))
}

/// An authorizer that denies `actor-2` reads once its flag is set, permitting
/// everything else — a peer subscribes while allowed, then loses the room.
fn revocable() -> (Arc<AtomicBool>, Box<dyn Authorizer>) {
    let revoked = Arc::new(AtomicBool::new(false));
    let flag = revoked.clone();
    let authorizer: Box<dyn Authorizer> =
        Box::new(move |id: &Identity, action: Action, _res: &Resource| {
            !(action == Action::Read && id.actor() == b"actor-2" && flag.load(Ordering::SeqCst))
        });
    (revoked, authorizer)
}

/// An unauthorized reader cannot subscribe, so a publisher's set reaches the
/// authorized peer and never the denied one.
#[test]
fn an_unauthorized_reader_is_refused_and_receives_no_awareness() {
    let mut r = registry();
    // A (1) and C (3) may read; B (2) may not.
    r.set_authorizer(allow_only(&[1, 3]));

    let a = hello_auth(&mut r, 1);
    let b = hello_auth(&mut r, 2);
    let c = hello_auth(&mut r, 3);
    subscribe(&mut r, a, 0, ROOM_A);
    let b_reply = subscribe(&mut r, b, 0, ROOM_A);
    subscribe(&mut r, c, 0, ROOM_A);

    assert!(
        matches!(b_reply.as_slice(), [m] if is_forbidden(m)),
        "the unauthorized reader's subscribe is refused"
    );

    set_awareness(&mut r, a, 0, b"cursor", vec![1, 2, 3]);

    assert_eq!(
        awareness_updates(r.take_outbox(c)),
        vec![Message::AwarenessUpdate {
            channel: Channel(0),
            actor: actor_of(1),
            key: b"cursor".to_vec(),
            value: vec![1, 2, 3],
        }],
        "the authorized peer receives the presence"
    );
    assert!(
        r.take_outbox(b).is_empty(),
        "the unauthorized peer receives no presence"
    );
}

/// A joiner that may not read replays no presence; an authorized joiner is
/// replayed the room's live entries.
#[test]
fn a_denied_joiner_replays_no_presence() {
    let mut r = registry();
    r.set_authorizer(allow_only(&[1, 3]));

    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 0, ROOM_A);
    set_awareness(&mut r, a, 0, b"cursor", vec![5]);

    let b = hello_auth(&mut r, 2);
    let b_reply = subscribe(&mut r, b, 0, ROOM_A);
    assert!(
        matches!(b_reply.as_slice(), [m] if is_forbidden(m)),
        "the denied joiner is refused"
    );
    assert!(
        awareness_updates(b_reply).is_empty(),
        "a refused joiner replays no presence"
    );

    let c = hello_auth(&mut r, 3);
    assert_eq!(
        awareness_updates(subscribe(&mut r, c, 0, ROOM_A)),
        vec![Message::AwarenessUpdate {
            channel: Channel(0),
            actor: actor_of(1),
            key: b"cursor".to_vec(),
            value: vec![5],
        }],
        "an authorized joiner is replayed the live presence"
    );
}

/// A grace-window actor clear is withheld from a peer whose read was revoked and
/// still delivered to an authorized one.
#[test]
fn an_actor_clear_is_withheld_from_a_revoked_reader() {
    let (revoked, authorizer) = revocable();
    let mut r = registry();
    let clock = Arc::new(ManualClock::new(0));
    r.set_clock(clock.clone());
    r.set_grace_millis(GRACE);
    r.set_authorizer(authorizer);

    let a = hello_auth(&mut r, 1);
    let b = hello_auth(&mut r, 2);
    let c = hello_auth(&mut r, 3);
    subscribe(&mut r, a, 0, ROOM_A);
    subscribe(&mut r, b, 0, ROOM_A);
    subscribe(&mut r, c, 0, ROOM_A);
    set_awareness(&mut r, a, 0, b"cursor", vec![1]);
    r.take_outbox(b);
    r.take_outbox(c);

    // B loses its read, then A departs — the grace sweep clears A's presence.
    revoked.store(true, Ordering::SeqCst);
    r.disconnect(a);
    clock.advance(GRACE);
    r.sweep();

    assert_eq!(
        r.take_outbox(c),
        vec![Message::AwarenessClear {
            channel: Channel(0),
            actor: actor_of(1),
        }],
        "the authorized peer is told of the departure"
    );
    assert!(
        r.take_outbox(b).is_empty(),
        "the revoked reader is not told of the departure"
    );
}

/// A per-key TTL clear is withheld from a peer whose read was revoked and still
/// delivered to an authorized one.
#[test]
fn a_key_clear_is_withheld_from_a_revoked_reader() {
    let (revoked, authorizer) = revocable();
    let mut r = registry();
    let clock = Arc::new(ManualClock::new(0));
    r.set_clock(clock.clone());
    r.set_awareness_policy(cursor_ttl_policy());
    r.set_authorizer(authorizer);

    let a = hello_auth(&mut r, 1);
    let b = hello_auth(&mut r, 2);
    let c = hello_auth(&mut r, 3);
    subscribe(&mut r, a, 0, ROOM_A);
    subscribe(&mut r, b, 0, ROOM_A);
    subscribe(&mut r, c, 0, ROOM_A);
    set_awareness(&mut r, a, 0, b"cursor", vec![1]);
    r.take_outbox(b);
    r.take_outbox(c);

    // B loses its read, then the cursor entry ages out — a per-key clear fans.
    revoked.store(true, Ordering::SeqCst);
    clock.advance(TTL + 1);
    r.sweep();

    assert_eq!(
        r.take_outbox(c),
        vec![Message::AwarenessClearKey {
            channel: Channel(0),
            actor: actor_of(1),
            key: b"cursor".to_vec(),
        }],
        "the authorized peer is told which entry expired"
    );
    assert!(
        r.take_outbox(b).is_empty(),
        "the revoked reader is told of no expiry"
    );
}

/// The default (no injected ACL — dev `PermitAll`) fans presence to every room
/// peer, so the read gate does not change the un-authorized deployment behavior.
#[test]
fn the_permissive_default_fans_to_every_peer() {
    let mut r = registry();
    let a = hello_auth(&mut r, 1);
    let b = hello_auth(&mut r, 2);
    subscribe(&mut r, a, 0, ROOM_A);
    subscribe(&mut r, b, 0, ROOM_A);

    set_awareness(&mut r, a, 0, b"cursor", vec![9]);

    assert_eq!(
        awareness_updates(r.take_outbox(b)),
        vec![Message::AwarenessUpdate {
            channel: Channel(0),
            actor: actor_of(1),
            key: b"cursor".to_vec(),
            value: vec![9],
        }],
        "every room peer gets presence under the permissive default"
    );
}
