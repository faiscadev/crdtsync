//! Request routing — a node redirects a room it does not lead to the leader.
//!
//! A room's leader is its placement primary (pre-election). When the node holds a
//! [`Membership`] and is *not* a room's primary, a Subscribe (or a write that
//! reaches it) is answered with `Redirect { room, leader_addr }` — the primary's
//! advertise address — instead of a catch-up or an ingest: a follower does not
//! serve the room directly, the client reconnects to the leader. When the node
//! *is* the primary, or runs single-node (no membership), it serves the room
//! exactly as before — the byte-identical behavior regression.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Scalar};
use crdtsync_server::membership::Membership;
use crdtsync_server::{
    step, AllowAll, Hub, ManualClock, PermitAll, Registry, Response, SchemaRegistry, Session,
};

const CH: Channel = Channel(0);
const N: usize = 3;
const SELF_ADDR: &str = "10.0.0.99:9000";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn hub() -> Hub {
    Hub::new(cid(0xFF))
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

/// A multi-node membership whose self is `SELF_ADDR`, so some rooms place their
/// primary on a peer and others on self.
fn membership() -> Membership {
    let peers = (0..6)
        .map(|i| format!("10.0.0.{i}:9000"))
        .collect::<Vec<_>>()
        .join(",");
    Membership::from_static_config(None, Some(SELF_ADDR), &peers, N).unwrap()
}

/// A room whose primary is (or is not) self under `m`.
fn find_room(m: &Membership, led_by_self: bool) -> Vec<u8> {
    for i in 0..100_000 {
        let room = format!("room-{i}").into_bytes();
        if m.is_primary_for(&room) == led_by_self {
            return room;
        }
    }
    panic!("no room found with led_by_self={led_by_self}");
}

fn sub(room: &[u8]) -> Message {
    Message::Subscribe {
        channel: CH,
        room: room.to_vec(),
        branch: Vec::new(),
        last_seen_seq: 0,
    }
}

/// Drive one message through `step` with the dev verifier / permit-all
/// authorizer, under the given membership view.
fn st(h: &mut Hub, s: &mut Session, m: Option<&Membership>, msg: Message) -> Response {
    step(
        h,
        s,
        &AllowAll,
        &PermitAll,
        None,
        &Mutex::new(SchemaRegistry::new()),
        None,
        m,
        0,
        None,
        msg,
    )
}

/// Hello + Auth, so the session is ready to subscribe.
fn handshake(h: &mut Hub, s: &mut Session, client: u8) {
    st(
        h,
        s,
        None,
        Message::Hello {
            client: cid(client),
            app_id: Vec::new(),
            schema_version: 0,
        },
    );
    st(
        h,
        s,
        None,
        Message::Auth {
            credential: b"cred".to_vec(),
        },
    );
}

// --- subscribe redirects off the leader ---

#[test]
fn subscribe_to_a_non_led_room_is_redirected() {
    let m = membership();
    let room = find_room(&m, false);
    let leader = m.primary_for(&room).unwrap();
    let mut h = hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, 1);
    let r = st(&mut h, &mut s, Some(&m), sub(&room));
    assert!(!r.close, "a redirect keeps the connection open");
    assert_eq!(
        r.replies,
        vec![Message::Redirect {
            room: room.clone(),
            leader_addr: leader.as_bytes().to_vec(),
        }],
        "the reply names the room's leader, and nothing else",
    );
    assert!(
        r.broadcast.is_empty() && r.broadcast_room.is_none(),
        "no catch-up, no fan-out"
    );
}

#[test]
fn a_redirected_subscribe_binds_no_channel() {
    // The subscribe was declined, so the channel never bound — a follow-up frame
    // on it is an unbound-channel violation, proving no subscription took.
    let m = membership();
    let room = find_room(&m, false);
    let mut h = hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, 1);
    st(&mut h, &mut s, Some(&m), sub(&room));
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let r = st(&mut h, &mut s, Some(&m), Message::Ops { channel: CH, ops });
    assert!(r.close, "ops on the unbound channel is a violation");
    assert!(matches!(
        r.replies.first(),
        Some(Message::Error {
            code: ErrorCode::ProtocolViolation,
            ..
        })
    ));
}

#[test]
fn subscribe_to_a_self_led_room_catches_up_normally() {
    let m = membership();
    let room = find_room(&m, true);
    let mut h = hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, 1);
    let r = st(&mut h, &mut s, Some(&m), sub(&room));
    assert!(!r.close);
    // The room self leads catches up as before — an op delta, never a redirect.
    assert!(
        matches!(r.replies.first(), Some(Message::Ops { channel, .. }) if *channel == CH),
        "a self-led subscribe is served a catch-up, got {:?}",
        r.replies,
    );
}

// --- single-node / no membership never redirects (regression) ---

#[test]
fn single_node_never_redirects() {
    // No membership at all: every room is local, exactly today's behavior.
    let mut h = hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, 1);
    let r = st(&mut h, &mut s, None, sub(b"any-room"));
    assert!(!r.close);
    assert!(
        matches!(r.replies.first(), Some(Message::Ops { .. })),
        "no membership serves the room, never a redirect",
    );
}

#[test]
fn a_solo_membership_leads_every_room() {
    // A single-member membership (self only) is primary for every room, so it
    // serves them all — a redirect never fires.
    let m = Membership::from_static_config(None, Some(SELF_ADDR), "", N).unwrap();
    for i in 0..200 {
        let room = format!("solo-{i}").into_bytes();
        let mut h = hub();
        let mut s = Session::new();
        handshake(&mut h, &mut s, 1);
        let r = st(&mut h, &mut s, Some(&m), sub(&room));
        assert!(
            matches!(r.replies.first(), Some(Message::Ops { .. })),
            "self leads every room in a solo membership",
        );
    }
}

// --- a write reaching a non-leader is redirected, not ingested ---

#[test]
fn ops_reaching_a_non_leader_is_redirected_not_ingested() {
    // The channel binds while self still leads (single-node subscribe); a later
    // membership makes self a follower — the write is redirected, never folded.
    let m = membership();
    let room = find_room(&m, false);
    let leader = m.primary_for(&room).unwrap();
    let mut h = hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, 1);
    // Bind the channel with no membership, so the subscribe takes.
    st(&mut h, &mut s, None, sub(&room));
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let r = st(
        &mut h,
        &mut s,
        Some(&m),
        Message::Ops {
            channel: CH,
            ops: ops.clone(),
        },
    );
    assert!(!r.close, "a redirected write keeps the connection open");
    assert_eq!(
        r.replies,
        vec![Message::Redirect {
            room: room.clone(),
            leader_addr: leader.as_bytes().to_vec(),
        }],
    );
    assert!(r.broadcast.is_empty(), "the write is not fanned out");
    assert_eq!(h.seq(&room), 0, "the write is not ingested");
}

#[test]
fn ops_on_a_self_led_room_ingests() {
    let m = membership();
    let room = find_room(&m, true);
    let mut h = hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, 1);
    st(&mut h, &mut s, Some(&m), sub(&room));
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let r = st(
        &mut h,
        &mut s,
        Some(&m),
        Message::Ops {
            channel: CH,
            ops: ops.clone(),
        },
    );
    assert_eq!(r.broadcast, ops, "the leader ingests and fans out");
    assert_eq!(h.seq(&room), 1);
}

// --- a durable version write reaching a non-leader is redirected, not persisted ---

#[test]
fn version_create_reaching_a_non_leader_is_redirected_not_persisted() {
    // A version mutation persists to the room, so a follower must not serve it:
    // the channel binds while self leads (no membership), a later membership makes
    // self a follower, and the create is redirected rather than persisted.
    let m = membership();
    let room = find_room(&m, false);
    let leader = m.primary_for(&room).unwrap();
    let mut h = hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, 1);
    // Seed a version while self leads, then bind the channel.
    st(&mut h, &mut s, None, sub(&room));
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    st(&mut h, &mut s, None, Message::Ops { channel: CH, ops });
    let before = h.version_names(&room);
    let r = st(
        &mut h,
        &mut s,
        Some(&m),
        Message::VersionCreate {
            channel: CH,
            name: b"v1".to_vec(),
        },
    );
    assert!(!r.close);
    assert_eq!(
        r.replies,
        vec![Message::Redirect {
            room: room.clone(),
            leader_addr: leader.as_bytes().to_vec(),
        }],
    );
    assert_eq!(
        h.version_names(&room),
        before,
        "the version is not persisted on a follower",
    );
}

// --- direction guard: a client never sends a redirect ---

#[test]
fn a_client_sent_redirect_is_a_violation() {
    let mut h = hub();
    let mut s = Session::new();
    handshake(&mut h, &mut s, 1);
    let r = st(
        &mut h,
        &mut s,
        Some(&membership()),
        Message::Redirect {
            room: b"room".to_vec(),
            leader_addr: b"node".to_vec(),
        },
    );
    assert!(r.close);
    assert!(matches!(
        r.replies.first(),
        Some(Message::Error {
            code: ErrorCode::ProtocolViolation,
            ..
        })
    ));
}

// --- registry wiring: set_membership routes the redirect through deliver ---

#[test]
fn registry_redirects_a_non_led_subscribe() {
    let m = membership();
    let room = find_room(&m, false);
    let leader = m.primary_for(&room).unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(m);
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"cred".to_vec()
        }
    ));
    r.take_outbox(id);
    assert!(
        r.deliver(id, sub(&room)),
        "a redirect keeps the connection open"
    );
    assert_eq!(
        r.take_outbox(id),
        vec![Message::Redirect {
            room,
            leader_addr: leader.as_bytes().to_vec(),
        }],
    );
}

#[test]
fn registry_without_membership_serves_locally() {
    // No set_membership: single-node, every room served — the regression that a
    // plain deployment is unchanged.
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"cred".to_vec()
        }
    ));
    r.take_outbox(id);
    assert!(r.deliver(id, sub(b"any-room")));
    let out = r.take_outbox(id);
    assert!(
        matches!(out.first(), Some(Message::Ops { .. })),
        "a membership-less registry serves the room, never a redirect",
    );
}
