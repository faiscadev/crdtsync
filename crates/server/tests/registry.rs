//! Registry — the many-connection fan-out over one hub.
//!
//! Each connection has its own session and an outbox of messages waiting to be
//! sent. Delivering an inbound message drives the session (see [`step`]),
//! queues any replies for that connection, and fans a broadcast out to the
//! room's *other* connections. A message that violates the protocol queues an
//! Error and signals close; the caller drains the outbox, then disconnects.
//! Pure, synchronous routing; the async transport pumps bytes through it.

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Scalar};
use crdtsync_server::{ConnId, Registry};

const CH: Channel = Channel(0);

fn ops_msg(ops: Vec<crdtsync_core::Op>) -> Message {
    Message::Ops { channel: CH, ops }
}

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn registry() -> Registry {
    Registry::new(cid(0xFF))
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

const ROOM: &[u8] = b"room-1";

fn sub(room: &[u8]) -> Message {
    Message::Subscribe {
        channel: CH,
        room: room.to_vec(),
        last_seen_seq: 0,
    }
}

/// Say Hello and authenticate (the dev verifier accepts any credential),
/// discarding the AuthOk reply.
fn auth(r: &mut Registry, id: ConnId, client: u8) {
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client)
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"cred".to_vec()
        }
    ));
    r.take_outbox(id);
}

/// Bring a connection up to a subscribed room, discarding the catch-up reply.
fn join(r: &mut Registry, client: u8, room: &[u8]) -> ConnId {
    let id = r.connect();
    auth(r, id, client);
    assert!(r.deliver(id, sub(room)));
    r.take_outbox(id);
    id
}

fn is_violation(m: &Message) -> bool {
    matches!(
        m,
        Message::Error {
            code: ErrorCode::ProtocolViolation,
            ..
        }
    )
}

// --- connections ---

#[test]
fn connections_get_distinct_ids() {
    let mut r = registry();
    assert_ne!(r.connect(), r.connect());
}

#[test]
fn subscribe_queues_the_catch_up_reply() {
    let mut r = registry();
    let a = r.connect();
    auth(&mut r, a, 1);
    r.deliver(a, sub(ROOM));
    assert_eq!(r.take_outbox(a), vec![ops_msg(Vec::new())]);
}

#[test]
fn draining_the_outbox_empties_it() {
    let mut r = registry();
    let a = join(&mut r, 1, ROOM);
    assert!(r.take_outbox(a).is_empty());
}

// --- fan-out ---

#[test]
fn ops_broadcast_to_other_members_but_not_the_sender() {
    let mut r = registry();
    let a = join(&mut r, 1, ROOM);
    let b = join(&mut r, 2, ROOM);

    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let through = ops.iter().map(|o| o.id.seq).max().unwrap();
    r.deliver(a, ops_msg(ops.clone()));

    assert_eq!(r.take_outbox(b), vec![ops_msg(ops)]);
    // The sender gets no op echo — only the acknowledgement of its own batch.
    assert_eq!(
        r.take_outbox(a),
        vec![Message::Accepted {
            channel: CH,
            through
        }]
    );
}

#[test]
fn a_broadcast_skips_other_rooms() {
    let mut r = registry();
    let a = join(&mut r, 1, ROOM);
    let other = join(&mut r, 2, b"room-2");

    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    r.deliver(a, ops_msg(ops));

    assert!(r.take_outbox(other).is_empty());
}

#[test]
fn a_late_joiner_catches_up_on_prior_ops() {
    let mut r = registry();
    let a = join(&mut r, 1, ROOM);
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    r.deliver(a, ops_msg(ops.clone()));

    // A connection that subscribes afterward draws the room's history.
    let b = r.connect();
    auth(&mut r, b, 2);
    r.deliver(b, sub(ROOM));
    assert_eq!(r.take_outbox(b), vec![ops_msg(ops)]);
}

#[test]
fn a_resent_batch_broadcasts_nothing() {
    let mut r = registry();
    let a = join(&mut r, 1, ROOM);
    let b = join(&mut r, 2, ROOM);

    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    r.deliver(a, ops_msg(ops.clone()));
    r.take_outbox(b);
    // The hub dedups the resend, so nothing new fans out.
    r.deliver(a, ops_msg(ops));
    assert!(r.take_outbox(b).is_empty());
}

#[test]
fn a_disconnected_member_stops_receiving() {
    let mut r = registry();
    let a = join(&mut r, 1, ROOM);
    let b = join(&mut r, 2, ROOM);
    r.disconnect(b);

    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    r.deliver(a, ops_msg(ops));
    assert!(r.take_outbox(b).is_empty());
}

// --- violations ---

#[test]
fn a_violation_queues_an_error_and_signals_close() {
    let mut r = registry();
    let c = r.connect();
    // Ops before Hello is a protocol violation.
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let keep_open = r.deliver(c, ops_msg(ops));
    assert!(!keep_open);
    let out = r.take_outbox(c);
    assert!(is_violation(&out[0]), "the error is queued for sending");
}

#[test]
fn delivering_to_an_unknown_connection_signals_close() {
    let mut r = registry();
    let c = r.connect();
    r.disconnect(c);
    assert!(!r.deliver(c, Message::Hello { client: cid(1) }));
}

// --- the upgrade fast path ---

#[test]
fn a_fast_path_connection_subscribes_without_the_auth_phase() {
    let mut r = registry();
    // The credential was verified at the transport upgrade, so the connection
    // opens already authenticated and goes straight from Hello to Subscribe.
    let id = r.connect_authenticated(b"alice".to_vec());
    assert!(r.deliver(id, Message::Hello { client: cid(1) }));
    assert!(r.deliver(id, sub(ROOM)));
    assert_eq!(r.take_outbox(id), vec![ops_msg(Vec::new())]);
}

#[test]
fn a_fast_path_actor_still_fans_out_awareness() {
    let mut r = registry();
    let a = r.connect_authenticated(b"alice".to_vec());
    assert!(r.deliver(a, Message::Hello { client: cid(1) }));
    assert!(r.deliver(a, sub(ROOM)));
    r.take_outbox(a);
    let b = join(&mut r, 2, ROOM);

    assert!(r.deliver(
        a,
        Message::AwarenessSet {
            channel: CH,
            key: b"cursor".to_vec(),
            value: vec![9],
        }
    ));
    // The peer sees the entry tagged with the fast-path actor.
    assert_eq!(
        r.take_outbox(b),
        vec![Message::AwarenessUpdate {
            channel: CH,
            actor: b"alice".to_vec(),
            key: b"cursor".to_vec(),
            value: vec![9],
        }]
    );
}

#[test]
fn verify_credential_reflects_the_verifier() {
    let mut r = registry();
    r.set_verifier(Box::new(|cred: &[u8]| {
        (cred == b"good").then(|| b"alice".to_vec())
    }));
    assert_eq!(r.verify_credential(b"good"), Some(b"alice".to_vec()));
    assert_eq!(r.verify_credential(b"bad"), None);
}
