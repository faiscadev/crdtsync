//! Session — the connection's protocol driver.
//!
//! A session is one client connection. It sequences the protocol: a client
//! must say Hello before anything else, then Subscribe to bind a room (drawing
//! a catch-up batch), then stream Ops that the hub ingests and the server
//! broadcasts to the room's other subscribers. Anything out of order is a
//! protocol violation — the driver replies with an Error and closes. Pure
//! logic over a [`Hub`]; the async transport wraps it.

use crdtsync_core::protocol::PROTOCOL_VERSION;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Scalar};
use crdtsync_server::{negotiate, step, Hub, Session};

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

const ROOM: &[u8] = b"room-1";

/// Drive a session through Hello, asserting it establishes cleanly.
fn hello(hub: &mut Hub, s: &mut Session, client: u8) {
    let r = step(
        hub,
        s,
        Message::Hello {
            client: cid(client),
        },
    );
    assert!(
        r.replies.is_empty() && !r.close,
        "hello should establish quietly"
    );
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

// --- handshake ordering ---

#[test]
fn hello_establishes_the_client() {
    let mut h = hub();
    let mut s = Session::new();
    step(&mut h, &mut s, Message::Hello { client: cid(1) });
    assert_eq!(s.client(), Some(cid(1)));
}

#[test]
fn a_message_before_hello_is_a_violation() {
    let mut h = hub();
    let mut s = Session::new();
    let r = step(
        &mut h,
        &mut s,
        Message::Subscribe {
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert!(r.close);
    assert_eq!(r.replies.len(), 1);
    assert!(is_violation(&r.replies[0]));
}

#[test]
fn a_second_hello_is_a_violation() {
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, 1);
    let r = step(&mut h, &mut s, Message::Hello { client: cid(2) });
    assert!(r.close);
    assert!(is_violation(&r.replies[0]));
}

#[test]
fn an_inbound_error_is_a_violation() {
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, 1);
    let r = step(
        &mut h,
        &mut s,
        Message::Error {
            code: ErrorCode::Internal,
            message: String::new(),
        },
    );
    assert!(r.close);
    assert!(is_violation(&r.replies[0]));
}

// --- subscribe ---

#[test]
fn subscribe_binds_the_room() {
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, 1);
    step(
        &mut h,
        &mut s,
        Message::Subscribe {
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert_eq!(s.room(), Some(ROOM));
}

#[test]
fn subscribe_replies_with_the_catch_up_batch() {
    let mut h = hub();
    // Seed the room with prior ops.
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    h.ingest(ROOM, ops.clone()).unwrap();

    let mut s = Session::new();
    hello(&mut h, &mut s, 2);
    let r = step(
        &mut h,
        &mut s,
        Message::Subscribe {
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert_eq!(r.replies, vec![Message::Ops(ops)]);
    assert!(!r.close);
}

#[test]
fn subscribe_below_a_compaction_floor_replies_with_a_snapshot() {
    let mut h = hub();
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    h.ingest(ROOM, ops).unwrap();
    let head = h.seq(ROOM);
    h.compact(ROOM).unwrap();

    let mut s = Session::new();
    hello(&mut h, &mut s, 2);
    // A subscriber that saw nothing is below the floor: it gets a snapshot.
    let r = step(
        &mut h,
        &mut s,
        Message::Subscribe {
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    );
    match r.replies.as_slice() {
        [Message::Snapshot { seq, state }] => {
            assert_eq!(*seq, head);
            let restored = Document::decode_state(state).unwrap();
            match restored.get(b"age") {
                Some(crdtsync_core::Element::Register(reg)) => {
                    assert_eq!(reg.borrow().read(), &Scalar::Int(30))
                }
                _ => panic!("expected the register in the snapshot"),
            }
        }
        other => panic!("expected a single snapshot reply, got {other:?}"),
    }
    assert!(!r.close);
}

#[test]
fn subscribe_at_the_head_of_a_compacted_room_replies_with_an_empty_batch() {
    let mut h = hub();
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    h.ingest(ROOM, ops).unwrap();
    let head = h.seq(ROOM);
    h.compact(ROOM).unwrap();

    let mut s = Session::new();
    hello(&mut h, &mut s, 2);
    let r = step(
        &mut h,
        &mut s,
        Message::Subscribe {
            room: ROOM.to_vec(),
            last_seen_seq: head,
        },
    );
    assert_eq!(r.replies, vec![Message::Ops(Vec::new())]);
}

#[test]
fn a_client_sending_a_snapshot_is_a_violation() {
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, 1);
    let r = step(
        &mut h,
        &mut s,
        Message::Snapshot {
            seq: 1,
            state: Vec::new(),
        },
    );
    assert!(r.replies.iter().any(is_violation) && r.close);
}

#[test]
fn subscribe_on_a_fresh_room_replies_with_an_empty_batch() {
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, 1);
    let r = step(
        &mut h,
        &mut s,
        Message::Subscribe {
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert_eq!(r.replies, vec![Message::Ops(Vec::new())]);
}

#[test]
fn resubscribing_switches_the_room() {
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, 1);
    let sub = |room: &[u8]| Message::Subscribe {
        room: room.to_vec(),
        last_seen_seq: 0,
    };
    step(&mut h, &mut s, sub(b"room-a"));
    step(&mut h, &mut s, sub(b"room-b"));
    assert_eq!(s.room(), Some(&b"room-b"[..]));
}

// --- ops ingest + broadcast ---

#[test]
fn ops_after_subscribe_ingest_and_broadcast() {
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, 1);
    step(
        &mut h,
        &mut s,
        Message::Subscribe {
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    );
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let r = step(&mut h, &mut s, Message::Ops(ops.clone()));
    // Applied ops fan out to the room's other subscribers; nothing echoes back.
    assert_eq!(r.broadcast, ops);
    assert!(r.replies.is_empty() && !r.close);
    assert_eq!(h.seq(ROOM), 1);
}

#[test]
fn ops_before_subscribe_is_a_violation() {
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, 1);
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let r = step(&mut h, &mut s, Message::Ops(ops));
    assert!(r.close);
    assert!(is_violation(&r.replies[0]));
}

#[test]
fn a_resent_op_batch_broadcasts_nothing() {
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, 1);
    step(
        &mut h,
        &mut s,
        Message::Subscribe {
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    );
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    step(&mut h, &mut s, Message::Ops(ops.clone()));
    // A reconnect resends: the hub dedups, so there is nothing new to fan out.
    let r = step(&mut h, &mut s, Message::Ops(ops));
    assert!(r.broadcast.is_empty());
    assert_eq!(h.seq(ROOM), 1);
}

#[test]
fn ops_stamped_by_another_client_are_a_violation() {
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, 1);
    step(
        &mut h,
        &mut s,
        Message::Subscribe {
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        },
    );
    // The session belongs to client 1; ops minted by client 2 assert a foreign
    // identity and must be refused, not ingested.
    let foreign = doc(2).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let r = step(&mut h, &mut s, Message::Ops(foreign));
    assert!(r.close);
    assert!(is_violation(&r.replies[0]));
    assert_eq!(h.seq(ROOM), 0);
}

// --- version negotiation ---

#[test]
fn the_current_version_negotiates() {
    assert!(negotiate(PROTOCOL_VERSION).is_ok());
}

#[test]
fn a_foreign_version_is_refused() {
    let err = negotiate(PROTOCOL_VERSION + 1).unwrap_err();
    assert!(matches!(
        err,
        Message::Error {
            code: ErrorCode::UnsupportedVersion,
            ..
        }
    ));
}
