//! Client session — the replica's side of the wire protocol.
//!
//! A [`ClientSession`] mirrors the server's session driver from the client's
//! seat. It opens with Hello, joins a room with Subscribe carrying the last
//! sequence it saw, folds the server's catch-up — an op delta or a whole-replica
//! snapshot — into a local replica, and tracks how far it has caught up so a
//! reconnect resumes from there instead of replaying from zero.
//!
//! Sequence tracking is by count: the server's deltas and broadcasts are
//! contiguous runs of ops at the head, each op holding one server sequence, so a
//! batch of `n` ops advances the seen sequence by `n`. A redelivered op — the
//! client's own write echoed back on reconnect, or a resend — still advances the
//! sequence (it occupies a server slot) but does not re-apply, because the
//! replica deduplicates.

use crdtsync_core::client::{ClientError, ClientSession};
use crdtsync_core::{ClientId, Document, Element, ErrorCode, Message, Op, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A stand-in for the server's authoritative replica, the source of the ops and
/// snapshots a client catches up from.
fn srv() -> Document {
    Document::new(cid(0xFF))
}

/// Read a top-level Register's int, panicking on any other shape.
fn int(e: Option<Element>) -> i64 {
    match e {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected Int, got {other:?}"),
        },
        _ => panic!("expected a Register"),
    }
}

/// Read a top-level Counter's value.
fn counter(e: Option<Element>) -> i64 {
    match e {
        Some(Element::Counter(c)) => c.borrow().read(),
        _ => panic!("expected a Counter"),
    }
}

/// Unwrap the ops of a `Message::Ops`.
fn ops_of(m: Message) -> Vec<Op> {
    match m {
        Message::Ops(ops) => ops,
        other => panic!("expected Ops, got {other:?}"),
    }
}

const ROOM: &[u8] = b"room-1";

// --- handshake framing ---

#[test]
fn hello_names_the_client() {
    let session = ClientSession::new(cid(1));
    match session.hello() {
        Message::Hello { client } => assert_eq!(client, cid(1)),
        other => panic!("expected Hello, got {other:?}"),
    }
}

#[test]
fn subscribe_binds_the_room_and_requests_from_zero() {
    let mut session = ClientSession::new(cid(1));
    match session.subscribe(ROOM) {
        Message::Subscribe {
            room,
            last_seen_seq,
        } => {
            assert_eq!(room, ROOM);
            // A fresh client has caught up to nothing.
            assert_eq!(last_seen_seq, 0);
        }
        other => panic!("expected Subscribe, got {other:?}"),
    }
    assert_eq!(session.room(), Some(ROOM));
}

// --- catch-up ---

#[test]
fn an_ops_catch_up_converges_the_replica_and_tracks_the_sequence() {
    // The server folds two ops into its replica; the client subscribes from zero
    // and is handed them as a delta.
    let mut srv = srv();
    let mut delta = srv.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    delta.extend(srv.transact(|tx| tx.register(b"b", Scalar::Int(2))));

    let mut session = ClientSession::new(cid(2));
    session.subscribe(ROOM);
    session.receive(Message::Ops(delta)).unwrap();

    assert_eq!(int(session.document().get(b"a")), 1);
    assert_eq!(int(session.document().get(b"b")), 2);
    // Two ops caught up: the seen sequence is the server's head.
    assert_eq!(session.last_seen_seq(), 2);
}

#[test]
fn a_snapshot_catch_up_rebuilds_the_replica_and_adopts_the_sequence() {
    // A client below the compaction floor is served the whole replica as a
    // snapshot tagged with the sequence it lands at, not a delta.
    let mut srv = srv();
    srv.transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.inc(b"n", 5);
    });

    let mut session = ClientSession::new(cid(2));
    session.subscribe(ROOM);
    session
        .receive(Message::Snapshot {
            seq: 9,
            state: srv.encode_state(),
        })
        .unwrap();

    assert_eq!(int(session.document().get(b"a")), 1);
    assert_eq!(counter(session.document().get(b"n")), 5);
    // The snapshot's tagged sequence is adopted wholesale.
    assert_eq!(session.last_seen_seq(), 9);
}

#[test]
fn live_ops_advance_the_replica_and_the_sequence() {
    let mut srv = srv();
    let first = srv.transact(|tx| tx.register(b"a", Scalar::Int(1)));

    let mut session = ClientSession::new(cid(2));
    session.subscribe(ROOM);
    session.receive(Message::Ops(first)).unwrap();
    assert_eq!(session.last_seen_seq(), 1);

    // A later broadcast lands on top and carries the sequence forward.
    let later = srv.transact(|tx| tx.register(b"b", Scalar::Int(2)));
    session.receive(Message::Ops(later)).unwrap();
    assert_eq!(int(session.document().get(b"b")), 2);
    assert_eq!(session.last_seen_seq(), 2);
}

// --- reconnect ---

#[test]
fn reconnect_subscribes_from_the_last_seen_sequence() {
    let mut srv = srv();
    let mut delta = srv.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    delta.extend(srv.transact(|tx| tx.register(b"b", Scalar::Int(2))));

    let mut session = ClientSession::new(cid(2));
    session.subscribe(ROOM);
    session.receive(Message::Ops(delta)).unwrap();

    // Reconnecting, the client asks only for what it missed past its position —
    // the server can answer with a small delta instead of the whole log.
    match session.subscribe(ROOM) {
        Message::Subscribe { last_seen_seq, .. } => assert_eq!(last_seen_seq, 2),
        other => panic!("expected Subscribe, got {other:?}"),
    }
}

#[test]
fn a_redelivered_op_is_idempotent_but_still_advances_the_sequence() {
    let mut srv = srv();
    let op = srv.transact(|tx| tx.register(b"a", Scalar::Int(1)));

    let mut session = ClientSession::new(cid(2));
    session.subscribe(ROOM);
    session.receive(Message::Ops(op.clone())).unwrap();
    assert_eq!(session.last_seen_seq(), 1);

    // The same op redelivered (a resend, or the client's own write echoed on a
    // reconnect) does not re-apply, but it still holds a server sequence.
    session.receive(Message::Ops(op)).unwrap();
    assert_eq!(int(session.document().get(b"a")), 1);
    assert_eq!(session.last_seen_seq(), 2);
}

// --- local edits ---

#[test]
fn a_local_edit_yields_ops_to_send_without_advancing_the_seen_sequence() {
    let mut session = ClientSession::new(cid(1));
    session.subscribe(ROOM);

    // A local edit applies to the replica and returns the ops to broadcast; the
    // seen sequence is the server's, so an unacknowledged local write leaves it
    // untouched.
    let outbound = session.edit(|tx| tx.register(b"a", Scalar::Int(7)));
    assert_eq!(int(session.document().get(b"a")), 7);
    assert_eq!(session.last_seen_seq(), 0);
    assert_eq!(ops_of(outbound).len(), 1);
}

// --- snapshot over prior state ---

#[test]
fn a_snapshot_replaces_prior_local_state_with_the_server_state() {
    // The client has one write; the server's snapshot reflects a different, fuller
    // replica. Adopting the snapshot yields the server's state, not a merge onto
    // stale local slots.
    let mut srv = srv();
    srv.transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.register(b"b", Scalar::Int(2));
    });

    let mut peer = Document::new(cid(3));
    let mut session = ClientSession::new(cid(2));
    session.subscribe(ROOM);
    session
        .receive(Message::Ops(
            peer.transact(|tx| tx.register(b"a", Scalar::Int(99))),
        ))
        .unwrap();

    session
        .receive(Message::Snapshot {
            seq: 2,
            state: srv.encode_state(),
        })
        .unwrap();
    assert_eq!(int(session.document().get(b"a")), 1);
    assert_eq!(int(session.document().get(b"b")), 2);
    assert_eq!(session.last_seen_seq(), 2);
}

#[test]
fn edits_after_a_snapshot_still_carry_the_clients_own_id() {
    // A snapshot is authored by the server's replica, so adopting it must not
    // reseat the client's identity: a later local write is still this client's.
    let mut srv = srv();
    srv.transact(|tx| tx.register(b"a", Scalar::Int(1)));

    let mut session = ClientSession::new(cid(2));
    session.subscribe(ROOM);
    session
        .receive(Message::Snapshot {
            seq: 1,
            state: srv.encode_state(),
        })
        .unwrap();

    let outbound = session.edit(|tx| tx.register(b"b", Scalar::Int(2)));
    assert!(ops_of(outbound).iter().all(|op| op.id.client == cid(2)));
}

// --- malformed / illegal server frames ---

#[test]
fn a_garbage_snapshot_is_rejected_and_leaves_the_replica_intact() {
    let mut srv = srv();
    let op = srv.transact(|tx| tx.register(b"a", Scalar::Int(1)));

    let mut session = ClientSession::new(cid(2));
    session.subscribe(ROOM);
    session.receive(Message::Ops(op)).unwrap();

    let err = session.receive(Message::Snapshot {
        seq: 5,
        state: vec![0xFF, 0xFF, 0xFF, 0xFF],
    });
    assert!(matches!(err, Err(ClientError::BadSnapshot)));
    // The rejected snapshot changed nothing.
    assert_eq!(int(session.document().get(b"a")), 1);
    assert_eq!(session.last_seen_seq(), 1);
}

#[test]
fn a_server_error_surfaces_to_the_caller() {
    let mut session = ClientSession::new(cid(1));
    let err = session.receive(Message::Error {
        code: ErrorCode::UnknownRoom,
        message: "no such room".to_string(),
    });
    match err {
        Err(ClientError::Server { code, message }) => {
            assert_eq!(code, ErrorCode::UnknownRoom);
            assert_eq!(message, "no such room");
        }
        other => panic!("expected a surfaced server error, got {other:?}"),
    }
}

#[test]
fn a_client_only_message_from_the_server_is_a_violation() {
    let mut session = ClientSession::new(cid(1));
    // Hello and Subscribe travel client to server; seeing one inbound is a
    // protocol violation, not something to fold in.
    assert!(matches!(
        session.receive(Message::Hello { client: cid(2) }),
        Err(ClientError::UnexpectedMessage(_))
    ));
    assert!(matches!(
        session.receive(Message::Subscribe {
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        }),
        Err(ClientError::UnexpectedMessage(_))
    ));
}
