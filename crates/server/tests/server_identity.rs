//! A node's replica identity is the node's alone — no connection may declare it.
//!
//! Every room a node serves is held as a [`Document`] authored under one node-wide
//! [`ClientId`], and in production that id is the fixed all-zero constant — public,
//! and guessable by anyone. Channel 0 authors under the id a connection declares at
//! Hello unchanged, so without a gate a client can declare the node's own identity
//! and push ops into the room's log carrying it.
//!
//! That is the one identity in the system an attacker does not have to guess 122
//! random bits for, and the node's replica is the amplifier behind it: its
//! `encode_state` rides every catch-up snapshot the node serves and every
//! compaction it writes, so anything a frame can install in that replica reaches
//! every client that later joins. The declared id is the single chokepoint —
//! `for_channel` of any other id never lands on it — so the handshake refuses it
//! and the connection closes before it can bind a channel.

use crdtsync_core::client::ClientSession;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Op, Scalar};
use crdtsync_server::{ConnId, Registry};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// The node identity `crdtsync-server`'s binary serves under.
fn production_node_id() -> ClientId {
    ClientId::from_bytes([0; 16])
}

const ROOM: &[u8] = b"room-a";

fn is_violation(m: &Message) -> bool {
    matches!(
        m,
        Message::Error {
            code: ErrorCode::ProtocolViolation,
            ..
        }
    )
}

fn hello_msg(client: ClientId) -> Message {
    Message::Hello {
        client,
        app_id: Vec::new(),
        schema_version: 0,
        codecs: Vec::new(),
    }
}

/// A connected, authenticated connection declaring `client`.
fn hello(r: &mut Registry, client: ClientId) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(id, hello_msg(client)));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"cred".to_vec()
        }
    ));
    r.take_outbox(id);
    id
}

fn subscribe(r: &mut Registry, id: ConnId, channel: Channel, room: &[u8], last_seen_seq: u64) {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel,
            room: room.to_vec(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq,
        }
    ));
}

/// Every op the connection's outbox carried, flattened out of its `Ops` frames.
fn received_ops(msgs: Vec<Message>) -> Vec<Op> {
    msgs.into_iter()
        .flat_map(|m| match m {
            Message::Ops { ops, .. } => ops,
            _ => Vec::new(),
        })
        .collect()
}

// --- the handshake gate ---

#[test]
fn a_hello_declaring_the_nodes_replica_identity_is_refused() {
    let mut r = Registry::new(cid(0xFF));
    let conn = r.connect();
    assert!(
        !r.deliver(conn, hello_msg(cid(0xFF))),
        "the connection stayed open"
    );
    assert!(r.take_outbox(conn).iter().any(is_violation));
}

#[test]
fn the_production_node_identity_is_refused() {
    // The shipped binary serves under the all-zero id — the guessable case the
    // gate exists for, pinned so a change to that constant cannot quietly reopen it.
    let mut r = Registry::new(production_node_id());
    let conn = r.connect();
    assert!(!r.deliver(conn, hello_msg(production_node_id())));
    assert!(r.take_outbox(conn).iter().any(is_violation));
}

#[test]
fn a_hello_under_any_other_identity_is_accepted() {
    let mut r = Registry::new(production_node_id());
    let conn = r.connect();
    assert!(r.deliver(conn, hello_msg(cid(1))));
    assert!(!r.take_outbox(conn).iter().any(is_violation));
}

#[test]
fn a_refused_connection_binds_no_channel_and_writes_no_op() {
    let mut r = Registry::new(cid(0xFF));
    let attacker = r.connect();
    r.deliver(attacker, hello_msg(cid(0xFF)));
    r.take_outbox(attacker);

    // With no Hello settled the connection can neither subscribe nor write, so
    // nothing authored under the node's identity reaches the room.
    r.deliver(
        attacker,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq: 0,
        },
    );
    let forged = Document::new(cid(0xFF)).transact(|tx| tx.set(b"x", Scalar::Int(1)));
    r.deliver(
        attacker,
        Message::Ops {
            channel: Channel(0),
            ops: forged,
        },
    );

    let observer = hello(&mut r, cid(1));
    subscribe(&mut r, observer, Channel(0), ROOM, 0);
    let ops = received_ops(r.take_outbox(observer));
    assert!(
        ops.iter().all(|op| op.id.client != cid(0xFF)),
        "an op authored under the node's identity reached the room's log"
    );
}

// --- the catch-up path the identity gate protects ---

#[test]
fn a_restarted_client_caught_up_by_an_op_delta_keeps_its_writes() {
    // The end-to-end shape of the loss: a client persists its `ClientId`, restarts
    // with a fresh replica, and the room's log is short enough to serve as an op
    // delta rather than a snapshot. Its pre-restart ops come back, and a counter
    // that ignored them would mint straight into ids the log already holds — every
    // post-restart write silently deduped at ingest.
    let mut r = Registry::new(production_node_id());
    let conn = hello(&mut r, cid(1));

    let mut before = ClientSession::new(cid(1));
    let (channel, sub) = before.subscribe(ROOM);
    assert!(r.deliver(conn, sub));
    r.take_outbox(conn);
    for i in 0..3 {
        let key = format!("before{i}").into_bytes();
        let frame = before
            .edit(channel, |tx| tx.set(&key, Scalar::Int(i as i64)))
            .expect("channel held");
        assert!(r.deliver(conn, frame));
    }
    r.take_outbox(conn);

    // The restart: same persisted identity, a fresh session and replica, joining
    // from the start so the node serves the whole log as a delta.
    let restarted_conn = hello(&mut r, cid(1));
    let mut after = ClientSession::new(cid(1));
    let (channel, sub) = after.subscribe(ROOM);
    assert!(r.deliver(restarted_conn, sub));
    let catch_up = r.take_outbox(restarted_conn);
    assert!(
        catch_up.iter().any(|m| matches!(m, Message::Ops { .. })),
        "the node answered with no op delta: {catch_up:?}"
    );
    assert!(
        !catch_up
            .iter()
            .any(|m| matches!(m, Message::Snapshot { .. })),
        "the node served a snapshot — this test must exercise the delta path"
    );
    for msg in catch_up {
        if matches!(msg, Message::Ops { .. }) {
            after.receive(msg).expect("catch-up folds");
        }
    }

    let frame = after
        .edit(channel, |tx| tx.set(b"after", Scalar::Int(9)))
        .expect("channel held");
    assert!(r.deliver(restarted_conn, frame));

    // A fresh joiner reads the room's log: the post-restart write must be in it.
    let observer = hello(&mut r, cid(2));
    subscribe(&mut r, observer, Channel(0), ROOM, 0);
    let mut replica = Document::new(cid(3));
    for op in received_ops(r.take_outbox(observer)) {
        replica.apply(&op);
    }
    assert!(
        replica.get(b"after").is_some(),
        "the post-restart write was deduped away as a duplicate"
    );
    assert!(replica.get(b"before0").is_some());
}
