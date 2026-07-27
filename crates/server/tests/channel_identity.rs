//! One connection, two channels on one room — the op-id spaces stay disjoint.
//!
//! A connection multiplexes several subscriptions, and two of them can name the
//! same room: a whole-room subscription beside a zone-scoped one, two zones, a
//! branch beside the default. Each holds its own replica, so each is its own
//! author — and the hub dedups a room's log by [`OpId`], so two channels sharing
//! an author identity would have one channel's ops dropped as duplicates of the
//! other's. A client authors each channel under
//! [`ClientId::for_channel`](crdtsync_core::ClientId::for_channel) of its Hello
//! id; the session driver re-derives that from the Hello id and the channel an op
//! batch names, and refuses a batch carrying any other identity.

use crdtsync_core::client::ClientSession;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Op, Scalar};
use crdtsync_server::{ConnId, Registry};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn registry() -> Registry {
    Registry::new(cid(0xFF))
}

const ROOM: &[u8] = b"room-a";

fn hello(r: &mut Registry, client: ClientId) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client,
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"cred".to_vec()
        }
    ));
    r.take_outbox(id);
    id
}

fn subscribe(r: &mut Registry, id: ConnId, channel: Channel, room: &[u8]) {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel,
            room: room.to_vec(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq: 0,
        }
    ));
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

/// Every op the connection's outbox carried, flattened out of its `Ops` frames.
fn received_ops(msgs: Vec<Message>) -> Vec<Op> {
    msgs.into_iter()
        .flat_map(|m| match m {
            Message::Ops { ops, .. } => ops,
            _ => Vec::new(),
        })
        .collect()
}

/// A one-op `Message::Ops` on `channel` authored under `client`.
fn ops_frame(channel: Channel, client: ClientId, key: &[u8]) -> Message {
    let ops = Document::new(client).transact(|tx| tx.register(key, Scalar::Int(1)));
    Message::Ops { channel, ops }
}

// --- both channels' writes survive the room's dedup ---

#[test]
fn two_channels_of_one_connection_both_reach_the_room() {
    let mut r = registry();
    let writer = hello(&mut r, cid(1));

    // A real client session: two subscriptions to one room, each with its own
    // replica, both minting from seq 0.
    let mut session = ClientSession::new(cid(1));
    let (a, sub_a) = session.subscribe(ROOM);
    let (b, sub_b) = session.subscribe(ROOM);
    assert!(r.deliver(writer, sub_a));
    assert!(r.deliver(writer, sub_b));
    r.take_outbox(writer);

    // A peer holding the whole room, to observe what the log kept.
    let peer = hello(&mut r, cid(2));
    subscribe(&mut r, peer, Channel(9), ROOM);
    r.take_outbox(peer);

    let from_a = session
        .edit(a, |tx| tx.register(b"a", Scalar::Int(1)))
        .unwrap();
    let from_b = session
        .edit(b, |tx| tx.register(b"b", Scalar::Int(2)))
        .unwrap();
    assert!(r.deliver(writer, from_a));
    assert!(r.deliver(writer, from_b));

    let ops = received_ops(r.take_outbox(peer));
    assert_eq!(ops.len(), 2, "the room's dedup dropped a channel's op");

    let mut replica = Document::new(cid(3));
    for op in &ops {
        replica.apply(op);
    }
    assert!(replica.get(b"a").is_some(), "lost the first channel's edit");
    assert!(
        replica.get(b"b").is_some(),
        "lost the second channel's edit"
    );
}

// --- the driver's op/channel consistency gate ---

/// A connection holding both channels of `ROOM`, its handshake replies drained.
fn two_channel_conn(r: &mut Registry) -> ConnId {
    let conn = hello(r, cid(1));
    subscribe(r, conn, Channel(0), ROOM);
    subscribe(r, conn, Channel(1), ROOM);
    r.take_outbox(conn);
    conn
}

/// A batch the gate admitted is acknowledged through its author's seq frontier —
/// the positive signal, which "no violation" alone would not distinguish from a
/// batch dropped for some other reason.
fn assert_accepted(r: &mut Registry, conn: ConnId, channel: Channel) {
    let out = r.take_outbox(conn);
    assert!(
        !out.iter().any(is_violation),
        "the batch was refused: {out:?}"
    );
    assert!(
        out.iter()
            .any(|m| matches!(m, Message::Accepted { channel: c, .. } if *c == channel)),
        "the batch was not acknowledged on {channel:?}: {out:?}"
    );
}

#[test]
fn channel_zero_writes_under_the_connections_own_identity() {
    let mut r = registry();
    let conn = two_channel_conn(&mut r);

    assert!(r.deliver(conn, ops_frame(Channel(0), cid(1), b"x")));
    assert_accepted(&mut r, conn, Channel(0));
}

#[test]
fn a_further_channel_writes_under_its_derived_identity() {
    let mut r = registry();
    let conn = two_channel_conn(&mut r);

    assert!(r.deliver(conn, ops_frame(Channel(1), cid(1).for_channel(1), b"x")));
    assert_accepted(&mut r, conn, Channel(1));
}

// The gate binds each identity to the channel its batch names: a batch carrying
// the identity of a *different* channel of the same connection is refused, in
// either direction.
#[test]
fn an_op_under_another_channels_identity_is_refused() {
    let mut r = registry();
    let conn = two_channel_conn(&mut r);
    r.deliver(conn, ops_frame(Channel(0), cid(1).for_channel(1), b"x"));
    assert!(r.take_outbox(conn).iter().any(is_violation));

    let mut r = registry();
    let conn = two_channel_conn(&mut r);
    r.deliver(conn, ops_frame(Channel(1), cid(1), b"x"));
    assert!(r.take_outbox(conn).iter().any(is_violation));
}

#[test]
fn an_op_under_an_unrelated_identity_is_refused() {
    let mut r = registry();
    let conn = two_channel_conn(&mut r);
    r.deliver(conn, ops_frame(Channel(1), cid(2).for_channel(1), b"x"));
    assert!(r.take_outbox(conn).iter().any(is_violation));
}
