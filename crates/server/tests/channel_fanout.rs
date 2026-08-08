//! Fan-out excludes the writing *channel*, not the writing connection.
//!
//! A connection multiplexes several subscriptions, and two of them can name the
//! same room. Each holds its own replica under its own
//! [`ClientId::for_channel`](crdtsync_core::ClientId::for_channel) author, so a
//! sibling channel is as distinct an author as a peer connection is — and its
//! replica converges only if the room's writes actually reach it. Skipping the
//! whole writing connection would leave a session's two channels on one room
//! permanently divergent for as long as the connection lives, their seen
//! sequences frozen over each other's writes.
//!
//! Awareness is the opposite case and stays connection-scoped: a connection's
//! presence is keyed by its Hello client id and its authenticated actor, both of
//! which its channels share, so a sibling echo would hand a client its own
//! presence back as a peer's with nothing on the wire to tell it apart.

use std::sync::Arc;

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::client::ClientSession;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{AclEffect, ClientId, Element, Message, Op, Scalar};
use crdtsync_server::acl::actor_key;
use crdtsync_server::{ConnId, ManualClock, Registry};

const ROOM: &[u8] = b"room-a";
const OTHER_ROOM: &[u8] = b"room-b";
const BRANCH: &[u8] = b"feature";
const CRED: &[u8] = b"cred";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A fixed clock keeps the suite Miri-clean — awareness stamps its entries with
/// the wall time, which Miri's isolation refuses to read.
fn registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

/// A connection driven by a real client session, handshake done and its replies
/// folded back in, so every channel it opens authors under the identity the
/// server's consistency gate expects.
fn client(r: &mut Registry, client: ClientId) -> (ConnId, ClientSession) {
    let conn = r.connect();
    let mut session = ClientSession::new(client);
    assert!(r.deliver(conn, session.hello()));
    assert!(r.deliver(conn, session.auth(CRED)));
    pump(r, conn, &mut session);
    (conn, session)
}

/// Drain `conn`'s outbox into its session, returning what it carried.
fn pump(r: &mut Registry, conn: ConnId, session: &mut ClientSession) -> Vec<Message> {
    let out = r.take_outbox(conn);
    for msg in out.clone() {
        session.receive(msg).expect("the client takes the frame");
    }
    out
}

/// The integer a channel's replica holds at top-level `key`, or `None` when it
/// holds nothing there.
fn reg(session: &ClientSession, channel: Channel, key: &[u8]) -> Option<i64> {
    match session.document(channel)?.get(key)? {
        Element::Register(r) => match r.borrow().read() {
            Scalar::Int(n) => Some(*n),
            other => panic!("expected an Int, got {other:?}"),
        },
        _ => panic!("expected a Register"),
    }
}

/// The ops `msgs` delivered on `channel`, flattened across its `Ops` frames.
fn ops_on(msgs: &[Message], channel: Channel) -> Vec<Op> {
    msgs.iter()
        .filter_map(|m| match m {
            Message::Ops { channel: c, ops } if *c == channel => Some(ops.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// A connection holding two channels on `ROOM`, its subscribe replies folded in.
fn two_channels(r: &mut Registry) -> (ConnId, ClientSession, Channel, Channel) {
    let (conn, mut session) = client(r, cid(1));
    let (a, sub_a) = session.subscribe(ROOM).unwrap();
    let (b, sub_b) = session.subscribe(ROOM).unwrap();
    assert!(r.deliver(conn, sub_a));
    assert!(r.deliver(conn, sub_b));
    pump(r, conn, &mut session);
    (conn, session, a, b)
}

// --- a write reaches the writing connection's other channels ---

#[test]
fn a_write_reaches_the_sibling_channel_of_the_writing_connection() {
    let mut r = registry();
    let (conn, mut session, a, b) = two_channels(&mut r);

    let write = session
        .edit(a, |tx| tx.register(b"k", Scalar::Int(7)))
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, write));
    pump(&mut r, conn, &mut session);

    assert_eq!(
        reg(&session, b, b"k"),
        Some(7),
        "the sibling channel's replica never received the write"
    );
}

#[test]
fn the_sibling_channels_seen_sequence_advances_over_the_write() {
    let mut r = registry();
    let (conn, mut session, a, b) = two_channels(&mut r);

    let write = session
        .edit(a, |tx| tx.register(b"k", Scalar::Int(7)))
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, write));
    pump(&mut r, conn, &mut session);

    assert_eq!(
        session.last_seen_seq(b),
        Some(1),
        "the sibling channel's seen sequence stalled over the write"
    );
}

#[test]
fn the_authoring_channel_is_not_echoed_its_own_write() {
    let mut r = registry();
    let (conn, mut session, a, _b) = two_channels(&mut r);

    let write = session
        .edit(a, |tx| tx.register(b"k", Scalar::Int(7)))
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, write));
    let out = pump(&mut r, conn, &mut session);

    assert!(
        ops_on(&out, a).is_empty(),
        "the authoring channel was echoed its own write: {out:?}"
    );
    assert_eq!(
        session.last_seen_seq(a),
        Some(0),
        "the authoring channel's seen sequence moved on its own write"
    );
}

#[test]
fn a_single_channel_connection_is_not_echoed_its_own_write() {
    let mut r = registry();
    let (conn, mut session) = client(&mut r, cid(1));
    let (a, sub) = session.subscribe(ROOM).unwrap();
    assert!(r.deliver(conn, sub));
    pump(&mut r, conn, &mut session);

    let write = session
        .edit(a, |tx| tx.register(b"k", Scalar::Int(7)))
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, write));
    let out = pump(&mut r, conn, &mut session);

    assert!(
        ops_on(&out, a).is_empty(),
        "the sole channel was echoed its own write: {out:?}"
    );
}

#[test]
fn a_peer_connection_still_receives_the_write() {
    let mut r = registry();
    let (conn, mut session, a, _b) = two_channels(&mut r);
    let (peer_conn, mut peer) = client(&mut r, cid(2));
    let (p, sub) = peer.subscribe(ROOM).unwrap();
    assert!(r.deliver(peer_conn, sub));
    pump(&mut r, peer_conn, &mut peer);

    let write = session
        .edit(a, |tx| tx.register(b"k", Scalar::Int(7)))
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, write));
    pump(&mut r, conn, &mut session);
    pump(&mut r, peer_conn, &mut peer);

    assert_eq!(
        reg(&peer, p, b"k"),
        Some(7),
        "the peer connection lost the write"
    );
}

#[test]
fn a_sibling_channel_on_another_room_receives_nothing() {
    let mut r = registry();
    let (conn, mut session) = client(&mut r, cid(1));
    let (a, sub_a) = session.subscribe(ROOM).unwrap();
    let (b, sub_b) = session.subscribe(OTHER_ROOM).unwrap();
    assert!(r.deliver(conn, sub_a));
    assert!(r.deliver(conn, sub_b));
    pump(&mut r, conn, &mut session);

    let write = session
        .edit(a, |tx| tx.register(b"k", Scalar::Int(7)))
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, write));
    let out = pump(&mut r, conn, &mut session);

    assert!(
        ops_on(&out, b).is_empty(),
        "a write crossed into another room's channel: {out:?}"
    );
}

#[test]
fn a_sibling_channel_on_another_branch_receives_nothing() {
    let mut r = registry();
    let (conn, mut session) = client(&mut r, cid(1));
    let (a, sub_a) = session.subscribe(ROOM).unwrap();
    assert!(r.deliver(conn, sub_a));
    pump(&mut r, conn, &mut session);
    // The room must exist before a branch can fork off it, so `a` bootstraps it.
    let seed = session
        .edit(a, |tx| tx.register(b"seed", Scalar::Int(0)))
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, seed));
    assert!(r.deliver(conn, session.fork_branch(ROOM, BRANCH, b"main")));
    let (b, sub_b) = session.subscribe_branch(ROOM, BRANCH).unwrap();
    assert!(r.deliver(conn, sub_b));
    pump(&mut r, conn, &mut session);

    let write = session
        .edit(a, |tx| tx.register(b"k", Scalar::Int(7)))
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, write));
    let out = pump(&mut r, conn, &mut session);

    assert!(
        ops_on(&out, b).is_empty(),
        "a main write crossed into a branch channel of the same connection: {out:?}"
    );
}

// --- the doc-ACL redacting fan-out takes the same channel exclusion ---

#[test]
fn a_redacted_rooms_write_reaches_the_sibling_channel() {
    let mut r = registry();
    let (conn, mut session, a, b) = two_channels(&mut r);

    // The first authenticated writer owns `/`, so this connection reads every op;
    // the tuple is what routes the room's fan-out through the redacting path.
    let grant = session
        .edit(a, |tx| {
            tx.acl().grant(
                AclSubject::Actor(actor_key(b"other")),
                AclGrant::Capability(Capability::Read),
                AclEffect::Allow,
                b"/pub".to_vec(),
                actor_key(CRED),
            );
        })
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, grant));
    pump(&mut r, conn, &mut session);

    let write = session
        .edit(a, |tx| tx.register(b"k", Scalar::Int(7)))
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, write));
    pump(&mut r, conn, &mut session);

    assert_eq!(
        reg(&session, b, b"k"),
        Some(7),
        "the redacting fan-out withheld the write from the sibling channel"
    );
}

#[test]
fn a_redacted_rooms_authoring_channel_is_not_echoed_its_own_write() {
    let mut r = registry();
    let (conn, mut session, a, _b) = two_channels(&mut r);

    let grant = session
        .edit(a, |tx| {
            tx.acl().grant(
                AclSubject::Actor(actor_key(b"other")),
                AclGrant::Capability(Capability::Read),
                AclEffect::Allow,
                b"/pub".to_vec(),
                actor_key(CRED),
            );
        })
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, grant));
    pump(&mut r, conn, &mut session);

    let write = session
        .edit(a, |tx| tx.register(b"k", Scalar::Int(7)))
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, write));
    let out = pump(&mut r, conn, &mut session);

    assert!(
        ops_on(&out, a).is_empty(),
        "the redacting fan-out echoed the authoring channel its own write: {out:?}"
    );
}

// --- awareness stays connection-scoped ---

/// A connection's presence is one entry keyed by its Hello client id and actor,
/// which its channels share — a sibling echo would surface a client's own
/// presence to itself as a peer's, indistinguishably, so the whole connection
/// stays excluded.
#[test]
fn awareness_does_not_echo_to_a_sibling_channel() {
    let mut r = registry();
    let (conn, mut session, a, b) = two_channels(&mut r);

    let set = session
        .set_awareness(a, b"cursor", b"7")
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, set));
    let out = pump(&mut r, conn, &mut session);

    assert!(
        !out.iter()
            .any(|m| matches!(m, Message::AwarenessUpdate { .. })),
        "presence echoed back to the publishing connection: {out:?}"
    );
    assert_eq!(
        session.awareness_len(b),
        0,
        "the sibling channel took its own connection's presence as a peer's"
    );
}

#[test]
fn awareness_still_reaches_a_peer_connection() {
    let mut r = registry();
    let (conn, mut session, a, _b) = two_channels(&mut r);
    let (peer_conn, mut peer) = client(&mut r, cid(2));
    let (p, sub) = peer.subscribe(ROOM).unwrap();
    assert!(r.deliver(peer_conn, sub));
    pump(&mut r, peer_conn, &mut peer);

    let set = session
        .set_awareness(a, b"cursor", b"7")
        .expect("the channel is subscribed");
    assert!(r.deliver(conn, set));
    pump(&mut r, conn, &mut session);
    pump(&mut r, peer_conn, &mut peer);

    assert_eq!(
        peer.awareness(p, CRED, b"cursor"),
        Some(b"7".as_slice()),
        "the peer connection lost the presence update"
    );
}
