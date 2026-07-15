//! Leader-to-follower op replication (Cluster Unit 4).
//!
//! A room's leader mirrors every commit to its follower replicas, so a client
//! redirected to the leader (Unit 3) reaches a node that already holds the room's
//! state. These drive two in-process nodes — a leader `Registry` and a follower
//! `Registry` over the same static cluster — with no socket: the leader queues
//! `Replicate` frames after a commit, the follower applies them into its own
//! replica and answers `ReplicaAck`, and the leader records the follower's
//! watermark. The loopback wiring of that exchange over the real WebSocket
//! transport is exercised separately (and is Miri-ignored).
//!
//! A single-node deployment (no membership) must be byte-identical to today: it
//! leads every room locally and never replicates — the regression these lock.

use std::sync::Arc;

use crdtsync_core::protocol::{Channel, PROTOCOL_VERSION};
use crdtsync_core::{
    decode_message, encode_header, encode_message, ClientId, Document, Message, Op, Scalar,
};
use crdtsync_server::membership::Membership;
use crdtsync_server::placement::NodeId;
use crdtsync_server::runtime::{serve_with, ServeConfig};
use crdtsync_server::{ConnId, ManualClock, Registry};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{accept_async, connect_async, MaybeTlsStream, WebSocketStream};

const CH: Channel = Channel(0);
const N: usize = 3;
const A: &str = "10.0.0.1:9000";
const B: &str = "10.0.0.2:9000";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

/// The static member set, shared by every node's view.
fn members() -> String {
    (1..=5)
        .map(|i| format!("10.0.0.{i}:9000"))
        .collect::<Vec<_>>()
        .join(",")
}

fn membership_for(self_addr: &str) -> Membership {
    Membership::from_static_config(None, Some(self_addr), &members(), N).unwrap()
}

/// A registry whose self is `self_addr` in the shared cluster, or single-node
/// (no membership) when `self_addr` is `None`.
fn node(self_addr: Option<&str>) -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    if let Some(addr) = self_addr {
        r.set_membership(membership_for(addr));
    }
    r
}

/// An authenticated client on `r`, handshake drained.
fn client(r: &mut Registry) -> ConnId {
    let id = r.connect();
    r.deliver(
        id,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
        },
    );
    r.deliver(
        id,
        Message::Auth {
            credential: b"cred".to_vec(),
        },
    );
    r.take_outbox(id);
    id
}

fn sub(room: &[u8]) -> Message {
    Message::Subscribe {
        channel: CH,
        room: room.to_vec(),
        branch: Vec::new(),
        zone: Vec::new(),
        last_seen_seq: 0,
    }
}

/// A room that node `A` leads and node `B` is a non-primary replica of — so `A`
/// replicates the room to `B`.
fn room_led_by_a_with_b_follower() -> Vec<u8> {
    let m = membership_for(A);
    let a = NodeId::from_addr(A);
    let b = NodeId::from_addr(B);
    for i in 0..1_000_000 {
        let room = format!("room-{i}").into_bytes();
        let replicas = m.replicas_for(&room);
        if replicas.first() == Some(&a) && replicas.iter().skip(1).any(|n| n == &b) {
            return room;
        }
    }
    panic!("no room led by A with B a follower");
}

// --- the leader replicates a commit, the follower acks, the leader records it ---

#[test]
fn a_leader_replicates_a_commit_to_its_follower() {
    let room = room_led_by_a_with_b_follower();
    let mut leader = node(Some(A));
    let c = client(&mut leader);
    assert!(leader.deliver(c, sub(&room)));
    leader.take_outbox(c);

    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert!(leader.deliver(
        c,
        Message::Ops {
            channel: CH,
            ops: ops.clone(),
        }
    ));

    let b = NodeId::from_addr(B);
    let pending = leader.take_replication();
    let to_b: Vec<&Message> = pending
        .iter()
        .filter(|(node, _)| *node == b)
        .map(|(_, frame)| frame)
        .collect();
    assert_eq!(to_b.len(), 1, "exactly one Replicate is queued for B");
    assert_eq!(
        *to_b[0],
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops: ops.clone(),
            base_seq: 0,
            epoch: 1,
        },
        "the frame carries the fresh ops on main from an uncompacted base at the \
         leader's first epoch",
    );
}

#[test]
fn the_leader_replicates_to_every_follower() {
    let room = room_led_by_a_with_b_follower();
    let m = membership_for(A);
    let followers: Vec<NodeId> = m
        .replicas_for(&room)
        .into_iter()
        .filter(|n| n != &NodeId::from_addr(A))
        .collect();
    assert!(
        followers.len() >= 2,
        "the fixture room has multiple followers to fan to",
    );

    let mut leader = node(Some(A));
    let c = client(&mut leader);
    leader.deliver(c, sub(&room));
    leader.take_outbox(c);
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    leader.deliver(c, Message::Ops { channel: CH, ops });

    let pending = leader.take_replication();
    let mut targets: Vec<NodeId> = pending.into_iter().map(|(node, _)| node).collect();
    targets.sort();
    let mut expected = followers;
    expected.sort();
    assert_eq!(
        targets, expected,
        "one Replicate per follower, self excluded"
    );
}

#[test]
fn a_follower_applies_a_replicate_and_advances_the_leader_watermark() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);

    // The leader commits and produces the frame for B.
    let mut leader = node(Some(A));
    let c = client(&mut leader);
    leader.deliver(c, sub(&room));
    leader.take_outbox(c);
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    leader.deliver(c, Message::Ops { channel: CH, ops });
    let frame = leader
        .take_replication()
        .into_iter()
        .find(|(node, _)| *node == b)
        .map(|(_, frame)| frame)
        .expect("a frame for B");

    // B, a follower of the room, applies it into its own replica.
    let mut follower = node(Some(B));
    let peer = follower.connect();
    assert!(follower.deliver(peer, frame));
    assert_eq!(follower.hub().seq(&room), 1, "the op reached B's replica");
    let outbox = follower.take_outbox(peer);
    let through = match outbox.as_slice() {
        [Message::ReplicaAck {
            room: acked,
            through_seq,
        }] if *acked == room => *through_seq,
        other => panic!("expected a single ReplicaAck for the room, got {other:?}"),
    };
    assert_eq!(through, 1, "the ack names the sequence B has reached");

    // The leader records B's watermark from the ack.
    assert_eq!(
        leader.replica_watermark(&room, &b),
        0,
        "unrecorded before the ack"
    );
    leader.record_replica_ack(b.clone(), &room, through);
    assert_eq!(leader.replica_watermark(&room, &b), 1);
}

#[test]
fn a_stale_ack_never_lowers_the_watermark() {
    let mut leader = node(Some(A));
    let room = b"room".to_vec();
    let b = NodeId::from_addr(B);
    leader.record_replica_ack(b.clone(), &room, 5);
    leader.record_replica_ack(b.clone(), &room, 2);
    assert_eq!(leader.replica_watermark(&room, &b), 5);
}

#[test]
fn a_follower_drops_a_branch_replicate() {
    // Unit 4 mirrors only the room's main stream. A Replicate naming a branch is
    // anomalous — a leader never sends one — so the follower drops it rather than
    // apply it to a branch it may not hold and falsely ack.
    let room = room_led_by_a_with_b_follower();
    let mut follower = node(Some(B));
    let peer = follower.connect();
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let kept = follower.deliver(
        peer,
        Message::Replicate {
            room: room.clone(),
            branch: b"feature".to_vec(),
            ops,
            base_seq: 0,
            epoch: 1,
        },
    );
    assert!(!kept, "a non-main Replicate drops the connection");
    assert_eq!(follower.hub().seq(&room), 0, "nothing was applied");
    assert!(
        follower.take_outbox(peer).is_empty(),
        "a dropped frame is not acked",
    );
}

// --- gating: a node that does not lead a room never originates replication ---

#[test]
fn a_non_leader_does_not_originate_replication() {
    // Bind the channel while single-node (self leads every room), then place a
    // membership that makes self a follower: the write is redirected, never
    // ingested, so nothing is queued to replicate.
    let m = membership_for(A);
    let a = NodeId::from_addr(A);
    let room = (0..1_000_000)
        .map(|i| format!("room-{i}").into_bytes())
        .find(|room| m.primary_for(room) != Some(a.clone()))
        .expect("a room A does not lead");

    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    let c = client(&mut r);
    r.deliver(c, sub(&room)); // binds while single-node
    r.take_outbox(c);
    r.set_membership(membership_for(A));

    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    r.deliver(c, Message::Ops { channel: CH, ops });
    assert_eq!(
        r.hub().seq(&room),
        0,
        "the write was redirected, not ingested"
    );
    assert!(
        r.take_replication().is_empty(),
        "a follower originates no replication",
    );
}

#[test]
fn a_follower_ignores_a_replicate_for_a_room_it_leads() {
    // A Replicate for a room this node is the primary of is a stray frame — the
    // leader never replicates to itself — so it is dropped, not applied.
    let m = membership_for(A);
    let a = NodeId::from_addr(A);
    let room = (0..1_000_000)
        .map(|i| format!("room-{i}").into_bytes())
        .find(|room| m.primary_for(room) == Some(a.clone()))
        .expect("a room A leads");

    let mut leader = node(Some(A));
    let peer = leader.connect();
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let kept = leader.deliver(
        peer,
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops,
            base_seq: 0,
            epoch: 1,
        },
    );
    assert!(
        !kept,
        "a Replicate for a self-led room drops the connection"
    );
    assert_eq!(leader.hub().seq(&room), 0, "nothing was applied");
}

// --- single-node regression: no membership, no replication ---

#[test]
fn single_node_never_replicates() {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    let c = client(&mut r);
    assert!(r.deliver(c, sub(b"any-room")));
    r.take_outbox(c);

    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert!(r.deliver(c, Message::Ops { channel: CH, ops }));
    assert_eq!(
        r.hub().seq(b"any-room"),
        1,
        "the write ingests locally as before"
    );
    assert!(
        r.take_replication().is_empty(),
        "single-node mode never replicates",
    );
}

#[test]
fn single_node_rejects_a_replicate() {
    // With no membership a node holds no follower role, so an unsolicited
    // Replicate is a stray frame and the connection is dropped.
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    let peer = r.connect();
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let kept = r.deliver(
        peer,
        Message::Replicate {
            room: b"any-room".to_vec(),
            branch: b"main".to_vec(),
            ops,
            base_seq: 0,
            epoch: 1,
        },
    );
    assert!(!kept);
    assert_eq!(r.hub().seq(b"any-room"), 0, "nothing was applied");
}

// ===================== socket transport (Miri-ignored) =====================
//
// These drive the replication frames over the real WebSocket peer connection —
// the outbound dial the leader opens to a follower, and the follower's inbound
// apply-and-ack. They bind loopback sockets, so they are excluded under Miri,
// whose isolation forbids `socket`; the frame logic itself is covered by the
// in-process tests above, which run under Miri.

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn send_frame(ws: &mut Ws, msg: &Message) {
    ws.send(WsMessage::Binary(encode_message(msg).into()))
        .await
        .unwrap();
}

/// The next protocol message on `ws`, decoding past the raw header frame and any
/// control frames.
async fn next_message(ws: &mut Ws) -> Message {
    loop {
        match ws.next().await.unwrap().unwrap() {
            WsMessage::Binary(b) => {
                if let Ok(msg) = decode_message(&b) {
                    return msg;
                }
            }
            WsMessage::Close(_) => panic!("connection closed before a message"),
            _ => continue,
        }
    }
}

fn register_op() -> Vec<Op> {
    doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)))
}

/// A membership over two members named by fixed strings, self chosen by `me`.
fn two_node_membership(me: &str, other: &str) -> Membership {
    Membership::from_static_config(Some(me), None, other, 2).unwrap()
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
async fn a_follower_applies_a_replicate_over_the_socket_and_acks() {
    // Pick a room the peer leads, so this node is a follower and applies a
    // Replicate for it. The leader must stay REACHABLE: under liveness failover
    // (Unit 6a) a follower whose placement primary is unreachable promotes itself
    // to effective leader and then correctly rejects a Replicate for the room it
    // now leads — so the peer is given a real listener that accepts this node's
    // outbound relay dial and holds it open, keeping the leader live.
    let me = "node-a";
    let leader_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let leader_addr = leader_listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        if let Ok((stream, _)) = leader_listener.accept().await {
            if let Ok(mut ws) = accept_async(stream).await {
                while let Some(Ok(_)) = ws.next().await {}
            }
        }
    });
    let m = two_node_membership(me, &leader_addr);
    let room = (0..1_000_000)
        .map(|i| format!("room-{i}").into_bytes())
        .find(|room| m.primary_for(room) == Some(NodeId::from_addr(&leader_addr)))
        .expect("a room the peer leads");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = ServeConfig {
        membership: Some(m),
        ..ServeConfig::default()
    };
    let server = tokio::spawn(serve_with(listener, cid(0xFF), None, config));

    // Dial the follower as a relay peer: header, then an empty-app_id Hello.
    let (mut ws, _) = connect_async(format!("ws://{addr}/")).await.unwrap();
    ws.send(WsMessage::Binary(
        encode_header(PROTOCOL_VERSION).to_vec().into(),
    ))
    .await
    .unwrap();
    send_frame(
        &mut ws,
        &Message::Hello {
            client: cid(0xEE),
            app_id: Vec::new(),
            schema_version: 0,
        },
    )
    .await;

    let ops = register_op();
    send_frame(
        &mut ws,
        &Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops: ops.clone(),
            base_seq: 0,
            epoch: 1,
        },
    )
    .await;

    assert_eq!(
        next_message(&mut ws).await,
        Message::ReplicaAck {
            room,
            through_seq: 1,
        },
        "the follower applies the batch and acks the sequence it reached",
    );
    server.abort();
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
async fn a_leader_dials_a_follower_and_sends_a_replicate() {
    // A follower listener the leader will dial; its address is the peer in the
    // leader's membership, so placement and the dial target agree.
    let follower_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let follower_addr = follower_listener.local_addr().unwrap();
    let leader_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let leader_addr = leader_listener.local_addr().unwrap();

    let leader_id = leader_addr.to_string();
    let follower_id = follower_addr.to_string();
    let m = two_node_membership(&leader_id, &follower_id);
    let room = (0..1_000_000)
        .map(|i| format!("room-{i}").into_bytes())
        .find(|room| m.primary_for(room) == Some(NodeId::from(leader_id.as_str())))
        .expect("a room the leader leads");

    // Stand in for the follower: accept the leader's peer dial and capture the
    // first Replicate it sends.
    let expected_room = room.clone();
    let follower = tokio::spawn(async move {
        let (stream, _) = follower_listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        loop {
            match ws.next().await.unwrap().unwrap() {
                WsMessage::Binary(b) => {
                    if let Ok(Message::Replicate { room, ops, .. }) = decode_message(&b) {
                        assert_eq!(room, expected_room);
                        return ops;
                    }
                }
                WsMessage::Close(_) => panic!("peer closed before a Replicate"),
                _ => continue,
            }
        }
    });

    let config = ServeConfig {
        membership: Some(m),
        ..ServeConfig::default()
    };
    let leader = tokio::spawn(serve_with(leader_listener, cid(0xFF), None, config));

    // Drive a client on the leader: subscribe to the self-led room and write.
    let (mut client, _) = connect_async(format!("ws://{leader_addr}/")).await.unwrap();
    client
        .send(WsMessage::Binary(
            encode_header(PROTOCOL_VERSION).to_vec().into(),
        ))
        .await
        .unwrap();
    send_frame(
        &mut client,
        &Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
        },
    )
    .await;
    send_frame(
        &mut client,
        &Message::Auth {
            credential: b"cred".to_vec(),
        },
    )
    .await;
    assert_eq!(
        next_message(&mut client).await,
        Message::AuthOk {
            actor: b"cred".to_vec(),
        }
    );
    send_frame(
        &mut client,
        &Message::Subscribe {
            channel: CH,
            room: room.clone(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq: 0,
        },
    )
    .await;
    next_message(&mut client).await; // catch-up
    let ops = register_op();
    send_frame(
        &mut client,
        &Message::Ops {
            channel: CH,
            ops: ops.clone(),
        },
    )
    .await;

    let replicated = tokio::time::timeout(std::time::Duration::from_secs(5), follower)
        .await
        .expect("the leader dials the follower and sends a Replicate")
        .unwrap();
    assert_eq!(replicated, ops, "the Replicate carries the fresh ops");
    leader.abort();
}
