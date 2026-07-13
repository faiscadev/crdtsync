//! Majority-ACK durability (Cluster Unit 5).
//!
//! A room's leader withholds a client's write-ack `Accepted` until a majority of
//! the room's replica set holds the write. The replica set is the primary (self)
//! plus its followers, size R; a majority is `R / 2 + 1`, and self — which holds
//! the write it just committed — always counts as one, so the leader needs
//! `R / 2` follower acks at or past the write's server sequence. These drive an
//! in-process leader `Registry`: the client subscribes and writes, and the
//! follower acks are fed in with `record_replica_ack` (as the peer connection
//! would), so no socket is bound and the suite runs under Miri.
//!
//! Single-node (no membership) and a self-only replica set are a majority of one:
//! the write is durable on commit and acked at once, byte-identical to before —
//! the regression these lock.

use std::sync::Arc;

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Op, Scalar};
use crdtsync_server::membership::Membership;
use crdtsync_server::placement::NodeId;
use crdtsync_server::{ConnId, ManualClock, Registry};

const CH: Channel = Channel(0);
const A: &str = "10.0.0.1:9000";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

/// A five-member cluster shared by every node's view, so a replication factor up
/// to five resolves without clamping.
fn members() -> String {
    (1..=5)
        .map(|i| format!("10.0.0.{i}:9000"))
        .collect::<Vec<_>>()
        .join(",")
}

fn membership_for(self_addr: &str, rf: usize) -> Membership {
    Membership::from_static_config(None, Some(self_addr), &members(), rf).unwrap()
}

/// A leader registry whose self is `A`, placing at replication factor `rf`.
fn leader(rf: usize) -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for(A, rf));
    r
}

/// A single-node registry: no membership, so it leads every room locally.
fn single_node() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
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
        last_seen_seq: 0,
    }
}

fn write() -> Vec<Op> {
    doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)))
}

/// The ordered followers of `room` at replication factor `rf`, self (A) excluded.
fn followers_of(room: &[u8], rf: usize) -> Vec<NodeId> {
    let m = membership_for(A, rf);
    let a = NodeId::from_addr(A);
    m.replicas_for(room)
        .into_iter()
        .filter(|n| n != &a)
        .collect()
}

/// A room `A` leads with exactly `follower_count` followers at replication factor
/// `rf` — so the majority math is exact and known.
fn room_led_by_a(rf: usize, follower_count: usize) -> Vec<u8> {
    let a = NodeId::from_addr(A);
    let m = membership_for(A, rf);
    for i in 0..1_000_000 {
        let room = format!("room-{i}").into_bytes();
        let replicas = m.replicas_for(&room);
        if replicas.first() == Some(&a) && replicas.len() == rf {
            let followers = replicas.iter().filter(|n| *n != &a).count();
            if followers == follower_count {
                return room;
            }
        }
    }
    panic!("no room led by A with {follower_count} followers at rf {rf}");
}

/// Whether `outbox` carries a write-ack `Accepted` on `CH` — the released ack.
/// Its `through` is the author's per-client op sequence; the majority gate turns
/// on the ack's presence, not that value.
fn has_accepted(outbox: &[Message]) -> bool {
    outbox
        .iter()
        .any(|m| matches!(m, Message::Accepted { channel, .. } if *channel == CH))
}

/// Whether `outbox` carries a write-ack on `CH` through the author op sequence
/// `through` — the ack for that specific write.
fn has_accepted_through(outbox: &[Message], through: u64) -> bool {
    outbox.iter().any(|m| {
        matches!(m, Message::Accepted { channel, through: t } if *channel == CH && *t == through)
    })
}

// --- R=3: the ack is withheld until one follower holds the write ---

#[test]
fn a_three_node_write_acks_only_after_a_follower_holds_it() {
    // R=3: replica set is A plus two followers, majority 2 — A plus one follower.
    let room = room_led_by_a(3, 2);
    let mut r = leader(3);
    let c = client(&mut r);
    r.deliver(c, sub(&room));
    r.take_outbox(c);

    // The write commits locally but is not yet held by any follower, so its ack
    // is withheld — nothing is queued for the client.
    assert!(r.deliver(
        c,
        Message::Ops {
            channel: CH,
            ops: write()
        }
    ));
    assert_eq!(r.hub().seq(&room), 1, "the write committed on the leader");
    assert!(
        r.take_outbox(c).is_empty(),
        "the Accepted is withheld until a majority holds the write",
    );

    // The first follower acks the write's sequence: A plus it is a majority, so
    // the ack is released.
    let followers = followers_of(&room, 3);
    r.record_replica_ack(followers[0].clone(), &room, 1);
    assert!(
        has_accepted(&r.take_outbox(c)),
        "one follower ack reaches a majority and releases the Accepted",
    );
}

#[test]
fn one_dead_follower_still_reaches_a_majority() {
    // R=3, two followers; one is dead (never acks). The live follower's ack alone
    // is a majority with self, so the write is still acked.
    let room = room_led_by_a(3, 2);
    let mut r = leader(3);
    let c = client(&mut r);
    r.deliver(c, sub(&room));
    r.take_outbox(c);
    r.deliver(
        c,
        Message::Ops {
            channel: CH,
            ops: write(),
        },
    );
    assert!(r.take_outbox(c).is_empty(), "withheld before any ack");

    // Only the second follower acks — the first stays silent (dead).
    let followers = followers_of(&room, 3);
    r.record_replica_ack(followers[1].clone(), &room, 1);
    assert!(
        has_accepted(&r.take_outbox(c)),
        "self plus one live follower is a majority even with the other dead",
    );
}

// --- a majority of followers dead stalls the ack — no false durability ---

#[test]
fn a_stalled_majority_never_acks() {
    // R=5: replica set is A plus four followers, majority 3 — A needs two follower
    // acks. Only one follower acks; the write must stay un-acked (no false
    // durability), asserted by the outbox staying empty. There is no failure
    // detection yet, so a non-durable write is correctly never acked.
    let room = room_led_by_a(5, 4);
    let mut r = leader(5);
    let c = client(&mut r);
    r.deliver(c, sub(&room));
    r.take_outbox(c);
    r.deliver(
        c,
        Message::Ops {
            channel: CH,
            ops: write(),
        },
    );

    let followers = followers_of(&room, 5);
    r.record_replica_ack(followers[0].clone(), &room, 1);
    assert!(
        r.take_outbox(c).is_empty(),
        "one of four followers is below the majority of three — no ack",
    );

    // A second follower ack reaches the majority of three and releases it.
    r.record_replica_ack(followers[1].clone(), &room, 1);
    assert!(
        has_accepted(&r.take_outbox(c)),
        "the second follower ack reaches the majority",
    );
}

#[test]
fn a_stale_follower_ack_does_not_release_a_later_write() {
    // A follower ack at a sequence below the write's does not count it as held: the
    // watermark must reach the write's own sequence.
    let room = room_led_by_a(3, 2);
    let mut r = leader(3);
    let c = client(&mut r);
    r.deliver(c, sub(&room));
    r.take_outbox(c);

    // Two distinct writes from one author: server sequences 1 and 2, each acked
    // through its own author op sequence.
    let mut d = doc(1);
    let ops1 = d.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let ops2 = d.transact(|tx| tx.register(b"b", Scalar::Int(2)));
    let t1 = ops1.iter().map(|o| o.id.seq).max().unwrap();
    let t2 = ops2.iter().map(|o| o.id.seq).max().unwrap();
    r.deliver(
        c,
        Message::Ops {
            channel: CH,
            ops: ops1,
        },
    );
    r.deliver(
        c,
        Message::Ops {
            channel: CH,
            ops: ops2,
        },
    );
    assert_eq!(r.hub().seq(&room), 2, "both writes committed");
    assert!(r.take_outbox(c).is_empty(), "both writes withheld");

    // A follower ack through server sequence 1 releases the first write but not the
    // second, which is not yet held to sequence 2.
    let followers = followers_of(&room, 3);
    r.record_replica_ack(followers[0].clone(), &room, 1);
    let out = r.take_outbox(c);
    assert!(
        has_accepted_through(&out, t1),
        "the first write reaches a majority",
    );
    assert!(
        !has_accepted_through(&out, t2),
        "the second write is not yet held to sequence 2",
    );

    // Advancing the follower to sequence 2 releases the second write.
    r.record_replica_ack(followers[0].clone(), &room, 2);
    assert!(
        has_accepted_through(&r.take_outbox(c), t2),
        "the follower reaching sequence 2 releases the second write",
    );
}

// --- single-node / self-only: majority one, immediate ack (regression) ---

#[test]
fn single_node_acks_immediately() {
    // No membership: the replica set is self alone, majority one — the write is
    // durable on commit and acked at once, exactly as before the gate existed.
    let mut r = single_node();
    let c = client(&mut r);
    r.deliver(c, sub(b"any-room"));
    r.take_outbox(c);

    assert!(r.deliver(
        c,
        Message::Ops {
            channel: CH,
            ops: write()
        }
    ));
    assert!(
        has_accepted(&r.take_outbox(c)),
        "single-node acks immediately, no deferral",
    );
}

#[test]
fn replication_factor_one_acks_immediately() {
    // R=1: the replica set is the primary alone, majority one — self satisfies it,
    // so the ack is immediate even with membership present.
    let room = room_led_by_a(1, 0);
    let mut r = leader(1);
    let c = client(&mut r);
    r.deliver(c, sub(&room));
    r.take_outbox(c);

    assert!(r.deliver(
        c,
        Message::Ops {
            channel: CH,
            ops: write()
        }
    ));
    assert!(
        has_accepted(&r.take_outbox(c)),
        "a self-only replica set is a majority of one — immediate ack",
    );
}

// --- an empty resend acks at once — nothing fresh to replicate ---

#[test]
fn a_resent_op_acks_without_waiting_on_replication() {
    // Re-submitting an already-logged op adds nothing fresh to replicate, so its
    // ack is not gated on a new follower watermark — it releases at once, else a
    // duplicate would hang forever.
    let room = room_led_by_a(3, 2);
    let mut r = leader(3);
    let c = client(&mut r);
    r.deliver(c, sub(&room));
    r.take_outbox(c);

    let ops = write();
    r.deliver(
        c,
        Message::Ops {
            channel: CH,
            ops: ops.clone(),
        },
    );
    // Release the first write so the outbox is clean.
    let followers = followers_of(&room, 3);
    r.record_replica_ack(followers[0].clone(), &room, 1);
    assert!(has_accepted(&r.take_outbox(c)));

    // Resend the same op: the hub already holds it, so nothing new is broadcast or
    // replicated, and the ack is released immediately.
    r.deliver(c, Message::Ops { channel: CH, ops });
    assert!(
        has_accepted(&r.take_outbox(c)),
        "a resent op is durable already and acks at once",
    );
}

#[test]
fn a_disconnect_drops_a_withheld_ack() {
    // A withheld write-ack for an author that disconnects before a majority is
    // dropped, so a room stalled below quorum does not retain the record: a later
    // follower ack releases nothing to the gone connection.
    let room = room_led_by_a(3, 2);
    let mut r = leader(3);
    let c = client(&mut r);
    r.deliver(c, sub(&room));
    r.take_outbox(c);
    r.deliver(
        c,
        Message::Ops {
            channel: CH,
            ops: write(),
        },
    );
    assert!(r.take_outbox(c).is_empty(), "the ack is withheld");

    // The author leaves before a majority holds the write.
    r.disconnect(c);

    // A follower ack now reaching a majority releases nothing — the record was
    // dropped with the connection, not left orphaned.
    let followers = followers_of(&room, 3);
    r.record_replica_ack(followers[0].clone(), &room, 1);
    assert!(
        r.take_outbox(c).is_empty(),
        "a disconnected author's withheld ack is not retained",
    );
}

// ===================== socket transport (Miri-ignored) =====================
//
// These drive the whole runtime wire path — the leader dials the follower, the
// follower applies the Replicate and acks over the real WebSocket peer
// connection, and the leader releases the withheld client `Accepted`. They bind
// loopback sockets, so they are excluded under Miri, whose isolation forbids
// `socket`; the majority logic itself is covered by the in-process tests above,
// which run under Miri.

use crdtsync_core::encode_header;
use crdtsync_core::protocol::PROTOCOL_VERSION;
use crdtsync_core::{decode_message, encode_message};
use crdtsync_server::runtime::{serve_with, ServeConfig};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// A two-member cluster, self chosen by `me`, at replication factor 2 — so a
/// room's replica set is the primary plus one follower and a majority is both.
fn two_node_membership(me: &str, other: &str) -> Membership {
    Membership::from_static_config(Some(me), None, other, 2).unwrap()
}

async fn send_frame(ws: &mut Ws, msg: &Message) {
    ws.send(WsMessage::Binary(encode_message(msg).into()))
        .await
        .unwrap();
}

/// Open the client end: the raw header, then Hello + Auth, draining the AuthOk.
async fn open_client(addr: &str) -> Ws {
    let (mut ws, _) = connect_async(format!("ws://{addr}/")).await.unwrap();
    ws.send(WsMessage::Binary(
        encode_header(PROTOCOL_VERSION).to_vec().into(),
    ))
    .await
    .unwrap();
    send_frame(
        &mut ws,
        &Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
        },
    )
    .await;
    send_frame(
        &mut ws,
        &Message::Auth {
            credential: b"cred".to_vec(),
        },
    )
    .await;
    // Drain the AuthOk.
    loop {
        if let WsMessage::Binary(b) = ws.next().await.unwrap().unwrap() {
            if matches!(decode_message(&b), Ok(Message::AuthOk { .. })) {
                break;
            }
        }
    }
    ws
}

/// Subscribe `ws` to `room` and drain the catch-up reply.
async fn subscribe(ws: &mut Ws, room: &[u8]) {
    send_frame(
        ws,
        &Message::Subscribe {
            channel: CH,
            room: room.to_vec(),
            branch: Vec::new(),
            last_seen_seq: 0,
        },
    )
    .await;
    loop {
        if let WsMessage::Binary(b) = ws.next().await.unwrap().unwrap() {
            if matches!(
                decode_message(&b),
                Ok(Message::Ops { .. } | Message::Snapshot { .. })
            ) {
                break;
            }
        }
    }
}

/// The next `Accepted` on `ws` within `within`, or `None` if none arrives — the
/// bounded poll a no-durability assertion needs so it never hangs.
async fn accepted_within(ws: &mut Ws, within: Duration) -> Option<u64> {
    tokio::time::timeout(within, async {
        loop {
            match ws.next().await {
                Some(Ok(WsMessage::Binary(b))) => {
                    if let Ok(Message::Accepted { through, .. }) = decode_message(&b) {
                        return through;
                    }
                }
                Some(Ok(_)) => continue,
                // On close, let the outer timeout resolve the poll to `None`.
                _ => futures_util::future::pending::<()>().await,
            }
        }
    })
    .await
    .ok()
}

/// A room the node named `leader_id` leads in the two-node cluster, so its write
/// is served there and replicated to the follower.
fn room_led_by(leader_id: &str, follower_id: &str) -> Vec<u8> {
    let m = two_node_membership(leader_id, follower_id);
    let leader = NodeId::from(leader_id);
    (0..1_000_000)
        .map(|i| format!("room-{i}").into_bytes())
        .find(|room| m.primary_for(room) == Some(leader.clone()))
        .expect("a room the leader leads")
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
async fn a_client_write_acks_only_after_the_follower_holds_it() {
    // A two-node cluster over real sockets: the leader withholds the client's
    // Accepted until the follower — dialed and fed the Replicate by the leader —
    // applies the write and acks it back, at which point the leader releases it.
    let follower_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let follower_addr = follower_listener.local_addr().unwrap().to_string();
    let leader_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let leader_addr = leader_listener.local_addr().unwrap().to_string();
    let room = room_led_by(&leader_addr, &follower_addr);

    let follower = tokio::spawn(serve_with(
        follower_listener,
        cid(0xF0),
        None,
        ServeConfig {
            membership: Some(two_node_membership(&follower_addr, &leader_addr)),
            ..ServeConfig::default()
        },
    ));
    let leader = tokio::spawn(serve_with(
        leader_listener,
        cid(0xFF),
        None,
        ServeConfig {
            membership: Some(two_node_membership(&leader_addr, &follower_addr)),
            ..ServeConfig::default()
        },
    ));

    let mut ws = open_client(&leader_addr).await;
    subscribe(&mut ws, &room).await;
    send_frame(
        &mut ws,
        &Message::Ops {
            channel: CH,
            ops: write(),
        },
    )
    .await;

    assert!(
        accepted_within(&mut ws, Duration::from_secs(5))
            .await
            .is_some(),
        "the leader releases the Accepted once the follower holds the write",
    );
    leader.abort();
    follower.abort();
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback server over a real socket
async fn a_client_write_stalls_with_no_follower() {
    // The follower's address is reserved then freed, so the leader's dial is
    // refused and retried forever — the write is never replicated. Its Accepted
    // must never arrive: a non-durable write is not falsely acked. Asserted by a
    // bounded poll, so the test does not hang.
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let follower_addr = reserved.local_addr().unwrap().to_string();
    drop(reserved); // free the port — nothing serves it
    let leader_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let leader_addr = leader_listener.local_addr().unwrap().to_string();
    let room = room_led_by(&leader_addr, &follower_addr);

    let leader = tokio::spawn(serve_with(
        leader_listener,
        cid(0xFF),
        None,
        ServeConfig {
            membership: Some(two_node_membership(&leader_addr, &follower_addr)),
            ..ServeConfig::default()
        },
    ));

    let mut ws = open_client(&leader_addr).await;
    subscribe(&mut ws, &room).await;
    send_frame(
        &mut ws,
        &Message::Ops {
            channel: CH,
            ops: write(),
        },
    )
    .await;

    assert!(
        accepted_within(&mut ws, Duration::from_millis(500))
            .await
            .is_none(),
        "with no follower a majority is never reached, so no Accepted is sent",
    );
    leader.abort();
}
