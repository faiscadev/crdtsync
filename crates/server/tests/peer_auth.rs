//! C10 — the peer plane is authenticated, and only the peer plane carries
//! node-to-node frames.
//!
//! `Replicate`, `ReplicateSnapshot`, `Gossip`, `FollowerHeads` and `PingReq` all
//! arrive node-to-node on a link a member dialed, and the registry handles them
//! ahead of the client session step. Nothing about those frames identifies a
//! member: every node dials under the same reserved replica id, so the link's
//! `Hello` distinguishes nobody, and a `FollowerHeads` simply names whichever node
//! it likes. Left ungated, a socket that has said nothing at all could push
//! arbitrary ops into any room a clustered node replicates — past the identity
//! reservation, the doc-ACL write tier, the schema tier, the cross-zone token gate
//! and leadership, in one frame — and an ordinary subscriber would read them.
//!
//! So a connection reaches those handlers only after presenting the deployment's
//! cluster secret in a `PeerAuth`. On any other connection the five frames fall
//! through to the session step, which answers each with the protocol violation it
//! is. The secret is the whole credential: a node with none configured has no peer
//! plane at all.
//!
//! Most of these drive the registry in process (no sockets), so they are
//! deterministic and run under Miri; the socket tests at the end pin that a real
//! two-node cluster still replicates end to end, and that a real node refuses the
//! same frames from a bare socket.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crdtsync_core::protocol::{Channel, PROTOCOL_VERSION};
use crdtsync_core::{
    decode_message, encode_header, encode_message, ClientId, Document, ErrorCode, MemberState,
    Message, Op, Scalar,
};
use crdtsync_server::membership::Membership;
use crdtsync_server::placement::NodeId;
use crdtsync_server::runtime::{serve_with, ServeConfig, MIN_CLUSTER_SECRET_LEN};
use crdtsync_server::{ConnId, ManualClock, Registry};

const CH: Channel = Channel(0);
const N: usize = 3;
const SELF_ADDR: &str = "10.0.0.6:9000";

/// The deployment's cluster secret — what every node in one cluster holds and
/// nobody else does.
const SECRET: &[u8] = b"cluster-secret-of-at-least-32-bytes";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

/// The static member set every node's view is built from — larger than the
/// replication factor, so a node follows some rooms and leads others.
fn members_str() -> String {
    (0..7)
        .map(|i| format!("10.0.0.{i}:9000"))
        .collect::<Vec<_>>()
        .join(",")
}

fn membership_for(self_addr: &str) -> Membership {
    Membership::from_static_config(None, Some(self_addr), &members_str(), N).unwrap()
}

/// A room self holds as a *follower* — the room a legitimate leader replicates to
/// this node, and the one an unauthenticated frame would land in.
fn room_self_follows(m: &Membership) -> Vec<u8> {
    for i in 0..1_000_000 {
        let room = format!("room-{i}").into_bytes();
        let r = m.replicas_for(&room);
        if r.len() >= 2 && !m.is_self(&r[0]) && r.iter().skip(1).any(|n| m.is_self(n)) {
            return room;
        }
    }
    panic!("no room places self as a follower");
}

/// A room self is the placement primary of — self leads it, so a forged epoch
/// above its own would step it down.
fn room_self_leads(m: &Membership) -> Vec<u8> {
    for i in 0..1_000_000 {
        let room = format!("room-{i}").into_bytes();
        if m.is_primary_for(&room) {
            return room;
        }
    }
    panic!("no room places self as primary");
}

/// A clustered registry with the cluster secret configured — the deployment a
/// peer can authenticate against.
fn registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for(SELF_ADDR));
    r.set_cluster_secret(SECRET.to_vec());
    r
}

/// A connection admitted to the peer plane — what a legitimate member's link is
/// once it has presented the secret.
fn peer(r: &mut Registry) -> ConnId {
    let id = r.connect();
    assert!(
        r.deliver(
            id,
            Message::PeerAuth {
                secret: SECRET.to_vec(),
            },
        ),
        "the cluster secret admits a peer",
    );
    id
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
            codecs: Vec::new(),
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

/// A leader's `Replicate` for `room`'s main stream at `epoch`, carrying one
/// register write. `writer`'s sequence advances so every frame carries a distinct
/// op id.
fn replicate(writer: &mut Document, room: &[u8], epoch: u64, key: &[u8], value: i64) -> Message {
    let ops = writer.transact(|tx| tx.register(key, Scalar::Int(value)));
    Message::Replicate {
        room: room.to_vec(),
        branch: b"main".to_vec(),
        ops,
        base_seq: 0,
        epoch,
    }
}

/// A whole-replica state transfer for `room` at `epoch`, carrying a real encoded
/// state so the frame would land if it were let through.
fn replicate_snapshot(room: &[u8], epoch: u64) -> Message {
    let mut writer = doc(9);
    writer.transact(|tx| tx.register(b"planted", Scalar::Int(1)));
    Message::ReplicateSnapshot {
        room: room.to_vec(),
        branch: b"main".to_vec(),
        seq: 1,
        state: writer.encode_state(),
        epoch,
    }
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

/// Whether the outbox holds a protocol-violation error — how the client plane
/// answers a node-to-node frame it has no claim to send.
fn refused_as_violation(msgs: &[Message]) -> bool {
    msgs.iter().any(|m| {
        matches!(
            m,
            Message::Error {
                code: ErrorCode::ProtocolViolation,
                ..
            }
        )
    })
}

// --- the attack: a bare socket pushing node-to-node frames ---

#[test]
fn a_bare_socket_cannot_replicate_into_a_room_the_node_follows() {
    // A connection that has sent no Hello, no Auth and no PeerAuth — the shape a
    // stranger who can reach the port opens — pushes a Replicate into a room this
    // node replicates. It is refused, and no op reaches the room's log.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();
    let stranger = r.connect();

    let kept = r.deliver(stranger, replicate(&mut doc(9), &room, 1, b"planted", 1));

    assert!(!kept, "an unauthenticated replicate drops the connection");
    assert_eq!(r.hub().seq(&room), 0, "no op reached the room's log");
    let out = r.take_outbox(stranger);
    assert!(
        refused_as_violation(&out),
        "the frame is answered as the client-plane violation it is, got {out:?}",
    );
    assert!(
        !out.iter().any(|m| matches!(m, Message::ReplicaAck { .. })),
        "a refused frame is never acked",
    );
}

#[test]
fn a_bare_sockets_replicate_reaches_no_subscriber() {
    // The whole point of the ingest seam: what lands in the replica is served to
    // ordinary readers. An unauthenticated write must reach none of them.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();

    let reader = client(&mut r);
    r.deliver(reader, sub(&room));
    r.take_outbox(reader);

    let stranger = r.connect();
    r.deliver(stranger, replicate(&mut doc(9), &room, 1, b"planted", 1));

    assert!(
        r.take_outbox(reader).is_empty(),
        "a subscriber was served an unauthenticated write",
    );

    // And a reader joining afterwards is not served it out of the log either.
    let latecomer = client(&mut r);
    r.deliver(latecomer, sub(&room));
    assert!(
        received_ops(r.take_outbox(latecomer)).is_empty(),
        "an unauthenticated write reached the room's log",
    );
}

#[test]
fn a_bare_socket_cannot_install_a_replica_snapshot() {
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();
    let stranger = r.connect();

    let kept = r.deliver(stranger, replicate_snapshot(&room, 1));

    assert!(!kept, "an unauthenticated snapshot drops the connection");
    assert_eq!(r.hub().seq(&room), 0, "no state was installed");
    assert!(refused_as_violation(&r.take_outbox(stranger)));
}

#[test]
fn a_bare_socket_cannot_gossip_membership() {
    // A gossip merge grows this node's member set and moves its liveness view —
    // enough to steer placement and leadership. It needs the peer plane too.
    let mut r = registry();
    let before = r.known_members().len();
    let stranger = r.connect();

    let kept = r.deliver(
        stranger,
        Message::Gossip {
            members: vec![(
                b"10.9.9.9:9000".to_vec(),
                b"10.9.9.9:9000".to_vec(),
                7,
                MemberState::Alive,
            )],
        },
    );

    assert!(!kept, "an unauthenticated gossip drops the connection");
    assert_eq!(
        r.known_members().len(),
        before,
        "an unauthenticated gossip joined a member to the cluster",
    );
    let out = r.take_outbox(stranger);
    assert!(refused_as_violation(&out));
    assert!(
        !out.iter().any(|m| matches!(m, Message::Gossip { .. })),
        "an unauthenticated gossip was answered with this node's member set",
    );
}

#[test]
fn a_bare_socket_cannot_report_follower_heads() {
    // A follower-heads report resets a follower's acked watermark and dials it a
    // catch-up — a durability claim and an outbound send, from a self-describing
    // frame anyone can compose.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);
    let mut r = registry();
    // Commit a write so the leader has a backlog a report could dial out.
    let author = client(&mut r);
    r.deliver(author, sub(&room));
    let ops = doc(1).transact(|tx| tx.register(b"k", Scalar::Int(1)));
    r.deliver(author, Message::Ops { channel: CH, ops });
    r.take_replication();

    // Name a node that really is one of the room's replicas, so an ungated report
    // would dial a catch-up — the assertion below is about the gate, not about the
    // reporter being unknown.
    let follower = m
        .replicas_for(&room)
        .into_iter()
        .find(|n| !m.is_self(n))
        .expect("the room has a follower");
    let stranger = r.connect();
    let kept = r.deliver(
        stranger,
        Message::FollowerHeads {
            reporter: follower.as_bytes().to_vec(),
            heads: vec![(room.clone(), 0)],
        },
    );

    assert!(!kept, "an unauthenticated head report drops the connection");
    assert!(
        r.take_replication().is_empty(),
        "an unauthenticated head report dialed a catch-up",
    );
    assert!(refused_as_violation(&r.take_outbox(stranger)));
}

#[test]
fn a_bare_socket_cannot_ask_for_a_liveness_opinion() {
    // A ping-req makes this node vouch for a member's liveness to whoever asked —
    // the SWIM signal that keeps a suspected node from being declared dead.
    let mut r = registry();
    let stranger = r.connect();

    let kept = r.deliver(
        stranger,
        Message::PingReq {
            target: b"10.0.0.1:9000".to_vec(),
        },
    );

    assert!(!kept, "an unauthenticated ping-req drops the connection");
    let out = r.take_outbox(stranger);
    assert!(
        !out.iter().any(|m| matches!(m, Message::PingAck { .. })),
        "an unauthenticated ping-req was answered with a liveness view",
    );
    assert!(refused_as_violation(&out));
}

#[test]
fn a_bare_socket_cannot_forge_an_epoch_to_step_the_leader_down() {
    // The same frame carries leadership: an epoch above the one this node leads at
    // supersedes its claim. From an unauthenticated socket that is a remote
    // leadership-stripping primitive, so the fence must never see the frame.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);
    let mut r = registry();

    // Lead the room: a committed client write claims and stamps this node's epoch.
    let author = client(&mut r);
    r.deliver(author, sub(&room));
    let mut w = doc(1);
    let ops = w.transact(|tx| tx.register(b"k", Scalar::Int(1)));
    r.deliver(author, Message::Ops { channel: CH, ops });
    let led_at = r.highest_epoch(&room);
    assert_eq!(led_at, 1, "the node leads the room at its first epoch");
    r.take_replication();

    let stranger = r.connect();
    let kept = r.deliver(stranger, replicate(&mut doc(9), &room, 99, b"forged", 9));

    assert!(!kept, "a forged-epoch replicate drops the connection");
    assert_eq!(
        r.highest_epoch(&room),
        led_at,
        "a forged epoch moved this node's leadership fence",
    );

    // Still leading at the same epoch: the next commit stamps it unchanged, rather
    // than opening a fresh generation above the forged one.
    let ops = w.transact(|tx| tx.register(b"k2", Scalar::Int(2)));
    r.deliver(author, Message::Ops { channel: CH, ops });
    let stamped = r
        .take_replication()
        .into_iter()
        .find_map(|(_, frame)| match frame {
            Message::Replicate { epoch, .. } => Some(epoch),
            _ => None,
        });
    assert_eq!(
        stamped,
        Some(led_at),
        "the node stepped down and re-claimed above the forged epoch",
    );
}

// --- the credential ---

#[test]
fn a_wrong_secret_admits_nothing() {
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();
    let stranger = r.connect();

    let kept = r.deliver(
        stranger,
        Message::PeerAuth {
            secret: b"cluster-secret-of-at-least-32-byteS".to_vec(),
        },
    );

    assert!(!kept, "a wrong secret drops the connection");
    assert!(
        r.take_outbox(stranger).is_empty(),
        "a rejected peer-auth is answered with nothing at all",
    );

    // The connection handle is stale, but a fresh one gets no further either.
    let retry = r.connect();
    r.deliver(
        retry,
        Message::PeerAuth {
            secret: b"wrong".to_vec(),
        },
    );
    assert!(!r.deliver(retry, replicate(&mut doc(9), &room, 1, b"planted", 1)));
    assert_eq!(r.hub().seq(&room), 0, "no op reached the room's log");
}

#[test]
fn an_empty_secret_admits_nothing() {
    // An omitted field decodes as an empty secret; it must never match, including
    // against a node whose own secret was set empty (which configures none).
    let mut r = registry();
    let stranger = r.connect();
    assert!(!r.deliver(stranger, Message::PeerAuth { secret: Vec::new() },));

    let mut unset = Registry::new(cid(0xFF));
    unset.set_clock(Arc::new(ManualClock::new(0)));
    unset.set_membership(membership_for(SELF_ADDR));
    unset.set_cluster_secret(Vec::new());
    let stranger = unset.connect();
    assert!(
        !unset.deliver(stranger, Message::PeerAuth { secret: Vec::new() },),
        "an empty secret configures no peer plane, so it opens none",
    );
}

#[test]
fn a_node_with_no_secret_has_no_peer_plane() {
    // The default. A clustered registry that was never given a secret refuses every
    // peer admission, so its node-to-node handlers are unreachable — it does not
    // replicate rather than replicating for anyone.
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for(SELF_ADDR));
    let room = room_self_follows(&membership_for(SELF_ADDR));

    let stranger = r.connect();
    assert!(
        !r.deliver(
            stranger,
            Message::PeerAuth {
                secret: SECRET.to_vec(),
            },
        ),
        "a node with no secret admits no peer",
    );

    let retry = r.connect();
    assert!(!r.deliver(retry, replicate(&mut doc(9), &room, 1, b"planted", 1)));
    assert_eq!(r.hub().seq(&room), 0);
}

/// The shape a single-node deployment actually runs as — no membership and no
/// secret, which is the only pairing `serve` allows outside a cluster. Its
/// counterparts in `replication.rs` / `gossip.rs` / `epoch_fence.rs` arm a secret on
/// a membership-less registry so they keep exercising the *cluster* gate; this one
/// covers the deployment those tests stopped representing.
#[test]
fn a_single_node_deployment_refuses_every_node_to_node_frame() {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));

    for frame in [
        replicate(&mut doc(9), b"any-room", 1, b"planted", 1),
        replicate_snapshot(b"any-room", 1),
        Message::Gossip {
            members: vec![(
                b"10.9.9.9:9000".to_vec(),
                b"10.9.9.9:9000".to_vec(),
                7,
                MemberState::Alive,
            )],
        },
        Message::FollowerHeads {
            reporter: b"10.9.9.9:9000".to_vec(),
            heads: vec![(b"any-room".to_vec(), 0)],
        },
        Message::PingReq {
            target: b"10.9.9.9:9000".to_vec(),
        },
    ] {
        let stranger = r.connect();
        assert!(
            !r.deliver(stranger, frame.clone()),
            "a single-node deployment kept a connection that sent {frame:?}",
        );
        assert!(
            refused_as_violation(&r.take_outbox(stranger)),
            "expected a protocol violation for {frame:?}",
        );
    }
    assert_eq!(
        r.hub().seq(b"any-room"),
        0,
        "nothing reached the room's log"
    );
    assert!(r.known_members().is_empty(), "no membership was created");
}

#[test]
fn an_authenticated_client_is_still_not_a_peer() {
    // Peer admission is its own credential, not a tier of the client one: a client
    // that completed Hello + Auth and subscribed the room reaches the peer handlers
    // no more than a bare socket does.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();
    let c = client(&mut r);
    r.deliver(c, sub(&room));
    r.take_outbox(c);

    let kept = r.deliver(c, replicate(&mut doc(9), &room, 1, b"planted", 1));

    assert!(!kept, "a client's replicate drops the connection");
    assert_eq!(r.hub().seq(&room), 0, "no op reached the room's log");
    assert!(refused_as_violation(&r.take_outbox(c)));
}

#[test]
fn peer_admission_confers_no_client_rights() {
    // The converse: holding the cluster secret opens the node-to-node plane and
    // nothing else. A peer connection has no session identity, so an ordinary op
    // write over it is refused exactly as an unauthenticated client's is.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();
    let p = peer(&mut r);

    // Reach the room the only way a client can — and be refused at the handshake.
    let kept = r.deliver(p, sub(&room));
    assert!(!kept, "a peer connection cannot subscribe as a client");
    assert!(refused_as_violation(&r.take_outbox(p)));
}

#[test]
fn peer_admission_is_per_connection() {
    // One member's link being admitted says nothing about any other socket.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();
    let admitted = peer(&mut r);
    let stranger = r.connect();

    assert!(!r.deliver(stranger, replicate(&mut doc(9), &room, 1, b"planted", 1)));
    assert_eq!(r.hub().seq(&room), 0, "no op reached the room's log");

    // The admitted link is unaffected by the stranger's refusal.
    assert!(r.deliver(admitted, replicate(&mut doc(8), &room, 1, b"real", 1)));
    assert_eq!(r.hub().seq(&room), 1, "the member's frame still applies");
}

// --- legitimate peer traffic still works ---

#[test]
fn an_admitted_peer_replicates_as_before() {
    // The C6 constraint: node-to-node links all Hello under the same reserved
    // replica id, so a gate keyed on the handshake would refuse replication itself.
    // Keyed on the secret, a member's link is admitted and every node-to-node frame
    // behaves exactly as it did.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();
    let p = peer(&mut r);
    let mut w = doc(9);

    assert!(r.deliver(p, replicate(&mut w, &room, 1, b"a", 1)));
    assert_eq!(r.hub().seq(&room), 1, "the leader's frame applied");
    let out = r.take_outbox(p);
    assert!(
        matches!(out.as_slice(), [Message::ReplicaAck { through_seq: 1, .. }]),
        "the follower acks the frame, got {out:?}",
    );

    // And an ordinary subscriber is served it.
    let reader = client(&mut r);
    r.deliver(reader, sub(&room));
    assert_eq!(
        received_ops(r.take_outbox(reader)).len(),
        1,
        "the replicated write is served to a reader",
    );
}

#[test]
fn an_admitted_peer_may_send_every_node_to_node_frame() {
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();

    let snap = peer(&mut r);
    assert!(r.deliver(snap, replicate_snapshot(&room, 1)));
    assert_eq!(r.hub().seq(&room), 1, "the state transfer installed");

    let gossiper = peer(&mut r);
    let before = r.known_members().len();
    assert!(r.deliver(
        gossiper,
        Message::Gossip {
            members: vec![(
                b"10.9.9.9:9000".to_vec(),
                b"10.9.9.9:9000".to_vec(),
                7,
                MemberState::Alive,
            )],
        },
    ));
    assert_eq!(
        r.known_members().len(),
        before + 1,
        "a member's gossip joins the advertised node",
    );
    assert!(r
        .take_outbox(gossiper)
        .iter()
        .any(|m| matches!(m, Message::Gossip { .. })));

    let prober = peer(&mut r);
    assert!(r.deliver(
        prober,
        Message::PingReq {
            target: b"10.0.0.1:9000".to_vec(),
        },
    ));
    assert!(r
        .take_outbox(prober)
        .iter()
        .any(|m| matches!(m, Message::PingAck { .. })));
}

#[test]
fn an_admitted_peers_head_report_still_dials_a_catch_up() {
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);
    let mut r = registry();
    let author = client(&mut r);
    r.deliver(author, sub(&room));
    let ops = doc(1).transact(|tx| tx.register(b"k", Scalar::Int(1)));
    r.deliver(author, Message::Ops { channel: CH, ops });
    r.take_replication();

    let follower = m
        .replicas_for(&room)
        .into_iter()
        .find(|n| !m.is_self(n))
        .expect("the room has a follower");
    let p = peer(&mut r);
    assert!(r.deliver(
        p,
        Message::FollowerHeads {
            reporter: follower.as_bytes().to_vec(),
            heads: vec![(room.clone(), 0)],
        },
    ));
    assert!(
        !r.take_replication().is_empty(),
        "a member's head report dials its catch-up",
    );
}

// --- deployment: a cluster is configured with a secret or does not start ---

/// Serve `config` and return the startup error it refuses with. A node that starts
/// serves forever, so the wait is bounded: an accepted misconfiguration reports as
/// a failed assertion rather than a hung test.
async fn startup_error(listener: TcpListener, config: ServeConfig) -> std::io::Error {
    tokio::time::timeout(
        Duration::from_secs(5),
        serve_with(listener, cid(0xFF), None, config),
    )
    .await
    .expect("the node came up instead of refusing the configuration")
    .expect_err("the node came up instead of refusing the configuration")
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback listener
async fn a_cluster_without_a_secret_refuses_to_start() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let err = startup_error(
        listener,
        ServeConfig {
            membership: Some(two_node_membership(&addr, "10.0.0.1:9000")),
            ..ServeConfig::default()
        },
    )
    .await;
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback listener
async fn a_short_secret_refuses_to_start() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let short = vec![b'x'; MIN_CLUSTER_SECRET_LEN - 1];
    let err = startup_error(
        listener,
        ServeConfig {
            membership: Some(two_node_membership(&addr, "10.0.0.1:9000")),
            cluster_secret: Some(short),
            ..ServeConfig::default()
        },
    )
    .await;
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback listener
async fn a_secret_without_a_cluster_refuses_to_start() {
    // The misconfiguration read the other way: a single-node deployment has no peer
    // plane, so a secret there is a cluster the operator meant to configure.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let err = startup_error(
        listener,
        ServeConfig {
            cluster_secret: Some(SECRET.to_vec()),
            ..ServeConfig::default()
        },
    )
    .await;
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

// --- over real sockets ---

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// A two-member cluster, self chosen by `me`, at replication factor 2 — a room's
/// replica set is the primary plus one follower.
fn two_node_membership(me: &str, other: &str) -> Membership {
    Membership::from_static_config(Some(me), None, other, 2).unwrap()
}

fn clustered(me: &str, other: &str) -> ServeConfig {
    ServeConfig {
        membership: Some(two_node_membership(me, other)),
        cluster_secret: Some(SECRET.to_vec()),
        ..ServeConfig::default()
    }
}

async fn send_frame(ws: &mut Ws, msg: &Message) {
    ws.send(WsMessage::Binary(encode_message(msg)))
        .await
        .unwrap();
}

/// Open a bare socket to `addr` and send the 8-byte header — the whole of what a
/// stranger who can reach the port has to do before a node-to-node frame.
async fn open_bare(addr: &str) -> Ws {
    let (mut ws, _) = connect_async(format!("ws://{addr}/")).await.unwrap();
    ws.send(WsMessage::Binary(encode_header(PROTOCOL_VERSION).to_vec()))
        .await
        .unwrap();
    ws
}

/// Open the client end: the header, then Hello + Auth, draining the AuthOk.
async fn open_client(addr: &str) -> Ws {
    let mut ws = open_bare(addr).await;
    send_frame(
        &mut ws,
        &Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
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
    loop {
        if let WsMessage::Binary(b) = ws.next().await.unwrap().unwrap() {
            if matches!(decode_message(&b), Ok(Message::AuthOk { .. })) {
                break;
            }
        }
    }
    ws
}

/// Subscribe `ws` to `room` and return the reply that settled it — the catch-up
/// (`Ops`/`Snapshot`) when this node serves the room, or a `Redirect` when it
/// declines to (an uncaught-up follower does). Bounded, so a caller never hangs.
async fn subscribe(ws: &mut Ws, room: &[u8]) -> Option<Message> {
    send_frame(
        ws,
        &Message::Subscribe {
            channel: CH,
            room: room.to_vec(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq: 0,
        },
    )
    .await;
    next_matching(ws, Duration::from_secs(10), |m| {
        matches!(
            m,
            Message::Ops { .. } | Message::Snapshot { .. } | Message::Redirect { .. }
        )
    })
    .await
}

/// The next message on `ws` matching `want` within `within`, or `None` — the
/// bounded poll a negative assertion needs so it never hangs.
async fn next_matching(
    ws: &mut Ws,
    within: Duration,
    want: impl Fn(&Message) -> bool,
) -> Option<Message> {
    tokio::time::timeout(within, async {
        loop {
            match ws.next().await {
                Some(Ok(WsMessage::Binary(b))) => match decode_message(&b) {
                    Ok(msg) if want(&msg) => return Some(msg),
                    _ => continue,
                },
                Some(Ok(_)) => continue,
                _ => return None,
            }
        }
    })
    .await
    .ok()
    .flatten()
}

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
async fn two_real_nodes_still_replicate_end_to_end() {
    // The end-to-end pin: a real leader dials a real follower, presents the secret
    // on that link, and a client's write travels the whole path — leader ingest,
    // peer link, follower replica — so a subscriber on the *follower* reads it.
    let follower_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let follower_addr = follower_listener.local_addr().unwrap().to_string();
    let leader_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let leader_addr = leader_listener.local_addr().unwrap().to_string();
    let room = room_led_by(&leader_addr, &follower_addr);

    let follower = tokio::spawn(serve_with(
        follower_listener,
        cid(0xF0),
        None,
        clustered(&follower_addr, &leader_addr),
    ));
    let leader = tokio::spawn(serve_with(
        leader_listener,
        cid(0xFF),
        None,
        clustered(&leader_addr, &follower_addr),
    ));

    let mut writer = open_client(&leader_addr).await;
    subscribe(&mut writer, &room).await;
    send_frame(
        &mut writer,
        &Message::Ops {
            channel: CH,
            ops: doc(1).transact(|tx| tx.register(b"k", Scalar::Int(1))),
        },
    )
    .await;

    // The leader withholds the client's Accepted until a majority — here, the
    // follower — holds the write, so this arriving is itself proof the peer link
    // was admitted and the replicated frame applied.
    assert!(
        next_matching(&mut writer, Duration::from_secs(10), |m| matches!(
            m,
            Message::Accepted { .. }
        ))
        .await
        .is_some(),
        "the leader never released the write's Accepted — replication did not complete",
    );

    // And the follower's own replica holds it: a reader there is served the write
    // out of the replicated copy rather than redirected to the leader.
    let mut reader = open_client(&follower_addr).await;
    let served = subscribe(&mut reader, &room).await;
    match served {
        Some(Message::Ops { ops, .. }) => {
            assert_eq!(ops.len(), 1, "the follower served the replicated write")
        }
        other => panic!("the follower did not serve the replicated write: {other:?}"),
    }

    leader.abort();
    follower.abort();
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
async fn two_real_nodes_with_different_secrets_do_not_replicate() {
    // The counterfactual to the test above, and the thing an operator most needs to
    // be true: a node whose secret does not match the cluster's is refused at the
    // peer plane exactly as a stranger is. The leader's dial is accepted at the TCP
    // level and then dropped, so the write never reaches a majority and its Accepted
    // is never released — the same fail-closed shape as having no follower at all.
    let follower_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let follower_addr = follower_listener.local_addr().unwrap().to_string();
    let leader_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let leader_addr = leader_listener.local_addr().unwrap().to_string();
    let room = room_led_by(&leader_addr, &follower_addr);

    let mut odd_one_out = clustered(&follower_addr, &leader_addr);
    // Long enough to start with, and not this cluster's — derived from the floor so
    // a shortened literal cannot silently turn this into a failed-startup test.
    let other_secret = vec![b'z'; MIN_CLUSTER_SECRET_LEN];
    assert_ne!(other_secret, SECRET, "the follower's secret must differ");
    odd_one_out.cluster_secret = Some(other_secret);
    let follower = tokio::spawn(serve_with(follower_listener, cid(0xF0), None, odd_one_out));
    let leader = tokio::spawn(serve_with(
        leader_listener,
        cid(0xFF),
        None,
        clustered(&leader_addr, &follower_addr),
    ));

    let mut writer = open_client(&leader_addr).await;
    // The leader really does lead the room and serve the write — so the missing
    // Accepted below isolates to replication, not to a redirect or a refused write.
    let served = subscribe(&mut writer, &room).await;
    assert!(
        matches!(served, Some(Message::Ops { .. } | Message::Snapshot { .. })),
        "the leader did not serve the room it leads, got {served:?}",
    );
    send_frame(
        &mut writer,
        &Message::Ops {
            channel: CH,
            ops: doc(1).transact(|tx| tx.register(b"k", Scalar::Int(1))),
        },
    )
    .await;
    // The leader ingested it locally: a second reader there is served it. So the
    // missing Accepted below is the follower's absence, not a write that never
    // landed.
    let mut on_leader = open_client(&leader_addr).await;
    let served = subscribe(&mut on_leader, &room).await;
    match served {
        Some(Message::Ops { ops, .. }) => assert_eq!(
            ops.len(),
            1,
            "the leader never ingested the write — the test is not measuring replication",
        ),
        other => panic!("the leader did not serve its own committed write: {other:?}"),
    }

    assert!(
        next_matching(&mut writer, Duration::from_secs(5), |m| matches!(
            m,
            Message::Accepted { .. }
        ))
        .await
        .is_none(),
        "the leader acked a write a mismatched-secret follower cannot have held",
    );

    // And nothing reached the follower's replica: a reader there is served an empty
    // room (or redirected to the leader), never the write the leader committed.
    let mut reader = open_client(&follower_addr).await;
    let served = subscribe(&mut reader, &room).await;
    match served {
        Some(Message::Ops { ops, .. }) => assert!(
            ops.is_empty(),
            "the write reached a follower that never presented the cluster secret",
        ),
        Some(Message::Redirect { .. }) | None => {}
        other => panic!("unexpected subscribe reply: {other:?}"),
    }

    leader.abort();
    follower.abort();
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials a loopback server over a real socket
async fn a_real_node_refuses_a_bare_sockets_replicate() {
    // The reproduction over the wire: a socket that has sent nothing but the header
    // pushes a Replicate at a swept epoch into a room the node replicates. The node
    // answers a protocol violation and closes, and no ack ever comes back.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let peer_addr = "10.0.0.1:9000";
    let room = room_led_by(peer_addr, &addr);
    let node = tokio::spawn(serve_with(
        listener,
        cid(0xFF),
        None,
        clustered(&addr, peer_addr),
    ));

    let mut ws = open_bare(&addr).await;
    send_frame(&mut ws, &replicate(&mut doc(9), &room, 1, b"planted", 1)).await;

    let reply = next_matching(&mut ws, Duration::from_secs(5), |m| {
        matches!(m, Message::Error { .. } | Message::ReplicaAck { .. })
    })
    .await;
    assert!(
        matches!(
            reply,
            Some(Message::Error {
                code: ErrorCode::ProtocolViolation,
                ..
            })
        ),
        "expected a protocol violation, got {reply:?}",
    );

    node.abort();
}
