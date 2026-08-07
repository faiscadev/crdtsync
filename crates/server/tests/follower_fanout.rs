//! A replicated op reaches a follower's local subscribers, not just its replica
//! (C59).
//!
//! A follower is an ordinary read-serving node — a caught-up one serves a
//! Subscribe from its own replica (`follower_reads.rs`) — so a client subscribed
//! there is subscribed to a stream the leader is the sole author of. The
//! replication apply path ingests the leader's batch and acks it; if that is
//! where the op stops, the client's replica silently stops advancing over every
//! write the leader commits while the follower's own state moves on beneath it,
//! self-healing no earlier than a reconnect (whose `resume` draws the missed span
//! out of the log).
//!
//! What the fan-out from that path owes:
//!
//! - Every channel subscribed to the stream receives it. The batch was authored
//!   on the leader, so no replica here already holds it and the exclusion set is
//!   empty — unlike a local write, which omits its own authoring channel (C5).
//! - The recipient's seen sequence advances over it, so its next resume asks for
//!   the right span rather than replaying what it already folded.
//! - Every per-recipient verdict is re-decided here, against this replica. The
//!   leader computed verdicts for *its own* subscribers and the wire frame carries
//!   none of them, so a subtree a follower-local reader may not read must not leak
//!   through this seam any more than through the write path's.
//!
//! Two in-process registries — a leader and a follower over one static cluster,
//! no socket — as in `follower_reads.rs`: the leader commits, its replication
//! frames are handed to the follower, and the follower's own subscribers are then
//! inspected. Deterministic and Miri-clean.

use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::client::ClientSession;
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{
    AclEffect, ClientId, Document, Element, ElementId, ElementKind, Message, Op, OpKind, Scalar,
    Schema,
};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::membership::Membership;
use crdtsync_server::placement::NodeId;
use crdtsync_server::{
    Action, ConnId, Identity, ManualClock, Registry, Resource, SchemaRegistry, StaticTokens,
};

const CH: Channel = Channel(0);
const N: usize = 3;
const A: &str = "10.0.0.1:9000";
const B: &str = "10.0.0.2:9000";
const CRED: &[u8] = b"cred";

/// The cluster secret these nodes share — what admits a node-to-node link to a
/// peer's replication plane.
const CLUSTER_SECRET: &[u8] = b"peer-plane-cluster-secret-for-tests";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn members() -> String {
    (1..=5)
        .map(|i| format!("10.0.0.{i}:9000"))
        .collect::<Vec<_>>()
        .join(",")
}

fn membership_for(self_addr: &str) -> Membership {
    Membership::from_static_config(None, Some(self_addr), &members(), N).unwrap()
}

/// A registry whose self is `self_addr`, on the shared cluster.
fn node(self_addr: &str) -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for(self_addr));
    r.set_cluster_secret(CLUSTER_SECRET.to_vec());
    r
}

/// A room `A` leads and `B` is a non-primary replica of.
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

/// A connection admitted to `r`'s peer plane as the room's leader `A` — the link
/// a leader's dial establishes, and the identity every frame on it is decided
/// against.
fn peer_conn(r: &mut Registry) -> ConnId {
    let id = r.connect();
    assert!(
        r.deliver(
            id,
            Message::PeerAuth {
                node: NodeId::from_addr(A).as_bytes().to_vec(),
                secret: CLUSTER_SECRET.to_vec(),
            },
        ),
        "the cluster secret admits a peer",
    );
    id
}

/// Hand every replication frame the leader queued for `B` to `follower`'s peer
/// link, returning how many it applied.
fn replicate(leader: &mut Registry, follower: &mut Registry, peer: ConnId) -> usize {
    let b = NodeId::from_addr(B);
    let mut applied = 0;
    for (node, frame) in leader.take_replication() {
        if node == b {
            assert!(
                follower.deliver(peer, frame),
                "the follower applies a frame"
            );
            applied += 1;
        }
    }
    applied
}

/// A connection on `r` driven by a real client session, handshake drained, so its
/// seen sequence and replica are the client's own rather than the test's
/// bookkeeping.
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

/// A subscribed client session on `r`'s `room`, its catch-up folded in.
fn subscriber(r: &mut Registry, room: &[u8], id: u8) -> (ConnId, ClientSession, Channel) {
    let (conn, mut session) = client(r, cid(id));
    let (channel, sub) = session.subscribe(room);
    assert!(r.deliver(conn, sub), "the subscribe is served");
    pump(r, conn, &mut session);
    (conn, session, channel)
}

/// The integer a channel's replica holds at top-level `key`.
fn reg(session: &ClientSession, channel: Channel, key: &[u8]) -> Option<i64> {
    match session.document(channel)?.get(key)? {
        Element::Register(r) => match r.borrow().read() {
            Scalar::Int(n) => Some(*n),
            other => panic!("expected an Int, got {other:?}"),
        },
        _ => panic!("expected a Register"),
    }
}

/// A subscribed authoring client on `leader`'s `room`, plus the doc it authors
/// from — the room's creator, since its first write establishes it.
fn author(leader: &mut Registry, room: &[u8]) -> (ConnId, Document) {
    author_as(leader, room, b"t-alice", &[], 0)
}

/// `author`, declaring `{app, version}` and authenticating as `credential`.
fn author_as(
    leader: &mut Registry,
    room: &[u8],
    credential: &[u8],
    app: &[u8],
    version: u32,
) -> (ConnId, Document) {
    let conn = leader.connect();
    assert!(leader.deliver(
        conn,
        Message::Hello {
            client: cid(1),
            app_id: app.to_vec(),
            schema_version: version,
            codecs: Vec::new(),
        }
    ));
    assert!(leader.deliver(
        conn,
        Message::Auth {
            credential: credential.to_vec(),
        }
    ));
    assert!(leader.deliver(
        conn,
        Message::Subscribe {
            channel: CH,
            room: room.to_vec(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq: 0,
        }
    ));
    leader.take_outbox(conn);
    (conn, Document::new(cid(1)))
}

fn submit(r: &mut Registry, conn: ConnId, ops: Vec<Op>) {
    assert!(
        r.deliver(conn, Message::Ops { channel: CH, ops }),
        "the write is accepted",
    );
}

/// The ops `msgs` carried, flattened across their `Ops` frames.
fn ops_in(msgs: &[Message]) -> Vec<Op> {
    msgs.iter()
        .filter_map(|m| match m {
            Message::Ops { ops, .. } => Some(ops.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

// --- the ordinary shape: a follower's subscriber advances over the leader's writes ---

#[test]
fn a_follower_subscriber_receives_a_replicated_write_without_reconnecting() {
    let room = room_led_by_a_with_b_follower();
    let mut leader = node(A);
    let mut follower = node(B);
    let peer = peer_conn(&mut follower);

    // The room exists on both nodes before the subscriber joins, so the write
    // under test is one the follower takes purely on the replication path.
    let (writer, mut doc) = author(&mut leader, &room);
    submit(
        &mut leader,
        writer,
        doc.transact(|tx| tx.register(b"seed", Scalar::Int(0))),
    );
    assert_eq!(replicate(&mut leader, &mut follower, peer), 1);

    let (conn, mut session, channel) = subscriber(&mut follower, &room, 9);
    assert_eq!(
        reg(&session, channel, b"seed"),
        Some(0),
        "the follower served the catch-up it already held",
    );

    // The leader commits; the follower ingests the frame and must serve it on.
    submit(
        &mut leader,
        writer,
        doc.transact(|tx| tx.register(b"k", Scalar::Int(7))),
    );
    assert_eq!(replicate(&mut leader, &mut follower, peer), 1);
    pump(&mut follower, conn, &mut session);

    assert_eq!(
        reg(&session, channel, b"k"),
        Some(7),
        "a client subscribed on a follower never saw the leader's committed write",
    );
}

#[test]
fn a_follower_subscribers_seen_sequence_advances_over_the_replicated_write() {
    let room = room_led_by_a_with_b_follower();
    let mut leader = node(A);
    let mut follower = node(B);
    let peer = peer_conn(&mut follower);

    let (writer, mut doc) = author(&mut leader, &room);
    submit(
        &mut leader,
        writer,
        doc.transact(|tx| tx.register(b"seed", Scalar::Int(0))),
    );
    assert_eq!(replicate(&mut leader, &mut follower, peer), 1);

    let (conn, mut session, channel) = subscriber(&mut follower, &room, 9);
    let before = session
        .last_seen_seq(channel)
        .expect("a subscribed channel");

    for i in 0..3 {
        submit(
            &mut leader,
            writer,
            doc.transact(|tx| tx.register(format!("k{i}").as_bytes(), Scalar::Int(i))),
        );
    }
    assert_eq!(replicate(&mut leader, &mut follower, peer), 3);
    pump(&mut follower, conn, &mut session);

    assert_eq!(
        session.last_seen_seq(channel),
        Some(before + 3),
        "the seen sequence stalled over the replicated span",
    );
    assert_eq!(
        session.last_seen_seq(channel),
        Some(follower.hub().seq(&room)),
        "the subscriber is at the follower's own watermark, so its next resume asks \
         for the span after it rather than replaying what it holds",
    );
}

#[test]
fn every_channel_subscribed_on_the_follower_receives_the_replicated_write() {
    // No local replica authored a replicated batch, so the exclusion set is empty
    // — including across two channels of one connection, which a local write would
    // have skipped one of (C5).
    let room = room_led_by_a_with_b_follower();
    let mut leader = node(A);
    let mut follower = node(B);
    let peer = peer_conn(&mut follower);

    let (writer, mut doc) = author(&mut leader, &room);
    submit(
        &mut leader,
        writer,
        doc.transact(|tx| tx.register(b"seed", Scalar::Int(0))),
    );
    assert_eq!(replicate(&mut leader, &mut follower, peer), 1);

    let (conn, mut session) = client(&mut follower, cid(9));
    let (first, sub_first) = session.subscribe(&room);
    let (second, sub_second) = session.subscribe(&room);
    assert!(follower.deliver(conn, sub_first));
    assert!(follower.deliver(conn, sub_second));
    pump(&mut follower, conn, &mut session);
    let (other_conn, mut other, other_channel) = subscriber(&mut follower, &room, 8);

    submit(
        &mut leader,
        writer,
        doc.transact(|tx| tx.register(b"k", Scalar::Int(7))),
    );
    assert_eq!(replicate(&mut leader, &mut follower, peer), 1);
    pump(&mut follower, conn, &mut session);
    pump(&mut follower, other_conn, &mut other);

    for channel in [first, second] {
        assert_eq!(
            reg(&session, channel, b"k"),
            Some(7),
            "{channel:?} of the multiplexing connection missed the replicated write",
        );
    }
    assert_eq!(
        reg(&other, other_channel, b"k"),
        Some(7),
        "the peer connection missed the replicated write",
    );
}

#[test]
fn a_resent_replication_frame_fans_nothing_out_twice() {
    // The fan-out carries the ops the ingest newly applied, so a redelivered frame
    // — the repair path a dropped peer link takes — costs the subscriber nothing.
    let room = room_led_by_a_with_b_follower();
    let mut leader = node(A);
    let mut follower = node(B);
    let peer = peer_conn(&mut follower);

    let (writer, mut doc) = author(&mut leader, &room);
    submit(
        &mut leader,
        writer,
        doc.transact(|tx| tx.register(b"seed", Scalar::Int(0))),
    );
    let frames: Vec<Message> = leader
        .take_replication()
        .into_iter()
        .filter(|(node, _)| *node == NodeId::from_addr(B))
        .map(|(_, frame)| frame)
        .collect();
    for frame in &frames {
        assert!(follower.deliver(peer, frame.clone()));
    }

    let (conn, mut session, channel) = subscriber(&mut follower, &room, 9);
    let at_join = session
        .last_seen_seq(channel)
        .expect("a subscribed channel");

    for frame in &frames {
        assert!(follower.deliver(peer, frame.clone()), "a resend is applied");
    }
    let out = pump(&mut follower, conn, &mut session);

    assert!(
        ops_in(&out).is_empty(),
        "a redelivered frame fanned ops out again: {out:?}",
    );
    assert_eq!(
        session.last_seen_seq(channel),
        Some(at_join),
        "the seen sequence ran past the follower's watermark on a resend",
    );
}

#[test]
fn a_fenced_frame_fans_nothing_out() {
    // A frame the replica gate refuses never reached the replica, so it must not
    // reach a subscriber either — the fan-out sits behind the gate, not beside it.
    let room = room_led_by_a_with_b_follower();
    let mut leader = node(A);
    let mut follower = node(B);
    let peer = peer_conn(&mut follower);

    let (writer, mut doc) = author(&mut leader, &room);
    submit(
        &mut leader,
        writer,
        doc.transact(|tx| tx.register(b"seed", Scalar::Int(0))),
    );
    assert_eq!(replicate(&mut leader, &mut follower, peer), 1);

    let (conn, mut session, channel) = subscriber(&mut follower, &room, 9);
    let at_join = session
        .last_seen_seq(channel)
        .expect("a subscribed channel");

    // The same batch re-framed at epoch 0 — below the epoch the follower has seen,
    // so a demoted leader's write is fenced.
    let stale = doc.transact(|tx| tx.register(b"ghost", Scalar::Int(1)));
    assert!(follower.deliver(
        peer,
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops: stale,
            base_seq: 0,
            epoch: 0,
            creator: None,
        },
    ));
    let out = pump(&mut follower, conn, &mut session);

    assert!(
        ops_in(&out).is_empty(),
        "a fenced frame reached a subscriber: {out:?}",
    );
    assert_eq!(session.last_seen_seq(channel), Some(at_join));
    assert!(
        follower.hub().get(&room, b"ghost").is_none(),
        "a fenced frame reached the replica",
    );
}

// --- redaction: the per-recipient verdict is re-decided on this seam ---

const ALICE: &str = "t-alice";
const BOB: &str = "t-bob";

/// A registry on the shared cluster whose deployment permits `alice` (the room
/// creator, who must subscribe to bootstrap it) to read and write `room`, and
/// **abstains on every other actor's read** — so bob's verdict is the doc-ACL
/// tier's alone and the per-path redaction actually bites.
fn acl_node(self_addr: &str, room: &[u8]) -> Registry {
    let mut r = node(self_addr);
    let mut tokens = StaticTokens::new();
    tokens.insert(ALICE.as_bytes().to_vec(), b"alice".to_vec());
    tokens.insert(BOB.as_bytes().to_vec(), b"bob".to_vec());
    r.set_verifier(Box::new(tokens));
    r.set_authorizer(Box::new(
        Acl::new()
            .allow(
                Subject::Actor(b"alice".to_vec()),
                Some(Action::Read),
                ResourceMatch::Room(room.to_vec()),
            )
            .allow(
                Subject::Actor(b"alice".to_vec()),
                Some(Action::Write),
                ResourceMatch::Room(room.to_vec()),
            ),
    ));
    r
}

/// A write into the top-level subtree `key` — a nested map holding one register,
/// so the batch is a `MapCreate` at `/key` plus a `RegisterSet` at `/key/v`, both
/// governed by a read grant on `/key`.
fn write_subtree(doc: &mut Document, key: &[u8], v: i64) -> Vec<Op> {
    doc.transact(|tx| {
        tx.map(key).register(b"v", Scalar::Int(v));
    })
}

/// The derived map id of a top-level subtree.
fn subtree_id(key: &[u8]) -> ElementId {
    ElementId::derive(Document::new(cid(0)).root_id(), key, ElementKind::Map)
}

/// Whether `ops` mutate the top-level subtree `key`.
fn touches_subtree(ops: &[Op], key: &[u8]) -> bool {
    let map_id = subtree_id(key);
    ops.iter().any(|op| match &op.kind {
        OpKind::MapCreate { key: k } => k == key,
        _ => op.target == map_id,
    })
}

#[test]
fn the_replication_fan_out_still_redacts_a_denied_subtree() {
    let room = room_led_by_a_with_b_follower();
    let mut leader = acl_node(A, &room);
    let mut follower = acl_node(B, &room);
    let peer = peer_conn(&mut follower);

    // alice bootstraps the room on the leader, writes both subtrees, and grants bob
    // read on `/a` alone.
    let (writer, mut doc) = author(&mut leader, &room);
    submit(&mut leader, writer, write_subtree(&mut doc, b"a", 1));
    submit(&mut leader, writer, write_subtree(&mut doc, b"b", 1));
    submit(
        &mut leader,
        writer,
        doc.transact(|tx| {
            tx.acl().grant(
                AclSubject::Actor(actor_key(b"bob")),
                AclGrant::Capability(Capability::Read),
                AclEffect::Allow,
                encode_path(&[b"a"]),
                actor_key(b"alice"),
            );
        }),
    );
    replicate(&mut leader, &mut follower, peer);

    // bob subscribes on the follower — a partial reader of `/a`.
    let conn = follower.connect();
    let mut session = ClientSession::new(cid(9));
    assert!(follower.deliver(conn, session.hello()));
    assert!(follower.deliver(conn, session.auth(BOB.as_bytes())));
    pump(&mut follower, conn, &mut session);
    let (_, sub) = session.subscribe(&room);
    assert!(follower.deliver(conn, sub), "bob's follower read is served");
    pump(&mut follower, conn, &mut session);

    // alice writes into both subtrees on the leader; the follower replicates both.
    submit(&mut leader, writer, write_subtree(&mut doc, b"a", 2));
    submit(&mut leader, writer, write_subtree(&mut doc, b"b", 2));
    replicate(&mut leader, &mut follower, peer);
    let out = pump(&mut follower, conn, &mut session);
    let received = ops_in(&out);

    assert!(
        touches_subtree(&received, b"a"),
        "bob's granted subtree was withheld on the replication fan-out: {out:?}",
    );
    assert!(
        !touches_subtree(&received, b"b"),
        "a denied subtree leaked through the replication fan-out: {out:?}",
    );
}

#[test]
fn a_wholly_denied_reader_gets_no_replication_frame_at_all() {
    // bob reads nothing in the room — the deployment abstains on him and he is the
    // subject of no grant — so the fan-out sends him nothing at all rather than an
    // empty frame. The whole-document gate and the per-path verdict both refuse
    // him, which is the point: neither of the two dispatch arms may deliver.
    let room = room_led_by_a_with_b_follower();
    let mut leader = acl_node(A, &room);
    let mut follower = acl_node(B, &room);
    let peer = peer_conn(&mut follower);

    let (writer, mut doc) = author(&mut leader, &room);
    submit(&mut leader, writer, write_subtree(&mut doc, b"a", 1));
    // A grant on `/a` bob is not the subject of, so the room holds live doc-ACL
    // tuples and the fan-out dispatches to the redacting arm.
    submit(
        &mut leader,
        writer,
        doc.transact(|tx| {
            tx.acl().grant(
                AclSubject::Actor(actor_key(b"carol")),
                AclGrant::Capability(Capability::Read),
                AclEffect::Allow,
                encode_path(&[b"a"]),
                actor_key(b"alice"),
            );
        }),
    );
    replicate(&mut leader, &mut follower, peer);

    let conn = follower.connect();
    let mut session = ClientSession::new(cid(9));
    assert!(follower.deliver(conn, session.hello()));
    assert!(follower.deliver(conn, session.auth(BOB.as_bytes())));
    pump(&mut follower, conn, &mut session);
    let (_, sub) = session.subscribe(&room);
    let subscribed = follower.deliver(conn, sub);
    follower.take_outbox(conn);
    assert!(subscribed, "the connection survives bob's subscribe");

    submit(&mut leader, writer, write_subtree(&mut doc, b"a", 2));
    replicate(&mut leader, &mut follower, peer);

    let out = follower.take_outbox(conn);
    assert!(
        ops_in(&out).is_empty(),
        "an unreadable subtree reached a denied reader through replication: {out:?}",
    );
}

// --- zones: the per-channel zone filter still narrows on this seam ---

const APP: &[u8] = b"z";

/// Two zoned map subtrees (`/board` → za, `/notes` → zb) over one root partition.
const ZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "board": "Sect", "notes": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "zones": { "za": "/board", "zb": "/notes" }
}"#;

/// Every actor may read the room — zone gating alone carves the isolation. The
/// author writes and reaches both zones; `za` reaches zone za and nothing else.
fn zone_authorizer(id: &Identity, action: Action, res: &Resource) -> bool {
    match res {
        Resource::Zone { zone, .. } => {
            let zone: &[u8] = zone;
            match id.actor() {
                b"author" => true,
                b"za" => zone == b"za",
                _ => false,
            }
        }
        _ => matches!(action, Action::Read) || id.actor() == b"author",
    }
}

/// A node on the shared cluster serving the zoned app.
fn zone_node(self_addr: &str) -> Registry {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, ZONED.as_bytes(), b"").unwrap();
    let mut tokens = StaticTokens::new();
    tokens.insert(b"c-author".to_vec(), b"author".to_vec());
    tokens.insert(b"c-za".to_vec(), b"za".to_vec());
    let mut r = node(self_addr);
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens));
    r.set_authorizer(Box::new(zone_authorizer));
    r
}

/// A content write into the zoned subtree `key` — a pure zoned `RegisterSet`, the
/// container already existing in the author's doc.
fn zoned_write(doc: &mut Document, container: &[u8], key: &[u8], v: i64) -> Vec<Op> {
    doc.transact(|tx| {
        tx.map(container).register(key, Scalar::Int(v));
    })
}

/// Whether `ops` carry a `RegisterSet` of `key` — the marker each write leaves.
fn has_key(ops: &[Op], key: &[u8]) -> bool {
    ops.iter()
        .any(|op| matches!(&op.kind, OpKind::RegisterSet { key: k, .. } if k == key))
}

#[test]
fn the_replication_fan_out_narrows_to_the_channels_authorized_zones() {
    let room = room_led_by_a_with_b_follower();
    let mut leader = zone_node(A);
    let mut follower = zone_node(B);
    let peer = peer_conn(&mut follower);

    // The author bootstraps the room and creates both zone containers, so a later
    // write into either is pure zoned content.
    let (writer, mut doc) = author_as(&mut leader, &room, b"c-author", APP, 1);
    doc.set_schema(Schema::parse(ZONED).expect("the zoned schema parses"));
    submit(
        &mut leader,
        writer,
        doc.transact(|tx| {
            tx.map(b"board").register(b"bseed", Scalar::Int(1));
            tx.map(b"notes").register(b"nseed", Scalar::Int(1));
        }),
    );
    replicate(&mut leader, &mut follower, peer);

    // A za-scoped subscriber on the follower. Its subscribe is also what binds the
    // room's governing app there, since replication carries the creator and not the
    // binding (C62).
    let conn = follower.connect();
    assert!(follower.deliver(
        conn,
        Message::Hello {
            client: cid(9),
            app_id: APP.to_vec(),
            schema_version: 1,
            codecs: Vec::new(),
        }
    ));
    assert!(follower.deliver(
        conn,
        Message::Auth {
            credential: b"c-za".to_vec(),
        }
    ));
    assert!(follower.deliver(
        conn,
        Message::Subscribe {
            channel: CH,
            room: room.clone(),
            branch: Vec::new(),
            zone: b"za".to_vec(),
            last_seen_seq: 0,
        }
    ));
    follower.take_outbox(conn);

    // One write into each zone on the leader, replicated together.
    submit(
        &mut leader,
        writer,
        zoned_write(&mut doc, b"notes", b"nk", 2),
    );
    submit(
        &mut leader,
        writer,
        zoned_write(&mut doc, b"board", b"bk", 2),
    );
    replicate(&mut leader, &mut follower, peer);

    let received = ops_in(&follower.take_outbox(conn));
    assert!(
        has_key(&received, b"bk"),
        "the za-scoped channel lost its own zone's replicated write: {received:?}",
    );
    assert!(
        !has_key(&received, b"nk"),
        "an unauthorized zone surfaced through the replication fan-out: {received:?}",
    );
}
