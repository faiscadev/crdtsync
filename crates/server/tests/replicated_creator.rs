//! A room's creator is replicated metadata — a replica that holds a room holds its
//! doc-ACL authority root (C29).
//!
//! A creatorless room has no authority root: `reads_whole_document` short-circuits
//! `true` and `doc_acl_read_at` abstains, so every doc-ACL deny that rode the log
//! decides nothing — while `acl_records` is non-empty, because ACL ops ride the log
//! like any other. Follower reads let a caught-up follower serve a read from its own
//! replica, so what a replica holds decides what a partial reader landing there is
//! served, out of every seam that serves a state blob: the op catch-up, the snapshot
//! catch-up, and the version fetch alike. These pin that a replicated room carries
//! its root, and that each of those three seams narrows by it.
//!
//! The schema tier is what makes the gap observable. It grants root read to any
//! authenticated actor, so bob passes the room gate on both nodes, while alice's
//! doc-ACL `Deny(Read)` at `/secret` carves that key out. Neither other tier
//! separates the two nodes: a deployment read-allow short-circuits the
//! whole-document check on the leader too, and a doc-ACL-only grant leaves bob
//! *refused* on a creatorless follower rather than over-served.
//!
//! Two in-process registries over one static cluster, no socket and a fixed clock —
//! the leader commits, its replication frames are handed to the follower, and a
//! client then reads the follower directly. Deterministic, Miri-clean.

use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{AclEffect, ClientId, Document, Message, Op, Scalar};
use crdtsync_server::acl::{actor_key, Acl};
use crdtsync_server::membership::Membership;
use crdtsync_server::placement::NodeId;
use crdtsync_server::{ConnId, Identity, ManualClock, Registry, SchemaRegistry, StaticTokens};

const CH: Channel = Channel(0);
const N: usize = 3;
const A: &str = "10.0.0.1:9000";
const B: &str = "10.0.0.2:9000";
const APP: &[u8] = b"collab";

/// The key alice denies bob, and one she leaves readable — the two halves every
/// assertion here reads apart.
const SECRET: &[u8] = b"secret";
const OPEN: &[u8] = b"open";

/// Read to any authenticated actor, write to `editor`. Root read arrives from the
/// schema tier, so the doc-ACL deny below is the only thing that can narrow it — and
/// the narrowing needs the creator.
const SCHEMA: &str = r#"{ "schema": "collab", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "auth": {
        "roles": ["editor"],
        "grants": [
            { "allow": "read",  "to": "authenticated", "on": "/" },
            { "allow": "write", "to": "editor",        "on": "/" }
        ]
    } }"#;

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

/// alice is an editor (she bootstraps the room, so she becomes its creator); mallory
/// is an editor who arrives later, so the schema lets her write a room she has no
/// authority over; bob holds no role, so only the schema's `authenticated` read grant
/// admits him.
fn tokens() -> StaticTokens {
    let mut t = StaticTokens::new();
    t.insert_identity(
        b"t-alice".to_vec(),
        Identity::with_claims(b"alice".to_vec(), vec!["editor".to_string()], Vec::new()),
    );
    t.insert_identity(
        b"t-mallory".to_vec(),
        Identity::with_claims(b"mallory".to_vec(), vec!["editor".to_string()], Vec::new()),
    );
    t.insert_identity(
        b"t-bob".to_vec(),
        Identity::with_claims(b"bob".to_vec(), Vec::new(), Vec::new()),
    );
    t
}

/// A cluster node whose self is `self_addr`, holding `SCHEMA` and an abstaining
/// deployment ACL — so the schema and doc-ACL tiers alone decide every read.
fn node(self_addr: &str) -> Registry {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, SCHEMA.as_bytes(), b"").unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens()));
    r.set_authorizer(Box::new(Acl::new()));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for(self_addr));
    r.set_cluster_secret(CLUSTER_SECRET.to_vec());
    r
}

/// Hello + Auth a connection as `credential`, declaring `{APP, 1}`.
fn hello_auth(r: &mut Registry, client: u8, credential: &str) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: APP.to_vec(),
            schema_version: 1,
            codecs: Vec::new(),
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: credential.as_bytes().to_vec(),
        }
    ));
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

fn submit(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(id, Message::Ops { channel: CH, ops }));
}

/// alice denies `actor` read at the top-level key `key`, authoring the tuple under
/// her own actor key (she is the room's creator, so the tuple is authoritative).
fn deny_read(doc: &mut Document, actor: &[u8], key: &[u8]) -> Vec<Op> {
    doc.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(actor_key(actor)),
            AclGrant::Capability(Capability::Read),
            AclEffect::Deny,
            encode_path(&[key]),
            actor_key(b"alice"),
        );
    })
}

/// A room `A` leads and `B` replicates second in HRW order — so `B` follows it while
/// `A` is up, and is the node promoted to lead it when `A` goes down.
fn room_led_by_a_with_b_next() -> Vec<u8> {
    let m = membership_for(A);
    let a = NodeId::from_addr(A);
    let b = NodeId::from_addr(B);
    for i in 0..1_000_000 {
        let room = format!("room-{i}").into_bytes();
        let replicas = m.replicas_for(&room);
        if replicas.first() == Some(&a) && replicas.get(1) == Some(&b) {
            return room;
        }
    }
    panic!("no room led by A with B its next replica");
}

/// A connection admitted to `r`'s peer plane as one of `room`'s other replicas, as a
/// member's dialed link is.
fn peer_conn(r: &mut Registry, room: &[u8]) -> ConnId {
    let node = r
        .membership()
        .and_then(|m| m.replicas_for(room).into_iter().find(|n| !m.is_self(n)))
        .expect("the room has another replica");
    let id = r.connect();
    assert!(
        r.deliver(
            id,
            Message::PeerAuth {
                node: node.as_bytes().to_vec(),
                secret: CLUSTER_SECRET.to_vec(),
            },
        ),
        "the cluster secret admits a peer",
    );
    id
}

/// A leader holding `room` with alice as its creator, `OPEN` and `SECRET` written,
/// and bob denied read at `/secret`.
fn seeded_leader(room: &[u8]) -> Registry {
    let mut leader = node(A);
    let alice = hello_auth(&mut leader, 1, "t-alice");
    assert!(leader.deliver(alice, sub(room)));
    leader.take_outbox(alice);

    let mut doc = Document::new(cid(1));
    // alice's first write establishes the room, so she becomes its creator — the
    // authority root her deny below is decided under.
    submit(
        &mut leader,
        alice,
        doc.transact(|tx| {
            tx.register(OPEN, Scalar::Int(1));
            tx.register(SECRET, Scalar::Int(2));
        }),
    );
    submit(&mut leader, alice, deny_read(&mut doc, b"bob", SECRET));
    leader.take_outbox(alice);
    assert_eq!(
        leader.hub().room_creator(room).as_deref(),
        Some(b"alice".as_slice()),
        "the first authenticated writer is the room's creator",
    );
    leader
}

/// Hand every replication frame the leader queued for `B` to a fresh follower — the
/// ops path, since nothing has compacted.
fn follower_by_ops(leader: &mut Registry, room: &[u8]) -> Registry {
    let b = NodeId::from_addr(B);
    let mut follower = node(B);
    let peer = peer_conn(&mut follower, room);
    let mut applied = 0;
    for (target, frame) in leader.take_replication() {
        if target != b {
            continue;
        }
        assert!(
            matches!(frame, Message::Replicate { .. }),
            "an uncompacted room replicates by ops: {frame:?}",
        );
        assert!(follower.deliver(peer, frame), "the follower applies it");
        applied += 1;
    }
    assert!(applied > 0, "the leader replicated something");
    assert_eq!(
        follower.hub().export_room(room),
        leader.hub().export_room(room),
        "the follower converged with the leader",
    );
    follower
}

/// Catch a fresh follower up on `room` by the below-floor snapshot state-transfer:
/// the leader compacts every commit into its snapshot, so a watermark-0 follower is
/// dialed a whole-replica `ReplicateSnapshot` rather than an ops delta.
fn follower_by_snapshot(leader: &mut Registry, room: &[u8]) -> Registry {
    let b = NodeId::from_addr(B);
    // Drop the steady-path frames: the follower has acked nothing, so its watermark
    // is 0 and the dial below is what converges it.
    leader.take_replication();
    assert!(
        leader.hub().base_seq(room) > 0,
        "the room compacted above the floor",
    );
    leader.catch_up_follower(&b);
    let mut frames: Vec<Message> = leader
        .take_replication()
        .into_iter()
        .filter(|(target, _)| *target == b)
        .map(|(_, frame)| frame)
        .collect();
    assert_eq!(frames.len(), 1, "exactly one catch-up frame: {frames:?}");
    let frame = frames.pop().expect("one frame");
    assert!(
        matches!(frame, Message::ReplicateSnapshot { .. }),
        "a below-floor follower is caught up by a snapshot: {frame:?}",
    );

    let mut follower = node(B);
    let peer = peer_conn(&mut follower, room);
    assert!(follower.deliver(peer, frame), "the follower applies it");
    assert_eq!(
        follower.hub().export_room(room),
        leader.hub().export_room(room),
        "the follower converged with the leader",
    );
    follower
}

/// Subscribe bob on `r` and fold whatever the catch-up served him — an op delta or a
/// projected snapshot — into one document.
fn bob_reads(r: &mut Registry, room: &[u8]) -> (ConnId, Document) {
    let bob = hello_auth(r, 2, "t-bob");
    assert!(r.deliver(bob, sub(room)));
    let out = r.take_outbox(bob);
    assert!(
        !out.iter().any(|m| matches!(m, Message::Error { .. })),
        "the schema's authenticated read grant admits bob: {out:?}",
    );
    let mut view = Document::new(cid(2));
    let mut served = false;
    for msg in out {
        match msg {
            Message::Ops { ops, .. } => {
                served = true;
                for op in &ops {
                    view.apply(op);
                }
            }
            Message::Snapshot { state, .. } => {
                served = true;
                view = Document::decode_state(&state).expect("a served snapshot decodes");
            }
            _ => {}
        }
    }
    assert!(served, "bob was served a catch-up");
    (bob, view)
}

/// Whether the folded view carries the denied key and the readable one.
fn reads(view: &Document) -> (bool, bool) {
    (view.get(SECRET).is_some(), view.get(OPEN).is_some())
}

/// The version `name` as `conn` is served it, folded into a document.
fn fetched_version(r: &mut Registry, conn: ConnId, name: &[u8]) -> Document {
    assert!(r.deliver(
        conn,
        Message::VersionFetch {
            channel: CH,
            name: name.to_vec(),
        }
    ));
    let out = r.take_outbox(conn);
    let state = out
        .iter()
        .find_map(|m| match m {
            Message::VersionState { state, .. } => Some(state.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("a version state came back: {out:?}"));
    Document::decode_state(&state).expect("a served version decodes")
}

// --- the leader redacts: the control the follower is read against ---

#[test]
fn the_leader_withholds_the_denied_key_from_a_partial_reader() {
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room);
    let (_, view) = bob_reads(&mut leader, &room);
    assert_eq!(
        reads(&view),
        (false, true),
        "the creator's deny carves /secret out of bob's catch-up",
    );
}

// --- a replicated room carries its creator, so the follower redacts too ---

#[test]
fn a_room_replicated_by_ops_carries_its_creator() {
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room);
    let follower = follower_by_ops(&mut leader, &room);
    assert_eq!(
        follower.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
        "the ops replication path carries the room's authority root",
    );
}

#[test]
fn a_room_born_in_one_commit_carries_its_creator_on_that_commit() {
    // The root is established by the write the frame carries, so the frame must be
    // built after it — a room whose whole history is one commit has no second frame
    // to make up for reading the root too early.
    let room = room_led_by_a_with_b_next();
    let mut leader = node(A);
    let alice = hello_auth(&mut leader, 1, "t-alice");
    assert!(leader.deliver(alice, sub(&room)));
    leader.take_outbox(alice);
    submit(
        &mut leader,
        alice,
        Document::new(cid(1)).transact(|tx| tx.register(OPEN, Scalar::Int(1))),
    );

    let follower = follower_by_ops(&mut leader, &room);
    assert_eq!(
        follower.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
        "the room's only commit carries the root it established",
    );
}

#[test]
fn a_dialed_ops_catch_up_carries_the_creator() {
    // The late-joiner dial, the wiped-follower self-heal and the redial after a
    // dropped link converge a follower through the dialed frame rather than a steady
    // commit — an ops delta here, its sibling arm's snapshot below the floor — so a
    // follower that never saw a steady commit must still come up rooted.
    let room = room_led_by_a_with_b_next();
    let b = NodeId::from_addr(B);
    let mut leader = seeded_leader(&room);
    // Drop the steady-path frames: the follower acked nothing, so the dial below is
    // what converges it — from the retained log, uncompacted, so an ops delta.
    leader.take_replication();
    leader.catch_up_follower(&b);
    let mut frames: Vec<Message> = leader
        .take_replication()
        .into_iter()
        .filter(|(target, _)| *target == b)
        .map(|(_, frame)| frame)
        .collect();
    assert_eq!(frames.len(), 1, "exactly one catch-up frame: {frames:?}");
    let frame = frames.pop().expect("one frame");
    assert!(
        matches!(frame, Message::Replicate { .. }),
        "an uncompacted room is dialed an ops delta: {frame:?}",
    );

    let mut follower = node(B);
    let peer = peer_conn(&mut follower, &room);
    assert!(follower.deliver(peer, frame), "the follower applies it");
    assert_eq!(
        follower.hub().export_room(&room),
        leader.hub().export_room(&room),
        "the dial converged the follower",
    );
    assert_eq!(
        follower.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
        "the dialed ops delta carries the room's authority root",
    );

    let (_, view) = bob_reads(&mut follower, &room);
    assert_eq!(
        reads(&view),
        (false, true),
        "so a read it serves is redacted like the leader's",
    );
}

#[test]
fn a_room_installed_by_snapshot_carries_its_creator() {
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room);
    leader.set_compaction_threshold(1);
    let alice = hello_auth(&mut leader, 3, "t-alice");
    assert!(leader.deliver(alice, sub(&room)));
    submit(
        &mut leader,
        alice,
        Document::new(cid(3)).transact(|tx| tx.register(b"more", Scalar::Int(3))),
    );
    leader.take_outbox(alice);

    let follower = follower_by_snapshot(&mut leader, &room);
    assert_eq!(
        follower.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
        "the snapshot state-transfer carries the room's authority root",
    );
}

#[test]
fn a_follower_served_op_catch_up_withholds_the_denied_key() {
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room);
    let mut follower = follower_by_ops(&mut leader, &room);
    let (_, view) = bob_reads(&mut follower, &room);
    assert_eq!(
        reads(&view),
        (false, true),
        "a follower's op catch-up applies the same deny the leader's does",
    );
}

#[test]
fn a_follower_served_snapshot_catch_up_withholds_the_denied_key() {
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room);
    leader.set_compaction_threshold(1);
    let alice = hello_auth(&mut leader, 3, "t-alice");
    assert!(leader.deliver(alice, sub(&room)));
    submit(
        &mut leader,
        alice,
        Document::new(cid(3)).transact(|tx| tx.register(b"more", Scalar::Int(3))),
    );
    leader.take_outbox(alice);

    let mut follower = follower_by_snapshot(&mut leader, &room);
    let (_, view) = bob_reads(&mut follower, &room);
    assert_eq!(
        reads(&view),
        (false, true),
        "a follower's snapshot catch-up projects the same deny the leader's does",
    );
}

// --- a promoted replica does not re-root the room on its first writer ---

#[test]
fn a_promoted_replica_keeps_the_rooms_creator() {
    // Failover makes a replica a leader, and a leader takes client writes — so
    // `ensure_creator` finally fires there. On a creatorless replica the *next actor
    // to write* took `/`, which is ownership, not just an over-wide read: mallory may
    // write under the schema's editor grant while holding no authority over the room.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room);
    let mut follower = follower_by_ops(&mut leader, &room);
    follower.set_peer_liveness(NodeId::from_addr(A), false);

    let mallory = hello_auth(&mut follower, 4, "t-mallory");
    assert!(follower.deliver(mallory, sub(&room)));
    follower.take_outbox(mallory);
    submit(
        &mut follower,
        mallory,
        Document::new(cid(4)).transact(|tx| tx.register(b"mine", Scalar::Int(9))),
    );
    let out = follower.take_outbox(mallory);
    assert!(
        !out.iter()
            .any(|m| matches!(m, Message::OpsRejected { .. } | Message::Redirect { .. })),
        "the promoted replica took the write rather than refusing or redirecting: {out:?}",
    );
    assert!(
        follower.hub().get(&room, b"mine").is_some(),
        "mallory's op landed in the replica, so the creator seam ran on it",
    );
    assert_eq!(
        follower.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
        "the replicated root stands; the first writer after promotion does not take it",
    );
}

// --- an installed root is set-once, exactly as a written one is ---

#[test]
fn a_resent_snapshot_never_displaces_a_standing_creator() {
    // A replica's authority root is set-once, so a re-sent state transfer replaces
    // the state and leaves the root alone — the same rule `ensure_creator` applies
    // to a write. Nothing else could be safe: the frame is a peer's assertion, and
    // adopting a later one would let it re-root a room it already holds.
    let room = room_led_by_a_with_b_next();
    let leader = seeded_leader(&room);
    let state = leader.hub().export_room(&room).expect("the room exports");
    let mut follower = node(B);
    follower
        .hub_mut()
        .install_snapshot(&room, &state, 2, Some(b"alice".to_vec()))
        .expect("the snapshot installs");

    follower
        .hub_mut()
        .install_snapshot(&room, &state, 2, Some(b"mallory".to_vec()))
        .expect("the re-sent snapshot installs");
    assert_eq!(
        follower.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
        "the standing root survives a snapshot naming another",
    );
}

#[test]
fn a_creatorless_snapshot_never_drops_a_standing_creator() {
    let room = room_led_by_a_with_b_next();
    let leader = seeded_leader(&room);
    let state = leader.hub().export_room(&room).expect("the room exports");
    let mut follower = node(B);
    follower
        .hub_mut()
        .install_snapshot(&room, &state, 2, Some(b"alice".to_vec()))
        .expect("the snapshot installs");

    follower
        .hub_mut()
        .install_snapshot(&room, &state, 2, None)
        .expect("the re-sent snapshot installs");
    assert_eq!(
        follower.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
        "a snapshot naming no root leaves the standing one alone",
    );
}

#[test]
fn a_second_frame_naming_another_root_does_not_displace_the_first() {
    // Set-once over the wire, on both paths — the composition above is reached through
    // the frames a peer actually sends, not only through the hub call they make.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room);
    let mut follower = follower_by_ops(&mut leader, &room);
    let peer = peer_conn(&mut follower, &room);
    let state = leader.hub().export_room(&room).expect("the room exports");
    let ops = Document::new(cid(7)).transact(|tx| tx.register(b"planted", Scalar::Int(1)));

    assert!(follower.deliver(
        peer,
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops,
            base_seq: 0,
            epoch: 1,
            creator: Some(b"mallory".to_vec()),
        },
    ));
    assert!(
        follower.hub().get(&room, b"planted").is_some(),
        "the frame applied, so the root it named was judged and refused",
    );
    assert!(follower.deliver(
        peer,
        Message::ReplicateSnapshot {
            room: room.clone(),
            branch: b"main".to_vec(),
            seq: 9,
            state,
            epoch: 1,
            creator: Some(b"mallory".to_vec()),
        },
    ));
    assert_eq!(
        follower.hub().seq(&room),
        9,
        "the snapshot installed, so the root it named was judged and refused",
    );
    assert_eq!(
        follower.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
        "neither frame re-roots a replica that already holds an authority root",
    );
}

#[test]
fn a_frame_naming_an_anonymous_root_roots_nothing() {
    // The root is set-once, so one that can never come back to exercise its ownership
    // would wedge the room's authority for good — the same rule the write path applies
    // to an anonymous writer, applied to what a peer asserts, on both frames.
    let room = room_led_by_a_with_b_next();
    let mut follower = node(B);
    let peer = peer_conn(&mut follower, &room);
    let ops = Document::new(cid(7)).transact(|tx| tx.register(b"planted", Scalar::Int(1)));
    assert!(follower.deliver(
        peer,
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops,
            base_seq: 0,
            epoch: 1,
            creator: Some(b"anon:ephemeral".to_vec()),
        },
    ));
    assert!(
        follower.hub().get(&room, b"planted").is_some(),
        "the frame applied, so the root it named was judged and refused",
    );
    assert_eq!(
        follower.hub().room_creator(&room),
        None,
        "an anonymous id is not an authority root",
    );

    let state = follower.hub().export_room(&room).expect("the room exports");
    assert!(follower.deliver(
        peer,
        Message::ReplicateSnapshot {
            room: room.clone(),
            branch: b"main".to_vec(),
            seq: 9,
            state,
            epoch: 1,
            creator: Some(b"anon:ephemeral".to_vec()),
        },
    ));
    assert_eq!(
        follower.hub().seq(&room),
        9,
        "the snapshot installed, so the root it named was judged and refused",
    );
    assert_eq!(
        follower.hub().room_creator(&room),
        None,
        "nor does it become one by riding a state transfer",
    );
}

#[test]
fn a_rootless_frame_leaves_the_replica_rootless() {
    // What is still not carried, pinned rather than left to prose: a leader with no
    // root of its own hands over none. A root derived from the state bytes would break
    // this — which is the point of pinning it.
    let room = room_led_by_a_with_b_next();
    let mut follower = node(B);
    let peer = peer_conn(&mut follower, &room);
    let ops = Document::new(cid(7)).transact(|tx| tx.register(b"planted", Scalar::Int(1)));
    assert!(follower.deliver(
        peer,
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops,
            base_seq: 0,
            epoch: 1,
            creator: None,
        },
    ));
    assert!(
        follower.hub().get(&room, b"planted").is_some(),
        "the frame applied rather than being fenced, so the absence below is its answer",
    );
    assert_eq!(
        follower.hub().room_creator(&room),
        None,
        "a frame naming no root establishes none",
    );
}

#[test]
fn an_authenticated_actor_roots_a_room_whatever_its_id_looks_like() {
    // The rule is exactly "not anonymous". Refusing to root is not fail-closed — a
    // room with no authority root reads every deny in it as inert — so a seam that
    // second-guesses what the verifier produced strips authority rather than
    // protecting it. An empty actor is the sharpest case: every other tier already
    // counts it as authenticated.
    let mut leader = node(A);
    let other = b"other-room".to_vec();
    leader
        .hub_mut()
        .ingest(
            &other,
            Document::new(cid(8)).transact(|tx| tx.register(OPEN, Scalar::Int(1))),
            None,
        )
        .expect("the room ingests");
    leader.hub_mut().ensure_creator(&other, b"");
    assert_eq!(
        leader.hub().room_creator(&other),
        Some(Vec::new()),
        "an empty actor is a credentialed one, so it roots the room",
    );
}

#[test]
fn a_version_captured_on_a_replica_withholds_the_denied_key() {
    // The fetch seam is not leader-gated, so it answers from any node holding the
    // version — and it reads the same authority root the subscribe seam does. The
    // version is captured through the hub directly, as `VersionCreate` does once a
    // failover has made this replica the room's leader.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room);
    let mut follower = follower_by_ops(&mut leader, &room);
    assert!(follower
        .hub_mut()
        .create_version(&room, b"v1")
        .expect("the version is captured"));

    let (bob, _) = bob_reads(&mut follower, &room);
    let view = fetched_version(&mut follower, bob, b"v1");
    assert_eq!(
        reads(&view),
        (false, true),
        "a version fetched off a replica is projected through the same deny",
    );
}
