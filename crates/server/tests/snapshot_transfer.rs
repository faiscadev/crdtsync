//! Cluster hardening — below-floor snapshot state-transfer.
//!
//! The late-joiner dial (#314) catches a reconnected follower up by REPLAYING the
//! ops it is missing from its acknowledged watermark. That works only while those
//! ops still exist: a compacted room folds the ops below its snapshot floor away,
//! so a brand-new follower (watermark `0`) — or one whose acked watermark predates
//! a compaction — cannot be converged by an ops delta. The leader instead branches
//! to a whole-replica SNAPSHOT state-transfer: it sends the current `encode_state`
//! snapshot tagged with the sequence it represents, the follower `decode_state`-loads
//! it, and the two converge byte-for-byte.
//!
//! These drive two in-process nodes — a leader `Registry` and a follower `Registry`
//! over the same static cluster — with no socket: the leader's `catch_up_follower`
//! queues the catch-up frame, the follower applies it and acks, and the two
//! converge. Deterministic, runs under Miri.

use std::sync::Arc;

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Scalar};
use crdtsync_server::membership::Membership;
use crdtsync_server::placement::NodeId;
use crdtsync_server::{ConnId, ManualClock, Registry};

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

fn members() -> String {
    (1..=5)
        .map(|i| format!("10.0.0.{i}:9000"))
        .collect::<Vec<_>>()
        .join(",")
}

fn membership_for(self_addr: &str) -> Membership {
    Membership::from_static_config(None, Some(self_addr), &members(), N).unwrap()
}

fn node(self_addr: Option<&str>) -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    if let Some(addr) = self_addr {
        r.set_membership(membership_for(addr));
    }
    r
}

/// An authenticated client on `r` declaring device `client`, handshake drained.
fn client_as(r: &mut Registry, client: u8) -> ConnId {
    let id = r.connect();
    r.deliver(
        id,
        Message::Hello {
            client: cid(client),
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

/// A subscribed writer client (device 1) on `leader`'s `room`, with its author doc.
fn writer(leader: &mut Registry, room: &[u8]) -> (ConnId, Document) {
    let c = client_as(leader, 1);
    leader.deliver(c, sub(room));
    leader.take_outbox(c);
    (c, doc(1))
}

/// Commit one register write `k{i} = i` to `leader`'s `room` on connection `c`.
fn write(leader: &mut Registry, c: ConnId, w: &mut Document, i: usize) {
    let ops = w.transact(|tx| tx.register(format!("k{i}").as_bytes(), Scalar::Int(i as i64)));
    leader.deliver(c, Message::Ops { channel: CH, ops });
}

/// Commit `count` distinct writes to `leader`'s `room` from one client.
fn commit_writes(leader: &mut Registry, room: &[u8], count: usize) {
    let (c, mut w) = writer(leader, room);
    for i in 0..count {
        write(leader, c, &mut w, i);
    }
}

/// The one catch-up frame the leader dialed to `follower` for `room` (asserting
/// exactly one).
fn catch_up_frame(leader: &mut Registry, follower: &NodeId) -> Message {
    let mut to_follower: Vec<Message> = leader
        .take_replication()
        .into_iter()
        .filter(|(n, _)| n == follower)
        .map(|(_, f)| f)
        .collect();
    assert_eq!(
        to_follower.len(),
        1,
        "exactly one catch-up frame, got {to_follower:?}"
    );
    to_follower.pop().unwrap()
}

// --- a brand-new follower below the floor is caught up via a snapshot ---

#[test]
fn a_below_floor_follower_is_caught_up_by_a_snapshot() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));
    leader.set_compaction_threshold(1); // fold each commit into the snapshot at once

    commit_writes(&mut leader, &room, 3);
    leader.take_replication();
    let floor = leader.hub().base_seq(&room);
    assert!(floor > 0, "the room compacted above the floor");
    assert_eq!(leader.hub().seq(&room), 3);

    // A brand-new follower (watermark 0) is below the floor: it is dialed a whole-
    // replica snapshot, not a partial ops delta.
    leader.catch_up_follower(&b);
    let frame = catch_up_frame(&mut leader, &b);
    match &frame {
        Message::ReplicateSnapshot { seq, state, .. } => {
            assert_eq!(*seq, 3, "the snapshot lands the follower at the head");
            assert!(!state.is_empty());
        }
        other => panic!("expected a ReplicateSnapshot, got {other:?}"),
    }

    // Applied to a fresh follower, it converges to the leader's state and sequence.
    let mut follower = node(Some(B));
    let peer = follower.connect();
    assert!(follower.deliver(peer, frame));
    assert_eq!(
        follower.hub().seq(&room),
        3,
        "the follower caught up to seq 3"
    );
    assert_eq!(
        follower.hub().export_room(&room),
        leader.hub().export_room(&room),
        "the below-floor follower converges byte-for-byte via the snapshot"
    );
}

// --- convergence after snapshot + a steady-path tail commit above the floor ---

#[test]
fn snapshot_then_steady_tail_converges() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));
    leader.set_compaction_threshold(1);

    let (c, mut w) = writer(&mut leader, &room);
    for i in 0..3 {
        write(&mut leader, c, &mut w, i);
    }
    leader.take_replication();

    // Snapshot-catch the follower up to the head (seq 3), then apply it.
    leader.catch_up_follower(&b);
    let snap = catch_up_frame(&mut leader, &b);
    let mut follower = node(Some(B));
    let peer = follower.connect();
    assert!(follower.deliver(peer, snap));
    follower.take_outbox(peer);

    // A fresh commit lands on the leader and flows the steady replication path.
    write(&mut leader, c, &mut w, 3);
    let tail: Vec<Message> = leader
        .take_replication()
        .into_iter()
        .filter(|(n, _)| *n == b)
        .map(|(_, f)| f)
        .collect();
    for frame in tail {
        assert!(follower.deliver(peer, frame));
    }

    assert_eq!(leader.hub().seq(&room), 4);
    assert_eq!(
        follower.hub().seq(&room),
        4,
        "the tail lands above the snapshot"
    );
    assert_eq!(
        follower.hub().export_room(&room),
        leader.hub().export_room(&room),
        "snapshot + steady tail converge byte-for-byte"
    );
}

// --- a follower whose acked watermark predates a compaction gets the snapshot ---

#[test]
fn a_watermark_below_a_later_floor_gets_the_snapshot() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));

    // The follower acked seq 2 while the room was uncompacted.
    let (c, mut w) = writer(&mut leader, &room);
    write(&mut leader, c, &mut w, 0);
    write(&mut leader, c, &mut w, 1);
    leader.take_replication();
    leader.record_replica_ack(b.clone(), &room, 2);

    // The leader then compacts past that watermark (floor climbs above 2).
    leader.set_compaction_threshold(1);
    write(&mut leader, c, &mut w, 2);
    write(&mut leader, c, &mut w, 3);
    leader.take_replication();
    assert!(
        leader.hub().base_seq(&room) > 2,
        "the floor climbed above the follower's watermark"
    );

    // On reconnect, the watermark (2) is below the floor: a snapshot, not a futile
    // ops-replay of the two tail ops that would leave it missing ops 0..2.
    leader.catch_up_follower(&b);
    let frame = catch_up_frame(&mut leader, &b);
    assert!(
        matches!(frame, Message::ReplicateSnapshot { .. }),
        "a watermark below the floor is served a snapshot, not an ops tail"
    );
    let mut follower = node(Some(B));
    let peer = follower.connect();
    assert!(follower.deliver(peer, frame));
    assert_eq!(
        follower.hub().export_room(&room),
        leader.hub().export_room(&room),
        "the follower converges via the snapshot"
    );
}

// --- a follower at or above the floor keeps the ops-tail path (#314 unchanged) ---

#[test]
fn a_watermark_at_or_above_the_floor_uses_the_ops_tail() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));

    // Compact at a window of 2, so the floor sits below the head with a retained tail.
    leader.set_compaction_threshold(2);
    let (c, mut w) = writer(&mut leader, &room);
    write(&mut leader, c, &mut w, 0);
    write(&mut leader, c, &mut w, 1); // compacts: floor -> 2, log emptied
    write(&mut leader, c, &mut w, 2); // retained tail: [op3]
    leader.take_replication();
    let floor = leader.hub().base_seq(&room);
    assert!(
        floor >= 2 && leader.hub().seq(&room) > floor,
        "floor {floor} below head"
    );

    // The follower acked exactly the floor: it is at the floor (not below), so the
    // ops path serves the retained tail — not a snapshot.
    leader.record_replica_ack(b.clone(), &room, floor);
    leader.catch_up_follower(&b);
    let frame = catch_up_frame(&mut leader, &b);
    match &frame {
        Message::Replicate { ops, .. } => assert_eq!(
            ops.len(),
            (leader.hub().seq(&room) - floor) as usize,
            "the ops tail past the floor is dialed"
        ),
        other => panic!("expected a Replicate ops tail, got {other:?}"),
    }
}

// --- a re-sent snapshot is idempotent (decode replaces state) ---

#[test]
fn a_resent_snapshot_is_idempotent() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));
    leader.set_compaction_threshold(1);
    commit_writes(&mut leader, &room, 3);
    leader.take_replication();

    leader.catch_up_follower(&b);
    let frame = catch_up_frame(&mut leader, &b);

    let mut follower = node(Some(B));
    let peer = follower.connect();
    // Deliver the same snapshot twice: the second decode replaces the state, landing
    // at the same sequence and the same converged bytes.
    assert!(follower.deliver(peer, frame.clone()));
    let after_first = follower.hub().export_room(&room);
    assert!(follower.deliver(peer, frame));
    assert_eq!(
        follower.hub().seq(&room),
        3,
        "seq is stable across a re-send"
    );
    assert_eq!(
        follower.hub().export_room(&room),
        after_first,
        "a re-sent snapshot leaves the replica byte-identical (idempotent)"
    );
    assert_eq!(
        follower.hub().export_room(&room),
        leader.hub().export_room(&room),
        "still converged with the leader"
    );
}

// --- fail-closed: a below-floor follower is never left without the snapshot ---

#[test]
fn a_below_floor_follower_is_not_left_stale() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));
    leader.set_compaction_threshold(1);
    commit_writes(&mut leader, &room, 2);
    leader.take_replication();

    // The below-floor follower MUST be served the snapshot — never skipped and left
    // to serve a partial or empty replica.
    leader.catch_up_follower(&b);
    let frame = catch_up_frame(&mut leader, &b);
    assert!(
        matches!(frame, Message::ReplicateSnapshot { .. }),
        "a below-floor follower is served the snapshot, fail-closed (never dropped)"
    );

    // Before the snapshot the follower does not hold the room at all — it cannot
    // serve stale/partial state; only after applying the snapshot does it converge.
    let mut follower = node(Some(B));
    assert!(
        follower.hub().export_room(&room).is_none(),
        "the follower holds nothing before catch-up"
    );
    let peer = follower.connect();
    assert!(follower.deliver(peer, frame));
    assert_eq!(
        follower.hub().export_room(&room),
        leader.hub().export_room(&room),
        "only after the snapshot does it converge"
    );
}

// --- a stale-epoch snapshot is fenced (split-brain safety) ---

#[test]
fn a_stale_epoch_snapshot_is_fenced() {
    let room = room_led_by_a_with_b_follower();
    let mut follower = node(Some(B));
    let peer = follower.connect();

    // The follower has seen epoch 5 (a live leader's frame at that epoch).
    let mut w = doc(7);
    let ops = w.transact(|tx| tx.register(b"x", Scalar::Int(1)));
    assert!(follower.deliver(
        peer,
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops,
            base_seq: 0,
            epoch: 5,
        },
    ));
    let seq_before = follower.hub().seq(&room);

    // A snapshot stamped with a lower epoch (a demoted-then-recovered stale leader)
    // is fenced: the connection stays open but the replica is untouched.
    let state = follower.hub().export_room(&room).unwrap();
    assert!(follower.deliver(
        peer,
        Message::ReplicateSnapshot {
            room: room.clone(),
            branch: b"main".to_vec(),
            seq: 99,
            state,
            epoch: 4,
        },
    ));
    assert_eq!(
        follower.hub().seq(&room),
        seq_before,
        "a stale-epoch snapshot does not install"
    );
}

// --- a non-leader / single-node / self never dials a snapshot ---

#[test]
fn single_node_snapshot_catch_up_is_inert() {
    let mut r = node(None);
    r.catch_up_follower(&NodeId::from_addr(B));
    assert!(r.take_replication().is_empty());
}
