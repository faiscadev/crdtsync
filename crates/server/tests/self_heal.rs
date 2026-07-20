//! Wiped-follower self-heal — a (re)joining follower reports its true durable head
//! and the leader catches it up from THERE, not from a stale remembered ack.
//!
//! The late-joiner dial (#314) catches a follower up from its *acknowledged*
//! watermark — safe only while the follower still durably holds everything up to
//! that ack. A follower whose durable state was WIPED below its last ack (a
//! store-less node, a wiped disk, a restore from an older backup) would be trusted
//! at a position it can no longer honor and caught up incorrectly — a silent gap.
//!
//! The self-heal closes that: on (re)join the follower reports the head it can
//! actually prove per room (its `durable_heads`), and the leader honors that
//! reported head over any remembered ack (`catch_up_follower_reporting`): below the
//! compaction floor it sends a whole-replica snapshot (#315), else the ops tail
//! (#314). Fail-closed — a room the follower no longer holds at all is absent from
//! its manifest and treated as head 0 (a full catch-up). These drive the registry
//! seam directly (no socket), matching `late_joiner.rs`. Deterministic, Miri-clean.

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

fn writer(leader: &mut Registry, room: &[u8]) -> (ConnId, Document) {
    let c = client_as(leader, 1);
    leader.deliver(c, sub(room));
    leader.take_outbox(c);
    (c, doc(1))
}

fn write(leader: &mut Registry, c: ConnId, w: &mut Document, i: usize) {
    let ops = w.transact(|tx| tx.register(format!("k{i}").as_bytes(), Scalar::Int(i as i64)));
    leader.deliver(c, Message::Ops { channel: CH, ops });
}

fn commit_writes(leader: &mut Registry, room: &[u8], count: usize) {
    let (c, mut w) = writer(leader, room);
    for i in 0..count {
        write(leader, c, &mut w, i);
    }
}

/// The ops carried in the frames dialed to `b`, summed.
fn ops_dialed_to(frames: &[(NodeId, Message)], b: &NodeId) -> usize {
    frames
        .iter()
        .filter(|(n, _)| n == b)
        .map(|(_, f)| match f {
            Message::Replicate { ops, .. } => ops.len(),
            _ => 0,
        })
        .sum()
}

/// Apply every frame dialed to `b` into `follower`.
fn apply_to(follower: &mut Registry, frames: Vec<(NodeId, Message)>, b: &NodeId) {
    let peer = follower.connect();
    for (n, frame) in frames {
        if &n == b {
            assert!(follower.deliver(peer, frame));
        }
    }
}

// --- a wiped follower reporting a lower head is caught up from THERE, not its ack ---

#[test]
fn a_wiped_follower_reporting_a_lower_head_gets_the_tail_past_it() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));

    commit_writes(&mut leader, &room, 5);
    leader.take_replication();
    // The leader believes B holds all five (a stale ack B can no longer honor).
    leader.record_replica_ack(b.clone(), &room, 5);
    assert_eq!(leader.hub().seq(&room), 5);

    // B was wiped to seq 2 and reports that true head. The leader honors it over the
    // ack and dials the tail past 2 — three ops — NOT nothing (which the stale ack
    // would have yielded: a silent gap).
    leader.catch_up_follower_reporting(&b, &[(room.clone(), 2)]);
    let frames = leader.take_replication();
    assert_eq!(
        ops_dialed_to(&frames, &b),
        3,
        "the tail past the reported head is dialed, not the empty tail the stale ack implies"
    );
}

// --- a fully-wiped follower (head 0) gets the whole log, byte-identically ---

#[test]
fn a_fully_wiped_follower_at_zero_gets_the_whole_log_byte_identically() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));

    commit_writes(&mut leader, &room, 3);
    leader.take_replication();
    leader.record_replica_ack(b.clone(), &room, 3); // stale ack: leader thinks B has 3

    // B lost everything and reports head 0. Despite the ack of 3, the leader dials
    // the whole retained log.
    leader.catch_up_follower_reporting(&b, &[(room.clone(), 0)]);
    let frames = leader.take_replication();
    assert_eq!(ops_dialed_to(&frames, &b), 3, "the whole log is dialed");

    let mut follower = node(Some(B));
    apply_to(&mut follower, frames, &b);
    assert_eq!(follower.hub().seq(&room), 3);
    assert_eq!(
        follower.hub().export_room(&room),
        leader.hub().export_room(&room),
        "the wiped follower converges byte-for-byte with the leader"
    );
}

// --- a wiped follower below the compaction floor gets a snapshot, byte-identically ---

#[test]
fn a_wiped_below_floor_follower_reporting_gets_a_snapshot() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));
    leader.set_compaction_threshold(1); // fold each commit into the snapshot at once

    commit_writes(&mut leader, &room, 3);
    leader.take_replication();
    leader.record_replica_ack(b.clone(), &room, 3);
    assert!(
        leader.hub().base_seq(&room) > 0,
        "the room compacted above the floor"
    );

    // The reported head (0) is below the compaction floor — the ops it needs are
    // folded away, so a snapshot state-transfer is dialed, never a partial delta.
    leader.catch_up_follower_reporting(&b, &[(room.clone(), 0)]);
    let frames = leader.take_replication();
    let to_b: Vec<&Message> = frames
        .iter()
        .filter(|(n, _)| *n == b)
        .map(|(_, f)| f)
        .collect();
    assert_eq!(to_b.len(), 1, "one catch-up frame for B, got {to_b:?}");
    assert!(
        matches!(to_b[0], Message::ReplicateSnapshot { .. }),
        "a below-floor reported head is served a snapshot: {:?}",
        to_b[0]
    );

    let mut follower = node(Some(B));
    apply_to(&mut follower, frames, &b);
    assert_eq!(follower.hub().seq(&room), 3);
    assert_eq!(
        follower.hub().export_room(&room),
        leader.hub().export_room(&room),
        "the below-floor follower converges byte-for-byte via the snapshot"
    );
}

// --- fail-closed: a room absent from the manifest is treated as head 0 ---

#[test]
fn a_follower_omitting_a_led_room_fails_closed_to_a_full_catch_up() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));

    commit_writes(&mut leader, &room, 3);
    leader.take_replication();
    leader.record_replica_ack(b.clone(), &room, 3);

    // B reports a manifest that omits the room entirely — it can prove nothing of it.
    // The leader must NOT trust its ack of 3; it fails closed to head 0 → full log.
    leader.catch_up_follower_reporting(&b, &[]);
    let frames = leader.take_replication();
    assert_eq!(
        ops_dialed_to(&frames, &b),
        3,
        "an omitted room fails closed to a full catch-up, not the stale ack"
    );
}

// --- regression: a healthy follower reporting its true head keeps the normal tail ---

#[test]
fn a_healthy_follower_reporting_its_true_head_gets_only_its_tail() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));

    commit_writes(&mut leader, &room, 4);
    leader.take_replication();

    // A healthy follower legitimately behind at 2 reports its true head 2 — it gets
    // exactly the two-op tail, no more.
    leader.catch_up_follower_reporting(&b, &[(room.clone(), 2)]);
    assert_eq!(ops_dialed_to(&leader.take_replication(), &b), 2);

    // One caught up to the head reports 4 and is dialed nothing.
    leader.catch_up_follower_reporting(&b, &[(room.clone(), 4)]);
    assert!(
        leader.take_replication().is_empty(),
        "a follower at the head is dialed no catch-up"
    );
}

// --- the reported head replaces the remembered watermark (may lower it) ---

#[test]
fn reporting_lowers_the_remembered_watermark_so_a_later_catch_up_honors_it() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));

    commit_writes(&mut leader, &room, 5);
    leader.take_replication();
    leader.record_replica_ack(b.clone(), &room, 5); // watermark 5

    // Reporting head 2 lowers the watermark to 2, so a subsequent *watermark-path*
    // catch-up (the ordinary #314 dial) now honors 2 and re-sends the tail — proof
    // the stale ack of 5 was replaced, not merely shadowed for one call.
    leader.catch_up_follower_reporting(&b, &[(room.clone(), 2)]);
    leader.take_replication();
    leader.catch_up_follower(&b);
    assert_eq!(
        ops_dialed_to(&leader.take_replication(), &b),
        3,
        "the watermark was lowered to the reported head, not left at the stale ack"
    );
}

// --- a reported head ABOVE the leader's own head is clamped (durability safety) ---

#[test]
fn a_reported_head_above_the_leaders_head_is_clamped() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));

    // The leader is at seq 5. A follower reports head 10 — a stale, higher position
    // from an older/other log (e.g. this leader was freshly promoted while lagging).
    let (c, mut w) = writer(&mut leader, &room);
    for i in 0..5 {
        write(&mut leader, c, &mut w, i);
    }
    leader.take_replication();
    assert_eq!(leader.hub().seq(&room), 5);

    leader.catch_up_follower_reporting(&b, &[(room.clone(), 10)]);
    leader.take_replication();

    // The watermark must have been clamped to the leader's head (5), NOT set to 10.
    // Proof: commit three more (head 8), then a watermark-path catch-up must dial the
    // three-op tail (6,7,8). Had the watermark stuck at the over-reported 10, the
    // catch-up would find nothing past 10 and the follower would silently miss 6-8.
    for i in 5..8 {
        write(&mut leader, c, &mut w, i);
    }
    leader.take_replication();
    leader.catch_up_follower(&b);
    assert_eq!(
        ops_dialed_to(&leader.take_replication(), &b),
        3,
        "the over-reported head was clamped to the leader's own head, not trusted at 10"
    );
}

// --- durable_heads reports the node's provable head per owned room ---

#[test]
fn durable_heads_reports_the_owned_room_head() {
    let room = room_led_by_a_with_b_follower();
    let mut leader = node(Some(A));
    commit_writes(&mut leader, &room, 3);
    let heads = leader.durable_heads();
    assert!(
        heads.iter().any(|(r, h)| r == &room && *h == 3),
        "durable_heads reports (room, 3) for the owned room: {heads:?}"
    );
}

// --- end-to-end: report_heads_to → self-describing frame → leader catches it up ---

#[test]
fn a_follower_reports_and_the_leader_catches_it_up_through_the_frame() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let a = NodeId::from_addr(A);
    let mut leader = node(Some(A));
    commit_writes(&mut leader, &room, 3);
    leader.take_replication();

    // A brand-new follower B (holding nothing) reports its heads to its leader A. Its
    // manifest is empty (it holds no rooms yet), and the frame names B as reporter.
    let mut follower = node(Some(B));
    follower.report_heads_to(&a);
    let reports = follower.take_replication();
    let to_a: Vec<&Message> = reports
        .iter()
        .filter(|(n, _)| *n == a)
        .map(|(_, f)| f)
        .collect();
    assert_eq!(to_a.len(), 1, "one report frame to the leader");
    match to_a[0] {
        Message::FollowerHeads { reporter, heads } => {
            assert_eq!(reporter, b.as_bytes(), "the frame names the reporting node");
            assert!(heads.is_empty(), "a brand-new follower holds nothing");
        }
        other => panic!("expected FollowerHeads, got {other:?}"),
    }

    // The leader receives the self-describing report and catches B up from head 0
    // (fail-closed, empty manifest) — no connection identity needed.
    let peer = leader.connect();
    for (_, frame) in reports {
        leader.deliver(peer, frame);
    }
    let frames = leader.take_replication();
    assert_eq!(
        ops_dialed_to(&frames, &b),
        3,
        "the leader dials the whole log"
    );

    apply_to(&mut follower, frames, &b);
    assert_eq!(follower.hub().seq(&room), 3);
    assert_eq!(
        follower.hub().export_room(&room),
        leader.hub().export_room(&room),
        "the reported-then-caught-up follower converges byte-for-byte"
    );
}

// --- inert / fail-safe boundaries ---

#[test]
fn reporting_is_inert_without_membership() {
    let mut r = node(None);
    let b = NodeId::from_addr(B);
    r.catch_up_follower_reporting(&b, &[(b"room".to_vec(), 0)]);
    assert!(r.take_replication().is_empty());
    r.report_heads_to(&b);
    assert!(r.take_replication().is_empty());
    assert!(r.durable_heads().is_empty());
}

#[test]
fn report_heads_to_self_is_inert() {
    let mut leader = node(Some(A));
    commit_writes(&mut leader, &room_led_by_a_with_b_follower(), 1);
    leader.take_replication();
    leader.report_heads_to(&NodeId::from_addr(A));
    assert!(
        leader.take_replication().is_empty(),
        "a node never reports its heads to itself"
    );
}

#[test]
fn reporting_a_room_this_node_does_not_lead_dials_nothing() {
    // A room B leads (A follows): A must originate no reporting catch-up for it.
    let m = membership_for(A);
    let a = NodeId::from_addr(A);
    let room = (0..1_000_000)
        .map(|i| format!("room-{i}").into_bytes())
        .find(|room| {
            let replicas = m.replicas_for(room);
            replicas.first() != Some(&a) && replicas.contains(&a)
        })
        .expect("a room A follows but does not lead");
    let mut a_node = node(Some(A));
    // Seed the room on A via a replicated frame so it exists in A's hub.
    let mut w = doc(7);
    let ops = w.transact(|tx| tx.register(b"x", Scalar::Int(1)));
    let peer = a_node.connect();
    a_node.deliver(
        peer,
        Message::Replicate {
            room: room.to_vec(),
            branch: b"main".to_vec(),
            ops,
            base_seq: 0,
            epoch: 1,
        },
    );
    a_node.take_outbox(peer);
    let leader = m.replicas_for(&room)[0].clone();
    a_node.catch_up_follower_reporting(&leader, &[(room.clone(), 0)]);
    assert!(
        a_node.take_replication().is_empty(),
        "a non-leader originates no reporting catch-up"
    );
}
