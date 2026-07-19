//! Cluster hardening 4 — late-joiner replication dial.
//!
//! A room's leader mirrors only *fresh* commits to its followers (Unit 4), so a
//! follower dialed after the leader has already advanced never receives the
//! backlog — it would be routed to (or promoted over) while missing state. The
//! late-joiner dial closes that: when a follower's link comes up, the leader sends
//! it the ops it is missing (from the follower's acknowledged watermark), which the
//! follower ingests and dedups exactly as a live commit, converging it before it
//! serves.
//!
//! These drive two in-process nodes — a leader `Registry` and a follower `Registry`
//! over the same static cluster — with no socket: the leader's `catch_up_follower`
//! queues the backlog, the follower applies it and acks, and the two converge. The
//! socket wiring of the trigger (a follower's `PeerLive` link-up) is the runtime's;
//! the catch-up computation is what these lock. Deterministic, runs under Miri.

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

/// A registry whose self is `self_addr`, or single-node (no membership) when `None`.
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

/// A subscribed writer client (device 1) on `leader`'s `room`, with its author doc
/// — the caller drives `write` against both to commit distinct ops.
fn writer(leader: &mut Registry, room: &[u8]) -> (ConnId, Document) {
    let c = client_as(leader, 1);
    leader.deliver(c, sub(room));
    leader.take_outbox(c);
    (c, doc(1))
}

/// Commit one register write `k{i} = i` to `leader`'s `room` on connection `c`,
/// advancing `w` so the op id is fresh.
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

// --- a brand-new follower catches up to the full backlog ---

#[test]
fn a_late_follower_catches_up_to_the_full_state() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));

    // The leader commits three ops while B is disconnected — discard the frames the
    // steady path queued, simulating a follower that was not connected.
    commit_writes(&mut leader, &room, 3);
    leader.take_replication();
    assert_eq!(leader.hub().seq(&room), 3);

    // B connects: the leader dials the backlog it missed.
    leader.catch_up_follower(&b);
    let frames = leader.take_replication();
    let to_b: Vec<&Message> = frames
        .iter()
        .filter(|(n, _)| *n == b)
        .map(|(_, f)| f)
        .collect();
    assert_eq!(to_b.len(), 1, "one catch-up frame for B, got {to_b:?}");
    match to_b[0] {
        Message::Replicate { ops, .. } => {
            assert_eq!(ops.len(), 3, "the frame carries all three missed ops")
        }
        other => panic!("expected a Replicate, got {other:?}"),
    }

    // Applied to a fresh follower, it converges to the leader's state and sequence.
    let mut follower = node(Some(B));
    let peer = follower.connect();
    for (n, frame) in frames {
        if n == b {
            assert!(follower.deliver(peer, frame));
        }
    }
    assert_eq!(
        follower.hub().seq(&room),
        3,
        "the follower caught up to seq 3"
    );
    // The whole-replica state converges byte-for-byte with the leader's.
    assert_eq!(
        follower.hub().export_room(&room),
        leader.hub().export_room(&room),
        "the follower's state converges with the leader's"
    );
}

// --- a reconnecting follower gets only the tail past its watermark ---

#[test]
fn a_reconnecting_follower_gets_only_its_tail() {
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));
    let (c, mut w) = writer(&mut leader, &room);

    // Two ops the follower already holds and acknowledged.
    write(&mut leader, c, &mut w, 0);
    write(&mut leader, c, &mut w, 1);
    leader.take_replication();
    leader.record_replica_ack(b.clone(), &room, 2);

    // Two more commits land while it is away.
    write(&mut leader, c, &mut w, 2);
    write(&mut leader, c, &mut w, 3);
    leader.take_replication();
    assert_eq!(leader.hub().seq(&room), 4);

    // On reconnect, the catch-up carries only the tail past the acknowledged
    // watermark (2 ops), not the whole log.
    leader.catch_up_follower(&b);
    let frames = leader.take_replication();
    let ops_sent: usize = frames
        .iter()
        .filter(|(n, _)| *n == b)
        .map(|(_, f)| match f {
            Message::Replicate { ops, .. } => ops.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(
        ops_sent, 2,
        "only the two ops past the watermark are dialed"
    );
}

// --- a non-leader / single-node never dials a catch-up ---

#[test]
fn a_non_leader_does_not_catch_up_a_follower() {
    // A room B leads (A is not its primary): A must not originate a catch-up for it.
    let m = membership_for(A);
    let a = NodeId::from_addr(A);
    let room = {
        let mut found = None;
        for i in 0..1_000_000 {
            let room = format!("room-{i}").into_bytes();
            let replicas = m.replicas_for(&room);
            if replicas.first() != Some(&a) && replicas.contains(&a) {
                found = Some(room);
                break;
            }
        }
        found.expect("a room A follows but does not lead")
    };
    let mut a_node = node(Some(A));
    commit_follower_state(&mut a_node, &room);
    // Ask A (a follower of this room) to catch up the room's actual leader — it must
    // not, because A does not lead the room.
    let leader = m.replicas_for(&room)[0].clone();
    a_node.catch_up_follower(&leader);
    assert!(
        a_node.take_replication().is_empty(),
        "a non-leader originates no catch-up"
    );
}

fn commit_follower_state(r: &mut Registry, room: &[u8]) {
    // Seed some state on the node via a replicated frame so the room exists.
    let mut w = doc(7);
    let ops = w.transact(|tx| tx.register(b"x", Scalar::Int(1)));
    let peer = r.connect();
    let frame = Message::Replicate {
        room: room.to_vec(),
        branch: b"main".to_vec(),
        ops,
        base_seq: 0,
        epoch: 1,
    };
    r.deliver(peer, frame);
    r.take_outbox(peer);
}

// --- the below-floor boundary: a compacted room is not ops-caught-up (follow-on) ---

#[test]
fn a_below_floor_follower_is_not_partially_dialed() {
    // A brand-new follower (watermark 0) joining a room the leader has compacted is
    // below the floor — the ops it needs are folded into a snapshot the ops path
    // cannot carry. The dial must NOT send a partial post-floor delta (which would
    // leave the follower divergent); the whole-replica snapshot transfer is a
    // documented follow-on, so the room is skipped.
    let room = room_led_by_a_with_b_follower();
    let b = NodeId::from_addr(B);
    let mut leader = node(Some(A));
    leader.set_compaction_threshold(1); // fold each commit into the snapshot at once

    commit_writes(&mut leader, &room, 3);
    leader.take_replication();
    assert!(
        leader.hub().base_seq(&room) > 0,
        "the room compacted above the floor"
    );

    // A brand-new follower is below the floor — no ops frame is dialed.
    leader.catch_up_follower(&b);
    let dialed = leader.take_replication().into_iter().any(|(n, _)| n == b);
    assert!(
        !dialed,
        "a below-floor follower is not served a partial delta (snapshot transfer is a follow-on)"
    );
}

#[test]
fn single_node_catch_up_is_inert() {
    let mut r = node(None);
    r.catch_up_follower(&NodeId::from_addr(B));
    assert!(r.take_replication().is_empty());
}

#[test]
fn catch_up_to_self_is_inert() {
    let room = room_led_by_a_with_b_follower();
    let mut leader = node(Some(A));
    commit_writes(&mut leader, &room, 1);
    leader.take_replication();
    leader.catch_up_follower(&NodeId::from_addr(A));
    assert!(
        leader.take_replication().is_empty(),
        "a node never catches up itself"
    );
}
