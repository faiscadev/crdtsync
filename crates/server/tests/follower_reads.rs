//! Transparent-proxy follower reads — a caught-up follower serves reads locally.
//!
//! Today a follower redirects every room-serving request to the leader (Unit 3).
//! This unit lets a CAUGHT-UP follower serve a READ (a Subscribe / catch-up) from
//! its own replicated state, offloading the leader, while WRITES still redirect.
//! The consistency model is bounded-staleness by default plus read-your-writes /
//! monotonicity via the client's `last_seen_seq` floor (its highest observed
//! server sequence):
//!
//! - A caught-up follower answers a read at its committed watermark — a monotonic,
//!   internally-consistent snapshot that may lag the leader (bounded staleness).
//! - The read carries a floor (`last_seen_seq`). The follower serves ONLY IF its
//!   watermark ≥ the floor; otherwise it redirects to the leader (which is ahead).
//!   A floor above the follower's watermark is a read-your-writes / monotonicity
//!   violation-in-waiting — fail safe to the leader, never serve stale-relative-to
//!   -what-the-client-saw.
//! - A follower that is not a replica of the room, or holds no materialized copy
//!   (not yet caught up), redirects — it never serves a torn or absent state.
//! - A WRITE reaching a follower redirects to the leader, unchanged.
//! - The leader always serves (bounded-staleness never applies to it).
//!
//! These drive two in-process registries — a leader `Registry` and a follower
//! `Registry` over the same static cluster — with no socket, mirroring
//! `late_joiner.rs`: the leader commits, its replication frames are handed to the
//! follower, and a client then reads the follower directly. Deterministic, Miri-
//! clean.

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

/// A Subscribe carrying the read floor `floor` in `last_seen_seq` — its highest
/// observed server sequence, both the catch-up cursor and the monotonicity gate.
fn sub_floor(room: &[u8], floor: u64) -> Message {
    Message::Subscribe {
        channel: CH,
        room: room.to_vec(),
        branch: Vec::new(),
        zone: Vec::new(),
        last_seen_seq: floor,
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

/// A room `B` is NOT a replica of (and does not lead) — B holds no copy of it.
fn room_b_does_not_hold() -> Vec<u8> {
    let m = membership_for(A);
    let b = NodeId::from_addr(B);
    for i in 0..1_000_000 {
        let room = format!("nb-{i}").into_bytes();
        if !m.replicas_for(&room).contains(&b) {
            return room;
        }
    }
    panic!("no room B does not replicate");
}

/// Commit `count` distinct register writes to `leader`'s `room` from one client.
fn commit_writes(leader: &mut Registry, room: &[u8], count: usize) {
    let c = client_as(leader, 1);
    leader.deliver(c, sub_floor(room, 0));
    leader.take_outbox(c);
    let mut w = doc(1);
    for i in 0..count {
        let ops = w.transact(|tx| tx.register(format!("k{i}").as_bytes(), Scalar::Int(i as i64)));
        leader.deliver(c, Message::Ops { channel: CH, ops });
    }
}

/// A follower `B` caught up to `leader`'s `room` after `writes` commits: the
/// leader's replication frames for B are applied into a fresh B registry.
fn caught_up_follower(leader: &mut Registry, room: &[u8], writes: usize) -> Registry {
    let b = NodeId::from_addr(B);
    commit_writes(leader, room, writes);
    let frames = leader.take_replication();
    let mut follower = node(Some(B));
    let peer = follower.connect();
    for (nodeid, frame) in frames {
        if nodeid == b {
            assert!(follower.deliver(peer, frame), "follower applies the frame");
        }
    }
    assert_eq!(
        follower.hub().seq(room),
        writes as u64,
        "the follower reached the leader's watermark"
    );
    follower
}

// --- bounded staleness: a caught-up follower serves the read locally ---

#[test]
fn a_caught_up_follower_serves_a_read_at_its_watermark() {
    let room = room_led_by_a_with_b_follower();
    let mut leader = node(Some(A));
    let mut follower = caught_up_follower(&mut leader, &room, 3);

    let c = client_as(&mut follower, 9);
    assert!(follower.deliver(c, sub_floor(&room, 0)));
    let out = follower.take_outbox(c);
    assert!(
        matches!(out.first(), Some(Message::Ops { .. })),
        "a caught-up follower serves the read from local state, never a redirect: {out:?}",
    );
    assert!(
        !out.iter().any(|m| matches!(m, Message::Redirect { .. })),
        "no redirect in a served read",
    );
}

#[test]
fn a_follower_served_read_matches_the_leader_state_at_that_watermark() {
    // The follower's replica is byte-identical to the leader's at the watermark, so
    // any read it serves is consistent with the leader's state at that sequence; and
    // the served catch-up ops, replayed into a fresh client doc, reconstruct it.
    let room = room_led_by_a_with_b_follower();
    let mut leader = node(Some(A));
    let mut follower = caught_up_follower(&mut leader, &room, 4);
    assert_eq!(
        follower.hub().export_room(&room),
        leader.hub().export_room(&room),
        "the follower's replica converges with the leader's",
    );

    let c = client_as(&mut follower, 9);
    assert!(follower.deliver(c, sub_floor(&room, 0)));
    let out = follower.take_outbox(c);
    let Some(Message::Ops { ops, .. }) = out.into_iter().next() else {
        panic!("expected a served catch-up");
    };
    // Replaying the follower-served catch-up reconstructs every key the leader holds
    // — the read carries the whole state at the watermark, not a torn subset.
    let mut view = doc(9);
    for op in &ops {
        view.apply(op);
    }
    for i in 0..4 {
        let key = format!("k{i}");
        assert!(
            view.get(key.as_bytes()).is_some(),
            "k{i} present in the follower-served read",
        );
        assert!(
            leader.hub().get(&room, key.as_bytes()).is_some(),
            "k{i} present in the leader state",
        );
    }
}

#[test]
fn a_read_at_or_below_the_watermark_is_served() {
    let room = room_led_by_a_with_b_follower();
    let mut leader = node(Some(A));
    let mut follower = caught_up_follower(&mut leader, &room, 3);

    // Floor exactly at the watermark: read-your-writes is satisfied, so it serves.
    let c = client_as(&mut follower, 9);
    assert!(follower.deliver(c, sub_floor(&room, 3)));
    let out = follower.take_outbox(c);
    assert!(
        !out.iter().any(|m| matches!(m, Message::Redirect { .. })),
        "a floor at the watermark is served, not redirected: {out:?}",
    );
    assert!(
        matches!(out.first(), Some(Message::Ops { .. })),
        "served with a (possibly empty) op catch-up: {out:?}",
    );
}

// --- read-your-writes / monotonicity: a read past the watermark redirects ---

#[test]
fn a_read_past_the_follower_watermark_redirects_to_the_leader() {
    let room = room_led_by_a_with_b_follower();
    let leader_addr = NodeId::from_addr(A).as_bytes().to_vec();
    let mut leader = node(Some(A));
    let mut follower = caught_up_follower(&mut leader, &room, 3);

    // The client saw / wrote seq 4 (ahead of the follower's watermark 3): serving
    // would be stale-relative-to-the-write, so the follower redirects.
    let c = client_as(&mut follower, 9);
    assert!(follower.deliver(c, sub_floor(&room, 4)));
    assert_eq!(
        follower.take_outbox(c),
        vec![Message::Redirect {
            room: room.clone(),
            leader_addr,
        }],
        "a read past the watermark fails safe to the leader",
    );
}

#[test]
fn monotonicity_a_laggier_follower_redirects_not_serves_backwards() {
    // A client saw seq 5 on one follower, then reads a laggier follower (watermark
    // 2). Passing its floor (5) makes the laggier one redirect rather than serve a
    // state behind what the client already observed.
    let room = room_led_by_a_with_b_follower();
    let leader_addr = NodeId::from_addr(A).as_bytes().to_vec();
    let mut leader = node(Some(A));
    let mut laggier = caught_up_follower(&mut leader, &room, 2);

    let c = client_as(&mut laggier, 9);
    assert!(laggier.deliver(c, sub_floor(&room, 5)));
    assert_eq!(
        laggier.take_outbox(c),
        vec![Message::Redirect {
            room: room.clone(),
            leader_addr,
        }],
        "a client never goes backwards across followers",
    );
}

// --- not caught up / not a replica: never serve ---

#[test]
fn a_not_caught_up_follower_redirects() {
    // B is a replica of the room but has received no replication — it holds no
    // materialized copy, so it must not serve (it would serve an absent/torn state).
    let room = room_led_by_a_with_b_follower();
    let leader_addr = NodeId::from_addr(A).as_bytes().to_vec();
    let mut follower = node(Some(B));
    assert!(!follower.hub().holds_room(&room), "B holds no copy yet");

    let c = client_as(&mut follower, 9);
    assert!(follower.deliver(c, sub_floor(&room, 0)));
    assert_eq!(
        follower.take_outbox(c),
        vec![Message::Redirect {
            room: room.clone(),
            leader_addr,
        }],
        "an uncaught follower redirects, even a bounded-staleness (floor 0) read",
    );
}

#[test]
fn a_non_replica_redirects() {
    // A node that is not in the room's replica set never serves it, even at floor 0.
    let room = room_b_does_not_hold();
    let leader = membership_for(A).primary_for(&room).unwrap();
    let mut b = node(Some(B));

    let c = client_as(&mut b, 9);
    assert!(b.deliver(c, sub_floor(&room, 0)));
    assert_eq!(
        b.take_outbox(c),
        vec![Message::Redirect {
            room: room.clone(),
            leader_addr: leader.as_bytes().to_vec(),
        }],
        "a non-replica redirects to the leader",
    );
}

// --- writes always redirect on a follower (unchanged) ---

#[test]
fn a_write_to_a_caught_up_follower_still_redirects() {
    // Even a fully caught-up follower never accepts a write.
    let room = room_led_by_a_with_b_follower();
    let leader_addr = NodeId::from_addr(A).as_bytes().to_vec();
    let mut leader = node(Some(A));
    let mut follower = caught_up_follower(&mut leader, &room, 3);

    // Subscribe (served) binds the channel, then a write on it is redirected.
    let c = client_as(&mut follower, 9);
    assert!(follower.deliver(c, sub_floor(&room, 0)));
    follower.take_outbox(c);
    let before = follower.hub().seq(&room);
    let ops = doc(9).transact(|tx| tx.register(b"w", Scalar::Int(1)));
    assert!(follower.deliver(c, Message::Ops { channel: CH, ops }));
    assert_eq!(
        follower.take_outbox(c),
        vec![Message::Redirect {
            room: room.clone(),
            leader_addr,
        }],
        "a write on a follower is redirected, never ingested",
    );
    assert_eq!(
        follower.hub().seq(&room),
        before,
        "the write did not advance the follower's replica",
    );
}

// --- the leader always serves (bounded staleness never applies) ---

#[test]
fn the_leader_serves_a_read_past_any_floor() {
    // The leader holds the head, so even a high floor is served (never a redirect).
    let room = room_led_by_a_with_b_follower();
    let mut leader = node(Some(A));
    commit_writes(&mut leader, &room, 3);
    leader.take_replication();

    let c = client_as(&mut leader, 9);
    assert!(leader.deliver(c, sub_floor(&room, 3)));
    let out = leader.take_outbox(c);
    assert!(
        !out.iter().any(|m| matches!(m, Message::Redirect { .. })),
        "the leader serves every read: {out:?}",
    );
}

#[test]
fn single_node_serves_every_read() {
    // No membership: every room is local, no floor gate — today's behavior.
    let mut r = node(None);
    let c = client_as(&mut r, 9);
    assert!(r.deliver(c, sub_floor(b"any", 0)));
    let out = r.take_outbox(c);
    assert!(
        matches!(out.first(), Some(Message::Ops { .. })),
        "single-node serves the room, never a redirect: {out:?}",
    );
}
