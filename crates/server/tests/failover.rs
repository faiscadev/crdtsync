//! Cluster Unit 6a — leader failover by liveness (HRW-next-live promotion).
//!
//! Leadership is the placement primary until that primary goes DOWN, at which
//! point it promotes to the next LIVE replica in HRW order. `Membership` gains a
//! per-node liveness view (`self` is always live) and an
//! `effective_primary_for(room)` that walks `replicas_for` and returns the first
//! live replica — byte-identical to `primary_for` while every replica is up. The
//! leadership decision points (redirect target, replication origination, the
//! follower gate) route on the effective (live) leader, so a promoted node starts
//! serving and mirroring its newly-led rooms while the down primary is no longer
//! the redirect target.
//!
//! These drive the logic in process with injected liveness (no sockets), so they
//! are deterministic and run under Miri. The liveness *signal* wiring — a dropped
//! inter-node relay link marking a peer down — is the `runtime.rs` seam
//! (`Cmd::PeerLive` → `Registry::set_peer_liveness`); this exercises that method
//! directly, the pattern the replication/majority-ack specs use.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Scalar};
use crdtsync_server::membership::Membership;
use crdtsync_server::placement::NodeId;
use crdtsync_server::{
    step, AllowAll, Hub, ManualClock, PermitAll, Registry, Response, SchemaRegistry, Session,
};

const CH: Channel = Channel(0);
const N: usize = 3;
/// Self's advertise address in the shared member set — a member placed late in
/// most rooms' HRW order, so plenty of rooms are led by a peer.
const SELF_ADDR: &str = "10.0.0.6:9000";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

/// The static member set every node's view is built from — larger than the
/// replication factor, so a room's replica set is a proper subset and rooms
/// place their primary on different members.
fn members_str() -> String {
    (0..7)
        .map(|i| format!("10.0.0.{i}:9000"))
        .collect::<Vec<_>>()
        .join(",")
}

fn membership_for(self_addr: &str) -> Membership {
    Membership::from_static_config(None, Some(self_addr), &members_str(), N).unwrap()
}

/// A room whose top-three placement replicas are all peers of `m`'s self, so the
/// primary and its first two promotion targets are deterministic peers this node
/// can mark down without hitting the always-live self guard.
fn room_led_by_peer(m: &Membership) -> Vec<u8> {
    for i in 0..1_000_000 {
        let room = format!("room-{i}").into_bytes();
        let r = m.replicas_for(&room);
        if r.len() >= 3 && !r[..3].iter().any(|n| m.is_self(n)) {
            return room;
        }
    }
    panic!("no room led by a peer with two peer promotion targets");
}

/// A room whose HRW-second replica is `m`'s self — so a down placement primary
/// promotes self to effective leader.
fn room_where_self_is_second(m: &Membership) -> Vec<u8> {
    for i in 0..1_000_000 {
        let room = format!("room-{i}").into_bytes();
        let r = m.replicas_for(&room);
        if r.len() >= 2 && m.is_self(&r[1]) {
            return room;
        }
    }
    panic!("no room places self second");
}

fn sub(room: &[u8]) -> Message {
    Message::Subscribe {
        channel: CH,
        room: room.to_vec(),
        branch: Vec::new(),
        last_seen_seq: 0,
    }
}

/// Drive one message through `step` under `m`, dev verifier / permit-all.
fn st(h: &mut Hub, s: &mut Session, m: Option<&Membership>, msg: Message) -> Response {
    step(
        h,
        s,
        &AllowAll,
        &PermitAll,
        None,
        &Mutex::new(SchemaRegistry::new()),
        None,
        m,
        0,
        None,
        msg,
    )
}

/// Hello + Auth so the session may subscribe.
fn handshake(h: &mut Hub, s: &mut Session, client: u8) {
    st(
        h,
        s,
        None,
        Message::Hello {
            client: cid(client),
            app_id: Vec::new(),
            schema_version: 0,
        },
    );
    st(
        h,
        s,
        None,
        Message::Auth {
            credential: b"cred".to_vec(),
        },
    );
}

/// An authenticated client on `r`, handshake drained.
fn client(r: &mut Registry) -> crdtsync_server::ConnId {
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

// --- effective primary tracks liveness, placement stays put ---

#[test]
fn all_replicas_live_effective_equals_placement() {
    // Steady state: no node marked down, so `effective_primary_for` is
    // byte-identical to `primary_for` for every room — the no-behavior-change
    // regression the whole unit rests on.
    let m = membership_for(SELF_ADDR);
    for i in 0..3000 {
        let room = format!("room-{i}").into_bytes();
        assert_eq!(m.effective_primary_for(&room), m.primary_for(&room));
        assert_eq!(m.is_effective_primary_for(&room), m.is_primary_for(&room));
        // Every member is live until proven down.
        for member in m.members() {
            assert!(m.is_live(member));
        }
    }
}

#[test]
fn a_down_primary_promotes_the_next_live_replica() {
    let mut m = membership_for(SELF_ADDR);
    let room = room_led_by_peer(&m);
    let replicas = m.replicas_for(&room);
    let primary = replicas[0].clone();
    let next = replicas[1].clone();
    assert_eq!(m.effective_primary_for(&room), Some(primary.clone()));

    m.mark_node_down(&primary);
    // Placement is unchanged — liveness shifts only *effective* leadership.
    assert_eq!(m.primary_for(&room), Some(primary.clone()));
    assert!(!m.is_live(&primary));
    assert_eq!(
        m.effective_primary_for(&room),
        Some(next),
        "the next live replica in HRW order leads",
    );
}

#[test]
fn two_replicas_down_the_third_leads() {
    let mut m = membership_for(SELF_ADDR);
    let room = room_led_by_peer(&m);
    let replicas = m.replicas_for(&room);
    m.mark_node_down(&replicas[0]);
    m.mark_node_down(&replicas[1]);
    assert_eq!(
        m.effective_primary_for(&room),
        Some(replicas[2].clone()),
        "with the top two down the third live replica leads",
    );
}

#[test]
fn a_recovered_primary_reclaims_leadership() {
    // The un-fenced first cut: once the placement primary is live again it is the
    // effective leader once more, with no epoch check. Unit 6b adds epoch fencing
    // so a recovered *stale* primary cannot serve a write it missed while down —
    // an accepted 6a gap.
    let mut m = membership_for(SELF_ADDR);
    let room = room_led_by_peer(&m);
    let primary = m.replicas_for(&room)[0].clone();

    m.mark_node_down(&primary);
    assert_ne!(m.effective_primary_for(&room), Some(primary.clone()));

    m.mark_node_live(&primary);
    assert_eq!(
        m.effective_primary_for(&room),
        m.primary_for(&room),
        "leadership returns to the recovered placement primary",
    );
}

#[test]
fn self_is_always_live_and_leads_when_promoted() {
    // Self can never be marked down (the always-live guarantee), and when the
    // placement primary above it goes down, self — the next in line — leads.
    let mut m = membership_for(SELF_ADDR);
    let self_id = m.self_id().clone();
    m.mark_node_down(&self_id);
    assert!(m.is_live(&self_id), "self is always live");

    let room = room_where_self_is_second(&m);
    let primary = m.replicas_for(&room)[0].clone();
    assert!(!m.is_effective_primary_for(&room));
    m.mark_node_down(&primary);
    assert!(
        m.is_effective_primary_for(&room),
        "self is promoted to effective leader when the primary above it is down",
    );
    assert_eq!(m.effective_primary_for(&room), Some(self_id));
}

// --- routing follows the effective leader ---

#[test]
fn a_subscribe_redirects_to_the_effective_leader() {
    // A follower redirects a client to the room's leader. While all replicas are
    // live that is the placement primary; when the primary is down the redirect
    // points at the promoted (next live) leader instead.
    let mut m = membership_for(SELF_ADDR);
    let room = room_led_by_peer(&m);
    let replicas = m.replicas_for(&room);
    let primary = replicas[0].clone();
    let promoted = replicas[1].clone();

    let mut h = Hub::new(cid(0xFF));
    let mut s = Session::new();
    handshake(&mut h, &mut s, 1);
    let r = st(&mut h, &mut s, Some(&m), sub(&room));
    assert_eq!(
        r.replies,
        vec![Message::Redirect {
            room: room.clone(),
            leader_addr: primary.as_bytes().to_vec(),
        }],
        "steady state redirects to the placement primary",
    );

    m.mark_node_down(&primary);
    let r = st(&mut h, &mut s, Some(&m), sub(&room));
    assert_eq!(
        r.replies,
        vec![Message::Redirect {
            room: room.clone(),
            leader_addr: promoted.as_bytes().to_vec(),
        }],
        "a down primary redirects to the promoted leader, not the dead node",
    );
}

#[test]
fn a_promoted_leader_serves_and_originates_replication() {
    // Self is the room's next-in-line replica. Steady state: the placement primary
    // leads, so self follows — a subscribe is redirected and self replicates
    // nothing. When the placement primary goes DOWN, self is promoted: it now
    // serves the room and mirrors its commits to the remaining replicas.
    let m = membership_for(SELF_ADDR);
    let room = room_where_self_is_second(&m);
    let primary = m.replicas_for(&room)[0].clone();

    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for(SELF_ADDR));
    let c = client(&mut r);

    // Steady state: self is a follower — the subscribe is redirected.
    assert!(r.deliver(c, sub(&room)));
    let out = r.take_outbox(c);
    assert!(
        matches!(out.as_slice(), [Message::Redirect { .. }]),
        "a follower redirects the subscribe, got {out:?}",
    );

    // The placement primary goes down: self is promoted to effective leader.
    r.set_peer_liveness(primary.clone(), false);
    assert!(r.deliver(c, sub(&room)));
    let out = r.take_outbox(c);
    assert!(
        !out.iter().any(|m| matches!(m, Message::Redirect { .. })),
        "the promoted leader serves the subscribe, got {out:?}",
    );

    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert!(r.deliver(c, Message::Ops { channel: CH, ops }));
    assert_eq!(
        r.hub().seq(&room),
        1,
        "the promoted leader ingested the write",
    );
    assert!(
        !r.take_replication().is_empty(),
        "the promoted leader mirrors its commit to the room's replicas",
    );
}

#[test]
fn a_recovered_primary_defers_to_no_one_and_a_follower_defers() {
    // The follower gate on `apply_replicate` routes on effective leadership: a
    // node applies a Replicate only while it merely follows the room. Once
    // promoted (its effective primary), it no longer accepts the down node's
    // frames.
    let m = membership_for(SELF_ADDR);
    let room = room_where_self_is_second(&m);
    let primary = m.replicas_for(&room)[0].clone();

    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for(SELF_ADDR));
    let peer = r.connect();

    // While self follows, a Replicate from the leader applies.
    let ops = doc(1).transact(|tx| tx.register(b"a", Scalar::Int(1)));
    assert!(r.deliver(
        peer,
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops,
            base_seq: 0,
        },
    ));
    assert_eq!(r.hub().seq(&room), 1, "a follower applied the replicate");

    // Promote self: it is now the effective leader and rejects further frames from
    // the (now down) old primary — a self-led room never accepts a Replicate.
    r.set_peer_liveness(primary, false);
    let ops = doc(1).transact(|tx| tx.register(b"b", Scalar::Int(2)));
    let peer2 = r.connect();
    assert!(
        !r.deliver(
            peer2,
            Message::Replicate {
                room: room.clone(),
                branch: b"main".to_vec(),
                ops,
                base_seq: 0,
            },
        ),
        "a promoted leader drops a Replicate for a room it now leads",
    );
    assert_eq!(r.hub().seq(&room), 1, "nothing further was applied");
}

#[test]
fn a_non_owner_with_every_replica_down_redirects_not_serves() {
    // Pathological: a node that does not hold the room sees every one of the
    // room's replicas down. It must not start serving the orphaned room itself —
    // it falls back to redirecting at the placement primary, so a client retries
    // a (dead) leader rather than writing to a node that would never replicate.
    let mut m = membership_for(SELF_ADDR);
    let room = room_led_by_peer(&m);
    assert!(!m.owns(&room), "self does not hold this room");
    let primary = m.primary_for(&room).unwrap();
    for replica in m.replicas_for(&room) {
        m.mark_node_down(&replica);
    }
    assert_eq!(m.effective_primary_for(&room), None, "no replica is live",);

    let mut h = Hub::new(cid(0xFF));
    let mut s = Session::new();
    handshake(&mut h, &mut s, 1);
    let r = st(&mut h, &mut s, Some(&m), sub(&room));
    assert_eq!(
        r.replies,
        vec![Message::Redirect {
            room: room.clone(),
            leader_addr: primary.as_bytes().to_vec(),
        }],
        "a non-owner redirects to the placement primary, never serves",
    );
    assert!(
        r.broadcast.is_empty() && r.broadcast_room.is_none(),
        "no catch-up was served",
    );
}

// --- regressions: single-node and steady-state cluster unchanged ---

#[test]
fn single_node_has_no_liveness_machinery() {
    // No membership: self leads every room, dials no peer, tracks no liveness. A
    // liveness signal is inert, and behavior is byte-identical to today.
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_peer_liveness(NodeId::from_addr("10.0.0.1:9000"), false);
    let c = client(&mut r);
    assert!(r.deliver(c, sub(b"any-room")));
    let out = r.take_outbox(c);
    assert!(
        !out.iter().any(|m| matches!(m, Message::Redirect { .. })),
        "single-node never redirects",
    );
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert!(r.deliver(c, Message::Ops { channel: CH, ops }));
    assert_eq!(r.hub().seq(b"any-room"), 1, "single-node serves locally");
    assert!(
        r.take_replication().is_empty(),
        "single-node never replicates",
    );
}

#[test]
fn steady_state_cluster_redirects_and_serves_as_before() {
    // Every replica live: a follower redirects to the placement primary and a
    // primary serves its own room — exactly Unit 3 behavior, no liveness engaged.
    let m = membership_for(SELF_ADDR);
    let follower_room = room_led_by_peer(&m);
    let leader = m.primary_for(&follower_room).unwrap();
    let self_room = (0..1_000_000)
        .map(|i| format!("room-{i}").into_bytes())
        .find(|room| m.is_primary_for(room))
        .expect("a room self leads");

    let mut h = Hub::new(cid(0xFF));
    let mut s = Session::new();
    handshake(&mut h, &mut s, 1);

    let r = st(&mut h, &mut s, Some(&m), sub(&follower_room));
    assert_eq!(
        r.replies,
        vec![Message::Redirect {
            room: follower_room.clone(),
            leader_addr: leader.as_bytes().to_vec(),
        }],
        "a follower redirects to the placement primary",
    );

    let r = st(&mut h, &mut s, Some(&m), sub(&self_room));
    assert!(
        !r.replies
            .iter()
            .any(|m| matches!(m, Message::Redirect { .. })),
        "a self-led room is served, not redirected",
    );
}
