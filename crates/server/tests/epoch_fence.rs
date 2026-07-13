//! Cluster Unit 6b — epoch/term split-brain fencing (+ takeover reconciliation).
//!
//! Failover (Unit 6a) promotes a room's next live replica when its placement
//! primary drops, but liveness alone leaves a gap: a demoted-then-recovered stale
//! primary still thinks it leads and could replicate writes it missed, and it and
//! the promoted leader could both act (split-brain). A per-room leadership
//! *epoch* — monotone, exactly Raft's `term` — fences that. A leader stamps every
//! outbound `Replicate` with its epoch for the room; a promotion leads at an
//! epoch strictly above any the promoting node has seen; a follower rejects a
//! frame whose epoch is below the highest it has seen, so a stale leader is
//! fenced and its writes cannot resurrect; and a leader that observes a higher
//! epoch steps down and converges on the new leader's stream.
//!
//! These drive the logic in process with injected liveness and epoch-stamped
//! frames (no sockets), so they are deterministic and run under Miri.

use std::sync::Arc;

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Scalar};
use crdtsync_server::membership::Membership;
use crdtsync_server::{ManualClock, Registry};

const CH: Channel = Channel(0);
const N: usize = 3;
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
/// replication factor, so rooms place their primary on different members and a
/// node is a follower of some rooms and the primary of others.
fn members_str() -> String {
    (0..7)
        .map(|i| format!("10.0.0.{i}:9000"))
        .collect::<Vec<_>>()
        .join(",")
}

fn membership_for(self_addr: &str) -> Membership {
    Membership::from_static_config(None, Some(self_addr), &members_str(), N).unwrap()
}

/// A room self holds as a *follower* — self is in the replica set but not its
/// head, so with the placement primary live self merely follows and applies
/// replicated frames.
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

/// A room self is the placement primary of — self leads it.
fn room_self_leads(m: &Membership) -> Vec<u8> {
    for i in 0..1_000_000 {
        let room = format!("room-{i}").into_bytes();
        if m.is_primary_for(&room) {
            return room;
        }
    }
    panic!("no room places self as primary");
}

/// A room whose HRW-second replica is self — a down placement primary promotes
/// self to effective leader.
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

/// A leader's `Replicate` for `room`'s main stream at `epoch`, carrying a single
/// register write of `value` to `key`. The op is authored by `writer`, a reused
/// [`Document`] whose sequence advances so every frame carries a distinct op id
/// (a fresh `doc(_)` each call would reissue the same id, which the hub dedups).
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

fn registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for(SELF_ADDR));
    r
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

fn sub(room: &[u8]) -> Message {
    Message::Subscribe {
        channel: CH,
        room: room.to_vec(),
        branch: Vec::new(),
        last_seen_seq: 0,
    }
}

/// The epoch of the first queued `Replicate`, or `None` if nothing was queued.
fn originated_epoch(r: &mut Registry) -> Option<u64> {
    r.take_replication()
        .into_iter()
        .find_map(|(_, frame)| match frame {
            Message::Replicate { epoch, .. } => Some(epoch),
            _ => None,
        })
}

// --- a follower applies a higher epoch and fences a lower one ---

#[test]
fn a_higher_epoch_replicate_is_applied_by_a_follower() {
    // The promoted leader leads at a higher epoch; its Replicate is applied.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();
    let mut w = doc(9);
    let peer = r.connect();

    assert!(r.deliver(peer, replicate(&mut w, &room, 1, b"a", 1)));
    assert_eq!(r.hub().seq(&room), 1, "the first leader's frame applied");
    r.take_outbox(peer);

    // A promotion: a new leader at a strictly higher epoch. Its frame is applied.
    assert!(r.deliver(peer, replicate(&mut w, &room, 2, b"b", 2)));
    assert_eq!(r.hub().seq(&room), 2, "the higher-epoch frame applied");
    let out = r.take_outbox(peer);
    assert!(
        matches!(out.as_slice(), [Message::ReplicaAck { through_seq: 2, .. }]),
        "the follower acks the higher-epoch frame, got {out:?}",
    );
}

#[test]
fn a_lower_epoch_replicate_is_fenced() {
    // A stale/demoted primary comes back at its old (lower) epoch: its Replicate
    // is dropped — not applied, not acked — and its write cannot resurrect.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();
    let mut w = doc(9);
    let peer = r.connect();

    // The room advances to epoch 2 under the promoted leader.
    assert!(r.deliver(peer, replicate(&mut w, &room, 1, b"a", 1)));
    assert!(r.deliver(peer, replicate(&mut w, &room, 2, b"b", 2)));
    assert_eq!(r.hub().seq(&room), 2);
    r.take_outbox(peer);

    // The stale primary replays at epoch 1 — fenced.
    let stale = r.connect();
    let kept = r.deliver(stale, replicate(&mut w, &room, 1, b"resurrected", 999));
    assert!(
        kept,
        "a fenced frame is dropped, not a connection-killing violation"
    );
    assert_eq!(
        r.hub().seq(&room),
        2,
        "the fenced write did not apply — no resurrection",
    );
    assert!(
        r.take_outbox(stale).is_empty(),
        "a fenced frame is not acked",
    );
}

#[test]
fn an_equal_epoch_replicate_still_applies() {
    // The steady leader keeps one epoch across its commits; a follower applies
    // every frame at that epoch, not just the first.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();
    let mut w = doc(9);
    let peer = r.connect();

    assert!(r.deliver(peer, replicate(&mut w, &room, 3, b"a", 1)));
    assert!(r.deliver(peer, replicate(&mut w, &room, 3, b"b", 2)));
    assert_eq!(r.hub().seq(&room), 2, "both same-epoch frames applied");
}

// --- promotion bumps the epoch on the leader side ---

#[test]
fn a_promotion_bumps_the_epoch() {
    // Self follows a room and observes the leader's epoch 5. The placement primary
    // then goes down: self is promoted and originates replication at an epoch
    // strictly greater than any it has seen — the promotion bumps it.
    let m = membership_for(SELF_ADDR);
    let room = room_where_self_is_second(&m);
    let primary = m.replicas_for(&room)[0].clone();

    let mut r = registry();
    let mut w = doc(9);
    let c = client(&mut r);
    let peer = r.connect();

    // While a follower, self observes epoch 5 from the live primary.
    assert!(r.deliver(peer, replicate(&mut w, &room, 5, b"seed", 1)));
    assert_eq!(r.hub().seq(&room), 1);
    r.take_outbox(peer);

    // The primary goes down: self is promoted, so it now serves the room's
    // subscribe and write. The write leads at an epoch bumped above the 5 it saw.
    r.set_peer_liveness(primary, false);
    assert!(r.deliver(c, sub(&room)));
    r.take_outbox(c);
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert!(r.deliver(c, Message::Ops { channel: CH, ops }));
    assert_eq!(
        originated_epoch(&mut r),
        Some(6),
        "the promoted leader leads at an epoch above the highest it has seen",
    );
}

#[test]
fn a_rejected_frame_does_not_churn_a_leaders_epoch() {
    // A leader steps down only for a frame it actually applies. A frame it rejects
    // — here a non-main branch at a high epoch — must not touch its leadership
    // epoch, so a malformed or hostile peer cannot force spurious leadership churn.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);
    let mut r = registry();
    let mut cw = doc(1);
    let mut w = doc(9);
    let c = client(&mut r);
    assert!(r.deliver(c, sub(&room)));
    r.take_outbox(c);

    // Self leads at epoch 1.
    let ops = cw.transact(|tx| tx.register(b"v", Scalar::Int(1)));
    assert!(r.deliver(c, Message::Ops { channel: CH, ops }));
    assert_eq!(originated_epoch(&mut r), Some(1));

    // A high-epoch frame on a non-main branch is rejected — it never applies.
    let peer = r.connect();
    let mut frame = replicate(&mut w, &room, 99, b"v", 2);
    if let Message::Replicate { branch, .. } = &mut frame {
        *branch = b"feature".to_vec();
    }
    assert!(
        !r.deliver(peer, frame),
        "a non-main frame drops the connection"
    );

    // Self still leads at epoch 1 — the rejected frame did not bump it.
    let ops = cw.transact(|tx| tx.register(b"v", Scalar::Int(3)));
    assert!(r.deliver(c, Message::Ops { channel: CH, ops }));
    assert_eq!(
        originated_epoch(&mut r),
        Some(1),
        "a rejected frame left the leader's epoch untouched",
    );
}

// --- a recovered old primary observes a higher epoch and steps down ---

#[test]
fn a_recovered_primary_steps_down_and_reconciles() {
    // Self is a room's placement primary, leading at epoch 1. It goes down and a
    // new leader takes over at epoch 2. When self recovers it receives the new
    // leader's higher-epoch stream: it steps down, converges on that stream, and
    // can only lead again at a fresh epoch strictly above the one it observed —
    // never replaying its stale epoch-1 leadership.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);

    let mut r = registry();
    let mut cw = doc(1);
    let mut w = doc(9);
    let c = client(&mut r);

    // Self leads and originates at epoch 1.
    assert!(r.deliver(c, sub(&room)));
    r.take_outbox(c);
    let ops = cw.transact(|tx| tx.register(b"v", Scalar::Int(1)));
    assert!(r.deliver(c, Message::Ops { channel: CH, ops }));
    assert_eq!(r.hub().seq(&room), 1);
    assert_eq!(originated_epoch(&mut r), Some(1), "self leads at epoch 1");

    // The new leader's epoch-2 stream reaches self (as the placement primary, now
    // live again). Self steps down and applies it — converging on the new log.
    let peer = r.connect();
    assert!(
        r.deliver(peer, replicate(&mut w, &room, 2, b"v", 2)),
        "self accepts the higher-epoch leader even as the placement primary",
    );
    assert_eq!(
        r.hub().seq(&room),
        2,
        "self converged on the new leader's stream",
    );
    let out = r.take_outbox(peer);
    assert!(
        matches!(out.as_slice(), [Message::ReplicaAck { through_seq: 2, .. }]),
        "self acks the new leader as a follower, got {out:?}",
    );

    // Having observed epoch 2, self can resume leadership only at a bumped epoch —
    // it can never replicate at its stale epoch 1 again.
    let ops = cw.transact(|tx| tx.register(b"v", Scalar::Int(3)));
    assert!(r.deliver(c, Message::Ops { channel: CH, ops }));
    assert_eq!(
        originated_epoch(&mut r),
        Some(3),
        "the recovered primary re-leads at an epoch above the one it observed",
    );
}

// --- takeover reconciliation: converge on the new leader, fenced ops stay out ---

#[test]
fn a_follower_converges_on_the_new_leader_and_a_fenced_op_stays_out() {
    // A follower carries an uncommitted tail from the old leader that the new
    // leader never saw. When the new leader takes over at a higher epoch the
    // follower converges on its stream; a stale-epoch replay from the old leader
    // is fenced and cannot resurrect, so the follower never diverges back onto the
    // demoted leader's log.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();
    let mut old = doc(10);
    let mut new = doc(20);
    let old_leader = r.connect();

    // The old leader (epoch 1) commits a tail: a, then b.
    assert!(r.deliver(old_leader, replicate(&mut old, &room, 1, b"a", 1)));
    assert!(r.deliver(old_leader, replicate(&mut old, &room, 1, b"b", 2)));
    assert_eq!(r.hub().seq(&room), 2, "the follower holds the old tail");
    r.take_outbox(old_leader);

    // The new leader takes over at epoch 2 with its own commit `c` (it never saw
    // `b`). The follower applies it — converging on the new leader's stream.
    let new_leader = r.connect();
    assert!(r.deliver(new_leader, replicate(&mut new, &room, 2, b"c", 3)));
    assert_eq!(
        r.hub().seq(&room),
        3,
        "the follower took the new leader's op"
    );
    r.take_outbox(new_leader);

    // The demoted old leader replays a divergent op at its stale epoch 1 — fenced.
    let kept = r.deliver(old_leader, replicate(&mut old, &room, 1, b"divergent", 999));
    assert!(kept);
    assert_eq!(
        r.hub().seq(&room),
        3,
        "the fenced op did not apply — the follower stays on the new leader's log",
    );
    assert!(
        r.take_outbox(old_leader).is_empty(),
        "the fenced replay is not acked",
    );
}

// --- regressions: single-node and steady-state cluster unchanged ---

#[test]
fn single_node_ignores_the_epoch() {
    // No membership: a node holds no follower role, so a Replicate is a stray
    // frame dropped regardless of its epoch, and a local write originates nothing.
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    let mut w = doc(9);
    let peer = r.connect();
    assert!(!r.deliver(peer, replicate(&mut w, b"any", 9, b"a", 1)));
    assert!(!r.deliver(peer, replicate(&mut w, b"any", 1, b"a", 1)));
    assert_eq!(r.hub().seq(b"any"), 0, "single-node applies no replicate");

    let c = client(&mut r);
    assert!(r.deliver(c, sub(b"any-room")));
    r.take_outbox(c);
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert!(r.deliver(c, Message::Ops { channel: CH, ops }));
    assert!(
        r.take_replication().is_empty(),
        "single-node originates no epoch-stamped replication",
    );
}

#[test]
fn a_stable_leader_holds_one_epoch() {
    // A leader whose leadership never changes stamps every commit with one epoch —
    // the machinery is inert in the steady state, byte-identical to before the
    // fence beyond the constant epoch on the wire.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);
    let mut r = registry();
    let mut cw = doc(1);
    let c = client(&mut r);
    assert!(r.deliver(c, sub(&room)));
    r.take_outbox(c);

    for _ in 0..3 {
        let ops = cw.transact(|tx| tx.register(b"age", Scalar::Int(30)));
        assert!(r.deliver(c, Message::Ops { channel: CH, ops }));
        assert_eq!(
            originated_epoch(&mut r),
            Some(1),
            "a stable leader never bumps its epoch",
        );
    }
}
