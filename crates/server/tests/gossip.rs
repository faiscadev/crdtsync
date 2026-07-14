//! Anti-entropy gossip membership discovery (Cluster Unit 7a) and gossip-driven
//! failure detection (Cluster Unit 7b).
//!
//! 7a: a node no longer needs the whole cluster at boot — it seeds from one peer
//! and learns the rest by gossip, each round exchanging member sets and unioning
//! them, so a node that knows only a seed converges on the full cluster within a
//! few rounds. Placement is order-independent, so once two nodes have learned the
//! same members they place every room identically.
//!
//! 7b: the gossip exchange also carries per-member liveness (a SWIM state plus a
//! refutation incarnation). A node that misses enough direct gossip rounds to a
//! peer escalates it `Alive → Suspect → Dead`; the verdict rides every gossip
//! frame, so a `Dead` propagates cluster-wide (not just to whoever's relay link
//! dropped) and excludes the member from room leadership through
//! `effective_primary_for`. A node falsely suspected refutes by bumping its
//! incarnation and re-disseminating `Alive`, which wins everywhere. The merge is
//! order-independent: higher incarnation wins, equal incarnation the more-
//! suspicious state wins.

use std::sync::Arc;

use crdtsync_core::{ClientId, MemberState, Message};
use crdtsync_server::gossip::{exchange, gossip_exchange, gossip_frame, merge_into};
use crdtsync_server::membership::{Membership, DEAD_AFTER_FAILURES, SUSPECT_AFTER_FAILURES};
use crdtsync_server::placement::NodeId;
use crdtsync_server::{ManualClock, Registry};

const N: usize = 3;
const A: &str = "10.0.0.1:9000";
const B: &str = "10.0.0.2:9000";
const C: &str = "10.0.0.3:9000";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A membership whose self is `advertise` and whose seed peers are `peers`.
fn seeded(advertise: &str, peers: &str) -> Membership {
    Membership::from_static_config(None, Some(advertise), peers, N).unwrap()
}

/// A membership over just `advertise` — a node that knows only itself.
fn alone(advertise: &str) -> Membership {
    seeded(advertise, "")
}

/// The canonical member set as a plain Vec, for set comparison.
fn member_set(m: &Membership) -> Vec<NodeId> {
    m.members().to_vec()
}

/// A handful of sample room ids to assert placement agreement over.
fn sample_rooms() -> Vec<Vec<u8>> {
    (0..32u32)
        .map(|i| format!("room-{i}").into_bytes())
        .collect()
}

/// Assert two memberships place every sample room identically — same ordered
/// replica set and same primary.
fn placement_agrees(x: &Membership, y: &Membership) {
    for room in sample_rooms() {
        assert_eq!(
            x.replicas_for(&room),
            y.replicas_for(&room),
            "replica sets diverge for {room:?}",
        );
        assert_eq!(
            x.primary_for(&room),
            y.primary_for(&room),
            "primaries diverge for {room:?}",
        );
    }
}

/// The wire payload a membership advertises — its known members with liveness, as
/// raw bytes.
fn payload(m: &Membership) -> Vec<(Vec<u8>, Vec<u8>, u64, MemberState)> {
    m.known_liveness()
        .into_iter()
        .map(|(node, addr, inc, state)| (node.as_bytes().to_vec(), addr, inc, state))
        .collect()
}

/// A converged node knowing all of A, B, C, with itself at `self_addr`.
fn full_node(self_addr: &str) -> Membership {
    let peers: Vec<&str> = [A, B, C].into_iter().filter(|p| *p != self_addr).collect();
    seeded(self_addr, &peers.join(","))
}

/// A liveness tuple for `node` at `addr`, for feeding `merge_liveness` directly.
fn tuple(
    node: &NodeId,
    addr: &str,
    inc: u64,
    state: MemberState,
) -> (NodeId, Vec<u8>, u64, MemberState) {
    (node.clone(), addr.as_bytes().to_vec(), inc, state)
}

// --- (a) convergence: a seed-only node learns the whole cluster ---

#[test]
fn a_seed_only_node_converges_on_the_full_cluster() {
    // A and B know each other; C boots knowing only its seed A — B is unknown to
    // C, and C is unknown to both A and B.
    let mut a = seeded(A, B);
    let mut b = seeded(B, A);
    let mut c = seeded(C, A);
    assert_eq!(
        member_set(&c),
        vec![NodeId::from_addr(A), NodeId::from_addr(C)]
    );
    assert!(!member_set(&a).contains(&NodeId::from_addr(C)));

    // Round 1: C gossips its seed A. Push-pull syncs both — A learns C, C learns B.
    exchange(&mut c, &mut a);
    // Round 2: A gossips B, so B learns C too.
    exchange(&mut a, &mut b);

    let full = vec![
        NodeId::from_addr(A),
        NodeId::from_addr(B),
        NodeId::from_addr(C),
    ];
    assert_eq!(member_set(&a), full, "A converged");
    assert_eq!(member_set(&b), full, "B converged");
    assert_eq!(member_set(&c), full, "C converged");

    // Placement converged: all three compute the same replica set and primary for
    // every sample room.
    placement_agrees(&a, &b);
    placement_agrees(&b, &c);
}

#[test]
fn convergence_holds_whatever_the_gossip_order() {
    // Drive several rounds of a fixed all-pairs schedule; the set must converge to
    // the union regardless of which exchanges ran when.
    let mut a = seeded(A, B);
    let mut b = seeded(B, "");
    let mut c = seeded(C, A);
    for _ in 0..3 {
        exchange(&mut a, &mut c);
        exchange(&mut b, &mut a);
        exchange(&mut c, &mut b);
    }
    let full = vec![
        NodeId::from_addr(A),
        NodeId::from_addr(B),
        NodeId::from_addr(C),
    ];
    assert_eq!(member_set(&a), full);
    assert_eq!(member_set(&b), full);
    assert_eq!(member_set(&c), full);
    placement_agrees(&a, &b);
    placement_agrees(&a, &c);
}

// --- (b) order-independence: learning order does not affect placement ---

#[test]
fn placement_is_independent_of_the_order_members_are_learned() {
    let self_addr = "10.0.0.9:9000";
    // One node learns B then C; another learns C then B.
    let mut bc = alone(self_addr);
    bc.add_member(NodeId::from_addr(B), B.as_bytes().to_vec());
    bc.add_member(NodeId::from_addr(C), C.as_bytes().to_vec());

    let mut cb = alone(self_addr);
    cb.add_member(NodeId::from_addr(C), C.as_bytes().to_vec());
    cb.add_member(NodeId::from_addr(B), B.as_bytes().to_vec());

    assert_eq!(
        member_set(&bc),
        member_set(&cb),
        "same member set either order"
    );
    placement_agrees(&bc, &cb);
}

// --- (c) idempotence: re-gossiping a fully-known set is inert ---

#[test]
fn re_gossiping_a_fully_known_set_changes_nothing() {
    let mut m = seeded(A, &format!("{B},{C}"));
    let members_before = member_set(&m);
    let known_before = m.known_members();
    let placement_before: Vec<_> = sample_rooms()
        .iter()
        .map(|room| m.replicas_for(room))
        .collect();

    // Union the node's own advertisement back into itself — a re-gossip of an
    // already-known set.
    let own = payload(&m);
    merge_into(&mut m, own);

    assert_eq!(member_set(&m), members_before, "no member churn");
    assert_eq!(m.known_members(), known_before, "addresses unchanged");
    let placement_after: Vec<_> = sample_rooms()
        .iter()
        .map(|room| m.replicas_for(room))
        .collect();
    assert_eq!(placement_after, placement_before, "no placement churn");
}

#[test]
fn add_member_is_idempotent_for_a_known_member() {
    let mut m = seeded(A, B);
    let before = member_set(&m);
    // Re-adding an existing member (even with a different address) is a no-op: the
    // set and placement are untouched.
    m.add_member(NodeId::from_addr(B), b"different:1234".to_vec());
    assert_eq!(member_set(&m), before);
    // The first-learned address wins — no churn.
    let b_addr = m
        .known_members()
        .into_iter()
        .find(|(n, _)| n == &NodeId::from_addr(B))
        .map(|(_, a)| a);
    assert_eq!(b_addr, Some(B.as_bytes().to_vec()));
}

#[test]
fn a_member_with_an_empty_node_id_is_dropped() {
    // A malformed gossip pair — an empty node id — is neither placeable nor
    // dialable, so it must not poison the member set.
    let mut m = seeded(A, B);
    let before = member_set(&m);
    m.add_member(NodeId::from(Vec::new()), b"10.0.0.9:9000".to_vec());
    assert_eq!(member_set(&m), before, "an empty-id member is not added");
    // The same guard holds through the wire merge path.
    merge_into(
        &mut m,
        vec![
            (Vec::new(), Vec::new(), 0, MemberState::Alive),
            (
                C.as_bytes().to_vec(),
                C.as_bytes().to_vec(),
                0,
                MemberState::Alive,
            ),
        ],
    );
    assert!(member_set(&m).contains(&NodeId::from_addr(C)), "C is added");
    assert!(
        !member_set(&m).contains(&NodeId::from(Vec::new())),
        "the empty-id pair is dropped",
    );
}

#[test]
fn a_batch_add_unions_every_new_member_at_once() {
    let mut m = alone("10.0.0.9:9000");
    m.add_members(vec![
        (NodeId::from_addr(A), A.as_bytes().to_vec()),
        (NodeId::from_addr(B), B.as_bytes().to_vec()),
        (NodeId::from_addr(A), A.as_bytes().to_vec()), // duplicate within the batch
    ]);
    let mut expected = vec![
        NodeId::from_addr("10.0.0.9:9000"),
        NodeId::from_addr(A),
        NodeId::from_addr(B),
    ];
    expected.sort();
    assert_eq!(
        member_set(&m),
        expected,
        "batch adds each distinct new member once"
    );
}

// --- (d) single-node regression: no membership, no gossip ---

#[test]
fn a_single_node_registry_knows_no_members_and_ignores_gossip() {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    assert!(r.membership().is_none());
    assert!(r.known_members().is_empty(), "no members to advertise");

    // Merging a gossip payload is inert with no membership.
    r.merge_gossip(vec![(
        A.as_bytes().to_vec(),
        A.as_bytes().to_vec(),
        0,
        MemberState::Alive,
    )]);
    assert!(r.known_members().is_empty());
    assert!(r.membership().is_none());

    // A stray Gossip frame on a single-node node drops the connection.
    let peer = r.connect();
    let kept = r.deliver(
        peer,
        Message::Gossip {
            members: vec![(
                A.as_bytes().to_vec(),
                A.as_bytes().to_vec(),
                0,
                MemberState::Alive,
            )],
        },
    );
    assert!(!kept, "a Gossip on a non-cluster node is a stray frame");
}

// --- (e) a learned member is usable as a replica / redirect target ---

#[test]
fn a_member_learned_by_gossip_becomes_a_placement_target() {
    // Start knowing only self, which leads every room. Learn a peer by gossip, then
    // some rooms place on — and are led by — the newcomer, so it is a live redirect
    // and replica target (effective_primary_for elects it; the redirect uses that
    // leader's address).
    let self_addr = "10.0.0.9:9000";
    let mut m = alone(self_addr);
    let newcomer = "10.0.0.7:9000";
    let newcomer_id = NodeId::from_addr(newcomer);

    // Before learning it, the newcomer leads nothing here.
    assert!(sample_rooms()
        .iter()
        .all(|room| m.primary_for(room) == Some(NodeId::from_addr(self_addr))));

    m.add_member(newcomer_id.clone(), newcomer.as_bytes().to_vec());

    let room = sample_rooms()
        .into_iter()
        .find(|room| m.primary_for(room) == Some(newcomer_id.clone()))
        .expect("some room places on the newcomer once it is a member");
    assert!(m.replicas_for(&room).contains(&newcomer_id));
    // The newcomer is live by default, so it is the effective leader — what a
    // redirect points a client at.
    assert_eq!(m.effective_primary_for(&room), Some(newcomer_id.clone()));
    // And it is dialable: gossip recorded its advertise address.
    let addr = m
        .known_members()
        .into_iter()
        .find(|(n, _)| n == &newcomer_id)
        .map(|(_, a)| a);
    assert_eq!(addr, Some(newcomer.as_bytes().to_vec()));
}

#[test]
fn a_registry_grows_its_membership_from_a_gossip_frame() {
    // The registry's merge path (what the gossip task feeds) grows the live view.
    let mut r = Registry::new(cid(0xFF));
    r.set_membership(seeded(A, B));
    assert_eq!(r.known_members().len(), 2);

    r.merge_gossip(vec![(
        C.as_bytes().to_vec(),
        C.as_bytes().to_vec(),
        0,
        MemberState::Alive,
    )]);
    let members: Vec<NodeId> = r
        .known_members()
        .into_iter()
        .map(|(node, _)| node)
        .collect();
    assert!(
        members.contains(&NodeId::from_addr(C)),
        "C learned by gossip"
    );
    assert_eq!(members.len(), 3);
}

// --- gossip frame shape sanity ---

#[test]
fn the_gossip_frame_carries_every_known_member_with_its_address_and_liveness() {
    let m = seeded(A, &format!("{B},{C}"));
    let Message::Gossip { members } = gossip_frame(&m.known_liveness()) else {
        panic!("gossip_frame builds a Gossip message");
    };
    assert_eq!(members.len(), 3);
    // Every member rides at incarnation 0, Alive, until gossip says otherwise.
    for addr in [A, B, C] {
        assert!(members.contains(&(
            addr.as_bytes().to_vec(),
            addr.as_bytes().to_vec(),
            0,
            MemberState::Alive,
        )));
    }
}

// ============ (7b) gossip-driven failure detection ============

/// A room whose placement primary is `node` — one it leads, so a fail-over is
/// observable through `effective_primary_for`.
fn room_led_by(m: &Membership, node: &NodeId) -> Vec<u8> {
    sample_rooms()
        .into_iter()
        .find(|room| m.primary_for(room).as_ref() == Some(node))
        .expect("some sample room places its primary on the node")
}

// --- (a) failure detection: a silent node is suspected, then declared dead ---

#[test]
fn a_node_that_stops_participating_is_suspected_then_declared_dead() {
    let a = full_node(A);
    let c_id = NodeId::from_addr(C);
    let mut a = a;

    // C is alive to start; A leads-elects it for the rooms it is primary of.
    assert!(a.is_live(&c_id));
    let room = room_led_by(&a, &c_id);
    assert_eq!(a.effective_primary_for(&room), Some(c_id.clone()));

    // Each failed direct round to C counts toward suspicion. Below the suspect
    // threshold C is still Alive (a dropped round or two is not a verdict).
    for _ in 0..SUSPECT_AFTER_FAILURES - 1 {
        a.note_gossip_unreachable(&c_id);
        assert_eq!(a.gossip_state(&c_id), MemberState::Alive);
        assert!(a.is_live(&c_id));
    }
    // Crossing the suspect threshold: C is Suspect — but a Suspect still routes
    // (optimistically live) until it hardens to Dead.
    a.note_gossip_unreachable(&c_id);
    assert_eq!(a.gossip_state(&c_id), MemberState::Suspect);
    assert!(a.is_live(&c_id), "a suspect is still live");
    assert_eq!(a.effective_primary_for(&room), Some(c_id.clone()));

    // Crossing the death threshold: C is Dead, excluded from leadership, and the
    // room fails over to the next live replica.
    for _ in SUSPECT_AFTER_FAILURES..DEAD_AFTER_FAILURES {
        a.note_gossip_unreachable(&c_id);
    }
    assert_eq!(a.gossip_state(&c_id), MemberState::Dead);
    assert!(!a.is_live(&c_id), "a dead node is not live");
    let failover = a
        .effective_primary_for(&room)
        .expect("a live replica remains");
    assert_ne!(failover, c_id, "the dead primary's room fails over");
    assert!(a.is_live(&failover));
    assert!(a.replicas_for(&room).contains(&failover));
}

#[test]
fn both_survivors_fail_over_off_a_dead_node() {
    // A and B independently stop reaching C; both declare it dead and both fail
    // over the rooms C led — the cluster-wide exclusion, reached independently.
    let mut a = full_node(A);
    let mut b = full_node(B);
    let c_id = NodeId::from_addr(C);
    let room = room_led_by(&a, &c_id);
    assert_eq!(
        a.primary_for(&room),
        b.primary_for(&room),
        "shared placement"
    );

    for _ in 0..DEAD_AFTER_FAILURES {
        a.note_gossip_unreachable(&c_id);
        b.note_gossip_unreachable(&c_id);
    }
    assert!(!a.is_live(&c_id));
    assert!(!b.is_live(&c_id));
    // Both promote the same next-live replica, so leadership stays single-valued.
    assert_eq!(
        a.effective_primary_for(&room),
        b.effective_primary_for(&room)
    );
    assert_ne!(a.effective_primary_for(&room), Some(c_id));
}

#[test]
fn a_successful_round_clears_accumulated_suspicion() {
    // A run of failed rounds short of death, then a success, resets the count — a
    // node that blips but recovers is not dragged to Dead.
    let mut a = full_node(A);
    let c_id = NodeId::from_addr(C);
    for _ in 0..DEAD_AFTER_FAILURES - 1 {
        a.note_gossip_unreachable(&c_id);
    }
    assert_eq!(a.gossip_state(&c_id), MemberState::Suspect);
    a.note_gossip_reachable(&c_id);
    assert_eq!(a.gossip_state(&c_id), MemberState::Alive);
    assert!(a.is_live(&c_id));
    // The counter reset: it again takes a full run to reach Dead.
    for _ in 0..DEAD_AFTER_FAILURES - 1 {
        a.note_gossip_unreachable(&c_id);
    }
    assert_ne!(a.gossip_state(&c_id), MemberState::Dead);
}

// --- (b) refutation: a falsely-suspected live node overrides the suspicion ---

#[test]
fn a_falsely_suspected_node_refutes_and_the_cluster_restores_it() {
    // A and B are told (staleley) that C is Suspect at incarnation 0. C is actually
    // alive: it hears the suspicion of itself, bumps its incarnation, and re-
    // disseminates Alive — which wins everywhere the stale suspicion reached.
    let mut a = full_node(A);
    let mut b = full_node(B);
    let mut c = full_node(C);
    let c_id = NodeId::from_addr(C);

    let stale = vec![tuple(&c_id, C, 0, MemberState::Suspect)];
    a.merge_liveness(stale.clone());
    b.merge_liveness(stale.clone());
    assert_eq!(a.gossip_state(&c_id), MemberState::Suspect);
    assert_eq!(b.gossip_state(&c_id), MemberState::Suspect);

    // C receives the same suspicion about itself and refutes: incarnation climbs
    // above the received 0, state re-asserted Alive.
    c.merge_liveness(stale);
    assert_eq!(c.gossip_state(&c_id), MemberState::Alive);
    assert!(
        c.incarnation(&c_id) > 0,
        "C bumped its incarnation to refute"
    );

    // C gossips its refutation to A and B; the higher-incarnation Alive overrides.
    exchange(&mut c, &mut a);
    exchange(&mut c, &mut b);
    assert_eq!(a.gossip_state(&c_id), MemberState::Alive);
    assert_eq!(b.gossip_state(&c_id), MemberState::Alive);
    assert!(a.is_live(&c_id));
    assert!(b.is_live(&c_id));
}

#[test]
fn refutation_overrides_even_a_dead_verdict() {
    // The strongest false positive: a peer already declared C Dead. C's refutation
    // at a higher incarnation still wins — a live node can never be stuck dead.
    let mut a = full_node(A);
    let mut c = full_node(C);
    let c_id = NodeId::from_addr(C);
    a.merge_liveness(vec![tuple(&c_id, C, 4, MemberState::Dead)]);
    assert!(!a.is_live(&c_id));

    // C hears the Dead@4 about itself, refutes above 4, re-disseminates Alive.
    c.merge_liveness(vec![tuple(&c_id, C, 4, MemberState::Dead)]);
    assert!(c.incarnation(&c_id) > 4);
    exchange(&mut c, &mut a);
    assert_eq!(a.gossip_state(&c_id), MemberState::Alive);
    assert!(a.is_live(&c_id));
}

// --- (c) incarnation merge: higher wins; equal-incarnation Dead>Suspect>Alive ---

#[test]
fn a_higher_incarnation_always_wins_the_merge() {
    let mut a = full_node(A);
    let c_id = NodeId::from_addr(C);
    a.merge_liveness(vec![tuple(&c_id, C, 5, MemberState::Dead)]);
    assert_eq!(a.gossip_state(&c_id), MemberState::Dead);
    // A fresher (higher-incarnation) Alive supersedes the older Dead.
    a.merge_liveness(vec![tuple(&c_id, C, 6, MemberState::Alive)]);
    assert_eq!(a.gossip_state(&c_id), MemberState::Alive);
    assert_eq!(a.incarnation(&c_id), 6);
    // A stale lower-incarnation Dead is ignored.
    a.merge_liveness(vec![tuple(&c_id, C, 5, MemberState::Dead)]);
    assert_eq!(a.gossip_state(&c_id), MemberState::Alive);
}

#[test]
fn at_equal_incarnation_the_more_suspicious_state_wins() {
    let mut a = full_node(A);
    let c_id = NodeId::from_addr(C);
    // Alive -> Suspect -> Dead all at incarnation 3, each more suspicious wins.
    a.merge_liveness(vec![tuple(&c_id, C, 3, MemberState::Suspect)]);
    assert_eq!(a.gossip_state(&c_id), MemberState::Suspect);
    a.merge_liveness(vec![tuple(&c_id, C, 3, MemberState::Dead)]);
    assert_eq!(a.gossip_state(&c_id), MemberState::Dead);
    // A less-suspicious state at the same incarnation does not un-do it.
    a.merge_liveness(vec![tuple(&c_id, C, 3, MemberState::Alive)]);
    assert_eq!(a.gossip_state(&c_id), MemberState::Dead);
}

#[test]
fn liveness_merge_is_order_independent() {
    // Two nodes receive the same three liveness updates in opposite orders and
    // converge on the identical state and incarnation.
    let c_id = NodeId::from_addr(C);
    let updates = [
        tuple(&c_id, C, 2, MemberState::Suspect),
        tuple(&c_id, C, 2, MemberState::Dead),
        tuple(&c_id, C, 3, MemberState::Alive),
    ];
    let mut fwd = full_node(A);
    for u in updates.iter().cloned() {
        fwd.merge_liveness(vec![u]);
    }
    let mut rev = full_node(A);
    for u in updates.iter().rev().cloned() {
        rev.merge_liveness(vec![u]);
    }
    assert_eq!(fwd.gossip_state(&c_id), rev.gossip_state(&c_id));
    assert_eq!(fwd.incarnation(&c_id), rev.incarnation(&c_id));
    // The highest incarnation is 3 (Alive), so both land there.
    assert_eq!(fwd.gossip_state(&c_id), MemberState::Alive);
    assert_eq!(fwd.incarnation(&c_id), 3);
}

// --- (d) dissemination: a death propagates cluster-wide, not connection-local ---

#[test]
fn a_death_detected_by_one_node_disseminates_to_another_by_gossip() {
    // A detects C dead (its own direct rounds to C fail). B's link to C never
    // dropped and B never probed C — yet a single gossip exchange with A teaches B
    // that C is Dead. This is the whole point of gossip failure detection: the
    // verdict is cluster-wide, not confined to whoever's link dropped.
    let mut a = full_node(A);
    let mut b = full_node(B);
    let c_id = NodeId::from_addr(C);

    for _ in 0..DEAD_AFTER_FAILURES {
        a.note_gossip_unreachable(&c_id);
    }
    assert!(!a.is_live(&c_id));
    assert!(b.is_live(&c_id), "B has no independent evidence yet");

    let room = room_led_by(&b, &c_id);
    exchange(&mut a, &mut b);
    assert_eq!(
        b.gossip_state(&c_id),
        MemberState::Dead,
        "B learned C is dead"
    );
    assert!(!b.is_live(&c_id));
    assert_ne!(
        b.effective_primary_for(&room),
        Some(c_id),
        "B fails the room over"
    );
}

// --- (e) relay-link and gossip signals compose; single-node is unaffected ---

#[test]
fn the_relay_link_signal_still_marks_a_node_down() {
    // The 6a relay-link path is untouched: a relay-down alone excludes a node even
    // while gossip still holds it Alive.
    let mut a = full_node(A);
    let c_id = NodeId::from_addr(C);
    assert!(a.is_live(&c_id));
    a.mark_node_down(&c_id);
    assert!(
        !a.is_live(&c_id),
        "relay-down excludes even a gossip-Alive node"
    );
    assert_eq!(a.gossip_state(&c_id), MemberState::Alive);
    a.mark_node_live(&c_id);
    assert!(a.is_live(&c_id));
}

#[test]
fn a_relay_reconnect_does_not_resurrect_a_gossip_dead_node() {
    // The two signals are unioned: a relay reconnect clears only the relay signal;
    // a node gossip declared Dead stays down until gossip itself refutes it.
    let mut a = full_node(A);
    let c_id = NodeId::from_addr(C);
    for _ in 0..DEAD_AFTER_FAILURES {
        a.note_gossip_unreachable(&c_id);
    }
    a.mark_node_down(&c_id);
    assert!(!a.is_live(&c_id));
    // Relay reconnects, but gossip still says Dead — the node remains excluded.
    a.mark_node_live(&c_id);
    assert!(!a.is_live(&c_id), "gossip-Dead survives a relay reconnect");
    // Only a gossip refutation (higher-incarnation Alive) restores it.
    a.merge_liveness(vec![tuple(&c_id, C, 99, MemberState::Alive)]);
    assert!(a.is_live(&c_id));
}

#[test]
fn a_single_node_has_no_liveness_to_gossip() {
    // A lone node advertises nothing and, having no peers, never suspects anyone.
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    assert!(r.known_liveness().is_empty());
    // A probe report is inert with no membership.
    r.note_gossip_probe(NodeId::from_addr(C), false);
    assert!(r.membership().is_none());
}

// ===================== socket transport (Miri-ignored) =====================
//
// This drives a real gossip exchange over a WebSocket against a live node: the
// dial, the relay handshake, the push of one member set, the server-side union,
// and the pulled reply. It binds a loopback socket, so it is excluded under Miri,
// whose isolation forbids `socket`; the union logic itself is covered by the
// in-process tests above, which run under Miri.

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials a loopback server over a real socket
async fn a_gossip_exchange_over_the_socket_merges_and_replies() {
    use crdtsync_core::protocol::PROTOCOL_VERSION;
    use crdtsync_server::runtime::{serve_with, ServeConfig};
    use tokio::net::TcpListener;

    let _ = PROTOCOL_VERSION;
    // The served node knows {A, B}. We will gossip a third member, C, to it.
    let m = seeded(A, B);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let config = ServeConfig {
        membership: Some(m),
        ..ServeConfig::default()
    };
    let server = tokio::spawn(serve_with(listener, cid(0xFF), None, config));

    // Advertise C to the node and read back the set it now knows.
    let frame = Message::Gossip {
        members: vec![(
            C.as_bytes().to_vec(),
            C.as_bytes().to_vec(),
            0,
            MemberState::Alive,
        )],
    };
    let learned = gossip_exchange(&addr, cid(0xEE), frame)
        .await
        .expect("the node replies with its member set");

    // The reply carries the node's members — its original A and B, plus the C we
    // just taught it (the union is bidirectional in one exchange).
    let ids: Vec<Vec<u8>> = learned.iter().map(|(n, ..)| n.clone()).collect();
    assert!(ids.contains(&A.as_bytes().to_vec()), "reply includes A");
    assert!(ids.contains(&B.as_bytes().to_vec()), "reply includes B");
    assert!(
        ids.contains(&C.as_bytes().to_vec()),
        "reply includes the learned C"
    );
    server.abort();
}
