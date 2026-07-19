//! Member reaping — bounding the roster so departed nodes do not accumulate.
//!
//! Gossip failure detection marks a gone node `Dead` and excludes it from
//! leadership, but it lingers in the placement set forever. Reaping removes a
//! member that has stayed `Dead` past a bounded dead-time (`REAP_AFTER_DEAD_TICKS`
//! reap checks), so the membership list does not grow without bound as nodes
//! depart. It is convergent and fail-safe: every replica reaps the same member off
//! its own `Dead` observation; a reaped member is tombstoned so stale gossip cannot
//! resurrect it, while a genuinely-returned node (a strictly higher incarnation)
//! escapes the tombstone and rejoins. A live or only-recently-dead member is never
//! reaped.
//!
//! Reaping is tick-driven (a reap check per sweep), not wall-clock, so the state
//! machine is deterministic and runs under Miri.

use crdtsync_core::{ClientId, MemberState};
use crdtsync_server::membership::{
    Membership, DEAD_AFTER_FAILURES, REAP_AFTER_DEAD_TICKS, SUSPECT_AFTER_FAILURES,
};
use crdtsync_server::placement::NodeId;
use crdtsync_server::Registry;

const N: usize = 3;
const A: &str = "10.0.0.1:9000";
const B: &str = "10.0.0.2:9000";
const C: &str = "10.0.0.3:9000";
const D: &str = "10.0.0.4:9000";

fn nid(addr: &str) -> NodeId {
    NodeId::from_addr(addr)
}

fn seeded(advertise: &str, peers: &str) -> Membership {
    Membership::from_static_config(None, Some(advertise), peers, N).unwrap()
}

fn cluster() -> Membership {
    seeded(A, &format!("{B},{C},{D}"))
}

/// Drive `node` to `Dead` in `m` — enough consecutive failed probes.
fn kill(m: &mut Membership, node: &NodeId) {
    for _ in 0..DEAD_AFTER_FAILURES {
        m.note_gossip_unreachable(node);
    }
    assert_eq!(m.gossip_state(node), MemberState::Dead);
}

/// Run `n` reap checks, returning every id reaped across them.
fn reap_n(m: &mut Membership, n: u32) -> Vec<NodeId> {
    let mut reaped = Vec::new();
    for _ in 0..n {
        reaped.extend(m.reap_dead());
    }
    reaped
}

#[test]
fn a_member_dead_past_the_threshold_is_reaped() {
    let mut m = cluster();
    let d = nid(D);
    kill(&mut m, &d);
    let reaped = reap_n(&mut m, REAP_AFTER_DEAD_TICKS);
    assert!(
        reaped.contains(&d),
        "D is reaped once dead past the threshold"
    );
    assert!(!m.members().contains(&d), "D is gone from the roster");
    assert!(!m.is_member(&d), "D is no longer a known member");
}

#[test]
fn reap_returns_the_reaped_ids_once() {
    let mut m = cluster();
    let d = nid(D);
    kill(&mut m, &d);
    // Below the threshold, no reap.
    let early = reap_n(&mut m, REAP_AFTER_DEAD_TICKS - 1);
    assert!(early.is_empty(), "not reaped before the threshold");
    // The threshold tick reaps it exactly once.
    let at = m.reap_dead();
    assert_eq!(at, vec![d.clone()]);
    // Further ticks reap nothing more — it is already gone.
    assert!(m.reap_dead().is_empty(), "idempotent after removal");
}

#[test]
fn a_recently_dead_member_is_not_reaped() {
    let mut m = cluster();
    let d = nid(D);
    kill(&mut m, &d);
    reap_n(&mut m, REAP_AFTER_DEAD_TICKS - 1);
    assert!(
        m.is_member(&d),
        "a member dead below the threshold survives"
    );
    assert_eq!(m.gossip_state(&d), MemberState::Dead);
}

#[test]
fn a_suspect_member_is_not_reaped() {
    let mut m = cluster();
    let d = nid(D);
    for _ in 0..SUSPECT_AFTER_FAILURES {
        m.note_gossip_unreachable(&d);
    }
    assert_eq!(m.gossip_state(&d), MemberState::Suspect);
    // Suspect is not Dead — the reap clock never starts, however many checks run.
    reap_n(&mut m, REAP_AFTER_DEAD_TICKS * 2);
    assert!(
        m.is_member(&d),
        "a Suspect (not Dead) member is never reaped"
    );
}

#[test]
fn a_live_member_is_never_reaped() {
    let mut m = cluster();
    let reaped = reap_n(&mut m, REAP_AFTER_DEAD_TICKS * 3);
    assert!(reaped.is_empty(), "no live member is reaped");
    assert_eq!(m.members().len(), 4, "the full roster stays");
}

#[test]
fn self_is_never_reaped() {
    let mut m = cluster();
    let me = m.self_id().clone();
    reap_n(&mut m, REAP_AFTER_DEAD_TICKS * 3);
    assert!(m.is_member(&me), "self is always a member");
}

#[test]
fn a_recovered_member_before_reaping_is_kept() {
    let mut m = cluster();
    let d = nid(D);
    kill(&mut m, &d);
    reap_n(&mut m, REAP_AFTER_DEAD_TICKS - 1);
    // D refutes with a higher incarnation just before the reap tick — its reap
    // clock resets and it is not removed.
    m.merge_liveness([(d.clone(), D.as_bytes().to_vec(), 5, MemberState::Alive)]);
    assert_eq!(m.gossip_state(&d), MemberState::Alive);
    reap_n(&mut m, REAP_AFTER_DEAD_TICKS);
    assert!(
        m.is_member(&d),
        "a member that recovered before reaping is kept"
    );
}

#[test]
fn reaping_is_idempotent() {
    let mut m = cluster();
    let d = nid(D);
    kill(&mut m, &d);
    reap_n(&mut m, REAP_AFTER_DEAD_TICKS);
    let before = m.members().to_vec();
    // Reaping again changes nothing and reaps nobody.
    assert!(reap_n(&mut m, REAP_AFTER_DEAD_TICKS).is_empty());
    assert_eq!(m.members().to_vec(), before);
}

#[test]
fn reaping_converges_across_replicas() {
    // Two independent views of the same cluster both reap D — their rosters agree.
    let mut a = seeded(A, &format!("{B},{C},{D}"));
    let mut b = seeded(B, &format!("{A},{C},{D}"));
    let d = nid(D);
    kill(&mut a, &d);
    kill(&mut b, &d);
    reap_n(&mut a, REAP_AFTER_DEAD_TICKS);
    reap_n(&mut b, REAP_AFTER_DEAD_TICKS);
    assert_eq!(
        a.members().to_vec(),
        b.members().to_vec(),
        "both replicas converge on the same reaped roster"
    );
    assert!(!a.members().contains(&d) && !b.members().contains(&d));
}

#[test]
fn a_reaped_member_is_not_relearned_from_stale_gossip() {
    let mut m = cluster();
    let d = nid(D);
    let d_inc = m.incarnation(&d);
    kill(&mut m, &d);
    reap_n(&mut m, REAP_AFTER_DEAD_TICKS);
    assert!(!m.is_member(&d));
    // A peer that has not yet reaped D keeps gossiping it Dead at the same
    // incarnation. The tombstone must keep it out — no reap-then-resurrect.
    m.merge_liveness([(d.clone(), D.as_bytes().to_vec(), d_inc, MemberState::Dead)]);
    assert!(
        !m.is_member(&d),
        "stale Dead gossip does not resurrect a reaped member"
    );
    // The additive union path is likewise blocked.
    m.add_member(d.clone(), D.as_bytes().to_vec());
    assert!(
        !m.is_member(&d),
        "a plain re-advertise does not resurrect it either"
    );
}

#[test]
fn the_registry_reaps_through_its_sweep_seam() {
    // The runtime drives reaping through `Registry::reap_dead_members` on the sweep
    // cadence — assert the seam removes a dead member from the live placement view.
    let mut r = Registry::new(ClientId::from_bytes([0xFF; 16]));
    r.set_membership(cluster());
    let d = nid(D);
    // Drive D to Dead through the gossip-probe seam the actor uses.
    for _ in 0..DEAD_AFTER_FAILURES {
        r.note_gossip_probe(d.clone(), false);
    }
    assert!(r.membership().unwrap().members().contains(&d));
    for _ in 0..REAP_AFTER_DEAD_TICKS {
        r.reap_dead_members();
    }
    assert!(
        !r.membership().unwrap().members().contains(&d),
        "the registry sweep seam reaps a durably-dead member"
    );
}

#[test]
fn reaping_is_inert_without_membership() {
    // Single-node mode has no membership — reaping is a no-op, never a panic.
    let mut r = Registry::new(ClientId::from_bytes([0xFF; 16]));
    r.reap_dead_members();
    assert!(r.membership().is_none());
}

#[test]
fn a_returned_member_with_a_higher_incarnation_is_resurrected() {
    let mut m = cluster();
    let d = nid(D);
    kill(&mut m, &d);
    let reap_inc = m.incarnation(&d);
    reap_n(&mut m, REAP_AFTER_DEAD_TICKS);
    assert!(!m.is_member(&d));
    // D returns Alive at a higher incarnation (it refuted before rejoining) — it
    // escapes the tombstone and rejoins the roster.
    m.merge_liveness([(
        d.clone(),
        D.as_bytes().to_vec(),
        reap_inc + 1,
        MemberState::Alive,
    )]);
    assert!(m.is_member(&d), "a genuinely-returned node rejoins");
    assert_eq!(m.gossip_state(&d), MemberState::Alive);
}

#[test]
fn a_crash_restarted_member_rejoins_at_incarnation_zero() {
    // The load-bearing case: a node crashes, is reaped everywhere, then restarts
    // FRESH — back at incarnation 0, unaware it was ever declared dead. Its Alive
    // self-advertisement must still let it rejoin (a stale incarnation must not
    // exile it forever). The escape keys on liveness, not incarnation.
    let mut m = cluster();
    let d = nid(D);
    // Refute D up to a non-zero incarnation first, so "reaped incarnation" > 0 and a
    // naive incarnation gate would permanently exclude the rebooted D.
    m.merge_liveness([(d.clone(), D.as_bytes().to_vec(), 7, MemberState::Alive)]);
    kill(&mut m, &d);
    assert!(m.incarnation(&d) >= 7);
    reap_n(&mut m, REAP_AFTER_DEAD_TICKS);
    assert!(!m.is_member(&d), "D was reaped");
    // Rebooted D gossips itself Alive at incarnation 0.
    m.merge_liveness([(d.clone(), D.as_bytes().to_vec(), 0, MemberState::Alive)]);
    assert!(
        m.is_member(&d),
        "a crash-restarted node at incarnation 0 still rejoins"
    );
    assert_eq!(m.gossip_state(&d), MemberState::Alive);
}

#[test]
fn a_higher_incarnation_dead_does_not_resurrect() {
    // A peer still holding the member Dead at a higher incarnation (it refuted then
    // died again before being reaped elsewhere) must NOT pull the reaped member back
    // into the roster — only a live return escapes, so there is no re-reap churn.
    let mut m = cluster();
    let d = nid(D);
    kill(&mut m, &d);
    reap_n(&mut m, REAP_AFTER_DEAD_TICKS);
    assert!(!m.is_member(&d));
    m.merge_liveness([(d.clone(), D.as_bytes().to_vec(), 99, MemberState::Dead)]);
    assert!(
        !m.is_member(&d),
        "a higher-incarnation Dead does not resurrect a reaped member"
    );
    m.merge_liveness([(d.clone(), D.as_bytes().to_vec(), 99, MemberState::Suspect)]);
    assert!(!m.is_member(&d), "nor does a Suspect");
}
