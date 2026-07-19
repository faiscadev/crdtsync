//! SWIM indirect probing (ping-req) — hardening failure detection against a single
//! bad link.
//!
//! When a node's *direct* gossip probe to a member fails, it asks up to
//! `INDIRECT_PROBE_COUNT` other members to probe that member on its behalf before
//! counting the failure toward suspicion. A member reachable through any relay is
//! credited alive (`probe_outcome`) and its suspicion clock reset, so a transient
//! or asymmetric blip on one node's path does not falsely escalate it; only a
//! member every indirect probe *also* fails to reach counts toward `Suspect`/`Dead`.
//!
//! The relay-selection and outcome logic is driven in-process (deterministic, runs
//! under Miri); the ping-req wire path is exercised once over a loopback socket
//! (Miri-ignored).

use crdtsync_core::MemberState;
use crdtsync_server::gossip::{
    choose_relays, indirect_reachable, probe_outcome, GossipMember, INDIRECT_PROBE_COUNT,
};
use crdtsync_server::membership::{Membership, DEAD_AFTER_FAILURES, SUSPECT_AFTER_FAILURES};
use crdtsync_server::placement::NodeId;

const N: usize = 3;
const A: &str = "10.0.0.1:9000";
const B: &str = "10.0.0.2:9000";
const C: &str = "10.0.0.3:9000";
const D: &str = "10.0.0.4:9000";
const E: &str = "10.0.0.5:9000";

fn nid(addr: &str) -> NodeId {
    NodeId::from_addr(addr)
}

/// A membership whose self is `advertise` and whose seed peers are `peers`.
fn seeded(advertise: &str, peers: &str) -> Membership {
    Membership::from_static_config(None, Some(advertise), peers, N).unwrap()
}

/// The gossip member list (with liveness) `A` holds over the whole cluster.
fn cluster() -> Vec<GossipMember> {
    seeded(A, &format!("{B},{C},{D},{E}")).known_liveness()
}

// ------------------------- choose_relays -------------------------

#[test]
fn relays_exclude_self_and_target() {
    let members = cluster();
    let self_id = nid(A);
    let target = nid(B);
    let relays = choose_relays(&members, &self_id, &target, INDIRECT_PROBE_COUNT);
    assert!(!relays.iter().any(|(n, _)| *n == self_id), "self excluded");
    assert!(!relays.iter().any(|(n, _)| *n == target), "target excluded");
}

#[test]
fn relays_are_capped_and_distinct() {
    let members = cluster();
    let relays = choose_relays(&members, &nid(A), &nid(B), 2);
    assert_eq!(relays.len(), 2, "capped at k");
    let mut ids: Vec<_> = relays.iter().map(|(n, _)| n.clone()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 2, "relays are distinct");
}

#[test]
fn relays_return_all_eligible_when_k_exceeds() {
    // Cluster of {A,B,C,D,E}; self=A, target=B leaves {C,D,E} eligible.
    let members = cluster();
    let relays = choose_relays(&members, &nid(A), &nid(B), 100);
    let mut ids: Vec<_> = relays.iter().map(|(n, _)| n.clone()).collect();
    ids.sort();
    let mut expect = vec![nid(C), nid(D), nid(E)];
    expect.sort();
    assert_eq!(ids, expect);
}

#[test]
fn relays_skip_dead_members() {
    // Mark C dead in A's view; it must never be asked to relay a probe.
    let mut m = seeded(A, &format!("{B},{C},{D},{E}"));
    for _ in 0..DEAD_AFTER_FAILURES {
        m.note_gossip_unreachable(&nid(C));
    }
    assert_eq!(m.gossip_state(&nid(C)), MemberState::Dead);
    let members = m.known_liveness();
    let relays = choose_relays(&members, &nid(A), &nid(B), 100);
    assert!(
        !relays.iter().any(|(n, _)| *n == nid(C)),
        "a dead member is never chosen as a relay"
    );
}

#[test]
fn relays_empty_when_only_self_and_target() {
    // A two-node view: self and the target, nobody left to relay through.
    let members = seeded(A, B).known_liveness();
    let relays = choose_relays(&members, &nid(A), &nid(B), INDIRECT_PROBE_COUNT);
    assert!(relays.is_empty());
}

// ------------------------- outcome folding -------------------------

#[test]
fn indirect_reachable_needs_a_confirmed_relay() {
    assert!(!indirect_reachable(&[]));
    assert!(!indirect_reachable(&[None, Some(false)]));
    assert!(indirect_reachable(&[None, Some(false), Some(true)]));
}

#[test]
fn probe_outcome_folds_direct_and_indirect() {
    // A direct success stands alone — no relays consulted.
    assert!(probe_outcome(true, &[]));
    // A direct failure is rescued by any confirming relay.
    assert!(probe_outcome(false, &[Some(false), Some(true)]));
    // A direct failure with no confirming relay is a failure.
    assert!(!probe_outcome(false, &[None, Some(false)]));
}

// ------------------------- escalation over rounds -------------------------

#[test]
fn indirect_success_prevents_false_death() {
    let mut m = seeded(A, &format!("{B},{C},{D},{E}"));
    let target = nid(B);
    // Every round the direct probe fails, but a relay always confirms the target.
    // Run well past the death threshold: the target must never even be suspected.
    for _ in 0..DEAD_AFTER_FAILURES * 3 {
        let reachable = probe_outcome(false, &[Some(false), Some(true)]);
        if reachable {
            m.note_gossip_reachable(&target);
        } else {
            m.note_gossip_unreachable(&target);
        }
    }
    assert_eq!(
        m.gossip_state(&target),
        MemberState::Alive,
        "a member reachable through a relay is not falsely suspected"
    );
}

#[test]
fn truly_dead_is_declared_after_indirect_also_fails() {
    let mut m = seeded(A, &format!("{B},{C},{D},{E}"));
    let target = nid(B);
    // Direct and every indirect probe fail — the member is genuinely gone.
    for round in 1..=DEAD_AFTER_FAILURES {
        let reachable = probe_outcome(false, &[None, Some(false), Some(false)]);
        assert!(!reachable);
        m.note_gossip_unreachable(&target);
        let state = m.gossip_state(&target);
        if round < SUSPECT_AFTER_FAILURES {
            assert_eq!(state, MemberState::Alive);
        } else if round < DEAD_AFTER_FAILURES {
            assert_eq!(state, MemberState::Suspect);
        } else {
            assert_eq!(state, MemberState::Dead);
        }
    }
    assert_eq!(m.gossip_state(&target), MemberState::Dead);
}

#[test]
fn a_recovered_indirect_reach_resets_a_building_suspicion() {
    let mut m = seeded(A, &format!("{B},{C},{D},{E}"));
    let target = nid(B);
    // A couple of failed rounds build toward suspicion...
    for _ in 0..SUSPECT_AFTER_FAILURES - 1 {
        m.note_gossip_unreachable(&target);
    }
    assert_eq!(m.gossip_state(&target), MemberState::Alive);
    // ...then a relay confirms the target, clearing the failed-probe clock, so a
    // later isolated failure does not tip it straight over the threshold.
    if probe_outcome(false, &[Some(true)]) {
        m.note_gossip_reachable(&target);
    }
    m.note_gossip_unreachable(&target);
    assert_eq!(m.gossip_state(&target), MemberState::Alive);
}

// ===================== socket transport (Miri-ignored) =====================
//
// End-to-end ping-req over real sockets: a requester asks a live relay node for its
// liveness view of a third member, and the relay answers from the registry actor
// (its own gossip state + member set — no dial to the target). Binds loopback
// sockets, so it is excluded under Miri; the selection + outcome logic above runs
// under Miri.

fn cid(first: u8) -> crdtsync_core::ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    crdtsync_core::ClientId::from_bytes(b)
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials a loopback server over a real socket
async fn ping_req_confirms_a_known_live_member() {
    use crdtsync_server::gossip::ping_req_exchange;
    use crdtsync_server::runtime::{serve_with, ServeConfig};
    use tokio::net::TcpListener;

    // A live relay node that knows members {A, B, C}, none marked down.
    let relay_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_addr = relay_l.local_addr().unwrap().to_string();
    let config = ServeConfig {
        membership: Some(seeded(A, &format!("{B},{C}"))),
        ..ServeConfig::default()
    };
    let relay = tokio::spawn(serve_with(relay_l, cid(0xF0), None, config));

    // Ask the relay for its view of B — a member it knows and holds live.
    let verdict = ping_req_exchange(&relay_addr, cid(0xEE), B.as_bytes())
        .await
        .expect("the relay answers a ping-ack");
    assert!(verdict, "the relay vouches for a known, live member");

    relay.abort();
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials a loopback server over a real socket
async fn ping_req_declines_a_stranger() {
    use crdtsync_server::gossip::ping_req_exchange;
    use crdtsync_server::runtime::{serve_with, ServeConfig};
    use tokio::net::TcpListener;

    // A live relay that knows only {A, B, C} — never learned D.
    let relay_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_addr = relay_l.local_addr().unwrap().to_string();
    let config = ServeConfig {
        membership: Some(seeded(A, &format!("{B},{C}"))),
        ..ServeConfig::default()
    };
    let relay = tokio::spawn(serve_with(relay_l, cid(0xF0), None, config));

    // Ask about D — not one of the relay's members: it never vouches for a
    // stranger (nor dials it), so the answer is unreachable, not optimistic.
    let verdict = ping_req_exchange(&relay_addr, cid(0xEE), D.as_bytes())
        .await
        .expect("the relay still answers a ping-ack");
    assert!(!verdict, "the relay does not vouch for a non-member");

    relay.abort();
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // dials a loopback address with nothing listening
async fn ping_req_to_an_unreachable_relay_is_no_evidence() {
    use crdtsync_server::gossip::ping_req_exchange;
    use tokio::net::TcpListener;

    // Bind then drop a listener to get an address nothing answers on.
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_relay = l.local_addr().unwrap().to_string();
    drop(l);
    let verdict = ping_req_exchange(&dead_relay, cid(0xEE), C.as_bytes()).await;
    assert_eq!(verdict, None, "an unreachable relay yields no verdict");
}
