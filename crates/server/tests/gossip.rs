//! Anti-entropy gossip membership discovery (Cluster Unit 7a).
//!
//! A node no longer needs the whole cluster at boot: it seeds from one peer and
//! learns the rest by gossip. Each round two nodes exchange member sets and union
//! them, so a node that knows only a seed converges on the full cluster within a
//! few rounds. Placement is order-independent, so once two nodes have learned the
//! same members they place every room identically. These pin: a seed-only node
//! converges (and placement converges with it), the union is order-independent,
//! re-gossiping a fully-known set is inert, a learned member becomes usable as a
//! replica/redirect target, and a single-node deployment runs no gossip at all.

use std::sync::Arc;

use crdtsync_core::{ClientId, Message};
use crdtsync_server::gossip::{exchange, gossip_exchange, gossip_frame, merge_into};
use crdtsync_server::membership::Membership;
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

/// The wire payload a membership advertises — its known members as byte pairs.
fn payload(m: &Membership) -> Vec<(Vec<u8>, Vec<u8>)> {
    m.known_members()
        .into_iter()
        .map(|(node, addr)| (node.as_bytes().to_vec(), addr))
        .collect()
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

// --- (d) single-node regression: no membership, no gossip ---

#[test]
fn a_single_node_registry_knows_no_members_and_ignores_gossip() {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    assert!(r.membership().is_none());
    assert!(r.known_members().is_empty(), "no members to advertise");

    // Merging a gossip payload is inert with no membership.
    r.merge_gossip(vec![(A.as_bytes().to_vec(), A.as_bytes().to_vec())]);
    assert!(r.known_members().is_empty());
    assert!(r.membership().is_none());

    // A stray Gossip frame on a single-node node drops the connection.
    let peer = r.connect();
    let kept = r.deliver(
        peer,
        Message::Gossip {
            members: vec![(A.as_bytes().to_vec(), A.as_bytes().to_vec())],
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

    r.merge_gossip(vec![(C.as_bytes().to_vec(), C.as_bytes().to_vec())]);
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
fn the_gossip_frame_carries_every_known_member_with_its_address() {
    let m = seeded(A, &format!("{B},{C}"));
    let Message::Gossip { members } = gossip_frame(&m.known_members()) else {
        panic!("gossip_frame builds a Gossip message");
    };
    assert_eq!(members.len(), 3);
    assert!(members.contains(&(A.as_bytes().to_vec(), A.as_bytes().to_vec())));
    assert!(members.contains(&(B.as_bytes().to_vec(), B.as_bytes().to_vec())));
    assert!(members.contains(&(C.as_bytes().to_vec(), C.as_bytes().to_vec())));
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
        members: vec![(C.as_bytes().to_vec(), C.as_bytes().to_vec())],
    };
    let learned = gossip_exchange(&addr, cid(0xEE), frame)
        .await
        .expect("the node replies with its member set");

    // The reply carries the node's members — its original A and B, plus the C we
    // just taught it (the union is bidirectional in one exchange).
    let ids: Vec<Vec<u8>> = learned.iter().map(|(n, _)| n.clone()).collect();
    assert!(ids.contains(&A.as_bytes().to_vec()), "reply includes A");
    assert!(ids.contains(&B.as_bytes().to_vec()), "reply includes B");
    assert!(
        ids.contains(&C.as_bytes().to_vec()),
        "reply includes the learned C"
    );
    server.abort();
}
