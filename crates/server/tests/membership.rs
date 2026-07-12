//! Spec for cluster membership + static join (cluster Unit 2).
//!
//! A node learns its cluster's member set from static config — its own advertise
//! address plus a peer list — and holds a live [`Membership`] view over the
//! Unit-1 [`Cluster`] placement. The load-bearing property is cross-node
//! agreement: two nodes built from the same peer set (differing only in which
//! member is `self`) compute the *same* replica set for a room and each answers
//! `owns`/`is_primary_for` consistently for its own id. These tests pin static
//! parsing (a malformed list is a clean error, never a panic), the single-node
//! default (self owns and leads every room), and that self's view agrees with
//! the shared placement.

use crdtsync_server::membership::{Membership, MembershipConfigError, DEFAULT_REPLICATION_FACTOR};
use crdtsync_server::placement::{Cluster, NodeId};

const N: usize = 3;

fn peer_addrs(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("10.0.0.{i}:9000")).collect()
}

#[test]
fn static_join_parses_self_and_peers() {
    let peers = peer_addrs(4).join(",");
    let m = Membership::from_static_config(None, Some("10.0.0.9:9000"), &peers, N).unwrap();
    // Self is always a member, alongside every parsed peer.
    assert!(m.is_self(&NodeId::from_addr("10.0.0.9:9000")));
    assert_eq!(m.members().len(), 5);
    for addr in peer_addrs(4) {
        assert!(m.members().contains(&NodeId::from_addr(&addr)));
    }
}

#[test]
fn self_is_always_a_member_even_absent_from_peers() {
    let m = Membership::from_static_config(None, Some("host-self:9000"), "host-a:9000", N).unwrap();
    assert!(m.is_self(&NodeId::from_addr("host-self:9000")));
    assert!(m.members().contains(&NodeId::from_addr("host-self:9000")));
    assert!(m.members().contains(&NodeId::from_addr("host-a:9000")));
}

#[test]
fn self_listed_among_peers_does_not_duplicate() {
    let peers = "host-self:9000,host-a:9000,host-self:9000";
    let m = Membership::from_static_config(None, Some("host-self:9000"), peers, N).unwrap();
    assert_eq!(m.members().len(), 2);
}

#[test]
fn explicit_node_id_overrides_advertise_addr() {
    let m = Membership::from_static_config(Some("node-7"), Some("10.0.0.1:9000"), "host-a:9000", N)
        .unwrap();
    assert_eq!(m.self_id(), &NodeId::from("node-7"));
    assert!(m.is_self(&NodeId::from("node-7")));
}

#[test]
fn empty_peer_list_is_single_node() {
    let m = Membership::from_static_config(None, Some("solo:9000"), "", N).unwrap();
    assert_eq!(m.members().len(), 1);
    assert!(m.is_self(&NodeId::from_addr("solo:9000")));
}

#[test]
fn whitespace_peer_list_is_single_node() {
    let m = Membership::from_static_config(None, Some("solo:9000"), "   ", N).unwrap();
    assert_eq!(m.members().len(), 1);
}

#[test]
fn peer_entries_are_trimmed() {
    let m =
        Membership::from_static_config(None, Some("solo:9000"), " host-a:9000 , host-b:9000 ", N)
            .unwrap();
    assert_eq!(m.members().len(), 3);
    assert!(m.members().contains(&NodeId::from_addr("host-a:9000")));
    assert!(m.members().contains(&NodeId::from_addr("host-b:9000")));
}

#[test]
fn malformed_peer_list_is_a_clean_error() {
    // A blank entry between commas is a config mistake, surfaced as an error
    // rather than silently dropped or a panic.
    let err =
        Membership::from_static_config(None, Some("solo:9000"), "host-a:9000,,host-b:9000", N)
            .unwrap_err();
    assert!(matches!(err, MembershipConfigError::EmptyPeer));
}

#[test]
fn peers_without_self_identity_is_a_clean_error() {
    // Peers configured but no advertise address or node id: the node cannot know
    // its own membership, so startup fails cleanly instead of guessing.
    let err = Membership::from_static_config(None, None, "host-a:9000", N).unwrap_err();
    assert!(matches!(err, MembershipConfigError::MissingSelfId));
}

#[test]
fn blank_self_identity_is_missing_not_a_zero_length_id() {
    // A blank advertise address (a templated deploy exporting the var empty) is
    // absent, not a zero-length self id that would place the node against an id
    // no peer addresses.
    let err = Membership::from_static_config(None, Some("   "), "host-a:9000", N).unwrap_err();
    assert!(matches!(err, MembershipConfigError::MissingSelfId));
    let err = Membership::from_static_config(Some(""), None, "host-a:9000", N).unwrap_err();
    assert!(matches!(err, MembershipConfigError::MissingSelfId));
}

#[test]
fn padded_node_id_matches_a_peers_derivation() {
    // Cross-node agreement needs a physical node's self id to equal the id every
    // peer derives for it. A padded node id is trimmed to the same bytes a peer's
    // trimmed `from_addr` yields, so the two never diverge on whitespace.
    let m = Membership::from_static_config(Some("  node-7  "), None, "host-a:9000", N).unwrap();
    assert_eq!(m.self_id(), &NodeId::from_addr(" node-7 "));
    assert!(m.is_self(&NodeId::from("node-7")));
}

#[test]
fn single_node_owns_and_leads_every_room() {
    let m = Membership::from_static_config(None, Some("solo:9000"), "", N).unwrap();
    for i in 0..500 {
        let room = format!("room-{i}").into_bytes();
        assert!(m.owns(&room), "single node owns every room");
        assert!(m.is_primary_for(&room), "single node leads every room");
        assert_eq!(m.replicas_for(&room), vec![NodeId::from_addr("solo:9000")]);
        assert_eq!(m.primary_for(&room), Some(NodeId::from_addr("solo:9000")));
    }
}

#[test]
fn self_view_agrees_with_shared_placement() {
    let peers = peer_addrs(6).join(",");
    let m = Membership::from_static_config(None, Some("10.0.0.100:9000"), &peers, N).unwrap();
    let cluster = Cluster::new(m.members().iter().cloned());
    for i in 0..1000 {
        let room = format!("room-{i}").into_bytes();
        // The node's own queries are exactly the shared placement, evaluated for
        // its own id — no independent, possibly-divergent computation.
        assert_eq!(m.replicas_for(&room), cluster.replicas(&room, N));
        assert_eq!(m.primary_for(&room), cluster.primary(&room));
        assert_eq!(
            m.owns(&room),
            cluster.replicas(&room, N).contains(m.self_id())
        );
        assert_eq!(
            m.is_primary_for(&room),
            cluster.primary(&room).as_ref() == Some(m.self_id())
        );
    }
}

#[test]
fn two_nodes_same_peer_set_agree_on_placement() {
    // The load-bearing cross-node property: two distinct nodes, each built from
    // the same total member set (differing only in which member is self), place
    // every room identically and each correctly answers for its own id.
    let all = peer_addrs(5);
    let node_a_self = &all[0];
    let node_b_self = &all[3];
    let peers_of = |me: &str| -> String {
        all.iter()
            .filter(|a| a.as_str() != me)
            .cloned()
            .collect::<Vec<_>>()
            .join(",")
    };
    let a =
        Membership::from_static_config(None, Some(node_a_self), &peers_of(node_a_self), N).unwrap();
    let b =
        Membership::from_static_config(None, Some(node_b_self), &peers_of(node_b_self), N).unwrap();
    assert_eq!(a.members(), b.members(), "same total member set");

    for i in 0..2000 {
        let room = format!("room-{i}").into_bytes();
        assert_eq!(a.replicas_for(&room), b.replicas_for(&room));
        assert_eq!(a.primary_for(&room), b.primary_for(&room));
        // Each node's ownership matches whether its own id is in the shared set.
        assert_eq!(a.owns(&room), a.replicas_for(&room).contains(a.self_id()));
        assert_eq!(b.owns(&room), b.replicas_for(&room).contains(b.self_id()));
        // A room's single primary is led by exactly one of them (or another peer).
        if a.is_primary_for(&room) {
            assert!(!b.is_primary_for(&room), "one primary per room");
        }
    }
}

#[test]
fn every_room_is_owned_by_some_node() {
    // Across the cluster the replica sets cover every room: for any room, the
    // union of all nodes' `owns` is non-empty — no room is orphaned.
    let all = peer_addrs(4);
    let views: Vec<Membership> = all
        .iter()
        .map(|me| {
            let peers = all
                .iter()
                .filter(|a| a.as_str() != me.as_str())
                .cloned()
                .collect::<Vec<_>>()
                .join(",");
            Membership::from_static_config(None, Some(me), &peers, N).unwrap()
        })
        .collect();
    for i in 0..500 {
        let room = format!("room-{i}").into_bytes();
        let owners = views.iter().filter(|v| v.owns(&room)).count();
        assert_eq!(
            owners,
            N.min(all.len()),
            "replica factor nodes own each room"
        );
        assert_eq!(views.iter().filter(|v| v.is_primary_for(&room)).count(), 1);
    }
}

#[test]
fn address_to_node_id_is_deterministic() {
    // Every node must derive the same id for a peer from its advertise address,
    // or two nodes would disagree on placement. Derivation is a pure function of
    // the address bytes.
    assert_eq!(
        NodeId::from_addr("host-a:9000"),
        NodeId::from_addr("host-a:9000")
    );
    assert_ne!(
        NodeId::from_addr("host-a:9000"),
        NodeId::from_addr("host-b:9000")
    );
    // Surrounding whitespace does not change the id — the peer-list carrier may
    // pad entries.
    assert_eq!(
        NodeId::from_addr("  host-a:9000 "),
        NodeId::from_addr("host-a:9000")
    );
}

#[test]
fn default_replication_factor_is_reasonable() {
    // The default clamps to the member count, so a small cluster still resolves.
    let m = Membership::from_static_config(None, Some("solo:9000"), "", DEFAULT_REPLICATION_FACTOR)
        .unwrap();
    assert_eq!(m.replicas_for(b"room").len(), 1);
    assert_eq!(m.replication_factor(), DEFAULT_REPLICATION_FACTOR);
}
