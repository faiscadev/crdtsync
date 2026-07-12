//! Spec for deterministic room→replica-set placement (cluster Unit 1).
//!
//! The placement function is the foundation routing, replication, and failover
//! all consult, so its load-bearing property is that every node computes the
//! *same* ordered replica set from only the room id and the member set. These
//! tests pin determinism, balanced distribution, minimal reshuffle on a
//! membership change, replication-factor clamping, and leader-first stability.

use crdtsync_server::placement::{Cluster, NodeId};

fn nodes(count: usize) -> Vec<NodeId> {
    (0..count)
        .map(|i| NodeId::from(format!("node-{i:02}")))
        .collect()
}

fn rooms(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| format!("room-{i}").into_bytes())
        .collect()
}

#[test]
fn deterministic_same_inputs_same_output() {
    let cluster = Cluster::new(nodes(7));
    let room = b"room-alpha";
    let first = cluster.replicas(room, 3);
    assert_eq!(first.len(), 3);
    for _ in 0..1000 {
        assert_eq!(cluster.replicas(room, 3), first);
    }
}

#[test]
fn deterministic_across_member_input_order() {
    let mut forward = nodes(9);
    let mut reversed = forward.clone();
    reversed.reverse();
    // A different construction order (as two nodes might list members) must not
    // change any placement — the whole point of replica-identical placement.
    let a = Cluster::new(forward.drain(..));
    let b = Cluster::new(reversed.drain(..));
    for room in rooms(2000) {
        assert_eq!(a.replicas(&room, 3), b.replicas(&room, 3));
        assert_eq!(a.primary(&room), b.primary(&room));
    }
}

#[test]
fn deterministic_ignores_duplicate_members() {
    let a = Cluster::new(nodes(5));
    let mut dupes = nodes(5);
    dupes.extend(nodes(5));
    let b = Cluster::new(dupes);
    assert_eq!(a.len(), 5);
    assert_eq!(b.len(), 5);
    for room in rooms(500) {
        assert_eq!(a.replicas(&room, 3), b.replicas(&room, 3));
    }
}

#[test]
fn balanced_primary_distribution() {
    let k = 10;
    let cluster = Cluster::new(nodes(k));
    let total = 10_000;
    let mut owned = std::collections::HashMap::new();
    for room in rooms(total) {
        let primary = cluster.primary(&room).unwrap();
        *owned.entry(primary).or_insert(0usize) += 1;
    }
    assert_eq!(owned.len(), k, "every node owns some rooms");
    let expected = total / k;
    let tol = expected / 5; // within 20% of a fair share
    for node in cluster.nodes() {
        let count = owned[node];
        assert!(
            count.abs_diff(expected) <= tol,
            "node {node:?} owns {count}, expected ~{expected}",
        );
    }
}

#[test]
fn balanced_replica_membership() {
    let k = 10;
    let n = 3;
    let cluster = Cluster::new(nodes(k));
    let total = 10_000;
    let mut appears = std::collections::HashMap::new();
    for room in rooms(total) {
        for node in cluster.replicas(&room, n) {
            *appears.entry(node).or_insert(0usize) += 1;
        }
    }
    let expected = total * n / k;
    let tol = expected / 5;
    for node in cluster.nodes() {
        let count = appears[node];
        assert!(
            count.abs_diff(expected) <= tol,
            "node {node:?} appears in {count} replica slots, expected ~{expected}",
        );
    }
}

#[test]
fn minimal_reshuffle_primary_on_join() {
    let k = 10;
    let total = 10_000;
    let before = Cluster::new(nodes(k));
    let after = Cluster::new(nodes(k + 1));
    let mut moved = 0;
    for room in rooms(total) {
        if before.primary(&room) != after.primary(&room) {
            moved += 1;
        }
    }
    let frac = moved as f64 / total as f64;
    // A join should move only ~1/(k+1) of primaries; plain modulo hashing would
    // remap nearly all of them.
    let ideal = 1.0 / (k + 1) as f64;
    assert!(
        frac < ideal * 1.5,
        "moved fraction {frac} too high (ideal ~{ideal})"
    );
    assert!(moved > 0, "a join must move some rooms to the new node");
}

#[test]
fn minimal_reshuffle_replica_set_on_join() {
    let k = 10;
    let n = 3;
    let total = 10_000;
    let before = Cluster::new(nodes(k));
    let after = Cluster::new(nodes(k + 1));
    let mut moved = 0;
    for room in rooms(total) {
        if before.replicas(&room, n) != after.replicas(&room, n) {
            moved += 1;
        }
    }
    let frac = moved as f64 / total as f64;
    // A replica set changes only when the new node scores into the top-N, so
    // ~n/(k+1) of rooms — far below modulo's near-total churn.
    assert!(
        frac < 0.4,
        "replica-set churn {frac} too high on a single join"
    );
}

#[test]
fn minimal_reshuffle_on_leave() {
    let k = 11;
    let n = 3;
    let total = 10_000;
    let full = nodes(k);
    let leaving = full[k - 1].clone();
    let before = Cluster::new(full.clone());
    let after = Cluster::new(full[..k - 1].to_vec());
    let mut moved = 0;
    for room in rooms(total) {
        let b = before.replicas(&room, n);
        let a = after.replicas(&room, n);
        if a != b {
            moved += 1;
            // A room only changes if the departed node was one of its replicas.
            assert!(
                b.contains(&leaving),
                "room reshuffled without hosting the leaver"
            );
        }
    }
    let frac = moved as f64 / total as f64;
    assert!(frac < 0.4, "leave churn {frac} too high");
}

#[test]
fn placement_beats_modulo_reshuffle() {
    // The point of rendezvous hashing over plain modulo: a membership change
    // moves a small share of rooms, not almost all of them.
    let k = 10;
    let total = 10_000;
    let hrw_before = Cluster::new(nodes(k));
    let hrw_after = Cluster::new(nodes(k + 1));
    let mut hrw_moved = 0;
    let mut modulo_moved = 0;
    let members = nodes(k);
    let members_after = nodes(k + 1);
    for (i, room) in rooms(total).into_iter().enumerate() {
        if hrw_before.primary(&room) != hrw_after.primary(&room) {
            hrw_moved += 1;
        }
        if members[i % k] != members_after[i % (k + 1)] {
            modulo_moved += 1;
        }
    }
    assert!(
        hrw_moved * 4 < modulo_moved,
        "hrw moved {hrw_moved}, modulo moved {modulo_moved}",
    );
}

#[test]
fn replication_factor_distinct_nodes() {
    let cluster = Cluster::new(nodes(6));
    for room in rooms(500) {
        let set = cluster.replicas(&room, 3);
        assert_eq!(set.len(), 3);
        let mut sorted = set.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 3, "replicas must be distinct");
    }
}

#[test]
fn replication_factor_clamps_to_member_count() {
    let cluster = Cluster::new(nodes(3));
    let room = b"room-x";
    let set = cluster.replicas(room, 10);
    assert_eq!(
        set.len(),
        3,
        "n > member count yields all members, no dupes"
    );
    let mut sorted = set.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 3);
}

#[test]
fn empty_cluster_yields_empty() {
    let cluster = Cluster::new(Vec::new());
    assert!(cluster.is_empty());
    assert_eq!(cluster.replicas(b"room", 3), Vec::new());
    assert_eq!(cluster.primary(b"room"), None);
}

#[test]
fn zero_factor_yields_empty() {
    let cluster = Cluster::new(nodes(5));
    assert_eq!(cluster.replicas(b"room", 0), Vec::new());
}

#[test]
fn single_node_owns_everything() {
    let only = NodeId::from("solo");
    let cluster = Cluster::new(vec![only.clone()]);
    for room in rooms(200) {
        assert_eq!(cluster.replicas(&room, 3), vec![only.clone()]);
        assert_eq!(cluster.primary(&room), Some(only.clone()));
    }
}

#[test]
fn leader_first_order_is_stable() {
    let cluster = Cluster::new(nodes(8));
    for room in rooms(1000) {
        let primary = cluster.primary(&room).unwrap();
        // The leader is the head of the replica set for any factor.
        for n in 1..=8 {
            assert_eq!(cluster.replicas(&room, n)[0], primary);
        }
    }
}

#[test]
fn replica_set_is_a_growing_prefix() {
    let cluster = Cluster::new(nodes(8));
    for room in rooms(1000) {
        for n in 1..8 {
            let smaller = cluster.replicas(&room, n);
            let larger = cluster.replicas(&room, n + 1);
            assert_eq!(
                larger[..n],
                smaller[..],
                "adding a replica extends the order"
            );
        }
    }
}
