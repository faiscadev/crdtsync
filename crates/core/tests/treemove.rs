//! The pure Kleppmann-2021 move-log contract, decoupled from the tree's
//! materialisation: parent edges only, resolved by lamport order with
//! undo-and-replay on out-of-order receipt.
//!
//! The guarantees under test: exactly one parent per node, no cycles, no
//! duplication, and deterministic convergence — every replica that observes the
//! same set of moves reaches the same parent relation regardless of arrival
//! order. Position among siblings is a separate (Fugue) concern and is not
//! modelled here.

use crdtsync_core::clientid::ClientId;
use crdtsync_core::elementid::ElementId;
use crdtsync_core::stamp::Stamp;
use crdtsync_core::treemove::TreeMoves;

fn node(n: u8) -> ElementId {
    let mut b = [0u8; 16];
    b[0] = n;
    ElementId::from_bytes(b)
}

fn client(c: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = c;
    ClientId::from_bytes(b)
}

fn stamp(lamport: u64, c: u8) -> Stamp {
    Stamp {
        lamport,
        client: client(c),
    }
}

/// The set of parent edges, sorted, as a comparable convergence fingerprint.
fn edges(t: &TreeMoves) -> Vec<(ElementId, ElementId)> {
    let mut e: Vec<(ElementId, ElementId)> = t.edges().collect();
    e.sort_by_key(|(c, p)| (c.as_bytes(), p.as_bytes()));
    e
}

/// Walk every edge chain to a root; panics if any node reaches itself — the
/// no-cycle guarantee, checked independently of the implementation.
fn assert_acyclic(t: &TreeMoves) {
    for (child, _) in t.edges() {
        let mut cur = child;
        let mut hops = 0usize;
        while let Some(p) = t.parent_of(cur) {
            assert_ne!(p, child, "cycle: {child:?} reaches itself");
            cur = p;
            hops += 1;
            assert!(hops <= 1024, "walk did not terminate — a cycle escaped");
        }
    }
}

#[test]
fn a_move_sets_the_parent() {
    let mut t = TreeMoves::new();
    assert!(t.apply(stamp(1, 1), node(10), node(1)));
    assert_eq!(t.parent_of(node(10)), Some(node(1)));
    assert_eq!(t.parent_of(node(1)), None);
}

#[test]
fn a_later_move_wins_regardless_of_arrival_order() {
    // Applied in stamp order.
    let mut a = TreeMoves::new();
    a.apply(stamp(1, 1), node(10), node(1));
    a.apply(stamp(2, 1), node(10), node(2));
    assert_eq!(a.parent_of(node(10)), Some(node(2)));

    // Applied newest-first: the earlier move must not clobber the later one.
    let mut b = TreeMoves::new();
    b.apply(stamp(2, 1), node(10), node(2));
    b.apply(stamp(1, 1), node(10), node(1));
    assert_eq!(b.parent_of(node(10)), Some(node(2)));

    assert_eq!(edges(&a), edges(&b));
}

#[test]
fn a_self_move_is_ignored() {
    let mut t = TreeMoves::new();
    assert!(t.apply(stamp(1, 1), node(5), node(5)));
    assert_eq!(t.parent_of(node(5)), None);
    assert_acyclic(&t);
}

#[test]
fn a_move_that_would_form_a_cycle_is_ignored() {
    let mut t = TreeMoves::new();
    // c under p.
    t.apply(stamp(1, 1), node(20), node(10));
    // Now try to move p under c — a cycle; must be skipped, leaving both edges
    // as they were.
    t.apply(stamp(2, 1), node(10), node(20));
    assert_eq!(t.parent_of(node(20)), Some(node(10)));
    assert_eq!(t.parent_of(node(10)), None);
    assert_acyclic(&t);
}

#[test]
fn a_deep_cycle_is_ignored() {
    let mut t = TreeMoves::new();
    // Chain a > b > c (c under b under a).
    t.apply(stamp(1, 1), node(2), node(1));
    t.apply(stamp(2, 1), node(3), node(2));
    // Move a under c — a would become its own descendant; skip.
    t.apply(stamp(3, 1), node(1), node(3));
    assert_eq!(t.parent_of(node(1)), None);
    assert_eq!(t.parent_of(node(2)), Some(node(1)));
    assert_eq!(t.parent_of(node(3)), Some(node(2)));
    assert_acyclic(&t);
}

#[test]
fn every_ordering_of_the_same_moves_converges() {
    // Three moves of one child; the highest stamp must win from any order.
    let moves = [
        (stamp(1, 1), node(10), node(1)),
        (stamp(2, 2), node(10), node(2)),
        (stamp(3, 3), node(10), node(3)),
    ];
    let perms = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    let mut reference: Option<Vec<(ElementId, ElementId)>> = None;
    for perm in perms {
        let mut t = TreeMoves::new();
        for &i in &perm {
            let (s, c, p) = moves[i];
            t.apply(s, c, p);
        }
        assert_eq!(t.parent_of(node(10)), Some(node(3)));
        let e = edges(&t);
        match &reference {
            None => reference = Some(e),
            Some(r) => assert_eq!(&e, r, "ordering {perm:?} diverged"),
        }
    }
}

#[test]
fn concurrent_cycle_resolves_deterministically() {
    // A moves x under y (@ stamp 1); B moves y under x (@ stamp 2). The two are
    // concurrent; whichever the total order places second and would-cycle is the
    // one skipped. Both replicas must agree.
    let ma = (stamp(1, 1), node(1), node(2)); // x under y
    let mb = (stamp(2, 2), node(2), node(1)); // y under x

    let mut r1 = TreeMoves::new();
    r1.apply(ma.0, ma.1, ma.2);
    r1.apply(mb.0, mb.1, mb.2);

    let mut r2 = TreeMoves::new();
    r2.apply(mb.0, mb.1, mb.2);
    r2.apply(ma.0, ma.1, ma.2);

    assert_eq!(edges(&r1), edges(&r2));
    assert_acyclic(&r1);
    assert_acyclic(&r2);
    // Exactly one of the two edges survives (no duplication of the child, no cycle).
    assert_eq!(r1.edges().count(), 1);
}

#[test]
fn edges_are_yielded_in_a_deterministic_order() {
    // Insert several children out of id order; edges() must come back sorted by
    // child so the document layer serializes identically on every replica.
    let mut t = TreeMoves::new();
    t.apply(stamp(1, 1), node(30), node(1));
    t.apply(stamp(2, 1), node(10), node(1));
    t.apply(stamp(3, 1), node(20), node(1));
    let yielded: Vec<(ElementId, ElementId)> = t.edges().collect();
    let mut sorted = yielded.clone();
    sorted.sort_by_key(|(c, p)| (c.as_bytes(), p.as_bytes()));
    assert_eq!(yielded, sorted, "edges() must be deterministically ordered");
}

#[test]
fn reapplying_a_move_is_idempotent() {
    let mut once = TreeMoves::new();
    once.apply(stamp(1, 1), node(10), node(1));

    let mut twice = TreeMoves::new();
    assert!(twice.apply(stamp(1, 1), node(10), node(1)));
    assert!(!twice.apply(stamp(1, 1), node(10), node(1)));

    assert_eq!(edges(&once), edges(&twice));
    assert_eq!(twice.edges().count(), 1);
}

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 17
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() as usize) % n
    }
}

#[test]
fn a_random_move_stream_converges_across_orderings_and_stays_acyclic() {
    let seeds: u64 = if cfg!(miri) { 4 } else { 200 };
    for seed in 0..seeds {
        let mut gen = Rng::new(seed);
        // A pool of 6 nodes; each move relocates one under another, with a unique
        // ascending stamp so the total order is a strict permutation.
        let mut moves: Vec<(Stamp, ElementId, ElementId)> = Vec::new();
        for i in 0..24u64 {
            let c = node(gen.below(6) as u8);
            let p = node(gen.below(6) as u8);
            moves.push((stamp(i + 1, (gen.below(4) + 1) as u8), c, p));
        }

        // Replica 1: arrival == mint order.
        let mut r1 = TreeMoves::new();
        for &(s, c, p) in &moves {
            r1.apply(s, c, p);
        }

        // Replica 2: a shuffled arrival order (Fisher–Yates over the same set).
        let mut shuffled = moves.clone();
        for i in (1..shuffled.len()).rev() {
            let j = gen.below(i + 1);
            shuffled.swap(i, j);
        }
        let mut r2 = TreeMoves::new();
        for &(s, c, p) in &shuffled {
            r2.apply(s, c, p);
        }

        assert_eq!(edges(&r1), edges(&r2), "seed {seed}: orderings diverged");
        assert_acyclic(&r1);
        assert_acyclic(&r2);
    }
}
