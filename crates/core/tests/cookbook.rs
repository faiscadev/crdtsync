//! Composition cookbook — higher-level data types built from the closed set of
//! CRDT primitives, no new engine support.
//!
//! crdtsync ships a fixed primitive set (Register, Counter, Map, List, Text) and
//! the claim that apps compose everything else from them rather than defining new
//! CRDT types. This file is that claim, executable: each recipe assembles a
//! familiar structure — a Set, a bounded counter, a multi-value register, a
//! tagged document — out of the primitives, and asserts it behaves, including
//! that concurrent replicas converge. The recipes are the documentation; the
//! assertions keep them honest as the primitives evolve.

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Element, Op, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

/// Fold every op of `ops` into `d` — the merge side of a two-replica exchange.
fn apply_all(d: &mut Document, ops: &[Op]) {
    for op in ops {
        d.apply(op);
    }
}

// --- Set: a Map whose present keys are the members ---
//
// A Set is a Map<member, true>: adding a member sets its key, removing it deletes
// the key, and membership is the key's presence. Add/remove converge with the
// Map's per-key last-writer-wins.

fn set_add(d: &mut Document, set: &[u8], member: &[u8]) -> Vec<Op> {
    d.transact(|tx| tx.map(set).set(member, Scalar::Bool(true)))
}

fn set_remove(d: &mut Document, set: &[u8], member: &[u8]) -> Vec<Op> {
    d.transact(|tx| tx.map(set).delete(member))
}

fn set_contains(d: &Document, set: &[u8], member: &[u8]) -> bool {
    match d.get(set) {
        Some(Element::Map(m)) => m.borrow().get(member).is_some(),
        _ => false,
    }
}

#[test]
fn a_set_is_a_map_of_present_keys() {
    let mut d = doc(1);
    set_add(&mut d, b"members", b"alice");
    set_add(&mut d, b"members", b"bob");
    assert!(set_contains(&d, b"members", b"alice"));
    assert!(set_contains(&d, b"members", b"bob"));
    assert!(!set_contains(&d, b"members", b"carol"));

    set_remove(&mut d, b"members", b"alice");
    assert!(
        !set_contains(&d, b"members", b"alice"),
        "removal drops membership"
    );
    assert!(set_contains(&d, b"members", b"bob"));
}

#[test]
fn set_membership_converges_across_replicas() {
    let mut a = doc(1);
    let mut b = doc(2);
    // Concurrent adds to the same set from two replicas.
    let from_a = set_add(&mut a, b"members", b"alice");
    let from_b = set_add(&mut b, b"members", b"bob");
    apply_all(&mut a, &from_b);
    apply_all(&mut b, &from_a);
    // Both members are present on both replicas — the union.
    for d in [&a, &b] {
        assert!(set_contains(d, b"members", b"alice"));
        assert!(set_contains(d, b"members", b"bob"));
    }
}

// --- Bounded counter: a Counter clamped on read ---
//
// The Counter merges by summing per-replica increments; bounds are an app
// concern applied when reading, so replicas still converge on the raw total and
// every reader clamps identically.

fn counter_value(d: &Document, key: &[u8]) -> i64 {
    match d.get(key) {
        Some(Element::Counter(c)) => c.borrow().read(),
        _ => 0,
    }
}

fn clamped(value: i64, min: i64, max: i64) -> i64 {
    value.max(min).min(max)
}

#[test]
fn a_bounded_counter_clamps_on_read() {
    let mut d = doc(1);
    d.transact(|tx| tx.inc(b"seats", 3));
    d.transact(|tx| tx.inc(b"seats", 5));
    // Raw total is the merged sum; the cap lives only at the read.
    assert_eq!(counter_value(&d, b"seats"), 8);
    assert_eq!(
        clamped(counter_value(&d, b"seats"), 0, 6),
        6,
        "read caps at the max"
    );
}

#[test]
fn a_bounded_counter_sums_concurrent_increments() {
    let mut a = doc(1);
    let mut b = doc(2);
    let from_a = a.transact(|tx| tx.inc(b"seats", 2));
    let from_b = b.transact(|tx| tx.inc(b"seats", 4));
    apply_all(&mut a, &from_b);
    apply_all(&mut b, &from_a);
    // Both converge on the summed total, then clamp identically.
    assert_eq!(counter_value(&a, b"seats"), 6);
    assert_eq!(counter_value(&b, b"seats"), 6);
    assert_eq!(clamped(counter_value(&a, b"seats"), 0, 5), 5);
}

// --- Multi-value register: a List of concurrent values ---
//
// A last-writer-wins Register collapses concurrent writes to one; when an app
// wants to surface every concurrent value (a "conflict" a human resolves), it
// appends to a List instead. Concurrent appends both survive the merge.

fn mv_values(d: &Document, key: &[u8]) -> Vec<i64> {
    match d.get(key) {
        Some(Element::List(list)) => {
            let list = list.borrow();
            (0..list.len())
                .filter_map(|i| match list.get(i) {
                    Some(Element::Scalar(Scalar::Int(n))) => Some(n),
                    _ => None,
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

#[test]
fn a_multi_value_register_keeps_concurrent_writes() {
    let mut a = doc(1);
    let mut b = doc(2);
    // Each replica records its own value concurrently at the list head.
    let from_a = a.transact(|tx| tx.list(b"name").insert(0, Scalar::Int(10)));
    let from_b = b.transact(|tx| tx.list(b"name").insert(0, Scalar::Int(20)));
    apply_all(&mut a, &from_b);
    apply_all(&mut b, &from_a);

    // Both concurrent values survive on both replicas, in the same order.
    let mut on_a = mv_values(&a, b"name");
    let mut on_b = mv_values(&b, b"name");
    assert_eq!(on_a, on_b, "replicas converge on one ordering");
    on_a.sort();
    on_b.sort();
    assert_eq!(on_a, vec![10, 20], "no concurrent value is lost");
}

// --- Tagged document: primitives composed under one Map ---
//
// A realistic record nests recipes: a Register for a scalar field, a Counter for
// a tally, a Map-as-Set for tags. One transaction builds the whole tree.

#[test]
fn a_document_composes_register_counter_and_set() {
    let mut d = doc(1);
    d.transact(|tx| {
        let mut post = tx.map(b"post");
        post.register(b"title", Scalar::Bytes(b"hello".to_vec()));
        post.inc(b"votes", 1);
        post.map(b"tags").set(b"rust", Scalar::Bool(true));
    });

    let Some(Element::Map(post)) = d.get(b"post") else {
        panic!("expected the post map");
    };
    let post = post.borrow();
    match post.get(b"title") {
        Some(Element::Register(r)) => {
            assert_eq!(r.borrow().read(), &Scalar::Bytes(b"hello".to_vec()))
        }
        _ => panic!("expected the title register"),
    }
    match post.get(b"votes") {
        Some(Element::Counter(c)) => assert_eq!(c.borrow().read(), 1),
        _ => panic!("expected the votes counter"),
    }
    match post.get(b"tags") {
        Some(Element::Map(tags)) => assert!(tags.borrow().get(b"rust").is_some()),
        _ => panic!("expected the tags set"),
    }
}
