//! List — an ordered sequence CRDT (Fugue). Items are Elements; each insert
//! is placed relative to its neighbours in a tree whose in-order traversal is
//! the sequence, so concurrent inserts at the same gap never interleave and
//! all replicas converge on one order. Deletes tombstone (positions must
//! survive to anchor concurrent inserts); the same algorithm backs Text.

use crdtsync_core::list::List;
use crdtsync_core::{Counter, Element, Scalar};
use std::cell::RefCell;
use std::rc::Rc;

mod common;
use common::{cid, default_id, eid, stmp};

/// One captured insert op: the node id, its value, and its placement.
type InsertOp = (crdtsync_core::Stamp, Element, crdtsync_core::list::Anchor);

fn list() -> List {
    List::new(default_id())
}

/// A one-byte scalar item, standing in for a character.
fn ch(c: u8) -> Element {
    Element::Scalar(Scalar::Bytes(vec![c]))
}

/// The live sequence as a string (each item is one byte).
fn text(l: &List) -> String {
    l.values()
        .iter()
        .map(|e| match e {
            Element::Scalar(Scalar::Bytes(b)) if b.len() == 1 => b[0] as char,
            _ => panic!("expected a one-byte scalar item"),
        })
        .collect()
}

/// Insert byte `c` at `index`, tagged with stamp `(lamport, client)`.
fn ins(l: &mut List, index: usize, c: u8, lamport: u64, client: u8) {
    l.insert(index, ch(c), stmp(lamport, client));
}

/// Insert a whole run from one client at ascending indices from `start`.
fn ins_run(l: &mut List, start: usize, s: &str, first_lamport: u64, client: u8) {
    for (i, c) in s.bytes().enumerate() {
        ins(l, start + i, c, first_lamport + i as u64, client);
    }
}

// --- construction / read ---

#[test]
fn new_is_empty() {
    let l = list();
    assert_eq!(l.len(), 0);
    assert!(l.is_empty());
    assert_eq!(text(&l), "");
}

#[test]
fn new_stores_id() {
    let l = List::new(eid(7, 42));
    assert_eq!(l.id(), eid(7, 42));
}

// --- local insert ---

#[test]
fn insert_builds_the_sequence_in_order() {
    let mut l = list();
    ins_run(&mut l, 0, "abc", 1, 1);
    assert_eq!(text(&l), "abc");
    assert_eq!(l.len(), 3);
}

#[test]
fn insert_at_front_prepends() {
    let mut l = list();
    ins(&mut l, 0, b'b', 1, 1);
    ins(&mut l, 0, b'a', 2, 1);
    assert_eq!(text(&l), "ab");
}

#[test]
fn insert_in_the_middle() {
    let mut l = list();
    ins_run(&mut l, 0, "ac", 1, 1);
    ins(&mut l, 1, b'b', 3, 1);
    assert_eq!(text(&l), "abc");
}

#[test]
fn get_reads_the_live_item() {
    let mut l = list();
    ins_run(&mut l, 0, "xy", 1, 1);
    match l.get(1) {
        Some(Element::Scalar(Scalar::Bytes(b))) => assert_eq!(b, vec![b'y']),
        _ => panic!("expected byte scalar"),
    }
    assert!(l.get(2).is_none());
}

// --- local delete (tombstone) ---

#[test]
fn delete_removes_from_the_live_view() {
    let mut l = list();
    ins_run(&mut l, 0, "abc", 1, 1);
    l.delete(1);
    assert_eq!(text(&l), "ac");
    assert_eq!(l.len(), 2);
}

#[test]
fn delete_then_insert_positions_correctly() {
    // A tombstone still anchors neighbouring inserts.
    let mut l = list();
    ins_run(&mut l, 0, "ab", 1, 1);
    l.delete(1); // tombstone 'b' -> "a"
    ins(&mut l, 1, b'c', 3, 1);
    assert_eq!(text(&l), "ac");
}

// --- merge laws ---

#[test]
fn merge_is_idempotent() {
    let mut l = list();
    ins_run(&mut l, 0, "abc", 1, 1);
    let before = text(&l);
    let twin = l.deep_clone();
    l.merge(&twin);
    assert_eq!(text(&l), before);
}

#[test]
fn merge_absorbs_disjoint_inserts() {
    let mut base = list();
    ins_run(&mut base, 0, "hello", 1, 1);

    let mut a = base.deep_clone();
    let mut b = base.deep_clone();
    ins(&mut a, 5, b'!', 10, 1); // "hello!"
    ins(&mut b, 0, b'>', 10, 2); // ">hello"

    a.merge(&b);
    b.merge(&a);
    assert_eq!(text(&a), text(&b));
    assert_eq!(text(&a), ">hello!");
}

#[test]
fn merge_is_commutative() {
    let mut base = list();
    ins_run(&mut base, 0, "mid", 1, 1);

    let mut a = base.deep_clone();
    let mut b = base.deep_clone();
    ins(&mut a, 0, b'L', 10, 1);
    ins(&mut b, 3, b'R', 10, 2);

    let mut ab = a.deep_clone();
    ab.merge(&b);
    let mut ba = b.deep_clone();
    ba.merge(&a);
    assert_eq!(text(&ab), text(&ba));
}

#[test]
fn merge_is_associative() {
    let mut base = list();
    ins_run(&mut base, 0, "0", 1, 1);
    let mut a = base.deep_clone();
    let mut b = base.deep_clone();
    let mut c = base.deep_clone();
    ins(&mut a, 1, b'a', 10, 1);
    ins(&mut b, 1, b'b', 10, 2);
    ins(&mut c, 1, b'c', 10, 3);

    // (a ∪ b) ∪ c
    let mut left = a.deep_clone();
    left.merge(&b);
    left.merge(&c);
    // a ∪ (b ∪ c)
    let mut bc = b.deep_clone();
    bc.merge(&c);
    let mut right = a.deep_clone();
    right.merge(&bc);

    assert_eq!(text(&left), text(&right));
}

#[test]
fn merge_carries_tombstones() {
    let mut base = list();
    ins_run(&mut base, 0, "abc", 1, 1);
    let mut a = base.deep_clone();
    let b = base.deep_clone();
    a.delete(1); // "ac"
    a.merge(&b); // b never deleted 'b', but a's tombstone must hold
    assert_eq!(text(&a), "ac");
}

// --- Fugue: no interleaving of concurrent runs at the same gap ---

#[test]
fn concurrent_runs_at_the_same_gap_do_not_interleave() {
    let mut a = list();
    let mut b = list();
    ins_run(&mut a, 0, "ABC", 1, 1);
    ins_run(&mut b, 0, "XYZ", 1, 2);

    a.merge(&b);
    b.merge(&a);

    assert_eq!(text(&a), text(&b), "replicas diverged");
    let t = text(&a);
    assert!(t == "ABCXYZ" || t == "XYZABC", "runs interleaved: {t}");
}

#[test]
fn concurrent_single_inserts_converge_deterministically() {
    // Same origin, one char each, different clients: both replicas agree.
    let mut a = list();
    let mut b = list();
    ins(&mut a, 0, b'a', 1, 1);
    ins(&mut b, 0, b'b', 1, 2);
    a.merge(&b);
    b.merge(&a);
    assert_eq!(text(&a), text(&b));
    assert_eq!(text(&a).len(), 2);
}

#[test]
fn interleaved_typing_converges() {
    // Two clients build runs at the same spot, delivered op-by-op interleaved.
    let mut a = list();
    let mut b = list();
    for i in 0..3u64 {
        ins(&mut a, i as usize, b'a' + i as u8, i + 1, 1);
        ins(&mut b, i as usize, b'x' + i as u8, i + 1, 2);
    }
    a.merge(&b);
    b.merge(&a);
    assert_eq!(text(&a), text(&b));
    // each client's run stays contiguous
    let t = text(&a);
    assert!(t.contains("abc"), "client-1 run broken: {t}");
    assert!(t.contains("xyz"), "client-2 run broken: {t}");
}

// --- lifecycle ---

#[test]
fn deep_clone_is_independent() {
    let mut l = list();
    ins_run(&mut l, 0, "ab", 1, 1);
    let mut c = l.deep_clone();
    ins(&mut c, 2, b'c', 5, 1);
    assert_eq!(text(&l), "ab");
    assert_eq!(text(&c), "abc");
}

#[test]
fn displace_flags_the_handle() {
    let l = list();
    assert!(!l.is_displaced());
    l.displace();
    assert!(l.is_displaced());
}

// --- op-oriented placement (an insert op carries its Fugue placement, so it
//     applies identically on every replica regardless of local index) ---

/// Build a run on `l`, capturing each insert as an op (id + value + anchor).
fn capture_run(l: &mut List, s: &str, client: u8) -> Vec<InsertOp> {
    let mut ops = Vec::new();
    for (k, c) in s.bytes().enumerate() {
        let anchor = l.place(k);
        let id = stmp(k as u64 + 1, client);
        let value = ch(c);
        l.insert_at(id, value.clone(), anchor);
        ops.push((id, value, anchor));
    }
    ops
}

fn apply_ops(l: &mut List, ops: &[InsertOp]) {
    for (id, value, anchor) in ops {
        l.insert_at(*id, value.clone(), *anchor);
    }
}

#[test]
fn place_then_insert_at_matches_index_insert() {
    let mut l = list();
    capture_run(&mut l, "abc", 1);
    assert_eq!(text(&l), "abc");
}

#[test]
fn captured_ops_replay_on_a_fresh_replica() {
    let mut a = list();
    let ops = capture_run(&mut a, "abc", 1);
    let mut b = list();
    apply_ops(&mut b, &ops);
    assert_eq!(text(&b), "abc");
}

#[test]
fn concurrent_op_runs_converge_without_interleaving() {
    let mut a = list();
    let mut b = list();
    let oa = capture_run(&mut a, "ABC", 1);
    let ob = capture_run(&mut b, "XYZ", 2);
    apply_ops(&mut a, &ob);
    apply_ops(&mut b, &oa);
    assert_eq!(text(&a), text(&b));
    let s = text(&a);
    assert!(s == "ABCXYZ" || s == "XYZABC", "interleaved: {s}");
}

#[test]
fn op_apply_order_does_not_matter() {
    let mut a = list();
    let oa = capture_run(&mut a, "hi", 1);
    let mut b = list();
    // apply in reverse order
    for op in oa.iter().rev() {
        apply_ops(&mut b, std::slice::from_ref(op));
    }
    assert_eq!(text(&b), "hi");
}

#[test]
fn insert_at_is_idempotent_on_node_id() {
    let mut l = list();
    let anchor = l.place(0);
    l.insert_at(stmp(1, 1), ch(b'x'), anchor);
    l.insert_at(stmp(1, 1), ch(b'y'), anchor); // same id, replayed
    assert_eq!(text(&l), "x");
}

#[test]
fn node_at_reads_the_live_node_id() {
    let mut l = list();
    ins_run(&mut l, 0, "abc", 1, 1);
    let b_id = l.node_at(1).unwrap();
    l.delete_id(b_id);
    assert_eq!(text(&l), "ac");
    assert!(l.node_at(5).is_none());
}

#[test]
fn delete_id_is_idempotent() {
    let mut l = list();
    ins_run(&mut l, 0, "ab", 1, 1);
    let id = l.node_at(0).unwrap();
    l.delete_id(id);
    l.delete_id(id); // repeat must not disturb the rest
    assert_eq!(text(&l), "b");
}

// --- idempotence / composite items ---

#[test]
fn replayed_insert_does_not_resurrect_a_tombstone() {
    // A retried/duplicated insert carries the same stamp; re-applying it must
    // not overwrite (and un-delete) the node.
    let mut l = list();
    ins(&mut l, 0, b'a', 1, 1);
    l.delete(0);
    ins(&mut l, 0, b'a', 1, 1); // same stamp, replayed
    assert_eq!(l.len(), 0);
    assert_eq!(text(&l), "");
}

#[test]
fn merge_folds_composite_item_values() {
    // An item that is itself a CRDT must merge, not just survive by tombstone —
    // concurrent edits to the same list item have to converge.
    let counter = || Element::Counter(Rc::new(RefCell::new(Counter::new(eid(9, 9)))));
    let mut a = list();
    a.insert(0, counter(), stmp(1, 1));
    let mut b = a.deep_clone(); // same node stamp, independent counter handle

    if let Some(Element::Counter(c)) = a.get(0) {
        c.borrow_mut().inc(cid(1), 3);
    }
    if let Some(Element::Counter(c)) = b.get(0) {
        c.borrow_mut().inc(cid(2), 4);
    }

    a.merge(&b);
    b.merge(&a);

    let read = |l: &List| match l.get(0) {
        Some(Element::Counter(c)) => c.borrow().read(),
        _ => panic!("expected a counter item"),
    };
    assert_eq!(read(&a), 7);
    assert_eq!(read(&b), 7);
}
