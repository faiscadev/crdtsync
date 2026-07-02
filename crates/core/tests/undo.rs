//! Per-user undo / redo over root scalar slots.
//!
//! An `UndoManager` wraps a replica and records each edit made through it, so a
//! later undo replays a forward op that restores the prior value and a redo
//! replays the edit again. It tracks only edits made through the manager — a
//! user's own intentions — and emits ordinary ops, so an undo converges on peers
//! exactly like any other edit.

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Element, Op, Scalar, UndoManager};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn apply_all(d: &mut Document, ops: &[Op]) {
    for op in ops {
        d.apply(op);
    }
}

fn reg(d: &Document, key: &[u8]) -> Option<Scalar> {
    match d.get(key) {
        Some(Element::Register(r)) => Some(r.borrow().read().clone()),
        _ => None,
    }
}

fn counter(d: &Document, key: &[u8]) -> i64 {
    match d.get(key) {
        Some(Element::Counter(c)) => c.borrow().read(),
        _ => 0,
    }
}

#[test]
fn undo_restores_a_registers_prior_value() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    u.register(&mut d, b"title", Scalar::Int(1));
    u.register(&mut d, b"title", Scalar::Int(2));
    assert_eq!(reg(&d, b"title"), Some(Scalar::Int(2)));

    u.undo(&mut d);
    assert_eq!(
        reg(&d, b"title"),
        Some(Scalar::Int(1)),
        "undo goes back one step"
    );
    u.undo(&mut d);
    assert_eq!(
        reg(&d, b"title"),
        None,
        "undoing the first set deletes the slot"
    );
}

#[test]
fn redo_replays_an_undone_edit() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    u.register(&mut d, b"n", Scalar::Int(1));
    u.register(&mut d, b"n", Scalar::Int(2));

    u.undo(&mut d);
    assert_eq!(reg(&d, b"n"), Some(Scalar::Int(1)));
    u.redo(&mut d);
    assert_eq!(
        reg(&d, b"n"),
        Some(Scalar::Int(2)),
        "redo restores the undone value"
    );
}

#[test]
fn a_fresh_edit_clears_the_redo_stack() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    u.register(&mut d, b"n", Scalar::Int(1));
    u.register(&mut d, b"n", Scalar::Int(2));
    u.undo(&mut d);
    assert!(u.can_redo());

    // A new edit invalidates the redone future.
    u.register(&mut d, b"n", Scalar::Int(9));
    assert!(!u.can_redo());
    assert_eq!(u.redo(&mut d), None);
    assert_eq!(reg(&d, b"n"), Some(Scalar::Int(9)));
}

#[test]
fn undo_of_a_counter_increment_decrements() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    u.inc(&mut d, b"votes", 5);
    u.inc(&mut d, b"votes", 3);
    assert_eq!(counter(&d, b"votes"), 8);

    u.undo(&mut d);
    assert_eq!(counter(&d, b"votes"), 5, "undo cancels the last increment");
    u.redo(&mut d);
    assert_eq!(counter(&d, b"votes"), 8, "redo re-applies it");
}

#[test]
fn undo_of_a_counter_decrement_increments() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    u.inc(&mut d, b"stock", 10);
    u.dec(&mut d, b"stock", 4);
    assert_eq!(counter(&d, b"stock"), 6);
    u.undo(&mut d);
    assert_eq!(
        counter(&d, b"stock"),
        10,
        "undoing a decrement adds it back"
    );
}

#[test]
fn undo_of_a_delete_restores_the_value() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    u.register(&mut d, b"k", Scalar::Bytes(b"hello".to_vec()));
    u.delete(&mut d, b"k");
    assert_eq!(reg(&d, b"k"), None);

    u.undo(&mut d);
    assert_eq!(
        reg(&d, b"k"),
        Some(Scalar::Bytes(b"hello".to_vec())),
        "undo of a delete brings the value back"
    );
}

#[test]
fn empty_stacks_return_none() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    assert!(!u.can_undo() && !u.can_redo());
    assert_eq!(u.undo(&mut d), None);
    assert_eq!(u.redo(&mut d), None);
}

#[test]
fn an_undo_is_an_ordinary_op_a_peer_converges_on() {
    let mut a = doc(1);
    let mut b = doc(2);
    let mut u = UndoManager::new();

    apply_all(&mut b, &u.register(&mut a, b"title", Scalar::Int(1)));
    apply_all(&mut b, &u.register(&mut a, b"title", Scalar::Int(2)));
    assert_eq!(reg(&b, b"title"), Some(Scalar::Int(2)));

    // The undo's ops travel to the peer like any edit, and it converges.
    let undo_ops = u.undo(&mut a).expect("something to undo");
    apply_all(&mut b, &undo_ops);
    assert_eq!(
        reg(&b, b"title"),
        Some(Scalar::Int(1)),
        "the peer sees the undo"
    );
}
