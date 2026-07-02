//! Per-user undo / redo over scalar slots, at the root and inside nested maps.
//!
//! An `UndoManager` wraps a replica and records each edit made through it, so a
//! later undo replays a forward op that restores the prior value and a redo
//! replays the edit again. It tracks only edits made through the manager — a
//! user's own intentions — and emits ordinary ops, so an undo converges on peers
//! exactly like any other edit. Edits are addressed by path, so a slot in a
//! nested map undoes exactly as a root one does.

use crdtsync_core::doc::Document;
use crdtsync_core::{path, ClientId, Op, Scalar, UndoManager};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn p(keys: &[&[u8]]) -> Vec<u8> {
    path::encode_path(keys)
}

fn apply_all(d: &mut Document, ops: &[Op]) {
    for op in ops {
        d.apply(op);
    }
}

fn reg(d: &Document, path: &[u8]) -> Option<Scalar> {
    path::get_register(d, path)
}

fn counter(d: &Document, path: &[u8]) -> i64 {
    path::get_counter(d, path).unwrap_or(0)
}

#[test]
fn undo_restores_a_registers_prior_value() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    u.register(&mut d, &p(&[b"title"]), Scalar::Int(1));
    u.register(&mut d, &p(&[b"title"]), Scalar::Int(2));
    assert_eq!(reg(&d, &p(&[b"title"])), Some(Scalar::Int(2)));

    u.undo(&mut d);
    assert_eq!(
        reg(&d, &p(&[b"title"])),
        Some(Scalar::Int(1)),
        "undo goes back one step"
    );
    u.undo(&mut d);
    assert_eq!(
        reg(&d, &p(&[b"title"])),
        None,
        "undoing the first set deletes the slot"
    );
}

#[test]
fn redo_replays_an_undone_edit() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    u.register(&mut d, &p(&[b"n"]), Scalar::Int(1));
    u.register(&mut d, &p(&[b"n"]), Scalar::Int(2));

    u.undo(&mut d);
    assert_eq!(reg(&d, &p(&[b"n"])), Some(Scalar::Int(1)));
    u.redo(&mut d);
    assert_eq!(
        reg(&d, &p(&[b"n"])),
        Some(Scalar::Int(2)),
        "redo restores the undone value"
    );
}

#[test]
fn a_fresh_edit_clears_the_redo_stack() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    u.register(&mut d, &p(&[b"n"]), Scalar::Int(1));
    u.register(&mut d, &p(&[b"n"]), Scalar::Int(2));
    u.undo(&mut d);
    assert!(u.can_redo());

    // A new edit invalidates the redone future.
    u.register(&mut d, &p(&[b"n"]), Scalar::Int(9));
    assert!(!u.can_redo());
    assert_eq!(u.redo(&mut d), None);
    assert_eq!(reg(&d, &p(&[b"n"])), Some(Scalar::Int(9)));
}

#[test]
fn undo_of_a_counter_increment_decrements() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    u.inc(&mut d, &p(&[b"votes"]), 5);
    u.inc(&mut d, &p(&[b"votes"]), 3);
    assert_eq!(counter(&d, &p(&[b"votes"])), 8);

    u.undo(&mut d);
    assert_eq!(
        counter(&d, &p(&[b"votes"])),
        5,
        "undo cancels the last increment"
    );
    u.redo(&mut d);
    assert_eq!(counter(&d, &p(&[b"votes"])), 8, "redo re-applies it");
}

#[test]
fn undo_of_a_counter_decrement_increments() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    u.inc(&mut d, &p(&[b"stock"]), 10);
    u.dec(&mut d, &p(&[b"stock"]), 4);
    assert_eq!(counter(&d, &p(&[b"stock"])), 6);
    u.undo(&mut d);
    assert_eq!(
        counter(&d, &p(&[b"stock"])),
        10,
        "undoing a decrement adds it back"
    );
}

#[test]
fn undo_of_a_delete_restores_the_value() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    u.register(&mut d, &p(&[b"k"]), Scalar::Bytes(b"hello".to_vec()));
    u.delete(&mut d, &p(&[b"k"]));
    assert_eq!(reg(&d, &p(&[b"k"])), None);

    u.undo(&mut d);
    assert_eq!(
        reg(&d, &p(&[b"k"])),
        Some(Scalar::Bytes(b"hello".to_vec())),
        "undo of a delete brings the value back"
    );
}

#[test]
fn undo_restores_a_nested_slots_prior_value() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    let name = p(&[b"profile", b"name"]);
    u.register(&mut d, &name, Scalar::Bytes(b"ann".to_vec()));
    u.register(&mut d, &name, Scalar::Bytes(b"bob".to_vec()));
    assert_eq!(reg(&d, &name), Some(Scalar::Bytes(b"bob".to_vec())));

    u.undo(&mut d);
    assert_eq!(
        reg(&d, &name),
        Some(Scalar::Bytes(b"ann".to_vec())),
        "undo reverts the nested slot"
    );
    u.undo(&mut d);
    assert_eq!(
        reg(&d, &name),
        None,
        "undoing the first nested set clears it"
    );
    u.redo(&mut d);
    assert_eq!(
        reg(&d, &name),
        Some(Scalar::Bytes(b"ann".to_vec())),
        "redo restores the nested slot"
    );
}

#[test]
fn undo_of_a_nested_counter_decrements() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    let hits = p(&[b"stats", b"hits"]);
    u.inc(&mut d, &hits, 7);
    assert_eq!(counter(&d, &hits), 7);

    u.undo(&mut d);
    assert_eq!(counter(&d, &hits), 0, "undo cancels the nested increment");
}

#[test]
fn a_group_undoes_root_and_nested_slots_together() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    let title = p(&[b"title"]);
    let author = p(&[b"meta", b"author"]);
    // A gesture that touches a root slot and a nested one as one intention.
    u.group(&mut d, |b| {
        b.register(&title, Scalar::Int(1));
        b.register(&author, Scalar::Bytes(b"ann".to_vec()));
    });
    assert_eq!(reg(&d, &title), Some(Scalar::Int(1)));
    assert_eq!(reg(&d, &author), Some(Scalar::Bytes(b"ann".to_vec())));

    u.undo(&mut d);
    assert_eq!(reg(&d, &title), None);
    assert_eq!(reg(&d, &author), None);
    assert!(!u.can_undo(), "the group was a single step");

    u.redo(&mut d);
    assert_eq!(reg(&d, &title), Some(Scalar::Int(1)));
    assert_eq!(reg(&d, &author), Some(Scalar::Bytes(b"ann".to_vec())));
}

#[test]
fn a_group_undoes_as_one_step() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    // Two fields set as a single gesture.
    u.group(&mut d, |b| {
        b.register(&p(&[b"first"]), Scalar::Int(1));
        b.register(&p(&[b"last"]), Scalar::Int(2));
    });
    assert_eq!(reg(&d, &p(&[b"first"])), Some(Scalar::Int(1)));
    assert_eq!(reg(&d, &p(&[b"last"])), Some(Scalar::Int(2)));

    // One undo reverts the whole group.
    u.undo(&mut d);
    assert_eq!(reg(&d, &p(&[b"first"])), None);
    assert_eq!(reg(&d, &p(&[b"last"])), None);
    assert!(!u.can_undo(), "the group was a single step");

    // One redo restores the whole group.
    u.redo(&mut d);
    assert_eq!(reg(&d, &p(&[b"first"])), Some(Scalar::Int(1)));
    assert_eq!(reg(&d, &p(&[b"last"])), Some(Scalar::Int(2)));
}

#[test]
fn a_group_mixes_register_and_counter() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    u.register(&mut d, &p(&[b"title"]), Scalar::Int(1));
    // A gesture that changes a field and bumps a counter together.
    u.group(&mut d, |b| {
        b.register(&p(&[b"title"]), Scalar::Int(2));
        b.inc(&p(&[b"votes"]), 3);
    });
    assert_eq!(reg(&d, &p(&[b"title"])), Some(Scalar::Int(2)));
    assert_eq!(counter(&d, &p(&[b"votes"])), 3);

    u.undo(&mut d);
    assert_eq!(
        reg(&d, &p(&[b"title"])),
        Some(Scalar::Int(1)),
        "field restored"
    );
    assert_eq!(counter(&d, &p(&[b"votes"])), 0, "counter bump undone");
}

#[test]
fn an_empty_group_records_no_step() {
    let mut d = doc(1);
    let mut u = UndoManager::new();
    let ops = u.group(&mut d, |_| {});
    assert!(ops.is_empty());
    assert!(!u.can_undo(), "an empty group is not an undo step");
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
    let name = p(&[b"profile", b"name"]);

    apply_all(&mut b, &u.register(&mut a, &name, Scalar::Int(1)));
    apply_all(&mut b, &u.register(&mut a, &name, Scalar::Int(2)));
    assert_eq!(reg(&b, &name), Some(Scalar::Int(2)));

    // The undo's ops travel to the peer like any edit, and it converges — even
    // for a nested slot.
    let undo_ops = u.undo(&mut a).expect("something to undo");
    apply_all(&mut b, &undo_ops);
    assert_eq!(
        reg(&b, &name),
        Some(Scalar::Int(1)),
        "the peer sees the nested undo"
    );
}
