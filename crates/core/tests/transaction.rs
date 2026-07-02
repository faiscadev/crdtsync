//! Atomic transactions — opt-in all-or-nothing visibility.
//!
//! A plain `transact` streams its ops: each merges independently on arrival, so
//! a peer can observe a partial group. An `atomic_transact` instead tags its ops
//! as one transaction; a receiver holds the members until the whole group is
//! present, then applies them together, so no peer ever sees a partial
//! transaction. Atomicity is a *view* guarantee — the same ops still merge, so an
//! atomic author and a non-atomic peer converge on identical state.

use crdtsync_core::doc::Document;
use crdtsync_core::op::Tx;
use crdtsync_core::{ClientId, Element, Op, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn reg(d: &Document, key: &[u8]) -> Option<Scalar> {
    match d.get(key) {
        Some(Element::Register(r)) => Some(r.borrow().read().clone()),
        _ => None,
    }
}

#[test]
fn atomic_transact_tags_every_member_with_one_tx() {
    let mut d = doc(1);
    let ops = d.atomic_transact(|tx| {
        tx.register(b"first", Scalar::Int(1));
        tx.register(b"last", Scalar::Int(2));
    });
    assert_eq!(ops.len(), 2);
    let txs: Vec<Tx> = ops.iter().map(|o| o.tx.clone().expect("tagged")).collect();
    assert_eq!(txs[0].id, txs[1].id, "members share one tx id");
    assert!(
        txs.iter().all(|t| t.count == 2),
        "each member knows the group size"
    );
}

#[test]
fn two_atomic_transactions_get_distinct_ids() {
    let mut d = doc(1);
    let a = d.atomic_transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
    });
    let b = d.atomic_transact(|tx| {
        tx.register(b"b", Scalar::Int(1));
    });
    assert_ne!(a[0].tx.as_ref().unwrap().id, b[0].tx.as_ref().unwrap().id);
}

#[test]
fn a_partial_atomic_transaction_is_invisible_until_it_commits() {
    let mut a = doc(1);
    let mut b = doc(2);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
    });

    // The first member alone shows nothing: a partial tx is held.
    assert!(!b.apply(&ops[0]), "an incomplete tx member is buffered");
    assert_eq!(reg(&b, b"x"), None);
    assert_eq!(reg(&b, b"y"), None);

    // The last member commits the whole group at once.
    assert!(b.apply(&ops[1]));
    assert_eq!(reg(&b, b"x"), Some(Scalar::Int(1)));
    assert_eq!(reg(&b, b"y"), Some(Scalar::Int(2)));
}

#[test]
fn an_atomic_transaction_commits_regardless_of_delivery_order() {
    let mut a = doc(1);
    let mut b = doc(2);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
    });

    // Deliver the members in reverse; the group still stays hidden until whole.
    assert!(!b.apply(&ops[1]));
    assert_eq!(reg(&b, b"y"), None);
    assert!(b.apply(&ops[0]));
    assert_eq!(reg(&b, b"x"), Some(Scalar::Int(1)));
    assert_eq!(reg(&b, b"y"), Some(Scalar::Int(2)));
}

#[test]
fn an_atomic_author_and_a_nonatomic_peer_converge() {
    let mut a = doc(1);
    let mut b = doc(2);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
    });
    for op in &ops {
        b.apply(op);
    }
    assert_eq!(reg(&b, b"x"), reg(&a, b"x"));
    assert_eq!(reg(&b, b"y"), reg(&a, b"y"));
}

#[test]
fn an_atomic_transaction_spanning_a_nested_create_is_atomic() {
    let mut a = doc(1);
    let mut b = doc(2);
    // Create a nested map and set a slot inside it as one gesture.
    let ops = a.atomic_transact(|tx| {
        tx.map(b"profile").register(b"name", Scalar::Int(7));
    });
    assert!(ops.len() >= 2, "a create plus a set");

    // Deliver all but the last: nothing is visible, even the container.
    for op in &ops[..ops.len() - 1] {
        assert!(!b.apply(op));
    }
    assert!(b.get(b"profile").is_none());

    // The final member commits the whole tx.
    assert!(b.apply(&ops[ops.len() - 1]));
    let child = match b.get(b"profile") {
        Some(Element::Map(m)) => m,
        _ => panic!("nested map missing after commit"),
    };
    let slot = child.borrow().get(b"name");
    match slot {
        Some(Element::Register(r)) => assert_eq!(r.borrow().read().clone(), Scalar::Int(7)),
        _ => panic!("nested slot missing after commit"),
    }
}

#[test]
fn replaying_a_committed_member_is_a_no_op() {
    let mut a = doc(1);
    let mut b = doc(2);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
    });
    for op in &ops {
        b.apply(op);
    }
    // A resend of any member after commit changes nothing.
    assert!(!b.apply(&ops[0]));
    assert_eq!(reg(&b, b"x"), Some(Scalar::Int(1)));
}

#[test]
fn a_single_op_atomic_transaction_applies_immediately() {
    let mut a = doc(1);
    let mut b = doc(2);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"solo", Scalar::Int(9));
    });
    assert_eq!(ops.len(), 1);
    assert!(b.apply(&ops[0]), "a complete one-member tx applies at once");
    assert_eq!(reg(&b, b"solo"), Some(Scalar::Int(9)));
}

#[test]
fn a_buffered_partial_tx_survives_a_snapshot_round_trip() {
    let mut a = doc(1);
    let ops = a.atomic_transact(|tx| {
        tx.register(b"x", Scalar::Int(1));
        tx.register(b"y", Scalar::Int(2));
    });

    let mut b = doc(2);
    b.apply(&ops[0]); // partial: buffered, invisible

    // A snapshot taken mid-tx preserves the held member, and the decoded replica
    // still commits when the rest arrives.
    let snap = b.encode_state();
    let mut restored = Document::decode_state(&snap).expect("decode");
    assert_eq!(reg(&restored, b"x"), None);
    restored.apply(&ops[1]);
    assert_eq!(reg(&restored, b"x"), Some(Scalar::Int(1)));
    assert_eq!(reg(&restored, b"y"), Some(Scalar::Int(2)));
}

#[test]
fn begin_and_commit_group_separate_edits_into_one_tx() {
    let mut d = doc(1);
    d.begin_atomic();
    assert!(d.is_atomic());
    // Each edit accumulates and returns nothing of its own while recording.
    assert!(d.transact(|c| c.register(b"x", Scalar::Int(1))).is_empty());
    assert!(d.transact(|c| c.register(b"y", Scalar::Int(2))).is_empty());
    let ops = d.commit_atomic();
    assert!(!d.is_atomic());
    assert_eq!(ops.len(), 2);
    let id = ops[0].tx.clone().expect("tagged").id;
    assert!(ops.iter().all(|o| o.tx.as_ref().unwrap().id == id));
    assert!(ops.iter().all(|o| o.tx.as_ref().unwrap().count == 2));
    // The author sees its own edits immediately.
    assert_eq!(reg(&d, b"x"), Some(Scalar::Int(1)));
}

#[test]
fn committing_with_no_recorded_edits_yields_nothing() {
    let mut d = doc(1);
    d.begin_atomic();
    assert!(d.commit_atomic().is_empty());
}

#[test]
fn a_begin_commit_group_commits_atomically_on_a_peer() {
    let mut a = doc(1);
    let mut b = doc(2);
    a.begin_atomic();
    a.transact(|c| c.register(b"x", Scalar::Int(1)));
    a.transact(|c| c.register(b"y", Scalar::Int(2)));
    let ops = a.commit_atomic();

    assert!(!b.apply(&ops[0]));
    assert_eq!(reg(&b, b"x"), None);
    assert!(b.apply(&ops[1]));
    assert_eq!(reg(&b, b"x"), Some(Scalar::Int(1)));
    assert_eq!(reg(&b, b"y"), Some(Scalar::Int(2)));
}

#[test]
fn ops_from_a_plain_transact_carry_no_tx() {
    let mut d = doc(1);
    let ops = d.transact(|tx| {
        tx.register(b"k", Scalar::Int(1));
    });
    assert!(ops.iter().all(|o: &Op| o.tx.is_none()));
}
