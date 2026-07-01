//! Document — the top-level replica: a root Map, a lamport clock, and the
//! transact/apply seam that turns editing intentions into ops and ops back
//! into state.
//!
//! Scope here is the root-level document. A `transact` mutates the live tree
//! and returns the ops it emitted; `apply` folds a foreign op back in. Two
//! documents that exchange ops converge regardless of arrival order. Ops are
//! keyed by `(client, seq)` for idempotent dedup, ordered by their stamp for
//! LWW, and route to map children by key (the receiver re-derives the child
//! id), so no separate "create element" op is needed.

use crdtsync_core::doc::Document;
use crdtsync_core::op::OpKind;
use crdtsync_core::{Element, Scalar};

mod common;
use common::cid;

fn doc(client_first: u8) -> Document {
    Document::new(cid(client_first))
}

fn int(e: Option<Element>) -> i64 {
    match e {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected Int, got {other:?}"),
        },
        _ => panic!("expected a Register"),
    }
}

fn counter(e: Option<Element>) -> i64 {
    match e {
        Some(Element::Counter(c)) => c.borrow().read(),
        _ => panic!("expected a Counter"),
    }
}

/// Replay every op from `src` into `dst`, in the given order.
fn replay(dst: &mut Document, ops: &[crdtsync_core::op::Op]) {
    for op in ops {
        dst.apply(op);
    }
}

// --- construction ---

#[test]
fn new_carries_its_client() {
    let d = doc(1);
    assert_eq!(d.client(), cid(1));
}

#[test]
fn all_replicas_share_the_root_id() {
    // The root is a well-known slot so every replica derives children under
    // the same parent.
    assert_eq!(doc(1).root_id(), doc(2).root_id());
}

#[test]
fn fresh_document_is_empty() {
    let d = doc(1);
    assert!(d.get(b"missing").is_none());
}

// --- transact: mutate + emit ---

#[test]
fn transact_applies_locally() {
    let mut d = doc(1);
    d.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert_eq!(int(d.get(b"age")), 30);
}

#[test]
fn transact_returns_the_emitted_ops() {
    let mut d = doc(1);
    let ops = d.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert_eq!(ops.len(), 1);
    assert_eq!(
        ops[0].kind,
        OpKind::RegisterSet {
            key: b"age".to_vec(),
            value: Scalar::Int(30),
        }
    );
}

#[test]
fn emitted_ops_carry_the_documents_client() {
    let mut d = doc(7);
    let ops = d.transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.inc(b"n", 2);
    });
    assert!(ops.iter().all(|op| op.id.client == cid(7)));
}

#[test]
fn client_seq_increases_per_op() {
    let mut d = doc(1);
    let ops = d.transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.register(b"b", Scalar::Int(2));
        tx.register(b"c", Scalar::Int(3));
    });
    let seqs: Vec<u64> = ops.iter().map(|op| op.id.seq).collect();
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "seqs not increasing: {seqs:?}"
    );
}

#[test]
fn lamport_increases_per_op() {
    let mut d = doc(1);
    let ops = d.transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.register(b"b", Scalar::Int(2));
    });
    assert!(ops[0].stamp < ops[1].stamp);
}

#[test]
fn seq_continues_across_transacts() {
    let mut d = doc(1);
    let first = d.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let second = d.transact(|tx| tx.register(b"b", Scalar::Int(2)));
    assert!(second[0].id.seq > first[0].id.seq);
    assert!(second[0].stamp > first[0].stamp);
}

// --- op kinds per intention ---

#[test]
fn counter_intents_emit_directional_ops() {
    let mut d = doc(1);
    let ops = d.transact(|tx| {
        tx.inc(b"n", 5);
        tx.dec(b"n", 2);
    });
    assert_eq!(
        ops[0].kind,
        OpKind::CounterInc {
            key: b"n".to_vec(),
            amount: 5
        }
    );
    assert_eq!(
        ops[1].kind,
        OpKind::CounterDec {
            key: b"n".to_vec(),
            amount: 2
        }
    );
}

#[test]
fn delete_emits_a_delete_op() {
    let mut d = doc(1);
    d.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let ops = d.transact(|tx| tx.delete(b"a"));
    assert_eq!(ops[0].kind, OpKind::MapDelete { key: b"a".to_vec() });
    assert!(d.get(b"a").is_none());
}

// --- apply: idempotent, ordered, convergent ---

#[test]
fn apply_reconstructs_state_on_a_peer() {
    let mut a = doc(1);
    let ops = a.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let mut b = doc(2);
    replay(&mut b, &ops);
    assert_eq!(int(b.get(b"age")), 30);
}

#[test]
fn apply_is_idempotent_on_op_id() {
    // A replayed / duplicated op (reconnect, retry) must not double-count.
    let mut a = doc(1);
    let ops = a.transact(|tx| tx.inc(b"n", 5));
    let mut b = doc(2);
    b.apply(&ops[0]);
    b.apply(&ops[0]);
    assert_eq!(counter(b.get(b"n")), 5);
}

#[test]
fn concurrent_ops_converge_regardless_of_order() {
    // Two clients set the same key concurrently; both replicas must land on
    // the same value (higher stamp wins).
    let mut a = doc(1);
    let mut b = doc(2);
    let oa = a.transact(|tx| tx.register(b"x", Scalar::Int(10)));
    let ob = b.transact(|tx| tx.register(b"x", Scalar::Int(20)));

    replay(&mut a, &ob); // a: local then remote
    replay(&mut b, &oa); // b: remote then local

    assert_eq!(int(a.get(b"x")), int(b.get(b"x")));
}

#[test]
fn apply_advances_the_local_clock() {
    // After absorbing a high-lamport op, the next local op must sort after it.
    let mut a = doc(1);
    let far = a.transact(|tx| {
        for _ in 0..5 {
            tx.inc(b"n", 1);
        }
    });
    let mut b = doc(2);
    replay(&mut b, &far);
    let next = b.transact(|tx| tx.register(b"z", Scalar::Int(0)));
    assert!(next[0].stamp > far.last().unwrap().stamp);
}

// --- displacement: orphaning is never silent ---

#[test]
fn overwriting_a_composite_slot_orphans_the_displaced_element() {
    let mut d = doc(1);
    let created = d.transact(|tx| tx.register(b"slot", Scalar::Int(1)));
    let orphaned_id = created[0].target; // the register lived under this map...
    let _ = orphaned_id;

    d.transact(|tx| tx.set(b"slot", Scalar::Bool(true))); // scalar over composite
    let orphans = d.take_orphans();
    assert_eq!(
        orphans.len(),
        1,
        "displacement must surface exactly one orphan"
    );
}

#[test]
fn plain_scalar_overwrite_does_not_orphan() {
    let mut d = doc(1);
    d.transact(|tx| tx.set(b"k", Scalar::Int(1)));
    d.transact(|tx| tx.set(b"k", Scalar::Int(2)));
    assert!(d.take_orphans().is_empty());
}
