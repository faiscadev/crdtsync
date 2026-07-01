//! Op envelope — the immutable, append-only unit the document emits.
//!
//! The envelope carries just what the pure CRDT core needs: identity
//! (`op_id = (client, seq)`), causal position (`stamp = lamport + client`),
//! the `target` element, the `kind` (a closed enum of primitive mutations),
//! and a reserved atomic-transaction slot. Authorship, scope, schema version,
//! and wall time are wire/server concerns and live outside the core.

use crdtsync_core::op::{Op, OpId, OpKind, TxId};
use crdtsync_core::Scalar;

mod common;
use common::{cid, eid, stmp};

/// OpId from a client's leading byte and a sequence number.
fn oid(client_first: u8, seq: u64) -> OpId {
    OpId {
        client: cid(client_first),
        seq,
    }
}

/// A register-set op targeting the default map, from client 1.
fn set_op(seq: u64, lamport: u64, value: Scalar) -> Op {
    Op::new(
        oid(1, seq),
        stmp(lamport, 1),
        eid(0xEE, 0),
        OpKind::RegisterSet {
            key: b"k".to_vec(),
            value,
        },
    )
}

// --- identity: op_id = (client, seq) ---

#[test]
fn opid_carries_client_and_seq() {
    let id = oid(3, 7);
    assert_eq!(id.client, cid(3));
    assert_eq!(id.seq, 7);
}

#[test]
fn opid_equal_when_client_and_seq_match() {
    assert_eq!(oid(2, 5), oid(2, 5));
}

#[test]
fn opid_differs_on_seq() {
    assert_ne!(oid(2, 5), oid(2, 6));
}

#[test]
fn opid_differs_on_client() {
    assert_ne!(oid(2, 5), oid(9, 5));
}

// --- idempotence: op_id is the dedup key (reconnects, retries, replays) ---

#[test]
fn same_opid_is_a_duplicate() {
    // Two envelopes minted for the same (client, seq) denote the same op; a
    // replica that has seen the id must ignore the second.
    let a = set_op(4, 10, Scalar::Int(1));
    let b = set_op(4, 10, Scalar::Int(1));
    assert_eq!(a.id, b.id);
}

#[test]
fn distinct_seq_is_not_a_duplicate() {
    let a = set_op(4, 10, Scalar::Int(1));
    let b = set_op(5, 11, Scalar::Int(1));
    assert_ne!(a.id, b.id);
}

// --- construction / read-back ---

#[test]
fn new_populates_every_field() {
    let kind = OpKind::CounterInc {
        key: b"n".to_vec(),
        amount: 5,
    };
    let op = Op::new(oid(1, 2), stmp(3, 1), eid(7, 42), kind.clone());
    assert_eq!(op.id, oid(1, 2));
    assert_eq!(op.stamp, stmp(3, 1));
    assert_eq!(op.target, eid(7, 42));
    assert_eq!(op.kind, kind);
}

#[test]
fn new_defaults_to_non_atomic() {
    // No transaction context unless one is explicitly attached.
    let op = set_op(1, 1, Scalar::Null);
    assert_eq!(op.tx, None);
}

#[test]
fn op_is_cloneable() {
    // Ops are immutable and append-only; cloning for the log/replay is cheap
    // and structural.
    let op = set_op(1, 1, Scalar::Int(9));
    assert_eq!(op.clone(), op);
}

// --- causal / total order: delegates to the stamp (lamport, then client) ---

#[test]
fn higher_lamport_orders_later() {
    let a = set_op(1, 1, Scalar::Null);
    let b = set_op(2, 2, Scalar::Null);
    assert!(a.stamp < b.stamp);
}

#[test]
fn equal_lamport_breaks_by_client() {
    let a = Op::new(
        oid(1, 1),
        stmp(5, 1),
        eid(1, 0),
        OpKind::MapDelete { key: b"k".to_vec() },
    );
    let b = Op::new(
        oid(2, 1),
        stmp(5, 2),
        eid(1, 0),
        OpKind::MapDelete { key: b"k".to_vec() },
    );
    assert!(a.stamp < b.stamp);
}

// --- closed OpKind enum: one variant per green-primitive mutation ---

#[test]
fn register_set_holds_a_scalar() {
    let k = OpKind::RegisterSet {
        key: b"greeting".to_vec(),
        value: Scalar::Bytes(b"hi".to_vec()),
    };
    match k {
        OpKind::RegisterSet { key, value } => {
            assert_eq!(key, b"greeting".to_vec());
            assert_eq!(value, Scalar::Bytes(b"hi".to_vec()));
        }
        _ => panic!("expected RegisterSet"),
    }
}

#[test]
fn counter_inc_and_dec_are_distinct() {
    // The direction is encoded in the kind, matching Counter::inc / ::dec; the
    // amount is unsigned, the acting client comes from op_id.
    assert_ne!(
        OpKind::CounterInc {
            key: b"n".to_vec(),
            amount: 3,
        },
        OpKind::CounterDec {
            key: b"n".to_vec(),
            amount: 3,
        },
    );
}

#[test]
fn map_set_holds_key_and_scalar_value() {
    let k = OpKind::MapSet {
        key: b"name".to_vec(),
        value: Scalar::Int(42),
    };
    match k {
        OpKind::MapSet { key, value } => {
            assert_eq!(key, b"name".to_vec());
            assert_eq!(value, Scalar::Int(42));
        }
        _ => panic!("expected MapSet"),
    }
}

#[test]
fn map_delete_holds_a_binary_key() {
    // Keys are raw bytes (binary-safe), matching Map's &[u8] API.
    let k = OpKind::MapDelete {
        key: vec![0, 1, 2, 0xFF],
    };
    match k {
        OpKind::MapDelete { key } => assert_eq!(key, vec![0, 1, 2, 0xFF]),
        _ => panic!("expected MapDelete"),
    }
}

// --- reserved: atomic-transaction membership slot ---

#[test]
fn tx_slot_can_carry_membership() {
    let mut op = set_op(1, 1, Scalar::Null);
    op.tx = Some(TxId(77));
    assert_eq!(op.tx, Some(TxId(77)));
}

#[test]
fn ops_share_a_tx_id_when_in_the_same_transaction() {
    let tx = TxId(1);
    let mut a = set_op(1, 1, Scalar::Int(1));
    let mut b = set_op(2, 2, Scalar::Int(2));
    a.tx = Some(tx);
    b.tx = Some(tx);
    assert_eq!(a.tx, b.tx);
}
