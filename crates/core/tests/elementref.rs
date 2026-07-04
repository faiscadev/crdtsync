//! Element ref — the reserved payload value type for an intra-document link.
//!
//! An element ref is a leaf value, not a CRDT primitive: it names another
//! element in the same room (a mention, a link, a foreign key) by a bare
//! [`ElementId`]. It merges like any other value (LWW on assignment), so it
//! lives in [`Scalar`] alongside the other leaf values — no substructure, no
//! merge, and a dangling target is an app concern. This reserves the wire slot
//! so the op envelope carries element refs; no producer/consumer yet.

use crdtsync_core::op::{Op, OpId, OpKind};
use crdtsync_core::{decode_op, encode_op, Anchor, DecodeError, ElementId, Scalar, Side};

mod common;
use common::{cid, eid, stmp};

fn target() -> ElementId {
    eid(0x1122, 0x3344)
}

fn op(kind: OpKind) -> Op {
    Op::new(
        OpId {
            client: cid(1),
            seq: 7,
        },
        stmp(42, 1),
        eid(0xAB, 0xCD),
        kind,
    )
}

// --- value semantics: a ref is a value, not an entity ---

#[test]
fn holds_its_target_verbatim() {
    let r = Scalar::ElementRef(target());
    assert_eq!(r, Scalar::ElementRef(eid(0x1122, 0x3344)));
}

#[test]
fn differing_target_is_not_equal() {
    assert_ne!(
        Scalar::ElementRef(target()),
        Scalar::ElementRef(eid(0x1122, 0x3345))
    );
}

// A ref names an element; raw bytes that happen to match the id are a different
// value — the ref has link semantics, bytes do not.
#[test]
fn a_ref_never_equals_a_bytes_scalar() {
    assert_ne!(
        Scalar::ElementRef(target()),
        Scalar::Bytes(target().as_bytes().to_vec())
    );
}

#[test]
fn a_ref_never_equals_a_null() {
    assert_ne!(Scalar::ElementRef(target()), Scalar::Null);
}

// --- wire slot: refs round-trip through the op codec ---

#[test]
fn a_ref_round_trips_as_a_register_value() {
    let o = op(OpKind::RegisterSet {
        key: b"author".to_vec(),
        value: Scalar::ElementRef(target()),
    });
    assert_eq!(decode_op(&encode_op(&o)).expect("decodes"), o);
}

#[test]
fn a_ref_round_trips_as_a_map_value() {
    let o = op(OpKind::MapSet {
        key: b"parent".to_vec(),
        value: Scalar::ElementRef(target()),
    });
    assert_eq!(decode_op(&encode_op(&o)).expect("decodes"), o);
}

#[test]
fn a_ref_round_trips_as_a_list_item() {
    let o = op(OpKind::ListInsert {
        value: Scalar::ElementRef(target()),
        anchor: Anchor {
            parent: None,
            side: Side::Right,
        },
    });
    assert_eq!(decode_op(&encode_op(&o)).expect("decodes"), o);
}

#[test]
fn a_ref_round_trips_as_a_standalone_value() {
    let v = Scalar::ElementRef(target());
    assert_eq!(Scalar::decode_state(&v.encode_state()).unwrap(), v);
}

#[test]
fn ref_encoding_is_deterministic() {
    let v = Scalar::ElementRef(target());
    assert_eq!(v.encode_state(), v.encode_state());
}

// --- decoding stays total over the new slot ---

#[test]
fn a_truncated_ref_is_an_error_not_a_panic() {
    let bytes = encode_op(&op(OpKind::RegisterSet {
        key: b"a".to_vec(),
        value: Scalar::ElementRef(target()),
    }));
    for cut in 0..bytes.len() {
        assert_eq!(decode_op(&bytes[..cut]), Err(DecodeError::UnexpectedEof));
    }
}
