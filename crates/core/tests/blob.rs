//! Blob ref — the reserved payload value type for out-of-band binary content.
//!
//! A blob ref is a leaf value, not a CRDT primitive: it carries an opaque,
//! unguessable public handle plus render metadata, and small blobs inline their
//! bytes to skip a fetch. It merges like any other value (LWW on assignment),
//! so it lives in [`Scalar`] alongside the other leaf values. Bytes live in a
//! separate blob store, fetched by handle; that store, dedup, and GC land later
//! — this reserves the wire slot so the op envelope carries refs from v0.1.

use crdtsync_core::op::{Op, OpId, OpKind};
use crdtsync_core::{decode_op, encode_op, Anchor, BlobRef, DecodeError, Scalar, Side};

mod common;
use common::{cid, eid, stmp};

fn external() -> BlobRef {
    BlobRef {
        id: [7u8; 16],
        mime: "image/png".to_string(),
        size: 1_048_576,
        inline: None,
    }
}

fn inlined() -> BlobRef {
    let bytes = vec![0x89, b'P', b'N', b'G', 0x00, 0xFF];
    BlobRef {
        id: [9u8; 16],
        mime: "image/png".to_string(),
        size: bytes.len() as u64,
        inline: Some(bytes),
    }
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
fn fields_are_held_verbatim() {
    let b = external();
    assert_eq!(b.id, [7u8; 16]);
    assert_eq!(b.mime, "image/png");
    assert_eq!(b.size, 1_048_576);
    assert_eq!(b.inline, None);
}

#[test]
fn equal_refs_are_equal() {
    assert_eq!(Scalar::BlobRef(external()), Scalar::BlobRef(external()));
    assert_eq!(Scalar::BlobRef(inlined()), Scalar::BlobRef(inlined()));
}

#[test]
fn differing_handle_is_not_equal() {
    let mut other = external();
    other.id = [8u8; 16];
    assert_ne!(Scalar::BlobRef(external()), Scalar::BlobRef(other));
}

#[test]
fn differing_metadata_is_not_equal() {
    let mut mime = external();
    mime.mime = "image/jpeg".to_string();
    assert_ne!(Scalar::BlobRef(external()), Scalar::BlobRef(mime));

    let mut size = external();
    size.size = 42;
    assert_ne!(Scalar::BlobRef(external()), Scalar::BlobRef(size));
}

#[test]
fn inline_bytes_are_significant() {
    let mut a = inlined();
    let mut b = inlined();
    assert_eq!(Scalar::BlobRef(a.clone()), Scalar::BlobRef(b.clone()));
    if let Some(bytes) = b.inline.as_mut() {
        bytes[0] ^= 1;
    }
    assert_ne!(Scalar::BlobRef(a.clone()), Scalar::BlobRef(b));

    // inline present vs absent, all else equal, still differ.
    a.inline = None;
    assert_ne!(Scalar::BlobRef(inlined()), Scalar::BlobRef(a));
}

// A blob ref that inlines the same bytes as a Bytes scalar is still a distinct
// value: the ref has identity and fetch semantics, raw bytes do not.
#[test]
fn a_ref_never_equals_a_bytes_scalar() {
    let raw = vec![0x89, b'P', b'N', b'G', 0x00, 0xFF];
    assert_ne!(Scalar::BlobRef(inlined()), Scalar::Bytes(raw));
}

// --- wire slot: refs round-trip through the op codec from v0.1 ---

#[test]
fn a_ref_round_trips_as_a_register_value() {
    for b in [external(), inlined()] {
        let o = op(OpKind::RegisterSet {
            key: b"avatar".to_vec(),
            value: Scalar::BlobRef(b),
        });
        assert_eq!(decode_op(&encode_op(&o)).expect("decodes"), o);
    }
}

#[test]
fn a_ref_round_trips_as_a_map_value() {
    let o = op(OpKind::MapSet {
        key: b"cover".to_vec(),
        value: Scalar::BlobRef(external()),
    });
    assert_eq!(decode_op(&encode_op(&o)).expect("decodes"), o);
}

#[test]
fn a_ref_round_trips_as_a_list_item() {
    let o = op(OpKind::ListInsert {
        value: Scalar::BlobRef(inlined()),
        anchor: Anchor {
            parent: None,
            side: Side::Right,
        },
    });
    assert_eq!(decode_op(&encode_op(&o)).expect("decodes"), o);
}

#[test]
fn a_ref_round_trips_as_a_standalone_value() {
    for b in [external(), inlined()] {
        let v = Scalar::BlobRef(b);
        assert_eq!(Scalar::decode_state(&v.encode_state()).unwrap(), v);
    }
}

#[test]
fn ref_encoding_is_deterministic() {
    let v = Scalar::BlobRef(inlined());
    assert_eq!(v.encode_state(), v.encode_state());
}

// --- decoding stays total over the new slot ---

#[test]
fn a_truncated_ref_is_an_error_not_a_panic() {
    let bytes = encode_op(&op(OpKind::RegisterSet {
        key: b"a".to_vec(),
        value: Scalar::BlobRef(inlined()),
    }));
    for cut in 0..bytes.len() {
        assert_eq!(decode_op(&bytes[..cut]), Err(DecodeError::UnexpectedEof));
    }
}
