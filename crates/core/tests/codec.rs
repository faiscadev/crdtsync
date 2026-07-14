//! Codec — the stable binary encoding for ops.
//!
//! Every op round-trips through `encode`/`decode` unchanged, an op log is a
//! length-framed sequence of them, and a replica rebuilt by replaying a decoded
//! log converges with the original. Decoding is total: malformed bytes yield a
//! `DecodeError`, never a panic.

use crdtsync_core::acl::{AclEffect, AclGrant, AclSubject, Capability};
use crdtsync_core::doc::Document;
use crdtsync_core::op::{Op, OpId, OpKind};
use crdtsync_core::{
    decode_op, decode_ops, encode_op, encode_ops, Anchor, BlobRef, DecodeError, Element, Scalar,
    Side,
};

mod common;
use common::{cid, eid, stmp};

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

/// One op of every kind, covering each scalar and anchor shape.
fn one_of_each() -> Vec<Op> {
    let anchor_root = Anchor {
        parent: None,
        side: Side::Right,
    };
    let anchor_child = Anchor {
        parent: Some(stmp(3, 9)),
        side: Side::Left,
    };
    vec![
        op(OpKind::RegisterSet {
            key: b"r".to_vec(),
            value: Scalar::Null,
        }),
        op(OpKind::RegisterSet {
            key: b"r".to_vec(),
            value: Scalar::Bool(true),
        }),
        op(OpKind::CounterInc {
            key: b"c".to_vec(),
            amount: 5,
        }),
        op(OpKind::CounterDec {
            key: b"c".to_vec(),
            amount: 4_000_000_000,
        }),
        op(OpKind::MapSet {
            key: b"m".to_vec(),
            value: Scalar::Int(-9),
        }),
        op(OpKind::MapSet {
            key: Vec::new(),
            value: Scalar::Bytes(vec![0, 1, 0, 255]), // embedded NUL is part of the value
        }),
        op(OpKind::MapSet {
            key: b"cover".to_vec(),
            value: Scalar::BlobRef(BlobRef {
                id: [3u8; 16],
                mime: "image/png".to_string(),
                size: 2048,
                inline: Some(vec![0x89, b'P', b'N', b'G']),
            }),
        }),
        op(OpKind::RegisterSet {
            key: b"doc".to_vec(),
            value: Scalar::BlobRef(BlobRef {
                id: [4u8; 16],
                mime: "application/pdf".to_string(),
                size: 9_000_000,
                inline: None,
            }),
        }),
        op(OpKind::MapDelete { key: b"d".to_vec() }),
        op(OpKind::MapCreate { key: b"n".to_vec() }),
        op(OpKind::ListCreate { key: b"l".to_vec() }),
        op(OpKind::ListInsert {
            value: Scalar::Bytes(vec![b'x']),
            anchor: anchor_root,
        }),
        op(OpKind::ListDelete { id: stmp(11, 2) }),
        op(OpKind::TextCreate { key: b"t".to_vec() }),
        op(OpKind::TextInsert {
            s: "héllo 👍".to_string(),
            anchor: anchor_child,
        }),
        op(OpKind::TextDelete {
            ids: vec![stmp(1, 1), stmp(2, 2), stmp(3, 3)],
        }),
        // Every ACL subject variant, both grant flavors, both effects.
        op(OpKind::AclGrant {
            subject: AclSubject::Actor(cid(2)),
            grant: AclGrant::Capability(Capability::Read),
            effect: AclEffect::Allow,
            path: b"\x03\0\0\0doc".to_vec(),
            grantor: cid(9),
        }),
        op(OpKind::AclGrant {
            subject: AclSubject::Group(b"designers".to_vec()),
            grant: AclGrant::Capability(Capability::Write),
            effect: AclEffect::Deny,
            path: Vec::new(),
            grantor: cid(9),
        }),
        op(OpKind::AclGrant {
            subject: AclSubject::Authenticated,
            grant: AclGrant::Capability(Capability::PublishAwareness),
            effect: AclEffect::Allow,
            path: b"p".to_vec(),
            grantor: cid(1),
        }),
        op(OpKind::AclGrant {
            subject: AclSubject::Anonymous,
            grant: AclGrant::Capability(Capability::Own),
            effect: AclEffect::Deny,
            path: b"p".to_vec(),
            grantor: cid(1),
        }),
        op(OpKind::AclGrant {
            subject: AclSubject::Anyone,
            grant: AclGrant::Role(b"editor".to_vec()),
            effect: AclEffect::Allow,
            path: b"p".to_vec(),
            grantor: cid(1),
        }),
        op(OpKind::AclRevoke {
            id: eid(0x11, 0x22),
        }),
    ]
}

#[test]
fn every_op_kind_round_trips() {
    for original in one_of_each() {
        let bytes = encode_op(&original);
        let back = decode_op(&bytes).expect("decodes");
        assert_eq!(back, original);
    }
}

#[test]
fn an_op_log_round_trips_in_order() {
    let ops = one_of_each();
    let bytes = encode_ops(&ops);
    let back = decode_ops(&bytes).expect("decodes");
    assert_eq!(back, ops);
}

#[test]
fn encoding_is_deterministic() {
    for o in one_of_each() {
        assert_eq!(encode_op(&o), encode_op(&o));
    }
}

#[test]
fn an_empty_log_round_trips() {
    assert!(decode_ops(&encode_ops(&[])).unwrap().is_empty());
}

#[test]
fn a_tx_id_round_trips() {
    use crdtsync_core::op::{Tx, TxId};
    let mut o = op(OpKind::MapDelete { key: b"k".to_vec() });
    o.tx = Some(Tx {
        id: TxId(99),
        count: 3,
    });
    assert_eq!(decode_op(&encode_op(&o)).unwrap(), o);
}

#[test]
fn a_zone_dimension_round_trips() {
    // The root partition (None) and a declared zone id both survive the codec.
    let mut root = op(OpKind::MapDelete { key: b"k".to_vec() });
    root.zone = None;
    assert_eq!(decode_op(&encode_op(&root)).unwrap(), root);

    let mut zoned = op(OpKind::MapSet {
        key: b"k".to_vec(),
        value: Scalar::Int(1),
    });
    zoned.zone = Some(3);
    assert_eq!(decode_op(&encode_op(&zoned)).unwrap(), zoned);
}

#[test]
fn a_truncated_zoned_op_is_an_error_not_a_panic() {
    let mut o = op(OpKind::MapCreate { key: b"n".to_vec() });
    o.zone = Some(42);
    let bytes = encode_op(&o);
    for cut in 0..bytes.len() {
        assert_eq!(decode_op(&bytes[..cut]), Err(DecodeError::UnexpectedEof));
    }
}

#[test]
fn an_unknown_zone_present_flag_is_an_error() {
    // The zone present-flag is the final byte an op with no tx encodes; a value
    // past 1 names no shape and must be rejected, not misread.
    let mut bytes = encode_op(&op(OpKind::MapCreate { key: b"n".to_vec() }));
    let flag_at = bytes.len() - 1;
    bytes[flag_at] = 7;
    assert_eq!(
        decode_op(&bytes),
        Err(DecodeError::BadTag {
            what: "op zone",
            tag: 7,
        })
    );
}

// --- decoding is total ---

#[test]
fn a_truncated_op_is_an_error_not_a_panic() {
    let bytes = encode_op(&op(OpKind::MapCreate { key: b"n".to_vec() }));
    for cut in 0..bytes.len() {
        assert_eq!(decode_op(&bytes[..cut]), Err(DecodeError::UnexpectedEof));
    }
}

#[test]
fn trailing_bytes_after_one_op_are_rejected() {
    let mut bytes = encode_op(&op(OpKind::MapCreate { key: b"n".to_vec() }));
    bytes.push(0);
    assert_eq!(decode_op(&bytes), Err(DecodeError::TrailingBytes));
}

#[test]
fn an_unknown_op_tag_is_an_error() {
    // Header: 16-byte client + u64 seq + stamp(8+16) + 16-byte target, then the
    // opkind tag. A tag past the last variant must be rejected.
    let mut bytes = encode_op(&op(OpKind::MapCreate { key: b"n".to_vec() }));
    let kind_tag_at = 16 + 8 + (8 + 16) + 16;
    bytes[kind_tag_at] = 200;
    assert_eq!(
        decode_op(&bytes),
        Err(DecodeError::BadTag {
            what: "opkind",
            tag: 200,
        })
    );
}

#[test]
fn non_utf8_text_is_an_error() {
    let good = op(OpKind::TextInsert {
        s: "ok".to_string(),
        anchor: Anchor {
            parent: None,
            side: Side::Right,
        },
    });
    let mut bytes = encode_op(&good);
    // The string bytes are the two "ok" bytes near the end, before the anchor's
    // two trailing tag bytes and the op's tx- and zone-flag bytes; corrupt the
    // first of them to an invalid lead byte.
    let s_at = bytes.len() - 5;
    bytes[s_at] = 0xFF;
    assert_eq!(decode_op(&bytes), Err(DecodeError::BadUtf8));
}

#[test]
fn a_corrupt_frame_length_is_an_error() {
    let mut bytes = encode_ops(&[op(OpKind::MapCreate { key: b"n".to_vec() })]);
    // Overwrite the leading u32 frame length with something enormous.
    bytes[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(decode_ops(&bytes), Err(DecodeError::UnexpectedEof));
}

// --- persistence: a replayed log rebuilds the document ---

use std::cell::RefCell;
use std::rc::Rc;

fn int(e: Option<Element>) -> i64 {
    match e {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected Int, got {other:?}"),
        },
        _ => panic!("expected a Register"),
    }
}

fn child_map(e: Option<Element>) -> Rc<RefCell<crdtsync_core::Map>> {
    match e {
        Some(Element::Map(m)) => m,
        _ => panic!("expected a Map"),
    }
}

fn text_str(e: Option<Element>) -> String {
    match e {
        Some(Element::Text(t)) => t.borrow().as_string(),
        _ => panic!("expected a Text"),
    }
}

#[test]
fn a_replayed_log_reconstructs_the_document() {
    let mut a = Document::new(cid(1));
    let mut log = Vec::new();
    log.extend(a.transact(|tx| {
        tx.register(b"age", Scalar::Int(30));
        tx.inc(b"hits", 3);
    }));
    log.extend(a.transact(|tx| {
        let mut profile = tx.map(b"profile");
        profile.register(b"score", Scalar::Int(7));
    }));
    log.extend(a.transact(|tx| tx.text(b"title").insert(0, "hello")));

    // Persist the whole log, then rebuild a fresh replica from the bytes alone.
    let bytes = encode_ops(&log);
    let decoded = decode_ops(&bytes).expect("decodes");
    let mut b = Document::new(cid(2));
    for op in &decoded {
        b.apply(op);
    }

    assert_eq!(int(b.get(b"age")), 30);
    let profile = child_map(b.get(b"profile"));
    assert_eq!(int(profile.borrow().get(b"score")), 7);
    assert_eq!(text_str(b.get(b"title")), "hello");
}

#[test]
fn a_log_replayed_out_of_order_still_converges() {
    let mut a = Document::new(cid(1));
    let mut log = Vec::new();
    log.extend(a.transact(|tx| {
        let mut outer = tx.map(b"a");
        let mut inner = outer.map(b"b");
        inner.register(b"deep", Scalar::Int(7));
    }));

    let decoded = decode_ops(&encode_ops(&log)).expect("decodes");
    let mut b = Document::new(cid(2));
    for op in decoded.iter().rev() {
        b.apply(op);
    }
    let outer = child_map(b.get(b"a"));
    let inner = child_map(outer.borrow().get(b"b"));
    assert_eq!(int(inner.borrow().get(b"deep")), 7);
}
