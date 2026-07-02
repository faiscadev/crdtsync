//! Change-list codec — a diff serialized to bytes for the SDK boundary.
//!
//! A diff computed in the core crosses to a language binding as one buffer:
//! [`encode_changes`] writes it, [`decode_changes`] reads it back. The encoding
//! round-trips every `Change` variant exactly and decodes totally — malformed
//! bytes yield a `DecodeError`, never a panic. It is not durable; a diff is a
//! transient result, not stored.

use crdtsync_core::codec::DecodeError;
use crdtsync_core::diff::{decode_changes, diff, encode_changes, Change, SeqItem};
use crdtsync_core::doc::Document;
use crdtsync_core::element::ElementKind;
use crdtsync_core::path::encode_path;
use crdtsync_core::{ClientId, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn p(keys: &[&[u8]]) -> Vec<u8> {
    encode_path(keys)
}

fn round_trips(changes: Vec<Change>) {
    let bytes = encode_changes(&changes);
    assert_eq!(decode_changes(&bytes).expect("decodes"), changes);
}

#[test]
fn an_empty_change_list_round_trips() {
    round_trips(Vec::new());
}

#[test]
fn every_variant_round_trips() {
    round_trips(vec![
        Change::Added {
            path: p(&[b"a"]),
            kind: ElementKind::Register,
        },
        Change::Removed {
            path: p(&[b"b", b"c"]),
            kind: ElementKind::Map,
        },
        Change::Value {
            path: p(&[b"age"]),
            old: Scalar::Int(30),
            new: Scalar::Int(31),
        },
        Change::Counter {
            path: p(&[b"hits"]),
            old: -5,
            new: 9,
        },
        Change::ListInsert {
            path: p(&[b"items"]),
            index: 2,
            items: vec![
                SeqItem::Scalar(Scalar::Int(7)),
                SeqItem::Composite(ElementKind::Text),
            ],
        },
        Change::ListDelete {
            path: p(&[b"items"]),
            index: 0,
            items: vec![SeqItem::Scalar(Scalar::Bytes(vec![1, 2, 3]))],
        },
        Change::TextInsert {
            path: p(&[b"body"]),
            index: 4,
            text: "café".to_string(),
        },
        Change::TextDelete {
            path: p(&[b"body"]),
            index: 0,
            text: "x".to_string(),
        },
    ]);
}

#[test]
fn a_computed_diff_round_trips() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.register(b"age", Scalar::Int(1));
        tx.text(b"body").insert(0, "hi");
        tx.list(b"xs").insert(0, Scalar::Int(9));
    });
    let old = Document::decode_state(&d.encode_state()).unwrap();
    d.transact(|tx| {
        tx.register(b"age", Scalar::Int(2));
        tx.text(b"body").insert(2, "!");
        tx.list(b"xs").insert(1, Scalar::Int(8));
    });
    let changes = diff(&old, &d);
    assert!(!changes.is_empty());
    round_trips(changes);
}

// --- decoding stays total ---

#[test]
fn a_truncated_change_list_is_an_error_not_a_panic() {
    let bytes = encode_changes(&[
        Change::Value {
            path: p(&[b"age"]),
            old: Scalar::Int(30),
            new: Scalar::Int(31),
        },
        Change::TextInsert {
            path: p(&[b"body"]),
            index: 2,
            text: "hello".to_string(),
        },
    ]);
    for cut in 0..bytes.len() {
        assert!(
            decode_changes(&bytes[..cut]).is_err(),
            "truncating to {cut} bytes must error",
        );
    }
}

#[test]
fn trailing_bytes_are_rejected() {
    let mut bytes = encode_changes(&[Change::Added {
        path: p(&[b"a"]),
        kind: ElementKind::Register,
    }]);
    bytes.push(0);
    assert_eq!(decode_changes(&bytes), Err(DecodeError::TrailingBytes));
}

#[test]
fn an_unknown_change_tag_is_an_error() {
    // A count of one, then a tag naming no variant.
    let bytes = [1, 0, 0, 0, 0xEE];
    assert!(matches!(
        decode_changes(&bytes),
        Err(DecodeError::BadTag { .. })
    ));
}
