//! The diff surface over the path façade — the read model the language bindings
//! forward.
//!
//! [`path::diff`] presents the structural change list a binding renders
//! per-change; [`path::diff_encoded`] serializes that diff to one buffer a
//! binding forwards opaquely across the SDK boundary, and [`path::decode_changes`]
//! reads such a buffer back. The decode is total — a truncated or malformed
//! buffer errors, never panics, because a diff crosses an untrusted boundary.

use crdtsync_core::codec::DecodeError;
use crdtsync_core::diff::{encode_changes, Change, SeqItem};
use crdtsync_core::doc::Document;
use crdtsync_core::element::ElementKind;
use crdtsync_core::elementid::ElementId;
use crdtsync_core::list::Side;
use crdtsync_core::path;
use crdtsync_core::ranged::RangeAnchor;
use crdtsync_core::{ClientId, Element, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc() -> Document {
    Document::new(cid(1))
}

/// A fresh doc with a "body" Text holding `s` — the sequence marks anchor to.
fn doc_with_body(s: &str) -> Document {
    let mut d = doc();
    d.transact(|tx| tx.text(b"body").insert(0, s));
    d
}

/// The stable id of the "body" Text — the sequence a mark over it names.
fn body_seq(d: &Document) -> ElementId {
    ElementId::derive(d.root_id(), b"body", ElementKind::Text)
}

/// A non-growing span `[i, j)` over the body Text.
fn body_span(d: &Document, i: usize, j: usize) -> (RangeAnchor, RangeAnchor) {
    let t = match d.get(b"body") {
        Some(Element::Text(t)) => t,
        _ => panic!("no body text"),
    };
    let seq = body_seq(d);
    let start = RangeAnchor {
        seq,
        pos: t.borrow().relative_position(i, Side::Right),
    };
    let end = RangeAnchor {
        seq,
        pos: t.borrow().relative_position(j, Side::Left),
    };
    (start, end)
}

/// A snapshot copy of `d` — the same replica, same identities, at this instant.
fn snapshot(d: &Document) -> Document {
    Document::decode_state(&d.encode_state()).expect("a fresh snapshot decodes")
}

fn p(keys: &[&[u8]]) -> Vec<u8> {
    path::encode_path(keys)
}

// --- the structured diff over the façade ---

#[test]
fn the_facade_diff_reports_a_register_set() {
    let d0 = doc();
    let old = snapshot(&d0);
    let mut new = d0;
    new.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert_eq!(
        path::diff(&old, &new),
        vec![Change::Added {
            path: p(&[b"age"]),
            kind: ElementKind::Register,
        }]
    );
}

#[test]
fn the_facade_diff_reports_a_list_insert() {
    let mut d = doc();
    d.transact(|tx| tx.list(b"items").insert(0, Scalar::Int(1)));
    let old = snapshot(&d);
    d.transact(|tx| tx.list(b"items").insert(1, Scalar::Int(2)));
    assert_eq!(
        path::diff(&old, &d),
        vec![Change::ListInsert {
            path: p(&[b"items"]),
            index: 1,
            items: vec![SeqItem::Scalar(Scalar::Int(2))],
        }]
    );
}

#[test]
fn the_facade_diff_reports_an_xml_attr_change() {
    let mut d = doc();
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .attrs()
            .register(b"align", Scalar::Int(1));
    });
    let old = snapshot(&d);
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .attrs()
            .register(b"align", Scalar::Int(2));
    });
    assert_eq!(
        path::diff(&old, &d),
        vec![Change::Value {
            path: p(&[b"body", b"align"]),
            old: Scalar::Int(1),
            new: Scalar::Int(2),
        }]
    );
}

#[test]
fn the_facade_diff_reports_a_mark_added() {
    let mut d = doc_with_body("hello world");
    let old = snapshot(&d);
    let (s, e) = body_span(&d, 0, 5);
    let mut id = None;
    d.transact(|tx| id = Some(tx.ranged().mark(b"bold", s, e, Scalar::Bool(true))));
    assert_eq!(
        path::diff(&old, &d),
        vec![Change::MarkAdded {
            id: id.unwrap(),
            seq: body_seq(&d),
            name: b"bold".to_vec(),
            value: Scalar::Bool(true),
        }]
    );
}

// --- the encoded byte surface + boundary decode ---

#[test]
fn diff_encoded_decodes_to_the_structured_diff() {
    let mut d = doc_with_body("hello world");
    let old = snapshot(&d);
    let (s, e) = body_span(&d, 0, 5);
    d.transact(|tx| {
        tx.register(b"age", Scalar::Int(1));
        tx.text(b"body").insert(11, "!");
        tx.list(b"xs").insert(0, Scalar::Int(9));
        tx.ranged().mark(b"bold", s, e, Scalar::Bool(true));
    });
    let structured = path::diff(&old, &d);
    assert!(!structured.is_empty());
    let bytes = path::diff_encoded(&old, &d);
    assert_eq!(path::decode_changes(&bytes).expect("decodes"), structured);
}

#[test]
fn an_empty_diff_encodes_and_decodes() {
    let d = doc();
    let old = snapshot(&d);
    let bytes = path::diff_encoded(&old, &d);
    assert_eq!(path::decode_changes(&bytes).expect("decodes"), Vec::new());
}

#[test]
fn every_change_variant_survives_the_facade_decode() {
    // The façade decode is the exact inverse of the canonical encoder, for every
    // Change variant — tree and mark alike.
    let changes = vec![
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
        Change::MarkAdded {
            id: ElementId::from_bytes([1; 16]),
            seq: ElementId::from_bytes([2; 16]),
            name: b"bold".to_vec(),
            value: Scalar::Bool(true),
        },
        Change::MarkRemoved {
            id: ElementId::from_bytes([3; 16]),
            seq: ElementId::from_bytes([4; 16]),
            name: b"link".to_vec(),
            value: Scalar::Bytes(b"http://a".to_vec()),
        },
        Change::MarkChanged {
            id: ElementId::from_bytes([5; 16]),
            seq: ElementId::from_bytes([6; 16]),
            name: b"link".to_vec(),
            old: Scalar::Bytes(b"a".to_vec()),
            new: Scalar::Bytes(b"b".to_vec()),
        },
    ];
    let bytes = encode_changes(&changes);
    assert_eq!(path::decode_changes(&bytes).expect("decodes"), changes);
}

// --- the boundary decode stays total ---

#[test]
fn a_truncated_diff_buffer_is_an_error_not_a_panic() {
    let mut d = doc_with_body("hello world");
    let old = snapshot(&d);
    let (s, e) = body_span(&d, 0, 5);
    d.transact(|tx| {
        tx.register(b"age", Scalar::Int(1));
        tx.text(b"body").insert(11, "!");
        tx.ranged().mark(b"bold", s, e, Scalar::Bool(true));
    });
    let bytes = path::diff_encoded(&old, &d);
    for cut in 0..bytes.len() {
        assert!(
            path::decode_changes(&bytes[..cut]).is_err(),
            "truncating to {cut} bytes must error",
        );
    }
}

#[test]
fn trailing_bytes_after_a_diff_are_rejected() {
    let d0 = doc();
    let old = snapshot(&d0);
    let mut new = d0;
    new.transact(|tx| tx.register(b"age", Scalar::Int(1)));
    let mut bytes = path::diff_encoded(&old, &new);
    bytes.push(0);
    assert_eq!(
        path::decode_changes(&bytes),
        Err(DecodeError::TrailingBytes)
    );
}

#[test]
fn garbage_bytes_decode_to_an_error() {
    // A count of one, then a tag naming no variant.
    let bytes = [1, 0, 0, 0, 0xEE];
    assert!(matches!(
        path::decode_changes(&bytes),
        Err(DecodeError::BadTag { .. })
    ));
}
