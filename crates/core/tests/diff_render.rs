//! The default text renderer for a diff.
//!
//! `render` turns a change list into one human-readable line per change — the
//! engine's sensible default for a debug dump, an audit view, or a CLI, over
//! the same structured changes an app can render its own way. Paths print
//! slash-joined; values print plainly; a sequence run prints its index and
//! contents.

use crdtsync_core::diff::{diff, render, Change, SeqItem};
use crdtsync_core::doc::Document;
use crdtsync_core::element::ElementKind;
use crdtsync_core::elementid::ElementId;
use crdtsync_core::path::encode_path;
use crdtsync_core::{ClientId, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn eid(n: u8) -> ElementId {
    ElementId::from_bytes([n; 16])
}

fn p(keys: &[&[u8]]) -> Vec<u8> {
    encode_path(keys)
}

#[test]
fn renders_added_and_removed_with_kind() {
    let changes = vec![
        Change::Added {
            path: p(&[b"user", b"name"]),
            kind: ElementKind::Register,
        },
        Change::Removed {
            path: p(&[b"age"]),
            kind: ElementKind::Counter,
        },
    ];
    assert_eq!(
        render(&changes),
        vec![
            "+ /user/name (register)".to_string(),
            "- /age (counter)".to_string(),
        ]
    );
}

#[test]
fn renders_a_value_and_counter_change() {
    let changes = vec![
        Change::Value {
            path: p(&[b"age"]),
            old: Scalar::Int(30),
            new: Scalar::Int(31),
        },
        Change::Counter {
            path: p(&[b"hits"]),
            old: 3,
            new: 5,
        },
    ];
    assert_eq!(
        render(&changes),
        vec![
            "~ /age: 30 -> 31".to_string(),
            "~ /hits: 3 -> 5".to_string(),
        ]
    );
}

#[test]
fn renders_scalar_variants() {
    let changes = vec![Change::Value {
        path: p(&[b"k"]),
        old: Scalar::Null,
        new: Scalar::Bool(true),
    }];
    assert_eq!(render(&changes), vec!["~ /k: null -> true".to_string()]);
}

#[test]
fn renders_list_runs_with_index_and_items() {
    let changes = vec![
        Change::ListInsert {
            path: p(&[b"xs"]),
            index: 1,
            items: vec![
                SeqItem::Scalar(Scalar::Int(2)),
                SeqItem::Composite(ElementKind::Map),
            ],
        },
        Change::ListDelete {
            path: p(&[b"xs"]),
            index: 0,
            items: vec![SeqItem::Scalar(Scalar::Int(9))],
        },
    ];
    assert_eq!(
        render(&changes),
        vec!["+ /xs[1]: 2, <map>".to_string(), "- /xs[0]: 9".to_string(),]
    );
}

#[test]
fn renders_text_runs_quoted() {
    let changes = vec![
        Change::TextInsert {
            path: p(&[b"body"]),
            index: 2,
            text: "hi".to_string(),
        },
        Change::TextDelete {
            path: p(&[b"body"]),
            index: 0,
            text: "x".to_string(),
        },
    ];
    assert_eq!(
        render(&changes),
        vec![
            "+ /body[2]: \"hi\"".to_string(),
            "- /body[0]: \"x\"".to_string(),
        ]
    );
}

#[test]
fn renders_mark_changes() {
    let changes = vec![
        Change::MarkAdded {
            id: eid(1),
            seq: eid(2),
            name: b"bold".to_vec(),
            value: Scalar::Bool(true),
        },
        Change::MarkRemoved {
            id: eid(3),
            seq: eid(4),
            name: b"bold".to_vec(),
            value: Scalar::Bool(true),
        },
        Change::MarkChanged {
            id: eid(5),
            seq: eid(6),
            name: b"link".to_vec(),
            old: Scalar::Bytes(b"a".to_vec()),
            new: Scalar::Bytes(b"b".to_vec()),
        },
    ];
    assert_eq!(
        render(&changes),
        vec![
            "+ mark bold: true".to_string(),
            "- mark bold: true".to_string(),
            "~ mark link: <1 bytes> -> <1 bytes>".to_string(),
        ]
    );
}

#[test]
fn an_empty_diff_renders_no_lines() {
    assert!(render(&[]).is_empty());
}

#[test]
fn renders_a_computed_diff_one_line_per_change() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.register(b"age", Scalar::Int(1)));
    let old = Document::decode_state(&d.encode_state()).unwrap();
    d.transact(|tx| tx.register(b"age", Scalar::Int(2)));
    let changes = diff(&old, &d);
    assert_eq!(render(&changes).len(), changes.len());
}
