//! Structural diff between two replica snapshots.
//!
//! A document is a structured Element tree, not an opaque blob, so the change
//! from one snapshot to another is computable as a list of structural changes:
//! slots added and removed, scalar / register / counter values changed, and a
//! nested map walked so a deep edit reports at its own path. Sequences (List /
//! Text) report *that* they changed — element- and char-level detail is a
//! follow-on. The change list is path-addressed and deterministically ordered,
//! so two callers diffing the same pair agree.

use crdtsync_core::diff::{diff, Change};
use crdtsync_core::doc::Document;
use crdtsync_core::element::ElementKind;
use crdtsync_core::path::encode_path;
use crdtsync_core::{ClientId, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc() -> Document {
    Document::new(cid(1))
}

/// A snapshot copy of `d` — the same replica, same identities, at this instant.
/// Diffing a copy against the later live doc mirrors version-vs-current.
fn snapshot(d: &Document) -> Document {
    Document::decode_state(&d.encode_state()).expect("a fresh snapshot decodes")
}

fn p(keys: &[&[u8]]) -> Vec<u8> {
    encode_path(keys)
}

#[test]
fn identical_documents_have_no_changes() {
    let mut d = doc();
    d.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let old = snapshot(&d);
    assert!(diff(&old, &d).is_empty());
}

#[test]
fn an_added_slot_is_reported() {
    let d0 = doc();
    let old = snapshot(&d0);
    let mut new = d0;
    new.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert_eq!(
        diff(&old, &new),
        vec![Change::Added {
            path: p(&[b"age"]),
            kind: ElementKind::Register,
        }]
    );
}

#[test]
fn a_removed_slot_is_reported() {
    let mut d = doc();
    d.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let old = snapshot(&d);
    d.transact(|tx| tx.delete(b"age"));
    assert_eq!(
        diff(&old, &d),
        vec![Change::Removed {
            path: p(&[b"age"]),
            kind: ElementKind::Register,
        }]
    );
}

#[test]
fn a_changed_register_reports_old_and_new() {
    let mut d = doc();
    d.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let old = snapshot(&d);
    d.transact(|tx| tx.register(b"age", Scalar::Int(31)));
    assert_eq!(
        diff(&old, &d),
        vec![Change::Value {
            path: p(&[b"age"]),
            old: Scalar::Int(30),
            new: Scalar::Int(31),
        }]
    );
}

#[test]
fn an_unchanged_register_is_not_reported() {
    let mut d = doc();
    d.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let old = snapshot(&d);
    // Re-writing the same value is a new op but no state change.
    d.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert!(diff(&old, &d).is_empty());
}

#[test]
fn a_changed_counter_reports_old_and_new() {
    let mut d = doc();
    d.transact(|tx| tx.inc(b"hits", 3));
    let old = snapshot(&d);
    d.transact(|tx| tx.inc(b"hits", 2));
    assert_eq!(
        diff(&old, &d),
        vec![Change::Counter {
            path: p(&[b"hits"]),
            old: 3,
            new: 5,
        }]
    );
}

#[test]
fn a_nested_field_change_reports_at_its_deep_path() {
    let mut d = doc();
    d.transact(|tx| tx.map(b"user").register(b"name", Scalar::Int(1)));
    let old = snapshot(&d);
    d.transact(|tx| tx.map(b"user").register(b"name", Scalar::Int(2)));
    assert_eq!(
        diff(&old, &d),
        vec![Change::Value {
            path: p(&[b"user", b"name"]),
            old: Scalar::Int(1),
            new: Scalar::Int(2),
        }]
    );
}

#[test]
fn an_added_nested_slot_is_reported_at_its_path() {
    let mut d = doc();
    d.transact(|tx| tx.map(b"user").register(b"name", Scalar::Int(1)));
    let old = snapshot(&d);
    d.transact(|tx| tx.map(b"user").register(b"age", Scalar::Int(9)));
    assert_eq!(
        diff(&old, &d),
        vec![Change::Added {
            path: p(&[b"user", b"age"]),
            kind: ElementKind::Register,
        }]
    );
}

#[test]
fn a_slot_that_changes_kind_is_a_remove_then_add() {
    let mut d = doc();
    d.transact(|tx| tx.register(b"x", Scalar::Int(1)));
    let old = snapshot(&d);
    d.transact(|tx| tx.delete(b"x"));
    d.transact(|tx| tx.inc(b"x", 1));
    assert_eq!(
        diff(&old, &d),
        vec![
            Change::Removed {
                path: p(&[b"x"]),
                kind: ElementKind::Register,
            },
            Change::Added {
                path: p(&[b"x"]),
                kind: ElementKind::Counter,
            },
        ]
    );
}

#[test]
fn a_changed_list_reports_the_sequence_differs() {
    let mut d = doc();
    d.transact(|tx| tx.list(b"items").insert(0, Scalar::Int(1)));
    let old = snapshot(&d);
    d.transact(|tx| tx.list(b"items").insert(1, Scalar::Int(2)));
    assert_eq!(
        diff(&old, &d),
        vec![Change::Sequence {
            path: p(&[b"items"]),
            kind: ElementKind::List,
        }]
    );
}

#[test]
fn an_unchanged_list_is_not_reported() {
    let mut d = doc();
    d.transact(|tx| tx.list(b"items").insert(0, Scalar::Int(1)));
    let old = snapshot(&d);
    d.transact(|tx| tx.register(b"other", Scalar::Int(9)));
    // Only the new register is a change; the untouched list is silent.
    assert_eq!(
        diff(&old, &d),
        vec![Change::Added {
            path: p(&[b"other"]),
            kind: ElementKind::Register,
        }]
    );
}

#[test]
fn a_text_insert_reports_a_run_at_its_new_index() {
    let mut d = doc();
    d.transact(|tx| tx.text(b"body").insert(0, "hi"));
    let old = snapshot(&d);
    d.transact(|tx| tx.text(b"body").insert(2, "!!"));
    assert_eq!(
        diff(&old, &d),
        vec![Change::TextInsert {
            path: p(&[b"body"]),
            index: 2,
            text: "!!".to_string(),
        }]
    );
}

#[test]
fn a_text_delete_reports_a_run_at_its_old_index() {
    let mut d = doc();
    d.transact(|tx| tx.text(b"body").insert(0, "hello"));
    let old = snapshot(&d);
    d.transact(|tx| tx.text(b"body").delete(1, 3)); // "hello" -> "ho"
    assert_eq!(
        diff(&old, &d),
        vec![Change::TextDelete {
            path: p(&[b"body"]),
            index: 1,
            text: "ell".to_string(),
        }]
    );
}

#[test]
fn a_text_replacement_reports_the_delete_then_the_insert() {
    let mut d = doc();
    d.transact(|tx| tx.text(b"body").insert(0, "cat"));
    let old = snapshot(&d);
    d.transact(|tx| {
        tx.text(b"body").delete(1, 1); // "cat" -> "ct"
        tx.text(b"body").insert(1, "o"); // "ct" -> "cot"
    });
    assert_eq!(
        diff(&old, &d),
        vec![
            Change::TextDelete {
                path: p(&[b"body"]),
                index: 1,
                text: "a".to_string(),
            },
            Change::TextInsert {
                path: p(&[b"body"]),
                index: 1,
                text: "o".to_string(),
            },
        ]
    );
}

#[test]
fn an_unchanged_text_is_not_reported() {
    let mut d = doc();
    d.transact(|tx| tx.text(b"body").insert(0, "hi"));
    let old = snapshot(&d);
    d.transact(|tx| tx.register(b"other", Scalar::Int(1)));
    assert_eq!(
        diff(&old, &d),
        vec![Change::Added {
            path: p(&[b"other"]),
            kind: ElementKind::Register,
        }]
    );
}

#[test]
fn a_text_diff_counts_in_codepoints_not_bytes() {
    let mut d = doc();
    d.transact(|tx| tx.text(b"body").insert(0, "café"));
    let old = snapshot(&d);
    d.transact(|tx| tx.text(b"body").insert(4, "☕")); // append after 4 codepoints
    assert_eq!(
        diff(&old, &d),
        vec![Change::TextInsert {
            path: p(&[b"body"]),
            index: 4,
            text: "☕".to_string(),
        }]
    );
}

#[test]
fn changes_are_ordered_by_path() {
    let d0 = doc();
    let old = snapshot(&d0);
    let mut new = d0;
    new.transact(|tx| {
        tx.register(b"c", Scalar::Int(1));
        tx.register(b"a", Scalar::Int(1));
        tx.register(b"b", Scalar::Int(1));
    });
    let changes = diff(&old, &new);
    let paths: Vec<Vec<u8>> = changes
        .into_iter()
        .map(|c| match c {
            Change::Added { path, .. } => path,
            other => panic!("expected an add, got {other:?}"),
        })
        .collect();
    assert_eq!(paths, vec![p(&[b"a"]), p(&[b"b"]), p(&[b"c"])]);
}
