//! Per-type marks allowlist — the read-time filter that drops a mark whose name
//! is outside the containing XmlElement's declared type `marks` (XmlElement Unit
//! 5b-ii).
//!
//! A mark is stored as a `RangedElement` regardless of schema; only the
//! `marks_at` read applies the allowlist. When the sequence a mark covers is a
//! text child of a schema-typed XmlElement, a covering mark whose name is not in
//! that element type's `marks` is dropped from the read — the `RangedElement`
//! stays in state, raw annotation reads are unfiltered. A sequence with no
//! XmlElement container (a plain map-slot text, or a text directly under a
//! tagless fragment) has no allowlist: every mark is kept.

mod common;

use common::cid;
use crdtsync_core::doc::Document;
use crdtsync_core::list::Side;
use crdtsync_core::marks::MarkState;
use crdtsync_core::ranged::RangeAnchor;
use crdtsync_core::text::Text;
use crdtsync_core::{Element, Scalar};
use std::cell::RefCell;
use std::rc::Rc;

// Para (tag "p") allows only the "bold" mark; "comment" is a declared object
// mark but not in Para's allowlist. "note" is a plain top-level text with no
// xml container, so no allowlist governs it.
const SCHEMA: &str = r#"{
    "schema": "prose", "version": 1, "root": "Doc",
    "types": {
        "Doc":  { "kind": "map", "children": { "body": "Para", "note": "Free" } },
        "Para": { "kind": "xml", "tag": "p", "children": ["Span"], "marks": ["bold"] },
        "Span": { "kind": "text" },
        "Free": { "kind": "text" }
    },
    "marks": {
        "bold":    { "flavor": "boolean" },
        "comment": { "flavor": "object" }
    }
}"#;

// The "body" slot holds a tagless Article fragment whose text children carry no
// marks allowlist — a fragment does not restrict marks.
const FRAGMENT_SCHEMA: &str = r#"{
    "schema": "prose", "version": 1, "root": "Doc",
    "types": {
        "Doc":     { "kind": "map", "children": { "body": "Article" } },
        "Article": { "kind": "fragment", "children": ["Span"] },
        "Span":    { "kind": "text" }
    },
    "marks": {
        "comment": { "flavor": "object" }
    }
}"#;

fn schema(src: &str) -> crdtsync_core::schema::Schema {
    crdtsync_core::schema::Schema::parse(src).expect("schema parses")
}

/// The text child at index 0 of the xml node (element or fragment) in slot `key`.
fn xml_text_child(d: &Document, key: &[u8]) -> Rc<RefCell<Text>> {
    let children = match d.get(key) {
        Some(Element::XmlElement(x)) => x.borrow().children(),
        Some(Element::XmlFragment(f)) => f.borrow().children(),
        _ => panic!("no xml node in slot"),
    };
    let child = children.borrow().get(0);
    match child {
        Some(Element::Text(t)) => t,
        _ => panic!("no text child"),
    }
}

/// The plain top-level text in slot `key`.
fn plain_text(d: &Document, key: &[u8]) -> Rc<RefCell<Text>> {
    match d.get(key) {
        Some(Element::Text(t)) => t,
        _ => panic!("no text in slot"),
    }
}

/// A fixed span `[i, j)` over `t` — start pinned right, end pinned left.
fn span(t: &Rc<RefCell<Text>>, i: usize, j: usize) -> (RangeAnchor, RangeAnchor) {
    let tb = t.borrow();
    let seq = tb.id();
    (
        RangeAnchor {
            seq,
            pos: tb.relative_position(i, Side::Right),
        },
        RangeAnchor {
            seq,
            pos: tb.relative_position(j, Side::Left),
        },
    )
}

fn mark_names(d: &Document, t: &Rc<RefCell<Text>>, index: usize) -> Vec<Vec<u8>> {
    let seq = t.borrow().id();
    d.marks_at(seq, index).into_iter().map(|m| m.name).collect()
}

#[test]
fn a_disallowed_mark_on_an_xml_typed_text_child_is_dropped() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema(SCHEMA));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .children()
            .insert_text(0)
            .insert(0, "hello");
    });
    let t = xml_text_child(&d, b"body");
    let (s, e) = span(&t, 0, 5);
    d.transact(|tx| {
        tx.ranged().mark(b"comment", s, e, Scalar::Bool(true));
    });
    // "comment" is not in Para's marks allowlist → dropped from the read.
    assert!(mark_names(&d, &t, 2).is_empty());
}

#[test]
fn an_allowed_mark_on_an_xml_typed_text_child_is_kept() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema(SCHEMA));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .children()
            .insert_text(0)
            .insert(0, "hello");
    });
    let t = xml_text_child(&d, b"body");
    let (s, e) = span(&t, 0, 5);
    d.transact(|tx| {
        tx.ranged().mark(b"bold", s, e, Scalar::Bool(true));
    });
    let seq = t.borrow().id();
    assert_eq!(
        d.marks_at(seq, 2),
        vec![crdtsync_core::marks::ResolvedMark {
            name: b"bold".to_vec(),
            state: MarkState::Boolean(true),
        }]
    );
}

#[test]
fn allowed_and_disallowed_marks_coexist_only_the_allowed_survives() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema(SCHEMA));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .children()
            .insert_text(0)
            .insert(0, "hello");
    });
    let t = xml_text_child(&d, b"body");
    let (bs, be) = span(&t, 0, 5);
    let (cs, ce) = span(&t, 0, 5);
    d.transact(|tx| {
        tx.ranged().mark(b"bold", bs, be, Scalar::Bool(true));
        tx.ranged().mark(b"comment", cs, ce, Scalar::Bool(true));
    });
    assert_eq!(mark_names(&d, &t, 2), vec![b"bold".to_vec()]);
}

#[test]
fn a_disallowed_mark_on_a_plain_top_level_text_is_kept() {
    // "note" has no XmlElement container, so no marks allowlist applies — the
    // same mark that drops under Para survives here.
    let mut d = Document::new(cid(1));
    d.set_schema(schema(SCHEMA));
    d.transact(|tx| {
        tx.text(b"note").insert(0, "hello");
    });
    let t = plain_text(&d, b"note");
    let (s, e) = span(&t, 0, 5);
    d.transact(|tx| {
        tx.ranged().mark(b"comment", s, e, Scalar::Bool(true));
    });
    assert_eq!(mark_names(&d, &t, 2), vec![b"comment".to_vec()]);
}

#[test]
fn a_mark_on_a_text_under_a_fragment_is_kept() {
    // A tagless fragment carries no marks allowlist — its text children keep
    // every mark, like a plain top-level text.
    let mut d = Document::new(cid(1));
    d.set_schema(schema(FRAGMENT_SCHEMA));
    d.transact(|tx| {
        tx.xml_fragment(b"body")
            .children()
            .insert_text(0)
            .insert(0, "hello");
    });
    let t = xml_text_child(&d, b"body");
    let (s, e) = span(&t, 0, 5);
    d.transact(|tx| {
        tx.ranged().mark(b"comment", s, e, Scalar::Bool(true));
    });
    assert_eq!(mark_names(&d, &t, 2), vec![b"comment".to_vec()]);
}

#[test]
fn the_disallowed_mark_stays_in_the_raw_annotation_set() {
    // The filter is read-only: marks_at drops it, but the RangedElement is still
    // in state — ranged_on surfaces it.
    let mut d = Document::new(cid(1));
    d.set_schema(schema(SCHEMA));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .children()
            .insert_text(0)
            .insert(0, "hello");
    });
    let t = xml_text_child(&d, b"body");
    let seq = t.borrow().id();
    let (s, e) = span(&t, 0, 5);
    d.transact(|tx| {
        tx.ranged().mark(b"comment", s, e, Scalar::Bool(true));
    });
    assert!(d.marks_at(seq, 2).is_empty());
    assert!(
        !d.ranged_on(seq).is_empty(),
        "the RangedElement is unfiltered in the raw annotation set"
    );
}
