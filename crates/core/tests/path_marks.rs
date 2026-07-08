//! Path addressing over the marks read/write model — the marks half of the SDK
//! surface (XmlElement Unit 6a-iv).
//!
//! A mark is a RangedElement carrying a name, authored over a span of a sequence
//! (a Text or List) and read back per its schema-declared flavor by
//! `Document::marks_at`. The path façade addresses that sequence by a path: it
//! resolves the path to the sequence's id, captures the two endpoints as
//! RelativePositions, and returns the emitted ops plus the mark's id as bytes —
//! the handle a later `mark_set_value`/`mark_delete` addresses it by. A path that
//! is not a live sequence is inert.

use crdtsync_core::list::Side;
use crdtsync_core::marks::{MarkState, ResolvedMark};
use crdtsync_core::op::Op;
use crdtsync_core::schema::Schema;
use crdtsync_core::{path, Document, Scalar};

mod common;
use common::cid;

// A schema declaring the mark flavors over a top-level text body, so the read
// model resolves boolean/value marks (an undeclared name defaults to object).
const SCHEMA: &str = r#"{
    "schema": "doc", "version": 1, "root": "Doc",
    "types": { "Doc": { "kind": "map", "children": { "body": "Body" } }, "Body": { "kind": "text" } },
    "marks": {
        "bold": { "flavor": "boolean" },
        "link": { "flavor": "value" }
    }
}"#;

fn schema() -> Schema {
    Schema::parse(SCHEMA).unwrap()
}

fn p(keys: &[&str]) -> Vec<u8> {
    let keys: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
    path::encode_path(&keys)
}

fn replay(d: &mut Document, ops: &[Op]) {
    for op in ops {
        d.apply(op);
    }
}

/// A fresh replica with the schema bound and a "body" Text holding `s`, authored
/// through the path façade.
fn doc_with_body(client: u8, s: &str) -> Document {
    let mut d = Document::new(cid(client));
    d.set_schema(schema());
    path::text_insert(&mut d, &p(&["body"]), 0, s);
    d
}

/// Whether `name` reads as a present boolean mark on character `index` of the
/// body — through the path façade read.
fn is_bold(d: &Document, index: usize, name: &[u8]) -> bool {
    path::marks_at(d, &p(&["body"]), index)
        .into_iter()
        .any(|m| m.name == name && m.state == MarkState::Boolean(true))
}

/// The winning value of value-mark `name` on character `index` of the body.
fn value_of(d: &Document, index: usize, name: &[u8]) -> Option<Scalar> {
    path::marks_at(d, &p(&["body"]), index)
        .into_iter()
        .find_map(|m| match m.state {
            MarkState::Value(v) if m.name == name => Some(v),
            _ => None,
        })
}

// --- author + read ---

#[test]
fn a_mark_covers_exactly_its_span() {
    // "hello world" — a non-growing bold [0,5) covers "hello", not the rest.
    let mut d = doc_with_body(1, "hello world");
    let (ops, id) = path::mark(
        &mut d,
        &p(&["body"]),
        0,
        Side::Right,
        5,
        Side::Left,
        b"bold",
        Scalar::Bool(true),
    );
    assert!(!ops.is_empty(), "authoring a mark emits ops");
    assert!(id.is_some(), "a live mark yields a handle");

    for i in 0..5 {
        assert!(is_bold(&d, i, b"bold"), "char {i} bold");
    }
    for i in 5..11 {
        assert!(!is_bold(&d, i, b"bold"), "char {i} not bold");
    }
}

#[test]
fn a_value_mark_carries_and_changes_its_value() {
    let mut d = doc_with_body(1, "hello world");
    let (_, id) = path::mark(
        &mut d,
        &p(&["body"]),
        0,
        Side::Right,
        5,
        Side::Left,
        b"link",
        Scalar::Bytes(b"http://a".to_vec()),
    );
    let id = id.expect("mark handle");
    assert_eq!(
        value_of(&d, 2, b"link"),
        Some(Scalar::Bytes(b"http://a".to_vec()))
    );
    assert_eq!(value_of(&d, 7, b"link"), None, "outside the span");

    let ops = path::mark_set_value(&mut d, &id, Scalar::Bytes(b"http://b".to_vec()));
    assert!(!ops.is_empty(), "changing a live mark's value emits ops");
    assert_eq!(
        value_of(&d, 2, b"link"),
        Some(Scalar::Bytes(b"http://b".to_vec())),
        "the read reflects the new value"
    );
}

#[test]
fn deleting_a_mark_drops_it() {
    let mut d = doc_with_body(1, "hello world");
    let (_, id) = path::mark(
        &mut d,
        &p(&["body"]),
        0,
        Side::Right,
        5,
        Side::Left,
        b"bold",
        Scalar::Bool(true),
    );
    let id = id.expect("mark handle");
    assert!(is_bold(&d, 2, b"bold"), "bold before the delete");

    let ops = path::mark_delete(&mut d, &id);
    assert!(!ops.is_empty(), "deleting a live mark emits ops");
    assert!(
        path::marks_at(&d, &p(&["body"]), 2)
            .iter()
            .all(|m| m.name != b"bold"),
        "the mark is gone from the active set"
    );
}

// --- inert guards ---

#[test]
fn a_mark_on_a_nonsequence_path_is_inert() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema());
    // A register, not a sequence.
    path::register(&mut d, &p(&["reg"]), Scalar::Int(1));

    let (ops, id) = path::mark(
        &mut d,
        &p(&["reg"]),
        0,
        Side::Right,
        1,
        Side::Left,
        b"bold",
        Scalar::Bool(true),
    );
    assert!(ops.is_empty(), "no ops on a non-sequence path");
    assert!(id.is_none(), "no handle on a non-sequence path");

    // An absent path is equally inert.
    let (ops, id) = path::mark(
        &mut d,
        &p(&["missing"]),
        0,
        Side::Right,
        1,
        Side::Left,
        b"bold",
        Scalar::Bool(true),
    );
    assert!(ops.is_empty() && id.is_none());

    // marks_at on a non-sequence path is empty, never a panic.
    assert!(path::marks_at(&d, &p(&["reg"]), 0).is_empty());
    assert!(path::marks_at(&d, &p(&["missing"]), 0).is_empty());
}

#[test]
fn a_stale_or_malformed_handle_is_inert() {
    let mut d = doc_with_body(1, "hello world");
    // A handle of the wrong width never decodes to an id.
    assert!(path::mark_set_value(&mut d, &[0u8; 3], Scalar::Int(1)).is_empty());
    assert!(path::mark_delete(&mut d, &[0u8; 3]).is_empty());
    // A well-formed but absent id emits nothing.
    assert!(path::mark_set_value(&mut d, &[9u8; 16], Scalar::Int(1)).is_empty());
    assert!(path::mark_delete(&mut d, &[9u8; 16]).is_empty());

    // A delete over a genuine handle drops the mark, and stays dropped under a
    // redundant re-delete (a materialised-then-tombstoned id re-emits an
    // idempotent delete, convergent on every replica).
    let (_, id) = path::mark(
        &mut d,
        &p(&["body"]),
        0,
        Side::Right,
        5,
        Side::Left,
        b"bold",
        Scalar::Bool(true),
    );
    let id = id.unwrap();
    assert!(!path::mark_delete(&mut d, &id).is_empty());
    path::mark_delete(&mut d, &id);
    assert!(
        path::marks_at(&d, &p(&["body"]), 2)
            .iter()
            .all(|m| m.name != b"bold"),
        "the mark stays gone"
    );
}

// --- convergence + determinism ---

#[test]
fn marks_converge_on_a_peer() {
    let mut author = Document::new(cid(1));
    author.set_schema(schema());
    let build = path::text_insert(&mut author, &p(&["body"]), 0, "hello world");
    let (bold, _) = path::mark(
        &mut author,
        &p(&["body"]),
        0,
        Side::Right,
        5,
        Side::Left,
        b"bold",
        Scalar::Bool(true),
    );
    let (link, _) = path::mark(
        &mut author,
        &p(&["body"]),
        6,
        Side::Right,
        11,
        Side::Left,
        b"link",
        Scalar::Bytes(b"u".to_vec()),
    );

    let mut peer = Document::new(cid(2));
    peer.set_schema(schema());
    replay(&mut peer, &build);
    replay(&mut peer, &bold);
    replay(&mut peer, &link);

    assert!(is_bold(&peer, 2, b"bold"));
    assert!(!is_bold(&peer, 8, b"bold"));
    assert_eq!(
        value_of(&peer, 8, b"link"),
        Some(Scalar::Bytes(b"u".to_vec()))
    );
    assert_eq!(value_of(&peer, 2, b"link"), None);
}

#[test]
fn authoring_is_deterministic() {
    // Two replicas of the same client author the identical marks — same ops, same
    // handle bytes, so an inverse computed on one replica is valid on the other.
    let build = |client: u8| -> (Vec<Op>, Vec<u8>) {
        let mut d = doc_with_body(client, "hello world");
        let (ops, id) = path::mark(
            &mut d,
            &p(&["body"]),
            0,
            Side::Right,
            5,
            Side::Left,
            b"bold",
            Scalar::Bool(true),
        );
        (ops, id.unwrap())
    };
    let (ops_a, id_a) = build(7);
    let (ops_b, id_b) = build(7);
    assert_eq!(
        id_a, id_b,
        "the handle is a deterministic function of authorship"
    );
    assert_eq!(
        format!("{ops_a:?}"),
        format!("{ops_b:?}"),
        "the emitted ops are deterministic"
    );
}

#[test]
fn the_read_shape_mirrors_the_core_read_model() {
    // The façade read returns the core ResolvedMark set unchanged — a boolean mark
    // reads Boolean(true), a value mark reads Value(scalar).
    let mut d = doc_with_body(1, "abcdef");
    path::mark(
        &mut d,
        &p(&["body"]),
        0,
        Side::Right,
        3,
        Side::Left,
        b"bold",
        Scalar::Bool(true),
    );
    path::mark(
        &mut d,
        &p(&["body"]),
        0,
        Side::Right,
        3,
        Side::Left,
        b"link",
        Scalar::Int(42),
    );
    let marks: Vec<ResolvedMark> = path::marks_at(&d, &p(&["body"]), 1);
    assert!(marks
        .iter()
        .any(|m| m.name == b"bold" && m.state == MarkState::Boolean(true)));
    assert!(marks
        .iter()
        .any(|m| m.name == b"link" && m.state == MarkState::Value(Scalar::Int(42))));
}
