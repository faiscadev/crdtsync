//! Schema state validation — walking a document's materialized element tree
//! against a parsed [`Schema`] and producing the set of violations.
//!
//! The validator is a pure read over the live tree. It reports, per built
//! primitive, exactly the constraints the schema model expresses: a map slot
//! outside its type's allowlist, an element whose runtime kind does not match its
//! declared type, a register/counter value outside its numeric bounds, and a
//! list/text longer than its `max`. It never mutates — invariant repair consumes
//! this violation set. Because the walk order is deterministic (map keys sorted,
//! list items in sequence order, depth-first), two replicas that merged the same
//! ops produce byte-identical violation sets.

mod common;

use common::cid;
use crdtsync_core::doc::Document;
use crdtsync_core::schema::Schema;
use crdtsync_core::validate::{validate, Step, Violation, ViolationKind};
use crdtsync_core::{ElementKind, Scalar};

/// A schema exercising every built-primitive constraint: a bounded register, a
/// max-length text, a max-cardinality list, a bounded counter, and a nested map.
const SCHEMA: &str = r#"{
    "schema": "notes", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "title": "Title", "body": "Body", "tags": "Tags",
            "hits": "Hits", "meta": "Meta" } },
        "Title": { "kind": "register", "min": 0, "max": 280 },
        "Body":  { "kind": "text", "max": 5 },
        "Tags":  { "kind": "list", "items": "Title", "max": 2 },
        "Hits":  { "kind": "counter", "min": 0, "max": 100 },
        "Meta":  { "kind": "map", "children": { "author": "Title" } }
    }
}"#;

fn schema() -> Schema {
    Schema::parse(SCHEMA).expect("schema parses")
}

fn key(s: &str) -> Step {
    Step::Key(s.as_bytes().to_vec())
}

/// Whether `violations` contains one with this exact path and kind.
fn has(violations: &[Violation], path: &[Step], kind: &ViolationKind) -> bool {
    violations.iter().any(|v| v.path == path && &v.kind == kind)
}

// --- conforming ---

#[test]
fn a_conforming_document_has_no_violations() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.register(b"title", Scalar::Int(42));
        tx.text(b"body").insert(0, "hi");
        tx.list(b"tags"); // an empty list conforms — 0 items, within max
        tx.inc(b"hits", 10);
        tx.map(b"meta").register(b"author", Scalar::Int(7));
    });
    assert!(validate(&d, &schema()).is_empty());
}

#[test]
fn an_absent_declared_slot_is_not_a_violation() {
    // Map slots are optional — a schema slot the document never set is fine.
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.register(b"title", Scalar::Int(1)));
    assert!(validate(&d, &schema()).is_empty());
}

// --- register / counter bounds ---

#[test]
fn a_register_below_its_min_is_reported() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.register(b"title", Scalar::Int(-5)));
    assert_eq!(
        validate(&d, &schema()),
        vec![Violation {
            path: vec![key("title")],
            kind: ViolationKind::BelowMin { value: -5, min: 0 },
        }],
    );
}

#[test]
fn a_register_above_its_max_is_reported() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.register(b"title", Scalar::Int(999)));
    assert_eq!(
        validate(&d, &schema()),
        vec![Violation {
            path: vec![key("title")],
            kind: ViolationKind::AboveMax {
                value: 999,
                max: 280,
            },
        }],
    );
}

#[test]
fn a_non_numeric_register_value_has_no_bound_violation() {
    // Bounds apply only to an integer payload; a bool/bytes register is unbounded.
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.register(b"title", Scalar::Bool(true)));
    assert!(validate(&d, &schema()).is_empty());
}

#[test]
fn a_counter_below_its_min_is_reported() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.dec(b"hits", 5));
    assert_eq!(
        validate(&d, &schema()),
        vec![Violation {
            path: vec![key("hits")],
            kind: ViolationKind::BelowMin { value: -5, min: 0 },
        }],
    );
}

#[test]
fn a_counter_above_its_max_is_reported() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.inc(b"hits", 200));
    assert_eq!(
        validate(&d, &schema()),
        vec![Violation {
            path: vec![key("hits")],
            kind: ViolationKind::AboveMax {
                value: 200,
                max: 100,
            },
        }],
    );
}

// --- sequence length ---

#[test]
fn a_text_over_its_max_is_reported() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.text(b"body").insert(0, "hello!")); // 6 codepoints, max 5
    assert_eq!(
        validate(&d, &schema()),
        vec![Violation {
            path: vec![key("body")],
            kind: ViolationKind::TooLong { len: 6, max: 5 },
        }],
    );
}

#[test]
fn a_list_over_its_max_is_reported() {
    // Three items past a max of two. The items are scalars while `Tags` declares
    // register items, so each item is also a kind mismatch — the length violation
    // sits alongside them. Assert the length violation is present.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut l = tx.list(b"tags");
        l.insert(0, Scalar::Int(1));
        l.insert(1, Scalar::Int(2));
        l.insert(2, Scalar::Int(3));
    });
    let v = validate(&d, &schema());
    assert!(
        has(
            &v,
            &[key("tags")],
            &ViolationKind::TooLong { len: 3, max: 2 }
        ),
        "list length violation present: {v:?}",
    );
}

// --- structure / kind ---

#[test]
fn an_unknown_map_slot_is_reported() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.register(b"bogus", Scalar::Int(1)));
    assert_eq!(
        validate(&d, &schema()),
        vec![Violation {
            path: vec![key("bogus")],
            kind: ViolationKind::UnknownSlot,
        }],
    );
}

#[test]
fn a_slot_holding_the_wrong_kind_is_reported() {
    // `title` is a register, but the slot holds a counter.
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.inc(b"title", 1));
    assert_eq!(
        validate(&d, &schema()),
        vec![Violation {
            path: vec![key("title")],
            kind: ViolationKind::KindMismatch {
                expected: ElementKind::Register,
                found: ElementKind::Counter,
            },
        }],
    );
}

#[test]
fn a_raw_scalar_in_a_composite_slot_is_a_kind_mismatch() {
    // `title` is a register; a raw MapSet scalar is not a register.
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.set(b"title", Scalar::Int(1)));
    assert_eq!(
        validate(&d, &schema()),
        vec![Violation {
            path: vec![key("title")],
            kind: ViolationKind::KindMismatch {
                expected: ElementKind::Register,
                found: ElementKind::Scalar,
            },
        }],
    );
}

#[test]
fn a_mismatched_element_is_not_recursed_into() {
    // `meta` is declared a map; holding a register there is one mismatch, and its
    // (absent) declared children are not then walked.
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.register(b"meta", Scalar::Int(1)));
    assert_eq!(
        validate(&d, &schema()),
        vec![Violation {
            path: vec![key("meta")],
            kind: ViolationKind::KindMismatch {
                expected: ElementKind::Map,
                found: ElementKind::Register,
            },
        }],
    );
}

// --- recursion ---

#[test]
fn a_violation_in_a_nested_map_carries_the_full_path() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.map(b"meta").register(b"author", Scalar::Int(-1)));
    assert_eq!(
        validate(&d, &schema()),
        vec![Violation {
            path: vec![key("meta"), key("author")],
            kind: ViolationKind::BelowMin { value: -1, min: 0 },
        }],
    );
}

#[test]
fn an_unknown_slot_inside_a_nested_map_is_reported() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.map(b"meta").register(b"stray", Scalar::Int(1)));
    assert_eq!(
        validate(&d, &schema()),
        vec![Violation {
            path: vec![key("meta"), key("stray")],
            kind: ViolationKind::UnknownSlot,
        }],
    );
}

// --- multiple + determinism ---

#[test]
fn multiple_violations_are_collected_in_a_deterministic_order() {
    // Map keys are walked sorted, so the order is stable regardless of set order.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.inc(b"hits", 500); // above max
        tx.register(b"title", Scalar::Int(-1)); // below min
    });
    // Sorted keys: "hits" < "title".
    assert_eq!(
        validate(&d, &schema()),
        vec![
            Violation {
                path: vec![key("hits")],
                kind: ViolationKind::AboveMax {
                    value: 500,
                    max: 100,
                },
            },
            Violation {
                path: vec![key("title")],
                kind: ViolationKind::BelowMin { value: -1, min: 0 },
            },
        ],
    );
}

#[test]
#[cfg_attr(miri, ignore = "stack depth is a native concern; slow under Miri")]
fn a_deeply_nested_document_is_validated_without_overflowing() {
    // A self-referential schema lets a document nest without bound; the walk is
    // iterative, so a deep tree is checked without recursing off the stack.
    const RECURSIVE: &str = r#"{ "schema": "deep", "version": 1, "root": "N",
        "types": { "N": { "kind": "map", "children": { "k": "N", "leaf": "V" } },
                   "V": { "kind": "register", "min": 0, "max": 10 } } }"#;
    let s = Schema::parse(RECURSIVE).expect("schema parses");

    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut cur = tx.map(b"k");
        for _ in 0..10_000 {
            cur = cur.into_map(b"k");
        }
        cur.register(b"leaf", Scalar::Int(-1)); // one deep violation, below min
    });

    let v = validate(&d, &s);
    assert_eq!(v.len(), 1, "only the leaf violates");
    assert_eq!(v[0].kind, ViolationKind::BelowMin { value: -1, min: 0 });
}

#[test]
fn two_replicas_that_merged_the_same_ops_produce_the_same_violation_set() {
    let mut a = Document::new(cid(1));
    let ops = a.transact(|tx| {
        tx.register(b"title", Scalar::Int(999));
        tx.inc(b"hits", 500);
        tx.map(b"meta").register(b"author", Scalar::Int(-3));
    });
    let mut b = Document::new(cid(2));
    for op in &ops {
        b.apply(op);
    }
    let s = schema();
    assert_eq!(validate(&a, &s), validate(&b, &s));
    assert!(
        !validate(&a, &s).is_empty(),
        "the shared state does violate"
    );
}
