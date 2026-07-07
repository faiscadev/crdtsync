//! Schema enforcement over XmlElement attrs — the spec (XmlElement Unit 5b-i).
//!
//! The validator descends an XmlElement's attrs Map against its declared type's
//! `attrs` allowlist and its children sequence against `children` (resolving each
//! child's type by tag), reporting the constraints the xml schema expresses: an
//! attribute key outside the allowlist (`DisallowedAttr`), an attribute whose
//! value is the wrong kind (`MistypedAttr`), and — reusing the built-primitive
//! rules once an attr recurses to its declared type — an out-of-range attr value
//! (`AboveMax`/`BelowMin`). Repair reads a disallowed / mistyped attr as dropped
//! and an out-of-range one as clamped. Everything is a pure read; state is never
//! mutated.

mod common;

use common::cid;
use crdtsync_core::doc::Document;
use crdtsync_core::repair::{repairs, Repair, RepairKind};
use crdtsync_core::schema::Schema;
use crdtsync_core::validate::{validate, Step, Violation, ViolationKind};
use crdtsync_core::{ElementKind, Scalar};

// A flat schema: the "body" slot holds a Para element (tag "p") whose only
// allowed attribute is a bounded "align" register.
const FLAT: &str = r#"{
    "schema": "prose", "version": 1, "root": "Doc",
    "types": {
        "Doc":  { "kind": "map", "children": { "body": "Para" } },
        "Para": { "kind": "xml", "tag": "p", "children": ["Span"], "attrs": { "align": "Align" } },
        "Span": { "kind": "text", "max": 1000 },
        "Align": { "kind": "register", "min": 0, "max": 2 }
    }
}"#;

// A nested schema: the "body" slot holds an Article fragment whose children are
// Para elements, so a nested element's attrs are reached only by recursion.
const NESTED: &str = r#"{
    "schema": "prose", "version": 1, "root": "Doc",
    "types": {
        "Doc":     { "kind": "map", "children": { "body": "Article" } },
        "Article": { "kind": "fragment", "children": ["Para"] },
        "Para":    { "kind": "xml", "tag": "p", "children": ["Span"], "attrs": { "align": "Align" } },
        "Span":    { "kind": "text", "max": 1000 },
        "Align":   { "kind": "register", "min": 0, "max": 2 }
    }
}"#;

fn schema(src: &str) -> Schema {
    Schema::parse(src).expect("schema parses")
}

fn key(s: &str) -> Step {
    Step::Key(s.as_bytes().to_vec())
}

fn has(violations: &[Violation], path: &[Step], kind: &ViolationKind) -> bool {
    violations.iter().any(|v| v.path == path && &v.kind == kind)
}

// --- validate: attrs ---

#[test]
fn a_conforming_xml_element_has_no_violations() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .attrs()
            .register(b"align", Scalar::Int(1));
    });
    assert!(validate(&d, &schema(FLAT)).is_empty());
}

#[test]
fn a_disallowed_attr_is_reported() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .attrs()
            .register(b"color", Scalar::Int(1));
    });
    let v = validate(&d, &schema(FLAT));
    assert!(has(
        &v,
        &[key("body"), key("color")],
        &ViolationKind::DisallowedAttr
    ));
}

#[test]
fn a_mistyped_attr_value_is_reported() {
    // "align" is declared a register; holding a Text there is a kind mismatch.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .attrs()
            .text(b"align")
            .insert(0, "left");
    });
    let v = validate(&d, &schema(FLAT));
    assert!(has(
        &v,
        &[key("body"), key("align")],
        &ViolationKind::MistypedAttr {
            expected: ElementKind::Register,
            found: ElementKind::Text,
        }
    ));
}

#[test]
fn an_out_of_range_attr_value_reuses_the_bounds_rule() {
    // A right-kind attr with an out-of-range value is a bounds violation, not a
    // mistype — the attr recurses to its declared type and the register rule fires.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .attrs()
            .register(b"align", Scalar::Int(9));
    });
    let v = validate(&d, &schema(FLAT));
    assert!(has(
        &v,
        &[key("body"), key("align")],
        &ViolationKind::AboveMax { value: 9, max: 2 }
    ));
}

#[test]
fn a_nested_elements_attr_is_validated_through_recursion() {
    // The Para is a child of the Article fragment, so its bad attr is reached only
    // by descending the children sequence and resolving the child's tag to Para.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_fragment(b"body")
            .children()
            .insert_element(0, b"p")
            .attrs()
            .register(b"color", Scalar::Int(1));
    });
    let v = validate(&d, &schema(NESTED));
    assert!(has(
        &v,
        &[key("body"), Step::Index(0), key("color")],
        &ViolationKind::DisallowedAttr
    ));
}

#[test]
fn a_conforming_nested_element_has_no_violations() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_fragment(b"body")
            .children()
            .insert_element(0, b"p")
            .attrs()
            .register(b"align", Scalar::Int(2));
    });
    assert!(validate(&d, &schema(NESTED)).is_empty());
}

// --- repair: attrs ---

#[test]
fn a_disallowed_attr_reads_dropped() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .attrs()
            .register(b"color", Scalar::Int(1));
    });
    let r = repairs(&d, &schema(FLAT));
    assert!(r.contains(&Repair {
        path: vec![key("body"), key("color")],
        kind: RepairKind::Dropped,
    }));
}

#[test]
fn a_mistyped_attr_reads_dropped() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .attrs()
            .text(b"align")
            .insert(0, "left");
    });
    let r = repairs(&d, &schema(FLAT));
    assert!(r.contains(&Repair {
        path: vec![key("body"), key("align")],
        kind: RepairKind::Dropped,
    }));
}

#[test]
fn an_out_of_range_attr_reads_clamped() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .attrs()
            .register(b"align", Scalar::Int(9));
    });
    let r = repairs(&d, &schema(FLAT));
    assert!(r.contains(&Repair {
        path: vec![key("body"), key("align")],
        kind: RepairKind::Clamped { value: 2 },
    }));
}

// A nested schema whose text leaf is tightly bounded, so an over-long text child
// produces a TooLong violation located *through* an xml element.
const NESTED_TEXT_MAX: &str = r#"{
    "schema": "prose", "version": 1, "root": "Doc",
    "types": {
        "Doc":     { "kind": "map", "children": { "body": "Article" } },
        "Article": { "kind": "fragment", "children": ["Para"] },
        "Para":    { "kind": "xml", "tag": "p", "children": ["Span"], "attrs": { "align": "Align" } },
        "Span":    { "kind": "text", "max": 3 },
        "Align":   { "kind": "register", "min": 0, "max": 2 }
    }
}"#;

#[test]
fn an_over_max_text_child_reads_truncated_through_xml() {
    // The bounded Span is a text child nested under a Para under the fragment, so
    // its repair path traverses two xml elements — element_at must walk them, or
    // the closure guarantee (every violation has a repair) breaks.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_fragment(b"body")
            .children()
            .insert_element(0, b"p")
            .children()
            .insert_text(0)
            .insert(0, "hello");
    });
    let r = repairs(&d, &schema(NESTED_TEXT_MAX));
    assert!(
        r.iter().any(
            |rep| rep.path == vec![key("body"), Step::Index(0), Step::Index(0)]
                && matches!(rep.kind, RepairKind::Truncated { .. })
        ),
        "an over-max text child nested in xml gets a truncation repair, got {r:?}"
    );
}

// --- determinism ---

#[test]
fn disallowed_attrs_emit_in_sorted_key_order() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut el = tx.xml_element(b"body", b"p");
        el.attrs().register(b"size", Scalar::Int(1));
        el.attrs().register(b"color", Scalar::Int(1));
    });
    let v = validate(&d, &schema(FLAT));
    let attrs: Vec<&Vec<Step>> = v
        .iter()
        .filter(|x| x.kind == ViolationKind::DisallowedAttr)
        .map(|x| &x.path)
        .collect();
    assert_eq!(
        attrs,
        vec![
            &vec![key("body"), key("color")],
            &vec![key("body"), key("size")],
        ],
        "disallowed attrs emit in sorted key order"
    );
}

#[test]
fn two_replicas_that_merged_the_same_ops_produce_the_same_violations() {
    let mut a = Document::new(cid(1));
    let ops = a.transact(|tx| {
        let mut el = tx.xml_element(b"body", b"p");
        el.attrs().register(b"color", Scalar::Int(1));
        el.attrs().register(b"align", Scalar::Int(9));
    });
    let mut b = Document::new(cid(2));
    for op in &ops {
        b.apply(op);
    }
    assert_eq!(validate(&a, &schema(FLAT)), validate(&b, &schema(FLAT)));
}
