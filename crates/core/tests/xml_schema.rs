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
use crdtsync_core::elementid::ElementId;
use crdtsync_core::repair::{repairs, Repair, RepairKind};
use crdtsync_core::schema::Schema;
use crdtsync_core::validate::{validate, Step, Violation, ViolationKind};
use crdtsync_core::{Element, ElementKind, Scalar};

// A flat schema: the "body" slot holds a Para element (tag "p") whose only
// allowed attribute is a bounded "align" register.
const FLAT: &str = r#"{
    "schema": "prose", "version": 1, "root": "Doc",
    "types": {
        "Doc":  { "kind": "map", "children": { "body": "Para" } },
        "Para": { "kind": "xml", "tag": "p", "children": { "Span": {} }, "attrs": { "align": "Align" } },
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
        "Article": { "kind": "fragment", "children": { "Para": {} } },
        "Para":    { "kind": "xml", "tag": "p", "children": { "Span": {} }, "attrs": { "align": "Align" } },
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

// --- validate: disallowed children ---

#[test]
fn a_disallowed_child_element_is_reported() {
    // Para allows only Span text children; a <b> element child matches no allowed
    // child type, so it is a disallowed child at its sequence position.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .children()
            .insert_element(0, b"b");
    });
    let v = validate(&d, &schema(FLAT));
    assert!(has(
        &v,
        &[key("body"), Step::Index(0)],
        &ViolationKind::DisallowedChild
    ));
}

#[test]
fn a_nested_disallowed_child_is_reported_through_recursion() {
    // Under the Article fragment a Para is allowed, but a <b> inside that Para is
    // not (Para allows only Span) — reached only by descending the conforming Para.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut body = tx.xml_fragment(b"body");
        let mut kids = body.children();
        let mut para = kids.insert_element(0, b"p");
        para.children().insert_element(0, b"b");
    });
    let v = validate(&d, &schema(NESTED));
    assert!(has(
        &v,
        &[key("body"), Step::Index(0), Step::Index(0)],
        &ViolationKind::DisallowedChild
    ));
}

#[test]
fn a_conforming_child_is_not_reported() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .children()
            .insert_text(0)
            .insert(0, "hi");
    });
    assert!(validate(&d, &schema(FLAT)).is_empty());
}

#[test]
fn disallowed_children_emit_in_sequence_order() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut el = tx.xml_element(b"body", b"p");
        let mut kids = el.children();
        kids.insert_element(0, b"b");
        kids.insert_element(1, b"i");
    });
    let v = validate(&d, &schema(FLAT));
    let children: Vec<&Vec<Step>> = v
        .iter()
        .filter(|x| x.kind == ViolationKind::DisallowedChild)
        .map(|x| &x.path)
        .collect();
    assert_eq!(
        children,
        vec![
            &vec![key("body"), Step::Index(0)],
            &vec![key("body"), Step::Index(1)],
        ],
        "disallowed children emit in sequence order"
    );
}

// A schema whose Para block declares an orphan-inline wrap target: loose inline
// text under it is to be wrapped (5c-ii), not dropped as a disallowed child.
const ORPHAN_INLINE: &str = r#"{
    "schema": "prose", "version": 1, "root": "Doc",
    "types": {
        "Doc":  { "kind": "map", "children": { "body": "Sect" } },
        "Sect": { "kind": "xml", "tag": "section", "children": { "Para": {} }, "repair": { "orphanInline": "Para" } },
        "Para": { "kind": "xml", "tag": "p", "children": { "Span": {} } },
        "Span": { "kind": "text" }
    }
}"#;

#[test]
fn loose_inline_text_under_an_orphan_inline_type_is_not_dropped() {
    // Sect allows only Para children and declares repair.orphanInline, so a bare
    // text child is an orphan to be wrapped (5c-ii), not a disallowed child — it
    // must not be reported/dropped here, or the wrap loses the content.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"section")
            .children()
            .insert_text(0)
            .insert(0, "hello");
    });
    let v = validate(&d, &schema(ORPHAN_INLINE));
    assert!(
        !v.iter().any(|x| x.kind == ViolationKind::DisallowedChild),
        "orphan inline text is not a disallowed child, got {v:?}"
    );
    assert!(
        repairs(&d, &schema(ORPHAN_INLINE))
            .iter()
            .all(|r| r.kind != RepairKind::Dropped),
        "orphan inline text is not dropped"
    );
}

/// The id of the loose text child at index 0 of the section in slot "body".
fn orphan_text_id(d: &Document) -> ElementId {
    match d.get(b"body") {
        Some(Element::XmlElement(x)) => {
            let c = x.borrow().children();
            let child = c.borrow().get(0);
            match child {
                Some(Element::Text(t)) => t.borrow().id(),
                _ => panic!("no text child"),
            }
        }
        _ => panic!("no section"),
    }
}

#[test]
fn an_orphan_inline_text_is_reported() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"section")
            .children()
            .insert_text(0)
            .insert(0, "hello");
    });
    let v = validate(&d, &schema(ORPHAN_INLINE));
    assert!(has(
        &v,
        &[key("body"), Step::Index(0)],
        &ViolationKind::OrphanInline {
            block: "Para".to_string(),
        },
    ));
}

#[test]
fn an_orphan_inline_text_reads_wrapped_in_the_derived_block() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"section")
            .children()
            .insert_text(0)
            .insert(0, "hello");
    });
    // The wrapper id derives from the orphan's own element id, so a later op can
    // target it and every replica synthesizes the same one.
    let want = ElementId::derive(orphan_text_id(&d), b"Para", ElementKind::XmlElement);
    let r = repairs(&d, &schema(ORPHAN_INLINE));
    assert!(r.contains(&Repair {
        path: vec![key("body"), Step::Index(0)],
        kind: RepairKind::Wrapped {
            block: "Para".to_string(),
            id: want,
        },
    }));
}

#[test]
fn the_orphan_wrapper_id_is_deterministic_across_replicas() {
    let mut a = Document::new(cid(1));
    let ops = a.transact(|tx| {
        tx.xml_element(b"body", b"section")
            .children()
            .insert_text(0)
            .insert(0, "hello");
    });
    let mut b = Document::new(cid(2));
    for op in &ops {
        b.apply(op);
    }
    assert_eq!(
        repairs(&a, &schema(ORPHAN_INLINE)),
        repairs(&b, &schema(ORPHAN_INLINE))
    );
}

#[test]
fn a_disallowed_element_child_under_an_orphan_inline_type_still_drops() {
    // The orphan-inline carve-out is for inline *text* only; a disallowed element
    // child (a <b> where only Para is allowed) is still a disallowed child.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"section")
            .children()
            .insert_element(0, b"b");
    });
    let v = validate(&d, &schema(ORPHAN_INLINE));
    assert!(has(
        &v,
        &[key("body"), Step::Index(0)],
        &ViolationKind::DisallowedChild
    ));
}

// --- repair: disallowed children ---

#[test]
fn a_disallowed_child_reads_dropped() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"p")
            .children()
            .insert_element(0, b"b");
    });
    let r = repairs(&d, &schema(FLAT));
    assert!(r.contains(&Repair {
        path: vec![key("body"), Step::Index(0)],
        kind: RepairKind::Dropped,
    }));
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
        "Article": { "kind": "fragment", "children": { "Para": {} } },
        "Para":    { "kind": "xml", "tag": "p", "children": { "Span": {} }, "attrs": { "align": "Align" } },
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

// --- validate + repair: per-type cardinality (exclusive collision) ---

// A schema whose Body element caps Heading (tag "h1") at one instance via a
// per-child-type `max`, while Para (tag "p") is unbounded. A second Heading is an
// exclusive collision: the lamport-newest excess drops, keeping the oldest.
const EXCLUSIVE: &str = r#"{
    "schema": "prose", "version": 1, "root": "Doc",
    "types": {
        "Doc":     { "kind": "map", "children": { "body": "Body" } },
        "Body":    { "kind": "xml", "tag": "body", "children": { "Heading": { "max": 1 }, "Para": {} } },
        "Heading": { "kind": "xml", "tag": "h1", "children": { "Span": {} } },
        "Para":    { "kind": "xml", "tag": "p", "children": { "Span": {} } },
        "Span":    { "kind": "text" }
    }
}"#;

#[test]
fn a_single_child_of_an_exclusive_type_conforms() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"body")
            .children()
            .insert_element(0, b"h1");
    });
    assert!(validate(&d, &schema(EXCLUSIVE)).is_empty());
}

#[test]
fn a_second_child_of_an_exclusive_type_is_reported() {
    // Body caps Heading at one; the second-inserted h1 (index 1) has the newer
    // stamp, so it is the excess while the lamport-oldest (index 0) survives.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut body = tx.xml_element(b"body", b"body");
        let mut kids = body.children();
        kids.insert_element(0, b"h1");
        kids.insert_element(1, b"h1");
    });
    let v = validate(&d, &schema(EXCLUSIVE));
    assert!(has(
        &v,
        &[key("body"), Step::Index(1)],
        &ViolationKind::ExcessChild { max: 1 }
    ));
    assert!(!has(
        &v,
        &[key("body"), Step::Index(0)],
        &ViolationKind::ExcessChild { max: 1 }
    ));
}

#[test]
fn the_dropped_excess_is_the_lamport_newest_not_the_last_in_sequence() {
    // Insert the newer heading at the front so sequence order and stamp order
    // disagree: index 0 is the newer stamp, index 1 the older. Keep-oldest drops
    // index 0, proving the drop is by stamp, not sequence position.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut body = tx.xml_element(b"body", b"body");
        let mut kids = body.children();
        kids.insert_element(0, b"h1");
        kids.insert_element(0, b"h1");
    });
    let v = validate(&d, &schema(EXCLUSIVE));
    assert!(has(
        &v,
        &[key("body"), Step::Index(0)],
        &ViolationKind::ExcessChild { max: 1 }
    ));
    assert!(!has(
        &v,
        &[key("body"), Step::Index(1)],
        &ViolationKind::ExcessChild { max: 1 }
    ));
}

#[test]
fn an_unbounded_child_type_never_excesses() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut body = tx.xml_element(b"body", b"body");
        let mut kids = body.children();
        kids.insert_element(0, b"p");
        kids.insert_element(1, b"p");
        kids.insert_element(2, b"p");
    });
    let v = validate(&d, &schema(EXCLUSIVE));
    assert!(
        v.iter()
            .all(|x| !matches!(x.kind, ViolationKind::ExcessChild { .. })),
        "unbounded Para never excesses, got {v:?}"
    );
}

#[test]
fn all_but_the_oldest_drop_when_the_cap_is_one() {
    // Three headings under a max-1 cap: the two newest drop in sequence order, the
    // lamport-oldest (index 0) survives.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut body = tx.xml_element(b"body", b"body");
        let mut kids = body.children();
        kids.insert_element(0, b"h1");
        kids.insert_element(1, b"h1");
        kids.insert_element(2, b"h1");
    });
    let v = validate(&d, &schema(EXCLUSIVE));
    let excess: Vec<&Vec<Step>> = v
        .iter()
        .filter(|x| matches!(x.kind, ViolationKind::ExcessChild { .. }))
        .map(|x| &x.path)
        .collect();
    assert_eq!(
        excess,
        vec![
            &vec![key("body"), Step::Index(1)],
            &vec![key("body"), Step::Index(2)],
        ],
        "the two newest of three drop, the oldest survives"
    );
}

#[test]
fn an_excess_child_reads_dropped() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut body = tx.xml_element(b"body", b"body");
        let mut kids = body.children();
        kids.insert_element(0, b"h1");
        kids.insert_element(1, b"h1");
    });
    let r = repairs(&d, &schema(EXCLUSIVE));
    assert!(r.contains(&Repair {
        path: vec![key("body"), Step::Index(1)],
        kind: RepairKind::Dropped,
    }));
}

// A cap of zero: the type is declared allowed but permits no instances, so every
// child of it drops.
const NO_HEADINGS: &str = r#"{
    "schema": "prose", "version": 1, "root": "Doc",
    "types": {
        "Doc":     { "kind": "map", "children": { "body": "Body" } },
        "Body":    { "kind": "xml", "tag": "body", "children": { "Heading": { "max": 0 }, "Para": {} } },
        "Heading": { "kind": "xml", "tag": "h1", "children": {} },
        "Para":    { "kind": "xml", "tag": "p", "children": {} }
    }
}"#;

#[test]
fn a_zero_cap_drops_every_child_of_that_type() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.xml_element(b"body", b"body")
            .children()
            .insert_element(0, b"h1");
    });
    let v = validate(&d, &schema(NO_HEADINGS));
    assert!(has(
        &v,
        &[key("body"), Step::Index(0)],
        &ViolationKind::ExcessChild { max: 0 }
    ));
    let r = repairs(&d, &schema(NO_HEADINGS));
    assert!(r.contains(&Repair {
        path: vec![key("body"), Step::Index(0)],
        kind: RepairKind::Dropped,
    }));
}

#[test]
fn two_replicas_produce_the_same_exclusive_violations() {
    let mut a = Document::new(cid(1));
    let ops = a.transact(|tx| {
        let mut body = tx.xml_element(b"body", b"body");
        let mut kids = body.children();
        kids.insert_element(0, b"h1");
        kids.insert_element(1, b"h1");
    });
    let mut b = Document::new(cid(2));
    for op in &ops {
        b.apply(op);
    }
    assert_eq!(
        validate(&a, &schema(EXCLUSIVE)),
        validate(&b, &schema(EXCLUSIVE))
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
