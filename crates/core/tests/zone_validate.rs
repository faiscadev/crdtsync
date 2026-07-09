//! Cross-zone anchor rejection — the read-time half of the cross-zone rule.
//!
//! A zone is a contiguous subtree rooted at its schema-declared path; the per-zone
//! clocks never order across zones, so a ranged annotation whose two endpoints sit
//! in different zones is not admissible. [`validate`] flags such a range as a
//! `CrossZoneAnchor`, and [`repairs`] reads it absent (a `Dropped`) — the same
//! normalization a disallowed child gets. Detection is deterministic: the ranged
//! set is id-sorted, and zone resolution is total, so replicas that merged the same
//! ops report the identical violation set.

mod common;

use common::cid;
use crdtsync_core::anchor::RelativePosition;
use crdtsync_core::doc::Document;
use crdtsync_core::elementid::{ElementId, ElementKind};
use crdtsync_core::ranged::RangeAnchor;
use crdtsync_core::repair::{repairs, Repair, RepairKind};
use crdtsync_core::schema::Schema;
use crdtsync_core::validate::{validate, Violation, ViolationKind};
use crdtsync_core::{Op, Scalar};

/// A schema whose root map holds four text slots, two of them zoned: `x` in zone
/// `za`, `y` in zone `zb`, and `z` / `w` in the unzoned default region.
const ZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "x": "Body", "y": "Body", "z": "Body", "w": "Body" } },
        "Body": { "kind": "text" }
    },
    "zones": { "za": "/x", "zb": "/y" }
}"#;

/// The same slot layout with no `zones` block — every location is unzoned.
const UNZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "x": "Body", "y": "Body", "z": "Body", "w": "Body" } },
        "Body": { "kind": "text" }
    }
}"#;

fn zoned() -> Schema {
    Schema::parse(ZONED).expect("schema parses")
}

fn unzoned() -> Schema {
    Schema::parse(UNZONED).expect("schema parses")
}

/// The id a Text under root slot `key` derives to — the sequence an anchor names.
fn text_id(d: &Document, key: &[u8]) -> ElementId {
    ElementId::derive(d.root_id(), key, ElementKind::Text)
}

fn at(seq: ElementId, pos: RelativePosition) -> RangeAnchor {
    RangeAnchor { seq, pos }
}

/// A doc with the four zoned/unzoned text slots populated.
fn doc_with_texts() -> Document {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.text(b"x").insert(0, "abc");
        tx.text(b"y").insert(0, "def");
        tx.text(b"z").insert(0, "ghi");
        tx.text(b"w").insert(0, "jkl");
    });
    d
}

// --- same zone: never flagged ---

#[test]
fn a_mark_within_one_zone_is_not_a_violation() {
    let mut d = doc_with_texts();
    let x = text_id(&d, b"x");
    d.transact(|tx| {
        tx.ranged().mark(
            b"bold",
            at(x, RelativePosition::Start),
            at(x, RelativePosition::End),
            Scalar::Bool(true),
        );
    });
    assert!(validate(&d, &zoned()).is_empty());
}

#[test]
fn two_unzoned_endpoints_do_not_cross() {
    // Both endpoints in the unzoned default region — no zone boundary between them.
    let mut d = doc_with_texts();
    let z = text_id(&d, b"z");
    let w = text_id(&d, b"w");
    d.transact(|tx| {
        tx.ranged().create(
            at(z, RelativePosition::Start),
            at(w, RelativePosition::End),
            Scalar::Bool(true),
        );
    });
    assert!(validate(&d, &zoned()).is_empty());
}

// --- cross zone: flagged, repaired absent ---

#[test]
fn a_range_straddling_two_zones_is_one_cross_zone_violation() {
    let mut d = doc_with_texts();
    let x = text_id(&d, b"x");
    let y = text_id(&d, b"y");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        rid = tx.ranged().create(
            at(x, RelativePosition::Start),
            at(y, RelativePosition::End),
            Scalar::Bool(true),
        );
    });
    assert_eq!(
        validate(&d, &zoned()),
        vec![Violation {
            path: Vec::new(),
            kind: ViolationKind::CrossZoneAnchor { id: rid },
        }],
    );
}

#[test]
fn a_zoned_and_an_unzoned_endpoint_cross() {
    // The unzoned default region is distinct from any zone, so a range from a zone
    // to it straddles the boundary just as two distinct zones do.
    let mut d = doc_with_texts();
    let x = text_id(&d, b"x");
    let z = text_id(&d, b"z");
    let mut rid = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        rid = tx.ranged().create(
            at(x, RelativePosition::Start),
            at(z, RelativePosition::End),
            Scalar::Bool(true),
        );
    });
    assert_eq!(
        validate(&d, &zoned()),
        vec![Violation {
            path: Vec::new(),
            kind: ViolationKind::CrossZoneAnchor { id: rid },
        }],
    );
}

#[test]
fn a_cross_zone_range_reads_absent() {
    let mut d = doc_with_texts();
    let x = text_id(&d, b"x");
    let y = text_id(&d, b"y");
    d.transact(|tx| {
        tx.ranged().create(
            at(x, RelativePosition::Start),
            at(y, RelativePosition::End),
            Scalar::Bool(true),
        );
    });
    assert_eq!(
        repairs(&d, &zoned()),
        vec![Repair {
            path: Vec::new(),
            kind: RepairKind::Dropped,
        }],
    );
}

// --- no zones declared: baseline regression ---

#[test]
fn no_zones_declared_never_flags_a_cross_zone_anchor() {
    // With no `zones` block every location is unzoned, so no pair of endpoints can
    // cross — the same range that straddles zones above is clean here.
    let mut d = doc_with_texts();
    let x = text_id(&d, b"x");
    let y = text_id(&d, b"y");
    d.transact(|tx| {
        tx.ranged().create(
            at(x, RelativePosition::Start),
            at(y, RelativePosition::End),
            Scalar::Bool(true),
        );
    });
    assert!(validate(&d, &unzoned()).is_empty());
    assert!(repairs(&d, &unzoned()).is_empty());
}

// --- determinism ---

#[test]
fn two_replicas_report_the_identical_violation_ordering() {
    // Two cross-zone ranges on one replica; a second replica that merged the same
    // ops must produce the byte-identical violation set — id-sorted, deterministic.
    let mut d1 = Document::new(cid(1));
    let mut ops: Vec<Op> = d1.transact(|tx| {
        tx.text(b"x").insert(0, "abc");
        tx.text(b"y").insert(0, "def");
    });
    let x = text_id(&d1, b"x");
    let y = text_id(&d1, b"y");
    ops.extend(d1.transact(|tx| {
        tx.ranged().create(
            at(x, RelativePosition::Start),
            at(y, RelativePosition::End),
            Scalar::Bool(true),
        );
        tx.ranged().create(
            at(y, RelativePosition::Start),
            at(x, RelativePosition::End),
            Scalar::Bool(false),
        );
    }));

    let mut d2 = Document::new(cid(2));
    for op in &ops {
        d2.apply(op);
    }

    let v1 = validate(&d1, &zoned());
    let v2 = validate(&d2, &zoned());
    assert_eq!(v1.len(), 2);
    assert_eq!(v1, v2);
    // Stable across repeated evaluation of the same state.
    assert_eq!(validate(&d1, &zoned()), v1);
}
