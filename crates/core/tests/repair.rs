//! Read-time invariant repair — the normalized value a schema-conformant read
//! returns over merged state, without minting an op.
//!
//! [`repairs`] reports, per element that a raw read would return non-conformant,
//! how to read it repaired: a register/counter integer clamped into its bounds, or
//! a list/text truncated to its `max` by dropping the lamport-newest excess. The
//! repair is a deterministic pure function of merged state — the drop-newest order
//! comes from the stamps already in state (total-ordered by `(lamport, client)`),
//! never the local clock — so replicas that merged the same ops read identically.
//! Repair never mutates: the stored ops are untouched.

mod common;

use common::cid;
use crdtsync_core::doc::Document;
use crdtsync_core::repair::{repairs, Repair, RepairKind};
use crdtsync_core::schema::Schema;
use crdtsync_core::validate::Step;
use crdtsync_core::Scalar;

const SCHEMA: &str = r#"{
    "schema": "notes", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "title": "Title", "body": "Body", "tags": "Tags", "hits": "Hits" } },
        "Title": { "kind": "register", "min": 0, "max": 280 },
        "Body":  { "kind": "text", "max": 5 },
        "Tags":  { "kind": "list", "items": "Title", "max": 2 },
        "Hits":  { "kind": "counter", "min": 0, "max": 100 }
    }
}"#;

fn schema() -> Schema {
    Schema::parse(SCHEMA).expect("schema parses")
}

fn key(s: &str) -> Step {
    Step::Key(s.as_bytes().to_vec())
}

// --- conforming ---

#[test]
fn a_conforming_document_needs_no_repair() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.register(b"title", Scalar::Int(42));
        tx.text(b"body").insert(0, "hi");
        tx.inc(b"hits", 10);
    });
    assert!(repairs(&d, &schema()).is_empty());
}

// --- scalar / counter clamp ---

#[test]
fn a_register_above_its_max_reads_clamped_to_the_max() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.register(b"title", Scalar::Int(999)));
    assert_eq!(
        repairs(&d, &schema()),
        vec![Repair {
            path: vec![key("title")],
            kind: RepairKind::Clamped { value: 280 },
        }],
    );
}

#[test]
fn a_register_below_its_min_reads_clamped_to_the_min() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.register(b"title", Scalar::Int(-5)));
    assert_eq!(
        repairs(&d, &schema()),
        vec![Repair {
            path: vec![key("title")],
            kind: RepairKind::Clamped { value: 0 },
        }],
    );
}

#[test]
fn a_counter_above_its_max_reads_clamped_to_the_max() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.inc(b"hits", 250));
    assert_eq!(
        repairs(&d, &schema()),
        vec![Repair {
            path: vec![key("hits")],
            kind: RepairKind::Clamped { value: 100 },
        }],
    );
}

#[test]
fn a_counter_below_its_min_reads_clamped_to_the_min() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.dec(b"hits", 5));
    assert_eq!(
        repairs(&d, &schema()),
        vec![Repair {
            path: vec![key("hits")],
            kind: RepairKind::Clamped { value: 0 },
        }],
    );
}

// --- sequence drop-newest ---

#[test]
fn a_list_over_its_max_drops_the_lamport_newest_appended_item() {
    // Append v1,v2,v3 (increasing lamport) → the last is newest and dropped.
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut l = tx.list(b"tags");
        l.insert(0, Scalar::Int(1));
        l.insert(1, Scalar::Int(2));
        l.insert(2, Scalar::Int(3));
    });
    assert_eq!(
        repairs(&d, &schema()),
        vec![Repair {
            path: vec![key("tags")],
            kind: RepairKind::Truncated { keep: vec![0, 1] },
        }],
    );
}

#[test]
fn a_list_drops_by_lamport_not_by_sequence_position() {
    // Sequence order [v2, v3, v1] but lamport order v1 < v2 < v3, so the newest
    // (v3, at sequence index 1) is dropped — not the sequence-last item (v1).
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        let mut l = tx.list(b"tags");
        l.insert(0, Scalar::Int(1)); // v1, lamport L1
        l.insert(0, Scalar::Int(2)); // v2 prepended, L2 → [v2, v1]
        l.insert(1, Scalar::Int(3)); // v3, L3 → [v2, v3, v1]
    });
    assert_eq!(
        repairs(&d, &schema()),
        vec![Repair {
            path: vec![key("tags")],
            kind: RepairKind::Truncated { keep: vec![0, 2] },
        }],
    );
}

#[test]
fn a_text_over_its_max_drops_the_lamport_newest_codepoints() {
    // "hello!" is 6 codepoints inserted as one run (increasing char ids), max 5 →
    // the newest codepoint ('!') is dropped, keeping the first five.
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.text(b"body").insert(0, "hello!"));
    assert_eq!(
        repairs(&d, &schema()),
        vec![Repair {
            path: vec![key("body")],
            kind: RepairKind::Truncated {
                keep: vec![0, 1, 2, 3, 4],
            },
        }],
    );
}

// --- multiple + determinism ---

#[test]
fn repairs_are_reported_in_deterministic_tree_order() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| {
        tx.inc(b"hits", 500); // above max
        tx.register(b"title", Scalar::Int(-1)); // below min
    });
    // Sorted keys: "hits" < "title".
    assert_eq!(
        repairs(&d, &schema()),
        vec![
            Repair {
                path: vec![key("hits")],
                kind: RepairKind::Clamped { value: 100 },
            },
            Repair {
                path: vec![key("title")],
                kind: RepairKind::Clamped { value: 0 },
            },
        ],
    );
}

#[test]
fn two_replicas_that_merged_the_same_ops_repair_identically() {
    let mut a = Document::new(cid(1));
    let ops = a.transact(|tx| {
        tx.register(b"title", Scalar::Int(999));
        tx.inc(b"hits", 500);
        let mut l = tx.list(b"tags");
        l.insert(0, Scalar::Int(1));
        l.insert(1, Scalar::Int(2));
        l.insert(2, Scalar::Int(3));
    });
    let mut b = Document::new(cid(2));
    for op in &ops {
        b.apply(op);
    }
    let s = schema();
    assert_eq!(repairs(&a, &s), repairs(&b, &s));
    assert!(
        !repairs(&a, &s).is_empty(),
        "the shared state does need repair"
    );
}
