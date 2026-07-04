//! `onRepaired` — the observation of which locations a bound schema has newly
//! come to repair over merged state.
//!
//! A schema is an opt-in runtime binding on the document. With one bound,
//! [`take_repairs`](Document::take_repairs) reports the located paths whose
//! repaired reading has changed against it since the last call: a location that
//! comes to need a repair, or a standing one whose reading changed (a re-clamp to
//! the other bound, a different surviving item). It reports *locations*, not
//! values — the repaired value is produced by a read
//! ([`repairs`](crdtsync_core::repair::repairs)) — so a consumer always reads the
//! fresh reading. Observation is of settled state: a violation that appears and
//! resolves between two calls is never reported, an open atomic transaction's
//! transient sub-states are not observed, and with no schema bound nothing is.

mod common;

use common::cid;
use crdtsync_core::doc::Document;
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

/// The single-element path `[key(s)]`, the located form `take_repairs` reports.
fn at(s: &str) -> Vec<Step> {
    vec![key(s)]
}

// --- no schema bound ---

#[test]
fn a_document_with_no_schema_reports_nothing() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.register(b"title", Scalar::Int(999)));
    assert!(d.take_repairs().is_empty());
}

// --- local edits ---

#[test]
fn a_conforming_local_edit_reports_nothing() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema());
    d.transact(|tx| tx.register(b"title", Scalar::Int(42)));
    assert!(d.take_repairs().is_empty());
}

#[test]
fn a_local_edit_that_lands_an_out_of_bounds_register_reports_its_path() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema());
    d.transact(|tx| tx.register(b"title", Scalar::Int(999)));
    assert_eq!(d.take_repairs(), vec![at("title")]);
}

#[test]
fn a_local_edit_that_overflows_a_sequence_reports_its_path() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema());
    d.transact(|tx| {
        let mut l = tx.list(b"tags");
        l.insert(0, Scalar::Int(1));
        l.insert(1, Scalar::Int(2));
        l.insert(2, Scalar::Int(3));
    });
    assert_eq!(d.take_repairs(), vec![at("tags")]);
}

// --- remote apply ---

#[test]
fn a_remote_op_that_lands_a_violation_reports_it_on_apply() {
    let mut author = Document::new(cid(1));
    let ops = author.transact(|tx| tx.inc(b"hits", 250));

    let mut d = Document::new(cid(2));
    d.set_schema(schema());
    for op in &ops {
        d.apply(op);
    }
    assert_eq!(d.take_repairs(), vec![at("hits")]);
}

// --- reading-change semantics ---

#[test]
fn a_clamp_that_moves_to_the_other_bound_surfaces_again() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema());
    d.transact(|tx| tx.register(b"title", Scalar::Int(999))); // above max
    assert_eq!(d.take_repairs(), vec![at("title")]);
    // Still out of bounds, now below min: the repaired reading changed (0, not
    // 280), so it surfaces again.
    d.transact(|tx| tx.register(b"title", Scalar::Int(-5)));
    assert_eq!(d.take_repairs(), vec![at("title")]);
}

#[test]
fn a_truncation_whose_survivors_only_shift_index_does_not_resurface() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema());
    d.transact(|tx| {
        let mut l = tx.list(b"tags");
        l.insert(0, Scalar::Int(1));
        l.insert(1, Scalar::Int(2));
        l.insert(2, Scalar::Int(3));
    });
    assert_eq!(d.take_repairs(), vec![at("tags")]);
    // Prepend a newer item: still over max, but the same two items survive (their
    // indices merely shift). The repaired reading is unchanged, and the consumer
    // observes the sequence edit through a normal read, so nothing resurfaces.
    d.transact(|tx| tx.list(b"tags").insert(0, Scalar::Int(9)));
    assert!(d.take_repairs().is_empty());
}

#[test]
fn a_truncation_whose_surviving_item_changes_surfaces_again() {
    // A max-1 list keeps only its oldest item; deleting that item leaves a
    // different item surviving at the same index, so the reading changed.
    const MAX1: &str = r#"{ "schema": "x", "version": 1, "root": "R", "types": {
        "R": { "kind": "map", "children": { "xs": "L" } },
        "L": { "kind": "list", "items": "V", "max": 1 },
        "V": { "kind": "register", "min": 0, "max": 9 } } }"#;
    let mut d = Document::new(cid(1));
    d.set_schema(Schema::parse(MAX1).expect("schema parses"));
    d.transact(|tx| {
        let mut l = tx.list(b"xs");
        l.insert(0, Scalar::Int(1));
        l.insert(1, Scalar::Int(2));
        l.insert(2, Scalar::Int(3));
    });
    assert_eq!(d.take_repairs(), vec![at("xs")]);
    // Delete the sole survivor: the next-oldest now survives at the same index.
    d.transact(|tx| tx.list(b"xs").delete(0));
    assert_eq!(d.take_repairs(), vec![at("xs")]);
}

// --- draining semantics ---

#[test]
fn taking_repairs_drains_them() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema());
    d.transact(|tx| tx.register(b"title", Scalar::Int(999)));
    assert!(!d.take_repairs().is_empty());
    assert!(d.take_repairs().is_empty(), "a second drain is empty");
}

#[test]
fn a_still_present_violation_is_not_re_reported_by_an_unrelated_later_edit() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema());
    d.transact(|tx| tx.register(b"title", Scalar::Int(999)));
    assert!(!d.take_repairs().is_empty());
    // A second, conforming edit settles while `title` is still out of bounds; the
    // standing repair was already reported and does not report again.
    d.transact(|tx| tx.inc(b"hits", 10));
    assert!(d.take_repairs().is_empty());
}

#[test]
fn a_delivery_that_transiently_overflows_then_settles_conformant_reports_nothing() {
    // One delivery pushes a list over its max and then back under before it
    // settles. Applied op-by-op the intermediate state violates, but observation
    // reads the settled state, so nothing surfaces.
    let mut author = Document::new(cid(1));
    let ops = author.transact(|tx| {
        let mut l = tx.list(b"tags");
        l.insert(0, Scalar::Int(1));
        l.insert(1, Scalar::Int(2));
        l.insert(2, Scalar::Int(3)); // transiently over max 2
        l.delete(2); // back to 2 — conformant
    });

    let mut d = Document::new(cid(2));
    d.set_schema(schema());
    for op in &ops {
        d.apply(op);
    }
    assert!(d.take_repairs().is_empty());
}

// --- binding to existing state ---

#[test]
fn binding_a_schema_to_already_violating_state_reports_nothing_until_a_new_op() {
    let mut d = Document::new(cid(1));
    d.transact(|tx| tx.register(b"title", Scalar::Int(999))); // violates, pre-binding
    d.set_schema(schema());
    // The baseline is not re-litigated.
    assert!(d.take_repairs().is_empty());
    // A fresh violation does surface.
    d.transact(|tx| tx.inc(b"hits", 250));
    assert_eq!(d.take_repairs(), vec![at("hits")]);
}

#[test]
fn rebinding_a_schema_reseeds_the_baseline() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema());
    d.transact(|tx| tx.register(b"title", Scalar::Int(999)));
    // Rebind without draining: the standing violation is baselined afresh, so it
    // does not surface under the new binding.
    d.set_schema(schema());
    assert!(d.take_repairs().is_empty());
}

#[test]
fn a_reintroduced_violation_surfaces_again() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema());
    d.transact(|tx| tx.register(b"title", Scalar::Int(999)));
    assert!(!d.take_repairs().is_empty());
    // Bring it back in bounds — no report for the fix — then out again.
    d.transact(|tx| tx.register(b"title", Scalar::Int(50)));
    assert!(d.take_repairs().is_empty());
    d.transact(|tx| tx.register(b"title", Scalar::Int(999)));
    assert_eq!(d.take_repairs(), vec![at("title")]);
}

// --- atomic transactions settle once, at commit ---

#[test]
fn polling_mid_atomic_transaction_reports_nothing_and_does_not_baseline_the_transient() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema());
    d.begin_atomic();
    d.transact(|tx| tx.register(b"title", Scalar::Int(999))); // transient violation
    assert!(
        d.take_repairs().is_empty(),
        "an open atomic transaction's transient state is not observed"
    );
    d.transact(|tx| tx.register(b"title", Scalar::Int(50))); // resolved before commit
    d.commit_atomic();
    assert!(d.take_repairs().is_empty(), "the committed state conforms");
}

#[test]
fn an_atomic_transaction_that_commits_a_violation_surfaces_once() {
    let mut d = Document::new(cid(1));
    d.set_schema(schema());
    d.begin_atomic();
    d.transact(|tx| tx.register(b"title", Scalar::Int(999)));
    d.transact(|tx| tx.inc(b"hits", 250));
    d.commit_atomic();
    // Sorted tree order: "hits" < "title".
    assert_eq!(d.take_repairs(), vec![at("hits"), at("title")]);
}
