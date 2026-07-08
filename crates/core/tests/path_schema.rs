//! The path façade's schema-bind + repair-observation surface.
//!
//! A binding crosses schema JSON as bytes and the `onRepaired` signal as located
//! paths, so the façade parses schema bytes (total — malformed never panics, it
//! fails), binds them to the document, and reports `take_repairs` locations in the
//! path encoding a binding forwards. It reports *locations*, not values: the
//! repaired value is produced by a read (`repair::repairs`), while a normal
//! `path::get_*` returns the raw stored value unnormalized.

use crdtsync_core::doc::Document;
use crdtsync_core::path;
use crdtsync_core::repair::{repairs, RepairKind};
use crdtsync_core::schema::Schema;
use crdtsync_core::validate::Step;

mod common;
use common::cid;

const SCHEMA: &str = r#"{
    "schema": "notes", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "title": "Title", "tags": "Tags", "hits": "Hits" } },
        "Title": { "kind": "register", "min": 0, "max": 280 },
        "Tags":  { "kind": "list", "items": "Title", "max": 2 },
        "Hits":  { "kind": "counter", "min": 0, "max": 100 }
    }
}"#;

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn p(keys: &[&str]) -> Vec<u8> {
    let keys: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
    path::encode_path(&keys)
}

fn key(s: &str) -> Step {
    Step::Key(s.as_bytes().to_vec())
}

// --- binding a schema by bytes ---

#[test]
fn binding_a_valid_schema_by_bytes_succeeds() {
    let mut d = doc(1);
    assert!(path::set_schema(&mut d, SCHEMA.as_bytes()));
}

#[test]
fn binding_malformed_schema_bytes_fails_without_panicking() {
    let mut d = doc(1);
    // Not JSON at all, a well-formed JSON that is not a schema, and non-UTF-8
    // bytes each fail cleanly rather than panic.
    assert!(!path::set_schema(&mut d, b"not json {"));
    assert!(!path::set_schema(&mut d, br#"{ "schema": "x" }"#));
    assert!(!path::set_schema(&mut d, &[0xff, 0xfe, 0x00]));
}

#[test]
fn a_failed_bind_leaves_no_schema_bound() {
    let mut d = doc(1);
    assert!(!path::set_schema(&mut d, b"garbage"));
    // No schema bound, so an out-of-range write reports nothing.
    path::register_int(&mut d, &p(&["title"]), 999);
    assert!(path::take_repairs(&mut d).is_empty());
}

// --- take_repairs read model ---

#[test]
fn no_schema_bound_reports_nothing() {
    let mut d = doc(1);
    path::register_int(&mut d, &p(&["title"]), 999);
    assert!(path::take_repairs(&mut d).is_empty());
}

#[test]
fn a_conforming_edit_reports_nothing() {
    let mut d = doc(1);
    assert!(path::set_schema(&mut d, SCHEMA.as_bytes()));
    path::register_int(&mut d, &p(&["title"]), 42);
    assert!(path::take_repairs(&mut d).is_empty());
}

#[test]
fn an_out_of_range_write_reports_its_path_once() {
    let mut d = doc(1);
    assert!(path::set_schema(&mut d, SCHEMA.as_bytes()));
    path::register_int(&mut d, &p(&["title"]), 999);
    // Reported once, as the located path in the façade's path encoding.
    let reported = path::take_repairs(&mut d);
    assert_eq!(reported, vec![path::encode_repair_path(&[key("title")])]);
    // Settle-point contract: a second drain with no new change is empty.
    assert!(path::take_repairs(&mut d).is_empty());
}

#[test]
fn a_reported_path_round_trips_the_facade_encoding() {
    let mut d = doc(1);
    assert!(path::set_schema(&mut d, SCHEMA.as_bytes()));
    path::register_int(&mut d, &p(&["title"]), 999);
    let reported = path::take_repairs(&mut d);
    assert_eq!(reported.len(), 1);
    assert_eq!(
        path::parse_repair_path(&reported[0]),
        Some(vec![key("title")])
    );
}

// --- path encoding round-trips both keys and indices ---

#[test]
fn a_repair_path_round_trips_keys_and_indices() {
    let steps = vec![key("body"), Step::Index(3), key("href")];
    let bytes = path::encode_repair_path(&steps);
    assert_eq!(path::parse_repair_path(&bytes), Some(steps));
}

#[test]
fn parsing_a_malformed_repair_path_is_total() {
    // An unknown step tag, a key length past the end, and a truncated index all
    // decode to None rather than panic — the boundary never trusts its framing.
    assert_eq!(path::parse_repair_path(&[0x7f]), None);
    assert_eq!(
        path::parse_repair_path(&[0x00, 0xff, 0xff, 0xff, 0xff]),
        None
    );
    assert_eq!(path::parse_repair_path(&[0x01, 0x00, 0x00]), None);
}

// --- consumption model: locations are reported, the value is read via repairs ---

#[test]
fn the_repaired_value_is_read_via_repairs_not_a_raw_get() {
    let mut d = doc(1);
    assert!(path::set_schema(&mut d, SCHEMA.as_bytes()));
    path::register_int(&mut d, &p(&["title"]), 999);
    assert_eq!(
        path::take_repairs(&mut d),
        vec![path::encode_repair_path(&[key("title")])]
    );

    // A normal path read returns the raw, unnormalized value.
    assert_eq!(path::get_int(&d, &p(&["title"])), Some(999));

    // The normalized reading is produced by the repair read, clamped to the max.
    let schema = Schema::parse(SCHEMA).expect("schema parses");
    let rs = repairs(&d, &schema);
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0].path, vec![key("title")]);
    assert_eq!(rs[0].kind, RepairKind::Clamped { value: 280 });
}

// --- settle-point contract preserved through the façade ---

#[test]
fn a_transiently_resolved_violation_reports_nothing() {
    let mut author = doc(1);
    let ops = author.transact(|tx| {
        let mut l = tx.list(b"tags");
        l.insert(0, crdtsync_core::Scalar::Int(1));
        l.insert(1, crdtsync_core::Scalar::Int(2));
        l.insert(2, crdtsync_core::Scalar::Int(3)); // transiently over max 2
        l.delete(2); // back under before it settles
    });
    let mut d = doc(2);
    assert!(path::set_schema(&mut d, SCHEMA.as_bytes()));
    for op in &ops {
        d.apply(op);
    }
    assert!(path::take_repairs(&mut d).is_empty());
}
