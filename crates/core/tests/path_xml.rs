//! Path addressing over the XmlElement tree — the attrs half.
//!
//! An XmlElement lives in a map slot; its attrs are an ordinary keyed Map. The
//! path façade descends into that attrs Map transparently, so the existing
//! scalar/register/counter read+write fns address an element's attrs by naming
//! the element then the attr key — no attr-specific op. A fragment carries no
//! attrs, so a key past it does not resolve. Children/move/marks are later
//! slices; this file is create + attrs + tag.

use crdtsync_core::doc::Document;
use crdtsync_core::op::Op;
use crdtsync_core::{path, Scalar};

mod common;
use common::cid;

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn p(keys: &[&str]) -> Vec<u8> {
    let keys: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
    path::encode_path(&keys)
}

fn replay(b: &mut Document, ops: &[Op]) {
    for op in ops {
        b.apply(op);
    }
}

// --- create + tag read ---

#[test]
fn an_xml_element_reads_its_tag() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    assert_eq!(path::xml_tag(&d, &p(&["body"])), Some(b"div".to_vec()));
}

#[test]
fn a_missing_element_has_no_tag() {
    let d = doc(1);
    assert_eq!(path::xml_tag(&d, &p(&["body"])), None);
}

#[test]
fn a_fragment_has_no_tag() {
    let mut d = doc(1);
    path::xml_fragment(&mut d, &p(&["root"]));
    assert_eq!(path::xml_tag(&d, &p(&["root"])), None);
}

#[test]
fn a_plain_map_is_not_an_element() {
    let mut d = doc(1);
    path::register_int(&mut d, &p(&["m", "x"]), 1);
    assert_eq!(path::xml_tag(&d, &p(&["m"])), None);
}

#[test]
fn an_element_nests_in_a_map() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["outer", "inner"]), b"span");
    assert_eq!(
        path::xml_tag(&d, &p(&["outer", "inner"])),
        Some(b"span".to_vec())
    );
}

// --- attrs descend transparently ---

#[test]
fn an_int_attr_reads_back() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    path::register_int(&mut d, &p(&["body", "tabindex"]), 3);
    assert_eq!(path::get_int(&d, &p(&["body", "tabindex"])), Some(3));
}

#[test]
fn a_bytes_attr_reads_back() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    path::set_bytes(&mut d, &p(&["body", "class"]), b"lead");
    assert_eq!(
        path::get_bytes(&d, &p(&["body", "class"])),
        Some(b"lead".to_vec())
    );
}

#[test]
fn a_register_attr_reads_back() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    path::register(&mut d, &p(&["body", "hidden"]), Scalar::Bool(true));
    assert_eq!(
        path::get_register(&d, &p(&["body", "hidden"])),
        Some(Scalar::Bool(true))
    );
}

#[test]
fn a_counter_attr_accumulates() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    path::inc(&mut d, &p(&["body", "hits"]), 5);
    path::inc(&mut d, &p(&["body", "hits"]), 2);
    assert_eq!(path::get_counter(&d, &p(&["body", "hits"])), Some(7));
}

#[test]
fn an_attr_is_deleted() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    path::register_int(&mut d, &p(&["body", "tabindex"]), 3);
    path::delete(&mut d, &p(&["body", "tabindex"]));
    assert_eq!(path::get_int(&d, &p(&["body", "tabindex"])), None);
}

#[test]
fn an_attr_can_hold_a_nested_map() {
    // Descent through an element's attrs is uniform: an attr value can itself be
    // a nested Map, reached by continuing the path past the attr key.
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    path::register_int(&mut d, &p(&["body", "data", "level"]), 9);
    assert_eq!(path::get_int(&d, &p(&["body", "data", "level"])), Some(9));
}

#[test]
fn attrs_on_two_elements_are_independent() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["a"]), b"div");
    path::xml_element(&mut d, &p(&["b"]), b"div");
    path::register_int(&mut d, &p(&["a", "x"]), 1);
    path::register_int(&mut d, &p(&["b", "x"]), 2);
    assert_eq!(path::get_int(&d, &p(&["a", "x"])), Some(1));
    assert_eq!(path::get_int(&d, &p(&["b", "x"])), Some(2));
}

// --- a fragment has no attrs ---

#[test]
fn a_key_past_a_fragment_does_not_resolve() {
    let mut d = doc(1);
    path::xml_fragment(&mut d, &p(&["root"]));
    let ops = path::register_int(&mut d, &p(&["root", "class"]), 1);
    assert!(ops.is_empty(), "a fragment attr write emits nothing");
    assert_eq!(path::get_int(&d, &p(&["root", "class"])), None);
}

#[test]
fn a_fragment_nested_in_a_map_is_still_a_dead_end() {
    // The dead end holds one level down too: a fragment under a map has no attrs,
    // so a write past it never lands on the fragment slot.
    let mut d = doc(1);
    path::xml_fragment(&mut d, &p(&["outer", "frag"]));
    path::register_int(&mut d, &p(&["outer", "frag", "class"]), 1);
    assert_eq!(path::get_int(&d, &p(&["outer", "frag", "class"])), None);
    assert_eq!(path::xml_tag(&d, &p(&["outer", "frag"])), None);
}

// --- convergence ---

#[test]
fn an_element_and_its_attrs_converge_on_a_peer() {
    let mut a = doc(1);
    let mut ops = path::xml_element(&mut a, &p(&["body"]), b"div");
    ops.extend(path::register_int(&mut a, &p(&["body", "tabindex"]), 4));
    ops.extend(path::set_bytes(&mut a, &p(&["body", "class"]), b"lead"));

    let mut b = doc(2);
    replay(&mut b, &ops);

    assert_eq!(path::xml_tag(&b, &p(&["body"])), Some(b"div".to_vec()));
    assert_eq!(path::get_int(&b, &p(&["body", "tabindex"])), Some(4));
    assert_eq!(
        path::get_bytes(&b, &p(&["body", "class"])),
        Some(b"lead".to_vec())
    );
}

// --- non-xml paths unchanged (regression) ---

#[test]
fn a_plain_nested_map_still_resolves() {
    let mut d = doc(1);
    path::register_int(&mut d, &p(&["profile", "stats", "score"]), 7);
    assert_eq!(
        path::get_int(&d, &p(&["profile", "stats", "score"])),
        Some(7)
    );
}
