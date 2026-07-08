//! Path addressing over the XmlElement tree — the attrs half.
//!
//! An XmlElement lives in a map slot; its attrs are an ordinary keyed Map. The
//! path façade descends into that attrs Map transparently, so the existing
//! scalar/register/counter read+write fns address an element's attrs by naming
//! the element then the attr key — no attr-specific op. A fragment carries no
//! attrs, so a key past it does not resolve. An element's/fragment's children
//! are an index-addressed sequence: the path names the element, the child is an
//! index within it. Move/marks are later slices; this file is create + attrs +
//! tag + children.

use crdtsync_core::doc::Document;
use crdtsync_core::op::Op;
use crdtsync_core::{path, Element, Scalar};

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

/// The children of the top-level XML node in slot `key`, rendered in order: an
/// element as `<tag>`, a text run as its quoted string. Reads the doc directly
/// because a child has no stable path key — it is index-addressed, and the path
/// façade addresses the children sequence, not a child's contents.
fn children_of(d: &Document, key: &str) -> Vec<String> {
    let el = d.get(key.as_bytes());
    let children = match &el {
        Some(Element::XmlElement(x)) => x.borrow().children(),
        Some(Element::XmlFragment(f)) => f.borrow().children(),
        _ => return Vec::new(),
    };
    let vals = children.borrow().values();
    vals.iter()
        .map(|e| match e {
            Element::XmlElement(x) => format!("<{}>", String::from_utf8_lossy(x.borrow().tag())),
            Element::Text(t) => format!("{:?}", t.borrow().as_string()),
            _ => "?".to_string(),
        })
        .collect()
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
    // so a write past it emits nothing (no phantom ancestor map) and never lands
    // on the fragment slot — the same emptiness the shallow dead end guarantees.
    let mut d = doc(1);
    path::xml_fragment(&mut d, &p(&["outer", "frag"]));
    let ops = path::register_int(&mut d, &p(&["outer", "frag", "class"]), 1);
    assert!(ops.is_empty(), "a nested fragment attr write emits nothing");
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

// --- children: insert element ---

#[test]
fn an_element_child_is_inserted() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    let ops = path::xml_insert_element(&mut d, &p(&["body"]), 0, b"h1");
    assert!(!ops.is_empty());
    assert_eq!(path::xml_children_len(&d, &p(&["body"])), Some(1));
    assert_eq!(children_of(&d, "body"), vec!["<h1>"]);
}

#[test]
fn element_children_keep_order() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    path::xml_insert_element(&mut d, &p(&["body"]), 0, b"h1");
    path::xml_insert_element(&mut d, &p(&["body"]), 1, b"p");
    assert_eq!(path::xml_children_len(&d, &p(&["body"])), Some(2));
    assert_eq!(children_of(&d, "body"), vec!["<h1>", "<p>"]);
}

#[test]
fn a_nested_element_holds_children() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["outer", "inner"]), b"span");
    path::xml_insert_element(&mut d, &p(&["outer", "inner"]), 0, b"b");
    assert_eq!(path::xml_children_len(&d, &p(&["outer", "inner"])), Some(1));
}

// --- children: insert text ---

#[test]
fn a_text_child_is_inserted_with_its_string() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    path::xml_insert_text(&mut d, &p(&["body"]), 0, "hello");
    assert_eq!(path::xml_children_len(&d, &p(&["body"])), Some(1));
    assert_eq!(children_of(&d, "body"), vec!["\"hello\""]);
}

#[test]
fn an_empty_text_child_is_inserted() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    path::xml_insert_text(&mut d, &p(&["body"]), 0, "");
    assert_eq!(path::xml_children_len(&d, &p(&["body"])), Some(1));
    assert_eq!(children_of(&d, "body"), vec!["\"\""]);
}

#[test]
fn mixed_element_and_text_children() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    path::xml_insert_element(&mut d, &p(&["body"]), 0, b"h1");
    path::xml_insert_text(&mut d, &p(&["body"]), 1, "tail");
    assert_eq!(children_of(&d, "body"), vec!["<h1>", "\"tail\""]);
}

// --- children on a fragment ---

#[test]
fn a_fragment_holds_children() {
    let mut d = doc(1);
    path::xml_fragment(&mut d, &p(&["root"]));
    path::xml_insert_element(&mut d, &p(&["root"]), 0, b"item");
    path::xml_insert_text(&mut d, &p(&["root"]), 1, "note");
    assert_eq!(path::xml_children_len(&d, &p(&["root"])), Some(2));
    assert_eq!(children_of(&d, "root"), vec!["<item>", "\"note\""]);
}

#[test]
fn a_fresh_fragment_has_zero_children() {
    let mut d = doc(1);
    path::xml_fragment(&mut d, &p(&["root"]));
    assert_eq!(path::xml_children_len(&d, &p(&["root"])), Some(0));
}

#[test]
fn a_fresh_element_has_zero_children() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    assert_eq!(path::xml_children_len(&d, &p(&["body"])), Some(0));
}

// --- children: delete ---

#[test]
fn a_child_is_deleted() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    path::xml_insert_element(&mut d, &p(&["body"]), 0, b"h1");
    path::xml_insert_element(&mut d, &p(&["body"]), 1, b"p");
    let ops = path::xml_child_delete(&mut d, &p(&["body"]), 0);
    assert!(!ops.is_empty());
    assert_eq!(path::xml_children_len(&d, &p(&["body"])), Some(1));
    assert_eq!(children_of(&d, "body"), vec!["<p>"]);
}

#[test]
fn an_out_of_range_delete_is_inert() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["body"]), b"div");
    path::xml_insert_element(&mut d, &p(&["body"]), 0, b"h1");
    let ops = path::xml_child_delete(&mut d, &p(&["body"]), 5);
    assert!(ops.is_empty(), "an out-of-range child delete emits nothing");
    assert_eq!(path::xml_children_len(&d, &p(&["body"])), Some(1));
}

// --- non-element paths are inert ---

#[test]
fn children_len_on_a_non_element_is_none() {
    let mut d = doc(1);
    path::register_int(&mut d, &p(&["m", "x"]), 1);
    assert_eq!(path::xml_children_len(&d, &p(&["m"])), None);
    assert_eq!(path::xml_children_len(&d, &p(&["m", "x"])), None);
}

#[test]
fn children_len_on_a_missing_path_is_none() {
    let d = doc(1);
    assert_eq!(path::xml_children_len(&d, &p(&["gone"])), None);
}

#[test]
fn inserting_a_child_on_a_non_element_is_inert() {
    let mut d = doc(1);
    path::register_int(&mut d, &p(&["reg"]), 1);
    let a = path::xml_insert_element(&mut d, &p(&["reg"]), 0, b"h1");
    let b = path::xml_insert_text(&mut d, &p(&["reg"]), 0, "x");
    assert!(
        a.is_empty() && b.is_empty(),
        "a non-element child write emits nothing"
    );
    assert_eq!(path::get_int(&d, &p(&["reg"])), Some(1));
}

#[test]
fn inserting_a_child_on_a_missing_path_is_inert() {
    let mut d = doc(1);
    let ops = path::xml_insert_element(&mut d, &p(&["gone"]), 0, b"h1");
    assert!(ops.is_empty());
    assert_eq!(path::xml_children_len(&d, &p(&["gone"])), None);
}

#[test]
fn deleting_a_child_on_a_non_element_is_inert() {
    let mut d = doc(1);
    path::register_int(&mut d, &p(&["reg"]), 1);
    let ops = path::xml_child_delete(&mut d, &p(&["reg"]), 0);
    assert!(ops.is_empty());
}

// --- convergence ---

#[test]
fn children_converge_on_a_peer() {
    let mut a = doc(1);
    let mut ops = path::xml_element(&mut a, &p(&["body"]), b"div");
    ops.extend(path::xml_insert_element(&mut a, &p(&["body"]), 0, b"h1"));
    ops.extend(path::xml_insert_text(&mut a, &p(&["body"]), 1, "world"));

    let mut b = doc(2);
    replay(&mut b, &ops);

    assert_eq!(path::xml_children_len(&b, &p(&["body"])), Some(2));
    assert_eq!(children_of(&b, "body"), vec!["<h1>", "\"world\""]);
}

#[test]
fn fragment_children_converge_on_a_peer() {
    let mut a = doc(1);
    let mut ops = path::xml_fragment(&mut a, &p(&["root"]));
    ops.extend(path::xml_insert_element(&mut a, &p(&["root"]), 0, b"item"));

    let mut b = doc(2);
    replay(&mut b, &ops);

    assert_eq!(path::xml_children_len(&b, &p(&["root"])), Some(1));
    assert_eq!(children_of(&b, "root"), vec!["<item>"]);
}

#[test]
fn a_child_delete_converges_on_a_peer() {
    let mut a = doc(1);
    let mut ops = path::xml_element(&mut a, &p(&["body"]), b"div");
    ops.extend(path::xml_insert_element(&mut a, &p(&["body"]), 0, b"h1"));
    ops.extend(path::xml_insert_element(&mut a, &p(&["body"]), 1, b"p"));
    ops.extend(path::xml_child_delete(&mut a, &p(&["body"]), 0));

    let mut b = doc(2);
    replay(&mut b, &ops);

    assert_eq!(path::xml_children_len(&b, &p(&["body"])), Some(1));
    assert_eq!(children_of(&b, "body"), vec!["<p>"]);
}

// --- move: a child relocates by (parent_path, index) ---
//
// A child lives in an index-addressed sequence, so the mover is named by its
// parent path and its live index; the destination is a path-addressed XML node.
// A map-slot root / fragment is never a child, so it is unaddressable as a mover
// — and a destination is always a map-slot node, never inside a mover's subtree,
// so this surface cannot express a cycle. Both no-op conditions are structural.

#[test]
fn a_child_moves_to_another_parent() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["a"]), b"div");
    path::xml_insert_element(&mut d, &p(&["a"]), 0, b"h1");
    path::xml_insert_element(&mut d, &p(&["a"]), 1, b"p");
    path::xml_element(&mut d, &p(&["b"]), b"section");

    let ops = path::xml_move_child(&mut d, &p(&["a"]), 0, &p(&["b"]), 0);
    assert!(!ops.is_empty(), "a move should emit an op");
    assert_eq!(children_of(&d, "a"), vec!["<p>"]);
    assert_eq!(children_of(&d, "b"), vec!["<h1>"]);
}

#[test]
fn a_move_converges_on_a_peer() {
    let mut a = doc(1);
    let mut ops = path::xml_element(&mut a, &p(&["a"]), b"div");
    ops.extend(path::xml_insert_element(&mut a, &p(&["a"]), 0, b"h1"));
    ops.extend(path::xml_insert_element(&mut a, &p(&["a"]), 1, b"p"));
    ops.extend(path::xml_element(&mut a, &p(&["b"]), b"section"));
    ops.extend(path::xml_move_child(&mut a, &p(&["a"]), 0, &p(&["b"]), 0));

    let mut b = doc(2);
    replay(&mut b, &ops);

    assert_eq!(children_of(&b, "a"), children_of(&a, "a"));
    assert_eq!(children_of(&b, "b"), children_of(&a, "b"));
    assert_eq!(children_of(&b, "a"), vec!["<p>"]);
    assert_eq!(children_of(&b, "b"), vec!["<h1>"]);
}

#[test]
fn a_same_parent_reorder_changes_order() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["a"]), b"div");
    path::xml_insert_element(&mut d, &p(&["a"]), 0, b"h1");
    path::xml_insert_element(&mut d, &p(&["a"]), 1, b"p");
    path::xml_insert_element(&mut d, &p(&["a"]), 2, b"span");

    // Move h1 (index 0) to index 1 → p, h1, span. The node's own slot is
    // discounted when reading the target index, so the reorder is not off by one.
    let ops = path::xml_move_child(&mut d, &p(&["a"]), 0, &p(&["a"]), 1);
    assert!(!ops.is_empty());
    assert_eq!(children_of(&d, "a"), vec!["<p>", "<h1>", "<span>"]);
}

#[test]
fn a_reorder_converges_on_a_peer() {
    let mut a = doc(1);
    let mut ops = path::xml_element(&mut a, &p(&["a"]), b"div");
    ops.extend(path::xml_insert_element(&mut a, &p(&["a"]), 0, b"h1"));
    ops.extend(path::xml_insert_element(&mut a, &p(&["a"]), 1, b"p"));
    ops.extend(path::xml_insert_element(&mut a, &p(&["a"]), 2, b"span"));
    ops.extend(path::xml_move_child(&mut a, &p(&["a"]), 0, &p(&["a"]), 1));

    let mut b = doc(2);
    replay(&mut b, &ops);
    assert_eq!(children_of(&b, "a"), vec!["<p>", "<h1>", "<span>"]);
}

#[test]
fn a_child_moves_into_a_fragment() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["a"]), b"div");
    path::xml_insert_element(&mut d, &p(&["a"]), 0, b"h1");
    path::xml_fragment(&mut d, &p(&["frag"]));

    let ops = path::xml_move_child(&mut d, &p(&["a"]), 0, &p(&["frag"]), 0);
    assert!(!ops.is_empty());
    assert!(children_of(&d, "a").is_empty());
    assert_eq!(children_of(&d, "frag"), vec!["<h1>"]);
}

#[test]
fn a_child_moves_from_a_fragment() {
    let mut d = doc(1);
    path::xml_fragment(&mut d, &p(&["frag"]));
    path::xml_insert_element(&mut d, &p(&["frag"]), 0, b"item");
    path::xml_element(&mut d, &p(&["b"]), b"section");

    let ops = path::xml_move_child(&mut d, &p(&["frag"]), 0, &p(&["b"]), 0);
    assert!(!ops.is_empty());
    assert!(children_of(&d, "frag").is_empty());
    assert_eq!(children_of(&d, "b"), vec!["<item>"]);
}

#[test]
fn a_text_child_moves() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["a"]), b"div");
    path::xml_insert_text(&mut d, &p(&["a"]), 0, "hi");
    path::xml_element(&mut d, &p(&["b"]), b"section");

    let ops = path::xml_move_child(&mut d, &p(&["a"]), 0, &p(&["b"]), 0);
    assert!(!ops.is_empty());
    assert!(children_of(&d, "a").is_empty());
    assert_eq!(children_of(&d, "b"), vec!["\"hi\""]);
}

#[test]
fn concurrent_moves_of_a_child_converge() {
    let mut base = doc(1);
    let mut build = path::xml_element(&mut base, &p(&["a"]), b"div");
    build.extend(path::xml_insert_element(&mut base, &p(&["a"]), 0, b"x"));
    build.extend(path::xml_element(&mut base, &p(&["b"]), b"section"));
    build.extend(path::xml_element(&mut base, &p(&["c"]), b"aside"));

    let mut r1 = doc(2);
    let mut r2 = doc(3);
    replay(&mut r1, &build);
    replay(&mut r2, &build);

    // r1 moves x under b; r2 concurrently moves x under c.
    let m1 = path::xml_move_child(&mut r1, &p(&["a"]), 0, &p(&["b"]), 0);
    let m2 = path::xml_move_child(&mut r2, &p(&["a"]), 0, &p(&["c"]), 0);
    replay(&mut r1, &m2);
    replay(&mut r2, &m1);

    assert_eq!(children_of(&r1, "b"), children_of(&r2, "b"), "b diverged");
    assert_eq!(children_of(&r1, "c"), children_of(&r2, "c"), "c diverged");
    let under_b = children_of(&r1, "b") == vec!["<x>"];
    let under_c = children_of(&r1, "c") == vec!["<x>"];
    assert!(under_b ^ under_c, "x must have exactly one parent");
    assert!(children_of(&r1, "a").is_empty(), "x left its old parent");
}

#[test]
fn a_move_is_deterministic() {
    let build = |d: &mut Document| {
        let mut ops = path::xml_element(d, &p(&["a"]), b"div");
        ops.extend(path::xml_insert_element(d, &p(&["a"]), 0, b"h1"));
        ops.extend(path::xml_insert_element(d, &p(&["a"]), 1, b"p"));
        ops.extend(path::xml_element(d, &p(&["b"]), b"section"));
        ops.extend(path::xml_move_child(d, &p(&["a"]), 0, &p(&["b"]), 0));
        ops
    };
    let mut d1 = doc(1);
    let mut d2 = doc(1);
    assert_eq!(build(&mut d1), build(&mut d2));
}

// --- move: inert guards ---

#[test]
fn moving_from_a_non_element_parent_is_inert() {
    let mut d = doc(1);
    path::register_int(&mut d, &p(&["reg"]), 1);
    path::xml_element(&mut d, &p(&["b"]), b"section");
    let ops = path::xml_move_child(&mut d, &p(&["reg"]), 0, &p(&["b"]), 0);
    assert!(
        ops.is_empty(),
        "a move from a non-element parent emits nothing"
    );
    assert!(children_of(&d, "b").is_empty());
}

#[test]
fn moving_from_a_missing_parent_is_inert() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["b"]), b"section");
    let ops = path::xml_move_child(&mut d, &p(&["gone"]), 0, &p(&["b"]), 0);
    assert!(ops.is_empty());
    assert!(children_of(&d, "b").is_empty());
}

#[test]
fn moving_an_out_of_range_child_is_inert() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["a"]), b"div");
    path::xml_insert_element(&mut d, &p(&["a"]), 0, b"h1");
    path::xml_element(&mut d, &p(&["b"]), b"section");
    let ops = path::xml_move_child(&mut d, &p(&["a"]), 5, &p(&["b"]), 0);
    assert!(ops.is_empty(), "an out-of-range mover emits nothing");
    assert_eq!(children_of(&d, "a"), vec!["<h1>"]);
    assert!(children_of(&d, "b").is_empty());
}

#[test]
fn moving_to_a_non_element_destination_is_inert() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["a"]), b"div");
    path::xml_insert_element(&mut d, &p(&["a"]), 0, b"h1");
    path::register_int(&mut d, &p(&["reg"]), 1);
    let ops = path::xml_move_child(&mut d, &p(&["a"]), 0, &p(&["reg"]), 0);
    assert!(
        ops.is_empty(),
        "a move to a non-element destination emits nothing"
    );
    assert_eq!(children_of(&d, "a"), vec!["<h1>"]);
}

#[test]
fn moving_to_a_missing_destination_is_inert() {
    let mut d = doc(1);
    path::xml_element(&mut d, &p(&["a"]), b"div");
    path::xml_insert_element(&mut d, &p(&["a"]), 0, b"h1");
    let ops = path::xml_move_child(&mut d, &p(&["a"]), 0, &p(&["gone"]), 0);
    assert!(ops.is_empty());
    assert_eq!(children_of(&d, "a"), vec!["<h1>"]);
}
