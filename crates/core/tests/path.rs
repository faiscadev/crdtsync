//! Path addressing — the stateless navigation the language bindings share.
//!
//! A slot is named by a path: a length-framed sequence of keys, the last the
//! slot, the rest nested maps. An edit applies locally and returns the ops to
//! broadcast; a read resolves the whole path or yields nothing. A path that
//! doesn't resolve is inert — it neither panics nor materialises a container.
//! This is the one navigation implementation every binding (FFI, wasm) wraps.

use crdtsync_core::doc::Document;
use crdtsync_core::path;

mod common;
use common::cid;

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn p(keys: &[&str]) -> Vec<u8> {
    let keys: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
    path::encode_path(&keys)
}

/// Fold `a`'s emitted ops into `b`.
fn replay(b: &mut Document, ops: &[crdtsync_core::op::Op]) {
    for op in ops {
        b.apply(op);
    }
}

// --- register / scalar ---

#[test]
fn register_int_reads_back() {
    let mut d = doc(1);
    path::register_int(&mut d, &p(&["age"]), 30);
    assert_eq!(path::get_int(&d, &p(&["age"])), Some(30));
}

#[test]
fn a_missing_slot_is_absent() {
    let d = doc(1);
    assert_eq!(path::get_int(&d, &p(&["age"])), None);
}

#[test]
fn a_nested_path_reads_back() {
    let mut d = doc(1);
    let path = p(&["profile", "stats", "score"]);
    path::register_int(&mut d, &path, 7);
    assert_eq!(path::get_int(&d, &path), Some(7));
}

#[test]
fn a_counter_accumulates() {
    let mut d = doc(1);
    path::inc(&mut d, &p(&["hits"]), 3);
    path::inc(&mut d, &p(&["hits"]), 4);
    assert_eq!(path::get_counter(&d, &p(&["hits"])), Some(7));
}

#[test]
fn bytes_round_trip() {
    let mut d = doc(1);
    let want = vec![0u8, 1, 255, 0];
    path::set_bytes(&mut d, &p(&["blob"]), &want);
    assert_eq!(path::get_bytes(&d, &p(&["blob"])), Some(want));
}

#[test]
fn delete_removes_a_slot() {
    let mut d = doc(1);
    path::register_int(&mut d, &p(&["age"]), 30);
    path::delete(&mut d, &p(&["age"]));
    assert_eq!(path::get_int(&d, &p(&["age"])), None);
}

// --- list ---

#[test]
fn list_inserts_and_reads() {
    let mut d = doc(1);
    let path = p(&["board", "cards"]);
    path::list_insert(&mut d, &path, 0, b"x");
    path::list_insert(&mut d, &path, 1, b"y");
    assert_eq!(path::list_len(&d, &path), Some(2));
    assert_eq!(path::list_get(&d, &path, 0), Some(b"x".to_vec()));
}

#[test]
fn a_no_op_list_delete_stays_inert() {
    let mut d = doc(1);
    // Deleting from an absent list emits nothing and materialises no container.
    assert!(path::list_delete(&mut d, &p(&["ghost"]), 0).is_empty());
    assert_eq!(path::list_len(&d, &p(&["ghost"])), None);
}

// --- text ---

#[test]
fn text_inserts_and_deletes_by_codepoint() {
    let mut d = doc(1);
    let path = p(&["doc", "title"]);
    path::text_insert(&mut d, &path, 0, "héllo");
    assert_eq!(path::text_len(&d, &path), Some(5));
    assert_eq!(path::text_get(&d, &path), Some("héllo".to_string()));
    path::text_delete(&mut d, &path, 1, 3);
    assert_eq!(path::text_get(&d, &path), Some("ho".to_string()));
}

// --- convergence + inert paths ---

#[test]
fn emitted_ops_converge_on_a_peer() {
    let mut a = doc(1);
    let mut b = doc(2);
    let path = p(&["user", "age"]);
    let ops = path::register_int(&mut a, &path, 30);
    replay(&mut b, &ops);
    assert_eq!(path::get_int(&b, &path), Some(30));
}

#[test]
fn a_path_through_a_non_map_does_not_resolve() {
    let mut d = doc(1);
    path::register_int(&mut d, &p(&["age"]), 30);
    // "age" is a register, not a map, so descending through it reads nothing.
    assert_eq!(path::get_int(&d, &p(&["age", "inner"])), None);
}

#[test]
fn an_empty_path_edit_emits_nothing() {
    let mut d = doc(1);
    assert!(path::register_int(&mut d, &[], 1).is_empty());
}

// --- path codec ---

#[test]
fn encode_and_parse_round_trip() {
    let keys: Vec<Vec<u8>> = vec![b"ab".to_vec(), b"c".to_vec()];
    let bytes = p(&["ab", "c"]);
    assert_eq!(path::parse_path(&bytes), Some(keys));
}

#[test]
fn a_truncated_path_does_not_parse() {
    // A length header promising more bytes than remain is rejected.
    assert_eq!(path::parse_path(&[2, 0, 0, 0, b'a']), None);
}
