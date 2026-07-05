//! Snapshot down-projection — `Document::drop_leaf_slots`.
//!
//! When a room's governing schema has added fields above a joining client's
//! version, that client's cold-start snapshot must read back the state it would
//! have reached from the same history delivered as a *down-translated* op delta.
//! The op seam (`Chain::translate_ops`) drops an added field's key-bearing
//! set/inc ops (`RegisterSet`/`MapSet`/`CounterInc`…) but carries its
//! container-creates (`MapCreate`/`ListCreate`/`TextCreate`) verbatim, since a
//! key-local rewrite cannot reach a container's keyless descendants without
//! tearing the subtree. `drop_leaf_slots` is that same projection at snapshot
//! granularity: across every map, a slot at one of the given keys is removed
//! unless it holds a live container. So a snapshot-served joiner and a peer
//! served the same history as a translated delta converge on the same
//! observable state.

use std::collections::BTreeSet;

use crdtsync_core::doc::Document;
use crdtsync_core::{Element, Scalar};

mod common;
use common::cid;

fn doc() -> Document {
    Document::new(cid(1))
}

fn keys(ks: &[&[u8]]) -> BTreeSet<Vec<u8>> {
    ks.iter().map(|k| k.to_vec()).collect()
}

/// The scalar behind a root register slot, or `None` if the slot is absent.
fn reg(d: &Document, key: &[u8]) -> Option<i64> {
    match d.get(key) {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => Some(*n),
            _ => panic!("expected an Int register"),
        },
        None => None,
        _ => panic!("expected a register or nothing"),
    }
}

#[test]
fn a_scalar_slot_at_a_dropped_key_is_removed() {
    let mut d = doc();
    d.transact(|tx| {
        tx.set(b"keep", Scalar::Int(1));
        tx.set(b"note", Scalar::Int(2));
    });
    d.drop_leaf_slots(&keys(&[b"note"]));
    assert!(d.get(b"note").is_none(), "a dropped scalar slot is gone");
    assert!(d.get(b"keep").is_some(), "an unlisted slot survives");
}

#[test]
fn a_register_slot_at_a_dropped_key_is_removed() {
    let mut d = doc();
    d.transact(|tx| {
        tx.register(b"keep", Scalar::Int(1));
        tx.register(b"note", Scalar::Int(2));
    });
    d.drop_leaf_slots(&keys(&[b"note"]));
    assert_eq!(reg(&d, b"note"), None);
    assert_eq!(reg(&d, b"keep"), Some(1));
}

#[test]
fn a_counter_slot_at_a_dropped_key_is_removed() {
    let mut d = doc();
    d.transact(|tx| {
        tx.inc(b"hits", 5);
        tx.inc(b"note", 9);
    });
    d.drop_leaf_slots(&keys(&[b"note"]));
    assert!(d.get(b"note").is_none(), "a dropped counter slot is gone");
    assert!(d.get(b"hits").is_some(), "an unlisted counter survives");
}

#[test]
fn a_map_slot_at_a_dropped_key_is_kept() {
    let mut d = doc();
    d.transact(|tx| {
        let mut m = tx.map(b"note");
        m.set(b"inner", Scalar::Int(7));
    });
    d.drop_leaf_slots(&keys(&[b"note"]));
    match d.get(b"note") {
        Some(Element::Map(m)) => assert!(
            m.borrow().get(b"inner").is_some(),
            "a kept container keeps its subtree"
        ),
        _ => panic!("a container slot is carried verbatim"),
    }
}

#[test]
fn a_list_slot_at_a_dropped_key_is_kept() {
    let mut d = doc();
    d.transact(|tx| {
        let mut l = tx.list(b"note");
        l.insert(0, Scalar::Int(1));
    });
    d.drop_leaf_slots(&keys(&[b"note"]));
    assert!(
        matches!(d.get(b"note"), Some(Element::List(_))),
        "a list container is carried verbatim"
    );
}

#[test]
fn a_text_slot_at_a_dropped_key_is_kept() {
    let mut d = doc();
    d.transact(|tx| {
        tx.text(b"note").insert(0, "hi");
    });
    d.drop_leaf_slots(&keys(&[b"note"]));
    assert!(
        matches!(d.get(b"note"), Some(Element::Text(_))),
        "a text container is carried verbatim"
    );
}

#[test]
fn a_leaf_inside_a_kept_container_is_also_dropped() {
    // The projection filters every map in the tree, not only the root — a
    // dropped key inside a surviving container is removed too.
    let mut d = doc();
    d.transact(|tx| {
        let mut m = tx.map(b"box");
        m.set(b"shared", Scalar::Int(1));
        m.register(b"note", Scalar::Int(2));
    });
    d.drop_leaf_slots(&keys(&[b"note"]));
    match d.get(b"box") {
        Some(Element::Map(m)) => {
            let m = m.borrow();
            assert!(m.get(b"shared").is_some(), "the shared leaf survives");
            assert!(m.get(b"note").is_none(), "the nested dropped leaf is gone");
        }
        _ => panic!("expected the surviving box map"),
    }
}

#[test]
fn an_unlisted_key_is_untouched() {
    let mut d = doc();
    d.transact(|tx| tx.register(b"keep", Scalar::Int(42)));
    d.drop_leaf_slots(&keys(&[b"other"]));
    assert_eq!(reg(&d, b"keep"), Some(42));
}

#[test]
fn an_empty_key_set_is_identity() {
    let mut d = doc();
    d.transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.inc(b"b", 2);
        tx.map(b"c").set(b"x", Scalar::Int(3));
    });
    let before = d.encode_state();
    d.drop_leaf_slots(&keys(&[]));
    assert_eq!(d.encode_state(), before, "dropping no keys changes nothing");
}

#[test]
fn a_projected_document_round_trips() {
    // After a projection the document is still a valid, canonical snapshot: it
    // re-encodes and decodes back to the same observable state.
    let mut d = doc();
    d.transact(|tx| {
        tx.register(b"keep", Scalar::Int(1));
        tx.register(b"note", Scalar::Int(2));
        tx.map(b"sub").set(b"note", Scalar::Int(3));
    });
    d.drop_leaf_slots(&keys(&[b"note"]));
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).unwrap();
    assert_eq!(reg(&back, b"keep"), Some(1));
    assert_eq!(reg(&back, b"note"), None);
    match back.get(b"sub") {
        Some(Element::Map(m)) => assert!(m.borrow().get(b"note").is_none()),
        _ => panic!("expected the sub map"),
    }
    assert_eq!(back.encode_state(), bytes, "re-encode is not canonical");
}

#[test]
fn dropping_a_key_present_in_no_map_is_a_no_op() {
    let mut d = doc();
    d.transact(|tx| tx.register(b"keep", Scalar::Int(1)));
    let before = d.encode_state();
    d.drop_leaf_slots(&keys(&[b"absent", b"missing"]));
    assert_eq!(d.encode_state(), before);
}
