//! Snapshot migration — `Document::migrate_leaf_slots`.
//!
//! When a joiner catches up below the compaction floor it is served the room's
//! merged state as a snapshot rather than an op delta. If the joiner's schema
//! version differs from the room's, that snapshot must be migrated to read back
//! the state the joiner would have reached from the same history delivered as a
//! *translated* op delta. The op seam rewrites each key-bearing op's key (drop
//! an added field down / a removed field up, rename a renamed field) while
//! carrying a container-create verbatim; `migrate_leaf_slots` is that same
//! transform at snapshot granularity — a per-key `SlotFate` over every leaf
//! slot, containers untouched — so a snapshot-served joiner and an op-delta
//! joiner converge. A dropped or renamed counter's element moves with its slot,
//! leaving no phantom behind.

use crdtsync_core::doc::{Document, SlotFate};
use crdtsync_core::{Element, Scalar};

mod common;
use common::cid;

fn doc() -> Document {
    Document::new(cid(1))
}

/// A fate that drops the listed keys and keeps the rest.
fn drop_keys(ks: &'static [&'static [u8]]) -> impl Fn(&[u8]) -> SlotFate {
    move |key| {
        if ks.contains(&key) {
            SlotFate::Drop
        } else {
            SlotFate::Keep
        }
    }
}

/// A fate that renames `from` to `to` and keeps the rest.
fn rename(from: &'static [u8], to: &'static [u8]) -> impl Fn(&[u8]) -> SlotFate {
    move |key| {
        if key == from {
            SlotFate::Rename(to.to_vec())
        } else {
            SlotFate::Keep
        }
    }
}

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

fn counter(d: &Document, key: &[u8]) -> Option<i64> {
    match d.get(key) {
        Some(Element::Counter(c)) => Some(c.borrow().read()),
        None => None,
        _ => panic!("expected a counter or nothing"),
    }
}

// --- drop ---

#[test]
fn a_dropped_scalar_slot_is_removed() {
    let mut d = doc();
    d.transact(|tx| {
        tx.set(b"keep", Scalar::Int(1));
        tx.set(b"note", Scalar::Int(2));
    });
    assert!(d.migrate_leaf_slots(drop_keys(&[b"note"])));
    assert!(d.get(b"note").is_none());
    assert!(d.get(b"keep").is_some());
}

#[test]
fn a_dropped_register_slot_is_removed() {
    let mut d = doc();
    d.transact(|tx| {
        tx.register(b"keep", Scalar::Int(1));
        tx.register(b"note", Scalar::Int(2));
    });
    assert!(d.migrate_leaf_slots(drop_keys(&[b"note"])));
    assert_eq!(reg(&d, b"note"), None);
    assert_eq!(reg(&d, b"keep"), Some(1));
}

#[test]
fn a_dropped_counter_slot_leaves_no_phantom() {
    // Dropping a counter slot must also drop its registry element. Otherwise a
    // phantom counter lingers and, when the key is later re-written, re-adopts
    // its old tally — diverging from an op-delta joiner whose CounterInc was
    // simply dropped and never materialised the counter.
    let mut d = doc();
    d.transact(|tx| {
        tx.inc(b"keep", 1);
        tx.inc(b"note", 5);
    });
    assert!(d.migrate_leaf_slots(drop_keys(&[b"note"])));
    assert_eq!(counter(&d, b"note"), None);
    assert_eq!(counter(&d, b"keep"), Some(1));
    // Re-creating the counter starts fresh, not resuming the dropped tally.
    d.transact(|tx| tx.inc(b"note", 3));
    assert_eq!(counter(&d, b"note"), Some(3), "no phantom tally re-adopted");
}

// --- containers are carried verbatim ---

/// Neither a drop nor a rename fate touches a live container `d.note`.
#[track_caller]
fn assert_container_survives(mut d: Document) {
    assert!(!d.migrate_leaf_slots(drop_keys(&[b"note"])));
    assert!(d.get(b"note").is_some(), "a container survives a drop");
    assert!(!d.migrate_leaf_slots(rename(b"note", b"renamed")));
    assert!(d.get(b"note").is_some(), "a container survives a rename");
    assert!(d.get(b"renamed").is_none());
}

#[test]
fn a_map_slot_is_never_dropped_or_renamed() {
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"note").set(b"inner", Scalar::Int(7));
    });
    assert_container_survives(d);
}

#[test]
fn a_list_slot_is_never_dropped_or_renamed() {
    let mut d = doc();
    d.transact(|tx| {
        tx.list(b"note").insert(0, Scalar::Int(1));
    });
    assert_container_survives(d);
}

#[test]
fn a_text_slot_is_never_dropped_or_renamed() {
    let mut d = doc();
    d.transact(|tx| {
        tx.text(b"note").insert(0, "hi");
    });
    assert_container_survives(d);
}

#[test]
fn a_leaf_inside_a_kept_container_is_migrated() {
    let mut d = doc();
    d.transact(|tx| {
        let mut m = tx.map(b"box");
        m.set(b"shared", Scalar::Int(1));
        m.register(b"note", Scalar::Int(2));
    });
    assert!(d.migrate_leaf_slots(drop_keys(&[b"note"])));
    match d.get(b"box") {
        Some(Element::Map(m)) => {
            let m = m.borrow();
            assert!(m.get(b"shared").is_some());
            assert!(m.get(b"note").is_none());
        }
        _ => panic!("expected the surviving box map"),
    }
}

// --- rename ---

#[test]
fn a_renamed_scalar_slot_moves_to_the_new_key() {
    let mut d = doc();
    d.transact(|tx| tx.set(b"a", Scalar::Int(9)));
    assert!(d.migrate_leaf_slots(rename(b"a", b"b")));
    assert!(d.get(b"a").is_none());
    assert!(matches!(d.get(b"b"), Some(Element::Scalar(Scalar::Int(9)))));
}

#[test]
fn a_renamed_register_slot_moves_to_the_new_key() {
    let mut d = doc();
    d.transact(|tx| tx.register(b"a", Scalar::Int(9)));
    assert!(d.migrate_leaf_slots(rename(b"a", b"b")));
    assert_eq!(reg(&d, b"a"), None);
    assert_eq!(reg(&d, b"b"), Some(9));
}

#[test]
fn a_renamed_counter_rehomes_its_tally_and_leaves_no_phantom() {
    // A renamed counter lands at the id its new key derives, carrying its tally,
    // with nothing left at the old id.
    let mut d = doc();
    d.transact(|tx| {
        tx.inc(b"a", 5);
        tx.dec(b"a", 2);
    });
    assert!(d.migrate_leaf_slots(rename(b"a", b"b")));
    assert_eq!(counter(&d, b"a"), None);
    assert_eq!(counter(&d, b"b"), Some(3), "the tally rides to the new key");
    // The old id holds no phantom: re-creating a counter at `a` starts fresh.
    d.transact(|tx| tx.inc(b"a", 10));
    assert_eq!(counter(&d, b"a"), Some(10), "no phantom left at the old id");
    assert_eq!(
        counter(&d, b"b"),
        Some(3),
        "the rehomed counter is undisturbed"
    );
}

#[test]
fn a_counter_renamed_onto_an_occupied_counter_merges_at_the_shared_id() {
    // A rename can land on a key already holding a counter (a cross-type key
    // collision the type-scope-blind seam does not narrow). It must merge into
    // the id the new key derives — as the renamed increment ops would at that
    // shared id — leaving the slot and the registry pointing at one merged
    // counter, never a phantom or a desync, whichever stamp wins the slot.
    let mut d = doc();
    d.transact(|tx| {
        tx.inc(b"a", 5);
        tx.inc(b"b", 10);
    });
    assert!(d.migrate_leaf_slots(rename(b"a", b"b")));
    assert_eq!(counter(&d, b"a"), None, "the source key is vacated");
    // Same author, so the PN-counter merge keeps the larger tally.
    assert_eq!(
        counter(&d, b"b"),
        Some(10),
        "the counters merge at the shared id"
    );
    // The slot and registry agree through a round-trip — no phantom, no desync.
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).unwrap();
    assert_eq!(counter(&back, b"b"), Some(10));
    assert_eq!(back.encode_state(), bytes, "re-encode is not canonical");
}

#[test]
fn a_chained_counter_rename_is_order_independent() {
    // A non-composed fate renames a→c and c→d in one pass. Each source must
    // contribute its ORIGINAL tally to its own target: capturing an isolated copy
    // when the slot is taken keeps a's tally from leaking through c's live handle
    // into d, so the result never depends on the traversal (HashMap) order.
    let mut d = doc();
    d.transact(|tx| {
        tx.inc(b"a", 5);
        tx.inc(b"c", 7);
    });
    let fate = |key: &[u8]| match key {
        b"a" => SlotFate::Rename(b"c".to_vec()),
        b"c" => SlotFate::Rename(b"d".to_vec()),
        _ => SlotFate::Keep,
    };
    assert!(d.migrate_leaf_slots(fate));
    assert_eq!(counter(&d, b"a"), None);
    assert_eq!(counter(&d, b"c"), Some(5), "c holds a's original tally");
    assert_eq!(
        counter(&d, b"d"),
        Some(7),
        "d holds c's original tally, not a+c"
    );
}

// --- identity / no-op ---

#[test]
fn an_all_keep_fate_is_a_no_op() {
    let mut d = doc();
    d.transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.inc(b"b", 2);
        tx.map(b"c").set(b"x", Scalar::Int(3));
    });
    let before = d.encode_state();
    assert!(!d.migrate_leaf_slots(|_| SlotFate::Keep));
    assert_eq!(d.encode_state(), before);
}

#[test]
fn a_fate_matching_no_slot_is_a_no_op() {
    let mut d = doc();
    d.transact(|tx| tx.register(b"keep", Scalar::Int(1)));
    let before = d.encode_state();
    assert!(!d.migrate_leaf_slots(drop_keys(&[b"absent"])));
    assert_eq!(d.encode_state(), before);
}

#[test]
fn a_migrated_document_round_trips_canonically() {
    let mut d = doc();
    d.transact(|tx| {
        tx.register(b"keep", Scalar::Int(1));
        tx.register(b"note", Scalar::Int(2));
        tx.map(b"sub").register(b"note", Scalar::Int(3));
    });
    assert!(d.migrate_leaf_slots(drop_keys(&[b"note"])));
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).unwrap();
    assert_eq!(reg(&back, b"keep"), Some(1));
    assert_eq!(reg(&back, b"note"), None);
    assert_eq!(back.encode_state(), bytes, "re-encode is not canonical");
}
