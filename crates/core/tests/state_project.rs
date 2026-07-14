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
use crdtsync_core::{Element, ElementId, ElementKind, Scalar};

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
fn a_deleted_container_slot_is_carried_verbatim() {
    // A create-then-delete leaves a tombstone slot (value None) plus the displaced
    // container retained in the registry. It must NOT migrate as a leaf tombstone:
    // the op seam carries the create verbatim (resurrecting it at the old key) and
    // the materialized snapshot has lost the create's stamp, so a snapshot cannot
    // re-key it faithfully — it is carried verbatim, both drop and rename a no-op.
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"note").set(b"inner", Scalar::Int(7));
    });
    d.transact(|tx| tx.delete(b"note"));
    assert!(
        !d.migrate_leaf_slots(drop_keys(&[b"note"])),
        "a deleted container's tombstone is not dropped as a leaf"
    );
    assert!(
        !d.migrate_leaf_slots(rename(b"note", b"renamed")),
        "a deleted container's tombstone is not re-keyed as a leaf"
    );
    assert!(d.get(b"renamed").is_none(), "nothing lands at the new key");
}

#[test]
fn a_phantom_counter_under_a_deleted_container_key_is_dropped() {
    // A key can hold BOTH a deleted container (retained in the container registry)
    // and a displaced counter (retained in the counter registry): create a
    // container, delete it, increment the same key (a counter wins the slot),
    // delete again. The slot is a tombstone with container identity, so its body
    // is carried verbatim — but the counter registry entry is a separate identity
    // and must still be pruned, or a phantom tally survives and diverges from an
    // op-served peer whose CounterInc was dropped.
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"note").set(b"x", Scalar::Int(1));
    });
    d.transact(|tx| tx.delete(b"note")); // container displaced, slot tombstoned
    d.transact(|tx| tx.inc(b"note", 5)); // a counter wins the slot
    d.transact(|tx| tx.delete(b"note")); // counter displaced, slot tombstoned
    assert!(
        d.migrate_leaf_slots(drop_keys(&[b"note"])),
        "the phantom counter is pruned even under a container-identity slot"
    );
    // Re-creating a counter at the key starts fresh, not resuming the phantom tally.
    d.transact(|tx| tx.inc(b"note", 3));
    assert_eq!(counter(&d, b"note"), Some(3), "no phantom tally re-adopted");
}

#[test]
fn a_phantom_counter_under_a_deleted_container_key_rehomes_on_rename() {
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"a").set(b"x", Scalar::Int(1));
    });
    d.transact(|tx| tx.delete(b"a"));
    d.transact(|tx| tx.inc(b"a", 5));
    d.transact(|tx| tx.delete(b"a")); // phantom counter + displaced map both at `a`
    assert!(d.migrate_leaf_slots(rename(b"a", b"b")));
    // The old key's counter id is vacated: a fresh increment there starts at zero.
    d.transact(|tx| tx.inc(b"a", 1));
    assert_eq!(counter(&d, b"a"), Some(1), "no phantom left at the old key");
    // The tally rehomed to the new key's counter id: a later increment resumes it.
    d.transact(|tx| tx.inc(b"b", 4));
    assert_eq!(
        counter(&d, b"b"),
        Some(9),
        "the phantom tally rehomed to the new key (5 + 4)"
    );
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

#[test]
fn a_renamed_register_is_rekeyed_to_the_new_id() {
    // A register carries an id derived from its slot key, encoded into the
    // snapshot. Moving it verbatim under a rename would keep the old-key id,
    // diverging from an op-served peer whose renamed RegisterSet derives the id
    // from the new key. The moved register must re-derive its id.
    let mut d = doc();
    d.transact(|tx| tx.register(b"a", Scalar::Int(9)));
    assert!(d.migrate_leaf_slots(rename(b"a", b"b")));
    match d.get(b"b") {
        Some(Element::Register(r)) => assert_eq!(
            r.borrow().id(),
            ElementId::derive(d.root_id(), b"b", ElementKind::Register),
            "the register id is re-derived from the new key"
        ),
        _ => panic!("expected the renamed register at b"),
    }
    // The re-key survives a round-trip: the snapshot encodes the new-key id.
    let back = Document::decode_state(&d.encode_state()).unwrap();
    match back.get(b"b") {
        Some(Element::Register(r)) => assert_eq!(
            r.borrow().id(),
            ElementId::derive(back.root_id(), b"b", ElementKind::Register)
        ),
        _ => panic!("expected the register to round-trip"),
    }
}

#[test]
fn a_dropped_displaced_counter_leaves_no_phantom() {
    // A scalar can displace a counter, which stays retained in the registry at
    // its derived id. Dropping the key must prune that retained tally too, or it
    // lingers as a phantom — diverging from an op-served peer whose CounterInc
    // was simply dropped.
    let mut d = doc();
    d.transact(|tx| tx.inc(b"note", 5));
    d.transact(|tx| tx.set(b"note", Scalar::Int(1))); // scalar displaces the counter
    assert!(d.migrate_leaf_slots(drop_keys(&[b"note"])));
    assert!(d.get(b"note").is_none());
    // Re-creating a counter at the key starts fresh, not resuming the displaced tally.
    d.transact(|tx| tx.inc(b"note", 3));
    assert_eq!(counter(&d, b"note"), Some(3), "no phantom tally re-adopted");
}

#[test]
fn a_renamed_displaced_counter_rehomes_its_tally() {
    // A displaced counter's tally must ride the rename to the new key's derived
    // id even though the slot body is now a scalar — matching an op-served peer
    // whose renamed CounterInc lands at that id while the renamed set holds the
    // slot.
    let mut d = doc();
    d.transact(|tx| tx.inc(b"a", 5));
    d.transact(|tx| tx.set(b"a", Scalar::Int(1))); // scalar displaces the counter
    assert!(d.migrate_leaf_slots(rename(b"a", b"b")));
    // The slot moves the scalar; the retained tally re-homes under b's counter id.
    assert!(matches!(d.get(b"b"), Some(Element::Scalar(Scalar::Int(1)))));
    // A later increment at b re-wins the slot and resumes the rehomed tally (5+4).
    d.transact(|tx| tx.inc(b"b", 4));
    assert_eq!(
        counter(&d, b"b"),
        Some(9),
        "the rehomed tally (5) resumes when the slot is re-won at the new key"
    );
    // Nothing lingers at the old key's counter id: an increment there starts fresh.
    d.transact(|tx| tx.inc(b"a", 1));
    assert_eq!(counter(&d, b"a"), Some(1), "no phantom left at the old id");
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

// --- scoped by owning map ---

/// The Int behind `outer.inner`, or `None` when either level is absent.
fn nested_reg(d: &Document, outer: &[u8], inner: &[u8]) -> Option<i64> {
    let m = match d.get(outer) {
        Some(Element::Map(m)) => m,
        _ => return None,
    };
    let child = m.borrow().get(inner);
    match child {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => Some(*n),
            _ => panic!("expected an Int register"),
        },
        None => None,
        _ => panic!("expected a register or nothing"),
    }
}

#[test]
fn a_scoped_fate_narrows_to_its_owning_map() {
    // Two maps hold the same slot key; a rename scoped to one map's id re-keys
    // only that map's slot and leaves the other's verbatim — the id-aware seam a
    // type-scoped migration reads, so a field rewrite on one type never touches a
    // same-named slot on another.
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"note").register(b"title", Scalar::Int(1));
        tx.map(b"task").register(b"title", Scalar::Int(2));
    });
    let root = d.root_id();
    let note = ElementId::derive(root, b"note", ElementKind::Map);

    let changed = d.migrate_leaf_slots_scoped(|map_id, key| {
        if map_id == note && key == b"title" {
            SlotFate::Rename(b"heading".to_vec())
        } else {
            SlotFate::Keep
        }
    });
    assert!(changed);
    assert_eq!(
        nested_reg(&d, b"note", b"heading"),
        Some(1),
        "note.title re-keys"
    );
    assert_eq!(nested_reg(&d, b"note", b"title"), None);
    assert_eq!(
        nested_reg(&d, b"task", b"title"),
        Some(2),
        "task.title is untouched"
    );
    assert_eq!(nested_reg(&d, b"task", b"heading"), None);
}
