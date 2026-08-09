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
//! slot, a live container carried verbatim — so a snapshot-served joiner and an
//! op-delta joiner converge. A *deleted* container's tombstone is re-keyed
//! faithfully by its retained create-stamp: the container resurrects live at the
//! old key while its delete re-keys, matching the op seam byte-for-byte. A
//! dropped or renamed counter's element moves with its slot, leaving no phantom
//! behind.

use crdtsync_core::doc::{Document, SlotFate};
use crdtsync_core::{Element, ElementId, ElementKind, Op, Scalar};

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
fn a_deleted_container_slot_is_re_keyed_faithfully_on_rename() {
    // A create-then-delete leaves a tombstone whose retained create-stamp lets the
    // snapshot resurrect the container live at the old key (the create the op seam
    // carries verbatim there) and re-key the delete to a fresh tombstone at the new
    // key — the same state an op-served joiner reaches, not a verbatim carry.
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"note").set(b"inner", Scalar::Int(7));
    });
    d.transact(|tx| tx.delete(b"note"));
    assert!(
        d.migrate_leaf_slots(rename(b"note", b"renamed")),
        "the deleted container is re-keyed, a real change"
    );
    match d.get(b"note") {
        Some(Element::Map(m)) => {
            assert!(
                matches!(
                    m.borrow().get(b"inner"),
                    Some(Element::Scalar(Scalar::Int(7)))
                ),
                "the container resurrects live at the old key with its content"
            );
        }
        _ => panic!("expected the resurrected container at the old key"),
    }
    assert!(
        d.get(b"renamed").is_none(),
        "the delete re-keyed: the new key holds a tombstone, not a live slot"
    );
    // The re-key survives a round-trip: the resurrected container and the moved
    // tombstone re-encode canonically.
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).unwrap();
    assert_eq!(back.encode_state(), bytes, "re-encode is canonical");
}

#[test]
fn a_multi_kind_key_resurrects_the_highest_create() {
    // A key can host more than one container kind over its life — a map created
    // then deleted, then a list created then deleted. Both ids stay registered, so
    // the resurrection must pick the highest-ranked create (the list), not a fixed
    // priority. The list won the slot before its delete, so the op seam resurrects
    // the list; the snapshot must match. Here the highest create is also the last
    // deleted; the two are told apart where one delete outranks both creates.
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"k").set(b"m", Scalar::Int(1));
    });
    d.transact(|tx| tx.delete(b"k"));
    d.transact(|tx| {
        tx.list(b"k").insert(0, Scalar::Int(2));
    });
    d.transact(|tx| tx.delete(b"k"));
    assert!(d.migrate_leaf_slots(rename(b"k", b"k2")));
    assert!(
        matches!(d.get(b"k"), Some(Element::List(_))),
        "the highest-ranked create (the list) resurrects, not the map a fixed priority would pick"
    );
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).unwrap();
    assert_eq!(back.encode_state(), bytes, "re-encode is canonical");
}

/// Fold `pool` into a fresh replica in the given order — the seam where a create
/// and the delete that outranks it can arrive either way round.
fn fold(pool: &[&Op]) -> Document {
    let mut d = Document::new(cid(9));
    for op in pool {
        assert!(d.apply(op), "every op in these pools targets the root map");
    }
    d
}

#[test]
fn a_create_the_delete_outranked_resurrects_all_the_same() {
    // A replica that saw the delete first never watched the container land, and it
    // re-keys exactly like one that did: the create the op seam carries verbatim at
    // the old key is the create the key retains, not the one a fold happened to see
    // installed. Both orders migrate to the same bytes.
    let mut a = doc();
    let create = a.transact(|tx| {
        tx.map(b"note");
    });
    let delete = a.transact(|tx| tx.delete(b"note"));
    let (create, delete) = (&create[0], &delete[0]);

    let mut late = fold(&[delete, create]);
    assert!(late.migrate_leaf_slots(rename(b"note", b"renamed")));
    assert!(
        matches!(late.get(b"note"), Some(Element::Map(_))),
        "the create the delete outranked resurrects at the old key"
    );
    let mut early = fold(&[create, delete]);
    assert!(early.migrate_leaf_slots(rename(b"note", b"renamed")));
    assert_eq!(
        late.encode_state(),
        early.encode_state(),
        "the two arrival orders migrate to the same snapshot"
    );
}

#[test]
fn the_highest_create_is_the_one_a_shared_delete_resurrects() {
    // Two creates of different kinds at one key, both outranked by a single delete.
    // Which one resurrects is the creates' own rank — the higher stamp, the one
    // that won or would have won the slot — never the order a replica saw them in.
    // The reading converges on every order regardless, so it is the resurrection
    // that pins the rank.
    let mut a = doc();
    let map = a.transact(|tx| {
        tx.map(b"k");
    });
    let list = a.transact(|tx| {
        tx.list(b"k");
    });
    let delete = a.transact(|tx| tx.delete(b"k"));
    let (map, list, delete) = (&map[0], &list[0], &delete[0]);

    for order in [
        [map, list, delete],
        [map, delete, list],
        [list, map, delete],
        [list, delete, map],
        [delete, map, list],
        [delete, list, map],
    ] {
        let mut d = fold(&order);
        assert!(d.migrate_leaf_slots(rename(b"k", b"k2")));
        assert!(
            matches!(d.get(b"k"), Some(Element::List(_))),
            "the higher-stamped create (the list) resurrects, whatever the order"
        );
    }
}

#[test]
fn a_container_a_leaf_displaced_before_the_delete_still_resurrects() {
    // A scalar takes the key from a live container, then a delete tombstones the
    // scalar. The op seam still holds the create and carries it verbatim, so the
    // snapshot resurrects it too — the create the key retains outlives the leaf
    // that outranked it.
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"note");
    });
    d.transact(|tx| tx.set(b"note", Scalar::Int(1)));
    d.transact(|tx| tx.delete(b"note"));
    assert!(d.migrate_leaf_slots(rename(b"note", b"renamed")));
    assert!(
        matches!(d.get(b"note"), Some(Element::Map(_))),
        "the displaced container resurrects at the old key"
    );
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).unwrap();
    assert_eq!(back.encode_state(), bytes, "re-encode is canonical");
}

#[test]
fn a_deleted_xml_fragment_resurrects_at_its_old_key() {
    // A fragment's id derives from its parent and key exactly as a map's does, so
    // the key names it and the create the tombstone retains resolves to it: the
    // fragment lands live at the old key and the delete re-keys, the same state an
    // op-served joiner reaches. Only an XML *element*, whose id mixes its tag in
    // below the key, is unreachable this way.
    let mut d = doc();
    d.transact(|tx| {
        tx.xml_fragment(b"body").children().insert_element(0, b"p");
    });
    d.transact(|tx| tx.delete(b"body"));
    assert!(d.migrate_leaf_slots(rename(b"body", b"renamed")));
    match d.get(b"body") {
        Some(Element::XmlFragment(f)) => assert_eq!(
            f.borrow().children().borrow().len(),
            1,
            "the resurrected fragment keeps the children its id derives"
        ),
        _ => panic!("expected the resurrected fragment at the old key"),
    }
    assert!(
        d.get(b"renamed").is_none(),
        "the delete re-keyed: the new key holds a tombstone, not a live slot"
    );
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).unwrap();
    assert_eq!(back.encode_state(), bytes, "re-encode is canonical");
}

#[test]
fn an_xml_element_create_outranking_a_map_create_leaves_the_key_carried_verbatim() {
    // An XML element wins the slot from a map create and is itself deleted. Its id
    // mixes its tag in below the key, so the key resolves no handle to resurrect —
    // and the map create it outranks is not the one the op seam leaves live there,
    // so resurrecting that instead would put the wrong container at the key. The
    // slot is carried verbatim, the pre-existing behaviour for an element.
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"k");
    });
    d.transact(|tx| {
        tx.xml_element(b"k", b"p");
    });
    d.transact(|tx| tx.delete(b"k"));
    assert!(
        !d.migrate_leaf_slots(rename(b"k", b"k2")),
        "the slot is carried verbatim, not re-keyed"
    );
    assert!(d.get(b"k").is_none(), "no container is resurrected");
    assert!(d.get(b"k2").is_none());
}

#[test]
fn a_deleted_container_slot_is_resurrected_on_drop() {
    // Dropping a deleted container's field drops its delete op (the op seam drops
    // a removed field's ops) while the create carries verbatim — so the container
    // resurrects live at its key, no tombstone re-keyed anywhere.
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"note").set(b"inner", Scalar::Int(7));
    });
    d.transact(|tx| tx.delete(b"note"));
    assert!(
        d.migrate_leaf_slots(drop_keys(&[b"note"])),
        "the deleted container is resurrected, a real change"
    );
    assert!(
        matches!(d.get(b"note"), Some(Element::Map(_))),
        "the container resurrects live at its key"
    );
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).unwrap();
    assert_eq!(back.encode_state(), bytes, "re-encode is canonical");
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

// --- rename onto a live container (LWW displacement) ---

/// Build a doc holding a live container at `dst` (lower stamp) and a scalar leaf
/// at `src` (higher stamp), returning the doc and the container handle. A rename
/// of `src` onto `dst` lands the higher-stamped leaf in the slot, evicting the
/// container — which the migration must DISPLACE, exactly as the op seam's renamed
/// leaf op would through `Map::set`.
fn container_then_leaf(build: impl FnOnce(&mut Document)) -> Document {
    let mut d = doc();
    build(&mut d);
    // The leaf's higher stamp lands after the container's create.
    d.transact(|tx| tx.set(b"src", Scalar::Int(99)));
    d
}

#[test]
fn a_leaf_renamed_onto_a_live_map_displaces_it() {
    let mut d = container_then_leaf(|d| {
        d.transact(|tx| {
            tx.map(b"dst").set(b"inner", Scalar::Int(7));
        });
    });
    let container = match d.get(b"dst") {
        Some(Element::Map(m)) => m,
        _ => panic!("expected the live map at dst"),
    };
    assert!(!container.borrow().is_displaced());
    assert!(d.migrate_leaf_slots(rename(b"src", b"dst")));
    assert!(
        matches!(d.get(b"dst"), Some(Element::Scalar(Scalar::Int(99)))),
        "the higher-stamped leaf won the slot"
    );
    assert!(
        container.borrow().is_displaced(),
        "the evicted map must be displaced, matching the op seam"
    );
}

#[test]
fn a_leaf_renamed_onto_a_live_list_displaces_it() {
    let mut d = container_then_leaf(|d| {
        d.transact(|tx| {
            tx.list(b"dst").insert(0, Scalar::Int(7));
        });
    });
    let container = match d.get(b"dst") {
        Some(Element::List(l)) => l,
        _ => panic!("expected the live list at dst"),
    };
    assert!(!container.borrow().is_displaced());
    assert!(d.migrate_leaf_slots(rename(b"src", b"dst")));
    assert!(matches!(
        d.get(b"dst"),
        Some(Element::Scalar(Scalar::Int(99)))
    ));
    assert!(
        container.borrow().is_displaced(),
        "the evicted list must be displaced, matching the op seam"
    );
}

#[test]
fn a_leaf_renamed_onto_a_live_text_displaces_it() {
    let mut d = container_then_leaf(|d| {
        d.transact(|tx| {
            tx.text(b"dst").insert(0, "hi");
        });
    });
    let container = match d.get(b"dst") {
        Some(Element::Text(t)) => t,
        _ => panic!("expected the live text at dst"),
    };
    assert!(!container.borrow().is_displaced());
    assert!(d.migrate_leaf_slots(rename(b"src", b"dst")));
    assert!(matches!(
        d.get(b"dst"),
        Some(Element::Scalar(Scalar::Int(99)))
    ));
    assert!(
        container.borrow().is_displaced(),
        "the evicted text must be displaced, matching the op seam"
    );
}

#[test]
fn a_leaf_renamed_onto_a_live_container_converges_with_the_op_seam() {
    // Snapshot seam: the migration renames the leaf onto the container's key.
    let mut snap = doc();
    snap.transact(|tx| {
        tx.map(b"dst").set(b"inner", Scalar::Int(7));
    });
    snap.transact(|tx| tx.set(b"src", Scalar::Int(99)));
    let container = match snap.get(b"dst") {
        Some(Element::Map(m)) => m,
        _ => panic!("expected the live map at dst"),
    };
    assert!(snap.migrate_leaf_slots(rename(b"src", b"dst")));
    assert!(
        container.borrow().is_displaced(),
        "the evicted map is displaced on the snapshot seam"
    );

    // Op seam: the renamed leaf op lands at dst and `Map::set` displaces the
    // container. Same ops, same order, same client — so the two seams project the
    // exact same snapshot bytes.
    let mut op = doc();
    op.transact(|tx| {
        tx.map(b"dst").set(b"inner", Scalar::Int(7));
    });
    op.transact(|tx| tx.set(b"dst", Scalar::Int(99)));
    assert_eq!(
        snap.encode_state(),
        op.encode_state(),
        "the snapshot and op seams converge"
    );

    // An op targeting the evicted container lands in it on the snapshot seam,
    // exactly as on the op seam — a displaced container is retained, so the write
    // is held there hidden rather than dropped or buffered, and the two seams stay
    // converged instead of one of them keeping what the other let go.
    let mut probe = doc();
    probe.transact(|tx| {
        tx.map(b"dst").set(b"inner", Scalar::Int(7));
    });
    // Pad the clock so the probe op's id is unseen by both seams (whose ops top out
    // below it), then it routes rather than dedups.
    probe.transact(|tx| tx.set(b"pad", Scalar::Int(0)));
    probe.transact(|tx| tx.set(b"pad2", Scalar::Int(0)));
    let late = probe.transact(|tx| {
        tx.map(b"dst").set(b"late", Scalar::Int(1));
    });
    for o in &late {
        snap.apply(o);
        op.apply(o);
    }
    assert!(
        container.borrow().get(b"late").is_some(),
        "the retained container takes the write that targets it"
    );
    assert_eq!(
        snap.encode_state(),
        op.encode_state(),
        "the seams stay converged under an op targeting the evicted container"
    );
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

// --- persisted container create-stamp (STATE_VERSION 10) ---

#[test]
fn a_deleted_container_tombstone_round_trips_its_create_stamp() {
    // The create-stamp a deleted container retains must survive encode/decode, or a
    // re-decoded snapshot could no longer resurrect it. A round-trip is canonical,
    // and the re-decoded tombstone still re-keys faithfully.
    let mut d = doc();
    d.transact(|tx| {
        tx.map(b"note").set(b"inner", Scalar::Int(7));
    });
    d.transact(|tx| tx.delete(b"note"));
    let bytes = d.encode_state();
    let mut back = Document::decode_state(&bytes).unwrap();
    assert_eq!(back.encode_state(), bytes, "the create-stamp round-trips");
    // The retained stamp still drives a faithful re-key after the round-trip.
    assert!(back.migrate_leaf_slots(rename(b"note", b"renamed")));
    assert!(
        matches!(back.get(b"note"), Some(Element::Map(_))),
        "the re-decoded tombstone resurrects at the old key"
    );
    assert!(back.get(b"renamed").is_none());
}

#[test]
fn a_stale_state_version_is_rejected() {
    // Pre-release, a snapshot at any version but the current one is refused hard —
    // no dual-decode path silently misreads a v9 container tombstone.
    let mut d = doc();
    d.transact(|tx| tx.set(b"a", Scalar::Int(1)));
    let mut bytes = d.encode_state();
    assert_eq!(bytes[0], 15, "the current state version");
    bytes[0] = 10;
    assert!(
        Document::decode_state(&bytes).is_err(),
        "a stale version is rejected, not migrated"
    );
}

#[test]
fn only_a_slot_that_retains_a_create_pays_the_extra_bytes() {
    // The retained create identity is a per-slot cost, and only on the two slot
    // shapes that have one to spell out: a tombstone over a create, and a live leaf
    // that outranks one. A key no container create ever named pays nothing new, and
    // neither does a live container, which is its own create.
    //
    // Deleting a leaf vs an (empty) container at the same key, both one tombstone
    // slot, isolates the delta: the create identity (one stamp = u64 lamport +
    // 16-byte client + 1-byte offset flag = 25 bytes, plus a 1-byte kind tag) plus
    // the pre-existing per-container costs a leaf never had — the child-map registry
    // entry (id + zero-slot count = 20 bytes) and its parent link (child id + parent
    // id = 32 bytes).
    let leaf_tomb = {
        let mut d = doc();
        d.transact(|tx| tx.set(b"k", Scalar::Int(1)));
        d.transact(|tx| tx.delete(b"k"));
        d.encode_state()
    };
    let container_tomb = {
        let mut d = doc();
        d.transact(|tx| {
            tx.map(b"k");
        });
        d.transact(|tx| tx.delete(b"k"));
        d.encode_state()
    };
    const CREATE_IDENTITY: usize = 25 + 1;
    const CHILD_MAP_REGISTRY: usize = 16 + 4;
    const PARENT_LINK: usize = 16 + 16;
    assert_eq!(
        container_tomb.len() - leaf_tomb.len(),
        CREATE_IDENTITY + CHILD_MAP_REGISTRY + PARENT_LINK,
        "of the delta over a leaf tombstone, only the create identity is this change's tax"
    );

    // The same tax, and only it, on a live leaf that shadows a create: a scalar at a
    // plain key against the same scalar at a key a container create sits under. The
    // baseline takes two writes as well, so the two differ only in what the first
    // one put at the key.
    let leaf = {
        let mut d = doc();
        d.transact(|tx| tx.set(b"k", Scalar::Int(0)));
        d.transact(|tx| tx.set(b"k", Scalar::Int(1)));
        d.encode_state()
    };
    let shadowed_leaf = {
        let mut d = doc();
        d.transact(|tx| {
            tx.map(b"k");
        });
        d.transact(|tx| tx.set(b"k", Scalar::Int(1)));
        d.encode_state()
    };
    assert_eq!(
        shadowed_leaf.len() - leaf.len(),
        CREATE_IDENTITY + CHILD_MAP_REGISTRY + PARENT_LINK,
        "a live leaf pays the create identity only where it shadows a create"
    );

    // A live container spells its create out through its own slot stamp and kind, so
    // it carries no second copy: one write putting a map at a key against one write
    // putting a scalar there differ by the slot body and the container's registry
    // entries, and by nothing for the identity.
    let one_leaf_write = {
        let mut d = doc();
        d.transact(|tx| tx.set(b"k", Scalar::Int(1)));
        d.encode_state()
    };
    let one_container_write = {
        let mut d = doc();
        d.transact(|tx| {
            tx.map(b"k");
        });
        d.encode_state()
    };
    const MAP_SLOT_BODY: usize = 1 + 16; // the slot's kind tag and the child's id
    const INT_SLOT_BODY: usize = 1 + 1 + 8; // the slot's kind tag, the scalar tag, the int
    assert_eq!(
        one_container_write.len() + INT_SLOT_BODY,
        one_leaf_write.len() + MAP_SLOT_BODY + CHILD_MAP_REGISTRY + PARENT_LINK,
        "a live container pays no create identity of its own"
    );
}
