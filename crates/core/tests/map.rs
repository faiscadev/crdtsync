use crdtsync_core::{Counter, Element, ElementId, ElementKind, Map, Register, Scalar};
use std::cell::RefCell;
use std::rc::Rc;

mod common;
use common::{assert_scalar, cid, default_id, eid, stmp};

fn fresh() -> Map {
    Map::new(default_id())
}
fn ei(n: i64) -> Element {
    Element::Scalar(Scalar::Int(n))
}
fn es(s: &str) -> Element {
    Element::Scalar(Scalar::Bytes(s.as_bytes().to_vec()))
}
fn counter(id: ElementId) -> Rc<RefCell<Counter>> {
    Rc::new(RefCell::new(Counter::new(id)))
}

// --- local set / get (scalar slots) ---

#[test]
fn empty_get_returns_none() {
    assert!(fresh().get(b"missing").is_none());
}

#[test]
fn set_then_get() {
    let mut m = fresh();
    m.set(b"k", ei(42), stmp(1, 1));
    assert_scalar(&m.get(b"k").unwrap(), Scalar::Int(42));
}

#[test]
fn set_overwrites_with_newer_stamp() {
    let mut m = fresh();
    m.set(b"k", ei(10), stmp(1, 1));
    m.set(b"k", ei(20), stmp(2, 1));
    assert_scalar(&m.get(b"k").unwrap(), Scalar::Int(20));
}

#[test]
fn set_lower_stamp_ignored() {
    let mut m = fresh();
    m.set(b"k", ei(20), stmp(5, 1));
    m.set(b"k", ei(10), stmp(3, 1));
    assert_scalar(&m.get(b"k").unwrap(), Scalar::Int(20));
}

#[test]
fn set_equal_lamport_higher_client_wins() {
    let mut m = fresh();
    m.set(b"k", ei(10), stmp(5, 1));
    m.set(b"k", ei(20), stmp(5, 2));
    assert_scalar(&m.get(b"k").unwrap(), Scalar::Int(20));
}

#[test]
fn set_equal_lamport_lower_client_ignored() {
    let mut m = fresh();
    m.set(b"k", ei(20), stmp(5, 2));
    m.set(b"k", ei(10), stmp(5, 1));
    assert_scalar(&m.get(b"k").unwrap(), Scalar::Int(20));
}

#[test]
fn set_same_stamp_idempotent() {
    let mut m = fresh();
    m.set(b"k", ei(42), stmp(5, 1));
    m.set(b"k", ei(42), stmp(5, 1));
    assert_scalar(&m.get(b"k").unwrap(), Scalar::Int(42));
}

#[test]
fn set_can_change_value_kind() {
    let mut m = fresh();
    m.set(b"k", ei(42), stmp(1, 1));
    m.set(b"k", es("hi"), stmp(2, 1));
    assert_scalar(&m.get(b"k").unwrap(), Scalar::Bytes(b"hi".to_vec()));
}

#[test]
fn distinct_keys_are_independent() {
    let mut m = fresh();
    m.set(b"a", ei(1), stmp(1, 1));
    m.set(b"b", ei(2), stmp(1, 1));
    assert_scalar(&m.get(b"a").unwrap(), Scalar::Int(1));
    assert_scalar(&m.get(b"b").unwrap(), Scalar::Int(2));
}

#[test]
fn keys_with_embedded_nul_are_distinct() {
    let mut m = fresh();
    m.set(&[0x01, 0x00, 0x02], ei(1), stmp(1, 1));
    m.set(&[0x01, 0x00, 0x03], ei(2), stmp(1, 1));
    assert_scalar(&m.get(&[0x01, 0x00, 0x02]).unwrap(), Scalar::Int(1));
    assert_scalar(&m.get(&[0x01, 0x00, 0x03]).unwrap(), Scalar::Int(2));
}

// --- delete / tombstones ---

#[test]
fn delete_makes_get_none() {
    let mut m = fresh();
    m.set(b"k", ei(42), stmp(1, 1));
    m.delete(b"k", stmp(2, 1));
    assert!(m.get(b"k").is_none());
}

#[test]
fn delete_with_lower_stamp_ignored() {
    let mut m = fresh();
    m.set(b"k", ei(42), stmp(5, 1));
    m.delete(b"k", stmp(3, 1));
    assert_scalar(&m.get(b"k").unwrap(), Scalar::Int(42));
}

#[test]
fn set_after_delete_higher_resurrects() {
    let mut m = fresh();
    m.set(b"k", ei(10), stmp(1, 1));
    m.delete(b"k", stmp(2, 1));
    m.set(b"k", ei(20), stmp(3, 1));
    assert_scalar(&m.get(b"k").unwrap(), Scalar::Int(20));
}

#[test]
fn set_after_delete_lower_ignored() {
    let mut m = fresh();
    m.set(b"k", ei(10), stmp(1, 1));
    m.delete(b"k", stmp(5, 1));
    m.set(b"k", ei(20), stmp(3, 1));
    assert!(m.get(b"k").is_none());
}

#[test]
fn set_vs_delete_higher_stamp_wins_delete() {
    let mut m = fresh();
    m.set(b"k", ei(10), stmp(1, 1));
    m.delete(b"k", stmp(5, 1));
    assert!(m.get(b"k").is_none());
}

#[test]
fn delete_idempotent_same_stamp() {
    let mut m = fresh();
    m.set(b"k", ei(10), stmp(1, 1));
    m.delete(b"k", stmp(5, 1));
    m.delete(b"k", stmp(5, 1));
    assert!(m.get(b"k").is_none());
}

#[test]
fn delete_absent_key_installs_tombstone() {
    let mut m = fresh();
    m.delete(b"ghost", stmp(10, 1));
    m.set(b"ghost", ei(1), stmp(5, 1)); // older, loses to tombstone
    assert!(m.get(b"ghost").is_none());
}

// --- size ---

#[test]
fn size_zero_initially() {
    assert_eq!(fresh().size(), 0);
}

#[test]
fn size_counts_live_entries() {
    let mut m = fresh();
    m.set(b"a", ei(1), stmp(1, 1));
    m.set(b"b", ei(2), stmp(1, 1));
    m.set(b"c", ei(3), stmp(1, 1));
    assert_eq!(m.size(), 3);
}

#[test]
fn size_excludes_tombstones() {
    let mut m = fresh();
    m.set(b"a", ei(1), stmp(1, 1));
    m.set(b"b", ei(2), stmp(1, 1));
    m.delete(b"b", stmp(2, 1));
    assert_eq!(m.size(), 1);
}

#[test]
fn size_recovers_on_resurrect() {
    let mut m = fresh();
    m.set(b"k", ei(1), stmp(1, 1));
    m.delete(b"k", stmp(2, 1));
    assert_eq!(m.size(), 0);
    m.set(b"k", ei(2), stmp(3, 1));
    assert_eq!(m.size(), 1);
}

// --- composite slot reads ---

#[test]
fn set_counter_then_get() {
    let mut m = fresh();
    let c = counter(default_id());
    c.borrow_mut().inc(cid(1), 5);
    m.set(b"votes", Element::Counter(c.clone()), stmp(1, 1));
    match m.get(b"votes").unwrap() {
        Element::Counter(got) => {
            assert!(Rc::ptr_eq(&got, &c));
            assert_eq!(got.borrow().read(), 5);
        }
        _ => panic!("expected counter"),
    }
}

#[test]
fn set_register_then_get() {
    let mut m = fresh();
    let r = Rc::new(RefCell::new(Register::new(
        default_id(),
        Scalar::Int(7),
        stmp(1, 1),
    )));
    m.set(b"title", Element::Register(r), stmp(1, 1));
    match m.get(b"title").unwrap() {
        Element::Register(got) => assert_eq!(got.borrow().read(), &Scalar::Int(7)),
        _ => panic!("expected register"),
    }
}

#[test]
fn set_nested_map_then_get() {
    let mut outer = fresh();
    let inner = Rc::new(RefCell::new(Map::new(default_id())));
    inner.borrow_mut().set(b"a", ei(1), stmp(1, 1));
    outer.set(b"child", Element::Map(inner), stmp(1, 1));
    match outer.get(b"child").unwrap() {
        Element::Map(got) => assert_scalar(&got.borrow().get(b"a").unwrap(), Scalar::Int(1)),
        _ => panic!("expected map"),
    }
}

// --- merge (scalar slots) ---

#[test]
fn merge_disjoint_keys_unions() {
    let mut a = fresh();
    let mut b = fresh();
    a.set(b"x", ei(1), stmp(1, 1));
    b.set(b"y", ei(2), stmp(1, 2));
    a.merge(&b);
    assert_scalar(&a.get(b"x").unwrap(), Scalar::Int(1));
    assert_scalar(&a.get(b"y").unwrap(), Scalar::Int(2));
    assert_eq!(a.size(), 2);
}

#[test]
fn merge_same_key_newer_wins() {
    let mut a = fresh();
    let mut b = fresh();
    a.set(b"k", ei(10), stmp(1, 1));
    b.set(b"k", ei(20), stmp(2, 2));
    a.merge(&b);
    assert_scalar(&a.get(b"k").unwrap(), Scalar::Int(20));
}

#[test]
fn merge_src_older_loses() {
    let mut a = fresh();
    let mut b = fresh();
    a.set(b"k", ei(20), stmp(5, 1));
    b.set(b"k", ei(10), stmp(2, 2));
    a.merge(&b);
    assert_scalar(&a.get(b"k").unwrap(), Scalar::Int(20));
}

#[test]
fn merge_delete_beats_older_set() {
    let mut a = fresh();
    let mut b = fresh();
    a.set(b"k", ei(10), stmp(1, 1));
    b.delete(b"k", stmp(5, 1));
    a.merge(&b);
    assert!(a.get(b"k").is_none());
}

#[test]
fn merge_set_beats_older_delete() {
    let mut a = fresh();
    let mut b = fresh();
    a.delete(b"k", stmp(1, 1));
    b.set(b"k", ei(42), stmp(5, 1));
    a.merge(&b);
    assert_scalar(&a.get(b"k").unwrap(), Scalar::Int(42));
}

#[test]
fn merge_commutative() {
    let mut a1 = fresh();
    let mut b1 = fresh();
    a1.set(b"k", ei(10), stmp(5, 1));
    b1.set(b"k", ei(20), stmp(5, 2));
    a1.merge(&b1);

    let a2 = fresh();
    let mut b2 = fresh();
    {
        let mut a2m = a2;
        a2m.set(b"k", ei(10), stmp(5, 1));
        b2.set(b"k", ei(20), stmp(5, 2));
        b2.merge(&a2m);
    }
    assert_scalar(&a1.get(b"k").unwrap(), Scalar::Int(20));
    assert_scalar(&b2.get(b"k").unwrap(), Scalar::Int(20));
}

#[test]
fn merge_idempotent() {
    let mut a = fresh();
    let mut b = fresh();
    a.set(b"k", ei(10), stmp(1, 1));
    b.set(b"k", ei(20), stmp(2, 1));
    a.merge(&b);
    a.merge(&b);
    assert_scalar(&a.get(b"k").unwrap(), Scalar::Int(20));
}

#[test]
fn merge_associative() {
    let mut a = fresh();
    let mut b = fresh();
    let mut c = fresh();
    a.set(b"k", ei(10), stmp(1, 1));
    b.set(b"k", ei(20), stmp(2, 1));
    c.set(b"k", ei(30), stmp(3, 1));
    a.merge(&b);
    a.merge(&c);

    let mut a2 = fresh();
    let mut b2 = fresh();
    let mut c2 = fresh();
    a2.set(b"k", ei(10), stmp(1, 1));
    b2.set(b"k", ei(20), stmp(2, 1));
    c2.set(b"k", ei(30), stmp(3, 1));
    b2.merge(&c2);
    a2.merge(&b2);

    assert_scalar(&a.get(b"k").unwrap(), Scalar::Int(30));
    assert_scalar(&a2.get(b"k").unwrap(), Scalar::Int(30));
}

#[test]
fn merge_does_not_mutate_src() {
    let mut a = fresh();
    let mut b = fresh();
    a.set(b"k", ei(99), stmp(10, 1));
    b.set(b"k", ei(7), stmp(1, 1));
    a.merge(&b);
    assert_scalar(&b.get(b"k").unwrap(), Scalar::Int(7));
}

#[test]
fn merge_takes_winning_string() {
    let mut a = fresh();
    let mut b = fresh();
    a.set(b"k", ei(0), stmp(1, 1));
    b.set(b"k", es("hello"), stmp(5, 1));
    a.merge(&b);
    drop(b);
    assert_scalar(&a.get(b"k").unwrap(), Scalar::Bytes(b"hello".to_vec()));
}

#[test]
fn merge_preserves_tombstone_against_older_set() {
    let mut a = fresh();
    let mut b = fresh();
    a.delete(b"k", stmp(5, 1));
    b.set(b"k", ei(10), stmp(2, 1));
    a.merge(&b);
    assert!(a.get(b"k").is_none());
}

// --- recursive merge: same kind + matching id recurses regardless of stamp ---

#[test]
fn merge_same_kind_counter_recurses() {
    let mut dst = fresh();
    let dc = counter(default_id());
    dc.borrow_mut().inc(cid(1), 5);
    dst.set(b"votes", Element::Counter(dc.clone()), stmp(1, 1));

    let mut src = fresh();
    let sc = counter(default_id());
    sc.borrow_mut().inc(cid(2), 3);
    src.set(b"votes", Element::Counter(sc), stmp(10, 1));

    dst.merge(&src);
    match dst.get(b"votes").unwrap() {
        Element::Counter(got) => {
            assert!(Rc::ptr_eq(&got, &dc)); // kept its own handle
            assert_eq!(got.borrow().read(), 8);
        }
        _ => panic!("expected counter"),
    }
}

#[test]
fn merge_same_kind_register_recurses() {
    let mut dst = fresh();
    let dr = Rc::new(RefCell::new(Register::new(
        default_id(),
        Scalar::Int(10),
        stmp(1, 1),
    )));
    dst.set(b"title", Element::Register(dr.clone()), stmp(1, 1));

    let mut src = fresh();
    let sr = Rc::new(RefCell::new(Register::new(
        default_id(),
        Scalar::Int(20),
        stmp(5, 1),
    )));
    src.set(b"title", Element::Register(sr), stmp(1, 1));

    dst.merge(&src);
    assert_eq!(dr.borrow().read(), &Scalar::Int(20));
}

#[test]
fn merge_same_kind_counter_does_not_mutate_src() {
    let mut dst = fresh();
    let dc = counter(default_id());
    dc.borrow_mut().inc(cid(1), 5);
    dst.set(b"votes", Element::Counter(dc), stmp(1, 1));

    let mut src = fresh();
    let sc = counter(default_id());
    sc.borrow_mut().inc(cid(2), 3);
    src.set(b"votes", Element::Counter(sc.clone()), stmp(1, 1));

    dst.merge(&src);
    assert_eq!(sc.borrow().read(), 3);
}

// Recursive merge advances the slot stamp to max(dst, src).
#[test]
fn merge_same_kind_counter_advances_slot_stamp() {
    let mut dst = fresh();
    let dc = counter(default_id());
    dc.borrow_mut().inc(cid(1), 5);
    dst.set(b"votes", Element::Counter(dc), stmp(1, 1));

    let mut src = fresh();
    let sc = counter(default_id());
    sc.borrow_mut().inc(cid(2), 3);
    src.set(b"votes", Element::Counter(sc), stmp(10, 1));

    dst.merge(&src);
    // Below src's slot stamp (10) but above dst's old (1): must be rejected.
    dst.set(b"votes", ei(99), stmp(5, 1));
    match dst.get(b"votes").unwrap() {
        Element::Counter(got) => assert_eq!(got.borrow().read(), 8),
        _ => panic!("expected counter"),
    }
}

// --- type-flip via LWW: loser displaced ---

#[test]
fn set_composite_displaces_scalar() {
    let mut m = fresh();
    m.set(b"score", ei(42), stmp(1, 1));
    let c = counter(default_id());
    m.set(b"score", Element::Counter(c.clone()), stmp(5, 1));
    match m.get(b"score").unwrap() {
        Element::Counter(got) => assert!(Rc::ptr_eq(&got, &c)),
        _ => panic!("expected counter"),
    }
}

#[test]
fn set_scalar_displaces_composite() {
    let mut m = fresh();
    let c = counter(default_id());
    m.set(b"score", Element::Counter(c), stmp(1, 1));
    m.set(b"score", ei(42), stmp(5, 1));
    assert_scalar(&m.get(b"score").unwrap(), Scalar::Int(42));
}

// A holder that kept its handle observes the displaced flag and outlives evict.
#[test]
fn evicted_composite_is_displaced_and_outlives_via_held_handle() {
    let mut m = fresh();
    let c = counter(default_id());
    c.borrow_mut().inc(cid(1), 5);
    m.set(b"score", Element::Counter(c.clone()), stmp(1, 1));
    m.set(b"score", ei(42), stmp(5, 1)); // evicts the counter
    assert!(c.borrow().is_displaced());
    assert_eq!(c.borrow().read(), 5);
}

#[test]
fn delete_composite_displaces_it() {
    let mut m = fresh();
    let c = counter(default_id());
    m.set(b"score", Element::Counter(c.clone()), stmp(1, 1));
    m.delete(b"score", stmp(5, 1));
    assert!(m.get(b"score").is_none());
    assert!(c.borrow().is_displaced());
}

// Re-setting the exact handle already installed must NOT displace it.
#[test]
fn set_same_composite_newer_stamp_keeps_it_live() {
    let mut m = fresh();
    let c = counter(default_id());
    c.borrow_mut().inc(cid(1), 5);
    m.set(b"votes", Element::Counter(c.clone()), stmp(1, 1));
    m.set(b"votes", Element::Counter(c.clone()), stmp(5, 1));
    assert!(!c.borrow().is_displaced());
    match m.get(b"votes").unwrap() {
        Element::Counter(got) => {
            assert!(Rc::ptr_eq(&got, &c));
            assert_eq!(got.borrow().read(), 5);
        }
        _ => panic!("expected counter"),
    }
}

// --- cross-replica composite LWW: clone winner ---

#[test]
fn merge_composite_src_wins_into_empty_slot_clones() {
    let mut dst = fresh();
    let mut src = fresh();
    let sc = counter(default_id());
    sc.borrow_mut().inc(cid(1), 5);
    src.set(b"votes", Element::Counter(sc.clone()), stmp(5, 1));

    dst.merge(&src);
    match dst.get(b"votes").unwrap() {
        Element::Counter(got) => {
            assert!(!Rc::ptr_eq(&got, &sc)); // dst owns a clone
            assert_eq!(got.borrow().read(), 5);
        }
        _ => panic!("expected counter"),
    }
}

#[test]
fn merge_kind_mismatch_clones_winner() {
    let mut dst = fresh();
    let dc = counter(default_id());
    dc.borrow_mut().inc(cid(1), 5);
    dst.set(b"x", Element::Counter(dc), stmp(1, 1));

    let mut src = fresh();
    let sr = Rc::new(RefCell::new(Register::new(
        default_id(),
        Scalar::Int(42),
        stmp(10, 1),
    )));
    src.set(b"x", Element::Register(sr.clone()), stmp(10, 1));

    dst.merge(&src);
    match dst.get(b"x").unwrap() {
        Element::Register(got) => {
            assert!(!Rc::ptr_eq(&got, &sr));
            assert_eq!(got.borrow().read(), &Scalar::Int(42));
        }
        _ => panic!("expected register"),
    }
}

// Same kind, DIFFERENT ids: LWW, not recursive union.
#[test]
fn merge_same_kind_different_id_uses_lww_not_recurse() {
    let mut dst = fresh();
    let dc = counter(eid(7, 1));
    dc.borrow_mut().inc(cid(1), 5);
    dst.set(b"votes", Element::Counter(dc), stmp(1, 1));

    let mut src = fresh();
    let sc = counter(eid(7, 2));
    sc.borrow_mut().inc(cid(2), 3);
    src.set(b"votes", Element::Counter(sc.clone()), stmp(5, 1));

    dst.merge(&src);
    match dst.get(b"votes").unwrap() {
        Element::Counter(got) => {
            assert_eq!(got.borrow().read(), 3); // clone of src, not unioned 8
            assert_eq!(got.borrow().id(), eid(7, 2));
            assert!(!Rc::ptr_eq(&got, &sc));
        }
        _ => panic!("expected counter"),
    }
}

// --- get-or-create helpers ---

#[test]
fn helper_counter_creates_and_installs() {
    let mut m = fresh();
    let c = m.counter(b"votes", stmp(1, 1));
    match m.get(b"votes").unwrap() {
        Element::Counter(got) => assert!(Rc::ptr_eq(&got, &c)),
        _ => panic!("expected counter"),
    }
}

#[test]
fn helper_counter_returns_same_on_repeat() {
    let mut m = fresh();
    let first = m.counter(b"votes", stmp(1, 1));
    let second = m.counter(b"votes", stmp(2, 1));
    assert!(Rc::ptr_eq(&first, &second));
}

#[test]
fn helper_register_creates_and_installs() {
    let mut m = fresh();
    let r = m.register(b"title", Scalar::Int(42), stmp(1, 1));
    assert_eq!(r.borrow().read(), &Scalar::Int(42));
    match m.get(b"title").unwrap() {
        Element::Register(got) => assert!(Rc::ptr_eq(&got, &r)),
        _ => panic!("expected register"),
    }
}

#[test]
fn helper_register_returns_same_on_repeat_ignoring_seed() {
    let mut m = fresh();
    let first = m.register(b"title", Scalar::Int(1), stmp(1, 1));
    let second = m.register(b"title", Scalar::Int(999), stmp(2, 1));
    assert!(Rc::ptr_eq(&first, &second));
    assert_eq!(first.borrow().read(), &Scalar::Int(1));
}

#[test]
fn helper_map_creates_and_installs() {
    let mut outer = fresh();
    let child = outer.map(b"child", stmp(1, 1));
    match outer.get(b"child").unwrap() {
        Element::Map(got) => assert!(Rc::ptr_eq(&got, &child)),
        _ => panic!("expected map"),
    }
}

#[test]
fn helper_map_returns_same_on_repeat() {
    let mut outer = fresh();
    let first = outer.map(b"child", stmp(1, 1));
    let second = outer.map(b"child", stmp(2, 1));
    assert!(Rc::ptr_eq(&first, &second));
}

#[test]
fn helper_list_creates_and_installs() {
    let mut m = fresh();
    let l = m.list(b"items", stmp(1, 1));
    match m.get(b"items").unwrap() {
        Element::List(got) => assert!(Rc::ptr_eq(&got, &l)),
        _ => panic!("expected list"),
    }
}

#[test]
fn helper_list_returns_same_on_repeat() {
    let mut m = fresh();
    let first = m.list(b"items", stmp(1, 1));
    let second = m.list(b"items", stmp(2, 1));
    assert!(Rc::ptr_eq(&first, &second));
}

#[test]
fn resetting_same_list_handle_advances_stamp_not_displaced() {
    // Re-setting the exact installed handle at a higher stamp advances the slot
    // stamp only; the still-installed sequence must not be flagged displaced.
    let mut m = fresh();
    let l = m.list(b"items", stmp(1, 1));
    m.set(b"items", Element::List(Rc::clone(&l)), stmp(5, 1));
    assert!(!l.borrow().is_displaced());
    match m.get(b"items").unwrap() {
        Element::List(got) => assert!(Rc::ptr_eq(&got, &l)),
        _ => panic!("expected list"),
    }
}

#[test]
fn helper_list_derives_id() {
    let parent = eid(3, 9);
    let mut m = Map::new(parent);
    let l = m.list(b"items", stmp(1, 1));
    let expected = ElementId::derive(parent, b"items", ElementKind::List);
    assert_eq!(l.borrow().id(), expected);
}

#[test]
fn helper_text_creates_and_installs() {
    let mut m = fresh();
    let t = m.text(b"body", stmp(1, 1));
    match m.get(b"body").unwrap() {
        Element::Text(got) => assert!(Rc::ptr_eq(&got, &t)),
        _ => panic!("expected text"),
    }
}

#[test]
fn helper_text_returns_same_on_repeat() {
    let mut m = fresh();
    let first = m.text(b"body", stmp(1, 1));
    let second = m.text(b"body", stmp(2, 1));
    assert!(Rc::ptr_eq(&first, &second));
}

#[test]
fn helper_text_derives_id() {
    let parent = eid(3, 9);
    let mut m = Map::new(parent);
    let t = m.text(b"body", stmp(1, 1));
    let expected = ElementId::derive(parent, b"body", ElementKind::Text);
    assert_eq!(t.borrow().id(), expected);
}

// Winning helper over a different-kind slot flips the kind; the evicted handle
// is displaced (observed via a retained clone).
#[test]
fn helper_register_after_counter_flips_kind() {
    let mut m = fresh();
    let c = m.counter(b"score", stmp(1, 1));
    let r = m.register(b"score", Scalar::Int(42), stmp(5, 1));
    match m.get(b"score").unwrap() {
        Element::Register(got) => assert!(Rc::ptr_eq(&got, &r)),
        _ => panic!("expected register"),
    }
    assert!(c.borrow().is_displaced());
}

// Losing helper returns a detached, born-displaced handle; slot is untouched.
#[test]
fn helper_losing_stamp_returns_detached_displaced() {
    let mut m = fresh();
    let c = m.counter(b"score", stmp(10, 1));
    let r = m.register(b"score", Scalar::Int(7), stmp(5, 1));
    assert_eq!(r.borrow().read(), &Scalar::Int(7));
    assert!(r.borrow().is_displaced());
    match m.get(b"score").unwrap() {
        Element::Counter(got) => assert!(Rc::ptr_eq(&got, &c)),
        _ => panic!("slot should still hold the counter"),
    }
}

#[test]
fn helper_counter_losing_stamp_detached_displaced() {
    let mut m = fresh();
    m.register(b"score", Scalar::Int(1), stmp(10, 1));
    let c = m.counter(b"score", stmp(5, 1));
    assert!(c.borrow().is_displaced());
    assert_eq!(m.get(b"score").unwrap().kind(), ElementKind::Register);
}

#[test]
fn helper_map_losing_stamp_detached_displaced() {
    let mut m = fresh();
    m.register(b"child", Scalar::Int(1), stmp(10, 1));
    let child = m.map(b"child", stmp(5, 1));
    assert!(child.borrow().is_displaced());
    assert_eq!(m.get(b"child").unwrap().kind(), ElementKind::Register);
}

// Cross-replica: same key + kind + derived id -> recursive merge.
#[test]
fn helper_counter_cross_replica_merge_recurses() {
    let mut dst = fresh();
    let mut src = fresh();
    let dc = dst.counter(b"votes", stmp(1, 1));
    let sc = src.counter(b"votes", stmp(1, 2));
    dc.borrow_mut().inc(cid(1), 5);
    sc.borrow_mut().inc(cid(2), 3);
    dst.merge(&src);
    match dst.get(b"votes").unwrap() {
        Element::Counter(got) => assert_eq!(got.borrow().read(), 8),
        _ => panic!("expected counter"),
    }
}

#[test]
fn helper_list_cross_replica_merge_recurses() {
    let mut dst = fresh();
    let mut src = fresh();
    let dl = dst.list(b"items", stmp(1, 1));
    let sl = src.list(b"items", stmp(1, 2));
    dl.borrow_mut().insert(0, ei(1), stmp(2, 1));
    sl.borrow_mut().insert(0, ei(2), stmp(2, 2));
    dst.merge(&src);
    match dst.get(b"items").unwrap() {
        Element::List(got) => assert_eq!(got.borrow().len(), 2),
        _ => panic!("expected list"),
    }
}

// --- helper id derivation ---

#[test]
fn helper_counter_derives_id() {
    let parent = eid(7, 42);
    let mut m = Map::new(parent);
    let c = m.counter(b"votes", stmp(1, 1));
    let expected = ElementId::derive(parent, b"votes", ElementKind::Counter);
    assert_eq!(c.borrow().id(), expected);
}

#[test]
fn helpers_converge_across_replicas() {
    let parent = eid(7, 42);
    let mut a = Map::new(parent);
    let mut b = Map::new(parent);
    let ca = a.counter(b"votes", stmp(1, 1));
    let cb = b.counter(b"votes", stmp(1, 2));
    assert_eq!(ca.borrow().id(), cb.borrow().id());
}

#[test]
fn helpers_same_key_different_kind_distinct_ids() {
    let mut m = Map::new(eid(7, 42));
    let c = m.counter(b"x", stmp(1, 1));
    let r = m.register(b"x", Scalar::Int(0), stmp(1, 1)); // loses, detached
    assert_ne!(c.borrow().id(), r.borrow().id());
}

// --- deep_clone ---

#[test]
fn clone_empty_map_is_empty() {
    let src = fresh();
    let clone = src.deep_clone();
    assert_eq!(clone.size(), 0);
}

#[test]
fn clone_preserves_scalar_slots() {
    let mut src = fresh();
    src.set(b"a", ei(1), stmp(1, 1));
    src.set(b"b", es("hi"), stmp(1, 1));
    let clone = src.deep_clone();
    assert_eq!(clone.size(), 2);
    assert_scalar(&clone.get(b"a").unwrap(), Scalar::Int(1));
    assert_scalar(&clone.get(b"b").unwrap(), Scalar::Bytes(b"hi".to_vec()));
}

#[test]
fn clone_survives_src_drop() {
    let mut src = fresh();
    src.set(b"k", es("hello"), stmp(1, 1));
    let clone = src.deep_clone();
    drop(src);
    assert_scalar(&clone.get(b"k").unwrap(), Scalar::Bytes(b"hello".to_vec()));
}

#[test]
fn clone_recurses_into_composite_slots() {
    let mut src = fresh();
    let sc = counter(default_id());
    sc.borrow_mut().inc(cid(1), 5);
    src.set(b"votes", Element::Counter(sc.clone()), stmp(1, 1));
    let clone = src.deep_clone();
    match clone.get(b"votes").unwrap() {
        Element::Counter(got) => {
            assert!(!Rc::ptr_eq(&got, &sc));
            assert_eq!(got.borrow().read(), 5);
        }
        _ => panic!("expected counter"),
    }
}

#[test]
fn clone_preserves_tombstones() {
    let mut src = fresh();
    src.set(b"k", ei(1), stmp(1, 1));
    src.delete(b"k", stmp(5, 1));
    let mut clone = src.deep_clone();
    clone.set(b"k", ei(99), stmp(3, 1)); // older, must lose to tombstone
    assert!(clone.get(b"k").is_none());
}

#[test]
fn clone_independent_of_src() {
    let mut src = fresh();
    src.set(b"k", ei(1), stmp(1, 1));
    let clone = src.deep_clone();
    src.set(b"k", ei(99), stmp(5, 1));
    src.set(b"new", ei(7), stmp(1, 1));
    assert_scalar(&clone.get(b"k").unwrap(), Scalar::Int(1));
    assert!(clone.get(b"new").is_none());
}

// --- map lifecycle ---

#[test]
fn map_displace_sets_flag() {
    let m = fresh();
    assert!(!m.is_displaced());
    m.displace();
    assert!(m.is_displaced());
}
