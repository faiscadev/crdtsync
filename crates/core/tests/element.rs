use crdtsync_core::{Counter, Element, ElementKind, Map, Register, Scalar};
use std::cell::RefCell;
use std::rc::Rc;

mod common;
use common::{assert_scalar, cid, default_id, eid, stmp};

fn counter(id: crdtsync_core::ElementId) -> Rc<RefCell<Counter>> {
    Rc::new(RefCell::new(Counter::new(id)))
}
fn register(id: crdtsync_core::ElementId, v: Scalar) -> Rc<RefCell<Register>> {
    Rc::new(RefCell::new(Register::new(id, v, stmp(1, 1))))
}
fn map(id: crdtsync_core::ElementId) -> Rc<RefCell<Map>> {
    Rc::new(RefCell::new(Map::new(id)))
}

// --- constructors set kind ---

#[test]
fn scalar_kind() {
    assert_eq!(Element::Scalar(Scalar::Int(42)).kind(), ElementKind::Scalar);
}

#[test]
fn register_kind() {
    let e = Element::Register(register(default_id(), Scalar::Int(1)));
    assert_eq!(e.kind(), ElementKind::Register);
}

#[test]
fn counter_kind() {
    assert_eq!(
        Element::Counter(counter(default_id())).kind(),
        ElementKind::Counter
    );
}

#[test]
fn map_kind() {
    assert_eq!(Element::Map(map(default_id())).kind(), ElementKind::Map);
}

// --- id reads the composite's id ---

#[test]
fn id_register() {
    let id = eid(7, 42);
    assert_eq!(Element::Register(register(id, Scalar::Int(1))).id(), id);
}

#[test]
fn id_counter() {
    let id = eid(7, 42);
    assert_eq!(Element::Counter(counter(id)).id(), id);
}

#[test]
fn id_map() {
    let id = eid(7, 42);
    assert_eq!(Element::Map(map(id)).id(), id);
}

// --- merge dispatches by kind ---

#[test]
fn merge_register_takes_newer() {
    let dst = Rc::new(RefCell::new(Register::new(
        default_id(),
        Scalar::Int(10),
        stmp(1, 1),
    )));
    let src = Rc::new(RefCell::new(Register::new(
        default_id(),
        Scalar::Int(20),
        stmp(5, 1),
    )));
    Element::Register(dst.clone()).merge(&Element::Register(src));
    assert_eq!(dst.borrow().read(), &Scalar::Int(20));
}

#[test]
fn merge_counter_unions() {
    let dst = counter(default_id());
    let src = counter(default_id());
    dst.borrow_mut().inc(cid(1), 5);
    src.borrow_mut().inc(cid(2), 3);
    Element::Counter(dst.clone()).merge(&Element::Counter(src));
    assert_eq!(dst.borrow().read(), 8);
}

#[test]
fn merge_map_takes_newer_slot() {
    let dst = map(default_id());
    let src = map(default_id());
    dst.borrow_mut()
        .set(b"k", Element::Scalar(Scalar::Int(10)), stmp(1, 1));
    src.borrow_mut()
        .set(b"k", Element::Scalar(Scalar::Int(20)), stmp(5, 1));
    Element::Map(dst.clone()).merge(&Element::Map(src));
    assert_scalar(&dst.borrow().get(b"k").unwrap(), Scalar::Int(20));
}

#[test]
fn merge_does_not_mutate_src() {
    let dst = counter(default_id());
    let src = counter(default_id());
    src.borrow_mut().inc(cid(1), 3);
    Element::Counter(dst).merge(&Element::Counter(src.clone()));
    assert_eq!(src.borrow().read(), 3);
}

// --- deep_clone ---

#[test]
fn clone_scalar_preserves_value() {
    let clone = Element::Scalar(Scalar::Bytes(b"hello".to_vec())).deep_clone();
    assert_scalar(&clone, Scalar::Bytes(b"hello".to_vec()));
}

#[test]
fn clone_counter_is_independent() {
    let src = counter(default_id());
    src.borrow_mut().inc(cid(1), 5);
    let clone = Element::Counter(src.clone()).deep_clone();
    src.borrow_mut().inc(cid(1), 100);
    match clone {
        Element::Counter(rc) => {
            assert!(!Rc::ptr_eq(&rc, &src));
            assert_eq!(rc.borrow().read(), 5);
        }
        _ => panic!("expected counter"),
    }
}

#[test]
fn clone_register_deep_copies_value() {
    let src = register(eid(7, 42), Scalar::Int(42));
    let clone = Element::Register(src.clone()).deep_clone();
    match clone {
        Element::Register(rc) => {
            assert!(!Rc::ptr_eq(&rc, &src));
            assert_eq!(rc.borrow().read(), &Scalar::Int(42));
            assert_eq!(rc.borrow().id(), eid(7, 42));
        }
        _ => panic!("expected register"),
    }
}

#[test]
fn clone_map_recurses() {
    let src = map(default_id());
    src.borrow_mut()
        .set(b"a", Element::Scalar(Scalar::Int(1)), stmp(1, 1));
    let clone = Element::Map(src.clone()).deep_clone();
    match clone {
        Element::Map(rc) => {
            assert!(!Rc::ptr_eq(&rc, &src));
            assert_scalar(&rc.borrow().get(b"a").unwrap(), Scalar::Int(1));
        }
        _ => panic!("expected map"),
    }
}

// --- displacement forwarding ---

#[test]
fn displace_forwards_to_counter() {
    let c = counter(default_id());
    let e = Element::Counter(c.clone());
    assert!(!e.is_displaced());
    e.displace();
    assert!(e.is_displaced());
    assert!(c.borrow().is_displaced());
}

#[test]
fn displace_forwards_to_register() {
    let r = register(default_id(), Scalar::Int(1));
    Element::Register(r.clone()).displace();
    assert!(r.borrow().is_displaced());
}

#[test]
fn scalar_displace_is_noop() {
    let e = Element::Scalar(Scalar::Int(7));
    e.displace();
    assert!(!e.is_displaced());
}

#[test]
fn clone_of_displaced_composite_is_not_displaced() {
    let src = counter(default_id());
    Element::Counter(src.clone()).displace();
    let clone = Element::Counter(src).deep_clone();
    assert!(!clone.is_displaced());
}
