use crdtsync_core::{Counter, Element, ElementKind, List, Map, Register, Scalar, Text};
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
fn list(id: crdtsync_core::ElementId) -> Rc<RefCell<List>> {
    Rc::new(RefCell::new(List::new(id)))
}
fn text(id: crdtsync_core::ElementId) -> Rc<RefCell<Text>> {
    Rc::new(RefCell::new(Text::new(id)))
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

#[test]
fn list_kind() {
    assert_eq!(Element::List(list(default_id())).kind(), ElementKind::List);
}

#[test]
fn text_kind() {
    assert_eq!(Element::Text(text(default_id())).kind(), ElementKind::Text);
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

#[test]
fn id_list() {
    let id = eid(7, 42);
    assert_eq!(Element::List(list(id)).id(), id);
}

#[test]
fn id_text() {
    let id = eid(7, 42);
    assert_eq!(Element::Text(text(id)).id(), id);
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
fn merge_list_unions() {
    let dst = list(default_id());
    let src = list(default_id());
    dst.borrow_mut()
        .insert(0, Element::Scalar(Scalar::Int(1)), stmp(1, 1));
    src.borrow_mut()
        .insert(0, Element::Scalar(Scalar::Int(2)), stmp(1, 2));
    Element::List(dst.clone()).merge(&Element::List(src));
    assert_eq!(dst.borrow().len(), 2);
}

#[test]
fn merge_text_converges() {
    let dst = text(default_id());
    let src = text(default_id());
    dst.borrow_mut().insert(0, "ABC", stmp(1, 1));
    src.borrow_mut().insert(0, "XYZ", stmp(1, 2));
    Element::Text(dst.clone()).merge(&Element::Text(src));
    assert_eq!(dst.borrow().len(), 6);
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

#[test]
fn clone_list_is_independent() {
    let src = list(default_id());
    src.borrow_mut()
        .insert(0, Element::Scalar(Scalar::Int(1)), stmp(1, 1));
    let clone = Element::List(src.clone()).deep_clone();
    src.borrow_mut()
        .insert(1, Element::Scalar(Scalar::Int(2)), stmp(2, 1));
    match clone {
        Element::List(rc) => {
            assert!(!Rc::ptr_eq(&rc, &src));
            assert_eq!(rc.borrow().len(), 1);
        }
        _ => panic!("expected list"),
    }
}

#[test]
fn clone_text_is_independent() {
    let src = text(eid(7, 42));
    src.borrow_mut().insert(0, "ab", stmp(1, 1));
    let clone = Element::Text(src.clone()).deep_clone();
    src.borrow_mut().insert(2, "c", stmp(3, 1));
    match clone {
        Element::Text(rc) => {
            assert!(!Rc::ptr_eq(&rc, &src));
            assert_eq!(rc.borrow().as_string(), "ab");
            assert_eq!(rc.borrow().id(), eid(7, 42));
        }
        _ => panic!("expected text"),
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
fn displace_forwards_to_list() {
    let l = list(default_id());
    Element::List(l.clone()).displace();
    assert!(l.borrow().is_displaced());
}

#[test]
fn displace_forwards_to_text() {
    let t = text(default_id());
    Element::Text(t.clone()).displace();
    assert!(t.borrow().is_displaced());
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
