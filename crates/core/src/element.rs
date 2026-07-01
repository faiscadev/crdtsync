//! Element — the tagged value a Map slot holds: an inline Scalar, or a shared
//! handle to a composite (Register / Counter / Map). Lifecycle and merge
//! forward to the underlying composite; Scalar is a leaf with neither.

use crate::counter::Counter;
use crate::elementid::ElementId;
use crate::map::Map;
use crate::register::Register;
use crate::scalar::Scalar;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Clone)]
pub enum Element {
    Scalar(Scalar),
    Register(Rc<RefCell<Register>>),
    Counter(Rc<RefCell<Counter>>),
    Map(Rc<RefCell<Map>>),
}

pub use crate::elementid::ElementKind;

impl Element {
    pub fn kind(&self) -> ElementKind {
        match self {
            Element::Scalar(_) => ElementKind::Scalar,
            Element::Register(_) => ElementKind::Register,
            Element::Counter(_) => ElementKind::Counter,
            Element::Map(_) => ElementKind::Map,
        }
    }

    /// Id of the underlying composite. Scalars have no id.
    pub fn id(&self) -> ElementId {
        match self {
            Element::Scalar(_) => panic!("scalar elements have no id"),
            Element::Register(r) => r.borrow().id(),
            Element::Counter(c) => c.borrow().id(),
            Element::Map(m) => m.borrow().id(),
        }
    }

    /// Merge `src` into the composite behind `self` (same kind required).
    pub fn merge(&self, src: &Element) {
        match (self, src) {
            (Element::Register(d), Element::Register(s)) => d.borrow_mut().merge(&s.borrow()),
            (Element::Counter(d), Element::Counter(s)) => d.borrow_mut().merge(&s.borrow()),
            (Element::Map(d), Element::Map(s)) => d.borrow_mut().merge(&s.borrow()),
            _ => panic!("element merge: scalar or kind mismatch"),
        }
    }

    /// Independent deep copy: fresh composite handles, not shared with `self`.
    pub fn deep_clone(&self) -> Element {
        match self {
            Element::Scalar(s) => Element::Scalar(s.clone()),
            Element::Register(r) => {
                Element::Register(Rc::new(RefCell::new(r.borrow().deep_clone())))
            }
            Element::Counter(c) => Element::Counter(Rc::new(RefCell::new(c.borrow().deep_clone()))),
            Element::Map(m) => Element::Map(Rc::new(RefCell::new(m.borrow().deep_clone()))),
        }
    }

    pub fn displace(&self) {
        match self {
            Element::Scalar(_) => {}
            Element::Register(r) => r.borrow().displace(),
            Element::Counter(c) => c.borrow().displace(),
            Element::Map(m) => m.borrow().displace(),
        }
    }

    pub fn is_displaced(&self) -> bool {
        match self {
            Element::Scalar(_) => false,
            Element::Register(r) => r.borrow().is_displaced(),
            Element::Counter(c) => c.borrow().is_displaced(),
            Element::Map(m) => m.borrow().is_displaced(),
        }
    }
}
