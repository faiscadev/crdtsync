//! Element — the tagged value a Map slot holds: an inline Scalar, or a shared
//! handle to a composite (Register / Counter / Map). Lifecycle and merge
//! forward to the underlying composite; Scalar is a leaf with neither.

use crate::counter::Counter;
use crate::elementid::ElementId;
use crate::list::List;
use crate::map::Map;
use crate::register::Register;
use crate::scalar::Scalar;
use crate::text::Text;
use crate::xml::{XmlElement, XmlFragment};
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Clone)]
pub enum Element {
    Scalar(Scalar),
    Register(Rc<RefCell<Register>>),
    Counter(Rc<RefCell<Counter>>),
    Map(Rc<RefCell<Map>>),
    List(Rc<RefCell<List>>),
    Text(Rc<RefCell<Text>>),
    XmlElement(Rc<RefCell<XmlElement>>),
    XmlFragment(Rc<RefCell<XmlFragment>>),
}

pub use crate::elementid::ElementKind;

impl Element {
    pub fn kind(&self) -> ElementKind {
        match self {
            Element::Scalar(_) => ElementKind::Scalar,
            Element::Register(_) => ElementKind::Register,
            Element::Counter(_) => ElementKind::Counter,
            Element::Map(_) => ElementKind::Map,
            Element::List(_) => ElementKind::List,
            Element::Text(_) => ElementKind::Text,
            Element::XmlElement(_) => ElementKind::XmlElement,
            Element::XmlFragment(_) => ElementKind::XmlFragment,
        }
    }

    /// Whether this element is a nested container (map / list / text) — the
    /// kinds addressed by element id, whose create a migration carries verbatim.
    /// The single source of truth for the container/leaf split the snapshot and
    /// op translations share; kept exhaustive with no catch-all, mirroring
    /// [`OpKind::creates_container`](crate::op::OpKind::creates_container), so a
    /// new kind must be classified here or the crate does not compile.
    pub fn is_container(&self) -> bool {
        match self {
            Element::Map(_)
            | Element::List(_)
            | Element::Text(_)
            | Element::XmlElement(_)
            | Element::XmlFragment(_) => true,
            Element::Scalar(_) | Element::Register(_) | Element::Counter(_) => false,
        }
    }

    /// Id of the underlying composite. Scalars have no id.
    pub fn id(&self) -> ElementId {
        match self {
            Element::Scalar(_) => panic!("scalar elements have no id"),
            Element::Register(r) => r.borrow().id(),
            Element::Counter(c) => c.borrow().id(),
            Element::Map(m) => m.borrow().id(),
            Element::List(l) => l.borrow().id(),
            Element::Text(t) => t.borrow().id(),
            Element::XmlElement(x) => x.borrow().id(),
            Element::XmlFragment(f) => f.borrow().id(),
        }
    }

    /// Merge `src` into the composite behind `self` (same kind required).
    pub fn merge(&self, src: &Element) {
        match (self, src) {
            (Element::Register(d), Element::Register(s)) => d.borrow_mut().merge(&s.borrow()),
            (Element::Counter(d), Element::Counter(s)) => d.borrow_mut().merge(&s.borrow()),
            (Element::Map(d), Element::Map(s)) => d.borrow_mut().merge(&s.borrow()),
            (Element::List(d), Element::List(s)) => d.borrow_mut().merge(&s.borrow()),
            (Element::Text(d), Element::Text(s)) => d.borrow_mut().merge(&s.borrow()),
            (Element::XmlElement(d), Element::XmlElement(s)) => d.borrow_mut().merge(&s.borrow()),
            (Element::XmlFragment(d), Element::XmlFragment(s)) => d.borrow_mut().merge(&s.borrow()),
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
            Element::List(l) => Element::List(Rc::new(RefCell::new(l.borrow().deep_clone()))),
            Element::Text(t) => Element::Text(Rc::new(RefCell::new(t.borrow().deep_clone()))),
            Element::XmlElement(x) => {
                Element::XmlElement(Rc::new(RefCell::new(x.borrow().deep_clone())))
            }
            Element::XmlFragment(f) => {
                Element::XmlFragment(Rc::new(RefCell::new(f.borrow().deep_clone())))
            }
        }
    }

    pub fn displace(&self) {
        match self {
            Element::Scalar(_) => {}
            Element::Register(r) => r.borrow().displace(),
            Element::Counter(c) => c.borrow().displace(),
            Element::Map(m) => m.borrow().displace(),
            Element::List(l) => l.borrow().displace(),
            Element::Text(t) => t.borrow().displace(),
            Element::XmlElement(x) => x.borrow().displace(),
            Element::XmlFragment(f) => f.borrow().displace(),
        }
    }

    pub fn reinstate(&self) {
        match self {
            Element::Scalar(_) => {}
            Element::Register(r) => r.borrow().reinstate(),
            Element::Counter(c) => c.borrow().reinstate(),
            Element::Map(m) => m.borrow().reinstate(),
            Element::List(l) => l.borrow().reinstate(),
            Element::Text(t) => t.borrow().reinstate(),
            Element::XmlElement(x) => x.borrow().reinstate(),
            Element::XmlFragment(f) => f.borrow().reinstate(),
        }
    }

    pub fn is_displaced(&self) -> bool {
        match self {
            Element::Scalar(_) => false,
            Element::Register(r) => r.borrow().is_displaced(),
            Element::Counter(c) => c.borrow().is_displaced(),
            Element::Map(m) => m.borrow().is_displaced(),
            Element::List(l) => l.borrow().is_displaced(),
            Element::Text(t) => t.borrow().is_displaced(),
            Element::XmlElement(x) => x.borrow().is_displaced(),
            Element::XmlFragment(f) => f.borrow().is_displaced(),
        }
    }
}
