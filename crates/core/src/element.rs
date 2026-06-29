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
        todo!()
    }

    /// Id of the underlying composite. Scalars have no id.
    pub fn id(&self) -> ElementId {
        todo!()
    }

    /// Merge `src` into the composite behind `self` (same kind required).
    pub fn merge(&self, src: &Element) {
        let _ = src;
        todo!()
    }

    /// Independent deep copy: fresh composite handles, not shared with `self`.
    pub fn deep_clone(&self) -> Element {
        todo!()
    }

    pub fn displace(&self) {
        todo!()
    }

    pub fn is_displaced(&self) -> bool {
        todo!()
    }
}
