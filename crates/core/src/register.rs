//! Register — LWW single value (Scalar) with a (lamport, client) stamp. A write
//! or merge takes effect iff its stamp strictly beats the current one.

use crate::elementid::ElementId;
use crate::scalar::Scalar;
use crate::stamp::Stamp;
use std::cell::Cell;

pub struct Register {
    id: ElementId,
    value: Scalar,
    stamp: Stamp,
    displaced: Cell<bool>,
}

impl Register {
    pub fn new(id: ElementId, value: Scalar, stamp: Stamp) -> Self {
        let _ = (id, value, stamp);
        todo!()
    }

    pub fn id(&self) -> ElementId {
        todo!()
    }

    pub fn read(&self) -> &Scalar {
        todo!()
    }

    /// LWW write: takes effect iff `stamp` strictly beats the current stamp.
    pub fn set(&mut self, value: Scalar, stamp: Stamp) {
        let _ = (value, stamp);
        todo!()
    }

    pub fn merge(&mut self, other: &Register) {
        let _ = other;
        todo!()
    }

    pub fn deep_clone(&self) -> Register {
        todo!()
    }

    pub fn displace(&self) {
        todo!()
    }

    pub fn is_displaced(&self) -> bool {
        todo!()
    }
}
