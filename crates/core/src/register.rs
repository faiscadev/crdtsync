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
        Self {
            id,
            value,
            stamp,
            displaced: Cell::new(false),
        }
    }

    pub fn id(&self) -> ElementId {
        self.id
    }

    pub fn read(&self) -> &Scalar {
        &self.value
    }

    /// LWW write: takes effect iff `stamp` strictly beats the current stamp.
    pub fn set(&mut self, value: Scalar, stamp: Stamp) {
        if stamp > self.stamp {
            self.value = value;
            self.stamp = stamp;
        }
    }

    pub fn merge(&mut self, other: &Self) {
        if other.stamp > self.stamp {
            self.value = other.value.clone();
            self.stamp = other.stamp;
        }
    }

    pub fn deep_clone(&self) -> Self {
        Self {
            id: self.id,
            value: self.value.clone(),
            stamp: self.stamp,
            displaced: Cell::new(false),
        }
    }

    pub fn displace(&self) {
        self.displaced.set(true);
    }

    pub fn reinstate(&self) {
        self.displaced.set(false);
    }

    pub fn is_displaced(&self) -> bool {
        self.displaced.get()
    }
}
