//! Register — LWW single value (Scalar) with a (lamport, client) stamp. A write
//! or merge takes effect iff its stamp strictly beats the current one.

use crate::codec::{put_scalar, put_stamp, Cursor, DecodeError};
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

    /// Append this register's state — id, value, and LWW stamp — to `out`.
    pub(crate) fn encode_state_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.id.as_bytes());
        put_scalar(out, &self.value);
        put_stamp(out, &self.stamp);
    }

    /// Read a register from `cur`, advancing it. The stamp comes back too, so a
    /// decoded register still resolves LWW against later writes.
    pub(crate) fn decode_state_from(cur: &mut Cursor) -> Result<Register, DecodeError> {
        let id = cur.element_id()?;
        let value = cur.scalar()?;
        let stamp = cur.stamp()?;
        Ok(Register {
            id,
            value,
            stamp,
            displaced: Cell::new(false),
        })
    }

    /// Serialize this register's state to self-contained bytes.
    pub fn encode_state(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_state_into(&mut out);
        out
    }

    /// Read a register from a complete byte slice, rejecting trailing bytes.
    pub fn decode_state(bytes: &[u8]) -> Result<Register, DecodeError> {
        let mut cur = Cursor::new(bytes);
        let register = Register::decode_state_from(&mut cur)?;
        if cur.at_end() {
            Ok(register)
        } else {
            Err(DecodeError::TrailingBytes)
        }
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

    /// A copy carrying the same value and LWW stamp under a new id — for moving a
    /// register to the id its slot key now derives when a migration renames the
    /// field, matching the id the renamed `RegisterSet` would derive at the new
    /// key.
    pub fn rehomed(&self, id: ElementId) -> Self {
        Self {
            id,
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
