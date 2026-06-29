//! Map — LWW map with tombstones, keyed on raw bytes, Element-valued.
//!
//! Share semantics: an accepted `set` of a composite takes a slot-owned handle;
//! callers keep their own. Eviction (winning set/delete, merge LWW-replace)
//! displaces the loser. `get` and the installing helper path return a slot
//! handle; the helper's losing path returns a detached, displaced one.

use crate::counter::Counter;
use crate::element::Element;
use crate::elementid::ElementId;
use crate::register::Register;
use crate::scalar::Scalar;
use crate::stamp::Stamp;
use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;

struct Entry {
    stamp: Stamp,
    /// `None` exactly when `tombstone` is true.
    value: Option<Element>,
    tombstone: bool,
}

pub struct Map {
    id: ElementId,
    slots: HashMap<Vec<u8>, Entry>,
    displaced: Cell<bool>,
}

impl Map {
    pub fn new(id: ElementId) -> Self {
        let _ = id;
        todo!()
    }

    pub fn id(&self) -> ElementId {
        todo!()
    }

    pub fn size(&self) -> usize {
        todo!()
    }

    /// Slot handle for a live key, else `None`.
    pub fn get(&self, key: &[u8]) -> Option<Element> {
        let _ = key;
        todo!()
    }

    pub fn set(&mut self, key: &[u8], value: Element, stamp: Stamp) {
        let _ = (key, value, stamp);
        todo!()
    }

    pub fn delete(&mut self, key: &[u8], stamp: Stamp) {
        let _ = (key, stamp);
        todo!()
    }

    pub fn merge(&mut self, src: &Map) {
        let _ = src;
        todo!()
    }

    /// Get-or-create a Counter at `key`. Installs and returns a slot handle
    /// when the slot is empty/different and `stamp` wins; otherwise returns the
    /// existing handle, or a detached displaced one if `stamp` loses.
    pub fn counter(&mut self, key: &[u8], stamp: Stamp) -> Rc<std::cell::RefCell<Counter>> {
        let _ = (key, stamp);
        todo!()
    }

    pub fn register(
        &mut self,
        key: &[u8],
        seed: Scalar,
        stamp: Stamp,
    ) -> Rc<std::cell::RefCell<Register>> {
        let _ = (key, seed, stamp);
        todo!()
    }

    pub fn map(&mut self, key: &[u8], stamp: Stamp) -> Rc<std::cell::RefCell<Map>> {
        let _ = (key, stamp);
        todo!()
    }

    pub fn deep_clone(&self) -> Map {
        todo!()
    }

    pub fn displace(&self) {
        todo!()
    }

    pub fn is_displaced(&self) -> bool {
        todo!()
    }
}
