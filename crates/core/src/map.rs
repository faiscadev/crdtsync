//! Map — LWW map with tombstones, keyed on raw bytes, Element-valued.
//!
//! Share semantics: an accepted `set` of a composite takes a slot-owned handle;
//! callers keep their own. Eviction (winning set/delete, merge LWW-replace)
//! displaces the loser. `get` and the installing helper path return a slot
//! handle; the helper's losing path returns a detached, displaced one.

use crate::counter::Counter;
use crate::element::Element;
use crate::elementid::{ElementId, ElementKind};
use crate::list::List;
use crate::register::Register;
use crate::scalar::Scalar;
use crate::stamp::Stamp;
use crate::text::Text;
use std::cell::{Cell, RefCell};
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

/// Two Elements holding the exact same composite handle.
fn same_handle(a: &Element, b: &Element) -> bool {
    match (a, b) {
        (Element::Counter(x), Element::Counter(y)) => Rc::ptr_eq(x, y),
        (Element::Register(x), Element::Register(y)) => Rc::ptr_eq(x, y),
        (Element::Map(x), Element::Map(y)) => Rc::ptr_eq(x, y),
        _ => false,
    }
}

/// Both composites of the same kind (Scalar excluded).
fn same_composite_kind(a: &Element, b: &Element) -> bool {
    matches!(
        (a, b),
        (Element::Counter(_), Element::Counter(_))
            | (Element::Register(_), Element::Register(_))
            | (Element::Map(_), Element::Map(_))
            | (Element::List(_), Element::List(_))
            | (Element::Text(_), Element::Text(_))
    )
}

impl Map {
    pub fn new(id: ElementId) -> Self {
        Self {
            id,
            slots: HashMap::new(),
            displaced: Cell::new(false),
        }
    }

    pub fn id(&self) -> ElementId {
        self.id
    }

    pub fn size(&self) -> usize {
        self.slots.values().filter(|e| !e.tombstone).count()
    }

    /// Slot handle for a live key, else `None`.
    pub fn get(&self, key: &[u8]) -> Option<Element> {
        self.slots
            .get(key)
            .filter(|e| !e.tombstone)
            .and_then(|e| e.value.clone())
    }

    pub fn set(&mut self, key: &[u8], value: Element, stamp: Stamp) {
        match self.slots.get(key) {
            Some(e) if !stamp.gt(&e.stamp) => {}
            Some(e)
                if !e.tombstone && e.value.as_ref().is_some_and(|old| same_handle(old, &value)) =>
            {
                // Re-setting the exact installed handle: advance the stamp only,
                // so a still-installed handle is never flagged displaced.
                self.slots.get_mut(key).unwrap().stamp = stamp;
            }
            _ => {
                self.evict(key);
                self.slots.insert(
                    key.to_vec(),
                    Entry {
                        stamp,
                        value: Some(value),
                        tombstone: false,
                    },
                );
            }
        }
    }

    pub fn delete(&mut self, key: &[u8], stamp: Stamp) {
        if self.slots.get(key).is_some_and(|e| !stamp.gt(&e.stamp)) {
            return;
        }
        self.evict(key);
        self.slots.insert(
            key.to_vec(),
            Entry {
                stamp,
                value: None,
                tombstone: true,
            },
        );
    }

    /// Displace the live composite (if any) currently at `key` — it is about to
    /// be replaced or tombstoned and is no longer installed.
    fn evict(&self, key: &[u8]) {
        if let Some(e) = self.slots.get(key) {
            if !e.tombstone {
                if let Some(old) = &e.value {
                    old.displace();
                }
            }
        }
    }

    pub fn merge(&mut self, src: &Self) {
        for (key, se) in &src.slots {
            // Same key, both live composites of the same kind AND same id ->
            // recurse in place (they are the same logical element).
            let recurse = self.slots.get(key).is_some_and(|de| {
                !de.tombstone
                    && !se.tombstone
                    && matches!((&de.value, &se.value), (Some(dv), Some(sv))
                        if same_composite_kind(dv, sv) && dv.id() == sv.id())
            });

            if recurse {
                if let (Some(dv), Some(sv)) = (
                    self.slots.get(key).and_then(|e| e.value.as_ref()),
                    se.value.as_ref(),
                ) {
                    dv.merge(sv);
                }
                let cur = self.slots.get(key).unwrap().stamp;
                if se.stamp.gt(&cur) {
                    self.slots.get_mut(key).unwrap().stamp = se.stamp;
                }
                continue;
            }

            // LWW: src wins iff strictly greater (or dst absent).
            if self
                .slots
                .get(key)
                .is_some_and(|de| !se.stamp.gt(&de.stamp))
            {
                continue;
            }
            self.evict(key);
            self.slots.insert(
                key.clone(),
                Entry {
                    stamp: se.stamp,
                    value: se.value.as_ref().map(|v| v.deep_clone()),
                    tombstone: se.tombstone,
                },
            );
        }
    }

    /// Get-or-create a Counter at `key`. Returns the existing live handle, or
    /// installs a fresh one (borrow) if the stamp wins, or a detached
    /// born-displaced handle if it loses.
    pub fn counter(&mut self, key: &[u8], stamp: Stamp) -> Rc<RefCell<Counter>> {
        if let Some(Element::Counter(c)) = self.live_value(key) {
            return c;
        }
        let id = ElementId::derive(self.id, key, ElementKind::Counter);
        let fresh = Rc::new(RefCell::new(Counter::new(id)));
        if self.wins(key, stamp) {
            self.evict(key);
            self.install(key, Element::Counter(Rc::clone(&fresh)), stamp);
        } else {
            fresh.borrow().displace();
        }
        fresh
    }

    pub fn register(&mut self, key: &[u8], seed: Scalar, stamp: Stamp) -> Rc<RefCell<Register>> {
        if let Some(Element::Register(r)) = self.live_value(key) {
            return r;
        }
        let id = ElementId::derive(self.id, key, ElementKind::Register);
        let fresh = Rc::new(RefCell::new(Register::new(id, seed, stamp)));
        if self.wins(key, stamp) {
            self.evict(key);
            self.install(key, Element::Register(Rc::clone(&fresh)), stamp);
        } else {
            fresh.borrow().displace();
        }
        fresh
    }

    pub fn map(&mut self, key: &[u8], stamp: Stamp) -> Rc<RefCell<Self>> {
        if let Some(Element::Map(m)) = self.live_value(key) {
            return m;
        }
        let id = ElementId::derive(self.id, key, ElementKind::Map);
        let fresh = Rc::new(RefCell::new(Self::new(id)));
        if self.wins(key, stamp) {
            self.evict(key);
            self.install(key, Element::Map(Rc::clone(&fresh)), stamp);
        } else {
            fresh.borrow().displace();
        }
        fresh
    }

    pub fn list(&mut self, key: &[u8], stamp: Stamp) -> Rc<RefCell<List>> {
        if let Some(Element::List(l)) = self.live_value(key) {
            return l;
        }
        let id = ElementId::derive(self.id, key, ElementKind::List);
        let fresh = Rc::new(RefCell::new(List::new(id)));
        if self.wins(key, stamp) {
            self.evict(key);
            self.install(key, Element::List(Rc::clone(&fresh)), stamp);
        } else {
            fresh.borrow().displace();
        }
        fresh
    }

    pub fn text(&mut self, key: &[u8], stamp: Stamp) -> Rc<RefCell<Text>> {
        if let Some(Element::Text(t)) = self.live_value(key) {
            return t;
        }
        let id = ElementId::derive(self.id, key, ElementKind::Text);
        let fresh = Rc::new(RefCell::new(Text::new(id)));
        if self.wins(key, stamp) {
            self.evict(key);
            self.install(key, Element::Text(Rc::clone(&fresh)), stamp);
        } else {
            fresh.borrow().displace();
        }
        fresh
    }

    fn live_value(&self, key: &[u8]) -> Option<Element> {
        self.slots
            .get(key)
            .filter(|e| !e.tombstone)
            .and_then(|e| e.value.clone())
    }

    fn wins(&self, key: &[u8], stamp: Stamp) -> bool {
        self.slots.get(key).map_or(true, |e| stamp.gt(&e.stamp))
    }

    fn install(&mut self, key: &[u8], value: Element, stamp: Stamp) {
        self.slots.insert(
            key.to_vec(),
            Entry {
                stamp,
                value: Some(value),
                tombstone: false,
            },
        );
    }

    pub fn deep_clone(&self) -> Self {
        let slots = self
            .slots
            .iter()
            .map(|(k, e)| {
                (
                    k.clone(),
                    Entry {
                        stamp: e.stamp,
                        value: e.value.as_ref().map(|v| v.deep_clone()),
                        tombstone: e.tombstone,
                    },
                )
            })
            .collect();
        Self {
            id: self.id,
            slots,
            displaced: Cell::new(false),
        }
    }

    pub fn displace(&self) {
        self.displaced.set(true);
    }

    pub fn is_displaced(&self) -> bool {
        self.displaced.get()
    }
}
