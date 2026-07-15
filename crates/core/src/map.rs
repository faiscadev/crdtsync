//! Map — LWW map with tombstones, keyed on raw bytes, Element-valued.
//!
//! Share semantics: an accepted `set` of a composite takes a slot-owned handle;
//! callers keep their own. Eviction (winning set/delete, merge LWW-replace)
//! displaces the loser. `get` and the installing helper path return a slot
//! handle; the helper's losing path returns a detached, displaced one.

use crate::codec::{len_u32, put_bytes, put_stamp, put_u32, put_u8, Cursor, DecodeError};
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

/// Slot-value tags in a map snapshot. Leaves (scalar, register) are inline; a
/// composite is a kind-tagged reference to its child's id, resolved from the
/// document registry on decode.
const SLOT_SCALAR: u8 = 0;
const SLOT_REGISTER: u8 = 1;
const SLOT_COUNTER: u8 = 2;
const SLOT_MAP: u8 = 3;
const SLOT_LIST: u8 = 4;
const SLOT_TEXT: u8 = 5;
const SLOT_XML_ELEMENT: u8 = 6;
const SLOT_XML_FRAGMENT: u8 = 7;

/// Slot presence tags: a slot is live, a leaf tombstone, or a deleted-container
/// tombstone carrying the create-stamp its snapshot migration resurrects at.
const SLOT_LIVE: u8 = 0;
const SLOT_LEAF_TOMB: u8 = 1;
const SLOT_CONTAINER_TOMB: u8 = 2;

/// A map read from a snapshot: its id and slots, with composite children still
/// unresolved references into the document's by-id registries.
pub(crate) struct DecodedMap {
    pub(crate) id: ElementId,
    pub(crate) slots: Vec<DecodedSlot>,
}

/// One decoded slot before its composite reference is wired to a handle.
pub(crate) struct DecodedSlot {
    pub(crate) key: Vec<u8>,
    pub(crate) stamp: Stamp,
    pub(crate) tombstone: bool,
    pub(crate) value: Option<SlotValue>,
    /// The retained (stamp, kind) of a deleted-container tombstone, `None`
    /// otherwise — carried through so a re-decoded snapshot can still resurrect.
    pub(crate) deleted: Option<(Stamp, ElementKind)>,
}

/// A decoded slot value: a leaf is self-contained; a composite is a kind-tagged
/// reference resolved from the document's by-id registry.
pub(crate) enum SlotValue {
    Scalar(Scalar),
    Register(Register),
    Ref(ElementKind, ElementId),
}

fn put_ref(out: &mut Vec<u8>, tag: u8, id: ElementId) {
    put_u8(out, tag);
    out.extend_from_slice(&id.as_bytes());
}

struct Entry {
    stamp: Stamp,
    /// `None` exactly when `tombstone` is true.
    value: Option<Element>,
    tombstone: bool,
    /// The container (map / list / text) that held this slot just before it was
    /// tombstoned — retained across the delete so a snapshot migration can
    /// resurrect the create at its old key and re-key the delete faithfully,
    /// matching the op seam (which carries the container-create verbatim in the
    /// log). `Some` only on a deleted-container tombstone; `None` on every live
    /// slot and every leaf tombstone, which pay no extra bytes.
    deleted: Option<DeletedContainer>,
}

/// The identity a snapshot migration resurrects a deleted container by: the stamp
/// its create landed at, plus which key-derived kind (map / list / text) it was,
/// so a key that hosted more than one container kind resurrects the exact one the
/// last delete tombstoned. XML kinds derive ids by node, not key, so they are not
/// resurrectable here and never recorded.
#[derive(Clone, Copy)]
struct DeletedContainer {
    stamp: Stamp,
    kind: ElementKind,
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
        (Element::List(x), Element::List(y)) => Rc::ptr_eq(x, y),
        (Element::Text(x), Element::Text(y)) => Rc::ptr_eq(x, y),
        (Element::XmlElement(x), Element::XmlElement(y)) => Rc::ptr_eq(x, y),
        (Element::XmlFragment(x), Element::XmlFragment(y)) => Rc::ptr_eq(x, y),
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
            | (Element::XmlElement(_), Element::XmlElement(_))
            | (Element::XmlFragment(_), Element::XmlFragment(_))
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

    /// Every slot key, live or tombstoned — the set a snapshot migration walks.
    pub(crate) fn slot_keys(&self) -> Vec<Vec<u8>> {
        self.slots.keys().cloned().collect()
    }

    /// Whether `key` holds a live container (map / list / text) — the slots a
    /// migration carries verbatim, never dropping or re-keying.
    pub(crate) fn slot_is_live_container(&self, key: &[u8]) -> bool {
        self.slots
            .get(key)
            .and_then(|e| e.value.as_ref())
            .is_some_and(Element::is_container)
    }

    /// Whether `key`'s slot is a tombstone (deleted, no live value). A migration
    /// consults this to tell a deleted container's slot from a live one.
    pub(crate) fn slot_is_tombstone(&self, key: &[u8]) -> bool {
        self.slots.get(key).is_some_and(|e| e.tombstone)
    }

    /// The retained (create-stamp, kind) of the deleted container that held `key`,
    /// if the slot is a container tombstone — what a snapshot migration resurrects
    /// the create by. `None` for a live slot or a leaf tombstone.
    pub(crate) fn slot_deleted_container(&self, key: &[u8]) -> Option<(Stamp, ElementKind)> {
        self.slots
            .get(key)
            .and_then(|e| e.deleted)
            .map(|d| (d.stamp, d.kind))
    }

    /// Remove the slot at `key`, returning its `(stamp, value, tombstone)`.
    pub(crate) fn take_slot(&mut self, key: &[u8]) -> Option<(Stamp, Option<Element>, bool)> {
        self.slots
            .remove(key)
            .map(|e| (e.stamp, e.value, e.tombstone))
    }

    /// Install a migrated slot at `key`, keeping the later stamp if one is already
    /// there — the same LWW rule a concurrent write resolves by, so re-keying onto
    /// an occupied slot converges with the op seam.
    pub(crate) fn put_slot_lww(
        &mut self,
        key: Vec<u8>,
        stamp: Stamp,
        value: Option<Element>,
        tombstone: bool,
    ) {
        if self.slots.get(&key).is_some_and(|e| !stamp.gt(&e.stamp)) {
            return;
        }
        self.slots.insert(
            key,
            Entry {
                stamp,
                value,
                tombstone,
                // A migration re-homes a leaf body, or resurrects a container as a
                // fresh live slot / a plain tombstone at the destination key —
                // neither retains a deleted-container identity under the new key.
                deleted: None,
            },
        );
    }

    /// Append this map's state — id and every slot, live or tombstoned — to
    /// `out`. Slots are ordered by key so equal states encode identically. A
    /// composite slot stores a kind-tagged reference to its child's id for the
    /// document registry to resolve; a scalar or register is inline.
    pub(crate) fn encode_state_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.id.as_bytes());
        let mut slots: Vec<(&Vec<u8>, &Entry)> = self.slots.iter().collect();
        slots.sort_by(|a, b| a.0.cmp(b.0));
        put_u32(out, len_u32(slots.len()));
        for (key, entry) in slots {
            put_bytes(out, key);
            put_stamp(out, &entry.stamp);
            // The slot tag: `0` live, `1` a leaf tombstone, `2` a deleted-container
            // tombstone carrying its create-stamp and kind. Only tag `2` costs the
            // extra stamp + kind byte; a live slot and a leaf tombstone encode
            // exactly as before.
            match (entry.tombstone, entry.deleted) {
                (false, _) => put_u8(out, SLOT_LIVE),
                (true, None) => {
                    put_u8(out, SLOT_LEAF_TOMB);
                    continue;
                }
                (true, Some(deleted)) => {
                    put_u8(out, SLOT_CONTAINER_TOMB);
                    put_stamp(out, &deleted.stamp);
                    put_u8(out, deleted.kind as u8);
                    continue;
                }
            }
            match entry.value.as_ref().expect("a live slot holds a value") {
                Element::Scalar(s) => {
                    put_u8(out, SLOT_SCALAR);
                    s.encode_state_into(out);
                }
                Element::Register(r) => {
                    put_u8(out, SLOT_REGISTER);
                    r.borrow().encode_state_into(out);
                }
                Element::Counter(c) => put_ref(out, SLOT_COUNTER, c.borrow().id()),
                Element::Map(m) => put_ref(out, SLOT_MAP, m.borrow().id()),
                Element::List(l) => put_ref(out, SLOT_LIST, l.borrow().id()),
                Element::Text(t) => put_ref(out, SLOT_TEXT, t.borrow().id()),
                Element::XmlElement(x) => put_ref(out, SLOT_XML_ELEMENT, x.borrow().id()),
                Element::XmlFragment(f) => put_ref(out, SLOT_XML_FRAGMENT, f.borrow().id()),
            }
        }
    }

    /// Read a map's id and slots from `cur`, advancing it. Composite slots come
    /// back as unresolved references for the document to wire against its
    /// registries once every container is materialised.
    pub(crate) fn decode_state_from(cur: &mut Cursor) -> Result<DecodedMap, DecodeError> {
        let id = cur.element_id()?;
        let count = cur.u32()?;
        let mut slots = Vec::with_capacity((count as usize).min(1024));
        for _ in 0..count {
            let key = cur.bytes()?;
            let stamp = cur.stamp()?;
            let (tombstone, deleted) = match cur.u8()? {
                SLOT_LIVE => (false, None),
                SLOT_LEAF_TOMB => (true, None),
                SLOT_CONTAINER_TOMB => {
                    let stamp = cur.stamp()?;
                    let kind = match ElementKind::from_tag(cur.u8()?) {
                        Some(k @ (ElementKind::Map | ElementKind::List | ElementKind::Text)) => k,
                        _ => {
                            return Err(DecodeError::BadTag {
                                what: "deleted-container kind",
                                tag: 0,
                            })
                        }
                    };
                    (true, Some((stamp, kind)))
                }
                tag => {
                    return Err(DecodeError::BadTag {
                        what: "map slot tombstone",
                        tag,
                    })
                }
            };
            let value = if tombstone {
                None
            } else {
                Some(match cur.u8()? {
                    SLOT_SCALAR => SlotValue::Scalar(Scalar::decode_state_from(cur)?),
                    SLOT_REGISTER => SlotValue::Register(Register::decode_state_from(cur)?),
                    SLOT_COUNTER => SlotValue::Ref(ElementKind::Counter, cur.element_id()?),
                    SLOT_MAP => SlotValue::Ref(ElementKind::Map, cur.element_id()?),
                    SLOT_LIST => SlotValue::Ref(ElementKind::List, cur.element_id()?),
                    SLOT_TEXT => SlotValue::Ref(ElementKind::Text, cur.element_id()?),
                    SLOT_XML_ELEMENT => SlotValue::Ref(ElementKind::XmlElement, cur.element_id()?),
                    SLOT_XML_FRAGMENT => {
                        SlotValue::Ref(ElementKind::XmlFragment, cur.element_id()?)
                    }
                    tag => {
                        return Err(DecodeError::BadTag {
                            what: "map slot value",
                            tag,
                        })
                    }
                })
            };
            slots.push(DecodedSlot {
                key,
                stamp,
                tombstone,
                value,
                deleted,
            });
        }
        Ok(DecodedMap { id, slots })
    }

    /// Install a slot decoded from a snapshot, reporting whether it displaced a
    /// prior entry — a repeated key in the stream is non-canonical.
    pub(crate) fn insert_decoded(
        &mut self,
        key: Vec<u8>,
        stamp: Stamp,
        value: Option<Element>,
        tombstone: bool,
        deleted: Option<(Stamp, ElementKind)>,
    ) -> bool {
        self.slots
            .insert(
                key,
                Entry {
                    stamp,
                    value,
                    tombstone,
                    deleted: deleted.map(|(stamp, kind)| DeletedContainer { stamp, kind }),
                },
            )
            .is_some()
    }

    /// The live slot values, for recomputing displacement after a decode.
    pub(crate) fn live_values(&self) -> impl Iterator<Item = Element> + '_ {
        self.slots
            .values()
            .filter(|e| !e.tombstone)
            .filter_map(|e| e.value.clone())
    }

    pub fn size(&self) -> usize {
        self.slots.values().filter(|e| !e.tombstone).count()
    }

    /// The live slot keys, sorted, for deterministic traversal — the order a
    /// structural diff or an ordered walk reports slots in.
    pub fn keys(&self) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = self
            .slots
            .iter()
            .filter(|(_, e)| !e.tombstone)
            .map(|(k, _)| k.clone())
            .collect();
        keys.sort();
        keys
    }

    /// Live `(key, handle)` slots, sorted by key — a single-pass ordered walk
    /// that avoids a re-lookup per key.
    pub(crate) fn entries(&self) -> Vec<(Vec<u8>, Element)> {
        let mut entries: Vec<(Vec<u8>, Element)> = self
            .slots
            .iter()
            .filter(|(_, e)| !e.tombstone)
            .filter_map(|(k, e)| e.value.clone().map(|v| (k.clone(), v)))
            .collect();
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        entries
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
                        deleted: None,
                    },
                );
            }
        }
    }

    pub fn delete(&mut self, key: &[u8], stamp: Stamp) {
        if self.slots.get(key).is_some_and(|e| !stamp.gt(&e.stamp)) {
            return;
        }
        // Retain the identity a snapshot migration needs to resurrect a deleted
        // container at its old key: capture the live container's (current stamp,
        // kind) when this delete tombstones a key-resurrectable one (map / list /
        // text — an XML kind derives its id by node, not key, so it is not
        // resurrectable here); a re-delete inherits the identity already held. A
        // leaf slot retains nothing.
        let deleted = match self.slots.get(key) {
            Some(e) if !e.tombstone => e.value.as_ref().and_then(|v| match v.kind() {
                kind @ (ElementKind::Map | ElementKind::List | ElementKind::Text) => {
                    Some(DeletedContainer {
                        stamp: e.stamp,
                        kind,
                    })
                }
                _ => None,
            }),
            Some(e) => e.deleted,
            None => None,
        };
        self.evict(key);
        self.slots.insert(
            key.to_vec(),
            Entry {
                stamp,
                value: None,
                tombstone: true,
                deleted,
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
            // The deleted-container identity rides with the LWW winner, whichever
            // entry that is — never a mix of the two — so a merge is commutative:
            // the retained (stamp, kind) is a deterministic function of the winning
            // delete, not of the merge order. Only a tombstone carries one.
            let deleted = se.deleted.filter(|_| se.tombstone);
            self.evict(key);
            self.slots.insert(
                key.clone(),
                Entry {
                    stamp: se.stamp,
                    value: se.value.as_ref().map(|v| v.deep_clone()),
                    tombstone: se.tombstone,
                    deleted,
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
                deleted: None,
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
                        deleted: e.deleted,
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

    /// Drop every slot entry. Used at document teardown to break parent→child
    /// links so a deeply nested tree frees without recursing.
    pub fn clear(&mut self) {
        self.slots.clear();
    }

    pub fn displace(&self) {
        self.displaced.set(true);
    }

    /// Re-install a previously displaced map: it has re-won its slot as the same
    /// logical element, retaining its content.
    pub fn reinstate(&self) {
        self.displaced.set(false);
    }

    pub fn is_displaced(&self) -> bool {
        self.displaced.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClientId;

    fn stamp(lamport: u64) -> Stamp {
        Stamp {
            lamport,
            client: ClientId::from_bytes([1; 16]),
        }
    }

    fn eid(b: u8) -> ElementId {
        ElementId::from_bytes([b; 16])
    }

    #[test]
    fn merge_of_a_deleted_container_identity_is_commutative() {
        // A container tombstone (deleted identity, lower stamp) and a leaf
        // tombstone (no identity, higher stamp) at the same key. The higher stamp
        // wins in either merge direction, so its (absent) identity must survive
        // regardless of order — a create identity is never mixed across the two,
        // or the two seams would encode the same key as different slot tags.
        let mut a = Map::new(eid(1));
        a.set(
            b"k",
            Element::Map(Rc::new(RefCell::new(Map::new(eid(9))))),
            stamp(1),
        );
        a.delete(b"k", stamp(2)); // container tombstone: deleted = Some((s1, Map))
        assert!(a.slot_deleted_container(b"k").is_some());

        let mut b = Map::new(eid(1));
        b.set(b"k", Element::Scalar(Scalar::Int(1)), stamp(3));
        b.delete(b"k", stamp(4)); // leaf tombstone: deleted = None
        assert!(b.slot_deleted_container(b"k").is_none());

        let mut ab = a.deep_clone();
        ab.merge(&b);
        let mut ba = b.deep_clone();
        ba.merge(&a);
        assert_eq!(
            ab.slot_deleted_container(b"k"),
            ba.slot_deleted_container(b"k"),
            "merge order must not change the retained create identity"
        );
        assert!(
            ab.slot_deleted_container(b"k").is_none(),
            "the winning (higher-stamp) leaf tombstone carries no create identity"
        );
    }
}
