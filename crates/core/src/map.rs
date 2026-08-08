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

/// Slot presence tags. A slot is live or a tombstone, and either shape may spell
/// out the create identity of a container the key retains. The plain tags are the
/// shapes with nothing to spell: a live slot whose identity is its own (a live
/// container is its own create) or absent, and a tombstone with none.
const SLOT_LIVE: u8 = 0;
const SLOT_LEAF_TOMB: u8 = 1;
const SLOT_CONTAINER_TOMB: u8 = 2;
const SLOT_LIVE_SHADOW: u8 = 3;

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
    /// The (stamp, kind) the slot spelled out, `None` when the tag implied it or
    /// the key retains none — carried through so a re-decoded snapshot can still
    /// resurrect.
    pub(crate) container: Option<(Stamp, ElementKind)>,
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
    /// The container create this key retains — kept across a delete so a snapshot
    /// migration can resurrect the create at its old key and re-key the delete
    /// faithfully, matching the op seam (which carries the container-create
    /// verbatim in the log). `None` on a key no container create ever named.
    container: Option<ContainerCreate>,
}

/// The identity a snapshot migration resurrects a container by: the stamp its
/// create landed at, plus which kind it was, so a key that hosted more than one
/// container kind resurrects the exact one. An XML element mixes its tag in below
/// the key, so the key alone does not name it: it is recorded — it ranks against
/// the creates it wins the slot from — but resolves to no handle, and its key
/// migrates by what the registry still holds there instead.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct ContainerCreate {
    stamp: Stamp,
    kind: ElementKind,
}

/// The create a key retains once it has seen both: the higher-stamped one, which
/// is the one that won — or would have won — the slot, since LWW at the slot ranks
/// the same way. An intrinsic rank, so every replica keeps the same create however
/// the two reached it; the kind tag breaks a stamp tie no honestly minted pair of
/// ops can produce, leaving the choice total rather than order-decided.
fn higher(a: Option<ContainerCreate>, b: Option<ContainerCreate>) -> Option<ContainerCreate> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if (b.stamp, b.kind as u8) > (a.stamp, a.kind as u8) {
            b
        } else {
            a
        }),
        (a, None) => a,
        (None, b) => b,
    }
}

/// The create identity installing `value` at `stamp` contributes: a container of
/// any kind records one, a leaf none. A migration re-keys a leaf write and leaves
/// every container create at the key, so the creates are what rank against each
/// other there — including an XML element, which ranks without being nameable by
/// the key it sits at.
fn create_of(value: &Element, stamp: Stamp) -> Option<ContainerCreate> {
    value.is_container().then_some(ContainerCreate {
        stamp,
        kind: value.kind(),
    })
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

    /// Whether `key` holds a live container of any kind — the slots a migration
    /// carries verbatim, never dropping or re-keying.
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

    /// The retained (create-stamp, kind) of the deleted container at `key`, if the
    /// slot is a tombstone the key's container create sits under — what a snapshot
    /// migration resurrects the create by, when the kind resolves to a handle at
    /// all. `None` for a live slot — a live container is carried verbatim, and a
    /// create under a live *leaf* is out of scope here (C76) — or for a tombstone
    /// on a key no container create ever named.
    pub(crate) fn slot_deleted_container(&self, key: &[u8]) -> Option<(Stamp, ElementKind)> {
        self.slots
            .get(key)
            .filter(|e| e.tombstone)
            .and_then(|e| e.container)
            .map(|c| (c.stamp, c.kind))
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
        // The destination's own retained create outlives whatever lands on it: a
        // migration re-keys a *delete*, never the create the op seam leaves at its
        // old key, so a create this key has seen is still one it has seen.
        let container = higher(
            self.slots.get(&key).and_then(|e| e.container),
            value.as_ref().and_then(|v| create_of(v, stamp)),
        );
        if let Some(e) = self.slots.get_mut(&key).filter(|e| !stamp.gt(&e.stamp)) {
            e.container = container;
            return;
        }
        // Displace the live composite this install evicts from the slot, mirroring
        // `evict` — the migration re-key must displace the loser exactly as the op
        // seam's winning `set` does, or the detached container reads installed and a
        // later op targeting it mutates it. A same-handle re-install (the resurrect
        // loop re-landing a container on its own key) stays installed.
        if let Some(old) = self.slots.get(&key).and_then(|e| {
            e.value
                .as_ref()
                .filter(|_| !e.tombstone)
                .filter(|old| !value.as_ref().is_some_and(|v| same_handle(old, v)))
        }) {
            old.displace();
        }
        self.slots.insert(
            key,
            Entry {
                stamp,
                value,
                tombstone,
                container,
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
            // The slot tag: `0` live, `1` a leaf tombstone, `2` a tombstone over a
            // retained container create, `3` a live slot whose value outranks one.
            // Only `2` and `3` cost the extra stamp + kind byte, and only where the
            // create is not already spelled out by the slot itself — a live
            // container is its own create, so it stays tag `0`, and a key no
            // container create ever named carries nothing to spell.
            let spelled = match entry.value.as_ref() {
                Some(v) if !entry.tombstone => entry
                    .container
                    .filter(|c| create_of(v, entry.stamp) != Some(*c)),
                _ => entry.container,
            };
            match (entry.tombstone, spelled) {
                (false, None) => put_u8(out, SLOT_LIVE),
                (false, Some(create)) => {
                    put_u8(out, SLOT_LIVE_SHADOW);
                    put_stamp(out, &create.stamp);
                    put_u8(out, create.kind as u8);
                }
                (true, None) => {
                    put_u8(out, SLOT_LEAF_TOMB);
                    continue;
                }
                (true, Some(create)) => {
                    put_u8(out, SLOT_CONTAINER_TOMB);
                    put_stamp(out, &create.stamp);
                    put_u8(out, create.kind as u8);
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
            // A slot's stamp is the id of the op that wrote it — one this replica
            // holds, and one LWW resolves strictly-greater against, so a re-issue
            // loses silently. It floors the id-space record
            // ([`Cursor::note_stamp_reach`]).
            cur.note_stamp_reach(stamp.client, stamp.lamport);
            let (tombstone, container) = match cur.u8()? {
                SLOT_LIVE => (false, None),
                SLOT_LEAF_TOMB => (true, None),
                tag @ (SLOT_CONTAINER_TOMB | SLOT_LIVE_SHADOW) => {
                    let stamp = cur.stamp()?;
                    // The create-stamp the key retains — the id a migration
                    // resurrect keys on.
                    cur.note_stamp_reach(stamp.client, stamp.lamport);
                    let kind_tag = cur.u8()?;
                    let kind = match ElementKind::from_tag(kind_tag) {
                        Some(k) if k.is_container() => k,
                        _ => {
                            return Err(DecodeError::BadTag {
                                what: "retained container kind",
                                tag: kind_tag,
                            })
                        }
                    };
                    (tag == SLOT_CONTAINER_TOMB, Some((stamp, kind)))
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
                container,
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
        container: Option<(Stamp, ElementKind)>,
    ) -> bool {
        // A live container spells its own create out through its tag, and a snapshot
        // that spelled out a lesser one alongside it is re-served by the rank rather
        // than as it was handed over.
        let container = higher(
            container.map(|(stamp, kind)| ContainerCreate { stamp, kind }),
            value
                .as_ref()
                .filter(|_| !tombstone)
                .and_then(|v| create_of(v, stamp)),
        );
        self.slots
            .insert(
                key,
                Entry {
                    stamp,
                    value,
                    tombstone,
                    container,
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
        let prior = self.slots.get(key);
        // A container create is retained whether or not it wins the slot: what the
        // key retains is the create the ops rank highest there, so a create a
        // tombstone or a later leaf outranks is recorded exactly as one that lands
        // live is.
        let container = higher(prior.and_then(|e| e.container), create_of(&value, stamp));
        let loses = prior.is_some_and(|e| !stamp.gt(&e.stamp));
        // Re-setting the exact installed handle advances the stamp only, so a
        // still-installed handle is never flagged displaced.
        let reinstalls = prior.is_some_and(|e| {
            !e.tombstone && e.value.as_ref().is_some_and(|old| same_handle(old, &value))
        });
        if loses || reinstalls {
            let e = self
                .slots
                .get_mut(key)
                .expect("the slot `prior` read is present");
            if !loses {
                e.stamp = stamp;
            }
            e.container = container;
            return;
        }
        self.evict(key);
        self.slots.insert(
            key.to_vec(),
            Entry {
                stamp,
                value: Some(value),
                tombstone: false,
                container,
            },
        );
    }

    pub fn delete(&mut self, key: &[u8], stamp: Stamp) {
        if self.slots.get(key).is_some_and(|e| !stamp.gt(&e.stamp)) {
            return;
        }
        // The delete carries the create the key retains across the tombstone, so a
        // snapshot migration can resurrect it at the old key and re-key the delete
        // faithfully. It reads that off the slot rather than off the value it is
        // tombstoning: the create a *later* leaf displaced, or one this delete
        // outranked and so never saw installed, is retained just the same.
        let container = self.slots.get(key).and_then(|e| e.container);
        self.evict(key);
        self.slots.insert(
            key.to_vec(),
            Entry {
                stamp,
                value: None,
                tombstone: true,
                container,
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
            // Both sides' retained creates join by rank, whichever entry LWW keeps:
            // a create one replica saw is one the merged key has seen, and the rank
            // is a function of the creates alone, so the join is commutative.
            let container = higher(self.slots.get(key).and_then(|e| e.container), se.container);

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
                let de = self.slots.get_mut(key).unwrap();
                if se.stamp.gt(&de.stamp) {
                    de.stamp = se.stamp;
                }
                de.container = container;
                continue;
            }

            // LWW: src wins iff strictly greater (or dst absent).
            if self
                .slots
                .get(key)
                .is_some_and(|de| !se.stamp.gt(&de.stamp))
            {
                self.slots.get_mut(key).unwrap().container = container;
                continue;
            }
            self.evict(key);
            self.slots.insert(
                key.clone(),
                Entry {
                    stamp: se.stamp,
                    value: se.value.as_ref().map(|v| v.deep_clone()),
                    tombstone: se.tombstone,
                    container,
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
        let won = self.wins(key, stamp);
        self.set(key, Element::Counter(Rc::clone(&fresh)), stamp);
        if !won {
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
        let won = self.wins(key, stamp);
        self.set(key, Element::Register(Rc::clone(&fresh)), stamp);
        if !won {
            fresh.borrow().displace();
        }
        fresh
    }

    pub fn map(&mut self, key: &[u8], stamp: Stamp) -> Rc<RefCell<Self>> {
        if let Some(Element::Map(m)) = self.live_value(key) {
            // Reaching a container already live at the key is a create there like any
            // other, so it goes through `set`: the slot advances where the create
            // wins, and the key's retained identity ranks it either way.
            self.set(key, Element::Map(Rc::clone(&m)), stamp);
            return m;
        }
        let id = ElementId::derive(self.id, key, ElementKind::Map);
        let fresh = Rc::new(RefCell::new(Self::new(id)));
        let won = self.wins(key, stamp);
        self.set(key, Element::Map(Rc::clone(&fresh)), stamp);
        if !won {
            fresh.borrow().displace();
        }
        fresh
    }

    pub fn list(&mut self, key: &[u8], stamp: Stamp) -> Rc<RefCell<List>> {
        if let Some(Element::List(l)) = self.live_value(key) {
            self.set(key, Element::List(Rc::clone(&l)), stamp);
            return l;
        }
        let id = ElementId::derive(self.id, key, ElementKind::List);
        let fresh = Rc::new(RefCell::new(List::new(id)));
        let won = self.wins(key, stamp);
        self.set(key, Element::List(Rc::clone(&fresh)), stamp);
        if !won {
            fresh.borrow().displace();
        }
        fresh
    }

    pub fn text(&mut self, key: &[u8], stamp: Stamp) -> Rc<RefCell<Text>> {
        if let Some(Element::Text(t)) = self.live_value(key) {
            self.set(key, Element::Text(Rc::clone(&t)), stamp);
            return t;
        }
        let id = ElementId::derive(self.id, key, ElementKind::Text);
        let fresh = Rc::new(RefCell::new(Text::new(id)));
        let won = self.wins(key, stamp);
        self.set(key, Element::Text(Rc::clone(&fresh)), stamp);
        if !won {
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
                        container: e.container,
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
            offset: 0,
        }
    }

    fn eid(b: u8) -> ElementId {
        ElementId::from_bytes([b; 16])
    }

    #[test]
    fn merge_joins_the_retained_create_at_a_key() {
        // A container tombstone (a retained create, lower stamp) and a leaf
        // tombstone (none, higher stamp) at one key. The leaf wins the slot in
        // either direction, and the create it outranks is retained all the same:
        // what the key has seen is not what the slot's LWW winner happens to be.
        // Folding the same four ops in one replica reaches exactly this — create,
        // delete, scalar, delete — so a merge that dropped the create would encode
        // one op set two ways.
        let mut a = Map::new(eid(1));
        a.set(
            b"k",
            Element::Map(Rc::new(RefCell::new(Map::new(eid(9))))),
            stamp(1),
        );
        a.delete(b"k", stamp(2));
        assert!(a.slot_deleted_container(b"k").is_some());

        let mut b = Map::new(eid(1));
        b.set(b"k", Element::Scalar(Scalar::Int(1)), stamp(3));
        b.delete(b"k", stamp(4));
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
        assert_eq!(
            ab.slot_deleted_container(b"k"),
            Some((stamp(1), ElementKind::Map)),
            "the create the winning leaf tombstone outranks is still retained"
        );
        let mut folded = Map::new(eid(1));
        folded.set(
            b"k",
            Element::Map(Rc::new(RefCell::new(Map::new(eid(9))))),
            stamp(1),
        );
        folded.delete(b"k", stamp(2));
        folded.set(b"k", Element::Scalar(Scalar::Int(1)), stamp(3));
        folded.delete(b"k", stamp(4));
        let mut merged = Vec::new();
        ab.encode_state_into(&mut merged);
        let mut direct = Vec::new();
        folded.encode_state_into(&mut direct);
        assert_eq!(merged, direct, "a merge and a fold of one op set agree");
    }

    #[test]
    fn a_create_reaching_an_already_live_container_ranks_with_the_rest() {
        // Reaching a container that is already live at the key is still a create
        // there, and ranks like any other: without that, two orders of the same two
        // creates leave the slot at different stamps and retaining different
        // identities — the same divergence a losing create left.
        let encoded = |stamps: [Stamp; 2]| {
            let mut m = Map::new(eid(1));
            for s in stamps {
                m.map(b"k", s);
            }
            m.delete(b"k", stamp(9));
            assert_eq!(
                m.slot_deleted_container(b"k"),
                Some((stamp(5), ElementKind::Map)),
                "the higher-stamped create is the one the key retains"
            );
            let mut out = Vec::new();
            m.encode_state_into(&mut out);
            out
        };
        assert_eq!(encoded([stamp(1), stamp(5)]), encoded([stamp(5), stamp(1)]));
    }

    #[test]
    fn the_higher_stamped_create_is_the_one_a_key_retains() {
        // Two creates of different kinds at one key, both outranked by the delete.
        // The rank is the creates' own — the higher stamp — so a replica that saw
        // the list first retains the same one as a replica that saw the map first.
        for reversed in [false, true] {
            let mut m = Map::new(eid(1));
            let mut creates: Vec<(Stamp, Element)> = vec![
                (
                    stamp(1),
                    Element::Map(Rc::new(RefCell::new(Map::new(eid(9))))),
                ),
                (
                    stamp(2),
                    Element::List(Rc::new(RefCell::new(List::new(eid(8))))),
                ),
            ];
            if reversed {
                creates.reverse();
            }
            m.delete(b"k", stamp(3));
            for (s, value) in creates {
                m.set(b"k", value, s);
            }
            assert_eq!(
                m.slot_deleted_container(b"k"),
                Some((stamp(2), ElementKind::List)),
                "the higher-stamped create is retained, not the first one seen"
            );
        }
    }
}
