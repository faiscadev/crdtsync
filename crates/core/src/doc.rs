//! Document — a replica: a tree of Maps rooted at a well-known id, a lamport
//! clock, and the transact/apply seam.
//!
//! A `transact` mutates the live tree through a cursor and returns the ops it
//! emitted; `apply` folds a foreign op back in. Ops are keyed by `(client,
//! seq)` for idempotent dedup and ordered by their stamp for LWW. Every op
//! targets a Map by id and names a slot key; the receiver reaches the child
//! through the map's get-or-create, re-deriving its id. Nested maps are
//! reached by resolving `Op.target` against an index of every map in the tree.

use crate::clientid::ClientId;
use crate::element::Element;
use crate::elementid::{ElementId, ElementKind};
use crate::list::List;
use crate::map::Map;
use crate::text::Text;
use crate::op::{Op, OpId, OpKind};
use crate::scalar::Scalar;
use crate::stamp::Stamp;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// The well-known root slot every replica shares, so children derive under the
/// same parent.
const ROOT_ID: [u8; 16] = *b"crdtsync\0\0\0\0root";

/// A composite that a mutation displaced from its slot and left unreachable.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OrphanEvent {
    pub id: ElementId,
}

pub struct Document {
    client: ClientId,
    root: Rc<RefCell<Map>>,
    /// Every map in the tree, keyed by id, for resolving an op's target.
    maps: HashMap<ElementId, Rc<RefCell<Map>>>,
    /// Every list in the tree, keyed by id, for resolving a sequence op.
    lists: HashMap<ElementId, Rc<RefCell<List>>>,
    /// Every text in the tree, keyed by id, for resolving a text op.
    texts: HashMap<ElementId, Rc<RefCell<Text>>>,
    lamport: u64,
    seq: u64,
    seen: HashSet<OpId>,
    orphans: Vec<OrphanEvent>,
    /// Ops emitted by the transact currently in progress.
    pending: Vec<Op>,
}

impl Document {
    pub fn new(client: ClientId) -> Self {
        let root = Rc::new(RefCell::new(Map::new(ElementId::from_bytes(ROOT_ID))));
        let mut maps = HashMap::new();
        maps.insert(root.borrow().id(), Rc::clone(&root));
        Self {
            client,
            root,
            maps,
            lists: HashMap::new(),
            texts: HashMap::new(),
            lamport: 0,
            seq: 0,
            seen: HashSet::new(),
            orphans: Vec::new(),
            pending: Vec::new(),
        }
    }

    pub fn client(&self) -> ClientId {
        self.client
    }

    /// The shared root id.
    pub fn root_id(&self) -> ElementId {
        self.root.borrow().id()
    }

    /// The live root Map handle.
    pub fn root(&self) -> Rc<RefCell<Map>> {
        Rc::clone(&self.root)
    }

    /// Read a live slot of the root map.
    pub fn get(&self, key: &[u8]) -> Option<Element> {
        self.root.borrow().get(key)
    }

    /// Drain the orphan events accumulated since the last call.
    pub fn take_orphans(&mut self) -> Vec<OrphanEvent> {
        std::mem::take(&mut self.orphans)
    }

    /// Gather local edits into ops, applying each as it is emitted.
    pub fn transact<F>(&mut self, f: F) -> Vec<Op>
    where
        F: FnOnce(&mut MapCursor),
    {
        self.pending.clear();
        let root_id = self.root_id();
        {
            let mut cursor = MapCursor {
                doc: self,
                map_id: root_id,
            };
            f(&mut cursor);
        }
        std::mem::take(&mut self.pending)
    }

    /// Fold a foreign op into local state. Returns `false` without applying it
    /// when the op targets a map this replica hasn't materialised, or when it
    /// was already applied (deduped on its id).
    pub fn apply(&mut self, op: &Op) -> bool {
        if !self.resolvable(op.target) {
            return false;
        }
        if !self.seen.insert(op.id) {
            return false;
        }
        // A text run occupies one char_id per codepoint from the op's stamp;
        // the clock must clear the last of them, not just the base.
        let last = op.stamp.lamport + span(&op.kind) - 1;
        if last > self.lamport {
            self.lamport = last;
        }
        self.apply_kind(op.target, &op.kind, op.stamp, op.id.client);
        true
    }

    /// Mint identity + causal position for a local edit, apply it, and record
    /// the op on the in-progress transact.
    fn emit(&mut self, target: ElementId, kind: OpKind) {
        self.lamport += 1;
        let stamp = Stamp {
            lamport: self.lamport,
            client: self.client,
        };
        // Reserve the rest of a run's char_ids so the next op sorts after it.
        self.lamport += span(&kind) - 1;
        let id = OpId {
            client: self.client,
            seq: self.seq,
        };
        self.seq += 1;
        self.seen.insert(id);
        let author = self.client;
        self.apply_kind(target, &kind, stamp, author);
        self.pending.push(Op::new(id, stamp, target, kind));
    }

    /// A target is reachable when it names a map or list that is present and
    /// still installed in the tree (a displaced element is unreachable).
    fn resolvable(&self, target: ElementId) -> bool {
        self.maps
            .get(&target)
            .is_some_and(|m| !m.borrow().is_displaced())
            || self
                .lists
                .get(&target)
                .is_some_and(|l| !l.borrow().is_displaced())
            || self
                .texts
                .get(&target)
                .is_some_and(|t| !t.borrow().is_displaced())
    }

    /// A live list handle for `target`, if any.
    fn live_list(&self, target: ElementId) -> Option<Rc<RefCell<List>>> {
        self.lists
            .get(&target)
            .filter(|l| !l.borrow().is_displaced())
            .cloned()
    }

    /// A live text handle for `target`, if any.
    fn live_text(&self, target: ElementId) -> Option<Rc<RefCell<Text>>> {
        self.texts
            .get(&target)
            .filter(|t| !t.borrow().is_displaced())
            .cloned()
    }

    /// Route a mutation to its target, recording any displaced composite and
    /// indexing any container it creates.
    fn apply_kind(&mut self, target: ElementId, kind: &OpKind, stamp: Stamp, author: ClientId) {
        // Sequence ops address a list directly.
        match kind {
            OpKind::ListInsert { value, anchor } => {
                if let Some(list) = self.live_list(target) {
                    list.borrow_mut()
                        .insert_at(stamp, Element::Scalar(value.clone()), *anchor);
                }
                return;
            }
            OpKind::ListDelete { id } => {
                if let Some(list) = self.live_list(target) {
                    list.borrow_mut().delete_id(*id);
                }
                return;
            }
            OpKind::TextInsert { s, anchor } => {
                if let Some(text) = self.live_text(target) {
                    text.borrow_mut().insert_run(stamp, s, *anchor);
                }
                return;
            }
            OpKind::TextDelete { ids } => {
                if let Some(text) = self.live_text(target) {
                    text.borrow_mut().delete_ids(ids);
                }
                return;
            }
            _ => {}
        }

        // The rest address a map slot (ListCreate installs a list child there).
        let Some(map) = self.maps.get(&target).cloned() else {
            return;
        };
        if map.borrow().is_displaced() {
            return;
        }
        let mut new_map: Option<Rc<RefCell<Map>>> = None;
        let mut new_list: Option<Rc<RefCell<List>>> = None;
        let mut new_text: Option<Rc<RefCell<Text>>> = None;
        let orphan = {
            let mut m = map.borrow_mut();
            match kind {
                OpKind::MapCreate { key } => {
                    let prior = m.get(key);
                    let child = m.map(key, stamp);
                    // Advance the slot stamp so a re-navigated child defends its
                    // slot against a stale scalar set.
                    m.set(key, Element::Map(Rc::clone(&child)), stamp);
                    // A losing create yields a detached, displaced child; only a
                    // reachable one belongs in the index.
                    if !child.borrow().is_displaced() {
                        new_map = Some(Rc::clone(&child));
                    }
                    displaced(prior)
                }
                OpKind::ListCreate { key } => {
                    let prior = m.get(key);
                    let child = m.list(key, stamp);
                    m.set(key, Element::List(Rc::clone(&child)), stamp);
                    if !child.borrow().is_displaced() {
                        new_list = Some(Rc::clone(&child));
                    }
                    displaced(prior)
                }
                OpKind::TextCreate { key } => {
                    let prior = m.get(key);
                    let child = m.text(key, stamp);
                    m.set(key, Element::Text(Rc::clone(&child)), stamp);
                    if !child.borrow().is_displaced() {
                        new_text = Some(Rc::clone(&child));
                    }
                    displaced(prior)
                }
                OpKind::MapSet { key, value } => {
                    let prior = m.get(key);
                    m.set(key, Element::Scalar(value.clone()), stamp);
                    displaced(prior)
                }
                OpKind::MapDelete { key } => {
                    let prior = m.get(key);
                    m.delete(key, stamp);
                    displaced(prior)
                }
                OpKind::RegisterSet { key, value } => {
                    let prior = m.get(key);
                    let r = m.register(key, value.clone(), stamp);
                    r.borrow_mut().set(value.clone(), stamp);
                    m.set(key, Element::Register(Rc::clone(&r)), stamp);
                    displaced(prior)
                }
                OpKind::CounterInc { key, amount } => {
                    let prior = m.get(key);
                    let c = m.counter(key, stamp);
                    c.borrow_mut().inc(author, *amount);
                    m.set(key, Element::Counter(Rc::clone(&c)), stamp);
                    displaced(prior)
                }
                OpKind::CounterDec { key, amount } => {
                    let prior = m.get(key);
                    let c = m.counter(key, stamp);
                    c.borrow_mut().dec(author, *amount);
                    m.set(key, Element::Counter(Rc::clone(&c)), stamp);
                    displaced(prior)
                }
                OpKind::ListInsert { .. }
                | OpKind::ListDelete { .. }
                | OpKind::TextInsert { .. }
                | OpKind::TextDelete { .. } => unreachable!(),
            }
        };
        if let Some(child) = new_map {
            let id = child.borrow().id();
            self.maps.insert(id, child);
        }
        if let Some(child) = new_list {
            let id = child.borrow().id();
            self.lists.insert(id, child);
        }
        if let Some(child) = new_text {
            let id = child.borrow().id();
            self.texts.insert(id, child);
        }
        if let Some(o) = orphan {
            self.orphans.push(o);
        }
    }
}

/// How many consecutive char_ids an op consumes from its stamp. A text run
/// takes one per codepoint; every other op takes one.
fn span(kind: &OpKind) -> u64 {
    match kind {
        OpKind::TextInsert { s, .. } => s.chars().count().max(1) as u64,
        _ => 1,
    }
}

/// A composite that was live before a mutation and is displaced after it is an
/// orphan; a scalar slot never orphans.
fn displaced(prior: Option<Element>) -> Option<OrphanEvent> {
    match prior {
        Some(e) if e.kind() != ElementKind::Scalar && e.is_displaced() => {
            Some(OrphanEvent { id: e.id() })
        }
        _ => None,
    }
}

/// A cursor over one Map in the tree. Each intention mutates the live tree and
/// appends the op it produced to the transact.
pub struct MapCursor<'a> {
    doc: &'a mut Document,
    map_id: ElementId,
}

impl MapCursor<'_> {
    /// Set a scalar directly in this map's slot.
    pub fn set(&mut self, key: &[u8], value: Scalar) {
        self.doc.emit(
            self.map_id,
            OpKind::MapSet {
                key: key.to_vec(),
                value,
            },
        );
    }

    /// Install-or-set a Register at `key`.
    pub fn register(&mut self, key: &[u8], value: Scalar) {
        self.doc.emit(
            self.map_id,
            OpKind::RegisterSet {
                key: key.to_vec(),
                value,
            },
        );
    }

    /// Install-or-increment a Counter at `key`.
    pub fn inc(&mut self, key: &[u8], amount: u32) {
        self.doc.emit(
            self.map_id,
            OpKind::CounterInc {
                key: key.to_vec(),
                amount,
            },
        );
    }

    /// Install-or-decrement a Counter at `key`.
    pub fn dec(&mut self, key: &[u8], amount: u32) {
        self.doc.emit(
            self.map_id,
            OpKind::CounterDec {
                key: key.to_vec(),
                amount,
            },
        );
    }

    /// Tombstone the slot at `key`.
    pub fn delete(&mut self, key: &[u8]) {
        self.doc
            .emit(self.map_id, OpKind::MapDelete { key: key.to_vec() });
    }

    /// Descend into a nested Map at `key`, creating it if absent.
    pub fn map(&mut self, key: &[u8]) -> MapCursor<'_> {
        self.doc
            .emit(self.map_id, OpKind::MapCreate { key: key.to_vec() });
        let child = ElementId::derive(self.map_id, key, ElementKind::Map);
        MapCursor {
            doc: self.doc,
            map_id: child,
        }
    }

    /// Descend into a List at `key`, creating it if absent.
    pub fn list(&mut self, key: &[u8]) -> ListCursor<'_> {
        self.doc
            .emit(self.map_id, OpKind::ListCreate { key: key.to_vec() });
        let list_id = ElementId::derive(self.map_id, key, ElementKind::List);
        ListCursor {
            doc: self.doc,
            list_id,
        }
    }

    /// Descend into a Text at `key`, creating it if absent.
    pub fn text(&mut self, key: &[u8]) -> TextCursor<'_> {
        self.doc
            .emit(self.map_id, OpKind::TextCreate { key: key.to_vec() });
        let text_id = ElementId::derive(self.map_id, key, ElementKind::Text);
        TextCursor {
            doc: self.doc,
            text_id,
        }
    }
}

/// A cursor over one List in the tree.
pub struct ListCursor<'a> {
    doc: &'a mut Document,
    list_id: ElementId,
}

impl ListCursor<'_> {
    /// Insert `value` at live `index`. The op carries the Fugue placement, so
    /// it applies identically on every replica.
    pub fn insert(&mut self, index: usize, value: Scalar) {
        let anchor = match self.doc.lists.get(&self.list_id) {
            Some(list) => list.borrow().place(index),
            None => return,
        };
        self.doc
            .emit(self.list_id, OpKind::ListInsert { value, anchor });
    }

    /// Tombstone the live item at `index`.
    pub fn delete(&mut self, index: usize) {
        let id = match self.doc.lists.get(&self.list_id) {
            Some(list) => list.borrow().node_at(index),
            None => return,
        };
        if let Some(id) = id {
            self.doc.emit(self.list_id, OpKind::ListDelete { id });
        }
    }
}

/// A cursor over one Text in the tree.
pub struct TextCursor<'a> {
    doc: &'a mut Document,
    text_id: ElementId,
}

impl TextCursor<'_> {
    /// Insert `s` at codepoint `index`. The op carries the Fugue placement, so
    /// it applies identically on every replica.
    pub fn insert(&mut self, index: usize, s: &str) {
        let anchor = match self.doc.texts.get(&self.text_id) {
            Some(text) => text.borrow().place(index),
            None => return,
        };
        self.doc.emit(
            self.text_id,
            OpKind::TextInsert {
                s: s.to_string(),
                anchor,
            },
        );
    }

    /// Tombstone `count` live codepoints starting at `index`.
    pub fn delete(&mut self, index: usize, count: usize) {
        let ids = match self.doc.texts.get(&self.text_id) {
            Some(text) => text.borrow().node_ids(index, count),
            None => return,
        };
        if !ids.is_empty() {
            self.doc.emit(self.text_id, OpKind::TextDelete { ids });
        }
    }
}
