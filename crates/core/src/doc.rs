//! Document — a replica: a tree of containers rooted at a well-known id, a
//! lamport clock, and the transact/apply seam.
//!
//! A `transact` mutates the live tree through a cursor and returns the ops it
//! emitted; `apply` folds a foreign op back in. Ops are keyed by `(client,
//! seq)` for idempotent dedup and ordered by their stamp for LWW. Each op
//! names its target container by id, resolved against a registry of every
//! container the replica has materialised. That registry retains displaced
//! containers, so a slot re-won after displacement is the same logical
//! element: identity persists across displacement. An op whose target isn't
//! reachable yet — its parent unseen, or an ancestor displaced — is buffered
//! and replays once a create restores reachability, so out-of-order delivery
//! converges.

use crate::clientid::ClientId;
use crate::counter::Counter;
use crate::element::Element;
use crate::elementid::{ElementId, ElementKind};
use crate::list::List;
use crate::map::Map;
use crate::op::{Op, OpId, OpKind};
use crate::scalar::Scalar;
use crate::stamp::Stamp;
use crate::text::Text;
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
    /// Every container the replica has ever materialised, keyed by id — the
    /// persistent identity registry. A displaced container stays here with its
    /// content so a later create re-installs the same logical element.
    maps: HashMap<ElementId, Rc<RefCell<Map>>>,
    lists: HashMap<ElementId, Rc<RefCell<List>>>,
    texts: HashMap<ElementId, Rc<RefCell<Text>>>,
    /// Every counter the replica has materialised, keyed by id. A counter's
    /// value is the sum of the increments applied to its id, so it is retained
    /// here across displacement: a slot re-won by a later increment resumes the
    /// same total.
    counters: HashMap<ElementId, Rc<RefCell<Counter>>>,
    /// Each container's parent map, for walking reachability up to the root.
    parents: HashMap<ElementId, ElementId>,
    lamport: u64,
    seq: u64,
    seen: HashSet<OpId>,
    /// Ops whose target isn't reachable yet, held until a create makes it so.
    buffer: Vec<Op>,
    buffered: HashSet<OpId>,
    orphans: Vec<OrphanEvent>,
    /// Ops emitted by the transact currently in progress.
    pending: Vec<Op>,
}

impl Drop for Document {
    fn drop(&mut self) {
        // Break every parent→child link first, via the flat registry, so a
        // deeply nested tree frees iteratively instead of recursing through the
        // chain of Rc drops (which a caller-supplied path depth could overflow).
        // Skip a handle a caller is still borrowing rather than panic in drop.
        for map in self.maps.values() {
            if let Ok(mut map) = map.try_borrow_mut() {
                map.clear();
            }
        }
    }
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
            counters: HashMap::new(),
            parents: HashMap::new(),
            lamport: 0,
            seq: 0,
            seen: HashSet::new(),
            buffer: Vec::new(),
            buffered: HashSet::new(),
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
        // A local create can restore a container that buffered remote ops were
        // waiting on; replay them now, not only on the next remote apply.
        self.drain_buffer();
        std::mem::take(&mut self.pending)
    }

    /// Fold a foreign op into local state. An op whose target isn't reachable
    /// yet is buffered and returns `false`; it replays once a create makes the
    /// target reachable. Returns `false` for an already-applied or already-held
    /// op. Returns `true` only when the op is applied now.
    pub fn apply(&mut self, op: &Op) -> bool {
        if self.seen.contains(&op.id) || self.buffered.contains(&op.id) {
            return false;
        }
        if !self.ready(op) {
            self.buffered.insert(op.id);
            self.buffer.push(op.clone());
            return false;
        }
        self.apply_now(op);
        self.drain_buffer();
        true
    }

    /// Apply a resolvable op unconditionally: mark it seen, advance the clock,
    /// and route it.
    fn apply_now(&mut self, op: &Op) {
        self.seen.insert(op.id);
        // A text run occupies one char_id per codepoint from the op's stamp;
        // the clock must clear the last of them, not just the base.
        let last = op.stamp.lamport.saturating_add(span(&op.kind) - 1);
        if last > self.lamport {
            self.lamport = last;
        }
        self.apply_kind(op.target, &op.kind, op.stamp, op.id.client);
    }

    /// Replay buffered ops that a state change just made reachable, to a
    /// fixpoint — one applied op can unblock a whole causal chain.
    fn drain_buffer(&mut self) {
        while let Some(i) = self.buffer.iter().position(|op| self.ready(op)) {
            let op = self.buffer.remove(i);
            self.buffered.remove(&op.id);
            self.apply_now(&op);
        }
    }

    /// Whether `op` can apply now: its target is reachable, and — for a delete —
    /// the nodes it removes are present. A delete of a not-yet-inserted node
    /// would silently no-op and be lost, so it waits for the insert.
    fn ready(&self, op: &Op) -> bool {
        if !self.resolvable(op.target) {
            return false;
        }
        match &op.kind {
            OpKind::ListDelete { id } => self
                .lists
                .get(&op.target)
                .is_some_and(|l| l.borrow().contains(*id)),
            OpKind::TextDelete { ids } => self.texts.get(&op.target).is_some_and(|t| {
                let t = t.borrow();
                ids.iter().all(|id| t.contains(*id))
            }),
            _ => true,
        }
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

    /// A target is reachable when it names a materialised container that is
    /// installed, and every ancestor up to the root is too. A displaced
    /// container anywhere on the chain breaks reachability.
    fn resolvable(&self, target: ElementId) -> bool {
        let mut cur = target;
        loop {
            if cur == self.root_id() {
                return true;
            }
            if self.displaced_container(cur) != Some(false) {
                return false;
            }
            match self.parents.get(&cur) {
                Some(&parent) => cur = parent,
                None => return false,
            }
        }
    }

    /// Whether the container `id` is displaced: `Some(false)` installed,
    /// `Some(true)` displaced, `None` not materialised.
    fn displaced_container(&self, id: ElementId) -> Option<bool> {
        if let Some(m) = self.maps.get(&id) {
            return Some(m.borrow().is_displaced());
        }
        if let Some(l) = self.lists.get(&id) {
            return Some(l.borrow().is_displaced());
        }
        if let Some(t) = self.texts.get(&id) {
            return Some(t.borrow().is_displaced());
        }
        None
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
    /// registering any container it creates.
    fn apply_kind(&mut self, target: ElementId, kind: &OpKind, stamp: Stamp, author: ClientId) {
        match kind {
            // Sequence and text ops address a list or text directly.
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
            // Container creates go through the persistent registry.
            OpKind::MapCreate { key } => {
                self.create_container(target, key, stamp, Container::Map);
                return;
            }
            OpKind::ListCreate { key } => {
                self.create_container(target, key, stamp, Container::List);
                return;
            }
            OpKind::TextCreate { key } => {
                self.create_container(target, key, stamp, Container::Text);
                return;
            }
            // Counter ops go through the persistent registry too, so a
            // displaced counter keeps accumulating toward its total.
            OpKind::CounterInc { key, amount } => {
                self.apply_counter(target, key, author, CounterDelta::Inc(*amount), stamp);
                return;
            }
            OpKind::CounterDec { key, amount } => {
                self.apply_counter(target, key, author, CounterDelta::Dec(*amount), stamp);
                return;
            }
            _ => {}
        }

        // The rest address a scalar or leaf composite in a map slot.
        let Some(map) = self.maps.get(&target).cloned() else {
            return;
        };
        if map.borrow().is_displaced() {
            return;
        }
        let orphan = {
            let mut m = map.borrow_mut();
            match kind {
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
                _ => unreachable!("container, counter, sequence, and text ops routed above"),
            }
        };
        if let Some(o) = orphan {
            self.orphans.push(o);
        }
    }

    /// Install a container child in `map_id`'s slot at `key`. The handle comes
    /// from the registry, so a slot re-won after displacement is the same
    /// logical element with its content intact; a fresh id is registered on
    /// first sight. A losing create leaves the handle displaced but retained.
    fn create_container(&mut self, map_id: ElementId, key: &[u8], stamp: Stamp, kind: Container) {
        let Some(map) = self.maps.get(&map_id).cloned() else {
            return;
        };
        if map.borrow().is_displaced() {
            return;
        }
        let child_id = ElementId::derive(map_id, key, kind.element_kind());
        let element = self.registered_handle(child_id, kind);
        let (won, orphan) = {
            let mut m = map.borrow_mut();
            let prior = m.get(key);
            m.set(key, element.clone(), stamp);
            let won = m
                .get(key)
                .as_ref()
                .is_some_and(|cur| handles_eq(cur, &element));
            (won, displaced(prior))
        };
        if won {
            element.reinstate();
        } else {
            element.displace();
        }
        self.parents.insert(child_id, map_id);
        if let Some(o) = orphan {
            self.orphans.push(o);
        }
    }

    /// Fold a counter delta into the counter at `key` in `map_id`. The counter
    /// comes from the persistent registry, so its total accumulates by id even
    /// while a scalar holds the slot; the delta re-wins the slot only if its
    /// stamp is the latest there, otherwise the counter stays displaced with its
    /// total intact.
    fn apply_counter(
        &mut self,
        map_id: ElementId,
        key: &[u8],
        author: ClientId,
        delta: CounterDelta,
        stamp: Stamp,
    ) {
        let Some(map) = self.maps.get(&map_id).cloned() else {
            return;
        };
        if map.borrow().is_displaced() {
            return;
        }
        let id = ElementId::derive(map_id, key, ElementKind::Counter);
        let counter = Rc::clone(
            self.counters
                .entry(id)
                .or_insert_with(|| Rc::new(RefCell::new(Counter::new(id)))),
        );
        match delta {
            CounterDelta::Inc(amount) => counter.borrow_mut().inc(author, amount),
            CounterDelta::Dec(amount) => counter.borrow_mut().dec(author, amount),
        }
        let (won, orphan) = {
            let mut m = map.borrow_mut();
            let prior = m.get(key);
            m.set(key, Element::Counter(Rc::clone(&counter)), stamp);
            let won = m
                .get(key)
                .as_ref()
                .is_some_and(|cur| handles_eq(cur, &Element::Counter(Rc::clone(&counter))));
            (won, displaced(prior))
        };
        if won {
            counter.borrow().reinstate();
        } else {
            counter.borrow().displace();
        }
        if let Some(o) = orphan {
            self.orphans.push(o);
        }
    }

    /// The registered container handle for `id`, wrapped as an Element,
    /// materialising and registering a fresh one on first sight.
    fn registered_handle(&mut self, id: ElementId, kind: Container) -> Element {
        match kind {
            Container::Map => Element::Map(Rc::clone(
                self.maps
                    .entry(id)
                    .or_insert_with(|| Rc::new(RefCell::new(Map::new(id)))),
            )),
            Container::List => Element::List(Rc::clone(
                self.lists
                    .entry(id)
                    .or_insert_with(|| Rc::new(RefCell::new(List::new(id)))),
            )),
            Container::Text => Element::Text(Rc::clone(
                self.texts
                    .entry(id)
                    .or_insert_with(|| Rc::new(RefCell::new(Text::new(id)))),
            )),
        }
    }
}

/// A directional counter change, so the registry keeps inc and dec tallies
/// apart for a PN-counter's per-client merge.
#[derive(Clone, Copy)]
enum CounterDelta {
    Inc(u32),
    Dec(u32),
}

/// The container kinds a create op installs.
#[derive(Clone, Copy)]
enum Container {
    Map,
    List,
    Text,
}

impl Container {
    fn element_kind(self) -> ElementKind {
        match self {
            Container::Map => ElementKind::Map,
            Container::List => ElementKind::List,
            Container::Text => ElementKind::Text,
        }
    }
}

/// Whether two Elements hold the exact same registered handle.
fn handles_eq(a: &Element, b: &Element) -> bool {
    match (a, b) {
        (Element::Map(x), Element::Map(y)) => Rc::ptr_eq(x, y),
        (Element::List(x), Element::List(y)) => Rc::ptr_eq(x, y),
        (Element::Text(x), Element::Text(y)) => Rc::ptr_eq(x, y),
        (Element::Counter(x), Element::Counter(y)) => Rc::ptr_eq(x, y),
        _ => false,
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

impl<'a> MapCursor<'a> {
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

    /// Descend into a nested Map at `key`, consuming this cursor. Chains without
    /// nesting borrows, so a caller can walk a runtime-length path in a loop.
    pub fn into_map(self, key: &[u8]) -> MapCursor<'a> {
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
