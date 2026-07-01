//! Document — a replica: a root Map, a lamport clock, and the transact/apply
//! seam.
//!
//! A `transact` mutates the live tree and returns the ops it emitted; `apply`
//! folds a foreign op back in. Ops are keyed by `(client, seq)` for idempotent
//! dedup and ordered by their stamp for LWW. Map-child ops name a slot key on
//! the target Map; the receiver reaches the child through the map's
//! get-or-create, re-deriving its id, so no separate create op is needed.

use crate::clientid::ClientId;
use crate::element::Element;
use crate::elementid::{ElementId, ElementKind};
use crate::map::Map;
use crate::op::{Op, OpId, OpKind};
use crate::scalar::Scalar;
use crate::stamp::Stamp;
use std::cell::RefCell;
use std::collections::HashSet;
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
    lamport: u64,
    seq: u64,
    seen: HashSet<OpId>,
    orphans: Vec<OrphanEvent>,
}

impl Document {
    pub fn new(client: ClientId) -> Self {
        Self {
            client,
            root: Rc::new(RefCell::new(Map::new(ElementId::from_bytes(ROOT_ID)))),
            lamport: 0,
            seq: 0,
            seen: HashSet::new(),
            orphans: Vec::new(),
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
        F: FnOnce(&mut Txn),
    {
        let mut txn = Txn {
            doc: self,
            ops: Vec::new(),
        };
        f(&mut txn);
        txn.ops
    }

    /// Fold a foreign op into local state. Returns `false` if the op was
    /// already applied (deduped on its id).
    pub fn apply(&mut self, op: &Op) -> bool {
        if op.target != self.root_id() {
            // Only the root map is addressable at this scope; an op naming any
            // other parent is not applied here.
            return false;
        }
        if !self.seen.insert(op.id) {
            return false;
        }
        if op.stamp.lamport > self.lamport {
            self.lamport = op.stamp.lamport;
        }
        self.apply_kind(&op.kind, op.stamp, op.id.client);
        true
    }

    /// Mint identity + causal position for a local edit, apply it, and return
    /// the op.
    fn emit(&mut self, kind: OpKind) -> Op {
        self.lamport += 1;
        let stamp = Stamp {
            lamport: self.lamport,
            client: self.client,
        };
        let id = OpId {
            client: self.client,
            seq: self.seq,
        };
        self.seq += 1;
        self.seen.insert(id);
        let author = self.client;
        self.apply_kind(&kind, stamp, author);
        Op::new(id, stamp, self.root_id(), kind)
    }

    /// Route a mutation to the root map, recording any displaced composite.
    fn apply_kind(&mut self, kind: &OpKind, stamp: Stamp, author: ClientId) {
        let root = Rc::clone(&self.root);
        let orphan = {
            let mut m = root.borrow_mut();
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
            }
        };
        if let Some(o) = orphan {
            self.orphans.push(o);
        }
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

/// The editing surface inside a [`Document::transact`]. Each intention mutates
/// the live tree and appends the op it produced.
pub struct Txn<'a> {
    doc: &'a mut Document,
    ops: Vec<Op>,
}

impl Txn<'_> {
    /// Set a scalar directly in the root map slot.
    pub fn set(&mut self, key: &[u8], value: Scalar) {
        let op = self.doc.emit(OpKind::MapSet {
            key: key.to_vec(),
            value,
        });
        self.ops.push(op);
    }

    /// Install-or-set a Register at `key`.
    pub fn register(&mut self, key: &[u8], value: Scalar) {
        let op = self.doc.emit(OpKind::RegisterSet {
            key: key.to_vec(),
            value,
        });
        self.ops.push(op);
    }

    /// Install-or-increment a Counter at `key`.
    pub fn inc(&mut self, key: &[u8], amount: u32) {
        let op = self.doc.emit(OpKind::CounterInc {
            key: key.to_vec(),
            amount,
        });
        self.ops.push(op);
    }

    /// Install-or-decrement a Counter at `key`.
    pub fn dec(&mut self, key: &[u8], amount: u32) {
        let op = self.doc.emit(OpKind::CounterDec {
            key: key.to_vec(),
            amount,
        });
        self.ops.push(op);
    }

    /// Tombstone the slot at `key`.
    pub fn delete(&mut self, key: &[u8]) {
        let op = self.doc.emit(OpKind::MapDelete { key: key.to_vec() });
        self.ops.push(op);
    }
}
