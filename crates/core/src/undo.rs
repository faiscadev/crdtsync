//! Per-user undo / redo — a client-side helper that records a user's own edits
//! and replays their inverses.
//!
//! Undo in a CRDT is not op reversal: the engine sees only ordinary forward ops
//! that restore the previously observed value, so there is no server-side undo
//! state and no special wire format. An [`UndoManager`] wraps a [`Document`],
//! and every edit made through it captures what it overwrote so a later
//! [`undo`](UndoManager::undo) can put it back; [`redo`](UndoManager::redo)
//! replays the undone change. Only edits made through the manager are tracked, so
//! it reverts a user's own intentions and never anyone else's — global undo is
//! deliberately unsupported.
//!
//! An undo step is one *intention* — a group of edits a user made as a single
//! gesture, reverted together. A single-edit method records a one-edit intention;
//! [`group`](UndoManager::group) records several edits as one.
//!
//! This helper covers root-level scalar slots: an integer/bytes/bool Register and
//! a Counter. Nested paths, lists, and text — whose inverses need element
//! revival — are layered on in later work.

use crate::doc::Document;
use crate::op::Op;
use crate::{Element, Scalar};

/// The inverse of one recorded edit — what to replay to undo it. Applying an
/// inverse yields the change that would in turn undo *it*, which is what makes
/// undo and redo symmetric.
enum Change {
    /// Restore a root Register slot to `value`, or delete it if the slot held
    /// nothing before the edit.
    Slot { key: Vec<u8>, value: Option<Scalar> },
    /// Apply this counter delta — one direction to cancel the recorded one.
    Counter { key: Vec<u8>, inc: u32, dec: u32 },
}

/// One undo step: the inverses of a group of edits, in the order they were made.
type Intention = Vec<Change>;

/// A user's undo/redo stacks over one [`Document`]. Each recorded intention
/// pushes onto the undo stack; a fresh edit clears the redo stack, as an
/// intervening edit makes the redone future ambiguous.
#[derive(Default)]
pub struct UndoManager {
    undo: Vec<Intention>,
    redo: Vec<Intention>,
}

/// The edits of one [`group`](UndoManager::group), applied as they are called
/// while their inverses and emitted ops accumulate into a single intention.
pub struct Batch<'a> {
    doc: &'a mut Document,
    inverses: Intention,
    ops: Vec<Op>,
}

impl Batch<'_> {
    /// Install-or-set a root Register at `key`.
    pub fn register(&mut self, key: &[u8], value: Scalar) -> &mut Self {
        let prior = read_register(self.doc, key);
        self.ops
            .extend(self.doc.transact(|tx| tx.register(key, value)));
        self.inverses.push(Change::Slot {
            key: key.to_vec(),
            value: prior,
        });
        self
    }

    /// Install-or-increment a root Counter at `key`.
    pub fn inc(&mut self, key: &[u8], amount: u32) -> &mut Self {
        self.ops.extend(self.doc.transact(|tx| tx.inc(key, amount)));
        self.inverses.push(Change::Counter {
            key: key.to_vec(),
            inc: 0,
            dec: amount,
        });
        self
    }

    /// Install-or-decrement a root Counter at `key`.
    pub fn dec(&mut self, key: &[u8], amount: u32) -> &mut Self {
        self.ops.extend(self.doc.transact(|tx| tx.dec(key, amount)));
        self.inverses.push(Change::Counter {
            key: key.to_vec(),
            inc: amount,
            dec: 0,
        });
        self
    }

    /// Tombstone a root Register slot at `key`.
    pub fn delete(&mut self, key: &[u8]) -> &mut Self {
        let prior = read_register(self.doc, key);
        self.ops.extend(self.doc.transact(|tx| tx.delete(key)));
        self.inverses.push(Change::Slot {
            key: key.to_vec(),
            value: prior,
        });
        self
    }
}

impl UndoManager {
    /// A manager tracking no history yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether there is a recorded intention to undo.
    pub fn can_undo(&self) -> bool {
        !self.undo.is_empty()
    }

    /// Whether there is an undone intention to redo.
    pub fn can_redo(&self) -> bool {
        !self.redo.is_empty()
    }

    /// Record several edits as one undo step, returning every op they emit. Undo
    /// reverts them together; an empty group records nothing.
    pub fn group<F>(&mut self, doc: &mut Document, edits: F) -> Vec<Op>
    where
        F: FnOnce(&mut Batch),
    {
        let mut batch = Batch {
            doc,
            inverses: Vec::new(),
            ops: Vec::new(),
        };
        edits(&mut batch);
        let Batch { inverses, ops, .. } = batch;
        if !inverses.is_empty() {
            self.undo.push(inverses);
            self.redo.clear();
        }
        ops
    }

    /// Install-or-set a root Register at `key` as its own undo step.
    pub fn register(&mut self, doc: &mut Document, key: &[u8], value: Scalar) -> Vec<Op> {
        self.group(doc, |b| {
            b.register(key, value);
        })
    }

    /// Install-or-increment a root Counter at `key` as its own undo step.
    pub fn inc(&mut self, doc: &mut Document, key: &[u8], amount: u32) -> Vec<Op> {
        self.group(doc, |b| {
            b.inc(key, amount);
        })
    }

    /// Install-or-decrement a root Counter at `key` as its own undo step.
    pub fn dec(&mut self, doc: &mut Document, key: &[u8], amount: u32) -> Vec<Op> {
        self.group(doc, |b| {
            b.dec(key, amount);
        })
    }

    /// Tombstone a root Register slot at `key` as its own undo step.
    pub fn delete(&mut self, doc: &mut Document, key: &[u8]) -> Vec<Op> {
        self.group(doc, |b| {
            b.delete(key);
        })
    }

    /// Revert the most recent intention, returning the ops to broadcast, or
    /// `None` if there is nothing to undo. The undone intention becomes redoable.
    pub fn undo(&mut self, doc: &mut Document) -> Option<Vec<Op>> {
        let intention = self.undo.pop()?;
        let (ops, inverse) = apply(doc, intention);
        self.redo.push(inverse);
        Some(ops)
    }

    /// Replay the most recently undone intention, returning the ops to broadcast,
    /// or `None` if there is nothing to redo. It becomes undoable again.
    pub fn redo(&mut self, doc: &mut Document) -> Option<Vec<Op>> {
        let intention = self.redo.pop()?;
        let (ops, inverse) = apply(doc, intention);
        self.undo.push(inverse);
        Some(ops)
    }
}

/// Apply an intention's inverses to `doc` — in reverse of the order they were
/// made, so the last edit is undone first — and return the ops they produced and
/// the mirror intention that would undo *this* application, so undo and redo
/// alternate over the same slots.
fn apply(doc: &mut Document, intention: Intention) -> (Vec<Op>, Intention) {
    let mut ops = Vec::new();
    let mut inverse = Vec::new();
    for change in intention.into_iter().rev() {
        let (o, inv) = apply_change(doc, change);
        ops.extend(o);
        inverse.push(inv);
    }
    (ops, inverse)
}

/// Apply one inverse change, returning its ops and its own inverse.
fn apply_change(doc: &mut Document, change: Change) -> (Vec<Op>, Change) {
    match change {
        Change::Slot { key, value } => {
            let current = read_register(doc, &key);
            let ops = match &value {
                Some(scalar) => {
                    let scalar = scalar.clone();
                    doc.transact(|tx| tx.register(&key, scalar))
                }
                None => doc.transact(|tx| tx.delete(&key)),
            };
            (
                ops,
                Change::Slot {
                    key,
                    value: current,
                },
            )
        }
        Change::Counter { key, inc, dec } => {
            let ops = doc.transact(|tx| {
                if inc > 0 {
                    tx.inc(&key, inc);
                }
                if dec > 0 {
                    tx.dec(&key, dec);
                }
            });
            // Undoing an (inc, dec) application is the mirrored (dec, inc).
            (
                ops,
                Change::Counter {
                    key,
                    inc: dec,
                    dec: inc,
                },
            )
        }
    }
}

/// The current value of a root Register slot, or `None` if the slot is empty or
/// holds a non-Register element.
fn read_register(doc: &Document, key: &[u8]) -> Option<Scalar> {
    match doc.get(key) {
        Some(Element::Register(reg)) => Some(reg.borrow().read().clone()),
        _ => None,
    }
}
