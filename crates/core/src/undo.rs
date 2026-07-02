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

/// A user's undo/redo stacks over one [`Document`]. Each recorded edit pushes its
/// inverse; a fresh edit clears the redo stack, as an intervening edit makes the
/// redone future ambiguous.
#[derive(Default)]
pub struct UndoManager {
    undo: Vec<Change>,
    redo: Vec<Change>,
}

impl UndoManager {
    /// A manager tracking no history yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether there is a recorded edit to undo.
    pub fn can_undo(&self) -> bool {
        !self.undo.is_empty()
    }

    /// Whether there is an undone edit to redo.
    pub fn can_redo(&self) -> bool {
        !self.redo.is_empty()
    }

    /// Install-or-set a root integer/bytes/bool Register at `key`, recording the
    /// prior value so the edit can be undone. Returns the ops to broadcast.
    pub fn register(&mut self, doc: &mut Document, key: &[u8], value: Scalar) -> Vec<Op> {
        let prior = read_register(doc, key);
        let ops = doc.transact(|tx| tx.register(key, value));
        self.record(Change::Slot {
            key: key.to_vec(),
            value: prior,
        });
        ops
    }

    /// Install-or-increment a root Counter at `key`. The inverse is a matching
    /// decrement, so no prior value is needed. Returns the ops to broadcast.
    pub fn inc(&mut self, doc: &mut Document, key: &[u8], amount: u32) -> Vec<Op> {
        let ops = doc.transact(|tx| tx.inc(key, amount));
        self.record(Change::Counter {
            key: key.to_vec(),
            inc: 0,
            dec: amount,
        });
        ops
    }

    /// Install-or-decrement a root Counter at `key`; the inverse is a matching
    /// increment. Returns the ops to broadcast.
    pub fn dec(&mut self, doc: &mut Document, key: &[u8], amount: u32) -> Vec<Op> {
        let ops = doc.transact(|tx| tx.dec(key, amount));
        self.record(Change::Counter {
            key: key.to_vec(),
            inc: amount,
            dec: 0,
        });
        ops
    }

    /// Tombstone a root Register slot at `key`, recording its prior value so undo
    /// restores it. Returns the ops to broadcast.
    pub fn delete(&mut self, doc: &mut Document, key: &[u8]) -> Vec<Op> {
        let prior = read_register(doc, key);
        let ops = doc.transact(|tx| tx.delete(key));
        self.record(Change::Slot {
            key: key.to_vec(),
            value: prior,
        });
        ops
    }

    /// Revert the most recent tracked edit, returning the ops to broadcast, or
    /// `None` if there is nothing to undo. The undone edit becomes redoable.
    pub fn undo(&mut self, doc: &mut Document) -> Option<Vec<Op>> {
        let change = self.undo.pop()?;
        let (ops, inverse) = apply(doc, change);
        self.redo.push(inverse);
        Some(ops)
    }

    /// Replay the most recently undone edit, returning the ops to broadcast, or
    /// `None` if there is nothing to redo. The redone edit becomes undoable again.
    pub fn redo(&mut self, doc: &mut Document) -> Option<Vec<Op>> {
        let change = self.redo.pop()?;
        let (ops, inverse) = apply(doc, change);
        self.undo.push(inverse);
        Some(ops)
    }

    /// Push a new edit's inverse and drop the redo future it invalidates.
    fn record(&mut self, change: Change) {
        self.undo.push(change);
        self.redo.clear();
    }
}

/// Apply one inverse change to `doc`, returning the ops it produced and the
/// change that would undo *this* application (its own inverse), so undo and redo
/// alternate over the same slot.
fn apply(doc: &mut Document, change: Change) -> (Vec<Op>, Change) {
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
