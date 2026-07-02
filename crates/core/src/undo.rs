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
//! Edits are addressed by path (see [`crate::path`]), so a slot inside a nested
//! Map undoes as readily as a root one. This helper covers scalar slots — a
//! Register and a Counter. List and text edits — whose inverses need element
//! revival — are layered on in later work.

use crate::doc::Document;
use crate::op::Op;
use crate::path;
use crate::Scalar;

/// The inverse of one recorded edit — what to replay to undo it. Applying an
/// inverse yields the change that would in turn undo *it*, which is what makes
/// undo and redo symmetric.
enum Change {
    /// Restore the Register slot at `path` to `value`, or delete it if the slot
    /// held nothing before the edit.
    Slot {
        path: Vec<u8>,
        value: Option<Scalar>,
    },
    /// Apply this counter delta at `path` — one direction to cancel the recorded
    /// one.
    Counter { path: Vec<u8>, inc: u32, dec: u32 },
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
    /// Install-or-set the Register at `path`.
    pub fn register(&mut self, path: &[u8], value: Scalar) -> &mut Self {
        let prior = path::get_register(self.doc, path);
        self.ops.extend(path::register(self.doc, path, value));
        self.inverses.push(Change::Slot {
            path: path.to_vec(),
            value: prior,
        });
        self
    }

    /// Install-or-increment the Counter at `path`.
    pub fn inc(&mut self, path: &[u8], amount: u32) -> &mut Self {
        self.ops.extend(path::inc(self.doc, path, amount));
        self.inverses.push(Change::Counter {
            path: path.to_vec(),
            inc: 0,
            dec: amount,
        });
        self
    }

    /// Install-or-decrement the Counter at `path`.
    pub fn dec(&mut self, path: &[u8], amount: u32) -> &mut Self {
        self.ops.extend(path::dec(self.doc, path, amount));
        self.inverses.push(Change::Counter {
            path: path.to_vec(),
            inc: amount,
            dec: 0,
        });
        self
    }

    /// Tombstone the Register slot at `path`.
    pub fn delete(&mut self, path: &[u8]) -> &mut Self {
        let prior = path::get_register(self.doc, path);
        self.ops.extend(path::delete(self.doc, path));
        self.inverses.push(Change::Slot {
            path: path.to_vec(),
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

    /// Install-or-set the Register at `path` as its own undo step.
    pub fn register(&mut self, doc: &mut Document, path: &[u8], value: Scalar) -> Vec<Op> {
        self.group(doc, |b| {
            b.register(path, value);
        })
    }

    /// Install-or-increment the Counter at `path` as its own undo step.
    pub fn inc(&mut self, doc: &mut Document, path: &[u8], amount: u32) -> Vec<Op> {
        self.group(doc, |b| {
            b.inc(path, amount);
        })
    }

    /// Install-or-decrement the Counter at `path` as its own undo step.
    pub fn dec(&mut self, doc: &mut Document, path: &[u8], amount: u32) -> Vec<Op> {
        self.group(doc, |b| {
            b.dec(path, amount);
        })
    }

    /// Tombstone the Register slot at `path` as its own undo step.
    pub fn delete(&mut self, doc: &mut Document, path: &[u8]) -> Vec<Op> {
        self.group(doc, |b| {
            b.delete(path);
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
        Change::Slot { path, value } => {
            let current = path::get_register(doc, &path);
            let ops = match value {
                Some(scalar) => path::register(doc, &path, scalar),
                None => path::delete(doc, &path),
            };
            (
                ops,
                Change::Slot {
                    path,
                    value: current,
                },
            )
        }
        Change::Counter { path, inc, dec } => {
            let ops = if inc > 0 {
                path::inc(doc, &path, inc)
            } else if dec > 0 {
                path::dec(doc, &path, dec)
            } else {
                Vec::new()
            };
            // Undoing an (inc, dec) application is the mirrored (dec, inc).
            (
                ops,
                Change::Counter {
                    path,
                    inc: dec,
                    dec: inc,
                },
            )
        }
    }
}
