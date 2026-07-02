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
//! Register and a Counter — and the sequence types, List and Text: undo of an
//! insert deletes the node(s) it minted, and undo of a delete revives the
//! removed value as a fresh insert (the op log has no un-tombstone).

use crate::doc::Document;
use crate::op::{Op, OpKind};
use crate::path;
use crate::stamp::Stamp;
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
    /// Tombstone the list node `id` at `path` — the inverse of an insert.
    ListDeleteNode { path: Vec<u8>, id: Stamp },
    /// Re-insert `value` at live `index` in the list at `path` — the inverse of
    /// a delete. Revival is a fresh insert (the op log has no un-tombstone), so
    /// the value returns with a new id.
    ListInsertValue {
        path: Vec<u8>,
        index: usize,
        value: Vec<u8>,
    },
    /// Tombstone the text codepoints `ids` at `path` — the inverse of an insert.
    TextDeleteRun { path: Vec<u8>, ids: Vec<Stamp> },
    /// Re-insert `s` at codepoint `index` in the text at `path` — the inverse of
    /// a delete. As with a list, revival is a fresh insert with new char_ids.
    TextInsertRun {
        path: Vec<u8>,
        index: usize,
        s: String,
    },
}

/// The live substring of `count` codepoints from `index` in the text at `path`,
/// so a delete can capture what it removed for a later revival.
fn text_substring(doc: &Document, path: &[u8], index: usize, count: usize) -> Option<String> {
    let full = path::text_get(doc, path)?;
    Some(full.chars().skip(index).take(count).collect())
}

/// The id of the node a `ListInsert` op minted — its stamp — for the last insert
/// in `ops`, so an inverse can later delete exactly that node.
fn inserted_list_id(ops: &[Op]) -> Option<Stamp> {
    ops.iter().rev().find_map(|op| match op.kind {
        OpKind::ListInsert { .. } => Some(op.stamp),
        _ => None,
    })
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

    /// Insert `value` at live `index` in the List at `path`.
    pub fn list_insert(&mut self, path: &[u8], index: usize, value: &[u8]) -> &mut Self {
        let ops = path::list_insert(self.doc, path, index, value);
        if let Some(id) = inserted_list_id(&ops) {
            self.inverses.push(Change::ListDeleteNode {
                path: path.to_vec(),
                id,
            });
        }
        self.ops.extend(ops);
        self
    }

    /// Tombstone the live item at `index` in the List at `path`, capturing its
    /// value so an undo can revive it.
    pub fn list_delete(&mut self, path: &[u8], index: usize) -> &mut Self {
        let Some(value) = path::list_get(self.doc, path, index) else {
            return self;
        };
        let ops = path::list_delete(self.doc, path, index);
        if !ops.is_empty() {
            self.inverses.push(Change::ListInsertValue {
                path: path.to_vec(),
                index,
                value,
            });
        }
        self.ops.extend(ops);
        self
    }

    /// Insert `s` at codepoint `index` in the Text at `path`.
    pub fn text_insert(&mut self, path: &[u8], index: usize, s: &str) -> &mut Self {
        let ops = path::text_insert(self.doc, path, index, s);
        let count = s.chars().count();
        if !ops.is_empty() && count > 0 {
            let ids = path::text_run_ids(self.doc, path, index, count);
            if !ids.is_empty() {
                self.inverses.push(Change::TextDeleteRun {
                    path: path.to_vec(),
                    ids,
                });
            }
        }
        self.ops.extend(ops);
        self
    }

    /// Tombstone `count` codepoints from `index` in the Text at `path`, capturing
    /// them so an undo can revive the substring.
    pub fn text_delete(&mut self, path: &[u8], index: usize, count: usize) -> &mut Self {
        let ids = path::text_run_ids(self.doc, path, index, count);
        if ids.is_empty() {
            return self;
        }
        let s = text_substring(self.doc, path, index, ids.len()).unwrap_or_default();
        let ops = path::text_delete(self.doc, path, index, count);
        if !ops.is_empty() {
            self.inverses.push(Change::TextInsertRun {
                path: path.to_vec(),
                index,
                s,
            });
        }
        self.ops.extend(ops);
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

    /// Insert `value` at live `index` in the List at `path` as its own undo step.
    pub fn list_insert(
        &mut self,
        doc: &mut Document,
        path: &[u8],
        index: usize,
        value: &[u8],
    ) -> Vec<Op> {
        self.group(doc, |b| {
            b.list_insert(path, index, value);
        })
    }

    /// Tombstone the live item at `index` in the List at `path` as its own undo
    /// step.
    pub fn list_delete(&mut self, doc: &mut Document, path: &[u8], index: usize) -> Vec<Op> {
        self.group(doc, |b| {
            b.list_delete(path, index);
        })
    }

    /// Insert `s` at codepoint `index` in the Text at `path` as its own undo step.
    pub fn text_insert(
        &mut self,
        doc: &mut Document,
        path: &[u8],
        index: usize,
        s: &str,
    ) -> Vec<Op> {
        self.group(doc, |b| {
            b.text_insert(path, index, s);
        })
    }

    /// Tombstone `count` codepoints from `index` in the Text at `path` as its own
    /// undo step.
    pub fn text_delete(
        &mut self,
        doc: &mut Document,
        path: &[u8],
        index: usize,
        count: usize,
    ) -> Vec<Op> {
        self.group(doc, |b| {
            b.text_delete(path, index, count);
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
        Change::ListDeleteNode { path, id } => {
            // Capture where the node sits so the mirror can revive it in place.
            let index = path::list_live_index(doc, &path, id);
            let value = index.and_then(|i| path::list_get(doc, &path, i));
            let ops = path::list_delete_id(doc, &path, id);
            match (index, value) {
                (Some(index), Some(value)) => (ops, Change::ListInsertValue { path, index, value }),
                // Nothing live to delete: the inverse is inert.
                _ => (ops, Change::ListDeleteNode { path, id }),
            }
        }
        Change::ListInsertValue { path, index, value } => {
            let ops = path::list_insert(doc, &path, index, &value);
            match inserted_list_id(&ops) {
                Some(id) => (ops, Change::ListDeleteNode { path, id }),
                None => (ops, Change::ListInsertValue { path, index, value }),
            }
        }
        Change::TextDeleteRun { path, ids } => {
            // Capture the run's live extent so the mirror can revive it in place.
            let mut live: Vec<usize> = ids
                .iter()
                .filter_map(|id| path::text_live_index(doc, &path, *id))
                .collect();
            live.sort_unstable();
            let index = live.first().copied();
            let s = index.and_then(|i| text_substring(doc, &path, i, live.len()));
            let ops = path::text_delete_ids(doc, &path, &ids);
            match (index, s) {
                (Some(index), Some(s)) => (ops, Change::TextInsertRun { path, index, s }),
                // Nothing live to delete: the inverse is inert.
                _ => (ops, Change::TextDeleteRun { path, ids }),
            }
        }
        Change::TextInsertRun { path, index, s } => {
            let ops = path::text_insert(doc, &path, index, &s);
            let ids = path::text_run_ids(doc, &path, index, s.chars().count());
            if ids.is_empty() {
                (ops, Change::TextInsertRun { path, index, s })
            } else {
                (ops, Change::TextDeleteRun { path, ids })
            }
        }
    }
}
