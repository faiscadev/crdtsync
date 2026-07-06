//! The move-log half of the Kleppmann-2021 replicated tree move.
//!
//! A move is `(stamp, child, parent)`: at causal time `stamp`, `child` becomes a
//! child of `parent`. Moves are held in one lamport-ordered log and folded into a
//! parent relation (`child -> parent`). Because the log is kept in the total
//! [`Stamp`] order, an out-of-order arrival is absorbed by **undo-and-replay** —
//! every already-applied move with a greater stamp is undone, the newcomer is
//! applied, then the undone moves are re-applied — so every replica that has seen
//! the same set of moves holds the same relation regardless of arrival order.
//!
//! A move whose `parent` is the `child` itself, or a descendant of it, would form
//! a cycle; it is recorded in the log (so redo is order-independent) but leaves
//! the relation unchanged. This gives the four guarantees the tree needs: exactly
//! one parent per node, no cycles, no duplication, deterministic convergence.
//!
//! This module is pure edges — the position of a child among its siblings is a
//! Fugue concern the document layers on top; it never reaches here.

use std::collections::HashMap;

use crate::elementid::ElementId;
use crate::stamp::Stamp;

/// One recorded move, carrying the parent the child held *just before* it applied
/// so the move can be undone exactly when a lower-stamped move arrives late.
struct LogOp {
    stamp: Stamp,
    child: ElementId,
    parent: ElementId,
    prev_parent: Option<ElementId>,
}

/// The lamport-ordered move log and the parent relation it folds to.
#[derive(Default)]
pub struct TreeMoves {
    /// A moved node's overriding parent — an entry only while a move governs it.
    tree: HashMap<ElementId, ElementId>,
    /// The static parent every node was *created* under. It never changes (a
    /// create is permanent), so it is not in the log; it is the fallback parent
    /// when no move governs a node, and it lets the cycle check see the whole
    /// tree, not only the moved edges. A node absent from both maps is a root.
    base: HashMap<ElementId, ElementId>,
    /// The log, kept sorted ascending by `stamp`. Holds moves only — base edges
    /// are re-derived from the creates on the way in.
    log: Vec<LogOp>,
}

impl TreeMoves {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the parent a node was created under. Idempotent — a node's birth
    /// parent is fixed — and never undone, so it anchors the tree that both the
    /// fallback parent and the cycle check walk.
    pub fn set_base(&mut self, child: ElementId, parent: ElementId) {
        self.base.entry(child).or_insert(parent);
    }

    /// Record a move. Returns `false` (a no-op) if this exact stamp was already
    /// applied; otherwise absorbs it in stamp order and returns `true`.
    pub fn apply(&mut self, stamp: Stamp, child: ElementId, parent: ElementId) -> bool {
        // The insertion point in the stamp-ordered log doubles as the dedup
        // probe: a stamp already present sits exactly here (stamps are unique).
        let at = self.log.partition_point(|op| op.stamp < stamp);
        if self.log.get(at).is_some_and(|op| op.stamp == stamp) {
            return false;
        }
        // Undo every later move, splice this one into stamp order, redo the rest.
        // The redo re-derives each move against the new intermediate tree, so a
        // move that now (or no longer) cycles is resolved consistently.
        let later: Vec<LogOp> = self.log.split_off(at);
        for op in later.iter().rev() {
            self.undo(op);
        }
        let recorded = self.redo(stamp, child, parent);
        self.log.push(recorded);
        for op in later {
            let recorded = self.redo(op.stamp, op.child, op.parent);
            self.log.push(recorded);
        }
        true
    }

    /// The current effective parent of `child`: its move-overriding parent if one
    /// governs it, else the parent it was created under, else `None` (a root).
    pub fn parent_of(&self, child: ElementId) -> Option<ElementId> {
        self.tree
            .get(&child)
            .or_else(|| self.base.get(&child))
            .copied()
    }

    /// Every `(child, parent)` edge in the current relation, ordered by child id
    /// so a consumer sees the same sequence on every replica — the underlying map
    /// iterates nondeterministically.
    pub fn edges(&self) -> impl Iterator<Item = (ElementId, ElementId)> {
        let mut edges: Vec<(ElementId, ElementId)> =
            self.tree.iter().map(|(&c, &p)| (c, p)).collect();
        edges.sort_by_key(|(child, _)| child.as_bytes());
        edges.into_iter()
    }

    /// The number of recorded moves.
    pub fn len(&self) -> usize {
        self.log.len()
    }

    pub fn is_empty(&self) -> bool {
        self.log.is_empty()
    }

    /// Apply one move to the live tree, returning its log entry. A move that
    /// would put `child` under itself or a descendant is a cycle: it is recorded
    /// but changes nothing, so undo restores the same state and redo skips it
    /// again — order-independent by construction.
    fn redo(&mut self, stamp: Stamp, child: ElementId, parent: ElementId) -> LogOp {
        let prev_parent = self.tree.get(&child).copied();
        if child != parent && !self.is_ancestor(child, parent) {
            self.tree.insert(child, parent);
        }
        LogOp {
            stamp,
            child,
            parent,
            prev_parent,
        }
    }

    /// Restore the parent `child` held before `op` applied.
    fn undo(&mut self, op: &LogOp) {
        match op.prev_parent {
            Some(p) => {
                self.tree.insert(op.child, p);
            }
            None => {
                self.tree.remove(&op.child);
            }
        }
    }

    /// Whether `a` is an ancestor of `b` in the effective tree — walk `b` upward
    /// through move-overriding and created parents alike, so a move under a node's
    /// *created* descendant is caught as a cycle, not only a move under a moved
    /// one. The tree is an invariant-acyclic forest, so the walk terminates; a
    /// length guard over the node count makes it total against a corrupt relation.
    fn is_ancestor(&self, a: ElementId, b: ElementId) -> bool {
        let mut cur = b;
        for _ in 0..=self.tree.len() + self.base.len() {
            match self.parent_of(cur) {
                Some(p) if p == a => return true,
                Some(p) => cur = p,
                None => return false,
            }
        }
        // Only reachable if the relation already held a cycle: treat as ancestral
        // so the guarded move is skipped rather than compounding it.
        true
    }
}
