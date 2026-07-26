//! Per-user undo / redo — the record-seam every locally emitted edit passes
//! through, and the origin-scoped handle over it.
//!
//! Undo in a CRDT is not op reversal: the engine sees only ordinary forward ops
//! that restore the previously observed value, so there is no server-side undo
//! state and no special wire format. A [`Document`] records the inverse of every
//! op it *emits* — whatever surface authored it, cursor or path façade, offline
//! replica or live channel — so an SDK that edits through its own handle graph
//! gets undo without routing through this module at all. A remote op is folded
//! in by `apply`, never emitted, so a collaborator's edit can never land on this
//! replica's stack: global undo stays deliberately unsupported.
//!
//! Recording is opt-in and tagged: [`Document::set_undo_origin`] names the
//! **origin** subsequent edits record under, and undo selects by that tag — a
//! user undoes their own intentions, and an app that wants an undo manager
//! scoped to a subtree edits that subtree under its own origin. A document with
//! no origin set records nothing, so a server replica carries no history.
//!
//! An undo step is one *intention* — the edits of a single transact, of an
//! explicit [`Document::begin_intention`] group, or of one atomic transaction,
//! which undoes and redoes as one atomic transaction in turn. Replaying an
//! intention emits ordinary forward ops, and those ops are themselves recorded,
//! so the mirror intention that would undo the undo is derived from live state
//! rather than guessed at record time — which is what makes undo and redo
//! symmetric across any number of alternations.
//!
//! Revival is a fresh insert: the op log has no un-tombstone, so undoing a
//! delete re-creates the removed value with a new id. A deleted composite
//! sequence node (an XML child) is re-created from a snapshot of its subtree
//! taken at record time, so undoing "delete this paragraph" brings its text
//! back rather than an empty shell.

use crate::acl::{AclEffect, AclGrant, AclScope, AclSubject};
use crate::clientid::ClientId;
use crate::doc::Document;
use crate::elementid::ElementId;
use crate::list::Anchor;
use crate::op::{Op, OpKind};
use crate::ranged::RangeAnchor;
use crate::scalar::Scalar;
use crate::stamp::Stamp;
use std::collections::HashMap;

/// How deep a subtree snapshot descends. A delete of a composite sequence node
/// captures its subtree so an undo can rebuild it; past this depth the capture
/// stops, so a pathologically nested tree cannot drive the recursive walk (or
/// the rebuild that replays it) off the stack. Far deeper than any document a
/// person edits.
pub(crate) const MAX_SNAPSHOT_DEPTH: u32 = 64;

/// A captured element, deep enough to rebuild it out of ordinary forward ops.
/// Held only for the values a delete makes unrecoverable — a sequence node's
/// contents and a RangedElement's payload. A slot's container needs no snapshot:
/// its handle is retained by id, so re-creating the slot restores the same
/// logical element with its content intact.
pub(crate) enum Snap {
    /// A bare map value.
    Scalar(Scalar),
    /// A register slot, restored as a register rather than a bare value.
    Register(Scalar),
    /// A counter's net total, replayed as increments toward it.
    Counter(i64),
    Map(Vec<(Vec<u8>, Snap)>),
    List(Vec<Scalar>),
    Text(String),
    XmlElement {
        tag: Vec<u8>,
        attrs: Vec<(Vec<u8>, Snap)>,
        children: Vec<Snap>,
    },
    XmlFragment {
        children: Vec<Snap>,
    },
}

/// One inverse action: what to emit to undo a single recorded op.
///
/// Most are a lone forward op. The revivals are not: the op log has no
/// un-tombstone, so bringing a deleted item back mints a *fresh* id, and the
/// document has to know both what to rebuild and which id the new one replaces
/// — an intention still on the stack names the ids its own edit minted, so a
/// later replay has to follow the substitution.
pub(crate) enum Step {
    /// Emit `kind` against `target`.
    Op { target: ElementId, kind: OpKind },
    /// Re-insert `value` in the sequence `list` at `anchor`, replacing the
    /// deleted node `was`.
    ReviveItem {
        list: ElementId,
        anchor: Anchor,
        value: Scalar,
        was: Stamp,
    },
    /// Re-insert `s` in the text `text` at `anchor`, replacing the deleted
    /// codepoints `was` (in sequence order).
    ReviveRun {
        text: ElementId,
        anchor: Anchor,
        s: String,
        was: Vec<Stamp>,
    },
    /// Re-create a deleted sequence node in the children list `list` at
    /// `anchor`, rebuilding its subtree, replacing the deleted node `was` — its
    /// sequence id, and the stable element id `was_node` that ops elsewhere on
    /// the stack (a move) address it by.
    ReviveNode {
        list: ElementId,
        anchor: Anchor,
        node: Snap,
        was: Stamp,
        was_node: ElementId,
    },
    /// Re-create a deleted RangedElement over the same span, rebuilding its
    /// payload, replacing the annotation `was`.
    Ranged {
        start: RangeAnchor,
        end: RangeAnchor,
        name: Option<Vec<u8>>,
        payload: Snap,
        was: ElementId,
    },
    /// Re-issue a revoked ACL tuple. A revoke is terminal, so the tuple comes
    /// back under a fresh id, replacing `was` — which the intentions still
    /// stacked beneath name it by.
    Regrant {
        target: ElementId,
        subject: AclSubject,
        grant: AclGrant,
        effect: AclEffect,
        scope: AclScope,
        grantor: ClientId,
        was: ElementId,
    },
}

/// One undo step: the inverses of the edits of a single intention, in the order
/// those edits were made, tagged with the origin that authored them. `atomic`
/// records that they were made as an atomic transaction, so the undo (and the
/// redo) replays as one atomic transaction too — a peer never sees a partially
/// undone group.
pub(crate) struct Intention {
    pub(crate) origin: Vec<u8>,
    pub(crate) steps: Vec<Step>,
    pub(crate) atomic: bool,
}

/// Which stack a closing intention lands on. A fresh edit lands on `Undo`; the
/// mirror of a replayed undo lands on `Redo`, and the mirror of a replayed redo
/// back on `Undo`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Landing {
    Undo,
    Redo,
}

/// A document's recorded undo history: the intention being recorded, and the
/// undo and redo stacks it closes onto.
pub(crate) struct History {
    /// The origin subsequent edits record under, or `None` to record nothing.
    origin: Option<Vec<u8>>,
    /// Inverses of the edits of the intention in progress, in emission order.
    open: Vec<Step>,
    /// Nesting of explicit [`Document::begin_intention`] groups; while non-zero,
    /// a transact boundary does not close the intention.
    depth: u32,
    landing: Landing,
    /// Whether the edits being recorded are the replay of an existing intention,
    /// so closing the mirror must not clear the future it came from.
    replaying: bool,
    undo: Vec<Intention>,
    redo: Vec<Intention>,
    /// Sequence ids a revival re-minted, keyed by the sequence they live in: the
    /// old id → the id it came back with. A tombstone is terminal, so undoing a
    /// delete re-inserts the value under a *fresh* id — while the intentions
    /// still stacked beneath name the ids their own edit minted. Following the
    /// substitution is what makes a second undo ("type, delete, undo, undo")
    /// reach the revived characters instead of leaving them behind.
    ///
    /// Keyed by the sequence, not by the id alone: an op is stamped from its own
    /// zone's clock, so a `Stamp` is unique per zone, not per document — two
    /// zones mint the same one, and a bare-id map would re-point one zone's
    /// delete at the other's revival.
    revived: HashMap<(ElementId, Stamp), Stamp>,
    /// Stable element ids a revival re-minted, old → new. The sequence-id map
    /// above re-points a *delete*; this one re-points everything that names an
    /// element directly — a tree move of a revived node, a payload change or a
    /// delete of a revived annotation, a revoke of a re-issued ACL tuple.
    /// Element ids are derived from a parent and a key or from a root-partition
    /// stamp, so they need no sequence qualifier.
    revived_elements: HashMap<ElementId, ElementId>,
}

impl Default for History {
    fn default() -> Self {
        Self {
            origin: None,
            open: Vec::new(),
            depth: 0,
            landing: Landing::Undo,
            replaying: false,
            undo: Vec::new(),
            redo: Vec::new(),
            revived: HashMap::new(),
            revived_elements: HashMap::new(),
        }
    }
}

impl History {
    /// Whether emitted edits are being recorded.
    pub(crate) fn recording(&self) -> bool {
        self.origin.is_some()
    }

    /// The origin edits record under, if any.
    pub(crate) fn origin(&self) -> Option<&[u8]> {
        self.origin.as_deref()
    }

    /// Record under `origin` from now on. An intention in progress belongs to the
    /// origin that opened it, so switching closes it rather than dropping it —
    /// an edit that was recorded must stay undoable by whoever recorded it.
    pub(crate) fn track(&mut self, origin: &[u8]) {
        if self.origin.as_deref() != Some(origin) {
            self.close(false);
        }
        self.origin = Some(origin.to_vec());
    }

    /// Stop recording. The intention in progress closes under the origin that
    /// opened it, and the closed stacks are kept — an app that pauses recording
    /// can still undo everything it recorded.
    pub(crate) fn untrack(&mut self) {
        self.close(false);
        self.origin = None;
    }

    /// The id `id` in sequence `seq` came back as, following a chain of
    /// revivals. Each hop consumes a distinct entry, so the map size bounds the
    /// walk and a cycle (unreachable — a revival always mints a fresh, strictly
    /// later id) exits at the bound rather than spinning.
    pub(crate) fn current(&self, seq: ElementId, id: Stamp) -> Stamp {
        let mut id = id;
        for _ in 0..self.revived.len() {
            match self.revived.get(&(seq, id)) {
                Some(&next) if next != id => id = next,
                _ => break,
            }
        }
        id
    }

    /// Record that `was` came back as `now` in sequence `seq`.
    pub(crate) fn substitute(&mut self, seq: ElementId, was: Stamp, now: Stamp) {
        if was != now {
            self.revived.insert((seq, was), now);
        }
    }

    /// Drop everything recorded, keeping the recording origin. For a boundary
    /// past which the recorded inverses no longer describe the document — a
    /// migration rewrites the slot shapes they name.
    pub(crate) fn forget(&mut self) {
        self.open.clear();
        self.undo.clear();
        self.redo.clear();
        self.revived.clear();
        self.revived_elements.clear();
    }

    /// The element `id` came back as, following a chain of revivals.
    pub(crate) fn current_element(&self, id: ElementId) -> ElementId {
        let mut id = id;
        for _ in 0..self.revived_elements.len() {
            match self.revived_elements.get(&id) {
                Some(&next) if next != id => id = next,
                _ => break,
            }
        }
        id
    }

    /// Record that element `was` came back as `now`.
    pub(crate) fn substitute_element(&mut self, was: ElementId, now: ElementId) {
        if was != now {
            self.revived_elements.insert(was, now);
        }
    }

    /// Add an inverse to the intention in progress.
    pub(crate) fn push(&mut self, step: Step) {
        self.open.push(step);
    }

    /// Whether a transact boundary should close the intention in progress —
    /// false while an explicit group is open.
    pub(crate) fn grouped(&self) -> bool {
        self.depth > 0
    }

    pub(crate) fn open_group(&mut self) {
        self.depth += 1;
    }

    /// Close one level of explicit grouping, reporting whether the outermost
    /// just closed.
    pub(crate) fn close_group(&mut self) -> bool {
        self.depth = self.depth.saturating_sub(1);
        self.depth == 0
    }

    /// Push the intention in progress, if it recorded anything. A fresh edit
    /// drops the redo future of its own origin — an intervening edit makes that
    /// origin's redone future ambiguous — while leaving other origins' alone.
    pub(crate) fn close(&mut self, atomic: bool) {
        // A replay always closes a mirror, even an empty one: an intention whose
        // inverses all turned out to be inert still came off its stack, and
        // dropping the mirror would leave N undos answered by N-1 redos.
        if self.open.is_empty() && !self.replaying {
            return;
        }
        let intention = Intention {
            origin: self.origin.clone().unwrap_or_default(),
            steps: std::mem::take(&mut self.open),
            atomic,
        };
        match self.landing {
            Landing::Undo => {
                if !self.replaying {
                    self.redo.retain(|i| i.origin != intention.origin);
                }
                self.undo.push(intention);
            }
            Landing::Redo => self.redo.push(intention),
        }
    }

    /// Drop the substitution maps once no stacked intention can name an id a
    /// revival replaced. Called after a replay settles, which is the only point
    /// at which both stacks can be empty.
    pub(crate) fn prune(&mut self) {
        if self.undo.is_empty() && self.redo.is_empty() {
            self.revived.clear();
            self.revived_elements.clear();
        }
    }

    /// Whether `origin` has a recorded intention to undo.
    pub(crate) fn can_undo(&self, origin: &[u8]) -> bool {
        self.undo.iter().any(|i| i.origin == origin)
    }

    /// Whether `origin` has an undone intention to redo.
    pub(crate) fn can_redo(&self, origin: &[u8]) -> bool {
        self.redo.iter().any(|i| i.origin == origin)
    }

    /// Take `origin`'s most recent intention off the named stack — the newest
    /// one it authored, skipping any another origin interleaved.
    pub(crate) fn take(&mut self, origin: &[u8], from: Landing) -> Option<Intention> {
        let stack = match from {
            Landing::Undo => &mut self.undo,
            Landing::Redo => &mut self.redo,
        };
        let at = stack.iter().rposition(|i| i.origin == origin)?;
        Some(stack.remove(at))
    }

    /// Enter a replay recording the mirror onto `landing`, returning the origin
    /// and landing to restore afterwards.
    pub(crate) fn begin_replay(
        &mut self,
        origin: &[u8],
        landing: Landing,
    ) -> (Option<Vec<u8>>, Landing) {
        let saved = (self.origin.take(), self.landing);
        self.origin = Some(origin.to_vec());
        self.landing = landing;
        self.replaying = true;
        saved
    }

    /// Leave a replay, restoring the recording context it interrupted.
    pub(crate) fn end_replay(&mut self, saved: (Option<Vec<u8>>, Landing)) {
        self.origin = saved.0;
        self.landing = saved.1;
        self.replaying = false;
    }
}

/// An origin-scoped handle over a document's recorded history: the thin surface
/// an SDK exposes as `undo` / `redo` / `canUndo` / `canRedo`. It holds no
/// history of its own — the stacks live in the [`Document`], which is what makes
/// undo work identically on an offline replica and on a live channel's replica.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct UndoManager {
    origin: Vec<u8>,
}

impl Default for UndoManager {
    fn default() -> Self {
        Self::new()
    }
}

/// The origin a manager records under when none is named.
pub const DEFAULT_ORIGIN: &[u8] = b"local";

impl UndoManager {
    /// A manager over the default origin.
    pub fn new() -> Self {
        Self::with_origin(DEFAULT_ORIGIN)
    }

    /// A manager over `origin` — the tag its edits record under and the one its
    /// undo selects by.
    pub fn with_origin(origin: &[u8]) -> Self {
        Self {
            origin: origin.to_vec(),
        }
    }

    /// The origin this manager records and selects by.
    pub fn origin(&self) -> &[u8] {
        &self.origin
    }

    /// Start recording `doc`'s emitted edits under this manager's origin.
    pub fn track(&self, doc: &mut Document) {
        doc.set_undo_origin(&self.origin);
    }

    /// Stop recording `doc`'s edits. The recorded stacks are kept.
    pub fn untrack(&self, doc: &mut Document) {
        doc.clear_undo_origin();
    }

    /// Whether this origin has an intention to undo on `doc`.
    pub fn can_undo(&self, doc: &Document) -> bool {
        doc.can_undo(&self.origin)
    }

    /// Whether this origin has an undone intention to redo on `doc`.
    pub fn can_redo(&self, doc: &Document) -> bool {
        doc.can_redo(&self.origin)
    }

    /// Revert this origin's most recent intention, returning the ops to
    /// broadcast, or `None` if there is nothing to undo.
    pub fn undo(&self, doc: &mut Document) -> Option<Vec<Op>> {
        doc.undo(&self.origin)
    }

    /// Replay this origin's most recently undone intention, returning the ops to
    /// broadcast, or `None` if there is nothing to redo.
    pub fn redo(&self, doc: &mut Document) -> Option<Vec<Op>> {
        doc.redo(&self.origin)
    }

    /// Record everything `edits` does as one intention, tracked under this
    /// manager's origin. Undo reverts the whole group.
    pub fn group<T, F>(&self, doc: &mut Document, edits: F) -> T
    where
        F: FnOnce(&mut Document) -> T,
    {
        self.track(doc);
        doc.begin_intention();
        let out = edits(doc);
        doc.end_intention();
        out
    }

    /// Like [`group`](Self::group), but the edits form one atomic transaction:
    /// their ops ship as a group a peer folds in all-or-nothing, and a later undo
    /// (or redo) of the intention replays as one atomic transaction too. Returns
    /// the group's ops.
    pub fn atomic_group<F>(&self, doc: &mut Document, edits: F) -> Vec<Op>
    where
        F: FnOnce(&mut Document),
    {
        self.track(doc);
        doc.begin_atomic();
        edits(doc);
        doc.commit_atomic()
    }
}
