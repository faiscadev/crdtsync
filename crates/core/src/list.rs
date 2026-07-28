//! List — an ordered sequence CRDT (Fugue).
//!
//! Items live in a tree: each insert attaches to a neighbour and the sequence
//! is the tree's in-order traversal. A run typed left-to-right forms a spine,
//! so two concurrent runs at the same gap stay whole and ordered by their
//! first item's stamp instead of interleaving. Deletes tombstone — a position
//! must survive to anchor inserts placed against it. Deleted ids are held run
//! length compressed: a contiguous delete removes ids that chain, so it costs
//! one record whatever its length, and live memory tracks the number of runs
//! rather than the number of deletions. The same algorithm backs Text.

use crate::anchor::RelativePosition;
use crate::clientid::ClientId;
use crate::codec::{len_u32, put_anchor, put_stamp, put_u32, put_u8, Cursor, DecodeError};
use crate::element::Element;
use crate::elementid::{ElementId, ElementKind};
use crate::scalar::Scalar;
use crate::stamp::Stamp;
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// The most ids one encoded run record may cover. The encoder splits a longer
/// run into chained records, so a record above this is malformed — and this cap
/// is what makes that split safe to invert. **Do not relax it.** It carries two
/// duties. It keeps decode canonical: a decoded run welds its records back, and
/// re-encoding re-splits on the same boundaries only because every record it
/// came from was capped here. And it bounds re-encode work: the split loop runs
/// `len / MAX_TOMBSTONE_RUN` times, so capping each record keeps that at most
/// the number of records the run was built from — linear in the input, with no
/// ratio for a crafted stream to exploit.
///
/// A sequence's *total* deleted count is deliberately unbounded. A run costs one
/// `DeadRun` however many ids it covers, so nothing amplifies with it, and a
/// long-lived document must be able to load its own snapshot back.
const MAX_TOMBSTONE_RUN: u32 = 1 << 20;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Side {
    Left,
    Right,
}

/// A decoded live node whose value is a composite reference — its Fugue stamp
/// plus the kind + id to resolve against the document's registries. Scalar nodes
/// are inlined and need no second pass; only these are returned for resolution.
pub(crate) type NodeRef = (Stamp, ElementKind, ElementId);

/// Storage order for an id: client, then sub-lamport offset, then lamport — so
/// the ids one delete run covers are contiguous and an interval query is a
/// range scan. Distinct from [`Stamp`]'s own order, which decides sequence
/// tiebreaks.
type SeqKey = (ClientId, u64, u64);

fn seq_key(id: &Stamp) -> SeqKey {
    (id.client, id.offset, id.lamport)
}

fn seq_stamp(key: &SeqKey) -> Stamp {
    Stamp {
        lamport: key.2,
        client: key.0,
        offset: key.1,
    }
}

/// The id `i` places after a run head — a plain lamport step, since a run's ids
/// share a client and offset. A text run that reaches the lamport ceiling carries
/// its surplus ids in a higher offset ([`Stamp::run_member`]), which is a
/// different key group, so no run can span the ceiling and the step never
/// overflows.
fn run_id(head: Stamp, i: u64) -> Stamp {
    Stamp {
        lamport: head
            .lamport
            .checked_add(i)
            .expect("a deleted run ends at a materialised id"),
        ..head
    }
}

/// Encode a live node's value: a scalar inline, any composite as a kind-tagged
/// reference to its child's id. The first byte is the [`ElementKind`] tag, so a
/// scalar (tag 0) is told from a reference (tags 2..=7) with no extra
/// discriminant — the sequence codec mirrors the map slot codec.
fn put_node_value(out: &mut Vec<u8>, value: &Element) {
    match value {
        Element::Scalar(s) => {
            put_u8(out, ElementKind::Scalar as u8);
            s.encode_state_into(out);
        }
        Element::Counter(c) => put_node_ref(out, ElementKind::Counter, c.borrow().id()),
        Element::Map(m) => put_node_ref(out, ElementKind::Map, m.borrow().id()),
        Element::List(l) => put_node_ref(out, ElementKind::List, l.borrow().id()),
        Element::Text(t) => put_node_ref(out, ElementKind::Text, t.borrow().id()),
        Element::XmlElement(x) => put_node_ref(out, ElementKind::XmlElement, x.borrow().id()),
        Element::XmlFragment(f) => put_node_ref(out, ElementKind::XmlFragment, f.borrow().id()),
        // A register only ever lives inline in a map slot, never as a sequence node.
        Element::Register(_) => unreachable!("a sequence node never holds a bare register"),
    }
}

fn put_node_ref(out: &mut Vec<u8>, kind: ElementKind, id: ElementId) {
    put_u8(out, kind as u8);
    out.extend_from_slice(&id.as_bytes());
}

/// Where a new node attaches in the Fugue tree: a parent node (or the root
/// when `None`) and the side it hangs on. Computed once at insert time so the
/// placement is replica-independent.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Anchor {
    pub parent: Option<Stamp>,
    pub side: Side,
}

struct Node {
    id: Stamp,
    value: Element,
    parent: Option<Stamp>,
    side: Side,
    /// Suppressed by a tree move: the node still anchors the Fugue tree but a live
    /// read skips it, because the element it holds now renders under a different
    /// parent. Unlike a delete this is reversible — the document sets it from
    /// the move-log fold, so an undo-and-replay can re-instate the placement.
    /// That is why a suppressed node stays a node while it is live; deleting it
    /// folds it into a run like any other, the delete being terminal.
    moved_away: bool,
}

impl Node {
    /// Whether a live read skips this node — it is moved under another parent.
    /// Fugue positioning still keeps it (it anchors later inserts).
    fn hidden(&self) -> bool {
        self.moved_away
    }

    fn deep_clone(&self) -> Self {
        Self {
            id: self.id,
            value: self.value.deep_clone(),
            parent: self.parent,
            side: self.side,
            moved_away: self.moved_away,
        }
    }
}

/// A run of deleted ids kept as one record: `len` ids counting up in lamport
/// from the run head (which shares their client and offset), the head placed at
/// `parent`/`side` and every later id the right child of its predecessor — the
/// chain a contiguous delete leaves behind. Deleted values are dropped: a
/// tombstone only has to hold a position, which is why the state codec drops
/// them too.
#[derive(Clone)]
struct DeadRun {
    len: u64,
    parent: Option<Stamp>,
    side: Side,
}

/// One record in sequence order: a node that carries a value, or a stretch of
/// deleted ids. The walk works in these, so a deleted region never costs a step
/// per deleted item.
#[derive(Clone, Copy)]
enum Span {
    Node(Stamp),
    Dead { head: Stamp, len: u64 },
}

impl Span {
    fn head(self) -> Stamp {
        match self {
            Span::Node(id) => id,
            Span::Dead { head, .. } => head,
        }
    }

    fn last(self) -> Stamp {
        match self {
            Span::Node(id) => id,
            Span::Dead { head, len } => run_id(head, len - 1),
        }
    }
}

pub struct List {
    id: ElementId,
    /// Every id that still carries a value — live, or suppressed by a tree move.
    nodes: BTreeMap<SeqKey, Node>,
    /// Deleted ids, run-length compressed and keyed by run head.
    dead: BTreeMap<SeqKey, DeadRun>,
    displaced: Cell<bool>,
}

impl List {
    pub fn new(id: ElementId) -> Self {
        Self {
            id,
            nodes: BTreeMap::new(),
            dead: BTreeMap::new(),
            displaced: Cell::new(false),
        }
    }

    pub fn id(&self) -> ElementId {
        self.id
    }

    /// Append this list's state to `out` in two sections: the nodes that carry a
    /// value in full, then the deleted ids as runs.
    ///
    /// A tombstone must survive to anchor later inserts, but its value is never
    /// read again, so a run record carries only (start, length, the run head's
    /// anchor) and a deleted region costs O(runs). Runs are maximal in the live
    /// structure, so the encoding is a function of the logical state: the node
    /// section is stamp-ordered and the runs are emitted in run-head stamp
    /// order, each split run's chunks in ascending offset — equal states encode
    /// identically.
    pub(crate) fn encode_state_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.id.as_bytes());

        let mut live: Vec<&Node> = self.nodes.values().collect();
        live.sort_by_key(|n| n.id);
        put_u32(out, len_u32(live.len()));
        for node in live {
            put_stamp(out, &node.id);
            put_node_value(out, &node.value);
            put_anchor(
                out,
                &Anchor {
                    parent: node.parent,
                    side: node.side,
                },
            );
        }

        let mut runs: Vec<(Stamp, u64, Anchor)> = self
            .dead
            .iter()
            .map(|(key, run)| {
                (
                    seq_stamp(key),
                    run.len,
                    Anchor {
                        parent: run.parent,
                        side: run.side,
                    },
                )
            })
            .collect();
        runs.sort_by_key(|(head, _, _)| *head);

        // Split any run past the cap into chained chunks so the decoder's bound
        // never rejects state this encoder produced.
        let mut chunk_count = 0u64;
        let mut chunks: Vec<u8> = Vec::new();
        for (start, len, anchor) in &runs {
            let mut off = 0u64;
            while off < *len {
                let chunk_len = (*len - off).min(MAX_TOMBSTONE_RUN as u64);
                let chunk_anchor = if off == 0 {
                    *anchor
                } else {
                    Anchor {
                        parent: Some(run_id(*start, off - 1)),
                        side: Side::Right,
                    }
                };
                put_stamp(&mut chunks, &run_id(*start, off));
                put_u32(&mut chunks, chunk_len as u32);
                put_anchor(&mut chunks, &chunk_anchor);
                chunk_count += 1;
                off += chunk_len;
            }
        }
        put_u32(
            out,
            u32::try_from(chunk_count).expect("codec: tombstone run count exceeds u32"),
        );
        out.extend_from_slice(&chunks);
    }

    /// Read a list from `cur`, advancing it. Mirrors [`encode_state_into`]: the
    /// valued nodes in full, then the deleted runs. Composite nodes come back
    /// holding a placeholder value with their reference returned alongside, for
    /// the document to resolve against its registries in a second pass (as map
    /// slots resolve).
    pub(crate) fn decode_state_from(cur: &mut Cursor) -> Result<(List, Vec<NodeRef>), DecodeError> {
        let id = cur.element_id()?;
        // Grow the maps as records are read rather than trusting a count to size
        // the reservation, so a bogus length fails on the missing bytes.
        let mut list = List::new(id);
        let mut refs: Vec<NodeRef> = Vec::new();

        let live_count = cur.u32()?;
        for _ in 0..live_count {
            let node_id = cur.stamp()?;
            let value = match cur.u8()? {
                // ElementKind::Scalar tag: an inline scalar.
                0 => Element::Scalar(cur.scalar()?),
                tag => {
                    let kind = match ElementKind::from_tag(tag) {
                        Some(
                            k @ (ElementKind::Counter
                            | ElementKind::Map
                            | ElementKind::List
                            | ElementKind::Text
                            | ElementKind::XmlElement
                            | ElementKind::XmlFragment),
                        ) => k,
                        _ => {
                            return Err(DecodeError::BadTag {
                                what: "list node value",
                                tag,
                            })
                        }
                    };
                    refs.push((node_id, kind, cur.element_id()?));
                    // A placeholder until the document resolves the reference.
                    Element::Scalar(Scalar::Null)
                }
            };
            let anchor = cur.anchor()?;
            let node = Node {
                id: node_id,
                value,
                parent: anchor.parent,
                side: anchor.side,
                moved_away: false,
            };
            if list.nodes.insert(seq_key(&node_id), node).is_some() {
                return Err(DecodeError::BadTag {
                    what: "list: duplicate node id",
                    tag: 0,
                });
            }
        }

        // Read every run record and check its declared length before installing
        // anything, so a malformed stream is rejected on the record rather than
        // part-way through building the sequence.
        let run_count = cur.u32()?;
        let mut runs = Vec::new();
        for _ in 0..run_count {
            let start = cur.stamp()?;
            let length = cur.u32()?;
            let anchor = cur.anchor()?;
            // The encoder splits past the per-record cap and never emits an
            // empty run, so a length outside `1..=MAX_TOMBSTONE_RUN` is
            // malformed.
            if length == 0 || length > MAX_TOMBSTONE_RUN {
                return Err(DecodeError::BadTag {
                    what: "list: tombstone run length",
                    tag: 0,
                });
            }
            runs.push((start, length as u64, anchor));
        }
        for (start, length, anchor) in runs {
            let last = start
                .lamport
                .checked_add(length - 1)
                .ok_or(DecodeError::BadTag {
                    what: "list: tombstone run overflows lamport",
                    tag: 0,
                })?;
            if list.holds_any(start, last) {
                return Err(DecodeError::BadTag {
                    what: "list: duplicate node id",
                    tag: 0,
                });
            }
            // The run holds every id from its head to `last`, but only the head is a
            // stamp on the wire — so the tail is reported explicitly, or a snapshot
            // could under-declare its id-space record by the length of its own
            // tombstones. Deleting a planted run is what makes that the mainline
            // path rather than a corner.
            cur.note_stamp_reach(start.client, last);
            list.add_dead(start, length, anchor.parent, anchor.side);
        }

        Ok((list, refs))
    }

    /// Set the value of an already-decoded node, wiring a composite reference to
    /// its resolved handle in the document's second decode pass.
    pub(crate) fn resolve_node(&mut self, id: Stamp, value: Element) {
        if let Some(node) = self.nodes.get_mut(&seq_key(&id)) {
            node.value = value;
        }
    }

    /// Serialize this list's state to self-contained bytes.
    pub fn encode_state(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_state_into(&mut out);
        out
    }

    /// Read a list from a complete byte slice, rejecting trailing bytes. A bare
    /// list holds only scalars, so a composite reference is rejected.
    pub fn decode_state(bytes: &[u8]) -> Result<List, DecodeError> {
        let mut cur = Cursor::new(bytes);
        let (list, refs) = List::decode_state_from(&mut cur)?;
        if !refs.is_empty() {
            return Err(DecodeError::BadTag {
                what: "bare list: composite node reference",
                tag: 0,
            });
        }
        if cur.at_end() {
            Ok(list)
        } else {
            Err(DecodeError::TrailingBytes)
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.values().filter(|n| !n.hidden()).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many records back the sequence: one per node that carries a value
    /// plus one per deleted run. A contiguous delete costs a single record
    /// however many items it removed, so this tracks the number of runs, not the
    /// number of deletions.
    pub fn stored_records(&self) -> usize {
        self.nodes.len() + self.dead.len()
    }

    /// The live item at `index`, if any.
    pub fn get(&self, index: usize) -> Option<Element> {
        self.live_order()
            .get(index)
            .map(|s| self.nodes[&seq_key(s)].value.clone())
    }

    /// The live items in sequence order.
    pub fn values(&self) -> Vec<Element> {
        self.live_order()
            .iter()
            .map(|s| self.nodes[&seq_key(s)].value.clone())
            .collect()
    }

    /// The live values, borrowed and in no particular order — for membership or
    /// validation passes (codepoint checking on decode) that need every live
    /// value but not sequence order, skipping both `values`' clone and the tree
    /// traversal `live_order` would do.
    pub(crate) fn live_values(&self) -> impl Iterator<Item = &Element> {
        self.nodes
            .values()
            .filter(|n| !n.hidden())
            .map(|n| &n.value)
    }

    /// Every composite node as `(stamp, value)` — what the document
    /// reconstructs a never-moved node's birth placement from after decode, when
    /// `moved_away` is not yet set so a moved node's suppressed birth placement
    /// is still visible here too. A deleted node is gone: its value went with it.
    pub(crate) fn composite_nodes(&self) -> impl Iterator<Item = (Stamp, &Element)> {
        self.nodes
            .values()
            .filter(|n| n.value.is_container())
            .map(|n| (n.id, &n.value))
    }

    /// Insert `value` at live `index`, identified by `stamp`. A stamp already
    /// seen is a replay and leaves the sequence untouched.
    pub fn insert(&mut self, index: usize, value: Element, stamp: Stamp) {
        if self.contains(stamp) {
            return;
        }
        let anchor = self.place(index);
        self.insert_at(stamp, value, anchor);
    }

    /// The Fugue placement for inserting at live `index`, computed without
    /// mutating. Feed it to [`insert_at`](Self::insert_at) to reproduce the
    /// insert on any replica.
    pub fn place(&self, index: usize) -> Anchor {
        self.place_excluding(index, None)
    }

    /// The Fugue placement for live `index`, counting one existing node (if given)
    /// as absent. A same-parent tree move re-places a node that is still live in
    /// this list, so its own slot must be discounted or a forward reorder lands
    /// one position too early.
    pub fn place_excluding(&self, index: usize, exclude: Option<Stamp>) -> Anchor {
        let order = self.tree_order();
        let (left, right) = self.gap_excluding(&order, index, exclude);
        let (parent, side) = self.placement(left, right);
        Anchor { parent, side }
    }

    /// Insert a node with an explicit id and placement. Idempotent on the id:
    /// a replayed op leaves the sequence untouched, and an id already deleted
    /// stays deleted.
    pub fn insert_at(&mut self, id: Stamp, value: Element, anchor: Anchor) {
        if self.contains(id) {
            return;
        }
        self.nodes.insert(
            seq_key(&id),
            Node {
                id,
                value,
                parent: anchor.parent,
                side: anchor.side,
                moved_away: false,
            },
        );
    }

    /// Suppress or re-instate a node's placement under a tree move. Idempotent and
    /// reversible — the document recomputes it from the move-log fold; positioning
    /// is untouched. Inert on a deleted id: a delete is terminal, and the document
    /// suppresses every placement of a deleted node anyway.
    pub(crate) fn set_moved_away(&mut self, id: Stamp, away: bool) {
        if let Some(node) = self.nodes.get_mut(&seq_key(&id)) {
            node.moved_away = away;
        }
    }

    /// The id of the live node at `index`, if any.
    pub fn node_at(&self, index: usize) -> Option<Stamp> {
        self.live_order().get(index).copied()
    }

    /// The ids of up to `count` live items starting at `index`, in one pass over
    /// the live order — deleting a range is linear, not one full traversal per
    /// item.
    pub fn node_ids(&self, index: usize, count: usize) -> Vec<Stamp> {
        self.live_order()
            .into_iter()
            .skip(index)
            .take(count)
            .collect()
    }

    /// The value node `id` holds, whether it renders here or is suppressed by a
    /// tree move. A deleted id holds nothing — a tombstone keeps only a position.
    pub(crate) fn node_value(&self, id: Stamp) -> Option<Element> {
        self.nodes.get(&seq_key(&id)).map(|n| n.value.clone())
    }

    /// The live position of node `id`, if it is present and rendered — neither
    /// deleted nor moved under another parent.
    pub fn live_index(&self, id: Stamp) -> Option<usize> {
        self.live_order().iter().position(|s| *s == id)
    }

    /// Capture a stable position at `index` (clamped to the sequence length, so a
    /// stale index is accepted) with the given gravity. `Left`
    /// binds to the right edge of the item before the gap (the start of the
    /// sequence at index 0); `Right` binds to the left edge of the item at the
    /// gap (the end of the sequence at `len`). The binding is by item id, so the
    /// position survives concurrent edits.
    pub fn relative_position(&self, index: usize, side: Side) -> RelativePosition {
        // A stale index past the end pins to the end boundary the same way on
        // both sides, so an out-of-bounds caller never lands at the wrong edge.
        let index = index.min(self.len());
        match side {
            Side::Left => match index.checked_sub(1).and_then(|i| self.node_at(i)) {
                Some(id) => RelativePosition::After(id),
                None => RelativePosition::Start,
            },
            Side::Right => match self.node_at(index) {
                Some(id) => RelativePosition::Before(id),
                None => RelativePosition::End,
            },
        }
    }

    /// The current live index of a captured [`RelativePosition`]. A live binding
    /// resolves to its item's edge; a deleted one walks the retained ids to the
    /// nearest live neighbour on the gravity side; the boundaries resolve to `0`
    /// and `len`.
    pub fn resolve_position(&self, pos: &RelativePosition) -> usize {
        match pos {
            RelativePosition::Start => 0,
            RelativePosition::End => self.len(),
            RelativePosition::Before(id) => self.resolve_before(*id),
            RelativePosition::After(id) => self.resolve_after(*id),
        }
    }

    /// Like [`resolve_position`](Self::resolve_position), but yields `None` when
    /// the position is bound to an item absent from the sequence — an anchor
    /// whose referent has not arrived yet — rather than clamping to a boundary.
    /// `Start`/`End` always resolve. A consumer that must distinguish "the anchor
    /// resolves here" from "the anchor cannot resolve yet" (mark coverage) uses
    /// this; a cursor that wants a best-effort index uses `resolve_position`.
    pub fn resolve_position_present(&self, pos: &RelativePosition) -> Option<usize> {
        match pos {
            RelativePosition::Start => Some(0),
            RelativePosition::End => Some(self.len()),
            RelativePosition::Before(id) => self.live_rank(*id).map(|(before, _)| before),
            RelativePosition::After(id) => self
                .live_rank(*id)
                .map(|(before, live)| before + usize::from(live)),
        }
    }

    /// The number of live items strictly before `id` in sequence order, and
    /// whether `id` itself is live — or `None` if `id` is not in the sequence.
    /// One traversal of the order, so resolving a position costs a single walk.
    fn live_rank(&self, id: Stamp) -> Option<(usize, bool)> {
        let mut before = 0;
        for span in self.tree_order() {
            match span {
                Span::Node(s) => {
                    let hidden = self.nodes[&seq_key(&s)].hidden();
                    if s == id {
                        return Some((before, !hidden));
                    }
                    if !hidden {
                        before += 1;
                    }
                }
                Span::Dead { head, len } => {
                    if covers(head, len, id) {
                        return Some((before, false));
                    }
                }
            }
        }
        None
    }

    /// The left edge of `id`: its live index, or — if it is deleted — the index
    /// of the nearest live item to its right, clamping to `len` past the end.
    /// Both equal the count of live items before `id`.
    fn resolve_before(&self, id: Stamp) -> usize {
        match self.live_rank(id) {
            Some((before, _)) => before,
            None => self.len(),
        }
    }

    /// The right edge of `id`: one past its live index, or — if it is deleted —
    /// one past the nearest live item to its left, clamping to `0` past the start.
    fn resolve_after(&self, id: Stamp) -> usize {
        match self.live_rank(id) {
            Some((before, live)) => before + usize::from(live),
            None => 0,
        }
    }

    /// Delete the live item at `index`.
    pub fn delete(&mut self, index: usize) {
        if let Some(id) = self.node_at(index) {
            self.delete_id(id);
        }
    }

    /// Whether the node `id` is present (live or deleted).
    pub fn contains(&self, id: Stamp) -> bool {
        self.nodes.contains_key(&seq_key(&id)) || self.dead_run(id).is_some()
    }

    /// Whether the node `id` is present and deleted.
    pub(crate) fn is_tombstoned(&self, id: Stamp) -> bool {
        self.dead_run(id).is_some()
    }

    /// Delete the node with `id`, folding it into the run it continues. Its value
    /// is dropped — a tombstone only holds a position. Idempotent: a no-op if
    /// absent or already deleted.
    pub fn delete_id(&mut self, id: Stamp) {
        let Some(node) = self.nodes.remove(&seq_key(&id)) else {
            return;
        };
        self.add_dead(id, 1, node.parent, node.side);
    }

    /// Fold another replica's sequence in. Delete wins, and it is terminal: an id
    /// deleted here stays deleted and keeps no value, so the peer's copy of a
    /// deleted node is dropped rather than folded — a snapshot round-trip already
    /// leaves exactly that, and a composite's content lives in the document's
    /// per-id registry, not in the node.
    pub fn merge(&mut self, other: &Self) {
        for (key, on) in &other.nodes {
            if self.dead_run(on.id).is_some() {
                continue;
            }
            match self.nodes.get_mut(key) {
                // Same logical item: fold composite values together; scalars are
                // immutable so their shared id already agrees.
                Some(sn) => {
                    if sn.value.kind() != ElementKind::Scalar && sn.value.kind() == on.value.kind()
                    {
                        sn.value.merge(&on.value);
                    }
                }
                None => {
                    self.nodes.insert(*key, on.deep_clone());
                }
            }
        }
        for (key, run) in &other.dead {
            self.bury(seq_stamp(key), run.len, run.parent, run.side);
        }
    }

    pub fn deep_clone(&self) -> Self {
        Self {
            id: self.id,
            nodes: self
                .nodes
                .iter()
                .map(|(k, n)| (*k, n.deep_clone()))
                .collect(),
            dead: self.dead.clone(),
            displaced: Cell::new(false),
        }
    }

    /// Drop every node. Used at document teardown to break the child links a
    /// sequence holds — a composite node references its child, and a tree move can
    /// place a node's own subtree back into it, so a list can close an `Rc` cycle
    /// that clearing the maps alone would not free.
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.dead.clear();
    }

    pub fn displace(&self) {
        self.displaced.set(true);
    }

    /// Re-install a previously displaced list: it has re-won its slot as the
    /// same logical element, retaining its content.
    pub fn reinstate(&self) {
        self.displaced.set(false);
    }

    pub fn is_displaced(&self) -> bool {
        self.displaced.get()
    }

    // --- deleted runs ---

    /// The run covering `id`, as its head and record.
    fn dead_run(&self, id: Stamp) -> Option<(Stamp, &DeadRun)> {
        let (key, run) = self.dead.range(..=seq_key(&id)).next_back()?;
        let head = seq_stamp(key);
        covers(head, run.len, id).then_some((head, run))
    }

    /// Whether any id from `start` through lamport `last` (in `start`'s client
    /// and offset group) is already stored.
    fn holds_any(&self, start: Stamp, last: u64) -> bool {
        if self.dead_run(start).is_some() {
            return true;
        }
        let from = seq_key(&start);
        let to = (start.client, start.offset, last);
        self.nodes.range(from..=to).next().is_some() || self.dead.range(from..=to).next().is_some()
    }

    /// Record `len` ids from `head` as deleted, welding the record onto an
    /// adjacent run whose chain it continues — so a contiguous delete stays one
    /// record however its pieces arrive.
    fn add_dead(&mut self, head: Stamp, len: u64, parent: Option<Stamp>, side: Side) {
        let mut head = head;
        let mut len = len;
        let mut parent = parent;
        let mut side = side;

        if let Some(prev) = head
            .lamport
            .checked_sub(1)
            .map(|lamport| Stamp { lamport, ..head })
        {
            if parent == Some(prev) && side == Side::Right {
                if let Some((phead, prun)) = self.dead_run(prev) {
                    let (plen, pparent, pside) = (prun.len, prun.parent, prun.side);
                    if run_id(phead, plen - 1) == prev {
                        self.dead.remove(&seq_key(&phead));
                        head = phead;
                        len += plen;
                        parent = pparent;
                        side = pside;
                    }
                }
            }
        }

        let last = run_id(head, len - 1);
        if let Some(next) = last
            .lamport
            .checked_add(1)
            .map(|lamport| Stamp { lamport, ..last })
        {
            let follows = self.dead.get(&seq_key(&next)).and_then(|nrun| {
                (nrun.parent == Some(last) && nrun.side == Side::Right).then_some(nrun.len)
            });
            if let Some(next_len) = follows {
                len += next_len;
                self.dead.remove(&seq_key(&next));
            }
        }

        self.dead
            .insert(seq_key(&head), DeadRun { len, parent, side });
    }

    /// Mark another replica's run deleted here: an id this list still holds keeps
    /// the placement it recorded, and a stretch it has never seen is adopted from
    /// the run's chain in one record — so absorbing an unseen run is O(1).
    fn bury(&mut self, head: Stamp, len: u64, parent: Option<Stamp>, side: Side) {
        let mut i = 0u64;
        while i < len {
            let id = run_id(head, i);
            if let Some((h, run)) = self.dead_run(id) {
                i += run.len - (id.lamport - h.lamport);
                continue;
            }
            if let Some(node) = self.nodes.remove(&seq_key(&id)) {
                self.add_dead(id, 1, node.parent, node.side);
                i += 1;
                continue;
            }
            let unseen = self.unseen_from(id, len - i);
            let (p, s) = if i == 0 {
                (parent, side)
            } else {
                (Some(run_id(head, i - 1)), Side::Right)
            };
            self.add_dead(id, unseen, p, s);
            i += unseen;
        }
    }

    /// How many ids from `id` onward (at most `max`) this list has never seen —
    /// the stretch an incoming run can be absorbed into as a single record.
    fn unseen_from(&self, id: Stamp, max: u64) -> u64 {
        let from = seq_key(&id);
        let group = (id.client, id.offset);
        let distance = |key: &SeqKey| ((key.0, key.1) == group).then(|| key.2 - id.lamport);
        let next_node = self
            .nodes
            .range(from..)
            .next()
            .and_then(|(k, _)| distance(k));
        let next_dead = self
            .dead
            .range(from..)
            .next()
            .and_then(|(k, _)| distance(k));
        [next_node, next_dead]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(max)
            .min(max)
    }

    // --- Fugue internals ---

    /// Records grouped by `(parent, side)`, each bucket sorted by stamp. A run
    /// whose interior an outside record attaches to is cut there, so every
    /// attachment hangs off the head or the last id of some piece and the walk
    /// never has to step through a run.
    fn children(&self) -> HashMap<(Option<Stamp>, Side), Vec<Span>> {
        let mut cuts: HashMap<SeqKey, BTreeSet<u64>> = HashMap::new();
        let anchors = self
            .nodes
            .values()
            .map(|n| (n.parent, n.side))
            .chain(self.dead.values().map(|r| (r.parent, r.side)));
        for (parent, side) in anchors {
            let Some(p) = parent else { continue };
            let Some((head, run)) = self.dead_run(p) else {
                continue;
            };
            let at = p.lamport - head.lamport;
            // A left attachment renders before `p`, so `p` must begin a piece; a
            // right attachment shares `p`'s bucket with the id that chains off
            // it, so `p` must end one.
            let cut = match side {
                Side::Left => at,
                Side::Right => at + 1,
            };
            if cut > 0 && cut < run.len {
                cuts.entry(seq_key(&head)).or_default().insert(cut);
            }
        }

        let mut map: HashMap<(Option<Stamp>, Side), Vec<Span>> = HashMap::new();
        for n in self.nodes.values() {
            map.entry((n.parent, n.side))
                .or_default()
                .push(Span::Node(n.id));
        }
        let uncut = BTreeSet::new();
        for (key, run) in &self.dead {
            let head = seq_stamp(key);
            let mut start = 0u64;
            let mut anchor = (run.parent, run.side);
            let ends = cuts.get(key).unwrap_or(&uncut);
            for &end in ends.iter().chain(std::iter::once(&run.len)) {
                map.entry(anchor).or_default().push(Span::Dead {
                    head: run_id(head, start),
                    len: end - start,
                });
                anchor = (Some(run_id(head, end - 1)), Side::Right);
                start = end;
            }
        }
        for bucket in map.values_mut() {
            bucket.sort_by_key(|span| span.head());
        }
        map
    }

    /// Every record in sequence order (deleted runs included).
    fn tree_order(&self) -> Vec<Span> {
        let children = self.children();
        let bucket = |p: Option<Stamp>, side: Side| -> Vec<Span> {
            children.get(&(p, side)).cloned().unwrap_or_default()
        };

        enum Step {
            Emit(Span),
            Expand(Option<Span>),
        }
        let mut out = Vec::with_capacity(self.stored_records());
        let mut stack = vec![Step::Expand(None)];
        while let Some(step) = stack.pop() {
            match step {
                Step::Emit(s) => out.push(s),
                Step::Expand(p) => {
                    // Reverse push so execution is: left children, self, right children.
                    for r in bucket(p.map(Span::last), Side::Right).into_iter().rev() {
                        stack.push(Step::Expand(Some(r)));
                    }
                    if let Some(s) = p {
                        stack.push(Step::Emit(s));
                    }
                    for l in bucket(p.map(Span::head), Side::Left).into_iter().rev() {
                        stack.push(Step::Expand(Some(l)));
                    }
                }
            }
        }
        out
    }

    /// Live nodes in sequence order — deleted runs and moved-away nodes skipped.
    fn live_order(&self) -> Vec<Stamp> {
        self.tree_order()
            .into_iter()
            .filter_map(|span| match span {
                Span::Node(id) if !self.nodes[&seq_key(&id)].hidden() => Some(id),
                _ => None,
            })
            .collect()
    }

    /// The ids bracketing the gap before live position `index`, counting
    /// `exclude` (if present) as not live — so a node being re-placed within its
    /// own list does not shift the target index. The gap always falls on a record
    /// boundary: only a live node advances the count, so the boundary lands right
    /// after one, never inside a run.
    fn gap_excluding(
        &self,
        order: &[Span],
        index: usize,
        exclude: Option<Stamp>,
    ) -> (Option<Stamp>, Option<Stamp>) {
        let mut live = 0;
        let mut boundary = order.len();
        for (k, span) in order.iter().enumerate() {
            if live == index {
                boundary = k;
                break;
            }
            if let Span::Node(id) = span {
                if !self.nodes[&seq_key(id)].hidden() && Some(*id) != exclude {
                    live += 1;
                }
            }
        }
        let left = (boundary > 0).then(|| order[boundary - 1].last());
        let right = order.get(boundary).map(|span| span.head());
        (left, right)
    }

    /// Attach after the left origin when it has no right subtree yet, else as
    /// the left child of the right origin — the rule that keeps concurrent runs
    /// from interleaving.
    fn placement(&self, left: Option<Stamp>, right: Option<Stamp>) -> (Option<Stamp>, Side) {
        match (left, right) {
            (Some(l), _) if !self.has_right_child(l) => (Some(l), Side::Right),
            (_, Some(r)) => (Some(r), Side::Left),
            (Some(l), None) => (Some(l), Side::Right),
            (None, None) => (None, Side::Right),
        }
    }

    fn has_right_child(&self, parent: Stamp) -> bool {
        if let Some((head, run)) = self.dead_run(parent) {
            // Every id in a run but the last chains to its successor.
            if parent != run_id(head, run.len - 1) {
                return true;
            }
        }
        self.nodes
            .values()
            .any(|n| n.parent == Some(parent) && n.side == Side::Right)
            || self
                .dead
                .values()
                .any(|run| run.parent == Some(parent) && run.side == Side::Right)
    }
}

/// Whether a run of `len` ids from `head` contains `id`.
fn covers(head: Stamp, len: u64, id: Stamp) -> bool {
    head.client == id.client
        && head.offset == id.offset
        && id.lamport >= head.lamport
        && id.lamport - head.lamport < len
}
