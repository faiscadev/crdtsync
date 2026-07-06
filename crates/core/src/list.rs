//! List — an ordered sequence CRDT (Fugue).
//!
//! Items live in a tree: each insert attaches to a neighbour and the sequence
//! is the tree's in-order traversal. A run typed left-to-right forms a spine,
//! so two concurrent runs at the same gap stay whole and ordered by their
//! first item's stamp instead of interleaving. Deletes tombstone — a position
//! must survive to anchor inserts placed against it. The same algorithm backs
//! Text.

use crate::anchor::RelativePosition;
use crate::codec::{len_u32, put_anchor, put_stamp, put_u32, put_u8, Cursor, DecodeError};
use crate::element::Element;
use crate::elementid::{ElementId, ElementKind};
use crate::scalar::Scalar;
use crate::stamp::Stamp;
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};

/// The most tombstones one encoded run record may reconstruct. A run longer
/// than this is split into chained records on encode, so this only rejects a
/// crafted record — bounding the decompression a single record can drive.
const MAX_TOMBSTONE_RUN: u32 = 1 << 20;

/// The most tombstones a whole decoded sequence may reconstruct across every
/// run. The per-record cap alone bounds one record but not their sum, so a
/// small stream of many records could still claim a huge node count on untrusted
/// input; this ceiling bounds total decode memory (a few hundred MB of nodes at
/// the limit). Run-length compression is inherently high-ratio, so a bytes-based
/// ratio cannot separate a bomb from a legitimately dense snapshot — an absolute
/// node ceiling is the meaningful guard. A document compacts far below this.
const MAX_TOMBSTONE_TOTAL: u64 = 1 << 22;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Side {
    Left,
    Right,
}

/// A decoded live node whose value is a composite reference — its Fugue stamp
/// plus the kind + id to resolve against the document's registries. Scalar nodes
/// are inlined and need no second pass; only these are returned for resolution.
pub(crate) type NodeRef = (Stamp, ElementKind, ElementId);

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
    tombstone: bool,
    /// Suppressed by a tree move: the node still anchors the Fugue tree but a live
    /// read skips it, because the element it holds now renders under a different
    /// parent. Unlike a tombstone this is reversible — the document sets it from
    /// the move-log fold, so an undo-and-replay can re-instate the placement.
    moved_away: bool,
}

impl Node {
    /// Whether a live read skips this node — deleted, or moved under another
    /// parent. Fugue positioning still keeps it (it anchors later inserts).
    fn hidden(&self) -> bool {
        self.tombstone || self.moved_away
    }

    fn deep_clone(&self) -> Self {
        Self {
            id: self.id,
            value: self.value.deep_clone(),
            parent: self.parent,
            side: self.side,
            tombstone: self.tombstone,
            moved_away: self.moved_away,
        }
    }
}

pub struct List {
    id: ElementId,
    nodes: HashMap<Stamp, Node>,
    displaced: Cell<bool>,
}

impl List {
    pub fn new(id: ElementId) -> Self {
        Self {
            id,
            nodes: HashMap::new(),
            displaced: Cell::new(false),
        }
    }

    pub fn id(&self) -> ElementId {
        self.id
    }

    /// Append this list's state to `out` in two sections: the live nodes in
    /// full, then the tombstones run-length compressed.
    ///
    /// A tombstone must survive to anchor later inserts, but its value is never
    /// read again and deleted content is contiguous — a run inserted together
    /// takes consecutive stamps chained parent-to-child. So a maximal run of
    /// tombstones with consecutive stamps forming that chain collapses to one
    /// range record (start, length, the run head's anchor); its dead values are
    /// dropped. A deleted region costs O(runs), not O(deleted items). The live
    /// section is stamp-ordered and the tombstone runs are emitted in run-head
    /// stamp order, each split run's chunks in ascending offset — a
    /// deterministic function of the node set (not a global stamp sort across
    /// runs), so equal states encode identically.
    pub(crate) fn encode_state_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.id.as_bytes());

        let mut live: Vec<&Node> = self.nodes.values().filter(|n| !n.tombstone).collect();
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

        // Tombstones sorted by stamp; a run extends while the next stamp
        // (same client, +1 lamport) is a tombstone chained to its predecessor.
        let dead: BTreeMap<Stamp, &Node> = self
            .nodes
            .values()
            .filter(|n| n.tombstone)
            .map(|n| (n.id, n))
            .collect();
        let mut runs: Vec<(Stamp, u64, Anchor)> = Vec::new();
        for (&start, node) in &dead {
            // Start a run only at its head — a tombstone whose predecessor is
            // not a tombstone chained into it. Every other tombstone is reached
            // by walking forward from some head, so each is emitted exactly once
            // without a separate visited set.
            if let Some(prev_lamport) = start.lamport.checked_sub(1) {
                let prev = Stamp {
                    lamport: prev_lamport,
                    client: start.client,
                };
                if dead.contains_key(&prev) && node.parent == Some(prev) && node.side == Side::Right
                {
                    continue;
                }
            }
            let mut len = 1u64;
            let mut cur_id = start;
            while let Some(next_id) = cur_id.lamport.checked_add(1).map(|lamport| Stamp {
                lamport,
                client: cur_id.client,
            }) {
                match dead.get(&next_id) {
                    Some(n) if n.parent == Some(cur_id) && n.side == Side::Right => {
                        len += 1;
                        cur_id = next_id;
                    }
                    _ => break,
                }
            }
            runs.push((
                start,
                len,
                Anchor {
                    parent: node.parent,
                    side: node.side,
                },
            ));
        }

        // Split any run past the cap into chained chunks so the decoder's bound
        // never rejects state this encoder produced.
        let mut chunk_count = 0u64;
        let mut chunks: Vec<u8> = Vec::new();
        for (start, len, anchor) in &runs {
            let mut off = 0u64;
            while off < *len {
                let chunk_len = (*len - off).min(MAX_TOMBSTONE_RUN as u64);
                // Every derived lamport equals a materialised node's, so it fits
                // u64; checked arithmetic keeps encode symmetric with the
                // decoder's `checked_add` rather than wrapping in release.
                let lamport = start
                    .lamport
                    .checked_add(off)
                    .expect("run chunk stamp within a materialised node's lamport");
                let chunk_start = Stamp {
                    lamport,
                    client: start.client,
                };
                let chunk_anchor = if off == 0 {
                    *anchor
                } else {
                    Anchor {
                        parent: Some(Stamp {
                            lamport: lamport - 1,
                            client: start.client,
                        }),
                        side: Side::Right,
                    }
                };
                put_stamp(&mut chunks, &chunk_start);
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
    /// live section in full, then the tombstone runs expanded back to nodes.
    /// Composite live nodes come back holding a placeholder value with their
    /// reference returned alongside, for the document to resolve against its
    /// registries in a second pass (as map slots resolve).
    pub(crate) fn decode_state_from(cur: &mut Cursor) -> Result<(List, Vec<NodeRef>), DecodeError> {
        let id = cur.element_id()?;
        // Grow the map as records are read rather than trusting a count to size
        // the reservation, so a bogus length fails on the missing bytes.
        let mut nodes: HashMap<Stamp, Node> = HashMap::new();
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
                tombstone: false,
                moved_away: false,
            };
            if nodes.insert(node_id, node).is_some() {
                return Err(DecodeError::BadTag {
                    what: "list: duplicate node id",
                    tag: 0,
                });
            }
        }

        // Read every run record and validate its declared size before
        // reconstructing any node, so a crafted stream is rejected on its
        // declared lengths rather than by materialising the bomb it describes.
        // Each record consumes real bytes, so the record count is itself bounded
        // by the input length.
        let run_count = cur.u32()?;
        let mut runs = Vec::new();
        let mut total_tombstones: u64 = 0;
        for _ in 0..run_count {
            let start = cur.stamp()?;
            let length = cur.u32()?;
            let anchor = cur.anchor()?;
            // A run reconstructs `length` nodes from a fixed record, so an
            // unbounded length is a decompression bomb; the encoder splits past
            // the per-record cap and never emits an empty run, so a length
            // outside `1..=MAX_TOMBSTONE_RUN` is malformed.
            if length == 0 || length > MAX_TOMBSTONE_RUN {
                return Err(DecodeError::BadTag {
                    what: "list: tombstone run length",
                    tag: 0,
                });
            }
            total_tombstones += length as u64;
            if total_tombstones > MAX_TOMBSTONE_TOTAL {
                return Err(DecodeError::BadTag {
                    what: "list: tombstone total exceeds decode budget",
                    tag: 0,
                });
            }
            runs.push((start, length, anchor));
        }
        for (start, length, anchor) in runs {
            let mut parent = anchor.parent;
            let mut side = anchor.side;
            for i in 0..length {
                let lamport = start
                    .lamport
                    .checked_add(i as u64)
                    .ok_or(DecodeError::BadTag {
                        what: "list: tombstone run overflows lamport",
                        tag: 0,
                    })?;
                let node_id = Stamp {
                    lamport,
                    client: start.client,
                };
                // A tombstone's value is never read; a placeholder stands in.
                let node = Node {
                    id: node_id,
                    value: Element::Scalar(Scalar::Null),
                    parent,
                    side,
                    tombstone: true,
                    moved_away: false,
                };
                if nodes.insert(node_id, node).is_some() {
                    return Err(DecodeError::BadTag {
                        what: "list: duplicate node id",
                        tag: 0,
                    });
                }
                parent = Some(node_id);
                side = Side::Right;
            }
        }

        Ok((
            List {
                id,
                nodes,
                displaced: Cell::new(false),
            },
            refs,
        ))
    }

    /// Set the value of an already-decoded node, wiring a composite reference to
    /// its resolved handle in the document's second decode pass.
    pub(crate) fn resolve_node(&mut self, id: Stamp, value: Element) {
        if let Some(node) = self.nodes.get_mut(&id) {
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

    /// The live item at `index`, if any.
    pub fn get(&self, index: usize) -> Option<Element> {
        self.live_order()
            .get(index)
            .map(|s| self.nodes[s].value.clone())
    }

    /// The live items in sequence order.
    pub fn values(&self) -> Vec<Element> {
        self.live_order()
            .iter()
            .map(|s| self.nodes[s].value.clone())
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

    /// Insert `value` at live `index`, identified by `stamp`. A stamp already
    /// seen is a replay and leaves the node untouched.
    pub fn insert(&mut self, index: usize, value: Element, stamp: Stamp) {
        if self.nodes.contains_key(&stamp) {
            return;
        }
        let anchor = self.place(index);
        self.insert_at(stamp, value, anchor);
    }

    /// The Fugue placement for inserting at live `index`, computed without
    /// mutating. Feed it to [`insert_at`](Self::insert_at) to reproduce the
    /// insert on any replica.
    pub fn place(&self, index: usize) -> Anchor {
        let order = self.tree_order();
        let (left, right) = self.gap(&order, index);
        let (parent, side) = self.placement(left, right);
        Anchor { parent, side }
    }

    /// Insert a node with an explicit id and placement. Idempotent on the id:
    /// a replayed op leaves the existing node untouched.
    pub fn insert_at(&mut self, id: Stamp, value: Element, anchor: Anchor) {
        if self.nodes.contains_key(&id) {
            return;
        }
        self.nodes.insert(
            id,
            Node {
                id,
                value,
                parent: anchor.parent,
                side: anchor.side,
                tombstone: false,
                moved_away: false,
            },
        );
    }

    /// Suppress or re-instate a node's placement under a tree move. Idempotent and
    /// reversible — the document recomputes it from the move-log fold; positioning
    /// is untouched.
    pub(crate) fn set_moved_away(&mut self, id: Stamp, away: bool) {
        if let Some(node) = self.nodes.get_mut(&id) {
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

    /// The live position of node `id`, if it is present and not tombstoned.
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
    /// resolves to its item's edge; a deleted one walks the retained tombstones
    /// to the nearest live neighbour on the gravity side; the boundaries resolve
    /// to `0` and `len`.
    pub fn resolve_position(&self, pos: &RelativePosition) -> usize {
        match pos {
            RelativePosition::Start => 0,
            RelativePosition::End => self.len(),
            RelativePosition::Before(id) => self.resolve_before(*id),
            RelativePosition::After(id) => self.resolve_after(*id),
        }
    }

    /// The number of live items strictly before `id` in sequence order, and
    /// whether `id` itself is live — or `None` if `id` is not in the sequence.
    /// One traversal of the order (no repeated `live_index` scans, which made the
    /// earlier resolution quadratic in traversals).
    fn live_rank(&self, id: Stamp) -> Option<(usize, bool)> {
        let mut before = 0;
        for s in self.tree_order() {
            if s == id {
                return Some((before, !self.nodes[&s].tombstone));
            }
            if !self.nodes[&s].tombstone {
                before += 1;
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

    /// Tombstone the live item at `index`.
    pub fn delete(&mut self, index: usize) {
        if let Some(id) = self.node_at(index) {
            self.delete_id(id);
        }
    }

    /// Whether the node `id` is present (live or tombstoned).
    pub fn contains(&self, id: Stamp) -> bool {
        self.nodes.contains_key(&id)
    }

    /// Tombstone the node with `id`. Idempotent: a no-op if absent or already
    /// tombstoned.
    pub fn delete_id(&mut self, id: Stamp) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.tombstone = true;
        }
    }

    pub fn merge(&mut self, other: &Self) {
        for (id, on) in &other.nodes {
            match self.nodes.get_mut(id) {
                Some(sn) => {
                    // Deletion is monotonic, so a tombstone anywhere wins.
                    sn.tombstone |= on.tombstone;
                    // Same logical item: fold composite values together; scalars
                    // are immutable so their shared id already agrees.
                    if sn.value.kind() != ElementKind::Scalar && sn.value.kind() == on.value.kind()
                    {
                        sn.value.merge(&on.value);
                    }
                }
                None => {
                    self.nodes.insert(*id, on.deep_clone());
                }
            }
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
            displaced: Cell::new(false),
        }
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

    // --- Fugue internals ---

    /// Child stamps grouped by `(parent, side)`, each bucket sorted by stamp.
    fn children(&self) -> HashMap<(Option<Stamp>, Side), Vec<Stamp>> {
        let mut map: HashMap<(Option<Stamp>, Side), Vec<Stamp>> = HashMap::new();
        for n in self.nodes.values() {
            map.entry((n.parent, n.side)).or_default().push(n.id);
        }
        for bucket in map.values_mut() {
            bucket.sort();
        }
        map
    }

    /// Every node in sequence order (tombstones included).
    fn tree_order(&self) -> Vec<Stamp> {
        let children = self.children();
        let bucket = |p: Option<Stamp>, side: Side| -> Vec<Stamp> {
            children.get(&(p, side)).cloned().unwrap_or_default()
        };

        enum Step {
            Emit(Stamp),
            Expand(Option<Stamp>),
        }
        let mut out = Vec::with_capacity(self.nodes.len());
        let mut stack = vec![Step::Expand(None)];
        while let Some(step) = stack.pop() {
            match step {
                Step::Emit(s) => out.push(s),
                Step::Expand(p) => {
                    // Reverse push so execution is: left children, self, right children.
                    for r in bucket(p, Side::Right).into_iter().rev() {
                        stack.push(Step::Expand(Some(r)));
                    }
                    if let Some(s) = p {
                        stack.push(Step::Emit(s));
                    }
                    for l in bucket(p, Side::Left).into_iter().rev() {
                        stack.push(Step::Expand(Some(l)));
                    }
                }
            }
        }
        out
    }

    /// Live nodes in sequence order — tombstoned and moved-away nodes skipped.
    fn live_order(&self) -> Vec<Stamp> {
        self.tree_order()
            .into_iter()
            .filter(|s| !self.nodes[s].hidden())
            .collect()
    }

    /// The nodes bracketing the gap before live position `index`.
    fn gap(&self, order: &[Stamp], index: usize) -> (Option<Stamp>, Option<Stamp>) {
        let mut live = 0;
        let mut boundary = order.len();
        for (k, s) in order.iter().enumerate() {
            if live == index {
                boundary = k;
                break;
            }
            if !self.nodes[s].hidden() {
                live += 1;
            }
        }
        let left = (boundary > 0).then(|| order[boundary - 1]);
        let right = order.get(boundary).copied();
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
        self.nodes
            .values()
            .any(|n| n.parent == Some(parent) && n.side == Side::Right)
    }
}
