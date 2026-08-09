//! Document — a replica: a tree of containers rooted at a well-known id, a
//! lamport clock, and the transact/apply seam.
//!
//! A `transact` mutates the live tree through a cursor and returns the ops it
//! emitted; `apply` folds a foreign op back in. Ops are keyed by `(client,
//! seq)` for idempotent dedup and ordered by their stamp for LWW. Each op
//! names its target container by id, resolved against a registry of every
//! container the replica has materialised. That registry retains displaced
//! containers, so a slot re-won after displacement is the same logical
//! element: identity persists across displacement — and an op naming a displaced
//! container applies into it hidden, since that is what a reinstatement brings
//! back. An op the current state cannot express — its target's create unseen, the
//! nodes it deletes absent, its transaction group incomplete — is held in the
//! buffer and replays once what it waits on arrives, so out-of-order delivery
//! converges. The buffer is encoded in op-id order, since nothing else about the
//! order it was filled in is state.

use crate::acl::{AclEffect, AclGrant, AclRecord, AclScope, AclSubject, AclTuple};
use crate::anchor::RelativePosition;
use crate::clientid::ClientId;
use crate::codec::{
    decode_ops, encode_op, len_u32, put_acl_effect, put_acl_grant, put_acl_scope, put_acl_subject,
    put_bytes, put_opt_bytes, put_range_anchor, put_scalar, put_stamp, put_u32, put_u64, put_u8,
    Cursor, DecodeError,
};
use crate::counter::Counter;
use crate::element::Element;
use crate::elementid::{ElementId, ElementKind};
use crate::list::{Anchor, List, Side};
use crate::map::{DecodedMap, Map, SlotValue};
use crate::marks::{MarkState, ResolvedMark};
use crate::op::{Op, OpId, OpKind, Tx, TxId, MAX_TX_MEMBERS};
use crate::ranged::{RangeAnchor, RangedElement, RangedInit, RangedPayload};
use crate::repair::{keyed_repairs, Repair, RepairId};
use crate::scalar::Scalar;
use crate::schema::{MarkFlavor, Schema};
use crate::stamp::{Stamp, LAMPORT_STATE_CEILING, LAMPORT_WIRE_CEILING};
use crate::text::Text;
use crate::treemove::TreeMoves;
use crate::undo::{History, Intention, Landing, Snap, Step as Inverse, MAX_SNAPSHOT_DEPTH};
use crate::validate::Step;
use crate::xml::{XmlElement, XmlFragment};
use crate::zone;

/// One placement of a movable XML node: a Fugue node in some children `list`,
/// keyed by its node `stamp`. A node moved N times has N+1 placements (birth plus
/// one per move); the move-log fold marks all but the governing one moved-away.
#[derive(Clone, Copy)]
struct Placement {
    list: ElementId,
    stamp: Stamp,
}
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::rc::Rc;

/// The answer to a node asking for a `(list, stamp)` placement key.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Claim {
    /// Nothing held the key. A claim owns the key, not the sequence id: a plain
    /// `ListInsert` reaches a children list without passing through the placement
    /// index, so the id may already be taken, in which case this answer's insert
    /// does not land and the eviction and join below overwrite what is there.
    Fresh,
    /// The claimant's own node already held it, so nothing changes hands — the
    /// sequence slot takes the meet of the two positions and the placement record
    /// is already there.
    Joined,
    /// A node this one outranks held it and now holds nothing at it, so the
    /// sequence slot is re-seated rather than inserted into.
    Evicted,
    /// A node this one does not outrank holds the key.
    Refused,
}

/// The well-known root slot every replica shares, so children derive under the
/// same parent.
const ROOT_ID: [u8; 16] = *b"crdtsync\0\0\0\0root";

/// The snapshot format version: a reader rejects any stream not stamped with it,
/// so a format change can never be misread as the current one.
const STATE_VERSION: u8 = 14;

/// A composite that a mutation displaced from its slot. Reported wherever the
/// displacement happens, a hidden subtree included: an op addressed to a retained
/// container applies into it, so a slot inside one changes hands like any other.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OrphanEvent {
    pub id: ElementId,
}

/// What a snapshot migration does to one leaf slot, keyed on its slot key — the
/// state-level image of an op's [`OpRewrite`](crate::migration::OpRewrite):
/// `Keep` it, `Drop` it (an added field down, a removed field up), or `Rename`
/// it to a new key (a renamed field).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SlotFate {
    Keep,
    Rename(Vec<u8>),
    Drop,
}

/// A rename mover, held until the second phase re-homes it at the new key. Its
/// slot body and its retained counter tally travel separately: a counter is
/// retained in the registry keyed by the id its key derives whether or not the
/// slot still holds it live — a scalar or register may have displaced it, it may
/// be tombstoned, or a deleted container may occupy the slot — so the tally must
/// re-home independent of, and even in the absence of, a slot-body move.
struct LeafMove {
    to: Vec<u8>,
    /// The counter tally retained at the old key's derived id, captured as an
    /// isolated copy so merging it into the destination cannot leak through to
    /// another mover. `None` when the key never had a counter.
    counter: Option<Counter>,
    /// The slot body to place at the new key. `None` for a container slot carried
    /// verbatim (live, or a deleted one) — only its counter, if any, re-homes.
    slot: Option<SlotMove>,
}

/// A slot body taken out of its old key during a rename.
struct SlotMove {
    stamp: Stamp,
    tombstone: bool,
    body: SlotBody,
}

/// A deleted-container slot the migration resurrects, held until the second
/// phase so a rename onto the old or new key resolves by LWW. The container
/// itself lands live at the old key (mirroring the op seam, which carries the
/// create verbatim there); the delete re-keys — to a fresh tombstone at the new
/// key under a rename, or nowhere under a drop (the delete op dropped).
struct Resurrect {
    old_key: Vec<u8>,
    container: Element,
    /// The create-stamp the live container lands at the old key with.
    create_stamp: Stamp,
    /// The delete's destination: `(new_key, delete_stamp)` under a rename, `None`
    /// under a drop.
    tombstone_at: Option<(Vec<u8>, Stamp)>,
}

/// The value a renamed slot places at its new key.
enum SlotBody {
    /// A scalar, a register (re-keyed to the new id on placement), or a tombstone
    /// (a `None` value).
    Value(Option<Element>),
    /// The slot held a live counter; its placed value is the rehomed registry
    /// counter at the new id, filled in the second phase.
    LiveCounter,
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
    /// Every XML tree node the replica has materialised, keyed by id. An element
    /// owns an attrs Map and a children List (also registered above under their
    /// derived ids); a fragment owns only a children List. Retained across
    /// displacement like every other container, so a re-won slot resumes the same
    /// node with its attrs and children intact.
    xml_elements: HashMap<ElementId, Rc<RefCell<XmlElement>>>,
    xml_fragments: HashMap<ElementId, Rc<RefCell<XmlFragment>>>,
    /// Each container's parent map, for walking reachability up to the root. For
    /// a movable XML node this tracks its *live* placement's list, so reachability
    /// follows a moved subtree to its new parent.
    parents: HashMap<ElementId, ElementId>,
    /// The lamport-ordered tree-move log (Kleppmann 2021): the effective parent of
    /// every moved node, resolved by undo-and-replay so arrival order never
    /// matters.
    moves: TreeMoves,
    /// Every children-list placement of a movable node, keyed by the node's
    /// element id: `(list, node stamp)` pairs. A node has one per list it was ever
    /// inserted or moved into; the move-log fold picks which is live.
    placements: HashMap<ElementId, Vec<Placement>>,
    /// Which node owns each `(list, stamp)` placement. A delete reads it to tell
    /// in O(1) whether it tombstoned a movable node — and re-folds only then, not
    /// on every plain-list delete — and a write reads it to resolve a collision:
    /// a document holds at most one placement per key, so two ops carrying one
    /// stamp into one children list contend for it, and
    /// [`claim_placement`](Self::claim_placement) ranks them by the ops alone.
    placement_index: HashMap<(ElementId, Stamp), ElementId>,
    /// The document-level annotation set: every `RangedElement` keyed by its id.
    /// Endpoints are fixed at create; the payload is LWW-by-stamp; a tombstoned
    /// entry is retained so delete wins over a concurrent payload change and
    /// survives a snapshot reload.
    ranged: HashMap<ElementId, RangedEntry>,
    /// The document-level authorization set: every ACL tuple keyed by its id.
    /// Tuples are immutable; a revoked one is retained as a tombstone so the
    /// revoke wins on merge and survives a snapshot reload. Storage only — core
    /// merges the set but enforces no authority (see [`crate::acl`]).
    acl: HashMap<ElementId, AclEntry>,
    /// The lamport clock of the root partition — the one every op the envelope's
    /// [`zone`](crate::op::Op::zone) leaves `None` is stamped from: an op governing an
    /// unzoned region, one whose region names no partition, and every op of a document
    /// with no zones. With no zones this is the document's whole lamport clock, exactly
    /// as before zones.
    lamport: u64,
    /// The per-zone lamport clocks, keyed by compact zone id
    /// ([`zone::zone_id_of`]). Each declared zone advances its own clock, so an op
    /// in one zone never bumps another's — the partitions are causally independent,
    /// the property that lets each zone later replicate as its own stream. A zone
    /// absent here has clock 0 (never yet stamped). The root partition is `lamport`
    /// above, not an entry here; an empty map is a document behaving exactly as one
    /// with no zones.
    zone_clocks: HashMap<u32, u64>,
    /// The id-space high-water of every client whose stamps this replica holds:
    /// the highest lamport reached by any stamp under that id, run
    /// reservations included. A partition clock is bounded, so it is not an upper
    /// bound over the stamps present in the document — this is, and it is what
    /// [`emit_stamped`](Self::emit_stamped) mints above, so a local mint clears its
    /// own id space instead of trusting the clock. Keyed by client because a stamp
    /// names its author: those are the only stamps a local mint can collide with,
    /// and a replica that adopts a snapshot under a different identity
    /// ([`adopt_as`](Self::adopt_as)) must read that identity's high-water, not the
    /// encoder's.
    ///
    /// Carried in the state encoding beside the clocks, because it cannot be
    /// recovered from the content alone: a counter stores no stamp, and an ACL or
    /// ranged entry stores only the id *derived* from one. What a decode *can* see —
    /// a sequence's node ids, a dead run's whole reach, and the reservations of the
    /// ops still in the encoded buffer — is used as a **floor** under the stored
    /// figure, since a stored figure is supplied by whoever hands the bytes over and
    /// under-declaring it must buy nothing ([`read_state`](Self::read_state)).
    ///
    /// A projection cuts it to the recipient's own entry and then re-floors from the
    /// content that survived ([`scrub_high_water_to`](Self::scrub_high_water_to)):
    /// every other client's entry counts what that replica minted inside the
    /// withheld partition, and the recipient's own is what lets a zone- or
    /// read-scoped snapshot be adopted without re-issuing its live ids.
    ///
    /// Only the lamport is kept. A stamp's derived-id key ([`stamp_key`]) is
    /// `lamport ++ client` and does not include the sub-lamport `offset`, so two
    /// stamps differing only in offset derive the *same* ACL, ranged and XML-child
    /// ids — the offset is a tiebreak inside one run, never a dimension a mint may
    /// move in, and an op that sits there is refused
    /// ([`stamp_occupies_a_mintable_position`]).
    stamp_high_water: HashMap<ClientId, u64>,
    /// Set when a mint inside the current intention was refused, and read by every
    /// later mint in it.
    ///
    /// A refusal has to take the rest of the intention, not just the edit that hit
    /// it. The edits in one transact address what the earlier ones created — so a
    /// create refused in an exhausted partition would leave the writes into it
    /// naming a container no replica will ever hold, and a peer buffers such an op
    /// forever waiting on an arrival that cannot come.
    ///
    /// It cuts the batch at the refusal; it does not take back what came before.
    /// Those ops are already applied to local state, and dropping them from the
    /// batch while keeping them locally is precisely a divergence. What the latch
    /// guarantees is that every op that *is* emitted names something that exists.
    ///
    /// Not persisted. It is raised for the rest of one intention — an atomic group
    /// is several transacts and one delivery, so it spans them all — and cleared
    /// only where an intention *opens*, which is what lets
    /// [`mint_refused`](Self::mint_refused) report on the intention the caller just
    /// ran.
    ///
    /// One rule follows and is the reason clearing is on the opening side alone: an
    /// atomic group nested inside an explicit intention **joins** that intention for
    /// the purpose of the mint, so the latch spans the whole of the outer one and
    /// neither end of the group clears it. Clearing at a nested `commit_atomic` would
    /// hand the mint a fresh answer mid-intention, which is exactly what
    /// [`begin_atomic`](Self::begin_atomic) is already guarded against on the way in.
    /// The undo history draws its own boundary at that same point and is not this
    /// field's concern.
    mint_refused: bool,
    seq: u64,
    /// When recording an atomic transaction (between `begin_atomic` and
    /// `commit_atomic`), the ops emitted so far accumulate here instead of being
    /// returned per edit, so several edits commit as one group.
    atomic: Option<Vec<Op>>,
    seen: HashSet<OpId>,
    /// Ops the current state cannot express, held until it can: a target whose
    /// create is unseen, a delete whose nodes are absent, a member of an incomplete
    /// atomic group ([`ready`](Self::ready)).
    buffer: Vec<Op>,
    buffered: HashSet<OpId>,
    /// The `(author, group id)` keys whose bucket has resolved. A member arriving
    /// under one is untagged and merges standalone.
    ///
    /// Committing a bucket *spends* its key: the members leave the buffer, and a
    /// later member of that key would otherwise start a fresh bucket at an arrival
    /// count the group has already met and no further arrival can meet again. Which
    /// members commit and which are left holding is then the arrival order's, so two
    /// replicas fold one op set to two states — a rewritten `count` consistent across
    /// every member, an unrelated op of the same author carrying a live group's id,
    /// and one op id delivered under two envelopes all reach that shape, and none is
    /// malformed on any member's own terms. Recording the key is what makes a stray's
    /// fate a function of the op set rather than of when it arrived.
    ///
    /// A key is spent at each of the four points a bucket resolves. A bucket that
    /// **commits** spends it. A local **mint** spends it too — the author applies its
    /// own edits as it makes them and buckets nothing, so a group it tags is resolved
    /// where a receiver's is on commit, or an author would hold a stray every receiver
    /// merged. **Eviction** spends the keys it gives up on, or a member arriving after
    /// one would wait on a group this replica has already released. And a member
    /// arriving under a group other than the one the buffer holds it under spends
    /// *both*: only one of the two can ever hold this id, and which one is the arrival
    /// order's.
    ///
    /// Carried in the state encoding: a group resolved before a restart is one whose
    /// stray still has to land after it. Every entry is charged to a bucket this
    /// replica held, committed, evicted or minted — the foreign key a conflicting
    /// envelope names is charged to the bucket it was caught against, and there is at
    /// most one, since spending untags that bucket and the next envelope for the same
    /// id then finds nothing held under a group. So the count is at most one key per
    /// group minted or committed, plus at most two per op id ever buffered under a
    /// group — the same order as the dedup set the replica already carries per op,
    /// and never more than a constant times what it has held. What bounds it is that
    /// dedup set and not the buffer: a key outlives the ops that earned it exactly as
    /// a `seen` entry does, so an eviction policy empties the buffer and keeps the
    /// key. A group id colliding with one this replica has already spent — the
    /// 64-bit birthday bound on [`TxId::derive`], which hashes member sequences alone
    /// — costs the later group its atomic view at receivers rather than its members.
    resolved_tx: HashSet<(ClientId, TxId)>,
    /// Movable nodes that hold no placement and are awaiting their first — a node
    /// materialized (identity + tag) with no position, which the next
    /// [`XmlMove`](crate::op::OpKind::XmlMove) places, where a move of any other
    /// placement-less node (a keyed root) is a no-op. Two things land here: an
    /// [`XmlReveal`](crate::op::OpKind::XmlReveal) shell, and a birth that lost its
    /// `(list, stamp)` key, which is left in the same placeless state. Cleared once
    /// the node is placed.
    ///
    /// Not itself persisted. A decode re-derives the entry for any unplaced node the
    /// snapshot holds under a children list
    /// ([`restore_unplaced`](Self::restore_unplaced)) — which recovers a losing birth,
    /// since it keeps its birth list as its parent link, but not a shell, which has no
    /// such link to key on.
    revealed_pending: HashSet<ElementId>,
    orphans: Vec<OrphanEvent>,
    /// Ops emitted by the transact currently in progress.
    pending: Vec<Op>,
    /// An opt-in schema the document is checked against. `None` disables all
    /// repair observation — the document reports no `onRepaired` repairs.
    schema: Option<Schema>,
    /// The repair readings surfaced as of the last `take_repairs`, so a standing
    /// repair is told apart from a newly-needed or newly-changed one. Kept
    /// meaningful only while a schema is bound.
    repair_baseline: Vec<RepairId>,
    /// The recorded undo history — the inverse of every op this replica emitted
    /// while an origin was set, stacked by intention. Inert (and empty) until
    /// [`set_undo_origin`](Self::set_undo_origin) turns recording on, so a
    /// replica that never authors undoable edits — a server's — carries none.
    history: History,
}

impl Drop for Document {
    fn drop(&mut self) {
        // Break every parent→child link first, via the flat registry, so a
        // deeply nested tree frees iteratively instead of recursing through the
        // chain of Rc drops (which a caller-supplied path depth could overflow).
        // Lists are cleared too: a composite sequence node holds its child, and a
        // tree move can place a node's own subtree back into it, closing an Rc
        // cycle that clearing the maps alone would leak.
        // Skip a handle a caller is still borrowing rather than panic in drop.
        for map in self.maps.values() {
            if let Ok(mut map) = map.try_borrow_mut() {
                map.clear();
            }
        }
        for list in self.lists.values() {
            if let Ok(mut list) = list.try_borrow_mut() {
                list.clear();
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
            xml_elements: HashMap::new(),
            xml_fragments: HashMap::new(),
            parents: HashMap::new(),
            moves: TreeMoves::new(),
            placements: HashMap::new(),
            placement_index: HashMap::new(),
            ranged: HashMap::new(),
            acl: HashMap::new(),
            lamport: 0,
            zone_clocks: HashMap::new(),
            stamp_high_water: HashMap::new(),
            mint_refused: false,
            seq: 0,
            atomic: None,
            seen: HashSet::new(),
            buffer: Vec::new(),
            buffered: HashSet::new(),
            resolved_tx: HashSet::new(),
            revealed_pending: HashSet::new(),
            orphans: Vec::new(),
            pending: Vec::new(),
            schema: None,
            repair_baseline: Vec::new(),
            history: History::default(),
        }
    }

    pub fn client(&self) -> ClientId {
        self.client
    }

    /// The ids of every op this replica has applied — the dedup set, so a
    /// reconstructing server can restore its own dedup from a decoded snapshot. In a
    /// projection that ran it is instead what the *recipient* published, one of
    /// several ways such a document is not a live replica.
    pub fn seen(&self) -> impl Iterator<Item = OpId> + '_ {
        self.seen.iter().copied()
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

    /// A live (non-tombstoned) RangedElement by id, or `None` if absent or deleted.
    pub fn ranged_element(&self, id: ElementId) -> Option<RangedElement> {
        self.ranged
            .get(&id)
            .filter(|e| !e.tombstone)
            .map(|e| e.view(id))
    }

    /// The composite payload container of a live RangedElement, as a read/edit
    /// handle — `None` when the range is absent, deleted, or its payload is a
    /// leaf scalar (read that from [`ranged_element`](Self::ranged_element)).
    pub fn ranged_payload(&self, id: ElementId) -> Option<Element> {
        let e = self.ranged.get(&id).filter(|e| !e.tombstone)?;
        let Payload::Composite { kind } = e.payload else {
            return None;
        };
        self.container_element(payload_id(id, kind))
    }

    /// The registered container handle for `id`, wrapped as an Element, or `None`
    /// if no map/list/text is registered there.
    fn container_element(&self, id: ElementId) -> Option<Element> {
        if let Some(m) = self.maps.get(&id) {
            return Some(Element::Map(Rc::clone(m)));
        }
        if let Some(l) = self.lists.get(&id) {
            return Some(Element::List(Rc::clone(l)));
        }
        if let Some(t) = self.texts.get(&id) {
            return Some(Element::Text(Rc::clone(t)));
        }
        None
    }

    /// Every live RangedElement, ordered by id so the sequence is identical on
    /// every replica.
    pub fn ranged_elements(&self) -> Vec<RangedElement> {
        self.sorted_view(|e| !e.tombstone)
    }

    /// Every live RangedElement with an endpoint in sequence `seq` — "the ranges
    /// annotating this element". A cross-element range is returned for either of
    /// its sequences.
    pub fn ranged_on(&self, seq: ElementId) -> Vec<RangedElement> {
        self.sorted_view(|e| !e.tombstone && (e.start.seq == seq || e.end.seq == seq))
    }

    /// Every RangedElement's endpoint sequences keyed by id, **tombstoned ones
    /// included** — the anchor resolution the outbound per-recipient redaction
    /// gates a `RangedSetPayload`/`RangedDelete` by, so a just-deleted range still
    /// resolves to the sequences it annotated. Mirrors
    /// [`acl_records`](Self::acl_records), which likewise carries tombstoned tuples;
    /// the live [`ranged_elements`](Self::ranged_elements) view drops deleted ranges
    /// and so cannot serve their anchors.
    ///
    /// A range with a **composite payload** keys its payload container and everything
    /// registered beneath it under the same anchors, beside its own id. A payload hangs off
    /// the range rather than a map slot, so the tree walk gives it and its descendants no
    /// path and an op editing their contents would otherwise resolve to none — while the
    /// state form is redacted with the mark it belongs to. Keying them here is what lets
    /// the outbound redaction gate such an op by the mark's own governing paths, so the two
    /// seams withhold the same content. The consumer resolves an op's target through the
    /// element index first and reaches this map only for a target that index does not hold,
    /// so an entry here can never override a live element's own path.
    pub fn ranged_anchors(&self) -> HashMap<ElementId, (ElementId, ElementId)> {
        let mut out: HashMap<ElementId, (ElementId, ElementId)> = self
            .ranged
            .iter()
            .map(|(id, e)| (*id, (e.start.seq, e.end.seq)))
            .collect();
        // The tree walk is paid only where a composite payload exists to enumerate, which
        // most rooms never hold.
        let composites: Vec<(ElementId, ElementKind)> = self
            .ranged
            .iter()
            .filter_map(|(id, e)| match e.payload {
                Payload::Composite { kind } => Some((*id, kind)),
                Payload::Scalar { .. } => None,
            })
            .collect();
        if composites.is_empty() {
            return out;
        }
        let live = self.element_paths();
        let below = self.parent_index();
        for (id, kind) in composites {
            let anchors = out[&id];
            let mut subtree = Vec::new();
            self.registered_subtree(payload_id(id, kind), &below, &live, &mut subtree);
            out.extend(subtree.into_iter().map(|child| (child, anchors)));
        }
        out
    }

    /// Every container id this replica has materialised — live in the tree, or retained
    /// in the persistent identity registry after a displacement — for a consumer that has
    /// to tell "this replica holds the element, the tree walk just does not reach it"
    /// from "this replica has never held the element at all".
    ///
    /// The two are one answer to a tree walk (both are simply absent from it) and
    /// different answers to a redaction: a *retained* container is state this replica
    /// keeps and a re-won slot restores whole, while a target the replica never held
    /// names nothing here to place, redact, or come back to.
    pub fn container_ids(&self) -> HashSet<ElementId> {
        self.maps
            .keys()
            .chain(self.lists.keys())
            .chain(self.texts.keys())
            .chain(self.counters.keys())
            .chain(self.xml_elements.keys())
            .chain(self.xml_fragments.keys())
            .copied()
            .collect()
    }

    /// A live (non-revoked) ACL tuple by id, or `None` if absent or revoked.
    pub fn acl_tuple(&self, id: ElementId) -> Option<AclTuple> {
        self.acl
            .get(&id)
            .filter(|e| !e.is_revoked())
            .map(|e| e.view(id))
    }

    /// Every ACL tuple with its revoke provenance, **revoked ones included** —
    /// id-sorted, so the sequence is identical on every replica. The authority
    /// evaluator's input ([`crate::acl::decide_capability_with_authority`]): it needs
    /// the tombstoned tuples and their revokers to decide whether each revoke was
    /// authorized. The live read views ([`acl_tuples`](Self::acl_tuples)) drop
    /// revoked tuples content-neutrally, so they cannot serve provenance.
    pub fn acl_records(&self) -> Vec<AclRecord> {
        let mut out: Vec<AclRecord> = self.acl.iter().map(|(id, e)| e.record(*id)).collect();
        out.sort_by_key(|r| r.tuple.id.as_bytes());
        out
    }

    /// Every live ACL tuple, ordered by id so the sequence is identical on every
    /// replica.
    pub fn acl_tuples(&self) -> Vec<AclTuple> {
        self.acl_view(|_| true)
    }

    /// Every live ACL tuple scoped to `path` exactly — a [`Path`](AclScope::Path)
    /// scope whose bytes equal `path`. A content-neutral storage filter:
    /// ancestor/prefix resolution is the evaluator's concern, not this set's, and an
    /// [`Element`](AclScope::Element) scope (which has no fixed path) never matches.
    pub fn acl_on(&self, path: &[u8]) -> Vec<AclTuple> {
        self.acl_view(|e| matches!(&e.scope, AclScope::Path(p) if p == path))
    }

    /// The live ACL tuples satisfying `keep`, id-sorted.
    fn acl_view(&self, keep: impl Fn(&AclEntry) -> bool) -> Vec<AclTuple> {
        let mut out: Vec<AclTuple> = self
            .acl
            .iter()
            .filter(|(_, e)| !e.is_revoked() && keep(e))
            .map(|(id, e)| e.view(*id))
            .collect();
        out.sort_by_key(|t| t.id.as_bytes());
        out
    }

    /// The active marks on character `index` of sequence `seq` — a read-time
    /// computation over the annotation set, never stored. Gathers every live
    /// same-named mark whose span covers the character and combines each name per
    /// its schema-declared [`MarkFlavor`](crate::schema::MarkFlavor): **boolean** →
    /// the presence of the highest-stamped covering mark (LWW), **value** → that
    /// mark's value, **object** → the ids of every covering instance. A name the
    /// schema does not declare defaults to object (each instance kept, nothing
    /// merged away). One [`ResolvedMark`] per covering name (a boolean that
    /// resolves to off is omitted — the set holds only the marks actually on the
    /// character), in name order.
    pub fn marks_at(&self, seq: ElementId, index: usize) -> Vec<ResolvedMark> {
        // When the sequence is a text child of a schema-typed XmlElement, only the
        // marks its type declares read as active. The allowlist is a function of
        // the enclosing element, so it is resolved once for the whole read, on the
        // first named mark.
        let mut allow: Option<Option<&[String]>> = None;
        // Group the covering marks by name, keeping each one's id and payload.
        let mut by_name: HashMap<&[u8], Vec<(ElementId, &RangedEntry)>> = HashMap::new();
        for (id, e) in &self.ranged {
            if e.tombstone {
                continue;
            }
            let Some(name) = &e.name else {
                continue;
            };
            let allowlist = *allow.get_or_insert_with(|| {
                self.schema
                    .as_ref()
                    .and_then(|s| crate::validate::marks_allowlist(self, s, seq))
            });
            if let Some(allowlist) = allowlist {
                if !allowlist.iter().any(|a| a.as_bytes() == name.as_slice()) {
                    continue;
                }
            }
            if self.covers(e, seq, index) {
                by_name.entry(name).or_default().push((*id, e));
            }
        }
        let mut out: Vec<ResolvedMark> = by_name
            .into_iter()
            .filter_map(|(name, covering)| {
                let state = self.combine_mark(name, &covering);
                // A boolean mark resolved to off is not an active mark — omit it,
                // so the result holds only the marks on the character.
                if state == MarkState::Boolean(false) {
                    return None;
                }
                Some(ResolvedMark {
                    name: name.to_vec(),
                    state,
                })
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Combine the marks of one name covering a character, per the schema flavor.
    fn combine_mark(&self, name: &[u8], covering: &[(ElementId, &RangedEntry)]) -> MarkState {
        let flavor = std::str::from_utf8(name)
            .ok()
            .and_then(|n| self.schema.as_ref()?.mark(n))
            .map(|d| d.flavor);
        match flavor {
            Some(MarkFlavor::Boolean) => {
                MarkState::Boolean(lww_scalar(covering).map_or(true, scalar_is_present))
            }
            Some(MarkFlavor::Value) => match lww_scalar(covering) {
                Some(value) => MarkState::Value(value.clone()),
                None => MarkState::Value(Scalar::Null),
            },
            // Object, and any name the schema does not declare: every covering
            // instance is independent, so keep the whole set (id-sorted).
            Some(MarkFlavor::Object) | None => {
                let mut ids: Vec<ElementId> = covering.iter().map(|(id, _)| *id).collect();
                ids.sort_by_key(|id| id.as_bytes());
                MarkState::Object(ids)
            }
        }
    }

    /// Whether the mark `e`'s span covers character `index` of sequence `seq`. A
    /// single-sequence range covers `[resolve(start), resolve(end))`; a
    /// cross-element range (its two anchors naming different sequences) is out of
    /// scope for this read and covers nothing. An anchor bound to a codepoint not
    /// yet arrived (the mark applied before its span's inserts) covers nothing
    /// until the codepoint is present — it does not collapse onto a boundary.
    fn covers(&self, e: &RangedEntry, seq: ElementId, index: usize) -> bool {
        if e.start.seq != seq || e.end.seq != seq {
            return false;
        }
        let (Some(start), Some(end)) = (
            self.resolve_index(seq, &e.start.pos),
            self.resolve_index(seq, &e.end.pos),
        ) else {
            return false;
        };
        start <= index && index < end
    }

    /// The live index a [`RelativePosition`] resolves to in sequence `seq`, or
    /// `None` if `seq` names no present Text or List, or the position is bound to
    /// a codepoint not yet in that sequence — an anchor whose sequence or referent
    /// hasn't arrived resolves to nothing rather than a boundary.
    fn resolve_index(&self, seq: ElementId, pos: &RelativePosition) -> Option<usize> {
        if let Some(t) = self.texts.get(&seq) {
            return t.borrow().resolve_position_present(pos);
        }
        if let Some(l) = self.lists.get(&seq) {
            return l.borrow().resolve_position_present(pos);
        }
        None
    }

    /// The live entries a predicate selects, viewed and ordered by id so the
    /// sequence is identical on every replica. Filters before sorting, so a
    /// selective query pays only for its matches.
    fn sorted_view(&self, keep: impl Fn(&RangedEntry) -> bool) -> Vec<RangedElement> {
        let mut out: Vec<RangedElement> = self
            .ranged
            .iter()
            .filter(|(_, e)| keep(e))
            .map(|(id, e)| e.view(*id))
            .collect();
        out.sort_by_key(|r| r.id.as_bytes());
        out
    }

    /// Whether `key` in `map_id` ever named a container — the registry retains a
    /// deleted container at the id its key derives, so a tombstoned slot can still
    /// carry container identity a leaf migration must not disturb.
    fn has_container_identity(&self, map_id: ElementId, key: &[u8]) -> bool {
        self.holds_any_container(map_id, key)
    }

    /// Whether `key` in `map_id` currently holds a retained container of a kind the
    /// key names — the identity a leaf migration must not disturb.
    fn holds_any_container(&self, map_id: ElementId, key: &[u8]) -> bool {
        ElementKind::KEY_DERIVED_CONTAINERS
            .into_iter()
            .any(|kind| self.container_handle(map_id, key, kind).is_some())
    }

    /// The retained handle of the `kind` container `key` in `map_id` derives —
    /// the exact element a snapshot migration resurrects at the old key, chosen by
    /// the kind the deleted-container tombstone recorded (a key that hosted more
    /// than one kind keeps each registered, so the recorded kind disambiguates).
    /// `None` for a kind the key does not name — an XML element, whose id mixes its
    /// tag in below the key — or one never created.
    fn container_handle(
        &self,
        map_id: ElementId,
        key: &[u8],
        kind: ElementKind,
    ) -> Option<Element> {
        if !kind.is_key_derived_container() {
            return None;
        }
        let id = ElementId::derive(map_id, key, kind);
        match kind {
            ElementKind::Map => self.maps.get(&id).map(|m| Element::Map(Rc::clone(m))),
            ElementKind::List => self.lists.get(&id).map(|l| Element::List(Rc::clone(l))),
            ElementKind::Text => self.texts.get(&id).map(|t| Element::Text(Rc::clone(t))),
            ElementKind::XmlFragment => self
                .xml_fragments
                .get(&XmlFragment::node_id(map_id, key))
                .map(|f| Element::XmlFragment(Rc::clone(f))),
            // Every kind the guard above admits resolves; the rest are spelled out
            // rather than swallowed, so a kind that becomes key-derived has to be
            // given its registry here.
            ElementKind::Scalar
            | ElementKind::Register
            | ElementKind::Counter
            | ElementKind::XmlElement => None,
        }
    }

    /// Migrate a snapshot's slots by `fate`, keyed on the slot key — the
    /// state-level analogue of translating the op stream between two schema
    /// versions, so a snapshot-served joiner converges byte-for-byte with a peer
    /// served the same history as a translated op delta. Across every map, each
    /// leaf slot (scalar / register / counter, live or tombstoned) is `Keep`t,
    /// `Drop`ped, or `Rename`d to a new key per `fate`. A live container slot is
    /// carried verbatim, mirroring the op seam, which carries a container-create
    /// verbatim rather than tear its subtree. A *deleted*
    /// container's tombstone is re-keyed faithfully: its retained create-stamp
    /// resurrects the container live at the old key — the create the op seam
    /// carries verbatim there — while the delete re-keys (a fresh tombstone at the
    /// new key under a rename, dropped under a drop), so both seams reach the same
    /// bytes. A dropped or renamed counter's element moves with its slot — dropped
    /// from the registry, or merged into the counter at the id its new key derives
    /// (matching the op seam, where renamed increments merge at that shared id) —
    /// so no phantom counter lingers. Returns whether any slot changed. `fate` is
    /// the composition of the chain's per-step key rewrites; supplying `|_| Keep`
    /// is a no-op.
    ///
    /// The create a tombstoned key retains is the highest-ranked one the key ever
    /// saw, so a container a leaf or counter displaced before the delete resurrects
    /// as readily as one the delete tombstoned directly — the op seam carries that
    /// create verbatim either way. A tombstone that resurrects nothing migrates by
    /// what the registry still holds at the key: a container of some key-derived
    /// kind there and the slot is carried verbatim rather than mis-migrated as a
    /// leaf, none and it re-keys as the leaf tombstone it reads as. An XML element
    /// ranks against the creates it wins a key from but resolves to no handle — its
    /// id mixes its tag in below the key, so the key alone does not name it — and so
    /// never resurrects; an XML fragment derives by key like the rest and does.
    pub fn migrate_leaf_slots(&mut self, fate: impl Fn(&[u8]) -> SlotFate) -> bool {
        self.migrate_leaf_slots_scoped(|_, key| fate(key))
    }

    /// As [`migrate_leaf_slots`](Self::migrate_leaf_slots), but each slot's fate is
    /// decided against its *owning map's* element id as well as its key — the seam a
    /// type-scoped migration reads, so a field rewrite declared for one map type
    /// narrows to that type's maps and leaves a same-named slot on another type
    /// untouched. The op seam narrows the same way (an op's owning element is its
    /// target map), so both converge. A `fate` that ignores the id is exactly
    /// [`migrate_leaf_slots`](Self::migrate_leaf_slots).
    pub fn migrate_leaf_slots_scoped(
        &mut self,
        fate: impl Fn(ElementId, &[u8]) -> SlotFate,
    ) -> bool {
        let mut changed = false;
        let map_ids: Vec<ElementId> = self.maps.keys().copied().collect();
        for map_id in map_ids {
            let map = Rc::clone(&self.maps[&map_id]);
            // Decide every slot's fate against the pre-migration key set, then
            // apply in two phases: take every mover out (capturing a counter's
            // tally as an isolated copy, so no in-place merge can leak between
            // movers), then re-home them. Both phases are order-independent — a
            // rename onto a key this pass also moves resolves by stamp at the
            // slot and by commutative merge at the counter id, never by the
            // traversal order.
            let mut moved: Vec<LeafMove> = Vec::new();
            let mut resurrects: Vec<Resurrect> = Vec::new();
            let keys = map.borrow().slot_keys();
            for key in keys {
                let fate = match fate(map_id, &key) {
                    SlotFate::Keep => continue,
                    other => other,
                };
                let old_counter = ElementId::derive(map_id, &key, ElementKind::Counter);
                // A deleted-container tombstone that recorded its create identity is
                // re-keyed faithfully: the container lands live at the old key (the
                // create the op seam carries verbatim there) and the delete re-keys
                // — a tombstone at the new key under a rename, dropped under a drop.
                // The recorded (stamp, kind) resolves the exact retained container,
                // and a live slot reports none — nothing there is deleted — so its
                // presence alongside a resolvable handle is the whole condition.
                // The counter registry at the key re-homes / prunes alongside via
                // the same machinery as a leaf, a separate identity from the
                // container.
                let deleted = map.borrow().slot_deleted_container(&key);
                if let Some((create_stamp, container)) = deleted.and_then(|(stamp, kind)| {
                    self.container_handle(map_id, &key, kind)
                        .map(|c| (stamp, c))
                }) {
                    let (delete_stamp, _, _) = map
                        .borrow_mut()
                        .take_slot(&key)
                        .expect("a key from slot_keys is present");
                    changed = true;
                    let tombstone_at = match fate {
                        SlotFate::Rename(to) => {
                            if let Some(captured) = self
                                .counters
                                .remove(&old_counter)
                                .map(|c| c.borrow().deep_clone())
                            {
                                moved.push(LeafMove {
                                    to: to.clone(),
                                    counter: Some(captured),
                                    slot: None,
                                });
                            }
                            Some((to, delete_stamp))
                        }
                        SlotFate::Drop => {
                            // The removed field's counter is dropped with it.
                            self.counters.remove(&old_counter);
                            None
                        }
                        SlotFate::Keep => unreachable!("filtered above"),
                    };
                    resurrects.push(Resurrect {
                        old_key: key,
                        container,
                        create_stamp,
                        tombstone_at,
                    });
                    continue;
                }
                // The slot body is carried verbatim for a container slot — a live
                // one, or a tombstoned one the branch above could not resurrect
                // (a key whose retained create resolves to no handle, an XML
                // element among them, or a tombstone retaining no create at all)
                // that the registry still holds a container for. The COUNTER
                // registry at the key's derived id migrates regardless: it is a
                // separate identity from the slot body and from any container at
                // the key, retained across displacement, so it must prune /
                // re-home even when the slot is
                // carried verbatim.
                let carry_slot = map.borrow().slot_is_live_container(&key)
                    || (map.borrow().slot_is_tombstone(&key)
                        && self.has_container_identity(map_id, &key));
                match fate {
                    SlotFate::Keep => unreachable!("filtered above"),
                    SlotFate::Drop => {
                        if self.counters.remove(&old_counter).is_some() {
                            changed = true;
                        }
                        if !carry_slot {
                            map.borrow_mut().take_slot(&key);
                            changed = true;
                        }
                    }
                    SlotFate::Rename(to) => {
                        // Take the slot body (unless the slot is carried verbatim),
                        // capturing a live counter's tally as an isolated copy.
                        let (slot, slot_counter) = if carry_slot {
                            (None, None)
                        } else {
                            let (stamp, value, tombstone) = map
                                .borrow_mut()
                                .take_slot(&key)
                                .expect("a key from slot_keys is present");
                            changed = true;
                            // Hold a cheap handle to a live counter for the body
                            // decision; the tally is deep-cloned lazily below, only
                            // if the registry misses.
                            let slot_counter = match &value {
                                Some(Element::Counter(c)) => Some(Rc::clone(c)),
                                _ => None,
                            };
                            let body = if slot_counter.is_some() {
                                SlotBody::LiveCounter
                            } else {
                                SlotBody::Value(value)
                            };
                            (
                                Some(SlotMove {
                                    stamp,
                                    tombstone,
                                    body,
                                }),
                                slot_counter,
                            )
                        };
                        // The retained tally rides from the registry, falling back
                        // to the live slot handle so a live counter carries its
                        // tally even if it was never registered.
                        let captured = self
                            .counters
                            .remove(&old_counter)
                            .map(|c| c.borrow().deep_clone())
                            .or_else(|| slot_counter.map(|c| c.borrow().deep_clone()));
                        if captured.is_some() {
                            changed = true;
                        }
                        if slot.is_some() || captured.is_some() {
                            moved.push(LeafMove {
                                to,
                                counter: captured,
                                slot,
                            });
                        }
                    }
                }
            }
            for mv in moved {
                let LeafMove { to, counter, slot } = mv;
                // Re-home a retained tally to the id the new key derives, merging
                // into any counter already there — a cross-type key collision sums
                // rather than clobbers, as the renamed increment ops would at that
                // shared id.
                let rehomed = counter.map(|captured| {
                    let new = ElementId::derive(map_id, &to, ElementKind::Counter);
                    let dest = Rc::clone(
                        self.counters
                            .entry(new)
                            .or_insert_with(|| Rc::new(RefCell::new(Counter::new(new)))),
                    );
                    dest.borrow_mut().merge(&captured);
                    dest
                });
                let Some(SlotMove {
                    stamp,
                    tombstone,
                    body,
                }) = slot
                else {
                    // Only the counter re-homed; a carried container slot stays put.
                    continue;
                };
                let value = match body {
                    // The live counter's slot points at the merged registry handle
                    // the LWW winner resolves through. `rehomed` is `Some` whenever
                    // a tally was captured (always, for a live counter); a `None`
                    // leaves the slot empty rather than panicking.
                    SlotBody::LiveCounter => rehomed.map(Element::Counter),
                    // Re-derive a register's id from the new key so a snapshot-served
                    // joiner encodes the same id an op-served peer derives from the
                    // renamed RegisterSet.
                    SlotBody::Value(Some(Element::Register(r))) => {
                        let new = ElementId::derive(map_id, &to, ElementKind::Register);
                        Some(Element::Register(Rc::new(RefCell::new(
                            r.borrow().rehomed(new),
                        ))))
                    }
                    SlotBody::Value(other) => other,
                };
                map.borrow_mut().put_slot_lww(to, stamp, value, tombstone);
            }
            // Land each resurrected container live at its old key and re-key its
            // delete. Both go through the LWW installer, so a rename onto either
            // key this pass also touched resolves by stamp, order-independent with
            // the leaf moves above.
            for r in resurrects {
                let Resurrect {
                    old_key,
                    container,
                    create_stamp,
                    tombstone_at,
                } = r;
                map.borrow_mut().put_slot_lww(
                    old_key.clone(),
                    create_stamp,
                    Some(container.clone()),
                    false,
                );
                // Reinstate only if the container actually won the old key; a
                // higher-stamped rename onto it this pass leaves the container
                // displaced, exactly as the op seam's later op at that key would.
                let won = map
                    .borrow()
                    .get(&old_key)
                    .is_some_and(|v| handles_eq(&v, &container));
                if won {
                    container.reinstate();
                } else {
                    container.displace();
                }
                if let Some((new_key, delete_stamp)) = tombstone_at {
                    map.borrow_mut()
                        .put_slot_lww(new_key, delete_stamp, None, true);
                }
            }
        }
        if changed {
            // The recorded inverses restore slot shapes this migration has just
            // rewritten, so replaying one would write back a pre-migration key.
            // The stack drops at the boundary; recording continues.
            self.history.forget();
        }
        changed
    }

    /// Drain the orphan events accumulated since the last call.
    pub fn take_orphans(&mut self) -> Vec<OrphanEvent> {
        std::mem::take(&mut self.orphans)
    }

    /// Bind a schema for `onRepaired` observation. The current state is taken as
    /// the baseline — an existing violation is not reported — so a later
    /// [`take_repairs`](Self::take_repairs) surfaces only a repair the state has
    /// come to need since. Rebinding reseeds the baseline against the new schema.
    /// Bind at a settle point, not inside an open atomic transaction, so the
    /// baseline is a committed state rather than a transient sub-state.
    pub fn set_schema(&mut self, schema: Schema) {
        self.repair_baseline = repair_ids(keyed_repairs(self, &schema));
        self.schema = Some(schema);
    }

    /// The located paths whose repair reading has newly changed against the bound
    /// schema since the last call — the `onRepaired` observation. A path surfaces
    /// when the location comes to need a repair, or a standing one's reading
    /// changes (a re-clamp to the other bound, a different surviving item after a
    /// truncation); the repaired value itself is produced by a read
    /// ([`repairs`](crate::repair::repairs)), so a consumer always reads the fresh
    /// reading and never caches a stale one.
    ///
    /// Observation is of settled state only, computed on demand: a violation that
    /// appears and resolves between two calls is never reported, and while a local
    /// atomic transaction is open the result is empty — its transient sub-states
    /// are not observed, only the state committed at `commit_atomic`. Empty with
    /// no schema bound.
    pub fn take_repairs(&mut self) -> Vec<Vec<Step>> {
        let Some(schema) = &self.schema else {
            return Vec::new();
        };
        if self.atomic.is_some() {
            return Vec::new();
        }
        let current = keyed_repairs(self, schema);
        let fresh = current
            .iter()
            .filter(|(_, id)| !self.repair_baseline.contains(id))
            .map(|(repair, _)| repair.path.clone())
            .collect();
        self.repair_baseline = repair_ids(current);
        fresh
    }

    /// Serialize the whole replica to a self-contained, canonical snapshot:
    /// every container in the by-id registries, the parent links, the LWW
    /// stamps, the dedup set, and any buffered ops. Equal states encode to
    /// identical bytes, so a re-encode of a decoded snapshot is byte-stable.
    pub fn encode_state(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_u8(&mut out, STATE_VERSION);
        out.extend_from_slice(&self.client.as_bytes());
        put_u64(&mut out, self.lamport);
        put_u64(&mut out, self.seq);

        // The per-zone lamport clocks (the root partition rides `lamport` above),
        // id-sorted for a deterministic encoding. Empty for a document with no
        // zones — a re-encode of a decoded no-zones snapshot is byte-stable.
        let mut zone_clocks: Vec<(&u32, &u64)> = self.zone_clocks.iter().collect();
        zone_clocks.sort_by_key(|(zone, _)| **zone);
        put_u32(&mut out, len_u32(zone_clocks.len()));
        for (zone, lamport) in zone_clocks {
            put_u32(&mut out, *zone);
            put_u64(&mut out, *lamport);
        }

        // The id-space high-water of every client whose stamps this replica holds,
        // id-sorted for a deterministic encoding.
        let mut high_water: Vec<(&ClientId, &u64)> = self.stamp_high_water.iter().collect();
        high_water.sort_by_key(|(client, _)| client.as_bytes());
        put_u32(&mut out, len_u32(high_water.len()));
        for (client, lamport) in high_water {
            out.extend_from_slice(&client.as_bytes());
            put_u64(&mut out, *lamport);
        }

        encode_registry(&mut out, &self.counters, |c, o| {
            c.borrow().encode_state_into(o)
        });
        encode_registry(&mut out, &self.lists, |l, o| {
            l.borrow().encode_state_into(o)
        });
        encode_registry(&mut out, &self.texts, |t, o| {
            t.borrow().encode_state_into(o)
        });
        encode_registry(&mut out, &self.maps, |m, o| m.borrow().encode_state_into(o));

        // XML nodes ride after the map/list registries their attrs/children live
        // in: an element as its id + tag, a fragment as its id alone.
        let mut xes: Vec<&Rc<RefCell<XmlElement>>> = self.xml_elements.values().collect();
        xes.sort_by_key(|x| x.borrow().id().as_bytes());
        put_u32(&mut out, len_u32(xes.len()));
        for x in xes {
            let x = x.borrow();
            out.extend_from_slice(&x.id().as_bytes());
            put_bytes(&mut out, x.tag());
        }
        let mut xfs: Vec<&Rc<RefCell<XmlFragment>>> = self.xml_fragments.values().collect();
        xfs.sort_by_key(|f| f.borrow().id().as_bytes());
        put_u32(&mut out, len_u32(xfs.len()));
        for f in xfs {
            out.extend_from_slice(&f.borrow().id().as_bytes());
        }

        // The tree-move log, in stamp order — moves only. A reloaded replica
        // replays it to restore the effective tree and the `moved_away` overlay;
        // the base edges are re-derived from the placements, not stored.
        put_u32(&mut out, len_u32(self.moves.len()));
        for (stamp, node, parent) in self.moves.log() {
            put_stamp(&mut out, &stamp);
            out.extend_from_slice(&node.as_bytes());
            out.extend_from_slice(&parent.as_bytes());
        }

        // A placement is stored only when it can't be recovered from the list
        // nodes on decode: a moved node (more than the one birth placement) whose
        // extra placements aren't derivable, or any node with a tombstoned
        // placement, whose composite value is dropped by tombstone compression. A
        // node with a single live placement — created, never moved, not deleted —
        // keeps it live in its list, so decode reconstructs it from there.
        let mut placed: Vec<(&ElementId, &Vec<Placement>)> = self
            .placements
            .iter()
            .filter(|(_, places)| {
                places.len() > 1
                    || places
                        .iter()
                        .any(|p| self.is_tombstoned_node(p.list, p.stamp))
            })
            .collect();
        placed.sort_by_key(|(node, _)| node.as_bytes());
        put_u32(&mut out, len_u32(placed.len()));
        for (node, places) in placed {
            out.extend_from_slice(&node.as_bytes());
            put_u32(&mut out, len_u32(places.len()));
            // Key-sorted, because a node's record is grown in arrival order while
            // nothing reads it positionally — the fold takes a max and the birth
            // test a find. Writing it as stored would make two replicas that saw
            // one node's moves in different orders encode different bytes.
            let mut places: Vec<&Placement> = places.iter().collect();
            places.sort_by_key(|p| (p.list.as_bytes(), p.stamp));
            for p in places {
                out.extend_from_slice(&p.list.as_bytes());
                put_stamp(&mut out, &p.stamp);
            }
        }

        // The annotation set — every RangedElement, tombstoned ones included so a
        // delete survives the reload. Ordered by id for a deterministic encoding.
        let mut ranged: Vec<(&ElementId, &RangedEntry)> = self.ranged.iter().collect();
        ranged.sort_by_key(|(id, _)| id.as_bytes());
        put_u32(&mut out, len_u32(ranged.len()));
        for (id, e) in ranged {
            out.extend_from_slice(&id.as_bytes());
            put_range_anchor(&mut out, &e.start);
            put_range_anchor(&mut out, &e.end);
            match &e.payload {
                Payload::Scalar { value, stamp } => {
                    put_u8(&mut out, 0);
                    put_scalar(&mut out, value);
                    put_stamp(&mut out, stamp);
                }
                // The composite's data rides the map/list/text registries; the
                // entry stores only its kind, the id being derived.
                Payload::Composite { kind } => {
                    put_u8(&mut out, 1);
                    put_u8(&mut out, *kind as u8);
                }
            }
            put_opt_bytes(&mut out, e.name.as_deref());
            put_u8(&mut out, e.tombstone as u8);
        }

        // The authorization set — every ACL tuple, revoked ones included so a
        // revoke survives the reload. Ordered by id for a deterministic encoding.
        let mut acl: Vec<(&ElementId, &AclEntry)> = self.acl.iter().collect();
        acl.sort_by_key(|(id, _)| id.as_bytes());
        put_u32(&mut out, len_u32(acl.len()));
        for (id, e) in acl {
            out.extend_from_slice(&id.as_bytes());
            put_acl_subject(&mut out, &e.subject);
            put_acl_grant(&mut out, &e.grant);
            put_acl_effect(&mut out, e.effect);
            put_acl_scope(&mut out, &e.scope);
            out.extend_from_slice(&e.grantor.as_bytes());
            // The revokers, sorted (BTreeSet order) for a deterministic encoding.
            put_u32(&mut out, len_u32(e.revokers.len()));
            for r in &e.revokers {
                out.extend_from_slice(&r.as_bytes());
            }
        }

        let mut parents: Vec<(&ElementId, &ElementId)> = self.parents.iter().collect();
        parents.sort_by_key(|(child, _)| child.as_bytes());
        put_u32(&mut out, len_u32(parents.len()));
        for (child, parent) in parents {
            out.extend_from_slice(&child.as_bytes());
            out.extend_from_slice(&parent.as_bytes());
        }

        let mut seen: Vec<&OpId> = self.seen.iter().collect();
        seen.sort_by_key(|op| (op.client.as_bytes(), op.seq));
        put_u32(&mut out, len_u32(seen.len()));
        for op in seen {
            out.extend_from_slice(&op.client.as_bytes());
            put_u64(&mut out, op.seq);
        }

        // The group keys whose bucket has resolved, key-sorted for a deterministic
        // encoding. A stray of a group resolved before a restart has to land at the
        // restored replica too, so the record is as durable as the buffer it rules.
        let mut resolved: Vec<&(ClientId, TxId)> = self.resolved_tx.iter().collect();
        resolved.sort_by_key(|(client, tx)| (client.as_bytes(), tx.0));
        put_u32(&mut out, len_u32(resolved.len()));
        for (client, tx) in resolved {
            out.extend_from_slice(&client.as_bytes());
            put_u64(&mut out, tx.0);
        }

        // The buffer is a framed op log, itself length-prefixed so the reader
        // knows where it ends inside the document stream. Written in `op_order`,
        // because the buffer holds its ops as they arrived while nothing reads it
        // in that order: two replicas holding the same waiting ops would otherwise
        // encode different bytes.
        let mut held: Vec<&Op> = self.buffer.iter().collect();
        held.sort_by_key(|op| op_order(op.id));
        let mut framed = Vec::new();
        for op in held {
            let body = encode_op(op);
            put_u32(&mut framed, len_u32(body.len()));
            framed.extend_from_slice(&body);
        }
        put_u32(&mut out, len_u32(framed.len()));
        out.extend_from_slice(&framed);
        out
    }

    /// Project this replica in place to the root partition plus the `authorized`
    /// zones, dropping every element, edge, clock, and annotation that resolves to
    /// an unauthorized zone so a re-encode carries no trace of it — not the hidden
    /// partition's content, structure, ids, op count, or clock. This is the state
    /// half of the per-zone replication streams: a subscriber scoped to a subset of
    /// a room's zones is served a snapshot narrowed by this projection, so an
    /// unauthorized zone is wholly absent rather than redacted-but-present.
    ///
    /// An annotation or ACL tuple whose governing element the live walk does not reach
    /// resolves to no partition — the key its container was derived under is one-way, so
    /// a displaced or deleted sequence cannot be re-attributed — and is dropped rather
    /// than read as the root partition every zone-scoped subscriber holds (C52). This
    /// runs only to narrow, so a subscriber holding every declared zone keeps it by the
    /// caller declining to project, as the paragraph below already requires. A container
    /// the walk does not reach is a different question and is still served: those
    /// registry entries are what displace-then-recreate identity retains, and purging
    /// them would drop state a scoped subscriber is entitled to when its slot is re-won
    /// (C67).
    ///
    /// `recipient` is the replica identity this snapshot is served to: the causal
    /// frontier is cut back to the ids that replica itself published and every other
    /// author's go, so the withheld partition's op count stays absent while the
    /// recipient can still tell which of its own ids the room's log holds (minting
    /// walks them). `None` names no recipient and scrubs the frontier whole — right
    /// for a projection no replica will author from, and a re-mint hazard for one
    /// that will.
    ///
    /// Sound only as the final transform before [`encode_state`](Self::encode_state)
    /// on a throwaway copy: it scrubs the causal `seen` frontier and leaves the
    /// derived move relation filtered only in its persisted log — neither is a valid
    /// live-replica state. A schema with no zones leaves the document untouched; a
    /// set covering every declared zone does not — it still runs, and still scrubs,
    /// so a whole-zone subscriber is served the room's replica by the caller
    /// declining to project at all.
    pub fn project_zones(
        &mut self,
        schema: &Schema,
        authorized: &HashSet<u32>,
        recipient: Option<ClientId>,
    ) {
        if schema.zones().is_empty() {
            return;
        }
        let root = self.root_id();
        // The reachable containers that fall in an unauthorized zone — resolved by
        // the same longest-prefix rule the op envelope stamps a zone with, over the
        // live tree. The root map is never hidden (a zone rooted at `/` would name
        // the whole document); its authorized subtrees are kept, its unauthorized
        // ones pruned below.
        let paths = self.element_paths();
        let mut purge: HashSet<ElementId> = HashSet::new();
        for (id, path) in &paths {
            if *id == root {
                continue;
            }
            if let Some(zone) = zone::zone_id_of(schema, path) {
                if !authorized.contains(&zone) {
                    purge.insert(*id);
                }
            }
        }
        // Detach each hidden zone-root slot from its retained parent map, so no
        // residual slot names the partition (its key would leak the zone's
        // existence). A hidden container's parent is either the root or an
        // authorized ancestor — both retained — so this reaches every zone root.
        // Gather the slots to cut under shared borrows, then cut them, so no map is
        // read and mutated at once.
        let mut detach: Vec<(Rc<RefCell<Map>>, Vec<u8>)> = Vec::new();
        for map in self.maps.values() {
            let m = map.borrow();
            if purge.contains(&m.id()) {
                continue;
            }
            for key in m.slot_keys() {
                if let Some(child) = m.get(&key) {
                    if child.is_container() && purge.contains(&child.id()) {
                        detach.push((Rc::clone(map), key));
                    }
                }
            }
        }
        for (map, key) in detach {
            map.borrow_mut().take_slot(&key);
        }
        // An annotation rides the partition of the sequences its endpoints anchor. An
        // anchor the walk does not reach resolves to no partition at all — the key its
        // container was derived under is one-way, so a displaced or deleted sequence
        // cannot be re-attributed, and the mark outlives its region since only an
        // explicit delete tombstones it. "Names no zone" would put it in the root
        // partition, which every zone-scoped subscriber holds, so it is dropped instead:
        // this transform only ever runs to narrow, and a subscriber holding every
        // declared zone is served by the caller declining to project (C52). Decided here
        // rather than at the retain below so a dropped mark's composite payload
        // container joins `purge` — see [`ranged_purge`](Self::ranged_purge).
        let (hidden_ranged, hidden_payloads) = self.ranged_purge(&paths, |id, e| {
            let anchored = |seq: &ElementId| !purge.contains(seq) && paths.contains_key(seq);
            purge.contains(id) || !anchored(&e.start.seq) || !anchored(&e.end.seq)
        });
        purge.extend(hidden_payloads);
        // Drop the hidden containers and every id-keyed edge and annotation that
        // names one, so the registries hold only authorized state.
        self.maps.retain(|id, _| !purge.contains(id));
        self.lists.retain(|id, _| !purge.contains(id));
        self.texts.retain(|id, _| !purge.contains(id));
        self.counters.retain(|id, _| !purge.contains(id));
        self.xml_elements.retain(|id, _| !purge.contains(id));
        self.xml_fragments.retain(|id, _| !purge.contains(id));
        self.parents
            .retain(|child, parent| !purge.contains(child) && !purge.contains(parent));
        self.ranged.retain(|id, _| !hidden_ranged.contains(id));
        // ACL tuples are keyed by their own id, not a container's, so they are
        // dropped by the zone their scope resolves into (an unauthorized zone's
        // grants would leak its path) as well as by a purged id. An element scope
        // resolves through the live tree's `paths`; a resolved scope naming no zone is
        // an unzoned grant and is kept. A scope that resolves to no key sequence at all
        // — an unresolvable element, a leaf element the walk records no path for, a
        // malformed path — names no partition either, so like an orphaned annotation it
        // is dropped rather than read as the root partition every zone-scoped subscriber
        // holds. A tuple in that state is inert for enforcement, the evaluator's resolver
        // dropping it on the same lookup, so what a narrowed subscriber loses is a grant
        // that decides nothing.
        self.acl.retain(|id, e| {
            if purge.contains(id) {
                return false;
            }
            let keys = match &e.scope {
                AclScope::Path(p) => crate::path::parse_path(p),
                AclScope::Element(eid) => paths.get(eid).cloned(),
            };
            match keys {
                None => false,
                Some(keys) => match zone::zone_id_of(schema, &keys) {
                    Some(zone) => authorized.contains(&zone),
                    None => true,
                },
            }
        });
        self.placements.retain(|node, places| {
            if purge.contains(node) {
                return false;
            }
            places.retain(|p| !purge.contains(&p.list));
            !places.is_empty()
        });
        self.placement_index = self
            .placements
            .iter()
            .flat_map(|(node, places)| places.iter().map(|p| ((p.list, p.stamp), *node)))
            .collect();
        self.moves
            .retain(|child, parent| !purge.contains(&child) && !purge.contains(&parent));
        self.zone_clocks.retain(|zone, _| authorized.contains(zone));
        // The causal frontier and buffer, scrubbed of the hidden partition: buffered
        // ops are filtered to the authorized partitions, and `seen` is cut back to
        // the ids the recipient itself published. Filtering the buffer splits any
        // atomic group that straddles the cut, so its survivors are untagged — the
        // state-side face of the rule every per-op delivery filter applies, since a
        // group missing a member here can never complete at the recipient either. A
        // group wholly inside the authorized partitions keeps its tag and its
        // all-or-nothing commit. The resolved-group record goes whole: a key names
        // an author and a group, never a partition, so a kept one counts the groups
        // the withheld partition resolved — the inference the frontier scrub closes.
        let published = self.published_by(recipient);
        let split = crate::op::split_groups(
            self.buffer
                .iter()
                .filter(|op| op.zone.is_some_and(|zone| !authorized.contains(&zone))),
        );
        self.buffer
            .retain(|op| op.zone.is_none_or(|zone| authorized.contains(&zone)));
        crate::op::destrand_split(self.buffer.iter_mut(), &split);
        self.buffered = self.buffer.iter().map(|op| op.id).collect();
        self.resolved_tx.clear();
        self.scrub_frontier_to(published);
        self.scrub_high_water_to(recipient);
    }

    /// Project this replica in place to the paths a reader may read, dropping every
    /// element `reads` does not admit so a re-encode carries no trace of it — not the
    /// hidden subtree's content, structure, ids, or the ACL grants that would reveal who
    /// else may read it. `reads` is the server's composed doc-ACL read verdict at a
    /// `core::path` key sequence — the exact per-path authority the per-op fan-out gates
    /// each op on (the server's `op_read_gate` resolves an op to this same path). This is
    /// the doc-ACL analogue of [`project_zones`](Self::project_zones): the state half of the
    /// per-path read redaction, so a compacted room's cold-start snapshot is narrowed to
    /// a partial reader's granted subtrees rather than refused, and a snapshot-served
    /// joiner converges with an op-served one — the two drop exactly the same elements.
    ///
    /// A container is served only if the reader may read its whole path down from the
    /// root: op catch-up withholds the create op at any path level it may not read, and
    /// a child whose parent create was withheld never applies. So a container is dropped
    /// when *any* prefix of its path is unreadable — its own path, or an ancestor the
    /// reader is denied even where a more-specific grant re-opens the child (an
    /// unreadable container drops its whole subtree, its slot detached from its retained
    /// parent so no residual key names it). A leaf slot is read-gated at the map's path
    /// plus the slot key — the same path a keyed leaf op resolves to — so a leaf-level
    /// deny drops the slot even inside a readable container. An ACL tuple is kept only
    /// where its own governing path is readable (the op-stream redacts each `AclGrant` to
    /// that path, so a snapshot reader materializes the same ACL subset an op reader does);
    /// a RangedElement is kept only where the path of EVERY sequence its endpoints anchor
    /// is readable (require-all, so a mark leaks no content-region info at an unreadable
    /// endpoint), and a dropped one takes its composite payload container with it — the
    /// payload hangs off the mark's id rather than a map slot, so no path verdict reaches
    /// it. A reader denied nothing over a document holding no pathless registry state is
    /// left untouched, byte-identical on re-encode.
    ///
    /// A tuple or mark whose governing element the live walk does not reach — a
    /// since-deleted or displaced sequence, a scope target that left the tree — has no
    /// path, so no path verdict places it, and it is **dropped**. The root verdict was
    /// the stand-in and is not strict enough: a root grant a subtree deny carves passes
    /// the root query, and serving it a mark's name, payload and anchor id out of the
    /// carved region is the leak this closes (C52). Like [`project_zones`](Self::project_zones)
    /// this is a narrowing transform, so a reader entitled to the whole document is served
    /// by the caller declining to project at all — which is what makes the drop
    /// unconditional here, and what keeps the rule out of the business of re-deriving a
    /// whole-document verdict the authority already holds. `op_read_gate` admits the
    /// matching ops to exactly that reader, so the two catch-up seams still materialize
    /// the same subset.
    ///
    /// `recipient` is the replica identity this snapshot is served to: the causal
    /// frontier is cut back to the ids that replica itself published and every other
    /// author's go, so the denied subtrees' op count stays absent while the recipient
    /// can still tell which of its own ids the room's log holds (minting walks them).
    /// `None` names no recipient and scrubs the frontier whole — right for a projection
    /// no replica will author from, and a re-mint hazard for one that will.
    ///
    /// Sound only as the final transform before [`encode_state`](Self::encode_state) on
    /// a throwaway copy: like [`project_zones`](Self::project_zones) it scrubs the causal
    /// `seen` frontier and clears the buffer once anything is dropped (a below-floor
    /// subscriber dedups nothing before the snapshot's sequence) and leaves the derived
    /// move relation filtered only in its persisted log — neither a valid live-replica
    /// state.
    pub fn project_read_paths(
        &mut self,
        reads: impl Fn(&[Vec<u8>]) -> bool,
        recipient: Option<ClientId>,
    ) {
        let root = self.root_id();
        let paths = self.element_paths();
        let root_reads = reads(&[]);
        // A container is dropped when any non-empty prefix of its path is unreadable —
        // its own path, or an ancestor level whose create op catch-up would withhold, so
        // the whole subtree below an unreadable level goes even where a deeper grant
        // re-opens a descendant. The root map is never purged structurally.
        let denied = |path: &[Vec<u8>]| (1..=path.len()).any(|i| !reads(&path[..i]));
        let mut purge: HashSet<ElementId> = HashSet::new();
        for (id, path) in &paths {
            if *id == root {
                continue;
            }
            if denied(path) {
                purge.insert(*id);
            }
        }
        // A movable XML node created in a readable subtree but moved into a denied one is
        // kept at its readable origin, not dropped by its current position. Op catch-up
        // delivers the node's create at its birth list's path but withholds the move (an
        // XmlMove's read path is its denied destination), so the reader holds the node
        // where it last saw it and never learns it left. Dropping it by its current
        // (denied) position instead would diverge from the op stream and leave the node's
        // birth slot dangling in the retained origin list. Un-purge such a node and the
        // attrs map + children list a decoded XmlElement needs; the denied content it
        // carried into the destination (its attrs and descendants) is still cut by the
        // position rules below, so it survives only as the emptied shell the op stream
        // leaves. A node born in a *denied* subtree keeps the position verdict: the reader
        // never received its create, and where a fresh joiner would hold it is a separate
        // redaction seam left to op-stream delivery (see DECISIONS 2026-07-15).
        let list_denied = |list: &ElementId| paths.get(list).is_none_or(|p| denied(p));
        for (node, places) in &self.placements {
            let Some(kind) = self.node_kind(*node) else {
                continue;
            };
            let Some(birth) = birth_placement(*node, places) else {
                continue;
            };
            // Birth readable, current position denied — a move into a denied subtree.
            if !list_denied(&birth.list) && purge.contains(node) {
                purge.remove(node);
                if kind == ElementKind::XmlElement {
                    purge.remove(&XmlElement::attrs_id(*node));
                    purge.remove(&XmlElement::children_id(*node));
                }
            }
        }
        // Cut, from each retained map, its purged-container child slots and its
        // unreadable leaf slots — a leaf's read path is the map's path plus the slot key,
        // the same path the per-op redaction gates a keyed leaf op on, so a leaf-level
        // deny drops the slot here too. A cut counter's registry entry joins `purge` so
        // no phantom tally survives the re-encode; a scalar or register is inline in the
        // slot. Gather under shared borrows, then cut, so no map is read and mutated at
        // once.
        let mut detach: Vec<(Rc<RefCell<Map>>, Vec<u8>)> = Vec::new();
        let mut cut_leaf = false;
        for map in self.maps.values() {
            let m = map.borrow();
            let map_id = m.id();
            if purge.contains(&map_id) {
                continue;
            }
            let Some(base) = paths.get(&map_id) else {
                continue;
            };
            for key in m.slot_keys() {
                match m.get(&key) {
                    Some(child) if child.is_container() => {
                        if purge.contains(&child.id()) {
                            detach.push((Rc::clone(map), key));
                        }
                    }
                    other => {
                        let mut leaf_path = base.clone();
                        leaf_path.push(key.clone());
                        if !reads(&leaf_path) {
                            if let Some(Element::Counter(c)) = other {
                                purge.insert(c.borrow().id());
                            }
                            detach.push((Rc::clone(map), key));
                            cut_leaf = true;
                        }
                    }
                }
            }
        }
        for (map, key) in detach {
            map.borrow_mut().take_slot(&key);
        }
        // A RangedElement is redacted by the path of EVERY sequence its endpoints
        // anchor — a require-all rule — since a mark/annotation reveals content-region
        // info at both endpoints: a reader that cannot read where the range starts OR
        // ends must not materialize it. A single-sequence mark has one governing path;
        // a cross-element range has two, and both must read. This mirrors the op-stream
        // rule (op_read_gate gates each Ranged op on its distinct anchor seq paths), so
        // a snapshot-served partial reader materializes the same RangedElement subset an
        // op-served one does — with the one deviation the *prefix* rule below introduces,
        // for a reader granted a subtree whose ancestor it is denied: the op seam gates a
        // mark at its anchor's own path and delivers it, while this drops it. What the op
        // reader then holds is a dangling record, because the anchor sequence never
        // materialized for it either — the create at the denied ancestor level was
        // withheld, which is the same reasoning the container rule above rests on — so the
        // difference is a record that renders nothing, not content. An anchor seq the walk does not resolve — a sequence deleted
        // or displaced out of the tree — names no path, so no path verdict places the mark it
        // anchors, and it is dropped for the same reason a pathless scope is: the mark
        // outlives its region (only an explicit delete tombstones one), so anything short of
        // the whole document would be served its name, payload and anchor id out of a region
        // it may not read. Decided here rather than at the retain below so a dropped mark's
        // composite payload container joins `purge` — see [`ranged_purge`](Self::ranged_purge).
        // The prefix rule, not the anchor's own verdict: a sequence under an unreadable
        // ancestor is purged even where a deeper grant re-opens its own path, so reading
        // that path alone would keep a mark whose sequence the same projection just cut.
        let anchor_reads = |seq: ElementId| paths.get(&seq).is_some_and(|p| reads(p) && !denied(p));
        let ranged_before = self.ranged.len();
        let (hidden_ranged, hidden_payloads) = self.ranged_purge(&paths, |id, e| {
            purge.contains(id) || !anchor_reads(e.start.seq) || !anchor_reads(e.end.seq)
        });
        purge.extend(hidden_payloads);
        // Drop the hidden containers and every id-keyed edge and annotation that names
        // one, so the registries hold only authorized state.
        self.maps.retain(|id, _| !purge.contains(id));
        self.lists.retain(|id, _| !purge.contains(id));
        self.texts.retain(|id, _| !purge.contains(id));
        self.counters.retain(|id, _| !purge.contains(id));
        self.xml_elements.retain(|id, _| !purge.contains(id));
        self.xml_fragments.retain(|id, _| !purge.contains(id));
        self.parents
            .retain(|child, parent| !purge.contains(child) && !purge.contains(parent));
        // A retained list at a denied path is the children list of a node kept at its
        // readable origin: every node it holds sat at that node's denied current position
        // and was dropped above, and a fresh op joiner never received any of them (their
        // create's read path is that denied position). Clear it so it names no dropped
        // node and matches the empty list the op joiner folds.
        for (id, list) in &self.lists {
            if paths.get(id).is_some_and(|p| denied(p)) {
                list.borrow_mut().clear();
            }
        }
        // An ACL tuple is redacted by the path it governs, not by root read: ACL state is
        // itself privacy-sensitive — a tuple reveals a subject, an effect, and the existence
        // of a governed path — so a reader keeps it only where it may read that path. This
        // mirrors the op-stream rule (op_read_gate maps an AclGrant to its scope's path), so a
        // snapshot-served partial reader materializes the same ACL subset an op-served one
        // would. A `Path` scope is the encoded key path; an `Element` scope resolves to its
        // element's current path through `paths` (the grant follows the element). A scope
        // that resolves to no path at all — a malformed `Path`, an `Element` whose target has
        // left the tree, an `Element` naming a *leaf*, which the walk records no path for —
        // names nothing a path verdict can place, so it is dropped: this transform only ever
        // runs to narrow, and the reader entitled to the whole document is served by the
        // caller declining to project. The op stream resolves every one of those the same
        // way and admits the grant op to exactly that reader, so the two catch-ups stay
        // convergent — and a tuple in that state is inert for enforcement anyway, the
        // evaluator's resolver dropping it on the same lookup.
        let acl_before = self.acl.len();
        self.acl.retain(|id, e| {
            if purge.contains(id) {
                return false;
            }
            match &e.scope {
                AclScope::Path(p) => crate::path::parse_path(p).is_some_and(|segs| reads(&segs)),
                AclScope::Element(eid) => paths.get(eid).is_some_and(|segs| reads(segs)),
            }
        });
        let acl_cut = self.acl.len() != acl_before;
        self.ranged.retain(|id, _| !hidden_ranged.contains(id));
        let ranged_cut = self.ranged.len() != ranged_before;
        // Drop every placement and move whose list/destination is purged or at a denied
        // path — the reader never received the op that put a node there (a create or move
        // into a denied position is withheld), so a kept node keeps only the placements it
        // could see and re-folds to the last one it did, matching the op joiner.
        self.placements.retain(|node, places| {
            if purge.contains(node) {
                return false;
            }
            places.retain(|p| !purge.contains(&p.list) && !list_denied(&p.list));
            !places.is_empty()
        });
        self.placement_index = self
            .placements
            .iter()
            .flat_map(|(node, places)| places.iter().map(|p| ((p.list, p.stamp), *node)))
            .collect();
        self.moves.retain(|child, parent| {
            !purge.contains(&child)
                && !purge.contains(&parent)
                && !list_denied(&XmlElement::children_id(parent))
        });
        // Once anything is dropped, scrub the causal frontier, the buffer and the
        // resolved-group record of the hidden state so none leaks another replica's op
        // or group count, and rebuild the tree-move fold so the derived parents and
        // `moved_away` overlay match the filtered log a
        // reload replays — a node kept at its readable origin renders there, not at the
        // denied destination it was folded to, so the projected snapshot is byte-stable
        // through a round-trip. A projection that cut nothing leaves them untouched,
        // staying byte-identical on re-encode — which a reader denied nothing reaches
        // only over a document holding no pathless registry state, since that is dropped
        // whatever the predicate admits.
        if !purge.is_empty() || cut_leaf || acl_cut || ranged_cut || !root_reads {
            let published = self.published_by(recipient);
            self.refold_projected_moves();
            self.buffer.clear();
            self.buffered.clear();
            self.resolved_tx.clear();
            self.scrub_frontier_to(published);
            self.scrub_high_water_to(recipient);
        }
    }

    /// The RangedElements a projection is about to drop, paired with the **composite
    /// payload containers** that must be purged with them. Resolved before the registry
    /// retains, so the container goes with the mark by the ordinary purge path.
    ///
    /// A composite payload is registered under an id derived from the RangedElement's,
    /// linked to it rather than held in any map slot, so the root walk never reaches it:
    /// it has no path, falls in no zone, and no verdict either projection computes would
    /// ever cut it. Dropping only the `ranged` entry would therefore withhold the mark's
    /// name and anchor while re-encoding the container holding its content — the region's
    /// content, served to a reader or subscriber the mark itself was just withheld from.
    ///
    /// The payload container's own subtree goes with it: a payload is an ordinary
    /// container, so an op stream can nest containers and counters inside it, and every
    /// one of those is registered under a derived id the walk reaches no more than it
    /// reaches the payload. Collected transitively by
    /// [`registered_subtree`](Self::registered_subtree), since nothing downstream would.
    ///
    /// The predicate is the projection's own drop rule, taking the entry so a Ranged op's
    /// anchors and scope are decided once.
    fn ranged_purge(
        &self,
        live: &HashMap<ElementId, Vec<Vec<u8>>>,
        drop: impl Fn(&ElementId, &RangedEntry) -> bool,
    ) -> (HashSet<ElementId>, Vec<ElementId>) {
        let mut dropped = HashSet::new();
        let mut payloads = Vec::new();
        let mut below = None;
        for (id, entry) in self.ranged.iter().filter(|(id, e)| drop(id, e)) {
            dropped.insert(*id);
            if let Payload::Composite { kind } = entry.payload {
                let below = below.get_or_insert_with(|| self.parent_index());
                self.registered_subtree(payload_id(*id, kind), below, live, &mut payloads);
            }
        }
        (dropped, payloads)
    }

    /// The document's `parents` relation inverted — each element mapped to the elements
    /// registered directly under it. Built once and shared across a walk's roots, since it
    /// costs a scan of every parent edge.
    fn parent_index(&self) -> HashMap<ElementId, Vec<ElementId>> {
        let mut below: HashMap<ElementId, Vec<ElementId>> = HashMap::new();
        for (child, parent) in &self.parents {
            below.entry(*parent).or_default().push(*child);
        }
        below
    }

    /// `root` plus every registered element beneath it, appended to `out`.
    ///
    /// Two views unioned, because neither alone is the set a re-encode emits. The live
    /// handles reach a counter, which carries no parent edge; the `parents` relation
    /// reaches a container whose slot was deleted or displaced, which the live handles
    /// skip and which `encode_state` writes all the same. `live` names the elements the
    /// root walk reaches and none of them is ever collected: an id with a path of its own
    /// is governed by that path, and a tree-move can leave a `parents` edge pointing into
    /// a subtree the node has since left.
    ///
    /// A counter is the awkward one: it carries no parent edge, and a tombstoned slot
    /// drops it from `keys()`, so neither view reaches it once deleted. It is swept from
    /// the holding map's **slot table**, which keeps a tombstoned key, since the id is
    /// derived from (map, key) — the one place a deleted counter is still named.
    ///
    /// `below` is the inverted [`parents`](Self::parent_index) relation, built **once** by
    /// the caller and reused across every root it walks: a room holding many composite
    /// marks would otherwise re-scan every parent edge per mark, which is quadratic on a
    /// path both the fan-out and the catch-up take.
    fn registered_subtree(
        &self,
        root: ElementId,
        below: &HashMap<ElementId, Vec<ElementId>>,
        live: &HashMap<ElementId, Vec<Vec<u8>>>,
        out: &mut Vec<ElementId>,
    ) {
        let mut stack = vec![root];
        let mut seen: HashSet<ElementId> = HashSet::new();
        while let Some(id) = stack.pop() {
            if live.contains_key(&id) || !seen.insert(id) {
                continue;
            }
            out.push(id);
            if let Some(kids) = below.get(&id) {
                stack.extend(kids);
            }
            let held: Vec<Element> = if let Some(m) = self.maps.get(&id) {
                let m = m.borrow();
                // A counter carries no parent edge, and it drops out of `keys()` the moment
                // its slot is tombstoned — so neither view above reaches a deleted counter
                // slot, while `encode_state` still emits its registry entry. Its id is
                // derived from (map, key) and a tombstoned slot keeps its key, so the slot
                // table is the one place that still names it.
                for key in m.slot_keys() {
                    let counter = ElementId::derive(id, &key, ElementKind::Counter);
                    if self.counters.contains_key(&counter) {
                        stack.push(counter);
                    }
                }
                m.keys().into_iter().filter_map(|k| m.get(&k)).collect()
            } else if let Some(l) = self.lists.get(&id) {
                l.borrow().values()
            } else if let Some(x) = self.xml_elements.get(&id) {
                let x = x.borrow();
                vec![Element::Map(x.attrs()), Element::List(x.children())]
            } else if let Some(f) = self.xml_fragments.get(&id) {
                vec![Element::List(f.borrow().children())]
            } else {
                Vec::new()
            };
            for child in held {
                match child {
                    Element::Scalar(_) | Element::Register(_) => {}
                    other => stack.push(other.id()),
                }
            }
        }
    }

    /// The ids `recipient` published that this replica holds — its entries in the
    /// dedup set and in the buffer alike, read before either is scrubbed of a
    /// withheld partition.
    fn published_by(&self, recipient: Option<ClientId>) -> HashSet<OpId> {
        let Some(client) = recipient else {
            return HashSet::new();
        };
        self.seen
            .iter()
            .chain(self.buffered.iter())
            .filter(|id| id.client == client)
            .copied()
            .collect()
    }

    /// Cut the causal frontier back to `published` — the ids the recipient of a
    /// projected snapshot authored itself, as [`published_by`](Self::published_by)
    /// read them before the projection scrubbed anything — minus whatever survived
    /// in the buffer, which already holds them.
    ///
    /// A projection withholds a partition, so it cannot serve that partition's
    /// frontier: the ids of ops the recipient may not read would name their
    /// existence and count. What it can serve back is the recipient's *own*
    /// authorship, which the recipient originated and which no scrub protects it
    /// from. That is also what it must serve back: minting walks the ids the replica
    /// holds ([`free_seq`](Self::free_seq)), so a recipient that persists its
    /// identity and adopts a snapshot naming none of its own ids mints straight into
    /// ids the room's log already holds — every one of those writes deduped away at
    /// ingest, silently. A run with a hole in it is the same case one step on: the
    /// position a replica reports is the first sequence it has not published, so a
    /// hole is where a projected snapshot is adopted, and every id *above* the hole
    /// re-mints unless the frontier names them. (A redacted op delta is what leaves
    /// the hole, by withholding a member the recipient authored on a path it may no
    /// longer read; closing that seam needs a carrier for the ids an Ops frame
    /// withholds, which no frame has.)
    ///
    /// So the frontier a projected snapshot carries names one replica: the one
    /// adopting it. The existence and count of every *other* replica's ops in the
    /// withheld partition stay absent, which is what the scrub is for. What the
    /// recipient learns of its own run is bounded by who may present its identity:
    /// the id is declared, and authenticating the declaration is the transport's
    /// credential check, not this projection's — the same trust boundary that
    /// already lets a declarer author under that identity.
    ///
    /// The frontier a projection leaves is what the recipient *published*, which in
    /// a scrubbed buffer is wider than what it applied: an own op the buffer held is
    /// named here once the buffer goes, because a free id is the worse answer — the
    /// replica would author a second, different op under an identity the room's log
    /// already binds. A projected document is not a live replica in the first place
    /// (its buffer and move relation are filtered too), and the ops behind those ids
    /// sit below the snapshot's sequence, so no later delivery meets them.
    fn scrub_frontier_to(&mut self, published: HashSet<OpId>) {
        self.seen = published
            .into_iter()
            .filter(|id| !self.buffered.contains(id))
            .collect();
    }

    /// Cut the id-space high-water back to the recipient's own entry plus whatever
    /// the *surviving* content still shows, on exactly the reasoning
    /// [`scrub_frontier_to`](Self::scrub_frontier_to) cuts the causal frontier on.
    ///
    /// The map is keyed by client and every other client's entry counts what that
    /// replica minted — including in the partition this projection withholds. Serving
    /// the pre-projection figure lets a zone-scoped subscriber read how busy a zone
    /// it cannot see is, which is the inference the scrub exists to close.
    ///
    /// The recipient's own entry stays, and must: a replica that persists its
    /// identity and adopts a snapshot naming none of its own ids mints straight onto
    /// ids the room already holds, and a sequence drops a re-issued id as a replay.
    /// What the recipient learns from its own entry is its own authorship.
    ///
    /// The re-floor is not optional. **A document's record always dominates the
    /// stamps its own content carries** — that is the invariant
    /// [`read_state`](Self::read_state) enforces on the way in, by flooring a
    /// declared record with what the bytes visibly hold, and a projection that left
    /// the record below its own surviving content would decode to a document
    /// different from itself. Reading it back through the codec, rather than walking
    /// the registries by hand, is what keeps the two definitions from drifting; a
    /// projection is O(document) several times over already.
    fn scrub_high_water_to(&mut self, recipient: Option<ClientId>) {
        let before = self.stamp_high_water.len();
        self.stamp_high_water
            .retain(|client, _| Some(*client) == recipient);
        // The retain removed nothing, so the record already holds at most the
        // recipient's own entry and still dominates the content — there is nothing to
        // restore. That is a narrow case (a single-client room, or a record already
        // scrubbed); any room with a second author pays the round-trip.
        if self.stamp_high_water.len() == before {
            return;
        }
        let round_trip = Document::decode_state(&self.encode_state());
        debug_assert!(
            round_trip.is_ok(),
            "a projected replica could not decode its own snapshot"
        );
        if let Ok(own) = round_trip {
            self.stamp_high_water = own.stamp_high_water.clone();
        }
    }

    /// The synthetic [`XmlReveal`](crate::op::OpKind::XmlReveal) shell ops that reveal,
    /// to a reader admitted by `reads`, every movable node **born in a subtree the reader
    /// may not read but whose current position it may** — the op-stream half of
    /// reveal-on-move-in. This is the exact mirror of [`project_read_paths`], which keeps
    /// these same nodes at their readable current position: a node born readable is
    /// delivered by its ordinary create (no shell); a node whose current position is
    /// denied stays hidden (no shell); a node born denied but now readable gets a shell
    /// so the reader materializes it and the reader's (readable) move + content ops fold
    /// it into place. So an op-served reader given these shells converges with a
    /// snapshot-served one, which materializes the identical nodes.
    ///
    /// Each shell carries only the node's current identity and `tag` — never an op of its
    /// private origin — and is stamped at **lamport 0** under an id derived from the node
    /// (unique, and never a real authored op, which the reader could not have seen anyway
    /// since its origin was denied). Lamport 0 is deliberate: a shell must move no clock
    /// and leave no id-space record, or a synthetic op would speak for the origin's
    /// position. `record_stamp`'s zero-reach skip is what keeps it out of the record.
    /// The server injects these into a partial reader's catch-up delta and live
    /// fan-out; they are never authored, logged, or persisted.
    pub fn reveal_ops(&self, reads: impl Fn(&[Vec<u8>]) -> bool) -> Vec<Op> {
        let paths = self.element_paths();
        let root = self.root_id();
        let denied = |path: &[Vec<u8>]| (1..=path.len()).any(|i| !reads(&path[..i]));
        let list_denied = |list: &ElementId| paths.get(list).is_none_or(|p| denied(p));
        // Deterministic order so a re-derivation on either seam emits the same sequence.
        let mut nodes: Vec<(&ElementId, &Vec<Placement>)> = self.placements.iter().collect();
        nodes.sort_by_key(|(id, _)| id.as_bytes());
        let mut out = Vec::new();
        for (node, places) in nodes {
            let Some(kind) = self.node_kind(*node) else {
                continue;
            };
            let Some(birth) = birth_placement(*node, places) else {
                continue;
            };
            // Born readable → an ordinary create carries it; no reveal.
            if !list_denied(&birth.list) {
                continue;
            }
            // Current position denied → the node stays hidden, matching the snapshot
            // projection (which purges it by current position).
            let Some(cur) = paths.get(node) else {
                continue;
            };
            if denied(cur) {
                continue;
            }
            let tag = match kind {
                ElementKind::XmlElement => self
                    .xml_elements
                    .get(node)
                    .map(|x| x.borrow().tag().to_vec()),
                _ => None,
            };
            // The shell's stamp advances only the reader's clock, and the readable move
            // that places the node always carries a later lamport (it follows the birth),
            // so the shell's own lamport is subsumed and need not — must not — be the
            // birth stamp: the birth stamp names the origin author, which a reader who
            // could not read the origin must not learn. A zero lamport under the shell's
            // own reveal client leaks nothing and advances no clock.
            let id = reveal_op_id(*node);
            let stamp = Stamp {
                lamport: 0,
                client: id.client,
                offset: 0,
            };
            out.push(Op::new(
                id,
                stamp,
                root,
                OpKind::XmlReveal { node: *node, tag },
            ));
        }
        out
    }

    /// The container ids in a movable node's **current** subtree — its attrs Map and
    /// children List, and recursively those of every descendant element (a text run has
    /// neither). These are the targets of the node's content ops; a live reveal replays
    /// the ones a reader may now read from the room log so a mid-session reader sees the
    /// node's full current state, not just its shell — the ops the catch-up seam already
    /// delivers by re-filtering the whole delta against the node's now-readable position.
    pub fn movable_subtree_containers(&self, node: ElementId) -> HashSet<ElementId> {
        let mut out = HashSet::new();
        let mut stack = vec![node];
        while let Some(n) = stack.pop() {
            // Only an XmlElement carries an attrs Map + children List; a text run has none.
            if !self.xml_elements.contains_key(&n) {
                continue;
            }
            out.insert(XmlElement::attrs_id(n));
            let children = XmlElement::children_id(n);
            out.insert(children);
            if let Some(list) = self.lists.get(&children) {
                for child in list.borrow().values() {
                    if let Element::XmlElement(x) = child {
                        stack.push(x.borrow().id());
                    }
                }
            }
        }
        out
    }

    /// Rebuild the tree-move fold on a projected copy so its derived parent relation and
    /// `moved_away` overlay match the filtered move log a reload replays — the same
    /// reconstruction [`restore_moves`](Self::restore_moves) runs on decode, minus the
    /// birth scan (every surviving placement is already recorded) and the cycle re-check
    /// (the pre-projection tree was acyclic and filtering only removes edges). A node
    /// whose move into a denied subtree was filtered out re-folds back under its readable
    /// origin here, so the live copy renders it where a decoded joiner will. Sound only
    /// as the final transform before [`encode_state`](Self::encode_state).
    ///
    /// The created-under relation is re-seeded from every source a decode reads it from,
    /// not from the birth placements alone: this replays a log against a tree it just
    /// emptied, so a source left out is an edge the replay's cycle check cannot see, and
    /// a move the live replica refused folds here into the cyclic `parents` the encode
    /// then writes out.
    fn refold_projected_moves(&mut self) {
        // A document with no placements is a document with no tree moves, so its fold is
        // already trivial — skip the rebuild rather than pay it on every non-XML snapshot.
        if self.placements.is_empty() {
            return;
        }
        let log: Vec<(Stamp, ElementId, ElementId)> = self.moves.log().collect();
        self.moves = TreeMoves::new();
        self.reseed_bases();
        for (stamp, node, parent) in log {
            self.moves.apply(stamp, node, parent);
        }
        self.refold_moves();
    }

    /// Rebuild a replica from a snapshot, rejecting trailing bytes.
    pub fn decode_state(bytes: &[u8]) -> Result<Document, DecodeError> {
        let mut cur = Cursor::new(bytes);
        let doc = Document::read_state(&mut cur)?;
        if cur.at_end() {
            Ok(doc)
        } else {
            Err(DecodeError::TrailingBytes)
        }
    }

    /// The next per-client op sequence this replica will mint — the first one it
    /// has not already published.
    pub fn next_seq(&self) -> u64 {
        self.free_seq(self.seq)
    }

    /// The first sequence at or past `seq` this replica has not published, read
    /// off the ids it holds.
    ///
    /// The op-seq position walks those ids rather than tracking a high-water read
    /// off arriving ops. A catch-up hands a replica back the ops it authored
    /// before a restart — as an op delta, or inside a snapshot it adopts, since
    /// both id sets ride the state encoding — and an id minted a second time is
    /// dropped at every peer's dedup set, so the write is lost with nothing
    /// downstream able to detect it. Reading the ids the replica already holds
    /// keeps the position's only input to evidence it folded itself: no `seq` off
    /// the wire is ever assigned to it, and one step per held id means no frame
    /// can drive it toward the end of the space.
    ///
    /// Held, not merely applied: an op waiting on its transaction group or on an
    /// unreachable target sits in the buffer with its id out of `seen`, and it is
    /// as published as any other — the room's log holds it, and a re-mint of its
    /// id would leave the replica applying two different ops under one identity
    /// once the buffer drains.
    ///
    /// The search wraps at the end of the space rather than stopping there. A
    /// sequence space is a finite *set*, not a ladder, and what a replica holds is
    /// bounded by its memory — so a free sequence always exists, and the position
    /// is a search hint rather than a frontier. One step per held id is enough to
    /// reach one: after that many misses the ids seen were all distinct and all
    /// held, so the next candidate cannot be. That is what makes an exhausted
    /// counter unrepresentable — a decoded position near the end of the space
    /// costs a few wrapped steps, not a replica that re-issues one id forever.
    fn free_seq(&self, from: u64) -> u64 {
        let mut seq = from;
        for _ in 0..self.seen.len() + self.buffered.len() {
            let id = OpId {
                client: self.client,
                seq,
            };
            if !self.seen.contains(&id) && !self.buffered.contains(&id) {
                return seq;
            }
            seq = seq.wrapping_add(1);
        }
        seq
    }

    /// The current lamport high-water of a replication partition: the root clock
    /// for `None`, or a declared zone's own clock (`0` if that zone has never been
    /// stamped) for `Some(zone_id)`. Two replicas that have folded the same op set
    /// report identical per-partition clocks — the causal-independence invariant
    /// the per-zone replication streams build on.
    pub fn zone_clock(&self, zone: Option<u32>) -> u64 {
        self.clock(zone)
    }

    /// Rebuild a replica from a snapshot but author future ops under `client`
    /// from the op counter `next_seq`, rather than the identity and counter the
    /// snapshot was encoded with. A replica adopting a snapshot keeps its own
    /// identity and its own op-seq position, so it never re-mints an `OpId` it
    /// already made durable (which a peer would dedup away, losing the write
    /// silently).
    ///
    /// The counter the snapshot carries belongs to whoever *authored* it —
    /// typically a server's room replica, which merges under its own identity —
    /// and says nothing about the ids this replica has published, so it is
    /// replaced rather than merged in. What the adopting replica has published
    /// comes from the ids the snapshot carries and minting walks — its dedup set and
    /// its buffer, a projected snapshot's cut back to the recipient's own so the walk
    /// still reaches them. `next_seq` is where that walk starts.
    pub fn decode_state_as(
        client: ClientId,
        next_seq: u64,
        bytes: &[u8],
    ) -> Result<Document, DecodeError> {
        let mut doc = Document::decode_state(bytes)?;
        doc.adopt_as(client, next_seq);
        Ok(doc)
    }

    /// Take over a decoded snapshot as `client`, authoring from `next_seq` — the
    /// second half of [`decode_state_as`](Self::decode_state_as), separated so an
    /// adopter can decode first and derive its own position only once the bytes
    /// are known good. Deriving that position walks the ids the adopting replica
    /// holds, and a snapshot that fails to decode must not be able to make it
    /// repeat that walk.
    pub fn adopt_as(&mut self, client: ClientId, next_seq: u64) {
        self.client = client;
        self.seq = next_seq;
    }

    fn read_state(cur: &mut Cursor) -> Result<Document, DecodeError> {
        // A snapshot's own stamps are ids it visibly holds, so they are read back as
        // they decode and used as the **floor** under the high-water it declares.
        cur.track_stamps();
        let version = cur.u8()?;
        if version != STATE_VERSION {
            return Err(DecodeError::BadTag {
                what: "document state version",
                tag: version,
            });
        }
        let client = cur.client()?;
        // A declared clock is as unbounded as an op's lamport and lands straight in
        // the slot the next local mint reads, so the root clock and every zone clock
        // below are bounded at [`LAMPORT_STATE_CEILING`] — by **refusal**, never by
        // a clamp. A stored clock is its author's own high-water over the ids it has
        // published, so lowering one hands the replica ids that are still live in the
        // state it just decoded.
        let lamport = clock_within_ceiling(cur.u64()?, "document: root clock")?;
        let seq = cur.u64()?;

        let zone_clock_count = cur.u32()?;
        let mut zone_clocks: HashMap<u32, u64> =
            HashMap::with_capacity((zone_clock_count as usize).min(1024));
        for _ in 0..zone_clock_count {
            let zone = cur.u32()?;
            let lamport = clock_within_ceiling(cur.u64()?, "document: zone clock")?;
            if zone_clocks.insert(zone, lamport).is_some() {
                return Err(DecodeError::BadTag {
                    what: "document: duplicate zone clock",
                    tag: 0,
                });
            }
        }

        let high_water_count = cur.u32()?;
        let mut stamp_high_water: HashMap<ClientId, u64> =
            HashMap::with_capacity((high_water_count as usize).min(1024));
        for _ in 0..high_water_count {
            let client = cur.client()?;
            // A declared high-water lands in the slot the next local mint reads, so
            // it is bounded on the same terms as a clock: refused above the ceiling,
            // never clamped, since lowering one hands the replica live ids back.
            let lamport = clock_within_ceiling(cur.u64()?, "document: stamp high-water")?;
            if stamp_high_water.insert(client, lamport).is_some() {
                return Err(DecodeError::BadTag {
                    what: "document: duplicate stamp high-water",
                    tag: 0,
                });
            }
        }

        let counters = decode_registry(cur, |c| Counter::decode_state_from(c), |c| c.id())?;
        // Lists decode into shells with composite nodes still unresolved (like map
        // slots), collected as `list_refs` and wired once every registry exists.
        let (lists, list_refs) = decode_list_registry(cur)?;
        let texts = decode_registry(cur, |c| Text::decode_state_from(c), |t| t.id())?;

        // Maps decode in two phases: read each map as an id plus unresolved
        // slots, building an empty shell per id first, so a slot referencing
        // another map resolves against a shell that already exists.
        let map_count = cur.u32()?;
        let cap = (map_count as usize).min(1024);
        let mut decoded: Vec<DecodedMap> = Vec::with_capacity(cap);
        let mut maps: HashMap<ElementId, Rc<RefCell<Map>>> = HashMap::with_capacity(cap);
        for _ in 0..map_count {
            let dm = Map::decode_state_from(cur)?;
            if maps
                .insert(dm.id, Rc::new(RefCell::new(Map::new(dm.id))))
                .is_some()
            {
                return Err(DecodeError::BadTag {
                    what: "document: duplicate map id",
                    tag: 0,
                });
            }
            decoded.push(dm);
        }

        // XML nodes pair the map/list shells already decoded (an element's attrs
        // Map + children List, a fragment's children List) under their derived
        // ids, so they must be built before any slot or node reference resolves.
        let xml_elements = decode_xml_element_registry(cur, &maps, &lists)?;
        let xml_fragments = decode_xml_fragment_registry(cur, &lists)?;

        // The tree-move log and the placement set ride here, after the XML
        // registries their nodes reference. Read raw now; replay after the
        // document is built (it needs the resolved lists + parent links).
        let log_count = cur.u32()?;
        let mut move_log: Vec<(Stamp, ElementId, ElementId)> =
            Vec::with_capacity((log_count as usize).min(1024));
        for _ in 0..log_count {
            let stamp = cur.stamp()?;
            // A move's stamp orders the log and dedups it exactly, so a re-issued
            // one makes the move a silent no-op. It is an id this replica holds.
            cur.note_stamp_reach(stamp.client, stamp.lamport);
            let node = cur.element_id()?;
            let parent = cur.element_id()?;
            move_log.push((stamp, node, parent));
        }
        let placed_count = cur.u32()?;
        let mut placements: HashMap<ElementId, Vec<Placement>> =
            HashMap::with_capacity((placed_count as usize).min(1024));
        let mut placement_index: HashMap<(ElementId, Stamp), ElementId> = HashMap::new();
        for _ in 0..placed_count {
            let node = cur.element_id()?;
            let n = cur.u32()?;
            let mut places = Vec::with_capacity((n as usize).min(1024));
            for _ in 0..n {
                let list = cur.element_id()?;
                let stamp = cur.stamp()?;
                if placement_index.insert((list, stamp), node).is_some() {
                    return Err(DecodeError::BadTag {
                        what: "document: duplicate placement",
                        tag: 0,
                    });
                }
                // A placement's stamp is the node's Fugue id in `list` — an id this
                // replica holds, so it floors the record like any other. It is
                // usually redundant (the same id is a live node or a dead-run member
                // of that list), but a snapshot may carry a placement whose list does
                // not hold the node, and nothing cross-checks that.
                cur.note_stamp_reach(stamp.client, stamp.lamport);
                places.push(Placement { list, stamp });
            }
            if placements.insert(node, places).is_some() {
                return Err(DecodeError::BadTag {
                    what: "document: duplicate placement node",
                    tag: 0,
                });
            }
        }

        let ranged_count = cur.u32()?;
        let mut ranged: HashMap<ElementId, RangedEntry> =
            HashMap::with_capacity((ranged_count as usize).min(1024));
        for _ in 0..ranged_count {
            let id = cur.element_id()?;
            let start = cur.range_anchor()?;
            let end = cur.range_anchor()?;
            let payload = match cur.u8()? {
                0 => {
                    let value = cur.scalar()?;
                    let stamp = cur.stamp()?;
                    // A ranged scalar payload resolves LWW on this stamp, so it is
                    // held for the same reason a register's is.
                    cur.note_stamp_reach(stamp.client, stamp.lamport);
                    Payload::Scalar { value, stamp }
                }
                1 => {
                    let kind = cur.composite_payload_kind()?;
                    // A valid snapshot encodes the payload container into the
                    // registries; a stream naming a composite payload without its
                    // container is corrupt, so reject rather than decode a range
                    // whose body silently resolves to nothing.
                    let pid = payload_id(id, kind);
                    let present = match kind {
                        ElementKind::Map => maps.contains_key(&pid),
                        ElementKind::List => lists.contains_key(&pid),
                        ElementKind::Text => texts.contains_key(&pid),
                        _ => false,
                    };
                    if !present {
                        return Err(DecodeError::BadTag {
                            what: "ranged composite payload: missing container",
                            tag: 0,
                        });
                    }
                    Payload::Composite { kind }
                }
                tag => {
                    return Err(DecodeError::BadTag {
                        what: "ranged element payload flavor",
                        tag,
                    })
                }
            };
            let name = cur.opt_bytes()?;
            let tombstone = match cur.u8()? {
                0 => false,
                1 => true,
                tag => {
                    return Err(DecodeError::BadTag {
                        what: "ranged element tombstone flag",
                        tag,
                    })
                }
            };
            if ranged
                .insert(
                    id,
                    RangedEntry {
                        start,
                        end,
                        payload,
                        name,
                        tombstone,
                    },
                )
                .is_some()
            {
                return Err(DecodeError::BadTag {
                    what: "document: duplicate ranged element",
                    tag: 0,
                });
            }
        }

        let acl_count = cur.u32()?;
        let mut acl: HashMap<ElementId, AclEntry> =
            HashMap::with_capacity((acl_count as usize).min(1024));
        for _ in 0..acl_count {
            let id = cur.element_id()?;
            let subject = cur.acl_subject()?;
            let grant = cur.acl_grant()?;
            let effect = cur.acl_effect()?;
            let scope = cur.acl_scope()?;
            let grantor = cur.client()?;
            let revoker_count = cur.u32()?;
            let mut revokers = BTreeSet::new();
            for _ in 0..revoker_count {
                revokers.insert(cur.client()?);
            }
            if acl
                .insert(
                    id,
                    AclEntry {
                        subject,
                        grant,
                        effect,
                        scope,
                        grantor,
                        revokers,
                    },
                )
                .is_some()
            {
                return Err(DecodeError::BadTag {
                    what: "document: duplicate acl tuple",
                    tag: 0,
                });
            }
        }

        // Resolve each slot: leaves inline, composites cloned from the matching
        // registry handle by id, so the whole tree shares the registry Rcs.
        for dm in decoded {
            let shell = Rc::clone(&maps[&dm.id]);
            let mut m = shell.borrow_mut();
            for slot in dm.slots {
                let value = match slot.value {
                    None => None,
                    Some(SlotValue::Scalar(s)) => Some(Element::Scalar(s)),
                    Some(SlotValue::Register(r)) => {
                        Some(Element::Register(Rc::new(RefCell::new(r))))
                    }
                    Some(SlotValue::Ref(kind, id)) => Some(resolve_ref(
                        kind,
                        id,
                        &counters,
                        &lists,
                        &texts,
                        &maps,
                        &xml_elements,
                        &xml_fragments,
                    )?),
                };
                if m.insert_decoded(slot.key, slot.stamp, value, slot.tombstone, slot.container) {
                    return Err(DecodeError::BadTag {
                        what: "document: duplicate map slot",
                        tag: 0,
                    });
                }
            }
        }

        // Resolve composite sequence nodes against the same registries.
        for (list_id, stamp, kind, ref_id) in list_refs {
            let element = resolve_ref(
                kind,
                ref_id,
                &counters,
                &lists,
                &texts,
                &maps,
                &xml_elements,
                &xml_fragments,
            )?;
            if let Some(list) = lists.get(&list_id) {
                list.borrow_mut().resolve_node(stamp, element);
            }
        }

        let parent_count = cur.u32()?;
        let mut parents = HashMap::with_capacity((parent_count as usize).min(1024));
        for _ in 0..parent_count {
            let child = cur.element_id()?;
            let parent = cur.element_id()?;
            if parents.insert(child, parent).is_some() {
                return Err(DecodeError::BadTag {
                    what: "document: duplicate parent link",
                    tag: 0,
                });
            }
        }

        let root_id = ElementId::from_bytes(ROOT_ID);
        // Following parents must terminate: a cycle would hang the readiness walk
        // (`materialised`) on a later op, and `resolvable` on an undo. Memoize
        // chains already proven to terminate so the walk stays linear over an
        // untrusted graph.
        reject_parent_cycles(&parents, root_id)?;

        let seen_count = cur.u32()?;
        let mut seen = HashSet::with_capacity((seen_count as usize).min(1024));
        for _ in 0..seen_count {
            let op = OpId {
                client: cur.client()?,
                seq: cur.u64()?,
            };
            if !seen.insert(op) {
                return Err(DecodeError::BadTag {
                    what: "document: duplicate seen op",
                    tag: 0,
                });
            }
        }

        let resolved_count = cur.u32()?;
        let mut resolved_tx = HashSet::with_capacity((resolved_count as usize).min(1024));
        for _ in 0..resolved_count {
            let key = (cur.client()?, TxId(cur.u64()?));
            if !resolved_tx.insert(key) {
                return Err(DecodeError::BadTag {
                    what: "document: duplicate resolved group key",
                    tag: 0,
                });
            }
        }

        let buf_len = cur.u32()? as usize;
        let framed = cur.take(buf_len)?;
        let buffer = decode_ops(framed)?;
        // A buffered op that is already applied, or repeated, would be replayed by
        // `drain_buffer`, which dedups against neither: reject both.
        let mut buffered = HashSet::with_capacity(buffer.len().min(1024));
        for op in &buffer {
            if seen.contains(&op.id) || !buffered.insert(op.id) {
                return Err(DecodeError::BadTag {
                    what: "document: buffered op already applied or repeated",
                    tag: 0,
                });
            }
            // The drain replays these straight through `apply_now`, so an op the
            // live `apply` seam would refuse must not reach state through the
            // snapshot seam instead.
            if !op.is_admissible() {
                return Err(DecodeError::BadTag {
                    what: "document: buffered op no replica can hold",
                    tag: 0,
                });
            }
        }

        let root = maps.get(&root_id).cloned().ok_or(DecodeError::BadTag {
            what: "document: missing root map",
            tag: 0,
        })?;

        // Displacement isn't stored: a container is installed iff it is reachable
        // from the root through live edges, so mark every other one displaced.
        mark_displaced(
            &maps,
            &lists,
            &texts,
            &counters,
            &xml_elements,
            &xml_fragments,
            &ranged,
            root_id,
        );

        // **The declared record is a floor-raiser, never a lowerer.** A snapshot is
        // supplied by whoever hands the bytes over, so a declared position is exactly
        // the kind of input C17 says the mint must not trust: under-declaring the
        // record would otherwise be free, and the replica would mint straight onto
        // ids the very bytes it just decoded carry. So the declaration is combined
        // with what the snapshot *visibly* holds, and the higher wins.
        //
        // What the floor reads is every stamp the stream carries that **is an id
        // this replica holds**: a sequence's live node ids and each dead run's whole
        // reach (only its head is a stamp on the wire), a map slot's stamp and the
        // create-stamp a deleted container retains, a register's and a ranged scalar
        // payload's LWW stamp, a move-log entry's and a placement's stamp, and the
        // whole reservation of every op still waiting in the encoded buffer — a waiting op's ids are as
        // published as an applied one's. It deliberately does *not* read a stamp that
        // merely **references** an id (an anchor's parent, a range anchor's
        // position): those may name a client whose own op has not arrived, so
        // flooring on them would invent record entries the encoder never held and a
        // decoded replica could not reproduce its own bytes.
        //
        // Storing the record is still what makes it complete, because some shapes
        // leave no stamp at all: a counter tally, an ACL tuple, and a ranged
        // element's *create* — each persists only the id derived from a stamp, never
        // the stamp. (A ranged scalar *payload* does persist one, and is floored
        // above.) Those are what the declaration
        // covers and the floor cannot see — and what is left, a snapshot that hides
        // ids in one of them *and* under-declares, is the residual this design names.
        let observed = cur.take_stamp_high_water();
        for (client, lamport) in observed {
            let slot = stamp_high_water.entry(client).or_insert(0);
            *slot = (*slot).max(lamport);
        }
        // The encoded buffer decodes through a cursor of its own, and a waiting op's
        // ids are held just as much as an applied one's — the room's log carries it
        // and no peer resends it. An op that stays buffered never reaches
        // `apply_kind`, so the drain cannot stand in for this.
        for op in &buffer {
            // Skip a zero reach for `record_stamp`'s reason: it stores none, so
            // creating one here would declare an entry the encoder never wrote and a
            // re-encode would not reproduce its own bytes.
            let reach = reservation_end(op.stamp, span(&op.kind));
            if reach == 0 {
                continue;
            }
            let slot = stamp_high_water.entry(op.stamp.client).or_insert(0);
            *slot = (*slot).max(reach);
        }

        let mut doc = Document {
            client,
            root,
            maps,
            lists,
            texts,
            counters,
            xml_elements,
            xml_fragments,
            parents,
            // The move log is replayed after construction (below); the explicit
            // placements of moved nodes come off the snapshot, never-moved nodes'
            // birth placements are reconstructed there from their list nodes.
            moves: TreeMoves::new(),
            placements,
            placement_index,
            ranged,
            acl,
            lamport,
            zone_clocks,
            stamp_high_water,
            mint_refused: false,
            seq,
            atomic: None,
            seen,
            buffer,
            buffered,
            resolved_tx,
            revealed_pending: HashSet::new(),
            orphans: Vec::new(),
            pending: Vec::new(),
            schema: None,
            repair_baseline: Vec::new(),
            // A snapshot carries state, not authorship: a replica restored from
            // one has emitted nothing, so it has nothing of its own to undo.
            history: History::default(),
        };
        // Rebuild the tree-move state: the created-under parent of each placed
        // node (for the cycle check + fallback parent), then replay the move log,
        // then re-fold so `moved_away` reflects the effective tree.
        doc.restore_moves(&move_log)?;

        // The buffer holds only ops still waiting on their target; a well-formed
        // snapshot already satisfies that, so this is a no-op there. Draining
        // restores the invariant for any op decoded as already reachable rather
        // than leaving it stuck until an unrelated mutation. The untag ahead of it
        // restores the other invariant the drain assumes: no member waits under a
        // key the record has already spent.
        doc.untag_resolved();
        doc.drain_buffer();
        Ok(doc)
    }

    /// Restore the tree-move overlay from a decoded snapshot: reconstruct the
    /// birth placement of every never-moved node from its live children-list
    /// node (only moved nodes are stored explicitly), re-seed the created-under
    /// relation ([`reseed_bases`](Self::reseed_bases)) and the record of which
    /// nodes hold no placement, then replay the move log and re-fold. Both
    /// re-seedings run *before* the replay, because they are the tree the replay's
    /// cycle check reads. Finally re-check the parent relation for a cycle: replay
    /// and re-fold mutate `parents` after decode's first check, so a crafted
    /// snapshot whose moves fold into a cycle is rejected here rather than
    /// hanging a later `resolvable` walk.
    fn restore_moves(&mut self, log: &[(Stamp, ElementId, ElementId)]) -> Result<(), DecodeError> {
        self.reconstruct_births();
        self.reseed_bases();
        self.restore_unplaced();
        for &(stamp, node, parent) in log {
            self.moves.apply(stamp, node, parent);
        }
        self.refold_moves();
        reject_parent_cycles(&self.parents, ElementId::from_bytes(ROOT_ID))
    }

    /// Re-seed the created-under relation — the half of the move log's tree that
    /// the log itself does not carry, a create being permanent and so never
    /// recorded as a move. It is what the cycle check walks, so a rebuild that
    /// leaves any of it out replays the log against a shorter tree and admits a
    /// move the replica that wrote these bytes refused as a loop.
    ///
    /// A movable node's edge skips the children list and names the element that
    /// owns it, which is the parent [`refold_moves`](Self::refold_moves) derives a
    /// live placement from. It comes from the node's birth placement — the
    /// `(list, stamp)` that re-derives the node's own id — or, for a birth that
    /// lost that key and so left no placement to read it back from, from the parent
    /// link the snapshot still carries. Every other container's edge is one hop up
    /// `parents`: a chain of them is as long as the nesting, so the walk reaches a
    /// map inside an attrs map inside an element, where a single hop over the map
    /// half spans only the first of those.
    ///
    /// The three do not cover every movable node. One born at a key it then lost
    /// while holding a move placement has no birth placement to read, is not
    /// placeless, and carries a parent link naming where it moved to rather than
    /// where it was created — nothing in a snapshot still names its birth owner, so
    /// it comes back edgeless.
    fn reseed_bases(&mut self) {
        let born: Vec<(ElementId, ElementId)> = self
            .placements
            .iter()
            .filter_map(|(node, places)| {
                let birth = birth_placement(*node, places)?;
                Some((*node, *self.parents.get(&birth.list)?))
            })
            .collect();
        let unplaced: Vec<(ElementId, ElementId)> = self
            .xml_elements
            .keys()
            .chain(self.texts.keys())
            .copied()
            .filter(|node| !self.placements.contains_key(node))
            .filter_map(|node| {
                let list = *self.parents.get(&node)?;
                self.lists.contains_key(&list).then_some(())?;
                Some((node, *self.parents.get(&list)?))
            })
            .collect();
        // A node parented to a children list is a movable one, and its entry is the
        // list it *renders* in rather than the one it was created in, so it is not
        // an edge — the two sources above are those nodes'.
        let nested: Vec<(ElementId, ElementId)> = self
            .parents
            .iter()
            .filter(|(_, parent)| !self.lists.contains_key(*parent))
            .map(|(&child, &parent)| (child, parent))
            .collect();
        for (node, owner) in born.into_iter().chain(unplaced).chain(nested) {
            self.moves.set_base(node, owner);
        }
    }

    /// Rebuild the birth placement of each movable node the snapshot did not
    /// store explicitly. Only moved nodes are persisted (their extra and
    /// tombstoned placements can't be recovered); a never-moved node keeps its
    /// single birth placement live in its owner's children list, so scan those
    /// lists and register any `XmlElement`/`Text` node not already placed. At
    /// this point `moved_away` is unset, so a moved node's suppressed birth
    /// placement is still visible here — it is skipped because the node is
    /// already present from the explicit records.
    fn reconstruct_births(&mut self) {
        // Each element/fragment id derives a distinct children-list id, so the
        // registry keys enumerate every children list once with no duplicates.
        let mut births: Vec<(ElementId, ElementId, Stamp)> = Vec::new();
        for list_id in self
            .xml_elements
            .keys()
            .map(|&e| XmlElement::children_id(e))
            .chain(
                self.xml_fragments
                    .keys()
                    .map(|&f| XmlFragment::children_id(f)),
            )
        {
            let Some(list) = self.lists.get(&list_id) else {
                continue;
            };
            for (stamp, value) in list.borrow().composite_nodes() {
                if matches!(value.kind(), ElementKind::XmlElement | ElementKind::Text) {
                    let node = value.id();
                    if !self.placements.contains_key(&node) {
                        births.push((node, list_id, stamp));
                    }
                }
            }
        }
        for (node, list, stamp) in births {
            // The node has to be unplaced *and* the key free. A snapshot is bytes
            // someone hands over: an explicit record may already name this key for
            // another node while the list holds this one, and reconstructing over
            // that would leave the document holding two placements at one key —
            // accepted here and refused by the next `read_state`, which is a
            // durable room no restart can load. The stored record wins; this node
            // comes back unplaced, which is the state a birth that lost a key is
            // left in anyway.
            if self.placements.contains_key(&node)
                || self.placement_index.contains_key(&(list, stamp))
            {
                continue;
            }
            self.placements
                .entry(node)
                .or_default()
                .push(Placement { list, stamp });
            self.placement_index.insert((list, stamp), node);
        }
    }

    /// Record every movable node the snapshot holds under a children list but
    /// gives no placement as awaiting its first — the state a birth that lost its
    /// `(list, stamp)` key is left in, so a move naming it lands after a reload
    /// exactly as it does on the replica that never restarted.
    ///
    /// The parent link is what separates such a node from a document root, which
    /// sits in a map slot: a root is keyed rather than positioned, so a move of it
    /// is a no-op and it must not become movable here.
    fn restore_unplaced(&mut self) {
        let unplaced: Vec<ElementId> = self
            .xml_elements
            .keys()
            .chain(self.texts.keys())
            .copied()
            .filter(|node| {
                !self.placements.contains_key(node)
                    && self
                        .parents
                        .get(node)
                        .is_some_and(|parent| self.lists.contains_key(parent))
            })
            .collect();
        self.revealed_pending.extend(unplaced);
    }

    /// The kind of a materialised movable node — `XmlElement` or `Text`.
    fn node_kind(&self, node: ElementId) -> Option<ElementKind> {
        if self.xml_elements.contains_key(&node) {
            Some(ElementKind::XmlElement)
        } else if self.texts.contains_key(&node) {
            Some(ElementKind::Text)
        } else {
            None
        }
    }

    /// Gather local edits into ops, applying each as it is emitted.
    pub fn transact<F>(&mut self, f: F) -> Vec<Op>
    where
        F: FnOnce(&mut MapCursor),
    {
        self.pending.clear();
        // The latch spans a whole intention, so it clears at the start of one and
        // not at every `transact`: an atomic group is several transacts and one
        // all-or-nothing delivery, and clearing between them would tear exactly the
        // group the latch exists to keep whole.
        if !self.recording_intention() {
            self.mint_refused = false;
        }
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
        let ops = std::mem::take(&mut self.pending);
        // A transact is one intention unless it is part of a larger group — an
        // explicit `begin_intention` or an open atomic transaction, each of which
        // closes the intention itself.
        if self.atomic.is_none() && !self.history.grouped() {
            self.history.close(false);
        }
        // While recording an atomic transaction, edits accumulate into the group
        // rather than returning per call; the group ships on `commit_atomic`.
        match self.atomic.as_mut() {
            Some(acc) => {
                acc.extend(ops);
                Vec::new()
            }
            None => ops,
        }
    }

    /// Begin recording an atomic transaction: until [`commit_atomic`], every edit
    /// accumulates into one group and returns no ops of its own. Pair with
    /// `commit_atomic`.
    ///
    /// Groups do not nest: a second `begin_atomic` joins the open group rather
    /// than opening one inside it, so the *first* `commit_atomic` closes and
    /// returns everything recorded so far and the outer one returns nothing. A
    /// caller handed a `&mut Document` mid-group (an undo `atomic_group` body,
    /// say) must therefore not commit a group it did not open.
    pub fn begin_atomic(&mut self) {
        if self.atomic.is_none() {
            // Read before the group opens: an atomic group nested inside an explicit
            // intention joins that intention rather than starting one, so clearing
            // here would tear exactly what the latch protects.
            let opens_an_intention = !self.recording_intention();
            self.atomic = Some(Vec::new());
            if opens_an_intention {
                self.mint_refused = false;
            }
        }
    }

    /// Close the atomic transaction opened by [`begin_atomic`] and return its ops,
    /// tagged for all-or-nothing delivery — one group per zone partition the ops
    /// fall in, so a transaction stays inside one zone as ARCHITECTURE §Scope
    /// Constraints requires; untagged, and so streamed, if a partition's group is
    /// past [`MAX_TX_MEMBERS`]. Returns empty (and tags nothing) if no edits were
    /// recorded or no transaction was open. See
    /// [`tag_atomic`](Self::tag_atomic) for what a commit that straddles zones keeps
    /// and gives up.
    ///
    /// Tagging spends each group's bucket key, so closing a transaction can also apply
    /// a *foreign* member the buffer was holding under one of those ids: the returned
    /// ops are this transaction's, not everything the close changed.
    pub fn commit_atomic(&mut self) -> Vec<Op> {
        // With no transaction open there is nothing to close — and nothing to
        // record either: closing here would cut an explicit intention in half and
        // mislabel the first half atomic.
        let Some(ops) = self.atomic.take() else {
            return Vec::new();
        };
        // The group is one intention, undone and redone as one transaction.
        self.history.close(true);
        self.tag_atomic(ops)
    }

    /// Whether an atomic transaction is currently open.
    pub fn is_atomic(&self) -> bool {
        self.atomic.is_some()
    }

    /// Tag a commit's ops as atomic transactions, **one group per partition** — the
    /// scope constraint ARCHITECTURE §Scope Constraints states and §Not Shipped
    /// repeats: a transaction stays inside one zone. A commit wholly in one
    /// partition, which is every commit in a document that declares no zones, is a
    /// single group.
    ///
    /// A group is only ever received whole by a subscriber admitted to every
    /// partition it spans, since a zone-scoped subscription withholds the other
    /// partitions' members and [destrands](crate::destrand_split) the survivors —
    /// so no zone-scoped filter can cut
    /// *through* a group — it runs between them. What a straddling commit gives up
    /// is atomicity *across* the zones, which is never offered; what it keeps is every
    /// edit, and per-zone atomicity where the constraint holds. The cut is the
    /// emitter's, not a property of the whole path: a relay that re-stamps an op's
    /// partition can hand a downstream filter a group spanning two again, so every
    /// filter still destrands what it splits.
    ///
    /// A partition whose size is outside the representable range — empty, or past
    /// [`MAX_TX_MEMBERS`] — is left untagged, so its ops stream and merge
    /// individually. Tagging it would put a size on the wire every recipient's codec
    /// refuses, and the refusal is of the whole framed batch, so an oversized
    /// transaction would become dropped ops rather than a non-atomic one. The bound
    /// is per group, so it applies to each partition rather than to the commit.
    ///
    /// Each id is [derived](TxId::derive) from its own partition's member sequences,
    /// so it is as durable as the op ids it sits beside and needs no state of its
    /// own — and two partitions of one commit hold disjoint sequences, so they
    /// collide only where two distinct member sets would.
    ///
    /// Tagging resolves each group's key here, as a receiver's commit resolves it
    /// there: the author applied these edits as it made them and never buckets
    /// them, so without this a stray arriving under one of the ids would be held
    /// at the author while every receiver merged it — one op set, two states.
    fn tag_atomic(&mut self, ops: Vec<Op>) -> Vec<Op> {
        let mut partitions: HashMap<Option<u32>, Vec<u64>> = HashMap::new();
        for op in &ops {
            partitions.entry(op.zone).or_default().push(op.id.seq);
        }
        let mut tags: HashMap<Option<u32>, Tx> = HashMap::new();
        for (zone, seqs) in partitions {
            let count = match u32::try_from(seqs.len()) {
                Ok(count) if (1..=MAX_TX_MEMBERS).contains(&count) => count,
                _ => continue,
            };
            let id = TxId::derive(seqs);
            // The keys are recorded together and released once below: releasing is a
            // scan of the buffer against the whole resolved set, so running it per
            // partition would re-scan for an answer only the union decides.
            self.resolved_tx.insert((self.client, id));
            tags.insert(zone, Tx { id, count });
        }
        if tags.is_empty() {
            return ops;
        }
        self.untag_resolved();
        // Spending a key releases what it held, and a released member applies on the
        // drain — never on whenever the next unrelated arrival happens to run one.
        self.drain_buffer();
        ops.into_iter()
            .map(|mut op| {
                op.tx = tags.get(&op.zone).copied();
                op
            })
            .collect()
    }

    /// Like [`transact`](Self::transact), but tag the emitted ops as an atomic
    /// transaction. A receiver holds the members until the whole group arrives,
    /// then applies them together, so no peer observes a partial transaction. An
    /// edit set that spans two zones is emitted as one transaction per zone rather
    /// than one straddling both, and a group past [`MAX_TX_MEMBERS`] is emitted
    /// untagged and streams instead — see [`tag_atomic`](Self::tag_atomic). A
    /// member whose own dependencies are still unmet when the group arrives keeps
    /// waiting on its own — grouping never changes what a set of ops merges to,
    /// and such a member almost never has an effect the current state could show
    /// (see ARCHITECTURE §Transactions for the two that can). The
    /// author applies its own edits immediately, as with any local edit. An empty
    /// transaction tags nothing.
    pub fn atomic_transact<F>(&mut self, f: F) -> Vec<Op>
    where
        F: FnOnce(&mut MapCursor),
    {
        self.begin_atomic();
        let _ = self.transact(f);
        self.commit_atomic()
    }

    /// Give up on every atomic transaction still waiting in the buffer: untag its
    /// held members so each merges standalone. Returns how many transactions were
    /// evicted.
    ///
    /// Completeness is the only thing that releases a group, and a member no
    /// arrival brings is indistinguishable from one still in flight, so this is a
    /// way to give up rather than a rule — how long to wait first is the caller's
    /// policy, the core reading no clock. ARCHITECTURE §Opt-In: Atomic states what
    /// it costs, and why a replica that never calls it does not converge with one
    /// that does.
    ///
    /// A member still passes the ordinary readiness gate, so one whose target is
    /// unreachable waits on in the buffer as an untagged op, and the ids stay held
    /// either way — an evicted member is applied or still buffered, never free for
    /// the sequence counter to mint again.
    ///
    /// Giving up on a group **spends its bucket key**, as completing it does: a
    /// member arriving afterwards carries a size the remainder can no longer reach,
    /// so untagging the buffer while keeping the key would leave that member waiting
    /// on a group this replica has already released. Two replicas running one
    /// eviction policy would then disagree over nothing but when each ticked — which
    /// of them had already evicted when the last member landed — so the record has
    /// to cover this way of resolving a bucket alongside the others.
    pub fn evict_partial_transactions(&mut self) -> usize {
        let groups = self.tx_buckets();
        if groups.is_empty() {
            return 0;
        }
        let evicted = groups.len();
        for (key, idxs) in groups {
            for i in idxs {
                self.buffer[i].tx = None;
            }
            self.resolved_tx.insert(key);
        }
        self.drain_buffer();
        evicted
    }

    /// Fold a foreign op into local state. Returns `true` only when the op is
    /// applied now.
    ///
    /// `false` covers three unrelated situations, and only the last is permanent:
    /// the op is already applied or already held (a duplicate); it is admissible but
    /// not applicable yet, so it is buffered and replays once a create makes its
    /// target reachable or its transaction group completes; or it is one
    /// [`Op::is_admissible`] refuses, which no later arrival changes. A caller that
    /// must tell "not yet" from "never" — an ingest seam deciding what to log, dedup
    /// and acknowledge — asks the op, since the refusal is a function of the op
    /// alone and never of this document's state.
    pub fn apply(&mut self, op: &Op) -> bool {
        // Judged before the dedup, so an envelope no replica may hold decides
        // nothing about the groups this one is holding.
        if !op.is_admissible() {
            return false;
        }
        if self.seen.contains(&op.id) || self.buffered.contains(&op.id) {
            // A second envelope for a member this replica is holding. The copy it
            // kept is the one its buckets see, so exactly one of the two groups
            // named will ever hold this id, and which one is the arrival order's —
            // so both keys resolve and every order lands the same set. A plain
            // resend names the group already held and decides nothing, which is
            // what leaves an honest group waiting whole for the member it lacks.
            // Only a member the buffer is *holding* is evidence of a group; that gate
            // is `buffered_tx`, which answers `None` for every id the buffer does not
            // hold under a group. The `buffered` check ahead of it decides nothing —
            // it just spares a resend of an applied op the scan, which is ordinary
            // traffic on any transport that retries.
            if let Some(tx) = op.tx.filter(|_| self.buffered.contains(&op.id)) {
                if let Some(held) = self.buffered_tx(op.id).filter(|held| *held != tx) {
                    self.resolve_tx((op.id.client, held.id));
                    self.resolve_tx((op.id.client, tx.id));
                    self.drain_buffer();
                }
            }
            return false;
        }
        // A member of a group whose bucket has already resolved is a stray of a
        // spent key: it joins no bucket and merges standalone, so it is untagged
        // here and takes the ordinary path below.
        let stray;
        let op = if op
            .tx
            .is_some_and(|tx| self.resolved_tx.contains(&(op.id.client, tx.id)))
        {
            stray = Op {
                tx: None,
                ..op.clone()
            };
            &stray
        } else {
            op
        };
        // An atomic-transaction member is always held first; its group commits
        // together once every member is present. A lone (single-member) tx
        // completes immediately. A member whose own dependencies are unmet at
        // that point keeps waiting on its own, so `apply` reports `false` for it.
        if op.tx.is_some() {
            self.record_stamp(op.stamp, span(&op.kind));
            self.hold(op.clone());
            self.drain_buffer();
            return self.seen.contains(&op.id);
        }
        if !self.ready(op) {
            // A waiting op's ids are as published as an applied one's — the room's
            // log holds it and no peer resends it — so the mint has to clear them
            // now, not once the buffer drains. Otherwise which replica re-mints onto
            // them is a function of arrival order, and two replicas that folded the
            // same ops disagree.
            self.record_stamp(op.stamp, span(&op.kind));
            self.hold(op.clone());
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
        // the clock must clear the last of them, not just the base. The op's zone
        // is honored from the envelope, never re-derived: the sender resolved it
        // deterministically from the shared schema, and a per-zone `max` merge is
        // order-independent, so two replicas folding the same ops converge to
        // identical clocks. An op in one zone leaves every other zone's clock
        // untouched, so it forms no causal edge across the partition boundary.
        //
        // What the fold may do to the clock is bounded at
        // [`LAMPORT_WIRE_CEILING`]; the op itself is applied either way. The
        // whole reservation is clamped, not the base, because a run is as long as
        // its text.
        let last = op
            .stamp
            .lamport
            .saturating_add(span(&op.kind) - 1)
            .min(LAMPORT_WIRE_CEILING);
        self.advance_clock(op.zone, last);
        self.apply_kind(op.target, &op.kind, op.stamp, op.id.client);
    }

    /// The highest stamp position a local mint in `zone` must clear: the partition
    /// clock, and this replica's whole id-space high-water.
    ///
    /// The high-water is read **unconditionally**, across every partition. A stamp
    /// is a document-global id — a `RangedElement`'s and an ACL tuple's ids derive
    /// from one alone, and the tree-move log orders every move by one — so no
    /// per-partition rule can make a mint unique, and every attempt to scope this to
    /// "the partitions whose clocks fall short" is defeated by the same two inputs:
    /// a snapshot may declare any clock it likes, and an op's envelope names its own
    /// partition, so a peer can raise one partition's clock while planting ids that
    /// land in another's containers. The clock's own guarantees are per-partition and
    /// bounded; this one is neither, which is exactly why the mint reads it.
    ///
    /// The cost is that a partition's mint counts on from the replica's own global
    /// stamp position rather than from that partition's clock, so a zone's lamports
    /// are no longer compact. Nothing orders differently for it: folding still
    /// advances one partition's clock alone, so the per-zone streams stay causally
    /// independent, and the numbering was never a guarantee to anyone.
    fn mint_floor(&self, zone: Option<u32>) -> u64 {
        let clock = self.clock(zone);
        match self.stamp_high_water.get(&self.client) {
            Some(own) => clock.max(*own),
            None => clock,
        }
    }

    /// Record `stamp`'s whole reservation against its author's id-space high-water.
    /// Every stamp this replica holds passes here — ops folded off the wire, ops
    /// still waiting in the buffer, and local mints alike — because an id is as
    /// published while it waits as after it lands ([`free_seq`](Self::free_seq)'s
    /// rule, for the same reason: no peer resends it).
    ///
    /// The recorded reach is held to [`LAMPORT_STATE_CEILING`] so that what
    /// [`encode_state`](Self::encode_state) writes here is always inside what
    /// [`read_state`](Self::read_state) admits — C18's contract, structurally rather
    /// than by argument. Every seam into this one already refuses a stamp reaching
    /// past the ceiling ([`stamp_occupies_a_mintable_position`] on the way in,
    /// [`mint_position`] on the way out), so the clamp discards nothing; the
    /// assertion is what would catch a seam that stopped doing so.
    fn record_stamp(&mut self, stamp: Stamp, span: u64) {
        let reach = reservation_end(stamp, span);
        // A floor of zero says nothing — the map is only ever read as a lower bound —
        // and every `XmlReveal` shell derives its own `ClientId` and stamps at
        // lamport 0, so recording them would add a dead entry per revealed node to
        // memory and to every later snapshot, growing with reveal traffic.
        if reach == 0 {
            return;
        }
        debug_assert!(
            reach <= LAMPORT_STATE_CEILING,
            "a stamp reaching past the id space was installed"
        );
        let slot = self.stamp_high_water.entry(stamp.client).or_insert(0);
        *slot = (*slot).max(reach.min(LAMPORT_STATE_CEILING));
    }

    /// The current lamport high-water of a partition: the root clock for `None`,
    /// else the zone's own clock (0 if never yet stamped).
    fn clock(&self, zone: Option<u32>) -> u64 {
        match zone {
            None => self.lamport,
            Some(z) => self.zone_clocks.get(&z).copied().unwrap_or(0),
        }
    }

    /// Raise a partition's clock to at least `to` — the per-partition monotonic
    /// merge, applied to the root clock or one zone's clock alone.
    fn advance_clock(&mut self, zone: Option<u32>, to: u64) {
        match zone {
            None => {
                if to > self.lamport {
                    self.lamport = to;
                }
            }
            Some(z) => {
                let slot = self.zone_clocks.entry(z).or_insert(0);
                if to > *slot {
                    *slot = to;
                }
            }
        }
    }

    /// The partition a local `kind` edit on `target` belongs to: the compact id of
    /// the zone it resolves to, or `None` (the root partition) when no schema is
    /// bound, the schema declares no zones, or the location is unzoned. Resolved from
    /// the structural path — of the tree, or of the annotation or ACL tuple the op
    /// governs — so it is a pure function of the shared schema and converged state,
    /// and every replica assigns the same op to the same partition.
    ///
    /// A container-create belongs to the partition of the *child* it installs, not
    /// the parent it targets: the child's path is the parent's extended by the
    /// created key, so a zone owns the creation of its own root container. Without
    /// this the zone-root create would ride the parent partition and reach a
    /// subscriber not authorized to the zone (the parent partition is one it does
    /// see), materializing an empty zone-root container for it — and diverging from
    /// the snapshot projection, which drops that container. With it the create is
    /// stamped in the zone, withheld from an unauthorized subscriber on every seam.
    ///
    /// An annotation and an ACL tuple are doc-level state addressed at the root, so
    /// the target names no region at all: their partition is the region they
    /// **govern**. A `RangedElement` op belongs to the partition of the sequences its
    /// endpoints anchor — require-agreement, since endpoints in two zones are a
    /// [`CrossZoneAnchor`](crate::validate::ViolationKind::CrossZoneAnchor) violation
    /// the read repairs away, so a straddling mark takes the root partition rather than
    /// asserting one of the two — and an op editing a mark's composite payload rides
    /// the mark, the payload hanging off the range rather than a map slot. An ACL op
    /// belongs to the partition its scope resolves into. These are the same regions
    /// the snapshot projection keeps such state by, so the op seam and the state seam
    /// withhold from the same subscribers; without them a mark over a zoned sequence,
    /// and a grant naming a zoned path, ride the root partition to *every* zone-scoped
    /// subscriber, carrying the mark's name, payload and anchor id — and the grant's
    /// subject and effect — out of a zone the recipient is not admitted to (C74).
    ///
    /// A governing region that resolves to no path names no partition, and the root is
    /// the only one an envelope can express, so such an op keeps the root partition
    /// while the snapshot projection drops the state form (C52) — one of the two places
    /// the two seams still part company, tracked as C82. The partition is also a mint
    /// floor, so a follow-on that falls back this way is stamped under the family its
    /// create was stamped in, and an LWW one loses to the value it means to replace.
    ///
    /// Every other op belongs to the partition of the container it targets. A keyed op
    /// naming a zone-root slot on the container *above* it therefore rides the parent's
    /// partition unless it is a create — a gap of the same shape as the one the
    /// container-create rule closed, tracked as C83.
    fn zone_of_op(&self, target: ElementId, kind: &OpKind) -> Option<u32> {
        let schema = self.schema.as_ref()?;
        if schema.zones().is_empty() {
            return None;
        }
        let paths = self.element_paths();
        let anchored_in = |start: ElementId, end: ElementId| {
            let start = zone::zone_id_of(schema, paths.get(&start)?);
            let end = zone::zone_id_of(schema, paths.get(&end)?);
            if start == end {
                start
            } else {
                None
            }
        };
        let scoped_in = |scope: &AclScope| {
            let keys = match scope {
                AclScope::Path(p) => crate::path::parse_path(p)?,
                AclScope::Element(id) => paths.get(id)?.clone(),
            };
            zone::zone_id_of(schema, &keys)
        };
        match kind {
            OpKind::RangedCreate { start, end, .. } => anchored_in(start.seq, end.seq),
            OpKind::RangedSetPayload { id, .. } | OpKind::RangedDelete { id } => {
                let e = self.ranged.get(id)?;
                anchored_in(e.start.seq, e.end.seq)
            }
            OpKind::AclGrant { scope, .. } => scoped_in(scope),
            OpKind::AclRevoke { id } => scoped_in(&self.acl.get(id)?.scope),
            // The tree ops, listed rather than caught, because the fall-through here is
            // the **root** partition — the one every zone-scoped subscriber holds. A
            // catch-all would let a new `Ranged*` or `Acl*` variant ride it silently,
            // which is the leak this rule exists to close; spelled out, the compiler
            // asks where the variant belongs. `op_read_gate` guards its own widest
            // answer the same way.
            OpKind::RegisterSet { .. }
            | OpKind::CounterInc { .. }
            | OpKind::CounterDec { .. }
            | OpKind::MapSet { .. }
            | OpKind::MapDelete { .. }
            | OpKind::MapCreate { .. }
            | OpKind::ListCreate { .. }
            | OpKind::ListInsert { .. }
            | OpKind::ListDelete { .. }
            | OpKind::TextCreate { .. }
            | OpKind::TextInsert { .. }
            | OpKind::TextDelete { .. }
            | OpKind::XmlElementCreate { .. }
            | OpKind::XmlFragmentCreate { .. }
            | OpKind::XmlInsertChild { .. }
            | OpKind::XmlMove { .. }
            | OpKind::XmlReveal { .. } => match paths.get(&target) {
                Some(base) => {
                    let mut path = base.clone();
                    if let Some(key) = create_child_key(kind) {
                        path.push(key.to_vec());
                    }
                    zone::zone_id_of(schema, &path)
                }
                // The tree walk reaches no payload container, so a target it does not
                // hold may still be one — or a container registered beneath one.
                None => {
                    let (start, end) = self.payload_host_anchors(target)?;
                    anchored_in(start, end)
                }
            },
        }
    }

    /// The anchors of the RangedElement whose composite payload holds `target` — the
    /// payload container itself, or anything registered beneath it — or `None` when no
    /// mark hosts it. A payload container's parent link names the range it hangs off
    /// ([`install_payload`](Self::install_payload)), so the walk up the links reaches
    /// the range from anywhere in the payload's subtree.
    fn payload_host_anchors(&self, target: ElementId) -> Option<(ElementId, ElementId)> {
        let mut cur = target;
        // A chain longer than the parent map has revisited a node, so the bound is
        // what keeps a cyclic link from spinning here.
        for _ in 0..=self.parents.len() {
            let parent = *self.parents.get(&cur)?;
            if let Some(e) = self.ranged.get(&parent) {
                return Some((e.start.seq, e.end.seq));
            }
            cur = parent;
        }
        None
    }

    /// Every materialised container mapped to its `core::path` key sequence — the
    /// projection zone resolution reads. A zone governs a whole subtree, so a
    /// node-addressed child (a list item, an XML attrs map / children list /
    /// positional child) inherits its holding container's path rather than keying a
    /// new segment; only a map slot extends the path. Walks the live tree from the
    /// root, so the result reflects moves, displacement, and deletes exactly.
    fn element_paths(&self) -> HashMap<ElementId, Vec<Vec<u8>>> {
        fn walk(elem: &Element, path: &[Vec<u8>], out: &mut HashMap<ElementId, Vec<Vec<u8>>>) {
            match elem {
                Element::Map(m) => {
                    let m = m.borrow();
                    out.insert(m.id(), path.to_vec());
                    let mut child_path = path.to_vec();
                    for key in m.keys() {
                        let Some(child) = m.get(&key) else { continue };
                        if !child.is_container() {
                            continue;
                        }
                        child_path.push(key.clone());
                        walk(&child, &child_path, out);
                        child_path.pop();
                    }
                }
                Element::List(l) => {
                    let l = l.borrow();
                    out.insert(l.id(), path.to_vec());
                    for child in l.values() {
                        if child.is_container() {
                            walk(&child, path, out);
                        }
                    }
                }
                Element::Text(t) => {
                    out.insert(t.borrow().id(), path.to_vec());
                }
                Element::XmlElement(x) => {
                    let x = x.borrow();
                    out.insert(x.id(), path.to_vec());
                    walk(&Element::Map(x.attrs()), path, out);
                    walk(&Element::List(x.children()), path, out);
                }
                Element::XmlFragment(f) => {
                    let f = f.borrow();
                    out.insert(f.id(), path.to_vec());
                    walk(&Element::List(f.children()), path, out);
                }
                Element::Scalar(_) | Element::Register(_) | Element::Counter(_) => {}
            }
        }
        let mut out = HashMap::new();
        walk(&Element::Map(self.root()), &[], &mut out);
        out
    }

    /// Hold `op` in the buffer until it can apply.
    ///
    /// Appended, not placed. The order that is *state* is the one
    /// [`encode_state`](Self::encode_state) writes, which is [`op_order`]; what
    /// the stored order decides — which of several ready ops the drain replays
    /// first, which of several complete groups commits first — is not a state
    /// decision, because the drain runs to a fixpoint. Keeping it sorted would
    /// instead let the sender choose the cost: nothing caps the buffer, and where
    /// each arrival lands in it is the delivery order's.
    fn hold(&mut self, op: Op) {
        self.buffered.insert(op.id);
        self.buffer.push(op);
    }

    /// Replay buffered ops that a state change just made reachable, to a
    /// fixpoint — one applied op can unblock a whole causal chain, and a
    /// non-atomic apply can complete a waiting transaction (or vice versa).
    fn drain_buffer(&mut self) {
        loop {
            let mut progressed = false;
            while let Some(i) = self
                .buffer
                .iter()
                .position(|op| op.tx.is_none() && self.ready(op))
            {
                let op = self.buffer.remove(i);
                self.buffered.remove(&op.id);
                self.apply_now(&op);
                progressed = true;
            }
            // One complete atomic transaction: apply every member in seq order, so a
            // member that targets a container an earlier member creates reaches it on
            // the first pass. Order is a shortcut, not the mechanism — a member that
            // is not ready is re-buffered untagged and lands on the drain's fixpoint
            // either way.
            if let Some(mut members) = self.take_complete_tx() {
                // The bucket's key is spent by this commit — its members are leaving
                // the buffer and the count they met cannot be met a second time — so
                // record it before anything else can arrive under it.
                if let Some(key) = members
                    .first()
                    .and_then(|op| op.tx.map(|tx| (op.id.client, tx.id)))
                {
                    self.resolve_tx(key);
                }
                members.sort_by_key(|op| op.id.seq);
                for mut op in members {
                    // Every applied op still passes the ordinary readiness gate:
                    // routing drops a mutation whose container this replica has not
                    // materialised, so a member waved through by the group gate —
                    // its target created by another member still in flight — would
                    // silently lose its effect while a replica that saw the group
                    // against a materialised container kept it. A member that is not
                    // ready is held instead, untagged, and drains with the ordinary
                    // buffer once its create lands. What blocks a member
                    // is almost always what makes it unobservable — an unresolvable
                    // target renders nothing — so holding it leaves the group's
                    // all-or-nothing view intact; ARCHITECTURE §Transactions names
                    // the two cases where it does not.
                    if self.ready(&op) {
                        self.buffered.remove(&op.id);
                        self.apply_now(&op);
                    } else {
                        op.tx = None;
                        self.hold(op);
                    }
                }
                progressed = true;
            }
            if !progressed {
                break;
            }
        }
    }

    /// Whether `op` can apply now: its target is **materialised** — this replica
    /// holds the container, displaced or installed — and, for a delete, the nodes
    /// it removes are present. A delete of a not-yet-inserted node would silently
    /// no-op and be lost, so it waits for the insert.
    ///
    /// Displacement is not a reason to wait. A container that lost its slot is
    /// retained, so the op lands in it hidden and is reinstated with it; holding
    /// it instead would wait on a slot that need never come back, and would make
    /// the same op set land differently by arrival order (§Map Slot Safety). The
    /// three sequence cases below ask for *less* than the general gate — the
    /// holding container alone, not the whole chain to the root — because the
    /// tree fold is a function of the move-set, not of any parent's reachability.
    fn ready(&self, op: &Op) -> bool {
        // An XmlInsertChild materialises a movable node into a parent's children
        // sequence. A displaced parent still retains that sequence, so the child is
        // materialised-but-hidden rather than buffered forever — else a concurrent
        // move that reparents the child into a live container would be lost the
        // moment the birth parent loses its slot (its create landing after a
        // scalar at the same key). Readiness needs only the sequence materialised;
        // the child's visibility is then the move log's concern, not this gate's.
        if let OpKind::XmlInsertChild { .. } = &op.kind {
            return self.lists.contains_key(&op.target);
        }
        // A move is a tree-relation edit that must be logged regardless of the
        // destination parent's transient displacement: a displaced parent retains
        // its children sequence, so the move records and folds against the live
        // move-set and the node renders hidden if its effective parent is
        // displaced — consistent on every replica. Gating on the destination being
        // *installed* would instead drop the move whenever the destination lost its
        // slot before the move arrived, so the same op set folds to different trees
        // by arrival order. Needs only the destination sequence materialised and the
        // moved node present (its create may still be in flight) or a reveal shell.
        if let OpKind::XmlMove { node, .. } = &op.kind {
            return self.lists.contains_key(&op.target)
                && (self.placements.contains_key(node) || self.revealed_pending.contains(node));
        }
        // A ListDelete tombstones a sequence node, so it must apply into a
        // materialised-but-displaced sequence too — symmetric with the insert/move
        // above. A moved node keeps a live placement under its new parent even while
        // its birth sequence is displaced; buffering the delete of that birth
        // placement (which never re-installs) would let the delete lose to the move
        // on this replica while winning on one that saw the delete before the
        // displacement — a delete-wins-over-move divergence. Needs only the node
        // present in its (possibly displaced) list.
        if let OpKind::ListDelete { id } = &op.kind {
            return self
                .lists
                .get(&op.target)
                .is_some_and(|l| l.borrow().contains(*id));
        }
        if !self.materialised(op.target) {
            return false;
        }
        match &op.kind {
            OpKind::TextDelete { ids } => self.texts.get(&op.target).is_some_and(|t| {
                let t = t.borrow();
                ids.iter().all(|id| t.contains(*id))
            }),
            // A payload change or delete waits for the RangedElement's create — a
            // create carries the entry, a set/delete only mutate it, so applied
            // against a missing entry they would be silently lost. A create itself
            // has no such dependency: it stores an opaque anchor, never touching
            // the sequence it names.
            OpKind::RangedSetPayload { id, .. } | OpKind::RangedDelete { id } => {
                self.ranged.contains_key(id)
            }
            // A revoke waits for the tuple's grant — the grant carries the entry,
            // the revoke only tombstones it, so applied against a missing entry it
            // would be silently lost. A grant has no such dependency.
            OpKind::AclRevoke { id } => self.acl.contains_key(id),
            _ => true,
        }
    }

    /// Remove and return the members of one buffered atomic transaction whose
    /// whole group has arrived — or `None` if none is complete. Completeness is
    /// the only group-level gate: a member's own dependencies are the readiness
    /// gate's business at the moment it applies, so a member waiting on something
    /// outside the group holds only itself, never its group-mates. Readiness is
    /// not monotone — a container is installed, displaced, and re-installed as
    /// ops arrive — so a group-wide resolution gate would make commit a window
    /// arrival order decides, and the same ops would fold to different states.
    fn take_complete_tx(&mut self) -> Option<Vec<Op>> {
        let groups = self.tx_buckets();
        // Lowest buffer position wins when more than one group is complete, so the
        // commit order is the buffer's, not the hash map's. Draining to a fixpoint
        // after every fold keeps a replica's own buffer down to at most one complete
        // group, so this decides nothing on the live path — it is the decode of a
        // *peer-supplied* snapshot that can present several at once, and two
        // replicas reading identical bytes have to reach identical state whatever
        // those bytes hold. Which of them commits first is not a state decision: the
        // drain runs to a fixpoint, so every complete group commits, and a member
        // left unready by an earlier commit is re-held untagged and lands on the
        // same pass.
        let complete = groups
            .into_values()
            .filter(|idxs| {
                self.tx_declared_count(idxs)
                    .is_some_and(|count| count as usize == idxs.len())
            })
            .min_by_key(|idxs| idxs[0])?;
        // Remove in descending index order so earlier indices stay valid.
        let mut idxs = complete;
        idxs.sort_unstable_by(|a, b| b.cmp(a));
        Some(idxs.into_iter().map(|i| self.buffer.remove(i)).collect())
    }

    /// The group a buffered member is held under, or `None` if the buffer holds no
    /// member at `id` or holds it untagged.
    fn buffered_tx(&self, id: OpId) -> Option<Tx> {
        self.buffer.iter().find(|op| op.id == id)?.tx
    }

    /// Record `key`'s bucket as resolved and release every member still held under
    /// it, so a group id decides a member's fate once and the same way for every
    /// arrival order.
    fn resolve_tx(&mut self, key: (ClientId, TxId)) {
        if self.resolved_tx.insert(key) {
            self.untag_resolved();
        }
    }

    /// Untag every buffered member whose bucket key has resolved, so it merges
    /// standalone rather than waiting on an arrival count its bucket has spent.
    fn untag_resolved(&mut self) {
        let resolved = &self.resolved_tx;
        for op in self.buffer.iter_mut() {
            if op
                .tx
                .is_some_and(|tx| resolved.contains(&(op.id.client, tx.id)))
            {
                op.tx = None;
            }
        }
    }

    /// The buffer positions of every held transaction member, bucketed by the
    /// `(author, group id)` key a group is identified by.
    fn tx_buckets(&self) -> HashMap<(ClientId, TxId), Vec<usize>> {
        let mut groups: HashMap<(ClientId, TxId), Vec<usize>> = HashMap::new();
        for (i, op) in self.buffer.iter().enumerate() {
            if let Some(tx) = &op.tx {
                groups.entry((op.id.client, tx.id)).or_default().push(i);
            }
        }
        groups
    }

    /// The size the members held at `idxs` declare, or `None` if they do not all
    /// declare the same one — a bucket without unanimity names no group, so it is
    /// never complete.
    ///
    /// The size is the group's, not that of whichever member the buffer holds
    /// first: read off one member, a rewritten envelope chooses when the group
    /// commits. It bounds what a rewrite buys rather than removing it — a rewrite
    /// consistent across every member is one no receiver can tell from an honest
    /// group of that size (ARCHITECTURE §Opt-In: Atomic).
    fn tx_declared_count(&self, idxs: &[usize]) -> Option<u32> {
        let mut counts = idxs.iter().map(|&i| self.buffer[i].tx.map(|tx| tx.count));
        let first = counts.next().flatten()?;
        counts.all(|count| count == Some(first)).then_some(first)
    }

    /// Take the next op id this replica has not published, recording it as taken
    /// and advancing the counter past it.
    fn mint_op_id(&mut self) -> OpId {
        self.seq = self.free_seq(self.seq);
        let id = OpId {
            client: self.client,
            seq: self.seq,
        };
        self.seq = self.seq.wrapping_add(1);
        self.seen.insert(id);
        id
    }

    /// Mint identity + causal position for a local edit, apply it, and record
    /// the op on the in-progress transact.
    fn emit(&mut self, target: ElementId, kind: OpKind) {
        let _ = self.emit_stamped(target, kind);
    }

    /// Like [`emit`](Self::emit), returning the stamp minted for the op — so a
    /// caller that creates a stamp-keyed child (an XML sequence child) can derive
    /// its id without re-minting.
    ///
    /// `None` when this replica has no id to mint: either the target's partition is
    /// spent, or an earlier mint in this same intention was refused and latched. The
    /// latch is document-global, so once set it refuses every partition for the rest
    /// of that intention, and it stands until the next one opens so
    /// [`mint_refused`](Self::mint_refused) can report it. The edit emits no op and
    /// changes no state. A caller that derives a child id from the stamp must refuse
    /// alongside, or it would name a child no op creates.
    fn emit_stamped(&mut self, target: ElementId, kind: OpKind) -> Option<Stamp> {
        // The op is stamped from its own partition's clock, so an edit in one zone
        // never advances another's and the op carries which partition it belongs
        // to. The target already exists (a mutation names a materialised
        // container), so its zone — the created child's, for a container-create —
        // resolves now.
        let zone = self.zone_of_op(target, &kind);
        // The mint starts above the replica's **own id space**, not just above the
        // partition clock. A bounded clock is no longer an upper bound over the
        // stamps the document holds: an op stamped under this replica's client id
        // and based above the clamp plants ids the clock does not move past, and a
        // mint that read the clock alone would land straight on them — the write
        // then vanishes as a replay, here and on every peer holding those ids. A
        // stamp names its author, so those planted ids are the only ones a local
        // mint can collide with, and clearing them is exactly the discipline
        // [`free_seq`](Self::free_seq) already applies to the op-seq position.
        if self.mint_refused {
            return None;
        }
        let span = span(&kind);
        let Some(stamp) = mint_position(self.mint_floor(zone), span, self.client) else {
            self.mint_refused = true;
            return None;
        };
        // Reserve the rest of a run's char_ids so the next op sorts after it.
        let last = stamp.lamport + span - 1;
        // The clock a snapshot stores stays inside what
        // [`read_state`](Self::read_state) admits without a bound here: the mint
        // above *refused* rather than clamped if the reservation would pass
        // [`LAMPORT_STATE_CEILING`], so `last` is already under it.
        self.advance_clock(zone, last);
        let id = self.mint_op_id();
        let author = self.client;
        // The record-seam: the inverse is read off the state this op is about to
        // overwrite, so it must be taken before the op lands.
        if self.history.recording() {
            for step in self.inverse(target, &kind, stamp) {
                self.history.push(step);
            }
        }
        self.apply_kind(target, &kind, stamp, author);
        self.pending.push(Op {
            id,
            stamp,
            target,
            kind,
            tx: None,
            zone,
        });
        Some(stamp)
    }

    /// Whether this replica still holds an unspent id in `zone`'s partition —
    /// capacity, read between operations.
    ///
    /// The mint counts on from the higher of the partition clock and this replica's
    /// own id-space high-water, and both stop at [`LAMPORT_STATE_CEILING`]. Honest
    /// traffic reaches it after 2^63 edits; a peer authoring under this replica's
    /// `ClientId` can put it there in one op, which is the residual
    /// [`mint_floor`](Self::mint_floor) carries — and a refused edit is the
    /// fail-closed answer to it, not a re-issued live id.
    ///
    /// A multi-codepoint text insert reserves one id per codepoint, so it can be
    /// refused where a single-id edit is still admitted; this reports the
    /// single-id case. Whether an edit *was* refused is a different question, and
    /// [`mint_refused`](Self::mint_refused) is what answers it: this one says
    /// nothing about the refusal latch, so an intention cut short for want of a
    /// run's length still reads as having room for the single-id edit it does.
    pub fn can_mint(&self, zone: Option<u32>) -> bool {
        mint_position(self.mint_floor(zone), 1, self.client).is_some()
    }

    /// Whether a local edit was refused for want of an id during the intention most
    /// recently opened — the signal a mutation seam reports, since a refused edit
    /// returns the same empty op batch an inert one does.
    ///
    /// True from the refusal until the next intention begins, so the caller that ran
    /// the transact, the atomic group, or the undo replay reads the answer for the
    /// edit it just made. It covers every reason the mint declined: the partition
    /// spent, a run longer than the space that is left, and the latch that carries
    /// the first refusal across the rest of the intention.
    ///
    /// Read it before opening the next intention. The next one clears it, so a
    /// reading taken after a later edit answers for *that* edit — a refusal read
    /// too late reads as none, and a later refusal reads as this edit's.
    ///
    /// Distinct from [`can_mint`](Self::can_mint), which reports capacity rather
    /// than an outcome: a replica with room for a single id answers `true` there
    /// and `true` here both, if the edit it just refused was a run.
    pub fn mint_refused(&self) -> bool {
        self.mint_refused
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
                // A RangedElement is a virtual container the annotation set holds
                // directly under the document, so its parent is the root — a
                // composite payload resolves through the range it hangs off.
                None if self.ranged.contains_key(&cur) => cur = self.root_id(),
                None => return false,
            }
        }
    }

    /// A target is materialised when it names a container this replica holds and
    /// every ancestor up to the root does too — displaced or installed alike.
    fn materialised(&self, target: ElementId) -> bool {
        let mut cur = target;
        // A chain longer than the parent map has revisited a node. The decode
        // refuses a cycle and the move replay re-checks for one, but the live
        // path edits `parents` between those points, and this gate runs over every
        // held op on every drain — so it stops rather than spins.
        for _ in 0..=self.parents.len() {
            if cur == self.root_id() {
                return true;
            }
            if self.displaced_container(cur).is_none() {
                return false;
            }
            match self.parents.get(&cur) {
                Some(&parent) => cur = parent,
                None if self.ranged.contains_key(&cur) => cur = self.root_id(),
                None => return false,
            }
        }
        false
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
        if let Some(x) = self.xml_elements.get(&id) {
            return Some(x.borrow().is_displaced());
        }
        if let Some(f) = self.xml_fragments.get(&id) {
            return Some(f.borrow().is_displaced());
        }
        // A materialised RangedElement is a virtual container holding its composite
        // payload. It reports installed even when tombstoned: delete-wins is a
        // read-layer filter (the payload is hidden from `ranged_payload`), not a
        // reachability break — so a peer edit that raced the delete applies to the
        // retained-hidden payload instead of buffering forever (which would leak
        // and desync the snapshot). An unmaterialised range (create unseen) is
        // absent here, so its payload stays unreachable until the create arrives.
        if self.ranged.contains_key(&id) {
            return Some(false);
        }
        None
    }

    /// The list handle `target` names, displaced or installed — a retained
    /// sequence still takes the edits addressed to it (§Map Slot Safety).
    fn list_at(&self, target: ElementId) -> Option<Rc<RefCell<List>>> {
        self.lists.get(&target).cloned()
    }

    /// The text handle `target` names, displaced or installed.
    fn text_at(&self, target: ElementId) -> Option<Rc<RefCell<Text>>> {
        self.texts.get(&target).cloned()
    }

    /// Route a mutation to its target, recording any displaced composite and
    /// registering any container it creates.
    fn apply_kind(&mut self, target: ElementId, kind: &OpKind, stamp: Stamp, author: ClientId) {
        // The single seam every stamp this replica installs passes through, so it is
        // where the id-space high-water is kept — one place covers `apply`, the
        // buffer drain a decode performs, and the local mint.
        self.record_stamp(stamp, span(kind));
        match kind {
            // Sequence and text ops address a list or text directly.
            OpKind::ListInsert { value, anchor } => {
                if let Some(list) = self.list_at(target) {
                    list.borrow_mut()
                        .insert_at(stamp, Element::Scalar(value.clone()), *anchor);
                }
                return;
            }
            OpKind::ListDelete { id } => {
                // Tombstone into the sequence even when its holding container is
                // displaced: the moved node it deletes still renders under its new
                // parent, so the delete must land to win over the move regardless of
                // the birth list's slot state.
                if let Some(list) = self.lists.get(&target).cloned() {
                    list.borrow_mut().delete_id(*id);
                }
                // A delete of a moved node's placement makes the delete win over a
                // concurrent move, so re-fold to hide every placement of that
                // node. Only a delete that tombstoned a real placement can change
                // the fold, so a plain-list delete skips the O(placements) work.
                if self.placement_index.contains_key(&(target, *id)) {
                    self.refold_moves();
                }
                return;
            }
            OpKind::XmlInsertChild { tag, anchor } => {
                self.insert_xml_child(target, tag.clone(), *anchor, stamp);
                return;
            }
            OpKind::XmlMove { node, anchor } => {
                self.apply_move(target, *node, *anchor, stamp);
                return;
            }
            OpKind::XmlReveal { node, tag } => {
                self.apply_reveal(*node, tag.clone());
                return;
            }
            // RangedElements live in a document-level set, not under `target`.
            OpKind::RangedCreate {
                start,
                end,
                payload,
                name,
            } => {
                let rid = ranged_id(stamp);
                // Idempotent: a replayed create must not reinstall the payload or
                // reset the entry. First sight installs the composite container (so
                // its parent link is present before any op resolves against it),
                // then records the entry.
                if !self.ranged.contains_key(&rid) {
                    let stored = match payload {
                        RangedInit::Scalar(value) => Payload::Scalar {
                            value: value.clone(),
                            stamp,
                        },
                        RangedInit::Composite(kind) => {
                            self.install_payload(rid, *kind);
                            Payload::Composite { kind: *kind }
                        }
                    };
                    self.ranged.insert(
                        rid,
                        RangedEntry {
                            start: *start,
                            end: *end,
                            payload: stored,
                            name: name.clone(),
                            tombstone: false,
                        },
                    );
                }
                return;
            }
            OpKind::RangedSetPayload { id, payload } => {
                // LWW replace, scalar payloads only — a composite is edited through
                // its container, never replaced wholesale.
                if let Some(e) = self.ranged.get_mut(id) {
                    if let Payload::Scalar { value, stamp: last } = &mut e.payload {
                        if stamp > *last {
                            *value = payload.clone();
                            *last = stamp;
                        }
                    }
                }
                return;
            }
            OpKind::RangedDelete { id } => {
                if let Some(e) = self.ranged.get_mut(id) {
                    e.tombstone = true;
                }
                return;
            }
            // ACL tuples live in a document-level set, not under `target`.
            OpKind::AclGrant {
                subject,
                grant,
                effect,
                scope,
                grantor,
            } => {
                let id = acl_id(stamp);
                // Idempotent: a tuple is immutable, so a replayed grant must not
                // reset it. First sight records the entry.
                self.acl.entry(id).or_insert_with(|| AclEntry {
                    subject: subject.clone(),
                    grant: grant.clone(),
                    effect: *effect,
                    scope: scope.clone(),
                    grantor: *grantor,
                    revokers: BTreeSet::new(),
                });
                return;
            }
            OpKind::AclRevoke { id } => {
                // Record the revoke's author (provenance). The tombstone is
                // content-neutral — every revoke lands; whether it carries authority
                // to strip the grant is the evaluator's call.
                if let Some(e) = self.acl.get_mut(id) {
                    e.revokers.insert(author);
                }
                return;
            }
            OpKind::TextInsert { s, anchor } => {
                if let Some(text) = self.text_at(target) {
                    text.borrow_mut().insert_run(stamp, s, *anchor);
                }
                return;
            }
            OpKind::TextDelete { ids } => {
                if let Some(text) = self.text_at(target) {
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
            OpKind::XmlElementCreate { key, tag } => {
                self.create_container(target, key, stamp, Container::XmlElement(tag.clone()));
                return;
            }
            OpKind::XmlFragmentCreate { key } => {
                self.create_container(target, key, stamp, Container::XmlFragment);
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
        let child_id = match &kind {
            Container::XmlElement(tag) => XmlElement::node_id(map_id, key, tag),
            Container::XmlFragment => XmlFragment::node_id(map_id, key),
            _ => ElementId::derive(map_id, key, kind.element_kind()),
        };
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
        // The move log's cycle check walks the created-under relation, so a
        // container keyed into a map has to be in it: without this edge the walk
        // stops at the map half of the tree and a move under a node reachable
        // *through* one of these — an element in the moved node's own attrs, say —
        // reads as acyclic, closes a loop in `parents`, and leaves the replica
        // holding a snapshot `read_state` refuses. One hop, to the map itself: the
        // map carries its own edge onward, and a chain of them is as long as the
        // nesting, where a single hop over the map spans only the first.
        self.moves.set_base(child_id, map_id);
        if let Some(o) = orphan {
            self.orphans.push(o);
        }
    }

    /// Install a child in an XML children List: materialise the child container
    /// (an `XmlElement` when `tag` is present, else a `Text` run), register it,
    /// and insert its handle as a sequence node keyed by the op's stamp. The
    /// child's element id derives from that stamp, so every replica builds the
    /// same child; `insert_at` is idempotent on the stamp, so a replay is inert.
    ///
    /// Inserts into the sequence even when its holding element is displaced: a
    /// displaced parent retains its children, so the child materialises hidden and
    /// a later move can relocate it into a live parent. Losing the child here would
    /// drop a move whose source parent was displaced before the child arrived,
    /// diverging by arrival order.
    fn insert_xml_child(
        &mut self,
        list_id: ElementId,
        tag: Option<Vec<u8>>,
        anchor: Anchor,
        stamp: Stamp,
    ) {
        let Some(list) = self.lists.get(&list_id).cloned() else {
            return;
        };
        let (kind, container) = match tag {
            Some(t) => (ElementKind::XmlElement, Container::XmlElement(t)),
            None => (ElementKind::Text, Container::Text),
        };
        let child_id = xml_child_id(list_id, stamp, kind);
        let element = self.registered_handle(child_id, container);
        self.parents.insert(child_id, list_id);
        // Record the birth placement so a later move can pick the live one of the
        // node's placements — but a document holds one placement per `(list, stamp)`,
        // so the child has to win the key. Two ops can carry one stamp into one list
        // and both pass every gate, since dedup is on `OpId` and an id-space record
        // only bounds an *honest* mint; they need not even name the same child, as
        // `xml_child_id` mixes the kind in, so a tagged and a tagless insert at one
        // stamp derive *different* ids. A losing child is materialised and parented
        // all the same, and holds no position — the placeless state a reveal shell
        // sits in, so it is recorded as one: a move naming it is admissible, which
        // is what makes the loser's later moves land wherever the refusal falls in
        // the arrival order.
        let claim = self.claim_placement(list_id, stamp, child_id);
        match claim {
            Claim::Fresh => list.borrow_mut().insert_at(stamp, element, anchor),
            Claim::Joined => list.borrow_mut().rejoin(stamp, element, anchor),
            Claim::Evicted => list.borrow_mut().reseat(stamp, element, anchor),
            Claim::Refused => {}
        }
        match claim {
            // Awaiting a first placement is what "materialised and unplaced" is
            // recorded as, so a child that already holds one is left placed: this
            // key went to a twin, and a move of the child since then gave it a
            // position elsewhere.
            Claim::Refused => {
                if !self.placements.contains_key(&child_id) {
                    self.revealed_pending.insert(child_id);
                }
            }
            // A join changes hands with no one, so the record the child already
            // holds at this key is the record: a second one would be the duplicate
            // placement `read_state` refuses.
            Claim::Joined => {}
            Claim::Fresh | Claim::Evicted => {
                self.placements
                    .entry(child_id)
                    .or_default()
                    .push(Placement {
                        list: list_id,
                        stamp,
                    });
                self.revealed_pending.remove(&child_id);
            }
        }
        // The created-under parent anchors cycle detection and is the fallback
        // parent (via the move log's base map) when no move governs this node.
        if let Some(&owner) = self.parents.get(&list_id) {
            self.moves.set_base(child_id, owner);
        }
        // Re-seating clears the suppression the fold wrote, and an eviction also
        // withdraws the incumbent's move edge — both leave the fold stale in the
        // node this birth names.
        if matches!(claim, Claim::Joined | Claim::Evicted) {
            self.refold_moves();
        }
    }

    /// Decide who owns the `(list, stamp)` placement key when `node` asks for it,
    /// and record the answer.
    ///
    /// A document holds at most one placement per key — `read_state` refuses a
    /// duplicate — while two ops can carry one stamp into one children list and
    /// both pass every gate, since dedup is on `OpId` and an id-space record only
    /// bounds an *honest* mint. So the key is contended, and which op takes it is a
    /// function of the ops alone, never of which arrived first.
    ///
    /// The rank is **a birth over a move, then the smaller element id**. A birth
    /// outranks because the key is where the born node's id comes from: a birth
    /// that lost would leave a node whose id names a position it does not hold and
    /// which nothing can re-derive, while a move brings an id of its own and
    /// survives losing. Between two of a kind, the id decides — total, and held by
    /// every replica that has either op. Only a claim naming a child this key derives
    /// can take a birth's, and only two ever do: `xml_child_id` mixes the kind in, so
    /// the tagged and the tagless child of a stamp are the whole field — either of
    /// which a move may name, and it is ranked as the child, not as a move.
    ///
    /// The rank orders the *nodes* two claims name, which is the whole question
    /// only while they name two. A move can name exactly the child a birth at the
    /// key derives, and two inserts carrying one tag derive one child between them
    /// — there the key is already the claimant's own, nothing changes hands, and
    /// what is left to settle is the position, which the sequence takes as the meet
    /// of the two ([`List::rejoin`]). A meet is the same whichever arrived first,
    /// where a contest between two claims on one node would have needed to know
    /// what put the incumbent there, and nothing answers that: the move log dedups
    /// on the stamp alone, so a move can hold the key having recorded no edge.
    ///
    /// A claimant that outranks the incumbent evicts it, leaving it exactly as the
    /// opposite arrival order does: holding nothing at this key and its move edge
    /// withdrawn, and — if that was its last placement — awaiting its first, which
    /// is where a refusal leaves a claimant too.
    fn claim_placement(&mut self, list: ElementId, stamp: Stamp, node: ElementId) -> Claim {
        let Some(&held) = self.placement_index.get(&(list, stamp)) else {
            self.placement_index.insert((list, stamp), node);
            return Claim::Fresh;
        };
        if held == node {
            return Claim::Joined;
        }
        // Both sides are ranked by the same question — is this node the child
        // this key derives — so the answer is the same whichever arrives
        // second. Asking it of the *claimant's op kind* instead is not: a move
        // naming a derivable id would be ranked a move on the way in and a
        // birth once it held the key, and the pair would refuse in both
        // directions, leaving the key to whoever arrived first.
        let outranks = match (
            self.born_at(list, stamp, node),
            self.born_at(list, stamp, held),
        ) {
            (true, false) => true,
            (false, true) => false,
            _ => node.as_bytes() < held.as_bytes(),
        };
        if !outranks {
            return Claim::Refused;
        }
        self.evict_placement(list, stamp, held);
        self.placement_index.insert((list, stamp), node);
        Claim::Evicted
    }

    /// Whether `node` is a child a birth at `(list, stamp)` derives — the test that
    /// tells a birth placement from the ones a move gave it.
    ///
    /// Pure in the key: a stamp derives exactly two children, the tagged and the
    /// tagless, so both are tried rather than the one the registry happens to hold.
    /// Reading the registry would let an `XmlReveal` naming a derivable id register
    /// it under the other kind and make an honest birth answer `false` here, which
    /// hands its key to a move.
    fn born_at(&self, list: ElementId, stamp: Stamp, node: ElementId) -> bool {
        [ElementKind::XmlElement, ElementKind::Text]
            .into_iter()
            .any(|kind| xml_child_id(list, stamp, kind) == node)
    }

    /// Take the `(list, stamp)` placement away from `held`, so a node that lost the
    /// key keeps nothing that depended on holding it: the placement record and the
    /// move edge it carried. The sequence slot is the caller's — the winner re-seats
    /// it — and so is the reachability edge of a node that keeps other placements,
    /// which the caller's re-fold derives; only a node left with none has it settled
    /// here, since the fold reads no node it holds no placement for.
    fn evict_placement(&mut self, list: ElementId, stamp: Stamp, held: ElementId) {
        // The move edge goes first, so what remains of the log is what the
        // reachability edge is re-derived from. A birth wrote no edge, and the
        // `(child, parent)` match leaves a second move at this stamp in another
        // list alone.
        if let Some(&owner) = self.parents.get(&list) {
            self.moves.remove(stamp, held, owner);
        }
        // The index names the holder of a record, so the record is there: every
        // writer of one writes the other, and every rebuild derives the index from
        // the records. Returning here would leave the node with neither a placement
        // nor the pending mark below — the state that refuses every later move of it.
        debug_assert!(
            self.placements.contains_key(&held),
            "a placement key names a node holding no placement"
        );
        let Some(places) = self.placements.get_mut(&held) else {
            return;
        };
        places.retain(|p| !(p.list == list && p.stamp == stamp));
        if !places.is_empty() {
            return;
        }
        self.placements.remove(&held);
        // A move is gated on the node holding a placement or awaiting its first, so
        // a node left with none goes back to awaiting one rather than being
        // stranded with neither, which would refuse every later move of it forever.
        // That is the state the opposite arrival order leaves it in either way — a
        // shell whose move is refused stays pending, and a birth whose key is
        // refused is recorded pending by `insert_xml_child`.
        self.revealed_pending.insert(held);
        // Reachability follows the move log, exactly as `refold_moves` derives it
        // for a placed node: a node created under a parent keeps that parent's
        // children list whether or not its birth took this key, and one with no
        // edge at all — a shell — reaches nothing.
        match self.moves.parent_of(held) {
            Some(owner) => {
                self.parents.insert(held, XmlElement::children_id(owner));
            }
            None => {
                self.parents.remove(&held);
            }
        }
    }

    /// Relocate `node` under the destination children `dest_list` at `anchor`.
    /// Inserts a placement referencing the node's stable element id, records the
    /// move in the lamport-ordered log, then re-folds so exactly one placement of
    /// the node renders — Kleppmann convergence, a cycle move left inert.
    fn apply_move(&mut self, dest_list: ElementId, node: ElementId, anchor: Anchor, stamp: Stamp) {
        // Only a node that lives in a children sequence is movable: it must already
        // hold a placement — or be a reveal shell awaiting its first placement (a
        // node born in a subtree this reader could not read, revealed here as it
        // moves into one it can). A node created straight into a map slot (a document
        // root) is keyed, not positioned, so a move of it is a no-op — and the same
        // no-op on every replica, since the local emit path reaches here directly,
        // bypassing the `ready` gate remotes apply.
        let revealing = self.revealed_pending.contains(&node);
        if !self.placements.contains_key(&node) && !revealing {
            return;
        }
        // Record into the destination sequence even when its holding container is
        // displaced: the move must land in the lamport-ordered log regardless of
        // the destination's transient slot state, or the log — and so the folded
        // tree — would differ by arrival order. A move onto a displaced parent
        // renders hidden; the fold re-derives visibility from the move-set.
        let Some(list) = self.lists.get(&dest_list).cloned() else {
            return;
        };
        let Some(&owner) = self.parents.get(&dest_list) else {
            return;
        };
        let Some(element) = self.node_element(node) else {
            return;
        };
        // One placement per `(list, stamp)`, for the reason `insert_xml_child` gives.
        // Here the colliding op need not name the same node — a move takes its node
        // from the payload — so a move that loses the key is refused **whole**,
        // before any mutation. Two things ride on refusing that early.
        //
        // The node must not vanish. A move whose edge is logged while its placement
        // is not makes this list the node's effective parent with nothing of the
        // node in it, and `refold_moves` then finds no live placement and suppresses
        // every placement it has anywhere. So the edge is recorded only once the key
        // is held.
        //
        // And a shell must stay movable. Both `ready` and this function gate a move
        // on the node holding a placement or awaiting its first, so clearing the
        // pending mark without storing a placement would leave it with neither and
        // refuse every later move of it. The mark is cleared below, once the
        // placement is stored; an eviction restores it for the same reason.
        let claim = self.claim_placement(dest_list, stamp, node);
        match claim {
            Claim::Fresh => list.borrow_mut().insert_at(stamp, element, anchor),
            Claim::Joined => list.borrow_mut().rejoin(stamp, element, anchor),
            Claim::Evicted => list.borrow_mut().reseat(stamp, element, anchor),
            Claim::Refused => return,
        }
        // A join is the node's own key, already recorded — a second record would be
        // the duplicate placement `read_state` refuses.
        if claim != Claim::Joined {
            self.placements.entry(node).or_default().push(Placement {
                list: dest_list,
                stamp,
            });
        }
        self.moves.apply(stamp, node, owner);
        // The shell is now placed — an ordinary moved node from here on.
        self.revealed_pending.remove(&node);
        self.refold_moves();
    }

    /// Materialize a movable node's shell — its identity and current `tag` (an
    /// `XmlElement` for `Some`, a `Text` run for `None`) — with no placement, and
    /// record it as awaiting the move that will place it. The op-stream analogue of
    /// the snapshot projection keeping a born-denied node at its readable current
    /// position: it registers the node's attrs Map and children List (by derived id)
    /// so the node's readable content ops resolve and drain onto it, then the readable
    /// move lands its first placement. Idempotent — a node already materialized (its
    /// real create arrived, or a duplicate reveal) is left as it is.
    fn apply_reveal(&mut self, node: ElementId, tag: Option<Vec<u8>>) {
        if self.node_element(node).is_some() {
            return;
        }
        let container = match tag {
            Some(t) => Container::XmlElement(t),
            None => Container::Text,
        };
        self.registered_handle(node, container);
        self.revealed_pending.insert(node);
    }

    /// Re-derive, for every movable node, which of its placements renders: the
    /// highest-stamped placement in the node's effective-parent list (`parent_of`
    /// falls back to the created-under parent, so a never-moved node resolves to
    /// its birth list). Every other placement is suppressed, and reachability is
    /// re-pointed at the live placement's list so a moved subtree resolves through
    /// its new parent. A node whose placement was tombstoned by a `ListDelete` is
    /// deleted — every placement is hidden, so a concurrent delete wins over a
    /// concurrent move rather than resurrecting the node under the new parent. It
    /// is re-pointed all the same: rendering nowhere is not the same as belonging
    /// nowhere, and leaving the edge at whatever the last fold happened to write
    /// would make it a function of arrival order rather than of the move log.
    ///
    /// This re-folds every placement on each move. Correct but not minimal: one
    /// move's undo-and-replay can shift several nodes' effective parents, so a
    /// scoped refold would need the move log to report exactly which nodes moved.
    fn refold_moves(&mut self) {
        let mut suppress: Vec<(ElementId, Stamp, bool)> = Vec::new();
        let mut reparent: Vec<(ElementId, ElementId)> = Vec::new();
        for (node, places) in &self.placements {
            let Some(owner) = self.moves.parent_of(*node) else {
                continue;
            };
            let eff_list = XmlElement::children_id(owner);
            let deleted = places
                .iter()
                .any(|p| self.is_tombstoned_node(p.list, p.stamp));
            let live = if deleted {
                None
            } else {
                places
                    .iter()
                    .filter(|p| p.list == eff_list)
                    .map(|p| p.stamp)
                    .max()
            };
            for p in places {
                let away = deleted || !(p.list == eff_list && Some(p.stamp) == live);
                suppress.push((p.list, p.stamp, away));
            }
            reparent.push((*node, eff_list));
        }
        for (list, stamp, away) in suppress {
            if let Some(list) = self.lists.get(&list) {
                list.borrow_mut().set_moved_away(stamp, away);
            }
        }
        for (node, eff_list) in reparent {
            self.parents.insert(node, eff_list);
        }
    }

    /// Whether the placement `(list, stamp)` has been tombstoned by a delete.
    fn is_tombstoned_node(&self, list: ElementId, stamp: Stamp) -> bool {
        self.lists
            .get(&list)
            .is_some_and(|l| l.borrow().is_tombstoned(stamp))
    }

    /// The registered handle for a movable node — an `XmlElement` or a `Text` run
    /// — wrapped as an Element to place in a children list.
    fn node_element(&self, node: ElementId) -> Option<Element> {
        if let Some(x) = self.xml_elements.get(&node) {
            return Some(Element::XmlElement(Rc::clone(x)));
        }
        if let Some(t) = self.texts.get(&node) {
            return Some(Element::Text(Rc::clone(t)));
        }
        None
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
        let id = ElementId::derive(map_id, key, ElementKind::Counter);
        let counter = match self.counters.get(&id) {
            Some(c) => Rc::clone(c),
            None => {
                // A counter installed straight through the Map API isn't in the
                // registry yet; adopt its tally rather than shadow it with a
                // fresh zero.
                let counter = match map.borrow().get(key) {
                    Some(Element::Counter(live)) if live.borrow().id() == id => live,
                    _ => Rc::new(RefCell::new(Counter::new(id))),
                };
                self.counters.insert(id, Rc::clone(&counter));
                counter
            }
        };
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

    /// Install a RangedElement's composite payload: materialise + register the
    /// container at its derived id and link it to the RangedElement, so an op
    /// targeting the payload resolves (reachability walks payload → range → root)
    /// and it rides the by-id registry through a snapshot. A fresh container is
    /// installed, not displaced — the payload owns its slot outright, so there is
    /// no LWW contention to lose.
    fn install_payload(&mut self, ranged: ElementId, kind: ElementKind) {
        let container = match kind {
            ElementKind::Map => Container::Map,
            ElementKind::List => Container::List,
            ElementKind::Text => Container::Text,
            // Only the three sequence/record containers are valid payloads; a
            // non-container kind is rejected at decode, never reaching here.
            _ => return,
        };
        let pid = payload_id(ranged, kind);
        self.registered_handle(pid, container);
        self.parents.insert(pid, ranged);
        self.moves.set_base(pid, ranged);
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
            Container::XmlElement(tag) => {
                let handle = Rc::clone(
                    self.xml_elements
                        .entry(id)
                        .or_insert_with(|| Rc::new(RefCell::new(XmlElement::new(id, tag)))),
                );
                // The node's attrs Map and children List are containers in their
                // own right — register them so ops targeting them resolve, and
                // link them to the node so reachability walks up through it.
                let (attrs, children) = {
                    let h = handle.borrow();
                    (h.attrs(), h.children())
                };
                let attrs_id = XmlElement::attrs_id(id);
                let children_id = XmlElement::children_id(id);
                self.maps.entry(attrs_id).or_insert(attrs);
                self.lists.entry(children_id).or_insert(children);
                self.parents.insert(attrs_id, id);
                self.parents.insert(children_id, id);
                // Both are also where the created-under relation runs on through
                // the node, so the cycle check reaches whatever is keyed into the
                // attrs map from the node that owns it.
                self.moves.set_base(attrs_id, id);
                self.moves.set_base(children_id, id);
                Element::XmlElement(handle)
            }
            Container::XmlFragment => {
                let handle = Rc::clone(
                    self.xml_fragments
                        .entry(id)
                        .or_insert_with(|| Rc::new(RefCell::new(XmlFragment::new(id)))),
                );
                let children = handle.borrow().children();
                let children_id = XmlFragment::children_id(id);
                self.lists.entry(children_id).or_insert(children);
                self.parents.insert(children_id, id);
                self.moves.set_base(children_id, id);
                Element::XmlFragment(handle)
            }
        }
    }

    // --- undo record-seam ---
    //
    // Every op this replica emits passes through `emit_stamped`, which asks
    // `inverse` what would put the state back. That is the whole seam: an edit is
    // recorded because it was *emitted*, not because it was made through some
    // particular helper, so a handle-graph SDK, the path façade and a raw cursor
    // all record identically, on an offline replica and on a channel's replica
    // alike. Replaying an intention emits ordinary forward ops, which record
    // their own inverses — so the mirror is always derived from live state.

    /// Record emitted edits under `origin` from now on. Recording is off until
    /// this is called; the origin is the tag [`undo`](Self::undo) selects by, so
    /// two managers over one document (a user's and a subtree-scoped one) keep
    /// separate histories.
    pub fn set_undo_origin(&mut self, origin: &[u8]) {
        self.history.track(origin);
    }

    /// Stop recording emitted edits. What was already recorded stays undoable.
    pub fn clear_undo_origin(&mut self) {
        self.history.untrack();
    }

    /// The origin edits are recording under, or `None` when recording is off.
    pub fn undo_origin(&self) -> Option<&[u8]> {
        self.history.origin()
    }

    /// Open an explicit intention: every edit until the matching
    /// [`end_intention`](Self::end_intention) records as one undo step, however
    /// many transacts it spans. Nests.
    pub fn begin_intention(&mut self) {
        if !self.recording_intention() {
            self.mint_refused = false;
        }
        self.history.open_group();
    }

    /// Close the intention opened by [`begin_intention`](Self::begin_intention).
    /// The outermost close records the step.
    pub fn end_intention(&mut self) {
        if self.history.close_group() && self.atomic.is_none() {
            self.history.close(false);
        }
    }

    /// Whether `origin` has a recorded intention to undo.
    pub fn can_undo(&self, origin: &[u8]) -> bool {
        self.history.can_undo(origin)
    }

    /// Whether `origin` has an undone intention to redo.
    pub fn can_redo(&self, origin: &[u8]) -> bool {
        self.history.can_redo(origin)
    }

    /// Revert `origin`'s most recent intention — skipping any another origin
    /// interleaved — and return the ordinary forward ops that did it, for the
    /// caller to broadcast. The intention becomes redoable. `None` when `origin`
    /// has nothing to undo, or while an intention is open — an atomic
    /// transaction or an explicit [`begin_intention`](Self::begin_intention)
    /// group: the undo's own edits would be recorded into that open intention,
    /// silently taking the group's edits onto the wrong stack.
    pub fn undo(&mut self, origin: &[u8]) -> Option<Vec<Op>> {
        if self.recording_intention() {
            return None;
        }
        let intention = self.history.take(origin, Landing::Undo)?;
        Some(self.replay(intention, Landing::Redo))
    }

    /// Replay `origin`'s most recently undone intention, returning the ops to
    /// broadcast. It becomes undoable again. `None` under the same conditions as
    /// [`undo`](Self::undo).
    pub fn redo(&mut self, origin: &[u8]) -> Option<Vec<Op>> {
        if self.recording_intention() {
            return None;
        }
        let intention = self.history.take(origin, Landing::Redo)?;
        Some(self.replay(intention, Landing::Undo))
    }

    /// Whether an intention is open, so a replay would record into it.
    fn recording_intention(&self) -> bool {
        self.atomic.is_some() || self.history.grouped()
    }

    /// Emit an intention's inverses — last edit undone first — and return their
    /// ops. The emitted ops record their own inverses, which close as the mirror
    /// intention on `landing`. An atomic intention replays inside one open
    /// transaction, which closes as one group per zone partition its inverses fall
    /// in, so the mirror is atomic on the terms the forward commit was.
    fn replay(&mut self, intention: Intention, landing: Landing) -> Vec<Op> {
        let Intention {
            origin,
            steps,
            atomic,
        } = intention;
        // A replay is a fresh intention, so it gets a fresh answer from the mint —
        // a run refused for its length must not go on refusing the single-id edits
        // a later undo is made of.
        self.mint_refused = false;
        let saved = self.history.begin_replay(&origin, landing);
        if atomic {
            self.begin_atomic();
        }
        self.pending.clear();
        let plan = self.plan(steps);
        // The container-restoring ops this intention holds, by the element each
        // makes live. A step whose target is displaced when its turn comes is
        // preceded by a *copy* of the op that puts the container back, rather
        // than by hoisting that op out of its place: re-creating a container is
        // idempotent, while moving a slot write past another inverts which of
        // them the slot ends up holding.
        let installers: HashMap<ElementId, (ElementId, OpKind)> = plan
            .iter()
            .filter_map(|step| match step {
                Inverse::Op { target, kind } => {
                    self.installs(step).map(|id| (id, (*target, kind.clone())))
                }
                _ => None,
            })
            .collect();
        for step in plan {
            // The element the step will actually address — a revival may have
            // replaced the one it names — so the reachability decision and the
            // emission are about the same thing.
            let target = self.history.current_element(self.step_target(&step));
            if !self.resolvable(target) {
                self.reinstate(target, &installers);
            }
            // Emitted whether or not the revival took: a step whose container is
            // displaced lands in the retained one, here and at every peer alike
            // (ARCHITECTURE §Map Slot Safety), so dropping it would lose the
            // intention on the one replica that holds it. A step naming a container
            // no replica has materialised is what cannot land, and nothing revives.
            if self.materialised(target) {
                self.emit_inverse(step);
            }
        }
        // A revived container can be what buffered remote ops were waiting on.
        self.drain_buffer();
        let ops = std::mem::take(&mut self.pending);
        let ops = if atomic {
            self.atomic.get_or_insert_with(Vec::new).extend(ops);
            self.commit_atomic()
        } else {
            self.history.close(false);
            ops
        };
        self.history.end_replay(saved);
        self.history.prune();
        ops
    }

    /// Emit one inverse action, following any revival that has since replaced the
    /// sequence ids it names, and recording the substitutions its own revival
    /// makes for the intentions still stacked beneath.
    fn emit_inverse(&mut self, step: Inverse) {
        match step {
            Inverse::Op { target, kind } => {
                // The target itself can be a container of something a revival
                // replaced — a revived annotation's payload, a revived node's
                // attrs or children — so it follows the element map too.
                let target = self.history.current_element(target);
                let kind = self.follow_revivals(target, kind);
                self.emit(target, kind)
            }
            Inverse::Regrant {
                target,
                subject,
                grant,
                effect,
                scope,
                grantor,
                was,
            } => {
                // A refused mint emits nothing, so there is no new element for the
                // steps stacked beneath to be re-pointed at.
                let Some(stamp) = self.emit_stamped(
                    target,
                    OpKind::AclGrant {
                        subject,
                        grant,
                        effect,
                        scope,
                        grantor,
                    },
                ) else {
                    return;
                };
                self.history.substitute_element(was, acl_id(stamp));
            }
            Inverse::ReviveItem {
                list,
                anchor,
                value,
                was,
            } => {
                let Some(now) = self.emit_stamped(list, OpKind::ListInsert { value, anchor })
                else {
                    return;
                };
                self.history.substitute(list, was, now);
            }
            Inverse::ReviveRun {
                text,
                anchor,
                s,
                was,
            } => {
                let Some(now) = self.emit_stamped(text, OpKind::TextInsert { s, anchor }) else {
                    return;
                };
                for (i, old) in was.into_iter().enumerate() {
                    self.history.substitute(text, old, now.run_member(i as u64));
                }
            }
            Inverse::ReviveNode {
                list,
                anchor,
                node,
                was,
                was_node,
            } => {
                if let Some(now) = self.revive_node(list, anchor, node) {
                    self.history.substitute(list, was, now);
                    if let Some(element) = self.node_at(list, now) {
                        self.history.substitute_element(was_node, element);
                        // A stacked step may target the node's attrs or children
                        // rather than the node, and those ids derive from it.
                        self.history.substitute_element(
                            XmlElement::attrs_id(was_node),
                            XmlElement::attrs_id(element),
                        );
                        self.history.substitute_element(
                            XmlElement::children_id(was_node),
                            XmlElement::children_id(element),
                        );
                    }
                }
            }
            Inverse::Ranged {
                start,
                end,
                name,
                payload,
                was,
            } => {
                if let Some(now) = self.revive_ranged(start, end, name, payload) {
                    self.history.substitute_element(was, now);
                    // A composite payload is a container in its own right, and a
                    // stacked step edits it by that derived id.
                    for kind in [ElementKind::Map, ElementKind::List, ElementKind::Text] {
                        self.history
                            .substitute_element(payload_id(was, kind), payload_id(now, kind));
                    }
                }
            }
        }
    }

    /// The element id of the node `stamp` just minted in the children list
    /// `list` — an XML element or a text run.
    fn node_at(&self, list: ElementId, stamp: Stamp) -> Option<ElementId> {
        [ElementKind::XmlElement, ElementKind::Text]
            .into_iter()
            .map(|kind| xml_child_id(list, stamp, kind))
            .find(|id| self.node_element(*id).is_some())
    }

    /// Re-point a sequence delete at the ids its items came back as. A tombstone
    /// is terminal, so an earlier undo revived them under fresh ids; an intention
    /// recorded before that revival still names the originals.
    fn follow_revivals(&self, seq: ElementId, kind: OpKind) -> OpKind {
        // An anchor names a node of the sequence it places into, so a revived
        // anchor has to be followed too or the step lands beside the tombstone
        // instead of beside the item that replaced it.
        let anchored = |anchor: Anchor| Anchor {
            parent: anchor.parent.map(|id| self.history.current(seq, id)),
            side: anchor.side,
        };
        match kind {
            OpKind::ListDelete { id } => OpKind::ListDelete {
                id: self.history.current(seq, id),
            },
            OpKind::ListInsert { value, anchor } => OpKind::ListInsert {
                value,
                anchor: anchored(anchor),
            },
            OpKind::TextInsert { s, anchor } => OpKind::TextInsert {
                s,
                anchor: anchored(anchor),
            },
            OpKind::XmlInsertChild { tag, anchor } => OpKind::XmlInsertChild {
                tag,
                anchor: anchored(anchor),
            },
            OpKind::TextDelete { ids } => OpKind::TextDelete {
                ids: ids
                    .into_iter()
                    .map(|id| self.history.current(seq, id))
                    .collect(),
            },
            // These name an element outright, so they follow the element map
            // rather than the sequence one: a revived node, annotation, or ACL
            // tuple carries a new id its own revival recorded.
            OpKind::XmlMove { node, anchor } => OpKind::XmlMove {
                node: self.history.current_element(node),
                anchor: anchored(anchor),
            },
            OpKind::RangedSetPayload { id, payload } => OpKind::RangedSetPayload {
                id: self.history.current_element(id),
                payload,
            },
            OpKind::RangedDelete { id } => OpKind::RangedDelete {
                id: self.history.current_element(id),
            },
            OpKind::AclRevoke { id } => OpKind::AclRevoke {
                id: self.history.current_element(id),
            },
            other => other,
        }
    }

    /// Order an intention's inverses for replay.
    ///
    /// The base order is the reverse of the edits — the last thing done is the
    /// first thing undone — and it is *kept*: two writes to one map slot are
    /// ordered by that reversal, and moving either past the other inverts which
    /// of them the slot ends up holding. A container a step needs is re-created
    /// on demand instead (see [`replay`](Self::replay)).
    ///
    /// The one thing dropped is a step addressing inside a container another
    /// step *terminally removes* — a slot emptied by a delete, or a sequence node
    /// tombstoned. Those leave the tree whole and come back whole, so tearing
    /// their interior down first is not merely redundant: it would leave the
    /// mirror an empty shell to snapshot, and the redo would lose everything the
    /// intention put inside. A vacated map slot is not that — the container is
    /// retained holding what the intention put there, and anything that
    /// re-installs the slot exposes it — so those steps still run.
    fn plan(&self, steps: Vec<Inverse>) -> Vec<Inverse> {
        let steps: Vec<Inverse> = steps.into_iter().rev().collect();
        let removed: Vec<ElementId> = steps.iter().filter_map(|s| self.removes(s)).collect();
        steps
            .into_iter()
            .filter(|s| !self.under_any(self.step_target(s), &removed))
            .collect()
    }

    /// Re-create the containers `target` sits under, outermost first, from the
    /// restoring ops this intention already holds. Inert when it holds none — the
    /// step is then dropped rather than emitted onto nothing.
    fn reinstate(
        &mut self,
        target: ElementId,
        installers: &HashMap<ElementId, (ElementId, OpKind)>,
    ) {
        let mut chain: Vec<(ElementId, OpKind)> = Vec::new();
        let mut cur = Some(target);
        // Bounded like the other parent walks, so a corrupt link stops the walk
        // rather than spinning.
        for _ in 0..=self.parents.len() {
            let Some(id) = cur else { break };
            if let Some((at, kind)) = installers.get(&id) {
                chain.push((*at, kind.clone()));
            }
            if id == self.root_id() {
                break;
            }
            cur = self.parents.get(&id).copied();
        }
        for (at, kind) in chain.into_iter().rev() {
            // The copies answer to the same rule as the steps: an op no replica can
            // resolve is not emitted, because a peer would buffer it against a
            // create that is not coming. Once one link of the chain cannot land,
            // nothing below it can either.
            if !self.materialised(at) {
                return;
            }
            self.emit(at, kind);
        }
    }

    /// The element a step addresses.
    fn step_target(&self, step: &Inverse) -> ElementId {
        match step {
            Inverse::Op { target, .. } => *target,
            Inverse::ReviveItem { list, .. } | Inverse::ReviveNode { list, .. } => *list,
            Inverse::ReviveRun { text, .. } => *text,
            Inverse::Regrant { target, .. } => *target,
            // An annotation hangs off the document, which is always reachable.
            Inverse::Ranged { .. } => self.root_id(),
        }
    }

    /// The element a step puts in a map slot, if any. The child is re-derived
    /// from the key, so this is a function of the op alone — not of what the slot
    /// happens to hold, which is what the step *displaces*.
    fn installs(&self, step: &Inverse) -> Option<ElementId> {
        let Inverse::Op { target, kind } = step else {
            return None;
        };
        let (key, kind) = match kind {
            OpKind::MapCreate { key } => (key, ElementKind::Map),
            OpKind::ListCreate { key } => (key, ElementKind::List),
            OpKind::TextCreate { key } => (key, ElementKind::Text),
            OpKind::XmlElementCreate { key, tag } => {
                return Some(XmlElement::node_id(*target, key, tag))
            }
            OpKind::XmlFragmentCreate { key } => return Some(XmlFragment::node_id(*target, key)),
            // A counter is a leaf: nothing is ever reached *through* it, so it can
            // never be a link in the chain a step's target hangs off. Listing it
            // would only let two deltas on one key collide over the entry.
            _ => return None,
        };
        Some(ElementId::derive(*target, key, kind))
    }

    /// The container a step puts permanently out of reach, if any — the one
    /// whose subtree the step therefore subsumes.
    ///
    /// Only a tombstoned **sequence node** qualifies. A delete there is terminal:
    /// the move fold hides every placement of a deleted node, so nothing can ever
    /// render it again, and its containers — retained by id like all of them —
    /// become unreachable for good. Undoing the node's contents before deleting
    /// it would therefore lose them outright, since the revival's snapshot is
    /// taken from what is left.
    ///
    /// A **map slot** is not terminal, however it is vacated: emptied by a
    /// delete or taken by another value, the container is retained holding what
    /// the intention put in it, and the next thing to install that slot brings it
    /// back — exposing exactly the edits a subsumption would have discarded. So
    /// those steps always run.
    fn removes(&self, step: &Inverse) -> Option<ElementId> {
        let Inverse::Op {
            target,
            kind: OpKind::ListDelete { id },
        } = step
        else {
            return None;
        };
        let held = self.lists.get(target)?.borrow().node_value(*id)?;
        held.is_container().then(|| held.id())
    }

    /// Whether `id` is one of `roots` or sits under one — so a step addressing it
    /// is subsumed by the step that removes that root.
    fn under_any(&self, id: ElementId, roots: &[ElementId]) -> bool {
        if roots.is_empty() {
            return false;
        }
        let mut cur = id;
        // A chain longer than the parent map has revisited a node, which only a
        // corrupt parent link could produce; stop rather than spin.
        for _ in 0..=self.parents.len() {
            if roots.contains(&cur) {
                return true;
            }
            if cur == self.root_id() {
                return false;
            }
            match self.parents.get(&cur) {
                Some(&parent) => cur = parent,
                None => return false,
            }
        }
        false
    }

    /// What would put the state back after `kind` lands on `target` with
    /// `stamp` — read against the state as it stands *now*, before the op
    /// applies. Empty for an op with nothing to restore; a counter delta needs
    /// two steps, since it both moves the tally and re-wins the slot.
    fn inverse(&self, target: ElementId, kind: &OpKind, stamp: Stamp) -> Vec<Inverse> {
        match kind {
            // A counter delta re-wins its slot on the way past, so cancelling the
            // tally is only half the inverse — whatever the delta displaced has to
            // come back too. The slot step is recorded *first* so the reversed
            // replay cancels the tally and then restores the slot; the other way
            // round the counter would re-take the slot it had just given up.
            OpKind::CounterInc { key, amount } | OpKind::CounterDec { key, amount } => {
                let cancel = if matches!(kind, OpKind::CounterInc { .. }) {
                    OpKind::CounterDec {
                        key: key.clone(),
                        amount: *amount,
                    }
                } else {
                    OpKind::CounterInc {
                        key: key.clone(),
                        amount: *amount,
                    }
                };
                let mut steps = Vec::new();
                // A slot the counter already held needs no restoring — that step
                // would be a zero-delta op re-winning a slot it never left.
                let counter_id = ElementId::derive(target, key, ElementKind::Counter);
                // A bare scalar has no id at all — asking for one panics — so the
                // "already this counter" test has to exclude it before comparing.
                let held = self
                    .maps
                    .get(&target)
                    .and_then(|m| m.borrow().get(key))
                    .is_some_and(|e| !matches!(e, Element::Scalar(_)) && e.id() == counter_id);
                if !held {
                    steps.extend(self.slot_inverse(target, key));
                }
                steps.push(Inverse::Op {
                    target,
                    kind: cancel,
                });
                steps
            }
            other => self
                .single_inverse(target, other, stamp)
                .into_iter()
                .collect(),
        }
    }

    /// The inverse of every op whose undo is a single action.
    fn single_inverse(&self, target: ElementId, kind: &OpKind, stamp: Stamp) -> Option<Inverse> {
        let at = |target, kind| Some(Inverse::Op { target, kind });
        match kind {
            // Every slot mutation — a value, a leaf, or a container install —
            // inverts to whatever the slot held before it.
            OpKind::RegisterSet { key, .. }
            | OpKind::MapSet { key, .. }
            | OpKind::MapDelete { key }
            | OpKind::MapCreate { key }
            | OpKind::ListCreate { key }
            | OpKind::TextCreate { key }
            | OpKind::XmlElementCreate { key, .. }
            | OpKind::XmlFragmentCreate { key } => self.slot_inverse(target, key),
            OpKind::CounterInc { .. } | OpKind::CounterDec { .. } => {
                unreachable!("a counter delta inverts to two steps, routed above")
            }
            // A sequence insert is undone by tombstoning exactly the node it
            // mints, whose id is the op's own stamp.
            OpKind::ListInsert { .. } | OpKind::XmlInsertChild { .. } => {
                at(target, OpKind::ListDelete { id: stamp })
            }
            OpKind::ListDelete { id } => self.list_delete_inverse(target, *id),
            // A run takes one char_id per codepoint from the op's stamp, so the
            // ids to tombstone are known without reading the text back. An empty
            // run mints nothing and has nothing to undo.
            OpKind::TextInsert { s, .. } => {
                let count = s.chars().count() as u64;
                if count == 0 {
                    return None;
                }
                let ids = (0..count).map(|i| stamp.run_member(i)).collect();
                at(target, OpKind::TextDelete { ids })
            }
            OpKind::TextDelete { ids } => self.text_delete_inverse(target, ids),
            OpKind::XmlMove { node, .. } => self.move_inverse(*node),
            // A reveal is synthesized by the server at redaction time, never
            // authored here, so it never reaches the seam — and it installs only
            // a shell, so there is nothing to restore.
            OpKind::XmlReveal { .. } => None,
            OpKind::RangedCreate { .. } => at(
                target,
                OpKind::RangedDelete {
                    id: ranged_id(stamp),
                },
            ),
            OpKind::RangedSetPayload { id, .. } => {
                let prior = match &self.ranged.get(id)?.payload {
                    Payload::Scalar { value, .. } => value.clone(),
                    // A composite payload is edited through its container, so
                    // this op is inert against one and needs no inverse.
                    Payload::Composite { .. } => return None,
                };
                at(
                    target,
                    OpKind::RangedSetPayload {
                        id: *id,
                        payload: prior,
                    },
                )
            }
            OpKind::RangedDelete { id } => self.ranged_inverse(*id),
            OpKind::AclGrant { .. } => at(target, OpKind::AclRevoke { id: acl_id(stamp) }),
            OpKind::AclRevoke { id } => {
                let e = self.acl.get(id)?;
                Some(Inverse::Regrant {
                    target,
                    subject: e.subject.clone(),
                    grant: e.grant.clone(),
                    effect: e.effect,
                    scope: e.scope.clone(),
                    grantor: e.grantor,
                    was: *id,
                })
            }
        }
    }

    /// Re-install whatever `key` holds in `map_id` right now. A container is
    /// restored by re-creating it at the same key: its handle is retained by id,
    /// so the same logical element comes back with its content intact. An empty
    /// or tombstoned slot inverts to a delete.
    fn slot_inverse(&self, map_id: ElementId, key: &[u8]) -> Option<Inverse> {
        let map = self.maps.get(&map_id)?;
        let key = key.to_vec();
        let kind = match map.borrow().get(&key) {
            None => OpKind::MapDelete { key },
            Some(Element::Scalar(value)) => OpKind::MapSet { key, value },
            Some(Element::Register(r)) => OpKind::RegisterSet {
                key,
                value: r.borrow().read().clone(),
            },
            // The tally lives in the registry keyed by the slot's derived id, so
            // re-winning the slot with a zero delta restores the counter whole.
            Some(Element::Counter(_)) => OpKind::CounterInc { key, amount: 0 },
            Some(Element::Map(_)) => OpKind::MapCreate { key },
            Some(Element::List(_)) => OpKind::ListCreate { key },
            Some(Element::Text(_)) => OpKind::TextCreate { key },
            Some(Element::XmlElement(x)) => OpKind::XmlElementCreate {
                tag: x.borrow().tag().to_vec(),
                key,
            },
            Some(Element::XmlFragment(_)) => OpKind::XmlFragmentCreate { key },
        };
        Some(Inverse::Op {
            target: map_id,
            kind,
        })
    }

    /// Revive the node about to be tombstoned, right where it sits: the revival
    /// anchors as the *left* child of the node it replaces, which renders
    /// immediately before it — the position the tombstone still holds, however
    /// the sequence has shifted since. A scalar comes back as a plain insert; a
    /// composite sequence node — an XML child — is rebuilt from a snapshot, since
    /// a tombstone drops the value it held.
    fn list_delete_inverse(&self, list_id: ElementId, id: Stamp) -> Option<Inverse> {
        let list = self.lists.get(&list_id)?;
        // By id, not by live index: a node suppressed by a tree move renders
        // elsewhere but is still what this placement holds, and deleting the
        // placement hides the node everywhere (delete wins over move) — so the
        // inverse has to be able to bring it back.
        let value = list.borrow().node_value(id)?;
        let anchor = Anchor {
            parent: Some(id),
            side: Side::Left,
        };
        match value {
            Element::Scalar(value) => Some(Inverse::ReviveItem {
                list: list_id,
                anchor,
                value,
                was: id,
            }),
            // A movable node is revived only when the delete is what takes it out
            // of view. Deleting one of its placements hides *every* placement
            // (delete wins over move), so a node rendering elsewhere still needs
            // an inverse — but a node already hidden loses nothing here, and
            // reviving it would put a duplicate beside whatever restores the
            // original. No edit surface can delete an already-hidden placement;
            // a replay can, by emitting a recorded delete against a node a later
            // step has since hidden, which is where the duplicate came from.
            other if self.renders(other.id()) => Some(Inverse::ReviveNode {
                list: list_id,
                anchor,
                node: self.snapshot(&other, 0)?,
                was: id,
                was_node: other.id(),
            }),
            _ => None,
        }
    }

    /// Whether a movable node currently renders — one of its placements is live
    /// in a reachable sequence.
    fn renders(&self, node: ElementId) -> bool {
        self.placements.get(&node).is_some_and(|places| {
            places.iter().any(|p| {
                self.resolvable(p.list)
                    && self
                        .lists
                        .get(&p.list)
                        .is_some_and(|l| l.borrow().live_index(p.stamp).is_some())
            })
        })
    }

    /// Revive the codepoints about to be tombstoned, anchored as the left child
    /// of the first of them — the left edge of the run, which is where the
    /// revived text belongs however the sequence has shifted since.
    fn text_delete_inverse(&self, text_id: ElementId, ids: &[Stamp]) -> Option<Inverse> {
        let text = self.texts.get(&text_id)?;
        let t = text.borrow();
        let chars: Vec<char> = t.as_string().chars().collect();
        let mut live: Vec<(usize, Stamp)> = ids
            .iter()
            .filter_map(|id| t.live_index(*id).map(|i| (i, *id)))
            .collect();
        live.sort_unstable_by_key(|(i, _)| *i);
        let (_, first) = *live.first()?;
        let s: String = live.iter().filter_map(|(i, _)| chars.get(*i)).collect();
        Some(Inverse::ReviveRun {
            text: text_id,
            anchor: Anchor {
                parent: Some(first),
                side: Side::Left,
            },
            s,
            was: live.into_iter().map(|(_, id)| id).collect(),
        })
    }

    /// Move `node` back where it renders now — its live placement's list, at its
    /// live index there, discounting its own slot exactly as a forward reorder
    /// does.
    fn move_inverse(&self, node: ElementId) -> Option<Inverse> {
        let from = *self.parents.get(&node)?;
        let list = self.lists.get(&from)?;
        let slot = self
            .placements
            .get(&node)?
            .iter()
            .filter(|p| p.list == from)
            .map(|p| p.stamp)
            .max()?;
        let l = list.borrow();
        let index = l.live_index(slot)?;
        Some(Inverse::Op {
            target: from,
            kind: OpKind::XmlMove {
                node,
                anchor: l.place_excluding(index, Some(slot)),
            },
        })
    }

    /// Re-create the RangedElement about to be tombstoned over the same span,
    /// carrying its payload. A tombstone is terminal, so the revival is a fresh
    /// annotation with a new id.
    fn ranged_inverse(&self, id: ElementId) -> Option<Inverse> {
        let e = self.ranged.get(&id)?;
        if e.tombstone {
            return None;
        }
        let payload = match &e.payload {
            Payload::Scalar { value, .. } => Snap::Scalar(value.clone()),
            Payload::Composite { kind } => {
                let pid = payload_id(id, *kind);
                match kind {
                    ElementKind::Map => {
                        self.snapshot(&Element::Map(self.maps.get(&pid)?.clone()), 0)
                    }
                    ElementKind::List => {
                        self.snapshot(&Element::List(self.lists.get(&pid)?.clone()), 0)
                    }
                    ElementKind::Text => {
                        self.snapshot(&Element::Text(self.texts.get(&pid)?.clone()), 0)
                    }
                    _ => None,
                }?
            }
        };
        Some(Inverse::Ranged {
            start: e.start,
            end: e.end,
            name: e.name.clone(),
            payload,
            was: id,
        })
    }

    /// Capture `element` deeply enough to rebuild it out of forward ops. `None`
    /// past [`MAX_SNAPSHOT_DEPTH`], so the walk cannot run off the stack.
    fn snapshot(&self, element: &Element, depth: u32) -> Option<Snap> {
        if depth >= MAX_SNAPSHOT_DEPTH {
            return None;
        }
        let next = depth + 1;
        Some(match element {
            Element::Scalar(v) => Snap::Scalar(v.clone()),
            Element::Register(r) => Snap::Register(r.borrow().read().clone()),
            Element::Counter(c) => Snap::Counter(c.borrow().read()),
            Element::Map(m) => Snap::Map(self.snapshot_slots(m, next)),
            Element::List(l) => Snap::List(
                l.borrow()
                    .values()
                    .into_iter()
                    .filter_map(|v| match v {
                        Element::Scalar(s) => Some(s),
                        _ => None,
                    })
                    .collect(),
            ),
            Element::Text(t) => Snap::Text(t.borrow().as_string()),
            Element::XmlElement(x) => {
                let (tag, attrs, children) = {
                    let x = x.borrow();
                    (x.tag().to_vec(), x.attrs(), x.children())
                };
                Snap::XmlElement {
                    tag,
                    attrs: self.snapshot_slots(&attrs, next),
                    children: self.snapshot_children(&children, next),
                }
            }
            Element::XmlFragment(f) => Snap::XmlFragment {
                children: self.snapshot_children(&f.borrow().children(), next),
            },
        })
    }

    /// Capture a map's live slots, skipping any too deep to rebuild.
    fn snapshot_slots(&self, map: &Rc<RefCell<Map>>, depth: u32) -> Vec<(Vec<u8>, Snap)> {
        map.borrow()
            .entries()
            .into_iter()
            .filter_map(|(key, value)| Some((key, self.snapshot(&value, depth)?)))
            .collect()
    }

    /// Capture a children sequence's live nodes, skipping any too deep.
    fn snapshot_children(&self, list: &Rc<RefCell<List>>, depth: u32) -> Vec<Snap> {
        list.borrow()
            .values()
            .into_iter()
            .filter_map(|v| self.snapshot(&v, depth))
            .collect()
    }

    /// Rebuild a captured sequence node in `list` at `anchor`, returning the
    /// sequence id it came back under. Only the two movable node kinds ever
    /// occupy a children sequence.
    fn revive_node(&mut self, list: ElementId, anchor: Anchor, node: Snap) -> Option<Stamp> {
        match node {
            Snap::Text(s) => {
                let stamp =
                    self.emit_stamped(list, OpKind::XmlInsertChild { tag: None, anchor })?;
                let child = xml_child_id(list, stamp, ElementKind::Text);
                self.fill_text(child, s);
                Some(stamp)
            }
            Snap::XmlElement {
                tag,
                attrs,
                children,
            } => {
                let stamp = self.emit_stamped(
                    list,
                    OpKind::XmlInsertChild {
                        tag: Some(tag),
                        anchor,
                    },
                )?;
                let child = xml_child_id(list, stamp, ElementKind::XmlElement);
                self.fill_slots(XmlElement::attrs_id(child), attrs);
                self.fill_children(XmlElement::children_id(child), children);
                Some(stamp)
            }
            _ => None,
        }
    }

    /// Re-create a captured RangedElement and rebuild its payload.
    fn revive_ranged(
        &mut self,
        start: RangeAnchor,
        end: RangeAnchor,
        name: Option<Vec<u8>>,
        payload: Snap,
    ) -> Option<ElementId> {
        let init = match &payload {
            Snap::Scalar(v) => RangedInit::Scalar(v.clone()),
            Snap::Map(_) => RangedInit::Composite(ElementKind::Map),
            Snap::List(_) => RangedInit::Composite(ElementKind::List),
            Snap::Text(_) => RangedInit::Composite(ElementKind::Text),
            _ => return None,
        };
        let root = self.root_id();
        let stamp = self.emit_stamped(
            root,
            OpKind::RangedCreate {
                start,
                end,
                payload: init,
                name,
            },
        )?;
        let ranged = ranged_id(stamp);
        match payload {
            Snap::Map(slots) => self.fill_slots(payload_id(ranged, ElementKind::Map), slots),
            Snap::List(values) => self.fill_list(payload_id(ranged, ElementKind::List), values),
            Snap::Text(s) => self.fill_text(payload_id(ranged, ElementKind::Text), s),
            _ => {}
        }
        Some(ranged)
    }

    /// Re-create captured slots in the map `map_id`, descending into each
    /// container as it is installed.
    fn fill_slots(&mut self, map_id: ElementId, slots: Vec<(Vec<u8>, Snap)>) {
        for (key, snap) in slots {
            match snap {
                Snap::Scalar(value) => self.emit(map_id, OpKind::MapSet { key, value }),
                Snap::Register(value) => self.emit(map_id, OpKind::RegisterSet { key, value }),
                Snap::Counter(total) => self.fill_counter(map_id, key, total),
                Snap::Map(slots) => {
                    let child = ElementId::derive(map_id, &key, ElementKind::Map);
                    self.emit(map_id, OpKind::MapCreate { key });
                    self.fill_slots(child, slots);
                }
                Snap::List(values) => {
                    let child = ElementId::derive(map_id, &key, ElementKind::List);
                    self.emit(map_id, OpKind::ListCreate { key });
                    self.fill_list(child, values);
                }
                Snap::Text(s) => {
                    let child = ElementId::derive(map_id, &key, ElementKind::Text);
                    self.emit(map_id, OpKind::TextCreate { key });
                    self.fill_text(child, s);
                }
                Snap::XmlElement {
                    tag,
                    attrs,
                    children,
                } => {
                    let child = XmlElement::node_id(map_id, &key, &tag);
                    self.emit(map_id, OpKind::XmlElementCreate { key, tag });
                    self.fill_slots(XmlElement::attrs_id(child), attrs);
                    self.fill_children(XmlElement::children_id(child), children);
                }
                Snap::XmlFragment { children } => {
                    let child = XmlFragment::node_id(map_id, &key);
                    self.emit(map_id, OpKind::XmlFragmentCreate { key });
                    self.fill_children(XmlFragment::children_id(child), children);
                }
            }
        }
    }

    /// Drive a counter at `key` to `total` from zero. The op carries a `u32`
    /// delta, so a total beyond that range takes several.
    fn fill_counter(&mut self, map_id: ElementId, key: Vec<u8>, total: i64) {
        let mut left = total.unsigned_abs();
        // A zero total still re-installs the counter in its slot.
        loop {
            let amount = u32::try_from(left.min(u64::from(u32::MAX))).unwrap_or(u32::MAX);
            left -= u64::from(amount);
            let kind = if total < 0 {
                OpKind::CounterDec {
                    key: key.clone(),
                    amount,
                }
            } else {
                OpKind::CounterInc {
                    key: key.clone(),
                    amount,
                }
            };
            self.emit(map_id, kind);
            if left == 0 {
                return;
            }
        }
    }

    /// Re-insert captured values into the list `list_id`, in order.
    fn fill_list(&mut self, list_id: ElementId, values: Vec<Scalar>) {
        for (index, value) in values.into_iter().enumerate() {
            let Some(anchor) = self.lists.get(&list_id).map(|l| l.borrow().place(index)) else {
                return;
            };
            self.emit(list_id, OpKind::ListInsert { value, anchor });
        }
    }

    /// Re-insert a captured string into the text `text_id`.
    fn fill_text(&mut self, text_id: ElementId, s: String) {
        if s.is_empty() {
            return;
        }
        let Some(anchor) = self.texts.get(&text_id).map(|t| t.borrow().place(0)) else {
            return;
        };
        self.emit(text_id, OpKind::TextInsert { s, anchor });
    }

    /// Rebuild captured children into the sequence `list_id`, in order.
    fn fill_children(&mut self, list_id: ElementId, children: Vec<Snap>) {
        for (index, child) in children.into_iter().enumerate() {
            let Some(anchor) = self.lists.get(&list_id).map(|l| l.borrow().place(index)) else {
                return;
            };
            let _ = self.revive_node(list_id, anchor, child);
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

/// A stored `RangedElement`: fixed endpoints, a payload, and a tombstone that a
/// delete raises (delete wins over a concurrent payload change).
struct RangedEntry {
    start: RangeAnchor,
    end: RangeAnchor,
    payload: Payload,
    name: Option<Vec<u8>>,
    tombstone: bool,
}

/// A RangedElement's stored payload. A `Scalar` is LWW, carrying the stamp that
/// last set it. A `Composite` names the kind of a nested container installed at
/// [`payload_id`]; its data lives in the matching by-id registry, edited through
/// the normal container ops, so the entry holds only the kind.
enum Payload {
    Scalar { value: Scalar, stamp: Stamp },
    Composite { kind: ElementKind },
}

impl RangedEntry {
    /// The public read view of this entry under its id.
    fn view(&self, id: ElementId) -> RangedElement {
        let payload = match &self.payload {
            Payload::Scalar { value, .. } => RangedPayload::Scalar(value.clone()),
            Payload::Composite { kind } => RangedPayload::Composite {
                id: payload_id(id, *kind),
                kind: *kind,
            },
        };
        RangedElement {
            id,
            start: self.start,
            end: self.end,
            payload,
            name: self.name.clone(),
        }
    }
}

/// A stored ACL tuple: the immutable grant fields plus the set of actors that have
/// revoked it (a tuple is immutable, so a revoke is the only mutation). The set is
/// grow-only, merged by union — order-independent and idempotent — and any revoke
/// tombstones the tuple content-neutrally; *which* revokes carry authority is the
/// evaluator's ([`crate::acl`]) concern, recorded here as the revokers' identities.
struct AclEntry {
    subject: AclSubject,
    grant: AclGrant,
    effect: AclEffect,
    scope: AclScope,
    grantor: ClientId,
    revokers: BTreeSet<ClientId>,
}

impl AclEntry {
    /// Whether any revoke has tombstoned this tuple — it drops from the live read
    /// views regardless of the revoker's authority (a storage filter; provenance is
    /// the evaluator's job).
    fn is_revoked(&self) -> bool {
        !self.revokers.is_empty()
    }

    /// The public read view of this tuple under its id.
    fn view(&self, id: ElementId) -> AclTuple {
        AclTuple {
            id,
            subject: self.subject.clone(),
            grant: self.grant.clone(),
            effect: self.effect,
            scope: self.scope.clone(),
            grantor: self.grantor,
        }
    }

    /// The public record of this tuple under its id: the grant plus its revoke
    /// provenance, the authority evaluator's input.
    fn record(&self, id: ElementId) -> AclRecord {
        AclRecord {
            tuple: self.view(id),
            revoked_by: self.revokers.iter().copied().collect(),
        }
    }
}

/// The scalar payload of the highest-stamped covering mark — the LWW winner for a
/// boolean/value flavor. A covering mark with a composite payload carries no LWW
/// stamp and is skipped (boolean/value marks author a scalar).
fn lww_scalar<'a>(covering: &[(ElementId, &'a RangedEntry)]) -> Option<&'a Scalar> {
    covering
        .iter()
        .filter_map(|(_, e)| match &e.payload {
            Payload::Scalar { value, stamp } => Some((*stamp, value)),
            Payload::Composite { .. } => None,
        })
        .max_by_key(|(stamp, _)| *stamp)
        .map(|(_, value)| value)
}

/// A boolean mark's presence from its payload: an explicit `Bool` decides, any
/// other scalar counts as present (the covering mark still marks the character).
fn scalar_is_present(s: &Scalar) -> bool {
    match s {
        Scalar::Bool(b) => *b,
        _ => true,
    }
}

/// The id a RangedElement's composite payload container derives to — under the
/// RangedElement id as namespace, so it cannot collide with a user map slot
/// (whose parent is a user map, never a stamp-derived RangedElement id).
fn payload_id(ranged: ElementId, kind: ElementKind) -> ElementId {
    ElementId::derive(ranged, b"payload", kind)
}

/// The container kinds a create op installs. An `XmlElement` carries its tag,
/// which folds into the child's derived id.
#[derive(Clone)]
enum Container {
    Map,
    List,
    Text,
    XmlElement(Vec<u8>),
    XmlFragment,
}

impl Container {
    fn element_kind(&self) -> ElementKind {
        match self {
            Container::Map => ElementKind::Map,
            Container::List => ElementKind::List,
            Container::Text => ElementKind::Text,
            Container::XmlElement(_) => ElementKind::XmlElement,
            Container::XmlFragment => ElementKind::XmlFragment,
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
        (Element::XmlElement(x), Element::XmlElement(y)) => Rc::ptr_eq(x, y),
        (Element::XmlFragment(x), Element::XmlFragment(y)) => Rc::ptr_eq(x, y),
        _ => false,
    }
}

/// The total order the held ops are encoded in — author, then the author's own
/// sequence. It matches the causal frontier's sort, and it is total over a buffer
/// because an id names one op: the buffer holds an id once, and a snapshot
/// presenting a repeat is refused.
fn op_order(id: OpId) -> ([u8; 16], u64) {
    (id.client.as_bytes(), id.seq)
}

/// A stamp rendered as derivation-key bytes, so a sequence child's element id
/// derives deterministically from its node stamp. This is a hash input for
/// [`ElementId::derive`], not a wire encoding — it only has to be a stable
/// injective function of the stamp, so it is deliberately independent of the
/// codec's stamp layout and need not track it.
fn stamp_key(stamp: Stamp) -> [u8; 24] {
    let mut b = [0u8; 24];
    b[..8].copy_from_slice(&stamp.lamport.to_le_bytes());
    b[8..].copy_from_slice(&stamp.client.as_bytes());
    b
}

/// The element id an XML sequence child takes: derived from its children List
/// and its node stamp, so the apply path, the readiness gate, and the cursor all
/// agree. `kind` is `XmlElement` for an element child, `Text` for a text run.
fn xml_child_id(list_id: ElementId, stamp: Stamp, kind: ElementKind) -> ElementId {
    ElementId::derive(list_id, &stamp_key(stamp), kind)
}

/// The placement a movable node was born at — the one whose `(list, stamp)` re-derives
/// the node's own id. A move keeps the node's birth id, so a move placement never does,
/// which tells the birth placement (the created-under list) from the move placements.
///
/// Pure in the key, for the reason [`Document::born_at`] is: a stamp derives exactly two
/// children, so both are tried rather than the one the registry happens to hold — an
/// `XmlReveal` naming a derivable id can register it under the other kind, and a birth
/// answering "born nowhere" here loses the created-under edge the cycle check walks.
fn birth_placement(node: ElementId, places: &[Placement]) -> Option<&Placement> {
    places.iter().find(|p| {
        [ElementKind::XmlElement, ElementKind::Text]
            .into_iter()
            .any(|kind| xml_child_id(p.list, p.stamp, kind) == node)
    })
}

/// The id of the RangedElement a create at `stamp` mints. Derived under a fixed
/// annotation namespace so it never collides with a user's map slot, and from the
/// globally-unique op stamp so every replica agrees and concurrent creates differ.
fn ranged_id(stamp: Stamp) -> ElementId {
    let ns = ElementId::from_bytes(*b"crdtsync\0ranged\0");
    ElementId::derive(ns, &stamp_key(stamp), ElementKind::Scalar)
}

/// The [`OpId`] a reveal shell for `node` carries. A reveal is a redaction-time
/// synthesis, not an authored op, so it has no real `(client, seq)` — but it must
/// dedup stably (a resumed catch-up re-derives the same shell) and never collide with a
/// real authored op the reader also receives. The client is derived from the node under
/// a fixed reveal namespace: deterministic and unique per node (so two revealed nodes
/// never alias). No *derived* replica id can coincide with it: this derivation's name is
/// a node id plus a kind tag (17 bytes), where the only other id-shaped derivation a
/// replica performs ([`for_channel`](ClientId::for_channel)) names a four-byte channel
/// number, so the two SHA-1 inputs differ by length alone whatever namespaces they run
/// under. A *declared* id is not a derivation at all — an embedder supplies its bytes —
/// so an embedder that reuses a shell's id here collides, the same one-id-one-replica
/// contract that governs two sessions sharing an id. `seq` is 0 — the derived client is
/// a namespace of one.
fn reveal_op_id(node: ElementId) -> OpId {
    let ns = ElementId::from_bytes(*b"crdtsync\0reveal\0");
    let derived = ElementId::derive(ns, &node.as_bytes(), ElementKind::XmlElement);
    OpId {
        client: ClientId::from_bytes(derived.as_bytes()),
        seq: 0,
    }
}

/// The id of the ACL tuple a grant at `stamp` mints. Derived under a fixed
/// authorization namespace so it never collides with a user's map slot, and from
/// the globally-unique op stamp so every replica agrees and concurrent grants
/// differ.
fn acl_id(stamp: Stamp) -> ElementId {
    let ns = ElementId::from_bytes(*b"crdtsync\0acl\0\0\0\0");
    ElementId::derive(ns, &stamp_key(stamp), ElementKind::Scalar)
}

/// A declared partition clock, refused rather than clamped when it sits above
/// [`LAMPORT_STATE_CEILING`]. See [`Document::read_state`]'s use — the bound has
/// to be a refusal, because a clock is a high-water over ids already published.
fn clock_within_ceiling(lamport: u64, what: &'static str) -> Result<u64, DecodeError> {
    if lamport > LAMPORT_STATE_CEILING {
        return Err(DecodeError::BadTag { what, tag: 0 });
    }
    Ok(lamport)
}

/// A stamp no op can ever occupy, for deriving the handle a refused mutation hands
/// back — an id that addresses nothing, so every edit through it is a no-op.
///
/// `u64::MAX` is above [`LAMPORT_STATE_CEILING`], and every seam that installs a
/// stamp refuses one reaching past that ceiling
/// ([`stamp_occupies_a_mintable_position`], [`mint_position`]), so no *op* can put
/// an element here on this replica or any other — which is what a handle to nothing
/// has to guarantee, or a caller could delete a stranger's element through it.
///
/// It is not absolute. *Every* registry — counter, list, text, map, XML element and
/// fragment, ranged and ACL — decodes its keys as raw [`ElementId`]s, so a crafted
/// snapshot can plant an entry at the derived id directly. That includes the
/// sequence registries a refused XML child insert resolves through, which is where
/// this handle is most used. Together with a declared record at the ceiling to force
/// the refusal, it aims the handle at an attacker-chosen element. Both halves take a
/// hand-built snapshot; a replica that only ever folds ops is exact.
fn unmintable_stamp(client: ClientId) -> Stamp {
    Stamp {
        lamport: u64::MAX,
        client,
        offset: 0,
    }
}

/// The stamp `client` mints to sit strictly past lamport `from` in its own id
/// space, reserving `span` consecutive ids from there — or `None` when the whole
/// reservation would not fit under [`LAMPORT_STATE_CEILING`].
///
/// **Refusal is the only total answer at the top of the space**, which is why the
/// mint returns an `Option` at all. Clamping would re-issue an id that is already
/// live: two edits would take one stamp, and so would the ids they derive, since
/// [`stamp_key`] reads the lamport and the client alone. Moving into the
/// sub-lamport `offset` instead is no escape for the same reason — that dimension
/// is not in the key. And letting it run past the ceiling puts the replica's own
/// high-water outside what [`Document::read_state`] admits, so the replica could
/// not reload the snapshot it just wrote.
///
/// The bound is the one a wire stamp is held to as well
/// ([`stamp_occupies_a_mintable_position`]), so a minted op is always inside what
/// every peer admits — a refusal here is never a divergence, it is the same edge
/// every replica sees.
///
/// `offset` is always `0`: the reservation fits below the ceiling, so counting a
/// run's codepoints up from the base never carries.
fn mint_position(from: u64, span: u64, client: ClientId) -> Option<Stamp> {
    let lamport = from.checked_add(1)?;
    if lamport.checked_add(span - 1)? > LAMPORT_STATE_CEILING {
        return None;
    }
    Some(Stamp {
        lamport,
        client,
        offset: 0,
    })
}

impl Op {
    /// Whether any replica may hold this op at all — the judgement
    /// [`Document::apply`] makes before it decides whether the op applies *now*.
    ///
    /// An inadmissible op is refused **forever**, by every replica: each condition
    /// below reads the op and nothing else, so two replicas at unrelated states
    /// reach the same verdict and the network converges on the op's absence rather
    /// than splitting over it. That is what makes rejecting it safe, and it is the
    /// line an ingest seam needs: an admissible op that does not apply yet is
    /// *waiting* — held until a create makes its target reachable or its
    /// transaction group completes — so it is state worth logging, fanning out and
    /// acknowledging, while an inadmissible one is worth none of those. Admissible
    /// says only that no rule forbids the op outright; it does not promise the op
    /// applies, and an already-applied op stays admissible.
    ///
    /// Nothing this codebase emits is inadmissible — the mint is bounded by the same
    /// constants — so refusing one costs an honest peer nothing.
    pub fn is_admissible(&self) -> bool {
        // A node's id is its op's stamp, so an op whose stamp names a client other
        // than its author mints ids inside *that* client's id space. Every op this
        // codebase emits stamps under its author, including the server's reveal
        // shells.
        //
        // What this does **not** do is protect that id space: both fields come off
        // the same op, so a peer authoring under the victim's `ClientId` satisfies
        // it. Keeping the victim's next mint off ids planted under its own id is
        // [`Document::mint_floor`]'s job, on the apply path and on the buffer a
        // decode drains alike. This raises the bar to impersonating an identity
        // rather than merely naming one, which is a transport-authenticated claim.
        stamp_names_its_author(self)
            // The ids an op reserves are recorded against its author's high-water,
            // and that high-water is stored, so it has to stay inside what a decode
            // admits — hence a bound on the *position a stamp may occupy*. Refusal,
            // not a clamp, and for the same reason a stored clock is refused: a
            // stamp is an id, so lowering one aliases whatever already holds it. The
            // local mint stops at the same constant ([`mint_position`]), so no
            // replica emits what this rejects.
            && stamp_occupies_a_mintable_position(self)
            // A member declaring a group size outside the representable range is
            // refused on the same terms: no honest op carries one ([`MAX_TX_MEMBERS`]
            // bounds the mint), and refusing holds nothing — the point of the bound
            // being that such a member would wait in the buffer for a completion no
            // arrival brings. The codec refuses the same op at the wire boundary;
            // this is the seam an in-process relay or an SDK reaches without
            // crossing one.
            && self
                .tx
                .is_none_or(|tx| (1..=MAX_TX_MEMBERS).contains(&tx.count))
    }
}

/// Whether an op's stamp names the client that authored it. A node's id is its
/// op's stamp, so a mismatch mints ids inside another client's id space; nothing
/// this codebase emits carries one.
fn stamp_names_its_author(op: &Op) -> bool {
    op.stamp.client == op.id.client
}

/// The lamport the last id of `stamp`'s reservation occupies. A text run takes one
/// per codepoint, so the reservation, not the base, is what a high-water records.
fn reservation_end(stamp: Stamp, span: u64) -> u64 {
    stamp.lamport.saturating_add(span - 1)
}

/// Whether `op`'s stamp sits where an id may exist at all: inside the lamport
/// space, and off the sub-lamport dimension.
///
/// **The bound is [`LAMPORT_STATE_CEILING`], the same constant a decoded clock and
/// a decoded high-water are held to, and the same one [`mint_position`] stops the
/// local mint at.** One constant for the whole id space is what keeps the rule
/// convergent: a replica emits exactly the set its peers admit, so an honest mint
/// is never refused by the network. Bounding a wire stamp at
/// [`LAMPORT_WIRE_CEILING`] instead would break that — one op stamped at the wire
/// ceiling parks a folding replica's clock there, its next honest mint is one
/// above, and every peer refuses it. [`LAMPORT_WIRE_CEILING`] bounds what a *fold*
/// may do to a **clock**, which is a different question; the runway between the two
/// is what a clock-parked replica still mints into, 2^62 ids of it.
///
/// **`offset` must be zero.** [`stamp_key`] — the derived-id input for an ACL
/// tuple, a ranged element and an XML sequence child — is `lamport ++ client` and
/// omits the offset, so two stamps differing only there derive the *same* id and
/// the second create is silently dropped. No honest stamp carries one: a mint emits
/// `offset == 0`, and [`Stamp::run_member`] only carries into the offset past
/// `u64::MAX`, which the lamport bound above puts out of reach. So the sub-lamport
/// dimension is not a place an op may be, and refusing it is what makes the
/// lamport-only high-water a complete record of the ids a client holds.
fn stamp_occupies_a_mintable_position(op: &Op) -> bool {
    op.stamp.offset == 0 && reservation_end(op.stamp, span(&op.kind)) <= LAMPORT_STATE_CEILING
}

/// How many consecutive char_ids an op consumes from its stamp. A text run
/// takes one per codepoint; every other op takes one.
fn span(kind: &OpKind) -> u64 {
    match kind {
        OpKind::TextInsert { s, .. } => s.chars().count().max(1) as u64,
        _ => 1,
    }
}

/// The map key a container-create installs its child under, for a create keyed by a
/// map slot — so the op's zone resolves at the child's path, not the parent's. A
/// positional or keyless create (a list/XML positional child, a composite ranged
/// payload) inherits its container's partition and is `None` here.
fn create_child_key(kind: &OpKind) -> Option<&[u8]> {
    match kind {
        OpKind::MapCreate { key }
        | OpKind::ListCreate { key }
        | OpKind::TextCreate { key }
        | OpKind::XmlFragmentCreate { key }
        | OpKind::XmlElementCreate { key, .. } => Some(key),
        _ => None,
    }
}

/// The reading-stable ids of a keyed-repair set — the `onRepaired` baseline.
fn repair_ids(keyed: Vec<(Repair, RepairId)>) -> Vec<RepairId> {
    keyed.into_iter().map(|(_, id)| id).collect()
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

/// Encode a container registry: a count followed by each container, ordered by
/// id so equal states encode identically.
fn encode_registry<T>(
    out: &mut Vec<u8>,
    reg: &HashMap<ElementId, Rc<RefCell<T>>>,
    encode: impl Fn(&Rc<RefCell<T>>, &mut Vec<u8>),
) {
    let mut items: Vec<(&ElementId, &Rc<RefCell<T>>)> = reg.iter().collect();
    items.sort_by_key(|(id, _)| id.as_bytes());
    put_u32(out, len_u32(items.len()));
    for (_, item) in items {
        encode(item, out);
    }
}

/// Decode a container registry into handles keyed by id, rejecting a repeated
/// id as non-canonical.
fn decode_registry<T>(
    cur: &mut Cursor,
    decode: impl Fn(&mut Cursor) -> Result<T, DecodeError>,
    id_of: impl Fn(&T) -> ElementId,
) -> Result<HashMap<ElementId, Rc<RefCell<T>>>, DecodeError> {
    let count = cur.u32()?;
    let mut reg = HashMap::with_capacity((count as usize).min(1024));
    for _ in 0..count {
        let item = decode(cur)?;
        let id = id_of(&item);
        if reg.insert(id, Rc::new(RefCell::new(item))).is_some() {
            return Err(DecodeError::BadTag {
                what: "document: duplicate registry id",
                tag: 0,
            });
        }
    }
    Ok(reg)
}

/// Decode the list registry into shells, returning the composite sequence-node
/// references (tagged with their owning list id) for the document to resolve once
/// every registry exists.
#[allow(clippy::type_complexity)]
fn decode_list_registry(
    cur: &mut Cursor,
) -> Result<
    (
        HashMap<ElementId, Rc<RefCell<List>>>,
        Vec<(ElementId, Stamp, ElementKind, ElementId)>,
    ),
    DecodeError,
> {
    let count = cur.u32()?;
    let mut reg = HashMap::with_capacity((count as usize).min(1024));
    let mut refs = Vec::new();
    for _ in 0..count {
        let (list, node_refs) = List::decode_state_from(cur)?;
        let id = list.id();
        for (stamp, kind, ref_id) in node_refs {
            refs.push((id, stamp, kind, ref_id));
        }
        if reg.insert(id, Rc::new(RefCell::new(list))).is_some() {
            return Err(DecodeError::BadTag {
                what: "document: duplicate list id",
                tag: 0,
            });
        }
    }
    Ok((reg, refs))
}

/// Decode the XmlElement registry, pairing each element with the attrs Map and
/// children List already decoded under its derived ids.
fn decode_xml_element_registry(
    cur: &mut Cursor,
    maps: &HashMap<ElementId, Rc<RefCell<Map>>>,
    lists: &HashMap<ElementId, Rc<RefCell<List>>>,
) -> Result<HashMap<ElementId, Rc<RefCell<XmlElement>>>, DecodeError> {
    let count = cur.u32()?;
    let mut reg = HashMap::with_capacity((count as usize).min(1024));
    for _ in 0..count {
        let id = cur.element_id()?;
        let tag = cur.bytes()?;
        let attrs = maps
            .get(&XmlElement::attrs_id(id))
            .cloned()
            .ok_or(DecodeError::BadTag {
                what: "xml element: missing attrs map",
                tag: 0,
            })?;
        let children =
            lists
                .get(&XmlElement::children_id(id))
                .cloned()
                .ok_or(DecodeError::BadTag {
                    what: "xml element: missing children list",
                    tag: 0,
                })?;
        let handle = Rc::new(RefCell::new(XmlElement::from_registry(
            id, tag, attrs, children,
        )));
        if reg.insert(id, handle).is_some() {
            return Err(DecodeError::BadTag {
                what: "document: duplicate xml element id",
                tag: 0,
            });
        }
    }
    Ok(reg)
}

/// Decode the XmlFragment registry, pairing each fragment with its decoded
/// children List.
fn decode_xml_fragment_registry(
    cur: &mut Cursor,
    lists: &HashMap<ElementId, Rc<RefCell<List>>>,
) -> Result<HashMap<ElementId, Rc<RefCell<XmlFragment>>>, DecodeError> {
    let count = cur.u32()?;
    let mut reg = HashMap::with_capacity((count as usize).min(1024));
    for _ in 0..count {
        let id = cur.element_id()?;
        let children =
            lists
                .get(&XmlFragment::children_id(id))
                .cloned()
                .ok_or(DecodeError::BadTag {
                    what: "xml fragment: missing children list",
                    tag: 0,
                })?;
        let handle = Rc::new(RefCell::new(XmlFragment::from_registry(id, children)));
        if reg.insert(id, handle).is_some() {
            return Err(DecodeError::BadTag {
                what: "document: duplicate xml fragment id",
                tag: 0,
            });
        }
    }
    Ok(reg)
}

/// Reject a decoded parent graph that doesn't terminate: every chain of parent
/// links must reach the root (or a container with no recorded parent) without
/// revisiting a node. A cycle would spin `resolvable` forever on a later op.
fn reject_parent_cycles(
    parents: &HashMap<ElementId, ElementId>,
    root_id: ElementId,
) -> Result<(), DecodeError> {
    let mut terminates: HashSet<ElementId> = HashSet::new();
    terminates.insert(root_id);
    for &start in parents.keys() {
        if terminates.contains(&start) {
            continue;
        }
        let mut chain: HashSet<ElementId> = HashSet::new();
        let mut cur = start;
        let ends = loop {
            if terminates.contains(&cur) {
                break true;
            }
            if !chain.insert(cur) {
                break false;
            }
            match parents.get(&cur) {
                Some(&parent) => cur = parent,
                None => break true,
            }
        };
        if !ends {
            return Err(DecodeError::BadTag {
                what: "document: parent cycle",
                tag: 0,
            });
        }
        terminates.extend(chain);
    }
    Ok(())
}

/// Resolve a decoded slot or sequence-node reference to the registered handle it
/// names.
#[allow(clippy::too_many_arguments)]
fn resolve_ref(
    kind: ElementKind,
    id: ElementId,
    counters: &HashMap<ElementId, Rc<RefCell<Counter>>>,
    lists: &HashMap<ElementId, Rc<RefCell<List>>>,
    texts: &HashMap<ElementId, Rc<RefCell<Text>>>,
    maps: &HashMap<ElementId, Rc<RefCell<Map>>>,
    xml_elements: &HashMap<ElementId, Rc<RefCell<XmlElement>>>,
    xml_fragments: &HashMap<ElementId, Rc<RefCell<XmlFragment>>>,
) -> Result<Element, DecodeError> {
    let element = match kind {
        ElementKind::Counter => counters.get(&id).map(|c| Element::Counter(Rc::clone(c))),
        ElementKind::List => lists.get(&id).map(|l| Element::List(Rc::clone(l))),
        ElementKind::Text => texts.get(&id).map(|t| Element::Text(Rc::clone(t))),
        ElementKind::Map => maps.get(&id).map(|m| Element::Map(Rc::clone(m))),
        ElementKind::XmlElement => xml_elements
            .get(&id)
            .map(|x| Element::XmlElement(Rc::clone(x))),
        ElementKind::XmlFragment => xml_fragments
            .get(&id)
            .map(|f| Element::XmlFragment(Rc::clone(f))),
        // A leaf has no registered handle to reference.
        ElementKind::Scalar | ElementKind::Register => None,
    };
    element.ok_or(DecodeError::BadTag {
        what: "document: dangling reference",
        tag: 0,
    })
}

/// Restore displacement flags a snapshot doesn't store: a container is installed
/// iff it currently occupies its own slot or node — a live map slot, a live
/// sequence node, or the attrs/children an installed XML node owns — regardless
/// of whether an ancestor is displaced. Displacement is per-slot and never
/// propagates to descendants, so a container losing its own slot is the only
/// thing that displaces it; every such one decodes displaced.
#[allow(clippy::too_many_arguments)]
fn mark_displaced(
    maps: &HashMap<ElementId, Rc<RefCell<Map>>>,
    lists: &HashMap<ElementId, Rc<RefCell<List>>>,
    texts: &HashMap<ElementId, Rc<RefCell<Text>>>,
    counters: &HashMap<ElementId, Rc<RefCell<Counter>>>,
    xml_elements: &HashMap<ElementId, Rc<RefCell<XmlElement>>>,
    xml_fragments: &HashMap<ElementId, Rc<RefCell<XmlFragment>>>,
    ranged: &HashMap<ElementId, RangedEntry>,
    root_id: ElementId,
) {
    // A container is installed iff some parent holds it live *now* — the root, a
    // live map slot, or a live sequence node — independent of that parent's own
    // reachability (a child of a displaced map keeps its own flag clear, so a
    // later re-win of the ancestor restores the whole subtree).
    let mut installed: HashSet<ElementId> = HashSet::new();
    installed.insert(root_id);
    // A materialised RangedElement's composite payload is held by the range, not a
    // slot, so seed it here; its own nested containers are picked up by the
    // map/list scans below. A tombstoned range's payload is seeded too, matching
    // live state (a delete hides the payload at the read layer but never displaces
    // its container), so the container decodes with the same flag on every replica.
    for (id, e) in ranged {
        if let Payload::Composite { kind } = &e.payload {
            installed.insert(payload_id(*id, *kind));
        }
    }
    for m in maps.values() {
        for value in m.borrow().live_values() {
            if value.kind() != ElementKind::Scalar {
                installed.insert(value.id());
            }
        }
    }
    for l in lists.values() {
        for value in l.borrow().live_values() {
            if value.kind() != ElementKind::Scalar {
                installed.insert(value.id());
            }
        }
    }
    // An installed XML node's attrs Map and children List are intrinsic to it —
    // never held by a slot — so they follow its flag (the halves are only ever
    // displaced with their node). The scans above already settled every node's
    // own installed status, so one pass suffices.
    for (id, x) in xml_elements {
        if installed.contains(id) {
            let x = x.borrow();
            installed.insert(x.attrs().borrow().id());
            installed.insert(x.children().borrow().id());
        }
    }
    for (id, f) in xml_fragments {
        if installed.contains(id) {
            installed.insert(f.borrow().children().borrow().id());
        }
    }
    for (id, c) in counters {
        if !installed.contains(id) {
            c.borrow().displace();
        }
    }
    for (id, l) in lists {
        if !installed.contains(id) {
            l.borrow().displace();
        }
    }
    for (id, t) in texts {
        if !installed.contains(id) {
            t.borrow().displace();
        }
    }
    for (id, x) in xml_elements {
        if !installed.contains(id) {
            x.borrow().displace();
        }
    }
    for (id, f) in xml_fragments {
        if !installed.contains(id) {
            f.borrow().displace();
        }
    }
    for (id, m) in maps {
        if *id != root_id && !installed.contains(id) {
            m.borrow().displace();
        }
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
        let child = ElementId::derive(self.map_id, key, ElementKind::Map);
        if !self.holds_live_map(key, child) {
            self.doc
                .emit(self.map_id, OpKind::MapCreate { key: key.to_vec() });
        }
        MapCursor {
            doc: self.doc,
            map_id: child,
        }
    }

    /// Descend into a nested Map at `key`, consuming this cursor. Chains without
    /// nesting borrows, so a caller can walk a runtime-length path in a loop.
    pub fn into_map(self, key: &[u8]) -> MapCursor<'a> {
        let child = ElementId::derive(self.map_id, key, ElementKind::Map);
        if !self.holds_live_map(key, child) {
            self.doc
                .emit(self.map_id, OpKind::MapCreate { key: key.to_vec() });
        }
        MapCursor {
            doc: self.doc,
            map_id: child,
        }
    }

    /// Whether `key` in this map already holds the live Map `child`. Descending into
    /// an existing map re-asserts nothing, so the create is elided — an idempotent
    /// re-create is a no-op on this replica's state, but it is still a real op in the
    /// parent's partition, and re-emitting it on every nested write would leak that
    /// partition's activity to its subscribers even when only the child changed. A
    /// missing, tombstoned, or differently-kinded slot still emits the create.
    fn holds_live_map(&self, key: &[u8], child: ElementId) -> bool {
        self.doc.maps.contains_key(&child)
            && self.doc.maps.get(&self.map_id).is_some_and(|m| {
                matches!(m.borrow().get(key), Some(Element::Map(c)) if c.borrow().id() == child)
            })
    }

    /// Descend into the keyed sub-namespace at `key`: an existing `XmlElement`'s
    /// attrs Map when the slot holds one (no op — the element already exists),
    /// else a nested Map (created if absent). An element's attrs and a Map are
    /// both keyed slot-holders, so the path façade descends them uniformly —
    /// naming an element then an attr key reaches the attr through the ordinary
    /// map value API. A fragment slot has no attrs; the façade filters that dead
    /// end before descending (`path::writable`), so it never reaches here.
    pub fn child(&mut self, key: &[u8]) -> MapCursor<'_> {
        match self.xml_attrs_id(key) {
            Some(map_id) => MapCursor {
                doc: self.doc,
                map_id,
            },
            None => self.map(key),
        }
    }

    /// As [`child`](Self::child), consuming this cursor to chain a runtime-length
    /// path without nesting borrows.
    pub fn into_child(self, key: &[u8]) -> MapCursor<'a> {
        match self.xml_attrs_id(key) {
            Some(map_id) => MapCursor {
                doc: self.doc,
                map_id,
            },
            None => self.into_map(key),
        }
    }

    /// The attrs Map id of a live `XmlElement` occupying `key` in this map, if
    /// the slot holds one — the seam the contextual descent branches on.
    fn xml_attrs_id(&self, key: &[u8]) -> Option<ElementId> {
        let map = self.doc.maps.get(&self.map_id)?;
        let value = map.borrow().get(key);
        match value {
            Some(Element::XmlElement(x)) => Some(XmlElement::attrs_id(x.borrow().id())),
            _ => None,
        }
    }

    /// A cursor over the children sequence of the live `XmlElement` or
    /// `XmlFragment` occupying `key` in this map, or `None` if the slot holds
    /// neither. The path façade names an element by its map slot, so it reaches an
    /// existing element's children here rather than through a create cursor.
    pub fn xml_children(&mut self, key: &[u8]) -> Option<XmlChildrenCursor<'_>> {
        let map = self.doc.maps.get(&self.map_id)?;
        let value = map.borrow().get(key);
        let list_id = match value {
            Some(Element::XmlElement(x)) => XmlElement::children_id(x.borrow().id()),
            Some(Element::XmlFragment(f)) => XmlFragment::children_id(f.borrow().id()),
            _ => return None,
        };
        Some(XmlChildrenCursor {
            doc: self.doc,
            list_id,
        })
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

    /// Descend into an `XmlElement` at `key` with `tag`, creating it if absent.
    /// The tag is part of the node's identity, so a different tag at the same key
    /// is a different element.
    pub fn xml_element(&mut self, key: &[u8], tag: &[u8]) -> XmlCursor<'_> {
        self.doc.emit(
            self.map_id,
            OpKind::XmlElementCreate {
                key: key.to_vec(),
                tag: tag.to_vec(),
            },
        );
        XmlCursor {
            doc: self.doc,
            xml_id: XmlElement::node_id(self.map_id, key, tag),
        }
    }

    /// Descend into an `XmlFragment` at `key`, creating it if absent. A fragment
    /// is tagless and has no attrs — only a children sequence.
    pub fn xml_fragment(&mut self, key: &[u8]) -> XmlFragmentCursor<'_> {
        self.doc
            .emit(self.map_id, OpKind::XmlFragmentCreate { key: key.to_vec() });
        XmlFragmentCursor {
            doc: self.doc,
            children_id: XmlFragment::children_id(XmlFragment::node_id(self.map_id, key)),
        }
    }

    /// Move the XML node `node` under `new_parent` (an element or fragment id) at
    /// live `index` in its children. The node keeps its identity and subtree;
    /// concurrent moves converge to one parent (Kleppmann 2021). A move under the
    /// node's own descendant is a cycle and is dropped. Addresses by id, so it is
    /// not tied to this cursor's map.
    pub fn move_xml(&mut self, node: ElementId, new_parent: ElementId, index: usize) {
        // A move is only defined for a node that lives in a children sequence; a
        // map-slot root has no placement to relocate, so emit nothing rather than
        // an op no replica can apply.
        if !self.doc.placements.contains_key(&node) {
            return;
        }
        let dest_list = XmlElement::children_id(new_parent);
        // A reorder within the same parent re-places a node that still occupies a
        // slot in this list; discount that slot so the target index is read
        // against the sequence as it will be once the node leaves it.
        let self_slot = self.doc.placements.get(&node).and_then(|ps| {
            ps.iter()
                .filter(|p| p.list == dest_list)
                .map(|p| p.stamp)
                .max()
        });
        let anchor = match self.doc.lists.get(&dest_list) {
            Some(list) => list.borrow().place_excluding(index, self_slot),
            None => return,
        };
        self.doc.emit(dest_list, OpKind::XmlMove { node, anchor });
    }

    /// A cursor over the document-level RangedElement annotation set. The set is
    /// the document's, not this map's — reachable from any cursor, it addresses
    /// the same annotations. Kept off the map value API because a range is not a
    /// map slot.
    pub fn ranged(&mut self) -> RangedCursor<'_> {
        RangedCursor { doc: self.doc }
    }

    /// A cursor over the document-level ACL tuple set. Like [`ranged`](Self::ranged),
    /// the set is the document's, addressed the same from any cursor — an ACL
    /// tuple is not a map slot.
    pub fn acl(&mut self) -> AclCursor<'_> {
        AclCursor { doc: self.doc }
    }
}

/// A cursor over the document-level RangedElement annotation set: create a range,
/// change its payload, or delete it. Its edits address the document, independent
/// of any map — a range lives in the document's annotation set, not in the
/// sequence it annotates.
pub struct RangedCursor<'a> {
    doc: &'a mut Document,
}

impl RangedCursor<'_> {
    /// Create a `RangedElement` spanning `start`..`end` (each a `(sequence,
    /// RelativePosition)` anchor; the two may name different sequences) with
    /// `payload`. Returns its stable id — the handle to change its payload or
    /// delete it.
    pub fn create(&mut self, start: RangeAnchor, end: RangeAnchor, payload: Scalar) -> ElementId {
        self.create_with(start, end, RangedInit::Scalar(payload), None)
    }

    /// Author a mark named `name` over `start`..`end` carrying `value` — a
    /// convention over the annotation set. A boolean mark passes `Scalar::Bool` for
    /// presence; a value mark its value (a link's href). The read model
    /// ([`Document::marks_at`](Document::marks_at)) combines same-named marks per
    /// the schema's declared flavor. Returns the mark's RangedElement id.
    pub fn mark(
        &mut self,
        name: &[u8],
        start: RangeAnchor,
        end: RangeAnchor,
        value: Scalar,
    ) -> ElementId {
        self.create_with(start, end, RangedInit::Scalar(value), Some(name.to_vec()))
    }

    /// Create a RangedElement whose payload is a nested Map — a structured comment
    /// body, an object-mark value. Returns the RangedElement id; edit the payload
    /// through [`payload_map`](Self::payload_map).
    pub fn create_map(&mut self, start: RangeAnchor, end: RangeAnchor) -> ElementId {
        self.create_with(start, end, RangedInit::Composite(ElementKind::Map), None)
    }

    /// Create a RangedElement whose payload is a nested List.
    pub fn create_list(&mut self, start: RangeAnchor, end: RangeAnchor) -> ElementId {
        self.create_with(start, end, RangedInit::Composite(ElementKind::List), None)
    }

    /// Create a RangedElement whose payload is a nested Text.
    pub fn create_text(&mut self, start: RangeAnchor, end: RangeAnchor) -> ElementId {
        self.create_with(start, end, RangedInit::Composite(ElementKind::Text), None)
    }

    fn create_with(
        &mut self,
        start: RangeAnchor,
        end: RangeAnchor,
        payload: RangedInit,
        name: Option<Vec<u8>>,
    ) -> ElementId {
        let root = self.doc.root_id();
        // A refused mint creates nothing, so the handle names nothing: an
        // unoccupiable stamp, which every later edit through it resolves to absent.
        // See [`Document::mint_refused`] — the refusal is reported once, for the
        // whole intention, so an `Option` here would only restate it at every
        // mutation.
        let stamp = self
            .doc
            .emit_stamped(
                root,
                OpKind::RangedCreate {
                    start,
                    end,
                    payload,
                    name,
                },
            )
            .unwrap_or_else(|| unmintable_stamp(self.doc.client));
        ranged_id(stamp)
    }

    /// A cursor over the Map payload of the live RangedElement `id`, or `None`
    /// when it is absent, deleted, or its payload is not a Map.
    pub fn payload_map(&mut self, id: ElementId) -> Option<MapCursor<'_>> {
        self.payload_cursor(id, ElementKind::Map)
            .map(|map_id| MapCursor {
                doc: self.doc,
                map_id,
            })
    }

    /// A cursor over the List payload of the live RangedElement `id`, or `None`
    /// when it is absent, deleted, or its payload is not a List.
    pub fn payload_list(&mut self, id: ElementId) -> Option<ListCursor<'_>> {
        self.payload_cursor(id, ElementKind::List)
            .map(|list_id| ListCursor {
                doc: self.doc,
                list_id,
            })
    }

    /// A cursor over the Text payload of the live RangedElement `id`, or `None`
    /// when it is absent, deleted, or its payload is not a Text.
    pub fn payload_text(&mut self, id: ElementId) -> Option<TextCursor<'_>> {
        self.payload_cursor(id, ElementKind::Text)
            .map(|text_id| TextCursor {
                doc: self.doc,
                text_id,
            })
    }

    /// The payload container id for a live RangedElement whose payload is exactly
    /// `kind` — the gate every payload cursor shares.
    fn payload_cursor(&self, id: ElementId, kind: ElementKind) -> Option<ElementId> {
        let e = self.doc.ranged.get(&id).filter(|e| !e.tombstone)?;
        matches!(&e.payload, Payload::Composite { kind: k } if *k == kind)
            .then(|| payload_id(id, kind))
    }

    /// Replace a RangedElement's scalar payload (last-writer-wins). Emits nothing
    /// for an id this replica has not yet materialised (a local apply would no-op
    /// while still broadcasting, diverging the author from a peer that applied the
    /// change against the present entry) or one whose payload is a composite — a
    /// composite is edited through its container, so a set here would be an inert
    /// op on every replica.
    pub fn set_payload(&mut self, id: ElementId, payload: Scalar) {
        if !matches!(self.doc.ranged.get(&id), Some(e) if matches!(e.payload, Payload::Scalar { .. }))
        {
            return;
        }
        let root = self.doc.root_id();
        self.doc
            .emit(root, OpKind::RangedSetPayload { id, payload });
    }

    /// Delete a RangedElement. Delete wins over a concurrent payload change.
    /// Emits nothing for an id this replica has not yet materialised (see
    /// [`set_payload`](Self::set_payload)).
    pub fn delete(&mut self, id: ElementId) {
        if !self.doc.ranged.contains_key(&id) {
            return;
        }
        let root = self.doc.root_id();
        self.doc.emit(root, OpKind::RangedDelete { id });
    }
}

/// A cursor over the document-level ACL tuple set: grant a tuple or revoke one.
/// Its edits address the document's authorization set, independent of any map.
/// Storage only — it records what the caller passes and never checks authority
/// (who may grant or revoke is the server's concern, in a later slice).
pub struct AclCursor<'a> {
    doc: &'a mut Document,
}

impl AclCursor<'_> {
    /// Grant an ACL tuple scoped to a fixed `path` — sugar for
    /// [`grant_scoped`](Self::grant_scoped) with an [`AclScope::Path`]. The grant
    /// governs whatever occupies that slot; use [`grant_element`](Self::grant_element)
    /// for a grant that follows a movable element instead.
    pub fn grant(
        &mut self,
        subject: AclSubject,
        grant: AclGrant,
        effect: AclEffect,
        path: Vec<u8>,
        grantor: ClientId,
    ) -> ElementId {
        self.grant_scoped(subject, grant, effect, AclScope::Path(path), grantor)
    }

    /// Grant an ACL tuple scoped to a stable element `id` — sugar for
    /// [`grant_scoped`](Self::grant_scoped) with an [`AclScope::Element`]. The grant
    /// resolves to the element's current path at evaluation, so it follows the
    /// element across a tree-move.
    pub fn grant_element(
        &mut self,
        subject: AclSubject,
        grant: AclGrant,
        effect: AclEffect,
        id: ElementId,
        grantor: ClientId,
    ) -> ElementId {
        self.grant_scoped(subject, grant, effect, AclScope::Element(id), grantor)
    }

    /// Grant an ACL tuple: an allow/deny of `grant` (a capability or role) to
    /// `subject`, on `scope` (a fixed path or a stable element id), recorded with
    /// `grantor` (the authoring actor, passed explicitly). Returns its stable id —
    /// the handle to revoke it. Core stores the tuple faithfully; it enforces no
    /// authority over the grantor here.
    pub fn grant_scoped(
        &mut self,
        subject: AclSubject,
        grant: AclGrant,
        effect: AclEffect,
        scope: AclScope,
        grantor: ClientId,
    ) -> ElementId {
        let root = self.doc.root_id();
        // A refused mint grants nothing, so the handle names nothing — the same
        // unoccupiable-stamp convention a refused ranged create returns.
        let stamp = self
            .doc
            .emit_stamped(
                root,
                OpKind::AclGrant {
                    subject,
                    grant,
                    effect,
                    scope,
                    grantor,
                },
            )
            .unwrap_or_else(|| unmintable_stamp(self.doc.client));
        acl_id(stamp)
    }

    /// Revoke an ACL tuple, tombstoning it. Emits nothing for an id this replica
    /// has not yet materialised (a local apply would no-op while still
    /// broadcasting, diverging the author from a peer that applied it against the
    /// present entry).
    pub fn revoke(&mut self, id: ElementId) {
        if !self.doc.acl.contains_key(&id) {
            return;
        }
        let root = self.doc.root_id();
        self.doc.emit(root, OpKind::AclRevoke { id });
    }
}

/// A cursor over one `XmlElement`. [`attrs`](Self::attrs) descends into its attrs
/// Map, [`children`](Self::children) into its children sequence.
pub struct XmlCursor<'a> {
    doc: &'a mut Document,
    xml_id: ElementId,
}

impl XmlCursor<'_> {
    /// This element's stable id — the handle to move it or address it later.
    pub fn id(&self) -> ElementId {
        self.xml_id
    }

    /// A cursor over this element's attrs Map, holding any CRDT values.
    pub fn attrs(&mut self) -> MapCursor<'_> {
        MapCursor {
            doc: self.doc,
            map_id: XmlElement::attrs_id(self.xml_id),
        }
    }

    /// A cursor over this element's children sequence.
    pub fn children(&mut self) -> XmlChildrenCursor<'_> {
        XmlChildrenCursor {
            doc: self.doc,
            list_id: XmlElement::children_id(self.xml_id),
        }
    }
}

/// A cursor over one `XmlFragment` — tagless and attr-less, so it exposes only a
/// children sequence. No `attrs` method: a mistaken attr write is a compile
/// error, not silent data loss.
pub struct XmlFragmentCursor<'a> {
    doc: &'a mut Document,
    children_id: ElementId,
}

impl XmlFragmentCursor<'_> {
    /// A cursor over this fragment's children sequence.
    pub fn children(&mut self) -> XmlChildrenCursor<'_> {
        XmlChildrenCursor {
            doc: self.doc,
            list_id: self.children_id,
        }
    }
}

/// A cursor over an XML children sequence — the ordered `XmlElement`/`Text` runs
/// under an element or fragment.
pub struct XmlChildrenCursor<'a> {
    doc: &'a mut Document,
    list_id: ElementId,
}

impl XmlChildrenCursor<'_> {
    /// Emit an `XmlInsertChild` for a child of `kind` at live `index`, returning
    /// the child's derived id.
    ///
    /// Two cases emit nothing and hand back an id derived from an unoccupiable
    /// stamp, so the cursor built on it addresses nothing and every edit through it
    /// is a no-op: the children List is not materialised (an op the author never
    /// applied would diverge a peer that has the List — unreachable through the
    /// public API, since a cursor is only handed out for a List a create already
    /// registered), or the mint refused for want of an id
    /// ([`Document::mint_refused`]).
    fn insert_child(&mut self, index: usize, tag: Option<Vec<u8>>, kind: ElementKind) -> ElementId {
        let absent = unmintable_stamp(self.doc.client);
        let anchor = match self.doc.lists.get(&self.list_id) {
            Some(list) => list.borrow().place(index),
            None => return xml_child_id(self.list_id, absent, kind),
        };
        let stamp = self
            .doc
            .emit_stamped(self.list_id, OpKind::XmlInsertChild { tag, anchor })
            .unwrap_or(absent);
        xml_child_id(self.list_id, stamp, kind)
    }

    /// Insert an `XmlElement` child with `tag` at `index`, returning a cursor over
    /// the new child.
    pub fn insert_element(&mut self, index: usize, tag: &[u8]) -> XmlCursor<'_> {
        let xml_id = self.insert_child(index, Some(tag.to_vec()), ElementKind::XmlElement);
        XmlCursor {
            doc: self.doc,
            xml_id,
        }
    }

    /// Insert a `Text` child (a text run) at `index`, returning a cursor over it.
    pub fn insert_text(&mut self, index: usize) -> TextCursor<'_> {
        let text_id = self.insert_child(index, None, ElementKind::Text);
        TextCursor {
            doc: self.doc,
            text_id,
        }
    }

    /// Tombstone the live child at `index`. Reuses the List delete on the same
    /// children sequence.
    pub fn delete(&mut self, index: usize) {
        let id = match self.doc.lists.get(&self.list_id) {
            Some(list) => list.borrow().node_at(index),
            None => return,
        };
        if let Some(id) = id {
            self.doc.emit(self.list_id, OpKind::ListDelete { id });
        }
    }

    /// The number of live children.
    pub fn len(&self) -> usize {
        self.doc
            .lists
            .get(&self.list_id)
            .map_or(0, |l| l.borrow().len())
    }

    /// Whether the sequence has no live children.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    /// Tombstone the node with `id` directly, when the caller already knows the
    /// stable id rather than a shifting index.
    pub fn delete_id(&mut self, id: Stamp) {
        let present =
            matches!(self.doc.lists.get(&self.list_id), Some(list) if list.borrow().contains(id));
        if present {
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

    /// Tombstone the codepoints with these char_ids directly, when the caller
    /// already knows the stable ids rather than a shifting index.
    pub fn delete_ids(&mut self, ids: &[Stamp]) {
        let present = matches!(
            self.doc.texts.get(&self.text_id),
            Some(text) if ids.iter().any(|id| text.borrow().contains(*id))
        );
        if present {
            self.doc
                .emit(self.text_id, OpKind::TextDelete { ids: ids.to_vec() });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cid(first: u8) -> ClientId {
        let mut b = [0u8; 16];
        b[0] = first;
        ClientId::from_bytes(b)
    }

    /// A snapshot whose move log folds the parent relation into a cycle is
    /// rejected at decode, not left to hang a later `resolvable` walk. Replay and
    /// re-fold mutate `parents` after decode's first cycle check, so `restore_moves`
    /// re-checks; here a placement is corrupted so one node's base can't derive —
    /// the move guard's ancestor walk misses the loop through it, but the recheck
    /// on the folded relation catches it. This state is unreachable through honest
    /// ops (the move guard never records a cycle), so the test builds it directly.
    #[test]
    fn a_move_log_that_folds_into_a_parent_cycle_is_rejected() {
        let mut d = Document::new(cid(1));
        let mut a_id = ElementId::from_bytes([0u8; 16]);
        let mut x_id = a_id;
        let mut grand_id = a_id;
        d.transact(|tx| {
            let mut frag = tx.xml_fragment(b"doc");
            let mut kids = frag.children();
            let mut a = kids.insert_element(0, b"a");
            a_id = a.id();
            let mut ac = a.children();
            let mut x = ac.insert_element(0, b"x");
            x_id = x.id();
            let mut xc = x.children();
            grand_id = xc.insert_element(0, b"grand").id();
        });

        // Break x's base: repoint its stored placement at a's children list, where
        // `(list, stamp)` no longer re-derives x, so `restore_moves` finds no birth
        // placement for x and sets no base for it.
        let a_list = d.placements[&a_id][0].list;
        let x_stamp = d.placements[&x_id][0].stamp;
        d.moves = TreeMoves::new();
        d.placements.insert(
            x_id,
            vec![Placement {
                list: a_list,
                stamp: x_stamp,
            }],
        );

        // Move a under grand. With x's base missing the guard's walk grand → x
        // stops short of a, so the move is applied; the fold then points
        // a → children(grand) → grand → children(x) → x → children(a) → a.
        let mv = Stamp {
            lamport: 1_000,
            client: cid(1),
            offset: 0,
        };
        assert!(d.restore_moves(&[(mv, a_id, grand_id)]).is_err());
    }
}
