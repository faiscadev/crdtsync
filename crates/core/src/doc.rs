//! Document — a replica: a tree of containers rooted at a well-known id, a
//! lamport clock, and the transact/apply seam.
//!
//! A `transact` mutates the live tree through a cursor and returns the ops it
//! emitted; `apply` folds a foreign op back in. Ops are keyed by `(client,
//! seq)` for idempotent dedup and ordered by their stamp for LWW. Each op
//! names its target container by id, resolved against a registry of every
//! container the replica has materialised. That registry retains displaced
//! containers, so a slot re-won after displacement is the same logical
//! element: identity persists across displacement. An op whose target isn't
//! reachable yet — its parent unseen, or an ancestor displaced — is buffered
//! and replays once a create restores reachability, so out-of-order delivery
//! converges.

use crate::acl::{AclEffect, AclGrant, AclRecord, AclScope, AclSubject, AclTuple};
use crate::anchor::RelativePosition;
use crate::clientid::ClientId;
use crate::codec::{
    decode_ops, encode_ops, len_u32, put_acl_effect, put_acl_grant, put_acl_scope, put_acl_subject,
    put_bytes, put_opt_bytes, put_range_anchor, put_scalar, put_stamp, put_u32, put_u64, put_u8,
    Cursor, DecodeError,
};
use crate::counter::Counter;
use crate::element::Element;
use crate::elementid::{ElementId, ElementKind};
use crate::list::{Anchor, List};
use crate::map::{DecodedMap, Map, SlotValue};
use crate::marks::{MarkState, ResolvedMark};
use crate::op::{Op, OpId, OpKind, Tx, TxId};
use crate::ranged::{RangeAnchor, RangedElement, RangedInit, RangedPayload};
use crate::repair::{keyed_repairs, Repair, RepairId};
use crate::scalar::Scalar;
use crate::schema::{MarkFlavor, Schema};
use crate::stamp::Stamp;
use crate::text::Text;
use crate::treemove::TreeMoves;
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

/// The well-known root slot every replica shares, so children derive under the
/// same parent.
const ROOT_ID: [u8; 16] = *b"crdtsync\0\0\0\0root";

/// The snapshot format version: a reader rejects any stream not stamped with it,
/// so a format change can never be misread as the current one.
const STATE_VERSION: u8 = 11;

/// A composite that a mutation displaced from its slot and left unreachable.
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
    /// The `(list, stamp)` of every placement, so a delete can tell in O(1)
    /// whether it tombstoned a movable node — and re-fold only then, not on every
    /// plain-list delete.
    placement_index: HashSet<(ElementId, Stamp)>,
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
    /// The lamport clock of the root partition — the zone every unzoned target
    /// (and every op of a document with no zones) is stamped from. With no zones
    /// this is the document's whole lamport clock, exactly as before zones.
    lamport: u64,
    /// The per-zone lamport clocks, keyed by compact zone id
    /// ([`zone::zone_id_of`]). Each declared zone advances its own clock, so an op
    /// in one zone never bumps another's — the partitions are causally independent,
    /// the property that lets each zone later replicate as its own stream. A zone
    /// absent here has clock 0 (never yet stamped). The root partition is `lamport`
    /// above, not an entry here; an empty map is a document behaving exactly as one
    /// with no zones.
    zone_clocks: HashMap<u32, u64>,
    seq: u64,
    /// The next atomic-transaction id to mint; namespaced by this replica's
    /// client, so `(client, tx)` is globally unique.
    next_tx: u64,
    /// When recording an atomic transaction (between `begin_atomic` and
    /// `commit_atomic`), the ops emitted so far accumulate here instead of being
    /// returned per edit, so several edits commit as one group.
    atomic: Option<Vec<Op>>,
    seen: HashSet<OpId>,
    /// Ops whose target isn't reachable yet, held until a create makes it so.
    buffer: Vec<Op>,
    buffered: HashSet<OpId>,
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
            placement_index: HashSet::new(),
            ranged: HashMap::new(),
            acl: HashMap::new(),
            lamport: 0,
            zone_clocks: HashMap::new(),
            seq: 0,
            next_tx: 0,
            atomic: None,
            seen: HashSet::new(),
            buffer: Vec::new(),
            buffered: HashSet::new(),
            orphans: Vec::new(),
            pending: Vec::new(),
            schema: None,
            repair_baseline: Vec::new(),
        }
    }

    pub fn client(&self) -> ClientId {
        self.client
    }

    /// The ids of every op this replica has applied — the dedup set, so a
    /// reconstructing server can restore its own dedup from a decoded snapshot.
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
    pub fn ranged_anchors(&self) -> HashMap<ElementId, (ElementId, ElementId)> {
        self.ranged
            .iter()
            .map(|(id, e)| (*id, (e.start.seq, e.end.seq)))
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

    /// Whether `key` in `map_id` currently holds a retained container of any
    /// key-derived kind (map / list / text) — the identity a leaf migration must
    /// not disturb.
    fn holds_any_container(&self, map_id: ElementId, key: &[u8]) -> bool {
        [ElementKind::Map, ElementKind::List, ElementKind::Text]
            .into_iter()
            .any(|kind| self.container_handle(map_id, key, kind).is_some())
    }

    /// The retained handle of the `kind` container `key` in `map_id` derives —
    /// the exact element a snapshot migration resurrects at the old key, chosen by
    /// the kind the deleted-container tombstone recorded (a key that hosted more
    /// than one kind keeps each registered, so the recorded kind disambiguates).
    /// `None` for a non-key-derived (XML) kind or one never created.
    fn container_handle(
        &self,
        map_id: ElementId,
        key: &[u8],
        kind: ElementKind,
    ) -> Option<Element> {
        let id = ElementId::derive(map_id, key, kind);
        match kind {
            ElementKind::Map => self.maps.get(&id).map(|m| Element::Map(Rc::clone(m))),
            ElementKind::List => self.lists.get(&id).map(|l| Element::List(Rc::clone(l))),
            ElementKind::Text => self.texts.get(&id).map(|t| Element::Text(Rc::clone(t))),
            _ => None,
        }
    }

    /// Migrate a snapshot's slots by `fate`, keyed on the slot key — the
    /// state-level analogue of translating the op stream between two schema
    /// versions, so a snapshot-served joiner converges byte-for-byte with a peer
    /// served the same history as a translated op delta. Across every map, each
    /// leaf slot (scalar / register / counter, live or tombstoned) is `Keep`t,
    /// `Drop`ped, or `Rename`d to a new key per `fate`. A live container slot (map
    /// / list / text) is carried verbatim, mirroring the op seam, which carries a
    /// container-create verbatim rather than tear its subtree. A *deleted*
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
    /// A deleted container whose create identity did not survive — a re-created
    /// key a scalar or counter later displaced — cannot be resurrected faithfully
    /// and is carried verbatim rather than mis-migrated as a leaf. An XML kind,
    /// whose id derives by node rather than key, is not resurrectable here and
    /// records no create identity; its deleted slot migrates as a leaf tombstone,
    /// the pre-existing behaviour, faithful XML-field migration being out of scope.
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
                // and is only ever set on such a tombstone, so its presence
                // alongside a resolvable handle is the whole condition. The counter
                // registry at the key re-homes / prunes alongside via the same
                // machinery as a leaf, a separate identity from the container.
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
                // one, or a tombstoned deleted one whose container identity the
                // registry still holds but whose create-stamp is gone (a re-created
                // key that a scalar or counter later displaced, so a faithful
                // resurrection is impossible). The COUNTER registry at the key's
                // derived id migrates regardless: it is a separate identity from the
                // slot body and from any container at the key, retained across
                // displacement, so it must prune / re-home even when the slot is
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

        // The buffer is a framed op log, itself length-prefixed so the reader
        // knows where it ends inside the document stream.
        let framed = encode_ops(&self.buffer);
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
    /// Sound only as the final transform before [`encode_state`](Self::encode_state)
    /// on a throwaway copy: it clears the causal `seen` frontier (a below-floor
    /// subscriber only ever ingests ops after the snapshot's sequence, so it needs
    /// no prior dedup, and an emptied frontier leaks no op count of the hidden
    /// partition) and leaves the derived move relation filtered only in its
    /// persisted log — neither is a valid live-replica state. A schema with no zones
    /// leaves the document untouched.
    pub fn project_zones(&mut self, schema: &Schema, authorized: &HashSet<u32>) {
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
        self.ranged.retain(|id, e| {
            !purge.contains(id) && !purge.contains(&e.start.seq) && !purge.contains(&e.end.seq)
        });
        // ACL tuples are keyed by their own id, not a container's, so they are
        // dropped by the zone their scope resolves into (an unauthorized zone's
        // grants would leak its path) as well as by a purged id. An element scope
        // resolves through the live tree's `paths`; a scope that resolves to no key
        // sequence (an unresolvable element, a malformed path) names no zone and is
        // kept, as an unzoned grant is.
        self.acl.retain(|id, e| {
            if purge.contains(id) {
                return false;
            }
            let keys = match &e.scope {
                AclScope::Path(p) => crate::path::parse_path(p),
                AclScope::Element(eid) => paths.get(eid).cloned(),
            };
            match keys.and_then(|keys| zone::zone_id_of(schema, &keys)) {
                Some(zone) => authorized.contains(&zone),
                None => true,
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
            .values()
            .flat_map(|places| places.iter().map(|p| (p.list, p.stamp)))
            .collect();
        self.moves
            .retain(|child, parent| !purge.contains(&child) && !purge.contains(&parent));
        self.zone_clocks.retain(|zone, _| authorized.contains(zone));
        // The causal frontier and buffer, scrubbed of the hidden partition: `seen`
        // is emptied (a below-floor subscriber dedups nothing before the snapshot
        // sequence, and an emptied set carries no op count), and buffered ops are
        // filtered to the authorized partitions.
        self.seen.clear();
        self.buffer
            .retain(|op| op.zone.is_none_or(|zone| authorized.contains(&zone)));
        self.buffered = self.buffer.iter().map(|op| op.id).collect();
    }

    /// Project this replica in place to the paths a reader may read, dropping every
    /// element `reads` does not admit so a re-encode carries no trace of it — not the
    /// hidden subtree's content, structure, ids, or the ACL grants that would reveal who
    /// else may read it. `reads` is the server's composed doc-ACL read verdict at a
    /// `core::path` key sequence — the exact per-path authority the per-op fan-out gates
    /// each op on (the server's `op_read_path` resolves an op to this same path). This is
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
    /// endpoint). A whole-document reader is left untouched, byte-identical on re-encode.
    ///
    /// Sound only as the final transform before [`encode_state`](Self::encode_state) on
    /// a throwaway copy: like [`project_zones`](Self::project_zones) it clears the causal
    /// `seen` frontier and the buffer once anything is dropped (a below-floor subscriber
    /// dedups nothing before the snapshot's sequence, and an emptied frontier leaks no op
    /// count of a hidden subtree) and leaves the derived move relation filtered only in
    /// its persisted log — neither a valid live-replica state.
    pub fn project_read_paths(&mut self, reads: impl Fn(&[Vec<u8>]) -> bool) {
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
            let Some(birth) = birth_placement(*node, places, kind) else {
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
        // mirrors the op-stream rule (op_read_path maps an AclGrant to its scope's path), so a
        // snapshot-served partial reader materializes the same ACL subset an op-served one
        // would. A `Path` scope is the encoded key path; an `Element` scope resolves to its
        // element's current path through `paths` (the grant follows the element). An `Element`
        // scope that does not resolve (an unresolvable element id) falls back to root read —
        // the same fallback the op-stream takes (`op_read_path` gates it at root), so an
        // unresolvable-element tuple reaches exactly the readers on either seam and the two
        // catch-ups stay convergent. A malformed `Path` fails closed (dropped).
        let acl_before = self.acl.len();
        self.acl.retain(|id, e| {
            if purge.contains(id) {
                return false;
            }
            match &e.scope {
                AclScope::Path(p) => crate::path::parse_path(p).is_some_and(|segs| reads(&segs)),
                AclScope::Element(eid) => paths.get(eid).map_or(root_reads, |segs| reads(segs)),
            }
        });
        let acl_cut = self.acl.len() != acl_before;
        // A RangedElement is redacted by the path of EVERY sequence its endpoints
        // anchor — a require-all rule — since a mark/annotation reveals content-region
        // info at both endpoints: a reader that cannot read where the range starts OR
        // ends must not materialize it. A single-sequence mark has one governing path;
        // a cross-element range has two, and both must read. This mirrors the op-stream
        // rule (op_read_paths gates each Ranged op on its distinct anchor seq paths), so
        // a snapshot-served partial reader materializes the same RangedElement subset an
        // op-served one does. An anchor seq the walk does not resolve (a since-deleted
        // sequence) falls back to root read, so only a whole-document reader keeps it.
        let ranged_before = self.ranged.len();
        let anchor_reads = |seq: ElementId| match paths.get(&seq) {
            Some(p) => reads(p),
            None => root_reads,
        };
        self.ranged.retain(|id, e| {
            !purge.contains(id) && anchor_reads(e.start.seq) && anchor_reads(e.end.seq)
        });
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
            .values()
            .flat_map(|places| places.iter().map(|p| (p.list, p.stamp)))
            .collect();
        self.moves.retain(|child, parent| {
            !purge.contains(&child)
                && !purge.contains(&parent)
                && !list_denied(&XmlElement::children_id(parent))
        });
        // Once anything is dropped, scrub the causal frontier and buffer of the hidden
        // state so neither leaks an op count, and rebuild the tree-move fold so the
        // derived parents and `moved_away` overlay match the filtered log a reload
        // replays — a node kept at its readable origin renders there, not at the denied
        // destination it was folded to, so the projected snapshot is byte-stable through
        // a round-trip. A pure identity projection (a whole-document reader) leaves both
        // untouched, staying byte-identical on re-encode.
        if !purge.is_empty() || cut_leaf || acl_cut || ranged_cut || !root_reads {
            self.refold_projected_moves();
            self.seen.clear();
            self.buffer.clear();
            self.buffered.clear();
        }
    }

    /// Rebuild the tree-move fold on a projected copy so its derived parent relation and
    /// `moved_away` overlay match the filtered move log a reload replays — the same
    /// reconstruction [`restore_moves`](Self::restore_moves) runs on decode, minus the
    /// birth scan (every surviving placement is already recorded) and the cycle re-check
    /// (the pre-projection tree was acyclic and filtering only removes edges). A node
    /// whose move into a denied subtree was filtered out re-folds back under its readable
    /// origin here, so the live copy renders it where a decoded joiner will. Sound only
    /// as the final transform before [`encode_state`](Self::encode_state).
    fn refold_projected_moves(&mut self) {
        // A document with no placements is a document with no tree moves, so its fold is
        // already trivial — skip the rebuild rather than pay it on every non-XML snapshot.
        if self.placements.is_empty() {
            return;
        }
        let log: Vec<(Stamp, ElementId, ElementId)> = self.moves.log().collect();
        let bases: Vec<(ElementId, ElementId)> = self
            .placements
            .iter()
            .filter_map(|(node, places)| {
                let kind = self.node_kind(*node)?;
                let birth = birth_placement(*node, places, kind)?;
                Some((*node, *self.parents.get(&birth.list)?))
            })
            .collect();
        self.moves = TreeMoves::new();
        for (node, owner) in bases {
            self.moves.set_base(node, owner);
        }
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

    /// The next per-client op sequence this replica will mint — its op-seq
    /// high-water mark.
    pub fn next_seq(&self) -> u64 {
        self.seq
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
    /// with an op counter no lower than `next_seq`, rather than the identity and
    /// counter the snapshot was encoded with. A replica adopting a snapshot
    /// keeps its own identity and its own op-seq high-water mark, so it never
    /// re-mints an `OpId` it already made durable (which a peer would dedup away,
    /// diverging silently).
    pub fn decode_state_as(
        client: ClientId,
        next_seq: u64,
        bytes: &[u8],
    ) -> Result<Document, DecodeError> {
        let mut doc = Document::decode_state(bytes)?;
        doc.client = client;
        doc.seq = doc.seq.max(next_seq);
        Ok(doc)
    }

    fn read_state(cur: &mut Cursor) -> Result<Document, DecodeError> {
        let version = cur.u8()?;
        if version != STATE_VERSION {
            return Err(DecodeError::BadTag {
                what: "document state version",
                tag: version,
            });
        }
        let client = cur.client()?;
        let lamport = cur.u64()?;
        let seq = cur.u64()?;

        let zone_clock_count = cur.u32()?;
        let mut zone_clocks: HashMap<u32, u64> =
            HashMap::with_capacity((zone_clock_count as usize).min(1024));
        for _ in 0..zone_clock_count {
            let zone = cur.u32()?;
            let lamport = cur.u64()?;
            if zone_clocks.insert(zone, lamport).is_some() {
                return Err(DecodeError::BadTag {
                    what: "document: duplicate zone clock",
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
            let node = cur.element_id()?;
            let parent = cur.element_id()?;
            move_log.push((stamp, node, parent));
        }
        let placed_count = cur.u32()?;
        let mut placements: HashMap<ElementId, Vec<Placement>> =
            HashMap::with_capacity((placed_count as usize).min(1024));
        let mut placement_index: HashSet<(ElementId, Stamp)> = HashSet::new();
        for _ in 0..placed_count {
            let node = cur.element_id()?;
            let n = cur.u32()?;
            let mut places = Vec::with_capacity((n as usize).min(1024));
            for _ in 0..n {
                let list = cur.element_id()?;
                let stamp = cur.stamp()?;
                if !placement_index.insert((list, stamp)) {
                    return Err(DecodeError::BadTag {
                        what: "document: duplicate placement",
                        tag: 0,
                    });
                }
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
                0 => Payload::Scalar {
                    value: cur.scalar()?,
                    stamp: cur.stamp()?,
                },
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
                if m.insert_decoded(slot.key, slot.stamp, value, slot.tombstone, slot.deleted) {
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
        // Following parents must terminate: a cycle would hang `resolvable` on a
        // later op. Memoize chains already proven to terminate so the walk stays
        // linear over an untrusted graph.
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

        let buf_len = cur.u32()? as usize;
        let framed = cur.take(buf_len)?;
        let buffer = decode_ops(framed)?;
        // A buffered op that is already applied, or repeated, would be replayed
        // by `drain_buffer` (which applies unconditionally): reject both.
        let mut buffered = HashSet::with_capacity(buffer.len().min(1024));
        for op in &buffer {
            if seen.contains(&op.id) || !buffered.insert(op.id) {
                return Err(DecodeError::BadTag {
                    what: "document: buffered op already applied or repeated",
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
            seq,
            // Tx ids scope only the buffering of remote partial transactions,
            // keyed by their author's client; a restored replica mints its own
            // under its own client with fresh op ids, so restarting at 0 cannot
            // collide with anything still buffered.
            next_tx: 0,
            atomic: None,
            seen,
            buffer,
            buffered,
            orphans: Vec::new(),
            pending: Vec::new(),
            schema: None,
            repair_baseline: Vec::new(),
        };
        // Rebuild the tree-move state: the created-under parent of each placed
        // node (for the cycle check + fallback parent), then replay the move log,
        // then re-fold so `moved_away` reflects the effective tree.
        doc.restore_moves(&move_log)?;

        // The buffer holds only ops still waiting on their target; a well-formed
        // snapshot already satisfies that, so this is a no-op there. Draining
        // restores the invariant for any op decoded as already reachable rather
        // than leaving it stuck until an unrelated mutation.
        doc.drain_buffer();
        Ok(doc)
    }

    /// Restore the tree-move overlay from a decoded snapshot: reconstruct the
    /// birth placement of every never-moved node from its live children-list
    /// node (only moved nodes are stored explicitly), re-derive each placed
    /// node's base (created-under) parent from its birth placement — the one
    /// whose `(list, stamp)` re-derives the node's own id — then replay the move
    /// log and re-fold. Finally re-check the parent relation for a cycle: replay
    /// and re-fold mutate `parents` after decode's first check, so a crafted
    /// snapshot whose moves fold into a cycle is rejected here rather than
    /// hanging a later `resolvable` walk.
    fn restore_moves(&mut self, log: &[(Stamp, ElementId, ElementId)]) -> Result<(), DecodeError> {
        self.reconstruct_births();
        let bases: Vec<(ElementId, ElementId)> = self
            .placements
            .iter()
            .filter_map(|(node, places)| {
                let kind = self.node_kind(*node)?;
                let birth = birth_placement(*node, places, kind)?;
                let owner = self.parents.get(&birth.list)?;
                Some((*node, *owner))
            })
            .collect();
        for (node, owner) in bases {
            self.moves.set_base(node, owner);
        }
        for &(stamp, node, parent) in log {
            self.moves.apply(stamp, node, parent);
        }
        self.refold_moves();
        reject_parent_cycles(&self.parents, ElementId::from_bytes(ROOT_ID))
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
            if self.placements.contains_key(&node) {
                continue;
            }
            self.placements
                .entry(node)
                .or_default()
                .push(Placement { list, stamp });
            self.placement_index.insert((list, stamp));
        }
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
    /// accumulates into one group and returns no ops of its own. Idempotent while
    /// already recording (the open group continues). Pair with `commit_atomic`.
    pub fn begin_atomic(&mut self) {
        if self.atomic.is_none() {
            self.atomic = Some(Vec::new());
        }
    }

    /// Close the atomic transaction opened by [`begin_atomic`] and return its ops,
    /// tagged as one group for all-or-nothing delivery. Returns empty (and tags
    /// nothing) if no edits were recorded or no transaction was open.
    pub fn commit_atomic(&mut self) -> Vec<Op> {
        let ops = self.atomic.take().unwrap_or_default();
        self.tag_atomic(ops)
    }

    /// Whether an atomic transaction is currently open.
    pub fn is_atomic(&self) -> bool {
        self.atomic.is_some()
    }

    /// Tag a group's ops as one atomic transaction. An empty group is left
    /// untagged.
    fn tag_atomic(&mut self, ops: Vec<Op>) -> Vec<Op> {
        let Ok(count) = u32::try_from(ops.len()) else {
            return ops;
        };
        if count == 0 {
            return ops;
        }
        let id = TxId(self.next_tx);
        self.next_tx += 1;
        ops.into_iter()
            .map(|mut op| {
                op.tx = Some(Tx { id, count });
                op
            })
            .collect()
    }

    /// Like [`transact`](Self::transact), but tag the emitted ops as one atomic
    /// transaction. A receiver holds the members until the whole group arrives,
    /// then applies them together, so no peer observes a partial transaction. The
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

    /// Fold a foreign op into local state. An op whose target isn't reachable
    /// yet is buffered and returns `false`; it replays once a create makes the
    /// target reachable. Returns `false` for an already-applied or already-held
    /// op. Returns `true` only when the op is applied now.
    pub fn apply(&mut self, op: &Op) -> bool {
        if self.seen.contains(&op.id) || self.buffered.contains(&op.id) {
            return false;
        }
        // An atomic-transaction member is always held first; its group commits
        // together once every member is present and the group's external
        // dependencies resolve. A lone (single-member) tx completes immediately.
        if op.tx.is_some() {
            self.buffered.insert(op.id);
            self.buffer.push(op.clone());
            self.drain_buffer();
            return self.seen.contains(&op.id);
        }
        if !self.ready(op) {
            self.buffered.insert(op.id);
            self.buffer.push(op.clone());
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
        let last = op.stamp.lamport.saturating_add(span(&op.kind) - 1);
        self.advance_clock(op.zone, last);
        self.apply_kind(op.target, &op.kind, op.stamp, op.id.client);
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
    /// the structural path, so it is a pure function of the shared schema and the
    /// tree — every replica assigns the same op to the same partition.
    ///
    /// A container-create belongs to the partition of the *child* it installs, not
    /// the parent it targets: the child's path is the parent's extended by the
    /// created key, so a zone owns the creation of its own root container. Without
    /// this the zone-root create would ride the parent partition and reach a
    /// subscriber not authorized to the zone (the parent partition is one it does
    /// see), materializing an empty zone-root container for it — and diverging from
    /// the snapshot projection, which drops that container. With it the create is
    /// stamped in the zone, withheld from an unauthorized subscriber on every seam.
    /// Every other op belongs to the partition of the container it targets.
    fn zone_of_op(&self, target: ElementId, kind: &OpKind) -> Option<u32> {
        let schema = self.schema.as_ref()?;
        if schema.zones().is_empty() {
            return None;
        }
        let paths = self.element_paths();
        let mut path = paths.get(&target)?.clone();
        if let Some(key) = create_child_key(kind) {
            path.push(key.to_vec());
        }
        zone::zone_id_of(schema, &path)
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
            // One complete atomic transaction: apply every member in seq order,
            // so a member that targets a container an earlier member creates
            // lands after it.
            if let Some(mut members) = self.take_complete_tx() {
                members.sort_by_key(|op| op.id.seq);
                for op in &members {
                    self.buffered.remove(&op.id);
                    self.apply_now(op);
                }
                progressed = true;
            }
            if !progressed {
                break;
            }
        }
    }

    /// Whether `op` can apply now: its target is reachable, and — for a delete —
    /// the nodes it removes are present. A delete of a not-yet-inserted node
    /// would silently no-op and be lost, so it waits for the insert.
    fn ready(&self, op: &Op) -> bool {
        if !self.resolvable(op.target) {
            return false;
        }
        match &op.kind {
            OpKind::ListDelete { id } => self
                .lists
                .get(&op.target)
                .is_some_and(|l| l.borrow().contains(*id)),
            OpKind::TextDelete { ids } => self.texts.get(&op.target).is_some_and(|t| {
                let t = t.borrow();
                ids.iter().all(|id| t.contains(*id))
            }),
            // A move waits until the node it relocates has been materialised — its
            // create may still be in flight — so the relocation is never lost.
            OpKind::XmlMove { node, .. } => self.placements.contains_key(node),
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

    /// Remove and return the members of one atomic transaction whose whole group
    /// is buffered and whose external dependencies resolve — or `None` if no
    /// buffered transaction is ready to commit.
    fn take_complete_tx(&mut self) -> Option<Vec<Op>> {
        let mut groups: HashMap<(ClientId, TxId), Vec<usize>> = HashMap::new();
        for (i, op) in self.buffer.iter().enumerate() {
            if let Some(tx) = &op.tx {
                groups.entry((op.id.client, tx.id)).or_default().push(i);
            }
        }
        let ready = groups.into_values().find(|idxs| {
            let members: Vec<&Op> = idxs.iter().map(|&i| &self.buffer[i]).collect();
            let count = members[0].tx.as_ref().map_or(0, |tx| tx.count) as usize;
            members.len() == count && self.tx_group_ready(&members)
        })?;
        // Remove in descending index order so earlier indices stay valid.
        let mut idxs = ready;
        idxs.sort_unstable_by(|a, b| b.cmp(a));
        Some(idxs.into_iter().map(|i| self.buffer.remove(i)).collect())
    }

    /// Whether a whole transaction can commit: every member either targets a
    /// container reachable now or one an earlier member creates, and every delete
    /// removes a node present now or inserted by an earlier member. Intra-group
    /// dependencies are satisfied by seq-order application, so they are counted
    /// as met here.
    fn tx_group_ready(&self, members: &[&Op]) -> bool {
        let mut ordered: Vec<&Op> = members.to_vec();
        ordered.sort_by_key(|op| op.id.seq);
        let mut created: HashSet<ElementId> = HashSet::new();
        let mut inserted: HashSet<Stamp> = HashSet::new();
        // Movable node ids a member inserts, so a later member's move of one is
        // judged ready — a move whose node has not yet been materialised must
        // wait, or apply_move drops it silently.
        let mut movable: HashSet<ElementId> = HashSet::new();
        // RangedElement ids a member creates, so a later member's payload change or
        // delete of one is judged ready — else the group would commit and the
        // change would apply against a missing entry and be lost.
        let mut created_ranged: HashSet<ElementId> = HashSet::new();
        // ACL tuple ids a member grants, so a later member's revoke of one is
        // judged ready — else the group commits and the revoke applies against a
        // missing entry and is lost.
        let mut created_acl: HashSet<ElementId> = HashSet::new();
        for op in &ordered {
            let target_ok = self.resolvable(op.target) || created.contains(&op.target);
            if !target_ok {
                return false;
            }
            match &op.kind {
                OpKind::MapCreate { key } => {
                    created.insert(ElementId::derive(op.target, key, ElementKind::Map));
                }
                OpKind::ListCreate { key } => {
                    created.insert(ElementId::derive(op.target, key, ElementKind::List));
                }
                OpKind::TextCreate { key } => {
                    created.insert(ElementId::derive(op.target, key, ElementKind::Text));
                }
                // An XML create installs a node whose attrs Map and children List
                // are the containers a later member of the same transaction
                // targets, so mark those reachable — not the node id itself,
                // which no op addresses directly.
                OpKind::XmlElementCreate { key, tag } => {
                    let node = XmlElement::node_id(op.target, key, tag);
                    created.insert(XmlElement::attrs_id(node));
                    created.insert(XmlElement::children_id(node));
                }
                OpKind::XmlFragmentCreate { key } => {
                    let node = XmlFragment::node_id(op.target, key);
                    created.insert(XmlFragment::children_id(node));
                }
                OpKind::XmlInsertChild { tag, .. } => {
                    // The node stamp so a later delete finds it, plus the child's
                    // targetable ids so a later member editing it is satisfied.
                    inserted.insert(op.stamp);
                    let kind = if tag.is_some() {
                        ElementKind::XmlElement
                    } else {
                        ElementKind::Text
                    };
                    let child = xml_child_id(op.target, op.stamp, kind);
                    movable.insert(child);
                    if tag.is_some() {
                        created.insert(XmlElement::attrs_id(child));
                        created.insert(XmlElement::children_id(child));
                    } else {
                        created.insert(child);
                    }
                }
                OpKind::ListInsert { .. } => {
                    inserted.insert(op.stamp);
                }
                OpKind::TextInsert { s, .. } => {
                    for k in 0..s.chars().count() as u64 {
                        inserted.insert(Stamp {
                            lamport: op.stamp.lamport.saturating_add(k),
                            client: op.stamp.client,
                        });
                    }
                }
                OpKind::ListDelete { id } => {
                    let present = inserted.contains(id)
                        || self
                            .lists
                            .get(&op.target)
                            .is_some_and(|l| l.borrow().contains(*id));
                    if !present {
                        return false;
                    }
                }
                OpKind::TextDelete { ids } => {
                    let present = ids.iter().all(|id| {
                        inserted.contains(id)
                            || self
                                .texts
                                .get(&op.target)
                                .is_some_and(|t| t.borrow().contains(*id))
                    });
                    if !present {
                        return false;
                    }
                }
                OpKind::XmlMove { node, .. } => {
                    // The moved node must already exist or be inserted by an
                    // earlier member — else the group would commit and apply_move
                    // would drop the move against a not-yet-materialised node.
                    if !self.placements.contains_key(node) && !movable.contains(node) {
                        return false;
                    }
                }
                OpKind::RangedCreate { payload, .. } => {
                    let rid = ranged_id(op.stamp);
                    created_ranged.insert(rid);
                    // A composite create installs the payload container a later
                    // member may target — mark it reachable within the group.
                    if let RangedInit::Composite(kind) = payload {
                        created.insert(payload_id(rid, *kind));
                    }
                }
                OpKind::RangedSetPayload { id, .. } | OpKind::RangedDelete { id } => {
                    if !self.ranged.contains_key(id) && !created_ranged.contains(id) {
                        return false;
                    }
                }
                OpKind::AclGrant { .. } => {
                    created_acl.insert(acl_id(op.stamp));
                }
                OpKind::AclRevoke { id } => {
                    if !self.acl.contains_key(id) && !created_acl.contains(id) {
                        return false;
                    }
                }
                _ => {}
            }
        }
        true
    }

    /// Mint identity + causal position for a local edit, apply it, and record
    /// the op on the in-progress transact.
    fn emit(&mut self, target: ElementId, kind: OpKind) {
        let _ = self.emit_stamped(target, kind);
    }

    /// Like [`emit`](Self::emit), returning the stamp minted for the op — so a
    /// caller that creates a stamp-keyed child (an XML sequence child) can derive
    /// its id without re-minting.
    fn emit_stamped(&mut self, target: ElementId, kind: OpKind) -> Stamp {
        // The op is stamped from its own partition's clock, so an edit in one zone
        // never advances another's and the op carries which partition it belongs
        // to. The target already exists (a mutation names a materialised
        // container), so its zone — the created child's, for a container-create —
        // resolves now.
        let zone = self.zone_of_op(target, &kind);
        let base = self.clock(zone) + 1;
        let stamp = Stamp {
            lamport: base,
            client: self.client,
        };
        // Reserve the rest of a run's char_ids so the next op sorts after it.
        let last = base + (span(&kind) - 1);
        self.advance_clock(zone, last);
        let id = OpId {
            client: self.client,
            seq: self.seq,
        };
        self.seq += 1;
        self.seen.insert(id);
        let author = self.client;
        self.apply_kind(target, &kind, stamp, author);
        self.pending.push(Op {
            id,
            stamp,
            target,
            kind,
            tx: None,
            zone,
        });
        stamp
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

    /// A live list handle for `target`, if any.
    fn live_list(&self, target: ElementId) -> Option<Rc<RefCell<List>>> {
        self.lists
            .get(&target)
            .filter(|l| !l.borrow().is_displaced())
            .cloned()
    }

    /// A live text handle for `target`, if any.
    fn live_text(&self, target: ElementId) -> Option<Rc<RefCell<Text>>> {
        self.texts
            .get(&target)
            .filter(|t| !t.borrow().is_displaced())
            .cloned()
    }

    /// Route a mutation to its target, recording any displaced composite and
    /// registering any container it creates.
    fn apply_kind(&mut self, target: ElementId, kind: &OpKind, stamp: Stamp, author: ClientId) {
        match kind {
            // Sequence and text ops address a list or text directly.
            OpKind::ListInsert { value, anchor } => {
                if let Some(list) = self.live_list(target) {
                    list.borrow_mut()
                        .insert_at(stamp, Element::Scalar(value.clone()), *anchor);
                }
                return;
            }
            OpKind::ListDelete { id } => {
                if let Some(list) = self.live_list(target) {
                    list.borrow_mut().delete_id(*id);
                }
                // A delete of a moved node's placement makes the delete win over a
                // concurrent move, so re-fold to hide every placement of that
                // node. Only a delete that tombstoned a real placement can change
                // the fold, so a plain-list delete skips the O(placements) work.
                if self.placement_index.contains(&(target, *id)) {
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
                if let Some(text) = self.live_text(target) {
                    text.borrow_mut().insert_run(stamp, s, *anchor);
                }
                return;
            }
            OpKind::TextDelete { ids } => {
                if let Some(text) = self.live_text(target) {
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
        if map.borrow().is_displaced() {
            return;
        }
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
        if map.borrow().is_displaced() {
            return;
        }
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
        if let Some(o) = orphan {
            self.orphans.push(o);
        }
    }

    /// Install a child in an XML children List: materialise the child container
    /// (an `XmlElement` when `tag` is present, else a `Text` run), register it,
    /// and insert its handle as a sequence node keyed by the op's stamp. The
    /// child's element id derives from that stamp, so every replica builds the
    /// same child; `insert_at` is idempotent on the stamp, so a replay is inert.
    fn insert_xml_child(
        &mut self,
        list_id: ElementId,
        tag: Option<Vec<u8>>,
        anchor: Anchor,
        stamp: Stamp,
    ) {
        let Some(list) = self.live_list(list_id) else {
            return;
        };
        let (kind, container) = match tag {
            Some(t) => (ElementKind::XmlElement, Container::XmlElement(t)),
            None => (ElementKind::Text, Container::Text),
        };
        let child_id = xml_child_id(list_id, stamp, kind);
        let element = self.registered_handle(child_id, container);
        self.parents.insert(child_id, list_id);
        list.borrow_mut().insert_at(stamp, element, anchor);
        // Record the birth placement so a later move can pick the live one of the
        // node's placements.
        self.placements
            .entry(child_id)
            .or_default()
            .push(Placement {
                list: list_id,
                stamp,
            });
        self.placement_index.insert((list_id, stamp));
        // The created-under parent anchors cycle detection and is the fallback
        // parent (via the move log's base map) when no move governs this node.
        if let Some(&owner) = self.parents.get(&list_id) {
            self.moves.set_base(child_id, owner);
        }
    }

    /// Relocate `node` under the destination children `dest_list` at `anchor`.
    /// Inserts a placement referencing the node's stable element id, records the
    /// move in the lamport-ordered log, then re-folds so exactly one placement of
    /// the node renders — Kleppmann convergence, a cycle move left inert.
    fn apply_move(&mut self, dest_list: ElementId, node: ElementId, anchor: Anchor, stamp: Stamp) {
        // Only a node that lives in a children sequence is movable: it must
        // already hold a placement. A node created straight into a map slot (a
        // document root) is keyed, not positioned, so a move of it is a no-op —
        // and the same no-op on every replica, since the local emit path reaches
        // here directly, bypassing the `ready` gate remotes apply.
        if !self.placements.contains_key(&node) {
            return;
        }
        let Some(list) = self.live_list(dest_list) else {
            return;
        };
        let Some(&owner) = self.parents.get(&dest_list) else {
            return;
        };
        let Some(element) = self.node_element(node) else {
            return;
        };
        list.borrow_mut().insert_at(stamp, element, anchor);
        self.placements.entry(node).or_default().push(Placement {
            list: dest_list,
            stamp,
        });
        self.placement_index.insert((dest_list, stamp));
        self.moves.apply(stamp, node, owner);
        self.refold_moves();
    }

    /// Re-derive, for every movable node, which of its placements renders: the
    /// highest-stamped placement in the node's effective-parent list (`parent_of`
    /// falls back to the created-under parent, so a never-moved node resolves to
    /// its birth list). Every other placement is suppressed, and reachability is
    /// re-pointed at the live placement's list so a moved subtree resolves through
    /// its new parent. A node whose placement was tombstoned by a `ListDelete` is
    /// deleted — every placement is hidden, so a concurrent delete wins over a
    /// concurrent move rather than resurrecting the node under the new parent.
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
            if !deleted {
                reparent.push((*node, eff_list));
            }
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
        if map.borrow().is_displaced() {
            return;
        }
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
                Element::XmlFragment(handle)
            }
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
fn birth_placement(node: ElementId, places: &[Placement], kind: ElementKind) -> Option<&Placement> {
    places
        .iter()
        .find(|p| xml_child_id(p.list, p.stamp, kind) == node)
}

/// The id of the RangedElement a create at `stamp` mints. Derived under a fixed
/// annotation namespace so it never collides with a user's map slot, and from the
/// globally-unique op stamp so every replica agrees and concurrent creates differ.
fn ranged_id(stamp: Stamp) -> ElementId {
    let ns = ElementId::from_bytes(*b"crdtsync\0ranged\0");
    ElementId::derive(ns, &stamp_key(stamp), ElementKind::Scalar)
}

/// The id of the ACL tuple a grant at `stamp` mints. Derived under a fixed
/// authorization namespace so it never collides with a user's map slot, and from
/// the globally-unique op stamp so every replica agrees and concurrent grants
/// differ.
fn acl_id(stamp: Stamp) -> ElementId {
    let ns = ElementId::from_bytes(*b"crdtsync\0acl\0\0\0\0");
    ElementId::derive(ns, &stamp_key(stamp), ElementKind::Scalar)
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
        let stamp = self.doc.emit_stamped(
            root,
            OpKind::RangedCreate {
                start,
                end,
                payload,
                name,
            },
        );
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
        let stamp = self.doc.emit_stamped(
            root,
            OpKind::AclGrant {
                subject,
                grant,
                effect,
                scope,
                grantor,
            },
        );
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
    /// the child's derived id. Emits nothing when the children List is not
    /// materialised — an op the author never applied would diverge a peer that
    /// has the List — so a would-be child id is returned with no op behind it.
    /// (That branch is unreachable through the public API: a cursor is only
    /// handed out for a List a create already registered; it is a defensive
    /// placeholder, not a live path.)
    fn insert_child(&mut self, index: usize, tag: Option<Vec<u8>>, kind: ElementKind) -> ElementId {
        let anchor = match self.doc.lists.get(&self.list_id) {
            Some(list) => list.borrow().place(index),
            None => {
                let zero = Stamp {
                    lamport: 0,
                    client: ClientId::from_bytes([0u8; 16]),
                };
                return xml_child_id(self.list_id, zero, kind);
            }
        };
        let stamp = self
            .doc
            .emit_stamped(self.list_id, OpKind::XmlInsertChild { tag, anchor });
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
        };
        assert!(d.restore_moves(&[(mv, a_id, grand_id)]).is_err());
    }
}
