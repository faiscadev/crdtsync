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

use crate::clientid::ClientId;
use crate::codec::{
    decode_ops, encode_ops, len_u32, put_u32, put_u64, put_u8, Cursor, DecodeError,
};
use crate::counter::Counter;
use crate::element::Element;
use crate::elementid::{ElementId, ElementKind};
use crate::list::List;
use crate::map::{DecodedMap, Map, SlotValue};
use crate::op::{Op, OpId, OpKind, Tx, TxId};
use crate::repair::{keyed_repairs, Repair, RepairId};
use crate::scalar::Scalar;
use crate::schema::Schema;
use crate::stamp::Stamp;
use crate::text::Text;
use crate::validate::Step;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// The well-known root slot every replica shares, so children derive under the
/// same parent.
const ROOT_ID: [u8; 16] = *b"crdtsync\0\0\0\0root";

/// The snapshot format version, bumped when the encoding changes so an old
/// reader rejects a newer stream rather than misreading it. v2 compresses
/// sequence tombstones into run records and drops their dead values.
const STATE_VERSION: u8 = 2;

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
    /// Each container's parent map, for walking reachability up to the root.
    parents: HashMap<ElementId, ElementId>,
    lamport: u64,
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
        // Skip a handle a caller is still borrowing rather than panic in drop.
        for map in self.maps.values() {
            if let Ok(mut map) = map.try_borrow_mut() {
                map.clear();
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
            parents: HashMap::new(),
            lamport: 0,
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

    /// Whether `key` in `map_id` ever named a container — the registry retains a
    /// deleted container at the id its key derives, so a tombstoned slot can still
    /// carry container identity a leaf migration must not disturb.
    fn has_container_identity(&self, map_id: ElementId, key: &[u8]) -> bool {
        let id = |kind| ElementId::derive(map_id, key, kind);
        self.maps.contains_key(&id(ElementKind::Map))
            || self.lists.contains_key(&id(ElementKind::List))
            || self.texts.contains_key(&id(ElementKind::Text))
    }

    /// Migrate a snapshot's leaf slots by `fate`, keyed on the slot key — the
    /// state-level analogue of translating the op stream between two schema
    /// versions, so a snapshot-served joiner converges with a peer served the
    /// same history as a translated op delta. Across every map, each leaf slot
    /// (scalar / register / counter, live or tombstoned) is `Keep`t, `Drop`ped,
    /// or `Rename`d to a new key per `fate`; a container slot (map / list / text)
    /// — live, or a tombstoned deleted one whose identity the registry still holds
    /// — is always carried verbatim, mirroring the op seam, which carries a
    /// container-create verbatim rather than tear its subtree. A dropped or
    /// renamed counter's element moves with its slot — dropped from the registry,
    /// or merged into the counter at the id its new key derives (matching the op
    /// seam, where renamed increments merge at that shared id) — so no phantom
    /// counter lingers. Returns whether any slot changed. `fate` is the
    /// composition of the chain's per-step key rewrites; supplying `|_| Keep` is
    /// a no-op.
    ///
    /// A container field's *full* migration is out of scope here: re-keying a
    /// deleted container to match the op seam (which resurrects the create at the
    /// old key while re-keying the delete) needs the create's stamp, which the
    /// materialized tombstone has dropped. Such a slot is left verbatim rather
    /// than mis-migrated as a leaf; faithful container-field migration is the
    /// deferred element-set-aware seam.
    pub fn migrate_leaf_slots(&mut self, fate: impl Fn(&[u8]) -> SlotFate) -> bool {
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
            let keys = map.borrow().slot_keys();
            for key in keys {
                let fate = match fate(&key) {
                    SlotFate::Keep => continue,
                    other => other,
                };
                // The slot body is carried verbatim for a container slot — a live
                // one, or a tombstoned deleted one whose container identity the
                // registry still holds (its value is `None`, so it would otherwise
                // migrate as a leaf tombstone the snapshot cannot re-key faithfully,
                // the create's stamp being gone). The COUNTER registry at the key's
                // derived id migrates regardless: it is a separate identity from the
                // slot body and from any container at the key, retained across
                // displacement, so it must prune / re-home even when the slot is
                // carried verbatim.
                let carry_slot = map.borrow().slot_is_live_container(&key)
                    || (map.borrow().slot_is_tombstone(&key)
                        && self.has_container_identity(map_id, &key));
                let old_counter = ElementId::derive(map_id, &key, ElementKind::Counter);
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

        let counters = decode_registry(cur, |c| Counter::decode_state_from(c), |c| c.id())?;
        let lists = decode_registry(cur, |c| List::decode_state_from(c), |l| l.id())?;
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
                    Some(SlotValue::Ref(kind, id)) => {
                        Some(resolve_ref(kind, id, &counters, &lists, &texts, &maps)?)
                    }
                };
                if m.insert_decoded(slot.key, slot.stamp, value, slot.tombstone) {
                    return Err(DecodeError::BadTag {
                        what: "document: duplicate map slot",
                        tag: 0,
                    });
                }
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

        // Displacement isn't stored: a registered container is installed iff a
        // live slot still holds its handle, so mark every other one displaced.
        mark_displaced(&maps, &lists, &texts, &counters, root_id);

        let mut doc = Document {
            client,
            root,
            maps,
            lists,
            texts,
            counters,
            parents,
            lamport,
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
        // The buffer holds only ops still waiting on their target; a well-formed
        // snapshot already satisfies that, so this is a no-op there. Draining
        // restores the invariant for any op decoded as already reachable rather
        // than leaving it stuck until an unrelated mutation.
        doc.drain_buffer();
        Ok(doc)
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
        // the clock must clear the last of them, not just the base.
        let last = op.stamp.lamport.saturating_add(span(&op.kind) - 1);
        if last > self.lamport {
            self.lamport = last;
        }
        self.apply_kind(op.target, &op.kind, op.stamp, op.id.client);
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
                _ => {}
            }
        }
        true
    }

    /// Mint identity + causal position for a local edit, apply it, and record
    /// the op on the in-progress transact.
    fn emit(&mut self, target: ElementId, kind: OpKind) {
        self.lamport += 1;
        let stamp = Stamp {
            lamport: self.lamport,
            client: self.client,
        };
        // Reserve the rest of a run's char_ids so the next op sorts after it.
        self.lamport += span(&kind) - 1;
        let id = OpId {
            client: self.client,
            seq: self.seq,
        };
        self.seq += 1;
        self.seen.insert(id);
        let author = self.client;
        self.apply_kind(target, &kind, stamp, author);
        self.pending.push(Op::new(id, stamp, target, kind));
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
        let child_id = ElementId::derive(map_id, key, kind.element_kind());
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

/// The container kinds a create op installs.
#[derive(Clone, Copy)]
enum Container {
    Map,
    List,
    Text,
}

impl Container {
    fn element_kind(self) -> ElementKind {
        match self {
            Container::Map => ElementKind::Map,
            Container::List => ElementKind::List,
            Container::Text => ElementKind::Text,
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

/// How many consecutive char_ids an op consumes from its stamp. A text run
/// takes one per codepoint; every other op takes one.
fn span(kind: &OpKind) -> u64 {
    match kind {
        OpKind::TextInsert { s, .. } => s.chars().count().max(1) as u64,
        _ => 1,
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

/// Resolve a decoded slot reference to the registered handle it names.
fn resolve_ref(
    kind: ElementKind,
    id: ElementId,
    counters: &HashMap<ElementId, Rc<RefCell<Counter>>>,
    lists: &HashMap<ElementId, Rc<RefCell<List>>>,
    texts: &HashMap<ElementId, Rc<RefCell<Text>>>,
    maps: &HashMap<ElementId, Rc<RefCell<Map>>>,
) -> Result<Element, DecodeError> {
    let element = match kind {
        ElementKind::Counter => counters.get(&id).map(|c| Element::Counter(Rc::clone(c))),
        ElementKind::List => lists.get(&id).map(|l| Element::List(Rc::clone(l))),
        ElementKind::Text => texts.get(&id).map(|t| Element::Text(Rc::clone(t))),
        ElementKind::Map => maps.get(&id).map(|m| Element::Map(Rc::clone(m))),
        // A leaf has no registered handle to reference; the tree kinds are not
        // resolved through this seam.
        ElementKind::Scalar
        | ElementKind::Register
        | ElementKind::XmlElement
        | ElementKind::XmlFragment => None,
    };
    element.ok_or(DecodeError::BadTag {
        what: "document: dangling slot reference",
        tag: 0,
    })
}

/// Restore displacement flags a snapshot doesn't store: a registered container
/// is installed iff a live slot still holds it; every other one lost its slot
/// and decodes displaced, so reachability and op emission stay correct.
fn mark_displaced(
    maps: &HashMap<ElementId, Rc<RefCell<Map>>>,
    lists: &HashMap<ElementId, Rc<RefCell<List>>>,
    texts: &HashMap<ElementId, Rc<RefCell<Text>>>,
    counters: &HashMap<ElementId, Rc<RefCell<Counter>>>,
    root_id: ElementId,
) {
    let mut installed: HashSet<ElementId> = HashSet::new();
    installed.insert(root_id);
    for m in maps.values() {
        for value in m.borrow().live_values() {
            if value.kind() != ElementKind::Scalar {
                installed.insert(value.id());
            }
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
        self.doc
            .emit(self.map_id, OpKind::MapCreate { key: key.to_vec() });
        let child = ElementId::derive(self.map_id, key, ElementKind::Map);
        MapCursor {
            doc: self.doc,
            map_id: child,
        }
    }

    /// Descend into a nested Map at `key`, consuming this cursor. Chains without
    /// nesting borrows, so a caller can walk a runtime-length path in a loop.
    pub fn into_map(self, key: &[u8]) -> MapCursor<'a> {
        self.doc
            .emit(self.map_id, OpKind::MapCreate { key: key.to_vec() });
        let child = ElementId::derive(self.map_id, key, ElementKind::Map);
        MapCursor {
            doc: self.doc,
            map_id: child,
        }
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
