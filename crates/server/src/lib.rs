//! Single-node sync server core.
//!
//! A [`Hub`] owns one authoritative replica per room plus that room's
//! append-only op log. Clients ingest ops; the hub deduplicates by op id,
//! folds each new op into the room's replica, and assigns it a monotonic
//! server sequence. A subscriber names the last sequence it saw and the hub
//! replays everything past it — the log a fresh replica replays back to the
//! same state. Pure state; the transport wraps it.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::sync::Arc;

use crdtsync_core::diff::Change;
use crdtsync_core::op::OpId;
use crdtsync_core::{ClientId, Document, Element, Op, Schema};

pub mod acl;
pub mod admin;
pub mod audit;
pub mod auth;
pub mod authz;
pub mod auto_version;
pub mod clock;
pub mod event;
pub mod placement;
pub mod registry;
pub mod runtime;
pub mod schema_registry;
pub mod session;
pub mod store;
pub mod translate;
pub mod webhook;
pub use admin::{admin_router, register_schema, serve_admin, RegisterOutcome, RegisterRequest};
pub use auth::{AllowAll, Identity, StaticTokens, Verifier};
pub use authz::{Action, Authorizer, PermitAll, Resource};
pub use clock::{Clock, ManualClock, SystemClock};
pub use event::{EngineEvent, EventSink};
pub use placement::{Cluster, NodeId};
pub use registry::{ConnId, Registry};
pub use schema_registry::{RegisterError, Registered, Resolution, SchemaRegistry};
pub use session::{negotiate, step, AwarenessBroadcast, Response, Session};
pub use store::{Branch, RoomLog, RoomMeta, Snapshot, Store, StoredOp};
pub use webhook::{WebhookConfig, WebhookSink};

/// A room name, opaque bytes chosen by the deployment.
pub type RoomId = Vec<u8>;

/// The default branch every room has: the one that shares the whole op log from
/// its origin. It is never deletable and never renamable, so a room always
/// resolves it.
pub const MAIN_BRANCH: &[u8] = b"main";

/// The default read-only publish target [`publish`](Hub::publish) points at when a
/// deployment names none: editors edit `main`, read-only consumers subscribe to
/// this branch's snapshot of the last published state.
pub const PUBLISHED_BRANCH: &[u8] = b"published";

/// Why a snapshot diff could not be computed. A diff runs the core engine over
/// two decoded whole-replica states; either a named version or branch is absent,
/// or a snapshot's encoded state does not decode into a document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffError {
    /// A named version the room does not have.
    UnknownVersion(Vec<u8>),
    /// A branch the room does not have.
    UnknownBranch(Vec<u8>),
    /// A snapshot's encoded state failed to decode into a document.
    Decode,
}

impl std::fmt::Display for DiffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiffError::UnknownVersion(name) => {
                write!(f, "unknown version {}", String::from_utf8_lossy(name))
            }
            DiffError::UnknownBranch(name) => {
                write!(f, "unknown branch {}", String::from_utf8_lossy(name))
            }
            DiffError::Decode => write!(f, "a snapshot's state failed to decode"),
        }
    }
}

impl std::error::Error for DiffError {}

/// Decode two encoded whole-replica states and diff them with the core engine. A
/// state that does not decode is [`DiffError::Decode`]; identical states diff to
/// an empty change list.
fn diff_states(old: &[u8], new: &[u8]) -> Result<Vec<Change>, DiffError> {
    let old = Document::decode_state(old).map_err(|_| DiffError::Decode)?;
    let new = Document::decode_state(new).map_err(|_| DiffError::Decode)?;
    Ok(crdtsync_core::path::diff(&old, &new))
}

/// A room's branches, always holding the default [`MAIN_BRANCH`] and any forks
/// past it. A fork shares immutable history up to its fork point; only the
/// divergent forks are persisted, `main` being synthesized. Listing order is
/// deterministic (by name), so replicas agree.
#[derive(Clone)]
pub struct BranchRegistry {
    branches: BTreeMap<Vec<u8>, Branch>,
}

impl Default for BranchRegistry {
    fn default() -> Self {
        let mut branches = BTreeMap::new();
        branches.insert(
            MAIN_BRANCH.to_vec(),
            Branch {
                name: MAIN_BRANCH.to_vec(),
                fork_point: 0,
                head: 0,
                published: false,
            },
        );
        Self { branches }
    }
}

impl BranchRegistry {
    /// A registry restored from its persisted forks, with the default `main`
    /// re-synthesized around them.
    fn from_forks(forks: impl IntoIterator<Item = Branch>) -> Self {
        let mut registry = Self::default();
        for fork in forks {
            registry.branches.insert(fork.name.clone(), fork);
        }
        registry
    }

    /// A branch by name, or `None` if this room has no such branch.
    pub fn branch(&self, name: &[u8]) -> Option<&Branch> {
        self.branches.get(name)
    }

    /// Every branch, in deterministic name order — always at least `main`.
    pub fn branches(&self) -> impl Iterator<Item = &Branch> {
        self.branches.values()
    }

    /// Fork a fresh branch `new` off the existing branch `from`, sharing its
    /// history up to position `at`. Refuses — changing nothing — if `new` already
    /// exists or `from` is absent. The new branch starts with no divergence past
    /// the fork point, so its head is the fork point.
    fn fork(&mut self, new: &[u8], from: &[u8], at: u64) -> bool {
        if self.branches.contains_key(new) || !self.branches.contains_key(from) {
            return false;
        }
        self.branches.insert(
            new.to_vec(),
            Branch {
                name: new.to_vec(),
                fork_point: at,
                head: at,
                published: false,
            },
        );
        true
    }

    /// Point a read-only publish target `name` at position `at`: fork it fresh (its
    /// base is the editor state a [`publish`](Hub::publish) captures) or, when it
    /// already exists, repoint its fork position to the newer publish. Either way it
    /// is marked `published` — a read-only target no client write advances. Refuses
    /// the default `main`, which is the editor branch, never a publish target.
    fn point_published(&mut self, name: &[u8], at: u64) -> bool {
        match self.branches.get_mut(name) {
            Some(branch) if branch.name == MAIN_BRANCH => false,
            Some(branch) => {
                branch.fork_point = at;
                branch.head = at;
                branch.published = true;
                true
            }
            None => {
                self.branches.insert(
                    name.to_vec(),
                    Branch {
                        name: name.to_vec(),
                        fork_point: at,
                        head: at,
                        published: true,
                    },
                );
                true
            }
        }
    }

    /// Whether branch `name` is a read-only publish target.
    fn is_published(&self, name: &[u8]) -> bool {
        self.branches.get(name).is_some_and(|b| b.published)
    }

    /// Rename branch `from` to `to`. Refuses — changing nothing — for the
    /// undeletable `main`, an absent `from`, or a `to` already taken.
    fn rename(&mut self, from: &[u8], to: &[u8]) -> bool {
        if from == MAIN_BRANCH
            || self.branches.contains_key(to)
            || !self.branches.contains_key(from)
        {
            return false;
        }
        let mut branch = self.branches.remove(from).expect("presence checked above");
        branch.name = to.to_vec();
        self.branches.insert(to.to_vec(), branch);
        true
    }

    /// Delete branch `name`, returning whether one was removed. `main` is never
    /// deletable, so a room always keeps its default branch.
    fn delete(&mut self, name: &[u8]) -> bool {
        if name == MAIN_BRANCH {
            return false;
        }
        self.branches.remove(name).is_some()
    }

    /// The forks past the default `main` — the persisted subset, `main` being
    /// synthesized on load.
    fn forks(&self) -> impl Iterator<Item = &Branch> {
        self.branches
            .values()
            .filter(|branch| branch.name != MAIN_BRANCH)
    }

    /// Point `main`'s head at the room's current log head, which it tracks.
    fn set_main_head(&mut self, head: u64) {
        if let Some(main) = self.branches.get_mut(MAIN_BRANCH) {
            main.head = head;
        }
    }

    /// Point branch `name`'s head at `head`, reporting whether it moved. A branch
    /// write advances its own head past the fork point; the default `main` tracks
    /// the log head instead and is not set here.
    fn set_head(&mut self, name: &[u8], head: u64) -> bool {
        match self.branches.get_mut(name) {
            Some(branch) if branch.head != head => {
                branch.head = head;
                true
            }
            _ => false,
        }
    }
}

/// A non-`main` branch's divergent op tail: the ops appended past its fork point.
/// The shared base — every op up to the fork — lives in `main`'s log and is never
/// duplicated here, so a branch's storage cost is only its divergence.
#[derive(Default)]
struct BranchLog {
    ops: Vec<StoredOp>,
    seen: HashSet<OpId>,
}

/// One room's authoritative replica and its op log. A server sequence is a
/// 1-based position across the room's whole history; `base_seq` counts the ops
/// already compacted away (sequences `1..=base_seq`), so a retained op at
/// `log[i]` carries seq `base_seq + i + 1`.
struct Room {
    doc: Document,
    log: Vec<StoredOp>,
    seen: HashSet<OpId>,
    base_seq: u64,
    /// The highest governing-app op version ever folded into this room — the
    /// worst-case op version a joiner must down-reach to be served the whole
    /// replica. It tracks the merged state, so compaction (which drops the log)
    /// leaves it standing; relay and foreign-app ops are untagged and excluded.
    max_op_version: Option<u32>,
}

impl Room {
    fn new(server: ClientId) -> Self {
        Self {
            doc: Document::new(server),
            log: Vec::new(),
            seen: HashSet::new(),
            base_seq: 0,
            max_op_version: None,
        }
    }

    /// The room's high-water server sequence.
    fn head(&self) -> u64 {
        self.base_seq + self.log.len() as u64
    }
}

/// What a subscriber needs to catch up, given the sequence it last saw.
pub enum Catchup {
    /// The subscriber is at or above the compaction floor: fold these ops, in
    /// server-sequence order. Each carries its stored creation version, so the
    /// subscribe seam can translate the heterogeneous delta to the joiner's own
    /// version — the delta can mix versions, unlike a single-writer broadcast.
    Ops(Vec<StoredOp>),
    /// The subscriber fell below the floor: load this whole-replica state, then
    /// treat `seq` as the sequence it has now caught up to.
    Snapshot { seq: u64, state: Vec<u8> },
}

/// A named version: a whole-replica snapshot captured at the server sequence it
/// covered, retained under an app-chosen name until deleted.
struct Version {
    seq: u64,
    /// The auto-version trigger that authored it (its stable identity), or `None`
    /// for a manually created version. Retention prunes within one origin, so it
    /// never touches a manual version or a different trigger's captures.
    origin: Option<Vec<u8>>,
    /// A monotonic capture order stamped by the hub — retention orders a trigger's
    /// captures by this, not by a wall-clock name, so a backward clock step cannot
    /// misorder them.
    ordinal: u64,
    state: Vec<u8>,
}

/// The most distinct awareness keys one client may hold in a room. Presence is
/// a handful of entries (cursor, selection, name, viewport, …); the cap bounds
/// the room's awareness map against a client that floods distinct keys.
const MAX_AWARENESS_KEYS_PER_CLIENT: usize = 64;

/// The timed-TTL policy an enforcing server applies to awareness entries: how
/// long an entry kind may go silent before the periodic sweep expires it.
pub trait AwarenessPolicy: Send + Sync {
    /// The timed TTL in milliseconds for entry `key` in `room`, or `None` for a
    /// session-lifetime entry — one cleared only on disconnect, never by silence.
    fn ttl(&self, room: &[u8], key: &[u8]) -> Option<u64>;

    /// Whether any entry can carry a timed TTL. A policy that declares none lets
    /// the sweep skip the whole per-entry expiry scan. Conservatively `true`.
    fn has_timed_ttls(&self) -> bool {
        true
    }

    /// The coalesce window in milliseconds for entry `key` in `room`, or `None`
    /// for an unthrottled kind — every update fans out at once. Within the window
    /// an update is coalesced: recorded but not fanned out.
    fn throttle(&self, _room: &[u8], _key: &[u8]) -> Option<u64> {
        None
    }
}

/// The default policy: every entry is session-lifetime, so a server with no
/// registered schema never times an entry out — awareness behaves as pure
/// presence cleared only on disconnect.
pub struct NoTimedTtl;

impl AwarenessPolicy for NoTimedTtl {
    fn ttl(&self, _room: &[u8], _key: &[u8]) -> Option<u64> {
        None
    }

    fn has_timed_ttls(&self) -> bool {
        false
    }
}

/// A policy resolved from each room's governing schema for one sweep: a snapshot
/// of `room → parsed schema`, built from the rooms with live presence and the
/// `{app_id, version}` bound to each. An entry's TTL is the `ttl` its kind
/// declares in the room's schema; a room with no governing schema (a relay
/// room), or a kind the schema gives no `ttl`, is session-lifetime. The parsed
/// schema is shared (an [`Arc`]), so many rooms of one app hold one copy.
pub struct SchemaAwarenessPolicy {
    schemas: HashMap<RoomId, Arc<Schema>>,
    /// Whether any mapped schema declares a timed TTL — precomputed so the sweep's
    /// `has_timed_ttls` check is O(1), not a rescan of every room's schema.
    has_timed_ttls: bool,
}

impl SchemaAwarenessPolicy {
    /// A policy over the resolved `room → schema` snapshot.
    pub fn new(schemas: HashMap<RoomId, Arc<Schema>>) -> Self {
        let has_timed_ttls = schemas
            .values()
            .any(|s| s.awareness().iter().any(|(_, e)| e.ttl.is_some()));
        Self {
            schemas,
            has_timed_ttls,
        }
    }

    fn entry(&self, room: &[u8], key: &[u8]) -> Option<&crdtsync_core::AwarenessEntry> {
        let schema = self.schemas.get(room)?;
        let kind = std::str::from_utf8(key).ok()?;
        schema.awareness_entry(kind)
    }
}

impl AwarenessPolicy for SchemaAwarenessPolicy {
    fn ttl(&self, room: &[u8], key: &[u8]) -> Option<u64> {
        self.entry(room, key).and_then(|e| e.ttl)
    }

    fn has_timed_ttls(&self) -> bool {
        self.has_timed_ttls
    }

    fn throttle(&self, room: &[u8], key: &[u8]) -> Option<u64> {
        self.entry(room, key).and_then(|e| e.throttle)
    }
}

/// How a departing client's presence is cleared from a room. An actor-wide
/// clear when no other connection of that actor remains; otherwise a per-key
/// clear for each key no surviving connection still holds — so closing one of an
/// actor's tabs never wipes the presence a sibling tab keeps live.
pub enum AwarenessRemoval {
    /// Every entry of `actor` in `room` is gone — no connection of it remains.
    Actor { room: RoomId, actor: Vec<u8> },
    /// Just `actor`'s `key` in `room` is gone; its other entries (via a sibling
    /// connection) live on.
    Key {
        room: RoomId,
        actor: Vec<u8>,
        key: Vec<u8>,
    },
}

impl AwarenessRemoval {
    /// The room this removal is scoped to.
    pub fn room(&self) -> &[u8] {
        match self {
            AwarenessRemoval::Actor { room, .. } | AwarenessRemoval::Key { room, .. } => room,
        }
    }

    /// The wire message telling a subscriber of the removal on `channel`.
    pub fn message(&self, channel: crdtsync_core::protocol::Channel) -> crdtsync_core::Message {
        match self {
            AwarenessRemoval::Actor { actor, .. } => crdtsync_core::Message::AwarenessClear {
                channel,
                actor: actor.clone(),
            },
            AwarenessRemoval::Key { actor, key, .. } => crdtsync_core::Message::AwarenessClearKey {
                channel,
                actor: actor.clone(),
                key: key.clone(),
            },
        }
    }
}

/// The result of recording an awareness entry: whether it was stored (a key past
/// the per-client cap is dropped) and whether it should fan out now (an update
/// arriving faster than its throttle window is coalesced — recorded, not sent).
pub struct SetOutcome {
    pub stored: bool,
    pub broadcast: bool,
}

/// One client's awareness entry for a key: the actor to surface it under, the last
/// value fanned out to the room, the wall-clock millis it was last set
/// (`last_seen`, the timed-TTL basis) and last fanned out (`last_broadcast`, the
/// throttle-window basis). `value` is always what peers were last sent, so a
/// joiner replaying it sees exactly what existing peers see.
struct Presence {
    actor: Vec<u8>,
    value: Vec<u8>,
    last_seen: u64,
    last_broadcast: u64,
}

/// The set of rooms a single node serves, optionally over a durable log.
pub struct Hub {
    server: ClientId,
    rooms: HashMap<RoomId, Room>,
    store: Option<Store>,
    compaction_threshold: u64,
    /// The durable governing `{app_id, version}` per room, the mirror of the
    /// store's persisted binding. Seeded from the store on load and updated by
    /// [`bind_governing`](Hub::bind_governing) when a store is attached, so it
    /// survives a restart and a dormant-room sweep that drops the registry's live
    /// binding; a store-less hub leaves it empty, keeping the in-memory
    /// rebuild-on-subscribe behavior. It rides here (not on `Room`) so a bound but
    /// never-written room needs no empty replica.
    governing: HashMap<RoomId, (Vec<u8>, u32)>,
    /// Ephemeral presence per room: each owner client's latest [`Presence`] per
    /// key. Never persisted or snapshotted. Nesting by client keeps the per-client
    /// key cap an O(1) check and lets a disconnect find a client's own entries
    /// directly.
    awareness: HashMap<RoomId, HashMap<ClientId, HashMap<Vec<u8>, Presence>>>,
    /// Named versions per room, keyed by name — sorted, for listing/pagination.
    /// The in-memory versions index over the snapshot storage primitive.
    versions: HashMap<RoomId, BTreeMap<Vec<u8>, Version>>,
    /// The next capture ordinal, stamped on each created version and never reused.
    /// Restored past the highest persisted ordinal on load, so the order survives a
    /// restart; a gap (a rolled-back persist) is harmless — only monotonicity
    /// matters.
    version_ordinal: u64,
    /// The branches per room, keyed by room. A room absent here has only the
    /// default `main` — the registry is materialized lazily on the first fork, so a
    /// never-forked room carries no per-room branch state and no branches file.
    branches: HashMap<RoomId, BranchRegistry>,
    /// The divergent op tail of each non-`main` branch, keyed by room then branch.
    /// Only the ops past a branch's fork point live here; its shared base is
    /// `main`'s log, never copied, so a room absent here has only branches that
    /// have not yet diverged (and `main`, which is the log itself).
    branch_logs: HashMap<RoomId, HashMap<Vec<u8>, BranchLog>>,
    /// Each snapshot-forked branch's owned base — the materialized state of the
    /// version it forked from — keyed by room then branch. A live-log fork shares
    /// `main`'s log and has no entry; a snapshot fork owns a copy of the version
    /// state, so it serves that state (never `main`'s later ops) and survives the
    /// source version's deletion. The presence of an entry is what marks a branch
    /// a snapshot fork, routing its catch-up to the owned base.
    branch_bases: HashMap<RoomId, HashMap<Vec<u8>, Vec<u8>>>,
    /// The active-HEAD branch per room — the branch a default (unnamed) subscribe
    /// follows. A room absent here serves the default `main`; a restore-as-branch
    /// switches it to the restored branch, so a plain subscriber then follows the
    /// restored state while the old branch stays subscribable by name. Durable, so
    /// the switch replays on reload.
    active_branch: HashMap<RoomId, Vec<u8>>,
    /// The engine event sinks, notified of each lifecycle moment. Empty by
    /// default — no sink, no emission cost.
    sinks: Vec<Box<dyn EventSink>>,
}

impl Hub {
    /// An in-memory hub whose per-room replicas are owned by `server`.
    pub fn new(server: ClientId) -> Self {
        Self {
            server,
            rooms: HashMap::new(),
            store: None,
            compaction_threshold: 0,
            governing: HashMap::new(),
            awareness: HashMap::new(),
            versions: HashMap::new(),
            version_ordinal: 0,
            branches: HashMap::new(),
            branch_logs: HashMap::new(),
            branch_bases: HashMap::new(),
            active_branch: HashMap::new(),
            sinks: Vec::new(),
        }
    }

    /// Register an [`EventSink`] to observe the engine's lifecycle events. Several
    /// may be registered; each is notified of every event, in registration order.
    pub fn add_event_sink(&mut self, sink: Box<dyn EventSink>) {
        self.sinks.push(sink);
    }

    /// Fan a lifecycle event out to every registered sink. Called after the moment
    /// has committed, so a sink observes settled state; a no-sink hub does nothing.
    pub(crate) fn emit(&self, event: EngineEvent) {
        for sink in &self.sinks {
            sink.on_event(&event);
        }
    }

    /// Record `client`'s ephemeral awareness entry `key` in `room`, last-writer-
    /// wins, so a later subscriber can be replayed the current presence. A new
    /// key past the per-client cap is dropped, so a client cannot grow the room's
    /// awareness map without bound. `now` stamps the entry's last-seen time on
    /// every set — including a coalesced one — so activity refreshes the TTL even
    /// while the throttle holds the wire quiet.
    ///
    /// `throttle` is the kind's coalesce window. The first update, any update on an
    /// unthrottled kind, and the first update once the window has elapsed fan out
    /// at once ([`SetOutcome::broadcast`] `true`) and become the entry's stored
    /// value. An update arriving inside the window is coalesced: it refreshes the
    /// last-seen time but does not replace the stored value or fan out — the server
    /// caps the outbound rate, and the client SDK's debounce owns delivering the
    /// trailing value on its next past-window send. So the stored value is always
    /// what the room was last sent, keeping every peer and any joiner in agreement.
    /// `checked_sub` treats a backward clock step as elapsed, so a skew fans out
    /// rather than freezing the entry. A dropped key is neither stored nor sent.
    pub fn set_awareness(
        &mut self,
        room: &[u8],
        client: ClientId,
        actor: Vec<u8>,
        key: Vec<u8>,
        value: Vec<u8>,
        now: u64,
        throttle: Option<u64>,
    ) -> SetOutcome {
        let keys = self
            .awareness
            .entry(room.to_vec())
            .or_default()
            .entry(client)
            .or_default();
        let len = keys.len();
        match keys.entry(key) {
            Entry::Occupied(mut slot) => {
                let p = slot.get_mut();
                // Fan out an unthrottled kind, or the first update once the window
                // has elapsed; otherwise coalesce — refresh the last-seen time
                // (activity, so it does not TTL-expire mid-stream) but keep the
                // stored value and hold the wire quiet. `checked_sub` treats a
                // backward clock step as elapsed, so a skew fans out.
                let broadcast = throttle.map_or(true, |window| {
                    now.checked_sub(p.last_broadcast)
                        .map_or(true, |elapsed| elapsed >= window)
                });
                p.last_seen = now;
                if broadcast {
                    p.actor = actor;
                    p.value = value;
                    p.last_broadcast = now;
                }
                SetOutcome {
                    stored: true,
                    broadcast,
                }
            }
            Entry::Vacant(slot) => {
                if len >= MAX_AWARENESS_KEYS_PER_CLIENT {
                    return SetOutcome {
                        stored: false,
                        broadcast: false,
                    };
                }
                slot.insert(Presence {
                    actor,
                    value,
                    last_seen: now,
                    last_broadcast: now,
                });
                SetOutcome {
                    stored: true,
                    broadcast: true,
                }
            }
        }
    }

    /// The current awareness entries in `room` as `(actor, key, value)`, for
    /// replaying presence to a joining subscriber.
    pub fn awareness_entries(&self, room: &[u8]) -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        self.awareness
            .get(room)
            .into_iter()
            .flatten()
            .flat_map(|(_, keys)| {
                keys.iter()
                    .map(|(key, p)| (p.actor.clone(), key.clone(), p.value.clone()))
            })
            .collect()
    }

    /// Expire every awareness entry whose silence since its last set exceeds the
    /// timed TTL `policy` assigns it, returning the per-key removals the caller
    /// should tell each room's peers. An entry `policy` gives no TTL is session-
    /// lifetime and never expires here. Empty client and room maps left behind are
    /// pruned, matching a disconnect clear.
    ///
    /// Peers key presence by `(actor, key)`, so a removal is returned only when no
    /// other client of that actor still holds that key in the room — a second
    /// connection of the same actor (another tab) with a live entry keeps the
    /// actor's presence, and a sibling's expiry must not wipe it from peers.
    pub fn expire_silent_awareness(
        &mut self,
        now: u64,
        policy: &dyn AwarenessPolicy,
    ) -> Vec<AwarenessRemoval> {
        let mut expired = Vec::new();
        for (room, by_client) in self.awareness.iter_mut() {
            for keys in by_client.values_mut() {
                keys.retain(|key, p| match policy.ttl(room, key) {
                    Some(ttl) if now.saturating_sub(p.last_seen) > ttl => {
                        expired.push((room.clone(), p.actor.clone(), key.clone()));
                        false
                    }
                    _ => true,
                });
            }
            by_client.retain(|_, keys| !keys.is_empty());
        }
        self.awareness.retain(|_, by_client| !by_client.is_empty());
        // Nothing timed out — the common sweep tick — so skip walking the rest of
        // the presence map for survivors there is no clear to suppress.
        if expired.is_empty() {
            return Vec::new();
        }
        // The `(room, actor, key)` triples a surviving client still holds after
        // the sweep — a second tab of the actor keeps the presence, so its
        // sibling's expiry must not clear it. One pass over what remains.
        let mut surviving: HashSet<(RoomId, Vec<u8>, Vec<u8>)> = HashSet::new();
        for (room, by_client) in &self.awareness {
            for keys in by_client.values() {
                for (key, p) in keys {
                    surviving.insert((room.clone(), p.actor.clone(), key.clone()));
                }
            }
        }
        // Suppress a clear a survivor still holds, then dedup (two tabs of one
        // actor can expire the same key at once).
        expired.retain(|triple| !surviving.contains(triple));
        expired.sort_unstable();
        expired.dedup();
        expired
            .into_iter()
            .map(|(room, actor, key)| AwarenessRemoval::Key { room, actor, key })
            .collect()
    }

    /// The rooms that currently hold any awareness presence — the sweep resolves
    /// a governing schema only for these, not for every room the hub serves.
    pub fn awareness_rooms(&self) -> impl Iterator<Item = &RoomId> {
        self.awareness.keys()
    }

    /// Whether `client` currently holds any awareness entry in any room — so a
    /// disconnect only starts a grace timer for a client whose presence a later
    /// sweep would actually clear.
    pub fn has_client_awareness(&self, client: ClientId) -> bool {
        self.awareness
            .values()
            .any(|by_client| by_client.get(&client).is_some_and(|keys| !keys.is_empty()))
    }

    /// Drop every awareness entry owned by `client` across all rooms, returning
    /// the removals the caller should tell each room's peers. Peers key presence
    /// by `(actor, key)`, so an actor with another live connection in the room
    /// (a second tab) keeps its presence: only the keys no surviving connection
    /// still holds are cleared, per-key. When no connection of the actor remains,
    /// the whole actor is cleared at once.
    pub fn clear_client_awareness(&mut self, client: ClientId) -> Vec<AwarenessRemoval> {
        let mut removals = Vec::new();
        for (room, by_client) in self.awareness.iter_mut() {
            let Some(removed) = by_client.remove(&client) else {
                continue;
            };
            let Some(first) = removed.values().next() else {
                continue;
            };
            let actor = first.actor.clone();
            let holds = |key: &[u8]| {
                by_client
                    .values()
                    .any(|keys| keys.get(key).is_some_and(|p| p.actor == actor))
            };
            let has_sibling = by_client
                .values()
                .any(|keys| keys.values().any(|p| p.actor == actor));
            if has_sibling {
                for key in removed.keys() {
                    if !holds(key) {
                        removals.push(AwarenessRemoval::Key {
                            room: room.clone(),
                            actor: actor.clone(),
                            key: key.clone(),
                        });
                    }
                }
            } else {
                removals.push(AwarenessRemoval::Actor {
                    room: room.clone(),
                    actor,
                });
            }
        }
        self.awareness.retain(|_, by_client| !by_client.is_empty());
        removals
    }

    /// Auto-compact a room once its retained log reaches `threshold` ops, folding
    /// the log into a snapshot in the same ingest that crosses it. The snapshot
    /// is persisted when a store is attached. `0` disables the policy, leaving
    /// compaction entirely to explicit [`compact`](Hub::compact) calls.
    pub fn set_compaction_threshold(&mut self, threshold: u64) {
        self.compaction_threshold = threshold;
    }

    /// A hub rebuilt from each room's persisted snapshot and log. A room with a
    /// snapshot loads its merged state and sequence floor from it, then replays
    /// the tail; one without replays its whole log from scratch. Either way the
    /// reloaded node reproduces the merged state, the server sequence, and the
    /// dedup set of the node that wrote the store. A corrupt snapshot is an
    /// error. The hub is in-memory until [`attach_store`](Hub::attach_store)
    /// makes further ingests durable.
    pub fn from_rooms(server: ClientId, rooms: Vec<(RoomId, RoomLog)>) -> io::Result<Self> {
        let mut hub = Self::new(server);
        for (room, log) in rooms {
            hub.install_room(room, log)?;
        }
        // Resume the capture order past every persisted ordinal, so a version
        // created after the restart never collides with or predates a restored one.
        hub.version_ordinal = hub
            .versions
            .values()
            .flat_map(|index| index.values())
            .map(|v| v.ordinal.saturating_add(1))
            .max()
            .unwrap_or(0);
        Ok(hub)
    }

    /// Restore one room from its snapshot (if any) and replay its retained log.
    /// A snapshot seeds the merged state, the sequence floor, and the dedup set;
    /// the log then replays through the same dedup as a live ingest, so a record
    /// the snapshot already covers is a no-op and a crash-left overlap converges.
    fn install_room(&mut self, room: RoomId, log: RoomLog) -> io::Result<()> {
        if let Some(snapshot) = log.snapshot {
            let doc = Document::decode_state(&snapshot.state)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{e:?}")))?;
            let seen = doc.seen().collect();
            self.rooms.insert(
                room.clone(),
                Room {
                    doc,
                    log: Vec::new(),
                    seen,
                    base_seq: snapshot.base_seq,
                    max_op_version: None,
                },
            );
        }
        // Store-less replay: these records are already durable and carry their
        // own creation versions, so replay commits them as-is (never re-tagging
        // the batch) and cannot fail.
        self.ingest_records(&room, log.ops)
            .expect("a store-less replay never fails");
        // Seed the durable governing metadata: the binding into the hub's mirror,
        // and the op-version high-water past what the replayed tail alone yields —
        // a compacted room's high-water counts ops folded into the snapshot, which
        // the tail no longer carries. The persisted value is the all-time
        // high-water, so it dominates the replay-derived one where they differ.
        if let Some(meta) = log.meta {
            if let Some(governing) = meta.governing {
                self.governing.insert(room.clone(), governing);
            }
            if let Some(persisted) = meta.max_op_version {
                if let Some(r) = self.rooms.get_mut(&room) {
                    r.max_op_version = r.max_op_version.max(Some(persisted));
                }
            }
        }
        if !log.versions.is_empty() {
            let index = self.versions.entry(room.clone()).or_default();
            for (name, seq, origin, ordinal, state) in log.versions {
                index.insert(
                    name,
                    Version {
                        seq,
                        origin,
                        ordinal,
                        state,
                    },
                );
            }
        }
        // Restore the room's forks; `main` is synthesized around them. An empty
        // set leaves the room with the lazy default `{main}` — no entry at all.
        if !log.branches.is_empty() {
            self.branches
                .insert(room.clone(), BranchRegistry::from_forks(log.branches));
        }
        // Restore each branch's divergent tail, seeding its dedup set from the
        // stored ops. A read-only publish target never diverges, so a tail persisted
        // under its name is a stale orphan (a former writable fork's, left by a
        // repoint whose best-effort tail removal failed) — dropped, so it never folds
        // onto the published base.
        if !log.branch_ops.is_empty() {
            let published: HashSet<Vec<u8>> = self
                .branches
                .get(&room)
                .map(|r| {
                    r.branches()
                        .filter(|b| b.published)
                        .map(|b| b.name.clone())
                        .collect()
                })
                .unwrap_or_default();
            let logs = self.branch_logs.entry(room.clone()).or_default();
            for (branch, ops) in log.branch_ops {
                if published.contains(&branch) {
                    continue;
                }
                let seen = ops.iter().map(|rec| rec.op.id).collect();
                logs.insert(branch, BranchLog { ops, seen });
            }
            if logs.is_empty() {
                self.branch_logs.remove(&room);
            }
        }
        // Restore each snapshot fork's owned base, so its catch-up serves the
        // version state it forked from rather than reading main's log. A base whose
        // branch is not a registered fork is an orphan — left by a crash between a
        // failed pointer persist and the base cleanup, or by a delete whose base
        // removal failed — and is dropped, so a stale base never shadows a later
        // live-log fork that reuses the name.
        if !log.branch_bases.is_empty() {
            let registered: HashSet<Vec<u8>> = self
                .branches
                .get(&room)
                .map(|r| r.branches().map(|b| b.name.clone()).collect())
                .unwrap_or_default();
            let bases = self.branch_bases.entry(room.clone()).or_default();
            for (branch, state) in log.branch_bases {
                if registered.contains(&branch) {
                    bases.insert(branch, state);
                }
            }
            if bases.is_empty() {
                self.branch_bases.remove(&room);
            }
        }
        // Restore the active-HEAD switch, so a default subscribe after a reload
        // follows the branch a restore made active. A pointer to a branch no longer
        // registered (its fork was deleted) falls back to `main`, so a plain
        // subscribe is never refused for a dangling HEAD.
        if let Some(branch) = log.active_branch {
            let known = branch == MAIN_BRANCH
                || self
                    .branches
                    .get(&room)
                    .is_some_and(|r| r.branch(&branch).is_some());
            if known && branch != MAIN_BRANCH {
                self.active_branch.insert(room.clone(), branch);
            }
        }
        // A branch's head is its fork point plus its tail length. Recompute it from
        // the restored tail so a crash between persisting the tail and the branch
        // pointer never leaves the head trailing the ops on disk.
        if let (Some(registry), Some(logs)) =
            (self.branches.get_mut(&room), self.branch_logs.get(&room))
        {
            for (branch, log) in logs {
                if let Some(fork) = registry.branch(branch).map(|b| b.fork_point) {
                    registry.set_head(branch, fork + log.ops.len() as u64);
                }
            }
        }
        Ok(())
    }

    /// Persist every future ingest to `store`. The rooms it already holds are
    /// assumed to be `store`'s contents, as [`from_rooms`](Hub::from_rooms)
    /// leaves them — this only redirects new writes to disk.
    pub fn attach_store(&mut self, store: Store) {
        self.store = Some(store);
    }

    /// Apply a client's ops to `room` (creating it if new), tagging each with
    /// the `schema_version` it was created under — the writing connection's
    /// enforced version, or `None` for a relay op with no schema. Skips any op
    /// already seen. A new op is durably logged before it is applied, so the
    /// merged state and the catch-up log never expose a write the disk has not
    /// accepted. Returns the ops newly applied, in server-sequence order — the
    /// batch to broadcast to the room's subscribers.
    pub fn ingest(
        &mut self,
        room: &[u8],
        ops: Vec<Op>,
        schema_version: Option<u32>,
    ) -> io::Result<Vec<Op>> {
        let records = ops
            .into_iter()
            .map(|op| StoredOp::new(op, schema_version))
            .collect();
        self.ingest_records(room, records)
    }

    /// Commit already-tagged records — the shared body of live [`ingest`](Hub::ingest)
    /// and store replay. Dedups against the room's seen set and within the batch,
    /// persists the fresh records (when a store is attached), then applies and
    /// logs them. Replay passes the records decoded from disk, preserving each
    /// op's own creation version rather than re-tagging the batch.
    fn ingest_records(&mut self, room: &[u8], records: Vec<StoredOp>) -> io::Result<Vec<Op>> {
        let server = self.server;
        let key = room;
        // The records not already logged, deduped within the batch too — the set
        // that would grow the log.
        let fresh: Vec<StoredOp> = {
            let room = self
                .rooms
                .entry(room.to_vec())
                .or_insert_with(|| Room::new(server));
            let mut batch = HashSet::new();
            records
                .into_iter()
                .filter(|rec| !room.seen.contains(&rec.op.id) && batch.insert(rec.op.id))
                .collect()
        };
        // Persist before committing: an op reaches the replica and the log only
        // once it is on disk, so a persist failure leaves no trace to advertise.
        if let Some(store) = self.store.as_mut() {
            store.append(room, &fresh)?;
        }
        let high_water_grew = {
            let room = self.rooms.get_mut(room).expect("room created above");
            let prev_high_water = room.max_op_version;
            for rec in &fresh {
                room.seen.insert(rec.op.id);
                room.doc.apply(&rec.op);
                room.max_op_version = room.max_op_version.max(rec.schema_version);
                room.log.push(rec.clone());
            }
            room.max_op_version != prev_high_water
        };
        // The op-version high-water grew, so its durable record is stale: persist
        // it beside the log now, before any compaction below drops the log the
        // high-water would otherwise have to be rebuilt from. Best-effort — the
        // metadata is a durability cache over derivable state, so a write failure
        // degrades to the rebuild-from-log fallback rather than failing the write.
        if high_water_grew {
            let _ = self.persist_meta(key);
        }
        // A retained log that has grown to the threshold folds into a snapshot
        // now, resetting the window; the applied batch is returned unchanged.
        if self.compaction_threshold > 0
            && self.rooms.get(key).map_or(0, |r| r.log.len() as u64) >= self.compaction_threshold
        {
            self.compact(key)?;
        }
        Ok(fresh.into_iter().map(|rec| rec.op).collect())
    }

    /// Apply a client's ops to a non-`main` branch of `room`, appending them to
    /// that branch's divergent tail and advancing its head — never `main`'s log.
    /// Each is tagged with the writer's `schema_version`, deduped against the
    /// branch's own seen set (and within the batch), and durably logged before it
    /// is applied. Returns the ops newly appended, in order — the batch to fan out
    /// to the `(room, branch)` stream's subscribers. A `main` branch delegates to
    /// [`ingest`](Hub::ingest); an unknown branch appends nothing.
    pub fn ingest_branch(
        &mut self,
        room: &[u8],
        branch: &[u8],
        ops: Vec<Op>,
        schema_version: Option<u32>,
    ) -> io::Result<Vec<Op>> {
        if branch == MAIN_BRANCH {
            return self.ingest(room, ops, schema_version);
        }
        // A non-`main` branch's fork point is a stored pointer (no `main`-head
        // overlay), so read it straight from the registry — no clone per write.
        let Some(fork_point) = self
            .branches
            .get(room)
            .and_then(|registry| registry.branch(branch))
            .map(|b| b.fork_point)
        else {
            return Ok(Vec::new());
        };
        let records: Vec<StoredOp> = ops
            .into_iter()
            .map(|op| StoredOp::new(op, schema_version))
            .collect();
        // The records not already in the branch's tail, deduped within the batch.
        let fresh: Vec<StoredOp> = {
            let log = self
                .branch_logs
                .entry(room.to_vec())
                .or_default()
                .entry(branch.to_vec())
                .or_default();
            let mut batch = HashSet::new();
            records
                .into_iter()
                .filter(|rec| !log.seen.contains(&rec.op.id) && batch.insert(rec.op.id))
                .collect()
        };
        // Persist before committing: a branch op reaches the tail only once it is
        // on disk, so a persist failure leaves no trace to advertise.
        if let Some(store) = self.store.as_mut() {
            store.append_branch(room, branch, &fresh)?;
        }
        let head = {
            let log = self
                .branch_logs
                .get_mut(room)
                .expect("tail created above")
                .get_mut(branch)
                .expect("tail created above");
            for rec in &fresh {
                log.seen.insert(rec.op.id);
                log.ops.push(rec.clone());
            }
            fork_point + log.ops.len() as u64
        };
        // Advance and persist the branch's head pointer beside its tail.
        self.mutate_branches(room, |registry| registry.set_head(branch, head))?;
        Ok(fresh.into_iter().map(|rec| rec.op).collect())
    }

    /// What a subscriber needs given the sequence it last saw. Above the
    /// compaction floor it gets the ops past `last_seen_seq` as a delta; below
    /// it — the ops it missed are compacted away — it gets a snapshot of the
    /// current state tagged with the head sequence. An unknown room yields an
    /// empty delta.
    pub fn catch_up(&mut self, room: &[u8], last_seen_seq: u64) -> Catchup {
        let Some(room) = self.rooms.get(room) else {
            return Catchup::Ops(Vec::new());
        };
        if last_seen_seq < room.base_seq {
            return Catchup::Snapshot {
                seq: room.head(),
                state: room.doc.encode_state(),
            };
        }
        // An offset past what the platform's usize can hold is far beyond the
        // head: nothing to send. The checked conversion avoids truncating it
        // back into the log's range.
        let Ok(start) = usize::try_from(last_seen_seq - room.base_seq) else {
            return Catchup::Ops(Vec::new());
        };
        let delta = room
            .log
            .get(start..)
            .map(|records| records.to_vec())
            .unwrap_or_default();
        Catchup::Ops(delta)
    }

    /// What a subscriber to the `(room, branch)` stream needs given the sequence
    /// it last saw: the shared base — `main`'s log records up to the branch's fork
    /// point — followed by the branch's own divergent tail past it. The base is
    /// never duplicated per branch; it is read straight from `main`'s log. Sequence
    /// numbering is the branch's own: a base record keeps its `main` sequence
    /// (≤ fork point), and a tail record at index `i` sits at `fork_point + i + 1`.
    /// A `main` branch is the whole log via [`catch_up`](Hub::catch_up); an unknown
    /// branch yields an empty delta.
    ///
    /// A snapshot-forked branch (one forked from a named version rather than a live
    /// log point) instead owns its base: it serves that version's materialized
    /// state — with its tail folded in — never main's log. See
    /// [`fork_branch_from_version`](Hub::fork_branch_from_version).
    pub fn catch_up_branch(&mut self, room: &[u8], branch: &[u8], last_seen_seq: u64) -> Catchup {
        if branch == MAIN_BRANCH {
            return self.catch_up(room, last_seen_seq);
        }
        let Some(fork_point) = self
            .branches
            .get(room)
            .and_then(|registry| registry.branch(branch))
            .map(|b| b.fork_point)
        else {
            return Catchup::Ops(Vec::new());
        };
        // A snapshot-forked branch owns its base — a version's materialized state
        // at `fork_point` — instead of sharing main's log. Its base and tail form a
        // self-contained stream: a fresh subscriber (below `fork_point`) is served
        // the base with the tail folded in as one whole-replica snapshot, while one
        // already past the base is served just the divergent tail.
        if let Some(base) = self.branch_bases.get(room).and_then(|m| m.get(branch)) {
            let tail = self
                .branch_logs
                .get(room)
                .and_then(|logs| logs.get(branch))
                .map(|log| log.ops.as_slice())
                .unwrap_or(&[]);
            if last_seen_seq < fork_point {
                let mut doc = match Document::decode_state(base) {
                    Ok(doc) => doc,
                    Err(_) => return Catchup::Ops(Vec::new()),
                };
                for rec in tail {
                    doc.apply(&rec.op);
                }
                return Catchup::Snapshot {
                    seq: fork_point + tail.len() as u64,
                    state: doc.encode_state(),
                };
            }
            let seen_in_tail = last_seen_seq.saturating_sub(fork_point);
            let delta = usize::try_from(seen_in_tail)
                .ok()
                .and_then(|start| tail.get(start..))
                .map(<[StoredOp]>::to_vec)
                .unwrap_or_default();
            return Catchup::Ops(delta);
        }
        let mut delta = Vec::new();
        // The shared base: `main`'s retained log records with sequence in
        // `(last_seen_seq, fork_point]`. A record at log index `i` carries sequence
        // `base_seq + i + 1`.
        if let Some(r) = self.rooms.get(room) {
            let base_end = fork_point.min(r.head());
            if base_end > last_seen_seq && base_end > r.base_seq {
                let lo = last_seen_seq.max(r.base_seq) - r.base_seq;
                let hi = base_end - r.base_seq;
                if let (Ok(lo), Ok(hi)) = (usize::try_from(lo), usize::try_from(hi)) {
                    if let Some(base) = r.log.get(lo..hi) {
                        delta.extend(base.iter().cloned());
                    }
                }
            }
        }
        // The branch's divergent tail: records past the fork point the subscriber
        // has not seen. A tail record at index `j` carries branch sequence
        // `fork_point + j + 1`.
        if let Some(log) = self.branch_logs.get(room).and_then(|logs| logs.get(branch)) {
            let seen_in_tail = last_seen_seq.saturating_sub(fork_point);
            if let Ok(start) = usize::try_from(seen_in_tail) {
                if let Some(tail) = log.ops.get(start..) {
                    delta.extend(tail.iter().cloned());
                }
            }
        }
        Catchup::Ops(delta)
    }

    /// Fold the room's logged ops into the merged replica and drop them,
    /// advancing the compaction floor to the head. The replica, the dedup set,
    /// and every op's sequence are untouched — only the retained log shrinks, so
    /// a below-floor subscriber is served a snapshot instead of a delta. With a
    /// store attached, the snapshot is persisted and the on-disk log truncated,
    /// so the reclaim survives a restart.
    pub fn compact(&mut self, room: &[u8]) -> io::Result<()> {
        let (floor, state, reclaimed) = match self.rooms.get_mut(room) {
            None => return Ok(()),
            Some(r) => {
                // An empty log reclaims nothing and cannot advance the floor; the
                // event is suppressed (as the version paths suppress their no-op),
                // though the snapshot is still re-persisted below.
                let reclaimed = !r.log.is_empty();
                r.base_seq += r.log.len() as u64;
                r.log.clear();
                (r.base_seq, r.doc.encode_state(), reclaimed)
            }
        };
        if let Some(store) = self.store.as_mut() {
            store.compact(room, floor, &state)?;
        }
        if reclaimed {
            self.emit(EngineEvent::Compacted { room, floor });
        }
        Ok(())
    }

    /// The room's whole-replica state as a portable snapshot — the bytes to move
    /// it to another node, back it up, or capture a debug repro. `None` for an
    /// unknown room. Import it elsewhere with [`import_room`](Hub::import_room).
    pub fn export_room(&self, room: &[u8]) -> Option<Vec<u8>> {
        self.rooms.get(room).map(|r| r.doc.encode_state())
    }

    /// Rebuild a room from a portable snapshot produced by
    /// [`export_room`](Hub::export_room). The merged state, element/client
    /// identities, and dedup set come back, so a client resending its ops is
    /// deduped exactly as against the origin. Returns `Ok(false)` — installing
    /// nothing — if `room` already exists: import is create-only, so moving onto
    /// live state needs an explicit delete first. Malformed bytes are an
    /// `InvalidData` error. With a store attached the snapshot is persisted
    /// before the room commits, so the import survives a restart.
    pub fn import_room(&mut self, room: &[u8], state: &[u8]) -> io::Result<bool> {
        if self.rooms.contains_key(room) {
            return Ok(false);
        }
        let doc = Document::decode_state(state)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{e:?}")))?;
        let seen: HashSet<OpId> = doc.seen().collect();
        // The whole imported history is folded into the snapshot, so its floor
        // sits at the op count — a fresh subscriber lands below it and is served
        // the state rather than an empty delta. Sequences renumber from here;
        // they are server-local, so a move never collides with the origin's.
        let base_seq = seen.len() as u64;
        if let Some(store) = self.store.as_mut() {
            store.compact(room, base_seq, state)?;
        }
        self.rooms.insert(
            room.to_vec(),
            Room {
                doc,
                log: Vec::new(),
                seen,
                base_seq,
                max_op_version: None,
            },
        );
        Ok(true)
    }

    /// Clone `src`'s live state into a fresh room `dst` — "duplicate this doc as a
    /// template". `dst` is created from `src`'s current whole-replica snapshot and
    /// takes further edits independently: its server sequences renumber from the
    /// clone's own floor, room-scoped, so they never collide with the origin's.
    ///
    /// Identities ride the snapshot, exactly as for [`import_room`](Hub::import_room):
    /// the clone comes up holding the origin's element ids and its op-dedup set. So
    /// a *new* author editing the clone diverges freely, but a client resending an
    /// op it already authored to the origin is deduped in the clone too — the same
    /// idempotency import gives a moved room, not a collision.
    ///
    /// Returns `Ok(false)` — cloning nothing — if `src` is unknown or `dst` already
    /// exists (clone is create-only, like import); with a store attached `dst` is
    /// persisted before it commits. The named-version index is not cloned: a
    /// template starts from the live state with a fresh version history.
    pub fn clone_room(&mut self, src: &[u8], dst: &[u8]) -> io::Result<bool> {
        let Some(state) = self.export_room(src) else {
            return Ok(false);
        };
        self.import_room(dst, &state)
    }

    /// The room's current high-water server sequence (0 if unseen or empty).
    pub fn seq(&self, room: &[u8]) -> u64 {
        self.rooms.get(room).map_or(0, Room::head)
    }

    /// Read the merged state of a top-level slot in `room`.
    pub fn get(&self, room: &[u8], key: &[u8]) -> Option<Element> {
        self.rooms.get(room).and_then(|r| r.doc.get(key))
    }

    /// The creation schema version of each op still retained in `room`'s log, in
    /// server-sequence order — `None` for a relay op. The heterogeneous log:
    /// ops from different schema versions coexist, each carrying its own, which
    /// per-recipient translation rewrites from. Empty for an unknown room.
    pub fn logged_versions(&self, room: &[u8]) -> Vec<Option<u32>> {
        self.rooms
            .get(room)
            .map(|r| r.log.iter().map(|rec| rec.schema_version).collect())
            .unwrap_or_default()
    }

    /// The governing app's op-version high-water for `room` — the highest op
    /// version ever folded into the merged replica, the worst-case op version a
    /// joiner must be able to down-reach to be served the whole state. It tracks
    /// the merged state, not the retained log, so compaction leaves it standing;
    /// relay and foreign-app ops are untagged and excluded. `None` when the room
    /// holds no governing-app op, so the handshake range-check has nothing to
    /// reach and the snapshot seam has no version to project from.
    pub fn max_op_version(&self, room: &[u8]) -> Option<u32> {
        self.rooms.get(room).and_then(|r| r.max_op_version)
    }

    /// The durable governing `{app_id, version}` bound to `room`, or `None` for an
    /// unbound room. The registry consults it to re-seed a live binding a dormant
    /// sweep dropped, or one a restart has not yet rebuilt from a live subscriber,
    /// so a populated room's first post-restart subscriber is served translated
    /// rather than verbatim. Empty on a store-less hub, which keeps the pure
    /// rebuild-on-subscribe behavior.
    pub fn governing_app(&self, room: &[u8]) -> Option<(Vec<u8>, u32)> {
        self.governing.get(room).cloned()
    }

    /// Bind `room`'s durable governing app to `{app_id, version}` and persist it
    /// beside the room's state, so the binding survives a restart and a
    /// dormant-room sweep. A no-op without a store attached — a binding is durable
    /// only where there is a store to hold it, and a store-less hub relies on the
    /// registry rebuilding the binding from live subscribers. Best-effort persist:
    /// the binding is derived state, so a write failure leaves it in the mirror to
    /// re-persist on the next bind rather than failing the caller.
    pub fn bind_governing(&mut self, room: &[u8], app_id: Vec<u8>, version: u32) {
        if self.store.is_none() {
            return;
        }
        let next = (app_id, version);
        if self.governing.get(room) == Some(&next) {
            return;
        }
        self.governing.insert(room.to_vec(), next);
        let _ = self.persist_meta(room);
    }

    /// Persist `room`'s governing metadata — the binding and the op-version
    /// high-water — to the store, if one is attached. The two fields are written
    /// together, each read from its own in-memory source, so a change to either
    /// re-emits the whole record.
    fn persist_meta(&mut self, room: &[u8]) -> io::Result<()> {
        if self.store.is_none() {
            return Ok(());
        }
        let meta = RoomMeta {
            governing: self.governing.get(room).cloned(),
            max_op_version: self.rooms.get(room).and_then(|r| r.max_op_version),
        };
        self.store
            .as_mut()
            .expect("store present, checked above")
            .write_meta(room, &meta)
    }

    /// Capture the room's current whole-replica state as a named version, keyed
    /// by `name`. Returns `Ok(false)` — capturing nothing — if the room is
    /// unknown or the name is already taken; a version is immutable, so a retake
    /// needs an explicit delete or a fresh name. With a store attached the index
    /// is persisted before the version is committed, so a persist failure leaves
    /// no version the disk has not accepted.
    pub fn create_version(&mut self, room: &[u8], name: &[u8]) -> io::Result<bool> {
        self.create_version_with(room, name, None, None)
    }

    /// Capture a version authored by an auto-version trigger, tagged with its
    /// `origin` (the trigger's stable identity) so [`retain_by_origin`] can prune
    /// that trigger's captures without touching a manual version or another
    /// trigger's. Otherwise identical to [`create_version`](Hub::create_version).
    pub(crate) fn create_auto_version(
        &mut self,
        room: &[u8],
        name: &[u8],
        origin: &[u8],
    ) -> io::Result<bool> {
        self.create_version_with(room, name, Some(origin), None)
    }

    /// Capture a version from an explicit whole-replica `state` covering server
    /// sequence `seq`, rather than the room's current live doc — the seam a
    /// [`publish`](Hub::publish) uses to record the published (editor-branch)
    /// snapshot, so each published state stays reachable for independent rollback.
    fn create_version_from_state(
        &mut self,
        room: &[u8],
        name: &[u8],
        seq: u64,
        state: Vec<u8>,
    ) -> io::Result<bool> {
        self.create_version_with(room, name, None, Some((seq, state)))
    }

    fn create_version_with(
        &mut self,
        room: &[u8],
        name: &[u8],
        origin: Option<&[u8]>,
        snapshot: Option<(u64, Vec<u8>)>,
    ) -> io::Result<bool> {
        let (seq, state) = match snapshot {
            Some(snapshot) => snapshot,
            None => {
                let Some(r) = self.rooms.get(room) else {
                    return Ok(false);
                };
                (r.head(), r.doc.encode_state())
            }
        };
        let version = Version {
            seq,
            origin: origin.map(<[u8]>::to_vec),
            ordinal: self.version_ordinal,
            state,
        };
        let index = self.versions.entry(room.to_vec()).or_default();
        if index.contains_key(name) {
            return Ok(false);
        }
        index.insert(name.to_vec(), version);
        // The ordinal is consumed only once the version is actually recorded, so a
        // no-op (unknown room / taken name) reuses it; a rolled-back persist leaves
        // a harmless gap, since only the relative order matters.
        self.version_ordinal += 1;
        if let Err(e) = self.persist_versions(room) {
            self.versions
                .get_mut(room)
                .expect("index created above")
                .remove(name);
            return Err(e);
        }
        self.emit(EngineEvent::VersionCreated { room, name });
        Ok(true)
    }

    /// Prune an auto-version trigger's captures to its `keep` retention window:
    /// keep the newest `keep` versions of `room` whose `origin` is this trigger's,
    /// deleting the older ones by capture order (the monotonic ordinal, so a
    /// backward clock step never misorders them). Only this trigger's own captures
    /// are eligible — a manual version (no origin) or another trigger's is never
    /// touched. Best-effort: a persist failure while deleting leaves an extra
    /// retained version, propagated so the caller can log it.
    pub(crate) fn retain_by_origin(
        &mut self,
        room: &[u8],
        origin: &[u8],
        keep: u64,
    ) -> io::Result<()> {
        let Some(index) = self.versions.get(room) else {
            return Ok(());
        };
        // Count in `u64` — a `keep` past `usize::MAX` (a 32-bit target) must not
        // truncate and prune. While the window is still filling this is the whole
        // cost: no sort, no allocation.
        let matches = index
            .values()
            .filter(|v| v.origin.as_deref() == Some(origin))
            .count();
        if matches as u64 <= keep {
            return Ok(());
        }
        // `keep` is now below the group size, so it fits `usize` losslessly.
        let remove = matches - keep as usize;
        // Partition the lowest `remove` ordinals (the oldest captures) to the front —
        // a linear select, not a full sort of the window, and no name is cloned until
        // it is known doomed.
        let mut by_ordinal: Vec<(u64, &[u8])> = index
            .iter()
            .filter(|(_, v)| v.origin.as_deref() == Some(origin))
            .map(|(name, v)| (v.ordinal, name.as_slice()))
            .collect();
        by_ordinal.select_nth_unstable_by_key(remove - 1, |&(ordinal, _)| ordinal);
        let doomed: Vec<Vec<u8>> = by_ordinal[..remove]
            .iter()
            .map(|&(_, name)| name.to_vec())
            .collect();
        drop(by_ordinal);

        // Evict the whole batch from the index, then persist once — not one atomic
        // rewrite (with its two fsyncs) per eviction. A persist failure restores the
        // entire batch, so retention never commits a partial prune.
        let index = self.versions.get_mut(room).expect("index present above");
        let evicted: Vec<(Vec<u8>, Version)> = doomed
            .into_iter()
            .map(|name| {
                let version = index.remove(&name).expect("name drawn from this index");
                (name, version)
            })
            .collect();
        if let Err(e) = self.persist_versions(room) {
            let index = self.versions.get_mut(room).expect("index present above");
            for (name, version) in evicted {
                index.insert(name, version);
            }
            return Err(e);
        }
        for (name, _) in &evicted {
            self.emit(EngineEvent::VersionDeleted { room, name });
        }
        Ok(())
    }

    /// The server sequence a named version covers, if it exists.
    pub fn version_seq(&self, room: &[u8], name: &[u8]) -> Option<u64> {
        self.versions.get(room)?.get(name).map(|v| v.seq)
    }

    /// The captured whole-replica state of a named version, for read / export /
    /// diff. Restoring it as live state is restore-as-branch, a separate layer.
    pub fn version_state(&self, room: &[u8], name: &[u8]) -> Option<&[u8]> {
        self.versions
            .get(room)?
            .get(name)
            .map(|v| v.state.as_slice())
    }

    /// The structural diff turning version `a`'s snapshot into version `b`'s: the
    /// [`Change`] list [`path::diff`](crdtsync_core::path::diff) computes over the
    /// two decoded whole-replica states. Diffing a version against itself is empty.
    /// An absent version is [`DiffError::UnknownVersion`]; a snapshot that does not
    /// decode is [`DiffError::Decode`] — never a panic.
    pub fn diff_versions(&self, room: &[u8], a: &[u8], b: &[u8]) -> Result<Vec<Change>, DiffError> {
        let old = self
            .version_state(room, a)
            .ok_or_else(|| DiffError::UnknownVersion(a.to_vec()))?;
        let new = self
            .version_state(room, b)
            .ok_or_else(|| DiffError::UnknownVersion(b.to_vec()))?;
        diff_states(old, new)
    }

    /// The structural diff turning branch `a`'s current state into branch `b`'s —
    /// each branch materialized (shared base plus divergent tail, or its owned
    /// snapshot base) then fed to the core engine, so a branch against its fork
    /// source yields only the divergence. Diffing a branch against itself is empty.
    /// An unknown branch is [`DiffError::UnknownBranch`]; a state that does not
    /// decode is [`DiffError::Decode`].
    pub fn diff_branches(
        &mut self,
        room: &[u8],
        a: &[u8],
        b: &[u8],
    ) -> Result<Vec<Change>, DiffError> {
        let old = self
            .materialize_branch(room, a)
            .ok_or_else(|| DiffError::UnknownBranch(a.to_vec()))?;
        let new = self
            .materialize_branch(room, b)
            .ok_or_else(|| DiffError::UnknownBranch(b.to_vec()))?;
        diff_states(&old, &new)
    }

    /// The names of a room's versions, sorted, for listing and pagination.
    pub fn version_names(&self, room: &[u8]) -> Vec<Vec<u8>> {
        self.versions
            .get(room)
            .into_iter()
            .flat_map(|index| index.keys().cloned())
            .collect()
    }

    /// Rename a version. Returns `Ok(false)` — changing nothing — if `from` is
    /// absent or `to` is already taken. The index is persisted before the rename
    /// commits when a store is attached.
    pub fn rename_version(&mut self, room: &[u8], from: &[u8], to: &[u8]) -> io::Result<bool> {
        let Some(index) = self.versions.get_mut(room) else {
            return Ok(false);
        };
        if !index.contains_key(from) || index.contains_key(to) {
            return Ok(false);
        }
        let mut version = index.remove(from).expect("presence checked above");
        // A rename is a deliberate operator act — the version is now curated, not a
        // disposable auto-capture, so detach it from its trigger's retention window.
        let prev_origin = version.origin.take();
        index.insert(to.to_vec(), version);
        if let Err(e) = self.persist_versions(room) {
            let index = self.versions.get_mut(room).expect("index present above");
            let mut version = index.remove(to).expect("just inserted");
            version.origin = prev_origin;
            index.insert(from.to_vec(), version);
            return Err(e);
        }
        self.emit(EngineEvent::VersionRenamed { room, from, to });
        Ok(true)
    }

    /// Delete a named version, returning whether one was removed. The index is
    /// persisted before the removal commits when a store is attached.
    pub fn delete_version(&mut self, room: &[u8], name: &[u8]) -> io::Result<bool> {
        let Some(index) = self.versions.get_mut(room) else {
            return Ok(false);
        };
        let Some(removed) = index.remove(name) else {
            return Ok(false);
        };
        if let Err(e) = self.persist_versions(room) {
            self.versions
                .get_mut(room)
                .expect("index present above")
                .insert(name.to_vec(), removed);
            return Err(e);
        }
        self.emit(EngineEvent::VersionDeleted { room, name });
        Ok(true)
    }

    /// Persist `room`'s version index to the store, if one is attached. The whole
    /// index is rewritten atomically — a version is immutable, but the set of
    /// versions is not.
    fn persist_versions(&mut self, room: &[u8]) -> io::Result<()> {
        let Some(store) = self.store.as_mut() else {
            return Ok(());
        };
        let empty = BTreeMap::new();
        let index = self.versions.get(room).unwrap_or(&empty);
        let records: Vec<(&[u8], u64, Option<&[u8]>, u64, &[u8])> = index
            .iter()
            .map(|(name, v)| {
                (
                    name.as_slice(),
                    v.seq,
                    v.origin.as_deref(),
                    v.ordinal,
                    v.state.as_slice(),
                )
            })
            .collect();
        store.write_versions(room, &records)
    }

    /// The room's branch registry as it should be observed — the stored forks
    /// plus a `main` whose head tracks the room's current log head. A room with no
    /// materialized entry observes the default `{main}`.
    fn observed_branches(&self, room: &[u8]) -> BranchRegistry {
        let mut registry = self.branches.get(room).cloned().unwrap_or_default();
        registry.set_main_head(self.rooms.get(room).map_or(0, Room::head));
        registry
    }

    /// The room's branches, in deterministic name order — always at least the
    /// default `main`, whose head tracks the room's log head.
    pub fn branches(&self, room: &[u8]) -> Vec<Branch> {
        self.observed_branches(room).branches().cloned().collect()
    }

    /// A room's branch by name, or `None` if it has no such branch. `main` always
    /// resolves.
    pub fn branch(&self, room: &[u8], name: &[u8]) -> Option<Branch> {
        self.observed_branches(room).branch(name).cloned()
    }

    /// Fork a fresh branch `new` off `from`, sharing its history up to position
    /// `at`. Returns `Ok(false)` — changing nothing — if `new` already exists or
    /// `from` is absent. With a store attached the set is persisted before the
    /// fork commits, so a persist failure leaves no branch the disk has not
    /// accepted.
    ///
    /// The fork point is clamped to the source's current head: a branch shares
    /// only history that exists, so forking past the source's head would leave a
    /// gap in the branch's sequence space (no ops between the head and `at`) and
    /// let the source's later writes into that gap leak into the branch's base.
    pub fn fork_branch(
        &mut self,
        room: &[u8],
        new: &[u8],
        from: &[u8],
        at: u64,
    ) -> io::Result<bool> {
        let at = match self.observed_branches(room).branch(from) {
            Some(source) => at.min(source.head),
            None => at,
        };
        self.mutate_branches(room, |registry| registry.fork(new, from, at))
    }

    /// Rename branch `from` to `to`. Returns `Ok(false)` — changing nothing — for
    /// the default `main`, an absent `from`, or a `to` already taken. Persisted
    /// before the rename commits when a store is attached.
    pub fn rename_branch(&mut self, room: &[u8], from: &[u8], to: &[u8]) -> io::Result<bool> {
        self.mutate_branches(room, |registry| registry.rename(from, to))
    }

    /// Delete branch `name`, returning whether one was removed. The default `main`
    /// is never deletable. Persisted before the removal commits when a store is
    /// attached. Its divergent tail is dropped with it — both in memory and on
    /// disk — so a later fork reusing the name never inherits stale ops.
    pub fn delete_branch(&mut self, room: &[u8], name: &[u8]) -> io::Result<bool> {
        let removed = self.mutate_branches(room, |registry| registry.delete(name))?;
        if removed {
            if let Some(logs) = self.branch_logs.get_mut(room) {
                logs.remove(name);
            }
            // A snapshot fork's owned base is dropped with it, so a later fork
            // reusing the name never inherits a stale base.
            if let Some(bases) = self.branch_bases.get_mut(room) {
                bases.remove(name);
            }
            if let Some(store) = self.store.as_mut() {
                store.remove_branch_log(room, name)?;
                store.remove_branch_base(room, name)?;
            }
            // Deleting the active HEAD resets it to `main`, so a default subscribe
            // is never left pointing at a branch that no longer exists.
            if self.active_branch.get(room).is_some_and(|b| b == name) {
                self.set_active_branch(room, MAIN_BRANCH)?;
            }
        }
        Ok(removed)
    }

    /// The room's active-HEAD branch — the branch a default (unnamed) subscribe
    /// follows. `main` until a restore-as-branch switches it, so an un-restored
    /// room behaves exactly as before.
    pub fn active_branch(&self, room: &[u8]) -> Vec<u8> {
        self.active_branch
            .get(room)
            .cloned()
            .unwrap_or_else(|| MAIN_BRANCH.to_vec())
    }

    /// Switch `room`'s active HEAD to `branch`, persisting it so the switch replays
    /// on reload. Setting it back to `main` clears the pointer (the default). A
    /// no-op if the branch is already active. Best-effort would lose the switch on
    /// a crash, so the persist is propagated: the pointer is durable before it is
    /// observed.
    pub fn set_active_branch(&mut self, room: &[u8], branch: &[u8]) -> io::Result<()> {
        let is_main = branch == MAIN_BRANCH;
        if self.active_branch(room) == branch {
            return Ok(());
        }
        if let Some(store) = self.store.as_mut() {
            store.write_active_branch(room, branch)?;
        }
        if is_main {
            self.active_branch.remove(room);
        } else {
            self.active_branch.insert(room.to_vec(), branch.to_vec());
        }
        Ok(())
    }

    /// Restore `room` to named version `version` as a fresh branch `new_branch`,
    /// switching the active HEAD to it — the first-class restore-as-branch op.
    ///
    /// It does not rewrite history or reset any sequence: it forks `new_branch`
    /// from the version's snapshot ([`fork_branch_from_version`](Hub::fork_branch_from_version)),
    /// captures an audit version of the pre-restore live (`main`) state, switches
    /// the active HEAD so a default subscribe now follows the restored branch, and
    /// emits [`EngineEvent::AfterRestore`]. The old branch is untouched — still
    /// subscribable by name — so an offline op in flight against the old HEAD lands
    /// on the old branch (its channel names it), never corrupting the restored
    /// state. Durable throughout, so the whole switch replays on reload.
    ///
    /// Returns `Ok(false)` — restoring nothing — if `version` is unknown or
    /// `new_branch` already exists.
    pub fn restore_as_branch(
        &mut self,
        room: &[u8],
        version: &[u8],
        new_branch: &[u8],
    ) -> io::Result<bool> {
        if !self.fork_branch_from_version(room, new_branch, version)? {
            return Ok(false);
        }
        // An audit version of the pre-restore live (`main`) state — a recoverable,
        // first-class record of the restore. Keyed on the branch and the captured
        // `main` sequence (the branch bytes verbatim, so an opaque name round-trips),
        // so a branch name reused after a delete still audits each restore: a later
        // restore captures a moved-on `main` under a new sequence, while a repeat at
        // the very same sequence (identical state) is a no-op the existing audit
        // already covers.
        let mut audit = b"audit/restore/".to_vec();
        audit.extend_from_slice(new_branch);
        audit.push(b'@');
        audit.extend_from_slice(self.seq(room).to_string().as_bytes());
        self.create_version(room, &audit)?;
        self.set_active_branch(room, new_branch)?;
        self.emit(EngineEvent::AfterRestore {
            room,
            branch: new_branch,
        });
        Ok(true)
    }

    /// Fork a fresh branch `new` off the snapshot of named version `version` — the
    /// deferred fork-from-snapshot base machinery. Unlike [`fork_branch`](Hub::fork_branch),
    /// which shares `main`'s live log up to a point, the new branch owns a copy of
    /// the version's materialized state at the sequence that version covered: its
    /// catch-up serves that state — never `main`'s later ops — and it survives the
    /// source version's deletion. Its divergent tail appends past the base exactly
    /// as a live-log fork's does.
    ///
    /// Returns `Ok(false)` — forking nothing — if `version` is unknown or `new`
    /// already exists. With a store attached the owned base is persisted before the
    /// branch pointer commits, so a persist failure leaves no branch whose base the
    /// disk has not accepted.
    pub fn fork_branch_from_version(
        &mut self,
        room: &[u8],
        new: &[u8],
        version: &[u8],
    ) -> io::Result<bool> {
        let Some((base_seq, state)) = self
            .versions
            .get(room)
            .and_then(|index| index.get(version))
            .map(|v| (v.seq, v.state.clone()))
        else {
            return Ok(false);
        };
        if self.observed_branches(room).branch(new).is_some() {
            return Ok(false);
        }
        // Persist the owned base before the pointer, so a crash never leaves a
        // snapshot fork whose base is missing.
        if let Some(store) = self.store.as_mut() {
            store.write_branch_base(room, new, &state)?;
        }
        self.branch_bases
            .entry(room.to_vec())
            .or_default()
            .insert(new.to_vec(), state);
        // Record the pointer at the version's covered sequence. The source-branch
        // check is satisfied by the always-present `main`; the name was checked
        // free above, so this only fails on a persist error — roll the base back.
        match self.mutate_branches(room, |registry| registry.fork(new, MAIN_BRANCH, base_seq)) {
            Ok(true) => Ok(true),
            other => {
                if let Some(bases) = self.branch_bases.get_mut(room) {
                    bases.remove(new);
                }
                if let Some(store) = self.store.as_mut() {
                    let _ = store.remove_branch_base(room, new);
                }
                other
            }
        }
    }

    /// Whether `(room, branch)` is a read-only publish target — a branch whose HEAD
    /// [`publish`](Hub::publish) advances and a client write never does. `main` is
    /// the editor branch, so it is never published.
    pub fn is_published(&self, room: &[u8], branch: &[u8]) -> bool {
        self.branches
            .get(room)
            .is_some_and(|registry| registry.is_published(branch))
    }

    /// Publish the active editor branch's current state onto the read-only
    /// `published` branch — the publish/draft workflow. Editors keep writing the
    /// editor branch (`main` by default); read-only consumers subscribe to
    /// `published` and are served the state as it stood at the last publish, until
    /// the next one. Returns `Ok(false)` — publishing nothing — for an empty/unknown
    /// room, or a `published` naming the editor branch or `main`.
    ///
    /// Republishing repoints `published`'s HEAD to the newer editor state. Each
    /// published state is first captured as an immutable named version
    /// (`publish/<published>@<seq>`), so the previous published state stays
    /// reachable — apps roll published state back independently of the editor
    /// branch. [`EngineEvent::BeforePublish`] fires before the repoint, so an
    /// `on: before-publish` auto-version trigger captures at the publish point.
    pub fn publish(&mut self, room: &[u8], published: &[u8]) -> io::Result<bool> {
        // `main` is the editor branch, never a publish target.
        if published == MAIN_BRANCH {
            return Ok(false);
        }
        // Publishing freezes the active editor branch's current state — `main` by
        // default, or a restored HEAD. A `published` that names the editor branch
        // would snapshot it onto itself, so it is refused.
        let source = self.active_branch(room);
        if source == published {
            return Ok(false);
        }
        let Some(state) = self.materialize_branch(room, &source) else {
            return Ok(false);
        };
        // The editor sequence being published — the source branch's head — names the
        // rollback version and marks the published fork point.
        let seq = self
            .branch(room, &source)
            .map_or_else(|| self.seq(room), |b| b.head);
        // BeforePublish fires before the repoint, so an `on: before-publish`
        // auto-version trigger captures at the publish point.
        self.emit(EngineEvent::BeforePublish {
            room,
            branch: published,
        });
        // Record the published state as an immutable named version, so this and
        // every prior published state stays reachable for independent rollback. The
        // name carries the source branch as well as its sequence: a branch's state at
        // a given head is fixed, so two publishes of the same `(source, seq)` are the
        // same state and reuse the name (a no-op the existing version covers), while
        // the same head number on a different editor branch stays a distinct version.
        let mut audit = b"publish/".to_vec();
        audit.extend_from_slice(published);
        audit.push(b'/');
        audit.extend_from_slice(&source);
        audit.push(b'@');
        audit.extend_from_slice(seq.to_string().as_bytes());
        self.create_version_from_state(room, &audit, seq, state.clone())?;
        self.repoint_published(room, published, seq, state)?;
        Ok(true)
    }

    /// Point the read-only `published` branch at the freshly published `state`,
    /// covering editor sequence `seq`. The published branch owns its base (the
    /// editor snapshot) and never carries a divergent tail — client writes to it are
    /// refused — so its catch-up serves that base to read-only consumers. The base
    /// is persisted before the pointer, so a crash never leaves a published branch
    /// whose base is missing.
    fn repoint_published(
        &mut self,
        room: &[u8],
        published: &[u8],
        seq: u64,
        state: Vec<u8>,
    ) -> io::Result<bool> {
        if let Some(store) = self.store.as_mut() {
            store.write_branch_base(room, published, &state)?;
        }
        let prev_base = self
            .branch_bases
            .entry(room.to_vec())
            .or_default()
            .insert(published.to_vec(), state);
        match self.mutate_branches(room, |registry| registry.point_published(published, seq)) {
            Ok(true) => {
                // The pointer committed. A published target never diverges — drop any
                // stale tail a name reused from a former writable branch left behind,
                // so its base alone serves it. Done only on success, so a failed
                // repoint leaves the prior branch's tail intact. The on-disk removal is
                // best-effort: the in-memory drop is authoritative, and the load path
                // drops a published branch's tail anyway, so an orphaned tail file
                // never folds onto the published base after a restart.
                if let Some(logs) = self.branch_logs.get_mut(room) {
                    logs.remove(published);
                }
                if let Some(store) = self.store.as_mut() {
                    let _ = store.remove_branch_log(room, published);
                }
                Ok(true)
            }
            other => {
                // The repoint did not commit — roll the base back to what it held, on
                // disk as well as in memory, so a persist failure never leaves the new
                // base beside the old pointer.
                let bases = self.branch_bases.entry(room.to_vec()).or_default();
                match prev_base {
                    Some(prev) => {
                        bases.insert(published.to_vec(), prev.clone());
                        if let Some(store) = self.store.as_mut() {
                            let _ = store.write_branch_base(room, published, &prev);
                        }
                    }
                    None => {
                        bases.remove(published);
                        if let Some(store) = self.store.as_mut() {
                            let _ = store.remove_branch_base(room, published);
                        }
                    }
                }
                other
            }
        }
    }

    /// The whole-replica state of `(room, branch)` — the bytes a publish freezes
    /// onto the published branch. `main` is the room's live replica; a named branch
    /// folds its own stream (shared base plus divergent tail, or its owned snapshot
    /// base) into one state. `None` for an unknown room or branch.
    fn materialize_branch(&mut self, room: &[u8], branch: &[u8]) -> Option<Vec<u8>> {
        if branch == MAIN_BRANCH {
            return self.export_room(room);
        }
        if self.branch(room, branch).is_none() {
            return None;
        }
        match self.catch_up_branch(room, branch, 0) {
            Catchup::Snapshot { state, .. } => Some(state),
            Catchup::Ops(ops) => {
                let mut doc = Document::new(self.server);
                for rec in &ops {
                    doc.apply(&rec.op);
                }
                Some(doc.encode_state())
            }
        }
    }

    /// Apply `change` to `room`'s registry, persisting the result before it
    /// commits. A no-op change (the closure returns `false`) installs nothing, so
    /// a never-forked room keeps no per-room entry; a persist failure rolls the
    /// registry back to its pre-change state, so it never reflects a set the disk
    /// rejected.
    fn mutate_branches(
        &mut self,
        room: &[u8],
        change: impl FnOnce(&mut BranchRegistry) -> bool,
    ) -> io::Result<bool> {
        // Work on a copy of the room's registry (the default `{main}` when it has
        // none), so a refused change leaves the map untouched — a room only
        // materializes an entry once a change actually takes.
        let mut registry = self.branches.get(room).cloned().unwrap_or_default();
        if !change(&mut registry) {
            return Ok(false);
        }
        let previous = self.branches.insert(room.to_vec(), registry);
        if let Err(e) = self.persist_branches(room) {
            match previous {
                Some(prev) => {
                    self.branches.insert(room.to_vec(), prev);
                }
                None => {
                    self.branches.remove(room);
                }
            }
            return Err(e);
        }
        Ok(true)
    }

    /// Persist `room`'s forks to the store, if one is attached. Only the forks
    /// past the default `main` are written; an empty set removes the file,
    /// restoring the room to `{main}`.
    fn persist_branches(&mut self, room: &[u8]) -> io::Result<()> {
        let Some(store) = self.store.as_mut() else {
            return Ok(());
        };
        let empty = BranchRegistry::default();
        let registry = self.branches.get(room).unwrap_or(&empty);
        let forks: Vec<Branch> = registry.forks().cloned().collect();
        store.write_branches(room, &forks)
    }
}
