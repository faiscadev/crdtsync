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

use crdtsync_core::op::OpId;
use crdtsync_core::{ClientId, Document, Element, Op, Schema};

pub mod acl;
pub mod admin;
pub mod audit;
pub mod auth;
pub mod authz;
pub mod clock;
pub mod event;
pub mod registry;
pub mod runtime;
pub mod schema_registry;
pub mod session;
pub mod store;
pub mod translate;
pub use admin::{admin_router, register_schema, serve_admin, RegisterOutcome, RegisterRequest};
pub use auth::{AllowAll, Identity, StaticTokens, Verifier};
pub use authz::{Action, Authorizer, PermitAll, Resource};
pub use clock::{Clock, ManualClock, SystemClock};
pub use event::{EngineEvent, EventSink};
pub use registry::{ConnId, Registry};
pub use schema_registry::{RegisterError, Registered, Resolution, SchemaRegistry};
pub use session::{negotiate, step, AwarenessBroadcast, Response, Session};
pub use store::{RoomLog, Snapshot, Store, StoredOp};

/// A room name, opaque bytes chosen by the deployment.
pub type RoomId = Vec<u8>;

/// One room's authoritative replica and its op log. A server sequence is a
/// 1-based position across the room's whole history; `base_seq` counts the ops
/// already compacted away (sequences `1..=base_seq`), so a retained op at
/// `log[i]` carries seq `base_seq + i + 1`.
struct Room {
    doc: Document,
    log: Vec<StoredOp>,
    seen: HashSet<OpId>,
    base_seq: u64,
}

impl Room {
    fn new(server: ClientId) -> Self {
        Self {
            doc: Document::new(server),
            log: Vec::new(),
            seen: HashSet::new(),
            base_seq: 0,
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
    /// Ephemeral presence per room: each owner client's latest [`Presence`] per
    /// key. Never persisted or snapshotted. Nesting by client keeps the per-client
    /// key cap an O(1) check and lets a disconnect find a client's own entries
    /// directly.
    awareness: HashMap<RoomId, HashMap<ClientId, HashMap<Vec<u8>, Presence>>>,
    /// Named versions per room, keyed by name — sorted, for listing/pagination.
    /// The in-memory versions index over the snapshot storage primitive.
    versions: HashMap<RoomId, BTreeMap<Vec<u8>, Version>>,
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
            awareness: HashMap::new(),
            versions: HashMap::new(),
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
                },
            );
        }
        // Store-less replay: these records are already durable and carry their
        // own creation versions, so replay commits them as-is (never re-tagging
        // the batch) and cannot fail.
        self.ingest_records(&room, log.ops)
            .expect("a store-less replay never fails");
        if !log.versions.is_empty() {
            let index = self.versions.entry(room).or_default();
            for (name, seq, state) in log.versions {
                index.insert(name, Version { seq, state });
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
        let room = self.rooms.get_mut(room).expect("room created above");
        for rec in &fresh {
            room.seen.insert(rec.op.id);
            room.doc.apply(&rec.op);
            room.log.push(rec.clone());
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

    /// Fold the room's logged ops into the merged replica and drop them,
    /// advancing the compaction floor to the head. The replica, the dedup set,
    /// and every op's sequence are untouched — only the retained log shrinks, so
    /// a below-floor subscriber is served a snapshot instead of a delta. With a
    /// store attached, the snapshot is persisted and the on-disk log truncated,
    /// so the reclaim survives a restart.
    pub fn compact(&mut self, room: &[u8]) -> io::Result<()> {
        let snapshot = match self.rooms.get_mut(room) {
            None => return Ok(()),
            Some(r) => {
                r.base_seq += r.log.len() as u64;
                r.log.clear();
                (r.base_seq, r.doc.encode_state())
            }
        };
        if let Some(store) = self.store.as_mut() {
            store.compact(room, snapshot.0, &snapshot.1)?;
        }
        self.emit(EngineEvent::Compacted {
            room,
            floor: snapshot.0,
        });
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
            },
        );
        Ok(true)
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

    /// Capture the room's current whole-replica state as a named version, keyed
    /// by `name`. Returns `Ok(false)` — capturing nothing — if the room is
    /// unknown or the name is already taken; a version is immutable, so a retake
    /// needs an explicit delete or a fresh name. With a store attached the index
    /// is persisted before the version is committed, so a persist failure leaves
    /// no version the disk has not accepted.
    pub fn create_version(&mut self, room: &[u8], name: &[u8]) -> io::Result<bool> {
        let Some(r) = self.rooms.get(room) else {
            return Ok(false);
        };
        let version = Version {
            seq: r.head(),
            state: r.doc.encode_state(),
        };
        let index = self.versions.entry(room.to_vec()).or_default();
        if index.contains_key(name) {
            return Ok(false);
        }
        index.insert(name.to_vec(), version);
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
        let version = index.remove(from).expect("presence checked above");
        index.insert(to.to_vec(), version);
        if let Err(e) = self.persist_versions(room) {
            let index = self.versions.get_mut(room).expect("index present above");
            let version = index.remove(to).expect("just inserted");
            index.insert(from.to_vec(), version);
            return Err(e);
        }
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
        let records: Vec<(&[u8], u64, &[u8])> = index
            .iter()
            .map(|(name, v)| (name.as_slice(), v.seq, v.state.as_slice()))
            .collect();
        store.write_versions(room, &records)
    }
}
