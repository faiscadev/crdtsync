//! Single-node sync server core.
//!
//! A [`Hub`] owns one authoritative replica per room plus that room's
//! append-only op log. Clients ingest ops; the hub deduplicates by op id,
//! folds each new op into the room's replica, and assigns it a monotonic
//! server sequence. A subscriber names the last sequence it saw and the hub
//! replays everything past it — the log a fresh replica replays back to the
//! same state. Pure state; the transport wraps it.

use std::collections::{HashMap, HashSet};
use std::io;

use crdtsync_core::op::OpId;
use crdtsync_core::{ClientId, Document, Element, Op};

pub mod auth;
pub mod clock;
pub mod registry;
pub mod runtime;
pub mod session;
pub mod store;
pub use auth::{AllowAll, Verifier};
pub use clock::{Clock, ManualClock, SystemClock};
pub use registry::{ConnId, Registry};
pub use session::{negotiate, step, AwarenessBroadcast, Response, Session};
pub use store::{RoomLog, Snapshot, Store};

/// A room name, opaque bytes chosen by the deployment.
pub type RoomId = Vec<u8>;

/// One room's authoritative replica and its op log. A server sequence is a
/// 1-based position across the room's whole history; `base_seq` counts the ops
/// already compacted away (sequences `1..=base_seq`), so a retained op at
/// `log[i]` carries seq `base_seq + i + 1`.
struct Room {
    doc: Document,
    log: Vec<Op>,
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
    /// server-sequence order.
    Ops(Vec<Op>),
    /// The subscriber fell below the floor: load this whole-replica state, then
    /// treat `seq` as the sequence it has now caught up to.
    Snapshot { seq: u64, state: Vec<u8> },
}

/// The set of rooms a single node serves, optionally over a durable log.
pub struct Hub {
    server: ClientId,
    rooms: HashMap<RoomId, Room>,
    store: Option<Store>,
    compaction_threshold: u64,
    /// Ephemeral presence per room: each owner client's latest entry per key,
    /// with the actor to surface it under. Never persisted or snapshotted.
    awareness: HashMap<RoomId, HashMap<(ClientId, Vec<u8>), (Vec<u8>, Vec<u8>)>>,
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
        }
    }

    /// Record `client`'s ephemeral awareness entry `key` in `room`, last-writer-
    /// wins, so a later subscriber can be replayed the current presence.
    pub fn set_awareness(
        &mut self,
        room: &[u8],
        client: ClientId,
        actor: Vec<u8>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) {
        self.awareness
            .entry(room.to_vec())
            .or_default()
            .insert((client, key), (actor, value));
    }

    /// The current awareness entries in `room` as `(actor, key, value)`, for
    /// replaying presence to a joining subscriber.
    pub fn awareness_entries(&self, room: &[u8]) -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        self.awareness
            .get(room)
            .into_iter()
            .flatten()
            .map(|((_, key), (actor, value))| (actor.clone(), key.clone(), value.clone()))
            .collect()
    }

    /// Whether `client` currently holds any awareness entry in any room — so a
    /// disconnect only starts a grace timer for a client whose presence a later
    /// sweep would actually clear.
    pub fn has_client_awareness(&self, client: ClientId) -> bool {
        self.awareness
            .values()
            .any(|entries| entries.keys().any(|(owner, _)| *owner == client))
    }

    /// Drop every awareness entry owned by `client` across all rooms, returning
    /// the `(room, actor)` pairs cleared so the caller can tell each room's peers
    /// the presence expired. A client holds one actor per room, so at most one
    /// pair per room it had presence in.
    pub fn clear_client_awareness(&mut self, client: ClientId) -> Vec<(RoomId, Vec<u8>)> {
        let mut cleared = Vec::new();
        for (room, entries) in self.awareness.iter_mut() {
            let mut actor = None;
            entries.retain(|(owner, _), (a, _)| {
                if *owner == client {
                    actor.get_or_insert_with(|| a.clone());
                    false
                } else {
                    true
                }
            });
            if let Some(actor) = actor {
                cleared.push((room.clone(), actor));
            }
        }
        cleared
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
        // Store-less replay: these ops are already durable, so ingest can't fail.
        self.ingest(&room, log.ops)
            .expect("a store-less replay never fails");
        Ok(())
    }

    /// Persist every future ingest to `store`. The rooms it already holds are
    /// assumed to be `store`'s contents, as [`from_rooms`](Hub::from_rooms)
    /// leaves them — this only redirects new writes to disk.
    pub fn attach_store(&mut self, store: Store) {
        self.store = Some(store);
    }

    /// Apply a client's ops to `room` (creating it if new), skipping any op
    /// already seen. A new op is durably logged before it is applied, so the
    /// merged state and the catch-up log never expose a write the disk has not
    /// accepted. Returns the ops newly applied, in server-sequence order — the
    /// batch to broadcast to the room's subscribers.
    pub fn ingest(&mut self, room: &[u8], ops: Vec<Op>) -> io::Result<Vec<Op>> {
        let server = self.server;
        let key = room;
        // The ops not already logged, deduped within the batch too — the set
        // that would grow the log.
        let fresh: Vec<Op> = {
            let room = self
                .rooms
                .entry(room.to_vec())
                .or_insert_with(|| Room::new(server));
            let mut batch = HashSet::new();
            ops.into_iter()
                .filter(|op| !room.seen.contains(&op.id) && batch.insert(op.id))
                .collect()
        };
        // Persist before committing: an op reaches the replica and the log only
        // once it is on disk, so a persist failure leaves no trace to advertise.
        if let Some(store) = self.store.as_mut() {
            store.append(room, &fresh)?;
        }
        let room = self.rooms.get_mut(room).expect("room created above");
        for op in &fresh {
            room.seen.insert(op.id);
            room.doc.apply(op);
            room.log.push(op.clone());
        }
        // A retained log that has grown to the threshold folds into a snapshot
        // now, resetting the window; the applied batch is returned unchanged.
        if self.compaction_threshold > 0
            && self.rooms.get(key).map_or(0, |r| r.log.len() as u64) >= self.compaction_threshold
        {
            self.compact(key)?;
        }
        Ok(fresh)
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
            .map(<[Op]>::to_vec)
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
        Ok(())
    }

    /// The room's current high-water server sequence (0 if unseen or empty).
    pub fn seq(&self, room: &[u8]) -> u64 {
        self.rooms.get(room).map_or(0, Room::head)
    }

    /// Read the merged state of a top-level slot in `room`.
    pub fn get(&self, room: &[u8], key: &[u8]) -> Option<Element> {
        self.rooms.get(room).and_then(|r| r.doc.get(key))
    }
}
