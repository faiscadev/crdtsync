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

pub mod registry;
pub mod runtime;
pub mod session;
pub mod store;
pub use registry::{ConnId, Registry};
pub use session::{negotiate, step, Response, Session};
pub use store::Store;

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
}

impl Hub {
    /// An in-memory hub whose per-room replicas are owned by `server`.
    pub fn new(server: ClientId) -> Self {
        Self {
            server,
            rooms: HashMap::new(),
            store: None,
        }
    }

    /// A hub rebuilt by replaying each room's persisted log. Replaying the ops
    /// restores the merged state, the server sequence, and the dedup set, so a
    /// reloaded node is indistinguishable from the one that wrote the log. The
    /// hub is in-memory until [`attach_store`](Hub::attach_store) makes further
    /// ingests durable.
    pub fn from_logs(server: ClientId, logs: Vec<(RoomId, Vec<Op>)>) -> Self {
        let mut hub = Self::new(server);
        for (room, ops) in logs {
            // Replay is in-memory: these ops are already on disk.
            hub.ingest(&room, ops)
                .expect("a store-less replay never fails");
        }
        hub
    }

    /// Persist every future ingest to `store`. The log it already holds is
    /// assumed to be `store`'s contents, as [`from_logs`](Hub::from_logs) leaves
    /// it — this only redirects new writes to disk.
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
        let start = (last_seen_seq - room.base_seq) as usize;
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
    /// a below-floor subscriber is served a snapshot instead of a delta.
    ///
    /// In-memory only: the durable log on disk is unchanged, so a restart
    /// replays it and re-derives the same state and sequence. Compacting the
    /// on-disk log is a separate concern.
    pub fn compact(&mut self, room: &[u8]) {
        if let Some(room) = self.rooms.get_mut(room) {
            room.base_seq += room.log.len() as u64;
            room.log.clear();
        }
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
