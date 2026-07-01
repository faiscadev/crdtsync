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

/// One room's authoritative replica and its durable op log. A server sequence
/// is a 1-based position in `log`, so `log[i]` carries seq `i + 1`.
struct Room {
    doc: Document,
    log: Vec<Op>,
    seen: HashSet<OpId>,
}

impl Room {
    fn new(server: ClientId) -> Self {
        Self {
            doc: Document::new(server),
            log: Vec::new(),
            seen: HashSet::new(),
        }
    }
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

    /// The catch-up batch for a subscriber: every op with server-sequence
    /// greater than `last_seen_seq`, in order. Seq 0 yields the whole log.
    pub fn catch_up(&mut self, room: &[u8], last_seen_seq: u64) -> Vec<Op> {
        let Some(room) = self.rooms.get(room) else {
            return Vec::new();
        };
        let start = usize::try_from(last_seen_seq).unwrap_or(usize::MAX);
        match room.log.get(start..) {
            Some(rest) => rest.to_vec(),
            None => Vec::new(),
        }
    }

    /// The room's current high-water server sequence (0 if unseen or empty).
    pub fn seq(&self, room: &[u8]) -> u64 {
        self.rooms.get(room).map_or(0, |r| r.log.len() as u64)
    }

    /// Read the merged state of a top-level slot in `room`.
    pub fn get(&self, room: &[u8], key: &[u8]) -> Option<Element> {
        self.rooms.get(room).and_then(|r| r.doc.get(key))
    }
}
