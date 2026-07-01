//! Single-node sync server core.
//!
//! A [`Hub`] owns one authoritative replica per room plus that room's
//! append-only op log. Clients ingest ops; the hub deduplicates by op id,
//! folds each new op into the room's replica, and assigns it a monotonic
//! server sequence. A subscriber names the last sequence it saw and the hub
//! replays everything past it — the log a fresh replica replays back to the
//! same state. Pure state; the transport wraps it.

use std::collections::{HashMap, HashSet};

use crdtsync_core::op::OpId;
use crdtsync_core::{ClientId, Document, Element, Op};

pub mod session;
pub use session::{negotiate, step, Response, Session};

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

/// The set of rooms a single node serves.
pub struct Hub {
    server: ClientId,
    rooms: HashMap<RoomId, Room>,
}

impl Hub {
    /// A hub whose per-room replicas are owned by `server`.
    pub fn new(server: ClientId) -> Self {
        Self {
            server,
            rooms: HashMap::new(),
        }
    }

    /// Apply a client's ops to `room` (creating it if new), skipping any op
    /// already seen. Returns the ops newly applied, in server-sequence order —
    /// the batch to broadcast to the room's subscribers.
    pub fn ingest(&mut self, room: &[u8], ops: Vec<Op>) -> Vec<Op> {
        let server = self.server;
        let room = self
            .rooms
            .entry(room.to_vec())
            .or_insert_with(|| Room::new(server));
        let mut applied = Vec::new();
        for op in ops {
            // A resend replays ops the room already logged; the log is the
            // dedup authority so replays never grow it.
            if !room.seen.insert(op.id) {
                continue;
            }
            room.doc.apply(&op);
            room.log.push(op.clone());
            applied.push(op);
        }
        applied
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
