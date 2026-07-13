//! Leader-to-follower op replication for the horizontal-scaling cluster.
//!
//! A room's leader (its placement primary) is the only node that serves the
//! room's writes; a subscribe or a stray write reaching a follower is redirected
//! there (Unit 3). For that redirect to land on a node that already holds the
//! room's state, the leader mirrors every commit to its follower replicas: after
//! a write advances the room's server sequence, the leader fans the fresh ops to
//! each other member of `replicas_for(room)` as a [`Message::Replicate`], and
//! each follower applies them into its own replica and answers a
//! [`Message::ReplicaAck`] naming the sequence it has reached.
//!
//! This holds the primary-side state of that exchange: the frames queued for
//! each follower and the per-`(room, follower)` acknowledged watermark. The
//! watermark is what a later majority-ack durability unit reads to decide a
//! write is safely replicated; this unit only records it.

use std::collections::HashMap;

use crdtsync_core::Message;

use crate::placement::NodeId;
use crate::RoomId;

/// The leader's replication bookkeeping: frames awaiting dispatch to each
/// follower, and the acknowledged server-sequence watermark per
/// `(room, follower)`.
#[derive(Default)]
pub struct Replication {
    /// [`Message::Replicate`] frames queued for their target follower, in commit
    /// order. Drained by the transport after each delivery and sent over that
    /// follower's peer connection.
    pending: Vec<(NodeId, Message)>,
    /// The highest server sequence each follower has confirmed for a room. Unit 5
    /// reads this to gate a client ack on a majority having the write; this unit
    /// only advances it as acks arrive.
    acked: HashMap<(RoomId, NodeId), u64>,
}

impl Replication {
    /// Queue `frame` for `follower`, to be dispatched over its peer connection.
    pub fn enqueue(&mut self, follower: NodeId, frame: Message) {
        self.pending.push((follower, frame));
    }

    /// Take every queued frame, leaving the queue empty — the transport routes
    /// each to its follower's peer connection.
    pub fn take_pending(&mut self) -> Vec<(NodeId, Message)> {
        std::mem::take(&mut self.pending)
    }

    /// Advance `follower`'s acknowledged watermark for `room` to `through_seq`.
    /// Monotonic: a stale or reordered ack never moves it backward.
    pub fn record_ack(&mut self, follower: NodeId, room: &[u8], through_seq: u64) {
        let entry = self.acked.entry((room.to_vec(), follower)).or_insert(0);
        *entry = (*entry).max(through_seq);
    }

    /// The server sequence `follower` has acknowledged for `room` — `0` if it has
    /// acknowledged nothing yet.
    pub fn watermark(&self, room: &[u8], follower: &NodeId) -> u64 {
        self.acked
            .get(&(room.to_vec(), follower.clone()))
            .copied()
            .unwrap_or(0)
    }
}
