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
    /// The server sequence each follower has confirmed for a room. Majority-ack
    /// durability reads this to gate a client's write-ack on a majority of replicas
    /// holding the write, so it must never claim a follower holds more than the leader
    /// produced. Advanced monotonically by [`record_ack`](Self::record_ack) as acks
    /// arrive; set outright (and possibly lowered) by [`set_watermark`](Self::set_watermark)
    /// when a rejoining follower reports its true durable head.
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

    /// Set `follower`'s watermark for `room` to exactly `through_seq`, replacing the
    /// recorded value even when that LOWERS it. Unlike [`record_ack`](Self::record_ack)
    /// this is not monotonic: it exists for the wiped-follower self-heal, where a
    /// follower reports a durable head below what it had previously acked and the
    /// leader must honor that true head over the stale ack — so majority-ack durability
    /// stops counting the follower toward quorum for data it can no longer prove. The
    /// caller must pass a value no greater than the leader's own head (a follower
    /// cannot hold ops the leader never produced); crediting it higher would falsely
    /// satisfy quorum.
    pub fn set_watermark(&mut self, follower: NodeId, room: &[u8], through_seq: u64) {
        self.acked.insert((room.to_vec(), follower), through_seq);
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
