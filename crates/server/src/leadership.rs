//! Per-room leadership epochs — the split-brain fence for cluster failover.
//!
//! Failover (Unit 6a) promotes a room's next live replica when its placement
//! primary's relay link drops, but liveness alone cannot tell a *recovered* stale
//! primary that it no longer leads: it and the promoted leader could both act
//! (split-brain), and the stale one could replicate writes it never lost. A
//! per-room leadership *epoch* — monotone, and exactly Raft's `term` — fences
//! that. A node stamps every outbound [`Replicate`](crdtsync_core::Message)
//! with the epoch at which it leads the room, a promotion leads at an epoch
//! strictly greater than any the promoting node has seen, and a follower rejects
//! a frame whose epoch is below the highest it has seen. A stale leader (lower
//! epoch) is thus fenced, and a leader that observes a higher epoch steps down.
//!
//! Held per node (a mutable sibling of the static [`Membership`](crate::membership::Membership),
//! which stays a pure placement view): the highest epoch seen per room, and the
//! epoch at which this node currently leads each room, if it does.

use std::collections::HashMap;

use crate::RoomId;

/// A node's per-room leadership-epoch state: the fence reference (highest epoch
/// seen) and this node's own leadership claim per room. Empty by default —
/// inert until the node leads or applies a replicated frame, so single-node and
/// steady-state clusters never touch it.
#[derive(Default)]
pub struct LeadershipEpochs {
    /// The highest epoch this node has observed for a room, from any source: a
    /// leader's own claim, or an applied inbound [`Replicate`](crdtsync_core::Message).
    /// Monotone. A frame below it is a stale leader's and is fenced.
    seen: HashMap<RoomId, u64>,
    /// The epoch at which this node currently leads a room, present only while it
    /// claims that room's leadership. Dropped when a higher epoch supersedes it.
    led: HashMap<RoomId, u64>,
}

impl LeadershipEpochs {
    /// The highest epoch this node has seen for `room` — `0` if none. A follower
    /// fences a [`Replicate`](crdtsync_core::Message) whose epoch is below this.
    pub fn highest_seen(&self, room: &[u8]) -> u64 {
        self.seen.get(room).copied().unwrap_or(0)
    }

    /// Record `epoch` as seen for `room`; monotone, never lowered.
    pub fn observe(&mut self, room: &[u8], epoch: u64) {
        let e = self.seen.entry(room.to_vec()).or_insert(0);
        *e = (*e).max(epoch);
    }

    /// Claim (or keep) this node's leadership epoch for `room`, returning the
    /// epoch to stamp on outbound frames. A node already leading at an epoch no
    /// lower than everything it has seen keeps that epoch, so a steady leader's
    /// frames all carry one stable epoch. Otherwise it opens a fresh generation
    /// strictly greater than any epoch seen — a promotion bumps the epoch — and
    /// records it as both its leadership claim and the highest seen.
    pub fn claim_leadership(&mut self, room: &[u8]) -> u64 {
        let seen = self.highest_seen(room);
        match self.led.get(room).copied() {
            Some(e) if e >= seen => e,
            _ => {
                let epoch = seen + 1;
                self.led.insert(room.to_vec(), epoch);
                self.seen.insert(room.to_vec(), epoch);
                epoch
            }
        }
    }

    /// Whether `epoch` would supersede this node's leadership of `room` — it leads
    /// the room at a strictly lower epoch. A non-mutating predicate: the apply gate
    /// consults it to decide whether to defer to a newer leader before committing
    /// to the frame.
    pub fn leads_below(&self, room: &[u8], epoch: u64) -> bool {
        self.led.get(room).is_some_and(|&e| epoch > e)
    }

    /// Step this node down from leading `room` if `epoch` is above the epoch it
    /// leads at — a newer leadership generation exists, so a stale leader defers
    /// to it. A node not leading `room`, or leading at an epoch already `>= epoch`,
    /// is unchanged.
    pub fn supersede_if_leading(&mut self, room: &[u8], epoch: u64) {
        if self.leads_below(room, epoch) {
            self.led.remove(room);
        }
    }
}
