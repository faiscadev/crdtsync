//! Read-only point-in-time reconstruction of a persisted room.
//!
//! A room's durable state is its nearest compaction snapshot plus the op tail
//! retained past it (see [`store`](crate::store)). Reconstructing the room as of
//! a past server sequence is therefore: seed from the snapshot (the state at its
//! base sequence), then replay the retained ops up to the target — exactly the
//! restore path [`Hub::from_rooms`] runs at startup, driven over a truncated log.
//! Because op-join and snapshot-join converge to the same merged state, the
//! result is byte-identical to what the room held at that sequence.
//!
//! Everything here is READ-ONLY: [`load_room`] opens the store to read, and
//! reconstruction builds a throwaway in-memory [`Hub`] with no store attached, so
//! no watermark advances and no durable byte is ever written back. This is a
//! debugging / recovery-investigation tool — replay a room at a suspect sequence,
//! or diff two points in time — not a mutation path.
//!
//! A crash between snapshot-write and log-truncate can leave a log prefix the
//! snapshot already covers (`append` never rewrites the log; the overlap survives
//! on disk until the next compaction removes it). Sequence numbering handles it
//! the way the hub does on replay: the retained tail is `log.ops` with every op
//! the snapshot already covers (or an earlier duplicate) filtered out, so the
//! floor stays at the snapshot's base sequence and the tail carries exactly
//! `base_seq+1..` — a replay against a mid-recovery store still labels sequences
//! correctly.

use std::collections::HashSet;
use std::fmt;
use std::io;

use crdtsync_core::diff::Change;
use crdtsync_core::op::OpId;
use crdtsync_core::{ClientId, Document};

use crate::store::{RoomLog, Snapshot, Store, StoredOp};
use crate::Hub;

/// A default reconstruction server identity for the CLI. A room's whole content
/// — the tree, every element id — is independent of this value; it surfaces only
/// as the leading replica id of the encoded state. A snapshot-backed room pins
/// that id in its snapshot, so any value reconstructs the room byte-identically;
/// an uncompacted room takes it from here, so to reproduce a specific node's
/// exact bytes pass that node's own server id.
pub const DEFAULT_REPLAY_SERVER: [u8; 16] = *b"crdtsync-replay0";

/// A room reconstructed as of a target server sequence.
#[derive(Clone, PartialEq, Eq)]
pub struct Reconstructed {
    /// The sequence this state is as of — ops `1..=seq` applied.
    pub seq: u64,
    /// The compaction floor: the base sequence of the snapshot the room was
    /// seeded from (`0` for an uncompacted room). Sequences at or below it, other
    /// than the floor itself, are unreconstructable — their ops are compacted
    /// away.
    pub floor: u64,
    /// The room's head sequence — the highest sequence its durable log reaches.
    pub head: u64,
    /// The room's whole-replica encoded state at `seq`, as [`encode_state`]
    /// produces it — byte-identical to the room's live export at that sequence.
    ///
    /// [`encode_state`]: crdtsync_core::Document::encode_state
    pub state: Vec<u8>,
}

impl fmt::Debug for Reconstructed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reconstructed")
            .field("seq", &self.seq)
            .field("floor", &self.floor)
            .field("head", &self.head)
            .field("state_len", &self.state.len())
            .finish()
    }
}

/// Why a replay could not reconstruct the requested point.
#[derive(Debug)]
pub enum ReplayError {
    /// The target sequence sits below the compaction floor: the ops between the
    /// floor and it were folded into the snapshot and dropped, so that exact point
    /// can no longer be reconstructed. The floor itself is still reachable (it is
    /// the snapshot).
    BelowFloor { floor: u64, requested: u64 },
    /// The target sequence is past the room's head — it does not exist yet.
    PastHead { head: u64, requested: u64 },
    /// The room's snapshot or a golden state failed to decode — a corrupt store.
    Decode,
}

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplayError::BelowFloor { floor, requested } => write!(
                f,
                "sequence {requested} is below the compaction floor {floor}; \
                 its ops are compacted away (earliest reconstructable is {floor})"
            ),
            ReplayError::PastHead { head, requested } => {
                write!(f, "sequence {requested} is past the room head {head}")
            }
            ReplayError::Decode => write!(f, "the room's persisted state failed to decode"),
        }
    }
}

impl std::error::Error for ReplayError {}

/// Load one room's durable log from `store`, or `None` if the store holds no such
/// room. Read-only: the store is opened and read, never written.
pub fn load_room(store: &Store, room: &[u8]) -> io::Result<Option<RoomLog>> {
    Ok(store
        .load()?
        .into_iter()
        .find(|(id, _)| id.as_slice() == room)
        .map(|(_, log)| log))
}

/// The head sequence of `log` — the highest server sequence its durable state
/// reaches (the compaction floor plus the retained tail length).
pub fn head_seq(log: &RoomLog) -> u64 {
    floor(log) + canonical_tail(log).len() as u64
}

/// The compaction floor of `log`: the snapshot's base sequence, or `0` when the
/// room is uncompacted.
pub fn floor(log: &RoomLog) -> u64 {
    log.snapshot.as_ref().map(|s| s.base_seq).unwrap_or(0)
}

/// The op ids the snapshot already covers — the decoded snapshot's seen set,
/// empty when there is no snapshot (or, defensively, when it fails to decode; a
/// corrupt snapshot then surfaces as a `Decode` error from reconstruction).
fn snapshot_seen(log: &RoomLog) -> HashSet<OpId> {
    match &log.snapshot {
        Some(s) => Document::decode_state(&s.state)
            .map(|d| d.seen().collect())
            .unwrap_or_default(),
        None => HashSet::new(),
    }
}

/// The canonical retained tail: `log.ops` with every op the snapshot already
/// covers, and any earlier duplicate, filtered out — in order. These are exactly
/// the ops that advance the sequence past the floor, mirroring the dedup the hub
/// runs on replay, so the op at tail index `i` carries server sequence
/// `floor + i + 1` even when a crash left the snapshot and log overlapping.
fn canonical_tail(log: &RoomLog) -> Vec<StoredOp> {
    let mut seen = snapshot_seen(log);
    log.ops
        .iter()
        .filter(|stored| seen.insert(stored.op.id))
        .cloned()
        .collect()
}

/// Reconstruct the room `log` describes as of `target_seq`: the exact merged
/// state after applying ops `1..=target_seq`. Read-only — reconstruction runs in
/// a throwaway in-memory hub and never writes back.
///
/// The snapshot seeds the state at the floor; the retained ops with sequence at
/// or below the target are replayed onto it. `target_seq` must lie in
/// `[floor, head]` — below the floor the ops are compacted away
/// ([`ReplayError::BelowFloor`]), above the head they do not exist
/// ([`ReplayError::PastHead`]). The floor itself yields the snapshot state alone.
pub fn reconstruct_at(
    log: &RoomLog,
    room: &[u8],
    target_seq: u64,
    server: ClientId,
) -> Result<Reconstructed, ReplayError> {
    let floor = floor(log);
    let tail = canonical_tail(log);
    let head = floor + tail.len() as u64;
    if target_seq < floor {
        return Err(ReplayError::BelowFloor {
            floor,
            requested: target_seq,
        });
    }
    if target_seq > head {
        return Err(ReplayError::PastHead {
            head,
            requested: target_seq,
        });
    }
    // The canonical tail's op at index `i` carries sequence `floor + i + 1`, so
    // keeping the first `target_seq - floor` records replays exactly through the
    // target.
    let keep = (target_seq - floor) as usize;
    let truncated = RoomLog {
        snapshot: log.snapshot.as_ref().map(|s| Snapshot {
            base_seq: s.base_seq,
            state: s.state.clone(),
        }),
        ops: tail[..keep].to_vec(),
        ..Default::default()
    };
    let hub = Hub::from_rooms(server, vec![(room.to_vec(), truncated)])
        .map_err(|_| ReplayError::Decode)?;
    let state = hub.export_room(room).ok_or(ReplayError::Decode)?;
    Ok(Reconstructed {
        seq: target_seq,
        floor,
        head,
        state,
    })
}

/// Reconstruct the room at `from_seq` and `to_seq` and diff them with the core
/// engine — the structural changes applied between the two points, exactly the
/// list [`diff`](crdtsync_core::path::diff) yields on the two states directly.
/// Read-only.
pub fn diff_at(
    log: &RoomLog,
    room: &[u8],
    from_seq: u64,
    to_seq: u64,
    server: ClientId,
) -> Result<Vec<Change>, ReplayError> {
    let old = reconstruct_at(log, room, from_seq, server)?;
    let new = reconstruct_at(log, room, to_seq, server)?;
    let old = Document::decode_state(&old.state).map_err(|_| ReplayError::Decode)?;
    let new = Document::decode_state(&new.state).map_err(|_| ReplayError::Decode)?;
    Ok(crdtsync_core::path::diff(&old, &new))
}
