// Real filesystem I/O, which Miri does not model.
#![cfg(not(miri))]

//! Replay tooling — read-only point-in-time reconstruction of a persisted room.
//!
//! [`reconstruct_at`] loads a room's nearest snapshot at or below a target
//! sequence, replays the retained tail up to that sequence, and yields the exact
//! merged state the room held after applying ops `1..=seq` — byte-identical to a
//! live hub's export at that point, across a compaction floor. [`diff_at`]
//! reconstructs two points and diffs them with the core engine. Neither ever
//! writes to the durable log or snapshot.

use std::fs;
use std::path::Path;

use crdtsync_core::{ClientId, Document, Scalar};
use crdtsync_server::replay::{diff_at, head_seq, load_room, reconstruct_at, ReplayError};
use crdtsync_server::store::Store;
use crdtsync_server::Hub;

const SERVER: u8 = 0xFF;
const ROOM: &[u8] = b"room-1";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

/// Every file in `dir` as sorted `(name, bytes)` — a fingerprint for the
/// read-only assertion.
fn dir_bytes(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = fs::read_dir(dir)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (
                e.file_name().to_string_lossy().into_owned(),
                fs::read(e.path()).unwrap(),
            )
        })
        .collect();
    out.sort();
    out
}

/// Drive a room through several register writes, ingesting each batch into a
/// store-backed hub and recording `(seq, live encoded state)` after every batch
/// — the golden point-in-time states to reconstruct against. Returns the golden
/// pairs; the store on disk holds the whole history.
fn persist_history(dir: &Path) -> Vec<(u64, Vec<u8>)> {
    let mut client = doc(1);
    let mut hub = Hub::new(cid(SERVER));
    hub.attach_store(Store::open(dir).unwrap());
    let mut golden = Vec::new();
    for i in 0..6u8 {
        let ops = client.transact(|tx| tx.register(&[b'k', i], Scalar::Int(i as i64)));
        hub.ingest(ROOM, ops, None).unwrap();
        golden.push((hub.seq(ROOM), hub.export_room(ROOM).unwrap()));
    }
    golden
}

// --- reconstruct-at-seq ---

#[test]
fn reconstruct_at_matches_live_state_at_that_seq() {
    let dir = tempdir();
    let golden = persist_history(dir.path());

    let log = load_room(&Store::open(dir.path()).unwrap(), ROOM)
        .unwrap()
        .expect("room present");

    for (seq, state) in &golden {
        let got = reconstruct_at(&log, ROOM, *seq, cid(SERVER)).unwrap();
        assert_eq!(got.seq, *seq);
        assert_eq!(
            &got.state, state,
            "state at seq {seq} must be byte-identical"
        );
    }
}

#[test]
fn reconstruct_at_head_is_the_current_state() {
    let dir = tempdir();
    let golden = persist_history(dir.path());
    let (head_seq_golden, head_state) = golden.last().unwrap();

    let log = load_room(&Store::open(dir.path()).unwrap(), ROOM)
        .unwrap()
        .unwrap();
    assert_eq!(head_seq(&log), *head_seq_golden);
    let got = reconstruct_at(&log, ROOM, head_seq(&log), cid(SERVER)).unwrap();
    assert_eq!(&got.state, head_state);
}

#[test]
fn reconstruct_at_seq_zero_is_the_empty_room() {
    let dir = tempdir();
    let _ = persist_history(dir.path());
    let log = load_room(&Store::open(dir.path()).unwrap(), ROOM)
        .unwrap()
        .unwrap();
    let empty = Document::new(cid(SERVER)).encode_state();
    let got = reconstruct_at(&log, ROOM, 0, cid(SERVER)).unwrap();
    assert_eq!(got.state, empty);
}

// --- across a compaction floor ---

/// Persist a history with a compaction in the middle: three batches, compact
/// (folding those into a snapshot), then three more retained in the tail.
/// Returns the golden `(seq, state)` pairs across the whole history.
fn persist_with_compaction(dir: &Path) -> Vec<(u64, Vec<u8>)> {
    let mut client = doc(1);
    let mut hub = Hub::new(cid(SERVER));
    hub.attach_store(Store::open(dir).unwrap());
    let mut golden = Vec::new();
    for i in 0..3u8 {
        let ops = client.transact(|tx| tx.register(&[b'a', i], Scalar::Int(i as i64)));
        hub.ingest(ROOM, ops, None).unwrap();
        golden.push((hub.seq(ROOM), hub.export_room(ROOM).unwrap()));
    }
    hub.compact(ROOM).unwrap();
    for i in 0..3u8 {
        let ops = client.transact(|tx| tx.register(&[b'b', i], Scalar::Int(10 + i as i64)));
        hub.ingest(ROOM, ops, None).unwrap();
        golden.push((hub.seq(ROOM), hub.export_room(ROOM).unwrap()));
    }
    golden
}

#[test]
fn reconstruct_across_a_compaction_floor_uses_the_snapshot() {
    let dir = tempdir();
    let golden = persist_with_compaction(dir.path());

    let store = Store::open(dir.path()).unwrap();
    let log = load_room(&store, ROOM).unwrap().unwrap();
    // The snapshot folded the first three ops; the floor sits at seq 3.
    assert!(log.snapshot.is_some(), "compaction must leave a snapshot");
    assert_eq!(log.snapshot.as_ref().unwrap().base_seq, 3);

    // Every point at or above the floor reconstructs byte-identically, seeded by
    // the snapshot rather than the folded-away ops.
    for (seq, state) in &golden {
        if *seq < 3 {
            continue;
        }
        let got = reconstruct_at(&log, ROOM, *seq, cid(SERVER)).unwrap();
        assert_eq!(&got.state, state, "state at seq {seq} across the floor");
    }
    // The floor itself reconstructs to the snapshot state alone (no tail).
    let at_floor = reconstruct_at(&log, ROOM, 3, cid(SERVER)).unwrap();
    assert_eq!(at_floor.state, log.snapshot.as_ref().unwrap().state);
}

#[test]
fn reconstruct_below_the_floor_is_rejected() {
    let dir = tempdir();
    let _ = persist_with_compaction(dir.path());
    let log = load_room(&Store::open(dir.path()).unwrap(), ROOM)
        .unwrap()
        .unwrap();
    match reconstruct_at(&log, ROOM, 2, cid(SERVER)) {
        Err(ReplayError::BelowFloor { floor, requested }) => {
            assert_eq!(floor, 3);
            assert_eq!(requested, 2);
        }
        other => panic!("expected BelowFloor, got {other:?}"),
    }
}

#[test]
fn reconstruct_past_the_head_is_rejected() {
    let dir = tempdir();
    let _ = persist_history(dir.path());
    let log = load_room(&Store::open(dir.path()).unwrap(), ROOM)
        .unwrap()
        .unwrap();
    let head = head_seq(&log);
    match reconstruct_at(&log, ROOM, head + 1, cid(SERVER)) {
        Err(ReplayError::PastHead { head: h, requested }) => {
            assert_eq!(h, head);
            assert_eq!(requested, head + 1);
        }
        other => panic!("expected PastHead, got {other:?}"),
    }
}

#[test]
fn unknown_room_has_no_log() {
    let dir = tempdir();
    let _ = persist_history(dir.path());
    let store = Store::open(dir.path()).unwrap();
    assert!(load_room(&store, b"no-such-room").unwrap().is_none());
}

// --- crash-left snapshot/log overlap ---

fn snap_path(root: &Path, room: &[u8]) -> std::path::PathBuf {
    let hex: String = room.iter().map(|b| format!("{b:02x}")).collect();
    root.join(format!("{hex}.snap"))
}

/// Write a snapshot file directly — an 8-byte little-endian base sequence then
/// the state — without touching the log, staging the crash window between a
/// snapshot write and the log truncation.
fn write_snapshot(root: &Path, room: &[u8], base_seq: u64, state: &[u8]) {
    let mut bytes = base_seq.to_le_bytes().to_vec();
    bytes.extend_from_slice(state);
    fs::write(snap_path(root, room), bytes).unwrap();
}

#[test]
fn reconstruct_is_correct_when_the_snapshot_and_log_overlap() {
    let dir = tempdir();
    let golden = persist_history(dir.path()); // seqs 1..=6, no snapshot yet

    // Stage a crash between snapshot-write and log-truncate: drop in a snapshot
    // covering the first 3 ops while the whole log stays on disk. The store now
    // holds a snapshot (base 3) beside a log whose first 3 records carry seqs the
    // snapshot already covers.
    let (seq3, state3) = &golden[2];
    assert_eq!(*seq3, 3);
    write_snapshot(dir.path(), ROOM, 3, state3);

    let log = load_room(&Store::open(dir.path()).unwrap(), ROOM)
        .unwrap()
        .unwrap();
    // The floor is the snapshot base; the head is the floor plus only the
    // non-overlapping tail — not floor + every raw log record.
    assert_eq!(head_seq(&log), 6);

    // Every point at or above the floor still reconstructs to the true state at
    // that seq, and the overlapping prefix does not shift the labels.
    for (seq, state) in &golden {
        if *seq < 3 {
            continue;
        }
        let got = reconstruct_at(&log, ROOM, *seq, cid(SERVER)).unwrap();
        assert_eq!(&got.state, state, "overlap: state at seq {seq}");
    }
    // The inflated raw-length head would have accepted seq 7; the deduped head
    // rejects it.
    assert!(matches!(
        reconstruct_at(&log, ROOM, 7, cid(SERVER)),
        Err(ReplayError::PastHead {
            head: 6,
            requested: 7
        })
    ));
    // Below the floor stays rejected.
    assert!(matches!(
        reconstruct_at(&log, ROOM, 2, cid(SERVER)),
        Err(ReplayError::BelowFloor {
            floor: 3,
            requested: 2
        })
    ));
}

// --- diff two points ---

#[test]
fn diff_two_seqs_matches_the_direct_engine_diff() {
    let dir = tempdir();
    let golden = persist_history(dir.path());
    let log = load_room(&Store::open(dir.path()).unwrap(), ROOM)
        .unwrap()
        .unwrap();

    let (seq_a, state_a) = &golden[1];
    let (seq_b, state_b) = &golden[4];

    let via_replay = diff_at(&log, ROOM, *seq_a, *seq_b, cid(SERVER)).unwrap();

    // The same diff the core engine yields on the two golden states directly.
    let old = Document::decode_state(state_a).unwrap();
    let new = Document::decode_state(state_b).unwrap();
    let direct = crdtsync_core::path::diff(&old, &new);

    assert_eq!(via_replay, direct);
    assert!(
        !via_replay.is_empty(),
        "the writes between must show as changes"
    );
}

#[test]
fn diff_identical_seqs_is_empty() {
    let dir = tempdir();
    let golden = persist_history(dir.path());
    let log = load_room(&Store::open(dir.path()).unwrap(), ROOM)
        .unwrap()
        .unwrap();
    let seq = golden[2].0;
    assert!(diff_at(&log, ROOM, seq, seq, cid(SERVER))
        .unwrap()
        .is_empty());
}

// --- read-only integrity ---

#[test]
fn replay_never_mutates_the_durable_store() {
    let dir = tempdir();
    let golden = persist_with_compaction(dir.path());
    let before = dir_bytes(dir.path());

    // A full sweep of replay operations: reconstruct at several points and diff.
    let log = load_room(&Store::open(dir.path()).unwrap(), ROOM)
        .unwrap()
        .unwrap();
    for (seq, _) in &golden {
        if *seq >= 3 {
            let _ = reconstruct_at(&log, ROOM, *seq, cid(SERVER)).unwrap();
        }
    }
    let _ = diff_at(&log, ROOM, 3, head_seq(&log), cid(SERVER)).unwrap();

    let after = dir_bytes(dir.path());
    assert_eq!(
        before, after,
        "replay must leave every durable file untouched"
    );
}

// --- a tempdir without pulling in a dev-dependency ---

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn tempdir() -> TempDir {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("crdtsync-replay-{pid}-{n}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}
