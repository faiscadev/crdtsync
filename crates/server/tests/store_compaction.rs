// Real filesystem I/O, which Miri does not model.
#![cfg(not(miri))]

//! Store compaction — folding a room's log prefix into a durable snapshot.
//!
//! `compact` writes a room's snapshot (its base sequence plus the encoded
//! document state) and drops the log records it covers, so a restart replays a
//! bounded tail instead of the whole history. The write is crash-safe: the
//! snapshot lands atomically and only then is the log truncated, so a crash
//! between the two leaves the snapshot beside a still-full log — an overlap the
//! loader hands back verbatim for the hub to dedup on replay. A half-written
//! snapshot (its temp file) is ignored. `load` surfaces each room's snapshot
//! (if any) and the op records still in its log.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Scalar};
use crdtsync_server::store::{RoomLog, Store, StoredOp};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A couple of real relay ops (no schema) from client `first`, distinct per
/// call site via `key`.
fn ops(first: u8, key: &[u8], value: i64) -> Vec<StoredOp> {
    Document::new(cid(first))
        .transact(|tx| tx.register(key, Scalar::Int(value)))
        .into_iter()
        .map(|op| StoredOp::new(op, None))
        .collect()
}

/// Some opaque snapshot bytes — the store treats a snapshot as a blob.
fn state(byte: u8) -> Vec<u8> {
    vec![byte; 24]
}

/// The room log loaded for `room`, or panic if the room is absent.
fn room_log(store: &Store, room: &[u8]) -> RoomLog {
    store
        .load()
        .unwrap()
        .into_iter()
        .find(|(r, _)| r == room)
        .map(|(_, rl)| rl)
        .unwrap_or_else(|| panic!("room not found in load"))
}

const ROOM: &[u8] = b"room-1";

// --- compact / load round-trip ---

#[test]
fn compact_persists_a_snapshot_and_empties_the_log() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    store.append(ROOM, &ops(1, b"a", 1)).unwrap();
    store.append(ROOM, &ops(1, b"b", 2)).unwrap();

    store.compact(ROOM, 2, &state(0xAB)).unwrap();

    let rl = room_log(&store, ROOM);
    let snap = rl.snapshot.expect("a snapshot after compaction");
    assert_eq!(snap.base_seq, 2);
    assert_eq!(snap.state, state(0xAB));
    // The prefix the snapshot covers is gone: no tail after a compact to head.
    assert!(rl.ops.is_empty());
}

#[test]
fn a_snapshot_only_room_still_surfaces_on_load() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    store.append(ROOM, &ops(1, b"a", 1)).unwrap();
    store.compact(ROOM, 1, &state(0x11)).unwrap();

    // The log is empty, but the room must still surface via its snapshot.
    assert_eq!(store.load().unwrap().len(), 1);
    assert!(room_log(&store, ROOM).snapshot.is_some());
}

#[test]
fn appends_after_compaction_are_the_tail() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    store.append(ROOM, &ops(1, b"a", 1)).unwrap();
    store.compact(ROOM, 1, &state(0x22)).unwrap();

    let tail = ops(1, b"b", 2);
    store.append(ROOM, &tail).unwrap();

    let rl = room_log(&store, ROOM);
    assert_eq!(rl.snapshot.unwrap().base_seq, 1);
    assert_eq!(rl.ops, tail);
}

#[test]
fn a_later_compaction_replaces_the_snapshot() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    store.append(ROOM, &ops(1, b"a", 1)).unwrap();
    store.compact(ROOM, 1, &state(0x33)).unwrap();
    store.append(ROOM, &ops(1, b"b", 2)).unwrap();
    store.compact(ROOM, 2, &state(0x44)).unwrap();

    let rl = room_log(&store, ROOM);
    assert_eq!(rl.snapshot.unwrap().base_seq, 2);
    assert!(rl.ops.is_empty());
}

// --- durability ---

#[test]
fn a_reopened_store_replays_the_snapshot() {
    let tmp = tempdir();
    {
        let mut store = Store::open(tmp.path()).unwrap();
        store.append(ROOM, &ops(1, b"a", 1)).unwrap();
        store.compact(ROOM, 1, &state(0x55)).unwrap();
    }
    let reopened = Store::open(tmp.path()).unwrap();
    let rl = room_log(&reopened, ROOM);
    assert_eq!(rl.snapshot.unwrap().state, state(0x55));
    assert!(rl.ops.is_empty());
}

// --- crash safety ---

#[test]
fn compact_leaves_no_temp_artifacts() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    store.append(ROOM, &ops(1, b"a", 1)).unwrap();
    store.compact(ROOM, 1, &state(0x66)).unwrap();

    // A committed compaction leaves only durable files behind.
    for entry in fs::read_dir(tmp.path()).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        assert!(!name.ends_with(".tmp"), "temp artifact left behind: {name}");
    }
}

#[test]
fn a_snapshot_beside_an_untruncated_log_loads_both() {
    // Simulate a crash after the snapshot is durable but before the log is
    // truncated: the snapshot exists and the log still holds the prefix. The
    // loader must return both — the hub dedups the overlap on replay.
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    let a = ops(1, b"a", 1);
    let b = ops(1, b"b", 2);
    store.append(ROOM, &a).unwrap();
    store.append(ROOM, &b).unwrap();

    // Write the snapshot file directly, leaving the full log in place.
    write_snapshot_file(tmp.path(), ROOM, 2, &state(0x77));

    let rl = room_log(&store, ROOM);
    assert_eq!(rl.snapshot.unwrap().base_seq, 2);
    let mut both = a;
    both.extend(b);
    assert_eq!(rl.ops, both, "the untruncated log is returned intact");
}

#[test]
fn a_half_written_snapshot_temp_is_ignored() {
    // A crash mid snapshot-write leaves a temp file and no committed snapshot;
    // the loader ignores the temp and returns the log unchanged.
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    let a = ops(1, b"a", 1);
    store.append(ROOM, &a).unwrap();
    write_snapshot_temp(tmp.path(), ROOM, 1, &state(0x88));

    let rl = room_log(&store, ROOM);
    assert!(rl.snapshot.is_none(), "a temp snapshot is not committed");
    assert_eq!(rl.ops, a);
}

// --- snapshot file format (stable on-disk contract) ---

/// The snapshot record: an 8-byte little-endian base sequence, then the state
/// bytes to end of file.
fn snapshot_bytes(base_seq: u64, state: &[u8]) -> Vec<u8> {
    let mut out = base_seq.to_le_bytes().to_vec();
    out.extend_from_slice(state);
    out
}

/// The `.snap` file backing `room`, mirroring the store's hex-of-room-id naming
/// with a `.snap` extension.
fn snapshot_path(root: &Path, room: &[u8]) -> PathBuf {
    let mut name = String::new();
    for byte in room {
        name.push_str(&format!("{byte:02x}"));
    }
    name.push_str(".snap");
    root.join(name)
}

fn write_snapshot_file(root: &Path, room: &[u8], base_seq: u64, state: &[u8]) {
    let mut f = File::create(snapshot_path(root, room)).unwrap();
    f.write_all(&snapshot_bytes(base_seq, state)).unwrap();
    f.sync_all().unwrap();
}

fn write_snapshot_temp(root: &Path, room: &[u8], base_seq: u64, state: &[u8]) {
    let path = snapshot_path(root, room).with_extension("snap.tmp");
    let mut f = File::create(path).unwrap();
    f.write_all(&snapshot_bytes(base_seq, state)).unwrap();
}

#[test]
fn a_compacted_snapshot_matches_the_documented_format() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    store.append(ROOM, &ops(1, b"a", 1)).unwrap();
    store.compact(ROOM, 1, &state(0x99)).unwrap();

    let on_disk = fs::read(snapshot_path(tmp.path(), ROOM)).unwrap();
    assert_eq!(on_disk, snapshot_bytes(1, &state(0x99)));
}

// --- a tempdir without pulling in a dev-dependency ---

struct TempDir(PathBuf);

impl TempDir {
    fn path(&self) -> &Path {
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
    let dir = std::env::temp_dir().join(format!("crdtsync-snap-{pid}-{n}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}
