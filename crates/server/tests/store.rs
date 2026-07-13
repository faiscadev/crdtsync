// Real filesystem I/O, which Miri does not model.
#![cfg(not(miri))]

//! Store — the durable, append-only op log behind the hub.
//!
//! A [`Store`] persists each room's ops to disk so a restarted node replays
//! back to the same state. One append-only file per room; each op is one
//! record, framed as a `u32` little-endian creation schema version (`0` for a
//! relay op), then a `u32` little-endian length prefix followed by its
//! `encode_op` bytes. `append` is durable — a second handle (a restart) sees
//! ops the moment the call returns. Loading tolerates a torn tail (a record
//! half-written when the process died) but rejects a complete, corrupt record.

use std::fs::{self, OpenOptions};
use std::io::Write;

use crdtsync_core::doc::Document;
use crdtsync_core::{encode_op, ClientId, Scalar};
use crdtsync_server::store::{Store, StoredOp};
use crdtsync_server::RoomId;

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

/// Frame a record the way the store does: `u32` LE creation version, `u32` LE
/// length prefix, then the op's bytes.
fn frame(stored: &StoredOp) -> Vec<u8> {
    let body = encode_op(&stored.op);
    let mut rec = stored.schema_version.unwrap_or(0).to_le_bytes().to_vec();
    rec.extend((body.len() as u32).to_le_bytes());
    rec.extend(body);
    rec
}

/// The single file in a store's root (each test writes exactly one room).
fn sole_file(root: &std::path::Path) -> std::path::PathBuf {
    let mut entries: Vec<_> = fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert_eq!(entries.len(), 1, "expected exactly one room file");
    entries.pop().unwrap()
}

/// The ops loaded for `room`, or panic if the room is absent. These rooms are
/// never compacted, so a room carries only its log.
fn loaded(store: &Store, room: &[u8]) -> Vec<StoredOp> {
    let logs = store.load().unwrap();
    logs.into_iter()
        .find(|(r, _)| r == room)
        .map(|(_, rl)| rl.ops)
        .unwrap_or_else(|| panic!("room not found in load"))
}

const ROOM: &[u8] = b"room-1";

// --- open ---

#[test]
fn open_creates_a_missing_root() {
    let tmp = tempdir();
    let root = tmp.path().join("nested/does/not/exist");
    assert!(!root.exists());
    Store::open(&root).unwrap();
    assert!(root.is_dir());
}

#[test]
fn loading_an_empty_store_yields_no_rooms() {
    let tmp = tempdir();
    let store = Store::open(tmp.path()).unwrap();
    assert!(store.load().unwrap().is_empty());
}

// --- append / load round-trip ---

#[test]
fn appended_ops_load_back_in_order() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    let ops = ops(1, b"age", 30);
    store.append(ROOM, &ops).unwrap();
    assert_eq!(loaded(&store, ROOM), ops);
}

#[test]
fn appends_accumulate_across_calls() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    let first = ops(1, b"a", 1);
    let second = ops(1, b"b", 2);
    store.append(ROOM, &first).unwrap();
    store.append(ROOM, &second).unwrap();

    let mut want = first;
    want.extend(second);
    assert_eq!(loaded(&store, ROOM), want);
}

#[test]
fn an_empty_append_writes_nothing() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    store.append(ROOM, &[]).unwrap();
    // No records, so no room surfaces on load.
    assert!(store.load().unwrap().is_empty());
}

#[test]
fn rooms_are_stored_independently() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    let a = ops(1, b"k", 1);
    let b = ops(2, b"k", 2);
    store.append(b"room-a", &a).unwrap();
    store.append(b"room-b", &b).unwrap();

    assert_eq!(loaded(&store, b"room-a"), a);
    assert_eq!(loaded(&store, b"room-b"), b);
    assert_eq!(store.load().unwrap().len(), 2);
}

#[test]
fn a_room_id_of_arbitrary_bytes_round_trips_without_escaping() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    // Bytes a filename can't hold verbatim: a separator, dot-dot, and non-utf8.
    let room: RoomId = vec![0xff, 0x00, b'/', b'.', b'.', b'/', 0xfe];
    let ops = ops(1, b"x", 7);
    store.append(&room, &ops).unwrap();

    // The room id survives the encoding exactly, and stays inside the root.
    assert_eq!(loaded(&store, &room), ops);
    assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 1);
}

// --- durability ---

#[test]
fn a_reopened_store_replays_prior_appends() {
    let tmp = tempdir();
    let ops = ops(1, b"age", 30);
    {
        let mut store = Store::open(tmp.path()).unwrap();
        store.append(ROOM, &ops).unwrap();
    }
    // A restart: a fresh handle over the same root sees the committed log.
    let reopened = Store::open(tmp.path()).unwrap();
    assert_eq!(loaded(&reopened, ROOM), ops);
}

#[test]
fn an_append_is_visible_to_a_concurrent_handle() {
    let tmp = tempdir();
    let mut writer = Store::open(tmp.path()).unwrap();
    let ops = ops(1, b"age", 30);
    writer.append(ROOM, &ops).unwrap();
    // append flushes, so a second handle opened afterward reads it immediately.
    let reader = Store::open(tmp.path()).unwrap();
    assert_eq!(loaded(&reader, ROOM), ops);
}

// --- corruption tolerance ---

#[test]
fn a_torn_tail_record_is_dropped_and_earlier_ops_survive() {
    let tmp = tempdir();
    let mut want;
    {
        let mut store = Store::open(tmp.path()).unwrap();
        let mut batch = ops(1, b"a", 1);
        batch.extend(ops(1, b"b", 2));
        store.append(ROOM, &batch).unwrap();
        want = batch;
    }
    // Simulate a crash mid-write: lop a byte off the final record so its length
    // prefix outruns the bytes present.
    let file = sole_file(tmp.path());
    let len = fs::metadata(&file).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&file)
        .unwrap()
        .set_len(len - 1)
        .unwrap();

    // The intact first record still loads; the torn tail is discarded.
    want.pop();
    let store = Store::open(tmp.path()).unwrap();
    assert_eq!(loaded(&store, ROOM), want);
}

#[test]
fn a_complete_but_undecodable_record_is_an_error() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    store.append(ROOM, &ops(1, b"a", 1)).unwrap();

    // Append a fully-present record whose body is not a decodable op. This is
    // real corruption, not a torn tail, so loading must surface it.
    let file = sole_file(tmp.path());
    let garbage = {
        let body = [0xffu8, 0xff, 0xff];
        // A complete record — version 0, then a length prefix that matches the
        // body — whose body is not a decodable op.
        let mut rec = 0u32.to_le_bytes().to_vec();
        rec.extend((body.len() as u32).to_le_bytes());
        rec.extend(body);
        rec
    };
    OpenOptions::new()
        .append(true)
        .open(&file)
        .unwrap()
        .write_all(&garbage)
        .unwrap();

    assert!(Store::open(tmp.path()).unwrap().load().is_err());
}

#[test]
fn framing_is_a_version_then_a_length_prefixed_encode_op() {
    // One record on disk is its `u32` LE creation version, then a `u32` LE
    // length prefix, then the op bytes.
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    let op = ops(1, b"age", 30);
    store.append(ROOM, &op).unwrap();

    let on_disk = fs::read(sole_file(tmp.path())).unwrap();
    assert_eq!(on_disk, frame(&op[0]));
}

#[test]
fn a_records_creation_version_round_trips() {
    // A heterogeneous batch — a relay op and an enforced-version op — reloads
    // with each op's own creation version intact.
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    let mut batch = ops(1, b"a", 1); // relay: None
    batch.extend(
        ops(2, b"b", 2)
            .into_iter()
            .map(|s| StoredOp::new(s.op, Some(7))),
    );
    store.append(ROOM, &batch).unwrap();

    let back = loaded(&store, ROOM);
    assert_eq!(
        back.iter().map(|s| s.schema_version).collect::<Vec<_>>(),
        vec![None, Some(7)]
    );
    assert_eq!(back, batch);
}

// --- governing metadata ---

use crdtsync_server::RoomMeta;

/// The metadata loaded for `room`, or `None` if the room has no record.
fn loaded_meta(store: &Store, room: &[u8]) -> Option<RoomMeta> {
    store
        .load()
        .unwrap()
        .into_iter()
        .find(|(r, _)| r == room)
        .and_then(|(_, rl)| rl.meta)
}

#[test]
fn governing_metadata_round_trips() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    store
        .write_meta(
            ROOM,
            &RoomMeta {
                governing: Some((b"app".to_vec(), 3)),
                max_op_version: Some(5),
                creator: None,
            },
        )
        .unwrap();

    let meta = loaded_meta(&store, ROOM).expect("metadata present");
    assert_eq!(meta.governing, Some((b"app".to_vec(), 3)));
    assert_eq!(meta.max_op_version, Some(5));
}

#[test]
fn a_relay_high_water_without_a_binding_round_trips() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    store
        .write_meta(
            ROOM,
            &RoomMeta {
                governing: None,
                max_op_version: Some(2),
                creator: None,
            },
        )
        .unwrap();

    let meta = loaded_meta(&store, ROOM).expect("metadata present");
    assert_eq!(meta.governing, None);
    assert_eq!(meta.max_op_version, Some(2));
}

#[test]
fn metadata_with_neither_field_removes_the_record() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    store
        .write_meta(
            ROOM,
            &RoomMeta {
                governing: Some((b"app".to_vec(), 1)),
                max_op_version: Some(1),
                creator: None,
            },
        )
        .unwrap();
    // Clearing both fields removes the file, so the room carries no metadata.
    store
        .write_meta(
            ROOM,
            &RoomMeta {
                governing: None,
                max_op_version: None,
                creator: None,
            },
        )
        .unwrap();
    assert!(loaded_meta(&store, ROOM).is_none());
}

#[test]
fn a_malformed_metadata_record_loads_as_absent_and_never_panics() {
    let tmp = tempdir();
    let mut store = Store::open(tmp.path()).unwrap();
    // A room with a real log, so its slot exists regardless of the metadata.
    store.append(ROOM, &ops(1, b"age", 30)).unwrap();
    store
        .write_meta(
            ROOM,
            &RoomMeta {
                governing: Some((b"app".to_vec(), 1)),
                max_op_version: Some(1),
                creator: None,
            },
        )
        .unwrap();

    // Truncate the metadata record mid-field: metadata is a durability cache, so
    // this loads as absent rather than failing the whole load.
    let meta_file = {
        let hex: String = ROOM.iter().map(|b| format!("{b:02x}")).collect();
        tmp.path().join(format!("{hex}.meta"))
    };
    fs::write(&meta_file, [1u8, 0, 0]).unwrap();

    let store = Store::open(tmp.path()).unwrap();
    let loaded = store.load().unwrap();
    let (_, rl) = loaded.iter().find(|(r, _)| r == ROOM).unwrap();
    assert!(rl.meta.is_none(), "a malformed record loads as absent");
    assert_eq!(rl.ops.len(), 1, "the log still loads");
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
    // A process- and test-unique directory under the OS temp root.
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("crdtsync-store-{pid}-{n}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}
