// Real filesystem I/O, which Miri does not model.
#![cfg(not(miri))]

//! Persistence — the hub and registry backed by a durable op log.
//!
//! [`Hub::from_rooms`] rebuilds a hub from each room's persisted snapshot and
//! log: the merged state, the server sequence, and the dedup set all come back.
//! A [`Registry`] opened over a [`Store`] persists every op it ingests and its
//! compaction snapshots, so a node that drops and reopens the same store
//! resumes exactly where it left off — same state, same sequence, same catch-up.

use std::fs;

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Element, Message, Op, Scalar};
use crdtsync_server::store::{Store, StoredOp};
use crdtsync_server::{Catchup, Hub, Registry, RoomId, RoomLog};

/// Tag a batch of ops as relay records (no schema) — the store's unit.
fn relay(ops: Vec<Op>) -> Vec<StoredOp> {
    ops.into_iter().map(|op| StoredOp::new(op, None)).collect()
}

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

/// Unwrap a catch-up that must be a plain op delta — these rooms are never
/// compacted.
fn ops(c: Catchup) -> Vec<Op> {
    match c {
        Catchup::Ops(v) => v.into_iter().map(|rec| rec.op).collect(),
        Catchup::Snapshot { .. } => panic!("expected an op delta, got a snapshot"),
    }
}

fn int(e: Option<Element>) -> i64 {
    match e {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected Int, got {other:?}"),
        },
        _ => panic!("expected a Register"),
    }
}

const SERVER: u8 = 0xFF;
const ROOM: &[u8] = b"room-1";

/// A room built from a client's transactions with no snapshot, as `load`
/// returns an uncompacted room.
fn one_room_log(room: &[u8], ops: Vec<Op>) -> Vec<(RoomId, RoomLog)> {
    vec![(
        room.to_vec(),
        RoomLog {
            snapshot: None,
            ops: relay(ops),
            versions: Vec::new(),
            meta: None,
            branches: Vec::new(),
            branch_ops: Vec::new(),
            branch_bases: Vec::new(),
            active_branch: None,
            epoch: None,
        },
    )]
}

// --- from_rooms replay ---

#[test]
fn from_rooms_rebuilds_state_and_sequence() {
    let mut a = doc(1);
    let mut ops = a.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    ops.extend(a.transact(|tx| tx.register(b"b", Scalar::Int(2))));

    let h = Hub::from_rooms(cid(SERVER), one_room_log(ROOM, ops)).unwrap();
    assert_eq!(h.seq(ROOM), 2);
    assert_eq!(int(h.get(ROOM, b"a")), 1);
    assert_eq!(int(h.get(ROOM, b"b")), 2);
}

#[test]
fn from_rooms_restores_the_dedup_set() {
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let mut h = Hub::from_rooms(cid(SERVER), one_room_log(ROOM, ops.clone())).unwrap();
    // A client that reconnects and resends a replayed op grows nothing: the
    // log's identities came back with the state.
    assert!(h.ingest(ROOM, ops, None).unwrap().is_empty());
    assert_eq!(h.seq(ROOM), 1);
}

#[test]
fn from_rooms_with_no_rooms_is_a_fresh_hub() {
    let h = Hub::from_rooms(cid(SERVER), Vec::new()).unwrap();
    assert_eq!(h.seq(ROOM), 0);
    assert!(h.get(ROOM, b"age").is_none());
}

#[test]
fn ingest_tags_the_creation_version_and_replay_preserves_it() {
    // Three writers at different versions — two enforced, one relay — build a
    // heterogeneous log; the tags survive a restart from the store, so each op
    // still knows the version it was created under for later translation.
    let tmp = tempdir();
    {
        let mut hub = Hub::new(cid(SERVER));
        hub.attach_store(Store::open(tmp.path()).unwrap());
        hub.ingest(
            ROOM,
            doc(1).transact(|tx| tx.register(b"a", Scalar::Int(1))),
            Some(1),
        )
        .unwrap();
        hub.ingest(
            ROOM,
            doc(2).transact(|tx| tx.register(b"b", Scalar::Int(2))),
            Some(2),
        )
        .unwrap();
        hub.ingest(
            ROOM,
            doc(3).transact(|tx| tx.register(b"c", Scalar::Int(3))),
            None,
        )
        .unwrap();
        assert_eq!(hub.logged_versions(ROOM), vec![Some(1), Some(2), None]);
    }
    // A restart replays the log from the store: the per-op versions come back.
    let hub = Hub::from_rooms(
        cid(SERVER),
        Store::open(tmp.path()).unwrap().load().unwrap(),
    )
    .unwrap();
    assert_eq!(hub.logged_versions(ROOM), vec![Some(1), Some(2), None]);
}

#[test]
fn a_rooms_doc_acl_creator_survives_a_restart() {
    // The creator is the doc-ACL authority root, so creator-auto-owns-`/` must
    // survive a reload — it is persisted beside the room's governing metadata.
    let tmp = tempdir();
    {
        let mut hub = Hub::new(cid(SERVER));
        hub.attach_store(Store::open(tmp.path()).unwrap());
        hub.ingest(
            ROOM,
            doc(1).transact(|tx| tx.register(b"a", Scalar::Int(1))),
            None,
        )
        .unwrap();
        hub.ensure_creator(ROOM, b"alice");
        assert_eq!(hub.room_creator(ROOM), Some(b"alice".to_vec()));
    }
    let hub = Hub::from_rooms(
        cid(SERVER),
        Store::open(tmp.path()).unwrap().load().unwrap(),
    )
    .unwrap();
    assert_eq!(
        hub.room_creator(ROOM),
        Some(b"alice".to_vec()),
        "the persisted creator comes back on reload",
    );
}

#[test]
fn from_rooms_replays_independent_rooms() {
    let a = doc(1).transact(|tx| tx.register(b"k", Scalar::Int(1)));
    let b = doc(2).transact(|tx| tx.register(b"k", Scalar::Int(2)));
    let rooms = vec![
        (
            b"room-a".to_vec(),
            RoomLog {
                snapshot: None,
                ops: relay(a),
                versions: Vec::new(),
                meta: None,
                branches: Vec::new(),
                branch_ops: Vec::new(),
                branch_bases: Vec::new(),
                active_branch: None,
                epoch: None,
            },
        ),
        (
            b"room-b".to_vec(),
            RoomLog {
                snapshot: None,
                ops: relay(b),
                versions: Vec::new(),
                meta: None,
                branches: Vec::new(),
                branch_ops: Vec::new(),
                branch_bases: Vec::new(),
                active_branch: None,
                epoch: None,
            },
        ),
    ];

    let h = Hub::from_rooms(cid(SERVER), rooms).unwrap();
    assert_eq!(int(h.get(b"room-a", b"k")), 1);
    assert_eq!(int(h.get(b"room-b", b"k")), 2);
}

// --- registry over a store ---

/// Drive one client through Hello/Subscribe/Ops on `r`, ingesting `ops`.
fn ingest_via(r: &mut Registry, client: u8, room: &[u8], ops: Vec<crdtsync_core::Op>) {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: Vec::new(),
            schema_version: 0,
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"cred".to_vec()
        }
    ));
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: room.to_vec(),
            zone: Vec::new(),
            last_seen_seq: 0,
            branch: Vec::new(),
        }
    ));
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: Channel(0),
            ops,
        }
    ));
    r.take_outbox(id);
}

#[test]
fn a_registry_survives_a_restart() {
    let tmp = tempdir();
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));

    {
        let mut r = Registry::with_store(cid(SERVER), Store::open(tmp.path()).unwrap()).unwrap();
        ingest_via(&mut r, 1, ROOM, ops);
        assert_eq!(int(r.hub().get(ROOM, b"age")), 30);
        assert_eq!(r.hub().seq(ROOM), 1);
    }

    // A new node over the same store comes back to the same state and sequence.
    let r = Registry::with_store(cid(SERVER), Store::open(tmp.path()).unwrap()).unwrap();
    assert_eq!(int(r.hub().get(ROOM, b"age")), 30);
    assert_eq!(r.hub().seq(ROOM), 1);
}

#[test]
fn a_reingested_op_after_restart_is_deduped() {
    let tmp = tempdir();
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));

    {
        let mut r = Registry::with_store(cid(SERVER), Store::open(tmp.path()).unwrap()).unwrap();
        ingest_via(&mut r, 1, ROOM, ops.clone());
    }

    // The reloaded node already holds these ops; a resend must not double-log
    // them or re-persist them.
    let mut r = Registry::with_store(cid(SERVER), Store::open(tmp.path()).unwrap()).unwrap();
    ingest_via(&mut r, 1, ROOM, ops);
    assert_eq!(r.hub().seq(ROOM), 1);

    let reopened = Store::open(tmp.path()).unwrap();
    let (_, logged) = reopened
        .load()
        .unwrap()
        .into_iter()
        .find(|(room, _)| room == ROOM)
        .unwrap();
    assert_eq!(logged.ops.len(), 1);
}

#[test]
fn catch_up_uses_stable_sequences_across_a_restart() {
    let tmp = tempdir();
    let mut a = doc(1);
    let first = a.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let second = a.transact(|tx| tx.register(b"b", Scalar::Int(2)));

    {
        let mut r = Registry::with_store(cid(SERVER), Store::open(tmp.path()).unwrap()).unwrap();
        let id = r.connect();
        r.deliver(
            id,
            Message::Hello {
                client: cid(1),
                app_id: Vec::new(),
                schema_version: 0,
            },
        );
        r.deliver(
            id,
            Message::Auth {
                credential: b"cred".to_vec(),
            },
        );
        r.deliver(
            id,
            Message::Subscribe {
                channel: Channel(0),
                room: ROOM.to_vec(),
                zone: Vec::new(),
                last_seen_seq: 0,
                branch: Vec::new(),
            },
        );
        r.deliver(
            id,
            Message::Ops {
                channel: Channel(0),
                ops: first.clone(),
            },
        );
        r.deliver(
            id,
            Message::Ops {
                channel: Channel(0),
                ops: second.clone(),
            },
        );
    }

    // After a restart the sequence numbering is unchanged: a subscriber that
    // saw seq 1 catches up on only the second op.
    let mut h = Hub::from_rooms(
        cid(SERVER),
        Store::open(tmp.path()).unwrap().load().unwrap(),
    )
    .unwrap();
    assert_eq!(ops(h.catch_up(ROOM, 0)).len(), 2);
    assert_eq!(ops(h.catch_up(ROOM, 1)), second);
}

// --- durable compaction ---

/// The `.snap` file path for `room`, matching the store's hex-of-id naming.
fn snap_path(root: &std::path::Path, room: &[u8]) -> std::path::PathBuf {
    let hex: String = room.iter().map(|b| format!("{b:02x}")).collect();
    root.join(format!("{hex}.snap"))
}

/// Write a snapshot file directly: an 8-byte little-endian base sequence then
/// the state. Used to stage a crash between snapshot-write and log-truncate.
fn write_snapshot(root: &std::path::Path, room: &[u8], base_seq: u64, state: &[u8]) {
    let mut bytes = base_seq.to_le_bytes().to_vec();
    bytes.extend_from_slice(state);
    fs::write(snap_path(root, room), bytes).unwrap();
}

/// An in-memory reference: the merged state and sequence a store-less hub reaches.
fn hub_over(ops: &[Op]) -> Hub {
    let mut hub = Hub::new(cid(SERVER));
    hub.ingest(ROOM, ops.to_vec(), None).unwrap();
    hub
}

#[test]
fn a_compacted_hub_reloads_from_its_snapshot() {
    let tmp = tempdir();
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let seq;
    {
        let mut hub = Hub::new(cid(SERVER));
        hub.attach_store(Store::open(tmp.path()).unwrap());
        hub.ingest(ROOM, ops, None).unwrap();
        hub.compact(ROOM).unwrap();
        seq = hub.seq(ROOM);
    }
    // On disk: a snapshot, and the log prefix it covers is gone.
    let rooms = Store::open(tmp.path()).unwrap().load().unwrap();
    let (_, rl) = rooms.iter().find(|(room, _)| room == ROOM).unwrap();
    assert!(rl.snapshot.is_some(), "compaction persisted a snapshot");
    assert!(rl.ops.is_empty(), "the log prefix was truncated");

    let reloaded = Hub::from_rooms(
        cid(SERVER),
        Store::open(tmp.path()).unwrap().load().unwrap(),
    )
    .unwrap();
    assert_eq!(int(reloaded.get(ROOM, b"age")), 30);
    assert_eq!(reloaded.seq(ROOM), seq);
}

#[test]
fn a_reingested_op_after_a_compacted_restart_is_deduped() {
    let tmp = tempdir();
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    {
        let mut hub = Hub::new(cid(SERVER));
        hub.attach_store(Store::open(tmp.path()).unwrap());
        hub.ingest(ROOM, ops.clone(), None).unwrap();
        hub.compact(ROOM).unwrap();
    }
    // The compacted op's id returns with the snapshot, so a resend is a no-op.
    let mut hub = Hub::from_rooms(
        cid(SERVER),
        Store::open(tmp.path()).unwrap().load().unwrap(),
    )
    .unwrap();
    hub.attach_store(Store::open(tmp.path()).unwrap());
    assert!(hub.ingest(ROOM, ops, None).unwrap().is_empty());
    assert_eq!(hub.seq(ROOM), 1);
    assert_eq!(int(hub.get(ROOM, b"age")), 30);
}

#[test]
fn a_snapshot_and_a_tail_reload_together() {
    let tmp = tempdir();
    let mut a = doc(1);
    let first = a.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let second = a.transact(|tx| tx.register(b"b", Scalar::Int(2)));
    {
        let mut hub = Hub::new(cid(SERVER));
        hub.attach_store(Store::open(tmp.path()).unwrap());
        hub.ingest(ROOM, first, None).unwrap();
        hub.compact(ROOM).unwrap(); // snapshot at seq 1
        hub.ingest(ROOM, second, None).unwrap(); // tail: seq 2
    }
    let hub = Hub::from_rooms(
        cid(SERVER),
        Store::open(tmp.path()).unwrap().load().unwrap(),
    )
    .unwrap();
    assert_eq!(int(hub.get(ROOM, b"a")), 1);
    assert_eq!(int(hub.get(ROOM, b"b")), 2);
    assert_eq!(hub.seq(ROOM), 2);
}

#[test]
fn a_snapshot_beside_a_full_log_reconstructs_correctly() {
    // A crash after the snapshot is durable but before the log is truncated:
    // snapshot at seq 2 sits beside the full log. Reconstruction dedups the
    // overlap and lands on the same state and head as a clean reload.
    let tmp = tempdir();
    let mut a = doc(1);
    let first = a.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let second = a.transact(|tx| tx.register(b"b", Scalar::Int(2)));
    let mut all = first.clone();
    all.extend(second.clone());

    {
        let mut store = Store::open(tmp.path()).unwrap();
        store.append(ROOM, &relay(first.clone())).unwrap();
        store.append(ROOM, &relay(second.clone())).unwrap();
    }
    // A snapshot of the state at seq 2, written beside the untruncated log.
    let mut snap = Document::new(cid(SERVER));
    for op in &all {
        snap.apply(op);
    }
    write_snapshot(tmp.path(), ROOM, 2, &snap.encode_state());

    let hub = Hub::from_rooms(
        cid(SERVER),
        Store::open(tmp.path()).unwrap().load().unwrap(),
    )
    .unwrap();
    let reference = hub_over(&all);
    assert_eq!(int(hub.get(ROOM, b"a")), 1);
    assert_eq!(int(hub.get(ROOM, b"b")), 2);
    assert_eq!(hub.seq(ROOM), reference.seq(ROOM));
}

#[test]
fn an_auto_compacted_room_persists_its_snapshot() {
    let tmp = tempdir();
    {
        let mut hub = Hub::new(cid(SERVER));
        hub.attach_store(Store::open(tmp.path()).unwrap());
        hub.set_compaction_threshold(2);
        let mut a = doc(1);
        hub.ingest(
            ROOM,
            a.transact(|tx| tx.register(b"a", Scalar::Int(1))),
            None,
        )
        .unwrap();
        // The second op takes the retained log to the threshold, folding it into
        // a durable snapshot.
        hub.ingest(
            ROOM,
            a.transact(|tx| tx.register(b"b", Scalar::Int(2))),
            None,
        )
        .unwrap();
    }
    let rooms = Store::open(tmp.path()).unwrap().load().unwrap();
    let (_, rl) = rooms.iter().find(|(room, _)| room == ROOM).unwrap();
    assert!(
        rl.snapshot.is_some(),
        "auto-compaction persisted a snapshot"
    );
    assert!(rl.ops.is_empty(), "the log was truncated on disk");

    let reloaded = Hub::from_rooms(
        cid(SERVER),
        Store::open(tmp.path()).unwrap().load().unwrap(),
    )
    .unwrap();
    assert_eq!(int(reloaded.get(ROOM, b"a")), 1);
    assert_eq!(int(reloaded.get(ROOM, b"b")), 2);
    assert_eq!(reloaded.seq(ROOM), 2);
}

// --- durable governing metadata ---

#[test]
fn the_op_version_high_water_survives_a_compacted_restart() {
    let tmp = tempdir();
    {
        let mut hub = Hub::new(cid(SERVER));
        hub.attach_store(Store::open(tmp.path()).unwrap());
        // An enforcing op at version 4 sets the high-water, then compaction folds
        // the whole log into the snapshot — the live log no longer carries the op
        // that raised the high-water.
        hub.ingest(
            ROOM,
            doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30))),
            Some(4),
        )
        .unwrap();
        hub.compact(ROOM).unwrap();
        assert_eq!(hub.max_op_version(ROOM), Some(4));
    }
    // A restart replays only the post-compaction tail (empty), so the high-water
    // is recovered from the persisted metadata rather than under-counted to None.
    let hub = Hub::from_rooms(
        cid(SERVER),
        Store::open(tmp.path()).unwrap().load().unwrap(),
    )
    .unwrap();
    assert_eq!(hub.max_op_version(ROOM), Some(4));
}

#[test]
fn the_op_version_high_water_survives_an_uncompacted_restart() {
    let tmp = tempdir();
    {
        let mut hub = Hub::new(cid(SERVER));
        hub.attach_store(Store::open(tmp.path()).unwrap());
        hub.ingest(
            ROOM,
            doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30))),
            Some(7),
        )
        .unwrap();
    }
    let hub = Hub::from_rooms(
        cid(SERVER),
        Store::open(tmp.path()).unwrap().load().unwrap(),
    )
    .unwrap();
    assert_eq!(hub.max_op_version(ROOM), Some(7));
}

#[test]
fn a_room_with_no_metadata_rebuilds_the_high_water_from_the_log() {
    // An uncompacted room whose log the store carries but no metadata record: the
    // high-water is rebuilt from the replayed ops, the standing fallback.
    let tmp = tempdir();
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    {
        let mut store = Store::open(tmp.path()).unwrap();
        let tagged: Vec<StoredOp> = ops
            .iter()
            .map(|op| StoredOp::new(op.clone(), Some(2)))
            .collect();
        store.append(ROOM, &tagged).unwrap();
    }
    let hub = Hub::from_rooms(
        cid(SERVER),
        Store::open(tmp.path()).unwrap().load().unwrap(),
    )
    .unwrap();
    assert_eq!(hub.max_op_version(ROOM), Some(2));
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
    let dir = std::env::temp_dir().join(format!("crdtsync-persist-{pid}-{n}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}
