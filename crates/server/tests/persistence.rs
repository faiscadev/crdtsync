// Real filesystem I/O, which Miri does not model.
#![cfg(not(miri))]

//! Persistence — the hub and registry backed by a durable op log.
//!
//! [`Hub::from_logs`] rebuilds a hub by replaying a loaded log: the merged
//! state, the server sequence, and the dedup set all come back. A
//! [`Registry`] opened over a [`Store`] persists every op it ingests, so a
//! node that drops and reopens the same store resumes exactly where it left
//! off — same state, same sequence, same catch-up.

use std::fs;

use crdtsync_core::{ClientId, Document, Element, Message, Op, Scalar};
use crdtsync_server::store::Store;
use crdtsync_server::{Catchup, Hub, Registry, RoomId};

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
        Catchup::Ops(v) => v,
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

/// A log for one room built from a client's transactions, as `load` returns it.
fn one_room_log(room: &[u8], ops: Vec<crdtsync_core::Op>) -> Vec<(RoomId, Vec<crdtsync_core::Op>)> {
    vec![(room.to_vec(), ops)]
}

// --- from_logs replay ---

#[test]
fn from_logs_rebuilds_state_and_sequence() {
    let mut a = doc(1);
    let mut ops = a.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    ops.extend(a.transact(|tx| tx.register(b"b", Scalar::Int(2))));

    let h = Hub::from_logs(cid(SERVER), one_room_log(ROOM, ops));
    assert_eq!(h.seq(ROOM), 2);
    assert_eq!(int(h.get(ROOM, b"a")), 1);
    assert_eq!(int(h.get(ROOM, b"b")), 2);
}

#[test]
fn from_logs_restores_the_dedup_set() {
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let mut h = Hub::from_logs(cid(SERVER), one_room_log(ROOM, ops.clone()));
    // A client that reconnects and resends a replayed op grows nothing: the
    // log's identities came back with the state.
    assert!(h.ingest(ROOM, ops).unwrap().is_empty());
    assert_eq!(h.seq(ROOM), 1);
}

#[test]
fn from_logs_with_no_rooms_is_a_fresh_hub() {
    let h = Hub::from_logs(cid(SERVER), Vec::new());
    assert_eq!(h.seq(ROOM), 0);
    assert!(h.get(ROOM, b"age").is_none());
}

#[test]
fn from_logs_replays_independent_rooms() {
    let a = doc(1).transact(|tx| tx.register(b"k", Scalar::Int(1)));
    let b = doc(2).transact(|tx| tx.register(b"k", Scalar::Int(2)));
    let logs = vec![(b"room-a".to_vec(), a), (b"room-b".to_vec(), b)];

    let h = Hub::from_logs(cid(SERVER), logs);
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
            client: cid(client)
        }
    ));
    assert!(r.deliver(
        id,
        Message::Subscribe {
            room: room.to_vec(),
            last_seen_seq: 0,
        }
    ));
    assert!(r.deliver(id, Message::Ops(ops)));
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
    assert_eq!(logged.len(), 1);
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
        r.deliver(id, Message::Hello { client: cid(1) });
        r.deliver(
            id,
            Message::Subscribe {
                room: ROOM.to_vec(),
                last_seen_seq: 0,
            },
        );
        r.deliver(id, Message::Ops(first.clone()));
        r.deliver(id, Message::Ops(second.clone()));
    }

    // After a restart the sequence numbering is unchanged: a subscriber that
    // saw seq 1 catches up on only the second op.
    let mut h = Hub::from_logs(
        cid(SERVER),
        Store::open(tmp.path()).unwrap().load().unwrap(),
    );
    assert_eq!(ops(h.catch_up(ROOM, 0)).len(), 2);
    assert_eq!(ops(h.catch_up(ROOM, 1)), second);
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
