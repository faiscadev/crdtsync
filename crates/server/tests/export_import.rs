//! Export / import — snapshots are portable across hubs.
//!
//! A room's whole-replica state is a portable snapshot: [`Hub::export_room`]
//! hands back the bytes, [`Hub::import_room`] rebuilds a fresh room from them.
//! The uses are backup, cross-server moves, and debug repro — carrying a room's
//! merged state, its element/client identities, and its dedup set to another
//! node without replaying the whole op log.
//!
//! Import is create-only: it refuses a room that already exists rather than
//! clobbering live state (an overwrite needs an explicit delete first), and it
//! rejects malformed bytes as an error, never a panic. Element and op
//! identities ride the state, so a client that resends its ops against the
//! imported room is deduped exactly as against the origin. Cloning a room under
//! a *new* id — with clock bumps and id namespacing so two live copies of the
//! same origin can't collide — is a separate layer; these cover the identity-
//! preserving move.

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Element, Scalar};
use crdtsync_server::{Catchup, Hub};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn hub() -> Hub {
    Hub::new(cid(0xFF))
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

const ROOM: &[u8] = b"room-a";
const DEST: &[u8] = b"room-b";

/// The `age` register value in a room's merged state.
fn age(h: &Hub, room: &[u8]) -> i64 {
    match h.get(room, b"age") {
        Some(Element::Register(reg)) => match reg.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected an int, got {other:?}"),
        },
        _ => panic!("expected the age register"),
    }
}

/// The `age` register value in a decoded state blob.
fn age_in(state: &[u8]) -> i64 {
    let restored = Document::decode_state(state).unwrap();
    match restored.get(b"age") {
        Some(Element::Register(reg)) => match reg.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected an int, got {other:?}"),
        },
        _ => panic!("expected the age register"),
    }
}

fn write_age(h: &mut Hub, room: &[u8], value: i64) {
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(value)));
    h.ingest(room, ops, None).unwrap();
}

// --- export ---

#[test]
fn export_returns_the_rooms_portable_state() {
    let mut h = hub();
    write_age(&mut h, ROOM, 30);
    let state = h.export_room(ROOM).expect("a known room exports its state");
    assert_eq!(age_in(&state), 30);
}

#[test]
fn export_of_an_unknown_room_is_none() {
    let h = hub();
    assert!(h.export_room(b"absent").is_none());
}

#[test]
fn export_matches_the_state_a_below_floor_subscriber_is_served() {
    // The exported bytes are the same whole-replica snapshot a cold-start
    // subscriber would be caught up with.
    let mut h = hub();
    write_age(&mut h, ROOM, 30);
    let exported = h.export_room(ROOM).unwrap();
    h.compact(ROOM).unwrap();
    match h.catch_up(ROOM, 0) {
        Catchup::Snapshot { state, .. } => assert_eq!(state, exported),
        Catchup::Ops(_) => panic!("a below-floor subscriber expects a snapshot"),
    }
}

// --- import ---

#[test]
fn import_creates_a_room_from_exported_state() {
    let mut src = hub();
    write_age(&mut src, ROOM, 42);
    let state = src.export_room(ROOM).unwrap();

    let mut dst = hub();
    assert!(dst.import_room(DEST, &state).unwrap());
    assert_eq!(age(&dst, DEST), 42);
}

#[test]
fn a_fresh_subscriber_to_an_imported_room_gets_a_snapshot() {
    // The imported state sits below the floor, so a subscriber that saw nothing
    // is caught up with a snapshot carrying it — not an empty op delta.
    let mut src = hub();
    write_age(&mut src, ROOM, 7);
    let state = src.export_room(ROOM).unwrap();

    let mut dst = hub();
    dst.import_room(DEST, &state).unwrap();
    match dst.catch_up(DEST, 0) {
        Catchup::Snapshot { state: caught, seq } => {
            assert_eq!(caught, state);
            assert_eq!(seq, dst.seq(DEST));
        }
        Catchup::Ops(_) => panic!("an imported room serves a snapshot to a fresh subscriber"),
    }
}

#[test]
fn import_refuses_an_existing_room() {
    let mut src = hub();
    write_age(&mut src, ROOM, 1);
    let state = src.export_room(ROOM).unwrap();

    let mut dst = hub();
    write_age(&mut dst, DEST, 99);
    // The room already holds live state; import declines rather than clobber it.
    assert!(!dst.import_room(DEST, &state).unwrap());
    assert_eq!(age(&dst, DEST), 99);
}

#[test]
fn import_rejects_malformed_state() {
    let mut dst = hub();
    let err = dst
        .import_room(DEST, b"not a snapshot")
        .expect_err("garbage bytes are an error, not a panic");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    // Nothing was installed for the failed import.
    assert!(dst.export_room(DEST).is_none());
}

#[test]
fn an_imported_room_dedups_its_origin_ops() {
    // Identities ride the state: a client that reconnects to the moved room and
    // resends its ops grows nothing — the dedup set came back with the import.
    let mut writer = doc(1);
    let ops = writer.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let mut src = hub();
    src.ingest(ROOM, ops.clone(), None).unwrap();
    let state = src.export_room(ROOM).unwrap();

    let mut dst = hub();
    dst.import_room(DEST, &state).unwrap();
    let seq_before = dst.seq(DEST);
    assert!(dst.ingest(DEST, ops, None).unwrap().is_empty());
    assert_eq!(dst.seq(DEST), seq_before);
}

#[test]
fn an_imported_room_takes_further_edits() {
    // After a move the room is live: new ops apply and advance its sequence past
    // the imported head.
    let mut src = hub();
    write_age(&mut src, ROOM, 1);
    let state = src.export_room(ROOM).unwrap();

    let mut dst = hub();
    dst.import_room(DEST, &state).unwrap();
    let head = dst.seq(DEST);

    let ops = doc(2).transact(|tx| tx.register(b"note", Scalar::Int(5)));
    assert!(!dst.ingest(DEST, ops, None).unwrap().is_empty());
    assert!(dst.seq(DEST) > head);
    assert_eq!(age(&dst, DEST), 1);
}

#[test]
fn export_import_round_trips_a_composite_document() {
    let mut src = hub();
    let mut w = doc(1);
    let mut ops = w.transact(|tx| tx.register(b"age", Scalar::Int(11)));
    ops.extend(w.transact(|tx| tx.inc(b"hits", 3)));
    src.ingest(ROOM, ops, None).unwrap();
    let state = src.export_room(ROOM).unwrap();

    let mut dst = hub();
    dst.import_room(DEST, &state).unwrap();
    assert_eq!(age(&dst, DEST), 11);
    match dst.get(DEST, b"hits") {
        Some(Element::Counter(c)) => assert_eq!(c.borrow().read(), 3),
        _ => panic!("expected the hits counter"),
    }
}

// --- durability ---

// Real filesystem I/O, which Miri does not model.
#[cfg(not(miri))]
mod durable {
    use super::*;
    use crdtsync_server::store::Store;
    use std::fs;

    #[test]
    fn an_imported_room_survives_a_restart() {
        let tmp = tempdir();
        let mut src = hub();
        write_age(&mut src, ROOM, 55);
        let state = src.export_room(ROOM).unwrap();

        let seq;
        {
            let mut dst = hub();
            dst.attach_store(Store::open(tmp.path()).unwrap());
            assert!(dst.import_room(DEST, &state).unwrap());
            seq = dst.seq(DEST);
        }

        // A new node over the same store reloads the imported room's state and
        // its sequence — the import was persisted as a durable snapshot.
        let reloaded =
            Hub::from_rooms(cid(0xFF), Store::open(tmp.path()).unwrap().load().unwrap()).unwrap();
        assert_eq!(age(&reloaded, DEST), 55);
        assert_eq!(reloaded.seq(DEST), seq);
    }

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
        let dir = std::env::temp_dir().join(format!("crdtsync-export-{pid}-{n}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}
