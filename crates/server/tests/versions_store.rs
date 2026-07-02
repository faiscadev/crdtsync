// Real filesystem I/O, which Miri does not model.
#![cfg(not(miri))]

//! Durable named versions — the versions index survives a store reopen.
//!
//! With a store attached, each version mutation rewrites the room's versions
//! file atomically before it commits, so a reopened hub reproduces exactly the
//! versions the writer held: their names, the sequences they covered, and their
//! captured state. Deleting the last version removes the file.

use std::fs;
use std::path::{Path, PathBuf};

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Element, Scalar};
use crdtsync_server::{Hub, Store};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

const ROOM: &[u8] = b"room-1";

/// Open a store-backed hub over `path`, replaying whatever it holds.
fn open_hub(path: &Path) -> Hub {
    let store = Store::open(path).unwrap();
    let rooms = store.load().unwrap();
    let mut hub = Hub::from_rooms(cid(0xFF), rooms).unwrap();
    hub.attach_store(store);
    hub
}

/// Ingest a register-write into the room from an evolving client document, so
/// successive writes carry advancing stamps and the latest value wins.
fn write_age(h: &mut Hub, d: &mut Document, value: i64) {
    h.ingest(
        ROOM,
        d.transact(|tx| tx.register(b"age", Scalar::Int(value))),
    )
    .unwrap();
}

/// The `age` register value in a decoded version state.
fn age_in(state: &[u8]) -> i64 {
    match Document::decode_state(state).unwrap().get(b"age") {
        Some(Element::Register(reg)) => match reg.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected an int, got {other:?}"),
        },
        _ => panic!("expected the age register"),
    }
}

#[test]
fn versions_survive_a_reopen() {
    let tmp = tempdir();
    let seq;
    {
        let mut hub = open_hub(tmp.path());
        let mut d = doc(1);
        write_age(&mut hub, &mut d, 30);
        seq = hub.seq(ROOM);
        assert!(hub.create_version(ROOM, b"v1").unwrap());
        write_age(&mut hub, &mut d, 40);
        assert!(hub.create_version(ROOM, b"v2").unwrap());
    }

    let hub = open_hub(tmp.path());
    assert_eq!(
        hub.version_names(ROOM),
        vec![b"v1".to_vec(), b"v2".to_vec()]
    );
    assert_eq!(hub.version_seq(ROOM, b"v1"), Some(seq));
    assert_eq!(age_in(hub.version_state(ROOM, b"v1").unwrap()), 30);
    assert_eq!(age_in(hub.version_state(ROOM, b"v2").unwrap()), 40);
}

#[test]
fn a_deleted_version_stays_gone_after_reopen() {
    let tmp = tempdir();
    {
        let mut hub = open_hub(tmp.path());
        let mut d = doc(1);
        write_age(&mut hub, &mut d, 30);
        assert!(hub.create_version(ROOM, b"keep").unwrap());
        assert!(hub.create_version(ROOM, b"drop").unwrap());
        assert!(hub.delete_version(ROOM, b"drop").unwrap());
    }

    let hub = open_hub(tmp.path());
    assert_eq!(hub.version_names(ROOM), vec![b"keep".to_vec()]);
    assert!(hub.version_state(ROOM, b"drop").is_none());
}

#[test]
fn a_rename_persists_across_reopen() {
    let tmp = tempdir();
    {
        let mut hub = open_hub(tmp.path());
        let mut d = doc(1);
        write_age(&mut hub, &mut d, 30);
        assert!(hub.create_version(ROOM, b"draft").unwrap());
        assert!(hub.rename_version(ROOM, b"draft", b"final").unwrap());
    }

    let hub = open_hub(tmp.path());
    assert_eq!(hub.version_names(ROOM), vec![b"final".to_vec()]);
    assert_eq!(age_in(hub.version_state(ROOM, b"final").unwrap()), 30);
}

#[test]
fn deleting_the_last_version_leaves_none_after_reopen() {
    let tmp = tempdir();
    {
        let mut hub = open_hub(tmp.path());
        let mut d = doc(1);
        write_age(&mut hub, &mut d, 30);
        assert!(hub.create_version(ROOM, b"only").unwrap());
        assert!(hub.delete_version(ROOM, b"only").unwrap());
    }

    let hub = open_hub(tmp.path());
    assert!(hub.version_names(ROOM).is_empty());
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
    let dir = std::env::temp_dir().join(format!("crdtsync-versions-{pid}-{n}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}
