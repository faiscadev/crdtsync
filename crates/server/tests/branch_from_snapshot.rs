//! Fork-from-snapshot — a branch whose shared base is a named version's snapshot,
//! not a live point in `main`'s log.
//!
//! A live-log fork shares `main`'s history up to a fork point (branch_sync.rs). A
//! snapshot fork instead pins its base to a captured version's materialized state
//! at the sequence that version covered: the branch owns that state, so it serves
//! the version's state — never `main`'s later ops — and survives the source
//! version's deletion. Its divergent tail appends past the base exactly as a
//! live-log fork's does.

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Element, Op, Scalar};
use crdtsync_server::{Catchup, Hub};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-1";
const RESTORED: &[u8] = b"restored";

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

/// A register write of `value` under `key` from `d`.
fn reg(d: &mut Document, key: &[u8], value: i64) -> Vec<Op> {
    d.transact(|tx| tx.register(key, Scalar::Int(value)))
}

/// The `key` register value in a decoded snapshot state.
fn int_in(state: &[u8], key: &[u8]) -> i64 {
    let restored = Document::decode_state(state).unwrap();
    match restored.get(key) {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected an int, got {other:?}"),
        },
        _ => panic!("expected the {key:?} register"),
    }
}

/// The whole-replica state a branch catch-up returns as a snapshot, or a panic if
/// it served an op delta instead.
fn snapshot(c: Catchup) -> Vec<u8> {
    match c {
        Catchup::Snapshot { state, .. } => state,
        Catchup::Ops(_) => panic!("expected a materialized snapshot, got an op delta"),
    }
}

/// The op delta a branch catch-up returns, or a panic if it served a snapshot.
fn delta(c: Catchup) -> Vec<Op> {
    match c {
        Catchup::Ops(v) => v.into_iter().map(|rec| rec.op).collect(),
        Catchup::Snapshot { .. } => panic!("expected an op delta, got a snapshot"),
    }
}

/// A hub with `ROOM` at `age = 20` and a version `v1` capturing it, then a
/// post-version `age = 30` write on `main`. Returns the sequence `v1` covers.
fn hub_with_v1() -> (Hub, u64) {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);
    hub.ingest(ROOM, reg(&mut main, b"age", 10), None).unwrap();
    hub.ingest(ROOM, reg(&mut main, b"age", 20), None).unwrap();
    let at = hub.seq(ROOM);
    assert!(hub.create_version(ROOM, b"v1").unwrap());
    // Main moves on past the version; the snapshot fork must not see this.
    hub.ingest(ROOM, reg(&mut main, b"age", 30), None).unwrap();
    (hub, at)
}

#[test]
fn a_snapshot_fork_serves_the_version_state_not_mains_later_ops() {
    let (mut hub, at) = hub_with_v1();
    assert!(hub.fork_branch_from_version(ROOM, RESTORED, b"v1").unwrap());

    // The branch's base is the version's covered sequence, not main's head.
    assert_eq!(hub.branch(ROOM, RESTORED).unwrap().fork_point, at);
    assert_eq!(hub.branch(ROOM, RESTORED).unwrap().head, at);

    // A fresh subscriber to the branch is served the version's state (age 20),
    // never main's post-version write (age 30).
    let state = snapshot(hub.catch_up_branch(ROOM, RESTORED, 0));
    assert_eq!(int_in(&state, b"age"), 20);
}

#[test]
fn main_is_unaffected_by_a_snapshot_fork() {
    let (mut hub, _at) = hub_with_v1();
    hub.fork_branch_from_version(ROOM, RESTORED, b"v1").unwrap();
    // Main still resolves its own live head (age 30), unchanged by the fork.
    match hub.catch_up(ROOM, 0) {
        Catchup::Ops(ops) => assert_eq!(ops.len(), 3),
        Catchup::Snapshot { .. } => panic!("main was not compacted"),
    }
    assert_eq!(hub.seq(ROOM), 3);
}

#[test]
fn a_snapshot_fork_tail_appends_past_the_base() {
    let (mut hub, _at) = hub_with_v1();
    hub.fork_branch_from_version(ROOM, RESTORED, b"v1").unwrap();

    // A branch write lands on a distinct key so the base value stays legible.
    let tail = hub
        .ingest_branch(ROOM, RESTORED, reg(&mut doc(2), b"note", 99), None)
        .unwrap();
    assert_eq!(tail.len(), 1);

    // A fresh subscriber's snapshot folds the tail over the base: both the
    // version's `age` and the branch's `note` are present.
    let state = snapshot(hub.catch_up_branch(ROOM, RESTORED, 0));
    assert_eq!(int_in(&state, b"age"), 20);
    assert_eq!(int_in(&state, b"note"), 99);
}

#[test]
fn a_reconnecting_subscriber_above_the_base_gets_the_tail_delta() {
    let (mut hub, at) = hub_with_v1();
    hub.fork_branch_from_version(ROOM, RESTORED, b"v1").unwrap();
    let tail = hub
        .ingest_branch(ROOM, RESTORED, reg(&mut doc(2), b"note", 99), None)
        .unwrap();
    let head = hub.branch(ROOM, RESTORED).unwrap().head;
    assert_eq!(head, at + 1);

    // A subscriber that already holds the base (last_seen == base_seq) is served
    // only the divergent tail as ops, not a whole snapshot.
    assert_eq!(delta(hub.catch_up_branch(ROOM, RESTORED, at)), tail);
    // At the head there is nothing left to send.
    assert!(delta(hub.catch_up_branch(ROOM, RESTORED, head)).is_empty());
}

#[test]
fn fork_from_version_refuses_unknown_version_or_duplicate_name() {
    let (mut hub, _at) = hub_with_v1();
    // An unknown version forks nothing.
    assert!(!hub
        .fork_branch_from_version(ROOM, RESTORED, b"ghost")
        .unwrap());
    assert!(hub.branch(ROOM, RESTORED).is_none());

    assert!(hub.fork_branch_from_version(ROOM, RESTORED, b"v1").unwrap());
    // A duplicate branch name changes nothing.
    assert!(!hub.fork_branch_from_version(ROOM, RESTORED, b"v1").unwrap());
}

#[test]
fn a_snapshot_fork_base_outlives_the_source_version() {
    let (mut hub, _at) = hub_with_v1();
    hub.fork_branch_from_version(ROOM, RESTORED, b"v1").unwrap();
    // The branch owns a copy of the base, so deleting the source version leaves
    // the branch's served state intact.
    assert!(hub.delete_version(ROOM, b"v1").unwrap());
    let state = snapshot(hub.catch_up_branch(ROOM, RESTORED, 0));
    assert_eq!(int_in(&state, b"age"), 20);
}

#[test]
fn deleting_a_snapshot_fork_drops_its_base_so_a_re_fork_is_clean() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);
    hub.ingest(ROOM, reg(&mut main, b"age", 20), None).unwrap();
    hub.create_version(ROOM, b"v1").unwrap();
    hub.ingest(ROOM, reg(&mut main, b"age", 40), None).unwrap();
    hub.create_version(ROOM, b"v2").unwrap();

    hub.fork_branch_from_version(ROOM, RESTORED, b"v1").unwrap();
    hub.ingest_branch(ROOM, RESTORED, reg(&mut doc(2), b"note", 1), None)
        .unwrap();
    assert!(hub.delete_branch(ROOM, RESTORED).unwrap());

    // Re-forking the same name from a different version inherits no stale base or
    // tail: it serves v2's state (age 40) with no `note`.
    hub.fork_branch_from_version(ROOM, RESTORED, b"v2").unwrap();
    let state = snapshot(hub.catch_up_branch(ROOM, RESTORED, 0));
    assert_eq!(int_in(&state, b"age"), 40);
    assert!(Document::decode_state(&state)
        .unwrap()
        .get(b"note")
        .is_none());
}

// --- durability: a snapshot fork's base and tail survive a store reopen ---
#[cfg(not(miri))]
mod durable {
    use super::*;
    use crdtsync_server::Store;
    use std::fs;
    use std::path::{Path, PathBuf};

    fn open_hub(path: &Path) -> Hub {
        let store = Store::open(path).unwrap();
        let rooms = store.load().unwrap();
        let mut hub = Hub::from_rooms(cid(0xFF), rooms).unwrap();
        hub.attach_store(store);
        hub
    }

    #[test]
    fn a_snapshot_fork_survives_a_reopen() {
        let tmp = tempdir();
        let at;
        {
            let mut hub = open_hub(tmp.path());
            let mut main = doc(1);
            hub.ingest(ROOM, reg(&mut main, b"age", 20), None).unwrap();
            at = hub.seq(ROOM);
            hub.create_version(ROOM, b"v1").unwrap();
            hub.ingest(ROOM, reg(&mut main, b"age", 30), None).unwrap();
            hub.fork_branch_from_version(ROOM, RESTORED, b"v1").unwrap();
            hub.ingest_branch(ROOM, RESTORED, reg(&mut doc(2), b"note", 99), None)
                .unwrap();
        }

        let mut hub = open_hub(tmp.path());
        assert_eq!(hub.branch(ROOM, RESTORED).unwrap().fork_point, at);
        assert_eq!(hub.branch(ROOM, RESTORED).unwrap().head, at + 1);
        let state = snapshot(hub.catch_up_branch(ROOM, RESTORED, 0));
        assert_eq!(int_in(&state, b"age"), 20);
        assert_eq!(int_in(&state, b"note"), 99);
    }

    /// Remove the single file with extension `ext` under `dir`.
    fn remove_ext(dir: &Path, ext: &str) {
        let file = fs::read_dir(dir)
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| p.extension().and_then(|e| e.to_str()) == Some(ext))
            .expect("a file with that extension");
        fs::remove_file(file).unwrap();
    }

    #[test]
    fn an_orphan_base_never_shadows_a_live_log_fork_after_reopen() {
        let tmp = tempdir();
        let fork_at;
        {
            let mut hub = open_hub(tmp.path());
            let mut main = doc(1);
            hub.ingest(ROOM, reg(&mut main, b"age", 20), None).unwrap();
            hub.create_version(ROOM, b"v1").unwrap();
            hub.ingest(ROOM, reg(&mut main, b"age", 30), None).unwrap();
            fork_at = hub.seq(ROOM);
            hub.fork_branch_from_version(ROOM, RESTORED, b"v1").unwrap();
        }
        // Simulate a crash that left the `.bbase` on disk but dropped the branch
        // pointer: remove the `.branches` file, keeping the orphan base.
        remove_ext(tmp.path(), "branches");

        let mut hub = open_hub(tmp.path());
        // The registry has no `restored` fork, so the orphan base was dropped.
        assert!(hub.branch(ROOM, RESTORED).is_none());
        // A fresh live-log fork reusing the name shares main's log (age 30), served
        // as an op delta — never the stale snapshot base (age 20).
        hub.fork_branch(ROOM, RESTORED, b"main", fork_at).unwrap();
        match hub.catch_up_branch(ROOM, RESTORED, 0) {
            Catchup::Ops(ops) => assert_eq!(ops.len(), 2),
            Catchup::Snapshot { .. } => panic!("an orphan base shadowed the live-log fork"),
        }
    }

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
        let dir = std::env::temp_dir().join(format!("crdtsync-branch-snapshot-{pid}-{n}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}
