//! Restore-as-branch — forking a chosen version into a branch and switching the
//! room's active HEAD to it.
//!
//! Restore does not rewrite history or reset any sequence. It forks a new branch
//! from a version's snapshot (fork-from-snapshot), captures an audit version of
//! the pre-restore live state, and switches the active HEAD so a plain (unnamed)
//! subscribe now follows the restored branch. The old branch is preserved,
//! immutable and still subscribable by name — an offline op in flight against the
//! old HEAD lands on the old branch, never corrupting the restored state. The
//! whole switch is durable and replays on reload.

use crdtsync_core::doc::Document;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Element, Message, Op, Scalar};
use crdtsync_server::{ConnId, Hub, Registry};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-1";
const RESTORED: &[u8] = b"restored";
const CH: Channel = Channel(0);

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn reg(d: &mut Document, key: &[u8], value: i64) -> Vec<Op> {
    d.transact(|tx| tx.register(key, Scalar::Int(value)))
}

/// The `key` register value in a decoded snapshot state.
fn int_in(state: &[u8], key: &[u8]) -> i64 {
    match Document::decode_state(state).unwrap().get(key) {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected an int, got {other:?}"),
        },
        _ => panic!("expected the {key:?} register"),
    }
}

fn registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(std::sync::Arc::new(crdtsync_server::ManualClock::new(0)));
    r
}

fn auth(r: &mut Registry, id: ConnId, client: u8) {
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
            credential: b"cred".to_vec(),
        }
    ));
    r.take_outbox(id);
}

/// Subscribe `id` to `(room, branch)` on channel `ch`, returning the catch-up
/// reply (the first outbox message).
fn subscribe(r: &mut Registry, id: ConnId, ch: Channel, room: &[u8], branch: &[u8]) -> Message {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: ch,
            room: room.to_vec(),
            branch: branch.to_vec(),
            zone: Vec::new(),
            last_seen_seq: 0,
        }
    ));
    let out = r.take_outbox(id);
    out.into_iter().next().expect("a catch-up reply")
}

/// The whole-replica state a snapshot catch-up carried.
fn snapshot_state(m: Message) -> Vec<u8> {
    match m {
        Message::Snapshot { state, .. } => state,
        other => panic!("expected a Snapshot catch-up, got {other:?}"),
    }
}

/// Seed `ROOM` on `main`: a subscriber writes `age = 10`, a version `v1` captures
/// it, then `age = 20` moves main past the version. Returns that subscriber and
/// its author document (its next op continues main's stream without colliding).
fn seed(r: &mut Registry) -> (ConnId, Document) {
    let a = r.connect();
    auth(r, a, 1);
    subscribe(r, a, CH, ROOM, b"");
    let mut main = doc(1);
    assert!(r.deliver(
        a,
        Message::Ops {
            channel: CH,
            ops: reg(&mut main, b"age", 10),
        }
    ));
    r.take_outbox(a);
    assert!(r.hub_mut().create_version(ROOM, b"v1").unwrap());
    assert!(r.deliver(
        a,
        Message::Ops {
            channel: CH,
            ops: reg(&mut main, b"age", 20),
        }
    ));
    r.take_outbox(a);
    (a, main)
}

#[test]
fn a_plain_subscribe_follows_the_restored_head() {
    let mut r = registry();
    let _ = seed(&mut r);
    assert!(r.restore_as_branch(ROOM, b"v1", RESTORED).unwrap());

    // A fresh plain (unnamed) subscriber now follows the restored branch: it is
    // served v1's state (age 10), not main's live head (age 20).
    let joiner = r.connect();
    auth(&mut r, joiner, 2);
    let state = snapshot_state(subscribe(&mut r, joiner, CH, ROOM, b""));
    assert_eq!(int_in(&state, b"age"), 10);
}

#[test]
fn the_old_branch_is_intact_and_still_subscribable_by_name() {
    let mut r = registry();
    let _ = seed(&mut r);
    r.restore_as_branch(ROOM, b"v1", RESTORED).unwrap();

    // An explicit `main` subscribe still serves the old branch's live history
    // (age 20) — restore preserved it, immutable.
    let onlooker = r.connect();
    auth(&mut r, onlooker, 3);
    let reply = subscribe(&mut r, onlooker, CH, ROOM, b"main");
    // Main is an uncompacted op delta; fold it to read the value.
    let Message::Ops { ops, .. } = reply else {
        panic!("expected main's op delta");
    };
    let mut d = Document::new(cid(9));
    for op in &ops {
        d.apply(op);
    }
    assert_eq!(int_in(&d.encode_state(), b"age"), 20);
}

#[test]
fn an_offline_op_against_the_old_head_lands_on_the_old_branch() {
    let mut r = registry();
    // This subscriber joined the default HEAD (main) before the restore, so its
    // channel is bound to `main`.
    let (offline, mut author) = seed(&mut r);
    r.restore_as_branch(ROOM, b"v1", RESTORED).unwrap();

    // Its in-flight op (continuing its own author stream) still targets that
    // channel — it lands on main (the old branch), advancing main, not the
    // restored HEAD.
    let main_before = r.hub().seq(ROOM);
    assert!(r.deliver(
        offline,
        Message::Ops {
            channel: CH,
            ops: reg(&mut author, b"stale", 7),
        }
    ));
    r.take_outbox(offline);
    assert_eq!(r.hub().seq(ROOM), main_before + 1, "the op advanced main");

    // The restored HEAD never sees it: a fresh plain subscriber's state carries no
    // `stale` key.
    let joiner = r.connect();
    auth(&mut r, joiner, 4);
    let state = snapshot_state(subscribe(&mut r, joiner, CH, ROOM, b""));
    assert!(Document::decode_state(&state)
        .unwrap()
        .get(b"stale")
        .is_none());
    assert_eq!(int_in(&state, b"age"), 10);
}

/// Whether any of `names` is an audit version of `branch` — `audit/restore/<branch>@…`.
fn has_audit(names: &[Vec<u8>], branch: &[u8]) -> bool {
    let mut prefix = b"audit/restore/".to_vec();
    prefix.extend_from_slice(branch);
    prefix.push(b'@');
    names.iter().any(|n| n.starts_with(&prefix))
}

#[test]
fn restore_auto_creates_an_audit_version() {
    let mut r = registry();
    let _ = seed(&mut r);
    r.restore_as_branch(ROOM, b"v1", RESTORED).unwrap();

    let names = r.hub().version_names(ROOM);
    assert!(
        has_audit(&names, RESTORED),
        "an audit version was captured: {names:?}"
    );
}

#[test]
fn a_reused_branch_name_audits_each_restore() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);
    hub.ingest(ROOM, reg(&mut main, b"age", 10), None).unwrap();
    hub.create_version(ROOM, b"v1").unwrap();
    assert!(hub.restore_as_branch(ROOM, b"v1", RESTORED).unwrap());
    let audit1 = format!("audit/restore/restored@{}", hub.seq(ROOM)).into_bytes();

    // Delete the branch, move main on, and restore the same name again.
    hub.delete_branch(ROOM, RESTORED).unwrap();
    hub.ingest(ROOM, reg(&mut main, b"age", 20), None).unwrap();
    hub.create_version(ROOM, b"v2").unwrap();
    assert!(hub.restore_as_branch(ROOM, b"v2", RESTORED).unwrap());
    let audit2 = format!("audit/restore/restored@{}", hub.seq(ROOM)).into_bytes();

    // Both restores left a distinct audit — the second is not silently skipped by
    // the first's lingering record.
    assert_ne!(audit1, audit2);
    let names = hub.version_names(ROOM);
    assert!(names.contains(&audit1), "first restore audited: {names:?}");
    assert!(names.contains(&audit2), "second restore audited: {names:?}");
}

#[test]
fn restore_refuses_an_unknown_version_or_a_taken_branch() {
    let mut r = registry();
    let _ = seed(&mut r);
    assert!(!r.restore_as_branch(ROOM, b"ghost", RESTORED).unwrap());
    assert!(r.hub().branch(ROOM, RESTORED).is_none());

    assert!(r.restore_as_branch(ROOM, b"v1", RESTORED).unwrap());
    // A second restore onto the same branch name changes nothing.
    assert!(!r.restore_as_branch(ROOM, b"v1", RESTORED).unwrap());
}

// --- hub-level active-HEAD semantics ---

#[test]
fn active_head_defaults_to_main_and_switches_on_restore() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);
    hub.ingest(ROOM, reg(&mut main, b"age", 10), None).unwrap();
    hub.create_version(ROOM, b"v1").unwrap();

    assert_eq!(hub.active_branch(ROOM), b"main".to_vec());
    assert!(hub.restore_as_branch(ROOM, b"v1", RESTORED).unwrap());
    assert_eq!(hub.active_branch(ROOM), RESTORED.to_vec());
}

#[test]
fn deleting_the_active_branch_resets_the_head_to_main() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);
    hub.ingest(ROOM, reg(&mut main, b"age", 10), None).unwrap();
    hub.create_version(ROOM, b"v1").unwrap();
    hub.restore_as_branch(ROOM, b"v1", RESTORED).unwrap();

    assert!(hub.delete_branch(ROOM, RESTORED).unwrap());
    assert_eq!(
        hub.active_branch(ROOM),
        b"main".to_vec(),
        "deleting the active HEAD falls back to main"
    );
}

// --- durability: the restore (branch + active HEAD + audit version) replays ---
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
    fn a_restore_replays_on_reload() {
        let tmp = tempdir();
        {
            let mut hub = open_hub(tmp.path());
            let mut main = doc(1);
            hub.ingest(ROOM, reg(&mut main, b"age", 10), None).unwrap();
            hub.create_version(ROOM, b"v1").unwrap();
            hub.ingest(ROOM, reg(&mut main, b"age", 20), None).unwrap();
            hub.restore_as_branch(ROOM, b"v1", RESTORED).unwrap();
        }

        let mut hub = open_hub(tmp.path());
        // The active HEAD switch came back.
        assert_eq!(hub.active_branch(ROOM), RESTORED.to_vec());
        // The restored branch and its snapshot base came back.
        let state = match hub.catch_up_branch(ROOM, RESTORED, 0) {
            crdtsync_server::Catchup::Snapshot { state, .. } => state,
            _ => panic!("expected the restored branch's snapshot"),
        };
        assert_eq!(int_in(&state, b"age"), 10);
        // The audit version came back.
        assert!(has_audit(&hub.version_names(ROOM), RESTORED));
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
        let dir = std::env::temp_dir().join(format!("crdtsync-restore-{pid}-{n}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}
