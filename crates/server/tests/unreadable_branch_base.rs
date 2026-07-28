//! A branch base this node cannot decode is a refusal, never an empty document.
//!
//! A snapshot-forked branch owns its base: a stored whole-replica state its
//! catch-up serves and a publish freezes onto its target. Every consumer of that
//! state used to read a decode failure as *absence* — an empty op delta, an empty
//! simulation — and absence is indistinguishable from "nothing there". So a
//! publish over a base this node could not read froze an **empty** replica onto the
//! published branch and its captured version, durably, with no error anywhere; and
//! the ingress simulations read an unreadable room as "crosses no zone" and
//! "violates no schema" and admitted the batch.
//!
//! Unknown is now its own answer at every one of those seams: the stream reports
//! itself unservable, the publish freezes nothing, and the gates refuse.

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Element, Op, Scalar};
use crdtsync_server::store::{Branch, RoomLog, Snapshot, Store};
use crdtsync_server::{Catchup, Hub};
use std::fs;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-1";
const SERVER: u8 = 0xFF;
const DRAFT: &[u8] = b"draft";
const PUBLISHED: &[u8] = b"published";

fn reg(d: &mut Document, key: &[u8], value: i64) -> Vec<Op> {
    d.transact(|tx| tx.register(key, Scalar::Int(value)))
}

fn int_in(state: &[u8], key: &[u8]) -> i64 {
    match Document::decode_state(state).unwrap().get(key) {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected an int, got {other:?}"),
        },
        _ => panic!("expected the {key:?} register"),
    }
}

/// A store-backed hub holding one room, a snapshot-forked `draft` branch active
/// over it, and a `published` branch already carrying `key = 1`.
fn seeded(dir: &std::path::Path) -> Hub {
    let mut hub = Hub::new(cid(SERVER));
    hub.attach_store(Store::open(dir).unwrap());
    let mut author = Document::new(cid(1));
    hub.ingest(ROOM, reg(&mut author, b"key", 1), None).unwrap();
    assert!(hub.create_version(ROOM, b"v1").unwrap());
    assert!(hub.fork_branch_from_version(ROOM, DRAFT, b"v1").unwrap());
    hub.set_active_branch(ROOM, DRAFT).unwrap();
    assert!(hub.publish(ROOM, PUBLISHED).unwrap());
    hub
}

/// Replace `draft`'s stored base with bytes no decoder accepts, then reload.
fn reopen_with_corrupt_draft_base(dir: &std::path::Path) -> Hub {
    let mut store = Store::open(dir).unwrap();
    store
        .write_branch_base(ROOM, DRAFT, b"not a snapshot")
        .unwrap();
    drop(store);
    let mut hub = Hub::from_rooms(cid(SERVER), Store::open(dir).unwrap().load().unwrap()).unwrap();
    hub.attach_store(Store::open(dir).unwrap());
    hub
}

#[test]
#[cfg_attr(miri, ignore)] // drives the store on the filesystem
fn publishing_over_an_unreadable_base_leaves_the_published_branch_alone() {
    let tmp = tempdir();
    let published_before = {
        let mut hub = seeded(tmp.path());
        match hub.catch_up_branch(ROOM, PUBLISHED, 0) {
            Catchup::Snapshot { state, .. } => state,
            _ => panic!("the published branch owns a base"),
        }
    };
    assert_eq!(int_in(&published_before, b"key"), 1);

    let mut hub = reopen_with_corrupt_draft_base(tmp.path());
    assert!(
        !hub.publish(ROOM, PUBLISHED).unwrap(),
        "a source whose state cannot be read publishes nothing"
    );

    // In memory and on disk both: the published branch still holds what it held.
    let after = match hub.catch_up_branch(ROOM, PUBLISHED, 0) {
        Catchup::Snapshot { state, .. } => state,
        _ => panic!("the published branch owns a base"),
    };
    assert_eq!(
        int_in(&after, b"key"),
        1,
        "publish froze an empty replica over the published branch"
    );
    let stored = Store::open(tmp.path()).unwrap().load().unwrap();
    let (_, log) = stored
        .into_iter()
        .find(|(room, _)| room == ROOM)
        .expect("the room persisted");
    let base = log
        .branch_bases
        .iter()
        .find(|(branch, _)| branch.as_slice() == PUBLISHED)
        .map(|(_, state)| state.clone())
        .expect("the published base persisted");
    assert_eq!(int_in(&base, b"key"), 1, "the durable base was overwritten");
}

#[test]
#[cfg_attr(miri, ignore)] // drives the store on the filesystem
fn an_unreadable_base_serves_no_catch_up() {
    let tmp = tempdir();
    drop(seeded(tmp.path()));
    let mut hub = reopen_with_corrupt_draft_base(tmp.path());

    // Not an empty delta: a subscriber told it is at the head of a stream it holds
    // none of would go on to edit from an empty document.
    assert!(
        matches!(hub.catch_up_branch(ROOM, DRAFT, 0), Catchup::Unavailable),
        "an unreadable base served a delta"
    );
}

// --- the same two rules, with no store behind them ---
//
// The pair above proves the durable path end to end and needs a real filesystem,
// which Miri's isolation refuses. These reach the same seams through
// `Hub::from_rooms`, so the rules themselves stay under the memory gate.

/// A room restored from a hand-built log: `key = 1` in its snapshot, a `draft`
/// branch whose owned base is `base`, and a `published` branch holding the same
/// snapshot state.
fn room_from(base: &[u8]) -> Hub {
    let mut author = Document::new(cid(1));
    let ops = reg(&mut author, b"key", 1);
    let state = author.encode_state();
    let log = RoomLog {
        snapshot: Some(Snapshot {
            base_seq: ops.len() as u64,
            state: state.clone(),
        }),
        branches: vec![
            Branch {
                name: b"main".to_vec(),
                fork_point: 0,
                head: ops.len() as u64,
                published: false,
            },
            Branch {
                name: DRAFT.to_vec(),
                fork_point: ops.len() as u64,
                head: ops.len() as u64,
                published: false,
            },
            Branch {
                name: PUBLISHED.to_vec(),
                fork_point: ops.len() as u64,
                head: ops.len() as u64,
                published: true,
            },
        ],
        branch_bases: vec![(DRAFT.to_vec(), base.to_vec()), (PUBLISHED.to_vec(), state)],
        active_branch: Some(DRAFT.to_vec()),
        ..RoomLog::default()
    };
    Hub::from_rooms(cid(SERVER), vec![(ROOM.to_vec(), log)]).unwrap()
}

#[test]
fn an_unreadable_base_serves_no_catch_up_in_memory() {
    let mut hub = room_from(b"not a snapshot");
    assert!(
        matches!(hub.catch_up_branch(ROOM, DRAFT, 0), Catchup::Unavailable),
        "an unreadable base served a delta"
    );
    // A readable one still serves, so the refusal is the decode's and not the seam
    // refusing everything.
    let mut author = Document::new(cid(1));
    reg(&mut author, b"key", 1);
    let mut ok = room_from(&author.encode_state());
    assert!(
        !matches!(ok.catch_up_branch(ROOM, DRAFT, 0), Catchup::Unavailable),
        "a readable base was refused"
    );
}

#[test]
fn publishing_over_an_unreadable_base_leaves_the_published_branch_alone_in_memory() {
    let mut hub = room_from(b"not a snapshot");
    assert!(
        !hub.publish(ROOM, PUBLISHED).unwrap(),
        "a source whose state cannot be read publishes nothing"
    );
    let after = match hub.catch_up_branch(ROOM, PUBLISHED, 0) {
        Catchup::Snapshot { state, .. } => state,
        _ => panic!("the published branch owns a base"),
    };
    assert_eq!(
        int_in(&after, b"key"),
        1,
        "publish froze an empty replica over the published branch"
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
    let dir = std::env::temp_dir().join(format!("crdtsync-unreadable-base-{pid}-{n}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}
