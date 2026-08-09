//! The three unrelated things a branch can fail to materialize as, told apart (C51).
//!
//! `Hub::materialize_branch` folds the tree a `(room, branch)` stream serves into a
//! whole-replica state. Three situations answer no state, and they are not the same
//! situation: the room's registry does not hold the name; the branch is enumerable
//! but this node cannot read the base it names — a durable snapshot base that no
//! longer decodes, or a live-log fork whose shared base `main`'s retained log no
//! longer covers; and `main` on a room this node holds no replica for, which is every
//! subscribed-but-never-written room.
//!
//! The diff seam collapsed all three into `DiffError::UnknownBranch`, so the wire
//! answered `NotFound "unknown branch draft"` for a branch `BranchList` enumerates,
//! and `NotFound "unknown branch main"` for a room that simply has no ops yet. They
//! are answered apart now: absent is the `NotFound` it always was, unreadable is an
//! `Internal` fault naming the branch, and an empty room's `main` diffs as the empty
//! state it is.
//!
//! The publish path reads the same seam and takes none of the three — it writes what
//! it folds over the target branch's base *and* captures it as a permanent version,
//! so a stream this node cannot state must freeze nothing. That is the constraint
//! that made the three have to be told apart rather than merged, and it is pinned
//! here beside them. `unreadable_branch_base` pins the same refusal against the
//! durable path end to end.

use crdtsync_core::diff::{decode_changes, Change};
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::{Channel, DiffKind};
use crdtsync_core::{ClientId, Document, ElementKind, ErrorCode, Message, Op, Scalar};
use crdtsync_server::store::{Branch, RoomLog, Snapshot, Store};
use crdtsync_server::{Catchup, ConnId, DiffError, Hub, Registry};
use std::fs;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-t";
const SERVER: u8 = 0xFF;
const MAIN: &[u8] = b"main";
const DRAFT: &[u8] = b"draft";
const PUBLISHED: &[u8] = b"published";
const CH: Channel = Channel(0);

fn reg(d: &mut Document, key: &[u8], value: i64) -> Vec<Op> {
    d.transact(|tx| tx.register(key, Scalar::Int(value)))
}

/// Hello + Auth + Subscribe, holding `ROOM` on `CH`. The subscribe is what binds the
/// channel a diff query is keyed by; it does not itself write the room.
fn joined(r: &mut Registry, client: u8) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"actor".to_vec(),
        }
    ));
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            last_seen_seq: 0,
            branch: Vec::new(),
            zone: Vec::new(),
        }
    ));
    r.take_outbox(id);
    id
}

/// A branch diff on `CH`, asserting the query left the connection open — every answer
/// in this taxonomy is recoverable, whichever code it carries.
fn diff(r: &mut Registry, id: ConnId, a: &[u8], b: &[u8]) -> Vec<Message> {
    assert!(
        r.deliver(
            id,
            Message::DiffQuery {
                channel: CH,
                kind: DiffKind::Branches,
                a: a.to_vec(),
                b: b.to_vec(),
            }
        ),
        "a diff answer closed the connection"
    );
    r.take_outbox(id)
}

/// The error code of a single-reply outbox, or a panic if it carries no error.
fn error_code(out: &[Message]) -> ErrorCode {
    match &out[0] {
        Message::Error { code, .. } => *code,
        other => panic!("expected an error, got {other:?}"),
    }
}

// --- absent: a name the room's registry does not hold ---

#[test]
fn a_branch_the_room_does_not_hold_is_a_not_found() {
    let mut r = Registry::new(cid(SERVER));
    let id = joined(&mut r, 1);
    let mut author = Document::new(cid(1));
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: CH,
            ops: reg(&mut author, b"key", 1)
        }
    ));
    r.take_outbox(id);

    let out = diff(&mut r, id, MAIN, b"ghost");
    assert_eq!(
        error_code(&out),
        ErrorCode::NotFound,
        "an absent branch is the recoverable not-found it always was"
    );
}

// --- empty: `main` on a room this node holds no replica for ---

#[test]
fn an_empty_room_diffs_main_as_the_empty_state() {
    // Subscribed, never written: the channel is bound and the hub holds no room.
    let mut r = Registry::new(cid(SERVER));
    let id = joined(&mut r, 1);
    assert!(
        r.hub().export_room(ROOM).is_none(),
        "a subscribe alone does not materialize the room"
    );
    assert!(
        r.hub().branches(ROOM).iter().any(|b| b.name == MAIN),
        "`main` is enumerable on every room"
    );

    let out = diff(&mut r, id, MAIN, MAIN);
    match &out[0] {
        Message::DiffResult { changes, .. } => assert!(
            decode_changes(changes).expect("decodes").is_empty(),
            "a room with no ops has no changes between two reads of `main`"
        ),
        other => panic!("a room with no ops answered `main` unknown: {other:?}"),
    }
}

#[test]
fn an_empty_room_diffs_main_against_a_written_branch() {
    // The asymmetric shape: one side has no replica behind it, the other does. A
    // room can hold a branch with a divergent tail before `main` itself is ingested
    // into, so the empty side is not merely the trivial self-diff above.
    let mut hub = Hub::new(cid(SERVER));
    assert!(hub.fork_branch(ROOM, DRAFT, MAIN, 0).unwrap());
    let mut author = Document::new(cid(1));
    hub.ingest_branch(ROOM, DRAFT, reg(&mut author, b"key", 7), None)
        .unwrap();

    let changes = hub
        .diff_branches(ROOM, MAIN, DRAFT, |s| s)
        .expect("an empty `main` is a state, not an unknown branch");
    assert_eq!(
        changes,
        vec![Change::Added {
            path: encode_path(&[b"key"]),
            kind: ElementKind::Register,
        }],
        "the branch's whole divergence is an add over the empty state"
    );
}

// --- unreadable: the branch is enumerable and its base does not decode ---

/// A room restored from a hand-built log: `key = 1` in its snapshot, a `draft`
/// branch whose owned base is `base`, and a `published` branch holding the snapshot
/// state. Mirrors `unreadable_branch_base`'s in-memory fixture, which is how a
/// damaged durable base is reached without a filesystem.
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
                name: MAIN.to_vec(),
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
fn an_unreadable_base_is_not_an_absent_branch() {
    let mut hub = room_from(b"not a snapshot");
    assert!(
        hub.branch(ROOM, DRAFT).is_some(),
        "the branch the registry enumerates"
    );
    assert!(
        matches!(hub.catch_up_branch(ROOM, DRAFT, 0), Catchup::Unavailable),
        "the base this node cannot read"
    );

    let err = hub
        .diff_branches(ROOM, MAIN, DRAFT, |s| s)
        .expect_err("an unreadable base has no state to diff");
    assert_eq!(
        err,
        DiffError::UnreadableBranch(DRAFT.to_vec()),
        "a branch that exists was not reported unreadable, by name"
    );
}

#[test]
fn a_shared_base_compaction_has_dropped_is_unreadable_too() {
    // The second way a branch the registry holds has no state here, and the one that
    // needs no damage: a live-log fork reads `main`'s retained log below its fork
    // point, and compacting past that point leaves the stream unfoldable (C88).
    let mut hub = Hub::new(cid(SERVER));
    let mut author = Document::new(cid(1));
    hub.ingest(ROOM, reg(&mut author, b"key", 1), None).unwrap();
    let fork = hub.seq(ROOM);
    assert!(hub.fork_branch(ROOM, DRAFT, MAIN, fork).unwrap());
    assert!(
        hub.diff_branches(ROOM, MAIN, DRAFT, |s| s).is_ok(),
        "the fork diffs while `main` still retains its base"
    );

    hub.compact(ROOM).unwrap();
    assert!(
        hub.branch(ROOM, DRAFT).is_some(),
        "compaction does not retire the branch"
    );
    assert_eq!(
        hub.diff_branches(ROOM, MAIN, DRAFT, |s| s),
        Err(DiffError::UnreadableBranch(DRAFT.to_vec())),
        "a shared base the floor has passed was reported absent"
    );
    // The publish refusal covers this flavour too, so the split does not let a
    // base-less fold reach a durable capture.
    hub.set_active_branch(ROOM, DRAFT).unwrap();
    assert!(
        !hub.publish(ROOM, PUBLISHED).unwrap(),
        "a clipped shared base publishes nothing"
    );
    assert!(
        hub.branch(ROOM, PUBLISHED).is_none(),
        "the published branch was pointed at a base-less fold"
    );
}

#[test]
fn a_publish_takes_neither_of_the_two_answers_a_diff_reports() {
    // The constraint that keeps the answers apart rather than merged. A diff reports
    // `Unreadable` and `Empty` differently, and a publish takes neither: it writes
    // what it folds over the target's base and captures it permanently, so both
    // refusals stand exactly where the diff seam's answers diverge.
    let mut unreadable = room_from(b"not a snapshot");
    assert_eq!(
        unreadable.diff_branches(ROOM, MAIN, DRAFT, |s| s),
        Err(DiffError::UnreadableBranch(DRAFT.to_vec()))
    );
    // What the split could have broken is the refusal itself; that the refused publish
    // leaves the target's own state untouched is `unreadable_branch_base`'s, in memory
    // and against the durable base both.
    assert!(
        !unreadable.publish(ROOM, PUBLISHED).unwrap(),
        "a source whose state cannot be read publishes nothing"
    );

    let mut empty = Hub::new(cid(SERVER));
    assert!(empty
        .diff_branches(ROOM, MAIN, MAIN, |s| s)
        .unwrap()
        .is_empty());
    assert!(
        !empty.publish(ROOM, PUBLISHED).unwrap(),
        "a room with no replica publishes nothing"
    );
    assert!(
        empty.branch(ROOM, PUBLISHED).is_none(),
        "the published branch was never pointed"
    );
}

// --- the same unreadable case at the wire, off a damaged durable base ---

#[test]
#[cfg_attr(miri, ignore)] // drives the store on the filesystem
fn a_damaged_durable_base_surfaces_as_a_fault_not_an_absence() {
    let tmp = tempdir();
    {
        let mut r = Registry::with_store(cid(SERVER), Store::open(tmp.path()).unwrap()).unwrap();
        let id = joined(&mut r, 1);
        let mut author = Document::new(cid(1));
        assert!(r.deliver(
            id,
            Message::Ops {
                channel: CH,
                ops: reg(&mut author, b"key", 1)
            }
        ));
        r.take_outbox(id);
        assert!(r.hub_mut().create_version(ROOM, b"v1").unwrap());
        assert!(r
            .hub_mut()
            .fork_branch_from_version(ROOM, DRAFT, b"v1")
            .unwrap());
    }
    // Damage the branch's durable base, then reload from it.
    let mut store = Store::open(tmp.path()).unwrap();
    store
        .write_branch_base(ROOM, DRAFT, b"not a snapshot")
        .unwrap();
    drop(store);

    let mut r = Registry::with_store(cid(SERVER), Store::open(tmp.path()).unwrap()).unwrap();
    let id = joined(&mut r, 1);
    assert!(
        r.hub().branches(ROOM).iter().any(|b| b.name == DRAFT),
        "`BranchList` enumerates the branch"
    );

    let out = diff(&mut r, id, MAIN, DRAFT);
    assert_eq!(
        error_code(&out),
        ErrorCode::Internal,
        "a base this node cannot read was reported as a branch that does not exist"
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
    let dir = std::env::temp_dir().join(format!("crdtsync-branch-taxonomy-{pid}-{n}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}
