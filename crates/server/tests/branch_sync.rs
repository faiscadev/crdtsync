//! Branch-scoped subscription and log resolution.
//!
//! A `(room, branch)` subscription serves that branch's stream: the room's base
//! history up to the branch's fork point (shared, immutable — from `main`'s log)
//! followed by the branch's own divergent ops past the fork. A write on a branch
//! appends to that branch's tail and fans out only to its subscribers, never
//! crossing into another branch's stream. `main` (the empty branch) is the whole
//! existing log, unchanged.

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Op, Scalar};
use crdtsync_server::{Catchup, ConnId, Hub, Registry};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-1";
const DRAFT: &[u8] = b"draft";

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn age(d: &mut Document, value: i64) -> Vec<Op> {
    d.transact(|tx| tx.register(b"age", Scalar::Int(value)))
}

fn delta(c: Catchup) -> Vec<Op> {
    match c {
        Catchup::Ops(v) => v.into_iter().map(|rec| rec.op).collect(),
        Catchup::Snapshot { .. } => panic!("expected an op delta, got a snapshot"),
    }
}

fn ids(ops: &[Op]) -> Vec<crdtsync_core::op::OpId> {
    ops.iter().map(|op| op.id).collect()
}

// --- Hub-level log resolution ---

#[test]
fn branch_catch_up_serves_the_shared_base_then_its_own_tail() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);
    let base1 = hub.ingest(ROOM, age(&mut main, 30), None).unwrap();
    let base2 = hub.ingest(ROOM, age(&mut main, 40), None).unwrap();
    let fork = hub.seq(ROOM);

    assert!(hub.fork_branch(ROOM, DRAFT, b"main", fork).unwrap());

    let mut draft = doc(2);
    let branch_ops = hub
        .ingest_branch(ROOM, DRAFT, age(&mut draft, 99), None)
        .unwrap();
    // A post-fork write on main must not leak into the draft stream.
    let main_only = hub.ingest(ROOM, age(&mut main, 50), None).unwrap();

    let mut expected = base1.clone();
    expected.extend(base2.clone());
    expected.extend(branch_ops.clone());
    assert_eq!(delta(hub.catch_up_branch(ROOM, DRAFT, 0)), expected);

    let served = ids(&delta(hub.catch_up_branch(ROOM, DRAFT, 0)));
    for op in &main_only {
        assert!(
            !served.contains(&op.id),
            "a post-fork main op must not appear in the draft stream"
        );
    }
}

#[test]
fn main_catch_up_is_unchanged_by_a_fork() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);
    let base1 = hub.ingest(ROOM, age(&mut main, 30), None).unwrap();
    let base2 = hub.ingest(ROOM, age(&mut main, 40), None).unwrap();
    let fork = hub.seq(ROOM);
    hub.fork_branch(ROOM, DRAFT, b"main", fork).unwrap();

    let mut draft = doc(2);
    let branch_ops = hub
        .ingest_branch(ROOM, DRAFT, age(&mut draft, 99), None)
        .unwrap();
    let main3 = hub.ingest(ROOM, age(&mut main, 50), None).unwrap();

    let mut expected = base1;
    expected.extend(base2);
    expected.extend(main3);
    assert_eq!(delta(hub.catch_up(ROOM, 0)), expected);

    // The branch's divergent ops are invisible to a main subscriber.
    let served = ids(&delta(hub.catch_up(ROOM, 0)));
    for op in &branch_ops {
        assert!(
            !served.contains(&op.id),
            "a draft op must not appear on main"
        );
    }
}

#[test]
fn the_shared_base_is_identical_and_not_duplicated() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);
    let base = hub.ingest(ROOM, age(&mut main, 30), None).unwrap();
    let fork = hub.seq(ROOM);
    hub.fork_branch(ROOM, DRAFT, b"main", fork).unwrap();

    let base_id = base[0].id;
    let on_main = ids(&delta(hub.catch_up(ROOM, 0)));
    let on_draft = ids(&delta(hub.catch_up_branch(ROOM, DRAFT, 0)));
    assert!(on_main.contains(&base_id));
    assert!(on_draft.contains(&base_id));
}

#[test]
fn a_branch_write_advances_only_that_branch_head() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);
    hub.ingest(ROOM, age(&mut main, 30), None).unwrap();
    let fork = hub.seq(ROOM);
    hub.fork_branch(ROOM, DRAFT, b"main", fork).unwrap();
    assert_eq!(hub.branch(ROOM, DRAFT).unwrap().head, fork);

    let mut draft = doc(2);
    let n = hub
        .ingest_branch(ROOM, DRAFT, age(&mut draft, 99), None)
        .unwrap()
        .len() as u64;
    assert_eq!(hub.branch(ROOM, DRAFT).unwrap().head, fork + n);
    // The room's own (main) head is untouched by a branch write.
    assert_eq!(hub.seq(ROOM), fork);
}

#[test]
fn a_branch_write_deduplicates_a_resend() {
    let mut hub = Hub::new(cid(0xFF));
    let fork = hub.seq(ROOM);
    hub.fork_branch(ROOM, DRAFT, b"main", fork).unwrap();
    let mut draft = doc(2);
    let ops = age(&mut draft, 99);
    assert!(!hub
        .ingest_branch(ROOM, DRAFT, ops.clone(), None)
        .unwrap()
        .is_empty());
    // A resend of the same ops is deduped — nothing fresh, no double-count.
    assert!(hub
        .ingest_branch(ROOM, DRAFT, ops, None)
        .unwrap()
        .is_empty());
    let head = hub.branch(ROOM, DRAFT).unwrap().head;
    assert_eq!(
        head,
        fork + delta(hub.catch_up_branch(ROOM, DRAFT, fork)).len() as u64
    );
}

#[test]
fn a_fork_point_is_clamped_to_existing_history() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);
    hub.ingest(ROOM, age(&mut main, 30), None).unwrap();
    // Forking "at 99" on a one-op room shares only the history that exists: the
    // fork point is clamped to the room head.
    assert!(hub.fork_branch(ROOM, DRAFT, b"main", 99).unwrap());
    assert_eq!(hub.branch(ROOM, DRAFT).unwrap().fork_point, hub.seq(ROOM));

    // A post-fork main write is main's divergence, not shared base — it must not
    // leak into the branch stream even though the branch was asked to fork "at 99".
    let main_only = hub.ingest(ROOM, age(&mut main, 40), None).unwrap();
    let served = ids(&delta(hub.catch_up_branch(ROOM, DRAFT, 0)));
    for op in &main_only {
        assert!(
            !served.contains(&op.id),
            "a post-fork main op must not leak into the branch base"
        );
    }
}

#[test]
fn a_branch_stream_is_contiguous_so_a_reconnect_at_head_serves_nothing() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);
    hub.ingest(ROOM, age(&mut main, 30), None).unwrap();
    let fork = hub.seq(ROOM);
    hub.fork_branch(ROOM, DRAFT, b"main", fork).unwrap();
    hub.ingest_branch(ROOM, DRAFT, age(&mut doc(2), 99), None)
        .unwrap();

    // The full stream length equals the branch head — no gap, no double-count —
    // so a client that counted every op to `head` is re-served nothing.
    let head = hub.branch(ROOM, DRAFT).unwrap().head;
    assert_eq!(
        delta(hub.catch_up_branch(ROOM, DRAFT, 0)).len() as u64,
        head
    );
    assert!(delta(hub.catch_up_branch(ROOM, DRAFT, head)).is_empty());
}

// --- Registry-level fan-out scoping ---

const CH: Channel = Channel(0);

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
            credential: b"cred".to_vec()
        }
    ));
    r.take_outbox(id);
}

fn sub(room: &[u8], branch: &[u8]) -> Message {
    Message::Subscribe {
        channel: CH,
        room: room.to_vec(),
        branch: branch.to_vec(),
        zone: Vec::new(),
        last_seen_seq: 0,
    }
}

/// Bring up a connection subscribed to `(room, branch)`, discarding the catch-up.
fn join(r: &mut Registry, client: u8, room: &[u8], branch: &[u8]) -> ConnId {
    let id = r.connect();
    auth(r, id, client);
    assert!(r.deliver(id, sub(room, branch)));
    r.take_outbox(id);
    id
}

#[test]
fn a_branch_write_reaches_only_that_branch_subscribers() {
    let mut r = registry();
    r.hub_mut().fork_branch(ROOM, DRAFT, b"main", 0).unwrap();

    let main1 = join(&mut r, 1, ROOM, b"main");
    let main2 = join(&mut r, 2, ROOM, b"");
    let draft1 = join(&mut r, 3, ROOM, DRAFT);
    let draft2 = join(&mut r, 4, ROOM, DRAFT);

    // A draft write reaches the other draft subscriber, and no main subscriber.
    let ops = age(&mut doc(3), 99);
    r.deliver(
        draft1,
        Message::Ops {
            channel: CH,
            ops: ops.clone(),
        },
    );
    assert_eq!(
        r.take_outbox(draft2),
        vec![Message::Ops { channel: CH, ops }]
    );
    assert!(
        r.take_outbox(main1).is_empty(),
        "a draft write must not reach main"
    );
    assert!(
        r.take_outbox(main2).is_empty(),
        "a draft write must not reach main"
    );
    r.take_outbox(draft1);

    // A main write reaches the other main subscriber, and no draft subscriber.
    let ops = age(&mut doc(1), 30);
    r.deliver(
        main1,
        Message::Ops {
            channel: CH,
            ops: ops.clone(),
        },
    );
    assert_eq!(
        r.take_outbox(main2),
        vec![Message::Ops { channel: CH, ops }]
    );
    assert!(
        r.take_outbox(draft1).is_empty(),
        "a main write must not reach draft"
    );
    assert!(
        r.take_outbox(draft2).is_empty(),
        "a main write must not reach draft"
    );
}

#[test]
fn a_draft_subscriber_catches_up_the_branch_stream() {
    let mut r = registry();
    // Base op on main, then fork draft at the current head.
    let main = join(&mut r, 1, ROOM, b"main");
    r.deliver(
        main,
        Message::Ops {
            channel: CH,
            ops: age(&mut doc(1), 30),
        },
    );
    r.take_outbox(main);
    let fork = r.hub().seq(ROOM);
    r.hub_mut().fork_branch(ROOM, DRAFT, b"main", fork).unwrap();

    // A draft write lands on the branch.
    let draft1 = join(&mut r, 2, ROOM, DRAFT);
    let branch_ops = age(&mut doc(2), 99);
    r.deliver(
        draft1,
        Message::Ops {
            channel: CH,
            ops: branch_ops.clone(),
        },
    );
    r.take_outbox(draft1);

    // A fresh draft joiner catches up the shared base plus the branch tail.
    let late = r.connect();
    auth(&mut r, late, 3);
    r.deliver(late, sub(ROOM, DRAFT));
    let out = r.take_outbox(late);
    let Message::Ops { ops, .. } = &out[0] else {
        panic!("expected a catch-up Ops, got {out:?}");
    };
    // Base (age 30) + the branch write (age 99): two ops on the stream.
    assert_eq!(ops.len(), 2);
    assert!(ops.contains(&branch_ops[0]));
}

#[test]
fn an_unknown_non_main_branch_is_rejected_not_served_as_main() {
    let mut r = registry();
    let a = join(&mut r, 1, ROOM, b"main");
    r.deliver(
        a,
        Message::Ops {
            channel: CH,
            ops: age(&mut doc(1), 30),
        },
    );
    r.take_outbox(a);

    let ghost = r.connect();
    auth(&mut r, ghost, 2);
    // Subscribing to a branch that was never forked is refused, not silently
    // served main's stream.
    assert!(r.deliver(ghost, sub(ROOM, b"ghost")));
    let out = r.take_outbox(ghost);
    assert!(
        matches!(
            out.as_slice(),
            [Message::Error {
                code: ErrorCode::UnknownRoom,
                ..
            }]
        ),
        "an unknown branch subscribe should be an error, got {out:?}"
    );

    // The channel is not bound: a later main write reaches it with nothing.
    r.deliver(
        a,
        Message::Ops {
            channel: CH,
            ops: age(&mut doc(1), 40),
        },
    );
    assert!(
        r.take_outbox(ghost).is_empty(),
        "a rejected branch subscribe must not become a live main subscription"
    );
}

// --- durability: a branch tail survives a store reopen ---
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
    fn a_branch_tail_survives_a_reopen() {
        let tmp = tempdir();
        let (fork, tail_len, expected);
        {
            let mut hub = open_hub(tmp.path());
            let mut main = doc(1);
            hub.ingest(ROOM, age(&mut main, 30), None).unwrap();
            fork = hub.seq(ROOM);
            hub.fork_branch(ROOM, DRAFT, b"main", fork).unwrap();
            let mut draft = doc(2);
            let branch_ops = hub
                .ingest_branch(ROOM, DRAFT, age(&mut draft, 99), None)
                .unwrap();
            tail_len = branch_ops.len() as u64;
            expected = delta(hub.catch_up_branch(ROOM, DRAFT, 0));
            assert!(tail_len > 0);
        }

        let mut hub = open_hub(tmp.path());
        assert_eq!(hub.branch(ROOM, DRAFT).unwrap().fork_point, fork);
        assert_eq!(hub.branch(ROOM, DRAFT).unwrap().head, fork + tail_len);
        assert_eq!(delta(hub.catch_up_branch(ROOM, DRAFT, 0)), expected);
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
        let dir = std::env::temp_dir().join(format!("crdtsync-branch-sync-{pid}-{n}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}
