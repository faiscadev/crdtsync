//! Publish/draft and per-user branches over the branch primitive.
//!
//! `publish` points a read-only `published` branch's HEAD at the active editor
//! branch's current state — a snapshot editors' later writes do not touch until the
//! next publish. Republishing repoints `published` to the newer state and leaves the
//! previous published state reachable as a named version, so an app rolls published
//! state back independently of the editor branch. A client write to `published` is
//! refused — it is a publish target, advanced only by `publish`. `BeforePublish`
//! fires before the repoint, so an `on: before-publish` auto-version trigger
//! captures at the publish point.
//!
//! Per-user branches are the same primitive: a `user/<id>` branch forked from a
//! shared base edits in isolation — neither user's writes reach the base or each
//! other.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Element, ErrorCode, Message, Op, Scalar};
use crdtsync_server::{
    Catchup, ConnId, EngineEvent, EventSink, Hub, ManualClock, Registry, SchemaRegistry,
    PUBLISHED_BRANCH,
};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-1";
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
    r.set_clock(Arc::new(ManualClock::new(0)));
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

/// Subscribe `id` to `(room, branch)` on channel `ch`, returning the catch-up reply.
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

/// A fresh authenticated connection subscribed to `(room, branch)`, returning its
/// catch-up reply.
fn joiner(r: &mut Registry, client: u8, branch: &[u8]) -> Message {
    let id = r.connect();
    auth(r, id, client);
    subscribe(r, id, CH, ROOM, branch)
}

fn snapshot_state(m: Message) -> Vec<u8> {
    match m {
        Message::Snapshot { state, .. } => state,
        other => panic!("expected a Snapshot catch-up, got {other:?}"),
    }
}

/// Bring `ROOM` up on `main` with `age = value` from editor `a`.
fn seed(r: &mut Registry, value: i64) -> (ConnId, Document) {
    let a = r.connect();
    auth(r, a, 1);
    subscribe(r, a, CH, ROOM, b"");
    let mut main = doc(1);
    assert!(r.deliver(
        a,
        Message::Ops {
            channel: CH,
            ops: reg(&mut main, b"age", value),
        }
    ));
    r.take_outbox(a);
    (a, main)
}

fn edit(r: &mut Registry, a: ConnId, main: &mut Document, value: i64) {
    assert!(r.deliver(
        a,
        Message::Ops {
            channel: CH,
            ops: reg(main, b"age", value),
        }
    ));
    r.take_outbox(a);
}

// --- publish/draft ---

#[test]
fn publish_serves_the_editor_state_to_a_read_only_subscriber() {
    let mut r = registry();
    let (a, mut main) = seed(&mut r, 10);
    assert!(r.publish(ROOM, PUBLISHED_BRANCH).unwrap());

    // A read-only consumer is served the published editor state (age 10).
    let state = snapshot_state(joiner(&mut r, 2, PUBLISHED_BRANCH));
    assert_eq!(int_in(&state, b"age"), 10);

    // Editors keep writing main — that edit is invisible on published until the
    // next publish.
    edit(&mut r, a, &mut main, 20);
    let published = snapshot_state(joiner(&mut r, 3, PUBLISHED_BRANCH));
    assert_eq!(
        int_in(&published, b"age"),
        10,
        "a main edit is not visible on published until the next publish"
    );
    // The editor branch itself has moved to 20.
    let Message::Ops { ops, .. } = joiner(&mut r, 4, b"main") else {
        panic!("expected main's op delta");
    };
    let mut d = doc(9);
    for op in &ops {
        d.apply(op);
    }
    assert_eq!(int_in(&d.encode_state(), b"age"), 20);
}

#[test]
fn republish_repoints_and_leaves_the_old_state_reachable() {
    let mut r = registry();
    let (a, mut main) = seed(&mut r, 10);
    assert!(r.publish(ROOM, PUBLISHED_BRANCH).unwrap());
    let first_seq = r.hub().seq(ROOM);

    // Move the editor on and republish.
    edit(&mut r, a, &mut main, 20);
    assert!(r.publish(ROOM, PUBLISHED_BRANCH).unwrap());

    // A read-only consumer now sees the newer published state (age 20).
    let state = snapshot_state(joiner(&mut r, 2, PUBLISHED_BRANCH));
    assert_eq!(int_in(&state, b"age"), 20);

    // The previous published state (age 10) remains reachable as a captured version
    // — an app rolls published state back to it independently of the editor branch.
    let rollback = format!("publish/published/main@{first_seq}").into_bytes();
    let names = r.hub().version_names(ROOM);
    assert!(
        names.contains(&rollback),
        "the prior published state is a captured version: {names:?}"
    );
    let old = r.hub().version_state(ROOM, &rollback).unwrap();
    assert_eq!(int_in(old, b"age"), 10);
}

#[test]
fn a_client_write_to_the_published_branch_is_refused() {
    let mut r = registry();
    let _ = seed(&mut r, 10);
    assert!(r.publish(ROOM, PUBLISHED_BRANCH).unwrap());

    // A consumer subscribes to the published branch, then tries to write it.
    let consumer = r.connect();
    auth(&mut r, consumer, 2);
    subscribe(&mut r, consumer, CH, ROOM, PUBLISHED_BRANCH);
    let mut author = doc(2);
    let ops = reg(&mut author, b"age", 99);
    // The write is refused recoverably — the connection stays open, the ops are
    // named as rejected, and nothing is ingested.
    assert!(r.deliver(
        consumer,
        Message::Ops {
            channel: CH,
            ops: ops.clone()
        }
    ));
    let out = r.take_outbox(consumer);
    assert!(
        matches!(
            out.as_slice(),
            [Message::OpsRejected {
                reason: ErrorCode::Forbidden,
                ..
            }]
        ),
        "a write to a published branch is refused Forbidden, got {out:?}"
    );

    // The published state is untouched by the refused write.
    let state = snapshot_state(joiner(&mut r, 3, PUBLISHED_BRANCH));
    assert_eq!(int_in(&state, b"age"), 10);
}

#[test]
fn a_republish_from_a_switched_editor_branch_keeps_a_distinct_rollback() {
    // Publishing from two different editor branches that happen to share a head
    // number must leave two distinct rollback versions — the name is keyed on the
    // source branch, not the head alone, so the earlier published state is not lost.
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);
    hub.ingest(ROOM, reg(&mut main, b"age", 10), None).unwrap();
    hub.create_version(ROOM, b"v-early").unwrap(); // age 10 at seq 1
    hub.ingest(ROOM, reg(&mut main, b"age", 99), None).unwrap(); // main → seq 2

    // Publish from main (source main, seq 2).
    assert!(hub.publish(ROOM, PUBLISHED_BRANCH).unwrap());

    // Restore v-early as a branch and switch the active editor HEAD to it, then edit
    // it up to head 2 — the same head number main published at.
    assert!(hub
        .restore_as_branch(ROOM, b"v-early", b"restored")
        .unwrap());
    hub.ingest_branch(ROOM, b"restored", reg(&mut doc(2), b"extra", 7), None)
        .unwrap();
    assert_eq!(hub.branch(ROOM, b"restored").unwrap().head, 2);

    // Republish — now from the restored branch, at the colliding head 2.
    assert!(hub.publish(ROOM, PUBLISHED_BRANCH).unwrap());

    // Both published states are reachable as distinct versions.
    let from_main = hub
        .version_state(ROOM, b"publish/published/main@2")
        .unwrap();
    assert_eq!(int_in(from_main, b"age"), 99);
    let from_restored = hub
        .version_state(ROOM, b"publish/published/restored@2")
        .unwrap();
    assert_eq!(int_in(from_restored, b"age"), 10);
}

// --- BeforePublish + auto-version ---

/// A recording event sink counting `BeforePublish` fires.
struct PublishCounter(Arc<Mutex<Vec<Vec<u8>>>>);

impl EventSink for PublishCounter {
    fn on_event(&self, event: &EngineEvent) {
        if let EngineEvent::BeforePublish { branch, .. } = event {
            self.0.lock().unwrap().push(branch.to_vec());
        }
    }
}

#[test]
fn before_publish_fires_exactly_once_per_publish() {
    let mut r = registry();
    let _ = seed(&mut r, 10);
    let fires = Arc::new(Mutex::new(Vec::new()));
    r.add_event_sink(Box::new(PublishCounter(Arc::clone(&fires))));

    assert!(r.publish(ROOM, PUBLISHED_BRANCH).unwrap());
    assert_eq!(
        *fires.lock().unwrap(),
        vec![PUBLISHED_BRANCH.to_vec()],
        "BeforePublish fires exactly once, naming the published branch"
    );
}

const APP: &[u8] = b"collab";

/// A registry whose shared schema registry holds `APP` version 1 declaring the
/// given `autoVersion` body, driven by a manual clock at 0.
fn registry_with(auto_version: &str) -> Registry {
    let schema = format!(
        r#"{{ "schema": "collab", "version": 1, "root": "R",
            "types": {{ "R": {{ "kind": "map" }} }},
            "autoVersion": {auto_version} }}"#
    );
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, schema.as_bytes(), b"").unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

/// An enforcing connection for `APP` v1, subscribed to `ROOM` main with a write, so
/// the room is bound to `APP` and has state to publish.
fn seed_enforcing(r: &mut Registry) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(1),
            app_id: APP.to_vec(),
            schema_version: 1,
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"cred".to_vec(),
        }
    ));
    r.take_outbox(id);
    subscribe(r, id, CH, ROOM, b"");
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: CH,
            ops: reg(&mut doc(1), b"age", 10),
        }
    ));
    r.take_outbox(id);
    id
}

#[test]
fn an_on_before_publish_trigger_captures_on_publish() {
    let mut r = registry_with(r#"[{ "on": "before-publish", "name": "auto/pub/${timestamp}" }]"#);
    seed_enforcing(&mut r);

    assert!(r.publish(ROOM, PUBLISHED_BRANCH).unwrap());
    let names = r.hub().version_names(ROOM);
    assert!(
        names.contains(&b"auto/pub/00000000000000000000".to_vec()),
        "an on:before-publish trigger captured a version: {names:?}"
    );
}

// --- per-user branches ---

fn age(d: &mut Document, value: i64) -> Vec<Op> {
    d.transact(|tx| tx.register(b"age", Scalar::Int(value)))
}

fn kv(d: &mut Document, key: &[u8], value: i64) -> Vec<Op> {
    d.transact(|tx| tx.register(key, Scalar::Int(value)))
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

#[test]
fn per_user_branches_edit_in_isolation_over_a_shared_base() {
    let mut hub = Hub::new(cid(0xFF));
    // A shared base template on main.
    let mut base = doc(1);
    let base_ops = hub.ingest(ROOM, age(&mut base, 1), None).unwrap();
    let fork = hub.seq(ROOM);

    // Two per-user forks off the shared base.
    assert!(hub.fork_branch(ROOM, b"user/alice", b"main", fork).unwrap());
    assert!(hub.fork_branch(ROOM, b"user/bob", b"main", fork).unwrap());

    // Each user customizes their own fork.
    let alice_ops = hub
        .ingest_branch(ROOM, b"user/alice", kv(&mut doc(2), b"alice", 100), None)
        .unwrap();
    let bob_ops = hub
        .ingest_branch(ROOM, b"user/bob", kv(&mut doc(3), b"bob", 200), None)
        .unwrap();
    // A later write to the shared base after the forks.
    let base2 = hub.ingest(ROOM, kv(&mut base, b"note", 5), None).unwrap();

    // Alice's stream: the base plus her own edit, and neither Bob's nor the
    // post-fork base write.
    let alice = ids(&delta(hub.catch_up_branch(ROOM, b"user/alice", 0)));
    assert!(alice.contains(&base_ops[0].id));
    assert!(alice.contains(&alice_ops[0].id));
    assert!(
        !alice.contains(&bob_ops[0].id),
        "bob's edit must not reach alice"
    );
    assert!(
        !alice.contains(&base2[0].id),
        "a post-fork base write must not reach a per-user branch"
    );

    // Bob's stream is the mirror image.
    let bob = ids(&delta(hub.catch_up_branch(ROOM, b"user/bob", 0)));
    assert!(bob.contains(&base_ops[0].id));
    assert!(bob.contains(&bob_ops[0].id));
    assert!(
        !bob.contains(&alice_ops[0].id),
        "alice's edit must not reach bob"
    );

    // The shared base (main) carries neither user's customization.
    let main = ids(&delta(hub.catch_up(ROOM, 0)));
    assert!(
        !main.contains(&alice_ops[0].id),
        "a per-user edit must not touch the base"
    );
    assert!(
        !main.contains(&bob_ops[0].id),
        "a per-user edit must not touch the base"
    );
}

// --- durability: the published branch (read-only + its base) replays ---
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
    fn a_published_branch_survives_a_reopen_read_only() {
        let tmp = tempdir();
        {
            let mut hub = open_hub(tmp.path());
            let mut main = doc(1);
            hub.ingest(ROOM, age(&mut main, 10), None).unwrap();
            assert!(hub.publish(ROOM, PUBLISHED_BRANCH).unwrap());
        }

        let mut hub = open_hub(tmp.path());
        // The published branch is still a read-only publish target.
        assert!(hub.is_published(ROOM, PUBLISHED_BRANCH));
        // It still serves the published state.
        let state = match hub.catch_up_branch(ROOM, PUBLISHED_BRANCH, 0) {
            Catchup::Snapshot { state, .. } => state,
            Catchup::Ops(_) => panic!("expected the published branch's snapshot"),
        };
        assert_eq!(int_in(&state, b"age"), 10);
    }

    #[test]
    fn an_orphan_tail_under_a_published_name_is_ignored_on_load() {
        use crdtsync_server::StoredOp;
        let tmp = tempdir();
        {
            let mut hub = open_hub(tmp.path());
            let mut main = doc(1);
            hub.ingest(ROOM, age(&mut main, 10), None).unwrap();
            assert!(hub.publish(ROOM, PUBLISHED_BRANCH).unwrap());
        }
        // Simulate the orphan a failed best-effort tail removal would leave: append a
        // divergent op straight to disk under the published branch's name.
        {
            let mut store = Store::open(tmp.path()).unwrap();
            let stray: Vec<StoredOp> = age(&mut doc(2), 999)
                .into_iter()
                .map(|op| StoredOp::new(op, None))
                .collect();
            store.append_branch(ROOM, PUBLISHED_BRANCH, &stray).unwrap();
        }

        // On reload the stray tail is dropped — a published branch never diverges —
        // so a read-only consumer still sees the published base (age 10), not 999.
        let mut hub = open_hub(tmp.path());
        assert!(hub.is_published(ROOM, PUBLISHED_BRANCH));
        let state = match hub.catch_up_branch(ROOM, PUBLISHED_BRANCH, 0) {
            Catchup::Snapshot { state, .. } => state,
            Catchup::Ops(_) => panic!("expected the published branch's snapshot"),
        };
        assert_eq!(int_in(&state, b"age"), 10);
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
        let dir = std::env::temp_dir().join(format!("crdtsync-publish-{pid}-{n}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}
