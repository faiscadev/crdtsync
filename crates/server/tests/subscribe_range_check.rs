//! The handshake range-check: a subscriber that cannot reach the room's
//! governing version across a back-compatible path is refused with
//! `onUpdateRequired` before it becomes a subscriber, so down-translation at
//! fan-out only ever traverses invertible edges. Forward is always reachable; a
//! back-compatible gap never rejects; a foreign-app client (a different version
//! space) is not range-checked.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, ErrorCode, Message, Scalar};
use crdtsync_core::{Document, Op};
use crdtsync_server::{ConnId, ManualClock, Registry, SchemaRegistry};

const ROOM: &[u8] = b"room-a";
/// v1→v2 renames `age`→`years`: a breaking (forward-only) edge.
const UP: &[u8] = b"up";
/// v1→v2 adds a `note` field: a back-compatible edge.
const DOWN: &[u8] = b"down";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const MAP_V1: &str = r#"{ "schema": "s", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } } }"#;
const MAP_V2: &str = r#"{ "schema": "s", "version": 2, "root": "R",
    "types": { "R": { "kind": "map" } } }"#;

fn schema_registry() -> Arc<Mutex<SchemaRegistry>> {
    let mut sr = SchemaRegistry::new();
    sr.register(UP, 1, MAP_V1.as_bytes(), b"").unwrap();
    sr.register(
        UP,
        2,
        MAP_V2.as_bytes(),
        br#"{ "from": 1, "to": 2, "steps": [ { "kind": "renameField", "type": "R", "from": "age", "to": "years" } ] }"#,
    )
    .unwrap();
    sr.register(DOWN, 1, MAP_V1.as_bytes(), b"").unwrap();
    sr.register(
        DOWN,
        2,
        MAP_V2.as_bytes(),
        br#"{ "from": 1, "to": 2, "steps": [ { "kind": "addField", "type": "R", "field": "note", "fieldType": "text" } ] }"#,
    )
    .unwrap();
    Arc::new(Mutex::new(sr))
}

fn registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(schema_registry());
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

/// A registry backed by the durable store at `path`, sharing the schema registry
/// and a deterministic clock — the shape a restart reopens.
fn store_registry(path: &std::path::Path) -> Registry {
    let store = crdtsync_server::store::Store::open(path).unwrap();
    let mut r = Registry::with_store(cid(0xFF), store).unwrap();
    r.set_schema_registry(schema_registry());
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tempdir() -> TempDir {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("crdtsync-range-restart-{pid}-{n}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}

fn hello(r: &mut Registry, client: u8, app: &[u8], version: u32) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: app.to_vec(),
            schema_version: version,
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: format!("actor-{client}").into_bytes(),
        }
    ));
    r.take_outbox(id);
    id
}

/// Deliver a Subscribe from `id` and return its reply messages.
fn subscribe_reply(r: &mut Registry, id: ConnId) -> Vec<Message> {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            last_seen_seq: 0,
            branch: Vec::new(),
        }
    ));
    r.take_outbox(id)
}

/// Whether `replies` carry an `UpdateRequired` error.
fn is_update_required(replies: &[Message]) -> bool {
    replies.iter().any(|m| {
        matches!(
            m,
            Message::Error {
                code: ErrorCode::UpdateRequired,
                ..
            }
        )
    })
}

/// Whether `replies` carry a catch-up (a successful subscribe).
fn is_subscribed(replies: &[Message]) -> bool {
    replies
        .iter()
        .any(|m| matches!(m, Message::Ops { .. } | Message::Snapshot { .. }))
}

fn set(client: u8, key: &[u8]) -> Vec<Op> {
    Document::new(cid(client)).transact(|tx| tx.register(key, Scalar::Int(1)))
}

/// Deliver a write of `key` from an already-subscribed enforcing connection and
/// assert the hub accepted it, so the op lands in the room's log tagged at the
/// writer's version — the log's op-version high-water the range-check reads.
fn write(r: &mut Registry, id: ConnId, client: u8, key: &[u8]) {
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: Channel(0),
            ops: set(client, key),
        }
    ));
    let replies = r.take_outbox(id);
    assert!(
        replies
            .iter()
            .any(|m| matches!(m, Message::Accepted { .. })),
        "the write must be accepted and logged"
    );
}

/// Bind `ROOM` to `app` at `version` by subscribing an enforcing client there,
/// then clear its outbox. Returns the binder so the caller can keep it live.
fn bind_room(r: &mut Registry, client: u8, app: &[u8], version: u32) -> ConnId {
    let id = hello(r, client, app, version);
    let replies = subscribe_reply(r, id);
    assert!(is_subscribed(&replies), "the binder itself must subscribe");
    id
}

#[test]
fn a_client_below_a_breaking_gap_is_refused_with_update_required() {
    let mut r = registry();
    // The room holds a real v2 op; v1→v2 is a breaking rename, so v1 cannot reach
    // the log's v2 high-water.
    let writer = bind_room(&mut r, 1, UP, 2);
    write(&mut r, writer, 1, b"years");

    let old = hello(&mut r, 2, UP, 1);
    let replies = subscribe_reply(&mut r, old);
    assert!(
        is_update_required(&replies),
        "a v1 client below the breaking rename is refused"
    );
    assert!(
        !is_subscribed(&replies),
        "and it never becomes a subscriber"
    );
}

#[test]
fn a_refused_client_receives_no_further_fan_out() {
    let mut r = registry();
    let writer = bind_room(&mut r, 1, UP, 2);
    // A real v2 op floors the log at v2, so the v1 joiner is genuinely refused.
    write(&mut r, writer, 1, b"years");

    let old = hello(&mut r, 2, UP, 1);
    let replies = subscribe_reply(&mut r, old);
    assert!(is_update_required(&replies), "the v1 joiner is refused");

    // A later write to the room must not reach the refused client — it never
    // joined, so it is not in the fan-out set.
    assert!(r.deliver(
        writer,
        Message::Ops {
            channel: Channel(0),
            ops: set(1, b"decades"),
        }
    ));
    let late = r.take_outbox(old);
    assert!(
        !late.iter().any(|m| matches!(m, Message::Ops { .. })),
        "a refused client receives no ops"
    );
}

#[test]
fn a_reachable_older_client_over_a_back_compatible_gap_subscribes() {
    let mut r = registry();
    // DOWN's v1→v2 adds a field: back-compatible, so v1 is reachable from the
    // log's v2 high-water.
    let writer = bind_room(&mut r, 1, DOWN, 2);
    write(&mut r, writer, 1, b"note");

    let old = hello(&mut r, 2, DOWN, 1);
    let replies = subscribe_reply(&mut r, old);
    assert!(
        !is_update_required(&replies),
        "a back-compat gap never rejects"
    );
    assert!(is_subscribed(&replies), "the older client joins");
}

#[test]
fn a_newer_client_is_never_refused() {
    let mut r = registry();
    // Room governed at v1; a v2 client joins — forward is always reachable.
    bind_room(&mut r, 1, UP, 1);

    let newer = hello(&mut r, 2, UP, 2);
    let replies = subscribe_reply(&mut r, newer);
    assert!(!is_update_required(&replies));
    assert!(is_subscribed(&replies));
}

#[test]
fn a_same_version_client_is_never_refused() {
    let mut r = registry();
    bind_room(&mut r, 1, UP, 2);

    let peer = hello(&mut r, 2, UP, 2);
    let replies = subscribe_reply(&mut r, peer);
    assert!(!is_update_required(&replies));
    assert!(is_subscribed(&replies));
}

#[test]
fn a_foreign_app_client_is_not_range_checked() {
    let mut r = registry();
    // UP governs the room at v2. A DOWN-app v1 client's version is a different
    // space, so the UP rename gap must not refuse it — it subscribes and is
    // served verbatim.
    bind_room(&mut r, 1, UP, 2);

    let foreign = hello(&mut r, 2, DOWN, 1);
    let replies = subscribe_reply(&mut r, foreign);
    assert!(
        !is_update_required(&replies),
        "a foreign app is not range-checked"
    );
    assert!(is_subscribed(&replies));
}

#[test]
fn a_client_reachable_over_the_logged_ops_is_admitted_despite_a_lifted_floor() {
    let mut r = registry();
    // A v1 writer binds the room and logs a v1 op — the whole log is v1.
    let v1_writer = bind_room(&mut r, 1, UP, 1);
    write(&mut r, v1_writer, 1, b"age");

    // A transient v2 peer subscribes, lifting the sticky governing floor to v2,
    // then leaves without ever writing — the log stays entirely v1.
    let transient = hello(&mut r, 2, UP, 2);
    assert!(is_subscribed(&subscribe_reply(&mut r, transient)));
    r.disconnect(transient);

    // A returning v1 client is admitted: the log's op-version high-water is v1,
    // which it reaches trivially, even though the sticky floor sits at v2 across
    // the breaking rename the departed peer would have required.
    let returning = hello(&mut r, 3, UP, 1);
    let replies = subscribe_reply(&mut r, returning);
    assert!(
        !is_update_required(&replies),
        "the log is all v1; a v1 client is served in full"
    );
    assert!(is_subscribed(&replies));
}

#[test]
fn a_below_gap_joiner_is_refused_even_after_the_log_is_compacted() {
    let mut r = registry();
    // Fold every op into the snapshot at once, so the live log is empty while the
    // merged state still embodies the v2 op.
    r.set_compaction_threshold(1);
    let writer = bind_room(&mut r, 1, UP, 2);
    write(&mut r, writer, 1, b"years");

    // A v1 joiner below the breaking rename is still refused: the op-version
    // high-water tracks the merged state, so an emptied log does not disarm the
    // gate and admit a joiner the snapshot would then serve un-reachable content.
    let old = hello(&mut r, 2, UP, 1);
    let replies = subscribe_reply(&mut r, old);
    assert!(
        is_update_required(&replies),
        "the high-water outlives the compacted log"
    );
    assert!(!is_subscribed(&replies));
}

#[test]
fn a_reachable_joiner_is_admitted_after_compaction() {
    let mut r = registry();
    r.set_compaction_threshold(1);
    // An all-v1 room whose single op is compacted into the snapshot.
    let v1_writer = bind_room(&mut r, 1, UP, 1);
    write(&mut r, v1_writer, 1, b"age");

    // A transient v2 peer lifts the sticky floor to v2, then leaves — the snapshot
    // still embodies only v1 content.
    let transient = hello(&mut r, 2, UP, 2);
    assert!(is_subscribed(&subscribe_reply(&mut r, transient)));
    r.disconnect(transient);

    // A returning v1 client is admitted and served the below-floor snapshot: the
    // high-water is v1, which it reaches, and the snapshot is sourced at v1.
    let returning = hello(&mut r, 3, UP, 1);
    let replies = subscribe_reply(&mut r, returning);
    assert!(!is_update_required(&replies));
    assert!(
        replies
            .iter()
            .any(|m| matches!(m, Message::Snapshot { .. })),
        "below the compaction floor it is served a snapshot"
    );
}

#[test]
fn an_empty_log_is_never_refused_on_the_op_version_basis() {
    let mut r = registry();
    // UP governs the room at v2 across a breaking rename, but nothing was ever
    // written — the log holds no versioned op for a joiner to reach.
    bind_room(&mut r, 1, UP, 2);

    let old = hello(&mut r, 2, UP, 1);
    let replies = subscribe_reply(&mut r, old);
    assert!(
        !is_update_required(&replies),
        "an empty log has no op version to reach"
    );
    assert!(is_subscribed(&replies));
}

#[test]
#[cfg_attr(miri, ignore)] // drives the durable store on the filesystem
fn a_below_gap_joiner_is_refused_after_a_compacted_restart() {
    let tmp = tempdir();
    // A v2 UP writer binds the room and writes a real v2 op, then compaction folds
    // the whole log into the snapshot — the on-disk log no longer carries the op
    // that set the high-water. The binding and the high-water persist beside it.
    {
        let mut r = store_registry(tmp.path());
        r.set_compaction_threshold(1);
        let writer = bind_room(&mut r, 1, UP, 2);
        write(&mut r, writer, 1, b"years");
    }

    // A fresh node restores the durable binding and high-water, so a v1 joiner
    // below the breaking rename is still refused — the compacted restart neither
    // unbinds the room (which would serve it verbatim) nor under-counts the
    // high-water to None (which would disarm the gate).
    let mut r = store_registry(tmp.path());
    let old = hello(&mut r, 2, UP, 1);
    let replies = subscribe_reply(&mut r, old);
    assert!(
        is_update_required(&replies),
        "the durable high-water outlives a compacted restart"
    );
    assert!(!is_subscribed(&replies));
}

#[test]
#[cfg_attr(miri, ignore)] // drives the durable store on the filesystem
fn a_reachable_joiner_is_admitted_after_a_restart() {
    let tmp = tempdir();
    // An all-v1 room, restarted: a returning v1 joiner reaches the restored v1
    // high-water and is served, so the durable metadata never over-refuses.
    {
        let mut r = store_registry(tmp.path());
        let writer = bind_room(&mut r, 1, UP, 1);
        write(&mut r, writer, 1, b"age");
    }
    let mut r = store_registry(tmp.path());
    let returning = hello(&mut r, 2, UP, 1);
    let replies = subscribe_reply(&mut r, returning);
    assert!(!is_update_required(&replies));
    assert!(is_subscribed(&replies));
}

#[test]
fn a_relay_client_is_not_range_checked() {
    let mut r = registry();
    let writer = hello(&mut r, 1, b"", 0);
    let replies = subscribe_reply(&mut r, writer);
    assert!(is_subscribed(&replies));

    let peer = hello(&mut r, 2, b"", 0);
    let replies = subscribe_reply(&mut r, peer);
    assert!(!is_update_required(&replies));
    assert!(is_subscribed(&replies));
}
