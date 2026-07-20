// Real filesystem I/O and loopback sockets, which Miri does not model.
#![cfg(not(miri))]

//! The audit trail — an append-only structured file-log of security-relevant
//! events, and the read-only operator query surface over it.
//!
//! The store persists each auditable event (a connect, an export, a version-read,
//! an ACL decision that denied, a write) as one immutable, time-ordered record; the
//! append is durable and the query path never mutates it. The operator surface is an
//! admin-HTTP endpoint gated by the same verifier + authorizer as the schema-
//! registration plane — the trail is never exposed to an app client — that filters
//! by actor / action / room / time-range and returns the matching records.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::http::Request;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Op, Scalar};
use crdtsync_server::{
    audit_router, Action, AuditDecision, AuditLog, AuditQuery, AuditResource, Audited, Authorizer,
    ConnId, DurableAccessLog, Identity, ManualClock, PermitAll, Registry, Resource, StaticTokens,
    AUDIT_APP,
};
use tower::ServiceExt;

// --- fixtures -------------------------------------------------------------

/// A process- and test-unique directory, removed on drop.
struct TempDir(std::path::PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tempdir() -> TempDir {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("crdtsync-audit-{pid}-{n}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}

/// A fresh audit log over a temp file, plus the manual clock stamping its records
/// (held so a test can advance time) and the temp dir guard (held so the file
/// outlives the test).
fn new_log() -> (Arc<AuditLog>, Arc<ManualClock>, TempDir) {
    let tmp = tempdir();
    let clock = Arc::new(ManualClock::new(1_000));
    let log = Arc::new(AuditLog::open(tmp.0.join("audit.log"), clock.clone()).unwrap());
    (log, clock, tmp)
}

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-a";
const CH: Channel = Channel(0);

// --- the append-only store: append, replay, query ------------------------

#[test]
fn append_then_read_replays_the_same_records_across_a_reopen() {
    let tmp = tempdir();
    let path = tmp.0.join("audit.log");
    let clock = Arc::new(ManualClock::new(10));

    {
        let log = AuditLog::open(&path, clock.clone()).unwrap();
        log.record(
            b"alice",
            Action::Connect,
            AuditResource::App(Vec::new()),
            AuditDecision::Permitted,
        )
        .unwrap();
        clock.advance(5);
        log.record(
            b"bob",
            Action::Read,
            AuditResource::Room(b"secret".to_vec()),
            AuditDecision::Denied,
        )
        .unwrap();
    }

    // A second handle opened on the same file replays exactly what was appended.
    let reopened = AuditLog::open(&path, clock).unwrap();
    let records = reopened.read_all().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].actor, b"alice");
    assert_eq!(records[0].action, Action::Connect);
    assert_eq!(records[0].timestamp, 10);
    assert_eq!(records[1].actor, b"bob");
    assert_eq!(records[1].action, Action::Read);
    assert_eq!(records[1].decision, AuditDecision::Denied);
    assert_eq!(records[1].resource.room(), Some(b"secret".as_slice()));
    assert_eq!(records[1].timestamp, 15);
    assert!(reopened.healthy());
}

#[test]
fn query_filters_by_actor_action_room_and_time_range() {
    let (log, clock, _tmp) = new_log();
    // t=1000 alice connect, t=1010 bob export, t=1020 alice denied read on room-a.
    log.record(
        b"alice",
        Action::Connect,
        AuditResource::App(Vec::new()),
        AuditDecision::Permitted,
    )
    .unwrap();
    clock.advance(10);
    log.record(
        b"bob",
        Action::Export,
        AuditResource::App(b"blob-1".to_vec()),
        AuditDecision::Permitted,
    )
    .unwrap();
    clock.advance(10);
    log.record(
        b"alice",
        Action::Read,
        AuditResource::Room(ROOM.to_vec()),
        AuditDecision::Denied,
    )
    .unwrap();

    // An empty filter returns every record, in time order.
    let all = log.query(&AuditQuery::default()).unwrap();
    assert_eq!(all.len(), 3);

    // By actor.
    let alice = log
        .query(&AuditQuery {
            actor: Some(b"alice".to_vec()),
            ..AuditQuery::default()
        })
        .unwrap();
    assert_eq!(alice.len(), 2);
    assert!(alice.iter().all(|r| r.actor == b"alice"));

    // By action.
    let exports = log
        .query(&AuditQuery {
            action: Some(Action::Export),
            ..AuditQuery::default()
        })
        .unwrap();
    assert_eq!(exports.len(), 1);
    assert_eq!(exports[0].actor, b"bob");

    // By room (an app-scoped export names no room, so it is excluded).
    let in_room = log
        .query(&AuditQuery {
            room: Some(ROOM.to_vec()),
            ..AuditQuery::default()
        })
        .unwrap();
    assert_eq!(in_room.len(), 1);
    assert_eq!(in_room[0].action, Action::Read);

    // By time range — half-open [since, until): [1010, 1020) is the export alone.
    let window = log
        .query(&AuditQuery {
            since: Some(1_010),
            until: Some(1_020),
            ..AuditQuery::default()
        })
        .unwrap();
    assert_eq!(window.len(), 1);
    assert_eq!(window[0].action, Action::Export);
}

#[test]
fn a_torn_trailing_record_is_dropped_a_complete_undecodable_one_is_an_error() {
    let tmp = tempdir();
    let path = tmp.0.join("audit.log");

    // One good record, then a torn tail (a length prefix that outruns the file).
    {
        let log = AuditLog::open(&path, Arc::new(ManualClock::new(1))).unwrap();
        log.record(
            b"alice",
            Action::Connect,
            AuditResource::App(Vec::new()),
            AuditDecision::Permitted,
        )
        .unwrap();
    }
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        // Claim a 100-byte record, supply only 3 — a crash mid-append.
        f.write_all(&100u32.to_le_bytes()).unwrap();
        f.write_all(&[1, 2, 3]).unwrap();
    }
    let log = AuditLog::open(&path, Arc::new(ManualClock::new(1))).unwrap();
    assert_eq!(log.read_all().unwrap().len(), 1, "the torn tail is dropped");

    // A complete but undecodable record is corruption, surfaced as an error.
    let corrupt = tempdir();
    let cpath = corrupt.0.join("audit.log");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&cpath).unwrap();
        // A full frame of length 1 whose body cannot decode (needs an 8-byte ts).
        f.write_all(&1u32.to_le_bytes()).unwrap();
        f.write_all(&[0xFF]).unwrap();
    }
    let clog = AuditLog::open(&cpath, Arc::new(ManualClock::new(1))).unwrap();
    assert!(
        clog.read_all().is_err(),
        "a complete undecodable record surfaces"
    );
}

// --- wiring: connect / version-read / denied decisions are audited --------

/// Drive a connection through Hello + Auth, returning its id (authenticated as
/// `actor-<client>`).
fn authed(r: &mut Registry, client: u8) -> ConnId {
    let id = r.connect();
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
            credential: format!("actor-{client}").into_bytes(),
        }
    ));
    r.take_outbox(id);
    id
}

#[test]
fn a_connect_and_a_denied_read_are_audited_but_a_permitted_read_is_not() {
    let (log, _clock, _tmp) = new_log();
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    // Only "open" is readable; every other room's read is denied.
    let policy: Box<dyn Authorizer> = Box::new(|_id: &Identity, action: Action, res: &Resource| {
        action == Action::Read && matches!(res, Resource::Room(room) if *room == b"open")
    });
    r.set_authorizer(Box::new(Audited::new(
        policy,
        Box::new(DurableAccessLog::new(log.clone())),
    )));

    let id = authed(&mut r, 1);
    // A permitted read (the "open" subscribe) and a denied one ("secret").
    for (ch, room) in [(0u32, b"open".as_slice()), (1, b"secret".as_slice())] {
        r.deliver(
            id,
            Message::Subscribe {
                channel: Channel(ch),
                room: room.to_vec(),
                zone: Vec::new(),
                last_seen_seq: 0,
                branch: Vec::new(),
            },
        );
        r.take_outbox(id);
    }

    let records = log.read_all().unwrap();
    // The connect and the denied read are recorded; the permitted read is not (no
    // noise) — so exactly two records.
    assert_eq!(records.len(), 2, "got {records:?}");
    assert!(records
        .iter()
        .any(|rec| rec.action == Action::Connect && rec.actor == b"actor-1"));
    assert!(records.iter().any(|rec| {
        rec.action == Action::Read
            && rec.decision == AuditDecision::Denied
            && rec.resource.room() == Some(b"secret".as_slice())
    }));
    assert!(
        !records
            .iter()
            .any(|rec| rec.action == Action::Read && rec.decision == AuditDecision::Permitted),
        "a routine permitted read must not be audited"
    );
}

#[test]
fn a_version_fetch_is_audited_as_a_version_read() {
    let (log, _clock, _tmp) = new_log();
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_authorizer(Box::new(Audited::new(
        Box::new(PermitAll),
        Box::new(DurableAccessLog::new(log.clone())),
    )));

    let id = authed(&mut r, 1);
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            zone: Vec::new(),
            last_seen_seq: 0,
            branch: Vec::new(),
        }
    ));
    r.take_outbox(id);
    // Give the room state, capture a version, then fetch it.
    let ops: Vec<Op> = Document::new(cid(1)).transact(|tx| tx.register(b"age", Scalar::Int(30)));
    r.deliver(id, Message::Ops { channel: CH, ops });
    r.take_outbox(id);
    r.deliver(
        id,
        Message::VersionCreate {
            channel: CH,
            name: b"v1".to_vec(),
        },
    );
    r.take_outbox(id);
    assert!(r.deliver(
        id,
        Message::VersionFetch {
            channel: CH,
            name: b"v1".to_vec(),
        }
    ));

    let records = log.read_all().unwrap();
    assert!(
        records.iter().any(|rec| {
            rec.action == Action::VersionRead
                && rec.actor == b"actor-1"
                && rec.resource.room() == Some(ROOM)
        }),
        "a version fetch records a VersionRead: {records:?}"
    );
}

// --- the operator query surface (admin HTTP), read-only + authorized ------

fn verifier() -> StaticTokens {
    let mut t = StaticTokens::new();
    t.insert(b"op-cred".to_vec(), b"operator".to_vec());
    t.insert(b"user-cred".to_vec(), b"user".to_vec());
    t
}

/// Only the operator may read the audit trail — Read on the reserved `$audit` app.
fn only_operator() -> impl Authorizer + Clone {
    |id: &Identity, action: Action, res: &Resource| {
        action == Action::Read
            && id.actor() == b"operator"
            && matches!(res, Resource::App(a) if *a == AUDIT_APP)
    }
}

/// A log seeded with three records for the query-surface tests.
fn seeded_log() -> (Arc<AuditLog>, TempDir) {
    let (log, clock, tmp) = new_log();
    log.record(
        b"alice",
        Action::Connect,
        AuditResource::App(Vec::new()),
        AuditDecision::Permitted,
    )
    .unwrap();
    clock.advance(10);
    log.record(
        b"bob",
        Action::Export,
        AuditResource::App(b"blob-1".to_vec()),
        AuditDecision::Permitted,
    )
    .unwrap();
    clock.advance(10);
    log.record(
        b"alice",
        Action::Read,
        AuditResource::Room(ROOM.to_vec()),
        AuditDecision::Denied,
    )
    .unwrap();
    (log, tmp)
}

/// Drive one GET request through a fresh audit router over `log`, returning the
/// status and the JSON body (empty for a non-200).
async fn query(log: Arc<AuditLog>, target: &str, cred: Option<&str>) -> (u16, serde_json::Value) {
    let router = audit_router(Box::new(verifier()), Box::new(only_operator()), log);
    let mut builder = Request::builder().method("GET").uri(target);
    if let Some(c) = cred {
        builder = builder.header("authorization", c);
    }
    let response = router
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if status == 200 {
        serde_json::from_slice(&bytes).unwrap()
    } else {
        serde_json::Value::Null
    };
    (status, json)
}

#[tokio::test]
async fn an_operator_reads_the_whole_trail_and_filters_it() {
    let (log, _tmp) = seeded_log();

    // The whole trail.
    let (status, body) = query(log.clone(), "/audit", Some("op-cred")).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().unwrap().len(), 3);

    // Filter by actor.
    let (_, body) = query(log.clone(), "/audit?actor=alice", Some("op-cred")).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert!(arr.iter().all(|r| r["actor"] == "alice"));

    // Filter by action.
    let (_, body) = query(log.clone(), "/audit?action=export", Some("op-cred")).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["actor"], "bob");
    assert_eq!(arr[0]["action"], "export");

    // Filter by time window [1010, 1020): the export alone.
    let (_, body) = query(log.clone(), "/audit?since=1010&until=1020", Some("op-cred")).await;
    assert_eq!(body.as_array().unwrap().len(), 1);

    // Filter by room.
    let (_, body) = query(log.clone(), "/audit?room=room-a", Some("op-cred")).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["decision"], "denied");
}

#[tokio::test]
async fn the_query_is_read_only_and_operator_authorized() {
    let (log, _tmp) = seeded_log();
    let before = log.read_all().unwrap().len();

    // No credential is unauthorized.
    let (status, _) = query(log.clone(), "/audit", None).await;
    assert_eq!(status, 401);

    // An authenticated non-operator is forbidden — the trail is not an app surface.
    let (status, _) = query(log.clone(), "/audit", Some("user-cred")).await;
    assert_eq!(status, 403);

    // A malformed action keyword is a bad request.
    let (status, _) = query(log.clone(), "/audit?action=bogus", Some("op-cred")).await;
    assert_eq!(status, 400);

    // The operator read succeeds — and left the log untouched (append-only).
    let (status, _) = query(log.clone(), "/audit", Some("op-cred")).await;
    assert_eq!(status, 200);
    assert_eq!(
        log.read_all().unwrap().len(),
        before,
        "a query never mutates the log"
    );
}

#[tokio::test]
async fn a_corrupt_trail_surfaces_as_an_error_never_a_clean_partial_read() {
    let tmp = tempdir();
    let path = tmp.0.join("audit.log");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&1u32.to_le_bytes()).unwrap();
        f.write_all(&[0xFF]).unwrap();
    }
    let log = Arc::new(AuditLog::open(&path, Arc::new(ManualClock::new(1))).unwrap());
    let (status, _) = query(log, "/audit", Some("op-cred")).await;
    assert_eq!(
        status, 500,
        "a dropped/corrupt record must surface, not read clean"
    );
}
