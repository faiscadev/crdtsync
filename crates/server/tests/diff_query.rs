//! The diff-query client-wire frame — the server computes a room's structural
//! diff and returns it to the client.
//!
//! A client asks for the diff between two of a room's saved versions or two of
//! its branches; the server routes the query to the matching `Hub::diff_*` seam,
//! encodes the change list into a `DiffResult`, and maps an absent version/branch
//! to a recoverable `NotFound`. The query is channel-keyed: the room, and the scope
//! the change list is narrowed to, come from the subscription the channel holds
//! (`diff_projection` pins the narrowing). Gated by the same read tier a version
//! request uses — a diff request before auth is a protocol violation, one on an
//! unbound channel likewise, a denied one a recoverable forbidden, never a panic.

use crdtsync_core::diff::{decode_changes, Change};
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::DiffKind;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Scalar};
use crdtsync_server::{Action, ConnId, DiffError, Identity, Registry, Resource};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn registry() -> Registry {
    Registry::new(cid(0xFF))
}

const ROOM: &[u8] = b"room-a";
const CH: crdtsync_core::protocol::Channel = crdtsync_core::protocol::Channel(0);

/// Drive a connection through Hello + Auth, subscribing `ROOM` on `CH`.
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
            credential: format!("actor-{client}").into_bytes(),
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

/// Ingest a register-write through the connection so the room advances. The
/// author document persists across calls, so its lamport clock advances and a
/// later write supersedes an earlier one.
fn write_age(r: &mut Registry, id: ConnId, main: &mut Document, value: i64) {
    let ops = main.transact(|tx| tx.register(b"age", Scalar::Int(value)));
    assert!(r.deliver(id, Message::Ops { channel: CH, ops }));
    r.take_outbox(id);
}

/// The decoded change list carried by a `DiffResult` reply, or a panic if the
/// reply is not a `DiffResult`.
fn result_changes(m: &Message) -> Vec<Change> {
    match m {
        Message::DiffResult { changes, .. } => decode_changes(changes).expect("decodes"),
        other => panic!("expected a diff result, got {other:?}"),
    }
}

/// The identity redaction, for the hub-side oracle: this suite's rooms declare no
/// zones and hold no doc-ACL state, so the served diff and the unnarrowed one agree
/// (`diff_projection` is where they are made to differ).
fn unnarrowed(state: Vec<u8>) -> Result<Vec<u8>, DiffError> {
    Ok(state)
}

fn query(r: &mut Registry, id: ConnId, kind: DiffKind, a: &[u8], b: &[u8]) -> Vec<Message> {
    assert!(r.deliver(
        id,
        Message::DiffQuery {
            channel: CH,
            kind,
            a: a.to_vec(),
            b: b.to_vec(),
        }
    ));
    r.take_outbox(id)
}

#[test]
fn a_version_diff_returns_what_the_hub_computes() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    let mut main = doc(1);
    write_age(&mut r, id, &mut main, 30);
    assert!(r.hub_mut().create_version(ROOM, b"v1").unwrap());
    write_age(&mut r, id, &mut main, 40);
    assert!(r.hub_mut().create_version(ROOM, b"v2").unwrap());

    let expected = r
        .hub_mut()
        .diff_versions(ROOM, b"v1", b"v2", unnarrowed)
        .unwrap();
    let out = query(&mut r, id, DiffKind::Versions, b"v1", b"v2");
    assert_eq!(result_changes(&out[0]), expected);
    assert_eq!(
        result_changes(&out[0]),
        vec![Change::Value {
            path: encode_path(&[b"age"]),
            old: Scalar::Int(30),
            new: Scalar::Int(40),
        }]
    );
}

#[test]
fn a_branch_diff_returns_what_the_hub_computes() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    let mut main = doc(1);
    write_age(&mut r, id, &mut main, 30);
    let fork = r.hub().seq(ROOM);
    assert!(r
        .hub_mut()
        .fork_branch(ROOM, b"draft", b"main", fork)
        .unwrap());
    let ops = doc(2).transact(|tx| tx.register(b"age", Scalar::Int(99)));
    r.hub_mut()
        .ingest_branch(ROOM, b"draft", ops, None)
        .unwrap();

    let expected = r
        .hub_mut()
        .diff_branches(ROOM, b"main", b"draft", unnarrowed)
        .unwrap();
    let out = query(&mut r, id, DiffKind::Branches, b"main", b"draft");
    assert_eq!(result_changes(&out[0]), expected);
    assert_eq!(
        result_changes(&out[0]),
        vec![Change::Value {
            path: encode_path(&[b"age"]),
            old: Scalar::Int(30),
            new: Scalar::Int(99),
        }]
    );
}

#[test]
fn a_self_diff_returns_an_empty_change_list() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    let mut main = doc(1);
    write_age(&mut r, id, &mut main, 30);
    assert!(r.hub_mut().create_version(ROOM, b"v1").unwrap());

    let out = query(&mut r, id, DiffKind::Versions, b"v1", b"v1");
    assert!(result_changes(&out[0]).is_empty(), "a self-diff is empty");
}

#[test]
fn an_unknown_version_is_a_recoverable_not_found() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    let mut main = doc(1);
    write_age(&mut r, id, &mut main, 30);
    assert!(r.hub_mut().create_version(ROOM, b"v1").unwrap());

    let keep_open = r.deliver(
        id,
        Message::DiffQuery {
            channel: CH,
            kind: DiffKind::Versions,
            a: b"v1".to_vec(),
            b: b"ghost".to_vec(),
        },
    );
    assert!(keep_open, "a not-found keeps the connection open");
    assert!(matches!(
        r.take_outbox(id)[0],
        Message::Error {
            code: ErrorCode::NotFound,
            ..
        }
    ));
}

#[test]
fn an_unknown_branch_is_a_recoverable_not_found() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    let mut main = doc(1);
    write_age(&mut r, id, &mut main, 30);

    let keep_open = r.deliver(
        id,
        Message::DiffQuery {
            channel: CH,
            kind: DiffKind::Branches,
            a: b"main".to_vec(),
            b: b"ghost".to_vec(),
        },
    );
    assert!(keep_open, "a not-found keeps the connection open");
    assert!(matches!(
        r.take_outbox(id)[0],
        Message::Error {
            code: ErrorCode::NotFound,
            ..
        }
    ));
}

#[test]
fn a_read_denied_actor_cannot_diff() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    // The subscription stands; the read verdict behind it is withdrawn. The gate is
    // re-asked per request, so the bound channel no longer carries a diff.
    r.set_authorizer(Box::new(
        |_id: &Identity, action: Action, _res: &Resource| action != Action::Read,
    ));

    let keep_open = r.deliver(
        id,
        Message::DiffQuery {
            channel: CH,
            kind: DiffKind::Versions,
            a: b"v1".to_vec(),
            b: b"v2".to_vec(),
        },
    );
    assert!(keep_open, "a denial keeps the connection open");
    assert!(matches!(
        r.take_outbox(id)[0],
        Message::Error {
            code: ErrorCode::Forbidden,
            ..
        }
    ));
}

#[test]
fn a_diff_on_an_unbound_channel_is_a_violation() {
    // The channel is what names the room and carries the scope the change list is
    // narrowed to, so a query naming no subscription has nothing to answer — and an
    // authenticated actor asking on a channel it never bound is a client bug, not a
    // denial.
    let mut r = registry();
    let id = joined(&mut r, 1);

    let keep_open = r.deliver(
        id,
        Message::DiffQuery {
            channel: crdtsync_core::protocol::Channel(7),
            kind: DiffKind::Versions,
            a: b"v1".to_vec(),
            b: b"v2".to_vec(),
        },
    );
    assert!(!keep_open, "a violation closes the connection");
    assert!(matches!(
        r.take_outbox(id)[0],
        Message::Error {
            code: ErrorCode::ProtocolViolation,
            ..
        }
    ));
}

#[test]
fn a_diff_request_before_auth_is_a_violation() {
    let mut r = registry();
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
        }
    ));
    r.take_outbox(id);

    let keep_open = r.deliver(
        id,
        Message::DiffQuery {
            channel: CH,
            kind: DiffKind::Versions,
            a: b"v1".to_vec(),
            b: b"v2".to_vec(),
        },
    );
    assert!(!keep_open, "a violation closes the connection");
    assert!(matches!(
        r.take_outbox(id)[0],
        Message::Error {
            code: ErrorCode::ProtocolViolation,
            ..
        }
    ));
}
