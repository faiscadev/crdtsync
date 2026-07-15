//! Branch management over the wire — the server handles the room-keyed
//! request/response sub-protocol.
//!
//! A client lists, forks, forks-from-version, restores, publishes, or deletes a
//! branch of a room. Every mutation and every list request is answered with the
//! room's current branch set (the authoritative post-state). Mutations require
//! write authorization, reads require read; a request before auth is a protocol
//! violation, a denied one a recoverable forbidden.

use crdtsync_core::protocol::{BranchInfo, Channel};
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Scalar};
use crdtsync_server::{Action, ConnId, Identity, Registry, Resource};

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
const CH: Channel = Channel(0);

/// Drive a connection through Hello + Auth, subscribing `room` on `CH`.
fn joined(r: &mut Registry, client: u8) -> ConnId {
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
    id
}

/// Ingest a register-write through the connection so the room has state.
fn write_age(r: &mut Registry, id: ConnId, channel: Channel, value: i64) {
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(value)));
    assert!(r.deliver(id, Message::Ops { channel, ops }));
    r.take_outbox(id);
}

fn branches(m: &Message) -> Vec<BranchInfo> {
    match m {
        Message::Branches { branches, .. } => branches.clone(),
        other => panic!("expected a branch set, got {other:?}"),
    }
}

fn names(m: &Message) -> Vec<Vec<u8>> {
    branches(m).into_iter().map(|b| b.name).collect()
}

#[test]
fn list_returns_the_rooms_branches() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    write_age(&mut r, id, CH, 30);

    assert!(r.deliver(
        id,
        Message::BranchList {
            room: ROOM.to_vec(),
        }
    ));
    assert_eq!(names(&r.take_outbox(id)[0]), vec![b"main".to_vec()]);
}

#[test]
fn fork_creates_a_branch_a_subscribe_can_write() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    write_age(&mut r, id, CH, 30);

    assert!(r.deliver(
        id,
        Message::BranchFork {
            room: ROOM.to_vec(),
            name: b"feature".to_vec(),
            from_branch: b"main".to_vec(),
        }
    ));
    assert_eq!(
        names(&r.take_outbox(id)[0]),
        vec![b"feature".to_vec(), b"main".to_vec()]
    );

    // The forked branch is subscribable and writable on its own channel.
    let ch2 = Channel(1);
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: ch2,
            room: ROOM.to_vec(),
            zone: Vec::new(),
            last_seen_seq: 0,
            branch: b"feature".to_vec(),
        }
    ));
    r.take_outbox(id);
    write_age(&mut r, id, ch2, 99);
    let feature = r
        .hub()
        .branches(ROOM)
        .into_iter()
        .find(|b| b.name == b"feature")
        .expect("feature branch present");
    assert!(feature.head > feature.fork_point, "the write advanced HEAD");
}

#[test]
fn publish_points_the_published_branch() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    write_age(&mut r, id, CH, 30);

    assert!(r.deliver(
        id,
        Message::BranchPublish {
            room: ROOM.to_vec(),
            published: b"live".to_vec(),
        }
    ));
    let set = branches(&r.take_outbox(id)[0]);
    let live = set
        .iter()
        .find(|b| b.name == b"live")
        .expect("published branch present");
    assert!(live.published, "the target is a read-only publish branch");
    assert!(r.hub().is_published(ROOM, b"live"));
}

#[test]
fn fork_from_version_creates_a_branch() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    write_age(&mut r, id, CH, 30);
    assert!(r.hub_mut().create_version(ROOM, b"v1").unwrap());

    assert!(r.deliver(
        id,
        Message::BranchForkFromVersion {
            room: ROOM.to_vec(),
            name: b"from-v1".to_vec(),
            version: b"v1".to_vec(),
        }
    ));
    assert!(names(&r.take_outbox(id)[0]).contains(&b"from-v1".to_vec()));
}

#[test]
fn restore_as_branch_switches_the_active_head() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    write_age(&mut r, id, CH, 30);
    assert!(r.hub_mut().create_version(ROOM, b"v1").unwrap());

    assert!(r.deliver(
        id,
        Message::BranchRestore {
            room: ROOM.to_vec(),
            name: b"restored".to_vec(),
            version: b"v1".to_vec(),
        }
    ));
    assert!(names(&r.take_outbox(id)[0]).contains(&b"restored".to_vec()));
    assert_eq!(r.hub().active_branch(ROOM), b"restored".to_vec());
}

#[test]
fn delete_removes_a_branch() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    write_age(&mut r, id, CH, 30);
    r.deliver(
        id,
        Message::BranchFork {
            room: ROOM.to_vec(),
            name: b"feature".to_vec(),
            from_branch: b"main".to_vec(),
        },
    );
    r.take_outbox(id);

    assert!(r.deliver(
        id,
        Message::BranchDelete {
            room: ROOM.to_vec(),
            name: b"feature".to_vec(),
        }
    ));
    assert_eq!(names(&r.take_outbox(id)[0]), vec![b"main".to_vec()]);
}

#[test]
fn a_write_denied_actor_cannot_fork_a_branch() {
    let mut r = registry();
    // Writes denied everywhere; reads (subscribe) allowed.
    r.set_authorizer(Box::new(
        |_id: &Identity, action: Action, _res: &Resource| action != Action::Write,
    ));
    let id = joined(&mut r, 1);

    let keep_open = r.deliver(
        id,
        Message::BranchFork {
            room: ROOM.to_vec(),
            name: b"feature".to_vec(),
            from_branch: b"main".to_vec(),
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
    assert_eq!(
        r.hub().branches(ROOM).len(),
        1,
        "the denied fork left only main"
    );
}

#[test]
fn a_branch_request_before_auth_is_a_violation() {
    let mut r = registry();
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
        }
    ));
    r.take_outbox(id);

    let keep_open = r.deliver(
        id,
        Message::BranchList {
            room: ROOM.to_vec(),
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
fn a_fork_of_a_missing_source_is_a_no_op_reply() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    write_age(&mut r, id, CH, 30);

    assert!(r.deliver(
        id,
        Message::BranchFork {
            room: ROOM.to_vec(),
            name: b"orphan".to_vec(),
            from_branch: b"ghost".to_vec(),
        }
    ));
    // Forking off an absent source changes nothing — the reply is still the
    // authoritative set, holding only main.
    assert_eq!(names(&r.take_outbox(id)[0]), vec![b"main".to_vec()]);
}
