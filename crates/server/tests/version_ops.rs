//! Named versions over the wire — the server handles the request/response
//! sub-protocol on a subscribed channel.
//!
//! A client creates, renames, deletes, lists, or fetches a version of the
//! channel's room. Every mutation and every list request is answered with the
//! room's current version names (the authoritative post-state); a fetch that
//! hits is answered with the version's captured state, and one that misses falls
//! back to the name list. Mutations require write authorization, reads require
//! read; a request on an unbound channel is a protocol violation.

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Element, ErrorCode, Message, Op, Scalar};
use crdtsync_server::{Action, ConnId, Registry, Resource};

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

/// Drive a connection through Hello + Auth + Subscribe, holding `room` on `CH`.
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
            last_seen_seq: 0,
        }
    ));
    r.take_outbox(id);
    id
}

/// Ingest a register-write through the connection so the room has state.
fn write_age(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(id, Message::Ops { channel: CH, ops }));
    r.take_outbox(id);
}

fn names(m: &Message) -> Vec<Vec<u8>> {
    match m {
        Message::Versions { names, .. } => names.clone(),
        other => panic!("expected a versions list, got {other:?}"),
    }
}

#[test]
fn create_replies_with_the_new_name_list() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    write_age(
        &mut r,
        id,
        doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30))),
    );

    assert!(r.deliver(
        id,
        Message::VersionCreate {
            channel: CH,
            name: b"v1".to_vec(),
        }
    ));
    assert_eq!(names(&r.take_outbox(id)[0]), vec![b"v1".to_vec()]);
}

#[test]
fn list_returns_the_current_names_sorted() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    write_age(
        &mut r,
        id,
        doc(1).transact(|tx| tx.register(b"age", Scalar::Int(1))),
    );
    for name in [b"v-b".as_slice(), b"v-a".as_slice()] {
        r.deliver(
            id,
            Message::VersionCreate {
                channel: CH,
                name: name.to_vec(),
            },
        );
        r.take_outbox(id);
    }

    assert!(r.deliver(id, Message::VersionList { channel: CH }));
    assert_eq!(
        names(&r.take_outbox(id)[0]),
        vec![b"v-a".to_vec(), b"v-b".to_vec()]
    );
}

#[test]
fn fetch_returns_the_captured_state() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    write_age(
        &mut r,
        id,
        doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30))),
    );
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
    match &r.take_outbox(id)[0] {
        Message::VersionState {
            channel,
            name,
            state,
            ..
        } => {
            assert_eq!(*channel, CH);
            assert_eq!(name, b"v1");
            let restored = Document::decode_state(state).unwrap();
            match restored.get(b"age") {
                Some(Element::Register(reg)) => {
                    assert_eq!(reg.borrow().read(), &Scalar::Int(30))
                }
                _ => panic!("expected the age register in the version state"),
            }
        }
        other => panic!("expected a version state, got {other:?}"),
    }
}

#[test]
fn fetch_of_a_missing_version_falls_back_to_the_list() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    write_age(
        &mut r,
        id,
        doc(1).transact(|tx| tx.register(b"age", Scalar::Int(1))),
    );

    assert!(r.deliver(
        id,
        Message::VersionFetch {
            channel: CH,
            name: b"ghost".to_vec(),
        }
    ));
    assert!(names(&r.take_outbox(id)[0]).is_empty());
}

#[test]
fn rename_and_delete_reflect_in_the_reply() {
    let mut r = registry();
    let id = joined(&mut r, 1);
    write_age(
        &mut r,
        id,
        doc(1).transact(|tx| tx.register(b"age", Scalar::Int(1))),
    );
    r.deliver(
        id,
        Message::VersionCreate {
            channel: CH,
            name: b"draft".to_vec(),
        },
    );
    r.take_outbox(id);

    assert!(r.deliver(
        id,
        Message::VersionRename {
            channel: CH,
            from: b"draft".to_vec(),
            to: b"final".to_vec(),
        }
    ));
    assert_eq!(names(&r.take_outbox(id)[0]), vec![b"final".to_vec()]);

    assert!(r.deliver(
        id,
        Message::VersionDelete {
            channel: CH,
            name: b"final".to_vec(),
        }
    ));
    assert!(names(&r.take_outbox(id)[0]).is_empty());
}

#[test]
fn a_write_denied_actor_cannot_create_a_version() {
    let mut r = registry();
    // Writes denied everywhere; reads (subscribe) allowed.
    r.set_authorizer(Box::new(
        |_actor: &[u8], action: Action, _res: &Resource| action != Action::Write,
    ));
    let id = joined(&mut r, 1);

    let keep_open = r.deliver(
        id,
        Message::VersionCreate {
            channel: CH,
            name: b"v1".to_vec(),
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
    assert!(
        r.hub().version_names(ROOM).is_empty(),
        "the denied create left no version"
    );
}

#[test]
fn a_version_request_on_an_unbound_channel_is_a_violation() {
    let mut r = registry();
    let id = joined(&mut r, 1);

    let keep_open = r.deliver(
        id,
        Message::VersionList {
            channel: Channel(9),
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
