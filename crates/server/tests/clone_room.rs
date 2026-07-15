//! Clone-room over the wire — the server exposes [`Hub::clone_room`] to clients.
//!
//! A client sends a room-keyed [`Message::CloneRoom`] naming a source and a fresh
//! destination; the server duplicates the source's live state into the
//! destination and replies with a [`Message::CloneRoomResult`] carrying whether
//! it was created. The clone is create-only: an unknown source or an existing
//! destination is a no-op (`created == false`), never an error. The gate composes
//! read on the source with the branch-management write tier on the destination; a
//! request before auth is a protocol violation, a denied one a recoverable
//! forbidden. Origin and clone take edits independently — the #196 independence
//! property, now driven over the wire.

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Element, ErrorCode, Message, Op, Scalar};
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

const SRC: &[u8] = b"template";
const DST: &[u8] = b"copy";
const CH: Channel = Channel(0);

/// Drive a connection through Hello + Auth, subscribing `room` on `CH`.
fn joined(r: &mut Registry, client: u8, room: &[u8]) -> ConnId {
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
            room: room.to_vec(),
            zone: Vec::new(),
            last_seen_seq: 0,
            branch: Vec::new(),
        }
    ));
    r.take_outbox(id);
    id
}

/// An authenticated connection holding no subscription.
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

/// The register-write ops from `author`, advancing its Lamport clock so repeated
/// writes to one room carry distinct op ids rather than deduping away.
fn reg(author: &mut Document, value: i64) -> Vec<Op> {
    author.transact(|tx| tx.register(b"age", Scalar::Int(value)))
}

/// Ingest a register-write authored by `author` through `channel`.
fn write_age(r: &mut Registry, id: ConnId, channel: Channel, author: &mut Document, value: i64) {
    let ops = reg(author, value);
    assert!(r.deliver(id, Message::Ops { channel, ops }));
    r.take_outbox(id);
}

fn clone_outcome(m: &Message) -> (Vec<u8>, bool) {
    match m {
        Message::CloneRoomResult { dst, created } => (dst.clone(), *created),
        other => panic!("expected a clone result, got {other:?}"),
    }
}

/// Deliver a clone request from `id` and return the single reply.
fn clone(r: &mut Registry, id: ConnId, src: &[u8], dst: &[u8]) -> Message {
    assert!(r.deliver(
        id,
        Message::CloneRoom {
            src: src.to_vec(),
            dst: dst.to_vec(),
        }
    ));
    r.take_outbox(id).into_iter().next().expect("a reply")
}

/// The `age` register in `room`'s merged state on the hub.
fn age(r: &Registry, room: &[u8]) -> i64 {
    match r.hub().get(room, b"age") {
        Some(Element::Register(reg)) => match reg.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected an int, got {other:?}"),
        },
        _ => panic!("expected the age register in {room:?}"),
    }
}

/// The `age` register a fresh subscriber to `room` is served over the wire,
/// whether the catch-up arrives as a snapshot or an op delta.
fn age_over_the_wire(r: &mut Registry, room: &[u8]) -> i64 {
    let id = authed(r, 9);
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: CH,
            room: room.to_vec(),
            zone: Vec::new(),
            last_seen_seq: 0,
            branch: Vec::new(),
        }
    ));
    let reply = r
        .take_outbox(id)
        .into_iter()
        .next()
        .expect("a catch-up reply");
    let state = match reply {
        Message::Snapshot { state, .. } => Document::decode_state(&state).unwrap(),
        Message::Ops { ops, .. } => {
            let mut d = doc(8);
            for op in &ops {
                d.apply(op);
            }
            d
        }
        other => panic!("expected a catch-up, got {other:?}"),
    };
    match state.get(b"age") {
        Some(Element::Register(reg)) => match reg.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected an int, got {other:?}"),
        },
        _ => panic!("expected the age register served for {room:?}"),
    }
}

#[test]
fn a_clone_creates_the_destination_with_the_sources_state() {
    let mut r = registry();
    let id = joined(&mut r, 1, SRC);
    write_age(&mut r, id, CH, &mut doc(1), 30);

    let (dst, created) = clone_outcome(&clone(&mut r, id, SRC, DST));
    assert_eq!(dst, DST);
    assert!(created, "a populated source cloned into a fresh dst");
    assert_eq!(age(&r, DST), 30, "the clone carries the source's state");
    assert_eq!(
        age_over_the_wire(&mut r, DST),
        30,
        "a subscriber to the clone is served the cloned content"
    );
}

#[test]
fn a_clone_of_an_unknown_source_is_a_no_op() {
    let mut r = registry();
    let id = joined(&mut r, 1, SRC);
    write_age(&mut r, id, CH, &mut doc(1), 30);

    let (dst, created) = clone_outcome(&clone(&mut r, id, b"ghost", DST));
    assert_eq!(dst, DST);
    assert!(!created, "an unknown source clones nothing");
    assert!(r.hub().get(DST, b"age").is_none(), "no dst was minted");
}

#[test]
fn a_clone_into_an_existing_destination_is_a_no_op() {
    let mut r = registry();
    let id = joined(&mut r, 1, SRC);
    write_age(&mut r, id, CH, &mut doc(1), 30);
    // Populate the destination room too, so it already exists.
    let id2 = joined(&mut r, 2, DST);
    write_age(&mut r, id2, CH, &mut doc(2), 99);

    let (_, created) = clone_outcome(&clone(&mut r, id, SRC, DST));
    assert!(!created, "an existing destination is not clobbered");
    assert_eq!(age(&r, DST), 99, "the destination's own state is intact");
}

#[test]
fn origin_and_clone_diverge_independently() {
    let mut r = registry();
    let mut src_author = doc(1);
    let src = joined(&mut r, 1, SRC);
    write_age(&mut r, src, CH, &mut src_author, 30);
    assert!(clone_outcome(&clone(&mut r, src, SRC, DST)).1);

    // Write to the origin; the clone is unchanged.
    write_age(&mut r, src, CH, &mut src_author, 31);
    assert_eq!(age(&r, SRC), 31);
    assert_eq!(age(&r, DST), 30, "an origin write does not touch the clone");

    // Write to the clone; the origin is unchanged.
    let dst = joined(&mut r, 2, DST);
    write_age(&mut r, dst, CH, &mut doc(2), 40);
    assert_eq!(age(&r, DST), 40);
    assert_eq!(age(&r, SRC), 31, "a clone write does not touch the origin");
}

#[test]
fn a_reader_without_read_on_the_source_is_forbidden() {
    let mut r = registry();
    // Seed the source before locking reads down.
    let id = joined(&mut r, 1, SRC);
    write_age(&mut r, id, CH, &mut doc(1), 30);

    // Reads denied everywhere; the clone must refuse rather than copy state the
    // actor cannot see.
    r.set_authorizer(Box::new(
        |_id: &Identity, action: Action, _res: &Resource| action != Action::Read,
    ));
    let actor = authed(&mut r, 2);
    let keep_open = r.deliver(
        actor,
        Message::CloneRoom {
            src: SRC.to_vec(),
            dst: DST.to_vec(),
        },
    );
    assert!(keep_open, "a denial keeps the connection open");
    assert!(matches!(
        r.take_outbox(actor)[0],
        Message::Error {
            code: ErrorCode::Forbidden,
            ..
        }
    ));
    assert!(
        r.hub().get(DST, b"age").is_none(),
        "the denied clone minted nothing"
    );
}

#[test]
fn a_clone_request_before_auth_is_a_violation() {
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
        Message::CloneRoom {
            src: SRC.to_vec(),
            dst: DST.to_vec(),
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
