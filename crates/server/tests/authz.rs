//! Authorization — the server enforces a pluggable policy at every room-level
//! access point.
//!
//! An authenticated actor may read (subscribe), write (ops), or publish
//! awareness to a room only if the deployment's [`Authorizer`] permits it. A
//! denial is a well-formed refusal — an `ErrorCode::Forbidden` reply that leaves
//! the connection open — not a protocol violation. The dev-mode `PermitAll`
//! default allows everything.

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Op, Scalar};
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

fn hello_auth(r: &mut Registry, client: u8) -> ConnId {
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

fn sub(channel: u32, room: &[u8]) -> Message {
    Message::Subscribe {
        channel: Channel(channel),
        room: room.to_vec(),
        last_seen_seq: 0,
    }
}

fn is_forbidden(m: &Message) -> bool {
    matches!(
        m,
        Message::Error {
            code: ErrorCode::Forbidden,
            ..
        }
    )
}

fn sample_ops() -> Vec<Op> {
    doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)))
}

#[test]
fn a_read_denied_room_is_forbidden_but_keeps_the_connection() {
    let mut r = registry();
    // Reads allowed only on "open".
    r.set_authorizer(Box::new(|_actor: &[u8], action: Action, res: &Resource| {
        let Resource::Room(room) = res else {
            return false;
        };
        action != Action::Read || room == b"open"
    }));
    let a = hello_auth(&mut r, 1);

    // The permitted room subscribes normally.
    assert!(r.deliver(a, sub(0, b"open")));
    assert!(matches!(r.take_outbox(a).as_slice(), [Message::Ops { .. }]));

    // The denied room is refused, connection intact.
    let keep_open = r.deliver(a, sub(1, b"secret"));
    assert!(keep_open, "a denial keeps the connection open");
    assert!(is_forbidden(&r.take_outbox(a)[0]));
}

#[test]
fn a_write_denied_room_rejects_ops_and_does_not_ingest() {
    let mut r = registry();
    // Writes denied everywhere; reads allowed.
    r.set_authorizer(Box::new(
        |_actor: &[u8], action: Action, _res: &Resource| action != Action::Write,
    ));
    let a = hello_auth(&mut r, 1);
    assert!(r.deliver(a, sub(0, b"room-a")));
    r.take_outbox(a);

    let keep_open = r.deliver(
        a,
        Message::Ops {
            channel: Channel(0),
            ops: sample_ops(),
        },
    );
    assert!(keep_open);
    assert!(is_forbidden(&r.take_outbox(a)[0]));
    assert_eq!(r.hub().seq(b"room-a"), 0, "a denied write never ingests");
}

#[test]
fn a_denied_write_does_not_reach_peers() {
    let mut r = registry();
    r.set_authorizer(Box::new(
        |_actor: &[u8], action: Action, _res: &Resource| action != Action::Write,
    ));
    let a = hello_auth(&mut r, 1);
    let b = hello_auth(&mut r, 2);
    assert!(r.deliver(a, sub(0, b"room-a")));
    assert!(r.deliver(b, sub(0, b"room-a")));
    r.take_outbox(a);
    r.take_outbox(b);

    r.deliver(
        a,
        Message::Ops {
            channel: Channel(0),
            ops: sample_ops(),
        },
    );
    assert!(
        r.take_outbox(b).is_empty(),
        "peer sees nothing from a denied write"
    );
}

#[test]
fn awareness_publish_can_be_denied() {
    let mut r = registry();
    r.set_authorizer(Box::new(
        |_actor: &[u8], action: Action, _res: &Resource| action != Action::PublishAwareness,
    ));
    let a = hello_auth(&mut r, 1);
    assert!(r.deliver(a, sub(0, b"room-a")));
    r.take_outbox(a);

    let keep_open = r.deliver(
        a,
        Message::AwarenessSet {
            channel: Channel(0),
            key: b"cursor".to_vec(),
            value: vec![1],
        },
    );
    assert!(keep_open);
    assert!(is_forbidden(&r.take_outbox(a)[0]));
}

#[test]
fn the_default_authorizer_permits_everything() {
    let mut r = registry();
    let a = hello_auth(&mut r, 1);
    assert!(r.deliver(a, sub(0, b"room-a")));
    r.take_outbox(a);
    assert!(r.deliver(
        a,
        Message::Ops {
            channel: Channel(0),
            ops: sample_ops(),
        }
    ));
    assert_eq!(r.hub().seq(b"room-a"), 1);
}
