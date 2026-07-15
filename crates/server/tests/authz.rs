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
use crdtsync_server::{Action, ConnId, Identity, ManualClock, Registry, Resource};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    // An awareness set reads the clock to stamp last-seen; the default SystemClock
    // is not readable under Miri isolation, so drive a fixed manual clock.
    r.set_clock(std::sync::Arc::new(ManualClock::new(0)));
    r
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
        zone: Vec::new(),
        last_seen_seq: 0,
        branch: Vec::new(),
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

/// A refusal of authored ops naming `reason` — the non-fatal reply on a denied
/// write, distinct from a connection-closing `Error`.
fn is_ops_rejected(m: &Message, reason: ErrorCode) -> bool {
    matches!(m, Message::OpsRejected { reason: r, .. } if *r == reason)
}

fn sample_ops() -> Vec<Op> {
    doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)))
}

#[test]
fn a_read_denied_room_is_forbidden_but_keeps_the_connection() {
    let mut r = registry();
    // Reads allowed only on "open".
    r.set_authorizer(Box::new(
        |_id: &Identity, action: Action, res: &Resource| {
            let Resource::Room(room) = res else {
                return false;
            };
            action != Action::Read || room == b"open"
        },
    ));
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
        |_id: &Identity, action: Action, _res: &Resource| action != Action::Write,
    ));
    let a = hello_auth(&mut r, 1);
    assert!(r.deliver(a, sub(0, b"room-a")));
    r.take_outbox(a);

    let ops = sample_ops();
    let keep_open = r.deliver(
        a,
        Message::Ops {
            channel: Channel(0),
            ops: ops.clone(),
        },
    );
    // A denied write is refused with a non-fatal OpsRejected naming the ops and
    // the reason — not a connection close.
    assert!(keep_open, "an ops rejection keeps the connection open");
    let out = r.take_outbox(a);
    assert!(is_ops_rejected(&out[0], ErrorCode::Forbidden));
    match &out[0] {
        Message::OpsRejected { channel, seqs, .. } => {
            assert_eq!(*channel, Channel(0));
            assert_eq!(seqs, &ops.iter().map(|o| o.id.seq).collect::<Vec<_>>());
        }
        other => panic!("expected OpsRejected, got {other:?}"),
    }
    assert_eq!(r.hub().seq(b"room-a"), 0, "a denied write never ingests");
}

#[test]
fn a_rejected_write_leaves_the_session_live() {
    let mut r = registry();
    // Writes denied; reads allowed, so a following subscribe still lands.
    r.set_authorizer(Box::new(
        |_id: &Identity, action: Action, _res: &Resource| action != Action::Write,
    ));
    let a = hello_auth(&mut r, 1);
    assert!(r.deliver(a, sub(0, b"room-a")));
    r.take_outbox(a);

    r.deliver(
        a,
        Message::Ops {
            channel: Channel(0),
            ops: sample_ops(),
        },
    );
    assert!(is_ops_rejected(&r.take_outbox(a)[0], ErrorCode::Forbidden));

    // The connection is intact — a following legal frame from the same session
    // still processes.
    assert!(r.deliver(a, sub(1, b"room-b")));
    assert!(matches!(r.take_outbox(a).as_slice(), [Message::Ops { .. }]));
}

#[test]
fn a_permitted_write_still_ingests_and_fans_out() {
    let mut r = registry();
    let a = hello_auth(&mut r, 1);
    let b = hello_auth(&mut r, 2);
    assert!(r.deliver(a, sub(0, b"room-a")));
    assert!(r.deliver(b, sub(0, b"room-a")));
    r.take_outbox(a);
    r.take_outbox(b);

    assert!(r.deliver(
        a,
        Message::Ops {
            channel: Channel(0),
            ops: sample_ops(),
        }
    ));
    // The author is acknowledged, the peer sees the fan-out, and the hub ingests.
    assert!(matches!(
        r.take_outbox(a).as_slice(),
        [Message::Accepted { .. }]
    ));
    assert!(matches!(r.take_outbox(b).as_slice(), [Message::Ops { .. }]));
    assert_eq!(r.hub().seq(b"room-a"), 1);
}

#[test]
fn a_denied_write_does_not_reach_peers() {
    let mut r = registry();
    r.set_authorizer(Box::new(
        |_id: &Identity, action: Action, _res: &Resource| action != Action::Write,
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
        |_id: &Identity, action: Action, _res: &Resource| action != Action::PublishAwareness,
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
