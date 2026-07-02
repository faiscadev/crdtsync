//! Per-recipient read redaction — the server re-checks read on every fan-out,
//! so a peer whose authorization is revoked mid-session stops receiving a room's
//! ops and awareness without needing to resubscribe. The check is room-level
//! today; the same hook narrows to element/zone resources as they land.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Op, Scalar};
use crdtsync_server::{Action, Authorizer, ConnId, Registry, Resource};

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

/// Drive a connection through Hello + Auth so it holds `actor-<client>`.
fn hello_auth(r: &mut Registry, client: u8) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client)
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

fn sample_ops() -> Vec<Op> {
    doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)))
}

/// An authorizer that denies `actor-2` reads once `revoked` is set, permitting
/// everything else — so a peer can subscribe while allowed and lose the room
/// mid-session.
fn revocable() -> (Arc<AtomicBool>, Box<dyn Authorizer>) {
    let revoked = Arc::new(AtomicBool::new(false));
    let flag = revoked.clone();
    let authorizer: Box<dyn Authorizer> =
        Box::new(move |actor: &[u8], action: Action, _res: &Resource| {
            !(action == Action::Read && actor == b"actor-2" && flag.load(Ordering::SeqCst))
        });
    (revoked, authorizer)
}

#[test]
fn read_redaction_is_per_recipient() {
    let (revoked, authorizer) = revocable();
    let mut r = registry();
    r.set_authorizer(authorizer);

    // All three subscribe to the room while permitted.
    let a = hello_auth(&mut r, 1);
    let b = hello_auth(&mut r, 2);
    let c = hello_auth(&mut r, 3);
    for id in [a, b, c] {
        assert!(r.deliver(id, sub(0, b"room-a")));
        r.take_outbox(id);
    }

    // Revoke actor-2's read, then a writes.
    revoked.store(true, Ordering::SeqCst);
    r.deliver(
        a,
        Message::Ops {
            channel: Channel(0),
            ops: sample_ops(),
        },
    );

    assert!(
        r.take_outbox(b).is_empty(),
        "a read-revoked peer receives no ops"
    );
    assert!(
        matches!(r.take_outbox(c).as_slice(), [Message::Ops { .. }]),
        "a still-permitted peer receives the broadcast"
    );
    assert_eq!(
        r.hub().seq(b"room-a"),
        1,
        "the write still ingests for permitted readers"
    );
}

#[test]
fn awareness_fan_out_is_redacted_for_a_revoked_reader() {
    let (revoked, authorizer) = revocable();
    let mut r = registry();
    r.set_authorizer(authorizer);

    let a = hello_auth(&mut r, 1);
    let b = hello_auth(&mut r, 2);
    assert!(r.deliver(a, sub(0, b"room-a")));
    assert!(r.deliver(b, sub(0, b"room-a")));
    r.take_outbox(a);
    r.take_outbox(b);

    revoked.store(true, Ordering::SeqCst);
    r.deliver(
        a,
        Message::AwarenessSet {
            channel: Channel(0),
            key: b"cursor".to_vec(),
            value: vec![1],
        },
    );

    assert!(
        r.take_outbox(b).is_empty(),
        "a read-revoked peer sees no presence"
    );
}
