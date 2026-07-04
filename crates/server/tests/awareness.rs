//! Awareness fan-out — ephemeral presence over the registry.
//!
//! A client publishes an awareness entry on a subscribed channel; the server
//! fans it out to the room's *other* subscribers as an AwarenessUpdate tagged
//! with the publisher's actor and each peer's own channel. Awareness never
//! touches the op log or a snapshot, and never echoes to the sender. Publishing
//! before auth, or on an unbound channel, is a protocol violation.

use std::sync::Arc;

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, ErrorCode, Message};
use crdtsync_server::{ConnId, ManualClock, Registry};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn registry() -> Registry {
    Registry::new(cid(0xFF))
}

const ROOM_A: &[u8] = b"room-a";
const ROOM_B: &[u8] = b"room-b";

/// The dev verifier (AllowAll) adopts the credential as the actor, so a client's
/// actor is its credential bytes.
fn actor_of(client: u8) -> Vec<u8> {
    format!("actor-{client}").into_bytes()
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
            credential: actor_of(client),
        }
    ));
    r.take_outbox(id);
    id
}

fn subscribe(r: &mut Registry, id: ConnId, channel: u32, room: &[u8]) {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(channel),
            room: room.to_vec(),
            last_seen_seq: 0,
        }
    ));
    r.take_outbox(id);
}

fn is_violation(m: &Message) -> bool {
    matches!(
        m,
        Message::Error {
            code: ErrorCode::ProtocolViolation,
            ..
        }
    )
}

// --- fan-out ---

#[test]
fn awareness_fans_out_to_room_peers_tagged_with_actor_and_their_channel() {
    let mut r = registry();
    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 5, ROOM_A);
    let b = hello_auth(&mut r, 2);
    subscribe(&mut r, b, 9, ROOM_A);

    assert!(r.deliver(
        a,
        Message::AwarenessSet {
            channel: Channel(5),
            key: b"cursor".to_vec(),
            value: vec![1, 2, 3],
        }
    ));

    assert_eq!(
        r.take_outbox(b),
        vec![Message::AwarenessUpdate {
            channel: Channel(9),
            actor: actor_of(1),
            key: b"cursor".to_vec(),
            value: vec![1, 2, 3],
        }]
    );
    assert!(r.take_outbox(a).is_empty(), "no echo to the sender");
}

#[test]
fn awareness_is_isolated_per_room() {
    let mut r = registry();
    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    let other = hello_auth(&mut r, 2);
    subscribe(&mut r, other, 1, ROOM_B);

    r.deliver(
        a,
        Message::AwarenessSet {
            channel: Channel(1),
            key: b"cursor".to_vec(),
            value: vec![7],
        },
    );
    assert!(r.take_outbox(other).is_empty());
}

#[test]
fn awareness_does_not_advance_the_op_log() {
    let mut r = registry();
    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    r.deliver(
        a,
        Message::AwarenessSet {
            channel: Channel(1),
            key: b"cursor".to_vec(),
            value: vec![1],
        },
    );
    assert_eq!(r.hub().seq(ROOM_A), 0, "awareness is not an op");
}

// --- late-joiner replay ---

fn awareness_updates(msgs: Vec<Message>) -> Vec<Message> {
    msgs.into_iter()
        .filter(|m| matches!(m, Message::AwarenessUpdate { .. }))
        .collect()
}

#[test]
fn a_late_joiner_is_replayed_current_presence() {
    let mut r = registry();
    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    r.deliver(
        a,
        Message::AwarenessSet {
            channel: Channel(1),
            key: b"cursor".to_vec(),
            value: vec![5],
        },
    );

    // A client subscribing afterward is replayed A's entry on its own channel.
    let b = hello_auth(&mut r, 2);
    assert!(r.deliver(
        b,
        Message::Subscribe {
            channel: Channel(7),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
        }
    ));
    assert_eq!(
        awareness_updates(r.take_outbox(b)),
        vec![Message::AwarenessUpdate {
            channel: Channel(7),
            actor: actor_of(1),
            key: b"cursor".to_vec(),
            value: vec![5],
        }]
    );
}

#[test]
fn replay_reflects_the_latest_value_per_key() {
    let mut r = registry();
    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    for v in [vec![1], vec![2]] {
        r.deliver(
            a,
            Message::AwarenessSet {
                channel: Channel(1),
                key: b"cursor".to_vec(),
                value: v,
            },
        );
    }
    let b = hello_auth(&mut r, 2);
    r.deliver(
        b,
        Message::Subscribe {
            channel: Channel(1),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert_eq!(
        awareness_updates(r.take_outbox(b)),
        vec![Message::AwarenessUpdate {
            channel: Channel(1),
            actor: actor_of(1),
            key: b"cursor".to_vec(),
            value: vec![2],
        }]
    );
}

#[test]
fn a_departed_clients_presence_is_not_replayed_after_the_grace_window() {
    let mut r = registry();
    let clock = Arc::new(ManualClock::new(0));
    r.set_clock(clock.clone());
    r.set_grace_millis(5000);

    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    r.deliver(
        a,
        Message::AwarenessSet {
            channel: Channel(1),
            key: b"cursor".to_vec(),
            value: vec![5],
        },
    );
    r.disconnect(a);
    // Past the grace window a sweep drops the departed client's presence.
    clock.advance(5000);
    r.sweep();

    let b = hello_auth(&mut r, 2);
    r.deliver(
        b,
        Message::Subscribe {
            channel: Channel(1),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert!(
        awareness_updates(r.take_outbox(b)).is_empty(),
        "a gone client's presence is cleared once the grace window lapses"
    );
}

#[test]
fn a_fresh_room_replays_no_presence() {
    let mut r = registry();
    let a = hello_auth(&mut r, 1);
    assert!(r.deliver(
        a,
        Message::Subscribe {
            channel: Channel(1),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
        }
    ));
    // Only the empty catch-up, no awareness replay.
    assert!(awareness_updates(r.take_outbox(a)).is_empty());
}

// --- violations ---

#[test]
fn awareness_before_auth_is_a_violation() {
    let mut r = registry();
    let c = r.connect();
    r.deliver(
        c,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
        },
    );
    r.take_outbox(c);
    let keep_open = r.deliver(
        c,
        Message::AwarenessSet {
            channel: Channel(0),
            key: b"cursor".to_vec(),
            value: vec![1],
        },
    );
    assert!(!keep_open);
    assert!(is_violation(&r.take_outbox(c)[0]));
}

#[test]
fn awareness_on_an_unbound_channel_is_a_violation() {
    let mut r = registry();
    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    let keep_open = r.deliver(
        a,
        Message::AwarenessSet {
            channel: Channel(2), // never subscribed
            key: b"cursor".to_vec(),
            value: vec![1],
        },
    );
    assert!(!keep_open);
    assert!(is_violation(&r.take_outbox(a)[0]));
}
