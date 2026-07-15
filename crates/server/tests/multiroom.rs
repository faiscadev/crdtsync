//! Multi-room — several room subscriptions multiplexed over one connection.
//!
//! A connection is no longer bound to a single room. It opens a [`Channel`] per
//! Subscribe, each mapped to a room; every inbound op batch names its channel,
//! so the registry routes it to that channel's room and ingests it there. A
//! broadcast fans out to the room's *other* subscribers, tagged with the
//! channel each of them opened for that room — so a peer multiplexing several
//! rooms still knows which subscription an op belongs to. Unsubscribe frees a
//! channel; ops on an unknown or freed channel are a protocol violation.

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Scalar};
use crdtsync_server::{ConnId, Registry};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn registry() -> Registry {
    Registry::new(cid(0xFF))
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

const ROOM_A: &[u8] = b"room-a";
const ROOM_B: &[u8] = b"room-b";

fn hello(r: &mut Registry, client: u8) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: Vec::new(),
            schema_version: 0,
        }
    ));
    // The dev-mode verifier accepts any credential; drop the AuthOk reply.
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"cred".to_vec()
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
            zone: Vec::new(),
            last_seen_seq: 0,
            branch: Vec::new(),
        }
    ));
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

// --- one connection, several rooms ---

#[test]
fn subscribe_replies_a_catch_up_on_its_channel() {
    let mut r = registry();
    let a = hello(&mut r, 1);
    subscribe(&mut r, a, 4, ROOM_A);
    assert_eq!(
        r.take_outbox(a),
        vec![Message::Ops {
            channel: Channel(4),
            ops: Vec::new(),
        }]
    );
}

#[test]
fn one_connection_holds_two_rooms_at_once() {
    let mut r = registry();
    let a = hello(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    subscribe(&mut r, a, 2, ROOM_B);
    r.take_outbox(a);

    // A peer in each room, to observe that A's writes land in the right room.
    let peer_a = hello(&mut r, 2);
    subscribe(&mut r, peer_a, 9, ROOM_A);
    let peer_b = hello(&mut r, 3);
    subscribe(&mut r, peer_b, 8, ROOM_B);
    r.take_outbox(peer_a);
    r.take_outbox(peer_b);

    let ops_a = doc(1).transact(|tx| tx.register(b"x", Scalar::Int(1)));
    r.deliver(
        a,
        Message::Ops {
            channel: Channel(1),
            ops: ops_a.clone(),
        },
    );

    // The room-A op reaches the room-A peer only, tagged with that peer's channel.
    assert_eq!(
        r.take_outbox(peer_a),
        vec![Message::Ops {
            channel: Channel(9),
            ops: ops_a,
        }]
    );
    assert!(r.take_outbox(peer_b).is_empty(), "room-B is isolated");
}

#[test]
fn a_broadcast_is_tagged_with_each_peers_own_channel() {
    let mut r = registry();
    let a = hello(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    // Two peers subscribe the same room under different channel numbers.
    let b = hello(&mut r, 2);
    subscribe(&mut r, b, 55, ROOM_A);
    let c = hello(&mut r, 3);
    subscribe(&mut r, c, 77, ROOM_A);
    r.take_outbox(a);
    r.take_outbox(b);
    r.take_outbox(c);

    let ops = doc(1).transact(|tx| tx.register(b"x", Scalar::Int(1)));
    r.deliver(
        a,
        Message::Ops {
            channel: Channel(1),
            ops: ops.clone(),
        },
    );

    assert_eq!(
        r.take_outbox(b),
        vec![Message::Ops {
            channel: Channel(55),
            ops: ops.clone(),
        }]
    );
    assert_eq!(
        r.take_outbox(c),
        vec![Message::Ops {
            channel: Channel(77),
            ops,
        }]
    );
}

#[test]
fn a_connection_receives_broadcasts_on_each_of_its_rooms() {
    let mut r = registry();
    // A multiplexes two rooms; two separate writers drive each room.
    let a = hello(&mut r, 1);
    subscribe(&mut r, a, 10, ROOM_A);
    subscribe(&mut r, a, 20, ROOM_B);
    r.take_outbox(a);

    let wa = hello(&mut r, 2);
    subscribe(&mut r, wa, 1, ROOM_A);
    let wb = hello(&mut r, 3);
    subscribe(&mut r, wb, 1, ROOM_B);
    r.take_outbox(wa);
    r.take_outbox(wb);

    let ops_a = doc(2).transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let ops_b = doc(3).transact(|tx| tx.register(b"b", Scalar::Int(2)));
    r.deliver(
        wa,
        Message::Ops {
            channel: Channel(1),
            ops: ops_a.clone(),
        },
    );
    r.deliver(
        wb,
        Message::Ops {
            channel: Channel(1),
            ops: ops_b.clone(),
        },
    );

    let out = r.take_outbox(a);
    assert!(out.contains(&Message::Ops {
        channel: Channel(10),
        ops: ops_a,
    }));
    assert!(out.contains(&Message::Ops {
        channel: Channel(20),
        ops: ops_b,
    }));
}

// --- unsubscribe ---

#[test]
fn unsubscribe_stops_delivery_on_that_channel() {
    let mut r = registry();
    let a = hello(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    let b = hello(&mut r, 2);
    subscribe(&mut r, b, 2, ROOM_A);
    r.take_outbox(a);
    r.take_outbox(b);

    assert!(r.deliver(
        b,
        Message::Unsubscribe {
            channel: Channel(2)
        }
    ));
    r.take_outbox(b);

    let ops = doc(1).transact(|tx| tx.register(b"x", Scalar::Int(1)));
    r.deliver(
        a,
        Message::Ops {
            channel: Channel(1),
            ops,
        },
    );
    assert!(
        r.take_outbox(b).is_empty(),
        "an unsubscribed channel receives nothing"
    );
}

#[test]
fn a_freed_channel_number_can_be_reused_for_another_room() {
    let mut r = registry();
    let a = hello(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    assert!(r.deliver(
        a,
        Message::Unsubscribe {
            channel: Channel(1)
        }
    ));
    r.take_outbox(a);
    // The same channel number now binds a different room.
    subscribe(&mut r, a, 1, ROOM_B);
    assert_eq!(
        r.take_outbox(a),
        vec![Message::Ops {
            channel: Channel(1),
            ops: Vec::new(),
        }]
    );
}

// --- violations ---

#[test]
fn ops_on_an_unbound_channel_are_a_violation() {
    let mut r = registry();
    let a = hello(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    r.take_outbox(a);

    let ops = doc(1).transact(|tx| tx.register(b"x", Scalar::Int(1)));
    let keep_open = r.deliver(
        a,
        Message::Ops {
            channel: Channel(2), // never subscribed
            ops,
        },
    );
    assert!(!keep_open);
    assert!(is_violation(&r.take_outbox(a)[0]));
}

#[test]
fn subscribing_an_already_bound_channel_is_a_violation() {
    let mut r = registry();
    let a = hello(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    r.take_outbox(a);

    let keep_open = r.deliver(
        a,
        Message::Subscribe {
            channel: Channel(1), // already in use on this connection
            room: ROOM_B.to_vec(),
            zone: Vec::new(),
            last_seen_seq: 0,
            branch: Vec::new(),
        },
    );
    assert!(!keep_open);
    assert!(is_violation(&r.take_outbox(a)[0]));
}

#[test]
fn unsubscribing_an_unbound_channel_is_a_violation() {
    let mut r = registry();
    let a = hello(&mut r, 1);
    let keep_open = r.deliver(
        a,
        Message::Unsubscribe {
            channel: Channel(9),
        },
    );
    assert!(!keep_open);
    assert!(is_violation(&r.take_outbox(a)[0]));
}
