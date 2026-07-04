//! Reconnect grace for ephemeral presence.
//!
//! A disconnect does not drop a client's awareness at once — the client is held
//! stale for a grace window so a brief reconnect keeps its presence alive across
//! the gap. Only a [`sweep`](crdtsync_server::Registry::sweep) past the deadline
//! clears the presence and tells the room's remaining subscribers with an
//! AwarenessClear on their own channel. A [`ManualClock`] drives the window.

use std::sync::Arc;

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Message};
use crdtsync_server::{ConnId, ManualClock, Registry};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM_A: &[u8] = b"room-a";
const GRACE: u64 = 5000;

/// The dev verifier (AllowAll) adopts the credential as the actor.
fn actor_of(client: u8) -> Vec<u8> {
    format!("actor-{client}").into_bytes()
}

/// A registry driven by a shared manual clock, with the default grace window.
fn registry() -> (Registry, Arc<ManualClock>) {
    let mut r = Registry::new(cid(0xFF));
    let clock = Arc::new(ManualClock::new(0));
    r.set_clock(clock.clone());
    r.set_grace_millis(GRACE);
    (r, clock)
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

fn set_awareness(r: &mut Registry, id: ConnId, channel: u32, key: &[u8], value: Vec<u8>) {
    assert!(r.deliver(
        id,
        Message::AwarenessSet {
            channel: Channel(channel),
            key: key.to_vec(),
            value,
        }
    ));
}

fn awareness_updates(msgs: Vec<Message>) -> Vec<Message> {
    msgs.into_iter()
        .filter(|m| matches!(m, Message::AwarenessUpdate { .. }))
        .collect()
}

fn awareness_clears(msgs: Vec<Message>) -> Vec<Message> {
    msgs.into_iter()
        .filter(|m| matches!(m, Message::AwarenessClear { .. }))
        .collect()
}

#[test]
fn a_sweep_past_the_grace_window_clears_departed_presence() {
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    set_awareness(&mut r, a, 1, b"cursor", vec![5]);
    r.disconnect(a);

    clock.advance(GRACE);
    r.sweep();

    // A joiner afterward is not replayed the cleared presence.
    let b = hello_auth(&mut r, 2);
    r.deliver(
        b,
        Message::Subscribe {
            channel: Channel(1),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert!(awareness_updates(r.take_outbox(b)).is_empty());
}

#[test]
fn a_sweep_tells_room_peers_the_presence_expired() {
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    let b = hello_auth(&mut r, 2);
    subscribe(&mut r, b, 9, ROOM_A);

    set_awareness(&mut r, a, 1, b"cursor", vec![5]);
    r.take_outbox(b); // drop the fan-out update
    r.disconnect(a);

    clock.advance(GRACE);
    r.sweep();

    // B learns A's presence expired on B's own channel, tagged with A's actor.
    assert_eq!(
        awareness_clears(r.take_outbox(b)),
        vec![Message::AwarenessClear {
            channel: Channel(9),
            actor: actor_of(1),
        }]
    );
}

#[test]
fn presence_is_retained_within_the_grace_window() {
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    let b = hello_auth(&mut r, 2);
    subscribe(&mut r, b, 9, ROOM_A);

    set_awareness(&mut r, a, 1, b"cursor", vec![5]);
    r.take_outbox(b);
    r.disconnect(a);

    // A sweep before the deadline changes nothing.
    clock.advance(GRACE - 1);
    r.sweep();
    assert!(awareness_clears(r.take_outbox(b)).is_empty());

    // A joiner is still replayed A's presence.
    let c = hello_auth(&mut r, 3);
    r.deliver(
        c,
        Message::Subscribe {
            channel: Channel(1),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert_eq!(
        awareness_updates(r.take_outbox(c)),
        vec![Message::AwarenessUpdate {
            channel: Channel(1),
            actor: actor_of(1),
            key: b"cursor".to_vec(),
            value: vec![5],
        }]
    );
}

#[test]
fn a_reconnect_within_the_window_cancels_the_clear() {
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    let b = hello_auth(&mut r, 2);
    subscribe(&mut r, b, 9, ROOM_A);

    set_awareness(&mut r, a, 1, b"cursor", vec![5]);
    r.take_outbox(b);
    r.disconnect(a);

    // The same client reappears before the deadline, cancelling the pending clear.
    clock.advance(GRACE - 1);
    let _a2 = hello_auth(&mut r, 1);

    // A later sweep well past the original deadline finds nothing stale.
    clock.advance(GRACE);
    r.sweep();
    assert!(awareness_clears(r.take_outbox(b)).is_empty());

    // The presence survived the gap: a joiner is replayed it.
    let c = hello_auth(&mut r, 3);
    r.deliver(
        c,
        Message::Subscribe {
            channel: Channel(1),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert_eq!(
        awareness_updates(r.take_outbox(c)),
        vec![Message::AwarenessUpdate {
            channel: Channel(1),
            actor: actor_of(1),
            key: b"cursor".to_vec(),
            value: vec![5],
        }]
    );
}

#[test]
fn a_sweep_with_nothing_stale_is_a_no_op() {
    let (mut r, clock) = registry();
    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    set_awareness(&mut r, a, 1, b"cursor", vec![5]);

    clock.advance(GRACE * 2);
    r.sweep();

    // A live client's presence is untouched: a joiner still sees it.
    let b = hello_auth(&mut r, 2);
    r.deliver(
        b,
        Message::Subscribe {
            channel: Channel(1),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert_eq!(awareness_updates(r.take_outbox(b)).len(), 1);
}

#[test]
fn a_second_connection_asserting_a_live_client_cannot_steal_its_presence() {
    let (mut r, clock) = registry();
    // The victim holds presence in the room over a live connection.
    let victim = hello_auth(&mut r, 1);
    subscribe(&mut r, victim, 1, ROOM_A);
    set_awareness(&mut r, victim, 1, b"cursor", vec![5]);

    // A second connection asserts the victim's client id, then drops. Its
    // departure must not schedule a sweep of a presence another live connection
    // still holds.
    let intruder = r.connect();
    assert!(r.deliver(
        intruder,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0
        }
    ));
    r.disconnect(intruder);

    clock.advance(GRACE);
    r.sweep();

    // The victim is still connected, so a joiner is still replayed its presence.
    let joiner = hello_auth(&mut r, 2);
    r.deliver(
        joiner,
        Message::Subscribe {
            channel: Channel(1),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert!(
        !awareness_updates(r.take_outbox(joiner)).is_empty(),
        "a live client's presence was swept by another connection's disconnect"
    );
}

#[test]
fn an_authenticated_second_connection_cannot_steal_a_live_clients_presence() {
    let (mut r, clock) = registry();
    let victim = hello_auth(&mut r, 1);
    subscribe(&mut r, victim, 1, ROOM_A);
    set_awareness(&mut r, victim, 1, b"cursor", vec![5]);

    // A different, authenticated connection asserting the victim's client id
    // still cannot schedule a sweep while the victim's connection is live.
    let intruder = hello_auth(&mut r, 1);
    r.disconnect(intruder);

    clock.advance(GRACE);
    r.sweep();

    let joiner = hello_auth(&mut r, 2);
    r.deliver(
        joiner,
        Message::Subscribe {
            channel: Channel(1),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert!(
        !awareness_updates(r.take_outbox(joiner)).is_empty(),
        "a live client's presence was swept by another authenticated connection"
    );
}

#[test]
fn an_awareness_key_dropped_at_the_cap_is_not_broadcast() {
    let (mut r, _clock) = registry();
    let a = hello_auth(&mut r, 1);
    subscribe(&mut r, a, 1, ROOM_A);
    let b = hello_auth(&mut r, 2);
    subscribe(&mut r, b, 1, ROOM_A);
    r.take_outbox(b);

    // A flood of distinct keys past the cap: only stored keys are broadcast, so
    // the peer sees the cap, not the flood.
    for k in 0..100u32 {
        set_awareness(&mut r, a, 1, &k.to_le_bytes(), vec![0]);
    }
    assert_eq!(
        awareness_updates(r.take_outbox(b)).len(),
        64,
        "a key dropped at the cap must not be broadcast to peers"
    );
}

#[test]
fn an_unauthenticated_socket_does_not_keep_a_departed_clients_presence() {
    let (mut r, clock) = registry();
    let victim = hello_auth(&mut r, 1);
    subscribe(&mut r, victim, 1, ROOM_A);
    set_awareness(&mut r, victim, 1, b"cursor", vec![5]);

    // An unauthenticated socket asserting the victim's id does not count as
    // holding the presence, so the victim's real departure still schedules a
    // sweep and the presence expires.
    let ghost = r.connect();
    assert!(r.deliver(
        ghost,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0
        }
    ));
    r.disconnect(victim);

    clock.advance(GRACE);
    r.sweep();

    let joiner = hello_auth(&mut r, 2);
    r.deliver(
        joiner,
        Message::Subscribe {
            channel: Channel(1),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert!(
        awareness_updates(r.take_outbox(joiner)).is_empty(),
        "an unauthenticated socket kept a departed client's presence alive"
    );
}

#[test]
fn an_unauthenticated_hello_does_not_cancel_a_pending_sweep() {
    let (mut r, clock) = registry();
    let victim = hello_auth(&mut r, 1);
    subscribe(&mut r, victim, 1, ROOM_A);
    set_awareness(&mut r, victim, 1, b"cursor", vec![5]);
    r.disconnect(victim); // schedules the grace timer

    // A bare Hello asserting the id must not cancel the pending sweep — only an
    // authenticated reconnect does.
    let ghost = r.connect();
    assert!(r.deliver(
        ghost,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0
        }
    ));

    clock.advance(GRACE);
    r.sweep();

    let joiner = hello_auth(&mut r, 2);
    r.deliver(
        joiner,
        Message::Subscribe {
            channel: Channel(1),
            room: ROOM_A.to_vec(),
            last_seen_seq: 0,
        },
    );
    assert!(
        awareness_updates(r.take_outbox(joiner)).is_empty(),
        "an unauthenticated Hello cancelled a pending presence sweep"
    );
}
