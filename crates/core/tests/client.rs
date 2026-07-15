//! Client session — the replica's side of the wire protocol.
//!
//! A [`ClientSession`] mirrors the server's session driver from the client's
//! seat. It opens with Hello, then holds several room subscriptions at once —
//! each on its own [`Channel`], with its own local replica and caught-up
//! position. Subscribe assigns the next channel and draws a catch-up (an op
//! delta or a whole-replica snapshot); inbound frames route to a room by their
//! channel; a reconnect resumes each room from where it left off.
//!
//! Sequence tracking is per room, by count: the server's deltas and broadcasts
//! are contiguous runs of ops at the head, each op holding one server sequence,
//! so a batch of `n` ops advances that room's seen sequence by `n`. A
//! redelivered op still advances the sequence (it occupies a server slot) but
//! does not re-apply, because the replica deduplicates.

use crdtsync_core::client::{ClientError, ClientSession};
use crdtsync_core::{Channel, ClientId, Document, Element, ErrorCode, Message, Op, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A stand-in for the server's authoritative replica of a room, the source of
/// the ops and snapshots a client catches up from.
fn srv() -> Document {
    Document::new(cid(0xFF))
}

/// Read a top-level Register's int from a room's replica.
fn int(doc: &Document, key: &[u8]) -> i64 {
    match doc.get(key) {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected Int, got {other:?}"),
        },
        _ => panic!("expected a Register"),
    }
}

fn counter(doc: &Document, key: &[u8]) -> i64 {
    match doc.get(key) {
        Some(Element::Counter(c)) => c.borrow().read(),
        _ => panic!("expected a Counter"),
    }
}

fn ops_msg(channel: Channel, ops: Vec<Op>) -> Message {
    Message::Ops { channel, ops }
}

/// Unwrap the ops of a `Message::Ops`.
fn ops_of(m: Message) -> Vec<Op> {
    match m {
        Message::Ops { ops, .. } => ops,
        other => panic!("expected Ops, got {other:?}"),
    }
}

const ROOM_A: &[u8] = b"room-a";
const ROOM_B: &[u8] = b"room-b";

// --- handshake framing ---

#[test]
fn hello_names_the_client() {
    let session = ClientSession::new(cid(1));
    match session.hello() {
        Message::Hello { client, .. } => assert_eq!(client, cid(1)),
        other => panic!("expected Hello, got {other:?}"),
    }
}

#[test]
fn a_bare_session_opens_as_a_relay() {
    let session = ClientSession::new(cid(1));
    assert_eq!(session.app_id(), b"");
    assert_eq!(session.schema_version(), 0);
    match session.hello() {
        Message::Hello {
            app_id,
            schema_version,
            ..
        } => {
            assert_eq!(app_id, b"");
            assert_eq!(schema_version, 0, "empty app + version 0 is a relay");
        }
        other => panic!("expected Hello, got {other:?}"),
    }
}

#[test]
fn declaring_an_app_carries_it_and_the_version_into_hello() {
    let mut session = ClientSession::new(cid(1));
    session.declare_app(b"app-x", 3);
    assert_eq!(session.app_id(), b"app-x");
    assert_eq!(session.schema_version(), 3);
    match session.hello() {
        Message::Hello {
            client,
            app_id,
            schema_version,
        } => {
            assert_eq!(client, cid(1));
            assert_eq!(app_id, b"app-x");
            assert_eq!(schema_version, 3);
        }
        other => panic!("expected Hello, got {other:?}"),
    }
}

#[test]
fn a_declared_app_with_version_zero_is_a_dynamic_client() {
    // Naming an app but leaving the version at 0 asks the server for its head.
    let mut session = ClientSession::new(cid(1));
    session.declare_app(b"app-x", 0);
    match session.hello() {
        Message::Hello {
            app_id,
            schema_version,
            ..
        } => {
            assert_eq!(app_id, b"app-x");
            assert_eq!(schema_version, 0);
        }
        other => panic!("expected Hello, got {other:?}"),
    }
}

#[test]
fn re_declaring_an_app_replaces_the_prior_declaration() {
    let mut session = ClientSession::new(cid(1));
    session.declare_app(b"app-x", 1);
    session.declare_app(b"app-y", 7);
    assert_eq!(session.app_id(), b"app-y");
    assert_eq!(session.schema_version(), 7);
}

#[test]
fn auth_presents_the_credential_verbatim() {
    let session = ClientSession::new(cid(1));
    match session.auth(b"my-token") {
        Message::Auth { credential } => assert_eq!(credential, b"my-token"),
        other => panic!("expected Auth, got {other:?}"),
    }
    // The actor is unknown until the server replies AuthOk.
    assert_eq!(session.actor(), None);
}

#[test]
fn authok_records_the_server_derived_actor() {
    let mut session = ClientSession::new(cid(1));
    session
        .receive(Message::AuthOk {
            actor: b"alice".to_vec(),
        })
        .unwrap();
    assert_eq!(session.actor(), Some(&b"alice"[..]));
}

#[test]
fn a_fresh_session_has_no_active_schema() {
    let session = ClientSession::new(cid(1));
    assert_eq!(session.active_schema_version(), None);
    assert_eq!(session.active_schema(), None);
}

#[test]
fn a_schema_advert_records_the_active_version_and_bytes() {
    let mut session = ClientSession::new(cid(1));
    session
        .receive(Message::SchemaAdvert {
            schema_version: 3,
            schema: br#"{"v":3}"#.to_vec(),
        })
        .unwrap();
    assert_eq!(session.active_schema_version(), Some(3));
    assert_eq!(session.active_schema(), Some(&br#"{"v":3}"#[..]));
}

#[test]
fn a_later_advert_replaces_the_recorded_schema() {
    // The server is authoritative — a fresh advertisement (e.g. a rolling upgrade)
    // supersedes the last.
    let mut session = ClientSession::new(cid(1));
    session
        .receive(Message::SchemaAdvert {
            schema_version: 1,
            schema: br#"{"v":1}"#.to_vec(),
        })
        .unwrap();
    session
        .receive(Message::SchemaAdvert {
            schema_version: 2,
            schema: br#"{"v":2}"#.to_vec(),
        })
        .unwrap();
    assert_eq!(session.active_schema_version(), Some(2));
    assert_eq!(session.active_schema(), Some(&br#"{"v":2}"#[..]));
}

// --- awareness ---

#[test]
fn set_awareness_frames_a_publish_on_the_channel() {
    let mut session = ClientSession::new(cid(1));
    let (ch, _) = session.subscribe(ROOM_A);
    match session.set_awareness(ch, b"cursor", &[1, 2, 3]) {
        Some(Message::AwarenessSet {
            channel,
            key,
            value,
        }) => {
            assert_eq!(channel, ch);
            assert_eq!(key, b"cursor");
            assert_eq!(value, vec![1, 2, 3]);
        }
        other => panic!("expected AwarenessSet, got {other:?}"),
    }
}

#[test]
fn set_awareness_on_an_unknown_channel_is_none() {
    let session = ClientSession::new(cid(1));
    assert!(session.set_awareness(Channel(3), b"cursor", &[1]).is_none());
}

#[test]
fn an_update_records_a_peers_entry() {
    let mut session = ClientSession::new(cid(1));
    let (ch, _) = session.subscribe(ROOM_A);
    session
        .receive(Message::AwarenessUpdate {
            channel: ch,
            actor: b"alice".to_vec(),
            key: b"cursor".to_vec(),
            value: vec![9],
        })
        .unwrap();
    assert_eq!(session.awareness(ch, b"alice", b"cursor"), Some(&[9][..]));
    assert_eq!(session.awareness_len(ch), 1);
}

#[test]
fn an_update_is_last_writer_wins_per_actor_and_key() {
    let mut session = ClientSession::new(cid(1));
    let (ch, _) = session.subscribe(ROOM_A);
    let upd = |value: Vec<u8>| Message::AwarenessUpdate {
        channel: ch,
        actor: b"alice".to_vec(),
        key: b"cursor".to_vec(),
        value,
    };
    session.receive(upd(vec![1])).unwrap();
    session.receive(upd(vec![2])).unwrap();
    assert_eq!(session.awareness(ch, b"alice", b"cursor"), Some(&[2][..]));
    assert_eq!(session.awareness_len(ch), 1);
}

#[test]
fn distinct_actors_coexist() {
    let mut session = ClientSession::new(cid(1));
    let (ch, _) = session.subscribe(ROOM_A);
    for actor in [b"alice".to_vec(), b"bob".to_vec()] {
        session
            .receive(Message::AwarenessUpdate {
                channel: ch,
                actor,
                key: b"cursor".to_vec(),
                value: vec![1],
            })
            .unwrap();
    }
    assert_eq!(session.awareness_len(ch), 2);
    assert_eq!(session.awareness(ch, b"bob", b"cursor"), Some(&[1][..]));
}

#[test]
fn an_update_on_an_unknown_channel_is_rejected() {
    let mut session = ClientSession::new(cid(1));
    session.subscribe(ROOM_A);
    let err = session.receive(Message::AwarenessUpdate {
        channel: Channel(9),
        actor: b"alice".to_vec(),
        key: b"cursor".to_vec(),
        value: vec![1],
    });
    assert!(matches!(err, Err(ClientError::UnknownChannel(_))));
}

#[test]
fn a_clear_drops_all_of_an_actors_entries() {
    let mut session = ClientSession::new(cid(1));
    let (ch, _) = session.subscribe(ROOM_A);
    let upd = |actor: &[u8], key: &[u8]| Message::AwarenessUpdate {
        channel: ch,
        actor: actor.to_vec(),
        key: key.to_vec(),
        value: vec![1],
    };
    session.receive(upd(b"alice", b"cursor")).unwrap();
    session.receive(upd(b"alice", b"typing")).unwrap();
    session.receive(upd(b"bob", b"cursor")).unwrap();

    session
        .receive(Message::AwarenessClear {
            channel: ch,
            actor: b"alice".to_vec(),
        })
        .unwrap();

    assert_eq!(session.awareness(ch, b"alice", b"cursor"), None);
    assert_eq!(session.awareness(ch, b"alice", b"typing"), None);
    assert_eq!(session.awareness(ch, b"bob", b"cursor"), Some(&[1][..]));
    assert_eq!(session.awareness_len(ch), 1);
}

#[test]
fn a_per_key_clear_drops_only_that_entry() {
    let mut session = ClientSession::new(cid(1));
    let (ch, _) = session.subscribe(ROOM_A);
    let upd = |actor: &[u8], key: &[u8]| Message::AwarenessUpdate {
        channel: ch,
        actor: actor.to_vec(),
        key: key.to_vec(),
        value: vec![1],
    };
    session.receive(upd(b"alice", b"cursor")).unwrap();
    session.receive(upd(b"alice", b"typing")).unwrap();
    session.receive(upd(b"bob", b"cursor")).unwrap();

    // One of alice's entries expires by timed TTL; her others and bob's stay.
    session
        .receive(Message::AwarenessClearKey {
            channel: ch,
            actor: b"alice".to_vec(),
            key: b"cursor".to_vec(),
        })
        .unwrap();

    assert_eq!(session.awareness(ch, b"alice", b"cursor"), None);
    assert_eq!(session.awareness(ch, b"alice", b"typing"), Some(&[1][..]));
    assert_eq!(session.awareness(ch, b"bob", b"cursor"), Some(&[1][..]));
    assert_eq!(session.awareness_len(ch), 2);
}

#[test]
fn a_clear_on_an_unknown_channel_is_rejected() {
    let mut session = ClientSession::new(cid(1));
    session.subscribe(ROOM_A);
    let err = session.receive(Message::AwarenessClear {
        channel: Channel(9),
        actor: b"alice".to_vec(),
    });
    assert!(matches!(err, Err(ClientError::UnknownChannel(_))));
}

#[test]
fn subscribe_assigns_a_channel_and_requests_from_zero() {
    let mut session = ClientSession::new(cid(1));
    let (channel, msg) = session.subscribe(ROOM_A);
    match msg {
        Message::Subscribe {
            channel: c,
            room,
            branch,
            zone: _,
            last_seen_seq,
        } => {
            assert_eq!(c, channel);
            assert_eq!(room, ROOM_A);
            // No branch named — the default `main`, empty on the wire.
            assert_eq!(branch, b"");
            assert_eq!(last_seen_seq, 0);
        }
        other => panic!("expected Subscribe, got {other:?}"),
    }
    assert_eq!(session.room(channel), Some(ROOM_A));
    assert_eq!(session.branch(channel), Some(&b""[..]));
    assert_eq!(session.last_seen_seq(channel), Some(0));
}

#[test]
fn subscribe_branch_names_its_branch_on_the_frame_and_records_it() {
    let mut session = ClientSession::new(cid(1));
    let (channel, msg) = session.subscribe_branch(ROOM_A, b"release-2");
    match msg {
        Message::Subscribe {
            channel: c,
            room,
            branch,
            zone: _,
            last_seen_seq,
        } => {
            assert_eq!(c, channel);
            assert_eq!(room, ROOM_A);
            assert_eq!(branch, b"release-2");
            assert_eq!(last_seen_seq, 0);
        }
        other => panic!("expected Subscribe, got {other:?}"),
    }
    assert_eq!(session.room(channel), Some(ROOM_A));
    assert_eq!(session.branch(channel), Some(&b"release-2"[..]));
}

#[test]
fn subscribe_branch_with_an_empty_branch_is_main() {
    let mut session = ClientSession::new(cid(1));
    let (channel, msg) = session.subscribe_branch(ROOM_A, b"");
    match msg {
        Message::Subscribe { branch, .. } => assert_eq!(branch, b""),
        other => panic!("expected Subscribe, got {other:?}"),
    }
    assert_eq!(session.branch(channel), Some(&b""[..]));
}

#[test]
fn subscribe_leaves_the_zone_empty_for_the_whole_room() {
    let mut session = ClientSession::new(cid(1));
    let (channel, msg) = session.subscribe(ROOM_A);
    match msg {
        Message::Subscribe { zone, .. } => assert_eq!(zone, b""),
        other => panic!("expected Subscribe, got {other:?}"),
    }
    assert_eq!(session.zone(channel), Some(&b""[..]));
}

#[test]
fn subscribe_zone_names_its_zone_on_the_frame_and_records_it() {
    let mut session = ClientSession::new(cid(1));
    let (channel, msg) = session.subscribe_zone(ROOM_A, b"west");
    match msg {
        Message::Subscribe {
            channel: c,
            room,
            branch,
            zone,
            last_seen_seq,
        } => {
            assert_eq!(c, channel);
            assert_eq!(room, ROOM_A);
            assert_eq!(branch, b"");
            assert_eq!(zone, b"west");
            assert_eq!(last_seen_seq, 0);
        }
        other => panic!("expected Subscribe, got {other:?}"),
    }
    assert_eq!(session.room(channel), Some(ROOM_A));
    assert_eq!(session.branch(channel), Some(&b""[..]));
    assert_eq!(session.zone(channel), Some(&b"west"[..]));
}

#[test]
fn subscribe_zone_selector_rides_the_wire() {
    let mut session = ClientSession::new(cid(1));
    let (_, msg) = session.subscribe_zone(ROOM_A, b"west");
    let decoded = crdtsync_core::decode_message(&crdtsync_core::encode_message(&msg))
        .expect("Subscribe round-trips");
    match decoded {
        Message::Subscribe { room, zone, .. } => {
            assert_eq!(room, ROOM_A);
            assert_eq!(zone, b"west");
        }
        other => panic!("expected Subscribe, got {other:?}"),
    }
}

#[test]
fn resume_preserves_the_zone() {
    let mut session = ClientSession::new(cid(1));
    let (channel, _) = session.subscribe_zone(ROOM_A, b"west");
    match session.resume(channel).expect("held channel resumes") {
        Message::Subscribe { zone, .. } => assert_eq!(zone, b"west"),
        other => panic!("expected Subscribe, got {other:?}"),
    }
}

#[test]
fn two_rooms_get_distinct_channels() {
    let mut session = ClientSession::new(cid(1));
    let (a, _) = session.subscribe(ROOM_A);
    let (b, _) = session.subscribe(ROOM_B);
    assert_ne!(a, b);
    assert_eq!(session.room(a), Some(ROOM_A));
    assert_eq!(session.room(b), Some(ROOM_B));
}

// --- catch-up, per room ---

#[test]
fn an_ops_catch_up_converges_the_rooms_replica_and_tracks_the_sequence() {
    let mut srv = srv();
    let mut delta = srv.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    delta.extend(srv.transact(|tx| tx.register(b"b", Scalar::Int(2))));

    let mut session = ClientSession::new(cid(2));
    let (ch, _) = session.subscribe(ROOM_A);
    session.receive(ops_msg(ch, delta)).unwrap();

    let doc = session.document(ch).unwrap();
    assert_eq!(int(doc, b"a"), 1);
    assert_eq!(int(doc, b"b"), 2);
    assert_eq!(session.last_seen_seq(ch), Some(2));
}

#[test]
fn a_snapshot_catch_up_rebuilds_the_replica_and_adopts_the_sequence() {
    let mut srv = srv();
    srv.transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.inc(b"n", 5);
    });

    let mut session = ClientSession::new(cid(2));
    let (ch, _) = session.subscribe(ROOM_A);
    session
        .receive(Message::Snapshot {
            channel: ch,
            seq: 9,
            state: srv.encode_state(),
        })
        .unwrap();

    let doc = session.document(ch).unwrap();
    assert_eq!(int(doc, b"a"), 1);
    assert_eq!(counter(doc, b"n"), 5);
    assert_eq!(session.last_seen_seq(ch), Some(9));
}

#[test]
fn live_ops_advance_the_replica_and_the_sequence() {
    let mut srv = srv();
    let first = srv.transact(|tx| tx.register(b"a", Scalar::Int(1)));

    let mut session = ClientSession::new(cid(2));
    let (ch, _) = session.subscribe(ROOM_A);
    session.receive(ops_msg(ch, first)).unwrap();
    assert_eq!(session.last_seen_seq(ch), Some(1));

    let later = srv.transact(|tx| tx.register(b"b", Scalar::Int(2)));
    session.receive(ops_msg(ch, later)).unwrap();
    assert_eq!(int(session.document(ch).unwrap(), b"b"), 2);
    assert_eq!(session.last_seen_seq(ch), Some(2));
}

// --- rooms are isolated ---

#[test]
fn ops_on_one_channel_do_not_touch_another_rooms_replica() {
    let mut srv_a = srv();
    let a_ops = srv_a.transact(|tx| tx.register(b"a", Scalar::Int(1)));

    let mut session = ClientSession::new(cid(2));
    let (ca, _) = session.subscribe(ROOM_A);
    let (cb, _) = session.subscribe(ROOM_B);
    session.receive(ops_msg(ca, a_ops)).unwrap();

    // Room A caught up; room B is untouched.
    assert_eq!(int(session.document(ca).unwrap(), b"a"), 1);
    assert!(session.document(cb).unwrap().get(b"a").is_none());
    assert_eq!(session.last_seen_seq(cb), Some(0));
}

// --- reconnect, per room ---

#[test]
fn resume_resubscribes_a_room_from_its_last_seen_sequence() {
    let mut srv = srv();
    let mut delta = srv.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    delta.extend(srv.transact(|tx| tx.register(b"b", Scalar::Int(2))));

    let mut session = ClientSession::new(cid(2));
    let (ch, _) = session.subscribe(ROOM_A);
    session.receive(ops_msg(ch, delta)).unwrap();

    // Reconnecting, the client asks only for what it missed past its position,
    // on the same channel and room.
    match session.resume(ch) {
        Some(Message::Subscribe {
            channel,
            room,
            branch,
            zone: _,
            last_seen_seq,
        }) => {
            assert_eq!(channel, ch);
            assert_eq!(room, ROOM_A);
            assert_eq!(branch, b"");
            assert_eq!(last_seen_seq, 2);
        }
        other => panic!("expected a Subscribe, got {other:?}"),
    }
}

#[test]
fn resume_carries_the_subscribed_branch() {
    let mut session = ClientSession::new(cid(2));
    let (ch, _) = session.subscribe_branch(ROOM_A, b"release-2");
    match session.resume(ch) {
        Some(Message::Subscribe { branch, .. }) => assert_eq!(branch, b"release-2"),
        other => panic!("expected a Subscribe, got {other:?}"),
    }
}

#[test]
fn resume_of_an_unknown_channel_is_none() {
    let session = ClientSession::new(cid(1));
    assert!(session.resume(Channel(7)).is_none());
}

#[test]
fn a_redelivered_op_is_idempotent_but_still_advances_the_sequence() {
    let mut srv = srv();
    let op = srv.transact(|tx| tx.register(b"a", Scalar::Int(1)));

    let mut session = ClientSession::new(cid(2));
    let (ch, _) = session.subscribe(ROOM_A);
    session.receive(ops_msg(ch, op.clone())).unwrap();
    assert_eq!(session.last_seen_seq(ch), Some(1));

    session.receive(ops_msg(ch, op)).unwrap();
    assert_eq!(int(session.document(ch).unwrap(), b"a"), 1);
    assert_eq!(session.last_seen_seq(ch), Some(2));
}

// --- local edits, per room ---

#[test]
fn a_local_edit_yields_ops_on_its_channel_without_advancing_the_sequence() {
    let mut session = ClientSession::new(cid(1));
    let (ch, _) = session.subscribe(ROOM_A);

    let outbound = session
        .edit(ch, |tx| tx.register(b"a", Scalar::Int(7)))
        .unwrap();
    assert_eq!(int(session.document(ch).unwrap(), b"a"), 7);
    assert_eq!(session.last_seen_seq(ch), Some(0));
    match &outbound {
        Message::Ops { channel, .. } => assert_eq!(*channel, ch),
        other => panic!("expected Ops, got {other:?}"),
    }
    assert_eq!(ops_of(outbound).len(), 1);
}

#[test]
fn an_edit_on_an_unknown_channel_is_none() {
    let mut session = ClientSession::new(cid(1));
    assert!(session
        .edit(Channel(3), |tx| tx.register(b"a", Scalar::Int(1)))
        .is_none());
}

// --- snapshot over prior state ---

#[test]
fn a_snapshot_replaces_prior_local_state_with_the_server_state() {
    let mut srv = srv();
    srv.transact(|tx| {
        tx.register(b"a", Scalar::Int(1));
        tx.register(b"b", Scalar::Int(2));
    });

    let mut peer = Document::new(cid(3));
    let mut session = ClientSession::new(cid(2));
    let (ch, _) = session.subscribe(ROOM_A);
    session
        .receive(ops_msg(
            ch,
            peer.transact(|tx| tx.register(b"a", Scalar::Int(99))),
        ))
        .unwrap();

    session
        .receive(Message::Snapshot {
            channel: ch,
            seq: 2,
            state: srv.encode_state(),
        })
        .unwrap();
    let doc = session.document(ch).unwrap();
    assert_eq!(int(doc, b"a"), 1);
    assert_eq!(int(doc, b"b"), 2);
    assert_eq!(session.last_seen_seq(ch), Some(2));
}

#[test]
fn edits_after_a_snapshot_still_carry_the_clients_own_id() {
    let mut srv = srv();
    srv.transact(|tx| tx.register(b"a", Scalar::Int(1)));

    let mut session = ClientSession::new(cid(2));
    let (ch, _) = session.subscribe(ROOM_A);
    session
        .receive(Message::Snapshot {
            channel: ch,
            seq: 1,
            state: srv.encode_state(),
        })
        .unwrap();

    let outbound = session
        .edit(ch, |tx| tx.register(b"b", Scalar::Int(2)))
        .unwrap();
    assert!(ops_of(outbound).iter().all(|op| op.id.client == cid(2)));
}

// --- unsubscribe ---

#[test]
fn unsubscribe_drops_the_room_and_frees_the_channel() {
    let mut session = ClientSession::new(cid(1));
    let (ch, _) = session.subscribe(ROOM_A);
    match session.unsubscribe(ch) {
        Some(Message::Unsubscribe { channel }) => assert_eq!(channel, ch),
        other => panic!("expected Unsubscribe, got {other:?}"),
    }
    assert_eq!(session.room(ch), None);
    assert_eq!(session.last_seen_seq(ch), None);
}

#[test]
fn unsubscribe_of_an_unknown_channel_is_none() {
    let mut session = ClientSession::new(cid(1));
    assert!(session.unsubscribe(Channel(4)).is_none());
}

// --- malformed / illegal server frames ---

#[test]
fn ops_on_an_unknown_channel_are_rejected() {
    let mut srv = srv();
    let op = srv.transact(|tx| tx.register(b"a", Scalar::Int(1)));
    let mut session = ClientSession::new(cid(2));
    session.subscribe(ROOM_A);
    let err = session.receive(ops_msg(Channel(9), op));
    assert!(matches!(err, Err(ClientError::UnknownChannel(_))));
}

#[test]
fn a_snapshot_on_an_unknown_channel_is_rejected() {
    let mut session = ClientSession::new(cid(2));
    session.subscribe(ROOM_A);
    let err = session.receive(Message::Snapshot {
        channel: Channel(9),
        seq: 1,
        state: srv().encode_state(),
    });
    assert!(matches!(err, Err(ClientError::UnknownChannel(_))));
}

#[test]
fn a_garbage_snapshot_is_rejected_and_leaves_the_replica_intact() {
    let mut srv = srv();
    let op = srv.transact(|tx| tx.register(b"a", Scalar::Int(1)));

    let mut session = ClientSession::new(cid(2));
    let (ch, _) = session.subscribe(ROOM_A);
    session.receive(ops_msg(ch, op)).unwrap();

    let err = session.receive(Message::Snapshot {
        channel: ch,
        seq: 5,
        state: vec![0xFF, 0xFF, 0xFF, 0xFF],
    });
    assert!(matches!(err, Err(ClientError::BadSnapshot)));
    // The rejected snapshot changed nothing.
    assert_eq!(int(session.document(ch).unwrap(), b"a"), 1);
    assert_eq!(session.last_seen_seq(ch), Some(1));
}

#[test]
fn a_server_error_surfaces_to_the_caller() {
    let mut session = ClientSession::new(cid(1));
    let err = session.receive(Message::Error {
        code: ErrorCode::UnknownRoom,
        message: "no such room".to_string(),
        details: Vec::new(),
    });
    match err {
        Err(ClientError::Server { code, message }) => {
            assert_eq!(code, ErrorCode::UnknownRoom);
            assert_eq!(message, "no such room");
        }
        other => panic!("expected a surfaced server error, got {other:?}"),
    }
}

#[test]
fn a_client_only_message_from_the_server_is_a_violation() {
    let mut session = ClientSession::new(cid(1));
    assert!(matches!(
        session.receive(Message::Hello {
            client: cid(2),
            app_id: Vec::new(),
            schema_version: 0
        }),
        Err(ClientError::UnexpectedMessage(_))
    ));
    assert!(matches!(
        session.receive(Message::Subscribe {
            channel: Channel(0),
            room: ROOM_A.to_vec(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq: 0,
        }),
        Err(ClientError::UnexpectedMessage(_))
    ));
    assert!(matches!(
        session.receive(Message::Unsubscribe {
            channel: Channel(0),
        }),
        Err(ClientError::UnexpectedMessage(_))
    ));
}

// --- atomic transactions over the wire ---

#[test]
fn atomic_edit_tags_its_ops_as_one_transaction() {
    let mut session = ClientSession::new(cid(1));
    let (ch, _) = session.subscribe(ROOM_A);
    let ops = ops_of(
        session
            .atomic_edit(ch, |tx| {
                tx.register(b"first", Scalar::Int(1));
                tx.register(b"last", Scalar::Int(2));
            })
            .expect("held channel"),
    );
    assert_eq!(ops.len(), 2);
    let tx0 = ops[0].tx.clone().expect("tagged");
    assert_eq!(ops[1].tx.clone().expect("tagged").id, tx0.id);
    assert!(ops.iter().all(|o| o.tx.as_ref().unwrap().count == 2));
}

#[test]
fn a_peer_folds_in_an_atomic_edit_all_or_nothing() {
    let mut a = ClientSession::new(cid(1));
    let mut b = ClientSession::new(cid(2));
    let (ca, _) = a.subscribe(ROOM_A);
    let (cb, _) = b.subscribe(ROOM_A);

    let ops = ops_of(
        a.atomic_edit(ca, |tx| {
            tx.register(b"x", Scalar::Int(1));
            tx.register(b"y", Scalar::Int(2));
        })
        .expect("held channel"),
    );

    // Deliver only the first member: the peer shows none of the transaction.
    b.receive(ops_msg(cb, vec![ops[0].clone()])).unwrap();
    let doc = b.document(cb).expect("room");
    assert!(doc.get(b"x").is_none() && doc.get(b"y").is_none());

    // The remaining member commits the whole transaction.
    b.receive(ops_msg(cb, vec![ops[1].clone()])).unwrap();
    let doc = b.document(cb).expect("room");
    assert_eq!(int(doc, b"x"), 1);
    assert_eq!(int(doc, b"y"), 2);
}

#[test]
fn begin_and_commit_atomic_group_edits_on_a_channel() {
    let mut a = ClientSession::new(cid(1));
    let mut b = ClientSession::new(cid(2));
    let (ca, _) = a.subscribe(ROOM_A);
    let (cb, _) = b.subscribe(ROOM_A);

    a.begin_atomic(ca).expect("held channel");
    // Edits accumulate while recording; each emits an empty op batch.
    assert!(ops_of(a.edit(ca, |c| c.register(b"x", Scalar::Int(1))).unwrap()).is_empty());
    assert!(ops_of(a.edit(ca, |c| c.register(b"y", Scalar::Int(2))).unwrap()).is_empty());
    let ops = ops_of(a.commit_atomic(ca).expect("held channel"));
    assert_eq!(ops.len(), 2);

    // Split delivery: the peer stays empty until the whole group lands.
    b.receive(ops_msg(cb, vec![ops[0].clone()])).unwrap();
    assert!(b.document(cb).unwrap().get(b"x").is_none());
    b.receive(ops_msg(cb, vec![ops[1].clone()])).unwrap();
    let doc = b.document(cb).expect("room");
    assert_eq!(int(doc, b"x"), 1);
    assert_eq!(int(doc, b"y"), 2);
}

#[test]
fn atomic_channel_methods_reject_an_unheld_channel() {
    let mut s = ClientSession::new(cid(1));
    assert!(s.begin_atomic(Channel(9)).is_none());
    assert!(s.commit_atomic(Channel(9)).is_none());
}
