//! A replica identity belongs to the actor that first wrote under it.
//!
//! A `ClientId` is declared, not authenticated: Hello names one and the op gate
//! only checks that the batch is *self-consistent* with the declaration. That is
//! enough for a connection to author under any identity it names, and the one
//! place that does lasting damage is the mint. A stamp names its author and the
//! mint counts on from its author's whole id-space high-water, so an op admitted
//! under a victim's identity moves *that* replica's floor — and one op stamped at
//! [`LAMPORT_STATE_CEILING`], a perfectly legal position, spends the space
//! outright. The victim can then never mint again: in any partition, on every
//! replica that folded the op, and in the durable snapshot. The refusal is
//! fail-closed rather than a re-issued live id, so nothing diverges; the replica
//! simply stops being able to write.
//!
//! The binding that closes it is server-side, per room, and set-once: the first
//! authenticated actor to write under a replica identity claims it, and no other
//! actor may author under it afterwards. Per room, because the damage is — an id
//! space is a property of a document. Set-once by the writer's own first batch,
//! because nothing else knows the pairing: a stored op carries the per-device
//! `ClientId`, never the credential actor behind it, which is why the claim is
//! recorded rather than derived — the same reason the room's creator is.

use crdtsync_core::client::ClientSession;
use crdtsync_core::protocol::Channel;
use crdtsync_core::stamp::LAMPORT_STATE_CEILING;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Op, Scalar};
use crdtsync_server::{ConnId, Hub, Registry, RoomLog, RoomMeta};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-a";
/// The victim's declared connection id; channel 0 authors under it unchanged.
const VICTIM: u8 = 1;
const ATTACKER: u8 = 2;

fn is_violation(m: &Message) -> bool {
    matches!(
        m,
        Message::Error {
            code: ErrorCode::ProtocolViolation,
            ..
        }
    )
}

/// A connected connection declaring `client` and authenticated as `actor` — the
/// default `AllowAll` verifier adopts the credential bytes as the actor, so the
/// credential *is* the actor here.
fn hello(r: &mut Registry, client: ClientId, actor: &[u8]) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client,
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: actor.to_vec()
        }
    ));
    r.take_outbox(id);
    id
}

fn subscribe(r: &mut Registry, id: ConnId, channel: Channel, room: &[u8]) {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel,
            room: room.to_vec(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq: 0,
        }
    ));
    r.take_outbox(id);
}

/// A one-op `Message::Ops` on `channel` authored under `client`.
fn ops_frame(channel: Channel, client: ClientId, key: &[u8]) -> Message {
    Message::Ops {
        channel,
        ops: Document::new(client).transact(|tx| tx.set(key, Scalar::Int(1))),
    }
}

/// The same, with the op's stamp moved to the last id of the space — the position
/// that spends its author's mint for good, and a legal one, so every admissibility
/// check in the stack takes it.
fn ceiling_frame(channel: Channel, client: ClientId, key: &[u8]) -> Message {
    let Message::Ops { channel, mut ops } = ops_frame(channel, client, key) else {
        unreachable!("ops_frame builds an Ops")
    };
    for op in ops.iter_mut() {
        op.stamp.lamport = LAMPORT_STATE_CEILING;
    }
    Message::Ops { channel, ops }
}

/// Whether `client` still holds an id to mint in the room's state, read the way a
/// replica adopting the room's snapshot would — in the root partition and in a
/// zone alike, since the mint floor is global.
fn victim_can_still_mint(r: &Registry, client: ClientId) -> bool {
    let state = r.hub().export_room(ROOM).expect("the room exists");
    let doc = Document::decode_state_as(client, 0, &state).expect("the snapshot decodes");
    doc.can_mint(None) && doc.can_mint(Some(0))
}

fn ops_of(msgs: &[Message]) -> Vec<Op> {
    msgs.iter()
        .flat_map(|m| match m {
            Message::Ops { ops, .. } => ops.clone(),
            _ => Vec::new(),
        })
        .collect()
}

// --- the gate ---

#[test]
fn an_op_under_another_actors_replica_identity_cannot_spend_its_mint() {
    let mut r = Registry::new(cid(0xFF));
    let victim = cid(VICTIM);

    // The victim writes once, which is what claims its replica identity.
    let alice = hello(&mut r, victim, b"alice");
    subscribe(&mut r, alice, Channel(0), ROOM);
    assert!(r.deliver(alice, ops_frame(Channel(0), victim, b"mine")));
    assert!(victim_can_still_mint(&r, victim));

    // A second actor declares the victim's id at Hello — nothing stops it — and
    // authors a batch that is legal in every other respect, at the top of the space.
    let mallory = hello(&mut r, victim, b"mallory");
    subscribe(&mut r, mallory, Channel(0), ROOM);
    r.take_outbox(alice);
    r.deliver(mallory, ceiling_frame(Channel(0), victim, b"planted"));

    let reply = r.take_outbox(mallory);
    assert!(
        reply.iter().any(is_violation),
        "the plant was admitted: {reply:?}"
    );
    assert!(
        ops_of(&r.take_outbox(alice)).is_empty(),
        "the plant reached the room's other subscribers"
    );
    assert!(
        victim_can_still_mint(&r, victim),
        "one op spent the victim's id space, room-wide and in the snapshot"
    );

    // And the victim goes on writing — the damage is a write-lock, so the proof is
    // that a later mint still lands.
    assert!(r.deliver(alice, ops_frame(Channel(0), victim, b"after")));
    assert!(r
        .take_outbox(alice)
        .iter()
        .all(|m| !is_violation(m) && !matches!(m, Message::OpsRejected { .. })));
    assert!(victim_can_still_mint(&r, victim));
}

#[test]
fn a_derived_channel_identity_is_claimed_as_its_own() {
    // Every channel of a connection authors under `for_channel` of the declared id,
    // so a session's second channel holds a replica identity the base does not name.
    // Claiming the base alone would leave every derived identity open, and they are
    // as public as the base — a peer reads them off the ops.
    let mut r = Registry::new(cid(0xFF));
    let victim = cid(VICTIM);
    let derived = victim.for_channel(1);

    let alice = hello(&mut r, victim, b"alice");
    subscribe(&mut r, alice, Channel(1), ROOM);
    assert!(r.deliver(alice, ops_frame(Channel(1), derived, b"mine")));
    assert_eq!(r.hub().client_actor(ROOM, derived), Some(&b"alice"[..]));

    // Mallory declares a *base* whose channel-1 identity is the victim's — it can
    // simply declare the victim's base and use the same channel number.
    let mallory = hello(&mut r, victim, b"mallory");
    subscribe(&mut r, mallory, Channel(1), ROOM);
    r.deliver(mallory, ceiling_frame(Channel(1), derived, b"planted"));
    assert!(r.take_outbox(mallory).iter().any(is_violation));
    assert!(victim_can_still_mint(&r, derived));
}

#[test]
fn an_unclaimed_identity_is_admitted_and_a_reconnect_keeps_its_own() {
    // The claim must not cost an honest client anything: a first write establishes
    // it, and the same actor coming back on a new connection under the same declared
    // id writes exactly as before.
    let mut r = Registry::new(cid(0xFF));
    let client = cid(VICTIM);

    let first = hello(&mut r, client, b"alice");
    subscribe(&mut r, first, Channel(0), ROOM);
    assert!(r.deliver(first, ops_frame(Channel(0), client, b"a")));
    assert!(r.take_outbox(first).iter().all(|m| !is_violation(m)));

    let second = hello(&mut r, client, b"alice");
    subscribe(&mut r, second, Channel(0), ROOM);
    assert!(r.deliver(second, ops_frame(Channel(0), client, b"b")));
    assert!(r.take_outbox(second).iter().all(|m| !is_violation(m)));
    assert_eq!(r.hub().client_actor(ROOM, client), Some(&b"alice"[..]));

    // A different room is a different id space, so the claim there is unmade and
    // whoever writes first takes it.
    let other: &[u8] = b"room-b";
    let mallory = hello(&mut r, client, b"mallory");
    subscribe(&mut r, mallory, Channel(0), other);
    assert!(r.deliver(mallory, ops_frame(Channel(0), client, b"c")));
    assert!(r.take_outbox(mallory).iter().all(|m| !is_violation(m)));
    assert_eq!(r.hub().client_actor(other, client), Some(&b"mallory"[..]));
}

#[test]
fn an_anonymous_writer_claims_nothing() {
    // An anonymous actor is minted per connection, so a claim under one would refuse
    // that very client's next connection — the same reason the room's creator is
    // never anonymous. The cost is stated rather than hidden: an identity whose only
    // writer was anonymous is unprotected.
    let mut r = Registry::new(cid(0xFF));
    let client = cid(VICTIM);

    let first = hello(&mut r, client, b"anon:one");
    subscribe(&mut r, first, Channel(0), ROOM);
    assert!(r.deliver(first, ops_frame(Channel(0), client, b"a")));
    assert_eq!(r.hub().client_actor(ROOM, client), None);

    let second = hello(&mut r, client, b"anon:two");
    subscribe(&mut r, second, Channel(0), ROOM);
    assert!(r.deliver(second, ops_frame(Channel(0), client, b"b")));
    assert!(
        r.take_outbox(second).iter().all(|m| !is_violation(m)),
        "an anonymous claim wedged the client's own next connection"
    );
}

#[test]
fn a_claimed_identity_still_serves_the_actor_that_holds_it_after_a_peer_tries() {
    // The refused batch must leave the claim exactly as it was — a failed attempt
    // that displaced or cleared it would be the attack by another route.
    let mut r = Registry::new(cid(0xFF));
    let client = cid(VICTIM);

    let alice = hello(&mut r, client, b"alice");
    subscribe(&mut r, alice, Channel(0), ROOM);
    assert!(r.deliver(alice, ops_frame(Channel(0), client, b"a")));

    let mallory = hello(&mut r, client, b"mallory");
    subscribe(&mut r, mallory, Channel(0), ROOM);
    r.deliver(mallory, ops_frame(Channel(0), client, b"b"));
    assert!(r.take_outbox(mallory).iter().any(is_violation));
    assert_eq!(r.hub().client_actor(ROOM, client), Some(&b"alice"[..]));

    let third = hello(&mut r, client, b"alice");
    subscribe(&mut r, third, Channel(0), ROOM);
    assert!(r.deliver(third, ops_frame(Channel(0), client, b"c")));
    assert!(r.take_outbox(third).iter().all(|m| !is_violation(m)));
}

#[test]
fn an_empty_batch_claims_nothing() {
    // A claim records the *use* of an identity, and an empty batch authors no stamp.
    // Admitting one would let any actor the room lets write reserve an identity it
    // never intends to author under — the same lockout the claim exists to prevent,
    // reached from the other side.
    let mut r = Registry::new(cid(0xFF));
    let client = cid(VICTIM);

    let mallory = hello(&mut r, client, b"mallory");
    subscribe(&mut r, mallory, Channel(0), ROOM);
    assert!(r.deliver(
        mallory,
        Message::Ops {
            channel: Channel(0),
            ops: Vec::new()
        }
    ));
    assert_eq!(r.hub().client_actor(ROOM, client), None);

    let alice = hello(&mut r, client, b"alice");
    subscribe(&mut r, alice, Channel(0), ROOM);
    assert!(r.deliver(alice, ops_frame(Channel(0), client, b"a")));
    assert_eq!(r.hub().client_actor(ROOM, client), Some(&b"alice"[..]));
}

// --- the record ---

#[test]
fn a_restored_room_comes_up_holding_its_claims() {
    // The pairing is not derivable from the log — a stored op carries the per-device
    // `ClientId`, never the actor — so it is recorded, and a restart that lost it
    // would leave every identity in the room open to whoever writes next. The
    // anonymous rule decides a record read back exactly as it decides one
    // established live, so a stored anonymous claimant is dropped rather than
    // installed where it could never re-present.
    let claimed = cid(VICTIM);
    let anon_claimed = cid(ATTACKER);
    let log = RoomLog {
        meta: Some(RoomMeta {
            governing: None,
            max_op_version: None,
            creator: Some(b"alice".to_vec()),
            client_actors: vec![
                (claimed, b"alice".to_vec()),
                (anon_claimed, b"anon:ephemeral".to_vec()),
            ],
        }),
        ..RoomLog::default()
    };
    let hub = Hub::from_rooms(cid(0xFF), vec![(ROOM.to_vec(), log)]).expect("the room restores");
    assert_eq!(hub.client_actor(ROOM, claimed), Some(&b"alice"[..]));
    assert_eq!(hub.client_actor(ROOM, anon_claimed), None);
}

#[test]
fn a_live_write_records_the_claim_for_the_room_to_persist() {
    // The hub-level reading the persist path takes its bytes from, so the record and
    // the gate cannot drift apart.
    let mut r = Registry::new(cid(0xFF));
    let client = cid(VICTIM);
    let alice = hello(&mut r, client, b"alice");
    subscribe(&mut r, alice, Channel(0), ROOM);
    assert_eq!(r.hub().client_actor(ROOM, client), None);
    assert!(r.deliver(alice, ops_frame(Channel(0), client, b"a")));
    assert_eq!(r.hub().client_actor(ROOM, client), Some(&b"alice"[..]));
}

// --- what the client seat then reports ---

#[test]
fn a_spent_replica_reports_its_refusal_rather_than_editing_into_silence() {
    // The other half of the same defect, at the seat where it is felt: even where a
    // replica *is* spent — an honest 2^63 edits, or a snapshot adopted at the
    // ceiling — every mutation path returned the empty batch an inert edit returns.
    let mut session = ClientSession::new(cid(VICTIM));
    let (channel, _) = session.subscribe(ROOM).expect("a channel");
    let replica = session
        .channel_client(channel)
        .expect("the channel is held");

    let mut spent = Document::new(replica);
    spent.transact(|tx| tx.set(b"seed", Scalar::Int(1)));
    let at_ceiling = Document::decode_state_as(replica, 0, &spent.encode_state())
        .map(|mut d| {
            let mut plant = Document::new(replica)
                .transact(|tx| tx.set(b"planted", Scalar::Int(1)))
                .remove(0);
            plant.stamp.lamport = LAMPORT_STATE_CEILING;
            plant.id.seq = 99;
            assert!(d.apply(&plant));
            d
        })
        .expect("the snapshot decodes");
    assert!(!at_ceiling.can_mint(None));

    let doc = session.document_mut(channel).expect("the channel is held");
    let mut plant = Document::new(replica)
        .transact(|tx| tx.set(b"planted", Scalar::Int(1)))
        .remove(0);
    plant.stamp.lamport = LAMPORT_STATE_CEILING;
    plant.id.seq = 99;
    assert!(doc.apply(&plant));

    let frame = session.edit(channel, |tx| tx.set(b"k", Scalar::Int(1)));
    assert!(matches!(frame, Some(Message::Ops { ref ops, .. }) if ops.is_empty()));
    assert_eq!(session.mint_refused(channel), Some(true));
}
