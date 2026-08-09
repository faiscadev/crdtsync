//! A room's doc-ACL authority root is established only over a room that has reached a
//! sequence (C99).
//!
//! The creator owns `/`: every doc-ACL grant in the room confers authority only if it
//! traces back to it, and the room's denies decide nothing without it. It is the
//! heaviest authority the server hands out, and it was established by *any* `main`
//! write whose ingest answered `Ok` — which an empty batch does. The ingest
//! materialises the room, appends nothing, persists nothing and returns `Ok(vec![])`,
//! so the first authenticated actor to send a no-op `Ops` frame at an unestablished
//! room owned it, having authored no byte the room retains. That is reserve-by-no-op,
//! the shape C23 (#389) closed one tier down for the replica-identity claim, and the
//! sharper of the two: a claim holds one id space, a creator holds the document.
//!
//! The rule that closes it is stated once, in `may_stand_as_root`, at the one seam
//! every *establishment* composes through — `Hub::ensure_creator`, which a client's
//! write, a peer's `Replicate` and the metadata-only `ReplicateMeta` all reach — rather
//! than bolted onto the client seam with the replication path left judging by another.
//! So the condition is about the **room**: no actor roots a room at sequence zero.
//! *Sequence*, not retention: at this seam they are one fact, because the sequence is
//! this node's own count of what it accepted.
//!
//! **Where the rule stops is measured rather than argued.** A root also arrives *with a
//! state* — a snapshot install, and the record read back off the store — and there the
//! sequence came in beside the root instead of being counted here. Extending the rule
//! to those two refuses nothing (a sender that wants the root names a nonzero floor)
//! and breaks two things: content installed at floor zero comes up rootless, and a room
//! a state transfer left at zero loses its legitimate root on the next reload. Both are
//! pinned below, as the reason the rule ends where it does.
//!
//! Which leaves a write the room's dedup swallows whole still rooting a room that
//! already holds ops, and that is deliberate rather than residue — see
//! `an_authenticated_resend_roots_a_room_that_already_retains_a_write`. It is the shape
//! C55 (#397) built `ReplicateMeta` to replicate, and refusing it would buy nothing:
//! whoever may resend a room's ops may land one fresh op and root it that way.
//!
//! The negatives here read as an absence — no root where one used to land. Each is
//! paired with the positive that shows the same seam still establishes a root when the
//! room has reached a sequence, so a seam that simply stopped working would fail this
//! file.

use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{AclEffect, ClientId, Document, Message, Op, Scalar};
use crdtsync_server::acl::{actor_key, Acl};
use crdtsync_server::membership::Membership;
use crdtsync_server::store::Store;
use crdtsync_server::{
    ConnId, Hub, Identity, ManualClock, Registry, RoomLog, RoomMeta, SchemaRegistry, StaticTokens,
    StoredOp,
};

const CH: Channel = Channel(0);
const ROOM: &[u8] = b"room-a";
const N: usize = 3;
const APP: &[u8] = b"collab";

/// The key alice denies bob, and one she leaves readable.
const SECRET: &[u8] = b"secret";
const OPEN: &[u8] = b"open";

/// Read to any authenticated actor, so the doc-ACL deny below is the only thing that
/// can narrow bob's read — and the narrowing needs the room's root to be alice.
const SCHEMA: &str = r#"{ "schema": "collab", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "auth": {
        "roles": ["editor"],
        "grants": [
            { "allow": "read",  "to": "authenticated", "on": "/" },
            { "allow": "write", "to": "authenticated", "on": "/" }
        ]
    } }"#;

const CLUSTER_SECRET: &[u8] = b"peer-plane-cluster-secret-for-tests";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn sub(room: &[u8]) -> Message {
    Message::Subscribe {
        channel: CH,
        room: room.to_vec(),
        branch: Vec::new(),
        zone: Vec::new(),
        last_seen_seq: 0,
    }
}

fn empty_ops() -> Message {
    Message::Ops {
        channel: CH,
        ops: Vec::new(),
    }
}

fn refusal(m: &Message) -> bool {
    matches!(m, Message::Error { .. } | Message::OpsRejected { .. })
}

// --- the client write seam ---

/// A connection on `r` declaring `client`, authenticated as `actor` — the default
/// `AllowAll` verifier adopts the credential bytes as the actor — subscribed to `ROOM`.
fn writer(r: &mut Registry, client: u8, actor: &[u8]) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
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
    assert!(r.deliver(id, sub(ROOM)));
    r.take_outbox(id);
    id
}

/// The same, over a connection the deployment admitted with no credential, under a
/// minted `anon:`-prefixed actor — one no seam accepts as a root.
fn anonymous_writer(r: &mut Registry, client: u8) -> ConnId {
    let id = r.connect_authenticated(Identity::new(b"anon:ghost".to_vec()));
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
        }
    ));
    assert!(r.deliver(id, sub(ROOM)));
    r.take_outbox(id);
    id
}

/// A one-op batch setting `key`, authored under `client`'s own fresh replica.
fn batch(client: u8, key: &[u8]) -> Vec<Op> {
    Document::new(cid(client)).transact(|tx| tx.set(key, Scalar::Int(1)))
}

#[test]
fn an_empty_ops_frame_does_not_root_the_room_it_opens() {
    // The defect, at the wire: an authenticated actor sends a batch with no ops at a
    // room nobody has established, and owns `/` of it for good.
    let mut r = Registry::new(cid(0xFF));
    let mallory = writer(&mut r, 1, b"mallory");
    assert!(r.deliver(mallory, empty_ops()));

    assert_eq!(r.hub().seq(ROOM), 0, "the no-op batch retained nothing");
    assert_eq!(
        r.hub().room_creator(ROOM),
        None,
        "a batch that put nothing in the room took its authority root",
    );

    // And the reservation does not outlive the room's first real write: the actor that
    // actually establishes the room gets it.
    let alice = writer(&mut r, 2, b"alice");
    assert!(r.deliver(
        alice,
        Message::Ops {
            channel: CH,
            ops: batch(2, OPEN)
        }
    ));
    assert_eq!(
        r.hub().room_creator(ROOM).as_deref(),
        Some(b"alice".as_slice()),
    );
}

#[test]
fn an_empty_ops_frame_is_not_refused() {
    // The fix is a narrower establishment rule, not a rejection. An inert edit frames
    // an `Ops` batch with no ops, and refusing one would disconnect an honest client
    // from a room it merely made a no-op edit in.
    let mut r = Registry::new(cid(0xFF));
    let alice = writer(&mut r, 1, b"alice");
    assert!(
        r.deliver(alice, empty_ops()),
        "the no-op write kept the connection",
    );
    let out = r.take_outbox(alice);
    assert!(
        !out.iter().any(refusal),
        "the no-op write was refused: {out:?}"
    );

    // And the same connection goes on to root the room with its next real write.
    assert!(r.deliver(
        alice,
        Message::Ops {
            channel: CH,
            ops: batch(1, OPEN)
        }
    ));
    assert_eq!(
        r.hub().room_creator(ROOM).as_deref(),
        Some(b"alice".as_slice()),
    );
}

#[test]
fn an_authenticated_resend_roots_a_room_that_already_retains_a_write() {
    // The scope of the rule, pinned: it is about the room, not about the batch. A
    // write the dedup swallows whole still roots a room that holds ops — the state
    // C55's `ReplicateMeta` exists to replicate. Refusing it would buy nothing, since
    // whoever can resend the room's ops can land one fresh op and root it that way,
    // and there is no form of "this batch landed" the `ReplicateMeta` seam could state.
    let mut r = Registry::new(cid(0xFF));
    let ghost = anonymous_writer(&mut r, 1);
    let ops = batch(1, OPEN);
    assert!(r.deliver(
        ghost,
        Message::Ops {
            channel: CH,
            ops: ops.clone()
        }
    ));
    assert_eq!(
        r.hub().room_creator(ROOM),
        None,
        "an anonymous actor roots nothing",
    );

    let alice = writer(&mut r, 1, b"alice");
    let before = r.hub().seq(ROOM);
    assert!(r.deliver(alice, Message::Ops { channel: CH, ops }));
    assert_eq!(
        r.hub().seq(ROOM),
        before,
        "the resend landed no op — the room deduped it whole",
    );
    assert_eq!(
        r.hub().room_creator(ROOM).as_deref(),
        Some(b"alice".as_slice()),
        "a room that retains a write is rooted by the resend",
    );
}

#[test]
fn an_empty_frame_does_not_take_a_rootless_rooms_authority() {
    // The room-level condition alone leaves the defect standing where it bites hardest.
    // A room that holds content and has no root is a real state, not a corner: an
    // anonymous establishing commit leaves one (C55 route 1), and so does a replica
    // whose best-effort metadata write was lost (route 2). There the room has long
    // reached a sequence, so a frame carrying *nothing* would satisfy the room rule and
    // take `/` over content its sender had no part in — needing no knowledge of the
    // room at all, unlike the resend the rule does admit.
    let mut r = Registry::new(cid(0xFF));
    let ghost = anonymous_writer(&mut r, 1);
    assert!(r.deliver(
        ghost,
        Message::Ops {
            channel: CH,
            ops: batch(1, OPEN)
        }
    ));
    assert_eq!(
        r.hub().room_creator(ROOM),
        None,
        "an anonymous actor roots nothing, so the room holds content and no root",
    );
    let before = r.hub().seq(ROOM);
    assert!(before > 0, "the room holds content");

    let mallory = writer(&mut r, 2, b"mallory");
    assert!(r.deliver(mallory, empty_ops()));
    assert_eq!(r.hub().seq(ROOM), before, "the frame carried nothing");
    assert_eq!(
        r.hub().room_creator(ROOM),
        None,
        "a frame carrying nothing took the room's authority",
    );

    // And the room's own author still can: the refusal narrows who establishes the
    // root, it does not lock the room out of ever having one.
    let alice = writer(&mut r, 3, b"alice");
    assert!(r.deliver(
        alice,
        Message::Ops {
            channel: CH,
            ops: batch(3, SECRET)
        }
    ));
    assert_eq!(
        r.hub().room_creator(ROOM).as_deref(),
        Some(b"alice".as_slice()),
    );
}

#[test]
fn a_batch_of_ops_no_replica_can_hold_presents_nothing_either() {
    // What makes "presented an op" more than a formality: a batch whose ops are
    // inadmissible lands nothing and costs its sender nothing to fabricate, so if it
    // reached the establishment seam it would be the empty frame under another name.
    // It does not — the session refuses such a batch recoverably before the ingest —
    // and that ordering is what this pins, since a reader has no other way to tell
    // whether the condition above can be satisfied for free.
    let mut r = Registry::new(cid(0xFF));
    let ghost = anonymous_writer(&mut r, 1);
    assert!(r.deliver(
        ghost,
        Message::Ops {
            channel: CH,
            ops: batch(1, OPEN)
        }
    ));
    let before = r.hub().seq(ROOM);
    assert!(before > 0 && r.hub().room_creator(ROOM).is_none());

    // An op whose stamp names a client other than its author: admissible-forever's
    // complement, a pure function of the op, so every replica refuses it.
    let mallory = writer(&mut r, 2, b"mallory");
    let mut ops = batch(2, SECRET);
    for op in ops.iter_mut() {
        op.stamp.client = cid(9);
    }
    assert!(r.deliver(mallory, Message::Ops { channel: CH, ops }));
    let out = r.take_outbox(mallory);
    assert!(
        out.iter().any(refusal),
        "the batch was admitted rather than refused: {out:?}",
    );
    assert_eq!(r.hub().seq(ROOM), before, "it landed nothing");
    assert_eq!(
        r.hub().room_creator(ROOM),
        None,
        "a batch no replica can hold took the room's authority",
    );
}

// --- what the reservation was worth: authority over the document ---

/// A single node holding `SCHEMA` and an abstaining deployment ACL, so the schema and
/// doc-ACL tiers alone decide every read.
fn schema_node() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, SCHEMA.as_bytes(), b"").unwrap();
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    let mut t = StaticTokens::new();
    for actor in [
        b"alice".as_slice(),
        b"bob".as_slice(),
        b"mallory".as_slice(),
    ] {
        t.insert_identity(
            [b"t-".as_slice(), actor].concat(),
            Identity::with_claims(actor.to_vec(), Vec::new(), Vec::new()),
        );
    }
    r.set_verifier(Box::new(t));
    r.set_authorizer(Box::new(Acl::new()));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

/// Hello + Auth a connection as `credential`, declaring `{APP, 1}`.
fn hello_auth(r: &mut Registry, client: u8, credential: &str) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: APP.to_vec(),
            schema_version: 1,
            codecs: Vec::new(),
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: credential.as_bytes().to_vec(),
        }
    ));
    r.take_outbox(id);
    id
}

/// The room's establishing commit: two keys and a `Deny(Read)` for **mallory** at
/// `/secret`, naming alice as its grantor.
///
/// The denied actor is the squatter deliberately. A deny beats the schema tier's static
/// grant whoever wrote it — `acl::denies_path` never drops one for an unrooted grantor —
/// so a deny aimed at a third party decides the same either way and would pin nothing
/// about the root. What only the root has is the *exemption*: the creator auto-owns `/`,
/// so no deny in the room reaches it. Aiming the deny at mallory is therefore the one
/// reading that moves when the reservation succeeds.
fn establishing_batch() -> Vec<Op> {
    Document::new(cid(2)).transact(|tx| {
        tx.register(OPEN, Scalar::Int(1));
        tx.register(SECRET, Scalar::Int(2));
        tx.acl().grant(
            AclSubject::Actor(actor_key(b"mallory")),
            AclGrant::Capability(Capability::Read),
            AclEffect::Deny,
            encode_path(&[SECRET]),
            actor_key(b"alice"),
        );
    })
}

/// Subscribe `credential` on a fresh connection and fold whatever the catch-up served
/// it into one document.
fn reads(r: &mut Registry, client: u8, credential: &str) -> Document {
    let id = hello_auth(r, client, credential);
    assert!(r.deliver(id, sub(ROOM)));
    let out = r.take_outbox(id);
    assert!(
        !out.iter().any(|m| matches!(m, Message::Error { .. })),
        "the schema's authenticated read grant admits the reader: {out:?}",
    );
    let mut view = Document::new(cid(client));
    let mut served = false;
    for msg in out {
        match msg {
            Message::Ops { ops, .. } => {
                served = true;
                for op in &ops {
                    view.apply(op);
                }
            }
            Message::Snapshot { state, .. } => {
                served = true;
                view = Document::decode_state(&state).expect("a served snapshot decodes");
            }
            _ => {}
        }
    }
    assert!(served, "the reader was served a catch-up");
    view
}

#[test]
fn a_no_op_reservation_does_not_take_the_documents_authority_from_its_author() {
    // The consequence the root carries, rather than the field. The creator auto-owns
    // `/`, so it is exempt from every deny the room holds — which is the privilege a
    // no-op reservation stole. Mallory reserves the room with a batch of no ops, alice
    // establishes it and denies mallory `/secret`, and mallory reads: denied, because
    // the room's root is its author. Had the reservation stood, mallory would own `/`
    // and read straight through alice's deny.
    let mut r = schema_node();
    let mallory = hello_auth(&mut r, 1, "t-mallory");
    assert!(r.deliver(mallory, sub(ROOM)));
    r.take_outbox(mallory);
    assert!(r.deliver(mallory, empty_ops()));

    let alice = hello_auth(&mut r, 2, "t-alice");
    assert!(r.deliver(alice, sub(ROOM)));
    r.take_outbox(alice);
    assert!(r.deliver(
        alice,
        Message::Ops {
            channel: CH,
            ops: establishing_batch()
        }
    ));
    let out = r.take_outbox(alice);
    assert!(
        !out.iter().any(refusal),
        "alice's write was refused: {out:?}"
    );

    assert_eq!(
        r.hub().room_creator(ROOM).as_deref(),
        Some(b"alice".as_slice()),
        "the room's authority root is the actor that established it",
    );
    let view = reads(&mut r, 7, "t-mallory");
    assert!(
        view.get(OPEN).is_some(),
        "mallory keeps the read the schema grants her",
    );
    assert!(
        view.get(SECRET).is_none(),
        "the squatter read through the deny, so it holds `/` after all",
    );
}

#[test]
fn a_later_writer_does_not_displace_the_root() {
    // The other half of the guard the rule shares: set-once. A room the reservation
    // failed to take is rooted by its first landed write and stays there, so the
    // squatter's *real* write afterwards inherits nothing.
    let mut r = Registry::new(cid(0xFF));
    let mallory = writer(&mut r, 1, b"mallory");
    assert!(r.deliver(mallory, empty_ops()));

    let alice = writer(&mut r, 2, b"alice");
    assert!(r.deliver(
        alice,
        Message::Ops {
            channel: CH,
            ops: batch(2, OPEN)
        }
    ));
    assert_eq!(
        r.hub().room_creator(ROOM).as_deref(),
        Some(b"alice".as_slice()),
    );

    assert!(r.deliver(
        mallory,
        Message::Ops {
            channel: CH,
            ops: batch(1, SECRET)
        }
    ));
    assert_eq!(
        r.hub().room_creator(ROOM).as_deref(),
        Some(b"alice".as_slice()),
        "a later write displaced the room's root",
    );
}

// --- the replication seams ---

fn members() -> String {
    (0..7)
        .map(|i| format!("10.0.0.{i}:9000"))
        .collect::<Vec<_>>()
        .join(",")
}

fn membership_for(self_addr: &str) -> Membership {
    Membership::from_static_config(None, Some(self_addr), &members(), N).unwrap()
}

/// A room this node holds as a *follower* — in the replica set but not its head, so a
/// leader's frames for it apply here.
fn followed_room(m: &Membership) -> Vec<u8> {
    for i in 0..1_000_000 {
        let room = format!("room-{i}").into_bytes();
        let r = m.replicas_for(&room);
        if r.len() >= 2 && !m.is_self(&r[0]) && r.iter().skip(1).any(|n| m.is_self(n)) {
            return room;
        }
    }
    panic!("no room places self as a follower");
}

/// A follower node, and a peer link on it admitted as one of `room`'s other replicas.
fn follower(room: &[u8]) -> (Registry, ConnId) {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for("10.0.0.6:9000"));
    r.set_cluster_secret(CLUSTER_SECRET.to_vec());
    let node = r
        .membership()
        .and_then(|m| m.replicas_for(room).into_iter().find(|n| !m.is_self(n)))
        .expect("the room has another replica");
    let peer = r.connect();
    assert!(r.deliver(
        peer,
        Message::PeerAuth {
            node: node.as_bytes().to_vec(),
            secret: CLUSTER_SECRET.to_vec(),
        },
    ));
    (r, peer)
}

/// A leader's `Replicate` for `room`'s main stream carrying `ops` and asserting
/// `creator` as the room's root.
fn replicate(room: &[u8], ops: Vec<Op>, creator: &[u8]) -> Message {
    Message::Replicate {
        room: room.to_vec(),
        branch: b"main".to_vec(),
        ops,
        base_seq: 0,
        epoch: 1,
        creator: Some(creator.to_vec()),
        governing: None,
        max_op_version: None,
    }
}

fn replicate_meta(room: &[u8], creator: &[u8]) -> Message {
    Message::ReplicateMeta {
        room: room.to_vec(),
        epoch: 1,
        creator: Some(creator.to_vec()),
    }
}

#[test]
fn an_empty_replicate_frame_does_not_root_the_replica_it_creates() {
    // The same reserve-by-no-op one seam over: a `Replicate` carrying no ops is
    // ingested exactly as a client's empty batch is, creating the room here, and its
    // asserted root landed on a replica that retains nothing.
    let m = membership_for("10.0.0.6:9000");
    let room = followed_room(&m);
    let (mut r, peer) = follower(&room);

    assert!(r.deliver(peer, replicate(&room, Vec::new(), b"mallory")));
    assert!(
        r.hub().holds_room(&room),
        "the empty frame reached the ingest and created the room",
    );
    assert_eq!(r.hub().seq(&room), 0, "the frame carried nothing");
    assert_eq!(
        r.hub().room_creator(&room),
        None,
        "a frame with no ops beneath it rooted the replica",
    );
}

#[test]
fn a_replicate_frame_that_lands_ops_roots_the_replica() {
    // The other side of the same seam: C29's root-rides-the-frame still holds, so the
    // rule above narrowed the establishment rather than stopping it.
    let m = membership_for("10.0.0.6:9000");
    let room = followed_room(&m);
    let (mut r, peer) = follower(&room);

    assert!(r.deliver(peer, replicate(&room, batch(1, OPEN), b"alice")));
    assert_eq!(r.hub().seq(&room), 1);
    assert_eq!(
        r.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
    );
}

#[test]
fn a_replicate_meta_does_not_root_a_replica_that_retains_nothing() {
    // `ReplicateMeta` is the arm with no batch at all, which is why the condition is
    // stated about the room. It is inert for a room this node does not hold, so the
    // shape that reaches the rule is a room held and empty — which an empty
    // `Replicate` leaves behind.
    let m = membership_for("10.0.0.6:9000");
    let room = followed_room(&m);
    let (mut r, peer) = follower(&room);

    assert!(r.deliver(peer, replicate(&room, Vec::new(), b"mallory")));
    assert!(
        r.hub().holds_room(&room),
        "the empty frame created the room"
    );
    assert!(r.deliver(peer, replicate_meta(&room, b"mallory")));
    assert_eq!(
        r.hub().room_creator(&room),
        None,
        "the metadata frame rooted a replica that retains nothing",
    );
}

#[test]
fn a_replicate_meta_roots_a_replica_that_holds_the_rooms_ops() {
    // C55's repair still lands: a follower converged on the room's ops and missing its
    // root is re-rooted by the metadata frame.
    let m = membership_for("10.0.0.6:9000");
    let room = followed_room(&m);
    let (mut r, peer) = follower(&room);

    assert!(r.deliver(
        peer,
        Message::Replicate {
            room: room.to_vec(),
            branch: b"main".to_vec(),
            ops: batch(1, OPEN),
            base_seq: 0,
            epoch: 1,
            creator: None,
            governing: None,
            max_op_version: None,
        }
    ));
    assert_eq!(r.hub().room_creator(&room), None);
    assert!(r.deliver(peer, replicate_meta(&room, b"alice")));
    assert_eq!(
        r.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
    );
}

// --- the install and store seams ---

/// A whole-replica snapshot of a room holding one write.
fn state_with_a_write() -> Vec<u8> {
    let mut hub = Hub::new(cid(0xFF));
    hub.ingest(ROOM, batch(1, OPEN), None).unwrap();
    hub.export_room(ROOM).expect("the room exists")
}

/// A snapshot of a room holding nothing.
fn empty_state() -> Vec<u8> {
    Document::new(cid(0xFF)).encode_state()
}

#[test]
fn a_state_installed_at_sequence_zero_still_carries_the_frames_root() {
    // Where the rule stops, and why — measured, not argued. This seam takes both the
    // content and the floor it lands at from the same frame, so refusing a root at
    // sequence zero refuses nothing (a sender that wants it names a nonzero floor) and
    // lands a room full of content with no authority its tuples are decided under: the
    // C29 hole, reached from the other side and strictly worse than the reservation.
    let mut hub = Hub::new(cid(0xFF));
    hub.install_snapshot(ROOM, &state_with_a_write(), 0, Some(b"alice".to_vec()))
        .expect("the state decodes");
    assert!(hub.get(ROOM, OPEN).is_some(), "the content installed");
    assert_eq!(
        hub.room_creator(ROOM).as_deref(),
        Some(b"alice".as_slice()),
        "content came up rootless, so every deny in it decides nothing",
    );
}

#[test]
fn a_snapshot_landing_at_sequence_zero_leaves_a_standing_root_alone() {
    // The one shape that still holds a root over a room at sequence zero, and it must:
    // dropping the standing root here would let a peer strip a room's authority — and
    // with it every deny in the room — by sending an empty state at sequence zero.
    let mut hub = Hub::new(cid(0xFF));
    hub.ingest(ROOM, batch(1, OPEN), None).unwrap();
    assert!(hub.ensure_creator(ROOM, b"alice"));
    hub.install_snapshot(ROOM, &empty_state(), 0, None)
        .expect("the state decodes");
    assert_eq!(hub.room_creator(ROOM).as_deref(), Some(b"alice".as_slice()));
}

/// A stored record for `ROOM` naming `actor` as its root, over `ops`.
fn stored(actor: &[u8], ops: Vec<Op>) -> RoomLog {
    RoomLog {
        ops: ops.into_iter().map(|op| StoredOp::new(op, None)).collect(),
        meta: Some(RoomMeta {
            governing: None,
            max_op_version: None,
            creator: Some(actor.to_vec()),
            client_actors: Vec::new(),
        }),
        ..RoomLog::default()
    }
}

#[test]
#[cfg_attr(miri, ignore)] // drives the room store on the filesystem
fn a_durable_root_survives_a_reload_of_a_room_left_at_sequence_zero() {
    // The other seam the rule stops short of, for the same reason read one restart
    // later. A state transfer landing at sequence zero leaves the standing root alone
    // — and persists it — so a load that judged the record by the room's sequence would
    // drop a root the node itself established over content, handing `/` to whoever
    // writes next.
    let dir = std::env::temp_dir().join(format!("c99-reload-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    {
        let mut hub = Hub::new(cid(0xFF));
        hub.attach_store(Store::open(&dir).expect("the store opens"));
        hub.ingest(ROOM, batch(1, OPEN), None)
            .expect("the write lands");
        assert!(hub.ensure_creator(ROOM, b"alice"));
        // The install lands *content* at floor zero, so what a reload would drop the
        // root over is a room holding a whole document — the inert-deny harm, not a
        // bookkeeping one. An empty state here would prove only that a root over
        // nothing survives a restart, which is the reservation this unit refuses.
        hub.install_snapshot(ROOM, &state_with_a_write(), 0, None)
            .expect("the state decodes");
        assert_eq!(hub.seq(ROOM), 0);
        assert!(hub.get(ROOM, OPEN).is_some(), "the room holds content");
    }
    let hub = Hub::from_rooms(
        cid(0xFF),
        Store::open(&dir)
            .expect("the store opens")
            .load()
            .expect("it loads"),
    )
    .expect("the record loads");
    let root = hub.room_creator(ROOM);
    let content = hub.get(ROOM, OPEN).is_some();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(content, "the room came back holding its content");
    assert_eq!(
        root.as_deref(),
        Some(b"alice".as_slice()),
        "the reload dropped a root over content the room still holds, leaving every deny in it inert",
    );
}

#[test]
fn a_stored_root_over_a_room_that_retains_a_write_comes_back() {
    let hub = Hub::from_rooms(
        cid(0xFF),
        vec![(ROOM.to_vec(), stored(b"alice", batch(1, OPEN)))],
    )
    .expect("the record loads");
    assert_eq!(hub.seq(ROOM), 1);
    assert_eq!(hub.room_creator(ROOM).as_deref(), Some(b"alice".as_slice()));
}
