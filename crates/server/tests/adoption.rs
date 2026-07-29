//! C25 — rooms are placed on *adopted* members, so a node cannot mint an id into a
//! replica set.
//!
//! C13 bound a peer link to a member and gated replication, leadership and durability
//! on it. Every one of those gates asks the same question — is the sender in
//! `replicas_for(room)` — and placement is HRW over the member set, a pure and
//! publicly computable function, so **which rooms a node replicates follows from its
//! node id**. The join path lets an unknown node introduce itself. So an admitted
//! member ground an id that HRW placed on the room it wanted, introduced itself under
//! it, and was inside that room's replica set: it could supersede the leader with a
//! forged epoch, push ops in, and report heads that made majority-ack release a client
//! `Accepted` for a write no majority held. Two lesser ways in had the same shape: the
//! reply half of a gossip round adopted members freely, and a minted member polluted
//! placement and quorum for the rooms it did *not* attack, so writes to them waited on
//! acks that never came.
//!
//! Learning a member and placing rooms on it are now two admissions. A gossip-learned
//! member is **pending** — dialed, probed and gossiped about, but in no room's replica
//! set and no room's quorum — until the cluster adopts it. Adoption cannot be a local
//! predicate, because placement must be identical on every node or the ring splits, so
//! a node records only what it knows first-hand — its **own dial** completed and the
//! transport authenticated the far end — and that claim rides the same anti-entropy
//! liveness does, attributed to the member the receiving link is bound to. A member is
//! placed once `ADOPTION_VERIFIERS` already-adopted **trust units** have verified it.
//!
//! Four rules make that hold. The adopted set is *derived* from the configured members
//! plus the evidence, never accumulated, so the ring is a function of state and not of
//! the order a node saw things in. A verifier is a **host**, because a certificate
//! names a host and a host mints unlimited node ids — and a member's own host is
//! excluded from its own count. An **inbound** link never verifies, however certified:
//! the member chooses when to dial in, so the vouch would be one it caused. And a
//! member is dialed at its **own id**, so no peer can decide where a later dial goes by
//! being the first to advertise an address.
//!
//! Two limits are pinned below as passing tests rather than left implied. With no
//! certificates configured a secret-holder binds a link to any member id it likes
//! (C13's own residual), so the bar is not a bar and what remains is reachability. And
//! under peer mTLS a member that owns a host owns every id under it — it answers at
//! each ground id and honest nodes verify one truthfully — so adoption bounds the mint
//! to the member's own host and no further; closing that needs the ring to weigh trust
//! units rather than ids (C27).
//!
//! These drive the registry and the membership in process (no sockets), so they are
//! deterministic and run under Miri.

use std::sync::Arc;

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, MemberState, Message, Op, Scalar};
use crdtsync_server::auth::Identity;
use crdtsync_server::gossip::GossipRoundOutcome;
use crdtsync_server::membership::{Membership, ADOPTION_VERIFIERS};
use crdtsync_server::placement::{Cluster, NodeId};
use crdtsync_server::{ConnId, ManualClock, Registry};

const CH: Channel = Channel(0);

/// One voucher can be the attacker itself, so the bar is more than one — the premise
/// the test below rests on, checked where it cannot drift.
const _: () = assert!(ADOPTION_VERIFIERS > 1);

/// The deployment's cluster secret — what every node in one cluster holds.
const SECRET: &[u8] = b"cluster-secret-of-at-least-32-bytes";

/// This node's advertise address in the in-process cluster below.
const SELF_ADDR: &str = "10.0.0.6:9000";

/// The per-room replication factor. Five, so a room's majority is three and one
/// follower's genuine ack is not on its own enough to release a write.
const N: usize = 5;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

/// The static member set every node's view is built from — larger than the
/// replication factor, so a node leads some rooms and holds no replica of others.
fn members_str() -> String {
    (0..9)
        .map(|i| format!("10.0.0.{i}:9000"))
        .collect::<Vec<_>>()
        .join(",")
}

fn membership_for(self_addr: &str) -> Membership {
    Membership::from_static_config(None, Some(self_addr), &members_str(), N).unwrap()
}

/// A clustered registry with the cluster secret configured.
fn registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for(SELF_ADDR));
    r.set_cluster_secret(SECRET.to_vec());
    r
}

/// A connection admitted to the peer plane as the member `node`, presenting no
/// certificate — what a deployment that has issued none has.
fn peer_as(r: &mut Registry, node: &NodeId) -> ConnId {
    let id = r.connect();
    assert!(
        r.deliver(
            id,
            Message::PeerAuth {
                node: node.as_bytes().to_vec(),
                secret: SECRET.to_vec(),
            },
        ),
        "the cluster secret admits a peer that names itself",
    );
    id
}

/// A connection admitted as `node` behind a verified client certificate that names
/// `node`'s own host — the binding C13 established, and the inbound half of a
/// verification.
fn certified_peer_as(r: &mut Registry, node: &NodeId) -> ConnId {
    let host =
        crdtsync_server::dial::member_host(node.as_bytes()).expect("the member names a host");
    let id = r.connect_cert_authenticated(Identity::new(b"peer".to_vec()), vec![host.into_bytes()]);
    assert!(r.deliver(
        id,
        Message::PeerAuth {
            node: node.as_bytes().to_vec(),
            secret: SECRET.to_vec(),
        },
    ));
    id
}

/// The frame a member sends to introduce itself: its own tuple, at its own address.
/// `verified` is the claim it makes about having reached that member — for a
/// self-introduction, a claim about itself.
fn introduces(node: &NodeId, verified: bool) -> Message {
    Message::Gossip {
        members: vec![(
            node.as_bytes().to_vec(),
            node.as_bytes().to_vec(),
            0,
            MemberState::Alive,
            verified,
        )],
    }
}

/// The wire tuples introducing each of `nodes` at its own address, carrying the
/// sender's `verified` claim about each — the payload the *reply* half of a gossip
/// round hands over, which introduces freely.
fn advertisements(nodes: &[&NodeId], verified: bool) -> Vec<crdtsync_core::MemberAdvert> {
    nodes
        .iter()
        .map(|node| {
            (
                node.as_bytes().to_vec(),
                node.as_bytes().to_vec(),
                0,
                MemberState::Alive,
                verified,
            )
        })
        .collect()
}

/// Have the cluster verify `node` to the bar, as `ADOPTION_VERIFIERS` members each
/// reporting their own completed link to it would.
fn cluster_verifies(r: &mut Registry, node: &NodeId) {
    for i in 0..ADOPTION_VERIFIERS {
        let voucher = NodeId::from(format!("10.0.0.{i}:9000"));
        r.merge_gossip(&voucher, advertisements(&[node], true));
    }
}

/// A room this node is the placement primary of — the room an attack from outside
/// the replica set targets.
fn room_self_leads(m: &Membership) -> Vec<u8> {
    (0..1_000_000)
        .map(|i| format!("room-{i}").into_bytes())
        .find(|room| m.is_primary_for(room))
        .expect("a room self leads")
}

/// An id HRW would place into `room`'s replica set once adopted — what a member
/// grinds for when it wants to hold a room it was never placed on. All on one host,
/// which is the mint space peer identity leaves open (C13): a certified member can
/// mint only on its own host.
fn minted_for(m: &Membership, room: &[u8]) -> NodeId {
    (0..1_000_000)
        .map(|i| NodeId::from(format!("10.9.9.9:{i}")))
        .find(|node| {
            let mut grown: Vec<NodeId> = m.adopted_members().to_vec();
            grown.push(node.clone());
            Cluster::new(grown).replicas(room, N).contains(node)
        })
        .expect("an id the room places on")
}

/// A replication frame for `room` at `epoch`, carrying one op.
fn replicate(d: &mut Document, room: &[u8], epoch: u64) -> Message {
    let ops = d.transact(|tx| tx.register(b"k", Scalar::Int(1)));
    Message::Replicate {
        room: room.to_vec(),
        branch: b"main".to_vec(),
        epoch,
        base_seq: 0,
        ops,
    }
}

/// An authenticated client on `r`, handshake drained.
fn client(r: &mut Registry) -> ConnId {
    let id = r.connect();
    r.deliver(
        id,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
        },
    );
    r.deliver(
        id,
        Message::Auth {
            credential: b"cred".to_vec(),
        },
    );
    r.take_outbox(id);
    id
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

fn write() -> Vec<Op> {
    doc(1).transact(|tx| tx.register(b"age", Scalar::Int(30)))
}

/// Whether `outbox` carries a write-ack `Accepted` on `CH`.
fn has_accepted(outbox: &[Message]) -> bool {
    outbox
        .iter()
        .any(|m| matches!(m, Message::Accepted { channel, .. } if *channel == CH))
}

/// Commit `r`'s write on a room this node leads and return the room, the client, and
/// the room's genuine followers.
fn led_room_with_a_withheld_write(r: &mut Registry) -> (Vec<u8>, ConnId, Vec<NodeId>) {
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);
    let me = NodeId::from(SELF_ADDR);
    let followers: Vec<NodeId> = m
        .replicas_for(&room)
        .into_iter()
        .filter(|n| n != &me)
        .collect();
    let c = client(r);
    r.deliver(c, sub(&room));
    r.take_outbox(c);
    r.deliver(
        c,
        Message::Ops {
            channel: CH,
            ops: write(),
        },
    );
    assert!(
        r.take_outbox(c).is_empty(),
        "the Accepted is withheld until a majority holds the write",
    );
    (room, c, followers)
}

/// The liveness tuple `m` advertises for `node`, or `None` if it advertises none.
fn advertised(m: &Membership, node: &NodeId) -> Option<(Vec<u8>, u64, MemberState, bool)> {
    m.known_liveness()
        .into_iter()
        .find(|(n, ..)| n == node)
        .map(|(_, addr, inc, state, verified)| (addr, inc, state, verified))
}

/// Whether `m` advertises `node` as one it has itself verified.
fn verified_by(m: &Membership, node: &NodeId) -> bool {
    advertised(m, node).map(|(.., v)| v).unwrap_or(false)
}

// --- the defect: a minted id reaches no room ---

#[test]
fn a_member_that_mints_a_node_id_does_not_enter_a_rooms_replica_set() {
    // The inversion of C13's residual. The id still places — HRW is a pure function of
    // the member set and grinding one that lands on a chosen room is a few tries — but
    // introducing itself no longer makes it a member rooms are placed on.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);
    let minted = minted_for(&m, &room);
    let mut r = registry();

    let p = peer_as(&mut r, &minted);
    assert!(
        r.deliver(p, introduces(&minted, false)),
        "the join path still admits an unknown node's self-introduction",
    );

    let view = r.membership().expect("clustered");
    assert!(
        view.is_member(&minted),
        "the minted node is learned — it has to be dialable to ever be verified",
    );
    assert!(
        !view.is_adopted(&minted),
        "but the cluster has not adopted it"
    );
    assert!(
        !view.replicas_for(&room).contains(&minted),
        "so no room places on it",
    );
    assert_eq!(
        view.replicas_for(&room),
        m.replicas_for(&room),
        "and the room's replica set is the one it always was",
    );

    // The grind was real, and pending is the whole of what kept it out: verified to the
    // bar by the cluster, that same id takes its place on that same room.
    cluster_verifies(&mut r, &minted);
    let view = r.membership().expect("clustered");
    assert!(view.is_adopted(&minted));
    assert!(
        view.replicas_for(&room).contains(&minted),
        "the id was one placement puts on the room",
    );
}

#[test]
fn a_minted_member_cannot_supersede_the_leader_with_a_forged_epoch() {
    // The leadership gate, reached from outside the replica set. Before adoption the
    // minted member was inside it and any epoch above this node's stripped it of the
    // room; now the frame comes from a member that replicates nothing and the link
    // goes, leaving the epoch where the leader left it.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);
    let minted = minted_for(&m, &room);
    let mut r = registry();
    let c = client(&mut r);
    r.deliver(c, sub(&room));
    r.deliver(
        c,
        Message::Ops {
            channel: CH,
            ops: write(),
        },
    );
    let epoch = r.highest_epoch(&room);

    let p = peer_as(&mut r, &minted);
    assert!(r.deliver(p, introduces(&minted, false)));
    assert!(
        !r.deliver(p, replicate(&mut doc(9), &room, epoch + 100)),
        "the forged frame drops the link",
    );
    assert_eq!(
        r.highest_epoch(&room),
        epoch,
        "and leaves this node's leadership epoch untouched",
    );
}

#[test]
fn a_minted_member_cannot_release_an_accepted_no_majority_holds() {
    // The durability gate. A room's majority is counted over its replica set, so a
    // member that mints its way in acks as one of five and the leader releases the
    // client's `Accepted` on a write only two nodes hold. Pending, its ack counts for
    // nothing — and the write still releases the moment a real majority holds it, so
    // this is a narrower quorum and not a stalled one.
    let m = membership_for(SELF_ADDR);
    let mut r = registry();
    let (room, c, followers) = led_room_with_a_withheld_write(&mut r);
    let minted = minted_for(&m, &room);
    let seq = r.hub().seq(&room);

    let p = peer_as(&mut r, &minted);
    assert!(r.deliver(p, introduces(&minted, false)));

    r.record_replica_ack(minted.clone(), &room, seq);
    r.record_replica_ack(followers[0].clone(), &room, seq);
    assert!(
        !has_accepted(&r.take_outbox(c)),
        "self plus one genuine follower is not a majority of five, whatever the \
         minted member acks",
    );

    r.record_replica_ack(followers[1].clone(), &room, seq);
    assert!(
        has_accepted(&r.take_outbox(c)),
        "a genuine majority still releases the write",
    );
}

#[test]
fn a_pending_member_does_not_enlarge_the_quorum_of_a_room_it_does_not_attack() {
    // The lesser way in: a minted member landing in the ring raises the majority of
    // every room it places on, so writes to rooms it never meant to touch wait on acks
    // it never sends. Pending, it changes no room's replica set at all, so the same
    // genuine acks release the same write.
    let m = membership_for(SELF_ADDR);
    let mut r = registry();
    let (room, c, followers) = led_room_with_a_withheld_write(&mut r);
    let seq = r.hub().seq(&room);

    // The member is minted for *this* room, so it would have entered its replica set —
    // and therefore raised its majority — had it been adopted. Pending, no room's
    // replica set moves at all, so the rooms it never meant to touch keep the quorum
    // they always had.
    let minted = minted_for(&m, &room);
    let p = peer_as(&mut r, &minted);
    assert!(r.deliver(p, introduces(&minted, false)));
    let view = r.membership().expect("clustered");
    for i in 0..32 {
        let sample = format!("room-{i}").into_bytes();
        assert_eq!(
            view.replicas_for(&sample),
            m.replicas_for(&sample),
            "no room's replica set moves",
        );
    }

    r.record_replica_ack(followers[0].clone(), &room, seq);
    r.record_replica_ack(followers[1].clone(), &room, seq);
    assert!(
        has_accepted(&r.take_outbox(c)),
        "the room's majority is the one it always had",
    );
}

// --- a node cannot vouch for itself ---

#[test]
fn a_member_cannot_verify_itself_into_the_ring() {
    // The claim is a member's own, so the obvious forgery is to make it about itself.
    // A tuple naming the sender is dropped: a node's place in the ring is never its own
    // to assert, which is the whole reason adoption is a cluster decision.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);
    let minted = minted_for(&m, &room);
    let mut r = registry();

    let p = peer_as(&mut r, &minted);
    assert!(r.deliver(p, introduces(&minted, true)));
    let view = r.membership().expect("clustered");
    assert!(
        !view.has_verified(&minted, &minted),
        "the claim never enters the evidence",
    );
    assert!(!view.is_adopted(&minted));
}

#[test]
fn whether_a_dial_establishes_identity_is_read_off_the_id_alone() {
    // Whether a member's claims are attributable decides what enters `verifiers`, and
    // `verifiers` is what the ring is derived from — so it has to be the same answer on
    // every node, including one meeting that member for the first time. A node id *is*
    // an advertise address, so the transport is a property of the id. Were this a
    // roster lookup it would answer `false` for a member not yet learned, and the frame
    // that *introduces* a sender would be judged differently from every later one: the
    // claims it carries kept by the nodes that already knew it and dropped by the nodes
    // meeting it, from one delivery.
    let known = membership_for(SELF_ADDR);
    let stranger =
        Membership::from_static_config(None, Some(SELF_ADDR), "10.7.7.7:9000", N).unwrap();
    for addr in [
        "wss://node-a.example:9000",
        "node-a.example:9000",
        "wss://10.0.0.1:9000",
        "10.0.0.1:9000",
    ] {
        let node = NodeId::from(addr);
        assert!(
            known.is_member(&node) || !stranger.is_member(&node),
            "{addr}: the two views hold different rosters",
        );
        assert_eq!(
            known.advertises_tls(&node),
            stranger.advertises_tls(&node),
            "{addr}: a view that has never seen the member answers the same",
        );
        assert_eq!(
            stranger.advertises_tls(&node),
            addr.starts_with("wss://"),
            "{addr}: and the answer is the id's own transport",
        );
    }
}

#[test]
fn a_claim_is_recorded_against_the_member_whose_link_carried_it() {
    // The claim is worth exactly the link it arrived on, so it is attributed to that
    // member and to nobody the payload names. Were it recorded against the named node
    // instead, every member would be its own voucher and the bar would be no bar.
    let mut m = membership_for(SELF_ADDR);
    let joiner = NodeId::from("10.9.9.9:9000");
    let voucher = NodeId::from("10.0.0.1:9000");
    m.add_member(joiner.clone());
    m.merge_liveness(
        &voucher,
        [(
            joiner.clone(),
            joiner.as_bytes().to_vec(),
            0,
            MemberState::Alive,
            true,
        )],
    );
    assert!(m.has_verified(&voucher, &joiner), "the sender vouched");
    assert!(
        !m.has_verified(&joiner, &joiner),
        "and the member the payload named did not",
    );
}

#[test]
fn this_node_never_advertises_itself_as_verified() {
    // The same rule from the inside: a node holds no verification of itself to
    // advertise, so it never contributes one to its own adoption anywhere.
    let mut m = membership_for(SELF_ADDR);
    let me = NodeId::from(SELF_ADDR);
    m.note_verified(&me);
    assert!(!verified_by(&m, &me));
}

// --- the threshold ---

#[test]
fn one_members_verification_does_not_place_a_node() {
    // Adoption takes more than one member, because a single compromised member is
    // exactly the attacker here: it would otherwise vouch for the id it ground and
    // place it itself.
    let mut m = membership_for(SELF_ADDR);
    let joiner = NodeId::from("10.9.9.9:9000");
    m.add_member(joiner.clone());

    m.merge_liveness(
        &NodeId::from("10.0.0.1:9000"),
        [(
            joiner.clone(),
            joiner.as_bytes().to_vec(),
            0,
            MemberState::Alive,
            true,
        )],
    );
    assert!(!m.is_adopted(&joiner));
}

#[test]
fn enough_members_verifications_place_a_node() {
    // The other side: the bar is reachable, and clearing it puts the member in every
    // room its id places on.
    let mut m = membership_for(SELF_ADDR);
    let joiner = NodeId::from("10.9.9.9:9000");
    m.add_member(joiner.clone());
    for voucher in ["10.0.0.1:9000", "10.0.0.2:9000"] {
        m.merge_liveness(
            &NodeId::from(voucher),
            [(
                joiner.clone(),
                joiner.as_bytes().to_vec(),
                0,
                MemberState::Alive,
                true,
            )],
        );
    }
    assert!(m.is_adopted(&joiner));
    assert!(m.adopted_members().contains(&joiner));
    assert!(
        (0..64)
            .map(|i| format!("room-{i}").into_bytes())
            .any(|room| m.replicas_for(&room).contains(&joiner)),
        "and it now holds rooms",
    );
}

#[test]
fn the_same_members_verification_twice_is_still_one_voucher() {
    // The evidence is a set of members, not a count of frames, so a member that
    // re-gossips its own claim every round does not accumulate into a majority of one.
    let mut m = membership_for(SELF_ADDR);
    let joiner = NodeId::from("10.9.9.9:9000");
    m.add_member(joiner.clone());
    for _ in 0..8 {
        m.merge_liveness(
            &NodeId::from("10.0.0.1:9000"),
            [(
                joiner.clone(),
                joiner.as_bytes().to_vec(),
                0,
                MemberState::Alive,
                true,
            )],
        );
    }
    assert!(!m.is_adopted(&joiner));
}

#[test]
fn a_pending_members_verification_does_not_count() {
    // Two nodes minted together would otherwise vouch each other in without a single
    // established member ever reaching either. Only an adopted member's claim counts.
    let mut m = membership_for(SELF_ADDR);
    // On *different* hosts, so nothing but their pending status keeps their word out:
    // the trust-unit count would otherwise reach the bar on the pair alone.
    let first = NodeId::from("10.9.9.9:9000");
    let second = NodeId::from("10.9.9.8:9000");
    for node in [&first, &second] {
        m.add_member((*node).clone());
    }
    let claim = |about: &NodeId| {
        [(
            about.clone(),
            about.as_bytes().to_vec(),
            0,
            MemberState::Alive,
            true,
        )]
    };
    m.merge_liveness(&second, claim(&first));
    m.merge_liveness(&first, claim(&second));
    // One genuine member reaches the first — still one short, because the pending
    // sibling's word is worth nothing.
    m.merge_liveness(&NodeId::from("10.0.0.1:9000"), claim(&first));
    assert!(!m.is_adopted(&first));
    assert!(!m.is_adopted(&second));

    // A second *adopted* member is what carries it, so the refusal is about who
    // vouched and not about the shape of the evidence.
    m.merge_liveness(&NodeId::from("10.0.0.2:9000"), claim(&first));
    assert!(m.is_adopted(&first));
    assert!(!m.is_adopted(&second));
}

// --- the address a member is verified at is the address its id names ---

#[test]
fn a_member_is_dialed_at_its_own_id_whatever_address_a_tuple_carries() {
    // The other lesser way in: the reply half of a gossip round hands over members at
    // whatever address their tuples carry, and a member's recorded address decided
    // where every later dial went — so whoever advertised a member *first* decided
    // what every node afterwards verified. Two nodes that saw it in a different order
    // then verified different endpoints and placed rooms differently, forever. A node
    // id is an advertise address, so the second name is dropped and the dial address is
    // a function of the id alone.
    let joiner = NodeId::from("10.9.9.9:9000");
    let foreign = b"10.6.6.6:9000".to_vec();
    // Poisoned through the additive path on one node and through the anti-entropy
    // merge on another — both are ways a peer's tuple reaches the roster, and the rule
    // has to hold on each or the two disagree.
    let mut poisoned = membership_for(SELF_ADDR);
    poisoned.add_member(joiner.clone());
    let mut merged = membership_for(SELF_ADDR);
    merged.merge_liveness(
        &NodeId::from("10.0.0.1:9000"),
        [(
            joiner.clone(),
            foreign.clone(),
            0,
            MemberState::Alive,
            false,
        )],
    );
    let mut clean = membership_for(SELF_ADDR);
    clean.add_member(joiner.clone());

    let dial_of = |m: &Membership| {
        m.known_members()
            .into_iter()
            .find(|(node, _)| node == &joiner)
            .map(|(_, addr)| addr)
    };
    assert_eq!(dial_of(&poisoned), Some(joiner.as_bytes().to_vec()));
    assert_eq!(dial_of(&merged), Some(joiner.as_bytes().to_vec()));
    assert_eq!(dial_of(&poisoned), dial_of(&clean));

    // So the same evidence places the same rooms on both, whichever address was
    // advertised first.
    for m in [&mut poisoned, &mut clean] {
        for voucher in ["10.0.0.1:9000", "10.0.0.2:9000"] {
            m.merge_liveness(
                &NodeId::from(voucher),
                [(
                    joiner.clone(),
                    joiner.as_bytes().to_vec(),
                    0,
                    MemberState::Alive,
                    true,
                )],
            );
        }
    }
    assert!(poisoned.is_adopted(&joiner));
    assert_eq!(poisoned.adopted_members(), clean.adopted_members());
}

#[test]
fn a_member_this_view_never_learned_is_not_verified_into_existence() {
    // A link is evidence *about* a member; where this view holds no member there is
    // nothing to be evidence about, and a link that could add one would restore the
    // unchecked join this closes. Learning a member stays gossip's job. Nothing is
    // *stored* either, which is what keeps the evidence bounded by the roster: peer
    // admission takes the id a link claims, so a certified member could otherwise open
    // a link per port on its own host and grow the map without limit.
    let mut m = membership_for(SELF_ADDR);
    let stranger = NodeId::from("10.9.9.9:9000");
    m.note_verified(&stranger);
    assert!(!m.is_member(&stranger));
    assert!(!m.is_adopted(&stranger));
    assert!(
        !m.has_verified(&NodeId::from(SELF_ADDR), &stranger),
        "no evidence is kept about a node that is no member",
    );
}

// --- what a link means ---

#[test]
fn only_the_dialing_side_verifies_the_other() {
    // A dial authenticates the far end against the address it dialed; being dialed
    // proves the sender reached here and nothing about who answers at the sender's own
    // address. So the initiator comes away holding a verification and the peer does not.
    let mut a = Membership::from_static_config(None, Some("10.0.0.1:9000"), "", N).unwrap();
    let mut b = Membership::from_static_config(None, Some("10.0.0.2:9000"), "", N).unwrap();
    let a_id = NodeId::from("10.0.0.1:9000");
    let b_id = NodeId::from("10.0.0.2:9000");

    crdtsync_server::gossip::exchange(&mut a, &mut b);
    assert!(verified_by(&a, &b_id), "the dialer verified the peer");
    assert!(!verified_by(&b, &a_id), "the dialed node verified nobody");
}

#[test]
fn an_uncertified_inbound_link_does_not_verify_the_member_it_claims() {
    // The cluster secret is one deployment-wide value, so an uncertified claim is a
    // member asserting an id rather than answering at one. Taking it as verification
    // would let any secret-holder vouch for any id it liked, which is the mint again
    // one step over.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);
    let minted = minted_for(&m, &room);
    let mut r = registry();

    let p = peer_as(&mut r, &minted);
    assert!(r.deliver(p, introduces(&minted, false)));
    // Re-admit on a second link: repetition is not evidence either.
    let again = peer_as(&mut r, &minted);
    assert!(r.deliver(again, introduces(&minted, false)));
    assert!(!verified_by(r.membership().expect("clustered"), &minted));
}

#[test]
fn no_inbound_link_verifies_the_member_it_names_however_it_is_certified() {
    // Admitting a link is not a verification, however well the certificate names the
    // member. A member chooses when to dial in and how often, so a vouch earned that
    // way is one the member *caused* rather than one this node independently made — and
    // a certificate names a host, which mints as many node ids as it likes, so a member
    // could dial in under each ground id in turn and have this node vouch for every
    // one. Verification is this node's own dial and nothing else.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);
    let minted = minted_for(&m, &room);
    let mut r = registry();

    let p = peer_as(&mut r, &minted);
    assert!(r.deliver(p, introduces(&minted, false)));
    certified_peer_as(&mut r, &minted);
    certified_peer_as(&mut r, &minted);

    let view = r.membership().expect("clustered");
    assert!(!verified_by(view, &minted), "no inbound link vouches");
    assert!(!view.is_adopted(&minted));

    // This node's own dial is what does, and one voucher is still short of the bar.
    r.note_peer_verified(&minted);
    let view = r.membership().expect("clustered");
    assert!(verified_by(view, &minted));
    assert!(!view.is_adopted(&minted));
}

// --- convergence ---

#[test]
fn adoption_is_independent_of_the_order_the_evidence_arrives() {
    // Placement must be identical on every node or the ring splits, so adoption has to
    // be a function of the evidence and not of the order it landed in.
    let joiner = NodeId::from("10.9.9.9:9000");
    let vouchers = ["10.0.0.1:9000", "10.0.0.2:9000", "10.0.0.3:9000"];
    let claim = [(
        joiner.clone(),
        joiner.as_bytes().to_vec(),
        0,
        MemberState::Alive,
        true,
    )];

    let mut forward = membership_for(SELF_ADDR);
    forward.add_member(joiner.clone());
    for v in vouchers {
        forward.merge_liveness(&NodeId::from(v), claim.clone());
    }

    let mut backward = membership_for(SELF_ADDR);
    backward.add_member(joiner.clone());
    for v in vouchers.iter().rev() {
        backward.merge_liveness(&NodeId::from(*v), claim.clone());
    }

    assert_eq!(forward.adopted_members(), backward.adopted_members());
    for i in 0..32 {
        let room = format!("room-{i}").into_bytes();
        assert_eq!(forward.replicas_for(&room), backward.replicas_for(&room));
    }
}

#[test]
fn a_configured_member_is_adopted_from_birth() {
    // The operator's config is the root of trust a cluster starts from — there is no
    // earlier authority for it to be vouched for by, and a cluster whose own seeds were
    // pending could never place a room at all.
    let m = membership_for(SELF_ADDR);
    assert_eq!(m.members(), m.adopted_members().to_vec());
    for i in 0..9 {
        assert!(m.is_adopted(&NodeId::from(format!("10.0.0.{i}:9000"))));
    }
}

#[test]
fn a_reaped_member_loses_its_place_and_its_vouches() {
    // Reaping removes a durably-gone member from the roster; it must take its adoption
    // and its word with it, or a departed node would keep vouching for joiners nobody
    // can reach.
    let mut m = membership_for(SELF_ADDR);
    let departing = NodeId::from("10.0.0.1:9000");
    let joiner = NodeId::from("10.9.9.9:9000");
    m.add_member(joiner.clone());
    m.merge_liveness(
        &departing,
        [(
            joiner.clone(),
            joiner.as_bytes().to_vec(),
            0,
            MemberState::Alive,
            true,
        )],
    );

    for _ in 0..crdtsync_server::membership::DEAD_AFTER_FAILURES {
        m.note_gossip_unreachable(&departing);
    }
    for _ in 0..crdtsync_server::membership::REAP_AFTER_DEAD_TICKS {
        m.reap_dead();
    }
    assert!(!m.is_member(&departing));
    assert!(!m.is_adopted(&departing));
    assert!(
        !m.has_verified(&departing, &joiner),
        "a departed member vouches for nobody, and its entries go with it",
    );

    // Its vouch went with it: one further genuine claim is now the first, not the
    // second.
    m.merge_liveness(
        &NodeId::from("10.0.0.2:9000"),
        [(
            joiner.clone(),
            joiner.as_bytes().to_vec(),
            0,
            MemberState::Alive,
            true,
        )],
    );
    assert!(!m.is_adopted(&joiner));
}

#[test]
fn vouches_banked_while_a_member_is_tombstoned_do_not_survive_its_return() {
    // Evidence is about a member of *this* view, and a tombstoned node is not one. If
    // claims about it were banked while it was gone, a member could be reaped and then
    // return pre-adopted — placed on rooms on the strength of vouches collected while
    // nobody could reach it, which is the unverified join the unit exists to close. The
    // vouches must arrive after it does.
    let mut m = membership_for(SELF_ADDR);
    let departing = NodeId::from("10.9.9.9:9000");
    m.add_member(departing.clone());
    for _ in 0..crdtsync_server::membership::DEAD_AFTER_FAILURES {
        m.note_gossip_unreachable(&departing);
    }
    for _ in 0..crdtsync_server::membership::REAP_AFTER_DEAD_TICKS {
        m.reap_dead();
    }
    assert!(!m.is_member(&departing));

    // Every configured member vouches for it while it is tombstoned — far past the bar.
    // The claims ride tuples that do not themselves lift the tombstone, which is the
    // whole point: SWIM's own resurrection rule is not being invoked, only the evidence
    // is being banked ahead of it.
    for i in 0..6 {
        let voucher = NodeId::from(format!("10.0.0.{i}:9000").as_str());
        m.merge_liveness(
            &voucher,
            [(
                departing.clone(),
                departing.as_bytes().to_vec(),
                0,
                MemberState::Suspect,
                true,
            )],
        );
        assert!(
            !m.has_verified(&voucher, &departing),
            "a claim about a node this view does not hold is about no member",
        );
    }
    assert!(
        !m.is_member(&departing),
        "and it did not slip back onto the roster",
    );
    assert!(!m.is_adopted(&departing));

    // It returns under SWIM's own rule, at a higher incarnation. It is a member again —
    // and pending, because none of those claims were kept.
    m.merge_liveness(
        &NodeId::from("10.0.0.1:9000"),
        [(
            departing.clone(),
            departing.as_bytes().to_vec(),
            1,
            MemberState::Alive,
            false,
        )],
    );
    assert!(m.is_member(&departing), "it rejoined");
    assert!(
        !m.is_adopted(&departing),
        "it returns pending: the vouches banked while it was gone are gone with it",
    );
}

#[test]
fn a_claim_from_a_sender_off_the_roster_is_not_retained() {
    // Peer admission takes the id a link claims, so one certified host can present as
    // many ids as it has ports. A claim is stored under its *sender*, so a sender that
    // is on no roster would bank an entry per link against every member — keyed on ids
    // no reap will ever strike, because reaping only strikes members. The evidence is
    // bounded by the roster or it is not bounded at all.
    let mut m = membership_for(SELF_ADDR);
    let joiner = NodeId::from("10.9.9.9:9000");
    m.add_member(joiner.clone());
    let before = m.members().len();

    for port in 9100..9200 {
        let stranger = NodeId::from(format!("evil.example:{port}").as_str());
        m.merge_liveness(
            &stranger,
            [(
                joiner.clone(),
                joiner.as_bytes().to_vec(),
                0,
                MemberState::Alive,
                true,
            )],
        );
        assert!(
            !m.has_verified(&stranger, &joiner),
            "a sender off the roster leaves nothing behind",
        );
    }
    assert_eq!(m.members().len(), before, "and joins nothing either");
    assert!(!m.is_adopted(&joiner));

    // A sender that *is* a member is still recorded — the bar is roster membership,
    // not distrust of gossip.
    let member = NodeId::from("10.0.0.1:9000");
    m.merge_liveness(
        &member,
        [(
            joiner.clone(),
            joiner.as_bytes().to_vec(),
            0,
            MemberState::Alive,
            true,
        )],
    );
    assert!(m.has_verified(&member, &joiner));
}

// --- a pending member is still a member ---

#[test]
fn a_pending_member_is_dialed_probed_and_gossiped_about() {
    // Pending is not exile: a member nobody dials can never be verified, so it would
    // never be adopted and a genuine joiner would never join. It rides the roster, the
    // gossip advertisement, and the indirect-probe roster exactly as an adopted member
    // does — it simply holds no room.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);
    let minted = minted_for(&m, &room);
    let mut r = registry();

    let p = peer_as(&mut r, &minted);
    assert!(r.deliver(p, introduces(&minted, false)));

    assert!(
        r.known_members().iter().any(|(node, _)| node == &minted),
        "the roster carries it, so the gossip loop dials it",
    );
    let advert = advertised(r.membership().expect("clustered"), &minted);
    assert_eq!(
        advert,
        Some((minted.as_bytes().to_vec(), 0, MemberState::Alive, false)),
        "and it is advertised, unverified, at its own address",
    );
    // The reply this node just queued carries it too, so the joiner's own view converges.
    let replied = r.take_outbox(p).into_iter().any(|msg| match msg {
        Message::Gossip { members } => members
            .iter()
            .any(|(node, ..)| node.as_slice() == minted.as_bytes()),
        _ => false,
    });
    assert!(replied, "the gossip reply advertises it back");
}

#[test]
fn a_plaintext_dial_does_not_verify_where_an_identified_peer_is_required() {
    // A deployment that requires an identified peer requires the dial to have
    // authenticated one. A plaintext member is dialed with no certificate to check, so
    // the round proves only that something answers there — and adopting on that would
    // give rooms to a member whose every inbound link is refused, stalling their
    // quorums. A `wss://` member is dialed over a transport that authenticates it, and
    // is verified.
    let mut r = registry();
    r.set_require_peer_identity(true);
    let plain = NodeId::from("10.9.9.9:9000");
    let tls = NodeId::from("wss://10.9.9.8:9000");
    r.merge_gossip(
        &NodeId::from("10.0.0.1:9000"),
        advertisements(&[&plain, &tls], false),
    );

    r.note_peer_verified(&plain);
    r.note_peer_verified(&tls);

    let view = r.membership().expect("clustered");
    assert!(
        !verified_by(view, &plain),
        "a plaintext dial checked nothing"
    );
    assert!(
        verified_by(view, &tls),
        "a TLS dial authenticated the far end"
    );
}

#[test]
fn a_plaintext_dial_still_verifies_where_no_identity_is_required() {
    // The honest floor, so the refusal above is the policy and not a blanket one. With
    // no certificates configured a completed dial vouches for reachability at the
    // address the id names — which is less than identity, and still more than a member
    // saying an id exists.
    let mut r = registry();
    let plain = NodeId::from("10.9.9.9:9000");
    r.merge_gossip(
        &NodeId::from("10.0.0.1:9000"),
        advertisements(&[&plain], false),
    );
    r.note_peer_verified(&plain);
    assert!(verified_by(r.membership().expect("clustered"), &plain));
}

// --- a trust unit is a host, not a node id ---

/// A membership whose configured set puts two members on one host and one on another
/// — the shape that tells "distinct verifiers" from "distinct trust units" apart.
fn shared_host_membership() -> Membership {
    Membership::from_static_config(
        None,
        Some(SELF_ADDR),
        "10.0.0.1:9000,10.0.0.1:9001,10.0.0.2:9000",
        N,
    )
    .unwrap()
}

/// `voucher`'s claim to have verified `node`, merged into `m`.
fn vouch(m: &mut Membership, voucher: &str, node: &NodeId) {
    m.merge_liveness(
        &NodeId::from(voucher),
        [(
            node.clone(),
            node.as_bytes().to_vec(),
            0,
            MemberState::Alive,
            true,
        )],
    );
}

#[test]
fn two_ids_on_one_host_are_one_voucher() {
    // A certificate names a *host* (C13), and a host mints as many node ids as it
    // likes — so counting distinct ids would let one machine raise the whole bar by
    // itself, which is the mint one level up. Two adopted members on one host vouch
    // once between them; a member on a second host is what carries it.
    let mut m = shared_host_membership();
    let joiner = NodeId::from("10.9.9.9:9000");
    m.add_member(joiner.clone());

    vouch(&mut m, "10.0.0.1:9000", &joiner);
    vouch(&mut m, "10.0.0.1:9001", &joiner);
    assert!(
        !m.is_adopted(&joiner),
        "both vouchers are the same machine, so they are one",
    );

    vouch(&mut m, "10.0.0.2:9000", &joiner);
    assert!(m.is_adopted(&joiner), "a second host carries it");
}

#[test]
fn a_sibling_on_a_members_own_host_does_not_vouch_for_it() {
    // A member vouching for a sibling on its own host is vouching for itself: they are
    // one trust unit, and the whole bar is that a member cannot place a node by itself.
    let mut m = shared_host_membership();
    let sibling = NodeId::from("10.0.0.1:9002");
    m.add_member(sibling.clone());

    // One voucher on the candidate's own host and one elsewhere: two members, two node
    // ids, and only *one* trust unit that is not the candidate itself.
    vouch(&mut m, "10.0.0.1:9000", &sibling);
    vouch(&mut m, "10.0.0.2:9000", &sibling);
    assert!(
        !m.is_adopted(&sibling),
        "its own host does not vouch for it"
    );

    // A second host does — this node, which reached it over its own link.
    m.note_verified(&sibling);
    assert!(m.is_adopted(&sibling), "two other hosts do");
}

#[test]
fn a_certified_member_still_mints_ids_on_its_own_host() {
    // The limit, pinned rather than implied. Placement keys on node ids and a
    // certificate names a host, so a member that owns a host owns every id under it:
    // it answers at each ground id, and the honest nodes that dial it are telling the
    // truth when they verify one. Adoption bounds the mint to the member's own host and
    // to ids the cluster can actually reach — it cannot bound it further, because every
    // verification here is genuine. Closing it needs the ring to weigh trust units
    // rather than ids, which is a placement change, not an evidence one.
    let mut m = membership_for(SELF_ADDR);
    let minted = NodeId::from("10.0.0.1:9999");
    m.add_member(minted.clone());
    vouch(&mut m, "10.0.0.2:9000", &minted);
    vouch(&mut m, "10.0.0.3:9000", &minted);
    assert!(
        m.is_adopted(&minted),
        "honest nodes that reached it vouched truthfully, and it is placed",
    );
}

// --- what a round means, and whose word counts ---

#[test]
fn only_a_direct_round_verifies_the_member_it_reached() {
    // A relay's second opinion says a *relay* reaches the target, which this node did
    // not observe and cannot attribute — so it is evidence of life and of nobody's
    // identity. Were it otherwise, one member confirming a target would place it.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads(&m);
    let minted = minted_for(&m, &room);
    let mut r = registry();
    let p = peer_as(&mut r, &minted);
    assert!(r.deliver(p, introduces(&minted, false)));

    r.note_gossip_round(minted.clone(), GossipRoundOutcome::Relayed);
    assert!(!verified_by(r.membership().expect("clustered"), &minted));
    r.note_gossip_round(minted.clone(), GossipRoundOutcome::Unreachable);
    assert!(!verified_by(r.membership().expect("clustered"), &minted));

    r.note_gossip_round(minted.clone(), GossipRoundOutcome::Direct);
    assert!(verified_by(r.membership().expect("clustered"), &minted));
}

#[test]
fn a_plaintext_peers_vouches_do_not_count_where_identity_is_required() {
    // The reply half comes from a node this one dialed, so what establishes the
    // sender's identity is that dial. A deployment that requires an identified peer
    // gets one from a `wss://` member's certificate and nothing at all from a plaintext
    // one — and an unattributable claim must not become an adopted member's vouch. The
    // liveness in the same payload still merges: reachability is nobody's identity.
    let mut r = registry();
    r.set_require_peer_identity(true);
    let joiner = NodeId::from("10.9.9.9:9000");
    let plaintext_member = NodeId::from("10.0.0.1:9000");

    r.merge_gossip(&plaintext_member, advertisements(&[&joiner], true));
    let view = r.membership().expect("clustered");
    assert!(view.is_member(&joiner), "the liveness still merged");
    assert!(
        !view.has_verified(&plaintext_member, &joiner),
        "but the claim is attributable to nobody",
    );
}

// --- bootstrap ---

#[test]
fn a_node_that_knows_only_itself_places_on_what_it_has_reached() {
    // The bar is a constant on every node, so two nodes never disagree about what the
    // evidence must show — except for a node configured with no peers at all, which has
    // no cluster to be outvoted by and would otherwise freeze forever: a second voucher
    // can only ever come from a member it has already adopted. That is the single-node
    // deployment, and the cluster it places on is what it has itself reached.
    let mut alone = Membership::from_static_config(None, Some(SELF_ADDR), "", N).unwrap();
    let first = NodeId::from("10.9.9.9:9000");
    let second = NodeId::from("10.9.9.8:9000");
    for node in [&first, &second] {
        alone.add_member((*node).clone());
    }

    alone.note_verified(&first);
    alone.note_verified(&second);
    assert!(
        alone.is_adopted(&first),
        "its own link is the whole verdict"
    );
    assert!(alone.is_adopted(&second));

    // The exception keys on configuration, not on the running set, so a node given a
    // single seed peer is held to the constant from its first round — the seed is
    // adopted from birth and there is a cluster to agree with.
    let mut seeded =
        Membership::from_static_config(None, Some(SELF_ADDR), "10.0.0.1:9000", N).unwrap();
    seeded.add_member(first.clone());
    seeded.note_verified(&first);
    assert!(
        !seeded.is_adopted(&first),
        "one voucher is short of the bar"
    );
}

// --- a trust unit is a machine, whatever the host is spelled like ---

#[test]
fn two_spellings_of_one_host_are_one_voucher() {
    // A host is the unit because a host is what a certificate names — so the two have
    // to agree on when two spellings are one host. The binding compares IP literals as
    // addresses and drops the root label; reading the host as raw text here would let
    // one machine present as several vouchers, which is the mint one level up.
    for (a, b) in [
        ("evil.example:9000", "evil.example.:9001"),
        ("EVIL.example:9000", "evil.EXAMPLE:9001"),
        (
            "[2001:db8::6]:9000",
            "[2001:0db8:0000:0000:0000:0000:0000:0006]:9001",
        ),
        ("wss://10.0.0.1:9000", "10.0.0.1:9001"),
    ] {
        assert_eq!(
            crdtsync_server::dial::member_trust_unit(a.as_bytes()),
            crdtsync_server::dial::member_trust_unit(b.as_bytes()),
            "{a} and {b} are one machine",
        );
    }
}

#[test]
fn a_machine_holding_two_spellings_holds_one_member() {
    // Canonicalization is what makes the trust unit hard to game: two spellings of one
    // machine are not two vouchers because they are not two *members*. The reduction in
    // the unit count is the second line of the same defence, for an id that ever
    // reached the roster unreduced.
    let m = Membership::from_static_config(
        None,
        Some(SELF_ADDR),
        "evil.example:9000,EVIL.example.:9000,ws://evil.example:09000",
        N,
    )
    .unwrap();
    let members: Vec<_> = m
        .members()
        .into_iter()
        .filter(|node| node != &NodeId::from(SELF_ADDR))
        .collect();
    assert_eq!(members, vec![NodeId::from("evil.example:9000")]);
}

// --- the ring is a fixpoint, and the bar does not move under it ---

#[test]
fn adopting_a_member_makes_its_own_vouch_count_in_the_same_pass() {
    // The evidence is a chain: one member clears the bar and its word is then what
    // carries the next. Evaluating it in a single pass would make the ring depend on
    // how many times the evidence happened to be re-merged, which is the same
    // history-dependence deriving the set exists to remove.
    let mut m = membership_for(SELF_ADDR);
    let first = NodeId::from("10.9.9.9:9000");
    let second = NodeId::from("10.9.9.8:9000");
    for node in [&first, &second] {
        m.add_member((*node).clone());
    }
    // `second` is vouched by one established member and by `first`, whose own word is
    // worth nothing until `first` itself clears the bar.
    vouch(&mut m, "10.0.0.3:9000", &second);
    m.merge_liveness(
        &first,
        [(
            second.clone(),
            second.as_bytes().to_vec(),
            0,
            MemberState::Alive,
            true,
        )],
    );
    vouch(&mut m, "10.0.0.1:9000", &first);
    assert!(!m.is_adopted(&first), "still one voucher short");
    assert!(!m.is_adopted(&second));

    vouch(&mut m, "10.0.0.2:9000", &first);
    assert!(m.is_adopted(&first));
    assert!(
        m.is_adopted(&second),
        "and the newly-adopted member's vouch carried the next, in the same pass",
    );
}

#[test]
fn reaping_a_seed_does_not_lower_the_bar() {
    // The bar is decided at construction, not read off a set that shrinks. A node given
    // one seed peer that later departs would otherwise fall to the single-node rule and
    // start placing on one vouch, while every peer still held the constant — a ring
    // split off an ordinary retirement.
    let mut m = Membership::from_static_config(None, Some(SELF_ADDR), "10.0.0.1:9000", N).unwrap();
    let seed = NodeId::from("10.0.0.1:9000");
    let joiner = NodeId::from("10.9.9.9:9000");
    m.add_member(joiner.clone());

    for _ in 0..crdtsync_server::membership::DEAD_AFTER_FAILURES {
        m.note_gossip_unreachable(&seed);
    }
    for _ in 0..crdtsync_server::membership::REAP_AFTER_DEAD_TICKS {
        m.reap_dead();
    }
    assert!(!m.is_member(&seed), "the seed departed");

    m.note_verified(&joiner);
    assert!(
        !m.is_adopted(&joiner),
        "and this node still holds the constant bar",
    );
}

// --- an advertise address names one endpoint, and one endpoint has one id ---

#[test]
fn an_address_that_carries_more_than_an_authority_is_no_address() {
    // The host a reader takes out of the string and the host a dialer connects to must
    // be the same one. A URL's userinfo splits them: `a.example:1@b.example:9000` reads
    // as `a.example` and connects to `b.example`, so a certificate for `a.example`
    // would bind an id that every honest peer verifies by dialing *`b`* — and the
    // attacker then speaks as a member of `b`'s rooms. A path splits nothing but is a
    // free alias, which is the other half of the same defect.
    for addr in [
        "wss://evil.example:1@10.0.0.1:9000",
        "evil.example@10.0.0.1:9000",
        "10.0.0.1:9000/x",
        "ws://10.0.0.1:9000/a/b",
        "10.0.0.1:9000?x=1",
        "10.0.0.1:9000#f",
    ] {
        assert!(
            crdtsync_server::dial::PeerEndpoint::parse(addr).is_err(),
            "{addr} is not an advertise address",
        );
        assert!(
            crdtsync_server::dial::canonical_member_addr(addr).is_none(),
            "{addr} has no canonical form",
        );
    }
}

#[test]
fn an_id_that_embeds_another_host_reaches_no_roster_and_no_link() {
    // End to end: the ground id is refused at every door. Gossip does not learn it, so
    // no node ever dials the honest host it embeds and verifies it; and no link can
    // bind to it, so it speaks as nobody even if some peer had.
    let minted = NodeId::from("wss://evil.example:1@10.0.0.1:9000");
    let mut r = registry();
    r.merge_gossip(
        &NodeId::from("10.0.0.1:9000"),
        advertisements(&[&minted], false),
    );
    let view = r.membership().expect("clustered");
    assert!(!view.is_member(&minted), "it is no member");
    assert!(!view.is_adopted(&minted));

    let id = r.connect();
    assert!(
        !r.deliver(
            id,
            Message::PeerAuth {
                node: minted.as_bytes().to_vec(),
                secret: SECRET.to_vec(),
            },
        ),
        "and no link binds to it",
    );
}

#[test]
fn one_endpoint_holds_one_node_id() {
    // A node id *is* an advertise address and placement hashes it, so two spellings of
    // one endpoint would be two positions in the ring that one node answers for. Every
    // peer that dialed either would verify it truthfully and both would be adopted —
    // and only one of them ever speaks, so a room that placed on the other waits on an
    // ack that never comes. The spellings collapse before the roster sees them.
    let canonical = NodeId::from("10.9.9.9:9000");
    let spellings = [
        "10.9.9.9:9000",
        "ws://10.9.9.9:9000",
        "WS://10.9.9.9:9000",
        "  10.9.9.9:9000  ",
        // A port is a number, not text: one listener, however it is written.
        "10.9.9.9:09000",
        "10.9.9.9:009000",
    ];
    for spelling in spellings {
        assert_eq!(NodeId::from_addr(spelling), canonical, "{spelling}");
    }

    let mut m = membership_for(SELF_ADDR);
    let before = m.members().len();
    m.add_members(spellings.iter().map(|s| NodeId::from(*s)));
    assert_eq!(
        m.members().len(),
        before + 1,
        "every spelling is the same one member",
    );
    assert!(m.is_member(&canonical));
}

#[test]
fn a_tls_member_keeps_its_scheme_and_a_plain_one_drops_it() {
    // The canonical form is not "strip the scheme": a member's transport is part of its
    // identity (§Peer Transport), and dropping `wss://` would make a TLS member and a
    // plaintext one on the same authority one id.
    assert_eq!(
        NodeId::from_addr("WSS://Node-A.Example.:9000"),
        NodeId::from("wss://node-a.example:9000"),
    );
    assert_ne!(
        NodeId::from_addr("wss://node-a.example:9000"),
        NodeId::from_addr("ws://node-a.example:9000"),
    );
    assert_eq!(
        NodeId::from_addr("[2001:0DB8:0000:0000:0000:0000:0000:0006]:9000"),
        NodeId::from_addr("[2001:db8::6]:9000"),
    );
}

#[test]
fn an_adopted_member_is_un_adopted_when_a_voucher_is_reaped() {
    // The ring is *derived* from the evidence, not accumulated as it goes past. So
    // evidence that goes away takes the placement with it — which is the whole point:
    // a node that kept the member placed because it happened to see the vouches before
    // the reap would disagree, permanently, with one that saw them after.
    let mut m = membership_for(SELF_ADDR);
    let joiner = NodeId::from("10.9.9.9:9000");
    let voucher = NodeId::from("10.0.0.1:9000");
    m.add_member(joiner.clone());
    vouch(&mut m, "10.0.0.1:9000", &joiner);
    vouch(&mut m, "10.0.0.2:9000", &joiner);
    assert!(m.is_adopted(&joiner));

    for _ in 0..crdtsync_server::membership::DEAD_AFTER_FAILURES {
        m.note_gossip_unreachable(&voucher);
    }
    for _ in 0..crdtsync_server::membership::REAP_AFTER_DEAD_TICKS {
        m.reap_dead();
    }
    assert!(!m.is_member(&voucher), "the voucher departed");
    assert!(
        !m.is_adopted(&joiner),
        "and the member it carried leaves the ring with it",
    );

    // A replacement voucher puts it back, so this is the evidence speaking and not a
    // one-way door.
    vouch(&mut m, "10.0.0.3:9000", &joiner);
    assert!(m.is_adopted(&joiner));
}

// --- the id a node is configured with is the id the cluster spells ---

#[test]
fn a_peer_list_naming_nobody_but_this_node_is_refused() {
    // Such a node has a peer plane, a cluster secret and gossip, and yet no cluster to
    // be outvoted by — its bar would fall to a single vouch and it would place a member
    // on rooms on its own word while every peer it met still held the cluster's bar.
    // The refusal reads the list, not the member count it collapses to: every spelling
    // below canonicalizes to this node's own id and de-duplicates away.
    for peers in [
        SELF_ADDR,
        "WS://10.0.0.6:9000",
        "  10.0.0.6:09000  ",
        "10.0.0.6:9000,ws://10.0.0.6:9000",
    ] {
        let e = Membership::from_static_config(None, Some(SELF_ADDR), peers, N)
            .expect_err("a peer list of only self is refused");
        assert!(
            e.to_string().contains("names nobody but this node"),
            "{peers}"
        );
    }
    // A list that names one real peer is a cluster, and is accepted.
    assert!(Membership::from_static_config(None, Some(SELF_ADDR), "10.0.0.1:9000", N).is_ok());
}

#[test]
fn an_explicit_node_id_is_the_id_the_cluster_spells() {
    // The explicit-id door canonicalizes like every other. Left verbatim, a node would
    // carry a *doppelgänger* of itself: its peers learn its canonical id, it does not
    // recognise that id as itself, two honest members vouch for it truthfully, and it
    // is adopted — rooms placed on a member that never answers, a suspicion of itself
    // it can never refute, and every follower-head report dropped because the link is
    // bound to the other spelling.
    let written = "WSS://Node-A.Example.:09000";
    let m = Membership::from_static_config(Some(written), None, "wss://node-b.example:9000", N)
        .unwrap();
    assert_eq!(m.self_id(), &NodeId::from("wss://node-a.example:9000"));
    assert_eq!(m.self_id(), &NodeId::from_addr(written));

    // So the canonical id arriving by gossip is recognised as itself and never learned
    // as a second member.
    let mut m = m;
    let before = m.members().len();
    m.add_member(NodeId::from_addr(written));
    m.add_member(NodeId::from("wss://node-a.example:9000"));
    assert_eq!(m.members().len(), before, "no doppelgänger");
}

#[test]
fn a_configured_address_no_peer_could_dial_is_refused() {
    // An id with no canonical form names a member the cluster could never verify and
    // this node could never be recognised as, so it is refused where it is written
    // rather than joined under.
    for id in [
        "::1:9000",
        "wss://a.example:1@b.example:9000",
        "10.0.0.1:9000/sync",
        "10.0.0.1:99999",
        "10.0.0.1:nine",
        "10.0.0.1:+9000",
        "10.0.0.1 :9000",
    ] {
        assert!(
            Membership::from_static_config(Some(id), None, "10.0.0.1:9000", N).is_err(),
            "{id}",
        );
    }
}

#[test]
fn a_link_bound_to_this_nodes_own_id_makes_it_vouch_for_nobody() {
    // A claim is the sender's. A frame arriving on a link that names *this* node would
    // otherwise insert this node into a member's verifier set — a whole trust unit,
    // for a member this node never dialed.
    let mut m = membership_for(SELF_ADDR);
    let joiner = NodeId::from("10.9.9.9:9000");
    m.add_member(joiner.clone());
    let me = NodeId::from(SELF_ADDR);
    m.merge_liveness(
        &me,
        [(
            joiner.clone(),
            joiner.as_bytes().to_vec(),
            0,
            MemberState::Alive,
            true,
        )],
    );
    assert!(
        !m.has_verified(&me, &joiner),
        "this node vouched for nobody"
    );
    assert!(!m.is_adopted(&joiner));
}

#[test]
fn a_node_advertises_only_the_verifications_it_made_itself() {
    // The flag is *first-hand*. Relaying what this node merely heard would make one
    // member's word enough to place any id it liked: a compromised member's claim
    // would be re-asserted by every honest node that received it, and the bar would be
    // met by one attacker and one echo.
    let mut m = membership_for(SELF_ADDR);
    let joiner = NodeId::from("10.9.9.9:9000");
    m.add_member(joiner.clone());
    vouch(&mut m, "10.0.0.1:9000", &joiner);
    assert!(
        m.has_verified(&NodeId::from("10.0.0.1:9000"), &joiner),
        "the claim was recorded",
    );
    assert!(
        !verified_by(&m, &joiner),
        "and this node advertises nothing it did not do itself",
    );

    // Its own link is what puts the flag on the wire.
    m.note_verified(&joiner);
    assert!(verified_by(&m, &joiner));
}

#[test]
fn a_peer_entry_no_peer_could_dial_is_refused_where_it_is_written() {
    // The peer-list door, not just the node-id door. A configured member is adopted
    // from birth, so an undialable id seeded here would be placed on rooms it can never
    // answer for — the write-stall, reached with no attacker at all.
    for peers in [
        "10.0.0.1:9000/sync",
        "10.0.0.1:9000,::1:9000",
        "wss://a.example:1@b.example:9000",
        "10.0.0.1:99999",
        "a..example:9000",
    ] {
        let e = Membership::from_static_config(None, Some(SELF_ADDR), peers, N)
            .expect_err("an undialable peer is refused");
        assert!(
            e.to_string().contains("is not an address a peer can dial"),
            "{peers}"
        );
    }
}

#[test]
fn a_second_spelling_of_an_honest_member_is_not_a_second_member() {
    // The shape a compromised member reaches for once it cannot mint on another host:
    // a spelling of an *honest* member's address that reduces to a different id. It
    // would resolve to the honest host, answer with the honest certificate, and be
    // verified truthfully by everyone — a ring position the honest node never speaks
    // as, and a room whose quorum can never be met. Every such spelling either reduces
    // to the same id or is no address at all.
    let honest = NodeId::from("wss://a.example:9000");
    let mut m = Membership::from_static_config(
        None,
        Some("wss://victim.example:9000"),
        "wss://a.example:9000,wss://b.example:9000",
        N,
    )
    .unwrap();
    let before = m.members().len();
    for spelling in [
        "wss://a.example.:9000",
        "wss://a.example..:9000",
        "wss://a.example...:9000",
        "wss://A.Example.:09000",
        "wss://[a.example:9000]:9000",
        "[a.example:]",
    ] {
        m.add_member(NodeId::from(spelling));
        assert!(
            NodeId::canonical(spelling.as_bytes()).is_none_or(|id| id == honest),
            "{spelling} is either no address or the honest member",
        );
    }
    assert_eq!(m.members().len(), before, "no ghost joined the roster");
}

#[test]
fn a_compromised_member_that_speaks_selectively_splits_the_ring() {
    // The accepted residual, pinned so it is a decision and not a surprise (KANBAN
    // C39). A claim is first-hand and never relayed — with no signature on the wire,
    // "A and B verified X" is free for any node to write — so a claim reaches exactly
    // the nodes its maker chooses to send it to, and no honest node can carry it
    // further or contradict it. A compromised member is therefore a swing vote for any
    // candidate that exactly one other trust unit has reached: it sets the flag toward
    // some peers and clears it toward others, and the two groups place rooms
    // differently. Where the candidate stays unreachable to the rest, no later gossip
    // repairs it, because there is no path by which the claim could arrive.
    let candidate = NodeId::from("wss://joiner.example:9000");
    // Both are configured members, so both are adopted and their word counts.
    let honest = "10.0.0.1:9000";
    let attacker = "10.0.0.2:9000";

    let mut told = membership_for(SELF_ADDR);
    let mut untold = membership_for(SELF_ADDR);
    for m in [&mut told, &mut untold] {
        m.add_member(candidate.clone());
        // The one honest unit that reached the candidate says so to both.
        vouch(m, honest, &candidate);
    }
    assert!(!told.is_adopted(&candidate), "one unit is not the bar");
    assert!(!untold.is_adopted(&candidate));

    // The compromised member sets the flag toward one node and clears it toward the
    // other. Both frames are well-formed and each is attributed to the member whose
    // link carried it, which is exactly what the rules require.
    vouch(&mut told, attacker, &candidate);
    untold.merge_liveness(
        &NodeId::from(attacker),
        [(
            candidate.clone(),
            candidate.as_bytes().to_vec(),
            0,
            MemberState::Alive,
            false,
        )],
    );

    assert!(
        told.is_adopted(&candidate),
        "two units, on the node it told"
    );
    assert!(
        !untold.is_adopted(&candidate),
        "and one unit on the node it did not",
    );

    // The rings differ, and they differ about rooms: the two nodes disagree on who
    // holds and who leads.
    let differing = (0..256u32)
        .map(|i| format!("room-{i}"))
        .filter(|room| told.replicas_for(room.as_bytes()) != untold.replicas_for(room.as_bytes()))
        .count();
    assert!(differing > 0, "a ring split is what the disagreement costs",);

    // And it does not heal: every honest exchange between the two carries only what
    // each verified itself, so neither can learn what the attacker told the other.
    for _ in 0..8 {
        crdtsync_server::gossip::exchange(&mut told, &mut untold);
        crdtsync_server::gossip::exchange(&mut untold, &mut told);
    }
    assert!(told.is_adopted(&candidate));
    assert!(
        !untold.is_adopted(&candidate),
        "no anti-entropy carries a claim its maker withheld",
    );
}

#[test]
fn the_same_frames_in_any_order_build_the_same_ring() {
    // `adopted` is derived from `verifiers`, so if retention depends on the order
    // frames arrive in, the ring does too — one level down, and invisibly. The frame
    // that vouches for a member and the frame that introduces it are two frames, and
    // which lands first is ordinary network ordering, not evidence.
    let joiner = NodeId::from("10.9.9.9:9000");
    let vouchers = ["10.0.0.1:9000", "10.0.0.2:9000"];

    let introduce = |m: &mut Membership| m.add_member(joiner.clone());
    let vouch_from = |m: &mut Membership, who: &str| vouch(m, who, &joiner);

    // Member first, then the vouches.
    let mut forward = membership_for(SELF_ADDR);
    introduce(&mut forward);
    for v in vouchers {
        vouch_from(&mut forward, v);
    }

    // The vouches first, then the member they are about.
    let mut backward = membership_for(SELF_ADDR);
    for v in vouchers {
        vouch_from(&mut backward, v);
    }
    introduce(&mut backward);

    assert!(forward.is_adopted(&joiner), "vouched then placed");
    assert!(
        backward.is_adopted(&joiner),
        "a claim about a member this view has not met yet is held, not dropped",
    );
    assert_eq!(
        forward.adopted_members(),
        backward.adopted_members(),
        "the ring is a function of the evidence, not of its arrival order",
    );
    let differing = (0..512u32)
        .map(|i| format!("room-{i}"))
        .filter(|r| forward.replicas_for(r.as_bytes()) != backward.replicas_for(r.as_bytes()))
        .count();
    assert_eq!(differing, 0, "and so is every room's replica set");
}

#[test]
fn a_claim_held_for_an_unmet_member_still_respects_the_tombstone() {
    // Holding a claim about a member this view has not met must not become a way to
    // bank evidence against one it has *reaped*: that member is not absent, it is
    // refused, and it must be re-verified after it returns.
    let mut m = membership_for(SELF_ADDR);
    let departed = NodeId::from("10.9.9.9:9000");
    m.add_member(departed.clone());
    for _ in 0..crdtsync_server::membership::DEAD_AFTER_FAILURES {
        m.note_gossip_unreachable(&departed);
    }
    for _ in 0..crdtsync_server::membership::REAP_AFTER_DEAD_TICKS {
        m.reap_dead();
    }
    assert!(!m.is_member(&departed));

    m.note_claims(
        &NodeId::from("10.0.0.1:9000"),
        [departed.clone(), departed.clone()],
    );
    m.note_claims(&NodeId::from("10.0.0.2:9000"), [departed.clone()]);
    assert!(
        !m.has_verified(&NodeId::from("10.0.0.1:9000"), &departed),
        "a tombstone refuses a held claim exactly as it refuses a carried one",
    );

    m.merge_liveness(
        &NodeId::from("10.0.0.1:9000"),
        [(
            departed.clone(),
            departed.as_bytes().to_vec(),
            1,
            MemberState::Alive,
            false,
        )],
    );
    assert!(m.is_member(&departed), "it rejoined");
    assert!(!m.is_adopted(&departed), "and it rejoined pending");
}
