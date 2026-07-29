//! C13 — a peer link is bound to a *member*, and every gate downstream of
//! admission decides against that member.
//!
//! C10 closed the peer plane behind the deployment's cluster secret, which
//! separates a member from a stranger. It separates members from each other not at
//! all, and three things downstream of admission assumed it did:
//!
//!  - **leadership** — `gate_replica_frame` supersedes this node's claim on any
//!    strictly higher epoch, so any admitted peer could strip the leader of any
//!    room, and push ops into a room it holds no replica of;
//!  - **membership** — `apply_gossip` merged whatever a peer advertised, so an
//!    admitted peer planted an arbitrary address in every node's member set, which
//!    every one of them then dialed and handed the cluster secret to, and which
//!    joined the placement ring and counted toward each room's quorum;
//!  - **durability** — `FollowerHeads` named its own reporter, so an admitted peer
//!    credited a *third* node with data it did not hold, and majority-ack then
//!    released a client `Accepted` for a write no majority ever held.
//!
//! So a link now says who it is: `PeerAuth` carries the dialer's node id alongside
//! the secret, and — where the deployment issues per-node certificates — the claim
//! must agree with the verified client certificate's subject. The binding is one
//! rule: **the certificate names the member's advertise host**, the same fact the
//! dialer already verifies in the other direction when it authenticates an
//! acceptor.
//!
//! What that closes: a member outside a room's replica set can no longer touch that
//! room at all, a peer introduces only itself and only at its own address, and a node
//! reports only its own heads. One limit is pinned below as a passing test rather
//! than left implied. Inside a room's replica set the epoch is still the only arbiter
//! — a genuinely promoted replica must be able to supersede a stale leader, and
//! nothing here tells it apart from a peer replica forging the bump, which needs a
//! real election. What was a second limit — that placement follows from a node id
//! while the join path lets an unknown node introduce itself, so a member could *mint*
//! an id that placed it into a room's replica set and reach these gates from inside it
//! — is closed by C25: rooms are placed on *adopted* members, and a node introducing
//! itself is pending until the cluster has verified it. See `adoption.rs`.
//!
//! Most of these drive the registry in process (no sockets), so they are
//! deterministic and run under Miri; the socket tests at the end stand up a real
//! mTLS cluster and pin that it replicates end to end, that a certificate for another
//! address reaches no peer plane, and the three ways the policy refuses to start.

use std::sync::Arc;

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, MemberState, Message, Op, Scalar};
use crdtsync_server::auth::Identity;
use crdtsync_server::membership::Membership;
use crdtsync_server::placement::NodeId;
use crdtsync_server::{ConnId, ManualClock, Registry};

const CH: Channel = Channel(0);

/// The deployment's cluster secret — what every node in one cluster holds and
/// nobody else does. It says a link is a member's; it never says *which* member's.
const SECRET: &[u8] = b"cluster-secret-of-at-least-32-bytes";

/// This node's advertise address in the in-process cluster below.
const SELF_ADDR: &str = "10.0.0.6:9000";

/// The per-room replication factor. Five, so a room's majority is three and one
/// follower's genuine ack is not on its own enough to release a write — which is
/// what makes a forged *second* follower observable.
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
/// replication factor, so a node follows some rooms, leads others, and holds no
/// replica of others still.
fn members_str() -> String {
    (0..9)
        .map(|i| format!("10.0.0.{i}:9000"))
        .collect::<Vec<_>>()
        .join(",")
}

fn membership_for(self_addr: &str) -> Membership {
    Membership::from_static_config(None, Some(self_addr), &members_str(), N).unwrap()
}

/// A clustered registry with the cluster secret configured — the deployment a peer
/// authenticates against.
fn registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for(SELF_ADDR));
    r.set_cluster_secret(SECRET.to_vec());
    r
}

/// A connection admitted to the peer plane as the member `node`.
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

/// A room this node is the placement primary of, and that `outsider` holds no
/// replica of — the pairing an attack from outside the replica set needs.
fn room_self_leads_without(m: &Membership, outsider: &NodeId) -> Vec<u8> {
    (0..1_000_000)
        .map(|i| format!("room-{i}").into_bytes())
        .find(|room| m.is_primary_for(room) && !m.replicas_for(room).contains(outsider))
        .expect("a room self leads that the outsider does not hold")
}

/// A room this node holds as a *follower* — the room a legitimate leader
/// replicates to it.
fn room_self_follows(m: &Membership) -> Vec<u8> {
    (0..1_000_000)
        .map(|i| format!("room-{i}").into_bytes())
        .find(|room| {
            let r = m.replicas_for(room);
            r.len() >= 2 && !m.is_self(&r[0]) && r.iter().skip(1).any(|n| m.is_self(n))
        })
        .expect("a room that places self as a follower")
}

/// A member of the cluster that holds no replica of `room`.
fn outsider_of(m: &Membership, room: &[u8]) -> NodeId {
    let replicas = m.replicas_for(room);
    m.members()
        .iter()
        .find(|node| !replicas.contains(node))
        .expect("the cluster is larger than a replica set")
        .clone()
}

/// The replicas of `room` other than this node, in placement order.
fn followers_of(m: &Membership, room: &[u8]) -> Vec<NodeId> {
    m.replicas_for(room)
        .into_iter()
        .filter(|n| !m.is_self(n))
        .collect()
}

/// Open an authenticated client on `r`, handshake drained.
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

/// Commit one client write into `room` through `r`, returning the author's
/// connection. This is what makes the node an actual *leader* of the room — it
/// claims a leadership epoch — so a later forged epoch has something to strip.
fn commit_write(r: &mut Registry, room: &[u8]) -> ConnId {
    let author = client(r);
    r.deliver(author, sub(room));
    let ops = doc(1).transact(|tx| tx.register(b"k", Scalar::Int(1)));
    assert!(r.deliver(author, Message::Ops { channel: CH, ops }));
    r.take_replication();
    author
}

/// A leader's `Replicate` for `room`'s main stream at `epoch`, carrying one
/// register write.
fn replicate(writer: &mut Document, room: &[u8], epoch: u64) -> Message {
    let ops = writer.transact(|tx| tx.register(b"planted", Scalar::Int(1)));
    Message::Replicate {
        room: room.to_vec(),
        branch: b"main".to_vec(),
        ops,
        base_seq: 0,
        epoch,
        creator: None,
    }
}

/// A whole-replica state transfer for `room` at `epoch`, carrying a real encoded
/// state so the frame would land if it were let through.
fn replicate_snapshot(room: &[u8], epoch: u64) -> Message {
    let mut writer = doc(9);
    writer.transact(|tx| tx.register(b"planted", Scalar::Int(1)));
    Message::ReplicateSnapshot {
        room: room.to_vec(),
        branch: b"main".to_vec(),
        seq: 1,
        state: writer.encode_state(),
        epoch,
        creator: None,
    }
}

/// Every op the connection's outbox carried, flattened out of its `Ops` frames.
fn received_ops(msgs: Vec<Message>) -> Vec<Op> {
    msgs.into_iter()
        .flat_map(|m| match m {
            Message::Ops { ops, .. } => ops,
            _ => Vec::new(),
        })
        .collect()
}

// --- leadership: a member outside the replica set holds nothing over the room ---

#[test]
fn a_member_outside_a_rooms_replica_set_cannot_strip_its_leader() {
    // The reproduction. This node leads the room at an epoch; an admitted member
    // that holds no replica of it sends a frame stamped far above that epoch. Before
    // the binding the fence took the higher epoch on its word and stepped the leader
    // down — at the word of a member that could not have led the room in any
    // placement.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads_without(&m, &outsider_of(&m, b"room-0"));
    let outsider = outsider_of(&m, &room);
    let mut r = registry();
    commit_write(&mut r, &room);
    let epoch = r.highest_epoch(&room);
    assert!(epoch > 0, "the node leads the room at an epoch");

    let p = peer_as(&mut r, &outsider);
    assert!(
        !r.deliver(p, replicate(&mut doc(9), &room, epoch + 100)),
        "a frame for a room the sender holds no replica of drops the connection",
    );
    assert_eq!(
        r.highest_epoch(&room),
        epoch,
        "the leader's epoch is untouched",
    );
    assert_eq!(
        r.hub().seq(&room),
        1,
        "and nothing of the frame was applied"
    );
}

#[test]
fn a_member_outside_a_rooms_replica_set_cannot_replicate_into_it() {
    // The same binding read as an ingest gate rather than a leadership one: a member
    // that holds no replica of a room can put nothing in it, and no subscriber reads
    // anything it sent.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let outsider = outsider_of(&m, &room);
    let mut r = registry();
    let reader = client(&mut r);
    r.deliver(reader, sub(&room));
    r.take_outbox(reader);

    let p = peer_as(&mut r, &outsider);
    assert!(!r.deliver(p, replicate(&mut doc(9), &room, 1)));
    assert_eq!(r.hub().seq(&room), 0, "no op reached the room's log");
    assert!(
        received_ops(r.take_outbox(reader)).is_empty(),
        "nothing was fanned out to the room's subscriber",
    );
    assert!(
        r.take_outbox(p).is_empty(),
        "and the sender is not acked, so it learns nothing of the room's state",
    );
}

#[test]
fn a_member_outside_a_rooms_replica_set_cannot_install_a_snapshot() {
    // The state-transfer path takes the same gate — otherwise what an outsider
    // replaces is the whole replica rather than a delta.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let outsider = outsider_of(&m, &room);
    let mut r = registry();
    let p = peer_as(&mut r, &outsider);
    assert!(!r.deliver(p, replicate_snapshot(&room, 1)));
    assert_eq!(r.hub().seq(&room), 0, "no state transfer installed");
}

#[test]
fn a_link_claiming_a_node_this_view_never_learned_is_dropped() {
    // The difference between a placement disagreement and a stray: a member this view
    // does not hold at all speaks for no leader under any placement, so its link goes
    // rather than the frame. A joining node is admitted and gossips; it does not
    // replicate.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();
    let p = peer_as(&mut r, &NodeId::from("10.9.9.9:9000"));
    assert!(!r.deliver(p, replicate(&mut doc(9), &room, 1)));
    assert_eq!(r.hub().seq(&room), 0);
}

#[test]
fn a_second_peer_auth_cannot_re_bind_an_admitted_link() {
    // The identity binds for the connection, not for the next frame: a link that could
    // re-bind would be one member's link speaking as another's, which is the whole of
    // what the binding exists to stop.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let leader = m.replicas_for(&room)[0].clone();
    let outsider = outsider_of(&m, &room);
    let mut r = registry();

    let p = peer_as(&mut r, &outsider);
    assert!(!r.deliver(
        p,
        Message::PeerAuth {
            node: leader.as_bytes().to_vec(),
            secret: SECRET.to_vec(),
        },
    ));
}

#[test]
fn a_link_claiming_this_node_itself_replicates_nothing() {
    // A member's id names the node on the *other* end of the link, so a link
    // claiming to be this node is either a loop or an impersonation. Either way the
    // node it names is not the one speaking.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let mut r = registry();
    let p = peer_as(&mut r, &NodeId::from(SELF_ADDR));
    assert!(!r.deliver(p, replicate(&mut doc(9), &room, 1)));
    assert_eq!(r.hub().seq(&room), 0);
}

#[test]
fn a_replica_of_the_room_still_supersedes_a_stale_leader() {
    // The limit, pinned rather than papered over. Inside a room's replica set the
    // epoch is the only arbiter: a replica promoted over a leader it believes down
    // must be able to supersede it, and this node cannot tell that from a peer
    // replica forging the bump. Distinguishing them needs a real election (the
    // HRW+epoch → Raft evolution), not a stronger identity — so a peer replica's
    // higher epoch still steps this node down, deliberately.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads_without(&m, &outsider_of(&m, b"room-0"));
    let mut r = registry();
    commit_write(&mut r, &room);
    let epoch = r.highest_epoch(&room);

    let replica = followers_of(&m, &room)[0].clone();
    let p = peer_as(&mut r, &replica);
    assert!(r.deliver(p, replicate(&mut doc(9), &room, epoch + 1)));
    assert_eq!(
        r.highest_epoch(&room),
        epoch + 1,
        "a replica of the room supersedes on a higher epoch, as a promotion must",
    );
}

// --- membership: a peer introduces itself and nobody else ---

#[test]
fn a_member_cannot_introduce_a_foreign_address_into_the_member_set() {
    // The reproduction. An admitted peer advertises an address that is no member of
    // anything; before the binding it joined every node's set, every node dialed it
    // and handed it the cluster secret, and it took a place in the placement ring.
    let mut r = registry();
    let before = r.known_members().len();
    let p = peer_as(&mut r, &NodeId::from("10.0.0.1:9000"));
    assert!(
        r.deliver(
            p,
            Message::Gossip {
                members: vec![(
                    b"evil.example:9000".to_vec(),
                    b"evil.example:9000".to_vec(),
                    7,
                    MemberState::Alive,
                    false,
                )],
            },
        ),
        "the exchange still answers — a rejected introduction is not a broken link",
    );
    assert_eq!(r.known_members().len(), before, "no member was added");
    assert!(
        !r.known_members()
            .iter()
            .any(|(node, _)| node.as_bytes() == b"evil.example:9000"),
        "the foreign address is not in the member set, so nothing dials it",
    );
}

#[test]
fn a_member_cannot_hide_a_foreign_address_among_legitimate_tuples() {
    // The same rejection when the injection rides alongside a real anti-entropy
    // payload — including the sender's own introduction, which is honored in the
    // same frame.
    let m = membership_for(SELF_ADDR);
    let joiner = NodeId::from("10.9.9.9:9000");
    let mut r = registry();
    let p = peer_as(&mut r, &joiner);
    let mut members: Vec<(Vec<u8>, Vec<u8>, u64, MemberState, bool)> = m
        .known_liveness()
        .into_iter()
        .map(|(node, addr, inc, state, v)| (node.as_bytes().to_vec(), addr, inc, state, v))
        .collect();
    members.push((
        joiner.as_bytes().to_vec(),
        joiner.as_bytes().to_vec(),
        0,
        MemberState::Alive,
        false,
    ));
    members.push((
        b"evil.example:9000".to_vec(),
        b"evil.example:9000".to_vec(),
        9,
        MemberState::Alive,
        false,
    ));
    assert!(r.deliver(p, Message::Gossip { members }));
    assert!(
        r.known_members().iter().any(|(node, _)| node == &joiner),
        "the sender's own introduction is honored",
    );
    assert!(
        !r.known_members()
            .iter()
            .any(|(node, _)| node.as_bytes() == b"evil.example:9000"),
        "the address it introduced for someone else is not",
    );
}

#[test]
fn a_member_cannot_introduce_itself_at_an_address_it_chose() {
    // The other half of the introduction rule. Constraining only the node id would
    // leave the same channel open one field over: the member set records the dial
    // address a tuple carries, so a peer introducing *itself* at a foreign address is
    // still an address every node dials and hands the cluster secret to.
    let joiner = NodeId::from("10.9.9.9:9000");
    let mut r = registry();
    let before = r.known_members().len();
    let p = peer_as(&mut r, &joiner);
    assert!(r.deliver(
        p,
        Message::Gossip {
            members: vec![(
                joiner.as_bytes().to_vec(),
                b"ws://evil.example:9000".to_vec(),
                0,
                MemberState::Alive,
                false,
            )],
        },
    ));
    assert_eq!(
        r.known_members().len(),
        before,
        "an introduction that does not dial at its own id is no introduction",
    );
}

#[test]
fn a_liveness_verdict_about_a_known_member_still_disseminates() {
    // The narrowing is about *introductions*, not about SWIM. A verdict on a member
    // this node already knows still merges, or a failure could never propagate past
    // the node that detected it.
    let m = membership_for(SELF_ADDR);
    let subject = outsider_of(&m, b"room-0");
    let mut r = registry();
    let p = peer_as(&mut r, &NodeId::from("10.0.0.1:9000"));
    assert!(r.deliver(
        p,
        Message::Gossip {
            members: vec![(
                subject.as_bytes().to_vec(),
                subject.as_bytes().to_vec(),
                5,
                MemberState::Dead,
                false,
            )],
        },
    ));
    assert_eq!(
        r.membership().unwrap().gossip_state(&subject),
        MemberState::Dead,
        "a third party's death still travels",
    );
}

#[test]
fn a_joining_node_introduces_itself() {
    // The join path the narrowing has to keep: a node the cluster has never heard of
    // dials in, names itself, and is learned. It is the *only* member it can add.
    let joiner = NodeId::from("10.9.9.9:9000");
    let mut r = registry();
    let p = peer_as(&mut r, &joiner);
    assert!(r.deliver(
        p,
        Message::Gossip {
            members: vec![(
                joiner.as_bytes().to_vec(),
                joiner.as_bytes().to_vec(),
                0,
                MemberState::Alive,
                false,
            )],
        },
    ));
    assert!(
        r.known_members().iter().any(|(node, _)| node == &joiner),
        "a node that dials in and names itself joins",
    );
    assert!(
        r.take_outbox(p)
            .iter()
            .any(|m| matches!(m, Message::Gossip { .. })),
        "and is answered with this node's view, which is how it learns the rest",
    );
}

// --- durability: a node reports its own heads and no other's ---

#[test]
fn a_member_cannot_report_another_nodes_heads() {
    // The reproduction. `FollowerHeads` names its reporter, and the leader overwrote
    // that node's watermark with the reported head — so one member credited another
    // with data it does not hold.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads_without(&m, &outsider_of(&m, b"room-0"));
    let followers = followers_of(&m, &room);
    let mut r = registry();
    commit_write(&mut r, &room);
    let head = r.hub().seq(&room);
    assert!(head > 0);

    let p = peer_as(&mut r, &followers[0]);
    assert!(
        !r.deliver(
            p,
            Message::FollowerHeads {
                reporter: followers[1].as_bytes().to_vec(),
                heads: vec![(room.clone(), head)],
            },
        ),
        "a report naming someone else drops the connection",
    );
    assert_eq!(
        r.replica_watermark(&room, &followers[1]),
        0,
        "the named node was credited with nothing",
    );
    assert_eq!(
        r.replica_watermark(&room, &followers[0]),
        0,
        "nor was the sender, whose frame was refused whole",
    );
}

#[test]
fn a_forged_head_report_releases_no_accepted_for_an_unreplicated_write() {
    // Why the reporter matters: a withheld client ack is released once a *majority*
    // of the replica set holds the write. With five replicas that is three — this
    // node and two followers — so crediting a third node that holds nothing is
    // exactly one forged report away from telling a client its write is durable when
    // it is not.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads_without(&m, &outsider_of(&m, b"room-0"));
    let followers = followers_of(&m, &room);
    assert!(followers.len() >= 2, "a five-way room has four followers");
    let mut r = registry();
    let author = commit_write(&mut r, &room);
    let head = r.hub().seq(&room);
    assert!(
        !r.take_outbox(author)
            .iter()
            .any(|m| matches!(m, Message::Accepted { .. })),
        "the write is withheld until a majority holds it",
    );

    // One follower genuinely holds the write: with the leader, that is two of the
    // three a majority needs.
    r.record_replica_ack(followers[0].clone(), &room, head);
    assert!(
        !r.take_outbox(author)
            .iter()
            .any(|m| matches!(m, Message::Accepted { .. })),
        "one follower is not a majority of five",
    );

    // The forgery: that same follower's link credits a second one.
    let p = peer_as(&mut r, &followers[0]);
    r.deliver(
        p,
        Message::FollowerHeads {
            reporter: followers[1].as_bytes().to_vec(),
            heads: vec![(room.clone(), head)],
        },
    );
    assert_eq!(
        r.replica_watermark(&room, &followers[1]),
        0,
        "the second follower holds nothing and is credited with nothing",
    );
    // A genuine ack re-checks the quorum; had the forged watermark landed, this
    // would find a majority and release the write.
    r.record_replica_ack(followers[0].clone(), &room, head);
    assert!(
        !r.take_outbox(author)
            .iter()
            .any(|m| matches!(m, Message::Accepted { .. })),
        "no Accepted is released for a write only one follower holds",
    );
}

#[test]
fn a_members_own_head_report_still_dials_its_catch_up() {
    // The wiped-follower self-heal, untouched: a node reporting its *own* heads is
    // authoritative for them, and the leader catches it up from where it says it is.
    let m = membership_for(SELF_ADDR);
    let room = room_self_leads_without(&m, &outsider_of(&m, b"room-0"));
    let follower = followers_of(&m, &room)[0].clone();
    let mut r = registry();
    commit_write(&mut r, &room);

    let p = peer_as(&mut r, &follower);
    assert!(r.deliver(
        p,
        Message::FollowerHeads {
            reporter: follower.as_bytes().to_vec(),
            heads: vec![(room.clone(), 0)],
        },
    ));
    assert!(
        !r.take_replication().is_empty(),
        "a member's own head report dials its catch-up",
    );
}

// --- the binding itself ---

#[test]
fn a_link_that_names_no_node_is_not_admitted() {
    // Fail-closed: a link with no identity has nothing for the gates to decide
    // against, so there is nothing to admit it as.
    let mut r = registry();
    let id = r.connect();
    assert!(!r.deliver(
        id,
        Message::PeerAuth {
            node: Vec::new(),
            secret: SECRET.to_vec(),
        },
    ));
}

#[test]
fn peer_identity_is_per_connection() {
    // Two links, two members: each frame is decided against the identity of the link
    // it arrived on, never against the last one admitted.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let leader = m.replicas_for(&room)[0].clone();
    let outsider = outsider_of(&m, &room);
    let mut r = registry();

    let stray = peer_as(&mut r, &outsider);
    let real = peer_as(&mut r, &leader);
    assert!(!r.deliver(stray, replicate(&mut doc(8), &room, 1)));
    assert_eq!(r.hub().seq(&room), 0, "the stray link landed nothing");
    assert!(r.deliver(real, replicate(&mut doc(9), &room, 1)));
    assert_eq!(
        r.hub().seq(&room),
        1,
        "only the room's leader landed a frame",
    );
}

#[test]
fn a_certificate_that_names_no_host_binds_nothing_and_is_refused() {
    // The difference between "presented no certificate" and "presented one that names
    // no host". The first is a deployment that has issued none, and its claim stands
    // on its own until identity is required. The second is a verified certificate
    // that simply does not bind this member — and a certificate must never *widen*
    // what a link may claim, so it is refused whatever the policy says.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let leader = m.replicas_for(&room)[0].clone();
    let mut r = registry();

    let id = r.connect_cert_authenticated(Identity::new(b"node-a".to_vec()), Vec::new());
    assert!(!r.deliver(
        id,
        Message::PeerAuth {
            node: leader.as_bytes().to_vec(),
            secret: SECRET.to_vec(),
        },
    ));
    assert_eq!(r.hub().seq(&room), 0);
}

#[test]
fn a_certificate_naming_the_member_binds_the_link() {
    // The other side of the same gate, so the refusal above is not simply "any
    // certificate refuses".
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let leader = m.replicas_for(&room)[0].clone();
    let host = crdtsync_server::dial::member_host(leader.as_bytes()).unwrap();
    let mut r = registry();
    r.set_require_peer_identity(true);

    let id = r.connect_cert_authenticated(
        Identity::new(host.clone().into_bytes()),
        vec![host.into_bytes()],
    );
    assert!(r.deliver(
        id,
        Message::PeerAuth {
            node: leader.as_bytes().to_vec(),
            secret: SECRET.to_vec(),
        },
    ));
    assert!(r.deliver(id, replicate(&mut doc(9), &room, 1)));
    assert_eq!(r.hub().seq(&room), 1);
}

#[test]
fn an_uncertified_link_is_refused_where_identity_is_required() {
    // The declared posture. A deployment that has issued per-node certificates says
    // so, and from then on a link presenting none is not admitted at all — the claim
    // it makes is no longer worth anything on its own.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let leader = m.replicas_for(&room)[0].clone();
    let mut r = registry();
    r.set_require_peer_identity(true);

    let id = r.connect();
    assert!(!r.deliver(
        id,
        Message::PeerAuth {
            node: leader.as_bytes().to_vec(),
            secret: SECRET.to_vec(),
        },
    ));
    let retry = r.connect();
    assert!(!r.deliver(retry, replicate(&mut doc(9), &room, 1)));
    assert_eq!(r.hub().seq(&room), 0);
}

#[test]
fn an_identified_member_still_sends_every_node_to_node_frame() {
    // The regression: nothing above narrows a legitimate member's traffic. Its
    // replication lands and is acked, its snapshot installs, its gossip is answered,
    // and its ping-req gets an opinion.
    let m = membership_for(SELF_ADDR);
    let room = room_self_follows(&m);
    let leader = m.replicas_for(&room)[0].clone();
    let mut r = registry();

    let p = peer_as(&mut r, &leader);
    assert!(r.deliver(p, replicate(&mut doc(9), &room, 1)));
    assert_eq!(r.hub().seq(&room), 1, "the leader's frame applied");
    assert!(
        r.take_outbox(p)
            .iter()
            .any(|m| matches!(m, Message::ReplicaAck { .. })),
        "and is acked",
    );

    let snap = peer_as(&mut r, &leader);
    assert!(r.deliver(snap, replicate_snapshot(&room, 2)));

    let gossiper = peer_as(&mut r, &leader);
    assert!(r.deliver(
        gossiper,
        Message::Gossip {
            members: m
                .known_liveness()
                .into_iter()
                .map(|(node, addr, inc, state, v)| (node.as_bytes().to_vec(), addr, inc, state, v))
                .collect(),
        },
    ));
    assert!(r
        .take_outbox(gossiper)
        .iter()
        .any(|m| matches!(m, Message::Gossip { .. })));

    let prober = peer_as(&mut r, &leader);
    assert!(r.deliver(
        prober,
        Message::PingReq {
            target: b"10.0.0.1:9000".to_vec(),
        },
    ));
    assert!(r
        .take_outbox(prober)
        .iter()
        .any(|m| matches!(m, Message::PingAck { .. })));
}

// --- a real cluster under peer identity ---

#[cfg(not(miri))]
mod live {
    use std::path::PathBuf;
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message as WsMessage;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    use crdtsync_core::protocol::PROTOCOL_VERSION;
    use crdtsync_core::{decode_message, encode_header, encode_message, ClientId, Message, Scalar};
    use crdtsync_server::membership::Membership;
    use crdtsync_server::placement::NodeId;
    use crdtsync_server::runtime::{serve_with, ServeConfig};
    use crdtsync_server::{
        client_config_from_pem, client_config_from_pem_with_identity, host_names_from_pem,
        server_config_from_pem_with_client_ca_mode, ClientAuthMode,
    };

    use super::{cid, doc, CH, SECRET};

    /// How long a positive convergence assertion waits. Generous rather than tight:
    /// these tests stand up real nodes that dial, complete a TLS handshake and
    /// replicate, and a loaded machine running the whole suite in parallel makes a
    /// short bound measure the machine rather than the code. A *negative* assertion
    /// never leans on this — each has its own bound.
    const CONVERGE: Duration = Duration::from_secs(60);

    /// A throwaway CA plus the leaf certs it issues, all on disk under one temp
    /// directory removed when the guard drops. A cluster's nodes chain to it, and a
    /// leaf's SAN is what binds a link to a member.
    struct Ca {
        dir: PathBuf,
        ca_path: PathBuf,
        cert: rcgen::Certificate,
        key: rcgen::KeyPair,
        issued: std::sync::atomic::AtomicU64,
    }

    impl Drop for Ca {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// An issued leaf: the PEM cert chain and private key on disk.
    struct Leaf {
        cert_path: PathBuf,
        key_path: PathBuf,
    }

    fn temp_dir(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("crdtsync-peerid-{tag}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    impl Ca {
        fn new(tag: &str) -> Self {
            let dir = temp_dir(tag);
            let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
            params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, "crdtsync-test-ca");
            let key = rcgen::KeyPair::generate().unwrap();
            let cert = params.self_signed(&key).unwrap();
            let ca_path = dir.join("ca.pem");
            std::fs::write(&ca_path, cert.pem()).unwrap();
            Self {
                dir,
                ca_path,
                cert,
                key,
                issued: std::sync::atomic::AtomicU64::new(0),
            }
        }

        /// Issue a leaf good for both ends of a peer handshake — `serverAuth` for the
        /// listener, `clientAuth` for the dial — whose SAN is `san`. The SAN is the
        /// whole binding: it must be the member's advertise host for the link to be
        /// admitted as that member.
        fn issue(&self, name: &str, san: &str) -> Leaf {
            self.issue_with_sans(name, &[san])
        }

        /// Issue a leaf naming several SANs at once — the shape a node certificate
        /// conventionally has, spelling its host as a DNS name *and* as the IP
        /// literal an advertise address may use.
        fn issue_with_sans(&self, name: &str, sans: &[&str]) -> Leaf {
            let n = self
                .issued
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut params = rcgen::CertificateParams::new(
                sans.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )
            .unwrap();
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, name);
            params.use_authority_key_identifier_extension = true;
            params.extended_key_usages = vec![
                rcgen::ExtendedKeyUsagePurpose::ServerAuth,
                rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let key = rcgen::KeyPair::generate().unwrap();
            let cert = params.signed_by(&key, &self.cert, &self.key).unwrap();
            let cert_path = self.dir.join(format!("{name}-{n}.pem"));
            let key_path = self.dir.join(format!("{name}-{n}.key"));
            std::fs::write(&cert_path, format!("{}{}", cert.pem(), self.cert.pem())).unwrap();
            std::fs::write(&key_path, key.serialize_pem()).unwrap();
            Leaf {
                cert_path,
                key_path,
            }
        }
    }

    /// A two-member cluster at replication factor 2 — a room's replica set is the
    /// primary plus one follower, so one peer link carries the whole story.
    fn two_node_membership(me: &str, other: &str) -> Membership {
        Membership::from_static_config(Some(me), None, other, 2).unwrap()
    }

    /// The same two-member view, built **without** the config validation. C25 refuses an
    /// address with no canonical form where it is written, so the startup refusals below
    /// — which are about a member that reaches the *serve* path naming no host — need a
    /// membership that did not go through that door to still have something to refuse.
    fn unvalidated_two_node_membership(me: &str, other: &str) -> Membership {
        Membership::new(NodeId::from(me), [NodeId::from(other)], 2)
    }

    /// A node that terminates mTLS, dials its peers with an identity of its own, and
    /// refuses any peer link carrying none — the whole declared posture.
    ///
    /// Client-certificate verification is in *request* mode: a peer presenting none
    /// is refused by the identity gate itself, so ordinary clients still need no
    /// certificate. The peer plane shares the client listener, and this is what keeps
    /// peer identity from becoming a certificate requirement for every application.
    fn identified_node(me: &str, other: &str, ca: &Ca, leaf: &Leaf) -> ServeConfig {
        ServeConfig {
            membership: Some(two_node_membership(me, other)),
            cluster_secret: Some(SECRET.to_vec()),
            tls: Some(
                server_config_from_pem_with_client_ca_mode(
                    &leaf.cert_path,
                    &leaf.key_path,
                    &ca.ca_path,
                    ClientAuthMode::Request,
                )
                .unwrap(),
            ),
            peer_tls: Some(
                client_config_from_pem_with_identity(&ca.ca_path, &leaf.cert_path, &leaf.key_path)
                    .unwrap(),
            ),
            require_peer_identity: true,
            client_cert_verification: true,
            peer_client_identity: Some(host_names_from_pem(&leaf.cert_path).unwrap()),
            ..ServeConfig::default()
        }
    }

    /// The first room this two-member cluster places on `leader_id`.
    fn room_led_by(leader_id: &str, follower_id: &str) -> Vec<u8> {
        let m = two_node_membership(leader_id, follower_id);
        let leader = NodeId::from(leader_id);
        (0..1_000_000)
            .map(|i| format!("room-{i}").into_bytes())
            .find(|room| m.primary_for(room) == Some(leader.clone()))
            .expect("a room the leader leads")
    }

    type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

    async fn send_frame(ws: &mut Ws, msg: &Message) {
        ws.send(WsMessage::Binary(encode_message(msg)))
            .await
            .unwrap();
    }

    /// Dial `url` over TLS, authenticating the acceptor against `ca` and presenting
    /// `identity` when one is given.
    async fn dial(url: &str, ca: &Ca, identity: Option<&Leaf>) -> Ws {
        let tls = match identity {
            Some(leaf) => {
                client_config_from_pem_with_identity(&ca.ca_path, &leaf.cert_path, &leaf.key_path)
                    .unwrap()
            }
            None => client_config_from_pem(&ca.ca_path).unwrap(),
        };
        let connector = tokio_tungstenite::Connector::Rustls(tls);
        let (mut ws, _) =
            tokio_tungstenite::connect_async_tls_with_config(url, None, false, Some(connector))
                .await
                .unwrap();
        ws.send(WsMessage::Binary(encode_header(PROTOCOL_VERSION).to_vec()))
            .await
            .unwrap();
        ws
    }

    /// Open a client on a *plaintext* listener and subscribe it to a room, so a test
    /// can tell "the node is serving" from "the node refused to start".
    async fn open_plain_client(addr: &str, client: ClientId) -> Ws {
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/"))
            .await
            .expect("the node refused to start over a certificate no peer will ever ask it for");
        ws.send(WsMessage::Binary(encode_header(PROTOCOL_VERSION).to_vec()))
            .await
            .unwrap();
        send_frame(
            &mut ws,
            &Message::Hello {
                client,
                app_id: Vec::new(),
                schema_version: 0,
                codecs: Vec::new(),
            },
        )
        .await;
        send_frame(
            &mut ws,
            &Message::Auth {
                credential: b"cred".to_vec(),
            },
        )
        .await;
        send_frame(
            &mut ws,
            &Message::Subscribe {
                channel: CH,
                room: b"any-room".to_vec(),
                branch: Vec::new(),
                zone: Vec::new(),
                last_seen_seq: 0,
            },
        )
        .await;
        ws
    }

    /// Open a certless client on a `wss://` listener running in request mode — the
    /// ordinary application connection a peer-identity deployment must keep serving.
    async fn open_client(url: &str, ca: &Ca, client: ClientId) -> Ws {
        let mut ws = dial(url, ca, None).await;
        send_frame(
            &mut ws,
            &Message::Hello {
                client,
                app_id: Vec::new(),
                schema_version: 0,
                codecs: Vec::new(),
            },
        )
        .await;
        send_frame(
            &mut ws,
            &Message::Auth {
                credential: b"cred".to_vec(),
            },
        )
        .await;
        loop {
            if let WsMessage::Binary(b) = ws.next().await.unwrap().unwrap() {
                if matches!(decode_message(&b), Ok(Message::AuthOk { .. })) {
                    break;
                }
            }
        }
        ws
    }

    /// The next message on `ws` matching `want` within `within`, or `None` — the
    /// bounded poll a negative assertion needs so it never hangs.
    async fn next_matching(
        ws: &mut Ws,
        within: Duration,
        want: impl Fn(&Message) -> bool,
    ) -> Option<Message> {
        tokio::time::timeout(within, async {
            loop {
                match ws.next().await {
                    Some(Ok(WsMessage::Binary(b))) => match decode_message(&b) {
                        Ok(msg) if want(&msg) => return Some(msg),
                        _ => continue,
                    },
                    Some(Ok(_)) => continue,
                    _ => return None,
                }
            }
        })
        .await
        .ok()
        .flatten()
    }

    /// Write one op through `writer` and report whether the leader released its
    /// `Accepted` — which it withholds until a majority (here: the one follower)
    /// holds the write, so the ack arriving is itself proof the peer link carried it.
    async fn write_reaches_the_follower(writer: &mut Ws, room: &[u8]) -> bool {
        send_frame(
            writer,
            &Message::Subscribe {
                channel: CH,
                room: room.to_vec(),
                branch: Vec::new(),
                zone: Vec::new(),
                last_seen_seq: 0,
            },
        )
        .await;
        let served = next_matching(writer, CONVERGE, |m| {
            matches!(
                m,
                Message::Ops { .. } | Message::Snapshot { .. } | Message::Redirect { .. }
            )
        })
        .await;
        assert!(
            matches!(served, Some(Message::Ops { .. } | Message::Snapshot { .. })),
            "the leader did not serve the room it leads, got {served:?}",
        );
        send_frame(
            writer,
            &Message::Ops {
                channel: CH,
                ops: doc(1).transact(|tx| tx.register(b"k", Scalar::Int(1))),
            },
        )
        .await;
        next_matching(writer, CONVERGE, |m| matches!(m, Message::Accepted { .. }))
            .await
            .is_some()
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
    async fn a_cluster_under_peer_identity_replicates_end_to_end() {
        // The headline: two nodes that each present a certificate naming their own
        // advertise host, each refusing any peer link that presents none, replicate a
        // client write — and the withheld `Accepted` arriving is proof the peer link
        // carried it all the way to the follower. The writer holds no certificate at
        // all, so this also pins that peer identity is not a client requirement.
        let ca = Ca::new("replicate");
        let leader_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let follower_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let leader_addr = format!("wss://{}", leader_listener.local_addr().unwrap());
        let follower_addr = format!("wss://{}", follower_listener.local_addr().unwrap());
        // Both nodes are on loopback, so one certificate naming `127.0.0.1` is the
        // identity of both: co-located members share a host and so share a trust
        // unit, which is the binding's stated cost.
        let leaf = ca.issue("node", "127.0.0.1");

        let room = room_led_by(&leader_addr, &follower_addr);
        let leader = tokio::spawn(serve_with(
            leader_listener,
            cid(0xF0),
            None,
            identified_node(&leader_addr, &follower_addr, &ca, &leaf),
        ));
        let follower = tokio::spawn(serve_with(
            follower_listener,
            cid(0xF1),
            None,
            identified_node(&follower_addr, &leader_addr, &ca, &leaf),
        ));

        let mut writer = open_client(&format!("{leader_addr}/"), &ca, cid(1)).await;
        assert!(
            write_reaches_the_follower(&mut writer, &room).await,
            "the write never reached a majority, so the peer link never carried it",
        );
        leader.abort();
        follower.abort();
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
    async fn a_certificate_for_another_address_reaches_no_peer_plane() {
        // The binding, over a real handshake. The dialer's certificate is one this
        // cluster's own CA signed — it is a member in every sense C10 and C12 can
        // check, and it holds the cluster secret — but it names an address that is
        // not the one the link claims, so the acceptor admits it to nothing.
        let ca = Ca::new("mismatch");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("wss://{}", listener.local_addr().unwrap());
        let peer_addr = "wss://127.0.0.1:9";
        let leaf = ca.issue("node", "127.0.0.1");
        let node = tokio::spawn(serve_with(
            listener,
            cid(0xF0),
            None,
            identified_node(&addr, peer_addr, &ca, &leaf),
        ));

        let imposter = ca.issue("imposter", "elsewhere.example");
        let mut ws = dial(&format!("{addr}/"), &ca, Some(&imposter)).await;
        send_frame(
            &mut ws,
            &Message::Hello {
                client: cid(0xEE),
                app_id: Vec::new(),
                schema_version: 0,
                codecs: Vec::new(),
            },
        )
        .await;
        send_frame(
            &mut ws,
            &Message::PeerAuth {
                node: peer_addr.as_bytes().to_vec(),
                secret: SECRET.to_vec(),
            },
        )
        .await;
        // A refused admission is answered with nothing and the link is dropped, so a
        // gossip on it is never answered.
        send_frame(
            &mut ws,
            &Message::Gossip {
                members: Vec::new(),
            },
        )
        .await;
        assert!(
            next_matching(&mut ws, Duration::from_secs(5), |m| matches!(
                m,
                Message::Gossip { .. }
            ))
            .await
            .is_none(),
            "a certificate for another address was admitted to the peer plane",
        );
        node.abort();
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
    async fn a_certificate_naming_a_member_several_ways_still_binds_it() {
        // A node certificate conventionally names its host more than one way, and a
        // member is addressed by whichever of those its advertise address spells. The
        // link is admitted when *any* of them binds — taking only the leading name
        // would refuse a cluster whose certificates lead with a DNS name and whose
        // addresses are IP literals, which is the ordinary case.
        let ca = Ca::new("multisan");
        let leader_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let follower_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let leader_addr = format!("wss://{}", leader_listener.local_addr().unwrap());
        let follower_addr = format!("wss://{}", follower_listener.local_addr().unwrap());
        // `localhost` leads; the address the cluster uses is the IP literal behind it.
        let leaf = ca.issue_with_sans("node", &["localhost", "127.0.0.1"]);

        let room = room_led_by(&leader_addr, &follower_addr);
        let leader = tokio::spawn(serve_with(
            leader_listener,
            cid(0xF0),
            None,
            identified_node(&leader_addr, &follower_addr, &ca, &leaf),
        ));
        let follower = tokio::spawn(serve_with(
            follower_listener,
            cid(0xF1),
            None,
            identified_node(&follower_addr, &leader_addr, &ca, &leaf),
        ));

        let mut writer = open_client(&format!("{leader_addr}/"), &ca, cid(1)).await;
        assert!(
            write_reaches_the_follower(&mut writer, &room).await,
            "a certificate whose leading name is not the advertise host refused the link",
        );
        leader.abort();
        follower.abort();
    }

    /// Serve `config` and return the startup error it refuses with. A node that
    /// starts serves forever, so the wait is bounded: an accepted misconfiguration
    /// reports as a failed assertion rather than a hung test.
    async fn startup_error(listener: TcpListener, config: ServeConfig) -> std::io::Error {
        tokio::time::timeout(
            Duration::from_secs(10),
            serve_with(listener, cid(0xFF), None, config),
        )
        .await
        .expect("a misconfigured node must refuse to start, not serve")
        .expect_err("a misconfigured node must refuse to start")
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
    async fn a_member_that_advertises_plaintext_still_identifies_itself_inbound() {
        // A member's advertised scheme describes *its own* listener — the transport
        // this node would dial it over. The link that carries its identity into this
        // node is the one it dials, which lands on this node's TLS listener and
        // presents its certificate whatever it advertises for itself. So requiring
        // peer identity does not silently require TLS of every member; refusing the
        // cluster secret to a plaintext member is `CRDTSYNC_CLUSTER_REQUIRE_TLS`'s own
        // separate declaration.
        let ca = Ca::new("mixed");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("wss://{}", listener.local_addr().unwrap());
        let plaintext_peer = "127.0.0.1:9";
        let leaf = ca.issue("node", "127.0.0.1");
        let node = tokio::spawn(serve_with(
            listener,
            cid(0xF0),
            None,
            identified_node(&addr, plaintext_peer, &ca, &leaf),
        ));

        let mut ws = dial(&format!("{addr}/"), &ca, Some(&leaf)).await;
        send_frame(
            &mut ws,
            &Message::Hello {
                client: cid(0xEE),
                app_id: Vec::new(),
                schema_version: 0,
                codecs: Vec::new(),
            },
        )
        .await;
        send_frame(
            &mut ws,
            &Message::PeerAuth {
                node: plaintext_peer.as_bytes().to_vec(),
                secret: SECRET.to_vec(),
            },
        )
        .await;
        send_frame(
            &mut ws,
            &Message::Gossip {
                members: Vec::new(),
            },
        )
        .await;
        assert!(
            next_matching(&mut ws, CONVERGE, |m| matches!(m, Message::Gossip { .. }))
                .await
                .is_some(),
            "a plaintext member's own dial carries its certificate and is admitted",
        );
        node.abort();
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // binds a loopback listener
    async fn requiring_peer_identity_without_a_cluster_refuses_to_start() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let e = startup_error(
            listener,
            ServeConfig {
                require_peer_identity: true,
                client_cert_verification: true,
                peer_client_identity: Some(vec![b"10.0.0.1".to_vec()]),
                ..ServeConfig::default()
            },
        )
        .await;
        assert!(e.to_string().contains("cluster membership"), "{e}");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // binds a loopback listener
    async fn requiring_peer_identity_without_verifying_client_certificates_refuses_to_start() {
        // Nothing inbound could carry an identity, so every peer link would be
        // refused — a cluster that starts, binds and never converges.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let e = startup_error(
            listener,
            ServeConfig {
                membership: Some(two_node_membership("10.0.0.1:9000", "10.0.0.2:9000")),
                cluster_secret: Some(SECRET.to_vec()),
                require_peer_identity: true,
                peer_client_identity: Some(vec![b"10.0.0.1".to_vec()]),
                ..ServeConfig::default()
            },
        )
        .await;
        assert!(e.to_string().contains("CRDTSYNC_TLS_CLIENT_CA"), "{e}");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // binds a loopback listener
    async fn an_advertise_address_that_names_no_host_refuses_to_start() {
        // No peer can dial it and no certificate could ever be bound to it, so it is
        // the address that is wrong — refused however the deployment is configured,
        // with the message for the problem it actually is rather than as a certificate
        // naming the wrong host. The refusal is now at the membership config itself
        // (C25): an id with no canonical form names a member the cluster could never
        // verify and this node could never be recognised as, so it never reaches a
        // listener at all.
        let e = Membership::from_static_config(Some("::1:9000"), None, "10.0.0.2:9000", 2)
            .expect_err("an unbracketed IPv6 literal names no host");
        assert!(
            e.to_string().contains("is not an address a peer can dial"),
            "{e}"
        );
        // And a member *learned* at such an address is still refused where it is read,
        // which is what keeps it a permanent dial failure rather than a forever-retry.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let e = startup_error(
            listener,
            ServeConfig {
                membership: Some(unvalidated_two_node_membership("10.0.0.1:9000", "::1:9000")),
                cluster_secret: Some(SECRET.to_vec()),
                ..ServeConfig::default()
            },
        )
        .await;
        assert!(
            e.to_string().contains("names no host, so no peer can dial"),
            "{e}"
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // binds a loopback listener
    async fn requiring_peer_identity_of_a_member_that_names_no_host_refuses_to_start() {
        // The binding is the member's host, so a member whose advertise address yields
        // none — an unbracketed IPv6 literal is the way to get there — could never be
        // identified, and every link to it would be refused. That is the same
        // starts-binds-never-converges failure the other three refusals prevent.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let e = startup_error(
            listener,
            ServeConfig {
                membership: Some(unvalidated_two_node_membership(
                    "wss://10.0.0.1:9000",
                    "wss://::1:9000",
                )),
                cluster_secret: Some(SECRET.to_vec()),
                require_peer_identity: true,
                client_cert_verification: true,
                peer_client_identity: Some(vec![b"10.0.0.1".to_vec()]),
                ..ServeConfig::default()
            },
        )
        .await;
        assert!(
            e.to_string().contains("names no host, so no certificate"),
            "{e}"
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // binds a loopback listener
    async fn requiring_peer_identity_with_a_certificate_for_another_host_refuses_to_start() {
        // The half of a peer's decision that is knowable here: the peers apply this
        // node's own rule to this node's own certificate, so a certificate naming
        // something other than this node's advertise host is refused by every one of
        // them — a cluster that starts, binds and never converges.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let e = startup_error(
            listener,
            ServeConfig {
                membership: Some(two_node_membership(
                    "wss://10.0.0.1:9000",
                    "wss://10.0.0.2:9000",
                )),
                cluster_secret: Some(SECRET.to_vec()),
                require_peer_identity: true,
                client_cert_verification: true,
                peer_client_identity: Some(vec![b"elsewhere.example".to_vec()]),
                ..ServeConfig::default()
            },
        )
        .await;
        assert!(e.to_string().contains("names no host binding it"), "{e}");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // binds a loopback listener
    async fn a_certificate_for_another_host_refuses_to_start_even_mid_rollout() {
        // The runtime rule is unconditional — a presented certificate that binds no
        // member refuses the link whether or not identity is required — so the startup
        // check that predicts it must be too. A node partway through a rollout, with a
        // certificate but not yet the policy, would otherwise have every peer link
        // refused silently by every peer that verifies client certificates.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let e = startup_error(
            listener,
            ServeConfig {
                membership: Some(two_node_membership(
                    "wss://10.0.0.1:9000",
                    "wss://10.0.0.2:9000",
                )),
                cluster_secret: Some(SECRET.to_vec()),
                require_peer_identity: false,
                client_cert_verification: true,
                peer_client_identity: Some(vec![b"elsewhere.example".to_vec()]),
                ..ServeConfig::default()
            },
        )
        .await;
        assert!(e.to_string().contains("names no host binding it"), "{e}");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // binds and dials a loopback listener
    async fn a_certificate_for_another_host_still_serves_where_no_peer_asks_for_one() {
        // The other side of that refusal, and why it is conditioned rather than
        // absolute. A client certificate is only sent when the acceptor asks for one,
        // so where no listener verifies client certificates the binding never runs and
        // a certificate naming something else — a URI-SAN service identity, a CN-only
        // one, a wildcard — is inert. Refusing to start there would break a working
        // deployment to report a failure that cannot happen; the node says so on stderr
        // instead, since it cannot see whether its peers ask.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let node = tokio::spawn(serve_with(
            listener,
            cid(0xFF),
            None,
            ServeConfig {
                membership: Some(two_node_membership(&addr, "10.0.0.2:9000")),
                cluster_secret: Some(SECRET.to_vec()),
                require_peer_identity: false,
                client_cert_verification: false,
                peer_client_identity: Some(vec![b"elsewhere.example".to_vec()]),
                ..ServeConfig::default()
            },
        ));
        // It started *and serves*: an ordinary client completes the handshake.
        let mut ws = open_plain_client(&addr, cid(1)).await;
        assert!(
            next_matching(&mut ws, CONVERGE, |m| matches!(
                m,
                Message::Ops { .. } | Message::Snapshot { .. } | Message::Redirect { .. }
            ))
            .await
            .is_some(),
            "the node did not serve, so the certificate refusal fired where no peer asks",
        );
        node.abort();
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // binds a loopback listener
    async fn requiring_peer_identity_without_an_identity_of_its_own_refuses_to_start() {
        // The symmetry `CRDTSYNC_CLUSTER_REQUIRE_TLS` already has: a node that demands
        // an identity of its peers must present one itself, or every peer running the
        // same policy refuses it.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let e = startup_error(
            listener,
            ServeConfig {
                membership: Some(two_node_membership("10.0.0.1:9000", "10.0.0.2:9000")),
                cluster_secret: Some(SECRET.to_vec()),
                require_peer_identity: true,
                client_cert_verification: true,
                ..ServeConfig::default()
            },
        )
        .await;
        assert!(
            e.to_string().contains("CRDTSYNC_CLUSTER_CLIENT_CERT"),
            "{e}"
        );
    }
}
