//! A version read is the room's leader's to answer, not whichever node holds the
//! channel (C33).
//!
//! `VersionList` and `VersionFetch` resolved their room and answered from local
//! state with no routing gate at all — unlike the version *mutations*, which go to
//! the leader outright, and unlike `Subscribe`, which lets a follower serve only
//! from a materialized replica caught up to the client's floor. Two facts make a
//! replica's answer a statement about itself rather than about the room, and this
//! suite pins both before it pins the routing:
//!
//! - **A room's version index is node-local.** Replication carries the room's log,
//!   never its captures, so the versions a replica holds are the ones it took
//!   itself.
//! - **A fetch redacts by the doc-ACL records the answering node holds**, and those
//!   tuples ride the room's log like any other op. A revoke committed on the leader
//!   and not yet replicated leaves a replica projecting by the grants it still has,
//!   and serving the subtree the revoke closed.
//!
//! The second is what a client cannot bound, and the reason the read is the
//! leader's rather than a caught-up replica's under a floor. A floor works for
//! `Subscribe` because a subscribe arrives on a *fresh* connection carrying a
//! cursor accumulated on some other node. A version read arrives on a **bound**
//! channel, and on a replica — the only path a floor is consulted on — that
//! channel's cursor is advanced solely by what this node delivered, so it cannot
//! name a sequence this node is behind on and would refuse nothing. That is pinned
//! here too, since it is the whole reason for the shape.
//!
//! A replica serves version reads once it holds the room's leadership: that is what
//! keeps the gate from being a blanket centralization, and it is as strong as the
//! leadership is — a node promoted on its own liveness view answers from its own
//! records, which is where the residue of this unit lives (C111).
//!
//! Two in-process registries — a leader `Registry` and a follower `Registry` over
//! one static cluster, no socket — as in `follower_reads.rs`: the leader commits,
//! its replication frames are handed to the follower (or deliberately withheld),
//! and a client then reads the follower directly. Deterministic, Miri-clean.

use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::client::ClientSession;
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::{Channel, DiffKind};
use crdtsync_core::{
    AclEffect, ClientId, Document, Element, ErrorCode, Message, Op, Scalar, Schema,
};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::membership::Membership;
use crdtsync_server::placement::NodeId;
use crdtsync_server::{ConnId, ManualClock, Registry, SchemaRegistry, StaticTokens};

const CH: Channel = Channel(0);
const N: usize = 3;
const A: &str = "10.0.0.1:9000";
const B: &str = "10.0.0.2:9000";
const V1: &[u8] = b"v1";
const APP: &[u8] = b"p";

/// The cluster secret these nodes share — what admits a node-to-node link.
const CLUSTER_SECRET: &[u8] = b"peer-plane-cluster-secret-for-tests";

/// Room read is granted by the schema tier to every authenticated actor, so bob
/// passes the version gate with no doc-ACL root grant — and a doc-ACL deny is what
/// carves `/b` back out of what he is served.
const PARTIAL: &str = r#"{
    "schema": "p", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "a": "Sect", "b": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "auth": { "grants": [ { "allow": "read", "to": "authenticated", "on": "/" } ] }
}"#;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn members() -> String {
    (1..=5)
        .map(|i| format!("10.0.0.{i}:9000"))
        .collect::<Vec<_>>()
        .join(",")
}

fn membership_for(self_addr: &str) -> Membership {
    Membership::from_static_config(None, Some(self_addr), &members(), N).unwrap()
}

/// A room `A` leads and `B` is the next replica of — so marking `A` down promotes
/// `B` to the room's effective leader.
fn room_led_by_a_with_b_next() -> Vec<u8> {
    let m = membership_for(A);
    let a = NodeId::from_addr(A);
    let b = NodeId::from_addr(B);
    for i in 0..1_000_000 {
        let room = format!("room-{i}").into_bytes();
        let replicas = m.replicas_for(&room);
        if replicas.first() == Some(&a) && replicas.get(1) == Some(&b) {
            return room;
        }
    }
    panic!("no room led by A with B next");
}

/// A node of the cluster — or single-node when `self_addr` is `None` — carrying the
/// schema registry, the alice/bob credentials, and the deployment ACL every node in
/// this suite shares, so a read decided on one is decided the same way on the other.
fn node(self_addr: Option<&str>, room: &[u8]) -> Registry {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, PARTIAL.as_bytes(), b"").unwrap();
    let mut tokens = StaticTokens::new();
    for (credential, actor) in [("t-alice", "alice"), ("t-bob", "bob")] {
        tokens.insert(credential.as_bytes().to_vec(), actor.as_bytes().to_vec());
    }

    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens));
    // The deployment permits alice everything and abstains on bob, so bob's room
    // read is the schema tier's and his content is the doc-ACL tier's alone.
    r.set_authorizer(Box::new(Acl::new().allow(
        Subject::Actor(b"alice".to_vec()),
        None,
        ResourceMatch::Room(room.to_vec()),
    )));
    r.set_clock(Arc::new(ManualClock::new(0)));
    if let Some(addr) = self_addr {
        r.set_membership(membership_for(addr));
        r.set_cluster_secret(CLUSTER_SECRET.to_vec());
    }
    r
}

/// Hello (enforcing the app) + Auth as `credential`, handshake drained.
fn auth(r: &mut Registry, client: u8, credential: &str) -> ConnId {
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

fn subscribe(room: &[u8]) -> Message {
    Message::Subscribe {
        channel: CH,
        room: room.to_vec(),
        branch: Vec::new(),
        zone: Vec::new(),
        last_seen_seq: 0,
    }
}

fn fetch(name: &[u8]) -> Message {
    Message::VersionFetch {
        channel: CH,
        name: name.to_vec(),
    }
}

/// Every channel-keyed frame of the version seam — the two reads, the three
/// mutations, and the diff arm that carries the same captured bytes. All six resolve
/// the channel's room, route on it, and then decide the gate, so a claim about that
/// order is a claim about all six.
fn channel_keyed_frames() -> Vec<Message> {
    vec![
        fetch(V1),
        Message::VersionList { channel: CH },
        Message::VersionCreate {
            channel: CH,
            name: b"v2".to_vec(),
        },
        Message::VersionRename {
            channel: CH,
            from: V1.to_vec(),
            to: b"v2".to_vec(),
        },
        Message::VersionDelete {
            channel: CH,
            name: V1.to_vec(),
        },
        Message::DiffQuery {
            channel: CH,
            kind: DiffKind::Versions,
            a: V1.to_vec(),
            b: V1.to_vec(),
        },
    ]
}

/// A connection admitted to `r`'s peer plane as the member `node`.
fn peer_conn_as(r: &mut Registry, node: &NodeId) -> ConnId {
    let id = r.connect();
    assert!(
        r.deliver(
            id,
            Message::PeerAuth {
                node: node.as_bytes().to_vec(),
                secret: CLUSTER_SECRET.to_vec(),
            },
        ),
        "the cluster secret admits a peer",
    );
    id
}

/// The redirect a node that declines to serve `room` answers with.
fn redirect_to_a(room: &[u8]) -> Vec<Message> {
    vec![Message::Redirect {
        room: room.to_vec(),
        leader_addr: NodeId::from_addr(A).as_bytes().to_vec(),
    }]
}

/// Whether a folded view carries `outer.inner` — the read the redaction is
/// measured by.
fn holds(d: &Document, outer: &[u8], inner: &[u8]) -> bool {
    let Some(Element::Map(m)) = d.get(outer) else {
        return false;
    };
    let child = m.borrow().get(inner);
    matches!(child, Some(Element::Register(_)))
}

/// The same read over a served state blob.
fn carries(state: &[u8], outer: &[u8], inner: &[u8]) -> bool {
    holds(
        &Document::decode_state(state).expect("the served state decodes"),
        outer,
        inner,
    )
}

/// The version state in a batch of reply frames, if the read was served at all.
fn served_state(replies: &[Message]) -> Option<Vec<u8>> {
    replies.iter().find_map(|m| match m {
        Message::VersionState { state, .. } => Some(state.clone()),
        _ => None,
    })
}

/// The read-deny tuple alice installs on `/b` against bob.
fn deny_b(doc: &mut Document) -> Vec<Op> {
    doc.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(actor_key(b"bob")),
            AclGrant::Capability(Capability::Read),
            AclEffect::Deny,
            encode_path(&[b"b"]),
            actor_key(b"alice"),
        );
    })
}

/// Whether `r`'s replica of `room` carries any doc-ACL read deny — the input the
/// projection redacts by, and the thing a lagging node is behind on.
fn holds_a_read_deny(r: &Registry, room: &[u8]) -> bool {
    r.hub()
        .acl_records(room)
        .iter()
        .any(|rec| rec.tuple.effect == AclEffect::Deny)
}

/// The leader `A`, its room seeded by alice with `/a` and `/b` and a root write
/// grant for bob, plus alice's own replica of it.
fn seeded_leader(room: &[u8]) -> (Registry, Document, ConnId) {
    let mut leader = node(Some(A), room);
    let alice = auth(&mut leader, 1, "t-alice");
    assert!(leader.deliver(alice, subscribe(room)));
    leader.take_outbox(alice);

    let mut alice_doc = Document::new(cid(1));
    alice_doc.set_schema(Schema::parse(PARTIAL).expect("the partial schema parses"));
    for ops in [
        alice_doc.transact(|tx| {
            tx.map(b"a").register(b"aseed", Scalar::Int(0));
            tx.map(b"b").register(b"bseed", Scalar::Int(0));
        }),
        // Write authority is a room-level verdict, so bob's write grant roots at `/`.
        alice_doc.transact(|tx| {
            tx.acl().grant(
                AclSubject::Actor(actor_key(b"bob")),
                AclGrant::Capability(Capability::Write),
                AclEffect::Allow,
                encode_path(&[]),
                actor_key(b"alice"),
            );
        }),
    ] {
        assert!(leader.deliver(alice, Message::Ops { channel: CH, ops }));
        leader.take_outbox(alice);
    }
    (leader, alice_doc, alice)
}

/// Hand `leader`'s pending replication frames for B to `follower`.
fn replicate(leader: &mut Registry, follower: &mut Registry, peer: ConnId) {
    let b = NodeId::from_addr(B);
    for (node, frame) in leader.take_replication() {
        if node == b {
            assert!(
                follower.deliver(peer, frame),
                "the follower applies a frame"
            );
        }
    }
}

/// The follower `B`, caught up to `leader`'s room and holding its own capture of it
/// as `V1` — the captures a node takes of its own replica, as an auto-version
/// trigger does, since the index itself never travels.
fn caught_up_follower_holding_v1(leader: &mut Registry, room: &[u8]) -> (Registry, ConnId) {
    let mut follower = node(Some(B), room);
    let peer = peer_conn_as(&mut follower, &NodeId::from_addr(A));
    replicate(leader, &mut follower, peer);
    assert_eq!(
        follower.hub().seq(room),
        leader.hub().seq(room),
        "the follower reached the leader's watermark",
    );
    assert!(
        follower
            .hub_mut()
            .create_version(room, V1)
            .expect("the capture takes"),
        "the follower captured its own replica as a version",
    );
    (follower, peer)
}

// --- why a replica's answer is about itself: the two premises ---

#[test]
fn a_replica_holds_none_of_the_leaders_captures() {
    // Replication carries the room's log, not its version index — so the versions a
    // node can answer for are the ones it took itself, whatever the room has.
    let room = room_led_by_a_with_b_next();
    let (mut leader, _alice_doc, alice) = seeded_leader(&room);
    assert!(leader.deliver(
        alice,
        Message::VersionCreate {
            channel: CH,
            name: b"on-the-leader".to_vec(),
        }
    ));
    leader.take_outbox(alice);

    let mut follower = node(Some(B), &room);
    let peer = peer_conn_as(&mut follower, &NodeId::from_addr(A));
    replicate(&mut leader, &mut follower, peer);

    assert_eq!(
        follower.hub().seq(&room),
        leader.hub().seq(&room),
        "the follower took every op the leader had",
    );
    assert!(
        leader
            .hub()
            .version_state(&room, b"on-the-leader")
            .is_some(),
        "the leader holds the capture it took",
    );
    assert!(
        follower
            .hub()
            .version_state(&room, b"on-the-leader")
            .is_none(),
        "a capture crossed the replication seam",
    );
}

#[test]
fn a_bound_channels_read_floor_never_outruns_the_replica_answering_it() {
    // Why a version read is not gated on a client-named floor the way a subscribe
    // is. A subscribe arrives on a fresh connection carrying a cursor accumulated
    // elsewhere, so it can name a sequence the node it is asking has not reached. A
    // version read arrives on a channel already bound here, and on a replica — the
    // only path a floor is ever consulted on, since the leader serves before one is
    // tested — that channel's cursor is advanced solely by what this node delivered.
    // So it cannot name a sequence this replica is behind on, and a floor would
    // refuse nothing.
    let room = room_led_by_a_with_b_next();
    let (mut leader, mut alice_doc, alice) = seeded_leader(&room);
    let (mut follower, _peer) = caught_up_follower_holding_v1(&mut leader, &room);

    // The leader moves ahead; the follower is not told.
    let ops = deny_b(&mut alice_doc);
    assert!(leader.deliver(alice, Message::Ops { channel: CH, ops }));
    leader.take_outbox(alice);
    leader.take_replication();
    assert!(follower.hub().seq(&room) < leader.hub().seq(&room));

    // A real client session joins the follower with its own subscribe frame and
    // folds in everything it is served.
    let mut session = ClientSession::new(cid(2));
    let (ch, sub) = session.subscribe(&room).expect("a channel is assigned");
    let bob = auth(&mut follower, 2, "t-bob");
    assert!(follower.deliver(bob, sub));
    for frame in follower.take_outbox(bob) {
        session
            .receive(frame)
            .expect("the session folds the catch-up");
    }
    let cursor = session.last_seen_seq(ch).expect("the channel is held");
    assert_eq!(
        cursor,
        follower.hub().seq(&room),
        "the session's cursor is exactly what this replica has delivered",
    );
    assert!(
        cursor < leader.hub().seq(&room),
        "and it is behind the room, with nothing on this channel able to raise it",
    );
}

// --- the headline: a revoke the replica has not seen ---

#[test]
fn a_version_fetch_on_a_replica_is_not_answered_through_its_stale_acl() {
    let room = room_led_by_a_with_b_next();
    let (mut leader, mut alice_doc, alice) = seeded_leader(&room);
    let (mut follower, _peer) = caught_up_follower_holding_v1(&mut leader, &room);

    // The revoke: alice denies bob read on `/b`, committed on the leader and
    // deliberately not replicated.
    let ops = deny_b(&mut alice_doc);
    assert!(leader.deliver(alice, Message::Ops { channel: CH, ops }));
    leader.take_outbox(alice);
    leader.take_replication();
    assert!(
        holds_a_read_deny(&leader, &room) && !holds_a_read_deny(&follower, &room),
        "the revoke is on the leader and not on the follower — the premise of the read",
    );

    let bob = auth(&mut follower, 2, "t-bob");
    assert!(follower.deliver(bob, subscribe(&room)));
    let joined = follower.take_outbox(bob);

    // The grants this node holds still admit `/b`, and its *live* seam serves it —
    // the bounded staleness a follower read carries by design. The version read is
    // what does not take part in it: an archived state redacted by a record set the
    // node cannot vouch for is a decision about now made on inputs nobody bounds.
    let mut served = false;
    let mut view = Document::new(cid(9));
    for frame in &joined {
        match frame {
            Message::Ops { ops, .. } => {
                served = true;
                for op in ops {
                    view.apply(op);
                }
            }
            Message::Snapshot { state, .. } => {
                served = true;
                view = Document::decode_state(state).expect("a served snapshot decodes");
            }
            _ => {}
        }
    }
    assert!(served, "bob is served a catch-up: {joined:?}");
    assert!(
        holds(&view, b"b", b"bseed"),
        "the follower's own ACL view still admits /b — the stale-grant read is real here",
    );

    assert!(follower.deliver(bob, fetch(V1)));
    let out = follower.take_outbox(bob);
    assert!(
        served_state(&out).is_none(),
        "the replica served a version state it redacts by a revoked grant: {out:?}",
    );
    assert_eq!(
        out,
        redirect_to_a(&room),
        "the version read is answered by the leader, whose records are the room's",
    );
}

#[test]
fn the_leader_serves_the_fetch_through_the_revoke_it_committed() {
    // Where the redirect points, and what the reader gets there: the same version,
    // redacted by the tuple the follower did not have.
    let room = room_led_by_a_with_b_next();
    let (mut leader, mut alice_doc, alice) = seeded_leader(&room);
    assert!(leader
        .hub_mut()
        .create_version(&room, V1)
        .expect("the capture takes"));
    let ops = deny_b(&mut alice_doc);
    assert!(leader.deliver(alice, Message::Ops { channel: CH, ops }));
    leader.take_outbox(alice);
    leader.take_replication();

    let bob = auth(&mut leader, 2, "t-bob");
    assert!(leader.deliver(bob, subscribe(&room)));
    leader.take_outbox(bob);

    assert!(leader.deliver(bob, fetch(V1)));
    let out = leader.take_outbox(bob);
    assert!(
        !out.iter().any(|m| matches!(m, Message::Redirect { .. })),
        "the leader answers its own room's version read: {out:?}",
    );
    let state = served_state(&out).expect("the leader serves the version state");
    assert!(
        carries(&state, b"a", b"aseed"),
        "the version withheld the subtree bob may read",
    );
    assert!(
        !carries(&state, b"b", b"bseed"),
        "the version served the subtree the revoke closed",
    );
}

// --- a replica serves once it is the room's leader ---

#[test]
fn a_promoted_replica_serves_the_versions_it_captured() {
    // The gate is not a blanket centralization: it names the room's *effective*
    // leader, so a replica promoted over a down primary answers its own captures —
    // redacted by the authority root and the tuples replication carried it.
    let room = room_led_by_a_with_b_next();
    let (mut leader, mut alice_doc, alice) = seeded_leader(&room);
    let ops = deny_b(&mut alice_doc);
    assert!(leader.deliver(alice, Message::Ops { channel: CH, ops }));
    leader.take_outbox(alice);
    let (mut promoted, _peer) = caught_up_follower_holding_v1(&mut leader, &room);
    assert!(holds_a_read_deny(&promoted, &room), "the revoke replicated");

    promoted
        .membership_mut_for_test()
        .mark_node_down(&NodeId::from_addr(A));

    let bob = auth(&mut promoted, 2, "t-bob");
    assert!(promoted.deliver(bob, subscribe(&room)));
    promoted.take_outbox(bob);

    assert!(promoted.deliver(bob, fetch(V1)));
    let out = promoted.take_outbox(bob);
    assert!(
        !out.iter().any(|m| matches!(m, Message::Redirect { .. })),
        "the promoted node leads the room, so it answers the read: {out:?}",
    );
    let state = served_state(&out).expect("the promoted leader serves the version state");
    assert!(
        carries(&state, b"a", b"aseed"),
        "the version withheld the subtree bob may read",
    );
    assert!(
        !carries(&state, b"b", b"bseed"),
        "the promoted leader served a subtree its replicated records deny",
    );
}

#[test]
fn a_lagging_promoted_leader_still_answers_from_its_own_records() {
    // The residue, measured at the version seam itself rather than inferred from the
    // live one — and the bound on what the gate buys. Leadership here is the node's
    // own liveness view: `effective_primary_for` walks the replica set for the first
    // node it believes live, with no lease, no quorum and no caught-up test. So a
    // replica that promotes over a leader still committing answers the fetch from
    // the records it has, and serves the subtree the revoke closed. The gate moves
    // the read to the node that holds the room's leadership; making that leadership
    // an authority the read can rely on is C111.
    let room = room_led_by_a_with_b_next();
    let (mut leader, mut alice_doc, alice) = seeded_leader(&room);
    let (mut promoted, _peer) = caught_up_follower_holding_v1(&mut leader, &room);

    let ops = deny_b(&mut alice_doc);
    assert!(leader.deliver(alice, Message::Ops { channel: CH, ops }));
    leader.take_outbox(alice);
    leader.take_replication();
    assert!(
        !holds_a_read_deny(&promoted, &room),
        "the revoke is withheld"
    );

    promoted
        .membership_mut_for_test()
        .mark_node_down(&NodeId::from_addr(A));

    let bob = auth(&mut promoted, 2, "t-bob");
    assert!(promoted.deliver(bob, subscribe(&room)));
    promoted.take_outbox(bob);
    assert!(promoted.deliver(bob, fetch(V1)));
    let state = served_state(&promoted.take_outbox(bob)).expect("the promoted node serves");
    assert!(
        carries(&state, b"b", b"bseed"),
        "the promotion residue is closed — if C111 is fixed, retire this pin",
    );
}

#[test]
fn a_version_diff_takes_the_gate_and_a_branch_diff_does_not() {
    // A version diff is two of this room's captures through the same projection a
    // fetch runs, so it is routed with the fetch. A branch diff is left alone: what
    // a replica may answer about a branch, and whether one unservable side refuses
    // the whole query, is C103's to rule on.
    let room = room_led_by_a_with_b_next();
    let (mut leader, _alice_doc, _alice) = seeded_leader(&room);
    let (mut follower, _peer) = caught_up_follower_holding_v1(&mut leader, &room);

    let bob = auth(&mut follower, 2, "t-bob");
    assert!(follower.deliver(bob, subscribe(&room)));
    follower.take_outbox(bob);

    assert!(follower.deliver(
        bob,
        Message::DiffQuery {
            channel: CH,
            kind: DiffKind::Versions,
            a: V1.to_vec(),
            b: V1.to_vec(),
        }
    ));
    assert_eq!(
        follower.take_outbox(bob),
        redirect_to_a(&room),
        "a version diff would serve the same captured bytes the fetch withholds",
    );

    assert!(follower.deliver(
        bob,
        Message::DiffQuery {
            channel: CH,
            kind: DiffKind::Branches,
            a: b"main".to_vec(),
            b: b"main".to_vec(),
        }
    ));
    let out = follower.take_outbox(bob);
    assert!(
        out.iter().any(|m| matches!(m, Message::DiffResult { .. })),
        "the branch arm no longer answers off a replica: {out:?}",
    );
}

// --- the rest of the sub-protocol on one routing ---

#[test]
fn a_version_list_on_a_replica_redirects() {
    // A list is names, not content, so it redacts nothing — but the names are this
    // node's own captures, which is the other half of why the read is the leader's.
    let room = room_led_by_a_with_b_next();
    let (mut leader, _alice_doc, _alice) = seeded_leader(&room);
    let (mut follower, _peer) = caught_up_follower_holding_v1(&mut leader, &room);

    let bob = auth(&mut follower, 2, "t-bob");
    assert!(follower.deliver(bob, subscribe(&room)));
    follower.take_outbox(bob);

    assert!(follower.deliver(bob, Message::VersionList { channel: CH }));
    assert_eq!(
        follower.take_outbox(bob),
        redirect_to_a(&room),
        "a list answered here would enumerate this node's captures, not the room's",
    );
}

#[test]
fn a_version_mutation_on_a_replica_still_redirects() {
    // A mutation persists, so it is the leader's and never lands here.
    let room = room_led_by_a_with_b_next();
    let (mut leader, _alice_doc, _alice) = seeded_leader(&room);
    let (mut follower, _peer) = caught_up_follower_holding_v1(&mut leader, &room);

    let alice = auth(&mut follower, 3, "t-alice");
    assert!(follower.deliver(alice, subscribe(&room)));
    follower.take_outbox(alice);

    assert!(follower.deliver(
        alice,
        Message::VersionCreate {
            channel: CH,
            name: b"v2".to_vec(),
        }
    ));
    assert_eq!(
        follower.take_outbox(alice),
        redirect_to_a(&room),
        "a version mutation on a replica is redirected, never persisted",
    );
    assert!(
        follower.hub().version_state(&room, b"v2").is_none(),
        "the redirected mutation still captured a version locally",
    );
}

#[test]
fn a_version_read_on_an_unbound_channel_is_a_violation_not_a_redirect() {
    // Where the gate sits: above the authorization, below the channel. The channel is
    // what names the room a redirect would point at, so a request that names no bound
    // channel is a protocol violation, not something to route.
    let room = room_led_by_a_with_b_next();
    let mut follower = node(Some(B), &room);
    let bob = auth(&mut follower, 2, "t-bob");

    assert!(
        !follower.deliver(bob, fetch(V1)),
        "a version read naming no bound channel closes the connection",
    );
    let out = follower.take_outbox(bob);
    assert!(
        out.iter().any(|m| matches!(
            m,
            Message::Error {
                code: ErrorCode::ProtocolViolation,
                ..
            }
        )),
        "the unbound channel is reported as a violation: {out:?}",
    );
    assert!(
        !out.iter().any(|m| matches!(m, Message::Redirect { .. })),
        "routing ran before the channel resolved: {out:?}",
    );
}

#[test]
fn a_reader_whose_room_read_was_revoked_is_routed_before_it_is_refused() {
    // What the ordering costs. A reader who bound a channel and then lost its room read
    // is told where the room is answered before it is told it may not read it — the
    // redirect names a room this client itself supplied at subscribe and a leader any
    // authenticated actor can already resolve, so it discloses nothing the subscribe
    // gate does not. The node that does answer the read is the node that refuses it.
    let room = room_led_by_a_with_b_next();
    let mut follower = node(Some(B), &room);
    // Leadership flaps under the bound channel: B leads while bob subscribes, then A
    // returns and B is a replica again — the only way a channel is bound on a node
    // that will later redirect it.
    follower
        .membership_mut_for_test()
        .mark_node_down(&NodeId::from_addr(A));
    let bob = auth(&mut follower, 2, "t-bob");
    assert!(follower.deliver(bob, subscribe(&room)));
    follower.take_outbox(bob);
    follower
        .membership_mut_for_test()
        .mark_node_live(&NodeId::from_addr(A));

    let revoked = Acl::new().deny(
        Subject::Actor(b"bob".to_vec()),
        None,
        ResourceMatch::Room(room.clone()),
    );
    follower.set_authorizer(Box::new(revoked.clone()));
    // Every frame of the seam takes the order, so every one is measured — a regression
    // on any single one is otherwise invisible.
    for request in channel_keyed_frames() {
        let named = format!("{request:?}");
        assert!(follower.deliver(bob, request));
        assert_eq!(
            follower.take_outbox(bob),
            redirect_to_a(&room),
            "the replica decided a gate it will not answer, for {named}",
        );
    }

    // The leader, which does answer, refuses. Same order: bob binds while he may read,
    // and the grant goes away under the bound channel.
    let mut leader = node(Some(A), &room);
    let bob = auth(&mut leader, 2, "t-bob");
    assert!(leader.deliver(bob, subscribe(&room)));
    leader.take_outbox(bob);
    leader.set_authorizer(Box::new(revoked));
    for request in channel_keyed_frames() {
        let named = format!("{request:?}");
        assert!(leader.deliver(bob, request));
        let out = leader.take_outbox(bob);
        assert!(
            out.iter().any(|m| matches!(
                m,
                Message::Error {
                    code: ErrorCode::Forbidden,
                    ..
                }
            )),
            "the node that answers the request did not refuse it, for {named}: {out:?}",
        );
    }
}

#[test]
fn an_empty_room_on_its_leader_still_serves_a_version_read() {
    // The gate resolves the leader and nothing else — it never asks whether this
    // node holds a materialized copy — so a leader whose room has no ops yet answers
    // its own (empty) version read rather than redirecting to itself.
    let room = room_led_by_a_with_b_next();
    let mut leader = node(Some(A), &room);
    let alice = auth(&mut leader, 1, "t-alice");
    assert!(leader.deliver(alice, subscribe(&room)));
    leader.take_outbox(alice);
    assert!(!leader.hub().holds_room(&room), "the room has no ops yet");

    assert!(leader.deliver(alice, Message::VersionList { channel: CH }));
    assert!(
        matches!(
            leader.take_outbox(alice).first(),
            Some(Message::Versions { names, .. }) if names.is_empty()
        ),
        "an unwritten room on its leader answers its own empty version list",
    );
}

#[test]
fn a_stranded_node_refuses_a_version_read_rather_than_answering_it_alone() {
    // The gate resolves the room's leadership, and a node whose ring has emptied has
    // none to resolve — so it refuses rather than answering from a replica no quorum
    // stands behind. The same reading the seam's mutations and an ops write already
    // take, now shared by its reads.
    let room = room_led_by_a_with_b_next();
    let mut stranded = node(Some(A), &room);
    let alice = auth(&mut stranded, 1, "t-alice");
    assert!(stranded.deliver(alice, subscribe(&room)));
    stranded.take_outbox(alice);

    {
        let view = stranded.membership_mut_for_test();
        let peers: Vec<NodeId> = view
            .members()
            .into_iter()
            .filter(|n| n != view.self_id())
            .collect();
        for peer in &peers {
            for _ in 0..crdtsync_server::membership::DEAD_AFTER_FAILURES {
                view.note_gossip_unreachable(peer);
            }
        }
        for _ in 0..crdtsync_server::membership::REAP_AFTER_DEAD_TICKS {
            view.reap_dead();
        }
        assert!(view.is_stranded(), "it can no longer rebuild a ring");
    }

    assert!(stranded.deliver(alice, Message::VersionList { channel: CH }));
    let out = stranded.take_outbox(alice);
    assert!(
        out.iter().any(|m| matches!(
            m,
            Message::Error {
                code: ErrorCode::Internal,
                ..
            }
        )),
        "a stranded node answered a version read from its own state: {out:?}",
    );
}

#[test]
fn single_node_serves_every_version_read() {
    // No membership: every room is local and nothing routes, unchanged.
    let room = b"any".to_vec();
    let mut r = node(None, &room);
    let alice = auth(&mut r, 1, "t-alice");
    assert!(r.deliver(alice, subscribe(&room)));
    r.take_outbox(alice);

    assert!(r.deliver(alice, Message::VersionList { channel: CH }));
    assert!(
        matches!(r.take_outbox(alice).first(), Some(Message::Versions { .. })),
        "single-node answers every version read",
    );
}
