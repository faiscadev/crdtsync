//! A room's governing metadata is replicated — a replica that holds a room holds the
//! op-version high-water its handshake range-check enforces, and the app binding that
//! high-water is a version of (C54).
//!
//! `Room::max_op_version` is the worst-case governing-app op version a joiner must be
//! able to down-reach; `subscriber_reaches_governing` refuses an under-versioned joiner
//! with `onUpdateRequired` against it. A client write raises it through the
//! `schema_version` it carries into `Hub::ingest`, which is a seam a replica never
//! reaches: its ops arrive already committed on the leader. So the frame carrying the
//! batch carries the high-water beside it, and the binding beside that — a high-water is
//! a number in some app's version space, and `governing_target` abstains on an unbound
//! room, so a replica holding the number without the app decides nothing with it. Both
//! ride the two frames the creator rides (C29), which is the same record the store
//! writes as one `RoomMeta`.
//!
//! These pin that a brand-new replica reports the leader's high-water and binding
//! through the ops path and through the state-transfer install; that an under-versioned
//! joiner is refused *there*, not only on the leader; that a promoted replica keeps
//! enforcing it after a failover; that a replicated lift evicts a follower-local
//! subscriber it strands, as a local write does; and how both fields compose against
//! what a replica already holds.
//!
//! The app `APP` is registered at v1 and v2 across a `renameField` — a breaking,
//! forward-only edge — so a v1 joiner genuinely cannot down-reach a v2 high-water while
//! a v2 joiner can. `SAFE` is the same shape across a back-compatible `addField`, the
//! control that shows the gate refuses only what it must.
//!
//! Two in-process registries over one static cluster, no socket and a fixed clock — the
//! leader commits, its replication frames are handed to the follower, and a client then
//! subscribes on the follower directly. Deterministic and Miri-clean, apart from the one
//! restart case, which reopens a real store and so is skipped under Miri's isolation.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Op, Scalar};
use crdtsync_server::membership::Membership;
use crdtsync_server::placement::NodeId;
use crdtsync_server::{ConnId, ManualClock, Registry, SchemaRegistry};

const CH: Channel = Channel(0);
const N: usize = 3;
const A: &str = "10.0.0.1:9000";
const B: &str = "10.0.0.2:9000";

/// v1→v2 renames `age`→`years`: a breaking, forward-only edge, so v1 cannot
/// down-reach a v2 high-water.
const APP: &[u8] = b"breaking";
/// v1→v2 adds a `note` field: a back-compatible edge a v1 joiner reaches down over.
const SAFE: &[u8] = b"compatible";

const MAP_V1: &str = r#"{ "schema": "s", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } } }"#;
const MAP_V2: &str = r#"{ "schema": "s", "version": 2, "root": "R",
    "types": { "R": { "kind": "map" } } }"#;
const MAP_V3: &str = r#"{ "schema": "s", "version": 3, "root": "R",
    "types": { "R": { "kind": "map" } } }"#;

/// A zone-declaring app: what a replica can only resolve through the room's binding,
/// since every enumeration of a room's zones comes from its governing schema.
const ZONED: &[u8] = b"zoned";
const ZONED_V1: &str = r#"{ "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "board": "Sect", "notes": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "zones": { "za": "/board", "zb": "/notes" } }"#;

/// The cluster secret these nodes share — what admits a node-to-node link to a peer's
/// replication plane.
const CLUSTER_SECRET: &[u8] = b"peer-plane-cluster-secret-for-tests";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn schema_registry() -> SchemaRegistry {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, MAP_V1.as_bytes(), b"").unwrap();
    sr.register(
        APP,
        2,
        MAP_V2.as_bytes(),
        br#"{ "from": 1, "to": 2, "steps": [ { "kind": "renameField", "type": "R", "from": "age", "to": "years" } ] }"#,
    )
    .unwrap();
    sr.register(
        APP,
        3,
        MAP_V3.as_bytes(),
        br#"{ "from": 2, "to": 3, "steps": [ { "kind": "renameField", "type": "R", "from": "years", "to": "decades" } ] }"#,
    )
    .unwrap();
    sr.register(ZONED, 1, ZONED_V1.as_bytes(), b"").unwrap();
    sr.register(SAFE, 1, MAP_V1.as_bytes(), b"").unwrap();
    sr.register(
        SAFE,
        2,
        MAP_V2.as_bytes(),
        br#"{ "from": 1, "to": 2, "steps": [ { "kind": "addField", "type": "R", "field": "note", "fieldType": "text" } ] }"#,
    )
    .unwrap();
    sr
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

/// A single-node registry over the same version chains — no membership, so it leads
/// every room it holds and nothing is replicated. What the clone and import seams are
/// measured on: neither is a replication path, and both share the install body the
/// state-transfer frame uses.
fn solo() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(schema_registry())));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

/// A cluster node whose self is `self_addr`, sharing the version chains above.
fn node(self_addr: &str) -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(schema_registry())));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for(self_addr));
    r.set_cluster_secret(CLUSTER_SECRET.to_vec());
    r
}

/// Hello + Auth a connection declaring `{app, version}`.
fn hello(r: &mut Registry, client: u8, app: &[u8], version: u32) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: app.to_vec(),
            schema_version: version,
            codecs: Vec::new(),
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

fn sub(room: &[u8]) -> Message {
    Message::Subscribe {
        channel: CH,
        room: room.to_vec(),
        branch: Vec::new(),
        zone: Vec::new(),
        last_seen_seq: 0,
    }
}

/// Deliver a Subscribe for `room` from `id` and return its reply messages.
fn subscribe_reply(r: &mut Registry, id: ConnId, room: &[u8]) -> Vec<Message> {
    assert!(r.deliver(id, sub(room)));
    r.take_outbox(id)
}

/// Whether `replies` carry an `UpdateRequired` error.
fn is_update_required(replies: &[Message]) -> bool {
    replies.iter().any(|m| {
        matches!(
            m,
            Message::Error {
                code: ErrorCode::UpdateRequired,
                ..
            }
        )
    })
}

/// Whether `replies` carry a catch-up — a successful subscribe.
fn is_subscribed(replies: &[Message]) -> bool {
    replies
        .iter()
        .any(|m| matches!(m, Message::Ops { .. } | Message::Snapshot { .. }))
}

fn set(client: u8, key: &[u8]) -> Vec<Op> {
    Document::new(cid(client)).transact(|tx| tx.register(key, Scalar::Int(1)))
}

/// Deliver a write from an already-subscribed enforcing connection and assert the hub
/// logged it, so the op lands tagged at the writer's version — what raises the room's
/// high-water. The `Accepted` reply is held back until the room's replica set confirms
/// the write durable, so what is asserted here is the commit, not the acknowledgement.
fn write(r: &mut Registry, id: ConnId, client: u8, room: &[u8], key: &[u8]) {
    let before = r.hub().seq(room);
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: CH,
            ops: set(client, key),
        }
    ));
    let replies = r.take_outbox(id);
    assert!(
        !replies
            .iter()
            .any(|m| matches!(m, Message::OpsRejected { .. } | Message::Error { .. })),
        "the write must not be refused: {replies:?}",
    );
    assert!(
        r.hub().seq(room) > before,
        "the write must reach the room's log",
    );
}

/// A room `A` leads and `B` replicates second in HRW order — so `B` follows it while
/// `A` is up, and is the node promoted to lead it when `A` goes down.
fn room_led_by_a_with_b_next() -> Vec<u8> {
    nth_room_led_by_a_with_b_next(0)
}

/// The `n`th such room, so a test needing two distinct ones on the same pair of nodes
/// has them.
fn nth_room_led_by_a_with_b_next(n: usize) -> Vec<u8> {
    let m = membership_for(A);
    let a = NodeId::from_addr(A);
    let b = NodeId::from_addr(B);
    let mut seen = 0;
    for i in 0..1_000_000 {
        let room = format!("room-{i}").into_bytes();
        let replicas = m.replicas_for(&room);
        if replicas.first() == Some(&a) && replicas.get(1) == Some(&b) {
            if seen == n {
                return room;
            }
            seen += 1;
        }
    }
    panic!("no room led by A with B its next replica");
}

/// Deliver `frame` to `r` on `peer` and assert it applied rather than being fenced or
/// deduped away — so a composition assertion beneath it is guarded by the frame having
/// run at all, not satisfied by a frame the gate refused.
fn deliver_landed(r: &mut Registry, peer: ConnId, room: &[u8], frame: Message) {
    let before = r.hub().seq(room);
    assert!(r.deliver(peer, frame), "the connection stays open");
    assert!(
        r.hub().seq(room) > before,
        "the frame's ops landed, so the gate admitted it",
    );
}

/// A connection admitted to `r`'s peer plane as one of `room`'s other replicas, as a
/// member's dialed link is.
fn peer_conn(r: &mut Registry, room: &[u8]) -> ConnId {
    let node = r
        .membership()
        .and_then(|m| m.replicas_for(room).into_iter().find(|n| !m.is_self(n)))
        .expect("the room has another replica");
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

/// A leader holding `room`, bound to `app` at `version` by its writer, with one op
/// written at that version — so the room's high-water is `version`.
fn seeded_leader(room: &[u8], app: &[u8], version: u32) -> Registry {
    let mut leader = node(A);
    let writer = hello(&mut leader, 1, app, version);
    let replies = subscribe_reply(&mut leader, writer, room);
    assert!(is_subscribed(&replies), "the binder itself subscribes");
    write(&mut leader, writer, 1, room, b"years");
    assert_eq!(
        leader.hub().max_op_version(room),
        Some(version),
        "the leader's own high-water is the writer's version",
    );
    leader
}

/// Hand every replication frame the leader queued for `B` to a fresh follower — the ops
/// path, since nothing has compacted.
fn follower_by_ops(leader: &mut Registry, room: &[u8]) -> Registry {
    let b = NodeId::from_addr(B);
    let mut follower = node(B);
    let peer = peer_conn(&mut follower, room);
    let mut applied = 0;
    for (target, frame) in leader.take_replication() {
        if target != b {
            continue;
        }
        assert!(
            matches!(frame, Message::Replicate { .. }),
            "an uncompacted room replicates by ops: {frame:?}",
        );
        assert!(follower.deliver(peer, frame), "the follower applies it");
        applied += 1;
    }
    assert!(applied > 0, "the leader replicated something");
    assert_eq!(
        follower.hub().export_room(room),
        leader.hub().export_room(room),
        "the follower converged with the leader",
    );
    follower
}

/// Catch a fresh follower up on `room` by the below-floor snapshot state-transfer: the
/// leader compacts every commit into its snapshot, so a watermark-0 follower is dialed a
/// whole-replica `ReplicateSnapshot` rather than an ops delta.
fn follower_by_snapshot(leader: &mut Registry, room: &[u8]) -> Registry {
    let b = NodeId::from_addr(B);
    leader.hub_mut().compact(room).expect("the room compacts");
    // Drop the steady-path frames: the follower has acked nothing, so its watermark is 0
    // and the dial below is what converges it.
    leader.take_replication();
    assert!(
        leader.hub().base_seq(room) > 0,
        "the room compacted above the floor",
    );
    leader.catch_up_follower(&b);
    let mut frames: Vec<Message> = leader
        .take_replication()
        .into_iter()
        .filter(|(target, _)| *target == b)
        .map(|(_, frame)| frame)
        .collect();
    assert_eq!(frames.len(), 1, "exactly one catch-up frame: {frames:?}");
    let frame = frames.pop().expect("one frame");
    assert!(
        matches!(frame, Message::ReplicateSnapshot { .. }),
        "a below-floor follower is caught up by a snapshot: {frame:?}",
    );

    let mut follower = node(B);
    let peer = peer_conn(&mut follower, room);
    assert!(follower.deliver(peer, frame), "the follower applies it");
    assert_eq!(
        follower.hub().export_room(room),
        leader.hub().export_room(room),
        "the follower converged with the leader",
    );
    follower
}

// --- a replica reports the room's high-water and binding, not `None` ---

#[test]
fn a_replica_converged_by_ops_holds_the_rooms_high_water() {
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let follower = follower_by_ops(&mut leader, &room);
    assert_eq!(
        follower.hub().max_op_version(&room),
        Some(2),
        "the ops frame carries the leader's high-water",
    );
}

#[test]
fn a_replica_converged_by_ops_holds_the_rooms_binding() {
    // The high-water is a number in the governing app's version space, so a replica
    // that held it without the binding could not use it: `governing_target` abstains on
    // an unbound room and the gate stands down.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let follower = follower_by_ops(&mut leader, &room);
    assert_eq!(
        follower.hub().governing_app(&room),
        Some((APP.to_vec(), 2)),
        "the ops frame carries the leader's governing binding",
    );
}

#[test]
fn a_replica_installed_from_a_snapshot_holds_the_rooms_high_water_and_binding() {
    // The state bytes are a `Document` encoding and carry no server-side metadata, so
    // the state-transfer install needs the frame to name both exactly as the ops path
    // does — and here there are no ops at all to infer one from.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let follower = follower_by_snapshot(&mut leader, &room);
    assert_eq!(follower.hub().max_op_version(&room), Some(2));
    assert_eq!(follower.hub().governing_app(&room), Some((APP.to_vec(), 2)));
}

// --- the range-check enforces on the replica, not only on the leader ---

#[test]
fn an_under_versioned_joiner_is_refused_on_a_replica_converged_by_ops() {
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let mut follower = follower_by_ops(&mut leader, &room);

    let old = hello(&mut follower, 2, APP, 1);
    let replies = subscribe_reply(&mut follower, old, &room);
    assert!(
        is_update_required(&replies),
        "a v1 joiner below the breaking rename is refused on the replica: {replies:?}",
    );
    assert!(
        !is_subscribed(&replies),
        "and it never becomes a subscriber there: {replies:?}",
    );
}

#[test]
fn an_under_versioned_joiner_is_refused_on_a_replica_installed_from_a_snapshot() {
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let mut follower = follower_by_snapshot(&mut leader, &room);

    let old = hello(&mut follower, 2, APP, 1);
    let replies = subscribe_reply(&mut follower, old, &room);
    assert!(
        is_update_required(&replies),
        "the state-transfer install carries the same refusal: {replies:?}",
    );
    assert!(!is_subscribed(&replies));
}

#[test]
fn the_first_joiner_on_a_replica_is_range_checked_too() {
    // The binding is what makes this the *first* joiner's problem and not only a later
    // one's: on a replica that adopted no binding, the first subscriber establishes one
    // at its own version, so it is admitted and every joiner after it refused. The
    // replicated binding is the incumbent before any client arrives.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let mut follower = follower_by_ops(&mut leader, &room);
    // The replica has taken no client subscribe, so nothing but the frame could have
    // bound the room — and the binding is already the leader's.
    assert_eq!(follower.hub().governing_app(&room), Some((APP.to_vec(), 2)));

    let first = hello(&mut follower, 2, APP, 1);
    let replies = subscribe_reply(&mut follower, first, &room);
    assert!(
        is_update_required(&replies),
        "the very first client to arrive at the replica is checked: {replies:?}",
    );
    // And the second, which a binding established by the first would also have caught —
    // so the assertion above is the one that distinguishes a carried binding from an
    // invented one.
    let second = hello(&mut follower, 3, APP, 1);
    assert!(is_update_required(&subscribe_reply(
        &mut follower,
        second,
        &room
    )));
}

#[test]
fn a_reachable_joiner_still_subscribes_on_a_replica() {
    // The control: the gate refuses only what it must. Over a back-compatible
    // `addField` a v1 joiner reaches down to a v2 high-water, so it joins the replica.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, SAFE, 2);
    let mut follower = follower_by_ops(&mut leader, &room);
    assert_eq!(follower.hub().max_op_version(&room), Some(2));

    let old = hello(&mut follower, 2, SAFE, 1);
    let replies = subscribe_reply(&mut follower, old, &room);
    assert!(
        !is_update_required(&replies),
        "a back-compatible gap never refuses: {replies:?}",
    );
    assert!(
        is_subscribed(&replies),
        "and the joiner is served: {replies:?}"
    );
}

#[test]
fn a_same_version_joiner_subscribes_on_a_replica() {
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let mut follower = follower_by_ops(&mut leader, &room);

    let peer = hello(&mut follower, 2, APP, 2);
    let replies = subscribe_reply(&mut follower, peer, &room);
    assert!(!is_update_required(&replies), "{replies:?}");
    assert!(is_subscribed(&replies), "{replies:?}");
}

#[test]
fn a_foreign_app_joiner_is_not_range_checked_on_a_replica() {
    // A foreign app is a different version space, so the replicated binding never
    // refuses it — the same rule the leader applies.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let mut follower = follower_by_ops(&mut leader, &room);

    let foreign = hello(&mut follower, 2, SAFE, 1);
    let replies = subscribe_reply(&mut follower, foreign, &room);
    assert!(
        !is_update_required(&replies),
        "a foreign-app joiner is a different version space: {replies:?}",
    );
    assert!(is_subscribed(&replies), "{replies:?}");
}

#[test]
fn a_dialed_ops_catch_up_carries_the_record_too() {
    // The late-joiner dial builds its own frame rather than reusing the steady fan-out's,
    // and its ops arm is a second copy of the three fields. A follower converged only
    // that way must hold the same record.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let b = NodeId::from_addr(B);
    // Drop the steady frames: the dial below is what converges this follower, and the
    // room is uncompacted so the dial takes the ops arm rather than the snapshot one.
    leader.take_replication();
    leader.catch_up_follower(&b);

    let mut follower = node(B);
    let peer = peer_conn(&mut follower, &room);
    let mut dialed = 0;
    for (target, frame) in leader.take_replication() {
        if target != b {
            continue;
        }
        assert!(
            matches!(frame, Message::Replicate { .. }),
            "an uncompacted room is dialed an ops delta: {frame:?}",
        );
        assert!(follower.deliver(peer, frame));
        dialed += 1;
    }
    assert_eq!(dialed, 1, "one catch-up frame");
    assert_eq!(follower.hub().max_op_version(&room), Some(2));
    assert_eq!(follower.hub().governing_app(&room), Some((APP.to_vec(), 2)));

    let old = hello(&mut follower, 2, APP, 1);
    assert!(is_update_required(&subscribe_reply(
        &mut follower,
        old,
        &room
    )));
}

// --- a promoted leader keeps enforcing it ---

#[test]
fn a_promoted_replica_still_refuses_an_under_versioned_joiner() {
    // The failover case the filing names: a promoted leader enforces the high-water it
    // was replicated, so it never down-translates across an edge it did not check was
    // invertible.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let mut follower = follower_by_ops(&mut leader, &room);
    follower.set_peer_liveness(NodeId::from_addr(A), false);
    follower
        .membership_mut_for_test()
        .mark_node_down(&NodeId::from_addr(A));

    let old = hello(&mut follower, 2, APP, 1);
    let replies = subscribe_reply(&mut follower, old, &room);
    assert!(
        is_update_required(&replies),
        "the promoted leader enforces the high-water it was replicated: {replies:?}",
    );
    assert!(!is_subscribed(&replies));
}

#[test]
fn a_promoted_replica_raises_the_high_water_on_its_own_writes() {
    // The replicated high-water is a floor the promoted leader's own writes lift, not a
    // frozen value: it composes as a max exactly as the write path composes one.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let mut follower = follower_by_ops(&mut leader, &room);
    follower.set_peer_liveness(NodeId::from_addr(A), false);
    follower
        .membership_mut_for_test()
        .mark_node_down(&NodeId::from_addr(A));

    let newer = hello(&mut follower, 3, APP, 3);
    let replies = subscribe_reply(&mut follower, newer, &room);
    assert!(
        is_subscribed(&replies),
        "a v3 joiner reaches up: {replies:?}"
    );
    write(&mut follower, newer, 3, &room, b"decades");
    assert_eq!(
        follower.hub().max_op_version(&room),
        Some(3),
        "the promoted leader's own write lifts the replicated floor",
    );
}

// --- composition against what the replica already holds ---

#[test]
fn a_frame_naming_a_lower_high_water_never_lowers_a_standing_one() {
    // The high-water is the all-time worst case, so it is composed as a max at every
    // seam. A frame is an assertion: a leader that compacted and re-sent, or a stale
    // one, must not be able to talk a replica down into admitting a joiner the state
    // still defeats.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let mut follower = follower_by_ops(&mut leader, &room);
    let peer = peer_conn(&mut follower, &room);

    deliver_landed(
        &mut follower,
        peer,
        &room,
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops: set(9, b"late"),
            base_seq: 0,
            epoch: 1,
            creator: None,
            governing: Some((APP.to_vec(), 1)),
            max_op_version: Some(1),
        },
    );
    assert_eq!(
        follower.hub().max_op_version(&room),
        Some(2),
        "the standing high-water stands",
    );
}

#[test]
fn a_frame_naming_no_high_water_never_drops_a_standing_one() {
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let mut follower = follower_by_ops(&mut leader, &room);
    let peer = peer_conn(&mut follower, &room);

    deliver_landed(
        &mut follower,
        peer,
        &room,
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops: set(9, b"late"),
            base_seq: 0,
            epoch: 1,
            creator: None,
            governing: None,
            max_op_version: None,
        },
    );
    assert_eq!(follower.hub().max_op_version(&room), Some(2));
    assert_eq!(follower.hub().governing_app(&room), Some((APP.to_vec(), 2)));
}

#[test]
fn a_resent_snapshot_never_lowers_a_standing_high_water() {
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let mut follower = follower_by_snapshot(&mut leader, &room);
    let state = leader
        .hub()
        .export_room(&room)
        .expect("the leader holds it");
    let peer = peer_conn(&mut follower, &room);

    // A sequence above the follower's head, so the install is observable: the frame
    // lands the replica there, which is what shows the state was replaced rather than
    // the frame fenced.
    let landed_at = leader.hub().seq(&room) + 5;
    assert!(follower.deliver(
        peer,
        Message::ReplicateSnapshot {
            room: room.clone(),
            branch: b"main".to_vec(),
            seq: landed_at,
            state: state.clone(),
            epoch: 1,
            creator: None,
            governing: Some((APP.to_vec(), 1)),
            max_op_version: Some(1),
        },
    ));
    assert_eq!(
        follower.hub().seq(&room),
        landed_at,
        "the snapshot installed, so the composition below is the install's",
    );
    assert_eq!(
        follower.hub().max_op_version(&room),
        Some(2),
        "replacing the state never drops the all-time worst case",
    );

    // And a snapshot naming none leaves it alone, the arm the ops seam pins separately.
    assert!(follower.deliver(
        peer,
        Message::ReplicateSnapshot {
            room: room.clone(),
            branch: b"main".to_vec(),
            seq: landed_at + 5,
            state,
            epoch: 1,
            creator: None,
            governing: None,
            max_op_version: None,
        },
    ));
    assert_eq!(follower.hub().seq(&room), landed_at + 5);
    assert_eq!(follower.hub().max_op_version(&room), Some(2));
    assert_eq!(follower.hub().governing_app(&room), Some((APP.to_vec(), 2)));
}

#[test]
fn a_replicated_binding_never_re_governs_a_room_under_another_app() {
    // A frame's binding composes on `bind_room_app`'s rule, the one the durable load and
    // a client subscribe both use: the incumbent app keeps the room, and only a
    // same-app assertion lifts the version.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let mut follower = follower_by_ops(&mut leader, &room);
    let peer = peer_conn(&mut follower, &room);

    deliver_landed(
        &mut follower,
        peer,
        &room,
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops: set(9, b"late"),
            base_seq: 0,
            epoch: 1,
            creator: None,
            governing: Some((SAFE.to_vec(), 2)),
            max_op_version: Some(2),
        },
    );
    assert_eq!(
        follower.hub().governing_app(&room),
        Some((APP.to_vec(), 2)),
        "the incumbent app is not displaced by a frame naming another",
    );
}

// --- a replicated lift strands a follower-local subscriber, as a local write does ---

#[test]
fn a_replicated_lift_evicts_a_stranded_follower_local_subscriber() {
    // A replicated batch re-decides every per-recipient verdict against the local
    // replica (C59); the high-water is one of those verdicts. A subscriber admitted at
    // the replica's old high-water that the leader's next commit opens past is evicted
    // with `UpdateRequired` — exactly as a local write evicts one — rather than left
    // silently un-updated on a stream the fan-out fail-closes its down-drop for.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 1);
    let mut follower = follower_by_ops(&mut leader, &room);
    assert_eq!(follower.hub().max_op_version(&room), Some(1));

    let old = hello(&mut follower, 2, APP, 1);
    let replies = subscribe_reply(&mut follower, old, &room);
    assert!(
        is_subscribed(&replies),
        "a v1 joiner joins a v1 room: {replies:?}"
    );

    // The leader takes a v2 write, lifting the room past the v1 subscriber's reach.
    let newer = hello(&mut leader, 3, APP, 2);
    assert!(is_subscribed(&subscribe_reply(&mut leader, newer, &room)));
    write(&mut leader, newer, 3, &room, b"years");
    assert_eq!(leader.hub().max_op_version(&room), Some(2));

    let b = NodeId::from_addr(B);
    let peer = peer_conn(&mut follower, &room);
    for (target, frame) in leader.take_replication() {
        if target == b {
            assert!(follower.deliver(peer, frame));
        }
    }
    assert_eq!(follower.hub().max_op_version(&room), Some(2));

    let out = follower.take_outbox(old);
    assert!(
        is_update_required(&out),
        "the stranded follower-local subscriber is evicted: {out:?}",
    );
}

#[test]
fn a_stranded_follower_local_subscriber_is_served_no_op_above_its_reach() {
    // What makes the eviction match the leader's rather than merely accompany it. A
    // leader's fan-out translates per recipient and drops a batch whose chain will not
    // resolve, so its stranded peer is told to update and handed nothing. A replicated
    // fan-out translates nothing (C71), so a peer still in the room when it runs would
    // apply a v2 op its own version cannot model. The two must agree.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 1);
    let mut follower = follower_by_ops(&mut leader, &room);

    let old = hello(&mut follower, 2, APP, 1);
    assert!(is_subscribed(&subscribe_reply(&mut follower, old, &room)));

    let newer = hello(&mut leader, 3, APP, 2);
    assert!(is_subscribed(&subscribe_reply(&mut leader, newer, &room)));
    write(&mut leader, newer, 3, &room, b"years");

    // The leader's own stranded subscriber: the reference behaviour.
    let leader_side = hello(&mut leader, 4, APP, 1);
    let replies = subscribe_reply(&mut leader, leader_side, &room);
    assert!(
        is_update_required(&replies),
        "the leader refuses a v1 joiner at high-water 2: {replies:?}",
    );

    let b = NodeId::from_addr(B);
    let peer = peer_conn(&mut follower, &room);
    for (target, frame) in leader.take_replication() {
        if target == b {
            assert!(follower.deliver(peer, frame));
        }
    }

    let out = follower.take_outbox(old);
    assert!(
        is_update_required(&out),
        "the stranded subscriber is evicted: {out:?}",
    );
    assert!(
        !out.iter().any(|m| matches!(m, Message::Ops { .. })),
        "and is handed no op above its reach on the way out: {out:?}",
    );
}

#[test]
fn a_high_water_from_another_apps_version_space_is_not_adopted() {
    // The binding composes incumbent-wins, so a frame whose app lost that composition
    // names a number in a version space this room is not read in. Adopting it anyway
    // would judge someone else's version against the incumbent's chain — refusing a
    // joiner nothing stranded, or admitting one the room's own state defeats. The two
    // fields are one record and travel together or not at all.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 1);
    let mut follower = follower_by_ops(&mut leader, &room);
    assert_eq!(follower.hub().governing_app(&room), Some((APP.to_vec(), 1)));
    assert_eq!(follower.hub().max_op_version(&room), Some(1));

    let joiner = hello(&mut follower, 2, APP, 1);
    assert!(is_subscribed(&subscribe_reply(
        &mut follower,
        joiner,
        &room
    )));

    // A frame naming another app and a number from that app's space.
    let peer = peer_conn(&mut follower, &room);
    deliver_landed(
        &mut follower,
        peer,
        &room,
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops: set(9, b"late"),
            base_seq: 0,
            epoch: 1,
            creator: None,
            governing: Some((SAFE.to_vec(), 2)),
            max_op_version: Some(2),
        },
    );
    assert_eq!(
        follower.hub().governing_app(&room),
        Some((APP.to_vec(), 1)),
        "the incumbent app stands",
    );
    assert_eq!(
        follower.hub().max_op_version(&room),
        Some(1),
        "and the number that came with the refused app stands out with it",
    );
    let out = follower.take_outbox(joiner);
    assert!(
        !is_update_required(&out),
        "so nothing evicts a joiner the room's own chain still reaches: {out:?}",
    );
}

#[test]
fn a_high_water_arriving_with_no_binding_is_not_adopted() {
    // The other arm of the same rule. A number with no app named beside it says nothing
    // — there is no chain to read it in — so a fresh replica does not take one. The only
    // way a leader sends this pair is with its metadata write lost while its log keeps
    // the versions its ops carry (C55), and inventing an attribution for it here would
    // hand whichever app binds the room next a floor measured in someone else's space.
    let room = room_led_by_a_with_b_next();
    let mut follower = node(B);
    let peer = peer_conn(&mut follower, &room);
    deliver_landed(
        &mut follower,
        peer,
        &room,
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops: set(9, b"x"),
            base_seq: 0,
            epoch: 1,
            creator: None,
            governing: None,
            max_op_version: Some(9),
        },
    );
    assert_eq!(follower.hub().governing_app(&room), None);
    assert_eq!(
        follower.hub().max_op_version(&room),
        None,
        "an unattributed high-water is not a floor for whoever binds the room next",
    );
}

#[test]
fn a_replicated_batch_that_lifts_nothing_evicts_no_one() {
    // Only a genuine lift re-checks: a same-version replicated batch moves nothing, so a
    // joined subscriber keeps its subscription.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let mut follower = follower_by_ops(&mut leader, &room);

    let peer_client = hello(&mut follower, 2, APP, 2);
    assert!(is_subscribed(&subscribe_reply(
        &mut follower,
        peer_client,
        &room
    )));

    let writer = hello(&mut leader, 3, APP, 2);
    assert!(is_subscribed(&subscribe_reply(&mut leader, writer, &room)));
    write(&mut leader, writer, 3, &room, b"more");

    let b = NodeId::from_addr(B);
    let peer = peer_conn(&mut follower, &room);
    for (target, frame) in leader.take_replication() {
        if target == b {
            assert!(follower.deliver(peer, frame));
        }
    }
    let out = follower.take_outbox(peer_client);
    assert!(
        !is_update_required(&out),
        "a same-version batch strands no one: {out:?}",
    );
    assert!(
        out.iter().any(|m| matches!(m, Message::Ops { .. })),
        "and the batch still reached it: {out:?}",
    );
}

#[test]
fn a_state_transfer_that_lifts_evicts_a_stranded_follower_local_subscriber() {
    // The state-transfer path lifts the high-water on the same terms the ops path does
    // — and this is the path with no ops at all, so the frame's record is the whole
    // lift. A subscriber it strands is evicted on the same rule.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 1);
    let mut follower = follower_by_ops(&mut leader, &room);
    assert_eq!(follower.hub().max_op_version(&room), Some(1));

    let old = hello(&mut follower, 2, APP, 1);
    assert!(is_subscribed(&subscribe_reply(&mut follower, old, &room)));

    // The leader takes a v2 write and compacts, so the follower's dial is a snapshot.
    let newer = hello(&mut leader, 3, APP, 2);
    assert!(is_subscribed(&subscribe_reply(&mut leader, newer, &room)));
    write(&mut leader, newer, 3, &room, b"years");
    leader.hub_mut().compact(&room).expect("the room compacts");
    leader.take_replication();

    let b = NodeId::from_addr(B);
    leader.catch_up_follower(&b);
    let peer = peer_conn(&mut follower, &room);
    let mut sent = 0;
    for (target, frame) in leader.take_replication() {
        if target != b {
            continue;
        }
        assert!(
            matches!(frame, Message::ReplicateSnapshot { .. }),
            "a below-floor follower is caught up by a snapshot: {frame:?}",
        );
        assert!(follower.deliver(peer, frame));
        sent += 1;
    }
    assert_eq!(sent, 1, "exactly one state-transfer frame");
    assert_eq!(follower.hub().max_op_version(&room), Some(2));

    let out = follower.take_outbox(old);
    assert!(
        is_update_required(&out),
        "the state transfer's lift evicts the subscriber it stranded: {out:?}",
    );
}

#[test]
fn a_replica_resolves_the_rooms_zones_from_the_replicated_binding() {
    // The binding half's other consequence, and C62's: every enumeration of a room's
    // zones comes from its governing schema, resolved through the room's `{app, version}`
    // binding. A replica that holds the room and not the binding reads "no schema" as
    // "declares no zones", so a named-zone subscribe there is refused as nonexistent.
    let room = room_led_by_a_with_b_next();
    let mut leader = node(A);
    let writer = hello(&mut leader, 1, ZONED, 1);
    assert!(is_subscribed(&subscribe_reply(&mut leader, writer, &room)));
    // A schema-conforming write into the `za` subtree, so the room is populated and
    // bound before it replicates.
    let before = leader.hub().seq(&room);
    assert!(leader.deliver(
        writer,
        Message::Ops {
            channel: CH,
            ops: Document::new(cid(1))
                .transact(|tx| tx.map(b"board").register(b"seed", Scalar::Int(1))),
        }
    ));
    assert!(leader.hub().seq(&room) > before, "the zoned write landed");
    leader.take_outbox(writer);
    let mut follower = follower_by_ops(&mut leader, &room);
    assert_eq!(
        follower.hub().governing_app(&room),
        Some((ZONED.to_vec(), 1))
    );

    // The reader speaks another app. That is what makes the assertion discriminating: if
    // the binding had not travelled, the room would be unbound here and this subscribe
    // would bind it to `APP`, which declares no zones — so `za` would be refused as
    // nonexistent rather than served.
    let reader = hello(&mut follower, 2, APP, 1);
    assert!(follower.deliver(
        reader,
        Message::Subscribe {
            channel: CH,
            room: room.clone(),
            branch: Vec::new(),
            zone: b"za".to_vec(),
            last_seen_seq: 0,
        }
    ));
    let replies = follower.take_outbox(reader);
    assert!(
        is_subscribed(&replies),
        "the replica declares `za` because the binding travelled: {replies:?}",
    );

    // And a zone the schema does not declare is still refused, so the assertion above is
    // the binding resolving rather than the gate standing down.
    let other = hello(&mut follower, 3, APP, 1);
    assert!(follower.deliver(
        other,
        Message::Subscribe {
            channel: CH,
            room: room.clone(),
            branch: Vec::new(),
            zone: b"zc".to_vec(),
            last_seen_seq: 0,
        }
    ));
    let replies = follower.take_outbox(other);
    assert!(
        !is_subscribed(&replies),
        "an undeclared zone is refused: {replies:?}",
    );
}

#[test]
fn a_leader_whose_hub_binding_a_sweep_dropped_still_replicates_a_bound_record() {
    // The room's binding lives in two places: the live presence map, and the hub's own
    // record. A dormant-room sweep prunes the hub's for a room the hub does not yet hold
    // — a subscribe binds before the first write materializes anything — while the map
    // keeps it, because a subscriber is present. The first write then tags its op from
    // the map and raises the high-water, leaving the room holding a high-water the hub
    // has no binding for. A replication record read from the hub alone would carry that
    // number with no app beside it, and a replica adopting only attributed numbers would
    // discard it — which is this unit's own defect, re-entered through its own gate. So
    // the record is read the way the write tag is.
    let room = room_led_by_a_with_b_next();
    let mut leader = node(A);
    let writer = hello(&mut leader, 1, APP, 2);
    assert!(is_subscribed(&subscribe_reply(&mut leader, writer, &room)));

    // The subscribe bound a room nothing has written yet, so the sweep prunes the hub's
    // copy while the subscriber keeps the live one.
    leader.sweep();
    assert_eq!(
        leader.hub().governing_app(&room),
        None,
        "the sweep pruned the hub's binding for a room it does not hold",
    );

    write(&mut leader, writer, 1, &room, b"years");
    assert_eq!(
        leader.hub().max_op_version(&room),
        Some(2),
        "the write still tagged from the live binding",
    );

    let mut follower = follower_by_ops(&mut leader, &room);
    assert_eq!(
        follower.hub().max_op_version(&room),
        Some(2),
        "the replica holds the high-water the swept leader still knows the app for",
    );
    assert_eq!(follower.hub().governing_app(&room), Some((APP.to_vec(), 2)));

    let old = hello(&mut follower, 2, APP, 1);
    assert!(
        is_update_required(&subscribe_reply(&mut follower, old, &room)),
        "so the range-check is live on the replica rather than inert",
    );
}

// --- the replicated record is durable, and travels the state-install seams ---

#[test]
#[cfg_attr(miri, ignore)] // reopens the durable store on the filesystem
fn a_swept_leaders_record_survives_a_restart_with_its_binding() {
    // The durable half of the same pair. A room bound, swept and then written holds its
    // high-water in the presence map's version space and nowhere else, so a restart —
    // which seeds the map from the stored record — comes back with the number and no app
    // at all, and the leader then replicates a version nothing can read, permanently. A
    // write re-asserts the binding it tagged from, so the stored record carries both.
    let dir = tempdir();
    let room = b"swept-room".to_vec();
    {
        let mut r = store_solo(dir.path());
        let w = hello(&mut r, 1, APP, 2);
        assert!(is_subscribed(&subscribe_reply(&mut r, w, &room)));
        r.sweep();
        write(&mut r, w, 1, &room, b"years");
        assert_eq!(r.hub().max_op_version(&room), Some(2));
    }
    let reopened = store_solo(dir.path());
    assert_eq!(
        reopened.hub().governing_app(&room),
        Some((APP.to_vec(), 2)),
        "the write's own binding was recorded beside the number it raised",
    );
    assert_eq!(reopened.hub().max_op_version(&room), Some(2));
}

/// A single-node registry backed by the durable store at `path`.
fn store_solo(path: &std::path::Path) -> Registry {
    let store = crdtsync_server::store::Store::open(path).unwrap();
    let mut r = Registry::with_store(cid(0xFF), store).unwrap();
    r.set_schema_registry(Arc::new(Mutex::new(schema_registry())));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

#[test]
#[cfg_attr(miri, ignore)] // reopens the durable store on the filesystem
fn a_state_transferred_replica_persists_the_whole_record() {
    // The state-transfer seam writes the record too, and a high-water without the
    // binding it was measured in is unreadable: the gate abstains on an unbound room,
    // so a replica reloading that pair refuses nobody. Both halves have to survive the
    // restart, not just the one the install itself writes.
    let dir = tempdir();
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    let b = NodeId::from_addr(B);
    {
        let mut follower = store_node(B, dir.path());
        let peer = peer_conn(&mut follower, &room);
        leader.hub_mut().compact(&room).expect("the room compacts");
        leader.take_replication();
        leader.catch_up_follower(&b);
        let mut sent = 0;
        for (target, frame) in leader.take_replication() {
            if target != b {
                continue;
            }
            assert!(
                matches!(frame, Message::ReplicateSnapshot { .. }),
                "a below-floor follower takes the state transfer: {frame:?}",
            );
            assert!(follower.deliver(peer, frame));
            sent += 1;
        }
        assert_eq!(sent, 1);
        assert_eq!(follower.hub().max_op_version(&room), Some(2));
        assert_eq!(follower.hub().governing_app(&room), Some((APP.to_vec(), 2)));
    }
    let reopened = store_node(B, dir.path());
    assert_eq!(
        reopened.hub().governing_app(&room),
        Some((APP.to_vec(), 2)),
        "the binding the install did not itself write survived",
    );
    assert_eq!(reopened.hub().max_op_version(&room), Some(2));
}

#[test]
#[cfg_attr(miri, ignore)] // reopens the durable store on the filesystem
fn a_restarted_replica_comes_back_holding_the_replicated_record() {
    let dir = tempdir();
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room, APP, 2);
    {
        let mut follower = store_node(B, dir.path());
        let peer = peer_conn(&mut follower, &room);
        let b = NodeId::from_addr(B);
        for (target, frame) in leader.take_replication() {
            if target == b {
                assert!(follower.deliver(peer, frame));
            }
        }
        assert_eq!(follower.hub().max_op_version(&room), Some(2));
    }
    let reopened = store_node(B, dir.path());
    assert_eq!(
        reopened.hub().max_op_version(&room),
        Some(2),
        "the replicated high-water was persisted beside the log",
    );
    assert_eq!(
        reopened.hub().governing_app(&room),
        Some((APP.to_vec(), 2)),
        "and so was the replicated binding",
    );
}

#[test]
fn a_cloned_room_carries_the_sources_high_water() {
    // A clone is the source's content, so a joiner to it must down-reach the same
    // worst case — the reason the source's creator and governing app already travel.
    let mut r = solo();
    let writer = hello(&mut r, 1, APP, 2);
    assert!(is_subscribed(&subscribe_reply(&mut r, writer, b"src")));
    write(&mut r, writer, 1, b"src", b"years");
    assert_eq!(r.hub().max_op_version(b"src"), Some(2));

    assert!(r.hub_mut().clone_room(b"src", b"dst").expect("it clones"));
    assert_eq!(
        r.hub().max_op_version(b"dst"),
        Some(2),
        "the clone inherits the worst case its state embodies",
    );
}

#[test]
fn a_clone_of_a_swept_source_carries_the_whole_record() {
    // A sweep prunes the hub's copy of a binding for a room it does not yet hold, and
    // the clone reads the hub. Without the write re-asserting what it tagged from, a
    // clone taken after that sweep came up with neither field — ungoverned, unversioned,
    // range-checking nobody, and with its zones unresolvable (C62's shape) — from a
    // source that is bound and populated.
    let mut r = solo();
    let w = hello(&mut r, 1, APP, 2);
    assert!(is_subscribed(&subscribe_reply(&mut r, w, b"src")));
    r.sweep();
    write(&mut r, w, 1, b"src", b"years");

    assert!(r.hub_mut().clone_room(b"src", b"dst").expect("it clones"));
    assert_eq!(r.hub().governing_app(b"dst"), Some((APP.to_vec(), 2)));
    assert_eq!(r.hub().max_op_version(b"dst"), Some(2));
    let old = hello(&mut r, 2, APP, 1);
    assert!(
        is_update_required(&subscribe_reply(&mut r, old, b"dst")),
        "so the clone refuses the joiner its source would",
    );
}

#[test]
#[cfg_attr(miri, ignore)] // reopens the durable store on the filesystem
fn a_restarted_leaders_dial_carries_the_whole_record() {
    // The dial is peer-triggered, not write-triggered, so it fires before any client
    // subscribe could re-bind the room. A leader restarted out of the swept state would
    // hand every late-joining follower a version with no app beside it, on the one path
    // a commit never passes through.
    let dir = tempdir();
    let room = room_led_by_a_with_b_next();
    {
        let mut r = store_node(A, dir.path());
        let w = hello(&mut r, 1, APP, 2);
        assert!(is_subscribed(&subscribe_reply(&mut r, w, &room)));
        r.sweep();
        write(&mut r, w, 1, &room, b"years");
    }
    let mut reopened = store_node(A, dir.path());
    reopened.take_replication();
    reopened.catch_up_follower(&NodeId::from_addr(B));
    let mut dialed = 0;
    for (target, frame) in reopened.take_replication() {
        if target != NodeId::from_addr(B) {
            continue;
        }
        dialed += 1;
        let (governing, high_water) = match &frame {
            Message::Replicate {
                governing,
                max_op_version,
                ..
            }
            | Message::ReplicateSnapshot {
                governing,
                max_op_version,
                ..
            } => (governing.clone(), *max_op_version),
            other => panic!("unexpected dial frame: {other:?}"),
        };
        assert_eq!(governing, Some((APP.to_vec(), 2)));
        assert_eq!(high_water, Some(2));
    }
    assert!(dialed > 0, "the leader dialed the follower something");
}

#[test]
fn a_clone_of_an_unbound_source_carries_no_high_water() {
    // The invariant, at the one seam that mints a room from another. `Hub::ingest` is
    // public and takes a version without binding anything, so a hub can hold a number
    // with no app to read it in even though the server's own paths no longer produce
    // one. A clone that copied the number without the binding would admit every joiner
    // — the handshake abstains on an unbound room however high the number is — and then
    // be bound at whatever version the first one speaks.
    let mut r = solo();
    r.hub_mut()
        .ingest(b"src", set(1, b"years"), Some(2))
        .expect("the hub ingests");
    assert_eq!(r.hub().max_op_version(b"src"), Some(2));
    assert_eq!(r.hub().governing_app(b"src"), None, "nothing bound it");

    assert!(r.hub_mut().clone_room(b"src", b"dst").expect("it clones"));
    assert_eq!(r.hub().governing_app(b"dst"), None, "nothing to bind it to");
    assert_eq!(
        r.hub().max_op_version(b"dst"),
        None,
        "so the clone takes no floor it could not have read",
    );
}

#[test]
fn an_imported_room_carries_no_high_water() {
    // A portable snapshot is a `Document` encoding and names no version, exactly as it
    // names no creator — so an import establishes none rather than inventing one.
    let mut r = solo();
    let writer = hello(&mut r, 1, APP, 2);
    assert!(is_subscribed(&subscribe_reply(&mut r, writer, b"src")));
    write(&mut r, writer, 1, b"src", b"years");
    let state = r.hub().export_room(b"src").expect("the room exports");

    assert!(r
        .hub_mut()
        .import_room(b"moved", &state)
        .expect("it imports"));
    assert_eq!(r.hub().max_op_version(b"moved"), None);
}

/// A cluster node at `self_addr` backed by the durable store at `path`.
fn store_node(self_addr: &str, path: &std::path::Path) -> Registry {
    let store = crdtsync_server::store::Store::open(path).unwrap();
    let mut r = Registry::with_store(cid(0xFF), store).unwrap();
    r.set_schema_registry(Arc::new(Mutex::new(schema_registry())));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for(self_addr));
    r.set_cluster_secret(CLUSTER_SECRET.to_vec());
    r
}

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tempdir() -> TempDir {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("crdtsync-replicated-opversion-{pid}-{n}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}
