//! A room's doc-ACL authority root reaches its replicas even when no op batch is
//! going that way (C55).
//!
//! C29 (#374) made the root replicated metadata, carried on `Replicate` and
//! `ReplicateSnapshot`. Two states it left open have a root standing on one node and
//! absent on another, with nothing scheduled to close the gap:
//!
//! 1. **A root established by a write that broadcast nothing.** `ensure_creator` fires
//!    on any `Ok` from `Hub::ingest`, a batch the room's dedup swallowed whole
//!    included, while replication is enqueued only for a *non-empty* broadcast. A room
//!    whose establishing commit was anonymous (root `None`, replicated as such) and
//!    whose next authenticated write is a pure resend of ops the hub already holds
//!    gains its root on the leader with no frame to carry it.
//! 2. **A root whose best-effort persist failed on a replica.** Set-once retries
//!    nothing, so the replica reloads creatorless — and unlike a leader it serves no
//!    client write to establish one afresh, so a quiescent room's root never returns:
//!    `catch_up_room_frame` declines to send an empty delta, which is the only frame a
//!    caught-up follower would otherwise get.
//!
//! Both leave the replica serving every doc-ACL deny in the room as inert
//! (`reads_whole_document` short-circuits `true` on a rootless room), which is the
//! hole C29 exists to close. The carrier is [`Message::ReplicateMeta`]: the root, an
//! epoch to fence it, no branch and no sequence. It creates no room — an empty
//! `Replicate` would, leaving a follower holding an empty replica at the head that
//! `holds_room` reports as servable.
//!
//! What makes route 1 observable is that a doc-ACL tuple's authority is resolved at
//! *read* time against the room's root: the client here authors its deny naming alice
//! as grantor while its connection is anonymous (an offline-authored batch flushed
//! over a connection the deployment admitted anonymously), the server checks no
//! grantor at write time, and the tuple therefore decides nothing until alice roots
//! the room. That resend is what roots it.
//!
//! Two in-process registries over one static cluster, no socket and a fixed clock,
//! as in `replicated_creator.rs`.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{AclEffect, ClientId, Document, Message, Op, Scalar};
use crdtsync_server::acl::{actor_key, Acl};
use crdtsync_server::membership::Membership;
use crdtsync_server::placement::NodeId;
use crdtsync_server::store::Store;
use crdtsync_server::{ConnId, Identity, ManualClock, Registry, SchemaRegistry, StaticTokens};

const CH: Channel = Channel(0);
const N: usize = 3;
const A: &str = "10.0.0.1:9000";
const B: &str = "10.0.0.2:9000";
const APP: &[u8] = b"collab";

/// The key alice denies bob, and one she leaves readable.
const SECRET: &[u8] = b"secret";
const OPEN: &[u8] = b"open";

/// Read and write to `editor` and to any anonymous actor — the deployment shape
/// route 1 needs, where a credential-less connection may still author. Root read
/// arrives from the schema tier, so the doc-ACL deny is the only thing that can
/// narrow it, and the narrowing needs the room's root.
const SCHEMA: &str = r#"{ "schema": "collab", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "auth": {
        "roles": ["editor"],
        "grants": [
            { "allow": "read",  "to": "authenticated", "on": "/" },
            { "allow": "read",  "to": "anonymous",     "on": "/" },
            { "allow": "write", "to": "editor",        "on": "/" },
            { "allow": "write", "to": "anonymous",     "on": "/" }
        ]
    } }"#;

const CLUSTER_SECRET: &[u8] = b"peer-plane-cluster-secret-for-tests";

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

fn tokens() -> StaticTokens {
    let mut t = StaticTokens::new();
    t.insert_identity(
        b"t-alice".to_vec(),
        Identity::with_claims(b"alice".to_vec(), vec!["editor".to_string()], Vec::new()),
    );
    t.insert_identity(
        b"t-bob".to_vec(),
        Identity::with_claims(b"bob".to_vec(), Vec::new(), Vec::new()),
    );
    t
}

fn configure(r: &mut Registry, self_addr: &str) {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, SCHEMA.as_bytes(), b"").unwrap();
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens()));
    r.set_authorizer(Box::new(Acl::new()));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r.set_membership(membership_for(self_addr));
    r.set_cluster_secret(CLUSTER_SECRET.to_vec());
}

/// A cluster node whose self is `self_addr`, holding `SCHEMA` and an abstaining
/// deployment ACL — so the schema and doc-ACL tiers alone decide every read.
fn node(self_addr: &str) -> Registry {
    let mut r = Registry::new(cid(0xFF));
    configure(&mut r, self_addr);
    r
}

/// The same node, durable: its hub reads and writes `dir`.
fn stored_node(self_addr: &str, dir: &Path) -> Registry {
    let mut r = Registry::with_store(cid(0xFF), Store::open(dir).unwrap()).unwrap();
    configure(&mut r, self_addr);
    r
}

fn hello(r: &mut Registry, id: ConnId, client: u8) {
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: APP.to_vec(),
            schema_version: 1,
            codecs: Vec::new(),
        }
    ));
}

/// Hello + Auth a connection as `credential`, declaring `{APP, 1}`.
fn hello_auth(r: &mut Registry, client: u8, credential: &str) -> ConnId {
    let id = r.connect();
    hello(r, id, client);
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: credential.as_bytes().to_vec(),
        }
    ));
    r.take_outbox(id);
    id
}

/// A connection the deployment admitted with no credential, under a minted
/// `anon:`-prefixed actor — the anonymous-mode path, whose actor
/// `acl::is_authenticated` refuses as a room's root.
fn hello_anonymous(r: &mut Registry, client: u8) -> ConnId {
    let id = r.connect_authenticated(Identity::new(b"anon:ghost".to_vec()));
    hello(r, id, client);
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

fn submit(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    let open = r.deliver(
        id,
        Message::Ops {
            channel: CH,
            ops: ops.clone(),
        },
    );
    let out = r.take_outbox(id);
    assert!(open, "the write kept the connection: {out:?}");
    for m in &out {
        if let Message::Error { .. } | Message::OpsRejected { .. } = m {
            panic!("the write was refused: {m:?}");
        }
    }
}

/// A room `A` leads and `B` replicates second in HRW order.
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
    panic!("no room led by A with B its next replica");
}

/// A second room with the same placement as `room`, so one node can be handed frames
/// for two rooms it does not hold.
fn room_led_by_a_with_b_next_after(room: &[u8]) -> Vec<u8> {
    let m = membership_for(A);
    let a = NodeId::from_addr(A);
    let b = NodeId::from_addr(B);
    for i in 0..1_000_000 {
        let candidate = format!("room-{i}").into_bytes();
        if candidate == room {
            continue;
        }
        let replicas = m.replicas_for(&candidate);
        if replicas.first() == Some(&a) && replicas.get(1) == Some(&b) {
            return candidate;
        }
    }
    panic!("no second room led by A with B its next replica");
}

/// A connection admitted to `r`'s peer plane as one of `room`'s other replicas.
fn peer_conn(r: &mut Registry, room: &[u8]) -> ConnId {
    let node = r
        .membership()
        .and_then(|m| m.replicas_for(room).into_iter().find(|n| !m.is_self(n)))
        .expect("the room has another replica");
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::PeerAuth {
            node: node.as_bytes().to_vec(),
            secret: CLUSTER_SECRET.to_vec(),
        },
    ));
    id
}

/// Every frame the leader has queued for `B`.
fn frames_for_b(leader: &mut Registry) -> Vec<Message> {
    let b = NodeId::from_addr(B);
    leader
        .take_replication()
        .into_iter()
        .filter(|(target, _)| *target == b)
        .map(|(_, frame)| frame)
        .collect()
}

/// Hand `frames` to `follower` over its peer link, asserting each is applied.
fn apply_all(follower: &mut Registry, peer: ConnId, frames: Vec<Message>) {
    for frame in frames {
        assert!(follower.deliver(peer, frame), "the follower applies it");
    }
}

/// Carry the follower's acknowledgements back to the leader, so the leader's
/// remembered watermark for `B` is where the follower actually is — what a dial then
/// computes its catch-up from.
fn ack_back(leader: &mut Registry, follower: &mut Registry, peer: ConnId) {
    for msg in follower.take_outbox(peer) {
        if let Message::ReplicaAck { room, through_seq } = msg {
            leader.record_replica_ack(NodeId::from_addr(B), &room, through_seq);
        }
    }
}

/// The room's establishing commit, authored by one client: two keys and a
/// `Deny(Read)` for bob at `/secret` naming alice as its grantor.
fn establishing_batch(doc: &mut Document) -> Vec<Op> {
    doc.transact(|tx| {
        tx.register(OPEN, Scalar::Int(1));
        tx.register(SECRET, Scalar::Int(2));
        tx.acl().grant(
            AclSubject::Actor(actor_key(b"bob")),
            AclGrant::Capability(Capability::Read),
            AclEffect::Deny,
            encode_path(&[SECRET]),
            actor_key(b"alice"),
        );
    })
}

/// Subscribe bob on `r` and fold whatever the catch-up served him — an op delta or a
/// projected snapshot — into one document. Each call opens a fresh connection, so the
/// same node can be read before and after a root lands on it.
fn bob_reads(r: &mut Registry, room: &[u8]) -> Document {
    let bob = hello_auth(r, 7, "t-bob");
    assert!(r.deliver(bob, sub(room)));
    let out = r.take_outbox(bob);
    assert!(
        !out.iter().any(|m| matches!(m, Message::Error { .. })),
        "the schema's authenticated read grant admits bob: {out:?}",
    );
    let mut view = Document::new(cid(7));
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
    assert!(served, "bob was served a catch-up");
    view
}

/// Whether the view is the whole document — the reading a rootless replica serves,
/// since it has no authority to resolve the deny against.
fn whole_document(view: &Document) -> bool {
    view.get(SECRET).is_some() && view.get(OPEN).is_some()
}

/// Whether the view is the narrowed document bob is entitled to.
fn narrowed(view: &Document) -> bool {
    view.get(SECRET).is_none() && view.get(OPEN).is_some()
}

// --- route 1: a root established by a write the dedup swallowed whole ---

/// The scenario both route-1 tests build: a room established anonymously on the
/// leader and replicated to a follower, both rootless, with the establishing batch
/// held back for the resend that roots it.
struct Rootless {
    room: Vec<u8>,
    leader: Registry,
    follower: Registry,
    peer: ConnId,
    batch: Vec<Op>,
}

fn rootless_room() -> Rootless {
    let room = room_led_by_a_with_b_next();
    let mut leader = node(A);
    let mut follower = node(B);
    let peer = peer_conn(&mut follower, &room);

    let ghost = hello_anonymous(&mut leader, 1);
    assert!(leader.deliver(ghost, sub(&room)));
    leader.take_outbox(ghost);

    let mut doc = Document::new(cid(1));
    let batch = establishing_batch(&mut doc);
    submit(&mut leader, ghost, batch.clone());
    leader.take_outbox(ghost);

    assert_eq!(
        leader.hub().room_creator(&room),
        None,
        "an anonymous actor roots nothing",
    );
    let frames = frames_for_b(&mut leader);
    assert!(!frames.is_empty(), "the commit replicated");
    apply_all(&mut follower, peer, frames);
    ack_back(&mut leader, &mut follower, peer);
    assert_eq!(
        follower.hub().export_room(&room),
        leader.hub().export_room(&room),
        "the follower converged with the leader",
    );
    assert_eq!(follower.hub().room_creator(&room), None);

    Rootless {
        room,
        leader,
        follower,
        peer,
        batch,
    }
}

#[test]
fn a_root_established_by_a_deduped_resend_reaches_the_replica() {
    let Rootless {
        room,
        mut leader,
        mut follower,
        peer,
        batch,
    } = rootless_room();

    // The same client, now authenticated as alice, flushes the ops it already sent.
    // Every one dedups away, so the write broadcasts nothing — and it is what roots
    // the room.
    let alice = hello_auth(&mut leader, 1, "t-alice");
    assert!(leader.deliver(alice, sub(&room)));
    leader.take_outbox(alice);
    let before = leader.hub().seq(&room);
    submit(&mut leader, alice, batch);
    assert_eq!(
        leader.hub().seq(&room),
        before,
        "the resend landed no op — the room deduped it whole",
    );
    assert_eq!(
        leader.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
        "the resend established the room's root on the leader",
    );

    let frames = frames_for_b(&mut leader);
    assert_eq!(
        frames.len(),
        1,
        "the root has a frame of its own to ride out on: {frames:?}",
    );
    assert!(
        matches!(
            &frames[0],
            Message::ReplicateMeta { room: r, creator: Some(c), .. }
                if r == &room && c == b"alice"
        ),
        "a metadata-only frame carrying the root: {frames:?}",
    );
    apply_all(&mut follower, peer, frames);

    assert_eq!(
        follower.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
        "the replica holds the root without waiting for a fresh commit",
    );
}

#[test]
fn a_replica_rooted_by_that_frame_narrows_the_read_it_was_over_serving() {
    let Rootless {
        room,
        mut leader,
        mut follower,
        peer,
        batch,
    } = rootless_room();

    // Rootless, both nodes hand bob the whole document: with no authority root the
    // deny that rode the log decides nothing.
    assert!(whole_document(&bob_reads(&mut leader, &room)));
    assert!(
        whole_document(&bob_reads(&mut follower, &room)),
        "a rootless replica serves the deny as inert",
    );

    let alice = hello_auth(&mut leader, 1, "t-alice");
    assert!(leader.deliver(alice, sub(&room)));
    leader.take_outbox(alice);
    submit(&mut leader, alice, batch);
    assert!(
        narrowed(&bob_reads(&mut leader, &room)),
        "the leader now resolves the deny under alice's authority",
    );

    apply_all(&mut follower, peer, frames_for_b(&mut leader));
    assert!(
        narrowed(&bob_reads(&mut follower, &room)),
        "and so does the replica the root reached",
    );
}

#[test]
fn a_write_that_carries_its_own_root_sends_no_second_frame() {
    // The ordinary path is untouched: a first write both roots the room and produces
    // the ops whose `Replicate` carries the root, so nothing extra is queued.
    let room = room_led_by_a_with_b_next();
    let mut leader = node(A);
    let alice = hello_auth(&mut leader, 1, "t-alice");
    assert!(leader.deliver(alice, sub(&room)));
    leader.take_outbox(alice);
    submit(
        &mut leader,
        alice,
        establishing_batch(&mut Document::new(cid(1))),
    );

    let frames = frames_for_b(&mut leader);
    assert_eq!(frames.len(), 1, "one frame: {frames:?}");
    assert!(
        matches!(
            &frames[0],
            Message::Replicate { creator: Some(c), .. } if c == b"alice"
        ),
        "the commit's own frame carries the root: {frames:?}",
    );
}

#[test]
fn a_deduped_resend_into_an_already_rooted_room_sends_nothing() {
    // Only the write that *establishes* the root has one to announce. A later
    // deduped resend re-roots nothing, so it queues no frame.
    let room = room_led_by_a_with_b_next();
    let mut leader = node(A);
    let alice = hello_auth(&mut leader, 1, "t-alice");
    assert!(leader.deliver(alice, sub(&room)));
    leader.take_outbox(alice);
    let batch = establishing_batch(&mut Document::new(cid(1)));
    submit(&mut leader, alice, batch.clone());
    leader.take_replication();

    submit(&mut leader, alice, batch);
    assert!(
        frames_for_b(&mut leader).is_empty(),
        "a resend into a rooted room announces nothing",
    );
}

#[test]
fn a_single_node_deployment_queues_no_frame() {
    // The root frame reaches whoever the ops frames would: a node with no membership
    // leads nothing and replicates to nobody, so establishing a root on one costs it
    // no queue traffic.
    let room = room_led_by_a_with_b_next();
    let mut solo = Registry::new(cid(0xFF));
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, SCHEMA.as_bytes(), b"").unwrap();
    solo.set_schema_registry(Arc::new(Mutex::new(sr)));
    solo.set_verifier(Box::new(tokens()));
    solo.set_authorizer(Box::new(Acl::new()));
    solo.set_clock(Arc::new(ManualClock::new(0)));

    let ghost = hello_anonymous(&mut solo, 1);
    assert!(solo.deliver(ghost, sub(&room)));
    let mut doc = Document::new(cid(1));
    let batch = establishing_batch(&mut doc);
    submit(&mut solo, ghost, batch.clone());

    let alice = hello_auth(&mut solo, 1, "t-alice");
    assert!(solo.deliver(alice, sub(&room)));
    submit(&mut solo, alice, batch);
    assert_eq!(
        solo.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
        "the resend still roots the room",
    );
    assert!(
        solo.take_replication().is_empty(),
        "and replicates to nobody",
    );
}

// --- route 2: a replica whose root persist failed, in a quiescent room ---

/// `write_meta` is best-effort — a failure leaves no record on disk — so removing
/// every metadata file reproduces exactly the state a failed persist leaves behind.
fn drop_persisted_meta(dir: &Path) {
    let mut removed = 0;
    for entry in fs::read_dir(dir).unwrap() {
        let path: PathBuf = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) == Some("meta") {
            fs::remove_file(&path).unwrap();
            removed += 1;
        }
    }
    assert!(removed > 0, "there was a metadata record to lose");
}

fn tempdir(tag: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("crdtsync-c55-{tag}-{pid}-{n}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// A leader holding `room` rooted at alice, with `OPEN`, `SECRET` and the deny.
fn seeded_leader(room: &[u8]) -> Registry {
    let mut leader = node(A);
    let alice = hello_auth(&mut leader, 1, "t-alice");
    assert!(leader.deliver(alice, sub(room)));
    leader.take_outbox(alice);
    submit(
        &mut leader,
        alice,
        establishing_batch(&mut Document::new(cid(1))),
    );
    leader.take_outbox(alice);
    assert_eq!(
        leader.hub().room_creator(room).as_deref(),
        Some(b"alice".as_slice()),
    );
    leader
}

#[test]
fn a_quiescent_replica_that_lost_its_root_is_re_rooted_by_the_dial() {
    let room = room_led_by_a_with_b_next();
    let dir = tempdir("quiescent");
    let mut leader = seeded_leader(&room);

    // Converge a durable follower, then lose its metadata record — the best-effort
    // persist that failed — and reload it. It comes up holding every op and no root.
    {
        let mut follower = stored_node(B, &dir);
        let peer = peer_conn(&mut follower, &room);
        apply_all(&mut follower, peer, frames_for_b(&mut leader));
        ack_back(&mut leader, &mut follower, peer);
        assert_eq!(
            follower.hub().room_creator(&room).as_deref(),
            Some(b"alice".as_slice()),
        );
    }
    drop_persisted_meta(&dir);
    let mut follower = stored_node(B, &dir);
    let peer = peer_conn(&mut follower, &room);
    assert_eq!(
        follower.hub().room_creator(&room),
        None,
        "the lost record leaves the reloaded replica rootless",
    );
    assert!(
        whole_document(&bob_reads(&mut follower, &room)),
        "and over-serving the read the root would narrow",
    );
    assert_eq!(
        follower.hub().seq(&room),
        leader.hub().seq(&room),
        "the room is quiescent and the replica is at the head — there is no delta",
    );

    // The leader dials the reconnecting follower. It has no ops to send.
    leader.catch_up_follower(&NodeId::from_addr(B));
    let frames = frames_for_b(&mut leader);
    assert_eq!(frames.len(), 1, "one catch-up frame: {frames:?}");
    assert!(
        matches!(&frames[0], Message::ReplicateMeta { creator: Some(c), .. } if c == b"alice"),
        "the root travels on its own where no delta can carry it: {frames:?}",
    );
    apply_all(&mut follower, peer, frames);

    assert_eq!(
        follower.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
    );
    assert!(
        narrowed(&bob_reads(&mut follower, &room)),
        "the re-rooted replica narrows bob's read again",
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_re_rooted_replica_persists_the_root_it_was_handed() {
    // The repair has to survive the *next* restart too, or a node whose disk is
    // healthy again would keep losing it.
    let room = room_led_by_a_with_b_next();
    let dir = tempdir("repersist");
    let mut leader = seeded_leader(&room);
    {
        let mut follower = stored_node(B, &dir);
        let peer = peer_conn(&mut follower, &room);
        apply_all(&mut follower, peer, frames_for_b(&mut leader));
        ack_back(&mut leader, &mut follower, peer);
    }
    drop_persisted_meta(&dir);
    {
        let mut follower = stored_node(B, &dir);
        let peer = peer_conn(&mut follower, &room);
        leader.catch_up_follower(&NodeId::from_addr(B));
        apply_all(&mut follower, peer, frames_for_b(&mut leader));
        assert_eq!(
            follower.hub().room_creator(&room).as_deref(),
            Some(b"alice".as_slice()),
        );
    }
    let reloaded = stored_node(B, &dir);
    assert_eq!(
        reloaded.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
        "the adopted root was written through, so the reload keeps it",
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_caught_up_follower_of_a_rootless_room_is_dialed_nothing() {
    // The frame exists to carry a root. A room that has none is still sent no empty
    // delta, so a caught-up follower of a creatorless room gets nothing at all.
    let Rootless {
        room: _room,
        mut leader,
        ..
    } = rootless_room();
    leader.take_replication();
    leader.catch_up_follower(&NodeId::from_addr(B));
    assert!(frames_for_b(&mut leader).is_empty(), "no root, no frame",);
}

// --- what the frame may and may not do on arrival ---

#[test]
fn the_frame_never_creates_the_room_it_names() {
    // The reason this is not an empty `Replicate`: `ingest_records` creates the room
    // unconditionally, so a follower that does not hold it would come up holding an
    // *empty* one at the head — which `holds_room` then reports as servable.
    let room = room_led_by_a_with_b_next();
    let mut follower = node(B);
    let peer = peer_conn(&mut follower, &room);
    assert!(!follower.hub().holds_room(&room));
    assert!(follower.deliver(
        peer,
        Message::ReplicateMeta {
            room: room.clone(),
            epoch: 1,
            creator: Some(b"alice".to_vec()),
        },
    ));
    assert!(
        !follower.hub().holds_room(&room),
        "a node missing the room stays missing it",
    );
    assert_eq!(follower.hub().room_creator(&room), None);
}

#[test]
fn an_empty_replicate_would_have_created_the_room_instead() {
    // The measurement behind the design, not an argument about it: the frame the
    // root could have ridden — a `Replicate` with an empty batch — creates the room
    // it names, because `ingest_records` does so unconditionally. The follower comes
    // up holding an empty replica at the head, which `holds_room` then reports as
    // servable, and a client routed there is served an empty document for a room
    // that has content. That is why the root gets a frame of its own.
    let room = room_led_by_a_with_b_next();
    let mut follower = node(B);
    let peer = peer_conn(&mut follower, &room);
    assert!(!follower.hub().holds_room(&room));
    assert!(follower.deliver(
        peer,
        Message::Replicate {
            room: room.clone(),
            branch: b"main".to_vec(),
            ops: Vec::new(),
            base_seq: 0,
            epoch: 1,
            creator: Some(b"alice".to_vec()),
        },
    ));
    assert!(
        follower.hub().holds_room(&room),
        "an empty delta creates the room — the reason it cannot be the root's carrier",
    );

    // The metadata-only frame, on the same node in the same state, does not.
    let other = room_led_by_a_with_b_next_after(&room);
    assert!(follower.deliver(
        peer,
        Message::ReplicateMeta {
            room: other.clone(),
            epoch: 1,
            creator: Some(b"alice".to_vec()),
        },
    ));
    assert!(!follower.hub().holds_room(&other));
}

#[test]
fn the_frame_is_not_acknowledged() {
    // It names no sequence and advances no stream, so there is no watermark to
    // report — an ack would advance one the frame did not move.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room);
    let mut follower = node(B);
    let peer = peer_conn(&mut follower, &room);
    apply_all(&mut follower, peer, frames_for_b(&mut leader));
    follower.take_outbox(peer);

    assert!(follower.deliver(
        peer,
        Message::ReplicateMeta {
            room: room.clone(),
            epoch: 1,
            creator: Some(b"alice".to_vec()),
        },
    ));
    assert!(
        follower.take_outbox(peer).is_empty(),
        "a metadata-only frame is answered with nothing",
    );
}

#[test]
fn a_frame_naming_another_root_does_not_displace_the_standing_one() {
    // Set-once, through the same `ensure_creator` every other arrival seam uses: a
    // frame is an assertion, and the receiver composes it against what it holds.
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room);
    let mut follower = node(B);
    let peer = peer_conn(&mut follower, &room);
    apply_all(&mut follower, peer, frames_for_b(&mut leader));

    assert!(follower.deliver(
        peer,
        Message::ReplicateMeta {
            room: room.clone(),
            epoch: 1,
            creator: Some(b"mallory".to_vec()),
        },
    ));
    assert_eq!(
        follower.hub().room_creator(&room).as_deref(),
        Some(b"alice".as_slice()),
        "the standing root stands",
    );
}

#[test]
fn a_frame_naming_an_anonymous_root_roots_nothing() {
    // The same rule every other seam applies: an anonymous id is minted per
    // connection, so set-once would wedge the room's authority on a principal that
    // can never re-present.
    let Rootless {
        room,
        mut follower,
        peer,
        ..
    } = rootless_room();
    assert!(follower.deliver(
        peer,
        Message::ReplicateMeta {
            room: room.clone(),
            epoch: 1,
            creator: Some(b"anon:ghost".to_vec()),
        },
    ));
    assert_eq!(follower.hub().room_creator(&room), None);
}

#[test]
fn a_stale_epoch_frame_is_fenced_rather_than_applied() {
    // Fenced exactly as an ops frame is: a demoted-then-recovered leader cannot
    // re-root a replica, and the connection stays open.
    let Rootless {
        room,
        mut follower,
        peer,
        ..
    } = rootless_room();
    // Observe a high epoch, as a live leader's frame would leave it.
    assert!(follower.deliver(
        peer,
        Message::ReplicateMeta {
            room: room.clone(),
            epoch: 9,
            creator: None,
        },
    ));
    assert!(
        follower.deliver(
            peer,
            Message::ReplicateMeta {
                room: room.clone(),
                epoch: 1,
                creator: Some(b"mallory".to_vec()),
            },
        ),
        "a fenced frame keeps the connection",
    );
    assert_eq!(
        follower.hub().room_creator(&room),
        None,
        "and roots nothing",
    );
}

#[test]
fn a_frame_from_a_node_that_does_not_replicate_the_room_drops_the_link() {
    let room = room_led_by_a_with_b_next();
    let mut follower = node(B);
    // A member of the cluster, but not one of this room's replicas.
    let outsider = follower
        .membership()
        .map(|m| {
            let replicas = m.replicas_for(&room);
            (1..=5)
                .map(|i| NodeId::from_addr(&format!("10.0.0.{i}:9000")))
                .find(|n| !replicas.contains(n) && !m.is_self(n))
                .expect("a member outside the replica set")
        })
        .expect("membership");
    let id = follower.connect();
    assert!(follower.deliver(
        id,
        Message::PeerAuth {
            node: outsider.as_bytes().to_vec(),
            secret: CLUSTER_SECRET.to_vec(),
        },
    ));
    assert!(
        !follower.deliver(
            id,
            Message::ReplicateMeta {
                room: room.clone(),
                epoch: 1,
                creator: Some(b"mallory".to_vec()),
            },
        ),
        "a stray frame drops the connection",
    );
}

#[test]
fn a_client_that_sends_one_commits_a_protocol_violation() {
    let room = room_led_by_a_with_b_next();
    let mut leader = seeded_leader(&room);
    let bob = hello_auth(&mut leader, 7, "t-bob");
    assert!(
        !leader.deliver(
            bob,
            Message::ReplicateMeta {
                room,
                epoch: 1,
                creator: Some(b"bob".to_vec()),
            },
        ),
        "the node-to-node frame is unreachable from the client plane",
    );
}
