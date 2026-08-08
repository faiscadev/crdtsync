//! Every seam that redacts a `(room, branch)` read resolves against that branch's own
//! tree (C60).
//!
//! C32 settled the rule at the catch-up *snapshot* gate: a redaction decision is made
//! against the state being served, and on a branch that state is not `main`. Three seams
//! kept resolving through the live `main` index while acting on a branch:
//!
//! - the **live fan-out**, which takes the branch and then builds its index from
//!   `main`'s tree, so a reader whose catch-up C32 narrows is still delivered the next
//!   live write into the subtree it is denied — no reconnect, no restore;
//! - the **subscribe admission gate**, which decides whether a reader holds read on any
//!   subtree at all before the branch's tree is known;
//! - the **`Catchup::Ops` per-op filter**, which replays a branch tail past the fork
//!   point through that same `main` index.
//!
//! Two shapes leak through each. An **element**-scoped tuple whose target has left
//! `main` resolves to no path there, and an unresolvable scope is inert — an inert deny
//! is no deny, an inert allow is no grant. And an op whose container target `main`
//! cannot resolve falls back to the **root** (`op_read_path`), so a root-readable but
//! subtree-denied reader carries a write into the very subtree it is denied — which
//! reaches a **path**-scoped deny too.
//!
//! The assertions read the **ops actually put on the wire**, by their target element,
//! rather than folding them into the reader's replica: a redacted reader does not hold
//! the denied container, so a leaked op would buffer there unapplied and a replica-level
//! check would call the leak a pass.
//!
//! Everything runs in-process through the [`Registry`] (no socket, no fs), so the suite
//! runs under Miri.

use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{
    AclEffect, ClientId, Document, Element, ElementId, Message, Op, Scalar, Schema,
};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::index::ElementPaths;
use crdtsync_server::{Catchup, ConnId, Hub, ManualClock, Registry, SchemaRegistry, StaticTokens};

const ROOM: &[u8] = b"room-c60";
const CH: Channel = Channel(0);
const V1: &[u8] = b"v1";
const RESTORED: &[u8] = b"restored";
const LIVE_FORK: &[u8] = b"live-fork";
const APP: &[u8] = b"p";

/// Room read is granted by the schema tier to every authenticated actor, so a reader
/// with no doc-ACL root grant passes the subscribe gate and reads the root — which is
/// what makes the root fallback a leak rather than a no-op.
const OPEN_READ: &str = r#"{
    "schema": "p", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "a": "Sect", "b": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "auth": { "grants": [ { "allow": "read", "to": "authenticated", "on": "/" } ] }
}"#;

/// The same shape with no `auth` grants at all, so a reader's only way in is a doc-ACL
/// grant of its own — the schema tier abstains, and so does the deployment. That is what
/// puts the subscribe admission gate (`has_any_read_grant`) on the critical path.
const NO_GRANTS: &str = r#"{
    "schema": "p", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "a": "Sect", "b": "Sect" } },
        "Sect": { "kind": "map" }
    }
}"#;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn tokens(rows: &[(&str, &str)]) -> StaticTokens {
    let mut t = StaticTokens::new();
    for (credential, actor) in rows {
        t.insert(credential.as_bytes().to_vec(), actor.as_bytes().to_vec());
    }
    t
}

/// Hello (enforcing `{app, version}`) + Auth as `credential`, without subscribing.
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

/// Subscribe `id` to `(ROOM, branch)` on `CH` from `last_seen_seq`, returning the
/// catch-up reply. An empty `branch` follows the room's active HEAD.
fn subscribe_from(r: &mut Registry, id: ConnId, branch: &[u8], last_seen_seq: u64) -> Message {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            branch: branch.to_vec(),
            zone: Vec::new(),
            last_seen_seq,
        },
    ));
    r.take_outbox(id)
        .into_iter()
        .next()
        .expect("a catch-up reply")
}

fn subscribe(r: &mut Registry, id: ConnId, branch: &[u8]) -> Message {
    subscribe_from(r, id, branch, 0)
}

fn submit(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(id, Message::Ops { channel: CH, ops }));
    r.take_outbox(id);
}

/// The whole-replica state a catch-up carried.
fn snapshot_state(m: Message) -> Vec<u8> {
    match m {
        Message::Snapshot { state, .. } => state,
        other => panic!("expected a Snapshot catch-up, got {other:?}"),
    }
}

/// The catch-up snapshot, decoded.
fn snapshot_doc(m: Message) -> Document {
    Document::decode_state(&snapshot_state(m)).expect("the served state decodes")
}

/// The ops in `msg`, or none when it carries no batch.
fn ops_of(msg: &Message) -> &[Op] {
    match msg {
        Message::Ops { ops, .. } => ops,
        _ => &[],
    }
}

/// The target element of every op the connection has been sent — what the seam under
/// test either put on the wire or withheld.
fn delivered_targets(r: &mut Registry, id: ConnId) -> Vec<ElementId> {
    r.take_outbox(id)
        .iter()
        .flat_map(|msg| ops_of(msg).iter().map(|op| op.target))
        .collect()
}

/// The `Int` behind `outer.inner`, or `None` when either level is absent.
fn nested(d: &Document, outer: &[u8], inner: &[u8]) -> Option<i64> {
    let Some(Element::Map(m)) = d.get(outer) else {
        return None;
    };
    let child = m.borrow().get(inner);
    match child {
        Some(Element::Register(reg)) => match reg.borrow().read() {
            Scalar::Int(n) => Some(*n),
            _ => panic!("expected an Int register"),
        },
        None => None,
        _ => panic!("expected a register or nothing"),
    }
}

/// The room's materialized `main` replica, decoded.
fn room_doc(r: &Registry) -> Document {
    Document::decode_state(&r.hub().export_room(ROOM).expect("the room exists"))
        .expect("the room's state decodes")
}

/// The container id `key` holds in `doc`.
fn slot_id(doc: &Document, key: &[u8]) -> ElementId {
    match doc.get(key) {
        Some(Element::Map(m)) => m.borrow().id(),
        _ => panic!("slot {key:?} is not a map"),
    }
}

/// Which shape of doc-ACL tuple the test installs on `/b`.
enum Deny {
    /// A fixed-path tuple on `/b` — resolvable in any tree, since it names no element.
    /// It leaks only through the op's *target*: a container `main` cannot resolve reads
    /// at the root, and the root is readable.
    Path,
    /// A tuple on the container `/b` holds, so it resolves only through a tree that
    /// still holds that element. It leaks through the target and through the tuple.
    Element,
}

/// A registry on `schema` with alice permitted everything by the deployment and every
/// other actor abstained on — so their verdicts are the schema and doc-ACL tiers' alone.
/// Returns the registry, alice's conn, and alice's author document.
fn base_room(schema: &str) -> (Registry, ConnId, Document) {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, schema.as_bytes(), b"").unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens(&[
        ("t-alice", "alice"),
        ("t-editor", "alice"),
        ("t-bob", "bob"),
    ])));
    r.set_authorizer(Box::new(Acl::new().allow(
        Subject::Actor(b"alice".to_vec()),
        None,
        ResourceMatch::Room(ROOM.to_vec()),
    )));
    r.set_clock(Arc::new(ManualClock::new(0)));

    let alice = auth(&mut r, 1, "t-alice");
    subscribe(&mut r, alice, b"");
    let mut alice_doc = Document::new(cid(1));
    alice_doc.set_schema(Schema::parse(schema).expect("the schema parses"));
    // `/a` seeds the room and gives every reader a readable subtree, so a narrowed serve
    // is distinguishable from an empty one.
    let ops = alice_doc.transact(|tx| {
        tx.map(b"a").register(b"aseed", Scalar::Int(0));
    });
    submit(&mut r, alice, ops);
    (r, alice, alice_doc)
}

/// Seed `/b` and install bob's read-deny on it, in the shape `deny` names.
fn seed_denied_slot(r: &mut Registry, alice: ConnId, doc: &mut Document, deny: Deny) {
    let ops = doc.transact(|tx| {
        tx.map(b"b").register(b"bseed", Scalar::Int(0));
    });
    submit(r, alice, ops);
    let target = slot_id(doc, b"b");
    let ops = match deny {
        Deny::Path => doc.transact(|tx| {
            tx.acl().grant(
                AclSubject::Actor(actor_key(b"bob")),
                AclGrant::Capability(Capability::Read),
                AclEffect::Deny,
                encode_path(&[b"b"]),
                actor_key(b"alice"),
            );
        }),
        Deny::Element => doc.transact(|tx| {
            tx.acl().grant_element(
                AclSubject::Actor(actor_key(b"bob")),
                AclGrant::Capability(Capability::Read),
                AclEffect::Deny,
                target,
                actor_key(b"alice"),
            );
        }),
    };
    submit(r, alice, ops);
}

/// `/a` and a `/b` bob is denied, captured as `v1`, `/b` then dropped from the live
/// `main` and the version restored as the active-HEAD branch `restored`. The denied
/// container now stands only on the branch; `main` resolves it to nothing.
fn restored_room(deny: Deny) -> Registry {
    let (mut r, alice, mut alice_doc) = base_room(OPEN_READ);
    seed_denied_slot(&mut r, alice, &mut alice_doc, deny);
    assert!(r.hub_mut().create_version(ROOM, V1).unwrap());
    let ops = alice_doc.transact(|tx| tx.delete(b"b"));
    submit(&mut r, alice, ops);
    assert!(
        room_doc(&r).get(b"b").is_none(),
        "the live room still holds the denied subtree",
    );
    assert!(r.restore_as_branch(ROOM, V1, RESTORED).unwrap());
    r
}

/// An editor connection bound to the restored branch, authoring from the branch state it
/// was served — so a write into `/b` targets the container the branch actually holds
/// (and the deny actually names), not a fresh one that would displace it.
fn branch_editor(r: &mut Registry, schema: &str) -> (ConnId, Document) {
    let editor = auth(r, 5, "t-editor");
    let state = snapshot_state(subscribe(r, editor, RESTORED));
    let mut doc = Document::decode_state_as(cid(5), 0, &state).expect("the branch state decodes");
    doc.set_schema(Schema::parse(schema).expect("the schema parses"));
    (editor, doc)
}

#[test]
fn a_live_branch_write_into_an_element_denied_subtree_is_withheld() {
    // The headline seam. The fan-out takes the branch and then indexes `main`, where the
    // denied container no longer stands: the deny resolves to nothing and goes inert, and
    // the op's own target resolves to nothing and falls back to the readable root. The
    // reader needs no reconnect and no restore — just an already-open branch subscription
    // and any branch write.
    assert_branch_write_is_redacted(Deny::Element);
}

#[test]
fn a_live_branch_write_into_a_path_denied_subtree_is_withheld() {
    // A path scope resolves without a tree, so the tuple itself always bit. The leak is
    // the op's *target*: `main` cannot resolve the branch's `/b` container, so
    // `op_read_path` falls back to the root, and the root is readable — the deny is never
    // consulted at all.
    assert_branch_write_is_redacted(Deny::Path);
}

/// A live write into the denied `/b` and a live write into the readable `/a`, both on the
/// restored branch: the denied one is withheld from bob, the readable one is not.
fn assert_branch_write_is_redacted(deny: Deny) {
    let mut r = restored_room(deny);

    // The reader joins the branch first, so the writes below reach it as a *live*
    // fan-out rather than through any catch-up.
    let bob = auth(&mut r, 2, "t-bob");
    let served = snapshot_doc(subscribe(&mut r, bob, b""));
    assert_eq!(
        nested(&served, b"a", b"aseed"),
        Some(0),
        "the reader was not served the subtree it may read",
    );
    assert!(
        served.get(b"b").is_none(),
        "the catch-up snapshot itself leaked the denied subtree (C32)",
    );

    let (editor, mut editor_doc) = branch_editor(&mut r, OPEN_READ);
    let denied = slot_id(&editor_doc, b"b");
    let readable = slot_id(&editor_doc, b"a");
    r.take_outbox(bob);
    let ops = editor_doc.transact(|tx| {
        tx.map(b"b").register(b"blive", Scalar::Int(7));
        tx.map(b"a").register(b"alive", Scalar::Int(9));
    });
    submit(&mut r, editor, ops);

    let targets = delivered_targets(&mut r, bob);
    assert!(
        !targets.contains(&denied),
        "the live fan-out delivered a write into a subtree the reader is denied",
    );
    assert!(
        targets.contains(&readable),
        "the live fan-out withheld a write the reader may read",
    );
}

#[test]
fn a_denied_element_born_on_the_branch_tail_is_withheld_from_the_next_write() {
    // The branch's tree is its base *plus its divergent tail*, and the tail grows under
    // the fan-out. A container created on the tail and denied there stands in no captured
    // base and in no `main` — only in the tree folded forward by the very ops this seam
    // fanned out. A later write into it must still find the deny.
    let (mut r, alice, mut alice_doc) = base_room(OPEN_READ);
    assert!(r.hub_mut().create_version(ROOM, V1).unwrap());
    assert!(r.restore_as_branch(ROOM, V1, RESTORED).unwrap());

    let (editor, mut editor_doc) = branch_editor(&mut r, OPEN_READ);
    let ops = editor_doc.transact(|tx| {
        tx.map(b"b").register(b"bseed", Scalar::Int(0));
    });
    submit(&mut r, editor, ops);
    let denied = slot_id(&editor_doc, b"b");
    assert!(
        room_doc(&r).get(b"b").is_none(),
        "the tail write leaked onto `main`",
    );
    // Doc-ACL tuples are a room fact, so the deny is authored on `main` — against a
    // container that exists only on the branch.
    let ops = alice_doc.transact(|tx| {
        tx.acl().grant_element(
            AclSubject::Actor(actor_key(b"bob")),
            AclGrant::Capability(Capability::Read),
            AclEffect::Deny,
            denied,
            actor_key(b"alice"),
        );
    });
    submit(&mut r, alice, ops);

    let bob = auth(&mut r, 2, "t-bob");
    subscribe(&mut r, bob, b"");
    r.take_outbox(bob);
    let readable = slot_id(&editor_doc, b"a");
    let ops = editor_doc.transact(|tx| {
        tx.map(b"b").register(b"blive", Scalar::Int(7));
        tx.map(b"a").register(b"alive", Scalar::Int(9));
    });
    submit(&mut r, editor, ops);
    let targets = delivered_targets(&mut r, bob);
    assert!(
        !targets.contains(&denied),
        "a write into a container the branch tail placed reached a denied reader",
    );
    assert!(
        targets.contains(&readable),
        "the readable half went with it, so the withholding above proves nothing",
    );
}

#[test]
fn a_branch_tail_replayed_as_ops_is_filtered_by_the_branch_tree() {
    // Seam (c). A subscriber already past the branch's fork point is served the divergent
    // tail as ops, not as a snapshot — filtered through the same index. A tail op into a
    // denied subtree must not replay, and a readable one must, or op-join and
    // snapshot-join disagree on the same branch.
    let mut r = restored_room(Deny::Element);
    let fork_point = r
        .hub()
        .branch(ROOM, RESTORED)
        .expect("the restored branch exists")
        .fork_point;

    let (editor, mut editor_doc) = branch_editor(&mut r, OPEN_READ);
    let denied = slot_id(&editor_doc, b"b");
    let readable = slot_id(&editor_doc, b"a");
    let ops = editor_doc.transact(|tx| {
        tx.map(b"b").register(b"blive", Scalar::Int(7));
        tx.map(b"a").register(b"alive", Scalar::Int(9));
    });
    submit(&mut r, editor, ops);

    let bob = auth(&mut r, 2, "t-bob");
    let reply = subscribe_from(&mut r, bob, RESTORED, fork_point);
    let targets: Vec<ElementId> = ops_of(&reply).iter().map(|op| op.target).collect();
    assert!(
        targets.contains(&readable),
        "the readable half of the replayed tail was withheld: {reply:?}",
    );
    assert!(
        !targets.contains(&denied),
        "a tail op into a denied subtree replayed to the reader",
    );
}

#[test]
fn an_element_grant_the_branch_holds_admits_the_subscribe() {
    // Seam (b). Bob's only read authority is an element-scoped *grant*, with the schema
    // and the deployment both abstaining — so `has_any_read_grant` is the whole gate.
    // Resolved through `main`, where the container no longer stands, the grant drops and
    // the reader is refused a subscription the branch would have served.
    let (mut r, alice, mut alice_doc) = base_room(NO_GRANTS);
    let ops = alice_doc.transact(|tx| {
        tx.map(b"b").register(b"bseed", Scalar::Int(0));
    });
    submit(&mut r, alice, ops);
    let target = slot_id(&alice_doc, b"b");
    grant_read_element(&mut r, alice, &mut alice_doc, target);
    assert!(r.hub_mut().create_version(ROOM, V1).unwrap());
    let ops = alice_doc.transact(|tx| tx.delete(b"b"));
    submit(&mut r, alice, ops);
    assert!(r.restore_as_branch(ROOM, V1, RESTORED).unwrap());

    let bob = auth(&mut r, 2, "t-bob");
    let served = snapshot_doc(subscribe(&mut r, bob, b""));
    assert_eq!(
        nested(&served, b"b", b"bseed"),
        Some(0),
        "the granted subtree was not served",
    );
    assert!(
        served.get(b"a").is_none(),
        "a subtree-scoped reader was served a subtree it holds no grant on",
    );
}

#[test]
fn a_reader_the_branch_grants_nothing_is_refused() {
    // The mirror of the gate: resolving against the branch must not admit a reader the
    // branch serves nothing. Bob's grant names a container created *after* the version
    // was captured, so the branch's tree resolves it to nothing — and the subscription is
    // refused rather than opened onto an empty serve.
    let (mut r, alice, mut alice_doc) = base_room(NO_GRANTS);
    assert!(r.hub_mut().create_version(ROOM, V1).unwrap());
    let ops = alice_doc.transact(|tx| {
        tx.map(b"b").register(b"bseed", Scalar::Int(0));
    });
    submit(&mut r, alice, ops);
    let target = slot_id(&alice_doc, b"b");
    grant_read_element(&mut r, alice, &mut alice_doc, target);
    assert!(r.restore_as_branch(ROOM, V1, RESTORED).unwrap());

    let bob = auth(&mut r, 2, "t-bob");
    let reply = subscribe(&mut r, bob, b"");
    assert!(
        matches!(reply, Message::Error { ref message, .. } if message == "read denied"),
        "a reader the branch grants nothing was admitted: {reply:?}",
    );
}

/// Grant bob doc-ACL read on the container `target`, wherever it stands.
fn grant_read_element(r: &mut Registry, alice: ConnId, doc: &mut Document, target: ElementId) {
    let ops = doc.transact(|tx| {
        tx.acl().grant_element(
            AclSubject::Actor(actor_key(b"bob")),
            AclGrant::Capability(Capability::Read),
            AclEffect::Allow,
            target,
            actor_key(b"alice"),
        );
    });
    submit(r, alice, ops);
}

#[test]
fn a_main_fan_out_still_resolves_against_the_live_room() {
    // The `main` stream *is* the live room, so nothing moves there: the deny bites on the
    // container that is still standing, and the readable half still arrives.
    let (mut r, alice, mut alice_doc) = base_room(OPEN_READ);
    seed_denied_slot(&mut r, alice, &mut alice_doc, Deny::Element);
    let denied = slot_id(&alice_doc, b"b");
    let readable = slot_id(&alice_doc, b"a");

    let bob = auth(&mut r, 2, "t-bob");
    subscribe(&mut r, bob, b"main");
    r.take_outbox(bob);
    let ops = alice_doc.transact(|tx| {
        tx.map(b"b").register(b"blive", Scalar::Int(7));
        tx.map(b"a").register(b"alive", Scalar::Int(9));
    });
    submit(&mut r, alice, ops);

    let targets = delivered_targets(&mut r, bob);
    assert!(
        targets.contains(&readable),
        "a readable `main` write was withheld",
    );
    assert!(
        !targets.contains(&denied),
        "a `main` write into a denied subtree reached the reader",
    );
}

#[test]
fn a_compacted_shared_base_stops_a_live_log_forks_fan_out() {
    // The seam-level reading of the same rule. A live-log fork shares `main`'s retained
    // log, and compaction drops those records — so the tree the branch would fold is its
    // tail over nothing, and every container the shared base placed stops resolving. That
    // is the fail-open shape: the deny on `/b` never resolves an op target into it, so
    // the write reads at the root, which this reader holds. Nothing is sent instead, and
    // a fresh joiner is told the branch is unreadable rather than served through a tree
    // that is not the stream's.
    let (mut r, alice, mut alice_doc) = base_room(OPEN_READ);
    seed_denied_slot(&mut r, alice, &mut alice_doc, Deny::Path);
    assert!(r
        .hub_mut()
        .fork_branch(ROOM, LIVE_FORK, b"main", u64::MAX)
        .unwrap());

    // Both the writer and the reader join the fork while its shared base is still
    // retained, so the compaction below lands under an open subscription. A live-log
    // fork catches up as ops, so the editor's replica is folded from them — and it
    // therefore addresses the very containers the shared base placed.
    let editor = auth(&mut r, 5, "t-editor");
    let served = subscribe(&mut r, editor, LIVE_FORK);
    let mut editor_doc = Document::new(cid(5));
    for op in ops_of(&served) {
        editor_doc.apply(op);
    }
    editor_doc.set_schema(Schema::parse(OPEN_READ).expect("the schema parses"));
    let denied = slot_id(&editor_doc, b"b");
    let bob = auth(&mut r, 2, "t-bob");
    subscribe(&mut r, bob, LIVE_FORK);

    r.hub_mut().compact(ROOM).unwrap();
    r.take_outbox(bob);
    let ops = editor_doc.transact(|tx| {
        tx.map(b"b").register(b"blive", Scalar::Int(7));
    });
    submit(&mut r, editor, ops);
    assert!(
        !delivered_targets(&mut r, bob).contains(&denied),
        "a compacted shared base let a branch write into a denied subtree through",
    );

    let joiner = auth(&mut r, 6, "t-bob");
    let reply = subscribe(&mut r, joiner, LIVE_FORK);
    assert!(
        matches!(reply, Message::Error { ref message, .. } if message == "branch state is unreadable"),
        "a branch this node cannot fold was served a catch-up: {reply:?}",
    );
}

#[test]
fn a_branch_write_addressed_to_a_container_the_branch_never_held_is_withheld() {
    // The mirror hazard of swapping the tree. `op_read_path` falls an unresolvable
    // container target back to the **root**, and which targets are unresolvable is a
    // property of the index: against `main`'s it meant "since deleted or displaced",
    // and against the branch's it also means "belongs to another stream". A replica
    // holding `main`'s state can address a container the branch never had — the write
    // lands nowhere on the branch, but it carries the payload of a denied subtree, and
    // the root is readable. The branch's own projected snapshot drops such a buffered op
    // whole, so withholding it is what the two joins agree on.
    let (mut r, alice, mut alice_doc) = base_room(OPEN_READ);
    // The branch is captured *before* `/b` exists, so `/b` stands only on `main`.
    assert!(r.hub_mut().create_version(ROOM, V1).unwrap());
    assert!(r.restore_as_branch(ROOM, V1, RESTORED).unwrap());
    seed_denied_slot(&mut r, alice, &mut alice_doc, Deny::Path);
    let denied = slot_id(&alice_doc, b"b");

    let bob = auth(&mut r, 2, "t-bob");
    subscribe(&mut r, bob, RESTORED);

    // A second connection of alice's, bound to the branch, authoring from a replica that
    // holds `main`'s tree — so its write names `main`'s `/b` container.
    let editor = auth(&mut r, 5, "t-editor");
    subscribe(&mut r, editor, RESTORED);
    let mut editor_doc = Document::decode_state_as(
        cid(5),
        0,
        &r.hub().export_room(ROOM).expect("the room exists"),
    )
    .expect("the room state decodes");
    editor_doc.set_schema(Schema::parse(OPEN_READ).expect("the schema parses"));
    assert_eq!(slot_id(&editor_doc, b"b"), denied);

    r.take_outbox(bob);
    let ops = editor_doc.transact(|tx| {
        tx.map(b"b").register(b"blive", Scalar::Int(7));
    });
    submit(&mut r, editor, ops);
    assert!(
        !delivered_targets(&mut r, bob).contains(&denied),
        "a branch write into a container only `main` holds reached a reader denied it",
    );
}

// The held tree's own contract, exercised on the [`Hub`] directly. Everything above
// reaches `stream_element_paths` through a redaction seam, which only ever observes
// the index the seam happened to need; these observe the index itself, against a
// fresh fold of the very stream a subscriber joining at sequence 0 is served.

/// The index `stream_element_paths` answers for a stream — id *and* path, since a
/// container placed under the wrong path redacts as wrongly as one missing outright.
fn indexed(hub: &mut Hub, branch: &[u8]) -> ElementPaths {
    hub.stream_element_paths(HROOM, branch)
        .expect("the stream has a tree")
}

/// The index a *fresh* fold of the same stream answers — the oracle the held tree owes
/// an equal answer to.
fn refolded(hub: &mut Hub, branch: &[u8]) -> ElementPaths {
    let doc = match hub.catch_up_branch(HROOM, branch, 0) {
        Catchup::Snapshot { state, .. } => {
            Document::decode_state(&state).expect("the stream's state decodes")
        }
        Catchup::Ops(ops) => {
            let mut doc = Document::new(cid(0xFF));
            for rec in ops {
                doc.apply(&rec.op);
            }
            doc
        }
        Catchup::Unavailable => panic!("the stream cannot be folded"),
    };
    crdtsync_server::index::element_paths(&doc)
}

const HROOM: &[u8] = b"room-c60-hub";
const B1: &[u8] = b"b1";
const B2: &[u8] = b"b2";

/// A map under `key`, holding one register — a container the index reaches.
fn sect(d: &mut Document, key: &[u8]) -> Vec<Op> {
    d.transact(|tx| {
        tx.map(key).register(b"n", Scalar::Int(1));
    })
}

#[test]
fn the_held_tree_follows_a_live_log_forks_growing_shared_base() {
    // A live-log fork's base is `main`'s log clamped to its fork point, so a fork point
    // *above* `main`'s head keeps admitting `main`'s later writes. Forking off a branch
    // whose tail runs past `main` is how a fork point gets there: `fork_branch` clamps
    // to the source's head, and a branch head counts its divergent tail. `main`'s head
    // is therefore an input to the fold, and a held tree that does not re-check it goes
    // stale reaching *less* than the stream serves — which is the fail-open direction,
    // since an element the index misses is an inert scope and a root-bound op target.
    let mut hub = Hub::new(cid(0xFF));
    let mut main = Document::new(cid(1));
    hub.ingest(HROOM, sect(&mut main, b"m1"), None).unwrap();
    assert!(hub.fork_branch(HROOM, B1, b"main", u64::MAX).unwrap());
    // A tail on `b1` lifts its head past `main`'s, so `b2` forks above `main`'s head.
    let mut tail = Document::new(cid(2));
    hub.ingest_branch(HROOM, B1, sect(&mut tail, b"t1"), None)
        .unwrap();
    assert!(hub.fork_branch(HROOM, B2, B1, u64::MAX).unwrap());
    assert!(hub.branch(HROOM, B2).unwrap().fork_point > hub.seq(HROOM));

    let first = indexed(&mut hub, B2);
    assert_eq!(first, refolded(&mut hub, B2));

    hub.ingest(HROOM, sect(&mut main, b"m2"), None).unwrap();
    let after = indexed(&mut hub, B2);
    assert_eq!(
        after,
        refolded(&mut hub, B2),
        "the held tree stopped following the shared base the stream still serves",
    );
    assert!(
        after.len() > first.len(),
        "the write into the shared base reached neither the held tree nor the fold",
    );
}

#[test]
fn the_held_tree_follows_its_own_tail_and_a_repointed_base() {
    // The tail is the input that moves under the fan-out, so it is the one folded
    // forward rather than refolded; a publish replaces the base under the same name and
    // drops the tail, which is the shape a forward fold must not be applied to.
    let mut hub = Hub::new(cid(0xFF));
    let mut main = Document::new(cid(1));
    hub.ingest(HROOM, sect(&mut main, b"m1"), None).unwrap();
    assert!(hub.fork_branch(HROOM, B1, b"main", u64::MAX).unwrap());

    let mut tail = Document::new(cid(2));
    assert_eq!(indexed(&mut hub, B1), refolded(&mut hub, B1));
    hub.ingest_branch(HROOM, B1, sect(&mut tail, b"t1"), None)
        .unwrap();
    let grown = indexed(&mut hub, B1);
    assert_eq!(grown, refolded(&mut hub, B1));
    hub.ingest_branch(HROOM, B1, sect(&mut tail, b"t2"), None)
        .unwrap();
    assert_eq!(indexed(&mut hub, B1), refolded(&mut hub, B1));

    // A publish repoints `B1`'s base onto `main`'s state and drops its tail, so the
    // stream it serves no longer contains `t1`/`t2`.
    assert!(hub.publish(HROOM, B1).unwrap());
    assert_eq!(
        indexed(&mut hub, B1),
        refolded(&mut hub, B1),
        "the held tree survived a repoint that replaced the stream under it",
    );
}

#[test]
fn a_refork_of_a_retired_name_inherits_no_held_tree() {
    // A name is reusable, and the stream behind it is not the retired one's. Its
    // recorded inputs can coincide exactly — same fork point, same absent base, same
    // shared-base window, and a fresh tail grown back to the retired one's length — at
    // which point nothing about the numbers says the tree is the wrong one. The
    // retirement is what says it.
    let mut hub = Hub::new(cid(0xFF));
    let mut main = Document::new(cid(1));
    hub.ingest(HROOM, sect(&mut main, b"m1"), None).unwrap();
    assert!(hub.fork_branch(HROOM, B1, b"main", u64::MAX).unwrap());
    let mut retired = Document::new(cid(2));
    hub.ingest_branch(HROOM, B1, sect(&mut retired, b"t1"), None)
        .unwrap();
    hub.ingest_branch(HROOM, B1, sect(&mut retired, b"t2"), None)
        .unwrap();
    let held = indexed(&mut hub, B1);

    assert!(hub.delete_branch(HROOM, B1).unwrap());
    assert!(hub.fork_branch(HROOM, B1, b"main", u64::MAX).unwrap());
    let mut fresh = Document::new(cid(3));
    hub.ingest_branch(HROOM, B1, sect(&mut fresh, b"u1"), None)
        .unwrap();
    hub.ingest_branch(HROOM, B1, sect(&mut fresh, b"u2"), None)
        .unwrap();
    let refork = indexed(&mut hub, B1);
    assert_eq!(
        refork,
        refolded(&mut hub, B1),
        "the re-forked name kept the retired branch's tree",
    );
    assert_ne!(
        refork, held,
        "the two streams are indistinguishable, so this pins nothing",
    );
}

#[test]
fn a_compacted_shared_base_leaves_a_live_log_fork_with_no_tree() {
    // Compaction folds `main`'s log into its replica and drops the records, so a
    // live-log fork's shared base goes with them and the stream serves its tail over
    // nothing. Narrowing the tree to match would be the fail-open direction — the base's
    // containers stop resolving, which turns every scope over them inert and roots every
    // op into them — so the stream has no tree to redact against at all, and the seams
    // refuse rather than redact by a poorer one.
    let mut hub = Hub::new(cid(0xFF));
    let mut main = Document::new(cid(1));
    hub.ingest(HROOM, sect(&mut main, b"m1"), None).unwrap();
    assert!(hub.fork_branch(HROOM, B1, b"main", u64::MAX).unwrap());
    let mut tail = Document::new(cid(2));
    hub.ingest_branch(HROOM, B1, sect(&mut tail, b"t1"), None)
        .unwrap();
    assert_eq!(indexed(&mut hub, B1), refolded(&mut hub, B1));

    hub.compact(HROOM).unwrap();
    assert!(
        hub.stream_element_paths(HROOM, B1).is_none(),
        "a compacted shared base left a tree that resolves less than the branch holds",
    );
    // `main` is unaffected: its replica carries what the log folded into it.
    assert!(hub.stream_element_paths(HROOM, b"main").is_some());
}
