//! The catch-up gate resolves element grants against the tree the snapshot *is* (C32).
//!
//! `project_served_state`'s whole-document gate decides whether a catch-up snapshot is
//! served verbatim or projected to the reader's readable subtrees. It resolves
//! element-scoped grants through an index, and an element scope the index cannot
//! resolve is dropped — so an index built from a *different* tree than the bytes being
//! decided for turns an element-scoped read-deny inert, and a gate that finds no deny
//! serves the state whole.
//!
//! A `main` catch-up snapshot is the live room, so the live index is that tree. A
//! branch that owns its base — a restore, a publish — is served that captured base with
//! its own divergent tail folded in, while `main` moves on: an element that has left
//! `main` resolves to no live path, and one born on the tail never had one. The gate
//! must resolve against the bytes it is about to hand out.
//!
//! Reached on the mainline subscribe path: restore-as-branch is a fork-from-version
//! plus an active-HEAD switch, and a plain (unnamed) Subscribe follows the room's
//! active branch — so an ordinary reader joining after a restore takes this seam, as
//! does every read-only consumer of a published branch.
//!
//! Everything runs in-process through the [`Registry`] (no socket, no fs), so the
//! suite runs under Miri.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{
    AclEffect, ClientId, Document, Element, ElementId, Message, Op, Scalar, Schema,
};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::{ConnId, ManualClock, Registry, SchemaRegistry, StaticTokens};

const ROOM: &[u8] = b"room-c32";
const CH: Channel = Channel(0);
const V1: &[u8] = b"v1";
const RESTORED: &[u8] = b"restored";
const PUBLISHED: &[u8] = b"published";
const APP: &[u8] = b"p";

/// Room read is granted by the schema tier to every authenticated actor, so a reader
/// with no doc-ACL root grant still passes the subscribe gate — and a doc-ACL deny is
/// what carves a subtree back out of it.
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

/// Subscribe `id` to `(ROOM, branch)` on `CH`, returning the catch-up reply.
fn subscribe(r: &mut Registry, id: ConnId, branch: &[u8]) -> Message {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            branch: branch.to_vec(),
            zone: Vec::new(),
            last_seen_seq: 0,
        },
    ));
    r.take_outbox(id)
        .into_iter()
        .next()
        .expect("a catch-up reply")
}

/// The whole-replica state a catch-up carried — the bytes under test.
fn snapshot_state(m: Message) -> Vec<u8> {
    match m {
        Message::Snapshot { state, .. } => state,
        other => panic!("expected a Snapshot catch-up, got {other:?}"),
    }
}

/// The state a fresh connection is served for `branch` — empty follows the active HEAD.
fn served(r: &mut Registry, client: u8, credential: &str, branch: &[u8]) -> Vec<u8> {
    let id = auth(r, client, credential);
    snapshot_state(subscribe(r, id, branch))
}

/// The state a fresh connection is served on a plain (unnamed) subscribe, decoded.
fn plain_snapshot(r: &mut Registry, client: u8, credential: &str) -> Document {
    let state = served(r, client, credential, b"");
    Document::decode_state(&state).expect("the served state decodes")
}

fn submit(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(id, Message::Ops { channel: CH, ops }));
    r.take_outbox(id);
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

/// The distinct replicas a served state's causal frontier names — the tell for whether
/// a projection ran at all, since every projection scrubs the frontier to the recipient.
fn frontier_authors(state: &[u8]) -> HashSet<ClientId> {
    Document::decode_state(state)
        .expect("the served state decodes")
        .seen()
        .map(|id| id.client)
        .collect()
}

/// The container id `key` holds in `doc`.
fn slot_id(doc: &Document, key: &[u8]) -> ElementId {
    match doc.get(key) {
        Some(Element::Map(m)) => m.borrow().id(),
        _ => panic!("slot {key:?} is not a map"),
    }
}

/// Which shape of read-deny alice installs on `/b`.
enum Deny {
    /// A fixed-path tuple on `/b` — resolvable in any tree, since it names no element.
    Path,
    /// A stable-element tuple on the container `/b` currently holds, so the deny
    /// resolves only through a tree that still holds that element.
    Element,
}

/// A registry with the partial schema bound, alice permitted everything by the
/// deployment and bob abstained on — so bob's verdicts are the schema and doc-ACL
/// tiers' alone. Returns the registry, alice's conn, and alice's author document.
fn base_room() -> (Registry, ConnId, Document) {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, PARTIAL.as_bytes(), b"").unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens(&[
        ("t-alice", "alice"),
        ("t-editor", "alice"),
        ("t-bob", "bob"),
        ("t-carol", "carol"),
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
    alice_doc.set_schema(Schema::parse(PARTIAL).expect("the partial schema parses"));
    // Write authority is a room-level verdict, so bob's write grant roots at `/`;
    // carol's read grant roots there too, making her a whole-document reader through
    // the doc-ACL tier rather than the deployment's.
    let ops = alice_doc.transact(|tx| {
        tx.map(b"a").register(b"aseed", Scalar::Int(0));
        tx.acl().grant(
            AclSubject::Actor(actor_key(b"bob")),
            AclGrant::Capability(Capability::Write),
            AclEffect::Allow,
            encode_path(&[]),
            actor_key(b"alice"),
        );
        tx.acl().grant(
            AclSubject::Actor(actor_key(b"carol")),
            AclGrant::Capability(Capability::Read),
            AclEffect::Allow,
            encode_path(&[]),
            actor_key(b"alice"),
        );
    });
    submit(&mut r, alice, ops);
    (r, alice, alice_doc)
}

/// Seed `/b` and deny bob read on it, in the shape `deny` names.
fn seed_denied_slot(r: &mut Registry, alice: ConnId, doc: &mut Document, deny: Deny) {
    let ops = doc.transact(|tx| {
        tx.map(b"b").register(b"bseed", Scalar::Int(0));
    });
    submit(r, alice, ops);
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
        Deny::Element => {
            let target = slot_id(doc, b"b");
            deny_element_ops(doc, target)
        }
    };
    submit(r, alice, ops);
}

/// Deny bob read on the element `target`, wherever it stands.
fn deny_element_ops(doc: &mut Document, target: ElementId) -> Vec<Op> {
    doc.transact(|tx| {
        tx.acl().grant_element(
            AclSubject::Actor(actor_key(b"bob")),
            AclGrant::Capability(Capability::Read),
            AclEffect::Deny,
            target,
            actor_key(b"alice"),
        );
    })
}

/// `/a` and a denied `/b`, `v1` capturing the tree that still holds `/b`, `/b` then
/// dropped from the live `main` — so the denied element resolves to no live path while
/// the captured base still carries it.
fn captured_then_dropped(deny: Deny) -> (Registry, ConnId, Document) {
    let (mut r, alice, mut alice_doc) = base_room();
    seed_denied_slot(&mut r, alice, &mut alice_doc, deny);
    assert!(r.hub_mut().create_version(ROOM, V1).unwrap());
    let ops = alice_doc.transact(|tx| tx.delete(b"b"));
    submit(&mut r, alice, ops);
    assert!(
        room_doc(&r).get(b"b").is_none(),
        "the live room still holds the denied element",
    );
    (r, alice, alice_doc)
}

/// `captured_then_dropped` plus the restore: `v1` becomes branch `restored` and the
/// room's active HEAD, so a plain subscribe catches up on that branch's own base.
fn restored_room(deny: Deny) -> Registry {
    let (mut r, _alice, _alice_doc) = captured_then_dropped(deny);
    assert!(r.restore_as_branch(ROOM, V1, RESTORED).unwrap());
    r
}

#[test]
fn an_element_scoped_deny_resolves_against_the_restored_branchs_own_base() {
    // The gate that decides whether to project at all was fed the live `main` tree,
    // where the denied element no longer stands. An unresolvable element scope is
    // inert, an inert deny is no deny, and a gate that finds none serves the branch's
    // base whole — handing the reader the very subtree it is denied.
    let mut r = restored_room(Deny::Element);
    let snapshot = plain_snapshot(&mut r, 2, "t-bob");
    assert_eq!(
        nested(&snapshot, b"a", b"aseed"),
        Some(0),
        "the snapshot withheld the subtree the reader may read",
    );
    assert!(
        snapshot.get(b"b").is_none(),
        "the branch base was served unprojected, carrying an element the reader is denied",
    );
}

#[test]
fn a_path_scoped_deny_narrows_the_restored_branchs_base_too() {
    // A path scope resolves without a tree, so this half always narrowed — it pins that
    // the element shape is the only thing the gate's index decided, and that the branch
    // seam projects at all.
    let mut r = restored_room(Deny::Path);
    let snapshot = plain_snapshot(&mut r, 2, "t-bob");
    assert_eq!(nested(&snapshot, b"a", b"aseed"), Some(0));
    assert!(
        snapshot.get(b"b").is_none(),
        "the branch base served a path the reader is denied",
    );
}

#[test]
fn a_doc_acl_whole_document_reader_takes_the_restored_base_whole() {
    // Carol reads the whole document through a doc-ACL root grant with the deployment
    // abstaining, so her verdict is decided by the carve-out scan — every tuple path
    // resolved through the very index this unit changed. Narrowing a partial reader
    // must not narrow her.
    let mut r = restored_room(Deny::Element);
    let state = served(&mut r, 4, "t-carol", b"");
    let snapshot = Document::decode_state(&state).expect("the served state decodes");
    assert_eq!(nested(&snapshot, b"a", b"aseed"), Some(0));
    assert_eq!(
        nested(&snapshot, b"b", b"bseed"),
        Some(0),
        "a whole-document reader's branch base was narrowed",
    );
    assert_eq!(
        frontier_authors(&state),
        HashSet::from([cid(1)]),
        "a whole-document reader's frontier was scrubbed, so a projection ran",
    );
}

#[test]
fn an_element_born_on_the_branch_tail_resolves_and_is_denied() {
    // What is served is the base with the branch's divergent tail folded in, so the
    // stored base is not the tree being decided for either. This element exists in
    // neither `main` nor the base — only in the tail — and the deny on it still bites.
    let (mut r, alice, mut alice_doc) = base_room();
    assert!(r.hub_mut().create_version(ROOM, V1).unwrap());
    assert!(r.restore_as_branch(ROOM, V1, RESTORED).unwrap());

    // Alice's original channel stayed bound to `main` across the restore, so the tail
    // is written from a second connection subscribed to the restored branch.
    let editor = auth(&mut r, 5, "t-editor");
    subscribe(&mut r, editor, RESTORED);
    let mut branch_doc = Document::new(cid(5));
    branch_doc.set_schema(Schema::parse(PARTIAL).expect("the partial schema parses"));
    let ops = branch_doc.transact(|tx| {
        tx.map(b"b").register(b"bseed", Scalar::Int(0));
    });
    submit(&mut r, editor, ops);
    let target = slot_id(&branch_doc, b"b");
    assert!(
        room_doc(&r).get(b"b").is_none(),
        "the tail write leaked onto `main`",
    );
    let ops = deny_element_ops(&mut alice_doc, target);
    submit(&mut r, alice, ops);

    let snapshot = plain_snapshot(&mut r, 2, "t-bob");
    assert_eq!(nested(&snapshot, b"a", b"aseed"), Some(0));
    assert!(
        snapshot.get(b"b").is_none(),
        "an element the tail placed was served to a reader denied it",
    );
}

#[test]
fn a_published_branchs_base_is_narrowed_for_a_named_subscribe() {
    // A published branch owns its base the same way a restored one does — and its whole
    // audience is read-only consumers, who reach it by name rather than by following
    // the active HEAD.
    let (mut r, alice, mut alice_doc) = base_room();
    seed_denied_slot(&mut r, alice, &mut alice_doc, Deny::Element);
    assert!(r.hub_mut().publish(ROOM, PUBLISHED).unwrap());
    let ops = alice_doc.transact(|tx| tx.delete(b"b"));
    submit(&mut r, alice, ops);
    assert!(room_doc(&r).get(b"b").is_none());

    let state = served(&mut r, 2, "t-bob", PUBLISHED);
    let snapshot = Document::decode_state(&state).expect("the served state decodes");
    assert_eq!(nested(&snapshot, b"a", b"aseed"), Some(0));
    assert!(
        snapshot.get(b"b").is_none(),
        "the published base was served unprojected to a denied reader",
    );
}

#[test]
fn an_element_scoped_deny_still_resolves_against_the_live_room_on_main() {
    // The `main` catch-up snapshot *is* the live room, so its gate keeps resolving
    // against the live index — the element is still there, and the deny still bites.
    let (mut r, alice, mut alice_doc) = base_room();
    seed_denied_slot(&mut r, alice, &mut alice_doc, Deny::Element);
    // Compact, so a fresh subscriber below the floor is served a snapshot rather than
    // an op delta — the `main` half of the same seam.
    r.hub_mut().compact(ROOM).unwrap();

    let snapshot = plain_snapshot(&mut r, 2, "t-bob");
    assert_eq!(nested(&snapshot, b"a", b"aseed"), Some(0));
    assert!(
        snapshot.get(b"b").is_none(),
        "a live `main` snapshot served an element the reader is denied",
    );
}
