//! A diff query is served through the same redactions the live stream is (C27).
//!
//! A change list is a room's own content in a different shape: every `Change` carries
//! a `core::path`, and a value change carries the scalar at it. `DiffQuery` resolved
//! two states and diffed them raw, gated only on the room-read tier a version request
//! uses — the tier that deliberately admits readers the live stream only ever serves
//! *narrowed*. So a zone-limited subscriber read any withheld partition as a diff
//! (create `va`, wait for a write into the hidden zone, create `vb`, diff), and
//! `DiffKind::Branches` needed no versions at all.
//!
//! The fix is C15's composition run per side, before the engine: each state goes
//! through `project_served_state` — the read projection then the zone projection —
//! and the change list is the diff of the two states this reader would itself have
//! been served. (The states are not byte-identical to a fetch's: the two seams scrub
//! the causal frontier differently. The change list is, because it carries no
//! frontier.) A partition the reader may not read therefore contributes no change at
//! all, rather than a redacted one — as far as the projections themselves reach, which
//! for an element the live walk does not reach is not yet far enough (C37). The scope that makes it possible is the channel: the
//! query is channel-keyed like a version fetch, so the subscription's zone set is
//! what a diff narrows by.
//!
//! Everything runs in-process through the [`Registry`] (no socket, no fs), so the
//! suite runs under Miri.

use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::diff::{decode_changes, Change};
use crdtsync_core::path::{encode_path, parse_path};
use crdtsync_core::protocol::{Channel, DiffKind};
use crdtsync_core::{
    AclEffect, ClientId, Document, Element, ElementId, Message, Op, Scalar, Schema, Side,
};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::{
    Action, ConnId, Identity, ManualClock, Registry, Resource, SchemaRegistry, StaticTokens,
};

const ROOM: &[u8] = b"room-d";
const CH: Channel = Channel(0);
const VA: &[u8] = b"va";
const VB: &[u8] = b"vb";
const DRAFT: &[u8] = b"draft";

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
fn auth(r: &mut Registry, client: u8, credential: &str, app: &[u8], version: u32) -> ConnId {
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
            credential: credential.as_bytes().to_vec(),
        }
    ));
    r.take_outbox(id);
    id
}

fn subscribe_on(r: &mut Registry, id: ConnId, channel: Channel, zone: &[u8]) -> Vec<Message> {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel,
            room: ROOM.to_vec(),
            branch: Vec::new(),
            zone: zone.to_vec(),
            last_seen_seq: 0,
        },
    ));
    r.take_outbox(id)
}

fn subscribe(r: &mut Registry, id: ConnId, zone: &[u8]) -> Vec<Message> {
    subscribe_on(r, id, CH, zone)
}

fn submit(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(id, Message::Ops { channel: CH, ops }));
    r.take_outbox(id);
}

/// Take a named version of the channel's room, through the wire sub-protocol.
fn create_version(r: &mut Registry, id: ConnId, name: &[u8]) {
    assert!(r.deliver(
        id,
        Message::VersionCreate {
            channel: CH,
            name: name.to_vec(),
        }
    ));
    let names = r
        .take_outbox(id)
        .into_iter()
        .find_map(|m| match m {
            Message::Versions { names, .. } => Some(names),
            _ => None,
        })
        .expect("the mutation replies with the fresh name list");
    assert!(names.contains(&name.to_vec()));
}

/// The state `id` is served for the named version — the C15 seam, this suite's oracle
/// for "what may this reader see".
fn fetch_version(r: &mut Registry, id: ConnId, name: &[u8]) -> Vec<u8> {
    assert!(r.deliver(
        id,
        Message::VersionFetch {
            channel: CH,
            name: name.to_vec(),
        }
    ));
    r.take_outbox(id)
        .into_iter()
        .find_map(|m| match m {
            Message::VersionState { state, .. } => Some(state),
            _ => None,
        })
        .expect("the fetch replies with the version's state")
}

/// The change list `id` is served for a diff on its own channel.
fn diff_on(
    r: &mut Registry,
    id: ConnId,
    channel: Channel,
    kind: DiffKind,
    a: &[u8],
    b: &[u8],
) -> Vec<Change> {
    assert!(r.deliver(
        id,
        Message::DiffQuery {
            channel,
            kind,
            a: a.to_vec(),
            b: b.to_vec(),
        }
    ));
    let out = r.take_outbox(id);
    out.iter()
        .find_map(|m| match m {
            Message::DiffResult { changes, .. } => Some(changes.clone()),
            _ => None,
        })
        .map(|changes| decode_changes(&changes).expect("the change list decodes"))
        .unwrap_or_else(|| panic!("no diff result: {out:?}"))
}

fn diff(r: &mut Registry, id: ConnId, kind: DiffKind, a: &[u8], b: &[u8]) -> Vec<Change> {
    diff_on(r, id, CH, kind, a, b)
}

/// The room's materialized replica, decoded — the oracle for what the live tree holds.
fn room_doc(r: &Registry) -> Document {
    Document::decode_state(&r.hub().export_room(ROOM).expect("the room exists"))
        .expect("the room's state decodes")
}

/// The ops in a batch of reply frames, flattened.
fn ops_in(replies: Vec<Message>) -> Vec<Op> {
    replies
        .into_iter()
        .flat_map(|m| match m {
            Message::Ops { ops, .. } => ops,
            _ => Vec::new(),
        })
        .collect()
}

/// The first key of every change's path — the top-level subtree a change touches.
/// A mark change carries no path and is reported as `None`.
fn touched(changes: &[Change]) -> Vec<Option<Vec<u8>>> {
    changes
        .iter()
        .map(|c| {
            let path = match c {
                Change::Added { path, .. }
                | Change::Removed { path, .. }
                | Change::Value { path, .. }
                | Change::Counter { path, .. }
                | Change::ListInsert { path, .. }
                | Change::ListDelete { path, .. }
                | Change::TextInsert { path, .. }
                | Change::TextDelete { path, .. } => path,
                _ => return None,
            };
            parse_path(path)
                .expect("a served change carries a well-formed path")
                .first()
                .cloned()
        })
        .collect()
}

/// Whether any change reports the scalar `value` — the leak a path-only assertion
/// would miss, since a `Value` change carries the state itself.
fn reports_value(changes: &[Change], value: i64) -> bool {
    changes.iter().any(|c| match c {
        Change::Value { old, new, .. } => *old == Scalar::Int(value) || *new == Scalar::Int(value),
        _ => false,
    })
}

/// The diff of the two version states this reader is *served*, computed client-side —
/// the independent oracle for what its diff query must return.
fn served_version_diff(r: &mut Registry, id: ConnId, a: &[u8], b: &[u8]) -> Vec<Change> {
    let old = Document::decode_state(&fetch_version(r, id, a)).expect("the served state decodes");
    let new = Document::decode_state(&fetch_version(r, id, b)).expect("the served state decodes");
    crdtsync_core::path::diff(&old, &new)
}

// --- a zone-limited subscriber ---

/// Two zoned map subtrees (`/board` → za, `/notes` → zb) and one unzoned slot.
const ZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Sect", "notes": "Sect", "loose": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "zones": { "za": "/board", "zb": "/notes" }
}"#;

const ZONE_APP: &[u8] = b"z";

/// Every actor may read and write the room; only the zone verdicts isolate. `author`
/// reaches both zones, `reader` only za — so zb is wholly hidden from it.
fn zone_authorizer(id: &Identity, _action: Action, res: &Resource) -> bool {
    match res {
        Resource::Zone { zone, .. } => {
            let zone: &[u8] = zone;
            match id.actor() {
                b"author" => true,
                b"reader" => zone == b"za",
                _ => false,
            }
        }
        _ => true,
    }
}

fn zoned_schema() -> Schema {
    Schema::parse(ZONED).expect("the zoned schema parses")
}

/// A room bootstrapped by `author` with all three subtrees seeded, plus a za-scoped
/// `reader` whose replica has caught up. Returns the registry, the author's doc +
/// conn, and the reader's conn.
fn zoned_room() -> (Registry, Document, ConnId, ConnId) {
    let mut sr = SchemaRegistry::new();
    sr.register(ZONE_APP, 1, ZONED.as_bytes(), b"").unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens(&[
        ("c-author", "author"),
        ("c-reader", "reader"),
    ])));
    r.set_authorizer(Box::new(zone_authorizer));
    r.set_clock(Arc::new(ManualClock::new(0)));

    let author = auth(&mut r, 1, "c-author", ZONE_APP, 1);
    subscribe(&mut r, author, b"");
    let mut author_doc = Document::new(cid(1));
    author_doc.set_schema(zoned_schema());
    let setup = author_doc.transact(|tx| {
        tx.map(b"board").register(b"bseed", Scalar::Int(0));
        tx.map(b"notes").register(b"nseed", Scalar::Int(0));
        tx.map(b"loose").register(b"lseed", Scalar::Int(0));
    });
    submit(&mut r, author, setup);

    let reader = auth(&mut r, 2, "c-reader", ZONE_APP, 1);
    subscribe(&mut r, reader, b"za");
    (r, author_doc, author, reader)
}

/// Overwrite `key` under the zoned subtree `sect`, so the change diffs as a `Value`
/// carrying both scalars rather than a bare structural add.
fn write_into(r: &mut Registry, id: ConnId, doc: &mut Document, sect: &[u8], key: &[u8], v: i64) {
    let ops = doc.transact(|tx| {
        tx.map(sect).register(key, Scalar::Int(v));
    });
    submit(r, id, ops);
}

#[test]
fn a_zone_limited_readers_version_diff_withholds_the_zone_it_may_not_read() {
    let (mut r, mut author_doc, author, reader) = zoned_room();
    create_version(&mut r, author, VA);
    write_into(&mut r, author, &mut author_doc, b"notes", b"nseed", 4242);
    create_version(&mut r, author, VB);

    let changes = diff(&mut r, reader, DiffKind::Versions, VA, VB);
    assert!(
        changes.is_empty(),
        "the diff reported a change in a zone the reader may not read: {changes:?}",
    );
    assert!(
        !reports_value(&changes, 4242),
        "the diff carried the hidden partition's scalar",
    );
}

#[test]
fn a_zone_limited_readers_version_diff_still_reports_the_zone_it_may_read() {
    // The redaction is targeted, not blanket: the partition the reader is entitled
    // to still diffs, values and all.
    let (mut r, mut author_doc, author, reader) = zoned_room();
    create_version(&mut r, author, VA);
    write_into(&mut r, author, &mut author_doc, b"board", b"bseed", 7);
    write_into(&mut r, author, &mut author_doc, b"notes", b"nseed", 4242);
    create_version(&mut r, author, VB);

    let changes = diff(&mut r, reader, DiffKind::Versions, VA, VB);
    assert_eq!(
        changes,
        vec![Change::Value {
            path: encode_path(&[b"board", b"bseed"]),
            old: Scalar::Int(0),
            new: Scalar::Int(7),
        }],
        "the readable partition's change did not survive intact",
    );
}

#[test]
fn a_zone_limited_readers_diff_is_the_diff_of_the_versions_it_is_served() {
    // The invariant the fix expresses: a change list is the diff of the two states
    // this reader would itself have been handed, so the diff seam and the fetch seam
    // cannot disagree about what it may see. It holds over the change list rather
    // than over the bytes — the two seams scrub the frontier differently — which is
    // exactly the reach a diff has.
    let (mut r, mut author_doc, author, reader) = zoned_room();
    create_version(&mut r, author, VA);
    write_into(&mut r, author, &mut author_doc, b"board", b"bseed", 7);
    write_into(&mut r, author, &mut author_doc, b"notes", b"nseed", 4242);
    write_into(&mut r, author, &mut author_doc, b"loose", b"lseed", 5);
    create_version(&mut r, author, VB);

    assert_eq!(
        diff(&mut r, reader, DiffKind::Versions, VA, VB),
        served_version_diff(&mut r, reader, VA, VB),
    );
}

#[test]
fn a_zone_limited_readers_branch_diff_withholds_the_zone_it_may_not_read() {
    // A branch diff needs no version at all, so it is the shorter path to the same
    // bytes — and it is narrowed by the same composition.
    let (mut r, mut author_doc, author, reader) = zoned_room();
    let fork = r.hub().seq(ROOM);
    assert!(r.hub_mut().fork_branch(ROOM, DRAFT, b"main", fork).unwrap());
    write_into(&mut r, author, &mut author_doc, b"notes", b"nseed", 4242);

    // The positive control, so an empty answer cannot come from the two branches
    // having nothing between them: the author sees exactly the change the reader
    // does not.
    let seen = diff(&mut r, author, DiffKind::Branches, DRAFT, b"main");
    assert_eq!(touched(&seen), vec![Some(b"notes".to_vec())]);
    assert!(reports_value(&seen, 4242));

    let changes = diff(&mut r, reader, DiffKind::Branches, DRAFT, b"main");
    assert!(
        changes.is_empty(),
        "the branch diff reported a change in a zone the reader may not read: {changes:?}",
    );
    assert!(
        !reports_value(&changes, 4242),
        "the branch diff carried the hidden partition's scalar",
    );
}

#[test]
fn a_zone_limited_readers_diff_withholds_a_structural_add_in_the_hidden_zone() {
    // The hidden change everywhere else in this suite is a `Value`, which carries the
    // state itself. A fresh key is the other shape: nothing but a path and a kind,
    // which a redaction that only scrubbed values would still let through.
    let (mut r, mut author_doc, author, reader) = zoned_room();
    create_version(&mut r, author, VA);
    write_into(&mut r, author, &mut author_doc, b"notes", b"fresh", 1);
    create_version(&mut r, author, VB);

    let seen = diff(&mut r, author, DiffKind::Versions, VA, VB);
    assert_eq!(touched(&seen), vec![Some(b"notes".to_vec())]);

    let changes = diff(&mut r, reader, DiffKind::Versions, VA, VB);
    assert!(
        changes.is_empty(),
        "the diff reported a structural add in a zone the reader may not read: {changes:?}",
    );
}

#[test]
fn a_zone_limited_readers_diff_withholds_a_mark_anchored_in_the_hidden_zone() {
    // A mark change carries no path at all — it is addressed by its own id and its
    // target sequence — which is why the redaction runs over the two *states* rather
    // than over the change list a path predicate could filter.
    let (mut r, mut author_doc, author, reader) = zoned_room();
    let body = author_doc.transact(|tx| {
        tx.map(b"notes").text(b"body").insert(0, "secret");
    });
    submit(&mut r, author, body);
    create_version(&mut r, author, VA);
    let (ops, id) = crdtsync_core::path::mark(
        &mut author_doc,
        &encode_path(&[b"notes", b"body"]),
        0,
        Side::Left,
        6,
        Side::Right,
        b"bold",
        Scalar::Bool(true),
    );
    assert!(id.is_some(), "the mark anchored on a live sequence");
    submit(&mut r, author, ops);
    create_version(&mut r, author, VB);

    let seen = diff(&mut r, author, DiffKind::Versions, VA, VB);
    assert!(
        seen.iter().any(|c| matches!(c, Change::MarkAdded { .. })),
        "the author's own diff reported no mark: {seen:?}",
    );

    let changes = diff(&mut r, reader, DiffKind::Versions, VA, VB);
    assert!(
        changes.is_empty(),
        "the diff reported a mark anchored in a zone the reader may not read: {changes:?}",
    );
}

#[test]
fn a_diff_from_a_channel_subscribed_to_a_branch_still_narrows() {
    // Every other fixture here queries from a channel on `main`. This one queries from
    // a channel that named `draft` at Subscribe, which nothing in the arm reads — only
    // the channel's room and its zone set — so what it pins is that the seam works
    // from such a channel at all, not that the branch selects the partitioning. It
    // cannot: zone ids are the schema's, so they mean the same thing in every tree.
    let (mut r, mut author_doc, author, _reader) = zoned_room();
    let fork = r.hub().seq(ROOM);
    assert!(r.hub_mut().fork_branch(ROOM, DRAFT, b"main", fork).unwrap());
    write_into(&mut r, author, &mut author_doc, b"notes", b"nseed", 4242);
    write_into(&mut r, author, &mut author_doc, b"board", b"bseed", 7);

    // A za-scoped reader whose subscription follows `draft`, not `main`.
    let reader = auth(&mut r, 3, "c-reader", ZONE_APP, 1);
    assert!(r.deliver(
        reader,
        Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            branch: DRAFT.to_vec(),
            zone: b"za".to_vec(),
            last_seen_seq: 0,
        },
    ));
    r.take_outbox(reader);

    assert_eq!(
        touched(&diff(&mut r, author, DiffKind::Branches, DRAFT, b"main")),
        vec![Some(b"board".to_vec()), Some(b"notes".to_vec())],
        "the control: both partitions changed between the branches",
    );
    let changes = diff(&mut r, reader, DiffKind::Branches, DRAFT, b"main");
    assert_eq!(
        touched(&changes),
        vec![Some(b"board".to_vec())],
        "a channel bound to a branch narrowed by the wrong partitions: {changes:?}",
    );
    assert!(!reports_value(&changes, 4242));
}

#[test]
fn a_whole_zone_readers_diff_reports_every_partition() {
    let (mut r, mut author_doc, author, _reader) = zoned_room();
    create_version(&mut r, author, VA);
    write_into(&mut r, author, &mut author_doc, b"notes", b"nseed", 4242);
    create_version(&mut r, author, VB);

    let changes = diff(&mut r, author, DiffKind::Versions, VA, VB);
    assert_eq!(
        touched(&changes),
        vec![Some(b"notes".to_vec())],
        "a reader entitled to every zone lost a partition",
    );
    assert!(reports_value(&changes, 4242));
}

#[test]
fn a_diff_follows_the_channels_zone_scope_not_the_actors_entitlement() {
    // The scope a diff narrows by is the *channel's*, stored when it subscribed — the
    // same set a version fetch on that channel uses (C15) and the same one the live
    // fan-out filters its ops by. Re-deriving a whole-room scope at query time would
    // report a partition the subscription deliberately left out.
    let (mut r, mut author_doc, author, _reader) = zoned_room();
    create_version(&mut r, author, VA);
    write_into(&mut r, author, &mut author_doc, b"notes", b"nseed", 4242);
    create_version(&mut r, author, VB);

    // The control, on this very fixture: the author's whole-room channel does report
    // the change, so an empty answer below is the scope's doing and not the room's.
    assert!(!diff(&mut r, author, DiffKind::Versions, VA, VB).is_empty());

    // `author` reaches both zones, and subscribes a second channel to za alone.
    let narrow = Channel(1);
    subscribe_on(&mut r, author, narrow, b"za");

    let changes = diff_on(&mut r, author, narrow, DiffKind::Versions, VA, VB);
    assert!(
        changes.is_empty(),
        "the diff reported a zone this channel did not subscribe to: {changes:?}",
    );
}

// --- a doc-ACL partial reader ---

/// Room read is granted by the schema tier to every authenticated actor, so a reader
/// with no doc-ACL root grant still passes the diff gate — and a doc-ACL deny is what
/// carves a subtree back out of it.
const PARTIAL: &str = r#"{
    "schema": "p", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "a": "Sect", "b": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "auth": { "grants": [ { "allow": "read", "to": "authenticated", "on": "/" } ] }
}"#;

const PARTIAL_APP: &[u8] = b"p";

/// Which shape of read-deny alice installs on `/b` before bob joins.
#[derive(Clone, Copy, PartialEq)]
enum Deny {
    /// A fixed-path tuple on `/b`.
    Path,
    /// A stable-element tuple on the container `/b` currently holds, so the deny
    /// resolves through whichever tree it is evaluated against.
    Element,
}

/// The container id `key` holds in `doc`.
fn slot_id(doc: &Document, key: &[u8]) -> ElementId {
    match doc.get(key) {
        Some(Element::Map(m)) => {
            let id = m.borrow().id();
            id
        }
        _ => panic!("slot {key:?} is not a map"),
    }
}

/// A room alice created with `/a` and `/b` seeded, where bob reads the room through
/// the schema tier, writes through a doc-ACL root grant, and is denied read on `/b`
/// in the shape `deny` names. Bob has joined and caught up.
fn partial_room(deny: Deny) -> (Registry, Document, ConnId, ConnId) {
    let mut sr = SchemaRegistry::new();
    sr.register(PARTIAL_APP, 1, PARTIAL.as_bytes(), b"")
        .unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens(&[("t-alice", "alice"), ("t-bob", "bob")])));
    // The deployment permits alice everything and abstains on bob, so bob's verdicts
    // are the schema and doc-ACL tiers' alone.
    r.set_authorizer(Box::new(Acl::new().allow(
        Subject::Actor(b"alice".to_vec()),
        None,
        ResourceMatch::Room(ROOM.to_vec()),
    )));
    r.set_clock(Arc::new(ManualClock::new(0)));

    let alice = auth(&mut r, 1, "t-alice", PARTIAL_APP, 1);
    subscribe(&mut r, alice, b"");
    let mut alice_doc = Document::new(cid(1));
    alice_doc.set_schema(Schema::parse(PARTIAL).expect("the partial schema parses"));
    let seed = alice_doc.transact(|tx| {
        tx.map(b"a").register(b"aseed", Scalar::Int(0));
        tx.map(b"b").register(b"bseed", Scalar::Int(0));
    });
    submit(&mut r, alice, seed);
    let ops = match deny {
        Deny::Path => alice_doc.transact(|tx| {
            tx.acl().grant(
                AclSubject::Actor(actor_key(b"bob")),
                AclGrant::Capability(Capability::Read),
                AclEffect::Deny,
                encode_path(&[b"b"]),
                actor_key(b"alice"),
            );
        }),
        Deny::Element => {
            let target = slot_id(&alice_doc, b"b");
            alice_doc.transact(|tx| {
                tx.acl().grant_element(
                    AclSubject::Actor(actor_key(b"bob")),
                    AclGrant::Capability(Capability::Read),
                    AclEffect::Deny,
                    target,
                    actor_key(b"alice"),
                );
            })
        }
    };
    submit(&mut r, alice, ops);

    let bob = auth(&mut r, 2, "t-bob", PARTIAL_APP, 1);
    subscribe(&mut r, bob, b"");
    (r, alice_doc, alice, bob)
}

#[test]
fn a_partial_readers_version_diff_withholds_the_denied_subtree() {
    let (mut r, mut alice_doc, alice, bob) = partial_room(Deny::Path);
    create_version(&mut r, alice, VA);
    write_into(&mut r, alice, &mut alice_doc, b"b", b"bseed", 4242);
    create_version(&mut r, alice, VB);

    let changes = diff(&mut r, bob, DiffKind::Versions, VA, VB);
    assert!(
        changes.is_empty(),
        "the diff reported a change in a subtree the reader is denied: {changes:?}",
    );
    assert!(
        !reports_value(&changes, 4242),
        "the diff carried the denied subtree's scalar",
    );
}

#[test]
fn a_partial_readers_version_diff_still_reports_the_subtree_it_may_read() {
    let (mut r, mut alice_doc, alice, bob) = partial_room(Deny::Path);
    create_version(&mut r, alice, VA);
    write_into(&mut r, alice, &mut alice_doc, b"a", b"aseed", 7);
    write_into(&mut r, alice, &mut alice_doc, b"b", b"bseed", 4242);
    create_version(&mut r, alice, VB);

    assert_eq!(
        diff(&mut r, bob, DiffKind::Versions, VA, VB),
        vec![Change::Value {
            path: encode_path(&[b"a", b"aseed"]),
            old: Scalar::Int(0),
            new: Scalar::Int(7),
        }],
    );
}

#[test]
fn a_partial_readers_diff_is_the_diff_of_the_versions_it_is_served() {
    // The doc-ACL half of the fetch-oracle equality. The read projection is the more
    // intricate of the two — it re-opens a moved-in node, cuts leaf slots, and retains
    // ACL tuples and annotations by their own rules — so agreeing with the fetch is a
    // stronger claim here than for the zone prune.
    let (mut r, mut alice_doc, alice, bob) = partial_room(Deny::Path);
    create_version(&mut r, alice, VA);
    write_into(&mut r, alice, &mut alice_doc, b"a", b"aseed", 7);
    write_into(&mut r, alice, &mut alice_doc, b"b", b"bseed", 4242);
    create_version(&mut r, alice, VB);

    assert_eq!(
        diff(&mut r, bob, DiffKind::Versions, VA, VB),
        served_version_diff(&mut r, bob, VA, VB),
    );
}

#[test]
fn an_element_denied_readers_diff_is_the_diff_of_the_versions_it_is_served() {
    // The same equality where the deny is element-scoped, so each side's gate resolves
    // it through that side's own tree.
    let (mut r, mut alice_doc, alice, bob) = partial_room(Deny::Element);
    create_version(&mut r, alice, VA);
    write_into(&mut r, alice, &mut alice_doc, b"a", b"aseed", 7);
    write_into(&mut r, alice, &mut alice_doc, b"b", b"bseed", 4242);
    create_version(&mut r, alice, VB);

    assert_eq!(
        diff(&mut r, bob, DiffKind::Versions, VA, VB),
        served_version_diff(&mut r, bob, VA, VB),
    );
}

#[test]
fn a_partial_readers_branch_diff_withholds_the_denied_subtree() {
    let (mut r, mut alice_doc, alice, bob) = partial_room(Deny::Path);
    let fork = r.hub().seq(ROOM);
    assert!(r.hub_mut().fork_branch(ROOM, DRAFT, b"main", fork).unwrap());
    write_into(&mut r, alice, &mut alice_doc, b"b", b"bseed", 4242);

    // The positive control: the same two branches, read by the reader the deny does
    // not reach, so an empty answer above cannot come from the branches agreeing.
    let seen = diff(&mut r, alice, DiffKind::Branches, DRAFT, b"main");
    assert_eq!(touched(&seen), vec![Some(b"b".to_vec())]);
    assert!(reports_value(&seen, 4242));

    let changes = diff(&mut r, bob, DiffKind::Branches, DRAFT, b"main");
    assert!(
        changes.is_empty(),
        "the branch diff reported a change in a denied subtree: {changes:?}",
    );
}

#[test]
fn an_element_scoped_deny_resolves_against_the_diffed_tree_not_the_live_room() {
    // An element-scoped grant resolves to where the element stands in the tree it is
    // evaluated against, so a deny whose target has since left the live room is
    // inert there — and an inert deny is no deny, which is a gate that serves the
    // state whole (the shape C32 names). The two sides of a diff are two archived
    // trees, and the deny still governs both.
    let (mut r, mut alice_doc, alice, bob) = partial_room(Deny::Element);
    create_version(&mut r, alice, VA);
    write_into(&mut r, alice, &mut alice_doc, b"b", b"bseed", 4242);
    create_version(&mut r, alice, VB);
    // The denied container leaves the live room. Both versions still hold it, so a
    // gate resolving the deny against live main would find no path for it.
    let drop_b = alice_doc.transact(|tx| tx.delete(b"b"));
    submit(&mut r, alice, drop_b);
    assert!(
        room_doc(&r).get(b"b").is_none(),
        "the denied container is still in the live room",
    );

    let seen = diff(&mut r, alice, DiffKind::Versions, VA, VB);
    assert_eq!(touched(&seen), vec![Some(b"b".to_vec())]);

    let changes = diff(&mut r, bob, DiffKind::Versions, VA, VB);
    assert!(
        changes.is_empty(),
        "an element-scoped deny left the diff unnarrowed: {changes:?}",
    );
}

#[test]
fn a_whole_document_readers_diff_reports_the_denied_subtree() {
    // Alice reads the room through the deployment tier, so no doc-ACL deny carves
    // anything out of her diff — the projection runs for the partial reader alone.
    let (mut r, mut alice_doc, alice, _bob) = partial_room(Deny::Path);
    create_version(&mut r, alice, VA);
    write_into(&mut r, alice, &mut alice_doc, b"b", b"bseed", 4242);
    create_version(&mut r, alice, VB);

    let changes = diff(&mut r, alice, DiffKind::Versions, VA, VB);
    assert_eq!(touched(&changes), vec![Some(b"b".to_vec())]);
    assert!(reports_value(&changes, 4242));
}

// --- an unnarrowable room is unaffected ---

#[test]
fn a_room_no_redaction_applies_to_diffs_exactly_as_before() {
    // A room with no doc-ACL state, queried by a channel that is not zone-limited,
    // is diffed from the stored bytes: the narrowing is targeted, and its absence is
    // the common case.
    let mut r = Registry::new(cid(0xFF));
    r.set_clock(Arc::new(ManualClock::new(0)));
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"actor-1".to_vec(),
        }
    ));
    r.take_outbox(id);
    let mut caught = Document::new(cid(1));
    for op in ops_in(subscribe(&mut r, id, b"")) {
        caught.apply(&op);
    }
    let ops = caught.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    submit(&mut r, id, ops);
    create_version(&mut r, id, VA);
    let ops = caught.transact(|tx| tx.register(b"age", Scalar::Int(40)));
    submit(&mut r, id, ops);
    create_version(&mut r, id, VB);

    assert_eq!(
        diff(&mut r, id, DiffKind::Versions, VA, VB),
        vec![Change::Value {
            path: encode_path(&[b"age"]),
            old: Scalar::Int(30),
            new: Scalar::Int(40),
        }],
    );
}
