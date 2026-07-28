//! A named version is served through the same redactions the live stream is (C15).
//!
//! A version is the room's own state at an earlier sequence, so its bytes carry every
//! partition the room carried. `VersionFetch` is gated on the deployment-tier room
//! read alone, and that gate deliberately admits readers the live stream only ever
//! serves *narrowed*: a zone-limited subscriber, and a doc-ACL partial reader whose
//! room read comes from the schema tier while a doc-ACL deny carves a subtree out. So
//! a fetch must run the same two projections a catch-up snapshot runs — the read
//! projection, then the zone projection — over the version's own state, or a reader
//! that passes the room gate reads the whole room out of any named version.
//!
//! The projections scrub the causal frontier down to the recipient's own ids (C9), so
//! a fetch must thread the recipient through as well: a reader that adopts a version
//! and authors on top of it must not re-mint into ids the room's log already holds.
//!
//! Everything runs in-process through the [`Registry`] (no socket, no fs), so the
//! suite runs under Miri.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{
    AclEffect, ClientId, Document, Element, ElementId, Message, Op, OpId, Scalar, Schema,
};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::{
    Action, ConnId, Identity, ManualClock, Registry, Resource, SchemaRegistry, StaticTokens,
};

const ROOM: &[u8] = b"room-v";
const CH: Channel = Channel(0);
const V1: &[u8] = b"v1";

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

fn submit_on(r: &mut Registry, id: ConnId, channel: Channel, ops: Vec<Op>) {
    assert!(r.deliver(id, Message::Ops { channel, ops }));
    r.take_outbox(id);
}

fn submit(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    submit_on(r, id, CH, ops);
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

/// The state `id` is served for the named version on `channel` — the bytes under test.
fn fetch_version_on(r: &mut Registry, id: ConnId, channel: Channel, name: &[u8]) -> Vec<u8> {
    assert!(r.deliver(
        id,
        Message::VersionFetch {
            channel,
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

fn fetch_version(r: &mut Registry, id: ConnId, name: &[u8]) -> Vec<u8> {
    fetch_version_on(r, id, CH, name)
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

/// The room's materialized replica, decoded — the oracle for "did the write land".
fn room_doc(r: &Registry) -> Document {
    Document::decode_state(&r.hub().export_room(ROOM).expect("the room exists"))
        .expect("the room's state decodes")
}

/// The distinct replicas a served state's causal frontier names.
fn frontier_authors(state: &[u8]) -> HashSet<ClientId> {
    Document::decode_state(state)
        .expect("the served version state decodes")
        .seen()
        .map(|id| id.client)
        .collect()
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
/// conn, and the reader's doc + conn.
fn zoned_room() -> (Registry, Document, ConnId, Document, ConnId) {
    let mut sr = SchemaRegistry::new();
    sr.register(ZONE_APP, 1, ZONED.as_bytes(), b"").unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens(&[
        ("c-author", "author"),
        ("c-reader", "reader"),
        ("c-reader2", "reader"),
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
    let mut reader_doc = Document::new(cid(2));
    reader_doc.set_schema(zoned_schema());
    for op in ops_in(subscribe(&mut r, reader, b"za")) {
        reader_doc.apply(&op);
    }
    (r, author_doc, author, reader_doc, reader)
}

/// The reader's durable run: three writes of its own into the zone it may read.
fn reader_writes(r: &mut Registry, conn: ConnId, doc: &mut Document) -> Vec<OpId> {
    let mut ids = Vec::new();
    for i in 0..3 {
        let key = format!("r{i}").into_bytes();
        let ops = doc.transact(|tx| {
            tx.map(b"board").register(&key, Scalar::Int(i));
        });
        ids.extend(ops.iter().map(|op| op.id));
        submit(r, conn, ops);
    }
    ids
}

#[test]
fn a_zone_limited_readers_version_withholds_the_zone_it_may_not_read() {
    let (mut r, _author_doc, author, _reader_doc, reader) = zoned_room();
    create_version(&mut r, author, V1);

    let served = Document::decode_state(&fetch_version(&mut r, reader, V1))
        .expect("the served version state decodes");
    assert_eq!(
        nested(&served, b"board", b"bseed"),
        Some(0),
        "the version withheld a zone the reader may read",
    );
    assert!(
        served.get(b"notes").is_none(),
        "the version served a zone the reader may not read",
    );
    assert_eq!(
        nested(&served, b"loose", b"lseed"),
        Some(0),
        "the version withheld the unzoned partition",
    );
}

#[test]
fn a_whole_room_readers_version_is_served_unnarrowed() {
    // The projection is targeted, not blanket: a subscriber that reaches every zone
    // still gets the version whole.
    let (mut r, _author_doc, author, _reader_doc, _reader) = zoned_room();
    create_version(&mut r, author, V1);

    let served = Document::decode_state(&fetch_version(&mut r, author, V1))
        .expect("the served version state decodes");
    assert_eq!(nested(&served, b"board", b"bseed"), Some(0));
    assert_eq!(nested(&served, b"notes", b"nseed"), Some(0));
}

#[test]
fn a_zone_limited_readers_version_names_no_other_replicas_ids() {
    // The privacy half of the scrub: the frontier the reader is served names the
    // reader and nobody else, so it can neither count nor detect another replica's
    // ops in the partition it may not read.
    let (mut r, _author_doc, author, mut reader_doc, reader) = zoned_room();
    reader_writes(&mut r, reader, &mut reader_doc);
    create_version(&mut r, author, V1);

    assert_eq!(
        frontier_authors(&fetch_version(&mut r, reader, V1)),
        HashSet::from([cid(2)]),
        "the version's frontier names an author other than the recipient",
    );
}

#[test]
fn a_restarted_zone_limited_reader_does_not_re_mint_across_a_fetched_version() {
    let (mut r, _author_doc, author, mut reader_doc, reader) = zoned_room();
    let durable: HashSet<OpId> = reader_writes(&mut r, reader, &mut reader_doc)
        .into_iter()
        .collect();
    create_version(&mut r, author, V1);

    // The restart: the `ClientId` persisted, the replica did not. A fresh connection
    // declares the same id, rejoins, and adopts the version as its state.
    let back = auth(&mut r, 2, "c-reader2", ZONE_APP, 1);
    subscribe(&mut r, back, b"za");
    let state = fetch_version(&mut r, back, V1);
    let mut restarted =
        Document::decode_state_as(cid(2), 0, &state).expect("the served version decodes");
    assert!(
        restarted.get(b"notes").is_none(),
        "the version still withholds zb",
    );

    let fresh = restarted.transact(|tx| {
        tx.map(b"board").register(b"after", Scalar::Int(9));
    });
    for op in &fresh {
        assert!(
            !durable.contains(&op.id),
            "re-minted an id the room's log already holds",
        );
    }

    // And the write is not merely distinct — it lands.
    submit(&mut r, back, fresh);
    assert_eq!(
        nested(&room_doc(&r), b"board", b"after"),
        Some(9),
        "the post-adoption write was deduped away",
    );
}

// --- a doc-ACL partial reader ---

/// Room read is granted by the schema tier to every authenticated actor, so a reader
/// with no doc-ACL root grant still passes the version gate — and a doc-ACL deny is
/// what carves a subtree back out of it.
const PARTIAL: &str = r#"{
    "schema": "p", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "a": "Sect", "b": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "auth": { "grants": [ { "allow": "read", "to": "authenticated", "on": "/" } ] }
}"#;

const PARTIAL_APP: &[u8] = b"p";

fn alice_tuple(
    doc: &mut Document,
    capability: Capability,
    effect: AclEffect,
    path: &[u8],
) -> Vec<Op> {
    doc.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(actor_key(b"bob")),
            AclGrant::Capability(capability),
            effect,
            path.to_vec(),
            actor_key(b"alice"),
        );
    })
}

/// Which shape of read-deny alice installs on `/b` before bob joins.
#[derive(Clone, Copy, PartialEq)]
enum Deny {
    /// None yet — the room reads whole until a test installs one.
    Absent,
    /// A fixed-path tuple on `/b`.
    Path,
    /// A stable-element tuple on the container `/b` currently holds, so the deny
    /// resolves through whichever tree it is evaluated against.
    Element,
}

/// The container id `/b` holds in `doc`.
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
/// in the shape `deny` names. Bob has joined and caught up. Returns the registry,
/// alice's doc + conn, bob's doc + conn.
fn partial_room(deny: Deny) -> (Registry, Document, ConnId, Document, ConnId) {
    let mut sr = SchemaRegistry::new();
    sr.register(PARTIAL_APP, 1, PARTIAL.as_bytes(), b"")
        .unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens(&[
        ("t-alice", "alice"),
        ("t-bob", "bob"),
        ("t-bob2", "bob"),
    ])));
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
    for ops in [
        alice_doc.transact(|tx| {
            tx.map(b"a").register(b"aseed", Scalar::Int(0));
            tx.map(b"b").register(b"bseed", Scalar::Int(0));
        }),
        // Write authority is a room-level verdict (there is no per-path write gate),
        // so bob's write grant roots at `/`.
        alice_tuple(
            &mut alice_doc,
            Capability::Write,
            AclEffect::Allow,
            &encode_path(&[]),
        ),
    ] {
        submit(&mut r, alice, ops);
    }
    match deny {
        Deny::Absent => {}
        Deny::Path => {
            let ops = alice_tuple(
                &mut alice_doc,
                Capability::Read,
                AclEffect::Deny,
                &encode_path(&[b"b"]),
            );
            submit(&mut r, alice, ops);
        }
        Deny::Element => {
            let target = slot_id(&alice_doc, b"b");
            let ops = alice_doc.transact(|tx| {
                tx.acl().grant_element(
                    AclSubject::Actor(actor_key(b"bob")),
                    AclGrant::Capability(Capability::Read),
                    AclEffect::Deny,
                    target,
                    actor_key(b"alice"),
                );
            });
            submit(&mut r, alice, ops);
        }
    }

    let bob = auth(&mut r, 2, "t-bob", PARTIAL_APP, 1);
    let mut bob_doc = Document::new(cid(2));
    bob_doc.set_schema(Schema::parse(PARTIAL).expect("the partial schema parses"));
    for op in ops_in(subscribe(&mut r, bob, b"")) {
        bob_doc.apply(&op);
    }
    (r, alice_doc, alice, bob_doc, bob)
}

#[test]
fn a_partial_readers_version_withholds_the_denied_subtree() {
    let (mut r, _alice_doc, alice, _bob_doc, bob) = partial_room(Deny::Path);
    create_version(&mut r, alice, V1);

    let served = Document::decode_state(&fetch_version(&mut r, bob, V1))
        .expect("the served version state decodes");
    assert_eq!(
        nested(&served, b"a", b"aseed"),
        Some(0),
        "the version withheld the subtree the reader may read",
    );
    assert!(
        served.get(b"b").is_none(),
        "the version served a subtree the reader is denied",
    );
}

#[test]
fn a_whole_document_readers_version_is_served_unnarrowed() {
    let (mut r, _alice_doc, alice, _bob_doc, _bob) = partial_room(Deny::Path);
    create_version(&mut r, alice, V1);

    let served = Document::decode_state(&fetch_version(&mut r, alice, V1))
        .expect("the served version state decodes");
    assert_eq!(nested(&served, b"a", b"aseed"), Some(0));
    assert_eq!(nested(&served, b"b", b"bseed"), Some(0));
}

#[test]
fn a_partial_readers_version_names_no_other_replicas_ids() {
    let (mut r, _alice_doc, alice, mut bob_doc, bob) = partial_room(Deny::Path);
    let ops = bob_doc.transact(|tx| {
        tx.map(b"a").register(b"b0", Scalar::Int(0));
    });
    submit(&mut r, bob, ops);
    create_version(&mut r, alice, V1);

    assert_eq!(
        frontier_authors(&fetch_version(&mut r, bob, V1)),
        HashSet::from([cid(2)]),
        "the version's frontier names an author other than the recipient",
    );
}

#[test]
fn a_restarted_partial_reader_does_not_re_mint_across_a_fetched_version() {
    let (mut r, _alice_doc, alice, mut bob_doc, bob) = partial_room(Deny::Path);
    let mut durable: HashSet<OpId> = HashSet::new();
    for i in 0..3 {
        let key = format!("b{i}").into_bytes();
        let ops = bob_doc.transact(|tx| {
            tx.map(b"a").register(&key, Scalar::Int(i));
        });
        durable.extend(ops.iter().map(|op| op.id));
        submit(&mut r, bob, ops);
    }
    create_version(&mut r, alice, V1);

    let back = auth(&mut r, 2, "t-bob2", PARTIAL_APP, 1);
    subscribe(&mut r, back, b"");
    let state = fetch_version(&mut r, back, V1);
    let mut restarted =
        Document::decode_state_as(cid(2), 0, &state).expect("the served version decodes");
    assert!(
        restarted.get(b"b").is_none(),
        "the version still withholds /b"
    );

    let fresh = restarted.transact(|tx| {
        tx.map(b"a").register(b"after", Scalar::Int(9));
    });
    for op in &fresh {
        assert!(
            !durable.contains(&op.id),
            "re-minted an id the room's log already holds",
        );
    }

    submit(&mut r, back, fresh);
    assert_eq!(
        nested(&room_doc(&r), b"a", b"after"),
        Some(9),
        "the post-adoption write was deduped away",
    );
}

#[test]
fn an_element_scoped_deny_resolves_against_the_versions_own_tree() {
    // An element grant resolves to the element's *current* path, and "current" for a
    // version read is the version's tree. Resolving against the live room instead
    // leaves the deny inert the moment the element leaves that tree — and an inert
    // deny is not a narrower read, it is the whole version served unredacted.
    let (mut r, mut alice_doc, alice, _bob_doc, bob) = partial_room(Deny::Element);
    create_version(&mut r, alice, V1);

    // `/b` leaves the live room, so its element resolves to no live path at all.
    let ops = alice_doc.transact(|tx| tx.delete(b"b"));
    submit(&mut r, alice, ops);
    assert!(
        room_doc(&r).get(b"b").is_none(),
        "the live room still holds the denied element",
    );

    let served = Document::decode_state(&fetch_version(&mut r, bob, V1))
        .expect("the served version state decodes");
    assert_eq!(nested(&served, b"a", b"aseed"), Some(0));
    assert!(
        served.get(b"b").is_none(),
        "the version served an element the reader is denied",
    );
}

#[test]
fn the_live_acl_governs_a_version_captured_before_the_deny() {
    // Authorization is a decision about now, not about the moment of capture: the
    // version's own captured tuples would resurrect the access the deployment has
    // since taken away.
    let (mut r, mut alice_doc, alice, _bob_doc, bob) = partial_room(Deny::Absent);
    create_version(&mut r, alice, V1);
    assert_eq!(
        nested(
            &Document::decode_state(&fetch_version(&mut r, bob, V1)).expect("decodes"),
            b"b",
            b"bseed",
        ),
        Some(0),
        "the reader was narrowed before anything denied it",
    );

    let ops = alice_tuple(
        &mut alice_doc,
        Capability::Read,
        AclEffect::Deny,
        &encode_path(&[b"b"]),
    );
    submit(&mut r, alice, ops);

    let served = Document::decode_state(&fetch_version(&mut r, bob, V1))
        .expect("the served version state decodes");
    assert!(
        served.get(b"b").is_none(),
        "a version captured before the deny served the denied subtree",
    );
}

#[test]
fn a_version_fetched_on_a_second_channel_keeps_that_channels_own_ids() {
    // A channel authors under `for_channel` of the id declared at Hello, so the
    // frontier the fetch keeps has to be cut to *that* identity — the connection's own
    // id answers only for channel 0. A session's second subscription is where a
    // mistake here hides.
    let (mut r, _author_doc, author, _reader_doc, reader) = zoned_room();
    let second = Channel(1);
    subscribe_on(&mut r, reader, second, b"za");

    let authoring = cid(2).for_channel(second.0);
    assert_ne!(authoring, cid(2), "the second channel derives its identity");
    let mut second_doc = Document::new(authoring);
    second_doc.set_schema(zoned_schema());
    let ops = second_doc.transact(|tx| {
        tx.map(b"board").register(b"s0", Scalar::Int(0));
    });
    submit_on(&mut r, reader, second, ops);
    create_version(&mut r, author, V1);

    assert_eq!(
        frontier_authors(&fetch_version_on(&mut r, reader, second, V1)),
        HashSet::from([authoring]),
        "the version's frontier is not the fetching channel's own identity",
    );
}

// --- both projections at once ---

/// Zoned like `ZONED`, and readable room-wide through the schema tier — so a reader
/// can be zone-limited *and* doc-ACL-partial in the same room.
const COMPOSED: &str = r#"{
    "schema": "c", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Sect", "notes": "Sect", "loose": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "zones": { "za": "/board", "zb": "/notes" },
    "auth": { "grants": [ { "allow": "read", "to": "authenticated", "on": "/" } ] }
}"#;

const COMPOSED_APP: &[u8] = b"c";

#[test]
fn a_version_is_narrowed_by_both_projections_at_once() {
    let mut sr = SchemaRegistry::new();
    sr.register(COMPOSED_APP, 1, COMPOSED.as_bytes(), b"")
        .unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens(&[("t-alice", "alice"), ("t-bob", "bob")])));
    // alice reaches everything; bob's room read is the schema tier's, and the
    // deployment carves the `zb` partition out of it.
    r.set_authorizer(Box::new(
        Acl::new()
            .allow(
                Subject::Actor(b"alice".to_vec()),
                None,
                ResourceMatch::Room(ROOM.to_vec()),
            )
            .deny(
                Subject::Actor(b"bob".to_vec()),
                Some(Action::Read),
                ResourceMatch::Zone {
                    room: ROOM.to_vec(),
                    zone: b"zb".to_vec(),
                },
            ),
    ));
    r.set_clock(Arc::new(ManualClock::new(0)));

    let alice = auth(&mut r, 1, "t-alice", COMPOSED_APP, 1);
    subscribe(&mut r, alice, b"");
    let mut alice_doc = Document::new(cid(1));
    alice_doc.set_schema(Schema::parse(COMPOSED).expect("the composed schema parses"));
    for ops in [
        alice_doc.transact(|tx| {
            tx.map(b"board").register(b"bseed", Scalar::Int(0));
            tx.map(b"notes").register(b"nseed", Scalar::Int(0));
            tx.map(b"loose").register(b"lseed", Scalar::Int(0));
        }),
        // A doc-ACL deny on the unzoned subtree, so the read projection bites on a
        // partition the zone projection does not touch.
        alice_tuple(
            &mut alice_doc,
            Capability::Read,
            AclEffect::Deny,
            &encode_path(&[b"loose"]),
        ),
    ] {
        submit(&mut r, alice, ops);
    }

    let bob = auth(&mut r, 2, "t-bob", COMPOSED_APP, 1);
    subscribe(&mut r, bob, b"");
    create_version(&mut r, alice, V1);

    let served = Document::decode_state(&fetch_version(&mut r, bob, V1))
        .expect("the served version state decodes");
    assert_eq!(
        nested(&served, b"board", b"bseed"),
        Some(0),
        "the version withheld the partition both tiers admit",
    );
    assert!(
        served.get(b"notes").is_none(),
        "the version served a zone the reader may not read",
    );
    assert!(
        served.get(b"loose").is_none(),
        "the version served a subtree the reader is denied",
    );
}
