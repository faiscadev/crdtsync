//! Doc-level ACL — outbound per-recipient op redaction on fan-out and catch-up
//! (slice 4b).
//!
//! The inbound half (4a) gates the op-submit Write path on the actor-keyed doc-ACL
//! tier. This is the outbound half: when a committed op fans out to a room's
//! subscribers, each recipient receives only the ops in subtrees its actor may
//! *read*. A write to a subtree one recipient can read but another cannot reaches
//! the first and is withheld from the second — per-recipient, from the same batch.
//! The room creator (owns `/`) reads everything; a room with no doc-ACL tuples fans
//! out byte-for-byte as before.
//!
//! The deployment authorizer here abstains on Read (delegating per-path reads to
//! doc-ACL) and permits `alice`'s bootstrap write, so every read verdict is the
//! doc-ACL tier's and the per-path redaction actually bites (a deployment Read allow
//! would short-circuit it). A fixed clock keeps the suite Miri-clean, and the whole
//! path is in-process (no socket / fs), so it runs under Miri.

use std::sync::Arc;

use crdtsync_core::acl::{AclGrant, AclScope, AclSubject, Capability};
use crdtsync_core::path::{encode_path, xml_fragment, xml_insert_element};
use crdtsync_core::protocol::Channel;
use crdtsync_core::{
    AclEffect, ClientId, Document, ElementId, ElementKind, ErrorCode, Message, Op, OpKind,
    RangeAnchor, Scalar, Side,
};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::{Action, ConnId, ManualClock, Registry, StaticTokens};

const ROOM: &[u8] = b"room-a";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A registry whose deployment permits `alice` (the creator) to read + write — she
/// must subscribe to bootstrap the room — but **abstains on every other actor's
/// read**, so bob's and carol's read verdicts are the doc-ACL tier's alone and the
/// per-path redaction bites (a deployment Read allow would short-circuit it). A fixed
/// clock keeps it Miri-clean.
fn registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_verifier(Box::new(tokens(&[
        ("t-alice", "alice"),
        ("t-alice2", "alice"),
        ("t-bob", "bob"),
        ("t-bob2", "bob"),
        ("t-carol", "carol"),
        ("t-dave", "dave"),
    ])));
    r.set_authorizer(Box::new(
        Acl::new()
            .allow(
                Subject::Actor(b"alice".to_vec()),
                Some(Action::Read),
                ResourceMatch::Room(ROOM.to_vec()),
            )
            .allow(
                Subject::Actor(b"alice".to_vec()),
                Some(Action::Write),
                ResourceMatch::Room(ROOM.to_vec()),
            ),
    ));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

fn tokens(rows: &[(&str, &str)]) -> StaticTokens {
    let mut t = StaticTokens::new();
    for (credential, actor) in rows {
        t.insert(credential.as_bytes().to_vec(), actor.as_bytes().to_vec());
    }
    t
}

/// Hello + Auth a relay connection as `credential`, without subscribing.
fn auth(r: &mut Registry, client: u8, credential: &str) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: Vec::new(),
            schema_version: 0,
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

fn subscribe(r: &mut Registry, id: ConnId) -> bool {
    r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            zone: Vec::new(),
            last_seen_seq: 0,
            branch: Vec::new(),
        },
    )
}

/// Hello + Auth + Subscribe, discarding the catch-up reply.
fn join(r: &mut Registry, client: u8, credential: &str) -> ConnId {
    let id = auth(r, client, credential);
    assert!(subscribe(r, id), "{credential} subscribes");
    r.take_outbox(id);
    id
}

/// A write into the top-level subtree `key` — a nested map holding one register, so
/// the batch is a `MapCreate` at `/key` plus a `RegisterSet` at `/key/v`, both
/// governed by a read grant on `/key`.
fn write_subtree(doc: &mut Document, key: &[u8], v: i64) -> Vec<Op> {
    doc.transact(|tx| {
        tx.map(key).register(b"v", Scalar::Int(v));
    })
}

/// alice grants `subject` `capability` with `effect` at `path`, authored by alice
/// (the creator).
fn grant_cap(
    doc: &mut Document,
    subject: AclSubject,
    capability: Capability,
    effect: AclEffect,
    path: &[u8],
) -> Vec<Op> {
    doc.transact(|tx| {
        tx.acl().grant(
            subject,
            AclGrant::Capability(capability),
            effect,
            path.to_vec(),
            actor_key(b"alice"),
        );
    })
}

/// alice grants `subject` read at `path`, authored by alice (the creator).
fn grant_read(doc: &mut Document, subject: AclSubject, path: &[u8]) -> Vec<Op> {
    grant_cap(doc, subject, Capability::Read, AclEffect::Allow, path)
}

/// alice grants `subject` read at `path`; returns the emitted ops and the tuple's id
/// (the handle a revoke names).
fn grant_read_id(doc: &mut Document, subject: AclSubject, path: &[u8]) -> (Vec<Op>, ElementId) {
    let mut id = None;
    let ops = doc.transact(|tx| {
        id = Some(tx.acl().grant(
            subject,
            AclGrant::Capability(Capability::Read),
            AclEffect::Allow,
            path.to_vec(),
            actor_key(b"alice"),
        ));
    });
    (ops, id.expect("a grant emits a tuple id"))
}

/// alice revokes the ACL tuple `id`.
fn revoke(doc: &mut Document, id: ElementId) -> Vec<Op> {
    doc.transact(|tx| {
        tx.acl().revoke(id);
    })
}

/// The governing paths of the `AclGrant` ops in `ops`, in order.
fn acl_grant_paths(ops: &[Op]) -> Vec<Vec<u8>> {
    ops.iter()
        .filter_map(|op| match &op.kind {
            OpKind::AclGrant {
                scope: AclScope::Path(p),
                ..
            } => Some(p.clone()),
            _ => None,
        })
        .collect()
}

/// Whether `ops` carries an `AclRevoke`.
fn has_revoke(ops: &[Op]) -> bool {
    ops.iter()
        .any(|op| matches!(op.kind, OpKind::AclRevoke { .. }))
}

/// The governing paths of a decoded snapshot's live ACL tuples, sorted.
fn acl_tuple_paths(d: &Document) -> Vec<Vec<u8>> {
    let mut p: Vec<Vec<u8>> = d
        .acl_tuples()
        .into_iter()
        .filter_map(|t| match t.scope {
            AclScope::Path(p) => Some(p),
            AclScope::Element(_) => None,
        })
        .collect();
    p.sort();
    p
}

fn submit(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: Channel(0),
            ops
        }
    ));
}

/// The ops in a connection's outbox, flattened across any `Ops` messages.
fn received_ops(r: &mut Registry, id: ConnId) -> Vec<Op> {
    r.take_outbox(id)
        .into_iter()
        .flat_map(|m| match m {
            Message::Ops { ops, .. } => ops,
            _ => Vec::new(),
        })
        .collect()
}

/// The derived map id of a top-level subtree — parent is the (client-independent)
/// document root, kind is Map.
fn subtree_id(key: &[u8]) -> ElementId {
    ElementId::derive(Document::new(cid(0)).root_id(), key, ElementKind::Map)
}

/// Whether `ops` mutate the top-level subtree `key` — a `MapCreate` of it, or an op
/// targeting its derived map id.
fn touches_subtree(ops: &[Op], key: &[u8]) -> bool {
    let map_id = subtree_id(key);
    ops.iter().any(|op| match &op.kind {
        OpKind::MapCreate { key: k } => k == key,
        _ => op.target == map_id,
    })
}

/// A registry seeded so alice (the creator) has bootstrapped the room and granted
/// bob read on `/a` and carol read on `/b`. Returns it plus alice's authoring doc and
/// connection.
fn seeded() -> (Registry, Document, ConnId) {
    let mut r = registry();
    let alice = auth(&mut r, 1, "t-alice");
    assert!(subscribe(&mut r, alice));
    r.take_outbox(alice);
    let mut alice_doc = Document::new(cid(1));

    // alice writes first (deployment permits it), establishing the room and becoming
    // its creator — the doc-ACL authority root that owns `/`.
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"seed", 0));
    submit(
        &mut r,
        alice,
        grant_read(
            &mut alice_doc,
            AclSubject::Actor(actor_key(b"bob")),
            &encode_path(&[b"a"]),
        ),
    );
    submit(
        &mut r,
        alice,
        grant_read(
            &mut alice_doc,
            AclSubject::Actor(actor_key(b"carol")),
            &encode_path(&[b"b"]),
        ),
    );
    r.take_outbox(alice);
    (r, alice_doc, alice)
}

#[test]
fn a_write_reaches_only_recipients_who_may_read_its_subtree() {
    let (mut r, mut alice_doc, alice) = seeded();
    let bob = join(&mut r, 2, "t-bob"); // reads /a
    let carol = join(&mut r, 3, "t-carol"); // reads /b

    // A write into /a reaches bob (reads /a) and is withheld from carol (reads /b).
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"a", 1));
    let bob_a = received_ops(&mut r, bob);
    let carol_a = received_ops(&mut r, carol);
    assert!(
        touches_subtree(&bob_a, b"a"),
        "bob reads /a, so a /a write reaches him"
    );
    assert!(
        carol_a.is_empty(),
        "carol cannot read /a, so the /a write is withheld from her",
    );

    // A write into /b reaches carol and is withheld from bob — same batch machinery,
    // opposite recipients (per-recipient redaction).
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"b", 2));
    let bob_b = received_ops(&mut r, bob);
    let carol_b = received_ops(&mut r, carol);
    assert!(
        touches_subtree(&carol_b, b"b"),
        "carol reads /b, so a /b write reaches her"
    );
    assert!(
        bob_b.is_empty(),
        "bob cannot read /b, so the /b write is withheld from him",
    );
}

#[test]
fn the_creator_receives_every_op() {
    let (mut r, mut alice_doc, alice) = seeded();
    // A second device of alice — the creator's actor — subscribes as a recipient.
    let alice2 = join(&mut r, 10, "t-alice2");

    submit(&mut r, alice, write_subtree(&mut alice_doc, b"a", 1));
    let got_a = received_ops(&mut r, alice2);
    assert!(touches_subtree(&got_a, b"a"), "the creator reads /a");

    submit(&mut r, alice, write_subtree(&mut alice_doc, b"b", 2));
    let got_b = received_ops(&mut r, alice2);
    assert!(
        touches_subtree(&got_b, b"b"),
        "the creator owns /, so it receives /b too",
    );
}

#[test]
fn a_room_with_no_acl_tuples_fans_out_to_everyone_unchanged() {
    // Regression: with no doc-ACL tuples the fan-out is the pre-4b path — every
    // subscriber the deployment admits receives every op, byte-identical.
    let mut r = Registry::new(cid(0xFF));
    r.set_verifier(Box::new(tokens(&[
        ("t-alice", "alice"),
        ("t-bob", "bob"),
        ("t-carol", "carol"),
    ])));
    // Deployment permits read + write for everyone; no doc-ACL grants are ever made.
    r.set_authorizer(Box::new(
        Acl::new()
            .allow(
                Subject::Anyone,
                Some(Action::Read),
                ResourceMatch::Room(ROOM.to_vec()),
            )
            .allow(
                Subject::Anyone,
                Some(Action::Write),
                ResourceMatch::Room(ROOM.to_vec()),
            ),
    ));
    r.set_clock(Arc::new(ManualClock::new(0)));

    let alice = join(&mut r, 1, "t-alice");
    let bob = join(&mut r, 2, "t-bob");
    let carol = join(&mut r, 3, "t-carol");
    let mut alice_doc = Document::new(cid(1));

    submit(&mut r, alice, write_subtree(&mut alice_doc, b"a", 1));
    let bob_ops = received_ops(&mut r, bob);
    let carol_ops = received_ops(&mut r, carol);
    assert!(
        touches_subtree(&bob_ops, b"a"),
        "no ACL ⇒ bob receives the write"
    );
    assert_eq!(
        bob_ops, carol_ops,
        "no ACL ⇒ every subscriber receives the identical batch",
    );
}

#[test]
fn catch_up_replays_only_the_subtrees_a_fresh_reader_may_read() {
    let (mut r, mut alice_doc, alice) = seeded();
    // Writes land before bob joins, so they reach him only via catch-up replay.
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"a", 1));
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"b", 2));
    r.take_outbox(alice);

    // bob (reads /a only) subscribes fresh from seq 0 — his catch-up replays the /a
    // write but neither the /b write, the /seed bootstrap, nor the ACL grants.
    let bob = auth(&mut r, 2, "t-bob");
    assert!(subscribe(&mut r, bob), "bob subscribes on his read of /a");
    let replay = received_ops(&mut r, bob);
    assert!(
        touches_subtree(&replay, b"a"),
        "the /a write is replayed to bob"
    );
    assert!(
        !touches_subtree(&replay, b"b"),
        "the /b write is withheld from bob's replay"
    );
    assert!(
        !touches_subtree(&replay, b"seed"),
        "the /seed bootstrap bob cannot read is withheld from his replay",
    );
    // ACL state is redacted by governing path: bob's replay carries the grant governing
    // /a (his own read, which he may read) but not the one governing /b (carol's).
    let grant_paths = acl_grant_paths(&replay);
    assert!(
        grant_paths.contains(&encode_path(&[b"a"])),
        "the ACL grant governing /a bob reads is replayed to him",
    );
    assert!(
        !grant_paths.contains(&encode_path(&[b"b"])),
        "the ACL grant governing /b bob cannot read is withheld from his replay",
    );
}

#[test]
fn an_acl_grant_reaches_only_recipients_who_may_read_its_governing_path() {
    let (mut r, mut alice_doc, alice) = seeded();
    let bob = join(&mut r, 2, "t-bob"); // reads /a
    let carol = join(&mut r, 3, "t-carol"); // reads /b

    // A grant governing /a reaches bob (reads /a) and is withheld from carol (reads /b)
    // — the ACL tuple itself is privacy-sensitive, so it rides its governing path.
    submit(
        &mut r,
        alice,
        grant_read(
            &mut alice_doc,
            AclSubject::Actor(actor_key(b"dave")),
            &encode_path(&[b"a"]),
        ),
    );
    assert_eq!(
        acl_grant_paths(&received_ops(&mut r, bob)),
        vec![encode_path(&[b"a"])],
        "bob receives the ACL grant governing /a he may read",
    );
    assert!(
        received_ops(&mut r, carol).is_empty(),
        "carol is withheld the /a grant she cannot read",
    );

    // A grant governing /b reaches carol and is withheld from bob — opposite recipients.
    submit(
        &mut r,
        alice,
        grant_read(
            &mut alice_doc,
            AclSubject::Actor(actor_key(b"dave")),
            &encode_path(&[b"b"]),
        ),
    );
    assert_eq!(
        acl_grant_paths(&received_ops(&mut r, carol)),
        vec![encode_path(&[b"b"])],
        "carol receives the ACL grant governing /b she may read",
    );
    assert!(
        received_ops(&mut r, bob).is_empty(),
        "bob is withheld the /b grant he cannot read",
    );
}

#[test]
fn an_acl_revoke_reaches_only_recipients_who_may_read_the_revoked_grants_path() {
    let (mut r, mut alice_doc, alice) = seeded();
    // Grant dave read on /a and on /b before the readers join.
    let (grant_a, id_a) = grant_read_id(
        &mut alice_doc,
        AclSubject::Actor(actor_key(b"dave")),
        &encode_path(&[b"a"]),
    );
    let (grant_b, id_b) = grant_read_id(
        &mut alice_doc,
        AclSubject::Actor(actor_key(b"dave")),
        &encode_path(&[b"b"]),
    );
    submit(&mut r, alice, grant_a);
    submit(&mut r, alice, grant_b);
    let bob = join(&mut r, 2, "t-bob"); // reads /a
    let carol = join(&mut r, 3, "t-carol"); // reads /b

    // Revoke the /a grant → resolved through the server's full tuple set to /a, so it
    // reaches bob (reads /a) and is withheld from carol.
    submit(&mut r, alice, revoke(&mut alice_doc, id_a));
    assert!(
        has_revoke(&received_ops(&mut r, bob)),
        "bob receives the revoke of the /a grant he may read",
    );
    assert!(
        received_ops(&mut r, carol).is_empty(),
        "carol is withheld the /a revoke",
    );

    // Revoke the /b grant → reaches carol, withheld from bob.
    submit(&mut r, alice, revoke(&mut alice_doc, id_b));
    assert!(
        has_revoke(&received_ops(&mut r, carol)),
        "carol receives the revoke of the /b grant she may read",
    );
    assert!(
        received_ops(&mut r, bob).is_empty(),
        "bob is withheld the /b revoke",
    );
}

#[test]
fn the_creator_receives_every_acl_op() {
    let (mut r, mut alice_doc, alice) = seeded();
    // A second device of alice — the creator's actor, who owns `/`.
    let alice2 = join(&mut r, 10, "t-alice2");

    submit(
        &mut r,
        alice,
        grant_read(
            &mut alice_doc,
            AclSubject::Actor(actor_key(b"dave")),
            &encode_path(&[b"a"]),
        ),
    );
    submit(
        &mut r,
        alice,
        grant_read(
            &mut alice_doc,
            AclSubject::Actor(actor_key(b"dave")),
            &encode_path(&[b"b"]),
        ),
    );
    let paths = acl_grant_paths(&received_ops(&mut r, alice2));
    assert!(
        paths.contains(&encode_path(&[b"a"])) && paths.contains(&encode_path(&[b"b"])),
        "the creator owns / and receives every ACL grant, unredacted",
    );
}

#[test]
fn the_same_actor_on_two_devices_is_redacted_identically() {
    let (mut r, mut alice_doc, alice) = seeded();
    // bob signs in from two devices — distinct client ids, distinct credentials, the
    // same actor. Both hold read on /a and neither on /b, so both are redacted alike.
    let bob1 = join(&mut r, 2, "t-bob");
    let bob2 = join(&mut r, 20, "t-bob2");

    submit(&mut r, alice, write_subtree(&mut alice_doc, b"a", 1));
    let a1 = received_ops(&mut r, bob1);
    let a2 = received_ops(&mut r, bob2);
    assert!(
        touches_subtree(&a1, b"a") && a1 == a2,
        "both bob devices receive the /a write"
    );

    submit(&mut r, alice, write_subtree(&mut alice_doc, b"b", 2));
    let b1 = received_ops(&mut r, bob1);
    let b2 = received_ops(&mut r, bob2);
    assert!(
        b1.is_empty() && b2.is_empty(),
        "both bob devices are withheld the /b write — one actor, one redaction",
    );
}

#[test]
fn a_nested_xml_write_reaches_a_subtree_reader() {
    let (mut r, mut alice_doc, alice) = seeded();
    let bob = join(&mut r, 2, "t-bob"); // reads /a
    let carol = join(&mut r, 3, "t-carol"); // reads /b

    // An XML fragment at /a, then a child element inserted into it. The insert
    // targets the fragment's children list — a derived id, not a map key — which the
    // recursive element index resolves to /a, so it reaches bob (reads /a) and is
    // withheld from carol (reads /b). Were the id unresolved and root-fallen-back,
    // neither would receive it (neither reads the root).
    submit(
        &mut r,
        alice,
        xml_fragment(&mut alice_doc, &encode_path(&[b"a"])),
    );
    r.take_outbox(bob);
    r.take_outbox(carol);
    submit(
        &mut r,
        alice,
        xml_insert_element(&mut alice_doc, &encode_path(&[b"a"]), 0, b"p"),
    );
    assert!(
        !received_ops(&mut r, bob).is_empty(),
        "the xml child insert under /a reaches bob, resolved through the container walk",
    );
    assert!(
        received_ops(&mut r, carol).is_empty(),
        "the xml child insert is withheld from carol, who cannot read /a",
    );
}

#[test]
fn a_deployment_read_deny_is_not_reopened_by_a_subtree_grant() {
    // Deployment explicitly DENIES bob read; alice (creator) grants him read on /a.
    // The deployment deny is terminal, so bob is refused subscribe — a doc-ACL subtree
    // grant never re-opens what the deployment refused.
    let mut r = Registry::new(cid(0xFF));
    r.set_verifier(Box::new(tokens(&[("t-alice", "alice"), ("t-bob", "bob")])));
    r.set_authorizer(Box::new(
        Acl::new()
            .allow(
                Subject::Actor(b"alice".to_vec()),
                Some(Action::Read),
                ResourceMatch::Room(ROOM.to_vec()),
            )
            .allow(
                Subject::Actor(b"alice".to_vec()),
                Some(Action::Write),
                ResourceMatch::Room(ROOM.to_vec()),
            )
            .deny(
                Subject::Actor(b"bob".to_vec()),
                Some(Action::Read),
                ResourceMatch::Room(ROOM.to_vec()),
            ),
    ));
    r.set_clock(Arc::new(ManualClock::new(0)));

    let alice = auth(&mut r, 1, "t-alice");
    assert!(subscribe(&mut r, alice));
    r.take_outbox(alice);
    let mut alice_doc = Document::new(cid(1));
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"seed", 0));
    submit(
        &mut r,
        alice,
        grant_read(
            &mut alice_doc,
            AclSubject::Actor(actor_key(b"bob")),
            &encode_path(&[b"a"]),
        ),
    );
    r.take_outbox(alice);

    let bob = auth(&mut r, 2, "t-bob");
    assert!(subscribe(&mut r, bob)); // forbidden is non-closing, so deliver is true
    assert!(
        r.take_outbox(bob).into_iter().any(|m| matches!(
            m,
            Message::Error {
                code: ErrorCode::Forbidden,
                ..
            }
        )),
        "a deployment read-deny is terminal — the subtree grant does not re-open it",
    );
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"a", 1));
    assert!(
        received_ops(&mut r, bob).is_empty(),
        "a deny-refused bob never subscribed, so he receives no fan-out",
    );
}

/// The `Snapshot` state in a connection's outbox, decoded — or `None` if it was not
/// served one.
fn served_snapshot(r: &mut Registry, id: ConnId) -> Option<Document> {
    r.take_outbox(id).into_iter().find_map(|m| match m {
        Message::Snapshot { state, .. } => {
            Some(Document::decode_state(&state).expect("a served snapshot decodes"))
        }
        _ => None,
    })
}

/// Whether a decoded doc holds the top-level subtree `key` as a live map.
fn has_subtree(d: &Document, key: &[u8]) -> bool {
    matches!(d.get(key), Some(crdtsync_core::Element::Map(_)))
}

/// Whether a decoded doc's map at `container` holds a live slot `key`.
fn leaf_present(d: &Document, container: &[u8], key: &[u8]) -> bool {
    match d.get(container) {
        Some(crdtsync_core::Element::Map(m)) => m.borrow().get(key).is_some(),
        _ => false,
    }
}

#[test]
fn a_partial_reader_is_served_a_projected_snapshot_catch_up() {
    let (mut r, mut alice_doc, alice) = seeded();
    // Compact the room so a fresh subscriber below the floor catches up via a
    // Snapshot (the whole materialized replica), not an op delta.
    r.set_compaction_threshold(1);
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"a", 1));
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"b", 2));
    r.take_outbox(alice);

    // bob (reads /a only) subscribes fresh. The snapshot is now PROJECTED to his
    // authorized subtree rather than refused: he is served /a with /b and the /seed
    // bootstrap he cannot read dropped.
    let bob = auth(&mut r, 2, "t-bob");
    assert!(subscribe(&mut r, bob));
    let snap =
        served_snapshot(&mut r, bob).expect("a partial reader is served a projected snapshot");
    assert!(has_subtree(&snap, b"a"), "the /a subtree is served");
    assert!(!has_subtree(&snap, b"b"), "the /b subtree is dropped");
    assert!(
        !has_subtree(&snap, b"seed"),
        "the /seed bootstrap bob cannot read is dropped",
    );

    // A whole-document reader — a second alice device, the creator who owns `/` — is
    // served the FULL snapshot, /a and /b both present.
    let alice2 = auth(&mut r, 10, "t-alice2");
    assert!(subscribe(&mut r, alice2));
    let full = served_snapshot(&mut r, alice2).expect("the creator is served the snapshot");
    assert!(
        has_subtree(&full, b"a") && has_subtree(&full, b"b"),
        "the whole-document reader gets the unprojected snapshot",
    );
}

#[test]
fn a_snapshot_joiner_converges_with_an_op_joiner_for_a_partial_reader() {
    // The load-bearing convergence property: the op-stream redaction withholds every
    // op on a subtree bob may not read, so a bob who joins live never materializes
    // those subtrees. Projecting them out of the snapshot must drop EXACTLY the same
    // subtrees, so a snapshot-served bob converges with an op-served bob.

    // op-join: an uncompacted room, bob catches up via the redacted op stream.
    let ops_replica = {
        let (mut r, mut alice_doc, alice) = seeded();
        submit(&mut r, alice, write_subtree(&mut alice_doc, b"a", 1));
        submit(&mut r, alice, write_subtree(&mut alice_doc, b"b", 2));
        r.take_outbox(alice);
        let bob = auth(&mut r, 2, "t-bob");
        assert!(subscribe(&mut r, bob));
        let mut replica = Document::new(cid(2));
        for op in received_ops(&mut r, bob) {
            replica.apply(&op);
        }
        replica
    };

    // snapshot-join: the same history compacted, bob catches up via a projected snapshot.
    let snap_replica = {
        let (mut r, mut alice_doc, alice) = seeded();
        r.set_compaction_threshold(1);
        submit(&mut r, alice, write_subtree(&mut alice_doc, b"a", 1));
        submit(&mut r, alice, write_subtree(&mut alice_doc, b"b", 2));
        r.take_outbox(alice);
        let bob = auth(&mut r, 2, "t-bob");
        assert!(subscribe(&mut r, bob));
        served_snapshot(&mut r, bob).expect("a partial reader is served a projected snapshot")
    };

    // Both joiners hold /a and neither holds /b or the /seed bootstrap — they converge.
    // The ACL set converges too: both materialize the tuple governing /a (bob's own read
    // grant) and neither the one governing /b — the same subset either seam yields.
    for (label, replica) in [("op-join", &ops_replica), ("snapshot-join", &snap_replica)] {
        assert!(has_subtree(replica, b"a"), "{label} bob has /a");
        assert!(!has_subtree(replica, b"b"), "{label} bob lacks /b");
        assert!(!has_subtree(replica, b"seed"), "{label} bob lacks /seed");
        assert_eq!(
            acl_tuple_paths(replica),
            vec![encode_path(&[b"a"])],
            "{label} bob materializes only the /a ACL tuple",
        );
    }
}

#[test]
fn a_downstream_read_deny_carve_out_is_served_a_projected_snapshot() {
    // bob is granted read on the whole document (`/`) then denied read on `/secret`
    // — the AWS-style downstream carve-out. He reads root (Allow, since a descendant
    // deny does not govern the root query), so he subscribes; the snapshot is
    // projected to drop `/secret` — exactly the subtree the live fan-out withholds —
    // and serve the rest, instead of refusing the whole catch-up.
    let mut r = registry();
    let alice = auth(&mut r, 1, "t-alice");
    assert!(subscribe(&mut r, alice));
    r.take_outbox(alice);
    let mut alice_doc = Document::new(cid(1));
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"secret", 0));
    submit(
        &mut r,
        alice,
        grant_read(
            &mut alice_doc,
            AclSubject::Actor(actor_key(b"bob")),
            &encode_path(&[]),
        ),
    );
    submit(
        &mut r,
        alice,
        grant_cap(
            &mut alice_doc,
            AclSubject::Actor(actor_key(b"bob")),
            Capability::Read,
            AclEffect::Deny,
            &encode_path(&[b"secret"]),
        ),
    );
    r.take_outbox(alice);

    // Compact so a fresh subscriber catches up via a Snapshot.
    r.set_compaction_threshold(1);
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"pub", 1));
    r.take_outbox(alice);

    let bob = auth(&mut r, 2, "t-bob");
    assert!(
        subscribe(&mut r, bob),
        "bob's root read admits him to subscribe"
    );
    let snap =
        served_snapshot(&mut r, bob).expect("the carved-out reader is served a projected snapshot");
    assert!(has_subtree(&snap, b"pub"), "the readable subtree is served",);
    assert!(
        !has_subtree(&snap, b"secret"),
        "the /secret carve-out is dropped from the projected snapshot",
    );
}

#[test]
fn a_room_with_no_acl_tuples_serves_the_full_snapshot() {
    // Regression: with no doc-ACL tuples the snapshot path is the pre-projection one —
    // every subscriber the deployment admits is served the whole materialized replica,
    // byte-identical to before.
    let mut r = Registry::new(cid(0xFF));
    r.set_verifier(Box::new(tokens(&[("t-alice", "alice"), ("t-bob", "bob")])));
    r.set_authorizer(Box::new(
        Acl::new()
            .allow(
                Subject::Anyone,
                Some(Action::Read),
                ResourceMatch::Room(ROOM.to_vec()),
            )
            .allow(
                Subject::Anyone,
                Some(Action::Write),
                ResourceMatch::Room(ROOM.to_vec()),
            ),
    ));
    r.set_clock(Arc::new(ManualClock::new(0)));

    let alice = join(&mut r, 1, "t-alice");
    let mut alice_doc = Document::new(cid(1));
    r.set_compaction_threshold(1);
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"a", 1));
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"b", 2));
    r.take_outbox(alice);

    let bob = auth(&mut r, 2, "t-bob");
    assert!(subscribe(&mut r, bob));
    let snap = served_snapshot(&mut r, bob).expect("a no-ACL room serves the snapshot");
    assert!(
        has_subtree(&snap, b"a") && has_subtree(&snap, b"b"),
        "no ACL ⇒ the whole materialized replica is served",
    );
}

#[test]
fn a_leaf_level_deny_is_projected_out_of_the_snapshot() {
    // bob reads /a but is denied the leaf /a/v. The op fan-out withholds the RegisterSet
    // at /a/v while delivering the /a MapCreate; the snapshot projection gates each
    // element on recipient_reads_path at the same path op_read_path resolves to, so it
    // serves /a as a map with the /a/v slot cut. A snapshot joiner converges with an op
    // joiner: both hold the /a map, neither holds /a/v.
    let build = |compact: bool| -> Document {
        let mut r = registry();
        let alice = auth(&mut r, 1, "t-alice");
        assert!(subscribe(&mut r, alice));
        r.take_outbox(alice);
        let mut alice_doc = Document::new(cid(1));
        submit(&mut r, alice, write_subtree(&mut alice_doc, b"seed", 0));
        submit(
            &mut r,
            alice,
            grant_read(
                &mut alice_doc,
                AclSubject::Actor(actor_key(b"bob")),
                &encode_path(&[b"a"]),
            ),
        );
        submit(
            &mut r,
            alice,
            grant_cap(
                &mut alice_doc,
                AclSubject::Actor(actor_key(b"bob")),
                Capability::Read,
                AclEffect::Deny,
                &encode_path(&[b"a", b"v"]),
            ),
        );
        r.take_outbox(alice);
        if compact {
            r.set_compaction_threshold(1);
        }
        // A MapCreate /a plus a RegisterSet /a/v.
        submit(&mut r, alice, write_subtree(&mut alice_doc, b"a", 1));
        r.take_outbox(alice);

        let bob = auth(&mut r, 2, "t-bob");
        assert!(subscribe(&mut r, bob));
        if compact {
            served_snapshot(&mut r, bob).expect("a projected snapshot is served")
        } else {
            let mut replica = Document::new(cid(2));
            for op in received_ops(&mut r, bob) {
                replica.apply(&op);
            }
            replica
        }
    };

    for (label, replica) in [("snapshot", build(true)), ("op-stream", build(false))] {
        assert!(
            has_subtree(&replica, b"a"),
            "{label} joiner holds the /a map"
        );
        assert!(
            !leaf_present(&replica, b"a", b"v"),
            "{label} joiner is denied the /a/v leaf",
        );
    }
}

/// alice creates a top-level List `key` (two items) in her doc and submits it — the
/// sequence a range anchors into.
fn seed_list(r: &mut Registry, alice: ConnId, doc: &mut Document, key: &[u8]) {
    submit(
        r,
        alice,
        crdtsync_core::path::list_insert(doc, &encode_path(&[key]), 0, b"x"),
    );
    submit(
        r,
        alice,
        crdtsync_core::path::list_insert(doc, &encode_path(&[key]), 1, b"y"),
    );
}

/// alice marks the span `[0, 1)` of the sequence at top-level `key`, submitting the
/// create. Returns the mark's RangedElement id bytes.
fn seed_mark(r: &mut Registry, alice: ConnId, doc: &mut Document, key: &[u8]) -> Vec<u8> {
    let (ops, id) = crdtsync_core::path::mark(
        doc,
        &encode_path(&[key]),
        0,
        Side::Left,
        1,
        Side::Right,
        b"bold",
        Scalar::Bool(true),
    );
    let id = id.expect("a mark over a live sequence emits an id");
    submit(r, alice, ops);
    id
}

/// A range endpoint at `index` in the top-level List `key`, gravity left.
fn seq_anchor(doc: &Document, key: &[u8], index: usize) -> RangeAnchor {
    let seq = match doc.get(key) {
        Some(crdtsync_core::Element::List(l)) => l.borrow().id(),
        _ => panic!("expected a live list at {key:?}"),
    };
    RangeAnchor {
        seq,
        pos: crdtsync_core::path::relative_position(doc, &encode_path(&[key]), index, Side::Left)
            .expect("a live sequence yields a position"),
    }
}

/// alice creates a cross-element range from the sequence at /`start_key` to the one at
/// /`end_key`, submitting it.
fn seed_cross_range(
    r: &mut Registry,
    alice: ConnId,
    doc: &mut Document,
    start_key: &[u8],
    end_key: &[u8],
) {
    let start = seq_anchor(doc, start_key, 0);
    let end = seq_anchor(doc, end_key, 1);
    let ops = doc.transact(|tx| {
        tx.ranged().create(start, end, Scalar::Bool(true));
    });
    submit(r, alice, ops);
}

fn has_ranged_create(ops: &[Op]) -> bool {
    ops.iter()
        .any(|o| matches!(o.kind, OpKind::RangedCreate { .. }))
}

fn has_ranged_set(ops: &[Op]) -> bool {
    ops.iter()
        .any(|o| matches!(o.kind, OpKind::RangedSetPayload { .. }))
}

fn has_ranged_delete(ops: &[Op]) -> bool {
    ops.iter()
        .any(|o| matches!(o.kind, OpKind::RangedDelete { .. }))
}

#[test]
fn a_single_sequence_mark_reaches_only_its_sequences_reader() {
    let (mut r, mut alice_doc, alice) = seeded();
    seed_list(&mut r, alice, &mut alice_doc, b"a");
    r.take_outbox(alice);
    let bob = join(&mut r, 2, "t-bob"); // reads /a
    let carol = join(&mut r, 3, "t-carol"); // reads /b

    // A mark over the /a sequence rides its anchor path: it reaches bob (reads /a) and is
    // withheld from carol (reads /b) — the annotation is privacy-sensitive like the region.
    seed_mark(&mut r, alice, &mut alice_doc, b"a");
    assert!(
        has_ranged_create(&received_ops(&mut r, bob)),
        "bob reads /a, so the /a mark reaches him",
    );
    assert!(
        received_ops(&mut r, carol).is_empty(),
        "carol cannot read /a, so the /a mark is withheld from her",
    );
}

#[test]
fn a_cross_element_range_requires_read_on_both_anchor_sequences() {
    // The load-bearing require-all case: a range with one endpoint in /a and one in /b
    // reaches a reader of BOTH but not a reader of only /a.
    let (mut r, mut alice_doc, alice) = seeded();
    // dave reads both /a and /b — a genuine both-subtree reader, not the creator.
    submit(
        &mut r,
        alice,
        grant_read(
            &mut alice_doc,
            AclSubject::Actor(actor_key(b"dave")),
            &encode_path(&[b"a"]),
        ),
    );
    submit(
        &mut r,
        alice,
        grant_read(
            &mut alice_doc,
            AclSubject::Actor(actor_key(b"dave")),
            &encode_path(&[b"b"]),
        ),
    );
    seed_list(&mut r, alice, &mut alice_doc, b"a");
    seed_list(&mut r, alice, &mut alice_doc, b"b");
    r.take_outbox(alice);

    let bob = join(&mut r, 2, "t-bob"); // reads /a only
    let dave = join(&mut r, 4, "t-dave"); // reads /a AND /b
    let alice2 = join(&mut r, 10, "t-alice2"); // creator, owns /

    seed_cross_range(&mut r, alice, &mut alice_doc, b"a", b"b");
    assert!(
        has_ranged_create(&received_ops(&mut r, dave)),
        "dave reads both anchor sequences, so the cross-element range reaches him",
    );
    assert!(
        has_ranged_create(&received_ops(&mut r, alice2)),
        "the creator owns /, so the cross-element range reaches it",
    );
    assert!(
        received_ops(&mut r, bob).is_empty(),
        "bob reads only /a, so a range spanning into /b is withheld (require-all)",
    );
}

#[test]
fn a_ranged_payload_change_and_delete_follow_the_ranges_visibility() {
    let (mut r, mut alice_doc, alice) = seeded();
    seed_list(&mut r, alice, &mut alice_doc, b"a");
    let id = seed_mark(&mut r, alice, &mut alice_doc, b"a"); // mark over /a
    r.take_outbox(alice);
    let bob = join(&mut r, 2, "t-bob"); // reads /a
    let carol = join(&mut r, 3, "t-carol"); // reads /b

    // A payload change carries only the range id; the server resolves it to /a, so it
    // reaches bob and is withheld from carol — the op follows its range's visibility.
    submit(
        &mut r,
        alice,
        crdtsync_core::path::mark_set_value(&mut alice_doc, &id, Scalar::Bool(false)),
    );
    assert!(
        has_ranged_set(&received_ops(&mut r, bob)),
        "bob reads /a, so the /a range's payload change reaches him",
    );
    assert!(
        received_ops(&mut r, carol).is_empty(),
        "carol is withheld the /a range's payload change",
    );

    // A delete likewise resolves to /a (the range is already tombstoned when fan-out
    // resolves it, so the anchor set must include tombstoned ranges).
    submit(
        &mut r,
        alice,
        crdtsync_core::path::mark_delete(&mut alice_doc, &id),
    );
    assert!(
        has_ranged_delete(&received_ops(&mut r, bob)),
        "bob reads /a, so the /a range's delete reaches him",
    );
    assert!(
        received_ops(&mut r, carol).is_empty(),
        "carol is withheld the /a range's delete",
    );
}

#[test]
fn the_creator_receives_every_ranged_op() {
    let (mut r, mut alice_doc, alice) = seeded();
    seed_list(&mut r, alice, &mut alice_doc, b"a");
    seed_list(&mut r, alice, &mut alice_doc, b"b");
    r.take_outbox(alice);
    let alice2 = join(&mut r, 10, "t-alice2"); // the creator, owns /

    let id = seed_mark(&mut r, alice, &mut alice_doc, b"a");
    seed_cross_range(&mut r, alice, &mut alice_doc, b"a", b"b");
    submit(
        &mut r,
        alice,
        crdtsync_core::path::mark_set_value(&mut alice_doc, &id, Scalar::Bool(false)),
    );
    let got = received_ops(&mut r, alice2);
    assert!(
        has_ranged_create(&got) && has_ranged_set(&got),
        "the creator owns / and receives every Ranged op, unredacted",
    );
}

#[test]
fn a_room_with_no_acl_tuples_fans_out_ranged_ops_unchanged() {
    // Regression: with no doc-ACL tuples every subscriber the deployment admits receives
    // every Ranged op, byte-identical to the pre-redaction path.
    let mut r = Registry::new(cid(0xFF));
    r.set_verifier(Box::new(tokens(&[
        ("t-alice", "alice"),
        ("t-bob", "bob"),
        ("t-carol", "carol"),
    ])));
    r.set_authorizer(Box::new(
        Acl::new()
            .allow(
                Subject::Anyone,
                Some(Action::Read),
                ResourceMatch::Room(ROOM.to_vec()),
            )
            .allow(
                Subject::Anyone,
                Some(Action::Write),
                ResourceMatch::Room(ROOM.to_vec()),
            ),
    ));
    r.set_clock(Arc::new(ManualClock::new(0)));

    let alice = join(&mut r, 1, "t-alice");
    let bob = join(&mut r, 2, "t-bob");
    let carol = join(&mut r, 3, "t-carol");
    let mut alice_doc = Document::new(cid(1));

    seed_list(&mut r, alice, &mut alice_doc, b"a");
    r.take_outbox(bob);
    r.take_outbox(carol);
    seed_mark(&mut r, alice, &mut alice_doc, b"a");
    let bob_ops = received_ops(&mut r, bob);
    let carol_ops = received_ops(&mut r, carol);
    assert!(
        has_ranged_create(&bob_ops),
        "no ACL ⇒ bob receives the mark"
    );
    assert_eq!(
        bob_ops, carol_ops,
        "no ACL ⇒ every subscriber receives the identical Ranged op",
    );
}

#[test]
fn a_partial_reader_snapshot_keeps_only_ranges_it_fully_reads() {
    // The snapshot half converges with the op half: a compacted room's projected snapshot
    // keeps a RangedElement iff the reader reads every anchor sequence's path.
    let (mut r, mut alice_doc, alice) = seeded();
    seed_list(&mut r, alice, &mut alice_doc, b"a");
    seed_list(&mut r, alice, &mut alice_doc, b"b");
    let mark_a = seed_mark(&mut r, alice, &mut alice_doc, b"a"); // wholly in /a
    seed_cross_range(&mut r, alice, &mut alice_doc, b"a", b"b"); // spans /a → /b
    r.take_outbox(alice);

    // Compact so a fresh subscriber catches up via a projected Snapshot.
    r.set_compaction_threshold(1);
    submit(
        &mut r,
        alice,
        crdtsync_core::path::list_insert(&mut alice_doc, &encode_path(&[b"a"]), 2, b"z"),
    );
    r.take_outbox(alice);

    let bob = auth(&mut r, 2, "t-bob"); // reads /a only
    assert!(subscribe(&mut r, bob));
    let snap =
        served_snapshot(&mut r, bob).expect("a partial reader is served a projected snapshot");
    let ids: Vec<Vec<u8>> = snap
        .ranged_elements()
        .into_iter()
        .map(|e| e.id.as_bytes().to_vec())
        .collect();
    assert_eq!(
        ids,
        vec![mark_a],
        "bob's snapshot keeps the /a mark and drops the range spanning into unreadable /b",
    );
}

// ---- element-scoped grants: a grant that follows a tree-move ---------------
//
// A doc-ACL grant keyed to a stable element id resolves to the element's CURRENT
// path at every enforcement seam (the server injects its element-context index as
// the resolver), so the grant moves atomically with the element across a real
// `XmlMove`. These tests drive the three security properties over the redaction
// seam with columns as the movable content's location:
//
//   1. move-safe    — a deny on a card follows it to its new column;
//   2. no-strand    — the old column is freed when the card leaves it;
//   3. no-exfil     — the card's column is denied wherever the card is dragged.

const COL_A: &[u8] = b"colA";
const COL_B: &[u8] = b"colB";

fn col(key: &[u8]) -> Vec<u8> {
    encode_path(&[key])
}

/// alice builds two `XmlFragment` columns (colA, colB) with one `card` element in
/// colA. Returns the emitted ops and the card's stable element id.
fn build_board(doc: &mut Document) -> (Vec<Op>, ElementId) {
    let mut card = ElementId::from_bytes([0u8; 16]);
    let ops = doc.transact(|tx| {
        tx.xml_fragment(COL_B);
        let mut frag = tx.xml_fragment(COL_A);
        let mut kids = frag.children();
        let mut c = kids.insert_element(0, b"card");
        card = c.id();
    });
    (ops, card)
}

/// Append an element into the column fragment at `col_key` (so the card keeps its
/// index). The op's read path resolves to that column.
fn add_to_col(doc: &mut Document, col_key: &[u8], tag: &[u8]) -> Vec<Op> {
    let p = col(col_key);
    let idx = crdtsync_core::path::xml_children_len(doc, &p).unwrap_or(0);
    xml_insert_element(doc, &p, idx, tag)
}

/// Move the card (index 0 of colA) to colB, a real `XmlMove`.
fn move_card(doc: &mut Document) -> Vec<Op> {
    crdtsync_core::path::xml_move_child(doc, &col(COL_A), 0, &col(COL_B), 0)
}

/// alice grants `subject` an element-scoped `Deny(Read)` on `id`, authored by alice
/// (the creator).
fn grant_deny_element(doc: &mut Document, subject: AclSubject, id: ElementId) -> Vec<Op> {
    doc.transact(|tx| {
        tx.acl().grant_element(
            subject,
            AclGrant::Capability(Capability::Read),
            AclEffect::Deny,
            id,
            actor_key(b"alice"),
        );
    })
}

/// A room where alice (creator) built the board and granted bob read on BOTH columns
/// minus an element `Deny(Read)` on the card. Returns the registry, alice's authoring
/// doc + conn, and the card id.
fn board_seeded() -> (Registry, Document, ConnId, ElementId) {
    let mut r = registry();
    let alice = auth(&mut r, 1, "t-alice");
    assert!(subscribe(&mut r, alice));
    r.take_outbox(alice);
    let mut alice_doc = Document::new(cid(1));
    let (board_ops, card) = build_board(&mut alice_doc);
    submit(&mut r, alice, board_ops);
    let bob = AclSubject::Actor(actor_key(b"bob"));
    submit(
        &mut r,
        alice,
        grant_read(&mut alice_doc, bob.clone(), &col(COL_A)),
    );
    submit(
        &mut r,
        alice,
        grant_read(&mut alice_doc, bob.clone(), &col(COL_B)),
    );
    submit(&mut r, alice, grant_deny_element(&mut alice_doc, bob, card));
    r.take_outbox(alice);
    (r, alice_doc, alice, card)
}

#[test]
fn an_element_deny_read_follows_the_card_across_a_move() {
    let (mut r, mut alice_doc, alice, _card) = board_seeded();
    let bob = join(&mut r, 2, "t-bob");

    // Before the move (card in colA): a colB write reaches bob; a colA write — the
    // card's column — is withheld (the element deny governs colA now).
    submit(&mut r, alice, add_to_col(&mut alice_doc, COL_B, b"x"));
    assert!(
        !received_ops(&mut r, bob).is_empty(),
        "bob reads colB — the card is not there"
    );
    submit(&mut r, alice, add_to_col(&mut alice_doc, COL_A, b"y"));
    assert!(
        received_ops(&mut r, bob).is_empty(),
        "bob is denied colA — the card's current column"
    );

    // Move the card colA -> colB.
    submit(&mut r, alice, move_card(&mut alice_doc));
    r.take_outbox(bob);

    // After the move (card in colB): the deny FOLLOWED — colB is withheld (no
    // exfil-by-move), and colA is freed (no stranded restriction).
    submit(&mut r, alice, add_to_col(&mut alice_doc, COL_A, b"z"));
    assert!(
        !received_ops(&mut r, bob).is_empty(),
        "colA is freed — the restriction moved with the card, it did not strand"
    );
    submit(&mut r, alice, add_to_col(&mut alice_doc, COL_B, b"w"));
    assert!(
        received_ops(&mut r, bob).is_empty(),
        "the deny followed the card to colB — relocating it did not free it"
    );
}

#[test]
fn a_whole_document_reader_receives_every_column_across_a_move() {
    let (mut r, mut alice_doc, alice, _card) = board_seeded();
    // alice's second device — the creator's actor, owns `/` — reads everything, both
    // columns, before and after the move.
    let alice2 = join(&mut r, 10, "t-alice2");
    submit(&mut r, alice, add_to_col(&mut alice_doc, COL_A, b"y"));
    assert!(
        !received_ops(&mut r, alice2).is_empty(),
        "the creator reads colA"
    );
    submit(&mut r, alice, move_card(&mut alice_doc));
    r.take_outbox(alice2);
    submit(&mut r, alice, add_to_col(&mut alice_doc, COL_B, b"w"));
    assert!(
        !received_ops(&mut r, alice2).is_empty(),
        "the creator reads colB — the card's new column — too"
    );
}

/// Seed the board + grants on an already-configured registry (a snapshot variant can
/// set its compaction threshold first). The card stays in colA — the element grant is
/// exercised on a stationary element, so the two catch-up seams are compared on the
/// element→path resolution alone, not the separate moved-node/create-position
/// interaction (that is what the op-seam tests above drive).
fn seed_board(r: &mut Registry) {
    let alice = auth(r, 1, "t-alice");
    assert!(subscribe(r, alice));
    r.take_outbox(alice);
    let mut alice_doc = Document::new(cid(1));
    let (board_ops, card) = build_board(&mut alice_doc);
    submit(r, alice, board_ops);
    let bob = AclSubject::Actor(actor_key(b"bob"));
    submit(
        r,
        alice,
        grant_read(&mut alice_doc, bob.clone(), &col(COL_A)),
    );
    submit(
        r,
        alice,
        grant_read(&mut alice_doc, bob.clone(), &col(COL_B)),
    );
    submit(r, alice, grant_deny_element(&mut alice_doc, bob, card));
    r.take_outbox(alice);
}

fn has_frag(d: &Document, key: &[u8]) -> bool {
    matches!(d.get(key), Some(crdtsync_core::Element::XmlFragment(_)))
}

/// alice grants `subject` an element-scoped `Allow(Read)` on `id`, authored by alice
/// (the creator). The grant follows the element across a move.
fn grant_read_element(doc: &mut Document, subject: AclSubject, id: ElementId) -> Vec<Op> {
    doc.transact(|tx| {
        tx.acl().grant_element(
            subject,
            AclGrant::Capability(Capability::Read),
            AclEffect::Allow,
            id,
            actor_key(b"alice"),
        );
    })
}

/// A parenthesised rendering of one movable XML node — its tag and, recursively, its
/// live children — so two replicas can be compared on their materialised tree.
fn render_node(e: &crdtsync_core::Element) -> String {
    match e {
        crdtsync_core::Element::XmlElement(x) => {
            let x = x.borrow();
            let tag = String::from_utf8_lossy(x.tag()).into_owned();
            let kids: Vec<String> = x
                .children()
                .borrow()
                .values()
                .iter()
                .map(render_node)
                .collect();
            format!("{tag}({})", kids.join(","))
        }
        crdtsync_core::Element::Text(_) => "text".to_string(),
        _ => "?".to_string(),
    }
}

/// A canonical rendering of the two board columns — `absent` when a column is not
/// materialised, else its fragment and live children — the materialised-tree summary
/// an op-served and a snapshot-served reader must agree on.
fn board_render(d: &Document) -> String {
    let col_render = |key: &[u8]| match d.get(key) {
        Some(crdtsync_core::Element::XmlFragment(f)) => {
            let kids: Vec<String> = f
                .borrow()
                .children()
                .borrow()
                .values()
                .iter()
                .map(render_node)
                .collect();
            format!("frag({})", kids.join(","))
        }
        None => "absent".to_string(),
        _ => "?".to_string(),
    };
    format!("colA={} colB={}", col_render(COL_A), col_render(COL_B))
}

/// A snapshot round-trips through the state codec with no dangling reference — a
/// re-encode of a decode is byte-identical, so no purged node is left referenced by a
/// retained list slot.
fn assert_reencodes(d: &Document, label: &str) {
    let bytes = d.encode_state();
    let back = Document::decode_state(&bytes).expect("a projected snapshot decodes");
    assert_eq!(
        back.encode_state(),
        bytes,
        "{label}: re-encode is not canonical — a dangling reference survived the projection",
    );
}

/// alice builds two columns (card born in colA) then moves the card colA -> colB.
/// Authored as separate transactions so a partial-transaction confound never masks the
/// redaction under test (a single cross-column transaction would strand its readable
/// members). Returns the card id.
fn build_and_move(r: &mut Registry, alice: ConnId, alice_doc: &mut Document) -> ElementId {
    submit(r, alice, xml_fragment(alice_doc, &col(COL_B)));
    submit(r, alice, xml_fragment(alice_doc, &col(COL_A)));
    submit(
        r,
        alice,
        xml_insert_element(alice_doc, &col(COL_A), 0, b"card"),
    );
    let card = xml_child_id(alice_doc, COL_A, 0);
    submit(r, alice, move_card(alice_doc));
    card
}

/// The stable element id of the live child at `index` under the fragment at map-slot
/// key `key`.
fn xml_child_id(doc: &Document, key: &[u8], index: usize) -> ElementId {
    match doc.get(key) {
        Some(crdtsync_core::Element::XmlFragment(f)) => {
            f.borrow().children().borrow().values()[index].id()
        }
        _ => panic!("expected a fragment at the map slot"),
    }
}

/// The stable element id of the fragment at map-slot key `key`.
fn frag_id(doc: &Document, key: &[u8]) -> ElementId {
    match doc.get(key) {
        Some(crdtsync_core::Element::XmlFragment(f)) => f.borrow().id(),
        _ => panic!("expected a fragment at the map slot"),
    }
}

#[test]
fn a_node_moved_into_a_denied_subtree_converges_op_join_with_snapshot_join() {
    // The move-into-denied convergence bug (path scope). bob reads colA only. The card
    // is born in colA (readable — bob receives its create) then moved to colB (denied —
    // the XmlMove's read path is the denied destination, so bob never learns the card
    // left). An op-served bob therefore keeps the card in colA; a snapshot-served bob
    // must materialise the identical tree, with no dangling reference on re-encode.
    let seed = |r: &mut Registry| {
        let alice = auth(r, 1, "t-alice");
        assert!(subscribe(r, alice));
        r.take_outbox(alice);
        let mut alice_doc = Document::new(cid(1));
        submit(
            r,
            alice,
            grant_read(
                &mut alice_doc,
                AclSubject::Actor(actor_key(b"bob")),
                &col(COL_A),
            ),
        );
        build_and_move(r, alice, &mut alice_doc);
        r.take_outbox(alice);
    };

    let ops_replica = {
        let mut r = registry();
        seed(&mut r);
        let bob = auth(&mut r, 2, "t-bob");
        assert!(subscribe(&mut r, bob));
        let mut replica = Document::new(cid(2));
        for op in received_ops(&mut r, bob) {
            replica.apply(&op);
        }
        replica
    };
    let snap_replica = {
        let mut r = registry();
        r.set_compaction_threshold(1);
        seed(&mut r);
        let bob = auth(&mut r, 2, "t-bob");
        assert!(subscribe(&mut r, bob));
        served_snapshot(&mut r, bob).expect("a partial reader is served a projected snapshot")
    };

    assert_eq!(
        board_render(&ops_replica),
        "colA=frag(card()) colB=absent",
        "op-join bob keeps the card in colA — he got its create, never its move-away",
    );
    assert_eq!(
        board_render(&snap_replica),
        board_render(&ops_replica),
        "snapshot-join bob must converge with op-join bob on the materialised tree",
    );
    assert_reencodes(&snap_replica, "snapshot-join");
}

#[test]
fn an_element_grant_on_the_origin_column_converges_op_join_with_snapshot_join() {
    // The move-into-denied convergence bug (element scope). bob's read is an
    // element-scoped Allow(Read) on the colA fragment — the card's ORIGIN — which the
    // server resolves through its element index to path colA. So bob reads colA and not
    // colB, exactly as a path-scope reader would, but via an element grant that must
    // resolve identically on the op fan-out and the snapshot projection. The card is born
    // in colA (readable) then moved to colB (denied): an op-served bob keeps it at colA
    // (he got its create, never its move-away), and a snapshot-served bob must converge.
    let seed = |r: &mut Registry| {
        let alice = auth(r, 1, "t-alice");
        assert!(subscribe(r, alice));
        r.take_outbox(alice);
        let mut alice_doc = Document::new(cid(1));
        submit(r, alice, xml_fragment(&mut alice_doc, &col(COL_B)));
        submit(r, alice, xml_fragment(&mut alice_doc, &col(COL_A)));
        let cola = frag_id(&alice_doc, COL_A);
        submit(
            r,
            alice,
            grant_read_element(&mut alice_doc, AclSubject::Actor(actor_key(b"bob")), cola),
        );
        submit(
            r,
            alice,
            xml_insert_element(&mut alice_doc, &col(COL_A), 0, b"card"),
        );
        submit(r, alice, move_card(&mut alice_doc));
        r.take_outbox(alice);
    };

    let ops_replica = {
        let mut r = registry();
        seed(&mut r);
        let bob = auth(&mut r, 2, "t-bob");
        assert!(subscribe(&mut r, bob));
        let mut replica = Document::new(cid(2));
        for op in received_ops(&mut r, bob) {
            replica.apply(&op);
        }
        replica
    };
    let snap_replica = {
        let mut r = registry();
        r.set_compaction_threshold(1);
        seed(&mut r);
        let bob = auth(&mut r, 2, "t-bob");
        assert!(subscribe(&mut r, bob));
        served_snapshot(&mut r, bob).expect("a partial reader is served a projected snapshot")
    };

    assert_eq!(
        board_render(&ops_replica),
        "colA=frag(card()) colB=absent",
        "op-join bob keeps the card in colA — the element grant on colA admits its create",
    );
    assert_eq!(
        board_render(&snap_replica),
        board_render(&ops_replica),
        "snapshot-join bob must converge with op-join bob on the materialised tree",
    );
    assert_reencodes(&snap_replica, "snapshot-join");
}

#[test]
fn an_element_grant_reader_converges_op_join_with_snapshot_join() {
    // bob reads colA + colB minus an element Deny(Read) on the card (in colA). Both an
    // op-served bob and a snapshot-served bob resolve the element grant through the
    // same server index to the card's column (colA), so both drop colA and keep colB —
    // the two catch-up seams converge on the same authorized subset.
    let ops_replica = {
        let mut r = registry();
        seed_board(&mut r);
        let bob = auth(&mut r, 2, "t-bob");
        assert!(subscribe(&mut r, bob));
        let mut replica = Document::new(cid(2));
        for op in received_ops(&mut r, bob) {
            replica.apply(&op);
        }
        replica
    };
    let snap_replica = {
        let mut r = registry();
        r.set_compaction_threshold(1);
        seed_board(&mut r);
        let bob = auth(&mut r, 2, "t-bob");
        assert!(subscribe(&mut r, bob));
        served_snapshot(&mut r, bob).expect("a partial reader is served a projected snapshot")
    };
    for (label, replica) in [("op-join", &ops_replica), ("snapshot-join", &snap_replica)] {
        assert!(
            has_frag(replica, COL_B),
            "{label} bob materializes colB — readable, holds no denied element"
        );
        assert!(
            !has_frag(replica, COL_A),
            "{label} bob drops colA — the element deny resolves there and hides it"
        );
    }
}

#[test]
fn a_room_with_no_acl_fans_out_a_move_to_every_reader() {
    // Regression: with no doc-ACL tuples the board + move fan out unredacted to a plain
    // subscriber — the pre-redaction path, untouched by element scopes.
    let mut r = Registry::new(cid(0xFF));
    r.set_verifier(Box::new(tokens(&[("t-alice", "alice"), ("t-bob", "bob")])));
    r.set_authorizer(Box::new(
        Acl::new()
            .allow(
                Subject::Anyone,
                Some(Action::Read),
                ResourceMatch::Room(ROOM.to_vec()),
            )
            .allow(
                Subject::Actor(b"alice".to_vec()),
                Some(Action::Write),
                ResourceMatch::Room(ROOM.to_vec()),
            ),
    ));
    r.set_clock(Arc::new(ManualClock::new(0)));
    let alice = auth(&mut r, 1, "t-alice");
    assert!(subscribe(&mut r, alice));
    r.take_outbox(alice);
    let mut alice_doc = Document::new(cid(1));
    let (board_ops, _card) = build_board(&mut alice_doc);
    submit(&mut r, alice, board_ops);
    r.take_outbox(alice);
    let bob = join(&mut r, 2, "t-bob");
    submit(&mut r, alice, move_card(&mut alice_doc));
    assert!(
        !received_ops(&mut r, bob).is_empty(),
        "no doc-ACL: the move fans out to bob unredacted"
    );
}
