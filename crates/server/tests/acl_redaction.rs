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

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::{encode_path, xml_fragment, xml_insert_element};
use crdtsync_core::protocol::Channel;
use crdtsync_core::{
    AclEffect, ClientId, Document, ElementId, ElementKind, ErrorCode, Message, Op, OpKind, Scalar,
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
    assert!(
        replay
            .iter()
            .all(|op| !matches!(op.kind, OpKind::AclGrant { .. })),
        "the ACL grants bob cannot read are withheld from his replay",
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

#[test]
fn a_partial_reader_is_refused_an_unredactable_snapshot_catch_up() {
    let (mut r, mut alice_doc, alice) = seeded();
    // Compact the room so a fresh subscriber below the floor catches up via a
    // Snapshot (the whole materialized replica), not an op delta.
    r.set_compaction_threshold(1);
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"a", 1));
    submit(&mut r, alice, write_subtree(&mut alice_doc, b"b", 2));
    r.take_outbox(alice);

    // bob (reads /a only) subscribes fresh. The snapshot cannot be path-redacted, so
    // a non-whole-document reader is refused rather than served the whole state
    // (carol's /b included).
    let bob = auth(&mut r, 2, "t-bob");
    assert!(subscribe(&mut r, bob));
    let out = r.take_outbox(bob);
    assert!(
        out.iter().any(|m| matches!(
            m,
            Message::Error {
                code: ErrorCode::Forbidden,
                ..
            }
        )),
        "a partial reader is refused an unredactable snapshot",
    );
    assert!(
        !out.iter().any(|m| matches!(m, Message::Snapshot { .. })),
        "no snapshot state leaks to the partial reader",
    );

    // A whole-document reader — a second alice device, the creator who owns `/` — IS
    // served the snapshot.
    let alice2 = auth(&mut r, 10, "t-alice2");
    assert!(subscribe(&mut r, alice2));
    assert!(
        r.take_outbox(alice2)
            .iter()
            .any(|m| matches!(m, Message::Snapshot { .. })),
        "the creator (a whole-document reader) is served the snapshot",
    );
}

#[test]
fn a_downstream_read_deny_carve_out_refuses_the_snapshot() {
    // bob is granted read on the whole document (`/`) then denied read on `/secret`
    // — the AWS-style downstream carve-out. He reads root (Allow, since a descendant
    // deny does not govern the root query), so he subscribes; but the snapshot
    // contains `/secret`, which the live fan-out withholds from him, so an
    // unredactable snapshot is refused rather than leaking the carve-out.
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
    let out = r.take_outbox(bob);
    assert!(
        out.iter().any(|m| matches!(
            m,
            Message::Error {
                code: ErrorCode::Forbidden,
                ..
            }
        )),
        "a reader carved out of a whole-document grant is refused the snapshot",
    );
    assert!(
        !out.iter().any(|m| matches!(m, Message::Snapshot { .. })),
        "no snapshot leaks the /secret carve-out to bob",
    );
}
