//! Blob-fetch authorization — the reference-site ACL that closes the
//! "authenticated but not authorized" gap.
//!
//! A blob is content-addressed and immutable, so authority cannot attach to the
//! bytes: it attaches to the paths that *reference* the blob. A fetch is allowed
//! iff the caller holds READ authority on at least one live reference to the id,
//! resolved through the SAME doc-ACL evaluator the op-stream redaction uses. This
//! drives that decision (`Registry::authorize_blob_fetch`) over a seeded room, so
//! the security properties are asserted deterministically against the live ACL —
//! the route-level 403 wiring lives in `blob_http`.
//!
//! The whole path is in-process (no socket / fs) with a fixed clock, so it runs
//! under Miri. The deployment authorizer abstains on every actor's read but
//! alice's bootstrap, so each read verdict is the doc-ACL tier's and the
//! reference-site gate actually bites.

use std::sync::Arc;

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::{encode_path, set_blob_ref};
use crdtsync_core::protocol::Channel;
use crdtsync_core::{AclEffect, ClientId, Document, Message, Op, Scalar};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::{Action, ConnId, Identity, ManualClock, Registry, StaticTokens};

const ROOM: &[u8] = b"room-a";

// Distinct blob handles referenced at known positions in the seeded room.
const BLOB_A: [u8; 16] = [0xA1; 16]; // referenced at /a/pic — readable by a /a reader
const BLOB_B: [u8; 16] = [0xB2; 16]; // referenced at /b/pic — readable by a /b reader
const BLOB_SECRET: [u8; 16] = [0x53; 16]; // referenced at /a/secret — under a leaf deny
const BLOB_SHARED: [u8; 16] = [0x5D; 16]; // referenced at /a/pic2 AND /b/pic2
const BLOB_UNREFERENCED: [u8; 16] = [0xFF; 16]; // never placed — a leaked/guessed id

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

/// A registry whose deployment permits alice (the creator) read + write, but
/// abstains on every other actor's read — so bob's, carol's, and dave's read
/// verdicts are the doc-ACL tier's alone and the reference-site gate bites. A fixed
/// clock keeps it Miri-clean.
fn registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_verifier(Box::new(tokens(&[
        ("t-alice", "alice"),
        ("t-bob", "bob"),
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

fn submit(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: Channel(0),
            ops
        }
    ));
}

/// A write into the top-level subtree `key` — a nested map holding one register.
fn write_subtree(doc: &mut Document, key: &[u8], v: i64) -> Vec<Op> {
    doc.transact(|tx| {
        tx.map(key).register(b"v", Scalar::Int(v));
    })
}

/// alice grants `subject` `capability` with `effect` at `path`, authored by alice.
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

/// alice grants `subject` read at `path`.
fn grant_read(doc: &mut Document, subject: AclSubject, path: &[u8]) -> Vec<Op> {
    grant_cap(doc, subject, Capability::Read, AclEffect::Allow, path)
}

/// The identity a credential resolves to — the actor a fetch authorizes as.
fn identity(r: &Registry, credential: &str) -> Identity {
    r.verify_credential(credential.as_bytes())
        .expect("a known credential resolves to an identity")
}

/// A room where alice (creator) wrote /a and /b, granted bob read /a and carol
/// read /b, denied bob read on the leaf /a/secret, and placed blob references:
///   - BLOB_A at /a/pic          (readable by a /a reader)
///   - BLOB_B at /b/pic          (readable by a /b reader)
///   - BLOB_SECRET at /a/secret  (under bob's leaf deny)
///   - BLOB_SHARED at /a/pic2 and /b/pic2 (two reference sites)
///
/// Returns the seeded registry.
fn seeded() -> Registry {
    let mut r = registry();
    let alice = auth(&mut r, 1, "t-alice");
    assert!(subscribe(&mut r, alice));
    r.take_outbox(alice);
    let mut doc = Document::new(cid(1));

    // alice writes first, becoming the room's creator (owns `/`).
    submit(&mut r, alice, write_subtree(&mut doc, b"a", 0));
    submit(&mut r, alice, write_subtree(&mut doc, b"b", 0));
    submit(
        &mut r,
        alice,
        grant_read(
            &mut doc,
            AclSubject::Actor(actor_key(b"bob")),
            &encode_path(&[b"a"]),
        ),
    );
    submit(
        &mut r,
        alice,
        grant_read(
            &mut doc,
            AclSubject::Actor(actor_key(b"carol")),
            &encode_path(&[b"b"]),
        ),
    );
    // bob may read /a as a whole but is denied the leaf /a/secret.
    submit(
        &mut r,
        alice,
        grant_cap(
            &mut doc,
            AclSubject::Actor(actor_key(b"bob")),
            Capability::Read,
            AclEffect::Deny,
            &encode_path(&[b"a", b"secret"]),
        ),
    );

    // Place the blob references.
    let refs: [(&[&[u8]], [u8; 16]); 5] = [
        (&[b"a", b"pic"], BLOB_A),
        (&[b"b", b"pic"], BLOB_B),
        (&[b"a", b"secret"], BLOB_SECRET),
        (&[b"a", b"pic2"], BLOB_SHARED),
        (&[b"b", b"pic2"], BLOB_SHARED),
    ];
    for (segs, id) in refs {
        let path = encode_path(segs);
        submit(
            &mut r,
            alice,
            set_blob_ref(&mut doc, &path, id, "application/octet-stream", 9000),
        );
    }
    r.take_outbox(alice);
    r
}

#[test]
fn a_reader_of_a_referencing_path_may_fetch() {
    let mut r = seeded();
    // bob reads /a, which references BLOB_A at /a/pic → allowed.
    assert!(
        r.authorize_blob_fetch(&identity(&r, "t-bob"), &BLOB_A),
        "a reader of the referencing subtree may fetch the blob",
    );
    // carol reads /b, which references BLOB_B at /b/pic → allowed.
    assert!(r.authorize_blob_fetch(&identity(&r, "t-carol"), &BLOB_B));
}

#[test]
fn a_non_reader_is_denied_even_when_authenticated() {
    let mut r = seeded();
    // carol is authenticated but reads only /b; BLOB_A is referenced only under /a.
    assert!(
        !r.authorize_blob_fetch(&identity(&r, "t-carol"), &BLOB_A),
        "an authenticated caller with no read on any referencing path is denied",
    );
    // bob, symmetrically, cannot fetch the /b-referenced blob.
    assert!(!r.authorize_blob_fetch(&identity(&r, "t-bob"), &BLOB_B));
    // dave holds no grant at all — denied everywhere.
    assert!(!r.authorize_blob_fetch(&identity(&r, "t-dave"), &BLOB_A));
    assert!(!r.authorize_blob_fetch(&identity(&r, "t-dave"), &BLOB_B));
}

#[test]
fn an_unreferenced_blob_is_denied_fail_closed() {
    let mut r = seeded();
    // A leaked or guessed id nothing references is denied for everyone — including
    // the creator who owns `/`. Authority is the reference site; with none, no fetch.
    assert!(
        !r.authorize_blob_fetch(&identity(&r, "t-bob"), &BLOB_UNREFERENCED),
        "an unreferenced id is denied (fail-closed)",
    );
    assert!(
        !r.authorize_blob_fetch(&identity(&r, "t-alice"), &BLOB_UNREFERENCED),
        "even the creator cannot fetch a blob nothing references",
    );
}

#[test]
fn a_blob_referenced_only_from_a_denied_position_is_denied() {
    let mut r = seeded();
    // BLOB_SECRET is referenced only at /a/secret, where bob holds a leaf Deny(Read)
    // despite reading /a as a whole. The reference site is unreadable → denied,
    // consistent with the op-stream leaf-deny redaction.
    assert!(
        !r.authorize_blob_fetch(&identity(&r, "t-bob"), &BLOB_SECRET),
        "a blob referenced only from a denied position is not fetchable",
    );
    // The creator (owns `/`, no carve-out applies to it) still reaches it — the blob
    // IS referenced, just not from a position bob may read.
    assert!(
        r.authorize_blob_fetch(&identity(&r, "t-alice"), &BLOB_SECRET),
        "the reference exists, so the owner may fetch it",
    );
}

#[test]
fn a_blob_referenced_from_multiple_paths_is_fetchable_from_any_readable_one() {
    let mut r = seeded();
    // BLOB_SHARED sits at /a/pic2 and /b/pic2. bob reads /a, carol reads /b — each is
    // authorized through the site it can read (any-readable grants).
    assert!(
        r.authorize_blob_fetch(&identity(&r, "t-bob"), &BLOB_SHARED),
        "bob is authorized via the /a reference site",
    );
    assert!(
        r.authorize_blob_fetch(&identity(&r, "t-carol"), &BLOB_SHARED),
        "carol is authorized via the /b reference site",
    );
    // dave reads neither site → denied even though the blob is multiply referenced.
    assert!(
        !r.authorize_blob_fetch(&identity(&r, "t-dave"), &BLOB_SHARED),
        "a caller who reads no referencing site is denied",
    );
}

#[test]
fn the_creator_may_fetch_every_referenced_blob() {
    let mut r = seeded();
    let alice = identity(&r, "t-alice");
    for id in [BLOB_A, BLOB_B, BLOB_SECRET, BLOB_SHARED] {
        assert!(
            r.authorize_blob_fetch(&alice, &id),
            "the creator owns / and reads every reference site",
        );
    }
}

#[test]
fn a_deleted_reference_makes_the_blob_unfetchable() {
    // Removing the only reference to a blob revokes the authority to fetch it — the
    // index is derived from the live tree, so a tombstoned slot drops the site.
    let mut r = registry();
    let alice = auth(&mut r, 1, "t-alice");
    assert!(subscribe(&mut r, alice));
    r.take_outbox(alice);
    let mut doc = Document::new(cid(1));
    submit(&mut r, alice, write_subtree(&mut doc, b"a", 0));
    submit(
        &mut r,
        alice,
        grant_read(
            &mut doc,
            AclSubject::Actor(actor_key(b"bob")),
            &encode_path(&[b"a"]),
        ),
    );
    submit(
        &mut r,
        alice,
        set_blob_ref(
            &mut doc,
            &encode_path(&[b"a", b"pic"]),
            BLOB_A,
            "application/octet-stream",
            9000,
        ),
    );
    r.take_outbox(alice);
    assert!(
        r.authorize_blob_fetch(&identity(&r, "t-bob"), &BLOB_A),
        "bob may fetch while the reference is live",
    );

    // Delete the referencing slot; the blob is now unreferenced → denied.
    submit(
        &mut r,
        alice,
        crdtsync_core::path::delete(&mut doc, &encode_path(&[b"a", b"pic"])),
    );
    r.take_outbox(alice);
    assert!(
        !r.authorize_blob_fetch(&identity(&r, "t-bob"), &BLOB_A),
        "deleting the only reference makes the blob unfetchable (fail-closed)",
    );
    assert!(
        !r.authorize_blob_fetch(&identity(&r, "t-alice"), &BLOB_A),
        "not even the creator can fetch it once no path references it",
    );
}
