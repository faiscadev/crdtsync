//! Doc-level ACL — server enforcement of the actor-keyed doc-ACL tier (slice 4a).
//!
//! The doc-ACL tuple tier composes between the deployment authorizer and the
//! schema `@auth` grants: the room's creator auto-owns `/`, and its grants let
//! other actors in. The ACL principal is the *authenticated actor* — the
//! credential-derived `Identity.actor`, keyed via [`actor_key`] — not the ephemeral
//! per-device client id, so one human across two devices is one ACL subject.
//!
//! Two layers of test: the pure evaluator wiring ([`doc_acl_tier`], Miri-clean, no
//! I/O), and the end-to-end path through the in-memory [`Registry`] (a creator
//! bootstraps a room, grants let others write, deny overrides, two devices of one
//! actor share a grant).

use std::collections::HashMap;
use std::sync::Arc;

use crdtsync_core::acl::{
    AclEffect, AclGrant, AclRecord, AclScope, AclSubject, AclTuple, Capability,
};
use crdtsync_core::elementid::ElementId;
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Op, Scalar};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::authz::Decision;
use crdtsync_server::{Action, ConnId, Identity, ManualClock, Registry, StaticTokens};

const ROOM: &[u8] = b"room-a";

// ---- pure evaluator wiring (Miri-clean) -----------------------------------

fn root() -> Vec<u8> {
    encode_path(&[])
}

// These tuples are all root path-scoped, so the element-context index is never
// consulted — pass an empty one. Element-scoped enforcement is covered in
// `acl_element.rs`.
fn doc_acl_tier(
    records: &[AclRecord],
    creator: Option<&[u8]>,
    identity: &Identity,
    action: Action,
) -> Decision {
    crdtsync_server::acl::doc_acl_tier(records, creator, &HashMap::new(), identity, action)
}

/// A live actor-capability grant rooted at `grantor`.
fn grant(
    subject: AclSubject,
    capability: Capability,
    effect: AclEffect,
    grantor: ClientId,
) -> AclRecord {
    AclRecord {
        tuple: AclTuple {
            id: ElementId::from_bytes([0u8; 16]),
            subject,
            grant: AclGrant::Capability(capability),
            effect,
            scope: AclScope::Path(root()),
            grantor,
        },
        revoked_by: Vec::new(),
    }
}

#[test]
fn the_creator_auto_owns_the_root_and_a_stranger_abstains() {
    let creator = b"alice".as_slice();
    let none: Vec<AclRecord> = Vec::new();
    let alice = Identity::new(b"alice".to_vec());
    let bob = Identity::new(b"bob".to_vec());

    // The creator owns `/`, so it may write and read with no explicit tuple.
    assert_eq!(
        doc_acl_tier(&none, Some(creator), &alice, Action::Write),
        Decision::Allow
    );
    assert_eq!(
        doc_acl_tier(&none, Some(creator), &alice, Action::Read),
        Decision::Allow
    );
    // A non-creator with no governing tuple is not denied here — the tier holds no
    // opinion, so a lower tier (schema, then default-deny) decides.
    assert_eq!(
        doc_acl_tier(&none, Some(creator), &bob, Action::Write),
        Decision::Abstain
    );
}

#[test]
fn a_room_with_no_creator_and_no_tuples_abstains_entirely() {
    // The regression basis: with no doc-ACL state the tier abstains for everyone,
    // so the composed decision is exactly the deployment/schema tiers alone.
    let none: Vec<AclRecord> = Vec::new();
    for actor in [b"alice".as_slice(), b"bob", b"anon:x"] {
        let id = Identity::new(actor.to_vec());
        for action in [Action::Read, Action::Write, Action::PublishAwareness] {
            assert_eq!(doc_acl_tier(&none, None, &id, action), Decision::Abstain);
        }
    }
}

#[test]
fn an_explicit_grant_lets_an_actor_write_and_a_deny_overrides_it() {
    let creator = b"alice".as_slice();
    let bob_key = actor_key(b"bob");
    let bob = Identity::new(b"bob".to_vec());

    let allow = vec![grant(
        AclSubject::Actor(bob_key),
        Capability::Write,
        AclEffect::Allow,
        actor_key(creator),
    )];
    assert_eq!(
        doc_acl_tier(&allow, Some(creator), &bob, Action::Write),
        Decision::Allow
    );

    // Deny-overrides: a deny of write to bob, authored by the creator, beats the
    // allow.
    let mut denied = allow.clone();
    denied.push(grant(
        AclSubject::Actor(bob_key),
        Capability::Write,
        AclEffect::Deny,
        actor_key(creator),
    ));
    assert_eq!(
        doc_acl_tier(&denied, Some(creator), &bob, Action::Write),
        Decision::Deny
    );
}

#[test]
fn a_group_grant_matches_an_actor_by_its_credential_group() {
    let creator = b"alice".as_slice();
    let records = vec![grant(
        AclSubject::Group(b"eng".to_vec()),
        Capability::Write,
        AclEffect::Allow,
        actor_key(creator),
    )];
    let in_group = Identity::with_claims(b"bob".to_vec(), Vec::new(), vec!["eng".to_string()]);
    let out_of_group =
        Identity::with_claims(b"carol".to_vec(), Vec::new(), vec!["design".to_string()]);

    assert_eq!(
        doc_acl_tier(&records, Some(creator), &in_group, Action::Write),
        Decision::Allow,
    );
    assert_eq!(
        doc_acl_tier(&records, Some(creator), &out_of_group, Action::Write),
        Decision::Abstain,
    );
}

#[test]
fn an_anonymous_grant_matches_only_an_unauthenticated_actor() {
    let creator = b"alice".as_slice();
    let records = vec![grant(
        AclSubject::Anonymous,
        Capability::Write,
        AclEffect::Allow,
        actor_key(creator),
    )];
    // An `anon:`-prefixed actor is unauthenticated — the anonymous grant matches.
    let anon = Identity::new(b"anon:mallory".to_vec());
    // A credentialed actor is authenticated — the anonymous grant does not match.
    let auth = Identity::new(b"bob".to_vec());

    assert_eq!(
        doc_acl_tier(&records, Some(creator), &anon, Action::Write),
        Decision::Allow,
    );
    assert_eq!(
        doc_acl_tier(&records, Some(creator), &auth, Action::Write),
        Decision::Abstain,
    );
}

#[test]
fn the_same_actor_across_two_identities_is_one_principal() {
    // Two `Identity`s for the same human (two devices / credentials, same actor
    // bytes) key to the same actor id, so a single grant governs both.
    let creator = b"alice".as_slice();
    let records = vec![grant(
        AclSubject::Actor(actor_key(b"bob")),
        Capability::Write,
        AclEffect::Allow,
        actor_key(creator),
    )];
    let laptop = Identity::new(b"bob".to_vec());
    let phone = Identity::new(b"bob".to_vec());
    assert_eq!(
        doc_acl_tier(&records, Some(creator), &laptop, Action::Write),
        Decision::Allow,
    );
    assert_eq!(
        doc_acl_tier(&records, Some(creator), &phone, Action::Write),
        Decision::Allow,
    );
}

#[test]
fn register_schema_has_no_capability_form_and_abstains() {
    let creator = b"alice".as_slice();
    let alice = Identity::new(b"alice".to_vec());
    // A control-plane meta-auth has no doc-level capability, so the tier abstains
    // even for the creator.
    assert_eq!(
        doc_acl_tier(&[], Some(creator), &alice, Action::RegisterSchema),
        Decision::Abstain,
    );
}

// ---- end to end through the Registry --------------------------------------

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A registry whose deployment permits any actor to read the room and permits
/// `alice`'s writes (the bootstrap), abstaining on every other write so the
/// doc-ACL tier decides it. A fixed clock keeps it Miri-clean.
fn registry(tokens: StaticTokens) -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_verifier(Box::new(tokens));
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
    r
}

fn tokens(rows: &[(&str, &str)]) -> StaticTokens {
    let mut t = StaticTokens::new();
    for (credential, actor) in rows {
        t.insert(credential.as_bytes().to_vec(), actor.as_bytes().to_vec());
    }
    t
}

/// A relay connection (no app declared) authenticated as `credential`, then
/// subscribed to the room on channel 0.
fn join(r: &mut Registry, client: u8, credential: &str) -> ConnId {
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
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            zone: Vec::new(),
            last_seen_seq: 0,
            branch: Vec::new(),
        }
    ));
    r.take_outbox(id);
    id
}

/// One op registering a value — each call advances `doc`'s per-client sequence, so
/// repeated writes from one author carry distinct op ids (never self-deduped).
fn write_op(doc: &mut Document) -> Vec<Op> {
    doc.transact(|tx| tx.register(b"age", Scalar::Int(30)))
}

/// The ops for the author behind `doc` to grant `subject` a capability with
/// `effect` at root, recorded under `grantor` (the ACL principal). Advances the
/// author's sequence, so it never collides with the author's data writes.
fn grant_op(
    doc: &mut Document,
    subject: AclSubject,
    capability: Capability,
    effect: AclEffect,
    grantor: ClientId,
) -> Vec<Op> {
    doc.transact(|tx| {
        tx.acl().grant(
            subject,
            AclGrant::Capability(capability),
            effect,
            encode_path(&[]),
            grantor,
        );
    })
}

/// Whether an ops write on channel 0 was accepted — no refusal reply.
fn submit(r: &mut Registry, id: ConnId, ops: Vec<Op>) -> bool {
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: Channel(0),
            ops
        }
    ));
    let denied = r
        .take_outbox(id)
        .into_iter()
        .any(|m| matches!(m, Message::Error { .. } | Message::OpsRejected { .. }));
    !denied
}

/// Grant bob write, authored by alice (the creator) — the tuple every grant test
/// shares.
fn grant_bob_write(alice_doc: &mut Document, effect: AclEffect) -> Vec<Op> {
    grant_op(
        alice_doc,
        AclSubject::Actor(actor_key(b"bob")),
        Capability::Write,
        effect,
        actor_key(b"alice"),
    )
}

#[test]
fn a_creator_bootstraps_the_room_then_grants_another_actor_write() {
    let mut r = registry(tokens(&[("t-alice", "alice"), ("t-bob", "bob")]));
    let alice = join(&mut r, 1, "t-alice");
    let bob = join(&mut r, 2, "t-bob");
    let mut alice_doc = Document::new(cid(1));
    let mut bob_doc = Document::new(cid(2));

    // alice writes first (deployment permits it), establishing the room and
    // becoming its creator.
    assert!(submit(&mut r, alice, write_op(&mut alice_doc)));

    // bob has no grant yet: the deployment abstains on his write and no tuple
    // governs him, so he is denied.
    assert!(
        !submit(&mut r, bob, write_op(&mut bob_doc)),
        "an ungranted actor is denied at the doc-ACL tier",
    );

    // alice (the creator, who owns `/`) grants bob write. The grant roots at the
    // creator, so bob may now write.
    assert!(submit(
        &mut r,
        alice,
        grant_bob_write(&mut alice_doc, AclEffect::Allow)
    ));
    assert!(
        submit(&mut r, bob, write_op(&mut bob_doc)),
        "the creator's grant lets bob write",
    );
}

#[test]
fn a_deny_overrides_a_grant_end_to_end() {
    let mut r = registry(tokens(&[("t-alice", "alice"), ("t-bob", "bob")]));
    let alice = join(&mut r, 1, "t-alice");
    let bob = join(&mut r, 2, "t-bob");
    let mut alice_doc = Document::new(cid(1));
    let mut bob_doc = Document::new(cid(2));
    assert!(submit(&mut r, alice, write_op(&mut alice_doc)));

    assert!(submit(
        &mut r,
        alice,
        grant_bob_write(&mut alice_doc, AclEffect::Allow)
    ));
    assert!(submit(&mut r, bob, write_op(&mut bob_doc)));

    // The creator denies bob write; deny-overrides, so bob is blocked again.
    assert!(submit(
        &mut r,
        alice,
        grant_bob_write(&mut alice_doc, AclEffect::Deny)
    ));
    assert!(
        !submit(&mut r, bob, write_op(&mut bob_doc)),
        "the creator's deny overrides its own earlier grant",
    );
}

#[test]
fn two_devices_of_the_same_actor_share_one_grant() {
    // bob signs in from two devices — distinct client ids, distinct credentials,
    // the same actor. A grant to the actor lets either device write, proving the
    // principal is the actor, not the per-device client.
    let mut r = registry(tokens(&[
        ("t-alice", "alice"),
        ("t-bob-laptop", "bob"),
        ("t-bob-phone", "bob"),
    ]));
    let alice = join(&mut r, 1, "t-alice");
    let laptop = join(&mut r, 20, "t-bob-laptop");
    let phone = join(&mut r, 21, "t-bob-phone");
    let mut alice_doc = Document::new(cid(1));
    let mut laptop_doc = Document::new(cid(20));
    let mut phone_doc = Document::new(cid(21));
    assert!(submit(&mut r, alice, write_op(&mut alice_doc)));

    assert!(submit(
        &mut r,
        alice,
        grant_bob_write(&mut alice_doc, AclEffect::Allow)
    ));

    assert!(
        submit(&mut r, laptop, write_op(&mut laptop_doc)),
        "bob's laptop writes under the actor grant",
    );
    assert!(
        submit(&mut r, phone, write_op(&mut phone_doc)),
        "bob's phone — a different device — writes under the same actor grant",
    );
}

#[test]
fn an_anonymous_first_writer_does_not_become_the_creator() {
    // An anonymous connection may bootstrap-write (the deployment permits it) but
    // must NOT claim the room's authority root — an anon id is ephemeral, so set-once
    // ownership on it would wedge the room. Proof: the later authenticated writer
    // (alice) still becomes creator, so her grant to bob roots and lets bob write.
    let mut r = Registry::new(cid(0xFF));
    r.set_verifier(Box::new(tokens(&[
        ("t-anon", "anon:mallory"),
        ("t-alice", "alice"),
        ("t-bob", "bob"),
    ])));
    // Read to anyone; write to anon:mallory and alice (both bootstrap-eligible);
    // abstain on bob's write so only the doc-ACL grant can admit him.
    r.set_authorizer(Box::new(
        Acl::new()
            .allow(
                Subject::Anyone,
                Some(Action::Read),
                ResourceMatch::Room(ROOM.to_vec()),
            )
            .allow(
                Subject::Actor(b"anon:mallory".to_vec()),
                Some(Action::Write),
                ResourceMatch::Room(ROOM.to_vec()),
            )
            .allow(
                Subject::Actor(b"alice".to_vec()),
                Some(Action::Write),
                ResourceMatch::Room(ROOM.to_vec()),
            ),
    ));
    r.set_clock(Arc::new(ManualClock::new(0)));

    let anon = join(&mut r, 30, "t-anon");
    let alice = join(&mut r, 1, "t-alice");
    let bob = join(&mut r, 2, "t-bob");
    let mut anon_doc = Document::new(cid(30));
    let mut alice_doc = Document::new(cid(1));
    let mut bob_doc = Document::new(cid(2));

    // The anon writes first — establishing the room, but not its creator.
    assert!(submit(&mut r, anon, write_op(&mut anon_doc)));
    // The authenticated alice writes next and becomes the creator (set-once did not
    // wedge on the anon).
    assert!(submit(&mut r, alice, write_op(&mut alice_doc)));
    // Alice's grant to bob roots only because she — not the anon — owns `/`.
    assert!(submit(
        &mut r,
        alice,
        grant_bob_write(&mut alice_doc, AclEffect::Allow)
    ));
    assert!(
        submit(&mut r, bob, write_op(&mut bob_doc)),
        "the second, authenticated writer owns the room, so its grant admits bob",
    );
}

#[test]
fn an_ungranted_actor_stays_denied_when_the_room_has_a_creator() {
    // Regression: adding a creator and tuples for bob changes nothing for a third
    // actor the deployment abstains on — it is denied exactly as before the tier.
    let mut r = registry(tokens(&[
        ("t-alice", "alice"),
        ("t-bob", "bob"),
        ("t-carol", "carol"),
    ]));
    let alice = join(&mut r, 1, "t-alice");
    let carol = join(&mut r, 3, "t-carol");
    let mut alice_doc = Document::new(cid(1));
    let mut carol_doc = Document::new(cid(3));
    assert!(submit(&mut r, alice, write_op(&mut alice_doc)));
    assert!(submit(
        &mut r,
        alice,
        grant_bob_write(&mut alice_doc, AclEffect::Allow)
    ));
    assert!(
        !submit(&mut r, carol, write_op(&mut carol_doc)),
        "a grant to bob confers nothing on carol",
    );
}
