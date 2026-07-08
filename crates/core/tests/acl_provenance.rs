//! Doc-level ACL — the provenance/authority foundation (Doc-level ACL slice 3a).
//!
//! Two authority-layer rules over the merged tuple set, on top of slice 2's
//! as-present evaluator:
//!
//! - **creator-auto-owns-`/`** — the doc creator (a `ClientId` the caller supplies,
//!   since core hosts no identity provider) implicitly holds `Own` at the root path,
//!   the bootstrap owner every grant chains from — no explicit tuple needed.
//! - **provenance-based revocation** — a revoke tombstone is honored only when its
//!   author is the revoked grant's `grantor` or a superior (the creator, or an owner
//!   of the grant's path). An unauthorized revoke is disregarded and the grant stays
//!   effective. This is an evaluation-layer rule: the set still tombstones every
//!   revoke content-neutrally (slice 1), and authority is decided here, over the
//!   merged view — never rejected at merge.

use crdtsync_core::acl::{
    decide_capability_with_authority, evaluate_with_authority, AclActor, AclDecision, AclEffect,
    AclGrant, AclRecord, AclSubject, AclTuple, Capability,
};
use crdtsync_core::doc::Document;
use crdtsync_core::elementid::ElementId;
use crdtsync_core::path::encode_path;
use crdtsync_core::{ClientId, Op};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn actor(id: u8) -> AclActor {
    AclActor::new(cid(id))
}

fn cap(c: Capability) -> AclGrant {
    AclGrant::Capability(c)
}

/// A tuple with the given grantor and a throwaway id — the evaluator reads the
/// grantor (provenance) but never the id.
fn tup(subject: AclSubject, grant: AclGrant, path: Vec<u8>, grantor: ClientId) -> AclTuple {
    AclTuple {
        id: ElementId::from_bytes([0u8; 16]),
        subject,
        grant,
        effect: AclEffect::Allow,
        path,
        grantor,
    }
}

/// A live record (no revoke).
fn live(t: AclTuple) -> AclRecord {
    AclRecord {
        tuple: t,
        revoked_by: Vec::new(),
    }
}

/// A record tombstoned by the given revokers.
fn revoked(t: AclTuple, by: &[ClientId]) -> AclRecord {
    AclRecord {
        tuple: t,
        revoked_by: by.to_vec(),
    }
}

fn apply_all(d: &mut Document, ops: &[Op]) {
    for op in ops {
        d.apply(op);
    }
}

// ---- creator-auto-owns-`/` ------------------------------------------------

#[test]
fn the_creator_owns_the_root_with_no_explicit_tuple() {
    // A fresh doc: no tuples at all, yet the creator holds every capability at the
    // root and every descendant — the bootstrap authority root.
    let none: Vec<AclRecord> = Vec::new();
    let creator = cid(1);
    let root = encode_path(&[]);
    let deep = encode_path(&[b"doc", b"content", b"p1"]);
    for c in [
        Capability::Read,
        Capability::Write,
        Capability::PublishAwareness,
        Capability::Own,
    ] {
        assert!(
            evaluate_with_authority(&none, creator, &actor(1), &root, c),
            "creator holds {c:?} at /"
        );
        assert!(
            evaluate_with_authority(&none, creator, &actor(1), &deep, c),
            "creator holds {c:?} deep in the tree"
        );
    }
}

#[test]
fn a_non_creator_with_no_grant_is_denied() {
    // Default-deny is unchanged: only the creator is bootstrapped, everyone else
    // needs a grant.
    let none: Vec<AclRecord> = Vec::new();
    let creator = cid(1);
    let doc = encode_path(&[b"doc"]);
    for c in [Capability::Read, Capability::Write, Capability::Own] {
        assert!(!evaluate_with_authority(&none, creator, &actor(2), &doc, c));
    }
    assert_eq!(
        decide_capability_with_authority(&none, creator, &actor(2), &doc, Capability::Read),
        AclDecision::Abstain,
        "no tuple governs a non-creator — the tuple tier abstains"
    );
}

#[test]
fn an_owners_grant_confers_the_capability() {
    // The creator grants Read to Bob on /doc; Bob reads there and below, but not a
    // sibling — the grant composes with slice 2's inheritance.
    let creator = cid(1);
    let doc = encode_path(&[b"doc"]);
    let set = vec![live(tup(
        AclSubject::Actor(cid(2)),
        cap(Capability::Read),
        doc.clone(),
        creator,
    ))];
    assert!(evaluate_with_authority(
        &set,
        creator,
        &actor(2),
        &doc,
        Capability::Read
    ));
    assert!(evaluate_with_authority(
        &set,
        creator,
        &actor(2),
        &encode_path(&[b"doc", b"x"]),
        Capability::Read
    ));
    assert!(!evaluate_with_authority(
        &set,
        creator,
        &actor(2),
        &encode_path(&[b"other"]),
        Capability::Read
    ));
    // And the creator still owns everything.
    assert!(evaluate_with_authority(
        &set,
        creator,
        &actor(1),
        &doc,
        Capability::Own
    ));
}

// ---- provenance-based revocation ------------------------------------------

#[test]
fn a_revoke_by_the_grant_grantor_is_honored() {
    // Owner Carol (granted Own on /doc by the creator) grants Read to Bob; Carol,
    // the grantor, revokes it — honored, so Bob loses the read.
    let creator = cid(1);
    let doc = encode_path(&[b"doc"]);
    let set = vec![
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc.clone(),
            creator,
        )),
        revoked(
            tup(
                AclSubject::Actor(cid(2)),
                cap(Capability::Read),
                doc.clone(),
                cid(3),
            ),
            &[cid(3)],
        ),
    ];
    assert!(!evaluate_with_authority(
        &set,
        creator,
        &actor(2),
        &doc,
        Capability::Read
    ));
}

#[test]
fn a_revoke_by_a_superior_is_honored() {
    // Carol owns /doc and granted Bob Read; the creator (a superior above Carol in
    // the chain) revokes it — honored though the creator is not the grantor.
    let creator = cid(1);
    let doc = encode_path(&[b"doc"]);
    let set = vec![
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc.clone(),
            creator,
        )),
        revoked(
            tup(
                AclSubject::Actor(cid(2)),
                cap(Capability::Read),
                doc.clone(),
                cid(3),
            ),
            &[creator],
        ),
    ];
    assert!(!evaluate_with_authority(
        &set,
        creator,
        &actor(2),
        &doc,
        Capability::Read
    ));
}

#[test]
fn a_revoke_by_a_path_owner_above_the_grantor_is_honored() {
    // Carol owns /doc (from the creator) and delegates Own of /doc/sub to Dan; Dan
    // grants Bob Read on /doc/sub. Carol — owner of an ancestor and superior to
    // Dan's grant — revokes Bob's read. Honored: an owner of the grant's path is a
    // superior.
    let creator = cid(1);
    let doc = encode_path(&[b"doc"]);
    let sub = encode_path(&[b"doc", b"sub"]);
    let set = vec![
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc.clone(),
            creator,
        )),
        live(tup(
            AclSubject::Actor(cid(4)),
            cap(Capability::Own),
            sub.clone(),
            cid(3),
        )),
        revoked(
            tup(
                AclSubject::Actor(cid(2)),
                cap(Capability::Read),
                sub.clone(),
                cid(4),
            ),
            &[cid(3)],
        ),
    ];
    assert!(!evaluate_with_authority(
        &set,
        creator,
        &actor(2),
        &sub,
        Capability::Read
    ));
}

#[test]
fn a_revoke_by_an_unrelated_actor_is_ignored() {
    // THE security case: an attacker with no grant and no ownership revokes Bob's
    // read. The revoke is disregarded — the grant stays effective. An attacker's
    // tombstone must never strip a legitimate grant.
    let creator = cid(1);
    let doc = encode_path(&[b"doc"]);
    let set = vec![
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc.clone(),
            creator,
        )),
        revoked(
            tup(
                AclSubject::Actor(cid(2)),
                cap(Capability::Read),
                doc.clone(),
                cid(3),
            ),
            &[cid(5)], // cid(5): no grant, no ownership
        ),
    ];
    assert!(
        evaluate_with_authority(&set, creator, &actor(2), &doc, Capability::Read),
        "an unauthorized revoke does not strip the grant"
    );
}

#[test]
fn an_unauthorized_revoke_alongside_an_authorized_one_still_revokes() {
    // The tuple carries two revokers: an attacker (ignored) and the grantor
    // (honored). One authorized revoke is enough to tombstone the grant.
    let creator = cid(1);
    let doc = encode_path(&[b"doc"]);
    let set = vec![revoked(
        tup(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            doc.clone(),
            creator, // grantor is the creator
        ),
        &[cid(5), creator], // attacker + grantor
    )];
    assert!(!evaluate_with_authority(
        &set,
        creator,
        &actor(2),
        &doc,
        Capability::Read
    ));
}

#[test]
fn an_unauthorized_revoke_of_an_owner_grant_leaves_ownership_intact() {
    // An attacker revoking an owner's Own grant is ignored — the owner keeps the
    // whole implied lattice.
    let creator = cid(1);
    let doc = encode_path(&[b"doc"]);
    let set = vec![revoked(
        tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc.clone(),
            creator,
        ),
        &[cid(5)],
    )];
    for c in [Capability::Read, Capability::Write, Capability::Own] {
        assert!(evaluate_with_authority(&set, creator, &actor(3), &doc, c));
    }
}

#[test]
fn an_own_granted_to_a_class_confers_no_revoke_authority() {
    // Own is granted to the `Anyone`/`Authenticated` class, then Bob is granted
    // Read. An arbitrary actor — who is not the grantor, not the creator, and holds
    // no *actor-id* Own grant — revokes Bob's read. The class-subject ownership must
    // not make that revoke authoritative (else anyone could strip any grant): the
    // revoke is disregarded and Bob keeps Read.
    let creator = cid(1);
    let doc = encode_path(&[b"doc"]);
    for class in [AclSubject::Anyone, AclSubject::Authenticated] {
        let set = vec![
            live(tup(
                class.clone(),
                cap(Capability::Own),
                doc.clone(),
                creator,
            )),
            revoked(
                tup(
                    AclSubject::Actor(cid(2)),
                    cap(Capability::Read),
                    doc.clone(),
                    creator,
                ),
                &[cid(5)], // arbitrary actor, no actor-id ownership
            ),
        ];
        assert!(
            evaluate_with_authority(&set, creator, &actor(2), &doc, Capability::Read),
            "class-subject Own must not confer revoke authority ({class:?})"
        );
    }
}

#[test]
fn an_own_granted_to_a_group_confers_no_revoke_authority() {
    // Group ownership cannot confer revoke authority: core sees only the revoker's
    // id (the op author), not its group membership, so a group-Own revoke is
    // disregarded (fail-closed) and the grant stays effective.
    let creator = cid(1);
    let doc = encode_path(&[b"doc"]);
    let set = vec![
        live(tup(
            AclSubject::Group(b"admins".to_vec()),
            cap(Capability::Own),
            doc.clone(),
            creator,
        )),
        revoked(
            tup(
                AclSubject::Actor(cid(2)),
                cap(Capability::Read),
                doc.clone(),
                creator,
            ),
            &[cid(5)], // a member in truth, but core cannot know that
        ),
    ];
    assert!(evaluate_with_authority(
        &set,
        creator,
        &actor(2),
        &doc,
        Capability::Read
    ));
}

// ---- determinism ----------------------------------------------------------

#[test]
fn the_decision_is_independent_of_record_order() {
    let creator = cid(1);
    let doc = encode_path(&[b"doc"]);
    let sub = encode_path(&[b"doc", b"sub"]);
    let mut set = vec![
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc.clone(),
            creator,
        )),
        revoked(
            tup(
                AclSubject::Actor(cid(2)),
                cap(Capability::Read),
                doc.clone(),
                cid(3),
            ),
            &[cid(3)], // grantor → honored
        ),
        revoked(
            tup(
                AclSubject::Actor(cid(4)),
                cap(Capability::Read),
                sub.clone(),
                cid(3),
            ),
            &[cid(9)], // unrelated → ignored
        ),
    ];
    let probe = |s: &[AclRecord]| {
        (
            evaluate_with_authority(s, creator, &actor(1), &doc, Capability::Own),
            evaluate_with_authority(s, creator, &actor(2), &doc, Capability::Read),
            evaluate_with_authority(s, creator, &actor(4), &sub, Capability::Read),
        )
    };
    let forward = probe(&set);
    set.reverse();
    let reversed = probe(&set);
    assert_eq!(forward, reversed);
    // creator owns; Bob's read honored-revoked away; Dan's read survives the bogus
    // revoke.
    assert_eq!(forward, (true, false, true));
}

// ---- convergence over Document-authored ops -------------------------------

#[test]
fn an_unauthorized_revoke_authored_across_replicas_is_ignored_and_converges() {
    // The creator grants Bob Read; an attacker replica revokes it. Two replicas
    // that merge the grant and the attacker's revoke in opposite orders both keep
    // Bob's read and evaluate identically.
    let doc = encode_path(&[b"doc"]);
    let creator = cid(1);

    let mut owner = Document::new(creator);
    let mut id = ElementId::from_bytes([0u8; 16]);
    let grant = owner.transact(|tx| {
        id = tx.acl().grant(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Allow,
            doc.clone(),
            creator,
        );
    });

    // The attacker sees the grant, then revokes it — a real op authored by cid(5).
    let mut attacker = Document::new(cid(5));
    apply_all(&mut attacker, &grant);
    let revoke = attacker.transact(|tx| tx.acl().revoke(id));
    assert!(!revoke.is_empty(), "revoke on a materialised tuple emits");

    let mut r1 = Document::new(cid(6));
    apply_all(&mut r1, &grant);
    apply_all(&mut r1, &revoke);
    let mut r2 = Document::new(cid(7));
    apply_all(&mut r2, &revoke); // revoke buffers until the grant lands
    apply_all(&mut r2, &grant);

    let rec1 = r1.acl_records();
    let rec2 = r2.acl_records();
    assert_eq!(rec1, rec2, "replicas converge on the record set");

    for recs in [&rec1, &rec2] {
        assert!(
            evaluate_with_authority(recs, creator, &actor(2), &doc, Capability::Read),
            "the attacker's revoke is disregarded — Bob still reads"
        );
    }
}

#[test]
fn an_authorized_revoke_authored_across_replicas_is_honored_and_converges() {
    // The creator grants Bob Read then revokes it; both replicas honor it (the
    // creator is the grantor) and converge.
    let doc = encode_path(&[b"doc"]);
    let creator = cid(1);

    let mut owner = Document::new(creator);
    let mut id = ElementId::from_bytes([0u8; 16]);
    let grant = owner.transact(|tx| {
        id = tx.acl().grant(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Allow,
            doc.clone(),
            creator,
        );
    });
    let revoke = owner.transact(|tx| tx.acl().revoke(id));

    let mut r1 = Document::new(cid(6));
    apply_all(&mut r1, &grant);
    apply_all(&mut r1, &revoke);
    let mut r2 = Document::new(cid(7));
    apply_all(&mut r2, &revoke);
    apply_all(&mut r2, &grant);

    let rec1 = r1.acl_records();
    let rec2 = r2.acl_records();
    assert_eq!(rec1, rec2);
    for recs in [&rec1, &rec2] {
        assert!(!evaluate_with_authority(
            recs,
            creator,
            &actor(2),
            &doc,
            Capability::Read
        ));
    }
}

// ---- totality -------------------------------------------------------------

#[test]
fn degenerate_inputs_are_total() {
    let none: Vec<AclRecord> = Vec::new();
    // No creator match, empty path, any capability: no panic, deny by default.
    assert!(!evaluate_with_authority(
        &none,
        cid(1),
        &actor(2),
        &[],
        Capability::Own
    ));
    // The creator on an empty path is owned (root).
    assert!(evaluate_with_authority(
        &none,
        cid(1),
        &actor(1),
        &[],
        Capability::Own
    ));
}
