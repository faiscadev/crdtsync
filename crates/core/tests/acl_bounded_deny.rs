//! Doc-level ACL — provenance-bounded deny (slice 3b-ii).
//!
//! 3b-i honored every `Deny` as-present (global deny-overrides): an unrooted or
//! cross-authority deny could suppress a peer's or a superior's grant — a backdoor
//! around revocation. 3b-ii bounds a deny to its author's authority. A `Deny`
//! suppresses a grant only when its author is **at or above the grant's grantor** in
//! the delegation hierarchy: the creator, the grantor itself, or a delegation
//! superior whose rooted `Own` the grantor's authority derives from. So:
//!
//! - a superior's deny binds a subordinate's grant (in-authority — the carve-out);
//! - a subordinate's or an unrelated peer's deny does NOT suppress a superior's or a
//!   peer's grant (out-of-authority — disregarded, the anti-backdoor property);
//! - a deny still beats an allow by the *same* authority (slice-2 deny-overrides);
//! - a `Deny(Own)` targeting the creator is disregarded (creator-deny immunity);
//! - a superior's `deny own` on a subpath carves the sub-region out of a
//!   subordinate's ownership (the superior carve-out).
//!
//! The bounding folds into the same order-independent authority fixpoint 3b-i
//! established: a deny whose author's ownership is itself contested resolves
//! deterministically and fail-closed (an ambiguous deny does not bind).

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

/// An allow tuple with the given grantor and a throwaway id.
fn allow(subject: AclSubject, grant: AclGrant, path: Vec<u8>, grantor: ClientId) -> AclTuple {
    AclTuple {
        id: ElementId::from_bytes([0u8; 16]),
        subject,
        grant,
        effect: AclEffect::Allow,
        path,
        grantor,
    }
}

/// A deny tuple whose `grantor` is its author.
fn deny(subject: AclSubject, grant: AclGrant, path: Vec<u8>, author: ClientId) -> AclTuple {
    AclTuple {
        id: ElementId::from_bytes([0u8; 16]),
        subject,
        grant,
        effect: AclEffect::Deny,
        path,
        grantor: author,
    }
}

fn live(t: AclTuple) -> AclRecord {
    AclRecord {
        tuple: t,
        revoked_by: Vec::new(),
    }
}

fn apply_all(d: &mut Document, ops: &[Op]) {
    for op in ops {
        d.apply(op);
    }
}

fn doc() -> Vec<u8> {
    encode_path(&[b"doc"])
}
fn sub() -> Vec<u8> {
    encode_path(&[b"doc", b"sub"])
}

// ---- in-authority deny binds (the carve-out direction) --------------------

#[test]
fn a_superior_deny_bounds_a_subordinate_grant() {
    // creator → A(Own /doc); A → B(Own /doc/sub); B → X(Read /doc/sub). A, a superior
    // of B, denies X's read. A is above the grant's grantor (B), so the deny binds and
    // X loses the read.
    let creator = cid(1);
    let set = vec![
        live(allow(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            creator,
        )),
        live(allow(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            sub(),
            cid(2),
        )),
        live(allow(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            sub(),
            cid(3),
        )),
        live(deny(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            sub(),
            cid(2), // A, a superior of the grantor B
        )),
    ];
    assert!(
        !evaluate_with_authority(&set, creator, &actor(4), &sub(), Capability::Read),
        "a superior's deny binds a subordinate's grant"
    );
}

#[test]
fn the_grantor_can_deny_its_own_grant() {
    // The grantor is at-or-above itself: B grants X read on /doc/sub, then B denies it.
    let creator = cid(1);
    let set = vec![
        live(allow(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            sub(),
            creator,
        )),
        live(allow(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            sub(),
            cid(3),
        )),
        live(deny(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            sub(),
            cid(3), // the grantor itself
        )),
    ];
    assert!(!evaluate_with_authority(
        &set,
        creator,
        &actor(4),
        &sub(),
        Capability::Read
    ));
}

#[test]
fn a_deny_beats_an_allow_by_the_same_authority() {
    // Slice-2 deny-overrides, preserved in the authority form: owner O grants X read on
    // /doc and denies it on the same path — the deny wins.
    let creator = cid(1);
    let set = vec![
        live(allow(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            creator,
        )),
        live(allow(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            doc(),
            cid(2),
        )),
        live(deny(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            doc(),
            cid(2),
        )),
    ];
    assert!(!evaluate_with_authority(
        &set,
        creator,
        &actor(4),
        &doc(),
        Capability::Read
    ));
    assert_eq!(
        decide_capability_with_authority(&set, creator, &actor(4), &doc(), Capability::Read),
        AclDecision::Deny
    );
}

#[test]
fn a_creator_deny_binds_everyone() {
    // The creator is above everyone: a creator deny of X's read (granted by an owner)
    // is honored.
    let creator = cid(1);
    let set = vec![
        live(allow(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            creator,
        )),
        live(allow(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            doc(),
            cid(2),
        )),
        live(deny(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            doc(),
            creator,
        )),
    ];
    assert!(!evaluate_with_authority(
        &set,
        creator,
        &actor(4),
        &doc(),
        Capability::Read
    ));
}

// ---- out-of-authority deny disregarded (the anti-backdoor property) --------

#[test]
fn a_subordinate_deny_does_not_suppress_a_superior_grant() {
    // THE anti-backdoor case. The creator grants X read on /doc. A subordinate B (an
    // owner of only /doc/sub) denies X's read. B is below the grant's grantor (the
    // creator), so the deny is disregarded — X keeps the read, at /doc and in B's own
    // subtree /doc/sub.
    let creator = cid(1);
    let set = vec![
        live(allow(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            doc(),
            creator,
        )),
        live(allow(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            sub(),
            creator,
        )),
        // B denies X's read on /doc/sub — below the creator's grant.
        live(deny(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            sub(),
            cid(3),
        )),
    ];
    assert!(
        evaluate_with_authority(&set, creator, &actor(4), &sub(), Capability::Read),
        "a subordinate cannot use a deny to carve out a superior's grant"
    );
    assert!(evaluate_with_authority(
        &set,
        creator,
        &actor(4),
        &doc(),
        Capability::Read
    ));
}

#[test]
fn an_unrooted_deny_by_a_non_owner_is_disregarded() {
    // 3b-i honored this as-present; 3b-ii bounds it. The creator grants Bob read on
    // /doc; cid(9), who owns nothing, denies it. cid(9) is not at-or-above the grantor
    // (the creator), so the deny is disregarded and Bob keeps the read — the same rule
    // an unauthorized *revoke* already gets.
    let creator = cid(1);
    let set = vec![
        live(allow(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            doc(),
            creator,
        )),
        live(deny(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            doc(),
            cid(9),
        )),
    ];
    assert!(
        evaluate_with_authority(&set, creator, &actor(2), &doc(), Capability::Read),
        "an out-of-authority deny does not strip a rooted grant"
    );
}

#[test]
fn a_peer_deny_does_not_suppress_a_peer_grant() {
    // A and B are independent creator-appointed co-owners of /doc. B grants X read; A
    // denies it. A is neither the creator, the grantor, nor a delegation superior of B
    // (both derive straight from the creator), so A's deny is disregarded.
    let creator = cid(1);
    let base = vec![
        live(allow(
            AclSubject::Actor(cid(2)), // A
            cap(Capability::Own),
            doc(),
            creator,
        )),
        live(allow(
            AclSubject::Actor(cid(3)), // B
            cap(Capability::Own),
            doc(),
            creator,
        )),
        live(allow(
            AclSubject::Actor(cid(4)), // X, granted by B
            cap(Capability::Read),
            doc(),
            cid(3),
        )),
    ];
    let mut peer_denies = base.clone();
    peer_denies.push(live(deny(
        AclSubject::Actor(cid(4)),
        cap(Capability::Read),
        doc(),
        cid(2), // A, a peer of the grantor B
    )));
    assert!(
        evaluate_with_authority(&peer_denies, creator, &actor(4), &doc(), Capability::Read),
        "a peer cannot suppress another peer's grant"
    );

    // Control: the grantor B *can* deny it.
    let mut grantor_denies = base;
    grantor_denies.push(live(deny(
        AclSubject::Actor(cid(4)),
        cap(Capability::Read),
        doc(),
        cid(3), // B, the grantor
    )));
    assert!(!evaluate_with_authority(
        &grantor_denies,
        creator,
        &actor(4),
        &doc(),
        Capability::Read
    ));
}

// ---- creator-deny immunity ------------------------------------------------

#[test]
fn a_deny_own_targeting_the_creator_is_disregarded() {
    // creator-owns-`/` is a synthetic `Own`; a naive deny-overrides would let a
    // subordinate's `Deny(Own)` cancel it. It must not — no one is above the creator.
    let creator = cid(1);
    let set = vec![
        live(allow(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            creator,
        )),
        // A subordinate purports to deny the creator's ownership of the root.
        live(deny(
            AclSubject::Actor(creator),
            cap(Capability::Own),
            encode_path(&[]),
            cid(2),
        )),
    ];
    for c in [Capability::Read, Capability::Write, Capability::Own] {
        assert!(
            evaluate_with_authority(&set, creator, &actor(1), &doc(), c),
            "the creator's root authority cannot be denied from below ({c:?})"
        );
    }
}

#[test]
fn a_subordinate_cannot_deny_the_creator_any_capability() {
    // Full creator immunity: a subordinate's `Deny(Read)` on the creator is disregarded.
    let creator = cid(1);
    let set = vec![
        live(allow(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            creator,
        )),
        live(deny(
            AclSubject::Actor(creator),
            cap(Capability::Read),
            doc(),
            cid(2),
        )),
    ];
    assert!(evaluate_with_authority(
        &set,
        creator,
        &actor(1),
        &doc(),
        Capability::Read
    ));
}

// ---- superior carve-out (deny own on a subpath) ---------------------------

#[test]
fn a_superior_deny_own_carves_a_subpath_out_of_a_subordinates_ownership() {
    // creator → A(Own /doc); A → B(Own /doc/sub). A carves /doc/sub back out with a
    // `Deny(Own)` targeting B. B loses Own — and the implied read/write — on /doc/sub
    // and everything below it, while A's ownership stands.
    let creator = cid(1);
    let set = vec![
        live(allow(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            creator,
        )),
        live(allow(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            sub(),
            cid(2),
        )),
        live(deny(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            sub(),
            cid(2), // A, B's superior
        )),
    ];
    for c in [Capability::Read, Capability::Write, Capability::Own] {
        assert!(
            !evaluate_with_authority(&set, creator, &actor(3), &sub(), c),
            "the carve-out strips B's {c:?} on /doc/sub"
        );
        assert!(
            !evaluate_with_authority(
                &set,
                creator,
                &actor(3),
                &encode_path(&[b"doc", b"sub", b"deep"]),
                c
            ),
            "and everywhere below it ({c:?})"
        );
    }
    // A keeps ownership of /doc, and B never owned /doc.
    assert!(evaluate_with_authority(
        &set,
        creator,
        &actor(2),
        &doc(),
        Capability::Own
    ));
}

#[test]
fn deny_own_leaves_a_separately_granted_capability() {
    // Capability separation carries into the authority form: A carves out B's Own on
    // /doc/sub, but a direct Read that A also granted B stands.
    let creator = cid(1);
    let set = vec![
        live(allow(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            creator,
        )),
        live(allow(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            sub(),
            cid(2),
        )),
        live(allow(
            AclSubject::Actor(cid(3)),
            cap(Capability::Read),
            sub(),
            cid(2),
        )),
        live(deny(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            sub(),
            cid(2),
        )),
    ];
    assert!(
        !evaluate_with_authority(&set, creator, &actor(3), &sub(), Capability::Write),
        "the Own-implied write is gone"
    );
    assert!(
        !evaluate_with_authority(&set, creator, &actor(3), &sub(), Capability::Own),
        "and Own itself"
    );
    assert!(
        evaluate_with_authority(&set, creator, &actor(3), &sub(), Capability::Read),
        "but the direct Read allow stands"
    );
}

// ---- determinism + termination on contested / cyclic deny provenance -------

#[test]
fn a_forged_owners_deny_confers_no_authority() {
    // A forged ownership cycle (X↔Y own /doc, rooting at no creator) cannot lend a deny
    // any authority: X denies Z's creator-granted read, but X is not at-or-above the
    // creator, so Z keeps the read. The walk terminates on the cycle.
    let creator = cid(1);
    let set = vec![
        live(allow(
            AclSubject::Actor(cid(5)), // Z, from the creator
            cap(Capability::Read),
            doc(),
            creator,
        )),
        live(allow(
            AclSubject::Actor(cid(2)), // X, forged by Y
            cap(Capability::Own),
            doc(),
            cid(3),
        )),
        live(allow(
            AclSubject::Actor(cid(3)), // Y, forged by X
            cap(Capability::Own),
            doc(),
            cid(2),
        )),
        live(deny(
            AclSubject::Actor(cid(5)),
            cap(Capability::Read),
            doc(),
            cid(2), // X, a forged owner
        )),
    ];
    assert!(
        evaluate_with_authority(&set, creator, &actor(5), &doc(), Capability::Read),
        "a forged owner's deny binds nothing"
    );
}

#[test]
fn a_long_delegation_chain_deny_terminates() {
    // A deep legitimate chain creator → a10 → a11 → … → a68(Own), then a leaf read, with
    // a deny by a mid-chain superior. Exercises the at-or-above walk's depth under Miri.
    let creator = cid(1);
    let mut set = Vec::new();
    let mut prev = creator;
    for i in 10u8..69 {
        set.push(live(allow(
            AclSubject::Actor(cid(i)),
            cap(Capability::Own),
            doc(),
            prev,
        )));
        prev = cid(i);
    }
    // The leaf a68 grants X read; a mid-chain owner a40 (above a68) denies it.
    set.push(live(allow(
        AclSubject::Actor(cid(90)),
        cap(Capability::Read),
        doc(),
        cid(68),
    )));
    set.push(live(deny(
        AclSubject::Actor(cid(90)),
        cap(Capability::Read),
        doc(),
        cid(40),
    )));
    assert!(
        !evaluate_with_authority(&set, creator, &actor(90), &doc(), Capability::Read),
        "a superior deep in a long chain still binds the leaf grant"
    );
    // A peer off the chain (never an owner) cannot.
    let mut peer = set.clone();
    peer.pop();
    peer.push(live(deny(
        AclSubject::Actor(cid(90)),
        cap(Capability::Read),
        doc(),
        cid(200),
    )));
    assert!(evaluate_with_authority(
        &peer,
        creator,
        &actor(90),
        &doc(),
        Capability::Read
    ));
}

#[test]
fn a_deny_whose_authority_rides_a_revocation_cycle_is_deterministic() {
    // D and C are a self-undermining revoke cycle (creator → D(Own); D → C(Own); C
    // revokes D). The fixpoint disregards the contested revoke, so D and C stay owners.
    // C, now a settled owner, denies X's read that C itself granted — an in-authority
    // deny that must bind, deterministically and regardless of record order.
    let creator = cid(1);
    let d_own = AclRecord {
        tuple: allow(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            creator,
        ),
        revoked_by: vec![cid(3)], // revoked by C — a self-undermining cycle
    };
    let mut set = vec![
        d_own,
        live(allow(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc(),
            cid(2),
        )),
        live(allow(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            doc(),
            cid(3),
        )),
        live(deny(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            doc(),
            cid(3), // C, the grantor
        )),
    ];
    let probe = |s: &[AclRecord]| {
        (
            evaluate_with_authority(s, creator, &actor(2), &doc(), Capability::Own),
            evaluate_with_authority(s, creator, &actor(3), &doc(), Capability::Own),
            evaluate_with_authority(s, creator, &actor(4), &doc(), Capability::Read),
        )
    };
    let forward = probe(&set);
    set.reverse();
    let reversed = probe(&set);
    assert_eq!(
        forward, reversed,
        "the verdict is independent of record order"
    );
    assert_eq!(forward, (true, true, false));
}

#[test]
fn the_decision_is_independent_of_record_order() {
    // A superior deny, an out-of-authority deny, and a peer grant, in one set — reversing
    // the order changes no verdict.
    let creator = cid(1);
    let mut set = vec![
        live(allow(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            creator,
        )),
        live(allow(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            sub(),
            cid(2),
        )),
        live(allow(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            sub(),
            cid(3),
        )),
        // In-authority deny (A above B) — binds.
        live(deny(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            sub(),
            cid(2),
        )),
        // Out-of-authority deny of a creator grant — disregarded.
        live(allow(
            AclSubject::Actor(cid(5)),
            cap(Capability::Read),
            doc(),
            creator,
        )),
        live(deny(
            AclSubject::Actor(cid(5)),
            cap(Capability::Read),
            doc(),
            cid(3),
        )),
    ];
    let probe = |s: &[AclRecord]| {
        (
            evaluate_with_authority(s, creator, &actor(4), &sub(), Capability::Read),
            evaluate_with_authority(s, creator, &actor(5), &doc(), Capability::Read),
        )
    };
    let forward = probe(&set);
    set.reverse();
    let reversed = probe(&set);
    assert_eq!(forward, reversed);
    assert_eq!(forward, (false, true));
}

// ---- convergence over Document-authored ops -------------------------------

#[test]
fn an_out_of_authority_deny_authored_across_replicas_is_disregarded_and_converges() {
    // The creator grants X read; a subordinate owner of /doc/sub denies it. Two replicas
    // merge the grant and the deny in opposite orders — both keep X's read and decide
    // identically.
    let creator = cid(1);
    let d = doc();

    let mut owner = Document::new(creator);
    let grant = owner.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            AclEffect::Allow,
            d.clone(),
            creator,
        );
        tx.acl().grant(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            AclEffect::Allow,
            sub(),
            creator,
        );
    });

    // The subordinate B (cid 3) denies X's read on its subtree — a real op authored by B.
    let mut b = Document::new(cid(3));
    apply_all(&mut b, &grant);
    let deny_op = b.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            AclEffect::Deny,
            sub(),
            cid(3),
        );
    });

    let mut r1 = Document::new(cid(6));
    apply_all(&mut r1, &grant);
    apply_all(&mut r1, &deny_op);
    let mut r2 = Document::new(cid(7));
    apply_all(&mut r2, &deny_op);
    apply_all(&mut r2, &grant);

    let rec1 = r1.acl_records();
    let rec2 = r2.acl_records();
    assert_eq!(rec1, rec2, "replicas converge on the record set");
    for recs in [&rec1, &rec2] {
        assert!(
            evaluate_with_authority(recs, creator, &actor(4), &sub(), Capability::Read),
            "the subordinate's deny is disregarded — X still reads on both replicas"
        );
    }
}

#[test]
fn an_in_authority_deny_authored_across_replicas_binds_and_converges() {
    // The creator grants X read then denies it — an in-authority deny. Both replicas
    // honor it and converge.
    let creator = cid(1);
    let d = doc();

    let mut owner = Document::new(creator);
    let grant = owner.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            AclEffect::Allow,
            d.clone(),
            creator,
        );
    });
    let deny_op = owner.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            AclEffect::Deny,
            d.clone(),
            creator,
        );
    });

    let mut r1 = Document::new(cid(6));
    apply_all(&mut r1, &grant);
    apply_all(&mut r1, &deny_op);
    let mut r2 = Document::new(cid(7));
    apply_all(&mut r2, &deny_op);
    apply_all(&mut r2, &grant);

    let rec1 = r1.acl_records();
    let rec2 = r2.acl_records();
    assert_eq!(rec1, rec2);
    for recs in [&rec1, &rec2] {
        assert!(!evaluate_with_authority(
            recs,
            creator,
            &actor(4),
            &d,
            Capability::Read
        ));
    }
}
