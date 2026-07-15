//! Doc-level ACL — attenuated recursive delegation chain (slice 3b-i).
//!
//! Authority is a recursive, attenuated walk up the grant chain to the creator
//! root. A grant confers authority only if its grantor validly held that authority:
//! an `Own` grant by `G` on path `P` is valid only if `G` themselves validly owned
//! `P` (or an ancestor) — recursively, up to the creator, the un-granted root of all
//! authority. A grant whose chain does not root at the creator is invalid and confers
//! nothing (self-granted or forged authority is inert). Attenuation: to write any
//! tuple on a path an actor must validly own it (or an ancestor), so a non-owner
//! cannot delegate. Recursive revoke-authority closes slice 3a's hole: an owner whose
//! ownership was itself authoritatively revoked can no longer authorize a revoke. The
//! walk terminates on any tuple graph — a forged cycle roots at no creator and
//! confers nothing.

use crdtsync_core::acl::{
    AclActor, AclEffect, AclGrant, AclRecord, AclScope, AclSubject, AclTuple, Capability,
};
use crdtsync_core::doc::Document;
use crdtsync_core::elementid::ElementId;
use crdtsync_core::path::encode_path;
use crdtsync_core::{ClientId, Op};

// These path-scoped delegation tests carry no element scopes, so they resolve
// nothing — a `Path` scope never consults the resolver. Element-scoped rooting has
// its own coverage in `acl_element.rs`.
fn evaluate_with_authority(
    records: &[AclRecord],
    creator: ClientId,
    actor: &AclActor,
    path: &[u8],
    capability: Capability,
) -> bool {
    crdtsync_core::acl::evaluate_with_authority(records, creator, actor, path, capability, &|_| {
        None
    })
}

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
fn tup(subject: AclSubject, grant: AclGrant, path: Vec<u8>, grantor: ClientId) -> AclTuple {
    AclTuple {
        id: ElementId::from_bytes([0u8; 16]),
        subject,
        grant,
        effect: AclEffect::Allow,
        scope: AclScope::Path(path),
        grantor,
    }
}

fn live(t: AclTuple) -> AclRecord {
    AclRecord {
        tuple: t,
        revoked_by: Vec::new(),
    }
}

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

/// creator=1, doc=/doc, sub=/doc/sub — the common fixtures.
fn doc() -> Vec<u8> {
    encode_path(&[b"doc"])
}
fn sub() -> Vec<u8> {
    encode_path(&[b"doc", b"sub"])
}

// ---- rooting at the creator -----------------------------------------------

#[test]
fn the_creators_grant_roots_immediately() {
    // The creator is the un-granted root: a grant they author is valid with no
    // further chain, and confers its capability.
    let creator = cid(1);
    let set = vec![live(tup(
        AclSubject::Actor(cid(2)),
        cap(Capability::Read),
        doc(),
        creator,
    ))];
    assert!(evaluate_with_authority(
        &set,
        creator,
        &actor(2),
        &doc(),
        Capability::Read
    ));
}

#[test]
fn an_owner_from_the_creator_can_delegate_own() {
    // The creator makes A an owner of /doc; A (a valid owner) delegates Own to B. B's
    // grant roots through A through the creator, so B holds the whole owner lattice.
    let creator = cid(1);
    let set = vec![
        live(tup(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            creator,
        )),
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc(),
            cid(2),
        )),
    ];
    for c in [Capability::Read, Capability::Write, Capability::Own] {
        assert!(
            evaluate_with_authority(&set, creator, &actor(3), &doc(), c),
            "B, validly delegated Own, holds {c:?}"
        );
    }
}

#[test]
fn a_multi_hop_chain_confers_the_leaf_capability() {
    // creator → A(Own) → B(Own) → Carol(Read). Every link roots, so Carol reads.
    let creator = cid(1);
    let set = vec![
        live(tup(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            creator,
        )),
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc(),
            cid(2),
        )),
        live(tup(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            doc(),
            cid(3),
        )),
    ];
    assert!(evaluate_with_authority(
        &set,
        creator,
        &actor(4),
        &doc(),
        Capability::Read
    ));
}

// ---- unrooted grants confer nothing ---------------------------------------

#[test]
fn a_non_owners_grant_confers_nothing() {
    // X owns nothing, yet purports to grant Read (and Own) to Bob. Neither roots at
    // the creator — the grants are inert.
    let creator = cid(1);
    let set = vec![
        live(tup(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            doc(),
            cid(9), // grantor cid(9): no ownership anywhere
        )),
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc(),
            cid(9),
        )),
    ];
    assert!(!evaluate_with_authority(
        &set,
        creator,
        &actor(2),
        &doc(),
        Capability::Read
    ));
    // And the Own grant confers nothing either — Bob(3) is not an owner.
    for c in [Capability::Read, Capability::Own] {
        assert!(!evaluate_with_authority(
            &set,
            creator,
            &actor(3),
            &doc(),
            c
        ));
    }
}

#[test]
fn a_reader_cannot_delegate() {
    // Attenuation: only ownership confers granting power. A holds only Read on /doc
    // (from the creator), then purports to grant Read to Bob. A is not an owner, so
    // its grant does not root — Bob gets nothing.
    let creator = cid(1);
    let set = vec![
        live(tup(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            doc(),
            creator,
        )),
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Read),
            doc(),
            cid(2), // A (a mere reader) tries to delegate
        )),
    ];
    assert!(
        evaluate_with_authority(&set, creator, &actor(2), &doc(), Capability::Read),
        "A's own read (from the creator) stands"
    );
    assert!(
        !evaluate_with_authority(&set, creator, &actor(3), &doc(), Capability::Read),
        "a reader cannot delegate — Bob's grant does not root"
    );
}

#[test]
fn cannot_grant_own_above_ones_ownership() {
    // Attenuation on scope: A owns /doc/sub (from the creator) and purports to grant
    // Own on /doc — broader than A's authority. A does not own /doc, so the grant
    // does not root; B is not an owner of /doc.
    let creator = cid(1);
    let set = vec![
        live(tup(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            sub(),
            creator,
        )),
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc(), // above A's /doc/sub ownership
            cid(2),
        )),
    ];
    assert!(
        !evaluate_with_authority(&set, creator, &actor(3), &doc(), Capability::Own),
        "a grant above the granter's ownership does not root"
    );
    // But A can validly delegate downward, within its subtree.
    let ok = vec![
        live(tup(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            sub(),
            creator,
        )),
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Read),
            sub(),
            cid(2),
        )),
    ];
    assert!(evaluate_with_authority(
        &ok,
        creator,
        &actor(3),
        &sub(),
        Capability::Read
    ));
}

#[test]
fn a_broken_link_invalidates_everything_below_it() {
    // B(Own) is granted by A, and Carol(Read) by B — but A never held Own. The chain
    // has a broken link at A, so B and Carol are both unrooted.
    let creator = cid(1);
    let broken = vec![
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc(),
            cid(2), // grantor A — but A owns nothing
        )),
        live(tup(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            doc(),
            cid(3),
        )),
    ];
    assert!(!evaluate_with_authority(
        &broken,
        creator,
        &actor(3),
        &doc(),
        Capability::Own
    ));
    assert!(!evaluate_with_authority(
        &broken,
        creator,
        &actor(4),
        &doc(),
        Capability::Read
    ));

    // Repair the link — give A a creator-rooted Own — and the whole chain lights up.
    let mut fixed = broken.clone();
    fixed.insert(
        0,
        live(tup(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            creator,
        )),
    );
    assert!(evaluate_with_authority(
        &fixed,
        creator,
        &actor(4),
        &doc(),
        Capability::Read
    ));
}

// ---- recursive revoke authority (the 3a hole) -----------------------------

#[test]
fn a_revoked_owner_can_no_longer_authorize_a_revoke() {
    // THE slice-3a hole. A owns /doc (from the creator). The creator grants Bob Read
    // on /doc (grantor: the creator, NOT A). A revokes Bob's read — under 3a honored
    // because A owns the path.
    let creator = cid(1);
    let a_own = |revoked_by: &[ClientId]| {
        if revoked_by.is_empty() {
            live(tup(
                AclSubject::Actor(cid(2)),
                cap(Capability::Own),
                doc(),
                creator,
            ))
        } else {
            revoked(
                tup(
                    AclSubject::Actor(cid(2)),
                    cap(Capability::Own),
                    doc(),
                    creator,
                ),
                revoked_by,
            )
        }
    };
    let bobs_read = revoked(
        tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Read),
            doc(),
            creator, // grantor is the creator, so A's authority is only via ownership
        ),
        &[cid(2)], // revoked by A
    );

    // Positive control: A still owns /doc, so A's ownership-based revoke is honored.
    let honored = vec![a_own(&[]), bobs_read.clone()];
    assert!(
        !evaluate_with_authority(&honored, creator, &actor(3), &doc(), Capability::Read),
        "a live owner's revoke is honored"
    );

    // The fix: the creator authoritatively revokes A's Own. A's ownership is gone, so
    // A's revoke of Bob's read is disregarded — Bob keeps Read.
    let closed = vec![a_own(&[creator]), bobs_read];
    assert!(
        evaluate_with_authority(&closed, creator, &actor(3), &doc(), Capability::Read),
        "a revoked owner can no longer authorize a revoke — Bob keeps Read"
    );
}

#[test]
fn revoking_an_owner_cascades_to_their_delegatees() {
    // creator → A(Own) → B(Own) → Carol(Read). The creator revokes A's Own. A's
    // ownership collapses, so B (granted by A) is no longer a valid owner, and Carol
    // (granted by B) loses her read — the whole delegated subtree falls.
    let creator = cid(1);
    let set = vec![
        revoked(
            tup(
                AclSubject::Actor(cid(2)),
                cap(Capability::Own),
                doc(),
                creator,
            ),
            &[creator],
        ),
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc(),
            cid(2),
        )),
        live(tup(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            doc(),
            cid(3),
        )),
    ];
    assert!(!evaluate_with_authority(
        &set,
        creator,
        &actor(3),
        &doc(),
        Capability::Own
    ));
    assert!(!evaluate_with_authority(
        &set,
        creator,
        &actor(4),
        &doc(),
        Capability::Read
    ));
}

// ---- termination on a forged cycle ----------------------------------------

#[test]
fn a_cycle_of_mutually_granting_tuples_confers_nothing() {
    // X's Own is granted by Y, Y's Own by X — a closed loop that roots at no creator.
    // The walk must terminate (no infinite loop / stack overflow) and confer nothing.
    let creator = cid(1);
    let set = vec![
        live(tup(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            cid(3), // X granted by Y
        )),
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc(),
            cid(2), // Y granted by X
        )),
        live(tup(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            doc(),
            cid(2), // Bob granted by the forged owner X
        )),
    ];
    for a in [2u8, 3, 4] {
        assert!(
            !evaluate_with_authority(&set, creator, &actor(a), &doc(), Capability::Own),
            "a forged cycle confers nothing to actor {a}"
        );
    }
    assert!(!evaluate_with_authority(
        &set,
        creator,
        &actor(4),
        &doc(),
        Capability::Read
    ));
    // The creator is untouched by the forgery.
    assert!(evaluate_with_authority(
        &set,
        creator,
        &actor(1),
        &doc(),
        Capability::Own
    ));
}

#[test]
fn a_long_forged_chain_terminates() {
    // A deep chain of mutually-granting non-creator tuples (no creator root anywhere)
    // must terminate — exercises the walk's bound under Miri without overflow.
    let creator = cid(1);
    let mut set = Vec::new();
    for i in 0u8..60 {
        // actor i+10 is granted Own by actor i+11 — a chain that never reaches the
        // creator, and the last link points back to the first (a long cycle).
        let grantor = if i == 59 { cid(10) } else { cid(i + 11) };
        set.push(live(tup(
            AclSubject::Actor(cid(i + 10)),
            cap(Capability::Own),
            doc(),
            grantor,
        )));
    }
    for i in 0u8..60 {
        assert!(!evaluate_with_authority(
            &set,
            creator,
            &actor(i + 10),
            &doc(),
            Capability::Own
        ));
    }
}

// ---- denies bind only within their author's authority (slice 3b-ii) --------

#[test]
fn an_out_of_authority_deny_no_longer_strips_a_rooted_grant() {
    // 3b-i honored this deny as-present; 3b-ii bounds it. Bob reads /doc from a
    // creator-rooted allow. A deny by cid(9) — who owns nothing and is not at-or-above
    // the grant's grantor (the creator) — is disregarded, the same rule an unauthorized
    // revoke gets. The full bounded-deny suite lives in `acl_bounded_deny`.
    let creator = cid(1);
    let set = vec![
        live(tup(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            doc(),
            creator,
        )),
        AclRecord {
            tuple: AclTuple {
                id: ElementId::from_bytes([0u8; 16]),
                subject: AclSubject::Actor(cid(2)),
                grant: cap(Capability::Read),
                effect: AclEffect::Deny,
                scope: AclScope::Path(doc()),
                grantor: cid(9),
            },
            revoked_by: Vec::new(),
        },
    ];
    assert!(
        evaluate_with_authority(&set, creator, &actor(2), &doc(), Capability::Read),
        "an out-of-authority deny does not strip a rooted grant"
    );
}

// ---- delegation+revocation cycles resolve fail-closed ----------------------

#[test]
fn a_derived_owner_cannot_revoke_the_owner_it_derives_from() {
    // creator → D(Own); D → C(Own); C revokes D's Own. C's authority to revoke derives
    // from D through the very grant it severs — a self-undermining cycle. The revoke is
    // disregarded (fail closed): D stays an owner, and C, derived from D, stays one too.
    let creator = cid(1);
    let set = vec![
        revoked(
            tup(
                AclSubject::Actor(cid(2)), // D
                cap(Capability::Own),
                doc(),
                creator,
            ),
            &[cid(3)], // revoked by C
        ),
        live(tup(
            AclSubject::Actor(cid(3)), // C
            cap(Capability::Own),
            doc(),
            cid(2), // granted by D
        )),
    ];
    assert!(
        evaluate_with_authority(&set, creator, &actor(2), &doc(), Capability::Own),
        "D's creator-rooted ownership survives its subordinate's self-undermining revoke"
    );
    assert!(evaluate_with_authority(
        &set,
        creator,
        &actor(3),
        &doc(),
        Capability::Own
    ));
}

#[test]
fn co_owners_cannot_revoke_each_other() {
    // A and B are both independently creator-appointed owners of /doc; each revokes the
    // other. Neither is above the other in the chain, so the mutual revocation cannot
    // be grounded — both revokes are disregarded and both keep ownership.
    let creator = cid(1);
    let set = vec![
        revoked(
            tup(
                AclSubject::Actor(cid(2)), // A
                cap(Capability::Own),
                doc(),
                creator,
            ),
            &[cid(3)], // revoked by B
        ),
        revoked(
            tup(
                AclSubject::Actor(cid(3)), // B
                cap(Capability::Own),
                doc(),
                creator,
            ),
            &[cid(2)], // revoked by A
        ),
    ];
    for a in [2u8, 3] {
        assert!(
            evaluate_with_authority(&set, creator, &actor(a), &doc(), Capability::Own),
            "co-owner {a} keeps ownership — peers cannot revoke each other"
        );
    }
}

// ---- determinism + convergence --------------------------------------------

#[test]
fn the_decision_is_independent_of_record_order() {
    // A valid multi-hop chain plus a revoked owner and a forged grant — reversing the
    // record order must not change any verdict.
    let creator = cid(1);
    let mut set = vec![
        live(tup(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            doc(),
            creator,
        )),
        live(tup(
            AclSubject::Actor(cid(3)),
            cap(Capability::Own),
            doc(),
            cid(2),
        )),
        live(tup(
            AclSubject::Actor(cid(4)),
            cap(Capability::Read),
            doc(),
            cid(3),
        )),
        // A forged grant that never roots.
        live(tup(
            AclSubject::Actor(cid(5)),
            cap(Capability::Own),
            doc(),
            cid(9),
        )),
    ];
    let probe = |s: &[AclRecord]| {
        (
            evaluate_with_authority(s, creator, &actor(3), &doc(), Capability::Own),
            evaluate_with_authority(s, creator, &actor(4), &doc(), Capability::Read),
            evaluate_with_authority(s, creator, &actor(5), &doc(), Capability::Own),
        )
    };
    let forward = probe(&set);
    set.reverse();
    let reversed = probe(&set);
    assert_eq!(forward, reversed);
    assert_eq!(forward, (true, true, false));
}

#[test]
fn a_delegation_chain_converges_across_replicas() {
    // The creator makes A an owner; A delegates Read to Bob. Two replicas merge the
    // grant chain in opposite orders and decide identically.
    let creator = cid(1);

    let mut owner = Document::new(creator);
    let a_own = owner.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(2)),
            cap(Capability::Own),
            AclEffect::Allow,
            doc(),
            creator,
        );
    });

    // A, now an owner, delegates Read to Bob (a real op authored by A).
    let mut a = Document::new(cid(2));
    apply_all(&mut a, &a_own);
    let a_grant = a.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(3)),
            cap(Capability::Read),
            AclEffect::Allow,
            doc(),
            cid(2),
        );
    });

    let mut r1 = Document::new(cid(6));
    apply_all(&mut r1, &a_own);
    apply_all(&mut r1, &a_grant);
    let mut r2 = Document::new(cid(7));
    apply_all(&mut r2, &a_grant); // arrives before the grant it roots from
    apply_all(&mut r2, &a_own);

    let rec1 = r1.acl_records();
    let rec2 = r2.acl_records();
    assert_eq!(rec1, rec2, "replicas converge on the record set");
    for recs in [&rec1, &rec2] {
        assert!(
            evaluate_with_authority(recs, creator, &actor(3), &doc(), Capability::Read),
            "Bob's read roots through A through the creator on both replicas"
        );
    }
}
