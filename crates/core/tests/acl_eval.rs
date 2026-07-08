//! Doc-level ACL — the evaluator (Doc-level ACL slice 2).
//!
//! A pure decision over the stored tuple set: does a subject hold a capability at
//! a path, and which roles does it effectively hold? Precedence is deny-overrides
//! (an explicit deny of a capability beats any allow, on the same or a broader
//! path), `Own` implies the read/write/publish-awareness lattice, roles inherit
//! downward, and the empty set denies by default. The evaluator reads tuples as
//! present — it never checks a grantor's authority (delegation is a later slice).

use crdtsync_core::acl::{
    decide_capability, effective_roles, evaluate, AclActor, AclDecision, AclEffect, AclGrant,
    AclSubject, AclTuple, Capability,
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

fn role(name: &[u8]) -> AclGrant {
    AclGrant::Role(name.to_vec())
}

/// A tuple with a throwaway id — the evaluator never reads the id, only the
/// subject / grant / effect / path.
fn tup(subject: AclSubject, grant: AclGrant, effect: AclEffect, path: Vec<u8>) -> AclTuple {
    AclTuple {
        id: ElementId::from_bytes([0u8; 16]),
        subject,
        grant,
        effect,
        path,
        grantor: cid(9),
    }
}

fn apply_all(d: &mut Document, ops: &[Op]) {
    for op in ops {
        d.apply(op);
    }
}

// ---- deny-by-default ------------------------------------------------------

#[test]
fn empty_set_denies_every_capability() {
    let none: Vec<AclTuple> = Vec::new();
    let p = encode_path(&[b"doc"]);
    for c in [
        Capability::Read,
        Capability::Write,
        Capability::PublishAwareness,
        Capability::Own,
    ] {
        assert!(!evaluate(&none, &actor(1), &p, c));
    }
    assert_eq!(
        decide_capability(&none, &actor(1), &p, Capability::Read),
        AclDecision::Abstain,
        "an empty set abstains — a lower tier or the default-deny decides"
    );
}

// ---- direct capability grants + prefix inheritance ------------------------

#[test]
fn a_direct_allow_confers_at_its_path_and_below() {
    let doc = encode_path(&[b"doc"]);
    let set = vec![tup(
        AclSubject::Actor(cid(1)),
        cap(Capability::Read),
        AclEffect::Allow,
        doc.clone(),
    )];

    // At the granted path and any descendant.
    assert!(evaluate(&set, &actor(1), &doc, Capability::Read));
    let below = encode_path(&[b"doc", b"content", b"p1"]);
    assert!(evaluate(&set, &actor(1), &below, Capability::Read));

    // Not for another actor, another capability, or a sibling subtree.
    assert!(!evaluate(&set, &actor(2), &doc, Capability::Read));
    assert!(!evaluate(&set, &actor(1), &doc, Capability::Write));
    let sibling = encode_path(&[b"other"]);
    assert!(!evaluate(&set, &actor(1), &sibling, Capability::Read));
}

#[test]
fn a_grant_does_not_leak_upward_to_an_ancestor() {
    // Granted on doc/content, not on doc: the ancestor is not covered.
    let content = encode_path(&[b"doc", b"content"]);
    let set = vec![tup(
        AclSubject::Actor(cid(1)),
        cap(Capability::Read),
        AclEffect::Allow,
        content.clone(),
    )];
    assert!(evaluate(&set, &actor(1), &content, Capability::Read));
    assert!(!evaluate(
        &set,
        &actor(1),
        &encode_path(&[b"doc"]),
        Capability::Read
    ));
}

// ---- deny-overrides -------------------------------------------------------

#[test]
fn a_deny_overrides_an_allow_at_the_same_scope() {
    let doc = encode_path(&[b"doc"]);
    let set = vec![
        tup(
            AclSubject::Actor(cid(1)),
            cap(Capability::Read),
            AclEffect::Allow,
            doc.clone(),
        ),
        tup(
            AclSubject::Actor(cid(1)),
            cap(Capability::Read),
            AclEffect::Deny,
            doc.clone(),
        ),
    ];
    assert!(!evaluate(&set, &actor(1), &doc, Capability::Read));
    assert_eq!(
        decide_capability(&set, &actor(1), &doc, Capability::Read),
        AclDecision::Deny
    );
}

#[test]
fn a_broader_deny_is_a_hard_floor_a_deeper_allow_cannot_reopen() {
    // deny read on doc, allow read on doc/content: at doc/content the deny wins —
    // path specificity is not a tiebreaker (AWS-style hard floor).
    let doc = encode_path(&[b"doc"]);
    let content = encode_path(&[b"doc", b"content"]);
    let set = vec![
        tup(
            AclSubject::Actor(cid(1)),
            cap(Capability::Read),
            AclEffect::Deny,
            doc.clone(),
        ),
        tup(
            AclSubject::Actor(cid(1)),
            cap(Capability::Read),
            AclEffect::Allow,
            content.clone(),
        ),
    ];
    assert!(!evaluate(&set, &actor(1), &content, Capability::Read));
    // A sibling the deny does not cover is unaffected (and ungranted → denied).
    let sibling = encode_path(&[b"doc", b"meta"]);
    assert!(!evaluate(&set, &actor(1), &sibling, Capability::Read));
}

#[test]
fn a_deeper_deny_carves_out_only_its_subtree() {
    // allow read on doc, deny read on doc/secret: read everywhere under doc except
    // the secret subtree.
    let doc = encode_path(&[b"doc"]);
    let secret = encode_path(&[b"doc", b"secret"]);
    let set = vec![
        tup(
            AclSubject::Actor(cid(1)),
            cap(Capability::Read),
            AclEffect::Allow,
            doc.clone(),
        ),
        tup(
            AclSubject::Actor(cid(1)),
            cap(Capability::Read),
            AclEffect::Deny,
            secret.clone(),
        ),
    ];
    assert!(evaluate(&set, &actor(1), &doc, Capability::Read));
    assert!(evaluate(
        &set,
        &actor(1),
        &encode_path(&[b"doc", b"public"]),
        Capability::Read
    ));
    assert!(!evaluate(&set, &actor(1), &secret, Capability::Read));
    assert!(!evaluate(
        &set,
        &actor(1),
        &encode_path(&[b"doc", b"secret", b"line"]),
        Capability::Read
    ));
}

// ---- Own implies the sub-lattice; capability separation -------------------

#[test]
fn own_confers_read_write_and_publish_awareness() {
    let doc = encode_path(&[b"doc"]);
    let set = vec![tup(
        AclSubject::Actor(cid(1)),
        cap(Capability::Own),
        AclEffect::Allow,
        doc.clone(),
    )];
    for c in [
        Capability::Read,
        Capability::Write,
        Capability::PublishAwareness,
        Capability::Own,
    ] {
        assert!(evaluate(&set, &actor(1), &doc, c), "owner holds {c:?}");
        // And over the whole subtree.
        assert!(evaluate(&set, &actor(1), &encode_path(&[b"doc", b"x"]), c));
    }
}

#[test]
fn deny_read_blocks_an_owner_but_leaves_write() {
    // deny read is capability-specific: it strips read even from an owner, without
    // touching the owner's write/publish.
    let doc = encode_path(&[b"doc"]);
    let set = vec![
        tup(
            AclSubject::Actor(cid(1)),
            cap(Capability::Own),
            AclEffect::Allow,
            doc.clone(),
        ),
        tup(
            AclSubject::Actor(cid(1)),
            cap(Capability::Read),
            AclEffect::Deny,
            doc.clone(),
        ),
    ];
    assert!(!evaluate(&set, &actor(1), &doc, Capability::Read));
    assert!(evaluate(&set, &actor(1), &doc, Capability::Write));
    assert!(evaluate(
        &set,
        &actor(1),
        &doc,
        Capability::PublishAwareness
    ));
}

#[test]
fn deny_own_strips_ownership_but_leaves_a_direct_read_allow() {
    // deny own removes the ownership (and its implied lattice) without denying a
    // separately granted read.
    let doc = encode_path(&[b"doc"]);
    let set = vec![
        tup(
            AclSubject::Actor(cid(1)),
            cap(Capability::Own),
            AclEffect::Allow,
            doc.clone(),
        ),
        tup(
            AclSubject::Actor(cid(1)),
            cap(Capability::Own),
            AclEffect::Deny,
            doc.clone(),
        ),
        tup(
            AclSubject::Actor(cid(1)),
            cap(Capability::Read),
            AclEffect::Allow,
            doc.clone(),
        ),
    ];
    // Not an owner, so write (which came only via Own) is gone…
    assert!(!evaluate(&set, &actor(1), &doc, Capability::Write));
    assert!(!evaluate(&set, &actor(1), &doc, Capability::Own));
    // …but the direct read allow stands.
    assert!(evaluate(&set, &actor(1), &doc, Capability::Read));
}

// ---- subject classes and groups -------------------------------------------

#[test]
fn subject_classes_match_by_authentication_state() {
    let doc = encode_path(&[b"doc"]);
    let authed = AclActor {
        authenticated: true,
        ..actor(1)
    };
    let anon = AclActor {
        authenticated: false,
        ..actor(2)
    };

    let to_authed = vec![tup(
        AclSubject::Authenticated,
        cap(Capability::Read),
        AclEffect::Allow,
        doc.clone(),
    )];
    assert!(evaluate(&to_authed, &authed, &doc, Capability::Read));
    assert!(!evaluate(&to_authed, &anon, &doc, Capability::Read));

    let to_anon = vec![tup(
        AclSubject::Anonymous,
        cap(Capability::Read),
        AclEffect::Allow,
        doc.clone(),
    )];
    assert!(!evaluate(&to_anon, &authed, &doc, Capability::Read));
    assert!(evaluate(&to_anon, &anon, &doc, Capability::Read));

    let to_anyone = vec![tup(
        AclSubject::Anyone,
        cap(Capability::Read),
        AclEffect::Allow,
        doc.clone(),
    )];
    assert!(evaluate(&to_anyone, &authed, &doc, Capability::Read));
    assert!(evaluate(&to_anyone, &anon, &doc, Capability::Read));
}

#[test]
fn a_group_grant_confers_to_a_member_only() {
    let doc = encode_path(&[b"doc"]);
    let member = AclActor {
        groups: vec![b"designers".to_vec()],
        ..actor(1)
    };
    let outsider = actor(2);
    let set = vec![tup(
        AclSubject::Group(b"designers".to_vec()),
        cap(Capability::Write),
        AclEffect::Allow,
        doc.clone(),
    )];
    assert!(evaluate(&set, &member, &doc, Capability::Write));
    assert!(!evaluate(&set, &outsider, &doc, Capability::Write));
}

// ---- role expansion (effective_roles) -------------------------------------

#[test]
fn a_role_grant_confers_the_role_to_a_holder_not_to_others() {
    let doc = encode_path(&[b"doc"]);
    let set = vec![tup(
        AclSubject::Actor(cid(1)),
        role(b"editor"),
        AclEffect::Allow,
        doc.clone(),
    )];
    // Alice (the subject) holds `editor` at the path and below…
    assert_eq!(
        effective_roles(&set, &actor(1), &doc),
        vec![b"editor".to_vec()]
    );
    assert_eq!(
        effective_roles(&set, &actor(1), &encode_path(&[b"doc", b"x"])),
        vec![b"editor".to_vec()]
    );
    // …Bob does not, and neither does Alice on a sibling subtree.
    assert!(effective_roles(&set, &actor(2), &doc).is_empty());
    assert!(effective_roles(&set, &actor(1), &encode_path(&[b"other"])).is_empty());
}

#[test]
fn token_roles_are_global_and_unioned_with_per_doc_assignments() {
    let doc = encode_path(&[b"doc"]);
    let alice = AclActor {
        roles: vec![b"admin".to_vec()],
        ..actor(1)
    };
    let set = vec![tup(
        AclSubject::Actor(cid(1)),
        role(b"editor"),
        AclEffect::Allow,
        doc.clone(),
    )];
    // Token role holds everywhere, even where no tuple governs.
    assert_eq!(
        effective_roles(&set, &alice, &encode_path(&[b"elsewhere"])),
        vec![b"admin".to_vec()]
    );
    // Under the doc, token ∪ per-doc (sorted).
    assert_eq!(
        effective_roles(&set, &alice, &doc),
        vec![b"admin".to_vec(), b"editor".to_vec()]
    );
}

#[test]
fn a_role_grant_to_a_group_reaches_its_members() {
    let doc = encode_path(&[b"doc"]);
    let member = AclActor {
        groups: vec![b"designers".to_vec()],
        ..actor(1)
    };
    let set = vec![tup(
        AclSubject::Group(b"designers".to_vec()),
        role(b"editor"),
        AclEffect::Allow,
        doc.clone(),
    )];
    assert_eq!(
        effective_roles(&set, &member, &doc),
        vec![b"editor".to_vec()]
    );
    assert!(effective_roles(&set, &actor(2), &doc).is_empty());
}

#[test]
fn a_role_deny_overrides_an_allow_and_a_token_claim() {
    let doc = encode_path(&[b"doc"]);
    let alice = AclActor {
        roles: vec![b"editor".to_vec()],
        ..actor(1)
    };
    let set = vec![
        tup(
            AclSubject::Actor(cid(1)),
            role(b"editor"),
            AclEffect::Allow,
            doc.clone(),
        ),
        tup(
            AclSubject::Actor(cid(1)),
            role(b"editor"),
            AclEffect::Deny,
            doc.clone(),
        ),
    ];
    // Denied per-doc: absent even though a token claim and an allow both grant it.
    assert!(effective_roles(&set, &alice, &doc).is_empty());
}

#[test]
fn role_resolution_is_a_single_terminating_pass() {
    // Subjects are never roles, so no assignment targets a role: the role graph has
    // no role→role edge and cannot cycle. A dense set of assignments (each keyed on
    // the actor) resolves in one pass — this both documents the structural property
    // and guards against a nonterminating resolver.
    let doc = encode_path(&[b"doc"]);
    let mut set = Vec::new();
    for i in 0u16..256 {
        set.push(tup(
            AclSubject::Actor(cid(1)),
            role(format!("role-{i}").as_bytes()),
            AclEffect::Allow,
            doc.clone(),
        ));
    }
    let roles = effective_roles(&set, &actor(1), &doc);
    assert_eq!(roles.len(), 256);
    assert!(roles.contains(&b"role-0".to_vec()));
    assert!(roles.contains(&b"role-255".to_vec()));
}

// ---- revocation (tombstoned tuples drop out of the live set) --------------

#[test]
fn a_revoked_tuple_no_longer_grants() {
    let mut d = Document::new(cid(1));
    let doc = encode_path(&[b"doc"]);
    let mut id = ElementId::from_bytes([0u8; 16]);
    d.transact(|tx| {
        id = tx.acl().grant(
            AclSubject::Actor(cid(2)),
            cap(Capability::Read),
            AclEffect::Allow,
            doc.clone(),
            cid(1),
        );
    });
    assert!(evaluate(&d.acl_tuples(), &actor(2), &doc, Capability::Read));

    d.transact(|tx| tx.acl().revoke(id));
    assert!(
        !evaluate(&d.acl_tuples(), &actor(2), &doc, Capability::Read),
        "a revoked tuple leaves the live set and stops granting"
    );
}

// ---- determinism ----------------------------------------------------------

#[test]
fn the_decision_is_independent_of_tuple_order() {
    let doc = encode_path(&[b"doc"]);
    let secret = encode_path(&[b"doc", b"secret"]);
    let mut set = vec![
        tup(
            AclSubject::Anyone,
            cap(Capability::Read),
            AclEffect::Allow,
            doc.clone(),
        ),
        tup(
            AclSubject::Actor(cid(1)),
            cap(Capability::Own),
            AclEffect::Allow,
            doc.clone(),
        ),
        tup(
            AclSubject::Actor(cid(1)),
            cap(Capability::Read),
            AclEffect::Deny,
            secret.clone(),
        ),
        tup(
            AclSubject::Actor(cid(1)),
            role(b"editor"),
            AclEffect::Allow,
            doc.clone(),
        ),
    ];

    let probe = |s: &[AclTuple]| {
        (
            evaluate(s, &actor(1), &doc, Capability::Write),
            evaluate(s, &actor(1), &secret, Capability::Read),
            evaluate(s, &actor(2), &doc, Capability::Read),
            evaluate(s, &actor(2), &doc, Capability::Write),
            effective_roles(s, &actor(1), &doc),
        )
    };

    let forward = probe(&set);
    set.reverse();
    let reversed = probe(&set);
    assert_eq!(forward, reversed);

    // And the values themselves are what deny-overrides + Own-implies dictate.
    assert_eq!(
        forward,
        (true, false, true, false, vec![b"editor".to_vec()])
    );
}

#[test]
fn two_replicas_with_the_same_merged_set_decide_identically() {
    let doc = encode_path(&[b"doc"]);
    let mut r1 = Document::new(cid(1));
    let mut r2 = Document::new(cid(2));

    let c1 = r1.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(5)),
            cap(Capability::Own),
            AclEffect::Allow,
            doc.clone(),
            cid(1),
        );
    });
    let c2 = r2.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(cid(5)),
            cap(Capability::Read),
            AclEffect::Deny,
            encode_path(&[b"doc", b"secret"]),
            cid(2),
        );
    });
    apply_all(&mut r1, &c2);
    apply_all(&mut r2, &c1);

    let s1 = r1.acl_tuples();
    let s2 = r2.acl_tuples();
    let subject = AclActor::new(cid(5));
    for path in [&doc, &encode_path(&[b"doc", b"secret"])] {
        for c in [Capability::Read, Capability::Write, Capability::Own] {
            assert_eq!(
                evaluate(&s1, &subject, path, c),
                evaluate(&s2, &subject, path, c),
                "replicas diverged on {c:?}"
            );
        }
    }
    // The owner reads/writes the doc but the concurrent deny carves out the secret.
    assert!(evaluate(&s1, &subject, &doc, Capability::Read));
    assert!(!evaluate(
        &s1,
        &subject,
        &encode_path(&[b"doc", b"secret"]),
        Capability::Read
    ));
}

// ---- totality -------------------------------------------------------------

#[test]
fn the_root_path_governs_everything_and_nothing_panics() {
    // The empty root path is an ancestor of every path.
    let root = encode_path(&[]);
    assert!(root.is_empty());
    let set = vec![tup(
        AclSubject::Anyone,
        cap(Capability::Read),
        AclEffect::Allow,
        root.clone(),
    )];
    assert!(evaluate(&set, &actor(1), &root, Capability::Read));
    assert!(evaluate(
        &set,
        &actor(1),
        &encode_path(&[b"anything", b"deep"]),
        Capability::Read
    ));

    // Degenerate inputs are total: no panic, deny/empty by default.
    let none: Vec<AclTuple> = Vec::new();
    assert!(!evaluate(&none, &actor(0), &[], Capability::Own));
    assert!(effective_roles(&none, &actor(0), &[]).is_empty());
}
