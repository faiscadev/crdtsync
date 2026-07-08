//! Doc-level ACL — the authorization tuple, a CRDT-merged doc-level set.
//!
//! An owner assigns a capability or a role, to an actor or a group, on a path,
//! with an allow or deny effect. Those grants live as document state: a set of
//! immutable [`AclTuple`]s keyed by a stable id, held on the [`Document`] beside
//! the annotation set and merged by the same laws (concurrent creates union to
//! distinct ids, a revoke tombstones, delete wins).
//!
//! The set is storage; this module also holds the **evaluator**. Two layers:
//!
//! - the pure as-present decision ([`evaluate`] / [`decide_capability`] /
//!   [`effective_roles`]) — reads tuples as stored, checking no authority; and
//! - the **authority** layer ([`evaluate_with_authority`] /
//!   [`decide_capability_with_authority`]) — realises creator-auto-owns-`/` (the doc
//!   creator implicitly owns the root, the bootstrap authority), attenuated recursive
//!   delegation (a grant confers authority only if its grantor validly held it,
//!   recursively up to the creator — an unrooted grant is inert), and recursive
//!   provenance-based revocation (a revoke is honored only when its author is the
//!   grant's grantor, the creator, or a *currently valid* owner of the path; an
//!   unauthorized revoke — including one by an owner whose own ownership was revoked —
//!   is disregarded and the grant stays effective).
//!
//! Core stores every tuple faithfully and merges the set content-neutrally (any
//! tuple that arrives is stored, every revoke tombstones), so authority is decided
//! here over the merged view — never rejected at merge. The authority layer also
//! realises **provenance-bounded deny** — a deny suppresses a grant only within its
//! author's authority (at or above the grant's grantor), so it cannot back-door around
//! revocation. Wiring a verdict into op-apply lives outside this module (the
//! server-side pipeline).
//!
//! [`Document`]: crate::doc::Document

use std::collections::BTreeSet;

use crate::clientid::ClientId;
use crate::elementid::ElementId;

/// Who a tuple grants to: a specific actor, a named group (the token carries an
/// actor's group membership; the tuple targets the group), or one of the
/// well-known classes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum AclSubject {
    Actor(ClientId),
    Group(Vec<u8>),
    Authenticated,
    Anonymous,
    Anyone,
}

/// A direct capability a grant confers — the four powers over a subtree.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Capability {
    Read,
    Write,
    PublishAwareness,
    Own,
}

/// What a tuple grants: a direct [`Capability`], or a schema-declared role by
/// name (its powers resolve through the schema `@auth` grants — a later slice's
/// concern; here the name is stored opaquely).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum AclGrant {
    Capability(Capability),
    Role(Vec<u8>),
}

/// Whether a tuple allows or denies its grant.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AclEffect {
    Allow,
    Deny,
}

/// One stored authorization tuple: an allow/deny of a capability-or-role, to a
/// subject, on a path, recorded with the actor that granted it. Immutable once
/// created — a change is a new tuple, and the only mutation is a revoke that
/// tombstones it. A read view over the document's ACL set; obtain one from
/// [`Document::acl_tuple`](crate::doc::Document::acl_tuple) or
/// [`acl_tuples`](crate::doc::Document::acl_tuples).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AclTuple {
    pub id: ElementId,
    pub subject: AclSubject,
    pub grant: AclGrant,
    pub effect: AclEffect,
    /// A path into the document (a `core::path` length-framed key path), opaque
    /// bytes to the set — stored and compared, never re-encoded.
    pub path: Vec<u8>,
    /// The actor that authored the grant. Stored faithfully; core performs no
    /// provenance check on it (that is the server-side evaluator's concern).
    pub grantor: ClientId,
}

/// The querying actor an ACL decision resolves for: the id the server trusts for
/// the connection, plus the membership its credential asserts. Core hosts no
/// identity provider — a caller (the server's `Identity`) supplies the actor id,
/// its group memberships, its global (token) roles, and whether the connection
/// authenticated. The evaluator matches a tuple's [`AclSubject`] against this and
/// never decides membership itself.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AclActor {
    /// The id a tuple's [`AclSubject::Actor`] is matched against.
    pub id: ClientId,
    /// The groups the credential asserts: an [`AclSubject::Group`] matches any of
    /// them, and a per-doc role assigned to a group flows to the actor.
    pub groups: Vec<Vec<u8>>,
    /// The global (token) roles the credential asserts — held everywhere in the
    /// app, so they seed [`effective_roles`] before any per-doc assignment.
    pub roles: Vec<Vec<u8>>,
    /// Whether the connection authenticated: [`AclSubject::Authenticated`] matches
    /// when true, [`AclSubject::Anonymous`] when false.
    pub authenticated: bool,
}

impl AclActor {
    /// An authenticated actor with no group or role membership.
    pub fn new(id: ClientId) -> Self {
        AclActor {
            id,
            groups: Vec::new(),
            roles: Vec::new(),
            authenticated: true,
        }
    }
}

impl AclSubject {
    /// Whether this subject covers `actor`: an actor id by equality, a group by
    /// membership, a class by the actor's authentication state, `Anyone` always.
    fn matches(&self, actor: &AclActor) -> bool {
        match self {
            AclSubject::Actor(id) => *id == actor.id,
            AclSubject::Group(g) => actor.groups.iter().any(|m| m == g),
            AclSubject::Authenticated => actor.authenticated,
            AclSubject::Anonymous => !actor.authenticated,
            AclSubject::Anyone => true,
        }
    }
}

/// Whether `scope` (a tuple's path) governs `target`: equal to it, or an ancestor
/// of it. Paths are length-framed key sequences ([`crate::path`]), so a byte
/// prefix that equals a well-formed path lands on a segment boundary — a byte
/// `starts_with` is exactly the ancestor-or-equal test, and the empty root path
/// governs every path.
fn governs(scope: &[u8], target: &[u8]) -> bool {
    target.starts_with(scope)
}

/// A tier's verdict on a capability request. [`Abstain`](AclDecision::Abstain) is
/// "no tuple governs this" — the doc-ACL set holds no opinion, so a lower tier
/// (the schema `@auth` role grants, then default-deny) decides. It is what lets
/// the composed decision flow layer the tuple set above the schema without a bare
/// `bool` forcing a premature allow or deny.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AclDecision {
    Allow,
    Deny,
    Abstain,
}

/// The doc-level ACL tuple tier's verdict on whether `actor` holds `capability`
/// at `path`, over the live tuple set. The precedence is **deny-overrides**,
/// realising ARCHITECTURE's decision flow (steps 1–3) for this tier:
///
/// 1. an explicit **Deny** of the exact capability, on `path` or an ancestor,
///    wins outright — a broader deny is a hard floor a more-specific allow below
///    it cannot re-open (AWS-style), so path specificity is never a tiebreaker;
/// 2. otherwise an **owner** (an allow of [`Own`](Capability::Own) not itself
///    denied) holds every capability over its subtree — `Own` implies
///    [`Read`](Capability::Read) / [`Write`](Capability::Write) /
///    [`PublishAwareness`](Capability::PublishAwareness);
/// 3. otherwise an explicit **Allow** of the exact capability grants it;
/// 4. otherwise [`Abstain`](AclDecision::Abstain): no tuple governs the request,
///    and a lower tier (schema `@auth` over [`effective_roles`], then
///    default-deny) decides.
///
/// Deny is **capability-separated**: a `Deny(Own)` strips ownership only (step 2)
/// and leaves a direct `Read` allow standing; a `Deny(Read)` blocks reads even for
/// an owner. `Role` grants are not read here — they confer capabilities only
/// through the schema tier, over [`effective_roles`]. The result depends only on
/// which tuples match, never their order, so replicas holding the same merged set
/// decide identically.
///
/// The `grantor` field is provenance the evaluator does not read: it never checks
/// whether a tuple's author held authority to write it (delegation and
/// provenance-bounded deny are a later slice). Tuples are evaluated as present.
pub fn decide_capability(
    tuples: &[AclTuple],
    actor: &AclActor,
    path: &[u8],
    capability: Capability,
) -> AclDecision {
    let mut deny_cap = false;
    let mut allow_cap = false;
    let mut deny_own = false;
    let mut allow_own = false;
    for t in tuples {
        if !t.subject.matches(actor) || !governs(&t.path, path) {
            continue;
        }
        let AclGrant::Capability(c) = t.grant else {
            continue;
        };
        match (t.effect, c == capability, c == Capability::Own) {
            (AclEffect::Deny, true, _) => deny_cap = true,
            (AclEffect::Deny, _, true) => deny_own = true,
            (AclEffect::Allow, true, _) => allow_cap = true,
            (AclEffect::Allow, _, true) => allow_own = true,
            _ => {}
        }
    }
    if deny_cap {
        return AclDecision::Deny;
    }
    if allow_own && !deny_own {
        return AclDecision::Allow;
    }
    if allow_cap {
        return AclDecision::Allow;
    }
    AclDecision::Abstain
}

/// Whether `actor` holds `capability` at `path`, over the live ACL tuple set — the
/// total, **deny-by-default** entrypoint. It resolves the doc-ACL tuple tier
/// ([`decide_capability`]); an [`Abstain`](AclDecision::Abstain) there (no tuple
/// governs the request) is denied, since a doc-level ACL grants nothing it was not
/// told to. Never panics — an empty set, an unknown actor, any path all yield
/// `false`.
///
/// This is the tuple tier alone. The schema `@auth` role-grant tier (decision flow
/// step 4) maps an actor's [`effective_roles`] to capabilities and composes
/// *above* this default-deny; that composition needs the schema and belongs to the
/// server, so it is not folded in here.
pub fn evaluate(
    tuples: &[AclTuple],
    actor: &AclActor,
    path: &[u8],
    capability: Capability,
) -> bool {
    matches!(
        decide_capability(tuples, actor, path, capability),
        AclDecision::Allow
    )
}

/// The roles `actor` effectively holds at `path`: its global (token) roles unioned
/// with the per-doc roles the ACL set assigns it, with any denied role removed.
///
/// A per-doc assignment is a `Role` grant whose subject covers the actor (its id, a
/// group it belongs to, or a class) on `path` or an ancestor — roles inherit
/// downward like capabilities. An `Allow` adds the role, a `Deny` removes it, and
/// **deny-overrides**: a denied role is absent even when a token claim or a broader
/// allow would grant it.
///
/// The schema `@auth` tier turns these effective roles into capabilities (decision
/// flow step 4); this resolver is that tier's input, computed purely from the tuple
/// set. Resolution is a single pass: an [`AclSubject`] is never a role, so no
/// assignment targets a role — the role graph has no role→role edge, nothing to
/// recurse, and no cycle to guard against. The result is sorted and deduplicated,
/// so it is identical regardless of tuple order.
/// A stored ACL tuple together with the revoke provenance the authority layer
/// needs: the actors that have tombstoned it. An empty `revoked_by` is a live
/// grant; a non-empty one is a revoke whose *effect* the evaluator still decides,
/// since the set tombstones every revoke content-neutrally and authority is an
/// evaluation-layer rule, not a merge-time rejection. Obtain the records from
/// [`Document::acl_records`](crate::doc::Document::acl_records).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AclRecord {
    pub tuple: AclTuple,
    /// The actors that have revoked `tuple`. A revoke counts only when its author
    /// is [authorized](decide_capability_with_authority); an unauthorized author's
    /// tombstone is disregarded.
    pub revoked_by: Vec<ClientId>,
}

/// Whether `actor` holds an actor-id `Own` allow governing `target`, given the
/// current `rooted`/`revoked` snapshot over `records` — the recursive ownership test
/// that both grant-rooting and revoke-authority build on. The `creator` owns every
/// path (the bootstrap root). Otherwise an `Own` counts only when it is itself rooted
/// and not effectively revoked.
///
/// Ownership is resolved from **actor-id** `Own` grants alone: core knows an actor's
/// id but not its group or class membership, so an `Own` granted to a `Group` or a
/// subject class confers no ownership an arbitrary actor could inherit (fail-closed).
fn owns_path(
    records: &[AclRecord],
    creator: ClientId,
    actor: ClientId,
    target: &[u8],
    rooted: &[bool],
    revoked: &[bool],
) -> bool {
    if actor == creator {
        return true;
    }
    records.iter().enumerate().any(|(i, r)| {
        r.tuple.subject == AclSubject::Actor(actor)
            && matches!(r.tuple.grant, AclGrant::Capability(Capability::Own))
            && r.tuple.effect == AclEffect::Allow
            && governs(&r.tuple.path, target)
            && rooted[i]
            && !revoked[i]
    })
}

/// Each record's rooting under a `rooted`/`revoked` snapshot: a grant roots iff its
/// grantor validly owned the grant's path (or an ancestor) — recursively, via
/// [`owns_path`], up to the creator.
fn root_pass(
    records: &[AclRecord],
    creator: ClientId,
    rooted: &[bool],
    revoked: &[bool],
) -> Vec<bool> {
    records
        .iter()
        .map(|r| {
            owns_path(
                records,
                creator,
                r.tuple.grantor,
                &r.tuple.path,
                rooted,
                revoked,
            )
        })
        .collect()
}

/// Each record's effective revocation under a `rooted`/`revoked` snapshot: a revoke
/// counts iff its author is the grant's `grantor`, the `creator`, or a currently valid
/// owner of the grant's path.
fn revoke_pass(
    records: &[AclRecord],
    creator: ClientId,
    rooted: &[bool],
    revoked: &[bool],
) -> Vec<bool> {
    records
        .iter()
        .map(|r| {
            r.revoked_by.iter().any(|&who| {
                who == creator
                    || who == r.tuple.grantor
                    || owns_path(records, creator, who, &r.tuple.path, rooted, revoked)
            })
        })
        .collect()
}

/// Whether `author` is at or above `grantor` in the delegation hierarchy — the
/// authority a deny needs to bind a grant. True when `author` is the `creator` (above
/// everyone), the `grantor` itself (same authority — slice-2 deny-overrides), or a
/// delegation superior whose rooted `Own` grant `grantor`'s authority derives from,
/// transitively up the grantor chain.
///
/// A subordinate or an unrelated peer is *not* at or above the grantor, so its deny
/// cannot suppress the grant — the anti-backdoor property (a deny must not do what a
/// revoke may not). Only rooted, unrevoked actor-id `Own` grants are walked, so forged
/// or revoked ownership lends no superiority; the `seen` set bounds the walk on any
/// grantor cycle, so it terminates and is order-independent.
fn at_or_above(
    records: &[AclRecord],
    creator: ClientId,
    author: ClientId,
    grantor: ClientId,
    rooted: &[bool],
    revoked: &[bool],
) -> bool {
    if author == creator || author == grantor {
        return true;
    }
    // Walk up `grantor`'s ownership provenance: the actors that delegated `Own` to it,
    // then their delegators, and so on. `author` is a superior iff it is reached.
    let mut seen: Vec<ClientId> = vec![grantor];
    let mut frontier: Vec<ClientId> = vec![grantor];
    for _ in 0..=records.len() {
        let mut next: Vec<ClientId> = Vec::new();
        for &who in &frontier {
            for (i, r) in records.iter().enumerate() {
                if !rooted[i]
                    || revoked[i]
                    || r.tuple.effect != AclEffect::Allow
                    || !matches!(r.tuple.grant, AclGrant::Capability(Capability::Own))
                    || r.tuple.subject != AclSubject::Actor(who)
                {
                    continue;
                }
                let up = r.tuple.grantor;
                if up == author {
                    return true;
                }
                if !seen.contains(&up) {
                    seen.push(up);
                    next.push(up);
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    false
}

/// Whether an authorized deny suppresses an allow of `capability` to `actor` at `path`
/// that was made by `granter` (conferring via the `Own` lattice when `via_own`). A
/// deny suppresses only when its author is [at or above](at_or_above) `granter`
/// (provenance-bounded); a `Deny(capability)` bounds a same-capability allow, while a
/// `Deny(Own)` bounds only an `Own`-implied one (capability separation — a `Deny(Own)`
/// strips ownership without touching a separately granted capability).
#[allow(clippy::too_many_arguments)]
fn deny_suppresses(
    records: &[AclRecord],
    creator: ClientId,
    actor: &AclActor,
    path: &[u8],
    capability: Capability,
    granter: ClientId,
    via_own: bool,
    rooted: &[bool],
    revoked: &[bool],
) -> bool {
    records.iter().enumerate().any(|(i, r)| {
        if revoked[i]
            || r.tuple.effect != AclEffect::Deny
            || !r.tuple.subject.matches(actor)
            || !governs(&r.tuple.path, path)
        {
            return false;
        }
        let AclGrant::Capability(dc) = r.tuple.grant else {
            return false;
        };
        let relevant = dc == capability || (dc == Capability::Own && via_own);
        relevant && at_or_above(records, creator, r.tuple.grantor, granter, rooted, revoked)
    })
}

/// The doc-level ACL tuple tier's verdict on whether `actor` holds `capability` at
/// `path`, over the merged record set — the authority-aware form of
/// [`decide_capability`]. It layers **attenuated recursive delegation** and
/// **provenance-based revocation** on top of the deny-overrides / own-implies-lattice
/// decision.
///
/// - **creator-auto-owns-`/`** — `creator` holds `Own` at the root (and, by the
///   lattice, every capability over every path) with no explicit tuple: the
///   un-granted root of all authority.
/// - **attenuated recursive delegation** — a positive (`Allow`) grant confers
///   authority only if its grantor validly held it. A grant *roots* iff its grantor
///   validly owned the grant's path (or an ancestor) — recursively, up to the creator.
///   An `Allow` whose chain does not root at the creator is inert (self-granted or
///   forged authority confers nothing), and a non-owner cannot delegate (only
///   ownership confers granting power).
/// - **provenance-bounded deny** — a `Deny` suppresses a grant only when its author is
///   [at or above](at_or_above) the grant's grantor: the creator, the grantor itself
///   (slice-2 deny-overrides), or a delegation superior. A subordinate's or an
///   unrelated peer's deny is disregarded, so a deny is not a backdoor around
///   revocation; the synthetic creator-owns-`/` is immune to any deny but the
///   creator's own. A deny still beats a static/default grant always, and it is never
///   dropped for an unrooted grantor — only its power to re-open a superior's or a
///   peer's grant is bounded.
/// - **recursive provenance-based revocation** — a record is dropped only when a
///   revoker was authorized: the tuple's `grantor`, the `creator`, or a **currently
///   valid** owner of the tuple's path. Validity here is rooted-at-creator and not
///   itself authoritatively revoked, so an owner whose own `Own` was revoked no longer
///   confers revoke authority.
///
/// Rooting and revocation are mutually recursive (a revoke's authority is an
/// ownership, an ownership is a rooted-and-unrevoked grant), so both are solved as a
/// bounded fixpoint: each pass recomputes `rooted`/`revoked` from the previous snapshot
/// only, which makes the result independent of record order, and the walk terminates
/// on any tuple graph. A forged granting cycle roots at no creator and simply confers
/// nothing. A delegation-plus-revocation cycle (only reachable with forged,
/// self-referential provenance) is non-monotone and may oscillate; it is resolved
/// fail-closed and deterministically — an ambiguous revoke is disregarded — so the
/// verdict is always a pure function of the merged record set, and replicas holding the
/// same set decide identically.
pub fn decide_capability_with_authority(
    records: &[AclRecord],
    creator: ClientId,
    actor: &AclActor,
    path: &[u8],
    capability: Capability,
) -> AclDecision {
    let n = records.len();
    let mut rooted = vec![false; n];
    let mut revoked = vec![false; n];

    // Solve the mutually-recursive rooting/revocation relation by iterating from the
    // empty (all-false) seed. Each pass reads only the previous snapshot, so the result
    // is independent of record order; the loop exits as soon as a pass changes nothing.
    // Every well-founded (acyclic) grant graph converges here to its exact least
    // fixpoint — the depth of any alternating rooting/revocation chain is at most `2n`.
    let bound = 2 * n + 2;
    let mut converged = false;
    for _ in 0..bound {
        let next_rooted = root_pass(records, creator, &rooted, &revoked);
        let next_revoked = revoke_pass(records, creator, &rooted, &revoked);
        if next_rooted == rooted && next_revoked == revoked {
            converged = true;
            break;
        }
        rooted = next_rooted;
        revoked = next_revoked;
    }

    // Revoke-authority is a *negative* dependency (an owner counts only while not
    // revoked), so a delegation-plus-revocation cycle — reachable only with forged,
    // self-referential provenance — is non-monotone and can leave the relation
    // oscillating rather than settling. Resolve it deterministically and fail-closed:
    // a revoke still in flux is **disregarded** (an ambiguous revoke never strips a
    // grant — the same rule an unauthorized revoke already gets), then rooting is
    // settled with those revokes pinned (rooting alone is monotone, so it converges).
    // This makes the verdict a stable function of the merged set, never an artifact of
    // the pass count.
    if !converged {
        let flux = revoke_pass(records, creator, &rooted, &revoked);
        for i in 0..n {
            if flux[i] != revoked[i] {
                revoked[i] = false;
            }
        }
        for _ in 0..(n + 1) {
            let next_rooted = root_pass(records, creator, &rooted, &revoked);
            if next_rooted == rooted {
                break;
            }
            rooted = next_rooted;
        }
    }

    // Provenance-bounded decision. `capability` is allowed iff some rooted, live allow
    // confers it (directly or via `Own`) and no *authorized* deny suppresses that allow —
    // a deny binds only when its author is at or above the allow's grantor (see
    // [`at_or_above`]). Creator-owns-`/` is the synthetic root allow: it confers every
    // capability everywhere and is immune to any deny but the creator's own (creator-deny
    // immunity). Rooting gates only positive grants; a deny is honored as-present up to
    // this authority bound (it is never dropped for an unrooted grantor).
    let creator_owns = actor.id == creator
        && !deny_suppresses(
            records, creator, actor, path, capability, creator, true, &rooted, &revoked,
        );
    let allowed = creator_owns
        || records.iter().enumerate().any(|(i, r)| {
            if revoked[i] || !rooted[i] || r.tuple.effect != AclEffect::Allow {
                return false;
            }
            if !r.tuple.subject.matches(actor) || !governs(&r.tuple.path, path) {
                return false;
            }
            let AclGrant::Capability(c) = r.tuple.grant else {
                return false;
            };
            let via_own = c == Capability::Own;
            if !via_own && c != capability {
                return false;
            }
            !deny_suppresses(
                records,
                creator,
                actor,
                path,
                capability,
                r.tuple.grantor,
                via_own,
                &rooted,
                &revoked,
            )
        });
    if allowed {
        return AclDecision::Allow;
    }

    // No allow survives. A matching same-capability deny still floors the verdict to
    // `Deny` (deny beats static/default grants always) — the floor needs no authority,
    // since with no standing grant there is nothing to suppress; authority gates only the
    // re-opening of a superior's or a peer's grant, decided above.
    let floored = records.iter().enumerate().any(|(i, r)| {
        !revoked[i]
            && r.tuple.effect == AclEffect::Deny
            && r.tuple.subject.matches(actor)
            && governs(&r.tuple.path, path)
            && matches!(r.tuple.grant, AclGrant::Capability(c) if c == capability)
    });
    if floored {
        AclDecision::Deny
    } else {
        AclDecision::Abstain
    }
}

/// Whether `actor` holds `capability` at `path`, over the merged record set with the
/// authority rules applied — the total, deny-by-default entrypoint. The
/// authority-aware form of [`evaluate`]: [`Abstain`](AclDecision::Abstain) denies.
pub fn evaluate_with_authority(
    records: &[AclRecord],
    creator: ClientId,
    actor: &AclActor,
    path: &[u8],
    capability: Capability,
) -> bool {
    matches!(
        decide_capability_with_authority(records, creator, actor, path, capability),
        AclDecision::Allow
    )
}

pub fn effective_roles(tuples: &[AclTuple], actor: &AclActor, path: &[u8]) -> Vec<Vec<u8>> {
    let mut allowed: BTreeSet<Vec<u8>> = actor.roles.iter().cloned().collect();
    let mut denied: BTreeSet<Vec<u8>> = BTreeSet::new();
    for t in tuples {
        if !t.subject.matches(actor) || !governs(&t.path, path) {
            continue;
        }
        let AclGrant::Role(ref r) = t.grant else {
            continue;
        };
        match t.effect {
            AclEffect::Allow => {
                allowed.insert(r.clone());
            }
            AclEffect::Deny => {
                denied.insert(r.clone());
            }
        }
    }
    allowed.difference(&denied).cloned().collect()
}
