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
//!   [`decide_capability_with_authority`]) — realises creator-auto-owns-`/` (the
//!   doc creator implicitly owns the root, the bootstrap authority) and
//!   provenance-based revocation (a revoke tombstone is honored only when its
//!   author is the grant's grantor or a superior; an unauthorized revoke is
//!   disregarded and the grant stays effective).
//!
//! Core stores every tuple faithfully and merges the set content-neutrally (any
//! tuple that arrives is stored, every revoke tombstones), so authority is decided
//! here over the merged view — never rejected at merge. Provenance-bounded deny,
//! attenuated recursive delegation, and wiring a verdict into op-apply live outside
//! this module (the server-side pipeline).
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

/// The synthetic tuple that realises **creator-auto-owns-`/`**: the doc creator
/// implicitly holds `Own` at the root path, the bootstrap owner every grant chains
/// from. Core hosts no identity provider, so the creator id is supplied by the
/// caller (the `AclActor` precedent) rather than stored — the rule is pure. The id
/// is unread by the evaluator.
fn creator_owns_root(creator: ClientId) -> AclTuple {
    AclTuple {
        id: ElementId::from_bytes([0u8; 16]),
        subject: AclSubject::Actor(creator),
        grant: AclGrant::Capability(Capability::Own),
        effect: AclEffect::Allow,
        path: Vec::new(),
        grantor: creator,
    }
}

/// Whether `revoker` had the authority to revoke `tuple`, given the as-present grant
/// set `as_present`. **Provenance, not path-ancestry:** a revoke is honored iff its
/// author is the tuple's `grantor`, the doc `creator` (who owns `/`), or an owner of
/// the tuple's path.
///
/// Path-ownership that confers revoke authority is resolved from the revoker's own
/// **actor-id** `Own` grants alone: core knows the revoker's id (the op author) but
/// not its group or class membership, so an `Own` granted to a `Group` or a subject
/// class must not let an arbitrary actor inherit revoke power. Membership-derived
/// ownership therefore does not confer revoke authority here — a revoke it would
/// justify is disregarded (fail-closed, the grant stays effective). The ownership
/// test reads `as_present` as it stands and does **not** re-check whether those
/// `Own` grants were themselves revoked: the chain here is shallow — the grantor, or
/// a root/path owner directly above.
fn revoke_authorized(
    tuple: &AclTuple,
    revoker: ClientId,
    creator: ClientId,
    as_present: &[AclTuple],
) -> bool {
    if revoker == creator || revoker == tuple.grantor {
        return true;
    }
    let owned: Vec<AclTuple> = as_present
        .iter()
        .filter(|t| t.subject == AclSubject::Actor(revoker))
        .cloned()
        .collect();
    evaluate(
        &owned,
        &AclActor::new(revoker),
        &tuple.path,
        Capability::Own,
    )
}

/// The doc-level ACL tuple tier's verdict on whether `actor` holds `capability` at
/// `path`, over the merged record set — the authority-aware form of
/// [`decide_capability`]. It layers the two authority rules on top of the
/// deny-overrides / own-implies-lattice decision:
///
/// - **creator-auto-owns-`/`** — `creator` holds `Own` at the root (and, by the
///   lattice, every capability over every path) with no explicit tuple.
/// - **provenance-based revocation** — a record is dropped from the live set only
///   when one of its revokers was [authorized](revoke_authorized) to revoke it; an
///   unauthorized revoke is disregarded and the grant stays effective.
///
/// The verdict depends only on which records match and their provenance, never on
/// their order, so replicas holding the same merged record set decide identically.
pub fn decide_capability_with_authority(
    records: &[AclRecord],
    creator: ClientId,
    actor: &AclActor,
    path: &[u8],
    capability: Capability,
) -> AclDecision {
    // The as-present grants backing the ownership check for revoke authority. Only
    // an actual revoke needs adjudicating, so build it only when one exists — the
    // no-revocation path (the common case) clones nothing extra.
    let as_present: Vec<AclTuple> = if records.iter().any(|r| !r.revoked_by.is_empty()) {
        records.iter().map(|r| r.tuple.clone()).collect()
    } else {
        Vec::new()
    };

    // The effective set: creator-owns, plus every tuple no *authorized* revoke has
    // tombstoned.
    let mut effective: Vec<AclTuple> = Vec::with_capacity(records.len() + 1);
    effective.push(creator_owns_root(creator));
    for r in records {
        let revoked = r
            .revoked_by
            .iter()
            .any(|&who| revoke_authorized(&r.tuple, who, creator, &as_present));
        if !revoked {
            effective.push(r.tuple.clone());
        }
    }
    decide_capability(&effective, actor, path, capability)
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
