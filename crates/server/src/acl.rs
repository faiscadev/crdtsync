//! The ACL decision flow — a concrete [`Authorizer`] over a set of ACL tuples.
//!
//! A deployment builds an [`Acl`] from allow/deny rules and plugs it into the
//! server via [`Registry::set_authorizer`](crate::Registry::set_authorizer). A
//! check walks every matching rule with standard IAM semantics: an explicit
//! DENY always wins, an explicit ALLOW grants, and the absence of any matching
//! allow denies. This is the doc-independent policy engine; the doc-level
//! ACL-as-CRDT layer feeds its tuples into the same evaluation once it lands.
//!
//! A [`Subject`] is matched against the full [`Identity`] an enforcement point
//! carries: its actor id, or the roles and groups its credential asserts. Within
//! an [`Acl`] the flow is its explicit-tuple steps: deny-wins, then allow, then —
//! for a request no rule mentions — [`Abstain`](crate::authz::Decision::Abstain).
//! [`authorized`] composes that abstain with the room's governing schema `@auth`
//! grants (decision-flow step 4) and a terminal default-deny.

use std::collections::HashMap;

use crdtsync_core::acl::{
    decide_capability_with_authority, AclActor, AclDecision, AclRecord, AclScope, Capability,
};
use crdtsync_core::path::encode_path;
use crdtsync_core::schema::{
    Action as SchemaAction, Auth, Effect as GrantEffect, Schema, Subject as GrantSubject,
    SubjectClass,
};
use crdtsync_core::{ClientId, ElementId, Op, OpKind};
use sha2::{Digest, Sha256};

use crate::auth::Identity;
use crate::authz::{Action, Authorizer, Decision, Resource};

/// Anonymous actors are minted with this prefix, so a subject can tell an
/// anonymous connection from a credentialed one without reading claims.
const ANON_PREFIX: &[u8] = b"anon:";

/// Whether a rule grants or withholds access. An explicit [`Deny`](Effect::Deny)
/// overrides any allow.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Effect {
    Allow,
    Deny,
}

/// Who a rule applies to, matched against the acting [`Identity`]: its actor id,
/// its subject class, or a role or group its credential asserts.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Subject {
    /// One specific actor id.
    Actor(Vec<u8>),
    /// Any actor holding this role among its credential's claims.
    Role(String),
    /// Any actor in this group among its credential's claims.
    Group(String),
    /// Any credentialed (non-anonymous) actor — `authenticated:*`.
    Authenticated,
    /// Any anonymous actor — `anonymous:*`, an `anon:`-prefixed id.
    Anonymous,
    /// Anyone at all — `*`.
    Anyone,
}

impl Subject {
    fn matches(&self, identity: &Identity) -> bool {
        let actor = identity.actor();
        match self {
            Subject::Actor(id) => id.as_slice() == actor,
            Subject::Role(role) => identity.roles().iter().any(|r| r == role),
            Subject::Group(group) => identity.groups().iter().any(|g| g == group),
            Subject::Authenticated => !actor.starts_with(ANON_PREFIX),
            Subject::Anonymous => actor.starts_with(ANON_PREFIX),
            Subject::Anyone => true,
        }
    }
}

/// Which resource a rule covers. Room- and app-scoped today; a variant widens
/// the room case to path / element as [`Resource`] grows. A room match never
/// covers an app resource, nor the reverse — the control plane and data plane
/// are distinct.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ResourceMatch {
    /// Every room.
    AnyRoom,
    /// One specific room.
    Room(Vec<u8>),
    /// One specific app, by `app_id` — the schema-registry control plane.
    App(Vec<u8>),
    /// One specific zone of one specific room — the partition-scoped grant a
    /// deployment writes to isolate a zone: a `deny` here carves the partition out
    /// of an otherwise room-readable actor, an `allow` opens it. The subscribe gate
    /// consults this on a per-zone read; with no matching zone rule the policy
    /// abstains, and the zone inherits the room's read verdict (visible by default
    /// within a readable room).
    Zone { room: Vec<u8>, zone: Vec<u8> },
}

impl ResourceMatch {
    fn matches(&self, resource: &Resource) -> bool {
        match (self, resource) {
            (ResourceMatch::AnyRoom, Resource::Room(_)) => true,
            (ResourceMatch::Room(name), Resource::Room(room)) => name.as_slice() == *room,
            (ResourceMatch::App(name), Resource::App(app)) => name.as_slice() == *app,
            (ResourceMatch::Zone { room, zone }, Resource::Zone { room: r, zone: z }) => {
                room.as_slice() == *r && zone.as_slice() == *z
            }
            _ => false,
        }
    }
}

/// One ACL tuple: an effect for a (subject, action, resource) pattern. A `None`
/// action matches every action.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Rule {
    subject: Subject,
    action: Option<Action>,
    resource: ResourceMatch,
    effect: Effect,
}

impl Rule {
    fn matches(&self, identity: &Identity, action: Action, resource: &Resource) -> bool {
        self.subject.matches(identity)
            && self.action.map_or(true, |a| a == action)
            && self.resource.matches(resource)
    }
}

/// A policy built from ACL tuples. Rules are order-independent — deny-wins makes
/// the result the same however they were added.
#[derive(Clone, Default, Debug)]
pub struct Acl {
    rules: Vec<Rule>,
}

impl Acl {
    /// An empty policy — denies everything until rules are added.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a rule with an explicit effect. The [`allow`](Acl::allow) /
    /// [`deny`](Acl::deny) builders are the fluent sugar over it.
    pub fn push(
        &mut self,
        subject: Subject,
        action: Option<Action>,
        resource: ResourceMatch,
        effect: Effect,
    ) {
        self.rules.push(Rule {
            subject,
            action,
            resource,
            effect,
        });
    }

    /// Grant a (subject, action, resource) pattern. `None` action means any.
    pub fn allow(
        mut self,
        subject: Subject,
        action: Option<Action>,
        resource: ResourceMatch,
    ) -> Self {
        self.push(subject, action, resource, Effect::Allow);
        self
    }

    /// Withhold a (subject, action, resource) pattern; a deny overrides any allow.
    pub fn deny(
        mut self,
        subject: Subject,
        action: Option<Action>,
        resource: ResourceMatch,
    ) -> Self {
        self.push(subject, action, resource, Effect::Deny);
        self
    }
}

/// Which field of a policy line failed to parse. Each variant that names a token
/// carries it, so an error message can point at what was wrong.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PolicyErrorKind {
    /// A rule line held this many whitespace-separated fields, not the four a
    /// rule requires.
    Arity(usize),
    /// The effect field was neither `allow` nor `deny`.
    Effect(String),
    /// The subject field named no known subject.
    Subject(String),
    /// An `actor:` subject carried a value that is not valid hex — an odd length
    /// or a non-hex digit.
    ActorHex(String),
    /// The action field named no known action.
    Action(String),
    /// The resource field named no known resource.
    Resource(String),
}

impl std::fmt::Display for PolicyErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyErrorKind::Arity(n) => write!(f, "expected 4 fields, found {n}"),
            PolicyErrorKind::Effect(t) => write!(f, "unknown effect \"{t}\" (want allow or deny)"),
            PolicyErrorKind::Subject(t) => write!(f, "unknown subject \"{t}\""),
            PolicyErrorKind::ActorHex(t) => write!(f, "invalid actor hex \"{t}\""),
            PolicyErrorKind::Action(t) => write!(f, "unknown action \"{t}\""),
            PolicyErrorKind::Resource(t) => write!(f, "unknown resource \"{t}\""),
        }
    }
}

/// A failure to parse a policy, pinned to the 1-based physical line it occurred
/// on so a deployment can find the bad rule in its file.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PolicyError {
    pub line: usize,
    pub kind: PolicyErrorKind,
}

impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {}: {}", self.line, self.kind)
    }
}

impl std::error::Error for PolicyError {}

impl Acl {
    /// Parse a declarative text policy into an [`Acl`]. One rule per line,
    /// `<effect> <subject> <action> <resource>` with whitespace-separated fields;
    /// blank lines and `#` comment lines are ignored. The result authorizes
    /// identically to the same rules pushed via [`allow`](Acl::allow) /
    /// [`deny`](Acl::deny). Parsing is total — a malformed line yields a
    /// [`PolicyError`] naming its physical line, never a panic.
    ///
    /// - effect: `allow` | `deny`
    /// - subject: `actor:<hex>` | `role:<name>` | `group:<name>` | `authenticated` | `anonymous` | `anyone` | `*`
    /// - action: `read` | `write` | `publish_awareness` | `*`
    /// - resource: `room:<name>` | `*`
    pub fn from_policy(text: &str) -> Result<Self, PolicyError> {
        let mut acl = Acl::new();
        for (i, raw) in text.lines().enumerate() {
            let line = i + 1;
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let fields: Vec<&str> = trimmed.split_whitespace().collect();
            let at = |kind| PolicyError { line, kind };
            if fields.len() != 4 {
                return Err(at(PolicyErrorKind::Arity(fields.len())));
            }
            let effect = parse_effect(fields[0])
                .ok_or_else(|| at(PolicyErrorKind::Effect(fields[0].into())))?;
            let subject = parse_subject(fields[1]).map_err(at)?;
            let action = parse_action(fields[2])
                .ok_or_else(|| at(PolicyErrorKind::Action(fields[2].into())))?;
            let resource = parse_resource(fields[3])
                .ok_or_else(|| at(PolicyErrorKind::Resource(fields[3].into())))?;
            acl.push(subject, action, resource, effect);
        }
        Ok(acl)
    }

    /// Load a policy from a file at `path` — read it, then [`from_policy`](Acl::from_policy)
    /// its contents. A deployment points the server at a policy file and the
    /// running server enforces it. The file being unreadable and its contents
    /// being malformed are distinct [`PolicyFileError`] arms.
    pub fn from_policy_file(path: impl AsRef<std::path::Path>) -> Result<Self, PolicyFileError> {
        let text = std::fs::read_to_string(path)?;
        Ok(Self::from_policy(&text)?)
    }
}

/// Why loading a policy file failed: the file could not be read, or its contents
/// did not parse.
#[derive(Debug)]
pub enum PolicyFileError {
    /// The file could not be read.
    Io(std::io::Error),
    /// The file was read but a line did not parse.
    Parse(PolicyError),
}

impl std::fmt::Display for PolicyFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyFileError::Io(e) => write!(f, "reading policy file: {e}"),
            PolicyFileError::Parse(e) => write!(f, "parsing policy file: {e}"),
        }
    }
}

impl std::error::Error for PolicyFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PolicyFileError::Io(e) => Some(e),
            PolicyFileError::Parse(e) => Some(e),
        }
    }
}

impl From<std::io::Error> for PolicyFileError {
    fn from(e: std::io::Error) -> Self {
        PolicyFileError::Io(e)
    }
}

impl From<PolicyError> for PolicyFileError {
    fn from(e: PolicyError) -> Self {
        PolicyFileError::Parse(e)
    }
}

/// A role or group name, rejecting the empty string (a malformed `role:` /
/// `group:` token whose name was omitted). The original token is quoted so the
/// error names what was wrong.
fn non_empty(name: &str) -> Result<String, PolicyErrorKind> {
    if name.is_empty() {
        Err(PolicyErrorKind::Subject(name.into()))
    } else {
        Ok(name.to_string())
    }
}

fn parse_effect(tok: &str) -> Option<Effect> {
    match tok {
        "allow" => Some(Effect::Allow),
        "deny" => Some(Effect::Deny),
        _ => None,
    }
}

fn parse_subject(tok: &str) -> Result<Subject, PolicyErrorKind> {
    match tok {
        "authenticated" => Ok(Subject::Authenticated),
        "anonymous" => Ok(Subject::Anonymous),
        "anyone" | "*" => Ok(Subject::Anyone),
        _ => {
            if let Some(hex) = tok.strip_prefix("actor:") {
                return decode_hex(hex)
                    .map(Subject::Actor)
                    .ok_or_else(|| PolicyErrorKind::ActorHex(hex.into()));
            }
            if let Some(role) = tok.strip_prefix("role:") {
                // An empty name is a dead rule no identity can match — a
                // credential never carries an empty role — so reject it as
                // malformed rather than load a silently-inert line.
                return non_empty(role).map(Subject::Role);
            }
            if let Some(group) = tok.strip_prefix("group:") {
                return non_empty(group).map(Subject::Group);
            }
            Err(PolicyErrorKind::Subject(tok.into()))
        }
    }
}

/// The outer `Option` is "known token?"; the inner is the action itself, `None`
/// for the `*` wildcard that matches every action.
fn parse_action(tok: &str) -> Option<Option<Action>> {
    match tok {
        "read" => Some(Some(Action::Read)),
        "write" => Some(Some(Action::Write)),
        "publish_awareness" => Some(Some(Action::PublishAwareness)),
        "register_schema" => Some(Some(Action::RegisterSchema)),
        "*" => Some(None),
        _ => None,
    }
}

fn parse_resource(tok: &str) -> Option<ResourceMatch> {
    if tok == "*" {
        Some(ResourceMatch::AnyRoom)
    } else if let Some(name) = tok.strip_prefix("room:") {
        Some(ResourceMatch::Room(name.as_bytes().to_vec()))
    } else {
        tok.strip_prefix("app:")
            .map(|id| ResourceMatch::App(id.as_bytes().to_vec()))
    }
}

/// Decode an even-length string of hex digits (either case) to its bytes; any odd
/// length or non-hex digit is a rejection.
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    let digits = s.as_bytes();
    if digits.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(digits.len() / 2);
    for pair in digits.chunks_exact(2) {
        out.push((unhex(pair[0])? << 4) | unhex(pair[1])?);
    }
    Some(out)
}

fn unhex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

impl Authorizer for Acl {
    fn authorize(&self, identity: &Identity, action: Action, resource: &Resource) -> bool {
        matches!(self.decide(identity, action, resource), Decision::Allow)
    }

    /// Deny-wins over the matching rules, then explicit-allow; a request no rule
    /// mentions is [`Abstain`](Decision::Abstain), not a deny — so the schema
    /// tier can still speak for it. (A standalone [`Acl`] resolves that abstain to
    /// a deny through [`authorize`](Acl::authorize); only the layered
    /// [`authorized`] evaluation consults the schema.)
    fn decide(&self, identity: &Identity, action: Action, resource: &Resource) -> Decision {
        let mut allowed = false;
        for rule in &self.rules {
            if rule.matches(identity, action, resource) {
                match rule.effect {
                    Effect::Deny => return Decision::Deny,
                    Effect::Allow => allowed = true,
                }
            }
        }
        if allowed {
            Decision::Allow
        } else {
            Decision::Abstain
        }
    }
}

/// The composed authorization decision the data-plane enforcement points make:
/// the deployment [`Authorizer`] first, then — only where it abstains — the room's
/// live doc-ACL tuple tier (`doc_acl`), then the room's governing schema `@auth`
/// grants, then a terminal default-deny. This is the decision flow (explicit deny →
/// explicit allow → per-actor doc-ACL grant → schema role-grant → default-deny).
///
/// `doc_acl` is the room's doc-ACL tuple tier verdict, pre-resolved by
/// [`doc_acl_tier`] over the room's live ACL records and creator; it is
/// [`Abstain`](Decision::Abstain) for a resource with no governing doc-ACL state
/// (an app resource, or a room with no creator and no tuples), so the composition
/// then behaves exactly as the deployment-plus-schema tiers alone. A deployment
/// `Deny`/`Allow` is terminal and never reaches it — a doc-ACL grant cannot re-open
/// what the deployment explicitly refused.
///
/// `schema` is the schema governing the request's room, or `None` for a relay
/// room (then the deployment authorizer and doc-ACL are the whole decision).
pub fn authorized(
    deployment: &dyn Authorizer,
    doc_acl: Decision,
    schema: Option<&Schema>,
    identity: &Identity,
    action: Action,
    resource: &Resource,
) -> bool {
    let granted = match deployment.decide(identity, action, resource) {
        Decision::Allow => true,
        Decision::Deny => false,
        Decision::Abstain => match doc_acl {
            Decision::Allow => true,
            Decision::Deny => false,
            Decision::Abstain => matches!(
                schema.map_or(Decision::Abstain, |s| schema_decision(
                    s.auth(),
                    identity,
                    action
                )),
                Decision::Allow
            ),
        },
    };
    // Report the composed verdict, not the deployment tier's — an auditing
    // authorizer records what was actually enforced, doc-ACL and schema grants
    // included.
    deployment.observe(identity, action, resource, granted);
    granted
}

/// The root path the doc-ACL tier evaluates at — a `core::path` key sequence with
/// no keys, which [`governs`](crdtsync_core::acl) every path. The first cut
/// evaluates whole-document (`/`) authority only, because the room-level
/// [`Resource`] carries no path; a path-carrying `Resource` widens it to a subtree
/// later.
///
/// Consequence, until then: a tuple scoped to a *subtree* — an allow **or a deny** —
/// does not `govern` the root query, so it is inert here. A subtree allow grants
/// nothing yet (fail-closed), and a subtree deny blocks nothing yet (fail-open at
/// this seam) — only whole-document (`/`) tuples are enforced. The op-submit gate is
/// room-level (it cannot see which path an op targets), so this is a scope boundary,
/// not a per-path decision that leaks.
fn root_path() -> Vec<u8> {
    crdtsync_core::path::encode_path(&[])
}

/// The doc-ACL tuple tier's verdict on whether `identity` may take `action` on a
/// room, over its live ACL `records` and `creator`. Composed between the
/// deployment and schema tiers by [`authorized`].
///
/// The room's `creator` (an authenticated actor) is the un-granted owner of the
/// root — it holds every capability with no explicit tuple (creator-auto-owns-`/`).
/// Every other verdict is a grant the creator (or a delegate it owns from) authored:
/// core's [`decide_capability_with_authority`] resolves attenuated delegation,
/// provenance-bounded deny, and deny-overrides over the record set. A room with no
/// creator has no doc-ACL authority root, so the tier abstains (its tuple set is
/// empty until a first write establishes the creator).
///
/// The querying actor's ACL identity is derived from the credential [`Identity`]:
/// its actor id keys the [`AclSubject::Actor`](crdtsync_core::acl::AclSubject) /
/// creator / grantor match via [`actor_key`], its groups and roles ride from the
/// credential's claims, and its authentication state distinguishes
/// `Authenticated` from `Anonymous`. `RegisterSchema` is a control-plane action
/// with no capability form, so it always abstains.
pub fn doc_acl_tier(
    records: &[AclRecord],
    creator: Option<&[u8]>,
    index: &HashMap<ElementId, Vec<Vec<u8>>>,
    identity: &Identity,
    action: Action,
) -> Decision {
    let Some(capability) = capability_for(action) else {
        return Decision::Abstain;
    };
    doc_acl_at(records, creator, index, identity, &root_path(), capability)
}

/// The doc-ACL tuple tier's verdict on whether `identity` holds `capability` at
/// `path`, over the room's live `records` and `creator` — the one resolver both the
/// root-scoped write/read gate ([`doc_acl_tier`]) and the per-path read redaction
/// ([`doc_acl_read_at`]) share, so a single change to how a doc-ACL verdict resolves
/// cannot let the two enforcement points drift. A room with no creator holds no
/// authority root (and, since a tuple is authored by a write that establishes the
/// creator, no tuples either), so it abstains.
fn doc_acl_at(
    records: &[AclRecord],
    creator: Option<&[u8]>,
    index: &HashMap<ElementId, Vec<Vec<u8>>>,
    identity: &Identity,
    path: &[u8],
    capability: Capability,
) -> Decision {
    let Some(creator) = creator else {
        return Decision::Abstain;
    };
    let actor = actor_acl(identity);
    let resolve = element_resolver(index);
    match decide_capability_with_authority(
        records,
        actor_key(creator),
        &actor,
        path,
        capability,
        &resolve,
    ) {
        AclDecision::Allow => Decision::Allow,
        AclDecision::Deny => Decision::Deny,
        AclDecision::Abstain => Decision::Abstain,
    }
}

/// The doc-ACL tuple tier's **Read** verdict for `identity` at `path`, over the
/// room's live `records` and `creator` — the per-path form the outbound fan-out and
/// catch-up redaction evaluate against each op's document path. The creator owns `/`
/// and so reads every path; a room with no creator abstains (no authority root).
/// Mirrors [`doc_acl_tier`] but at an arbitrary path with [`Capability::Read`].
pub fn doc_acl_read_at(
    records: &[AclRecord],
    creator: Option<&[u8]>,
    index: &HashMap<ElementId, Vec<Vec<u8>>>,
    identity: &Identity,
    path: &[u8],
) -> Decision {
    doc_acl_at(records, creator, index, identity, path, Capability::Read)
}

/// The doc-ACL tuple tier's **Write** verdict for `identity` at `path`, over the
/// room's live `records` and `creator` — the per-path form the cross-zone-move token
/// issuance evaluates against the element's location and the destination zone's root.
/// The creator owns `/` and so writes every path; a room with no creator abstains
/// (no authority root). Mirrors [`doc_acl_read_at`] but with [`Capability::Write`].
pub fn doc_acl_write_at(
    records: &[AclRecord],
    creator: Option<&[u8]>,
    index: &HashMap<ElementId, Vec<Vec<u8>>>,
    identity: &Identity,
    path: &[u8],
) -> Decision {
    doc_acl_at(records, creator, index, identity, path, Capability::Write)
}

/// Whether `identity` holds doc-ACL read on the room at *any* path — the room's
/// creator (owns `/`), or the subject of a rooted read/own grant on some subtree.
/// The room-connect read gate widens to this: a subtree-scoped reader that abstains
/// at the root must still be admitted to subscribe, since the per-op redaction then
/// serves it exactly the subtrees it is granted. Empty/creatorless rooms hold no
/// such grant, so the gate falls back to the deployment/schema decision unchanged.
pub fn has_any_read_grant(
    records: &[AclRecord],
    creator: Option<&[u8]>,
    index: &HashMap<ElementId, Vec<Vec<u8>>>,
    identity: &Identity,
) -> bool {
    let Some(creator) = creator else {
        return false;
    };
    let creator_key = actor_key(creator);
    let actor = actor_acl(identity);
    if actor.id == creator_key {
        return true;
    }
    let resolve = element_resolver(index);
    // The distinct current paths the grants govern — an element scope resolved to
    // its element's live path, an unresolvable one dropped (it grants nothing).
    let mut paths: Vec<Vec<u8>> = records
        .iter()
        .filter_map(|r| scope_path(index, &r.tuple.scope))
        .collect();
    paths.sort_unstable();
    paths.dedup();
    paths.into_iter().any(|path| {
        matches!(
            decide_capability_with_authority(
                records,
                creator_key,
                &actor,
                &path,
                Capability::Read,
                &resolve,
            ),
            AclDecision::Allow
        )
    })
}

/// Whether `identity` may read the document `op_path`, composing the doc-ACL read
/// tier at that path with the deployment and schema tiers exactly as the write gate
/// composes — the per-recipient outbound read check. A recipient is served an op
/// only when this holds for the op's path, so an unauthorized subtree's ops are
/// withheld from it while an authorized peer still receives them.
#[allow(clippy::too_many_arguments)]
pub fn recipient_reads_path(
    deployment: &dyn Authorizer,
    records: &[AclRecord],
    creator: Option<&[u8]>,
    index: &HashMap<ElementId, Vec<Vec<u8>>>,
    schema: Option<&Schema>,
    identity: &Identity,
    room: &[u8],
    op_path: &[u8],
) -> bool {
    let doc_acl = doc_acl_read_at(records, creator, index, identity, op_path);
    authorized(
        deployment,
        doc_acl,
        schema,
        identity,
        Action::Read,
        &Resource::Room(room),
    )
}

/// A path *predicate* over `core::path` **segments** — [`recipient_reads_path`] with its
/// authority context bound, bridging the segment form the core read projections
/// ([`Document::project_read_paths`](crdtsync_core::Document::project_read_paths),
/// [`reveal_ops`](crdtsync_core::Document::reveal_ops)) call to the encoded path the
/// verdict is computed at. The one place the encode-then-decide bridge lives, so the
/// snapshot projection, the catch-up reveal, and the live-fan-out reveal cannot drift
/// apart — the property their op-join≡snapshot-join convergence rests on.
pub fn recipient_reads_predicate<'a>(
    deployment: &'a dyn Authorizer,
    records: &'a [AclRecord],
    creator: Option<&'a [u8]>,
    index: &'a HashMap<ElementId, Vec<Vec<u8>>>,
    schema: Option<&'a Schema>,
    identity: &'a Identity,
    room: &'a [u8],
) -> impl Fn(&[Vec<u8>]) -> bool + 'a {
    move |path: &[Vec<u8>]| {
        let encoded = encode_path(&path.iter().map(Vec::as_slice).collect::<Vec<_>>());
        recipient_reads_path(
            deployment, records, creator, index, schema, identity, room, &encoded,
        )
    }
}

/// Whether `identity` may read the **entire** document — the gate for serving it an
/// unredacted whole-replica snapshot. Whole-document root read is necessary but not
/// sufficient: a downstream `Deny(Read)` (or `Deny(Own)`) carves a subtree out of an
/// otherwise whole-document grant — the AWS-style deny the model supports — and a
/// root-only check cannot see it, since a descendant deny does not `govern` the root
/// query. So this also re-checks read at every governing tuple path and treats a
/// reader denied at any of them as **partial**: it is refused the snapshot rather
/// than served a carved-out subtree the per-op fan-out correctly withholds.
///
/// A deployment read-*allow* is terminal — it grants the whole room regardless of
/// any doc-ACL deny, and the per-op fan-out is then equally unredacted for the
/// reader — so it needs no carve-out scan. A creatorless room has no doc-ACL
/// authority (only the deployment/schema gate spoke), so root read is the whole
/// answer.
pub fn reads_whole_document(
    deployment: &dyn Authorizer,
    records: &[AclRecord],
    creator: Option<&[u8]>,
    index: &HashMap<ElementId, Vec<Vec<u8>>>,
    schema: Option<&Schema>,
    identity: &Identity,
    room: &[u8],
) -> bool {
    let root = encode_path(&[]);
    if !recipient_reads_path(
        deployment, records, creator, index, schema, identity, room, &root,
    ) {
        return false;
    }
    if matches!(
        deployment.decide(identity, Action::Read, &Resource::Room(room)),
        Decision::Allow
    ) {
        return true;
    }
    let Some(creator) = creator else {
        return true;
    };
    // Root read here is doc-ACL-granted (a root grant or the creator). Any effective
    // read-deny at a governing tuple path is a subtree carve-out, so the reader does
    // not read the whole document.
    let actor = actor_acl(identity);
    let creator_key = actor_key(creator);
    let resolve = element_resolver(index);
    let mut paths: Vec<Vec<u8>> = records
        .iter()
        .filter_map(|r| scope_path(index, &r.tuple.scope))
        .collect();
    paths.sort_unstable();
    paths.dedup();
    !paths.into_iter().any(|path| {
        matches!(
            decide_capability_with_authority(
                records,
                creator_key,
                &actor,
                &path,
                Capability::Read,
                &resolve,
            ),
            AclDecision::Deny
        )
    })
}

/// The document path an op reads — the `core::path` key sequence its target
/// resolves to, for the per-recipient read redaction. `index` maps each live
/// container element id to its key path (see
/// [`Hub::element_paths`](crate::Hub::element_paths)); a keyed op appends its slot
/// key, an id-addressed op takes its target container's own path.
///
/// An op whose target the index does not resolve — a since-deleted container, a
/// composite annotation payload the walk does not enter — resolves to the **root**,
/// so only a whole-document reader carries it. Root is the strictest read authority
/// (it governs no narrower than `/`), so an unresolved op never leaks to a
/// subtree-scoped reader, yet a whole-document reader (the creator, a root grantee)
/// still receives it and stays convergent.
///
/// An ACL op is redacted by the document path it governs, not by root: ACL state is
/// itself privacy-sensitive — a tuple reveals a subject, an effect, and the existence
/// of a governed path — so a recipient receives an [`AclGrant`](OpKind::AclGrant) only
/// where it may read that grant's path, and an [`AclRevoke`](OpKind::AclRevoke) only
/// where it may read the tombstoned tuple's path (resolved through `records`, the full
/// tuple set the server holds) — so a recipient sees the revoke exactly where it saw,
/// or would have seen, the grant. A revoke naming an id no held tuple carries falls back
/// to the root (a moot revoke reaching only a whole-document reader).
///
/// A RangedElement op is governed by the *set* of its anchor-sequence paths, which a
/// single path cannot express: it goes through [`op_read_paths`], the multi-path front
/// this function's single-path cases fold into. Here the three Ranged ops fall to the
/// root as the single-path degenerate — the whole-document reader always carries them —
/// but the fan-out and catch-up filters resolve them through [`op_read_paths`], never
/// this arm.
pub fn op_read_path(
    index: &HashMap<ElementId, Vec<Vec<u8>>>,
    records: &[AclRecord],
    op: &Op,
) -> Vec<u8> {
    let root = || encode_path(&[]);
    let encode =
        |segs: &[Vec<u8>]| encode_path(&segs.iter().map(|s| s.as_slice()).collect::<Vec<_>>());
    match &op.kind {
        // A keyed op addresses a slot in its target map: the target's path plus key.
        OpKind::RegisterSet { key, .. }
        | OpKind::CounterInc { key, .. }
        | OpKind::CounterDec { key, .. }
        | OpKind::MapSet { key, .. }
        | OpKind::MapDelete { key }
        | OpKind::MapCreate { key }
        | OpKind::ListCreate { key }
        | OpKind::TextCreate { key }
        | OpKind::XmlElementCreate { key, .. }
        | OpKind::XmlFragmentCreate { key } => match index.get(&op.target) {
            Some(base) => {
                let mut segs = base.clone();
                segs.push(key.clone());
                encode(&segs)
            }
            None => root(),
        },
        // An id-addressed op mutates its target container in place: its own path.
        OpKind::ListInsert { .. }
        | OpKind::ListDelete { .. }
        | OpKind::TextInsert { .. }
        | OpKind::TextDelete { .. }
        | OpKind::XmlInsertChild { .. }
        | OpKind::XmlMove { .. } => resolve_read_path(index, op.target),
        // A RangedElement op is governed by the set of its anchor-sequence paths, which
        // `op_read_paths` resolves; the single-path form folds it to root (the whole-
        // document reader always carries it) and is never taken by the redaction filters.
        OpKind::RangedCreate { .. }
        | OpKind::RangedSetPayload { .. }
        | OpKind::RangedDelete { .. } => root(),
        // An `XmlReveal` is a redaction-time synthesis injected for a specific reader
        // (reveal-on-move-in), never a committed op the redaction filters gate — but as
        // the strictest read authority it folds to root here, so it can never leak.
        OpKind::XmlReveal { .. } => root(),
        // An ACL grant is gated by the path its scope governs: a fixed path directly,
        // an element scope resolved to its element's current path through `index` (so
        // the grant op reaches exactly the readers of the element's live location). An
        // unresolvable element scope falls back to the root, reaching only a
        // whole-document reader.
        OpKind::AclGrant { scope, .. } => {
            scope_path(index, scope).unwrap_or_else(|| encode_path(&[]))
        }
        // A revoke names only the tombstoned tuple's id; gate it by that tuple's
        // governing path so a recipient sees the revoke where it may read the grant.
        // For a `Path` scope that is the same fixed path the grant was gated at; an
        // `Element` scope gates at the element's *current* path, so — as with the grant
        // itself — the revoke's audience follows the element, and a move between a grant
        // and its revoke shifts both with the element (consistent with the grant's
        // enforcement, which likewise tracks the element). An id resolving to no held
        // tuple is a moot revoke → root.
        OpKind::AclRevoke { id } => records
            .iter()
            .find(|r| r.tuple.id == *id)
            .map_or_else(root, |r| {
                scope_path(index, &r.tuple.scope).unwrap_or_else(|| encode_path(&[]))
            }),
    }
}

/// The set of document paths an op reads — the governing paths a recipient must
/// **all** be able to read to receive it. For every op but the three RangedElement
/// ops this is the single [`op_read_path`] wrapped in a one-element vec; a
/// RangedElement is redacted by the path of *every* sequence its endpoints anchor
/// (a require-all rule), since a mark/annotation reveals content-region info at both
/// endpoints — a reader that cannot read where the range starts **or** ends must not
/// see it. The common single-sequence mark yields one governing path; a cross-element
/// range yields two.
///
/// A [`RangedCreate`](OpKind::RangedCreate) carries its `start`/`end` anchors, so its
/// governing seqs are read straight off the op. A
/// [`RangedSetPayload`](OpKind::RangedSetPayload) /
/// [`RangedDelete`](OpKind::RangedDelete) carries only the RangedElement id, so its
/// anchors resolve through `ranged` — the server's held anchor set, tombstoned ranges
/// included (a delete's target is already tombstoned) — exactly as a revoke resolves
/// its tuple through `records`. An id that resolves to no held range is one the
/// recipient never received (its region was unreadable): it falls back to the root,
/// reaching only a whole-document reader. Each governing seq resolves to its path
/// through `index`; an unresolved seq falls back to the root, so only a whole-document
/// reader carries the op — never a subtree-scoped one.
pub fn op_read_paths(
    index: &HashMap<ElementId, Vec<Vec<u8>>>,
    ranged: &HashMap<ElementId, (ElementId, ElementId)>,
    records: &[AclRecord],
    op: &Op,
) -> Vec<Vec<u8>> {
    match &op.kind {
        OpKind::RangedCreate { start, end, .. } => anchor_paths(index, start.seq, end.seq),
        OpKind::RangedSetPayload { id, .. } | OpKind::RangedDelete { id } => match ranged.get(id) {
            Some((start, end)) => anchor_paths(index, *start, *end),
            None => vec![encode_path(&[])],
        },
        _ => vec![op_read_path(index, records, op)],
    }
}

/// The distinct governing paths of a range's two anchor sequences: each seq's own
/// path through `index`, an unresolved seq falling back to the root. Deduped, so the
/// common single-sequence mark yields one path and a cross-element range two.
fn anchor_paths(
    index: &HashMap<ElementId, Vec<Vec<u8>>>,
    start_seq: ElementId,
    end_seq: ElementId,
) -> Vec<Vec<u8>> {
    let start = resolve_read_path(index, start_seq);
    let end = resolve_read_path(index, end_seq);
    if start == end {
        vec![start]
    } else {
        vec![start, end]
    }
}

/// The encoded `core::path` element `id` resolves to through the context `index`, or
/// the root when the index does not hold it — a since-deleted or otherwise unindexed
/// target reads at the root, so only a whole-document reader carries an op naming it.
fn resolve_read_path(index: &HashMap<ElementId, Vec<Vec<u8>>>, id: ElementId) -> Vec<u8> {
    index.get(&id).map_or_else(
        || encode_path(&[]),
        |segs| encode_path(&segs.iter().map(|s| s.as_slice()).collect::<Vec<_>>()),
    )
}

/// The current path a grant's [`AclScope`] governs, or `None` when it does not
/// resolve — a fixed [`Path`](AclScope::Path) is its own bytes; an
/// [`Element`](AclScope::Element) resolves to the element's current path through the
/// room's `index`, so an element-scoped grant follows the element across a tree-move.
/// An id the index does not hold yields `None`: the grant is inert (fail-closed).
fn scope_path(index: &HashMap<ElementId, Vec<Vec<u8>>>, scope: &AclScope) -> Option<Vec<u8>> {
    match scope {
        AclScope::Path(p) => Some(p.clone()),
        AclScope::Element(id) => index
            .get(id)
            .map(|segs| encode_path(&segs.iter().map(|s| s.as_slice()).collect::<Vec<_>>())),
    }
}

/// The core [`PathResolver`] the doc-ACL evaluator resolves an element-scoped grant
/// through: the room's element-context `index`. This is the one seam that carries
/// element scopes into the pure evaluator, so the write gate, per-op read redaction,
/// and snapshot projection all resolve an element to the same current path.
fn element_resolver(
    index: &HashMap<ElementId, Vec<Vec<u8>>>,
) -> impl Fn(ElementId) -> Option<Vec<u8>> + '_ {
    move |id| scope_path(index, &AclScope::Element(id))
}

/// The doc-ACL [`Capability`] a data-plane [`Action`] resolves to, or `None` for
/// `RegisterSchema` — a control-plane meta-auth with no doc-level capability form.
fn capability_for(action: Action) -> Option<Capability> {
    match action {
        Action::Read => Some(Capability::Read),
        Action::Write => Some(Capability::Write),
        Action::PublishAwareness => Some(Capability::PublishAwareness),
        // The audit-only operator actions carry no doc-level capability form —
        // they name auditable events, not doc-ACL-governed subtree powers.
        Action::RegisterSchema | Action::Connect | Action::Export | Action::VersionRead => None,
    }
}

/// The doc-ACL querying actor built from the credential [`Identity`]: the actor id
/// keyed via [`actor_key`], the credential's groups and roles carried as byte
/// strings, and the authentication state read from the actor prefix (an
/// [`ANON_PREFIX`] actor is anonymous).
fn actor_acl(identity: &Identity) -> AclActor {
    AclActor {
        id: actor_key(identity.actor()),
        groups: identity
            .groups()
            .iter()
            .map(|g| g.clone().into_bytes())
            .collect(),
        roles: identity
            .roles()
            .iter()
            .map(|r| r.clone().into_bytes())
            .collect(),
        authenticated: is_authenticated(identity.actor()),
    }
}

/// Whether `actor` is a credentialed (non-anonymous) actor — the doc-ACL
/// `Authenticated`/`Anonymous` distinction, read from the [`ANON_PREFIX`] an
/// anonymous connection's id carries. The room creator must be one of these: an
/// anonymous id is ephemeral per-connection, so it would never re-present to
/// reclaim the ownership it was granted.
pub fn is_authenticated(actor: &[u8]) -> bool {
    !actor.starts_with(ANON_PREFIX)
}

/// The doc-ACL actor key for a credential actor: a stable 16-byte id derived from
/// the (variable-length) actor bytes by truncating their SHA-256 digest. Core's
/// ACL set matches an actor, a grantor, and the creator by this fixed-width key, so
/// the *authenticated actor* — not the ephemeral per-device `ClientId` — is the ACL
/// principal: the same human across two devices presents the same credential-derived
/// actor, derives the same key, and so is one ACL subject. The derivation is fixed
/// (SHA-256, never a process-seeded hash), so a persisted grant's embedded key
/// re-matches after a restart.
pub fn actor_key(actor: &[u8]) -> ClientId {
    let digest = Sha256::digest(actor);
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    ClientId::from_bytes(bytes)
}

/// The schema `@auth` grant tier: deny-wins over the whole-document grants whose
/// action matches, then explicit-allow, then [`Abstain`](Decision::Abstain).
///
/// Only root (`/`) grants are evaluated — a path-scoped grant needs a
/// path-carrying resource the room-level check does not yet have, so it is left
/// unenforced (deferred with doc-ACL). Ownership templates (`${actor_id}` …)
/// never match: there is no ownership carrier at this tier yet.
fn schema_decision(auth: &Auth, identity: &Identity, action: Action) -> Decision {
    let Some(want) = schema_action(action) else {
        return Decision::Abstain;
    };
    let mut allowed = false;
    for grant in auth.grants() {
        if grant.action != want
            || grant.path != "/"
            || !grant_subject_matches(&grant.subject, identity)
        {
            continue;
        }
        match grant.effect {
            GrantEffect::Deny => return Decision::Deny,
            GrantEffect::Allow => allowed = true,
        }
    }
    if allowed {
        Decision::Allow
    } else {
        Decision::Abstain
    }
}

/// The schema action a data-plane [`Action`] maps to. `RegisterSchema` is a
/// control-plane meta-auth with no schema-grantable form, so it never resolves
/// through the grant tier.
fn schema_action(action: Action) -> Option<SchemaAction> {
    match action {
        Action::Read => Some(SchemaAction::Read),
        Action::Write => Some(SchemaAction::Write),
        Action::PublishAwareness => Some(SchemaAction::PublishAwareness),
        // The audit-only operator actions are not schema-grantable — they never
        // resolve through the `@auth` grant tier.
        Action::RegisterSchema | Action::Connect | Action::Export | Action::VersionRead => None,
    }
}

/// Whether a grant's subject covers `identity`. A role matches an asserted claim;
/// a class matches by credential kind; an ownership template never matches here.
fn grant_subject_matches(subject: &GrantSubject, identity: &Identity) -> bool {
    match subject {
        GrantSubject::Role(role) => identity.roles().iter().any(|r| r == role),
        GrantSubject::Class(SubjectClass::Authenticated) => {
            !identity.actor().starts_with(ANON_PREFIX)
        }
        GrantSubject::Class(SubjectClass::Anonymous) => identity.actor().starts_with(ANON_PREFIX),
        GrantSubject::Class(SubjectClass::Anyone) => true,
        GrantSubject::Template(_) => false,
    }
}
