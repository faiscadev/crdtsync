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
//! carries: its actor id, or the roles and groups its credential asserts. Schema
//! `@auth` role grants (decision-flow step 4) feed into this same evaluation once
//! the schema tier lands; here the flow is its explicit-tuple steps: deny-wins,
//! then allow, then default-deny.

use crate::auth::Identity;
use crate::authz::{Action, Authorizer, Resource};

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
}

impl ResourceMatch {
    fn matches(&self, resource: &Resource) -> bool {
        match (self, resource) {
            (ResourceMatch::AnyRoom, Resource::Room(_)) => true,
            (ResourceMatch::Room(name), Resource::Room(room)) => name.as_slice() == *room,
            (ResourceMatch::App(name), Resource::App(app)) => name.as_slice() == *app,
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
        let mut allowed = false;
        for rule in &self.rules {
            if rule.matches(identity, action, resource) {
                match rule.effect {
                    // An explicit deny wins outright, whatever else matched.
                    Effect::Deny => return false,
                    Effect::Allow => allowed = true,
                }
            }
        }
        // No matching allow (and no deny) is a denial: absence of a grant denies.
        allowed
    }
}
