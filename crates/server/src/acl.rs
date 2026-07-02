//! The ACL decision flow — a concrete [`Authorizer`] over a set of ACL tuples.
//!
//! A deployment builds an [`Acl`] from allow/deny rules and plugs it into the
//! server via [`Registry::set_authorizer`](crate::Registry::set_authorizer). A
//! check walks every matching rule with standard IAM semantics: an explicit
//! DENY always wins, an explicit ALLOW grants, and the absence of any matching
//! allow denies. This is the doc-independent policy engine; the doc-level
//! ACL-as-CRDT layer feeds its tuples into the same evaluation once it lands.
//!
//! A [`Subject`] is matched from the server-derived actor id alone — the only
//! thing an enforcement point carries. Role and group subjects need a claims
//! model the engine does not read yet; schema `@auth` role grants (decision-flow
//! step 4) need the schema layer. Neither participates here, so the flow reduces
//! to its explicit-tuple steps: deny-wins, then allow, then default-deny.

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

/// Who a rule applies to, matched against the server-derived actor id.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Subject {
    /// One specific actor id.
    Actor(Vec<u8>),
    /// Any credentialed (non-anonymous) actor — `authenticated:*`.
    Authenticated,
    /// Any anonymous actor — `anonymous:*`, an `anon:`-prefixed id.
    Anonymous,
    /// Anyone at all — `*`.
    Anyone,
}

impl Subject {
    fn matches(&self, actor: &[u8]) -> bool {
        match self {
            Subject::Actor(id) => id.as_slice() == actor,
            Subject::Authenticated => !actor.starts_with(ANON_PREFIX),
            Subject::Anonymous => actor.starts_with(ANON_PREFIX),
            Subject::Anyone => true,
        }
    }
}

/// Which resource a rule covers. Room-scoped today; a variant widens it to
/// path / element as [`Resource`] grows.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ResourceMatch {
    /// Every room.
    AnyRoom,
    /// One specific room.
    Room(Vec<u8>),
}

impl ResourceMatch {
    fn matches(&self, resource: &Resource) -> bool {
        let Resource::Room(room) = *resource;
        match self {
            ResourceMatch::AnyRoom => true,
            ResourceMatch::Room(name) => name.as_slice() == room,
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
    fn matches(&self, actor: &[u8], action: Action, resource: &Resource) -> bool {
        self.subject.matches(actor)
            && self.action.map_or(true, |a| a == action)
            && self.resource.matches(resource)
    }
}

/// A policy built from ACL tuples. Rules are order-independent — deny-wins makes
/// the result the same however they were added.
#[derive(Clone, Default)]
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

impl Authorizer for Acl {
    fn authorize(&self, actor: &[u8], action: Action, resource: &Resource) -> bool {
        let mut allowed = false;
        for rule in &self.rules {
            if rule.matches(actor, action, resource) {
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
