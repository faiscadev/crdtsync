//! The authorization decision seam.
//!
//! The engine ships no policy of its own — a deployment plugs in an
//! [`Authorizer`] that decides whether an authenticated actor may take an action
//! on a resource. It is the read/write analog of the [`Verifier`](crate::auth)
//! credential seam: the session consults it at every enforcement point, and the
//! server is the final authority. The contract is default-deny — an authorizer
//! returns `true` only for what it explicitly permits — though the dev-mode
//! [`PermitAll`] allows everything for local development and tests.
//!
//! Resources cover rooms (the data plane) and apps (the schema-registry control
//! plane); the room case widens to path / element / branch as those land,
//! without disturbing the trait.
//!
//! A check carries the full [`Identity`] the [`Verifier`](crate::auth) derived,
//! not just its actor id, so an authorizer can match a rule by the credential's
//! roles and groups — not only by who the actor is.

use crate::auth::Identity;

/// What an actor is attempting.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Action {
    /// Read a room's ops — subscribing and receiving the catch-up.
    Read,
    /// Write ops into a room.
    Write,
    /// Publish an ephemeral awareness entry to a room.
    PublishAwareness,
    /// Register (or migrate) an app's schema — the app-admin meta-auth on an
    /// [`Resource::App`], distinct from any data-plane room action.
    RegisterSchema,
}

/// What an [`Action`] targets: a room (the data plane) or an app (the schema
/// registry control plane). Room-level within a room for now; a later variant
/// carries a path or element id for fine-grained checks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Resource<'a> {
    /// A room, named by its id.
    Room(&'a [u8]),
    /// An app, named by its `app_id` — the target of schema registration.
    App(&'a [u8]),
}

/// One layer's verdict on a request. [`Abstain`](Decision::Abstain) is "no
/// opinion" — it lets a lower-priority tier decide, where a bare `bool` would
/// force a premature allow or deny. It is what lets the schema `@auth` grant tier
/// sit *below* a deployment's explicit allow/deny but *above* the terminal
/// default-deny: a deployment that abstains defers to the schema, and a schema
/// that abstains falls through to default-deny.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Decision {
    Allow,
    Deny,
    Abstain,
}

/// Decides whether `identity` may take `action` on `resource`. Deployments supply
/// their own; the engine only asks.
pub trait Authorizer {
    /// The final verdict: `true` only for what is explicitly permitted.
    fn authorize(&self, identity: &Identity, action: Action, resource: &Resource) -> bool;

    /// The three-valued verdict the layered evaluation composes on. A plain
    /// `bool` authorizer never abstains — its `false` is an explicit deny, its
    /// final word — so the schema tier is consulted only under an authorizer that
    /// can [`Abstain`](Decision::Abstain) (the [`Acl`](crate::acl::Acl)). A
    /// deployment wanting the schema to fill its gaps supplies one that abstains.
    fn decide(&self, identity: &Identity, action: Action, resource: &Resource) -> Decision {
        if self.authorize(identity, action, resource) {
            Decision::Allow
        } else {
            Decision::Deny
        }
    }

    /// Observe the *final* enforced verdict for a request — after the schema
    /// `@auth` tier has resolved any deployment [`Abstain`](Decision::Abstain).
    /// The composition ([`authorized`](crate::acl::authorized)) calls it once per
    /// data-plane access; a plain authorizer ignores it, an auditing one records
    /// the true decision (which [`decide`](Authorizer::decide) alone cannot know,
    /// since a lower tier may still turn its abstain into an allow).
    fn observe(&self, identity: &Identity, action: Action, resource: &Resource, granted: bool) {
        let _ = (identity, action, resource, granted);
    }
}

/// An authorizer from a plain closure, so a deployment (or a test) can supply the
/// policy inline.
impl<F> Authorizer for F
where
    F: Fn(&Identity, Action, &Resource) -> bool,
{
    fn authorize(&self, identity: &Identity, action: Action, resource: &Resource) -> bool {
        self(identity, action, resource)
    }
}

/// Dev-mode authorizer: permits every action on every resource. No real policy —
/// for local development and tests only, never production.
pub struct PermitAll;

impl Authorizer for PermitAll {
    fn authorize(&self, _identity: &Identity, _action: Action, _resource: &Resource) -> bool {
        true
    }
}
