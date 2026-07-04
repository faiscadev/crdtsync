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

/// Decides whether `actor` may take `action` on `resource`. Deployments supply
/// their own; the engine only asks.
pub trait Authorizer {
    fn authorize(&self, actor: &[u8], action: Action, resource: &Resource) -> bool;
}

/// An authorizer from a plain closure, so a deployment (or a test) can supply the
/// policy inline.
impl<F> Authorizer for F
where
    F: Fn(&[u8], Action, &Resource) -> bool,
{
    fn authorize(&self, actor: &[u8], action: Action, resource: &Resource) -> bool {
        self(actor, action, resource)
    }
}

/// Dev-mode authorizer: permits every action on every resource. No real policy —
/// for local development and tests only, never production.
pub struct PermitAll;

impl Authorizer for PermitAll {
    fn authorize(&self, _actor: &[u8], _action: Action, _resource: &Resource) -> bool {
        true
    }
}
