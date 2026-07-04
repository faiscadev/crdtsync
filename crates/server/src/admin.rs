//! The schema-registration admin surface — the control-plane operation an app
//! owner's CI performs to register a schema version.
//!
//! Registration is separate from the data-plane sync connection but reuses its
//! trust seams: the request's credential authenticates to an [`Identity`] via the
//! [`Verifier`], and the [`Authorizer`] decides the `RegisterSchema` action on
//! the request's [`Resource::App`] — the app-admin meta-auth, distinct from any
//! room action. Only a permitted identity reaches the [`SchemaRegistry`]. This
//! handler is pure over an already-decoded request; the HTTP transport that
//! decodes one and maps the outcome to a status is a separate layer.
//!
//! [`Identity`]: crate::auth::Identity

use crate::auth::Verifier;
use crate::authz::{Action, Authorizer, Resource};
use crate::schema_registry::{RegisterError, Registered, SchemaRegistry};

/// A decoded registration request: which app and version, the schema and
/// migration bytes to lock, and the credential presented on the admin transport
/// (absent if none was supplied).
pub struct RegisterRequest<'a> {
    pub app_id: &'a [u8],
    pub version: u32,
    pub schema: &'a [u8],
    pub migration: &'a [u8],
    pub credential: Option<&'a [u8]>,
}

/// The outcome of an admin registration, which the transport maps to a status.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RegisterOutcome {
    /// Registered — either a new version or an idempotent no-op retry.
    Accepted(Registered),
    /// No credential was presented, or the verifier rejected the one that was:
    /// the caller is not an authenticated identity.
    Unauthenticated,
    /// An authenticated identity without the `register_schema` capability on the
    /// request's app.
    Forbidden,
    /// The registry refused the chain write — a gap, a backward version, or a
    /// content change under a locked version.
    Rejected(RegisterError),
}

/// Register a schema version on behalf of `req`'s credential: authenticate it,
/// authorize `RegisterSchema` on the request's app, then append to `registry` —
/// authenticate → authorize → register, the order every data-plane enforcement
/// point uses, so an unauthenticated or forbidden request never reaches the
/// registry.
pub fn register_schema(
    req: &RegisterRequest,
    verifier: &dyn Verifier,
    authorizer: &dyn Authorizer,
    registry: &mut SchemaRegistry,
) -> RegisterOutcome {
    let Some(credential) = req.credential else {
        return RegisterOutcome::Unauthenticated;
    };
    let Some(identity) = verifier.verify(credential) else {
        return RegisterOutcome::Unauthenticated;
    };
    let resource = Resource::App(req.app_id);
    if !authorizer.authorize(identity.actor(), Action::RegisterSchema, &resource) {
        return RegisterOutcome::Forbidden;
    }
    match registry.register(req.app_id, req.version, req.schema, req.migration) {
        Ok(registered) => RegisterOutcome::Accepted(registered),
        Err(error) => RegisterOutcome::Rejected(error),
    }
}
