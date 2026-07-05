//! The access log — a record of every authorization decision.
//!
//! Authorization is the single seam every enforcement point consults, so it is
//! also the one place every access decision passes through. [`Audited`] wraps an
//! inner [`Authorizer`], forwarding its verdict unchanged while handing each
//! decision to a pluggable [`AccessLog`] sink. This captures read-only accesses
//! (subscribe) that generate no op, alongside the writes the op log already
//! records with their actor and timestamp — together the authoritative audit
//! trail.
//!
//! A record carries the actor, the action, the resource, and the verdict. It
//! never carries the credential that authenticated the actor, nor an awareness
//! entry's key or value: an awareness publish is logged as *that a publish was
//! decided*, never as the ephemeral presence it carried.

use crate::auth::Identity;
use crate::authz::{Action, Authorizer, Resource};

/// How an access was decided.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Decision {
    Permitted,
    Denied,
}

/// One authorization decision, as handed to an [`AccessLog`]. Borrows the check's
/// inputs; a sink that retains records copies what it needs.
pub struct AccessRecord<'a> {
    pub actor: &'a [u8],
    pub action: Action,
    pub resource: &'a Resource<'a>,
    pub decision: Decision,
}

/// A sink for access decisions. A deployment plugs in its own — a file, a
/// database, a metrics pipeline; the engine only emits.
pub trait AccessLog {
    fn record(&self, record: &AccessRecord);
}

/// An access log from a plain closure, so a deployment (or a test) can supply the
/// sink inline.
impl<F> AccessLog for F
where
    F: Fn(&AccessRecord),
{
    fn record(&self, record: &AccessRecord) {
        self(record)
    }
}

/// An [`Authorizer`] that records every decision of an inner one. Compose it
/// around a real policy and set it as the registry's authorizer; the decision is
/// logged at the same instant it is enforced, so the log cannot drift from what
/// the server actually allowed.
pub struct Audited {
    inner: Box<dyn Authorizer>,
    log: Box<dyn AccessLog>,
}

impl Audited {
    pub fn new(inner: Box<dyn Authorizer>, log: Box<dyn AccessLog>) -> Self {
        Self { inner, log }
    }
}

impl Authorizer for Audited {
    fn authorize(&self, identity: &Identity, action: Action, resource: &Resource) -> bool {
        let allowed = self.inner.authorize(identity, action, resource);
        let decision = if allowed {
            Decision::Permitted
        } else {
            Decision::Denied
        };
        self.log.record(&AccessRecord {
            actor: identity.actor(),
            action,
            resource,
            decision,
        });
        allowed
    }
}
