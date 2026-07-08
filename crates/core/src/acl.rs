//! Doc-level ACL — the authorization tuple, a CRDT-merged doc-level set.
//!
//! An owner assigns a capability or a role, to an actor or a group, on a path,
//! with an allow or deny effect. Those grants live as document state: a set of
//! immutable [`AclTuple`]s keyed by a stable id, held on the [`Document`] beside
//! the annotation set and merged by the same laws (concurrent creates union to
//! distinct ids, a revoke tombstones, delete wins).
//!
//! Slice 1 is storage only. Core stores every tuple faithfully and merges the
//! set — it does **not** enforce authority: no provenance check on who may
//! revoke, no delegation or deny-precedence rules, no evaluation of a decision.
//! The merge is content-neutral (any tuple that arrives is stored); the server
//! gates who may emit one, and the evaluator resolves what the set grants, in
//! later slices.
//!
//! [`Document`]: crate::doc::Document

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
