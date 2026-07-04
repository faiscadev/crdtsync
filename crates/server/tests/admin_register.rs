//! The schema-registration admin handler — the control-plane operation that
//! registers a schema version, gated by the same verifier + authorizer seams the
//! data plane uses.
//!
//! A request authenticates its credential to an `Identity`, then the authorizer
//! decides `RegisterSchema` on the request's `App(app_id)`, and only a permitted
//! identity reaches the registry. The order is authenticate → authorize →
//! register, so an absent/unknown credential is `Unauthenticated` and an
//! authenticated-but-unpermitted one is `Forbidden`, each before any chain
//! write; a hash-lock refusal surfaces as `Rejected`.

use crdtsync_server::admin::{register_schema, RegisterOutcome, RegisterRequest};
use crdtsync_server::schema_registry::{RegisterError, Registered, SchemaRegistry};
use crdtsync_server::{Action, Authorizer, Resource, StaticTokens};

const APP: &[u8] = b"app-x";
const S1: &[u8] = br#"{"schema":1}"#;
const S2: &[u8] = br#"{"schema":2}"#;

/// A verifier that maps `admin-cred`→actor `admin` and `user-cred`→actor `user`.
fn verifier() -> StaticTokens {
    let mut t = StaticTokens::new();
    t.insert(b"admin-cred".to_vec(), b"admin".to_vec());
    t.insert(b"user-cred".to_vec(), b"user".to_vec());
    t
}

/// An authorizer that grants `register_schema` to actor `admin` on `app-x` only.
fn only_admin_on_app_x() -> impl Authorizer {
    |actor: &[u8], action: Action, res: &Resource| {
        action == Action::RegisterSchema
            && actor == b"admin"
            && matches!(res, Resource::App(a) if *a == APP)
    }
}

fn req<'a>(
    app: &'a [u8],
    version: u32,
    schema: &'a [u8],
    cred: Option<&'a [u8]>,
) -> RegisterRequest<'a> {
    RegisterRequest {
        app_id: app,
        version,
        schema,
        migration: b"",
        credential: cred,
    }
}

#[test]
fn a_permitted_admin_registers_a_schema() {
    let mut reg = SchemaRegistry::new();
    let out = register_schema(
        &req(APP, 1, S1, Some(b"admin-cred")),
        &verifier(),
        &only_admin_on_app_x(),
        &mut reg,
    );
    assert_eq!(out, RegisterOutcome::Accepted(Registered::Appended));
    assert_eq!(
        reg.resolve(APP, 1),
        Some(S1),
        "the schema is now registered"
    );
}

#[test]
fn an_absent_credential_is_unauthenticated() {
    let mut reg = SchemaRegistry::new();
    let out = register_schema(
        &req(APP, 1, S1, None),
        &verifier(),
        &only_admin_on_app_x(),
        &mut reg,
    );
    assert_eq!(out, RegisterOutcome::Unauthenticated);
    assert_eq!(reg.head_version(APP), None, "nothing was registered");
}

#[test]
fn an_unknown_credential_is_unauthenticated() {
    let mut reg = SchemaRegistry::new();
    let out = register_schema(
        &req(APP, 1, S1, Some(b"bogus")),
        &verifier(),
        &only_admin_on_app_x(),
        &mut reg,
    );
    assert_eq!(out, RegisterOutcome::Unauthenticated);
    assert_eq!(reg.head_version(APP), None);
}

#[test]
fn an_authenticated_but_unpermitted_credential_is_forbidden() {
    let mut reg = SchemaRegistry::new();
    // `user` authenticates but is not granted register_schema.
    let out = register_schema(
        &req(APP, 1, S1, Some(b"user-cred")),
        &verifier(),
        &only_admin_on_app_x(),
        &mut reg,
    );
    assert_eq!(out, RegisterOutcome::Forbidden);
    assert_eq!(
        reg.head_version(APP),
        None,
        "a forbidden request never writes"
    );
}

#[test]
fn the_authorizer_is_scoped_to_the_request_app() {
    let mut reg = SchemaRegistry::new();
    // The same admin is permitted on app-x but not on app-y.
    let denied = register_schema(
        &req(b"app-y", 1, S1, Some(b"admin-cred")),
        &verifier(),
        &only_admin_on_app_x(),
        &mut reg,
    );
    assert_eq!(denied, RegisterOutcome::Forbidden);
    let allowed = register_schema(
        &req(APP, 1, S1, Some(b"admin-cred")),
        &verifier(),
        &only_admin_on_app_x(),
        &mut reg,
    );
    assert_eq!(allowed, RegisterOutcome::Accepted(Registered::Appended));
}

#[test]
fn a_hash_lock_refusal_surfaces_as_rejected() {
    let mut reg = SchemaRegistry::new();
    let v = verifier();
    let a = only_admin_on_app_x();
    register_schema(&req(APP, 1, S1, Some(b"admin-cred")), &v, &a, &mut reg);
    // Re-registering version 1 with a different body is a locked-content change.
    let out = register_schema(&req(APP, 1, S2, Some(b"admin-cred")), &v, &a, &mut reg);
    assert_eq!(
        out,
        RegisterOutcome::Rejected(RegisterError::HashMismatch { version: 1 })
    );
    // A skip-ahead is likewise refused, not accepted.
    let gap = register_schema(&req(APP, 3, S2, Some(b"admin-cred")), &v, &a, &mut reg);
    assert_eq!(
        gap,
        RegisterOutcome::Rejected(RegisterError::Gap {
            expected: 2,
            got: 3
        })
    );
}

#[test]
fn an_identical_retry_is_accepted_unchanged() {
    let mut reg = SchemaRegistry::new();
    let v = verifier();
    let a = only_admin_on_app_x();
    register_schema(&req(APP, 1, S1, Some(b"admin-cred")), &v, &a, &mut reg);
    let retry = register_schema(&req(APP, 1, S1, Some(b"admin-cred")), &v, &a, &mut reg);
    assert_eq!(retry, RegisterOutcome::Accepted(Registered::Unchanged));
}

#[test]
fn authentication_precedes_the_registry() {
    let mut reg = SchemaRegistry::new();
    // An unauthenticated request that would also be a gap reports the auth
    // failure, not the chain refusal — auth is checked before the registry.
    let out = register_schema(
        &req(APP, 5, S1, None),
        &verifier(),
        &only_admin_on_app_x(),
        &mut reg,
    );
    assert_eq!(out, RegisterOutcome::Unauthenticated);
    assert_eq!(reg.head_version(APP), None);
}
