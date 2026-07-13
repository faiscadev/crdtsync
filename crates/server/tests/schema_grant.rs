//! The schema `@auth` grant tier and its composition with a deployment ACL.
//!
//! [`authorized`] is the data-plane decision: the deployment [`Authorizer`]
//! first, then — only where it abstains — the room's governing schema grants,
//! then a terminal default-deny. A plain `bool` authorizer never abstains, so it
//! is the final word; the schema tier is reached only under an authorizer that
//! can [`Decision::Abstain`] (an [`Acl`] with no matching rule).

use crdtsync_core::schema::Schema;
use crdtsync_server::acl::{authorized, Acl, ResourceMatch, Subject};
use crdtsync_server::audit::{AccessRecord, Audited};
use crdtsync_server::authz::{Action, Authorizer, Decision, Resource};
use crdtsync_server::{Identity, PermitAll};

/// A schema whose `@auth` grants read to any authenticated actor, write to the
/// `editor` role, and explicitly deny write to `viewer`. The `${actor_id}` write
/// grant (an ownership template) and the `/doc` read grant (a sub-path) exercise
/// the two deferrals: templates and path-scoping are not enforced here.
const SCHEMA: &str = r#"{ "schema": "app", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "auth": {
        "roles": ["editor", "viewer"],
        "grants": [
            { "allow": "read",  "to": "authenticated", "on": "/" },
            { "allow": "write", "to": "editor",        "on": "/" },
            { "deny":  "write", "to": "viewer",        "on": "/" },
            { "allow": "write", "to": "${actor_id}",   "on": "/" },
            { "allow": "read",  "to": "editor",        "on": "/doc" }
        ]
    } }"#;

/// A schema whose only read grant is path-scoped (`/doc`), so a room-level read
/// resolves through no grant at all.
const SUBPATH_ONLY: &str = r#"{ "schema": "app", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "auth": { "grants": [ { "allow": "read", "to": "authenticated", "on": "/doc" } ] } }"#;

fn schema(src: &str) -> Schema {
    Schema::parse(src).expect("schema parses")
}

fn with_roles(actor: &str, roles: &[&str]) -> Identity {
    Identity::with_claims(
        actor.as_bytes().to_vec(),
        roles.iter().map(|r| r.to_string()).collect(),
        Vec::new(),
    )
}

fn actor(actor: &str) -> Identity {
    with_roles(actor, &[])
}

const ROOM: Resource = Resource::Room(b"room");

/// An empty ACL abstains on every request, deferring the whole decision to the
/// schema — the composition the data plane runs.
fn abstaining() -> Acl {
    Acl::new()
}

// --- The schema tier under an abstaining deployment ------------------------

#[test]
fn an_authenticated_actor_is_granted_the_read_its_class_is_allowed() {
    let s = schema(SCHEMA);
    assert!(authorized(
        &abstaining(),
        Decision::Abstain,
        Some(&s),
        &actor("alice"),
        Action::Read,
        &ROOM
    ));
}

#[test]
fn a_role_grant_turns_on_the_claim_not_the_actor() {
    let s = schema(SCHEMA);
    // `editor` is granted write; a bare actor without the claim is not.
    assert!(authorized(
        &abstaining(),
        Decision::Abstain,
        Some(&s),
        &with_roles("bob", &["editor"]),
        Action::Write,
        &ROOM
    ));
    assert!(!authorized(
        &abstaining(),
        Decision::Abstain,
        Some(&s),
        &actor("bob"),
        Action::Write,
        &ROOM
    ));
}

#[test]
fn a_schema_deny_wins_over_a_schema_allow_for_the_same_actor() {
    let s = schema(SCHEMA);
    // Holds both claims: `editor` allows write, `viewer` denies it — deny wins.
    assert!(!authorized(
        &abstaining(),
        Decision::Abstain,
        Some(&s),
        &with_roles("carol", &["editor", "viewer"]),
        Action::Write,
        &ROOM
    ));
}

#[test]
fn an_action_no_grant_mentions_defaults_to_deny() {
    let s = schema(SCHEMA);
    // No grant allows a bare authenticated actor to write.
    assert!(!authorized(
        &abstaining(),
        Decision::Abstain,
        Some(&s),
        &actor("alice"),
        Action::Write,
        &ROOM
    ));
}

#[test]
fn an_anonymous_actor_is_not_authenticated() {
    let s = schema(SCHEMA);
    // The read grant is to `authenticated`; an `anon:`-prefixed actor is not.
    assert!(!authorized(
        &abstaining(),
        Decision::Abstain,
        Some(&s),
        &actor("anon:x"),
        Action::Read,
        &ROOM
    ));
}

#[test]
fn an_ownership_template_subject_never_matches_here() {
    let s = schema(SCHEMA);
    // `allow write to ${actor_id}` is the only write grant an unroled `alice`
    // could match; templates are unenforced, so she is still denied write.
    assert!(!authorized(
        &abstaining(),
        Decision::Abstain,
        Some(&s),
        &actor("alice"),
        Action::Write,
        &ROOM
    ));
}

#[test]
fn a_path_scoped_grant_is_not_enforced_at_the_room_level() {
    let s = schema(SUBPATH_ONLY);
    // The lone read grant is on `/doc`, not the whole document, so a room-level
    // read resolves through no grant and defaults to deny.
    assert!(!authorized(
        &abstaining(),
        Decision::Abstain,
        Some(&s),
        &actor("alice"),
        Action::Read,
        &ROOM
    ));
}

#[test]
fn register_schema_never_resolves_through_the_grant_tier() {
    let s = schema(SCHEMA);
    // A control-plane meta-auth has no schema-grantable form: the tier abstains,
    // so the composition defaults to deny.
    assert!(!authorized(
        &abstaining(),
        Decision::Abstain,
        Some(&s),
        &with_roles("bob", &["editor"]),
        Action::RegisterSchema,
        &Resource::App(b"app")
    ));
}

// --- Composition: deployment tier over the schema tier ---------------------

#[test]
fn a_deployment_allow_wins_over_a_schema_deny() {
    let s = schema(SCHEMA);
    let deploy = Acl::new().allow(Subject::Anyone, Some(Action::Write), ResourceMatch::AnyRoom);
    // The schema denies a viewer write, but the operator's explicit allow is
    // higher in the flow.
    assert!(authorized(
        &deploy,
        Decision::Abstain,
        Some(&s),
        &with_roles("carol", &["viewer"]),
        Action::Write,
        &ROOM
    ));
}

#[test]
fn a_deployment_deny_wins_over_a_schema_allow() {
    let s = schema(SCHEMA);
    let deploy = Acl::new().deny(Subject::Anyone, Some(Action::Write), ResourceMatch::AnyRoom);
    // The schema allows an editor write, but the operator's explicit deny wins.
    assert!(!authorized(
        &deploy,
        Decision::Abstain,
        Some(&s),
        &with_roles("bob", &["editor"]),
        Action::Write,
        &ROOM
    ));
}

#[test]
fn a_bool_authorizer_never_abstains_so_the_schema_is_not_consulted() {
    let s = schema(SCHEMA);
    // `PermitAll` decides Allow outright; the schema (which would deny an unroled
    // write) is never reached.
    assert!(authorized(
        &PermitAll,
        Decision::Abstain,
        Some(&s),
        &actor("alice"),
        Action::Write,
        &ROOM
    ));
    // A closure returning false is an explicit deny, its final word — the schema's
    // read allow is never reached.
    let deny_all = |_: &Identity, _: Action, _: &Resource| false;
    assert!(!authorized(
        &deny_all,
        Decision::Abstain,
        Some(&s),
        &actor("alice"),
        Action::Read,
        &ROOM
    ));
}

#[test]
fn a_relay_room_with_no_schema_is_the_deployment_decision_alone() {
    // No schema: an abstaining deployment falls straight to default-deny.
    assert!(!authorized(
        &abstaining(),
        Decision::Abstain,
        None,
        &actor("alice"),
        Action::Read,
        &ROOM
    ));
    // ...and an explicit deployment allow still grants.
    let deploy = Acl::new().allow(Subject::Anyone, Some(Action::Read), ResourceMatch::AnyRoom);
    assert!(authorized(
        &deploy,
        Decision::Abstain,
        None,
        &actor("alice"),
        Action::Read,
        &ROOM
    ));
}

#[test]
fn an_empty_acl_abstains_rather_than_denies() {
    // The three-valued verdict an empty ACL yields is Abstain — what lets the
    // schema tier speak. Standalone, `authorize` resolves that abstain to a deny.
    let acl = Acl::new();
    assert_eq!(
        acl.decide(&actor("alice"), Action::Read, &ROOM),
        Decision::Abstain
    );
    assert!(!acl.authorize(&actor("alice"), Action::Read, &ROOM));
}

#[test]
fn an_audited_wrapper_preserves_the_abstain_so_the_schema_tier_still_runs() {
    let s = schema(SCHEMA);
    // The documented composition: audit an abstaining ACL. The wrapper must
    // forward the Abstain, not flatten it to a deny — else the schema read grant
    // (to any authenticated actor) is silently suppressed for every audited
    // deployment.
    let audited = Audited::new(Box::new(Acl::new()), Box::new(|_: &AccessRecord| {}));
    assert!(authorized(
        &audited,
        Decision::Abstain,
        Some(&s),
        &actor("alice"),
        Action::Read,
        &ROOM
    ));
    // A definitive inner verdict still flows through unchanged.
    assert_eq!(
        audited.decide(&actor("alice"), Action::Read, &ROOM),
        Decision::Abstain
    );
}

#[test]
fn an_explicit_rule_makes_the_acl_decide_not_abstain() {
    let acl = Acl::new().deny(Subject::Anyone, Some(Action::Write), ResourceMatch::AnyRoom);
    assert_eq!(
        acl.decide(&actor("alice"), Action::Write, &ROOM),
        Decision::Deny
    );
    let acl = Acl::new().allow(Subject::Anyone, Some(Action::Write), ResourceMatch::AnyRoom);
    assert_eq!(
        acl.decide(&actor("alice"), Action::Write, &ROOM),
        Decision::Allow
    );
}
