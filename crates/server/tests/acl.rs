//! The ACL decision flow — a concrete [`Authorizer`] that walks a set of ACL
//! tuples with standard IAM semantics: explicit deny always wins, an explicit
//! allow grants, and the absence of any matching allow denies (default-deny).
//!
//! Subjects are matched against the acting identity: an exact actor, anyone
//! (`*`), any authenticated or anonymous (`anon:`) actor, or a role or group the
//! credential asserts. Schema `@auth` defaults, which feed into this same
//! evaluation, await the schema tier and are not exercised here.

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, ErrorCode, Message};
use crdtsync_server::acl::{Acl, Effect, ResourceMatch, Subject};
use crdtsync_server::{Action, Authorizer, Identity, Registry, Resource};

const ROOM: &[u8] = b"room-a";

fn id(actor: &[u8]) -> Identity {
    Identity::new(actor.to_vec())
}

fn read_room(a: &dyn Authorizer, actor: &[u8], room: &[u8]) -> bool {
    a.authorize(&id(actor), Action::Read, &Resource::Room(room))
}

#[test]
fn an_empty_acl_denies_everything() {
    let acl = Acl::new();
    assert!(!read_room(&acl, b"alice", ROOM));
    assert!(!acl.authorize(&id(b"alice"), Action::Write, &Resource::Room(ROOM)));
}

#[test]
fn an_explicit_allow_grants_only_the_matched_tuple() {
    let acl = Acl::new().allow(
        Subject::Actor(b"alice".to_vec()),
        Some(Action::Read),
        ResourceMatch::Room(ROOM.to_vec()),
    );
    assert!(
        read_room(&acl, b"alice", ROOM),
        "the granted tuple is allowed"
    );
    assert!(
        !read_room(&acl, b"bob", ROOM),
        "another actor is not covered"
    );
    assert!(
        !read_room(&acl, b"alice", b"room-b"),
        "another room is not covered"
    );
    assert!(
        !acl.authorize(&id(b"alice"), Action::Write, &Resource::Room(ROOM)),
        "another action is not covered"
    );
}

#[test]
fn an_explicit_deny_always_wins_over_an_allow() {
    // Deny added after the allow.
    let deny_after = Acl::new()
        .allow(Subject::Anyone, None, ResourceMatch::AnyRoom)
        .deny(
            Subject::Actor(b"mallory".to_vec()),
            None,
            ResourceMatch::Room(ROOM.to_vec()),
        );
    // Deny added before the allow — order must not matter.
    let deny_before = Acl::new()
        .deny(
            Subject::Actor(b"mallory".to_vec()),
            None,
            ResourceMatch::Room(ROOM.to_vec()),
        )
        .allow(Subject::Anyone, None, ResourceMatch::AnyRoom);

    for acl in [&deny_after, &deny_before] {
        assert!(
            read_room(acl, b"alice", ROOM),
            "an unrelated actor is allowed"
        );
        assert!(!read_room(acl, b"mallory", ROOM), "the denied actor loses");
    }
}

#[test]
fn the_anyone_subject_matches_every_actor() {
    let acl = Acl::new().allow(Subject::Anyone, Some(Action::Read), ResourceMatch::AnyRoom);
    assert!(read_room(&acl, b"alice", ROOM));
    assert!(read_room(&acl, b"anon:deadbeef", ROOM));
}

#[test]
fn authenticated_and_anonymous_subjects_are_disjoint() {
    let for_authed = Acl::new().allow(
        Subject::Authenticated,
        Some(Action::Read),
        ResourceMatch::AnyRoom,
    );
    assert!(
        read_room(&for_authed, b"alice", ROOM),
        "a real actor is authenticated"
    );
    assert!(
        !read_room(&for_authed, b"anon:deadbeef", ROOM),
        "an anon actor is not authenticated:*"
    );

    let for_anon = Acl::new().allow(
        Subject::Anonymous,
        Some(Action::Read),
        ResourceMatch::AnyRoom,
    );
    assert!(
        read_room(&for_anon, b"anon:deadbeef", ROOM),
        "an anon actor matches anonymous:*"
    );
    assert!(
        !read_room(&for_anon, b"alice", ROOM),
        "a real actor is not anonymous:*"
    );
}

#[test]
fn a_none_action_matches_every_action() {
    let acl = Acl::new().allow(
        Subject::Actor(b"alice".to_vec()),
        None,
        ResourceMatch::Room(ROOM.to_vec()),
    );
    assert!(acl.authorize(&id(b"alice"), Action::Read, &Resource::Room(ROOM)));
    assert!(acl.authorize(&id(b"alice"), Action::Write, &Resource::Room(ROOM)));
    assert!(acl.authorize(
        &id(b"alice"),
        Action::PublishAwareness,
        &Resource::Room(ROOM)
    ));
}

#[test]
fn an_any_room_resource_matches_every_room() {
    let acl = Acl::new().allow(
        Subject::Actor(b"alice".to_vec()),
        Some(Action::Read),
        ResourceMatch::AnyRoom,
    );
    assert!(read_room(&acl, b"alice", b"room-a"));
    assert!(read_room(&acl, b"alice", b"room-z"));
}

#[test]
fn a_deny_scoped_to_one_action_leaves_others_allowed() {
    // Alice may do anything in the room, except write.
    let acl = Acl::new()
        .allow(
            Subject::Actor(b"alice".to_vec()),
            None,
            ResourceMatch::Room(ROOM.to_vec()),
        )
        .deny(
            Subject::Actor(b"alice".to_vec()),
            Some(Action::Write),
            ResourceMatch::Room(ROOM.to_vec()),
        );
    assert!(acl.authorize(&id(b"alice"), Action::Read, &Resource::Room(ROOM)));
    assert!(!acl.authorize(&id(b"alice"), Action::Write, &Resource::Room(ROOM)));
    assert!(acl.authorize(
        &id(b"alice"),
        Action::PublishAwareness,
        &Resource::Room(ROOM)
    ));
}

#[test]
fn effect_can_be_pushed_directly() {
    // The builder helpers are sugar over pushing a rule with an explicit effect.
    let mut acl = Acl::new();
    acl.push(
        Subject::Anyone,
        Some(Action::Read),
        ResourceMatch::AnyRoom,
        Effect::Allow,
    );
    assert!(read_room(&acl, b"alice", ROOM));
}

// --- role and group subjects match the credential's claims ---

#[test]
fn a_role_subject_matches_an_identity_that_holds_the_role() {
    let acl = Acl::new().allow(
        Subject::Role("editor".to_string()),
        Some(Action::Write),
        ResourceMatch::AnyRoom,
    );
    let editor = Identity::with_claims(b"alice".to_vec(), vec!["editor".to_string()], vec![]);
    let viewer = Identity::with_claims(b"bob".to_vec(), vec!["viewer".to_string()], vec![]);
    assert!(
        acl.authorize(&editor, Action::Write, &Resource::Room(ROOM)),
        "the role holder is allowed"
    );
    assert!(
        !acl.authorize(&viewer, Action::Write, &Resource::Room(ROOM)),
        "a different role is not covered"
    );
    assert!(
        !acl.authorize(&id(b"carol"), Action::Write, &Resource::Room(ROOM)),
        "no roles matches no role subject"
    );
}

#[test]
fn a_group_subject_matches_an_identity_in_the_group() {
    let acl = Acl::new().allow(
        Subject::Group("staff".to_string()),
        Some(Action::Read),
        ResourceMatch::AnyRoom,
    );
    let staff = Identity::with_claims(b"alice".to_vec(), vec![], vec!["staff".to_string()]);
    assert!(acl.authorize(&staff, Action::Read, &Resource::Room(ROOM)));
    assert!(
        !read_room(&acl, b"bob", ROOM),
        "an actor without the group is not covered"
    );
}

#[test]
fn a_role_grant_turns_on_the_claim_not_the_actor_id() {
    // Same actor bytes; the decision turns only on the asserted role.
    let acl = Acl::new().allow(
        Subject::Role("admin".to_string()),
        None,
        ResourceMatch::AnyRoom,
    );
    let as_admin = Identity::with_claims(b"same".to_vec(), vec!["admin".to_string()], vec![]);
    let no_claims = Identity::new(b"same".to_vec());
    assert!(acl.authorize(&as_admin, Action::Write, &Resource::Room(ROOM)));
    assert!(!acl.authorize(&no_claims, Action::Write, &Resource::Room(ROOM)));
}

#[test]
fn a_role_deny_wins_over_a_broader_allow() {
    // Anyone may read, but the "banned" role is denied — deny wins across subject
    // kinds, whatever the actor id.
    let acl = Acl::new()
        .allow(Subject::Anyone, Some(Action::Read), ResourceMatch::AnyRoom)
        .deny(
            Subject::Role("banned".to_string()),
            Some(Action::Read),
            ResourceMatch::AnyRoom,
        );
    let banned = Identity::with_claims(b"alice".to_vec(), vec!["banned".to_string()], vec![]);
    assert!(read_room(&acl, b"bob", ROOM), "an unbanned actor reads");
    assert!(
        !acl.authorize(&banned, Action::Read, &Resource::Room(ROOM)),
        "the banned role loses"
    );
}

// --- plugged into the server as the live authorizer ---

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// An `Acl` set as the registry's authorizer gates subscribe at the real
/// enforcement point: a room outside the policy is refused, one inside is served.
#[test]
fn an_acl_gates_subscribe_when_set_on_the_registry() {
    let mut r = Registry::new(cid(0xFF));
    // Only actor-1 may read "open"; everything else is default-denied.
    r.set_authorizer(Box::new(Acl::new().allow(
        Subject::Actor(b"actor-1".to_vec()),
        Some(Action::Read),
        ResourceMatch::Room(b"open".to_vec()),
    )));

    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"actor-1".to_vec(),
        }
    ));
    r.take_outbox(id);

    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: b"open".to_vec(),
            last_seen_seq: 0,
        }
    ));
    assert!(
        matches!(r.take_outbox(id).as_slice(), [Message::Ops { .. }]),
        "the permitted room subscribes"
    );

    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(1),
            room: b"secret".to_vec(),
            last_seen_seq: 0,
        }
    ));
    assert!(
        matches!(
            r.take_outbox(id).as_slice(),
            [Message::Error {
                code: ErrorCode::Forbidden,
                ..
            }]
        ),
        "a room outside the policy is forbidden"
    );
}
