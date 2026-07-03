//! The declarative policy file — parsing a text policy into an [`Acl`].
//!
//! A deployment writes its authorization policy as a line-oriented text file and
//! loads it with [`Acl::from_policy`]; the resulting `Acl` behaves exactly as one
//! built with the programmatic `allow`/`deny` builders. Parsing is total — every
//! input either yields an `Acl` or a [`PolicyError`] naming the offending line,
//! never a panic.
//!
//! Format: one rule per line, `<effect> <subject> <action> <resource>`. Blank
//! lines and `#` comment lines are ignored.

use crdtsync_server::acl::{Acl, PolicyErrorKind};
use crdtsync_server::{Action, Authorizer, Resource};

fn read(a: &Acl, actor: &[u8], room: &[u8]) -> bool {
    a.authorize(actor, Action::Read, &Resource::Room(room))
}
fn write(a: &Acl, actor: &[u8], room: &[u8]) -> bool {
    a.authorize(actor, Action::Write, &Resource::Room(room))
}
fn publish(a: &Acl, actor: &[u8], room: &[u8]) -> bool {
    a.authorize(actor, Action::PublishAwareness, &Resource::Room(room))
}

const ROOM: &[u8] = b"room-a";

#[test]
fn an_empty_policy_denies_everything() {
    let acl = Acl::from_policy("").expect("empty policy parses");
    assert!(!read(&acl, b"alice", ROOM));
    assert!(!write(&acl, b"alice", ROOM));
}

#[test]
fn a_single_allow_grants_only_the_matched_tuple() {
    let acl = Acl::from_policy("allow actor:616c696365 read room:room-a").unwrap();
    // 616c696365 is the hex of "alice".
    assert!(read(&acl, b"alice", ROOM), "the granted tuple is allowed");
    assert!(!read(&acl, b"bob", ROOM), "another actor is not covered");
    assert!(
        !read(&acl, b"alice", b"room-b"),
        "another room is not covered"
    );
    assert!(
        !write(&acl, b"alice", ROOM),
        "another action is not covered"
    );
}

#[test]
fn deny_wins_over_allow_order_independent() {
    let deny_after = Acl::from_policy(
        "allow anyone * *\n\
         deny actor:6d616c read room:room-a",
    )
    .unwrap();
    let deny_before = Acl::from_policy(
        "deny actor:6d616c read room:room-a\n\
         allow anyone * *",
    )
    .unwrap();
    for acl in [&deny_after, &deny_before] {
        assert!(read(acl, b"alice", ROOM), "an unrelated actor is allowed");
        assert!(!read(acl, b"mal", ROOM), "the denied actor loses");
        assert!(write(acl, b"mal", ROOM), "the deny is scoped to read only");
    }
}

#[test]
fn default_deny_when_no_rule_matches() {
    let acl = Acl::from_policy("allow authenticated read room:other").unwrap();
    assert!(!read(&acl, b"alice", ROOM), "no rule covers this room");
    assert!(
        !write(&acl, b"alice", b"other"),
        "no rule covers this action"
    );
}

#[test]
fn comments_blank_lines_and_whitespace_are_ignored() {
    let acl = Acl::from_policy(
        "# room policy\n\
         \n\
         allow anyone read *\n\
         \n\
           # trailing note, indented\n\
         allow authenticated write *\n",
    )
    .unwrap();
    assert!(read(&acl, b"alice", ROOM));
    assert!(read(&acl, b"anon:deadbeef", ROOM));
    assert!(write(&acl, b"alice", ROOM), "authenticated may write");
    assert!(!write(&acl, b"anon:deadbeef", ROOM), "anon may not write");
}

#[test]
fn extra_whitespace_between_and_around_fields_is_tolerated() {
    let acl = Acl::from_policy("   allow    anyone   read    *   ").unwrap();
    assert!(read(&acl, b"alice", ROOM));
}

#[test]
fn subject_tokens_map_to_every_subject_variant() {
    // anyone / * are both "anyone".
    for tok in ["anyone", "*"] {
        let acl = Acl::from_policy(&format!("allow {tok} read *")).unwrap();
        assert!(read(&acl, b"alice", ROOM));
        assert!(read(&acl, b"anon:x", ROOM));
    }
    // authenticated excludes anon:.
    let authed = Acl::from_policy("allow authenticated read *").unwrap();
    assert!(read(&authed, b"alice", ROOM));
    assert!(!read(&authed, b"anon:x", ROOM));
    // anonymous is only anon:.
    let anon = Acl::from_policy("allow anonymous read *").unwrap();
    assert!(read(&anon, b"anon:x", ROOM));
    assert!(!read(&anon, b"alice", ROOM));
}

#[test]
fn action_tokens_map_to_every_action_and_the_wildcard() {
    let r = Acl::from_policy("allow anyone read *").unwrap();
    assert!(read(&r, b"a", ROOM) && !write(&r, b"a", ROOM) && !publish(&r, b"a", ROOM));
    let w = Acl::from_policy("allow anyone write *").unwrap();
    assert!(!read(&w, b"a", ROOM) && write(&w, b"a", ROOM) && !publish(&w, b"a", ROOM));
    let p = Acl::from_policy("allow anyone publish_awareness *").unwrap();
    assert!(!read(&p, b"a", ROOM) && !write(&p, b"a", ROOM) && publish(&p, b"a", ROOM));
    let any = Acl::from_policy("allow anyone * *").unwrap();
    assert!(read(&any, b"a", ROOM) && write(&any, b"a", ROOM) && publish(&any, b"a", ROOM));
}

#[test]
fn resource_tokens_map_to_a_named_room_and_the_wildcard() {
    let named = Acl::from_policy("allow anyone read room:room-a").unwrap();
    assert!(read(&named, b"a", b"room-a"));
    assert!(!read(&named, b"a", b"room-b"));
    let any = Acl::from_policy("allow anyone read *").unwrap();
    assert!(read(&any, b"a", b"room-a"));
    assert!(read(&any, b"a", b"room-z"));
}

#[test]
fn actor_hex_decodes_to_the_matching_actor_bytes() {
    // A room name may itself contain a colon; only the first `room:` prefix is
    // stripped, the remainder is the raw name.
    let acl = Acl::from_policy("allow anyone read room:ns:room-a").unwrap();
    assert!(read(&acl, b"a", b"ns:room-a"));
    assert!(!read(&acl, b"a", b"room-a"));
}

#[test]
fn a_parsed_policy_authorizes_identically_to_the_programmatic_builder() {
    use crdtsync_server::acl::{ResourceMatch, Subject};
    let parsed = Acl::from_policy(
        "allow authenticated * *\n\
         deny anonymous write room:locked\n\
         allow actor:616c696365 read room:locked",
    )
    .unwrap();
    let built = Acl::new()
        .allow(Subject::Authenticated, None, ResourceMatch::AnyRoom)
        .deny(
            Subject::Anonymous,
            Some(Action::Write),
            ResourceMatch::Room(b"locked".to_vec()),
        )
        .allow(
            Subject::Actor(b"alice".to_vec()),
            Some(Action::Read),
            ResourceMatch::Room(b"locked".to_vec()),
        );

    let actors: [&[u8]; 3] = [b"alice", b"bob", b"anon:x"];
    let rooms: [&[u8]; 2] = [b"locked", b"open"];
    let actions = [Action::Read, Action::Write, Action::PublishAwareness];
    for actor in actors {
        for room in rooms {
            for action in actions {
                let res = Resource::Room(room);
                assert_eq!(
                    parsed.authorize(actor, action, &res),
                    built.authorize(actor, action, &res),
                    "parsed and built disagree for {actor:?} {action:?} {room:?}",
                );
            }
        }
    }
}

// --- malformed input: every class is a typed error on the offending line, never a panic ---

fn err_kind(policy: &str) -> (usize, PolicyErrorKind) {
    let e = Acl::from_policy(policy).expect_err("policy must be rejected");
    (e.line, e.kind)
}

#[test]
fn too_few_fields_is_an_arity_error() {
    let (line, kind) = err_kind("allow anyone read");
    assert_eq!(line, 1);
    assert!(matches!(kind, PolicyErrorKind::Arity(3)), "got {kind:?}");
}

#[test]
fn too_many_fields_is_an_arity_error() {
    let (line, kind) = err_kind("allow anyone read * extra");
    assert_eq!(line, 1);
    assert!(matches!(kind, PolicyErrorKind::Arity(5)), "got {kind:?}");
}

#[test]
fn an_unknown_effect_is_an_effect_error() {
    let (line, kind) = err_kind("permit anyone read *");
    assert_eq!(line, 1);
    assert!(matches!(kind, PolicyErrorKind::Effect(_)), "got {kind:?}");
}

#[test]
fn an_unknown_subject_is_a_subject_error() {
    let (line, kind) = err_kind("allow nobody read *");
    assert_eq!(line, 1);
    assert!(matches!(kind, PolicyErrorKind::Subject(_)), "got {kind:?}");
}

#[test]
fn a_non_hex_actor_is_an_actor_hex_error() {
    let (_, kind) = err_kind("allow actor:zz read *");
    assert!(matches!(kind, PolicyErrorKind::ActorHex(_)), "got {kind:?}");
}

#[test]
fn an_odd_length_actor_hex_is_an_actor_hex_error() {
    let (_, kind) = err_kind("allow actor:abc read *");
    assert!(matches!(kind, PolicyErrorKind::ActorHex(_)), "got {kind:?}");
}

#[test]
fn an_unknown_action_is_an_action_error() {
    let (line, kind) = err_kind("allow anyone fly *");
    assert_eq!(line, 1);
    assert!(matches!(kind, PolicyErrorKind::Action(_)), "got {kind:?}");
}

#[test]
fn an_unknown_resource_is_a_resource_error() {
    let (_, bare) = err_kind("allow anyone read planet");
    assert!(matches!(bare, PolicyErrorKind::Resource(_)), "got {bare:?}");
    let (_, prefixed) = err_kind("allow anyone read planet:mars");
    assert!(
        matches!(prefixed, PolicyErrorKind::Resource(_)),
        "got {prefixed:?}"
    );
}

#[test]
fn the_error_line_counts_physical_lines_including_comments_and_blanks() {
    // The bad rule is on physical line 4 (comment, blank, good rule, bad rule).
    let (line, kind) = err_kind(
        "# header\n\
         \n\
         allow anyone read *\n\
         allow anyone fly *",
    );
    assert_eq!(line, 4, "line number points at the offending physical line");
    assert!(matches!(kind, PolicyErrorKind::Action(_)));
}

#[test]
fn a_policy_error_renders_a_message_naming_the_line() {
    let e = Acl::from_policy("allow anyone fly *").unwrap_err();
    let shown = e.to_string();
    assert!(shown.contains("line 1"), "message names the line: {shown}");
    assert!(
        shown.contains("fly"),
        "message names the bad token: {shown}"
    );
}
