//! Loading a declarative policy from a file — the operator-facing entry point.
//!
//! `Acl::from_policy_file` reads a policy file and parses it into an `Acl`. The
//! two failure modes are distinct: the file being unreadable (`Io`) and its
//! contents being malformed (`Parse`, carrying the offending line).
//!
//! Excluded under Miri, which cannot touch the real filesystem.
#![cfg(not(miri))]

use crdtsync_server::acl::{Acl, PolicyFileError};
use crdtsync_server::{Action, Authorizer, Identity, Resource};
use std::path::PathBuf;

fn read(a: &Acl, actor: &[u8], room: &[u8]) -> bool {
    a.authorize(
        &Identity::new(actor.to_vec()),
        Action::Read,
        &Resource::Room(room),
    )
}

/// A unique temp path for this test, so parallel tests don't collide.
fn temp_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "crdtsync-policy-{}-{tag}.policy",
        std::process::id()
    ))
}

struct TempPolicy(PathBuf);
impl TempPolicy {
    fn write(tag: &str, contents: &str) -> Self {
        let path = temp_path(tag);
        std::fs::write(&path, contents).unwrap();
        TempPolicy(path)
    }
}
impl Drop for TempPolicy {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn a_valid_policy_file_loads_and_enforces() {
    let file = TempPolicy::write(
        "valid",
        "# room policy\n\
         allow actor:616c696365 read room:room-a\n\
         allow authenticated read room:open\n",
    );
    let acl = Acl::from_policy_file(&file.0).expect("valid policy loads");
    assert!(read(&acl, b"alice", b"room-a"), "the granted actor reads");
    assert!(!read(&acl, b"bob", b"room-a"), "another actor is denied");
    assert!(read(&acl, b"alice", b"open"), "authenticated reads open");
    assert!(!read(&acl, b"anon:x", b"open"), "anon is not authenticated");
}

#[test]
fn a_missing_file_is_an_io_error() {
    let missing = temp_path("does-not-exist");
    let err = Acl::from_policy_file(&missing).expect_err("a missing file fails");
    assert!(matches!(err, PolicyFileError::Io(_)), "got {err:?}");
}

#[test]
fn a_malformed_policy_file_is_a_parse_error_carrying_the_line() {
    let file = TempPolicy::write(
        "malformed",
        "allow anyone read *\n\
         allow anyone fly *\n",
    );
    let err = Acl::from_policy_file(&file.0).expect_err("a malformed file fails");
    match err {
        PolicyFileError::Parse(e) => assert_eq!(e.line, 2, "the bad rule is on line 2"),
        other => panic!("expected a parse error, got {other:?}"),
    }
}

#[test]
fn an_empty_file_loads_a_deny_all_policy() {
    let file = TempPolicy::write("empty", "");
    let acl = Acl::from_policy_file(&file.0).expect("an empty file is a valid empty policy");
    assert!(!read(&acl, b"alice", b"room-a"), "an empty policy denies");
}

#[test]
fn the_error_renders_a_message_naming_the_cause() {
    let missing = temp_path("render");
    let err = Acl::from_policy_file(&missing).unwrap_err();
    assert!(
        err.to_string().contains("policy file"),
        "message names the policy file: {err}"
    );
}
