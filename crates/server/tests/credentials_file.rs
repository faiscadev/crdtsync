//! Loading a static credentials table from a file — the operator-facing verifier.
//!
//! `StaticTokens::from_credentials_file` reads a credentials file and parses it
//! into a verifier. The two failure modes are distinct: the file being unreadable
//! (`Io`) and its contents being malformed (`Parse`, carrying the offending line).
//!
//! Excluded under Miri, which cannot touch the real filesystem.
#![cfg(not(miri))]

use crdtsync_server::auth::{CredentialsFileError, StaticTokens};
use crdtsync_server::{Identity, Verifier};
use std::path::PathBuf;

fn temp_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("crdtsync-creds-{}-{tag}.creds", std::process::id()))
}

struct TempCreds(PathBuf);
impl TempCreds {
    fn write(tag: &str, contents: &str) -> Self {
        let path = temp_path(tag);
        std::fs::write(&path, contents).unwrap();
        TempCreds(path)
    }
}
impl Drop for TempCreds {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn a_valid_file_loads_and_authenticates() {
    let file = TempCreds::write(
        "valid",
        "# team\n\
         secret-alice alice\n\
         secret-bob   bob\n",
    );
    let table = StaticTokens::from_credentials_file(&file.0).expect("valid file loads");
    assert_eq!(
        table.verify(b"secret-alice"),
        Some(Identity::new(b"alice".to_vec()))
    );
    assert_eq!(
        table.verify(b"secret-bob"),
        Some(Identity::new(b"bob".to_vec()))
    );
    assert_eq!(
        table.verify(b"unknown"),
        None,
        "an unknown credential is refused"
    );
}

#[test]
fn a_missing_file_is_an_io_error() {
    let missing = temp_path("does-not-exist");
    let err = StaticTokens::from_credentials_file(&missing).expect_err("a missing file fails");
    assert!(matches!(err, CredentialsFileError::Io(_)), "got {err:?}");
}

#[test]
fn a_malformed_file_is_a_parse_error_carrying_the_line() {
    let file = TempCreds::write(
        "malformed",
        "secret-alice alice\n\
         cred actor roles groups extra\n",
    );
    let err = StaticTokens::from_credentials_file(&file.0).expect_err("a malformed file fails");
    match err {
        CredentialsFileError::Parse(e) => assert_eq!(e.line, 2, "the bad entry is on line 2"),
        other => panic!("expected a parse error, got {other:?}"),
    }
}

#[test]
fn an_empty_file_refuses_everything() {
    let file = TempCreds::write("empty", "");
    let table = StaticTokens::from_credentials_file(&file.0).expect("an empty file is valid");
    assert_eq!(table.verify(b"anything"), None);
}

#[test]
fn the_error_renders_a_message_naming_the_cause() {
    let missing = temp_path("render");
    let err = StaticTokens::from_credentials_file(&missing).unwrap_err();
    assert!(
        err.to_string().contains("credentials file"),
        "message names the credentials file: {err}"
    );
}
