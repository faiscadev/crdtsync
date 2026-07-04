//! The schema registry — the control-plane store an app's schema + migration
//! chain is registered into, hash-locked, and the handshake resolves a client's
//! `{app_id, version}` against.
//!
//! A chain is contiguous from version 1 and every link is immutable once
//! registered: the registry appends the next version, safely no-ops an identical
//! retry, and refuses a gap, a backward version, or a content change under an
//! already-registered version. Resolution returns the exact schema bytes a
//! registered version holds, and nothing for an app or version it never saw.

use crdtsync_server::schema_registry::{RegisterError, Registered, SchemaRegistry};

const APP: &[u8] = b"app-x";
const S1: &[u8] = br#"{"schema":1}"#;
const S2: &[u8] = br#"{"schema":2}"#;
const S3: &[u8] = br#"{"schema":3}"#;
const M2: &[u8] = br#"[{"rename":"a->b"}]"#;

fn reg(
    r: &mut SchemaRegistry,
    app: &[u8],
    v: u32,
    schema: &[u8],
) -> Result<Registered, RegisterError> {
    r.register(app, v, schema, b"")
}

#[test]
fn a_chain_appends_contiguously_from_version_one() {
    let mut r = SchemaRegistry::default();
    assert_eq!(reg(&mut r, APP, 1, S1), Ok(Registered::Appended));
    assert_eq!(reg(&mut r, APP, 2, S2), Ok(Registered::Appended));
    assert_eq!(reg(&mut r, APP, 3, S3), Ok(Registered::Appended));
    assert_eq!(r.head_version(APP), Some(3));
}

#[test]
fn resolve_returns_each_registered_version_verbatim() {
    let mut r = SchemaRegistry::default();
    reg(&mut r, APP, 1, S1).unwrap();
    reg(&mut r, APP, 2, S2).unwrap();
    assert_eq!(r.resolve(APP, 1), Some(S1));
    assert_eq!(r.resolve(APP, 2), Some(S2));
}

#[test]
fn resolve_is_none_for_an_unknown_app_or_version() {
    let mut r = SchemaRegistry::default();
    reg(&mut r, APP, 1, S1).unwrap();
    assert_eq!(r.resolve(b"nope", 1), None, "unknown app");
    assert_eq!(r.resolve(APP, 0), None, "version 0 is not a valid position");
    assert_eq!(r.resolve(APP, 2), None, "past the head");
    assert_eq!(r.head_version(b"nope"), None);
}

#[test]
fn skipping_a_version_is_a_gap() {
    let mut r = SchemaRegistry::default();
    // A fresh app must start at 1.
    assert_eq!(
        reg(&mut r, APP, 2, S2),
        Err(RegisterError::Gap {
            expected: 1,
            got: 2
        })
    );
    // A rejected first registration leaves the app unregistered.
    assert_eq!(r.head_version(APP), None);

    reg(&mut r, APP, 1, S1).unwrap();
    assert_eq!(
        reg(&mut r, APP, 3, S3),
        Err(RegisterError::Gap {
            expected: 2,
            got: 3
        })
    );
    assert_eq!(
        r.head_version(APP),
        Some(1),
        "a gap does not advance the chain"
    );
}

#[test]
fn a_backward_or_zero_version_is_out_of_sequence() {
    let mut r = SchemaRegistry::default();
    assert_eq!(
        reg(&mut r, APP, 0, S1),
        Err(RegisterError::OutOfSequence {
            expected: 1,
            got: 0
        })
    );
    reg(&mut r, APP, 1, S1).unwrap();
    reg(&mut r, APP, 2, S2).unwrap();
    reg(&mut r, APP, 3, S3).unwrap();
    // Version 1 is superseded; the chain only moves forward.
    assert_eq!(
        reg(&mut r, APP, 1, S1),
        Err(RegisterError::OutOfSequence {
            expected: 4,
            got: 1
        })
    );
    assert_eq!(r.head_version(APP), Some(3));
}

#[test]
fn re_registering_a_version_with_new_content_is_a_hash_mismatch() {
    let mut r = SchemaRegistry::default();
    reg(&mut r, APP, 1, S1).unwrap();
    // Same version, different schema body — the link is immutable.
    assert_eq!(
        reg(&mut r, APP, 1, S2),
        Err(RegisterError::HashMismatch { version: 1 })
    );
    // The rejected write did not mutate the locked content.
    assert_eq!(r.resolve(APP, 1), Some(S1));
    assert_eq!(r.head_version(APP), Some(1));
}

#[test]
fn an_identical_retry_is_an_idempotent_no_op() {
    let mut r = SchemaRegistry::default();
    assert_eq!(reg(&mut r, APP, 1, S1), Ok(Registered::Appended));
    // The CI re-pushes the same head after an unacked response.
    assert_eq!(reg(&mut r, APP, 1, S1), Ok(Registered::Unchanged));
    assert_eq!(
        r.head_version(APP),
        Some(1),
        "a retry does not grow the chain"
    );

    reg(&mut r, APP, 2, S2).unwrap();
    assert_eq!(reg(&mut r, APP, 2, S2), Ok(Registered::Unchanged));
    assert_eq!(r.head_version(APP), Some(2));
}

#[test]
fn the_lock_covers_the_migration_edge_too() {
    let mut r = SchemaRegistry::default();
    reg(&mut r, APP, 1, S1).unwrap();
    assert_eq!(r.register(APP, 2, S2, M2), Ok(Registered::Appended));
    // Same schema body, different migration bytes — still a mismatch.
    assert_eq!(
        r.register(APP, 2, S2, br#"[{"rename":"a->c"}]"#),
        Err(RegisterError::HashMismatch { version: 2 })
    );
    // The identical retry (schema and migration) is a no-op.
    assert_eq!(r.register(APP, 2, S2, M2), Ok(Registered::Unchanged));
    assert_eq!(r.migration(APP, 2), Some(M2));
    assert_eq!(
        r.migration(APP, 1),
        Some(&b""[..]),
        "version 1 has no migration edge"
    );
}

#[test]
fn apps_hold_independent_chains() {
    let mut r = SchemaRegistry::default();
    reg(&mut r, b"app-a", 1, S1).unwrap();
    reg(&mut r, b"app-a", 2, S2).unwrap();
    // A different app starts its own chain at 1 with the same version numbers.
    assert_eq!(reg(&mut r, b"app-b", 1, S3), Ok(Registered::Appended));
    assert_eq!(r.head_version(b"app-a"), Some(2));
    assert_eq!(r.head_version(b"app-b"), Some(1));
    assert_eq!(r.resolve(b"app-a", 1), Some(S1));
    assert_eq!(r.resolve(b"app-b", 1), Some(S3));
}

#[test]
fn the_content_hash_is_stable_and_content_addressed() {
    let mut r = SchemaRegistry::default();
    reg(&mut r, APP, 1, S1).unwrap();
    let h1 = r.hash(APP, 1).expect("registered");
    assert_eq!(h1.len(), 32);
    // The same content in a different app hashes identically — the lock is over
    // content, not the app it was registered under.
    reg(&mut r, b"app-y", 1, S1).unwrap();
    assert_eq!(r.hash(b"app-y", 1), Some(h1));
    // Different content hashes differently.
    reg(&mut r, APP, 2, S2).unwrap();
    assert_ne!(r.hash(APP, 2), Some(h1));
    assert_eq!(r.hash(APP, 3), None, "no such version");
}
