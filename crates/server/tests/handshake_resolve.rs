//! Handshake resolution — the tier decision the server makes when a client names
//! its `{app_id, schema_version}` at Hello.
//!
//! The registry answers whether a connection is a relay (no app, or an app that
//! never registered a schema — served with zero enforcement), an enforcing
//! connection pinned to a registered version (a declared known version, or the
//! head adopted by a version-0 dynamic client), or a rejection (a registered app
//! for which the client declared a version the registry does not hold).

use crdtsync_server::schema_registry::{Resolution, SchemaRegistry};

const APP: &[u8] = b"app-x";
const S1: &[u8] = br#"{"v":1}"#;
const S2: &[u8] = br#"{"v":2}"#;

fn registered() -> SchemaRegistry {
    let mut r = SchemaRegistry::new();
    r.register(APP, 1, S1, b"").unwrap();
    r.register(APP, 2, S2, b"").unwrap();
    r
}

#[test]
fn no_app_id_is_a_relay() {
    let r = registered();
    // An empty app id means no app — a relay connection, whatever the version.
    assert_eq!(r.resolve_handshake(b"", 0), Resolution::Relay);
    assert_eq!(r.resolve_handshake(b"", 5), Resolution::Relay);
}

#[test]
fn an_unregistered_app_is_a_relay() {
    let r = registered();
    // A named app that never registered a schema is still served, as a relay.
    assert_eq!(r.resolve_handshake(b"unknown-app", 0), Resolution::Relay);
    assert_eq!(r.resolve_handshake(b"unknown-app", 1), Resolution::Relay);
}

#[test]
fn a_declared_known_version_enforces_that_version() {
    let r = registered();
    assert_eq!(
        r.resolve_handshake(APP, 1),
        Resolution::Enforcing { version: 1 }
    );
    assert_eq!(
        r.resolve_handshake(APP, 2),
        Resolution::Enforcing { version: 2 }
    );
}

#[test]
fn a_dynamic_client_adopts_the_head_version() {
    let r = registered();
    // Version 0 declares none — a dynamic client adopts whatever the server
    // serves, i.e. the chain head.
    assert_eq!(
        r.resolve_handshake(APP, 0),
        Resolution::Enforcing { version: 2 }
    );
}

#[test]
fn a_declared_unknown_version_is_rejected() {
    let r = registered();
    // The app is registered but the client asked for a version the registry does
    // not hold — rejected, never fabricated.
    assert_eq!(r.resolve_handshake(APP, 3), Resolution::Reject);
    assert_eq!(r.resolve_handshake(APP, 99), Resolution::Reject);
}
