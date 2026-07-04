//! The handshake gate — the server resolves a client's `{app_id, schema_version}`
//! at Hello and refuses a registered app's unknown version.
//!
//! A relay connection (no app, or an app that never registered) always proceeds
//! and records no enforced version. A registered app pins the session to a
//! version — the declared one, or the head a version-0 dynamic client adopts —
//! and a declared version the registry does not hold closes the connection with
//! an `UnsupportedVersion` error, before the client is ever a subscriber.

use std::sync::{Arc, Mutex};

use crdtsync_core::{ClientId, ErrorCode, Message};
use crdtsync_server::{step, AllowAll, Hub, PermitAll, Registry, SchemaRegistry, Session};

const APP: &[u8] = b"app-x";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn registered() -> SchemaRegistry {
    let mut r = SchemaRegistry::new();
    r.register(APP, 1, br#"{"v":1}"#, b"").unwrap();
    r.register(APP, 2, br#"{"v":2}"#, b"").unwrap();
    r
}

fn hello(app_id: &[u8], schema_version: u32) -> Message {
    Message::Hello {
        client: cid(1),
        app_id: app_id.to_vec(),
        schema_version,
    }
}

// --- step level: the session records the resolved binding ---

fn resolve(reg: &Mutex<SchemaRegistry>, msg: Message) -> (Session, crdtsync_server::Response) {
    let mut hub = Hub::new(cid(0xFF));
    let mut session = Session::new();
    let resp = step(&mut hub, &mut session, &AllowAll, &PermitAll, reg, msg);
    (session, resp)
}

#[test]
fn a_relay_connection_records_no_enforced_version() {
    let reg = Mutex::new(registered());
    // No app named at all.
    let (session, resp) = resolve(&reg, hello(b"", 0));
    assert!(!resp.close);
    assert_eq!(session.client(), Some(cid(1)));
    assert_eq!(session.app_id(), b"");
    assert_eq!(session.schema_version(), None);

    // A named app that never registered is still a relay.
    let (session, resp) = resolve(&reg, hello(b"other-app", 4));
    assert!(!resp.close);
    assert_eq!(session.app_id(), b"other-app");
    assert_eq!(
        session.schema_version(),
        None,
        "unregistered app is a relay"
    );
}

#[test]
fn a_registered_app_pins_the_declared_version() {
    let reg = Mutex::new(registered());
    let (session, resp) = resolve(&reg, hello(APP, 1));
    assert!(!resp.close);
    assert_eq!(session.app_id(), APP);
    assert_eq!(session.schema_version(), Some(1));
}

#[test]
fn a_dynamic_client_adopts_the_head_version() {
    let reg = Mutex::new(registered());
    let (session, resp) = resolve(&reg, hello(APP, 0));
    assert!(!resp.close);
    assert_eq!(
        session.schema_version(),
        Some(2),
        "version 0 adopts the head"
    );
}

#[test]
fn an_unknown_version_closes_with_unsupported_version() {
    let reg = Mutex::new(registered());
    let (session, resp) = resolve(&reg, hello(APP, 3));
    assert!(resp.close, "an unknown version closes the connection");
    assert!(matches!(
        resp.replies.as_slice(),
        [Message::Error {
            code: ErrorCode::UnsupportedVersion,
            ..
        }]
    ));
    // The handshake did not complete: no client, no enforced version.
    assert_eq!(session.client(), None);
    assert_eq!(session.schema_version(), None);
}

// --- registry level: the gate holds end to end through deliver ---

#[test]
fn the_registry_refuses_an_unknown_version_and_admits_a_known_one() {
    let schema = Arc::new(Mutex::new(registered()));
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(schema);

    // An unknown version: deliver reports close, the outbox carries the error.
    let bad = r.connect();
    assert!(!r.deliver(bad, hello(APP, 3)));
    assert!(matches!(
        r.take_outbox(bad).as_slice(),
        [Message::Error {
            code: ErrorCode::UnsupportedVersion,
            ..
        }]
    ));

    // A known version proceeds — the handshake is quiet and the connection stays.
    let good = r.connect();
    assert!(r.deliver(good, hello(APP, 2)));
    assert!(r.take_outbox(good).is_empty());
}

#[test]
fn a_relay_connection_is_admitted_without_a_registry() {
    // The default (no shared schema registry) resolves every app to a relay.
    let mut r = Registry::new(cid(0xFF));
    let id = r.connect();
    assert!(r.deliver(id, hello(APP, 7)));
    assert!(
        r.take_outbox(id).is_empty(),
        "no registry → relay, admitted"
    );
}
