//! Handshake auth — the Auth phase between Hello and Subscribe.
//!
//! After Hello, a client presents an opaque credential; the session hands it to
//! the deployment's [`Verifier`], and on success adopts the server-derived actor
//! and replies AuthOk. A rejected credential is an AuthFailed error that closes
//! the connection. Nothing past Hello/Auth is allowed until an actor is
//! established: Subscribe, Ops, or Unsubscribe before auth is a violation. The
//! server never trusts a client-asserted actor.

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, ErrorCode, Message};
use crdtsync_server::auth::{AllowAll, Identity, Verifier};
use crdtsync_server::{step, Hub, PermitAll, Response, SchemaRegistry, Session};
use std::sync::Mutex;

/// Drive a message through `step` under the dev-mode permit-all authorizer; these
/// tests exercise the auth phase, not authorization.
fn drive(hub: &mut Hub, s: &mut Session, v: &dyn Verifier, msg: Message) -> Response {
    step(
        hub,
        s,
        v,
        &PermitAll,
        None,
        &Mutex::new(SchemaRegistry::new()),
        None,
        None,
        0,
        None,
        msg,
    )
}

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn hub() -> Hub {
    Hub::new(cid(0xFF))
}

/// A verifier that accepts exactly one credential, mapping it to a fixed actor.
fn only_good() -> impl Verifier {
    |cred: &[u8]| (cred == b"good").then(|| Identity::new(b"alice".to_vec()))
}

fn hello(hub: &mut Hub, s: &mut Session, v: &dyn Verifier, client: u8) {
    let r = drive(
        hub,
        s,
        v,
        Message::Hello {
            client: cid(client),
            app_id: Vec::new(),
            schema_version: 0,
        },
    );
    assert!(
        r.replies.is_empty() && !r.close,
        "hello establishes quietly"
    );
}

fn is_violation(m: &Message) -> bool {
    matches!(
        m,
        Message::Error {
            code: ErrorCode::ProtocolViolation,
            ..
        }
    )
}

fn is_auth_failed(m: &Message) -> bool {
    matches!(
        m,
        Message::Error {
            code: ErrorCode::AuthFailed,
            ..
        }
    )
}

// --- the happy path ---

#[test]
fn a_verified_credential_establishes_the_actor_and_replies_authok() {
    let v = only_good();
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, &v, 1);

    let r = drive(
        &mut h,
        &mut s,
        &v,
        Message::Auth {
            credential: b"good".to_vec(),
        },
    );
    assert_eq!(
        r.replies,
        vec![Message::AuthOk {
            actor: b"alice".to_vec()
        }]
    );
    assert!(!r.close);
    assert_eq!(s.actor(), Some(&b"alice"[..]));
}

#[test]
fn after_auth_subscribe_is_allowed() {
    let v = only_good();
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, &v, 1);
    drive(
        &mut h,
        &mut s,
        &v,
        Message::Auth {
            credential: b"good".to_vec(),
        },
    );
    let r = drive(
        &mut h,
        &mut s,
        &v,
        Message::Subscribe {
            channel: Channel(0),
            room: b"room-1".to_vec(),
            last_seen_seq: 0,
            branch: Vec::new(),
        },
    );
    assert_eq!(
        r.replies,
        vec![Message::Ops {
            channel: Channel(0),
            ops: Vec::new(),
        }]
    );
    assert!(!r.close);
}

// --- rejection ---

#[test]
fn a_rejected_credential_is_auth_failed_and_closes() {
    let v = only_good();
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, &v, 1);
    let r = drive(
        &mut h,
        &mut s,
        &v,
        Message::Auth {
            credential: b"bad".to_vec(),
        },
    );
    assert!(r.close);
    assert!(is_auth_failed(&r.replies[0]));
    assert_eq!(s.actor(), None);
}

// --- phase ordering ---

#[test]
fn auth_before_hello_is_a_violation() {
    let v = only_good();
    let mut h = hub();
    let mut s = Session::new();
    let r = drive(
        &mut h,
        &mut s,
        &v,
        Message::Auth {
            credential: b"good".to_vec(),
        },
    );
    assert!(r.close);
    assert!(is_violation(&r.replies[0]));
}

#[test]
fn a_second_auth_is_a_violation() {
    let v = only_good();
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, &v, 1);
    drive(
        &mut h,
        &mut s,
        &v,
        Message::Auth {
            credential: b"good".to_vec(),
        },
    );
    let r = drive(
        &mut h,
        &mut s,
        &v,
        Message::Auth {
            credential: b"good".to_vec(),
        },
    );
    assert!(r.close);
    assert!(is_violation(&r.replies[0]));
}

#[test]
fn a_client_sent_schema_advert_is_a_violation() {
    // The advertisement travels server-to-client only; a client that sends one is
    // speaking out of turn.
    let v = only_good();
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, &v, 1);
    let r = drive(
        &mut h,
        &mut s,
        &v,
        Message::SchemaAdvert {
            schema_version: 1,
            schema: b"{}".to_vec(),
        },
    );
    assert!(r.close);
    assert!(is_violation(&r.replies[0]));
}

#[test]
fn subscribe_before_auth_is_a_violation() {
    let v = only_good();
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, &v, 1);
    let r = drive(
        &mut h,
        &mut s,
        &v,
        Message::Subscribe {
            channel: Channel(0),
            room: b"room-1".to_vec(),
            last_seen_seq: 0,
            branch: Vec::new(),
        },
    );
    assert!(r.close);
    assert!(is_violation(&r.replies[0]));
}

#[test]
fn ops_before_auth_is_a_violation() {
    let v = only_good();
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, &v, 1);
    let r = drive(
        &mut h,
        &mut s,
        &v,
        Message::Ops {
            channel: Channel(0),
            ops: Vec::new(),
        },
    );
    assert!(r.close);
    assert!(is_violation(&r.replies[0]));
}

// --- the upgrade fast path (auth established before the message loop) ---

#[test]
fn a_fast_path_session_starts_with_the_actor_already_set() {
    let s = Session::authenticated(Identity::new(b"alice".to_vec()));
    assert_eq!(s.actor(), Some(&b"alice"[..]));
    assert_eq!(s.client(), None, "hello still names the client");
}

#[test]
fn a_session_captures_the_full_identity_claims() {
    // The session holds the roles/groups the credential asserted, so the policy
    // evaluator can read membership from it (consumed by the role-grant tier).
    let id = Identity::with_claims(
        b"alice".to_vec(),
        vec!["editor".into()],
        vec!["design".into()],
    );
    let s = Session::authenticated(id);
    let held = s.identity().expect("authenticated");
    assert_eq!(held.actor(), b"alice");
    assert_eq!(held.roles(), ["editor"]);
    assert_eq!(held.groups(), ["design"]);
}

#[test]
fn a_fast_path_session_subscribes_without_an_auth_phase() {
    let v = only_good();
    let mut h = hub();
    // The credential was verified at the transport upgrade; the session begins
    // authenticated and the client goes straight from Hello to Subscribe.
    let mut s = Session::authenticated(Identity::new(b"alice".to_vec()));
    hello(&mut h, &mut s, &v, 1);
    let r = drive(
        &mut h,
        &mut s,
        &v,
        Message::Subscribe {
            channel: Channel(0),
            room: b"room-1".to_vec(),
            last_seen_seq: 0,
            branch: Vec::new(),
        },
    );
    assert_eq!(
        r.replies,
        vec![Message::Ops {
            channel: Channel(0),
            ops: Vec::new(),
        }]
    );
    assert!(!r.close);
}

#[test]
fn an_in_band_auth_on_a_fast_path_session_is_a_violation() {
    let v = only_good();
    let mut h = hub();
    let mut s = Session::authenticated(Identity::new(b"alice".to_vec()));
    hello(&mut h, &mut s, &v, 1);
    // The actor is already established, so a redundant Auth is out of order.
    let r = drive(
        &mut h,
        &mut s,
        &v,
        Message::Auth {
            credential: b"good".to_vec(),
        },
    );
    assert!(r.close);
    assert!(is_violation(&r.replies[0]));
    assert_eq!(s.actor(), Some(&b"alice"[..]), "the actor is unchanged");
}

// --- the dev-mode default ---

#[test]
fn allow_all_accepts_any_credential_and_adopts_it_as_the_actor() {
    let v = AllowAll;
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, &v, 1);
    let r = drive(
        &mut h,
        &mut s,
        &v,
        Message::Auth {
            credential: b"whoever".to_vec(),
        },
    );
    assert_eq!(
        r.replies,
        vec![Message::AuthOk {
            actor: b"whoever".to_vec(),
        }]
    );
    assert_eq!(s.actor(), Some(&b"whoever"[..]));
}
