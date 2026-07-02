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
use crdtsync_server::auth::{AllowAll, Verifier};
use crdtsync_server::{step, Hub, Session};

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
    |cred: &[u8]| (cred == b"good").then(|| b"alice".to_vec())
}

fn hello(hub: &mut Hub, s: &mut Session, v: &dyn Verifier, client: u8) {
    let r = step(
        hub,
        s,
        v,
        Message::Hello {
            client: cid(client),
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

    let r = step(
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
    step(
        &mut h,
        &mut s,
        &v,
        Message::Auth {
            credential: b"good".to_vec(),
        },
    );
    let r = step(
        &mut h,
        &mut s,
        &v,
        Message::Subscribe {
            channel: Channel(0),
            room: b"room-1".to_vec(),
            last_seen_seq: 0,
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
    let r = step(
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
    let r = step(
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
    step(
        &mut h,
        &mut s,
        &v,
        Message::Auth {
            credential: b"good".to_vec(),
        },
    );
    let r = step(
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
fn subscribe_before_auth_is_a_violation() {
    let v = only_good();
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, &v, 1);
    let r = step(
        &mut h,
        &mut s,
        &v,
        Message::Subscribe {
            channel: Channel(0),
            room: b"room-1".to_vec(),
            last_seen_seq: 0,
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
    let r = step(
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

// --- the dev-mode default ---

#[test]
fn allow_all_accepts_any_credential_and_adopts_it_as_the_actor() {
    let v = AllowAll;
    let mut h = hub();
    let mut s = Session::new();
    hello(&mut h, &mut s, &v, 1);
    let r = step(
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
