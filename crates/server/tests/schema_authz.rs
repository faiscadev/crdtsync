//! Schema `@auth` grants enforced end to end through the registry.
//!
//! An enforcing client declares an `{app_id, version}` at the handshake; the
//! server resolves its registered schema and composes its grants under the
//! deployment authorizer at every data-plane enforcement point. With the
//! deployment ACL abstaining, the schema alone decides: an authenticated actor
//! may subscribe (read), an `editor` may write ops, and a `viewer` is denied the
//! write the schema explicitly refuses. A per-recipient read grant also gates the
//! op fan-out — a peer the schema does not grant read never receives the ops.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, Message, Op, Scalar};
use crdtsync_server::acl::{Acl, ResourceMatch, Subject};
use crdtsync_server::{Action, ConnId, Identity, ManualClock, NoTimedTtl, Registry, StaticTokens};

const APP: &[u8] = b"collab";
const ROOM: &[u8] = b"room-a";

/// Read to any authenticated actor, write to `editor`, write explicitly denied to
/// `viewer` — the whole document (`/`).
const SCHEMA: &str = r#"{ "schema": "collab", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "auth": {
        "roles": ["editor", "viewer"],
        "grants": [
            { "allow": "read",  "to": "authenticated", "on": "/" },
            { "allow": "write", "to": "editor",        "on": "/" },
            { "deny":  "write", "to": "viewer",        "on": "/" }
        ]
    } }"#;

/// A second app whose schema grants read only to the `owner` role — a room this
/// app governs is closed to a bare authenticated actor.
const APP_STRICT: &[u8] = b"strict";
const SCHEMA_STRICT: &str = r#"{ "schema": "strict", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "auth": {
        "roles": ["owner"],
        "grants": [ { "allow": "read", "to": "owner", "on": "/" } ]
    } }"#;

/// A third app whose registered body is not valid schema text — it binds a room
/// but resolves to no parsed schema (so it grants nothing).
const APP_BROKEN: &[u8] = b"broken";
const SCHEMA_BROKEN: &[u8] = b"not valid schema json {";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A registry holding `SCHEMA` at version 1 of `APP`, an abstaining deployment
/// ACL (so the schema alone decides), and a token table minting the given actors
/// with their roles.
fn registry(tokens: StaticTokens) -> Registry {
    let mut sr = crdtsync_server::SchemaRegistry::new();
    sr.register(APP, 1, SCHEMA.as_bytes(), b"").unwrap();
    sr.register(APP_STRICT, 1, SCHEMA_STRICT.as_bytes(), b"")
        .unwrap();
    sr.register(APP_BROKEN, 1, SCHEMA_BROKEN, b"").unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens));
    // An empty ACL abstains on every request, deferring to the schema grants.
    r.set_authorizer(Box::new(Acl::new()));
    // A fixed clock: the default SystemClock is unreadable under Miri isolation.
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

fn tokens(rows: &[(&str, &str, &[&str])]) -> StaticTokens {
    let mut t = StaticTokens::new();
    for (credential, actor, roles) in rows {
        t.insert_identity(
            credential.as_bytes().to_vec(),
            Identity::with_claims(
                actor.as_bytes().to_vec(),
                roles.iter().map(|r| r.to_string()).collect(),
                Vec::new(),
            ),
        );
    }
    t
}

/// Hello + Auth a connection as `credential` declaring `{APP, 1}`.
fn hello_auth(r: &mut Registry, client: u8, credential: &str) -> ConnId {
    hello_auth_app(r, client, credential, APP)
}

/// Hello + Auth a connection as `credential` declaring `{app, 1}`.
fn hello_auth_app(r: &mut Registry, client: u8, credential: &str, app: &[u8]) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: app.to_vec(),
            schema_version: 1,
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: credential.as_bytes().to_vec(),
        }
    ));
    r.take_outbox(id);
    id
}

/// Whether a subscribe on channel 0 was accepted — a bound channel, no error.
fn subscribe_ok(r: &mut Registry, id: ConnId) -> bool {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            last_seen_seq: 0,
        }
    ));
    let denied = r
        .take_outbox(id)
        .into_iter()
        .any(|m| matches!(m, Message::Error { .. }));
    !denied
}

fn sample_ops(client: u8) -> Vec<Op> {
    Document::new(cid(client)).transact(|tx| tx.register(b"age", Scalar::Int(30)))
}

/// Whether an ops write on channel 0 was accepted — no refusal reply. A denied
/// write comes back as a non-fatal `OpsRejected`; a handshake failure as `Error`.
fn write_ok(r: &mut Registry, id: ConnId, client: u8) -> bool {
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: Channel(0),
            ops: sample_ops(client),
        }
    ));
    let denied = r
        .take_outbox(id)
        .into_iter()
        .any(|m| matches!(m, Message::Error { .. } | Message::OpsRejected { .. }));
    !denied
}

#[test]
fn an_authenticated_actor_may_subscribe_under_the_schema_read_grant() {
    let mut r = registry(tokens(&[("t-alice", "alice", &[])]));
    let alice = hello_auth(&mut r, 1, "t-alice");
    assert!(
        subscribe_ok(&mut r, alice),
        "the schema grants read to any authenticated actor",
    );
}

#[test]
fn an_editor_may_write_ops_but_an_unroled_actor_may_not() {
    let mut r = registry(tokens(&[
        ("t-ed", "ed", &["editor"]),
        ("t-alice", "alice", &[]),
    ]));
    let ed = hello_auth(&mut r, 1, "t-ed");
    assert!(subscribe_ok(&mut r, ed));
    assert!(
        write_ok(&mut r, ed, 1),
        "the schema grants write to the editor role",
    );

    let alice = hello_auth(&mut r, 2, "t-alice");
    assert!(subscribe_ok(&mut r, alice));
    assert!(
        !write_ok(&mut r, alice, 2),
        "no grant allows an unroled actor to write",
    );
}

#[test]
fn a_viewer_is_denied_the_write_the_schema_refuses() {
    // A viewer that is also an editor: the schema's explicit write deny wins.
    let mut r = registry(tokens(&[("t-v", "v", &["editor", "viewer"])]));
    let v = hello_auth(&mut r, 1, "t-v");
    assert!(subscribe_ok(&mut r, v), "read is still granted");
    assert!(
        !write_ok(&mut r, v, 1),
        "the schema's explicit write deny for viewer wins over its editor allow",
    );
}

#[test]
fn the_op_fan_out_is_gated_by_each_peers_schema_read_grant() {
    // ed writes; a peer authenticated under the schema receives the fan-out.
    let mut r = registry(tokens(&[
        ("t-ed", "ed", &["editor"]),
        ("t-peer", "peer", &["viewer"]),
    ]));
    let ed = hello_auth(&mut r, 1, "t-ed");
    let peer = hello_auth(&mut r, 2, "t-peer");
    assert!(subscribe_ok(&mut r, ed));
    assert!(subscribe_ok(&mut r, peer));
    r.take_outbox(ed);
    r.take_outbox(peer);
    assert!(write_ok(&mut r, ed, 1));

    let got: Vec<Op> = r
        .take_outbox(peer)
        .into_iter()
        .filter_map(|m| match m {
            Message::Ops { ops, .. } => Some(ops),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(
        got.len(),
        1,
        "an authenticated peer receives the broadcast ops"
    );
}

#[test]
fn a_foreign_app_cannot_read_a_room_governed_by_a_stricter_schema() {
    // Room bound to `strict` (grants read only to `owner`) by an owner. An
    // attacker holding no `owner` role declares the permissive `collab` app and
    // subscribes: the room's *governing* schema (strict) gates the read, not the
    // attacker's self-declared one, so the escalation is refused.
    let mut r = registry(tokens(&[
        ("t-owner", "owner", &["owner"]),
        ("t-attacker", "mallory", &[]),
    ]));
    let owner = hello_auth_app(&mut r, 1, "t-owner", APP_STRICT);
    assert!(
        subscribe_ok(&mut r, owner),
        "the owner establishes the room"
    );

    let attacker = hello_auth_app(&mut r, 2, "t-attacker", APP);
    assert!(
        !subscribe_ok(&mut r, attacker),
        "a foreign app's schema must not authorize a read of a room another app governs",
    );
}

#[test]
fn a_room_bound_to_an_unparseable_schema_does_not_fall_back_to_a_foreign_app() {
    // `keeper` (app `broken`, whose registered body will not parse) binds the room
    // — its own read is permitted by the deployment, not its schema. `broken`
    // resolves to no parsed schema, so the room grants nothing. A foreign `mallory`
    // (app `collab`, which grants read to any authenticated actor) then subscribes:
    // the room is *bound*, so its (empty) governing schema gates the read — the
    // fallback to mallory's own permissive schema must not fire.
    let mut r = registry(tokens(&[
        ("t-keeper", "keeper", &[]),
        ("t-mallory", "mallory", &[]),
    ]));
    // The deployment permits only keeper's read, abstaining for everyone else.
    r.set_authorizer(Box::new(Acl::new().allow(
        Subject::Actor(b"keeper".to_vec()),
        Some(Action::Read),
        ResourceMatch::Room(ROOM.to_vec()),
    )));

    let keeper = hello_auth_app(&mut r, 1, "t-keeper", APP_BROKEN);
    assert!(
        subscribe_ok(&mut r, keeper),
        "the deployment permits keeper's read, binding the room to the broken app",
    );

    let mallory = hello_auth_app(&mut r, 2, "t-mallory", APP);
    assert!(
        !subscribe_ok(&mut r, mallory),
        "a room bound to an unparseable schema must not fall back to a foreign app's grants",
    );
}

#[test]
fn the_schema_read_grant_gates_fan_out_even_under_an_injected_awareness_policy() {
    // An injected awareness policy must not disable the schema authorization tier:
    // the room binding it depends on is maintained regardless. A schema-granted
    // peer keeps receiving the live op fan-out.
    let mut r = registry(tokens(&[
        ("t-ed", "ed", &["editor"]),
        ("t-peer", "peer", &["viewer"]),
    ]));
    r.set_awareness_policy(Arc::new(NoTimedTtl));
    let ed = hello_auth(&mut r, 1, "t-ed");
    let peer = hello_auth(&mut r, 2, "t-peer");
    assert!(subscribe_ok(&mut r, ed));
    assert!(subscribe_ok(&mut r, peer));
    r.take_outbox(ed);
    r.take_outbox(peer);
    assert!(write_ok(&mut r, ed, 1));

    let ops: usize = r
        .take_outbox(peer)
        .into_iter()
        .filter_map(|m| match m {
            Message::Ops { ops, .. } => Some(ops.len()),
            _ => None,
        })
        .sum();
    assert_eq!(
        ops, 1,
        "the schema-granted peer still receives the fan-out under an injected policy",
    );
}

#[test]
fn a_relay_client_of_an_unregistered_app_falls_to_the_deployment_deny() {
    // No app declared: the connection is a relay, no schema governs it, and the
    // abstaining deployment ACL denies the subscribe.
    let mut r = registry(tokens(&[("t-alice", "alice", &[])]));
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: b"t-alice".to_vec(),
        }
    ));
    r.take_outbox(id);
    assert!(
        !subscribe_ok(&mut r, id),
        "a relay connection has no schema grant, so the abstaining ACL denies it",
    );
}
