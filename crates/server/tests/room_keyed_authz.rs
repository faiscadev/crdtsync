//! The acting schema of a **room-keyed** management frame — branch management and a
//! cross-zone token request. (The third, a clone, is held by `clone_redaction.rs`.)
//!
//! A room-keyed frame carries the room it acts on in the frame itself, so a caller
//! may send one without holding any subscription to that room. Two rulings follow,
//! and this suite holds both.
//!
//! **Every room-keyed frame resolves its acting room.** The `@auth` grants the
//! enforcement points compose under come from the room the frame names. A frame that
//! resolved no room at all reached the schema tier with nothing, so that tier could
//! not speak: a schema grant naming branch management was silently ignored and an
//! abstaining deployment default-denied. Fail-closed, so a gap rather than a hole —
//! but a gap that made the schema tier unreachable for the whole branch
//! sub-protocol. A room-keyed frame naming a room *nothing has bound* resolves its
//! room and, correctly, no schema at all.
//!
//! **That schema is the named room's binding, never the connection's declared app.**
//! The connection's own app is the fallback for [`Message::Subscribe`] alone — the
//! one frame whose caller is about to become the room's incumbent. Anywhere else it
//! would be the caller picking which grants and zone declarations govern a room it
//! does not establish.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, ElementId, ErrorCode, Message, Op, Scalar};
use crdtsync_server::acl::Acl;
use crdtsync_server::{ConnId, Identity, ManualClock, Registry, StaticTokens};

const ROOM: &[u8] = b"room-a";
const CH: Channel = Channel(0);
/// The zone key sealing a cross-zone token; any 32 bytes.
const KEY: [u8; 32] = [0x5a; 32];

/// Read and write to any authenticated actor — a permissive app.
const APP_OPEN: &[u8] = b"open";
const SCHEMA_OPEN: &str = r#"{ "schema": "open", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "auth": { "grants": [
        { "allow": "read",  "to": "authenticated", "on": "/" },
        { "allow": "write", "to": "authenticated", "on": "/" }
    ] } }"#;

/// Read to any authenticated actor and write to nobody — a room whose branches may
/// be listed but not mutated.
const APP_READONLY: &[u8] = b"readonly";
const SCHEMA_READONLY: &str = r#"{ "schema": "readonly", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "auth": { "grants": [ { "allow": "read", "to": "authenticated", "on": "/" } ] } }"#;

/// Read and write only to the `owner` role — a room closed to a bare authenticated
/// actor.
const APP_STRICT: &[u8] = b"strict";
const SCHEMA_STRICT: &str = r#"{ "schema": "strict", "version": 1, "root": "R",
    "types": { "R": { "kind": "map" } },
    "auth": {
        "roles": ["owner"],
        "grants": [
            { "allow": "read",  "to": "owner", "on": "/" },
            { "allow": "write", "to": "owner", "on": "/" }
        ]
    } }"#;

/// A zoned app whose grants let any authenticated actor write anywhere — so a
/// cross-zone token request under *this* schema is granted, and the only thing that
/// can refuse it is the schema not being the acting one.
const APP_ZONED: &[u8] = b"zoned";
const SCHEMA_ZONED: &str = r#"{ "schema": "zoned", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "board": "Frag", "notes": "Frag" } },
        "Frag": { "kind": "fragment", "children": { "a": {} } },
        "a": { "kind": "xml", "tag": "a", "children": {} }
    },
    "zones": { "za": "/board", "zb": "/notes" },
    "auth": { "grants": [
        { "allow": "read",  "to": "authenticated", "on": "/" },
        { "allow": "write", "to": "authenticated", "on": "/" }
    ] } }"#;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A registry holding all four apps at version 1, an abstaining deployment ACL — so
/// the acting schema alone decides — and a token table minting the given actors.
fn registry(rows: &[(&str, &str, &[&str])]) -> Registry {
    let mut sr = crdtsync_server::SchemaRegistry::new();
    for (app, src) in [
        (APP_OPEN, SCHEMA_OPEN),
        (APP_READONLY, SCHEMA_READONLY),
        (APP_STRICT, SCHEMA_STRICT),
        (APP_ZONED, SCHEMA_ZONED),
    ] {
        sr.register(app, 1, src.as_bytes(), b"").unwrap();
    }
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
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(t));
    r.set_authorizer(Box::new(Acl::new()));
    // A fixed clock: the default SystemClock is unreadable under Miri isolation.
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

/// Hello + Auth a connection declaring `{app, 1}`. It subscribes nothing — a
/// room-keyed frame needs no subscription.
fn hello_auth(r: &mut Registry, client: u8, credential: &str, app: &[u8]) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: app.to_vec(),
            schema_version: 1,
            codecs: Vec::new(),
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

/// Subscribe `id` to `ROOM` on `CH`, binding the room to that connection's app.
fn subscribe(r: &mut Registry, id: ConnId) {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            zone: Vec::new(),
            last_seen_seq: 0,
            branch: Vec::new(),
        }
    ));
    assert!(
        !r.take_outbox(id).iter().any(is_error),
        "the binding subscribe is expected to be accepted",
    );
}

/// Write a register through `id`'s channel so the room holds state a version can
/// capture.
fn write_state(r: &mut Registry, id: ConnId, client: u8) {
    let mut replica = Document::new(cid(client).for_channel(CH.0));
    let ops = replica.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    assert!(r.deliver(id, Message::Ops { channel: CH, ops }));
    r.take_outbox(id);
}

fn is_error(m: &Message) -> bool {
    matches!(m, Message::Error { .. })
}

/// Deliver `msg` and return the single reply it produced.
fn reply(r: &mut Registry, id: ConnId, msg: Message) -> Message {
    assert!(r.deliver(id, msg), "a denial keeps the connection open");
    let mut out = r.take_outbox(id);
    assert_eq!(out.len(), 1, "one request, one reply");
    out.remove(0)
}

fn is_forbidden(m: &Message) -> bool {
    matches!(
        m,
        Message::Error {
            code: ErrorCode::Forbidden,
            ..
        }
    )
}

fn is_branch_set(m: &Message) -> bool {
    matches!(m, Message::Branches { .. })
}

/// The six room-keyed branch frames, each naming `ROOM`. A list request needs read
/// authority, the five mutations write.
fn branch_frames() -> Vec<Message> {
    vec![
        Message::BranchList {
            room: ROOM.to_vec(),
        },
        Message::BranchFork {
            room: ROOM.to_vec(),
            name: b"forked".to_vec(),
            from_branch: b"main".to_vec(),
        },
        Message::BranchForkFromVersion {
            room: ROOM.to_vec(),
            name: b"from-v1".to_vec(),
            version: b"v1".to_vec(),
        },
        Message::BranchRestore {
            room: ROOM.to_vec(),
            name: b"restored".to_vec(),
            version: b"v1".to_vec(),
        },
        Message::BranchPublish {
            room: ROOM.to_vec(),
            published: b"forked".to_vec(),
        },
        Message::BranchDelete {
            room: ROOM.to_vec(),
            name: b"forked".to_vec(),
        },
    ]
}

/// Only the five mutating frames — the ones a write grant decides.
fn branch_mutations() -> Vec<Message> {
    branch_frames().into_iter().skip(1).collect()
}

// --- the schema tier is reachable at every room-keyed branch frame ----------

#[test]
fn a_schema_grant_authorizes_every_room_keyed_branch_frame() {
    // The deployment abstains throughout, so the room's schema grants are the whole
    // decision: reaching a branch set at all proves the schema tier spoke.
    let mut r = registry(&[("t-own", "own", &[]), ("t-mgr", "mgr", &[])]);
    let owner = hello_auth(&mut r, 1, "t-own", APP_OPEN);
    subscribe(&mut r, owner);
    write_state(&mut r, owner, 1);
    assert!(r.hub_mut().create_version(ROOM, b"v1").unwrap());

    // A second connection that never subscribed — a room-keyed frame carries its
    // own room.
    let mgr = hello_auth(&mut r, 2, "t-mgr", APP_OPEN);
    for frame in branch_frames() {
        let got = reply(&mut r, mgr, frame.clone());
        assert!(
            is_branch_set(&got),
            "the room's schema grants this actor branch management, so {frame:?} is \
             answered with the branch set, got {got:?}",
        );
    }
}

#[test]
fn a_read_only_schema_lists_branches_but_refuses_every_mutation() {
    // The same reachability, in the refusing direction: the acting schema decides
    // list and mutation separately, so it is a real grant and not a blanket allow.
    let mut r = registry(&[("t-own", "own", &[]), ("t-mgr", "mgr", &[])]);
    let owner = hello_auth(&mut r, 1, "t-own", APP_READONLY);
    subscribe(&mut r, owner);

    let mgr = hello_auth(&mut r, 2, "t-mgr", APP_READONLY);
    let listed = reply(
        &mut r,
        mgr,
        Message::BranchList {
            room: ROOM.to_vec(),
        },
    );
    assert!(
        is_branch_set(&listed),
        "the room's schema grants read, so the list is served, got {listed:?}",
    );
    for frame in branch_mutations() {
        let got = reply(&mut r, mgr, frame.clone());
        assert!(
            is_forbidden(&got),
            "the room's schema grants no write, so {frame:?} is refused, got {got:?}",
        );
    }
}

// --- the acting schema is the room's binding, not the caller's app ----------

#[test]
fn a_branch_frame_resolves_the_named_rooms_binding_not_the_connections_app() {
    // The room is governed by the permissive app. A caller whose *own* app grants it
    // nothing is still served: the frame is decided by the room's binding.
    let mut r = registry(&[("t-own", "own", &[]), ("t-out", "out", &[])]);
    let owner = hello_auth(&mut r, 1, "t-own", APP_OPEN);
    subscribe(&mut r, owner);

    let outsider = hello_auth(&mut r, 2, "t-out", APP_STRICT);
    let got = reply(
        &mut r,
        outsider,
        Message::BranchFork {
            room: ROOM.to_vec(),
            name: b"forked".to_vec(),
            from_branch: b"main".to_vec(),
        },
    );
    assert!(
        is_branch_set(&got),
        "the room's own app grants the write; the caller's stricter app does not \
         govern the room, got {got:?}",
    );
}

#[test]
fn a_permissive_declared_app_cannot_manage_a_strictly_governed_rooms_branches() {
    // The mirror, and the escalation the resolution exists to refuse: the room is
    // governed by the strict app, and an actor holding no `owner` role declares the
    // permissive one. Its own app's grants decide nothing here.
    //
    // The refusal alone cannot tell the strict schema deciding from no schema
    // deciding, so a role-holder declaring the very same permissive app is driven
    // through it too: it is served, which is only possible if the acting schema is
    // the *strict* one — the app it declares grants by class, not by role.
    let mut r = registry(&[
        ("t-own", "own", &["owner"]),
        ("t-mal", "mallory", &[]),
        ("t-role", "roleholder", &["owner"]),
    ]);
    let owner = hello_auth(&mut r, 1, "t-own", APP_STRICT);
    subscribe(&mut r, owner);

    let mallory = hello_auth(&mut r, 2, "t-mal", APP_OPEN);
    for frame in branch_frames() {
        let got = reply(&mut r, mallory, frame.clone());
        assert!(
            is_forbidden(&got),
            "the strict room's schema decides {frame:?}, not mallory's declared app, \
             got {got:?}",
        );
    }
    assert_eq!(
        r.hub().branches(ROOM).len(),
        1,
        "every refused mutation left only main",
    );

    let roleholder = hello_auth(&mut r, 3, "t-role", APP_OPEN);
    let got = reply(
        &mut r,
        roleholder,
        Message::BranchFork {
            room: ROOM.to_vec(),
            name: b"forked".to_vec(),
            from_branch: b"main".to_vec(),
        },
    );
    assert!(
        is_branch_set(&got),
        "the strict schema's owner grant is what serves this fork — the declared \
         permissive app grants every authenticated actor alike, got {got:?}",
    );
}

// --- an unbound room lends its grants from nowhere --------------------------

#[test]
fn an_unbound_room_lends_a_branch_frame_none_of_the_callers_grants() {
    // A room nothing ever subscribed — here an ingest straight into the hub, the
    // shape an import or a promoted replica also produces. It has no binding, so a
    // room-keyed frame naming it is governed by nothing: the caller's declared app
    // is not a fallback outside a subscribe.
    let mut r = registry(&[("t-mal", "mallory", &[])]);
    let mut seed = Document::new(cid(9));
    let ops = seed.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    r.hub_mut().ingest(ROOM, ops, None).unwrap();

    let mallory = hello_auth(&mut r, 1, "t-mal", APP_OPEN);
    for frame in branch_frames() {
        let got = reply(&mut r, mallory, frame.clone());
        assert!(
            is_forbidden(&got),
            "an unbound room is governed by no schema, so {frame:?} default-denies, \
             got {got:?}",
        );
    }
}

#[test]
fn an_unbound_room_lends_a_cross_zone_token_none_of_the_callers_zone_layout() {
    // The same ruling at the sibling room-keyed seam: a cross-zone token resolves
    // the source zone, the destination's root keys and both write verdicts off the
    // acting schema, so a caller-supplied one would let it name the zone layout of
    // the token authorizing its own move.
    let mut r = registry(&[("t-mal", "mallory", &[])]);
    let (ops, child) = board_with_child();
    r.hub_mut().set_zone_key(KEY);
    r.hub_mut().ingest(ROOM, ops, None).unwrap();

    let mallory = hello_auth(&mut r, 1, "t-mal", APP_ZONED);
    let got = reply(
        &mut r,
        mallory,
        Message::CrossZoneToken {
            room: ROOM.to_vec(),
            element: child,
            dst_zone: b"zb".to_vec(),
        },
    );
    assert!(
        is_forbidden(&got),
        "an unbound room declares no zones the caller may name, got {got:?}",
    );
}

#[test]
fn a_bound_rooms_own_zoned_schema_still_issues_a_cross_zone_token() {
    // The grant half of the same seam: bound to the zoned app, the token is minted —
    // so the refusal above is the missing binding, not a broken issuance path.
    let mut r = registry(&[("t-own", "own", &[])]);
    let (ops, child) = board_with_child();
    r.hub_mut().set_zone_key(KEY);
    r.hub_mut().ingest(ROOM, ops, None).unwrap();

    let owner = hello_auth(&mut r, 1, "t-own", APP_ZONED);
    subscribe(&mut r, owner);
    let got = reply(
        &mut r,
        owner,
        Message::CrossZoneToken {
            room: ROOM.to_vec(),
            element: child,
            dst_zone: b"zb".to_vec(),
        },
    );
    assert!(
        matches!(got, Message::CrossZoneTokenGrant { .. }),
        "the room's own zoned schema declares `zb` and grants the write, got {got:?}",
    );
}

/// The ops seeding a room with `board` and `notes` fragments and one child element
/// in `board`, and that child's id — the `za` → `zb` crossing a cross-zone token
/// authorizes.
fn board_with_child() -> (Vec<Op>, ElementId) {
    let mut d = Document::new(cid(1));
    let mut child = ElementId::from_bytes([0u8; 16]);
    let ops = d.transact(|tx| {
        let mut board = tx.xml_fragment(b"board");
        child = board.children().insert_element(0, b"a").id();
        tx.xml_fragment(b"notes");
    });
    (ops, child)
}
