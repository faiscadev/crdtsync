//! A clone carries every redaction that governs its source (C28).
//!
//! `Hub::clone_room` is `export_room(src)` into a fresh `dst` — the raw
//! materialized replica, doc-ACL tuples and all. Two things decide whether that is
//! a laundering channel. The **gate**: a room-tier read admits a *partial* reader —
//! one an ACL deny carves a subtree out of, or one a deployment denies a zone to —
//! so the clone hands it, whole, the state its own subscription would have narrowed.
//! And the **authority root**: the tuples ride the snapshot but the root does not, so
//! a creatorless `dst` short-circuits `reads_whole_document` to `true` and abstains in
//! `doc_acl_read_at` — every deny that rode along is inert there.
//!
//! So the clone is gated on reading the source **whole** — the same `reads_whole_document`
//! seam the catch-up and version projections narrow by, composed with every declared
//! zone being readable — and the clone installs the source's creator, so the tuples
//! land under the authority they were authored against and keep deciding.
//!
//! The schema tier is what makes the gap observable: it grants root read to any
//! authenticated actor, so bob passes the room tier, while alice's doc-ACL
//! `Deny(Read)` at `/secret` carves that key out of his whole-document read. Both
//! readers hold the schema's `editor` write grant, so the destination half of the
//! gate never decides a case here — only the source read does.
//!
//! Which schema that is, is half the gate: it carries the zone declarations, so the
//! two cases where a caller could name it are pinned too — a bound source (the
//! caller's own app is ignored) and a never-bound one (nothing governs it, so
//! nothing grants). In-process, fixed clock, no socket or fs: Miri-clean.

use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{AclEffect, ClientId, Document, ErrorCode, Message, Op, Scalar};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::{
    Action, ConnId, Identity, ManualClock, Registry, SchemaRegistry, StaticTokens,
};

const CH: Channel = Channel(0);
const APP: &[u8] = b"collab";
const ZONELESS_APP: &[u8] = b"flat";
const SRC: &[u8] = b"template";
const DST: &[u8] = b"copy";

/// The key alice denies bob, and one she leaves readable — the two halves every
/// assertion here reads apart.
const SECRET: &[u8] = b"secret";
const OPEN: &[u8] = b"open";

/// Read to any authenticated actor, write to `editor`, and two declared zones. Root
/// read arrives from the schema tier, so a doc-ACL deny is the only thing that can
/// narrow it — and narrowing it needs the creator.
const SCHEMA: &str = r#"{ "schema": "collab", "version": 1, "root": "Doc",
    "types": { "Doc": { "kind": "map" } },
    "zones": { "za": "/board", "zb": "/notes" },
    "auth": {
        "roles": ["editor"],
        "grants": [
            { "allow": "read",  "to": "authenticated", "on": "/" },
            { "allow": "write", "to": "editor",        "on": "/" }
        ]
    } }"#;

/// The same grants with no `zones` block — the app a caller declares when it would
/// rather the gate read its zone declarations than the source's.
const ZONELESS: &str = r#"{ "schema": "flat", "version": 1, "root": "Doc",
    "types": { "Doc": { "kind": "map" } },
    "auth": {
        "roles": ["editor"],
        "grants": [
            { "allow": "read",  "to": "authenticated", "on": "/" },
            { "allow": "write", "to": "editor",        "on": "/" }
        ]
    } }"#;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// alice bootstraps the source, so she is its creator and reads it whole. bob, zoe
/// and dana are editors too — each passes the destination write gate, so only the
/// source read gate can refuse them.
fn tokens() -> StaticTokens {
    let mut t = StaticTokens::new();
    for actor in ["alice", "bob", "zoe", "dana"] {
        t.insert_identity(
            format!("t-{actor}").into_bytes(),
            Identity::with_claims(
                actor.as_bytes().to_vec(),
                vec!["editor".to_string()],
                Vec::new(),
            ),
        );
    }
    t
}

/// A deployment that abstains on everything except zoe's read of the `zb` partition,
/// which it denies in both the source and the copy — the zone carve-out the room-keyed
/// tier cannot see, since a zone verdict names a room and the clone's whole point is to
/// put the bytes under another one. Naming both is what an operator has to write, and
/// what lets the copy be read apart from the original here.
fn deployment() -> Acl {
    Acl::new()
        .deny(
            Subject::Actor(b"zoe".to_vec()),
            Some(Action::Read),
            ResourceMatch::Zone {
                room: SRC.to_vec(),
                zone: b"zb".to_vec(),
            },
        )
        .deny(
            Subject::Actor(b"zoe".to_vec()),
            Some(Action::Read),
            ResourceMatch::Zone {
                room: DST.to_vec(),
                zone: b"zb".to_vec(),
            },
        )
}

fn registry() -> Registry {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, SCHEMA.as_bytes(), b"").unwrap();
    sr.register(ZONELESS_APP, 1, ZONELESS.as_bytes(), b"")
        .unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens()));
    r.set_authorizer(Box::new(deployment()));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

/// Hello + Auth a connection as `actor`, declaring `{APP, 1}`.
fn hello_auth(r: &mut Registry, client: u8, actor: &str) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: APP.to_vec(),
            schema_version: 1,
            codecs: Vec::new(),
        }
    ));
    assert!(r.deliver(
        id,
        Message::Auth {
            credential: format!("t-{actor}").into_bytes(),
        }
    ));
    r.take_outbox(id);
    id
}

fn sub(room: &[u8]) -> Message {
    Message::Subscribe {
        channel: CH,
        room: room.to_vec(),
        branch: Vec::new(),
        zone: Vec::new(),
        last_seen_seq: 0,
    }
}

/// alice denies `actor` read at the top-level key `key`, authoring the tuple under
/// her own actor key (she is the source's creator, so the tuple is authoritative).
fn deny_read(doc: &mut Document, actor: &[u8], key: &[u8]) -> Vec<Op> {
    doc.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(actor_key(actor)),
            AclGrant::Capability(Capability::Read),
            AclEffect::Deny,
            encode_path(&[key]),
            actor_key(b"alice"),
        );
    })
}

/// A registry holding `SRC` with alice as its creator, `OPEN` and `SECRET` written,
/// and bob denied read at `/secret`.
fn seeded() -> Registry {
    let mut r = registry();
    let alice = hello_auth(&mut r, 1, "alice");
    assert!(r.deliver(alice, sub(SRC)));
    r.take_outbox(alice);

    let mut doc = Document::new(cid(1));
    // alice's first write establishes the room, so she becomes its creator — the
    // authority root her deny below is decided under.
    assert!(r.deliver(
        alice,
        Message::Ops {
            channel: CH,
            ops: doc.transact(|tx| {
                tx.register(OPEN, Scalar::Int(1));
                tx.register(SECRET, Scalar::Int(2));
            }),
        }
    ));
    assert!(r.deliver(
        alice,
        Message::Ops {
            channel: CH,
            ops: deny_read(&mut doc, b"bob", SECRET),
        }
    ));
    r.take_outbox(alice);
    assert_eq!(
        r.hub().room_creator(SRC).as_deref(),
        Some(b"alice".as_slice()),
        "the first authenticated writer is the source's creator",
    );
    r
}

/// Deliver a clone request from `id` and return its single reply.
fn clone(r: &mut Registry, id: ConnId) -> Message {
    assert!(
        r.deliver(
            id,
            Message::CloneRoom {
                src: SRC.to_vec(),
                dst: DST.to_vec(),
            }
        ),
        "a clone verdict keeps the connection open",
    );
    r.take_outbox(id).into_iter().next().expect("a reply")
}

/// The state `actor` is served for `room`, folded from whatever catch-up shape it
/// arrives in.
fn served(r: &mut Registry, client: u8, actor: &str, room: &[u8]) -> Document {
    let id = hello_auth(r, client, actor);
    assert!(r.deliver(id, sub(room)));
    let out = r.take_outbox(id);
    assert!(
        !out.iter().any(|m| matches!(m, Message::Error { .. })),
        "the schema's authenticated read grant admits {actor}: {out:?}",
    );
    let mut view = Document::new(cid(client));
    let mut caught_up = false;
    for msg in out {
        match msg {
            Message::Ops { ops, .. } => {
                caught_up = true;
                for op in &ops {
                    view.apply(op);
                }
            }
            Message::Snapshot { state, .. } => {
                caught_up = true;
                view = Document::decode_state(&state).expect("a served snapshot decodes");
            }
            _ => {}
        }
    }
    assert!(caught_up, "{actor} was served a catch-up for {room:?}");
    view
}

fn reads(doc: &Document, key: &[u8]) -> Option<i64> {
    match doc.get(key) {
        Some(crdtsync_core::Element::Register(reg)) => match reg.borrow().read() {
            Scalar::Int(n) => Some(*n),
            other => panic!("expected an int, got {other:?}"),
        },
        None => None,
        Some(other) => panic!("expected a register, got a {:?}", other.kind()),
    }
}

fn forbidden(m: &Message) -> bool {
    matches!(
        m,
        Message::Error {
            code: ErrorCode::Forbidden,
            ..
        }
    )
}

#[test]
fn a_partial_reader_is_refused_the_clone() {
    let mut r = seeded();
    let bob = hello_auth(&mut r, 2, "bob");

    let reply = clone(&mut r, bob);
    assert!(
        forbidden(&reply),
        "a reader an ACL deny carves a subtree out of cannot clone the source: {reply:?}",
    );
    assert!(
        !r.hub().holds_room(DST),
        "the refused clone minted no destination",
    );
}

#[test]
fn a_zone_denied_reader_is_refused_the_clone() {
    let mut r = seeded();
    let zoe = hello_auth(&mut r, 3, "zoe");

    let reply = clone(&mut r, zoe);
    assert!(
        forbidden(&reply),
        "a zone verdict names the source room, so a zone-denied reader cannot clone \
         out from under it: {reply:?}",
    );
    assert!(
        !r.hub().holds_room(DST),
        "the refused clone minted no destination",
    );
}

#[test]
fn a_whole_document_reader_clones_the_source_with_its_creator() {
    let mut r = seeded();
    let alice = hello_auth(&mut r, 4, "alice");

    let reply = clone(&mut r, alice);
    assert!(
        matches!(&reply, Message::CloneRoomResult { dst, created } if dst == DST && *created),
        "the source's whole-document reader clones it: {reply:?}",
    );
    assert_eq!(
        r.hub().room_creator(DST).as_deref(),
        Some(b"alice".as_slice()),
        "the clone carries the source's authority root",
    );
    let state = r.hub().export_room(DST).expect("the clone is materialized");
    let clone = Document::decode_state(&state).expect("the clone's state decodes");
    assert_eq!(reads(&clone, OPEN), Some(1), "the clone carries the source");
    assert_eq!(reads(&clone, SECRET), Some(2), "both halves of it");
}

/// The clone is rooted at the *source's* creator, not at whoever asked for it: the
/// tuples in the state were authored against that root, and a cloner who became the
/// root instead would own `/` in the clone and read exactly what the source withheld
/// from it. So the cloner holds no authority over the room it minted, and the
/// template's author holds it — read apart here by dana, a whole-document reader who
/// is not the source's creator.
#[test]
fn the_clone_is_rooted_at_the_source_creator_not_the_cloner() {
    let mut r = seeded();
    let dana = hello_auth(&mut r, 7, "dana");

    assert!(matches!(
        clone(&mut r, dana),
        Message::CloneRoomResult { created: true, .. }
    ));
    assert_eq!(
        r.hub().room_creator(DST).as_deref(),
        Some(b"alice".as_slice()),
        "the cloner does not become the clone's authority root",
    );
}

/// The zone dimension resolves against the schema governing the *source*, so a
/// caller cannot erase it by declaring an app of its own that happens to declare no
/// zones. zoe is denied `zb` on the source and asks under a zoneless app; the gate
/// still reads the source's zone block.
#[test]
fn a_caller_cannot_swap_in_its_own_schema_to_erase_the_sources_zones() {
    let mut r = seeded();
    let zoe = r.connect();
    assert!(r.deliver(
        zoe,
        Message::Hello {
            client: cid(8),
            app_id: ZONELESS_APP.to_vec(),
            schema_version: 1,
            codecs: Vec::new(),
        }
    ));
    assert!(r.deliver(
        zoe,
        Message::Auth {
            credential: b"t-zoe".to_vec(),
        }
    ));
    r.take_outbox(zoe);

    let reply = clone(&mut r, zoe);
    assert!(
        forbidden(&reply),
        "the source's own schema declares the zones the gate reads: {reply:?}",
    );
    assert!(!r.hub().holds_room(DST), "the refused clone minted nothing");
}

#[test]
fn the_clone_keeps_the_sources_denies_live() {
    let mut r = seeded();
    let alice = hello_auth(&mut r, 4, "alice");
    assert!(matches!(
        clone(&mut r, alice),
        Message::CloneRoomResult { created: true, .. }
    ));

    let view = served(&mut r, 5, "bob", DST);
    assert_eq!(
        reads(&view, OPEN),
        Some(1),
        "bob reads the clone's unredacted half",
    );
    assert_eq!(
        reads(&view, SECRET),
        None,
        "the deny that rode the snapshot still carves /secret out of the clone",
    );
}

#[test]
fn the_source_is_still_read_whole_by_its_creator() {
    let mut r = seeded();
    let view = served(&mut r, 6, "alice", SRC);
    assert_eq!(reads(&view, OPEN), Some(1));
    assert_eq!(
        reads(&view, SECRET),
        Some(2),
        "the creator reads the source whole",
    );
}

/// The clone carries the source's governing app, so the gate reads the same zone
/// block over the copy as over the original — a clone of a clone is still governed,
/// where a clone that came up unbound would resolve to no schema and so to no zones.
#[test]
fn the_clone_carries_the_sources_governing_app() {
    let mut r = seeded();
    let alice = hello_auth(&mut r, 4, "alice");
    assert!(matches!(
        clone(&mut r, alice),
        Message::CloneRoomResult { created: true, .. }
    ));
    assert_eq!(
        r.hub().governing_app(DST),
        r.hub().governing_app(SRC),
        "the clone is governed by the app that governs its source",
    );

    // The copy is now the source of a second clone. The deployment denies zoe `zb`
    // there too, and the gate can only find that deny by enumerating the zones the
    // copy's *own* governing schema declares — which it has because the clone carried
    // it. An unbound copy would declare none and serve zoe the partition.
    let zoe = hello_auth(&mut r, 8, "zoe");
    let second: &[u8] = b"copy-of-copy";
    assert!(r.deliver(
        zoe,
        Message::CloneRoom {
            src: DST.to_vec(),
            dst: second.to_vec(),
        }
    ));
    let reply = r.take_outbox(zoe).into_iter().next().expect("a reply");
    assert!(
        forbidden(&reply),
        "the clone's own zone block refuses a zone-denied cloner: {reply:?}",
    );
    assert!(!r.hub().holds_room(second), "no second copy was minted");
}

/// A source no app governs — one a relay connection opened, or an import minted —
/// is the case where the acting schema has no room binding to resolve. The caller's
/// own declared app must not stand in for it: the fallback exists for a room's first
/// subscriber, about to become its incumbent, and a cloner is neither. Letting it
/// stand would hand the caller the `@auth` grants that decide whether it may read
/// someone else's room whole.
mod a_never_bound_source {
    use super::*;

    const PERM_APP: &[u8] = b"perm";

    /// Root read to any authenticated actor — the grant mallory would like the gate
    /// to compose under.
    const PERMISSIVE: &str = r#"{ "schema": "perm", "version": 1, "root": "Doc",
        "types": { "Doc": { "kind": "map" } },
        "auth": {
            "roles": ["editor"],
            "grants": [ { "allow": "read", "to": "authenticated", "on": "/" } ]
        } }"#;

    /// alice reads and writes any room — she seeds the source — and mallory writes
    /// any room, so the destination half of the gate is settled and only the source
    /// read decides her clone. Nothing here grants mallory a read.
    fn deployment() -> Acl {
        Acl::new()
            .allow(
                Subject::Actor(b"alice".to_vec()),
                Some(Action::Read),
                ResourceMatch::AnyRoom,
            )
            .allow(
                Subject::Actor(b"alice".to_vec()),
                Some(Action::Write),
                ResourceMatch::AnyRoom,
            )
            .allow(
                Subject::Actor(b"mallory".to_vec()),
                Some(Action::Write),
                ResourceMatch::AnyRoom,
            )
    }

    fn registry() -> Registry {
        let mut sr = SchemaRegistry::new();
        sr.register(PERM_APP, 1, PERMISSIVE.as_bytes(), b"")
            .unwrap();
        let mut t = StaticTokens::new();
        for actor in ["alice", "mallory"] {
            t.insert_identity(
                format!("t-{actor}").into_bytes(),
                Identity::with_claims(actor.as_bytes().to_vec(), Vec::new(), Vec::new()),
            );
        }
        let mut r = Registry::new(cid(0xFF));
        r.set_schema_registry(Arc::new(Mutex::new(sr)));
        r.set_verifier(Box::new(t));
        r.set_authorizer(Box::new(deployment()));
        r.set_clock(Arc::new(ManualClock::new(0)));
        r
    }

    /// Hello + Auth as `actor`, declaring `app` (empty for a relay connection).
    fn hello_auth(r: &mut Registry, client: u8, actor: &str, app: &[u8]) -> ConnId {
        let id = r.connect();
        assert!(r.deliver(
            id,
            Message::Hello {
                client: cid(client),
                app_id: app.to_vec(),
                schema_version: if app.is_empty() { 0 } else { 1 },
                codecs: Vec::new(),
            }
        ));
        assert!(r.deliver(
            id,
            Message::Auth {
                credential: format!("t-{actor}").into_bytes(),
            }
        ));
        r.take_outbox(id);
        id
    }

    /// The source, opened and written over a relay connection, so no app governs it.
    fn seeded() -> Registry {
        let mut r = registry();
        let alice = hello_auth(&mut r, 1, "alice", b"");
        assert!(r.deliver(alice, sub(SRC)));
        r.take_outbox(alice);
        let mut doc = Document::new(cid(1));
        assert!(r.deliver(
            alice,
            Message::Ops {
                channel: CH,
                ops: doc.transact(|tx| {
                    tx.register(OPEN, Scalar::Int(1));
                    tx.register(SECRET, Scalar::Int(2));
                }),
            }
        ));
        r.take_outbox(alice);
        assert!(
            r.hub().governing_app(SRC).is_none(),
            "a relay connection binds the room to no app",
        );
        r
    }

    /// A destination name a subscriber bound before anything materialized under it
    /// must not lend its app to the copy — that is the same caller-chosen schema by
    /// another door, and the clone of an ungoverned source is ungoverned.
    #[test]
    fn a_binding_squatting_on_the_destination_name_does_not_govern_the_clone() {
        let mut r = seeded();
        // A connection declaring `PERM_APP` subscribes to the destination name. The
        // room never materializes, but the name is bound.
        let squatter = hello_auth(&mut r, 3, "alice", PERM_APP);
        assert!(r.deliver(squatter, sub(DST)));
        r.take_outbox(squatter);
        assert!(
            r.hub().governing_app(DST).is_some(),
            "the subscribe bound the destination name",
        );
        assert!(!r.hub().holds_room(DST), "and materialized nothing");

        let alice = hello_auth(&mut r, 4, "alice", b"");
        assert!(matches!(
            clone(&mut r, alice),
            Message::CloneRoomResult { created: true, .. }
        ));
        assert_eq!(
            r.hub().governing_app(DST),
            None,
            "the clone of an ungoverned source is ungoverned",
        );
    }

    #[test]
    fn the_caller_cannot_supply_the_schema_that_decides_its_own_read() {
        let mut r = seeded();
        let mallory = hello_auth(&mut r, 2, "mallory", PERM_APP);

        let reply = clone(&mut r, mallory);
        assert!(
            forbidden(&reply),
            "an ungoverned source grants nothing, whatever the caller declares: {reply:?}",
        );
        assert!(!r.hub().holds_room(DST), "the refused clone minted nothing");
    }
}

/// The gate reads its inputs from this node's own state — the source's ACL records,
/// its creator, and the binding that names its zones — so it is only as authoritative
/// as this node's copy of the source. A replica holds one: `export_room` would hand
/// it over at that replica's replication and binding freshness. So the clone is
/// served only where one node leads both rooms, and elsewhere it is the no-op it
/// already was for a source the node does not hold.
mod a_source_this_node_does_not_lead {
    use super::*;
    use crdtsync_server::membership::Membership;
    use crdtsync_server::placement::NodeId;

    const SELF_ADDR: &str = "10.0.0.1:9000";
    const REPLICAS: usize = 3;

    fn members() -> String {
        (1..=5)
            .map(|i| format!("10.0.0.{i}:9000"))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn membership() -> Membership {
        Membership::from_static_config(None, Some(SELF_ADDR), &members(), REPLICAS)
            .expect("a static cluster config")
    }

    /// `(a room this node leads, a room it does not)` — the destination and the
    /// source of a clone that spans two leaders.
    fn led_here_and_elsewhere() -> (Vec<u8>, Vec<u8>) {
        let m = membership();
        let me = NodeId::from_addr(SELF_ADDR);
        let (mut here, mut elsewhere) = (None, None);
        for i in 0..1_000 {
            let room = format!("room-{i}").into_bytes();
            let slot = if m.primary_for(&room).as_ref() == Some(&me) {
                &mut here
            } else {
                &mut elsewhere
            };
            slot.get_or_insert(room);
            if here.is_some() && elsewhere.is_some() {
                break;
            }
        }
        (
            here.expect("a room this node leads"),
            elsewhere.expect("a room it does not"),
        )
    }

    /// alice reads and writes every room, so the clone's authorization is settled and
    /// only the routing decides it.
    fn registry() -> Registry {
        let mut t = StaticTokens::new();
        t.insert_identity(
            b"t-alice".to_vec(),
            Identity::with_claims(b"alice".to_vec(), Vec::new(), Vec::new()),
        );
        let mut r = Registry::new(cid(0xFF));
        r.set_verifier(Box::new(t));
        r.set_authorizer(Box::new(
            Acl::new()
                .allow(
                    Subject::Actor(b"alice".to_vec()),
                    Some(Action::Read),
                    ResourceMatch::AnyRoom,
                )
                .allow(
                    Subject::Actor(b"alice".to_vec()),
                    Some(Action::Write),
                    ResourceMatch::AnyRoom,
                ),
        ));
        r.set_clock(Arc::new(ManualClock::new(0)));
        r.set_membership(membership());
        r
    }

    #[test]
    fn the_clone_is_the_no_op_it_is_for_a_source_this_node_lacks() {
        let (dst, src) = led_here_and_elsewhere();
        let mut r = registry();

        // The replica this node holds of a room another node leads.
        let mut doc = Document::new(cid(1));
        doc.transact(|tx| tx.register(OPEN, Scalar::Int(1)));
        r.hub_mut()
            .import_room(&src, &doc.encode_state())
            .expect("the replica installs");
        assert!(r.hub().holds_room(&src), "this node holds the source");

        let alice = r.connect();
        assert!(r.deliver(
            alice,
            Message::Hello {
                client: cid(2),
                app_id: Vec::new(),
                schema_version: 0,
                codecs: Vec::new(),
            }
        ));
        assert!(r.deliver(
            alice,
            Message::Auth {
                credential: b"t-alice".to_vec(),
            }
        ));
        r.take_outbox(alice);

        assert!(r.deliver(
            alice,
            Message::CloneRoom {
                src: src.clone(),
                dst: dst.clone(),
            }
        ));
        let reply = r.take_outbox(alice).into_iter().next().expect("a reply");
        assert!(
            matches!(&reply, Message::CloneRoomResult { created, .. } if !*created),
            "a source this node does not lead clones nothing: {reply:?}",
        );
        assert!(
            !r.hub().holds_room(&dst),
            "and the destination was not minted from the replica",
        );
    }
}
