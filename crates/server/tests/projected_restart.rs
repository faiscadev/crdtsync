//! A partial reader that restarts and is caught up by a *projected* snapshot.
//!
//! Minting walks the ids a replica holds, and a full snapshot carries them in its
//! dedup set. A projected one — a zone-scoped subscriber's, or a partial reader's —
//! withholds a partition, so it cannot carry that partition's frontier: the ids would
//! name the withheld ops' existence and count. Scrubbing it whole also loses the
//! *recipient's* own ids, and a reader that persists its `ClientId` across a restart
//! then mints straight into ids the room's log already holds — every such write
//! deduped away at ingest, silently.
//!
//! So the projections keep the recipient's own ids and drop every other author's. The
//! caller here is what makes that possible: a projection cannot know who it serves,
//! and the session does — the channel-derived replica identity declared at Hello.
//!
//! Both catch-up seams are driven end to end through the in-process [`Registry`] (no
//! socket, no fs, so it runs under Miri): a zone-scoped subscriber whose snapshot
//! `project_zones` narrows, and a doc-ACL partial reader whose snapshot
//! `project_read_paths` narrows, the latter through a real `ClientSession`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::client::ClientSession;
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{AclEffect, ClientId, Document, Element, Message, Op, OpId, Scalar, Schema};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::{
    Action, ConnId, Identity, ManualClock, Registry, Resource, SchemaRegistry, StaticTokens,
};

const ROOM: &[u8] = b"room-r";
const APP: &[u8] = b"z";

/// Two zoned map subtrees (`/board` → za, `/notes` → zb) and one unzoned slot.
const ZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Sect", "notes": "Sect", "loose": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "zones": { "za": "/board", "zb": "/notes" }
}"#;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn tokens(rows: &[(&str, &str)]) -> StaticTokens {
    let mut t = StaticTokens::new();
    for (credential, actor) in rows {
        t.insert(credential.as_bytes().to_vec(), actor.as_bytes().to_vec());
    }
    t
}

/// The room's materialized replica, decoded — the oracle for "did the write land".
fn room_doc(r: &Registry) -> Document {
    Document::decode_state(&r.hub().export_room(ROOM).expect("the room exists"))
        .expect("the room's state decodes")
}

/// The Int behind `outer.inner`, or `None` when either level is absent.
fn nested(d: &Document, outer: &[u8], inner: &[u8]) -> Option<i64> {
    let Some(Element::Map(m)) = d.get(outer) else {
        return None;
    };
    let child = m.borrow().get(inner);
    match child {
        Some(Element::Register(r)) => match r.borrow().read() {
            Scalar::Int(n) => Some(*n),
            _ => panic!("expected an Int register"),
        },
        None => None,
        _ => panic!("expected a register or nothing"),
    }
}

/// The `Snapshot` state among a batch of reply frames — the projected bytes as served.
fn snapshot_in(replies: Vec<Message>) -> Option<Vec<u8>> {
    replies.into_iter().find_map(|m| match m {
        Message::Snapshot { state, .. } => Some(state),
        _ => None,
    })
}

// --- zone-scoped subscriber ---

/// Every actor may read and write the room; only the zone verdicts isolate. `author`
/// reaches both zones, `reader` only za — so its own writes land in za and zb stays
/// wholly hidden from it.
fn zone_authorizer(id: &Identity, _action: Action, res: &Resource) -> bool {
    match res {
        Resource::Zone { zone, .. } => {
            let zone: &[u8] = zone;
            match id.actor() {
                b"author" => true,
                b"reader" => zone == b"za",
                _ => false,
            }
        }
        _ => true,
    }
}

fn zone_registry() -> Registry {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, ZONED.as_bytes(), b"").unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens(&[
        ("c-author", "author"),
        ("c-reader", "reader"),
        ("c-reader2", "reader"),
        ("c-other", "author"),
    ])));
    r.set_authorizer(Box::new(zone_authorizer));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

/// Hello (enforcing `{APP, v1}`) + Auth as `credential`, without subscribing.
fn zone_auth(r: &mut Registry, client: u8, credential: &str) -> ConnId {
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
            credential: credential.as_bytes().to_vec(),
        }
    ));
    r.take_outbox(id);
    id
}

fn subscribe_zone(r: &mut Registry, id: ConnId, zone: &[u8]) -> Vec<Message> {
    r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            branch: Vec::new(),
            zone: zone.to_vec(),
            last_seen_seq: 0,
        },
    );
    r.take_outbox(id)
}

fn submit(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: Channel(0),
            ops
        }
    ));
}

/// The ops in a batch of reply frames, flattened.
fn ops_in(replies: Vec<Message>) -> Vec<Op> {
    replies
        .into_iter()
        .flat_map(|m| match m {
            Message::Ops { ops, .. } => ops,
            _ => Vec::new(),
        })
        .collect()
}

fn zoned_schema() -> Schema {
    Schema::parse(ZONED).expect("zoned schema parses")
}

/// A room bootstrapped by `author` with all three subtrees seeded, plus a za-scoped
/// reader connection whose replica has caught up. Returns the registry, the author's
/// doc + conn, and the reader's doc + conn.
fn zoned_room() -> (Registry, Document, ConnId, Document, ConnId) {
    let mut r = zone_registry();
    let author = zone_auth(&mut r, 1, "c-author");
    subscribe_zone(&mut r, author, b"");

    let mut author_doc = Document::new(cid(1));
    author_doc.set_schema(zoned_schema());
    let setup = author_doc.transact(|tx| {
        tx.map(b"board").register(b"bseed", Scalar::Int(0));
        tx.map(b"notes").register(b"nseed", Scalar::Int(0));
        tx.map(b"loose").register(b"lseed", Scalar::Int(0));
    });
    submit(&mut r, author, setup);
    r.take_outbox(author);

    // The reader joins za, folds its catch-up, and so can author into /board without
    // re-creating it.
    let reader = zone_auth(&mut r, 2, "c-reader");
    let mut reader_doc = Document::new(cid(2));
    reader_doc.set_schema(zoned_schema());
    for op in ops_in(subscribe_zone(&mut r, reader, b"za")) {
        reader_doc.apply(&op);
    }
    (r, author_doc, author, reader_doc, reader)
}

/// The reader's durable run: three writes of its own into the zone it may read.
fn reader_writes(r: &mut Registry, conn: ConnId, doc: &mut Document) -> Vec<OpId> {
    let mut ids = Vec::new();
    for i in 0..3 {
        let key = format!("r{i}").into_bytes();
        let ops = doc.transact(|tx| {
            tx.map(b"board").register(&key, Scalar::Int(i));
        });
        ids.extend(ops.iter().map(|op| op.id));
        submit(r, conn, ops);
    }
    r.take_outbox(conn);
    ids
}

#[test]
fn a_restarted_zone_scoped_reader_does_not_re_mint_across_a_projected_snapshot() {
    let (mut r, _author_doc, _author, mut reader_doc, reader) = zoned_room();
    let durable: HashSet<OpId> = reader_writes(&mut r, reader, &mut reader_doc)
        .into_iter()
        .collect();
    // Compact, so a join below the floor is served the materialized replica rather
    // than an op delta.
    r.hub_mut().compact(ROOM).expect("compact");

    // The restart: the `ClientId` persisted, the replica did not. A fresh connection
    // declares the same id and joins from zero, and is served a projected snapshot.
    let back = zone_auth(&mut r, 2, "c-reader2");
    let state = snapshot_in(subscribe_zone(&mut r, back, b"za"))
        .expect("a below-floor join is served a snapshot");
    let mut restarted =
        Document::decode_state_as(cid(2), 0, &state).expect("the projected snapshot decodes");
    assert!(
        restarted.get(b"notes").is_none(),
        "the projection still withholds zb",
    );

    let fresh = restarted.transact(|tx| {
        tx.map(b"board").register(b"after", Scalar::Int(9));
    });
    for op in &fresh {
        assert!(
            !durable.contains(&op.id),
            "re-minted an id the room's log already holds",
        );
    }

    // And the write is not merely distinct — it lands.
    submit(&mut r, back, fresh);
    assert_eq!(
        nested(&room_doc(&r), b"board", b"after"),
        Some(9),
        "the post-restart write was deduped away",
    );
}

#[test]
fn a_zone_scoped_readers_snapshot_names_no_other_replicas_ids() {
    // The privacy property the scrub exists for, at the server seam: the frontier the
    // reader is served names the reader and nobody else, so it can neither count nor
    // detect another replica's ops in the partition it may not read.
    let (mut r, mut author_doc, author, mut reader_doc, reader) = zoned_room();
    reader_writes(&mut r, reader, &mut reader_doc);
    for i in 0..4 {
        let key = format!("n{i}").into_bytes();
        let ops = author_doc.transact(|tx| {
            tx.map(b"notes").register(&key, Scalar::Int(i));
        });
        submit(&mut r, author, ops);
    }
    r.take_outbox(author);
    r.hub_mut().compact(ROOM).expect("compact");

    let back = zone_auth(&mut r, 2, "c-reader2");
    let state = snapshot_in(subscribe_zone(&mut r, back, b"za"))
        .expect("a below-floor join is served a snapshot");
    let projected = Document::decode_state(&state).expect("decodes");

    let authors: HashSet<ClientId> = projected.seen().map(|id| id.client).collect();
    assert_eq!(
        authors,
        HashSet::from([cid(2)]),
        "the projected frontier names an author other than the recipient",
    );
    assert!(projected.get(b"notes").is_none());
}

// --- doc-ACL partial reader, through a real session ---

/// The deployment permits alice (the creator) everything and abstains on bob, so
/// bob's read and write verdicts are the doc-ACL tier's alone.
fn acl_registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_verifier(Box::new(tokens(&[
        ("t-alice", "alice"),
        ("t-bob", "bob"),
        ("t-bob2", "bob"),
    ])));
    r.set_authorizer(Box::new(
        Acl::new()
            .allow(
                Subject::Actor(b"alice".to_vec()),
                Some(Action::Read),
                ResourceMatch::Room(ROOM.to_vec()),
            )
            .allow(
                Subject::Actor(b"alice".to_vec()),
                Some(Action::Write),
                ResourceMatch::Room(ROOM.to_vec()),
            ),
    ));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

fn alice_grant(doc: &mut Document, capability: Capability, path: &[u8]) -> Vec<Op> {
    doc.transact(|tx| {
        tx.acl().grant(
            AclSubject::Actor(actor_key(b"bob")),
            AclGrant::Capability(capability),
            AclEffect::Allow,
            path.to_vec(),
            actor_key(b"alice"),
        );
    })
}

/// A room alice has bootstrapped with `/a` and `/b`, granting bob read + write on
/// `/a` alone — so bob is a partial reader who authors into his own subtree.
fn acl_room() -> (Registry, Document, ConnId) {
    let mut r = acl_registry();
    let alice = r.connect();
    assert!(r.deliver(
        alice,
        Message::Hello {
            client: cid(1),
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
    assert!(r.deliver(
        alice,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq: 0,
        }
    ));
    r.take_outbox(alice);

    let mut doc = Document::new(cid(1));
    for ops in [
        doc.transact(|tx| {
            tx.map(b"a").register(b"seed", Scalar::Int(0));
            tx.map(b"b").register(b"seed", Scalar::Int(0));
        }),
        alice_grant(&mut doc, Capability::Read, &encode_path(&[b"a"])),
        // Write authority is a room-level verdict (there is no per-path write gate),
        // so bob's write grant roots at `/` — he may write anywhere but still reads
        // only `/a`, which is what makes his catch-up a projected one.
        alice_grant(&mut doc, Capability::Write, &encode_path(&[])),
    ] {
        submit(&mut r, alice, ops);
    }
    r.take_outbox(alice);
    (r, doc, alice)
}

/// Drive a fresh session's whole handshake against the registry, feeding every reply
/// back into it — the honest client half. Returns the session, its connection, and
/// the channel it holds.
fn bob_session(r: &mut Registry, credential: &[u8]) -> (ClientSession, ConnId, Channel) {
    let mut session = ClientSession::new(cid(2));
    let conn = r.connect();
    assert!(r.deliver(conn, session.hello()));
    assert!(r.deliver(conn, session.auth(credential)));
    let (channel, subscribe) = session.subscribe(ROOM);
    assert!(r.deliver(conn, subscribe));
    for reply in r.take_outbox(conn) {
        session
            .receive(reply)
            .expect("the session folds its replies");
    }
    (session, conn, channel)
}

/// The ops in the frame a session's edit produced.
fn edit_ops(session: &mut ClientSession, channel: Channel, key: &[u8], v: i64) -> Vec<Op> {
    let sent = session
        .edit(channel, |tx| {
            tx.map(b"a").register(key, Scalar::Int(v));
        })
        .expect("the channel is held");
    match sent {
        Message::Ops { ops, .. } => ops,
        other => panic!("expected an Ops frame, got {other:?}"),
    }
}

#[test]
fn a_restarted_partial_reader_does_not_re_mint_across_a_projected_snapshot() {
    let (mut r, _alice_doc, _alice) = acl_room();

    // bob joins, folds his redacted catch-up, and authors his own durable run into
    // the one subtree he may read and write.
    let (mut bob, conn, channel) = bob_session(&mut r, b"t-bob");
    let mut durable: HashSet<OpId> = HashSet::new();
    for i in 0..3 {
        let key = format!("v{i}").into_bytes();
        let ops = edit_ops(&mut bob, channel, &key, i);
        durable.extend(ops.iter().map(|op| op.id));
        submit(&mut r, conn, ops);
    }
    r.take_outbox(conn);
    r.hub_mut().compact(ROOM).expect("compact");

    // The restart: same persisted `ClientId`, a session rebuilt from nothing, caught
    // up by a snapshot projected to his readable subtree.
    let (mut back, conn2, channel2) = bob_session(&mut r, b"t-bob2");
    assert!(
        back.document(channel2)
            .expect("the channel is held")
            .get(b"b")
            .is_none(),
        "the projection still withholds /b",
    );

    let fresh = edit_ops(&mut back, channel2, b"after", 9);
    for op in &fresh {
        assert!(
            !durable.contains(&op.id),
            "re-minted an id the room's log already holds",
        );
    }

    submit(&mut r, conn2, fresh);
    assert_eq!(
        nested(&room_doc(&r), b"a", b"after"),
        Some(9),
        "the post-restart write was deduped away",
    );
    // The run it restarted onto is still there — the fresh write added to the room
    // rather than replacing what the reader had already published.
    assert_eq!(nested(&room_doc(&r), b"a", b"v2"), Some(2));
}

#[test]
fn a_partial_readers_snapshot_names_no_other_replicas_ids() {
    let (mut r, mut alice_doc, alice) = acl_room();
    let (mut bob, conn, channel) = bob_session(&mut r, b"t-bob");
    for i in 0..3 {
        let key = format!("v{i}").into_bytes();
        let ops = edit_ops(&mut bob, channel, &key, i);
        submit(&mut r, conn, ops);
    }
    // alice keeps writing into the subtree bob cannot read.
    for i in 0..4 {
        let ops = alice_doc.transact(|tx| {
            tx.map(b"b")
                .register(&format!("x{i}").into_bytes(), Scalar::Int(i));
        });
        submit(&mut r, alice, ops);
    }
    r.take_outbox(alice);
    r.take_outbox(conn);
    r.hub_mut().compact(ROOM).expect("compact");

    let back = r.connect();
    let mut probe = ClientSession::new(cid(2));
    assert!(r.deliver(back, probe.hello()));
    assert!(r.deliver(back, probe.auth(b"t-bob2")));
    let (_, subscribe) = probe.subscribe(ROOM);
    assert!(r.deliver(back, subscribe));
    let state = snapshot_in(r.take_outbox(back)).expect("a below-floor join is served a snapshot");
    let projected = Document::decode_state(&state).expect("decodes");

    let authors: HashSet<ClientId> = projected.seen().map(|id| id.client).collect();
    assert_eq!(
        authors,
        HashSet::from([cid(2)]),
        "the projected frontier names an author other than the recipient",
    );
    assert!(projected.get(b"b").is_none());
}
