//! A partial reader that restarts and is caught up by a *redacted op delta*.
//!
//! C9 closed the snapshot half: a projected snapshot names the recipient's own ids,
//! so a reader that persists its `ClientId` never adopts a state naming none of its
//! own authorship. An **uncompacted** room serves an op delta instead, and the
//! per-op read filter is authorship-blind — it withholds the recipient's *own* ops
//! on paths it may no longer read, leaving a hole in its own run. Minting walks the
//! ids a replica holds, so the next write lands in the hole, onto an id the room's
//! log already binds, and dedups away at ingest with nothing able to detect the loss.
//!
//! The state encoding carried the ids in the snapshot case; an `Ops` frame carries
//! none. So a redacted catch-up delta is led by [`Message::Frontier`], naming the
//! per-client sequences the delta withholds and nothing else — not the ops, whose
//! targets and content name the structure the redaction exists to withhold.
//!
//! The other half of the unit is the seam that is *not* given the carrier. A live
//! fan-out redacts the same way and is given no frame, on one invariant: **a replica
//! applies its own op at authoring time**, so the filter withholding its own echo
//! leaves nothing missing. That is a property of one replica per identity, not of the
//! frame — two live connections declaring one `ClientId` break it, and nothing refuses
//! them within a single actor (C23 binds a replica identity across actors; C96 holds
//! the ruling on the rest). Where it holds, the live seam needs nothing; where it
//! fails, the hole is identity sharing's and not this frame's.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::client::ClientSession;
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{AclEffect, ClientId, Document, Element, Message, Op, OpId, Scalar, Schema};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::{
    Action, ConnId, ManualClock, Registry, Resource, SchemaRegistry, StaticTokens,
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

fn submit(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: Channel(0),
            ops
        }
    ));
}

/// The sequences a batch of reply frames names as withheld, or `None` when no
/// frontier frame rode with them.
fn frontier_in(replies: &[Message]) -> Option<Vec<u64>> {
    replies.iter().find_map(|m| match m {
        Message::Frontier { seqs, .. } => Some(seqs.clone()),
        _ => None,
    })
}

/// The id-space position a frontier frame reports, or `None` when none rode.
fn reach_in(replies: &[Message]) -> Option<u64> {
    replies.iter().find_map(|m| match m {
        Message::Frontier { reach, .. } => Some(*reach),
        _ => None,
    })
}

/// The ops in a batch of reply frames, flattened.
fn ops_in(replies: &[Message]) -> Vec<Op> {
    replies
        .iter()
        .flat_map(|m| match m {
            Message::Ops { ops, .. } => ops.clone(),
            _ => Vec::new(),
        })
        .collect()
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

/// A room alice has bootstrapped with `/a` and `/b`, granting bob read on `/a` and
/// write at `/` — so bob writes into `/b`, a subtree he may never read back. The
/// room is left uncompacted, so every catch-up over it is an op delta.
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
        // only `/a`, which is what makes his catch-up a redacted one.
        alice_grant(&mut doc, Capability::Write, &encode_path(&[])),
    ] {
        submit(&mut r, alice, ops);
    }
    r.take_outbox(alice);
    (r, doc, alice)
}

/// Drive a fresh session's whole handshake against the registry, feeding every reply
/// back into it — the honest client half. Returns the session, its connection, the
/// channel it holds, and the reply frames its catch-up arrived in.
fn bob_session(
    r: &mut Registry,
    credential: &[u8],
) -> (ClientSession, ConnId, Channel, Vec<Message>) {
    let mut session = ClientSession::new(cid(2));
    let conn = r.connect();
    assert!(r.deliver(conn, session.hello()));
    assert!(r.deliver(conn, session.auth(credential)));
    let (channel, subscribe) = session.subscribe(ROOM).unwrap();
    assert!(r.deliver(conn, subscribe));
    let replies = r.take_outbox(conn);
    for reply in replies.clone() {
        session
            .receive(reply)
            .expect("the session folds its replies");
    }
    (session, conn, channel, replies)
}

/// Write `key` into the subtree `outer`, returning the ops the frame carried.
fn edit_ops(
    session: &mut ClientSession,
    channel: Channel,
    outer: &[u8],
    key: &[u8],
    v: i64,
) -> Vec<Op> {
    let sent = session
        .edit(channel, |tx| {
            tx.map(outer).register(key, Scalar::Int(v));
        })
        .expect("the channel is held");
    match sent {
        Message::Ops { ops, .. } => ops,
        other => panic!("expected an Ops frame, got {other:?}"),
    }
}

/// bob's durable run: two writes into `/b` (which he may write and never read),
/// then one into `/a`. Returns his ids in the order the room's log holds them, split
/// by whether a delta redacted for him may carry them.
fn bobs_durable_run(r: &mut Registry) -> (Vec<OpId>, Vec<OpId>) {
    let (mut bob, conn, channel, _) = bob_session(r, b"t-bob");
    let mut hidden = Vec::new();
    let mut shown = Vec::new();
    for (outer, key, v) in [
        (&b"b"[..], &b"hidden0"[..], 0i64),
        (&b"b"[..], &b"hidden1"[..], 1),
        (&b"a"[..], &b"shown"[..], 2),
    ] {
        let ops = edit_ops(&mut bob, channel, outer, key, v);
        let bucket = if outer == b"b" {
            &mut hidden
        } else {
            &mut shown
        };
        bucket.extend(ops.iter().map(|op| op.id));
        submit(r, conn, ops);
    }
    r.take_outbox(conn);
    (hidden, shown)
}

#[test]
fn a_restarted_partial_reader_does_not_re_mint_across_a_redacted_delta() {
    // The filing's measured case: seqs 0 and 1 into `/b`, seq 2 into `/a`, then a
    // restart onto an op delta that withholds the first two.
    let (mut r, _alice_doc, _alice) = acl_room();
    let (hidden, shown) = bobs_durable_run(&mut r);

    let (mut back, conn2, channel2, _) = bob_session(&mut r, b"t-bob2");
    assert!(
        back.document(channel2)
            .expect("the channel is held")
            .get(b"b")
            .is_none(),
        "the delta still withholds /b",
    );

    let fresh = edit_ops(&mut back, channel2, b"a", b"after", 9);
    for op in &fresh {
        assert!(
            !hidden.contains(&op.id) && !shown.contains(&op.id),
            "re-minted an id the room's log already holds: {:?}",
            op.id,
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
    assert_eq!(nested(&room_doc(&r), b"a", b"shown"), Some(2));
    assert_eq!(nested(&room_doc(&r), b"b", b"hidden1"), Some(1));
}

#[test]
fn a_frontier_names_the_withheld_run_and_the_delta_still_withholds_its_ops() {
    // The carrier hands back sequences, never the ops behind them: the subtree bob
    // wrote into stays as absent from his replica as it was before the fix, which is
    // the reason the seam could not simply stop scrubbing.
    let (mut r, _alice_doc, _alice) = acl_room();
    let (hidden, shown) = bobs_durable_run(&mut r);

    let (back, _conn2, channel2, replies) = bob_session(&mut r, b"t-bob2");
    let named = frontier_in(&replies).expect("a redacted delta is led by its frontier");
    // Named in the order the stream holds them, so the frame is a function of the log
    // and not of a set's iteration.
    let expected: Vec<u64> = hidden.iter().map(|id| id.seq).collect();
    assert_eq!(named, expected, "the frontier names the wrong run");

    // The ops themselves are still gone — and so is every trace of them in the
    // reader's replica.
    let served: HashSet<OpId> = ops_in(&replies).iter().map(|op| op.id).collect();
    assert!(
        hidden.iter().all(|id| !served.contains(id)),
        "an op the read filter withheld rode the delta anyway",
    );
    assert!(
        shown.iter().all(|id| served.contains(id)),
        "the readable half of the run was withheld too",
    );
    let doc = back.document(channel2).expect("the channel is held");
    assert!(doc.get(b"b").is_none(), "the withheld subtree materialised");
    assert_eq!(
        doc.next_seq(),
        (hidden.len() + shown.len()) as u64,
        "the run still has a hole in it",
    );
}

#[test]
fn a_frontier_names_no_other_replicas_run() {
    // The scrub's whole point: what a redaction hands back is the recipient's own
    // authorship. alice's writes into the withheld subtree are neither carried nor
    // counted, so the frame's length does not report how busy `/b` has been.
    let (mut r, mut alice_doc, alice) = acl_room();
    let (hidden, _shown) = bobs_durable_run(&mut r);
    for i in 0..5 {
        let ops = alice_doc.transact(|tx| {
            tx.map(b"b")
                .register(&format!("x{i}").into_bytes(), Scalar::Int(i));
        });
        submit(&mut r, alice, ops);
    }
    r.take_outbox(alice);

    let (_back, _conn2, _channel2, replies) = bob_session(&mut r, b"t-bob2");
    let named = frontier_in(&replies).expect("a redacted delta is led by its frontier");
    assert_eq!(
        named.len(),
        hidden.len(),
        "the frontier counted an author other than the recipient",
    );
}

#[test]
fn a_frontier_leads_the_delta_it_answers_for() {
    // A delivery truncated between the two frames must cost a skipped sequence, not
    // a lost write — so the frontier comes first.
    let (mut r, _alice_doc, _alice) = acl_room();
    let _ = bobs_durable_run(&mut r);

    let (_back, _conn2, _channel2, replies) = bob_session(&mut r, b"t-bob2");
    let frontier = replies
        .iter()
        .position(|m| matches!(m, Message::Frontier { .. }))
        .expect("a redacted delta is led by its frontier");
    let ops = replies
        .iter()
        .position(|m| matches!(m, Message::Ops { .. }))
        .expect("the delta rides too");
    assert!(frontier < ops, "the delta arrived ahead of its frontier");
}

#[test]
fn an_unredacted_catch_up_sends_no_frontier() {
    // alice reads the whole document, so nothing of hers is withheld and there is no
    // hole to name. A frame here would be pure noise on every ordinary join.
    let (mut r, _alice_doc, _alice) = acl_room();
    let _ = bobs_durable_run(&mut r);

    let conn = r.connect();
    assert!(r.deliver(
        conn,
        Message::Hello {
            client: cid(3),
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
        }
    ));
    assert!(r.deliver(
        conn,
        Message::Auth {
            credential: b"t-alice".to_vec(),
        }
    ));
    assert!(r.deliver(
        conn,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq: 0,
        }
    ));
    let replies = r.take_outbox(conn);
    assert!(!ops_in(&replies).is_empty(), "the join was served no delta");
    assert_eq!(
        frontier_in(&replies),
        None,
        "an unredacted catch-up carried a frontier",
    );
}

#[test]
fn a_redacted_delta_carrying_none_of_the_recipients_own_ops_sends_no_frontier() {
    // A partial reader that has published nothing is redacted just as hard, and has
    // no run to repair. The frame answers for the recipient's own authorship alone,
    // so it is absent here even though the delta is narrowed.
    let (mut r, _alice_doc, _alice) = acl_room();
    let (_bob, _conn, _channel, replies) = bob_session(&mut r, b"t-bob");

    let served = ops_in(&replies);
    assert!(!served.is_empty(), "the join was served no delta");
    assert!(
        served.len() < r.hub().seq(ROOM) as usize,
        "the delta was not narrowed at all, so this measures nothing",
    );
    assert_eq!(
        frontier_in(&replies),
        None,
        "a reader with no run of its own was sent a frontier",
    );
}

#[test]
fn a_restarted_reader_does_not_re_mint_on_a_second_channel() {
    // A channel authors under `for_channel` of the id declared at Hello, so the
    // frontier has to be cut to *that* identity — the connection's own id answers
    // only for channel 0. A session's second subscription is where a mistake hides.
    let (mut r, _alice_doc, _alice) = acl_room();

    let mut bob = ClientSession::new(cid(2));
    let conn = r.connect();
    assert!(r.deliver(conn, bob.hello()));
    assert!(r.deliver(conn, bob.auth(b"t-bob")));
    for _ in 0..2 {
        let (_, subscribe) = bob.subscribe(ROOM).unwrap();
        assert!(r.deliver(conn, subscribe));
    }
    for reply in r.take_outbox(conn) {
        bob.receive(reply).expect("the session folds its replies");
    }
    let second = Channel(1);

    let mut durable: HashSet<OpId> = HashSet::new();
    for (outer, key, v) in [
        (&b"b"[..], &b"hidden0"[..], 0i64),
        (&b"b"[..], &b"hidden1"[..], 1),
        (&b"a"[..], &b"shown"[..], 2),
    ] {
        let ops = edit_ops(&mut bob, second, outer, key, v);
        durable.extend(ops.iter().map(|op| op.id));
        assert!(r.deliver(
            conn,
            Message::Ops {
                channel: second,
                ops
            }
        ));
    }
    r.take_outbox(conn);

    // The restart, subscribing in the same order so the second channel is the same
    // replica identity as before.
    let mut back = ClientSession::new(cid(2));
    let conn2 = r.connect();
    assert!(r.deliver(conn2, back.hello()));
    assert!(r.deliver(conn2, back.auth(b"t-bob2")));
    for _ in 0..2 {
        let (_, subscribe) = back.subscribe(ROOM).unwrap();
        assert!(r.deliver(conn2, subscribe));
    }
    for reply in r.take_outbox(conn2) {
        back.receive(reply).expect("the session folds its replies");
    }

    let fresh = edit_ops(&mut back, second, b"a", b"after", 9);
    for op in &fresh {
        assert!(
            !durable.contains(&op.id),
            "re-minted an id the room's log already holds",
        );
    }
    assert!(r.deliver(
        conn2,
        Message::Ops {
            channel: second,
            ops: fresh
        }
    ));
    assert_eq!(
        nested(&room_doc(&r), b"a", b"after"),
        Some(9),
        "the post-restart write was deduped away",
    );
}

#[test]
fn a_client_sent_frontier_is_a_protocol_violation() {
    // The frame is the server's own account of what it withheld. A client that sends
    // one is claiming sequences into a replica's id space, which would push that
    // replica's mint past ids no room's log binds.
    let (mut r, _alice_doc, _alice) = acl_room();
    let (_bob, conn, channel, _) = bob_session(&mut r, b"t-bob");
    assert!(
        !r.deliver(
            conn,
            Message::Frontier {
                channel,
                seqs: vec![0, 1, 2],
                reach: 0,
            }
        ),
        "a client drove a frontier into the server",
    );
}

// --- the seam that is deliberately not given the carrier ---

#[test]
fn a_live_redacted_fan_out_needs_no_frontier() {
    // The catch-up seam gets the frame and the live seam does not, so the two are
    // measured against one another here: the same reader, redacted the same way,
    // live and then restarted. An implementation that emitted the frame at the
    // fan-out site fails the first half; one that emitted it nowhere fails the
    // second. In between is the invariant — a session that stays up folded its own
    // op locally before the frame was built, so the live filter withholding it
    // leaves no hole — measured by running the whole exchange out and reading the
    // position back.
    let (mut r, mut alice_doc, alice) = acl_room();
    let (mut bob, conn, channel, joined) = bob_session(&mut r, b"t-bob");
    assert_eq!(frontier_in(&joined), None, "an empty room named a run");

    for (outer, key, v) in [
        (&b"b"[..], &b"hidden0"[..], 0i64),
        (&b"b"[..], &b"hidden1"[..], 1),
        (&b"a"[..], &b"shown"[..], 2),
    ] {
        let ops = edit_ops(&mut bob, channel, outer, key, v);
        submit(&mut r, conn, ops);
    }
    // alice writes into both subtrees, so bob's live stream is redacted on the way
    // in as well as on the way out.
    for (outer, i) in [(&b"a"[..], 0i64), (&b"b"[..], 1)] {
        let ops = alice_doc.transact(|tx| {
            tx.map(outer)
                .register(&format!("alice{i}").into_bytes(), Scalar::Int(i));
        });
        submit(&mut r, alice, ops);
    }
    r.take_outbox(alice);

    let live = r.take_outbox(conn);
    assert_eq!(
        frontier_in(&live),
        None,
        "the live fan-out sent a frontier it does not need",
    );
    for reply in live {
        bob.receive(reply).expect("the session folds its replies");
    }
    let doc = bob.document(channel).expect("the channel is held");
    assert_eq!(
        doc.next_seq(),
        4,
        "a session that stayed up lost its own run to the live filter",
    );
    assert!(
        doc.get(b"b").is_some(),
        "an author holds what it wrote, whatever the filter withholds from it",
    );

    let fresh = edit_ops(&mut bob, channel, b"a", b"after", 9);
    submit(&mut r, conn, fresh);
    assert_eq!(nested(&room_doc(&r), b"a", b"after"), Some(9));

    // The other half of the contrast: the same reader restarted onto the same
    // redaction *is* sent one, so "no frame" is this seam's answer and not the
    // implementation's answer everywhere.
    let (_back, _conn2, _channel2, rejoin) = bob_session(&mut r, b"t-bob2");
    assert!(
        frontier_in(&rejoin).is_some(),
        "the catch-up seam served no frame either, so this measures nothing",
    );
}

// --- the zone seam, which redacts on a different dimension ---

fn zone_authorizer(id: &crdtsync_server::Identity, action: Action, res: &Resource) -> bool {
    match res {
        Resource::Zone { zone, .. } => {
            let zone: &[u8] = zone;
            match id.actor() {
                b"author" => true,
                // The reader writes into `zb` and may never read it back — the zone
                // dimension's version of a write-only subtree.
                b"reader" => zone == b"za" || action == Action::Write,
                _ => false,
            }
        }
        _ => true,
    }
}

fn zoned_schema() -> Schema {
    Schema::parse(ZONED).expect("zoned schema parses")
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
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            branch: Vec::new(),
            zone: zone.to_vec(),
            last_seen_seq: 0,
        },
    ));
    r.take_outbox(id)
}

/// Fold a catch-up into `doc` the way a session does: the frontier's sequences
/// first, then the delta's ops, in the order the frames arrived.
fn fold_catch_up(doc: &mut Document, replies: &[Message]) {
    for reply in replies {
        match reply {
            Message::Frontier { seqs, reach, .. } => doc.note_published(seqs, *reach),
            Message::Ops { ops, .. } => {
                for op in ops {
                    doc.apply(op);
                }
            }
            _ => {}
        }
    }
}

#[test]
fn a_restarted_zone_scoped_reader_does_not_re_mint_across_a_redacted_delta() {
    // The zone filter drops ops on a second dimension, after the read filter and
    // with the same blindness to authorship. The withheld run is measured on the
    // frame the recipient is actually served, so it answers for both.
    let mut r = zone_registry();
    let author = zone_auth(&mut r, 1, "c-author");
    subscribe_zone(&mut r, author, b"");
    let mut author_doc = Document::new(cid(1));
    author_doc.set_schema(zoned_schema());
    submit(
        &mut r,
        author,
        author_doc.transact(|tx| {
            tx.map(b"board").register(b"bseed", Scalar::Int(0));
            tx.map(b"notes").register(b"nseed", Scalar::Int(0));
        }),
    );
    r.take_outbox(author);

    // The reader joins za, so its replica holds /board and never /notes — and it
    // writes into both, since its write verdict spans every zone.
    let reader = zone_auth(&mut r, 2, "c-reader");
    let mut reader_doc = Document::new(cid(2));
    reader_doc.set_schema(zoned_schema());
    fold_catch_up(&mut reader_doc, &subscribe_zone(&mut r, reader, b"za"));

    let mut durable: HashSet<OpId> = HashSet::new();
    for (outer, key, v) in [
        (&b"notes"[..], &b"hidden0"[..], 0i64),
        (&b"notes"[..], &b"hidden1"[..], 1),
        (&b"board"[..], &b"shown"[..], 2),
    ] {
        let ops = reader_doc.transact(|tx| {
            tx.map(outer).register(key, Scalar::Int(v));
        });
        durable.extend(ops.iter().map(|op| op.id));
        submit(&mut r, reader, ops);
    }
    r.take_outbox(reader);
    assert_eq!(
        nested(&room_doc(&r), b"notes", b"hidden1"),
        Some(1),
        "the writes into the unreadable zone never landed, so this measures nothing",
    );

    // The restart: the `ClientId` persisted, the replica did not. The room is
    // uncompacted, so the catch-up is an op delta.
    let back = zone_auth(&mut r, 2, "c-reader2");
    let replies = subscribe_zone(&mut r, back, b"za");
    let mut restarted = Document::new(cid(2));
    restarted.set_schema(zoned_schema());
    fold_catch_up(&mut restarted, &replies);
    assert!(
        restarted.get(b"notes").is_none(),
        "the delta still withholds zb",
    );
    assert!(
        frontier_in(&replies).is_some(),
        "the zone-redacted delta named none of the run it withheld",
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
    submit(&mut r, back, fresh);
    assert_eq!(
        nested(&room_doc(&r), b"board", b"after"),
        Some(9),
        "the post-restart write was deduped away",
    );
}

// --- the id-space half of the hole ---

#[test]
fn a_restarted_partial_reader_mints_above_the_stamps_its_delta_withheld() {
    // A mint reads two records and the redaction holes both. The sequence half is
    // above; this is the other — every id taken from a stamp alone (an ACL tuple's,
    // a ranged element's, an XML sequence child's) re-derives from a position the
    // withheld ops already occupy, so the element it names is swallowed at ingest
    // exactly as a re-minted sequence is.
    let (mut r, _alice_doc, _alice) = acl_room();
    let (mut bob, conn, channel, _) = bob_session(&mut r, b"t-bob");

    // The readable write first, so the withheld ones are the run's high water.
    let mut withheld_reach = 0;
    for (outer, key, v) in [
        (&b"a"[..], &b"shown"[..], 0i64),
        (&b"b"[..], &b"hidden0"[..], 1),
        (&b"b"[..], &b"hidden1"[..], 2),
    ] {
        let ops = edit_ops(&mut bob, channel, outer, key, v);
        if outer == b"b" {
            withheld_reach = withheld_reach.max(
                ops.iter()
                    .map(|op| op.reservation_end())
                    .max()
                    .expect("a batch"),
            );
        }
        submit(&mut r, conn, ops);
    }
    r.take_outbox(conn);

    let (mut back, _conn2, channel2, replies) = bob_session(&mut r, b"t-bob2");
    assert_eq!(
        reach_in(&replies),
        Some(withheld_reach),
        "the frame reported a position other than the one its withheld run reaches",
    );

    let fresh = edit_ops(&mut back, channel2, b"a", b"after", 9);
    assert!(
        fresh.iter().all(|op| op.stamp.lamport > withheld_reach),
        "minted onto a position the room's log already holds",
    );
}

// --- a named run is not the ops' grave ---

#[test]
fn a_reader_that_regains_read_still_gets_its_own_withheld_ops() {
    // Naming the run must not put the ops in the dedup set. The room's log holds
    // them, and the client's cursor advances by the *delivered* batch length, so a
    // reader whose run ends in the withheld subtree resumes from below its own last
    // ops — and a widened grant re-serves exactly them. If the name doubled as a
    // refusal, the recipient would be the one author whose content it never got
    // back, while every other author's folded normally.
    let (mut r, mut alice_doc, alice) = acl_room();
    let (mut bob, conn, channel, _) = bob_session(&mut r, b"t-bob");
    for (outer, key, v) in [
        (&b"a"[..], &b"shown"[..], 0i64),
        (&b"b"[..], &b"hidden0"[..], 10),
        (&b"b"[..], &b"hidden1"[..], 11),
    ] {
        let ops = edit_ops(&mut bob, channel, outer, key, v);
        submit(&mut r, conn, ops);
    }
    r.take_outbox(conn);
    // alice writes into the same subtree — the control, whose ids the frame never
    // names.
    submit(
        &mut r,
        alice,
        alice_doc.transact(|tx| {
            tx.map(b"b").register(b"alice", Scalar::Int(7));
        }),
    );
    r.take_outbox(alice);

    let (mut back, conn2, channel2, replies) = bob_session(&mut r, b"t-bob2");
    assert!(
        frontier_in(&replies).is_some(),
        "the restart was not redacted, so this measures nothing",
    );

    // alice opens the subtree, and bob reconnects: a session outlives its
    // connections, so the resume rides a fresh one and asks from the cursor the
    // redacted catch-up left it on.
    submit(
        &mut r,
        alice,
        alice_grant(&mut alice_doc, Capability::Read, &encode_path(&[b"b"])),
    );
    r.take_outbox(alice);
    r.take_outbox(conn2);
    let conn3 = r.connect();
    assert!(r.deliver(conn3, back.hello()));
    assert!(r.deliver(conn3, back.auth(b"t-bob")));
    let resume = back.resume(channel2).expect("the channel is held");
    assert!(r.deliver(conn3, resume));
    for reply in r.take_outbox(conn3) {
        back.receive(reply).expect("the session folds its replies");
    }

    let doc = back.document(channel2).expect("the channel is held");
    assert_eq!(
        nested(doc, b"b", b"alice"),
        Some(7),
        "an author the frame never named did not fold on the re-serve",
    );
    assert_eq!(
        nested(doc, b"b", b"hidden0"),
        Some(10),
        "the reader's own withheld op was dropped as a replay",
    );
    assert_eq!(
        nested(doc, b"b", b"hidden1"),
        Some(11),
        "the reader's own withheld op was dropped as a replay",
    );
}
