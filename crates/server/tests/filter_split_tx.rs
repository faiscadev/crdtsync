//! A per-op filter must not strand an atomic group's survivors (C11).
//!
//! Four per-recipient seams withhold individual ops from a batch: the catch-up
//! delta's read-ACL filter and its zone filter, the live fan-out's per-recipient
//! read filter, and the per-channel zone filter the live fan-out ends on. Dropping
//! one member of an atomic transaction while leaving the rest tagged hands the
//! recipient a group whose `count` its bucket can never reach — the survivors are
//! buffered against a member that will never arrive, so they are invisible to that
//! reader forever and no later traffic rescues them.
//!
//! Every seam therefore destrands: a survivor of a split group rides untagged and
//! merges standalone, exactly as the migration translation seam has always done.
//! The atomic *view* is lost at such a recipient — unavoidably, since it cannot see
//! the withheld member — but the ops still merge, so state converges. A group a
//! filter carries whole keeps its tags and stays atomic.
//!
//! Both harnesses drive the whole path in-process through the multi-subscriber
//! [`Registry`] (no socket, no fs), so the suite runs under Miri. The doc-ACL half
//! uses a deployment authorizer that abstains on reads so the per-path redaction
//! actually bites; the zones half uses a schema declaring two zoned subtrees.

use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{
    AclEffect, ClientId, Document, Element, Message, Op, OpKind, Scalar, Schema, Tx, TxId,
};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::{
    Action, ConnId, Identity, ManualClock, Registry, Resource, SchemaRegistry, StaticTokens,
};

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

/// Hello + Auth as `credential` on `app` at `version`, without subscribing.
fn auth(r: &mut Registry, client: u8, credential: &str, app: &[u8], version: u32) -> ConnId {
    let id = r.connect();
    assert!(r.deliver(
        id,
        Message::Hello {
            client: cid(client),
            app_id: app.to_vec(),
            schema_version: version,
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

fn subscribe(r: &mut Registry, id: ConnId, room: &[u8], zone: &[u8]) -> bool {
    r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: room.to_vec(),
            zone: zone.to_vec(),
            last_seen_seq: 0,
            branch: Vec::new(),
        },
    )
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

/// The ops delivered to `id`, flattened across every `Ops` frame in its outbox.
fn received_ops(r: &mut Registry, id: ConnId) -> Vec<Op> {
    r.take_outbox(id)
        .into_iter()
        .flat_map(|m| match m {
            Message::Ops { ops, .. } => ops,
            _ => Vec::new(),
        })
        .collect()
}

/// A fresh replica that folded `ops` in delivery order — what the recipient's own
/// document makes of what the seam handed it.
fn folded(client: u8, ops: &[Op]) -> Document {
    let mut d = Document::new(cid(client));
    for op in ops {
        d.apply(op);
    }
    d
}

/// The `Int` in a top-level subtree's `v` slot, or `None` when the write never
/// became visible (withheld, or stranded in the buffer).
fn nested_int(d: &Document, key: &[u8]) -> Option<i64> {
    let map = match d.get(key)? {
        Element::Map(m) => m,
        _ => return None,
    };
    let inner = map.borrow().get(b"v")?;
    match inner {
        Element::Register(r) => match r.borrow().read() {
            Scalar::Int(i) => Some(*i),
            _ => None,
        },
        _ => None,
    }
}

/// The `Int` in a top-level register slot.
fn top_int(d: &Document, key: &[u8]) -> Option<i64> {
    match d.get(key)? {
        Element::Register(r) => match r.borrow().read() {
            Scalar::Int(i) => Some(*i),
            _ => None,
        },
        _ => None,
    }
}

/// Whether every op in `ops` carries a transaction tag.
fn all_tagged(ops: &[Op]) -> bool {
    !ops.is_empty() && ops.iter().all(|op| op.tx.is_some())
}

/// Re-tag `ops` as one group spanning every zone partition they fall in — the
/// envelopes a peer that does not cut its commits to partitions (C2) puts on the
/// wire. A local commit mints no such group, but the wire admits one, so every filter
/// has to destrand what it splits.
fn as_one_group(ops: Vec<Op>) -> Vec<Op> {
    let count = u32::try_from(ops.len()).expect("a small group");
    let id = TxId::derive(ops.iter().map(|op| op.id.seq));
    ops.into_iter()
        .map(|mut op| {
            op.tx = Some(Tx { id, count });
            op
        })
        .collect()
}

// --- doc-ACL harness -------------------------------------------------------

const ROOM: &[u8] = b"room-a";

/// A registry whose deployment permits `alice` (the creator) to read + write but
/// abstains on every other actor's read, so bob's read verdicts are the doc-ACL
/// tier's alone and the per-path redaction bites. A fixed clock keeps it Miri-clean.
fn acl_registry() -> Registry {
    let mut r = Registry::new(cid(0xFF));
    r.set_verifier(Box::new(tokens(&[
        ("t-alice", "alice"),
        ("t-alice2", "alice"),
        ("t-bob", "bob"),
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

/// alice grants `subject` read at `path`, authored by alice (the creator).
fn grant_read(doc: &mut Document, subject: AclSubject, path: &[u8]) -> Vec<Op> {
    doc.transact(|tx| {
        tx.acl().grant(
            subject,
            AclGrant::Capability(Capability::Read),
            AclEffect::Allow,
            path.to_vec(),
            actor_key(b"alice"),
        );
    })
}

/// A registry where alice has bootstrapped the room and granted bob read on `/a`
/// alone. Returns it plus alice's authoring doc and connection.
fn acl_seeded() -> (Registry, Document, ConnId) {
    let mut r = acl_registry();
    let alice = auth(&mut r, 1, "t-alice", b"", 0);
    assert!(subscribe(&mut r, alice, ROOM, b""));
    r.take_outbox(alice);
    let mut doc = Document::new(cid(1));
    submit(
        &mut r,
        alice,
        doc.transact(|tx| {
            tx.map(b"seed").register(b"v", Scalar::Int(0));
        }),
    );
    submit(
        &mut r,
        alice,
        grant_read(
            &mut doc,
            AclSubject::Actor(actor_key(b"bob")),
            &encode_path(&[b"a"]),
        ),
    );
    r.take_outbox(alice);
    (r, doc, alice)
}

/// One atomic transaction spanning `/a` (bob reads it) and `/b` (he does not): four
/// members, of which the read filter admits bob exactly two.
fn atomic_across_subtrees(doc: &mut Document) -> Vec<Op> {
    doc.atomic_transact(|tx| {
        tx.map(b"a").register(b"v", Scalar::Int(1));
        tx.map(b"b").register(b"v", Scalar::Int(2));
    })
}

/// One atomic transaction wholly inside `/a` — nothing the read filter withholds
/// from bob.
fn atomic_inside_a(doc: &mut Document) -> Vec<Op> {
    doc.atomic_transact(|tx| {
        tx.map(b"a").register(b"v", Scalar::Int(7));
        tx.map(b"a").register(b"w", Scalar::Int(8));
    })
}

/// Hello + Auth + Subscribe bob-style on the doc-ACL room, returning the connection
/// and the catch-up ops it was served — a reader folds those before the live stream,
/// as a real client does.
fn acl_join(r: &mut Registry, client: u8, credential: &str) -> (ConnId, Vec<Op>) {
    let id = auth(r, client, credential, b"", 0);
    assert!(subscribe(r, id, ROOM, b""), "{credential} subscribes");
    let catch_up = received_ops(r, id);
    (id, catch_up)
}

/// The members of `sent` that reached this recipient.
fn members_of<'a>(sent: &[Op], got: &'a [Op]) -> Vec<&'a Op> {
    got.iter()
        .filter(|op| sent.iter().any(|s| s.id == op.id))
        .collect()
}

#[test]
fn the_live_read_filter_does_not_strand_a_split_groups_survivors() {
    let (mut r, mut alice_doc, alice) = acl_seeded();
    let (bob, base) = acl_join(&mut r, 2, "t-bob");

    let sent = atomic_across_subtrees(&mut alice_doc);
    submit(&mut r, alice, sent.clone());
    let got = received_ops(&mut r, bob);
    let members = members_of(&sent, &got);
    assert_eq!(members.len(), 2, "bob receives the /a half of the group");
    assert!(
        members.iter().all(|op| op.tx.is_none()),
        "a survivor of a split group rides untagged"
    );

    let mut bob_doc = folded(2, &base);
    for op in &got {
        bob_doc.apply(op);
    }
    assert_eq!(
        nested_int(&bob_doc, b"a"),
        Some(1),
        "the survivors applied instead of stranding in the buffer"
    );
    assert_eq!(
        nested_int(&bob_doc, b"b"),
        None,
        "the /b half stayed withheld"
    );

    // The stranding was permanent, so arbitrary later traffic is the check that it
    // is gone: the survivors are already applied, and later writes fold on top.
    submit(
        &mut r,
        alice,
        alice_doc.transact(|tx| {
            tx.map(b"a").register(b"v", Scalar::Int(9));
        }),
    );
    for op in received_ops(&mut r, bob) {
        bob_doc.apply(&op);
    }
    assert_eq!(nested_int(&bob_doc, b"a"), Some(9));
}

#[test]
fn the_catch_up_read_filter_does_not_strand_a_split_groups_survivors() {
    let (mut r, mut alice_doc, alice) = acl_seeded();
    // The group lands before bob joins, so it reaches him only via catch-up replay.
    let sent = atomic_across_subtrees(&mut alice_doc);
    submit(&mut r, alice, sent.clone());
    r.take_outbox(alice);

    let (_bob, replay) = acl_join(&mut r, 2, "t-bob");
    let members = members_of(&sent, &replay);
    assert_eq!(members.len(), 2, "bob's replay carries the /a half");
    assert!(
        members.iter().all(|op| op.tx.is_none()),
        "a replayed survivor of a split group rides untagged"
    );

    let bob_doc = folded(2, &replay);
    assert_eq!(
        nested_int(&bob_doc, b"a"),
        Some(1),
        "the replayed survivors applied instead of stranding"
    );
    assert_eq!(nested_int(&bob_doc, b"b"), None);
    assert_eq!(
        bob_doc.seen().count(),
        replay.len(),
        "every replayed op is an applied id, not a held one"
    );
}

#[test]
fn a_group_the_read_filter_carries_whole_stays_atomic() {
    let (mut r, mut alice_doc, alice) = acl_seeded();
    let (bob, base) = acl_join(&mut r, 2, "t-bob");

    let sent = atomic_inside_a(&mut alice_doc);
    submit(&mut r, alice, sent.clone());
    let got = received_ops(&mut r, bob);
    let members = members_of(&sent, &got);
    assert_eq!(members.len(), sent.len(), "nothing was withheld");
    assert!(all_tagged(&got), "an uncut group keeps its tags");

    // Still all-or-nothing at the recipient: a partial fold shows nothing.
    let mut partial = folded(2, &base);
    for op in &got[..got.len() - 1] {
        partial.apply(op);
    }
    assert_eq!(nested_int(&partial, b"a"), None);
    partial.apply(&got[got.len() - 1]);
    assert_eq!(nested_int(&partial, b"a"), Some(7));
}

#[test]
fn a_whole_document_reader_still_receives_the_group_intact() {
    let (mut r, mut alice_doc, alice) = acl_seeded();
    // A second device of alice — the creator's actor, who owns `/` — reads both
    // subtrees, so no filter cuts the group and it must arrive atomic.
    let (alice2, base) = acl_join(&mut r, 10, "t-alice2");

    let sent = atomic_across_subtrees(&mut alice_doc);
    submit(&mut r, alice, sent.clone());
    let got = received_ops(&mut r, alice2);
    assert_eq!(got, sent, "the creator receives every member unchanged");
    assert!(all_tagged(&got), "an uncut group keeps its tags");

    let mut doc = folded(10, &base);
    for op in &got {
        doc.apply(op);
    }
    assert_eq!(nested_int(&doc, b"a"), Some(1));
    assert_eq!(nested_int(&doc, b"b"), Some(2));
}

#[test]
fn a_split_group_converges_with_the_delta_a_later_joiner_replays() {
    // The live and catch-up seams cut the same group the same way, so a reader who
    // was present for the write and one who joined after it hold identical state.
    let (mut r, mut alice_doc, alice) = acl_seeded();
    let (live, base) = acl_join(&mut r, 2, "t-bob");
    submit(&mut r, alice, atomic_across_subtrees(&mut alice_doc));
    let live_ops = received_ops(&mut r, live);

    // A second device of the same actor, joining after the write.
    let (_late, late_ops) = acl_join(&mut r, 3, "t-bob");

    // The two seams admitted the same ops and untagged the same ones.
    let served: Vec<Op> = base.iter().chain(&live_ops).cloned().collect();
    assert_eq!(
        ids_and_tags(&served),
        ids_and_tags(&late_ops),
        "the live and catch-up seams cut the split group differently"
    );

    let mut live_doc = folded(2, &base);
    for op in &live_ops {
        live_doc.apply(op);
    }
    let late_doc = folded(3, &late_ops);
    for doc in [&live_doc, &late_doc] {
        assert_eq!(nested_int(doc, b"a"), Some(1), "the survivors landed");
        assert_eq!(nested_int(doc, b"b"), None, "the withheld half stayed out");
    }
}

/// Each op's identity and whether it still carries a transaction tag, ordered by
/// sequence — what a recipient's bucket sees of a batch, independent of who folds it.
fn ids_and_tags(ops: &[Op]) -> Vec<(u64, bool)> {
    let mut out: Vec<(u64, bool)> = ops.iter().map(|op| (op.id.seq, op.tx.is_some())).collect();
    out.sort_unstable();
    out
}

// --- zones harness ---------------------------------------------------------

const ZONE_ROOM: &[u8] = b"room-z";
const ZONE_APP: &[u8] = b"z";

/// Two zoned map subtrees (`/board` → za, `/notes` → zb) plus an unzoned slot.
const ZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Sect", "notes": "Sect", "loose": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "zones": { "za": "/board", "zb": "/notes" }
}"#;

/// Every actor may read the room (zone gating carries the isolation); the author
/// may do everything; `za` is admitted to zone za alone.
fn zone_authorizer(id: &Identity, action: Action, res: &Resource) -> bool {
    let actor = id.actor();
    match res {
        Resource::Zone { zone, .. } => {
            let zone: &[u8] = zone;
            match actor {
                b"author" => true,
                b"za" => zone == b"za",
                _ => false,
            }
        }
        _ => matches!(action, Action::Read) || actor == b"author",
    }
}

fn zone_registry() -> Registry {
    let mut sr = SchemaRegistry::new();
    sr.register(ZONE_APP, 1, ZONED.as_bytes(), b"").unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens(&[("c-author", "author"), ("c-za", "za")])));
    r.set_authorizer(Box::new(zone_authorizer));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

/// An author that has bootstrapped the room and created the three zone containers,
/// so a later write is pure zoned content.
fn zone_seeded() -> (Registry, Document, ConnId) {
    let mut r = zone_registry();
    let author = auth(&mut r, 1, "c-author", ZONE_APP, 1);
    assert!(subscribe(&mut r, author, ZONE_ROOM, b""));
    r.take_outbox(author);

    let mut doc = Document::new(cid(1));
    doc.set_schema(Schema::parse(ZONED).expect("zoned schema parses"));
    let setup = doc.transact(|tx| {
        tx.map(b"board").register(b"bseed", Scalar::Int(1));
        tx.map(b"notes").register(b"nseed", Scalar::Int(1));
        tx.map(b"loose").register(b"lseed", Scalar::Int(1));
    });
    submit(&mut r, author, setup);
    r.take_outbox(author);
    (r, doc, author)
}

/// Hello + Auth + Subscribe on the zoned room scoped to `zone`, returning the
/// connection and the catch-up ops it was served.
fn zone_join(r: &mut Registry, client: u8, credential: &str, zone: &[u8]) -> (ConnId, Vec<Op>) {
    let id = auth(r, client, credential, ZONE_APP, 1);
    assert!(subscribe(r, id, ZONE_ROOM, zone), "{credential} subscribes");
    let catch_up = received_ops(r, id);
    (id, catch_up)
}

/// One atomic transaction spanning zone za (`/board`) and zone zb (`/notes`): two
/// members, of which a za-scoped subscription admits exactly one.
fn atomic_across_zones(doc: &mut Document) -> Vec<Op> {
    as_one_group(doc.atomic_transact(|tx| {
        tx.map(b"board").register(b"bk", Scalar::Int(2));
        tx.map(b"notes").register(b"nk", Scalar::Int(3));
    }))
}

/// Whether `ops` carry a `RegisterSet` of `key`.
fn has_key(ops: &[Op], key: &[u8]) -> bool {
    ops.iter()
        .any(|op| matches!(&op.kind, OpKind::RegisterSet { key: k, .. } if k == key))
}

/// The `Int` at `/<container>/<key>` of a folded replica.
fn zoned_int(d: &Document, container: &[u8], key: &[u8]) -> Option<i64> {
    let map = match d.get(container)? {
        Element::Map(m) => m,
        _ => return None,
    };
    let inner = map.borrow().get(key)?;
    match inner {
        Element::Register(r) => match r.borrow().read() {
            Scalar::Int(i) => Some(*i),
            _ => None,
        },
        _ => None,
    }
}

#[test]
fn the_live_zone_filter_does_not_strand_a_split_groups_survivors() {
    let (mut r, mut doc, author) = zone_seeded();
    let (za, base) = zone_join(&mut r, 2, "c-za", b"za");

    let sent = atomic_across_zones(&mut doc);
    submit(&mut r, author, sent.clone());
    let got = received_ops(&mut r, za);
    assert!(has_key(&got, b"bk"), "the za member is delivered");
    assert!(!has_key(&got, b"nk"), "the zb member is withheld");
    assert!(
        members_of(&sent, &got).iter().all(|op| op.tx.is_none()),
        "a survivor of a zone-split group rides untagged"
    );

    let mut za_doc = folded(2, &base);
    for op in &got {
        za_doc.apply(op);
    }
    assert_eq!(
        zoned_int(&za_doc, b"board", b"bk"),
        Some(2),
        "the survivor applied instead of stranding in the buffer"
    );

    // Later traffic in the same zone folds on top, rather than finding a permanently
    // buffered predecessor.
    submit(
        &mut r,
        author,
        doc.transact(|tx| {
            tx.map(b"board").register(b"bk", Scalar::Int(5));
        }),
    );
    for op in received_ops(&mut r, za) {
        za_doc.apply(&op);
    }
    assert_eq!(zoned_int(&za_doc, b"board", b"bk"), Some(5));
}

#[test]
fn the_catch_up_zone_filter_does_not_strand_a_split_groups_survivors() {
    let (mut r, mut doc, author) = zone_seeded();
    // The group lands before the za subscriber joins, so it rides the catch-up delta.
    let sent = atomic_across_zones(&mut doc);
    submit(&mut r, author, sent.clone());
    r.take_outbox(author);

    let (_za, replay) = zone_join(&mut r, 2, "c-za", b"za");
    assert!(has_key(&replay, b"bk"), "the za member is replayed");
    assert!(!has_key(&replay, b"nk"), "the zb member is withheld");
    assert!(
        members_of(&sent, &replay).iter().all(|op| op.tx.is_none()),
        "a replayed survivor of a zone-split group rides untagged"
    );

    let za_doc = folded(2, &replay);
    assert_eq!(
        zoned_int(&za_doc, b"board", b"bk"),
        Some(2),
        "the replayed survivor applied instead of stranding"
    );
    assert_eq!(zoned_int(&za_doc, b"notes", b"nk"), None);
}

#[test]
fn a_group_the_zone_filter_carries_whole_stays_atomic() {
    let (mut r, mut doc, author) = zone_seeded();
    let (za, base) = zone_join(&mut r, 2, "c-za", b"za");

    // Both members live in zone za, so nothing is withheld from this subscription.
    let sent = doc.atomic_transact(|tx| {
        tx.map(b"board").register(b"one", Scalar::Int(1));
        tx.map(b"board").register(b"two", Scalar::Int(2));
    });
    submit(&mut r, author, sent.clone());
    let got = received_ops(&mut r, za);
    assert_eq!(got, sent, "both za members are delivered unchanged");
    assert!(all_tagged(&got), "an uncut group keeps its tags");

    let mut doc = folded(2, &base);
    doc.apply(&got[0]);
    assert_eq!(zoned_int(&doc, b"board", b"one"), None);
    doc.apply(&got[1]);
    assert_eq!(zoned_int(&doc, b"board", b"one"), Some(1));
    assert_eq!(zoned_int(&doc, b"board", b"two"), Some(2));
}

#[test]
fn an_emitters_cross_zone_commit_reaches_a_zone_subscriber_atomically() {
    // C2's payoff, driven end to end on the emitter's own output rather than a
    // hand-forged group: a commit spanning za and zb is cut to a group per zone, so
    // the za subscriber's filter withholds zb's members without splitting anything
    // it holds. Its za members therefore arrive tagged and all-or-nothing, where a
    // group spanning both would have been destranded here and merged one at a time.
    let (mut r, mut doc, author) = zone_seeded();
    let (za, base) = zone_join(&mut r, 2, "c-za", b"za");

    let sent = doc.atomic_transact(|tx| {
        tx.map(b"board").register(b"bk", Scalar::Int(2));
        tx.map(b"notes").register(b"nk", Scalar::Int(3));
        tx.map(b"board").register(b"bk2", Scalar::Int(4));
    });
    submit(&mut r, author, sent.clone());
    let got = received_ops(&mut r, za);
    assert!(has_key(&got, b"bk"), "the za members are delivered");
    assert!(!has_key(&got, b"nk"), "the zb member is withheld");

    let members = members_of(&sent, &got);
    assert!(
        members.len() > 1,
        "the za group has members to hold together"
    );
    assert!(
        members.iter().all(|op| op.tx.is_some()),
        "the za group survives the cut tagged"
    );

    // All-or-nothing at the subscriber: every member but the last is held, and they
    // land together on the arrival that completes the group.
    let mut za_doc = folded(2, &base);
    let (last, held) = got.split_last().expect("the batch is not empty");
    for op in held {
        za_doc.apply(op);
    }
    assert_eq!(
        (
            zoned_int(&za_doc, b"board", b"bk"),
            zoned_int(&za_doc, b"board", b"bk2")
        ),
        (None, None),
        "the partial group is invisible, whichever member the batch ends on"
    );
    za_doc.apply(last);
    assert_eq!(zoned_int(&za_doc, b"board", b"bk"), Some(2));
    assert_eq!(zoned_int(&za_doc, b"board", b"bk2"), Some(4));
    assert_eq!(zoned_int(&za_doc, b"notes", b"nk"), None);
}

#[test]
fn the_root_partition_travels_with_a_zone_split_group() {
    // A group spanning the root partition and one zone: the unzoned member is always
    // admitted, the foreign zone's is not, so the survivors — root and za alike —
    // ride untagged and land.
    let (mut r, mut doc, author) = zone_seeded();
    let (za, base) = zone_join(&mut r, 2, "c-za", b"za");

    let sent = as_one_group(doc.atomic_transact(|tx| {
        tx.map(b"loose").register(b"lk", Scalar::Int(4));
        tx.map(b"board").register(b"bk", Scalar::Int(5));
        tx.map(b"notes").register(b"nk", Scalar::Int(6));
    }));
    submit(&mut r, author, sent.clone());
    let got = received_ops(&mut r, za);
    assert!(
        has_key(&got, b"lk"),
        "the root-partition member is delivered"
    );
    assert!(has_key(&got, b"bk"), "the za member is delivered");
    assert!(!has_key(&got, b"nk"), "the zb member is withheld");
    assert!(
        members_of(&sent, &got).iter().all(|op| op.tx.is_none()),
        "every survivor of the split group rides untagged"
    );

    let mut za_doc = folded(2, &base);
    for op in &got {
        za_doc.apply(op);
    }
    assert_eq!(zoned_int(&za_doc, b"loose", b"lk"), Some(4));
    assert_eq!(zoned_int(&za_doc, b"board", b"bk"), Some(5));
}

// --- interaction with the readiness gate -----------------------------------

#[test]
fn a_destranded_survivor_still_waits_on_a_dependency_outside_the_group() {
    // C1's rule survives destranding: completeness is the only group-level gate, and
    // a member passes the readiness gate at its own apply moment. A survivor whose
    // container has not arrived is held by that gate — and drains the moment it does.
    let (mut r, mut alice_doc, alice) = acl_seeded();
    let (bob, base) = acl_join(&mut r, 2, "t-bob");

    submit(&mut r, alice, atomic_across_subtrees(&mut alice_doc));
    let mut got = received_ops(&mut r, bob);
    assert_eq!(got.len(), 2);
    // The `MapCreate` of /a leads its `RegisterSet`; withhold it and the survivor has
    // no reachable target.
    let create = got.remove(0);
    let mut bob_doc = folded(2, &base);
    for op in &got {
        bob_doc.apply(op);
    }
    assert_eq!(
        nested_int(&bob_doc, b"a"),
        None,
        "a survivor whose container is missing waits on its own"
    );
    bob_doc.apply(&create);
    assert_eq!(
        nested_int(&bob_doc, b"a"),
        Some(1),
        "it drains once its dependency lands"
    );
}

#[test]
fn destranded_survivors_count_as_ids_the_replica_holds() {
    // C6/C9: a replica reads its next sequence off the ids it holds, buffered ones
    // included. Destranding moves a survivor from held-forever to applied, so the
    // accounting has to see it in the applied set.
    let (mut r, mut alice_doc, alice) = acl_seeded();
    let (bob, base) = acl_join(&mut r, 2, "t-bob");

    submit(&mut r, alice, atomic_across_subtrees(&mut alice_doc));
    let got = received_ops(&mut r, bob);
    let mut bob_doc = folded(2, &base);
    for op in &got {
        bob_doc.apply(op);
    }
    let seen: Vec<_> = bob_doc.seen().collect();
    for op in &got {
        assert!(
            seen.contains(&op.id),
            "a survivor is missing from the dedup set"
        );
    }
}

#[test]
fn a_partial_group_that_no_filter_cut_is_still_held() {
    // Destranding is scoped to what a filter withholds. A group merely in flight —
    // its remaining members still coming — keeps its tags and its all-or-nothing
    // view, and completes when they land.
    let (mut r, mut alice_doc, alice) = acl_seeded();
    let (alice2, base) = acl_join(&mut r, 10, "t-alice2");

    submit(&mut r, alice, atomic_across_subtrees(&mut alice_doc));
    let got = received_ops(&mut r, alice2);
    assert_eq!(got.len(), 4);

    let mut doc = folded(10, &base);
    for op in &got[..3] {
        doc.apply(op);
    }
    assert_eq!(nested_int(&doc, b"a"), None, "a partial group stays hidden");
    doc.apply(&got[3]);
    assert_eq!(nested_int(&doc, b"a"), Some(1));
    assert_eq!(nested_int(&doc, b"b"), Some(2));
}

/// A top-level register write, for a group whose members sit at the document root.
fn top_write(doc: &mut Document, key: &[u8], v: i64) -> Vec<Op> {
    doc.transact(|tx| {
        tx.register(key, Scalar::Int(v));
    })
}

#[test]
fn a_room_with_no_filtering_delivers_groups_byte_identically() {
    // Regression: with no doc-ACL tuples and no zones, no seam withholds anything, so
    // every subscriber receives the identical batch with its tags intact.
    let mut r = Registry::new(cid(0xFF));
    r.set_verifier(Box::new(tokens(&[("t-alice", "alice"), ("t-bob", "bob")])));
    r.set_authorizer(Box::new(
        Acl::new()
            .allow(
                Subject::Anyone,
                Some(Action::Read),
                ResourceMatch::Room(ROOM.to_vec()),
            )
            .allow(
                Subject::Anyone,
                Some(Action::Write),
                ResourceMatch::Room(ROOM.to_vec()),
            ),
    ));
    r.set_clock(Arc::new(ManualClock::new(0)));

    let alice = auth(&mut r, 1, "t-alice", b"", 0);
    assert!(subscribe(&mut r, alice, ROOM, b""));
    let bob = auth(&mut r, 2, "t-bob", b"", 0);
    assert!(subscribe(&mut r, bob, ROOM, b""));
    r.take_outbox(alice);
    r.take_outbox(bob);

    let mut doc = Document::new(cid(1));
    let plain = top_write(&mut doc, b"plain", 1);
    let group = doc.atomic_transact(|tx| {
        tx.register(b"g1", Scalar::Int(2));
        tx.register(b"g2", Scalar::Int(3));
    });
    submit(&mut r, alice, plain.clone());
    submit(&mut r, alice, group.clone());

    let got = received_ops(&mut r, bob);
    let sent: Vec<Op> = plain.into_iter().chain(group).collect();
    assert_eq!(got, sent, "an unfiltered fan-out is byte-identical");
    let bob_doc = folded(2, &got);
    assert_eq!(top_int(&bob_doc, b"plain"), Some(1));
    assert_eq!(top_int(&bob_doc, b"g1"), Some(2));
    assert_eq!(top_int(&bob_doc, b"g2"), Some(3));
}
