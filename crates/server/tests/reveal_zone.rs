//! The reveal-on-move-in prefix and the per-zone wire redaction, together (C16).
//!
//! A node moved out of a subtree a recipient cannot read into one it can is revealed
//! to that recipient: the live fan-out prepends a synthetic shell plus the node's
//! now-readable content replayed from the log, and stamps both in the partition the
//! node lands in so a zone-scoped channel cannot drop the content while keeping the
//! shell. Two things break that co-travel unless the landing partition is read off the
//! folded tree rather than off each move's own envelope. The back-fill skips an op the
//! delivered batch already carries — a second copy would take that op out of its
//! transaction's count — and the surviving copy is the batch's, stamped where its
//! author resolved it at emit time. And a move emitted before the one that relocates
//! its new parent resolves in the partition the subtree is *leaving*, so a nested
//! reveal's inner shell disagrees with its outer one.
//!
//! The shape is reachable on `main` through the authorized cross-zone move: the
//! capability token admits exactly the one crossing, and the batch riding it may carry
//! any number of un-crossing relocations inside the moved node.
//!
//! Every edit inside an already-materialised XML node that one transaction can express
//! alongside the move is itself a move — the authoring surface addresses a deep node by
//! id only there. The partition all of them resolve through is the same one
//! (`zone_of_op` over the op's target container in the live tree), so the fixtures are
//! moves throughout.

use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::elementid::ElementKind;
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::xml::XmlFragment;
use crdtsync_core::{AclEffect, ClientId, Document, ElementId, Message, Op, OpKind};
use crdtsync_server::acl::{actor_key, Acl, ResourceMatch, Subject};
use crdtsync_server::{Action, ConnId, ManualClock, Registry, SchemaRegistry, StaticTokens};

const ROOM: &[u8] = b"room-rz";
const APP: &[u8] = b"z";
/// `za` roots at `/board`, `zb` at `/notes`; `/notes` splits into a readable `pub`
/// and a denied `priv`, so a node can be born denied *inside* the destination zone.
const ZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "board": "Frag", "notes": "Notes" } },
        "Notes": { "kind": "map", "children": { "pub": "Frag", "priv": "Frag" } },
        "Frag": { "kind": "fragment", "children": { "a": {} } },
        "a": { "kind": "xml", "tag": "a", "children": { "a": {} } }
    },
    "zones": { "za": "/board", "zb": "/notes" }
}"#;

const BOARD: &[u8] = b"board";
const NOTES: &[u8] = b"notes";
const PUB: &[u8] = b"pub";
const PRIV: &[u8] = b"priv";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A registry whose deployment allows alice (the room's creator) to read and write
/// the room and **abstains on every other actor**, so bob's and carol's read verdicts
/// are the doc-ACL tier's alone and the per-path redaction bites. A zone key is
/// installed so the cross-zone capability token can be minted. A fixed clock keeps it
/// Miri-clean.
fn registry() -> Registry {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, ZONED.as_bytes(), b"").unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    let mut t = StaticTokens::new();
    for (cred, actor) in [("t-alice", "alice"), ("t-bob", "bob"), ("t-carol", "carol")] {
        t.insert(cred.as_bytes().to_vec(), actor.as_bytes().to_vec());
    }
    r.set_verifier(Box::new(t));
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
    r.hub_mut().set_zone_key([0x5a; 32]);
    r
}

/// Hello (enforcing `{APP, v1}`) + Auth as `credential`, without subscribing.
fn auth(r: &mut Registry, client: u8, credential: &str) -> ConnId {
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

/// Subscribe `id` on channel 0 with zone selector `zone` (empty is the whole room),
/// returning the catch-up frames.
fn subscribe(r: &mut Registry, id: ConnId, zone: &[u8]) -> Vec<Message> {
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: Channel(0),
            room: ROOM.to_vec(),
            branch: Vec::new(),
            zone: zone.to_vec(),
            last_seen_seq: 0,
        }
    ));
    r.take_outbox(id)
}

/// Whether `msgs` carries a rejection — a write refused recoverably still leaves the
/// connection open, so a fixture that mis-builds a batch would otherwise surface many
/// assertions later as "the reader never received it".
fn rejected(msgs: &[Message]) -> bool {
    msgs.iter()
        .any(|m| matches!(m, Message::OpsRejected { .. }))
}

fn submit(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: Channel(0),
            ops
        }
    ));
    assert!(!rejected(&r.take_outbox(id)), "the write is accepted");
}

/// Every op the frames in `msgs` carry.
fn flatten(msgs: &[Message]) -> Vec<Op> {
    msgs.iter()
        .filter_map(|m| match m {
            Message::Ops { ops, .. } => Some(ops.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

fn received_ops(r: &mut Registry, id: ConnId) -> Vec<Op> {
    flatten(&r.take_outbox(id))
}

/// A parenthesised rendering of one XML node — its tag and, recursively, its live
/// children — so two replicas can be compared on their materialised tree.
fn render_node(e: &crdtsync_core::Element) -> String {
    match e {
        crdtsync_core::Element::XmlElement(x) => {
            let x = x.borrow();
            let tag = String::from_utf8_lossy(x.tag()).into_owned();
            let kids: Vec<String> = x
                .children()
                .borrow()
                .values()
                .iter()
                .map(render_node)
                .collect();
            format!("{tag}({})", kids.join(","))
        }
        crdtsync_core::Element::Text(_) => "text".to_string(),
        _ => "?".to_string(),
    }
}

/// The materialised `/notes/<key>` fragment — `absent` where the reader does not
/// hold it.
fn frag_render(d: &Document, key: &[u8]) -> String {
    let slot = match d.get(NOTES) {
        Some(crdtsync_core::Element::Map(m)) => m.borrow().get(key),
        _ => None,
    };
    match slot {
        Some(crdtsync_core::Element::XmlFragment(f)) => {
            let kids: Vec<String> = f
                .borrow()
                .children()
                .borrow()
                .values()
                .iter()
                .map(render_node)
                .collect();
            format!("frag({})", kids.join(","))
        }
        _ => "absent".to_string(),
    }
}

/// The id of the fragment at `/notes/<key>`.
fn notes_frag(d: &Document, key: &[u8]) -> ElementId {
    XmlFragment::node_id(ElementId::derive(d.root_id(), NOTES, ElementKind::Map), key)
}

/// The op in `batch` that relocates `node`.
fn moves(batch: &[Op], node: ElementId) -> Op {
    batch
        .iter()
        .find(|op| matches!(&op.kind, OpKind::XmlMove { node: n, .. } if *n == node))
        .expect("the batch relocates the node")
        .clone()
}

/// The ids the fixture's tree is addressed by.
struct Tree {
    /// Born in `/board` (zone za), denied to bob, carrying a private grandchild.
    card: ElementId,
    /// Born in `/notes/pub` (zone zb), readable to bob.
    pin: ElementId,
    /// Born in `/notes/priv` (zone zb), denied to bob.
    x: ElementId,
    /// Born in `/notes/priv` beside `x`, denied to bob.
    y: ElementId,
    /// The `/notes/pub` fragment — every move-in destination below.
    dest: ElementId,
}

/// alice bootstraps the room: bob reads `/notes` but not `/notes/priv`, carol reads
/// the whole document, and the tree above is built. Returns the registry, alice's authoring doc,
/// her connection, and the tree's ids.
fn seeded() -> (Registry, Document, ConnId, Tree) {
    let mut r = registry();
    let alice = auth(&mut r, 1, "t-alice");
    subscribe(&mut r, alice, b"");
    let mut doc = Document::new(cid(1));
    doc.set_schema(crdtsync_core::Schema::parse(ZONED).expect("schema parses"));
    submit(
        &mut r,
        alice,
        doc.transact(|tx| {
            let mut acl = tx.acl();
            acl.grant(
                AclSubject::Actor(actor_key(b"bob")),
                AclGrant::Capability(Capability::Read),
                AclEffect::Allow,
                encode_path(&[NOTES]),
                actor_key(b"alice"),
            );
            acl.grant(
                AclSubject::Actor(actor_key(b"bob")),
                AclGrant::Capability(Capability::Read),
                AclEffect::Deny,
                encode_path(&[NOTES, PRIV]),
                actor_key(b"alice"),
            );
            acl.grant(
                AclSubject::Actor(actor_key(b"carol")),
                AclGrant::Capability(Capability::Read),
                AclEffect::Allow,
                encode_path(&[]),
                actor_key(b"alice"),
            );
        }),
    );
    let mut card = ElementId::from_bytes([0u8; 16]);
    let (mut pin, mut x, mut y) = (card, card, card);
    submit(
        &mut r,
        alice,
        doc.transact(|tx| {
            {
                // The card carries a grandchild born in the denied column — the content
                // the reveal back-fill replays out of the log.
                let mut board = tx.xml_fragment(BOARD);
                let mut kids = board.children();
                let mut c = kids.insert_element(0, b"a");
                card = c.id();
                c.children().insert_element(0, b"a");
            }
            let mut notes = tx.map(NOTES);
            {
                let mut public = notes.xml_fragment(PUB);
                pin = public.children().insert_element(0, b"a").id();
            }
            let mut private = notes.xml_fragment(PRIV);
            let mut kids = private.children();
            x = kids.insert_element(0, b"a").id();
            y = kids.insert_element(1, b"a").id();
        }),
    );
    let dest = notes_frag(&doc, PUB);
    (
        r,
        doc,
        alice,
        Tree {
            card,
            pin,
            x,
            y,
            dest,
        },
    )
}

/// Request a cross-zone capability token for `element` into `dst_zone`.
fn token(r: &mut Registry, id: ConnId, element: ElementId, dst_zone: &[u8]) -> Vec<u8> {
    assert!(r.deliver(
        id,
        Message::CrossZoneToken {
            room: ROOM.to_vec(),
            element,
            dst_zone: dst_zone.to_vec(),
        }
    ));
    r.take_outbox(id)
        .into_iter()
        .find_map(|m| match m {
            Message::CrossZoneTokenGrant { token, .. } => Some(token),
            _ => None,
        })
        .expect("the creator is granted a cross-zone token")
}

/// Redeem `tok` for `ops` on alice's channel.
fn cross_zone(r: &mut Registry, id: ConnId, ops: Vec<Op>, tok: Vec<u8>) {
    assert!(r.deliver(
        id,
        Message::CrossZoneOps {
            channel: Channel(0),
            ops,
            token: tok,
        }
    ));
    assert!(
        !rejected(&r.take_outbox(id)),
        "the tokened cross-zone move is admitted"
    );
}

/// bob: subscribed to zone zb, reading `/notes` but not `/notes/priv`. Returns his
/// connection and the replica his catch-up folds to.
fn bob_joins(r: &mut Registry) -> (ConnId, Document) {
    let bob = auth(r, 2, "t-bob");
    let mut replica = Document::new(cid(2));
    for op in flatten(&subscribe(r, bob, b"zb")) {
        replica.apply(&op);
    }
    assert_eq!(
        frag_render(&replica, PUB),
        "frag(a())",
        "bob starts holding the readable notes fragment and its pin, nothing else",
    );
    (bob, replica)
}

#[test]
fn a_zone_scoped_reader_revealed_a_moved_node_receives_the_batchs_own_content_too() {
    let (mut r, mut doc, alice, t) = seeded();
    let (bob, mut replica) = bob_joins(&mut r);

    // One atomic transaction: the pin moves *into* the card — an edit inside the card's
    // subtree, resolved while the card still sits in the board column, so its envelope
    // says za — and then the card moves into the readable notes fragment, whose
    // envelope says zb. Only the card crosses a zone boundary, so one capability token
    // authorizes the batch.
    let tok = token(&mut r, alice, t.card, b"zb");
    let batch = doc.atomic_transact(|tx| {
        tx.move_xml(t.pin, t.card, 0);
        tx.move_xml(t.card, t.dest, 0);
    });
    let edit = moves(&batch, t.pin);
    let place = moves(&batch, t.card);
    // The precondition the whole shape rests on: one transaction, two partitions. A
    // core change that collapsed them would leave the assertions below passing for a
    // reason that has nothing to do with the co-travel.
    assert_ne!(
        edit.zone, place.zone,
        "the edit inside the card and the move that places it must carry different partitions",
    );
    assert!(
        edit.tx.is_some() && edit.tx == place.tx,
        "the fixture's two ops are one atomic group",
    );
    cross_zone(&mut r, alice, batch, tok);

    let revealed = received_ops(&mut r, bob);
    assert!(
        revealed
            .iter()
            .any(|op| matches!(op.kind, OpKind::XmlReveal { .. })),
        "the move-in reveals the card to the zone-scoped reader",
    );
    let delivered = revealed
        .iter()
        .find(|op| op.id == edit.id)
        .expect("the batch's own edit inside the revealed node reaches the reader");
    assert_eq!(
        delivered.zone, place.zone,
        "the surviving copy rides the partition the card lands in, as its back-filled copy would",
    );
    assert_eq!(
        delivered.tx, edit.tx,
        "co-travelling leaves the transaction whole rather than destranding it",
    );
    for op in &revealed {
        replica.apply(op);
    }
    assert_eq!(
        frag_render(&replica, PUB),
        frag_render(&doc, PUB),
        "the zone-scoped reader converges with the author on the revealed node's contents",
    );
}

#[test]
fn a_nested_reveals_inner_shell_rides_the_partition_its_subtree_lands_in() {
    // Two reveals in one batch, and the inner node's placing move is emitted *before*
    // the outer node's — so its envelope carries the partition the subtree is leaving,
    // not the one it lands in. `x` is born denied inside the destination zone
    // (`/notes/priv`), `card` denied in the origin zone (`/board`), and both surface to
    // bob at `/notes/pub/card/x` once the batch lands. A shell stamped from its own
    // move's envelope would strand `x` unmaterialised on bob's zb channel forever.
    let (mut r, mut doc, alice, t) = seeded();
    let (bob, mut replica) = bob_joins(&mut r);

    let tok = token(&mut r, alice, t.card, b"zb");
    let batch = doc.atomic_transact(|tx| {
        tx.move_xml(t.y, t.x, 0);
        tx.move_xml(t.x, t.card, 0);
        tx.move_xml(t.card, t.dest, 0);
    });
    // Only the card crosses; `x` and `y` start and end inside zb, so the one token
    // covers the batch. The inner placement's envelope is the origin partition — the
    // condition the shell derivation must not inherit.
    let inner = moves(&batch, t.x);
    let place = moves(&batch, t.card);
    assert_ne!(
        inner.zone, place.zone,
        "the move that places x into the card must carry the partition the card is leaving",
    );
    cross_zone(&mut r, alice, batch, tok);

    let revealed = received_ops(&mut r, bob);
    let shells: Vec<Option<u32>> = revealed
        .iter()
        .filter(|op| matches!(op.kind, OpKind::XmlReveal { .. }))
        .map(|op| op.zone)
        .collect();
    assert_eq!(
        shells.len(),
        3,
        "every born-denied node the batch relocates is revealed",
    );
    assert!(
        shells.iter().all(|z| *z == place.zone),
        "every shell rides the partition the relocated subtree lands in: {shells:?}",
    );
    for op in &revealed {
        replica.apply(op);
    }
    assert_eq!(
        frag_render(&replica, PUB),
        frag_render(&doc, PUB),
        "the zone-scoped reader converges with the author on the whole nested subtree",
    );
}

#[test]
fn a_reader_the_reveal_does_not_fire_for_receives_the_batch_unaltered() {
    // The co-travel rewrite is scoped to a recipient a reveal actually fires for. carol
    // reads the whole document, so no node is born denied to her and she is revealed
    // nothing; every op her za channel admits carries the partition its author stamped
    // it in.
    let (mut r, mut doc, alice, t) = seeded();
    let carol = auth(&mut r, 3, "t-carol");
    subscribe(&mut r, carol, b"za");

    let tok = token(&mut r, alice, t.card, b"zb");
    let batch = doc.atomic_transact(|tx| {
        tx.move_xml(t.pin, t.card, 0);
        tx.move_xml(t.card, t.dest, 0);
    });
    let edit = moves(&batch, t.pin);
    cross_zone(&mut r, alice, batch, tok);

    let got = received_ops(&mut r, carol);
    assert!(
        !got.iter()
            .any(|op| matches!(op.kind, OpKind::XmlReveal { .. })),
        "a whole-document reader is revealed nothing",
    );
    assert_eq!(
        got.iter().find(|op| op.id == edit.id).map(|op| op.zone),
        Some(edit.zone),
        "the op reaches the za channel in the partition its author stamped",
    );
}
