//! Zone-scoped subscription, per-zone replication streams, and wire redaction
//! (Zones Unit 3).
//!
//! A zone is a schema-declared, path-rooted subtree partition destined to be its
//! own replication stream with *strong* isolation: an actor the deployment does not
//! admit to a zone receives nothing about it — no ops, no snapshot state, no
//! structure, no op count, not even a clock jump or a signal that the zone exists.
//! This is stronger than doc-ACL redaction, which withholds ops within one shared
//! stream but still leaks structure; here the unauthorized partition is *wholly
//! absent* from what the recipient sees.
//!
//! The op envelope already carries which zone each op belongs to (Unit 2), and the
//! per-zone lamport clocks keep the partitions causally independent, so filtering
//! the fan-out and catch-up by `op.zone` is a clean cut — a recipient never observes
//! another zone advancing. These tests drive the whole path in-process through the
//! multi-subscriber [`Registry`], so they run under Miri (no socket, no fs).
//!
//! A container-create belongs to the created child's zone (not the parent it
//! targets), so a zone owns its own root container's creation — the whole lifecycle
//! of a zoned subtree, its root container included, is stamped in the zone and
//! withheld from an unauthorized subscriber on every seam (live fan-out, catch-up
//! delta, and cold-start snapshot alike). The tests create the containers in setup
//! (before the subscribers join) only so a later content write is a pure set the
//! marker assertions can key on, not because the create would otherwise leak.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Op, OpKind, Scalar, Schema};
use crdtsync_server::acl::{Acl, ResourceMatch, Subject};
use crdtsync_server::{
    Action, ConnId, Identity, ManualClock, Registry, Resource, SchemaRegistry, StaticTokens,
};

const ROOM: &[u8] = b"room-z";
const APP: &[u8] = b"z";
const APP_UNZONED: &[u8] = b"u";

/// Two zoned map subtrees (`/board` → za, `/notes` → zb) and one unzoned slot
/// (`/loose`, the root partition).
const ZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Sect", "notes": "Sect", "loose": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "zones": { "za": "/board", "zb": "/notes" }
}"#;

/// The same slot layout with no `zones` block — every location is the one implicit
/// root partition.
const UNZONED: &str = r#"{
    "schema": "u", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Sect", "notes": "Sect", "loose": "Sect" } },
        "Sect": { "kind": "map" }
    }
}"#;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn zoned_schema() -> Schema {
    Schema::parse(ZONED).expect("zoned schema parses")
}

/// The deployment authorizer: every actor may read the room (zone gating does the
/// isolation), the author may do everything (it bootstraps and writes), and each
/// reader is admitted only to the zones its role names. `za` reads zone za, `zb`
/// reads zb, `partial` reads za alone; none of them reads the other's zone.
fn authorizer(id: &Identity, action: Action, res: &Resource) -> bool {
    let actor = id.actor();
    match res {
        Resource::Zone { zone, .. } => {
            let zone: &[u8] = zone;
            match actor {
                b"author" => true,
                b"za" | b"partial" => zone == b"za",
                b"zb" => zone == b"zb",
                _ => false,
            }
        }
        // Room read admits everyone; only the author writes. Zone reads above carve
        // the isolation, so the room gate stays open.
        _ => matches!(action, Action::Read) || actor == b"author",
    }
}

fn registry() -> Registry {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, ZONED.as_bytes(), b"").unwrap();
    sr.register(APP_UNZONED, 1, UNZONED.as_bytes(), b"")
        .unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens()));
    r.set_authorizer(Box::new(authorizer));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

fn tokens() -> StaticTokens {
    let mut t = StaticTokens::new();
    for (cred, actor) in [
        ("c-author", "author"),
        ("c-za", "za"),
        ("c-za2", "za"),
        ("c-zb", "zb"),
        ("c-partial", "partial"),
    ] {
        t.insert(cred.as_bytes().to_vec(), actor.as_bytes().to_vec());
    }
    t
}

/// Hello (enforcing `{app, v1}`) + Auth as `cred`, without subscribing.
fn auth(r: &mut Registry, client: u8, cred: &str, app: &[u8]) -> ConnId {
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
            credential: cred.as_bytes().to_vec(),
        }
    ));
    r.take_outbox(id);
    id
}

/// Subscribe `id` to the room on channel 0 with zone selector `zone` (empty is the
/// whole room), returning the raw reply frames so a test can inspect a refusal.
fn subscribe(r: &mut Registry, id: ConnId, zone: &[u8]) -> Vec<Message> {
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

fn write(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(
        id,
        Message::Ops {
            channel: Channel(0),
            ops
        }
    ));
    r.take_outbox(id);
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

/// Whether `ops` carry a `RegisterSet` of `key` — the marker each content write
/// leaves in its zone.
fn has_key(ops: &[Op], key: &[u8]) -> bool {
    ops.iter()
        .any(|op| matches!(&op.kind, OpKind::RegisterSet { key: k, .. } if k == key))
}

/// An author connection that has bootstrapped the room and created the three zone
/// containers, each seeded with one register — returns the registry, the author's
/// authoring doc (reused so later writes are pure zoned content, no re-create), and
/// the author's conn.
fn seeded() -> (Registry, Document, ConnId) {
    let mut r = registry();
    let author = auth(&mut r, 1, "c-author", APP);
    // The author subscribes first (whole room), so its own writes establish the
    // room, its creator, and the zone containers.
    assert!(matches!(
        subscribe(&mut r, author, b"").as_slice(),
        [Message::Ops { .. }, ..] | []
    ));
    r.take_outbox(author);

    let mut doc = Document::new(cid(1));
    doc.set_schema(zoned_schema());
    // Create /board, /notes, /loose, each seeded with a register. The MapCreate of
    // each container is a root-partition op (it mutates the root map); the seed
    // register inside a zoned container is a zoned op.
    let setup = doc.transact(|tx| {
        tx.map(b"board").register(b"bseed", Scalar::Int(1));
        tx.map(b"notes").register(b"nseed", Scalar::Int(1));
        tx.map(b"loose").register(b"lseed", Scalar::Int(1));
    });
    write(&mut r, author, setup);
    (r, doc, author)
}

/// A content write into `/board` — a pure zoned (za) `RegisterSet`, since board
/// already exists in the author's doc.
fn board_write(doc: &mut Document, key: &[u8], v: i64) -> Vec<Op> {
    doc.transact(|tx| {
        tx.map(b"board").register(key, Scalar::Int(v));
    })
}

/// A content write into `/notes` — a pure zoned (zb) `RegisterSet`.
fn notes_write(doc: &mut Document, key: &[u8], v: i64) -> Vec<Op> {
    doc.transact(|tx| {
        tx.map(b"notes").register(key, Scalar::Int(v));
    })
}

#[test]
fn zone_scoped_subscribe_delivers_only_that_zones_content() {
    let (mut r, mut doc, author) = seeded();
    // A subscriber scoped to zone za. Its catch-up carries the root partition plus
    // za — board's seed — but not zb's (notes' seed).
    let za = auth(&mut r, 2, "c-za", APP);
    let catchup = subscribe(&mut r, za, b"za");
    let catch_ops: Vec<Op> = catchup
        .into_iter()
        .flat_map(|m| match m {
            Message::Ops { ops, .. } => ops,
            _ => Vec::new(),
        })
        .collect();
    assert!(has_key(&catch_ops, b"bseed"), "za catch-up carries board");
    assert!(
        has_key(&catch_ops, b"lseed"),
        "za catch-up carries the root partition"
    );
    assert!(
        !has_key(&catch_ops, b"nseed"),
        "za catch-up omits notes (zone zb)"
    );

    // A live board write reaches the za subscriber; a live notes write does not.
    write(&mut r, author, board_write(&mut doc, b"bk", 2));
    let got = received_ops(&mut r, za);
    assert!(has_key(&got, b"bk"), "za sees the board write");

    write(&mut r, author, notes_write(&mut doc, b"nk", 2));
    let got = received_ops(&mut r, za);
    assert!(!has_key(&got, b"nk"), "za never sees the notes write");
}

#[test]
fn unauthorized_zone_subscribe_is_refused_generically() {
    let (mut r, _doc, _author) = seeded();
    // The za reader asks for zone zb, which it may not read. It is refused — and the
    // refusal is a generic Forbidden that names no zone, so it does not confirm zb
    // exists.
    let za = auth(&mut r, 2, "c-za", APP);
    let reply = subscribe(&mut r, za, b"zb");
    match reply.as_slice() {
        [Message::Error { code, message, .. }] => {
            assert_eq!(*code, ErrorCode::Forbidden);
            assert!(
                !message.contains("zb") && !message.to_lowercase().contains("zone"),
                "the denial leaks no zone identity: {message:?}"
            );
        }
        other => panic!("expected a lone Forbidden, got {other:?}"),
    }
}

#[test]
fn a_forbidden_zone_and_a_nonexistent_zone_are_indistinguishable() {
    let (mut r, _doc, _author) = seeded();
    // A forbidden-but-real zone (zb, which za may not read) and a zone that does not
    // exist at all (`zx`) are answered with byte-identical refusals — so a probe
    // cannot tell an existing hidden zone from a nonexistent one.
    let za1 = auth(&mut r, 2, "c-za", APP);
    let forbidden_real = subscribe(&mut r, za1, b"zb");
    let za2 = auth(&mut r, 3, "c-za2", APP);
    let nonexistent = subscribe(&mut r, za2, b"zx");
    assert_eq!(
        forbidden_real, nonexistent,
        "a hidden real zone and a nonexistent one are indistinguishable"
    );
}

#[test]
fn whole_room_subscribe_omits_the_unauthorized_zone() {
    let (mut r, mut doc, author) = seeded();
    // `partial` reads za but not zb. A whole-room (empty selector) subscribe admits
    // it — it may read the room — but carries only its authorized zones: board and
    // the root partition, never notes.
    let partial = auth(&mut r, 2, "c-partial", APP);
    let catchup = subscribe(&mut r, partial, b"");
    let catch_ops: Vec<Op> = catchup
        .into_iter()
        .flat_map(|m| match m {
            Message::Ops { ops, .. } => ops,
            _ => Vec::new(),
        })
        .collect();
    assert!(
        has_key(&catch_ops, b"bseed"),
        "partial carries its authorized zone"
    );
    assert!(
        has_key(&catch_ops, b"lseed"),
        "partial carries the root partition"
    );
    assert!(
        !has_key(&catch_ops, b"nseed"),
        "partial omits the unauthorized zone"
    );

    // Live: the board write reaches it, the notes write is wholly absent.
    write(&mut r, author, board_write(&mut doc, b"bk", 2));
    write(&mut r, author, notes_write(&mut doc, b"nk", 2));
    let got = received_ops(&mut r, partial);
    assert!(
        has_key(&got, b"bk"),
        "partial sees the authorized zone's write"
    );
    assert!(
        !has_key(&got, b"nk"),
        "partial never sees the unauthorized zone"
    );
}

#[test]
fn zone_w_activity_delivers_nothing_to_a_zone_z_subscriber() {
    let (mut r, mut doc, author) = seeded();
    let za = auth(&mut r, 2, "c-za", APP);
    subscribe(&mut r, za, b"za");
    r.take_outbox(za);

    // A burst of writes into zone zb (notes) — pure zoned content. The za-only
    // subscriber receives *nothing*: no ops, and thus no clock jump or count signal
    // of zb's activity. Its stream is untouched.
    for i in 0..5 {
        write(
            &mut r,
            author,
            notes_write(&mut doc, format!("n{i}").as_bytes(), i),
        );
    }
    let got = r.take_outbox(za);
    assert!(
        got.is_empty(),
        "zone-zb activity delivers no frame at all to a zone-za subscriber: {got:?}"
    );
}

#[test]
fn a_no_zones_room_regresses_identically() {
    // A room bound to a schema with no `zones` block is one implicit root partition:
    // a whole-room subscribe carries everything, exactly as before zones existed.
    let mut r = registry();
    let author = auth(&mut r, 1, "c-author", APP_UNZONED);
    subscribe(&mut r, author, b"");
    r.take_outbox(author);
    let mut doc = Document::new(cid(1));
    doc.set_schema(Schema::parse(UNZONED).unwrap());
    let setup = doc.transact(|tx| {
        tx.map(b"board").register(b"bseed", Scalar::Int(1));
        tx.map(b"notes").register(b"nseed", Scalar::Int(1));
    });
    write(&mut r, author, setup);

    let reader = auth(&mut r, 2, "c-za", APP_UNZONED);
    let catchup = subscribe(&mut r, reader, b"");
    let catch_ops: Vec<Op> = catchup
        .into_iter()
        .flat_map(|m| match m {
            Message::Ops { ops, .. } => ops,
            _ => Vec::new(),
        })
        .collect();
    assert!(has_key(&catch_ops, b"bseed") && has_key(&catch_ops, b"nseed"));

    // A named-zone subscribe against a zoneless room selects a partition that does
    // not exist — refused generically, the zoneless room indistinguishable from one
    // that hides the named zone.
    let probe = auth(&mut r, 3, "c-za2", APP_UNZONED);
    match subscribe(&mut r, probe, b"za").as_slice() {
        [Message::Error { code, .. }] => assert_eq!(*code, ErrorCode::Forbidden),
        other => panic!("expected Forbidden, got {other:?}"),
    }
}

#[test]
fn a_cold_start_snapshot_to_a_zone_limited_subscriber_omits_the_other_zone() {
    let (mut r, mut doc, author) = seeded();
    // More content in both zones, then compact so the log is below the floor and a
    // fresh join is served a snapshot rather than an op delta.
    write(&mut r, author, board_write(&mut doc, b"bk", 2));
    write(&mut r, author, notes_write(&mut doc, b"nk", 2));
    r.hub_mut().compact(ROOM).expect("compact");

    // A cold za-only subscriber (last_seen 0, below the floor) is served a snapshot.
    // The snapshot is projected to its authorized partitions: board's state is
    // present, notes' state wholly absent — no register, no key, no structure.
    let za = auth(&mut r, 2, "c-za", APP);
    let reply = subscribe(&mut r, za, b"za");
    let snapshot = reply
        .into_iter()
        .find_map(|m| match m {
            Message::Snapshot { state, .. } => Some(state),
            _ => None,
        })
        .expect("a below-floor join is served a snapshot");
    let projected = Document::decode_state(&snapshot).expect("projected snapshot decodes");

    // Board (za) survives with its content; notes (zb) is gone entirely — the slot
    // is not even present in the root map.
    let board = projected.get(b"board").expect("board present");
    let crdtsync_core::Element::Map(board) = board else {
        panic!("board is a map");
    };
    assert!(
        board.borrow().get(b"bseed").is_some(),
        "board keeps its zoned state"
    );
    assert!(
        board.borrow().get(b"bk").is_some(),
        "board keeps its later write"
    );
    assert!(
        projected.get(b"notes").is_none(),
        "notes is wholly absent from the snapshot"
    );
    assert!(
        projected.get(b"loose").is_some(),
        "the root partition survives"
    );

    // The hidden zone leaves no clock trace either: zb's per-zone clock is absent, so
    // the recipient cannot infer zb activity from a clock jump.
    assert_eq!(
        projected.zone_clock(Some(1)),
        0,
        "zb's clock is scrubbed to zero"
    );
}

#[test]
fn zone_streams_converge_for_an_authorized_subscriber() {
    // An authorized zone-za subscriber, fed its catch-up then the live stream,
    // materializes exactly the author's board subtree — the partition converges.
    let (mut r, mut doc, author) = seeded();
    let za = auth(&mut r, 2, "c-za", APP);
    let catchup = subscribe(&mut r, za, b"za");

    let mut replica = Document::new(cid(2));
    for m in catchup {
        if let Message::Ops { ops, .. } = m {
            for op in ops {
                replica.apply(&op);
            }
        }
    }
    for (key, v) in [(b"b1".as_slice(), 10), (b"b2".as_slice(), 20)] {
        write(&mut r, author, board_write(&mut doc, key, v));
    }
    for op in received_ops(&mut r, za) {
        replica.apply(&op);
    }

    // The replica's board subtree matches the author's.
    let author_board = doc.get(b"board").expect("author board");
    let replica_board = replica.get(b"board").expect("replica board");
    let (crdtsync_core::Element::Map(a), crdtsync_core::Element::Map(b)) =
        (author_board, replica_board)
    else {
        panic!("both boards are maps");
    };
    for key in [b"bseed".as_slice(), b"b1", b"b2"] {
        assert_eq!(
            b.borrow().get(key).is_some(),
            a.borrow().get(key).is_some(),
            "board key {key:?} converges"
        );
        assert!(
            b.borrow().get(key).is_some(),
            "replica has board key {key:?}"
        );
    }
}

#[test]
fn an_acl_zone_deny_isolates_the_partition_through_the_shipped_policy() {
    // Zone isolation is enforceable by the built-in Acl, not only a custom
    // authorizer: a `deny` on `Resource::Zone` carves the partition out of an
    // otherwise room-readable actor, while a zone with no rule inherits the room's
    // read verdict (visible by default within a readable room).
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, ZONED.as_bytes(), b"").unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens()));
    r.set_authorizer(Box::new(
        Acl::new()
            .allow(
                Subject::Actor(b"author".to_vec()),
                None,
                ResourceMatch::AnyRoom,
            )
            .allow(
                Subject::Actor(b"partial".to_vec()),
                Some(Action::Read),
                ResourceMatch::AnyRoom,
            )
            .deny(
                Subject::Actor(b"partial".to_vec()),
                Some(Action::Read),
                ResourceMatch::Zone {
                    room: ROOM.to_vec(),
                    zone: b"zb".to_vec(),
                },
            ),
    ));
    r.set_clock(Arc::new(ManualClock::new(0)));

    // Author bootstraps the room and seeds each zone.
    let author = auth(&mut r, 1, "c-author", APP);
    subscribe(&mut r, author, b"");
    r.take_outbox(author);
    let mut doc = Document::new(cid(1));
    doc.set_schema(zoned_schema());
    let setup = doc.transact(|tx| {
        tx.map(b"board").register(b"bseed", Scalar::Int(1));
        tx.map(b"notes").register(b"nseed", Scalar::Int(1));
        tx.map(b"loose").register(b"lseed", Scalar::Int(1));
    });
    write(&mut r, author, setup);

    // `partial` may read the room, is denied zone zb, and has no rule for za. Its
    // whole-room catch-up carries za (abstain → room-read) plus the root partition,
    // never zb.
    let partial = auth(&mut r, 2, "c-partial", APP);
    let catchup = subscribe(&mut r, partial, b"");
    let ops: Vec<Op> = catchup
        .into_iter()
        .flat_map(|m| match m {
            Message::Ops { ops, .. } => ops,
            _ => Vec::new(),
        })
        .collect();
    assert!(has_key(&ops, b"bseed"), "za inherits the room read verdict");
    assert!(has_key(&ops, b"lseed"), "the root partition is visible");
    assert!(
        !has_key(&ops, b"nseed"),
        "the Acl-denied zone is wholly absent"
    );

    // A direct subscribe to the denied zone is refused generically.
    let probe = auth(&mut r, 3, "c-partial", APP);
    match subscribe(&mut r, probe, b"zb").as_slice() {
        [Message::Error { code, .. }] => assert_eq!(*code, ErrorCode::Forbidden),
        other => panic!("expected Forbidden, got {other:?}"),
    }
}
