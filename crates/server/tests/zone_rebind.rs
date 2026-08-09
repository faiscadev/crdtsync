//! A channel's zone scope against a schema that changes underneath it (C30).
//!
//! A zone id is the zone's *position* in the acting schema's order-preserving
//! `zones()` block, and the schema acting over a room is not pinned to the moment a
//! channel subscribed: `bind_room_app` lifts the room's governing version whenever a
//! newer client of the same app joins, and a room nothing had bound yet acquires a
//! governing schema the first time an enforcing client subscribes. A scope resolved
//! once, at Subscribe, therefore stops describing what it was resolved to describe —
//! most sharply where the room declared **no** zones at the time, so the channel held
//! no set at all and every partition the room later declares is served to it, denied
//! ones included.
//!
//! So a subscription carries the zone *name* it was admitted under and every seam
//! that narrows by zone — the live fan-out, the catch-up, the version fetch, the diff
//! query — resolves it again against the schema it is about to narrow with, and
//! re-gates each resolved zone on the deployment's current verdict. The reordering
//! and removal that would re-point an already-stamped id at a different partition are
//! refused a version earlier, at the registry (see `schema_registry.rs`), so within
//! one registered chain a `zones` block only ever grows.

use std::sync::{Arc, Mutex};

use crdtsync_core::protocol::Channel;
use crdtsync_core::{ClientId, Document, ErrorCode, Message, Op, OpKind, Scalar, Schema};
use crdtsync_server::{
    Action, ConnId, Identity, ManualClock, Registry, Resource, SchemaRegistry, StaticTokens,
};

const ROOM: &[u8] = b"room-r";
const APP: &[u8] = b"r";
/// An ungoverned room — materialized by a relay write, so nothing ever binds it.
const SRC: &[u8] = b"room-src";
/// A name a channel subscribes to before anything materializes under it.
const DST: &[u8] = b"room-dst";

/// v1 declares no zones at all: every slot is the one implicit root partition.
const V1: &str = r#"{
    "schema": "r", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Sect", "notes": "Sect", "loose": "Sect" } },
        "Sect": { "kind": "map" }
    }
}"#;

/// v2 partitions the same layout: `/board` → za, `/notes` → zb. An append over v1's
/// empty block, which is what the registry admits.
const V2: &str = r#"{
    "schema": "r", "version": 2, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Sect", "notes": "Sect", "loose": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "zones": { "za": "/board", "zb": "/notes" }
}"#;

/// v3 appends `zc` over `/loose`, leaving `za` and `zb` at their positions — the one
/// shape of `zones` change the registry admits.
const V3: &str = r#"{
    "schema": "r", "version": 3, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": {
            "board": "Sect", "notes": "Sect", "loose": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "zones": { "za": "/board", "zb": "/notes", "zc": "/loose" }
}"#;

/// The edges between them. Each pair declares the same types, so no edge carries a
/// step — the schema change is the `zones` block, which no op rewrite touches.
const EDGE_V2: &[u8] = br#"{ "from": 1, "to": 2, "steps": [] }"#;
const EDGE_V3: &[u8] = br#"{ "from": 2, "to": 3, "steps": [] }"#;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// Which zones each actor may read. `author` reads everything (it writes); `za` reads
/// za alone; `both` reads both. Room read admits every authenticated actor, so the
/// per-zone verdicts are what carve the isolation.
fn authorizer(id: &Identity, action: Action, res: &Resource) -> bool {
    let actor = id.actor();
    match res {
        Resource::Zone { zone, .. } => {
            let zone: &[u8] = zone;
            match actor {
                b"author" | b"both" => true,
                b"za" => zone == b"za",
                _ => false,
            }
        }
        _ => matches!(action, Action::Read) || actor == b"author",
    }
}

fn tokens() -> StaticTokens {
    let mut t = StaticTokens::new();
    for (cred, actor) in [
        ("c-author", "author"),
        ("c-za", "za"),
        ("c-both", "both"),
        ("c-relay", "za"),
    ] {
        t.insert(cred.as_bytes().to_vec(), actor.as_bytes().to_vec());
    }
    t
}

fn registry() -> Registry {
    let mut sr = SchemaRegistry::new();
    sr.register(APP, 1, V1.as_bytes(), b"").unwrap();
    sr.register(APP, 2, V2.as_bytes(), EDGE_V2).unwrap();
    sr.register(APP, 3, V3.as_bytes(), EDGE_V3).unwrap();
    let mut r = Registry::new(cid(0xFF));
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    r.set_verifier(Box::new(tokens()));
    r.set_authorizer(Box::new(authorizer));
    r.set_clock(Arc::new(ManualClock::new(0)));
    r
}

/// Hello (declaring `{APP, version}`; version 0 is a relay's empty app) + Auth.
fn auth(r: &mut Registry, client: u8, cred: &str, app: &[u8], version: u32) -> ConnId {
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
            credential: cred.as_bytes().to_vec(),
        }
    ));
    r.take_outbox(id);
    id
}

/// Subscribe `id` to the room on channel 0 with zone selector `zone`, returning the
/// reply frames. Every subscribe in this file is meant to be admitted, so a refusal
/// fails here rather than downstream: a denied subscribe leaves the channel unbound
/// and a channel that receives nothing satisfies every `!has_key` assertion, which is
/// the shape that would quietly hollow out these tests.
fn subscribe(r: &mut Registry, id: ConnId, zone: &[u8]) -> Vec<Message> {
    subscribe_room(r, id, ROOM, zone)
}

/// [`subscribe`] against a named room, for the tests that hold more than one.
fn subscribe_room(r: &mut Registry, id: ConnId, room: &[u8], zone: &[u8]) -> Vec<Message> {
    assert!(
        r.deliver(
            id,
            Message::Subscribe {
                channel: Channel(0),
                room: room.to_vec(),
                branch: Vec::new(),
                zone: zone.to_vec(),
                last_seen_seq: 0,
            },
        ),
        "the subscribe closed the connection",
    );
    let replies = r.take_outbox(id);
    assert!(
        !replies.iter().any(|m| matches!(m, Message::Error { .. })),
        "the subscribe was refused: {replies:?}",
    );
    replies
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

/// Every op `msgs` carried, across all channels.
fn ops_in(msgs: &[Message]) -> Vec<Op> {
    msgs.iter()
        .filter_map(|m| match m {
            Message::Ops { ops, .. } => Some(ops.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

fn received(r: &mut Registry, id: ConnId) -> Vec<Op> {
    let msgs = r.take_outbox(id);
    ops_in(&msgs)
}

/// Whether `ops` carry a `RegisterSet` of `key` — the marker each content write
/// leaves in its partition.
fn has_key(ops: &[Op], key: &[u8]) -> bool {
    ops.iter()
        .any(|op| matches!(&op.kind, OpKind::RegisterSet { key: k, .. } if k == key))
}

fn v2_schema() -> Schema {
    Schema::parse(V2).expect("v2 parses")
}

/// An author on v2 whose doc holds the three containers, and the registry it wrote
/// them into. The author subscribes first so its writes establish the room, then the
/// room is *rebound* down to v1 by nobody — v2 is where it starts in the tests that
/// take this; the lift tests build their own room.
fn v2_seeded() -> (Registry, Document, ConnId) {
    let mut r = registry();
    let author = auth(&mut r, 1, "c-author", APP, 2);
    subscribe(&mut r, author, b"");
    let mut doc = Document::new(cid(1));
    doc.set_schema(v2_schema());
    let setup = doc.transact(|tx| {
        tx.map(b"board").register(b"bseed", Scalar::Int(1));
        tx.map(b"notes").register(b"nseed", Scalar::Int(1));
        tx.map(b"loose").register(b"lseed", Scalar::Int(1));
    });
    write(&mut r, author, setup);
    (r, doc, author)
}

fn board_write(doc: &mut Document, key: &[u8]) -> Vec<Op> {
    doc.transact(|tx| {
        tx.map(b"board").register(key, Scalar::Int(2));
    })
}

fn notes_write(doc: &mut Document, key: &[u8]) -> Vec<Op> {
    doc.transact(|tx| {
        tx.map(b"notes").register(key, Scalar::Int(2));
    })
}

/// The reproduced shape. A channel that joined while the room's schema declared no
/// zones resolved to *no* scope — nothing to filter by — and a governing version that
/// then declares zones leaves it receiving every one of them, the ones its actor is
/// denied included. Re-resolving its whole-room selector against the acting schema
/// narrows it to what it may actually read.
#[test]
fn a_channel_bound_before_zones_were_declared_is_narrowed_once_they_are() {
    let mut r = registry();
    // The room comes up governed by v1, which partitions nothing.
    let founder = auth(&mut r, 1, "c-author", APP, 1);
    subscribe(&mut r, founder, b"");
    let mut doc = Document::new(cid(1));
    doc.set_schema(Schema::parse(V1).expect("v1 parses"));
    write(
        &mut r,
        founder,
        doc.transact(|tx| {
            tx.map(b"board").register(b"bseed", Scalar::Int(1));
            tx.map(b"notes").register(b"nseed", Scalar::Int(1));
            tx.map(b"loose").register(b"lseed", Scalar::Int(1));
        }),
    );

    // `za` joins while there are no zones to be scoped to, so it names the whole room
    // and is admitted to all of it — there is only the root partition.
    let za = auth(&mut r, 2, "c-za", APP, 1);
    subscribe(&mut r, za, b"");

    // A v2 client joins and lifts the room's governing version. `/notes` is now zone
    // zb, which `za` is denied.
    let newer = auth(&mut r, 3, "c-author", APP, 2);
    let mut v2doc = Document::new(cid(3));
    v2doc.set_schema(v2_schema());
    for op in ops_in(&subscribe(&mut r, newer, b"")) {
        v2doc.apply(&op);
    }
    r.take_outbox(za);

    write(&mut r, newer, board_write(&mut v2doc, b"bk"));
    let got = received(&mut r, za);
    assert!(
        has_key(&got, b"bk"),
        "the zone it may read still reaches the bound channel",
    );

    write(&mut r, newer, notes_write(&mut v2doc, b"nk"));
    let got = received(&mut r, za);
    assert!(
        !has_key(&got, b"nk"),
        "a channel bound before the zones existed was served a zone it is denied",
    );
}

/// The same lift, on the seam the filing reached it by: a room the channel joined
/// while nothing governed it at all. A relay connection binds no app, so the room
/// stays ungoverned until an enforcing client subscribes — and the relay's channel is
/// still narrowed by the zones that client's schema declares.
#[test]
fn a_channel_bound_before_the_room_had_a_schema_is_narrowed_once_it_does() {
    let mut r = registry();
    // A relay connection: no app, so its subscribe governs nothing.
    let relay = auth(&mut r, 2, "c-relay", b"", 0);
    subscribe(&mut r, relay, b"");

    // The first enforcing client binds the room to the zoned app.
    let author = auth(&mut r, 1, "c-author", APP, 2);
    subscribe(&mut r, author, b"");
    let mut doc = Document::new(cid(1));
    doc.set_schema(v2_schema());
    write(
        &mut r,
        author,
        doc.transact(|tx| {
            tx.map(b"board").register(b"bseed", Scalar::Int(1));
            tx.map(b"notes").register(b"nseed", Scalar::Int(1));
        }),
    );

    let got = received(&mut r, relay);
    assert!(
        has_key(&got, b"bseed"),
        "the relay channel keeps the zone its actor may read",
    );
    assert!(
        !has_key(&got, b"nseed"),
        "a channel bound while the room had no schema was served a zone it is denied",
    );
}

/// The authorizer half of the same staleness: the scope's *verdicts* are re-taken
/// too, so a `Resource::Zone` read the deployment revokes after Subscribe stops
/// reaching the channel it was revoked on, without waiting for a resubscribe.
#[test]
fn a_zone_read_revoked_after_subscribe_narrows_the_bound_channel() {
    let (mut r, mut doc, author) = v2_seeded();
    // `both` joins admitted to za and zb.
    let both = auth(&mut r, 2, "c-both", APP, 2);
    subscribe(&mut r, both, b"");
    r.take_outbox(both);

    write(&mut r, author, notes_write(&mut doc, b"before"));
    let got = received(&mut r, both);
    assert!(has_key(&got, b"before"), "zb reaches it while granted");

    // The deployment revokes zb from every actor but the author.
    r.set_authorizer(Box::new(
        |id: &Identity, action: Action, res: &Resource| match res {
            Resource::Zone { zone, .. } => {
                let zone: &[u8] = zone;
                id.actor() == b"author" || zone == b"za"
            }
            _ => matches!(action, Action::Read) || id.actor() == b"author",
        },
    ));

    write(&mut r, author, notes_write(&mut doc, b"after"));
    let got = received(&mut r, both);
    assert!(
        !has_key(&got, b"after"),
        "a revoked zone kept reaching a channel bound before the revoke",
    );

    write(&mut r, author, board_write(&mut doc, b"still"));
    let got = received(&mut r, both);
    assert!(
        has_key(&got, b"still"),
        "the zone it still reads is untouched by the revoke",
    );
}

/// A dormant sweep drops the room from the live app map while the room's own binding
/// survives on the hub, so the fan-out must recover it the way an authorizing frame
/// does. Otherwise the two disagree on whether the room declares partitions at all —
/// the subscribe narrows the channel and every write after it serves the channel
/// everything.
#[test]
fn a_dormant_rooms_fan_out_still_narrows_by_the_rooms_own_binding() {
    let mut r = registry();
    // An enforcing client binds the room and seeds both zones, then leaves.
    let founder = auth(&mut r, 1, "c-author", APP, 2);
    subscribe(&mut r, founder, b"");
    let mut doc = Document::new(cid(1));
    doc.set_schema(v2_schema());
    write(
        &mut r,
        founder,
        doc.transact(|tx| {
            tx.map(b"board").register(b"bseed", Scalar::Int(1));
            tx.map(b"notes").register(b"nseed", Scalar::Int(1));
        }),
    );
    r.disconnect(founder);
    // With neither presence nor a subscriber the room is dormant: the live app map
    // drops it, the hub keeps the room's own binding.
    r.sweep();

    // Two relay connections — neither binds an app, so the live map stays empty for
    // this room while they hold it.
    let reader = auth(&mut r, 2, "c-relay", b"", 0);
    subscribe(&mut r, reader, b"");
    let writer = auth(&mut r, 3, "c-author", b"", 0);
    let mut wdoc = Document::new(cid(3));
    wdoc.set_schema(v2_schema());
    for op in ops_in(&subscribe(&mut r, writer, b"")) {
        wdoc.apply(&op);
    }
    r.take_outbox(reader);
    write(&mut r, writer, notes_write(&mut wdoc, b"nk"));
    let got = received(&mut r, reader);
    assert!(
        !has_key(&got, b"nk"),
        "a dormant room's fan-out read no schema and served a denied zone",
    );

    write(&mut r, writer, board_write(&mut wdoc, b"bk"));
    let got = received(&mut r, reader);
    assert!(has_key(&got, b"bk"), "the zone it may read still arrives");
}

/// The control on the append axis: a named-zone channel keeps naming its zone across a
/// lift that appends another. The id it resolves to holds because the block only grows,
/// and the partition the lift declared is not one it asked for — `/loose` was the root
/// partition it received under v2 and is zone `zc` under v3, which its selector does
/// not name. A frozen set answers this one identically, which is the point: the fix
/// must not widen a named channel while it narrows the whole-room ones.
#[test]
fn a_named_zone_channel_keeps_its_partition_across_an_append() {
    let (mut r, mut doc, author) = v2_seeded();
    let za = auth(&mut r, 2, "c-za", APP, 2);
    let caught = ops_in(&subscribe(&mut r, za, b"za"));
    assert!(has_key(&caught, b"bseed"), "za's catch-up carries za");
    assert!(!has_key(&caught, b"nseed"), "and not zb");
    assert!(
        has_key(&caught, b"lseed"),
        "and the root partition, which /loose still is under v2",
    );

    // A v3 client lifts the room's governing version, appending `zc` over `/loose`.
    let newer = auth(&mut r, 3, "c-author", APP, 3);
    let mut v3doc = Document::new(cid(3));
    v3doc.set_schema(Schema::parse(V3).expect("v3 parses"));
    for op in ops_in(&subscribe(&mut r, newer, b"")) {
        v3doc.apply(&op);
    }
    r.take_outbox(za);

    write(&mut r, author, board_write(&mut doc, b"bk"));
    assert!(
        has_key(&received(&mut r, za), b"bk"),
        "the named zone's id survives the append",
    );
    write(&mut r, author, notes_write(&mut doc, b"nk"));
    assert!(!has_key(&received(&mut r, za), b"nk"), "zb stays withheld");
    write(
        &mut r,
        newer,
        v3doc.transact(|tx| {
            tx.map(b"loose").register(b"lk", Scalar::Int(3));
        }),
    );
    assert!(
        !has_key(&received(&mut r, za), b"lk"),
        "a partition the lift declared is not one this selector named, so it goes \
         where the root partition it used to be would not",
    );
}

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tempdir() -> TempDir {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("crdtsync-zone-rebind-{pid}-{n}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}

/// A room's binding is durable while the schema registry is in-memory, so a restart
/// before the app re-registers leaves the room bound to a version that resolves to
/// nothing — and a whole-room channel is served every partition until it does. The
/// registration that ends that window has to be picked up: caching the unresolved
/// answer would make a startup-order window permanent for the life of the process, on
/// exactly the rooms that most need the schema they are bound to.
#[cfg_attr(miri, ignore)] // drives the durable store on the filesystem
#[test]
fn a_version_registered_after_a_room_resolved_without_it_takes_effect() {
    let dir = tempdir();
    // First boot: the room binds to the zoned app and its content lands.
    {
        let store = crdtsync_server::store::Store::open(dir.path()).unwrap();
        let mut sr = SchemaRegistry::new();
        sr.register(APP, 1, V1.as_bytes(), b"").unwrap();
        sr.register(APP, 2, V2.as_bytes(), EDGE_V2).unwrap();
        let mut r = Registry::with_store(cid(0xFF), store).unwrap();
        r.set_schema_registry(Arc::new(Mutex::new(sr)));
        r.set_verifier(Box::new(tokens()));
        r.set_authorizer(Box::new(authorizer));
        r.set_clock(Arc::new(ManualClock::new(0)));
        let author = auth(&mut r, 1, "c-author", APP, 2);
        subscribe(&mut r, author, b"");
        let mut doc = Document::new(cid(1));
        doc.set_schema(v2_schema());
        write(
            &mut r,
            author,
            doc.transact(|tx| {
                tx.map(b"board").register(b"bseed", Scalar::Int(1));
                tx.map(b"notes").register(b"nseed", Scalar::Int(1));
            }),
        );
    }

    // Second boot: the binding comes back, the registry is empty.
    let shared = Arc::new(Mutex::new(SchemaRegistry::new()));
    let store = crdtsync_server::store::Store::open(dir.path()).unwrap();
    let mut r = Registry::with_store(cid(0xFF), store).unwrap();
    r.set_schema_registry(shared.clone());
    r.set_verifier(Box::new(tokens()));
    r.set_authorizer(Box::new(authorizer));
    r.set_clock(Arc::new(ManualClock::new(0)));

    // Relay connections, so nothing re-binds the room from a client's own app.
    let reader = auth(&mut r, 2, "c-relay", b"", 0);
    subscribe(&mut r, reader, b"");
    let writer = auth(&mut r, 3, "c-author", b"", 0);
    let mut wdoc = Document::new(cid(3));
    wdoc.set_schema(v2_schema());
    for op in ops_in(&subscribe(&mut r, writer, b"")) {
        wdoc.apply(&op);
    }
    // A write inside the window: the binding resolves to nothing, so nothing narrows.
    write(&mut r, writer, notes_write(&mut wdoc, b"during"));
    r.take_outbox(reader);

    // The app re-registers over the control plane, into the registry the data plane
    // shares.
    {
        let mut sr = shared.lock().unwrap();
        sr.register(APP, 1, V1.as_bytes(), b"").unwrap();
        sr.register(APP, 2, V2.as_bytes(), EDGE_V2).unwrap();
    }

    write(&mut r, writer, notes_write(&mut wdoc, b"after"));
    let got = received(&mut r, reader);
    assert!(
        !has_key(&got, b"after"),
        "the room resolved no schema once and kept that answer past the registration \
         that should have ended it",
    );
    write(&mut r, writer, board_write(&mut wdoc, b"bk"));
    assert!(
        has_key(&received(&mut r, reader), b"bk"),
        "the zone it may read arrives once the schema resolves",
    );
}

/// A channel that named a zone can outlive the binding that made the name mean
/// something, and the two seams then answer a state read and an op differently.
///
/// A subscribe binds a room name before anything materializes under it, so a channel
/// can be admitted to zone `za` against the *connection's* schema, and a clone from an
/// **ungoverned** source then removes that binding outright (`Hub::clone_room`'s
/// `None` arm, which exists so a caller cannot pick the schema its own clone is read
/// under). The channel now names a partition of a room that declares none. Its live
/// ops are filtered to the root partition — an op carries its partition in its
/// envelope — but the state seam has no schema to project by, so without a refusal it
/// hands over the whole cloned room, including the subtree that partition never held.
#[test]
fn a_zone_named_channel_whose_room_loses_its_binding_is_refused_a_state_read() {
    let mut r = registry();
    // An ungoverned source: a relay connection binds no app, so `src` materializes
    // with no governing schema. Its content is unpartitioned, which is exactly why
    // serving it to a zone-scoped channel is a leak — under the schema that channel
    // was admitted against, `/notes` is a partition it may not read.
    let srcw = auth(&mut r, 4, "c-author", b"", 0);
    subscribe_room(&mut r, srcw, SRC, b"");
    let mut srcdoc = Document::new(cid(4));
    write(
        &mut r,
        srcw,
        srcdoc.transact(|tx| {
            tx.map(b"board").register(b"bseed", Scalar::Int(1));
            tx.map(b"notes").register(b"nseed", Scalar::Int(1));
        }),
    );

    // `za` names a zone on a destination nothing has materialized. The room is
    // unbound, so the subscribe resolves against this connection's own schema — and
    // binds the room to it on the way out.
    let za = auth(&mut r, 2, "c-za", APP, 2);
    subscribe_room(&mut r, za, DST, b"za");
    // An author on the same name, to capture a version after the clone.
    let author = auth(&mut r, 1, "c-author", APP, 2);
    subscribe_room(&mut r, author, DST, b"");

    // The clone lands an ungoverned source on the bound name, which drops the binding.
    assert!(r.deliver(
        author,
        Message::CloneRoom {
            src: SRC.to_vec(),
            dst: DST.to_vec(),
        }
    ));
    let cloned = r.take_outbox(author);
    assert!(
        cloned
            .iter()
            .any(|m| matches!(m, Message::CloneRoomResult { created, .. } if *created)),
        "the clone landed: {cloned:?}",
    );

    // Capture a version of the cloned state to read back.
    assert!(r.deliver(
        author,
        Message::VersionCreate {
            channel: Channel(0),
            name: b"v1".to_vec(),
        }
    ));
    r.take_outbox(author);
    r.take_outbox(za);

    // The positive control, and what proves the version is the clone's content rather
    // than an empty document: the author's channel named no zone, so its scope is
    // `None` — not zone-limited, nothing to project — and it is served the bytes
    // verbatim. Those are the bytes `za` is refused, and they carry `/notes`.
    let whole = fetch_version(&mut r, author, b"v1");
    let whole = Document::decode_state(&whole.expect("the unscoped channel is served"))
        .expect("the served version state decodes");
    assert!(
        whole.get(b"board").is_some() && whole.get(b"notes").is_some(),
        "the version captured the cloned content",
    );

    let reply = fetch_version_reply(&mut r, za, b"v1");
    assert!(
        !reply
            .iter()
            .any(|m| matches!(m, Message::VersionState { .. })),
        "a zone-named channel was served state for a room that declares no \
         partitions, so nothing narrowed it: {reply:?}",
    );
    assert!(
        reply.iter().any(|m| matches!(
            m,
            Message::Error { code, message, .. }
                if *code == ErrorCode::Internal && message == "room schema is unavailable"
        )),
        "the unprojectable state read is refused, and for that reason: {reply:?}",
    );

    // The other half of the ruling, which is otherwise only prose: the *op* seam is
    // unaffected, because an op names its own partition. An unzoned write on the same
    // room still reaches the same channel while its state read is refused.
    let mut adoc = Document::new(cid(1));
    write(
        &mut r,
        author,
        adoc.transact(|tx| {
            tx.map(b"loose").register(b"lk", Scalar::Int(9));
        }),
    );
    assert!(
        has_key(&received(&mut r, za), b"lk"),
        "the root partition still reaches a channel whose state read is refused",
    );
}

/// The `VersionState` bytes `id` is served for `name`, or `None` if it was refused.
fn fetch_version(r: &mut Registry, id: ConnId, name: &[u8]) -> Option<Vec<u8>> {
    fetch_version_reply(r, id, name)
        .into_iter()
        .find_map(|m| match m {
            Message::VersionState { state, .. } => Some(state),
            _ => None,
        })
}

/// The raw reply frames for a version fetch, so a test can inspect a refusal.
fn fetch_version_reply(r: &mut Registry, id: ConnId, name: &[u8]) -> Vec<Message> {
    assert!(r.deliver(
        id,
        Message::VersionFetch {
            channel: Channel(0),
            name: name.to_vec(),
        }
    ));
    r.take_outbox(id)
}
