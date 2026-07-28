// Tampers with the durable versions file, which Miri does not model.
#![cfg(not(miri))]

//! A version whose captured state does not decode is refused, not served raw (C15).
//!
//! Version bytes come back off durable storage, so — unlike a snapshot materialized
//! in the same instant by this build — failing to decode one is reachable: a codec
//! revision, or a damaged file, leaves the live room fine and the archive unreadable.
//! Undecodable is unprojectable, and unprojected bytes still carry everything a
//! redaction would have cut, so a reader any redaction could apply to is refused.
//! A reader entitled to the room whole is served them, exactly as before.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crdtsync_core::acl::{AclGrant, AclSubject, Capability};
use crdtsync_core::path::encode_path;
use crdtsync_core::protocol::Channel;
use crdtsync_core::{AclEffect, ClientId, Document, ErrorCode, Message, Op, Scalar, Schema};
use crdtsync_server::acl::actor_key;
use crdtsync_server::{
    Action, ConnId, Identity, Registry, Resource, SchemaRegistry, StaticTokens, Store,
};

const ROOM: &[u8] = b"room-u";
const ZONE_APP: &[u8] = b"z";
const TUPLE_APP: &[u8] = b"t";
const CH: Channel = Channel(0);
const V1: &[u8] = b"v1";

/// One zoned map subtree (`/board` → za) beside a hidden one (`/notes` → zb).
const ZONED: &str = r#"{
    "schema": "z", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "board": "Sect", "notes": "Sect" } },
        "Sect": { "kind": "map" }
    },
    "zones": { "za": "/board", "zb": "/notes" }
}"#;

/// The same shape with no zones, so only the doc-ACL half of the guard can fire.
const TUPLED: &str = r#"{
    "schema": "t", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "board": "Sect", "notes": "Sect" } },
        "Sect": { "kind": "map" }
    }
}"#;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// `author` reaches both zones, `reader` only za.
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

fn wire(r: &mut Registry) {
    let mut sr = SchemaRegistry::new();
    sr.register(ZONE_APP, 1, ZONED.as_bytes(), b"").unwrap();
    sr.register(TUPLE_APP, 1, TUPLED.as_bytes(), b"").unwrap();
    r.set_schema_registry(Arc::new(Mutex::new(sr)));
    let mut t = StaticTokens::new();
    for (credential, actor) in [("c-author", "author"), ("c-reader", "reader")] {
        t.insert(credential.as_bytes().to_vec(), actor.as_bytes().to_vec());
    }
    r.set_verifier(Box::new(t));
    r.set_authorizer(Box::new(zone_authorizer));
}

/// Hello + Auth + Subscribe to `zone`, holding the room on `CH`.
fn joined(r: &mut Registry, client: u8, credential: &str, app: &[u8], zone: &[u8]) -> ConnId {
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
    assert!(r.deliver(
        id,
        Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            branch: Vec::new(),
            zone: zone.to_vec(),
            last_seen_seq: 0,
        }
    ));
    r.take_outbox(id);
    id
}

fn submit(r: &mut Registry, id: ConnId, ops: Vec<Op>) {
    assert!(r.deliver(id, Message::Ops { channel: CH, ops }));
    r.take_outbox(id);
}

fn fetch(r: &mut Registry, id: ConnId) -> Vec<Message> {
    assert!(r.deliver(
        id,
        Message::VersionFetch {
            channel: CH,
            name: V1.to_vec(),
        }
    ));
    r.take_outbox(id)
}

fn store_at(dir: &Path) -> Registry {
    let store = Store::open(dir).expect("the store opens");
    let mut r = Registry::with_store(cid(0xFF), store).expect("the registry loads");
    wire(&mut r);
    r
}

/// Overwrite the room's versions file with one record carrying `state` — the same
/// framing `Store::write_versions` lays down: `name`, seq, no auto-version origin,
/// ordinal, `state`, each byte string length-prefixed `u32` little-endian.
fn rewrite_version_state(dir: &Path, state: &[u8]) {
    let mut buf = Vec::new();
    let put = |buf: &mut Vec<u8>, bytes: &[u8]| {
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(bytes);
    };
    put(&mut buf, V1);
    buf.extend_from_slice(&1u64.to_le_bytes());
    buf.push(0);
    buf.extend_from_slice(&0u64.to_le_bytes());
    put(&mut buf, state);

    let path = fs::read_dir(dir)
        .expect("the store dir reads")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().is_some_and(|e| e == "versions"))
        .expect("the room's versions file exists");
    fs::write(path, buf).expect("the versions file rewrites");
}

/// A store-backed room under `app`, seeded and versioned, then damaged so that
/// version's captured state no longer decodes. `tuple` adds a doc-ACL record, the
/// other half of what makes a version narrowable. Returns the reopened registry.
fn damaged_room(dir: &Path, app: &[u8], src: &str, tuple: bool) -> Registry {
    {
        let mut r = store_at(dir);
        let author = joined(&mut r, 1, "c-author", app, b"");
        let mut doc = Document::new(cid(1));
        doc.set_schema(Schema::parse(src).expect("the schema parses"));
        submit(
            &mut r,
            author,
            doc.transact(|tx| {
                tx.map(b"board").register(b"bseed", Scalar::Int(0));
                tx.map(b"notes").register(b"nseed", Scalar::Int(0));
            }),
        );
        if tuple {
            submit(
                &mut r,
                author,
                doc.transact(|tx| {
                    tx.acl().grant(
                        AclSubject::Actor(actor_key(b"reader")),
                        AclGrant::Capability(Capability::Read),
                        AclEffect::Allow,
                        encode_path(&[b"board"]),
                        actor_key(b"author"),
                    );
                }),
            );
        }
        assert!(r.deliver(
            author,
            Message::VersionCreate {
                channel: CH,
                name: V1.to_vec(),
            }
        ));
        r.take_outbox(author);
    }
    rewrite_version_state(dir, b"not a document");
    store_at(dir)
}

#[test]
fn a_zone_limited_reader_is_refused_an_undecodable_version() {
    let dir = tempdir();
    let mut r = damaged_room(dir.path(), ZONE_APP, ZONED, false);
    let reader = joined(&mut r, 2, "c-reader", ZONE_APP, b"za");

    match &fetch(&mut r, reader)[..] {
        [Message::Error { code, .. }] => assert_eq!(*code, ErrorCode::Internal),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn a_whole_room_reader_is_still_served_an_undecodable_version() {
    // The refusal is scoped to readers a redaction could apply to — nothing else
    // becomes unavailable because one archived state stopped decoding.
    let dir = tempdir();
    let mut r = damaged_room(dir.path(), ZONE_APP, ZONED, false);
    let author = joined(&mut r, 1, "c-author", ZONE_APP, b"");

    match &fetch(&mut r, author)[..] {
        [Message::VersionState { state, .. }] => assert_eq!(state, b"not a document"),
        other => panic!("expected the version's state, got {other:?}"),
    }
}

#[test]
fn a_reader_of_a_room_holding_doc_acl_state_is_refused_an_undecodable_version() {
    // The other half of "narrowable": no zones at all, one doc-ACL tuple. The guard
    // asks whether a redaction *could* apply to these bytes, not whether it would
    // have cut anything — an unreadable state cannot answer the second question.
    let dir = tempdir();
    let mut r = damaged_room(dir.path(), TUPLE_APP, TUPLED, true);
    let reader = joined(&mut r, 2, "c-reader", TUPLE_APP, b"");

    match &fetch(&mut r, reader)[..] {
        [Message::Error { code, .. }] => assert_eq!(*code, ErrorCode::Internal),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn a_zoneless_tuple_free_room_still_serves_its_version() {
    // And the no-redaction-possible path is untouched: no zones, no doc-ACL state,
    // the captured bytes as they always were.
    let dir = tempdir();
    let mut r = damaged_room(dir.path(), TUPLE_APP, TUPLED, false);
    let reader = joined(&mut r, 2, "c-reader", TUPLE_APP, b"");

    match &fetch(&mut r, reader)[..] {
        [Message::VersionState { state, .. }] => assert_eq!(state, b"not a document"),
        other => panic!("expected the version's state, got {other:?}"),
    }
}

// --- a tempdir without pulling in a dev-dependency ---

struct TempDir(PathBuf);

impl TempDir {
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn tempdir() -> TempDir {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("crdtsync-version-unreadable-{pid}-{n}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}
