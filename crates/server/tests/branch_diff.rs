//! Schema-aware diff between two branch/version snapshots.
//!
//! A version or branch names a whole-replica state. The hub decodes two of them
//! and runs the core diff engine over the pair, returning the structural change
//! list — a value change, a text edit, a structural add/remove — the same
//! `Change` set the engine renders. Diffing a snapshot against itself is empty;
//! an unknown version or branch is a clean error, never a panic.

use crdtsync_core::diff::{Change, SeqItem};
use crdtsync_core::element::ElementKind;
use crdtsync_core::path::encode_path;
use crdtsync_core::{ClientId, Document, Op, Scalar};
use crdtsync_server::{DiffError, Hub};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-1";

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

fn reg(d: &mut Document, key: &[u8], value: i64) -> Vec<Op> {
    d.transact(|tx| tx.register(key, Scalar::Int(value)))
}

fn p(keys: &[&[u8]]) -> Vec<u8> {
    encode_path(keys)
}

#[test]
fn diff_versions_reports_a_value_change() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);

    hub.ingest(ROOM, reg(&mut main, b"age", 30), None).unwrap();
    assert!(hub.create_version(ROOM, b"v1").unwrap());
    hub.ingest(ROOM, reg(&mut main, b"age", 40), None).unwrap();
    assert!(hub.create_version(ROOM, b"v2").unwrap());

    let changes = hub.diff_versions(ROOM, b"v1", b"v2").unwrap();
    assert_eq!(
        changes,
        vec![Change::Value {
            path: p(&[b"age"]),
            old: Scalar::Int(30),
            new: Scalar::Int(40),
        }]
    );
}

#[test]
fn diff_versions_reports_a_text_edit() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);

    let insert = main.transact(|tx| tx.text(b"body").insert(0, "hi"));
    hub.ingest(ROOM, insert, None).unwrap();
    assert!(hub.create_version(ROOM, b"v1").unwrap());
    let edit = main.transact(|tx| tx.text(b"body").insert(2, "!"));
    hub.ingest(ROOM, edit, None).unwrap();
    assert!(hub.create_version(ROOM, b"v2").unwrap());

    let changes = hub.diff_versions(ROOM, b"v1", b"v2").unwrap();
    assert_eq!(
        changes,
        vec![Change::TextInsert {
            path: p(&[b"body"]),
            index: 2,
            text: "!".to_string(),
        }]
    );
}

#[test]
fn diff_versions_reports_a_structural_add() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);

    hub.ingest(ROOM, reg(&mut main, b"age", 30), None).unwrap();
    assert!(hub.create_version(ROOM, b"v1").unwrap());
    let add = main.transact(|tx| {
        tx.map(b"meta").register(b"lang", Scalar::Int(1));
    });
    hub.ingest(ROOM, add, None).unwrap();
    assert!(hub.create_version(ROOM, b"v2").unwrap());

    let changes = hub.diff_versions(ROOM, b"v1", b"v2").unwrap();
    assert!(
        changes.contains(&Change::Added {
            path: p(&[b"meta"]),
            kind: ElementKind::Map,
        }),
        "expected a structural add of the meta map, got {changes:?}"
    );
}

#[test]
fn diffing_a_version_against_itself_is_empty() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);

    hub.ingest(ROOM, reg(&mut main, b"age", 30), None).unwrap();
    assert!(hub.create_version(ROOM, b"v1").unwrap());

    assert_eq!(hub.diff_versions(ROOM, b"v1", b"v1").unwrap(), Vec::new());
}

#[test]
fn an_unknown_version_is_a_clean_error() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);

    hub.ingest(ROOM, reg(&mut main, b"age", 30), None).unwrap();
    assert!(hub.create_version(ROOM, b"v1").unwrap());

    assert_eq!(
        hub.diff_versions(ROOM, b"v1", b"nope"),
        Err(DiffError::UnknownVersion(b"nope".to_vec()))
    );
    assert_eq!(
        hub.diff_versions(ROOM, b"gone", b"v1"),
        Err(DiffError::UnknownVersion(b"gone".to_vec()))
    );
}

#[test]
fn diff_branches_reports_only_the_divergence_from_the_fork_source() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);

    hub.ingest(ROOM, reg(&mut main, b"age", 30), None).unwrap();
    let fork = hub.seq(ROOM);
    assert!(hub.fork_branch(ROOM, b"draft", b"main", fork).unwrap());

    let mut draft = doc(2);
    hub.ingest_branch(ROOM, b"draft", reg(&mut draft, b"age", 99), None)
        .unwrap();

    let changes = hub.diff_branches(ROOM, b"main", b"draft").unwrap();
    assert_eq!(
        changes,
        vec![Change::Value {
            path: p(&[b"age"]),
            old: Scalar::Int(30),
            new: Scalar::Int(99),
        }]
    );
}

#[test]
fn a_branch_against_itself_is_empty() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);

    hub.ingest(ROOM, reg(&mut main, b"age", 30), None).unwrap();
    let fork = hub.seq(ROOM);
    assert!(hub.fork_branch(ROOM, b"draft", b"main", fork).unwrap());

    assert_eq!(
        hub.diff_branches(ROOM, b"draft", b"draft").unwrap(),
        Vec::new()
    );
}

#[test]
fn an_unknown_branch_is_a_clean_error() {
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);

    hub.ingest(ROOM, reg(&mut main, b"age", 30), None).unwrap();

    assert_eq!(
        hub.diff_branches(ROOM, b"main", b"ghost"),
        Err(DiffError::UnknownBranch(b"ghost".to_vec()))
    );
}

#[test]
fn diff_versions_reports_a_list_insert_into_an_existing_list() {
    // A run appended to a list that already existed at the fork diffs as a
    // ListInsert — the same run the renderer prints — so the server forwards the
    // core vocabulary unchanged rather than collapsing it to a composite add.
    let mut hub = Hub::new(cid(0xFF));
    let mut main = doc(1);

    let seed = main.transact(|tx| {
        tx.list(b"xs").insert(0, Scalar::Int(1));
    });
    hub.ingest(ROOM, seed, None).unwrap();
    assert!(hub.create_version(ROOM, b"v1").unwrap());
    let push = main.transact(|tx| {
        tx.list(b"xs").insert(1, Scalar::Int(7));
    });
    hub.ingest(ROOM, push, None).unwrap();
    assert!(hub.create_version(ROOM, b"v2").unwrap());

    let changes = hub.diff_versions(ROOM, b"v1", b"v2").unwrap();
    assert!(
        changes.contains(&Change::ListInsert {
            path: p(&[b"xs"]),
            index: 1,
            items: vec![SeqItem::Scalar(Scalar::Int(7))],
        }),
        "expected the list insert to survive the round trip, got {changes:?}"
    );
}
