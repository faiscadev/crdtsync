//! Named versions — a versions index over the snapshot storage primitive.
//!
//! A named version captures the room's whole-replica state at the server
//! sequence it was taken, under an app-chosen name. Create, get its state (for
//! read / export / diff), list (sorted, for pagination), rename, and delete are
//! first-class. Later edits to the room never disturb an existing version — it is
//! a point-in-time snapshot, retained until the app deletes it.
//!
//! Restoring a version as live state is restore-as-branch, gated on the branch
//! layer; durable persistence of the index and auto-version triggers are
//! follow-ons. This suite covers the in-memory index over the merged replica.

use crdtsync_core::doc::Document;
use crdtsync_core::{ClientId, Element, Scalar};
use crdtsync_server::Hub;

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn hub() -> Hub {
    Hub::new(cid(0xFF))
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

const ROOM: &[u8] = b"room-a";

/// Ingest a register-write into the room, returning the value written.
fn write_age(h: &mut Hub, value: i64) {
    let ops = doc(1).transact(|tx| tx.register(b"age", Scalar::Int(value)));
    h.ingest(ROOM, ops).unwrap();
}

/// The `age` register value in a decoded version state.
fn age_in(state: &[u8]) -> i64 {
    let restored = Document::decode_state(state).unwrap();
    match restored.get(b"age") {
        Some(Element::Register(reg)) => match reg.borrow().read() {
            Scalar::Int(n) => *n,
            other => panic!("expected an int, got {other:?}"),
        },
        _ => panic!("expected the age register"),
    }
}

#[test]
fn a_created_version_captures_the_current_state_and_seq() {
    let mut h = hub();
    write_age(&mut h, 30);
    let seq = h.seq(ROOM);

    assert!(h.create_version(ROOM, b"v1"));
    assert_eq!(h.version_seq(ROOM, b"v1"), Some(seq));
    assert_eq!(age_in(h.version_state(ROOM, b"v1").unwrap()), 30);
}

#[test]
fn a_version_is_a_point_in_time_untouched_by_later_edits() {
    let mut h = hub();
    write_age(&mut h, 30);
    let at_v1 = h.seq(ROOM);
    assert!(h.create_version(ROOM, b"v1"));

    // The room moves on; the version does not.
    write_age(&mut h, 40);
    assert_eq!(h.version_seq(ROOM, b"v1"), Some(at_v1));
    assert_eq!(age_in(h.version_state(ROOM, b"v1").unwrap()), 30);
}

#[test]
fn a_duplicate_name_does_not_overwrite() {
    let mut h = hub();
    write_age(&mut h, 30);
    assert!(h.create_version(ROOM, b"v1"));

    write_age(&mut h, 40);
    assert!(!h.create_version(ROOM, b"v1"), "a taken name is refused");
    assert_eq!(
        age_in(h.version_state(ROOM, b"v1").unwrap()),
        30,
        "the original version is intact"
    );
}

#[test]
fn versions_list_sorted_for_pagination() {
    let mut h = hub();
    write_age(&mut h, 1);
    assert!(h.create_version(ROOM, b"v-c"));
    assert!(h.create_version(ROOM, b"v-a"));
    assert!(h.create_version(ROOM, b"v-b"));
    assert_eq!(
        h.version_names(ROOM),
        vec![b"v-a".to_vec(), b"v-b".to_vec(), b"v-c".to_vec()]
    );
}

#[test]
fn rename_moves_a_version_and_preserves_its_state() {
    let mut h = hub();
    write_age(&mut h, 30);
    assert!(h.create_version(ROOM, b"draft"));

    assert!(h.rename_version(ROOM, b"draft", b"final"));
    assert_eq!(h.version_seq(ROOM, b"draft"), None, "the old name is gone");
    assert_eq!(age_in(h.version_state(ROOM, b"final").unwrap()), 30);
}

#[test]
fn rename_refuses_an_absent_source_or_a_taken_target() {
    let mut h = hub();
    write_age(&mut h, 30);
    assert!(h.create_version(ROOM, b"a"));
    assert!(h.create_version(ROOM, b"b"));

    assert!(!h.rename_version(ROOM, b"missing", b"c"), "absent source");
    assert!(!h.rename_version(ROOM, b"a", b"b"), "taken target");
    assert_eq!(h.version_names(ROOM), vec![b"a".to_vec(), b"b".to_vec()]);
}

#[test]
fn delete_removes_a_version() {
    let mut h = hub();
    write_age(&mut h, 30);
    assert!(h.create_version(ROOM, b"v1"));

    assert!(h.delete_version(ROOM, b"v1"));
    assert_eq!(h.version_seq(ROOM, b"v1"), None);
    assert!(!h.delete_version(ROOM, b"v1"), "a second delete is a no-op");
}

#[test]
fn an_unknown_room_has_no_versions() {
    let mut h = hub();
    assert!(
        !h.create_version(b"ghost", b"v1"),
        "a room with no state cannot be versioned"
    );
    assert_eq!(h.version_seq(b"ghost", b"v1"), None);
    assert!(h.version_state(b"ghost", b"v1").is_none());
    assert!(h.version_names(b"ghost").is_empty());
}

#[test]
fn versions_are_isolated_per_room() {
    let mut h = hub();
    write_age(&mut h, 30);
    let other: &[u8] = b"room-b";
    h.ingest(
        other,
        doc(2).transact(|tx| tx.register(b"age", Scalar::Int(99))),
    )
    .unwrap();

    assert!(h.create_version(ROOM, b"v1"));
    assert!(h.create_version(other, b"v1"));

    assert_eq!(age_in(h.version_state(ROOM, b"v1").unwrap()), 30);
    assert_eq!(age_in(h.version_state(other, b"v1").unwrap()), 99);
    assert_eq!(h.version_names(ROOM), vec![b"v1".to_vec()]);
}
