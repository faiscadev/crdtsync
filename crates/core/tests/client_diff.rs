//! Client session — the diff-query view and issue method.
//!
//! A [`ClientSession`] frames a diff request keyed by channel — a change list
//! carries the room's own paths and values, so the server resolves the room and
//! the scope it narrows to from the subscription — and folds the server's
//! `DiffResult` reply into a per-room change-list view keyed by that resolved room.
//! A malformed change payload is refused without touching the view; a diff-query
//! frame that arrives from the server (they only travel client-to-server) is
//! refused.

use crdtsync_core::client::{ClientError, ClientSession};
use crdtsync_core::diff::{encode_changes, Change};
use crdtsync_core::path::encode_path;
use crdtsync_core::{Channel, ClientId, DiffKind, Message, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-a";

fn value_change() -> Change {
    Change::Value {
        path: encode_path(&[b"age"]),
        old: Scalar::Int(30),
        new: Scalar::Int(40),
    }
}

#[test]
fn diff_query_frames_a_channel_keyed_request() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM);
    assert!(matches!(
        s.diff_query(ch, DiffKind::Versions, b"v1", b"v2"),
        Some(Message::DiffQuery { channel, kind: DiffKind::Versions, a, b })
            if channel == ch && a == b"v1" && b == b"v2"
    ));
    assert!(matches!(
        s.diff_query(ch, DiffKind::Branches, b"main", b"draft"),
        Some(Message::DiffQuery { channel, kind: DiffKind::Branches, a, b })
            if channel == ch && a == b"main" && b == b"draft"
    ));
}

#[test]
fn a_diff_query_on_an_unheld_channel_frames_nothing() {
    // The server answers a query on a channel this connection never bound with a
    // protocol violation, which closes the session — so the frame is refused here,
    // exactly as a version fetch on an unheld channel is.
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM);
    assert!(s
        .diff_query(Channel(ch.0 + 1), DiffKind::Versions, b"v1", b"v2")
        .is_none());
}

#[test]
fn a_diff_result_updates_the_per_room_view() {
    let mut s = ClientSession::new(cid(1));
    assert!(s.diff(ROOM).is_none(), "none until a reply arrives");

    s.receive(Message::DiffResult {
        room: ROOM.to_vec(),
        changes: encode_changes(&[value_change()]),
    })
    .unwrap();
    assert_eq!(s.diff(ROOM), Some([value_change()].as_slice()));

    // A later result replaces the room's view — a diff is a transient query.
    s.receive(Message::DiffResult {
        room: ROOM.to_vec(),
        changes: encode_changes(&[]),
    })
    .unwrap();
    assert_eq!(s.diff(ROOM), Some([].as_slice()), "an empty diff, not None");
}

#[test]
fn diff_views_are_isolated_per_room() {
    let mut s = ClientSession::new(cid(1));
    s.receive(Message::DiffResult {
        room: b"room-a".to_vec(),
        changes: encode_changes(&[value_change()]),
    })
    .unwrap();
    assert!(s.diff(b"room-a").is_some());
    assert!(s.diff(b"room-b").is_none(), "another room is untouched");
}

#[test]
fn a_malformed_change_payload_is_refused_without_touching_the_view() {
    let mut s = ClientSession::new(cid(1));
    assert_eq!(
        s.receive(Message::DiffResult {
            room: ROOM.to_vec(),
            changes: vec![0xFF, 0xFF, 0xFF],
        }),
        Err(ClientError::BadDiff)
    );
    assert!(s.diff(ROOM).is_none(), "a bad payload left no view");
}

#[test]
fn a_server_sent_diff_query_is_refused() {
    let mut s = ClientSession::new(cid(1));
    assert_eq!(
        s.receive(Message::DiffQuery {
            channel: Channel(0),
            kind: DiffKind::Versions,
            a: b"v1".to_vec(),
            b: b"v2".to_vec(),
        }),
        Err(ClientError::UnexpectedMessage(
            "server sent a branch or diff request"
        ))
    );
}
