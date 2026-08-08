//! Client session — the diff-query view and issue method.
//!
//! A [`ClientSession`] frames a diff request keyed by channel — a change list
//! carries the room's own paths and values, so the server resolves the room and
//! the scope it narrows to from the subscription — and folds the server's
//! `DiffResult` reply into the answering channel's change-list view. The reply is
//! keyed by the channel that asked, so two channels of one room, each narrowed to
//! its own zone scope, read back their own answers. A malformed change payload is
//! refused without touching the view; a result on a channel this session does not
//! hold is refused; a diff-query frame that arrives from the server (they only
//! travel client-to-server) is refused.

use crdtsync_core::client::{ClientError, ClientSession};
use crdtsync_core::diff::{encode_changes, Change};
use crdtsync_core::path::encode_path;
use crdtsync_core::{Channel, ClientId, DiffKind, ElementKind, Message, Scalar};

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

fn added_change(name: &[u8]) -> Change {
    Change::Added {
        path: encode_path(&[name]),
        kind: ElementKind::Map,
    }
}

#[test]
fn diff_query_frames_a_channel_keyed_request() {
    let mut s = ClientSession::new(cid(1));
    let (one, _) = s.subscribe(ROOM).unwrap();
    // A second subscription, so the frame carrying the channel it was *asked* for is
    // told from a frame that hardcodes the first one.
    let (two, _) = s.subscribe(b"room-b").unwrap();
    assert_ne!(one, two);
    assert!(matches!(
        s.diff_query(one, DiffKind::Versions, b"v1", b"v2"),
        Some(Message::DiffQuery { channel, kind: DiffKind::Versions, a, b })
            if channel == one && a == b"v1" && b == b"v2"
    ));
    assert!(matches!(
        s.diff_query(two, DiffKind::Branches, b"main", b"draft"),
        Some(Message::DiffQuery { channel, kind: DiffKind::Branches, a, b })
            if channel == two && a == b"main" && b == b"draft"
    ));
}

#[test]
fn a_diff_query_on_an_unheld_channel_frames_nothing() {
    // The server answers a query on a channel this connection never bound with a
    // protocol violation, which closes the session — so the frame is refused here,
    // exactly as a version fetch on an unheld channel is.
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM).unwrap();
    // Channels are handed out monotonically and never reused, so the next number is
    // one this session has not assigned.
    assert!(s
        .diff_query(Channel(ch.0 + 1), DiffKind::Versions, b"v1", b"v2")
        .is_none());
}

#[test]
fn a_diff_result_updates_the_answering_channel_view() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM).unwrap();
    assert!(
        s.diff(ch).is_none(),
        "a view existed before any reply arrived"
    );

    s.receive(Message::DiffResult {
        channel: ch,
        changes: encode_changes(&[value_change()]),
    })
    .unwrap();
    assert_eq!(s.diff(ch), Some([value_change()].as_slice()));

    // A later result replaces the channel's view — a diff is a transient query.
    s.receive(Message::DiffResult {
        channel: ch,
        changes: encode_changes(&[]),
    })
    .unwrap();
    assert_eq!(
        s.diff(ch),
        Some([].as_slice()),
        "an empty diff did not replace the view"
    );
}

#[test]
fn two_channels_on_one_room_read_back_their_own_diffs() {
    // The point of a channel-keyed reply: a wide channel and a zone-narrowed one on
    // the same room are served genuinely different change lists, and each reader gets
    // the answer served to *its* channel, not the last reply to arrive.
    let mut s = ClientSession::new(cid(1));
    let (wide, _) = s.subscribe(ROOM).unwrap();
    let (narrow, _) = s.subscribe_zone(ROOM, b"zone-b").unwrap();
    assert_ne!(wide, narrow);

    let wide_changes = [added_change(b"root-field"), added_change(b"zone-b-field")];
    let narrow_changes = [added_change(b"zone-b-field")];

    s.receive(Message::DiffResult {
        channel: wide,
        changes: encode_changes(&wide_changes),
    })
    .unwrap();
    s.receive(Message::DiffResult {
        channel: narrow,
        changes: encode_changes(&narrow_changes),
    })
    .unwrap();

    assert_eq!(
        s.diff(wide),
        Some(wide_changes.as_slice()),
        "the narrow reply overwrote the wide channel's answer"
    );
    assert_eq!(s.diff(narrow), Some(narrow_changes.as_slice()));

    // And in the other arrival order, over a fresh pair of answers — the wide reply
    // lands last and still lands on its own channel.
    let wide_again = [added_change(b"root-field")];
    let narrow_again = [added_change(b"zone-b-other")];
    s.receive(Message::DiffResult {
        channel: narrow,
        changes: encode_changes(&narrow_again),
    })
    .unwrap();
    s.receive(Message::DiffResult {
        channel: wide,
        changes: encode_changes(&wide_again),
    })
    .unwrap();
    assert_eq!(s.diff(narrow), Some(narrow_again.as_slice()));
    assert_eq!(s.diff(wide), Some(wide_again.as_slice()));
}

#[test]
fn diff_views_are_isolated_per_channel() {
    let mut s = ClientSession::new(cid(1));
    let (one, _) = s.subscribe(ROOM).unwrap();
    // A second channel on the *same* room, so isolation is the channel's doing and
    // not the room's.
    let (two, _) = s.subscribe(ROOM).unwrap();
    s.receive(Message::DiffResult {
        channel: one,
        changes: encode_changes(&[value_change()]),
    })
    .unwrap();
    assert!(s.diff(one).is_some());
    assert!(
        s.diff(two).is_none(),
        "the other channel's view was written"
    );
}

#[test]
fn a_diff_result_on_an_unheld_channel_is_refused() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM).unwrap();
    s.receive(Message::DiffResult {
        channel: ch,
        changes: encode_changes(&[value_change()]),
    })
    .unwrap();
    let unheld = Channel(ch.0 + 1);
    assert_eq!(
        s.receive(Message::DiffResult {
            channel: unheld,
            changes: encode_changes(&[]),
        }),
        Err(ClientError::UnknownChannel(unheld))
    );
    assert_eq!(
        s.diff(ch),
        Some([value_change()].as_slice()),
        "the held channel's answer was overwritten",
    );
    assert!(s.diff(unheld).is_none());
}

#[test]
fn an_unheld_channel_is_refused_before_the_payload_is_read() {
    // The channel names the view a result belongs to, so a result with nowhere to
    // land is refused as an unknown channel — the payload is never read.
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM).unwrap();
    let unheld = Channel(ch.0 + 1);
    assert_eq!(
        s.receive(Message::DiffResult {
            channel: unheld,
            changes: vec![0xFF, 0xFF, 0xFF],
        }),
        Err(ClientError::UnknownChannel(unheld))
    );
}

#[test]
fn a_malformed_change_payload_is_refused_without_touching_the_view() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM).unwrap();
    assert_eq!(
        s.receive(Message::DiffResult {
            channel: ch,
            changes: vec![0xFF, 0xFF, 0xFF],
        }),
        Err(ClientError::BadDiff)
    );
    assert!(s.diff(ch).is_none(), "a bad payload wrote a view");
}

#[test]
fn unsubscribing_drops_the_channel_diff_view() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM).unwrap();
    s.receive(Message::DiffResult {
        channel: ch,
        changes: encode_changes(&[value_change()]),
    })
    .unwrap();
    s.unsubscribe(ch);
    assert!(
        s.diff(ch).is_none(),
        "the retired channel still holds an answer"
    );
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
