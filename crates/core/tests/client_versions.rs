//! Client session — the named-version view and issue methods.
//!
//! A [`ClientSession`] frames version requests (create / rename / delete / list
//! / fetch) on a held channel and folds the server's replies into a per-room
//! view: a `Versions` list replaces the known names; a `VersionState` caches a
//! fetched version's bytes under its name. A frame for a channel the session
//! does not hold is refused.

use crdtsync_core::client::{ClientError, ClientSession};
use crdtsync_core::{Channel, ClientId, Document, Element, Message, Scalar};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM_A: &[u8] = b"room-a";

#[test]
fn create_frames_a_request_on_the_channel() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    match s.create_version(ch, b"v1") {
        Some(Message::VersionCreate { channel, name }) => {
            assert_eq!(channel, ch);
            assert_eq!(name, b"v1");
        }
        other => panic!("expected VersionCreate, got {other:?}"),
    }
}

#[test]
fn rename_delete_list_fetch_frame_their_requests() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    assert!(matches!(
        s.rename_version(ch, b"a", b"b"),
        Some(Message::VersionRename { channel, from, to })
            if channel == ch && from == b"a" && to == b"b"
    ));
    assert!(matches!(
        s.delete_version(ch, b"a"),
        Some(Message::VersionDelete { channel, name }) if channel == ch && name == b"a"
    ));
    assert!(matches!(
        s.list_versions(ch),
        Some(Message::VersionList { channel }) if channel == ch
    ));
    assert!(matches!(
        s.fetch_version(ch, b"v1"),
        Some(Message::VersionFetch { channel, name }) if channel == ch && name == b"v1"
    ));
}

#[test]
fn issue_methods_on_an_unknown_channel_are_none() {
    let s = ClientSession::new(cid(1));
    let ch = Channel(7);
    assert!(s.create_version(ch, b"v1").is_none());
    assert!(s.rename_version(ch, b"a", b"b").is_none());
    assert!(s.delete_version(ch, b"a").is_none());
    assert!(s.list_versions(ch).is_none());
    assert!(s.fetch_version(ch, b"v1").is_none());
}

#[test]
fn a_versions_reply_replaces_the_name_view() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);
    assert_eq!(s.versions(ch), Some(&[][..]), "empty until a reply arrives");

    s.receive(Message::Versions {
        channel: ch,
        names: vec![b"v1".to_vec(), b"v2".to_vec()],
    })
    .unwrap();
    assert_eq!(s.versions(ch).unwrap(), &[b"v1".to_vec(), b"v2".to_vec()]);

    // A later list is authoritative — it replaces, not merges.
    s.receive(Message::Versions {
        channel: ch,
        names: vec![b"v2".to_vec()],
    })
    .unwrap();
    assert_eq!(s.versions(ch).unwrap(), &[b"v2".to_vec()]);
}

#[test]
fn a_version_state_reply_is_cached_by_name() {
    let mut s = ClientSession::new(cid(1));
    let (ch, _) = s.subscribe(ROOM_A);

    let mut server = Document::new(cid(2));
    server.transact(|tx| tx.register(b"age", Scalar::Int(30)));
    let bytes = server.encode_state();

    assert!(s.version_state(ch, b"v1").is_none(), "nothing fetched yet");
    s.receive(Message::VersionState {
        channel: ch,
        name: b"v1".to_vec(),
        seq: 1,
        state: bytes.clone(),
    })
    .unwrap();

    let cached = s.version_state(ch, b"v1").expect("v1 is cached");
    assert_eq!(cached, bytes.as_slice());
    let restored = Document::decode_state(cached).unwrap();
    match restored.get(b"age") {
        Some(Element::Register(reg)) => assert_eq!(reg.borrow().read(), &Scalar::Int(30)),
        _ => panic!("expected the age register in the fetched state"),
    }
    assert!(
        s.version_state(ch, b"other").is_none(),
        "an unfetched name has no cached state"
    );
}

#[test]
fn a_version_reply_for_an_unknown_channel_is_refused() {
    let mut s = ClientSession::new(cid(1));
    s.subscribe(ROOM_A);
    assert_eq!(
        s.receive(Message::Versions {
            channel: Channel(9),
            names: vec![b"v1".to_vec()],
        }),
        Err(ClientError::UnknownChannel(Channel(9)))
    );
    assert_eq!(
        s.receive(Message::VersionState {
            channel: Channel(9),
            name: b"v1".to_vec(),
            seq: 1,
            state: Vec::new(),
        }),
        Err(ClientError::UnknownChannel(Channel(9)))
    );
}
