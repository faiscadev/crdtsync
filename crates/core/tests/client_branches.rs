//! Client session — the branch-management view and issue methods.
//!
//! A [`ClientSession`] frames branch requests (list / fork / fork-from-version /
//! restore / publish / delete) keyed by room — not channel, so a client may
//! manage a room's branches before it subscribes any of them — and folds the
//! server's `Branches` reply into a per-room view. A branch request frame that
//! arrives from the server (they only travel client-to-server) is refused.

use crdtsync_core::client::{ClientError, ClientSession};
use crdtsync_core::protocol::BranchInfo;
use crdtsync_core::{ClientId, Message};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-a";

#[test]
fn list_frames_a_room_keyed_request() {
    let s = ClientSession::new(cid(1));
    match s.list_branches(ROOM) {
        Message::BranchList { room } => assert_eq!(room, ROOM),
        other => panic!("expected BranchList, got {other:?}"),
    }
}

#[test]
fn mutation_methods_frame_their_requests() {
    let s = ClientSession::new(cid(1));
    assert!(matches!(
        s.fork_branch(ROOM, b"feature", b"main"),
        Message::BranchFork { room, name, from_branch }
            if room == ROOM && name == b"feature" && from_branch == b"main"
    ));
    assert!(matches!(
        s.fork_branch_from_version(ROOM, b"feature", b"v1"),
        Message::BranchForkFromVersion { room, name, version }
            if room == ROOM && name == b"feature" && version == b"v1"
    ));
    assert!(matches!(
        s.restore_branch(ROOM, b"restored", b"v1"),
        Message::BranchRestore { room, name, version }
            if room == ROOM && name == b"restored" && version == b"v1"
    ));
    assert!(matches!(
        s.publish_branch(ROOM, b"live"),
        Message::BranchPublish { room, published }
            if room == ROOM && published == b"live"
    ));
    assert!(matches!(
        s.delete_branch(ROOM, b"feature"),
        Message::BranchDelete { room, name } if room == ROOM && name == b"feature"
    ));
}

#[test]
fn a_branches_reply_replaces_the_view_per_room() {
    let mut s = ClientSession::new(cid(1));
    assert!(s.branches(ROOM).is_none(), "none until a reply arrives");

    s.receive(Message::Branches {
        room: ROOM.to_vec(),
        branches: vec![
            BranchInfo {
                name: b"main".to_vec(),
                fork_point: 0,
                head: 5,
                published: false,
            },
            BranchInfo {
                name: b"live".to_vec(),
                fork_point: 5,
                head: 5,
                published: true,
            },
        ],
    })
    .unwrap();
    let view = s.branches(ROOM).expect("a set is held");
    assert_eq!(view.len(), 2);
    assert_eq!(view[1].name, b"live");
    assert!(view[1].published);

    // A later reply is authoritative — it replaces, not merges.
    s.receive(Message::Branches {
        room: ROOM.to_vec(),
        branches: vec![BranchInfo {
            name: b"main".to_vec(),
            fork_point: 0,
            head: 9,
            published: false,
        }],
    })
    .unwrap();
    let view = s.branches(ROOM).unwrap();
    assert_eq!(view.len(), 1);
    assert_eq!(view[0].head, 9);
}

#[test]
fn branch_views_are_isolated_per_room() {
    let mut s = ClientSession::new(cid(1));
    s.receive(Message::Branches {
        room: b"room-a".to_vec(),
        branches: vec![BranchInfo {
            name: b"main".to_vec(),
            fork_point: 0,
            head: 1,
            published: false,
        }],
    })
    .unwrap();
    assert!(s.branches(b"room-a").is_some());
    assert!(
        s.branches(b"room-b").is_none(),
        "another room's view is untouched"
    );
}

#[test]
fn a_server_sent_branch_request_is_refused() {
    let mut s = ClientSession::new(cid(1));
    assert_eq!(
        s.receive(Message::BranchList {
            room: ROOM.to_vec(),
        }),
        Err(ClientError::UnexpectedMessage(
            "server sent a branch or diff request"
        ))
    );
}
