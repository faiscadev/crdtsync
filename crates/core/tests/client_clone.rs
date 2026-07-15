//! Client session — the clone-room issue method and result view.
//!
//! A [`ClientSession`] frames a room-keyed clone request naming source and
//! destination — not channel, so a client may clone a room before it subscribes
//! any of it — and folds the server's `CloneRoomResult` reply into a per-`dst`
//! view. A clone request frame that arrives from the server (they only travel
//! client-to-server) is refused.

use crdtsync_core::client::{ClientError, ClientSession};
use crdtsync_core::{ClientId, Message};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const SRC: &[u8] = b"template";
const DST: &[u8] = b"copy";

#[test]
fn clone_frames_a_room_keyed_request() {
    let s = ClientSession::new(cid(1));
    assert!(matches!(
        s.clone_room(SRC, DST),
        Message::CloneRoom { src, dst } if src == SRC && dst == DST
    ));
}

#[test]
fn a_clone_result_folds_into_the_view_per_dst() {
    let mut s = ClientSession::new(cid(1));
    assert!(s.clone_result(DST).is_none(), "none until a reply arrives");

    s.receive(Message::CloneRoomResult {
        dst: DST.to_vec(),
        created: true,
    })
    .unwrap();
    assert_eq!(s.clone_result(DST), Some(true));

    // A later reply for the same dst is authoritative — it replaces.
    s.receive(Message::CloneRoomResult {
        dst: DST.to_vec(),
        created: false,
    })
    .unwrap();
    assert_eq!(s.clone_result(DST), Some(false));
}

#[test]
fn clone_results_are_isolated_per_dst() {
    let mut s = ClientSession::new(cid(1));
    s.receive(Message::CloneRoomResult {
        dst: b"copy-a".to_vec(),
        created: true,
    })
    .unwrap();
    assert_eq!(s.clone_result(b"copy-a"), Some(true));
    assert!(
        s.clone_result(b"copy-b").is_none(),
        "another destination's result is untouched"
    );
}

#[test]
fn a_server_sent_clone_request_is_refused() {
    let mut s = ClientSession::new(cid(1));
    assert_eq!(
        s.receive(Message::CloneRoom {
            src: SRC.to_vec(),
            dst: DST.to_vec(),
        }),
        Err(ClientError::UnexpectedMessage(
            "server sent a clone request"
        ))
    );
}
