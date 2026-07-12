//! The client surfaces a server `Redirect` for the transport to act on.
//!
//! A node that does not lead a room answers with `Redirect { room, leader_addr }`
//! instead of a catch-up. The core [`ClientSession`] holds no socket, so it
//! cannot reconnect itself — it buffers the target and surfaces it through
//! [`take_redirects`](ClientSession::take_redirects), the same drain split as
//! `onOpsRejected`, so the transport layer reconnects to the leader. A normal
//! frame is unaffected.

use crdtsync_core::client::{ClientSession, Redirect};
use crdtsync_core::{ClientId, Message};

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

const ROOM: &[u8] = b"room-1";
const LEADER: &[u8] = b"10.0.0.7:9000";

#[test]
fn receive_surfaces_the_redirect_target() {
    let mut session = ClientSession::new(cid(1));
    session
        .receive(Message::Redirect {
            room: ROOM.to_vec(),
            leader_addr: LEADER.to_vec(),
        })
        .expect("a redirect is accepted, not an error");
    assert_eq!(
        session.take_redirects(),
        vec![Redirect {
            room: ROOM.to_vec(),
            leader_addr: LEADER.to_vec(),
        }]
    );
}

#[test]
fn take_redirects_drains() {
    let mut session = ClientSession::new(cid(1));
    session
        .receive(Message::Redirect {
            room: ROOM.to_vec(),
            leader_addr: LEADER.to_vec(),
        })
        .unwrap();
    assert_eq!(session.take_redirects().len(), 1);
    // Draining, so a second call reports nothing new.
    assert!(session.take_redirects().is_empty());
}

#[test]
fn no_redirect_is_empty() {
    let mut session = ClientSession::new(cid(1));
    assert!(session.take_redirects().is_empty());
}

#[test]
fn redirects_accumulate_across_rooms() {
    let mut session = ClientSession::new(cid(1));
    session
        .receive(Message::Redirect {
            room: b"room-a".to_vec(),
            leader_addr: b"node-1".to_vec(),
        })
        .unwrap();
    session
        .receive(Message::Redirect {
            room: b"room-b".to_vec(),
            leader_addr: b"node-2".to_vec(),
        })
        .unwrap();
    assert_eq!(
        session.take_redirects(),
        vec![
            Redirect {
                room: b"room-a".to_vec(),
                leader_addr: b"node-1".to_vec(),
            },
            Redirect {
                room: b"room-b".to_vec(),
                leader_addr: b"node-2".to_vec(),
            },
        ]
    );
}

#[test]
fn a_normal_frame_surfaces_no_redirect() {
    let mut session = ClientSession::new(cid(1));
    let (channel, _sub) = session.subscribe(ROOM);
    session
        .receive(Message::Ops {
            channel,
            ops: Vec::new(),
        })
        .unwrap();
    assert!(session.take_redirects().is_empty());
}
