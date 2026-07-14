//! C ABI — the wire client session.
//!
//! A client holds a replica per subscribed room and turns local edits into wire
//! frames to send; folding a peer's frame back in converges the replicas. Frames
//! cross the boundary as encoded byte buffers, a room addressed by the `u32`
//! channel the client assigned at subscribe. Every buffer and handle is freed so
//! the round trip is leak-clean under Miri.

use crdtsync_core::protocol::BranchInfo;
use crdtsync_core::{
    decode_message, decode_ops, encode_message, encode_op, Channel, ErrorCode, Message, Op, Scalar,
};
use crdtsync_ffi::*;
use std::ptr;

/// A freshly-nulled output buffer for the read entry points to fill.
fn out_buf() -> CrdtBuf {
    CrdtBuf {
        ptr: ptr::null_mut(),
        len: 0,
    }
}

fn client_id(first: u8) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0] = first;
    b
}

/// Encode a path: each key as a u32 length prefix followed by its bytes.
fn path(keys: &[&[u8]]) -> Vec<u8> {
    let mut b = Vec::new();
    for k in keys {
        b.extend_from_slice(&(k.len() as u32).to_le_bytes());
        b.extend_from_slice(k);
    }
    b
}

unsafe fn subscribe(c: *mut CrdtClient, room: &[u8]) -> (u32, CrdtBuf) {
    let mut channel: u32 = u32::MAX;
    let frame = crdtsync_client_subscribe(c, room.as_ptr(), room.len(), &mut channel);
    (channel, frame)
}

unsafe fn subscribe_branch(c: *mut CrdtClient, room: &[u8], branch: &[u8]) -> (u32, CrdtBuf) {
    let mut channel: u32 = u32::MAX;
    let frame = crdtsync_client_subscribe_branch(
        c,
        room.as_ptr(),
        room.len(),
        branch.as_ptr(),
        branch.len(),
        &mut channel,
    );
    (channel, frame)
}

/// The branch a Subscribe frame carries, or panics on any other frame.
unsafe fn subscribe_frame_branch(frame: &CrdtBuf) -> Vec<u8> {
    let bytes = std::slice::from_raw_parts(frame.ptr, frame.len);
    match decode_message(bytes).unwrap() {
        Message::Subscribe { branch, .. } => branch,
        other => panic!("expected Subscribe, got {other:?}"),
    }
}

unsafe fn register_int(c: *mut CrdtClient, channel: u32, p: &[u8], v: i64) -> CrdtBuf {
    crdtsync_client_register_int(c, channel, p.as_ptr(), p.len(), v)
}

unsafe fn get_int(c: *const CrdtClient, channel: u32, p: &[u8]) -> (i32, i64) {
    let mut out: i64 = 0;
    let rc = crdtsync_client_get_int(c, channel, p.as_ptr(), p.len(), &mut out);
    (rc, out)
}

unsafe fn receive(c: *mut CrdtClient, frame: &CrdtBuf) -> i32 {
    crdtsync_client_receive(c, frame.ptr, frame.len, ptr::null_mut())
}

#[test]
fn a_local_edit_travels_to_a_peer_over_the_wire_client() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let b = crdtsync_client_new(client_id(2).as_ptr());
        assert!(!a.is_null() && !b.is_null());

        // Both fresh sessions assign channel 0 to their first subscription.
        let (ca, sub_a) = subscribe(a, b"room-1");
        let (cb, sub_b) = subscribe(b, b"room-1");
        assert_eq!(ca, 0);
        assert_eq!(cb, 0);
        crdtsync_buf_free(sub_a);
        crdtsync_buf_free(sub_b);

        let p = path(&[b"age"]);
        // A's edit yields the Ops frame to send and applies locally.
        let ops = register_int(a, ca, &p, 30);
        assert!(ops.len > 0);
        assert_eq!(get_int(a, ca, &p), (1, 30));

        // B folds the frame in and converges; the batch advances its seen seq.
        assert_eq!(receive(b, &ops), 1);
        assert_eq!(get_int(b, cb, &p), (1, 30));
        let mut seen: u64 = 0;
        assert_eq!(crdtsync_client_last_seen_seq(b, cb, &mut seen), 1);
        assert_eq!(seen, 1);

        crdtsync_buf_free(ops);
        crdtsync_client_free(a);
        crdtsync_client_free(b);
    }
}

#[test]
fn subscribe_branch_carries_the_named_branch() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());

        // A named branch rides along in the Subscribe frame.
        let (ch, frame) = subscribe_branch(a, b"room-1", b"feature-x");
        assert_eq!(ch, 0);
        assert_eq!(subscribe_frame_branch(&frame), b"feature-x");
        crdtsync_buf_free(frame);

        // An empty branch is the default/active branch, as the plain subscribe.
        let (ch, frame) = subscribe_branch(a, b"room-1", b"");
        assert_eq!(ch, 1);
        assert!(subscribe_frame_branch(&frame).is_empty());
        crdtsync_buf_free(frame);

        let (_, frame) = subscribe(a, b"room-1");
        assert!(subscribe_frame_branch(&frame).is_empty());
        crdtsync_buf_free(frame);

        // A null handle yields the empty-buffer sentinel and assigns no channel.
        let mut channel: u32 = u32::MAX;
        let frame = crdtsync_client_subscribe_branch(
            ptr::null_mut(),
            b"room-1".as_ptr(),
            6,
            b"feature-x".as_ptr(),
            9,
            &mut channel,
        );
        assert_eq!(frame.len, 0);
        assert_eq!(channel, u32::MAX);
        crdtsync_buf_free(frame);

        crdtsync_client_free(a);
    }
}

#[test]
fn a_bytes_scalar_round_trips_through_the_client() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let b = crdtsync_client_new(client_id(2).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        let (cb, sb) = subscribe(b, b"room-1");
        crdtsync_buf_free(sa);
        crdtsync_buf_free(sb);

        let p = path(&[b"blob"]);
        let value = b"hello";
        let ops =
            crdtsync_client_set_bytes(a, ca, p.as_ptr(), p.len(), value.as_ptr(), value.len());
        assert_eq!(receive(b, &ops), 1);

        let mut out = CrdtBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        let rc = crdtsync_client_get_bytes(b, cb, p.as_ptr(), p.len(), &mut out);
        assert_eq!(rc, 1);
        assert_eq!(std::slice::from_raw_parts(out.ptr, out.len), value);

        crdtsync_buf_free(out);
        crdtsync_buf_free(ops);
        crdtsync_client_free(a);
        crdtsync_client_free(b);
    }
}

#[test]
fn a_bad_handle_is_rejected_not_dereferenced() {
    unsafe {
        // Null handles never crash the boundary.
        let hello = crdtsync_client_hello(ptr::null());
        assert_eq!(hello.len, 0);
        crdtsync_buf_free(hello);
        let p = path(&[b"age"]);
        let ops = register_int(ptr::null_mut(), 0, &p, 1);
        assert_eq!(ops.len, 0);
        crdtsync_buf_free(ops);
        assert_eq!(get_int(ptr::null(), 0, &p), (-1, 0));
        assert_eq!(
            crdtsync_client_receive(ptr::null_mut(), p.as_ptr(), p.len(), ptr::null_mut()),
            -1
        );
    }
}

#[test]
fn a_server_error_frame_surfaces_its_code_as_the_out_param() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());

        // A server Error frame is refused (0) and writes its code — UpdateRequired
        // (6), the onUpdateRequired signal — to the out-param.
        let err = encode_message(&Message::Error {
            code: ErrorCode::UpdateRequired,
            message: "please update".to_string(),
            details: Vec::new(),
        });
        let mut code: i32 = -1;
        assert_eq!(
            crdtsync_client_receive(c, err.as_ptr(), err.len(), &mut code),
            0
        );
        assert_eq!(code, 6);

        // A null out-param is tolerated: the same refusal, no crash.
        assert_eq!(
            crdtsync_client_receive(c, err.as_ptr(), err.len(), ptr::null_mut()),
            0
        );

        // A malformed frame is refused without writing a spurious code.
        let mut untouched: i32 = -1;
        assert_eq!(
            crdtsync_client_receive(c, [0xff, 0xff, 0xff].as_ptr(), 3, &mut untouched),
            0
        );
        assert_eq!(untouched, -1);

        crdtsync_client_free(c);
    }
}

#[test]
fn declaring_an_app_carries_it_into_the_hello_frame() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());

        // A bare client's Hello opens a relay: no app, version 0.
        let hello = crdtsync_client_hello(c);
        match decode_message(std::slice::from_raw_parts(hello.ptr, hello.len)).unwrap() {
            Message::Hello {
                app_id,
                schema_version,
                ..
            } => {
                assert!(app_id.is_empty());
                assert_eq!(schema_version, 0);
            }
            other => panic!("expected Hello, got {other:?}"),
        }
        crdtsync_buf_free(hello);

        // Declaring an app names it and the version in the next Hello.
        let app = b"app-x";
        assert_eq!(
            crdtsync_client_declare_app(c, app.as_ptr(), app.len(), 3),
            1
        );
        let hello = crdtsync_client_hello(c);
        match decode_message(std::slice::from_raw_parts(hello.ptr, hello.len)).unwrap() {
            Message::Hello {
                app_id,
                schema_version,
                ..
            } => {
                assert_eq!(app_id, b"app-x");
                assert_eq!(schema_version, 3);
            }
            other => panic!("expected Hello, got {other:?}"),
        }
        crdtsync_buf_free(hello);

        // A bad handle is rejected, not dereferenced.
        assert_eq!(
            crdtsync_client_declare_app(ptr::null_mut(), app.as_ptr(), app.len(), 1),
            -1
        );

        crdtsync_client_free(c);
    }
}

#[test]
fn the_server_advertised_schema_is_recorded_and_readable() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());

        // Nothing advertised yet: both accessors report absence (0), untouched out.
        let mut version: u32 = 0;
        assert_eq!(crdtsync_client_active_schema_version(c, &mut version), 0);
        let mut schema = out_buf();
        assert_eq!(crdtsync_client_active_schema(c, &mut schema), 0);

        // Folding a SchemaAdvert records the concrete version and its bytes.
        let advert = encode_message(&Message::SchemaAdvert {
            schema_version: 4,
            schema: b"schema-body".to_vec(),
        });
        assert_eq!(
            crdtsync_client_receive(c, advert.as_ptr(), advert.len(), ptr::null_mut()),
            1
        );
        assert_eq!(crdtsync_client_active_schema_version(c, &mut version), 1);
        assert_eq!(version, 4);
        assert_eq!(crdtsync_client_active_schema(c, &mut schema), 1);
        assert_eq!(
            std::slice::from_raw_parts(schema.ptr, schema.len),
            b"schema-body"
        );
        crdtsync_buf_free(schema);
        schema = out_buf();

        // A later advert supersedes the recorded one.
        let advert = encode_message(&Message::SchemaAdvert {
            schema_version: 5,
            schema: b"next-body".to_vec(),
        });
        assert_eq!(
            crdtsync_client_receive(c, advert.as_ptr(), advert.len(), ptr::null_mut()),
            1
        );
        assert_eq!(crdtsync_client_active_schema_version(c, &mut version), 1);
        assert_eq!(version, 5);
        assert_eq!(crdtsync_client_active_schema(c, &mut schema), 1);
        assert_eq!(
            std::slice::from_raw_parts(schema.ptr, schema.len),
            b"next-body"
        );
        crdtsync_buf_free(schema);
        schema = out_buf();

        // An advert whose body is empty is still an advertisement: present (1),
        // not collapsed into the absent (0) reading.
        let advert = encode_message(&Message::SchemaAdvert {
            schema_version: 6,
            schema: Vec::new(),
        });
        assert_eq!(
            crdtsync_client_receive(c, advert.as_ptr(), advert.len(), ptr::null_mut()),
            1
        );
        assert_eq!(crdtsync_client_active_schema_version(c, &mut version), 1);
        assert_eq!(version, 6);
        assert_eq!(crdtsync_client_active_schema(c, &mut schema), 1);
        assert_eq!(schema.len, 0);
        crdtsync_buf_free(schema);
        schema = out_buf();

        // A bad handle is rejected (-1), never dereferenced.
        assert_eq!(
            crdtsync_client_active_schema_version(ptr::null(), &mut version),
            -1
        );
        assert_eq!(crdtsync_client_active_schema(ptr::null(), &mut schema), -1);

        // A null out pointer on a live handle is rejected too, never written.
        assert_eq!(
            crdtsync_client_active_schema_version(c, ptr::null_mut()),
            -1
        );
        assert_eq!(crdtsync_client_active_schema(c, ptr::null_mut()), -1);

        crdtsync_client_free(c);
    }
}

#[test]
fn auth_establishes_the_actor_once_authok_arrives() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());
        let cred = b"token";
        let auth = crdtsync_client_auth(c, cred.as_ptr(), cred.len());
        assert!(auth.len > 0);
        crdtsync_buf_free(auth);

        // No actor until the server's AuthOk is folded in.
        let mut out = out_buf();
        assert_eq!(crdtsync_client_actor(c, &mut out), 0);

        let frame = encode_message(&Message::AuthOk {
            actor: b"alice".to_vec(),
        });
        assert_eq!(
            crdtsync_client_receive(c, frame.as_ptr(), frame.len(), ptr::null_mut()),
            1
        );
        assert_eq!(crdtsync_client_actor(c, &mut out), 1);
        assert_eq!(std::slice::from_raw_parts(out.ptr, out.len), b"alice");

        crdtsync_buf_free(out);
        crdtsync_client_free(c);
    }
}

#[test]
fn a_peer_awareness_update_is_folded_and_readable() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());
        let (ch, sub) = subscribe(c, b"room-1");
        crdtsync_buf_free(sub);

        // Publishing yields a frame to send.
        let published =
            crdtsync_client_set_awareness(c, ch, b"cursor".as_ptr(), 6, b"x".as_ptr(), 1);
        assert!(published.len > 0);
        crdtsync_buf_free(published);

        // A peer's update on this channel folds in and reads back by (actor, key).
        let frame = encode_message(&Message::AwarenessUpdate {
            channel: Channel(ch),
            actor: b"bob".to_vec(),
            key: b"cursor".to_vec(),
            value: vec![9],
        });
        assert_eq!(
            crdtsync_client_receive(c, frame.as_ptr(), frame.len(), ptr::null_mut()),
            1
        );

        let mut out = out_buf();
        let rc =
            crdtsync_client_awareness(c, ch, b"bob".as_ptr(), 3, b"cursor".as_ptr(), 6, &mut out);
        assert_eq!(rc, 1);
        assert_eq!(std::slice::from_raw_parts(out.ptr, out.len), &[9]);
        crdtsync_buf_free(out);

        let mut n: usize = 0;
        assert_eq!(crdtsync_client_awareness_len(c, ch, &mut n), 1);
        assert_eq!(n, 1);

        crdtsync_client_free(c);
    }
}

#[test]
fn named_versions_round_trip_over_the_client() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());
        let (ch, sub) = subscribe(c, b"room-1");
        crdtsync_buf_free(sub);

        // Every issue method frames a non-empty request to send.
        for frame in [
            crdtsync_client_create_version(c, ch, b"v1".as_ptr(), 2),
            crdtsync_client_rename_version(c, ch, b"v1".as_ptr(), 2, b"v2".as_ptr(), 2),
            crdtsync_client_delete_version(c, ch, b"v1".as_ptr(), 2),
            crdtsync_client_list_versions(c, ch),
            crdtsync_client_fetch_version(c, ch, b"v1".as_ptr(), 2),
        ] {
            assert!(frame.len > 0, "a version request frames bytes to send");
            crdtsync_buf_free(frame);
        }

        // The server's name list lands in the view.
        let listing = encode_message(&Message::Versions {
            channel: Channel(ch),
            names: vec![b"v1".to_vec(), b"v2".to_vec()],
        });
        assert_eq!(
            crdtsync_client_receive(c, listing.as_ptr(), listing.len(), ptr::null_mut()),
            1
        );

        let mut n: usize = 0;
        assert_eq!(crdtsync_client_version_count(c, ch, &mut n), 1);
        assert_eq!(n, 2);
        let mut name = out_buf();
        assert_eq!(crdtsync_client_version_name(c, ch, 1, &mut name), 1);
        assert_eq!(std::slice::from_raw_parts(name.ptr, name.len), b"v2");
        crdtsync_buf_free(name);
        // Out of range reports absent.
        let mut oob = out_buf();
        assert_eq!(crdtsync_client_version_name(c, ch, 9, &mut oob), 0);

        // A fetched state is cached by name.
        let state = encode_message(&Message::VersionState {
            channel: Channel(ch),
            name: b"v1".to_vec(),
            seq: 1,
            state: vec![7, 8, 9],
        });
        assert_eq!(
            crdtsync_client_receive(c, state.as_ptr(), state.len(), ptr::null_mut()),
            1
        );
        let mut st = out_buf();
        assert_eq!(
            crdtsync_client_version_state(c, ch, b"v1".as_ptr(), 2, &mut st),
            1
        );
        assert_eq!(std::slice::from_raw_parts(st.ptr, st.len), &[7, 8, 9]);
        crdtsync_buf_free(st);

        // An unfetched name has no cached state.
        let mut none = out_buf();
        assert_eq!(
            crdtsync_client_version_state(c, ch, b"other".as_ptr(), 5, &mut none),
            0
        );

        crdtsync_client_free(c);
    }
}

#[test]
fn branch_management_round_trips_over_the_client() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());

        // Every issue method frames a non-empty request to send — room-keyed, so
        // no subscription is needed first.
        for frame in [
            crdtsync_client_list_branches(c, b"room-1".as_ptr(), 6),
            crdtsync_client_fork_branch(
                c,
                b"room-1".as_ptr(),
                6,
                b"f".as_ptr(),
                1,
                b"main".as_ptr(),
                4,
            ),
            crdtsync_client_fork_branch_from_version(
                c,
                b"room-1".as_ptr(),
                6,
                b"f".as_ptr(),
                1,
                b"v1".as_ptr(),
                2,
            ),
            crdtsync_client_restore_branch(
                c,
                b"room-1".as_ptr(),
                6,
                b"r".as_ptr(),
                1,
                b"v1".as_ptr(),
                2,
            ),
            crdtsync_client_publish_branch(c, b"room-1".as_ptr(), 6, b"live".as_ptr(), 4),
            crdtsync_client_delete_branch(c, b"room-1".as_ptr(), 6, b"f".as_ptr(), 1),
        ] {
            assert!(frame.len > 0, "a branch request frames bytes to send");
            crdtsync_buf_free(frame);
        }

        // The server's branch set lands in the view, keyed by room.
        let listing = encode_message(&Message::Branches {
            room: b"room-1".to_vec(),
            branches: vec![
                BranchInfo {
                    name: b"main".to_vec(),
                    fork_point: 0,
                    head: 3,
                    published: false,
                },
                BranchInfo {
                    name: b"live".to_vec(),
                    fork_point: 3,
                    head: 3,
                    published: true,
                },
            ],
        });
        assert_eq!(
            crdtsync_client_receive(c, listing.as_ptr(), listing.len(), ptr::null_mut()),
            1
        );

        let mut n: usize = 0;
        assert_eq!(
            crdtsync_client_branch_count(c, b"room-1".as_ptr(), 6, &mut n),
            1
        );
        assert_eq!(n, 2);

        let mut name = out_buf();
        let (mut fork_point, mut head, mut published) = (0u64, 0u64, 0i32);
        assert_eq!(
            crdtsync_client_branch_at(
                c,
                b"room-1".as_ptr(),
                6,
                1,
                &mut name,
                &mut fork_point,
                &mut head,
                &mut published,
            ),
            1
        );
        assert_eq!(std::slice::from_raw_parts(name.ptr, name.len), b"live");
        assert_eq!(fork_point, 3);
        assert_eq!(head, 3);
        assert_eq!(published, 1);
        crdtsync_buf_free(name);

        // Out of range reports absent.
        let mut oob = out_buf();
        assert_eq!(
            crdtsync_client_branch_at(
                c,
                b"room-1".as_ptr(),
                6,
                9,
                &mut oob,
                &mut fork_point,
                &mut head,
                &mut published,
            ),
            0
        );

        // A room with no reported set counts zero.
        let mut z: usize = 7;
        assert_eq!(
            crdtsync_client_branch_count(c, b"ghost".as_ptr(), 5, &mut z),
            1
        );
        assert_eq!(z, 0);

        crdtsync_client_free(c);
    }
}

#[test]
fn unsubscribe_drops_the_channel() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());
        let (ch, sub) = subscribe(c, b"room-1");
        crdtsync_buf_free(sub);

        let un = crdtsync_client_unsubscribe(c, ch);
        assert!(un.len > 0);
        crdtsync_buf_free(un);

        // The channel is gone: reads report absent, resume yields nothing.
        let mut seen: u64 = 0;
        assert_eq!(crdtsync_client_last_seen_seq(c, ch, &mut seen), 0);
        let resume = crdtsync_client_resume(c, ch);
        assert_eq!(resume.len, 0);
        crdtsync_buf_free(resume);

        crdtsync_client_free(c);
    }
}

#[test]
fn the_outbox_drains_against_an_ack_over_the_wire_client() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());
        let (ch, sub) = subscribe(c, b"room-1");
        crdtsync_buf_free(sub);
        let p = path(&[b"age"]);

        let e1 = register_int(c, ch, &p, 30);
        crdtsync_buf_free(e1);
        let e2 = register_int(c, ch, &p, 31);
        crdtsync_buf_free(e2);

        let mut n: usize = 0;
        assert_eq!(crdtsync_client_outbox_len(c, ch, &mut n), 1);
        assert_eq!(n, 2);

        // The unacknowledged tail replays as one Ops frame.
        let tail = crdtsync_client_resend(c, ch);
        assert!(tail.len > 0);
        crdtsync_buf_free(tail);

        // An Accepted through u64::MAX drains the outbox.
        let accepted = encode_message(&Message::Accepted {
            channel: Channel(ch),
            through: u64::MAX,
        });
        assert_eq!(
            crdtsync_client_receive(c, accepted.as_ptr(), accepted.len(), ptr::null_mut()),
            1
        );

        assert_eq!(crdtsync_client_outbox_len(c, ch, &mut n), 1);
        assert_eq!(n, 0);
        let empty = crdtsync_client_resend(c, ch);
        assert_eq!(empty.len, 0);
        crdtsync_buf_free(empty);

        crdtsync_client_free(c);
    }
}

#[test]
fn an_xml_edit_enqueues_and_resends_over_the_wire_client() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let b = crdtsync_client_new(client_id(2).as_ptr());
        let (ca, sub_a) = subscribe(a, b"room-1");
        let (cb, sub_b) = subscribe(b, b"room-1");
        crdtsync_buf_free(sub_a);
        crdtsync_buf_free(sub_b);
        let p = path(&[b"doc", b"body"]);

        // An xml install routes through the outbox like every other edit, so it
        // can be resent and acknowledged rather than framed and forgotten.
        let root = crdtsync_client_xml_element(a, ca, p.as_ptr(), p.len(), b"body".as_ptr(), 4);
        let kid =
            crdtsync_client_xml_insert_element(a, ca, p.as_ptr(), p.len(), 0, b"p".as_ptr(), 1);
        assert!(root.len > 0 && kid.len > 0, "the edits frame ops to send");

        // Each xml edit emits several ops (a container install plus its child
        // placement); every one enters the outbox rather than being framed and
        // forgotten.
        let mut n: usize = 0;
        assert_eq!(crdtsync_client_outbox_len(a, ca, &mut n), 1);
        assert!(n >= 2, "the xml edits entered the outbox, got {n}");

        // The unacknowledged tail replays as one Ops frame and folds into the peer.
        let tail = crdtsync_client_resend(a, ca);
        assert!(tail.len > 0);
        assert!(
            receive(b, &tail) >= 1,
            "the peer applies the replayed xml ops"
        );
        crdtsync_buf_free(tail);

        // An ack drains the queue.
        let accepted = encode_message(&Message::Accepted {
            channel: Channel(ca),
            through: u64::MAX,
        });
        assert_eq!(
            crdtsync_client_receive(a, accepted.as_ptr(), accepted.len(), ptr::null_mut()),
            1
        );
        assert_eq!(crdtsync_client_outbox_len(a, ca, &mut n), 1);
        assert_eq!(n, 0, "the ack drained the xml edits");

        let _ = cb;
        crdtsync_buf_free(root);
        crdtsync_buf_free(kid);
        crdtsync_client_free(a);
        crdtsync_client_free(b);
    }
}

#[test]
fn an_atomic_transaction_travels_over_the_wire_client() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let b = crdtsync_client_new(client_id(2).as_ptr());
        let (ca, sub_a) = subscribe(a, b"room-1");
        let (cb, sub_b) = subscribe(b, b"room-1");
        crdtsync_buf_free(sub_a);
        crdtsync_buf_free(sub_b);

        let x = path(&[b"x"]);
        let y = path(&[b"y"]);
        crdtsync_client_begin_atomic(a, ca);
        // Edits accumulate while recording; each frame carries no ops.
        let e1 = register_int(a, ca, &x, 1);
        let e2 = register_int(a, ca, &y, 2);
        let frame = crdtsync_client_commit_atomic(a, ca);
        assert!(frame.len > 0);
        assert_eq!(get_int(a, ca, &x), (1, 1));

        // The whole group folds into the peer atomically.
        assert!(receive(b, &frame) >= 1);
        assert_eq!(get_int(b, cb, &x), (1, 1));
        assert_eq!(get_int(b, cb, &y), (1, 2));

        crdtsync_buf_free(e1);
        crdtsync_buf_free(e2);
        crdtsync_buf_free(frame);
        crdtsync_client_free(a);
        crdtsync_client_free(b);
    }
}

/// Fold a "body" text into `channel`'s replica by applying the ops a scratch doc
/// author produced — the client surface has no text insert, so a mark's sequence
/// is seeded from a peer frame.
unsafe fn seed_body_text(c: *mut CrdtClient, channel: u32, p: &[u8], s: &str) {
    let scratch = crdtsync_doc_new(client_id(9).as_ptr());
    let ops_buf = crdtsync_doc_text_insert(scratch, p.as_ptr(), p.len(), 0, s.as_ptr(), s.len());
    let ops = decode_ops(std::slice::from_raw_parts(ops_buf.ptr, ops_buf.len)).unwrap();
    let frame = encode_message(&Message::Ops {
        channel: Channel(channel),
        ops,
    });
    assert!(
        crdtsync_client_receive(c, frame.as_ptr(), frame.len(), ptr::null_mut()) >= 1,
        "the seeded text applies to the replica"
    );
    crdtsync_buf_free(ops_buf);
    crdtsync_doc_free(scratch);
}

#[test]
fn a_mark_enqueues_and_resends_over_the_wire_client() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let b = crdtsync_client_new(client_id(2).as_ptr());
        let (ca, sub_a) = subscribe(a, b"room-1");
        let (cb, sub_b) = subscribe(b, b"room-1");
        crdtsync_buf_free(sub_a);
        crdtsync_buf_free(sub_b);
        let body = path(&[b"body"]);

        // Both replicas hold the text the mark annotates.
        seed_body_text(a, ca, &body, "hello world");
        seed_body_text(b, cb, &body, "hello world");

        // Authoring a mark routes its ops through the outbox so they are resent /
        // acknowledged rather than framed and forgotten.
        let value = Scalar::Bool(true).encode_state();
        let mut mid = out_buf();
        let frame = crdtsync_client_mark(
            a,
            ca,
            body.as_ptr(),
            body.len(),
            0,
            1,
            5,
            0,
            b"bold".as_ptr(),
            4,
            value.as_ptr(),
            value.len(),
            &mut mid,
        );
        assert!(frame.len > 0, "the mark frames ops to send");
        assert_eq!(mid.len, 16, "the author returns the mark id");

        let mut n: usize = 0;
        assert_eq!(crdtsync_client_outbox_len(a, ca, &mut n), 1);
        assert!(n >= 1, "the mark entered the outbox, got {n}");

        // The unacknowledged tail replays as one frame and folds into the peer.
        let tail = crdtsync_client_resend(a, ca);
        assert!(tail.len > 0);
        assert!(receive(b, &tail) >= 1, "the peer applies the replayed mark");
        crdtsync_buf_free(tail);

        // A value change and a delete on the handle enqueue too.
        let value2 = Scalar::Int(3).encode_state();
        let set =
            crdtsync_client_mark_set_value(a, ca, mid.ptr, mid.len, value2.as_ptr(), value2.len());
        assert!(set.len > 0, "the value change frames ops");
        let del = crdtsync_client_mark_delete(a, ca, mid.ptr, mid.len);
        assert!(del.len > 0, "the delete frames ops");
        assert_eq!(crdtsync_client_outbox_len(a, ca, &mut n), 1);
        assert!(
            n >= 3,
            "the mark, value change, and delete all enqueued, got {n}"
        );

        // An ack through u64::MAX drains the outbox.
        let accepted = encode_message(&Message::Accepted {
            channel: Channel(ca),
            through: u64::MAX,
        });
        assert_eq!(
            crdtsync_client_receive(a, accepted.as_ptr(), accepted.len(), ptr::null_mut()),
            1
        );
        assert_eq!(crdtsync_client_outbox_len(a, ca, &mut n), 1);
        assert_eq!(n, 0, "the ack drained the mark edits");

        let _ = cb;
        crdtsync_buf_free(frame);
        crdtsync_buf_free(set);
        crdtsync_buf_free(del);
        crdtsync_buf_free(mid);
        crdtsync_client_free(a);
        crdtsync_client_free(b);
    }
}

/// A little-endian reader over the `take_rejected` buffer.
struct Reader<'a> {
    d: &'a [u8],
    i: usize,
}

impl<'a> Reader<'a> {
    fn u32(&mut self) -> u32 {
        let v = u32::from_le_bytes(self.d[self.i..self.i + 4].try_into().unwrap());
        self.i += 4;
        v
    }

    fn i32(&mut self) -> i32 {
        let v = i32::from_le_bytes(self.d[self.i..self.i + 4].try_into().unwrap());
        self.i += 4;
        v
    }

    fn blob(&mut self) -> &'a [u8] {
        let n = self.u32() as usize;
        let b = &self.d[self.i..self.i + n];
        self.i += n;
        b
    }
}

/// One decoded rejected batch: its channel, reason discriminant, and op bytes.
struct DecodedRejected {
    channel: u32,
    reason: i32,
    ops: Vec<Vec<u8>>,
}

fn decode_rejected(data: &[u8]) -> Vec<DecodedRejected> {
    if data.is_empty() {
        return Vec::new();
    }
    let mut r = Reader { d: data, i: 0 };
    let n = r.u32();
    (0..n)
        .map(|_| {
            let channel = r.u32();
            let reason = r.i32();
            let count = r.u32();
            let ops = (0..count).map(|_| r.blob().to_vec()).collect();
            DecodedRejected {
                channel,
                reason,
                ops,
            }
        })
        .collect()
}

/// The per-client sequences of an authored Ops frame — how the server names the
/// ops it refuses.
fn seqs_of_frame(frame: &CrdtBuf) -> (Vec<u64>, Vec<Op>) {
    unsafe {
        match decode_message(std::slice::from_raw_parts(frame.ptr, frame.len)).unwrap() {
            Message::Ops { ops, .. } => (ops.iter().map(|o| o.id.seq).collect(), ops),
            other => panic!("expected Ops, got {other:?}"),
        }
    }
}

#[test]
fn a_server_ops_rejection_surfaces_the_refused_batch() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());
        let (ch, sub) = subscribe(c, b"room-1");
        crdtsync_buf_free(sub);
        let p = path(&[b"age"]);

        // Author an edit; its ops enter the outbox with per-client sequences.
        let authored = register_int(c, ch, &p, 30);
        let (seqs, ops) = seqs_of_frame(&authored);
        crdtsync_buf_free(authored);

        // The server refuses that batch — Forbidden, the auth-revoked rejection.
        let rejection = encode_message(&Message::OpsRejected {
            channel: Channel(ch),
            seqs,
            reason: ErrorCode::Forbidden,
        });
        assert_eq!(
            crdtsync_client_receive(c, rejection.as_ptr(), rejection.len(), ptr::null_mut()),
            1
        );

        // The drain yields the one batch: the channel, the reason (5 = Forbidden),
        // and the refused ops still carrying their bytes.
        let mut out = out_buf();
        assert_eq!(crdtsync_client_take_rejected(c, &mut out), 1);
        let decoded = decode_rejected(std::slice::from_raw_parts(out.ptr, out.len));
        crdtsync_buf_free(out);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].channel, ch);
        assert_eq!(decoded[0].reason, 5);
        let expected: Vec<Vec<u8>> = ops.iter().map(encode_op).collect();
        assert_eq!(decoded[0].ops, expected);

        // Draining: a second call is a bare zero count, no batches.
        let mut again = out_buf();
        assert_eq!(crdtsync_client_take_rejected(c, &mut again), 1);
        assert!(decode_rejected(std::slice::from_raw_parts(again.ptr, again.len)).is_empty());
        crdtsync_buf_free(again);

        crdtsync_client_free(c);
    }
}

#[test]
fn take_rejected_on_a_bad_handle_or_null_out_is_rejected() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());
        // A null out on a live handle is rejected, never written.
        assert_eq!(crdtsync_client_take_rejected(c, ptr::null_mut()), -1);
        // A bad handle is rejected, never dereferenced.
        let mut out = out_buf();
        assert_eq!(crdtsync_client_take_rejected(ptr::null_mut(), &mut out), -1);
        crdtsync_client_free(c);
    }
}

/// One decoded redirect: the room and the leader's advertise address.
struct DecodedRedirect {
    room: Vec<u8>,
    leader_addr: Vec<u8>,
}

fn decode_redirects(data: &[u8]) -> Vec<DecodedRedirect> {
    if data.is_empty() {
        return Vec::new();
    }
    let mut r = Reader { d: data, i: 0 };
    let n = r.u32();
    (0..n)
        .map(|_| DecodedRedirect {
            room: r.blob().to_vec(),
            leader_addr: r.blob().to_vec(),
        })
        .collect()
}

#[test]
fn a_server_redirect_surfaces_the_room_and_leader() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());

        // A node that does not lead the room tells the client where the leader is.
        let redirect = encode_message(&Message::Redirect {
            room: b"room-1".to_vec(),
            leader_addr: b"10.0.0.7:4000".to_vec(),
        });
        assert_eq!(
            crdtsync_client_receive(c, redirect.as_ptr(), redirect.len(), ptr::null_mut()),
            1
        );

        // The drain yields the one target: the room and the leader's address.
        let mut out = out_buf();
        assert_eq!(crdtsync_client_take_redirects(c, &mut out), 1);
        let decoded = decode_redirects(std::slice::from_raw_parts(out.ptr, out.len));
        crdtsync_buf_free(out);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].room, b"room-1");
        assert_eq!(decoded[0].leader_addr, b"10.0.0.7:4000");

        // Draining: a second call is a bare zero count, no targets.
        let mut again = out_buf();
        assert_eq!(crdtsync_client_take_redirects(c, &mut again), 1);
        assert!(decode_redirects(std::slice::from_raw_parts(again.ptr, again.len)).is_empty());
        crdtsync_buf_free(again);

        crdtsync_client_free(c);
    }
}

#[test]
fn take_redirects_on_a_bad_handle_or_null_out_is_rejected() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());
        // A null out on a live handle is rejected, never written.
        assert_eq!(crdtsync_client_take_redirects(c, ptr::null_mut()), -1);
        // A bad handle is rejected, never dereferenced.
        let mut out = out_buf();
        assert_eq!(
            crdtsync_client_take_redirects(ptr::null_mut(), &mut out),
            -1
        );
        crdtsync_client_free(c);
    }
}

#[test]
fn a_mark_on_a_bad_client_handle_is_inert() {
    unsafe {
        let value = Scalar::Bool(true).encode_state();
        let body = path(&[b"body"]);
        let mut mid = out_buf();
        // A null handle never emits, yields no id, and never dereferences.
        let frame = crdtsync_client_mark(
            ptr::null_mut(),
            0,
            body.as_ptr(),
            body.len(),
            0,
            1,
            5,
            0,
            b"bold".as_ptr(),
            4,
            value.as_ptr(),
            value.len(),
            &mut mid,
        );
        assert_eq!(frame.len, 0, "null handle frames nothing");
        assert_eq!(mid.len, 0, "null handle yields no id");
        crdtsync_buf_free(frame);

        let id = [0u8; 16];
        let set = crdtsync_client_mark_set_value(
            ptr::null_mut(),
            0,
            id.as_ptr(),
            16,
            value.as_ptr(),
            value.len(),
        );
        assert_eq!(set.len, 0, "null handle sets nothing");
        crdtsync_buf_free(set);
        let del = crdtsync_client_mark_delete(ptr::null_mut(), 0, id.as_ptr(), 16);
        assert_eq!(del.len, 0, "null handle deletes nothing");
        crdtsync_buf_free(del);
    }
}

unsafe fn outbox_len(c: *const CrdtClient, channel: u32) -> usize {
    let mut out: usize = 0;
    crdtsync_client_outbox_len(c, channel, &mut out);
    out
}

#[test]
fn a_blob_edit_enqueues_and_travels_over_the_client() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let b = crdtsync_client_new(client_id(2).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        let (_cb, sb) = subscribe(b, b"room-1");
        crdtsync_buf_free(sa);
        crdtsync_buf_free(sb);

        // Inline blob: enqueues one outbox entry and travels to the peer.
        let p = path(&[b"avatar"]);
        let mime = b"image/png";
        let bytes = b"tiny-png";
        let frame = crdtsync_client_set_blob(
            a,
            ca,
            p.as_ptr(),
            p.len(),
            mime.as_ptr(),
            mime.len(),
            bytes.as_ptr(),
            bytes.len(),
        );
        assert!(frame.len > 0, "an inline blob edit frames its ops");
        assert_eq!(outbox_len(a, ca), 1, "the edit entered the outbox");
        assert_eq!(receive(b, &frame), 1, "the peer folds the blob in");
        crdtsync_buf_free(frame);

        // Ref blob: a second outbox entry, also travelling.
        let pr = path(&[b"video"]);
        let id = [7u8; 16];
        let rmime = b"video/mp4";
        let rframe = crdtsync_client_set_blob_ref(
            a,
            ca,
            pr.as_ptr(),
            pr.len(),
            id.as_ptr(),
            rmime.as_ptr(),
            rmime.len(),
            10_000_000,
        );
        assert!(rframe.len > 0, "a ref blob edit frames its ops");
        assert_eq!(outbox_len(a, ca), 2, "the ref edit entered the outbox");
        assert_eq!(receive(b, &rframe), 1, "the peer folds the ref in");
        crdtsync_buf_free(rframe);

        crdtsync_client_free(a);
        crdtsync_client_free(b);
    }
}

#[test]
fn an_over_ceiling_client_blob_enqueues_nothing() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        crdtsync_buf_free(sa);

        let p = path(&[b"huge"]);
        let mime = b"application/octet-stream";
        let bytes = vec![0u8; 4097];
        let frame = crdtsync_client_set_blob(
            a,
            ca,
            p.as_ptr(),
            p.len(),
            mime.as_ptr(),
            mime.len(),
            bytes.as_ptr(),
            bytes.len(),
        );
        assert_eq!(outbox_len(a, ca), 0, "over the ceiling enqueues no op");
        crdtsync_buf_free(frame);
        crdtsync_client_free(a);
    }
}

/// The grant/revoke authoring surface on the wire client: the op is framed to send,
/// enters the outbox (acked / resent), decodes to the expected `OpKind`, and folds
/// into a peer.
#[test]
fn acl_grant_and_revoke_route_through_the_client_outbox() {
    use crdtsync_core::{AclEffect, AclGrant, AclSubject, Capability, ClientId, OpKind};
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let b = crdtsync_client_new(client_id(2).as_ptr());
        let (ca, sub_a) = subscribe(a, b"room-1");
        let (cb, sub_b) = subscribe(b, b"room-1");
        crdtsync_buf_free(sub_a);
        crdtsync_buf_free(sub_b);
        let _ = cb;

        let subject = client_id(7);
        let grantor = client_id(1);
        let p = path(&[b"doc"]);

        // Author: Allow Write to Actor(7) at /doc through channel `ca`.
        let mut id = out_buf();
        let frame = crdtsync_client_acl_grant(
            a,
            ca,
            0, // subject kind: actor
            subject.as_ptr(),
            subject.len(),
            0, // grant kind: capability
            1, // capability: write
            ptr::null(),
            0,
            0, // effect: allow
            p.as_ptr(),
            p.len(),
            grantor.as_ptr(),
            grantor.len(),
            &mut id,
        );
        assert!(frame.len > 0, "the grant frames an Ops message to send");
        assert_eq!(id.len, 16, "the grant hands back the tuple id");
        assert_eq!(outbox_len(a, ca), 1, "the grant entered the outbox");

        // The framed op decodes to the expected AclGrant.
        let msg = decode_message(std::slice::from_raw_parts(frame.ptr, frame.len)).unwrap();
        let Message::Ops { ops, channel } = msg else {
            panic!("expected an Ops frame");
        };
        assert_eq!(channel, Channel(ca));
        let OpKind::AclGrant {
            subject: subj,
            grant,
            effect,
            grantor: gtor,
            ..
        } = &ops[0].kind
        else {
            panic!("expected AclGrant, got {:?}", ops[0].kind);
        };
        assert_eq!(*subj, AclSubject::Actor(ClientId::from_bytes(subject)));
        assert_eq!(*grant, AclGrant::Capability(Capability::Write));
        assert_eq!(*effect, AclEffect::Allow);
        assert_eq!(*gtor, ClientId::from_bytes(grantor));

        // It folds into the peer.
        assert!(receive(b, &frame) >= 1, "the peer applies the grant");

        // Revoke by the returned id enqueues an AclRevoke.
        let rev = crdtsync_client_acl_revoke(a, ca, id.ptr, id.len);
        assert!(rev.len > 0, "the revoke frames an Ops message");
        assert_eq!(outbox_len(a, ca), 2, "the revoke also entered the outbox");
        let msg = decode_message(std::slice::from_raw_parts(rev.ptr, rev.len)).unwrap();
        let Message::Ops { ops, .. } = msg else {
            panic!("expected an Ops frame");
        };
        let id_bytes = std::slice::from_raw_parts(id.ptr, id.len);
        match &ops[0].kind {
            OpKind::AclRevoke { id: rid } => assert_eq!(rid.as_bytes().as_slice(), id_bytes),
            other => panic!("expected AclRevoke, got {other:?}"),
        }
        assert!(receive(b, &rev) >= 1, "the peer applies the revoke");

        // A bad handle is inert — an empty frame, no panic.
        let empty = crdtsync_client_acl_revoke(ptr::null_mut(), ca, id.ptr, id.len);
        assert_eq!(empty.len, 0);
        crdtsync_buf_free(empty);

        crdtsync_buf_free(id);
        crdtsync_buf_free(frame);
        crdtsync_buf_free(rev);
        crdtsync_client_free(a);
        crdtsync_client_free(b);
    }
}
