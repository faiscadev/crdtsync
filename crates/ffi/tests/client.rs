//! C ABI — the wire client session.
//!
//! A client holds a replica per subscribed room and turns local edits into wire
//! frames to send; folding a peer's frame back in converges the replicas. Frames
//! cross the boundary as encoded byte buffers, a room addressed by the `u32`
//! channel the client assigned at subscribe. Every buffer and handle is freed so
//! the round trip is leak-clean under Miri.

use crdtsync_core::diff::{decode_changes, encode_changes, Change};
use crdtsync_core::protocol::BranchInfo;
use crdtsync_core::{
    decode_message, decode_ops, encode_message, encode_op, Channel, ElementKind, ErrorCode,
    Message, Op, Scalar,
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

unsafe fn subscribe_zone(c: *mut CrdtClient, room: &[u8], zone: &[u8]) -> (u32, CrdtBuf) {
    let mut channel: u32 = u32::MAX;
    let frame = crdtsync_client_subscribe_zone(
        c,
        room.as_ptr(),
        room.len(),
        zone.as_ptr(),
        zone.len(),
        &mut channel,
    );
    (channel, frame)
}

/// The zone a Subscribe frame carries, or panics on any other frame.
unsafe fn subscribe_frame_zone(frame: &CrdtBuf) -> Vec<u8> {
    let bytes = std::slice::from_raw_parts(frame.ptr, frame.len);
    match decode_message(bytes).unwrap() {
        Message::Subscribe { zone, .. } => zone,
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

unsafe fn get_counter(c: *const CrdtClient, channel: u32, p: &[u8]) -> (i32, i64) {
    let mut out: i64 = i64::MIN;
    let rc = crdtsync_client_get_counter(c, channel, p.as_ptr(), p.len(), &mut out);
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
fn subscribe_zone_carries_the_named_zone() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());

        // A named zone rides along in the Subscribe frame, on the default branch.
        let (ch, frame) = subscribe_zone(a, b"room-1", b"west");
        assert_eq!(ch, 0);
        assert_eq!(subscribe_frame_zone(&frame), b"west");
        assert!(subscribe_frame_branch(&frame).is_empty());
        crdtsync_buf_free(frame);

        // An empty zone is the whole room, as the plain subscribe.
        let (ch, frame) = subscribe_zone(a, b"room-1", b"");
        assert_eq!(ch, 1);
        assert!(subscribe_frame_zone(&frame).is_empty());
        crdtsync_buf_free(frame);

        let (_, frame) = subscribe(a, b"room-1");
        assert!(subscribe_frame_zone(&frame).is_empty());
        crdtsync_buf_free(frame);

        // A null handle yields the empty-buffer sentinel and assigns no channel.
        let mut channel: u32 = u32::MAX;
        let frame = crdtsync_client_subscribe_zone(
            ptr::null_mut(),
            b"room-1".as_ptr(),
            6,
            b"west".as_ptr(),
            4,
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
fn a_diff_query_round_trips_over_the_client() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());
        let (ch, sub) = subscribe(c, b"room-1");
        crdtsync_buf_free(sub);

        // A diff query frames a non-empty request — channel-keyed, so the server
        // resolves the room and the scope it narrows to. Both kinds frame; a bad
        // kind frames nothing, and neither does a channel this client does not hold.
        for kind in [0u32, 1u32] {
            let frame = crdtsync_client_diff_query(c, ch, kind, b"a".as_ptr(), 1, b"b".as_ptr(), 1);
            assert!(frame.len > 0, "a diff query frames bytes to send");
            crdtsync_buf_free(frame);
        }
        let bad = crdtsync_client_diff_query(c, ch, 9, b"a".as_ptr(), 1, b"b".as_ptr(), 1);
        assert_eq!(bad.len, 0, "a bad kind frames nothing");
        crdtsync_buf_free(bad);
        let unheld = crdtsync_client_diff_query(c, ch + 1, 0, b"a".as_ptr(), 1, b"b".as_ptr(), 1);
        assert_eq!(unheld.len, 0, "an unheld channel frames nothing");
        crdtsync_buf_free(unheld);

        // No result until one is answered.
        let mut none = out_buf();
        assert_eq!(crdtsync_client_diff_result(c, ch, &mut none), 0);

        // The server's diff result lands in the view, keyed by the channel that
        // asked.
        let expected = Change::Value {
            path: path(&[b"age"]),
            old: Scalar::Int(30),
            new: Scalar::Int(40),
        };
        let result = encode_message(&Message::DiffResult {
            channel: Channel(ch),
            changes: encode_changes(std::slice::from_ref(&expected)),
        });
        assert_eq!(
            crdtsync_client_receive(c, result.as_ptr(), result.len(), ptr::null_mut()),
            1
        );

        let mut got = out_buf();
        assert_eq!(crdtsync_client_diff_result(c, ch, &mut got), 1);
        let raw = std::slice::from_raw_parts(got.ptr, got.len);
        // The buffer feeds the existing diff-decode binding back to the change.
        assert_eq!(decode_changes(raw).unwrap(), vec![expected]);
        let mut decoded = out_buf();
        assert_eq!(crdtsync_diff_decode(got.ptr, got.len, &mut decoded), 1);
        crdtsync_buf_free(decoded);
        crdtsync_buf_free(got);

        crdtsync_client_free(c);
    }
}

#[test]
fn two_channels_of_one_room_read_back_their_own_diff() {
    // A wide channel and a zone-narrowed one on the same room are served genuinely
    // different change lists; each reader gets the answer to its own query.
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());
        let (wide, sub) = subscribe(c, b"room-1");
        crdtsync_buf_free(sub);
        let (narrow, sub) = subscribe_zone(c, b"room-1", b"zone-b");
        crdtsync_buf_free(sub);
        assert_ne!(wide, narrow);

        let wide_change = Change::Added {
            path: path(&[b"root-field"]),
            kind: ElementKind::Map,
        };
        let narrow_change = Change::Added {
            path: path(&[b"zone-b-field"]),
            kind: ElementKind::Map,
        };
        for (channel, change) in [(wide, &wide_change), (narrow, &narrow_change)] {
            let result = encode_message(&Message::DiffResult {
                channel: Channel(channel),
                changes: encode_changes(std::slice::from_ref(change)),
            });
            assert_eq!(
                crdtsync_client_receive(c, result.as_ptr(), result.len(), ptr::null_mut()),
                1
            );
        }

        for (channel, change) in [(wide, wide_change), (narrow, narrow_change)] {
            let mut got = out_buf();
            assert_eq!(crdtsync_client_diff_result(c, channel, &mut got), 1);
            let raw = std::slice::from_raw_parts(got.ptr, got.len);
            assert_eq!(decode_changes(raw).unwrap(), vec![change]);
            crdtsync_buf_free(got);
        }

        crdtsync_client_free(c);
    }
}

#[test]
fn clone_room_round_trips_over_the_client() {
    unsafe {
        let c = crdtsync_client_new(client_id(1).as_ptr());

        // The clone request frames non-empty bytes to send — room-keyed, no
        // subscription needed first.
        let frame = crdtsync_client_clone_room(c, b"template".as_ptr(), 8, b"copy".as_ptr(), 4);
        assert!(frame.len > 0, "a clone request frames bytes to send");
        match decode_message(std::slice::from_raw_parts(frame.ptr, frame.len)).unwrap() {
            Message::CloneRoom { src, dst } => {
                assert_eq!(src, b"template");
                assert_eq!(dst, b"copy");
            }
            other => panic!("expected a CloneRoom, got {other:?}"),
        }
        crdtsync_buf_free(frame);

        // No result until one is answered.
        let mut created = -1i32;
        assert_eq!(
            crdtsync_client_clone_result(c, b"copy".as_ptr(), 4, &mut created),
            0
        );

        // The server's clone result lands in the view, keyed by destination.
        let result = encode_message(&Message::CloneRoomResult {
            dst: b"copy".to_vec(),
            created: true,
        });
        assert_eq!(
            crdtsync_client_receive(c, result.as_ptr(), result.len(), ptr::null_mut()),
            1
        );
        let mut created = -1i32;
        assert_eq!(
            crdtsync_client_clone_result(c, b"copy".as_ptr(), 4, &mut created),
            1
        );
        assert_eq!(created, 1);

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
/// author produced. A mark's anchors name sequence element ids, so both peers must
/// hold the *same* ids: element ids are deterministic in the authoring client id
/// and counter, so a scratch author with one fixed client id and the same text
/// mints identical ids on every side. Authoring in each replica directly would
/// stamp each with its own client id and the anchors would not resolve on the peer.
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

// --- per-channel sequence, scalar, and map surface ---
//
// The list/text/scalar/map reads and edits on a subscribed room, matching the
// doc-level surface but routed through the session: an edit frames its Ops
// message, enters the outbox, and converges on a peer that folds the frame in.

unsafe fn list_len(c: *const CrdtClient, channel: u32, p: &[u8]) -> (i32, usize) {
    let mut out: usize = usize::MAX;
    let rc = crdtsync_client_list_len(c, channel, p.as_ptr(), p.len(), &mut out);
    (rc, out)
}

unsafe fn list_get(c: *const CrdtClient, channel: u32, p: &[u8], index: usize) -> (i32, Vec<u8>) {
    let mut out = out_buf();
    let rc = crdtsync_client_list_get(c, channel, p.as_ptr(), p.len(), index, &mut out);
    let bytes = if out.len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(out.ptr, out.len).to_vec()
    };
    crdtsync_buf_free(out);
    (rc, bytes)
}

unsafe fn text_len(c: *const CrdtClient, channel: u32, p: &[u8]) -> (i32, usize) {
    let mut out: usize = usize::MAX;
    let rc = crdtsync_client_text_len(c, channel, p.as_ptr(), p.len(), &mut out);
    (rc, out)
}

unsafe fn text_get(c: *const CrdtClient, channel: u32, p: &[u8]) -> (i32, String) {
    let mut out = out_buf();
    let rc = crdtsync_client_text_get(c, channel, p.as_ptr(), p.len(), &mut out);
    let s = if out.len == 0 {
        String::new()
    } else {
        String::from_utf8(std::slice::from_raw_parts(out.ptr, out.len).to_vec()).unwrap()
    };
    crdtsync_buf_free(out);
    (rc, s)
}

unsafe fn get_scalar(c: *const CrdtClient, channel: u32, p: &[u8]) -> (i32, Option<Scalar>) {
    let mut out = out_buf();
    let rc = crdtsync_client_get_scalar(c, channel, p.as_ptr(), p.len(), &mut out);
    let value = (out.len > 0)
        .then(|| Scalar::decode_state(std::slice::from_raw_parts(out.ptr, out.len)).unwrap());
    crdtsync_buf_free(out);
    (rc, value)
}

/// Decode the `u32`-count, `u32`-length-prefixed key list `map_keys` frames.
unsafe fn map_keys(c: *const CrdtClient, channel: u32, p: &[u8]) -> (i32, Vec<Vec<u8>>) {
    let mut out = out_buf();
    let rc = crdtsync_client_map_keys(c, channel, p.as_ptr(), p.len(), &mut out);
    let mut keys = Vec::new();
    if out.len > 0 {
        let raw = std::slice::from_raw_parts(out.ptr, out.len);
        let count = u32::from_le_bytes(raw[0..4].try_into().unwrap()) as usize;
        let mut i = 4;
        for _ in 0..count {
            let len = u32::from_le_bytes(raw[i..i + 4].try_into().unwrap()) as usize;
            i += 4;
            keys.push(raw[i..i + len].to_vec());
            i += len;
        }
    }
    crdtsync_buf_free(out);
    keys.sort();
    (rc, keys)
}

#[test]
fn list_edits_route_through_the_client_outbox_and_travel() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let b = crdtsync_client_new(client_id(2).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        let (cb, sb) = subscribe(b, b"room-1");
        crdtsync_buf_free(sa);
        crdtsync_buf_free(sb);

        let p = path(&[b"todos"]);
        assert_eq!(list_len(a, ca, &p), (0, usize::MAX), "no list yet");

        let first = crdtsync_client_list_insert(a, ca, p.as_ptr(), p.len(), 0, b"one".as_ptr(), 3);
        assert!(first.len > 0, "an insert frames its ops");
        let queued = outbox_len(a, ca);
        assert!(queued > 0, "the insert entered the outbox");
        let second = crdtsync_client_list_insert(a, ca, p.as_ptr(), p.len(), 1, b"two".as_ptr(), 3);
        assert!(outbox_len(a, ca) > queued, "so did the second");
        let queued = outbox_len(a, ca);

        assert_eq!(list_len(a, ca, &p), (1, 2));
        assert_eq!(list_get(a, ca, &p, 0), (1, b"one".to_vec()));
        assert_eq!(list_get(a, ca, &p, 1), (1, b"two".to_vec()));

        // The frames carry the channel and converge on the peer.
        let msg = decode_message(std::slice::from_raw_parts(first.ptr, first.len)).unwrap();
        let Message::Ops { channel, .. } = msg else {
            panic!("expected an Ops frame");
        };
        assert_eq!(channel, Channel(ca));
        assert_eq!(receive(b, &first), 1);
        assert_eq!(receive(b, &second), 1);
        assert_eq!(list_len(b, cb, &p), (1, 2));
        assert_eq!(list_get(b, cb, &p, 1), (1, b"two".to_vec()));
        crdtsync_buf_free(first);
        crdtsync_buf_free(second);

        // A delete tombstones the item and travels the same way.
        let del = crdtsync_client_list_delete(a, ca, p.as_ptr(), p.len(), 0);
        assert!(del.len > 0);
        assert!(outbox_len(a, ca) > queued, "the delete entered the outbox");
        assert_eq!(list_len(a, ca, &p), (1, 1));
        assert_eq!(list_get(a, ca, &p, 0), (1, b"two".to_vec()));
        assert_eq!(receive(b, &del), 1);
        assert_eq!(list_len(b, cb, &p), (1, 1));
        crdtsync_buf_free(del);

        // Reading past the live end is absent, not an error.
        assert_eq!(list_get(a, ca, &p, 9), (0, Vec::new()));

        // An index past the live end appends rather than being rejected.
        let queued = outbox_len(a, ca);
        let app =
            crdtsync_client_list_insert(a, ca, p.as_ptr(), p.len(), usize::MAX, b"end".as_ptr(), 3);
        assert!(outbox_len(a, ca) > queued);
        assert_eq!(list_len(a, ca, &p), (1, 2));
        assert_eq!(list_get(a, ca, &p, 1), (1, b"end".to_vec()));
        crdtsync_buf_free(app);

        // A delete naming no live item is inert: the frame carries no ops and the
        // outbox does not grow, so it never installs or re-stamps the List.
        let queued = outbox_len(a, ca);
        let noop = crdtsync_client_list_delete(a, ca, p.as_ptr(), p.len(), usize::MAX);
        assert_eq!(outbox_len(a, ca), queued, "a no-op delete enqueues nothing");
        crdtsync_buf_free(noop);
        let absent = path(&[b"nowhere"]);
        let noop = crdtsync_client_list_delete(a, ca, absent.as_ptr(), absent.len(), 0);
        assert_eq!(outbox_len(a, ca), queued, "nor does one on an absent path");
        assert_eq!(
            list_len(a, ca, &absent),
            (0, usize::MAX),
            "no List installed"
        );
        crdtsync_buf_free(noop);

        // The reads discriminate on element type: a List is not a Text.
        assert_eq!(text_len(a, ca, &p), (0, usize::MAX));
        assert_eq!(text_get(a, ca, &p), (0, String::new()));

        crdtsync_client_free(a);
        crdtsync_client_free(b);
    }
}

#[test]
fn text_edits_route_through_the_client_outbox_and_travel() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let b = crdtsync_client_new(client_id(2).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        let (cb, sb) = subscribe(b, b"room-1");
        crdtsync_buf_free(sa);
        crdtsync_buf_free(sb);

        let p = path(&[b"body"]);
        assert_eq!(text_len(a, ca, &p), (0, usize::MAX), "no text yet");

        let hello = "héllo".as_bytes();
        let ins =
            crdtsync_client_text_insert(a, ca, p.as_ptr(), p.len(), 0, hello.as_ptr(), hello.len());
        assert!(ins.len > 0, "an insert frames its ops");
        let queued = outbox_len(a, ca);
        assert!(queued > 0, "the insert entered the outbox");
        // Length is counted in codepoints, not bytes.
        assert_eq!(text_len(a, ca, &p), (1, 5));
        assert_eq!(text_get(a, ca, &p), (1, "héllo".to_string()));
        assert_eq!(receive(b, &ins), 1);
        assert_eq!(text_get(b, cb, &p), (1, "héllo".to_string()));
        crdtsync_buf_free(ins);

        // Deleting two codepoints from index 1 works on codepoints too.
        let del = crdtsync_client_text_delete(a, ca, p.as_ptr(), p.len(), 1, 2);
        assert!(del.len > 0);
        assert!(outbox_len(a, ca) > queued, "the delete entered the outbox");
        let queued = outbox_len(a, ca);
        assert_eq!(text_get(a, ca, &p), (1, "hlo".to_string()));
        assert_eq!(receive(b, &del), 1);
        assert_eq!(text_get(b, cb, &p), (1, "hlo".to_string()));
        assert_eq!(text_len(b, cb, &p), (1, 3));
        crdtsync_buf_free(del);

        // Non-UTF-8 input is rejected without enqueueing anything.
        let bad = crdtsync_client_text_insert(a, ca, p.as_ptr(), p.len(), 0, [0xffu8].as_ptr(), 1);
        assert_eq!(bad.len, 0, "invalid UTF-8 frames nothing");
        assert_eq!(outbox_len(a, ca), queued, "and enqueues nothing");

        // An index past the live end appends rather than being rejected.
        let queued = outbox_len(a, ca);
        let app =
            crdtsync_client_text_insert(a, ca, p.as_ptr(), p.len(), usize::MAX, b"!".as_ptr(), 1);
        assert!(outbox_len(a, ca) > queued);
        assert_eq!(text_get(a, ca, &p), (1, "hlo!".to_string()));
        crdtsync_buf_free(app);

        // A delete naming no live codepoint is inert — including a zero count, and
        // including on a path holding no Text at all.
        let queued = outbox_len(a, ca);
        for noop in [
            crdtsync_client_text_delete(a, ca, p.as_ptr(), p.len(), usize::MAX, 1),
            crdtsync_client_text_delete(a, ca, p.as_ptr(), p.len(), 0, 0),
        ] {
            crdtsync_buf_free(noop);
        }
        assert_eq!(outbox_len(a, ca), queued, "a no-op delete enqueues nothing");
        let absent = path(&[b"nowhere"]);
        let noop = crdtsync_client_text_delete(a, ca, absent.as_ptr(), absent.len(), 0, 1);
        assert_eq!(outbox_len(a, ca), queued, "nor does one on an absent path");
        assert_eq!(
            text_len(a, ca, &absent),
            (0, usize::MAX),
            "no Text installed"
        );
        crdtsync_buf_free(noop);

        // The reads discriminate on element type: a Text is not a List.
        assert_eq!(list_len(a, ca, &p), (0, usize::MAX));
        assert_eq!(list_get(a, ca, &p, 0), (0, Vec::new()));
        crdtsync_buf_free(bad);

        crdtsync_client_free(a);
        crdtsync_client_free(b);
    }
}

#[test]
fn scalars_keep_their_type_across_a_client_round_trip() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let b = crdtsync_client_new(client_id(2).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        let (cb, sb) = subscribe(b, b"room-1");
        crdtsync_buf_free(sa);
        crdtsync_buf_free(sb);

        let p = path(&[b"flag"]);
        let encoded = Scalar::Bool(true).encode_state();
        let frame =
            crdtsync_client_set_scalar(a, ca, p.as_ptr(), p.len(), encoded.as_ptr(), encoded.len());
        assert!(frame.len > 0, "a scalar set frames its ops");
        let queued = outbox_len(a, ca);
        assert!(queued > 0, "the set entered the outbox");
        assert_eq!(get_scalar(a, ca, &p), (1, Some(Scalar::Bool(true))));

        // The type survives the wire, not just the local write.
        assert_eq!(receive(b, &frame), 1);
        assert_eq!(get_scalar(b, cb, &p), (1, Some(Scalar::Bool(true))));
        crdtsync_buf_free(frame);

        // A malformed payload frames nothing and enqueues nothing.
        let bad =
            crdtsync_client_set_scalar(a, ca, p.as_ptr(), p.len(), [0xffu8, 0xff].as_ptr(), 2);
        assert_eq!(bad.len, 0);
        assert_eq!(
            outbox_len(a, ca),
            queued,
            "a malformed payload enqueues nothing"
        );
        crdtsync_buf_free(bad);

        // A slot holding no register reads absent.
        let missing = path(&[b"nope"]);
        assert_eq!(get_scalar(a, ca, &missing), (0, None));

        crdtsync_client_free(a);
        crdtsync_client_free(b);
    }
}

#[test]
fn map_keys_enumerates_a_rooms_live_slots() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        crdtsync_buf_free(sa);

        crdtsync_buf_free(register_int(a, ca, &path(&[b"m", b"x"]), 1));
        crdtsync_buf_free(register_int(a, ca, &path(&[b"m", b"y"]), 2));

        let mp = path(&[b"m"]);
        let (rc, keys) = map_keys(a, ca, &mp);
        assert_eq!(rc, 1, "a live map reports its keys");
        assert_eq!(keys, vec![b"x".to_vec(), b"y".to_vec()]);

        // The root map is named by the empty path.
        let (rc, keys) = map_keys(a, ca, &[]);
        assert_eq!(rc, 1);
        assert_eq!(keys, vec![b"m".to_vec()]);

        // A leaf is not a map — 0, distinct from an empty map's 1.
        let leaf = path(&[b"m", b"x"]);
        assert_eq!(map_keys(a, ca, &leaf), (0, Vec::new()));

        // Emptying the map keeps it live: the status stays 1 with no keys, which
        // is what separates "a map with nothing in it" from "not a map".
        for key in [b"x", b"y"] {
            let slot = path(&[b"m", key]);
            crdtsync_buf_free(crdtsync_client_delete(a, ca, slot.as_ptr(), slot.len()));
        }
        assert_eq!(map_keys(a, ca, &mp), (1, Vec::new()));

        // An XmlElement names its attrs map, so it reports 1 as well.
        let el = path(&[b"el"]);
        crdtsync_buf_free(crdtsync_client_xml_element(
            a,
            ca,
            el.as_ptr(),
            el.len(),
            b"p".as_ptr(),
            1,
        ));
        assert_eq!(map_keys(a, ca, &el), (1, Vec::new()));

        crdtsync_client_free(a);
    }
}

#[test]
fn the_per_channel_surface_addresses_one_room_at_a_time() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let (c1, s1) = subscribe(a, b"room-1");
        let (c2, s2) = subscribe(a, b"room-2");
        crdtsync_buf_free(s1);
        crdtsync_buf_free(s2);
        assert_ne!(c1, c2);

        let p = path(&[b"notes"]);
        crdtsync_buf_free(crdtsync_client_text_insert(
            a,
            c1,
            p.as_ptr(),
            p.len(),
            0,
            b"one-room".as_ptr(),
            8,
        ));
        assert_eq!(text_get(a, c1, &p), (1, "one-room".to_string()));
        assert_eq!(text_len(a, c2, &p), (0, usize::MAX), "the sibling is empty");
        assert_eq!(outbox_len(a, c2), 0);

        // An unheld channel holds no replica: reads are absent, edits inert.
        let unheld = 99;
        assert_eq!(text_len(a, unheld, &p), (0, usize::MAX));
        assert_eq!(list_len(a, unheld, &p), (0, usize::MAX));
        assert_eq!(get_scalar(a, unheld, &p), (0, None));
        assert_eq!(map_keys(a, unheld, &[]), (0, Vec::new()));
        let inert =
            crdtsync_client_list_insert(a, unheld, p.as_ptr(), p.len(), 0, b"x".as_ptr(), 1);
        assert_eq!(inert.len, 0, "an unheld channel frames nothing");
        crdtsync_buf_free(inert);

        crdtsync_client_free(a);
    }
}

#[test]
fn the_per_channel_surface_rejects_null_handles() {
    unsafe {
        let p = path(&[b"k"]);
        let scalar = Scalar::Bool(true).encode_state();
        let mut buf = out_buf();
        let mut n: usize = 0;

        for frame in [
            crdtsync_client_list_insert(
                ptr::null_mut(),
                0,
                p.as_ptr(),
                p.len(),
                0,
                b"x".as_ptr(),
                1,
            ),
            crdtsync_client_list_delete(ptr::null_mut(), 0, p.as_ptr(), p.len(), 0),
            crdtsync_client_text_insert(
                ptr::null_mut(),
                0,
                p.as_ptr(),
                p.len(),
                0,
                b"a".as_ptr(),
                1,
            ),
            crdtsync_client_text_delete(ptr::null_mut(), 0, p.as_ptr(), p.len(), 0, 1),
            crdtsync_client_set_scalar(
                ptr::null_mut(),
                0,
                p.as_ptr(),
                p.len(),
                scalar.as_ptr(),
                scalar.len(),
            ),
        ] {
            assert_eq!(frame.len, 0, "a null handle frames nothing");
            crdtsync_buf_free(frame);
        }

        assert_eq!(
            crdtsync_client_list_len(ptr::null(), 0, p.as_ptr(), p.len(), &mut n),
            -1
        );
        assert_eq!(
            crdtsync_client_text_len(ptr::null(), 0, p.as_ptr(), p.len(), &mut n),
            -1
        );
        assert_eq!(
            crdtsync_client_list_get(ptr::null(), 0, p.as_ptr(), p.len(), 0, &mut buf),
            -1
        );
        assert_eq!(
            crdtsync_client_text_get(ptr::null(), 0, p.as_ptr(), p.len(), &mut buf),
            -1
        );
        assert_eq!(
            crdtsync_client_get_scalar(ptr::null(), 0, p.as_ptr(), p.len(), &mut buf),
            -1
        );
        assert_eq!(
            crdtsync_client_map_keys(ptr::null(), 0, p.as_ptr(), p.len(), &mut buf),
            -1
        );

        // A null output pointer is rejected the same way, with a live handle.
        let a = crdtsync_client_new(client_id(1).as_ptr());
        assert_eq!(
            crdtsync_client_list_len(a, 0, p.as_ptr(), p.len(), ptr::null_mut()),
            -1
        );
        assert_eq!(
            crdtsync_client_text_get(a, 0, p.as_ptr(), p.len(), ptr::null_mut()),
            -1
        );

        // A null payload or path pointer with a nonzero length is rejected rather
        // than dereferenced, and leaves the outbox untouched.
        let (ca, sa) = subscribe(a, b"room-1");
        crdtsync_buf_free(sa);
        for frame in [
            crdtsync_client_list_insert(a, ca, p.as_ptr(), p.len(), 0, ptr::null(), 3),
            crdtsync_client_text_insert(a, ca, p.as_ptr(), p.len(), 0, ptr::null(), 3),
            crdtsync_client_set_scalar(a, ca, p.as_ptr(), p.len(), ptr::null(), 3),
            crdtsync_client_list_insert(a, ca, ptr::null(), 4, 0, b"x".as_ptr(), 1),
            crdtsync_client_text_insert(a, ca, ptr::null(), 4, 0, b"x".as_ptr(), 1),
        ] {
            assert_eq!(frame.len, 0, "a rejected pointer frames nothing");
            crdtsync_buf_free(frame);
        }
        assert_eq!(outbox_len(a, ca), 0, "and enqueues nothing");

        crdtsync_client_free(a);
    }
}

// --- client channel state, blobs, marks, anchors, and xml reads ---
//
// The per-channel reads addressed at one subscribed room: its canonical
// snapshot, blob refs, resolved marks, anchors, and xml shape.

/// The state buffer of `channel`'s replica and the status, as an owned copy.
unsafe fn channel_state(c: *const CrdtClient, channel: u32) -> (i32, Vec<u8>) {
    let mut out = out_buf();
    let rc = crdtsync_client_channel_state(c, channel, &mut out);
    let bytes = if out.len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(out.ptr, out.len).to_vec()
    };
    crdtsync_buf_free(out);
    (rc, bytes)
}

/// The changes turning one channel snapshot into another: the state pair through
/// `crdtsync_diff`, decoded with the change reader.
unsafe fn diff_states(old: &[u8], new: &[u8]) -> Vec<Change> {
    let buf = crdtsync_diff(old.as_ptr(), old.len(), new.as_ptr(), new.len());
    let changes = decode_changes(std::slice::from_raw_parts(buf.ptr, buf.len)).unwrap();
    crdtsync_buf_free(buf);
    changes
}

/// A blob ref decoded from the client's `get_blob` framing: id, mime, size, and
/// the inline bytes when the payload rode along.
struct DecodedBlob {
    id: [u8; 16],
    mime: String,
    size: u64,
    inline: Option<Vec<u8>>,
}

fn decode_blob(b: &[u8]) -> DecodedBlob {
    let mut id = [0u8; 16];
    id.copy_from_slice(&b[..16]);
    let mut i = 16;
    let mime_len = u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as usize;
    i += 4;
    let mime = String::from_utf8(b[i..i + mime_len].to_vec()).unwrap();
    i += mime_len;
    let size = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
    i += 8;
    let present = b[i];
    i += 1;
    let inline = if present == 1 {
        let n = u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as usize;
        let start = i + 4;
        i = start + n;
        Some(b[start..i].to_vec())
    } else {
        None
    };
    assert_eq!(i, b.len(), "the blob framing is fully consumed");
    DecodedBlob {
        id,
        mime,
        size,
        inline,
    }
}

unsafe fn get_blob(c: *const CrdtClient, channel: u32, p: &[u8]) -> (i32, Option<DecodedBlob>) {
    let mut out = out_buf();
    let rc = crdtsync_client_get_blob(c, channel, p.as_ptr(), p.len(), &mut out);
    let blob = (out.len > 0).then(|| decode_blob(std::slice::from_raw_parts(out.ptr, out.len)));
    crdtsync_buf_free(out);
    (rc, blob)
}

/// The name of each resolved mark in a `marks_at` buffer — the `u32` count, then
/// per mark a `u32`-length-prefixed name, a flavor tag, and that tag's payload.
fn parse_mark_names(buf: &[u8]) -> Vec<Vec<u8>> {
    let u32_at = |b: &[u8], i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as usize;
    let mut c = 0usize;
    let count = u32_at(buf, c);
    c += 4;
    let mut names = Vec::new();
    for _ in 0..count {
        let nl = u32_at(buf, c);
        c += 4;
        names.push(buf[c..c + nl].to_vec());
        c += nl;
        let tag = buf[c];
        c += 1;
        match tag {
            0 => c += 1,
            1 => {
                let vl = u32_at(buf, c);
                c += 4 + vl;
            }
            2 => {
                let n = u32_at(buf, c);
                c += 4 + n * 16;
            }
            _ => panic!("unknown mark flavor tag {tag}"),
        }
    }
    assert_eq!(c, buf.len(), "the marks framing is fully consumed");
    names
}

unsafe fn marks_at(
    c: *const CrdtClient,
    channel: u32,
    p: &[u8],
    index: usize,
) -> (i32, Vec<Vec<u8>>) {
    let mut out = out_buf();
    let rc = crdtsync_client_marks_at(c, channel, p.as_ptr(), p.len(), index, &mut out);
    let names = if out.len == 0 {
        Vec::new()
    } else {
        parse_mark_names(std::slice::from_raw_parts(out.ptr, out.len))
    };
    crdtsync_buf_free(out);
    (rc, names)
}

unsafe fn relative_position(
    c: *const CrdtClient,
    channel: u32,
    p: &[u8],
    index: usize,
    side: u32,
) -> (i32, Vec<u8>) {
    let mut out = out_buf();
    let rc =
        crdtsync_client_relative_position(c, channel, p.as_ptr(), p.len(), index, side, &mut out);
    let bytes = if out.len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(out.ptr, out.len).to_vec()
    };
    crdtsync_buf_free(out);
    (rc, bytes)
}

unsafe fn resolve_position(
    c: *const CrdtClient,
    channel: u32,
    p: &[u8],
    pos: &[u8],
) -> (i32, usize) {
    let mut out: usize = usize::MAX;
    let rc = crdtsync_client_resolve_position(
        c,
        channel,
        p.as_ptr(),
        p.len(),
        pos.as_ptr(),
        pos.len(),
        &mut out,
    );
    (rc, out)
}

unsafe fn xml_tag(c: *const CrdtClient, channel: u32, p: &[u8]) -> (i32, Vec<u8>) {
    let mut out = out_buf();
    let rc = crdtsync_client_xml_tag(c, channel, p.as_ptr(), p.len(), &mut out);
    let tag = if out.len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(out.ptr, out.len).to_vec()
    };
    crdtsync_buf_free(out);
    (rc, tag)
}

unsafe fn xml_children_len(c: *const CrdtClient, channel: u32, p: &[u8]) -> (i32, usize) {
    let mut out: usize = usize::MAX;
    let rc = crdtsync_client_xml_children_len(c, channel, p.as_ptr(), p.len(), &mut out);
    (rc, out)
}

#[test]
fn a_counter_reads_back_over_the_client() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let b = crdtsync_client_new(client_id(2).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        let (cb, sb) = subscribe(b, b"room-1");
        crdtsync_buf_free(sa);
        crdtsync_buf_free(sb);

        let hits = path(&[b"hits"]);
        let up = crdtsync_client_inc(a, ca, hits.as_ptr(), hits.len(), 7);
        let down = crdtsync_client_dec(a, ca, hits.as_ptr(), hits.len(), 2);
        assert_eq!(get_counter(a, ca, &hits), (1, 5));

        // The peer's replica converges on the same value from the frames alone.
        assert_eq!(receive(b, &up), 1);
        assert_eq!(receive(b, &down), 1);
        assert_eq!(get_counter(b, cb, &hits), (1, 5));
        crdtsync_buf_free(up);
        crdtsync_buf_free(down);

        // A register is not a counter, and an absent slot is neither — both read
        // absent with `out` untouched.
        let n = path(&[b"n"]);
        crdtsync_buf_free(register_int(a, ca, &n, 1));
        assert_eq!(get_counter(a, ca, &n), (0, i64::MIN));
        assert_eq!(get_counter(a, ca, &path(&[b"nope"])), (0, i64::MIN));
        assert_eq!(get_counter(a, 99, &hits), (0, i64::MIN));
        assert_eq!(
            crdtsync_client_get_counter(ptr::null(), 0, hits.as_ptr(), hits.len(), &mut 0i64),
            -1
        );
        assert_eq!(
            crdtsync_client_get_counter(a, ca, hits.as_ptr(), hits.len(), ptr::null_mut()),
            -1
        );

        crdtsync_client_free(a);
        crdtsync_client_free(b);
    }
}

#[test]
fn channel_state_snapshots_the_room_an_sdk_diffs() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        crdtsync_buf_free(sa);

        // A held-but-untouched channel snapshots: 1 with the empty room's state,
        // which is what separates "nothing here yet" from "no such channel".
        let (rc, before) = channel_state(a, ca);
        assert_eq!(rc, 1, "a held channel snapshots its replica");
        assert!(
            !before.is_empty(),
            "an empty room still snapshots to a non-empty buffer"
        );

        let p = path(&[b"age"]);
        let seed = register_int(a, ca, &p, 41);
        let (rc, seeded) = channel_state(a, ca);
        assert_eq!(rc, 1);
        assert_ne!(before, seeded, "the edit moved the snapshot");

        let bump = register_int(a, ca, &p, 42);
        let (_, after) = channel_state(a, ca);

        // The pair is exactly what the ergonomic SDKs diff to derive the change
        // events an inbound frame never surfaces on its own.
        assert_eq!(
            diff_states(&seeded, &after),
            vec![Change::Value {
                path: p.clone(),
                old: Scalar::Int(41),
                new: Scalar::Int(42),
            }]
        );

        // And the buffer is a real snapshot: it reopens as a document.
        let reopened = crdtsync_doc_decode_state(after.as_ptr(), after.len());
        assert!(!reopened.is_null(), "the state buffer reopens");
        let mut n: i64 = 0;
        assert_eq!(
            crdtsync_doc_get_int(reopened, p.as_ptr(), p.len(), &mut n),
            1
        );
        assert_eq!(n, 42);
        crdtsync_doc_free(reopened);

        // A peer's inbound frame moves the snapshot the same way, so the same
        // diff derives change the peer never saw as a local edit.
        let b = crdtsync_client_new(client_id(2).as_ptr());
        let (cb, sb) = subscribe(b, b"room-1");
        crdtsync_buf_free(sb);
        assert_eq!(receive(b, &seed), 1);
        let (_, b_before) = channel_state(b, cb);
        assert_eq!(receive(b, &bump), 1);
        let (_, b_after) = channel_state(b, cb);
        assert_eq!(
            diff_states(&b_before, &b_after),
            vec![Change::Value {
                path: p,
                old: Scalar::Int(41),
                new: Scalar::Int(42),
            }]
        );

        crdtsync_buf_free(seed);
        crdtsync_buf_free(bump);
        crdtsync_client_free(a);
        crdtsync_client_free(b);
    }
}

#[test]
fn a_blob_reads_back_over_the_client() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        crdtsync_buf_free(sa);

        let p = path(&[b"avatar"]);
        let bytes = [0x89u8, b'P', b'N', b'G', 0x00, 0xFF];
        crdtsync_buf_free(crdtsync_client_set_blob(
            a,
            ca,
            p.as_ptr(),
            p.len(),
            b"image/png".as_ptr(),
            9,
            bytes.as_ptr(),
            bytes.len(),
        ));

        let (rc, blob) = get_blob(a, ca, &p);
        assert_eq!(rc, 1, "a live blob ref reads back");
        let blob = blob.unwrap();
        assert_eq!(blob.mime, "image/png");
        assert_eq!(blob.size, bytes.len() as u64);
        assert_eq!(blob.inline.as_deref(), Some(&bytes[..]));
        assert_ne!(blob.id, [0u8; 16], "a minted handle is not all-zero");

        // An out-of-band ref carries the caller's handle and no bytes.
        let vid = path(&[b"clip"]);
        let id = [7u8; 16];
        crdtsync_buf_free(crdtsync_client_set_blob_ref(
            a,
            ca,
            vid.as_ptr(),
            vid.len(),
            id.as_ptr(),
            b"video/mp4".as_ptr(),
            9,
            10_000_000,
        ));
        let (rc, blob) = get_blob(a, ca, &vid);
        assert_eq!(rc, 1);
        let blob = blob.unwrap();
        assert_eq!(blob.id, id);
        assert_eq!(blob.size, 10_000_000);
        assert_eq!(blob.inline, None, "a ref carries no inline bytes");

        // A slot holding something else, and an absent one, both read absent.
        crdtsync_buf_free(register_int(a, ca, &path(&[b"n"]), 1));
        assert_eq!(get_blob(a, ca, &path(&[b"n"])).0, 0);
        assert_eq!(get_blob(a, ca, &path(&[b"nope"])).0, 0);

        crdtsync_client_free(a);
    }
}

#[test]
fn marks_read_back_and_isolate_channels() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        let (cb, sb) = subscribe(a, b"room-2");
        crdtsync_buf_free(sa);
        crdtsync_buf_free(sb);

        let body = path(&[b"body"]);
        seed_body_text(a, ca, &body, "hello world");
        let value = Scalar::Bool(true).encode_state();
        let mut mid = out_buf();
        crdtsync_buf_free(crdtsync_client_mark(
            a,
            ca,
            body.as_ptr(),
            body.len(),
            0,
            0,
            5,
            0,
            b"bold".as_ptr(),
            4,
            value.as_ptr(),
            value.len(),
            &mut mid,
        ));
        assert_eq!(mid.len, 16, "the author returns the mark handle");
        crdtsync_buf_free(mid);

        assert_eq!(marks_at(a, ca, &body, 2), (1, vec![b"bold".to_vec()]));
        assert_eq!(
            marks_at(a, ca, &body, 7),
            (1, Vec::new()),
            "an uncovered index resolves to no marks"
        );

        // The sibling room carries neither the text nor the mark, so the same
        // path there resolves to nothing — the read addresses one channel.
        assert_eq!(marks_at(a, cb, &body, 2), (1, Vec::new()));

        // A live non-sequence slot, and an absent one, each resolve to no marks
        // rather than an error.
        let n = path(&[b"n"]);
        crdtsync_buf_free(register_int(a, ca, &n, 1));
        assert_eq!(marks_at(a, ca, &n, 0), (1, Vec::new()));
        assert_eq!(marks_at(a, ca, &path(&[b"nope"]), 0), (1, Vec::new()));

        crdtsync_client_free(a);
    }
}

#[test]
fn anchors_capture_and_resolve_over_the_client() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        crdtsync_buf_free(sa);

        let body = path(&[b"body"]);
        seed_body_text(a, ca, &body, "hello world");

        // Two anchors at the "world" boundary, one bound to the character on
        // each side of it. Both read 6 until something lands in the gap.
        let (rc, left) = relative_position(a, ca, &body, 6, 0);
        assert_eq!(rc, 1, "a live sequence captures an anchor");
        assert!(!left.is_empty());
        let (rc, right) = relative_position(a, ca, &body, 6, 1);
        assert_eq!(rc, 1);
        assert_ne!(left, right, "the sides bind to different characters");
        assert_eq!(resolve_position(a, ca, &body, &left), (1, 6));
        assert_eq!(resolve_position(a, ca, &body, &right), (1, 6));

        // An insert at the gap separates them: the left anchor stays behind it,
        // the right anchor rides ahead of it.
        crdtsync_buf_free(crdtsync_client_text_insert(
            a,
            ca,
            body.as_ptr(),
            body.len(),
            6,
            b"big ".as_ptr(),
            4,
        ));
        assert_eq!(resolve_position(a, ca, &body, &left), (1, 6));
        assert_eq!(resolve_position(a, ca, &body, &right), (1, 10));

        // An insert before the whole span carries both along.
        crdtsync_buf_free(crdtsync_client_text_insert(
            a,
            ca,
            body.as_ptr(),
            body.len(),
            0,
            b">> ".as_ptr(),
            3,
        ));
        assert_eq!(resolve_position(a, ca, &body, &left), (1, 9));
        assert_eq!(resolve_position(a, ca, &body, &right), (1, 13));

        // A List anchors the same way — the entry points take either sequence.
        let items = path(&[b"items"]);
        for v in [b"a", b"b", b"c"] {
            crdtsync_buf_free(crdtsync_client_list_insert(
                a,
                ca,
                items.as_ptr(),
                items.len(),
                usize::MAX,
                v.as_ptr(),
                1,
            ));
        }
        let (rc, at_c) = relative_position(a, ca, &items, 2, 1);
        assert_eq!(rc, 1);
        assert_eq!(resolve_position(a, ca, &items, &at_c), (1, 2));
        crdtsync_buf_free(crdtsync_client_list_insert(
            a,
            ca,
            items.as_ptr(),
            items.len(),
            0,
            b"z".as_ptr(),
            1,
        ));
        assert_eq!(resolve_position(a, ca, &items, &at_c), (1, 3));

        // A non-sequence slot captures nothing; an unknown side is refused. A
        // refused capture leaves `out` untouched, so the buffer stays empty.
        crdtsync_buf_free(register_int(a, ca, &path(&[b"n"]), 1));
        assert_eq!(
            relative_position(a, ca, &path(&[b"n"]), 0, 0),
            (0, Vec::new())
        );
        assert_eq!(
            relative_position(a, ca, &body, 0, 9),
            (0, Vec::new()),
            "an unknown side is refused"
        );

        // Malformed position bytes resolve to nothing rather than panicking, and
        // leave the out index untouched.
        assert_eq!(
            resolve_position(a, ca, &body, &[0xff, 0xff]),
            (0, usize::MAX)
        );

        crdtsync_client_free(a);
    }
}

#[test]
fn xml_shape_reads_back_over_the_client() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        crdtsync_buf_free(sa);

        let root = path(&[b"doc"]);
        crdtsync_buf_free(crdtsync_client_xml_element(
            a,
            ca,
            root.as_ptr(),
            root.len(),
            b"article".as_ptr(),
            7,
        ));
        assert_eq!(xml_tag(a, ca, &root), (1, b"article".to_vec()));
        assert_eq!(xml_children_len(a, ca, &root), (1, 0));

        for tag in [b"p1", b"p2"] {
            crdtsync_buf_free(crdtsync_client_xml_insert_element(
                a,
                ca,
                root.as_ptr(),
                root.len(),
                0,
                tag.as_ptr(),
                2,
            ));
        }
        assert_eq!(xml_children_len(a, ca, &root), (1, 2));

        // A fragment is tagless, so it reports no tag but still counts children.
        let frag = path(&[b"frag"]);
        crdtsync_buf_free(crdtsync_client_xml_fragment(
            a,
            ca,
            frag.as_ptr(),
            frag.len(),
        ));
        assert_eq!(xml_tag(a, ca, &frag), (0, Vec::new()));
        assert_eq!(xml_children_len(a, ca, &frag), (1, 0));
        crdtsync_buf_free(crdtsync_client_xml_insert_element(
            a,
            ca,
            frag.as_ptr(),
            frag.len(),
            0,
            b"li".as_ptr(),
            2,
        ));
        assert_eq!(xml_children_len(a, ca, &frag), (1, 1));

        // A non-xml slot is neither.
        let n = path(&[b"n"]);
        crdtsync_buf_free(register_int(a, ca, &n, 1));
        assert_eq!(xml_tag(a, ca, &n), (0, Vec::new()));
        assert_eq!(xml_children_len(a, ca, &n), (0, usize::MAX));

        crdtsync_client_free(a);
    }
}

#[test]
fn the_channel_reads_address_one_room_and_refuse_an_unheld_channel() {
    unsafe {
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let (c1, s1) = subscribe(a, b"room-1");
        let (c2, s2) = subscribe(a, b"room-2");
        crdtsync_buf_free(s1);
        crdtsync_buf_free(s2);
        assert_ne!(c1, c2);

        // Everything under test lives in room-1 only, and each slot read reports
        // 1 there — so a 0 on the sibling or an unheld channel can only come
        // from the channel, not from the path naming nothing anywhere. The mark
        // read never reports 0 for a path, so it only appears below.
        let body = path(&[b"body"]);
        seed_body_text(a, c1, &body, "hello world");
        let el = path(&[b"doc"]);
        crdtsync_buf_free(crdtsync_client_xml_element(
            a,
            c1,
            el.as_ptr(),
            el.len(),
            b"article".as_ptr(),
            7,
        ));
        let pic = path(&[b"pic"]);
        crdtsync_buf_free(crdtsync_client_set_blob_ref(
            a,
            c1,
            pic.as_ptr(),
            pic.len(),
            [3u8; 16].as_ptr(),
            b"image/png".as_ptr(),
            9,
            64,
        ));
        let (rc, anchor) = relative_position(a, c1, &body, 6, 0);
        assert_eq!(rc, 1);
        assert_eq!(xml_tag(a, c1, &el), (1, b"article".to_vec()));
        assert_eq!(xml_children_len(a, c1, &el), (1, 0));
        assert_eq!(get_blob(a, c1, &pic).0, 1);
        assert_eq!(resolve_position(a, c1, &body, &anchor), (1, 6));

        // The sibling room holds none of it.
        assert_eq!(xml_tag(a, c2, &el), (0, Vec::new()));
        assert_eq!(xml_children_len(a, c2, &el), (0, usize::MAX));
        assert_eq!(relative_position(a, c2, &body, 0, 0), (0, Vec::new()));
        assert_eq!(get_blob(a, c2, &pic).0, 0);
        assert_eq!(resolve_position(a, c2, &body, &anchor), (0, usize::MAX));
        let (rc1, state1) = channel_state(a, c1);
        let (rc2, state2) = channel_state(a, c2);
        assert_eq!((rc1, rc2), (1, 1), "both channels are held");
        assert_ne!(state1, state2, "each channel snapshots its own replica");

        // An unheld channel holds no replica at all: every read is 0, distinct
        // from the -1 a bad handle reports.
        let unheld = 99;
        assert_eq!(channel_state(a, unheld), (0, Vec::new()));
        assert_eq!(get_blob(a, unheld, &pic).0, 0);
        assert_eq!(marks_at(a, unheld, &body, 0), (0, Vec::new()));
        assert_eq!(relative_position(a, unheld, &body, 0, 0), (0, Vec::new()));
        assert_eq!(resolve_position(a, unheld, &body, &anchor), (0, usize::MAX));
        assert_eq!(xml_tag(a, unheld, &el), (0, Vec::new()));
        assert_eq!(xml_children_len(a, unheld, &el), (0, usize::MAX));

        // Unsubscribing drops the replica, so a channel that was held reads the
        // same absent as one that never was.
        crdtsync_buf_free(crdtsync_client_unsubscribe(a, c1));
        assert_eq!(channel_state(a, c1), (0, Vec::new()));
        assert_eq!(get_blob(a, c1, &pic).0, 0);
        assert_eq!(marks_at(a, c1, &body, 0), (0, Vec::new()));
        assert_eq!(relative_position(a, c1, &body, 0, 0), (0, Vec::new()));
        assert_eq!(resolve_position(a, c1, &body, &anchor), (0, usize::MAX));
        assert_eq!(xml_tag(a, c1, &el), (0, Vec::new()));
        assert_eq!(xml_children_len(a, c1, &el), (0, usize::MAX));

        crdtsync_client_free(a);
    }
}

#[test]
fn the_channel_reads_reject_null_handles_and_pointers() {
    unsafe {
        let p = path(&[b"k"]);
        let mut buf = out_buf();
        let mut n: usize = 0;

        // A null handle is rejected before any payload is looked at — each call
        // here passes a well-formed path and side so the null check is what
        // fires, not an earlier validation guard.
        assert_eq!(crdtsync_client_channel_state(ptr::null(), 0, &mut buf), -1);
        assert_eq!(
            crdtsync_client_get_blob(ptr::null(), 0, p.as_ptr(), p.len(), &mut buf),
            -1
        );
        assert_eq!(
            crdtsync_client_marks_at(ptr::null(), 0, p.as_ptr(), p.len(), 0, &mut buf),
            -1
        );
        assert_eq!(
            crdtsync_client_relative_position(ptr::null(), 0, p.as_ptr(), p.len(), 0, 0, &mut buf),
            -1
        );
        assert_eq!(
            crdtsync_client_resolve_position(
                ptr::null(),
                0,
                p.as_ptr(),
                p.len(),
                [0u8; 4].as_ptr(),
                4,
                &mut n
            ),
            -1
        );
        assert_eq!(
            crdtsync_client_xml_tag(ptr::null(), 0, p.as_ptr(), p.len(), &mut buf),
            -1
        );
        assert_eq!(
            crdtsync_client_xml_children_len(ptr::null(), 0, p.as_ptr(), p.len(), &mut n),
            -1
        );

        // A null output pointer is rejected the same way, with a live handle and
        // a held channel — so the guard under test is the `out` check.
        let a = crdtsync_client_new(client_id(1).as_ptr());
        let (ca, sa) = subscribe(a, b"room-1");
        crdtsync_buf_free(sa);
        assert_eq!(crdtsync_client_channel_state(a, ca, ptr::null_mut()), -1);
        assert_eq!(
            crdtsync_client_get_blob(a, ca, p.as_ptr(), p.len(), ptr::null_mut()),
            -1
        );
        assert_eq!(
            crdtsync_client_marks_at(a, ca, p.as_ptr(), p.len(), 0, ptr::null_mut()),
            -1
        );
        assert_eq!(
            crdtsync_client_relative_position(a, ca, p.as_ptr(), p.len(), 0, 0, ptr::null_mut()),
            -1
        );
        assert_eq!(
            crdtsync_client_resolve_position(
                a,
                ca,
                p.as_ptr(),
                p.len(),
                [0u8; 4].as_ptr(),
                4,
                ptr::null_mut()
            ),
            -1
        );
        assert_eq!(
            crdtsync_client_xml_tag(a, ca, p.as_ptr(), p.len(), ptr::null_mut()),
            -1
        );
        assert_eq!(
            crdtsync_client_xml_children_len(a, ca, p.as_ptr(), p.len(), ptr::null_mut()),
            -1
        );

        // A null path or position pointer with a nonzero length is rejected
        // rather than dereferenced — 0, not the -1 a bad handle reports.
        // `marks_at` answers 1 for any well-formed path, so its 0 can only come
        // from the pointer rejection.
        assert_eq!(crdtsync_client_get_blob(a, ca, ptr::null(), 4, &mut buf), 0);
        assert_eq!(
            crdtsync_client_marks_at(a, ca, ptr::null(), 4, 0, &mut buf),
            0
        );
        assert_eq!(
            crdtsync_client_relative_position(a, ca, ptr::null(), 4, 0, 0, &mut buf),
            0
        );
        assert_eq!(crdtsync_client_xml_tag(a, ca, ptr::null(), 4, &mut buf), 0);
        assert_eq!(
            crdtsync_client_xml_children_len(a, ca, ptr::null(), 4, &mut n),
            0
        );

        // A null path or position pointer is refused against a path and anchor
        // that do resolve, so each 0 is the pointer rejection, not the payload.
        let body = path(&[b"body"]);
        seed_body_text(a, ca, &body, "hello world");
        let (rc, anchor) = relative_position(a, ca, &body, 6, 0);
        assert_eq!(rc, 1);
        assert_eq!(resolve_position(a, ca, &body, &anchor), (1, 6));
        assert_eq!(
            crdtsync_client_resolve_position(
                a,
                ca,
                ptr::null(),
                4,
                anchor.as_ptr(),
                anchor.len(),
                &mut n
            ),
            0
        );
        assert_eq!(
            crdtsync_client_resolve_position(
                a,
                ca,
                body.as_ptr(),
                body.len(),
                ptr::null(),
                4,
                &mut n
            ),
            0
        );

        // A structurally malformed path — a key length prefix past the end of
        // the buffer — names no slot, so the slot reads report absent. The mark
        // read still answers 1 with no marks, since a path naming no sequence is
        // an empty resolution, not a failure.
        let torn = 0xffff_ffffu32.to_le_bytes().to_vec();
        assert_eq!(
            crdtsync_client_get_blob(a, ca, torn.as_ptr(), torn.len(), &mut buf),
            0
        );
        assert_eq!(
            crdtsync_client_xml_tag(a, ca, torn.as_ptr(), torn.len(), &mut buf),
            0
        );
        assert_eq!(
            crdtsync_client_xml_children_len(a, ca, torn.as_ptr(), torn.len(), &mut n),
            0
        );
        assert_eq!(relative_position(a, ca, &torn, 0, 0), (0, Vec::new()));
        assert_eq!(resolve_position(a, ca, &torn, &anchor), (0, usize::MAX));
        assert_eq!(marks_at(a, ca, &torn, 0), (1, Vec::new()));

        crdtsync_client_free(a);
    }
}
