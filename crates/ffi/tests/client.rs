//! C ABI — the wire client session.
//!
//! A client holds a replica per subscribed room and turns local edits into wire
//! frames to send; folding a peer's frame back in converges the replicas. Frames
//! cross the boundary as encoded byte buffers, a room addressed by the `u32`
//! channel the client assigned at subscribe. Every buffer and handle is freed so
//! the round trip is leak-clean under Miri.

use crdtsync_core::{decode_message, encode_message, Channel, Message};
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

unsafe fn register_int(c: *mut CrdtClient, channel: u32, p: &[u8], v: i64) -> CrdtBuf {
    crdtsync_client_register_int(c, channel, p.as_ptr(), p.len(), v)
}

unsafe fn get_int(c: *const CrdtClient, channel: u32, p: &[u8]) -> (i32, i64) {
    let mut out: i64 = 0;
    let rc = crdtsync_client_get_int(c, channel, p.as_ptr(), p.len(), &mut out);
    (rc, out)
}

unsafe fn receive(c: *mut CrdtClient, frame: &CrdtBuf) -> i32 {
    crdtsync_client_receive(c, frame.ptr, frame.len)
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
            crdtsync_client_receive(ptr::null_mut(), p.as_ptr(), p.len()),
            -1
        );
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
        assert_eq!(crdtsync_client_receive(c, advert.as_ptr(), advert.len()), 1);
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
        assert_eq!(crdtsync_client_receive(c, advert.as_ptr(), advert.len()), 1);
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
        assert_eq!(crdtsync_client_receive(c, advert.as_ptr(), advert.len()), 1);
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
        assert_eq!(crdtsync_client_receive(c, frame.as_ptr(), frame.len()), 1);
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
        assert_eq!(crdtsync_client_receive(c, frame.as_ptr(), frame.len()), 1);

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
            crdtsync_client_receive(c, listing.as_ptr(), listing.len()),
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
        assert_eq!(crdtsync_client_receive(c, state.as_ptr(), state.len()), 1);
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
            crdtsync_client_receive(c, accepted.as_ptr(), accepted.len()),
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
            crdtsync_client_receive(a, accepted.as_ptr(), accepted.len()),
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
