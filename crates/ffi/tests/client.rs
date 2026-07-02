//! C ABI — the wire client session.
//!
//! A client holds a replica per subscribed room and turns local edits into wire
//! frames to send; folding a peer's frame back in converges the replicas. Frames
//! cross the boundary as encoded byte buffers, a room addressed by the `u32`
//! channel the client assigned at subscribe. Every buffer and handle is freed so
//! the round trip is leak-clean under Miri.

use crdtsync_ffi::*;
use std::ptr;

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
