//! C ABI — the boundary the server and SDKs drive the core through.
//!
//! Handles and byte buffers cross; the `Rc<RefCell>` graph never does. A local
//! edit returns the encoded ops to broadcast and applies locally; `apply` folds
//! a peer's op back; two docs that exchange those bytes converge. Every entry
//! point isolates panics rather than unwinding across the boundary.

use crdtsync_ffi::*;
use std::ptr;

fn client(first: u8) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0] = first;
    b
}

/// Apply every op in an encoded buffer from `src` into `dst`.
unsafe fn exchange(dst: *mut CrdtDoc, ops: &CrdtBuf) {
    crdtsync_doc_apply(dst, ops.ptr, ops.len);
}

#[test]
fn new_and_free_a_document() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        assert!(!doc.is_null());
        crdtsync_doc_free(doc);
    }
}

#[test]
fn a_register_reads_back_locally() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        let ops = crdtsync_doc_register_int(doc, b"age".as_ptr(), 3, 30);
        let mut out: i64 = 0;
        assert_eq!(crdtsync_doc_get_int(doc, b"age".as_ptr(), 3, &mut out), 1);
        assert_eq!(out, 30);
        crdtsync_buf_free(ops);
        crdtsync_doc_free(doc);
    }
}

#[test]
fn a_missing_key_reports_not_found() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        let mut out: i64 = 7;
        assert_eq!(crdtsync_doc_get_int(doc, b"nope".as_ptr(), 4, &mut out), 0);
        assert_eq!(out, 7, "out must be left untouched when not found");
        crdtsync_doc_free(doc);
    }
}

#[test]
fn edits_broadcast_and_converge_on_a_peer() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());

        let reg = crdtsync_doc_register_int(a, b"age".as_ptr(), 3, 30);
        let inc = crdtsync_doc_inc(a, b"hits".as_ptr(), 4, 5);
        exchange(b, &reg);
        exchange(b, &inc);

        let mut age: i64 = 0;
        let mut hits: i64 = 0;
        assert_eq!(crdtsync_doc_get_int(b, b"age".as_ptr(), 3, &mut age), 1);
        assert_eq!(
            crdtsync_doc_get_counter(b, b"hits".as_ptr(), 4, &mut hits),
            1
        );
        assert_eq!(age, 30);
        assert_eq!(hits, 5);

        crdtsync_buf_free(reg);
        crdtsync_buf_free(inc);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

#[test]
fn a_counter_accumulates_across_replicas() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());

        let ia = crdtsync_doc_inc(a, b"n".as_ptr(), 1, 3);
        let ib = crdtsync_doc_inc(b, b"n".as_ptr(), 1, 4);
        exchange(b, &ia);
        exchange(a, &ib);

        let mut na: i64 = 0;
        let mut nb: i64 = 0;
        crdtsync_doc_get_counter(a, b"n".as_ptr(), 1, &mut na);
        crdtsync_doc_get_counter(b, b"n".as_ptr(), 1, &mut nb);
        assert_eq!(na, 7);
        assert_eq!(na, nb);

        crdtsync_buf_free(ia);
        crdtsync_buf_free(ib);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

#[test]
fn applying_garbage_bytes_is_reported_not_fatal() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        let junk = [0xFFu8; 8];
        assert_eq!(crdtsync_doc_apply(doc, junk.as_ptr(), junk.len()), -1);
        crdtsync_doc_free(doc);
    }
}

#[test]
fn a_null_document_is_handled_not_dereferenced() {
    unsafe {
        let mut out: i64 = 0;
        // A null handle must be reported, never dereferenced.
        assert_eq!(
            crdtsync_doc_get_int(ptr::null_mut(), b"k".as_ptr(), 1, &mut out),
            -1
        );
        assert_eq!(crdtsync_doc_apply(ptr::null_mut(), b"".as_ptr(), 0), -1);
    }
}

#[test]
fn a_null_data_pointer_is_rejected_not_dereferenced() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        let mut out: i64 = 0;
        // Null key/bytes with a nonzero length must be reported, not read.
        assert_eq!(crdtsync_doc_get_int(doc, ptr::null(), 4, &mut out), -1);
        assert_eq!(crdtsync_doc_apply(doc, ptr::null(), 8), -1);
        let buf = crdtsync_doc_register_int(doc, ptr::null(), 4, 1);
        assert_eq!(buf.len, 0, "a null key yields no ops");
        crdtsync_buf_free(buf);
        crdtsync_doc_free(doc);
    }
}
