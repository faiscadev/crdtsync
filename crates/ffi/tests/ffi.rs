//! C ABI — the boundary the server and SDKs drive the core through.
//!
//! Handles and byte buffers cross; the `Rc<RefCell>` graph never does. A slot
//! is addressed by a path — a length-prefixed sequence of keys naming nested
//! maps, the last key the slot itself. A local edit returns the encoded ops to
//! broadcast and applies locally; `apply` folds a peer's op back; two docs that
//! exchange those bytes converge. Every entry point isolates panics rather than
//! unwinding across the boundary.

use crdtsync_ffi::*;
use std::ptr;

fn client(first: u8) -> [u8; 16] {
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

unsafe fn exchange(dst: *mut CrdtDoc, ops: &CrdtBuf) {
    crdtsync_doc_apply(dst, ops.ptr, ops.len);
}

unsafe fn register_int(doc: *mut CrdtDoc, p: &[u8], v: i64) -> CrdtBuf {
    crdtsync_doc_register_int(doc, p.as_ptr(), p.len(), v)
}

unsafe fn inc(doc: *mut CrdtDoc, p: &[u8], amount: u32) -> CrdtBuf {
    crdtsync_doc_inc(doc, p.as_ptr(), p.len(), amount)
}

unsafe fn get_int(doc: *const CrdtDoc, p: &[u8]) -> (i32, i64) {
    let mut out: i64 = 0;
    let rc = crdtsync_doc_get_int(doc, p.as_ptr(), p.len(), &mut out);
    (rc, out)
}

unsafe fn get_counter(doc: *const CrdtDoc, p: &[u8]) -> (i32, i64) {
    let mut out: i64 = 0;
    let rc = crdtsync_doc_get_counter(doc, p.as_ptr(), p.len(), &mut out);
    (rc, out)
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
        let ops = register_int(doc, &path(&[b"age"]), 30);
        assert_eq!(get_int(doc, &path(&[b"age"])), (1, 30));
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
        let p = path(&[b"nope"]);
        assert_eq!(crdtsync_doc_get_int(doc, p.as_ptr(), p.len(), &mut out), 0);
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

        let reg = register_int(a, &path(&[b"age"]), 30);
        let hit = inc(a, &path(&[b"hits"]), 5);
        exchange(b, &reg);
        exchange(b, &hit);

        assert_eq!(get_int(b, &path(&[b"age"])), (1, 30));
        assert_eq!(get_counter(b, &path(&[b"hits"])), (1, 5));

        crdtsync_buf_free(reg);
        crdtsync_buf_free(hit);
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

        let ia = inc(a, &path(&[b"n"]), 3);
        let ib = inc(b, &path(&[b"n"]), 4);
        exchange(b, &ia);
        exchange(a, &ib);

        assert_eq!(get_counter(a, &path(&[b"n"])), (1, 7));
        assert_eq!(get_counter(b, &path(&[b"n"])).1, 7);

        crdtsync_buf_free(ia);
        crdtsync_buf_free(ib);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

// --- nested paths ---

#[test]
fn a_nested_edit_reads_back_and_converges() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());

        // profile.stats.score = 7, two maps deep
        let p = path(&[b"profile", b"stats", b"score"]);
        let ops = register_int(a, &p, 7);
        assert_eq!(get_int(a, &p), (1, 7));

        exchange(b, &ops);
        assert_eq!(get_int(b, &p), (1, 7));

        crdtsync_buf_free(ops);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

#[test]
fn a_path_through_a_missing_map_is_not_found() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        assert_eq!(get_int(doc, &path(&[b"missing", b"x"])).0, 0);
        crdtsync_doc_free(doc);
    }
}

// --- bytes + delete ---

#[test]
fn bytes_round_trip_through_the_boundary() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        let p = path(&[b"blob"]);
        let val = [0u8, 1, 2, 255, 0];
        let ops = crdtsync_doc_set_bytes(doc, p.as_ptr(), p.len(), val.as_ptr(), val.len());

        let mut out = CrdtBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        assert_eq!(
            crdtsync_doc_get_bytes(doc, p.as_ptr(), p.len(), &mut out),
            1
        );
        let got = std::slice::from_raw_parts(out.ptr, out.len);
        assert_eq!(got, &val);

        crdtsync_buf_free(out);
        crdtsync_buf_free(ops);
        crdtsync_doc_free(doc);
    }
}

#[test]
fn delete_removes_a_slot_and_converges() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());
        let p = path(&[b"k"]);

        let set = register_int(a, &p, 5);
        let del = crdtsync_doc_delete(a, p.as_ptr(), p.len());
        assert_eq!(get_int(a, &p).0, 0, "deleted locally");

        exchange(b, &set);
        exchange(b, &del);
        assert_eq!(get_int(b, &p).0, 0, "delete converges");

        crdtsync_buf_free(set);
        crdtsync_buf_free(del);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

// --- list + text ---

unsafe fn list_get(doc: *const CrdtDoc, p: &[u8], index: usize) -> Vec<u8> {
    let mut out = CrdtBuf {
        ptr: ptr::null_mut(),
        len: 0,
    };
    let rc = crdtsync_doc_list_get(doc, p.as_ptr(), p.len(), index, &mut out);
    assert_eq!(rc, 1, "list_get missed");
    let v = std::slice::from_raw_parts(out.ptr, out.len).to_vec();
    crdtsync_buf_free(out);
    v
}

unsafe fn text_get(doc: *const CrdtDoc, p: &[u8]) -> String {
    let mut out = CrdtBuf {
        ptr: ptr::null_mut(),
        len: 0,
    };
    let rc = crdtsync_doc_text_get(doc, p.as_ptr(), p.len(), &mut out);
    assert_eq!(rc, 1, "text_get missed");
    let s = String::from_utf8(std::slice::from_raw_parts(out.ptr, out.len).to_vec()).unwrap();
    crdtsync_buf_free(out);
    s
}

#[test]
fn a_list_edits_read_back_and_converge() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());
        let p = path(&[b"board", b"cards"]); // list under a nested map

        let o0 = crdtsync_doc_list_insert(a, p.as_ptr(), p.len(), 0, b"x".as_ptr(), 1);
        let o1 = crdtsync_doc_list_insert(a, p.as_ptr(), p.len(), 1, b"y".as_ptr(), 1);
        exchange(b, &o0);
        exchange(b, &o1);

        let mut len: usize = 0;
        assert_eq!(crdtsync_doc_list_len(b, p.as_ptr(), p.len(), &mut len), 1);
        assert_eq!(len, 2);
        assert_eq!(list_get(b, &p, 0), b"x");
        assert_eq!(list_get(b, &p, 1), b"y");

        crdtsync_buf_free(o0);
        crdtsync_buf_free(o1);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

#[test]
fn a_list_delete_converges() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());
        let p = path(&[b"items"]);

        let o0 = crdtsync_doc_list_insert(a, p.as_ptr(), p.len(), 0, b"a".as_ptr(), 1);
        let o1 = crdtsync_doc_list_insert(a, p.as_ptr(), p.len(), 1, b"b".as_ptr(), 1);
        let od = crdtsync_doc_list_delete(a, p.as_ptr(), p.len(), 0);
        exchange(b, &o0);
        exchange(b, &o1);
        exchange(b, &od);

        let mut len: usize = 0;
        crdtsync_doc_list_len(b, p.as_ptr(), p.len(), &mut len);
        assert_eq!(len, 1);
        assert_eq!(list_get(b, &p, 0), b"b");

        for buf in [o0, o1, od] {
            crdtsync_buf_free(buf);
        }
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

#[test]
fn a_text_edits_read_back_and_converge() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());
        let p = path(&[b"doc", b"title"]);

        let s = "héllo";
        let o0 = crdtsync_doc_text_insert(a, p.as_ptr(), p.len(), 0, s.as_ptr(), s.len());
        exchange(b, &o0);

        let mut len: usize = 0;
        assert_eq!(crdtsync_doc_text_len(b, p.as_ptr(), p.len(), &mut len), 1);
        assert_eq!(len, 5, "codepoint count");
        assert_eq!(text_get(b, &p), "héllo");

        crdtsync_buf_free(o0);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

#[test]
fn a_text_delete_converges() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());
        let p = path(&[b"t"]);

        let s = "hello";
        let ins = crdtsync_doc_text_insert(a, p.as_ptr(), p.len(), 0, s.as_ptr(), s.len());
        let del = crdtsync_doc_text_delete(a, p.as_ptr(), p.len(), 1, 3); // drop "ell"
        exchange(b, &ins);
        exchange(b, &del);

        assert_eq!(text_get(b, &p), "ho");

        crdtsync_buf_free(ins);
        crdtsync_buf_free(del);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

#[test]
fn non_utf8_text_insert_yields_no_ops() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        let p = path(&[b"t"]);
        let bad = [0xFFu8, 0xFE];
        let buf = crdtsync_doc_text_insert(doc, p.as_ptr(), p.len(), 0, bad.as_ptr(), bad.len());
        assert_eq!(buf.len, 0, "invalid UTF-8 must not emit");
        // ...and must not materialise the text either.
        let mut len: usize = 0;
        assert_eq!(
            crdtsync_doc_text_len(doc, p.as_ptr(), p.len(), &mut len),
            0,
            "no text may exist"
        );
        crdtsync_buf_free(buf);
        crdtsync_doc_free(doc);
    }
}

#[test]
fn a_no_op_delete_does_not_create_a_container() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        let lp = path(&[b"list"]);
        let tp = path(&[b"text"]);

        // Deleting from absent / out-of-range sequences must emit nothing and
        // leave no empty container behind.
        let a = crdtsync_doc_list_delete(doc, lp.as_ptr(), lp.len(), 0);
        let b = crdtsync_doc_text_delete(doc, tp.as_ptr(), tp.len(), 0, 3);
        assert_eq!(a.len, 0, "absent list delete emits nothing");
        assert_eq!(b.len, 0, "absent text delete emits nothing");

        let mut len: usize = 0;
        assert_eq!(
            crdtsync_doc_list_len(doc, lp.as_ptr(), lp.len(), &mut len),
            0
        );
        assert_eq!(
            crdtsync_doc_text_len(doc, tp.as_ptr(), tp.len(), &mut len),
            0
        );

        // A zero-count delete on an existing text is also a no-op.
        let s = "hi";
        let ins = crdtsync_doc_text_insert(doc, tp.as_ptr(), tp.len(), 0, s.as_ptr(), s.len());
        let z = crdtsync_doc_text_delete(doc, tp.as_ptr(), tp.len(), 0, 0);
        assert_eq!(z.len, 0, "zero-count delete emits nothing");
        assert_eq!(text_get(doc, &tp), "hi");

        for buf in [a, b, ins, z] {
            crdtsync_buf_free(buf);
        }
        crdtsync_doc_free(doc);
    }
}

#[test]
#[cfg_attr(miri, ignore = "stack depth is a native concern; slow under Miri")]
fn a_very_deep_path_does_not_overflow_the_stack() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        // Path depth is caller-supplied; a deep walk must stay iterative.
        let mut keys: Vec<&[u8]> = vec![b"k"; 10_000];
        keys.push(b"leaf");
        let p = path(&keys);
        let ops = register_int(doc, &p, 42);
        assert_eq!(get_int(doc, &p), (1, 42));
        crdtsync_buf_free(ops);
        crdtsync_doc_free(doc);
    }
}

// --- robustness ---

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
fn a_malformed_path_is_rejected_not_fatal() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        // A key length that runs past the buffer end must not be read.
        let bad = 0xFFFF_FFFEu32.to_le_bytes();
        let mut out: i64 = 0;
        assert_eq!(crdtsync_doc_get_int(doc, bad.as_ptr(), 4, &mut out), 0);
        let buf = crdtsync_doc_register_int(doc, bad.as_ptr(), 4, 1);
        assert_eq!(buf.len, 0, "a malformed path yields no ops");
        crdtsync_buf_free(buf);
        crdtsync_doc_free(doc);
    }
}

#[test]
fn a_null_document_is_handled_not_dereferenced() {
    unsafe {
        let mut out: i64 = 0;
        let p = path(&[b"k"]);
        assert_eq!(
            crdtsync_doc_get_int(ptr::null(), p.as_ptr(), p.len(), &mut out),
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
        assert_eq!(crdtsync_doc_get_int(doc, ptr::null(), 4, &mut out), 0);
        assert_eq!(crdtsync_doc_apply(doc, ptr::null(), 8), -1);
        let buf = crdtsync_doc_register_int(doc, ptr::null(), 4, 1);
        assert_eq!(buf.len, 0, "a null path yields no ops");
        crdtsync_buf_free(buf);
        crdtsync_doc_free(doc);
    }
}
