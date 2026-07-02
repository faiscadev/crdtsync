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

unsafe fn dec(doc: *mut CrdtDoc, p: &[u8], amount: u32) -> CrdtBuf {
    crdtsync_doc_dec(doc, p.as_ptr(), p.len(), amount)
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

#[test]
fn a_counter_decrements_across_replicas() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());

        let up = inc(a, &path(&[b"stock"]), 10);
        let down = dec(a, &path(&[b"stock"]), 4);
        exchange(b, &up);
        exchange(b, &down);

        assert_eq!(get_counter(a, &path(&[b"stock"])), (1, 6));
        assert_eq!(get_counter(b, &path(&[b"stock"])).1, 6);

        crdtsync_buf_free(up);
        crdtsync_buf_free(down);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

// --- state snapshot ---

#[test]
fn a_snapshot_round_trips_through_a_decode() {
    unsafe {
        let c = client(1);
        let a = crdtsync_doc_new(c.as_ptr());
        let reg = register_int(a, &path(&[b"age"]), 30);
        let hit = inc(a, &path(&[b"hits"]), 5);

        let snap = crdtsync_doc_encode_state(a);
        assert!(!snap.ptr.is_null() && snap.len > 0);

        // A fresh handle decoded from the snapshot reads the same state.
        let b = crdtsync_doc_decode_state(snap.ptr, snap.len);
        assert!(!b.is_null());
        assert_eq!(get_int(b, &path(&[b"age"])), (1, 30));
        assert_eq!(get_counter(b, &path(&[b"hits"])), (1, 5));

        crdtsync_buf_free(reg);
        crdtsync_buf_free(hit);
        crdtsync_buf_free(snap);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

#[test]
fn diff_of_two_snapshots_reports_the_change() {
    use crdtsync_core::diff::{decode_changes, Change};
    unsafe {
        let c = client(1);
        let a = crdtsync_doc_new(c.as_ptr());
        let reg = register_int(a, &path(&[b"age"]), 30);
        let old = crdtsync_doc_encode_state(a);

        let reg2 = register_int(a, &path(&[b"age"]), 31);
        let new = crdtsync_doc_encode_state(a);

        let buf = crdtsync_diff(old.ptr, old.len, new.ptr, new.len);
        assert!(!buf.ptr.is_null() && buf.len > 0);
        let bytes = std::slice::from_raw_parts(buf.ptr, buf.len);
        let changes = decode_changes(bytes).expect("the encoded change list decodes");
        assert!(matches!(changes.as_slice(), [Change::Value { .. }],));

        crdtsync_buf_free(reg);
        crdtsync_buf_free(reg2);
        crdtsync_buf_free(old);
        crdtsync_buf_free(new);
        crdtsync_buf_free(buf);
        crdtsync_doc_free(a);
    }
}

#[test]
fn diff_of_malformed_input_is_empty() {
    unsafe {
        let junk = [1u8, 2, 3];
        let buf = crdtsync_diff(junk.as_ptr(), junk.len(), junk.as_ptr(), junk.len());
        assert_eq!(buf.len, 0, "a bad snapshot yields an empty change list");
        // A null pair is empty too, not a crash.
        let buf2 = crdtsync_diff(ptr::null(), 0, ptr::null(), 0);
        assert_eq!(buf2.len, 0);
        crdtsync_buf_free(buf);
        crdtsync_buf_free(buf2);
    }
}

#[test]
fn a_decoded_snapshot_still_dedups_and_converges() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let reg = register_int(a, &path(&[b"age"]), 30);

        // Reload `a` from a snapshot, then a peer's later edit still lands and a
        // replay of the covered op is a no-op.
        let snap = crdtsync_doc_encode_state(a);
        let reloaded = crdtsync_doc_decode_state(snap.ptr, snap.len);

        assert_eq!(
            crdtsync_doc_apply(reloaded, reg.ptr, reg.len),
            0,
            "replay is deduped"
        );

        let b = crdtsync_doc_new(cb.as_ptr());
        exchange(b, &reg);
        let hit = inc(b, &path(&[b"hits"]), 4);
        assert_eq!(
            crdtsync_doc_apply(reloaded, hit.ptr, hit.len),
            1,
            "later op applies"
        );
        assert_eq!(get_counter(reloaded, &path(&[b"hits"])), (1, 4));

        crdtsync_buf_free(reg);
        crdtsync_buf_free(hit);
        crdtsync_buf_free(snap);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
        crdtsync_doc_free(reloaded);
    }
}

#[test]
fn decoding_garbage_state_is_null_not_a_crash() {
    unsafe {
        // A malformed snapshot must be rejected as a null handle, never a panic
        // across the boundary.
        let garbage = [0xFFu8; 8];
        assert!(crdtsync_doc_decode_state(garbage.as_ptr(), garbage.len()).is_null());
        // A null/empty input is likewise a null handle, not UB.
        assert!(crdtsync_doc_decode_state(ptr::null(), 0).is_null());
    }
}

#[test]
fn encoding_a_null_handle_is_an_empty_buffer() {
    unsafe {
        let snap = crdtsync_doc_encode_state(ptr::null());
        assert_eq!(snap.len, 0);
        crdtsync_buf_free(snap);
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

// --- undo / redo ---

#[test]
fn undo_and_redo_a_register_across_the_boundary() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        let undo = crdtsync_undo_new();
        let p = path(&[b"title"]);

        let o1 = crdtsync_undo_register_int(undo, doc, p.as_ptr(), p.len(), 1);
        let o2 = crdtsync_undo_register_int(undo, doc, p.as_ptr(), p.len(), 2);
        assert_eq!(get_int(doc, &p), (1, 2));
        assert_eq!(crdtsync_undo_can_undo(undo), 1);

        let u1 = crdtsync_undo_undo(undo, doc);
        assert_eq!(get_int(doc, &p), (1, 1), "undo steps back one value");
        let r1 = crdtsync_undo_redo(undo, doc);
        assert_eq!(get_int(doc, &p), (1, 2), "redo restores it");
        assert_eq!(crdtsync_undo_can_redo(undo), 0);

        crdtsync_buf_free(o1);
        crdtsync_buf_free(o2);
        crdtsync_buf_free(u1);
        crdtsync_buf_free(r1);
        crdtsync_undo_free(undo);
        crdtsync_doc_free(doc);
    }
}

#[test]
fn an_undo_converges_on_a_peer() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());
        let undo = crdtsync_undo_new();
        let p = path(&[b"votes"]);

        let up = crdtsync_undo_inc(undo, a, p.as_ptr(), p.len(), 5);
        exchange(b, &up);
        assert_eq!(get_counter(b, &p).1, 5);

        // The undo's ops travel like any edit and the peer converges.
        let un = crdtsync_undo_undo(undo, a);
        exchange(b, &un);
        assert_eq!(get_counter(a, &p).1, 0);
        assert_eq!(get_counter(b, &p).1, 0, "the peer sees the undo");

        crdtsync_buf_free(up);
        crdtsync_buf_free(un);
        crdtsync_undo_free(undo);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

#[test]
fn undo_removes_a_list_insert() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        let undo = crdtsync_undo_new();
        let p = path(&[b"items"]);
        let v = b"a";

        let ins = crdtsync_undo_list_insert(undo, doc, p.as_ptr(), p.len(), 0, v.as_ptr(), v.len());
        let mut len: usize = 0;
        assert_eq!(crdtsync_doc_list_len(doc, p.as_ptr(), p.len(), &mut len), 1);
        assert_eq!(len, 1);

        let un = crdtsync_undo_undo(undo, doc);
        assert_eq!(crdtsync_doc_list_len(doc, p.as_ptr(), p.len(), &mut len), 1);
        assert_eq!(len, 0, "the inserted item is removed");

        crdtsync_buf_free(ins);
        crdtsync_buf_free(un);
        crdtsync_undo_free(undo);
        crdtsync_doc_free(doc);
    }
}

#[test]
fn a_null_undo_handle_is_inert() {
    unsafe {
        assert_eq!(crdtsync_undo_can_undo(ptr::null()), -1);
        assert_eq!(crdtsync_undo_can_redo(ptr::null()), -1);
        let doc = crdtsync_doc_new(client(1).as_ptr());
        let buf = crdtsync_undo_undo(ptr::null_mut(), doc);
        crdtsync_buf_free(buf);
        crdtsync_doc_free(doc);
        crdtsync_undo_free(ptr::null_mut());
    }
}

// --- atomic transactions ---

#[test]
fn a_doc_atomic_transaction_commits_all_or_nothing() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());
        let x = path(&[b"x"]);
        let y = path(&[b"y"]);

        crdtsync_doc_begin_atomic(a);
        // Edits accumulate while recording; each returns an empty buffer.
        let e1 = register_int(a, &x, 1);
        let e2 = register_int(a, &y, 2);
        assert_eq!(e1.len, 0);
        assert_eq!(e2.len, 0);
        let group = crdtsync_doc_commit_atomic(a);
        assert!(group.len > 0);

        // Split the group so the peer sees a partial delivery first.
        let ops = crdtsync_core::decode_ops(std::slice::from_raw_parts(group.ptr, group.len))
            .expect("decode");
        assert_eq!(ops.len(), 2);
        let first = crdtsync_core::encode_ops(&ops[..1]);
        let rest = crdtsync_core::encode_ops(&ops[1..]);

        crdtsync_doc_apply(b, first.as_ptr(), first.len());
        assert_eq!(get_int(b, &x).0, 0, "partial tx is invisible");
        crdtsync_doc_apply(b, rest.as_ptr(), rest.len());
        assert_eq!(get_int(b, &x), (1, 1));
        assert_eq!(get_int(b, &y), (1, 2));

        crdtsync_buf_free(e1);
        crdtsync_buf_free(e2);
        crdtsync_buf_free(group);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}
