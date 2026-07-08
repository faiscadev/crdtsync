//! C ABI — the boundary the server and SDKs drive the core through.
//!
//! Handles and byte buffers cross; the `Rc<RefCell>` graph never does. A slot
//! is addressed by a path — a length-prefixed sequence of keys naming nested
//! maps, the last key the slot itself. A local edit returns the encoded ops to
//! broadcast and applies locally; `apply` folds a peer's op back; two docs that
//! exchange those bytes converge. Every entry point isolates panics rather than
//! unwinding across the boundary.

use crdtsync_core::Scalar;
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

// --- xml navigation ---

unsafe fn xml_tag(doc: *const CrdtDoc, p: &[u8]) -> (i32, Vec<u8>) {
    let mut out = CrdtBuf {
        ptr: ptr::null_mut(),
        len: 0,
    };
    let rc = crdtsync_doc_xml_tag(doc, p.as_ptr(), p.len(), &mut out);
    let tag = if rc == 1 {
        std::slice::from_raw_parts(out.ptr, out.len).to_vec()
    } else {
        Vec::new()
    };
    crdtsync_buf_free(out);
    (rc, tag)
}

unsafe fn children_len(doc: *const CrdtDoc, p: &[u8]) -> (i32, usize) {
    let mut out: usize = 0;
    let rc = crdtsync_doc_xml_children_len(doc, p.as_ptr(), p.len(), &mut out);
    (rc, out)
}

#[test]
fn an_xml_element_reads_its_tag_back_and_converges() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());
        let p = path(&[b"doc", b"body"]);

        let o = crdtsync_doc_xml_element(a, p.as_ptr(), p.len(), b"section".as_ptr(), 7);
        assert!(o.len > 0, "the element install emits ops");
        assert_eq!(xml_tag(a, &p), (1, b"section".to_vec()));
        exchange(b, &o);
        assert_eq!(xml_tag(b, &p), (1, b"section".to_vec()), "tag converges");

        crdtsync_buf_free(o);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

#[test]
fn an_xml_fragment_has_no_tag() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());
        let p = path(&[b"root"]);

        let o = crdtsync_doc_xml_fragment(doc, p.as_ptr(), p.len());
        assert!(o.len > 0, "the fragment install emits ops");
        // A fragment is tagless, so the tag read reports absent (0), not a panic.
        assert_eq!(xml_tag(doc, &p).0, 0, "a fragment has no tag");
        // But it is a live xml node: it owns a children sequence.
        assert_eq!(children_len(doc, &p), (1, 0));

        crdtsync_buf_free(o);
        crdtsync_doc_free(doc);
    }
}

#[test]
fn xml_children_insert_delete_and_len_converge() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());
        let p = path(&[b"doc", b"body"]);

        let root = crdtsync_doc_xml_element(a, p.as_ptr(), p.len(), b"body".as_ptr(), 4);
        let e = crdtsync_doc_xml_insert_element(a, p.as_ptr(), p.len(), 0, b"p".as_ptr(), 1);
        let t = crdtsync_doc_xml_insert_text(a, p.as_ptr(), p.len(), 1, b"hi".as_ptr(), 2);
        assert!(e.len > 0 && t.len > 0, "child inserts emit ops");
        assert_eq!(children_len(a, &p), (1, 2));

        for o in [&root, &e, &t] {
            exchange(b, o);
        }
        assert_eq!(children_len(b, &p), (1, 2), "children converge");

        // Delete the text child; the live count drops on both replicas.
        let d = crdtsync_doc_xml_child_delete(a, p.as_ptr(), p.len(), 1);
        assert!(d.len > 0, "the delete emits ops");
        assert_eq!(children_len(a, &p), (1, 1));
        exchange(b, &d);
        assert_eq!(children_len(b, &p), (1, 1));

        for o in [root, e, t, d] {
            crdtsync_buf_free(o);
        }
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

#[test]
fn an_xml_move_relocates_a_child_and_converges() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());
        let src = path(&[b"a"]);
        let dst = path(&[b"b"]);

        let fa = crdtsync_doc_xml_fragment(a, src.as_ptr(), src.len());
        let fb = crdtsync_doc_xml_fragment(a, dst.as_ptr(), dst.len());
        let kid = crdtsync_doc_xml_insert_element(a, src.as_ptr(), src.len(), 0, b"li".as_ptr(), 2);
        assert_eq!(children_len(a, &src), (1, 1));
        assert_eq!(children_len(a, &dst), (1, 0));

        // Move the child from `src[0]` to `dst[0]` — a Kleppmann tree move.
        let mv = crdtsync_doc_xml_move(a, src.as_ptr(), src.len(), 0, dst.as_ptr(), dst.len(), 0);
        assert!(mv.len > 0, "the move emits ops");
        assert_eq!(children_len(a, &src), (1, 0), "child left the source");
        assert_eq!(children_len(a, &dst), (1, 1), "child arrived at the dest");

        for o in [&fa, &fb, &kid, &mv] {
            exchange(b, o);
        }
        assert_eq!(children_len(b, &src), (1, 0), "move converges");
        assert_eq!(children_len(b, &dst), (1, 1));

        for o in [fa, fb, kid, mv] {
            crdtsync_buf_free(o);
        }
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

#[test]
fn xml_edits_on_a_bad_handle_or_path_are_inert() {
    unsafe {
        let c = client(1);
        let doc = crdtsync_doc_new(c.as_ptr());

        // A null handle never emits and never dereferences.
        let o = crdtsync_doc_xml_element(ptr::null_mut(), ptr::null(), 0, b"x".as_ptr(), 1);
        assert_eq!(o.len, 0, "null handle emits nothing");
        crdtsync_buf_free(o);
        // A valid out buffer isolates the null-handle guard: it must report -1 for
        // the handle, not merely because the out pointer is null.
        let mut out = CrdtBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        assert_eq!(
            crdtsync_doc_xml_tag(ptr::null(), ptr::null(), 0, &mut out),
            -1,
            "null handle reads -1"
        );

        // A register slot is not an xml node: reads report absent, child edits and
        // moves are inert and must not re-stamp it into an element.
        let reg = path(&[b"age"]);
        let r = register_int(doc, &reg, 30);
        crdtsync_buf_free(r);
        assert_eq!(xml_tag(doc, &reg).0, 0, "a register has no tag");
        assert_eq!(children_len(doc, &reg).0, 0, "a register has no children");
        let ins =
            crdtsync_doc_xml_insert_element(doc, reg.as_ptr(), reg.len(), 0, b"p".as_ptr(), 1);
        assert_eq!(ins.len, 0, "insert into a non-node emits nothing");
        crdtsync_buf_free(ins);
        let del = crdtsync_doc_xml_child_delete(doc, reg.as_ptr(), reg.len(), 0);
        assert_eq!(del.len, 0, "delete on a non-node emits nothing");
        crdtsync_buf_free(del);
        // Reading it as an int still succeeds: the slot was never clobbered.
        assert_eq!(get_int(doc, &reg), (1, 30));

        // A move naming a non-node parent is inert.
        let mv = crdtsync_doc_xml_move(doc, reg.as_ptr(), reg.len(), 0, reg.as_ptr(), reg.len(), 0);
        assert_eq!(mv.len, 0, "move on a non-node emits nothing");
        crdtsync_buf_free(mv);

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

// --- relative positions (anchors) ---

unsafe fn capture(doc: *const CrdtDoc, p: &[u8], index: usize, side: u32) -> Vec<u8> {
    let buf = crdtsync_doc_relative_position(doc, p.as_ptr(), p.len(), index, side);
    assert!(buf.len > 0, "capture missed");
    let v = std::slice::from_raw_parts(buf.ptr, buf.len).to_vec();
    crdtsync_buf_free(buf);
    v
}

unsafe fn resolve(doc: *const CrdtDoc, p: &[u8], pos: &[u8]) -> (i32, usize) {
    let mut out: usize = 0;
    let rc =
        crdtsync_doc_resolve_position(doc, p.as_ptr(), p.len(), pos.as_ptr(), pos.len(), &mut out);
    (rc, out)
}

#[test]
fn a_relative_position_tracks_edits_across_the_boundary() {
    unsafe {
        let ca = client(1);
        let a = crdtsync_doc_new(ca.as_ptr());
        let p = path(&[b"board", b"cards"]);

        for (i, v) in [b"a", b"b", b"c"].iter().enumerate() {
            let o = crdtsync_doc_list_insert(a, p.as_ptr(), p.len(), i, v.as_ptr(), 1);
            crdtsync_buf_free(o);
        }
        // Anchor left of index 2 ("c"), then insert ahead of it.
        let pos = capture(a, &p, 2, 0);
        assert_eq!(resolve(a, &p, &pos), (1, 2));
        let o = crdtsync_doc_list_insert(a, p.as_ptr(), p.len(), 0, b"z".as_ptr(), 1);
        crdtsync_buf_free(o);
        assert_eq!(resolve(a, &p, &pos), (1, 3), "anchor slid with the insert");

        crdtsync_doc_free(a);
    }
}

#[test]
fn a_text_relative_position_round_trips() {
    unsafe {
        let ca = client(1);
        let a = crdtsync_doc_new(ca.as_ptr());
        let t = path(&[b"doc", b"title"]);

        let o = crdtsync_doc_text_insert(a, t.as_ptr(), t.len(), 0, b"hello".as_ptr(), 5);
        crdtsync_buf_free(o);
        let pos = capture(a, &t, 5, 0);
        assert_eq!(resolve(a, &t, &pos), (1, 5));
        let o = crdtsync_doc_text_insert(a, t.as_ptr(), t.len(), 0, b">>".as_ptr(), 2);
        crdtsync_buf_free(o);
        assert_eq!(resolve(a, &t, &pos), (1, 7), "anchor slid with the insert");

        crdtsync_doc_free(a);
    }
}

#[test]
fn a_position_on_a_bad_or_non_sequence_path_is_reported() {
    unsafe {
        let ca = client(1);
        let a = crdtsync_doc_new(ca.as_ptr());
        let age = path(&[b"age"]);
        let r = register_int(a, &age, 30);
        crdtsync_buf_free(r);

        // Capture on a register slot yields an empty buffer.
        let buf = crdtsync_doc_relative_position(a, age.as_ptr(), age.len(), 0, 0);
        assert_eq!(buf.len, 0, "no anchor on a non-sequence");
        crdtsync_buf_free(buf);
        // An unknown side is rejected too.
        let buf = crdtsync_doc_relative_position(a, age.as_ptr(), age.len(), 0, 9);
        assert_eq!(buf.len, 0, "unknown side rejected");
        crdtsync_buf_free(buf);

        // Resolving on a non-sequence path returns 0; malformed bytes return 0.
        let good = path(&[b"list"]);
        let o = crdtsync_doc_list_insert(a, good.as_ptr(), good.len(), 0, b"x".as_ptr(), 1);
        crdtsync_buf_free(o);
        let pos = capture(a, &good, 0, 0);
        assert_eq!(resolve(a, &age, &pos).0, 0, "non-sequence path");
        assert_eq!(resolve(a, &good, &[0xff, 0xff]).0, 0, "malformed position");

        // A null handle is -1.
        assert_eq!(
            crdtsync_doc_resolve_position(
                ptr::null(),
                good.as_ptr(),
                good.len(),
                pos.as_ptr(),
                pos.len(),
                &mut 0usize
            ),
            -1
        );

        crdtsync_doc_free(a);
    }
}

// --- marks ---

/// The name and (for an object-flavored mark) covering ids of each resolved mark
/// in a `crdtsync_doc_marks_at` buffer — the encoding the SDK marks reader decodes.
fn parse_marks(buf: &[u8]) -> Vec<(Vec<u8>, Vec<[u8; 16]>)> {
    let u32_at = |b: &[u8], i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as usize;
    let mut c = 0usize;
    let count = u32_at(buf, c);
    c += 4;
    let mut marks = Vec::new();
    for _ in 0..count {
        let nl = u32_at(buf, c);
        c += 4;
        let name = buf[c..c + nl].to_vec();
        c += nl;
        let tag = buf[c];
        c += 1;
        let mut ids = Vec::new();
        match tag {
            0 => c += 1,
            1 => {
                let vl = u32_at(buf, c);
                c += 4 + vl;
            }
            2 => {
                let n = u32_at(buf, c);
                c += 4;
                for _ in 0..n {
                    ids.push(buf[c..c + 16].try_into().unwrap());
                    c += 16;
                }
            }
            _ => panic!("unknown mark flavor tag {tag}"),
        }
        marks.push((name, ids));
    }
    marks
}

unsafe fn marks_at(doc: *const CrdtDoc, p: &[u8], index: usize) -> Vec<(Vec<u8>, Vec<[u8; 16]>)> {
    let mut out = CrdtBuf {
        ptr: ptr::null_mut(),
        len: 0,
    };
    let rc = crdtsync_doc_marks_at(doc, p.as_ptr(), p.len(), index, &mut out);
    assert_eq!(rc, 1, "a live handle reads its marks");
    let buf = std::slice::from_raw_parts(out.ptr, out.len).to_vec();
    crdtsync_buf_free(out);
    parse_marks(&buf)
}

#[test]
fn a_mark_reads_back_over_its_span_and_converges() {
    unsafe {
        let (ca, cb) = (client(1), client(2));
        let a = crdtsync_doc_new(ca.as_ptr());
        let b = crdtsync_doc_new(cb.as_ptr());
        let body = path(&[b"body"]);

        // A "body" text to annotate — the mark spans [0,5) of "hello world".
        let text =
            crdtsync_doc_text_insert(a, body.as_ptr(), body.len(), 0, b"hello world".as_ptr(), 11);
        let value = Scalar::Bool(true).encode_state();
        let mut mid = CrdtBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        let ops = crdtsync_doc_mark(
            a,
            body.as_ptr(),
            body.len(),
            0,
            1, // start side: right of index 0
            5,
            0, // end side: left of index 5
            b"bold".as_ptr(),
            4,
            value.as_ptr(),
            value.len(),
            &mut mid,
        );
        assert!(ops.len > 0, "authoring a mark emits ops");
        assert_eq!(mid.len, 16, "a live mark yields a 16-byte id");
        let mid_bytes: [u8; 16] = std::slice::from_raw_parts(mid.ptr, mid.len)
            .try_into()
            .unwrap();

        // Covered inside the span, absent outside it.
        let inside = marks_at(a, &body, 2);
        assert!(
            inside
                .iter()
                .any(|(name, ids)| name == b"bold" && ids.contains(&mid_bytes)),
            "the covering mark reads back with its id"
        );
        assert!(marks_at(a, &body, 7).is_empty(), "no mark past the span");

        // The mark converges onto a peer.
        exchange(b, &text);
        exchange(b, &ops);
        assert!(
            marks_at(b, &body, 2)
                .iter()
                .any(|(name, _)| name == b"bold"),
            "the mark converges"
        );

        // Changing the value re-emits ops and keeps the mark live.
        let value2 = Scalar::Int(9).encode_state();
        let set = crdtsync_doc_mark_set_value(a, mid.ptr, mid.len, value2.as_ptr(), value2.len());
        assert!(set.len > 0, "changing a live mark's value emits ops");
        exchange(b, &set);
        assert!(
            marks_at(a, &body, 2)
                .iter()
                .any(|(name, _)| name == b"bold"),
            "the mark stays live through a value change"
        );

        // Deleting drops it from the active set on both replicas.
        let del = crdtsync_doc_mark_delete(a, mid.ptr, mid.len);
        assert!(del.len > 0, "deleting a live mark emits ops");
        exchange(b, &del);
        assert!(
            marks_at(a, &body, 2)
                .iter()
                .all(|(name, _)| name != b"bold"),
            "the mark is gone from the author"
        );
        assert!(
            marks_at(b, &body, 2)
                .iter()
                .all(|(name, _)| name != b"bold"),
            "the delete converges"
        );

        for o in [text, ops, set, del] {
            crdtsync_buf_free(o);
        }
        crdtsync_buf_free(mid);
        crdtsync_doc_free(a);
        crdtsync_doc_free(b);
    }
}

#[test]
fn marks_on_a_bad_handle_or_non_sequence_are_inert() {
    unsafe {
        let value = Scalar::Bool(true).encode_state();

        // A null handle never emits, yields no id, and never dereferences.
        let mut mid = CrdtBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        let o = crdtsync_doc_mark(
            ptr::null_mut(),
            ptr::null(),
            0,
            0,
            1,
            1,
            0,
            b"x".as_ptr(),
            1,
            value.as_ptr(),
            value.len(),
            &mut mid,
        );
        assert_eq!(o.len, 0, "null handle emits nothing");
        assert_eq!(mid.len, 0, "null handle yields no id");
        crdtsync_buf_free(o);

        // A null handle read is -1, isolated from a null out pointer by a live out.
        let mut out = CrdtBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        assert_eq!(
            crdtsync_doc_marks_at(ptr::null(), ptr::null(), 0, 0, &mut out),
            -1,
            "null handle reads -1"
        );

        // A register is not a sequence: authoring is inert and the read is empty.
        let doc = crdtsync_doc_new(client(1).as_ptr());
        let reg = path(&[b"age"]);
        let r = register_int(doc, &reg, 30);
        crdtsync_buf_free(r);
        let mut mid2 = CrdtBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        let o = crdtsync_doc_mark(
            doc,
            reg.as_ptr(),
            reg.len(),
            0,
            1,
            1,
            0,
            b"bold".as_ptr(),
            4,
            value.as_ptr(),
            value.len(),
            &mut mid2,
        );
        assert_eq!(o.len, 0, "a mark on a non-sequence emits nothing");
        assert_eq!(mid2.len, 0, "no id on a non-sequence");
        crdtsync_buf_free(o);
        assert!(
            marks_at(doc, &reg, 0).is_empty(),
            "no marks on a non-sequence"
        );

        // A malformed or absent handle mutates nothing.
        let stale = [0u8; 3];
        let sv = crdtsync_doc_mark_set_value(doc, stale.as_ptr(), 3, value.as_ptr(), value.len());
        assert_eq!(sv.len, 0, "a bad-width handle sets nothing");
        crdtsync_buf_free(sv);
        let absent = [9u8; 16];
        let del = crdtsync_doc_mark_delete(doc, absent.as_ptr(), 16);
        assert_eq!(del.len, 0, "an absent handle deletes nothing");
        crdtsync_buf_free(del);

        // The register slot was never clobbered into a sequence.
        assert_eq!(get_int(doc, &reg), (1, 30));

        crdtsync_doc_free(doc);
    }
}
