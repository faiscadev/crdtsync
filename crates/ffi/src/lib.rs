//! C ABI for the CRDT core.
//!
//! Embedders (Go via cgo, Python via cffi/PyO3) hold opaque handles and call
//! paired constructor/destructor functions. The `Rc<RefCell>` value graph never
//! crosses this boundary — only handles and byte buffers do.
//!
//! A slot is addressed by a path: a length-prefixed sequence of keys (`u32` len
//! then bytes, repeated) naming nested maps from the root, the last key the slot
//! itself. A local edit returns the ops to broadcast (encoded) and applies
//! locally; `apply` folds a peer's op log back in. Navigation itself lives in
//! [`crdtsync_core::path`]; this layer only marshals pointers and buffers.
//!
//! Ownership contract:
//!   - Each `*_new` hands the caller a handle; the matching `*_free` reclaims it.
//!   - Byte buffers produced by the core are released with `crdtsync_buf_free`.
//!   - The caller never frees core-owned memory with its own allocator.
//!
//! Every entry point catches panics so one never unwinds across the C frame, and
//! rejects null or malformed input rather than dereferencing it.

use crdtsync_core::op::Op;
use crdtsync_core::{encode_ops, path, ClientId, Document};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::slice;

/// Opaque document handle.
pub struct CrdtDoc {
    doc: Document,
}

/// Owned byte buffer handed to the caller, released by [`crdtsync_buf_free`].
#[repr(C)]
pub struct CrdtBuf {
    pub ptr: *mut u8,
    pub len: usize,
}

impl CrdtBuf {
    fn from_vec(bytes: Vec<u8>) -> Self {
        let boxed = bytes.into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut u8;
        CrdtBuf { ptr, len }
    }

    fn empty() -> Self {
        CrdtBuf::from_vec(Vec::new())
    }
}

/// Borrow `len` bytes at `ptr`. A zero length is always the empty slice; a null
/// pointer with a nonzero length is rejected (`None`) rather than dereferenced,
/// since that would be UB the boundary's `catch_unwind` can't contain.
///
/// # Safety
/// When `ptr` is non-null and `len > 0`, it must point to `len` readable bytes.
unsafe fn as_slice<'a>(ptr: *const u8, len: usize) -> Option<&'a [u8]> {
    if len == 0 {
        Some(&[])
    } else if ptr.is_null() {
        None
    } else {
        Some(slice::from_raw_parts(ptr, len))
    }
}

/// Open a document for the 16-byte client id at `client`. Null on a bad handle.
///
/// # Safety
/// `client` must point to 16 readable bytes.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_new(client: *const u8) -> *mut CrdtDoc {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return std::ptr::null_mut();
        }
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(slice::from_raw_parts(client, 16));
        let id = ClientId::from_bytes(bytes);
        Box::into_raw(Box::new(CrdtDoc {
            doc: Document::new(id),
        }))
    }))
    .unwrap_or(std::ptr::null_mut())
}

/// # Safety
/// `doc` must be a handle returned by `crdtsync_doc_new` and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_free(doc: *mut CrdtDoc) {
    if doc.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| drop(Box::from_raw(doc))));
}

/// # Safety
/// `buf` must be a buffer produced by the core and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_buf_free(buf: CrdtBuf) {
    if buf.ptr.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        drop(Box::from_raw(slice::from_raw_parts_mut(buf.ptr, buf.len)));
    }));
}

/// Install-or-set an integer Register at a path. Returns the ops to broadcast;
/// empty on a bad handle or path.
///
/// # Safety
/// `doc` is a live handle; `path`/`path_len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_register_int(
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    value: i64,
) -> CrdtBuf {
    edit(doc, path, path_len, |d, p| path::register_int(d, p, value))
}

/// Install-or-increment a Counter at a path. Returns the ops to broadcast.
///
/// # Safety
/// As [`crdtsync_doc_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_inc(
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    amount: u32,
) -> CrdtBuf {
    edit(doc, path, path_len, |d, p| path::inc(d, p, amount))
}

/// Set a bytes scalar at a path. Returns the ops to broadcast.
///
/// # Safety
/// `doc` is a live handle; `path`/`path_len` and `value`/`value_len` each follow
/// [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_set_bytes(
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    value: *const u8,
    value_len: usize,
) -> CrdtBuf {
    let Some(val) = as_slice(value, value_len) else {
        return CrdtBuf::empty();
    };
    edit(doc, path, path_len, |d, p| path::set_bytes(d, p, val))
}

/// Tombstone the slot at a path. Returns the ops to broadcast.
///
/// # Safety
/// As [`crdtsync_doc_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_delete(
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
) -> CrdtBuf {
    edit(doc, path, path_len, path::delete)
}

/// Read an integer Register at a path into `out`. Returns 1 when found and an
/// integer, 0 when absent or another type, -1 on a bad handle.
///
/// # Safety
/// `doc` is a live handle or null; `path`/`path_len` follow [`as_slice`]; `out`
/// points to a writable `i64`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_get_int(
    doc: *const CrdtDoc,
    path: *const u8,
    path_len: usize,
    out: *mut i64,
) -> i32 {
    read_i64(doc, path, path_len, out, path::get_int)
}

/// Read a Counter's value at a path into `out`. Returns 1/0/-1 as
/// [`crdtsync_doc_get_int`].
///
/// # Safety
/// As [`crdtsync_doc_get_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_get_counter(
    doc: *const CrdtDoc,
    path: *const u8,
    path_len: usize,
    out: *mut i64,
) -> i32 {
    read_i64(doc, path, path_len, out, path::get_counter)
}

/// Read a bytes scalar at a path into `out` (a fresh buffer the caller frees).
/// Returns 1 when found, 0 when absent or another type, -1 on a bad handle.
///
/// # Safety
/// `doc` is a live handle or null; `path`/`path_len` follow [`as_slice`]; `out`
/// points to a writable `CrdtBuf`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_get_bytes(
    doc: *const CrdtDoc,
    path: *const u8,
    path_len: usize,
    out: *mut CrdtBuf,
) -> i32 {
    read_buf(doc, path, path_len, out, path::get_bytes)
}

/// Insert a bytes item into the List at a path, at live `index`. Returns the ops
/// to broadcast.
///
/// # Safety
/// `doc` is a live handle; `path`/`path_len` and `value`/`value_len` follow
/// [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_list_insert(
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    index: usize,
    value: *const u8,
    value_len: usize,
) -> CrdtBuf {
    let Some(val) = as_slice(value, value_len) else {
        return CrdtBuf::empty();
    };
    edit(doc, path, path_len, |d, p| {
        path::list_insert(d, p, index, val)
    })
}

/// Tombstone the live item at `index` in the List at a path.
///
/// # Safety
/// As [`crdtsync_doc_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_list_delete(
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    index: usize,
) -> CrdtBuf {
    edit(doc, path, path_len, |d, p| path::list_delete(d, p, index))
}

/// Read the live length of the List at a path into `out`. Returns 1/0/-1.
///
/// # Safety
/// As [`crdtsync_doc_get_int`], with `out` a writable `usize`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_list_len(
    doc: *const CrdtDoc,
    path: *const u8,
    path_len: usize,
    out: *mut usize,
) -> i32 {
    read_usize(doc, path, path_len, out, path::list_len)
}

/// Read the bytes item at live `index` in the List at a path into `out`. Returns
/// 1 when present and a bytes item, 0 otherwise, -1 on a bad handle.
///
/// # Safety
/// As [`crdtsync_doc_get_bytes`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_list_get(
    doc: *const CrdtDoc,
    path: *const u8,
    path_len: usize,
    index: usize,
    out: *mut CrdtBuf,
) -> i32 {
    read_buf(doc, path, path_len, out, |d, p| path::list_get(d, p, index))
}

/// Insert UTF-8 `text` into the Text at a path, at codepoint `index`. Returns the
/// ops to broadcast; empty on a bad handle/path or non-UTF-8 input.
///
/// # Safety
/// `doc` is a live handle; `path`/`path_len` and `text`/`text_len` follow
/// [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_text_insert(
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    index: usize,
    text: *const u8,
    text_len: usize,
) -> CrdtBuf {
    let Some(raw) = as_slice(text, text_len) else {
        return CrdtBuf::empty();
    };
    let Ok(s) = std::str::from_utf8(raw) else {
        return CrdtBuf::empty();
    };
    edit(doc, path, path_len, |d, p| {
        path::text_insert(d, p, index, s)
    })
}

/// Tombstone `count` codepoints from codepoint `index` in the Text at a path.
///
/// # Safety
/// As [`crdtsync_doc_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_text_delete(
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    index: usize,
    count: usize,
) -> CrdtBuf {
    edit(doc, path, path_len, |d, p| {
        path::text_delete(d, p, index, count)
    })
}

/// Read the codepoint length of the Text at a path into `out`. Returns 1/0/-1.
///
/// # Safety
/// As [`crdtsync_doc_list_len`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_text_len(
    doc: *const CrdtDoc,
    path: *const u8,
    path_len: usize,
    out: *mut usize,
) -> i32 {
    read_usize(doc, path, path_len, out, path::text_len)
}

/// Read the Text at a path into `out` as UTF-8 bytes. Returns 1/0/-1.
///
/// # Safety
/// As [`crdtsync_doc_get_bytes`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_text_get(
    doc: *const CrdtDoc,
    path: *const u8,
    path_len: usize,
    out: *mut CrdtBuf,
) -> i32 {
    read_buf(doc, path, path_len, out, |d, p| {
        path::text_get(d, p).map(String::into_bytes)
    })
}

/// Fold an encoded op log (as returned by an edit) from a peer into the
/// document. Returns the number of ops applied now (a duplicate or one buffered
/// pending its target counts as not-applied), or -1 on a bad handle or
/// malformed bytes.
///
/// # Safety
/// `doc` is a live handle or null; `bytes`/`len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_apply(
    doc: *mut CrdtDoc,
    bytes: *const u8,
    len: usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() {
            return -1;
        }
        let handle = &mut *doc;
        let Some(raw) = as_slice(bytes, len) else {
            return -1;
        };
        match crdtsync_core::decode_ops(raw) {
            Ok(ops) => ops.iter().filter(|op| handle.doc.apply(op)).count() as i32,
            Err(_) => -1,
        }
    }))
    .unwrap_or(-1)
}

/// Serialize the whole replica to a canonical snapshot. Empty on a bad handle.
///
/// # Safety
/// `doc` must be a handle returned by a constructor and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_encode_state(doc: *const CrdtDoc) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() {
            return CrdtBuf::empty();
        }
        CrdtBuf::from_vec((*doc).doc.encode_state())
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Open a document from a snapshot produced by [`crdtsync_doc_encode_state`].
/// Null on a malformed snapshot or bad input, never a panic across the frame.
///
/// # Safety
/// `bytes`/`len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_decode_state(bytes: *const u8, len: usize) -> *mut CrdtDoc {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(raw) = as_slice(bytes, len) else {
            return std::ptr::null_mut();
        };
        match Document::decode_state(raw) {
            Ok(doc) => Box::into_raw(Box::new(CrdtDoc { doc })),
            Err(_) => std::ptr::null_mut(),
        }
    }))
    .unwrap_or(std::ptr::null_mut())
}

/// Marshal a path-addressed edit: delegate the navigation to `run`, encode the
/// emitted ops, and never let a panic cross the C frame.
unsafe fn edit<F>(doc: *mut CrdtDoc, path: *const u8, path_len: usize, run: F) -> CrdtBuf
where
    F: FnOnce(&mut Document, &[u8]) -> Vec<Op>,
{
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() {
            return CrdtBuf::empty();
        }
        let Some(p) = as_slice(path, path_len) else {
            return CrdtBuf::empty();
        };
        let ops = run(&mut (*doc).doc, p);
        CrdtBuf::from_vec(encode_ops(&ops))
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Read an `i64`-valued slot through `run` into `out`.
unsafe fn read_i64<F>(
    doc: *const CrdtDoc,
    path: *const u8,
    path_len: usize,
    out: *mut i64,
    run: F,
) -> i32
where
    F: FnOnce(&Document, &[u8]) -> Option<i64>,
{
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() || out.is_null() {
            return -1;
        }
        let Some(p) = as_slice(path, path_len) else {
            return 0;
        };
        match run(&(*doc).doc, p) {
            Some(n) => {
                *out = n;
                1
            }
            None => 0,
        }
    }))
    .unwrap_or(-1)
}

/// Read a `usize`-valued slot through `run` into `out`.
unsafe fn read_usize<F>(
    doc: *const CrdtDoc,
    path: *const u8,
    path_len: usize,
    out: *mut usize,
    run: F,
) -> i32
where
    F: FnOnce(&Document, &[u8]) -> Option<usize>,
{
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() || out.is_null() {
            return -1;
        }
        let Some(p) = as_slice(path, path_len) else {
            return 0;
        };
        match run(&(*doc).doc, p) {
            Some(n) => {
                *out = n;
                1
            }
            None => 0,
        }
    }))
    .unwrap_or(-1)
}

/// Read a byte payload through `run` into a fresh buffer at `out` the caller frees.
unsafe fn read_buf<F>(
    doc: *const CrdtDoc,
    path: *const u8,
    path_len: usize,
    out: *mut CrdtBuf,
    run: F,
) -> i32
where
    F: FnOnce(&Document, &[u8]) -> Option<Vec<u8>>,
{
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() || out.is_null() {
            return -1;
        }
        let Some(p) = as_slice(path, path_len) else {
            return 0;
        };
        match run(&(*doc).doc, p) {
            Some(b) => {
                *out = CrdtBuf::from_vec(b);
                1
            }
            None => 0,
        }
    }))
    .unwrap_or(-1)
}
