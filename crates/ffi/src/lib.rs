//! C ABI for the CRDT core.
//!
//! Embedders (Go via cgo, Python via cffi/PyO3) hold opaque handles and call
//! paired constructor/destructor functions. The `Rc<RefCell>` value graph never
//! crosses this boundary — only handles and byte buffers do.
//!
//! Ownership contract:
//!   - Each `*_new` hands the caller a handle; the matching `*_free` reclaims it.
//!   - Byte buffers produced by the core are released with `crdtsync_buf_free`.
//!   - The caller never frees core-owned memory with its own allocator.
//!
//! A local edit returns the ops to broadcast (encoded) and applies locally;
//! `apply` folds a peer's op back in. Every entry point catches panics so one
//! never unwinds across the C frame; a bad handle or malformed input is
//! reported, never dereferenced.

use crdtsync_core::doc::MapCursor;
use crdtsync_core::{decode_ops, encode_ops, ClientId, Document, Element, Scalar};
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

/// Install-or-set an integer Register at a root key. Returns the ops to
/// broadcast, encoded; empty on a bad handle.
///
/// # Safety
/// `doc` is a live handle; `key` points to `key_len` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_register_int(
    doc: *mut CrdtDoc,
    key: *const u8,
    key_len: usize,
    value: i64,
) -> CrdtBuf {
    emit(doc, key, key_len, |tx, k| {
        tx.register(k, Scalar::Int(value))
    })
}

/// Install-or-increment a Counter at a root key. Returns the ops to broadcast.
///
/// # Safety
/// `doc` is a live handle; `key` points to `key_len` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_inc(
    doc: *mut CrdtDoc,
    key: *const u8,
    key_len: usize,
    amount: u32,
) -> CrdtBuf {
    emit(doc, key, key_len, |tx, k| tx.inc(k, amount))
}

/// Read an integer Register at a root key into `out`. Returns 1 when found and
/// an integer, 0 when absent or another type, -1 on a bad handle.
///
/// # Safety
/// `doc` is a live handle or null; `key` points to `key_len` readable bytes;
/// `out` points to a writable `i64`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_get_int(
    doc: *const CrdtDoc,
    key: *const u8,
    key_len: usize,
    out: *mut i64,
) -> i32 {
    read(doc, key, key_len, out, |e| match e {
        Element::Register(r) => match r.borrow().read() {
            Scalar::Int(n) => Some(*n),
            _ => None,
        },
        _ => None,
    })
}

/// Read a Counter's value at a root key into `out`. Returns 1 when found, 0 when
/// absent or another type, -1 on a bad handle.
///
/// # Safety
/// As [`crdtsync_doc_get_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_get_counter(
    doc: *const CrdtDoc,
    key: *const u8,
    key_len: usize,
    out: *mut i64,
) -> i32 {
    read(doc, key, key_len, out, |e| match e {
        Element::Counter(c) => Some(c.borrow().read()),
        _ => None,
    })
}

/// Fold an encoded op log (as returned by an edit) from a peer into the
/// document. Returns the number of ops applied now (a duplicate or one buffered
/// pending its target counts as not-applied), or -1 on a bad handle or
/// malformed bytes.
///
/// # Safety
/// `doc` is a live handle or null; `bytes` points to `len` readable bytes.
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
        let raw = if len == 0 {
            &[][..]
        } else {
            slice::from_raw_parts(bytes, len)
        };
        match decode_ops(raw) {
            Ok(ops) => ops.iter().filter(|op| handle.doc.apply(op)).count() as i32,
            Err(_) => -1,
        }
    }))
    .unwrap_or(-1)
}

/// Run a root-level edit, apply it locally, and return its encoded ops.
unsafe fn emit<F>(doc: *mut CrdtDoc, key: *const u8, key_len: usize, edit: F) -> CrdtBuf
where
    F: FnOnce(&mut MapCursor, &[u8]),
{
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() {
            return CrdtBuf::empty();
        }
        let handle = &mut *doc;
        let key = slice::from_raw_parts(key, key_len).to_vec();
        let ops = handle.doc.transact(|tx| edit(tx, &key));
        CrdtBuf::from_vec(encode_ops(&ops))
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Read a root slot through `pick`, writing `out` and returning 1/0/-1.
unsafe fn read<F>(
    doc: *const CrdtDoc,
    key: *const u8,
    key_len: usize,
    out: *mut i64,
    pick: F,
) -> i32
where
    F: FnOnce(&Element) -> Option<i64>,
{
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() || out.is_null() {
            return -1;
        }
        let handle = &*doc;
        let key = slice::from_raw_parts(key, key_len);
        match handle.doc.get(key).as_ref().and_then(pick) {
            Some(n) => {
                *out = n;
                1
            }
            None => 0,
        }
    }))
    .unwrap_or(-1)
}
