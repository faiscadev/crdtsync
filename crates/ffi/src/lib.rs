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
//! Scaffold only — every body is `todo!()`. `extern "C"` bodies will wrap their
//! work in `catch_unwind` once implemented; a panic must not unwind past the
//! boundary.

use crdtsync_core::map::Map;

/// Opaque root document handle. (A dedicated Doc type lands with the Op layer;
/// the root is a Map for now.)
pub struct CrdtDoc {
    _root: Map,
}

/// Owned byte buffer handed to the caller, released by [`crdtsync_buf_free`].
#[repr(C)]
pub struct CrdtBuf {
    ptr: *mut u8,
    len: usize,
}

#[no_mangle]
pub extern "C" fn crdtsync_doc_new() -> *mut CrdtDoc {
    todo!()
}

/// # Safety
/// `doc` must be a handle returned by `crdtsync_doc_new` and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_free(doc: *mut CrdtDoc) {
    let _ = doc;
    todo!()
}

/// # Safety
/// `buf` must be a buffer produced by the core and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_buf_free(buf: CrdtBuf) {
    let _ = buf;
    todo!()
}
