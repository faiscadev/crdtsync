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

use crdtsync_core::diff::{diff, encode_changes};
use crdtsync_core::list::Side;
use crdtsync_core::op::Op;
use crdtsync_core::{
    decode_message, encode_message, encode_op, encode_ops, path, Channel, ClientError, ClientId,
    ClientSession, Document, ErrorCode, MarkState, Message, Rejected, RelativePosition,
    ResolvedMark, Scalar, UndoManager,
};
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

/// Install-or-decrement a Counter at a path. Returns the ops to broadcast.
///
/// # Safety
/// As [`crdtsync_doc_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_dec(
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    amount: u32,
) -> CrdtBuf {
    edit(doc, path, path_len, |d, p| path::dec(d, p, amount))
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

// --- xml navigation ---
//
// An XmlElement/XmlFragment is a container installed at a map-slot path like any
// other; its children are an index-addressed sequence a child has no stable path
// key of its own, so a child edit names its parent path plus a live index. Edits
// return the ops to broadcast; reads follow the present/absent status idiom.

/// Install an `XmlElement` tagged `tag` at a map-slot path. Returns the ops to
/// broadcast; empty on a bad handle/path or a null tag.
///
/// # Safety
/// `doc` is a live handle; `path`/`path_len` and `tag`/`tag_len` follow
/// [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_xml_element(
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    tag: *const u8,
    tag_len: usize,
) -> CrdtBuf {
    let Some(t) = as_slice(tag, tag_len) else {
        return CrdtBuf::empty();
    };
    edit(doc, path, path_len, |d, p| path::xml_element(d, p, t))
}

/// Install a tagless `XmlFragment` at a map-slot path. Returns the ops to
/// broadcast.
///
/// # Safety
/// As [`crdtsync_doc_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_xml_fragment(
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
) -> CrdtBuf {
    edit(doc, path, path_len, path::xml_fragment)
}

/// Read the tag of the live `XmlElement` at a path into `out`. Returns 1 when
/// found, 0 when absent or not a tagged element (a fragment is tagless), -1 on a
/// bad handle.
///
/// # Safety
/// As [`crdtsync_doc_get_bytes`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_xml_tag(
    doc: *const CrdtDoc,
    path: *const u8,
    path_len: usize,
    out: *mut CrdtBuf,
) -> i32 {
    read_buf(doc, path, path_len, out, path::xml_tag)
}

/// Insert a nested `XmlElement` child tagged `tag` at live `index` in the children
/// of the element/fragment at `elem_path`. Inert (empty ops) if `elem_path` is not
/// a live xml node or `tag` is null.
///
/// # Safety
/// `doc` is a live handle; `elem_path`/`elem_path_len` and `tag`/`tag_len` follow
/// [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_xml_insert_element(
    doc: *mut CrdtDoc,
    elem_path: *const u8,
    elem_path_len: usize,
    index: usize,
    tag: *const u8,
    tag_len: usize,
) -> CrdtBuf {
    let Some(t) = as_slice(tag, tag_len) else {
        return CrdtBuf::empty();
    };
    edit(doc, elem_path, elem_path_len, |d, p| {
        path::xml_insert_element(d, p, index, t)
    })
}

/// Insert a `Text`-run child initialised with UTF-8 `s` at live `index` in the
/// children of the element/fragment at `elem_path`. Inert if the target is not a
/// live xml node or `s` is non-UTF-8.
///
/// # Safety
/// `doc` is a live handle; `elem_path`/`elem_path_len` and `s`/`s_len` follow
/// [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_xml_insert_text(
    doc: *mut CrdtDoc,
    elem_path: *const u8,
    elem_path_len: usize,
    index: usize,
    s: *const u8,
    s_len: usize,
) -> CrdtBuf {
    let Some(raw) = as_slice(s, s_len) else {
        return CrdtBuf::empty();
    };
    let Ok(text) = std::str::from_utf8(raw) else {
        return CrdtBuf::empty();
    };
    edit(doc, elem_path, elem_path_len, |d, p| {
        path::xml_insert_text(d, p, index, text)
    })
}

/// Tombstone the child at live `index` in the children of the element/fragment at
/// `elem_path`. Inert if the target is not a live xml node or `index` names no
/// live child.
///
/// # Safety
/// As [`crdtsync_doc_register_int`], with `elem_path` the parent's path.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_xml_child_delete(
    doc: *mut CrdtDoc,
    elem_path: *const u8,
    elem_path_len: usize,
    index: usize,
) -> CrdtBuf {
    edit(doc, elem_path, elem_path_len, |d, p| {
        path::xml_child_delete(d, p, index)
    })
}

/// Read the count of live children of the element/fragment at `elem_path` into
/// `out`. Returns 1 when found, 0 when the path is not a live xml node, -1 on a
/// bad handle.
///
/// # Safety
/// As [`crdtsync_doc_list_len`], with `elem_path` the node's path.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_xml_children_len(
    doc: *const CrdtDoc,
    elem_path: *const u8,
    elem_path_len: usize,
    out: *mut usize,
) -> i32 {
    read_usize(doc, elem_path, elem_path_len, out, path::xml_children_len)
}

/// Relocate the live child at `child_index` under the xml node at `parent_path` to
/// `dest_index` in the children of the xml node at `new_parent_path` — a Kleppmann
/// tree move that keeps the child's identity and subtree. Inert if either path is
/// not a live xml node or `child_index` names no live child.
///
/// # Safety
/// `doc` is a live handle; `parent_path`/`parent_path_len` and
/// `new_parent_path`/`new_parent_path_len` each follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_xml_move(
    doc: *mut CrdtDoc,
    parent_path: *const u8,
    parent_path_len: usize,
    child_index: usize,
    new_parent_path: *const u8,
    new_parent_path_len: usize,
    dest_index: usize,
) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() {
            return CrdtBuf::empty();
        }
        let (Some(pp), Some(np)) = (
            as_slice(parent_path, parent_path_len),
            as_slice(new_parent_path, new_parent_path_len),
        ) else {
            return CrdtBuf::empty();
        };
        let ops = path::xml_move_child(&mut (*doc).doc, pp, child_index, np, dest_index);
        CrdtBuf::from_vec(encode_ops(&ops))
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Map the C `side` argument to a [`Side`]: 0 is left of the index, 1 is right.
fn side_from_u32(side: u32) -> Option<Side> {
    match side {
        0 => Some(Side::Left),
        1 => Some(Side::Right),
        _ => None,
    }
}

// --- marks ---
//
// A mark is a named range over a sequence (a Text or List), authored with the two
// endpoints as `(index, side)` pairs and a scalar payload, and read back per its
// resolved state at a character. Authoring emits ops like any edit; the create
// returns the mark's 16-byte id — the handle a later set-value/delete names it by
// — through an out buffer the caller frees, empty when the author was inert. The
// scalar payload crosses as its canonical [`Scalar::encode_state`] bytes and the
// id as its raw 16 bytes, the same shapes those values cross elsewhere in the ABI.

/// Serialize the resolved marks on a character to one buffer the SDK decodes: a
/// `u32` count, then per mark a `u32`-length-prefixed name, a one-byte flavor tag,
/// and that tag's payload — `0` a boolean byte, `1` a `u32`-length-prefixed encoded
/// [`Scalar`], `2` a `u32` count of raw 16-byte element ids.
fn encode_resolved_marks(marks: &[ResolvedMark]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(marks.len() as u32).to_le_bytes());
    for m in marks {
        out.extend_from_slice(&(m.name.len() as u32).to_le_bytes());
        out.extend_from_slice(&m.name);
        match &m.state {
            MarkState::Boolean(b) => {
                out.push(0);
                out.push(*b as u8);
            }
            MarkState::Value(v) => {
                out.push(1);
                let bytes = v.encode_state();
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(&bytes);
            }
            MarkState::Object(ids) => {
                out.push(2);
                out.extend_from_slice(&(ids.len() as u32).to_le_bytes());
                for id in ids {
                    out.extend_from_slice(&id.as_bytes());
                }
            }
        }
    }
    out
}

/// Author a named mark over `[start, end)` of the sequence at `seq_path`, each
/// endpoint an `(index, side)` pair (`side` 0 left of the index, 1 right) and
/// `value` an encoded [`Scalar`] payload. Returns the ops to broadcast and writes
/// the mark's 16-byte id into `out_mark_id` (a fresh buffer the caller frees).
/// Inert — empty ops, `out_mark_id` left empty — on a bad handle, a non-sequence
/// path, an unknown `side`, or a malformed value.
///
/// # Safety
/// `doc` is a live handle; `seq_path`/`seq_path_len`, `name`/`name_len`, and
/// `value`/`value_len` each follow [`as_slice`]; `out_mark_id`, when non-null,
/// points to a writable `CrdtBuf`.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn crdtsync_doc_mark(
    doc: *mut CrdtDoc,
    seq_path: *const u8,
    seq_path_len: usize,
    start_index: usize,
    start_side: u32,
    end_index: usize,
    end_side: u32,
    name: *const u8,
    name_len: usize,
    value: *const u8,
    value_len: usize,
    out_mark_id: *mut CrdtBuf,
) -> CrdtBuf {
    let Some(m) = mark_endpoints(start_side, end_side, name, name_len, value, value_len) else {
        return CrdtBuf::empty();
    };
    edit(doc, seq_path, seq_path_len, |d, p| {
        let (ops, id) = path::mark(
            d,
            p,
            start_index,
            m.start_side,
            end_index,
            m.end_side,
            m.name,
            m.value,
        );
        unsafe { write_mark_id(out_mark_id, id) };
        ops
    })
}

/// Change the scalar payload of the mark handle `mark_id` (16 bytes from
/// [`crdtsync_doc_mark`]) to the encoded [`Scalar`] `value`. Returns the ops to
/// broadcast; inert (empty) on a bad handle, a handle that names no live mark, or
/// a malformed value.
///
/// # Safety
/// `doc` is a live handle; `mark_id`/`mark_id_len` and `value`/`value_len` follow
/// [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_mark_set_value(
    doc: *mut CrdtDoc,
    mark_id: *const u8,
    mark_id_len: usize,
    value: *const u8,
    value_len: usize,
) -> CrdtBuf {
    let Some(scalar) = decode_scalar(value, value_len) else {
        return CrdtBuf::empty();
    };
    edit(doc, mark_id, mark_id_len, |d, id| {
        path::mark_set_value(d, id, scalar)
    })
}

/// Tombstone the mark handle `mark_id` (16 bytes from [`crdtsync_doc_mark`]).
/// Returns the ops to broadcast; inert (empty) on a bad handle or a handle that
/// names no live mark.
///
/// # Safety
/// `doc` is a live handle; `mark_id`/`mark_id_len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_mark_delete(
    doc: *mut CrdtDoc,
    mark_id: *const u8,
    mark_id_len: usize,
) -> CrdtBuf {
    edit(doc, mark_id, mark_id_len, path::mark_delete)
}

/// Read the marks active on character `index` of the sequence at `seq_path` into
/// `out` — the [`encode_resolved_marks`] buffer the caller frees, decoded with the
/// SDK's marks reader. Returns 1 with the encoded list (a non-sequence path or an
/// uncovered index encodes zero marks), 0 on a malformed `seq_path`, -1 on a bad
/// handle or a null `out`.
///
/// # Safety
/// `doc` is a live handle or null; `seq_path`/`seq_path_len` follow [`as_slice`];
/// `out` points to a writable `CrdtBuf`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_marks_at(
    doc: *const CrdtDoc,
    seq_path: *const u8,
    seq_path_len: usize,
    index: usize,
    out: *mut CrdtBuf,
) -> i32 {
    read_buf(doc, seq_path, seq_path_len, out, |d, p| {
        Some(encode_resolved_marks(&path::marks_at(d, p, index)))
    })
}

/// The validated endpoints of a mark author, shared by the doc and client
/// surfaces; the sequence path is validated by the surface's edit helper.
struct MarkEndpoints<'a> {
    start_side: Side,
    end_side: Side,
    name: &'a [u8],
    value: Scalar,
}

/// Borrow and validate a mark author's sides, name, and encoded value. `None` if a
/// side is unknown, the name pointer is rejected, or the value is malformed.
unsafe fn mark_endpoints<'a>(
    start_side: u32,
    end_side: u32,
    name: *const u8,
    name_len: usize,
    value: *const u8,
    value_len: usize,
) -> Option<MarkEndpoints<'a>> {
    Some(MarkEndpoints {
        start_side: side_from_u32(start_side)?,
        end_side: side_from_u32(end_side)?,
        name: as_slice(name, name_len)?,
        value: decode_scalar(value, value_len)?,
    })
}

/// Decode a mark's [`Scalar`] payload from `value`/`value_len`. `None` if the
/// pointer is rejected or the bytes are not a canonical scalar encoding.
unsafe fn decode_scalar(value: *const u8, value_len: usize) -> Option<Scalar> {
    Scalar::decode_state(as_slice(value, value_len)?).ok()
}

/// Write a freshly-authored mark's id into `out` (a caller-freed buffer), when the
/// author yielded one and `out` is non-null. An inert author leaves `out` untouched
/// — the caller's nulled buffer stays empty, its len-0 the absent signal.
unsafe fn write_mark_id(out: *mut CrdtBuf, id: Option<Vec<u8>>) {
    if let (Some(id), false) = (id, out.is_null()) {
        *out = CrdtBuf::from_vec(id);
    }
}

/// Capture a stable position in the List or Text at a path — the encoded
/// [`RelativePosition`] bytes, resolved later with
/// [`crdtsync_doc_resolve_position`]. `side` is 0 (left of `index`) or 1 (right).
/// Empty on a bad handle/path, a non-sequence slot, or an unknown `side`.
///
/// # Safety
/// `doc` is a live handle or null; `path`/`path_len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_relative_position(
    doc: *const CrdtDoc,
    path: *const u8,
    path_len: usize,
    index: usize,
    side: u32,
) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() {
            return CrdtBuf::empty();
        }
        let (Some(p), Some(side)) = (as_slice(path, path_len), side_from_u32(side)) else {
            return CrdtBuf::empty();
        };
        match path::relative_position(&(*doc).doc, p, index, side) {
            Some(pos) => CrdtBuf::from_vec(pos.encode()),
            None => CrdtBuf::empty(),
        }
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Resolve a captured position (bytes from [`crdtsync_doc_relative_position`])
/// back to a live index in the List or Text at a path, written to `out`. Returns
/// 1 when resolved, 0 on a bad path / non-sequence slot / malformed position
/// bytes, -1 on a bad handle or panic.
///
/// # Safety
/// `doc` is a live handle or null; `path`/`path_len` and `pos`/`pos_len` follow
/// [`as_slice`]; `out` is a writable `usize`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_resolve_position(
    doc: *const CrdtDoc,
    path: *const u8,
    path_len: usize,
    pos: *const u8,
    pos_len: usize,
    out: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() || out.is_null() {
            return -1;
        }
        let (Some(p), Some(pos_raw)) = (as_slice(path, path_len), as_slice(pos, pos_len)) else {
            return 0;
        };
        let Ok(position) = RelativePosition::decode(pos_raw) else {
            return 0;
        };
        match path::resolve_position(&(*doc).doc, p, &position) {
            Some(n) => {
                *out = n;
                1
            }
            None => 0,
        }
    }))
    .unwrap_or(-1)
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

/// Begin recording an atomic transaction: until [`crdtsync_doc_commit_atomic`],
/// edits accumulate into one group and each returns an empty ops buffer.
///
/// # Safety
/// `doc` must be a handle returned by a constructor and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_begin_atomic(doc: *mut CrdtDoc) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !doc.is_null() {
            (*doc).doc.begin_atomic();
        }
    }));
}

/// Commit the atomic transaction opened by [`crdtsync_doc_begin_atomic`],
/// returning the group's ops tagged for all-or-nothing delivery. Empty on a bad
/// handle, no open transaction, or an empty group.
///
/// # Safety
/// `doc` must be a handle returned by a constructor and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_commit_atomic(doc: *mut CrdtDoc) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() {
            return CrdtBuf::empty();
        }
        CrdtBuf::from_vec(encode_ops(&(*doc).doc.commit_atomic()))
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
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

/// Diff two snapshots (each a state buffer from [`crdtsync_doc_encode_state`],
/// a named version, or an exported room) into the encoded change list — the
/// structural changes turning the old state into the new. Decode it with the
/// SDK's change-list reader. Empty on malformed input or a bad snapshot, never
/// a panic across the frame.
///
/// # Safety
/// `old`/`old_len` and `new`/`new_len` each follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_diff(
    old: *const u8,
    old_len: usize,
    new: *const u8,
    new_len: usize,
) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(old_raw), Some(new_raw)) = (as_slice(old, old_len), as_slice(new, new_len))
        else {
            return CrdtBuf::empty();
        };
        let (Ok(old_doc), Ok(new_doc)) = (
            Document::decode_state(old_raw),
            Document::decode_state(new_raw),
        ) else {
            return CrdtBuf::empty();
        };
        CrdtBuf::from_vec(encode_changes(&diff(&old_doc, &new_doc)))
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Decode a change-list buffer from [`crdtsync_diff`] back into its canonical,
/// SDK-marshalable form, written to `out` — the boundary read that turns opaque
/// diff bytes into the structured change list a binding walks. A diff crosses an
/// untrusted boundary (a wire message or a stored snapshot), so the decode is
/// total: a truncated or garbage buffer yields 0 with `out` left untouched, never
/// a panic across the frame. Returns 1 with the canonical change list on a
/// well-formed buffer, -1 on a null `out` or a panic.
///
/// # Safety
/// `bytes`/`len` follow [`as_slice`]; `out` points to a writable `CrdtBuf`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_diff_decode(
    bytes: *const u8,
    len: usize,
    out: *mut CrdtBuf,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if out.is_null() {
            return -1;
        }
        let Some(raw) = as_slice(bytes, len) else {
            return 0;
        };
        match path::decode_changes(raw) {
            Ok(changes) => {
                *out = CrdtBuf::from_vec(encode_changes(&changes));
                1
            }
            Err(_) => 0,
        }
    }))
    .unwrap_or(-1)
}

// --- schema + repair ---
//
// A schema binds to the local document as runtime state — it authors no op and
// broadcasts nothing, so nothing crosses to enqueue. It crosses as opaque JSON
// bytes the façade parses (total — malformed fails, never panics). `take_repairs`
// is a read: it drains the `onRepaired` signal, the located paths whose repaired
// reading newly changed against the bound schema, each an `encode_repair_path`
// byte string. The drain reseeds the baseline, so a standing repair reports once;
// the whole list crosses as one buffer with a `u32` count and per-path length
// prefixes, the same list shape marks and diffs cross as.

/// Serialize a repair-path list to one buffer the SDK decodes: a `u32` count, then
/// per path a `u32`-length-prefixed `encode_repair_path` byte string. An empty list
/// is a bare zero count, the no-repair signal.
fn encode_repair_paths(paths: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(paths.len() as u32).to_le_bytes());
    for p in paths {
        out.extend_from_slice(&(p.len() as u32).to_le_bytes());
        out.extend_from_slice(p);
    }
    out
}

/// Parse schema JSON bytes and bind the schema to the local document for
/// `onRepaired` observation. A binding is runtime state, not a CRDT op — it
/// authors and broadcasts nothing — so there is nothing to return but the outcome.
/// Parsing is total: returns 1 when the schema bound, 0 when the bytes are not a
/// valid schema (malformed JSON, non-UTF-8, or well-formed JSON that is not a
/// schema — rejected cleanly, binding nothing), -1 on a bad handle or a null
/// pointer. Binding takes the current state as the baseline, so a later
/// [`crdtsync_doc_take_repairs`] surfaces only a repair the state comes to need.
///
/// # Safety
/// `doc` is a live handle or null; `schema`/`schema_len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_set_schema(
    doc: *mut CrdtDoc,
    schema: *const u8,
    schema_len: usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() {
            return -1;
        }
        let Some(bytes) = as_slice(schema, schema_len) else {
            return -1;
        };
        if path::set_schema(&mut (*doc).doc, bytes) {
            1
        } else {
            0
        }
    }))
    .unwrap_or(-1)
}

/// Drain the `onRepaired` signal into `out`: the located paths whose repaired
/// reading has newly changed against the bound schema since the last call, each an
/// `encode_repair_path` byte string the SDK decodes with the repair-path reader (or
/// [`crdtsync_repair_path_decode`]). Empty — a bare zero count — when no schema is
/// bound or nothing newly needs repair; the drain reseeds the baseline, so a
/// standing repair reports once (the settle-point contract). A reported path names
/// a *location*, not a value: the repaired value is read separately. Returns 1 with
/// the encoded list, -1 on a bad handle or a null `out`.
///
/// # Safety
/// `doc` is a live handle or null; `out` points to a writable `CrdtBuf`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_doc_take_repairs(doc: *mut CrdtDoc, out: *mut CrdtBuf) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if doc.is_null() || out.is_null() {
            return -1;
        }
        let paths = path::take_repairs(&mut (*doc).doc);
        *out = CrdtBuf::from_vec(encode_repair_paths(&paths));
        1
    }))
    .unwrap_or(-1)
}

/// Decode a repair-path buffer from [`crdtsync_doc_take_repairs`] back into its
/// canonical form, written to `out` — the boundary read that turns opaque repair
/// bytes into the step path a binding walks, mirroring [`crdtsync_diff_decode`]. A
/// repair path can cross an untrusted boundary, so the decode is total: an unknown
/// step tag or a length past the end yields 0 with `out` left untouched, never a
/// panic across the frame. Returns 1 with the canonical step path on a well-formed
/// buffer, -1 on a null `out` or a panic.
///
/// # Safety
/// `bytes`/`len` follow [`as_slice`]; `out` points to a writable `CrdtBuf`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_repair_path_decode(
    bytes: *const u8,
    len: usize,
    out: *mut CrdtBuf,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if out.is_null() {
            return -1;
        }
        let Some(raw) = as_slice(bytes, len) else {
            return 0;
        };
        match path::parse_repair_path(raw) {
            Some(steps) => {
                *out = CrdtBuf::from_vec(path::encode_repair_path(&steps));
                1
            }
            None => 0,
        }
    }))
    .unwrap_or(-1)
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

// --- undo / redo ---
//
// A per-user undo manager over one document. Edits recorded through it capture
// their inverse; `undo`/`redo` emit ordinary ops that converge on peers like any
// edit. The manager is a handle distinct from the document it drives, so every
// call names both. Edits return the ops to broadcast, encoded like a doc edit.

/// Opaque undo-manager handle.
pub struct CrdtUndo {
    undo: UndoManager,
}

/// Open an undo manager. It drives whichever document is passed to each call.
///
/// # Safety
/// The returned handle is freed with [`crdtsync_undo_free`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_new() -> *mut CrdtUndo {
    catch_unwind(AssertUnwindSafe(|| {
        Box::into_raw(Box::new(CrdtUndo {
            undo: UndoManager::new(),
        }))
    }))
    .unwrap_or(std::ptr::null_mut())
}

/// # Safety
/// `undo` must be a handle from `crdtsync_undo_new`, not yet freed.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_free(undo: *mut CrdtUndo) {
    if undo.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| drop(Box::from_raw(undo))));
}

/// Record a path-addressed edit through the manager, applying it to `doc` and
/// returning the ops to broadcast.
unsafe fn undo_edit<F>(
    undo: *mut CrdtUndo,
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    run: F,
) -> CrdtBuf
where
    F: FnOnce(&mut UndoManager, &mut Document, &[u8]) -> Vec<Op>,
{
    catch_unwind(AssertUnwindSafe(|| {
        if undo.is_null() || doc.is_null() {
            return CrdtBuf::empty();
        }
        let Some(p) = as_slice(path, path_len) else {
            return CrdtBuf::empty();
        };
        let ops = run(&mut (*undo).undo, &mut (*doc).doc, p);
        CrdtBuf::from_vec(encode_ops(&ops))
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Set an integer Register at a path as one undo step. Returns the ops.
///
/// # Safety
/// `undo`/`doc` are live handles; `path`/`path_len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_register_int(
    undo: *mut CrdtUndo,
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    value: i64,
) -> CrdtBuf {
    undo_edit(undo, doc, path, path_len, |u, d, p| {
        u.register(d, p, Scalar::Int(value))
    })
}

/// Increment a Counter at a path as one undo step. Returns the ops.
///
/// # Safety
/// As [`crdtsync_undo_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_inc(
    undo: *mut CrdtUndo,
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    amount: u32,
) -> CrdtBuf {
    undo_edit(undo, doc, path, path_len, |u, d, p| u.inc(d, p, amount))
}

/// Decrement a Counter at a path as one undo step. Returns the ops.
///
/// # Safety
/// As [`crdtsync_undo_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_dec(
    undo: *mut CrdtUndo,
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    amount: u32,
) -> CrdtBuf {
    undo_edit(undo, doc, path, path_len, |u, d, p| u.dec(d, p, amount))
}

/// Tombstone the Register slot at a path as one undo step. Returns the ops.
///
/// # Safety
/// As [`crdtsync_undo_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_delete(
    undo: *mut CrdtUndo,
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
) -> CrdtBuf {
    undo_edit(undo, doc, path, path_len, |u, d, p| u.delete(d, p))
}

/// Insert a bytes item at a live index in the List at a path as one undo step.
///
/// # Safety
/// `undo`/`doc` are live handles; `path`/`path_len` and `value`/`value_len` each
/// follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_list_insert(
    undo: *mut CrdtUndo,
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
    undo_edit(undo, doc, path, path_len, |u, d, p| {
        u.list_insert(d, p, index, val)
    })
}

/// Tombstone the live item at an index in the List at a path as one undo step.
///
/// # Safety
/// As [`crdtsync_undo_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_list_delete(
    undo: *mut CrdtUndo,
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    index: usize,
) -> CrdtBuf {
    undo_edit(undo, doc, path, path_len, |u, d, p| {
        u.list_delete(d, p, index)
    })
}

/// Insert UTF-8 text at a codepoint index in the Text at a path as one undo step.
///
/// # Safety
/// `undo`/`doc` are live handles; `path`/`path_len` and `s`/`s_len` each follow
/// [`as_slice`]. `s` must be valid UTF-8; invalid bytes yield an empty result.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_text_insert(
    undo: *mut CrdtUndo,
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    index: usize,
    s: *const u8,
    s_len: usize,
) -> CrdtBuf {
    let Some(bytes) = as_slice(s, s_len) else {
        return CrdtBuf::empty();
    };
    let Ok(text) = std::str::from_utf8(bytes) else {
        return CrdtBuf::empty();
    };
    undo_edit(undo, doc, path, path_len, |u, d, p| {
        u.text_insert(d, p, index, text)
    })
}

/// Tombstone `count` codepoints from an index in the Text at a path as one undo
/// step. Returns the ops.
///
/// # Safety
/// As [`crdtsync_undo_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_text_delete(
    undo: *mut CrdtUndo,
    doc: *mut CrdtDoc,
    path: *const u8,
    path_len: usize,
    index: usize,
    count: usize,
) -> CrdtBuf {
    undo_edit(undo, doc, path, path_len, |u, d, p| {
        u.text_delete(d, p, index, count)
    })
}

/// Revert the most recent intention, applying it to `doc` and returning the ops
/// to broadcast — empty when there is nothing to undo.
///
/// # Safety
/// `undo`/`doc` are live handles.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_undo(undo: *mut CrdtUndo, doc: *mut CrdtDoc) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if undo.is_null() || doc.is_null() {
            return CrdtBuf::empty();
        }
        let ops = (*undo).undo.undo(&mut (*doc).doc).unwrap_or_default();
        CrdtBuf::from_vec(encode_ops(&ops))
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Replay the most recently undone intention. Returns the ops — empty when there
/// is nothing to redo.
///
/// # Safety
/// `undo`/`doc` are live handles.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_redo(undo: *mut CrdtUndo, doc: *mut CrdtDoc) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if undo.is_null() || doc.is_null() {
            return CrdtBuf::empty();
        }
        let ops = (*undo).undo.redo(&mut (*doc).doc).unwrap_or_default();
        CrdtBuf::from_vec(encode_ops(&ops))
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Whether there is a recorded intention to undo (1), none (0), or a bad handle
/// (-1).
///
/// # Safety
/// `undo` is a live handle.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_can_undo(undo: *const CrdtUndo) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if undo.is_null() {
            return -1;
        }
        i32::from((*undo).undo.can_undo())
    }))
    .unwrap_or(-1)
}

/// Whether there is an undone intention to redo (1), none (0), or a bad handle
/// (-1).
///
/// # Safety
/// `undo` is a live handle.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_undo_can_redo(undo: *const CrdtUndo) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if undo.is_null() {
            return -1;
        }
        i32::from((*undo).undo.can_redo())
    }))
    .unwrap_or(-1)
}

// --- wire client session ---
//
// The sync client on top of the CRDT core: it holds a replica per subscribed
// room and turns local edits into wire messages to send, and folds received
// wire messages back in. Messages cross the boundary as encoded byte buffers
// (the same frames the server speaks); a room is addressed by the `u32` channel
// the client assigns at subscribe.

/// Opaque wire-client handle.
pub struct CrdtClient {
    session: ClientSession,
}

/// Open a wire client for the 16-byte client id at `client`. Null on bad input.
///
/// # Safety
/// `client` must point to 16 readable bytes.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_new(client: *const u8) -> *mut CrdtClient {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return std::ptr::null_mut();
        }
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(slice::from_raw_parts(client, 16));
        Box::into_raw(Box::new(CrdtClient {
            session: ClientSession::new(ClientId::from_bytes(bytes)),
        }))
    }))
    .unwrap_or(std::ptr::null_mut())
}

/// # Safety
/// `client` must be a handle from `crdtsync_client_new`, not yet freed.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_free(client: *mut CrdtClient) {
    if client.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| drop(Box::from_raw(client))));
}

/// Declare the app this client speaks for and the schema version it targets,
/// carried in the next Hello. An empty `app_id` opens a relay connection; a named
/// app with `schema_version` 0 is a dynamic client that adopts the server's head.
/// Returns 1 on success, -1 on a bad handle or input.
///
/// # Safety
/// `client` is a live handle; `app_id`/`app_id_len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_declare_app(
    client: *mut CrdtClient,
    app_id: *const u8,
    app_id_len: usize,
    schema_version: u32,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return -1;
        }
        let Some(app_id) = as_slice(app_id, app_id_len) else {
            return -1;
        };
        (*client).session.declare_app(app_id, schema_version);
        1
    }))
    .unwrap_or(-1)
}

/// Write the concrete schema version the enforcing server advertised for this
/// session into `out`. Returns 1 once an advert has arrived, 0 before it, -1 on
/// a bad handle or output pointer. Distinct from the declared version: a dynamic
/// client (declared 0) learns the served version here. The app persists it
/// across restart itself; the SDK caches, owns no storage.
///
/// # Safety
/// `client` is a live handle; `out` points to a writable `u32`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_active_schema_version(
    client: *const CrdtClient,
    out: *mut u32,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out.is_null() {
            return -1;
        }
        match (*client).session.active_schema_version() {
            Some(version) => {
                *out = version;
                1
            }
            None => 0,
        }
    }))
    .unwrap_or(-1)
}

/// The bytes of the schema the enforcing server advertised for this session into
/// a fresh buffer at `out` the caller frees. Returns 1 once an advert has arrived
/// (its body may be empty), 0 before it, -1 on a bad handle or output pointer.
/// Pairs with [`crdtsync_client_active_schema_version`].
///
/// # Safety
/// `client` is a live handle; `out` points to a writable `CrdtBuf`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_active_schema(
    client: *const CrdtClient,
    out: *mut CrdtBuf,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out.is_null() {
            return -1;
        }
        match (*client).session.active_schema() {
            Some(schema) => {
                *out = CrdtBuf::from_vec(schema.to_vec());
                1
            }
            None => 0,
        }
    }))
    .unwrap_or(-1)
}

/// The opening Hello frame to send, naming this client. Empty on a bad handle.
///
/// # Safety
/// `client` is a live handle.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_hello(client: *const CrdtClient) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return CrdtBuf::empty();
        }
        CrdtBuf::from_vec(encode_message(&(*client).session.hello()))
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Join `room` on a fresh channel, writing the assigned channel to `out_channel`
/// and returning the Subscribe frame to send. Empty on a bad handle or input.
///
/// # Safety
/// `client` is a live handle; `room`/`room_len` follow [`as_slice`];
/// `out_channel` points to a writable `u32`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_subscribe(
    client: *mut CrdtClient,
    room: *const u8,
    room_len: usize,
    out_channel: *mut u32,
) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out_channel.is_null() {
            return CrdtBuf::empty();
        }
        let Some(r) = as_slice(room, room_len) else {
            return CrdtBuf::empty();
        };
        let (channel, msg) = (*client).session.subscribe(r);
        *out_channel = channel.0;
        CrdtBuf::from_vec(encode_message(&msg))
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Join `branch` of `room` on a fresh channel, writing the assigned channel to
/// `out_channel` and returning the Subscribe frame to send. An empty `branch` is
/// the default/active branch, matching [`crdtsync_client_subscribe`]. Empty on a
/// bad handle or input.
///
/// # Safety
/// `client` is a live handle; `room`/`room_len` and `branch`/`branch_len` follow
/// [`as_slice`]; `out_channel` points to a writable `u32`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_subscribe_branch(
    client: *mut CrdtClient,
    room: *const u8,
    room_len: usize,
    branch: *const u8,
    branch_len: usize,
    out_channel: *mut u32,
) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out_channel.is_null() {
            return CrdtBuf::empty();
        }
        let (Some(r), Some(b)) = (as_slice(room, room_len), as_slice(branch, branch_len)) else {
            return CrdtBuf::empty();
        };
        let (channel, msg) = (*client).session.subscribe_branch(r, b);
        *out_channel = channel.0;
        CrdtBuf::from_vec(encode_message(&msg))
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// The stable integer a server [`ErrorCode`] crosses the boundary as, mirroring
/// the wire tags so every SDK decodes it identically: `0` ProtocolViolation, `1`
/// UnsupportedVersion, `2` AuthFailed, `3` UnknownRoom, `4` Internal, `5`
/// Forbidden, `6` UpdateRequired — the `onUpdateRequired` signal.
fn error_code_discriminant(code: ErrorCode) -> i32 {
    match code {
        ErrorCode::ProtocolViolation => 0,
        ErrorCode::UnsupportedVersion => 1,
        ErrorCode::AuthFailed => 2,
        ErrorCode::UnknownRoom => 3,
        ErrorCode::Internal => 4,
        ErrorCode::Forbidden => 5,
        ErrorCode::UpdateRequired => 6,
    }
}

/// Fold one received wire frame into the addressed room. Returns 1 when applied,
/// 0 when the frame is undecodable or the session refuses it, -1 on a bad handle.
/// When the server refused with an `Error` frame, writes the failure's
/// [`error_code_discriminant`] to `out_error_code` (`6` is `UpdateRequired`, the
/// `onUpdateRequired` signal) and returns 0; every other outcome leaves
/// `out_error_code` untouched. A null `out_error_code` skips the write.
///
/// # Safety
/// `client` is a live handle; `msg`/`msg_len` follow [`as_slice`]; `out_error_code`
/// is null or points to a writable `i32`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_receive(
    client: *mut CrdtClient,
    msg: *const u8,
    msg_len: usize,
    out_error_code: *mut i32,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return -1;
        }
        let Some(bytes) = as_slice(msg, msg_len) else {
            return -1;
        };
        let Ok(message) = decode_message(bytes) else {
            return 0;
        };
        match (*client).session.receive(message) {
            Ok(()) => 1,
            Err(ClientError::Server { code, .. }) => {
                if !out_error_code.is_null() {
                    *out_error_code = error_code_discriminant(code);
                }
                0
            }
            Err(_) => 0,
        }
    }))
    .unwrap_or(-1)
}

/// Serialize the refused op batches to one buffer the SDK decodes: a `u32` count,
/// then per batch the channel (`u32`), the reason [`error_code_discriminant`]
/// (`i32`), and the ops — a `u32` op-count then per op a `u32`-length-prefixed
/// `encode_op` byte string. An empty list is a bare zero count, the no-rejection
/// signal.
fn encode_rejected(rejected: &[Rejected]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(rejected.len() as u32).to_le_bytes());
    for r in rejected {
        out.extend_from_slice(&r.channel.0.to_le_bytes());
        out.extend_from_slice(&error_code_discriminant(r.reason).to_le_bytes());
        out.extend_from_slice(&(r.ops.len() as u32).to_le_bytes());
        for op in &r.ops {
            let body = encode_op(op);
            out.extend_from_slice(&(body.len() as u32).to_le_bytes());
            out.extend_from_slice(&body);
        }
    }
    out
}

/// Drain the op batches the server refused since the last call — the
/// `onOpsRejected` observation — into `out`: each batch names its channel, the
/// reason [`error_code_discriminant`] (`5` is `Forbidden`, the auth-revoked
/// rejection), and the refused ops still carrying their bytes so the app can show,
/// discard, or export them. Draining, so a second call reports a bare zero count;
/// empty likewise when no rejection has arrived. Returns 1 with the encoded list,
/// -1 on a bad handle or a null `out`.
///
/// # Safety
/// `client` is a live handle or null; `out` points to a writable `CrdtBuf`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_take_rejected(
    client: *mut CrdtClient,
    out: *mut CrdtBuf,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out.is_null() {
            return -1;
        }
        let rejected = (*client).session.take_rejected();
        *out = CrdtBuf::from_vec(encode_rejected(&rejected));
        1
    }))
    .unwrap_or(-1)
}

/// The highest server sequence `channel`'s room has caught up to, into `out`.
/// Returns 1 on success, 0 if the channel isn't held, -1 on a bad handle.
///
/// # Safety
/// `client` is a live handle; `out` points to a writable `u64`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_last_seen_seq(
    client: *const CrdtClient,
    channel: u32,
    out: *mut u64,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out.is_null() {
            return -1;
        }
        match (*client).session.last_seen_seq(Channel(channel)) {
            Some(seq) => {
                *out = seq;
                1
            }
            None => 0,
        }
    }))
    .unwrap_or(-1)
}

/// Install-or-set an integer Register at a path in `channel`'s room. Returns the
/// Ops frame to send; empty on a bad handle, path, or unheld channel.
///
/// # Safety
/// `client` is a live handle; `path`/`path_len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_register_int(
    client: *mut CrdtClient,
    channel: u32,
    path: *const u8,
    path_len: usize,
    value: i64,
) -> CrdtBuf {
    client_edit(client, channel, path, path_len, |d, p| {
        path::register_int(d, p, value)
    })
}

/// Install-or-increment a Counter at a path in `channel`'s room. Returns the Ops
/// frame to send.
///
/// # Safety
/// As [`crdtsync_client_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_inc(
    client: *mut CrdtClient,
    channel: u32,
    path: *const u8,
    path_len: usize,
    amount: u32,
) -> CrdtBuf {
    client_edit(client, channel, path, path_len, |d, p| {
        path::inc(d, p, amount)
    })
}

/// Install-or-decrement a Counter at a path in `channel`'s room. Returns the Ops
/// frame to send.
///
/// # Safety
/// As [`crdtsync_client_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_dec(
    client: *mut CrdtClient,
    channel: u32,
    path: *const u8,
    path_len: usize,
    amount: u32,
) -> CrdtBuf {
    client_edit(client, channel, path, path_len, |d, p| {
        path::dec(d, p, amount)
    })
}

/// Set a bytes scalar at a path in `channel`'s room. Returns the Ops frame.
///
/// # Safety
/// `client` is a live handle; `path`/`path_len` and `value`/`value_len` each
/// follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_set_bytes(
    client: *mut CrdtClient,
    channel: u32,
    path: *const u8,
    path_len: usize,
    value: *const u8,
    value_len: usize,
) -> CrdtBuf {
    let Some(val) = as_slice(value, value_len) else {
        return CrdtBuf::empty();
    };
    client_edit(client, channel, path, path_len, |d, p| {
        path::set_bytes(d, p, val)
    })
}

/// Tombstone the slot at a path in `channel`'s room. Returns the Ops frame.
///
/// # Safety
/// As [`crdtsync_client_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_delete(
    client: *mut CrdtClient,
    channel: u32,
    path: *const u8,
    path_len: usize,
) -> CrdtBuf {
    client_edit(client, channel, path, path_len, |d, p| path::delete(d, p))
}

// --- client xml navigation ---
//
// The xml edits mirror the doc surface but on a subscribed room's replica, so
// their ops route through the outbox (like every routed edit) and are resent /
// acknowledged rather than framed and forgotten.

/// Install an `XmlElement` tagged `tag` at a path in `channel`'s room. Returns the
/// Ops frame to send; empty on a bad handle, path, tag, or unheld channel.
///
/// # Safety
/// `client` is a live handle; `path`/`path_len` and `tag`/`tag_len` follow
/// [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_xml_element(
    client: *mut CrdtClient,
    channel: u32,
    path: *const u8,
    path_len: usize,
    tag: *const u8,
    tag_len: usize,
) -> CrdtBuf {
    let Some(t) = as_slice(tag, tag_len) else {
        return CrdtBuf::empty();
    };
    client_edit(client, channel, path, path_len, |d, p| {
        path::xml_element(d, p, t)
    })
}

/// Install a tagless `XmlFragment` at a path in `channel`'s room. Returns the Ops
/// frame to send.
///
/// # Safety
/// As [`crdtsync_client_register_int`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_xml_fragment(
    client: *mut CrdtClient,
    channel: u32,
    path: *const u8,
    path_len: usize,
) -> CrdtBuf {
    client_edit(client, channel, path, path_len, |d, p| {
        path::xml_fragment(d, p)
    })
}

/// Insert a nested `XmlElement` child tagged `tag` at live `index` in the children
/// of the element/fragment at `elem_path` in `channel`'s room. Returns the Ops
/// frame; empty on a bad handle, an unheld channel, or a null tag. An insert into
/// a non-node target is inert — the frame it returns carries no ops.
///
/// # Safety
/// `client` is a live handle; `elem_path`/`elem_path_len` and `tag`/`tag_len`
/// follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_xml_insert_element(
    client: *mut CrdtClient,
    channel: u32,
    elem_path: *const u8,
    elem_path_len: usize,
    index: usize,
    tag: *const u8,
    tag_len: usize,
) -> CrdtBuf {
    let Some(t) = as_slice(tag, tag_len) else {
        return CrdtBuf::empty();
    };
    client_edit(client, channel, elem_path, elem_path_len, |d, p| {
        path::xml_insert_element(d, p, index, t)
    })
}

/// Insert a `Text`-run child initialised with UTF-8 `s` at live `index` in the
/// children of the element/fragment at `elem_path` in `channel`'s room. Returns
/// the Ops frame; empty on a bad handle, an unheld channel, or non-UTF-8 input. An
/// insert into a non-node target is inert — the frame it returns carries no ops.
///
/// # Safety
/// `client` is a live handle; `elem_path`/`elem_path_len` and `s`/`s_len` follow
/// [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_xml_insert_text(
    client: *mut CrdtClient,
    channel: u32,
    elem_path: *const u8,
    elem_path_len: usize,
    index: usize,
    s: *const u8,
    s_len: usize,
) -> CrdtBuf {
    let Some(raw) = as_slice(s, s_len) else {
        return CrdtBuf::empty();
    };
    let Ok(text) = std::str::from_utf8(raw) else {
        return CrdtBuf::empty();
    };
    client_edit(client, channel, elem_path, elem_path_len, |d, p| {
        path::xml_insert_text(d, p, index, text)
    })
}

/// Tombstone the child at live `index` in the children of the element/fragment at
/// `elem_path` in `channel`'s room. Returns the Ops frame; empty on a bad handle
/// or an unheld channel. A delete on a non-node target or an `index` naming no
/// live child is inert — the frame it returns carries no ops.
///
/// # Safety
/// As [`crdtsync_client_register_int`], with `elem_path` the parent's path.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_xml_child_delete(
    client: *mut CrdtClient,
    channel: u32,
    elem_path: *const u8,
    elem_path_len: usize,
    index: usize,
) -> CrdtBuf {
    client_edit(client, channel, elem_path, elem_path_len, |d, p| {
        path::xml_child_delete(d, p, index)
    })
}

/// Relocate the live child at `child_index` under the xml node at `parent_path` to
/// `dest_index` in the children of the xml node at `new_parent_path`, in
/// `channel`'s room — the tree move routed through the outbox. Empty on a bad
/// handle or an unheld channel; a move naming a non-node path or a child index
/// naming no live child is inert — the frame it returns carries no ops.
///
/// # Safety
/// `client` is a live handle; `parent_path`/`parent_path_len` and
/// `new_parent_path`/`new_parent_path_len` each follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_xml_move(
    client: *mut CrdtClient,
    channel: u32,
    parent_path: *const u8,
    parent_path_len: usize,
    child_index: usize,
    new_parent_path: *const u8,
    new_parent_path_len: usize,
    dest_index: usize,
) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return CrdtBuf::empty();
        }
        let (Some(pp), Some(np)) = (
            as_slice(parent_path, parent_path_len),
            as_slice(new_parent_path, new_parent_path_len),
        ) else {
            return CrdtBuf::empty();
        };
        let Some(doc) = (*client).session.document_mut(Channel(channel)) else {
            return CrdtBuf::empty();
        };
        let ops = path::xml_move_child(doc, pp, child_index, np, dest_index);
        match (*client).session.enqueue_ops(Channel(channel), ops) {
            Some(msg) => CrdtBuf::from_vec(encode_message(&msg)),
            None => CrdtBuf::empty(),
        }
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

// --- client marks ---
//
// Marks authored on a subscribed room route through the outbox like every other
// client edit, so they are resent and acknowledged rather than framed and
// forgotten. The read (`marks_at`) is a doc-surface entry point.

/// Author a named mark over `[start, end)` of the sequence at `seq_path` in
/// `channel`'s room, routed through the outbox. Endpoints and `value` cross as for
/// [`crdtsync_doc_mark`]; the mark's 16-byte id is written into `out_mark_id` (a
/// fresh buffer the caller frees). Empty on a bad handle, an unheld channel, an
/// unknown `side`, or a malformed value; a non-sequence path enqueues nothing and
/// leaves `out_mark_id` empty.
///
/// # Safety
/// `client` is a live handle; `seq_path`/`seq_path_len`, `name`/`name_len`, and
/// `value`/`value_len` each follow [`as_slice`]; `out_mark_id`, when non-null,
/// points to a writable `CrdtBuf`.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn crdtsync_client_mark(
    client: *mut CrdtClient,
    channel: u32,
    seq_path: *const u8,
    seq_path_len: usize,
    start_index: usize,
    start_side: u32,
    end_index: usize,
    end_side: u32,
    name: *const u8,
    name_len: usize,
    value: *const u8,
    value_len: usize,
    out_mark_id: *mut CrdtBuf,
) -> CrdtBuf {
    let Some(m) = mark_endpoints(start_side, end_side, name, name_len, value, value_len) else {
        return CrdtBuf::empty();
    };
    client_edit(client, channel, seq_path, seq_path_len, |d, p| {
        let (ops, id) = path::mark(
            d,
            p,
            start_index,
            m.start_side,
            end_index,
            m.end_side,
            m.name,
            m.value,
        );
        unsafe { write_mark_id(out_mark_id, id) };
        ops
    })
}

/// Change the payload of the mark handle `mark_id` (16 bytes from
/// [`crdtsync_client_mark`]) to the encoded [`Scalar`] `value`, in `channel`'s
/// room, routed through the outbox. Empty on a bad handle, an unheld channel, a
/// malformed value, or a handle that names no live mark.
///
/// # Safety
/// `client` is a live handle; `mark_id`/`mark_id_len` and `value`/`value_len`
/// follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_mark_set_value(
    client: *mut CrdtClient,
    channel: u32,
    mark_id: *const u8,
    mark_id_len: usize,
    value: *const u8,
    value_len: usize,
) -> CrdtBuf {
    let Some(scalar) = decode_scalar(value, value_len) else {
        return CrdtBuf::empty();
    };
    client_edit(client, channel, mark_id, mark_id_len, |d, id| {
        path::mark_set_value(d, id, scalar)
    })
}

/// Tombstone the mark handle `mark_id` (16 bytes from [`crdtsync_client_mark`]) in
/// `channel`'s room, routed through the outbox. Empty on a bad handle, an unheld
/// channel, or a handle that names no live mark.
///
/// # Safety
/// `client` is a live handle; `mark_id`/`mark_id_len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_mark_delete(
    client: *mut CrdtClient,
    channel: u32,
    mark_id: *const u8,
    mark_id_len: usize,
) -> CrdtBuf {
    client_edit(client, channel, mark_id, mark_id_len, path::mark_delete)
}

/// Read an integer Register at a path in `channel`'s room into `out`. Returns 1
/// on success, 0 if absent or the channel isn't held, -1 on a bad handle.
///
/// # Safety
/// `client` is a live handle; `path`/`path_len` follow [`as_slice`]; `out`
/// points to a writable `i64`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_get_int(
    client: *const CrdtClient,
    channel: u32,
    path: *const u8,
    path_len: usize,
    out: *mut i64,
) -> i32 {
    client_read(
        client,
        channel,
        path,
        path_len,
        out,
        |d, p, o| match path::get_int(d, p) {
            Some(n) => {
                *o = n;
                1
            }
            None => 0,
        },
    )
}

/// Read a bytes scalar at a path in `channel`'s room into a fresh buffer at
/// `out` the caller frees. Returns 1 on success, 0 if absent or the channel
/// isn't held, -1 on a bad handle.
///
/// # Safety
/// `client` is a live handle; `path`/`path_len` follow [`as_slice`]; `out`
/// points to a writable `CrdtBuf`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_get_bytes(
    client: *const CrdtClient,
    channel: u32,
    path: *const u8,
    path_len: usize,
    out: *mut CrdtBuf,
) -> i32 {
    client_read(
        client,
        channel,
        path,
        path_len,
        out,
        |d, p, o| match path::get_bytes(d, p) {
            Some(b) => {
                *o = CrdtBuf::from_vec(b);
                1
            }
            None => 0,
        },
    )
}

/// Begin recording an atomic transaction on `channel`'s room: subsequent edits
/// on the channel accumulate into one group until
/// [`crdtsync_client_commit_atomic`], each returning an empty frame.
///
/// # Safety
/// `client` must be a handle from a constructor and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_begin_atomic(client: *mut CrdtClient, channel: u32) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !client.is_null() {
            (*client).session.begin_atomic(Channel(channel));
        }
    }));
}

/// Commit the atomic transaction opened on `channel` by
/// [`crdtsync_client_begin_atomic`], returning the Ops frame carrying the tagged
/// group. Empty on a bad handle, an unheld channel, or an empty group.
///
/// # Safety
/// `client` must be a handle from a constructor and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_commit_atomic(
    client: *mut CrdtClient,
    channel: u32,
) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return CrdtBuf::empty();
        }
        match (*client).session.commit_atomic(Channel(channel)) {
            Some(msg) => CrdtBuf::from_vec(encode_message(&msg)),
            None => CrdtBuf::empty(),
        }
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Marshal a path-addressed edit on `channel`'s room: run the navigation against
/// the room's replica, wrap the emitted ops in the Ops frame to send, and never
/// let a panic cross the C frame. Empty when the channel isn't held.
unsafe fn client_edit<F>(
    client: *mut CrdtClient,
    channel: u32,
    path: *const u8,
    path_len: usize,
    run: F,
) -> CrdtBuf
where
    F: FnOnce(&mut Document, &[u8]) -> Vec<Op>,
{
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return CrdtBuf::empty();
        }
        let Some(p) = as_slice(path, path_len) else {
            return CrdtBuf::empty();
        };
        let Some(doc) = (*client).session.document_mut(Channel(channel)) else {
            return CrdtBuf::empty();
        };
        let ops = run(doc, p);
        // Route through the session so the ops enter the outbox and are resent /
        // acknowledged like a closure edit, not just framed and forgotten.
        match (*client).session.enqueue_ops(Channel(channel), ops) {
            Some(msg) => CrdtBuf::from_vec(encode_message(&msg)),
            None => CrdtBuf::empty(),
        }
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Read a slot on `channel`'s room through `run`, which writes into `out` and
/// returns the status code. -1 on a bad handle or output pointer.
unsafe fn client_read<T, F>(
    client: *const CrdtClient,
    channel: u32,
    path: *const u8,
    path_len: usize,
    out: *mut T,
    run: F,
) -> i32
where
    F: FnOnce(&Document, &[u8], *mut T) -> i32,
{
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out.is_null() {
            return -1;
        }
        let Some(p) = as_slice(path, path_len) else {
            return 0;
        };
        match (*client).session.document(Channel(channel)) {
            Some(doc) => run(doc, p, out),
            None => 0,
        }
    }))
    .unwrap_or(-1)
}

// --- client auth ---

/// Present an opaque credential; the returned Auth frame asks the server to
/// verify it and derive the actor. Empty on a bad handle or input.
///
/// # Safety
/// `client` is a live handle; `cred`/`cred_len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_auth(
    client: *const CrdtClient,
    cred: *const u8,
    cred_len: usize,
) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return CrdtBuf::empty();
        }
        let Some(credential) = as_slice(cred, cred_len) else {
            return CrdtBuf::empty();
        };
        CrdtBuf::from_vec(encode_message(&(*client).session.auth(credential)))
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// The server-derived actor for this session into a fresh buffer at `out`.
/// Returns 1 once AuthOk has arrived, 0 before, -1 on a bad handle.
///
/// # Safety
/// `client` is a live handle; `out` points to a writable `CrdtBuf`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_actor(
    client: *const CrdtClient,
    out: *mut CrdtBuf,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out.is_null() {
            return -1;
        }
        match (*client).session.actor() {
            Some(actor) => {
                *out = CrdtBuf::from_vec(actor.to_vec());
                1
            }
            None => 0,
        }
    }))
    .unwrap_or(-1)
}

// --- client subscription lifecycle ---

/// Re-issue the Subscribe for a held channel from its caught-up position, so a
/// reconnect resumes with a delta. Empty on a bad handle or unheld channel.
///
/// # Safety
/// `client` is a live handle.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_resume(
    client: *const CrdtClient,
    channel: u32,
) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return CrdtBuf::empty();
        }
        match (*client).session.resume(Channel(channel)) {
            Some(msg) => CrdtBuf::from_vec(encode_message(&msg)),
            None => CrdtBuf::empty(),
        }
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// Re-emit the authored ops on `channel` the server has not yet acknowledged,
/// as one Ops frame to replay after a reconnect. Empty on a bad handle, an
/// unheld channel, or nothing outstanding.
///
/// # Safety
/// `client` is a live handle.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_resend(
    client: *const CrdtClient,
    channel: u32,
) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return CrdtBuf::empty();
        }
        match (*client).session.resend(Channel(channel)) {
            Some(msg) => CrdtBuf::from_vec(encode_message(&msg)),
            None => CrdtBuf::empty(),
        }
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// How many authored ops on `channel` await acknowledgement — the offline queue
/// depth — into `out`. Returns 1 on success, -1 on a bad handle (an unheld
/// channel reports 0).
///
/// # Safety
/// `client` is a live handle; `out` points to a writable `usize`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_outbox_len(
    client: *const CrdtClient,
    channel: u32,
    out: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out.is_null() {
            return -1;
        }
        *out = (*client).session.outbox_len(Channel(channel));
        1
    }))
    .unwrap_or(-1)
}

/// Leave the room on `channel`, dropping its replica; returns the Unsubscribe
/// frame to send. Empty on a bad handle or unheld channel.
///
/// # Safety
/// `client` is a live handle.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_unsubscribe(
    client: *mut CrdtClient,
    channel: u32,
) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return CrdtBuf::empty();
        }
        match (*client).session.unsubscribe(Channel(channel)) {
            Some(msg) => CrdtBuf::from_vec(encode_message(&msg)),
            None => CrdtBuf::empty(),
        }
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

// --- client awareness ---

/// Publish an ephemeral awareness entry `key` on `channel`'s room; returns the
/// frame to send. Empty on a bad handle, input, or unheld channel.
///
/// # Safety
/// `client` is a live handle; `key`/`key_len` and `value`/`value_len` each follow
/// [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_set_awareness(
    client: *const CrdtClient,
    channel: u32,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> CrdtBuf {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return CrdtBuf::empty();
        }
        let (Some(k), Some(v)) = (as_slice(key, key_len), as_slice(value, value_len)) else {
            return CrdtBuf::empty();
        };
        match (*client).session.set_awareness(Channel(channel), k, v) {
            Some(msg) => CrdtBuf::from_vec(encode_message(&msg)),
            None => CrdtBuf::empty(),
        }
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// A peer's awareness entry on `channel` — by publishing `actor` and `key` — into
/// a fresh buffer at `out`. Returns 1 if present, 0 if absent or the channel
/// isn't held, -1 on a bad handle.
///
/// # Safety
/// `client` is a live handle; `actor`/`actor_len` and `key`/`key_len` each follow
/// [`as_slice`]; `out` points to a writable `CrdtBuf`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_awareness(
    client: *const CrdtClient,
    channel: u32,
    actor: *const u8,
    actor_len: usize,
    key: *const u8,
    key_len: usize,
    out: *mut CrdtBuf,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out.is_null() {
            return -1;
        }
        let (Some(a), Some(k)) = (as_slice(actor, actor_len), as_slice(key, key_len)) else {
            return 0;
        };
        match (*client).session.awareness(Channel(channel), a, k) {
            Some(value) => {
                *out = CrdtBuf::from_vec(value.to_vec());
                1
            }
            None => 0,
        }
    }))
    .unwrap_or(-1)
}

/// How many awareness entries `channel` currently holds, into `out`. Returns 1
/// on success, -1 on a bad handle (an unheld channel reports 0 entries).
///
/// # Safety
/// `client` is a live handle; `out` points to a writable `usize`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_awareness_len(
    client: *const CrdtClient,
    channel: u32,
    out: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out.is_null() {
            return -1;
        }
        *out = (*client).session.awareness_len(Channel(channel));
        1
    }))
    .unwrap_or(-1)
}

// --- client named versions ---

/// Frame a request to capture `channel`'s room as version `name`; returns the
/// frame to send. Empty on a bad handle, input, or unheld channel.
///
/// # Safety
/// `client` is a live handle; `name`/`name_len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_create_version(
    client: *const CrdtClient,
    channel: u32,
    name: *const u8,
    name_len: usize,
) -> CrdtBuf {
    version_frame(client, |s| {
        as_slice(name, name_len).and_then(|n| s.create_version(Channel(channel), n))
    })
}

/// Frame a request to rename version `from` to `to` on `channel`'s room. Empty on
/// a bad handle, input, or unheld channel.
///
/// # Safety
/// `client` is a live handle; `from`/`from_len` and `to`/`to_len` follow
/// [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_rename_version(
    client: *const CrdtClient,
    channel: u32,
    from: *const u8,
    from_len: usize,
    to: *const u8,
    to_len: usize,
) -> CrdtBuf {
    version_frame(client, |s| {
        match (as_slice(from, from_len), as_slice(to, to_len)) {
            (Some(f), Some(t)) => s.rename_version(Channel(channel), f, t),
            _ => None,
        }
    })
}

/// Frame a request to delete version `name` on `channel`'s room. Empty on a bad
/// handle, input, or unheld channel.
///
/// # Safety
/// `client` is a live handle; `name`/`name_len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_delete_version(
    client: *const CrdtClient,
    channel: u32,
    name: *const u8,
    name_len: usize,
) -> CrdtBuf {
    version_frame(client, |s| {
        as_slice(name, name_len).and_then(|n| s.delete_version(Channel(channel), n))
    })
}

/// Frame a request for the version names of `channel`'s room. Empty on a bad
/// handle or unheld channel.
///
/// # Safety
/// `client` is a live handle.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_list_versions(
    client: *const CrdtClient,
    channel: u32,
) -> CrdtBuf {
    version_frame(client, |s| s.list_versions(Channel(channel)))
}

/// Frame a request for the captured state of version `name` on `channel`'s room.
/// Empty on a bad handle, input, or unheld channel.
///
/// # Safety
/// `client` is a live handle; `name`/`name_len` follow [`as_slice`].
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_fetch_version(
    client: *const CrdtClient,
    channel: u32,
    name: *const u8,
    name_len: usize,
) -> CrdtBuf {
    version_frame(client, |s| {
        as_slice(name, name_len).and_then(|n| s.fetch_version(Channel(channel), n))
    })
}

/// Marshal a version request `frame` produces from the session into the wire
/// frame to send, never letting a panic cross the C frame. Empty when `frame`
/// yields nothing (a bad input or unheld channel).
unsafe fn version_frame<F>(client: *const CrdtClient, frame: F) -> CrdtBuf
where
    F: FnOnce(&ClientSession) -> Option<Message>,
{
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return CrdtBuf::empty();
        }
        match frame(&(*client).session) {
            Some(msg) => CrdtBuf::from_vec(encode_message(&msg)),
            None => CrdtBuf::empty(),
        }
    }))
    .unwrap_or_else(|_| CrdtBuf::empty())
}

/// How many version names `channel`'s room currently holds in the client view,
/// into `out`. Returns 1 on success, -1 on a bad handle (an unheld channel
/// reports 0).
///
/// # Safety
/// `client` is a live handle; `out` points to a writable `usize`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_version_count(
    client: *const CrdtClient,
    channel: u32,
    out: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out.is_null() {
            return -1;
        }
        *out = (*client)
            .session
            .versions(Channel(channel))
            .map_or(0, <[Vec<u8>]>::len);
        1
    }))
    .unwrap_or(-1)
}

/// The version name at `index` in `channel`'s view into a fresh buffer at `out`.
/// Returns 1 if present, 0 if out of range or the channel isn't held, -1 on a bad
/// handle.
///
/// # Safety
/// `client` is a live handle; `out` points to a writable `CrdtBuf`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_version_name(
    client: *const CrdtClient,
    channel: u32,
    index: usize,
    out: *mut CrdtBuf,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out.is_null() {
            return -1;
        }
        match (*client)
            .session
            .versions(Channel(channel))
            .and_then(|names| names.get(index))
        {
            Some(name) => {
                *out = CrdtBuf::from_vec(name.clone());
                1
            }
            None => 0,
        }
    }))
    .unwrap_or(-1)
}

/// The captured state of fetched version `name` on `channel` into a fresh buffer
/// at `out`. Returns 1 if present, 0 if not fetched or the channel isn't held, -1
/// on a bad handle.
///
/// # Safety
/// `client` is a live handle; `name`/`name_len` follow [`as_slice`]; `out` points
/// to a writable `CrdtBuf`.
#[no_mangle]
pub unsafe extern "C" fn crdtsync_client_version_state(
    client: *const CrdtClient,
    channel: u32,
    name: *const u8,
    name_len: usize,
    out: *mut CrdtBuf,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() || out.is_null() {
            return -1;
        }
        let Some(n) = as_slice(name, name_len) else {
            return 0;
        };
        match (*client).session.version_state(Channel(channel), n) {
            Some(state) => {
                *out = CrdtBuf::from_vec(state.to_vec());
                1
            }
            None => 0,
        }
    }))
    .unwrap_or(-1)
}
