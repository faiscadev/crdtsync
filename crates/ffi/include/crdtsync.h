/* crdtsync C ABI — generated from the Rust source by build.rs (cbindgen). Do not edit by hand. */

#ifndef CRDTSYNC_H
#define CRDTSYNC_H

#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// Opaque document handle.
typedef struct CrdtDoc CrdtDoc;

// Owned byte buffer handed to the caller, released by [`crdtsync_buf_free`].
typedef struct {
    uint8_t *ptr;
    uintptr_t len;
} CrdtBuf;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

// Open a document for the 16-byte client id at `client`. Null on a bad handle.
//
// # Safety
// `client` must point to 16 readable bytes.
CrdtDoc *crdtsync_doc_new(const uint8_t *client);

// # Safety
// `doc` must be a handle returned by `crdtsync_doc_new` and not yet freed.
void crdtsync_doc_free(CrdtDoc *doc);

// # Safety
// `buf` must be a buffer produced by the core and not yet freed.
void crdtsync_buf_free(CrdtBuf buf);

// Install-or-set an integer Register at a path. Returns the ops to broadcast;
// empty on a bad handle or path.
//
// # Safety
// `doc` is a live handle; `path`/`path_len` follow [`as_slice`].
CrdtBuf crdtsync_doc_register_int(CrdtDoc *doc,
                                  const uint8_t *path,
                                  uintptr_t path_len,
                                  int64_t value);

// Install-or-increment a Counter at a path. Returns the ops to broadcast.
//
// # Safety
// As [`crdtsync_doc_register_int`].
CrdtBuf crdtsync_doc_inc(CrdtDoc *doc, const uint8_t *path, uintptr_t path_len, uint32_t amount);

// Set a bytes scalar at a path. Returns the ops to broadcast.
//
// # Safety
// `doc` is a live handle; `path`/`path_len` and `value`/`value_len` each follow
// [`as_slice`].
CrdtBuf crdtsync_doc_set_bytes(CrdtDoc *doc,
                               const uint8_t *path,
                               uintptr_t path_len,
                               const uint8_t *value,
                               uintptr_t value_len);

// Tombstone the slot at a path. Returns the ops to broadcast.
//
// # Safety
// As [`crdtsync_doc_register_int`].
CrdtBuf crdtsync_doc_delete(CrdtDoc *doc, const uint8_t *path, uintptr_t path_len);

// Read an integer Register at a path into `out`. Returns 1 when found and an
// integer, 0 when absent or another type, -1 on a bad handle.
//
// # Safety
// `doc` is a live handle or null; `path`/`path_len` follow [`as_slice`]; `out`
// points to a writable `i64`.
int32_t crdtsync_doc_get_int(const CrdtDoc *doc,
                             const uint8_t *path,
                             uintptr_t path_len,
                             int64_t *out);

// Read a Counter's value at a path into `out`. Returns 1/0/-1 as
// [`crdtsync_doc_get_int`].
//
// # Safety
// As [`crdtsync_doc_get_int`].
int32_t crdtsync_doc_get_counter(const CrdtDoc *doc,
                                 const uint8_t *path,
                                 uintptr_t path_len,
                                 int64_t *out);

// Read a bytes scalar at a path into `out` (a fresh buffer the caller frees).
// Returns 1 when found, 0 when absent or another type, -1 on a bad handle.
//
// # Safety
// `doc` is a live handle or null; `path`/`path_len` follow [`as_slice`]; `out`
// points to a writable `CrdtBuf`.
int32_t crdtsync_doc_get_bytes(const CrdtDoc *doc,
                               const uint8_t *path,
                               uintptr_t path_len,
                               CrdtBuf *out);

// Insert a bytes item into the List at a path, at live `index`. Returns the ops
// to broadcast.
//
// # Safety
// `doc` is a live handle; `path`/`path_len` and `value`/`value_len` follow
// [`as_slice`].
CrdtBuf crdtsync_doc_list_insert(CrdtDoc *doc,
                                 const uint8_t *path,
                                 uintptr_t path_len,
                                 uintptr_t index,
                                 const uint8_t *value,
                                 uintptr_t value_len);

// Tombstone the live item at `index` in the List at a path.
//
// # Safety
// As [`crdtsync_doc_register_int`].
CrdtBuf crdtsync_doc_list_delete(CrdtDoc *doc,
                                 const uint8_t *path,
                                 uintptr_t path_len,
                                 uintptr_t index);

// Read the live length of the List at a path into `out`. Returns 1/0/-1.
//
// # Safety
// As [`crdtsync_doc_get_int`], with `out` a writable `usize`.
int32_t crdtsync_doc_list_len(const CrdtDoc *doc,
                              const uint8_t *path,
                              uintptr_t path_len,
                              uintptr_t *out);

// Read the bytes item at live `index` in the List at a path into `out`. Returns
// 1 when present and a bytes item, 0 otherwise, -1 on a bad handle.
//
// # Safety
// As [`crdtsync_doc_get_bytes`].
int32_t crdtsync_doc_list_get(const CrdtDoc *doc,
                              const uint8_t *path,
                              uintptr_t path_len,
                              uintptr_t index,
                              CrdtBuf *out);

// Insert UTF-8 `text` into the Text at a path, at codepoint `index`. Returns the
// ops to broadcast; empty on a bad handle/path or non-UTF-8 input.
//
// # Safety
// `doc` is a live handle; `path`/`path_len` and `text`/`text_len` follow
// [`as_slice`].
CrdtBuf crdtsync_doc_text_insert(CrdtDoc *doc,
                                 const uint8_t *path,
                                 uintptr_t path_len,
                                 uintptr_t index,
                                 const uint8_t *text,
                                 uintptr_t text_len);

// Tombstone `count` codepoints from codepoint `index` in the Text at a path.
//
// # Safety
// As [`crdtsync_doc_register_int`].
CrdtBuf crdtsync_doc_text_delete(CrdtDoc *doc,
                                 const uint8_t *path,
                                 uintptr_t path_len,
                                 uintptr_t index,
                                 uintptr_t count);

// Read the codepoint length of the Text at a path into `out`. Returns 1/0/-1.
//
// # Safety
// As [`crdtsync_doc_list_len`].
int32_t crdtsync_doc_text_len(const CrdtDoc *doc,
                              const uint8_t *path,
                              uintptr_t path_len,
                              uintptr_t *out);

// Read the Text at a path into `out` as UTF-8 bytes. Returns 1/0/-1.
//
// # Safety
// As [`crdtsync_doc_get_bytes`].
int32_t crdtsync_doc_text_get(const CrdtDoc *doc,
                              const uint8_t *path,
                              uintptr_t path_len,
                              CrdtBuf *out);

// Fold an encoded op log (as returned by an edit) from a peer into the
// document. Returns the number of ops applied now (a duplicate or one buffered
// pending its target counts as not-applied), or -1 on a bad handle or
// malformed bytes.
//
// # Safety
// `doc` is a live handle or null; `bytes`/`len` follow [`as_slice`].
int32_t crdtsync_doc_apply(CrdtDoc *doc, const uint8_t *bytes, uintptr_t len);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* CRDTSYNC_H */
