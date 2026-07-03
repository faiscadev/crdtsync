/* crdtsync C ABI — generated from the Rust source by build.rs (cbindgen). Do not edit by hand. */

#ifndef CRDTSYNC_H
#define CRDTSYNC_H

#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// Opaque wire-client handle.
typedef struct CrdtClient CrdtClient;

// Opaque document handle.
typedef struct CrdtDoc CrdtDoc;

// Opaque undo-manager handle.
typedef struct CrdtUndo CrdtUndo;

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

// Install-or-decrement a Counter at a path. Returns the ops to broadcast.
//
// # Safety
// As [`crdtsync_doc_register_int`].
CrdtBuf crdtsync_doc_dec(CrdtDoc *doc, const uint8_t *path, uintptr_t path_len, uint32_t amount);

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

// Capture a stable position in the List or Text at a path — the encoded
// [`RelativePosition`] bytes, resolved later with
// [`crdtsync_doc_resolve_position`]. `side` is 0 (left of `index`) or 1 (right).
// Empty on a bad handle/path, a non-sequence slot, or an unknown `side`.
//
// # Safety
// `doc` is a live handle or null; `path`/`path_len` follow [`as_slice`].
CrdtBuf crdtsync_doc_relative_position(const CrdtDoc *doc,
                                       const uint8_t *path,
                                       uintptr_t path_len,
                                       uintptr_t index,
                                       uint32_t side);

// Resolve a captured position (bytes from [`crdtsync_doc_relative_position`])
// back to a live index in the List or Text at a path, written to `out`. Returns
// 1 when resolved, 0 on a bad path / non-sequence slot / malformed position
// bytes, -1 on a bad handle or panic.
//
// # Safety
// `doc` is a live handle or null; `path`/`path_len` and `pos`/`pos_len` follow
// [`as_slice`]; `out` is a writable `usize`.
int32_t crdtsync_doc_resolve_position(const CrdtDoc *doc,
                                      const uint8_t *path,
                                      uintptr_t path_len,
                                      const uint8_t *pos,
                                      uintptr_t pos_len,
                                      uintptr_t *out);

// Fold an encoded op log (as returned by an edit) from a peer into the
// document. Returns the number of ops applied now (a duplicate or one buffered
// pending its target counts as not-applied), or -1 on a bad handle or
// malformed bytes.
//
// # Safety
// `doc` is a live handle or null; `bytes`/`len` follow [`as_slice`].
int32_t crdtsync_doc_apply(CrdtDoc *doc, const uint8_t *bytes, uintptr_t len);

// Begin recording an atomic transaction: until [`crdtsync_doc_commit_atomic`],
// edits accumulate into one group and each returns an empty ops buffer.
//
// # Safety
// `doc` must be a handle returned by a constructor and not yet freed.
void crdtsync_doc_begin_atomic(CrdtDoc *doc);

// Commit the atomic transaction opened by [`crdtsync_doc_begin_atomic`],
// returning the group's ops tagged for all-or-nothing delivery. Empty on a bad
// handle, no open transaction, or an empty group.
//
// # Safety
// `doc` must be a handle returned by a constructor and not yet freed.
CrdtBuf crdtsync_doc_commit_atomic(CrdtDoc *doc);

// Serialize the whole replica to a canonical snapshot. Empty on a bad handle.
//
// # Safety
// `doc` must be a handle returned by a constructor and not yet freed.
CrdtBuf crdtsync_doc_encode_state(const CrdtDoc *doc);

// Open a document from a snapshot produced by [`crdtsync_doc_encode_state`].
// Null on a malformed snapshot or bad input, never a panic across the frame.
//
// # Safety
// `bytes`/`len` follow [`as_slice`].
CrdtDoc *crdtsync_doc_decode_state(const uint8_t *bytes, uintptr_t len);

// Diff two snapshots (each a state buffer from [`crdtsync_doc_encode_state`],
// a named version, or an exported room) into the encoded change list — the
// structural changes turning the old state into the new. Decode it with the
// SDK's change-list reader. Empty on malformed input or a bad snapshot, never
// a panic across the frame.
//
// # Safety
// `old`/`old_len` and `new`/`new_len` each follow [`as_slice`].
CrdtBuf crdtsync_diff(const uint8_t *old,
                      uintptr_t old_len,
                      const uint8_t *new_,
                      uintptr_t new_len);

// Open an undo manager. It drives whichever document is passed to each call.
//
// # Safety
// The returned handle is freed with [`crdtsync_undo_free`].
CrdtUndo *crdtsync_undo_new(void);

// # Safety
// `undo` must be a handle from `crdtsync_undo_new`, not yet freed.
void crdtsync_undo_free(CrdtUndo *undo);

// Set an integer Register at a path as one undo step. Returns the ops.
//
// # Safety
// `undo`/`doc` are live handles; `path`/`path_len` follow [`as_slice`].
CrdtBuf crdtsync_undo_register_int(CrdtUndo *undo,
                                   CrdtDoc *doc,
                                   const uint8_t *path,
                                   uintptr_t path_len,
                                   int64_t value);

// Increment a Counter at a path as one undo step. Returns the ops.
//
// # Safety
// As [`crdtsync_undo_register_int`].
CrdtBuf crdtsync_undo_inc(CrdtUndo *undo,
                          CrdtDoc *doc,
                          const uint8_t *path,
                          uintptr_t path_len,
                          uint32_t amount);

// Decrement a Counter at a path as one undo step. Returns the ops.
//
// # Safety
// As [`crdtsync_undo_register_int`].
CrdtBuf crdtsync_undo_dec(CrdtUndo *undo,
                          CrdtDoc *doc,
                          const uint8_t *path,
                          uintptr_t path_len,
                          uint32_t amount);

// Tombstone the Register slot at a path as one undo step. Returns the ops.
//
// # Safety
// As [`crdtsync_undo_register_int`].
CrdtBuf crdtsync_undo_delete(CrdtUndo *undo, CrdtDoc *doc, const uint8_t *path, uintptr_t path_len);

// Insert a bytes item at a live index in the List at a path as one undo step.
//
// # Safety
// `undo`/`doc` are live handles; `path`/`path_len` and `value`/`value_len` each
// follow [`as_slice`].
CrdtBuf crdtsync_undo_list_insert(CrdtUndo *undo,
                                  CrdtDoc *doc,
                                  const uint8_t *path,
                                  uintptr_t path_len,
                                  uintptr_t index,
                                  const uint8_t *value,
                                  uintptr_t value_len);

// Tombstone the live item at an index in the List at a path as one undo step.
//
// # Safety
// As [`crdtsync_undo_register_int`].
CrdtBuf crdtsync_undo_list_delete(CrdtUndo *undo,
                                  CrdtDoc *doc,
                                  const uint8_t *path,
                                  uintptr_t path_len,
                                  uintptr_t index);

// Insert UTF-8 text at a codepoint index in the Text at a path as one undo step.
//
// # Safety
// `undo`/`doc` are live handles; `path`/`path_len` and `s`/`s_len` each follow
// [`as_slice`]. `s` must be valid UTF-8; invalid bytes yield an empty result.
CrdtBuf crdtsync_undo_text_insert(CrdtUndo *undo,
                                  CrdtDoc *doc,
                                  const uint8_t *path,
                                  uintptr_t path_len,
                                  uintptr_t index,
                                  const uint8_t *s,
                                  uintptr_t s_len);

// Tombstone `count` codepoints from an index in the Text at a path as one undo
// step. Returns the ops.
//
// # Safety
// As [`crdtsync_undo_register_int`].
CrdtBuf crdtsync_undo_text_delete(CrdtUndo *undo,
                                  CrdtDoc *doc,
                                  const uint8_t *path,
                                  uintptr_t path_len,
                                  uintptr_t index,
                                  uintptr_t count);

// Revert the most recent intention, applying it to `doc` and returning the ops
// to broadcast — empty when there is nothing to undo.
//
// # Safety
// `undo`/`doc` are live handles.
CrdtBuf crdtsync_undo_undo(CrdtUndo *undo, CrdtDoc *doc);

// Replay the most recently undone intention. Returns the ops — empty when there
// is nothing to redo.
//
// # Safety
// `undo`/`doc` are live handles.
CrdtBuf crdtsync_undo_redo(CrdtUndo *undo, CrdtDoc *doc);

// Whether there is a recorded intention to undo (1), none (0), or a bad handle
// (-1).
//
// # Safety
// `undo` is a live handle.
int32_t crdtsync_undo_can_undo(const CrdtUndo *undo);

// Whether there is an undone intention to redo (1), none (0), or a bad handle
// (-1).
//
// # Safety
// `undo` is a live handle.
int32_t crdtsync_undo_can_redo(const CrdtUndo *undo);

// Open a wire client for the 16-byte client id at `client`. Null on bad input.
//
// # Safety
// `client` must point to 16 readable bytes.
CrdtClient *crdtsync_client_new(const uint8_t *client);

// # Safety
// `client` must be a handle from `crdtsync_client_new`, not yet freed.
void crdtsync_client_free(CrdtClient *client);

// The opening Hello frame to send, naming this client. Empty on a bad handle.
//
// # Safety
// `client` is a live handle.
CrdtBuf crdtsync_client_hello(const CrdtClient *client);

// Join `room` on a fresh channel, writing the assigned channel to `out_channel`
// and returning the Subscribe frame to send. Empty on a bad handle or input.
//
// # Safety
// `client` is a live handle; `room`/`room_len` follow [`as_slice`];
// `out_channel` points to a writable `u32`.
CrdtBuf crdtsync_client_subscribe(CrdtClient *client,
                                  const uint8_t *room,
                                  uintptr_t room_len,
                                  uint32_t *out_channel);

// Fold one received wire frame into the addressed room. Returns 1 when applied,
// 0 when the frame is undecodable or the session refuses it, -1 on a bad handle.
//
// # Safety
// `client` is a live handle; `msg`/`msg_len` follow [`as_slice`].
int32_t crdtsync_client_receive(CrdtClient *client, const uint8_t *msg, uintptr_t msg_len);

// The highest server sequence `channel`'s room has caught up to, into `out`.
// Returns 1 on success, 0 if the channel isn't held, -1 on a bad handle.
//
// # Safety
// `client` is a live handle; `out` points to a writable `u64`.
int32_t crdtsync_client_last_seen_seq(const CrdtClient *client, uint32_t channel, uint64_t *out);

// Install-or-set an integer Register at a path in `channel`'s room. Returns the
// Ops frame to send; empty on a bad handle, path, or unheld channel.
//
// # Safety
// `client` is a live handle; `path`/`path_len` follow [`as_slice`].
CrdtBuf crdtsync_client_register_int(CrdtClient *client,
                                     uint32_t channel,
                                     const uint8_t *path,
                                     uintptr_t path_len,
                                     int64_t value);

// Install-or-increment a Counter at a path in `channel`'s room. Returns the Ops
// frame to send.
//
// # Safety
// As [`crdtsync_client_register_int`].
CrdtBuf crdtsync_client_inc(CrdtClient *client,
                            uint32_t channel,
                            const uint8_t *path,
                            uintptr_t path_len,
                            uint32_t amount);

// Install-or-decrement a Counter at a path in `channel`'s room. Returns the Ops
// frame to send.
//
// # Safety
// As [`crdtsync_client_register_int`].
CrdtBuf crdtsync_client_dec(CrdtClient *client,
                            uint32_t channel,
                            const uint8_t *path,
                            uintptr_t path_len,
                            uint32_t amount);

// Set a bytes scalar at a path in `channel`'s room. Returns the Ops frame.
//
// # Safety
// `client` is a live handle; `path`/`path_len` and `value`/`value_len` each
// follow [`as_slice`].
CrdtBuf crdtsync_client_set_bytes(CrdtClient *client,
                                  uint32_t channel,
                                  const uint8_t *path,
                                  uintptr_t path_len,
                                  const uint8_t *value,
                                  uintptr_t value_len);

// Tombstone the slot at a path in `channel`'s room. Returns the Ops frame.
//
// # Safety
// As [`crdtsync_client_register_int`].
CrdtBuf crdtsync_client_delete(CrdtClient *client,
                               uint32_t channel,
                               const uint8_t *path,
                               uintptr_t path_len);

// Read an integer Register at a path in `channel`'s room into `out`. Returns 1
// on success, 0 if absent or the channel isn't held, -1 on a bad handle.
//
// # Safety
// `client` is a live handle; `path`/`path_len` follow [`as_slice`]; `out`
// points to a writable `i64`.
int32_t crdtsync_client_get_int(const CrdtClient *client,
                                uint32_t channel,
                                const uint8_t *path,
                                uintptr_t path_len,
                                int64_t *out);

// Read a bytes scalar at a path in `channel`'s room into a fresh buffer at
// `out` the caller frees. Returns 1 on success, 0 if absent or the channel
// isn't held, -1 on a bad handle.
//
// # Safety
// `client` is a live handle; `path`/`path_len` follow [`as_slice`]; `out`
// points to a writable `CrdtBuf`.
int32_t crdtsync_client_get_bytes(const CrdtClient *client,
                                  uint32_t channel,
                                  const uint8_t *path,
                                  uintptr_t path_len,
                                  CrdtBuf *out);

// Begin recording an atomic transaction on `channel`'s room: subsequent edits
// on the channel accumulate into one group until
// [`crdtsync_client_commit_atomic`], each returning an empty frame.
//
// # Safety
// `client` must be a handle from a constructor and not yet freed.
void crdtsync_client_begin_atomic(CrdtClient *client, uint32_t channel);

// Commit the atomic transaction opened on `channel` by
// [`crdtsync_client_begin_atomic`], returning the Ops frame carrying the tagged
// group. Empty on a bad handle, an unheld channel, or an empty group.
//
// # Safety
// `client` must be a handle from a constructor and not yet freed.
CrdtBuf crdtsync_client_commit_atomic(CrdtClient *client, uint32_t channel);

// Present an opaque credential; the returned Auth frame asks the server to
// verify it and derive the actor. Empty on a bad handle or input.
//
// # Safety
// `client` is a live handle; `cred`/`cred_len` follow [`as_slice`].
CrdtBuf crdtsync_client_auth(const CrdtClient *client, const uint8_t *cred, uintptr_t cred_len);

// The server-derived actor for this session into a fresh buffer at `out`.
// Returns 1 once AuthOk has arrived, 0 before, -1 on a bad handle.
//
// # Safety
// `client` is a live handle; `out` points to a writable `CrdtBuf`.
int32_t crdtsync_client_actor(const CrdtClient *client, CrdtBuf *out);

// Re-issue the Subscribe for a held channel from its caught-up position, so a
// reconnect resumes with a delta. Empty on a bad handle or unheld channel.
//
// # Safety
// `client` is a live handle.
CrdtBuf crdtsync_client_resume(const CrdtClient *client, uint32_t channel);

// Re-emit the authored ops on `channel` the server has not yet acknowledged,
// as one Ops frame to replay after a reconnect. Empty on a bad handle, an
// unheld channel, or nothing outstanding.
//
// # Safety
// `client` is a live handle.
CrdtBuf crdtsync_client_resend(const CrdtClient *client, uint32_t channel);

// How many authored ops on `channel` await acknowledgement — the offline queue
// depth — into `out`. Returns 1 on success, -1 on a bad handle (an unheld
// channel reports 0).
//
// # Safety
// `client` is a live handle; `out` points to a writable `usize`.
int32_t crdtsync_client_outbox_len(const CrdtClient *client, uint32_t channel, uintptr_t *out);

// Leave the room on `channel`, dropping its replica; returns the Unsubscribe
// frame to send. Empty on a bad handle or unheld channel.
//
// # Safety
// `client` is a live handle.
CrdtBuf crdtsync_client_unsubscribe(CrdtClient *client, uint32_t channel);

// Publish an ephemeral awareness entry `key` on `channel`'s room; returns the
// frame to send. Empty on a bad handle, input, or unheld channel.
//
// # Safety
// `client` is a live handle; `key`/`key_len` and `value`/`value_len` each follow
// [`as_slice`].
CrdtBuf crdtsync_client_set_awareness(const CrdtClient *client,
                                      uint32_t channel,
                                      const uint8_t *key,
                                      uintptr_t key_len,
                                      const uint8_t *value,
                                      uintptr_t value_len);

// A peer's awareness entry on `channel` — by publishing `actor` and `key` — into
// a fresh buffer at `out`. Returns 1 if present, 0 if absent or the channel
// isn't held, -1 on a bad handle.
//
// # Safety
// `client` is a live handle; `actor`/`actor_len` and `key`/`key_len` each follow
// [`as_slice`]; `out` points to a writable `CrdtBuf`.
int32_t crdtsync_client_awareness(const CrdtClient *client,
                                  uint32_t channel,
                                  const uint8_t *actor,
                                  uintptr_t actor_len,
                                  const uint8_t *key,
                                  uintptr_t key_len,
                                  CrdtBuf *out);

// How many awareness entries `channel` currently holds, into `out`. Returns 1
// on success, -1 on a bad handle (an unheld channel reports 0 entries).
//
// # Safety
// `client` is a live handle; `out` points to a writable `usize`.
int32_t crdtsync_client_awareness_len(const CrdtClient *client, uint32_t channel, uintptr_t *out);

// Frame a request to capture `channel`'s room as version `name`; returns the
// frame to send. Empty on a bad handle, input, or unheld channel.
//
// # Safety
// `client` is a live handle; `name`/`name_len` follow [`as_slice`].
CrdtBuf crdtsync_client_create_version(const CrdtClient *client,
                                       uint32_t channel,
                                       const uint8_t *name,
                                       uintptr_t name_len);

// Frame a request to rename version `from` to `to` on `channel`'s room. Empty on
// a bad handle, input, or unheld channel.
//
// # Safety
// `client` is a live handle; `from`/`from_len` and `to`/`to_len` follow
// [`as_slice`].
CrdtBuf crdtsync_client_rename_version(const CrdtClient *client,
                                       uint32_t channel,
                                       const uint8_t *from,
                                       uintptr_t from_len,
                                       const uint8_t *to,
                                       uintptr_t to_len);

// Frame a request to delete version `name` on `channel`'s room. Empty on a bad
// handle, input, or unheld channel.
//
// # Safety
// `client` is a live handle; `name`/`name_len` follow [`as_slice`].
CrdtBuf crdtsync_client_delete_version(const CrdtClient *client,
                                       uint32_t channel,
                                       const uint8_t *name,
                                       uintptr_t name_len);

// Frame a request for the version names of `channel`'s room. Empty on a bad
// handle or unheld channel.
//
// # Safety
// `client` is a live handle.
CrdtBuf crdtsync_client_list_versions(const CrdtClient *client, uint32_t channel);

// Frame a request for the captured state of version `name` on `channel`'s room.
// Empty on a bad handle, input, or unheld channel.
//
// # Safety
// `client` is a live handle; `name`/`name_len` follow [`as_slice`].
CrdtBuf crdtsync_client_fetch_version(const CrdtClient *client,
                                      uint32_t channel,
                                      const uint8_t *name,
                                      uintptr_t name_len);

// How many version names `channel`'s room currently holds in the client view,
// into `out`. Returns 1 on success, -1 on a bad handle (an unheld channel
// reports 0).
//
// # Safety
// `client` is a live handle; `out` points to a writable `usize`.
int32_t crdtsync_client_version_count(const CrdtClient *client, uint32_t channel, uintptr_t *out);

// The version name at `index` in `channel`'s view into a fresh buffer at `out`.
// Returns 1 if present, 0 if out of range or the channel isn't held, -1 on a bad
// handle.
//
// # Safety
// `client` is a live handle; `out` points to a writable `CrdtBuf`.
int32_t crdtsync_client_version_name(const CrdtClient *client,
                                     uint32_t channel,
                                     uintptr_t index,
                                     CrdtBuf *out);

// The captured state of fetched version `name` on `channel` into a fresh buffer
// at `out`. Returns 1 if present, 0 if not fetched or the channel isn't held, -1
// on a bad handle.
//
// # Safety
// `client` is a live handle; `name`/`name_len` follow [`as_slice`]; `out` points
// to a writable `CrdtBuf`.
int32_t crdtsync_client_version_state(const CrdtClient *client,
                                      uint32_t channel,
                                      const uint8_t *name,
                                      uintptr_t name_len,
                                      CrdtBuf *out);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* CRDTSYNC_H */
