// Package crdtsync provides Go bindings over the CRDT core's C ABI.
//
// A Document is a local replica. A slot is addressed by a path: a slice of
// []byte keys naming nested maps, the last key the slot itself. An edit applies
// locally and returns the encoded ops to broadcast; Apply folds a peer's ops
// back in. Two documents that exchange those bytes converge.
//
// cgo links the core's static library from target/release, so build it first:
//
//	cargo build -p crdtsync-ffi --release
package crdtsync

/*
#cgo CFLAGS: -I${SRCDIR}/../../../crates/ffi/include
#cgo LDFLAGS: ${SRCDIR}/../../../target/release/libcrdtsync_ffi.a
#cgo darwin LDFLAGS: -lm
#cgo linux LDFLAGS: -lm -ldl -lpthread
#include <stdlib.h>
#include "crdtsync.h"
*/
import "C"

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

// Document is a CRDT replica for one 16-byte client id.
type Document struct {
	h *C.CrdtDoc
}

// New opens a document for the given 16-byte client id.
func New(clientID []byte) (*Document, error) {
	if len(clientID) != 16 {
		return nil, errors.New("client id must be 16 bytes")
	}
	h := C.crdtsync_doc_new((*C.uint8_t)(unsafe.Pointer(&clientID[0])))
	if h == nil {
		return nil, errors.New("failed to open document")
	}
	return &Document{h: h}, nil
}

// DecodeState opens a document from a snapshot produced by EncodeState.
func DecodeState(state []byte) (*Document, error) {
	sp, sl := bytesArg(state)
	h := C.crdtsync_doc_decode_state(sp, sl)
	if h == nil {
		return nil, errors.New("failed to decode document snapshot")
	}
	return &Document{h: h}, nil
}

// Close frees the document. Safe to call more than once.
func (d *Document) Close() {
	if d.h != nil {
		C.crdtsync_doc_free(d.h)
		d.h = nil
	}
}

// EncodeState serializes the whole replica to a canonical snapshot.
func (d *Document) EncodeState() []byte {
	return takeBuf(C.crdtsync_doc_encode_state(d.h))
}

// EncodePath encodes a path as the ABI expects: each key a little-endian u32
// length followed by its bytes.
func EncodePath(keys [][]byte) []byte {
	var buf []byte
	var hdr [4]byte
	for _, k := range keys {
		if uint64(len(k)) > uint64(^uint32(0)) {
			panic("crdtsync: path key length exceeds uint32")
		}
		binary.LittleEndian.PutUint32(hdr[:], uint32(len(k)))
		buf = append(buf, hdr[:]...)
		buf = append(buf, k...)
	}
	return buf
}

// bytesArg yields a C pointer + length for a Go slice; nil for the empty slice.
// The pointer is only read during the call (the core copies), which cgo allows.
func bytesArg(b []byte) (*C.uint8_t, C.uintptr_t) {
	if len(b) == 0 {
		return nil, 0
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0])), C.uintptr_t(len(b))
}

// takeBuf copies an owned buffer out and frees it.
func takeBuf(b C.CrdtBuf) []byte {
	if b.ptr == nil {
		return nil
	}
	// Copy through a Go-sized slice so a buffer larger than a C int can't be
	// truncated; free the FFI buffer regardless.
	src := unsafe.Slice((*byte)(unsafe.Pointer(b.ptr)), int(b.len))
	out := make([]byte, len(src))
	copy(out, src)
	C.crdtsync_buf_free(b)
	return out
}

// --- map / scalar ---

// RegisterInt installs-or-sets an integer Register at path. Returns the ops to
// broadcast.
func (d *Document) RegisterInt(path [][]byte, value int64) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_doc_register_int(d.h, pp, pl, C.int64_t(value)))
}

// Inc installs-or-increments a Counter at path.
func (d *Document) Inc(path [][]byte, amount uint32) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_doc_inc(d.h, pp, pl, C.uint32_t(amount)))
}

// SetBytes sets a bytes scalar at path.
func (d *Document) SetBytes(path [][]byte, value []byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	vp, vl := bytesArg(value)
	return takeBuf(C.crdtsync_doc_set_bytes(d.h, pp, pl, vp, vl))
}

// Delete tombstones the slot at path.
func (d *Document) Delete(path [][]byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_doc_delete(d.h, pp, pl))
}

// GetInt reads an integer Register at path.
func (d *Document) GetInt(path [][]byte) (int64, bool) {
	pp, pl := bytesArg(EncodePath(path))
	var out C.int64_t
	rc := C.crdtsync_doc_get_int(d.h, pp, pl, &out)
	return int64(out), rc == 1
}

// GetCounter reads a Counter's value at path.
func (d *Document) GetCounter(path [][]byte) (int64, bool) {
	pp, pl := bytesArg(EncodePath(path))
	var out C.int64_t
	rc := C.crdtsync_doc_get_counter(d.h, pp, pl, &out)
	return int64(out), rc == 1
}

// GetBytes reads a bytes scalar at path.
func (d *Document) GetBytes(path [][]byte) ([]byte, bool) {
	pp, pl := bytesArg(EncodePath(path))
	var out C.CrdtBuf
	rc := C.crdtsync_doc_get_bytes(d.h, pp, pl, &out)
	if rc != 1 {
		return nil, false
	}
	return takeBuf(out), true
}

// --- list ---

// ListInsert inserts a bytes item at live index in the List at path.
func (d *Document) ListInsert(path [][]byte, index uint, value []byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	vp, vl := bytesArg(value)
	return takeBuf(C.crdtsync_doc_list_insert(d.h, pp, pl, C.uintptr_t(index), vp, vl))
}

// ListDelete tombstones the live item at index in the List at path.
func (d *Document) ListDelete(path [][]byte, index uint) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_doc_list_delete(d.h, pp, pl, C.uintptr_t(index)))
}

// ListLen reads the live length of the List at path.
func (d *Document) ListLen(path [][]byte) (uint, bool) {
	pp, pl := bytesArg(EncodePath(path))
	var out C.uintptr_t
	rc := C.crdtsync_doc_list_len(d.h, pp, pl, &out)
	return uint(out), rc == 1
}

// ListGet reads the bytes item at live index in the List at path.
func (d *Document) ListGet(path [][]byte, index uint) ([]byte, bool) {
	pp, pl := bytesArg(EncodePath(path))
	var out C.CrdtBuf
	rc := C.crdtsync_doc_list_get(d.h, pp, pl, C.uintptr_t(index), &out)
	if rc != 1 {
		return nil, false
	}
	return takeBuf(out), true
}

// --- text ---

// TextInsert inserts text at codepoint index in the Text at path.
func (d *Document) TextInsert(path [][]byte, index uint, text string) []byte {
	pp, pl := bytesArg(EncodePath(path))
	tp, tl := bytesArg([]byte(text))
	return takeBuf(C.crdtsync_doc_text_insert(d.h, pp, pl, C.uintptr_t(index), tp, tl))
}

// TextDelete tombstones count codepoints from index in the Text at path.
func (d *Document) TextDelete(path [][]byte, index, count uint) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_doc_text_delete(d.h, pp, pl, C.uintptr_t(index), C.uintptr_t(count)))
}

// TextLen reads the codepoint length of the Text at path.
func (d *Document) TextLen(path [][]byte) (uint, bool) {
	pp, pl := bytesArg(EncodePath(path))
	var out C.uintptr_t
	rc := C.crdtsync_doc_text_len(d.h, pp, pl, &out)
	return uint(out), rc == 1
}

// TextGet reads the Text at path as a string.
func (d *Document) TextGet(path [][]byte) (string, bool) {
	pp, pl := bytesArg(EncodePath(path))
	var out C.CrdtBuf
	rc := C.crdtsync_doc_text_get(d.h, pp, pl, &out)
	if rc != 1 {
		return "", false
	}
	return string(takeBuf(out)), true
}

// --- sync ---

// Apply folds a peer's encoded ops in. Returns the number applied, -1 on error.
func (d *Document) Apply(ops []byte) int {
	pp, pl := bytesArg(ops)
	return int(C.crdtsync_doc_apply(d.h, pp, pl))
}

// --- wire client session ---

// Client is a wire client session for one 16-byte client id. It holds a replica
// per subscribed room and turns local edits into wire frames to send; Receive
// folds a peer's frame back in. A room is addressed by the channel Subscribe
// returns.
type Client struct {
	h *C.CrdtClient
}

// NewClient opens a wire client for the given 16-byte client id.
func NewClient(clientID []byte) (*Client, error) {
	if len(clientID) != 16 {
		return nil, errors.New("client id must be 16 bytes")
	}
	h := C.crdtsync_client_new((*C.uint8_t)(unsafe.Pointer(&clientID[0])))
	if h == nil {
		return nil, errors.New("failed to open client")
	}
	return &Client{h: h}, nil
}

// Close frees the client. Safe to call more than once.
func (c *Client) Close() {
	if c.h != nil {
		C.crdtsync_client_free(c.h)
		c.h = nil
	}
}

// Hello is the opening frame to send, naming this client.
func (c *Client) Hello() []byte {
	return takeBuf(C.crdtsync_client_hello(c.h))
}

// Auth is the frame asking the server to verify credential and derive the actor.
func (c *Client) Auth(credential []byte) []byte {
	cp, cl := bytesArg(credential)
	return takeBuf(C.crdtsync_client_auth(c.h, cp, cl))
}

// Actor is the server-derived actor, present once AuthOk has been received.
func (c *Client) Actor() ([]byte, bool) {
	var out C.CrdtBuf
	rc := C.crdtsync_client_actor(c.h, &out)
	if rc != 1 {
		return nil, false
	}
	return takeBuf(out), true
}

// Subscribe joins room on a fresh channel; returns the channel and the frame.
func (c *Client) Subscribe(room []byte) (uint32, []byte) {
	rp, rl := bytesArg(room)
	var channel C.uint32_t
	frame := takeBuf(C.crdtsync_client_subscribe(c.h, rp, rl, &channel))
	return uint32(channel), frame
}

// Resume re-issues Subscribe for a held channel from its caught-up position.
func (c *Client) Resume(channel uint32) []byte {
	return takeBuf(C.crdtsync_client_resume(c.h, C.uint32_t(channel)))
}

// Unsubscribe leaves the room on channel, dropping its replica.
func (c *Client) Unsubscribe(channel uint32) []byte {
	return takeBuf(C.crdtsync_client_unsubscribe(c.h, C.uint32_t(channel)))
}

// Receive folds one received wire frame in. 1 applied, 0 refused, -1 bad handle.
func (c *Client) Receive(msg []byte) int {
	mp, ml := bytesArg(msg)
	return int(C.crdtsync_client_receive(c.h, mp, ml))
}

// LastSeenSeq is the highest server sequence channel has caught up to.
func (c *Client) LastSeenSeq(channel uint32) (uint64, bool) {
	var out C.uint64_t
	rc := C.crdtsync_client_last_seen_seq(c.h, C.uint32_t(channel), &out)
	return uint64(out), rc == 1
}

// RegisterInt installs-or-sets an integer Register in channel's room.
func (c *Client) RegisterInt(channel uint32, path [][]byte, value int64) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_client_register_int(c.h, C.uint32_t(channel), pp, pl, C.int64_t(value)))
}

// Inc installs-or-increments a Counter in channel's room.
func (c *Client) Inc(channel uint32, path [][]byte, amount uint32) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_client_inc(c.h, C.uint32_t(channel), pp, pl, C.uint32_t(amount)))
}

// SetBytes sets a bytes scalar in channel's room.
func (c *Client) SetBytes(channel uint32, path [][]byte, value []byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	vp, vl := bytesArg(value)
	return takeBuf(C.crdtsync_client_set_bytes(c.h, C.uint32_t(channel), pp, pl, vp, vl))
}

// Delete tombstones the slot at path in channel's room.
func (c *Client) Delete(channel uint32, path [][]byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_client_delete(c.h, C.uint32_t(channel), pp, pl))
}

// GetInt reads an integer Register at path in channel's room.
func (c *Client) GetInt(channel uint32, path [][]byte) (int64, bool) {
	pp, pl := bytesArg(EncodePath(path))
	var out C.int64_t
	rc := C.crdtsync_client_get_int(c.h, C.uint32_t(channel), pp, pl, &out)
	return int64(out), rc == 1
}

// GetBytes reads a bytes scalar at path in channel's room.
func (c *Client) GetBytes(channel uint32, path [][]byte) ([]byte, bool) {
	pp, pl := bytesArg(EncodePath(path))
	var out C.CrdtBuf
	rc := C.crdtsync_client_get_bytes(c.h, C.uint32_t(channel), pp, pl, &out)
	if rc != 1 {
		return nil, false
	}
	return takeBuf(out), true
}

// SetAwareness publishes an ephemeral awareness entry key in channel's room.
func (c *Client) SetAwareness(channel uint32, key, value []byte) []byte {
	kp, kl := bytesArg(key)
	vp, vl := bytesArg(value)
	return takeBuf(C.crdtsync_client_set_awareness(c.h, C.uint32_t(channel), kp, kl, vp, vl))
}

// Awareness reads a peer's awareness entry on channel by publishing actor and key.
func (c *Client) Awareness(channel uint32, actor, key []byte) ([]byte, bool) {
	ap, al := bytesArg(actor)
	kp, kl := bytesArg(key)
	var out C.CrdtBuf
	rc := C.crdtsync_client_awareness(c.h, C.uint32_t(channel), ap, al, kp, kl, &out)
	if rc != 1 {
		return nil, false
	}
	return takeBuf(out), true
}

// AwarenessLen reports how many awareness entries channel currently holds.
func (c *Client) AwarenessLen(channel uint32) uint {
	var out C.uintptr_t
	rc := C.crdtsync_client_awareness_len(c.h, C.uint32_t(channel), &out)
	if rc != 1 {
		return 0
	}
	return uint(out)
}

// CreateVersion frames a request to capture channel's room as version name.
func (c *Client) CreateVersion(channel uint32, name []byte) []byte {
	np, nl := bytesArg(name)
	return takeBuf(C.crdtsync_client_create_version(c.h, C.uint32_t(channel), np, nl))
}

// RenameVersion frames a request to rename version from to to.
func (c *Client) RenameVersion(channel uint32, from, to []byte) []byte {
	fp, fl := bytesArg(from)
	tp, tl := bytesArg(to)
	return takeBuf(C.crdtsync_client_rename_version(c.h, C.uint32_t(channel), fp, fl, tp, tl))
}

// DeleteVersion frames a request to delete version name.
func (c *Client) DeleteVersion(channel uint32, name []byte) []byte {
	np, nl := bytesArg(name)
	return takeBuf(C.crdtsync_client_delete_version(c.h, C.uint32_t(channel), np, nl))
}

// ListVersions frames a request for channel's room's version names.
func (c *Client) ListVersions(channel uint32) []byte {
	return takeBuf(C.crdtsync_client_list_versions(c.h, C.uint32_t(channel)))
}

// FetchVersion frames a request for the captured state of version name.
func (c *Client) FetchVersion(channel uint32, name []byte) []byte {
	np, nl := bytesArg(name)
	return takeBuf(C.crdtsync_client_fetch_version(c.h, C.uint32_t(channel), np, nl))
}

// Versions returns the version names last reported for channel's room, in order.
func (c *Client) Versions(channel uint32) [][]byte {
	var count C.uintptr_t
	if C.crdtsync_client_version_count(c.h, C.uint32_t(channel), &count) != 1 {
		return nil
	}
	names := make([][]byte, 0, int(count))
	for i := 0; i < int(count); i++ {
		var out C.CrdtBuf
		if C.crdtsync_client_version_name(c.h, C.uint32_t(channel), C.uintptr_t(i), &out) == 1 {
			names = append(names, takeBuf(out))
		}
	}
	return names
}

// VersionState returns the captured state of a fetched version name, if present.
func (c *Client) VersionState(channel uint32, name []byte) ([]byte, bool) {
	np, nl := bytesArg(name)
	var out C.CrdtBuf
	rc := C.crdtsync_client_version_state(c.h, C.uint32_t(channel), np, nl, &out)
	if rc != 1 {
		return nil, false
	}
	return takeBuf(out), true
}
