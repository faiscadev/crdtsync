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

// Close frees the document. Safe to call more than once.
func (d *Document) Close() {
	if d.h != nil {
		C.crdtsync_doc_free(d.h)
		d.h = nil
	}
}

// EncodePath encodes a path as the ABI expects: each key a little-endian u32
// length followed by its bytes.
func EncodePath(keys [][]byte) []byte {
	var buf []byte
	var hdr [4]byte
	for _, k := range keys {
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
	out := C.GoBytes(unsafe.Pointer(b.ptr), C.int(b.len))
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
