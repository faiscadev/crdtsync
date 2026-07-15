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

// Dec installs-or-decrements a Counter at path.
func (d *Document) Dec(path [][]byte, amount uint32) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_doc_dec(d.h, pp, pl, C.uint32_t(amount)))
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

// --- blobs ---

// BlobRef is a reference to out-of-band binary content read back by GetBlob. ID
// is the 16-byte public handle, Mime the content type, Size the byte length.
// Inline carries the bytes for a small blob that rides in the ref, and is nil
// for a store-backed ref fetched by ID.
type BlobRef struct {
	ID     [16]byte
	Mime   string
	Size   uint64
	Inline []byte
}

// idArg yields a C pointer to a 16-byte id. The core reads exactly 16 bytes.
func idArg(id *[16]byte) *C.uint8_t {
	return (*C.uint8_t)(unsafe.Pointer(&id[0]))
}

// SetBlob sets an inline blob at path, minting the blob's public handle. Returns
// the ops to broadcast and true when the blob was inlined; nil and false when
// bytes exceed the inline ceiling — a large blob is uploaded out of band and set
// with SetBlobRef.
func (d *Document) SetBlob(path [][]byte, mime string, bytes []byte) ([]byte, bool) {
	pp, pl := bytesArg(EncodePath(path))
	mp, ml := bytesArg([]byte(mime))
	bp, bl := bytesArg(bytes)
	var out C.CrdtBuf
	rc := C.crdtsync_doc_set_blob(d.h, pp, pl, mp, ml, bp, bl, &out)
	if rc != 1 {
		return nil, false
	}
	return takeBuf(out), true
}

// SetBlobRef sets a store-backed blob ref at path from a 16-byte id handle, mime,
// and size. Carries no bytes; the content is fetched by id. Returns the ops to
// broadcast.
func (d *Document) SetBlobRef(path [][]byte, id [16]byte, mime string, size uint64) []byte {
	pp, pl := bytesArg(EncodePath(path))
	mp, ml := bytesArg([]byte(mime))
	var out C.CrdtBuf
	rc := C.crdtsync_doc_set_blob_ref(d.h, pp, pl, idArg(&id), mp, ml, C.uint64_t(size), &out)
	if rc != 1 {
		return nil
	}
	return takeBuf(out)
}

// GetBlob reads the BlobRef at path. The bool is false when the slot is absent or
// holds no blob ref.
func (d *Document) GetBlob(path [][]byte) (BlobRef, bool) {
	pp, pl := bytesArg(EncodePath(path))
	var out C.CrdtBuf
	if C.crdtsync_doc_get_blob(d.h, pp, pl, &out) != 1 {
		return BlobRef{}, false
	}
	return decodeBlobRef(takeBuf(out)), true
}

// decodeBlobRef decodes the get_blob buffer: the 16-byte id, a u32-length mime,
// the u64 size, then a present flag and, when set, the u32-length inline bytes.
func decodeBlobRef(b []byte) BlobRef {
	var ref BlobRef
	copy(ref.ID[:], b[:16])
	i := 16
	mimeLen := int(binary.LittleEndian.Uint32(b[i:]))
	i += 4
	ref.Mime = string(b[i : i+mimeLen])
	i += mimeLen
	ref.Size = binary.LittleEndian.Uint64(b[i:])
	i += 8
	present := b[i]
	i++
	if present == 1 {
		n := int(binary.LittleEndian.Uint32(b[i:]))
		i += 4
		ref.Inline = append([]byte(nil), b[i:i+n]...)
	}
	return ref
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

// --- relative positions (anchors) ---

// Side selects which edge of an index a captured position anchors to.
type Side uint32

const (
	// Left anchors to the left of the index.
	Left Side = 0
	// Right anchors to the right of the index.
	Right Side = 1
)

// SubjectKind names who a doc-ACL grant targets: an actor id, a group name, or a
// well-known class.
type SubjectKind uint32

const (
	// SubjectActor targets a 16-byte actor id.
	SubjectActor SubjectKind = 0
	// SubjectGroup targets a membership name.
	SubjectGroup SubjectKind = 1
	// SubjectAuthenticated targets every authenticated connection.
	SubjectAuthenticated SubjectKind = 2
	// SubjectAnonymous targets every unauthenticated connection.
	SubjectAnonymous SubjectKind = 3
	// SubjectAnyone targets everyone.
	SubjectAnyone SubjectKind = 4
)

// Capability is a direct power a grant confers over a subtree.
type Capability uint32

const (
	CapRead             Capability = 0
	CapWrite            Capability = 1
	CapPublishAwareness Capability = 2
	CapOwn              Capability = 3
)

// Effect is whether a grant allows or denies.
type Effect uint32

const (
	Allow Effect = 0
	Deny  Effect = 1
)

// Grant is what an ACL tuple confers: a direct Capability or a named role. Build
// one with CapabilityGrant or RoleGrant.
type Grant struct {
	kind uint32
	cap  Capability
	role []byte
}

// CapabilityGrant confers the direct capability c.
func CapabilityGrant(c Capability) Grant { return Grant{kind: 0, cap: c} }

// RoleGrant confers the schema-declared role named name.
func RoleGrant(name []byte) Grant { return Grant{kind: 1, role: name} }

// ErrorCode is a failure the server reports to the client through Receive.
type ErrorCode int32

const (
	// NoErrorCode marks a Receive that carried no server Error.
	NoErrorCode        ErrorCode = -1
	ProtocolViolation  ErrorCode = 0
	UnsupportedVersion ErrorCode = 1
	AuthFailed         ErrorCode = 2
	UnknownRoom        ErrorCode = 3
	Internal           ErrorCode = 4
	Forbidden          ErrorCode = 5
	// UpdateRequired is the onUpdateRequired signal: the client's version can't
	// bridge the room's across a breaking gap, so the app prompts an update or
	// falls back read-only.
	UpdateRequired ErrorCode = 6
	// NotFound is a diff query over a version or branch the room does not have —
	// recoverable, the connection stays open.
	NotFound ErrorCode = 7
)

// Rejected is an op batch the server refused, surfaced by TakeRejected for the
// app to show, discard, or export. Channel names the room, Reason the ErrorCode
// the server reported (Forbidden for auth revoked), and Ops the refused ops still
// carrying their bytes.
type Rejected struct {
	Channel uint32
	Reason  ErrorCode
	Ops     [][]byte
}

// Redirect is a room the server redirected to its leader, surfaced by
// TakeRedirects. A node that does not lead Room reports the leader's advertise
// address LeaderAddr so the transport reconnects there; the core holds no socket,
// so reconnecting is the app's job.
type Redirect struct {
	Room       []byte
	LeaderAddr []byte
}

// RelativePosition captures a stable position in the List or Text at path — the
// encoded bytes to resolve later with ResolvePosition. Nil for a bad handle or
// path, a non-sequence slot, or an unknown side.
func (d *Document) RelativePosition(path [][]byte, index uint, side Side) []byte {
	pp, pl := bytesArg(EncodePath(path))
	b := takeBuf(C.crdtsync_doc_relative_position(d.h, pp, pl, C.uintptr_t(index), C.uint32_t(side)))
	if len(b) == 0 {
		return nil
	}
	return b
}

// ResolvePosition resolves a captured position back to a live index in the List
// or Text at path. The bool is false for a bad handle or path, a non-sequence
// slot, or malformed position bytes.
func (d *Document) ResolvePosition(path [][]byte, pos []byte) (uint, bool) {
	pp, pl := bytesArg(EncodePath(path))
	qp, ql := bytesArg(pos)
	var out C.uintptr_t
	rc := C.crdtsync_doc_resolve_position(d.h, pp, pl, qp, ql, &out)
	return uint(out), rc == 1
}

// --- sync ---

// Apply folds a peer's encoded ops in. Returns the number applied, -1 on error.
func (d *Document) Apply(ops []byte) int {
	pp, pl := bytesArg(ops)
	return int(C.crdtsync_doc_apply(d.h, pp, pl))
}

// BeginAtomic starts recording an atomic transaction; edits accumulate until
// CommitAtomic.
func (d *Document) BeginAtomic() {
	C.crdtsync_doc_begin_atomic(d.h)
}

// CommitAtomic commits the atomic transaction, returning the group's ops to
// broadcast.
func (d *Document) CommitAtomic() []byte {
	return takeBuf(C.crdtsync_doc_commit_atomic(d.h))
}

// --- undo / redo ---

// Undo is a per-user undo/redo manager over a Document. Each edit made through
// it records its inverse; Undo and Redo emit ordinary ops that converge on peers
// like any edit. The manager is separate from the document it drives, so every
// call names the document.
type Undo struct {
	h *C.CrdtUndo
}

// NewUndo opens an undo manager.
func NewUndo() (*Undo, error) {
	h := C.crdtsync_undo_new()
	if h == nil {
		return nil, errors.New("failed to open undo manager")
	}
	return &Undo{h: h}, nil
}

// Close frees the manager. Safe to call more than once.
func (u *Undo) Close() {
	if u.h != nil {
		C.crdtsync_undo_free(u.h)
		u.h = nil
	}
}

// RegisterInt sets an integer Register at path as one undo step.
func (u *Undo) RegisterInt(doc *Document, path [][]byte, value int64) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_undo_register_int(u.h, doc.h, pp, pl, C.int64_t(value)))
}

// Inc increments a Counter at path as one undo step.
func (u *Undo) Inc(doc *Document, path [][]byte, amount uint32) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_undo_inc(u.h, doc.h, pp, pl, C.uint32_t(amount)))
}

// Dec decrements a Counter at path as one undo step.
func (u *Undo) Dec(doc *Document, path [][]byte, amount uint32) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_undo_dec(u.h, doc.h, pp, pl, C.uint32_t(amount)))
}

// Delete tombstones the Register slot at path as one undo step.
func (u *Undo) Delete(doc *Document, path [][]byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_undo_delete(u.h, doc.h, pp, pl))
}

// ListInsert inserts a bytes item at live index in the List at path as one undo
// step.
func (u *Undo) ListInsert(doc *Document, path [][]byte, index uint, value []byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	vp, vl := bytesArg(value)
	return takeBuf(C.crdtsync_undo_list_insert(u.h, doc.h, pp, pl, C.uintptr_t(index), vp, vl))
}

// ListDelete tombstones the live item at index in the List at path as one undo
// step.
func (u *Undo) ListDelete(doc *Document, path [][]byte, index uint) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_undo_list_delete(u.h, doc.h, pp, pl, C.uintptr_t(index)))
}

// TextInsert inserts UTF-8 text at a codepoint index in the Text at path as one
// undo step.
func (u *Undo) TextInsert(doc *Document, path [][]byte, index uint, s string) []byte {
	pp, pl := bytesArg(EncodePath(path))
	sp, sl := bytesArg([]byte(s))
	return takeBuf(C.crdtsync_undo_text_insert(u.h, doc.h, pp, pl, C.uintptr_t(index), sp, sl))
}

// TextDelete tombstones count codepoints from index in the Text at path as one
// undo step.
func (u *Undo) TextDelete(doc *Document, path [][]byte, index, count uint) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_undo_text_delete(u.h, doc.h, pp, pl, C.uintptr_t(index), C.uintptr_t(count)))
}

// Undo reverts the most recent intention; returns the ops (empty if none).
func (u *Undo) Undo(doc *Document) []byte {
	return takeBuf(C.crdtsync_undo_undo(u.h, doc.h))
}

// Redo replays the most recently undone intention; returns the ops (empty if none).
func (u *Undo) Redo(doc *Document) []byte {
	return takeBuf(C.crdtsync_undo_redo(u.h, doc.h))
}

// CanUndo reports whether there is a recorded intention to undo.
func (u *Undo) CanUndo() bool {
	return C.crdtsync_undo_can_undo(u.h) == 1
}

// CanRedo reports whether there is an undone intention to redo.
func (u *Undo) CanRedo() bool {
	return C.crdtsync_undo_can_redo(u.h) == 1
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

// DeclareApp declares the app this client speaks for and the schema version it
// targets, carried in the next Hello. An empty appID opens a relay connection; a
// named app with schemaVersion 0 is a dynamic client that adopts the server's
// head. Call before Hello.
func (c *Client) DeclareApp(appID []byte, schemaVersion uint32) {
	ap, al := bytesArg(appID)
	C.crdtsync_client_declare_app(c.h, ap, al, C.uint32_t(schemaVersion))
}

// ActiveSchemaVersion is the concrete schema version the enforcing server
// advertised for this session, present once a SchemaAdvert has been received.
// It is distinct from the version declared in DeclareApp: a dynamic client
// (declared 0) learns the served version here. The app persists it across
// restart itself; the SDK caches but owns no storage.
func (c *Client) ActiveSchemaVersion() (uint32, bool) {
	var out C.uint32_t
	if C.crdtsync_client_active_schema_version(c.h, &out) != 1 {
		return 0, false
	}
	return uint32(out), true
}

// ActiveSchema is the bytes of the schema the enforcing server advertised for
// this session (possibly empty), present once a SchemaAdvert has been received.
// Pairs with ActiveSchemaVersion.
func (c *Client) ActiveSchema() ([]byte, bool) {
	var out C.CrdtBuf
	if C.crdtsync_client_active_schema(c.h, &out) != 1 {
		return nil, false
	}
	return takeBuf(out), true
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

// SubscribeBranch joins branch of room on a fresh channel; returns the channel
// and the frame. An empty branch is the default/active branch, as Subscribe.
func (c *Client) SubscribeBranch(room, branch []byte) (uint32, []byte) {
	rp, rl := bytesArg(room)
	bp, bl := bytesArg(branch)
	var channel C.uint32_t
	frame := takeBuf(C.crdtsync_client_subscribe_branch(c.h, rp, rl, bp, bl, &channel))
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

// Resend re-emits the authored ops on channel the server has not yet
// acknowledged, as one Ops frame to replay after a reconnect. Empty when
// nothing is outstanding.
func (c *Client) Resend(channel uint32) []byte {
	return takeBuf(C.crdtsync_client_resend(c.h, C.uint32_t(channel)))
}

// OutboxLen reports how many authored ops on channel await acknowledgement —
// the offline queue depth.
func (c *Client) OutboxLen(channel uint32) uint {
	var out C.uintptr_t
	rc := C.crdtsync_client_outbox_len(c.h, C.uint32_t(channel), &out)
	if rc != 1 {
		return 0
	}
	return uint(out)
}

// Receive folds one received wire frame in, returning the apply status (1
// applied, 0 refused, -1 bad handle) and the server's ErrorCode. The code is
// NoErrorCode unless the frame was a server Error: then the status is 0 and the
// code is what the server reported — UpdateRequired being the onUpdateRequired
// signal.
func (c *Client) Receive(msg []byte) (int, ErrorCode) {
	mp, ml := bytesArg(msg)
	code := C.int32_t(NoErrorCode)
	rc := int(C.crdtsync_client_receive(c.h, mp, ml, &code))
	return rc, ErrorCode(code)
}

// TakeRejected drains the op batches the server refused since the last call — the
// onOpsRejected observation. Each Rejected names the channel, the reason, and the
// refused ops still carrying their bytes. Draining, so a second call is empty.
func (c *Client) TakeRejected() []Rejected {
	var out C.CrdtBuf
	if C.crdtsync_client_take_rejected(c.h, &out) != 1 {
		return nil
	}
	return decodeRejected(takeBuf(out))
}

// decodeRejected reads the take_rejected buffer: a u32 count, then per batch the
// channel (u32), the reason ErrorCode (i32), and the ops — a u32 op-count then
// per op a length-prefixed op byte string.
func decodeRejected(data []byte) []Rejected {
	r := &changeReader{d: data}
	n := int(r.u32())
	out := make([]Rejected, 0, n)
	for k := 0; k < n && r.err == nil; k++ {
		channel := r.u32()
		reason := ErrorCode(int32(r.u32()))
		m := int(r.u32())
		ops := make([][]byte, 0, m)
		for j := 0; j < m && r.err == nil; j++ {
			ops = append(ops, r.blob())
		}
		out = append(out, Rejected{Channel: channel, Reason: reason, Ops: ops})
	}
	if r.err != nil {
		return nil
	}
	return out
}

// TakeRedirects drains the room redirects the server has sent since the last call
// — a node that does not lead a room reporting the leader's address. Each Redirect
// names the Room and the leader's LeaderAddr; reconnecting is the app's job.
// Draining, so a second call is empty.
func (c *Client) TakeRedirects() []Redirect {
	var out C.CrdtBuf
	if C.crdtsync_client_take_redirects(c.h, &out) != 1 {
		return nil
	}
	return decodeRedirects(takeBuf(out))
}

// decodeRedirects reads the take_redirects buffer: a u32 count, then per redirect
// a length-prefixed room and a length-prefixed leader_addr byte string.
func decodeRedirects(data []byte) []Redirect {
	r := &changeReader{d: data}
	n := int(r.u32())
	out := make([]Redirect, 0, n)
	for k := 0; k < n && r.err == nil; k++ {
		room := r.blob()
		leaderAddr := r.blob()
		out = append(out, Redirect{Room: room, LeaderAddr: leaderAddr})
	}
	if r.err != nil {
		return nil
	}
	return out
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

// Dec installs-or-decrements a Counter in channel's room.
func (c *Client) Dec(channel uint32, path [][]byte, amount uint32) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_client_dec(c.h, C.uint32_t(channel), pp, pl, C.uint32_t(amount)))
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

// SetBlob sets an inline blob at path in channel's room, routed through the
// outbox. Returns the Ops frame to send; a bytes length over the inline ceiling
// enqueues no op (use SetBlobRef for a large blob).
func (c *Client) SetBlob(channel uint32, path [][]byte, mime string, bytes []byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	mp, ml := bytesArg([]byte(mime))
	bp, bl := bytesArg(bytes)
	return takeBuf(C.crdtsync_client_set_blob(c.h, C.uint32_t(channel), pp, pl, mp, ml, bp, bl))
}

// SetBlobRef sets a store-backed blob ref at path in channel's room from a
// 16-byte id handle, mime, and size, routed through the outbox. Returns the Ops
// frame to send.
func (c *Client) SetBlobRef(channel uint32, path [][]byte, id [16]byte, mime string, size uint64) []byte {
	pp, pl := bytesArg(EncodePath(path))
	mp, ml := bytesArg([]byte(mime))
	return takeBuf(C.crdtsync_client_set_blob_ref(c.h, C.uint32_t(channel), pp, pl, idArg(&id), mp, ml, C.uint64_t(size)))
}

// BeginAtomic starts an atomic transaction on channel's room; edits accumulate
// until CommitAtomic.
func (c *Client) BeginAtomic(channel uint32) {
	C.crdtsync_client_begin_atomic(c.h, C.uint32_t(channel))
}

// CommitAtomic commits the atomic transaction on channel, returning the Ops
// frame to send.
func (c *Client) CommitAtomic(channel uint32) []byte {
	return takeBuf(C.crdtsync_client_commit_atomic(c.h, C.uint32_t(channel)))
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

// --- branch management ---

// Branch is one branch of a room as the client observes it: its Name, the
// ForkPoint it shares history up to, its own Head position, and whether it is a
// read-only Published target.
type Branch struct {
	Name      []byte
	ForkPoint uint64
	Head      uint64
	Published bool
}

// ListBranches frames a request for room's branches. Room-keyed: a client may
// enumerate a room's branches before it subscribes any of them.
func (c *Client) ListBranches(room []byte) []byte {
	rp, rl := bytesArg(room)
	return takeBuf(C.crdtsync_client_list_branches(c.h, rp, rl))
}

// ForkBranch frames a request to fork branch name off from's HEAD in room.
func (c *Client) ForkBranch(room, name, from []byte) []byte {
	rp, rl := bytesArg(room)
	np, nl := bytesArg(name)
	fp, fl := bytesArg(from)
	return takeBuf(C.crdtsync_client_fork_branch(c.h, rp, rl, np, nl, fp, fl))
}

// ForkBranchFromVersion frames a request to fork branch name off the snapshot of
// version in room.
func (c *Client) ForkBranchFromVersion(room, name, version []byte) []byte {
	rp, rl := bytesArg(room)
	np, nl := bytesArg(name)
	vp, vl := bytesArg(version)
	return takeBuf(C.crdtsync_client_fork_branch_from_version(c.h, rp, rl, np, nl, vp, vl))
}

// RestoreBranch frames a request to restore room to version as a fresh branch
// name, switching the active HEAD to it.
func (c *Client) RestoreBranch(room, name, version []byte) []byte {
	rp, rl := bytesArg(room)
	np, nl := bytesArg(name)
	vp, vl := bytesArg(version)
	return takeBuf(C.crdtsync_client_restore_branch(c.h, rp, rl, np, nl, vp, vl))
}

// PublishBranch frames a request to publish room's active editor branch onto the
// read-only published branch.
func (c *Client) PublishBranch(room, published []byte) []byte {
	rp, rl := bytesArg(room)
	pp, pl := bytesArg(published)
	return takeBuf(C.crdtsync_client_publish_branch(c.h, rp, rl, pp, pl))
}

// DeleteBranch frames a request to delete branch name of room. The default main
// is never deletable.
func (c *Client) DeleteBranch(room, name []byte) []byte {
	rp, rl := bytesArg(room)
	np, nl := bytesArg(name)
	return takeBuf(C.crdtsync_client_delete_branch(c.h, rp, rl, np, nl))
}

// Branches returns the branch set last reported for room, in order.
func (c *Client) Branches(room []byte) []Branch {
	rp, rl := bytesArg(room)
	var count C.uintptr_t
	if C.crdtsync_client_branch_count(c.h, rp, rl, &count) != 1 {
		return nil
	}
	branches := make([]Branch, 0, int(count))
	for i := 0; i < int(count); i++ {
		var name C.CrdtBuf
		var forkPoint, head C.uint64_t
		var published C.int32_t
		if C.crdtsync_client_branch_at(c.h, rp, rl, C.uintptr_t(i), &name, &forkPoint, &head, &published) == 1 {
			branches = append(branches, Branch{
				Name:      takeBuf(name),
				ForkPoint: uint64(forkPoint),
				Head:      uint64(head),
				Published: published == 1,
			})
		}
	}
	return branches
}

// DiffKind selects which pair of a room's states a client DiffQuery compares.
type DiffKind uint32

const (
	// DiffVersions diffs two of a room's saved versions.
	DiffVersions DiffKind = 0
	// DiffBranches diffs two of a room's branches' HEADs.
	DiffBranches DiffKind = 1
)

// DiffQuery frames a request for the structural diff turning state a into state b
// in room. kind selects whether a/b name two saved versions or two branches.
// Room-keyed: a client may diff a room before it subscribes any of its branches.
// The reply updates the diff view, read with DiffResult.
func (c *Client) DiffQuery(room []byte, kind DiffKind, a, b []byte) []byte {
	rp, rl := bytesArg(room)
	ap, al := bytesArg(a)
	bp, bl := bytesArg(b)
	return takeBuf(C.crdtsync_client_diff_query(c.h, rp, rl, C.uint32_t(kind), ap, al, bp, bl))
}

// DiffResult returns the change list from the last diff query answered for room,
// decoded through the shared change reader. The bool is false until a result is
// answered; an empty diff is a non-nil empty slice with the bool true.
func (c *Client) DiffResult(room []byte) ([]Change, bool, error) {
	rp, rl := bytesArg(room)
	var out C.CrdtBuf
	if C.crdtsync_client_diff_result(c.h, rp, rl, &out) != 1 {
		return nil, false, nil
	}
	changes, err := decodeChanges(takeBuf(out))
	return changes, true, err
}

// --- schema-aware diff ---

// Scalar is a tagged scalar value: T names the kind and the matching field
// holds it. It crosses both as a diff leaf value and as a mark's payload. Bytes
// also carries a BlobRef's opaque encoded bytes, or an ElementRef's 16-byte id.
type Scalar struct {
	T     string // "null" | "bool" | "int" | "bytes" | "blobref" | "elementref"
	Bool  bool
	Int   int64
	Bytes []byte
}

// Item is a List item in a diff: either an inline scalar or a composite's kind.
type Item struct {
	Scalar *Scalar
	Kind   string
}

// Change is one structural change between two snapshots. Op names the variant;
// which fields it populates follows Op — "add"/"remove": Kind; "value": Old/New;
// "counter": OldInt/NewInt; "listInsert"/"listDelete": Index/Items;
// "textInsert"/"textDelete": Index/Text. A mark change is addressed by its own
// id and target sequence, not a Path: "markAdded" carries ID/Seq/Name and New,
// "markRemoved" ID/Seq/Name and Old (its last value), "markChanged" ID/Seq/Name
// and Old/New.
type Change struct {
	Op     string
	Path   []byte
	Kind   string
	Old    *Scalar
	New    *Scalar
	OldInt int64
	NewInt int64
	Index  uint
	Items  []Item
	Text   string
	ID     []byte
	Seq    []byte
	Name   []byte
}

var kindNames = [...]string{"scalar", "register", "counter", "map", "list", "text"}

// changeReader reads the change-list byte format the core emits (little-endian).
type changeReader struct {
	d   []byte
	i   int
	err error
}

func (r *changeReader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if r.i+n > len(r.d) {
		r.err = errors.New("truncated change list")
		return nil
	}
	b := r.d[r.i : r.i+n]
	r.i += n
	return b
}

func (r *changeReader) u8() byte {
	b := r.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}

func (r *changeReader) u32() uint32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}

func (r *changeReader) u64() uint64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

func (r *changeReader) blob() []byte {
	return append([]byte(nil), r.take(int(r.u32()))...)
}

// id16 reads a 16-byte element id (a mark id or its target sequence id).
func (r *changeReader) id16() []byte {
	return append([]byte(nil), r.take(16)...)
}

func (r *changeReader) kind() string {
	t := r.u8()
	if int(t) >= len(kindNames) {
		if r.err == nil {
			r.err = errors.New("bad element kind")
		}
		return ""
	}
	return kindNames[t]
}

func (r *changeReader) scalar() *Scalar {
	start := r.i
	switch r.u8() {
	case 0:
		return &Scalar{T: "null"}
	case 1:
		return &Scalar{T: "bool", Bool: r.u8() != 0}
	case 2:
		return &Scalar{T: "int", Int: int64(r.u64())}
	case 3:
		return &Scalar{T: "bytes", Bytes: r.blob()}
	case 4:
		r.take(16) // id
		r.blob()   // mime
		r.u64()    // size
		if r.u8() == 1 {
			r.blob() // inline
		}
		return &Scalar{T: "blobref", Bytes: append([]byte(nil), r.d[start:r.i]...)}
	case 5:
		return &Scalar{T: "elementref", Bytes: append([]byte(nil), r.take(16)...)}
	default:
		if r.err == nil {
			r.err = errors.New("bad scalar tag")
		}
		return &Scalar{}
	}
}

func (r *changeReader) items() []Item {
	n := int(r.u32())
	items := make([]Item, 0, n)
	for k := 0; k < n && r.err == nil; k++ {
		switch r.u8() {
		case 0:
			items = append(items, Item{Scalar: r.scalar()})
		case 1:
			items = append(items, Item{Kind: r.kind()})
		default:
			if r.err == nil {
				r.err = errors.New("bad diff item tag")
			}
		}
	}
	return items
}

func decodeChanges(data []byte) ([]Change, error) {
	r := &changeReader{d: data}
	count := int(r.u32())
	out := make([]Change, 0, count)
	for k := 0; k < count && r.err == nil; k++ {
		var ch Change
		switch r.u8() {
		case 0:
			ch = Change{Op: "add", Path: r.blob(), Kind: r.kind()}
		case 1:
			ch = Change{Op: "remove", Path: r.blob(), Kind: r.kind()}
		case 2:
			ch = Change{Op: "value", Path: r.blob(), Old: r.scalar(), New: r.scalar()}
		case 3:
			ch = Change{Op: "counter", Path: r.blob(), OldInt: int64(r.u64()), NewInt: int64(r.u64())}
		case 4:
			ch = Change{Op: "listInsert", Path: r.blob(), Index: uint(r.u64()), Items: r.items()}
		case 5:
			ch = Change{Op: "listDelete", Path: r.blob(), Index: uint(r.u64()), Items: r.items()}
		case 6:
			ch = Change{Op: "textInsert", Path: r.blob(), Index: uint(r.u64()), Text: string(r.blob())}
		case 7:
			ch = Change{Op: "textDelete", Path: r.blob(), Index: uint(r.u64()), Text: string(r.blob())}
		case 8:
			ch = Change{Op: "markAdded", ID: r.id16(), Seq: r.id16(), Name: r.blob(), New: r.scalar()}
		case 9:
			ch = Change{Op: "markRemoved", ID: r.id16(), Seq: r.id16(), Name: r.blob(), Old: r.scalar()}
		case 10:
			ch = Change{Op: "markChanged", ID: r.id16(), Seq: r.id16(), Name: r.blob(), Old: r.scalar(), New: r.scalar()}
		default:
			r.err = errors.New("bad change tag")
		}
		if r.err == nil {
			out = append(out, ch)
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	return out, nil
}

// DiffEncode computes the opaque change-list bytes turning oldState into
// newState — the buffer a peer decodes with DiffDecode. Each snapshot is a
// snapshot from EncodeState, a named version, or an exported room. Empty on a
// malformed snapshot.
func DiffEncode(oldState, newState []byte) []byte {
	op, ol := bytesArg(oldState)
	np, nl := bytesArg(newState)
	return takeBuf(C.crdtsync_diff(op, ol, np, nl))
}

// Diff computes the structural changes turning oldState into newState — each a
// snapshot from EncodeState, a named version, or an exported room. Each Change
// carries an Op tag and its variant's fields. Returns an error on a malformed
// snapshot.
func Diff(oldState, newState []byte) ([]Change, error) {
	data := DiffEncode(oldState, newState)
	if len(data) == 0 {
		return nil, errors.New("malformed snapshot")
	}
	return decodeChanges(data)
}

// DiffDecode decodes an opaque change-list buffer from DiffEncode back into its
// structural changes, validating the framing through the core's total boundary
// decoder — the read a peer runs on a diff that crossed a wire or a snapshot
// store. Returns an error on a truncated or malformed buffer.
func DiffDecode(data []byte) ([]Change, error) {
	dp, dl := bytesArg(data)
	var out C.CrdtBuf
	if C.crdtsync_diff_decode(dp, dl, &out) != 1 {
		return nil, errors.New("malformed diff buffer")
	}
	return decodeChanges(takeBuf(out))
}

// --- xml (document surface) ---
//
// An XmlElement/XmlFragment is a node in an ordered tree of element and text
// children. Reads (XmlTag, XmlChildrenLen) resolve the live node; edits emit
// ops like any other document edit. XmlMove is a Kleppmann tree move that keeps
// the child's identity and subtree.

// XmlElement installs a tagged XmlElement at path. Returns the ops to broadcast.
func (d *Document) XmlElement(path [][]byte, tag []byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	tp, tl := bytesArg(tag)
	return takeBuf(C.crdtsync_doc_xml_element(d.h, pp, pl, tp, tl))
}

// XmlFragment installs a tagless XmlFragment at path. Returns the ops.
func (d *Document) XmlFragment(path [][]byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_doc_xml_fragment(d.h, pp, pl))
}

// XmlTag reads the tag of the live XmlElement at path. The bool is false when
// the path is absent or a tagless fragment.
func (d *Document) XmlTag(path [][]byte) ([]byte, bool) {
	pp, pl := bytesArg(EncodePath(path))
	var out C.CrdtBuf
	if C.crdtsync_doc_xml_tag(d.h, pp, pl, &out) != 1 {
		return nil, false
	}
	return takeBuf(out), true
}

// XmlInsertElement inserts a nested XmlElement tagged tag at live index in the
// children of the node at path. Returns the ops.
func (d *Document) XmlInsertElement(path [][]byte, index uint, tag []byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	tp, tl := bytesArg(tag)
	return takeBuf(C.crdtsync_doc_xml_insert_element(d.h, pp, pl, C.uintptr_t(index), tp, tl))
}

// XmlInsertText inserts a Text-run child holding text at live index in the
// children of the node at path. Returns the ops.
func (d *Document) XmlInsertText(path [][]byte, index uint, text string) []byte {
	pp, pl := bytesArg(EncodePath(path))
	tp, tl := bytesArg([]byte(text))
	return takeBuf(C.crdtsync_doc_xml_insert_text(d.h, pp, pl, C.uintptr_t(index), tp, tl))
}

// XmlChildDelete tombstones the child at live index in the children of the node
// at path. Returns the ops.
func (d *Document) XmlChildDelete(path [][]byte, index uint) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_doc_xml_child_delete(d.h, pp, pl, C.uintptr_t(index)))
}

// XmlChildrenLen reads the count of live children of the node at path.
func (d *Document) XmlChildrenLen(path [][]byte) (uint, bool) {
	pp, pl := bytesArg(EncodePath(path))
	var out C.uintptr_t
	rc := C.crdtsync_doc_xml_children_len(d.h, pp, pl, &out)
	return uint(out), rc == 1
}

// XmlMove relocates the live child at childIndex under parentPath to destIndex
// in the children of newParentPath — a Kleppmann tree move that keeps the
// child's identity and subtree. Returns the ops.
func (d *Document) XmlMove(parentPath [][]byte, childIndex uint, newParentPath [][]byte, destIndex uint) []byte {
	pp, pl := bytesArg(EncodePath(parentPath))
	np, nl := bytesArg(EncodePath(newParentPath))
	return takeBuf(C.crdtsync_doc_xml_move(d.h, pp, pl, C.uintptr_t(childIndex), np, nl, C.uintptr_t(destIndex)))
}

// --- xml (client surface) ---
//
// The same edits on a subscribed room's replica; their ops route through the
// outbox, so they are resent / acknowledged rather than framed and forgotten.

// XmlElement installs a tagged XmlElement at path in channel's room. Returns
// the Ops frame to send.
func (c *Client) XmlElement(channel uint32, path [][]byte, tag []byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	tp, tl := bytesArg(tag)
	return takeBuf(C.crdtsync_client_xml_element(c.h, C.uint32_t(channel), pp, pl, tp, tl))
}

// XmlFragment installs a tagless XmlFragment at path in channel's room. Returns
// the Ops frame.
func (c *Client) XmlFragment(channel uint32, path [][]byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_client_xml_fragment(c.h, C.uint32_t(channel), pp, pl))
}

// XmlInsertElement inserts a nested XmlElement tagged tag at live index in the
// children of the node at path in channel's room. Returns the Ops frame.
func (c *Client) XmlInsertElement(channel uint32, path [][]byte, index uint, tag []byte) []byte {
	pp, pl := bytesArg(EncodePath(path))
	tp, tl := bytesArg(tag)
	return takeBuf(C.crdtsync_client_xml_insert_element(c.h, C.uint32_t(channel), pp, pl, C.uintptr_t(index), tp, tl))
}

// XmlInsertText inserts a Text-run child holding text at live index in the
// children of the node at path in channel's room. Returns the Ops frame.
func (c *Client) XmlInsertText(channel uint32, path [][]byte, index uint, text string) []byte {
	pp, pl := bytesArg(EncodePath(path))
	tp, tl := bytesArg([]byte(text))
	return takeBuf(C.crdtsync_client_xml_insert_text(c.h, C.uint32_t(channel), pp, pl, C.uintptr_t(index), tp, tl))
}

// XmlChildDelete tombstones the child at live index in the children of the node
// at path in channel's room. Returns the Ops frame.
func (c *Client) XmlChildDelete(channel uint32, path [][]byte, index uint) []byte {
	pp, pl := bytesArg(EncodePath(path))
	return takeBuf(C.crdtsync_client_xml_child_delete(c.h, C.uint32_t(channel), pp, pl, C.uintptr_t(index)))
}

// XmlMove relocates the live child at childIndex under parentPath to destIndex
// in the children of newParentPath, in channel's room. Returns the Ops frame.
func (c *Client) XmlMove(channel uint32, parentPath [][]byte, childIndex uint, newParentPath [][]byte, destIndex uint) []byte {
	pp, pl := bytesArg(EncodePath(parentPath))
	np, nl := bytesArg(EncodePath(newParentPath))
	return takeBuf(C.crdtsync_client_xml_move(c.h, C.uint32_t(channel), pp, pl, C.uintptr_t(childIndex), np, nl, C.uintptr_t(destIndex)))
}

// --- marks ---
//
// A mark is a named range over a sequence (a Text or List), authored with two
// (index, side) endpoints and a scalar payload, and read back per its resolved
// state at a character. Authoring returns the mark's 16-byte id — the handle a
// later MarkSetValue / MarkDelete names it by — alongside the ops to broadcast.

// Mark is a resolved mark on a character: Name and, by Flavor, its payload —
// "bool" a boolean in Bool, "value" a scalar in Value, "object" a set of
// element ids in IDs.
type Mark struct {
	Name   []byte
	Flavor string
	Bool   bool
	Value  *Scalar
	IDs    [][]byte
}

// encodeScalar serializes a Scalar to the core's canonical value bytes — the
// shape a mark payload crosses as. A "blobref"/"elementref" carries its already
// encoded bytes verbatim.
func encodeScalar(s Scalar) []byte {
	switch s.T {
	case "null":
		return []byte{0}
	case "bool":
		b := byte(0)
		if s.Bool {
			b = 1
		}
		return []byte{1, b}
	case "int":
		out := make([]byte, 9)
		out[0] = 2
		binary.LittleEndian.PutUint64(out[1:], uint64(s.Int))
		return out
	case "bytes":
		out := make([]byte, 5+len(s.Bytes))
		out[0] = 3
		binary.LittleEndian.PutUint32(out[1:], uint32(len(s.Bytes)))
		copy(out[5:], s.Bytes)
		return out
	case "elementref":
		out := make([]byte, 1+len(s.Bytes))
		out[0] = 5
		copy(out[1:], s.Bytes)
		return out
	case "blobref":
		return append([]byte(nil), s.Bytes...)
	default:
		panic("crdtsync: unknown scalar kind " + s.T)
	}
}

// decodeMarks reads the resolved-marks buffer crdtsync_doc_marks_at emits: a
// u32 count, then per mark a length-prefixed name, a one-byte flavor tag, and
// that tag's payload.
func decodeMarks(data []byte) []Mark {
	r := &changeReader{d: data}
	n := int(r.u32())
	marks := make([]Mark, 0, n)
	for k := 0; k < n && r.err == nil; k++ {
		name := r.blob()
		switch r.u8() {
		case 0:
			marks = append(marks, Mark{Name: name, Flavor: "bool", Bool: r.u8() != 0})
		case 1:
			r.u32() // scalar byte length; the scalar reader self-delimits
			marks = append(marks, Mark{Name: name, Flavor: "value", Value: r.scalar()})
		case 2:
			c := int(r.u32())
			ids := make([][]byte, 0, c)
			for j := 0; j < c && r.err == nil; j++ {
				ids = append(ids, r.id16())
			}
			marks = append(marks, Mark{Name: name, Flavor: "object", IDs: ids})
		default:
			if r.err == nil {
				r.err = errors.New("bad mark flavor")
			}
		}
	}
	if r.err != nil {
		return nil
	}
	return marks
}

// Mark authors a named mark over [start, end) of the sequence at seqPath, each
// endpoint an (index, side) pair, carrying the scalar value. Returns the mark's
// 16-byte id handle and the ops to broadcast. The id is nil on an inert author
// (a bad handle, a non-sequence path, or a malformed value).
func (d *Document) Mark(seqPath [][]byte, startIndex uint, startSide Side, endIndex uint, endSide Side, name []byte, value Scalar) (markID []byte, ops []byte) {
	pp, pl := bytesArg(EncodePath(seqPath))
	np, nl := bytesArg(name)
	vp, vl := bytesArg(encodeScalar(value))
	var mid C.CrdtBuf
	ops = takeBuf(C.crdtsync_doc_mark(d.h, pp, pl, C.uintptr_t(startIndex), C.uint32_t(startSide), C.uintptr_t(endIndex), C.uint32_t(endSide), np, nl, vp, vl, &mid))
	id := takeBuf(mid)
	if len(id) == 0 {
		return nil, ops
	}
	return id, ops
}

// MarkSetValue changes the scalar payload of the mark handle markID. Returns
// the ops to broadcast.
func (d *Document) MarkSetValue(markID []byte, value Scalar) []byte {
	mp, ml := bytesArg(markID)
	vp, vl := bytesArg(encodeScalar(value))
	return takeBuf(C.crdtsync_doc_mark_set_value(d.h, mp, ml, vp, vl))
}

// MarkDelete tombstones the mark handle markID. Returns the ops to broadcast.
func (d *Document) MarkDelete(markID []byte) []byte {
	mp, ml := bytesArg(markID)
	return takeBuf(C.crdtsync_doc_mark_delete(d.h, mp, ml))
}

// AclGrant grants a doc-level ACL tuple: an allow/deny (effect) of grant to the
// subject (a SubjectKind plus its bytes — a 16-byte actor id, a group name, or nil
// for a class), on path, recorded with the authoring actor grantor (16 bytes).
// Returns the new tuple's 16-byte id — the handle AclRevoke names it by — and the
// ops to broadcast. Both are nil on a malformed subject/grant/grantor.
func (d *Document) AclGrant(subjectKind SubjectKind, subject []byte, grant Grant, effect Effect, path [][]byte, grantor []byte) (tupleID []byte, ops []byte) {
	sp, sl := bytesArg(subject)
	rp, rl := bytesArg(grant.role)
	pp, pl := bytesArg(EncodePath(path))
	gp, gl := bytesArg(grantor)
	var outID, outOps C.CrdtBuf
	rc := C.crdtsync_doc_acl_grant(d.h, C.uint32_t(subjectKind), sp, sl, C.uint32_t(grant.kind), C.uint32_t(grant.cap), rp, rl, C.uint32_t(effect), pp, pl, gp, gl, &outID, &outOps)
	if rc != 1 {
		return nil, nil
	}
	return takeBuf(outID), takeBuf(outOps)
}

// ActorKey derives the doc-ACL actor key for a credential actor: the fixed 16-byte
// SHA-256 truncation the server keys tuples by. Build an AclGrant SubjectActor
// subject and its grantor from this so the authenticated actor — not an ephemeral
// per-device id — is the matched ACL principal, identical across devices and after a
// restart.
func ActorKey(actor []byte) []byte {
	ap, al := bytesArg(actor)
	var out C.CrdtBuf
	if C.crdtsync_actor_key(ap, al, &out) != 1 {
		return nil
	}
	return takeBuf(out)
}

// AclRevoke tombstones the ACL tuple tupleID (16 bytes from AclGrant). Returns the
// ops to broadcast; empty when tupleID names no tuple this replica holds or is
// malformed.
func (d *Document) AclRevoke(tupleID []byte) []byte {
	ip, il := bytesArg(tupleID)
	var out C.CrdtBuf
	if C.crdtsync_doc_acl_revoke(d.h, ip, il, &out) < 0 {
		return nil
	}
	return takeBuf(out)
}

// MarksAt reads the marks active on character index of the sequence at seqPath.
func (d *Document) MarksAt(seqPath [][]byte, index uint) []Mark {
	pp, pl := bytesArg(EncodePath(seqPath))
	var out C.CrdtBuf
	if C.crdtsync_doc_marks_at(d.h, pp, pl, C.uintptr_t(index), &out) != 1 {
		return nil
	}
	return decodeMarks(takeBuf(out))
}

// Mark authors a named mark over [start, end) of the sequence at seqPath in
// channel's room, routed through the outbox. Returns the mark's 16-byte id
// handle and the Ops frame to send; the id is nil on an inert author.
func (c *Client) Mark(channel uint32, seqPath [][]byte, startIndex uint, startSide Side, endIndex uint, endSide Side, name []byte, value Scalar) (markID []byte, frame []byte) {
	pp, pl := bytesArg(EncodePath(seqPath))
	np, nl := bytesArg(name)
	vp, vl := bytesArg(encodeScalar(value))
	var mid C.CrdtBuf
	frame = takeBuf(C.crdtsync_client_mark(c.h, C.uint32_t(channel), pp, pl, C.uintptr_t(startIndex), C.uint32_t(startSide), C.uintptr_t(endIndex), C.uint32_t(endSide), np, nl, vp, vl, &mid))
	id := takeBuf(mid)
	if len(id) == 0 {
		return nil, frame
	}
	return id, frame
}

// MarkSetValue changes the payload of the mark handle markID in channel's room,
// routed through the outbox. Returns the Ops frame to send.
func (c *Client) MarkSetValue(channel uint32, markID []byte, value Scalar) []byte {
	mp, ml := bytesArg(markID)
	vp, vl := bytesArg(encodeScalar(value))
	return takeBuf(C.crdtsync_client_mark_set_value(c.h, C.uint32_t(channel), mp, ml, vp, vl))
}

// MarkDelete tombstones the mark handle markID in channel's room, routed
// through the outbox. Returns the Ops frame to send.
func (c *Client) MarkDelete(channel uint32, markID []byte) []byte {
	mp, ml := bytesArg(markID)
	return takeBuf(C.crdtsync_client_mark_delete(c.h, C.uint32_t(channel), mp, ml))
}

// AclGrant grants a doc-level ACL tuple in channel's room, routed through the
// outbox. Same fields as Document.AclGrant. Returns the new tuple's 16-byte id and
// the Ops frame to send; the id is nil when the channel isn't held.
func (c *Client) AclGrant(channel uint32, subjectKind SubjectKind, subject []byte, grant Grant, effect Effect, path [][]byte, grantor []byte) (tupleID []byte, frame []byte) {
	sp, sl := bytesArg(subject)
	rp, rl := bytesArg(grant.role)
	pp, pl := bytesArg(EncodePath(path))
	gp, gl := bytesArg(grantor)
	var outID C.CrdtBuf
	frame = takeBuf(C.crdtsync_client_acl_grant(c.h, C.uint32_t(channel), C.uint32_t(subjectKind), sp, sl, C.uint32_t(grant.kind), C.uint32_t(grant.cap), rp, rl, C.uint32_t(effect), pp, pl, gp, gl, &outID))
	id := takeBuf(outID)
	if len(id) == 0 {
		return nil, frame
	}
	return id, frame
}

// AclRevoke tombstones the ACL tuple tupleID in channel's room, routed through the
// outbox. Returns the Ops frame to send; empty when the channel isn't held or the id
// names no live tuple.
func (c *Client) AclRevoke(channel uint32, tupleID []byte) []byte {
	ip, il := bytesArg(tupleID)
	return takeBuf(C.crdtsync_client_acl_revoke(c.h, C.uint32_t(channel), ip, il))
}

// --- schema + repair ---
//
// A schema binds to the local document as runtime state — it authors no op and
// broadcasts nothing. TakeRepairs drains the located paths whose repaired
// reading newly changed against the bound schema since the last call.

// Step is one hop of a repair path: a map-slot Key or a sequence Index,
// discriminated by IsIndex. A repair path can descend a sequence index (a
// bounded list item, an xml child), which a bare key path cannot express.
type Step struct {
	Key     []byte
	Index   uint
	IsIndex bool
}

// SetSchema parses schema JSON bytes and binds the schema to the document for
// repair observation. Reports true when the schema bound, false when the bytes
// are not a valid schema. Binding takes the current state as the baseline.
func (d *Document) SetSchema(schema []byte) bool {
	sp, sl := bytesArg(schema)
	return C.crdtsync_doc_set_schema(d.h, sp, sl) == 1
}

// TakeRepairs drains the repair signal: the located paths whose repaired
// reading has newly changed against the bound schema since the last call. The
// drain reseeds the baseline, so a standing repair reports once. Each path is
// a sequence of Steps naming a location, not a value.
func (d *Document) TakeRepairs() [][]Step {
	var out C.CrdtBuf
	if C.crdtsync_doc_take_repairs(d.h, &out) != 1 {
		return nil
	}
	return decodeRepairPaths(takeBuf(out))
}

// decodeRepairPaths reads the repair-path list crdtsync_doc_take_repairs emits:
// a u32 count, then per path a length-prefixed encoded repair-path byte string.
func decodeRepairPaths(data []byte) [][]Step {
	r := &changeReader{d: data}
	n := int(r.u32())
	paths := make([][]Step, 0, n)
	for k := 0; k < n && r.err == nil; k++ {
		paths = append(paths, parseRepairPath(r.blob()))
	}
	if r.err != nil {
		return nil
	}
	return paths
}

// parseRepairPath decodes one repair-path byte string into its Steps: each step
// a one-byte tag then its payload — a key a u32 length then its bytes, an index
// a u64. Total over any bytes: a bad tag or a length past the end truncates.
func parseRepairPath(b []byte) []Step {
	var steps []Step
	i := 0
	for i < len(b) {
		tag := b[i]
		i++
		switch tag {
		case 0: // key
			if i+4 > len(b) {
				return steps
			}
			klen := int(binary.LittleEndian.Uint32(b[i:]))
			i += 4
			if i+klen > len(b) {
				return steps
			}
			steps = append(steps, Step{Key: append([]byte(nil), b[i:i+klen]...)})
			i += klen
		case 1: // index
			if i+8 > len(b) {
				return steps
			}
			steps = append(steps, Step{Index: uint(binary.LittleEndian.Uint64(b[i:])), IsIndex: true})
			i += 8
		default:
			return steps
		}
	}
	return steps
}
