package crdtsync

// The ergonomic handle-graph surface (§SDK-Ergonomic-Surface): a Doc wraps the
// low-level Document additively and exposes live typed handles — CrdtMap /
// CrdtList / CrdtText (CrdtXml lands with the rich-content surface) — addressed
// by ergonomic string keys, never byte-paths. A handle holds its logical path
// (a sequence of keys) and re-resolves it on every operation, so it stays valid
// as the document mutates and converges — a view, never a cached pointer.
// Handles compose. The byte-path Document stays available as the low-level
// power-user surface.
//
// Native value marshaling matches the JS/Python boundary exactly (the pinned
// cross-SDK contract): string <-> Scalar::Bytes (utf-8), int64 <-> Scalar::Int,
// bool <-> Scalar::Bool, nil <-> Scalar::Null, []byte <-> Scalar::Bytes (raw). A
// leaf is written with an explicit native scalar; a container is created only
// with an explicit GetMap/GetList/GetText accessor — passing anything else
// to Set is an error, never an implicit subtree (Automerge-style deep-seed is a
// rejected non-goal). A Go string carries arbitrary bytes, so it is both the
// utf-8 key and the raw-key carrier ([]byte(key) recovers the bytes); string and
// []byte values both land in Scalar::Bytes, which the core cannot itself tell
// apart, so the SDK prefixes the payload with a one-byte string/binary
// discriminator — an SDK framing detail invisible to the value read back.

import (
	"crypto/rand"
	"fmt"
)

const (
	discBinary = 0x00
	discString = 0x01
)

// marshalValue encodes a native scalar into the Scalar bytes a leaf stores,
// routed through the canonical encodeScalar so the handle graph and the
// path-based Scalar surface never drift. A string/[]byte payload is prefixed
// with the one-byte string/binary discriminator before it becomes a Scalar's
// Bytes. Rejects a container/other type (create a nested container with an
// explicit accessor). i64 is Go's native int64 so no overflow guard is needed,
// unlike Python's arbitrary int or JS's number.
func marshalValue(value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return encodeScalar(Scalar{T: "null"}), nil
	case bool:
		return encodeScalar(Scalar{T: "bool", Bool: v}), nil
	case int:
		return encodeScalar(Scalar{T: "int", Int: int64(v)}), nil
	case int64:
		return encodeScalar(Scalar{T: "int", Int: v}), nil
	case int32:
		return encodeScalar(Scalar{T: "int", Int: int64(v)}), nil
	case int16:
		return encodeScalar(Scalar{T: "int", Int: int64(v)}), nil
	case int8:
		return encodeScalar(Scalar{T: "int", Int: int64(v)}), nil
	case string:
		return encodeScalar(Scalar{T: "bytes", Bytes: withDiscriminator(discString, []byte(v))}), nil
	case []byte:
		return encodeScalar(Scalar{T: "bytes", Bytes: withDiscriminator(discBinary, v)}), nil
	default:
		return nil, fmt.Errorf(
			"crdtsync: value must be string, int64, bool, []byte, or nil (got %T); "+
				"create a nested container with GetMap/GetList/GetText", value)
	}
}

func withDiscriminator(disc byte, payload []byte) []byte {
	out := make([]byte, 1+len(payload))
	out[0] = disc
	copy(out[1:], payload)
	return out
}

// unmarshalValue reads encoded Scalar bytes back into a native value — the
// inverse of marshalValue, decoded through the shared, bounds-checked
// changeReader so a truncated buffer degrades to opaque bytes rather than
// panicking. A string leaf reads back as string, a binary leaf as []byte; a
// blob/element ref (no native leaf form) hands back the opaque bytes.
func unmarshalValue(data []byte) any {
	r := &changeReader{d: data}
	s := r.scalar()
	if r.err != nil {
		return append([]byte(nil), data...)
	}
	switch s.T {
	case "null":
		return nil
	case "bool":
		return s.Bool
	case "int":
		return s.Int
	case "bytes":
		body := s.Bytes
		if len(body) == 0 {
			return []byte(nil)
		}
		switch body[0] {
		case discString:
			return string(body[1:])
		case discBinary:
			return append([]byte(nil), body[1:]...)
		default:
			return append([]byte(nil), body...)
		}
	default: // blobref / elementref — no native leaf form
		return append([]byte(nil), data...)
	}
}

// appendKey extends a handle path by one key, copying so a child handle never
// aliases a parent's backing array.
func appendKey(path [][]byte, k []byte) [][]byte {
	out := make([][]byte, len(path)+1)
	copy(out, path)
	out[len(path)] = k
	return out
}

// keyString renders a slot key as a Go string. A Go string carries arbitrary
// bytes, so a binary (non-utf-8) key is preserved verbatim and its value is
// still read by its raw bytes.
func keyString(k []byte) string { return string(k) }

// UpdateEvent is an applied change delivered to Doc.OnUpdate. Origin is "local"
// for an edit on this replica, "remote" for an applied peer update; Ops are the
// wire-bound bytes the edit produced.
type UpdateEvent struct {
	Origin string
	Ops    []byte
}

// Doc is a local CRDT replica with a single root map, edited through live typed
// handles. Two docs that exchange each other's update ops (forwarded via
// OnUpdate) converge. The low-level path API stays available on the wrapped
// Document for power users (Doc.Backend()).
type Doc struct {
	backend         *Document
	updateListeners map[int]func(UpdateEvent)
	nextListenerID  int
}

// NewDoc opens a Doc for a fresh random 16-byte client id.
func NewDoc() (*Doc, error) {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return nil, err
	}
	return NewDocWithClientID(id)
}

// NewDocWithClientID opens a Doc for the given 16-byte client id.
func NewDocWithClientID(clientID []byte) (*Doc, error) {
	backend, err := New(clientID)
	if err != nil {
		return nil, err
	}
	return &Doc{backend: backend}, nil
}

// DecodeDoc opens a Doc from a snapshot produced by Doc.EncodeState.
func DecodeDoc(state []byte) (*Doc, error) {
	backend, err := DecodeState(state)
	if err != nil {
		return nil, err
	}
	return &Doc{backend: backend}, nil
}

// Backend returns the wrapped low-level Document — the byte-path power-user
// surface underneath the handle graph.
func (d *Doc) Backend() *Document { return d.backend }

// Close frees the document. Safe to call more than once.
func (d *Doc) Close() { d.backend.Close() }

// EncodeState serializes the whole replica to a canonical snapshot.
func (d *Doc) EncodeState() []byte { return d.backend.EncodeState() }

// GetMap returns a live root Map handle at key.
func (d *Doc) GetMap(key string) *CrdtMap {
	return &CrdtMap{doc: d, path: [][]byte{[]byte(key)}}
}

// GetList returns a live root List handle at key.
func (d *Doc) GetList(key string) *CrdtList {
	return &CrdtList{doc: d, path: [][]byte{[]byte(key)}}
}

// GetText returns a live root Text handle at key.
func (d *Doc) GetText(key string) *CrdtText {
	return &CrdtText{doc: d, path: [][]byte{[]byte(key)}}
}

// OnUpdate subscribes to applied changes to the document; returns a function
// that unsubscribes.
func (d *Doc) OnUpdate(cb func(UpdateEvent)) func() {
	if d.updateListeners == nil {
		d.updateListeners = map[int]func(UpdateEvent){}
	}
	id := d.nextListenerID
	d.nextListenerID++
	d.updateListeners[id] = cb
	return func() { delete(d.updateListeners, id) }
}

// ApplyUpdate folds a peer's update ops into this replica; returns the count
// applied.
func (d *Doc) ApplyUpdate(ops []byte) int {
	applied := d.backend.Apply(ops)
	if applied > 0 {
		d.dispatch("remote", ops)
	}
	return applied
}

// mutate runs one edit and dispatches its ops as a local update.
func (d *Doc) mutate(run func(*Document) []byte) []byte {
	ops := run(d.backend)
	if len(ops) == 0 {
		return ops
	}
	d.dispatch("local", ops)
	return ops
}

func (d *Doc) dispatch(origin string, ops []byte) {
	event := UpdateEvent{Origin: origin, Ops: ops}
	for _, l := range snapshotUpdateListeners(d.updateListeners) {
		l(event)
	}
}

// snapshotUpdateListeners copies the listener set so one subscribed or removed
// during dispatch does not perturb this in-flight fire.
func snapshotUpdateListeners(m map[int]func(UpdateEvent)) []func(UpdateEvent) {
	out := make([]func(UpdateEvent), 0, len(m))
	for _, l := range m {
		out = append(out, l)
	}
	return out
}

func (d *Doc) containerKind(slot [][]byte) string {
	if _, ok := d.backend.MapKeys(slot); ok {
		return "map"
	}
	if _, ok := d.backend.ListLen(slot); ok {
		return "list"
	}
	if _, ok := d.backend.TextLen(slot); ok {
		return "text"
	}
	return ""
}

func (d *Doc) handleFor(kind string, path [][]byte) any {
	switch kind {
	case "map":
		return &CrdtMap{doc: d, path: path}
	case "list":
		return &CrdtList{doc: d, path: path}
	case "text":
		return &CrdtText{doc: d, path: path}
	}
	return nil
}

// Entry is one live (key, value) pair of a CrdtMap.
type Entry struct {
	Key   string
	Value any
}

// CrdtMap is a live handle to a Map slot, addressed by ergonomic string keys.
type CrdtMap struct {
	doc  *Doc
	path [][]byte
}

func (m *CrdtMap) slot(key string) [][]byte { return appendKey(m.path, []byte(key)) }

// Set writes a leaf at key to a native scalar (string, int64, bool, []byte, or
// nil). Returns an error for an unsupported type — a nested container is created
// with GetMap/GetList/GetText/GetXml, never implicitly seeded here.
func (m *CrdtMap) Set(key string, value any) error {
	scalar, err := marshalValue(value)
	if err != nil {
		return err
	}
	slot := m.slot(key)
	m.doc.mutate(func(b *Document) []byte { return b.SetScalar(slot, scalar) })
	return nil
}

// Get reads key: a native scalar for a leaf, a BlobRef for a blob, a nested
// handle for a container slot, or (nil, false) when the slot is empty.
func (m *CrdtMap) Get(key string) (any, bool) {
	slot := m.slot(key)
	if blob, ok := m.doc.backend.GetBlob(slot); ok {
		return blob, true
	}
	if scalar, ok := m.doc.backend.GetScalar(slot); ok {
		return unmarshalValue(scalar), true
	}
	kind := m.doc.containerKind(slot)
	if kind == "" {
		return nil, false
	}
	if h := m.doc.handleFor(kind, slot); h != nil {
		return h, true
	}
	return nil, false
}

// Delete tombstones the slot at key.
func (m *CrdtMap) Delete(key string) {
	slot := m.slot(key)
	m.doc.mutate(func(b *Document) []byte { return b.Delete(slot) })
}

// Has reports whether key holds a leaf, a blob, or a container.
func (m *CrdtMap) Has(key string) bool {
	slot := m.slot(key)
	if _, ok := m.doc.backend.GetScalar(slot); ok {
		return true
	}
	if _, ok := m.doc.backend.GetBlob(slot); ok {
		return true
	}
	return m.doc.containerKind(slot) != ""
}

func (m *CrdtMap) rawKeys() [][]byte {
	keys, _ := m.doc.backend.MapKeys(m.path)
	return keys
}

// Keys returns the live slot keys, rendered best-effort as utf-8 strings.
func (m *CrdtMap) Keys() []string {
	raw := m.rawKeys()
	out := make([]string, len(raw))
	for i, k := range raw {
		out[i] = keyString(k)
	}
	return out
}

// Entries returns the live (key, value) pairs. Values are read by the raw key
// bytes, so a non-utf-8 (binary) key's value is never lost.
func (m *CrdtMap) Entries() []Entry {
	raw := m.rawKeys()
	out := make([]Entry, 0, len(raw))
	for _, k := range raw {
		v, _ := m.Get(keyString(k))
		out = append(out, Entry{Key: keyString(k), Value: v})
	}
	return out
}

// Len returns the number of live slots.
func (m *CrdtMap) Len() int { return len(m.rawKeys()) }

// GetMap returns a nested Map handle at key.
func (m *CrdtMap) GetMap(key string) *CrdtMap {
	return &CrdtMap{doc: m.doc, path: m.slot(key)}
}

// GetList returns a nested List handle at key.
func (m *CrdtMap) GetList(key string) *CrdtList {
	return &CrdtList{doc: m.doc, path: m.slot(key)}
}

// GetText returns a nested Text handle at key.
func (m *CrdtMap) GetText(key string) *CrdtText {
	return &CrdtText{doc: m.doc, path: m.slot(key)}
}

// CrdtList is a live handle to a List of scalar items, addressed by live index.
type CrdtList struct {
	doc  *Doc
	path [][]byte
}

// Insert inserts a scalar item at a live index (clamped into range). Returns an
// error for an unsupported value type.
func (l *CrdtList) Insert(index int, value any) error {
	item, err := marshalValue(value)
	if err != nil {
		return err
	}
	n := l.Len()
	if index < 0 {
		index = n + index
		if index < 0 {
			index = 0
		}
	}
	if index > n {
		index = n
	}
	idx := index
	l.doc.mutate(func(b *Document) []byte { return b.ListInsert(l.path, uint(idx), item) })
	return nil
}

// Append appends a scalar item.
func (l *CrdtList) Append(value any) error { return l.Insert(l.Len(), value) }

// Delete tombstones the live item at index. Returns an error when index is out
// of range.
func (l *CrdtList) Delete(index int) error {
	idx, err := l.checked(index)
	if err != nil {
		return err
	}
	l.doc.mutate(func(b *Document) []byte { return b.ListDelete(l.path, idx) })
	return nil
}

// Get reads the item at index. The bool is false when index is out of range.
func (l *CrdtList) Get(index int) (any, bool) {
	idx, err := l.checked(index)
	if err != nil {
		return nil, false
	}
	item, ok := l.doc.backend.ListGet(l.path, idx)
	if !ok {
		return nil, false
	}
	return unmarshalValue(item), true
}

// Len returns the live length of the list.
func (l *CrdtList) Len() int {
	n, _ := l.doc.backend.ListLen(l.path)
	return int(n)
}

// Values returns the live items in order.
func (l *CrdtList) Values() []any {
	n := l.Len()
	out := make([]any, 0, n)
	for i := 0; i < n; i++ {
		v, _ := l.Get(i)
		out = append(out, v)
	}
	return out
}

func (l *CrdtList) checked(index int) (uint, error) {
	n := l.Len()
	if index < 0 {
		index += n
	}
	if index < 0 || index >= n {
		return 0, fmt.Errorf("crdtsync: list index %d out of range (len %d)", index, n)
	}
	return uint(index), nil
}

// CrdtText is a live handle to a collaborative Text run, indexed by codepoint.
type CrdtText struct {
	doc  *Doc
	path [][]byte
}

// Insert inserts text at a codepoint index.
func (t *CrdtText) Insert(index int, text string) {
	t.doc.mutate(func(b *Document) []byte { return b.TextInsert(t.path, uint(index), text) })
}

// Delete tombstones count codepoints from index.
func (t *CrdtText) Delete(index, count int) {
	t.doc.mutate(func(b *Document) []byte { return b.TextDelete(t.path, uint(index), uint(count)) })
}

// String returns the text content.
func (t *CrdtText) String() string {
	s, _ := t.doc.backend.TextGet(t.path)
	return s
}

// Len returns the codepoint length of the text.
func (t *CrdtText) Len() int {
	n, _ := t.doc.backend.TextLen(t.path)
	return int(n)
}
