package crdtsync

// The ergonomic handle-graph surface (§SDK-Ergonomic-Surface): a Doc wraps the
// low-level Document additively and exposes live typed handles — CrdtMap /
// CrdtList / CrdtText / CrdtXml — addressed by ergonomic string keys, never
// byte-paths. A handle holds its logical path (a sequence of keys) and
// re-resolves it on every operation, so it stays valid as the document mutates
// and converges — a view, never a cached pointer. Handles compose. The
// byte-path Document stays available as the low-level power-user surface.
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
	s, err := marshalScalar(value)
	if err != nil {
		return nil, err
	}
	return encodeScalar(s), nil
}

// marshalScalar maps a native scalar to a tagged Scalar (a string/[]byte payload
// prefixed with the string/binary discriminator). It is the shared seam behind
// marshalValue (leaf writes) and mark authoring (a mark value marshals like a
// leaf so it round-trips a native value cross-SDK).
func marshalScalar(value any) (Scalar, error) {
	switch v := value.(type) {
	case nil:
		return Scalar{T: "null"}, nil
	case bool:
		return Scalar{T: "bool", Bool: v}, nil
	case int:
		return Scalar{T: "int", Int: int64(v)}, nil
	case int64:
		return Scalar{T: "int", Int: v}, nil
	case int32:
		return Scalar{T: "int", Int: int64(v)}, nil
	case int16:
		return Scalar{T: "int", Int: int64(v)}, nil
	case int8:
		return Scalar{T: "int", Int: int64(v)}, nil
	case string:
		return Scalar{T: "bytes", Bytes: withDiscriminator(discString, []byte(v))}, nil
	case []byte:
		return Scalar{T: "bytes", Bytes: withDiscriminator(discBinary, v)}, nil
	default:
		return Scalar{}, fmt.Errorf(
			"crdtsync: value must be string, int64, bool, []byte, or nil (got %T); "+
				"create a nested container with GetMap/GetList/GetText/GetXml", value)
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
// wire-bound bytes the edit produced; Changes are the diff-derived ergonomic
// changes (empty when nothing is observing).
type UpdateEvent struct {
	Origin  string
	Ops     []byte
	Changes []EventChange
}

// observer is a subtree subscription: a callback fired only for changes whose
// framed key-path begins with prefix.
type observer struct {
	prefix []byte
	cb     func(ChangeEvent)
}

// idListener pairs a callback with a stable id for ordered removal.
type idListener[T any] struct {
	id int
	cb T
}

// listenerList holds callbacks in registration order so they fire
// deterministically (matching the JS/Python reference, which iterate an
// insertion-ordered list — a Go map would randomize the order). add returns an
// unsubscribe func; snapshot copies the current callbacks so subscribing or
// unsubscribing during a fire is safe.
type listenerList[T any] struct {
	next  int
	items []idListener[T]
}

func (l *listenerList[T]) add(cb T) func() {
	id := l.next
	l.next++
	l.items = append(l.items, idListener[T]{id: id, cb: cb})
	return func() {
		for i, it := range l.items {
			if it.id == id {
				// Shift the tail down and clear the vacated slot so the removed
				// callback's closure is released for GC, not pinned by the array.
				copy(l.items[i:], l.items[i+1:])
				var zero idListener[T]
				l.items[len(l.items)-1] = zero
				l.items = l.items[:len(l.items)-1]
				return
			}
		}
	}
}

func (l *listenerList[T]) len() int { return len(l.items) }

func (l *listenerList[T]) snapshot() []T {
	out := make([]T, len(l.items))
	for i, it := range l.items {
		out[i] = it.cb
	}
	return out
}

// Doc is a local CRDT replica with a single root map, edited through live typed
// handles. Two docs that exchange each other's update ops (forwarded via
// OnUpdate) converge. The low-level path API stays available on the wrapped
// Document for power users (Doc.Backend()).
type Doc struct {
	backend         *Document
	updateListeners listenerList[func(UpdateEvent)]
	observers       listenerList[observer]
	repairListeners listenerList[func(RepairEvent)]
	transacting     bool
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

// GetXml returns a live root Xml handle at key (an XML element or fragment).
func (d *Doc) GetXml(key string) *CrdtXml {
	return &CrdtXml{doc: d, path: [][]byte{[]byte(key)}}
}

// Transact runs fn's edits as one atomic group — they apply together on every
// replica, ride the wire as a single batch, and fire one update. Nested calls
// flatten into the outermost transaction.
func (d *Doc) Transact(fn func()) {
	if d.transacting {
		fn()
		return
	}
	var before []byte
	if d.observing() {
		before = d.backend.EncodeState()
	}
	d.transacting = true
	d.backend.BeginAtomic()
	defer func() {
		d.transacting = false
		ops := d.backend.CommitAtomic()
		if len(ops) > 0 {
			d.dispatch("local", ops, before)
			d.emitRepairs()
		}
	}()
	fn()
}

// OnUpdate subscribes to applied changes to the document; returns a function
// that unsubscribes.
func (d *Doc) OnUpdate(cb func(UpdateEvent)) func() {
	return d.updateListeners.add(cb)
}

// OnRepair subscribes to the schema-repair signal (fires only once a schema is
// bound via SetSchema): the located paths whose repaired reading changed against
// the schema after an edit. Returns a function that unsubscribes.
func (d *Doc) OnRepair(cb func(RepairEvent)) func() {
	return d.repairListeners.add(cb)
}

// SetSchema binds a schema (its JSON, as bytes) to this replica, returning
// whether it bound. A bound schema gives named marks their declared flavor and
// turns on the OnRepair signal.
func (d *Doc) SetSchema(schema []byte) bool { return d.backend.SetSchema(schema) }

// ApplyUpdate folds a peer's update ops into this replica; returns the count
// applied.
func (d *Doc) ApplyUpdate(ops []byte) int {
	var before []byte
	if d.observing() {
		before = d.backend.EncodeState()
	}
	applied := d.backend.Apply(ops)
	if applied > 0 {
		d.dispatch("remote", ops, before)
		d.emitRepairs()
	}
	return applied
}

// mutate runs one edit and dispatches its ops as a local update. Inside a
// transaction the edit just accumulates; Transact's commit dispatches once.
func (d *Doc) mutate(run func(*Document) []byte) []byte {
	if d.transacting {
		run(d.backend)
		return nil
	}
	var before []byte
	if d.observing() {
		before = d.backend.EncodeState()
	}
	ops := run(d.backend)
	if len(ops) == 0 {
		return ops
	}
	d.dispatch("local", ops, before)
	d.emitRepairs()
	return ops
}

// observing reports whether any update listener or subtree observer is
// subscribed — a snapshot+diff runs only then, so an unobserved doc pays nothing.
func (d *Doc) observing() bool {
	return d.updateListeners.len() > 0 || d.observers.len() > 0
}

func (d *Doc) dispatch(origin string, ops []byte, before []byte) {
	var raws []changeWithPath
	if before != nil {
		raws = d.computeChanges(before)
	}
	changes := make([]EventChange, len(raws))
	for i, r := range raws {
		changes[i] = r.change
	}
	// A remote frame that changed nothing (an ack) fires no update; a local edit
	// always reports its ops.
	if origin == "local" || len(changes) > 0 {
		event := UpdateEvent{Origin: origin, Ops: ops, Changes: changes}
		for _, l := range d.updateListeners.snapshot() {
			l(event)
		}
	}
	for _, obs := range d.observers.snapshot() {
		var matched []EventChange
		for _, r := range raws {
			if pathStartsWith(r.pathBytes, obs.prefix) {
				matched = append(matched, r.change)
			}
		}
		if len(matched) > 0 {
			obs.cb(ChangeEvent{Origin: origin, Changes: matched})
		}
	}
}

// computeChanges diffs the replica against a pre-edit snapshot and re-marshals
// each raw change into an ergonomic EventChange plus its framed path (for
// observer prefix matching).
func (d *Doc) computeChanges(before []byte) []changeWithPath {
	after := d.backend.EncodeState()
	if len(before) == 0 || len(after) == 0 {
		return nil
	}
	raw := DiffEncode(before, after)
	if len(raw) == 0 {
		return nil
	}
	changes, err := decodeChanges(raw)
	if err != nil {
		return nil
	}
	out := make([]changeWithPath, 0, len(changes))
	for _, c := range changes {
		pb, ch := remarshalChange(c)
		out = append(out, changeWithPath{pathBytes: pb, change: ch})
	}
	return out
}

func (d *Doc) emitRepairs() {
	// Drain only when observed — the drain reseeds the baseline, so draining
	// unobserved would lose the signal (and take_repairs is empty until a schema
	// is bound).
	if d.repairListeners.len() == 0 {
		return
	}
	raw := d.backend.TakeRepairs()
	if len(raw) == 0 {
		return
	}
	paths := make([][]RepairStep, len(raw))
	for i, p := range raw {
		steps := make([]RepairStep, len(p))
		for j, s := range p {
			if s.IsIndex {
				steps[j] = RepairStep{Index: int(s.Index), IsIndex: true}
			} else {
				steps[j] = RepairStep{Key: string(s.Key)}
			}
		}
		paths[i] = steps
	}
	event := RepairEvent{Paths: paths}
	for _, l := range d.repairListeners.snapshot() {
		l(event)
	}
}

func (d *Doc) addObserver(prefix []byte, cb func(ChangeEvent)) func() {
	return d.observers.add(observer{prefix: prefix, cb: cb})
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
	if _, ok := d.backend.XmlChildrenLen(slot); ok {
		return "xml"
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
	case "xml":
		return &CrdtXml{doc: d, path: path}
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

// GetXml returns a nested Xml handle at key.
func (m *CrdtMap) GetXml(key string) *CrdtXml {
	return &CrdtXml{doc: m.doc, path: m.slot(key)}
}

// SetBlob stores a small blob inline at key, minting its public handle. Returns
// false when data exceeds the inline ceiling — upload it out of band with
// UploadBlob and set the returned handle via SetBlobRef.
func (m *CrdtMap) SetBlob(key, mime string, data []byte) bool {
	slot := m.slot(key)
	ok := false
	m.doc.mutate(func(b *Document) []byte {
		ops, inlined := b.SetBlob(slot, mime, data)
		if !inlined {
			return nil
		}
		ok = true
		return ops
	})
	return ok
}

// SetBlobRef sets a store-backed blob ref at key from a 16-byte id handle, mime,
// and size — the content is fetched by id, not carried in the op.
func (m *CrdtMap) SetBlobRef(key string, id [16]byte, mime string, size uint64) {
	slot := m.slot(key)
	m.doc.mutate(func(b *Document) []byte { return b.SetBlobRef(slot, id, mime, size) })
}

// GetBlob reads the BlobRef at key, or false when the slot holds no blob.
func (m *CrdtMap) GetBlob(key string) (BlobRef, bool) {
	return m.doc.backend.GetBlob(m.slot(key))
}

// Observe subscribes to changes under this map's subtree (local edits and
// applied remote updates); returns a function that unsubscribes.
func (m *CrdtMap) Observe(cb func(ChangeEvent)) func() {
	return m.doc.addObserver(EncodePath(m.path), cb)
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

// Observe subscribes to changes to this list (local edits and applied remote
// updates); returns a function that unsubscribes.
func (l *CrdtList) Observe(cb func(ChangeEvent)) func() {
	return l.doc.addObserver(EncodePath(l.path), cb)
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

// Observe subscribes to changes to this text (local edits and applied remote
// updates); returns a function that unsubscribes.
func (t *CrdtText) Observe(cb func(ChangeEvent)) func() {
	return t.doc.addObserver(EncodePath(t.path), cb)
}
