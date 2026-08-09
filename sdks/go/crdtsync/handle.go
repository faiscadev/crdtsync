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
	"sync"
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

// Doc is a CRDT replica with a single root map, edited through live typed
// handles. A local Doc is backed by its own Document; two that exchange each
// other's update ops (forwarded via OnUpdate) converge. A networked Doc is
// backed by one channel of a wire Client — its edits frame for the wire and its
// provider syncs them. The low-level path API stays available underneath the
// handle graph (Doc.Backend()).
//
// A Doc is safe for concurrent use: every operation runs under its lock, and a
// networked Doc shares that lock with the provider driving its socket, so an
// inbound frame and a local edit never touch the replica at once. Listener
// callbacks always run with the lock released, so a listener may edit the doc or
// drive its provider. Transact is the one exception — the atomic group is
// doc-wide, so an edit another goroutine makes while it is open joins the group.
type Doc struct {
	mu sync.Mutex

	backend Backend
	// wire transmits an edit's bytes as they are authored. Nil for a local doc,
	// whose updates travel through OnUpdate instead.
	wire            func([]byte)
	updateListeners listenerList[func(UpdateEvent)]
	observers       listenerList[observer]
	repairListeners listenerList[func(RepairEvent)]
	transacting     bool
	// txSawRemote records that a peer's frame landed while a transaction was open.
	// The transaction's pre-edit snapshot then predates work that was not its own,
	// so the diff it would produce is not this transaction's change set.
	txSawRemote bool
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

// newNetworkedDoc builds a Doc over a provider-supplied networked backend. wire
// carries each authored edit's frame to the socket.
func newNetworkedDoc(backend Backend, wire func([]byte)) *Doc {
	return &Doc{backend: backend, wire: wire}
}

// Backend returns the replica underneath the handle graph — the byte-path
// power-user surface. Drive it only from the goroutine that holds the doc; it
// carries no lock of its own.
func (d *Doc) Backend() Backend { return d.backend }

// Close frees the document. Safe to call more than once. A networked doc's
// backend is owned by its provider, so closing the doc leaves the wire session
// alone — close the provider instead.
func (d *Doc) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.backend.Close()
}

// EncodeState serializes the whole replica to a canonical snapshot. Empty only
// when the backing replica is gone — a networked doc whose provider has closed.
func (d *Doc) EncodeState() []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.backend.EncodeState()
}

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

// Transact runs fn's edits as an atomic group — they apply together on every replica
// served the zone they fall in, ride the wire as a single batch, and fire one update.
// Edits spanning two zones form one group per zone, since a transaction stays inside
// one zone. Nested calls flatten into the outermost transaction. The group is doc-wide, so an edit another goroutine makes
// while it is open joins it.
func (d *Doc) Transact(fn func()) {
	d.mu.Lock()
	if d.transacting {
		d.mu.Unlock()
		fn()
		return
	}
	var before []byte
	if d.observingLocked() {
		before = d.backend.EncodeState()
	}
	d.transacting = true
	d.txSawRemote = false
	d.backend.BeginAtomic()
	d.mu.Unlock()

	// Commit even if fn panics, so a failed transaction never strands the group
	// open and silently swallows every later edit.
	defer d.commitTransaction(before)
	fn()
}

func (d *Doc) commitTransaction(before []byte) {
	d.mu.Lock()
	d.transacting = false
	if d.txSawRemote {
		// A peer's frame folded in mid-transaction, so the snapshot taken when the
		// group opened no longer isolates this transaction's own work. Report the
		// ops with no change set rather than crediting the peer's edit to it — the
		// frame already fired its own remote event with the right changes.
		before = nil
		d.txSawRemote = false
	}
	ops := d.backend.CommitAtomic()
	if len(ops) == 0 {
		d.mu.Unlock()
		return
	}
	plan := d.planDispatchLocked("local", ops, before)
	d.mu.Unlock()
	plan.run()
}

// OnUpdate subscribes to applied changes to the document; returns a function
// that unsubscribes.
func (d *Doc) OnUpdate(cb func(UpdateEvent)) func() {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.guarded(d.updateListeners.add(cb))
}

// OnRepair subscribes to the schema-repair signal (fires only once a schema is
// bound via SetSchema): the located paths whose repaired reading changed against
// the schema after an edit. Returns a function that unsubscribes.
func (d *Doc) OnRepair(cb func(RepairEvent)) func() {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.guarded(d.repairListeners.add(cb))
}

// guarded wraps an unsubscribe so removal runs under the doc's lock, like the
// registration it undoes.
func (d *Doc) guarded(off func()) func() {
	return func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		off()
	}
}

// SetSchema binds a schema (its JSON, as bytes) to this replica, returning
// whether it bound. A bound schema gives named marks their declared flavor and
// turns on the OnRepair signal.
func (d *Doc) SetSchema(schema []byte) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.backend.SetSchema(schema)
}

// ApplyUpdate folds a peer's update ops into this replica. Local docs only — a
// networked doc syncs through its provider and refuses with -1.
//
// The two counts separate an op that did not apply yet from one that never will.
// applied is what the fold took as the ops arrived; one it did not take may be a
// duplicate, or be waiting — buffered until a create makes its target reachable or
// its transaction group resolves, which a later update does, including one later
// in this same batch (released that way, it is not counted). refused is what no
// replica will ever hold, which is a bug in whoever wrote it: a peer reached
// offline, directly, or over a byte pipe the app carries itself has no server
// between it and this fold to reject such an op first, so a non-zero refused is
// the only signal the app gets that a peer's edits are dropped for good. A refused
// op does not hold back the rest of the batch.
func (d *Doc) ApplyUpdate(ops []byte) (applied, refused int) {
	d.mu.Lock()
	var before []byte
	if d.observingLocked() {
		before = d.backend.EncodeState()
	}
	applied, refused = d.backend.Apply(ops)
	if applied <= 0 {
		d.mu.Unlock()
		return applied, refused
	}
	plan := d.planDispatchLocked("remote", ops, before)
	d.mu.Unlock()
	plan.run()
	return applied, refused
}

// applyRemote brackets a provider-driven inbound receive with reactivity. The
// caller holds the doc's lock; the returned func delivers the events and must
// run once it is released.
func (d *Doc) applyRemote(receive func()) func() {
	if d.transacting {
		d.txSawRemote = true
	}
	var before []byte
	if d.observingLocked() {
		before = d.backend.EncodeState()
	}
	receive()
	if before == nil {
		// Nothing observing (or no snapshot to diff against): the frame still
		// folded, there is just no change set to report.
		return func() {}
	}
	plan := d.planDispatchLocked("remote", nil, before)
	return plan.run
}

// mutate runs one edit, transmits its bytes, and dispatches them as a local
// update. Inside a transaction the edit just accumulates; Transact's commit
// transmits and dispatches once.
func (d *Doc) mutate(run func(Backend) []byte) ([]byte, error) {
	d.mu.Lock()
	if d.transacting {
		run(d.backend)
		refused := d.backend.MintRefused()
		d.mu.Unlock()
		return nil, refusal(refused)
	}
	var before []byte
	if d.observingLocked() {
		before = d.backend.EncodeState()
	}
	ops := run(d.backend)
	// Read straight after the edit, never before: the core clears the latch as
	// each intention opens, so this answers for the edit just made.
	refused := d.backend.MintRefused()
	if len(ops) == 0 {
		d.mu.Unlock()
		return ops, refusal(refused)
	}
	plan := d.planDispatchLocked("local", ops, before)
	d.mu.Unlock()
	// The ops are stamped into this replica and its outbox already, so they reach
	// the room even when the edit that followed them could not mint.
	plan.run()
	return ops, refusal(refused)
}

// refusal turns the backend's reading into the sentinel the mutators return.
func refusal(refused bool) error {
	if refused {
		return ErrMintExhausted
	}
	return nil
}

// Err reports ErrMintExhausted when the edit most recently made through this
// document was refused for want of an id, and nil otherwise.
//
// It is the seam for the mutators that return no error of their own — the XML
// handles chain, so an error return there would cost the chaining the API is
// built on, and Delete/Insert have nothing to say but this. A mutator that does
// return an error returns this same sentinel directly; both read the one
// condition, so a caller may use whichever suits its call site.
//
// It is intention-scoped, not per-edit: a Transact body is one intention and so is
// an atomic group, commit included, so a refusal inside one stays raised for the
// rest of it and is cleared by the next intention rather than by a later edit
// within this one. Read it before the next edit — that edit's intention clears the
// latch, so a later reading answers for it and not for the call in question.
//
// It answers for edits that reached the replica. A call refused at Go's own type
// boundary — Set, Insert or Append handed a value with no CRDT scalar — never
// resolves a path, opens no intention, and leaves the previous reading standing;
// those return their error directly, which is the answer to read there. Filed as
// C105.
//
// It answers for the edit most recently made on this document by any goroutine,
// so it is meaningful only where the document is edited from one. A concurrent
// edit between a refused call and its Err clears the answer; the mutators that
// return an error hand it back per call and are unaffected.
func (d *Doc) Err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return refusal(d.backend.MintRefused())
}

// observingLocked reports whether any update listener or subtree observer is
// subscribed — a snapshot+diff runs only then, so an unobserved doc pays nothing.
func (d *Doc) observingLocked() bool {
	return d.updateListeners.len() > 0 || d.observers.len() > 0
}

// dispatchPlan is the delivery half of an applied edit: the frame to transmit
// and the listener snapshots with the events they receive, all captured under
// the doc's lock so delivery itself runs with it released — a listener is free
// to edit the doc or drive its provider.
type dispatchPlan struct {
	wire       func([]byte)
	ops        []byte
	updates    []func(UpdateEvent)
	update     UpdateEvent
	fireUpdate bool
	observers  []observer
	raws       []changeWithPath
	origin     string
	repairs    []func(RepairEvent)
	repair     RepairEvent
}

func (p dispatchPlan) run() {
	if p.wire != nil {
		p.wire(p.ops)
	}
	if p.fireUpdate {
		for _, l := range p.updates {
			l(p.update)
		}
	}
	for _, obs := range p.observers {
		var matched []EventChange
		for _, r := range p.raws {
			if pathStartsWith(r.pathBytes, obs.prefix) {
				matched = append(matched, r.change)
			}
		}
		if len(matched) > 0 {
			obs.cb(ChangeEvent{Origin: p.origin, Changes: matched})
		}
	}
	if len(p.repair.Paths) > 0 {
		for _, l := range p.repairs {
			l(p.repair)
		}
	}
}

// planDispatchLocked computes an applied edit's change set and captures who
// receives it. A local edit's ops are transmitted; an inbound frame carries nil
// ops and is already on the wire.
func (d *Doc) planDispatchLocked(origin string, ops []byte, before []byte) dispatchPlan {
	plan := dispatchPlan{ops: ops, origin: origin}
	if origin == "local" {
		plan.wire = d.wire
	}
	if before != nil {
		plan.raws = d.computeChangesLocked(before)
	}
	changes := make([]EventChange, len(plan.raws))
	for i, r := range plan.raws {
		changes[i] = r.change
	}
	// A remote frame that changed nothing (an ack, an awareness update) fires no
	// update; a local edit always reports its ops.
	if origin == "local" || len(changes) > 0 {
		plan.fireUpdate = true
		plan.update = UpdateEvent{Origin: origin, Ops: ops, Changes: changes}
		plan.updates = d.updateListeners.snapshot()
	}
	plan.observers = d.observers.snapshot()
	plan.repairs, plan.repair = d.drainRepairsLocked()
	return plan
}

// computeChangesLocked diffs the replica against a pre-edit snapshot and
// re-marshals each raw change into an ergonomic EventChange plus its framed path
// (for observer prefix matching).
func (d *Doc) computeChangesLocked(before []byte) []changeWithPath {
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

// drainRepairsLocked takes the schema-repair signal and who receives it. It
// drains only when observed — the drain reseeds the baseline, so draining
// unobserved would lose the signal (and TakeRepairs is empty until a schema is
// bound).
func (d *Doc) drainRepairsLocked() ([]func(RepairEvent), RepairEvent) {
	if d.repairListeners.len() == 0 {
		return nil, RepairEvent{}
	}
	raw := d.backend.TakeRepairs()
	if len(raw) == 0 {
		return nil, RepairEvent{}
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
	return d.repairListeners.snapshot(), RepairEvent{Paths: paths}
}

func (d *Doc) addObserver(prefix []byte, cb func(ChangeEvent)) func() {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.guarded(d.observers.add(observer{prefix: prefix, cb: cb}))
}

func (d *Doc) containerKindLocked(slot [][]byte) string {
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
	_, err = m.doc.mutate(func(b Backend) []byte { return b.SetScalar(slot, scalar) })
	return err
}

// Get reads key: a native scalar for a leaf, a BlobRef for a blob, a nested
// handle for a container slot, or (nil, false) when the slot is empty.
func (m *CrdtMap) Get(key string) (any, bool) {
	m.doc.mu.Lock()
	defer m.doc.mu.Unlock()
	return m.getLocked(key)
}

func (m *CrdtMap) getLocked(key string) (any, bool) {
	slot := m.slot(key)
	if blob, ok := m.doc.backend.GetBlob(slot); ok {
		return blob, true
	}
	if scalar, ok := m.doc.backend.GetScalar(slot); ok {
		return unmarshalValue(scalar), true
	}
	kind := m.doc.containerKindLocked(slot)
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
	m.doc.mutate(func(b Backend) []byte { return b.Delete(slot) })
}

// Has reports whether key holds a leaf, a blob, or a container.
func (m *CrdtMap) Has(key string) bool {
	m.doc.mu.Lock()
	defer m.doc.mu.Unlock()
	slot := m.slot(key)
	if _, ok := m.doc.backend.GetScalar(slot); ok {
		return true
	}
	if _, ok := m.doc.backend.GetBlob(slot); ok {
		return true
	}
	return m.doc.containerKindLocked(slot) != ""
}

func (m *CrdtMap) rawKeysLocked() [][]byte {
	keys, _ := m.doc.backend.MapKeys(m.path)
	return keys
}

// Keys returns the live slot keys, rendered best-effort as utf-8 strings.
func (m *CrdtMap) Keys() []string {
	m.doc.mu.Lock()
	defer m.doc.mu.Unlock()
	raw := m.rawKeysLocked()
	out := make([]string, len(raw))
	for i, k := range raw {
		out[i] = keyString(k)
	}
	return out
}

// Entries returns the live (key, value) pairs. Values are read by the raw key
// bytes, so a non-utf-8 (binary) key's value is never lost.
func (m *CrdtMap) Entries() []Entry {
	m.doc.mu.Lock()
	defer m.doc.mu.Unlock()
	raw := m.rawKeysLocked()
	out := make([]Entry, 0, len(raw))
	for _, k := range raw {
		v, _ := m.getLocked(keyString(k))
		out = append(out, Entry{Key: keyString(k), Value: v})
	}
	return out
}

// Len returns the number of live slots.
func (m *CrdtMap) Len() int {
	m.doc.mu.Lock()
	defer m.doc.mu.Unlock()
	return len(m.rawKeysLocked())
}

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
// false when the replica could not mint the ids the write needed, and false on a
// local document when data exceeds the inline ceiling — upload that out of band
// with UploadBlob and set the returned handle via SetBlobRef. Doc.Err reports
// ErrMintExhausted for the first and nil for the second.
//
// The size reading is the local document's. A networked Doc derives it from the
// frame its edit surface returns, and that frame is non-empty even for an edit
// that enqueued nothing, so an over-size blob there reads as stored. Filed as
// C109; the mint reading is correct against either backend.
func (m *CrdtMap) SetBlob(key, mime string, data []byte) bool {
	slot := m.slot(key)
	ok := false
	_, err := m.doc.mutate(func(b Backend) []byte {
		ops, inlined := b.SetBlob(slot, mime, data)
		if !inlined {
			return nil
		}
		ok = true
		return ops
	})
	// Fitting under the ceiling is not the same as landing: a blob that inlines
	// still needs an id, so a refused mint stores nothing and the answer is false.
	// Doc.Err carries which of the two happened.
	return ok && err == nil
}

// SetBlobRef sets a store-backed blob ref at key from a 16-byte id handle, mime,
// and size — the content is fetched by id, not carried in the op.
func (m *CrdtMap) SetBlobRef(key string, id [16]byte, mime string, size uint64) {
	slot := m.slot(key)
	m.doc.mutate(func(b Backend) []byte { return b.SetBlobRef(slot, id, mime, size) })
}

// GetBlob reads the BlobRef at key, or false when the slot holds no blob.
func (m *CrdtMap) GetBlob(key string) (BlobRef, bool) {
	m.doc.mu.Lock()
	defer m.doc.mu.Unlock()
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

// Insert inserts a scalar item at a live index (clamped into range). A negative
// index counts from the end. Returns an error for an unsupported value type.
func (l *CrdtList) Insert(index int, value any) error {
	item, err := marshalValue(value)
	if err != nil {
		return err
	}
	// Resolve the index against the same live length the insert lands in — an
	// index read in an earlier critical section could be stale by the time the
	// item is placed.
	_, err = l.doc.mutate(func(b Backend) []byte {
		at := index
		n := l.lenLocked()
		if at < 0 {
			at += n
			if at < 0 {
				at = 0
			}
		}
		if at > n {
			at = n
		}
		return b.ListInsert(l.path, uint(at), item)
	})
	return err
}

// Append appends a scalar item.
func (l *CrdtList) Append(value any) error {
	item, err := marshalValue(value)
	if err != nil {
		return err
	}
	_, err = l.doc.mutate(func(b Backend) []byte {
		return b.ListInsert(l.path, uint(l.lenLocked()), item)
	})
	return err
}

// Delete tombstones the live item at index. A negative index counts from the
// end. Returns an error when index is out of range.
func (l *CrdtList) Delete(index int) error {
	var err error
	_, mintErr := l.doc.mutate(func(b Backend) []byte {
		var idx uint
		idx, err = l.checkedLocked(index)
		if err != nil {
			// An index off the end names no live item, which the core answers with
			// an *inert* edit rather than with nothing at all — and reaching that
			// seam is what opens the intention this call's refusal reading answers
			// for. The live length is out of range by definition, so the call is
			// inert whatever index was asked for.
			idx = uint(l.lenLocked())
		}
		return b.ListDelete(l.path, idx)
	})
	if err != nil {
		return err
	}
	return mintErr
}

// Get reads the item at index. The bool is false when index is out of range.
func (l *CrdtList) Get(index int) (any, bool) {
	l.doc.mu.Lock()
	defer l.doc.mu.Unlock()
	return l.getLocked(index)
}

func (l *CrdtList) getLocked(index int) (any, bool) {
	idx, err := l.checkedLocked(index)
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
	l.doc.mu.Lock()
	defer l.doc.mu.Unlock()
	return l.lenLocked()
}

func (l *CrdtList) lenLocked() int {
	n, _ := l.doc.backend.ListLen(l.path)
	return int(n)
}

// Values returns the live items in order.
func (l *CrdtList) Values() []any {
	l.doc.mu.Lock()
	defer l.doc.mu.Unlock()
	n := l.lenLocked()
	out := make([]any, 0, n)
	for i := 0; i < n; i++ {
		v, _ := l.getLocked(i)
		out = append(out, v)
	}
	return out
}

// Observe subscribes to changes to this list (local edits and applied remote
// updates); returns a function that unsubscribes.
func (l *CrdtList) Observe(cb func(ChangeEvent)) func() {
	return l.doc.addObserver(EncodePath(l.path), cb)
}

func (l *CrdtList) checkedLocked(index int) (uint, error) {
	n := l.lenLocked()
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
	t.doc.mutate(func(b Backend) []byte { return b.TextInsert(t.path, uint(index), text) })
}

// Delete tombstones count codepoints from index.
func (t *CrdtText) Delete(index, count int) {
	t.doc.mutate(func(b Backend) []byte { return b.TextDelete(t.path, uint(index), uint(count)) })
}

// String returns the text content.
func (t *CrdtText) String() string {
	t.doc.mu.Lock()
	defer t.doc.mu.Unlock()
	s, _ := t.doc.backend.TextGet(t.path)
	return s
}

// Len returns the codepoint length of the text.
func (t *CrdtText) Len() int {
	t.doc.mu.Lock()
	defer t.doc.mu.Unlock()
	n, _ := t.doc.backend.TextLen(t.path)
	return int(n)
}

// Observe subscribes to changes to this text (local edits and applied remote
// updates); returns a function that unsubscribes.
func (t *CrdtText) Observe(cb func(ChangeEvent)) func() {
	return t.doc.addObserver(EncodePath(t.path), cb)
}
