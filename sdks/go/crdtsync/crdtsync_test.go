package crdtsync

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func cid(first byte) []byte {
	b := make([]byte, 16)
	b[0] = first
	return b
}

// newDoc opens a document, failing the test (rather than nil-panicking on a
// deferred Close) if construction errors.
func newDoc(t *testing.T, first byte) *Document {
	t.Helper()
	d, err := New(cid(first))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d
}

func key(s string) []byte { return []byte(s) }

func path(keys ...string) [][]byte {
	p := make([][]byte, len(keys))
	for i, k := range keys {
		p[i] = key(k)
	}
	return p
}

func TestRegisterReadsBackAndConverges(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()

	ops := a.RegisterInt(path("age"), 30)
	if v, ok := a.GetInt(path("age")); !ok || v != 30 {
		t.Fatalf("local read: got %d ok=%v", v, ok)
	}
	if n := b.Apply(ops); n != 1 {
		t.Fatalf("apply: got %d", n)
	}
	if v, ok := b.GetInt(path("age")); !ok || v != 30 {
		t.Fatalf("peer read: got %d ok=%v", v, ok)
	}
}

func TestMissingKeyIsAbsent(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	if _, ok := a.GetInt(path("nope")); ok {
		t.Fatal("expected absent")
	}
}

func TestCounterAccumulatesAcrossReplicas(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()

	oa := a.Inc(path("n"), 3)
	ob := b.Inc(path("n"), 4)
	b.Apply(oa)
	a.Apply(ob)

	va, _ := a.GetCounter(path("n"))
	vb, _ := b.GetCounter(path("n"))
	if va != 7 || vb != 7 {
		t.Fatalf("counter diverged: a=%d b=%d", va, vb)
	}
}

func TestCounterDecrementsAcrossReplicas(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()

	up := a.Inc(path("stock"), 10)
	down := a.Dec(path("stock"), 4)
	b.Apply(up)
	b.Apply(down)

	va, _ := a.GetCounter(path("stock"))
	vb, _ := b.GetCounter(path("stock"))
	if va != 6 || vb != 6 {
		t.Fatalf("counter diverged: a=%d b=%d", va, vb)
	}
}

func TestNestedPathConverges(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()

	p := path("profile", "stats", "score")
	b.Apply(a.RegisterInt(p, 7))
	if v, ok := b.GetInt(p); !ok || v != 7 {
		t.Fatalf("nested read: got %d ok=%v", v, ok)
	}
}

func TestBytesRoundTrip(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	want := []byte{0, 1, 255, 0}
	a.SetBytes(path("blob"), want)
	got, ok := a.GetBytes(path("blob"))
	if !ok || !bytes.Equal(got, want) {
		t.Fatalf("bytes: got %v ok=%v", got, ok)
	}
}

func TestListConvergesAndNoOpDeleteInert(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()

	p := path("board", "cards")
	b.Apply(a.ListInsert(p, 0, key("x")))
	b.Apply(a.ListInsert(p, 1, key("y")))
	if n, ok := b.ListLen(p); !ok || n != 2 {
		t.Fatalf("list len: got %d ok=%v", n, ok)
	}
	if v, _ := b.ListGet(p, 0); !bytes.Equal(v, key("x")) {
		t.Fatalf("list[0]: got %v", v)
	}
	// A delete of an absent list is a no-op: no ops, no container.
	if ops := a.ListDelete(path("ghost"), 0); len(ops) != 0 {
		t.Fatalf("no-op delete emitted %d bytes", len(ops))
	}
	if _, ok := a.ListLen(path("ghost")); ok {
		t.Fatal("ghost list must not exist")
	}
}

func TestTextConvergesAndDeletes(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()

	p := path("doc", "title")
	b.Apply(a.TextInsert(p, 0, "héllo"))
	if n, ok := b.TextLen(p); !ok || n != 5 {
		t.Fatalf("text len: got %d ok=%v", n, ok)
	}
	if s, _ := b.TextGet(p); s != "héllo" {
		t.Fatalf("text: got %q", s)
	}
	b.Apply(a.TextDelete(p, 1, 3))
	if s, _ := b.TextGet(p); s != "ho" {
		t.Fatalf("after delete: got %q", s)
	}
}

func TestApplyRejectsGarbage(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	if rc := a.Apply([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}); rc != -1 {
		t.Fatalf("garbage apply: got %d", rc)
	}
}

func TestEncodePathShape(t *testing.T) {
	got := EncodePath(path("ab", "c"))
	want := []byte{2, 0, 0, 0, 'a', 'b', 1, 0, 0, 0, 'c'}
	if !bytes.Equal(got, want) {
		t.Fatalf("encode path: got %v", got)
	}
}

func TestSnapshotRoundTrips(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	a.RegisterInt(path("age"), 30)
	a.Inc(path("hits"), 5)

	back, err := DecodeState(a.EncodeState())
	if err != nil {
		t.Fatalf("DecodeState: %v", err)
	}
	defer back.Close()
	if v, ok := back.GetInt(path("age")); !ok || v != 30 {
		t.Fatalf("age: got %d ok=%v", v, ok)
	}
	if v, ok := back.GetCounter(path("hits")); !ok || v != 5 {
		t.Fatalf("hits: got %d ok=%v", v, ok)
	}
}

func TestDecodedDocumentDedupsAndConverges(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	reg := a.RegisterInt(path("age"), 30)

	back, err := DecodeState(a.EncodeState())
	if err != nil {
		t.Fatalf("DecodeState: %v", err)
	}
	defer back.Close()

	// A replay of the covered op is a no-op; a later peer op still lands.
	if n := back.Apply(reg); n != 0 {
		t.Fatalf("replay applied %d ops, want 0", n)
	}
	b := newDoc(t, 2)
	defer b.Close()
	b.Apply(reg)
	hit := b.Inc(path("hits"), 4)
	if n := back.Apply(hit); n != 1 {
		t.Fatalf("later op applied %d ops, want 1", n)
	}
	if v, ok := back.GetCounter(path("hits")); !ok || v != 4 {
		t.Fatalf("hits: got %d ok=%v", v, ok)
	}
}

func TestDecodeGarbageStateErrors(t *testing.T) {
	if _, err := DecodeState([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err == nil {
		t.Fatal("DecodeState on garbage: want error, got nil")
	}
}

func TestEncodeStateIsCanonical(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	a.RegisterInt(path("age"), 30)
	back, err := DecodeState(a.EncodeState())
	if err != nil {
		t.Fatalf("DecodeState: %v", err)
	}
	defer back.Close()
	if !bytes.Equal(a.EncodeState(), back.EncodeState()) {
		t.Fatal("re-encode of a decoded snapshot is not canonical")
	}
}

// newClient opens a wire client, failing the test on error.
func newClient(t *testing.T, first byte) *Client {
	t.Helper()
	c, err := NewClient(cid(first))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func TestDeclaredAppRidesAlongInTheHelloFrame(t *testing.T) {
	c := newClient(t, 1)
	defer c.Close()

	// A bare client opens as a relay — no app named in the frame.
	if bytes.Contains(c.Hello(), []byte("app-x")) {
		t.Fatalf("bare client named an app in its Hello")
	}
	// Declaring an app names it in the next Hello.
	c.DeclareApp([]byte("app-x"), 3)
	if !bytes.Contains(c.Hello(), []byte("app-x")) {
		t.Fatalf("declared app missing from Hello")
	}
}

func TestServerAdvertisedSchemaIsReadable(t *testing.T) {
	// SchemaAdvert: tag 21, u32 version, u32 length prefix, bytes.
	advert := func(version uint32, body []byte) []byte {
		frame := make([]byte, 9+len(body))
		frame[0] = 21
		binary.LittleEndian.PutUint32(frame[1:], version)
		binary.LittleEndian.PutUint32(frame[5:], uint32(len(body)))
		copy(frame[9:], body)
		return frame
	}

	c := newClient(t, 1)
	defer c.Close()

	// Nothing advertised yet.
	if v, ok := c.ActiveSchemaVersion(); ok || v != 0 {
		t.Fatalf("fresh client reported an active schema version %d", v)
	}
	if got, ok := c.ActiveSchema(); ok || got != nil {
		t.Fatalf("fresh client reported active schema bytes")
	}

	// Folding a SchemaAdvert records the served version and its bytes.
	if rc, _ := c.Receive(advert(4, []byte("schema-body"))); rc != 1 {
		t.Fatalf("advert not applied: rc=%d", rc)
	}
	if v, ok := c.ActiveSchemaVersion(); !ok || v != 4 {
		t.Fatalf("active version %d ok=%v, want 4 true", v, ok)
	}
	if got, ok := c.ActiveSchema(); !ok || !bytes.Equal(got, []byte("schema-body")) {
		t.Fatalf("active schema %q ok=%v, want schema-body true", got, ok)
	}

	// A later advert supersedes it.
	if rc, _ := c.Receive(advert(5, []byte("next-body"))); rc != 1 {
		t.Fatalf("second advert not applied: rc=%d", rc)
	}
	if v, _ := c.ActiveSchemaVersion(); v != 5 {
		t.Fatalf("active version %d, want 5", v)
	}
	if got, _ := c.ActiveSchema(); !bytes.Equal(got, []byte("next-body")) {
		t.Fatalf("active schema %q, want next-body", got)
	}

	// An empty body is still an advertisement, not "none".
	if rc, _ := c.Receive(advert(6, []byte{})); rc != 1 {
		t.Fatalf("empty-body advert not applied: rc=%d", rc)
	}
	if v, ok := c.ActiveSchemaVersion(); !ok || v != 6 {
		t.Fatalf("active version %d ok=%v, want 6 true", v, ok)
	}
	if got, ok := c.ActiveSchema(); !ok || len(got) != 0 {
		t.Fatalf("active schema %q ok=%v, want empty present", got, ok)
	}
}

func TestClientEditTravelsToAPeer(t *testing.T) {
	a := newClient(t, 1)
	defer a.Close()
	b := newClient(t, 2)
	defer b.Close()

	// Both fresh sessions assign channel 0 to their first subscription.
	ca, _ := a.Subscribe(key("room-1"))
	cb, _ := b.Subscribe(key("room-1"))
	if ca != 0 || cb != 0 {
		t.Fatalf("first channel: got %d and %d, want 0 and 0", ca, cb)
	}

	ops := a.RegisterInt(ca, path("age"), 30)
	if v, ok := a.GetInt(ca, path("age")); !ok || v != 30 {
		t.Fatalf("local read: got (%d,%v), want (30,true)", v, ok)
	}
	if rc, _ := b.Receive(ops); rc != 1 {
		t.Fatalf("receive: got %d, want 1", rc)
	}
	if v, ok := b.GetInt(cb, path("age")); !ok || v != 30 {
		t.Fatalf("peer read: got (%d,%v), want (30,true)", v, ok)
	}
	if seq, ok := b.LastSeenSeq(cb); !ok || seq != 1 {
		t.Fatalf("last seen: got (%d,%v), want (1,true)", seq, ok)
	}
}

func TestClientOutboxDrainsOnAck(t *testing.T) {
	a := newClient(t, 1)
	defer a.Close()
	ca, _ := a.Subscribe(key("room-1"))

	a.RegisterInt(ca, path("age"), 30)
	if n := a.OutboxLen(ca); n != 1 {
		t.Fatalf("outbox after one edit: got %d, want 1", n)
	}
	a.RegisterInt(ca, path("age"), 31)
	if n := a.OutboxLen(ca); n != 2 {
		t.Fatalf("outbox after two edits: got %d, want 2", n)
	}
	if len(a.Resend(ca)) == 0 {
		t.Fatal("resend should replay the unacked tail")
	}

	// An Accepted through u64::MAX drains the outbox: tag 18, u32 channel,
	// u64 frontier.
	accepted := make([]byte, 13)
	accepted[0] = 18
	binary.LittleEndian.PutUint32(accepted[1:], ca)
	binary.LittleEndian.PutUint64(accepted[5:], math.MaxUint64)
	if rc, _ := a.Receive(accepted); rc != 1 {
		t.Fatalf("receive accepted: got %d, want 1", rc)
	}
	if n := a.OutboxLen(ca); n != 0 {
		t.Fatalf("outbox after ack: got %d, want 0", n)
	}
	if len(a.Resend(ca)) != 0 {
		t.Fatal("resend should be empty after a full ack")
	}
}

func TestClientBytesRoundTrip(t *testing.T) {
	a := newClient(t, 1)
	defer a.Close()
	b := newClient(t, 2)
	defer b.Close()
	ca, _ := a.Subscribe(key("room-1"))
	cb, _ := b.Subscribe(key("room-1"))

	b.Receive(a.SetBytes(ca, path("blob"), []byte{0, 1, 0xff}))
	if got, ok := b.GetBytes(cb, path("blob")); !ok || !bytes.Equal(got, []byte{0, 1, 0xff}) {
		t.Fatalf("bytes: got (%v,%v)", got, ok)
	}
}

func TestClientHandshakeAndLifecycle(t *testing.T) {
	c := newClient(t, 1)
	defer c.Close()

	if len(c.Hello()) == 0 || len(c.Auth(key("token"))) == 0 {
		t.Fatal("handshake frames should be non-empty")
	}
	if _, ok := c.Actor(); ok {
		t.Fatal("actor should be absent before AuthOk")
	}

	ch, _ := c.Subscribe(key("room-1"))
	if len(c.SetAwareness(ch, key("cursor"), key("x"))) == 0 {
		t.Fatal("set_awareness should yield a frame")
	}
	if n := c.AwarenessLen(ch); n != 0 {
		t.Fatalf("awareness len: got %d, want 0", n)
	}
	if len(c.Unsubscribe(ch)) == 0 {
		t.Fatal("unsubscribe should yield a frame")
	}
	if _, ok := c.LastSeenSeq(ch); ok {
		t.Fatal("channel should be gone after unsubscribe")
	}
	if len(c.Resume(ch)) != 0 {
		t.Fatal("resume of an unheld channel should be empty")
	}
}

func TestClientVersionRequestsMarshal(t *testing.T) {
	c := newClient(t, 1)
	defer c.Close()

	ch, _ := c.Subscribe(key("room-1"))
	frames := [][]byte{
		c.CreateVersion(ch, key("v1")),
		c.RenameVersion(ch, key("v1"), key("v2")),
		c.DeleteVersion(ch, key("v1")),
		c.ListVersions(ch),
		c.FetchVersion(ch, key("v1")),
	}
	for i, f := range frames {
		if len(f) == 0 {
			t.Fatalf("version request %d should yield a frame", i)
		}
	}
	// Nothing reported until a server reply is folded in.
	if names := c.Versions(ch); len(names) != 0 {
		t.Fatalf("versions: got %d, want 0", len(names))
	}
	if _, ok := c.VersionState(ch, key("v1")); ok {
		t.Fatal("no version state before a fetch reply")
	}
}

func TestClientReceiveRejectsGarbage(t *testing.T) {
	c := newClient(t, 1)
	defer c.Close()
	if rc, code := c.Receive([]byte{0xff, 0xff, 0xff, 0xff}); rc != 0 || code != NoErrorCode {
		t.Fatalf("garbage receive: got rc=%d code=%d, want 0 %d", rc, code, NoErrorCode)
	}
}

func TestServerErrorFrameSurfacesItsCode(t *testing.T) {
	// Error: tag 3, u16 code, u32-prefixed message, u32-prefixed details.
	errFrame := func(code uint16, message []byte) []byte {
		frame := make([]byte, 3+4+len(message)+4)
		frame[0] = 3
		binary.LittleEndian.PutUint16(frame[1:], code)
		binary.LittleEndian.PutUint32(frame[3:], uint32(len(message)))
		copy(frame[7:], message)
		return frame
	}

	c := newClient(t, 1)
	defer c.Close()

	// A server Error is refused (rc 0) and surfaces its code — UpdateRequired (6)
	// is the onUpdateRequired signal.
	if rc, code := c.Receive(errFrame(6, []byte("please update"))); rc != 0 || code != UpdateRequired {
		t.Fatalf("server error: got rc=%d code=%d, want 0 %d", rc, code, UpdateRequired)
	}

	// A normal frame still applies cleanly, carrying no error code.
	ca, _ := c.Subscribe(key("room-1"))
	if rc, code := c.Receive(c.RegisterInt(ca, path("age"), 30)); rc != 1 || code != NoErrorCode {
		t.Fatalf("normal frame: got rc=%d code=%d, want 1 %d", rc, code, NoErrorCode)
	}
}

func newUndo(t *testing.T) *Undo {
	t.Helper()
	u, err := NewUndo()
	if err != nil {
		t.Fatalf("NewUndo: %v", err)
	}
	return u
}

func TestUndoAndRedoARegister(t *testing.T) {
	d := newDoc(t, 1)
	defer d.Close()
	u := newUndo(t)
	defer u.Close()

	u.RegisterInt(d, path("title"), 1)
	u.RegisterInt(d, path("title"), 2)
	if v, _ := d.GetInt(path("title")); v != 2 {
		t.Fatalf("want 2, got %d", v)
	}
	if !u.CanUndo() {
		t.Fatal("expected can-undo")
	}

	u.Undo(d)
	if v, _ := d.GetInt(path("title")); v != 1 {
		t.Fatalf("after undo want 1, got %d", v)
	}
	u.Redo(d)
	if v, _ := d.GetInt(path("title")); v != 2 {
		t.Fatalf("after redo want 2, got %d", v)
	}
	if u.CanRedo() {
		t.Fatal("redo stack should be empty")
	}
}

func TestUndoOfAListInsert(t *testing.T) {
	d := newDoc(t, 1)
	defer d.Close()
	u := newUndo(t)
	defer u.Close()

	u.ListInsert(d, path("items"), 0, []byte("a"))
	if n, _ := d.ListLen(path("items")); n != 1 {
		t.Fatalf("want len 1, got %d", n)
	}
	u.Undo(d)
	if n, _ := d.ListLen(path("items")); n != 0 {
		t.Fatalf("after undo want len 0, got %d", n)
	}
}

func TestUndoOfATextEdit(t *testing.T) {
	d := newDoc(t, 1)
	defer d.Close()
	u := newUndo(t)
	defer u.Close()

	u.TextInsert(d, path("body"), 0, "hi")
	if s, _ := d.TextGet(path("body")); s != "hi" {
		t.Fatalf("want hi, got %q", s)
	}
	u.Undo(d)
	if s, _ := d.TextGet(path("body")); s != "" {
		t.Fatalf("after undo want empty, got %q", s)
	}
}

func TestUndoConvergesOnAPeer(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()
	u := newUndo(t)
	defer u.Close()

	b.Apply(u.Inc(a, path("votes"), 5))
	if v, _ := b.GetCounter(path("votes")); v != 5 {
		t.Fatalf("peer want 5, got %d", v)
	}
	b.Apply(u.Undo(a))
	if v, _ := b.GetCounter(path("votes")); v != 0 {
		t.Fatalf("peer after undo want 0, got %d", v)
	}
}

func TestAtomicTransactionGroupsEditsAndConverges(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()

	a.BeginAtomic()
	// Edits accumulate while recording; each returns no ops of its own.
	if got := a.RegisterInt(path("x"), 1); len(got) != 0 {
		t.Fatalf("recording edit returned %d bytes, want 0", len(got))
	}
	a.RegisterInt(path("y"), 2)
	group := a.CommitAtomic()
	if len(group) == 0 {
		t.Fatal("commit returned no ops")
	}

	b.Apply(group)
	if v, _ := b.GetInt(path("x")); v != 1 {
		t.Fatalf("peer x want 1, got %d", v)
	}
	if v, _ := b.GetInt(path("y")); v != 2 {
		t.Fatalf("peer y want 2, got %d", v)
	}
}

func TestClientAtomicTransactionTravelsToAPeer(t *testing.T) {
	a := newClient(t, 1)
	defer a.Close()
	b := newClient(t, 2)
	defer b.Close()

	ca, _ := a.Subscribe(key("room-1"))
	cb, _ := b.Subscribe(key("room-1"))

	a.BeginAtomic(ca)
	// Edits accumulate while recording; only the commit frame is sent.
	a.RegisterInt(ca, path("x"), 1)
	a.RegisterInt(ca, path("y"), 2)
	frame := a.CommitAtomic(ca)
	if len(frame) == 0 {
		t.Fatal("commit returned no frame")
	}

	b.Receive(frame)
	if v, _ := b.GetInt(cb, path("x")); v != 1 {
		t.Fatalf("peer x want 1, got %d", v)
	}
	if v, _ := b.GetInt(cb, path("y")); v != 2 {
		t.Fatalf("peer y want 2, got %d", v)
	}
}

func TestDiffValueChange(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	a.RegisterInt(path("age"), 30)
	old := a.EncodeState()
	a.RegisterInt(path("age"), 31)
	changes, err := Diff(old, a.EncodeState())
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("want 1 change, got %d", len(changes))
	}
	c := changes[0]
	if c.Op != "value" || !bytes.Equal(c.Path, EncodePath(path("age"))) {
		t.Fatalf("unexpected change %+v", c)
	}
	if c.Old == nil || c.Old.T != "int" || c.Old.Int != 30 {
		t.Fatalf("bad old %+v", c.Old)
	}
	if c.New == nil || c.New.T != "int" || c.New.Int != 31 {
		t.Fatalf("bad new %+v", c.New)
	}
}

func TestDiffCounterAndAdd(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	a.Inc(path("hits"), 3)
	old := a.EncodeState()
	a.Inc(path("hits"), 2)
	a.RegisterInt(path("age"), 9)
	changes, err := Diff(old, a.EncodeState())
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	byOp := map[string]Change{}
	for _, c := range changes {
		byOp[c.Op] = c
	}
	if c := byOp["counter"]; c.OldInt != 3 || c.NewInt != 5 {
		t.Fatalf("bad counter %+v", c)
	}
	if c := byOp["add"]; c.Kind != "register" || !bytes.Equal(c.Path, EncodePath(path("age"))) {
		t.Fatalf("bad add %+v", c)
	}
}

func TestDiffTextAndListRuns(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	a.TextInsert(path("body"), 0, "hi")
	a.ListInsert(path("xs"), 0, key("x"))
	old := a.EncodeState()
	a.TextInsert(path("body"), 2, "!")
	a.ListInsert(path("xs"), 1, key("y"))
	changes, err := Diff(old, a.EncodeState())
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	byOp := map[string]Change{}
	for _, c := range changes {
		byOp[c.Op] = c
	}
	if c := byOp["textInsert"]; c.Text != "!" || c.Index != 2 {
		t.Fatalf("bad text %+v", c)
	}
	if c := byOp["listInsert"]; c.Index != 1 || len(c.Items) != 1 || c.Items[0].Scalar == nil {
		t.Fatalf("bad list %+v", c)
	}
}

func TestDiffIdenticalIsEmpty(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	a.RegisterInt(path("age"), 30)
	state := a.EncodeState()
	changes, err := Diff(state, state)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("want no changes, got %d", len(changes))
	}
}

func TestDiffMalformedErrors(t *testing.T) {
	if _, err := Diff([]byte{0xff, 0xff, 0xff, 0xff}, []byte{0xff, 0xff, 0xff, 0xff}); err == nil {
		t.Fatal("want an error for a malformed snapshot")
	}
}

func TestRelativePositionTracksEditsAndRoundTrips(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()

	p := path("board", "cards")
	a.ListInsert(p, 0, key("a"))
	a.ListInsert(p, 1, key("b"))
	a.ListInsert(p, 2, key("c"))

	// Anchor left of index 2 ("c"), then insert ahead of it.
	pos := a.RelativePosition(p, 2, Left)
	if pos == nil {
		t.Fatal("capture returned nil")
	}
	if i, ok := a.ResolvePosition(p, pos); !ok || i != 2 {
		t.Fatalf("resolve: got %d ok=%v", i, ok)
	}
	a.ListInsert(p, 0, key("z"))
	if i, ok := a.ResolvePosition(p, pos); !ok || i != 3 {
		t.Fatalf("resolve after insert: got %d ok=%v", i, ok)
	}
}

func TestTextRelativePositionRoundTrips(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()

	p := path("doc", "title")
	a.TextInsert(p, 0, "hello")
	pos := a.RelativePosition(p, 5, Left)
	if pos == nil {
		t.Fatal("capture returned nil")
	}
	if i, ok := a.ResolvePosition(p, pos); !ok || i != 5 {
		t.Fatalf("resolve: got %d ok=%v", i, ok)
	}
	a.TextInsert(p, 0, ">>")
	if i, ok := a.ResolvePosition(p, pos); !ok || i != 7 {
		t.Fatalf("resolve after insert: got %d ok=%v", i, ok)
	}
}

func TestRelativePositionOnNonSequenceIsAbsent(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()

	a.RegisterInt(path("age"), 30)
	if pos := a.RelativePosition(path("age"), 0, Left); pos != nil {
		t.Fatal("no anchor on a non-sequence")
	}
	a.ListInsert(path("list"), 0, key("x"))
	pos := a.RelativePosition(path("list"), 0, Left)
	if _, ok := a.ResolvePosition(path("age"), pos); ok {
		t.Fatal("resolve on a non-sequence must fail")
	}
	if _, ok := a.ResolvePosition(path("list"), []byte{0xff, 0xff}); ok {
		t.Fatal("resolve of malformed bytes must fail")
	}
}

// acceptedThrough builds an Accepted frame for channel acknowledging every op
// through frontier: tag 18, u32 channel, u64 frontier.
func acceptedThrough(channel uint32, frontier uint64) []byte {
	frame := make([]byte, 13)
	frame[0] = 18
	binary.LittleEndian.PutUint32(frame[1:], channel)
	binary.LittleEndian.PutUint64(frame[5:], frontier)
	return frame
}

// hasMarkNamed reports whether any resolved mark carries name.
func hasMarkNamed(marks []Mark, name string) bool {
	for _, m := range marks {
		if bytes.Equal(m.Name, []byte(name)) {
			return true
		}
	}
	return false
}

func TestXmlElementReadsBackChildrenAndConverges(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()
	p := path("doc", "body")

	root := a.XmlElement(p, key("body"))
	e := a.XmlInsertElement(p, 0, key("p"))
	txt := a.XmlInsertText(p, 1, "hi")
	if len(e) == 0 || len(txt) == 0 {
		t.Fatal("child inserts emit ops")
	}
	if tag, ok := a.XmlTag(p); !ok || !bytes.Equal(tag, key("body")) {
		t.Fatalf("tag: got %q ok=%v, want body true", tag, ok)
	}
	if n, ok := a.XmlChildrenLen(p); !ok || n != 2 {
		t.Fatalf("children: got %d ok=%v, want 2 true", n, ok)
	}

	for _, ops := range [][]byte{root, e, txt} {
		if n := b.Apply(ops); n < 1 {
			t.Fatalf("apply: got %d", n)
		}
	}
	if tag, ok := b.XmlTag(p); !ok || !bytes.Equal(tag, key("body")) {
		t.Fatalf("peer tag: got %q ok=%v", tag, ok)
	}
	if n, ok := b.XmlChildrenLen(p); !ok || n != 2 {
		t.Fatalf("peer children converge: got %d ok=%v", n, ok)
	}

	// Deleting a child drops the live count on both replicas.
	del := a.XmlChildDelete(p, 1)
	if len(del) == 0 {
		t.Fatal("delete emits ops")
	}
	b.Apply(del)
	if n, _ := a.XmlChildrenLen(p); n != 1 {
		t.Fatalf("author child count after delete: %d", n)
	}
	if n, _ := b.XmlChildrenLen(p); n != 1 {
		t.Fatalf("peer child count after delete: %d", n)
	}
}

func TestXmlMoveRelocatesChildAndConverges(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()
	src := path("a")
	dst := path("b")

	fa := a.XmlFragment(src)
	fb := a.XmlFragment(dst)
	kid := a.XmlInsertElement(src, 0, key("li"))
	if n, _ := a.XmlChildrenLen(src); n != 1 {
		t.Fatalf("src children before move: %d", n)
	}

	mv := a.XmlMove(src, 0, dst, 0)
	if len(mv) == 0 {
		t.Fatal("move emits ops")
	}
	if n, _ := a.XmlChildrenLen(src); n != 0 {
		t.Fatalf("child left the source: src has %d", n)
	}
	if n, _ := a.XmlChildrenLen(dst); n != 1 {
		t.Fatalf("child arrived at dest: dst has %d", n)
	}

	for _, ops := range [][]byte{fa, fb, kid, mv} {
		b.Apply(ops)
	}
	if n, _ := b.XmlChildrenLen(src); n != 0 {
		t.Fatalf("move converges at src: %d", n)
	}
	if n, _ := b.XmlChildrenLen(dst); n != 1 {
		t.Fatalf("move converges at dest: %d", n)
	}
}

func TestXmlReadsOnANonNodeAreAbsent(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	a.RegisterInt(path("age"), 30)
	if _, ok := a.XmlTag(path("age")); ok {
		t.Fatal("a register has no tag")
	}
	if _, ok := a.XmlChildrenLen(path("age")); ok {
		t.Fatal("a register has no children")
	}
	if ins := a.XmlInsertElement(path("age"), 0, key("p")); len(ins) != 0 {
		t.Fatal("insert into a non-node is inert")
	}
}

func TestMarkReadsBackAndChangesAndDeletes(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()
	body := path("body")

	text := a.TextInsert(body, 0, "hello world")
	id, ops := a.Mark(body, 0, Right, 5, Left, key("bold"), Scalar{T: "bool", Bool: true})
	if len(id) != 16 {
		t.Fatalf("a live mark yields a 16-byte id, got %d", len(id))
	}
	if len(ops) == 0 {
		t.Fatal("authoring a mark emits ops")
	}
	if !hasMarkNamed(a.MarksAt(body, 2), "bold") {
		t.Fatal("the covering mark reads back inside its span")
	}
	if hasMarkNamed(a.MarksAt(body, 7), "bold") {
		t.Fatal("no mark past the span")
	}

	// The mark converges onto a peer.
	b.Apply(text)
	b.Apply(ops)
	if !hasMarkNamed(b.MarksAt(body, 2), "bold") {
		t.Fatal("the mark converges")
	}

	// A value change keeps the mark live.
	set := a.MarkSetValue(id, Scalar{T: "int", Int: 9})
	if len(set) == 0 {
		t.Fatal("changing a live mark's value emits ops")
	}
	b.Apply(set)
	if !hasMarkNamed(a.MarksAt(body, 2), "bold") {
		t.Fatal("the mark stays live through a value change")
	}

	// Deleting drops it from the active set on both replicas.
	del := a.MarkDelete(id)
	if len(del) == 0 {
		t.Fatal("deleting a live mark emits ops")
	}
	b.Apply(del)
	if hasMarkNamed(a.MarksAt(body, 2), "bold") {
		t.Fatal("the mark is gone from the author")
	}
	if hasMarkNamed(b.MarksAt(body, 2), "bold") {
		t.Fatal("the delete converges")
	}
}

func TestMarkOnANonSequenceIsInert(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	a.RegisterInt(path("age"), 30)
	id, ops := a.Mark(path("age"), 0, Left, 1, Right, key("x"), Scalar{T: "bool", Bool: true})
	if id != nil || len(ops) != 0 {
		t.Fatalf("a mark on a non-sequence is inert: id=%v ops=%d", id, len(ops))
	}
}

func TestClientXmlEditRoutesThroughTheOutbox(t *testing.T) {
	a := newClient(t, 1)
	defer a.Close()
	b := newClient(t, 2)
	defer b.Close()
	ca, _ := a.Subscribe(key("room-1"))
	cb, _ := b.Subscribe(key("room-1"))
	_ = cb
	doc := path("doc")

	frag := a.XmlFragment(ca, doc)
	ins := a.XmlInsertElement(ca, doc, 0, key("p"))
	if len(frag) == 0 || len(ins) == 0 {
		t.Fatal("client xml edits frame ops")
	}
	if n := a.OutboxLen(ca); n != 2 {
		t.Fatalf("both edits enqueued, got %d", n)
	}

	// The unacknowledged tail replays as one frame and folds into the peer.
	tail := a.Resend(ca)
	if len(tail) == 0 {
		t.Fatal("the tail replays")
	}
	if rc, _ := b.Receive(tail); rc < 1 {
		t.Fatalf("peer applies the replayed xml edits: rc=%d", rc)
	}

	// An ack through the tip drains the outbox.
	accepted := acceptedThrough(ca, math.MaxUint64)
	if rc, _ := a.Receive(accepted); rc != 1 {
		t.Fatalf("ack applied: rc=%d", rc)
	}
	if n := a.OutboxLen(ca); n != 0 {
		t.Fatalf("ack drained the outbox, got %d", n)
	}
}

func TestClientMarkOnAFreshRoomIsInert(t *testing.T) {
	a := newClient(t, 1)
	defer a.Close()
	ca, _ := a.Subscribe(key("room-1"))

	// No sequence exists at the path yet, so the mark author yields no id handle
	// and enqueues nothing to resend.
	id, _ := a.Mark(ca, path("body"), 0, Right, 5, Left, key("bold"), Scalar{T: "bool", Bool: true})
	if id != nil {
		t.Fatalf("a mark over an absent sequence yields no id, got %v", id)
	}
	if n := a.OutboxLen(ca); n != 0 {
		t.Fatalf("nothing enqueued, got %d", n)
	}
	// A set-value / delete on an unknown handle enqueues nothing either.
	a.MarkSetValue(ca, make([]byte, 16), Scalar{T: "int", Int: 1})
	a.MarkDelete(ca, make([]byte, 16))
	if n := a.OutboxLen(ca); n != 0 {
		t.Fatalf("edits on an unknown mark enqueue nothing, got %d", n)
	}
}

func TestDiffRoundTripsThroughDecode(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()

	// An XmlElement with an attr, plus a Text to annotate.
	a.XmlElement(path("doc"), key("section"))
	a.SetBytes(path("doc", "class"), key("a"))
	a.TextInsert(path("body"), 0, "hello world")
	old := a.EncodeState()

	// Change the attr and add a mark over the text.
	a.SetBytes(path("doc", "class"), key("b"))
	if id, _ := a.Mark(path("body"), 0, Right, 5, Left, key("bold"), Scalar{T: "bool", Bool: true}); len(id) != 16 {
		t.Fatal("the mark authored")
	}
	newState := a.EncodeState()

	// The opaque diff decodes both directly and through the boundary decoder to
	// the same change list — the mark-change variants cross intact.
	raw := DiffEncode(old, newState)
	if len(raw) == 0 {
		t.Fatal("DiffEncode produced no bytes")
	}
	direct, err := Diff(old, newState)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	decoded, err := DiffDecode(raw)
	if err != nil {
		t.Fatalf("DiffDecode: %v", err)
	}
	if len(direct) != len(decoded) {
		t.Fatalf("round-trip length mismatch: %d vs %d", len(direct), len(decoded))
	}

	var sawAttr, sawMark bool
	for _, c := range decoded {
		if c.Op == "value" && bytes.Equal(c.Path, EncodePath(path("doc", "class"))) {
			if c.New != nil && c.New.T == "bytes" && bytes.Equal(c.New.Bytes, key("b")) {
				sawAttr = true
			}
		}
		if c.Op == "markAdded" && bytes.Equal(c.Name, key("bold")) {
			if len(c.ID) != 16 || len(c.Seq) != 16 {
				t.Fatalf("mark change missing ids: %+v", c)
			}
			sawMark = true
		}
	}
	if !sawAttr {
		t.Fatal("the xml attr change did not round-trip as a Value")
	}
	if !sawMark {
		t.Fatal("the mark did not round-trip as a markAdded")
	}
}

func TestDiffDecodeRejectsGarbage(t *testing.T) {
	if _, err := DiffDecode([]byte{0xff, 0xff, 0xff, 0xff}); err == nil {
		t.Fatal("want an error for a malformed diff buffer")
	}
}

const testSchema = `{
    "schema": "notes", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "title": "Title" } },
        "Title": { "kind": "register", "min": 0, "max": 280 }
    }
}`

func TestSchemaBindsAndReportsARepairOnce(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()

	if !a.SetSchema([]byte(testSchema)) {
		t.Fatal("a valid schema binds")
	}
	// Malformed schema bytes reject cleanly.
	if a.SetSchema([]byte("not json {")) {
		t.Fatal("malformed schema must not bind")
	}

	// A conforming edit reports nothing.
	a.RegisterInt(path("title"), 42)
	if r := a.TakeRepairs(); len(r) != 0 {
		t.Fatalf("a conforming edit reports nothing, got %d", len(r))
	}

	// An out-of-range write reports its located path once.
	a.RegisterInt(path("title"), 999)
	reported := a.TakeRepairs()
	if len(reported) != 1 || len(reported[0]) != 1 {
		t.Fatalf("want one one-step repair path, got %+v", reported)
	}
	step := reported[0][0]
	if step.IsIndex || !bytes.Equal(step.Key, key("title")) {
		t.Fatalf("repair step: got %+v, want Key(title)", step)
	}

	// The settle-point contract: a second drain is empty.
	if again := a.TakeRepairs(); len(again) != 0 {
		t.Fatalf("a settled repair does not report twice, got %d", len(again))
	}
}
