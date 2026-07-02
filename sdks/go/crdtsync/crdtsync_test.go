package crdtsync

import (
	"bytes"
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
	if rc := b.Receive(ops); rc != 1 {
		t.Fatalf("receive: got %d, want 1", rc)
	}
	if v, ok := b.GetInt(cb, path("age")); !ok || v != 30 {
		t.Fatalf("peer read: got (%d,%v), want (30,true)", v, ok)
	}
	if seq, ok := b.LastSeenSeq(cb); !ok || seq != 1 {
		t.Fatalf("last seen: got (%d,%v), want (1,true)", seq, ok)
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

func TestClientReceiveRejectsGarbage(t *testing.T) {
	c := newClient(t, 1)
	defer c.Close()
	if rc := c.Receive([]byte{0xff, 0xff, 0xff, 0xff}); rc != 0 {
		t.Fatalf("garbage receive: got %d, want 0", rc)
	}
}
