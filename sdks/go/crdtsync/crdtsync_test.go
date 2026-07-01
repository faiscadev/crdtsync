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
