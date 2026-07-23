package crdtsync

import (
	"bytes"
	"testing"
)

func newErgoDoc(t *testing.T, first byte) *Doc {
	t.Helper()
	d, err := NewDocWithClientID(cid(first))
	if err != nil {
		t.Fatalf("NewDocWithClientID: %v", err)
	}
	return d
}

func TestHandleScalarRoundTrip(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	m := d.GetMap("root")

	cases := []struct {
		key string
		val any
	}{
		{"s", "hello"},
		{"i", int64(42)},
		{"neg", int64(-7)},
		{"btrue", true},
		{"bfalse", false},
		{"nul", nil},
		{"raw", []byte{0, 1, 2, 255}},
	}
	for _, c := range cases {
		if err := m.Set(c.key, c.val); err != nil {
			t.Fatalf("Set %s: %v", c.key, err)
		}
	}
	for _, c := range cases {
		got, ok := m.Get(c.key)
		if !ok {
			t.Fatalf("Get %s: absent", c.key)
		}
		switch want := c.val.(type) {
		case []byte:
			gb, isBytes := got.([]byte)
			if !isBytes || !bytes.Equal(gb, want) {
				t.Fatalf("Get %s: got %#v want %#v", c.key, got, want)
			}
		default:
			if got != c.val {
				t.Fatalf("Get %s: got %#v want %#v", c.key, got, c.val)
			}
		}
	}
}

// A string value and its identical bytes must not alias — the discriminator
// keeps them distinct across a round trip (the cross-SDK marshaling contract).
func TestHandleStringBytesDistinct(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	m := d.GetMap("root")
	if err := m.Set("s", "abc"); err != nil {
		t.Fatal(err)
	}
	if err := m.Set("b", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	sv, _ := m.Get("s")
	bv, _ := m.Get("b")
	if _, ok := sv.(string); !ok {
		t.Fatalf("string slot read back as %T", sv)
	}
	if _, ok := bv.([]byte); !ok {
		t.Fatalf("bytes slot read back as %T", bv)
	}
	if sv.(string) != "abc" || !bytes.Equal(bv.([]byte), []byte("abc")) {
		t.Fatalf("values wrong: s=%#v b=%#v", sv, bv)
	}
}

// The marshaling must be JS/Python-exact: a string leaf encodes to the same
// bytes as the pinned discriminator framing (tag 0x03, u32 len, 0x01 string
// discriminator, utf-8). A known-good encoding guards cross-SDK convergence.
func TestMarshalCrossSDKEncoding(t *testing.T) {
	// "hi" -> 0x03 | len=3 (LE u32) | 0x01 (string) | "hi"
	wantStr := []byte{0x03, 0x03, 0x00, 0x00, 0x00, discString, 'h', 'i'}
	got, err := marshalValue("hi")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, wantStr) {
		t.Fatalf("string marshal: got %v want %v", got, wantStr)
	}
	// bytes {0xAA} -> 0x03 | len=2 | 0x00 (binary) | 0xAA
	wantBin := []byte{0x03, 0x02, 0x00, 0x00, 0x00, discBinary, 0xAA}
	got, _ = marshalValue([]byte{0xAA})
	if !bytes.Equal(got, wantBin) {
		t.Fatalf("bytes marshal: got %v want %v", got, wantBin)
	}
	// int64(1) -> 0x02 | LE i64
	wantInt := []byte{0x02, 1, 0, 0, 0, 0, 0, 0, 0}
	got, _ = marshalValue(int64(1))
	if !bytes.Equal(got, wantInt) {
		t.Fatalf("int marshal: got %v want %v", got, wantInt)
	}
	if b, _ := marshalValue(true); !bytes.Equal(b, []byte{0x01, 0x01}) {
		t.Fatalf("bool marshal: got %v", b)
	}
	if b, _ := marshalValue(nil); !bytes.Equal(b, []byte{0x00}) {
		t.Fatalf("null marshal: got %v", b)
	}
}

// The explicit leaf/container boundary: a non-scalar Set is an error, never an
// implicit nested container (deep-seed is a rejected non-goal).
func TestHandleNoDeepSeed(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	m := d.GetMap("root")
	if err := m.Set("k", map[string]int{"a": 1}); err == nil {
		t.Fatal("expected error setting a map value (deep-seed rejected)")
	}
	if err := m.Set("k", []string{"a"}); err == nil {
		t.Fatal("expected error setting a slice value (deep-seed rejected)")
	}
	if err := m.Set("k", 1.5); err == nil {
		t.Fatal("expected error setting a float value")
	}
}

func TestHandleNestedComposition(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	inner := d.GetMap("a").GetMap("b").GetText("body")
	inner.Insert(0, "hello")
	if got := d.GetMap("a").GetMap("b").GetText("body").String(); got != "hello" {
		t.Fatalf("nested text: got %q", got)
	}
	// The container slot resolves to a handle through Get.
	bv, ok := d.GetMap("a").Get("b")
	if !ok {
		t.Fatal("nested map slot absent")
	}
	if _, isMap := bv.(*CrdtMap); !isMap {
		t.Fatalf("Get on container slot: got %T want *CrdtMap", bv)
	}
}

func TestHandleListSemantics(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	l := d.GetList("items")
	if err := l.Append("a"); err != nil {
		t.Fatal(err)
	}
	l.Append("c")
	l.Insert(1, "b")
	if l.Len() != 3 {
		t.Fatalf("len: %d", l.Len())
	}
	want := []any{"a", "b", "c"}
	for i, w := range want {
		got, ok := l.Get(i)
		if !ok || got != w {
			t.Fatalf("Get %d: got %#v ok=%v want %#v", i, got, ok, w)
		}
	}
	if err := l.Delete(0); err != nil {
		t.Fatal(err)
	}
	if v, _ := l.Get(0); v != "b" {
		t.Fatalf("after delete: %#v", v)
	}
	if _, ok := l.Get(10); ok {
		t.Fatal("out-of-range Get should be absent")
	}
	if err := l.Delete(10); err == nil {
		t.Fatal("out-of-range Delete should error")
	}
}

func TestHandleTextSemantics(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	tx := d.GetText("doc")
	tx.Insert(0, "helo")
	tx.Insert(2, "l") // "hello"? insert 'l' at index 2 -> "hello"
	if tx.String() != "hello" {
		t.Fatalf("text: %q", tx.String())
	}
	if tx.Len() != 5 {
		t.Fatalf("len: %d", tx.Len())
	}
	tx.Delete(0, 1)
	if tx.String() != "ello" {
		t.Fatalf("after delete: %q", tx.String())
	}
}

func TestHandleMapKeysAndEntries(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	m := d.GetMap("root")
	m.Set("x", int64(1))
	m.Set("y", "two")
	if m.Len() != 2 {
		t.Fatalf("len: %d", m.Len())
	}
	if !m.Has("x") || m.Has("z") {
		t.Fatal("Has wrong")
	}
	keys := map[string]bool{}
	for _, k := range m.Keys() {
		keys[k] = true
	}
	if !keys["x"] || !keys["y"] {
		t.Fatalf("keys: %v", m.Keys())
	}
	got := map[string]any{}
	for _, e := range m.Entries() {
		got[e.Key] = e.Value
	}
	if got["x"] != int64(1) || got["y"] != "two" {
		t.Fatalf("entries: %#v", got)
	}
	m.Delete("x")
	if m.Has("x") {
		t.Fatal("x should be deleted")
	}
}

func TestHandleConvergesSequential(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	b := newErgoDoc(t, 2)
	defer b.Close()

	var ops [][]byte
	unsub := a.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			ops = append(ops, e.Ops)
		}
	})
	defer unsub()

	a.GetMap("user").Set("name", "alice")
	a.GetMap("user").Set("age", int64(30))
	a.GetList("tags").Append("x")
	a.GetText("bio").Insert(0, "hi")

	for _, o := range ops {
		b.ApplyUpdate(o)
	}
	if v, _ := b.GetMap("user").Get("name"); v != "alice" {
		t.Fatalf("name: %#v", v)
	}
	if v, _ := b.GetMap("user").Get("age"); v != int64(30) {
		t.Fatalf("age: %#v", v)
	}
	if v, _ := b.GetList("tags").Get(0); v != "x" {
		t.Fatalf("tag: %#v", v)
	}
	if s := b.GetText("bio").String(); s != "hi" {
		t.Fatalf("bio: %q", s)
	}
}

func TestHandleConvergesConcurrent(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	b := newErgoDoc(t, 2)
	defer b.Close()

	var aOps, bOps []byte
	// Capture each doc's local ops via OnUpdate (a remote apply also fires an
	// update, so filter to local to capture only each doc's own edit).
	a.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			aOps = e.Ops
		}
	})
	b.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			bOps = e.Ops
		}
	})

	a.GetList("l").Append("a-item")
	b.GetList("l").Append("b-item")
	b.ApplyUpdate(aOps)
	a.ApplyUpdate(bOps)

	// Both concurrent items survive on both replicas in the same converged order
	// (Fugue tie-break by client id) — logical convergence, which encode-state
	// byte-equality does not imply for concurrent inserts across replicas.
	if a.GetList("l").Len() != 2 || b.GetList("l").Len() != 2 {
		t.Fatalf("list lengths: a=%d b=%d", a.GetList("l").Len(), b.GetList("l").Len())
	}
	for i := 0; i < 2; i++ {
		va, _ := a.GetList("l").Get(i)
		vb, _ := b.GetList("l").Get(i)
		if va != vb {
			t.Fatalf("item %d diverged: a=%#v b=%#v", i, va, vb)
		}
	}
}

func TestHandleSnapshotRoundTrip(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	a.GetMap("user").Set("name", "bob")
	a.GetText("bio").Insert(0, "hey")

	snap := a.EncodeState()
	b, err := DecodeDoc(snap)
	if err != nil {
		t.Fatalf("DecodeDoc: %v", err)
	}
	defer b.Close()
	if v, _ := b.GetMap("user").Get("name"); v != "bob" {
		t.Fatalf("name after decode: %#v", v)
	}
	if s := b.GetText("bio").String(); s != "hey" {
		t.Fatalf("bio after decode: %q", s)
	}
}

func TestOnUpdateFiresLocalAndUnsub(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	n := 0
	off := d.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			n++
		}
	})
	d.GetMap("m").Set("a", int64(1))
	d.GetMap("m").Set("b", int64(2))
	if n != 2 {
		t.Fatalf("expected 2 local updates, got %d", n)
	}
	off()
	d.GetMap("m").Set("c", int64(3))
	if n != 2 {
		t.Fatalf("unsubscribe failed, got %d", n)
	}
}

// A malformed scalar reaching the handle layer (e.g. raw bytes inserted through
// the low-level Backend().ListInsert power-user surface, which list_get returns
// verbatim) must degrade to opaque bytes, never panic the process.
func TestUnmarshalMalformedDoesNotPanic(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	l := d.GetList("l")
	// Truncated int (tag 0x02, 1 of 8 LE bytes), truncated bytes-length (0x03,
	// 1 byte), and a bytes tag claiming a huge length with a 1-byte body.
	for _, raw := range [][]byte{
		{0x02, 0x01},
		{0x03, 0x01},
		{0x03, 0xFF, 0xFF, 0xFF, 0x7F, 0x01},
	} {
		d.Backend().ListInsert(l.path, uint(l.Len()), raw)
	}
	for i := 0; i < l.Len(); i++ {
		if _, ok := l.Get(i); !ok { // must not panic; returns opaque bytes
			t.Fatalf("Get %d: absent", i)
		}
	}
}

func TestOnUpdateFiresRemote(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	b := newErgoDoc(t, 2)
	defer b.Close()

	var localOps []byte
	a.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			localOps = e.Ops
		}
	})
	a.GetMap("m").Set("k", "v")

	remote := 0
	b.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "remote" {
			remote++
		}
	})
	if n := b.ApplyUpdate(localOps); n <= 0 {
		t.Fatalf("apply: %d", n)
	}
	if remote != 1 {
		t.Fatalf("expected 1 remote update, got %d", remote)
	}
}
