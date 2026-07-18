// The typed accessors crdtsync-codegen emits for a schema compile against the Go
// SDK and forward to the same path surface as a raw call. This drives the
// committed golden (note_generated.go, pinned byte-for-byte by the Rust codegen
// determinism test) against a real Document and asserts a value set through a
// generated setter reads back through both the generated getter and the
// equivalent raw path call — the round-trip the codegen exists to make type-safe.
package notegen

import (
	"bytes"
	"testing"

	"github.com/faiscadev/crdtsync/sdks/go/crdtsync"
)

func cid(first byte) []byte {
	b := make([]byte, 16)
	b[0] = first
	return b
}

func newDoc(t *testing.T) *crdtsync.Document {
	t.Helper()
	d, err := crdtsync.New(cid(1))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d
}

func TestTextSlotRoundTripsThroughGeneratedAccessors(t *testing.T) {
	doc := newDoc(t)
	defer doc.Close()
	note := Bind(doc)

	note.InsertTitle(0, "hello")
	if got, ok := note.GetTitle(); !ok || got != "hello" {
		t.Fatalf("GetTitle() = %q, %v; want \"hello\", true", got, ok)
	}
	if n, ok := note.LenTitle(); !ok || n != 5 {
		t.Fatalf("LenTitle() = %d, %v; want 5, true", n, ok)
	}
	// forwards to the same raw path the accessor bakes in
	if got, ok := doc.TextGet([][]byte{[]byte("title")}); !ok || got != "hello" {
		t.Fatalf("raw TextGet = %q, %v; want \"hello\", true", got, ok)
	}
	note.DeleteTitle(0, 5)
	if n, ok := note.LenTitle(); !ok || n != 0 {
		t.Fatalf("LenTitle() after delete = %d, %v; want 0, true", n, ok)
	}
}

func TestCounterSlotRoundTrips(t *testing.T) {
	doc := newDoc(t)
	defer doc.Close()
	note := Bind(doc)

	note.IncViews(3)
	note.DecViews(1)
	if v, ok := note.GetViews(); !ok || v != 2 {
		t.Fatalf("GetViews() = %d, %v; want 2, true", v, ok)
	}
	if v, ok := doc.GetCounter([][]byte{[]byte("views")}); !ok || v != 2 {
		t.Fatalf("raw GetCounter = %d, %v; want 2, true", v, ok)
	}
}

func TestListSlotRoundTrips(t *testing.T) {
	doc := newDoc(t)
	defer doc.Close()
	note := Bind(doc)

	note.InsertTags(0, []byte("draft"))
	note.InsertTags(1, []byte("urgent"))
	if n, ok := note.LenTags(); !ok || n != 2 {
		t.Fatalf("LenTags() = %d, %v; want 2, true", n, ok)
	}
	if got, ok := note.GetTags(0); !ok || !bytes.Equal(got, []byte("draft")) {
		t.Fatalf("GetTags(0) = %q, %v; want \"draft\", true", got, ok)
	}
	if got, ok := note.GetTags(1); !ok || !bytes.Equal(got, []byte("urgent")) {
		t.Fatalf("GetTags(1) = %q, %v; want \"urgent\", true", got, ok)
	}
}

func TestNestedMapSlotForwardsToNestedPath(t *testing.T) {
	doc := newDoc(t)
	defer doc.Close()
	note := Bind(doc)

	note.Meta().SetPriority(4)
	if p, ok := note.Meta().GetPriority(); !ok || p != 4 {
		t.Fatalf("Meta().GetPriority() = %d, %v; want 4, true", p, ok)
	}
	// the nested wrapper addresses the extended path
	if p, ok := doc.GetInt([][]byte{[]byte("meta"), []byte("priority")}); !ok || p != 4 {
		t.Fatalf("raw GetInt(meta/priority) = %d, %v; want 4, true", p, ok)
	}
}
