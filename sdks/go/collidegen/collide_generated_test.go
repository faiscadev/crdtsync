// The typed accessors crdtsync-codegen emits for a schema whose slot names collide
// under identifier sanitization (`a-b`, `a_b`, `a b` all sanitize to `A_b`) compile
// against the Go SDK and stay independent: each disambiguated method forwards to its
// own distinct byte key. Drives the committed golden (collide_generated.go, pinned
// byte-for-byte by the Rust codegen determinism test) against a real Document.
package collidegen

import (
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

func TestCollidingSlotsStayIndependent(t *testing.T) {
	doc := newDoc(t)
	defer doc.Close()
	c := Bind(doc)

	c.IncA_b(1)
	c.IncA_b_2(2)
	c.IncA_b_3(3)

	// each disambiguated accessor round-trips to its own distinct field
	if v, ok := c.GetA_b(); !ok || v != 1 {
		t.Fatalf("GetA_b() = %d, %v; want 1, true", v, ok)
	}
	if v, ok := c.GetA_b_2(); !ok || v != 2 {
		t.Fatalf("GetA_b_2() = %d, %v; want 2, true", v, ok)
	}
	if v, ok := c.GetA_b_3(); !ok || v != 3 {
		t.Fatalf("GetA_b_3() = %d, %v; want 3, true", v, ok)
	}

	// and forwards to the exact byte key the schema declared
	if v, ok := doc.GetCounter([][]byte{[]byte("a-b")}); !ok || v != 1 {
		t.Fatalf("raw GetCounter(a-b) = %d, %v; want 1, true", v, ok)
	}
	if v, ok := doc.GetCounter([][]byte{[]byte("a_b")}); !ok || v != 2 {
		t.Fatalf("raw GetCounter(a_b) = %d, %v; want 2, true", v, ok)
	}
	if v, ok := doc.GetCounter([][]byte{[]byte("a b")}); !ok || v != 3 {
		t.Fatalf("raw GetCounter(a b) = %d, %v; want 3, true", v, ok)
	}
}
