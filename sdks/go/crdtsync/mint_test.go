// A stamp is drawn from the replica's own id space, and the space is finite. When
// it runs out an edit is *refused* — nothing emitted, nothing changed — because
// the alternative is re-issuing an id that is already live, which every peer drops
// as a replay. Refusal is the right answer and silence is not: every mutator
// returns the same empty ops an inert edit returns, so without a reported error the
// caller reports a write that never happened.

package crdtsync

import (
	"encoding/binary"
	"testing"
)

// An op body opens with its author's 16-byte client id and its 8-byte sequence, so
// the stamp's lamport runs from body offset 24 — past the frame's 4-byte length
// prefix.
const lamportAt = 4 + 16 + 8

// The last id of the space: math.MaxUint64 >> 1. A stamp may legally sit there,
// which is why one op is enough to spend its author's mint.
const lamportCeiling uint64 = 1<<63 - 1

// planted returns one op frame authored under first's client id, its stamp moved
// to the last id of the space.
func planted(t *testing.T, first byte) []byte {
	t.Helper()
	d := newErgoDoc(t, first)
	defer d.Close()
	var emitted [][]byte
	d.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			emitted = append(emitted, e.Ops)
		}
	})
	if err := d.GetMap("root").Set("k", int64(1)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if len(emitted) == 0 {
		t.Fatal("the seed edit emitted nothing")
	}
	frame := append([]byte(nil), opFrames(emitted[0])[0]...)
	binary.LittleEndian.PutUint64(frame[lamportAt:], lamportCeiling)
	return frame
}

func TestASpentIDSpaceReportsItsRefusal(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	// A peer authoring under this replica's own client id needs one admissible op
	// to put the id space at its end.
	d.ApplyUpdate(planted(t, 1))

	if err := d.GetMap("root").Set("k", int64(1)); err != ErrMintExhausted {
		t.Fatalf("Set on a spent replica reported %v, want ErrMintExhausted", err)
	}
	if _, ok := d.GetMap("root").Get("k"); ok {
		t.Fatal("a refused edit reached local state")
	}
	if err := d.Err(); err != ErrMintExhausted {
		t.Fatalf("Doc.Err reported %v, want ErrMintExhausted", err)
	}
}

func TestAMutatorWithNoErrorOfItsOwnReportsThroughErr(t *testing.T) {
	// The XML handles chain, so an error return there would cost the chaining the
	// API is built on; Delete and Insert have nothing else to say. Err is their
	// seam, and it reads the same condition the error-returning mutators do.
	d := newErgoDoc(t, 2)
	defer d.Close()
	d.ApplyUpdate(planted(t, 2))

	d.GetText("t").Insert(0, "hello")
	if err := d.Err(); err != ErrMintExhausted {
		t.Fatalf("Text.Insert on a spent replica left Err as %v", err)
	}

	d.GetXml("x").Element("p")
	if err := d.Err(); err != ErrMintExhausted {
		t.Fatalf("Xml.Element on a spent replica left Err as %v", err)
	}
}

func TestAnOrdinaryEditIsUntouched(t *testing.T) {
	d := newErgoDoc(t, 3)
	defer d.Close()
	if err := d.GetMap("root").Set("k", int64(1)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// An inert edit emits nothing either, and that is not a refusal.
	if err := d.GetMap("root").Set("k", int64(1)); err != nil {
		t.Fatalf("an inert edit reported %v", err)
	}
	if err := d.Err(); err != nil {
		t.Fatalf("Doc.Err reported %v on a healthy replica", err)
	}
}
