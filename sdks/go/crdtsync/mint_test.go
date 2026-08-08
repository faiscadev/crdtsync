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

// An op body opens with its author's 16-byte client id, its 8-byte sequence and
// the stamp's 8-byte lamport — so the sequence runs from body offset 16 and the
// lamport from 24, both past the frame's 4-byte length prefix.
const (
	opSeqAt   = 4 + 16
	lamportAt = 4 + 16 + 8
)

// The last id of the space: math.MaxUint64 >> 1. A stamp may legally sit there,
// which is why one op is enough to spend its author's mint.
const lamportCeiling uint64 = 1<<63 - 1

// An op-id sequence the receiving replica has not spent, so the plant is not
// deduplicated away as one of that replica's own ops.
const unspentSeq uint64 = 9999

// stampedAt returns one op frame authored under first's client id, its stamp moved
// to lamport.
func stampedAt(t *testing.T, first byte, lamport uint64) []byte {
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
	binary.LittleEndian.PutUint64(frame[opSeqAt:], unspentSeq)
	binary.LittleEndian.PutUint64(frame[lamportAt:], lamport)
	return frame
}

// planted is the plant that spends the space outright.
func planted(t *testing.T, first byte) []byte {
	t.Helper()
	return stampedAt(t, first, lamportCeiling)
}

// nearlySpent leaves a handful of ids — enough for a single-id edit, not for a
// ten-codepoint run.
func nearlySpent(t *testing.T, first byte) []byte {
	t.Helper()
	return stampedAt(t, first, lamportCeiling-6)
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

func TestARefusedCallStillDispatchesWhatItEmitted(t *testing.T) {
	// One handle call is one core transaction, and a refusal cuts it at the edit
	// that could not mint — so a refused call can carry ops that did. They are
	// applied to this replica already; withholding them would leave it ahead of
	// every peer.
	d := newErgoDoc(t, 3)
	defer d.Close()
	var updates int
	d.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			updates++
		}
	})
	d.ApplyUpdate(nearlySpent(t, 3))

	// The text does not exist, so this emits a container-create the space still has
	// room for, then a ten-codepoint run it does not.
	d.GetText("t").Insert(0, "abcdefghij")
	if err := d.Err(); err != ErrMintExhausted {
		t.Fatalf("the refused run left Err as %v", err)
	}
	if updates != 1 {
		t.Fatalf("the refused call dispatched %d updates, want 1", updates)
	}
	if got := d.GetText("t").String(); got != "" {
		t.Fatalf("the refused run landed %q", got)
	}
}

func TestAnInertEditIsNotReportedAsARefusal(t *testing.T) {
	// An inert edit and a refused one both emit nothing, which is the whole reason
	// the query exists — so an edit that resolves to nothing must answer for itself
	// rather than inherit the previous edit's refusal.
	d := newErgoDoc(t, 4)
	defer d.Close()
	d.GetText("t").Insert(0, "ab")
	d.ApplyUpdate(nearlySpent(t, 4))

	d.GetText("t").Insert(0, "abcdefghij")
	if err := d.Err(); err != ErrMintExhausted {
		t.Fatalf("the refused run left Err as %v", err)
	}
	// An XML insert on a path that holds no XML node resolves to nothing.
	d.GetXml("nope").InsertElement(0, "p")
	if err := d.Err(); err != nil {
		t.Fatalf("an inert edit reported %v", err)
	}
	// And the replica really did still have room.
	d.GetText("t").Insert(0, "z")
	if err := d.Err(); err != nil {
		t.Fatalf("a healthy edit reported %v", err)
	}
	if got := d.GetText("t").String(); got != "zab" {
		t.Fatalf("text reads %q", got)
	}
}

func TestAnOrdinaryEditIsUntouched(t *testing.T) {
	d := newErgoDoc(t, 5)
	defer d.Close()
	if err := d.GetMap("root").Set("k", int64(1)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := d.Err(); err != nil {
		t.Fatalf("Doc.Err reported %v on a healthy replica", err)
	}
}
