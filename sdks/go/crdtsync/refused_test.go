// A fold reports how many ops it took now *and* how many no replica will ever
// hold, because the two zeros mean opposite things. A buffered op is waiting on a
// create a later update carries; a refused one is a bug in whoever wrote it, and
// offline, P2P and relayed peers reach this fold with no server between them to
// reject it first.

package crdtsync

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// An op body opens with its author's 16-byte client id, its 8-byte sequence and
// the stamp's 8-byte lamport, so the stamp's own client id runs from body offset
// 32 — past the frame's 4-byte length prefix. Naming another client there mints
// node ids inside that client's id space, which no replica will ever hold.
const stampClientAt = 4 + 16 + 8 + 8

// frames splits an op log, which frames each op as a u32 length then its body.
func frames(log []byte) [][]byte {
	var out [][]byte
	for at := 0; at < len(log); {
		size := int(binary.LittleEndian.Uint32(log[at:]))
		out = append(out, log[at:at+4+size])
		at += 4 + size
	}
	return out
}

// join concatenates framed ops back into one op log.
func join(ops ...[]byte) []byte {
	var out []byte
	for _, op := range ops {
		out = append(out, op...)
	}
	return out
}

func forgeStampClient(t *testing.T, frame, author []byte) []byte {
	t.Helper()
	// Read the field back first, so a codec reordering fails here by name rather
	// than as an unexplained "nothing was refused" further down.
	if !bytes.Equal(frame[stampClientAt:stampClientAt+16], author) {
		t.Fatalf("the stamp's client id does not sit at body offset %d", stampClientAt-4)
	}
	forged := append([]byte(nil), frame...)
	for i := stampClientAt; i < stampClientAt+16; i++ {
		forged[i] = 0xff
	}
	return forged
}

// openedMap returns a doc that wrote twice into one map, with its create, write
// and later ops. The first write into a map is two ops — the container create,
// then the write into it; a second write is one op, targeting that container.
func openedMap(t *testing.T, first byte) (create, write, later []byte) {
	t.Helper()
	d := newErgoDoc(t, first)
	t.Cleanup(d.Close)
	var emitted [][]byte
	d.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			emitted = append(emitted, e.Ops)
		}
	})
	d.GetMap("root").Set("k", int64(1))
	d.GetMap("root").Set("k2", int64(2))
	opened := frames(emitted[0])
	if len(opened) != 2 {
		t.Fatalf("opening a map is a create and a write, got %d ops", len(opened))
	}
	return opened[0], opened[1], frames(emitted[1])[0]
}

func TestARefusedOpIsCountedApartFromABufferedOne(t *testing.T) {
	create, write, later := openedMap(t, 1)

	b := newErgoDoc(t, 2)
	defer b.Close()
	if applied, refused := b.ApplyUpdate(join(forgeStampClient(t, later, cid(1)), write)); applied != 0 || refused != 1 {
		t.Fatalf("applied %d refused %d, want 0 and 1", applied, refused)
	}

	// The buffered op was waiting, not refused: the create releases it. The forged
	// one is gone for good, though its target is now reachable.
	if applied, refused := b.ApplyUpdate(create); applied != 1 || refused != 0 {
		t.Fatalf("the create applied %d refused %d, want 1 and 0", applied, refused)
	}
	if v, ok := b.GetMap("root").Get("k"); !ok || v != int64(1) {
		t.Fatalf("the buffered write commits: got %v ok=%v", v, ok)
	}
	if _, ok := b.GetMap("root").Get("k2"); ok {
		t.Fatal("the forged op is refused forever, even once its target is reachable")
	}

	// A replay of what already landed is a duplicate, never a refusal.
	if applied, refused := b.ApplyUpdate(join(create, write)); applied != 0 || refused != 0 {
		t.Fatalf("a replay applied %d refused %d, want 0 and 0", applied, refused)
	}
}

func TestTheRestOfABatchCarryingOneForgeryApplies(t *testing.T) {
	create, write, later := openedMap(t, 1)

	// The everyday shape: one forgery riding a stream of honest ops. The refusal
	// is per op, not per batch.
	b := newErgoDoc(t, 2)
	defer b.Close()
	batch := join(forgeStampClient(t, later, cid(1)), create, write)
	if applied, refused := b.ApplyUpdate(batch); applied != 2 || refused != 1 {
		t.Fatalf("applied %d refused %d, want 2 and 1", applied, refused)
	}
	if v, ok := b.GetMap("root").Get("k"); !ok || v != int64(1) {
		t.Fatalf("the honest ops still land: got %v ok=%v", v, ok)
	}
}

func TestAMalformedBatchIsNeitherAppliedNorRefused(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	// Nothing decoded, so there is no op to judge.
	if applied, refused := d.ApplyUpdate([]byte{0xff, 0xff, 0xff, 0xff}); applied != -1 || refused != 0 {
		t.Fatalf("applied %d refused %d, want -1 and 0", applied, refused)
	}
}
