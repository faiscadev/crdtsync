// A fold reports how many ops it took now *and* how many no replica will ever
// hold, because the two zeros mean opposite things. A buffered op is waiting on a
// create a later update carries; a refused one is a bug in whoever wrote it, and
// offline, P2P and relayed peers reach this fold with no server between them to
// reject it first.

package crdtsync

import (
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

func forgeStampClient(frame []byte) []byte {
	forged := append([]byte(nil), frame...)
	for i := stampClientAt; i < stampClientAt+16; i++ {
		forged[i] = 0xff
	}
	return forged
}

func TestARefusedOpIsCountedApartFromABufferedOne(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	var emitted [][]byte
	a.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			emitted = append(emitted, e.Ops)
		}
	})

	// The first write into a map is two ops: the container create, then the write
	// into it. A second write is one op, targeting the same container.
	a.GetMap("root").Set("k", int64(1))
	a.GetMap("root").Set("k2", int64(2))
	opened := frames(emitted[0])
	if len(opened) != 2 {
		t.Fatalf("opening a map is a create and a write, got %d ops", len(opened))
	}
	create, write := opened[0], opened[1]
	later := frames(emitted[1])[0]

	b := newErgoDoc(t, 2)
	defer b.Close()
	batch := append(forgeStampClient(later), write...)
	if applied, refused := b.ApplyUpdate(batch); applied != 0 || refused != 1 {
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
}

func TestAMalformedBatchIsNeitherAppliedNorRefused(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	// Nothing decoded, so there is no op to judge.
	if applied, refused := d.ApplyUpdate([]byte{0xff, 0xff, 0xff, 0xff}); applied != -1 || refused != 0 {
		t.Fatalf("applied %d refused %d, want -1 and 0", applied, refused)
	}
}
