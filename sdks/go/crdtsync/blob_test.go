package crdtsync

import (
	"bytes"
	"testing"
)

func TestInlineBlobReadsBackWithBytes(t *testing.T) {
	d := newDoc(t, 1)
	defer d.Close()

	raw := []byte{0x89, 'P', 'N', 'G', 0x00, 0xFF}
	ops, inlined := d.SetBlob(path("avatar"), "image/png", raw)
	if !inlined {
		t.Fatal("a small blob should inline")
	}
	if len(ops) == 0 {
		t.Fatal("an inlined blob returns ops to broadcast")
	}
	blob, ok := d.GetBlob(path("avatar"))
	if !ok {
		t.Fatal("get_blob should find the inline blob")
	}
	if blob.Mime != "image/png" {
		t.Fatalf("mime: got %q, want image/png", blob.Mime)
	}
	if blob.Size != uint64(len(raw)) {
		t.Fatalf("size: got %d, want %d", blob.Size, len(raw))
	}
	if !bytes.Equal(blob.Inline, raw) {
		t.Fatalf("inline: got %v, want %v", blob.Inline, raw)
	}
	if blob.ID == ([16]byte{}) {
		t.Fatal("a real handle should be minted")
	}
}

func TestBlobRefReadsBackWithoutBytes(t *testing.T) {
	d := newDoc(t, 1)
	defer d.Close()

	var id [16]byte
	for i := range id {
		id[i] = byte(i)
	}
	d.SetBlobRef(path("video"), id, "video/mp4", 10_000_000)
	blob, ok := d.GetBlob(path("video"))
	if !ok {
		t.Fatal("get_blob should find the ref")
	}
	if blob.ID != id {
		t.Fatalf("id: got %v, want %v", blob.ID, id)
	}
	if blob.Mime != "video/mp4" || blob.Size != 10_000_000 {
		t.Fatalf("ref fields: got (%q,%d)", blob.Mime, blob.Size)
	}
	if blob.Inline != nil {
		t.Fatalf("a ref carries no inline bytes, got %v", blob.Inline)
	}
}

func TestOverCeilingBlobIsNotInlined(t *testing.T) {
	d := newDoc(t, 1)
	defer d.Close()

	ops, inlined := d.SetBlob(path("huge"), "application/octet-stream", make([]byte, 4097))
	if inlined {
		t.Fatal("over the inline ceiling must not inline")
	}
	if ops != nil {
		t.Fatal("no ops when not inlined")
	}
	if _, ok := d.GetBlob(path("huge")); ok {
		t.Fatal("nothing should be written over the ceiling")
	}
}

func TestBlobConvergesOnAPeer(t *testing.T) {
	a := newDoc(t, 1)
	defer a.Close()
	b := newDoc(t, 2)
	defer b.Close()

	ops, _ := a.SetBlob(path("pic"), "image/png", []byte("tiny-png"))
	if b.Apply(ops) < 1 {
		t.Fatal("peer should apply the blob op")
	}
	blob, ok := b.GetBlob(path("pic"))
	if !ok || !bytes.Equal(blob.Inline, []byte("tiny-png")) {
		t.Fatalf("peer blob: got (%v,%v)", blob.Inline, ok)
	}
}

func TestAbsentSlotReadsNoBlob(t *testing.T) {
	d := newDoc(t, 1)
	defer d.Close()
	if _, ok := d.GetBlob(path("nope")); ok {
		t.Fatal("an absent slot holds no blob")
	}
}

func TestClientBlobEditEnqueuesAndTravels(t *testing.T) {
	a := newClient(t, 1)
	defer a.Close()
	b := newClient(t, 2)
	defer b.Close()

	ca, _ := a.Subscribe(key("room-1"))
	b.Subscribe(key("room-1"))

	frame := a.SetBlob(ca, path("avatar"), "image/png", []byte("tiny-png"))
	if n := a.OutboxLen(ca); n != 1 {
		t.Fatalf("outbox after inline blob: got %d, want 1", n)
	}
	if rc, _ := b.Receive(frame); rc != 1 {
		t.Fatalf("peer receive inline: got %d, want 1", rc)
	}

	rframe := a.SetBlobRef(ca, path("video"), [16]byte{7}, "video/mp4", 10_000_000)
	if n := a.OutboxLen(ca); n != 2 {
		t.Fatalf("outbox after ref blob: got %d, want 2", n)
	}
	if rc, _ := b.Receive(rframe); rc != 1 {
		t.Fatalf("peer receive ref: got %d, want 1", rc)
	}
}
