package crdtsync

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fixedHandle is the 0..15 handle the mock server echoes back.
func fixedHandle() [16]byte {
	var h [16]byte
	for i := range h {
		h[i] = byte(i)
	}
	return h
}

func TestUploadBlobReturnsHandleAndSendsRequest(t *testing.T) {
	handle := fixedHandle()
	var seen *http.Request
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		seen = r
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": hex.EncodeToString(handle[:]), "size": len(body), "inline": false,
		})
	}))
	defer srv.Close()

	raw := []byte("a-large-object")
	id, err := UploadBlob(srv.URL, "user-cred", "image/png", raw)
	if err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}
	if id != handle {
		t.Fatalf("handle: got %v, want %v", id, handle)
	}
	if seen.URL.Path != "/blobs" {
		t.Fatalf("path: got %q, want /blobs", seen.URL.Path)
	}
	if got := seen.Header.Get("Authorization"); got != "user-cred" {
		t.Fatalf("authorization: got %q", got)
	}
	if got := seen.Header.Get("Content-Type"); got != "image/png" {
		t.Fatalf("content-type: got %q", got)
	}
	if !bytes.Equal(body, raw) {
		t.Fatalf("body: got %v, want %v", body, raw)
	}
}

func TestUploadedHandleRoundTripsThroughSetBlobRef(t *testing.T) {
	handle := fixedHandle()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": hex.EncodeToString(handle[:]), "size": len(body), "inline": false,
		})
	}))
	defer srv.Close()

	id, err := UploadBlob(srv.URL, "user-cred", "application/octet-stream", make([]byte, 10_000))
	if err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}
	d := newDoc(t, 1)
	defer d.Close()
	d.SetBlobRef(path("video"), id, "video/mp4", 10_000)
	blob, ok := d.GetBlob(path("video"))
	if !ok {
		t.Fatal("get_blob should find the ref")
	}
	if blob.ID != id || blob.Mime != "video/mp4" || blob.Size != 10_000 {
		t.Fatalf("ref: got (%v,%q,%d)", blob.ID, blob.Mime, blob.Size)
	}
	if blob.Inline != nil {
		t.Fatal("a store-backed ref carries no inline bytes")
	}
}

func TestUploadBlobSurfacesServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	if _, err := UploadBlob(srv.URL, "bad", "application/octet-stream", []byte("x")); err == nil {
		t.Fatal("an unauthorized upload should error")
	}
}
