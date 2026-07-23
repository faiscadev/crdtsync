package crdtsync

import (
	"bytes"
	"testing"
)

// --- Xml ---

func TestXmlInstallAndEditChildren(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	root := d.GetXml("doc")
	root.Element("doc")
	if tag, ok := root.Tag(); !ok || tag != "doc" {
		t.Fatalf("tag: %q ok=%v", tag, ok)
	}
	root.InsertElement(0, "p")
	root.InsertText(1, "hello")
	if root.Len() != 2 {
		t.Fatalf("len: %d", root.Len())
	}
	root.DeleteChild(0)
	if root.Len() != 1 {
		t.Fatalf("len after delete: %d", root.Len())
	}
}

func TestXmlResolvesFromParentMap(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	d.GetMap("root").GetXml("body").Fragment()
	v, ok := d.GetMap("root").Get("body")
	if !ok {
		t.Fatal("xml slot absent")
	}
	x, isXml := v.(*CrdtXml)
	if !isXml {
		t.Fatalf("Get: got %T want *CrdtXml", v)
	}
	if _, ok := x.Tag(); ok {
		t.Fatal("a fragment should be tagless")
	}
}

func TestXmlTreeMove(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	a := d.GetXml("a")
	a.Element("a").InsertElement(0, "x").InsertElement(1, "y")
	b := d.GetXml("b")
	b.Element("b")
	a.Move(0, b, 0)
	if a.Len() != 1 || b.Len() != 1 {
		t.Fatalf("after move: a=%d b=%d", a.Len(), b.Len())
	}
}

func TestXmlConverges(t *testing.T) {
	p := newErgoDoc(t, 1)
	defer p.Close()
	q := newErgoDoc(t, 2)
	defer q.Close()
	p.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			q.ApplyUpdate(e.Ops)
		}
	})
	p.GetXml("doc").Element("doc").InsertElement(0, "p").InsertText(1, "hi")
	qd := q.GetXml("doc")
	if tag, ok := qd.Tag(); !ok || tag != "doc" {
		t.Fatalf("peer tag: %q ok=%v", tag, ok)
	}
	if qd.Len() != 2 {
		t.Fatalf("peer children: %d", qd.Len())
	}
}

// --- cursors ---

func TestTextCursorTracksEdits(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	tx := d.GetText("body")
	tx.Insert(0, "hello world")
	pos := tx.RelativePosition(6, "before")
	if pos == nil {
		t.Fatal("nil cursor")
	}
	if i, ok := tx.Resolve(pos); !ok || i != 6 {
		t.Fatalf("resolve: %d ok=%v", i, ok)
	}
	tx.Insert(0, ">> ")
	if i, _ := tx.Resolve(pos); i != 9 {
		t.Fatalf("after front insert: %d", i)
	}
	tx.Delete(0, 3)
	if i, _ := tx.Resolve(pos); i != 6 {
		t.Fatalf("after front delete: %d", i)
	}
}

func TestListCursorRoundTrips(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	xs := d.GetList("xs")
	xs.Append("a")
	xs.Append("b")
	xs.Append("c")
	pos := xs.RelativePosition(2, "before")
	if pos == nil {
		t.Fatal("nil cursor")
	}
	if i, ok := xs.Resolve(pos); !ok || i != 2 {
		t.Fatalf("resolve: %d ok=%v", i, ok)
	}
	xs.Insert(0, "z")
	if i, _ := xs.Resolve(pos); i != 3 {
		t.Fatalf("after front insert: %d", i)
	}
}

func TestCursorSurvivesConcurrentRemote(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	b := newErgoDoc(t, 2)
	defer b.Close()
	a.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			b.ApplyUpdate(e.Ops)
		}
	})
	b.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			a.ApplyUpdate(e.Ops)
		}
	})
	a.GetText("t").Insert(0, "abcdef")
	cursor := a.GetText("t").RelativePosition(4, "before")
	if i, _ := a.GetText("t").Resolve(cursor); i != 4 {
		t.Fatalf("initial resolve: %d", i)
	}
	b.GetText("t").Insert(0, "XYZ") // concurrent front insert
	if i, _ := a.GetText("t").Resolve(cursor); i != 7 {
		t.Fatalf("after concurrent front insert: %d", i)
	}
	if s := a.GetText("t").String(); s != "XYZabcdef" {
		t.Fatalf("text: %q", s)
	}
}

// --- marks ---

func markNames(marks []MarkInfo) []string {
	out := make([]string, len(marks))
	for i, m := range marks {
		out[i] = m.Name
	}
	return out
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

func TestMarkAuthorAndRead(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	text := d.GetText("body")
	text.Insert(0, "hello world")
	id, err := text.Mark(0, 5, "comment", "note-1")
	if err != nil || id == nil {
		t.Fatalf("mark: id=%v err=%v", id, err)
	}
	if !contains(markNames(text.MarksAt(0)), "comment") {
		t.Fatal("comment not covering index 0")
	}
	if len(text.MarksAt(8)) != 0 {
		t.Fatal("mark should not cover index 8 (outside range)")
	}
}

func TestMarkRemoveByHandle(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	text := d.GetText("body")
	text.Insert(0, "abcdef")
	id, _ := text.Mark(0, 6, "comment", "x")
	if !contains(markNames(text.MarksAt(2)), "comment") {
		t.Fatal("comment missing before delete")
	}
	text.DeleteMark(id)
	if len(text.MarksAt(2)) != 0 {
		t.Fatal("mark still present after delete")
	}
}

func TestMarkSyncsToPeer(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	b := newErgoDoc(t, 2)
	defer b.Close()
	a.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			b.ApplyUpdate(e.Ops)
		}
	})
	a.GetText("t").Insert(0, "hello")
	id, _ := a.GetText("t").Mark(0, 5, "comment", "hi")
	if id == nil {
		t.Fatal("nil mark id")
	}
	if !contains(markNames(b.GetText("t").MarksAt(1)), "comment") {
		t.Fatal("mark did not sync to peer")
	}
}

func TestMarkReactivity(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	text := d.GetText("t")
	text.Insert(0, "hello")
	var kinds []string
	d.OnUpdate(func(e UpdateEvent) {
		for _, c := range e.Changes {
			kinds = append(kinds, c.Kind)
		}
	})
	text.Mark(0, 5, "bold", true)
	if !contains(kinds, "mark") {
		t.Fatalf("no mark change reported: %v", kinds)
	}
}

func TestValueFlavorMarkReadsNative(t *testing.T) {
	schema := []byte(`{
        "schema": "notes", "version": 1, "root": "Doc",
        "types": { "Doc": { "kind": "map", "children": { "t": "Body" } },
                   "Body": { "kind": "text" } },
        "marks": { "color": { "flavor": "value" } }
    }`)
	d := newErgoDoc(t, 1)
	defer d.Close()
	text := d.GetText("t")
	text.Insert(0, "hello")
	if !d.SetSchema(schema) {
		t.Fatal("schema did not bind")
	}
	text.Mark(0, 5, "color", "red")
	var color *MarkInfo
	for _, m := range text.MarksAt(2) {
		if m.Name == "color" {
			mm := m
			color = &mm
		}
	}
	if color == nil || color.Value != "red" {
		t.Fatalf("color mark: %#v", color)
	}
}

// --- atomic transactions ---

func TestTransactSingleUpdate(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	m := d.GetMap("root")
	m.Set("init", int64(0))
	var events []UpdateEvent
	d.OnUpdate(func(e UpdateEvent) { events = append(events, e) })
	d.Transact(func() {
		m.Set("a", int64(1))
		m.Set("b", int64(2))
		m.Set("c", int64(3))
	})
	if len(events) != 1 {
		t.Fatalf("expected 1 batched update, got %d", len(events))
	}
	if events[0].Origin != "local" {
		t.Fatalf("origin: %s", events[0].Origin)
	}
	if v, _ := m.Get("a"); v != int64(1) {
		t.Fatalf("a=%#v", v)
	}
	if v, _ := m.Get("c"); v != int64(3) {
		t.Fatalf("c=%#v", v)
	}
}

func TestTransactAtomicOnPeer(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	b := newErgoDoc(t, 2)
	defer b.Close()
	a.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			b.ApplyUpdate(e.Ops)
		}
	})
	a.GetMap("root").Set("init", int64(0))
	a.Transact(func() {
		a.GetMap("root").Set("x", int64(1))
		a.GetMap("root").Set("y", int64(2))
		a.GetList("log").Append("entry")
	})
	if v, _ := b.GetMap("root").Get("x"); v != int64(1) {
		t.Fatalf("peer x=%#v", v)
	}
	if v, _ := b.GetMap("root").Get("y"); v != int64(2) {
		t.Fatalf("peer y=%#v", v)
	}
	if v, _ := b.GetList("log").Get(0); v != "entry" {
		t.Fatalf("peer log[0]=%#v", v)
	}
}

func TestTransactFlattensNested(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	m := d.GetMap("root")
	m.Set("init", int64(0))
	var events []UpdateEvent
	d.OnUpdate(func(e UpdateEvent) { events = append(events, e) })
	d.Transact(func() {
		m.Set("a", int64(1))
		d.Transact(func() { m.Set("b", int64(2)) })
		m.Set("c", int64(3))
	})
	if len(events) != 1 {
		t.Fatalf("nested transaction should flatten to 1 update, got %d", len(events))
	}
	if v, _ := m.Get("b"); v != int64(2) {
		t.Fatalf("b=%#v", v)
	}
}

// --- blobs ---

func TestBlobInlineStoreAndRead(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	m := d.GetMap("root")
	data := []byte{1, 2, 3, 4}
	if !m.SetBlob("avatar", "image/png", data) {
		t.Fatal("inline blob not stored")
	}
	ref, ok := m.GetBlob("avatar")
	if !ok {
		t.Fatal("blob absent")
	}
	if ref.Mime != "image/png" || ref.Size != 4 || !bytes.Equal(ref.Inline, data) {
		t.Fatalf("ref: %+v", ref)
	}
	if len(ref.ID) != 16 {
		t.Fatalf("id len: %d", len(ref.ID))
	}
}

func TestBlobGenericGet(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	m := d.GetMap("root")
	m.SetBlob("file", "text/plain", []byte{9})
	v, ok := m.Get("file")
	if !ok {
		t.Fatal("file absent")
	}
	ref, isBlob := v.(BlobRef)
	if !isBlob {
		t.Fatalf("Get: got %T want BlobRef", v)
	}
	if ref.Mime != "text/plain" || !bytes.Equal(ref.Inline, []byte{9}) {
		t.Fatalf("ref: %+v", ref)
	}
}

func TestBlobStoreBackedRef(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	m := d.GetMap("root")
	var blobID [16]byte
	for i := range blobID {
		blobID[i] = 7
	}
	m.SetBlobRef("big", blobID, "video/mp4", 1_000_000)
	ref, ok := m.GetBlob("big")
	if !ok {
		t.Fatal("blob absent")
	}
	if ref.Mime != "video/mp4" || ref.Size != 1_000_000 {
		t.Fatalf("ref: %+v", ref)
	}
	if ref.Inline != nil {
		t.Fatal("store-backed ref should carry no inline bytes")
	}
	if ref.ID != blobID {
		t.Fatalf("id: %v", ref.ID)
	}
}

func TestBlobOverCeilingNotInlined(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	m := d.GetMap("root")
	big := make([]byte, 4096+1)
	if m.SetBlob("huge", "application/octet-stream", big) {
		t.Fatal("over-ceiling blob should not inline")
	}
	if _, ok := m.GetBlob("huge"); ok {
		t.Fatal("over-ceiling blob should not be stored")
	}
}

func TestBlobConverges(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	b := newErgoDoc(t, 2)
	defer b.Close()
	a.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			b.ApplyUpdate(e.Ops)
		}
	})
	a.GetMap("root").SetBlob("pic", "image/gif", []byte{1, 2, 3})
	ref, ok := b.GetMap("root").GetBlob("pic")
	if !ok || ref.Mime != "image/gif" || !bytes.Equal(ref.Inline, []byte{1, 2, 3}) {
		t.Fatalf("peer blob: %+v ok=%v", ref, ok)
	}
}
