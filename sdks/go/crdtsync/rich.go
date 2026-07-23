package crdtsync

// Rich-content handle surface (§SDK-Ergonomic-Surface): XML, cursors
// (RelativePosition/Resolve), and text marks — all over the low-level Document's
// existing xml/anchor/mark methods, marshaling native values and hiding paths.

// sideFromString maps a gravity name to a Side: "after" is right-gravity, any
// other value (default "before") is left-gravity.
func sideFromString(side string) Side {
	if side == "after" {
		return Right
	}
	return Left
}

// --- cursors (List) ---

// RelativePosition captures a stable cursor at a live index ("before" is
// left-gravity, "after" right-gravity), resolved later with Resolve. Nil for a
// bad or non-sequence path.
func (l *CrdtList) RelativePosition(index int, side string) []byte {
	return l.doc.backend.RelativePosition(l.path, uint(index), sideFromString(side))
}

// Resolve resolves a captured cursor back to a live index. The bool is false
// when it can't resolve.
func (l *CrdtList) Resolve(pos []byte) (int, bool) {
	n, ok := l.doc.backend.ResolvePosition(l.path, pos)
	return int(n), ok
}

// --- cursors + marks (Text) ---

// RelativePosition captures a stable cursor at a codepoint index ("before" is
// left-gravity, "after" right-gravity). The cursor tracks its spot as text is
// inserted and deleted around it. Nil for a bad path.
func (t *CrdtText) RelativePosition(index int, side string) []byte {
	return t.doc.backend.RelativePosition(t.path, uint(index), sideFromString(side))
}

// Resolve resolves a captured cursor back to a live codepoint index.
func (t *CrdtText) Resolve(pos []byte) (int, bool) {
	n, ok := t.doc.backend.ResolvePosition(t.path, pos)
	return int(n), ok
}

// MarkInfo is a resolved mark on a character: Name and, per the schema-declared
// flavor, its Value — a bool (boolean flavor), a native scalar (value flavor),
// or the covering element ids as [][]byte (object flavor, the default with no
// bound schema).
type MarkInfo struct {
	Name  string
	Value any
}

func markInfo(m Mark) MarkInfo {
	switch m.Flavor {
	case "bool":
		return MarkInfo{Name: string(m.Name), Value: m.Bool}
	case "object":
		return MarkInfo{Name: string(m.Name), Value: m.IDs}
	default: // value
		return MarkInfo{Name: string(m.Name), Value: nativeFromDiffScalar(m.Value)}
	}
}

// Mark authors a mark named name with native value over [start, end), returning
// the mark's handle (nil if the author was inert) and an error for an
// unsupported value type. The range grows with text inserted at its edges (start
// left-gravity, end right-gravity); use MarkWithGravity to choose.
func (t *CrdtText) Mark(start, end int, name string, value any) ([]byte, error) {
	return t.MarkWithGravity(start, end, "before", "after", name, value)
}

// MarkWithGravity authors a mark with explicit endpoint gravity ("before" =
// left, "after" = right).
func (t *CrdtText) MarkWithGravity(start, end int, startSide, endSide, name string, value any) ([]byte, error) {
	sc, err := marshalScalar(value)
	if err != nil {
		return nil, err
	}
	var markID []byte
	t.doc.mutate(func(b *Document) []byte {
		id, ops := b.Mark(t.path, uint(start), sideFromString(startSide), uint(end), sideFromString(endSide), []byte(name), sc)
		markID = id
		return ops
	})
	return markID, nil
}

// SetMarkValue changes the native value of the mark markID. Returns an error for
// an unsupported value type.
func (t *CrdtText) SetMarkValue(markID []byte, value any) error {
	sc, err := marshalScalar(value)
	if err != nil {
		return err
	}
	t.doc.mutate(func(b *Document) []byte { return b.MarkSetValue(markID, sc) })
	return nil
}

// DeleteMark tombstones the mark markID.
func (t *CrdtText) DeleteMark(markID []byte) {
	t.doc.mutate(func(b *Document) []byte { return b.MarkDelete(markID) })
}

// MarksAt reads the marks covering the character at index, each an ergonomic
// MarkInfo.
func (t *CrdtText) MarksAt(index int) []MarkInfo {
	raw := t.doc.backend.MarksAt(t.path, uint(index))
	out := make([]MarkInfo, 0, len(raw))
	for _, m := range raw {
		out = append(out, markInfo(m))
	}
	return out
}

// --- Xml ---

// CrdtXml is a live handle to an XML element or fragment. Children are addressed
// by live index — the core stores a child with no path of its own, so this
// handle edits a node's direct children (insert element/text, delete, tree-move)
// but does not recurse into a child element's contents (deep XML navigation is a
// core follow-on, matching the JS/Python SDKs' XML surface).
type CrdtXml struct {
	doc  *Doc
	path [][]byte
}

// Element installs a tagged XML element at this slot; returns the handle for
// chaining.
func (x *CrdtXml) Element(tag string) *CrdtXml {
	x.doc.mutate(func(b *Document) []byte { return b.XmlElement(x.path, []byte(tag)) })
	return x
}

// Fragment installs a tagless XML fragment at this slot; returns the handle.
func (x *CrdtXml) Fragment() *CrdtXml {
	x.doc.mutate(func(b *Document) []byte { return b.XmlFragment(x.path) })
	return x
}

// Tag reads this element's tag. The bool is false for a fragment or an absent
// node.
func (x *CrdtXml) Tag() (string, bool) {
	t, ok := x.doc.backend.XmlTag(x.path)
	if !ok {
		return "", false
	}
	return string(t), true
}

// Len returns the count of live children.
func (x *CrdtXml) Len() int {
	n, _ := x.doc.backend.XmlChildrenLen(x.path)
	return int(n)
}

// InsertElement inserts a child element with tag at a live child index; returns
// the handle.
func (x *CrdtXml) InsertElement(index int, tag string) *CrdtXml {
	x.doc.mutate(func(b *Document) []byte { return b.XmlInsertElement(x.path, uint(index), []byte(tag)) })
	return x
}

// InsertText inserts a text-run child holding text at a live child index;
// returns the handle.
func (x *CrdtXml) InsertText(index int, text string) *CrdtXml {
	x.doc.mutate(func(b *Document) []byte { return b.XmlInsertText(x.path, uint(index), text) })
	return x
}

// DeleteChild tombstones the child at a live index; returns the handle.
func (x *CrdtXml) DeleteChild(index int) *CrdtXml {
	x.doc.mutate(func(b *Document) []byte { return b.XmlChildDelete(x.path, uint(index)) })
	return x
}

// Move relocates this node's child at childIndex to destIndex in newParent's
// children — an identity-preserving tree move; returns the handle.
func (x *CrdtXml) Move(childIndex int, newParent *CrdtXml, destIndex int) *CrdtXml {
	dest := newParent.path
	x.doc.mutate(func(b *Document) []byte {
		return b.XmlMove(x.path, uint(childIndex), dest, uint(destIndex))
	})
	return x
}

// Observe subscribes to changes to this node's children (local edits and applied
// remote updates); returns a function that unsubscribes.
func (x *CrdtXml) Observe(cb func(ChangeEvent)) func() {
	return x.doc.addObserver(EncodePath(x.path), cb)
}
