package crdtsync

// Diff-derived ergonomic reactivity (§SDK-Ergonomic-Surface): an applied edit or
// remote update is diffed against a pre-edit snapshot and each raw change
// (byte-path + tagged scalars, from the core diff seam) is re-marshaled into an
// EventChange (native values, ergonomic key/index target). Snapshot+diff runs
// only when something is observing, so an unobserved Doc pays nothing.

import (
	"bytes"
	"encoding/binary"
)

// ChangeEvent is a change notification for an observed subtree, delivered to a
// handle's Observe callback. Origin is "local" or "remote"; Changes are the
// changes under the observed subtree.
type ChangeEvent struct {
	Origin  string
	Changes []EventChange
}

// RepairStep is one hop of a repair path: a map-slot Key or a sequence Index,
// discriminated by IsIndex.
type RepairStep struct {
	Key     string
	Index   int
	IsIndex bool
}

// RepairEvent is the schema-repair signal delivered to Doc.OnRepair: the located
// Paths whose repaired reading changed against the bound schema after an edit,
// each a sequence of steps (a map key or a sequence index). A repair names a
// location to re-read, not an edit, so it carries no origin.
type RepairEvent struct {
	Paths [][]RepairStep
}

// ContainerMarker stands in a list change's Values slot for an item that is a
// nested container rather than a scalar, naming the container Kind.
type ContainerMarker struct {
	Kind string
}

// EventChange is one diff-derived change. Kind names the variant and selects
// which fields are populated:
//
//	"update"      — Path, Old, New (native scalar)
//	"counter"     — Path, Old, New (int64)
//	"list_insert" — Path, Index, Values ([]any: native scalar or ContainerMarker)
//	"list_delete" — Path, Index, Values
//	"text_insert" — Path, Index, Text
//	"text_delete" — Path, Index, Text
//	"add"         — Path, ValueKind (the created element kind)
//	"remove"      — Path, ValueKind
//	"mark"        — Op ("add"/"remove"/"change"), Name, Old/New (native scalar)
type EventChange struct {
	Kind      string
	Path      []string
	Index     int
	Old       any
	New       any
	Text      string
	Values    []any
	ValueKind string
	Op        string
	Name      string
}

// changeWithPath pairs a re-marshaled change with the framed byte-path it
// targets, used for observer prefix matching (a mark change has an empty path).
type changeWithPath struct {
	pathBytes []byte
	change    EventChange
}

// nativeFromDiffScalar converts a diff-reported map-leaf scalar (a tagged
// Scalar) to a native value. A map leaf's Bytes carry the SDK string/binary
// discriminator; a list item's enveloped scalar instead decodes through
// unmarshalValue.
func nativeFromDiffScalar(s *Scalar) any {
	if s == nil {
		return nil
	}
	switch s.T {
	case "null":
		return nil
	case "bool":
		return s.Bool
	case "int":
		return s.Int
	case "bytes":
		p := s.Bytes
		if len(p) == 0 {
			return []byte(nil)
		}
		switch p[0] {
		case discString:
			return string(p[1:])
		case discBinary:
			return append([]byte(nil), p[1:]...)
		default:
			return append([]byte(nil), p...)
		}
	default: // blobref / elementref — no native leaf form
		return append([]byte(nil), s.Bytes...)
	}
}

// listItemValue is a native scalar for a leaf list item (whose stored bytes are
// a full enveloped scalar) or a ContainerMarker for a composite item.
func listItemValue(it Item) any {
	if it.Scalar != nil {
		return unmarshalValue(it.Scalar.Bytes)
	}
	return ContainerMarker{Kind: it.Kind}
}

// markChange re-marshals a mark change (which carries no path).
func markChange(raw Change) EventChange {
	name := string(raw.Name)
	switch raw.Op {
	case "markAdded":
		return EventChange{Kind: "mark", Op: "add", Name: name, New: nativeFromDiffScalar(raw.New)}
	case "markRemoved":
		return EventChange{Kind: "mark", Op: "remove", Name: name, Old: nativeFromDiffScalar(raw.Old)}
	default: // markChanged
		return EventChange{Kind: "mark", Op: "change", Name: name, Old: nativeFromDiffScalar(raw.Old), New: nativeFromDiffScalar(raw.New)}
	}
}

// remarshalChange re-marshals one raw diff change into an ergonomic EventChange
// plus its framed byte-path (for observer prefix matching).
func remarshalChange(raw Change) ([]byte, EventChange) {
	switch raw.Op {
	case "markAdded", "markRemoved", "markChanged":
		return nil, markChange(raw)
	}
	pathBytes := raw.Path
	p := decodePath(pathBytes)
	switch raw.Op {
	case "value":
		return pathBytes, EventChange{Kind: "update", Path: p, Old: nativeFromDiffScalar(raw.Old), New: nativeFromDiffScalar(raw.New)}
	case "counter":
		return pathBytes, EventChange{Kind: "counter", Path: p, Old: raw.OldInt, New: raw.NewInt}
	case "listInsert", "listDelete":
		kind := "list_insert"
		if raw.Op == "listDelete" {
			kind = "list_delete"
		}
		vals := make([]any, len(raw.Items))
		for i, it := range raw.Items {
			vals[i] = listItemValue(it)
		}
		return pathBytes, EventChange{Kind: kind, Path: p, Index: int(raw.Index), Values: vals}
	case "textInsert", "textDelete":
		kind := "text_insert"
		if raw.Op == "textDelete" {
			kind = "text_delete"
		}
		return pathBytes, EventChange{Kind: kind, Path: p, Index: int(raw.Index), Text: raw.Text}
	case "remove":
		return pathBytes, EventChange{Kind: "remove", Path: p, ValueKind: raw.Kind}
	default: // "add" and any future path-bearing op
		vk := raw.Kind
		if vk == "" {
			vk = raw.Op
		}
		return pathBytes, EventChange{Kind: "add", Path: p, ValueKind: vk}
	}
}

// decodePath decodes a length-framed path buffer (u32 key length + bytes per
// key) into its keys, rendered as Go strings.
func decodePath(data []byte) []string {
	var keys []string
	i := 0
	for i+4 <= len(data) {
		n := int(binary.LittleEndian.Uint32(data[i:]))
		i += 4
		if i+n > len(data) {
			break
		}
		keys = append(keys, keyString(data[i:i+n]))
		i += n
	}
	return keys
}

// pathStartsWith reports whether whole's framed bytes begin with prefix — a
// key-path prefix test, sound because each key is self-delimiting (length +
// bytes), so "a" never matches "ab".
func pathStartsWith(whole, prefix []byte) bool {
	return len(whole) >= len(prefix) && bytes.Equal(whole[:len(prefix)], prefix)
}
