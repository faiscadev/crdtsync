package crdtsync

import (
	"testing"
)

// collectUpdates subscribes and returns a pointer to the captured events plus
// the unsubscribe function.
func collectUpdates(d *Doc) (*[]UpdateEvent, func()) {
	var got []UpdateEvent
	off := d.OnUpdate(func(e UpdateEvent) { got = append(got, e) })
	return &got, off
}

func TestReactivityValueChangeShape(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	d.GetMap("root").Set("name", "old")
	got, _ := collectUpdates(d)
	d.GetMap("root").Set("name", "new")
	if len(*got) != 1 {
		t.Fatalf("expected 1 update, got %d", len(*got))
	}
	ch := (*got)[0]
	if ch.Origin != "local" || len(ch.Changes) != 1 {
		t.Fatalf("event: origin=%s changes=%d", ch.Origin, len(ch.Changes))
	}
	c := ch.Changes[0]
	if c.Kind != "update" {
		t.Fatalf("kind: %s", c.Kind)
	}
	if len(c.Path) != 2 || c.Path[0] != "root" || c.Path[1] != "name" {
		t.Fatalf("path: %v", c.Path)
	}
	if c.Old != "old" || c.New != "new" {
		t.Fatalf("old=%#v new=%#v", c.Old, c.New)
	}
}

func TestReactivityListInsertShape(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	d.GetList("l").Append("seed") // pre-create so the next edit is fine-grained
	got, _ := collectUpdates(d)
	d.GetList("l").Append(int64(7))
	changes := lastChanges(t, got)
	c := findKind(t, changes, "list_insert")
	if c.Index != 1 || len(c.Values) != 1 || c.Values[0] != int64(7) {
		t.Fatalf("list_insert: index=%d values=%#v", c.Index, c.Values)
	}
}

func TestReactivityTextInsertShape(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	d.GetText("tx").Insert(0, "ab")
	got, _ := collectUpdates(d)
	d.GetText("tx").Insert(2, "cd")
	c := findKind(t, lastChanges(t, got), "text_insert")
	if c.Index != 2 || c.Text != "cd" {
		t.Fatalf("text_insert: index=%d text=%q", c.Index, c.Text)
	}
}

func TestReactivityCounterShape(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	d.Backend().Inc(path("root", "cnt"), 1)
	got, _ := collectUpdates(d)
	d.Backend().Inc(path("root", "cnt"), 5)
	// The Inc goes through the backend directly, bypassing Doc.mutate, so no
	// event fires — assert reactivity only reflects handle-driven edits.
	if len(*got) != 0 {
		t.Fatalf("backend edit should not fire the ergonomic update path, got %d", len(*got))
	}
}

func TestReactivityLateListenerSeesOnlyFuture(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	d.GetMap("m").Set("a", int64(1)) // before any listener
	got, _ := collectUpdates(d)
	d.GetMap("m").Set("b", int64(2))
	if len(*got) != 1 {
		t.Fatalf("late listener saw %d events, want 1", len(*got))
	}
}

func TestReactivityUnsubscribeStopsComputation(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	got, off := collectUpdates(d)
	d.GetMap("m").Set("a", int64(1))
	off()
	d.GetMap("m").Set("b", int64(2))
	if len(*got) != 1 {
		t.Fatalf("after unsubscribe saw %d events, want 1", len(*got))
	}
}

func TestReactivitySubtreeObserveOnly(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	d.GetMap("a").Set("x", int64(0))
	d.GetMap("b").Set("y", int64(0))
	var aChanges []EventChange
	off := d.GetMap("a").Observe(func(e ChangeEvent) { aChanges = append(aChanges, e.Changes...) })
	defer off()

	d.GetMap("b").Set("y", int64(9)) // outside the observed subtree
	if len(aChanges) != 0 {
		t.Fatalf("observer fired for a sibling subtree: %#v", aChanges)
	}
	d.GetMap("a").Set("x", int64(1)) // inside
	if len(aChanges) != 1 || aChanges[0].Kind != "update" {
		t.Fatalf("observer missed its own subtree change: %#v", aChanges)
	}
}

func TestReactivityRemoteOrigin(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	b := newErgoDoc(t, 2)
	defer b.Close()
	a.GetMap("m").Set("k", int64(0)) // pre-create on a

	var localOps []byte
	a.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			localOps = e.Ops
		}
	})
	a.GetMap("m").Set("k", int64(1))

	// b must already have the container so the remote edit is a fine-grained value.
	b.GetMap("m").Set("k", int64(0))
	var remote []UpdateEvent
	b.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "remote" {
			remote = append(remote, e)
		}
	})
	b.ApplyUpdate(localOps)
	if len(remote) != 1 {
		t.Fatalf("expected 1 remote update, got %d", len(remote))
	}
	if len(remote[0].Changes) == 0 || remote[0].Changes[0].Kind != "update" {
		t.Fatalf("remote change shape: %#v", remote[0].Changes)
	}
}

var repairSchema = []byte(`{
    "schema": "notes", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "body": "Body" } },
        "Body": { "kind": "text", "max": 5 }
    }
}`)

func TestRepairFiresOnOverflow(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	if !d.SetSchema(repairSchema) {
		t.Fatal("schema did not bind")
	}
	var paths [][]RepairStep
	d.OnRepair(func(e RepairEvent) { paths = append(paths, e.Paths...) })
	d.GetText("body").Insert(0, "hello world") // 11 > max 5
	if !hasBodyPath(paths) {
		t.Fatalf("expected a repair path [body], got %#v", paths)
	}
}

func TestRepairSilentOnConforming(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	if !d.SetSchema(repairSchema) {
		t.Fatal("schema did not bind")
	}
	fired := 0
	d.OnRepair(func(e RepairEvent) { fired++ })
	d.GetText("body").Insert(0, "hi") // within max 5
	if fired != 0 {
		t.Fatalf("conforming edit fired %d repairs", fired)
	}
}

func TestRepairSilentWithoutSchema(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	fired := 0
	d.OnRepair(func(e RepairEvent) { fired++ })
	d.GetText("body").Insert(0, "a very long body over any bound")
	if fired != 0 {
		t.Fatalf("no schema bound but fired %d repairs", fired)
	}
}

func TestRepairStopsAfterUnsubscribe(t *testing.T) {
	d := newErgoDoc(t, 1)
	defer d.Close()
	if !d.SetSchema(repairSchema) {
		t.Fatal("schema did not bind")
	}
	fired := 0
	off := d.OnRepair(func(e RepairEvent) { fired++ })
	d.GetText("body").Insert(0, "overflowing")
	off()
	d.GetText("body").Insert(0, "more overflow")
	if fired != 1 {
		t.Fatalf("expected 1 repair fire, got %d", fired)
	}
}

// --- helpers ---

func lastChanges(t *testing.T, got *[]UpdateEvent) []EventChange {
	t.Helper()
	if len(*got) == 0 {
		t.Fatal("no update events")
	}
	return (*got)[len(*got)-1].Changes
}

func findKind(t *testing.T, changes []EventChange, kind string) EventChange {
	t.Helper()
	for _, c := range changes {
		if c.Kind == kind {
			return c
		}
	}
	t.Fatalf("no %q change in %#v", kind, changes)
	return EventChange{}
}

func hasBodyPath(paths [][]RepairStep) bool {
	for _, p := range paths {
		if len(p) == 1 && !p[0].IsIndex && p[0].Key == "body" {
			return true
		}
	}
	return false
}
