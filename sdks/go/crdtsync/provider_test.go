package crdtsync

import "testing"

// link binds two providers so each provider's outbound send feeds the other's
// Receive — an in-memory transport for the convergence tests.
func link(pa, pb *Provider) {
	pa.send = func(ops []byte) { pb.Receive(ops) }
	pb.send = func(ops []byte) { pa.Receive(ops) }
	pa.Connect()
	pb.Connect()
}

func TestProviderTwoDocsSync(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	b := newErgoDoc(t, 2)
	defer b.Close()
	pa := NewProvider(a, nil)
	pb := NewProvider(b, nil)
	link(pa, pb)

	a.GetMap("root").Set("name", "alice")
	if v, _ := b.GetMap("root").Get("name"); v != "alice" {
		t.Fatalf("peer name: %#v", v)
	}
	b.GetList("l").Append("x")
	if v, _ := a.GetList("l").Get(0); v != "x" {
		t.Fatalf("peer list: %#v", v)
	}
}

func TestProviderNoEchoLoop(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	b := newErgoDoc(t, 2)
	defer b.Close()
	pa := NewProvider(a, nil)
	pb := NewProvider(b, nil)
	link(pa, pb)

	// A single edit must terminate — a remote apply never re-emits, so this
	// returns rather than looping forever.
	a.GetMap("root").Set("k", int64(1))
	if v, _ := b.GetMap("root").Get("k"); v != int64(1) {
		t.Fatalf("peer k: %#v", v)
	}
}

func TestProviderOfflineFlushOnConnect(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	b := newErgoDoc(t, 2)
	defer b.Close()
	pb := NewProvider(b, nil)
	pb.Connect()
	pa := NewProvider(a, func(ops []byte) { pb.Receive(ops) })

	// pa is disconnected — edits queue in its outbox.
	a.GetMap("m").Set("a", int64(1))
	a.GetMap("m").Set("b", int64(2))
	if pa.OutboxLen() != 2 {
		t.Fatalf("outbox: %d", pa.OutboxLen())
	}
	if _, ok := b.GetMap("m").Get("a"); ok {
		t.Fatal("b should not have the offline edits yet")
	}
	pa.Connect()
	if pa.OutboxLen() != 0 {
		t.Fatalf("outbox after connect: %d", pa.OutboxLen())
	}
	if v, _ := b.GetMap("m").Get("a"); v != int64(1) {
		t.Fatalf("peer a after flush: %#v", v)
	}
	if v, _ := b.GetMap("m").Get("b"); v != int64(2) {
		t.Fatalf("peer b after flush: %#v", v)
	}
}

func TestProviderStateTransitionsNotify(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	p := NewProvider(a, func([]byte) {})
	var states []string
	p.OnState(func(s string) { states = append(states, s) })
	if p.State() != "disconnected" {
		t.Fatalf("initial state: %s", p.State())
	}
	p.Connect()
	p.Disconnect()
	p.Disconnect() // no-op, same state
	p.Connect()
	want := []string{"connected", "disconnected", "connected"}
	if len(states) != len(want) {
		t.Fatalf("states: %v", states)
	}
	for i, s := range want {
		if states[i] != s {
			t.Fatalf("states: %v want %v", states, want)
		}
	}
}

func TestProviderRemoteReactivityOnReceive(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	b := newErgoDoc(t, 2)
	defer b.Close()
	// Pre-create the container on b so the remote edit is a fine-grained value.
	a.GetMap("m").Set("k", int64(0))
	b.GetMap("m").Set("k", int64(0))

	pb := NewProvider(b, nil)
	pb.Connect()
	var remote []UpdateEvent
	b.OnUpdate(func(e UpdateEvent) {
		if e.Origin == "remote" {
			remote = append(remote, e)
		}
	})
	pa := NewProvider(a, func(ops []byte) { pb.Receive(ops) })
	pa.Connect()

	a.GetMap("m").Set("k", int64(9))
	if len(remote) != 1 || len(remote[0].Changes) == 0 {
		t.Fatalf("remote reactivity: %#v", remote)
	}
}

func TestProviderCloseStopsForwarding(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	b := newErgoDoc(t, 2)
	defer b.Close()
	pb := NewProvider(b, nil)
	pb.Connect()
	pa := NewProvider(a, func(ops []byte) { pb.Receive(ops) })
	pa.Connect()

	a.GetMap("m").Set("a", int64(1))
	pa.Close()
	a.GetMap("m").Set("b", int64(2)) // not forwarded after Close
	if v, _ := b.GetMap("m").Get("a"); v != int64(1) {
		t.Fatalf("peer a: %#v", v)
	}
	if _, ok := b.GetMap("m").Get("b"); ok {
		t.Fatal("edit after Close should not forward")
	}
}

// Listeners fire in registration order (matching the JS/Python reference), not
// the random order a Go map would give.
func TestProviderStateListenersFireInOrder(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	p := NewProvider(a, func([]byte) {})
	var order []int
	for i := 0; i < 5; i++ {
		i := i
		p.OnState(func(string) { order = append(order, i) })
	}
	p.Connect()
	want := []int{0, 1, 2, 3, 4}
	if len(order) != len(want) {
		t.Fatalf("order: %v", order)
	}
	for i, v := range want {
		if order[i] != v {
			t.Fatalf("listener order: %v want %v", order, want)
		}
	}
}

func TestProviderConnectedEditSendsImmediately(t *testing.T) {
	a := newErgoDoc(t, 1)
	defer a.Close()
	sent := 0
	p := NewProvider(a, func([]byte) { sent++ })
	p.Connect()
	a.GetMap("m").Set("k", int64(1))
	if sent != 1 {
		t.Fatalf("connected edit sent %d times, want 1", sent)
	}
	if p.OutboxLen() != 0 {
		t.Fatalf("connected edit should not queue, outbox=%d", p.OutboxLen())
	}
}
