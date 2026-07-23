package crdtsync

// Provider is the ergonomic, offline-first sync binding over a Doc's apply/emit
// seam — the Go realization of the §SDK-Ergonomic-Surface provider model.
//
// Like the Python SDK, the Go SDK is embedded/offline-first: it owns no socket
// loop, so the app supplies the transport. Bind a Doc with a send callback
// (invoked with each local edit's ops to transmit) and feed a peer's ops to
// Receive. The provider owns the connection state and an offline outbox, so
// edits made while disconnected queue and flush in order on reconnect; inbound
// ops apply and fire the doc's reactivity as "remote". A remote apply never
// re-emits as a local edit, so a pair of linked providers can't loop.
//
// The fully-networked provider that owns a socket and backs the Doc with a
// single wire-client replica (the JS connect(url, room) model, with awareness
// and the operator-tier request/reply surface) is a documented follow-on shared
// with Python: the Go Client wire surface does not yet expose the per-channel
// list/text/scalar/map-key handle ops a single-replica networked handle graph
// needs. Until then this seam plus the low-level Client cover sync.
type Provider struct {
	doc            *Doc
	send           func([]byte)
	state          string
	outbox         [][]byte
	stateListeners listenerList[func(string)]
	unsub          func()
}

// NewProvider binds doc to an app-supplied transport. send is invoked with each
// local edit's ops when connected. The provider starts disconnected — call
// Connect to go online (which flushes any queued edits).
func NewProvider(doc *Doc, send func([]byte)) *Provider {
	p := &Provider{doc: doc, send: send, state: "disconnected"}
	p.unsub = doc.OnUpdate(p.onUpdate)
	return p
}

func (p *Provider) onUpdate(e UpdateEvent) {
	// Only a local edit is transmitted; a remote apply must not echo (or a pair
	// of linked providers would loop forever).
	if e.Origin != "local" {
		return
	}
	if p.state == "connected" {
		p.send(e.Ops)
	} else {
		p.outbox = append(p.outbox, e.Ops)
	}
}

// Receive folds a peer's ops into the bound doc (firing "remote" reactivity);
// returns the count applied.
func (p *Provider) Receive(ops []byte) int {
	return p.doc.ApplyUpdate(ops)
}

// State reports the connection state: "connected" or "disconnected".
func (p *Provider) State() string { return p.state }

// OutboxLen reports how many local edits are queued awaiting a reconnect flush.
func (p *Provider) OutboxLen() int { return len(p.outbox) }

// Connect marks the transport connected and flushes the offline outbox in order.
func (p *Provider) Connect() {
	p.setState("connected")
	pending := p.outbox
	p.outbox = nil
	for _, ops := range pending {
		p.send(ops)
	}
}

// Disconnect marks the transport disconnected; subsequent local edits queue.
func (p *Provider) Disconnect() { p.setState("disconnected") }

// OnState observes connection-state changes; returns a function that
// unsubscribes.
func (p *Provider) OnState(cb func(string)) func() {
	return p.stateListeners.add(cb)
}

// Close unbinds from the doc; local edits stop being forwarded/queued.
func (p *Provider) Close() {
	if p.unsub != nil {
		p.unsub()
		p.unsub = nil
	}
}

func (p *Provider) setState(state string) {
	if state == p.state {
		return
	}
	p.state = state
	for _, l := range p.stateListeners.snapshot() {
		l(state)
	}
}
