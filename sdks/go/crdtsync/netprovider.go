package crdtsync

// The networked, socket-owning provider (§SDK-Ergonomic-Surface): the Go
// counterpart of the JS provider. It owns one wire session (a Client) and one
// room channel, and backs a Doc with that channel — so an ergonomic handle edit
// frames, outboxes, and sends in one step, and an inbound frame folds into the
// same replica and fires the doc's reactivity. One replica per room, never two
// divergent copies.
//
// A dropped socket leaves unacknowledged edits in the channel's outbox; the
// provider redials, resumes the channel from its caught-up position, and resends
// the outbox — so edits made offline converge once the link returns, and the
// server deduplicates a replayed op by its id rather than applying it twice.
//
// The offline-first Provider remains the seam for an app that owns its own
// transport; this is the batteries-included path for talking to a crdtsync
// server.

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Connection states a provider reports through State and OnState.
const (
	// StateConnecting is dialing, handshaking, or catching up.
	StateConnecting = "connecting"
	// StateConnected is synced: the room's state is caught up and edits stream.
	StateConnected = "connected"
	// StateDisconnected has no live socket; edits queue in the outbox.
	StateDisconnected = "disconnected"
)

// protocolVersion is the wire protocol version this SDK speaks, mirroring
// crdtsync_core::protocol::PROTOCOL_VERSION.
const protocolVersion = 1

// protocolMagic identifies a crdtsync stream, so a foreign connection is
// rejected at once — crdtsync_core::protocol::MAGIC.
const protocolMagic = "CRDT"

// protocolHeader is the 8-byte header a client writes once, before its Hello, to
// open a connection at this SDK's protocol version.
func protocolHeader() []byte {
	out := make([]byte, 8)
	copy(out, protocolMagic)
	binary.LittleEndian.PutUint32(out[4:], protocolVersion)
	return out
}

// The wire tags the provider reads off a frame directly, from
// crdtsync_core::protocol::encode_message. The core folds a frame's contents; the
// provider needs only to tell which frame answers which step of the handshake.
const (
	msgOps      = 2
	msgSnapshot = 4
	msgAuthOk   = 7
)

// isAuthOk reports whether a frame is the AuthOk that opens a session. A server
// may speak before it authenticates — an enforcing one answers the Hello with a
// SchemaAdvert — so "the frame folded cleanly" is not the same as "the session
// is authenticated", and subscribing on the strength of it is a violation.
func isAuthOk(frame []byte) bool {
	return len(frame) > 0 && frame[0] == msgAuthOk
}

// isCatchupReply reports whether a frame is the subscribe reply for channel: the
// Ops delta or the whole-replica Snapshot that carries the room's state. An
// awareness update or a schema advert riding ahead of it must not be mistaken
// for the room having synced.
func isCatchupReply(frame []byte, channel uint32) bool {
	if len(frame) < 5 || (frame[0] != msgOps && frame[0] != msgSnapshot) {
		return false
	}
	return binary.LittleEndian.Uint32(frame[1:]) == channel
}

// The handshake phase a socket is in. Each fresh socket restarts at phaseAuth:
// the actor a previous socket authenticated stays set on the client, so it
// cannot stand in for this socket having authenticated.
const (
	// phaseAuth awaits the AuthOk that opens the session.
	phaseAuth = iota
	// phaseCatchup awaits the subscribe reply carrying the room's state.
	phaseCatchup
	// phaseReady is synced: edits and updates stream.
	phaseReady
)

// ProviderOptions configures a networked provider. The zero value is valid: a
// random client id, an anonymous credential, a WebSocket transport, and
// reconnect enabled.
type ProviderOptions struct {
	// ClientID is a fixed 16-byte replica id; a random one is minted when empty.
	ClientID []byte

	// Credential authenticates the session, carried in the Auth frame. Defaults to
	// "anonymous", which a dev server accepts. To authenticate at the transport
	// upgrade instead, put the credential in Header.
	Credential string

	// AppID names the app this client speaks for and SchemaVersion the schema
	// version it targets, carried in the Hello. An empty AppID opens a relay
	// connection; a named app with version 0 adopts the server's head.
	AppID         []byte
	SchemaVersion uint32

	// Dial opens the socket. DialWebSocket when nil.
	Dial Dialer

	// Header carries extra headers to the transport upgrade. Setting Authorization
	// here implies AuthAtUpgrade.
	Header http.Header

	// AuthAtUpgrade declares that the connection is already authenticated when the
	// socket opens, so the provider owes no in-band Auth — offering one on an
	// authenticated session is a protocol violation. Set it whenever the
	// credential rides the upgrade by a carrier other than the Authorization
	// header (a cookie, the WebSocket subprotocol, a query parameter), or when the
	// server serves anonymous sessions. Leave it false to authenticate in band
	// with Credential, the carrier every transport supports.
	AuthAtUpgrade bool

	// DisableReconnect stops the provider redialing after an unexpected close.
	DisableReconnect bool

	// MaxReconnectDelay caps the reconnect backoff. Defaults to 10s.
	MaxReconnectDelay time.Duration

	// ConnectTimeout bounds the dial, the authentication that follows it, and the
	// subscribe-or-resume that asks for the room — on every connection rather than
	// only the first — and is how long Connect waits before giving up. Defaults to
	// 15s.
	ConnectTimeout time.Duration

	// CatchupTimeout bounds how long a fresh subscription waits for the reply
	// carrying the room's state. It is separate from ConnectTimeout and far more
	// generous: that reply is the whole room, the wire reports no progress while it
	// transfers, and a budget a large room cannot meet would sever every attempt
	// and never converge. It exists only so a server that accepts a Subscribe and
	// answers nothing eventually recycles the socket. Unset defaults to 5 minutes;
	// a negative value waits indefinitely. A socket that resumes does not use it —
	// its replica persisted, so the resume's delta is an ordinary update, not a
	// sync it waits on.
	CatchupTimeout time.Duration

	// OnError reports a server Error raised mid-session — UpdateRequired is the
	// onUpdateRequired signal, telling the app to prompt an update or fall back to
	// read-only. A handshake-time error instead fails Connect.
	OnError func(ErrorCode)

	// OnOpsRejected reports op batches the server refused (the author keeps their
	// bytes, so the app can show, discard, or export them).
	OnOpsRejected func([]Rejected)

	// OnRedirect reports rooms whose leader is elsewhere. The provider holds the
	// socket it has; dialing the leader is the app's call.
	OnRedirect func([]Redirect)
}

// NetProvider keeps a Doc in sync with a crdtsync server over one socket.
//
// Its state lives under the Doc's lock, shared so an inbound frame and a local
// edit never touch the replica at once. Listener callbacks run with it released,
// so a listener is free to edit the doc or drive the provider.
type NetProvider struct {
	doc *Doc

	client         *Client
	channel        uint32
	room           []byte
	subscribeFrame []byte
	credential     []byte
	// authAtUpgrade records that the connection is authenticated at accept, so the
	// server has already answered with an AuthOk and no in-band Auth is owed.
	authAtUpgrade bool

	url               string
	dial              Dialer
	connectTimeout    time.Duration
	catchupTimeout    time.Duration
	header            http.Header
	reconnect         bool
	maxReconnectDelay time.Duration

	onError       func(ErrorCode)
	onOpsRejected func([]Rejected)
	onRedirect    func([]Redirect)

	transport        Transport
	phase            int
	state            string
	stateListeners   listenerList[func(string)]
	reconnectAttempt int
	reconnectTimer   *time.Timer
	// generation identifies the current socket, so a read loop that outlives its
	// socket cannot act on a frame after a reconnect replaced it.
	generation    uint64
	closed        bool
	connectedOnce bool

	settled bool
	syncErr error
	synced  chan struct{}

	// sendMu orders what goes on the wire: it is taken before the doc's lock and
	// held across the handshake's transition and frames, so an edit authored in
	// that window queues behind the Subscribe rather than arriving on a channel
	// the server has not bound. The doc's lock is always released before the write
	// itself, so a congested socket stalls only what is waiting to send — for as
	// long as the transport's write deadline allows.
	sendMu sync.Mutex

	// cancelDial aborts the dial in flight, so Close does not leave a socket
	// opening into a provider that is already gone.
	cancelDial context.CancelFunc
}

// Connect opens a provider on url, joins room, and returns once the room's
// initial state has synced. It gives up on the first connection when ctx is done
// or opts.ConnectTimeout elapses, and closes what it opened before returning the
// error.
func Connect(ctx context.Context, url, room string, opts ProviderOptions) (*NetProvider, error) {
	p, err := NewNetProvider(url, room, opts)
	if err != nil {
		return nil, err
	}
	if err := p.WaitSynced(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

// NewNetProvider opens a provider and starts connecting in the background. The
// Doc is live immediately and empty until the room syncs; edits made before then
// queue in the channel's outbox. Use WaitSynced (or Connect) to wait for the
// initial state.
func NewNetProvider(url, room string, opts ProviderOptions) (*NetProvider, error) {
	clientID := opts.ClientID
	if len(clientID) == 0 {
		clientID = make([]byte, 16)
		if _, err := rand.Read(clientID); err != nil {
			return nil, err
		}
	}
	client, err := NewClient(clientID)
	if err != nil {
		return nil, err
	}
	if len(opts.AppID) > 0 || opts.SchemaVersion > 0 {
		client.DeclareApp(opts.AppID, opts.SchemaVersion)
	}

	credential := opts.Credential
	if credential == "" {
		credential = "anonymous"
	}
	header := opts.Header.Clone()
	if header == nil {
		header = http.Header{}
	}

	p := &NetProvider{
		client:            client,
		room:              []byte(room),
		credential:        []byte(credential),
		url:               url,
		authAtUpgrade:     opts.AuthAtUpgrade || header.Get("Authorization") != "",
		dial:              opts.Dial,
		connectTimeout:    opts.ConnectTimeout,
		catchupTimeout:    opts.CatchupTimeout,
		header:            header,
		reconnect:         !opts.DisableReconnect,
		maxReconnectDelay: opts.MaxReconnectDelay,
		onError:           opts.OnError,
		onOpsRejected:     opts.OnOpsRejected,
		onRedirect:        opts.OnRedirect,
		state:             StateConnecting,
		synced:            make(chan struct{}),
	}
	if p.dial == nil {
		p.dial = DialWebSocket
	}
	if p.maxReconnectDelay <= 0 {
		p.maxReconnectDelay = 10 * time.Second
	}
	if p.connectTimeout <= 0 {
		p.connectTimeout = 15 * time.Second
	}
	// Unset takes the generous default; a negative value is the caller asking for
	// no bound at all, which armDeadline reads as such.
	if p.catchupTimeout == 0 {
		p.catchupTimeout = 5 * time.Minute
	}
	channel, subscribeFrame, err := client.Subscribe(p.room)
	if err != nil {
		client.Close()
		return nil, err
	}
	p.channel, p.subscribeFrame = channel, subscribeFrame
	p.doc = newNetworkedDoc(NewClientBackend(client, p.channel), p.sendFrame)

	go p.openSocket(0)
	return p, nil
}

// Doc is the document this provider keeps in sync.
func (p *NetProvider) Doc() *Doc { return p.doc }

// Channel is the wire channel the room is subscribed on.
func (p *NetProvider) Channel() uint32 { return p.channel }

// State reports the current connection state.
func (p *NetProvider) State() string {
	p.doc.mu.Lock()
	defer p.doc.mu.Unlock()
	return p.state
}

// OnState observes connection-state transitions; returns a function that
// unsubscribes. Listeners fire in registration order, with the provider's lock
// released, so a listener is free to edit the doc or drive the provider.
func (p *NetProvider) OnState(cb func(string)) func() {
	p.doc.mu.Lock()
	defer p.doc.mu.Unlock()
	return p.doc.guarded(p.stateListeners.add(cb))
}

// WaitSynced blocks until the room's initial state has synced, returning the
// error that ended the attempt instead if the first connection failed or the
// provider was closed first. It gives up after ProviderOptions.ConnectTimeout or
// when ctx is done, and gives up only on waiting: the provider keeps dialing, so
// close it if the wait was the whole point.
func (p *NetProvider) WaitSynced(ctx context.Context) error {
	timer := time.NewTimer(p.connectTimeout)
	defer timer.Stop()
	select {
	case <-p.synced:
		p.doc.mu.Lock()
		defer p.doc.mu.Unlock()
		return p.syncErr
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errors.New("crdtsync: connection timed out")
	}
}

// OutboxLen reports how many authored ops await the server's acknowledgement —
// the offline queue depth.
func (p *NetProvider) OutboxLen() uint {
	p.doc.mu.Lock()
	defer p.doc.mu.Unlock()
	return p.client.OutboxLen(p.channel)
}

// Actor is the server-derived actor for this session, present once the
// handshake has completed.
func (p *NetProvider) Actor() ([]byte, bool) {
	p.doc.mu.Lock()
	defer p.doc.mu.Unlock()
	return p.client.Actor()
}

// SchemaVersion is the concrete schema version an enforcing server advertised
// for this session, present once its advert has arrived. A client that declared
// version 0 learns the served version here.
func (p *NetProvider) SchemaVersion() (uint32, bool) {
	p.doc.mu.Lock()
	defer p.doc.mu.Unlock()
	return p.client.ActiveSchemaVersion()
}

// Schema is the schema bytes an enforcing server advertised for this session,
// pairing with SchemaVersion.
func (p *NetProvider) Schema() ([]byte, bool) {
	p.doc.mu.Lock()
	defer p.doc.mu.Unlock()
	return p.client.ActiveSchema()
}

// SetAwareness publishes an ephemeral awareness entry (presence) for this client
// — a cursor, a selection, a name. Awareness is never durable: it fans out to
// the room's other subscribers and expires with the session.
func (p *NetProvider) SetAwareness(key string, value []byte) {
	p.doc.mu.Lock()
	frame := p.client.SetAwareness(p.channel, []byte(key), value)
	p.doc.mu.Unlock()
	if len(frame) > 0 {
		p.sendFrame(frame)
	}
}

// Awareness reads a peer's awareness entry by publishing actor and key.
func (p *NetProvider) Awareness(actor []byte, key string) ([]byte, bool) {
	p.doc.mu.Lock()
	defer p.doc.mu.Unlock()
	return p.client.Awareness(p.channel, actor, []byte(key))
}

// AwarenessLen reports how many awareness entries the room currently holds.
func (p *NetProvider) AwarenessLen() uint {
	p.doc.mu.Lock()
	defer p.doc.mu.Unlock()
	return p.client.AwarenessLen(p.channel)
}

// AclGrant authors a doc-ACL grant over the room, routed through the op path so
// it is acknowledged and resent like any edit. Returns the tuple id AclRevoke
// names it by; nil when the grant was malformed. grantor is the 16-byte doc-ACL
// actor key crediting the grant — pass nil to credit this session's
// authenticated actor.
func (p *NetProvider) AclGrant(subjectKind SubjectKind, subject []byte, grant Grant, effect Effect, path [][]byte, grantor []byte) ([]byte, error) {
	p.doc.mu.Lock()
	if grantor == nil {
		actor, ok := p.client.Actor()
		if !ok {
			p.doc.mu.Unlock()
			return nil, errors.New("crdtsync: no authenticated actor to credit the grant; pass a grantor")
		}
		grantor = ActorKey(actor)
	}
	id, frame := p.client.AclGrant(p.channel, subjectKind, subject, grant, effect, path, grantor)
	p.doc.mu.Unlock()
	if len(frame) > 0 {
		p.sendFrame(frame)
	}
	if id == nil {
		return nil, errors.New("crdtsync: the grant was refused (malformed subject, grant, or grantor)")
	}
	return id, nil
}

// AclRevoke tombstones the doc-ACL tuple tupleID (from AclGrant), routed through
// the op path.
func (p *NetProvider) AclRevoke(tupleID []byte) {
	p.doc.mu.Lock()
	frame := p.client.AclRevoke(p.channel, tupleID)
	p.doc.mu.Unlock()
	if len(frame) > 0 {
		p.sendFrame(frame)
	}
}

// Close drops the connection, stops reconnecting, and frees the wire session.
// Safe to call more than once, and from a listener.
func (p *NetProvider) Close() {
	p.doc.mu.Lock()
	if p.closed {
		p.doc.mu.Unlock()
		return
	}
	transport, notify := p.teardownLocked(errors.New("crdtsync: closed before it synced"))
	p.doc.mu.Unlock()

	if transport != nil {
		transport.Close()
	}
	notify()
}

// sendFrame transmits one frame when the socket can carry it. The doc's lock is
// released before the write, so a congested socket never blocks the replica —
// only what else is waiting to send. Two edits authored concurrently reach the
// server in whichever order takes the send lock, which is exactly what a CRDT op
// stream tolerates. An edit authored before the socket can carry it stays in the
// outbox and rides the resend after the next handshake.
func (p *NetProvider) sendFrame(frame []byte) {
	if len(frame) == 0 {
		return
	}
	p.sendMu.Lock()
	defer p.sendMu.Unlock()

	p.doc.mu.Lock()
	transport := p.transport
	// Before the socket is subscribed the server would refuse the frame as a
	// protocol violation; the outbox holds it instead.
	if p.closed || p.phase == phaseAuth {
		transport = nil
	}
	p.doc.mu.Unlock()
	if transport == nil {
		return
	}
	if err := transport.Send(frame); err != nil {
		// The read loop turns the broken socket into a disconnect and a reconnect.
		transport.Close()
	}
}

// sendSequence transmits frames as one uninterruptible run, so nothing an app
// goroutine authors can slot between them.
func (p *NetProvider) sendSequence(transport Transport, frames [][]byte) bool {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	return p.sendFramesLocked(transport, frames)
}

// sendFramesLocked writes frames in order; the caller holds the send lock.
func (p *NetProvider) sendFramesLocked(transport Transport, frames [][]byte) bool {
	for _, frame := range frames {
		if err := transport.Send(frame); err != nil {
			transport.Close()
			return false
		}
	}
	return true
}

// openSocket dials and drives one socket for its whole life. Every step carries
// its generation, so a socket abandoned by a reconnect or a Close acts on
// nothing once its successor has taken over.
func (p *NetProvider) openSocket(generation uint64) {
	p.doc.mu.Lock()
	if p.closed || p.generation != generation {
		p.doc.mu.Unlock()
		return
	}
	p.phase = phaseAuth
	notify := p.setStateLocked(StateConnecting)
	p.doc.mu.Unlock()
	notify()

	// Bound the dial so a server that accepts and stalls cannot wedge the socket
	// goroutine past the point where a reconnect should have been attempted, and
	// hand Close the cancel so it need not wait that bound out.
	dialCtx, cancel := context.WithTimeout(context.Background(), p.connectTimeout)
	p.doc.mu.Lock()
	if p.closed || p.generation != generation {
		p.doc.mu.Unlock()
		cancel()
		return
	}
	p.cancelDial = cancel
	p.doc.mu.Unlock()
	transport, err := p.dial(dialCtx, p.url, p.header)
	cancel()
	if err != nil {
		p.socketEnded(generation)
		return
	}

	p.doc.mu.Lock()
	if p.closed || p.generation != generation {
		p.doc.mu.Unlock()
		transport.Close()
		return
	}
	p.transport = transport
	opening := [][]byte{protocolHeader(), p.client.Hello()}
	if !p.authAtUpgrade {
		opening = append(opening, p.client.Auth(p.credential))
	}
	p.doc.mu.Unlock()

	// Reads block indefinitely once the room is synced — an idle collaborative
	// session is normal — so each stage before that carries its own deadline. A
	// server that upgrades and never authenticates, or accepts a Subscribe and
	// answers nothing, would otherwise hold the socket open with nothing to
	// reconnect from.
	deadline := p.armDeadline(generation, transport, p.connectTimeout, p.askedForTheRoomLocked)
	defer func() {
		if deadline != nil {
			deadline.Stop()
		}
	}()

	if !p.sendSequence(transport, opening) {
		p.socketEnded(generation)
		return
	}

	// The loop knows it is still handshaking without consulting the shared phase,
	// so a handshake frame can take the send lock before the doc's — the order an
	// app-goroutine edit already uses, and what keeps an edit from overtaking the
	// Subscribe.
	handshaking := true
	for {
		message, err := transport.Receive()
		if err != nil {
			transport.Close()
			p.socketEnded(generation)
			return
		}
		if handshaking {
			if !p.handleHandshakeFrame(generation, transport, message) {
				continue
			}
			handshaking = false
			// Authenticated. A fresh subscription now waits on the room's state
			// under its own, far longer budget; a reconnect is already synced and
			// waits on nothing.
			if deadline != nil {
				deadline.Stop()
				deadline = nil
			}
			if p.catchingUp() {
				deadline = p.armDeadline(generation, transport, p.catchupTimeout, p.syncedLocked)
			}
			continue
		}
		p.handleFrame(generation, message)
		if deadline != nil && !p.catchingUp() {
			deadline.Stop()
			deadline = nil
		}
	}
}

// armDeadline severs transport if this socket has not left the stage within
// budget. done reports, under the lock, whether the stage has been reached — so
// a stage that completed in the same instant the timer fired keeps its healthy
// socket. A budget at or below zero waits indefinitely.
func (p *NetProvider) armDeadline(generation uint64, transport Transport, budget time.Duration, done func() bool) *time.Timer {
	if budget <= 0 {
		return nil
	}
	return time.AfterFunc(budget, func() {
		p.doc.mu.Lock()
		stale := p.closed || p.generation != generation || done()
		p.doc.mu.Unlock()
		if !stale {
			transport.Close()
		}
	})
}

// askedForTheRoomLocked reports whether this socket's subscribe-or-resume
// reached the wire — the phase advances only once it did. It is what ends the
// connect stage: a socket that has asked for the room is waiting on the server,
// while one still trying to write is wedged and wants recycling.
func (p *NetProvider) askedForTheRoomLocked() bool { return p.phase != phaseAuth }

// syncedLocked reports whether the room has answered its subscription.
func (p *NetProvider) syncedLocked() bool { return p.phase == phaseReady }

// catchingUp reports whether the room is still waiting on its subscribe reply.
func (p *NetProvider) catchingUp() bool {
	p.doc.mu.Lock()
	defer p.doc.mu.Unlock()
	return !p.syncedLocked()
}

// handleFrame folds one inbound frame in and delivers everything it produced.
func (p *NetProvider) handleFrame(generation uint64, message []byte) {
	p.doc.mu.Lock()
	if p.closed || p.generation != generation {
		p.doc.mu.Unlock()
		return
	}

	var applied int
	var code ErrorCode
	deliver := p.doc.applyRemote(func() { applied, code = p.client.Receive(message) })
	rejected := p.client.TakeRejected()
	redirects := p.client.TakeRedirects()

	// The subscribe reply carrying the room's state is what completes the initial
	// sync, and the only thing that does: an awareness update, a schema advert, or
	// a refusal can all arrive first, and none of them means the room is caught up.
	// It is also what proves the socket serves this room, so it is where a
	// reconnect earns its backoff reset — an authenticated socket that then drops
	// must not wind the delay back to the floor on every cycle.
	var notify func()
	if applied == 1 && isCatchupReply(message, p.channel) {
		p.reconnectAttempt = 0
		if p.phase == phaseCatchup {
			notify = p.markConnectedLocked()
		}
	}
	p.doc.mu.Unlock()

	deliver()
	if notify != nil {
		notify()
	}
	if code != NoErrorCode {
		p.handleServerError(code)
	}
	if len(rejected) > 0 && p.onOpsRejected != nil {
		p.onOpsRejected(rejected)
	}
	if len(redirects) > 0 && p.onRedirect != nil {
		p.onRedirect(redirects)
	}
}

// handleHandshakeFrame folds a frame received before the session is
// authenticated — the AuthOk, whatever the server said ahead of it, or its
// refusal — and reports whether the handshake is now complete. It holds the send
// lock across the transition and the frames it produces, so an edit authored in
// that window queues behind the Subscribe rather than arriving on a channel the
// server has not bound.
func (p *NetProvider) handleHandshakeFrame(generation uint64, transport Transport, message []byte) bool {
	p.sendMu.Lock()
	p.doc.mu.Lock()
	if p.closed || p.generation != generation {
		p.doc.mu.Unlock()
		p.sendMu.Unlock()
		return false
	}
	applied, code := p.client.Receive(message)
	if code != NoErrorCode {
		p.doc.mu.Unlock()
		p.sendMu.Unlock()
		p.handleServerError(code)
		return false
	}
	if applied != 1 || !isAuthOk(message) {
		// A server frame that precedes the AuthOk — an enforcing server's schema
		// advert — has folded and is all it needs to be, and one the core refused
		// says nothing at all; either way the session is still unauthenticated, so
		// nothing may be sent on it yet.
		p.doc.mu.Unlock()
		p.sendMu.Unlock()
		return false
	}

	// Authenticated. A fresh session subscribes and waits for the room's state; a
	// reconnect resumes the channel from where it left off, which streams the
	// delta as ordinary ops — the replica persisted, so it is already synced.
	frames := make([][]byte, 0, 2)
	resumed := false
	if p.connectedOnce {
		if resume := p.client.Resume(p.channel); len(resume) > 0 {
			frames = append(frames, resume)
			resumed = true
		}
	}
	if !resumed {
		// A fresh subscription, or a channel with no position to resume from: the
		// room's state has to arrive before this socket is synced.
		frames = append(frames, p.subscribeFrame)
	}
	if p.client.OutboxLen(p.channel) > 0 {
		if resend := p.client.Resend(p.channel); len(resend) > 0 {
			frames = append(frames, resend)
		}
	}
	p.doc.mu.Unlock()

	sent := p.sendFramesLocked(transport, frames)
	if sent {
		// The room has been asked for: the send gate opens on the frames that
		// actually reached the wire, and from here the wait is the server's to
		// answer under whichever budget the stage carries. A socket whose write
		// failed stays shut — it holds no bound channel to edit onto.
		p.doc.mu.Lock()
		p.phase = phaseCatchup
		p.doc.mu.Unlock()
	}
	p.sendMu.Unlock()
	if !sent {
		return false
	}

	// A resumed socket is synced the moment its resume is away — the replica
	// persisted across the drop. Published after the frames so nothing observes
	// "connected" on a socket whose resume never made it out, and only if the
	// provider still wants this socket: a Close landing during the write has
	// already announced the session finished.
	if resumed {
		p.doc.mu.Lock()
		var notify func()
		if !p.closed && p.generation == generation {
			notify = p.markConnectedLocked()
		}
		p.doc.mu.Unlock()
		if notify != nil {
			notify()
		}
	}
	return true
}

// handleServerError routes a server Error: one raised before the session ever
// synced is fatal (a bad credential, an unsupported version), while one raised
// mid-session is an app-level signal.
func (p *NetProvider) handleServerError(code ErrorCode) {
	p.doc.mu.Lock()
	if p.closed {
		p.doc.mu.Unlock()
		return
	}
	if p.connectedOnce {
		p.doc.mu.Unlock()
		if p.onError != nil {
			p.onError(code)
		}
		return
	}
	transport, notify := p.teardownLocked(fmt.Errorf("crdtsync: server rejected the connection (code %d)", code))
	p.doc.mu.Unlock()

	if transport != nil {
		transport.Close()
	}
	notify()
}

// teardownLocked closes the provider for good: it stops reconnecting, abandons
// the socket, settles the initial-sync wait with err, and frees the wire
// session. Freeing under the lock is what makes it safe — every other client
// call holds the same lock, and the C ABI answers a freed handle inertly.
// Returns the transport to close and the notification to run, both once the lock
// is released.
func (p *NetProvider) teardownLocked(err error) (Transport, func()) {
	p.closed = true
	if p.reconnectTimer != nil {
		p.reconnectTimer.Stop()
		p.reconnectTimer = nil
	}
	if p.cancelDial != nil {
		p.cancelDial()
		p.cancelDial = nil
	}
	transport := p.transport
	p.transport = nil
	p.settleLocked(err)
	notify := p.setStateLocked(StateDisconnected)
	p.client.Close()
	return transport, notify
}

// socketEnded records that the socket for generation is gone and schedules the
// redial.
func (p *NetProvider) socketEnded(generation uint64) {
	p.doc.mu.Lock()
	if p.closed || p.generation != generation {
		p.doc.mu.Unlock()
		return
	}
	p.transport = nil
	p.phase = phaseAuth
	notify := p.setStateLocked(StateDisconnected)
	if !p.reconnect {
		p.settleLocked(errors.New("crdtsync: the connection closed before it synced"))
		p.doc.mu.Unlock()
		notify()
		return
	}

	delay := time.Duration(250*(1<<min(p.reconnectAttempt, 16))) * time.Millisecond
	if delay > p.maxReconnectDelay {
		delay = p.maxReconnectDelay
	}
	p.reconnectAttempt++
	p.generation++
	next := p.generation
	p.reconnectTimer = time.AfterFunc(delay, func() { p.openSocket(next) })
	p.doc.mu.Unlock()
	notify()
}

// markConnectedLocked publishes the room as synced — settling the initial-sync
// wait the first time. Returns the notification to run once the lock is
// released.
func (p *NetProvider) markConnectedLocked() func() {
	p.phase = phaseReady
	p.connectedOnce = true
	p.settleLocked(nil)
	return p.setStateLocked(StateConnected)
}

// settleLocked resolves the initial-sync wait, once. A later failure never
// overwrites a successful sync.
func (p *NetProvider) settleLocked(err error) {
	if p.settled {
		return
	}
	p.settled = true
	p.syncErr = err
	close(p.synced)
}

// setStateLocked records a state transition and returns the notification to run
// once the lock is released.
func (p *NetProvider) setStateLocked(state string) func() {
	if state == p.state {
		return func() {}
	}
	p.state = state
	listeners := p.stateListeners.snapshot()
	return func() {
		for _, l := range listeners {
			l(state)
		}
	}
}
