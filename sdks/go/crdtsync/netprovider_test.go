package crdtsync

// The networked provider's spec, driven over a fake transport so every step of
// the connection lifecycle is deterministic and no server is needed. The frames
// the fake server sends are encoded exactly as crdtsync_core::protocol's
// encode_message writes them and are decoded by the real core, so a wrong shape
// fails loudly here rather than passing as a plausible one.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"
)

// --- server-side wire frames ---

// Wire tags, from crdtsync_core::protocol::encode_message.
const (
	tagHello           = 0
	tagSubscribe       = 1
	tagOps             = 2
	tagError           = 3
	tagSnapshot        = 4
	tagAuth            = 6
	tagAuthOk          = 7
	tagAwarenessSet    = 8
	tagAwarenessUpdate = 9
	tagOpsRejected     = 22
	tagRedirect        = 23
	tagSchemaAdvert    = 21
)

func frameSchemaAdvert(version uint32, schema []byte) []byte {
	out := binary.LittleEndian.AppendUint32([]byte{tagSchemaAdvert}, version)
	return framedBytes(out, schema)
}

func framedBytes(out []byte, b []byte) []byte {
	out = binary.LittleEndian.AppendUint32(out, uint32(len(b)))
	return append(out, b...)
}

func frameAuthOk(actor []byte) []byte {
	return framedBytes([]byte{tagAuthOk}, actor)
}

func frameOps(channel uint32, ops []byte) []byte {
	out := binary.LittleEndian.AppendUint32([]byte{tagOps}, channel)
	return append(out, ops...)
}

func frameSnapshot(channel uint32, seq uint64, state []byte) []byte {
	out := binary.LittleEndian.AppendUint32([]byte{tagSnapshot}, channel)
	out = binary.LittleEndian.AppendUint64(out, seq)
	return framedBytes(out, state)
}

func frameError(code ErrorCode) []byte {
	out := binary.LittleEndian.AppendUint16([]byte{tagError}, uint16(code))
	out = framedBytes(out, []byte("refused"))
	return framedBytes(out, nil)
}

func frameAwarenessUpdate(channel uint32, actor, key, value []byte) []byte {
	out := binary.LittleEndian.AppendUint32([]byte{tagAwarenessUpdate}, channel)
	out = framedBytes(out, actor)
	out = framedBytes(out, key)
	return framedBytes(out, value)
}

func frameOpsRejected(channel uint32, reason ErrorCode, seqs []uint64) []byte {
	out := binary.LittleEndian.AppendUint32([]byte{tagOpsRejected}, channel)
	out = binary.LittleEndian.AppendUint16(out, uint16(reason))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(seqs)))
	for _, seq := range seqs {
		out = binary.LittleEndian.AppendUint64(out, seq)
	}
	return out
}

func frameRedirect(room, leaderAddr []byte) []byte {
	out := framedBytes([]byte{tagRedirect}, room)
	return framedBytes(out, leaderAddr)
}

// subscribeLastSeen reads the caught-up position off a Subscribe frame — what
// tells a resume apart from a fresh subscribe.
func subscribeLastSeen(t *testing.T, frame []byte) uint64 {
	t.Helper()
	if len(frame) == 0 || frame[0] != tagSubscribe {
		t.Fatalf("not a subscribe frame: %v", frame)
	}
	i := 5                   // tag + channel
	for k := 0; k < 3; k++ { // room, branch, zone
		if i+4 > len(frame) {
			t.Fatalf("truncated subscribe frame")
		}
		i += 4 + int(binary.LittleEndian.Uint32(frame[i:]))
	}
	if i+8 > len(frame) {
		t.Fatalf("truncated subscribe frame")
	}
	return binary.LittleEndian.Uint64(frame[i:])
}

// --- the fake transport + server ---

// fakeTransport is one socket the test drives from both ends: frames the
// provider sends land in sent, frames the test pushes are delivered to the
// provider's read loop, and Break severs it the way a dropped connection does.
type fakeTransport struct {
	sent   chan []byte
	inbox  chan []byte
	closed chan struct{}
	once   sync.Once

	mu sync.Mutex
	// sends counts what the provider has written, so a test can fail a chosen
	// frame; keepAlive models an app-supplied transport that reports a transient
	// write error without the socket actually dying.
	sends     int
	failAt    int
	stallAt   int
	stall     chan struct{}
	keepAlive bool
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		sent:   make(chan []byte, 64),
		inbox:  make(chan []byte, 64),
		closed: make(chan struct{}),
	}
}

func (f *fakeTransport) Send(message []byte) error {
	select {
	case <-f.closed:
		return errors.New("fake transport: closed")
	default:
	}
	f.mu.Lock()
	f.sends++
	fail := f.sends == f.failAt
	var stall chan struct{}
	if f.sends == f.stallAt {
		stall = f.stall
	}
	f.mu.Unlock()
	if stall != nil {
		select {
		case <-stall:
		case <-f.closed:
			return errors.New("fake transport: closed")
		}
	}
	if fail {
		return errors.New("fake transport: send refused")
	}
	f.sent <- append([]byte(nil), message...)
	return nil
}

func (f *fakeTransport) Receive() ([]byte, error) {
	select {
	case m := <-f.inbox:
		return m, nil
	case <-f.closed:
		return nil, io.EOF
	}
}

func (f *fakeTransport) Close() error {
	f.mu.Lock()
	keepAlive := f.keepAlive
	f.mu.Unlock()
	if keepAlive {
		return nil
	}
	f.once.Do(func() { close(f.closed) })
	return nil
}

// failSendAt makes the nth send report a transient error while the socket stays
// up, so a write failure is not the same thing as a dropped connection. Pair it
// with sever, or the read loop never lets go.
func (f *fakeTransport) failSendAt(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAt = n
	f.keepAlive = true
}

// keepOpen makes Close a no-op on this socket, so a write parked mid-handshake
// still completes after the provider tears down. Pair it with sever.
func (f *fakeTransport) keepOpen() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keepAlive = true
}

// stallSendAt holds the nth send until the returned release is called, so a test
// can park the provider inside a chosen stage.
func (f *fakeTransport) stallSendAt(n int) func() {
	gate := make(chan struct{})
	f.mu.Lock()
	f.stallAt = n
	f.stall = gate
	f.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// sever ends the socket regardless of keepAlive, releasing the read loop.
func (f *fakeTransport) sever() {
	f.mu.Lock()
	f.keepAlive = false
	f.mu.Unlock()
	f.Close()
}

// push delivers a server frame to the provider.
func (f *fakeTransport) push(frame []byte) { f.inbox <- frame }

// next takes the frame the provider sent, failing the test if none arrives.
func (f *fakeTransport) next(t *testing.T) []byte {
	t.Helper()
	select {
	case frame := <-f.sent:
		return frame
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a frame from the provider")
		return nil
	}
}

// nextTagged takes frames until one carries tag, so an assertion is not thrown
// off by an unrelated frame (an ack, a resend) riding ahead of it.
func (f *fakeTransport) nextTagged(t *testing.T, tag byte) []byte {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case frame := <-f.sent:
			if len(frame) > 0 && frame[0] == tag {
				return frame
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a frame tagged %d", tag)
			return nil
		}
	}
}

// handshake plays the server side of the opening exchange: it takes the header,
// Hello, and Auth, answers with AuthOk, then takes the Subscribe and answers
// with an empty catch-up — leaving the provider synced.
func (f *fakeTransport) handshake(t *testing.T, channel uint32) {
	t.Helper()
	f.next(t) // protocol header
	f.next(t) // hello
	f.next(t) // auth
	f.push(frameAuthOk([]byte("actor-1")))
	f.nextTagged(t, tagSubscribe)
	f.push(frameOps(channel, nil))
}

// fakeServer hands out fakeTransports and remembers them in dial order, so a
// test can drive the socket the provider is on and watch it open the next one.
type fakeServer struct {
	mu      sync.Mutex
	conns   []*fakeTransport
	dialErr error
}

func (s *fakeServer) dial(context.Context, string, http.Header) (Transport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dialErr != nil {
		return nil, s.dialErr
	}
	tr := newFakeTransport()
	s.conns = append(s.conns, tr)
	return tr, nil
}

// conn waits for the index'th socket the provider opened.
func (s *fakeServer) conn(t *testing.T, index int) *fakeTransport {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for {
		s.mu.Lock()
		if index < len(s.conns) {
			tr := s.conns[index]
			s.mu.Unlock()
			return tr
		}
		s.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for socket %d", index)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// newFakeProvider opens a provider over server, with the reconnect backoff wound
// down so a test never waits on it.
func newFakeProvider(t *testing.T, server *fakeServer, mutate func(*ProviderOptions)) *NetProvider {
	t.Helper()
	opts := ProviderOptions{
		Dial:              server.dial,
		MaxReconnectDelay: 5 * time.Millisecond,
		ConnectTimeout:    2 * time.Second,
	}
	if mutate != nil {
		mutate(&opts)
	}
	p, err := NewNetProvider("ws://fake/room", "room", opts)
	if err != nil {
		t.Fatalf("NewNetProvider: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

// waitFor polls until the condition holds, so a test never sleeps a fixed span
// waiting on the provider's read loop.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// emptyState is a decodable snapshot of an untouched replica — the state a
// catch-up carries for a room with nothing in it yet.
func emptyState(t *testing.T) []byte {
	t.Helper()
	doc, err := New(cid(8))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer doc.Close()
	return doc.EncodeState()
}

// peerOps authors an edit on a standalone replica and returns its raw ops — a
// peer's work for the fake server to fan out.
func peerOps(t *testing.T, clientID byte, edit func(*Document) []byte) []byte {
	t.Helper()
	doc, err := New(cid(clientID))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer doc.Close()
	return edit(doc)
}

// --- the handshake ---

func TestProviderHandshakeOrdering(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)

	header := tr.next(t)
	if string(header[:4]) != protocolMagic || binary.LittleEndian.Uint32(header[4:]) != protocolVersion {
		t.Fatalf("first frame is not the protocol header: %v", header)
	}
	if hello := tr.next(t); hello[0] != tagHello {
		t.Fatalf("second frame tag %d, want Hello", hello[0])
	}
	if auth := tr.next(t); auth[0] != tagAuth {
		t.Fatalf("third frame tag %d, want Auth", auth[0])
	}

	// Nothing else goes out until the server authenticates the socket — a
	// Subscribe ahead of AuthOk is a protocol violation.
	select {
	case frame := <-tr.sent:
		t.Fatalf("sent tag %d before AuthOk", frame[0])
	case <-time.After(50 * time.Millisecond):
	}
	if p.State() != StateConnecting {
		t.Fatalf("state %q before the handshake completes", p.State())
	}

	tr.push(frameAuthOk([]byte("actor-1")))
	if sub := tr.nextTagged(t, tagSubscribe); subscribeLastSeen(t, sub) != 0 {
		t.Fatal("a first subscribe must ask for the room from the start")
	}
}

// A server may speak before it authenticates — an enforcing one answers the
// Hello with a schema advert. Subscribing on the strength of that frame is a
// protocol violation, so only the AuthOk may open the session.
func TestProviderWaitsForAuthOkNotJustAnyFrame(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, func(o *ProviderOptions) {
		o.AppID = []byte("app")
		o.SchemaVersion = 1
	})
	tr := server.conn(t, 0)
	tr.next(t) // protocol header
	tr.next(t) // hello
	tr.next(t) // auth

	tr.push(frameSchemaAdvert(1, []byte(`{"schema":1,"root":"Root","types":{"Root":{"map":{}}}}`)))
	select {
	case frame := <-tr.sent:
		t.Fatalf("sent tag %d on the strength of a schema advert", frame[0])
	case <-time.After(80 * time.Millisecond):
	}
	if v, ok := p.SchemaVersion(); !ok || v != 1 {
		t.Fatalf("the advert was not folded: version=%d ok=%v", v, ok)
	}

	tr.push(frameAuthOk([]byte("actor-1")))
	tr.nextTagged(t, tagSubscribe)
}

// The initial sync completes on the subscribe reply, not on whatever frame
// happens to arrive first — an awareness update means nothing about the room
// having been caught up.
func TestProviderAwarenessDoesNotCompleteTheCatchup(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.next(t)
	tr.next(t)
	tr.next(t)
	tr.push(frameAuthOk([]byte("actor-1")))
	tr.nextTagged(t, tagSubscribe)

	tr.push(frameAwarenessUpdate(p.Channel(), []byte("peer-1"), []byte("cursor"), []byte("1")))
	waitFor(t, "the awareness to arrive", func() bool { return p.AwarenessLen() > 0 })
	if p.State() != StateConnecting {
		t.Fatalf("state %q — an awareness frame completed the catch-up", p.State())
	}

	tr.push(frameOps(p.Channel(), nil))
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
}

// A frame the core does not apply — one addressed to a channel this session does
// not hold — is not a subscribe reply, so it cannot complete the catch-up.
func TestProviderUnappliedFrameDoesNotCompleteTheCatchup(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.next(t)
	tr.next(t)
	tr.next(t)
	tr.push(frameAuthOk([]byte("actor-1")))
	tr.nextTagged(t, tagSubscribe)

	tr.push(frameOps(p.Channel()+1, nil))
	// The awareness frame behind it proves the stray was processed, without itself
	// completing the catch-up.
	tr.push(frameAwarenessUpdate(p.Channel(), []byte("peer-1"), []byte("cursor"), []byte("1")))
	waitFor(t, "the frame behind the stray to arrive", func() bool { return p.AwarenessLen() > 0 })
	if p.State() != StateConnecting {
		t.Fatalf("state %q — a frame the core refused completed the catch-up", p.State())
	}

	tr.push(frameOps(p.Channel(), nil))
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced after the real reply: %v", err)
	}
}

// An edit authored while the handshake is mid-flight must queue behind the
// Subscribe: ops on a channel the server has not bound yet are a protocol
// violation that closes the connection.
func TestProviderEditCannotOvertakeTheSubscribe(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.next(t) // protocol header
	tr.next(t) // hello
	tr.next(t) // auth

	// Editors already contending for the wire when the AuthOk lands: the frames
	// they author must not slip out between the handshake opening the gate and the
	// Subscribe reaching the socket. Several of them, so one is reliably mid-send
	// at the moment the gate opens rather than by luck of the scheduler.
	stop := make(chan struct{})
	var editors sync.WaitGroup
	for w := 0; w < 8; w++ {
		editors.Add(1)
		go func(w int) {
			defer editors.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				p.Doc().GetMap("root").Set(fmt.Sprintf("k%d", w), int64(i))
			}
		}(w)
	}
	tr.push(frameAuthOk([]byte("actor-1")))

	frame := tr.next(t)
	close(stop)
	// Keep draining: an editor parked in Send holds the send lock, and the others
	// would never reach their stop check.
	drained := make(chan struct{})
	defer close(drained)
	go func() {
		for {
			select {
			case <-tr.sent:
			case <-drained:
				return
			}
		}
	}()
	editors.Wait()
	if frame[0] != tagSubscribe {
		t.Fatalf("frame tag %d reached the wire before the Subscribe", frame[0])
	}
}

// A server that upgrades the socket and then says nothing must not hold the
// provider open forever: the handshake carries its own deadline, and expiring it
// drops the socket into the ordinary reconnect.
func TestProviderHandshakeDeadlineDropsASilentSocket(t *testing.T) {
	server := &fakeServer{}
	newFakeProvider(t, server, func(o *ProviderOptions) { o.ConnectTimeout = 500 * time.Millisecond })
	first := server.conn(t, 0)
	first.next(t)
	first.next(t)
	first.next(t)

	// No AuthOk ever comes; the deadline must sever the socket and redial.
	server.conn(t, 1)
}

// A socket whose subscribe cannot reach the wire is wedged, not waiting, so the
// connect deadline is what recycles it.
func TestProviderConnectDeadlineRecyclesAWedgedHandshakeWrite(t *testing.T) {
	server := &fakeServer{}
	newFakeProvider(t, server, func(o *ProviderOptions) { o.ConnectTimeout = 150 * time.Millisecond })
	tr := server.conn(t, 0)
	tr.next(t)
	tr.next(t)
	tr.next(t)

	release := tr.stallSendAt(4) // the Subscribe never lands
	defer release()
	tr.push(frameAuthOk([]byte("actor-1")))

	// The deadline must sever the socket and redial rather than wait on the write.
	server.conn(t, 1)
}

// The stage a socket reached does not carry to its successor: a reconnect whose
// own handshake write wedges must be recycled just like a first connection's.
func TestProviderConnectDeadlineRecyclesAWedgedReconnectWrite(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, func(o *ProviderOptions) { o.ConnectTimeout = 150 * time.Millisecond })
	first := server.conn(t, 0)
	first.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
	first.Close()

	second := server.conn(t, 1)
	second.next(t)
	second.next(t)
	second.next(t)
	release := second.stallSendAt(4) // the Resume never lands
	defer release()
	second.push(frameAuthOk([]byte("actor-1")))

	// The deadline must sever this socket too, not stand down on what the first
	// one managed to send.
	server.conn(t, 2)
}

// A Close that lands while the handshake is still writing has already announced
// the session finished; the write completing afterwards must not undo that.
func TestProviderCloseDuringTheHandshakeWriteStaysFinal(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	first := server.conn(t, 0)
	first.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
	first.Close()

	second := server.conn(t, 1)
	second.next(t)
	second.next(t)
	second.next(t)

	// Park the reconnect inside its Resume write, close, then let the write finish.
	// The socket survives the Close, so the write really does land afterwards.
	second.keepOpen()
	defer second.sever()
	release := second.stallSendAt(4)
	defer release()
	var mu sync.Mutex
	var states []string
	p.OnState(func(s string) {
		mu.Lock()
		states = append(states, s)
		mu.Unlock()
	})
	second.push(frameAuthOk([]byte("actor-1")))
	waitFor(t, "the resume write to start", func() bool {
		second.mu.Lock()
		defer second.mu.Unlock()
		return second.sends >= 4
	})
	p.Close()
	release()
	// The frame only proves the parked write returned; the resumed block runs
	// after it, so give it room to get the announcement wrong.
	second.nextTagged(t, tagSubscribe)
	time.Sleep(100 * time.Millisecond)
	if p.State() != StateDisconnected {
		t.Fatalf("state %q after Close", p.State())
	}
	mu.Lock()
	defer mu.Unlock()
	for _, s := range states {
		if s == StateConnected {
			t.Fatalf("a closed provider announced itself connected: %v", states)
		}
	}
}

// A resumed socket is synced the moment its resume is away, so it waits on no
// catch-up budget — even one far too short for a room to answer within.
func TestProviderReconnectWaitsOnNoCatchupBudget(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, func(o *ProviderOptions) { o.CatchupTimeout = 20 * time.Millisecond })
	first := server.conn(t, 0)
	first.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
	first.Close()

	second := server.conn(t, 1)
	second.next(t)
	second.next(t)
	second.next(t)
	second.push(frameAuthOk([]byte("actor-1")))
	second.nextTagged(t, tagSubscribe)
	waitFor(t, "the reconnect to report connected", func() bool { return p.State() == StateConnected })

	// Well past the catch-up budget with no reply: a resumed socket is not waiting
	// on one, so nothing severs it.
	time.Sleep(120 * time.Millisecond)
	if p.State() != StateConnected {
		t.Fatalf("state %q — a resumed socket was held to the catch-up budget", p.State())
	}
}

// The catch-up carries its own budget: a server that authenticates, takes the
// Subscribe, and then never answers it leaves the room unsynced on a live socket,
// which is the same wedge as one that never authenticates at all — but it must be
// bounded far more loosely, since that reply is the whole room.
func TestProviderCatchupTimeoutDropsAnUnansweredSubscribe(t *testing.T) {
	server := &fakeServer{}
	newFakeProvider(t, server, func(o *ProviderOptions) { o.CatchupTimeout = 300 * time.Millisecond })
	first := server.conn(t, 0)
	first.next(t)
	first.next(t)
	first.next(t)
	first.push(frameAuthOk([]byte("actor-1")))
	first.nextTagged(t, tagSubscribe)

	// No catch-up reply ever comes; the deadline must sever the socket and redial.
	server.conn(t, 1)
}

// The subscribe reply carries the whole room and the wire reports no progress
// while it transfers, so it must not share the handshake's budget: a room slower
// to arrive than ConnectTimeout would be severed on every attempt and never
// converge at all.
func TestProviderSlowCatchupStillSyncs(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, func(o *ProviderOptions) { o.ConnectTimeout = 100 * time.Millisecond })
	tr := server.conn(t, 0)
	tr.next(t)
	tr.next(t)
	tr.next(t)
	tr.push(frameAuthOk([]byte("actor-1")))
	tr.nextTagged(t, tagSubscribe)

	// Well past the handshake budget, but the catch-up has its own.
	time.Sleep(300 * time.Millisecond)
	ops := peerOps(t, 9, func(d *Document) []byte {
		scalar, _ := marshalValue("late")
		return d.SetScalar(path("root", "title"), scalar)
	})
	tr.push(frameOps(p.Channel(), ops))

	// Waiting on the state rather than WaitSynced, whose own budget is the very
	// ConnectTimeout this test deliberately sets short; the settled error is then
	// read back without waiting.
	waitFor(t, "the slow catch-up to sync", func() bool { return p.State() == StateConnected })
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("a slow catch-up must settle cleanly: %v", err)
	}
	if v, _ := p.Doc().GetMap("root").Get("title"); v != "late" {
		t.Fatalf("caught-up title: %#v", v)
	}
}

// A frame the core refuses says nothing about the session, whatever tag it
// carries — the AuthOk gate holds to the same standard as the catch-up gate.
func TestProviderUnappliedAuthOkDoesNotOpenTheSession(t *testing.T) {
	server := &fakeServer{}
	newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.next(t)
	tr.next(t)
	tr.next(t)

	// Tagged as an AuthOk but truncated, so the core cannot decode it.
	tr.push([]byte{tagAuthOk, 0xFF})
	select {
	case frame := <-tr.sent:
		t.Fatalf("sent tag %d on a frame the core refused", frame[0])
	case <-time.After(80 * time.Millisecond):
	}
}

// A Subscribe that never reached the server leaves the channel unbound. A
// transport that reports a transient write error is still up, so nothing tears
// the socket down for us — the socket itself must go back to withholding edits
// rather than putting ops on a channel the server never bound.
func TestProviderFailedSubscribeWithholdsEdits(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.next(t)
	tr.next(t)
	tr.next(t)

	defer tr.sever()

	tr.failSendAt(4) // the Subscribe alone; later writes succeed
	tr.push(frameAuthOk([]byte("actor-1")))
	waitFor(t, "the failed subscribe to unbind the socket", func() bool {
		tr.mu.Lock()
		attempted := tr.sends >= 4
		tr.mu.Unlock()
		p.doc.mu.Lock()
		defer p.doc.mu.Unlock()
		return attempted && p.phase == phaseAuth
	})

	p.Doc().GetMap("root").Set("k", int64(1))
	select {
	case frame := <-tr.sent:
		t.Fatalf("sent tag %d on a channel the Subscribe never bound", frame[0])
	case <-time.After(80 * time.Millisecond):
	}
	if p.OutboxLen() == 0 {
		t.Fatal("an edit on an unsubscribed socket must stay in the outbox")
	}
}

func TestProviderCatchupCompletesTheSync(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.next(t)
	tr.next(t)
	tr.next(t)
	tr.push(frameAuthOk([]byte("actor-1")))
	tr.nextTagged(t, tagSubscribe)

	// Still catching up: the subscribe reply has not arrived.
	if p.State() != StateConnecting {
		t.Fatalf("state %q while catching up", p.State())
	}

	// The catch-up carries a peer's existing edit, so the doc holds the room's
	// state the moment the sync completes.
	ops := peerOps(t, 9, func(d *Document) []byte {
		scalar, _ := marshalValue("hello")
		return d.SetScalar(path("root", "title"), scalar)
	})
	tr.push(frameOps(p.Channel(), ops))

	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
	if p.State() != StateConnected {
		t.Fatalf("state %q after the catch-up", p.State())
	}
	if v, _ := p.Doc().GetMap("root").Get("title"); v != "hello" {
		t.Fatalf("caught-up title: %#v", v)
	}
}

// The send gate opens as soon as the room has been asked for: an edit authored
// while the catch-up is still in flight goes out. Withholding it would strand it
// until the next reconnect, since the outbox is only replayed by a handshake.
func TestProviderEditDuringCatchupReachesTheWire(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.next(t)
	tr.next(t)
	tr.next(t)
	tr.push(frameAuthOk([]byte("actor-1")))
	tr.nextTagged(t, tagSubscribe)

	// Subscribed, not yet caught up.
	if p.State() != StateConnecting {
		t.Fatalf("state %q before the catch-up reply", p.State())
	}
	p.Doc().GetMap("root").Set("k", int64(1))

	frame := tr.nextTagged(t, tagOps)
	if binary.LittleEndian.Uint32(frame[1:]) != p.Channel() {
		t.Fatal("the edit framed the wrong channel")
	}
}

func TestProviderCatchupFromSnapshot(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.next(t)
	tr.next(t)
	tr.next(t)
	tr.push(frameAuthOk([]byte("actor-1")))
	tr.nextTagged(t, tagSubscribe)

	peer, err := New(cid(9))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer peer.Close()
	scalar, _ := marshalValue(int64(42))
	peer.SetScalar(path("root", "n"), scalar)
	tr.push(frameSnapshot(p.Channel(), 7, peer.EncodeState()))

	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
	if v, _ := p.Doc().GetMap("root").Get("n"); v != int64(42) {
		t.Fatalf("snapshot-caught-up n: %#v", v)
	}
}

// --- edits ---

func TestProviderEditFramesAndSends(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}

	var updates []UpdateEvent
	var mu sync.Mutex
	p.Doc().OnUpdate(func(e UpdateEvent) {
		mu.Lock()
		updates = append(updates, e)
		mu.Unlock()
	})

	p.Doc().GetMap("root").Set("title", "Hello")
	frame := tr.nextTagged(t, tagOps)
	if binary.LittleEndian.Uint32(frame[1:]) != p.Channel() {
		t.Fatalf("edit framed for the wrong channel")
	}
	// The edit landed on the provider's own replica too, read back through the
	// same handle graph.
	if v, _ := p.Doc().GetMap("root").Get("title"); v != "Hello" {
		t.Fatalf("own edit reads back as %#v", v)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(updates) != 1 || updates[0].Origin != "local" {
		t.Fatalf("local update events: %#v", updates)
	}
	if len(updates[0].Ops) == 0 {
		t.Fatal("a local update must report the bytes it put on the wire")
	}
}

func TestProviderRemoteFrameAppliesToTheDoc(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}

	ops := peerOps(t, 9, func(d *Document) []byte { return d.TextInsert(path("body"), 0, "hi") })
	tr.push(frameOps(p.Channel(), ops))

	waitFor(t, "the remote text to apply", func() bool { return p.Doc().GetText("body").String() == "hi" })
}

// A frame replayed after a reconnect must not apply twice — the op ids make the
// fold idempotent, which is what lets the outbox resend freely.
func TestProviderRepeatedFrameAppliesOnce(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}

	ops := peerOps(t, 9, func(d *Document) []byte { return d.TextInsert(path("body"), 0, "abc") })
	tr.push(frameOps(p.Channel(), ops))
	waitFor(t, "the remote text to apply", func() bool { return p.Doc().GetText("body").String() == "abc" })
	tr.push(frameOps(p.Channel(), ops))
	tr.push(frameOps(p.Channel(), ops))
	// A frame that lands after the replays proves they were processed, so the
	// reading below is taken with the double-apply (if any) already folded.
	// A different replica, so the marker's op ids cannot collide with the replayed
	// ones and be deduplicated as already applied.
	fresh := peerOps(t, 10, func(d *Document) []byte { return d.TextInsert(path("marker"), 0, "!") })
	tr.push(frameOps(p.Channel(), fresh))
	waitFor(t, "the frame behind the replays to apply", func() bool {
		return p.Doc().GetText("marker").String() == "!"
	})

	if got := p.Doc().GetText("body").String(); got != "abc" {
		t.Fatalf("replayed ops applied more than once: %q", got)
	}
}

// --- reactivity over the channel replica ---

// The channel replica is snapshot-and-diffed like a local one, so an inbound
// frame fires the doc's reactivity rather than silently mutating it.
func TestProviderRemoteFrameFiresReactivity(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}

	// Seed the slot, so the observed change is a fine-grained value update rather
	// than the container's creation.
	seed := peerOps(t, 9, func(d *Document) []byte {
		scalar, _ := marshalValue("first")
		return d.SetScalar(path("root", "k"), scalar)
	})
	tr.push(frameOps(p.Channel(), seed))
	waitFor(t, "the seed to apply", func() bool {
		v, _ := p.Doc().GetMap("root").Get("k")
		return v == "first"
	})

	var mu sync.Mutex
	var remote []UpdateEvent
	p.Doc().OnUpdate(func(e UpdateEvent) {
		if e.Origin == "remote" {
			mu.Lock()
			remote = append(remote, e)
			mu.Unlock()
		}
	})

	ops := peerOps(t, 10, func(d *Document) []byte {
		d.Apply(seed)
		scalar, _ := marshalValue("second")
		return d.SetScalar(path("root", "k"), scalar)
	})
	tr.push(frameOps(p.Channel(), ops))

	waitFor(t, "the remote update event", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(remote) > 0
	})
	mu.Lock()
	defer mu.Unlock()
	e := remote[0]
	if len(e.Changes) == 0 {
		t.Fatal("a remote update must report the changes it made")
	}
	c := e.Changes[0]
	if c.Kind != "update" || c.New != "second" || c.Old != "first" {
		t.Fatalf("remote change: %#v", c)
	}
}

// A subtree observer sees a remote change under its own path and nothing else.
func TestProviderObserveFiresForARemoteChange(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}

	var mu sync.Mutex
	var seen []ChangeEvent
	p.Doc().GetMap("watched").Observe(func(e ChangeEvent) {
		mu.Lock()
		seen = append(seen, e)
		mu.Unlock()
	})

	ops := peerOps(t, 9, func(d *Document) []byte {
		scalar, _ := marshalValue(int64(7))
		return d.SetScalar(path("watched", "n"), scalar)
	})
	tr.push(frameOps(p.Channel(), ops))
	waitFor(t, "the observer to fire", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) > 0
	})

	mu.Lock()
	defer mu.Unlock()
	if seen[0].Origin != "remote" || len(seen[0].Changes) == 0 {
		t.Fatalf("observed event: %#v", seen[0])
	}

	// A change outside the observed subtree does not reach it.
	before := len(seen)
	other := peerOps(t, 10, func(d *Document) []byte {
		scalar, _ := marshalValue(int64(1))
		return d.SetScalar(path("elsewhere", "n"), scalar)
	})
	tr.push(frameOps(p.Channel(), other))
	waitFor(t, "the unrelated change to apply", func() bool {
		v, _ := p.Doc().GetMap("elsewhere").Get("n")
		return v == int64(1)
	})
	if len(seen) != before {
		t.Fatalf("observer fired for a change outside its subtree: %#v", seen[before:])
	}
}

// A local edit reports its diff too, not just the bytes it put on the wire.
func TestProviderLocalEditReportsItsChanges(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}

	// Create the text first: on an empty doc the edit's diff is the container
	// appearing, not an insert into it.
	p.Doc().GetText("body").Insert(0, "he")

	var mu sync.Mutex
	var local []UpdateEvent
	p.Doc().OnUpdate(func(e UpdateEvent) {
		if e.Origin == "local" {
			mu.Lock()
			local = append(local, e)
			mu.Unlock()
		}
	})
	p.Doc().GetText("body").Insert(2, "llo")

	mu.Lock()
	defer mu.Unlock()
	if len(local) != 1 {
		t.Fatalf("local update events: %d", len(local))
	}
	var inserted bool
	for _, c := range local[0].Changes {
		if c.Kind == "text_insert" && c.Text == "llo" && c.Index == 2 {
			inserted = true
		}
	}
	if !inserted {
		t.Fatalf("local changes: %#v", local[0].Changes)
	}
}

// A peer's frame folding in while a transaction is open must not be credited to
// that transaction: the frame fires its own remote event with the right changes,
// and the commit reports only that it happened.
func TestProviderRemoteFrameDuringTransactionIsNotCreditedToIt(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
	doc := p.Doc()

	seed := peerOps(t, 9, func(d *Document) []byte {
		scalar, _ := marshalValue("seed")
		return d.SetScalar(path("root", "peer"), scalar)
	})
	tr.push(frameOps(p.Channel(), seed))
	waitFor(t, "the seed to apply", func() bool {
		v, _ := doc.GetMap("root").Get("peer")
		return v == "seed"
	})
	doc.GetMap("root").Set("mine", "before")

	var mu sync.Mutex
	var events []UpdateEvent
	doc.OnUpdate(func(e UpdateEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})

	peer := peerOps(t, 10, func(d *Document) []byte {
		d.Apply(seed)
		scalar, _ := marshalValue("from-peer")
		return d.SetScalar(path("root", "peer"), scalar)
	})
	doc.Transact(func() {
		doc.GetMap("root").Set("mine", "after")
		tr.push(frameOps(p.Channel(), peer))
		waitFor(t, "the peer's frame to land mid-transaction", func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(events) > 0
		})
	})

	mu.Lock()
	defer mu.Unlock()
	var remote, local []UpdateEvent
	for _, e := range events {
		if e.Origin == "remote" {
			remote = append(remote, e)
		} else {
			local = append(local, e)
		}
	}
	if len(remote) != 1 || len(remote[0].Changes) != 1 {
		t.Fatalf("remote events: %#v", remote)
	}
	if remote[0].Changes[0].New != "from-peer" {
		t.Fatalf("remote change: %#v", remote[0].Changes[0])
	}
	if len(local) != 1 {
		t.Fatalf("local events: %#v", local)
	}
	for _, c := range local[0].Changes {
		if len(c.Path) == 2 && c.Path[1] == "peer" {
			t.Fatalf("the transaction was credited with the peer's change: %#v", c)
		}
	}
	// The group still rode the wire, and both edits are in the replica.
	if len(local[0].Ops) == 0 {
		t.Fatal("the transaction reported no ops")
	}
	if v, _ := doc.GetMap("root").Get("mine"); v != "after" {
		t.Fatalf("own edit: %#v", v)
	}
	if v, _ := doc.GetMap("root").Get("peer"); v != "from-peer" {
		t.Fatalf("peer edit: %#v", v)
	}
}

// The rich reads answer off the channel replica: blobs, xml, cursors, marks, and
// counters are all live on a networked doc.
func TestProviderRichReadsOnTheChannelReplica(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
	doc := p.Doc()

	if !doc.GetMap("m").SetBlob("logo", "image/png", []byte{1, 2, 3}) {
		t.Fatal("SetBlob did not inline")
	}
	if ref, ok := doc.GetMap("m").GetBlob("logo"); !ok || ref.Mime != "image/png" {
		t.Fatalf("blob read back: %#v ok=%v", ref, ok)
	}

	x := doc.GetXml("doc").Element("section")
	x.InsertText(0, "hello")
	if tag, ok := x.Tag(); !ok || tag != "section" {
		t.Fatalf("xml tag: %q ok=%v", tag, ok)
	}
	if got := x.Len(); got != 1 {
		t.Fatalf("xml children: %d", got)
	}

	text := doc.GetText("body")
	text.Insert(0, "abcdef")
	pos := text.RelativePosition(3, "before")
	if pos == nil {
		t.Fatal("no cursor captured")
	}
	text.Insert(0, "xy")
	if at, ok := text.Resolve(pos); !ok || at != 5 {
		t.Fatalf("cursor resolved to %d ok=%v", at, ok)
	}

	markID, err := text.Mark(0, 3, "bold", true)
	if err != nil {
		t.Fatalf("Mark: %v", err)
	}
	if markID == nil {
		t.Fatal("Mark authored nothing")
	}
	marks := text.MarksAt(1)
	if len(marks) == 0 || marks[0].Name != "bold" {
		t.Fatalf("marks at 1: %#v", marks)
	}
	// No schema binds to a channel replica, so a mark keeps the default object
	// flavor — its value is the covering element ids, not the authored bool.
	if _, ok := marks[0].Value.([][]byte); !ok {
		t.Fatalf("mark flavor: %#v", marks[0].Value)
	}
	// The documented exception, asserted so it fails loudly if a per-channel seat
	// ever lands and this comment goes stale.
	if doc.SetSchema([]byte(`{"schema":1,"root":"Root","types":{"Root":{"map":{}}}}`)) {
		t.Fatal("a networked doc bound a schema; the seam grew a per-channel setter")
	}

	doc.Backend().Inc(path("counters", "hits"), 4)
	if n, ok := doc.Backend().GetCounter(path("counters", "hits")); !ok || n != 4 {
		t.Fatalf("counter read back: %d ok=%v", n, ok)
	}
}

// --- the offline outbox ---

func TestProviderOfflineEditsQueueThenResend(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.next(t) // header
	tr.next(t) // hello
	tr.next(t) // auth

	// The socket is up but unauthenticated: an edit here cannot go out, so it
	// waits in the channel's outbox.
	p.Doc().GetMap("root").Set("a", int64(1))
	p.Doc().GetMap("root").Set("b", int64(2))
	// The outbox counts ops, not calls: seeding the root map is an op of its own.
	if p.OutboxLen() < 2 {
		t.Fatalf("outbox %d, want the offline edits queued", p.OutboxLen())
	}
	select {
	case frame := <-tr.sent:
		t.Fatalf("an unauthenticated socket sent tag %d", frame[0])
	case <-time.After(50 * time.Millisecond):
	}

	tr.push(frameAuthOk([]byte("actor-1")))
	tr.nextTagged(t, tagSubscribe)
	// The subscribe is followed by the outbox replay, carrying both edits.
	resend := tr.nextTagged(t, tagOps)
	if binary.LittleEndian.Uint32(resend[1:]) != p.Channel() {
		t.Fatal("the resend framed the wrong channel")
	}
	if len(resend) <= 5 {
		t.Fatal("the resend carried no ops")
	}
}

// --- reconnect ---

func TestProviderReconnectResumesAndResends(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	first := server.conn(t, 0)
	first.next(t)
	first.next(t)
	first.next(t)
	first.push(frameAuthOk([]byte("actor-1")))
	first.nextTagged(t, tagSubscribe)
	// Catch up at server sequence 4, so a resume must ask to continue from there.
	first.push(frameSnapshot(p.Channel(), 4, emptyState(t)))
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}

	// An edit the server never acknowledges stays outstanding.
	p.Doc().GetMap("root").Set("k", int64(1))
	first.nextTagged(t, tagOps)
	if p.OutboxLen() == 0 {
		t.Fatal("an unacknowledged edit must stay in the outbox")
	}

	first.Close()
	waitFor(t, "the provider to notice the drop", func() bool { return p.State() != StateConnected })

	second := server.conn(t, 1)
	second.next(t) // header
	second.next(t) // hello
	second.next(t) // auth
	second.push(frameAuthOk([]byte("actor-1")))

	resume := second.nextTagged(t, tagSubscribe)
	if got := subscribeLastSeen(t, resume); got != 4 {
		t.Fatalf("resume asked from %d, want the caught-up position 4", got)
	}
	if resend := second.nextTagged(t, tagOps); len(resend) <= 5 {
		t.Fatal("the reconnect carried no outbox resend")
	}
	// A reconnect is already synced — the replica persisted across the drop.
	waitFor(t, "the reconnect to report connected", func() bool { return p.State() == StateConnected })
	if v, _ := p.Doc().GetMap("root").Get("k"); v != int64(1) {
		t.Fatalf("the edit did not survive the reconnect: %#v", v)
	}
}

func TestProviderEditWhileDisconnectedRidesTheReconnect(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, func(o *ProviderOptions) { o.MaxReconnectDelay = time.Second })
	first := server.conn(t, 0)
	first.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}

	first.Close()
	waitFor(t, "the provider to notice the drop", func() bool { return p.State() == StateDisconnected })
	p.Doc().GetMap("root").Set("offline", "yes")
	if p.OutboxLen() == 0 {
		t.Fatal("an edit made while disconnected must queue")
	}

	second := server.conn(t, 1)
	second.next(t)
	second.next(t)
	second.next(t)
	second.push(frameAuthOk([]byte("actor-1")))
	second.nextTagged(t, tagSubscribe)
	if resend := second.nextTagged(t, tagOps); len(resend) <= 5 {
		t.Fatal("the offline edit did not ride the reconnect")
	}
}

// A socket that authenticates and then dies has not proved it serves the room,
// so it must not wind the reconnect backoff back to its floor — otherwise a
// server failing over is redialed at the floor rate forever.
func TestProviderBackoffGrowsWhenTheSocketNeverServes(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, func(o *ProviderOptions) { o.MaxReconnectDelay = time.Second })

	first := server.conn(t, 0)
	first.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
	first.Close()

	// Two reconnects that authenticate and then die without ever answering the
	// resume. Each should step the backoff up: 250ms, then 500ms, then 1s.
	authOkThenDie := func(index int) {
		tr := server.conn(t, index)
		tr.next(t)
		tr.next(t)
		tr.next(t)
		tr.push(frameAuthOk([]byte("actor-1")))
		tr.nextTagged(t, tagSubscribe)
		tr.Close()
	}
	authOkThenDie(1)
	authOkThenDie(2)

	start := time.Now()
	server.conn(t, 3)
	if elapsed := time.Since(start); elapsed < 600*time.Millisecond {
		t.Fatalf("redialed after %v — the backoff reset on a socket that never served", elapsed)
	}
}

// The mirror of the test above: a socket that does answer the resume has served
// the room, so the backoff goes back to its floor.
func TestProviderBackoffResetsWhenTheSocketServes(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, func(o *ProviderOptions) { o.MaxReconnectDelay = time.Second })

	first := server.conn(t, 0)
	first.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
	first.Close()

	// Two reconnects that authenticate and answer the resume before dying. Each
	// earns the reset, so the next redial is back at the ~250ms floor rather than
	// stepped up to 500ms and 1s.
	serveThenDie := func(index int) {
		tr := server.conn(t, index)
		tr.next(t)
		tr.next(t)
		tr.next(t)
		tr.push(frameAuthOk([]byte("actor-1")))
		tr.nextTagged(t, tagSubscribe)
		tr.push(frameOps(p.Channel(), nil))
		waitFor(t, "the resume reply to be folded", func() bool {
			p.doc.mu.Lock()
			defer p.doc.mu.Unlock()
			return p.reconnectAttempt == 0
		})
		tr.Close()
	}
	serveThenDie(1)
	serveThenDie(2)

	start := time.Now()
	server.conn(t, 3)
	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Fatalf("redialed after %v — the backoff did not reset on a socket that served", elapsed)
	}
}

func TestProviderStateTransitions(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)

	var mu sync.Mutex
	var states []string
	p.OnState(func(s string) {
		mu.Lock()
		states = append(states, s)
		mu.Unlock()
	})

	first := server.conn(t, 0)
	first.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
	first.Close()
	second := server.conn(t, 1)
	second.handshake(t, 0)

	// The listener history is what this pins, so wait on it rather than on State,
	// which is published a moment ahead of the callback.
	want := []string{StateConnected, StateDisconnected, StateConnecting, StateConnected}
	waitFor(t, "the reconnect's transitions to be delivered", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(states) >= len(want)
	})

	mu.Lock()
	got := append([]string(nil), states...)
	mu.Unlock()
	// The listener subscribes after the provider is already "connecting", so the
	// observed run starts at the first sync.
	if len(got) != len(want) {
		t.Fatalf("states %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("states %v, want %v", got, want)
		}
	}
}

// --- awareness ---

func TestProviderAwarenessPublishesAndReceives(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}

	p.SetAwareness("cursor", []byte("10"))
	if frame := tr.nextTagged(t, tagAwarenessSet); binary.LittleEndian.Uint32(frame[1:]) != p.Channel() {
		t.Fatal("awareness framed the wrong channel")
	}

	// A peer's entry fans in and is readable by its publishing actor.
	tr.push(frameAwarenessUpdate(p.Channel(), []byte("peer-1"), []byte("cursor"), []byte("42")))
	waitFor(t, "the peer's awareness to arrive", func() bool { return p.AwarenessLen() > 0 })
	value, ok := p.Awareness([]byte("peer-1"), "cursor")
	if !ok || string(value) != "42" {
		t.Fatalf("peer awareness: %q ok=%v", value, ok)
	}
}

// --- server signals ---

func TestProviderUpdateRequiredSignal(t *testing.T) {
	server := &fakeServer{}
	codes := make(chan ErrorCode, 4)
	p := newFakeProvider(t, server, func(o *ProviderOptions) {
		o.OnError = func(code ErrorCode) { codes <- code }
	})
	tr := server.conn(t, 0)
	tr.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}

	tr.push(frameError(UpdateRequired))
	select {
	case code := <-codes:
		if code != UpdateRequired {
			t.Fatalf("OnError code %d, want UpdateRequired", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the UpdateRequired signal")
	}
	// A mid-session error is recoverable: the session stays up.
	if p.State() != StateConnected {
		t.Fatalf("state %q after a recoverable server error", p.State())
	}
}

func TestProviderOpsRejectedSignal(t *testing.T) {
	server := &fakeServer{}
	rejected := make(chan []Rejected, 4)
	p := newFakeProvider(t, server, func(o *ProviderOptions) {
		o.OnOpsRejected = func(r []Rejected) { rejected <- r }
	})
	tr := server.conn(t, 0)
	tr.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}

	// The refusal names an op by its per-client sequence, which the client
	// resolves against the outbox — so the edit has to be outstanding first.
	p.Doc().GetMap("root").Set("k", int64(1))
	tr.nextTagged(t, tagOps)
	tr.push(frameOpsRejected(p.Channel(), Forbidden, []uint64{1}))

	select {
	case batches := <-rejected:
		if len(batches) != 1 || batches[0].Reason != Forbidden {
			t.Fatalf("rejected batches: %#v", batches)
		}
		if len(batches[0].Ops) == 0 {
			t.Fatal("a rejected batch must still carry the refused ops")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the ops-rejected signal")
	}
}

func TestProviderRedirectSignal(t *testing.T) {
	server := &fakeServer{}
	redirects := make(chan []Redirect, 4)
	p := newFakeProvider(t, server, func(o *ProviderOptions) {
		o.OnRedirect = func(r []Redirect) { redirects <- r }
	})
	tr := server.conn(t, 0)
	tr.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}

	tr.push(frameRedirect([]byte("room"), []byte("10.0.0.2:6060")))
	select {
	case got := <-redirects:
		if len(got) != 1 || string(got[0].LeaderAddr) != "10.0.0.2:6060" {
			t.Fatalf("redirects: %#v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the redirect signal")
	}
}

// --- failure paths ---

func TestProviderHandshakeErrorFailsConnect(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.next(t)
	tr.next(t)
	tr.next(t)
	tr.push(frameError(AuthFailed))

	if err := p.WaitSynced(context.Background()); err == nil {
		t.Fatal("a refused handshake must fail the initial sync")
	}
	waitFor(t, "the refusal to disconnect the provider", func() bool { return p.State() == StateDisconnected })
	// A refusal is terminal: the provider must not redial into the same rejection.
	time.Sleep(50 * time.Millisecond)
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.conns) != 1 {
		t.Fatalf("the provider redialed after a refused handshake (%d sockets)", len(server.conns))
	}
	if p.client.h != nil {
		t.Fatal("a refused handshake left the wire session allocated")
	}
}

func TestProviderConnectTimesOutOnASilentServer(t *testing.T) {
	server := &fakeServer{}
	start := time.Now()
	_, err := Connect(context.Background(), "ws://fake/room", "room", ProviderOptions{
		Dial:           server.dial,
		ConnectTimeout: 60 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("connecting to a server that never answers must fail")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("the connect timeout did not bound the wait")
	}
}

func TestProviderConnectFailsWhenTheDialIsRefused(t *testing.T) {
	server := &fakeServer{dialErr: errors.New("connection refused")}
	_, err := Connect(context.Background(), "ws://fake/room", "room", ProviderOptions{
		Dial:             server.dial,
		DisableReconnect: true,
		ConnectTimeout:   2 * time.Second,
	})
	if err == nil {
		t.Fatal("a refused dial with reconnect off must fail the connect")
	}
}

func TestProviderCloseBeforeSync(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	server.conn(t, 0)
	p.Close()

	if err := p.WaitSynced(context.Background()); err == nil {
		t.Fatal("closing before the sync must fail the wait")
	}
	if p.State() != StateDisconnected {
		t.Fatalf("state %q after Close", p.State())
	}
	p.Close() // idempotent
}

func TestProviderCloseStopsReconnecting(t *testing.T) {
	server := &fakeServer{}
	p := newFakeProvider(t, server, nil)
	tr := server.conn(t, 0)
	tr.handshake(t, 0)
	if err := p.WaitSynced(context.Background()); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}

	p.Close()
	time.Sleep(50 * time.Millisecond)
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.conns) != 1 {
		t.Fatalf("a closed provider redialed (%d sockets)", len(server.conns))
	}
}

func TestProviderConnectHonoursACancelledContext(t *testing.T) {
	server := &fakeServer{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Connect(ctx, "ws://fake/room", "room", ProviderOptions{Dial: server.dial}); err == nil {
		t.Fatal("a cancelled context must fail the connect")
	}
}

// --- drift guard ---

// The 8-byte connection header is hand-written here because the C ABI carries no
// entry point for it; this pins the version to the core's so a bump cannot
// silently leave the Go SDK speaking the old protocol.
func TestProtocolVersionMatchesTheCore(t *testing.T) {
	source, err := os.ReadFile("../../../crates/core/src/protocol.rs")
	if err != nil {
		t.Skipf("core protocol source unavailable: %v", err)
	}
	m := regexp.MustCompile(`pub const PROTOCOL_VERSION: u32 = (\d+);`).FindSubmatch(source)
	if m == nil {
		t.Fatal("could not find PROTOCOL_VERSION in the core protocol source")
	}
	want, err := strconv.Atoi(string(m[1]))
	if err != nil {
		t.Fatalf("PROTOCOL_VERSION: %v", err)
	}
	if want != protocolVersion {
		t.Fatalf("the SDK speaks protocol version %d, the core %d", protocolVersion, want)
	}
}
