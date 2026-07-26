package crdtsync

// The networked provider against the real crdtsync server, spawned in relay mode
// (no admin plane, no data dir) so two providers sync a room over a real
// WebSocket — the built-in transport included. Skipped when the server binary is
// absent; build it with `cargo build -p crdtsync-server`.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func serverBinary() string {
	if bin := os.Getenv("CRDTSYNC_SERVER_BIN"); bin != "" {
		return bin
	}
	return filepath.Join("..", "..", "..", "target", "debug", "crdtsync-server")
}

// One server serves the whole package: rooms are named per test, so they do not
// collide, and a single process keeps the suite cheap.
var (
	sharedServerOnce sync.Once
	sharedServerURL  string
	sharedServerErr  error
	sharedServerCmd  *exec.Cmd
)

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedServerCmd != nil {
		_ = sharedServerCmd.Process.Kill()
		_ = sharedServerCmd.Wait()
	}
	os.Exit(code)
}

// testServer returns the URL of the package's server, starting it on first use
// and skipping the calling test when the binary has not been built.
func testServer(t *testing.T) string {
	t.Helper()
	sharedServerOnce.Do(func() { sharedServerURL, sharedServerErr = startServer() })
	if sharedServerErr != nil {
		t.Skipf("crdtsync server unavailable: %v", sharedServerErr)
	}
	return sharedServerURL
}

// startServer boots the server on a free port and returns its ws:// URL.
func startServer() (string, error) {
	bin := serverBinary()
	if _, err := os.Stat(bin); err != nil {
		return "", fmt.Errorf("binary not built (%s); run `cargo build -p crdtsync-server`", bin)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := listener.Addr().String()
	listener.Close()

	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "CRDTSYNC_ADDR="+addr)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return "", err
	}
	if err := cmd.Start(); err != nil {
		return "", err
	}
	sharedServerCmd = cmd

	ready := make(chan struct{})
	go func() {
		// Keep draining after the readiness line: the server logs for the life of
		// the suite, and a full pipe would block its writes.
		var once sync.Once
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), "serving on") {
				once.Do(func() { close(ready) })
			}
		}
	}()
	select {
	case <-ready:
		return "ws://" + addr, nil
	case <-time.After(20 * time.Second):
		return "", errors.New("the server did not report itself ready")
	}
}

// joinRoom connects a provider to room and closes it when the test ends.
func joinRoom(t *testing.T, url, room string) *NetProvider {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	p, err := Connect(ctx, url, room, ProviderOptions{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestIntegrationTwoProvidersSync(t *testing.T) {
	url := testServer(t)
	room := fmt.Sprintf("room-%d-sync", time.Now().UnixNano())
	a := joinRoom(t, url, room)
	b := joinRoom(t, url, room)
	if a.State() != StateConnected {
		t.Fatalf("state %q after Connect", a.State())
	}

	a.Doc().GetMap("root").Set("title", "Hello")
	a.Doc().GetMap("root").Set("n", int64(7))
	a.Doc().GetList("items").Append("x")
	a.Doc().GetList("items").Append("y")
	a.Doc().GetText("body").Insert(0, "hi")

	waitFor(t, "the map to converge", func() bool {
		v, _ := b.Doc().GetMap("root").Get("title")
		return v == "Hello"
	})
	waitFor(t, "the list to converge", func() bool { return b.Doc().GetList("items").Len() == 2 })
	waitFor(t, "the text to converge", func() bool { return b.Doc().GetText("body").String() == "hi" })

	if v, _ := b.Doc().GetMap("root").Get("n"); v != int64(7) {
		t.Fatalf("peer n: %#v", v)
	}
	if got := b.Doc().GetList("items").Values(); len(got) != 2 || got[0] != "x" || got[1] != "y" {
		t.Fatalf("peer items: %#v", got)
	}
}

func TestIntegrationRemoteReactivityFires(t *testing.T) {
	url := testServer(t)
	room := fmt.Sprintf("room-%d-reactivity", time.Now().UnixNano())
	a := joinRoom(t, url, room)
	b := joinRoom(t, url, room)

	var mu sync.Mutex
	var values []string
	b.Doc().OnUpdate(func(e UpdateEvent) {
		if e.Origin != "remote" {
			return
		}
		mu.Lock()
		for _, c := range e.Changes {
			if c.Kind == "update" {
				values = append(values, fmt.Sprint(c.New))
			}
		}
		mu.Unlock()
	})

	a.Doc().GetMap("root").Set("k", "first")
	a.Doc().GetMap("root").Set("k", "second")

	waitFor(t, "the peer's reactivity to report the second value", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, v := range values {
			if v == "second" {
				return true
			}
		}
		return false
	})
	if v, _ := b.Doc().GetMap("root").Get("k"); v != "second" {
		t.Fatalf("peer k: %#v", v)
	}
}

func TestIntegrationLateJoinerCatchesUp(t *testing.T) {
	url := testServer(t)
	room := fmt.Sprintf("room-%d-catchup", time.Now().UnixNano())
	a := joinRoom(t, url, room)
	a.Doc().GetMap("root").Set("early", "value")
	// Let the edit reach the server before the joiner subscribes, so the value
	// arrives in its catch-up rather than as a live update.
	waitFor(t, "the edit to be acknowledged", func() bool { return a.OutboxLen() == 0 })

	b := joinRoom(t, url, room)
	if v, _ := b.Doc().GetMap("root").Get("early"); v != "value" {
		t.Fatalf("the joiner's catch-up did not carry the room's state: %#v", v)
	}
}

// A dropped socket must resume rather than replay: the provider reconnects,
// resends what the server never acknowledged, and the room ends up with each
// edit applied exactly once.
func TestIntegrationReconnectResendsWithoutDuplicating(t *testing.T) {
	url := testServer(t)
	room := fmt.Sprintf("room-%d-reconnect", time.Now().UnixNano())

	// A dialer that hands the test the sockets it opens, so one can be severed.
	var mu sync.Mutex
	var sockets []Transport
	dial := func(ctx context.Context, u string, header http.Header) (Transport, error) {
		tr, err := DialWebSocket(ctx, u, header)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		sockets = append(sockets, tr)
		mu.Unlock()
		return tr, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	a, err := Connect(ctx, url, room, ProviderOptions{Dial: dial, MaxReconnectDelay: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer a.Close()
	b := joinRoom(t, url, room)

	a.Doc().GetText("body").Insert(0, "abc")
	waitFor(t, "the first edit to converge", func() bool { return b.Doc().GetText("body").String() == "abc" })

	mu.Lock()
	first := sockets[0]
	mu.Unlock()
	first.Close()
	waitFor(t, "the drop to register", func() bool { return a.State() != StateConnected })

	// Authored while offline: the outbox holds it until the link returns.
	a.Doc().GetText("body").Insert(3, "def")
	waitFor(t, "the reconnect to sync", func() bool { return a.State() == StateConnected })
	waitFor(t, "the offline edit to converge", func() bool { return b.Doc().GetText("body").String() == "abcdef" })

	// The resend replays ops the server already holds; they must not apply twice.
	// A fresh edit that converges after them proves the replays were processed.
	waitFor(t, "the outbox to drain", func() bool { return a.OutboxLen() == 0 })
	b.Doc().GetText("marker").Insert(0, "!")
	waitFor(t, "the frame behind the replays to apply", func() bool {
		return a.Doc().GetText("marker").String() == "!"
	})
	if got := a.Doc().GetText("body").String(); got != "abcdef" {
		t.Fatalf("author's text after the resend: %q", got)
	}
	if got := b.Doc().GetText("body").String(); got != "abcdef" {
		t.Fatalf("peer's text after the resend: %q", got)
	}
}

// A credential presented at the upgrade authenticates the connection at accept,
// so the provider must not also offer one in band — the server refuses a second
// authentication as a protocol violation.
func TestIntegrationUpgradeFastPathSkipsInBandAuth(t *testing.T) {
	url := testServer(t)
	room := fmt.Sprintf("room-%d-upgrade", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	p, err := Connect(ctx, url, room, ProviderOptions{
		Header: http.Header{"Authorization": []string{"upgrade-token"}},
	})
	if err != nil {
		t.Fatalf("Connect over the upgrade fast path: %v", err)
	}
	defer p.Close()

	p.Doc().GetMap("root").Set("k", "v")
	waitFor(t, "the edit to be acknowledged", func() bool { return p.OutboxLen() == 0 })
}

func TestIntegrationAwarenessFansOut(t *testing.T) {
	url := testServer(t)
	room := fmt.Sprintf("room-%d-awareness", time.Now().UnixNano())
	a := joinRoom(t, url, room)
	b := joinRoom(t, url, room)

	a.SetAwareness("cursor", []byte("10"))
	waitFor(t, "the peer's awareness to arrive", func() bool { return b.AwarenessLen() > 0 })

	actor, ok := a.Actor()
	if !ok {
		t.Fatal("the session reported no actor after the handshake")
	}
	value, ok := b.Awareness(actor, "cursor")
	if !ok || string(value) != "10" {
		t.Fatalf("peer awareness: %q ok=%v", value, ok)
	}
}

// Concurrent editors on one provider share the doc's lock with the socket's read
// loop, so the replica is never touched from two goroutines at once.
func TestIntegrationConcurrentEditorsConverge(t *testing.T) {
	url := testServer(t)
	room := fmt.Sprintf("room-%d-concurrent", time.Now().UnixNano())
	a := joinRoom(t, url, room)
	b := joinRoom(t, url, room)

	const writers, each = 4, 10
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				_ = a.Doc().GetList("log").Append(fmt.Sprintf("w%d-%d", w, i))
				_, _ = a.Doc().GetMap("root").Get("title")
			}
		}(w)
	}
	wg.Wait()

	if got := a.Doc().GetList("log").Len(); got != writers*each {
		t.Fatalf("author's list holds %d items, want %d", got, writers*each)
	}
	waitFor(t, "the peer to converge", func() bool { return b.Doc().GetList("log").Len() == writers*each })
}

func TestIntegrationCloseIsFinal(t *testing.T) {
	url := testServer(t)
	room := fmt.Sprintf("room-%d-close", time.Now().UnixNano())
	p := joinRoom(t, url, room)
	p.Close()
	waitFor(t, "the close to register", func() bool { return p.State() == StateDisconnected })
	// An edit after Close cannot reach the wire, and reading is inert rather than
	// a crash on the freed session.
	p.Doc().GetMap("root").Set("after", "close")
	p.Close()
}
