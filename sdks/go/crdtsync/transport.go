package crdtsync

// The message-oriented socket a networked Provider drives. crdtsync's wire
// protocol is a stream of self-delimiting binary messages, so the transport owes
// the provider exactly three things: send one message, receive the next one, and
// close. A WebSocket satisfies that natively (binary frames are messages), and
// the built-in DialWebSocket speaks enough of RFC 6455 to talk to a crdtsync
// server without pulling a dependency into the SDK. An application that already
// carries a WebSocket library — or a test that wants a socket it can break on
// demand — supplies its own through ProviderOptions.Dial.

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Transport is one live connection carrying wire messages. Send and Close may be
// called from any goroutine; Receive is driven by the provider's read loop
// alone. Receive blocks until a message arrives and reports an error once the
// connection is finished — the signal the provider reconnects on. Close must
// release a blocked Receive: it is how the provider abandons a socket, and an
// implementation that does not strands the read loop. Send must not block
// indefinitely either: it is serialized with every other write, so a Send that
// never returns holds the wire shut for the whole provider — every edit, every
// awareness update. The built-in transport bounds each write with a deadline.
type Transport interface {
	Send(message []byte) error
	Receive() ([]byte, error)
	Close() error
}

// Dialer opens a Transport to url. header carries the credential and any
// application headers the server reads at the upgrade.
type Dialer func(ctx context.Context, url string, header http.Header) (Transport, error)

// The RFC 6455 pieces this client needs: the handshake's fixed GUID, the opcodes
// it sends and answers, and the header bits framing them.
const (
	wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA

	finBit  = 0x80
	maskBit = 0x80
	rsvBits = 0x70

	// maxControlPayload is RFC 6455's ceiling on a control frame's payload.
	maxControlPayload = 125
)

// maxMessageSize caps a single inbound message. A catch-up snapshot of a large
// room is the biggest thing a server sends, so the ceiling is generous — it
// exists to bound what a hostile peer can make the client allocate, not to
// constrain a real document.
const maxMessageSize = 128 << 20

// handshakeHeaders are written by the upgrade itself, so an application may not
// supply them.
var handshakeHeaders = map[string]bool{
	"Host":                  true,
	"Upgrade":               true,
	"Connection":            true,
	"Sec-Websocket-Key":     true,
	"Sec-Websocket-Version": true,
}

// wsConn is a client-side WebSocket connection over one net.Conn.
type wsConn struct {
	conn    net.Conn
	reader  *bufio.Reader
	writeMu sync.Mutex

	closeOnce sync.Once
	closeErr  error

	writeTimeout time.Duration
}

// DialWebSocket opens a WebSocket to url ("ws://host/path" or "wss://…") and
// returns it as a Transport. It is ProviderOptions.Dial's default.
func DialWebSocket(ctx context.Context, rawURL string, header http.Header) (Transport, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("crdtsync: bad url %q: %w", rawURL, err)
	}
	var secure bool
	switch u.Scheme {
	case "ws", "http":
		secure = false
	case "wss", "https":
		secure = true
	default:
		return nil, fmt.Errorf("crdtsync: unsupported url scheme %q", u.Scheme)
	}

	host := u.Host
	if u.Port() == "" {
		if secure {
			host = net.JoinHostPort(host, "443")
		} else {
			host = net.JoinHostPort(host, "80")
		}
	}

	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, err
	}
	if secure {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: u.Hostname()})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, err
		}
		conn = tlsConn
	}

	// The handshake is bounded by the dial context so a server that accepts the
	// TCP connection and then goes silent cannot wedge the caller.
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	ws, err := clientHandshake(conn, u, header)
	if err != nil {
		conn.Close()
		return nil, err
	}
	// Reads block indefinitely afterwards — an idle collaborative session is
	// normal, and a broken link surfaces as a read error, not a timeout.
	_ = conn.SetDeadline(time.Time{})
	return ws, nil
}

// clientHandshake performs the RFC 6455 opening handshake and verifies the
// server's accept token.
func clientHandshake(conn net.Conn, u *url.URL, header http.Header) (*wsConn, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(nonce)

	target := u.RequestURI()
	if target == "" {
		target = "/"
	}
	var req strings.Builder
	fmt.Fprintf(&req, "GET %s HTTP/1.1\r\n", target)
	fmt.Fprintf(&req, "Host: %s\r\n", u.Host)
	req.WriteString("Upgrade: websocket\r\n")
	req.WriteString("Connection: Upgrade\r\n")
	fmt.Fprintf(&req, "Sec-WebSocket-Key: %s\r\n", key)
	req.WriteString("Sec-WebSocket-Version: 13\r\n")
	for name, values := range header {
		// The upgrade's own headers are the handshake's to write; a second copy
		// would leave the server choosing between two.
		if handshakeHeaders[http.CanonicalHeaderKey(name)] {
			return nil, fmt.Errorf("crdtsync: %s is set by the websocket handshake", name)
		}
		for _, v := range values {
			// A newline in an application-supplied value would split the request into
			// two; refuse rather than smuggle one.
			if strings.ContainsAny(name, "\r\n:") || strings.ContainsAny(v, "\r\n") {
				return nil, fmt.Errorf("crdtsync: header %q is not a single header line", name)
			}
			fmt.Fprintf(&req, "%s: %s\r\n", name, v)
		}
	}
	req.WriteString("\r\n")
	if _, err := io.WriteString(conn, req.String()); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		return nil, fmt.Errorf("crdtsync: websocket handshake: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		return nil, fmt.Errorf("crdtsync: websocket handshake: server answered %s", resp.Status)
	}
	if !strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") {
		return nil, errors.New("crdtsync: websocket handshake: server did not upgrade")
	}
	sum := sha1.Sum([]byte(key + wsGUID))
	if resp.Header.Get("Sec-WebSocket-Accept") != base64.StdEncoding.EncodeToString(sum[:]) {
		return nil, errors.New("crdtsync: websocket handshake: bad accept token")
	}
	return &wsConn{conn: conn, reader: reader, writeTimeout: 30 * time.Second}, nil
}

// Send writes message as one masked binary frame. Client frames are always
// masked, as RFC 6455 requires.
func (w *wsConn) Send(message []byte) error {
	return w.writeFrame(opBinary, message)
}

func (w *wsConn) writeFrame(opcode byte, payload []byte) error {
	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}

	head := make([]byte, 0, 14)
	head = append(head, finBit|opcode)
	n := len(payload)
	switch {
	case n < 126:
		head = append(head, maskBit|byte(n))
	case n <= 0xFFFF:
		head = append(head, maskBit|126)
		head = binary.BigEndian.AppendUint16(head, uint16(n))
	default:
		head = append(head, maskBit|127)
		head = binary.BigEndian.AppendUint64(head, uint64(n))
	}
	head = append(head, mask[:]...)

	// Mask into a copy: the caller's buffer (an op frame the core still owns)
	// must come back unchanged.
	masked := make([]byte, n)
	for i := 0; i < n; i++ {
		masked[i] = payload[i] ^ mask[i%4]
	}

	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if w.writeTimeout > 0 {
		_ = w.conn.SetWriteDeadline(time.Now().Add(w.writeTimeout))
	}
	if _, err := w.conn.Write(head); err != nil {
		return err
	}
	_, err := w.conn.Write(masked)
	return err
}

// Receive reads the next data message, transparently answering pings and
// reassembling a fragmented message. A close frame ends the stream with io.EOF.
// maxFragments bounds how many frames one message may be split across, so a peer
// cannot hold the read loop open forever with a stream of empty continuations.
const maxFragments = 1 << 16

func (w *wsConn) Receive() ([]byte, error) {
	var message []byte
	var assembling bool
	fragments := 0
	for {
		fin, opcode, payload, err := w.readFrame()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case opPing:
			if err := w.writeFrame(opPong, payload); err != nil {
				return nil, err
			}
		case opPong:
			// Nothing to do: the provider tracks liveness by the frames it expects.
		case opClose:
			// Echo the close so the peer can shut down cleanly, then finish the stream.
			_ = w.writeFrame(opClose, nil)
			w.Close()
			return nil, io.EOF
		case opBinary, opText:
			if assembling {
				return nil, errors.New("crdtsync: websocket: data frame inside a fragmented message")
			}
			if fin {
				return payload, nil
			}
			message = payload
			assembling = true
		case opContinuation:
			if !assembling {
				return nil, errors.New("crdtsync: websocket: continuation without a start frame")
			}
			fragments++
			if fragments > maxFragments {
				return nil, errors.New("crdtsync: websocket: message exceeds the fragment ceiling")
			}
			if len(message)+len(payload) > maxMessageSize {
				return nil, errors.New("crdtsync: websocket: message exceeds the size ceiling")
			}
			message = append(message, payload...)
			if fin {
				return message, nil
			}
		default:
			return nil, fmt.Errorf("crdtsync: websocket: unknown opcode %d", opcode)
		}
	}
}

func (w *wsConn) readFrame() (fin bool, opcode byte, payload []byte, err error) {
	var head [2]byte
	if _, err := io.ReadFull(w.reader, head[:]); err != nil {
		return false, 0, nil, err
	}
	// No extension is ever negotiated, so a reserved bit means the peer is
	// speaking something this client cannot read.
	if head[0]&rsvBits != 0 {
		return false, 0, nil, errors.New("crdtsync: websocket: reserved bits set")
	}
	fin = head[0]&finBit != 0
	opcode = head[0] & 0x0F
	// A server never masks, so a masked frame did not come from one.
	if head[1]&maskBit != 0 {
		return false, 0, nil, errors.New("crdtsync: websocket: server frame is masked")
	}
	length := uint64(head[1] & 0x7F)
	// A control frame carries its whole payload in one frame, capped at 125 bytes,
	// so it can always be answered without buffering.
	if opcode >= opClose && (!fin || length > maxControlPayload) {
		return false, 0, nil, fmt.Errorf("crdtsync: websocket: malformed control frame (opcode %d)", opcode)
	}
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(w.reader, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(w.reader, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	if length > maxMessageSize {
		return false, 0, nil, errors.New("crdtsync: websocket: frame exceeds the size ceiling")
	}
	payload = make([]byte, length)
	if _, err := io.ReadFull(w.reader, payload); err != nil {
		return false, 0, nil, err
	}
	return fin, opcode, payload, nil
}

// Close shuts the connection down. Safe to call more than once and from any
// goroutine; a blocked Receive returns an error once it lands.
func (w *wsConn) Close() error {
	w.closeOnce.Do(func() { w.closeErr = w.conn.Close() })
	return w.closeErr
}
