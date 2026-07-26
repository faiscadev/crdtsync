"""The bundled RFC 6455 client, against a raw loopback server.

The provider's transport is hand-rolled so the SDK carries no dependency, which
makes the framing its own spec: the upgrade handshake, the three payload-length
encodings, fragmentation, interleaved control frames, masking in both
directions, and the malformed frames that must be refused rather than
misinterpreted."""

import base64
import hashlib
import socket
import struct
import threading
import time

import pytest
from crdtsync import _websocket
from crdtsync._websocket import WebSocketError

ACCEPT_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def server_frame(opcode: int, payload: bytes = b"", *, fin: bool = True, rsv: int = 0) -> bytes:
    """A frame as a server writes it — unmasked, per RFC 6455."""
    head = bytes([(0x80 if fin else 0) | rsv | opcode])
    length = len(payload)
    if length < 126:
        return head + bytes([length]) + payload
    if length < 1 << 16:
        return head + bytes([126]) + struct.pack("!H", length) + payload
    return head + bytes([127]) + struct.pack("!Q", length) + payload


def read_client_message(conn: socket.socket) -> bytes:
    """Read one masked client frame off a raw socket, returning its payload."""

    def exact(count: int) -> bytes:
        buf = b""
        while len(buf) < count:
            chunk = conn.recv(count - len(buf))
            if not chunk:
                raise AssertionError("the client closed mid-frame")
            buf += chunk
        return buf

    head = exact(2)
    length = head[1] & 0x7F
    if length == 126:
        length = struct.unpack("!H", exact(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", exact(8))[0]
    assert head[1] & 0x80, "a client frame must be masked"
    mask = exact(4)
    payload = exact(length) if length else b""
    return bytes(b ^ mask[i % 4] for i, b in enumerate(payload))


class LoopbackServer:
    """Accepts one connection, runs ``handler(conn, key)`` on it in a thread."""

    def __init__(
        self,
        handler,
        *,
        accept_key=True,
        status_line=b"HTTP/1.1 101 Switching Protocols",
        pipelined=b"",
    ):
        self._handler = handler
        self._accept_key = accept_key
        self._status_line = status_line
        self._pipelined = pipelined
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(1)
        self.url = "ws://127.0.0.1:%d" % self._listener.getsockname()[1]
        self.error = None
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        try:
            conn, _ = self._listener.accept()
        except OSError:
            return
        try:
            request = b""
            while b"\r\n\r\n" not in request:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                request += chunk
            key = b""
            for line in request.split(b"\r\n"):
                name, sep, value = line.partition(b":")
                if sep and name.strip().lower() == b"sec-websocket-key":
                    key = value.strip()
            accept = base64.b64encode(hashlib.sha1(key + ACCEPT_GUID).digest())
            if not self._accept_key:
                accept = base64.b64encode(b"x" * 20)
            conn.sendall(
                self._status_line
                + b"\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                + accept
                + b"\r\n\r\n"
                + self._pipelined
            )
            self._handler(conn)
        except Exception as err:  # surfaced by the test rather than lost in a thread
            self.error = err
        finally:
            conn.close()

    def close(self) -> None:
        self._listener.close()
        self._thread.join(timeout=2.0)


def echo_once(conn: socket.socket) -> None:
    """Bounce the client's first message back, then close."""
    payload = read_client_message(conn)
    conn.sendall(server_frame(0x2, payload))
    conn.sendall(server_frame(0x8))


@pytest.fixture
def serve():
    servers = []

    def _serve(handler, **options):
        server = LoopbackServer(handler, **options)
        servers.append(server)
        return server

    yield _serve
    for server in servers:
        server.close()


class TestHandshake:
    def test_refuses_a_bad_accept_key(self, serve):
        server = serve(lambda conn: None, accept_key=False)
        with pytest.raises(WebSocketError, match="accept key"):
            _websocket.connect(server.url)

    def test_refuses_a_non_upgrade_response(self, serve):
        server = serve(lambda conn: None, status_line=b"HTTP/1.1 404 Not Found")
        with pytest.raises(WebSocketError, match="refused"):
            _websocket.connect(server.url)

    def test_refuses_an_unsupported_scheme(self):
        with pytest.raises(WebSocketError, match="scheme"):
            _websocket.connect("http://127.0.0.1:1")

    def test_refuses_a_101_that_is_not_an_upgrade(self, serve):
        server = serve(
            lambda conn: None,
            status_line=b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c",
        )
        with pytest.raises(WebSocketError, match="did not upgrade"):
            _websocket.connect(server.url)

    def test_refuses_a_header_carrying_a_newline(self, serve):
        # A newline would splice a header — or a whole second request — in.
        server = serve(lambda conn: None)
        with pytest.raises(WebSocketError, match="newline"):
            _websocket.connect(server.url, headers={"X-Trace": "a\r\nX-Admin: 1"})

    def test_keeps_frames_pipelined_with_the_upgrade_response(self, serve):
        # A peer may write the 101 and its first frames in one go; the bytes read
        # past the response head belong to the session, not the handshake, and
        # dropping them loses whole messages with no error.
        server = serve(
            lambda conn: None,
            pipelined=server_frame(0x2, b"first") + server_frame(0x2, b"second"),
        )
        ws = _websocket.connect(server.url)
        assert ws.recv() == b"first"
        assert ws.recv() == b"second"
        ws.close()


class TestFraming:
    @pytest.mark.parametrize("size", [0, 5, 125, 126, 300, 70000])
    def test_round_trips_every_payload_length_encoding(self, serve, size):
        server = serve(echo_once)
        ws = _websocket.connect(server.url)
        payload = bytes((i * 7) % 256 for i in range(size))
        ws.send(payload)
        assert ws.recv() == payload
        ws.close()
        assert server.error is None

    def test_reassembles_a_fragmented_message(self, serve):
        def handler(conn):
            conn.sendall(server_frame(0x2, b"frag-", fin=False))
            conn.sendall(server_frame(0x0, b"mented", fin=True))

        ws = _websocket.connect(serve(handler).url)
        assert ws.recv() == b"frag-mented"
        ws.close()

    def test_answers_a_ping_interleaved_inside_a_fragment(self, serve):
        def handler(conn):
            conn.sendall(server_frame(0x2, b"a", fin=False))
            conn.sendall(server_frame(0x9, b"beat"))
            conn.sendall(server_frame(0x0, b"b", fin=True))
            assert read_client_message(conn) == b"beat"  # the pong

        server = serve(handler)
        ws = _websocket.connect(server.url)
        assert ws.recv() == b"ab"
        ws.close()
        assert server.error is None

    def test_a_peer_close_ends_the_stream(self, serve):
        ws = _websocket.connect(serve(lambda conn: conn.sendall(server_frame(0x8))).url)
        assert ws.recv() is None
        assert ws.closed

    def test_an_abrupt_disconnect_ends_the_stream(self, serve):
        ws = _websocket.connect(serve(lambda conn: conn.close()).url)
        assert ws.recv() is None
        assert ws.closed

    def test_refuses_a_reserved_bit(self, serve):
        ws = _websocket.connect(serve(lambda conn: conn.sendall(server_frame(0x2, b"x", rsv=0x40))).url)
        with pytest.raises(WebSocketError, match="reserved bit"):
            ws.recv()

    def test_refuses_a_fragmented_control_frame(self, serve):
        handler = lambda conn: conn.sendall(server_frame(0x9, b"x", fin=False))
        ws = _websocket.connect(serve(handler).url)
        with pytest.raises(WebSocketError, match="control frame"):
            ws.recv()

    def test_refuses_a_text_frame(self, serve):
        ws = _websocket.connect(serve(lambda conn: conn.sendall(server_frame(0x1, b"hi"))).url)
        with pytest.raises(WebSocketError, match="non-binary"):
            ws.recv()

    def test_refuses_a_masked_frame(self, serve):
        # RFC 6455 §5.1: a server never masks. One that does is not speaking the
        # server half of the protocol.
        def handler(conn):
            conn.sendall(b"\x82\x81\x00\x00\x00\x00" + b"x")

        ws = _websocket.connect(serve(handler).url)
        with pytest.raises(WebSocketError, match="masked"):
            ws.recv()

    def test_refuses_a_message_over_the_ceiling(self, serve):
        # A length header alone must not be able to make the client allocate.
        oversized = struct.pack("!Q", _websocket.MAX_MESSAGE_BYTES + 1)
        ws = _websocket.connect(serve(lambda conn: conn.sendall(b"\x82\x7f" + oversized)).url)
        with pytest.raises(WebSocketError, match="too large"):
            ws.recv()

    def test_a_framing_violation_releases_the_socket(self, serve):
        ws = _websocket.connect(serve(lambda conn: conn.sendall(server_frame(0x1, b"hi"))).url)
        with pytest.raises(WebSocketError):
            ws.recv()
        # The cursor is mid-stream, so nothing after it can be trusted.
        assert ws.closed
        assert ws._sock is None


class TestClose:
    def test_a_peer_disconnect_releases_the_descriptor(self, serve):
        # A reconnect loop that leaked the socket per drop would exhaust the
        # process's descriptors, so the peer's EOF has to release it outright.
        ws = _websocket.connect(serve(lambda conn: conn.close()).url)
        raw = ws._sock
        assert ws.recv() is None
        assert raw.fileno() == -1
        ws.close()
        assert ws._sock is None

    def test_close_is_idempotent(self, serve):
        ws = _websocket.connect(serve(lambda conn: None).url)
        ws.close()
        ws.close()
        assert ws.closed

    def test_a_control_reply_never_waits_for_a_parked_writer(self, serve):
        # Reads and writes run on different threads. A reader that queued behind
        # a writer parked in `sendall` would stop draining the very socket whose
        # reads let the peer drain — a full-duplex deadlock with nothing to end
        # it. The pong is skipped instead; the peer simply pings again.
        def handler(conn):
            conn.sendall(server_frame(0x9, b"beat") + server_frame(0x2, b"payload"))
            time.sleep(0.5)

        ws = _websocket.connect(serve(handler).url)
        try:
            ws._send_lock.acquire()  # stand in for a writer parked mid-send
            assert ws.recv() == b"payload"
        finally:
            ws._send_lock.release()
            ws.close()

    def test_sending_on_a_closed_socket_raises(self, serve):
        ws = _websocket.connect(serve(lambda conn: None).url)
        ws.close()
        with pytest.raises(WebSocketError, match="closed"):
            ws.send(b"x")
