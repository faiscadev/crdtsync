"""A minimal RFC 6455 WebSocket client, over the standard library only.

The SDK ships no third-party dependency, so the networked provider carries the
slice of the protocol a crdtsync connection uses: a client-side upgrade
handshake, binary data frames (masked outbound, as a client must), and the
control frames a peer may interleave — ping answered with a pong, close
answered and then reported as end-of-stream. Text frames never appear on the
wire protocol and are refused.

The transport is a plain blocking socket: :meth:`WebSocket.recv` blocks until a
message arrives, and :meth:`WebSocket.close` shuts the socket down so a blocked
reader returns. Sends are serialized, so the reader thread's pong and an
application thread's data frame cannot interleave on the wire.
"""

from __future__ import annotations

import base64
import hashlib
import os
import socket
import ssl
import struct
import threading
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlsplit

#: The fixed GUID RFC 6455 concatenates with the client key to derive the accept.
_ACCEPT_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

_OP_CONTINUATION = 0x0
_OP_TEXT = 0x1
_OP_BINARY = 0x2
_OP_CLOSE = 0x8
_OP_PING = 0x9
_OP_PONG = 0xA

#: The largest message accepted, whether as one frame or reassembled from a run
#: of fragments, so a hostile or corrupt length header cannot make the client
#: allocate unbounded memory. Comfortably above a room's catch-up snapshot.
MAX_MESSAGE_BYTES = 256 * 1024 * 1024

#: How long the connect + upgrade handshake may take before it fails.
DEFAULT_HANDSHAKE_TIMEOUT = 15.0

#: How long a session may sit idle before the kernel starts probing the peer. A
#: silently half-open socket — a NAT idle-drop, a peer that died without a reset
#: — is otherwise indistinguishable from a quiet room, and the connection would
#: wedge as `connected` forever. Probing is the kernel's job rather than a read
#: deadline's, so reads stay plainly blocking and a local close still wakes them.
DEFAULT_KEEPALIVE_IDLE = 30

#: The gap between unanswered probes, and how many may go unanswered before the
#: kernel declares the connection dead.
KEEPALIVE_INTERVAL = 10
KEEPALIVE_PROBES = 3

#: How far the read cursor may run ahead before the consumed prefix is dropped.
_COMPACT_AFTER = 1 << 20


class WebSocketError(RuntimeError):
    """The connection could not be established, or the peer broke the protocol."""


def connect(
    url: str,
    *,
    timeout: float = DEFAULT_HANDSHAKE_TIMEOUT,
    keepalive_idle: int = DEFAULT_KEEPALIVE_IDLE,
    headers: Optional[Dict[str, str]] = None,
) -> "WebSocket":
    """Open a WebSocket to ``url`` (``ws://`` or ``wss://``) and return it once
    the upgrade handshake has completed."""
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme not in ("ws", "wss"):
        raise WebSocketError(f"crdtsync: unsupported websocket scheme {parts.scheme!r}")
    if not parts.hostname:
        raise WebSocketError(f"crdtsync: no host in websocket url {url!r}")
    port = parts.port or (443 if scheme == "wss" else 80)
    resource = parts.path or "/"
    if parts.query:
        resource = f"{resource}?{parts.query}"

    # The Host header names the origin only: any userinfo in the URL is a
    # credential, and echoing it into a header would both break the request and
    # leak it.
    host = parts.hostname if parts.port is None else f"{parts.hostname}:{parts.port}"

    sock = socket.create_connection((parts.hostname, port), timeout=timeout)
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        _enable_keepalive(sock, keepalive_idle)
        if scheme == "wss":
            context = ssl.create_default_context()
            sock = context.wrap_socket(sock, server_hostname=parts.hostname)
        key = base64.b64encode(os.urandom(16))
        pending = _handshake(sock, host, resource, key, headers or {})
    except Exception:
        sock.close()
        raise
    # The handshake is bounded; the session that follows blocks until a frame
    # arrives, the peer's keepalive probes fail, or the socket is shut down.
    sock.settimeout(None)
    return WebSocket(sock, pending)


def _enable_keepalive(sock: socket.socket, idle: int) -> None:
    """Ask the kernel to probe an idle connection, tuning the timers where the
    platform exposes them (the option names differ between BSD and Linux)."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    for name, value in (
        ("TCP_KEEPIDLE", idle),
        ("TCP_KEEPALIVE", idle),
        ("TCP_KEEPINTVL", KEEPALIVE_INTERVAL),
        ("TCP_KEEPCNT", KEEPALIVE_PROBES),
    ):
        option = getattr(socket, name, None)
        if option is None:
            continue
        try:
            sock.setsockopt(socket.IPPROTO_TCP, option, value)
        except OSError:
            pass  # the platform names the option but will not set it


def _handshake(
    sock: socket.socket,
    host: str,
    resource: str,
    key: bytes,
    headers: Dict[str, str],
) -> bytes:
    """Run the client upgrade, returning any bytes read past the response head —
    a peer may pipeline its first frames into the same write as the 101, and
    dropping them would silently lose messages."""
    lines = [
        f"GET {resource} HTTP/1.1",
        f"Host: {host}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key.decode('ascii')}",
        "Sec-WebSocket-Version: 13",
    ]
    lines += [f"{name}: {value}" for name, value in headers.items()]
    sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("ascii"))

    head, pending = _read_response_head(sock)
    status, _, rest = head.partition(b"\r\n")
    fields = status.split(None, 2)
    if len(fields) < 2 or fields[1] != b"101":
        raise WebSocketError(
            f"crdtsync: websocket upgrade refused ({status.decode('latin-1')})"
        )
    accept = _header(rest, b"sec-websocket-accept")
    expected = base64.b64encode(hashlib.sha1(key + _ACCEPT_GUID).digest())
    if accept != expected:
        raise WebSocketError("crdtsync: websocket upgrade returned a bad accept key")
    return pending


def _read_response_head(sock: socket.socket) -> Tuple[bytes, bytes]:
    """Read the upgrade response head, returning it and whatever followed the
    blank line in the same read."""
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise WebSocketError("crdtsync: connection closed during the upgrade")
        buf += chunk
        if len(buf) > 64 * 1024:
            raise WebSocketError("crdtsync: oversized upgrade response")
    head, _, rest = buf.partition(b"\r\n\r\n")
    return head, rest


def _header(block: bytes, name: bytes) -> Optional[bytes]:
    for line in block.split(b"\r\n"):
        field, sep, value = line.partition(b":")
        if sep and field.strip().lower() == name:
            return value.strip()
    return None


class WebSocket:
    """One open connection. Binary messages in, binary messages out."""

    def __init__(self, sock: socket.socket, pending: bytes = b""):
        self._sock: Optional[socket.socket] = sock
        self._send_lock = threading.Lock()
        self._closed = False
        # A read buffer plus a cursor, so draining a frame off the front does not
        # recopy what is behind it — a large catch-up snapshot arrives in many
        # reads, and re-slicing per read would make reassembly quadratic.
        self._buffer = bytearray(pending)
        self._offset = 0

    def send(self, data: bytes) -> None:
        """Send one binary message."""
        self._write_frame(_OP_BINARY, bytes(data))

    def recv(self) -> Optional[bytes]:
        """Block for the next binary message, or return ``None`` once the peer
        has closed the connection (or it was closed locally)."""
        fragments: List[bytes] = []
        total = 0
        started = False
        while True:
            frame = self._read_frame()
            if frame is None:
                return None
            fin, opcode, payload = frame
            if opcode == _OP_CLOSE:
                self._write_frame(_OP_CLOSE, b"", ignore_errors=True)
                self._shutdown()
                return None
            if opcode == _OP_PING:
                self._write_frame(_OP_PONG, payload, ignore_errors=True)
                continue
            if opcode == _OP_PONG:
                continue
            if opcode == _OP_CONTINUATION:
                if not started:
                    raise WebSocketError("crdtsync: continuation frame with no message")
            elif opcode == _OP_BINARY:
                if started:
                    raise WebSocketError("crdtsync: interleaved data frames")
                started = True
            elif opcode == _OP_TEXT:
                raise WebSocketError("crdtsync: unexpected non-binary websocket frame")
            else:
                raise WebSocketError(f"crdtsync: unknown websocket opcode {opcode}")
            total += len(payload)
            if total > MAX_MESSAGE_BYTES:
                raise WebSocketError("crdtsync: websocket message exceeds the size ceiling")
            fragments.append(payload)
            if fin:
                return b"".join(fragments)

    def close(self) -> None:
        """Send a close frame (best effort) and shut the socket down, so a
        blocked :meth:`recv` returns. Idempotent."""
        if self._sock is None:
            return
        if not self._closed:
            self._write_frame(_OP_CLOSE, b"", ignore_errors=True)
        self._shutdown()

    @property
    def closed(self) -> bool:
        return self._closed

    def _shutdown(self) -> None:
        self._closed = True
        sock, self._sock = self._sock, None
        if sock is None:
            return
        # Shut the socket down first: that unblocks a writer parked in `sendall`,
        # so taking the send lock before releasing the descriptor cannot hang —
        # and releasing it under a live write could hand the descriptor number to
        # another thread's fresh socket.
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        with self._send_lock:
            try:
                sock.close()
            except OSError:
                pass

    def _write_frame(self, opcode: int, payload: bytes, *, ignore_errors: bool = False) -> None:
        header = bytearray([0x80 | opcode])
        length = len(payload)
        if length < 126:
            header.append(0x80 | length)
        elif length < 1 << 16:
            header.append(0x80 | 126)
            header += struct.pack("!H", length)
        else:
            header.append(0x80 | 127)
            header += struct.pack("!Q", length)
        mask = os.urandom(4)
        header += mask
        masked = _mask(payload, mask)
        failure = None
        with self._send_lock:
            sock = self._sock
            if sock is None:
                if ignore_errors:
                    return
                raise WebSocketError("crdtsync: send on a closed websocket")
            try:
                sock.sendall(bytes(header) + masked)
            except OSError as err:
                failure = err
        if failure is None:
            return
        # A write that failed part-way leaves a truncated frame on the wire and
        # the peer's parser out of step, so the connection is finished — tearing
        # it down here is what turns it into a reconnect instead of a garbled
        # stream. Shutting down outside the send lock keeps it deadlock-free.
        self._shutdown()
        if not ignore_errors:
            raise WebSocketError(f"crdtsync: websocket send failed: {failure}") from failure

    def _read_frame(self):
        head = self._read_exact(2)
        if head is None:
            return None
        fin = bool(head[0] & 0x80)
        if head[0] & 0x70:
            raise WebSocketError("crdtsync: websocket frame set a reserved bit")
        opcode = head[0] & 0x0F
        masked = bool(head[1] & 0x80)
        length = head[1] & 0x7F
        if opcode & 0x8 and (length > 125 or not fin):
            raise WebSocketError("crdtsync: oversized or fragmented control frame")
        if length == 126:
            extended = self._read_exact(2)
            if extended is None:
                return None
            length = struct.unpack("!H", extended)[0]
        elif length == 127:
            extended = self._read_exact(8)
            if extended is None:
                return None
            length = struct.unpack("!Q", extended)[0]
        if length > MAX_MESSAGE_BYTES:
            raise WebSocketError(f"crdtsync: websocket frame of {length} bytes is too large")
        mask = b""
        if masked:
            got = self._read_exact(4)
            if got is None:
                return None
            mask = got
        payload = self._read_exact(length) if length else b""
        if payload is None:
            return None
        if mask:
            payload = _mask(payload, mask)
        return fin, opcode, payload

    def _read_exact(self, count: int) -> Optional[bytes]:
        while len(self._buffer) - self._offset < count:
            sock = self._sock
            if sock is None:
                return None
            try:
                chunk = sock.recv(65536)
            except OSError:
                self._shutdown()
                return None
            if not chunk:
                self._shutdown()
                return None
            self._buffer += chunk
        end = self._offset + count
        out = bytes(self._buffer[self._offset : end])
        self._offset = end
        if self._offset >= len(self._buffer):
            self._buffer.clear()
            self._offset = 0
        elif self._offset > _COMPACT_AFTER:
            del self._buffer[: self._offset]
            self._offset = 0
        return out


def _mask(payload: bytes, mask: bytes) -> bytes:
    """XOR ``payload`` with the repeating 4-byte ``mask``, whole-word at a time
    so a large frame does not pay a Python-level loop per byte."""
    if not payload:
        return b""
    repeated = (mask * (len(payload) // 4 + 1))[: len(payload)]
    xored = int.from_bytes(payload, "big") ^ int.from_bytes(repeated, "big")
    return xored.to_bytes(len(payload), "big")
