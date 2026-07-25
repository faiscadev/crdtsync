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
from typing import Dict, List, Optional
from urllib.parse import urlsplit

#: The fixed GUID RFC 6455 concatenates with the client key to derive the accept.
_ACCEPT_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

_OP_CONTINUATION = 0x0
_OP_TEXT = 0x1
_OP_BINARY = 0x2
_OP_CLOSE = 0x8
_OP_PING = 0x9
_OP_PONG = 0xA

#: The largest single frame payload accepted, so a hostile or corrupt length
#: header cannot make the client allocate unbounded memory. Comfortably above a
#: room's catch-up snapshot.
MAX_FRAME_BYTES = 256 * 1024 * 1024

#: How long the connect + upgrade handshake may take before it fails.
DEFAULT_HANDSHAKE_TIMEOUT = 15.0


class WebSocketError(RuntimeError):
    """The connection could not be established, or the peer broke the protocol."""


def connect(
    url: str,
    *,
    timeout: float = DEFAULT_HANDSHAKE_TIMEOUT,
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

    sock = socket.create_connection((parts.hostname, port), timeout=timeout)
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if scheme == "wss":
            context = ssl.create_default_context()
            sock = context.wrap_socket(sock, server_hostname=parts.hostname)
        key = base64.b64encode(os.urandom(16))
        _handshake(sock, parts.netloc, resource, key, headers or {})
    except Exception:
        sock.close()
        raise
    # The handshake is bounded; the session that follows blocks until a frame
    # arrives or the socket is shut down.
    sock.settimeout(None)
    return WebSocket(sock)


def _handshake(
    sock: socket.socket,
    netloc: str,
    resource: str,
    key: bytes,
    headers: Dict[str, str],
) -> None:
    lines = [
        f"GET {resource} HTTP/1.1",
        f"Host: {netloc}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key.decode('ascii')}",
        "Sec-WebSocket-Version: 13",
    ]
    lines += [f"{name}: {value}" for name, value in headers.items()]
    sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("ascii"))

    raw = _read_until_headers_end(sock)
    head, _, _ = raw.partition(b"\r\n\r\n")
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


def _read_until_headers_end(sock: socket.socket) -> bytes:
    """Read the upgrade response head. The server sends no body before the
    first frame, so the bytes stop at the blank line that ends the headers."""
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise WebSocketError("crdtsync: connection closed during the upgrade")
        buf += chunk
        if len(buf) > 64 * 1024:
            raise WebSocketError("crdtsync: oversized upgrade response")
    return buf


def _header(block: bytes, name: bytes) -> Optional[bytes]:
    for line in block.split(b"\r\n"):
        field, sep, value = line.partition(b":")
        if sep and field.strip().lower() == name:
            return value.strip()
    return None


class WebSocket:
    """One open connection. Binary messages in, binary messages out."""

    def __init__(self, sock: socket.socket):
        self._sock: Optional[socket.socket] = sock
        self._send_lock = threading.Lock()
        self._closed = False
        self._pending: bytes = b""

    def send(self, data: bytes) -> None:
        """Send one binary message."""
        self._write_frame(_OP_BINARY, bytes(data))

    def recv(self) -> Optional[bytes]:
        """Block for the next binary message, or return ``None`` once the peer
        has closed the connection (or it was closed locally)."""
        fragments: List[bytes] = []
        message_opcode: Optional[int] = None
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
                if message_opcode is None:
                    raise WebSocketError("crdtsync: continuation frame with no message")
            elif opcode == _OP_BINARY:
                if message_opcode is not None:
                    raise WebSocketError("crdtsync: interleaved data frames")
                message_opcode = opcode
            elif opcode == _OP_TEXT:
                raise WebSocketError("crdtsync: unexpected non-binary websocket frame")
            else:
                raise WebSocketError(f"crdtsync: unknown websocket opcode {opcode}")
            fragments.append(payload)
            if fin:
                return b"".join(fragments)

    def close(self) -> None:
        """Send a close frame (best effort) and shut the socket down, so a
        blocked :meth:`recv` returns. Idempotent."""
        if self._closed:
            return
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
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
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
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        with self._send_lock:
            sock = self._sock
            if sock is None:
                if ignore_errors:
                    return
                raise WebSocketError("crdtsync: send on a closed websocket")
            try:
                sock.sendall(bytes(header) + masked)
            except OSError as err:
                if ignore_errors:
                    return
                raise WebSocketError(f"crdtsync: websocket send failed: {err}") from err

    def _read_frame(self):
        head = self._read_exact(2)
        if head is None:
            return None
        fin = bool(head[0] & 0x80)
        opcode = head[0] & 0x0F
        masked = bool(head[1] & 0x80)
        length = head[1] & 0x7F
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
        if length > MAX_FRAME_BYTES:
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
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        return fin, opcode, payload

    def _read_exact(self, count: int) -> Optional[bytes]:
        while len(self._pending) < count:
            sock = self._sock
            if sock is None:
                return None
            try:
                chunk = sock.recv(65536)
            except OSError:
                return None
            if not chunk:
                self._closed = True
                return None
            self._pending += chunk
        out, self._pending = self._pending[:count], self._pending[count:]
        return out
