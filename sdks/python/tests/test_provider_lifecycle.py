"""The networked provider's connection lifecycle, driven deterministically with
a fake transport — no server.

The provider is scripted with the frames a server would send, so the handshake
ordering, catch-up, reconnect resume/resend, outbox drain, connection-state
transitions, awareness fan-out, and the server-signal hooks are all observable
without timing on a real socket. The frame builders below encode the handful of
server-directed wire messages the lifecycle turns on."""

import queue
import struct
import threading
import time

import pytest
from crdtsync import Client, ErrorCode, Provider, ServerError, connect

# --- server frames (tag byte, then the message's fields, all little-endian) ---

_TAG_HELLO = 0
_TAG_SUBSCRIBE = 1
_TAG_OPS = 2
_TAG_ERROR = 3
_TAG_AUTH = 6
_TAG_AUTH_OK = 7
_TAG_AWARENESS_SET = 8
_TAG_AWARENESS_UPDATE = 9
_TAG_ACCEPTED = 18
_TAG_OPS_REJECTED = 22
_TAG_REDIRECT = 23

PROTOCOL_HEADER = b"CRDT" + struct.pack("<I", 1)


def _field(value: bytes) -> bytes:
    return struct.pack("<I", len(value)) + value


def auth_ok(actor: bytes = b"anonymous") -> bytes:
    return bytes([_TAG_AUTH_OK]) + _field(actor)


def ops(channel: int, payload: bytes = b"") -> bytes:
    """An Ops delta. An empty payload is the catch-up reply for an empty room."""
    return bytes([_TAG_OPS]) + struct.pack("<I", channel) + payload


def accepted(channel: int, through: int) -> bytes:
    return bytes([_TAG_ACCEPTED]) + struct.pack("<IQ", channel, through)


def error(code: ErrorCode) -> bytes:
    return bytes([_TAG_ERROR]) + struct.pack("<H", int(code)) + _field(b"") + _field(b"")


def ops_rejected(channel: int, seqs, reason: ErrorCode) -> bytes:
    out = bytes([_TAG_OPS_REJECTED]) + struct.pack("<IHI", channel, int(reason), len(seqs))
    return out + b"".join(struct.pack("<Q", seq) for seq in seqs)


def redirect(room: bytes, leader_addr: bytes) -> bytes:
    return bytes([_TAG_REDIRECT]) + _field(room) + _field(leader_addr)


def awareness_update(channel: int, actor: bytes, key: bytes, value: bytes) -> bytes:
    return (
        bytes([_TAG_AWARENESS_UPDATE])
        + struct.pack("<I", channel)
        + _field(actor)
        + _field(key)
        + _field(value)
    )


def tag(frame: bytes) -> int:
    return frame[0]


def peer_edit(channel: int) -> bytes:
    """An Ops frame authored by another replica, retargeted at ``channel`` — what
    the server fans out when a peer edits the room."""
    peer = Client(bytes([9] + [0] * 15))
    peer_channel, _ = peer.subscribe(b"room")
    frame = peer.text_insert(peer_channel, [b"note"], 0, "ok")
    return frame[:1] + struct.pack("<I", channel) + frame[5:]


# --- fake transport ---


class FakeSocket:
    """A scripted socket: the provider's writes land in :attr:`sent`, and a test
    hands it inbound frames with :meth:`deliver`."""

    def __init__(self):
        self.sent = []
        self.closed = False
        self._inbox = queue.Queue()

    def send(self, data: bytes) -> None:
        if self.closed:
            raise OSError("crdtsync test: send on a closed socket")
        self.sent.append(bytes(data))

    def recv(self):
        return self._inbox.get()

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._inbox.put(None)  # unblock the reader, as a real shutdown does

    def deliver(self, frame: bytes) -> None:
        self._inbox.put(frame)

    def wait_sent(self, count: int, timeout: float = 2.0) -> list:
        wait_for(lambda: len(self.sent) >= count, timeout)
        return self.sent


class FakeTransport:
    """Hands out a fresh :class:`FakeSocket` per dial, keeping them all so a test
    can watch a reconnect open the next one."""

    def __init__(self):
        self.sockets = []

    def __call__(self, _url: str) -> FakeSocket:
        socket = FakeSocket()
        self.sockets.append(socket)
        return socket

    def socket(self, index: int, timeout: float = 2.0) -> FakeSocket:
        wait_for(lambda: len(self.sockets) > index, timeout)
        return self.sockets[index]


class RefusingTransport:
    """A dial that never connects."""

    def __call__(self, _url: str):
        raise OSError("crdtsync test: connection refused")


def wait_for(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("timed out waiting for a condition")
        time.sleep(0.005)


def handshake(transport: FakeTransport, index: int = 0) -> FakeSocket:
    """Drive one socket from dial to synced: authenticate it, then answer its
    Subscribe with an empty catch-up."""
    socket = transport.socket(index)
    socket.wait_sent(3)
    socket.deliver(auth_ok())
    socket.wait_sent(4)
    socket.deliver(ops(0))
    return socket


@pytest.fixture
def transport():
    return FakeTransport()


@pytest.fixture
def provider(transport):
    p = Provider("ws://fake", "room", transport=transport, connect_timeout=2.0)
    yield p
    p.close()


class TestHandshake:
    def test_opens_with_the_header_then_hello_then_auth(self, transport, provider):
        socket = transport.socket(0)
        header, hello, auth = socket.wait_sent(3)[:3]
        assert header == PROTOCOL_HEADER
        assert tag(hello) == _TAG_HELLO
        assert tag(auth) == _TAG_AUTH

    def test_subscribes_only_once_the_socket_has_authenticated(self, transport, provider):
        socket = transport.socket(0)
        socket.wait_sent(3)
        # Nothing beyond the opening three frames until the AuthOk folds.
        time.sleep(0.05)
        assert len(socket.sent) == 3
        socket.deliver(auth_ok())
        subscribe = socket.wait_sent(4)[3]
        assert tag(subscribe) == _TAG_SUBSCRIBE
        assert b"room" in subscribe

    def test_catch_up_completes_the_initial_sync(self, transport, provider):
        handshake(transport)
        provider.wait_connected(timeout=2.0)
        assert provider.state == "connected"

    def test_an_edit_during_the_handshake_waits_for_the_channel(self, transport, provider):
        socket = transport.socket(0)
        socket.wait_sent(3)
        socket.deliver(auth_ok())
        socket.wait_sent(4)  # the Subscribe; the channel is not caught up yet

        provider.doc.get_text("body").insert(0, "eager")
        # Writing an edit into the handshake is a protocol violation the server
        # closes on, so it stays in the outbox until the channel is ready.
        time.sleep(0.05)
        assert len(socket.sent) == 4
        assert provider.outbox_len > 0

        socket.deliver(ops(0))
        replay = socket.wait_sent(5)[4]
        assert tag(replay) == _TAG_OPS
        provider.wait_connected(timeout=2.0)

    def test_a_handshake_error_is_fatal(self, transport):
        p = Provider("ws://fake", "room", transport=transport, connect_timeout=2.0)
        try:
            socket = transport.socket(0)
            socket.wait_sent(3)
            socket.deliver(error(ErrorCode.AUTH_FAILED))
            with pytest.raises(ServerError) as raised:
                p.wait_connected(timeout=2.0)
            assert raised.value.code == ErrorCode.AUTH_FAILED
        finally:
            p.close()

    def test_credential_rides_the_auth_frame(self, transport):
        p = Provider("ws://fake", "room", credential="token-42", transport=transport)
        try:
            auth = transport.socket(0).wait_sent(3)[2]
            assert b"token-42" in auth
        finally:
            p.close()


class TestConnectFailures:
    def test_a_refused_dial_rejects_when_reconnect_is_off(self):
        with pytest.raises(ConnectionError):
            connect("ws://fake", "room", transport=RefusingTransport(), reconnect=False)

    def test_a_silent_server_times_the_connect_out(self, transport):
        p = Provider("ws://fake", "room", transport=transport, connect_timeout=0.1)
        try:
            with pytest.raises(TimeoutError):
                p.wait_connected()
        finally:
            p.close()

    def test_close_rejects_a_pending_wait(self, transport):
        p = Provider("ws://fake", "room", transport=transport, connect_timeout=5.0)
        transport.socket(0).wait_sent(3)
        threading.Timer(0.05, p.close).start()
        with pytest.raises(ConnectionError):
            p.wait_connected()
        assert p.state == "disconnected"

    def test_a_refused_dial_keeps_retrying_when_reconnect_is_on(self):
        dials = []

        def transport(_url):
            dials.append(_url)
            raise OSError("crdtsync test: connection refused")

        p = Provider("ws://fake", "room", transport=transport, max_reconnect_delay=0.01)
        try:
            wait_for(lambda: len(dials) >= 3)
        finally:
            p.close()


class TestEditsAndReconnect:
    def test_a_local_edit_frames_and_sends(self, transport, provider):
        socket = handshake(transport)
        provider.wait_connected(timeout=2.0)
        provider.doc.get_text("body").insert(0, "hi")
        edit = socket.wait_sent(5)[4]
        assert tag(edit) == _TAG_OPS
        assert provider.outbox_len > 0  # unacknowledged until the server accepts

    def test_an_edit_made_offline_waits_in_the_outbox(self, transport, provider):
        socket = handshake(transport)
        provider.wait_connected(timeout=2.0)
        socket.close()
        wait_for(lambda: provider.state != "connected")
        provider.doc.get_text("body").insert(0, "offline")
        assert provider.outbox_len > 0

    def test_a_reconnect_resumes_the_channel_and_resends_the_outbox(
        self, transport, provider
    ):
        first = handshake(transport)
        provider.wait_connected(timeout=2.0)
        provider.doc.get_text("body").insert(0, "hi")
        edit = first.wait_sent(5)[4]

        first.close()
        second = transport.socket(1)
        second.wait_sent(3)
        second.deliver(auth_ok())
        resume, resend = second.wait_sent(5)[3:5]
        assert tag(resume) == _TAG_SUBSCRIBE
        assert tag(resend) == _TAG_OPS
        assert resend == edit
        wait_for(lambda: provider.state == "connected")

    def test_a_resent_edit_relayed_back_applies_once(self, transport, provider):
        first = handshake(transport)
        provider.wait_connected(timeout=2.0)
        provider.doc.get_text("body").insert(0, "hi")
        edit = first.wait_sent(5)[4]

        first.close()
        second = transport.socket(1)
        second.wait_sent(3)
        second.deliver(auth_ok())
        second.wait_sent(5)
        # The server fans the replayed batch back to its author; the replica
        # deduplicates by op id, so the text must not double. A peer's frame
        # delivered after it proves the fold path is live — without it, a
        # provider that ignored both frames would look identical.
        second.deliver(edit)
        second.deliver(peer_edit(provider._channel))
        wait_for(lambda: str(provider.doc.get_text("note")) == "ok")
        assert str(provider.doc.get_text("body")) == "hi"

    def test_an_acknowledgement_drains_the_outbox(self, transport, provider):
        socket = handshake(transport)
        provider.wait_connected(timeout=2.0)
        provider.doc.get_text("body").insert(0, "hi")
        assert provider.outbox_len > 0
        socket.deliver(accepted(provider._channel, 2**32))
        wait_for(lambda: provider.outbox_len == 0)


class TestConnectionState:
    def test_transitions_are_reported_in_order(self, transport):
        states = []
        p = Provider("ws://fake", "room", transport=transport, reconnect=False)
        try:
            p.on_state(states.append)
            handshake(transport)
            p.wait_connected(timeout=2.0)
            transport.socket(0).close()
            wait_for(lambda: states[-1:] == ["disconnected"])
            assert states == ["connected", "disconnected"]
        finally:
            p.close()

    def test_a_fatal_error_never_reports_connected(self, transport):
        states = []
        p = Provider("ws://fake", "room", transport=transport, reconnect=False)
        try:
            p.on_state(states.append)
            socket = transport.socket(0)
            socket.wait_sent(3)
            socket.deliver(auth_ok())
            socket.wait_sent(4)
            socket.deliver(error(ErrorCode.UNSUPPORTED_VERSION))
            with pytest.raises(ServerError):
                p.wait_connected(timeout=2.0)
            assert "connected" not in states
        finally:
            p.close()

    def test_unsubscribing_a_listener_stops_it(self, transport, provider):
        states = []
        off = provider.on_state(states.append)
        off()
        handshake(transport)
        provider.wait_connected(timeout=2.0)
        assert states == []


class TestServerSignals:
    def test_an_error_after_sync_reports_without_closing(self, transport):
        seen = []
        p = Provider("ws://fake", "room", transport=transport, on_error=seen.append)
        try:
            socket = handshake(transport)
            p.wait_connected(timeout=2.0)
            socket.deliver(error(ErrorCode.UPDATE_REQUIRED))
            wait_for(lambda: seen == [ErrorCode.UPDATE_REQUIRED])
            assert p.state == "connected"
        finally:
            p.close()

    def test_refused_ops_reach_the_rejection_hook(self, transport):
        seen = []
        p = Provider("ws://fake", "room", transport=transport, on_ops_rejected=seen.extend)
        try:
            socket = handshake(transport)
            p.wait_connected(timeout=2.0)
            p.doc.get_text("body").insert(0, "hi")
            socket.deliver(ops_rejected(p._channel, [1], ErrorCode.FORBIDDEN))
            wait_for(lambda: len(seen) == 1)
            assert seen[0].channel == p._channel
            assert seen[0].reason == ErrorCode.FORBIDDEN
        finally:
            p.close()

    def test_a_room_redirect_reaches_the_redirect_hook(self, transport):
        seen = []
        p = Provider("ws://fake", "room", transport=transport, on_redirect=seen.extend)
        try:
            socket = handshake(transport)
            p.wait_connected(timeout=2.0)
            socket.deliver(redirect(b"room", b"10.0.0.2:9000"))
            wait_for(lambda: len(seen) == 1)
            assert seen[0].room == b"room"
            assert seen[0].leader_addr == b"10.0.0.2:9000"
        finally:
            p.close()


class TestAwareness:
    def test_publishing_sends_an_awareness_frame(self, transport, provider):
        socket = handshake(transport)
        provider.wait_connected(timeout=2.0)
        provider.set_awareness("cursor", "10")
        frame = socket.wait_sent(5)[4]
        assert tag(frame) == _TAG_AWARENESS_SET
        assert b"cursor" in frame

    def test_a_peer_entry_folds_in(self, transport, provider):
        socket = handshake(transport)
        provider.wait_connected(timeout=2.0)
        socket.deliver(awareness_update(provider._channel, b"peer", b"cursor", b"5"))
        wait_for(lambda: provider.awareness_len() == 1)
        assert provider.awareness(b"peer", "cursor") == b"5"

    def test_presence_is_republished_after_a_reconnect(self, transport, provider):
        first = handshake(transport)
        provider.wait_connected(timeout=2.0)
        provider.set_awareness("cursor", "10")
        first.wait_sent(5)

        # Presence is not durable — the server drops it with the socket — so the
        # provider owes the room a fresh publish once the channel is back.
        first.close()
        second = transport.socket(1)
        second.wait_sent(3)
        second.deliver(auth_ok())
        republished = second.wait_sent(5)[4]
        assert tag(republished) == _TAG_AWARENESS_SET
        assert b"cursor" in republished


class TestCallbackFailures:
    def test_a_raising_hook_does_not_strand_the_connection(self, transport):
        def explode(_signal):
            raise RuntimeError("callback bug")

        p = Provider("ws://fake", "room", transport=transport, on_error=explode)
        try:
            socket = handshake(transport)
            p.wait_connected(timeout=2.0)
            socket.deliver(error(ErrorCode.UPDATE_REQUIRED))
            # The listener's bug is reported, not fatal: the reader keeps folding
            # frames and a later edit still reaches the socket.
            socket.deliver(peer_edit(p._channel))
            wait_for(lambda: str(p.doc.get_text("note")) == "ok")
            p.doc.get_text("body").insert(0, "after")
            assert tag(socket.wait_sent(5)[4]) == _TAG_OPS
            assert p.state == "connected"
        finally:
            p.close()

    def test_a_raising_state_listener_does_not_break_close(self, transport):
        def explode(_state):
            raise RuntimeError("listener bug")

        p = Provider("ws://fake", "room", transport=transport)
        p.on_state(explode)
        handshake(transport)
        p.wait_connected(timeout=2.0)
        p.close()
        assert p.state == "disconnected"
        assert not p._thread.is_alive()
