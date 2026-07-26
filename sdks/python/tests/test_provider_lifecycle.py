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
from typing import Optional

import pytest
from crdtsync import (
    Capability,
    Client,
    ErrorCode,
    Provider,
    ServerError,
    SubjectKind,
    connect,
)
from crdtsync import _MIN_RECONNECT_DELAY

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


def peer_edit(channel: int, key: bytes = b"note", replica: int = 9) -> bytes:
    """An Ops frame authored by another replica, retargeted at ``channel`` — what
    the server fans out when a peer edits the room. Each peer gets its own
    replica id, so two of them author distinct ops."""
    peer = Client(bytes([replica] + [0] * 15))
    peer_channel, _ = peer.subscribe(b"room")
    frame = peer.text_insert(peer_channel, [key], 0, "ok")
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


class BlockingSocket(FakeSocket):
    """A socket whose writes park, as a real one does once the peer stops
    reading and the send window fills."""

    def __init__(self):
        super().__init__()
        self.block = threading.Event()
        self.blocked = threading.Event()

    def send(self, data: bytes) -> None:
        if self.block.is_set() and not self.closed:
            self.blocked.set()
            # A real socket only gives this up when it is shut down.
            while not self.closed:
                time.sleep(0.002)
            raise OSError("crdtsync test: socket shut down under a write")
        super().send(data)


def wait_for(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("timed out waiting for a condition")
        time.sleep(0.005)


def handshake(
    transport: FakeTransport, index: int = 0, catch_up: Optional[bytes] = None
) -> FakeSocket:
    """Drive one socket from dial to synced: authenticate it, then answer its
    Subscribe with a catch-up (an empty one, for an empty room, unless given)."""
    socket = transport.socket(index)
    socket.wait_sent(3)
    socket.deliver(auth_ok())
    socket.wait_sent(4)
    socket.deliver(ops(0) if catch_up is None else catch_up)
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

    def test_the_backoff_escalates_and_stays_finite(self, transport):
        p = Provider(
            "ws://fake",
            "room",
            transport=transport,
            max_reconnect_delay=10.0,
            reconnect=False,
        )
        p.close()  # the reader is done, so the backoff is this test's to drive
        delays = [p._backoff() for _ in range(12)]
        assert all(0 < d <= 10.0 for d in delays)
        assert delays[0] <= 0.25
        assert delays[-1] >= 5.0

        # Jittered, so a restarted server is not met by every client at once.
        p._attempt = 32
        assert len({p._backoff() for _ in range(20)}) > 1

        # An unreachable server left overnight builds a large attempt count; an
        # unclamped exponent becomes an integer too large to turn into a float,
        # and that failure kills the reader thread rather than delaying it.
        p._attempt = 100_000
        assert 5.0 <= p._backoff() <= 10.0

    def test_a_zero_reconnect_ceiling_does_not_spin(self, transport):
        p = Provider(
            "ws://fake", "room", transport=transport, max_reconnect_delay=0, reconnect=False
        )
        p.close()
        assert p._backoff() >= _MIN_RECONNECT_DELAY * 0.5

    def test_a_server_error_does_not_restart_the_backoff(self, transport):
        # An error is not proof the connection works — the update-required push
        # is precisely a server saying so before it drops. Restarting the backoff
        # on one pins every client at the floor delay, hammering the server.
        p = Provider("ws://fake", "room", transport=transport, on_error=lambda _c: None)
        try:
            socket = handshake(transport)
            p.wait_connected(timeout=2.0)
            p._attempt = 6
            socket.deliver(error(ErrorCode.UPDATE_REQUIRED))
            wait_for(lambda: p.state == "connected")
            time.sleep(0.05)
            assert p._attempt == 6

            # A frame the session applies is proof, and does restart it.
            socket.deliver(peer_edit(p._channel))
            wait_for(lambda: p._attempt == 0)
        finally:
            p.close()

    def test_a_refused_reconnect_handshake_dials_again(self, transport):
        seen = []
        p = Provider(
            "ws://fake",
            "room",
            transport=transport,
            max_reconnect_delay=0.01,
            on_error=seen.append,
        )
        try:
            first = handshake(transport)
            p.wait_connected(timeout=2.0)
            first.close()

            # The reconnect's credential is refused. A socket that will not
            # authenticate never carries a Subscribe, so sitting on it would
            # wedge the provider — it has to be dropped and retried.
            second = transport.socket(1)
            second.wait_sent(3)
            second.deliver(error(ErrorCode.AUTH_FAILED))
            transport.socket(2)
            assert seen == [ErrorCode.AUTH_FAILED]
        finally:
            p.close()

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
        # The catch-up carries a peer's ops, so the channel resumes from a
        # position past zero and the resume differs from a fresh Subscribe.
        first = handshake(transport, catch_up=peer_edit(0))
        provider.wait_connected(timeout=2.0)
        provider.doc.get_text("body").insert(0, "hi")
        edit = first.wait_sent(5)[4]

        first.close()
        second = transport.socket(1)
        second.wait_sent(3)
        second.deliver(auth_ok())
        resume, resend = second.wait_sent(5)[3:5]
        assert tag(resume) == _TAG_SUBSCRIBE
        assert resume != provider._subscribe_frame  # resumed, not restarted
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

    def test_an_inert_edit_reaches_neither_the_wire_nor_the_listeners(
        self, transport, provider
    ):
        socket = handshake(transport)
        provider.wait_connected(timeout=2.0)
        # The client seat frames every edit, even one that matched nothing; a
        # frame carrying no ops is not an edit, exactly as on a local doc.
        provider.doc.get_text("body").delete(0, 0)
        time.sleep(0.05)
        assert len(socket.sent) == 4

    def test_close_breaks_a_write_that_is_parked(self, transport):
        blocking = BlockingSocket()
        p = Provider("ws://fake", "room", transport=lambda _url: blocking, reconnect=False)
        try:
            blocking.wait_sent(3)
            blocking.deliver(auth_ok())
            blocking.wait_sent(4)
            blocking.deliver(ops(0))
            p.wait_connected(timeout=2.0)

            # An author parked in a socket write holds the send lock. Closing has
            # to be able to shut that socket down — waiting for the lock the
            # stuck writer owns would deadlock the very call meant to free it.
            blocking.block.set()
            author = threading.Thread(
                target=lambda: p.doc.get_text("body").insert(0, "stuck"), daemon=True
            )
            author.start()
            assert blocking.blocked.wait(2.0)

            closed = threading.Thread(target=p.close, daemon=True)
            closed.start()
            closed.join(timeout=5.0)
            assert not closed.is_alive()
            author.join(timeout=2.0)
            assert not author.is_alive()
        finally:
            blocking.close()
            p.close()

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


class TestAcl:
    def test_a_grant_rides_the_op_path_and_names_its_tuple(self, transport, provider):
        socket = handshake(transport)
        provider.wait_connected(timeout=2.0)
        tuple_id = provider.acl_grant(
            SubjectKind.ACTOR, bytes(16), ["root"], capability=Capability.READ
        )
        assert len(tuple_id) == 16
        assert tag(socket.wait_sent(5)[4]) == _TAG_OPS
        provider.acl_revoke(tuple_id)
        assert tag(socket.wait_sent(6)[5]) == _TAG_OPS

    def test_a_malformed_grant_raises_rather_than_granting_nothing(
        self, transport, provider
    ):
        socket = handshake(transport)
        provider.wait_connected(timeout=2.0)
        # An access-control call that returns as though it succeeded is the worst
        # way to fail: the caller believes access was given.
        with pytest.raises(ValueError):
            provider.acl_grant(SubjectKind.ACTOR, b"too-short", capability=Capability.READ)
        with pytest.raises(ValueError):
            provider.acl_grant(SubjectKind.ACTOR, bytes(16))  # neither capability nor role
        time.sleep(0.05)
        assert len(socket.sent) == 4

    def test_a_revoke_naming_no_tuple_writes_nothing(self, transport, provider):
        socket = handshake(transport)
        provider.wait_connected(timeout=2.0)
        provider.acl_revoke(bytes(16))
        time.sleep(0.05)
        assert len(socket.sent) == 4

    def test_a_grant_needs_an_actor_to_credit(self, transport):
        p = Provider("ws://fake", "room", transport=transport)
        try:
            socket = transport.socket(0)
            socket.wait_sent(3)
            # No AuthOk yet, so the connection has no authenticated actor.
            with pytest.raises(ValueError, match="grantor"):
                p.acl_grant(SubjectKind.ACTOR, bytes(16), capability=Capability.READ)
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

    def test_a_hook_that_raises_on_the_first_frame_still_completes_the_sync(
        self, transport
    ):
        def explode(_rejected):
            raise RuntimeError("callback bug")

        p = Provider("ws://fake", "room", transport=transport, on_ops_rejected=explode)
        try:
            socket = transport.socket(0)
            socket.wait_sent(3)
            socket.deliver(auth_ok())
            socket.wait_sent(4)
            # The catch-up frame carries a rejection the hook fumbles. If that
            # aborted the frame the provider would sit at "catchup" forever,
            # dropping every later edit into an outbox nothing replays.
            socket.deliver(ops_rejected(p._channel, [1], ErrorCode.FORBIDDEN))
            p.wait_connected(timeout=2.0)
            p.doc.get_text("body").insert(0, "after")
            assert tag(socket.wait_sent(5)[4]) == _TAG_OPS
        finally:
            p.close()

    def test_a_listener_may_edit_while_an_inbound_frame_folds(self, transport, provider):
        socket = handshake(transport)
        provider.wait_connected(timeout=2.0)

        # A listener runs on the reader thread, and an edit from it takes the
        # author's gate — which the fold must already have let go of, or the two
        # locks invert against every application thread.
        def echo(_event):
            if not str(provider.doc.get_text("echo")):
                provider.doc.get_text("echo").insert(0, "seen")

        provider.doc.on_update(echo)
        socket.deliver(peer_edit(provider._channel))
        wait_for(lambda: str(provider.doc.get_text("echo")) == "seen")
        assert tag(socket.wait_sent(5)[4]) == _TAG_OPS

    def test_a_raising_doc_listener_leaves_the_connection_alone(self, transport, provider):
        socket = handshake(transport)
        provider.wait_connected(timeout=2.0)
        off = provider.doc.on_update(
            lambda _e: (_ for _ in ()).throw(RuntimeError("listener bug"))
        )
        socket.deliver(peer_edit(provider._channel))
        wait_for(lambda: str(provider.doc.get_text("note")) == "ok")

        # The connection is still folding and still writing after the raise, not
        # merely still labelled connected. A local edit's listener raises at its
        # caller, so the observer stands down before that half.
        socket.deliver(peer_edit(provider._channel, key=b"second", replica=10))
        wait_for(lambda: str(provider.doc.get_text("second")) == "ok")
        off()
        provider.doc.get_text("body").insert(0, "after")
        assert tag(socket.wait_sent(5)[4]) == _TAG_OPS
        assert provider.state == "connected"

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
