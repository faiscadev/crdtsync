"""The Python SDK drives the wire client over the C ABI: a local edit produces a
frame a peer folds in and converges on, and the handshake surface marshals."""

from crdtsync import Client, ErrorCode, Rejected, ServerError


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def test_a_local_edit_travels_to_a_peer():
    with Client(cid(1)) as a, Client(cid(2)) as b:
        # Both fresh sessions assign channel 0 to their first subscription.
        ca, _ = a.subscribe(b"room-1")
        cb, _ = b.subscribe(b"room-1")
        assert ca == 0 and cb == 0

        ops = a.register_int(ca, [b"age"], 30)
        assert a.get_int(ca, [b"age"]) == 30
        assert b.receive(ops) == 1
        assert b.get_int(cb, [b"age"]) == 30
        assert b.last_seen_seq(cb) == 1


def test_offline_queue_outbox_drains_on_ack():
    import struct

    with Client(cid(1)) as a:
        ca, _ = a.subscribe(b"room-1")
        a.register_int(ca, [b"age"], 30)
        assert a.outbox_len(ca) == 1
        a.register_int(ca, [b"age"], 31)
        assert a.outbox_len(ca) == 2
        # The unacknowledged tail replays as one Ops frame.
        assert len(a.resend(ca)) > 0
        # An Accepted through u64::MAX drains the outbox: tag 18, u32 channel,
        # u64 frontier.
        accepted = struct.pack("<BIQ", 18, ca, (1 << 64) - 1)
        assert a.receive(accepted) == 1
        assert a.outbox_len(ca) == 0
        assert a.resend(ca) == b""


def test_bytes_scalar_round_trips():
    with Client(cid(1)) as a, Client(cid(2)) as b:
        ca, _ = a.subscribe(b"room-1")
        cb, _ = b.subscribe(b"room-1")
        b.receive(a.set_bytes(ca, [b"blob"], b"\x00\x01\xff"))
        assert b.get_bytes(cb, [b"blob"]) == b"\x00\x01\xff"


def test_handshake_frames_marshal():
    with Client(cid(1)) as a:
        assert len(a.hello()) > 0
        assert len(a.auth(b"token")) > 0
        # No actor until the server's AuthOk is folded in.
        assert a.actor() is None


def test_declared_app_rides_along_in_the_hello_frame():
    with Client(cid(1)) as a:
        # A bare client opens as a relay — no app named in the frame.
        assert b"app-x" not in a.hello()
        # Declaring an app names it in the next Hello.
        a.declare_app(b"app-x", 3)
        assert b"app-x" in a.hello()


def test_server_advertised_schema_is_readable():
    import struct

    def advert(version: int, body: bytes) -> bytes:
        # SchemaAdvert: tag 21, u32 version, u32 length prefix, bytes.
        return struct.pack("<BII", 21, version, len(body)) + body

    with Client(cid(1)) as a:
        # Nothing advertised yet.
        assert a.active_schema_version() is None
        assert a.active_schema() is None
        # Folding a SchemaAdvert records the served version and its bytes.
        assert a.receive(advert(4, b"schema-body")) == 1
        assert a.active_schema_version() == 4
        assert a.active_schema() == b"schema-body"
        # A later advert supersedes it.
        assert a.receive(advert(5, b"next-body")) == 1
        assert a.active_schema_version() == 5
        assert a.active_schema() == b"next-body"
        # An empty body is still an advertisement, not "none".
        assert a.receive(advert(6, b"")) == 1
        assert a.active_schema_version() == 6
        assert a.active_schema() == b""


def test_awareness_publish_and_lifecycle():
    with Client(cid(1)) as a:
        ch, _ = a.subscribe(b"room-1")
        assert len(a.set_awareness(ch, b"cursor", b"x")) > 0
        assert a.awareness_len(ch) == 0  # no peer has published yet
        # Unsubscribe drops the channel: reads report absent, resume is empty.
        assert len(a.unsubscribe(ch)) > 0
        assert a.last_seen_seq(ch) is None
        assert a.resume(ch) == b""


def test_version_requests_marshal():
    with Client(cid(1)) as a:
        ch, _ = a.subscribe(b"room-1")
        assert len(a.create_version(ch, b"v1")) > 0
        assert len(a.rename_version(ch, b"v1", b"v2")) > 0
        assert len(a.delete_version(ch, b"v1")) > 0
        assert len(a.list_versions(ch)) > 0
        assert len(a.fetch_version(ch, b"v1")) > 0
        # Nothing reported until a server reply is folded in.
        assert a.versions(ch) == []
        assert a.version_state(ch, b"v1") is None


def test_receive_rejects_garbage():
    with Client(cid(1)) as a:
        assert a.receive(b"\xff\xff\xff\xff") == 0


def test_server_error_frame_raises_with_its_code():
    import struct

    import pytest

    def error(code: int, message: bytes) -> bytes:
        # Error: tag 3, u16 code, u32-prefixed message, u32-prefixed details.
        return (
            struct.pack("<BH", 3, code)
            + struct.pack("<I", len(message))
            + message
            + struct.pack("<I", 0)
        )

    with Client(cid(1)) as a:
        # A server Error surfaces as ServerError carrying the code; UPDATE_REQUIRED
        # (6) is the onUpdateRequired signal.
        with pytest.raises(ServerError) as excinfo:
            a.receive(error(6, b"please update"))
        assert excinfo.value.code is ErrorCode.UPDATE_REQUIRED
        # A normal frame still applies cleanly.
        ca, _ = a.subscribe(b"room-1")
        assert a.receive(a.register_int(ca, [b"age"], 30)) == 1


def test_a_server_ops_rejection_surfaces_the_refused_batch():
    import struct

    def ops_rejected(channel: int, seqs: list, reason: int) -> bytes:
        # OpsRejected: tag 22, u32 channel, u16 reason, u32 seq-count, u64 seqs.
        out = struct.pack("<BIHI", 22, channel, reason, len(seqs))
        for s in seqs:
            out += struct.pack("<Q", s)
        return out

    with Client(cid(1)) as a:
        ca, _ = a.subscribe(b"room-1")
        # Author an edit; its ops enter the outbox with per-client sequences 0..n.
        a.register_int(ca, [b"age"], 30)
        n = a.outbox_len(ca)
        assert n >= 1

        # The server refuses that batch — Forbidden, the auth-revoked rejection.
        assert a.receive(ops_rejected(ca, list(range(n)), ErrorCode.FORBIDDEN)) == 1

        # The drain yields the one batch: the channel, the reason, and the refused
        # ops still carrying their bytes.
        rejected = a.take_rejected()
        assert len(rejected) == 1
        r = rejected[0]
        assert isinstance(r, Rejected)
        assert r.channel == ca
        assert r.reason is ErrorCode.FORBIDDEN
        assert len(r.ops) == n
        assert all(isinstance(op, bytes) and len(op) > 0 for op in r.ops)

        # The refused ops left the outbox; draining, a second call is empty.
        assert a.outbox_len(ca) == 0
        assert a.take_rejected() == []


def test_take_rejected_is_empty_without_a_rejection():
    with Client(cid(1)) as a:
        a.subscribe(b"room-1")
        assert a.take_rejected() == []


def test_channel_bounds_are_checked():
    import pytest

    with Client(cid(1)) as a:
        with pytest.raises(ValueError):
            a.register_int(-1, [b"k"], 1)
        with pytest.raises(ValueError):
            a.get_int(2**32, [b"k"])


def test_atomic_transaction_over_the_client():
    with Client(cid(1)) as a, Client(cid(2)) as b:
        ca, _ = a.subscribe(b"room-1")
        cb, _ = b.subscribe(b"room-1")

        a.begin_atomic(ca)
        # Edits accumulate while recording; only the commit frame is sent.
        a.register_int(ca, [b"x"], 1)
        a.register_int(ca, [b"y"], 2)
        frame = a.commit_atomic(ca)
        assert len(frame) > 0
        assert a.get_int(ca, [b"x"]) == 1

        b.receive(frame)
        assert b.get_int(cb, [b"x"]) == 1
        assert b.get_int(cb, [b"y"]) == 2
