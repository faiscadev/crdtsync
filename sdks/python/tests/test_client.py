"""The Python SDK drives the wire client over the C ABI: a local edit produces a
frame a peer folds in and converges on, and the handshake surface marshals."""

from crdtsync import Client


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


def test_channel_bounds_are_checked():
    import pytest

    with Client(cid(1)) as a:
        with pytest.raises(ValueError):
            a.register_int(-1, [b"k"], 1)
        with pytest.raises(ValueError):
            a.get_int(2**32, [b"k"])
