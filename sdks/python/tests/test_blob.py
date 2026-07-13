"""The Python SDK sets an inline blob and a store-backed ref, reads both back as
BlobRefs, signals the over-ceiling not-inlined case, and enqueues a blob edit
through the client's outbox."""

from crdtsync import BlobRef, Client, Document

INLINE_MAX = 4096


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def test_inline_blob_reads_back_with_bytes():
    with Document(cid(1)) as a:
        raw = bytes([0x89]) + b"PNG\x00\xff"
        ops = a.set_blob([b"avatar"], "image/png", raw)
        assert ops  # inlined, with ops to broadcast
        blob = a.get_blob([b"avatar"])
        assert isinstance(blob, BlobRef)
        assert blob.mime == "image/png"
        assert blob.size == len(raw)
        assert blob.inline == raw
        assert len(blob.id) == 16
        assert blob.id != bytes(16)  # a real handle was minted


def test_blob_ref_reads_back_without_bytes():
    with Document(cid(1)) as a:
        handle = bytes(range(16))
        a.set_blob_ref([b"video"], handle, "video/mp4", 10_000_000)
        blob = a.get_blob([b"video"])
        assert blob == BlobRef(id=handle, mime="video/mp4", size=10_000_000, inline=None)


def test_over_ceiling_blob_is_not_inlined():
    with Document(cid(1)) as a:
        assert a.set_blob([b"huge"], "application/octet-stream", b"\x00" * (INLINE_MAX + 1)) is None
        assert a.get_blob([b"huge"]) is None


def test_inline_and_ref_are_distinct_at_two_paths():
    with Document(cid(1)) as a:
        a.set_blob([b"small"], "image/png", b"tiny")
        a.set_blob_ref([b"large"], bytes([9] * 16), "video/mp4", 9_000_000)
        assert a.get_blob([b"small"]).inline == b"tiny"
        assert a.get_blob([b"large"]).inline is None


def test_blob_converges_on_a_peer():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        ops = a.set_blob([b"pic"], "image/png", b"tiny-png")
        assert b.apply(ops) >= 1
        assert b.get_blob([b"pic"]) == a.get_blob([b"pic"])


def test_absent_slot_reads_none():
    with Document(cid(1)) as a:
        assert a.get_blob([b"nope"]) is None


def test_client_blob_edit_enqueues_and_travels():
    with Client(cid(1)) as a, Client(cid(2)) as b:
        ca, _ = a.subscribe(b"room-1")
        cb, _ = b.subscribe(b"room-1")

        frame = a.set_blob(ca, [b"avatar"], "image/png", b"tiny-png")
        assert a.outbox_len(ca) == 1
        assert b.receive(frame) == 1

        rframe = a.set_blob_ref(ca, [b"video"], bytes([7] * 16), "video/mp4", 10_000_000)
        assert a.outbox_len(ca) == 2
        assert b.receive(rframe) == 1
