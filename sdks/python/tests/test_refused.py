"""A fold reports how many ops it took now *and* how many no replica will ever
hold, because the two zeros mean opposite things. A buffered op is waiting on a
create a later update carries; a refused one is a bug in whoever wrote it, and
offline, P2P and relayed peers reach this fold with no server between them to
reject it first."""

import struct
from typing import List

from crdtsync import Doc

# An op body opens with its author's 16-byte client id, its 8-byte sequence and
# the stamp's 8-byte lamport, so the stamp's own client id runs from body offset
# 32 — past the frame's 4-byte length prefix. Naming another client there mints
# node ids inside that client's id space, which no replica will ever hold.
_STAMP_CLIENT = 4 + 16 + 8 + 8


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def frames(log: bytes) -> List[bytes]:
    """An op log frames each op as a u32 length then its body; split it apart."""
    out = []
    at = 0
    while at < len(log):
        (size,) = struct.unpack_from("<I", log, at)
        out.append(log[at : at + 4 + size])
        at += 4 + size
    return out


def forge_stamp_client(frame: bytes) -> bytes:
    return frame[:_STAMP_CLIENT] + b"\xff" * 16 + frame[_STAMP_CLIENT + 16 :]


def test_a_refused_op_is_counted_apart_from_a_buffered_one():
    a = Doc(cid(1))
    emitted = []
    a.on_update(lambda e: emitted.append(e.ops) if e.origin == "local" else None)

    # The first write into a map is two ops: the container create, then the write
    # into it. A second write is one op, targeting the same container.
    a.get_map("root").set("k", 1)
    a.get_map("root").set("k2", 2)
    opened = frames(emitted[0])
    assert len(opened) == 2
    create, write = opened
    (later,) = frames(emitted[1])

    b = Doc(cid(2))
    assert b.apply_update(forge_stamp_client(later) + write) == (0, 1)

    # The buffered op was waiting, not refused: the create releases it. The forged
    # one is gone for good, though its target is now reachable.
    assert b.apply_update(create) == (1, 0)
    assert b.get_map("root").get("k") == 1
    assert b.get_map("root").get("k2") is None


def test_a_malformed_batch_is_neither_applied_nor_refused():
    # Nothing decoded, so there is no op to judge.
    assert Doc(cid(1)).apply_update(b"\xff\xff\xff\xff") == (-1, 0)
