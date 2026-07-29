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


def forge_stamp_client(frame: bytes, author: bytes) -> bytes:
    # Read the field back first, so a codec reordering fails here by name rather
    # than as an unexplained "nothing was refused" further down.
    assert frame[_STAMP_CLIENT : _STAMP_CLIENT + 16] == author
    return frame[:_STAMP_CLIENT] + b"\xff" * 16 + frame[_STAMP_CLIENT + 16 :]


def opened_map(first: int):
    """A doc that wrote twice into one map, with its (create, write, later) ops.
    The first write into a map is two ops — the container create, then the write
    into it; a second write is one op, targeting the same container."""
    d = Doc(cid(first))
    emitted = []
    d.on_update(lambda e: emitted.append(e.ops) if e.origin == "local" else None)
    d.get_map("root").set("k", 1)
    d.get_map("root").set("k2", 2)
    assert len(emitted) == 2
    opened = frames(emitted[0])
    assert len(opened) == 2
    (later,) = frames(emitted[1])
    return opened[0], opened[1], later


def test_a_refused_op_is_counted_apart_from_a_buffered_one():
    create, write, later = opened_map(1)

    b = Doc(cid(2))
    assert b.apply_update(forge_stamp_client(later, cid(1)) + write) == (0, 1)

    # The buffered op was waiting, not refused: the create releases it. The forged
    # one is gone for good, though its target is now reachable.
    assert b.apply_update(create) == (1, 0)
    assert b.get_map("root").get("k") == 1
    assert b.get_map("root").get("k2") is None

    # A replay of what already landed is a duplicate, never a refusal.
    assert b.apply_update(create + write) == (0, 0)


def test_the_rest_of_a_batch_carrying_one_forgery_applies():
    create, write, later = opened_map(1)

    # The everyday shape: one forgery riding a stream of honest ops. The refusal
    # is per op, not per batch.
    b = Doc(cid(2))
    assert b.apply_update(forge_stamp_client(later, cid(1)) + create + write) == (2, 1)
    assert b.get_map("root").get("k") == 1


def test_a_malformed_batch_is_neither_applied_nor_refused():
    # Nothing decoded, so there is no op to judge.
    assert Doc(cid(1)).apply_update(b"\xff\xff\xff\xff") == (-1, 0)
