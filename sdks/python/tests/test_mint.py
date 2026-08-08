"""A stamp is drawn from the replica's own id space, and the space is finite. When
it runs out an edit is *refused* — nothing emitted, nothing changed — because the
alternative is re-issuing an id that is already live, which every peer drops as a
replay. Refusal is the right answer and silence is not: every mutator returns the
same empty ops an inert edit returns, so without a raise the application reports a
write that never happened."""

import struct

import pytest

from crdtsync import Doc, MintExhausted

# An op body opens with its author's 16-byte client id and its 8-byte sequence, so
# the stamp's lamport runs from body offset 24 — past the frame's 4-byte length
# prefix.
_LAMPORT = 4 + 16 + 8
# The last id of the space: ``u64::MAX >> 1``. A stamp may legally sit there, which
# is why one op is enough to spend its author's mint.
_CEILING = (1 << 63) - 1


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def planted(client: bytes) -> bytes:
    """One op frame from a doc authored under ``client``, its stamp moved to the
    last id of the space."""
    doc = Doc(client)
    emitted = []
    doc.on_update(lambda e: emitted.append(e.ops) if e.origin == "local" else None)
    doc.get_map("root").set("k", 1)
    (size,) = struct.unpack_from("<I", emitted[0], 0)
    frame = bytearray(emitted[0][: 4 + size])
    struct.pack_into("<Q", frame, _LAMPORT, _CEILING)
    return bytes(frame)


def test_a_spent_id_space_raises_rather_than_editing_into_silence():
    me = cid(1)
    doc = Doc(me)
    # A peer authoring under this replica's own client id needs one admissible op
    # to put the id space at its end.
    doc.apply_update(planted(me))

    with pytest.raises(MintExhausted):
        doc.get_map("root").set("k", 1)
    assert doc.get_map("root").get("k") is None


def test_a_transaction_raises_too():
    me = cid(2)
    doc = Doc(me)
    doc.apply_update(planted(me))

    with pytest.raises(MintExhausted):
        doc.transact(lambda: doc.get_list("items").insert(0, "a"))


def test_an_ordinary_edit_is_untouched():
    doc = Doc(cid(3))
    doc.get_map("root").set("k", 1)
    # An inert edit emits nothing either, and that is not a refusal.
    doc.get_map("root").set("k", 1)
    assert doc.get_map("root").get("k") == 1
