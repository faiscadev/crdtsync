"""A stamp is drawn from the replica's own id space, and the space is finite. When
it runs out an edit is *refused* — nothing emitted, nothing changed — because the
alternative is re-issuing an id that is already live, which every peer drops as a
replay. Refusal is the right answer and silence is not: every mutator returns the
same empty ops an inert edit returns, so without a raise the application reports a
write that never happened."""

import struct

import pytest

from crdtsync import Doc, MintExhausted

# An op body opens with its author's 16-byte client id, its 8-byte sequence and the
# stamp's 8-byte lamport — so the sequence runs from body offset 16 and the lamport
# from 24, both past the frame's 4-byte length prefix.
_OP_SEQ = 4 + 16
_LAMPORT = 4 + 16 + 8
# The last id of the space: ``u64::MAX >> 1``. A stamp may legally sit there, which
# is why one op is enough to spend its author's mint.
_CEILING = (1 << 63) - 1
# An op-id sequence the receiving replica has not spent, so the plant is not
# deduplicated away as one of that replica's own ops.
_UNSPENT_SEQ = 9999


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def stamped_at(client: bytes, lamport: int) -> bytes:
    """One op frame from a doc authored under ``client``, its stamp moved to
    ``lamport``."""
    doc = Doc(client)
    emitted = []
    doc.on_update(lambda e: emitted.append(e.ops) if e.origin == "local" else None)
    doc.get_map("root").set("k", 1)
    (size,) = struct.unpack_from("<I", emitted[0], 0)
    frame = bytearray(emitted[0][: 4 + size])
    struct.pack_into("<Q", frame, _OP_SEQ, _UNSPENT_SEQ)
    struct.pack_into("<Q", frame, _LAMPORT, lamport)
    return bytes(frame)


def planted(client: bytes) -> bytes:
    """The plant that spends the space outright."""
    return stamped_at(client, _CEILING)


def nearly_spent(client: bytes) -> bytes:
    """A plant that leaves a handful of ids — enough for a single-id edit, not for
    a ten-codepoint run."""
    return stamped_at(client, _CEILING - 6)


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


def test_a_refused_call_still_publishes_what_it_emitted():
    # One handle call is one core transaction, and a refusal cuts it at the edit
    # that could not mint — so a refused call can carry ops that did. They are
    # applied to this replica already; withholding them would leave it ahead of
    # every peer.
    me = cid(3)
    doc = Doc(me)
    updates = []
    doc.on_update(lambda e: updates.append(e.ops) if e.origin == "local" else None)
    doc.apply_update(nearly_spent(me))

    # The text does not exist, so this emits a container-create the space still has
    # room for, then a ten-codepoint run it does not.
    with pytest.raises(MintExhausted):
        doc.get_text("t").insert(0, "abcdefghij")
    assert len(updates) == 1
    assert str(doc.get_text("t")) == ""


def test_an_inert_edit_is_not_reported_as_a_refusal():
    # An inert edit and a refused one both emit nothing, which is the whole reason
    # the query exists — so an edit that resolves to nothing must answer for itself
    # rather than inherit the previous edit's refusal.
    me = cid(4)
    doc = Doc(me)
    doc.get_text("t").insert(0, "ab")
    doc.apply_update(nearly_spent(me))

    with pytest.raises(MintExhausted):
        doc.get_text("t").insert(0, "abcdefghij")
    # An XML insert on a path that holds no XML node resolves to nothing.
    doc.get_xml("nope").insert_element(0, "p")
    # And the replica really did still have room.
    doc.get_text("t").insert(0, "z")
    assert str(doc.get_text("t")) == "zab"


def test_a_throwing_listener_does_not_take_the_refusals_place():
    # The refusal is the answer to the call the application made; a listener's own
    # failure must not replace it, or the edit reads as a listener bug and the write
    # that never happened is reported as one that did.
    me = cid(5)
    doc = Doc(me)
    doc.apply_update(nearly_spent(me))

    boom = RuntimeError("listener")

    def explode(event):
        if event.origin == "local":
            raise boom

    doc.on_update(explode)
    with pytest.raises(MintExhausted) as caught:
        doc.get_text("t").insert(0, "abcdefghij")
    assert caught.value.__cause__ is boom


def test_a_transactions_commit_delivery_does_not_take_the_refusals_place():
    # ``transact`` commits and delivers in its ``finally``, so a listener that raises
    # there sits between the refusal and the caller. The refusal still wins.
    me = cid(6)
    doc = Doc(me)
    doc.apply_update(nearly_spent(me))

    boom = RuntimeError("listener")

    def explode(event):
        if event.origin == "local":
            raise boom

    doc.on_update(explode)
    with pytest.raises(MintExhausted) as caught:
        doc.transact(lambda: doc.get_text("t").insert(0, "abcdefghij"))
    assert caught.value.__cause__ is boom
