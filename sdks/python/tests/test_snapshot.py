"""The Python SDK can serialize a replica to a snapshot and rebuild one from it,
so a client served a snapshot can reconstruct its document."""

import pytest

from crdtsync import Document


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def test_snapshot_round_trips():
    with Document(cid(1)) as a:
        a.register_int([b"age"], 30)
        a.inc([b"hits"], 5)
        back = Document.decode_state(a.encode_state())
        try:
            assert back.get_int([b"age"]) == 30
            assert back.get_counter([b"hits"]) == 5
        finally:
            back.close()


def test_decoded_document_dedups_and_converges():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        reg = a.register_int([b"age"], 30)
        back = Document.decode_state(a.encode_state())
        try:
            # A replay of the covered op is a no-op; a later peer op still lands.
            assert back.apply(reg) == 0
            b.apply(reg)
            hit = b.inc([b"hits"], 4)
            assert back.apply(hit) == 1
            assert back.get_counter([b"hits"]) == 4
        finally:
            back.close()


def test_decode_garbage_state_raises():
    with pytest.raises(ValueError):
        Document.decode_state(b"\xff\xff\xff\xff")


def test_encode_state_is_canonical():
    with Document(cid(1)) as a:
        a.register_int([b"age"], 30)
        snapshot = a.encode_state()
        back = Document.decode_state(snapshot)
        try:
            assert back.encode_state() == snapshot
        finally:
            back.close()
