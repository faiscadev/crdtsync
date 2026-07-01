"""The Python SDK drives the core over the C ABI: local edits read back, and two
documents that exchange ops converge."""

import crdtsync
from crdtsync import Document


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def test_register_reads_back_and_converges():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        ops = a.register_int([b"age"], 30)
        assert a.get_int([b"age"]) == 30
        assert b.apply(ops) == 1
        assert b.get_int([b"age"]) == 30


def test_missing_key_is_none():
    with Document(cid(1)) as a:
        assert a.get_int([b"nope"]) is None


def test_counter_accumulates_across_replicas():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        oa = a.inc([b"n"], 3)
        ob = b.inc([b"n"], 4)
        b.apply(oa)
        a.apply(ob)
        assert a.get_counter([b"n"]) == 7
        assert b.get_counter([b"n"]) == 7


def test_nested_path_converges():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        path = [b"profile", b"stats", b"score"]
        b.apply(a.register_int(path, 7))
        assert b.get_int(path) == 7


def test_bytes_round_trip():
    with Document(cid(1)) as a:
        a.set_bytes([b"blob"], b"\x00\x01\xff\x00")
        assert a.get_bytes([b"blob"]) == b"\x00\x01\xff\x00"


def test_delete_converges():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        b.apply(a.register_int([b"k"], 5))
        b.apply(a.delete([b"k"]))
        assert b.get_int([b"k"]) is None


def test_list_converges_and_no_op_delete_is_inert():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        p = [b"board", b"cards"]
        b.apply(a.list_insert(p, 0, b"x"))
        b.apply(a.list_insert(p, 1, b"y"))
        assert b.list_len(p) == 2
        assert b.list_get(p, 0) == b"x"
        assert b.list_get(p, 1) == b"y"
        # a delete of an absent list is a no-op: no ops, no container created.
        assert a.list_delete([b"ghost"], 0) == b""
        assert a.list_len([b"ghost"]) is None


def test_text_converges_and_rejects_bad_utf8():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        p = [b"doc", b"title"]
        b.apply(a.text_insert(p, 0, "héllo"))
        assert b.text_len(p) == 5
        assert b.text_get(p) == "héllo"
        b.apply(a.text_delete(p, 1, 3))
        assert b.text_get(p) == "ho"


def test_apply_rejects_garbage():
    with Document(cid(1)) as a:
        assert a.apply(b"\xff\xff\xff\xff\xff\xff\xff\xff") == -1


def test_encode_path_shape():
    assert crdtsync.encode_path([b"ab", b"c"]) == b"\x02\x00\x00\x00ab\x01\x00\x00\x00c"
