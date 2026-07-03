"""The Python SDK captures and resolves stable list/text positions (anchors)."""

from crdtsync import Document, Side


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def test_a_list_position_tracks_an_insert_before_it():
    with Document(cid(1)) as a:
        path = [b"board", b"cards"]
        a.list_insert(path, 0, b"a")
        a.list_insert(path, 1, b"b")
        a.list_insert(path, 2, b"c")
        pos = a.relative_position(path, 2, Side.LEFT)
        assert pos is not None
        assert a.resolve_position(path, pos) == 2
        # Insert ahead of the anchor; it slides to keep the same gap.
        a.list_insert(path, 0, b"z")
        assert a.resolve_position(path, pos) == 3


def test_a_text_position_round_trips():
    with Document(cid(1)) as a:
        path = [b"doc", b"title"]
        a.text_insert(path, 0, "hello")
        pos = a.relative_position(path, 5, Side.LEFT)
        assert pos is not None
        assert a.resolve_position(path, pos) == 5
        a.text_insert(path, 0, ">>")
        assert a.resolve_position(path, pos) == 7


def test_a_non_sequence_or_malformed_position_is_none():
    with Document(cid(1)) as a:
        a.register_int([b"age"], 30)
        assert a.relative_position([b"age"], 0) is None
        a.list_insert([b"list"], 0, b"x")
        pos = a.relative_position([b"list"], 0)
        assert pos is not None
        assert a.resolve_position([b"age"], pos) is None
        assert a.resolve_position([b"list"], b"\xff\xff") is None
        # An in-range but unknown side (not LEFT/RIGHT) is absent, not a wrap.
        assert a.relative_position([b"list"], 0, 5) is None
