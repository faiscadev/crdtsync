"""The Python SDK binds a schema by bytes and drains the repair signal: an
out-of-range write reports its located path once, then settles."""

from crdtsync import Document

SCHEMA = b"""{
    "schema": "notes", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "title": "Title" } },
        "Title": { "kind": "register", "min": 0, "max": 280 }
    }
}"""


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def test_a_valid_schema_binds():
    with Document(cid(1)) as a:
        assert a.set_schema(SCHEMA) is True


def test_malformed_schema_bytes_are_rejected():
    with Document(cid(1)) as a:
        assert a.set_schema(b"not json {") is False
        assert a.set_schema(b'{ "schema": "x" }') is False
        assert a.set_schema(b"\xff\xfe\x00") is False


def test_no_schema_bound_reports_no_repairs():
    with Document(cid(1)) as a:
        a.register_int([b"title"], 999)
        assert a.take_repairs() == []


def test_an_out_of_range_write_reports_its_path_once():
    with Document(cid(1)) as a:
        assert a.set_schema(SCHEMA)
        # A conforming edit reports nothing.
        a.register_int([b"title"], 42)
        assert a.take_repairs() == []
        # An out-of-range write reports its located path once, as decoded steps.
        a.register_int([b"title"], 999)
        assert a.take_repairs() == [[{"key": b"title"}]]
        # The settle-point contract: a second drain is empty.
        assert a.take_repairs() == []
