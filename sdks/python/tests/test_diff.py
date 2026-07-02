"""The Python SDK diffs two snapshots into a list of structural change dicts,
mirroring the wasm change shape."""

import pytest

from crdtsync import Document, diff, encode_path


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def test_value_change_reports_old_and_new():
    with Document(cid(1)) as a:
        a.register_int([b"age"], 30)
        old = a.encode_state()
        a.register_int([b"age"], 31)
        new = a.encode_state()

    changes = diff(old, new)
    assert changes == [
        {
            "op": "value",
            "path": encode_path([b"age"]),
            "old": {"t": "int", "v": 30},
            "new": {"t": "int", "v": 31},
        }
    ]


def test_added_and_counter_changes():
    with Document(cid(1)) as a:
        a.inc([b"hits"], 3)
        old = a.encode_state()
        a.inc([b"hits"], 2)
        a.register_int([b"age"], 9)
        new = a.encode_state()

    changes = diff(old, new)
    by_op = {c["op"]: c for c in changes}
    assert by_op["counter"] == {
        "op": "counter",
        "path": encode_path([b"hits"]),
        "old": 3,
        "new": 5,
    }
    assert by_op["add"]["kind"] == "register"
    assert by_op["add"]["path"] == encode_path([b"age"])


def test_text_and_list_runs():
    with Document(cid(1)) as a:
        a.text_insert([b"body"], 0, "hi")
        a.list_insert([b"xs"], 0, b"\x00")
        old = a.encode_state()
        a.text_insert([b"body"], 2, "!")
        a.list_insert([b"xs"], 1, b"\x01")
        new = a.encode_state()

    by_op = {c["op"]: c for c in diff(old, new)}
    assert by_op["textInsert"]["text"] == "!"
    assert by_op["textInsert"]["index"] == 2
    assert by_op["listInsert"]["index"] == 1
    assert len(by_op["listInsert"]["items"]) == 1
    assert "scalar" in by_op["listInsert"]["items"][0]


def test_identical_snapshots_have_no_changes():
    with Document(cid(1)) as a:
        a.register_int([b"age"], 30)
        state = a.encode_state()
    assert diff(state, state) == []


def test_malformed_snapshot_raises():
    with pytest.raises(ValueError):
        diff(b"\xff\xff\xff\xff", b"\xff\xff\xff\xff")
