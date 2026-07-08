"""The Python SDK diffs two snapshots into a list of structural change dicts,
mirroring the wasm change shape."""

import pytest

from crdtsync import Document, Side, diff, diff_decode, encode_path


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


def test_diff_decode_round_trips_a_change_list_buffer():
    from crdtsync import _diff_raw

    with Document(cid(1)) as a:
        a.register_int([b"age"], 30)
        old = a.encode_state()
        a.register_int([b"age"], 31)
        new = a.encode_state()

    encoded = _diff_raw(old, new)
    assert diff_decode(encoded) == diff(old, new)


def test_diff_decode_carries_an_xml_attr_and_a_mark_change():
    from crdtsync import _diff_raw

    with Document(cid(1)) as a:
        a.xml_element([b"doc"], b"section")
        a.set_bytes([b"doc", b"class"], b"a")
        a.text_insert([b"body"], 0, "hello world")
        old = a.encode_state()
        a.set_bytes([b"doc", b"class"], b"b")
        a.mark([b"body"], 0, Side.RIGHT, 5, Side.LEFT, b"bold", True)
        new = a.encode_state()

    changes = diff_decode(_diff_raw(old, new))
    # The attr change surfaces as a value at its path; the mark as markAdded.
    assert any(
        c["op"] == "value" and c["path"] == encode_path([b"doc", b"class"])
        for c in changes
    )
    assert any(c["op"] == "markAdded" and c["name"] == b"bold" for c in changes)


def test_diff_decode_of_an_empty_change_list_is_empty():
    assert diff_decode(b"\x00\x00\x00\x00") == []


def test_diff_decode_rejects_a_malformed_buffer():
    with pytest.raises(ValueError):
        diff_decode(b"\xff\xff\xff\xff")
