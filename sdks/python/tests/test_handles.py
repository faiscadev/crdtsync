"""The ergonomic handle-graph API: a ``Doc`` yields live typed handles
(``CrdtMap``/``CrdtList``/``CrdtText``) addressed by ergonomic keys, native values
marshal to scalars over the explicit leaf/container boundary, and two docs
converge by exchanging update ops."""

import pytest

from crdtsync import CrdtList, CrdtMap, CrdtText, Doc


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


class TestCrdtMap:
    def test_round_trips_native_scalars(self):
        m = Doc(cid(1)).get_map("root")
        m.set("s", "hello")
        m.set("n", 42)
        m.set("big", 9007199254740993)
        m.set("b", True)
        m.set("nil", None)
        m.set("bin", bytes([1, 2, 3]))

        assert m.get("s") == "hello"
        assert m.get("n") == 42
        assert m.get("big") == 9007199254740993
        assert m.get("b") is True
        assert m.get("nil") is None
        assert m.get("bin") == bytes([1, 2, 3])

    def test_string_and_lookalike_bytes_stay_distinct(self):
        m = Doc(cid(1)).get_map("root")
        m.set("str", "AB")
        m.set("bin", bytes([0x41, 0x42]))  # same bytes as "AB"
        assert m.get("str") == "AB"
        assert m.get("bin") == bytes([0x41, 0x42])

    def test_rejects_container_seed_and_non_integer(self):
        m = Doc(cid(1)).get_map("root")
        with pytest.raises(TypeError):
            m.set("o", {"a": 1})
        with pytest.raises(TypeError):
            m.set("l", [1, 2])
        with pytest.raises(TypeError):
            m.set("f", 1.5)

    def test_i64_overflow_raises_overflowerror(self):
        m = Doc(cid(1)).get_map("root")
        with pytest.raises(OverflowError):
            m.set("over", 2**63)
        with pytest.raises(OverflowError):
            m.set("under", -(2**63) - 1)
        # The boundary values themselves are storable.
        m.set("max", 2**63 - 1)
        m.set("min", -(2**63))
        assert m.get("max") == 2**63 - 1
        assert m.get("min") == -(2**63)

    def test_binary_key_value_is_not_lost(self):
        m = Doc(cid(1)).get_map("root")
        key = bytes([0xFF, 0xFE])
        m.set(key, "kept")
        assert m.get(key) == "kept"
        assert "kept" in [v for _, v in m.items()]
        assert len(m) == 1

    def test_contains_delete_keys_items_len(self):
        m = Doc(cid(1)).get_map("root")
        m.set("a", 1).set("b", 2)
        assert len(m) == 2
        assert "a" in m
        assert "z" not in m
        assert sorted(m.keys()) == ["a", "b"]
        assert dict(m.items())["b"] == 2

        m.delete("a")
        assert "a" not in m
        assert len(m) == 1

    def test_composes_nested_map_handles(self):
        root = Doc(cid(1)).get_map("root")
        root.get_map("child").set("k", "v")
        child = root.get("child")
        assert isinstance(child, CrdtMap)
        assert child.get("k") == "v"

    def test_is_iterable_over_keys(self):
        m = Doc(cid(1)).get_map("root")
        m.set("x", 1).set("y", 2)
        assert sorted(m) == ["x", "y"]

    def test_missing_key_is_none(self):
        m = Doc(cid(1)).get_map("root")
        assert m.get("nope") is None


class TestCrdtList:
    def test_insert_append_read_delete(self):
        xs = Doc(cid(1)).get_list("items")
        xs.append("a").append("b").insert(1, "c")
        assert len(xs) == 3
        assert list(xs) == ["a", "c", "b"]
        xs.delete(0)
        assert list(xs) == ["c", "b"]

    def test_getitem_and_bounds(self):
        xs = Doc(cid(1)).get_list("items")
        xs.append(7).append(8)
        assert xs[0] == 7
        assert xs[-1] == 8
        with pytest.raises(IndexError):
            _ = xs[5]

    def test_resolves_as_handle_from_a_parent_map(self):
        root = Doc(cid(1)).get_map("root")
        root.get_list("xs").append(7)
        xs = root.get("xs")
        assert isinstance(xs, CrdtList)
        assert xs[0] == 7


class TestCrdtText:
    def test_edits_by_codepoint_index(self):
        t = Doc(cid(1)).get_text("body")
        t.insert(0, "hello world")
        t.delete(5, 6)
        t.insert(5, "!")
        assert str(t) == "hello!"
        assert len(t) == 6

    def test_resolves_as_handle_from_a_parent_map(self):
        root = Doc(cid(1)).get_map("root")
        root.get_text("t").insert(0, "hi")
        assert isinstance(root.get("t"), CrdtText)
        assert str(root.get("t")) == "hi"


class TestConvergence:
    def test_two_docs_converge_sequentially(self):
        a, b = Doc(cid(1)), Doc(cid(2))
        a.on_update(lambda e: b.apply_update(e.ops) if e.origin == "local" else None)
        a.get_map("root").set("k", 1)
        a.get_map("root").set("k", 2)
        assert b.get_map("root").get("k") == 2

    def test_two_docs_converge_concurrently(self):
        a, b = Doc(cid(1)), Doc(cid(2))
        # Concurrent edits to distinct keys both survive the merge; exchange the
        # raw ops each edit produced.
        events_a, events_b = [], []
        a.on_update(lambda e: events_a.append(e.ops) if e.origin == "local" else None)
        b.on_update(lambda e: events_b.append(e.ops) if e.origin == "local" else None)
        a.get_map("root").set("from_a", "x")
        b.get_map("root").set("from_b", "y")
        for ops in events_a:
            b.apply_update(ops)
        for ops in events_b:
            a.apply_update(ops)
        assert a.get_map("root").get("from_a") == "x"
        assert a.get_map("root").get("from_b") == "y"
        assert b.get_map("root").get("from_a") == "x"
        assert b.get_map("root").get("from_b") == "y"

    def test_snapshot_round_trips_the_handle_graph(self):
        a = Doc(cid(1))
        a.get_map("root").set("k", 7)
        a.get_map("root").get_text("t").insert(0, "hi")
        b = Doc.decode_state(a.encode_state())
        assert b.get_map("root").get("k") == 7
        assert str(b.get_map("root").get_text("t")) == "hi"
