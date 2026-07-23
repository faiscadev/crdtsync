"""The rich-content parity surface: CrdtXml (elements/children/tree-move), Text
cursors (relative positions), marks (author/read/change/delete), atomic
transactions, and blobs — all Pythonic, over the same backend."""

from crdtsync import BlobRef, CrdtXml, Doc


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


class TestCrdtXml:
    def test_installs_an_element_and_edits_children_by_index(self):
        root = Doc(cid(1)).get_xml("doc")
        root.element("doc")
        assert root.tag == "doc"

        root.insert_element(0, "p")
        root.insert_text(1, "hello")
        assert len(root) == 2

        root.delete_child(0)
        assert len(root) == 1

    def test_resolves_as_an_xml_handle_from_a_parent_map(self):
        doc = Doc(cid(1))
        doc.get_map("root").get_xml("body").fragment()
        body = doc.get_map("root").get("body")
        assert isinstance(body, CrdtXml)
        assert body.tag is None  # a fragment is tagless

    def test_tree_moves_a_child_between_parents(self):
        doc = Doc(cid(1))
        a = doc.get_xml("a")
        a.element("a").insert_element(0, "x").insert_element(1, "y")
        b = doc.get_xml("b")
        b.element("b")

        a.move(0, b, 0)  # move a's child 0 into b
        assert len(a) == 1
        assert len(b) == 1

    def test_converges_xml_between_two_docs(self):
        p, q = Doc(cid(1)), Doc(cid(2))
        p.on_update(lambda e: q.apply_update(e.ops) if e.origin == "local" else None)
        p.get_xml("doc").element("doc").insert_element(0, "p").insert_text(1, "hi")

        qd = q.get_xml("doc")
        assert qd.tag == "doc"
        assert len(qd) == 2


class TestCursors:
    def test_tracks_a_text_position_across_inserts_and_deletes(self):
        text = Doc(cid(1)).get_text("body")
        text.insert(0, "hello world")

        pos = text.relative_position(6)  # a cursor at "world"
        assert pos is not None
        assert text.resolve(pos) == 6

        text.insert(0, ">> ")  # insert before → cursor shifts right
        assert text.resolve(pos) == 9
        text.delete(0, 3)  # delete before → cursor shifts back
        assert text.resolve(pos) == 6

    def test_a_list_cursor_round_trips(self):
        xs = Doc(cid(1)).get_list("xs")
        xs.append("a").append("b").append("c")
        pos = xs.relative_position(2)
        assert pos is not None
        assert xs.resolve(pos) == 2
        xs.insert(0, "z")
        assert xs.resolve(pos) == 3

    def test_survives_a_concurrent_remote_edit(self):
        a, b = Doc(cid(1)), Doc(cid(2))
        a.on_update(lambda e: b.apply_update(e.ops) if e.origin == "local" else None)
        b.on_update(lambda e: a.apply_update(e.ops) if e.origin == "local" else None)

        a.get_text("t").insert(0, "abcdef")
        cursor = a.get_text("t").relative_position(4)  # before "e"
        assert a.get_text("t").resolve(cursor) == 4

        b.get_text("t").insert(0, "XYZ")  # concurrent front insert
        assert a.get_text("t").resolve(cursor) == 7
        assert str(a.get_text("t")) == "XYZabcdef"


class TestMarks:
    def test_authors_a_mark_and_reads_it_back_by_name(self):
        text = Doc(cid(1)).get_text("body")
        text.insert(0, "hello world")

        mark_id = text.mark(0, 5, "comment", "note-1")
        assert mark_id is not None

        assert "comment" in [m["name"] for m in text.marks_at(0)]
        assert text.marks_at(8) == []  # outside the range

    def test_removes_a_mark_by_handle(self):
        text = Doc(cid(1)).get_text("body")
        text.insert(0, "abcdef")
        mark_id = text.mark(0, 6, "comment", "x")
        assert "comment" in [m["name"] for m in text.marks_at(2)]

        text.delete_mark(mark_id)
        assert text.marks_at(2) == []

    def test_syncs_a_mark_to_a_peer(self):
        a, b = Doc(cid(1)), Doc(cid(2))
        a.on_update(lambda e: b.apply_update(e.ops) if e.origin == "local" else None)
        a.get_text("t").insert(0, "hello")
        mark_id = a.get_text("t").mark(0, 5, "comment", "hi")
        assert mark_id is not None
        assert "comment" in [m["name"] for m in b.get_text("t").marks_at(1)]

    def test_reports_a_mark_change_through_reactivity(self):
        doc = Doc(cid(1))
        text = doc.get_text("t")
        text.insert(0, "hello")
        kinds = []
        doc.on_update(lambda e: kinds.extend(c["kind"] for c in e.changes))
        text.mark(0, 5, "bold", True)
        assert "mark" in kinds

    def test_value_flavor_mark_reads_a_native_value(self):
        schema = b"""{
            "schema": "notes", "version": 1, "root": "Doc",
            "types": { "Doc": { "kind": "map", "children": { "t": "Body" } },
                       "Body": { "kind": "text" } },
            "marks": { "color": { "flavor": "value" } }
        }"""
        doc = Doc(cid(1))
        text = doc.get_text("t")
        text.insert(0, "hello")
        assert doc.set_schema(schema)
        text.mark(0, 5, "color", "red")
        color = next(m for m in text.marks_at(2) if m["name"] == "color")
        assert color["value"] == "red"


class TestAtomicTransactions:
    def test_groups_edits_into_a_single_update(self):
        doc = Doc(cid(1))
        m = doc.get_map("root")
        m.set("init", 0)

        events = []
        doc.on_update(events.append)
        with_txn = doc.transact(lambda: (m.set("a", 1), m.set("b", 2), m.set("c", 3)))
        assert with_txn is None

        assert len(events) == 1  # one batched update, not three
        assert events[0].origin == "local"
        assert m.get("a") == 1
        assert m.get("c") == 3

    def test_applies_the_whole_batch_atomically_on_a_peer(self):
        a, b = Doc(cid(1)), Doc(cid(2))
        a.on_update(lambda e: b.apply_update(e.ops) if e.origin == "local" else None)
        a.get_map("root").set("init", 0)

        def edits():
            a.get_map("root").set("x", 1)
            a.get_map("root").set("y", 2)
            a.get_list("log").append("entry")

        a.transact(edits)
        assert b.get_map("root").get("x") == 1
        assert b.get_map("root").get("y") == 2
        assert list(b.get_list("log")) == ["entry"]

    def test_flattens_a_nested_transaction(self):
        doc = Doc(cid(1))
        m = doc.get_map("root")
        m.set("init", 0)
        events = []
        doc.on_update(events.append)

        def outer():
            m.set("a", 1)
            doc.transact(lambda: m.set("b", 2))
            m.set("c", 3)

        doc.transact(outer)
        assert len(events) == 1
        assert m.get("b") == 2


class TestBlobs:
    def test_stores_and_reads_an_inline_blob(self):
        m = Doc(cid(1)).get_map("root")
        data = bytes([1, 2, 3, 4])
        assert m.set_blob("avatar", "image/png", data) is True

        ref = m.get_blob("avatar")
        assert isinstance(ref, BlobRef)
        assert ref.mime == "image/png"
        assert ref.size == 4
        assert ref.inline == data
        assert len(ref.id) == 16

    def test_generic_get_returns_the_blob_ref(self):
        m = Doc(cid(1)).get_map("root")
        m.set_blob("file", "text/plain", bytes([9]))
        value = m.get("file")
        assert isinstance(value, BlobRef)
        assert value.mime == "text/plain"
        assert value.inline == bytes([9])

    def test_sets_a_store_backed_blob_ref(self):
        m = Doc(cid(1)).get_map("root")
        blob_id = bytes([7] * 16)
        m.set_blob_ref("big", blob_id, "video/mp4", 1_000_000)
        ref = m.get_blob("big")
        assert ref.mime == "video/mp4"
        assert ref.size == 1_000_000
        assert ref.inline is None  # store-backed, not inline
        assert ref.id == blob_id

    def test_over_ceiling_blob_is_not_inlined(self):
        m = Doc(cid(1)).get_map("root")
        assert m.set_blob("huge", "application/octet-stream", b"\x00" * (4096 + 1)) is False
        assert m.get_blob("huge") is None

    def test_converges_a_blob_between_two_docs(self):
        a, b = Doc(cid(1)), Doc(cid(2))
        a.on_update(lambda e: b.apply_update(e.ops) if e.origin == "local" else None)
        a.get_map("root").set_blob("pic", "image/gif", bytes([1, 2, 3]))
        ref = b.get_map("root").get_blob("pic")
        assert ref.inline == bytes([1, 2, 3])
