"""The Python SDK authors named marks over a sequence, reads the marks active on a
character, and changes or deletes a mark by its returned id."""

from crdtsync import Document, Side

# A schema declaring `color` as a value-flavored mark, so a covered character
# resolves to the last-written scalar payload (flavor 1) rather than the id set.
VALUE_MARK_SCHEMA = b"""{
    "schema": "notes", "version": 1, "root": "Doc",
    "types": { "Doc": { "kind": "map" } },
    "marks": { "color": { "flavor": "value" } }
}"""


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def bold(doc: Document, seq):
    """Author a `bold` mark over [0, 5) of the sequence at `seq`; return (id, ops)."""
    return doc.mark(seq, 0, Side.RIGHT, 5, Side.LEFT, b"bold", None)


def test_a_mark_is_reported_at_a_covered_index():
    with Document(cid(1)) as a:
        a.text_insert([b"body"], 0, "hello world")
        mark_id, _ = bold(a, [b"body"])
        assert mark_id is not None and len(mark_id) == 16

        marks = a.marks_at([b"body"], 2)
        m = next(m for m in marks if m["name"] == b"bold")
        # An undeclared name resolves to the object flavor: the covering ids.
        assert m["flavor"] == "object"
        assert mark_id in m["ids"]


def test_deleting_a_mark_clears_it():
    with Document(cid(1)) as a:
        a.text_insert([b"body"], 0, "hello")
        mark_id, _ = bold(a, [b"body"])
        assert a.marks_at([b"body"], 2)
        assert len(a.mark_delete(mark_id)) > 0
        assert a.marks_at([b"body"], 2) == []


def test_a_value_flavored_mark_reports_its_scalar():
    with Document(cid(1)) as a:
        assert a.set_schema(VALUE_MARK_SCHEMA)
        a.text_insert([b"body"], 0, "hello")
        a.mark([b"body"], 0, Side.RIGHT, 5, Side.LEFT, b"color", 7)
        marks = a.marks_at([b"body"], 2)
        m = next(m for m in marks if m["name"] == b"color")
        assert m["flavor"] == "value"
        assert m["value"] == {"t": "int", "v": 7}


def test_set_value_emits_ops():
    with Document(cid(1)) as a:
        a.text_insert([b"body"], 0, "hello")
        mark_id, _ = a.mark([b"body"], 0, Side.RIGHT, 5, Side.LEFT, b"color", b"red")
        assert len(a.mark_set_value(mark_id, b"blue")) > 0


def test_a_non_sequence_path_authors_nothing():
    with Document(cid(1)) as a:
        a.register_int([b"age"], 30)
        mark_id, ops = a.mark([b"age"], 0, Side.LEFT, 1, Side.LEFT, b"bold", None)
        assert mark_id is None
        assert ops == b""


def test_marks_converge_across_documents():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        text_ops = a.text_insert([b"body"], 0, "hello world")
        _, mark_ops = bold(a, [b"body"])
        assert b.apply(text_ops) >= 0
        assert b.apply(mark_ops) >= 0
        marks = b.marks_at([b"body"], 2)
        assert any(m["name"] == b"bold" for m in marks)


def test_client_mark_surface_marshals():
    from crdtsync import Client

    with Client(cid(1)) as a:
        ca, _ = a.subscribe(b"room-1")
        # A client room has no top-level sequence to annotate, so the author is
        # inert: no id (an empty out buffer). The surface still marshals the call,
        # returning a wire frame envelope.
        mark_id, frame = a.mark(ca, [b"body"], 0, Side.RIGHT, 5, Side.LEFT, b"bold", None)
        assert mark_id is None
        assert isinstance(frame, bytes)
        # set-value / delete against an unknown handle marshal to a frame envelope.
        assert isinstance(a.mark_set_value(ca, b"\x00" * 16, b"red"), bytes)
        assert isinstance(a.mark_delete(ca, b"\x00" * 16), bytes)
