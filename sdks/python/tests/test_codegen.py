"""The typed accessors ``crdtsync-codegen`` emits for a schema import cleanly and
forward to the same path surface as a raw call.

The generated module is the committed golden the Rust codegen tests pin
(``crates/codegen/tests/fixtures/note_generated.py``); here it drives a real
``Document`` and asserts a value set through a generated setter reads back through
both the generated getter and the equivalent raw path call — the round-trip the
codegen exists to make type-safe."""

import importlib.util
import os

from crdtsync import Document

_FIXTURES = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "..",
    "crates",
    "codegen",
    "tests",
    "fixtures",
)


def _load_generated():
    path = os.path.join(_FIXTURES, "note_generated.py")
    spec = importlib.util.spec_from_file_location("note_generated", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def test_generated_module_imports():
    gen = _load_generated()
    assert hasattr(gen, "Note")
    assert hasattr(gen, "Meta")
    assert hasattr(gen, "bind")


def test_text_slot_round_trips_through_the_generated_accessors():
    gen = _load_generated()
    with Document(cid(1)) as doc:
        note = gen.bind(doc)
        note.insert_title(0, "hello")
        assert note.get_title() == "hello"
        assert note.len_title() == 5
        # forwards to the same raw path the accessor bakes in
        assert doc.text_get([b"title"]) == "hello"


def test_counter_slot_round_trips():
    gen = _load_generated()
    with Document(cid(1)) as doc:
        note = gen.bind(doc)
        note.inc_views(3)
        note.dec_views(1)
        assert note.get_views() == 2
        assert doc.get_counter([b"views"]) == 2


def test_list_slot_round_trips():
    gen = _load_generated()
    with Document(cid(1)) as doc:
        note = gen.bind(doc)
        note.insert_tags(0, b"draft")
        note.insert_tags(1, b"urgent")
        assert note.len_tags() == 2
        assert note.get_tags(0) == b"draft"
        assert note.get_tags(1) == b"urgent"


def test_nested_map_slot_forwards_to_the_nested_path():
    gen = _load_generated()
    with Document(cid(1)) as doc:
        note = gen.bind(doc)
        note.meta().set_priority(4)
        assert note.meta().get_priority() == 4
        # the nested wrapper addresses the extended path
        assert doc.get_int([b"meta", b"priority"]) == 4
