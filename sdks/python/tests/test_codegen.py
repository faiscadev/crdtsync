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


def _load_module(filename, module_name):
    path = os.path.join(_FIXTURES, filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_generated():
    return _load_module("note_generated.py", "note_generated")


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


def test_colliding_slots_import_and_stay_independent():
    # `a-b`, `a_b`, `a b` all sanitize to `a_b`; the disambiguated module must
    # import (no duplicate methods) and each accessor forward to its own field.
    gen = _load_module("collide_generated.py", "collide_generated")
    with Document(cid(1)) as doc:
        c = gen.bind(doc)
        c.inc_a_b(1)
        c.inc_a_b_2(2)
        c.inc_a_b_3(3)
        # each accessor round-trips to its own distinct field, unaffected by the others
        assert c.get_a_b() == 1
        assert c.get_a_b_2() == 2
        assert c.get_a_b_3() == 3
        # and forwards to the exact byte key the schema declared
        assert doc.get_counter([b"a-b"]) == 1
        assert doc.get_counter([b"a_b"]) == 2
        assert doc.get_counter([b"a b"]) == 3
