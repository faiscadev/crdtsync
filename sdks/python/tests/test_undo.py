import crdtsync
from crdtsync import Document, Undo


def cid(first: int) -> bytes:
    return bytes([first]) + b"\x00" * 15


def test_undo_and_redo_a_register():
    with Document(cid(1)) as d, Undo() as u:
        u.register_int(d, [b"title"], 1)
        u.register_int(d, [b"title"], 2)
        assert d.get_int([b"title"]) == 2
        assert u.can_undo()

        u.undo(d)
        assert d.get_int([b"title"]) == 1
        u.redo(d)
        assert d.get_int([b"title"]) == 2
        assert not u.can_redo()


def test_undo_of_a_counter():
    with Document(cid(1)) as d, Undo() as u:
        u.inc(d, [b"votes"], 5)
        u.dec(d, [b"votes"], 2)
        assert d.get_counter([b"votes"]) == 3
        u.undo(d)
        assert d.get_counter([b"votes"]) == 5


def test_undo_of_a_list_insert():
    with Document(cid(1)) as d, Undo() as u:
        u.list_insert(d, [b"items"], 0, b"a")
        assert d.list_len([b"items"]) == 1
        u.undo(d)
        assert d.list_len([b"items"]) == 0


def test_undo_of_a_text_edit():
    with Document(cid(1)) as d, Undo() as u:
        u.text_insert(d, [b"body"], 0, "hi")
        assert d.text_get([b"body"]) == "hi"
        u.undo(d)
        assert d.text_get([b"body"]) == ""


def test_an_undo_converges_on_a_peer():
    with Document(cid(1)) as a, Document(cid(2)) as b, Undo() as u:
        b.apply(u.register_int(a, [b"n"], 1))
        b.apply(u.register_int(a, [b"n"], 2))
        assert b.get_int([b"n"]) == 2
        b.apply(u.undo(a))
        assert b.get_int([b"n"]) == 1
