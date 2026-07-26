from crdtsync import Document, Undo


def cid(first: int) -> bytes:
    return bytes([first]) + b"\x00" * 15


def test_undo_and_redo_a_register():
    with Document(cid(1)) as d, Undo() as u:
        u.track(d)
        d.register_int([b"title"], 1)
        d.register_int([b"title"], 2)
        assert d.get_int([b"title"]) == 2
        assert u.can_undo(d)

        u.undo(d)
        assert d.get_int([b"title"]) == 1
        u.redo(d)
        assert d.get_int([b"title"]) == 2
        assert not u.can_redo(d)


def test_an_untracked_document_records_nothing():
    with Document(cid(1)) as d, Undo() as u:
        d.register_int([b"title"], 1)
        assert not u.can_undo(d)
        assert u.undo(d) == b""
        assert d.get_int([b"title"]) == 1


def test_undo_of_a_counter():
    with Document(cid(1)) as d, Undo() as u:
        u.track(d)
        d.inc([b"votes"], 5)
        d.dec([b"votes"], 2)
        assert d.get_counter([b"votes"]) == 3
        u.undo(d)
        assert d.get_counter([b"votes"]) == 5
        # The first delta installed the counter, so undoing it empties the slot.
        u.undo(d)
        assert d.get_counter([b"votes"]) is None


def test_undo_of_a_list_insert():
    with Document(cid(1)) as d, Undo() as u:
        u.track(d)
        d.list_insert([b"items"], 0, b"a")
        assert d.list_len([b"items"]) == 1
        u.undo(d)
        assert d.list_len([b"items"]) is None


def test_undo_of_a_text_edit():
    with Document(cid(1)) as d, Undo() as u:
        u.track(d)
        d.text_insert([b"body"], 0, "hi")
        assert d.text_get([b"body"]) == "hi"
        d.text_insert([b"body"], 2, "!")
        assert d.text_get([b"body"]) == "hi!"
        u.undo(d)
        assert d.text_get([b"body"]) == "hi"
        # The first insert created the slot, so its undo takes the slot with it.
        u.undo(d)
        assert d.text_get([b"body"]) is None


def test_an_explicit_intention_undoes_as_one_step():
    with Document(cid(1)) as d, Undo() as u:
        u.track(d)
        u.begin_intention(d)
        d.register_int([b"a"], 1)
        d.register_int([b"b"], 2)
        u.end_intention(d)

        u.undo(d)
        assert d.get_int([b"a"]) is None
        assert d.get_int([b"b"]) is None
        assert not u.can_undo(d)


def test_two_origins_keep_separate_histories():
    with Document(cid(1)) as d, Undo(b"mine") as mine, Undo(b"theirs") as theirs:
        mine.track(d)
        d.register_int([b"mine"], 1)
        theirs.track(d)
        d.register_int([b"theirs"], 2)

        mine.undo(d)
        assert d.get_int([b"mine"]) is None
        assert d.get_int([b"theirs"]) == 2


def test_an_undo_converges_on_a_peer():
    with Document(cid(1)) as a, Document(cid(2)) as b, Undo() as u:
        u.track(a)
        b.apply(a.register_int([b"n"], 1))
        b.apply(a.register_int([b"n"], 2))
        assert b.get_int([b"n"]) == 2
        b.apply(u.undo(a))
        assert b.get_int([b"n"]) == 1
