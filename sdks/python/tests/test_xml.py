"""The Python SDK installs XmlElements/XmlFragments, edits their children, moves
a child between parents, and reads a node's tag and child count."""

from crdtsync import Client, Document


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def test_element_tag_round_trips():
    with Document(cid(1)) as a:
        a.xml_element([b"doc"], b"section")
        assert a.xml_tag([b"doc"]) == b"section"


def test_fragment_is_tagless():
    with Document(cid(1)) as a:
        a.xml_fragment([b"frag"])
        assert a.xml_tag([b"frag"]) is None
        assert a.xml_children_len([b"frag"]) == 0


def test_insert_children_and_count():
    with Document(cid(1)) as a:
        a.xml_element([b"doc"], b"section")
        a.xml_insert_element([b"doc"], 0, b"p")
        a.xml_insert_text([b"doc"], 1, "hello")
        assert a.xml_children_len([b"doc"]) == 2
        a.xml_child_delete([b"doc"], 0)
        assert a.xml_children_len([b"doc"]) == 1


def test_move_relocates_a_child_between_parents():
    with Document(cid(1)) as a:
        a.xml_fragment([b"a"])
        a.xml_fragment([b"b"])
        a.xml_insert_element([b"a"], 0, b"p")
        assert a.xml_children_len([b"a"]) == 1
        assert a.xml_children_len([b"b"]) == 0
        a.xml_move([b"a"], 0, [b"b"], 0)
        assert a.xml_children_len([b"a"]) == 0
        assert a.xml_children_len([b"b"]) == 1


def test_reads_on_a_non_xml_path_are_absent():
    with Document(cid(1)) as a:
        a.register_int([b"age"], 30)
        assert a.xml_tag([b"age"]) is None
        assert a.xml_children_len([b"age"]) is None


def test_edits_converge_across_documents():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        ops_elem = a.xml_element([b"doc"], b"section")
        ops_child = a.xml_insert_element([b"doc"], 0, b"p")
        assert b.apply(ops_elem) >= 0
        assert b.apply(ops_child) >= 0
        assert b.xml_tag([b"doc"]) == b"section"
        assert b.xml_children_len([b"doc"]) == 1


def test_client_xml_edit_frames_and_travels_to_a_peer():
    with Client(cid(1)) as a, Client(cid(2)) as b:
        ca, _ = a.subscribe(b"room-1")
        cb, _ = b.subscribe(b"room-1")
        frame = a.xml_element(ca, [b"doc"], b"section")
        assert len(frame) > 0
        assert b.receive(frame) == 1
        child = a.xml_insert_element(ca, [b"doc"], 0, b"p")
        assert len(child) > 0
        assert b.receive(child) == 1
