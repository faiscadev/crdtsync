"""Reactivity: doc-level and per-handle observation deliver diff-derived change
events (kind / ergonomic target / native old-new / local-vs-remote origin), and
the schema-repair signal fires on located repairs. Snapshot+diff runs only when
something is observing."""

from crdtsync import Doc


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


class TestOnUpdate:
    def test_value_change_with_native_old_new(self):
        doc = Doc(cid(1))
        m = doc.get_map("root")
        m.set("k", 1)  # creates the map; observed below is the update

        events = []
        doc.on_update(events.append)
        m.set("k", 2)

        assert len(events) == 1
        assert events[0].origin == "local"
        assert len(events[0].ops) > 0
        assert {"kind": "update", "path": ["root", "k"], "old": 1, "new": 2} in [
            dict(c) for c in events[0].changes
        ]

    def test_list_insert_change(self):
        doc = Doc(cid(1))
        xs = doc.get_list("xs")
        xs.append("a")

        changes = []
        doc.on_update(lambda e: changes.extend(e.changes))
        xs.append("b")

        assert {
            "kind": "list_insert",
            "path": ["xs"],
            "index": 1,
            "values": ["b"],
        } in [dict(c) for c in changes]

    def test_text_insert_change(self):
        doc = Doc(cid(1))
        t = doc.get_text("t")
        t.insert(0, "a")

        changes = []
        doc.on_update(lambda e: changes.extend(e.changes))
        t.insert(1, "bc")

        assert {"kind": "text_insert", "path": ["t"], "index": 1, "text": "bc"} in [
            dict(c) for c in changes
        ]

    def test_late_listener_not_fired_for_in_flight_event(self):
        doc = Doc(cid(1))
        m = doc.get_map("root")
        m.set("k", 1)

        state = {"added": False, "late": 0}

        def first(_e):
            if not state["added"]:
                state["added"] = True
                doc.on_update(lambda _e2: state.__setitem__("late", state["late"] + 1))

        doc.on_update(first)
        m.set("k", 2)  # adds the late listener; it must not see this event
        assert state["late"] == 0
        m.set("k", 3)
        assert state["late"] == 1

    def test_no_computation_after_unsubscribe(self):
        doc = Doc(cid(1))
        m = doc.get_map("root")
        m.set("k", 1)

        def boom(_e):
            raise AssertionError("should not fire after unsubscribe")

        off = doc.on_update(boom)
        off()
        m.set("k", 2)  # no listener → no throw


class TestObserve:
    def test_fires_only_for_observed_subtree(self):
        doc = Doc(cid(1))
        root = doc.get_map("root")
        root.get_map("a").set("x", 1)
        root.get_map("b").set("y", 1)

        a_events = []
        root.get_map("a").observe(lambda e: a_events.append(e.changes))

        root.get_map("a").set("x", 2)  # under "a" — observed
        root.get_map("b").set("y", 2)  # under "b" — not observed

        assert len(a_events) == 1
        assert {"kind": "update", "path": ["root", "a", "x"], "old": 1, "new": 2} in [
            dict(c) for c in a_events[0]
        ]

    def test_stops_after_unsubscribe(self):
        doc = Doc(cid(1))
        m = doc.get_map("root")
        m.set("k", 1)
        fired = []
        off = m.observe(lambda e: fired.append(e))
        m.set("k", 2)
        off()
        m.set("k", 3)
        assert len(fired) == 1


class TestRemoteOrigin:
    def test_applied_peer_update_tagged_remote(self):
        a, b = Doc(cid(1)), Doc(cid(2))
        a.on_update(lambda e: b.apply_update(e.ops) if e.origin == "local" else None)
        a.get_map("root").set("k", 1)  # b receives the creation (no listener yet)

        b_events = []
        b.on_update(b_events.append)
        a.get_map("root").set("k", 2)  # forwarded to b as a remote update

        assert len(b_events) == 1
        assert b_events[0].origin == "remote"
        assert {"kind": "update", "path": ["root", "k"], "old": 1, "new": 2} in [
            dict(c) for c in b_events[0].changes
        ]


REPAIR_SCHEMA = b"""{
    "schema": "notes", "version": 1, "root": "Doc",
    "types": {
        "Doc": { "kind": "map", "children": { "body": "Body" } },
        "Body": { "kind": "text", "max": 5 }
    }
}"""


class TestRepair:
    def test_fires_for_a_local_edit_overflowing_a_bounded_sequence(self):
        doc = Doc(cid(1))
        assert doc.set_schema(REPAIR_SCHEMA)
        repaired = []
        doc.on_repair(lambda e: repaired.extend(e.paths))
        doc.get_text("body").insert(0, "hello world")  # 11 > max 5
        assert ["body"] in repaired

    def test_fires_nothing_for_a_conforming_edit(self):
        doc = Doc(cid(1))
        assert doc.set_schema(REPAIR_SCHEMA)
        fired = []
        doc.on_repair(lambda e: fired.extend(e.paths))
        doc.get_text("body").insert(0, "hi")  # within max 5
        assert fired == []

    def test_fires_nothing_when_no_schema_bound(self):
        doc = Doc(cid(1))
        fired = []
        doc.on_repair(lambda e: fired.append(e))
        doc.get_text("body").insert(0, "a very long body over any bound")
        assert fired == []

    def test_stops_after_unsubscribe(self):
        doc = Doc(cid(1))
        assert doc.set_schema(REPAIR_SCHEMA)
        fired = []
        off = doc.on_repair(lambda e: fired.append(e))
        doc.get_text("body").insert(0, "overflowing")  # fires once
        off()
        doc.get_text("body").insert(0, "more overflow")
        assert len(fired) == 1
