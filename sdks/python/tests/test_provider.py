"""The ergonomic sync provider: an offline-first binding over a Doc's apply/emit
seam. The app supplies the transport (a `send` callback for outbound ops, feeds
inbound ops to `receive`); the provider owns the connection state and an offline
outbox so edits made while disconnected flush on reconnect. Two docs bound to a
pair of providers converge."""

from crdtsync import Doc, Provider


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def _linked_pair():
    """Two docs each bound to a provider that forwards to the other's `receive`."""
    a, b = Doc(cid(1)), Doc(cid(2))
    link: dict = {}
    pa = Provider(a, lambda ops: link["b"].receive(ops), connected=True)
    pb = Provider(b, lambda ops: link["a"].receive(ops), connected=True)
    link["a"], link["b"] = pa, pb
    return a, b, pa, pb


class TestProvider:
    def test_two_docs_sync_through_providers(self):
        a, b, pa, pb = _linked_pair()
        a.get_map("root").set("k", 1)
        a.get_map("root").get_text("body").insert(0, "hi")
        assert b.get_map("root").get("k") == 1
        assert str(b.get_map("root").get_text("body")) == "hi"

    def test_no_echo_loop_between_providers(self):
        a, b, pa, pb = _linked_pair()
        # A remote apply must not re-emit as a local edit (would loop forever).
        a.get_map("root").set("k", 1)
        assert b.get_map("root").get("k") == 1
        assert a.get_map("root").get("k") == 1

    def test_offline_edits_flush_on_connect(self):
        a, b = Doc(cid(1)), Doc(cid(2))
        link: dict = {}
        pa = Provider(a, lambda ops: link["b"].receive(ops))  # disconnected
        pb = Provider(b, lambda ops: link["a"].receive(ops), connected=True)
        link["a"], link["b"] = pa, pb

        assert pa.state == "disconnected"
        a.get_map("root").set("k", 1)  # queued in the offline outbox
        assert b.get_map("root").get("k") is None  # not sent yet
        assert pa.outbox_len == 1

        pa.connect()
        assert b.get_map("root").get("k") == 1  # flushed on reconnect
        assert pa.outbox_len == 0

    def test_connection_state_transitions_and_notifies(self):
        p = Provider(Doc(cid(1)), lambda ops: None)
        states = []
        p.on_state(states.append)
        assert p.state == "disconnected"
        p.connect()
        assert p.state == "connected"
        p.disconnect()
        assert states == ["connected", "disconnected"]

    def test_receive_applies_and_fires_remote_reactivity(self):
        a, b, pa, pb = _linked_pair()
        events = []
        b.on_update(events.append)
        a.get_map("root").set("k", 1)
        a.get_map("root").set("k", 2)
        assert any(e.origin == "remote" for e in events)

    def test_close_stops_forwarding(self):
        a, b, pa, pb = _linked_pair()
        pa.close()
        a.get_map("root").set("k", 1)
        assert b.get_map("root").get("k") is None

    def test_connected_edit_sends_immediately(self):
        sent = []
        p = Provider(Doc(cid(1)), sent.append, connected=True)
        p.doc.get_map("root").set("k", 1)
        assert len(sent) == 1
