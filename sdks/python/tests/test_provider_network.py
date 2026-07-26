"""The networked provider against the real crdtsync server.

The server is spawned in relay mode (no admin plane, no data dir), so two
providers sync a room over a real WebSocket. Skipped when the server binary is
absent — build it with `cargo build -p crdtsync-server`."""

import os
import platform
import socket
import subprocess
import threading
import time

import pytest
from crdtsync import Provider, connect


def _server_binary():
    override = os.environ.get("CRDTSYNC_SERVER_BIN")
    if override:
        return override if os.path.exists(override) else None
    name = "crdtsync-server.exe" if platform.system() == "Windows" else "crdtsync-server"
    directory = os.path.dirname(os.path.abspath(__file__))
    for _ in range(8):
        for profile in ("release", "debug"):
            candidate = os.path.join(directory, "target", profile, name)
            if os.path.exists(candidate):
                return candidate
        directory = os.path.dirname(directory)
    return None


SERVER_BINARY = _server_binary()

pytestmark = pytest.mark.skipif(
    SERVER_BINARY is None,
    reason="the crdtsync-server binary is not built (cargo build -p crdtsync-server)",
)


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("timed out waiting for a condition")
        time.sleep(0.01)


def _spawn_server(port: int):
    """Start the server on ``port``; return it once it is listening, or ``None``
    with its log when it refused to (a port claimed between probe and bind)."""
    process = subprocess.Popen(
        [SERVER_BINARY],
        env={**os.environ, "CRDTSYNC_ADDR": f"127.0.0.1:{port}"},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    # Drain the log on a thread: a blocking readline could not be given up on,
    # and an undrained pipe would wedge the server once it filled.
    started = threading.Event()
    log = []

    def drain():
        for line in process.stderr:
            log.append(line.decode("utf-8", "replace").rstrip())
            if b"serving on" in line:
                started.set()
        started.set()  # the stream ended — unblock whoever is waiting

    reader = threading.Thread(target=drain, daemon=True)
    reader.start()
    if started.wait(20.0) and process.poll() is None:
        return process, reader, log
    process.kill()
    process.wait()
    reader.join(timeout=2.0)
    process.stderr.close()
    return None, None, log


@pytest.fixture(scope="module")
def server_url():
    # A probed-then-released port can be claimed before the server binds it, and
    # a module-scoped fixture failing takes every test in the file with it — so
    # retry on a fresh port and report the server's own log if it never starts.
    logs = []
    for _ in range(3):
        port = _free_port()
        process, reader, log = _spawn_server(port)
        if process is not None:
            try:
                yield f"ws://127.0.0.1:{port}"
            finally:
                process.kill()
                process.wait()
                reader.join(timeout=2.0)
                process.stderr.close()
            return
        logs.extend(log)
    raise AssertionError("the crdtsync server did not start: " + "; ".join(logs))


@pytest.fixture
def join(server_url):
    opened = []

    def _join(room: str):
        provider = connect(server_url, room)
        opened.append(provider)
        return provider

    yield _join
    for provider in opened:
        provider.close()


class TestNetworkedProvider:
    def test_map_list_and_text_edits_reach_the_other_client(self, join):
        a = join("room-edits")
        b = join("room-edits")

        a.doc.get_map("root").set("title", "Hello")
        a.doc.get_list("items").append("x").append("y")
        a.doc.get_text("body").insert(0, "hi")

        wait_for(lambda: str(b.doc.get_text("body")) == "hi")
        wait_for(lambda: len(b.doc.get_list("items")) == 2)
        assert list(b.doc.get_list("items")) == ["x", "y"]
        assert b.doc.get_map("root").keys() == ["title"]

    def test_the_handle_graph_reads_back_through_the_room_replica(self, join):
        a = join("room-graph")
        b = join("room-graph")
        a.doc.get_map("root").set("title", "Hello")
        a.doc.get_map("root").get_map("nested").set("flag", True)
        a.doc.get_map("root").set_blob("logo", "image/png", b"\x89PNG")

        wait_for(lambda: b.doc.get_map("root").get("title") == "Hello")
        wait_for(lambda: b.doc.get_map("root").get("nested") is not None)
        assert b.doc.get_map("root").get("nested").get("flag") is True
        assert "title" in b.doc.get_map("root")
        assert "absent" not in b.doc.get_map("root")
        wait_for(lambda: b.doc.get_map("root").get_blob("logo") is not None)
        assert b.doc.get_map("root").get_blob("logo").mime == "image/png"
        assert b.doc.encode_state()

    def test_xml_and_marks_read_back_over_the_wire(self, join):
        a = join("room-rich")
        b = join("room-rich")
        a.doc.get_xml("tree").element("article")
        a.doc.get_xml("tree").insert_element(0, "p")
        a.doc.get_text("body").insert(0, "hello")
        a.doc.get_text("body").mark(0, 2, "bold", True)

        wait_for(lambda: b.doc.get_xml("tree").tag == "article")
        wait_for(lambda: len(b.doc.get_xml("tree")) == 1)
        wait_for(lambda: [m["name"] for m in b.doc.get_text("body").marks_at(0)] == ["bold"])
        assert b.doc.get_text("body").marks_at(4) == []
        assert b.doc.get_xml("tree").tag == "article"
        assert b.doc.get_xml("absent").tag is None

    def test_a_peer_edit_fires_remote_reactivity(self, join):
        a = join("room-reactivity")
        b = join("room-reactivity")
        updates, observed = [], []
        b.doc.on_update(updates.append)
        b.doc.get_map("root").observe(observed.append)

        a.doc.get_map("root").set("k", "first")
        a.doc.get_map("root").set("k", "second")

        wait_for(lambda: b.doc.get_map("root").get("k") == "second")
        wait_for(lambda: any(e.origin == "remote" for e in updates))
        assert all(e.origin == "remote" for e in updates)
        # The change list names what actually moved, not just that something did.
        assert any(c["kind"] in ("add", "update") for e in updates for c in e.changes)
        wait_for(lambda: len(observed) > 0)
        assert all(e.origin == "remote" for e in observed)

    def test_a_local_edit_reports_itself_before_the_room_answers(self, join):
        a = join("room-local-reactivity")
        updates = []
        a.doc.on_update(updates.append)
        a.doc.get_text("body").insert(0, "hi")
        assert [e.origin for e in updates] == ["local"]
        assert updates[0].ops  # the frame it put on the wire

    def test_cursors_survive_a_concurrent_remote_insert(self, join):
        a = join("room-cursors")
        b = join("room-cursors")
        a.doc.get_text("body").insert(0, "hello")
        wait_for(lambda: str(b.doc.get_text("body")) == "hello")

        anchor = b.doc.get_text("body").relative_position(5)
        assert anchor is not None
        a.doc.get_text("body").insert(0, ">> ")
        wait_for(lambda: str(b.doc.get_text("body")) == ">> hello")
        # The anchor tracks the character it was taken against, not the index.
        assert b.doc.get_text("body").resolve(anchor) == 8

    def test_a_late_joiner_catches_up_to_existing_state(self, join):
        a = join("room-catchup")
        a.doc.get_text("body").insert(0, "early")
        wait_for(lambda: a.outbox_len == 0)

        b = join("room-catchup")
        wait_for(lambda: str(b.doc.get_text("body")) == "early")

    def test_the_server_acknowledges_and_drains_the_outbox(self, join):
        a = join("room-outbox")
        # An acknowledgement may already have landed by the time the edit
        # returns, so the drain is the observable: the ops leave the outbox
        # rather than accumulating unacknowledged.
        for i in range(20):
            a.doc.get_list("items").append(i)
        wait_for(lambda: a.outbox_len == 0)
        assert a.state == "connected"

    def test_concurrent_authors_all_reach_the_room(self, join):
        a = join("room-threads")
        b = join("room-threads")
        # Two threads editing one doc stamp their ops under the replica lock but
        # write them after; a later op acknowledged ahead of an earlier one would
        # drop the earlier from the outbox before it was ever sent.
        threads = [
            threading.Thread(target=lambda tag=tag: [
                a.doc.get_list(tag).append(i) for i in range(25)
            ])
            for tag in ("left", "right")
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        wait_for(lambda: a.outbox_len == 0)
        wait_for(lambda: len(b.doc.get_list("left")) == 25)
        wait_for(lambda: len(b.doc.get_list("right")) == 25)

    def test_a_transaction_body_that_raises_still_ships_what_it_committed(self, join):
        a = join("room-tx-raise")
        b = join("room-tx-raise")

        def body():
            a.doc.get_list("items").append("kept")
            raise RuntimeError("halfway")

        with pytest.raises(RuntimeError):
            a.doc.transact(body)
        # The edit is committed to this replica, so the room has to see it too.
        wait_for(lambda: len(b.doc.get_list("items")) == 1)

    def test_an_edit_survives_a_dropped_socket(self, join):
        a = join("room-reconnect")
        b = join("room-reconnect")
        a.doc.get_text("body").insert(0, "before")
        wait_for(lambda: str(b.doc.get_text("body")) == "before")

        # Drop a's socket underneath it and edit while it is offline; the
        # reconnect resumes the channel and resends the outbox.
        wait_for(lambda: a._ws is not None)
        socket = a._ws
        socket.close()
        wait_for(lambda: a.state != "connected")
        a.doc.get_text("body").insert(6, "-after")
        wait_for(lambda: a.state == "connected")
        wait_for(lambda: str(b.doc.get_text("body")) == "before-after")
        assert str(a.doc.get_text("body")) == "before-after"

    def test_a_transaction_rides_the_wire_as_one_batch(self, join):
        a = join("room-transact")
        b = join("room-transact")
        a.doc.transact(lambda: a.doc.get_list("items").append("x").append("y").append("z"))
        wait_for(lambda: len(b.doc.get_list("items")) == 3)
        assert list(b.doc.get_list("items")) == ["x", "y", "z"]

    def test_awareness_fans_out_to_the_room(self, join):
        a = join("room-awareness")
        b = join("room-awareness")
        a.set_awareness("cursor", "10")
        wait_for(lambda: b.awareness(a.actor, "cursor") == b"10")

    def test_a_closed_provider_stops_syncing_but_keeps_its_replica(self, join):
        a = join("room-closed")
        b = join("room-closed")
        a.doc.get_text("body").insert(0, "before")
        wait_for(lambda: str(b.doc.get_text("body")) == "before")

        b.close()
        a.doc.get_text("body").insert(6, "-after")
        wait_for(lambda: a.outbox_len == 0)
        time.sleep(0.2)
        # The closed provider's replica is still readable — closing releases the
        # connection, not the document — and it stopped at what it had synced.
        assert str(b.doc.get_text("body")) == "before"

    def test_edits_authored_during_the_handshake_still_reach_the_room(self, server_url, join):
        room = "room-handshake"
        peer = join(room)
        provider = connect(server_url, room)
        try:
            # The connect above already synced; author against a second provider
            # while its own handshake is still in flight.
            racer = Provider(server_url, room)
            try:
                for i in range(20):
                    racer.doc.get_list("items").append(i)
                racer.wait_connected(timeout=10.0)
                wait_for(lambda: len(peer.doc.get_list("items")) == 20)
                assert racer.state == "connected"
            finally:
                racer.close()
        finally:
            provider.close()
