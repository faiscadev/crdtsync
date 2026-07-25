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


@pytest.fixture(scope="module")
def server_url():
    port = _free_port()
    process = subprocess.Popen(
        [SERVER_BINARY],
        env={**os.environ, "CRDTSYNC_ADDR": f"127.0.0.1:{port}"},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    # Drain the server's log on a thread: a blocking readline could not be given
    # up on, and an undrained pipe would wedge the server once it filled.
    started = threading.Event()

    def drain():
        for line in process.stderr:
            if b"serving on" in line:
                started.set()
        started.set()  # the stream ended — unblock whoever is waiting

    reader = threading.Thread(target=drain, daemon=True)
    reader.start()
    try:
        if not started.wait(20.0) or process.poll() is not None:
            raise AssertionError("the crdtsync server did not start")
        yield f"ws://127.0.0.1:{port}"
    finally:
        process.kill()
        process.wait()
        reader.join(timeout=2.0)
        process.stderr.close()


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
        assert a.state == "connected"

        a.doc.get_map("root").set("title", "Hello")
        a.doc.get_list("items").append("x").append("y")
        a.doc.get_text("body").insert(0, "hi")

        wait_for(lambda: str(b.doc.get_text("body")) == "hi")
        wait_for(lambda: len(b.doc.get_list("items")) == 2)
        assert list(b.doc.get_list("items")) == ["x", "y"]
        assert b.doc.get_map("root").keys() == ["title"]

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
        assert socket is not None
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
