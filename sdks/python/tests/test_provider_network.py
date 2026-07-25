"""The networked provider against the real crdtsync server.

The server is spawned in relay mode (no admin plane, no data dir), so two
providers sync a room over a real WebSocket. Skipped when the server binary is
absent — build it with `cargo build -p crdtsync-server`."""

import os
import platform
import socket
import subprocess
import time

import pytest
from crdtsync import connect


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
    try:
        deadline = time.monotonic() + 15.0
        while True:
            line = process.stderr.readline()
            if b"serving on" in line:
                break
            if process.poll() is not None or time.monotonic() > deadline:
                raise AssertionError("the crdtsync server did not start")
        yield f"ws://127.0.0.1:{port}"
    finally:
        process.kill()
        process.wait()
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
        a.doc.get_text("body").insert(0, "hi")
        assert a.outbox_len > 0
        wait_for(lambda: a.outbox_len == 0)

    def test_an_edit_survives_a_dropped_socket(self, join):
        a = join("room-reconnect")
        b = join("room-reconnect")
        a.doc.get_text("body").insert(0, "before")
        wait_for(lambda: str(b.doc.get_text("body")) == "before")

        # Drop a's socket underneath it and edit while it is offline; the
        # reconnect resumes the channel and resends the outbox.
        a._ws.close()
        wait_for(lambda: a.state != "connected")
        a.doc.get_text("body").insert(6, "-after")
        wait_for(lambda: a.state == "connected")
        wait_for(lambda: str(b.doc.get_text("body")) == "before-after")
        assert str(a.doc.get_text("body")) == "before-after"

    def test_awareness_fans_out_to_the_room(self, join):
        a = join("room-awareness")
        b = join("room-awareness")
        a.set_awareness("cursor", "10")
        wait_for(lambda: b.awareness(a.actor, "cursor") == b"10")

    def test_a_closed_provider_stops_syncing(self, join):
        a = join("room-closed")
        b = join("room-closed")
        b.close()
        a.doc.get_text("body").insert(0, "after-close")
        time.sleep(0.2)
        assert str(b.doc.get_text("body")) == ""
