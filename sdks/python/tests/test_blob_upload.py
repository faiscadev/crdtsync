"""The Python SDK's ``upload_blob`` helper POSTs bytes to the server's ``POST
/blobs`` and returns the 16-byte handle, which round-trips through
``set_blob_ref`` + ``get_blob``. The server is a stdlib mock that echoes a fixed
handle and records the request it received, so the test asserts request
construction and response parsing without a live server."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from crdtsync import BlobRef, Document, upload_blob

HANDLE = bytes(range(16))


class _BlobHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers["Content-Length"])
        body = self.rfile.read(length)
        self.server.received = {
            "path": self.path,
            "authorization": self.headers.get("Authorization"),
            "content_type": self.headers.get("Content-Type"),
            "body": body,
        }
        payload = json.dumps({"id": HANDLE.hex(), "size": len(body), "inline": False}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):  # silence the test server's stderr chatter
        pass


def _serve() -> HTTPServer:
    server = HTTPServer(("127.0.0.1", 0), _BlobHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _base_url(server: HTTPServer) -> str:
    host, port = server.server_address
    return f"http://{host}:{port}"


def test_upload_returns_the_handle_and_sends_the_request():
    server = _serve()
    try:
        raw = b"\x89PNG\x00\xff-a-large-object"
        blob_id = upload_blob(_base_url(server), raw, b"user-cred", "image/png")
        assert blob_id == HANDLE
        assert len(blob_id) == 16
        req = server.received
        assert req["path"] == "/blobs"
        assert req["authorization"] == "user-cred"
        assert req["content_type"] == "image/png"
        assert req["body"] == raw
    finally:
        server.shutdown()


def test_uploaded_handle_round_trips_through_set_blob_ref():
    server = _serve()
    try:
        raw = b"\x00" * 10_000
        blob_id = upload_blob(_base_url(server), raw, b"user-cred")
        with Document(bytes([1] + [0] * 15)) as doc:
            doc.set_blob_ref([b"video"], blob_id, "video/mp4", len(raw))
            blob = doc.get_blob([b"video"])
            assert blob == BlobRef(id=blob_id, mime="video/mp4", size=len(raw), inline=None)
    finally:
        server.shutdown()


def test_default_mime_is_octet_stream():
    server = _serve()
    try:
        upload_blob(_base_url(server), b"bytes", b"user-cred")
        assert server.received["content_type"] == "application/octet-stream"
    finally:
        server.shutdown()
