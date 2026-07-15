"""The Python SDK drives the wire client over the C ABI: a local edit produces a
frame a peer folds in and converges on, and the handshake surface marshals."""

from crdtsync import Client, DiffKind, Document, ErrorCode, Redirect, Rejected, ServerError


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def test_a_local_edit_travels_to_a_peer():
    with Client(cid(1)) as a, Client(cid(2)) as b:
        # Both fresh sessions assign channel 0 to their first subscription.
        ca, _ = a.subscribe(b"room-1")
        cb, _ = b.subscribe(b"room-1")
        assert ca == 0 and cb == 0

        ops = a.register_int(ca, [b"age"], 30)
        assert a.get_int(ca, [b"age"]) == 30
        assert b.receive(ops) == 1
        assert b.get_int(cb, [b"age"]) == 30
        assert b.last_seen_seq(cb) == 1


def test_offline_queue_outbox_drains_on_ack():
    import struct

    with Client(cid(1)) as a:
        ca, _ = a.subscribe(b"room-1")
        a.register_int(ca, [b"age"], 30)
        assert a.outbox_len(ca) == 1
        a.register_int(ca, [b"age"], 31)
        assert a.outbox_len(ca) == 2
        # The unacknowledged tail replays as one Ops frame.
        assert len(a.resend(ca)) > 0
        # An Accepted through u64::MAX drains the outbox: tag 18, u32 channel,
        # u64 frontier.
        accepted = struct.pack("<BIQ", 18, ca, (1 << 64) - 1)
        assert a.receive(accepted) == 1
        assert a.outbox_len(ca) == 0
        assert a.resend(ca) == b""


def test_bytes_scalar_round_trips():
    with Client(cid(1)) as a, Client(cid(2)) as b:
        ca, _ = a.subscribe(b"room-1")
        cb, _ = b.subscribe(b"room-1")
        b.receive(a.set_bytes(ca, [b"blob"], b"\x00\x01\xff"))
        assert b.get_bytes(cb, [b"blob"]) == b"\x00\x01\xff"


def test_handshake_frames_marshal():
    with Client(cid(1)) as a:
        assert len(a.hello()) > 0
        assert len(a.auth(b"token")) > 0
        # No actor until the server's AuthOk is folded in.
        assert a.actor() is None


def test_declared_app_rides_along_in_the_hello_frame():
    with Client(cid(1)) as a:
        # A bare client opens as a relay — no app named in the frame.
        assert b"app-x" not in a.hello()
        # Declaring an app names it in the next Hello.
        a.declare_app(b"app-x", 3)
        assert b"app-x" in a.hello()


def test_server_advertised_schema_is_readable():
    import struct

    def advert(version: int, body: bytes) -> bytes:
        # SchemaAdvert: tag 21, u32 version, u32 length prefix, bytes.
        return struct.pack("<BII", 21, version, len(body)) + body

    with Client(cid(1)) as a:
        # Nothing advertised yet.
        assert a.active_schema_version() is None
        assert a.active_schema() is None
        # Folding a SchemaAdvert records the served version and its bytes.
        assert a.receive(advert(4, b"schema-body")) == 1
        assert a.active_schema_version() == 4
        assert a.active_schema() == b"schema-body"
        # A later advert supersedes it.
        assert a.receive(advert(5, b"next-body")) == 1
        assert a.active_schema_version() == 5
        assert a.active_schema() == b"next-body"
        # An empty body is still an advertisement, not "none".
        assert a.receive(advert(6, b"")) == 1
        assert a.active_schema_version() == 6
        assert a.active_schema() == b""


def test_awareness_publish_and_lifecycle():
    with Client(cid(1)) as a:
        ch, _ = a.subscribe(b"room-1")
        assert len(a.set_awareness(ch, b"cursor", b"x")) > 0
        assert a.awareness_len(ch) == 0  # no peer has published yet
        # Unsubscribe drops the channel: reads report absent, resume is empty.
        assert len(a.unsubscribe(ch)) > 0
        assert a.last_seen_seq(ch) is None
        assert a.resume(ch) == b""


def test_subscribe_branch_carries_the_named_branch():
    with Client(cid(1)) as a:
        # A named branch rides along in the Subscribe frame.
        ch, frame = a.subscribe_branch(b"room-1", b"feature-x")
        assert ch == 0
        assert b"feature-x" in frame
        # An empty branch is the default/active branch, as the plain subscribe.
        _, default_frame = a.subscribe_branch(b"room-1", b"")
        assert b"feature-x" not in default_frame
        _, plain = a.subscribe(b"room-1")
        assert b"feature-x" not in plain


def test_subscribe_zone_carries_the_named_zone():
    with Client(cid(1)) as a:
        # A named zone rides along in the Subscribe frame.
        ch, frame = a.subscribe_zone(b"room-1", b"west")
        assert ch == 0
        assert b"west" in frame
        # An empty zone is the whole room, as the plain subscribe.
        _, default_frame = a.subscribe_zone(b"room-1", b"")
        assert b"west" not in default_frame
        _, plain = a.subscribe(b"room-1")
        assert b"west" not in plain


def test_version_requests_marshal():
    with Client(cid(1)) as a:
        ch, _ = a.subscribe(b"room-1")
        assert len(a.create_version(ch, b"v1")) > 0
        assert len(a.rename_version(ch, b"v1", b"v2")) > 0
        assert len(a.delete_version(ch, b"v1")) > 0
        assert len(a.list_versions(ch)) > 0
        assert len(a.fetch_version(ch, b"v1")) > 0
        # Nothing reported until a server reply is folded in.
        assert a.versions(ch) == []
        assert a.version_state(ch, b"v1") is None


def test_branch_requests_marshal():
    with Client(cid(1)) as a:
        room = b"room-1"
        assert len(a.list_branches(room)) > 0
        assert len(a.fork_branch(room, b"feature", b"main")) > 0
        assert len(a.fork_branch_from_version(room, b"feature", b"v1")) > 0
        assert len(a.restore_branch(room, b"restored", b"v1")) > 0
        assert len(a.publish_branch(room, b"live")) > 0
        assert len(a.delete_branch(room, b"feature")) > 0
        # Nothing reported until a server reply is folded in.
        assert a.branches(room) == []


def test_diff_query_round_trips():
    import struct

    from crdtsync import _diff_raw

    def put_bytes(b: bytes) -> bytes:
        return struct.pack("<I", len(b)) + b

    with Client(cid(1)) as a:
        room = b"room-1"
        # Both kinds frame a request; room-keyed, no subscription needed.
        assert len(a.diff_query(room, DiffKind.VERSIONS, b"a", b"b")) > 0
        assert len(a.diff_query(room, DiffKind.BRANCHES, b"main", b"draft")) > 0
        # No result until one is answered.
        assert a.diff(room) is None

        # Build the change payload the server would return.
        with Document(cid(2)) as d:
            d.register_int([b"age"], 30)
            old = d.encode_state()
            d.register_int([b"age"], 40)
            changes = _diff_raw(old, d.encode_state())

        # A DiffResult reply: tag 41, u32-prefixed room, u32-prefixed change list.
        frame = struct.pack("<B", 41) + put_bytes(room) + put_bytes(changes)
        assert a.receive(frame) == 1

        result = a.diff(room)
        assert result is not None
        assert len(result) == 1
        assert result[0]["op"] == "value"
        assert result[0]["new"] == {"t": "int", "v": 40}


def test_clone_room_round_trips():
    import struct

    def put_bytes(b: bytes) -> bytes:
        return struct.pack("<I", len(b)) + b

    with Client(cid(1)) as a:
        src, dst = b"template", b"copy"
        # The clone request frames a room-keyed request; no subscription needed.
        assert len(a.clone_room(src, dst)) > 0
        # No result until one is answered.
        assert a.clone_result(dst) is None

        # A CloneRoomResult reply: tag 43, u32-prefixed dst, one byte created.
        frame = struct.pack("<B", 43) + put_bytes(dst) + struct.pack("<B", 1)
        assert a.receive(frame) == 1
        assert a.clone_result(dst) is True
        # An unrelated destination is untouched.
        assert a.clone_result(b"other") is None


def test_receive_rejects_garbage():
    with Client(cid(1)) as a:
        assert a.receive(b"\xff\xff\xff\xff") == 0


def test_server_error_frame_raises_with_its_code():
    import struct

    import pytest

    def error(code: int, message: bytes) -> bytes:
        # Error: tag 3, u16 code, u32-prefixed message, u32-prefixed details.
        return (
            struct.pack("<BH", 3, code)
            + struct.pack("<I", len(message))
            + message
            + struct.pack("<I", 0)
        )

    with Client(cid(1)) as a:
        # A server Error surfaces as ServerError carrying the code; UPDATE_REQUIRED
        # (6) is the onUpdateRequired signal.
        with pytest.raises(ServerError) as excinfo:
            a.receive(error(6, b"please update"))
        assert excinfo.value.code is ErrorCode.UPDATE_REQUIRED
        # A normal frame still applies cleanly.
        ca, _ = a.subscribe(b"room-1")
        assert a.receive(a.register_int(ca, [b"age"], 30)) == 1


def test_a_server_ops_rejection_surfaces_the_refused_batch():
    import struct

    def ops_rejected(channel: int, seqs: list, reason: int) -> bytes:
        # OpsRejected: tag 22, u32 channel, u16 reason, u32 seq-count, u64 seqs.
        out = struct.pack("<BIHI", 22, channel, reason, len(seqs))
        for s in seqs:
            out += struct.pack("<Q", s)
        return out

    with Client(cid(1)) as a:
        ca, _ = a.subscribe(b"room-1")
        # Author an edit; its ops enter the outbox with per-client sequences 0..n.
        a.register_int(ca, [b"age"], 30)
        n = a.outbox_len(ca)
        assert n >= 1

        # The server refuses that batch — Forbidden, the auth-revoked rejection.
        assert a.receive(ops_rejected(ca, list(range(n)), ErrorCode.FORBIDDEN)) == 1

        # The drain yields the one batch: the channel, the reason, and the refused
        # ops still carrying their bytes.
        rejected = a.take_rejected()
        assert len(rejected) == 1
        r = rejected[0]
        assert isinstance(r, Rejected)
        assert r.channel == ca
        assert r.reason is ErrorCode.FORBIDDEN
        assert len(r.ops) == n
        assert all(isinstance(op, bytes) and len(op) > 0 for op in r.ops)

        # The refused ops left the outbox; draining, a second call is empty.
        assert a.outbox_len(ca) == 0
        assert a.take_rejected() == []


def test_take_rejected_is_empty_without_a_rejection():
    with Client(cid(1)) as a:
        a.subscribe(b"room-1")
        assert a.take_rejected() == []


def test_a_server_redirect_surfaces_the_room_and_leader():
    import struct

    def redirect(room: bytes, leader_addr: bytes) -> bytes:
        # Redirect: tag 23, then a u32-length-prefixed room and leader_addr.
        return (
            struct.pack("<BI", 23, len(room))
            + room
            + struct.pack("<I", len(leader_addr))
            + leader_addr
        )

    with Client(cid(1)) as a:
        # A node that does not lead the room reports where the leader is.
        assert a.receive(redirect(b"room-1", b"10.0.0.7:4000")) == 1

        # The drain yields the one target: the room and the leader's address.
        redirects = a.take_redirects()
        assert len(redirects) == 1
        target = redirects[0]
        assert isinstance(target, Redirect)
        assert target.room == b"room-1"
        assert target.leader_addr == b"10.0.0.7:4000"

        # Draining: a second call is empty.
        assert a.take_redirects() == []


def test_take_redirects_is_empty_without_a_redirect():
    with Client(cid(1)) as a:
        assert a.take_redirects() == []


def test_channel_bounds_are_checked():
    import pytest

    with Client(cid(1)) as a:
        with pytest.raises(ValueError):
            a.register_int(-1, [b"k"], 1)
        with pytest.raises(ValueError):
            a.get_int(2**32, [b"k"])


def test_atomic_transaction_over_the_client():
    with Client(cid(1)) as a, Client(cid(2)) as b:
        ca, _ = a.subscribe(b"room-1")
        cb, _ = b.subscribe(b"room-1")

        a.begin_atomic(ca)
        # Edits accumulate while recording; only the commit frame is sent.
        a.register_int(ca, [b"x"], 1)
        a.register_int(ca, [b"y"], 2)
        frame = a.commit_atomic(ca)
        assert len(frame) > 0
        assert a.get_int(ca, [b"x"]) == 1

        b.receive(frame)
        assert b.get_int(cb, [b"x"]) == 1
        assert b.get_int(cb, [b"y"]) == 2
