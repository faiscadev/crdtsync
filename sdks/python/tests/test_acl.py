"""The Python SDK authors doc-level ACL grants and revokes over the C ABI: a grant
hands back the tuple id and ops a peer converges on; a revoke tombstones by that id.
Both the doc surface and the per-channel client surface marshal the subject /
capability-or-role / effect / path / grantor faithfully."""

import struct

import pytest

from crdtsync import Capability, Client, Document, Effect, SubjectKind, actor_key


def cid(first: int) -> bytes:
    return bytes([first] + [0] * 15)


def test_actor_key_is_deterministic_and_16_bytes():
    k = actor_key(b"user:alice")
    assert len(k) == 16
    assert k == actor_key(b"user:alice")
    assert k != actor_key(b"user:bob")


def test_grant_to_an_actor_key_converges():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        subject = actor_key(b"user:bob")
        grantor = actor_key(b"user:alice")
        tuple_id, ops = a.acl_grant(
            SubjectKind.ACTOR,
            subject,
            grantor=grantor,
            path=[b"doc"],
            capability=Capability.OWN,
        )
        assert len(tuple_id) == 16
        assert b.apply(ops) >= 1


def test_grant_returns_a_tuple_id_and_converges():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        tuple_id, ops = a.acl_grant(
            SubjectKind.ACTOR,
            cid(7),
            grantor=cid(1),
            path=[b"doc"],
            capability=Capability.READ,
        )
        assert len(tuple_id) == 16
        assert ops, "a grant emits ops"
        # The grant tuple materialises on a second replica.
        assert b.apply(ops) >= 1


def test_revoke_by_the_returned_id():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        tuple_id, ops = a.acl_grant(
            SubjectKind.GROUP,
            b"editors",
            grantor=cid(1),
            role=b"reviewer",
            effect=Effect.DENY,
        )
        b.apply(ops)
        revoke_ops = a.acl_revoke(tuple_id)
        assert revoke_ops, "revoking a held tuple emits ops"
        assert b.apply(revoke_ops) >= 1


def test_grant_needs_exactly_one_of_capability_or_role():
    with Document(cid(1)) as a:
        with pytest.raises(ValueError):
            a.acl_grant(SubjectKind.ANYONE, b"", grantor=cid(1))
        with pytest.raises(ValueError):
            a.acl_grant(
                SubjectKind.ANYONE,
                b"",
                grantor=cid(1),
                capability=Capability.READ,
                role=b"reviewer",
            )


def test_grant_rejects_a_malformed_grantor():
    with Document(cid(1)) as a:
        with pytest.raises(ValueError):
            a.acl_grant(
                SubjectKind.ACTOR,
                cid(7),
                grantor=b"short",
                capability=Capability.OWN,
            )


def test_revoke_of_an_unknown_id_is_inert():
    with Document(cid(1)) as a:
        # A valid 16-byte id the replica never held: no ops, no error.
        assert a.acl_revoke(bytes(16)) == b""
        # A malformed id is a programming error.
        with pytest.raises(ValueError):
            a.acl_revoke(b"short")


def test_class_and_authenticated_subjects_marshal():
    with Document(cid(1)) as a, Document(cid(2)) as b:
        _, ops = a.acl_grant(
            SubjectKind.AUTHENTICATED,
            b"",
            grantor=cid(1),
            path=[b"doc"],
            capability=Capability.WRITE,
            effect=Effect.ALLOW,
        )
        assert b.apply(ops) >= 1


def test_client_acl_grant_and_revoke_route_through_the_outbox():
    with Client(cid(1)) as a, Client(cid(2)) as b:
        ca, _ = a.subscribe(b"room-1")
        cb, _ = b.subscribe(b"room-1")

        tuple_id, frame = a.acl_grant(
            ca,
            SubjectKind.ACTOR,
            cid(7),
            grantor=cid(1),
            path=[b"doc"],
            capability=Capability.WRITE,
        )
        assert tuple_id is not None and len(tuple_id) == 16
        assert frame, "the grant frames an Ops message"
        assert a.outbox_len(ca) == 1
        assert b.receive(frame) >= 1

        revoke_frame = a.acl_revoke(ca, tuple_id)
        assert revoke_frame, "the revoke frames an Ops message"
        assert a.outbox_len(ca) == 2
        assert b.receive(revoke_frame) >= 1

        # An ack through u64::MAX drains the outbox (tag 18, u32 channel, u64 frontier).
        accepted = struct.pack("<BIQ", 18, ca, (1 << 64) - 1)
        assert a.receive(accepted) == 1
        assert a.outbox_len(ca) == 0


def test_client_acl_on_an_unheld_channel_is_inert():
    with Client(cid(1)) as a:
        tuple_id, frame = a.acl_grant(
            99,
            SubjectKind.ANYONE,
            b"",
            grantor=cid(1),
            capability=Capability.READ,
        )
        assert tuple_id is None
        assert frame == b""
        assert a.acl_revoke(99, bytes(16)) == b""
