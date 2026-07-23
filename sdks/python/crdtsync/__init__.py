"""crdtsync — Python bindings over the CRDT core's C ABI.

A :class:`Document` is a local replica. A slot is addressed by a *path*: a list
of ``bytes`` keys naming nested maps, the last key the slot itself. An edit
applies locally and returns the encoded ops to broadcast; :meth:`Document.apply`
folds a peer's ops back in. Two documents that exchange those bytes converge.

The native library is loaded at import time from ``target/{release,debug}`` (or
``$CRDTSYNC_LIB``); nothing is compiled here.
"""

from __future__ import annotations

import ctypes
import enum
import json
import os
import platform
import struct
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, List, NamedTuple, Optional, Tuple, Union

__all__ = [
    "BlobRef",
    "Branch",
    "Capability",
    "ChangeEvent",
    "Client",
    "CrdtList",
    "CrdtMap",
    "CrdtText",
    "CrdtXml",
    "DiffKind",
    "Doc",
    "Document",
    "Effect",
    "ErrorCode",
    "Key",
    "Provider",
    "Redirect",
    "Rejected",
    "RepairEvent",
    "ServerError",
    "Side",
    "SubjectKind",
    "Undo",
    "UpdateEvent",
    "actor_key",
    "diff",
    "diff_decode",
    "encode_path",
    "upload_blob",
]

Path = List[bytes]

#: An ergonomic map key: a ``str`` (utf-8) or raw ``bytes``. Byte-paths stay hidden.
Key = Union[str, bytes]


class Side(enum.IntEnum):
    """Which edge of an index a captured position anchors to."""

    LEFT = 0
    RIGHT = 1


class SubjectKind(enum.IntEnum):
    """Who a doc-ACL grant targets. ``ACTOR`` names a 16-byte actor id; ``GROUP`` a
    membership name; the rest are the well-known classes."""

    ACTOR = 0
    GROUP = 1
    AUTHENTICATED = 2
    ANONYMOUS = 3
    ANYONE = 4


class Capability(enum.IntEnum):
    """A direct power a grant confers over a subtree."""

    READ = 0
    WRITE = 1
    PUBLISH_AWARENESS = 2
    OWN = 3


class Effect(enum.IntEnum):
    """Whether a grant allows or denies."""

    ALLOW = 0
    DENY = 1


def _acl_grant_args(subject_kind, subject, capability, role, effect):
    """Resolve a grant's subject/capability-or-role/effect to the C discriminants and
    byte strings. A grant confers exactly one of ``capability`` or ``role``."""
    sk = int(SubjectKind(subject_kind))
    subject = subject or b""
    if (capability is None) == (role is None):
        raise ValueError("a grant confers exactly one of a capability or a role")
    if capability is not None:
        grant_kind, cap, role_bytes = 0, int(Capability(capability)), b""
    else:
        grant_kind, cap, role_bytes = 1, 0, role
    return sk, subject, grant_kind, cap, role_bytes, int(Effect(effect))


class ErrorCode(enum.IntEnum):
    """A failure the server reports to the client. ``UPDATE_REQUIRED`` is the
    ``onUpdateRequired`` signal: the client's version can't bridge the room's
    across a breaking gap, so the app prompts an update or falls back read-only."""

    PROTOCOL_VIOLATION = 0
    UNSUPPORTED_VERSION = 1
    AUTH_FAILED = 2
    UNKNOWN_ROOM = 3
    INTERNAL = 4
    FORBIDDEN = 5
    UPDATE_REQUIRED = 6
    NOT_FOUND = 7
    SCHEMA_VIOLATION = 8


class DiffKind(enum.IntEnum):
    """Which pair of a room's states a client :meth:`Client.diff_query` compares."""

    VERSIONS = 0  # two of a room's saved versions
    BRANCHES = 1  # two of a room's branches' HEADs


class ServerError(RuntimeError):
    """A server ``Error`` frame folded in through :meth:`Client.receive`, carrying
    the :class:`ErrorCode` the server reported."""

    def __init__(self, code: ErrorCode):
        super().__init__(f"server reported {code.name}")
        self.code = code


class Redirect(NamedTuple):
    """A room the server redirected to its leader, surfaced by
    :meth:`Client.take_redirects`. A node that does not lead ``room`` reports the
    leader's advertise address ``leader_addr`` so the transport reconnects there;
    the core holds no socket, so reconnecting is the app's job."""

    room: bytes
    leader_addr: bytes


class Rejected(NamedTuple):
    """An op batch the server refused, surfaced by :meth:`Client.take_rejected`
    for the app to show, discard, or export. ``channel`` names the room, ``reason``
    the :class:`ErrorCode` (``FORBIDDEN`` for auth revoked), and ``ops`` the refused
    ops still carrying their bytes."""

    channel: int
    reason: ErrorCode
    ops: List[bytes]


class Branch(NamedTuple):
    """One branch of a room as the client observes it, returned by
    :meth:`Client.branches`. ``name`` is the branch name, ``fork_point`` the
    history position it shares up to, ``head`` its own high-water position, and
    ``published`` whether it is a read-only publish target."""

    name: bytes
    fork_point: int
    head: int
    published: bool


class BlobRef(NamedTuple):
    """A reference to out-of-band binary content read back by
    :meth:`Document.get_blob`. ``id`` is the 16-byte public handle, ``mime`` the
    content type, ``size`` the byte length. ``inline`` carries the bytes for a
    small blob that rides in the ref, and is ``None`` for a store-backed ref
    fetched by ``id``."""

    id: bytes
    mime: str
    size: int
    inline: Optional[bytes]


class _CrdtBuf(ctypes.Structure):
    _fields_ = [("ptr", ctypes.POINTER(ctypes.c_uint8)), ("len", ctypes.c_size_t)]


def _library_path() -> str:
    override = os.environ.get("CRDTSYNC_LIB")
    if override:
        return override
    name = {
        "Darwin": "libcrdtsync_ffi.dylib",
        "Linux": "libcrdtsync_ffi.so",
        "Windows": "crdtsync_ffi.dll",
    }.get(platform.system())
    if name is None:
        raise RuntimeError(f"unsupported platform: {platform.system()}")
    directory = os.path.dirname(os.path.abspath(__file__))
    for _ in range(8):
        for profile in ("release", "debug"):
            candidate = os.path.join(directory, "target", profile, name)
            if os.path.exists(candidate):
                return candidate
        directory = os.path.dirname(directory)
    raise RuntimeError(
        "crdtsync native library not found; build `cargo build -p crdtsync-ffi` "
        "or set CRDTSYNC_LIB"
    )


def _bind(lib: ctypes.CDLL) -> ctypes.CDLL:
    c = ctypes
    doc, cbytes, size = c.c_void_p, c.c_char_p, c.c_size_t
    buf = _CrdtBuf

    def sig(fn, argtypes, restype):
        fn.argtypes = argtypes
        fn.restype = restype

    sig(lib.crdtsync_doc_new, [cbytes], doc)
    sig(lib.crdtsync_doc_free, [doc], None)
    sig(lib.crdtsync_buf_free, [buf], None)
    sig(lib.crdtsync_doc_register_int, [doc, cbytes, size, c.c_int64], buf)
    sig(lib.crdtsync_doc_inc, [doc, cbytes, size, c.c_uint32], buf)
    sig(lib.crdtsync_doc_dec, [doc, cbytes, size, c.c_uint32], buf)
    sig(lib.crdtsync_doc_set_bytes, [doc, cbytes, size, cbytes, size], buf)
    sig(lib.crdtsync_doc_set_scalar, [doc, cbytes, size, cbytes, size], buf)
    sig(lib.crdtsync_doc_delete, [doc, cbytes, size], buf)
    sig(lib.crdtsync_doc_get_int, [doc, cbytes, size, c.POINTER(c.c_int64)], c.c_int32)
    sig(lib.crdtsync_doc_get_counter, [doc, cbytes, size, c.POINTER(c.c_int64)], c.c_int32)
    sig(lib.crdtsync_doc_get_bytes, [doc, cbytes, size, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_doc_get_scalar, [doc, cbytes, size, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_doc_map_keys, [doc, cbytes, size, c.POINTER(buf)], c.c_int32)
    sig(
        lib.crdtsync_doc_set_blob,
        [doc, cbytes, size, cbytes, size, cbytes, size, c.POINTER(buf)],
        c.c_int32,
    )
    sig(
        lib.crdtsync_doc_set_blob_ref,
        [doc, cbytes, size, cbytes, cbytes, size, c.c_uint64, c.POINTER(buf)],
        c.c_int32,
    )
    sig(lib.crdtsync_doc_get_blob, [doc, cbytes, size, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_doc_list_insert, [doc, cbytes, size, size, cbytes, size], buf)
    sig(lib.crdtsync_doc_list_delete, [doc, cbytes, size, size], buf)
    sig(lib.crdtsync_doc_list_len, [doc, cbytes, size, c.POINTER(size)], c.c_int32)
    sig(lib.crdtsync_doc_list_get, [doc, cbytes, size, size, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_doc_text_insert, [doc, cbytes, size, size, cbytes, size], buf)
    sig(lib.crdtsync_doc_text_delete, [doc, cbytes, size, size, size], buf)
    sig(lib.crdtsync_doc_text_len, [doc, cbytes, size, c.POINTER(size)], c.c_int32)
    sig(lib.crdtsync_doc_text_get, [doc, cbytes, size, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_doc_relative_position, [doc, cbytes, size, size, c.c_uint32], buf)
    sig(
        lib.crdtsync_doc_resolve_position,
        [doc, cbytes, size, cbytes, size, c.POINTER(size)],
        c.c_int32,
    )
    sig(lib.crdtsync_doc_apply, [doc, cbytes, size], c.c_int32)
    sig(lib.crdtsync_doc_encode_state, [doc], buf)
    sig(lib.crdtsync_doc_decode_state, [cbytes, size], doc)
    sig(lib.crdtsync_doc_begin_atomic, [doc], None)
    sig(lib.crdtsync_doc_commit_atomic, [doc], buf)
    sig(lib.crdtsync_diff, [cbytes, size, cbytes, size], buf)
    sig(lib.crdtsync_diff_decode, [cbytes, size, c.POINTER(buf)], c.c_int32)

    # xml navigation (doc)
    sig(lib.crdtsync_doc_xml_element, [doc, cbytes, size, cbytes, size], buf)
    sig(lib.crdtsync_doc_xml_fragment, [doc, cbytes, size], buf)
    sig(lib.crdtsync_doc_xml_tag, [doc, cbytes, size, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_doc_xml_insert_element, [doc, cbytes, size, size, cbytes, size], buf)
    sig(lib.crdtsync_doc_xml_insert_text, [doc, cbytes, size, size, cbytes, size], buf)
    sig(lib.crdtsync_doc_xml_child_delete, [doc, cbytes, size, size], buf)
    sig(lib.crdtsync_doc_xml_children_len, [doc, cbytes, size, c.POINTER(size)], c.c_int32)
    sig(lib.crdtsync_doc_xml_move, [doc, cbytes, size, size, cbytes, size, size], buf)

    # marks (doc)
    sig(
        lib.crdtsync_doc_mark,
        [doc, cbytes, size, size, c.c_uint32, size, c.c_uint32, cbytes, size, cbytes, size, c.POINTER(buf)],
        buf,
    )
    sig(lib.crdtsync_doc_mark_set_value, [doc, cbytes, size, cbytes, size], buf)
    sig(lib.crdtsync_doc_mark_delete, [doc, cbytes, size], buf)
    sig(lib.crdtsync_doc_marks_at, [doc, cbytes, size, size, c.POINTER(buf)], c.c_int32)

    # acl authoring (doc)
    sig(lib.crdtsync_actor_key, [cbytes, size, c.POINTER(buf)], c.c_int32)
    sig(
        lib.crdtsync_doc_acl_grant,
        [
            doc, c.c_uint32, cbytes, size, c.c_uint32, c.c_uint32, cbytes, size,
            c.c_uint32, cbytes, size, cbytes, size, c.POINTER(buf), c.POINTER(buf),
        ],
        c.c_int32,
    )
    sig(lib.crdtsync_doc_acl_revoke, [doc, cbytes, size, c.POINTER(buf)], c.c_int32)

    # schema + repair (doc)
    sig(lib.crdtsync_doc_set_schema, [doc, cbytes, size], c.c_int32)
    sig(lib.crdtsync_doc_take_repairs, [doc, c.POINTER(buf)], c.c_int32)

    # undo / redo
    undo = c.c_void_p
    sig(lib.crdtsync_undo_new, [], undo)
    sig(lib.crdtsync_undo_free, [undo], None)
    sig(lib.crdtsync_undo_register_int, [undo, doc, cbytes, size, c.c_int64], buf)
    sig(lib.crdtsync_undo_inc, [undo, doc, cbytes, size, c.c_uint32], buf)
    sig(lib.crdtsync_undo_dec, [undo, doc, cbytes, size, c.c_uint32], buf)
    sig(lib.crdtsync_undo_delete, [undo, doc, cbytes, size], buf)
    sig(lib.crdtsync_undo_list_insert, [undo, doc, cbytes, size, size, cbytes, size], buf)
    sig(lib.crdtsync_undo_list_delete, [undo, doc, cbytes, size, size], buf)
    sig(lib.crdtsync_undo_text_insert, [undo, doc, cbytes, size, size, cbytes, size], buf)
    sig(lib.crdtsync_undo_text_delete, [undo, doc, cbytes, size, size, size], buf)
    sig(lib.crdtsync_undo_undo, [undo, doc], buf)
    sig(lib.crdtsync_undo_redo, [undo, doc], buf)
    sig(lib.crdtsync_undo_can_undo, [undo], c.c_int32)
    sig(lib.crdtsync_undo_can_redo, [undo], c.c_int32)

    # wire client session
    ch = c.c_uint32
    sig(lib.crdtsync_client_new, [cbytes], doc)
    sig(lib.crdtsync_client_free, [doc], None)
    sig(lib.crdtsync_client_hello, [doc], buf)
    sig(lib.crdtsync_client_declare_app, [doc, cbytes, size, c.c_uint32], c.c_int32)
    sig(lib.crdtsync_client_active_schema_version, [doc, c.POINTER(c.c_uint32)], c.c_int32)
    sig(lib.crdtsync_client_active_schema, [doc, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_client_auth, [doc, cbytes, size], buf)
    sig(lib.crdtsync_client_actor, [doc, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_client_subscribe, [doc, cbytes, size, c.POINTER(ch)], buf)
    sig(
        lib.crdtsync_client_subscribe_branch,
        [doc, cbytes, size, cbytes, size, c.POINTER(ch)],
        buf,
    )
    sig(
        lib.crdtsync_client_subscribe_zone,
        [doc, cbytes, size, cbytes, size, c.POINTER(ch)],
        buf,
    )
    sig(lib.crdtsync_client_resume, [doc, ch], buf)
    sig(lib.crdtsync_client_resend, [doc, ch], buf)
    sig(lib.crdtsync_client_outbox_len, [doc, ch, c.POINTER(size)], c.c_int32)
    sig(lib.crdtsync_client_unsubscribe, [doc, ch], buf)
    sig(
        lib.crdtsync_client_receive,
        [doc, cbytes, size, c.POINTER(c.c_int32)],
        c.c_int32,
    )
    sig(lib.crdtsync_client_take_rejected, [doc, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_client_take_redirects, [doc, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_client_last_seen_seq, [doc, ch, c.POINTER(c.c_uint64)], c.c_int32)
    sig(lib.crdtsync_client_register_int, [doc, ch, cbytes, size, c.c_int64], buf)
    sig(lib.crdtsync_client_inc, [doc, ch, cbytes, size, c.c_uint32], buf)
    sig(lib.crdtsync_client_dec, [doc, ch, cbytes, size, c.c_uint32], buf)
    sig(lib.crdtsync_client_set_bytes, [doc, ch, cbytes, size, cbytes, size], buf)
    sig(lib.crdtsync_client_set_blob, [doc, ch, cbytes, size, cbytes, size, cbytes, size], buf)
    sig(
        lib.crdtsync_client_set_blob_ref,
        [doc, ch, cbytes, size, cbytes, cbytes, size, c.c_uint64],
        buf,
    )
    sig(lib.crdtsync_client_delete, [doc, ch, cbytes, size], buf)
    # xml navigation (client)
    sig(lib.crdtsync_client_xml_element, [doc, ch, cbytes, size, cbytes, size], buf)
    sig(lib.crdtsync_client_xml_fragment, [doc, ch, cbytes, size], buf)
    sig(lib.crdtsync_client_xml_insert_element, [doc, ch, cbytes, size, size, cbytes, size], buf)
    sig(lib.crdtsync_client_xml_insert_text, [doc, ch, cbytes, size, size, cbytes, size], buf)
    sig(lib.crdtsync_client_xml_child_delete, [doc, ch, cbytes, size, size], buf)
    sig(lib.crdtsync_client_xml_move, [doc, ch, cbytes, size, size, cbytes, size, size], buf)
    # marks (client)
    sig(
        lib.crdtsync_client_mark,
        [doc, ch, cbytes, size, size, c.c_uint32, size, c.c_uint32, cbytes, size, cbytes, size, c.POINTER(buf)],
        buf,
    )
    sig(lib.crdtsync_client_mark_set_value, [doc, ch, cbytes, size, cbytes, size], buf)
    sig(lib.crdtsync_client_mark_delete, [doc, ch, cbytes, size], buf)
    # acl authoring (client)
    sig(
        lib.crdtsync_client_acl_grant,
        [
            doc, ch, c.c_uint32, cbytes, size, c.c_uint32, c.c_uint32, cbytes, size,
            c.c_uint32, cbytes, size, cbytes, size, c.POINTER(buf),
        ],
        buf,
    )
    sig(lib.crdtsync_client_acl_revoke, [doc, ch, cbytes, size], buf)
    sig(lib.crdtsync_client_begin_atomic, [doc, ch], None)
    sig(lib.crdtsync_client_commit_atomic, [doc, ch], buf)
    sig(lib.crdtsync_client_get_int, [doc, ch, cbytes, size, c.POINTER(c.c_int64)], c.c_int32)
    sig(lib.crdtsync_client_get_bytes, [doc, ch, cbytes, size, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_client_set_awareness, [doc, ch, cbytes, size, cbytes, size], buf)
    sig(
        lib.crdtsync_client_awareness,
        [doc, ch, cbytes, size, cbytes, size, c.POINTER(buf)],
        c.c_int32,
    )
    sig(lib.crdtsync_client_awareness_len, [doc, ch, c.POINTER(size)], c.c_int32)
    sig(lib.crdtsync_client_create_version, [doc, ch, cbytes, size], buf)
    sig(lib.crdtsync_client_rename_version, [doc, ch, cbytes, size, cbytes, size], buf)
    sig(lib.crdtsync_client_delete_version, [doc, ch, cbytes, size], buf)
    sig(lib.crdtsync_client_list_versions, [doc, ch], buf)
    sig(lib.crdtsync_client_fetch_version, [doc, ch, cbytes, size], buf)
    sig(lib.crdtsync_client_version_count, [doc, ch, c.POINTER(size)], c.c_int32)
    sig(lib.crdtsync_client_version_name, [doc, ch, size, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_client_version_state, [doc, ch, cbytes, size, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_client_list_branches, [doc, cbytes, size], buf)
    sig(lib.crdtsync_client_fork_branch, [doc, cbytes, size, cbytes, size, cbytes, size], buf)
    sig(
        lib.crdtsync_client_fork_branch_from_version,
        [doc, cbytes, size, cbytes, size, cbytes, size],
        buf,
    )
    sig(lib.crdtsync_client_restore_branch, [doc, cbytes, size, cbytes, size, cbytes, size], buf)
    sig(lib.crdtsync_client_publish_branch, [doc, cbytes, size, cbytes, size], buf)
    sig(lib.crdtsync_client_delete_branch, [doc, cbytes, size, cbytes, size], buf)
    sig(lib.crdtsync_client_branch_count, [doc, cbytes, size, c.POINTER(size)], c.c_int32)
    sig(
        lib.crdtsync_client_branch_at,
        [
            doc,
            cbytes,
            size,
            size,
            c.POINTER(buf),
            c.POINTER(c.c_uint64),
            c.POINTER(c.c_uint64),
            c.POINTER(c.c_int32),
        ],
        c.c_int32,
    )
    sig(
        lib.crdtsync_client_diff_query,
        [doc, cbytes, size, c.c_uint32, cbytes, size, cbytes, size],
        buf,
    )
    sig(lib.crdtsync_client_diff_result, [doc, cbytes, size, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_client_clone_room, [doc, cbytes, size, cbytes, size], buf)
    sig(
        lib.crdtsync_client_clone_result,
        [doc, cbytes, size, c.POINTER(c.c_int32)],
        c.c_int32,
    )
    return lib


_LIB = _bind(ctypes.CDLL(_library_path()))


def encode_path(keys: Path) -> bytes:
    """Encode a path as the C ABI expects: each key a u32 length then its bytes."""
    out = bytearray()
    for key in keys:
        out += struct.pack("<I", len(key))
        out += key
    return bytes(out)


def _u32(name: str, value: int) -> int:
    """Reject values that ctypes would silently wrap into a C `uint32_t`."""
    if not isinstance(value, int) or not 0 <= value <= 0xFFFFFFFF:
        raise ValueError(f"{name} must be an int in 0..=4294967295, got {value!r}")
    return value


_SIZE_T_MAX = (1 << (ctypes.sizeof(ctypes.c_size_t) * 8)) - 1


def _usize(name: str, value: int) -> int:
    """Reject values that ctypes would wrap around C `size_t` (both signs)."""
    if not isinstance(value, int) or not 0 <= value <= _SIZE_T_MAX:
        raise ValueError(f"{name} must be an int in 0..={_SIZE_T_MAX}, got {value!r}")
    return value


def _i64(name: str, value: int) -> int:
    """Reject values that ctypes would silently wrap into a C `int64_t`."""
    if not isinstance(value, int) or not -(2**63) <= value <= 2**63 - 1:
        raise ValueError(f"{name} must fit in a signed 64-bit int, got {value!r}")
    return value


def _take_buf(buf: _CrdtBuf) -> bytes:
    """Copy an owned buffer out and free it."""
    if not buf.ptr:
        return b""
    data = ctypes.string_at(buf.ptr, buf.len)
    _LIB.crdtsync_buf_free(buf)
    return data


def actor_key(actor: bytes) -> bytes:
    """The doc-ACL actor key for a credential ``actor``: the fixed 16-byte SHA-256
    truncation the server keys tuples by. Build an :meth:`Document.acl_grant`
    ``ACTOR`` subject and its ``grantor`` from this so the authenticated actor — not
    an ephemeral per-device id — is the matched ACL principal, identical across
    devices and after a restart."""
    out = _CrdtBuf()
    _LIB.crdtsync_actor_key(actor, len(actor), ctypes.byref(out))
    return _take_buf(out)


_KINDS = ("scalar", "register", "counter", "map", "list", "text", "xmlElement", "xmlFragment")


class _Reader:
    """Reads the change-list byte format the core emits (little-endian)."""

    def __init__(self, data: bytes):
        self._d = data
        self._i = 0

    def _take(self, n: int) -> bytes:
        end = self._i + n
        if end > len(self._d):
            raise ValueError("truncated change list")
        chunk = self._d[self._i : end]
        self._i = end
        return chunk

    def at_end(self) -> bool:
        return self._i >= len(self._d)

    def u8(self) -> int:
        return self._take(1)[0]

    def u32(self) -> int:
        return int.from_bytes(self._take(4), "little")

    def u64(self) -> int:
        return int.from_bytes(self._take(8), "little")

    def i32(self) -> int:
        return int.from_bytes(self._take(4), "little", signed=True)

    def i64(self) -> int:
        return int.from_bytes(self._take(8), "little", signed=True)

    def blob(self) -> bytes:
        return self._take(self.u32())

    def kind(self) -> str:
        tag = self.u8()
        if tag >= len(_KINDS):
            raise ValueError(f"bad element kind {tag}")
        return _KINDS[tag]

    def scalar(self) -> dict:
        """A scalar as a tagged ``{"t", "v"}`` dict, mirroring the wasm shape."""
        start = self._i
        tag = self.u8()
        if tag == 0:
            return {"t": "null"}
        if tag == 1:
            return {"t": "bool", "v": self.u8() != 0}
        if tag == 2:
            return {"t": "int", "v": self.i64()}
        if tag == 3:
            return {"t": "bytes", "v": self.blob()}
        if tag == 4:
            self._take(16)  # id
            self.blob()  # mime
            self.u64()  # size
            if self.u8() == 1:
                self.blob()  # inline bytes
            return {"t": "blobref", "v": self._d[start : self._i]}
        if tag == 5:
            return {"t": "elementRef", "v": self._take(16)}
        raise ValueError(f"bad scalar tag {tag}")

    def items(self) -> list:
        out = []
        for _ in range(self.u32()):
            tag = self.u8()
            if tag == 0:
                out.append({"scalar": self.scalar()})
            elif tag == 1:
                out.append({"kind": self.kind()})
            else:
                raise ValueError(f"bad diff item tag {tag}")
        return out


def _decode_changes(data: bytes) -> list:
    r = _Reader(data)
    out = []
    for _ in range(r.u32()):
        tag = r.u8()
        if tag == 0:
            out.append({"op": "add", "path": r.blob(), "kind": r.kind()})
        elif tag == 1:
            out.append({"op": "remove", "path": r.blob(), "kind": r.kind()})
        elif tag == 2:
            out.append({"op": "value", "path": r.blob(), "old": r.scalar(), "new": r.scalar()})
        elif tag == 3:
            out.append({"op": "counter", "path": r.blob(), "old": r.i64(), "new": r.i64()})
        elif tag == 4:
            out.append({"op": "listInsert", "path": r.blob(), "index": r.u64(), "items": r.items()})
        elif tag == 5:
            out.append({"op": "listDelete", "path": r.blob(), "index": r.u64(), "items": r.items()})
        elif tag == 6:
            out.append(
                {"op": "textInsert", "path": r.blob(), "index": r.u64(), "text": r.blob().decode("utf-8")}
            )
        elif tag == 7:
            out.append(
                {"op": "textDelete", "path": r.blob(), "index": r.u64(), "text": r.blob().decode("utf-8")}
            )
        elif tag == 8:
            out.append(
                {"op": "markAdded", "id": r._take(16), "seq": r._take(16), "name": r.blob(), "value": r.scalar()}
            )
        elif tag == 9:
            out.append(
                {"op": "markRemoved", "id": r._take(16), "seq": r._take(16), "name": r.blob(), "value": r.scalar()}
            )
        elif tag == 10:
            out.append(
                {
                    "op": "markChanged",
                    "id": r._take(16),
                    "seq": r._take(16),
                    "name": r.blob(),
                    "old": r.scalar(),
                    "new": r.scalar(),
                }
            )
        else:
            raise ValueError(f"bad change tag {tag}")
    return out


def _encode_scalar(value) -> bytes:
    """Encode a Python value as the tagged ``Scalar`` bytes the ABI marshals: the
    same tags :meth:`_Reader.scalar` reads back — ``None`` a null, a ``bool`` a
    boolean, an ``int`` a signed 64-bit int, ``bytes`` a byte string."""
    if value is None:
        return b"\x00"
    if isinstance(value, bool):
        return b"\x01" + (b"\x01" if value else b"\x00")
    if isinstance(value, int):
        _i64("value", value)
        return b"\x02" + struct.pack("<q", value)
    if isinstance(value, (bytes, bytearray)):
        b = bytes(value)
        return b"\x03" + struct.pack("<I", len(b)) + b
    raise ValueError(f"unsupported scalar value: {value!r}")


def _decode_blob_ref(data: bytes) -> BlobRef:
    """Decode the ``get_blob`` buffer: the 16-byte id, a ``u32``-length mime, the
    ``u64`` size, then a present flag and, when set, the ``u32``-length inline
    bytes."""
    r = _Reader(data)
    blob_id = r._take(16)
    mime = r.blob().decode("utf-8")
    size = r.u64()
    inline = r.blob() if r.u8() == 1 else None
    return BlobRef(id=blob_id, mime=mime, size=size, inline=inline)


def _decode_marks(data: bytes) -> list:
    """Decode the ``marks_at`` buffer: a ``u32`` count, then per mark a name, a
    flavor tag, and its payload — ``0`` a boolean, ``1`` a scalar value, ``2`` the
    covering element ids. Each mark is a dict with ``name``, ``flavor``, and the
    flavor's field (``value`` or ``ids``)."""
    r = _Reader(data)
    out = []
    for _ in range(r.u32()):
        name = r.blob()
        flavor = r.u8()
        if flavor == 0:
            out.append({"name": name, "flavor": "boolean", "value": r.u8() != 0})
        elif flavor == 1:
            # The value flavor frames its Scalar with a u32 length prefix.
            out.append({"name": name, "flavor": "value", "value": _Reader(r.blob()).scalar()})
        elif flavor == 2:
            out.append({"name": name, "flavor": "object", "ids": [r._take(16) for _ in range(r.u32())]})
        else:
            raise ValueError(f"bad mark flavor {flavor}")
    return out


def _decode_repair_path(data: bytes) -> list:
    """Decode one repair path into its steps: each a ``{"key": bytes}`` map-slot key
    or a ``{"index": int}`` sequence index."""
    r = _Reader(data)
    steps = []
    while not r.at_end():
        tag = r.u8()
        if tag == 0x00:
            steps.append({"key": r.blob()})
        elif tag == 0x01:
            steps.append({"index": r.u64()})
        else:
            raise ValueError(f"bad repair step tag {tag}")
    return steps


def _decode_repair_paths(data: bytes) -> list:
    """Decode the ``take_repairs`` buffer: a ``u32`` count, then per path a
    length-prefixed repair-path byte string, each decoded to its steps."""
    if not data:
        return []
    r = _Reader(data)
    return [_decode_repair_path(r.blob()) for _ in range(r.u32())]


def _decode_rejected(data: bytes) -> List[Rejected]:
    """Decode the ``take_rejected`` buffer: a ``u32`` count, then per batch the
    channel (``u32``), the reason ``ErrorCode`` (``i32``), and the ops — a ``u32``
    op-count then per op a length-prefixed op byte string."""
    if not data:
        return []
    r = _Reader(data)
    out = []
    for _ in range(r.u32()):
        channel = r.u32()
        reason = ErrorCode(r.i32())
        ops = [r.blob() for _ in range(r.u32())]
        out.append(Rejected(channel=channel, reason=reason, ops=ops))
    return out


def _decode_redirects(data: bytes) -> List[Redirect]:
    """Decode the ``take_redirects`` buffer: a ``u32`` count, then per redirect a
    length-prefixed ``room`` byte string and a length-prefixed ``leader_addr``
    byte string."""
    if not data:
        return []
    r = _Reader(data)
    return [Redirect(room=r.blob(), leader_addr=r.blob()) for _ in range(r.u32())]


def _decode_key_list(data: bytes) -> List[bytes]:
    """Decode a ``map_keys`` buffer: a ``u32`` count, then each key a
    ``u32``-length-prefixed byte string."""
    if not data:
        return []
    r = _Reader(data)
    return [r.blob() for _ in range(r.u32())]


def _diff_raw(old_state: bytes, new_state: bytes) -> bytes:
    """The raw encoded change list turning ``old_state`` into ``new_state`` — the
    canonical buffer :func:`diff_decode` reads. Empty on a malformed snapshot."""
    return _take_buf(
        _LIB.crdtsync_diff(old_state, len(old_state), new_state, len(new_state))
    )


def diff(old_state: bytes, new_state: bytes) -> list:
    """Diff two snapshots — each a state buffer from ``Document.encode_state``, a
    named version, or an exported room — into a list of structural change dicts
    turning the old state into the new. Each change has an ``op`` tag, a ``path``
    (bytes), and its variant's fields; a scalar is a tagged ``{"t", "v"}`` dict.
    Raises ``ValueError`` on a malformed snapshot."""
    data = _diff_raw(old_state, new_state)
    if not data:
        raise ValueError("malformed snapshot")
    return _decode_changes(data)


def diff_decode(data: bytes) -> list:
    """Decode a change-list buffer (as produced by the diff over the wire or a
    stored snapshot) into the same structural change dicts :func:`diff` returns —
    the boundary read that validates opaque diff bytes through the core's total
    decoder. Raises ``ValueError`` on a truncated or garbage buffer."""
    out = _CrdtBuf()
    rc = _LIB.crdtsync_diff_decode(data, len(data), ctypes.byref(out))
    if rc != 1:
        raise ValueError("malformed change list")
    return _decode_changes(_take_buf(out))


def upload_blob(
    base_url: str,
    data: bytes,
    credential: bytes,
    mime: str = "application/octet-stream",
) -> bytes:
    """Upload raw bytes to the server's ``POST /blobs`` and return the 16-byte
    blob handle, ready to pass to :meth:`Document.set_blob_ref`.

    ``base_url`` is the origin of the blob plane (e.g. ``"http://host:6060"``);
    the bytes POST to ``{base_url}/blobs``. ``credential`` authenticates through
    the ``Authorization`` header — the same credential the wire client sends in
    :meth:`Client.auth` — and ``mime`` sets ``Content-Type``. Whether upload is
    permitted is whatever ``POST /blobs`` enforces. Raises on a non-2xx response
    or a handle that is not a 16-byte hex id."""
    request = urllib.request.Request(
        base_url.rstrip("/") + "/blobs",
        data=data,
        method="POST",
        headers={
            "Authorization": credential.decode("latin-1"),
            "Content-Type": mime,
        },
    )
    with urllib.request.urlopen(request) as response:
        handle = json.loads(response.read())
    blob_id = bytes.fromhex(handle["id"])
    if len(blob_id) != 16:
        raise ValueError(f"server returned a {len(blob_id)}-byte handle, want 16")
    return blob_id


class Document:
    """A CRDT replica for one client id (16 bytes)."""

    def __init__(self, client_id: bytes):
        if len(client_id) != 16:
            raise ValueError("client_id must be 16 bytes")
        self._handle = _LIB.crdtsync_doc_new(client_id)
        if not self._handle:
            raise RuntimeError("failed to open document")

    def close(self) -> None:
        if getattr(self, "_handle", None):
            _LIB.crdtsync_doc_free(self._handle)
            self._handle = None

    def __enter__(self) -> "Document":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self):
        self.close()

    # --- map / scalar ---

    def register_int(self, path: Path, value: int) -> bytes:
        _i64("value", value)
        p = encode_path(path)
        return _take_buf(_LIB.crdtsync_doc_register_int(self._handle, p, len(p), value))

    def inc(self, path: Path, amount: int) -> bytes:
        _u32("amount", amount)
        p = encode_path(path)
        return _take_buf(_LIB.crdtsync_doc_inc(self._handle, p, len(p), amount))

    def dec(self, path: Path, amount: int) -> bytes:
        _u32("amount", amount)
        p = encode_path(path)
        return _take_buf(_LIB.crdtsync_doc_dec(self._handle, p, len(p), amount))

    def set_bytes(self, path: Path, value: bytes) -> bytes:
        p = encode_path(path)
        return _take_buf(
            _LIB.crdtsync_doc_set_bytes(self._handle, p, len(p), value, len(value))
        )

    def set_scalar(self, path: Path, scalar: bytes) -> bytes:
        """Install-or-set a Register holding any encoded ``Scalar`` at a path — the
        typed-leaf seam the ergonomic handle layer marshals native values through,
        so a leaf keeps its type across a round trip. Returns the ops to broadcast
        (empty on a malformed payload)."""
        p = encode_path(path)
        return _take_buf(
            _LIB.crdtsync_doc_set_scalar(self._handle, p, len(p), scalar, len(scalar))
        )

    def delete(self, path: Path) -> bytes:
        p = encode_path(path)
        return _take_buf(_LIB.crdtsync_doc_delete(self._handle, p, len(p)))

    def get_scalar(self, path: Path) -> Optional[bytes]:
        """The encoded ``Scalar`` bytes of the Register at a path, whatever its type,
        or ``None`` when the slot holds no register. The inverse of
        :meth:`set_scalar`."""
        return self._read_buf(_LIB.crdtsync_doc_get_scalar, path)

    def map_keys(self, path: Path) -> Optional[List[bytes]]:
        """The live slot keys of the Map at a path, or ``None`` when the path is not
        a live Map (an empty path names the root map). An empty map reads back as an
        empty list, distinct from ``None``."""
        p = encode_path(path)
        out = _CrdtBuf()
        rc = _LIB.crdtsync_doc_map_keys(self._handle, p, len(p), ctypes.byref(out))
        return _decode_key_list(_take_buf(out)) if rc == 1 else None

    def get_int(self, path: Path) -> Optional[int]:
        return self._read_i64(_LIB.crdtsync_doc_get_int, path)

    def get_counter(self, path: Path) -> Optional[int]:
        return self._read_i64(_LIB.crdtsync_doc_get_counter, path)

    def get_bytes(self, path: Path) -> Optional[bytes]:
        return self._read_buf(_LIB.crdtsync_doc_get_bytes, path)

    # --- blobs ---

    def set_blob(self, path: Path, mime: str, bytes_: bytes) -> Optional[bytes]:
        """Set an inline blob at a path, minting the blob's public handle. Returns
        the ops to broadcast, or ``None`` when ``bytes_`` exceeds the inline
        ceiling — a large blob is uploaded out of band and set with
        :meth:`set_blob_ref`."""
        p = encode_path(path)
        m = mime.encode("utf-8")
        out = _CrdtBuf()
        rc = _LIB.crdtsync_doc_set_blob(
            self._handle, p, len(p), m, len(m), bytes_, len(bytes_), ctypes.byref(out)
        )
        return _take_buf(out) if rc == 1 else None

    def set_blob_ref(self, path: Path, blob_id: bytes, mime: str, size: int) -> bytes:
        """Set a store-backed blob ref at a path from a 16-byte ``blob_id`` handle,
        ``mime``, and ``size``. Carries no bytes; the content is fetched by id.
        Returns the ops to broadcast."""
        if len(blob_id) != 16:
            raise ValueError("blob id must be 16 bytes")
        if not isinstance(size, int) or not 0 <= size <= 2**64 - 1:
            raise ValueError(f"size must be an int in 0..=2**64-1, got {size!r}")
        p = encode_path(path)
        m = mime.encode("utf-8")
        out = _CrdtBuf()
        rc = _LIB.crdtsync_doc_set_blob_ref(
            self._handle, p, len(p), blob_id, m, len(m), size, ctypes.byref(out)
        )
        return _take_buf(out) if rc == 1 else b""

    def get_blob(self, path: Path) -> Optional[BlobRef]:
        """Read the :class:`BlobRef` at a path, or ``None`` when the slot holds no
        blob ref."""
        raw = self._read_buf(_LIB.crdtsync_doc_get_blob, path)
        return None if raw is None else _decode_blob_ref(raw)

    # --- list ---

    def list_insert(self, path: Path, index: int, value: bytes) -> bytes:
        _usize("index", index)
        p = encode_path(path)
        return _take_buf(
            _LIB.crdtsync_doc_list_insert(self._handle, p, len(p), index, value, len(value))
        )

    def list_delete(self, path: Path, index: int) -> bytes:
        _usize("index", index)
        p = encode_path(path)
        return _take_buf(_LIB.crdtsync_doc_list_delete(self._handle, p, len(p), index))

    def list_len(self, path: Path) -> Optional[int]:
        return self._read_usize(_LIB.crdtsync_doc_list_len, path)

    def list_get(self, path: Path, index: int) -> Optional[bytes]:
        _usize("index", index)
        p = encode_path(path)
        out = _CrdtBuf()
        rc = _LIB.crdtsync_doc_list_get(self._handle, p, len(p), index, ctypes.byref(out))
        return _take_buf(out) if rc == 1 else None

    # --- text ---

    def text_insert(self, path: Path, index: int, text: str) -> bytes:
        _usize("index", index)
        p = encode_path(path)
        s = text.encode("utf-8")
        return _take_buf(
            _LIB.crdtsync_doc_text_insert(self._handle, p, len(p), index, s, len(s))
        )

    def text_delete(self, path: Path, index: int, count: int) -> bytes:
        _usize("index", index)
        _usize("count", count)
        p = encode_path(path)
        return _take_buf(
            _LIB.crdtsync_doc_text_delete(self._handle, p, len(p), index, count)
        )

    def text_len(self, path: Path) -> Optional[int]:
        return self._read_usize(_LIB.crdtsync_doc_text_len, path)

    def text_get(self, path: Path) -> Optional[str]:
        raw = self._read_buf(_LIB.crdtsync_doc_text_get, path)
        return None if raw is None else raw.decode("utf-8")

    # --- xml ---

    def xml_element(self, path: Path, tag: bytes) -> bytes:
        """Install an ``XmlElement`` tagged ``tag`` at a map-slot path; return the
        ops to broadcast (empty on a bad path or a null tag)."""
        p = encode_path(path)
        return _take_buf(
            _LIB.crdtsync_doc_xml_element(self._handle, p, len(p), tag, len(tag))
        )

    def xml_fragment(self, path: Path) -> bytes:
        """Install a tagless ``XmlFragment`` at a map-slot path; return the ops."""
        p = encode_path(path)
        return _take_buf(_LIB.crdtsync_doc_xml_fragment(self._handle, p, len(p)))

    def xml_tag(self, path: Path) -> Optional[bytes]:
        """The tag of the live ``XmlElement`` at ``path``, or ``None`` when absent
        or not a tagged element (a fragment is tagless)."""
        return self._read_buf(_LIB.crdtsync_doc_xml_tag, path)

    def xml_insert_element(self, elem_path: Path, index: int, tag: bytes) -> bytes:
        """Insert a nested ``XmlElement`` child tagged ``tag`` at live ``index`` in
        the children of the node at ``elem_path``; return the ops (empty if inert)."""
        _usize("index", index)
        p = encode_path(elem_path)
        return _take_buf(
            _LIB.crdtsync_doc_xml_insert_element(self._handle, p, len(p), index, tag, len(tag))
        )

    def xml_insert_text(self, elem_path: Path, index: int, text: str) -> bytes:
        """Insert a ``Text``-run child holding ``text`` at live ``index`` in the
        children of the node at ``elem_path``; return the ops (empty if inert)."""
        _usize("index", index)
        p = encode_path(elem_path)
        s = text.encode("utf-8")
        return _take_buf(
            _LIB.crdtsync_doc_xml_insert_text(self._handle, p, len(p), index, s, len(s))
        )

    def xml_child_delete(self, elem_path: Path, index: int) -> bytes:
        """Tombstone the child at live ``index`` in the children of the node at
        ``elem_path``; return the ops (empty if inert)."""
        _usize("index", index)
        p = encode_path(elem_path)
        return _take_buf(
            _LIB.crdtsync_doc_xml_child_delete(self._handle, p, len(p), index)
        )

    def xml_children_len(self, elem_path: Path) -> Optional[int]:
        """The count of live children of the node at ``elem_path``, or ``None`` when
        the path is not a live ``XmlElement`` or ``XmlFragment``."""
        return self._read_usize(_LIB.crdtsync_doc_xml_children_len, elem_path)

    def xml_move(
        self, parent_path: Path, child_index: int, new_parent_path: Path, dest_index: int
    ) -> bytes:
        """Relocate the live child at ``child_index`` under ``parent_path`` to
        ``dest_index`` in the children of ``new_parent_path`` — a Kleppmann tree
        move keeping the child's identity and subtree. Ops (empty if inert)."""
        _usize("child_index", child_index)
        _usize("dest_index", dest_index)
        pp = encode_path(parent_path)
        np = encode_path(new_parent_path)
        return _take_buf(
            _LIB.crdtsync_doc_xml_move(
                self._handle, pp, len(pp), child_index, np, len(np), dest_index
            )
        )

    # --- marks ---

    def mark(
        self,
        seq_path: Path,
        start_index: int,
        start_side: Side,
        end_index: int,
        end_side: Side,
        name: bytes,
        value,
    ) -> Tuple[Optional[bytes], bytes]:
        """Author a named mark over ``[start, end)`` of the sequence at
        ``seq_path``, each endpoint an ``(index, Side)`` pair and ``value`` a
        scalar payload. Returns ``(mark_id, ops)``: the mark's 16-byte id — the
        handle a later :meth:`mark_set_value`/:meth:`mark_delete` names it by — and
        the ops to broadcast. ``mark_id`` is ``None`` and ``ops`` empty when the
        author was inert (a non-sequence path, an unknown side, or a bad value)."""
        return self._mark_encoded(
            seq_path, start_index, start_side, end_index, end_side, name, _encode_scalar(value)
        )

    def _mark_encoded(
        self, seq_path, start_index, start_side, end_index, end_side, name, scalar: bytes
    ) -> Tuple[Optional[bytes], bytes]:
        """Author a mark whose payload is already encoded ``Scalar`` bytes — the seam
        the ergonomic handle layer marshals a native value through (:meth:`mark`
        encodes the value itself)."""
        _usize("start_index", start_index)
        _usize("end_index", end_index)
        _u32("start_side", int(start_side))
        _u32("end_side", int(end_side))
        p = encode_path(seq_path)
        out = _CrdtBuf()
        ops = _take_buf(
            _LIB.crdtsync_doc_mark(
                self._handle,
                p,
                len(p),
                start_index,
                int(start_side),
                end_index,
                int(end_side),
                name,
                len(name),
                scalar,
                len(scalar),
                ctypes.byref(out),
            )
        )
        mark_id = _take_buf(out)
        return (mark_id if mark_id else None), ops

    def mark_set_value(self, mark_id: bytes, value) -> bytes:
        """Change the scalar payload of the mark handle ``mark_id`` to ``value``;
        return the ops (empty if the handle names no live mark or the value is bad)."""
        return self._mark_set_value_encoded(mark_id, _encode_scalar(value))

    def _mark_set_value_encoded(self, mark_id: bytes, scalar: bytes) -> bytes:
        """Change a mark's payload from already-encoded ``Scalar`` bytes."""
        return _take_buf(
            _LIB.crdtsync_doc_mark_set_value(self._handle, mark_id, len(mark_id), scalar, len(scalar))
        )

    def mark_delete(self, mark_id: bytes) -> bytes:
        """Tombstone the mark handle ``mark_id``; return the ops (empty if it names
        no live mark)."""
        return _take_buf(
            _LIB.crdtsync_doc_mark_delete(self._handle, mark_id, len(mark_id))
        )

    # --- acl authoring ---

    def acl_grant(
        self,
        subject_kind: SubjectKind,
        subject: bytes,
        grantor: bytes,
        path: Path = (),
        *,
        capability: Optional[Capability] = None,
        role: Optional[bytes] = None,
        effect: Effect = Effect.ALLOW,
    ) -> Tuple[bytes, bytes]:
        """Grant a doc-level ACL tuple: an allow/deny (``effect``) of ``capability``
        or ``role`` to ``subject`` (a ``SubjectKind`` plus its bytes — a 16-byte
        actor id, a group name, or empty for a class), on ``path``, recorded with the
        authoring actor ``grantor`` (16 bytes). Returns ``(tuple_id, ops)``: the new
        tuple's 16-byte id — the handle a later :meth:`acl_revoke` names it by — and
        the ops to broadcast. Raises ``ValueError`` on a malformed subject/grant/
        grantor."""
        sk, subj, gk, cap, role_b, eff = _acl_grant_args(
            subject_kind, subject, capability, role, effect
        )
        p = encode_path(path)
        grantor = grantor or b""
        out_id = _CrdtBuf()
        out_ops = _CrdtBuf()
        rc = _LIB.crdtsync_doc_acl_grant(
            self._handle,
            sk, subj, len(subj),
            gk, cap, role_b, len(role_b),
            eff, p, len(p),
            grantor, len(grantor),
            ctypes.byref(out_id),
            ctypes.byref(out_ops),
        )
        if rc != 1:
            raise ValueError("malformed acl grant (subject, grant, or grantor)")
        return _take_buf(out_id), _take_buf(out_ops)

    def acl_revoke(self, tuple_id: bytes) -> bytes:
        """Revoke the ACL tuple ``tuple_id`` (16 bytes from :meth:`acl_grant`),
        tombstoning it; return the ops to broadcast (empty when ``tuple_id`` names no
        tuple this replica holds). Raises ``ValueError`` on a malformed id."""
        out_ops = _CrdtBuf()
        rc = _LIB.crdtsync_doc_acl_revoke(
            self._handle, tuple_id, len(tuple_id), ctypes.byref(out_ops)
        )
        if rc < 0:
            raise ValueError("malformed acl tuple id")
        return _take_buf(out_ops)

    def marks_at(self, seq_path: Path, index: int) -> list:
        """The marks active on character ``index`` of the sequence at ``seq_path``,
        each a dict with ``name``, ``flavor`` (``boolean``/``value``/``object``),
        and the flavor's field. Empty for a non-sequence path or an uncovered
        index."""
        _usize("index", index)
        p = encode_path(seq_path)
        out = _CrdtBuf()
        rc = _LIB.crdtsync_doc_marks_at(self._handle, p, len(p), index, ctypes.byref(out))
        return _decode_marks(_take_buf(out)) if rc == 1 else []

    # --- schema + repair ---

    def set_schema(self, schema: bytes) -> bool:
        """Parse schema JSON bytes and bind the schema for ``onRepaired``
        observation. Returns ``True`` when it bound, ``False`` when the bytes are
        not a valid schema. Binding authors nothing; it takes the current state as
        the baseline for :meth:`take_repairs`."""
        return _LIB.crdtsync_doc_set_schema(self._handle, schema, len(schema)) == 1

    def take_repairs(self) -> list:
        """Drain the ``onRepaired`` signal: the located paths whose repaired reading
        newly changed against the bound schema since the last call, each a list of
        steps (``{"key": bytes}`` or ``{"index": int}``). The drain reseeds the
        baseline, so a standing repair reports once."""
        out = _CrdtBuf()
        rc = _LIB.crdtsync_doc_take_repairs(self._handle, ctypes.byref(out))
        return _decode_repair_paths(_take_buf(out)) if rc == 1 else []

    # --- relative positions (anchors) ---

    def relative_position(
        self, path: Path, index: int, side: Side = Side.LEFT
    ) -> Optional[bytes]:
        """Capture a stable position in the List or Text at ``path`` — encoded
        bytes to resolve later with :meth:`resolve_position`. ``None`` for a bad
        or non-sequence path, or an unknown ``side`` (any value other than
        ``LEFT``/``RIGHT``)."""
        _usize("index", index)
        _u32("side", int(side))
        p = encode_path(path)
        data = _take_buf(
            _LIB.crdtsync_doc_relative_position(self._handle, p, len(p), index, int(side))
        )
        return data if data else None

    def resolve_position(self, path: Path, pos: bytes) -> Optional[int]:
        """Resolve a captured position back to a live index in the List or Text
        at ``path``. ``None`` for a non-sequence slot or malformed bytes."""
        p = encode_path(path)
        out = ctypes.c_size_t()
        rc = _LIB.crdtsync_doc_resolve_position(
            self._handle, p, len(p), pos, len(pos), ctypes.byref(out)
        )
        return out.value if rc == 1 else None

    # --- sync ---

    def apply(self, ops: bytes) -> int:
        """Fold a peer's encoded ops in. Returns the number applied, -1 on error."""
        return _LIB.crdtsync_doc_apply(self._handle, ops, len(ops))

    def begin_atomic(self) -> None:
        """Start recording an atomic transaction; edits accumulate until commit."""
        _LIB.crdtsync_doc_begin_atomic(self._handle)

    def commit_atomic(self) -> bytes:
        """Commit the atomic transaction; returns the group's ops to broadcast."""
        return _take_buf(_LIB.crdtsync_doc_commit_atomic(self._handle))

    def encode_state(self) -> bytes:
        """Serialize the whole replica to a canonical snapshot."""
        return _take_buf(_LIB.crdtsync_doc_encode_state(self._handle))

    @classmethod
    def decode_state(cls, state: bytes) -> "Document":
        """Open a document from a snapshot produced by :meth:`encode_state`."""
        obj = cls.__new__(cls)
        obj._handle = _LIB.crdtsync_doc_decode_state(state, len(state))
        if not obj._handle:
            raise ValueError("failed to decode document snapshot")
        return obj

    # --- helpers ---

    def _read_i64(self, fn, path: Path) -> Optional[int]:
        p = encode_path(path)
        out = ctypes.c_int64()
        rc = fn(self._handle, p, len(p), ctypes.byref(out))
        return out.value if rc == 1 else None

    def _read_usize(self, fn, path: Path) -> Optional[int]:
        p = encode_path(path)
        out = ctypes.c_size_t()
        rc = fn(self._handle, p, len(p), ctypes.byref(out))
        return out.value if rc == 1 else None

    def _read_buf(self, fn, path: Path) -> Optional[bytes]:
        p = encode_path(path)
        out = _CrdtBuf()
        rc = fn(self._handle, p, len(p), ctypes.byref(out))
        return _take_buf(out) if rc == 1 else None


class Undo:
    """A per-user undo/redo manager over a :class:`Document`.

    Each edit made through the manager records its inverse; :meth:`undo` and
    :meth:`redo` emit ordinary ops that converge on peers like any edit. The
    manager is separate from the document it drives, so every call names the
    document.
    """

    def __init__(self):
        self._handle = _LIB.crdtsync_undo_new()
        if not self._handle:
            raise RuntimeError("failed to open undo manager")

    def close(self) -> None:
        if getattr(self, "_handle", None):
            _LIB.crdtsync_undo_free(self._handle)
            self._handle = None

    def __enter__(self) -> "Undo":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self):
        self.close()

    def register_int(self, doc: "Document", path: Path, value: int) -> bytes:
        _i64("value", value)
        p = encode_path(path)
        return _take_buf(
            _LIB.crdtsync_undo_register_int(self._handle, doc._handle, p, len(p), value)
        )

    def inc(self, doc: "Document", path: Path, amount: int) -> bytes:
        _u32("amount", amount)
        p = encode_path(path)
        return _take_buf(_LIB.crdtsync_undo_inc(self._handle, doc._handle, p, len(p), amount))

    def dec(self, doc: "Document", path: Path, amount: int) -> bytes:
        _u32("amount", amount)
        p = encode_path(path)
        return _take_buf(_LIB.crdtsync_undo_dec(self._handle, doc._handle, p, len(p), amount))

    def delete(self, doc: "Document", path: Path) -> bytes:
        p = encode_path(path)
        return _take_buf(_LIB.crdtsync_undo_delete(self._handle, doc._handle, p, len(p)))

    def list_insert(self, doc: "Document", path: Path, index: int, value: bytes) -> bytes:
        _usize("index", index)
        p = encode_path(path)
        return _take_buf(
            _LIB.crdtsync_undo_list_insert(
                self._handle, doc._handle, p, len(p), index, value, len(value)
            )
        )

    def list_delete(self, doc: "Document", path: Path, index: int) -> bytes:
        _usize("index", index)
        p = encode_path(path)
        return _take_buf(
            _LIB.crdtsync_undo_list_delete(self._handle, doc._handle, p, len(p), index)
        )

    def text_insert(self, doc: "Document", path: Path, index: int, text: str) -> bytes:
        _usize("index", index)
        p = encode_path(path)
        s = text.encode("utf-8")
        return _take_buf(
            _LIB.crdtsync_undo_text_insert(
                self._handle, doc._handle, p, len(p), index, s, len(s)
            )
        )

    def text_delete(self, doc: "Document", path: Path, index: int, count: int) -> bytes:
        _usize("index", index)
        _usize("count", count)
        p = encode_path(path)
        return _take_buf(
            _LIB.crdtsync_undo_text_delete(
                self._handle, doc._handle, p, len(p), index, count
            )
        )

    def undo(self, doc: "Document") -> bytes:
        """Revert the most recent intention; returns the ops (empty if none)."""
        return _take_buf(_LIB.crdtsync_undo_undo(self._handle, doc._handle))

    def redo(self, doc: "Document") -> bytes:
        """Replay the most recently undone intention; returns the ops (empty if none)."""
        return _take_buf(_LIB.crdtsync_undo_redo(self._handle, doc._handle))

    def can_undo(self) -> bool:
        return _LIB.crdtsync_undo_can_undo(self._handle) == 1

    def can_redo(self) -> bool:
        return _LIB.crdtsync_undo_can_redo(self._handle) == 1


class Client:
    """A wire client session for one client id (16 bytes).

    It holds a replica per subscribed room and turns local edits into wire
    frames to send; :meth:`receive` folds a peer's frame back in. A room is
    addressed by the ``channel`` returned from :meth:`subscribe`.
    """

    def __init__(self, client_id: bytes):
        if len(client_id) != 16:
            raise ValueError("client_id must be 16 bytes")
        self._handle = _LIB.crdtsync_client_new(client_id)
        if not self._handle:
            raise RuntimeError("failed to open client")

    def close(self) -> None:
        if getattr(self, "_handle", None):
            _LIB.crdtsync_client_free(self._handle)
            self._handle = None

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self):
        self.close()

    # --- handshake ---

    def declare_app(self, app_id: bytes, schema_version: int) -> None:
        """Declare the app this client speaks for and the schema version it
        targets, carried in the next :meth:`hello`. An empty ``app_id`` opens a
        relay connection; a named app with ``schema_version`` 0 is a dynamic
        client that adopts the server's head. Call before :meth:`hello`."""
        _LIB.crdtsync_client_declare_app(
            self._handle, app_id, len(app_id), schema_version
        )

    def active_schema_version(self) -> Optional[int]:
        """The concrete schema version the enforcing server advertised for this
        session, or ``None`` before any advertisement. Distinct from the version
        declared in :meth:`declare_app`: a dynamic client (declared 0) learns the
        served version here. The app persists it across restart itself."""
        out = ctypes.c_uint32()
        rc = _LIB.crdtsync_client_active_schema_version(self._handle, ctypes.byref(out))
        return out.value if rc == 1 else None

    def active_schema(self) -> Optional[bytes]:
        """The bytes of the schema the enforcing server advertised for this
        session (possibly empty), or ``None`` before any advertisement. Pairs
        with :meth:`active_schema_version`."""
        out = _CrdtBuf()
        rc = _LIB.crdtsync_client_active_schema(self._handle, ctypes.byref(out))
        return _take_buf(out) if rc == 1 else None

    def hello(self) -> bytes:
        """The opening Hello frame to send, naming this client."""
        return _take_buf(_LIB.crdtsync_client_hello(self._handle))

    def auth(self, credential: bytes) -> bytes:
        """The Auth frame asking the server to verify ``credential``."""
        return _take_buf(
            _LIB.crdtsync_client_auth(self._handle, credential, len(credential))
        )

    def actor(self) -> Optional[bytes]:
        """The server-derived actor, or ``None`` before AuthOk has arrived."""
        out = _CrdtBuf()
        rc = _LIB.crdtsync_client_actor(self._handle, ctypes.byref(out))
        return _take_buf(out) if rc == 1 else None

    # --- subscription lifecycle ---

    def subscribe(self, room: bytes) -> Tuple[int, bytes]:
        """Join ``room`` on a fresh channel; return ``(channel, subscribe_frame)``."""
        channel = ctypes.c_uint32()
        frame = _take_buf(
            _LIB.crdtsync_client_subscribe(
                self._handle, room, len(room), ctypes.byref(channel)
            )
        )
        return channel.value, frame

    def subscribe_branch(self, room: bytes, branch: bytes) -> Tuple[int, bytes]:
        """Join ``branch`` of ``room`` on a fresh channel; return
        ``(channel, subscribe_frame)``. An empty ``branch`` is the default/active
        branch, matching :meth:`subscribe`."""
        channel = ctypes.c_uint32()
        frame = _take_buf(
            _LIB.crdtsync_client_subscribe_branch(
                self._handle, room, len(room), branch, len(branch), ctypes.byref(channel)
            )
        )
        return channel.value, frame

    def subscribe_zone(self, room: bytes, zone: bytes) -> Tuple[int, bytes]:
        """Join ``room`` on a fresh channel scoped to one ``zone``; return
        ``(channel, subscribe_frame)``. An empty ``zone`` is the whole room (every
        zone the actor may read), matching :meth:`subscribe`; a named ``zone``
        narrows the stream to that partition plus the unzoned root it is entitled
        to. Scoped to the default branch."""
        channel = ctypes.c_uint32()
        frame = _take_buf(
            _LIB.crdtsync_client_subscribe_zone(
                self._handle, room, len(room), zone, len(zone), ctypes.byref(channel)
            )
        )
        return channel.value, frame

    def resume(self, channel: int) -> bytes:
        """Re-issue Subscribe for a held channel from its caught-up position."""
        _u32("channel", channel)
        return _take_buf(_LIB.crdtsync_client_resume(self._handle, channel))

    def resend(self, channel: int) -> bytes:
        """Re-emit the unacknowledged authored ops on ``channel`` as one Ops
        frame to replay after a reconnect; empty when nothing is outstanding."""
        _u32("channel", channel)
        return _take_buf(_LIB.crdtsync_client_resend(self._handle, channel))

    def outbox_len(self, channel: int) -> int:
        """How many authored ops on ``channel`` await acknowledgement."""
        _u32("channel", channel)
        out = ctypes.c_size_t()
        rc = _LIB.crdtsync_client_outbox_len(self._handle, channel, ctypes.byref(out))
        return out.value if rc == 1 else 0

    def unsubscribe(self, channel: int) -> bytes:
        """Leave ``channel``'s room, dropping its replica; return the frame."""
        _u32("channel", channel)
        return _take_buf(_LIB.crdtsync_client_unsubscribe(self._handle, channel))

    def receive(self, msg: bytes) -> int:
        """Fold one received wire frame in. 1 applied, 0 refused, -1 bad handle.
        Raises :class:`ServerError` when the frame is a server ``Error`` — read its
        ``.code``, ``ErrorCode.UPDATE_REQUIRED`` being the ``onUpdateRequired``
        signal."""
        code = ctypes.c_int32(-1)
        rc = _LIB.crdtsync_client_receive(
            self._handle, msg, len(msg), ctypes.byref(code)
        )
        if rc == 0 and code.value >= 0:
            raise ServerError(ErrorCode(code.value))
        return rc

    def take_rejected(self) -> List[Rejected]:
        """Drain the op batches the server refused since the last call — the
        ``onOpsRejected`` observation. Each :class:`Rejected` names the channel, the
        :class:`ErrorCode` reason, and the refused ops (their bytes, to show,
        discard, or export). Draining, so a second call is empty."""
        out = _CrdtBuf()
        rc = _LIB.crdtsync_client_take_rejected(self._handle, ctypes.byref(out))
        return _decode_rejected(_take_buf(out)) if rc == 1 else []

    def take_redirects(self) -> List[Redirect]:
        """Drain the room redirects the server has sent since the last call — a
        node that does not lead a room reporting the leader's address. Each
        :class:`Redirect` names the ``room`` and the leader's ``leader_addr``;
        reconnecting is the app's job. Draining, so a second call is empty."""
        out = _CrdtBuf()
        rc = _LIB.crdtsync_client_take_redirects(self._handle, ctypes.byref(out))
        return _decode_redirects(_take_buf(out)) if rc == 1 else []

    def last_seen_seq(self, channel: int) -> Optional[int]:
        """The highest server sequence ``channel`` has caught up to."""
        _u32("channel", channel)
        out = ctypes.c_uint64()
        rc = _LIB.crdtsync_client_last_seen_seq(self._handle, channel, ctypes.byref(out))
        return out.value if rc == 1 else None

    # --- per-channel edits ---

    def register_int(self, channel: int, path: Path, value: int) -> bytes:
        _u32("channel", channel)
        _i64("value", value)
        p = encode_path(path)
        return _take_buf(
            _LIB.crdtsync_client_register_int(self._handle, channel, p, len(p), value)
        )

    def inc(self, channel: int, path: Path, amount: int) -> bytes:
        _u32("channel", channel)
        _u32("amount", amount)
        p = encode_path(path)
        return _take_buf(_LIB.crdtsync_client_inc(self._handle, channel, p, len(p), amount))

    def dec(self, channel: int, path: Path, amount: int) -> bytes:
        _u32("channel", channel)
        _u32("amount", amount)
        p = encode_path(path)
        return _take_buf(_LIB.crdtsync_client_dec(self._handle, channel, p, len(p), amount))

    def set_bytes(self, channel: int, path: Path, value: bytes) -> bytes:
        _u32("channel", channel)
        p = encode_path(path)
        return _take_buf(
            _LIB.crdtsync_client_set_bytes(self._handle, channel, p, len(p), value, len(value))
        )

    def delete(self, channel: int, path: Path) -> bytes:
        _u32("channel", channel)
        p = encode_path(path)
        return _take_buf(_LIB.crdtsync_client_delete(self._handle, channel, p, len(p)))

    # --- per-channel blobs ---

    def set_blob(self, channel: int, path: Path, mime: str, bytes_: bytes) -> bytes:
        """Set an inline blob at a path in ``channel``'s room, routed through the
        outbox. Returns the Ops frame to send; a ``bytes_`` length over the inline
        ceiling enqueues no op (use :meth:`set_blob_ref` for a large blob)."""
        _u32("channel", channel)
        p = encode_path(path)
        m = mime.encode("utf-8")
        return _take_buf(
            _LIB.crdtsync_client_set_blob(
                self._handle, channel, p, len(p), m, len(m), bytes_, len(bytes_)
            )
        )

    def set_blob_ref(self, channel: int, path: Path, blob_id: bytes, mime: str, size: int) -> bytes:
        """Set a store-backed blob ref at a path in ``channel``'s room from a
        16-byte ``blob_id`` handle, ``mime``, and ``size``, routed through the
        outbox. Returns the Ops frame to send."""
        _u32("channel", channel)
        if len(blob_id) != 16:
            raise ValueError("blob id must be 16 bytes")
        if not isinstance(size, int) or not 0 <= size <= 2**64 - 1:
            raise ValueError(f"size must be an int in 0..=2**64-1, got {size!r}")
        p = encode_path(path)
        m = mime.encode("utf-8")
        return _take_buf(
            _LIB.crdtsync_client_set_blob_ref(
                self._handle, channel, p, len(p), blob_id, m, len(m), size
            )
        )

    # --- per-channel xml ---

    def xml_element(self, channel: int, path: Path, tag: bytes) -> bytes:
        """Install an ``XmlElement`` tagged ``tag`` at a path in ``channel``'s room;
        return the Ops frame to send."""
        _u32("channel", channel)
        p = encode_path(path)
        return _take_buf(
            _LIB.crdtsync_client_xml_element(self._handle, channel, p, len(p), tag, len(tag))
        )

    def xml_fragment(self, channel: int, path: Path) -> bytes:
        """Install a tagless ``XmlFragment`` at a path in ``channel``'s room; return
        the Ops frame."""
        _u32("channel", channel)
        p = encode_path(path)
        return _take_buf(
            _LIB.crdtsync_client_xml_fragment(self._handle, channel, p, len(p))
        )

    def xml_insert_element(self, channel: int, elem_path: Path, index: int, tag: bytes) -> bytes:
        """Insert a nested ``XmlElement`` child tagged ``tag`` at live ``index`` in
        the children of the node at ``elem_path`` in ``channel``'s room; Ops frame."""
        _u32("channel", channel)
        _usize("index", index)
        p = encode_path(elem_path)
        return _take_buf(
            _LIB.crdtsync_client_xml_insert_element(
                self._handle, channel, p, len(p), index, tag, len(tag)
            )
        )

    def xml_insert_text(self, channel: int, elem_path: Path, index: int, text: str) -> bytes:
        """Insert a ``Text``-run child holding ``text`` at live ``index`` in the
        children of the node at ``elem_path`` in ``channel``'s room; Ops frame."""
        _u32("channel", channel)
        _usize("index", index)
        p = encode_path(elem_path)
        s = text.encode("utf-8")
        return _take_buf(
            _LIB.crdtsync_client_xml_insert_text(
                self._handle, channel, p, len(p), index, s, len(s)
            )
        )

    def xml_child_delete(self, channel: int, elem_path: Path, index: int) -> bytes:
        """Tombstone the child at live ``index`` in the children of the node at
        ``elem_path`` in ``channel``'s room; Ops frame."""
        _u32("channel", channel)
        _usize("index", index)
        p = encode_path(elem_path)
        return _take_buf(
            _LIB.crdtsync_client_xml_child_delete(self._handle, channel, p, len(p), index)
        )

    def xml_move(
        self,
        channel: int,
        parent_path: Path,
        child_index: int,
        new_parent_path: Path,
        dest_index: int,
    ) -> bytes:
        """Relocate the live child at ``child_index`` under ``parent_path`` to
        ``dest_index`` in the children of ``new_parent_path`` in ``channel``'s room —
        the tree move routed through the outbox; Ops frame."""
        _u32("channel", channel)
        _usize("child_index", child_index)
        _usize("dest_index", dest_index)
        pp = encode_path(parent_path)
        np = encode_path(new_parent_path)
        return _take_buf(
            _LIB.crdtsync_client_xml_move(
                self._handle, channel, pp, len(pp), child_index, np, len(np), dest_index
            )
        )

    # --- per-channel marks ---

    def mark(
        self,
        channel: int,
        seq_path: Path,
        start_index: int,
        start_side: Side,
        end_index: int,
        end_side: Side,
        name: bytes,
        value,
    ) -> Tuple[Optional[bytes], bytes]:
        """Author a named mark over ``[start, end)`` of the sequence at ``seq_path``
        in ``channel``'s room, routed through the outbox. Returns
        ``(mark_id, frame)``: the mark's 16-byte id and the Ops frame to send.
        ``mark_id`` is ``None`` and ``frame`` empty when the author was inert."""
        _u32("channel", channel)
        _usize("start_index", start_index)
        _usize("end_index", end_index)
        _u32("start_side", int(start_side))
        _u32("end_side", int(end_side))
        p = encode_path(seq_path)
        v = _encode_scalar(value)
        out = _CrdtBuf()
        frame = _take_buf(
            _LIB.crdtsync_client_mark(
                self._handle,
                channel,
                p,
                len(p),
                start_index,
                int(start_side),
                end_index,
                int(end_side),
                name,
                len(name),
                v,
                len(v),
                ctypes.byref(out),
            )
        )
        mark_id = _take_buf(out)
        return (mark_id if mark_id else None), frame

    def mark_set_value(self, channel: int, mark_id: bytes, value) -> bytes:
        """Change the payload of the mark handle ``mark_id`` to ``value`` in
        ``channel``'s room; Ops frame (empty if inert)."""
        _u32("channel", channel)
        v = _encode_scalar(value)
        return _take_buf(
            _LIB.crdtsync_client_mark_set_value(
                self._handle, channel, mark_id, len(mark_id), v, len(v)
            )
        )

    def mark_delete(self, channel: int, mark_id: bytes) -> bytes:
        """Tombstone the mark handle ``mark_id`` in ``channel``'s room; Ops frame
        (empty if it names no live mark)."""
        _u32("channel", channel)
        return _take_buf(
            _LIB.crdtsync_client_mark_delete(self._handle, channel, mark_id, len(mark_id))
        )

    # --- per-channel acl authoring ---

    def acl_grant(
        self,
        channel: int,
        subject_kind: SubjectKind,
        subject: bytes,
        grantor: bytes,
        path: Path = (),
        *,
        capability: Optional[Capability] = None,
        role: Optional[bytes] = None,
        effect: Effect = Effect.ALLOW,
    ) -> Tuple[Optional[bytes], bytes]:
        """Grant a doc-level ACL tuple in ``channel``'s room, routed through the
        outbox. Same fields as :meth:`Document.acl_grant`. Returns
        ``(tuple_id, frame)``: the new tuple's 16-byte id and the Ops frame to send.
        ``tuple_id`` is ``None`` and ``frame`` empty when the channel isn't held."""
        _u32("channel", channel)
        sk, subj, gk, cap, role_b, eff = _acl_grant_args(
            subject_kind, subject, capability, role, effect
        )
        p = encode_path(path)
        grantor = grantor or b""
        out_id = _CrdtBuf()
        frame = _take_buf(
            _LIB.crdtsync_client_acl_grant(
                self._handle,
                channel,
                sk, subj, len(subj),
                gk, cap, role_b, len(role_b),
                eff, p, len(p),
                grantor, len(grantor),
                ctypes.byref(out_id),
            )
        )
        tuple_id = _take_buf(out_id)
        return (tuple_id if tuple_id else None), frame

    def acl_revoke(self, channel: int, tuple_id: bytes) -> bytes:
        """Revoke the ACL tuple ``tuple_id`` in ``channel``'s room, routed through the
        outbox; Ops frame (empty when the channel isn't held or the id names no live
        tuple)."""
        _u32("channel", channel)
        return _take_buf(
            _LIB.crdtsync_client_acl_revoke(self._handle, channel, tuple_id, len(tuple_id))
        )

    def begin_atomic(self, channel: int) -> None:
        """Start an atomic transaction on ``channel``; edits accumulate until commit."""
        _u32("channel", channel)
        _LIB.crdtsync_client_begin_atomic(self._handle, channel)

    def commit_atomic(self, channel: int) -> bytes:
        """Commit the atomic transaction on ``channel``; returns the Ops frame to send."""
        _u32("channel", channel)
        return _take_buf(_LIB.crdtsync_client_commit_atomic(self._handle, channel))

    # --- per-channel reads ---

    def get_int(self, channel: int, path: Path) -> Optional[int]:
        _u32("channel", channel)
        p = encode_path(path)
        out = ctypes.c_int64()
        rc = _LIB.crdtsync_client_get_int(self._handle, channel, p, len(p), ctypes.byref(out))
        return out.value if rc == 1 else None

    def get_bytes(self, channel: int, path: Path) -> Optional[bytes]:
        _u32("channel", channel)
        p = encode_path(path)
        out = _CrdtBuf()
        rc = _LIB.crdtsync_client_get_bytes(self._handle, channel, p, len(p), ctypes.byref(out))
        return _take_buf(out) if rc == 1 else None

    # --- awareness ---

    def set_awareness(self, channel: int, key: bytes, value: bytes) -> bytes:
        """Publish an ephemeral awareness entry ``key``; return the frame to send."""
        _u32("channel", channel)
        return _take_buf(
            _LIB.crdtsync_client_set_awareness(
                self._handle, channel, key, len(key), value, len(value)
            )
        )

    def awareness(self, channel: int, actor: bytes, key: bytes) -> Optional[bytes]:
        """A peer's awareness entry on ``channel`` by publishing ``actor`` and ``key``."""
        _u32("channel", channel)
        out = _CrdtBuf()
        rc = _LIB.crdtsync_client_awareness(
            self._handle, channel, actor, len(actor), key, len(key), ctypes.byref(out)
        )
        return _take_buf(out) if rc == 1 else None

    def awareness_len(self, channel: int) -> int:
        """How many awareness entries ``channel`` currently holds."""
        _u32("channel", channel)
        out = ctypes.c_size_t()
        rc = _LIB.crdtsync_client_awareness_len(self._handle, channel, ctypes.byref(out))
        return out.value if rc == 1 else 0

    # --- named versions ---

    def create_version(self, channel: int, name: bytes) -> bytes:
        """Frame a request to capture ``channel``'s room as version ``name``."""
        _u32("channel", channel)
        return _take_buf(
            _LIB.crdtsync_client_create_version(self._handle, channel, name, len(name))
        )

    def rename_version(self, channel: int, frm: bytes, to: bytes) -> bytes:
        """Frame a request to rename version ``frm`` to ``to``."""
        _u32("channel", channel)
        return _take_buf(
            _LIB.crdtsync_client_rename_version(
                self._handle, channel, frm, len(frm), to, len(to)
            )
        )

    def delete_version(self, channel: int, name: bytes) -> bytes:
        """Frame a request to delete version ``name``."""
        _u32("channel", channel)
        return _take_buf(
            _LIB.crdtsync_client_delete_version(self._handle, channel, name, len(name))
        )

    def list_versions(self, channel: int) -> bytes:
        """Frame a request for ``channel``'s room's version names."""
        _u32("channel", channel)
        return _take_buf(_LIB.crdtsync_client_list_versions(self._handle, channel))

    def fetch_version(self, channel: int, name: bytes) -> bytes:
        """Frame a request for the captured state of version ``name``."""
        _u32("channel", channel)
        return _take_buf(
            _LIB.crdtsync_client_fetch_version(self._handle, channel, name, len(name))
        )

    def versions(self, channel: int) -> List[bytes]:
        """The version names last reported for ``channel``'s room, in order."""
        _u32("channel", channel)
        count = ctypes.c_size_t()
        rc = _LIB.crdtsync_client_version_count(self._handle, channel, ctypes.byref(count))
        if rc != 1:
            return []
        out = []
        for i in range(count.value):
            buf = _CrdtBuf()
            got = _LIB.crdtsync_client_version_name(self._handle, channel, i, ctypes.byref(buf))
            if got == 1:
                out.append(_take_buf(buf))
        return out

    def version_state(self, channel: int, name: bytes) -> Optional[bytes]:
        """The captured state of a fetched version ``name``, once it has arrived."""
        _u32("channel", channel)
        out = _CrdtBuf()
        rc = _LIB.crdtsync_client_version_state(
            self._handle, channel, name, len(name), ctypes.byref(out)
        )
        return _take_buf(out) if rc == 1 else None

    # --- branch management ---

    def list_branches(self, room: bytes) -> bytes:
        """Frame a request for ``room``'s branches. Room-keyed: a client may
        enumerate a room's branches before it subscribes any of them."""
        return _take_buf(_LIB.crdtsync_client_list_branches(self._handle, room, len(room)))

    def fork_branch(self, room: bytes, name: bytes, frm: bytes) -> bytes:
        """Frame a request to fork branch ``name`` off ``frm``'s HEAD in ``room``."""
        return _take_buf(
            _LIB.crdtsync_client_fork_branch(
                self._handle, room, len(room), name, len(name), frm, len(frm)
            )
        )

    def fork_branch_from_version(self, room: bytes, name: bytes, version: bytes) -> bytes:
        """Frame a request to fork branch ``name`` off the snapshot of ``version``."""
        return _take_buf(
            _LIB.crdtsync_client_fork_branch_from_version(
                self._handle, room, len(room), name, len(name), version, len(version)
            )
        )

    def restore_branch(self, room: bytes, name: bytes, version: bytes) -> bytes:
        """Frame a request to restore ``room`` to ``version`` as a fresh branch
        ``name``, switching the active HEAD to it."""
        return _take_buf(
            _LIB.crdtsync_client_restore_branch(
                self._handle, room, len(room), name, len(name), version, len(version)
            )
        )

    def publish_branch(self, room: bytes, published: bytes) -> bytes:
        """Frame a request to publish ``room``'s active editor branch onto the
        read-only ``published`` branch."""
        return _take_buf(
            _LIB.crdtsync_client_publish_branch(
                self._handle, room, len(room), published, len(published)
            )
        )

    def delete_branch(self, room: bytes, name: bytes) -> bytes:
        """Frame a request to delete branch ``name`` of ``room``. The default
        ``main`` is never deletable."""
        return _take_buf(
            _LIB.crdtsync_client_delete_branch(
                self._handle, room, len(room), name, len(name)
            )
        )

    def branches(self, room: bytes) -> List[Branch]:
        """The branch set last reported for ``room``, in order."""
        count = ctypes.c_size_t()
        rc = _LIB.crdtsync_client_branch_count(
            self._handle, room, len(room), ctypes.byref(count)
        )
        if rc != 1:
            return []
        out: List[Branch] = []
        for i in range(count.value):
            name = _CrdtBuf()
            fork_point = ctypes.c_uint64()
            head = ctypes.c_uint64()
            published = ctypes.c_int32()
            got = _LIB.crdtsync_client_branch_at(
                self._handle,
                room,
                len(room),
                i,
                ctypes.byref(name),
                ctypes.byref(fork_point),
                ctypes.byref(head),
                ctypes.byref(published),
            )
            if got == 1:
                out.append(
                    Branch(
                        name=_take_buf(name),
                        fork_point=fork_point.value,
                        head=head.value,
                        published=published.value == 1,
                    )
                )
        return out

    def diff_query(
        self, room: bytes, kind: DiffKind, a: bytes, b: bytes
    ) -> bytes:
        """Frame a request for the structural diff turning state ``a`` into state
        ``b`` in ``room``. ``kind`` selects whether ``a``/``b`` name two saved
        versions or two branches. Room-keyed: a client may diff a room before it
        subscribes any of its branches. The reply updates the diff view, read with
        :meth:`diff`."""
        return _take_buf(
            _LIB.crdtsync_client_diff_query(
                self._handle, room, len(room), int(kind), a, len(a), b, len(b)
            )
        )

    def diff(self, room: bytes) -> Optional[list]:
        """The change list from the last diff query answered for ``room``, or
        ``None`` if none has been. An empty diff is an empty list, not ``None``."""
        out = _CrdtBuf()
        rc = _LIB.crdtsync_client_diff_result(
            self._handle, room, len(room), ctypes.byref(out)
        )
        if rc != 1:
            return None
        return _decode_changes(_take_buf(out))

    def clone_room(self, src: bytes, dst: bytes) -> bytes:
        """Frame a request to duplicate room ``src``'s live state into a fresh room
        ``dst``. Room-keyed: a client may clone a room before it subscribes any of
        it. The reply updates the clone-result view, read with
        :meth:`clone_result`."""
        return _take_buf(
            _LIB.crdtsync_client_clone_room(
                self._handle, src, len(src), dst, len(dst)
            )
        )

    def clone_result(self, dst: bytes) -> Optional[bool]:
        """Whether the last clone answered for destination ``dst`` created it, or
        ``None`` if none has been answered. ``False`` when the clone was a no-op
        (source unknown or ``dst`` already existed)."""
        created = ctypes.c_int32()
        rc = _LIB.crdtsync_client_clone_result(
            self._handle, dst, len(dst), ctypes.byref(created)
        )
        if rc != 1:
            return None
        return created.value == 1


# --- ergonomic handle-graph layer ---------------------------------------------
#
# A `Doc` is a local replica with a single root map, edited through live typed
# handles (`get_map`/`get_list`/`get_text`) rather than byte-paths. A handle owns
# its logical path (a sequence of ergonomic keys) and re-resolves it on every
# operation, so it stays valid as the document mutates and converges — a view,
# never a cached pointer. Handles compose. The byte-path core (`Document`) stays
# available as the low-level power-user surface; this layer marshals native values
# and hides paths/ops on top of it.
#
# Native value marshaling matches the JS boundary exactly (the pinned cross-SDK
# contract): `str` <-> Scalar::Bytes (utf-8), `int` <-> Scalar::Int, `bool` <->
# Scalar::Bool, `None` <-> Scalar::Null, `bytes` <-> Scalar::Bytes (raw). A leaf is
# written with an explicit native scalar; a container is created only with an
# explicit `get_map`/`get_list`/`get_text` accessor — passing a dict/list to `set`
# is a `TypeError`, never an implicit subtree (Automerge-style deep-seed is a
# rejected non-goal). `str` and `bytes` both land in Scalar::Bytes, which the core
# cannot itself tell apart, so the SDK prefixes the payload with a one-byte
# discriminator (string vs binary) — an SDK framing detail, invisible to the value
# the caller reads back.

_BINARY = 0x00
_STRING = 0x01

_I64_MIN = -(2**63)
_I64_MAX = 2**63 - 1


def _key_bytes(key: Key) -> bytes:
    if isinstance(key, str):
        return key.encode("utf-8")
    if isinstance(key, (bytes, bytearray)):
        return bytes(key)
    raise TypeError(f"key must be str or bytes, got {type(key).__name__}")


def _key_string(key: bytes) -> str:
    """A best-effort utf-8 rendering of a slot key (a binary key's value is still
    read by its raw bytes, so nothing is lost)."""
    return key.decode("utf-8", "replace")


def _encode_value(value) -> bytes:
    """Marshal a native scalar into the encoded ``Scalar`` bytes a leaf stores.
    Rejects a plain ``dict``/``list`` and a non-integer ``float`` (create a nested
    container with ``get_map``/``get_list``/``get_text``); raises ``OverflowError``
    on an ``int`` outside the signed 64-bit range rather than wrapping."""
    if value is None:
        return b"\x00"
    # `bool` is a subclass of `int`, so it must be checked first.
    if isinstance(value, bool):
        return b"\x01" + (b"\x01" if value else b"\x00")
    if isinstance(value, int):
        if not _I64_MIN <= value <= _I64_MAX:
            raise OverflowError(
                f"integer {value} is outside the signed 64-bit range storable as a scalar"
            )
        return b"\x02" + struct.pack("<q", value)
    if isinstance(value, str):
        body = bytes([_STRING]) + value.encode("utf-8")
        return b"\x03" + struct.pack("<I", len(body)) + body
    if isinstance(value, (bytes, bytearray)):
        body = bytes([_BINARY]) + bytes(value)
        return b"\x03" + struct.pack("<I", len(body)) + body
    raise TypeError(
        f"value must be str, int, bool, bytes, or None (got {type(value).__name__}); "
        "create a nested container with get_map/get_list/get_text"
    )


def _decode_value(data: bytes):
    """Read encoded ``Scalar`` bytes back into a native value — the inverse of
    :func:`_encode_value`."""
    tag = data[0]
    if tag == 0x00:
        return None
    if tag == 0x01:
        return data[1] != 0
    if tag == 0x02:
        return struct.unpack_from("<q", data, 1)[0]
    if tag == 0x03:
        length = struct.unpack_from("<I", data, 1)[0]
        body = data[5 : 5 + length]
        if body[:1] == bytes([_STRING]):
            return body[1:].decode("utf-8")
        if body[:1] == bytes([_BINARY]):
            return bytes(body[1:])
        return bytes(body)  # foreign untagged bytes read as binary
    # A blob/element ref has no native leaf form here — the ergonomic reads for
    # these are get_blob / a dedicated accessor; hand back the opaque encoding.
    return bytes(data)


# --- reactivity: diff-derived ergonomic change events -------------------------
#
# A change event is a plain dict (mirroring the module-level `diff()` shape,
# Pythonic + directly comparable): a `kind`, an ergonomic key/index target path,
# and native `old`/`new` values — re-marshaled from the core `diff` the SDK
# already decodes. A snapshot+diff is taken only when something is observing, so
# an unobserved document pays nothing.


def _decode_path(data: bytes) -> List[str]:
    """Decode a length-framed path buffer (as the diff machinery reports) into its
    keys, rendered best-effort as utf-8 strings."""
    keys: List[str] = []
    i, n = 0, len(data)
    while i < n:
        length = struct.unpack_from("<I", data, i)[0]
        i += 4
        keys.append(_key_string(data[i : i + length]))
        i += length
    return keys


def _path_starts_with(whole: bytes, prefix: bytes) -> bool:
    """Whether ``whole``'s framed bytes begin with ``prefix`` — a key-path prefix
    test, sound because each key is self-delimiting (length + bytes)."""
    return whole[: len(prefix)] == prefix


def _native_from_diff_scalar(s: dict):
    """Convert a diff-reported map-leaf scalar (a tagged ``{t, v}`` dict) to a
    native value. A map leaf's bytes carry the SDK string/binary discriminator; a
    list item's enveloped scalar bytes instead decode through
    :func:`_decode_value`."""
    t = s["t"]
    if t == "null":
        return None
    if t in ("bool", "int"):
        return s["v"]
    if t == "bytes":
        payload = s["v"]
        if payload[:1] == bytes([_STRING]):
            return payload[1:].decode("utf-8")
        if payload[:1] == bytes([_BINARY]):
            return bytes(payload[1:])
        return bytes(payload)
    # blobref / elementref: no native leaf form — hand back the raw bytes.
    return s.get("v")


def _list_item_value(item: dict):
    """A list-change item: a native scalar for a leaf, or a container marker."""
    if "scalar" in item:
        return _decode_value(item["scalar"]["v"])
    return {"container": item.get("kind", "unknown")}


def _mark_change(raw: dict) -> dict:
    name = _key_string(raw["name"])
    if raw["op"] == "markAdded":
        return {
            "kind": "mark",
            "op": "add",
            "name": name,
            "new": _native_from_diff_scalar(raw["value"]),
        }
    if raw["op"] == "markRemoved":
        return {
            "kind": "mark",
            "op": "remove",
            "name": name,
            "old": _native_from_diff_scalar(raw["value"]),
        }
    return {
        "kind": "mark",
        "op": "change",
        "name": name,
        "old": _native_from_diff_scalar(raw["old"]),
        "new": _native_from_diff_scalar(raw["new"]),
    }


def _remarshal_change(raw: dict) -> Tuple[bytes, dict]:
    """Re-marshal one raw diff change (byte-path + tagged scalars) into an ergonomic
    change (native values, key/index target) plus its raw byte-path for observer
    prefix matching. A mark change carries no path (empty)."""
    op = raw["op"]
    if op in ("markAdded", "markRemoved", "markChanged"):
        return b"", _mark_change(raw)
    path_bytes = raw.get("path", b"")
    path = _decode_path(path_bytes)
    if op == "value":
        change = {
            "kind": "update",
            "path": path,
            "old": _native_from_diff_scalar(raw["old"]),
            "new": _native_from_diff_scalar(raw["new"]),
        }
    elif op == "counter":
        change = {"kind": "counter", "path": path, "old": raw["old"], "new": raw["new"]}
    elif op in ("listInsert", "listDelete"):
        kind = "list_insert" if op == "listInsert" else "list_delete"
        change = {
            "kind": kind,
            "path": path,
            "index": raw["index"],
            "values": [_list_item_value(i) for i in raw.get("items", [])],
        }
    elif op in ("textInsert", "textDelete"):
        kind = "text_insert" if op == "textInsert" else "text_delete"
        change = {"kind": kind, "path": path, "index": raw["index"], "text": raw["text"]}
    elif op == "remove":
        change = {"kind": "remove", "path": path, "value_kind": raw.get("kind", "unknown")}
    else:  # "add" and any future path-bearing op
        change = {"kind": "add", "path": path, "value_kind": raw.get("kind", op)}
    return path_bytes, change


def _repair_step(step: dict):
    """One repair-path step: a map-slot key (str) or a sequence index (int)."""
    return _key_string(step["key"]) if "key" in step else step["index"]


def _mark_info(m: dict) -> dict:
    """Re-marshal a raw mark (from ``marks_at``) into an ergonomic ``{name, value}``:
    a boolean for a boolean mark, a native scalar for a value mark, or the covering
    element ids for an object mark (the default with no bound schema)."""
    name = _key_string(m["name"])
    flavor = m["flavor"]
    if flavor == "boolean":
        return {"name": name, "value": m["value"]}
    if flavor == "object":
        return {"name": name, "value": m["ids"]}
    return {"name": name, "value": _native_from_diff_scalar(m["value"])}


@dataclass(frozen=True)
class UpdateEvent:
    """An applied change delivered to :meth:`Doc.on_update`. ``origin`` is
    ``"local"`` for an edit on this replica, ``"remote"`` for an applied peer
    update; ``ops`` are the wire-bound bytes the edit produced; ``changes`` are the
    diff-derived change dicts (empty when nothing is observing)."""

    origin: str
    ops: bytes
    changes: tuple = field(default_factory=tuple)


@dataclass(frozen=True)
class ChangeEvent:
    """A change notification for an observed subtree, delivered to
    :meth:`CrdtMap.observe` (and the list/text handles). Carries the same
    ``origin`` and the ``changes`` under the observed subtree."""

    origin: str
    changes: tuple = field(default_factory=tuple)


@dataclass(frozen=True)
class RepairEvent:
    """The schema-repair signal delivered to :meth:`Doc.on_repair`: the located
    ``paths`` whose repaired reading changed against the bound schema after an
    edit, each a list of steps (a map key ``str`` or a sequence index ``int``). A
    repair names a *location* to re-read, not an edit, so it carries no origin."""

    paths: tuple = field(default_factory=tuple)


class CrdtMap:
    """A live handle to a Map slot, addressed by ergonomic keys (``str`` or
    ``bytes``)."""

    def __init__(self, doc: "Doc", path: Tuple[bytes, ...]):
        self._doc = doc
        self._path = tuple(path)

    def _slot(self, key: Key) -> Path:
        return list(self._path) + [_key_bytes(key)]

    def _child(self, key: Key) -> Tuple[bytes, ...]:
        return self._path + (_key_bytes(key),)

    def set(self, key: Key, value) -> "CrdtMap":
        """Set a leaf at ``key`` to a native scalar. A ``dict``/``list`` raises a
        ``TypeError`` — a nested container is created with ``get_map``/``get_list``/
        ``get_text``."""
        slot = self._slot(key)
        scalar = _encode_value(value)
        self._doc._mutate(lambda b: b.set_scalar(slot, scalar))
        return self

    def get(self, key: Key):
        """Read ``key``: a native scalar for a leaf, a :class:`BlobRef` for a blob, a
        nested handle for a container slot, or ``None`` when the slot is empty."""
        slot = self._slot(key)
        backend = self._doc._backend
        blob = backend.get_blob(slot)
        if blob is not None:
            return blob
        scalar = backend.get_scalar(slot)
        if scalar is not None:
            return _decode_value(scalar)
        kind = self._container_kind(slot)
        if kind is None:
            return None
        return _HANDLE_CTORS[kind](self._doc, self._child(key))

    def _container_kind(self, slot: Path) -> Optional[str]:
        backend = self._doc._backend
        if backend.map_keys(slot) is not None:
            return "map"
        if backend.list_len(slot) is not None:
            return "list"
        if backend.text_len(slot) is not None:
            return "text"
        if backend.xml_children_len(slot) is not None:
            return "xml"
        return None

    def delete(self, key: Key) -> "CrdtMap":
        """Tombstone the slot at ``key``."""
        slot = self._slot(key)
        self._doc._mutate(lambda b: b.delete(slot))
        return self

    def __contains__(self, key: Key) -> bool:
        slot = self._slot(key)
        backend = self._doc._backend
        return (
            backend.get_scalar(slot) is not None
            or backend.get_blob(slot) is not None
            or self._container_kind(slot) is not None
        )

    def _raw_keys(self) -> List[bytes]:
        return self._doc._backend.map_keys(list(self._path)) or []

    def keys(self) -> List[str]:
        """The live slot keys, rendered best-effort as utf-8 strings."""
        return [_key_string(k) for k in self._raw_keys()]

    def items(self) -> List[Tuple[str, object]]:
        """The live ``(key, value)`` pairs. Values are read by the raw key bytes, so
        a non-utf-8 (binary) key's value is never lost."""
        return [(_key_string(k), self.get(k)) for k in self._raw_keys()]

    def __len__(self) -> int:
        return len(self._raw_keys())

    def __iter__(self):
        return iter(self.keys())

    def get_map(self, key: Key) -> "CrdtMap":
        """A nested Map handle at ``key``."""
        return CrdtMap(self._doc, self._child(key))

    def get_list(self, key: Key) -> "CrdtList":
        """A nested List handle at ``key``."""
        return CrdtList(self._doc, self._child(key))

    def get_text(self, key: Key) -> "CrdtText":
        """A nested Text handle at ``key``."""
        return CrdtText(self._doc, self._child(key))

    def get_xml(self, key: Key) -> "CrdtXml":
        """A nested Xml handle at ``key`` (an XML element or fragment)."""
        return CrdtXml(self._doc, self._child(key))

    def set_blob(self, key: Key, mime: str, data: bytes) -> bool:
        """Store a small blob inline at ``key``, minting its public handle. Returns
        ``False`` when ``data`` exceeds the inline ceiling — upload it out of band
        with :func:`upload_blob` and set the returned handle via :meth:`set_blob_ref`."""
        slot = self._slot(key)
        holder = {"ok": False}

        def run(b: Document) -> bytes:
            ops = b.set_blob(slot, mime, data)
            if ops is None:
                return b""  # over the inline ceiling — nothing enqueued
            holder["ok"] = True
            return ops

        self._doc._mutate(run)
        return holder["ok"]

    def set_blob_ref(self, key: Key, blob_id: bytes, mime: str, size: int) -> "CrdtMap":
        """Set a store-backed blob ref at ``key`` from a 16-byte ``blob_id`` handle,
        ``mime``, and ``size`` — the content is fetched by id, not carried in the op."""
        slot = self._slot(key)
        self._doc._mutate(lambda b: b.set_blob_ref(slot, blob_id, mime, size))
        return self

    def get_blob(self, key: Key) -> "Optional[BlobRef]":
        """Read the :class:`BlobRef` at ``key``, or ``None`` when the slot holds no
        blob."""
        return self._doc._backend.get_blob(self._slot(key))

    def observe(self, callback: "Callable[[ChangeEvent], None]") -> Callable[[], None]:
        """Observe changes to this map's subtree (local edits and applied remote
        updates); returns a function that unsubscribes."""
        return self._doc._add_observer(encode_path(list(self._path)), callback)


class CrdtList:
    """A live handle to a List of scalar items, addressed by live index."""

    def __init__(self, doc: "Doc", path: Tuple[bytes, ...]):
        self._doc = doc
        self._path = tuple(path)

    @property
    def _self(self) -> Path:
        return list(self._path)

    def insert(self, index: int, value) -> "CrdtList":
        """Insert a scalar item at a live ``index`` (clamped into range)."""
        n = len(self)
        if index < 0:
            index = max(0, n + index)
        index = min(index, n)
        item = _encode_value(value)
        self._doc._mutate(lambda b: b.list_insert(self._self, index, item))
        return self

    def append(self, value) -> "CrdtList":
        """Append a scalar item."""
        return self.insert(len(self), value)

    def delete(self, index: int) -> "CrdtList":
        """Tombstone the live item at ``index``."""
        idx = self._checked(index)
        self._doc._mutate(lambda b: b.list_delete(self._self, idx))
        return self

    def __getitem__(self, index: int):
        idx = self._checked(index)
        item = self._doc._backend.list_get(self._self, idx)
        if item is None:
            raise IndexError("list index out of range")
        return _decode_value(item)

    def __len__(self) -> int:
        return self._doc._backend.list_len(self._self) or 0

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def _checked(self, index: int) -> int:
        n = len(self)
        if index < 0:
            index += n
        if index < 0 or index >= n:
            raise IndexError("list index out of range")
        return index

    def observe(self, callback: "Callable[[ChangeEvent], None]") -> Callable[[], None]:
        """Observe changes to this list (local edits and applied remote updates);
        returns a function that unsubscribes."""
        return self._doc._add_observer(encode_path(list(self._path)), callback)

    def relative_position(self, index: int, side: str = "before") -> Optional[bytes]:
        """Capture a stable cursor at a live ``index`` (``side`` ``"before"`` is
        left-gravity, ``"after"`` right-gravity), resolved later with
        :meth:`resolve`. ``None`` for a bad or non-sequence path."""
        s = Side.RIGHT if side == "after" else Side.LEFT
        return self._doc._backend.relative_position(self._self, index, s)

    def resolve(self, pos: bytes) -> Optional[int]:
        """Resolve a captured cursor back to a live index, or ``None`` if it can't."""
        return self._doc._backend.resolve_position(self._self, pos)


class CrdtText:
    """A live handle to a collaborative Text run, indexed by codepoint."""

    def __init__(self, doc: "Doc", path: Tuple[bytes, ...]):
        self._doc = doc
        self._path = tuple(path)

    @property
    def _self(self) -> Path:
        return list(self._path)

    def insert(self, index: int, text: str) -> "CrdtText":
        """Insert ``text`` at a codepoint ``index``."""
        self._doc._mutate(lambda b: b.text_insert(self._self, index, text))
        return self

    def delete(self, index: int, count: int) -> "CrdtText":
        """Tombstone ``count`` codepoints from ``index``."""
        self._doc._mutate(lambda b: b.text_delete(self._self, index, count))
        return self

    def __str__(self) -> str:
        return self._doc._backend.text_get(self._self) or ""

    def __len__(self) -> int:
        return self._doc._backend.text_len(self._self) or 0

    def observe(self, callback: "Callable[[ChangeEvent], None]") -> Callable[[], None]:
        """Observe changes to this text (local edits and applied remote updates);
        returns a function that unsubscribes."""
        return self._doc._add_observer(encode_path(list(self._path)), callback)

    def relative_position(self, index: int, side: str = "before") -> Optional[bytes]:
        """Capture a stable cursor at a codepoint ``index`` (``side`` ``"before"`` is
        left-gravity, ``"after"`` right-gravity). The cursor tracks its spot as text
        is inserted and deleted around it. ``None`` for a bad path."""
        s = Side.RIGHT if side == "after" else Side.LEFT
        return self._doc._backend.relative_position(self._self, index, s)

    def resolve(self, pos: bytes) -> Optional[int]:
        """Resolve a captured cursor back to a live codepoint index, or ``None``."""
        return self._doc._backend.resolve_position(self._self, pos)

    def mark(
        self,
        start: int,
        end: int,
        name: Key,
        value,
        start_side: str = "before",
        end_side: str = "after",
    ) -> Optional[bytes]:
        """Author a mark named ``name`` with native ``value`` over ``[start, end)``,
        returning the mark's handle (or ``None`` if the author was inert). By default
        the range grows with text inserted at its edges (start left-gravity, end
        right-gravity)."""
        n = _key_bytes(name)
        scalar = _encode_value(value)
        ss = Side.RIGHT if start_side == "after" else Side.LEFT
        es = Side.RIGHT if end_side == "after" else Side.LEFT
        holder: dict = {}

        def run(b: Document) -> bytes:
            mark_id, ops = b._mark_encoded(self._self, start, ss, end, es, n, scalar)
            holder["id"] = mark_id
            return ops

        self._doc._mutate(run)
        return holder.get("id")

    def set_mark_value(self, mark_id: bytes, value) -> "CrdtText":
        """Change the native ``value`` of the mark ``mark_id``."""
        scalar = _encode_value(value)
        self._doc._mutate(lambda b: b._mark_set_value_encoded(mark_id, scalar))
        return self

    def delete_mark(self, mark_id: bytes) -> "CrdtText":
        """Tombstone the mark ``mark_id``."""
        self._doc._mutate(lambda b: b.mark_delete(mark_id))
        return self

    def marks_at(self, index: int) -> List[dict]:
        """The marks covering the character at ``index``, each an ergonomic
        ``{name, value}`` dict."""
        return [_mark_info(m) for m in self._doc._backend.marks_at(self._self, index)]


class CrdtXml:
    """A live handle to an XML element or fragment. Children are addressed by live
    index — the core stores a child with no path of its own, so this handle edits a
    node's direct children (insert element/text, delete, tree-move) but does not
    recurse into a child element's contents (deep XML navigation is a core
    follow-on, matching the JS/Go SDKs' XML surface)."""

    def __init__(self, doc: "Doc", path: Tuple[bytes, ...]):
        self._doc = doc
        self._path = tuple(path)

    @property
    def _self(self) -> Path:
        return list(self._path)

    def element(self, tag: str) -> "CrdtXml":
        """Install a tagged XML element at this slot."""
        t = _key_bytes(tag)
        self._doc._mutate(lambda b: b.xml_element(self._self, t))
        return self

    def fragment(self) -> "CrdtXml":
        """Install a tagless XML fragment at this slot."""
        self._doc._mutate(lambda b: b.xml_fragment(self._self))
        return self

    @property
    def tag(self) -> Optional[str]:
        """This element's tag, or ``None`` for a fragment or an absent node."""
        t = self._doc._backend.xml_tag(self._self)
        return None if t is None else _key_string(t)

    def __len__(self) -> int:
        return self._doc._backend.xml_children_len(self._self) or 0

    def insert_element(self, index: int, tag: str) -> "CrdtXml":
        """Insert a child element with ``tag`` at a live child ``index``."""
        t = _key_bytes(tag)
        self._doc._mutate(lambda b: b.xml_insert_element(self._self, index, t))
        return self

    def insert_text(self, index: int, text: str) -> "CrdtXml":
        """Insert a text-run child holding ``text`` at a live child ``index``."""
        self._doc._mutate(lambda b: b.xml_insert_text(self._self, index, text))
        return self

    def delete_child(self, index: int) -> "CrdtXml":
        """Tombstone the child at a live ``index``."""
        self._doc._mutate(lambda b: b.xml_child_delete(self._self, index))
        return self

    def move(self, child_index: int, new_parent: "CrdtXml", dest_index: int) -> "CrdtXml":
        """Relocate this node's child at ``child_index`` to ``dest_index`` in
        ``new_parent``'s children — an identity-preserving tree move."""
        dest = new_parent._self
        self._doc._mutate(lambda b: b.xml_move(self._self, child_index, dest, dest_index))
        return self

    def observe(self, callback: "Callable[[ChangeEvent], None]") -> Callable[[], None]:
        """Observe changes to this node's children (local edits and applied remote
        updates); returns a function that unsubscribes."""
        return self._doc._add_observer(encode_path(list(self._path)), callback)


_HANDLE_CTORS = {"map": CrdtMap, "list": CrdtList, "text": CrdtText, "xml": CrdtXml}


class Doc:
    """A local CRDT replica with a single root map, edited through live typed
    handles. A ``Doc`` is a pure local replica: two docs that exchange each other's
    update ops (forwarded via :meth:`on_update`) converge. The low-level path API
    stays available on the wrapped :class:`Document` for power users."""

    def __init__(self, client_id: Optional[bytes] = None):
        self._backend = Document(client_id if client_id is not None else os.urandom(16))
        self._init_listeners()

    def _init_listeners(self) -> None:
        self._update_listeners: List[Callable[[UpdateEvent], None]] = []
        self._repair_listeners: List[Callable[[RepairEvent], None]] = []
        self._observers: List[Tuple[bytes, Callable[[ChangeEvent], None]]] = []
        self._transacting = False

    def get_map(self, key: Key) -> CrdtMap:
        """A live root Map handle at ``key``."""
        return CrdtMap(self, (_key_bytes(key),))

    def get_list(self, key: Key) -> CrdtList:
        """A live root List handle at ``key``."""
        return CrdtList(self, (_key_bytes(key),))

    def get_text(self, key: Key) -> CrdtText:
        """A live root Text handle at ``key``."""
        return CrdtText(self, (_key_bytes(key),))

    def get_xml(self, key: Key) -> CrdtXml:
        """A live root Xml handle at ``key``."""
        return CrdtXml(self, (_key_bytes(key),))

    def transact(self, fn: Callable[[], object]) -> None:
        """Run ``fn``'s edits as one atomic group — they apply together on every
        replica, ride the wire as a single batch, and fire one update. Nested calls
        flatten into the outermost transaction."""
        if self._transacting:
            fn()
            return
        before = self._backend.encode_state() if self._observing() else None
        self._transacting = True
        self._backend.begin_atomic()
        try:
            fn()
        finally:
            self._transacting = False
            ops = self._backend.commit_atomic()
            if ops:
                self._dispatch("local", ops, before)
                self._emit_repairs()

    def on_update(self, callback: Callable[[UpdateEvent], None]) -> Callable[[], None]:
        """Subscribe to every applied change to the document; returns a function
        that unsubscribes."""
        self._update_listeners.append(callback)

        def off() -> None:
            try:
                self._update_listeners.remove(callback)
            except ValueError:
                pass

        return off

    def on_repair(self, callback: Callable[[RepairEvent], None]) -> Callable[[], None]:
        """Subscribe to the schema-repair signal (fires only once a schema is bound):
        the located paths whose repaired reading changed against the schema after an
        edit. Returns a function that unsubscribes."""
        self._repair_listeners.append(callback)

        def off() -> None:
            try:
                self._repair_listeners.remove(callback)
            except ValueError:
                pass

        return off

    def set_schema(self, schema: bytes) -> bool:
        """Bind a schema (its JSON, as bytes) to this replica, returning whether it
        bound. A bound schema gives named marks their declared flavor and turns on
        the :meth:`on_repair` signal."""
        return self._backend.set_schema(schema)

    def apply_update(self, ops: bytes) -> int:
        """Fold a peer's update ops into this replica; returns the count applied."""
        before = self._backend.encode_state() if self._observing() else None
        applied = self._backend.apply(ops)
        if applied > 0:
            self._dispatch("remote", ops, before)
            self._emit_repairs()
        return applied

    def encode_state(self) -> bytes:
        """Serialize the whole replica to a canonical snapshot."""
        return self._backend.encode_state()

    @classmethod
    def decode_state(cls, state: bytes) -> "Doc":
        """Open a ``Doc`` from a snapshot produced by :meth:`encode_state`."""
        obj = cls.__new__(cls)
        obj._backend = Document.decode_state(state)
        obj._init_listeners()
        return obj

    def close(self) -> None:
        self._backend.close()

    def __enter__(self) -> "Doc":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def _add_observer(
        self, prefix: bytes, callback: "Callable[[ChangeEvent], None]"
    ) -> Callable[[], None]:
        observer = (prefix, callback)
        self._observers.append(observer)

        def off() -> None:
            try:
                self._observers.remove(observer)
            except ValueError:
                pass

        return off

    def _observing(self) -> bool:
        return bool(self._update_listeners) or bool(self._observers)

    def _mutate(self, run: Callable[[Document], bytes]) -> bytes:
        # Inside a transaction the edit just accumulates; the commit dispatches.
        if self._transacting:
            run(self._backend)
            return b""
        before = self._backend.encode_state() if self._observing() else None
        ops = run(self._backend)
        if not ops:
            return ops
        self._dispatch("local", ops, before)
        self._emit_repairs()
        return ops

    def _dispatch(self, origin: str, ops: bytes, before: Optional[bytes]) -> None:
        raws = [] if before is None else self._compute_changes(before)
        changes = [change for _pb, change in raws]
        # A remote frame that changed nothing (an ack) fires no update; a local edit
        # always reports its ops. Snapshot the listener sets so one subscribed during
        # dispatch does not receive this in-flight event.
        if origin == "local" or changes:
            event = UpdateEvent(origin=origin, ops=ops, changes=tuple(changes))
            for listener in list(self._update_listeners):
                listener(event)
        for prefix, listener in list(self._observers):
            matched = [c for pb, c in raws if _path_starts_with(pb, prefix)]
            if matched:
                listener(ChangeEvent(origin=origin, changes=tuple(matched)))

    def _compute_changes(self, before: bytes) -> List[Tuple[bytes, dict]]:
        after = self._backend.encode_state()
        # A missing state is not a decodable snapshot; treat it as no changes rather
        # than letting the diff raise.
        if not before or not after:
            return []
        raw = _diff_raw(before, after)
        if not raw:
            return []
        return [_remarshal_change(d) for d in _decode_changes(raw)]

    def _emit_repairs(self) -> None:
        # Drain the schema-repair signal only when observed — the drain reseeds the
        # baseline, so draining unobserved would lose the signal; an unobserved doc
        # pays nothing (and take_repairs is empty until a schema is bound).
        if not self._repair_listeners:
            return
        raw = self._backend.take_repairs()
        if not raw:
            return
        paths = [[_repair_step(step) for step in path] for path in raw]
        event = RepairEvent(paths=paths)
        for listener in list(self._repair_listeners):
            listener(event)



class Provider:
    """An ergonomic, offline-first sync binding over a :class:`Doc`'s apply/emit
    seam — the Python realization of the §SDK-Ergonomic-Surface provider model.

    The Python SDK is embedded/offline-first: unlike the JS provider it owns no
    socket loop, so the app supplies the transport. Bind a ``Doc`` with a ``send``
    callback (invoked with each local edit's ops to transmit), and feed a peer's
    ops to :meth:`receive`. The provider owns the connection state and an offline
    outbox, so edits made while disconnected queue and flush on reconnect; inbound
    ops apply and fire the doc's reactivity as ``remote``. A remote apply never
    re-emits as a local edit, so a pair of linked providers can't loop.

    The fully-networked provider that owns a socket and backs the ``Doc`` with a
    single wire-client replica (the JS ``connect(url, room)`` model, with awareness
    and the operator-tier request/reply surface) is a documented follow-on: the
    Python ``Client`` wire surface does not yet expose the per-channel
    list/text/scalar/map-key handle ops a single-replica networked handle graph
    needs. Until then this seam plus the low-level :class:`Client` cover sync.
    """

    def __init__(self, doc: "Doc", send: "Callable[[bytes], None]", *, connected: bool = False):
        self.doc = doc
        self._send = send
        self._state = "connected" if connected else "disconnected"
        self._outbox: List[bytes] = []
        self._state_listeners: List[Callable[[str], None]] = []
        self._unsub = doc.on_update(self._on_update)

    def _on_update(self, event: "UpdateEvent") -> None:
        # Only a local edit is transmitted; a remote apply must not echo (or a pair
        # of linked providers would loop forever).
        if event.origin != "local":
            return
        if self._state == "connected":
            self._send(event.ops)
        else:
            self._outbox.append(event.ops)

    def receive(self, ops: bytes) -> int:
        """Fold a peer's ops into the bound doc (firing ``remote`` reactivity);
        returns the count applied."""
        return self.doc.apply_update(ops)

    @property
    def state(self) -> str:
        """The connection state: ``"connected"`` or ``"disconnected"``."""
        return self._state

    @property
    def outbox_len(self) -> int:
        """How many local edits are queued awaiting a reconnect flush."""
        return len(self._outbox)

    def connect(self) -> None:
        """Mark the transport connected and flush the offline outbox in order."""
        self._set_state("connected")
        pending, self._outbox = self._outbox, []
        for ops in pending:
            self._send(ops)

    def disconnect(self) -> None:
        """Mark the transport disconnected; subsequent local edits queue."""
        self._set_state("disconnected")

    def on_state(self, callback: "Callable[[str], None]") -> Callable[[], None]:
        """Observe connection-state changes; returns a function that unsubscribes."""
        self._state_listeners.append(callback)

        def off() -> None:
            try:
                self._state_listeners.remove(callback)
            except ValueError:
                pass

        return off

    def close(self) -> None:
        """Unbind from the doc; local edits stop being forwarded/queued."""
        self._unsub()

    def __enter__(self) -> "Provider":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _set_state(self, state: str) -> None:
        if state == self._state:
            return
        self._state = state
        for listener in list(self._state_listeners):
            listener(state)
