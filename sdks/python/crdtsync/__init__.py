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
import os
import platform
import struct
from typing import List, Optional, Tuple

__all__ = ["Client", "Document", "Side", "Undo", "diff", "encode_path"]

Path = List[bytes]


class Side(enum.IntEnum):
    """Which edge of an index a captured position anchors to."""

    LEFT = 0
    RIGHT = 1


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
    sig(lib.crdtsync_doc_delete, [doc, cbytes, size], buf)
    sig(lib.crdtsync_doc_get_int, [doc, cbytes, size, c.POINTER(c.c_int64)], c.c_int32)
    sig(lib.crdtsync_doc_get_counter, [doc, cbytes, size, c.POINTER(c.c_int64)], c.c_int32)
    sig(lib.crdtsync_doc_get_bytes, [doc, cbytes, size, c.POINTER(buf)], c.c_int32)
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
    sig(lib.crdtsync_client_auth, [doc, cbytes, size], buf)
    sig(lib.crdtsync_client_actor, [doc, c.POINTER(buf)], c.c_int32)
    sig(lib.crdtsync_client_subscribe, [doc, cbytes, size, c.POINTER(ch)], buf)
    sig(lib.crdtsync_client_resume, [doc, ch], buf)
    sig(lib.crdtsync_client_resend, [doc, ch], buf)
    sig(lib.crdtsync_client_outbox_len, [doc, ch, c.POINTER(size)], c.c_int32)
    sig(lib.crdtsync_client_unsubscribe, [doc, ch], buf)
    sig(lib.crdtsync_client_receive, [doc, cbytes, size], c.c_int32)
    sig(lib.crdtsync_client_last_seen_seq, [doc, ch, c.POINTER(c.c_uint64)], c.c_int32)
    sig(lib.crdtsync_client_register_int, [doc, ch, cbytes, size, c.c_int64], buf)
    sig(lib.crdtsync_client_inc, [doc, ch, cbytes, size, c.c_uint32], buf)
    sig(lib.crdtsync_client_dec, [doc, ch, cbytes, size, c.c_uint32], buf)
    sig(lib.crdtsync_client_set_bytes, [doc, ch, cbytes, size, cbytes, size], buf)
    sig(lib.crdtsync_client_delete, [doc, ch, cbytes, size], buf)
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


_KINDS = ("scalar", "register", "counter", "map", "list", "text")


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

    def u8(self) -> int:
        return self._take(1)[0]

    def u32(self) -> int:
        return int.from_bytes(self._take(4), "little")

    def u64(self) -> int:
        return int.from_bytes(self._take(8), "little")

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
        else:
            raise ValueError(f"bad change tag {tag}")
    return out


def diff(old_state: bytes, new_state: bytes) -> list:
    """Diff two snapshots — each a state buffer from ``Document.encode_state``, a
    named version, or an exported room — into a list of structural change dicts
    turning the old state into the new. Each change has an ``op`` tag, a ``path``
    (bytes), and its variant's fields; a scalar is a tagged ``{"t", "v"}`` dict.
    Raises ``ValueError`` on a malformed snapshot."""
    data = _take_buf(
        _LIB.crdtsync_diff(old_state, len(old_state), new_state, len(new_state))
    )
    if not data:
        raise ValueError("malformed snapshot")
    return _decode_changes(data)


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

    def delete(self, path: Path) -> bytes:
        p = encode_path(path)
        return _take_buf(_LIB.crdtsync_doc_delete(self._handle, p, len(p)))

    def get_int(self, path: Path) -> Optional[int]:
        return self._read_i64(_LIB.crdtsync_doc_get_int, path)

    def get_counter(self, path: Path) -> Optional[int]:
        return self._read_i64(_LIB.crdtsync_doc_get_counter, path)

    def get_bytes(self, path: Path) -> Optional[bytes]:
        return self._read_buf(_LIB.crdtsync_doc_get_bytes, path)

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
        """Fold one received wire frame in. 1 applied, 0 refused, -1 bad handle."""
        return _LIB.crdtsync_client_receive(self._handle, msg, len(msg))

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
