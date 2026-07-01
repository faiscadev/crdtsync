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
import os
import platform
import struct
from typing import List, Optional

__all__ = ["Document", "encode_path"]

Path = List[bytes]


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
    sig(lib.crdtsync_doc_apply, [doc, cbytes, size], c.c_int32)
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

    # --- sync ---

    def apply(self, ops: bytes) -> int:
        """Fold a peer's encoded ops in. Returns the number applied, -1 on error."""
        return _LIB.crdtsync_doc_apply(self._handle, ops, len(ops))

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
