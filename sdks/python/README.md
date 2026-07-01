# crdtsync (Python)

Python bindings for the crdtsync CRDT core, loaded over its C ABI with `ctypes`
(no compile step). Build the native library once, then use `Document`:

```sh
cargo build -p crdtsync-ffi          # produces target/debug/libcrdtsync_ffi.*
cd sdks/python && python -m pytest
```

```python
from crdtsync import Document

a, b = Document(bytes([1] + [0]*15)), Document(bytes([2] + [0]*15))
ops = a.register_int([b"user", b"age"], 30)   # nested path; returns ops to broadcast
b.apply(ops)                                   # peer folds them in
assert b.get_int([b"user", b"age"]) == 30      # converged
```

A slot is addressed by a **path** — a list of `bytes` keys naming nested maps,
the last the slot. Edits (`register_int`, `inc`, `set_bytes`, `delete`,
`list_*`, `text_*`) apply locally and return the encoded ops to send to peers;
`apply` folds a peer's ops back in.

The native library is found under `target/{release,debug}` or via `$CRDTSYNC_LIB`.
