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
the last the slot. Edit methods (`register_int`, `inc`, `set_bytes`, `delete`,
`list_insert`, `list_delete`, `text_insert`, `text_delete`) apply locally and
return the encoded ops to send to peers; `apply` folds a peer's ops back in.
Read methods (`get_int`, `get_counter`, `get_bytes`, `list_len`, `list_get`,
`text_len`, `text_get`) return the value or `None`.

The native library is found under `target/{release,debug}` or via `$CRDTSYNC_LIB`.
