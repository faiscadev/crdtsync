# crdtsync (Python)

Python bindings for the crdtsync CRDT core, loaded over its C ABI with `ctypes`
(no compile step). Build the native library once, then edit through the ergonomic
handle graph:

```sh
cargo build -p crdtsync-ffi          # produces target/debug/libcrdtsync_ffi.*
cd sdks/python && python -m pytest
```

## Handle graph (recommended)

A `Doc` is a local replica edited through live typed handles — `CrdtMap` /
`CrdtList` / `CrdtText` / `CrdtXml` — addressed by ergonomic keys (`str`, or
`bytes` for raw). Native values marshal to scalars; byte-paths stay hidden.

```python
from crdtsync import Doc

doc = Doc()                                   # a random 16-byte replica id
users = doc.get_map("users")
users.set("alice", 30)                        # int / str / bool / bytes / None
users.get_map("bob").set("age", 41)           # nested container, explicitly
doc.get_text("body").insert(0, "hello")       # collaborative text
assert users.get("alice") == 30
```

Containers are created **explicitly** with `get_map`/`get_list`/`get_text`/
`get_xml`; a native scalar is a leaf. `set(k, {...})` is a `TypeError` (no
deep-seed), and an `int` outside the signed 64-bit range raises `OverflowError`.

Two docs converge by exchanging update ops:

```python
a, b = Doc(bytes([1] + [0]*15)), Doc(bytes([2] + [0]*15))
a.on_update(lambda e: b.apply_update(e.ops) if e.origin == "local" else None)
a.get_map("root").set("k", 1)
assert b.get_map("root").get("k") == 1
```

Handles support Python protocols — `len(m)`, `k in m`, `m.items()`, `list(xs)`,
`str(text)` — plus reactivity (`doc.on_update`, `handle.observe`, `doc.on_repair`
once a schema is bound), Text cursors (`relative_position`/`resolve`), marks,
atomic transactions (`doc.transact`), blobs, and XML with tree-move.

### Sync

`Provider(doc, send)` is an offline-first binding over the doc's apply/emit seam:
the app supplies the transport, the provider owns the connection state and an
offline outbox that flushes on reconnect.

```python
from crdtsync import Provider
p = Provider(doc, send=my_socket.send, connected=True)  # forwards local ops
p.receive(incoming_ops)                                  # folds a peer's ops
```

## Low-level path API

The original path-based, bytes-valued surface stays available for power users:
`Document` addresses a slot by a **path** (a list of `bytes` keys), edit methods
return the encoded ops to broadcast, and `apply` folds a peer's ops back in. The
wire `Client` drives the full sync protocol. See the source docstrings.

The native library is found under `target/{release,debug}` or via `$CRDTSYNC_LIB`.
