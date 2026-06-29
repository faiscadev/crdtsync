# Rust rewrite — plan

Rewrite the CRDT core in Rust. Same semantics (refcount + displacement lifecycle,
Share slot ownership, LWW/tombstone CRDT behavior), with lifetime bookkeeping
handled by the language instead of by hand.

## Decisions

- **Embedding:** native **C ABI** (`cdylib`/`staticlib`) for Go (cgo) and Python
  (cffi/PyO3). **wasm** (`wasm-bindgen`) for JavaScript/browser only. No
  "wasm-everywhere" runtime embedded in native hosts.
- **`std`, not `no_std`.** `Vec`/`HashMap`/`Rc` compile to every target we care
  about. `no_std` buys nothing here.
- **Representation:** composites are `Rc<RefCell<T>>`; an `Element` is an inline
  `Scalar` or a shared composite handle. The value graph is a downward tree
  (Map → children), so handles never form a cycle.
- **Host seam = effects only:** `entropy()` + `now()`. Allocation is not in the
  seam.

## Crate layout

```
Cargo.toml                 workspace
crates/
  core/                    crdtsync-core — pure CRDT logic, Host trait, no I/O
    src/{host,clientid,stamp,elementid,scalar,counter,register,element,map}.rs
  ffi/                     crdtsync-ffi  — extern "C" opaque-handle API
    src/lib.rs
# later:
#  wasm/                   crdtsync-wasm — wasm-bindgen layer for JS
```

## Memory model

- **Internal:** no manual allocation or freeing. Values allocate on creation and
  are reclaimed when dropped; the downward tree means the whole graph frees from
  the root.
- **FFI boundary** (the only place ownership is manual, and it's coarse):
  - handles: `crdtsync_doc_new` / `crdtsync_doc_free` pairs.
  - buffers out of the core: released via `crdtsync_buf_free`.
  - rule: core-allocated memory is freed only through the core's free functions;
    host languages free their own memory with their own allocators. The two
    never cross.
- `extern "C"` bodies wrap work in `catch_unwind` — a panic must not unwind past
  the boundary.

## Port order (bottom-up; each step a green milestone)

The existing test suites are the behavioral spec — port them per primitive and
make them pass before moving up.

1. **host** — `Host` trait (`entropy`, `now_unix_millis`); a test/native impl.
2. **clientid** — UUIDv7 from host entropy + clock (built by hand to avoid a
   getrandom dependency). **stamp** — `(lamport, client)`, strict-greater order.
3. **elementid** — UUIDv5 over `(parent, key, kind)` for convergent derivation.
4. **scalar** — `enum { Null, Bool, Int, Bytes }`.
5. **counter** — per-client tallies, per-direction max merge, `displaced`.
6. **register** — LWW Scalar + stamp, `displaced`.
7. **element** — tagged value; lifecycle/merge/clone forward to the composite.
8. **map** — LWW slots + tombstones (`value: Option<Element>` so a tombstone
   can't hold a value), Share semantics, get-or-create helpers, recursive merge.

## Testing

- Port each C suite to `#[cfg(test)]` / `tests/`. Names map 1:1 so behavior is
  comparable against the current core.
- Run under **Miri** (`cargo +nightly miri test`) for UB + leak detection —
  deterministic and cross-platform.
- The same-composite-reset guard and other domain invariants still need explicit
  tests; the language does not enforce CRDT semantics.

## FFI surface (after the core is green)

- `cbindgen` generates the C header consumed by Go/Python.
- Thin Go and Python wrappers over the C ABI (`open`/`free`, ops, buffer return).

## What the language does not solve

- CRDT correctness (convergence, tombstones, id derivation) — same effort, same
  tests.
- Displacement *semantics* — the lifetime is managed, the meaning is not.
- `RefCell` borrow conflicts surface at runtime (e.g. merging a map into itself);
  tests and Miri catch them.

## Status

Scaffold committed: crate layout, all types, and signatures stubbed with
`todo!()`. Nothing implemented.
