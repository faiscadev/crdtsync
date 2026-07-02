# Design decisions

Log of design changes to [ARCHITECTURE.md](ARCHITECTURE.md) that implementation forced. ARCHITECTURE always reads as the current intended end-state; this file is *why* it changed. Newest first.

`/cs-implement` appends an entry when building a unit reveals a genuine design change — a forced realization (the design as written is wrong, impossible, or clearly worse than what the code must do), never a mere preference. The human appends when they revise scope. Format: **date · unit/PR · what changed · why**.

The entries below (2026-07-02) are a backfill: design changes made during the v0.1→v0.2 build that predate this log, recovered from the sessions and commit history.

---

## 2026-07-02 · #60 · blob-ref reserved as a `Scalar` variant, not a separate `Value` enum
**Changed:** the blob-ref value slot is `Scalar::BlobRef(BlobRef)` (value-codec tag 4), sitting alongside the other leaf values, rather than a distinct `Value = Scalar | BlobRef | ElementRef` payload type. `BlobRef { id: [u8;16], mime, size, inline: Option<Vec<u8>> }`. Slot-only: the blob store, dedup, presigned fetch, and GC still land v0.5. The element-ref value type is left unreserved (no v0.1 promise, shape under-specified).
**Why:** a blob ref merges as an LWW replace on assignment — exactly a leaf value's semantics — so it is the true analog of the `tx` slot reservation: one codec tag, near-zero churn, no refactor of `RegisterSet`/`MapSet`/`ListInsert`. ARCHITECTURE's scalar/blob-ref/element-ref taxonomy distinguishes *merge semantics*, which `Scalar` already models; it is not a mandate for a distinct Rust enum. Trade-off: `Scalar` is documented "no id" in the CRDT-identity sense — `BlobRef`'s handle is opaque payload data (like `Bytes`), carries no CRDT identity, and does not merge or displace, so it still reads as a value, not an entity.

## 2026-07-02 · rewrite · core language is Rust, not C
**Changed:** the core and server are implemented in Rust — a downward `Rc<RefCell<T>>` value graph, `#![forbid(unsafe_code)]`, Miri-gated — not C. Export surface is unchanged: a stable C ABI (cbindgen) for native SDKs + wasm (wasm-bindgen) for the browser. Host seam is `entropy()` + `now()` only; `std`, not `no_std`.
**Why:** the C core's manual refcount/arena lifetime management was error-prone ("memory management was hell"); Rust removes the use-after-free / double-free / aliasing hazard class at compile time while keeping the same portability. Decided at the cheapest moment — only the primitives existed.

## 2026-07-02 · #48, #32 · displacement retains + reinstates, not orphan-and-forget
**Changed:** a displaced container/counter is kept in a persistent per-id registry and reinstated if its slot is re-won; a displaced counter keeps accumulating. Replaces "the displaced element_id may become unreachable; core surfaces an orphan event."
**Why:** orphan-and-forget diverges — two replicas that saw the same ops disagreed on a counter's value across displace-then-recreate. Retention is a convergence requirement, not a nicety. The orphan event still fires for the app; the state itself is retained.

## 2026-07-02 · core op vs. wire envelope
**Changed:** the core op the CRDT engine merges is `{id, stamp, target, kind, tx}`. Authorship (`actor_id`), scope (`room`/`branch`/`zone`), `schema_version`, and wall time are wire/server-envelope concerns wrapping the core op — not core op fields.
**Why:** keeps the CRDT engine pure and portable; the envelope is layered at the wire/server boundary, which is where those fields are actually consumed.

## 2026-07-02 · #52–#56 · snapshot keyed on server seq; cold-start is snapshot-or-delta, no tail
**Changed:** a snapshot is keyed by the server sequence it covers (`base_seq`), not a lamport timestamp, and is regenerated live from the merged replica. Catch-up returns *either* an op delta (at/above the room's compaction floor) *or* a whole-replica snapshot (below it) — never snapshot-plus-tail.
**Why:** the server sequence is what the durable log truncates on; regenerating from the live doc is always current, so a stored-snapshot-plus-tail merge is unnecessary. *Revisit* (KANBAN): O(state) CPU per below-floor cold-start — cache the encoded snapshot if snapshots grow.

## 2026-07-02 · #33, #40 · one custom binary codec, shared by wire and log
**Changed:** a single deterministic little-endian, length-framed, total-decode codec — not CBOR/MessagePack — reused for both the durable op-log and the wire envelope. The 8-byte header (`"CRDT"` magic + version) reserves the version for future codec negotiation.
**Why:** a closed op enum doesn't benefit from a self-describing format; one codec is one code path and one test surface. Negotiation stays possible via the reserved version field.

## 2026-07-02 · #47, #56 · persistence is a per-room append-only file log, not SQLite
**Changed:** the store is `<room>.log` (length-framed ops) + `<room>.snap` (compaction snapshot); crash-safety is hand-rolled (append flushes before return; compaction is temp → fsync → rename → dir fsync → truncate, with dedup-on-replay). No SQLite, no relational tables.
**Why:** the op stream is pure append-only sequential — relational storage buys nothing on the hot path and drops a dependency. *Revisit* (KANBAN): the metadata/query side (admin UI, op-log viewer, audit) may still want an embedded DB (SQLite/redb).

## 2026-07-02 · element_id derives from (parent, key, kind)
**Changed:** the derivation tuple includes the element kind, so a type-flip on a slot yields a different element_id. (ARCHITECTURE previously wrote the tuple as `(parent_id, key)`.)
**Why:** two differently-typed elements at the same slot are genuinely different elements; the kind in the tuple drives the displacement/orphan path correctly.
