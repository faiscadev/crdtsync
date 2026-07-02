# crdtsync — build board

Live status of the Rust core + server + SDKs. [ARCHITECTURE.md](ARCHITECTURE.md) is the plan of record (the *what* and *why*); this file is the *where we are*.

`/cs-next` reads this board plus the dependency graph below to pick the next unit, then hands it to `/cs-implement`. The **test suites are the spec** — a unit is "done" when its suite is green and Miri-clean, merged to `main`.

## Dependency order

Primitives are built bottom-up; never advance a unit whose dependencies are red.

```
host → stamp → clientid → elementid → scalar → counter → register → element → map
                                                    ↓
                          op envelope → doc (transact/apply, buffering, displacement)
                                                    ↓
                       list (Fugue) → text (codepoint) → binary codec
                                                    ↓
              wire framing → room hub → session driver → connection registry → websocket
                                                    ↓
                            persistence → state codec → compaction
                                                    ↓
                                    SDKs (FFI / wasm / Python / Go)
```

Element + Map are one coupled unit (Map slots hold Elements; Element forwards lifecycle to composites).

---

## ✅ Done (on `main`)

**Core primitives** — all green, Miri-clean:
scalar / counter / register / element / map (#22–#27), list Fugue (#24), text codepoint (#25), op envelope (#22), doc transact/apply (#30/#31), out-of-order buffering + persistent container identity (#32), binary op codec + log framing (#33).

**Server** — wire message framing (#40), room hub: op-log + idempotent ingest + catch-up (#41), session protocol driver (#42), connection registry fan-out (#43), tokio WebSocket transport (#44), durable op-log disk persistence (#47).

**SDKs** — C ABI / FFI (#34–#36), cbindgen header (#37), Python (#38), Go (#39), shared `core::path` navigation façade (#45), WebAssembly / JS (#46).

**Correctness** — counter identity across displacement fixed (#48); randomized convergence property harness (#49/#50); server durability property fuzz (#51).

**v0.2 state codec + compaction** — leaf-value state serialization (#52), sequence-CRDT state serialization (#53), whole-replica document state serialization (#54), in-memory op-log compaction + `Message::Snapshot` (#55), durable disk-log compaction, crash-safe (#56).

---

## 🚧 In progress

- **SDK snapshot-decode** (#57, branch `feat/sdk-snapshot-codec`) — surface `Document::encode_state`/`decode_state` across FFI + wasm + Go + Python so a client served a `Message::Snapshot` rebuilds its replica. **Gates turning compaction on in production** — a below-floor client breaks without it. Core commits landed; SDK layer + tests in the working tree, not yet PR'd.

---

## ⏭ Next

- **Compaction trigger / policy** — nothing auto-calls `Hub::compact` yet; it's operator-driven. A size/age threshold + config closes the v0.2 compaction arc end-to-end (codec → in-mem → disk → SDK decode → auto-trigger).

---

## 📋 Backlog (v0.2+, ordered loosely; several are product calls)

- **Awareness subsystem** — ephemeral presence (cursors/selections/typing), TTL + throttle + auth filtering + reconnect grace. (v0.2)
- **Handshake auth** — three-phase Hello/Auth/Subscribe, pluggable credential verifier, opaque credentials, server-derived `actor_id`. (v0.2)
- **Declarative policy + audit log** — authorization enforcement. (v0.2)
- **Channel multiplexing** — logical channels per `(room, branch, zone)` over one WebSocket. (v0.2)
- **Named versions + auto-version triggers**, **UndoManager** for v0.1 primitives, **composition cookbook**, **admin dashboard**, **replay tooling**. (v0.2)
- **Blob refs** — refs in ops, bytes in a separate content-addressable store (slot already reserved in the op envelope). (v0.5)
- **Mixed-version migrations** — migration entries as log entries, per-op `schema_version`, four detection gates. (v0.6)
- **Distributed cluster** — room sharding, replication, failover, leader election, first-class branches. (v0.4)
- **XmlElement / marks / schema / invariant repair / zones**. (v0.5)

---

## Loop

Each unit runs `/cs-next` → `/cs-implement`: spec-first tests → implement to green → Miri gate (UB + leaks) → `make fmt` → PR → react to CI + review with pushback → squash-merge → continue up the chain.
