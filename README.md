# crdtsync

> Self-hosted collaborative sync backend with a portable CRDT core.

[crdtsync.com](https://crdtsync.com) · part of the [faiscadev](https://github.com/faiscadev) org

---

## ⚠️ Status: Active Construction / WIP

This project is in early design. Nothing here is shippable yet. The architecture, primitives, schema language, and APIs are still being finalized. Wire format and op envelope are explicitly **not** locked yet — see [ARCHITECTURE.md → Foundational Decisions](./ARCHITECTURE.md#foundational-decisions) for what's still open.

If you stumbled here looking for a working collaborative sync engine: come back later, or use [Yjs](https://github.com/yjs/yjs) / [Automerge](https://github.com/automerge/automerge) / [Liveblocks](https://liveblocks.io) in the meantime.

---

## What it is

A realtime collaborative sync backend + portable CRDT engine, designed around:

- **Batteries-included deployment** — single container, no Postgres / Redis / Kafka / NATS / etc. required
- **Portable CRDT core** — one OCaml implementation, exported as WASM (browser/Node) and stable C ABI (native bindings for Python / Go / Rust / JVM)
- **Multi-language SDKs** that all edit the same document natively
- **First-class everything** — schema, invariant repair, schema migration, undo/redo, named versions, branches, authorization (with wire-level redaction), awareness — all built into core rather than left for apps to reinvent

## What it is NOT

- Not "yet another CRDT library." The differentiation is the **infrastructure** around CRDTs (deployment, scaling, schema, auth, versioning), not the data structures themselves.
- Not a hosted SaaS. Self-hosted first.
- Not a Yjs fork. Yjs interop is via a one-way snapshot importer (`fromYDoc`), not wire-protocol compatibility.

## High-level architecture

```
               ┌───────────────────┐
               │    Client SDKs    │
               │ JS / Python / Go  │
               │  / Rust / JVM     │
               └─────────┬─────────┘
                         │
                         ▼
               ┌───────────────────┐
               │ Shared CRDT Core  │
               │      OCaml        │
               └─────────┬─────────┘
                         │
         ┌───────────────┴───────────────┐
         ▼                               ▼
   WASM Export                      C ABI Export
         │                               │
         ▼                               ▼
 Browser / Node                Native language bindings


               ┌───────────────────┐
               │    Sync Server    │
               │       OCaml       │
               └─────────┬─────────┘
                         │
                         ▼
               ┌───────────────────┐
               │ Embedded Storage  │
               │ SQLite / RocksDB  │
               └───────────────────┘
```

## Repository

This is a **monorepo**. Core + SDKs + adapters + CLI + website all live together. See [MONOREPO.md](./MONOREPO.md) for layout and contributing guidance.

## Documentation

| File | Purpose |
|------|---------|
| [README.md](./README.md) | This file — project overview |
| [ROADMAP.md](./ROADMAP.md) | High-level milestones (v0.1 through v0.7) |
| [KANBAN.md](./KANBAN.md) | Detailed task breakdown for the current milestone |
| [MONOREPO.md](./MONOREPO.md) | Repository layout, build conventions, release process |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | Full design spec — primitives, schema, auth, migrations, branches, awareness, foundational decisions |

## License

AGPL-3.0-or-later. See [LICENSE](./LICENSE).

## Where to follow along

- Issues & discussions: this repo (GitHub)
- Site: [crdtsync.com](https://crdtsync.com) (coming)
