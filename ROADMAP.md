# Roadmap

High-level milestones. Detailed feature bullets per milestone live in [ARCHITECTURE.md → Suggested Roadmap](./ARCHITECTURE.md#suggested-roadmap).

**Current status: v0.0.0 — pre-implementation. All foundational design decisions locked. Implementation has not started.**

---

## v0.1 — Single Node MVP

First working sync engine. Single OCaml server, no cluster, one binary, one storage backend.

- core CRDT primitives: Map, List, Text, Register, Counter
- WebSocket sync server with SQLite persistence
- OCaml SDK (consumes core directly, no FFI) — sibling of the other SDKs and a second-client implementation that validates the wire protocol against the server in the same workspace
- TypeScript SDK (via WASM core)
- op envelope finalized per Foundational Decisions
- basic auth (token + actor_id), single multiplexed connection per actor
- snapshots + local blob backend
- enough to demo "two clients edit the same doc"

## v0.2 — Developer Experience

Make the v0.1 engine usable by app developers.

- declarative auth policy (roles / paths / audit log)
- awareness (cursors, selections, user identity) with auth filtering
- UndoManager (per-user, redo, intention grouping)
- named versions + auto-version triggers
- admin dashboard + replay / compaction tooling
- composition cookbook (Set, MV-Register, etc.)

## v0.3 — Portable Runtime + Interop

Multi-language story.

- WASM export polished for browser + Node
- stable C ABI for native bindings
- Python, Go bindings
- Yjs one-way snapshot importer (`fromYDoc`) for migration
- sharing/embed token helpers

## v0.4 — Distributed Cluster + Branches

Horizontal scale and first-class branches.

- room sharding + replication + leader election + failover
- gossip / static cluster discovery
- branches as first-class (named pointers, fork points, per-branch HEAD)
- restore-as-branch, publish/draft, per-user branches
- branch-level ACL + branch-scoped replication

## v0.5 — Rich Text, Document Trees, Schema

The hardest milestone. Editor-grade collaboration + schema as first-class.

- XmlElement / XmlFragment / RangedElement primitives
- Peritext-style marks
- Kleppmann 2021 tree-move algorithm
- declarative Schema (versioned, JSON, optional)
- opinionated Invariant Repair (fixed rules, no config)
- schema-level `@auth` + doc-level ACL CRDT subsystem
- zones + per-zone replication + wire-level redaction
- atomic transactions opt-in (`atomic: true`) with repair-in-commit pipeline
- `sync-prosemirror` adapter
- schema-aware diff API

## v0.6 — Schema Migration

Schema evolution with full history preservation.

- migration entries as first-class log entries (no history rewrite)
- two-tier migration format (built-in step kinds + pattern-rewrite DSL)
- `crdtsync migrate` CLI (status / check / verify / test / generate / apply)
- schema-diff-based migration generation (Prisma-style)
- mixed-version sync (bidirectional / forward-only translation)
- migration immutability via SHA-256 hash lock
- ACL audit / query CLI

## v0.7 — Production Features

Operational polish for real-world deployment.

- metrics, tracing
- snapshot export/import with `--with-blobs`
- replication tuning, durability modes
- CDN tier for blob fetches
- per-tenant HMAC-keyed blob hashing (ultra-privacy mode)
- federation: `crdtsync blob sync` for cross-server moves
- WASM tier-3 migration escape hatch (if real demand surfaces)

## Beyond v0.7

Not currently milestoned. Candidates: binary attachments beyond blobs (audio/video tooling), end-to-end encryption of payloads, edge deployment, IPFS backend, branch merging UX, exotic CRDT primitive proposals via RFC.

---

## Process notes

- Versioning is **lockstep** across all components (core, SDKs, adapters, CLI, website). Single `VERSION` file at repo root drives every release.
- Foundational decisions are frozen (see [ARCHITECTURE.md → Foundational Decisions](./ARCHITECTURE.md#foundational-decisions)). Implementation choices (wire codec, compression, framing, etc.) can be revisited.
- New CRDT primitives require an RFC and ship through normal engine releases.
- This is an active-construction project. Milestones may shift. Don't depend on dates that aren't here yet because there aren't any.
