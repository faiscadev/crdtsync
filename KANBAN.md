# Kanban — v0.1 Single Node MVP

> Detailed task breakdown for the [v0.1 milestone](./ROADMAP.md#v01--single-node-mvp).
> Higher-level milestones live in [ROADMAP.md](./ROADMAP.md). Full design in [ARCHITECTURE.md](./ARCHITECTURE.md).

**Status convention:**

- `- [ ]` = todo
- `- [x]` = done
- Inline `(in progress)` or `(blocked: <reason>)` annotates current state when relevant

---

## Goal

Ship a working single-node sync engine. One OCaml server, SQLite persistence, TypeScript SDK via WASM, two browsers can collaboratively edit a doc using all core primitives, with basic auth and blob support.

## Repo Hygiene & Setup

- [ ] **SETUP-1** OCaml workspace (`dune-project`, base lib + bin structure, build verified)
- [ ] **SETUP-2** TS SDK workspace (pnpm + tsconfig + build pipeline + test runner)
- [ ] **SETUP-3** CI: path-filtered per-component jobs (core / wasm / cabi / sdk-ts / lint / e2e)
- [ ] **SETUP-4** Lint + format baseline (ocamlformat, prettier, eslint)

## Core CRDT (OCaml)

- [ ] **CORE-1** Op envelope type + per-zone lamport infrastructure + UUID v7 generation
- [ ] **CORE-2** Map primitive (with slot safety: `initOnce`, `live`, `replace`, orphan event)
- [ ] **CORE-3** List primitive
- [ ] **CORE-4** Text primitive (codepoint identity, char_id, UTF-8 in/out)
- [ ] **CORE-5** Register + Counter primitives
- [ ] **CORE-6** Anchors / RelativePosition (CharAnchor / IndexAnchor / Whole)
- [ ] **CORE-7** `doc.transact()` server-side semantics: client batching + log atomic write (non-atomic default)
- [ ] **CORE-8** Closed `op.kind` enum + dispatch

## Persistence (OCaml)

- [ ] **PERSIST-1** SQLite schema + open/close lifecycle
- [ ] **PERSIST-2** Op log: append-only writes, replay reads, per-client `last_seen_seq` tracking
- [ ] **PERSIST-3** Snapshot table + envelope serialization
- [ ] **PERSIST-4** Snapshot triggers (op-count + time intervals, manual API)
- [ ] **PERSIST-5** Cold-start delivery (latest snapshot + ops_since(snapshot.lamport))

## Wire Protocol (OCaml)

- [ ] **WIRE-1** Codec pick + implement (CBOR recommended — deferred-to-impl decision, lock now)
- [ ] **WIRE-2** Format-stable wire-version header (4-byte magic + 4-byte version)
- [ ] **WIRE-3** Hello / ServerHello exchange + codec/capability negotiation
- [ ] **WIRE-4** Auth phase: bearer header on upgrade + in-band `Auth` message (cookie carrier post-v0.1)
- [ ] **WIRE-5** Subscribe / SubscribeAck + multiplexed channels per `(room, branch, zone)`
- [ ] **WIRE-6** Standardized `Error` envelope (closed-enum code + message + details)
- [ ] **WIRE-7** `BlobRef` value type + `tx_id` / `tx_role` fields reserved in op envelope

## Server (OCaml)

- [ ] **SERVER-1** WebSocket server skeleton (accept, run handshake, dispatch by channel)
- [ ] **SERVER-2** Room support (create implicit on first connect, route ops to channel subscribers)
- [ ] **SERVER-3** Op apply pipeline (validate kind + actor + room ACL, persist, fan-out)
- [ ] **SERVER-4** Reconnect resume from `last_seen_seq` per channel
- [ ] **SERVER-5** Token validation (one carrier for v0.1: JWT bearer) + actor_id binding to session
- [ ] **SERVER-6** Basic auth enforcement: room-level `read` / `write` on every op
- [ ] **SERVER-7** `crdtsync serve` CLI binary

## Blobs (OCaml)

- [ ] **BLOB-1** Local FS backend (put / get by random UUID, internal sha256 dedup, ref-count storage)
- [ ] **BLOB-2** HMAC-signed presigned URL generation (PUT + GET with expiry)
- [ ] **BLOB-3** Co-located HTTP route for blob PUT/GET with signed-token verification
- [ ] **BLOB-4** `requestUpload` / `confirmUpload` / `requestFetch` wire ops + flow

## TS SDK

- [ ] **SDK-1** WASM build of OCaml core (`wasm_of_ocaml` / `js_of_ocaml` pipeline + glue)
- [ ] **SDK-2** Document open/connect API + client-side handshake
- [ ] **SDK-3** Map / List / Text / Register / Counter ergonomic wrappers + observe API
- [ ] **SDK-4** Anchors / RelativePosition surface
- [ ] **SDK-5** `doc.transact()` ergonomic wrapper (observer fire boundary + intention undo unit)
- [ ] **SDK-6** Grapheme-aware Text helpers (`Intl.Segmenter`)
- [ ] **SDK-7** UUID v7 generation + sessionStorage persistence for `client_id`
- [ ] **SDK-8** Reconnect logic + per-channel `last_seen_seq` tracking
- [ ] **SDK-9** Token / auth helpers (set bearer, prepare upgrade headers / in-band Auth)
- [ ] **SDK-10** Blob upload / fetch / url helpers (presigned URL two-step + inline ≤4KB)
- [ ] **SDK-11** `Error` envelope decoding + typed error surfaces

## Demo & E2E

- [ ] **DEMO-1** Minimal "two clients edit the same Text + Map" demo (no UI framework, just SDK in browser)
- [ ] **DEMO-2** E2E integration test harness (spin up server, connect two clients, assert convergence + reconnect + auth rejection)

---

## Definition of Done — v0.1

- All checkboxes above complete
- `crdtsync serve` runs single-node, accepts WebSocket connections, persists ops + snapshots to SQLite
- Two browsers can collaboratively edit Map / List / Text / Register / Counter and converge to identical state
- Reconnect resumes from `last_seen_seq` per channel without data loss
- JWT bearer auth works: token validates, `actor_id` bound, room-level read/write enforced server-side
- Blobs upload via presigned URL, reference from a doc, fetch via presigned URL (with inline-under-4KB fast path)
- All foundational op envelope fields (`actor_id`, `branch`, `zone`, `schema_version`, `lamport`, `tx_id`, `tx_role`, `BlobRef` value type) present on the wire, even if not all fully utilized in v0.1
- CI green on push to main

## Notes

- Some fields exist in the op envelope from v0.1 but aren't fully exercised until later milestones (e.g., `zone` defaults to a single implicit zone in v0.1; `schema_version` is hardcoded; `tx_id` / `tx_role` non-null only inside `doc.transact()`). The wire format reserves them; behavior fills in over time.
- Schema, migrations, ACL CRDT subsystem, branches, awareness, rich text — none of those are v0.1 scope. See ROADMAP.md for which milestone each lands in.
