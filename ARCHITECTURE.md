# crdtsync

> Self-hosted collaborative sync backend with a portable CRDT core.
> [crdtsync.com](https://crdtsync.com)

Project lives in the [faisca](https://github.com/faisca) org alongside `fila` (messaging), `fakecloud` (local AWS emulator), `pensum` (tasks), and others.

## Vision

Build a language-agnostic collaborative sync engine inspired by Yjs/Liveblocks, but designed around:

- batteries-included deployment
- self-hosting first
- no Postgres/Redis dependencies
- portable CRDT core
- official backend
- horizontal scalability
- multi-language support
- offline-first synchronization
- operational simplicity

The core insight:

> Existing CRDT ecosystems solve the data structure problem, but not the infrastructure problem.

The goal is to create:

> A collaborative sync backend that can be deployed as a single container and embedded into applications across many languages.

---

# Core Product Positioning

## What this is

A realtime collaborative backend + portable CRDT engine.

Features:

- collaborative document editing
- offline-first synchronization
- realtime replication
- embedded persistence
- horizontal scaling
- awareness (cursors, selections, user identity, typing, viewport — what Liveblocks calls "presence")
- snapshots + compaction
- multi-language SDKs
- self-hosted deployment
- official sync protocol

## What this is NOT

### Not just a CRDT library

The focus is not "yet another CRDT implementation" or academic CRDT research. The focus is operational infrastructure, deployment simplicity, production-ready sync, batteries-included collaboration.

## Problems with existing solutions

### Yjs

Excellent CRDT, battle-tested, strong JS ecosystem. Backend story is fragmented, websocket providers handwritten, persistence DIY, scaling architecture unclear, multi-language editing awkward, operational setup fragmented.

### Liveblocks / hosted providers

Batteries-included, polished DX. SaaS lock-in, opaque internals, expensive at scale, less control, difficult self-hosting story.

---

# Main Product Goals

## 1. Batteries-Included Deployment

One command runs the whole thing. No Postgres, Redis, Kafka, NATS, etcd, external brokers. Storage, replication, pubsub, snapshots, clustering, failover, room routing all inside one deployable unit.

## 2. Portable CRDT Core

CRDT implementation exists exactly once. No reimplementing merge logic per language, no divergent implementations.

Core is implemented in **Rust** (`std`, a refcounted `Rc<RefCell<T>>` value graph, Miri-gated), exported as WASM and as a stable C ABI, wrapped by thin SDKs per language. The C ABI stays the canonical cross-language interface; the implementation language behind it is invisible to the SDKs.

## 3. Multi-Language Support

Clients in JavaScript, TypeScript, Python, Go, Rust, Node.js, JVM languages all edit the same document naturally.

## 4. Operational Simplicity

Should feel like SQLite, Tailscale, Fly.io, LiteFS. Not Kubernetes-first stacks.

---

# High-Level Architecture

```text
               Client SDKs
        JS / Python / Go / ...
                 │
                 ▼
          Shared CRDT Core
               Rust
                 │
        ┌────────┴────────┐
        ▼                 ▼
   WASM Export        C ABI Export
        │                 │
        ▼                 ▼
 Browser / Node     Native bindings


            Sync Server
               Rust
                 │
                 ▼
          Embedded Storage
```

---

# CRDT Model

Closed set of primitives. No generic CRDT abstractions.

- **Map** — string-keyed, recursive values
- **List** — ordered items, recursive values
- **Text** — collaborative char sequence, lives anywhere
- **XmlElement** — tag + attrs + children (children: XmlElement | Text)
- **XmlFragment** — root container of XmlElements (no own tag)
- **RangedElement** — generic ranged annotation (start_anchor, end_anchor, payload)
- **Register** — single LWW value
- **Counter** — increment / decrement

Document root is a Map of named top-level Elements.

*Built today (v0.2):* Map, List, Text, Register, Counter (plus the Scalar leaf). XmlElement, XmlFragment, and RangedElement are v0.5 — described here as the data model, not yet implemented.

## Rationale

Map / List / Text / Register / Counter cover structured collaborative apps (Kanban, settings, code editors, dashboards, forms). XmlElement covers document-style trees (ProseMirror, HTML, SVG, OOXML-shaped data) with first-class attributes that can themselves hold CRDTs. RangedElement is the generic ranged annotation: marks (bold / italic / link), comments, suggestions, highlights, mentions, domain overlays — all the same primitive, recursive payload.

## Why XmlElement not "Tree"

Generic Tree without attributes is a strict subset of XmlElement with attributes. XML three-way split (tag / attrs / children) fits document data — HTML, SVG, ProseMirror, RSS all chose it. Claim the data model, not the angle brackets. Wire stays binary.

---

# Extensibility

Primitive set is **closed**. Apps cannot define new CRDT types in app code.

## Why Closed

Custom CRDT types in app code = custom merge logic shipped per SDK language = divergence. Sandboxing custom merge has the same cost as the migration DSL machinery. Wire format stays compact only if op kind is an enum. Yjs, Automerge, Loro all reach this conclusion.

## Composition Covers ~95% of "Custom" Wants

Counter with bounds = Counter + clamp on read. Set = Map<key, true>. MV-Register = List<{ts, value}>. Position = Map { lat: Register, lng: Register }. Comment = RangedElement with payload. Tag list = Map<tag, true>. Nested data = composition of Map / List / Text / XmlElement.

What composition cannot cover: fundamentally new merge semantics. Rare. App approximates or proposes a new primitive through the escape hatch.

## Schema Customization (Not "Custom Types")

Apps freely customize within the schema layer: new XML element types, new mark names, new attr types, declared constraints, awareness entry shapes, ACL tuples. Structural / type-system features layered on fixed CRDT primitives.

## Escape Hatch

New primitives proposed via RFC, reviewed against criteria (cross-language implementability, schema fit, no conflict with existing primitives, real demand), accepted into core through normal release cycle. Bumps engine version; old clients reject the new kind at handshake.

## Cookbook

SDK ships a documented cookbook of "build this custom-feeling type from these primitives" recipes. Ships v0.2.

---

# Internal Data Model

Every operation is immutable and append-only. This describes the **wire/stored envelope**: identity, authorship (`client_id` + `actor_id`), scope (`room` / `branch` / `zone`), versioning (`schema_version`), causality (`lamport`), wall time (informational, not used for causality), kind, target, payload. The **core op** the CRDT engine actually merges is the inner subset — `{id, stamp, target, kind, tx}`; authorship, scope, schema version, and wall time are envelope concerns layered around the core op, not core op fields (see *Implementation Status & Divergences*).

Value types in op payloads: scalars, blob refs, element refs. (The blob-ref slot is reserved in the built op envelope as `Scalar::BlobRef`; the element-ref slot is not yet reserved — see *Implementation Status & Divergences*.)

---

# Client ID

Each connecting Document instance carries a `client_id`. Used for op identity (`op_id = (client_id, client_seq)`), per-instance undo, reconnect routing, audit. Distinct from `actor_id` (the authenticated human, from token).

## Locked Decisions

| Decision | Choice |
|----------|--------|
| Format | UUID v7 (128-bit, time-sortable, RFC 9562) |
| Generation | client-side at first Document instance |
| Server-issued | not supported |
| Lifetime | per-instance, persisted across same-instance restart (sessionStorage on web / app temp storage on native) |
| Multi-tab coordination | none — each tab is a distinct `client_id` |
| Multi-device | each device own `client_id`; same `actor_id` ties them |
| Wire encoding | 16 bytes binary |
| Trust model | `client_id` untrusted; `actor_id` (token) is trusted identity |
| Renewal | only on storage wipe; no rotation |
| v4 privacy-mode toggle | possible future config; no wire-format change |

Client-generated because CRDTs are offline-first — editing must work before first server contact. Per-tab gives up "same device = same client" abstraction in exchange for zero coordination complexity (no leader election, no BroadcastChannel, no SharedWorker).

---

# Important Design Principle: Intentions vs Internal Ops

SDKs expose high-level editing intentions. CRDT internals stay hidden. Server / core transforms intentions into actual CRDT operations.

---

# Anchors and Element IDs

Every Element receives a stable CRDT identifier at creation. Element IDs never change — survive renames, moves, structural mutations. All cross-references inside the document graph go through element IDs, never integer paths.

## Anchor Model

Anchors identify positions inside collaborative containers. Used for cursors, selections, marks, comments, RangedElement boundaries. Anchors tie to specific CRDT char / item IDs (not integer offsets) — survive concurrent inserts and deletes without drifting.

Exposed at SDK level as `RelativePosition`. Editor bindings (cursors, selections) must use these instead of integer offsets. Without them, cursors jump on remote edits. Core primitive, not a per-SDK concern.

---

# Text and Unicode

Permanent decisions. Yjs got this wrong and pays for it forever; we do not get to revisit it once shipped.

| Layer | Choice |
|-------|--------|
| CRDT identity granularity | codepoint (Unicode scalar value) |
| Wire encoding | UTF-8 |
| Internal storage | codepoint sequence with per-codepoint stable char_id |
| Public API default unit | grapheme cluster (via SDK Unicode helper) |
| Codepoint-level API | opt-in for advanced use |
| Unicode version mismatch | cosmetic only — codepoints stable, graphemes may render differently |
| Auto-normalization (NFC / NFD / NFKC / NFKD) | none — app responsibility |

## Why Not Other Combinations

Byte identity → multi-byte chars shatter, mid-byte cursor = corruption. Code unit (UTF-16) identity → Yjs's bug, mid-emoji cursor, family/flag emoji break. Grapheme cluster identity → Unicode-version-dependent, mathematically impossible to maintain identity across version mismatch. UTF-16 wire → doubles ASCII bandwidth. UTF-32 wire → quadruples for no win.

Codepoint identity + UTF-8 wire + grapheme-aware API is the only combination that preserves CRDT correctness across all clients and gives users grapheme-level UX.

## Why Codepoint Identity Works Across Unicode Versions

Codepoints are universal (Unicode is append-only). What differs is grapheme cluster boundaries. Mismatched versions = cosmetic rendering differences only. Both clients converge on the same codepoint sequence, both can edit, no data corruption, no CRDT identity break. Right failure mode.

## What Core Does Not Ship

NFC / NFD / NFKC / NFKD normalization (changes char_ids — app opt-in only if it accepts the cost). Locale-aware collation. Bidi / RTL display order. Locale-aware case folding. Word / sentence boundary detection beyond grapheme. Auto-repair of broken ZWJ sequences. Editor adapters handle their target editor's idiosyncrasies. Core stays Unicode-neutral beyond codepoint identity + grapheme helpers.

---

# Marks (Rich Text Formatting)

Range overlays on Text — bold, italic, links, highlights, comments. Convention over RangedElement, not a separate primitive.

## Open-Ended

Core does not predefine mark names. App decides what marks exist and how to render them.

## Merge Flavors

Each mark name needs declared merge semantics. Three kinds: **boolean** (presence only, add+add = present), **value** (LWW on conflict), **object** (each mark independent, no range merging across instances).

## Anchor Expansion

Per-mark flags control whether a mark grows when text is inserted at its boundary. Bold typically grows both ways; link typically grows neither.

## Algorithm

Peritext-style range CRDT (Litt, van Hardenberg, Kleppmann — Ink & Switch 2022).

---

# Map Slot Safety

`Map.set(key, value)` uses LWW. For scalar values, fine. For child CRDTs, convergence comes from **deterministic element_id derivation**, not API guardrails. Two clients concurrently creating "the same child" derive the same element_id from `(parent_id, key, kind)` and converge by construction. Derivation guarantees *convergence*; *propagation* is separate — creating a child emits a create-op so a peer learns the container exists before any op targets it (see *Implementation Status & Divergences*).

If a Set displaces an existing Element ref (e.g., set scalar onto a slot previously holding Text), the displaced element is **retained in a persistent per-id registry, not discarded** — a later Set that re-wins the slot reinstates the same element, and a displaced counter keeps accumulating. This is a convergence requirement, not a nicety: two replicas that saw the same ops must agree even across displace-then-recreate, so orphan-and-forget would diverge and is not an option. Core still surfaces an orphan event for the app; the state itself is kept. Orphaning is never silent.

Standalone CRDT construction (a la `new Text()` in Yjs) is intentionally not supported in v0.1: elements must be created at their final location so the deterministic id has a parent. Removes the "type not yet integrated" footgun.

---

# Algorithms and Invariants

## Causality

Total order: per-zone lamport timestamp + client_id tiebreak. Client order: client_seq monotonic per client. Wall clocks not trusted.

## Dependency Model: Lamport + Implicit (No Explicit Deps List)

Ops carry only lamport on wire. Causal dependencies are implicit through payload refs — each op references the char_ids or element_ids it operates on, and those refs ARE the dependencies. Receivers buffer ops whose refs point to unseen ids; apply when refs arrive.

Rejected: explicit per-op dependency lists (Automerge-style hashes), vector clocks (O(n_actors) per op). Lamport-only wins on smaller wire bytes and simpler protocol; CRDT primitives merge correctly regardless of concurrent-vs-causal distinction at engine level.

## Tree Moves (XmlElement)

Kleppmann 2021 ("A highly-available move operation for replicated trees"). Lamport-ordered apply, undo-and-replay on out-of-order receive, bounded undo log. Guarantees: exactly one parent per node, no cycles, no duplication, deterministic convergence.

## List

**Fugue** (Weidner & Kleppmann 2023, "The Art of the Fugue"). Tree-based, formally proven no-interleaving on concurrent inserts at the same point. Same algorithm reused for Text.

## LWW

Used by Register values, Map scalar set, XmlElement attr values, mark values of `kind: value`. Resolution: higher lamport wins, tiebreak by client_id.

## Tombstone GC

CRDT text/list deletions leave tombstones (required to position concurrent inserts). Watermark = min(last_seen_seq) across all known clients. At snapshot boundary: discard tombstones older than watermark. Offline clients block GC for ops they haven't acknowledged.

## Op Batching

Wire format supports run-length encoding for consecutive same-client inserts from v0.1, even if v0.1 encoder ships single-op only. Locking the format early avoids breaking changes later.

---

# Schema

Document carries an optional declarative schema. Schema-less docs work; apps that ship versioned releases over time should declare a schema.

## Why Declare

Producer-side op validation catches bugs at the write site. Type-aware SDK API. Enables deterministic invariant repair under concurrent merges. Enables schema migration with full history preservation. Cross-language: schema is JSON, every SDK enforces identically.

## Enforcement Points

Producer SDK rejects op that violates schema before sending (invalid ops never enter the log). Server ingress validates inbound (defense in depth). Apply boundary at every replica validates merged state (triggers Invariant Repair on violation).

## What Predefined vs Not

Core predefines: validation engine, mark kinds, attr type primitives, repair rules. App declares: type names, mark names, attr keys, allowed children, defaults, exclusivity, anchor expansion per mark, default block type for repair.

## Versioning

Every schema declares a version. Every Document records the schema_version it was created under. Versioning mandatory once a schema is declared.

---

# Invariant Repair

Concurrent merges can produce schema-invalid states even when each individual op is valid (e.g., schema says "at most one heading," Alice and Bob each insert one concurrently).

## Opinionated, Not Configurable

Core ships fixed repair rules. Apps don't pick. Configurable repair = configurable footguns + cross-language divergence + decision fatigue. Each rule is a deterministic function of (current state, schema, lamport order). All replicas independently converge to the same repaired state.

## Rule Shape

Orphan inline → wrap in declared default block. Disallowed child → drop. Exclusive collision → keep lamport-oldest, demote rest. Out-of-range scalar → clamp. Disallowed / mistyped attr → drop. Mark on disallowed type → drop. Tree-move cycle and Map slot type mismatch handled by their respective algorithms, not repair.

## Observation, Not Override

Apps cannot change what repair does. Apps can observe that it happened via a `repaired` event. UX uses: "we resolved a concurrent edit," offer undo, log, audit.

## Closure of Violation Set

Schema language has finite dimensions: type membership, children cardinality, attr presence / type / range, mark allowance, mark value shape. Every violation maps to one dimension. Every dimension has a rule. Schema declarations validated at parse time so apps cannot write a schema that admits unrepairable runtime states.

## Out of Scope: Semantic Invariants

Uniqueness, cross-field relations, aggregate constraints, reference integrity. Not in scope — not CRDT-mergeable with deterministic repair. Apps handle in app layer (producer-side best-effort, reactive UI warnings, derived aggregates). Boundary: **structure = core, semantics = app**.

---

# Schema Migration

When schema version changes between app releases, existing documents must be transformed. Migrations live in the core (same logic as CRDT merge — one implementation, deterministic, cross-language).

## Migrations as Log Entries

Op log is append-only forever, including migration entries. Every op carries its creation schema_version. Migration entries are checkpoints in the log. Replay walks entries in order. Preserves time-travel debugging, audit, rollback. Snapshots cache state at intervals — migration cost paid once when a snapshot crosses a migration boundary.

## Generated, Not Hand-Written

Schema is source of truth. Migrations derived artifacts. Same model as Prisma / Atlas / Rails / EF Core. Differ inspects schema change, emits migration file, app dev reviews, CI gates check drift + verify output.

## Two-Tier Expressiveness

Built-in step kinds (rename / add / remove / wrap / setAttr / mapValues / ...) cover ~80% of migrations. Pattern-rewrite DSL (selectors + transforms, pure, no I/O, terminating) covers custom tree rewrites tier 1 can't express. WASM tier-3 escape hatch deferred until real demand surfaces.

## Determinism

Migrations can't do I/O, wall-clock, random, network. Determinism is the entire reason migrations live in the core. If app needs user input for an ambiguous transform: run with safe default, surface follow-up edit task in UI, user-driven edits flow through the normal op stream after migration.

## Mixed-Version Sync

Server checks client schema_version on handshake. Gap covered entirely by bidirectional migrations → server translates ops in flight transparently, old client keeps working. Gap includes any forward-only migration → server rejects with `please-update-app`. Forward-only is the breakpoint.

## Four Detection Gates

1. **Drift detection** — declared schema vs cumulative migrations match. CI gate.
2. **Verification** — apply migration to fixture, validate result against new schema. Property-based variant generates random docs. CI gate.
3. **Server boot** — chain completeness + immutability via SHA-256 hash lock on applied migrations. Server refuses start on gap / out-of-sequence / hash mismatch.
4. **Per-doc runtime** — version reachability check. Missing chain → reject doc load with explicit error, don't corrupt.

## Detection Limits

Intent violations and semantically-wrong custom transforms are app-level test concerns. Structural correctness = detectable. Semantic correctness = not. Acceptable line.

---

# Transactions

Group of ops sent together as one wire message, batched into one local observer fire, treated as one undo entry. Optionally made atomic across replicas via opt-in.

## Default: Non-Atomic Batching

Most ops should be independent and stream as they arrive. Typing should appear character-by-character on remote screens. Non-atomic batching guarantees: client observer fires once, network sent as one message, undo treats as one intention, server log atomic write. Does **not** guarantee cross-replica view boundary — each op merges independently on arrival. CRDT default.

## Opt-In: Atomic

For cases where intermediate state is genuinely unsafe: privilege grant + use of new permission, delete + remove all refs, multi-element invariant schema cannot repair. Receivers buffer member ops until commit marker arrives; on commit, all apply atomically to local view. Costs latency, buffering complexity, partial-tx timeout handling.

## Why Atomic Is NOT Default

Atomic-by-default wrecks streaming UX. Typing "hello" pops in all-at-once when the typist pauses. Paragraph moves hidden until "all done." Cursor moves buffered, never feels live. CRDTs exist specifically to avoid coordination. Atomic-by-default reintroduces it for every op. Atomic is the deliberate override for the 5% that need it.

## Scope Constraints

Tx must stay within one branch, one zone, one schema version. Cannot include migration ops. Atomic tx member-op count capped (default 1000).

## Interaction with Invariant Repair

For atomic txs, repair runs inside the commit pipeline, not after. Visible effect of a tx is the repaired state. No two-step "tx done + then repair changed it" surprise.

## Interaction with Undo

A transaction is naturally an undo intention. Undo of atomic tx = generate inverse ops for all members, wrap in new atomic tx, apply atomically. Atomicity preserved through undo / redo.

## Not Shipped

Strong consensus / 2PC across replicas (defeats coordination-free property). Compare-and-swap / conditional ops (break CRDT mergeability; deferred to v0.7+ if demand). Cross-branch / cross-zone / cross-schema-version txs. Long-running txs (app state, not engine txs).

---

# Undo / Redo

Per-user undo via SDK helper. Core sees only inverse ops — no server-side undo state, no special wire format.

Each user's undo stack contains intentions (op groups) the user authored. Undo reverts only that user's ops, even when others' ops are interleaved. Per-op identity makes targeting precise.

Global undo (revert anyone's op regardless of author) is **not** supported — produces broken UX in collaborative settings. Apps that want "revert someone else's change" build it as a deliberate edit feature, not undo.

Inverse ops emit into the normal op stream. Ops that overwrite or delete state require prior-state capture at op creation time.

Auto-grouping on debounced gaps (>500ms idle = boundary by default). Manual begin / end intention for explicit grouping (paste, paragraph break).

Stack lives in SDK on client, persists in local storage. Offline editing produces undoable ops without network. Stack drops at migration boundaries.

---

# Persistence

Zero external infrastructure. As built, the store is a **per-room append-only file log** (`<room>.log`, one length-framed op per record) plus an optional `<room>.snap` compaction snapshot — no SQLite, no relational tables. Durability is hand-rolled: an append flushes before it returns; compaction lands atomically (temp → fsync → rename → directory fsync) before the log is truncated, and a crash-left overlap is deduped on replay. *Revisit:* the op hot-path is well served by the file log, but the admin UI / op-log viewer / audit-query / retention features described below want queryability, and durability is now bespoke (a directory-fsync crash bug already shipped and was fixed) — reconsider an embedded DB (SQLite/redb) for the metadata/index side if those consumers land (see *Implementation Status & Divergences*).

---

# Snapshots

Serialized materialized Document state. As built, a snapshot is keyed by the **server sequence** it covers (`base_seq`), not a lamport timestamp, and is generated on demand from the live merged replica. It makes replay fast and is the compaction artifact; it will also drive tombstone GC, migration checkpoints, and the versioning layer (those consumers are not built yet).

## Frequency Triggers

Op count since last snapshot (default 10,000), time (default 1 hour), migration boundary (always, immediately after), manual admin / app API. All tunable per room.

## Retention

Latest per branch always retained. Migration-boundary snapshots retained forever (or until explicit compaction) — only way to fast-replay across a migration. Periodic snapshots between migrations: rolling window, default keep last 3. Named versions retained until app deletes.

## Tombstone GC

Snapshots are when GC actually happens. Until a snapshot crosses the watermark, tombstones must be retained — offline clients could need them. **Not yet built:** current compaction retains all tombstones — there is no `min(last_seen_seq)` watermark GC (see *Implementation Status & Divergences*).

## Cold Start

When a client connects to a room it has not seen, catch-up returns **either** the ops since its last-seen sequence (at/above the room's compaction floor) **or**, if it fell below the floor, a whole-replica snapshot regenerated live — never snapshot-plus-tail. No full-history replay on the client. *Revisit:* regenerating a whole-replica snapshot per below-floor cold-start is O(state) CPU; cache it per floor if snapshots grow large or cold-starts get frequent (see *Implementation Status & Divergences*).

## Export / Import

Snapshots are portable. CLI ships export / import. Use cases: backup, cloning rooms (templates), cross-server moves, debug repro. Import bumps clocks past imported lamport; element / client IDs are namespaced so no identity conflict.

---

# Versioning and Branches

Snapshots are the storage primitive. Versioning is the user-facing layer on top. Apps that need named versions, restore, publish/draft workflows, per-user forks, or diff between revisions should not have to reinvent these.

## Named Versions

Snapshot + entry in a versions index. List, paginate, rename, delete are first-class.

## Auto-Version Triggers

Versions can be created declaratively in response to engine events (`before-publish`, `after-restore`, `before-migration`, ...) or schedules.

## Branches

Named pointer into the op log. Default branch `main`. Each branch has stable name, HEAD lamport, fork point. Branches share immutable history before their fork point — storage cost = only divergent ops past the fork. Adding a branch is cheap.

## Restore as Branch

Restore does not rewrite history or reset state vectors. Forks a new branch from a chosen snapshot, switches the active HEAD. Old branch preserved as immutable history. Offline-client ops in flight against the old HEAD land on the old branch, not on the restored live state — not lost, not corrupting. Audit version auto-created. Restore is itself a first-class log entry.

## Publish / Draft

Pattern: edit on `main`, sync a `published` branch's HEAD for read-only consumers. Republishing updates `published`'s HEAD pointer. Old `published` snapshots remain reachable as versions — apps can roll back published state independently of editor state.

## Per-User Branches

Same primitive supports per-user forks. Useful when each user customizes a base template (form-builder, dashboard, per-user filters) without affecting the shared base.

## Branch-Scoped Replication

`(room, branch)` is the unit of replication. Replica sets shard by `(room, branch)` if needed. Cross-branch sync via internal engine ops, not normal client ops.

## Schema-Aware Diff

Documents are structured Element trees with declared schema (not opaque blobs). Diff between any two snapshots is computable as structural change lists. Text values produce char-level diffs; XmlElement subtrees produce structural diffs; attrs / marks / Map / Register / Counter show value diffs. Engine ships sensible default renderers; apps can override.

## Branch Merging

Out of scope for v0.x. The primitive (fork point + HEAD pointers) is there; merge tooling can land later.

---

# Binary Blobs

Files, images, audio, video, PDFs. Treated as separate concern from the op stream because access patterns are fundamentally different (size, mutability, merge semantics, delivery, dedup).

Inlining blobs in the op stream wrecks everything: log balloons, snapshots bloat, every replica receives bytes whether or not they render. Blobs need a parallel system designed for their access pattern.

## Architecture: Refs in Ops, Bytes in Blob Store

Op payloads carry blob refs (random UUID + metadata), not raw bytes. Actual bytes live in a separate addressable blob store, fetched lazily on render.

Server-side, blobs are stored content-addressable (keyed by sha256) for dedup. Mapping random_id → sha256 lives server-side only — **never exposed on the wire or to apps**. Same bytes uploaded twice produce two distinct refs with two random IDs that internally point to one stored blob.

Gives global dedup without leaking content fingerprints. Confirmation attacks (adversary checking "does the server have this file?") blocked because public IDs are unpredictable.

## Blob Is a Value Type, Not a CRDT Primitive

Blobs don't merge, don't have substructure. Fit as values inside any container. Replacing a blob value = LWW on the assignment. No "edit" semantics. To "edit," upload a new version and assign the new ref.

## Inline Threshold

Small blobs (default ≤ 4 KB) embed directly in the ref to skip fetch roundtrip. Schema can override per field.

## Presigned URLs: Universal Interface

All upload and fetch goes through presigned URLs. **Engine never proxies blob bytes through its main RPC/websocket channel.** Backend-specific implementation; uniform SDK interface.

Trade-offs: engine cannot middleware-process bytes (compression, virus scan) without explicit middleware mode. Direct-to-S3 means engine doesn't observe upload — relies on S3 event hooks or post-upload verification. Local FS backend needs co-located HTTP route + signed-token verification.

Worth it for uniform API + CDN-native + bandwidth savings.

## Backends

Local filesystem (single-node, dev). S3-compatible (S3 / R2 / B2 / MinIO) for production. CDN tier and IPFS deferred.

## Authorization

Two-layer, server-side. Reference-site Element auth: can recipient read the Element containing the ref? Wire-level guarantee. Blob-fetch auth: server checks ACL in the context of the reference site that delivered the ref. No global "Alice can read blob X" tuple — auth flows through the containing element.

## Dedup

Same content → same sha256 → stored once. Reference counting across all docs / branches / snapshots. Big savings on user avatars, template assets, brand images, shared PDFs. Transparent to clients.

## Garbage Collection

When all reference sites disappear, blob becomes orphan. Default 30-day grace period (tunable) protects against undo restoring a ref, restore-as-branch re-referencing old blobs, mistaken delete recovery. Conservative — trades storage for safety.

## Wire-Format Reservation

Blob ref slot reserved in op envelope from v0.1, even though full implementation lands v0.5. Cheap now, painful later.

---

# Networking Layer

## Transport

WebSocket. WSS over TLS in production.

## Connection / Multiplexing

**One WebSocket per `(server, actor session)`. Logical channels multiplexed per `(room, branch, zone)` subscription.** Subscribe / unsubscribe via in-band control messages, runtime-mutable.

*As built (v0.2):* the server multiplexes many rooms over one connection — each Subscribe opens a client-assigned `Channel`, ops/snapshots/unsubscribes name their channel, and fan-out tags each peer on the channel it opened for the room. The SDK-side `ClientSession` still drives a single room (pinned to one channel); its multi-room extension is planned (see *Implementation Status & Divergences*).

Five docs in five tabs = five connections (per-tab `client_id`). Five docs in one tab = one connection with five channels.

## Handshake

Three phases (planned). *As built (v0.2):* two phases — Hello → Subscribe. There is no Auth phase, no `actor_id`, and no token validation yet (the `AuthFailed` error code is reserved but unused); `Hello` carries an untrusted, peer-asserted `client_id`. The Auth phase below is a v0.2 target (see *Implementation Status & Divergences*). Wire structure fixed; credential carrier deployment-pluggable.

1. **Hello** — version + codec negotiation. Format-stable header in the first 8 bytes (4-byte magic + 4-byte protocol version) so new codecs ship in later releases without breaking older clients.
2. **Auth** — only if credentials weren't present at upgrade. Pluggable carriers: cookie, WS subprotocol, `Authorization` header, in-band, mTLS, API key, query param (supported but logs leak). Credentials opaque bytes interpreted by deployment-configured verifier. Clients never assert `actor_id` — server derives it from verified credential.
3. **Subscribe** — repeatable, per `(room, branch)`.

Fast path: credentials present at upgrade → server validates during accept → auth state established → skip Phase 2. One round trip saved.

Operations before auth established: only Hello / Auth. Anything else = protocol violation, terminate.

Anonymous mode: server emits `actor_id = "anon:<random>"` if deployment policy permits. Treated as any other authenticated actor by authorization.

## Error Envelope

Standardized error response with closed enum code + human message + opaque details. Closed enum keeps wire compact, cross-language error handling uniform. New codes ship through engine releases. *As built:* code + message + an opaque `details` byte string, all three on the wire; `details` is reserved (round-tripped, empty) — no producer populates it yet, so the SDK error surface still exposes only code + message.

## Not Locked

Binary codec choice (CBOR / MessagePack / Cap'n Proto / custom) deferred to implementation, negotiated via Hello. *As built:* one custom deterministic little-endian codec (not CBOR/MessagePack), shared by the wire and the durable log; the 8-byte header reserves a version field for the negotiation, but only one codec exists today. Compression, framing details, TLS profile, heartbeat interval, op size limits — all infrastructure / runtime config.

---

# Realtime Synchronization

Connection flow: connect → authenticate → join room → send last_seen_seq → receive missing operations → subscribe to live ops.

Reconnect: client stored last_seen_seq, server replays missing ops.

---

# Idempotency

Every operation must be idempotent. Necessary because of reconnects, retries, failovers, duplicate packets. `op_id = (client_id, client_seq)` — server ignores already-seen ops.

---

# Offline-First

Local optimistic editing, offline op queues, reconnect sync, local snapshots. Enabled by embedding the CRDT core locally.

---

# Export Strategy

## WASM Export

Browser, Node.js, Electron. Single implementation, deterministic, web-distributable.

## Stable C ABI

Python, Go, Rust, JVM bindings. The C ABI is the canonical native interface.

SDKs are thin wrappers over the ABI.

---

# SDK Philosophy

SDKs contain serialization, networking, reconnect logic, API ergonomics. SDKs do NOT contain merge logic, causality logic, CRDT internals.

---

# Horizontal Scaling

## Constraint

No Redis / Postgres dependencies. Cluster layer must be internal.

## Room-Based Sharding

Each room maps to a replica set. Consistent hashing on `room_id` for deterministic placement, horizontal scaling, balanced distribution.

## Leader Model

Per room: leader handles writes, followers replicate. Clients can connect to any node; wrong node proxies or redirects to leader.

## Replication Flow

Client → leader → leader persists → leader replicates to followers → followers ACK → leader ACKs client.

## Durability

Recommended: ACK only after majority replication. Avoids losing acknowledged edits.

## Failover

Leader dies → followers elect new leader → clients reconnect → resume from last_seen_seq.

## Cluster Discovery

Static join via CLI flag, or gossip-based for liveness / room ownership / replication state / membership.

---

# Awareness

Ephemeral per-client state surfaced to others. Cursors, selections, user identity, typing indicators, viewport, mouse position, app-defined transient state.

Other libraries call this presence (Liveblocks, Slack, Firebase). We use awareness — the Yjs term, grounded in CSCW workspace-awareness literature, more accurate (cursor positions and viewport are not "presence" in the chat sense). Synonyms across ecosystems.

## Properties

- not durably persisted (ephemeral by design)
- not in op log, not in snapshots, not replicated for durability
- replicates on a separate lower-latency channel from doc ops
- per-entry TTL (some session-lifetime, others auto-expire after silence)
- per-entry throttle (server caps high-frequency entries like mouse / cursor)
- LWW per-client (each client owns its own state; no CRDT merge across clients)
- auth-filtered per recipient
- carries `actor_id` so receivers know which human is publishing

## Schema-Declared

Awareness entries declared in the same schema file as content. Entry has a type, TTL, throttle, publish / see auth. Schema-validated on publish — bad shape rejected at SDK before wire.

## TTL Handling

Server sweeps entries. `session` TTL cleared only on disconnect. Timed TTL cleared on expiry; removal broadcast. SDK auto-refreshes high-traffic entries (cursor) on activity; lets low-traffic entries (typing) expire naturally.

## Throttling (Two-Layer)

Client-side SDK debounces at throttle interval before sending. Server-side caps inbound — faster updates coalesce, keep latest only. Critical for mouse / cursor in whiteboard apps with many participants.

## Reconnect Grace Window

On disconnect, server marks state stale but doesn't immediately clear. Grace window (default 5s). Same `client_id` reconnects within grace → state preserved, no user-left fires. Grace expires → state cleared. Fixes flash-of-user-left-then-user-joined on brief reconnects.

## Anchors

Cursor / selection / viewport use the same `RelativePosition` model as doc anchors. Survive concurrent edits without drifting.

## Auth-Aware Filtering

Awareness is not pure broadcast — server filters per recipient. Two permissions per entry: publish (actor can publish), see (recipient can observe). Cursor in a private zone never sent to clients without access.

## Branch and Zone Scoping

Awareness scoped per `(room, branch)`. Anchors must target Elements in zones both publisher and recipient can access.

## Storage / Cluster

In-memory only. Not persisted, not in op log, not in snapshots. Leader holds state in memory, forwards ephemerally to followers. On failover, awareness lost — clients republish to new leader. Acceptable for ephemeral subsystem.

## What's Not Awareness

Things that look like awareness but belong in document content:

- "Show poll results everyone sees" → Counter / Register in doc content
- "Last edited by X at time Y" → audit log / content metadata
- "User X commented" → Comment is a RangedElement
- "Active users in this room" → derived from connected client states (awareness)

Rule of thumb: must persist beyond disconnect → not awareness.

---

# Admin UI

Lightweight dashboard. Rooms, connected users, ops/sec, snapshot size, replication lag, cluster health, op log viewer.

---

# Debugging

CRDT systems are difficult to debug. Tooling: op inspection, replay, timeline visualization, causal graph visualization, room export / import.

---

# Authentication

Engine validates signed tokens at connection time. Engine does **not** ship an identity provider — apps bring tokens from their own auth backend (JWT, OIDC, custom). Engine never issues tokens; the app's auth provider does.

For sharing / embed: app generates a restricted-scope token (limited role, scoped room / branch, near-term expiration).

`client_id` identifies device / session. `actor_id` identifies the human. Same user across two devices = same `actor_id`, different `client_id`. Critical for per-user undo, per-user branches, audit. `actor_id` mandatory from v0.1; dev-mode without auth uses anonymous tokens.

---

# Authorization

Authorization in a collaborative sync engine has to be first-class. Bolting it on after the fact is the most common reason CRDT-based apps end up reinventing huge amounts of infrastructure badly.

## Engine Ships

Token validation. Declarative policy enforcement. Two-tier auth model (schema-level defaults + doc-level dynamic ACLs). Wire-level redaction (unauthorized bytes never leave server). Audit log.

## Engine Does Not Ship

Identity provider, login, password reset, MFA. User / team / org management UI. Permission management UI (admins build their own). Organization modeling beyond claims in token.

## Two-Tier Model

**Schema-level `@auth`** — declared in schema, version-controlled, ships with app code. Static type-wide defaults: "all paragraphs writable by editor role."

**Doc-level ACL** — CRDT-merged state inside the document. Dynamic per-instance grants: "this specific comment readable only by Alice."

Apps need both. Schema covers default policy for things of type X. Doc-level covers specific instance Y has unique sharing. Matches Google Docs, Notion, Linear, AWS IAM.

## Subject Types

User, role, group — all first-class peers, composable. `authenticated:*`, `anonymous:*`, `*` (anyone) supported. Engine reads claims from token, never decides role membership itself; that's the app auth provider's job.

## Actions

Per room, branch, element, mark, version, snapshot, migration, awareness entry kind, and meta-auth on the ACL system itself.

## Resources

By room, branch, path (inherits downward), element id (survives moves), mark name, mark instance, version. Path-based inherit; instance-based precise.

## Templating

Schema `@auth` supports `${actor_id}` / `${author_id}` / `${room_id}` / `${branch_id}` resolved at check time. Expresses "user can do X to resources they own" cleanly without instance-by-instance tuples.

## Decision Flow

For every check:

1. Walk ACL tuples matching (actor, action, resource) and ancestors.
2. Any explicit DENY match → DENY.
3. Any explicit ALLOW match → ALLOW.
4. Schema `@auth` grants actor's role → ALLOW.
5. Otherwise → DENY (default-deny).

Standard IAM semantics: explicit deny always wins, user-specific not stronger than role for allow, absence of declaration = denial. Single source of truth used at every enforcement point.

## Enforcement Points

Connect, op submit, op outbound (per recipient), awareness publish / outbound, version create / restore / delete, branch create / delete, migration apply, snapshot export, ACL grant / revoke. Server is final authority. SDK exposes `canDo` for UI hints — client-side checks advisory only.

## Wire-Level Redaction

If bytes hit the client, assume they leak. Server never sends unauthorized data, ever. Per-recipient filtering on every op send and every cold-start snapshot.

## Zones (Coarse Partition)

For docs with large auth-uniform subtrees, declare zones — separately replicated streams. Per-zone lamport clocks (avoids cross-zone activity leakage). Client subscribes only to zones it's authorized for. Unauthorized zone ops, snapshots, structure, even element counts never sent. Cross-zone tree moves forbidden at schema level. Cross-zone anchors forbidden by default; opt-in opaque references for marks / comments.

Zones are a perf and isolation optimization. For fine-grained per-instance auth, ACL set carries the load. For coarse uniform-auth subtrees, zones are highly efficient. Both work together.

## ACL State Is Itself Privacy-Sensitive

Existence of "Alice can read X" leaks that X exists and Alice has access. ACL tuples redacted per recipient: sent only if recipient is the subject, or has `acl.read` on the resource. Admins see all. Regular users see only tuples involving them.

## Meta-Auth

Schema declares meta-rules about who can mutate the ACL subsystem. App tunes per-app: some apps let any editor share a section; some restrict grants to owner only.

## Producer-Side Defense in Depth

SDK won't let a client construct an op targeting elements / paths / zones it can't write to. Invalid op never leaves client. Server still re-validates — client-side is advisory.

## Audit

Op log is the authoritative record. Every op has `actor_id` + lamport + timestamp. Audit = log query. Separate access log for read-only actions (connect, snapshot export, branch read) since those don't generate ops.

## Hard Problems

### Offline Edits + Permission Revocation

User offline editing locally. Permissions revoked while offline. Reconnects → server rejects unauthorized ops with details. SDK surfaces "these ops were rejected" + op contents. App decides UX (discard / export / show user). Local state reverts to last server-acknowledged state. Not silent. Not data-loss without notice.

### Race: Op Submitted As Permission Revoked

Permission state itself is versioned in lamport time. Server checks ops against permissions at the op's lamport position. Deterministic across replicas.

### Schema Migration + Auth Migration

Auth declarations migrate alongside schema in the same migration files. Ops tagged version N checked against version N auth; ops tagged N+1 against N+1.

### Migration As Admin Op

Migration entries require `migration.apply` permission. Signed by admin actor. Server rejects from non-admins.

### Cross-Zone References

Comments anchored across auth zones, mentions in unauthorized zones, suggestions bridging zones — restricted by default. App can opt into opaque-reference behavior where the anchor is a token the client can pass back but cannot decode.

---

# API Surface

Main editing API is SDK-based. HTTP APIs mainly for observability, snapshots, exports, admin, cluster inspection.

---

# Deployment

## Single Node

One container. Provides websocket server, persistence, snapshots, admin UI.

## Cluster Mode

Room sharding, replication, failover, distributed ownership.

---

# Use Cases

Collaborative text editors (notes, docs, markdown, CMS). Kanban / productivity (tasks, boards, comments, shared state). Multiplayer apps (whiteboards, collaborative tools). Embedded sync engine (apps embed local core, sync automatically).

---

# Yjs Interoperability

A `fromYDoc` importer ships in v0.3 alongside the WASM / C ABI work.

## Scope

Snapshot import only: walk a Y.Doc's current state, reconstruct as native Document. One-way migration tool, not a live bridge. Imported doc starts fresh history; merge with live Yjs peers after import is not supported.

## Type Mapping

Y.Map → Map. Y.Array → List. Y.Text → Text (+ marks via RangedElement). Y.Xml* → XmlElement / XmlFragment / Text (v0.5+). Y.Doc → Document.

## Non-Goals

YATA wire-compat or binary update format — would amount to reimplementing Yjs core and defeat the portable-core architecture. Y.UndoManager parity — undo is reimplemented natively. Y.Awareness import — ephemeral, not part of the snapshot.

Importer framed explicitly as a **migration tool** to avoid setting expectations of drop-in replacement.

---

# Why Rust?

Same portability surface a C core would give, with memory safety the compiler enforces:

- **exports the same boundaries** — WASM (`wasm-bindgen`) for browser / Node, a stable **C ABI** (`cdylib` / `staticlib`, header generated by cbindgen) for every native language. The C ABI remains the canonical cross-language interface; SDKs never see the implementation language behind it.
- **memory safety without a GC** — ownership + borrow checking eliminate the use-after-free / double-free / aliasing hazard class at compile time; no GC pauses, predictable performance. The value graph is a downward tree of `Rc<RefCell<T>>` handles, so they never form a cycle and the whole graph frees from the root.
- **Miri gate** — every primitive runs under Miri for undefined-behavior + leak detection, deterministic and cross-platform; higher signal than a C sanitizer sweep.
- **`std`, not `no_std`** — `Vec` / `HashMap` / `Rc` compile to every target that matters; `no_std` buys nothing here.
- **mature toolchain** — cargo, property-based tests, fuzzers, Miri.

CRDT correctness (convergence, tombstones, id derivation, displacement semantics) is the same effort in any language — no type system enforces merge laws. That discipline comes from the test suites (which are the spec), fuzzing, and Miri. What Rust removes is the manual-lifetime hazard the equivalent C core carried by hand: internal allocation is no longer manual, and ownership is explicit only at the FFI boundary (`doc_new` / `doc_free`, `buf_free`), where `extern "C"` bodies wrap work in `catch_unwind` so a panic never unwinds past the ABI.

---

# Foundational Decisions

Decisions that shape the wire format, op model, or schema language. Bind early — adding them after v0.1 ships requires breaking changes.

**Status: all foundational decisions are decided.** Implementation choices (wire codec, compression, framing details, TLS profile, keepalive intervals, op size limits) are deferred to implementation time and can be revisited without breaking the model. ("Decided" means the *design* is settled, not that it is built — several rows are still planned; see *Implementation Status & Divergences* for what has shipped.)

| Status | Decision | Why foundational |
|--------|----------|------------------|
| decided | **Binary blob model** | Refs in ops, bytes in separate blob store, content-addressable internally (sha256), random UUIDs publicly. Universal presigned-URL interface across backends. Inline only for blobs ≤ 4 KB. ACL per reference site. |
| decided | **Atomic multi-op transactions** | Single transact API. Non-atomic batching default. Atomic opt-in for privilege / reference / cross-element invariants. Tx fields reserved in op envelope from v0.1. |
| decided | **Unicode / Text char-id strategy** | Codepoint as CRDT identity (stable across Unicode versions), UTF-8 on wire, grapheme-cluster API default with codepoint-level opt-in. Mismatched Unicode versions produce cosmetic differences only — no data corruption. |
| decided | **Op causality model** | Lamport timestamp + implicit dependency via payload refs. No explicit deps list, no vector clocks. Receivers buffer out-of-order ops by looking up referenced ids. |
| decided | **Custom Element types / plugin extensibility** | Closed primitive set. Wire-format op kind is a fixed enum. Apps cannot define new CRDT types in app code; they compose from existing primitives (cookbook ships v0.2). Genuinely new primitives ship through engine releases via RFC. App-level customization (XML types, marks, attrs, schema constraints, awareness, ACL) is fully supported through schema. |
| decided | **Client ID strategy** | UUID v7, client-generated, per-Document-instance, persisted across same-instance restart. Each tab a distinct client_id; multi-device handled by shared actor_id. 16 bytes binary on wire. |
| decided | **Connection / multiplexing model** | One WebSocket per (server, actor session); logical channels multiplexed per (room, branch, zone); subscribe / unsubscribe in-band. |
| decided | **Handshake structure** | Three phases (Hello / Auth / Subscribe); format-stable wire-version header in the first 8 bytes; pluggable auth carriers; opaque credentials; clients never assert actor_id. |
| deferred | **Wire format codec** (CBOR / MessagePack / Cap'n Proto / custom) | Negotiated via Hello; new codecs ship in later releases without breaking older clients. |
| deferred | Compression, framing, TLS profile, keepalive, op size limits | Implementation / infrastructure, not foundational. |

## Additive (No Foundational Pressure)

Can land cleanly later without breaking the v0.1 model: editor adapter contract, storage layout refresh, search / indexing, quotas / rate limits, debugging tools, E2E encryption, branch merging, webhooks / external integrations.

---

# Implementation Status & Divergences

This document is the **end-state** — the full scope + intended design; everything here is meant to be built eventually. The **live worklist is [KANBAN.md](KANBAN.md)** (the prioritized breakdown of what's not yet built), and design changes that implementation forced are logged in [DECISIONS.md](DECISIONS.md). As the Rust core, server, and SDKs were built (v0.1 → v0.2, 2026-07), several concrete choices diverged from the prose above. This section is the reconciliation: where they disagree, the note here (and the code) is authoritative.

## Deliberate divergences — code is authoritative

- **Core language is Rust**, not C — a downward `Rc<RefCell<T>>` value graph, `#![forbid(unsafe_code)]`, Miri-gated. Portability is unchanged: a stable C ABI (cbindgen) for native SDKs + wasm (wasm-bindgen) for the browser. Native hosts embed the C ABI directly; only JS gets wasm (no wasm runtime embedded in a native host). Host seam is `entropy()` + `now()` only; `std`, not `no_std`.
- **Two op layers.** The *core op* carries only what merge needs — `{id, stamp, target, kind, tx}`. Authorship (`actor_id`), scope (`room`/`branch`/`zone`), `schema_version`, and wall time are **wire/server-envelope** concerns wrapping the core op, not core op fields.
- **element_id derives from `(parent_id, key, kind)`** — the kind is in the tuple, so a type-flip on a slot yields a different id, which drives the displacement path correctly.
- **Displacement retains, it does not forget.** A displaced container/counter is kept in a persistent per-id registry and *reinstated* if its slot is re-won; a displaced counter keeps accumulating. This is a **convergence requirement** — orphan-and-forget (as the older Map Slot Safety prose implied) diverges across replicas. The orphan event still fires for the app; the state is retained.
- **Creation emits an op.** Get-or-create emits an op on the create path (silent on get). Derivation gives *convergence* for concurrent same-slot creates; the op gives *propagation* (a peer learns the container exists before a child op targets it). Both are needed — "convergence by derivation, not API" holds for convergence only.
- **The op-log is the source of truth; a snapshot is a compaction artifact,** not a separate cold-start channel. Every state change is an op; replaying the log reproduces the state.
- **Persistence is a per-room append-only file log** + optional `<room>.snap` snapshot — not SQLite. Crash-safety is hand-rolled (append flushes before return; compaction is temp → fsync → rename → dir fsync → truncate, with dedup-on-replay).
- **One binary codec, shared by the wire and the log.** Deterministic little-endian, length-framed, total-decode (a `DecodeError`/`ProtocolError`, never a panic). Not CBOR/MessagePack. The 8-byte header (`"CRDT"` magic + version) reserves the version for future codec negotiation.
- **Compaction is keyed on the server sequence** (`base_seq`), not a lamport timestamp. Cold-start (`catch_up`) returns **either** an op delta (at/above the room's floor) **or** a whole-replica snapshot regenerated live (below it) — never snapshot-plus-tail.

## Planned, not yet built (the prose above reads present-tense — it isn't yet)

- **Auth** — the handshake is Hello → Subscribe today; no Auth phase, no token validation, no `actor_id` (the `AuthFailed` code is reserved, unused). `Hello` carries an untrusted, peer-asserted `client_id`. Three-phase auth + `actor_id` is a v0.2 item.
- **Connection multiplexing** — the server multiplexes many rooms over one connection via client-assigned channels; the SDK-side `ClientSession` still holds a single room on one channel. Multi-room `ClientSession` is the remaining piece.
- **Tombstone GC / watermark** — compaction retains all tombstones; no `min(last_seen_seq)` watermark, no retention window ("keep last 3"), only an op-count trigger (no time/migration triggers). Snapshot state grows with tombstones until GC lands.
- **Element-ref envelope slot** — the `tx` and blob-ref slots are reserved (`Scalar::BlobRef`); the **element-ref value slot is not**. Its shape is under-specified and it carries no v0.1 reservation promise, so it is deferred until its design settles. Tracked in KANBAN.
- **Op-batching RLE** — the codec frames one op per record; cross-op run-length encoding is a later additive op kind.
- **Also absent:** Error `details` field, `RelativePosition`/anchor SDK type, client_id generation/persistence in the SDKs (they take a caller-supplied 16-byte id), codec negotiation, and the XmlElement / XmlFragment / RangedElement primitives (v0.5).

## Revisit items (accepted now, flagged for a later look)

- **File-log vs. an embedded DB for the query/metadata side.** The append-only file log is right for the op hot-path, but the admin UI / op-log viewer / audit-query / retention features want queryability, and durability is now hand-rolled (a directory-fsync crash bug already shipped and was fixed). Reconsider SQLite/redb for the *metadata/index* side if those consumers land — a checkpoint, not a reversal.
- **Cold-start snapshot CPU.** A below-floor subscriber triggers a whole-replica `encode_state` regenerated live on every cold-start — O(state) CPU per connection. Fine at current scale; cache the encoded snapshot per compaction floor if snapshots grow large or cold-starts get frequent.

---

# Roadmap

> **Live build status** — what's actually shipped vs. in progress lives in [KANBAN.md](KANBAN.md); this roadmap is the plan of record. Build order has diverged where dependencies allowed: the portable-runtime work (WASM, C ABI, Python, Go bindings) landed early alongside the v0.1 core rather than waiting for v0.3.

## v0.1 — Single Node MVP

Websocket sync, room support, op log, snapshots, embedded persistence, TS SDK, shared CRDT core, primitives (Map, List, Text, Register, Counter), anchors / RelativePosition, Map slot safety, op batching wire format, token validation + actor_id, blob ref reservation + local FS backend + small-blob inline, tx field reservation + non-atomic transact, Text codepoint identity + UTF-8 + grapheme helpers, closed op kind enum, UUID v7 client_id, single multiplexed WS, three-phase handshake, standardized Error envelope.

## v0.2 — Developer Experience

Declarative policy file with audit log, awareness subsystem (TTL + throttle + auth filtering + reconnect grace), reconnect, compaction with tombstone GC watermark, admin dashboard, replay tooling, UndoManager for v0.1 primitives, composition cookbook, named versions + auto-version triggers.

## v0.3 — Portable Runtime + Interop

WASM export, stable C ABI, Python bindings, Go bindings, Yjs snapshot importer.

## v0.4 — Distributed Cluster + Branches

Room sharding, replication, failover, leader election, cluster membership, first-class branches, branch-scoped replication, branch-level ACL, restore-as-branch, publish / draft, per-user branches.

## v0.5 — Rich Text, Document Trees, Schema

XmlElement / XmlFragment / RangedElement, Marks (Peritext-style), Kleppmann tree-move, declarative Schema + producer-side validation, Invariant Repair, sync-prosemirror adapter, UndoManager extensions, schema-aware diff, schema-level `@auth`, doc-level ACL CRDT subsystem, zones + per-zone streams + wire-level redaction, S3-compatible blob backend + dedup + GC + range requests, atomic transactions opt-in.

## v0.6 — Schema Migration

Migration entries as first-class log entries, per-op schema_version tagging, two-tier migration format, migrate CLI suite, schema-diff-based generation, schema annotations, four detection gates, mixed-version sync, migration immutability via hash lock, ACL audit / query CLI, opaque cross-zone anchors.

## v0.7 — Production Features

Metrics, tracing, snapshot export / import, replication tuning, durability modes, compaction policies, WASM migration escape hatch (if demand), CDN-tier blob fetches, per-tenant HMAC-keyed blob hashing.

## Potential Future

Binary attachments / media synchronization. End-to-end encryption. Edge deployment (small sync nodes geographically).

---

# Final Positioning

**crdtsync** should be positioned as:

> A self-hosted collaborative sync backend with a portable CRDT core.

Not merely:

> A CRDT library.

Differentiation: batteries-included infrastructure, operational simplicity, no external infra dependencies, portable shared runtime, multi-language editing, first-class versioning / branches / schema / auth / awareness, official backend architecture, self-hosted deployment, horizontal scalability.

---

# One-Sentence Pitch

> **crdtsync** — open-source collaborative sync infrastructure with a portable CRDT core, deployable as a single container with no Redis or Postgres required.
