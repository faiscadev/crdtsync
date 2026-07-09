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

*Built today (v0.2):* Map, List, Text, Register, Counter (plus the Scalar leaf). XmlElement, XmlFragment, and RangedElement are v0.5.

**XmlElement / XmlFragment are composition + one hard algorithm, not new machinery.** `XmlElement { tag: String, attrs: Map, children: <Fugue sequence of XmlElement | Text> }` reuses the **Map** primitive for attrs (attrs hold CRDT values, not just scalars) and **Fugue** (the List/Text sequence engine) for children; `XmlFragment` is a tagless, attr-less children sequence — the document tree's root container. The only genuinely new algorithm is the **tree move** (§Tree Moves), sliced separately from the structural build (create / edit / delete children) so the structure lands before the hardest, most bug-prone part.

**RangedElement is a first-class generic annotation, not a Text-local one.** A `RangedElement { start, end, payload }` where each endpoint is an anchor `(element_id, RelativePosition)` — so a range may span elements (a comment from one paragraph to another), not only a single Text run. RangedElements live in a **document/fragment-level annotation set** (a CRDT set keyed by RangedElement id), not inside the Text they annotate; "the marks on this Text" is a query over that set filtered by `element_id`. Marks are a convention over RangedElement (§Marks).

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

Value types in op payloads: scalars, blob refs, element refs. Both ref slots are `Scalar` leaves: `Scalar::BlobRef` (reserved, #60) and `Scalar::ElementRef(ElementId)` — a leaf that names another element in the same room (mentions, links, foreign keys). An element-ref is a plain LWW value like any scalar: no substructure, does not merge; a dangling target (the element was deleted) is an app concern, not a merge concern. It carries a bare `ElementId` (references are same-room — a room is the sync-isolation unit, so no room qualifier is needed); a `kind` hint can be added later if schema validation wants it. Reserved forward-compat like the blob-ref slot — round-tripped in the codec, no producer / consumer yet.

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

NFC / NFD / NFKC / NFKD normalization (changes char_ids — app opt-in only if it accepts the cost). Locale-aware collation. Bidi / RTL display order. Locale-aware case folding. Word / sentence / grapheme boundary detection. Auto-repair of broken ZWJ sequences. Editor adapters handle their target editor's idiosyncrasies, grapheme segmentation included. Core stays Unicode-neutral: codepoint identity only, no Unicode-segmentation dependency (*as built* — see *Implementation Status & Divergences*).

---

# Marks (Rich Text Formatting)

Range overlays on Text — bold, italic, links, highlights, comments. Convention over RangedElement, not a separate primitive.

## Open-Ended

Core does not predefine mark names. App decides what marks exist and how to render them.

## Merge Flavors

Each mark name needs declared merge semantics (in the schema `marks` block). Three kinds: **boolean** (presence only — concurrent add + add = present; concurrent **add + remove** on the same span resolves **LWW by stamp**, the highest-stamped op covering a character decides its presence — consistent with Register LWW), **value** (LWW on conflict — e.g. a link's href), **object** (each mark instance independent, no range merging across instances — e.g. comments; two overlapping comments both exist).

## Anchor Expansion

Per-mark flags control whether a mark grows when text is inserted at its boundary. Bold typically grows both ways; link typically grows neither. This maps directly onto the **`RelativePosition` gravity** already built (`Before` / `After`): a boundary anchor's gravity *is* its expansion direction, so anchor expansion needs no new mechanism — it is the gravity chosen for the mark's start / end anchor.

## Algorithm

Peritext-style range CRDT (Litt, van Hardenberg, Kleppmann — Ink & Switch 2022).

## Representation

A mark is a **RangedElement** (§CRDT Model) whose `payload` carries the mark name + value, whose `start` / `end` anchors are `RelativePosition`s (gravity = anchor expansion), stored in the document/fragment-level annotation set. The active marks on a character are **computed** from the set — each character's mark state is derived by resolving every RangedElement of a given name that covers it, per that name's declared flavor (boolean → LWW-by-stamp presence, value → LWW value, object → the set of instances). No per-character mark storage; the RangedElement set is the source of truth and per-character state is a read-time computation, so it converges by construction (a deterministic function of the merged mark set). A cross-element RangedElement (comment spanning paragraphs) is the same primitive with `start.element ≠ end.element`.

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

CRDT text/list deletions leave tombstones (required to position concurrent inserts). **Tombstones are never removed** — removing a node another replica still references would force either orphan-reparenting (a replicated GC op with forwarding, since peers keep tombstones the server drops) or edit-rejection (silent-ish data loss on cold regions). Both are complex or lossy, and removal buys nothing convergence can't get more cheaply. Instead tombstones are **compressed**: a contiguous deleted run — consecutive char/list ids that form one insert's parent-chain, all tombstoned — encodes as a single range record, and a tombstone's dead value (never read again) is dropped. A deleted region costs O(runs), not O(deleted items); real editing deletes contiguous spans (words, lines, paragraphs), so runs are few. Convergence is untouched — same logical state, fewer bytes — so it needs no watermark, no client acknowledgement, and no distributed decision. This is what mature CRDTs (Yjs's deleted-item structs, Automerge's columnar RLE) actually ship.

## Op Batching

Wire format supports run-length encoding for consecutive same-client inserts from v0.1, even if v0.1 encoder ships single-op only. Locking the format early avoids breaking changes later.

---

# Schema

Document carries an optional declarative schema. **Schema is opt-in** — schema-less documents are first-class: they converge, persist, fan out, snapshot, sync offline, and enforce room-level ACLs with zero ceremony. A schema is adopted only to unlock the schema-gated feature tier (producer validation, invariant repair, migration, fine-grained `@auth`, type-aware SDK API, awareness TTL / throttle, marks / attrs / structural constraints). Nothing requires one; adding a schema is a later, incremental choice, so adoption is never a prerequisite for using the engine.

## Why Declare

Producer-side op validation catches bugs at the write site. Type-aware SDK API. Enables deterministic invariant repair under concurrent merges. Enables schema migration with full history preservation. Cross-language: schema is JSON, and the core is the sole validator — every SDK forwards the schema bytes to core rather than reimplementing validation, so "every SDK enforces identically" holds by construction (one implementation).

## Schema Is Code, Not Document State

A schema is an app-developer artifact: authored as a JSON file, versioned, checked into the app repo, CI-gated. It is **never** carried inside the document — the document records only the `schema_version` each op was created under (an envelope field). Schema-as-document-state is rejected: it would make the schema concurrently mergeable (destroying migration determinism), defeat the CI drift / verification gates and the boot-time hash-lock, and create a bootstrap cycle (reading the doc would require the schema the doc contains).

## Distribution

Schema reaches the two parties that use it through separate channels:

- **Client** — a build-time **bundle** (required for the code-generated type-aware API and for cold-offline validation before first server contact) and / or a **handshake advertisement** (an enforcing server sends its active schema + version; the client caches it across restarts). Bundling is therefore optional: a *typed* client bundles (its accessors are generated code), a *dynamic* client fetches at handshake and adopts whatever version the server serves.
- **Server** — **registration**, not deploy-time config. The app owner's CI pushes `{app_id, version, schema, generated_migrations}` to the server over an admin API on release. The server stays a generic engine (it hardcodes no app's types) while serving any tenant — a multi-tenant SaaS server is a per-`app_id` schema **registry**. A connecting client names its `app_id` + `schema_version`; the server resolves that to the schema it holds.

## Two Server Tiers

CRDT merge needs no schema — the core op `{id, stamp, target, kind, tx}` converges on its own — so a server hosts an app at one of two tiers. **The tier is decided per `app_id`** (by whether that app registered a schema), not globally and not per document: one binary serves enforcing apps and relay apps side by side.

- **Relay** (app not registered) — stores, dedups, fans out, persists, snapshots; enforces only connection / room-level ACLs. No ingress validation, server-side repair, or in-flight migration. Clients still validate and repair locally against their own schema (deterministic repair converges regardless). This is the zero-config default, and it hosts apps that never registered.
- **Enforcing** (registered schema) — adds producer-ingress validation (defense in depth), authoritative invariant repair, in-flight version translation, and schema-level `@auth`.

## Trust Boundary

A client-supplied schema **body is never trusted for enforcement**. The enforcing server enforces only its **registered** schema; a connecting client asserts a version *number*, used solely as a lookup key into the server's registered set (an unknown version is rejected, not fabricated). The registered schema is admin-provisioned — **registration is a meta-authed surface** (the app owner's CI credential, distinct from any sync connection) and hash-locked, so a client cannot slip a different body under a known version. A client's own schema is **advisory**: it drives the client's optimistic local validation / repair / typing, and the server re-validates every op against the trusted registered schema — client-side is advisory, the server is final authority. (Repair is `f(state, schema, lamport)`, so it converges across replicas *only* when they share the schema; the registered server is the arbiter that corrects a replica which repaired under a divergent schema.)

## Registration

Registration is a **control-plane** operation, separate from the data-plane sync WebSocket: the app owner's CI pushes `{app_id, version, schema, migrations}` to a dedicated **HTTP admin endpoint** (served with axum over hyper — an untrusted network boundary, so its HTTP/1.1 parsing is a mature library's rather than hand-rolled; the server crate already carries tokio, unlike the dep-minimal, wasm-embeddable core). It is the **app-admin** surface (§Authorization) — gated by the `register_schema` action on the `App(app_id)` resource, authenticated with a registration credential (a `StaticTokens`-style admin key that maps to an admin `Identity`), the same authorization seam every data-plane check uses. The registry is keyed per `app_id`; the handshake resolves a client's `{app_id, version}` against it.

The **hash-lock** pins the schema + migration chain by SHA-256 (matching the content-addressable blob store), so the server refuses to boot on a gap / out-of-sequence / hash mismatch (§Schema Migration gate 3). The crypto lives in the **server** crate, not core — core stays dependency-minimal (`#![forbid(unsafe_code)]`, `uuid`-only, wasm-embeddable) and a client never hash-verifies (it already trusts the server it connects to); only the server, which is not embedded, takes the `sha2` dependency.

## Enforcement Points

Producer SDK rejects an op that violates the schema before sending (invalid ops never enter the log). An enforcing server validates inbound (defense in depth). The apply boundary at every schema-bearing replica validates merged state (triggers Invariant Repair on violation).

## What Predefined vs Not

Core predefines: the validation engine, mark merge-kinds, attr type primitives, repair rules. App declares: type names, mark names, attr keys, allowed children, defaults, exclusivity, anchor expansion per mark, default block type for repair, awareness entry shapes / TTL / throttle, schema-level `@auth` grants.

## Schema File

JSON. Top-level keys: `schema` (name), `version`, `root` (top-level Map slot → type), `types` (named definitions, each a `kind` = one of the eight primitives with its constraints), `marks` (name → merge flavor + anchor expansion + value shape), `awareness` (entry kind → TTL + throttle + value shape), `auth` (`roles` — the static role vocabulary — plus `grants` — role / subject → action → path, with `${actor_id}` / `${author_id}` templating), `zones` (name → subtree root path — coarse auth partitions, §Zones), `autoVersion` (declarative version triggers — event / schedule + name template + retention, §Auto-Version Triggers). `auth` holds **only the static role-based defaults**; per-instance ownership and per-actor grants are **dynamic doc-level ACL state**, never declared in the schema (§Authorization). Every schema dimension maps to exactly one repair rule with a declaration home, so parse-time validation guarantees no schema admits an unrepairable runtime state:

| Repair rule | Declared by |
|-------------|-------------|
| Orphan inline → wrap in default block | `repair.orphanInline` on an xml type |
| Disallowed child → drop | `children` allowlist |
| Exclusive collision → keep lamport-oldest | `children.<T>.max` |
| Out-of-range scalar → clamp | `min` / `max` on scalar / counter / attr |
| Disallowed / mistyped attr → drop | `attrs` allowlist + `type` |
| Mark on disallowed type → drop | type `marks` allowlist |

## Versioning

Every schema declares a version; every Document records the `schema_version` it was created under. Versioning is mandatory once a schema is declared. Cross-version coexistence is handled by Schema Migration (below), not by version equality — a client declares the *range* of versions it speaks, and the server translates in flight per recipient.

## Lifecycle Hooks

Schema-driven events the engine detects and surfaces as SDK callbacks — the engine observes, the app decides UX (never an override, never a hard crash): `onRepaired` (invariant repair ran on a merge — offer undo / "we resolved a concurrent edit"), `onOpsRejected` (server rejected the client's ops — auth revoked while offline, or schema-invalid — app shows / discards / exports them), `onUpdateRequired` (the client's version range cannot bridge the document's version across a breaking gap — app prompts an update / falls back to read-only).

---

# Invariant Repair

Concurrent merges can produce schema-invalid states even when each individual op is valid (e.g., schema says "at most one heading," Alice and Bob each insert one concurrently).

## Opinionated, Not Configurable

Core ships fixed repair rules. Apps don't pick. Configurable repair = configurable footguns + cross-language divergence + decision fatigue. Each rule is a deterministic function of (current state, schema, lamport order). All replicas independently converge to the same repaired state.

## Rule Shape

Orphan inline → wrap in declared default block. Disallowed child → drop. Exclusive collision → keep lamport-oldest, demote rest. Out-of-range scalar → clamp. Disallowed / mistyped attr → drop. Mark on disallowed type → drop. Sequence over `max` → drop the lamport-newest excess. Tree-move cycle and Map slot type mismatch handled by their respective algorithms, not repair.

## Mechanism: Read-Time Normalization

Repair is a **deterministic read-time normalization of the merged state — never a minted op**. The stored/encoded state is the raw merged op-set; every materialized read applies the repair function to produce the canonical view. This is convergent *by construction*: repair is a pure function of the merged op-set, and the lamport order it needs (keep-oldest, drop-newest) comes from the **stamps already in the state** (Map-slot / Register / sequence-node stamps), never the local replica clock — so two replicas with the same ops produce byte-identical `encode_state` and identical repaired reads.

- **No op, no stamp.** A clamp returns the value clamped on read; the stored value/stamp is untouched. A disallowed value or over-`max` excess is hidden on read. Nothing is written, so there is no repair-op stamp to diverge — the reason repair is normalization, not a new op.
- **Element-creating repairs use derived ids.** The one repair that introduces structure — orphan inline → wrap in a declared default block — mints no op either: the wrapper's `element_id` is *derived* from the violating position (as Map slots derive theirs from `(parent, key, kind)`), so every replica synthesizes the same wrapper and a later op can target it. (Requires XmlElement / default-block; ships with those.)
- **`onRepaired` fires at the apply boundary** — the validator (a deterministic function of state) detects the violation there and emits the observation event; the repaired *value* is produced at read. Apply detects and emits; read normalizes.

Apply-time *materialization* (rewriting stored state to its repaired form) is rejected: a clamp that overwrote the stored value would need a new stamp and reintroduce the divergence problem. Read-time normalization sidesteps it entirely.

## Observation, Not Override

Apps cannot change what repair does. Apps can observe that it happened via a `repaired` event. UX uses: "we resolved a concurrent edit," offer undo, log, audit.

## Closure of Violation Set

Schema language has finite dimensions: type membership, children cardinality, attr presence / type / range, mark allowance, mark value shape. Every violation maps to one dimension. Every dimension has a rule. Schema declarations validated at parse time so apps cannot write a schema that admits unrepairable runtime states.

**The closure invariant is why a sequence has no `min`.** An *upper* bound is repairable (drop the lamport-newest excess); a *minimum count* is not — concurrent deletes can underflow it and repair cannot invent items. Admitting a sequence `min` would let a schema describe a runtime state with no repair, breaking closure — so a `min` on a list (or text) is **rejected at schema parse time**. Minimum cardinality is a *semantic* constraint (structure = core, semantics = app, below), and apps express it without it:

- **Structural floor** — model the required minimum as fixed Map slots (a slot cannot be concurrently deleted out of existence), with a List only for the variable part above it: two `optionA` / `optionB` slots + a `moreOptions` list guarantees "≥ 2 options" by construction, convergent under any concurrency.
- **Gate at a transition** — enforce the minimum where best-effort actually holds (one actor, one moment): refuse to flip `published = true` unless the count is met. A draft may sit under the minimum; it just can't ship.

Reactive UI (grey out the last delete) covers the everyday case on top of either.

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

The **server is the compatibility layer; a client speaks a single version.** A typed client is generated for one version (its build version); a dynamic client adopts whatever the server serves. On handshake the client declares the version it wants ops delivered at — normally a single point (a "range of one"); a multi-version-codegen client, rare, may accept a small range. The server never makes a client understand more than its one version — it translates every op to that client's version.

Translation rides the existing per-recipient fan-out seam (the same one that redacts, §Wire-Level Redaction). Mechanism:

- The op log is **heterogeneous and immutable** — each op is stored tagged with its creation `schema_version` and never rewritten (audit / time-travel intact). Translation is a fan-out-time transform, not a log mutation.
- Each migration edge carries a **bidirectional op-rewrite** — the built-in step kinds each define how to rewrite one op forward (up) and, for a back-compatible edge, inverse (down). A **breaking** edge has no inverse; that is what makes it breaking.
- On fan-out, for each (op, recipient) the server composes the edge-rewrites along the chain from the op's creation version to the recipient's version and sends the rewritten op. Cheap structural surgery, no state materialization — with one bound: the rewrite is **key-local**, so it faithfully bridges scalar-field edges but cannot elide a *container* subtree. An op inside a container (a list/text insert, a nested set) targets the container's element id and carries no field key, so a key-local rewrite never matches it; dropping the container's create while its descendants survive would strand them, and rewriting the create's key would repoint it away from descendants that derive their element id from the original key. So a container-create (`MapCreate`/`ListCreate`/`TextCreate`) whose field a recipient's version does not model is carried **verbatim**, subtree intact — it surfaces as an unknown slot the recipient's invariant repair elides, never a strand. Faithful subtree elision (dropping the whole container for a version that lacks it) needs per-recipient element-set awareness — the state materialization this seam avoids — and is a later refinement.
- The **handshake range-check is the guard**: a recipient that cannot be reached from the document's version across a back-compatible path (i.e. a breaking gap with no inverse) is **refused at handshake with `onUpdateRequired`**, before it is ever a subscriber — so a down-translation at fan-out only ever traverses invertible edges. Forward-only is the sole breakpoint; a back-compatible gap never rejects.
- **Ingest** validates an inbound op against its *own* creation version and stores it at that version — no inbound translation.
- **Cold start** is the same migrations at coarser granularity: a peer joining below the compaction floor gets a snapshot of state materialized and migrated to that peer's version, then encoded.

## Compatibility Classes

Each migration edge is classified — by the CI drift / verification gates — as **back-compatible** (bidirectional: a down-migration exists — add type / add optional field / add mark / widen range, where down = drop the addition) or **breaking** (forward-only: the down-migration is lossy or impossible — remove a required field / narrow a type / bare rename). Back-compatible edges let mixed-version fleets coexist on one document; breaking edges strand any client that cannot reach the new version.

## Rolling Upgrades (Expand / Contract)

A zero-downtime schema change decomposes a breaking change into a back-compatible **expand**, a data **migrate**, and a **contract**, so the connected fleet is never split across a forward-only edge:

1. **Expand** — introduce version N+1 as a back-compatible superset; deploy clients that speak `{N, N+1}`. Mixed N / N+1 clients coexist (the server translates both directions).
2. **Migrate** — flip writes to the new construct, backfill; old-only clients stay served by down-migration to N.
3. **Contract** — deploy clients that speak `{N+1}` only; a later edge may now drop N, since no live client speaks it.

This discipline is **opt-in**, giving three ceremony tiers the app chooses per change, all on the same machinery — the only difference is whether an edge is made back-compatible:

- **No schema** — no migration concept; documents just converge.
- **Lazy-breaking** — make breaking edges freely; stranded clients receive `onUpdateRequired` and the app prompts an update. Minimal ceremony, a brief forced-update window.
- **Zero-downtime** — the expand / migrate / contract dance with version ranges; no user ever hits a wall.

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

Tombstones are compressed, never removed (see §Tombstone GC above). Compression happens at **state-encoding time** — every artifact that carries the merged state (a snapshot, a below-floor cold-start catch-up, the durable `.snap` file) run-length-encodes contiguous deleted runs into range records and drops dead values. This bounds exactly the things that compound as a room ages: snapshot size, wire catch-up, disk, replay cost. It is a pure codec optimization — no watermark, no acknowledgement, no removal — so no client can be forced to re-sync by it. The compression is at encoding only; an in-memory range representation (bounding live RAM, not just encoded bytes) is a deferred follow-on (see *Implementation Status & Divergences*).

## Cold Start

When a client connects to a room it has not seen, catch-up returns **either** the ops since its last-seen sequence (at/above the room's compaction floor) **or**, if it fell below the floor, a whole-replica snapshot regenerated live — never snapshot-plus-tail. No full-history replay on the client. *Revisit:* regenerating a whole-replica snapshot per below-floor cold-start is O(state) CPU; cache it per floor if snapshots grow large or cold-starts get frequent (see *Implementation Status & Divergences*).

## Export / Import

Snapshots are portable. CLI ships export / import. Use cases: backup, cloning rooms (templates), cross-server moves, debug repro. The identity-preserving move (backup / cross-server / debug — the origin ceases, the target takes over its id) landed in #107.

**Cloning under a new room id** (a live template — origin and clone both live) is a thin layer over the same primitives: `clone_room(src, dst)` = `export_room(src)` then `import_room(dst, …)` under a fresh room id. It is safe **by room-scoping**, without the id-rewrite / namespacing once feared: server sequences renumber per-room on import; `OpId (client_id, client_seq)` never collides because `client_seq` is monotonic *per-client-global* (a client editing a clone of its own past work still mints fresh seqs); a client subscribed to both origin and clone holds *separate per-room replicas*, so a shared `element_id` names distinct objects in distinct documents; and the clock-bump past the imported lamport rides the existing snapshot-adoption high-water (#126). An explicit id-namespacing scheme (prefix element / client ids) would be needed **only** if cross-room id references or cross-room merge ever existed — they don't (element-refs are same-room, rooms are isolated sync units) — so it is deferred until such a feature appears.

---

# Versioning and Branches

Snapshots are the storage primitive. Versioning is the user-facing layer on top. Apps that need named versions, restore, publish/draft workflows, per-user forks, or diff between revisions should not have to reinvent these.

## Named Versions

Snapshot + entry in a versions index. List, paginate, rename, delete are first-class.

## Auto-Version Triggers

Versions can be created declaratively in response to engine events (`before-publish`, `after-restore`, `before-migration`, ...) or schedules.

**Built on a general engine event bus.** The engine emits typed `EngineEvent`s at lifecycle points and dispatches them to pluggable `EventSink`s — the same pattern as the audit `AccessLog` sink (§Audit), generalized. Auto-versioning is the first built-in sink; the same bus is the substrate for external integrations (webhooks) and can subsume the audit sink later. One event system, many sinks.

**Triggers are schema-declared** (an `autoVersion` block — app-level declarative policy that ships with app code and is version-controlled, like `@auth` / `zones`). Each trigger is an event or a schedule, a name template, and an optional retention count:

```json
"autoVersion": [
  { "on": "before-publish", "name": "auto/publish/${timestamp}", "keep": 20 },
  { "every": "1h",          "name": "auto/hourly/${timestamp}",  "keep": 24 }
]
```

- **`on: <event>`** fires the version create when that `EngineEvent` is emitted; **`every: <duration>`** is a schedule, driven by the `Clock` seam + periodic sweep already used for the awareness grace window. `name` is a template (`${timestamp}`, `${event}`, ...); `keep: N` prunes the oldest auto-versions of that trigger (the retention-window mechanism).
- **Event vocabulary is staged.** The available events fire now — version created / deleted, connect, subscribe, snapshot / compaction; the branch / migration events (`before-publish`, `after-restore`, `before-migration`) are declarable but **fire once those operations exist** (gated on the branch / migration layers). A trigger on an unavailable event parses and waits, never errors.

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

*As built (v0.2):* the server multiplexes many rooms over one connection — each Subscribe opens a client-assigned `Channel`, ops/snapshots/unsubscribes name their channel, and fan-out tags each peer on the channel it opened for the room. The SDK-side `ClientSession` holds N rooms too, each with its own replica and last-seen sequence, routing inbound frames by channel and resuming per channel. Channels still key on `room`; widening to `(room, branch, zone)` waits on the branch/zone layers.

Five docs in five tabs = five connections (per-tab `client_id`). Five docs in one tab = one connection with five channels.

## Handshake

Three phases. *As built (v0.2):* all three — Hello → Auth → Subscribe. The server derives the actor from a verified credential through a pluggable `Verifier` (dev-mode `AllowAll` default; real JWT/OIDC/API-key verifiers plug in via `serve_with_verifier`), `AuthOk` carries the server-derived actor, and `AuthFailed` closes a rejected credential. `Hello` still carries a peer-asserted `client_id` — an addressing handle, not an identity claim; identity is the server-derived actor. Wire structure fixed; credential carrier deployment-pluggable.

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

## Op Acknowledgement

Acknowledgement frames carry a per-channel commit frontier so each side learns the other's progress — the sender is never echoed its own ops, so the op stream alone can't tell it.

- **`Accepted { channel, through }`** (server → client). After the server durably logs an authored batch from client C, it replies with the highest **per-client op sequence (`OpId.seq`) of C's own ops** it has committed. Keyed by the author's op seq, not the server sequence: the op identity `(client_id, seq)` is what dedup already keys on and is stable across reconnect, so a resent op re-acks to the same `through`. Server-sequence correlation would shift when ops are resent and break the outbox match.
- **`Ack { channel, seq }`** (client → server). "I have applied `channel`'s log through server sequence `seq`." **Reserved, no consumer.** It was intended to feed a `min(last-acked seq)` tombstone-GC watermark; that whole approach was dropped in favour of tombstone *compression* (§Tombstone GC), which needs no distributed progress tracking. The frame stays defined on the wire (accepted-and-ignored by the server) as a forward-compat reservation; nothing produces it today.

### Offline op queue

`ClientSession` retains its authored ops per channel in an outbox. `edit` appends; an inbound `Accepted { through }` prunes every outbox op with `id.seq <= through`; a reconnect re-emits the unpruned tail — ops authored while disconnected, or in flight when the connection dropped. `Accepted` is the only signal that a local write reached durable storage, so without it the outbox could never drain. Ops the server rejects (permission revoked while offline, §Offline Edits + Permission Revocation) come back as Error, not Accepted, and stay in the outbox for the app to resolve.

---

# Idempotency

Every operation must be idempotent. Necessary because of reconnects, retries, failovers, duplicate packets. `op_id = (client_id, client_seq)` — server ignores already-seen ops.

---

# Offline-First

Local optimistic editing, offline op queues, reconnect sync, local snapshots. Enabled by embedding the CRDT core locally. The offline op queue is the `ClientSession` outbox drained by `Accepted` acks (§Op Acknowledgement); a reconnect re-emits the unpruned tail.

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

User, role, group — all first-class peers, composable. `authenticated:*`, `anonymous:*`, `*` (anyone) supported. **Claims model:** the verifier maps a credential to an `Identity { actor, groups, roles }` — the engine reads membership *from the token* (the app's identity provider issues it) and never decides membership itself. A grant's subject matches against that identity: an actor id against `identity.actor`, a group against `identity.groups`, a role name against the identity's *effective* roles, or a subject class.

**Role membership has two sources — one global, one per-doc:**

- **Token roles are deliberately global.** A role claimed in the token holds *everywhere* in the `app_id`. Reserve token roles for genuinely app-wide authority (e.g. `admin`); a bare token `editor` means editor of *every* document.
- **Per-doc roles are assigned in the doc-level ACL.** An owner grants a role to an actor **or a group**, scoped to a path — "Alice is `editor` of doc X," "group `designers` is `editor` of `X/content`." This is the normal way to scope a role to a document (the Notion / Google-Docs model), and it never touches the token.

**Groups** are the membership indirection: the *token* carries which groups the actor belongs to; the *doc-level ACL* carries which groups hold which role / capability where. So `alice ∈ designers` (token) + `designers = editor on X` (doc-ACL) makes Alice an editor of X — assign a whole team at once.

An actor's **effective roles** on a resource = token roles (global) ∪ roles assigned to the actor or any of its groups on that resource or an ancestor (per-doc). Schema `@auth` then maps those effective roles to permissions.

## Three Authority Tiers

Distinct mechanisms, not interchangeable — conflating them is a security hole:

- **App admin** — the schema-registry authority (the app owner / CI). Lives *above* every document: registers schemas, migrations, and the static `@auth` for an `app_id`, and is a **superuser** that may act on every document in the app (bypasses the policy, decision-flow step 0). A credential class (the registration key), **not** a role and **not** an owner; never appears in `@auth` grants.
- **Owner** — a **dynamic, recursive, path-scoped capability** held by an actor over a room or a path within it. An owner has full access to its subtree *and* meta-authority (grant / revoke) over it. The document creator auto-owns the root path `/`; multiple owners per path are allowed. Owners live as **doc-level ACL state** (the CRDT tier), self-organized at runtime — never declared in the schema.
- **Role** — a static, schema-declared name (`viewer` / `editor`) whose powers are the schema `@auth` grants. Membership is two-source: a **token** claim (global, for app-wide roles) or a **doc-level ACL** assignment to an actor or group (per-doc — the usual case). The schema defines what a role *can do*; who *has* it is a token claim or a per-doc grant, never the schema.

## Ownership (Dynamic Capability Model)

Ownership is pure runtime doc-level ACL state — the app admin never writes it in stone; owners grow the authority tree themselves. A doc-level ACL tuple is:

```
{ subject:  Actor(id) | Group(name) | Authenticated | Anonymous | Anyone,
  grant:    Capability(read | write | publish_awareness | own) | Role(name),
  effect:   allow | deny,
  path, grantor }
```

An owner assigns a **capability or a role**, to an **actor or a group**, on a path, with an allow or deny effect. `Role(name)` is per-doc role assignment (resolved through the schema `@auth` grants); `Capability` is a direct grant.

- **Delegation with attenuation** — an owner of path P may write a tuple on P **or any subpath of P** (never above or outside): grant a **co-owner** of P, an **owner of a subpath** P/x (who can further delegate downward — recursive), a **role** to an actor / group, or a **leaf** capability. Uniform rule: *an actor may write an ACL tuple on Q iff it owns Q or an ancestor of Q — or is app admin.*
- **`own` is delegable authority; other grants are not** — an `own` grantee becomes an owner and can re-delegate; a plain capability or role grantee gets access only and **cannot** hand out further grants. Only ownership confers granting power.
- **Provenance-based revocation** — a tuple is removable only by its **grantor** (recorded as the tuple's author — un-forgeable, since the op carries `actor_id`) or someone above the grantor in the grant chain, **not** by whoever merely owns an ancestor path. So co-owners granted by a common superior cannot revoke each other (only their shared grantor / admin can), and a superior-imposed constraint on a subordinate's subtree cannot be removed by that subordinate. Revocation authority follows **provenance, not path-ancestry**.
- **Deny: beats static defaults always, provenance-bounded between doc-ACL grants.** Grants and denies inherit downward. A `deny` **always** overrides static policy — a schema `@auth` role-grant or a global token role (so an owner's `deny read alice` on doc X beats Alice's app-wide `viewer` role). Between *doc-level* grants, a deny is **provenance-bounded**: it overrides an allow / ownership only from the deny author's **own subtree** (a superior carving out a subordinate — `deny own` on `a/b/c` strips a subordinate a/b-owner, and provenance-removal makes it stick), and **cannot** override an allow / ownership granted by a **peer or a superior**. This is the same guarantee as revocation — a co-owner can no more *deny* a peer than *revoke* one; only their shared grantor / admin can. Deny is not a backdoor around provenance.
- **Downstream deny** — `read` on `a/b` + `deny read` on `a/b/c` yields "read a/b, not a/b/c"; an ancestor deny is a hard floor over its subtree, no re-opening below it (AWS-style). Capability separation lets a carve-out excise one dimension surgically (`deny own` while leaving `read`).

## Actions

Read, write, publish-awareness per room / branch / path / element / mark; version create / restore / delete; branch create / delete; migration apply; snapshot export; ACL grant / revoke (meta-auth); and `register_schema` (app-admin meta-auth on the `App(app_id)` resource). Room + path level ship first; element / mark / branch widen as those land.

## Resources

By app (registration), room, branch, path (inherits downward), element id (survives moves), mark name, mark instance, version. Path-based inherit; instance-based precise. A resource carries its `author` so `${author_id}` templating resolves at check time.

## Templating

Schema `@auth` supports `${actor_id}` / `${author_id}` / `${room_id}` / `${branch_id}` resolved at check time. Expresses "user can do X to resources they own" cleanly without instance-by-instance tuples.

## Decision Flow

For every check, over the merged view of doc-level ACL tuples and schema `@auth` grants:

0. Identity is **app admin** → ALLOW (superuser, bypasses policy).
1. An explicit **DENY** (doc-level ACL) on the resource or an ancestor → DENY — provenance-bounded: it fires against a static default (schema role-grant / global token role) or against a grant from the deny author's own subtree, but not against a peer's or superior's allow / ownership.
2. Identity **owns** the resource or any ancestor path → ALLOW.
3. An explicit **ALLOW** (doc-level ACL capability grant) on the resource or an ancestor → ALLOW.
4. Schema **`@auth`** grants one of the identity's **effective roles** (token roles ∪ per-doc role assignments for the actor or its groups) on the resource → ALLOW.
5. Otherwise → DENY (default-deny).

Standard IAM semantics: explicit deny wins over static and same-or-lower-provenance policy (below superuser), user-specific not stronger than role for allow, absence of declaration = denial. Permission state is versioned in lamport time, so a concurrent grant / revoke is checked at the op's lamport position (§Hard Problems) and resolves deterministically across replicas. Single source of truth used at every enforcement point.

## Enforcement Points

Connect, op submit, op outbound (per recipient), awareness publish / outbound, version create / restore / delete, branch create / delete, migration apply, snapshot export, ACL grant / revoke. Server is final authority. SDK exposes `canDo` for UI hints — client-side checks advisory only.

## Wire-Level Redaction

If bytes hit the client, assume they leak. Server never sends unauthorized data, ever. Per-recipient filtering on every op send and every cold-start snapshot.

## Zones (Coarse Partition)

For docs with large auth-uniform subtrees, declare zones — separately replicated streams. Per-zone lamport clocks (avoids cross-zone activity leakage). Client subscribes only to zones it's authorized for. Unauthorized zone ops, snapshots, structure, even element counts never sent. Cross-zone tree moves forbidden at schema level. Cross-zone anchors forbidden by default; opt-in opaque references for marks / comments.

Zones are a perf and isolation optimization. For fine-grained per-instance auth, ACL set carries the load. For coarse uniform-auth subtrees, zones are highly efficient. Both work together.

**Zone vs. doc-level ACL — different strengths, deliberately.** ACL redaction (§Wire-Level Redaction) filters *within one replication stream* — an unauthorized client still learns the document *structure* (that a redacted subtree exists, its element counts, activity via the shared lamport). A **zone is a separately replicated stream** with its **own lamport clock** and op-log partition: an unauthorized client receives *nothing* — not the ops, snapshot, structure, existence, or size, and cannot infer activity from clock jumps. Zones are the coarse, subtree-aligned, strong-isolation primitive; ACL is the fine, within-stream one. Per-element dynamic zoning is deliberately *not* a thing — that scatters a zone across the tree (defeating the subtree=stream isolation and duplicating ACL); fine-grained dynamic control is ACL's job.

**Static, path-rooted, schema-declared.** A zone is declared in the schema (`zones` block) as a name → a subtree **root path**; every element under that path is in the zone, by structure. Static (ships with the schema, like `@auth`). This is what makes the isolation cheap — a zone is a contiguous subtree, so it maps to one stream, one lamport, one "don't send this subtree" redaction. Causal independence is *enforced* (cross-zone tree moves and cross-zone anchors forbidden), so the N per-zone lamport clocks never need cross-zone ordering. Zone access reuses the authorization seam (`Resource::Zone`, subscribe-gated); the `Channel` handle widens to `(room, branch, zone)`, each authorized zone a subscribable stream.

**Cross-zone references — opt-in, sealed handle (deferred).** By default a cross-zone anchor is rejected at schema validation. The opt-in (a comment / mention in zone A anchoring into zone B) is a **per-recipient redaction**, not merged state: the authoring client (authorized for both zones) writes a *real* anchor, the server stores it, and only at fan-out to a recipient lacking zone B does the server replace the real `(zone, element_id, position)` with an **opaque token** — an **AEAD-sealed handle** (server key; deterministic sealing so a given ref yields a stable token; associated data binds it to the room so it can't be replayed). The unauthorized client holds the token, round-trips it, renders "anchored in a restricted area," and it resolves only if the client later gains zone B access. Stateless (the token *is* the sealed data — no server mapping table, no GC), reusing the server-crate crypto precedent (schema hash-lock). Deferred to a follow-on; the first zones cut ships with cross-zone anchors simply forbidden.

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
- **Text is codepoint-only; grapheme segmentation is an SDK / editor-adapter concern.** The v0.1 roadmap listed "grapheme helpers"; the built core keeps them out — `Text` indexes by codepoint and ships no grapheme API, so no Unicode-segmentation table is pulled into the core (the same dependency-minimalism that keeps `getrandom` out). An editor adapter, which already handles its editor's idiosyncrasies, maps grapheme positions to codepoint indices. Convergence is codepoint-based and unaffected.

## Planned, not yet built (the prose above reads present-tense — it isn't yet)

- **In-memory tombstone range representation** — the state codec now collapses contiguous deleted runs to range records and drops dead values, so snapshot/wire/disk state no longer grows linearly with deleted items (§Tombstone GC is design-of-record). What remains: the in-memory `List` still holds one node per tombstone, so live RAM (not encoded size) still grows with deletes until a range representation lands. The `Accepted` frame + `ClientSession` outbox (the offline queue) **are** built; the `Ack` frame is a reserved no-consumer wire slot (its GC-watermark purpose was dropped for compression).
- **Element-ref value slot** — `Scalar::ElementRef(ElementId)` (a bare same-room element id — §Internal Data Model) is built as a forward-compat reservation like the blob-ref slot: round-tripped in the codec (`tests/elementref.rs`), no producer / consumer yet. A `kind` hint on the ref is the remaining additive step, deferred until schema validation wants it.
- **Op-batching RLE** — the codec frames one op per record; cross-op run-length encoding is a later additive op kind.
- **Also absent:** client_id generation/persistence in the SDKs (they take a caller-supplied 16-byte id) and codec negotiation. (`RelativePosition`/anchor SDK type shipped (#137); the XmlElement / XmlFragment / RangedElement primitives + their path/SDK surface shipped (XmlElement epic complete). The Error `details` field is reserved on the wire — round-tripped, empty, no producer — see §Error Envelope.)

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
