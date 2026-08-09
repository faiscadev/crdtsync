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

Every operation is immutable and append-only. This describes the **wire/stored envelope**: identity, authorship (`client_id` + `actor_id`), scope (`room` / `branch` / `zone`), versioning (`schema_version`), causality (`lamport`), wall time (informational, not used for causality), kind, target, payload. The **core op** the CRDT engine actually merges is the inner subset — `{id, stamp, target, kind, tx, zone}`; the `zone` is a **compact zone id** (an index into the schema's order-preserving `zones()`, `None` = the root/unzoned partition) that carries which per-zone lamport partition the op belongs to — a core concern, since the clock is partitioned by it. The partition is the zone of the region the op **governs**, not of the position it is emitted at: a container-create's is the created child's, an annotation's is its anchors' (which must agree, cross-zone anchors being forbidden), an ACL tuple's is its scope's — the last two being doc-level state addressed at the root, whose target names no region at all — and an edit inside a mark's composite payload rides the mark, since a payload hangs off the range rather than a map slot; every other op's is its target's own position. That is the same region the snapshot projection keeps such state by, so an op stream and a projected snapshot withhold from the same subscribers wherever that region resolves and its partition holds still. Authorship, the `room`/`branch` scope, schema version, and wall time remain envelope concerns layered around the core op, not core op fields (see *Implementation Status & Divergences*).

Value types in op payloads: scalars, blob refs, element refs. Both ref slots are `Scalar` leaves: `Scalar::BlobRef` (reserved, #60) and `Scalar::ElementRef(ElementId)` — a leaf that names another element in the same room (mentions, links, foreign keys). An element-ref is a plain LWW value like any scalar: no substructure, does not merge; a dangling target (the element was deleted) is an app concern, not a merge concern. It carries a bare `ElementId` (references are same-room — a room is the sync-isolation unit, so no room qualifier is needed); a `kind` hint can be added later if schema validation wants it. Reserved forward-compat like the blob-ref slot — round-tripped in the codec, no producer / consumer yet.

---

# Client ID

Each connecting Document instance carries a `client_id`. Used for op identity (`op_id = (client_id, client_seq)`), per-instance undo, reconnect routing, audit. Distinct from `actor_id` (the authenticated human, from token).

**Server-side, a replica identity belongs to the actor that first writes under it in a room.** The `client_id` a connection declares at Hello is a claim the transport does not authenticate, and a declaration is enough to reach the mint: a stamp names its author and the mint counts on from its author's whole id-space high-water, so an op admitted under another replica's `client_id` moves *that* replica's floor, and one op at the top of the space spends it for good. The server therefore records, per room, which authenticated actor each replica identity belongs to — set once by that identity's first authenticated writer, never displaced, and durable beside the room's creator, which it is the same species of fact as: a stored op carries the per-device `client_id`, never the credential actor, so the pairing is recorded rather than derived. An op authored under an identity another actor holds is refused recoverably — ownership is state the client cannot observe and cannot rotate away from, so it keeps its connection and its ops. Only a batch that actually lands an op claims: an empty one authors no stamp, and one the room deduped away would let a replay of another replica's historic ops take the identity that wrote them. An anonymous actor claims nothing (its id is minted per connection, so a claim under one would refuse that client's own next connection), which leaves an identity whose only writer was anonymous unprotected. And because first-write is a race the owner only wins where it has already written, an identity is claimable in a room its owner has not written to by any actor that room already lets write — the room its ids are actually spendable in is the protected one.

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
| Internal storage | codepoint sequence with per-codepoint stable char_id `(lamport, client, offset)` |
| Public API default unit | grapheme cluster (via SDK Unicode helper) |
| Codepoint-level API | opt-in for advanced use |
| Unicode version mismatch | cosmetic only — codepoints stable, graphemes may render differently |
| Auto-normalization (NFC / NFD / NFKC / NFKD) | none — app responsibility |

## Why Not Other Combinations

Byte identity → multi-byte chars shatter, mid-byte cursor = corruption. Code unit (UTF-16) identity → Yjs's bug, mid-emoji cursor, family/flag emoji break. Grapheme cluster identity → Unicode-version-dependent, mathematically impossible to maintain identity across version mismatch. UTF-16 wire → doubles ASCII bandwidth. UTF-32 wire → quadruples for no win.

Codepoint identity + UTF-8 wire + grapheme-aware API is the only combination that preserves CRDT correctness across all clients and gives users grapheme-level UX.

## Why Codepoint Identity Works Across Unicode Versions

Codepoints are universal (Unicode is append-only). What differs is grapheme cluster boundaries. Mismatched versions = cosmetic rendering differences only. Both clients converge on the same codepoint sequence, both can edit, no data corruption, no CRDT identity break. Right failure mode.

## char_id has a sub-lamport offset so a run never collapses at the ceiling

A char_id is `(lamport, client, offset)`. A run inserted together takes one id per codepoint by counting the lamport up from its base stamp; `offset` is `0` for every op stamp and almost every codepoint. It exists because the lamport is bounded: an op's stamp is wire-derived, so an adversarial or ceiling-clocked op can base a multi-codepoint run at `lamport == u64::MAX`, where a plain `base.lamport + k` has no distinct value to give the codepoints past the ceiling. Rather than saturate the lamport (which collapses every trailing codepoint onto one id — silent data loss, still convergent so undetectable) or reject the run (also data loss), the surplus carries into `offset`, so N codepoints yield N distinct, replica-independent ids. A minted op always bases a run at `offset == 0`, where the carry is exact for any length; the `offset` carry itself saturates only past a base `offset` within a run's length of `u64::MAX` — a corner no minted op reaches, only a crafted decoded stamp — keeping a hostile stamp total (never panics) and convergent. The total order is `(lamport, client, offset)`, so `offset` is only ever a tiebreak among a single client's ceiling-run codepoints; all other comparisons are unchanged.

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

**A retained container is still a target.** An op naming a displaced container — a write into its slots, a create under it, an edit of a sequence it holds — is **applied into it and kept hidden**, never held back waiting for the slot to return. What is retained is exactly what such an op addresses, and the alternative is order-dependent: the same write applies at a replica that saw it before the displacement and waits at one that saw it after, so two replicas that folded the same op set read alike and encode different bytes — and the wait is unbounded, since nothing obliges a slot to come back. Reinstatement then shows the write, which is what a replica that saw the op earlier already shows. A target is a reason to wait only when it is **unmaterialised** — the replica has never seen its create — and that wait is bounded by an op already in flight. (A delete still waits on the nodes it removes, and an atomic member on its group; what displacement stops being is a *third* reason.) The tree fold states this for children sequences under §Tree Moves and the atomic gate restates it under §Opt-In: Atomic; it is one rule, over every op family — the undo seam included, which emits an inverse into a retained container rather than dropping it.

Standalone CRDT construction (a la `new Text()` in Yjs) is intentionally not supported in v0.1: elements must be created at their final location so the deterministic id has a parent. Removes the "type not yet integrated" footgun.

---

# Algorithms and Invariants

## Causality

Total order: per-zone lamport timestamp + client_id tiebreak. Client order: client_seq monotonic per client. Wall clocks not trusted.

## Dependency Model: Lamport + Implicit (No Explicit Deps List)

Ops carry only lamport on wire. Causal dependencies are implicit through payload refs — each op references the char_ids or element_ids it operates on, and those refs ARE the dependencies. Receivers buffer ops whose refs point to unseen ids; apply when refs arrive.

The buffer of held ops is **replica state, not a scratch queue** — it rides the state encoding, and a snapshot hands it to the next replica — so it is kept in **op-id order**: author, then that author's own sequence. An id names one op and every replica holds the ids of the ops it holds, which is what makes it the total order both ends of a snapshot can agree on; arrival order is not, so storing the buffer as it arrived would have two replicas that folded the same ops encode different bytes while reading alike. The order is imposed where the buffer is read as state, not maintained as it fills: what a replica holds is state, the order it happened to fill in is not, and paying to keep it sorted would let a sender choose the cost by choosing a delivery order.

Rejected: explicit per-op dependency lists (Automerge-style hashes), vector clocks (O(n_actors) per op). Lamport-only wins on smaller wire bytes and simpler protocol; CRDT primitives merge correctly regardless of concurrent-vs-causal distinction at engine level.

## Tree Moves (XmlElement)

Kleppmann 2021 ("A highly-available move operation for replicated trees"). Lamport-ordered apply, undo-and-replay on out-of-order receive, bounded undo log. Guarantees: exactly one parent per node, no cycles, no duplication, deterministic convergence.

The fold is a pure function of the move-set + tombstone-set, **independent of the transient displaced/installed state of any parent at apply time.** A child insert, a move, or a child delete into a *materialised-but-displaced* children sequence (its holding container lost its map slot to a concurrent scalar/other create) is applied and retained hidden — never buffered-forever or dropped — exactly as **Displacement retains, it does not forget** requires. Gating these on the parent being *installed* would drop them whenever the parent lost its slot before the op arrived, folding the same op set to different trees by arrival order: an insert lost while a concurrent move relocates the node elsewhere; a delete lost while it must still win over that move. A parent is only truly unready when its container is *unmaterialised* (its create unseen), which still buffers.

**A children-list position is owned by a birth over a move, then by the smaller element id.** A node's position in a children sequence is keyed `(list, stamp)` and a document holds at most one per key, since the snapshot codec refuses a duplicate. Two ops can nevertheless carry one stamp into one list and both be admissible — op dedup is by `OpId`, and the id-space record bounds only an *honest* mint — so the key is contended, and which op takes it is decided by that rank, never by which arrived first. A birth outranks because the key is where the born node's element id comes from: a birth that lost would leave a node whose id names a position it does not hold and which nothing can re-derive, while a move brings an id of its own and survives losing. The birth test is a pure function of the key — a stamp derives exactly two children, the tagged and the tagless — so nothing a reveal registers can invert the rank, and a move naming either of them is ranked as that child rather than as a move. A later-arriving winner evicts the incumbent from the key, from the move edge that came with it, and from the reachability edge the fold derived. A losing *move* is refused whole; a losing *birth* still materialises its child, which is then left with no position. **A node left with no position is left movable**, whether it was refused or evicted, because that is the state the opposite arrival order leaves it in and a move naming it must land the same way either way — and a snapshot carries that for a node created under a parent, which is what a reload re-derives it from. Arrival-order ownership folded one op set into two states, so two replicas that saw the same ops disagreed and their snapshots differed byte for byte. That rank orders the *nodes* two claims name, which is the whole question only while they name two. A move can name exactly the child a birth at the key derives, and two inserts at one stamp derive one child between them whenever their kinds agree: there the key is already the claimant's own, nothing changes hands, and the position is the **meet** of the two anchors under a total order that is arbitrary but the same everywhere. A *contest* is what takes the winner's position outright, the loser's being nothing to it. A meet cannot show which arrived first, where a contest between two claims on one node would have had to know what put the incumbent at the key — and nothing answers that, since the move log dedups on the stamp alone and a move can hold a key having recorded no edge.

**The created-under relation spans the whole tree, the map half included.** A node's position among its siblings is a children-sequence concern, but the relation the move log's cycle check walks is not: a container keyed into a map is created under that map, and the map under the element that owns it. Without those edges the walk stops where the children lists stop, and a move under a node reachable only *through* a map — an element in the moved node's own attrs — reads as acyclic, closes a loop, and leaves the replica holding a document that is no longer a tree. Every rebuild of the relation re-seeds it: a movable node's edge from its birth placement, or from its parent link when it holds no placement at all, and every other container's one hop up that link.

## List

**Fugue** (Weidner & Kleppmann 2023, "The Art of the Fugue"). Tree-based, formally proven no-interleaving on concurrent inserts at the same point. Same algorithm reused for Text.

## LWW

Used by Register values, Map scalar set, XmlElement attr values, mark values of `kind: value`. Resolution: higher lamport wins, tiebreak by client_id.

## Tombstone GC

CRDT text/list deletions leave tombstones (required to position concurrent inserts). **Tombstones are never removed** — removing a node another replica still references would force either orphan-reparenting (a replicated GC op with forwarding, since peers keep tombstones the server drops) or edit-rejection (silent-ish data loss on cold regions). Both are complex or lossy, and removal buys nothing convergence can't get more cheaply. Instead tombstones are **compressed**: a contiguous deleted run — consecutive char/list ids that form one insert's parent-chain, all tombstoned — is a single range record, and a tombstone's dead value (never read again) is dropped. This is the representation both on the wire and **in memory**: the live sequence stores the same range records, so a deleted region costs O(runs) in RAM as well as in bytes. Real editing deletes contiguous spans (words, lines, paragraphs), so runs are few. A range still carries every id — an insert anchored inside one lands there, the walk expanding the run around it — so convergence is untouched (same logical state, fewer records) and it needs no watermark, no client acknowledgement, and no distributed decision. This is what mature CRDTs (Yjs's deleted-item structs, Automerge's columnar RLE) actually ship.

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

CRDT merge needs no schema — the core op `{id, stamp, target, kind, tx, zone}` converges on its own (the `zone` id is resolved by the emitter and carried, so a relay merges without re-deriving it) — so a server hosts an app at one of two tiers. **The tier is decided per `app_id`** (by whether that app registered a schema), not globally and not per document: one binary serves enforcing apps and relay apps side by side.

- **Relay** (app not registered) — stores, dedups, fans out, persists, snapshots; enforces only connection / room-level ACLs. No ingress validation, server-side repair, or in-flight migration. Clients still validate and repair locally against their own schema (deterministic repair converges regardless). This is the zero-config default, and it hosts apps that never registered.
- **Enforcing** (registered schema) — adds producer-ingress validation (defense in depth), authoritative invariant repair, in-flight version translation, and schema-level `@auth`.

## Trust Boundary

A client-supplied schema **body is never trusted for enforcement**. The enforcing server enforces only its **registered** schema; a connecting client asserts a version *number*, used solely as a lookup key into the server's registered set (an unknown version is rejected, not fabricated). The registered schema is admin-provisioned — **registration is a meta-authed surface** (the app owner's CI credential, distinct from any sync connection) and hash-locked, so a client cannot slip a different body under a known version. A client's own schema is **advisory**: it drives the client's optimistic local validation / repair / typing, and the server re-validates every op against the trusted registered schema — client-side is advisory, the server is final authority. (Repair is `f(state, schema, lamport)`, so it converges across replicas *only* when they share the schema; the registered server is the arbiter that corrects a replica which repaired under a divergent schema.)

## Registration

Registration is a **control-plane** operation, separate from the data-plane sync WebSocket: the app owner's CI pushes `{app_id, version, schema, migrations}` to a dedicated **HTTP admin endpoint** (served with axum over hyper — an untrusted network boundary, so its HTTP/1.1 parsing is a mature library's rather than hand-rolled; the server crate already carries tokio, unlike the dep-minimal, wasm-embeddable core). It is the **app-admin** surface (§Authorization) — gated by the `register_schema` action on the `App(app_id)` resource, authenticated with a registration credential (a `StaticTokens`-style admin key that maps to an admin `Identity`), the same authorization seam every data-plane check uses. The registry is keyed per `app_id`; the handshake resolves a client's `{app_id, version}` against it.

The **hash-lock** pins the schema + migration chain by SHA-256 (matching the content-addressable blob store), so the server refuses to boot on a gap / out-of-sequence / hash mismatch (§Schema Migration gate 3). The crypto lives in the **server** crate, not core — core stays dependency-minimal (`#![forbid(unsafe_code)]`, `uuid`-only, wasm-embeddable) and a client never hash-verifies (it already trusts the server it connects to); only the server, which is not embedded, takes the `sha2` dependency.

## Enforcement Points

The **enforcing server's op-ingress is the authoritative reject boundary.** On a client write to a main-branch room it holds an enforcing schema for, the server simulates the batch against the room's current document and validates the result: a batch that *introduces* a **runtime-kind mismatch at a declared slot** — a slot materializing an element of the wrong kind for its schema type, unrepairable at read (a counter cannot be read as the register its slot declares) *and* inadmissible (it stands at a slot the schema declared, unlike an undeclared slot) — is refused at ingress with an `OpsRejected` / `SchemaViolation`, the op never enters the log, and the author keeps its ops (the existing `onOpsRejected` surface). A second, schema-independent reject boundary sits beside it: an op **no replica can hold** — one whose stamp names a client other than its author, whose stamp occupies no position an id may occupy, or which declares a transaction size no group can have — is refused at ingress with an `OpsRejected` / `MalformedOp`, on the same recoverable terms (the frame was well-formed, so the connection stays open and the author keeps its ops). It is refused rather than logged because the judgement is a **pure function of the op**: every replica reaches the same verdict, so the room converges on the op's absence rather than splitting over it — which is precisely what a *repairable* violation cannot claim, and why that one rides into the log instead. The whole batch is refused, not the offending op alone, because the ack frontier is a max over the submitted batch. This boundary is not the session's alone: the `Hub` ingest seams enforce it independently, since a peer's node-to-node replication frame reaches them without crossing the session. **The retained log therefore holds only ops the replica applied or is holding** — never one it refused, which would otherwise be durable, entered in the room's dedup set (swallowing the author's corrected resend under the same op id), fanned out, replayed on every reload, and acknowledged as landed. An op that is merely *waiting* — its target not yet reachable, or its atomic group incomplete — is **not** refused: it is admissible, and is logged, fanned out and acked as it lands, because a later arrival commits it. Within the schema tier, this is the only op-rejection point: a **relay** connection carries no schema and never validates (pass-through); a **repairable** violation (out-of-range, over-`max`, disallowed/mistyped attr, disallowed/excess child, orphan inline) is *not* rejected — it rides into the log and is folded away by read-time Invariant Repair, which is what keeps convergence under concurrency; and an **undeclared map slot** is *not* a violation to reject — a Map is an open container, slot membership is not a schema dimension (§Closure of Violation Set), so an untyped extra slot is admissible. Only a *newly-introduced* mismatch is refused — the gate compares the mismatched *locations* before and after the simulated batch and refuses only when one stands where the pre-apply state had none; a mismatch already standing in committed state (put there by a non-enforcing write) is exempt, so an unrelated edit near it is never wedged, and however that committed mismatch renders is the Map slot's own last-writer-wins concern (§Invariant Repair, "Map slot type mismatch handled by the algorithm, not repair"), not the gate's. Comparing locations rather than a bare count is load-bearing: a count nets to zero — and admits the mismatch — when a batch heals one standing mismatch while planting a fresh one elsewhere; the location key resolves each sequence index to its stable node stamp so an unrelated insert/delete that only renumbers a standing mismatch is not read as new. The producer SDK **may** run the same validator locally as an optional, advisory fail-fast pre-check (reject before send for UX) — it is never the trust boundary, since a malicious or buggy client bypasses any client-side check. The apply boundary at every schema-bearing replica validates merged state and triggers Invariant Repair on the repairable violations.

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

## Typed SDK API (Codegen)

A schema can be *consumed at build time* to generate a typed accessor layer, so a typed client calls `note.get_title()` / `note.meta().set_priority(4)` instead of raw path strings. The generator (`crdtsync-codegen`, an in-repo tool) reads a schema JSON, validates it through the **core `Schema` parser** (the sole validator — codegen never re-implements schema semantics), and emits a source file per target SDK language. The emitted code is a **thin facade over the SDK's existing path surface**: one wrapper class per declared map type (holding a document + the path prefix it lives at), each slot a typed accessor that forwards to the existing path method with the slot's path key + type baked in — register → int get/set, counter → get/inc/dec, text → get/len/insert/delete, list → len/get/insert/delete, nested map → an accessor returning the nested wrapper at the extended path. Because it forwards to primitives core already implements, codegen adds **no runtime behavior** and nothing new to keep convergent across languages — it is a convenience surface, never a second source of truth. Output is **deterministic** (declaration-order emission, no timestamp) so a checked-in generated artifact regenerates as a no-op diff. Codegen only *reads* the schema; it never alters the `Schema` type or the wire format (§Schema Is Code). **Python and Go are shipped targets** (`generate_python`, `generate_go`), each a language target on the same per-language emitter — Go emits exported PascalCase methods with `(value, ok)` getters and a copying `joinPath` helper so nested/sibling accessors never alias a shared path slice. A **TypeScript-for-wasm target is deliberately not generated**: the wasm SDK is already statically typed by wasm-bindgen, and its methods take a pre-encoded opaque path buffer rather than the append-a-key list façade the Python/Go targets forward to, so there is no natural surface to wrap. (A first-class ergonomic TS package now lands separately — §SDK-Ergonomic-Surface — but it is a *hand-written* handle-graph layer, not a schema-codegen target; codegen over its handle surface is a possible later target, not this codegen unit.) Xml typed accessors are a follow-on.

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
- On fan-out, for each (op, recipient) the server composes the edge-rewrites along the chain from the op's creation version to the recipient's version and sends the rewritten op. Cheap structural surgery, no state materialization — with one bound: the rewrite is **key-local**, so it faithfully bridges scalar-field edges but cannot elide a *container* subtree. An op inside a container (a list/text insert, a nested set) targets the container's element id and carries no field key, so a key-local rewrite never matches it; dropping the container's create while its descendants survive would strand them, and rewriting the create's key would repoint it away from descendants that derive their element id from the original key. So a container-create (`MapCreate`/`ListCreate`/`TextCreate`) whose field a recipient's version does not model is carried **verbatim**, subtree intact — it surfaces as an unknown slot the recipient's invariant repair elides, never a strand. Faithful subtree elision (dropping the whole container for a version that lacks it) needs per-recipient element-set awareness. The per-op rewrite itself materializes nothing, but the server does maintain one authoritative document per room and derives a lean **element-context index** (id → path/zone/type) from it, so a fan-out consumer that needs an element's declared type resolves it there rather than re-materializing. A **field step is scoped to its declared type**: the step names a type `ty`, and the fan-out narrows the rewrite to the ops whose owning element (the map holding the slot) is of `ty`, resolved through that id→type projection — so a rename/remove of a field on one type never rewrites a same-named slot on another (the rewrite is *not* correct-only-while-field-names-are-globally-unique). An owning element the projection cannot type falls back to the key-based rewrite; the snapshot seam narrows over the same tree with the same schema, so both converge byte-identically.
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

**Arrival of the whole group is the only commit condition, and convergence outranks the view boundary.** A member still passes the ordinary readiness gate at the moment it applies; one that cannot land yet waits by itself and drains behind the op it needs, rather than holding back the group or applying to nothing. A container that lost its *slot* is not such a case — it is retained, so a member addressed to it lands there hidden (§Map Slot Safety); what a member waits on is a container no replica has materialised for it yet. Gating the *group* on every member resolving makes commit a window that arrival order decides, so replicas fold the same ops to different states — a CRDT-law violation traded for a view nicety.

The view boundary survives because a member that is not ready is almost always one the current state cannot express — its target's create has not arrived, the entry it edits does not exist, the nodes it deletes are absent — so holding it withholds nothing an observer could see. Two residuals are real and accepted rather than papered over: a text delete spanning runs that only partly arrived waits while the characters that *did* arrive stay visible, and a tree move whose destination sequence is not yet materialised waits while the node keeps rendering where it was. Both leave the group's other members visible. Convergence is the law; the view boundary is the nicety, and this is where they are traded.

A group id must outlive the replica object that minted it — a receiver buckets buffered members by author and group id, so an id counter restarted by a snapshot restore merges a stale partial group with a fresh one, commits a mixed set, and strands the remainder. The id is therefore derived from the group's own member sequences, hashed as a set: minted by the counter that already distinguishes the members, carried rather than reset by the restore paths that preserve identity, and needing no separate persistence. The whole set, not one representative sequence — a replica reads its next sequence off the ids it holds, so a sequence it does not hold is free again, and a group built over such a hole would share its lowest member with the group that first used it. Two groups collide only if their members' sequences match one for one, which means their op ids collided first.

**A filter that withholds a member destrands the survivors.** Redaction is per op: a doc-ACL read verdict, a zone scope, or a migration rewrite drops individual members out of a batch. The rest then carry a count their bucket can never reach, so a recipient holds them against a member that will never arrive — invisible to it forever, and still counted among the ids it holds. Every seam that withholds a member therefore delivers the group's survivors untagged, so they merge standalone. Delivering them is the convergence requirement: every op a recipient may receive has to reach it, or it diverges from the correct projection of the sender's state. The atomic view is lost at such a recipient, unavoidably — it cannot see the member that was withheld — and the ops still merge. A group a filter carries whole keeps its tags and stays atomic. One seam cannot follow the rule: a read projection of a snapshot has no per-op verdict to apply to buffered ops, whose paths may not resolve at all, so it drops the buffer entire rather than destranding it — the survivors go with the withheld member.

**A rewritten `count` is a memory-retention instruction, and the answer to it is eviction, not a repair rule.** `count` tells the receiver to hold the group's members until that many arrive, so a size no arrival meets tells it to hold them for the life of the replica — and the buffer rides the state encoding, so the next replica holds them too. Two judgements over a member's own envelope are safe to make locally. A size outside the cap is **refused** where it arrives — at the decode boundary, and at the apply seam an in-process caller reaches without crossing one — because no honest sender mints one, the judgement is on that member alone, and refusing holds nothing. And a group's size is what its members *agree* it is: read off whichever member the buffer happens to hold first, a rewritten count decides when the group commits, so a bucket whose members disagree names no group and is never complete. Unanimity is judged over the members that have *arrived*, so it bounds a rewrite rather than closing it — a unanimous **subset** can reach its own declared count before the dissenting member lands, which is a further shape of the same defect the record below closes for its own three.

Neither of those reaches a rewrite consistent across every member: a group of three retagged to declare two is, to a receiver, an honest group of two followed by a stray. It commits at the size it was told, and *which* members that is belongs to the arrival order, so the third is left holding a size its bucket has already met and no arrival can meet again. Two more shapes land in the same place: an unrelated op of the same author carrying a live group's id, and two copies of one op id carrying envelopes that disagree — in group id or in declared size — where the bucket reads whichever the dedup kept. What is common to them is that a bucket key is *consumed* when it resolves, so a late member of a resolved group is indistinguishable from the first member of a fresh one — and the replica folds one op set to two states.

So the judgement that closes them is over the group rather than the member: **record the key**. A `(author, group id)` set marks a bucket resolved, and a member arriving under a resolved key is untagged and merges standalone. A key is spent at each of the four points a bucket resolves: when it **commits**; when the author **mints** the group, since the author applies its own edits as it makes them and buckets nothing, so without this it would hold a stray every receiver merged; when **eviction** gives up on it, or a member arriving after one would wait on a group the replica has already released, and two replicas on one policy would disagree over nothing but which had ticked first; and when a member arrives naming a group other than the one the buffer is holding that same id under, which spends **both** — only one of the two can ever hold the id, and which one is the arrival order's. Each of the three shapes then lands the same op set from every arrival order, which is what the law asks; what it costs is the atomic *view* of a group a rewrite has already made unservable, and only for the members that follow the commit. The record is persisted, carried in the state encoding beside the buffer it rules — a group resolved before a restart is one whose stray still has to land after it — and every entry is charged to a bucket the replica held, committed, evicted or minted, so it is bounded by the ops it holds rather than by what arrives.

Two rules the record deliberately does not take. It does not read a member whose id is merely **applied**: a resend is ordinary traffic on every transport that retries, so a delivery that spent a key would make state a function of how often an op arrived rather than of which ops did — the same law, broken in the other dimension. And it does not release a bucket the moment it *looks* unreachable, because whether it looks that way is a function of which members have landed, so replicas served the same ops in different orders would release different sets. What those two leave is three further shapes. Two are order-dependent before the record existed too; the third the record itself opens, because the record is per-replica *evidence* and a destranding seam destroys the evidence at exactly the recipients it serves. First, a second envelope of one id that the buffer holds **nothing tagged** to contradict — because the other copy already committed out of the buffer under a different group, or because it carries **no** tag at all, which is what a filtering seam's destranding produces. The honest group's own member is then left holding on an id that will never join it. Its members converge on eviction; which keys each replica has spent does not, and on some arrival orders the record adds a *third* reading where the un-recorded replica had two — the conflict rule fires on the orders that buffer the disagreeing copy and not on the rest. Measured against the un-recorded replica over 392 forged pools it is better on 156 and worse on 4, so the record improves this shape without closing it. And a **minority** count rewrite, where the members left unanimous are exactly as many as they now declare: that subset commits and the dissenter lands as a stray, or the dissenter arrives first and the bucket names no group at all. Closing the second means a disagreeing bucket spends its key rather than merely never completing, which is a change to what unanimity *is* and wants its own decision. And third: a recipient served a group **destranded** never buckets it, so it never spends the key, while the author spends it at the mint and a whole-delivery recipient spends it on commit — a later stray under that id then merges at those two and is held at the destranded one, where before the record all three held it alike. Eviction closes it; the destranding seams knowing the keys they cut is the other way, and it has to answer what a projection may reveal about a group that straddles its cut. A projection drops the record whole: a key names an author and a group, never a partition, so a kept one would count the groups a withheld partition resolved — the same inference the causal-frontier scrub closes.

So the residue is what no local judgement separates: a member that never arrives looks exactly like one still in flight. The replica exposes a way to give up rather than a rule — **eviction** untags every group still waiting, and how long to wait first is the caller's policy, the core reading no clock. Eviction untags rather than discards for the reason a filter destrands its survivors: the members are ops the replica holds and no peer will send again, so dropping them diverges, while untagging costs only the atomic view a group that never completes was never going to deliver. A replica that never evicts holds those members and does not converge with one that does — which is what makes eviction a policy every deployment runs, not an optional cleanup. Eviction **spends the bucket key** it gives up on, which is what makes a bare period over a seam with no notion of a bucket's age safe for a caller that keeps ticking: a tick landing between two members of an *honest* group untags it, and the member that follows is then a stray of a key the replica has already spent rather than the first member of a fresh group, so replicas ticking out of phase converge on the state a reader sees rather than diverging on it. What remains is narrower and still real — a replica that never evicts at all does not converge with one that does; a single tick placement can still leave a group's member unread until the next tick, so it is a policy that repeats rather than one tick that carries the guarantee; and the buffer residue of members untagged but not yet ready still differs by tick placement, so snapshot bytes are not preserved even where the reading is.

## Why Atomic Is NOT Default

Atomic-by-default wrecks streaming UX. Typing "hello" pops in all-at-once when the typist pauses. Paragraph moves hidden until "all done." Cursor moves buffered, never feels live. CRDTs exist specifically to avoid coordination. Atomic-by-default reintroduces it for every op. Atomic is the deliberate override for the 5% that need it.

## Scope Constraints

Tx must stay within one branch, one zone, one schema version. Cannot include migration ops. Atomic tx member-op count capped at 1000.

The cap is a protocol constant, not a per-deployment setting. A receiver decides a group complete from the `count` its members declare, so a deployment that raised its own cap would emit groups every peer refuses, and one that lowered it would refuse groups the room holds; a bound both ends compute identically is what makes the field decidable at all. It is enforced at three points: the decode boundary, where a `count` of zero or one past the cap fails the frame carrying it; the apply seam an in-process caller reaches without decoding, where the one op is refused and its group-mates are not; and the mint, where a group past the cap is emitted untagged rather than tagged with a size no receiver will accept — so an oversized transaction is a non-atomic one rather than a frame every peer rejects.

The zone constraint is enforced at that same mint, on the same principle. A commit whose edits fall in more than one zone is emitted as **one atomic transaction per zone** rather than one straddling both, each group's id derived from its own members' sequences. Only a subscriber admitted to every partition a group spans can receive it whole — a zone-scoped subscription withholds the other partitions' members and destrands the survivors (§Opt-In: Atomic) — so no zone-scoped filter can cut *through* a group — it runs between them. A straddling commit gives up atomicity *across* the zones, which §Not Shipped never offered, and keeps every edit plus per-zone atomicity everywhere the constraint does hold.

The cut is by the partition each op is stamped in, which is the region it **governs** rather than the position it is emitted at (§Internal Data Model). So an op whose governing region resolves to no single partition — a mark whose two anchors land in different zones, which is a `CrossZoneAnchor` violation the read repairs away — keeps the root partition and groups there, rather than being assigned to one of the two zones it names. The group boundary follows the envelope's partition exactly; nothing re-derives it.

Two limits on how far that reaches. The cut holds at the emitter, not end-to-end: a relay that re-stamps an op's partition can hand a downstream filter a group spanning two again, and the doc-ACL read filter cuts on paths no group is aligned to at all — which is why destranding stays the floor beneath all of this rather than being replaced by it. And the cap applies per group, so it bounds each partition rather than the commit: a commit's total held members is bounded by the number of partitions it spans times the cap, at most one per declared zone plus the root.

The remaining halves of the constraint line rest on something other than a seam. A channel's replica is bound to one branch, so a commit has no second one to span. A document binds one schema version, but the zone ids a commit is cut on are indices into that schema's declaration order, so the guarantee rests on an unenforced precondition — rebinding a schema is a settle-point operation, and nothing yet refuses one inside an open transaction (C97). Migration needs no prohibition at all: no op kind represents one, so a transaction cannot carry one.

## Interaction with Invariant Repair

For atomic txs, repair runs inside the commit pipeline, not after. Visible effect of a tx is the repaired state. No two-step "tx done + then repair changed it" surprise.

## Interaction with Undo

A transaction is naturally an undo intention. Undo of atomic tx = generate inverse ops for all members, wrap in a new atomic tx per zone partition the inverses fall in (§Scope Constraints), apply atomically. Atomicity preserved through undo / redo, on the same terms the forward commit had it.

## Not Shipped

Strong consensus / 2PC across replicas (defeats coordination-free property). Compare-and-swap / conditional ops (break CRDT mergeability; deferred to v0.7+ if demand). Cross-branch / cross-zone / cross-schema-version txs. Long-running txs (app state, not engine txs).

---

# Undo / Redo

Per-user undo over a core record-seam, with a thin per-SDK handle. Core sees only inverse ops — no server-side undo state, no special wire format.

Each user's undo stack contains intentions (op groups) the user authored. Undo reverts only that user's ops, even when others' ops are interleaved. Per-op identity makes targeting precise.

Global undo (revert anyone's op regardless of author) is **not** supported — produces broken UX in collaborative settings. Apps that want "revert someone else's change" build it as a deliberate edit feature, not undo.

## Undo for the ergonomic handle-graph SDKs (design resolved, human, 2026-07-25)

The ergonomic handle-graph SDKs (§SDK-Ergonomic-Surface) edit through a `mutate` flow that doesn't route through the legacy `UndoManager` (which only records edits made through *its own* methods, covers a subset of ops, and is local-only). The resolved design: **inversion lives in the shared core**, exactly like every other op-semantic (merge, tree-move, marks) — never reimplemented per SDK language (three divergent inverters would violate the thin-SDK principle and risk non-convergence). Concretely:

- **Core record-seam.** The core records every edit it *emits* into a per-document, per-channel undo stack — a remote op is folded in by `apply` and deliberately never recorded, which is what makes a collaborator's edit structurally incapable of landing on a local stack — extending the existing `UndoManager` inversion to **all op kinds** (register/scalar/counter/map/list/text/xml/mark/blob-ref, root or nested) and to the networked (`Client`-channel) path, so undo works over a live connection, not just an offline doc. Inversion produces ordinary forward inverse ops (no server-side undo state, no wire change) that converge on peers like any edit.
- **Origin / scope selection.** Recording is opt-in and tagged: a document records only while an **origin** is set on it, and undo asks the core stack to "undo the newest intention from origin X", skipping any another origin interleaved. The tag is opaque to core, so it is equally the *scope* selector — an undo manager scoped to a subtree edits that subtree under its own origin and undoes only that. A remote op is folded in by `apply` and never emitted, so a collaborator's edit is structurally incapable of landing on a local stack; global undo needs no separate guard. The selection layer is the good idea from the observer approach; the inversion mechanism stays in core.
- **Thin SDK handle.** Each SDK (JS/Python/Go) exposes a small undo handle (`undo()`/`redo()`/`canUndo`/`canRedo`, origin-scoped) over the core stack — no per-language inversion logic. Atomic transactions undo as one intention (existing `atomic_group` semantics).

Rejected: an SDK-layer origin-filtered *inverter* (Yjs-style, but Yjs is single-language) — it would put op-inversion in each SDK, fragmenting convergence across three languages, contradicting the core-owns-semantics principle.

Inverse ops emit into the normal op stream. Ops that overwrite or delete state require prior-state capture at op creation time — the seam reads the inverse off the state an op is about to overwrite, before it lands. A tombstone drops the value it held, so reviving a deleted sequence node re-creates it: a scalar as a fresh insert, a composite XML child rebuilt from a subtree snapshot taken at record time, both anchored on the tombstone so the revival lands where the node was.

An intention is one transact, one explicit begin/end intention group, or one atomic transaction — which undoes and redoes atomically in turn: as one transaction, or as one per zone partition where the intention spans several (§Scope Constraints). Manual begin / end intention covers explicit grouping (paste, paragraph break); auto-grouping on debounced gaps (>500ms idle = boundary by default) is an SDK concern, since core injects its clock rather than reading one.

The stack lives in the **document** — so a channel's replica carries its own, and undo works identically offline and over a live connection — with the SDK holding only the origin tag. Offline editing produces undoable ops without network. The stack drops at a migration boundary and when a channel adopts a server snapshot — in both cases the recorded inverses describe slot shapes the document no longer has — while recording itself continues past either.

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

Tombstones are compressed, never removed (see §Tombstone GC above). Every artifact that carries the merged state (a snapshot, a below-floor cold-start catch-up, the durable `.snap` file) carries contiguous deleted runs as range records with dead values dropped, and the live sequence holds them the same way — so the things that compound as a room ages are all bounded: snapshot size, wire catch-up, disk, replay cost, and resident memory. No watermark, no acknowledgement, no removal — so no client can be forced to re-sync by it.

## Cold Start

When a client connects to a room it has not seen, catch-up returns **either** the ops since its last-seen sequence (at/above the room's compaction floor) **or**, if it fell below the floor, a whole-replica snapshot regenerated live — never snapshot-plus-tail. No full-history replay on the client. *Revisit:* regenerating a whole-replica snapshot per below-floor cold-start is O(state) CPU; cache it per floor if snapshots grow large or cold-starts get frequent (see *Implementation Status & Divergences*).

## Export / Import

Snapshots are portable. CLI ships export / import. Use cases: backup, cloning rooms (templates), cross-server moves, debug repro. The identity-preserving move (backup / cross-server / debug — the origin ceases, the target takes over its id) landed in #107.

**Cloning under a new room id** (a live template — origin and clone both live) is a thin layer over the same primitives: `clone_room(src, dst)` = `export_room(src)` installed under a fresh room id, create-only as an import is. It carries the source's **creator** as the clone's authority root: the doc-ACL tuples ride the snapshot, and a room with no root decides none of them — a creatorless clone would land every deny in the source inert. The cloner therefore holds no authority over the room it minted; a template's author roots every copy of it, which is the only rooting under which the tuples mean in the clone what they meant in the source. Its gate is reading the source **whole** — the `reads_whole_document` seam plus every declared zone being readable — not the room-read tier: both redaction dimensions are keyed by the room the bytes leave, so a partial reader granted the room tier could otherwise copy what it may not read into a room of its own and subscribe there. The gate's inputs (the ACL records, the creator, the governing schema whose zones it reads) are per-node, so a clone is served only where one node leads **both** rooms — a replica holds the source and would export it, at its own replication freshness; elsewhere the clone is the no-op it always was for an absent source. The governing app travels with the clone as the creator does, for the same reason: the clone is the source's content, so the schema that decides how it is read is the source's. What leadership does *not* supply is a binding nobody ever made — a room imported or cloned from an ungoverned one reads as having no zones, indistinguishable from a relay room that genuinely has none (C62). It is safe **by room-scoping**, without the id-rewrite / namespacing once feared: server sequences renumber per-room on import; `OpId (client_id, client_seq)` does not collide across a clone because `client_seq` is monotonic within a live replica and each of a connection's replicas authors under its own `client_id` (§Connection model) — a client subscribed to both origin and clone holds *separate per-room replicas* on separate channels, so their op ids are disjoint and a shared `element_id` names distinct objects in distinct documents; and the clock-bump past the imported lamport rides the existing snapshot-adoption high-water (#126). (Monotonicity holds *within* a replica's life: a replica that keeps its identity but rebuilds its counter from an op delta rather than a snapshot re-mints seqs it already spent — tracked as C6, and orthogonal to cloning.) An explicit id-namespacing scheme (prefix element / client ids) would be needed **only** if cross-room id references or cross-room merge ever existed — they don't (element-refs are same-room, rooms are isolated sync units) — so it is deferred until such a feature appears.

---

# Versioning and Branches

Snapshots are the storage primitive. Versioning is the user-facing layer on top. Apps that need named versions, restore, publish/draft workflows, per-user forks, or diff between revisions should not have to reinvent these.

## Named Versions

Snapshot + entry in a versions index. List, paginate, rename, delete are first-class.

A version *read* is a redacted read. The captured bytes are the room's own state at an earlier sequence, so they carry every partition the room carried; the **state and its causal frontier** are therefore subject to the same per-recipient redactions the live catch-up applies — the doc-ACL path projection, then the zone projection — narrowed to the requesting channel's zone scope, and — wherever a projection actually runs — carrying only the requesting replica's own frontier. Both seams that hand a client a state blob run that one composition, so a redaction added to it cannot reach one and miss the other.

**Current** policy governs a version read; the version's own captured ACL state does not, or a revoked grant would be reachable by fetching a version taken before the revocation. That holds for every tier resolved at request time — deployment, schema, and the doc-ACL tuples — with the caveat that "current" means *current on the node answering*: the doc-ACL tuples ride the room's log, so a node answering that has not replicated a revoke *would* redact by the grants it still holds. A **version** read is answered by the room's leader alone (§Follower Read-Serving) — routed there because a version index is per node, which puts it on the freshest records as a consequence rather than as its purpose. It is no wider an exposure than the live stream that node serves beside it: both redact by the same records at the same instant, and a promoted node that has not caught up answers from the records it has (C113). It does *not* yet hold at all for the zone scope, which a channel resolves once when it subscribes and then carries: a zone verdict revoked afterwards narrows nothing on that bound channel, at a fetch or at the live fan-out. An element-scoped grant resolves against **the tree of the bytes being served** — for a version read, the version's own, where an element's redaction path is where it stood at capture — since a grant that resolves to no path in that tree is inert, and an inert deny over the bytes is the whole state. That is a property of every seam that hands out a state blob, not of versions: a catch-up serving a branch that owns its base — a restore, a publish — hands out a captured tree with the branch's divergent tail folded in, and resolving against the live room there is the same inert deny. A path-scoped grant is unchanged by this: it names a position, and governs whatever occupies that position in the tree it is evaluated against. A *version* state that cannot be decoded, and so cannot be projected, is refused rather than served unnarrowed wherever a redaction is configured over it — the archive is read back off durable storage, unlike a snapshot this build materialized in the same instant.

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

A restored branch **owns** its base — the chosen version's captured state, served with the branch's own divergent tail folded in — so a catch-up on it hands out a tree `main` has moved on from, and the redactions it runs resolve against *that* tree rather than the live room's (§Named Versions). Switching the active HEAD is what makes this the ordinary path rather than a named-branch one: a plain subscribe follows the active branch, so an everyday reader joining after a restore is served the base.

## Publish / Draft

Pattern: edit on `main`, sync a `published` branch's HEAD for read-only consumers. Republishing replaces `published`'s owned base with the editor snapshot and repoints its HEAD; the branch never carries a divergent tail, since a client write to it is refused. Old `published` snapshots remain reachable as versions — apps can roll back published state independently of editor state.

A published branch therefore serves a captured tree, exactly as a restored one does, and its readers are read-only consumers rather than editors — so it is where an inert element-scoped deny is served to the widest audience. Its catch-up resolves redactions against that base, not against the `main` the editors have moved on (§Named Versions).

## Per-User Branches

Same primitive supports per-user forks. Useful when each user customizes a base template (form-builder, dashboard, per-user filters) without affecting the shared base.

## Branch-Scoped Replication

`(room, branch)` is the unit of replication. Replica sets shard by `(room, branch)` if needed. Cross-branch sync via internal engine ops, not normal client ops.

## Schema-Aware Diff

Documents are structured Element trees with declared schema (not opaque blobs). Diff between any two snapshots is computable as structural change lists. Text values produce char-level diffs; XmlElement subtrees produce structural diffs; attrs / marks / Map / Register / Counter show value diffs. Engine ships sensible default renderers; apps can override.

A change list is the room's own content in a different shape — every change carries a path, and a value change carries the scalar at it — so **a diff served to a client is a redacted read**, on the same terms as a version read: the same composition, the same audit action, and an archived side that cannot be decoded refuses the query without closing the connection, and — for the **version** arm, whose sides are the room's own captures — the same leader routing (§Follower Read-Serving). The **branch** arm is not routed with it: what a replica may answer about a branch, and whether one unservable side refuses the whole query, is its own question (C103). Both of a diff's sides go through that composition *before* the engine sees them, so a served change list is by construction the diff of the two states that reader would itself have been served — the causal frontier aside, which the two seams scrub differently and a change list does not carry. A partition the reader may not read therefore contributes no change at all, rather than a change it may not see — as far as the projections themselves reach, which for a *container* the live walk does not reach is not yet far enough (C67). That is why a client's diff query names a **channel** rather than a room, and why its reply names the channel that asked: two channels of one room under different zone scopes are served genuinely different change lists, so only the channel attributes an answer to the query it answers. The general rule the two seams share: a request whose *answer* carries a room's content is channel-keyed on both halves, because the channel is what carries the reader's zone scope and so what tells two answers apart; a request that answers with only *names* may ride a room off the frame. A room-management mutation may ride a room too — but only where its effect stays inside the room's own governance: one that relocates content into a room governed by other verdicts is a content read wearing a mutation's shape, and needs a gate over what it moves rather than over the room it names.

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

**End-state (as built).** A blob is content-addressed + immutable, so authority cannot attach to the content — the id *is* the content hash, shared across every reference. Authorization attaches to the **reference site**: a fetch of `GET /blobs/{id}` is allowed iff the authenticated caller holds **READ** authority on at least one live `core::path` that currently references that id, resolved through the *same* per-recipient read evaluator the op-stream redaction uses (`acl::recipient_reads_path` — deployment policy, doc-ACL tuples, and schema `@auth` grants composed identically). The server derives an `id → referencing paths` index from the live document (`index::blob_ref_paths`, walked exactly like `element_paths`; a map-slot ref is gated at its leaf path, a node-addressed ref inherits its container's subtree path), so the gate tracks moves, deletes, and redactions with no separate state. The out-of-band blob plane owns no replicas; it round-trips the decision to the registry actor over a `RegistryHandle` (`Registry::authorize_blob_fetch`, scanning every room since a handle is room-independent). **Fail-closed** on every ambiguous case: an unreferenced or since-deleted id, or one referenced only under paths the caller cannot read (a redacted / denied position), is denied — even for an authenticated caller, and even for the room creator who owns `/` (an owner cannot fetch a blob nothing references). This is the drag-to-exfil analogue for blobs: a reference the recipient cannot see must not be fetchable, mirroring the element-id ACL / redaction model. Upload (`POST /blobs`) is authentication-only — a producer stores bytes before any reference exists; the gate is on fetch. A cross-zone opaque-token path (AEAD, no reference site available to the recipient's server) is the deferred Zones-4 case, out of scope here.

## Dedup

Same content → same sha256 → stored once. Reference counting across all docs / branches / snapshots. Big savings on user avatars, template assets, brand images, shared PDFs. Transparent to clients.

## Garbage Collection

When all reference sites disappear, blob becomes orphan. Default 30-day grace period (tunable) protects against undo restoring a ref, restore-as-branch re-referencing old blobs, mistaken delete recovery. Conservative — trades storage for safety.

## Wire-Format Reservation

Blob ref slot reserved in op envelope from v0.1, even though full implementation lands v0.5. Cheap now, painful later.

---

# Networking Layer

## Transport

WebSocket. WSS over TLS in production. *As built (client-facing listener; the node-to-node link is §Peer Transport):* TLS is **terminated at the server's own listener** (terminate-at-server, not an assumed external proxy) via **rustls** (pure-Rust, ring provider — no OpenSSL/system dep, hermetic build). It is **config-gated and opt-in**: `CRDTSYNC_TLS_CERT` + `CRDTSYNC_TLS_KEY` name PEM cert-chain + private-key paths that load a `rustls::ServerConfig` at startup, wrapping every accepted socket in a rustls session before the unchanged, transport-agnostic wire protocol runs over it (`wss://`); with neither set the listener binds plaintext exactly as before (dev stays frictionless). A configured-but-broken cert/key (missing, empty, mismatched) is a **loud startup error — never a silent downgrade to plaintext**, which would turn a deployment that asked for encryption into an unencrypted one. The TLS handshake is bounded (a pre-auth blocking point) so a stalled client cannot pin a task + FD.

**mTLS (client-cert authentication) — as built.** Setting `CRDTSYNC_TLS_CLIENT_CA` to a PEM trust-anchor bundle turns on mutual TLS: the `ServerConfig`'s `with_no_client_auth()` slot is replaced with a `rustls::server::WebPkiClientVerifier` built against those roots (`server_config_from_pem_with_client_ca_mode`). mTLS is opt-in and non-regressing: with no client-CA configured the listener stays server-auth-only exactly as before, and a client CA set without a server cert/key is a clean startup error (mTLS needs TLS). An empty client-CA bundle is a loud error, never a silent fall back to server-auth-only.

`CRDTSYNC_TLS_CLIENT_AUTH` selects how strict the client-cert requirement is (`ClientAuthMode`), **defaulting to `require`** when a client-CA is set — the secure default is unchanged, `request` is strictly opt-in; an unrecognized value is a clean startup error, never silently the permissive mode:

- **`require` (default).** **Fail-closed by construction** — a client presenting no certificate, or one that does not chain to a configured root, is rejected by rustls **at the handshake**, before the connection ever reaches the wire protocol (the verifier *requires* a client cert).
- **`request` (opportunistic mTLS, authenticate-if-presented-don't-require).** The verifier is built with `allow_unauthenticated()`, so it **still validates a presented cert** against the roots — an untrusted/invalid *presented* cert is **still rejected at the handshake** — but a client presenting **no** cert is admitted and falls through to the ordinary certless session path (in-band credential / anonymous rules, `Cmd::Connect.cert_actor = None`). The **only** relaxation is admitting cert *absence*; a bad presented cert is never treated as anonymous. This reuses the existing certless path — no new session-actor concept.

**Cert identity = the ACL authenticated actor** (§ACL identity=authenticated-actor): a verified connection's leaf cert is parsed (`x509-parser`) for its **SAN** (first DNS/email/URI entry), **falling back to CN**, and that name is bound as the connection's actor through the *same* `connect_authenticated`/`Identity` plumbing an in-band credential establishes — a cert-authed connection is not a parallel identity concept, just another way to establish the authenticated actor, so the ACL evaluator sees the cert actor at every read/write point. It takes precedence over any in-band credential on the same connection. A verified cert carrying no usable SAN/CN is **rejected, not admitted anonymously** — an identity-less cert cannot slip past authentication.

## Connection / Multiplexing

**One WebSocket per `(server, actor session)`. Logical channels multiplexed per `(room, branch, zone)` subscription.** Subscribe / unsubscribe via in-band control messages, runtime-mutable.

*As built (v0.2):* the server multiplexes many rooms over one connection — each Subscribe opens a client-assigned `Channel`, ops/snapshots/unsubscribes name their channel, and fan-out tags each peer on the channel it opened for the room. The SDK-side `ClientSession` holds N rooms too, each with its own replica and last-seen sequence, routing inbound frames by channel and resuming per channel. Channels still key on `room`; widening to `(room, branch, zone)` waits on the branch/zone layers.

**A channel's replica is its own author.** Two channels of one connection can name the same room — a whole-room subscription beside a zone-scoped one, two zones, a branch beside the default — and each holds an independent replica minting op seqs and lamports from its own zero. So channel `c` authors under `client_id.for_channel(c)`: the declared id itself on channel 0, a UUIDv5 over `(declared id, c)` beyond it. Channel numbers are assigned in subscribe order and never recycled — reusing a freed one would hand a fresh replica, minting from seq 0, the identity of ops the retired one still has in flight. Non-recycling makes the numbers a finite resource, so subscribe is fallible: a session that has spent its `u32` range refuses further subscriptions, keeping every room it already holds, rather than wrapping onto a number a live channel is bound to. The derivation is pure, so the session driver re-derives it from the Hello id and the channel an op batch names, and refuses a batch carrying any other identity. Without it both replicas mint identical `OpId`s and identical stamps for unrelated edits: the op id is the dedup key, so one channel's write is dropped as an already-applied duplicate, and repairing only the seq moves the loss down to the stamp, which is a sequence node's identity.

**So the fan-out's unit is the channel, and awareness's is the connection.** An op fan-out omits only the *authoring* channel — the one replica that already folded the batch — and delivers to every other channel of the writing connection exactly as it delivers to a peer's, because a sibling channel is a distinct author whose replica converges, and whose seen sequence advances, only on what actually reaches it. Awareness is the opposite and stays connection-scoped: a connection holds one presence entry per room, keyed by its Hello id and its authenticated actor, both of which its channels share — a sibling's set replaces that entry rather than coexisting with it, and an update carries only the actor, so an echo would hand a client its own presence back as a peer's with nothing on the wire to tell the two apart.

One `client_id` still names one replica. A session splits the id it is *given* across the replicas it holds internally; handing the same id to two separately-constructed replicas — two sessions, or a session beside a bare document — collides under any derivation and is the embedder's contract to keep.

Five docs in five tabs = five connections (per-tab `client_id`). Five docs in one tab = one connection with five channels: the tab's own id on the first, four derived from it.

## Handshake

Three phases. *As built (v0.2):* all three — Hello → Auth → Subscribe. The server derives the actor from a verified credential through a pluggable `Verifier` (dev-mode `AllowAll` default; real JWT/OIDC/API-key verifiers plug in via `serve_with_verifier`), `AuthOk` carries the server-derived actor, and `AuthFailed` closes a rejected credential. `Hello` still carries a peer-asserted `client_id` — an addressing handle, not an identity claim; identity is the server-derived actor. Wire structure fixed; credential carrier deployment-pluggable.

1. **Hello** — version + codec negotiation. Format-stable header in the first 8 bytes (4-byte magic + 4-byte protocol version) so new codecs ship in later releases without breaking older clients. *As built:* the client advertises the codec versions it speaks as a trailing optional field of `Hello`; the server selects the highest both ends hold, records it on the session, and answers with a `CodecSelected` — but only when the selection moves off the base codec, because **silence carries the base codec in both directions**. An omitted advertisement and an absent selection both settle there, so a peer that names no codec exchanges byte-identical frames and no selection frame appears on the wire until a second codec exists. A client sharing no codec is closed with `UnsupportedVersion` rather than served frames it would misdecode. The header and the handshake frames themselves are always the base codec; a selection governs the frames after it, never the frame announcing it.
2. **Auth** — only if credentials weren't present at upgrade. Pluggable carriers: cookie, WS subprotocol, `Authorization` header, in-band, mTLS, API key, query param (supported but logs leak). Credentials opaque bytes interpreted by deployment-configured verifier. Clients never assert `actor_id` — server derives it from verified credential.
3. **Subscribe** — repeatable, per `(room, branch)`.

Fast path: credentials present at upgrade → server validates during accept → auth state established → skip Phase 2. One round trip saved.

Operations before auth established: only Hello / Auth. Anything else = protocol violation, terminate.

Anonymous mode: server emits `actor_id = "anon:<random>"` if deployment policy permits. Treated as any other authenticated actor by authorization.

## Error Envelope

Standardized error response with closed enum code + human message + opaque details. Closed enum keeps wire compact, cross-language error handling uniform. New codes ship through engine releases. *As built:* code + message + an opaque `details` byte string, all three on the wire; `details` is reserved (round-tripped, empty) — no producer populates it yet, so the SDK error surface still exposes only code + message.

## Not Locked

Binary codec choice (CBOR / MessagePack / Cap'n Proto / custom) deferred to implementation, negotiated via Hello. *As built:* one custom deterministic little-endian codec (not CBOR/MessagePack), shared by the wire and the durable log. The negotiation seam is built — `Hello` advertises, the server selects and answers — but only one codec exists to select, so every connection settles on it and no selection frame appears on the wire yet. Compression, framing details, TLS profile, heartbeat interval, op size limits — all infrastructure / runtime config.

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

# SDK Ergonomic Surface

The §SDK-Philosophy and §Multi-Language-Support intent ("edit the same document naturally", "high-level editing intentions, CRDT internals stay hidden") pins a *philosophy* but not a concrete API *shape*. This section pins the shape: the **handle-graph model** every first-class SDK realizes. It is a language-agnostic contract with a per-language reference realization; **JavaScript/TypeScript is the reference** (the first SDK built to this shape), and **Python/Go are lifted to it later** (enumerated in KANBAN, not built by the JS epic).

The layering is unchanged: an SDK is still a thin wrapper over the wasm/C-ABI core (§Portable-CRDT-Core). The core surface (`WasmDocument`/`WasmClient`, the FFI) is **byte-path + bytes-valued** — a path is an opaque length-framed key buffer, every value crosses as a `Scalar` byte payload, every edit returns raw op bytes. That surface is correct but not ergonomic; it stays as the **low-level seam**. The ergonomic layer is a hand-written package *on top of* it — not codegen (§Typed-SDK-API is a separate, schema-driven convenience). No core CRDT logic moves into the SDK; the handle layer only marshals values and hides path/op bytes.

## The Handle-Graph Model (language-agnostic contract)

A `Doc` is a local replica with a single root map. Editing is done through **live typed handles** into that root, never through byte-paths:

- **`Doc.getMap(key)` / `getList(key)` / `getText(key)` / `getXml(key)`** return a live handle — `CrdtMap` / `CrdtList` / `CrdtText` / `CrdtXml` — addressed by an **ergonomic key** (a language-native map key: a JS string, a Python `str`, a Go string), never a byte-path. The handle **owns its path internally** and **re-resolves on every operation** — it holds the logical path (a sequence of keys / a container identity), not a cached pointer into the value graph, so it stays valid as the document mutates and converges. Requesting a container handle installs that container if the slot is empty and it is convergent to do so (idempotent install, the existing `path::*` install-or-get semantics).
- **Nested handles compose.** `doc.getMap("a").getMap("b").getText("body")` walks the graph; each step extends the parent's internal path by one key. A list/text handle is obtained from its parent map by key; a list yields child handles by index where the child is a container.
- A handle is a **view, not a snapshot**: reads reflect the current converged state, writes apply immediately (locally) and — when the `Doc` is bound to a provider — flow to peers.

This is the Yjs `Y.Map`/`Y.Array`/`Y.Text` shape, chosen (user decision, 2026-07-22) because it is the model JS collaboration developers already know and the one editors bind to.

## Native Value Marshaling

A language's native scalars map to CRDT `Scalar`s, so the developer never hand-encodes bytes:

- **JS:** `string` ↔ a UTF-8 `Scalar::Bytes` (the SDK owns the encoding), `number` (integer) ↔ `Scalar::Int`, `boolean` ↔ `Scalar::Bool`, `Uint8Array` ↔ raw `Scalar::Bytes`, `null` ↔ `Scalar::Null`. A non-integer `number` is rejected (the core `Scalar::Int` is 64-bit integer; a float has no lossless scalar — the SDK throws rather than silently truncate). `bigint` covers the full 64-bit `Int` range.
- **The leaf/container boundary is explicit, not inferred by shape — a permanent design principle.** A **leaf** is written with `map.set(key, scalar)` / `list.insert(i, scalar)` and holds one `Scalar`. A **container** is created with the explicit `getMap` / `getList` / `getText` / `getXml` accessor. The SDK does **not** deep-seed a nested container from a plain object/array passed to `set` — `set(k, {a:1})` is a **type error**, never an implicit `getMap(k)` + recursion. This is the load-bearing marshaling rule every SDK must match, and it is **permanent, not a v1 simplification**: **native scalar ⇒ leaf; explicit accessor ⇒ container.** It is what makes marshaling total and unambiguous (a JS value is a leaf or it is nothing — there is no third "is this a subtree?" case to disambiguate) and what keeps the surface statically type-safe.
- **Rejected non-goal: Automerge-style deep-seed.** Implicitly converting a passed plain object/array into a nested `CrdtMap`/`CrdtList` subtree (Automerge's `doc.map = {a: {b: 1}}`) is a **rejected alternative, not a deferred feature** (user preference, 2026-07-22). It trades the explicit/type-safe boundary above for a guess about whether a value is a leaf or a container, which erodes type-safety and makes marshaling non-total. Containers are always created explicitly. This rejection is **cross-SDK** — the Python/Go lift keeps the same explicit boundary; no SDK offers deep-seed.
- A blob is set through a dedicated handle method (`map.setBlob` / `setBlobRef`, wrapping the existing inline/out-of-band split), not through scalar marshaling — a blob is a value type, not a CRDT primitive (§Binary-Blobs).

## Handle Methods (idiomatic per type)

Method *names* are idiomatic per language; the *semantics* are the contract. The reference (JS) surface:

- **`CrdtMap`**: `.set(key, value)`, `.get(key)`, `.delete(key)`, `.has(key)`, `.keys()`, `.entries()`, `.size`, plus the container accessors `.getMap/.getList/.getText/.getXml(key)`. `.get` returns a marshaled scalar for a leaf, or a handle for a container slot.
- **`CrdtList`**: `.insert(index, value)`, `.push(value)`, `.delete(index)`, `.get(index)`, `.length`, and iteration (`[Symbol.iterator]` in JS). Container items yield child handles.
- **`CrdtText`**: `.insert(index, str)`, `.delete(index, count)`, `.toString()`, `.length`, plus **cursors**: `.relativePosition(index, side?)` captures a stable position and `.resolve(pos)` reads it back to a live index (wrapping the core `RelativePosition`). Cursors are the feature editors require to keep selections stable under concurrent edits.
- **`CrdtXml`**: element/fragment construction, typed child insert (element / text-run), child delete, **tree-move** (Kleppmann, identity-preserving), tag/children reads, and **marks** (author / set-value / delete / read `marksAt`) — to full parity with the core XML + marks surface.

Indices are **live indices** over the current sequence (the core semantics), codepoint-based for text.

## Reactivity

The load-bearing feature for JS collaboration. Two observation entry points:

- **`handle.observe(cb)`** — fires when *that handle's* subtree changes, whether from a local edit or an applied remote op.
- **`Doc.on("update", cb)`** — fires on every applied change to the document.

The **event shape** is derived from the diff/changes machinery the core already exposes (`core::diff`, the wasm `diff`/`diffEncoded`/`decodeChanges` seam): a change event carries the structural change list — for each change, its **kind** (add / remove / value / counter / listInsert / listDelete / textInsert / textDelete / mark\*), its **target** (as an ergonomic path — a key/index sequence, not raw path bytes), and the **before/after** values (marshaled scalars, not `{t,v}` byte tags). An event also flags its **origin** (local vs remote) so a binding can avoid echoing its own edits. The SDK computes the diff by snapshotting before/after an applied batch (or consuming the provider's inbound diff), so reactivity needs no new core surface — it re-marshals the existing change objects into ergonomic events.

**Schema binding** — `doc.setSchema(schema)` binds a schema (its JSON, as bytes) to the replica. Two effects, no core change: a named mark reads with its schema-declared **flavor** (boolean / value / object) rather than the default object-flavor range annotation (`marks_at` already resolves flavor from the bound schema); and the **repair signal** turns on — `doc.on("repair", cb)` delivers the locations whose repaired reading changed against the schema after an edit (each a step path of map keys + sequence indices). A repair names a *location* to re-read, not an edit, so its event carries **no origin**. Emitted from every mutation path, drained only when observed.

## Sync Provider

An ergonomic transport binding wraps the low-level `WasmClient` op-flow (which is pure framing — it holds no socket; §Networking-Layer) plus a real WebSocket:

- **`connect(url, room)` → a `Provider`** (or `new Provider(url, room)`), which a `Doc` binds to. The provider owns the socket, drives the handshake (`hello` → optional `auth` → `subscribe`), sends outbound ops framed by `WasmClient`, applies inbound frames back into the `Doc` (firing reactivity), and handles **catch-up / resume / reconnect** (re-`subscribe` from `last_seen_seq`, `resend` the unacked outbox) and the **offline op queue** (§Offline-First) transparently.
- It exposes **connection state** (an observable `connecting`/`connected`/`disconnected`, plus the `onUpdateRequired` / `onOpsRejected` / redirect signals the core surfaces) and **awareness** (ephemeral presence: set the local entry, observe peers').
- It exposes the **operator-tier surface** (versions, branches, ACL, structural diff, room clone) as `Promise`-returning `Provider` methods — a different interaction model from handle-graph editing: a request framed by `WasmClient` is sent, its reply awaited, and read back as a typed object (`Branch`/version names/`Change[]`). Correlation rides a per-connection FIFO — a room's leader answers a socket's requests in order, so a matched reply (a `takeReplies` drain), a request-refusal `Error`, a redirect, or a socket close settles the awaiting request. ACL grant/revoke ride the op path (acked/resent like an edit), taking typed `SubjectKind`/`Capability`/`Effect`.
- **Node + browser.** The WebSocket implementation is detected/injected — the platform `WebSocket` in a browser, an injected `ws` in Node — so one package serves both. The blob upload helper (`uploadBlob`, already in wasm) is wrapped for the out-of-band large-blob path.

Binding a `Doc` to a provider is the only networked entry point; an unbound `Doc` is a pure local replica (the two-doc convergence model, exchanging op bytes directly, still holds and is the base test).

**The direct fold reports what it refused beside what it applied.** Docs that exchange op bytes with no crdtsync server between them — offline, peer-to-peer, or over a byte pipe the app carries itself — reach the fold with nobody upstream to answer `MalformedOp` on the app's behalf (§Enforcement-Points; that boundary is schema-independent, so even a relay-tier connection answers it, which is exactly what a bare byte pipe does not). So the fold (`applyUpdate` and its per-language equivalents) answers two counts: how many ops applied as they arrived, and how many no replica will ever hold — the same pure per-op judgement the server's ingress boundary makes, so the two seams cannot disagree about *which* ops those are. The distinction is the point: an op that did not apply may be a duplicate, or be *waiting* on a create a later update carries, while a refused one is a bug in whoever wrote it and no arrival lifts it. Those call for opposite responses, and a single applied count renders both as silence. The *policy* on the rest of the batch differs from the server's deliberately: a refused op does not hold back the batch's other ops, where the server's ingress refuses the whole frame, because only the server has an ack frontier taken as a max over the batch (§Enforcement-Points).

**A refused *local* edit is reported too, and by a different signal, because it emits nothing at all.** A replica mints from its own id space and the space is finite: the mint refuses rather than re-issue an id that is already live (§Client ID; honest traffic reaches the end after 2^63 edits, a peer authoring under this replica's `client_id` in one op). A refused edit emits no ops and changes no state — and every SDK mutation path returns exactly the ops it produced, so a refusal is the empty batch an *inert* edit returns, an exhausted replica presents as edits that silently do nothing, and the application reports writes that never happened. So a replica answers **whether the edit just made was refused**, read straight after the edit, and each SDK raises its own language's error from the one mutation seam its handle graph funnels through: `MintExhausted` thrown in JS, raised in Python, `ErrMintExhausted` returned in Go (with `Doc.Err()` for the chaining and value-less mutators that have no error return of their own), a `JsError` from the wasm boundary, and a `1`/`0`/`-1` query on the C ABI — never a second meaning on that layer's empty buffer, which already carries both a bad handle and an inert edit. It is a *report on the edit*, distinct from the *capacity* predicate (`can_mint`): a run refused for its length leaves room for the single-id edit that follows, so the two answer differently and conflating them makes each unreadable. A refusal takes the rest of its intention, so inside a transaction it is raised at the edit that could not mint and what the transaction emitted before that still ships. **The reading is cleared by the next intention opening, never by one closing** — that is what makes "read straight after the edit" load-bearing rather than a style note: the caller that ran the transact, the atomic commit or the undo replay is the one holding the answer, an atomic group's own commit does not clear it, and a reading taken after a further edit answers for that edit instead.

## Language-Agnostic Contract vs JS-Specific Choices

Pinned so Python/Go lift to the *same* model, not a JS transliteration:

- **Language-agnostic (the contract every SDK realizes):** the handle-graph model (root map → live typed handles → nested composition, handles own+re-resolve their path); the native-scalar-to-`Scalar` marshaling table and the **explicit leaf-vs-container boundary**; the per-type method semantics (Map/List/Text/Xml + cursors + marks); the reactivity event model (diff-derived change events with kind/target/before-after/origin, plus per-handle and per-doc observation); the provider model (transport-wrapping sync object owning handshake/reconnect/catch-up/awareness, `Doc` binds to it); the direct fold's **applied-beside-refused** report; and a **refused local edit raised as that language's error** from the handle graph's mutation seam.
- **JS-specific (reference realizations of the above, other languages substitute their idiom):** `Symbol.iterator` for list/map iteration; `Promise`-based `connect`; `Uint8Array` as the bytes scalar; `bigint` for the full `Int` range; ESM + `.d.ts` types; npm packaging; browser/Node WebSocket detection. Python would use `__iter__`/`__len__`/`__getitem__`, `async def connect`, `bytes`, context managers; Go would use exported methods, channels for observation, an `io`-shaped transport.

The JS epic ships the reference implementation and this contract; **lifting Python and Go to the handle-graph shape is a follow-on epic.** The **Python lift is complete** — an ergonomic `Doc` (wrapping the existing low-level `Document`) with `get_map`/`get_list`/`get_text` handles, native marshaling over the same JS-exact string/binary discriminator, and Python-idiom methods (`__iter__`/`__len__`/`__contains__`/`__getitem__`, `OverflowError` for an out-of-`i64` int, `TypeError` for a deep-seed) — additive over the same C-ABI/ctypes binding (which gained `set_scalar`/`get_scalar`/`map_keys` for wasm parity; no core change). The **Go lift is in progress** — the same handle-graph over the same cgo/C-ABI binding, in Go idiom: a `Doc` wrapping the low-level `Document`, `GetMap`/`GetList`/`GetText`/`GetXml` → `*CrdtMap`/`*CrdtList`/`*CrdtText`/`*CrdtXml` handles by `string` key (a Go string carries arbitrary bytes, so it is both the utf-8 key and the raw-key carrier), the same string/binary discriminator marshaling (routed through the canonical `encodeScalar` so it can't drift), `(value, ok)` reads, and `error` returns where Python raised. Go's native `int64` needs no overflow guard (unlike Python's arbitrary int / JS's number). The low-level path-based + bytes-valued surface stays for power users in every SDK.

---

# Horizontal Scaling

## Constraint

No Redis / Postgres dependencies. Cluster layer must be internal.

## Room-Based Sharding

Each room maps to a replica set. Consistent hashing on `room_id` for deterministic placement, horizontal scaling, balanced distribution. The set hashed over is the cluster's **adopted** members, not every node it has heard of (§Member Adoption).

## Leader Model

Per room: leader handles writes, followers replicate. Clients can connect to any node; wrong node proxies or redirects to leader.

## Replication Flow

Client → leader → leader persists → leader replicates to followers → followers ACK → leader ACKs client.

## Durability

Recommended: ACK only after majority replication. Avoids losing acknowledged edits.

## Failover

Leader dies → followers elect new leader → clients reconnect → resume from last_seen_seq.

## Peer-Plane Authentication

Node-to-node traffic — `Replicate`, `ReplicateSnapshot`, `Gossip`, `FollowerHeads`, `PingReq` — shares the data-plane listener with clients, and the engine handles it ahead of the client session so a member's link needs no room subscription. That makes the peer plane a **second ingest seam**, reaching the replica directly, past the identity reservation, the doc-ACL write tier, the schema tier, the cross-zone token gate and leadership alike. It is therefore authenticated in its own right.

**The credential is a deployment-wide cluster secret**, presented once per link in a `PeerAuth` frame and required before any node-to-node frame is honored on that connection. Nothing else on those frames identifies a member: every node dials under the same reserved replica id, so the link's `Hello` distinguishes nobody. On a connection that has not presented the secret, all five frames are the **client-plane protocol violation they look like** — answered with an error and closed — so the client data plane is structurally incapable of reaching a peer handler.

**Fail-closed at configuration.** A node with a membership and no secret refuses to start, as does one whose secret is below a floor short enough to brute-force, and as does a single-node deployment given a secret it has no peer plane to use. There is no mode in which a clustered node comes up with an open peer plane. A rejected attempt is answered with nothing at all and the connection dropped, so a guess costs a fresh connection and whether a secret is configured at all is not observable. Where one is, the comparison is constant-time over the content, so a rejection leaks no prefix of it (its *length* is leaked by the length check, which for a random secret is not a secret).

**Peer admission and client authentication are disjoint.** Holding the secret opens the node-to-node plane and confers no client rights — admission never sets a session identity, so a peer link is subject to exactly the handshake a client is; and completing Hello + Auth confers no peer rights.

**Admission is one-directional *in band*, and the transport is what supplies the other direction.** A dialing node proves itself with the secret; the frame exchange proves nothing back. So on a plaintext link a node writes the cluster secret to whatever answers a member's advertise address, before anything establishes that the other end is a member — and an address is not hard to become: a port freed by a restart, a DNS or ARP win, or an address any already-admitted peer can inject into every node's member set through gossip. Whoever answers **harvests the secret**, and with it write access to every room on every node; it also acks whatever it likes, so a leader can believe a write is durably replicated when it is not. On a `wss://` member this is closed: the dial verifies the acceptor's certificate against the cluster's trust anchors *before the first frame is written*, so an impostor answering the address gets a failed handshake rather than the credential (§Peer Transport). What remains open is a member that advertises plaintext, and — since a bearer secret is replayable by any member that holds it — a challenge-response peer credential, which the encrypted link makes worth designing rather than obviates.

**What the secret does not distinguish is members from each other**, so the `PeerAuth` also carries the dialer's **node id** and the link is bound to that member for its lifetime. Every gate downstream of admission decides against the bound identity, never against a node id a frame asserts (§Peer Identity).

The secret is a **bearer** credential, so the peer link carries it over an encrypted transport wherever the member advertises one (§Peer Transport). TLS is not the peer credential and does not replace it — it is optional by configuration, and an optional gate cannot be what closes an ingest seam — but it is what makes a bearer credential safe to *send*. A separate peer listener remains a deployment choice (bind the advertise address on a private interface) rather than an engine mandate: relocating a port authenticates nobody, and the structural half of that benefit — a client plane that cannot carry peer frames — is already had by gating on the secret.

## Peer Identity

**A peer link is bound to a member, and the binding is the member's advertise host.** The `PeerAuth` carries the dialer's node id alongside the secret; where the deployment issues per-node certificates, that claim must agree with the verified mTLS client certificate's subject, and a link whose certificate names another host is admitted to nothing. One rule decides it: **the certificate names the host of the member's advertise address** (its port is no part of it — no certificate carries one). A node certificate conventionally spells its host several ways at once — a DNS name alongside the IP literal — so *any* of them may bind it, the same way a TLS client verifies an acceptor against the name it dialed. Only the SAN kinds that *are* host names count — `dNSName` and `iPAddress`, the same two a TLS client checks — never an e-mail or URI SAN and never the Common Name, which are free text a CA fills in for a subject rather than an address it vouches for reaching: admitting them would let a certificate issued for one node carry another node's host and speak as it. A wildcard binds nothing, deliberately: `*.internal` would bind every member of that domain to one certificate, giving up the per-member identity rather than establishing it.

**The authority is the listener's client-certificate trust bundle**, since that is what verifies the certificate a peer presents — not the trust anchors this node dials *out* with. The two want to be the same bundle, or no broader: a client-CA wide enough to issue certificates for arbitrary hosts is wide enough to mint a member. Nothing else was available to map a node id to a certificate subject: node ids are advertise addresses and subjects are whatever the CA issued, an explicit map would be a second source of truth that must be updated on every join and can skew per node, and the host is exactly what a certificate legitimately names — the same fact the dialer already verifies in the *other* direction when it authenticates an acceptor (§Peer Transport). The stated cost is that members sharing a host share a trust unit and may speak for each other.

**Identity is declared, like transport.** A cluster cannot acquire per-node certificates at one instant, so an uncertified link is admitted at the id it claims — which binds one link to one identity and vouches for none — until `CRDTSYNC_CLUSTER_REQUIRE_PEER_IDENTITY=1` declares the rollout finished and refuses every certificate-less peer. A node that requires it while verifying no client certificate, while presenting no identity of its own, or while holding a member whose advertise address names no host, refuses to start: each would present as a cluster whose every peer link is refused. Two refusals do not wait for the policy. An advertise address that names no host is refused outright, this node's and any configured member's alike — no peer could dial it and no certificate could be bound to it — and a gossip-learned one is classified a *permanent* dial failure rather than redialed forever. That refusal lives in the address parse every consumer shares, so the host is read in one place. And a node that *verifies* client certificates while presenting one naming no host matching the id it dials under refuses to start whether or not identity is required, because that is the half of a *peer's* decision which is decidable locally: the peers apply this node's own rule to this node's own certificate. That condition is a **proxy** — this node's own listener standing in for whether its *peers* request a certificate — exact where the posture is uniform and wrong in both directions during a rolling change, so where the local evidence says no certificate is asked for the node warns rather than refusing, breaking no deployment that may be perfectly inert. Keeping the client-certificate posture uniform is therefore a deployment requirement rather than a recommendation: no local check can replace it, because the fact being predicted is on the other end of the link. A member's advertised *transport* is deliberately not among those preconditions — the scheme describes that member's own listener, while the link carrying its identity into this node is the one it dials, so a plaintext member still identifies itself; refusing it the cluster secret is `CRDTSYNC_CLUSTER_REQUIRE_TLS`'s separate declaration. Where certificates *are* configured they are decisive whether or not identity is required — a verified certificate that contradicts a link's claim, **or that names no host to support it**, is evidence rather than an opinion — so a cluster already running peer mTLS must have each node's certificate name that node's advertise host, and a node whose own certificate does not refuses to start even mid-rollout. Client-certificate verification in `request` mode is enough, so peer identity never becomes a certificate requirement for ordinary application clients.

**Three gates read the bound identity.** *Replication and leadership*: a `Replicate`/`ReplicateSnapshot` is honored only from a member that itself holds the room, since placement decides which members may ever lead it and one outside the replica set can hold no copy — so a member outside a room's replica set can neither push ops into it nor supersede its leader. The rejection drops the link, which is also the repair: two nodes' member sets differ for the propagation window of every join and reap, so a legitimate leader is transiently outside a room's replica set as a follower sees it, and *silently* discarding its frames would leave a permanent gap — the steady path mirrors only fresh commits, so nothing would re-send them. Dropping makes the leader redial, and the redial re-runs the late-joiner catch-up that closes the gap. *Membership growth*: an inbound `Gossip` may introduce only the node that sent it, **at its own id as its dial address**; a tuple naming an already-known member still merges whole, so a `Dead` verdict still travels, but an unknown member is adopted only from the node it names and only pointing at itself — constraining the id alone would leave the same channel open one field over, since the member set records the address a tuple carries and every node dials it. A joiner still converges — it learns the cluster from the seed it *dialed*, whose reply may introduce freely because the set a node dials is its own member set, and then introduces itself to each member directly. What a self-introduction or a reply buys is a place on the *roster*, not in the ring: rooms are placed on adopted members (§Member Adoption). *Durability*: a `FollowerHeads` must name the member its link was admitted as, because a node is the only authority on what it durably holds; one member crediting another's watermark is what let majority-ack release a client `Accepted` for a write no majority held.

**What identity cannot decide is which claim to leadership is legitimate.** Inside a room's replica set the epoch remains the only arbiter: a replica promoted over a leader it believes down must be able to supersede it, and no identity distinguishes that from a peer replica forging the bump. That is the election's question, answered by the HRW+epoch → Raft evolution (§Cluster Leadership), not by a stronger credential.

**Nor does identity alone decide who belongs in a room's replica set.** Placement is HRW over the member set — a pure, publicly computable function — so which rooms a node replicates follows from its node id, and the join path lets an unknown node introduce itself. A member could therefore *mint* an id that placed it into a chosen room's replica set and reach the gates above from inside it. Identity bounds the mint space to the member's own certified host and stops it minting on anyone else's; what closes it is that rooms are placed on *adopted* members only (§Member Adoption), and introducing yourself does not adopt you.

## Member Adoption

**Learning a member and placing rooms on it are two admissions.** The roster is what a node dials, probes and gossips about; the placement ring is built from the **adopted** members alone. A member learned by gossip is *pending* — reachable and converging, but in no room's replica set and no room's quorum — until the cluster adopts it. Pending is not exile, and cannot be: a member nobody dials could never be verified, so a genuine joiner would never join. What it removes is the step from "a node said this id exists" to "rooms live on that id", which is the whole of the mint.

**Adoption is the cluster's decision, never one node's.** Placement must be identical on every node or the ring diverges and the cluster splits, so "verified" cannot be a local predicate over a shared member set — two nodes would place the same room differently forever. So the *evidence* is disseminated instead of the verdict: a node records only what it knows first-hand, and that claim rides the same anti-entropy that carries liveness, as one flag per member on the gossip tuple. A member is placed once **two** already-adopted trust units have verified it. The evidence is a grow-only set of verifiers, so it merges by union.

**The adopted set is derived, never accumulated.** It is recomputed from the configured members plus the verifier evidence every time either changes, so it is a pure function of state: two nodes holding the same evidence *and the same roster* hold the same ring however they came by it. The roster qualifier is load-bearing — the fixpoint only ever scans members this node has met, so a node that has not yet learned a member does not place it however many units have vouched. Accumulating instead — adopting on the way past and never revisiting — would make the ring a function of *history*: a member adopted before its vouchers were reaped would stay placed on the node that saw that order and never be placed on one that did not, and the split would be permanent.

**Only this node's own dial verifies.** The dial goes to the address the member's id names and the transport authenticates the far end before a byte is written; that is an act this node chose to take. An *inbound* link never verifies the member it names, however well a certificate names it — a member chooses when to dial in and how often, so a vouch earned that way is one the member caused rather than one this node made, and a certificate names a host, which mints as many node ids as it likes, so a member could dial in under each ground id in turn and be vouched for under every one. An *indirect* (ping-req) confirmation is liveness only: it says a relay reaches the target, which this node did not observe and cannot attribute.

**A verifier is a trust unit, and a trust unit is a host.** A host is what a certificate names (§Peer Identity) and one host holds as many node ids as it likes, so the bar counts the distinct *hosts* of the adopted members that vouched, not the ids. Counting ids would let one machine holding two of them raise the whole bar by itself. A member's own host is excluded from its own count for the same reason: a member vouching for a sibling on its own host is vouching for itself. **The host is reduced by the same relation the certificate binding compares by** — lowercased, root label dropped, IP literals compared as addresses — because the two must agree on when two spellings are one host: reading the host as raw text while the binding read it semantically would let one machine present as several vouchers under `evil.example` beside `evil.example.`, or an IPv6 literal beside its expanded form.

**A claim is worth exactly the link that carried it.** A `verified` flag is recorded against the member the receiving link is bound to (§Peer Identity) and against nobody the payload names — and never against *this* node, so a frame arriving on a link that claims this node's own id cannot make it vouch for a member it never dialed — so a member can assert its own verifications and no one else's; relaying another node's would make one member's word enough to place any id it liked. A tuple claiming the sender itself is dropped — a node's place in the ring is never its own to assert — and a claim by a member that is not itself adopted does not count, so two nodes minted together cannot vouch each other in. On the *reply* half of a round, the claim counts only where the dial established who answered: under `CRDTSYNC_CLUSTER_REQUIRE_PEER_IDENTITY=1` a `wss://` member's certificate does and a plaintext member's transport does not, and an unattributable claim must not become an adopted member's vouch. The cost of first-hand-only is that verifications travel by direct exchange rather than by relay: a joiner is placed once enough members have each reached it, which randomized peer selection delivers in a handful of gossip rounds rather than one.

**Two trust units, because one of them is the attacker.** A single compromised member would otherwise vouch for the id it ground and place it itself; requiring a second means an honest member must have independently reached that id, which also keeps an unreachable id — one that would pollute placement and stall every quorum it landed in — out of the ring entirely. The bar is a **constant**, not a fraction of the cluster, so two nodes never disagree about what the evidence has to show. **Configured members are adopted from birth**: the operator's config is the root of trust a cluster starts from, and there is no earlier authority for it to be vouched for by. The one exception is a node configured with *no peers at all*, which has no cluster to be outvoted by and which the constant would freeze forever — its ring is the members it has itself reached. That is the single-node deployment, and it is the peer list being **unset**. A peer list that is set but names nobody other than this node — empty, or every entry a spelling of this node's own address — is refused at startup: such a node has a peer plane and a cluster secret, and would place a member on rooms on its own word alone while every peer it met still held the constant. The exception is decided from the *list*, not from the member count it collapses to, so de-duplication cannot lower the bar.

**An advertise address is an authority and nothing else, in one spelling.** Anything past `host:port` is refused — userinfo, a path, a query, a fragment, and any character a host and a port are not written with — and every address is reduced to a single canonical form before it becomes a node id: the redundant `ws://` dropped, the host lowercased with its root label removed, an IP literal reserialized, and **the port read as a number** so `:9000` and `:09000` are one port on one listener. An address with no canonical form is refused where it is written — in the peer list, in `CRDTSYNC_NODE_ID`, and at every door an id arrives through — rather than joined under, because an id nobody can dial names a member the cluster can never verify and its own node can never be recognised as. Both rules exist because a node id *is* an address and placement hashes it. Userinfo makes the host a reader takes out of the string differ from the host a dialer connects to (`wss://a.example:1@b.example:9000` reads as `a.example` and connects to `b.example`), so a certificate for one host would bind an id that every honest peer verifies by dialing *another* — and the holder then speaks as a member of that other node's rooms. A path, a scheme spelling, or an unnormalized host is a free alias, so one endpoint would hold unboundedly many positions in the ring: every peer that dialed any of them would verify it truthfully, all would be adopted, and only one of them ever speaks — so a room whose replica set filled with them would wait on acks that never come.

**A member is dialed at its own node id.** A node id *is* an advertise address, so a second address for the same member is a second, unauthenticated name for one thing — and the ring turns on it, because a node dials a member in order to verify it. Keeping the advertised address made the roster first-write-wins over a field any peer may set: whoever advertised a member first decided where every later dial went, so two nodes that saw it in a different order verified different endpoints and placed rooms differently, forever. The address a gossip tuple carries is therefore dropped, which also ends the *reply* half's freedom to point the ring anywhere.

**A node converges its ring a little after its roster.** A joiner adopts the cluster's members as their vouches reach it, so for the first few gossip rounds it places rooms over the members it was configured with and holds a ring smaller than the cluster's. A client that reaches it in that window is served by a node that believes it leads rooms it does not — and the write *stalls* rather than being wrongly accepted, because majority-ack counts over the same small replica set and the real replicas refuse its `Replicate`. The window closes as every member's claims arrive, which happens on their rounds as well as its own.

**Reaping takes adoption and vouches with it.** A member removed from the roster leaves the configured set, its verifier set is dropped, and it is struck from every other member's, so the ring is recomputed without its word. A return is a fresh join and is verified again.

**Adoption is only as strong as the identity beneath it, and it does not bound a host.** Under peer mTLS a verification means a certificate for that member's host answered there, so no member can manufacture another's verification. **With no certificates configured none of that holds**: a verification means only that *something* answers at the address the id names, and a secret-holder can bind a link to any member id it likes, so it can raise the whole bar itself. `CRDTSYNC_CLUSTER_REQUIRE_PEER_IDENTITY=1` is what makes the bar a bar. Even then one limit remains, deliberately: **placement keys on node ids and a certificate names a host**, so a member that owns a host owns every id under it — it answers at each ground id, and the honest nodes that dial it verify one truthfully. Adoption bounds the mint to the member's own host and to ids the cluster can actually reach; it cannot bound it further, because every verification involved is genuine. Closing that needs the ring to weigh trust units rather than ids — a placement change, not an evidence one.

**A reduction that is not a fixpoint is not a reduction.** Every spelling must reduce to the same id in one step, or a peer sends the un-reduced form, a door reduces it once, and the roster holds a *second* id for an endpoint it already has — one that resolves to the same host and that TLS accepts against the same certificate, so every honest node verifies it truthfully. So all trailing dots fold, not one; a host with an empty label is refused; and a bracketed host must be followed by its port and nothing else, since a *wrong* canonical form is worse than none because it is accepted. A certificate name carrying a run of root labels binds nothing — refused as malformed rather than folded, which keeps the binding narrow without giving up the fixpoint.

**One further limit follows from the same gap: a certificate that names several hosts is several trust units.** §Peer Identity deliberately lets a node certificate spell its host more than one way — a DNS name beside its IP literal — and each spelling is a different host, so a machine holding such a certificate can hold an adopted member under each and vouch as two units. Until the ring weighs trust units, a deployment that wants the bar to mean two *machines* issues each node a certificate naming one host — and a **wildcard** SAN is the unbounded case of the same thing: the inbound binding refuses a wildcard deliberately, but the outbound dial is plain TLS, which honours one, so a member holding `*.x.example` answers truthfully at every name under it and each reads as a different trust unit. Issue per-host certificates.

## Peer Transport

**A member's advertise address declares the transport its peers dial it over.** `wss://host:port` terminates TLS; `ws://host:port`, or a bare `host:port`, does not. Nothing else can decide it: whether a node terminates TLS says nothing about whether the member it is dialing does, and the advertise address is the only per-member datum the cluster already agrees on and already disseminates through gossip. Any other scheme is a configuration error rather than part of a hostname. The scheme is part of the address and so part of the node id, which is what keeps every node's view of a member's transport identical without a second field on the wire.

**Trust anchors are explicit.** `CRDTSYNC_CLUSTER_CA` names the PEM bundle a TLS member's certificate must chain to; no platform store and no bundled public root set stands in for it. The link carries a bearer credential granting write access to every room the cluster replicates, so trusting an ambient store would widen the set of issuers that can impersonate a member to every CA on the host, and a cluster's certificates are an operator-controlled input. A `wss://` member with no anchors configured is a startup error, not a dial that fails every round.

**mTLS on the peer link is the same handshake read both ways.** `CRDTSYNC_CLUSTER_CLIENT_CERT` + `_KEY` present this node's own identity on its outbound dials; with peers configured with `CRDTSYNC_TLS_CLIENT_CA`, one handshake then authenticates both ends. The client identity is its own configuration, never inferred from the node's listener certificate: a server certificate commonly carries `serverAuth` alone, so reusing it would work in a lab and be rejected in a deployment that issues its certificates properly.

**A cluster may mix plaintext and TLS members, deliberately.** A live cluster cannot switch every node at one instant, and forbidding the mixed state would make a TLS rollout a flag-day restart of a replicated store. So each member's transport stands on its own and every node dials every other correctly at every point of the rollout — noting that a scheme change re-identifies the node, so a rolling migration is a rolling re-join, which the existing reap + gossip-discovery path already handles. But one plaintext member still writes the deployment-wide secret in the clear, so the mixed state is transitional and never silent: a TLS-terminating node names its plaintext peers at startup, and `CRDTSYNC_CLUSTER_REQUIRE_TLS=1` declares the rollout finished and refuses them outright.

**A transport disagreement is a startup error, not a cluster that never converges.** A node that terminates TLS while advertising `ws://` — or advertises `wss://` while terminating nothing — is unreachable to every one of its peers, and the symptom is a node that starts, binds, and silently never replicates. Both directions of that mismatch refuse to start, as does a node that requires TLS of its peers while serving plaintext itself. A member learned by gossip after startup cannot be checked there, so the same rules apply at its dial and a peer this node has failed to open a link to is named after a run of attempts. The dial itself is bounded like the handshake the accept loop terminates, so a far end that accepts a socket and goes silent cannot wedge the task that owns a follower's link.

## Cluster Discovery

Static join via CLI flag, or gossip-based for liveness / room ownership / replication state / membership.

Failure detection is SWIM-style over the gossip exchange: a missed direct probe escalates a member `Alive → Suspect → Dead`, disseminated by anti-entropy and refutable by incarnation bump. **Indirect probing** hardens it against a single bad link — a failed direct probe consults up to k other members (`PingReq`/`PingAck`) before counting the failure, so a member reachable through any relay is not falsely suspected. Each relay answers from its own cached liveness view (an independent vantage), synchronously off the registry actor — never a fresh outbound dial on a requester's behalf, so a ping-req is neither a task-spawn nor an SSRF surface.

The per-room leadership epoch (the split-brain fence) is **persisted** to the durable store as it advances and reloaded on startup, so a restarted node cannot forget the highest epoch it had seen and re-accept a demoted leader's stale-epoch writes it would otherwise fence; the fence is monotone across a restart.

A **late-joining follower** (one whose link comes up after the leader advanced) is **caught up**: on its link-up the leader dials the ops it is missing (from its acknowledged watermark — the whole retained log for a brand-new follower, the tail for a reconnecting one), which it ingests and dedups, converging before it is routed to or promoted over. The leader **branches the catch-up mode by comparing the follower's acknowledged watermark to the room's compaction floor** (the oldest retained op sequence): at or above the floor it dials the ops tail (the ops path, CRDT idempotency); **below the floor** — a brand-new follower joining a compacted room, or one whose acked position predates a compaction — the ops it needs have been folded away, so the leader instead sends a **whole-replica snapshot state-transfer**: the current `encode_state` snapshot tagged with the sequence it represents (the leader's head), carried in a dedicated `ReplicateSnapshot` replication frame. The follower `decode_state`-loads it, *replacing* its replica (so a re-sent snapshot is idempotent), lands its sequence at that head, and resumes the ops tail above it via the steady replication path. Snapshot-load + ops-tail leaves the follower **byte-identical** to the leader — crdtsync's op-join ≡ snapshot-join property (the snapshot at the floor plus the ops above it equal the full state) is what makes the transfer sound. A below-floor follower is never served a partial post-floor delta (which would leave it divergent) and never serves until it has converged — fail-closed. The snapshot rides one frame, bounded by the transport's WebSocket max message size (tungstenite's 16 MiB default, shared with the client-facing `Snapshot` frame); chunking a state larger than that is a documented follow-on.

A **wiped follower self-heals** rather than being trusted at a stale ack. The catch-up above ranges from the follower's *acknowledged* watermark, safe only while the follower still durably holds everything up to that ack; a follower whose durable state was wiped below its ack (a store-less node, a wiped disk, an older-backup restore) would be caught up incorrectly — a silent gap. So on (re)join a follower **reports its true durable head** per room (its current server sequence, read from its own state) in a self-describing `FollowerHeads` frame, and the leader **honors that reported head over any remembered ack** as the catch-up floor: it re-converges the follower from where it actually is (an ops tail, or a snapshot when the reported head is below the compaction floor) and replaces its watermark with the reported head (clamped to the leader's own head, so an over-report can never falsely satisfy majority-ack quorum), so majority-ack durability stops counting it for data it can no longer prove. Fail-closed: a room the leader leads that is absent from the follower's manifest (a room it lost entirely) is treated as head 0 — a full catch-up — never trusted at the stale ack.

**Member reaping** bounds the roster: a member that stays `Dead` past a bounded dead-time (a per-sweep tick count) is removed entirely, so departed nodes do not accumulate as placement replicas. Reaping is convergent, resurrection-proof, and rejoin-safe — a reaped member is tombstoned so a peer still gossiping it `Dead` cannot re-add it, while a genuinely-live return (an `Alive` tuple, which only a reachable node produces) escapes the tombstone and rejoins regardless of its incarnation, so a crash-restarted node (back at incarnation 0) is not permanently exiled. Only a `Dead` member is ever reaped, never a live or reachable-through-a-relay one. The **reap tombstone is itself bounded**: on the same per-sweep tick clock, a tombstone retained past a fixed retention (an order of magnitude beyond the reap window, so well past any in-flight gossip that could still name the member) is **pruned** — the member forgotten — keeping the tombstone set bounded on a long-lived churning cluster. Because the retention outlives all gossip about the member, a pruned member only ever reappears as a fresh join, never an unsafe resurrection.

## Follower Read-Serving (Transparent Proxy)

Writes always go to the leader; **a live read may be served by a caught-up follower** directly from its replicated state (an archived read is the leader's — see the version bullet below), offloading the leader while the client stays topology-agnostic. The consistency model is **bounded-staleness by default plus read-your-writes / monotonicity via a client-supplied floor**:

- A **caught-up follower serves a read at its committed watermark** — its `Hub::seq(room)`, the sequence it has applied via replication. This is bounded staleness: the follower may lag the leader by its replication delay but always serves a **monotonic, internally-consistent snapshot** (the replication path only ever lands a whole ops-delta or a whole snapshot, so any watermark is a consistent state — never torn/partial).
- **Read-your-writes / monotonicity floor.** A read carries the client's highest observed server sequence for the `(room, branch)` — its `Subscribe.last_seen_seq`, which is both the catch-up delta cursor and the read floor. For a **zone-limited** reader that is the highest sequence observed *in the partitions it is served* (§Zones) — the catch-up frames do not hand it a room head it may not see, though other seams still do (§Zones, C116/C118/C119). Read-your-writes survives the narrowing, but not for the tidy reason that a readable write always sits at or below the watermark: above the compaction floor it does, and below it the watermark collapses to `0` and orders nothing. What carries the guarantee is that the answer is never *lower* than the floor the reader arrived on, and that a reader claiming `0` is served the stream from its beginning — the whole projected state where the room has compacted, the whole log where it has not — never a partial delta over content it does not hold. What the client sends back is a separate question: `ClientSession` advances a room's cursor by each delivered batch's length, so a zone-limited reader's floor drifts below what it holds and the gate admits a follower behind it (C117). A follower serves **only if its watermark ≥ that floor**; otherwise it **redirects to the leader** (by definition at or ahead of the floor). A read with floor `0` gets bounded staleness (served at the follower's watermark). Because the client passes the max sequence it has ever seen/written, a laggier follower redirects rather than serving a state behind what the client already observed — the per-client monotonicity guarantee. (A dedicated pre-echo write-seq token, for a *stateless* writer reading back a just-written server seq before the fan-out echo lands, is a documented follow-on — a stateful client already holds its own writes, so its cursor covers them.)
- **A follower serves a read only when it (1) is not the room's effective leader** (the leader always serves, unchanged), **(2) is a replica of the room, (3) holds a materialized copy** (it has been caught up — never an absent or not-yet-converged replica), **and (4) its watermark ≥ the floor.** Any failure redirects to the leader. Every unsafe read — a not-caught-up follower, a non-replica, a read past the floor, or a write — **fails safe by redirect**: a follower never serves a torn, missing-a-just-written-op, or backwards-in-time read, and never accepts a write. The follower is kept converged by the steady replication + late-joiner catch-up above, so serving is a pure read of already-converged state with no new replication path.
- **A version read is the leader's, not a caught-up follower's.** `VersionList` and `VersionFetch` take the same leader gate the version *mutations* take, because a **version index is per node, not per room**: replication carries the room's log and never its captures, so a read answered off whichever node holds the channel answers about that node's own captures rather than the ones the room's mutations built — plainly, a version the client just created and a read that says it does not exist. Routing the read to where the mutation lands is what makes a room's captures answerable, and it carries with it the freshest `acl_records` a fetch redacts by (those tuples ride the log, so a replica's are as old as its last replicated commit). The redaction is a reason to prefer the leader, not a hole the routing closes: the same replica's live stream is behind by the same records at the same instant and serves the same reader the same subtree. A **client-named floor cannot substitute**: it bounds a `Subscribe` because a subscribe arrives on a fresh connection carrying a cursor accumulated on another node, while a version read arrives on a channel already bound *here*, whose cursor a conforming client advances only from what this node delivered — so it never names a sequence this node is behind on. A replica serves version reads once it holds the room's leadership, which is what keeps the gate from being a blanket centralization — and is as strong as the leadership is: promotion on the read path is each node's own liveness view, with no lease or caught-up condition, so a node that promotes over a still-committing leader answers from its own records for the promotion window (C113). The archival read is also not the load the follower-read seam exists to offload: what it offloads is the live stream. A **version diff** hands out the same captured bytes through the same projection, so it takes the same gate; a **branch** diff does not (C103).
- **A served subscription is a live stream, so the replication apply path fans out.** A Subscribe is not a point read: it binds a channel that must keep advancing, and on a follower the leader is the stream's sole author. So a batch the follower ingests from a `Replicate` frame is fanned out to that follower's own subscribers, through the same seam a locally-authored write takes — per-recipient doc-ACL redaction, the whole-document read gate, per-channel zone scoping. Two things distinguish it. The **exclusion set is empty**: the batch was authored on the leader, so no channel here already holds it, unlike a local write which omits its own authoring channel (§Connection / Multiplexing). And **every verdict is re-decided against this replica**, never inherited — what the leader computed was an answer for *its own* subscribers, a different set of actors holding different grants at different schema versions under different zone scopes, and the frame carries none of it (the relay seam replicates committed ops verbatim and untagged, which is also why no migration translation runs on this path — the same verbatim delta the follower's own catch-up replays from its log). A follower-local subscriber's replica advances on exactly what this seam delivers and on nothing else until it reconnects, so the two — the live stream and the resume delta — must agree, which is what reusing the write path's seam rather than growing a second one buys. *Which schema* the follower composes those verdicts under is still the binding its own first subscriber seeded, since replication carries the creator and not the room's `{app, version}` — the gap C62 closes.

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

**Replay tool (as built).** A `crdtsync-replay` bin in the server crate reconstructs a room's exact state as of any past server sequence and diffs two such points — the non-UI vertical slice of the above. It is **read-only over the durable room data**: it opens the same `<room>.log` + `<room>.snap` files the server persists, rebuilds in memory, and never writes back — no watermark advances, no byte changes. Reconstruction reuses the server's own restore path (`Hub::from_rooms` over a log truncated to the target sequence): the nearest snapshot at or below the target seeds the state, then the retained op tail replays up to it, so the result is byte-identical to what the room held at that sequence — across a compaction floor, leveraging op-join ≡ snapshot-join. A target below the compaction floor is rejected (its ops are folded away); the floor itself reconstructs from the snapshot alone. The reconstruction/diff logic lives as library functions (`server::replay::{reconstruct_at, diff_at}`) with the bin a thin CLI over them. Reconstructing a no-snapshot room's exact leading replica id needs that node's server id (an optional flag); a snapshot-backed room pins it. Richer CLI UX (op inspection, timeline/causal-graph visualization) and the **admin-UI dashboard** (§Admin UI) stay deferred/last.

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
- **Owner** — a **dynamic, recursive, path-scoped capability** held by an actor over a room or a path within it. An owner has full access to its subtree *and* meta-authority (grant / revoke) over it. The document creator auto-owns the root path `/`; multiple owners per path are allowed. Owners live as **doc-level ACL state** (the CRDT tier), self-organized at runtime — never declared in the schema. The **creator binding itself is not CRDT state** — it is the first authenticated writer, which the document cannot name — so it is server-side **room metadata, replicated with the room** and durable beside it. A replica that holds a room must therefore hold the root its redactions resolve against: a node holding only the ACL tuples holds the grants without the authority they are decided under, and evaluates every one of them as inert.
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

**Which schema the composition consults** — step 4 of the decision flow — is the **acted-on room's** governing binding, never the connection's self-declared app. At the points a client **frame** reaches, that room comes from the frame: a **channel-keyed** frame takes the room its channel is subscribed to, and a **room-keyed** one — branch management, a cross-zone token request, a clone (off its *source*) — takes the room it names, since a caller may manage a room it holds no subscription to. The connection's own app is the fallback for a **subscribe** alone: that is the one frame whose caller is about to become the room's incumbent, and anywhere else it would be the caller choosing which `@auth` grants and zone declarations govern someone else's room. (The points reached without a frame resolve their room from what they are serving instead — a fan-out's own room, a blob's referencing room — and the connect and admin-plane gates carry no room at all, deciding on `Resource::App`.) A frame resolving no binding — a subscribe aside — is governed by no schema, so step 4 abstains and the flow default-denies, including a frame naming a room nothing ever bound (C62).

## Wire-Level Redaction

If bytes hit the client, assume they leak. Server never sends unauthorized data, ever. Per-recipient filtering on every op send and every cold-start snapshot.

**A redaction resolves against the tree of the *stream* it is serving — ops as much as state.** "The tree of the bytes being served" (§Named Versions) is not a property of state blobs; it is a property of every seam that redacts. A `(room, branch)` stream serves the branch's own tree — its owned base (a restore, a publish) or the history it shares with `main` below its fork point, in both cases with its divergent tail folded in — and `main` moves on past that. So the live op fan-out, the op catch-up, the subscribe's admission gate and the catch-up snapshot gate all resolve one branch stream's element scopes and op targets through that branch's tree rather than the live room's — with the one exception that a gate deciding whether to *admit* a reader at all may fall back to the live room's when the branch has no tree, since it then refuses on the branch's own terms and serves nothing either way. Resolving through `main` is the same inert scope twice over: an element-scoped grant whose target has left `main` resolves to nothing, and an *op* whose container target has left it resolves to the **root**, which a root-readable but subtree-denied reader carries. Both are reachable without any reconnect or restore, by an ordinary branch write. What does *not* move to the branch is the **authority**: the doc-ACL tuple set governing a branch read is the room's live one, not the branch tail's (branch-level ACL is v0.4 scope). Serving a per-branch tree at op-rate means the server materializes one per branch it actually redacts for and folds it forward by the tail ops it fans out, so a branch write costs the ops it wrote rather than the base it wrote onto. A stream with **no** tree at all — an owned base this node cannot decode, or a shared base compaction has dropped out from under a live-log fork — is refused rather than redacted through a substitute tree, wherever the room holds tuples to redact by. Every substitute available (the live room's index, an empty one, the branch's own narrowed fold) resolves *less* than the truth, and what resolves to nothing is admitted rather than withheld: a scope that resolves to nothing is an inert deny, and an op target that resolves to nothing reads at the root.

**State that resolves to no path or partition is withheld from every narrowed serve.** Both filters decide by the *live* tree: an op's read path and a projection's purge set are resolved by walking from the root. State whose governing element the walk does not reach therefore has no verdict of its own — an annotation outlives the sequence it anchors (only an explicit delete tombstones one), an element-scoped ACL tuple outlives its target. The stand-ins available are all wider than the reader entitled to it: the root read verdict admits a root grant a subtree deny carves out, and "names no zone" admits every zone-scoped subscriber, since all of them hold the root partition. So a narrowing serve drops it outright — for a zone the attribution is not merely unresolved but unrecoverable, since the key a container was derived under is one-way — and the reader entitled to the whole document is served by the caller declining to narrow at all — the rule the zone projection has always followed for a whole-zone subscriber, now the read projection's too. The op seam names the same audience explicitly, since it has no decline to lean on: such an op is gated on the whole-document read verdict rather than on any path. On the *zone* dimension it cannot say the same thing — every value the envelope's partition can take names a per-zone clock, so there is no "no partition" to stamp — and such an op keeps the root partition while the projection drops the state form (C82) — which, since the partition is also a mint floor, costs the op's family a lost update as well as an audience. **The whole-view verdict belongs to the authority, never to the document.** A projection must not re-derive it from the ACL tuples the state it is narrowing happens to carry: whether a reader is denied anywhere is a property of the room's live authority, and the seams that serve an *archived* state — a version, a branch base, a diff side — narrow bytes whose tuple set is not the live one. A retained *container* the walk does not reach is a separate question and is still served: those registry entries are what displace-then-recreate identity retains, and dropping them loses content a subscriber entitled to the partition gets back when the slot is re-won.

## Zones (Coarse Partition)

For docs with large auth-uniform subtrees, declare zones — separately replicated streams. Per-zone lamport clocks (avoids cross-zone activity leakage). Client subscribes only to zones it's authorized for. Unauthorized zone ops, snapshots, structure, even element counts never sent. Cross-zone tree moves forbidden at schema level. Cross-zone anchors forbidden by default; opt-in opaque references for marks / comments.

Zones are a perf and isolation optimization. For fine-grained per-instance auth, ACL set carries the load. For coarse uniform-auth subtrees, zones are highly efficient. Both work together.

**Zone vs. doc-level ACL — different strengths, deliberately.** ACL redaction (§Wire-Level Redaction) filters *within one replication stream* — an unauthorized client still learns the document *structure* (that a redacted subtree exists, its element counts, activity via the shared lamport). A **zone is a separately replicated stream** with its **own lamport clock** and op-log partition: an unauthorized client receives *nothing* — not the ops, snapshot, structure, existence, or size, and cannot infer activity from clock jumps. Zones are the coarse, subtree-aligned, strong-isolation primitive; ACL is the fine, within-stream one. Per-element dynamic zoning is deliberately *not* a thing — that scatters a zone across the tree (defeating the subtree=stream isolation and duplicating ACL); fine-grained dynamic control is ACL's job.

**Static, path-rooted, schema-declared.** A zone is declared in the schema (`zones` block) as a name → a subtree **root path**; every element under that path is in the zone, by structure. Static (ships with the schema, like `@auth`). This is what makes the isolation cheap — a zone is a contiguous subtree, so it maps to one stream, one lamport, one "don't send this subtree" redaction. Causal independence is *enforced* (cross-zone tree moves and cross-zone anchors forbidden), so the N per-zone lamport clocks never need cross-zone ordering. A cross-zone **anchor** is caught by the read-time `validate()` pass (a static tree property); a cross-zone **tree move** is *not* statically detectable from the post-move tree (the node renders well-placed under its new parent), so it is refused at the **op-submit gate** — a recoverable `OpsRejected{Forbidden}`, the op never entering the log so replicas converge on its absence — resolved against the server's **element-context index**: a lean derived id → context (path, zone, and later declared type) projection over the room's already-materialized document, not a separately-maintained replica. Zone access reuses the authorization seam (`Resource::Zone`, subscribe-gated); the `Channel` handle widens to `(room, branch, zone)`, each authorized zone a subscribable stream.

**A zone id is a position, so the block is append-only.** An op's `zone` is the zone's **index** into the schema's order-preserving `zones()` (§Internal Data Model), so the block's declaration order is persisted meaning: every op already in a room's log, and every scope a subscription resolved, denotes whichever zone now sits at that position. The registry therefore accepts a new version only where its `zones` block **extends** its predecessor's — the earlier `(name, path)` pairs unchanged, in order — and refuses a reorder, a rename, a re-root, or a removal. Retiring a zone means leaving it declared; adding one is the only evolution. The registry also **parses** every body it stores, because a version that resolves to no schema at all reads at each zone seam as a room with no partitions and is served whole.

**A subscription carries zone names, not ids.** The schema acting over a room is not pinned to the moment a channel subscribed: the governing version lifts whenever a newer client of the same app joins, a clone landing re-points a name's binding at the source's app — or *removes* it, where the source is ungoverned, and a room nothing had bound acquires a schema the first time an enforcing client subscribes. So a channel holds the **selector it was admitted under** — a zone name, or the empty whole-room selector — and every seam that narrows by zone (the live fan-out, the catch-up delta, the version fetch, each side of a diff query) resolves it to ids again against the schema it is about to narrow with, re-taking each zone's `Resource::Zone` read verdict at the same time. A scope frozen at Subscribe goes stale in three directions: a channel that joined a room declaring no zones held *no* scope and was served every partition the room later declared; one that joined a zoned room held ids the append-only rule now keeps meaningful but a foreign block would not; and a per-zone read the deployment revoked afterwards narrowed nothing. A **named** selector that no longer resolves — no such zone in the acting schema, or its read now denied — narrows to the **root partition alone**, never to "do not filter". The whole-room selector against a schema that does not resolve is the one case this cannot answer: a room genuinely declaring no partitions is indistinguishable there from one whose schema the node cannot resolve, so it is unfiltered (C101, C62).

**The partition an op declares is its author's reading.** `op.zone` is stamped by the writing replica against the schema *it* holds, and the server relays that stamp rather than re-deriving it — so a writer whose schema does not declare a region emits its edits in the root partition, which every scope admits. That covers a **relay** connection, which holds no schema at all and is a permanently supported mode, and any client mid-rolling-upgrade — the intended way to lift a `zones` block — so both leak across the partition their readers are narrowed by. The reader's half of the seam is exact wherever the room's schema resolves; the writer's half is the author's word, and closing it means re-deriving each op's partition at ingest against the acting schema (or refusing a batch that disagrees with it).

**The catch-up watermark is narrowed like the content it tags.** A `Snapshot` and a `VersionState` each carry the sequence their bytes stand at, and a room's sequence counts its *whole* log — so serving that scalar unnarrowed hands a zone-limited reader the count of ops written into the partitions it is never sent, and the difference between two readings charts a hidden partition's write volume over the window between them. Version *names* are a room-read fact (`VersionList` returns them all) and `autoVersion` schedule triggers mint them on a clock, so the readings are enumerable without any further access. A narrowed reader gets a different answer on each frame, because the two frames can afford different ones. On the **catch-up** frame it is told the last sequence in the stream its own scope admits (`Hub::partition_head`), floored at the watermark it already holds — that scalar *is* the resume cursor and the read floor of §Follower Read-Serving, so it has to be a real room sequence. Within a compaction epoch it moves only when a partition the reader can see is written, so a window holding nothing but hidden writes reads like an idle one; across a compaction it can collapse to `0`, since `partition_head` reads the *retained* log and the floor moves with the room's total volume — a residue, not a regression, because it replaces an exact count with one bit and refines a boundary the `Snapshot`-versus-`Ops` frame type already discloses for free. On the **version** frame it is told `0`, always: that read feeds no cursor and carries no floor, so it can refuse the field outright, and it must — a retained-log answer there would make one *fixed* capture's scalar flip when hidden writes alone triggered a compaction, which is a signal the unnarrowed scalar never carried. Read-your-writes survives: a write into a partition the reader may read is admitted by its own scope, and a write into one it may not read is withheld from it on every seam anyway.

**The promise is not yet whole, and the gap is enumerated rather than implied.** Narrowing those two scalars is necessary and not sufficient: the room's sequence, or a proxy for it, still reaches a zone-limited reader by several other routes, each measured and each its own unit. `Message::Branches` reports `main`'s head, which *is* the room's log head, to any reader holding room read (**C118**) — the same number the watermark used to hand over, but reachable without a compaction or a name to enumerate, and at any rate. The follower-read gate compares an attacker-chosen floor against the answering node's whole-room watermark and answers redirect-or-serve, a one-bit oracle that binary-searches the number in a couple of dozen probes, and it resolves before the reader's zone scope is even computed (**C119**). `Hub::catch_up` branches to a `Snapshot` exactly when the floor falls below the compaction floor, so sweeping floors recovers `base_seq`, which steps with the room's *total* op volume (**C119**). A `publish` and a restore-as-branch mint a raw sequence — the room's, or the active editor branch's — into the version *name*, and the `Versions` reply hands every name over unnarrowed (**C116**). And the watermark itself still names a position in the room's shared sequence space, so a reader that may write probes it with its own op and reads hidden volume at its own chosen resolution — not a diminished residue of the closed channel but the same channel at full fidelity, one write more expensive (**C115**). Retiring the shared sequence space for the per-zone one this section's end-state names dissolves C115 and C119 together — but only because that end-state partitions the *op-log* too, not merely the numbering: a per-partition log carries a per-partition compaction floor and a per-partition replication watermark, which is what both of C119's oracles compare against, so each recovers a number about the reader's own partition and no other. The rest do not collapse into it or into each other: what a *name* may say (C116), what a *client cursor* may count (C117), and what a branch *head* may report (C118) are three separate rulings, and each has a cost the watermark's did not.

**Cross-zone move — opt-in, AEAD capability token (as built, Zones-4).** The default cross-zone-move rejection has one authorized bypass: a **server-sealed capability token** authorizing exactly **one** cross-zone move. The server holds a 32-byte **zone-master key** (config, like the TLS cert — `CRDTSYNC_ZONE_KEY`, never leaving the server; unset ⇒ the escape hatch is off and every crossing stays rejected, fail-closed). **Issuance:** a client sends `CrossZoneToken{room, element, dst_zone}`; the server ACL-authorizes it through the *same* `authorized` evaluator the write gate uses — the actor must hold **write authority at the element's current path** (move authority) *and* **write authority to the destination zone** (`Resource::Zone{room, dst}`, or the room for the unzoned root) — and, if allowed, replies `CrossZoneTokenGrant{token}`; a denial mints nothing. The token **AEAD-seals** (ChaCha20-Poly1305, RustCrypto — constant-time in software, no AES-NI) the binding tuple `(room, actor, element, src_zone, dst_zone, expiry)`, the whole tuple authenticated so it is unforgeable and non-transferable to a different actor / element / src / dst / room; a fresh random 96-bit nonce per seal (never fixed). It is **opaque** to the client. **Redemption:** the client submits the move as `CrossZoneOps{ops, token}`; at op-ingress the server decrypts+authenticates the token under the zone key and admits the crossing only when the sealed binding matches the op's *actual* `(actor, element, src, dst)` crossing (computed by `index::batch_zone_crossings`, the same document-simulation the rejection uses) and has not expired. Any mismatch, forgery, expiry, or absent token ⇒ the crossing stays rejected exactly as before (`OpsRejected{Forbidden}`, op never logged). The token is **consumed at ingress** — never entering the log or fanning out, so the committed move op is token-free and a plain `Ops` write is entirely unchanged. **Replay** is bounded by the `(actor, element, src, dst)` binding + a short expiry (30 s) + the existing op-id dedup; a seen-nonce cache is a hardening follow-on, unbuilt (the binding + expiry suffice for v1). Redemption is leader-served (it resolves against the room's authoritative document), and gated to `main` like the base rejection.

**Cross-zone references — opt-in, sealed handle (deferred).** By default a cross-zone anchor is rejected at schema validation. The opt-in (a comment / mention in zone A anchoring into zone B) is a **per-recipient redaction**, not merged state: the authoring client (authorized for both zones) writes a *real* anchor, the server stores it, and only at fan-out to a recipient lacking zone B does the server replace the real `(zone, element_id, position)` with an **opaque token** — an **AEAD-sealed handle** (server key; deterministic sealing so a given ref yields a stable token; associated data binds it to the room so it can't be replayed). The unauthorized client holds the token, round-trips it, renders "anchored in a restricted area," and it resolves only if the client later gains zone B access. Stateless (the token *is* the sealed data — no server mapping table, no GC), reusing the server-crate crypto precedent (schema hash-lock). Deferred to a follow-on; the first zones cut ships with cross-zone anchors simply forbidden.

## ACL State Is Itself Privacy-Sensitive

Existence of "Alice can read X" leaks that X exists and Alice has access. ACL tuples redacted per recipient: sent only if recipient is the subject, or has `acl.read` on the resource. Admins see all. Regular users see only tuples involving them.

## Meta-Auth

Schema declares meta-rules about who can mutate the ACL subsystem. App tunes per-app: some apps let any editor share a section; some restrict grants to owner only.

## Producer-Side Defense in Depth

SDK won't let a client construct an op targeting elements / paths / zones it can't write to. Invalid op never leaves client. Server still re-validates — client-side is advisory.

## Audit

Op log is the authoritative record. Every op has `actor_id` + lamport + timestamp. Audit = log query. Separate access log for read-only actions (connect, snapshot export, branch read) since those don't generate ops.

**As built — the audit trail + operator query surface.** The read-only-action access log is durable: a single append-only structured file-log (`AuditLog`), the same durability shape the op store uses — one length-framed record per event (`u64` timestamp, action + decision tags, length-prefixed actor, tagged resource), flushed before the append returns, never mutated or removed. It is *not* a DB (deferred — see Revisit). Records are the security-relevant events: every **denied** access decision and every **write** (the ACL grant/revoke mutations ride the write path), plus three read-only-action variants added to the `Action` enum — **`Connect`** (a client authenticated), **`Export`** (a blob/state/snapshot left the server), **`VersionRead`** (a captured version's state was fetched). Routine *permitted* reads and awareness publishes are not persisted — the trail is refusals, mutations, and the explicit events, not the read stream.

Auditing is wired at the existing seams: the durable log is an `AccessLog` sink composed under an `Audited` authorizer, so every enforced decision persists at the point it is enforced; `Connect` and `VersionRead` route through the authorizer's `observe` at the connect/auth site (registry) and the version-fetch site (session); `Export` is recorded at the blob-fetch route. A failed append is fail-loud — it returns the IO error to a direct caller and latches a health flag the query surface reports (a `500`), never silently dropping a security event.

The **query surface** is an operator admin-HTTP endpoint (`GET /audit`, its own `serve_audit` listener) — the operator→admin-HTTP audience side, never the app→wire client path. It is gated by the same verifier + authorizer as the schema-registration plane, requiring `Read` on the reserved `$audit` app resource, so the trail is never exposed to an app client. It filters by actor / action / room / time-range (half-open `[since, until)`) and returns the matching records as JSON. The query is a straight scan over the whole log (v1); a rebuildable in-memory index is a scale follow-on. The path is strictly read-only — it never writes, mutates, or removes a record.

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
- **Two op layers.** The *core op* carries only what merge needs — `{id, stamp, target, kind, tx, zone}`, where `zone` is the compact zone id (the per-zone lamport partition, `None` = root). Authorship (`actor_id`), the `room`/`branch` scope, `schema_version`, and wall time are **wire/server-envelope** concerns wrapping the core op, not core op fields.
- **element_id derives from `(parent_id, key, kind)`** — the kind is in the tuple, so a type-flip on a slot yields a different id, which drives the displacement path correctly.
- **Displacement retains, it does not forget.** A displaced container/counter is kept in a persistent per-id registry and *reinstated* if its slot is re-won; a displaced counter keeps accumulating. This is a **convergence requirement** — orphan-and-forget (as the older Map Slot Safety prose implied) diverges across replicas. The orphan event still fires for the app; the state is retained.
- **Creation emits an op.** Get-or-create emits an op on the create path (silent on get). Derivation gives *convergence* for concurrent same-slot creates; the op gives *propagation* (a peer learns the container exists before a child op targets it). Both are needed — "convergence by derivation, not API" holds for convergence only.
- **The op-log is the source of truth; a snapshot is a compaction artifact,** not a separate cold-start channel. Every state change is an op; replaying the log reproduces the state.
- **Persistence is a per-room append-only file log** + optional `<room>.snap` snapshot — not SQLite. Crash-safety is hand-rolled (append flushes before return; compaction is temp → fsync → rename → dir fsync → truncate, with dedup-on-replay).
- **One binary codec, shared by the wire and the log.** Deterministic little-endian, length-framed, total-decode (a `DecodeError`/`ProtocolError`, never a panic). Not CBOR/MessagePack. The 8-byte header (`"CRDT"` magic + version) pins the protocol version; the codec itself is negotiated in `Hello`, and one codec (`CODEC_V1`) exists to negotiate.
- **Compaction is keyed on the server sequence** (`base_seq`), not a lamport timestamp. Cold-start (`catch_up`) returns **either** an op delta (at/above the room's floor) **or** a whole-replica snapshot regenerated live (below it) — never snapshot-plus-tail.
- **Text is codepoint-only; grapheme segmentation is an SDK / editor-adapter concern.** The v0.1 roadmap listed "grapheme helpers"; the built core keeps them out — `Text` indexes by codepoint and ships no grapheme API, so no Unicode-segmentation table is pulled into the core (the same dependency-minimalism that keeps `getrandom` out). An editor adapter, which already handles its editor's idiosyncrasies, maps grapheme positions to codepoint indices. Convergence is codepoint-based and unaffected.

## Planned, not yet built (the prose above reads present-tense — it isn't yet)

- **`Ack` frame** — a reserved no-consumer wire slot; its GC-watermark purpose was dropped for compression (§Tombstone GC), which needs no acknowledgement. The `Accepted` frame + `ClientSession` outbox (the offline queue) **are** built.
- **Element-ref value slot** — `Scalar::ElementRef(ElementId)` (a bare same-room element id — §Internal Data Model) is built as a forward-compat reservation like the blob-ref slot: round-tripped in the codec (`tests/elementref.rs`), no producer / consumer yet. A `kind` hint on the ref is the remaining additive step, deferred until schema validation wants it.
- **Op-batching RLE** — the codec frames one op per record; cross-op run-length encoding is a later additive op kind.
- **Also absent:** client_id generation/persistence in the SDKs (they take a caller-supplied 16-byte id). (Codec negotiation shipped as a forward-compat reservation — the `Hello` advertise / `CodecSelected` answer seam is built and exercised, with exactly one codec to select. Cluster peer links deliberately do not negotiate: they advertise nothing and speak the base codec, since neither the replication pump nor the gossip round-trip has anywhere to fold a selection — negotiating them is a follow-on for when a second codec exists. `RelativePosition`/anchor SDK type shipped (#137); the XmlElement / XmlFragment / RangedElement primitives + their path/SDK surface shipped (XmlElement epic complete). The Error `details` field is reserved on the wire — round-tripped, empty, no producer — see §Error Envelope.)

## Revisit items (accepted now, flagged for a later look)

- **File-log vs. an embedded DB for the query/metadata side.** The append-only file log is right for the op hot-path, but the admin UI / op-log viewer / audit-query / retention features want queryability, and durability is now hand-rolled (a directory-fsync crash bug already shipped and was fixed). Reconsider SQLite/redb for the *metadata/index* side if those consumers land — a checkpoint, not a reversal.
- **Cold-start snapshot CPU.** A below-floor subscriber triggers a whole-replica `encode_state` regenerated live on every cold-start — O(state) CPU per connection. Fine at current scale; cache the encoded snapshot per compaction floor if snapshots grow large or cold-starts get frequent.

---

# Roadmap

> **Live build status** — what's actually shipped vs. in progress lives in [KANBAN.md](KANBAN.md); this roadmap is the plan of record. Build order has diverged where dependencies allowed: the portable-runtime work (WASM, C ABI, Python, Go bindings) landed early alongside the v0.1 core rather than waiting for v0.3.

## v0.1 — Single Node MVP

Websocket sync, room support, op log, snapshots, embedded persistence, TS SDK, shared CRDT core, primitives (Map, List, Text, Register, Counter), anchors / RelativePosition, Map slot safety, op batching wire format, token validation + actor_id, blob ref reservation + local FS backend + small-blob inline, tx field reservation + non-atomic transact, Text codepoint identity + UTF-8 + grapheme helpers, closed op kind enum, UUID v7 client_id, single multiplexed WS, three-phase handshake, standardized Error envelope.

## v0.2 — Developer Experience

Declarative policy file with audit log, awareness subsystem (TTL + throttle + auth filtering + reconnect grace), reconnect, compaction with tombstone GC watermark, admin dashboard, replay tooling, UndoManager for v0.1 primitives, composition cookbook, named versions + auto-version triggers, ergonomic JS/TS SDK (handle-graph surface, §SDK-Ergonomic-Surface).

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
