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

---

## What this is NOT

### Not just a CRDT library

The focus is not:

- "yet another CRDT implementation"
- purely academic CRDT research
- a data structure package only

The focus is:

- operational infrastructure
- deployment simplicity
- production-ready sync
- batteries-included collaboration

---

## Problems with existing solutions

### Yjs

Strengths:

- excellent CRDT implementation
- battle-tested
- strong JS ecosystem

Weaknesses:

- backend story is fragmented
- websocket providers often handwritten
- persistence is DIY
- scaling architecture unclear
- multi-language editing awkward
- operational setup fragmented

---

### Liveblocks / hosted providers

Strengths:

- batteries-included
- easy onboarding
- polished developer experience

Weaknesses:

- SaaS lock-in
- opaque internals
- expensive at scale
- less control
- difficult self-hosting story

---

# Main Product Goals

## 1. Batteries-Included Deployment

Users should deploy:

```bash
crdtsync serve
```

or:

```bash
docker run crdtsync
```

without provisioning:

- Postgres
- Redis
- Kafka
- ZooKeeper
- NATS
- etcd
- Elasticsearch
- external brokers

The system should contain:

- storage
- replication
- pubsub
- snapshots
- clustering
- failover
- room routing

inside one deployable unit.

---

## 2. Portable CRDT Core

The CRDT implementation should exist exactly once.

Avoid:

- reimplementing merge logic in every language
- duplicated protocol semantics
- divergent implementations
- compatibility nightmares

The core should be:

- implemented in OCaml
- exported as WASM
- exported as stable C ABI
- wrapped by thin SDKs

---

## 3. Multi-Language Support

Clients in:

- JavaScript
- TypeScript
- Python
- Go
- Rust
- OCaml
- Node.js
- JVM languages

should all edit the same document naturally.

---

## 4. Operational Simplicity

Infrastructure should feel like:

- SQLite
- Tailscale
- Fly.io
- LiteFS

rather than:

- Kubernetes-first infra stacks
- distributed systems requiring many dependencies

---

# High-Level Architecture

```text
               ┌───────────────────┐
               │    Client SDKs    │
               │ JS / Python / Go  │
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

---

# CRDT Model

The system supports a focused set of primitives. Avoid generic CRDT abstractions early.

```text
Document               root container: named Map<string, Element>
 ├── Map               string-keyed, recursive values
 ├── List              ordered items, recursive values
 ├── Text              collaborative char sequence, lives anywhere
 ├── XmlElement        tag + attrs (Map<string,Value>) + children (List<XmlElement|Text>)
 ├── XmlFragment       root container of XmlElements (no own tag)
 ├── RangedElement     (start_anchor, end_anchor, payload: Element) — generic ranged annotation
 ├── Register          single LWW value
 └── Counter           increment/decrement
```

## Rationale

- Map/List/Text/Register/Counter cover structured collaborative apps (Kanban, settings, code editors, dashboards, forms).
- XmlElement covers document-style trees (ProseMirror, HTML, SVG, OOXML-shaped data). Attributes are first-class: modeled as a collaborative `Map<string, Value>`, so attribute values can themselves be `Text` or other CRDTs.
- RangedElement is the generic ranged annotation. Models marks (bold/italic/links), comments, suggestions, highlights, mentions, and domain-specific overlays. Payload is any Element — recursive composition.

## Document Shape

Document root is `Map<string, Element>`. Top-level types accessed by name:

```ts
doc.getText("body")
doc.getMap("settings")
doc.getXmlFragment("content")
doc.getList("comments")
```

Text is standalone — can live at the doc root, inside a Map value, inside a List item, or inside an XmlElement's children. XmlElement children are restricted to `XmlElement | Text` (matches DOM/XML model). All other containers accept any Element type.

## Why XmlElement (not "Tree")

A generic Tree without attributes is a strict subset of XmlElement with attributes. Solving for XmlElement solves for Tree (leave attrs empty); the reverse does not hold. XML's three-way split (tag / attrs / children) is the right shape for document data — HTML, SVG, ProseMirror, RSS all chose it because it fits. We claim the data model, not the angle-bracket serialization (wire format stays binary).

---

# Supported Operations

## Text

```text
insert(index, chars)
remove(index, length)
replace(index, length, chars)
```

Positions in user-facing APIs are integer offsets; internally these resolve to char-ID anchors so concurrent edits don't drift.

---

## Map

Wire-level ops:

```text
set(key, value)              // unconditional LWW; displaced Element refs surface as orphans
delete(key)                  // remove key; displaced Element ref surfaces as orphan
```

SDK adds ergonomic getOrCreate surfaces (e.g. `Map.text key`, `Map.list key`,
`Map.map key`) that internally derive the child element_id from `(parent_id,
key)` and emit a `set` op — concurrent calls converge on the same Element by
construction. See Map Slot Safety.

Nested values supported (any Element type). Scalar set uses LWW.

---

## List

```text
insert(index, item)
remove(index, count)
move(from, to)
```

---

## XmlElement

```text
setAttr(key, value)          // attr values may themselves be Element (e.g. Text)
removeAttr(key)
insertChild(index, child)    // child: XmlElement | Text
removeChild(index)
move(node, newParent, index) // tree move — implements Kleppmann 2021 (see Algorithms)
```

---

## RangedElement

```text
create(start_anchor, end_anchor, payload: Element)
removeRange(rangedElementId)
updatePayload(rangedElementId, op)   // routes to payload's own CRDT ops
```

---

## Register

```text
set(value)                   // LWW
```

---

## Counter

```text
increment(n)
decrement(n)
```

---

# Extensibility

The CRDT primitive set is **closed**: Map, List, Text, XmlElement, XmlFragment, RangedElement, Register, Counter. The wire-format op `kind` field is a fixed enum. Apps cannot define new CRDT types in app code.

## Why Closed

Custom CRDT types in app code would mean custom merge logic shipped per SDK language. Different impls = divergence risk. Sandboxing custom merge functions has the same cost as the migration DSL machinery. Wire format stays compact only if `kind` is an enum. Yjs, Automerge, and Loro all reach the same conclusion.

## What Composition Covers (~95% of "Custom" Wants)

| Want | Build with |
|------|------------|
| Counter with min/max bounds | Counter + app-side clamping on read |
| Set | `Map<key, true>` with delete |
| MV-Register | `List<{ timestamp, value }>` |
| Geographic position | `Map { lat: Register, lng: Register }` |
| Numeric range | `Map { start: Register, end: Register }` |
| Time interval | `Map { start: Register<timestamp>, end: Register<timestamp> }` |
| Task with status enum | XmlElement + schema-declared enum attr |
| Tag list | `Map<tag, true>` or `List<string>` |
| Comment / annotation | `RangedElement` with payload |
| Nested arbitrary structured data | composition of Map / List / Text / XmlElement |

What composition cannot cover: fundamentally new merge semantics (max-CRDT, min-CRDT, custom convergence rules). These are rare. Apps approximate via composition or propose a new primitive through the escape hatch.

## What IS App-Customizable (Schema, Not "Custom Types")

Apps freely customize within the schema layer:

- new XML element types with custom tags, attrs, children rules
- new mark names with custom value shapes and merge kinds
- new attr value types within supported value primitives
- declared constraints (ranges, enums, required fields)
- awareness entry shapes
- ACL tuples and meta-auth rules

These are **structural / type-system** features layered on top of fixed CRDT primitives. Schema handles them. Not "custom types" in the new-CRDT-primitive sense.

## Escape Hatch

New CRDT primitives can be proposed via RFC, reviewed against criteria — cross-language implementability, schema fit, no conflict with existing primitives, real demand — and accepted into core through the normal release cycle. Adding a new primitive bumps the engine version; old clients reject the new kind at handshake-time version negotiation.

Apps do not ship custom CRDT primitives in their own code.

## Op `kind` as a Closed Enum

```text
op.kind = enum {
  text.insert | text.remove | text.replace
  map.set | map.delete
  list.insert | list.remove | list.move
  xml.setAttr | xml.removeAttr | xml.insertChild | xml.removeChild | xml.move
  ranged.create | ranged.remove | ranged.updatePayload
  register.set
  counter.increment | counter.decrement
  acl.grant | acl.revoke
  tx.commit
  migrate
}
```

Receivers panic on unknown `kind` — protocol violation, indicates bug or version mismatch caught at the handshake layer. Compact wire encoding (small integer per kind, not a string).

## Cookbook

SDK ships a documented cookbook of "build this custom-feeling type from these primitives" recipes for the common cases above. Ships v0.2.

---

# Internal Data Model

Every operation is immutable and append-only.

```json
{
  "op_id":          "client-7:493",
  "client_id":      "client-7",
  "client_seq":     493,
  "actor_id":       "user_42",
  "room":           "doc-1",
  "branch":         "main",
  "zone":           "shared_content",
  "schema_version": 5,
  "lamport":        18923,
  "wall_time":      1733000000,
  "kind":           "text.insert",
  "target":         { "path": ["body", "children", 7, "text"] },
  "payload":        { "index": 42, "value": "hello" }
}
```

Field roles:

- `op_id = (client_id, client_seq)` — globally unique op identity, used for idempotency
- `actor_id` — authenticated human (from token); same actor across devices = same id
- `branch` — which branch this op belongs to (default `main`)
- `zone` — derived from target Element; routing key for per-zone replication and auth
- `schema_version` — schema this op was authored under
- `lamport` — per-zone lamport timestamp (zone-scoped to avoid cross-zone activity leakage)
- `wall_time` — informational only; not used for causality
- `kind` — op type (`text.insert`, `xml.setAttr`, `xml.move`, `acl.grant`, `migrate`, ...)
- `target` — where in the doc the op applies
- `payload` — op-specific data

## Value Types in Op Payloads

```text
Value =
  | Scalar     (string, int, bool, null)
  | BlobRef    { id, size, mime_type, filename?, inline?, created_by, created_at }
  | ElementRef element_id
```

`BlobRef` is reserved from v0.1 in the wire format even though full blob implementation lands in v0.5 — see **Binary Blobs**. Adding new value types later would be a wire-format break.

---

# Client ID

Each connecting Document instance carries a `client_id` — used for op identity (`op_id = (client_id, client_seq)`), per-instance undo stacks, reconnect routing, and audit. Distinct from `actor_id` (the authenticated human, from token).

## Format and Generation

| Property | Choice |
|----------|--------|
| Format | **UUID v7** (128-bit, time-sortable, RFC 9562) |
| Generation | **client-side at first Document instance** — never server-issued |
| Wire encoding | **16 bytes binary**; rendered as 36-char hex string in logs |
| Trust | **untrusted** — actor_id (from token) is the trusted identity; forged client_id grants no impersonation |

Client-generated because CRDTs are offline-first: editing must work before the first server contact. UUID v7 because it's standard, sortable by creation time (useful for debug / audit), and has wide ecosystem support across SDK languages. Birthday collisions at 128-bit width are negligible.

## Lifetime: Per-Instance, Persisted Within the Instance

Each Document instance gets its own `client_id`. The id persists across same-instance restart (page reload on web, app process restart on native) via:

- web: `sessionStorage` (per-tab, survives page reload, lost on tab close)
- native: app-local temp/process storage (survives in-process restart)

New tab / new process = new `client_id`. No coordination across tabs.

## Why Per-Tab and Not Per-Device

Per-device (one `client_id` shared across all tabs in a browser) would require:

- coordination of `client_seq` across tabs to prevent duplicate `op_id`s
- leader election among tabs to own the counter
- BroadcastChannel or SharedWorker to message between tabs
- complex reconnect logic when the leader tab closes

Per-tab gives up the "same device = same client" abstraction in exchange for zero coordination complexity. Engine treats each tab as a distinct client of the same actor. Undo stacks per-tab feel natural; the same user can have multiple parallel editing contexts.

If an app genuinely needs shared undo across tabs (rare), it builds it on top via its own mechanism — not core's concern.

## Multi-Device, Same Actor

Each device has its own `client_id`. Ops carry both `client_id` (device/session) and `actor_id` (human). Per-device undo stacks; cross-device undo is an app-level layering if desired.

```text
Alice's laptop:  client_id = ABC..., actor_id = "user:alice"
Alice's phone:   client_id = XYZ..., actor_id = "user:alice"
```

Audit queries can group by `actor_id` (all activity by Alice) or by `client_id` (specific device).

## Renewal

`client_id` is stable forever within an instance. It regenerates only on storage wipe (clearing cookies, clearing sessionStorage, clearing app data). Old `client_id`'s ops remain valid orphan history in the op log — they don't affect convergence, just take some tombstone GC effort eventually.

No periodic rotation. No server-driven revocation.

## Wire Footprint

```text
op_id    = client_id (16 bytes) + client_seq (u64, 8 bytes) = 24 bytes per op_id
```

24 bytes per op_id reference is acceptable. Compactness preserved in CBOR / MessagePack / Cap'n Proto.

## Future v4-only Privacy Mode

UUID v7's timestamp prefix leaks the device's first-connect time. Not a real privacy concern (`wall_time` on each op is more revealing). If a deployment ever requires unlinkable IDs, a config toggle could switch generation to UUID v4 instead. Same wire format (16 bytes), no breaking change. Deferred.

## Locked Decisions

| Decision | Choice |
|----------|--------|
| Format | UUID v7 |
| Generation | client-side at first Document instance |
| Server-issued option | not supported |
| Lifetime | per-instance, persisted across same-instance restart (sessionStorage / app temp storage) |
| Multi-tab coordination | none — each tab is a distinct `client_id` |
| Multi-device | each device has its own `client_id`; same `actor_id` ties them together |
| Wire encoding | 16 bytes binary |
| Trust model | `client_id` untrusted; `actor_id` (token) is the trusted identity |
| Renewal | only on storage wipe; no rotation |
| v4 privacy-mode toggle | possible future config; not a wire-format change |

---

# Important Design Principle

## Intentions vs Internal CRDT Ops

SDKs should expose high-level editing operations.

Example:

```ts
room.text("body").insert(0, "hello")
```

NOT:

```ts
apply_crdt_delta(...)
```

The CRDT internals should remain hidden.

The server/core transforms user intentions into actual CRDT operations.

---

# Anchors and Element IDs

Every Element receives a stable CRDT identifier at creation:

```text
element_id = (client_id, client_seq)
```

Element IDs never change — survive renames, moves, structural mutations. All cross-references inside the document graph go through element IDs, never integer paths.

## Anchor Model

Anchors identify positions inside collaborative containers. Used for cursors, selections, marks, comments, and any RangedElement boundary.

```text
Anchor = {
  target: element_id           // stable ID of any Element
  sub:    SubPosition
}

SubPosition =
  | CharAnchor  { char_id, side: Before | After }   // target is Text
  | IndexAnchor { item_id, side: Before | After }   // target is List or XmlElement children
  | Whole                                            // target is Map, Register, Counter, or whole element
```

CharAnchor and IndexAnchor tie to specific CRDT char/item IDs (not integer offsets) — they survive concurrent inserts and deletes without drifting.

## RelativePosition

Anchors are exposed at the SDK level as `RelativePosition`. Editor bindings (cursors, selections) must use these instead of integer offsets:

```text
pos_to_relative(view_position) -> RelativePosition
relative_to_pos(rel)           -> view_position    // resolved against current state
```

Without RelativePosition, cursors jump on remote edits. This is a core primitive, not a per-SDK concern.

---

# Text and Unicode

What "one character" means in the `Text` primitive — the choice of identity granularity and wire encoding. Permanent. Yjs got this wrong and pays for it forever; we do not get to revisit it once shipped.

## The Choice (Locked)

| Layer | Choice |
|-------|--------|
| **CRDT identity granularity** | codepoint (Unicode scalar value) |
| **Wire encoding** | UTF-8 |
| **Internal storage** | codepoint sequence with per-codepoint `char_id = (client_id, client_seq)` |
| **Public API default unit** | grapheme cluster (via SDK Unicode helper) |
| **Codepoint-level API** | available as opt-in for advanced use |
| **Unicode version mismatch** | cosmetic only — codepoints stable, graphemes may render differently |
| **Auto-normalization (NFC / NFD / NFKC / NFKD)** | none — app responsibility |

## Why Not Other Combinations

| Option | Why rejected |
|--------|-------------|
| **Byte (UTF-8 byte) as identity** | every multi-byte char shatters; cursor mid-byte = corruption |
| **Code unit (UTF-16) as identity** | what Yjs does; cursor lands mid-emoji (surrogate pair); family/flag emoji break; documented pain Yjs cannot fix |
| **Grapheme cluster as identity** | grapheme boundaries are Unicode-version-dependent. Clients on different versions disagree about boundaries. Mathematically impossible to maintain CRDT identity across version mismatch if grapheme is the identity unit. |
| **UTF-16 on wire** | doubles bandwidth for ASCII content, carries surrogate-pair baggage |
| **UTF-32 on wire** | quadruples bandwidth for no win |

Codepoint identity + UTF-8 wire + grapheme-aware API is the only combination that preserves CRDT correctness across all clients and gives users grapheme-level UX.

## Why Codepoint Identity Works Across Unicode Versions

Codepoints are universal. Unicode 14 and Unicode 15 agree on what codepoints exist (Unicode is append-only). What differs is **grapheme cluster boundaries** — how codepoints group into user-perceived characters.

Scenario: Alice on Unicode 15, Bob on Unicode 14. Alice types a new emoji introduced in Unicode 15.

- Alice's SDK emits N codepoints with stable char_ids
- Bob's SDK receives the same N codepoints with the same char_ids
- Alice renders 1 grapheme on screen
- Bob renders multiple separate codepoints (cosmetic artifact)
- Both can edit; both converge on the same codepoint sequence
- No data corruption, no CRDT identity break

Visual quirk only. Right failure mode.

## API

Default operations operate on grapheme clusters (the unit users perceive). SDK ships with a Unicode lib per language:

- TypeScript: `Intl.Segmenter` (built-in to modern JS engines)
- Python: `grapheme` package
- Go: `golang.org/x/text/unicode/norm` + grapheme segmenter
- Rust: `unicode-segmentation` crate
- Java: `java.text.BreakIterator`

```ts
text.insert(5, "🎉")               // position 5 = 5th grapheme cluster
text.delete(5, 1)                   // delete 1 grapheme (handles surrogates, combining seqs, ZWJ correctly)
text.length()                       // grapheme cluster count
text.cursor(5)                      // anchor at start of 5th grapheme — returns CharAnchor (codepoint-level char_id)

// opt-in: codepoint-level for collation, programmatic, or advanced use
text.insertCodepoints(5, "...")
text.lengthCodepoints()
text.iterateCodepoints()
```

## Anchors

`CharAnchor { char_id, side: Before | After }` references a codepoint. Cursors render at the nearest grapheme boundary on display. Anchors never drift under concurrent edits because char_id is stable.

If an anchor lands mid-grapheme (e.g., between an emoji's component codepoints) due to a remote edit, render position snaps to the nearest grapheme boundary. The anchor stays valid at the CRDT level.

## Wire Format

- `text.insert` payload: position (codepoint offset within Text) + UTF-8 byte string + count
- Char_ids assigned per codepoint at producer side
- Receivers decode UTF-8, split into codepoints, attach incoming char_ids in order
- Long inserts (paste large content) chunked into reasonable op sizes and sent as a non-atomic batch tx

## Failure Modes (Honest)

| Scenario | What happens |
|----------|-------------|
| Mismatched Unicode versions across clients | cosmetic rendering differences only; data converges; no corruption |
| User types "café" decomposed (e + combining accent, 2 codepoints) | stored as 2 codepoints; grapheme-API backspace deletes both as 1 grapheme |
| User types "café" composed (é as single codepoint) | stored as 1 codepoint; grapheme-API backspace deletes 1 codepoint |
| App receives input mixing composed and decomposed forms | app's responsibility to normalize on input if it cares; core stores both forms as-given |
| Cursor anchored mid-grapheme by app bug | anchor still valid at CRDT level; renders at nearest grapheme boundary |
| Pasting 1 MB of text | chunked into reasonable op sizes, sent as a non-atomic batch tx; observers fire once at the batch boundary |

## What Core Does Not Ship

| Concern | Why deferred to app / editor adapter |
|---------|---------------------------------------|
| NFC / NFD / NFKC / NFKD normalization | normalization changes char_ids; app must opt in on input only if it accepts that cost |
| Locale-aware collation | not Unicode-universal; app uses its own intl lib |
| Bidi / RTL display order | rendering concern, lives in the editor adapter |
| Locale-aware case folding | not Unicode-universal |
| Word / sentence boundary detection | available via SDK helper for grapheme; locale-specific cases pushed to app |
| Auto-repair of broken ZWJ sequences | codepoints stored as-given; app filters at input if needed |

Editor adapters (`sync-prosemirror`, `sync-codemirror`, etc.) handle their target editor's idiosyncrasies. Core stays Unicode-neutral beyond codepoint identity + grapheme helpers.

## Roadmap

| Capability | Milestone |
|-----------|-----------|
| Codepoint-level char_id, UTF-8 wire, grapheme-aware insert / delete / length / cursor helpers in SDK | v0.1 |
| Bundled Unicode-segmentation lib in each SDK | v0.1 |
| Grapheme-aware behavior extended to RangedElement + Marks (e.g., mark anchored at grapheme boundary) | v0.5 (with rich text) |
| Locale-aware operations (collation, case folding) | not in scope — app concern |

---

# Marks (Rich Text Formatting)

Marks are range overlays on Text — bold, italic, links, highlights, comments. Implemented as a convention over `RangedElement`, not as a separate primitive.

## Open-Ended

Core does not predefine mark names. Marks are `(name: string, value?: Element)`. The app decides what marks exist and how to render them.

```ts
text.mark(start, end, "bold")
text.mark(start, end, "link", { href: "https://..." })
text.mark(start, end, "comment", { thread_id: "t-42" })
text.unmark(start, end, "bold")
```

## Mark Registration

Each mark name needs declared merge semantics. The app registers at doc setup:

```ts
doc.registerMark("bold",    { kind: "boolean" })
doc.registerMark("link",    { kind: "value", merge: "lww", growRight: false })
doc.registerMark("comment", { kind: "object" })
```

Unregistered marks default to `{ kind: "value", merge: "lww", growLeft: true, growRight: true }`.

## Merge Flavors

| kind | example | concurrent behavior |
|------|---------|---------------------|
| boolean | bold | presence only. add+add = present. add+remove = LWW. |
| value | link={href} | single value, LWW on conflict. |
| object | comment={id} | each mark independent — no range merging across instances. |

## Anchor Expansion

Per-mark flags control whether the mark grows when text is inserted at its boundary:

```text
growLeft  : bool   // expand to cover new chars before start
growRight : bool   // expand to cover new chars after end
```

Bold: typically grows both ways (typing at the end of a bold word stays bold). Link: typically grows neither (don't extend the URL).

## Algorithm

Peritext-style range CRDT (Litt, van Hardenberg, Kleppmann — Ink & Switch 2022). Marks stored separately from Text content as a `RangedElement` log per mark name.

---

# Map Slot Safety

`Map.set(key, value)` uses LWW. For scalar values this is fine. For child CRDTs, the convergence guarantee comes from **deterministic element_id derivation**, not from API guardrails.

## The Problem (and Solution)

```text
Alice: map.text("body")   @ t=10        // SDK derives id = v5(map_id, "body")
Bob:   map.text("body")   @ t=12        // SDK derives same id (concurrent)
→ Both clients compute the SAME element_id from (parent_id, "body").
  Both Set ops carry the same Element value.
  LWW picks one wire op as the winner; the value is identical either way.
  No orphan, no divergence. Both Alice and Bob edit the same Text.
```

## SDK Surface (illustrative; final naming lives in each SDK)

```ocaml
Map.set    : Map.t -> key:string -> value:Value.t -> unit
Map.delete : Map.t -> key:string -> unit
Map.text   : Map.t -> key:string -> Text.t        (* getOrCreate; deterministic id *)
Map.map    : Map.t -> key:string -> Map.t         (* nested map; same idea *)
Map.list   : Map.t -> key:string -> List.t
Map.live   : Map.t -> key:string -> live_handle   (* reactive ref for editor bindings *)
```

Wire-level ops are minimal: just `Set { key; value }` and `Delete { key }`. The
ergonomic getOrCreate surfaces (`text`, `get_map`, `get_list`) are SDK sugar
that derives the child element_id from `(parent_id, key)` and emits a `Set`
op with that derived id as the value. Two clients calling the same SDK helper
on the same (parent, key) compute the same id and converge by construction.

Standalone CRDT construction (a la `new Text()` in Yjs) is intentionally not
supported in v0.1: elements must be created at their final location so the
deterministic id has a parent. Removes the "type not yet integrated" footgun.

Editor bindings hold `Map.live key` references, not direct CRDT references. When LWW swaps a value, the live ref re-binds, observers fire, the view re-attaches.

## Orphan Event

If a `Set` or `Delete` displaces an Element ref (e.g. set("body", "scalar") on a slot that previously held a Text), the displaced element_id may become unreachable. Core surfaces this:

```ocaml
Doc.on_orphan : Doc.t -> (Element_id.t -> unit) -> unit
```

Orphaning is never silent. Note: with deterministic ids and SDK getOrCreate, concurrent same-key initialization no longer causes orphans — only deliberate `Set`/`Delete` does.

---

# Algorithms and Invariants

## Causality

```text
op identity:    (client_id, client_seq)
total order:    per-zone lamport timestamp + client_id tiebreak
client order:   client_seq monotonic per client (FIFO)
```

Lamport = `max(seen_ts_in_zone) + 1`. Used by tree moves, mark merging, Register LWW. Wall clocks are not trusted.

### Dependency Model: Lamport + Implicit (No Explicit Deps List)

Ops carry only the lamport timestamp on the wire. Causal dependencies are **implicit through payload refs** — each op references the `char_id`s or `element_id`s it operates on, and those refs ARE the dependencies.

Receivers buffer ops whose payload refs point to ids not yet seen; apply when the referenced ids arrive. Apply within a zone in lamport order.

Rejected: explicit per-op dependency lists (Automerge-style hashes / op_id sets).

| Approach | Why |
|----------|-----|
| **Lamport + implicit (chosen)** | Smaller wire bytes per op (no deps list); simpler protocol; lookup by referenced char_id is natural; track record at Yjs scale. |
| Explicit deps list | Deps would duplicate refs already in the payload; larger ops; protocol overhead with no engine-level benefit. |
| Vector clocks (O(n_actors) per op) | Lets engine distinguish concurrent vs causal precisely. CRDT primitives merge correctly regardless, so the distinction is not needed at engine level. |

Receivers do buffer out-of-order ops, but the buffering machinery looks up by referenced id — no separate `deps` field on the wire.

Both approaches buffer when prerequisites are missing. The difference is **how prerequisites are identified**: lamport-only reads the payload's existing refs; explicit-deps adds a redundant field. We pick lamport-only.

## Tree Moves (XmlElement)

Implements Kleppmann 2021 ("A highly-available move operation for replicated trees"):

- every move op carries a lamport timestamp
- ops applied in timestamp order
- on out-of-order receive: undo later ops, insert new op, replay
- maintains a bounded undo log sized to the concurrent op window

Guarantees: exactly one parent per node, no cycles, no duplication, deterministic convergence.

## Marks

Peritext-style range CRDT. Anchors tied to char IDs (not offsets). Range merging per mark `kind` (boolean / value / object). Anchor expansion via `growLeft` / `growRight` flags.

## LWW

Used by:

- `Register` values
- `Map.set` of scalar values
- `XmlElement` attribute values
- mark values of `kind: value`

Resolution: higher lamport timestamp wins, tiebreak by `client_id`.

## Tombstone GC

CRDT text/list deletions leave tombstones (required to position concurrent inserts). Without GC, document size grows unbounded.

```text
watermark = min(last_seen_seq) across all known clients
at snapshot boundary: discard tombstones older than watermark
```

Offline clients block GC for ops they haven't acknowledged. Server tracks per-client `last_seen_seq` (already required for reconnect resume).

## Op Batching

Wire format supports run-length encoding for consecutive same-client inserts from v0.1, even if the v0.1 encoder ships single-op only. Locking the format early avoids breaking changes later.

## Schema and Repair

Schema enforcement and invariant repair are first-class core concerns. See the dedicated **Schema**, **Invariant Repair**, and **Schema Migration** sections below.

---

# Schema

The Document carries an optional declarative schema. Schema-less docs work (free-form Map / List / Text), but apps that ship versioned releases over time should declare a schema.

## Why Declare

- producer-side op validation catches bugs at the write site (invalid op never enters the log)
- type-aware SDK API (autocomplete on `xml.insertChild(...)`, attr setters narrow by type)
- enables deterministic invariant repair under concurrent merges
- enables schema migration with full history preservation
- cross-language: schema is JSON, every SDK enforces identically

## Declaration

Schema is data, JSON-serializable. Declared once at doc setup, committed alongside app code:

```json
{
  "version": 5,
  "types": {
    "doc":       { "kind": "xml", "children": ["block+"] },
    "block":     { "anyOf": ["paragraph", "heading", "list"] },
    "paragraph": {
      "kind": "xml",
      "tag": "p",
      "attrs": { "align": { "type": "lww-enum", "values": ["left","center","right"], "default": "left" } },
      "children": ["inline*"]
    },
    "heading": {
      "kind": "xml",
      "tag": "h",
      "attrs": { "level": { "type": "lww-int", "range": [1,6] } },
      "children": ["text"],
      "exclusive": true
    },
    "inline": { "anyOf": ["text"] },
    "text":   { "kind": "text", "marks": ["bold", "italic", "link"] }
  },
  "marks": {
    "bold":   { "kind": "boolean" },
    "italic": { "kind": "boolean" },
    "link":   { "kind": "value", "value": { "href": "url" }, "growRight": false }
  }
}
```

## Enforcement Points

| Where | Behavior |
|-------|----------|
| Producer SDK | Rejects an op that violates schema before sending. App bugs caught at the write site. Convergence preserved (invalid ops never enter the log). |
| Server ingress | Validates incoming ops against current schema, rejects invalid. Defense in depth. |
| Apply boundary (every replica) | Validates merged state after each apply. Invariant violations trigger Invariant Repair (next section). |

## What Schema Predefines vs Not

Predefined by core: validation engine, mark kinds (boolean / value / object), attr types (`lww-string`, `lww-int`, `lww-enum`, `url`, ...), repair rules.

Declared by app: type names, mark names, attr keys, allowed children, default values, exclusivity, anchor expansion per mark, the chosen default block type for repair.

## Schema-less Mode

No schema declared → no validation, no schema-driven repair, no migration story. Core invariants (Kleppmann moves, Map Slot Safety, anchor stability) still hold. Fine for ephemeral / free-form data. Wrong for any app that ships versioned releases over time.

## Versioning

Every schema declares a `version`. Every Document records the `schema_version` it was created under, bumped after each migration. Versioning is mandatory once a schema is declared — required for safe evolution.

---

# Invariant Repair

Concurrent merges can produce schema-invalid states even when each individual op is valid. Example: schema says "at most one heading per block." Alice and Bob each concurrently insert a heading. Both ops valid individually. Merged state has two headings — schema violation.

## Opinionated, Not Configurable

Core ships fixed repair rules. Apps don't pick. Same rationale as why CRDT merge behavior isn't configurable: configurable repair = configurable footguns + cross-language divergence + decision fatigue.

## Repair Rules (Normative)

| Violation | Fixed rule |
|-----------|-----------|
| Orphan inline (inline outside its required block) | Wrap in schema's declared default block for that scope |
| Disallowed child (child kind not in parent's allowed set) | Drop child |
| Exclusive-child collision (>1 child where schema allows ≤1) | Keep lamport-oldest, demote rest to default sibling type |
| Out-of-range scalar (e.g., heading level 7 when max is 6) | Clamp to nearest valid value |
| Disallowed attr | Drop attr |
| Attr type mismatch | Drop attr |
| Mark on type that disallows marks | Drop mark |
| Tree move cycle | Handled by Kleppmann move algorithm — not a separate repair |
| Type mismatch on Map slot | Handled by Map Slot Safety — not a separate repair |

Each rule is a deterministic function of (current state, schema, lamport order). All replicas independently converge to the same repaired state.

## Observation, Not Override

Apps cannot change what repair does. Apps can observe that it happened:

```ts
doc.on("repaired", (details) => {
  // details: { violation_kind, location, original_op, resulting_state }
  // UX uses: "we resolved a concurrent edit", offer undo, log, audit
})
```

There is no `unrepairable` event. The rule table above is provably complete (see Closure of Violation Set).

## Closure of Violation Set

The schema language has finite dimensions: type membership, children cardinality, attr presence, attr type, attr value range, mark allowance, mark value shape. Every violation maps to one dimension. Every dimension has a rule.

To keep the table complete, schema declarations are validated at parse time. The following are declaration errors caught at build time, not runtime violations:

- required attr without declared default
- exclusive container without declared demotion target
- container allowing inline without declared default wrap block
- mark value type without declared malformed-value rule (URL → drop, enum → drop, int → clamp, etc.)

Apps cannot write a schema that admits unrepairable runtime states.

## Out of Scope: Semantic Invariants

Core schema covers **structural** invariants: shape, type, cardinality. It does not cover **semantic** invariants:

- uniqueness ("list items must be unique IDs")
- cross-field relations ("end_date ≥ start_date")
- aggregate constraints ("sum of child counts ≤ total")
- reference integrity ("foreign key must resolve")

These are not in scope because they are not CRDT-mergeable with deterministic repair. Two users can each produce a unique-individually but duplicate-together state, and no fixed rule recovers a "correct" choice without losing intent.

Apps handle semantic invariants in the app layer:

- producer-side: app refuses to send an op that would violate locally (best-effort, doesn't prevent concurrent violation)
- reactive: app observes doc state, surfaces warnings to the user ("duplicate ID detected — please rename"), prompts the user to fix
- derived: aggregates computed by app code; failures surface in UI

This boundary is explicit: structure = core, semantics = app.

## Acknowledged Risk

Opinions are hard to change later. Mitigations:

- Each rule documented as normative spec — apps know what to expect
- Schema-driven defaults provide the one knob that matters (which block wraps an orphan inline, etc.)
- New violation kinds get new opinions in schema-version bumps, not retroactively
- `repaired` event gives apps a UX-level escape valve without changing core behavior

---

# Schema Migration

When schema version changes between app releases, existing documents must be transformed. Migrations live in the core (same logic as CRDT merge — one implementation, deterministic, cross-language).

## Migrations as Log Entries

The op log is append-only forever, including migration entries:

```text
log: [
  op@v1,
  op@v1,
  op@v1,
  ▶ migrate(v1 → v2) ◀     // first-class log entry
  op@v2,
  op@v2,
  ▶ migrate(v2 → v3) ◀
  op@v3,
  ...
]
```

Every op carries its creation `schema_version`. Migration entries are checkpoints. Replay walks entries in order — user ops apply under their schema, migration entries transform state at their position.

This preserves time-travel debugging, audit (`when did body become content?`), and rollback. Snapshots cache state at intervals to keep steady-state replay fast — migration cost is paid once when a snapshot crosses a migration boundary.

## Generated, Not Hand-Written

Schema is the source of truth. Migrations are derived artifacts. Same model as Prisma / Atlas / Rails / EF Core.

```text
1. App dev edits schema.json
2. `crdtsync migrate generate` diffs new schema vs last applied schema
3. Tool emits migration file: migrations/0005_<name>.json
4. App dev reviews; edits custom transforms if generated migration needs them
5. Both schema + migration committed to repo
6. CI runs `crdtsync migrate check` + `crdtsync migrate verify` — PR fails on drift or invalid output
7. Deploy: server validates migration on load; per-replica safety net at apply time
```

## Auto-Generation Coverage

| Schema change | Generated migration | Confidence |
|---------------|---------------------|------------|
| New field with default | `addField` | 100% |
| Field removed | `removeKey` | 100% |
| Attr added with default | `setAttr` | 100% |
| Attr removed | `removeAttr` | 100% |
| Mark added | `addMark` | 100% |
| Mark removed | `removeMark` | 100% |
| Type's allowed children expanded | (no migration needed) | 100% |
| Field renamed (with `@renamedFrom` annotation) | `renameKey` | 100% |
| Type renamed (with annotation) | structural rename | 100% |
| Wrap (child X now inside new Y) | `wrap` | 90% (heuristic-detectable) |
| Field split / type change | scaffolded TODO + pattern-rewrite skeleton | manual |

~70-80% of real-world schema changes auto-generate cleanly. The rest get a scaffolded skeleton; CLI exits with a warning at `migrate generate` until the app dev completes the custom step.

## Migration File Format

JSON. Storage and edit format both. Cross-language by construction, diff-friendly in PR review, no parser to maintain, LSP/JSON-Schema tooling for free.

```json
{
  "version": 5,
  "name": "split_user_name_and_add_created_at",
  "from": 4,
  "to": 5,
  "steps": [
    { "kind": "splitField",
      "at": "users.*",
      "from_key": "name",
      "to_keys": ["first", "last"],
      "by": { "split_string": " ", "first_n": 1, "rest_to": "last" } },
    { "kind": "addField",
      "at": "doc",
      "key": "created_at",
      "default": "epoch:now-at-migration-time" }
  ]
}
```

Two-tier expressiveness:

| Tier | What | Use |
|------|------|-----|
| 1. Built-in `kind` steps | `renameKey`, `removeKey`, `addField`, `wrap`, `unwrap`, `setAttr`, `removeAttr`, `addMark`, `removeMark`, `mapValues`, ... | ~80% of migrations |
| 2. Pattern-rewrite DSL | Small pure language: selectors + transforms, no I/O, no clocks, terminates | Custom tree rewrites tier 1 can't express |

WASM escape hatch (tier 3) deferred to v0.7+. Only added if real demand surfaces.

## Schema Annotations as Diff Hints

Transforms the differ can't infer from shape alone are declared next to the field via annotations:

```json
{
  "type": "User",
  "fields": {
    "first": { "kind": "text", "@renamedFrom": "name", "@derive": "split:0" },
    "last":  { "kind": "text", "@derivedFrom": "name", "@derive": "split:1.." }
  }
}
```

Differ reads annotations, generates the corresponding transform. App dev declares intent on the field, never writes DSL by hand for the common rename / derive cases.

## Opinionated Choices (No Config)

| Concern | Choice | Why |
|---------|--------|-----|
| Replay model | append-only log with migration entries | Preserves history; consistent with rest of system |
| Snapshot cache | per-snapshot, includes `schema_version` | Steady-state replay stays fast |
| Log compaction | optional admin action at migration boundaries | Storage knob when needed, default = preserve everything |
| Sync policy | derived from migration kind, not declared | `renameKey` / `addField` / `removeKey-with-default` → bidirectional; `wrap` / `split` / custom DSL → forward-only |
| Determinism | enforced by core (built-ins pure, DSL sandboxed) | Convergence by construction, not by app discipline |
| Custom logic | DSL only, no app hooks | Hooks reintroduce per-language divergence |

## Mixed-Version Sync

Server checks client `schema_version` on handshake:

- Gap covered entirely by bidirectional migrations → server translates ops in flight transparently. Old client keeps working.
- Gap includes any forward-only migration → server rejects with `please-update-app`. Client must upgrade.

No separate "breakpoint" policy. Forward-only is the breakpoint.

## Detection — Four Gates

### 1. Drift detection — `crdtsync migrate check`

Compares declared `schema.json` against the cumulative effect of applying all migration files. If they don't match → drift. CI gate. Catches both directions: schema-edited-without-migration AND migration-edited-without-schema-update.

```text
$ crdtsync migrate check
✗ Drift detected:
  schema.json declares field 'doc.subtitle' (Text)
  but migrations only end at schema v5, which doesn't have 'subtitle'

  Run `crdtsync migrate generate` to create the missing migration.
```

### 2. Verification — `crdtsync migrate verify`

Applies the migration to a synthetic fixture (or app-provided fixture), validates the result against the new schema. Property-based variant `crdtsync migrate test --samples N` generates N random docs valid under the old schema, applies migration, validates all against new. CI gate.

### 3. Server boot — chain completeness + immutability

Migration files are immutable once applied. Each gets a SHA-256 hash recorded on first apply, stored in a doc-local migrations lock:

```text
.migrations.lock
  v1: sha256:abc...
  v2: sha256:def...
  v3: sha256:hij...
```

Server refuses to start if any migration in the chain is missing, out of sequence, or has a hash mismatch (someone modified a previously-shipped migration).

### 4. Per-doc runtime — version reachability

When a doc with `schema_version=N` loads against a server on `schema_version=M`:

- If `M > N` and chain N→M exists with current migration files → apply chain.
- If chain incomplete → reject doc load with explicit error, don't corrupt.

Same check for incoming ops: op tagged version X arrives → server checks chain X→current. Missing → reject op with `migration-gap` error sent back to the client.

## Tooling Surface

```text
crdtsync migrate status              show declared schema version, last migration, drift y/n, hash status
crdtsync migrate check               exit non-zero on drift, gap, or hash mismatch
crdtsync migrate verify [--fixture]  apply pending migration to fixture, validate against new schema
crdtsync migrate test [--samples N]  property-based: random valid-under-old docs, apply, validate all
crdtsync migrate generate            diff schema → emit migration file (refuses if pre-existing drift)
crdtsync migrate apply               run migrations (includes verify pre-flight)
```

Standard Prisma surface, adapted to the doc/CRDT model. Familiar muscle memory.

## What Migrations Cannot Do

- no I/O, no wall-clock, no random, no network
- no reaching outside doc state
- no interaction with the user

These constraints are non-negotiable — determinism is the entire reason migrations live in the core. If the app needs user input to resolve an ambiguous transform, run the migration with a safe default, then surface a follow-up edit task in the UI; user-driven edits flow through the normal op stream after migration completes.

## Detection Limits (Be Honest)

- **Intent violations** — user's migration produces schema-valid state that doesn't match their intent. System sees consistent state, can't read minds. App-level unit tests catch this.
- **Semantically wrong custom transforms** — DSL program produces valid types but wrong values (e.g., split heuristic at wrong index). App-level fixture tests catch this.
- Structural correctness = detectable. Semantic correctness = not. Acceptable line.

---

# Transactions

A group of ops sent together as one wire message, batched into one local observer fire, and treated as one undo entry. Optionally made atomic across replicas via opt-in.

## API

```ts
// default: non-atomic batching (CRDT-correct default, streaming UX)
doc.transact(() => {
  text.insert(0, "hello")
  text.insert(5, " world")
})

// opt-in: cross-replica atomicity for the cases that need it
doc.transact(() => {
  acl.grant({ subject: "user:bob", action: "write", resource: elem.id })
  elem.setAttr("owner", "user:bob")
}, { atomic: true })

// named intention for undo
doc.transact(() => { ... }, { intention: "rename-section" })

// combine
doc.transact(() => { ... }, { atomic: true, intention: "grant-and-set-owner" })
```

Single API. Two effects: batching (always) + atomicity (opt-in).

## Default: Non-Atomic

Most ops should be independent and stream as they arrive. Typing should appear character-by-character on remote screens, not all-at-once when the typist pauses.

What non-atomic batching guarantees:

| Guarantee | Always |
|-----------|--------|
| Client-side observer fires once for the whole batch | yes |
| Network sent as one wire message | yes |
| Undo treats the batch as one intention | yes |
| Server-side: applies batch as one atomic log write | yes |

What it does NOT guarantee:

| Not guaranteed | Why |
|----------------|-----|
| Other replicas see ops "together" | Each op merges independently on arrival (CRDT semantics) |
| Cross-replica view boundary | Other clients can render mid-batch state |
| Schema invariant preservation across batch | Invariant repair handles partial-state cases deterministically |

This is the CRDT default. Independent merge, eventual convergence, streaming UX.

## Opt-In: `atomic: true`

For the cases where intermediate state is genuinely unsafe:

| Use case | Why atomic |
|----------|-----------|
| Privilege grant + use of new permission | Race window between op 1 and op 2 = security gap |
| Delete entity + remove all refs to it | Refs become dangling if delete lands first |
| Multi-element invariant schema cannot repair | Both ops together avoid invalid intermediate state |

What `atomic: true` adds:

- ops carry `tx_id` and a `tx.commit` marker
- receivers buffer member ops by `tx_id` until commit marker arrives
- on commit marker: all buffered ops apply atomically to the local view
- between member-receive and commit-receive, local state shows pre-tx
- view-layer atomic transition across replicas

Cost: latency (commit marker must arrive), buffering complexity, partial-tx timeout (default 30s) + abort handling.

## Why Atomic Is NOT the Default

Atomic-by-default would wreck streaming UX:

| Operation | Non-atomic default | Atomic-by-default would |
|-----------|--------------------|--------------------------|
| typing "Hello world!" | streams char-by-char on remote screens | pops in all at once when typist pauses |
| moving paragraphs | each move visible as it happens | hidden until "all moves done" marker |
| cursor moves | fire-and-forget, instant | buffered, never feels live |
| typing indicator on/off | instant | lagged by RTT |

CRDTs exist specifically to **avoid** coordination. Atomic-by-default reintroduces it for every op, costing latency on the 95% of ops that do not need it. Atomic is the deliberate override for the 5% that do.

## Scope Constraints (Producer-Side Enforced)

| Constraint | Why |
|------------|-----|
| Tx must stay within one branch | Branches are independent timelines |
| Tx must stay within one zone | Per-zone lamport clocks make cross-zone ordering ill-defined |
| Tx must stay within one schema version | Migration entries are boundaries; tx cannot straddle |
| Tx cannot include migration ops | Migrations are special log entries, applied alone |
| Atomic tx member-op count capped | Default 1000; prevents pathological buffering |

Producer SDK rejects out-of-scope txs at the write site.

## Interaction with Invariant Repair

For atomic txs: repair runs **inside the commit pipeline**, not after. The visible effect of a tx is the repaired state. Apps see one atomic event:

```text
tx ops applied → repair fires deterministically → committed state visible
```

No two-step "tx done + then repair changed it" surprise. The repaired state IS the transaction's outcome.

For non-atomic batches: repair runs per-op as usual, independent of the batch boundary.

## Interaction with Undo

A transaction is naturally an undo intention. Same construct:

```ts
doc.transact(() => { ... }, { intention: "rename-section" })
// → goes into undo stack as one intention labelled "rename-section"
// → ctrl-z undoes the entire batch as one step
```

Undo of an atomic tx = generate inverse ops for all members, wrap in a new atomic tx, apply atomically. Atomicity is preserved through undo / redo.

## Wire Envelope

Op envelope reserves two fields from v0.1:

```text
{
  ...existing fields...
  tx_id:   string?              // null = standalone op
  tx_role: "member" | "commit"  // present iff tx_id present
}
```

| `tx_id` | `tx_role` | Meaning |
|---------|-----------|---------|
| null | — | standalone op, applied immediately |
| T | "member" | part of tx T |
| T | "commit" | commit marker for tx T |

The `atomic` flag is encoded in the commit marker, not in member ops. Receivers know whether to buffer based on the commit-marker's atomic flag:

- Non-atomic batch: member ops apply immediately on receive; commit marker exists for undo grouping + close-the-batch signal.
- Atomic tx: member ops buffered; commit marker triggers atomic apply of all buffered members.

## Partial-Tx Failure (Atomic Only)

If commit marker never arrives within timeout (default 30s):

- receiver discards buffered partial tx
- originator notified to retry the entire tx
- no partial state is ever applied

Causes:
- network partition between member ops and commit marker
- crash of the originating client before sending commit
- intentional abort by originator (`doc.transact.abort()`)

## What Is NOT Shipped

- **Strong consensus / 2PC across replicas** — defeats CRDT coordination-free property
- **Compare-and-swap / conditional ops** — break CRDT mergeability. Deferred to v0.7+ if real demand surfaces
- **Cross-branch / cross-zone / cross-schema-version transactions** — would require distributed coordination
- **Long-running transactions** — txs are short (default 30s atomic timeout). Long-running app workflows are app state, not engine txs

## Locked Decisions

| Decision | Choice |
|----------|--------|
| Adopt transactions | Yes, single API |
| Default semantics | Non-atomic batching (CRDT-correct, streaming UX) |
| Opt-in semantics | `atomic: true` for cross-replica view-boundary atomicity |
| Wire reservation | `tx_id` + `tx_role` in op envelope from v0.1 |
| Atomic implementation | Member-op buffering at receivers, triggered by commit marker |
| Scope | single branch, single zone, single schema version |
| Repair (atomic txs) | runs inside commit pipeline, atomic with tx |
| Undo intention | same construct as tx |
| Strong consensus | not shipped |
| CAS / conditional ops | not shipped; deferred to v0.7+ |
| Atomic tx timeout | 30s default, tunable |
| Atomic tx member cap | 1000 ops default |

---

# Undo / Redo

Per-user undo via SDK helper. Core sees only inverse ops — no server-side undo state, no special wire format.

## Per-User Model

Each user's undo stack contains intentions (op groups) the user authored. Undo reverts only that user's ops, even when other users' ops are interleaved. Per-op identity (`client_id, client_seq`) makes targeting precise; other users' ops are unaffected.

Global undo (revert any op regardless of author) is not supported in core — it produces broken UX in collaborative settings. Apps that want "revert someone else's change" build it as a deliberate edit feature, not as undo.

## Inverse Ops

Undo emits an inverse op into the normal op stream. The inverse op replicates, persists, and merges like any other op.

| Forward op | Inverse |
|------------|---------|
| `text.insert(i, "abc")` | `text.remove` of inserted chars by anchor id |
| `text.remove(i, n)` | re-insert of removed chars at their original anchors |
| `map.set(k, v_new)` | `map.set(k, v_prev)` — requires capturing `v_prev` |
| `map.delete(k)` | re-create with captured prior value |
| `xml.insertChild(p, i, c)` | `xml.removeChild(p, c.id)` |
| `xml.removeChild(p, c)` | re-insert subtree (captured pre-state) |
| `xml.move(c, new_p, i)` | `xml.move(c, prev_p, prev_i)` |
| `xml.setAttr(k, v_new)` | `xml.setAttr(k, v_prev)` |
| `text.mark(s, e, name)` | `text.unmark(s, e, name)` + restore overlapping marks |
| `text.unmark(s, e, name)` | `text.mark(s, e, name, v_prev)` |
| `counter.increment(n)` | `counter.decrement(n)` |
| `ranged.create(...)` | `ranged.remove(id)` |

Ops that overwrite or delete state require prior-state capture at op creation time. SDK captures into the local undo stack alongside the op id.

## Intention Grouping

Undo is per-intention, not per-op. SDK groups ops into intentions:

```ts
doc.beginIntention("type-word")
text.insert(5, "h")
text.insert(6, "i")
doc.endIntention()
// undo() reverts both inserts as one atomic intention
```

Auto-grouping on debounced gaps (>500ms idle = boundary by default). Manual `beginIntention` / `endIntention` for explicit grouping (paste, structured edit, paragraph break).

## Redo

Standard pattern: undo pushes the now-undone intention onto a redo stack. Any new user op clears the redo stack — diverging from the undone path retires it. Identical to every text editor.

## Local Stack

Undo state lives in the SDK on the client, not the server. The stack persists in local storage so reload doesn't lose it. Offline editing produces undoable ops without network.

## Interaction with Invariant Repair

User op triggers repair (e.g., demotes a duplicate heading). User undoes the op. Inverse op emits, state reverts, repair re-evaluates against the new state. If state is now valid, no repair fires. If still invalid, repair fires deterministically.

Undo does not undo the repair specifically — it undoes the user's op. Repair re-runs naturally.

## Interaction with Migrations

Schema migration is a major event. Undo stack drops at migration boundaries. Users do not expect undo to cross schema versions; matches the convention of every other versioned system.

## Where It Lives

SDK, not core. Wire format unchanged — inverses are normal ops.

| Capability | Milestone |
|-----------|-----------|
| Basic UndoManager for Map / List / Text / Register / Counter | v0.2 |
| XmlElement (including move), RangedElement, marks | v0.5 |

---

# Persistence Architecture

## Main Goal

Persistence should require zero external infrastructure.

---

## Initial Storage Engine

Recommendation:

```text
SQLite + append-only operation log
```

Advantages:

- easy deployment
- mature
- reliable
- inspectable
- backup-friendly
- WAL support
- transactional

---

## Storage Layout

### Tables

```text
rooms
snapshots
operations
clients
cluster_membership
```

---

## Operations Table

```text
room_id
server_seq
client_id
client_seq
op_payload
created_at
```

---

## Snapshot Table

```text
room_id
snapshot_blob
last_seq
created_at
```

---

# Snapshots

A snapshot is a serialized materialized Document state at a specific lamport timestamp. Snapshots make replay fast, drive tombstone garbage collection, mark migration checkpoints, and back the user-facing versioning layer.

## Envelope

```text
SnapshotEnvelope {
  room_id:        string
  branch:         string         // default "main"
  schema_version: int
  lamport:        int            // all ops with lamport ≤ this are included
  produced_at:    timestamp
  format_version: int
  body:           bytes          // CBOR / MessagePack / Cap'n Proto encoded Document state
  body_hash:      sha256
}
```

Includes full Element tree with stable element IDs preserved, all CRDT internal state (char IDs, anchor indexes, retained tombstones), and `schema_version`. Identical binary format to the on-wire serialization.

## Frequency Triggers

| Trigger | Default |
|---------|---------|
| Op count since last snapshot | every 10,000 ops |
| Time since last snapshot | every 1 hour |
| Migration boundary | always, immediately after applying a migration entry |
| Manual | admin / app API: `doc.snapshot()` |

All thresholds tunable per room.

## Retention Policy

| Snapshot kind | Default retention |
|---------------|------------------|
| Latest per branch | always retained |
| Migration-boundary snapshots | retained forever (or until explicit log compaction at that boundary) |
| Periodic snapshots between migrations | rolling window, default keep last 3 |
| Named versions (see Versioning and Branches) | retained until app deletes |

Migration-boundary snapshots are sticky because they are the only way to fast-replay across a migration. Removing them forces full history replay through the migration step.

## Tombstone GC

Snapshots are when GC actually happens. Until a snapshot crosses the watermark, tombstones must be retained — concurrent ops from offline clients could need them.

```text
watermark = min(last_seen_seq) across all known clients
at snapshot time: discard tombstones older than watermark
```

Offline clients block GC for ops they haven't acknowledged.

## Migration Interaction

When a `migrate(vN → vN+1)` entry hits the log, every replica:

1. Applies the migration to current state in memory
2. Validates the result against the new schema
3. Persists a new snapshot tagged `schema_version = N+1, lamport = entry.lamport`
4. New snapshot becomes the latest for the branch

Future replays of post-migration ops start from this snapshot; the pre-migration ops + migration entry remain in the log for time-travel + audit.

## Time Travel

```text
1. Find nearest snapshot S with S.lamport ≤ target T
2. Load S
3. Replay log entries from S.lamport to T
4. Materialized state at T
```

If only the latest snapshot is retained, time-travel before that point requires full-history replay (slow but possible).

Migration-boundary snapshots make cross-migration time-travel cheap.

## Replication

Default: leader takes snapshots, replicates them to followers. Followers verify `body_hash` before swapping their snapshot pointer.

Alternative (per-replica snapshots): each follower computes independently. More CPU, more resilient — followers can serve cold-starts without leader involvement. Move to per-replica when perf demands; default leader-only.

## Cold Start

When a client connects to a room it has not seen before, the server sends `latest_snapshot + ops_since(snapshot.lamport)`. No full-history replay on the client.

## Export / Import

Snapshots are portable. CLI:

```bash
crdtsync snapshot export <room_id> [--branch <name>] > backup.snap
crdtsync snapshot import backup.snap [--as <new_room_id>]
```

Use cases: backup / restore, cloning rooms (templates), cross-server moves, debugging (load a customer snapshot locally to reproduce a bug). Import bumps client/server lamport clocks past the imported snapshot's lamport so subsequent ops have higher timestamps. Element IDs and client IDs are namespaced so no identity conflict on import.

## Compaction

Tombstone GC at snapshot time is the default compaction mechanism. Optional admin action `crdtsync compact --before-migration vN` can additionally truncate pre-migration ops from the log, sacrificing time-travel before that point for storage. Default behavior preserves everything.

---

# Versioning and Branches

Snapshots are the storage primitive. Versioning is the user-facing layer built on top. Apps that need named versions, restore, publish/draft workflows, per-user forks, or diff between revisions should not have to reinvent these on top of raw snapshots.

## Named Versions

Every snapshot can carry user metadata:

```ts
const v = doc.createVersion({
  name: "before_q4_refactor",
  description: "Saved before restructuring the analytics section",
  type: "manual" | "scheduled" | "publish" | "restore-audit",
  createdBy: userId,
})
```

A version is a snapshot plus an entry in a versions index. List, paginate, rename, delete are first-class API operations:

```ts
doc.listVersions({ page, pageSize, branch })
doc.updateVersion(id, { name, description })
doc.deleteVersion(id)
```

## Auto-Version Triggers

Versions can be created declaratively in response to engine events or schedules:

```ts
doc.autoVersion({
  onEvent:    ["before-publish", "after-restore"],
  onSchedule: "@daily",
  onOpCount:  5000,
})
```

App-defined events fire when the app calls `doc.event("event-name")`. Engine-defined events include `before-publish`, `after-restore`, `before-migration`, `after-migration`, `on-snapshot`.

## Branches

A branch is a named pointer into the op log. Default branch is `main`. Apps create additional branches:

```ts
const draft     = doc.branch("draft").forkFrom("main")
const published = doc.branch("published").forkFrom("main")
const userFork  = doc.branch(`user-${userId}`).forkFrom("published")
```

Each branch has:

- a stable name
- a HEAD lamport timestamp
- a fork point (the snapshot or lamport position it diverged at)

Branches share immutable history before their fork point. Storage cost = only divergent ops past the fork. Adding a new branch is cheap.

## Restore as Branch

Restore does not rewrite history or reset state vectors. It forks a new branch from a chosen snapshot and switches the active HEAD:

```text
main: [op1 op2 op3 op4 op5 op6]
                  ↑
              snapshot @ op3

doc.restoreToVersion(v3):
main:     [op1 op2 op3 op4 op5 op6]     ← old branch, preserved
restored: ───── op3 ─►                    ← new branch becomes live, future ops land here
```

Properties:

- old branch preserved as immutable history
- offline-client ops in flight against the old HEAD land on the old branch, not on the restored live state — not lost, not corrupting
- no state-vector reset, no custom clock hacks
- audit version created automatically on the old branch before the restore, so the pre-restore state is named and reachable
- restore is itself a first-class log entry with creator + timestamp

## Publish / Draft Convention

A common pattern: edit on `main`, publish a read-only snapshot for consumers.

```ts
doc.connect({ branch: "main" })                  // editor users connect here
doc.branch("published").syncFrom("main")         // publish: update published HEAD to main's current state
doc.connect({ branch: "published" })             // readers connect here
```

Republishing updates `published`'s HEAD pointer to a new snapshot of `main`. Old `published` snapshots remain reachable as versions on the published branch — apps can roll back published state independently of editor state.

## Per-User Branches

The same primitive supports per-user state forks:

```ts
const userBranch = doc.branch(`user-${userId}`).forkFrom("published")
```

Each user's edits go into their branch. Storage shared from the fork point. Sync isolated. Replication scoped per branch.

Useful when each user customizes a base template (form-builder, dashboard, notebook with per-user filters) without affecting the shared base.

## Branch-Scoped Replication

Connection establishes which branch the client edits/reads:

```text
client → connect(branch = "main")        // gets ops on main
client → connect(branch = "user-42")     // gets ops on user-42 branch
```

The `(room, branch)` tuple is the unit of replication. Replica sets shard by `(room, branch)` if needed. Cross-branch sync (e.g., `published.syncFrom("main")`) happens via internal engine operations, not normal client ops.

## Schema-Aware Diff

Because Documents are structured Element trees with declared schema (not opaque binary blobs), diffs between any two snapshots are computable as structural change lists:

```ts
const diff = doc.diff(versionA, versionB)
// returns ordered list of structural changes, e.g.:
//   { path: "doc.body.children.[3]",                kind: "added",      value: <Element> }
//   { path: "doc.body.children.[5].attrs.align",    kind: "changed",    from: "left", to: "center" }
//   { path: "doc.body.children.[2]",                kind: "removed",    was: <Element> }
//   { path: "doc.body.children.[7].text",           kind: "text-diff",  chunks: [{op:"keep", n:5}, {op:"ins", str:"foo"}, ...] }
//   { path: "doc.body.children.[7].marks.[bold:1]", kind: "mark-added", range: [12, 18] }
```

Diff is schema-aware:

- Text values produce char-level diffs (insertions / deletions / keeps)
- XmlElement subtrees produce structural diffs (added / removed / moved children)
- Attr changes show old → new values
- Marks show added / removed / range-changed
- Map / Register / Counter show value diffs

Engine ships sensible default text/structural renderers; apps can override.

## Branch Merging (Out of Scope for v0.x)

Merging two divergent branches back into one is the harder version-control problem. Snapshots + CRDT semantics make it possible (merge the two branches' op logs from their fork point), but conflict resolution UX is app-specific. Not in scope for v0.x. The primitive (fork point + HEAD pointers) is there; merge tooling can land later.

---

# Binary Blobs

Files, images, audio, video, PDFs, plot outputs — collaborative apps need to attach binary content. Treated as a separate concern from the op stream because the access patterns are fundamentally different.

## Why Blobs Are Different from Other Data

| Property | Doc ops | Blobs |
|----------|---------|-------|
| Size | bytes–KB | KB–GB |
| Mutability | edited collaboratively | immutable once created |
| Merge semantics | needed | not applicable |
| Delivery | eager (replicated to every replica) | lazy (fetched on render) |
| Storage tier | op log | content-addressable blob store |
| Bandwidth pattern | low per op, every op | high per fetch, only on demand |
| Dedup | rarely | critical (same avatar uploaded by many users) |

Inlining blobs in the op stream wrecks everything: log balloons, snapshots bloat, every replica receives bytes whether or not they render. Blobs need a parallel system designed for their access pattern.

## Architecture: Refs in Ops, Bytes in Blob Store

Op payloads carry `BlobRef` values, not raw bytes. Actual bytes live in a separate, addressable blob store and are fetched lazily on render.

```text
BlobRef {
  id:          random UUID            // public reference; never reveals content
  size:        bytes
  mime_type:   string
  filename:    string?                // user-provided original name
  created_by:  actor_id
  created_at:  lamport
  inline:      bytes?                 // present iff size ≤ inline threshold
}
```

Server-side, blobs are stored content-addressable (keyed by sha256) for dedup. The mapping `random_id → sha256` lives server-side only — **never exposed on the wire or to apps**. Same bytes uploaded twice produce two distinct `BlobRef`s with two random IDs that internally point to one stored blob.

This gives global dedup without leaking content fingerprints. Confirmation attacks (adversary with the same file checking "does the server have this?") are blocked because public IDs are unpredictable.

For ultra-paranoid mode (per-tenant HMAC-keyed hashing so even server admins cannot cross-correlate across tenants) — deferred to v0.7.

## Blob is a Value Type, Not a CRDT Primitive

Blobs do not merge, do not have substructure. They fit as values inside any container:

```ts
map.set("avatar", blobRef)
xmlElement.setAttr("image", blobRef)
list.insert(0, blobRef)
ranged.create(start, end, { kind: "attachment", file: blobRef })
```

Replacing a blob value = LWW on the assignment (same as Map Slot Safety). No "edit" semantics. To "edit" a blob, upload a new version and assign the new ref.

## Inline Threshold

Small blobs can be embedded directly in the `BlobRef` to skip the fetch roundtrip:

```text
size ≤ inline_under: BlobRef carries bytes inline; no fetch needed
size  > inline_under: BlobRef carries only metadata; client fetches on render
```

Default threshold: **4 KB**. Covers most icons, tiny avatars, thumbnails. Schema can override per-field.

## Schema Declaration

```json
{
  "types": {
    "image_block": {
      "kind": "xml",
      "tag": "img",
      "attrs": {
        "file": {
          "type":         "blob",
          "max_size":     "10MB",
          "allowed_mime": ["image/png", "image/jpeg", "image/webp", "image/gif"],
          "inline_under": "8KB"
        },
        "alt": { "type": "lww-string" }
      }
    },
    "attachment": {
      "kind": "xml",
      "attrs": { "file": { "type": "blob", "max_size": "100MB" } }
    },
    "video_clip": {
      "kind": "xml",
      "attrs": {
        "file": {
          "type":         "blob",
          "max_size":     "5GB",
          "allowed_mime": ["video/mp4", "video/webm"],
          "inline_under": "0"
        }
      }
    }
  }
}
```

Producer SDK rejects out-of-spec uploads at the write site (oversize, wrong MIME).

## Presigned URLs: The Universal Interface

All blob upload and fetch goes through presigned URLs. **The engine never proxies blob bytes through its main RPC/websocket channel.** Backend-specific implementation; uniform client/SDK interface.

### Upload

```ts
const { url, ref } = await doc.blobs.requestUpload({ size, mime_type, filename })
// 'ref' is the future BlobRef (random_id assigned upfront, not yet "available")

await fetch(url, {
  method:  "PUT",
  body:    file,
  headers: { "Content-Type": mime_type },
  onProgress,
})

await doc.blobs.confirmUpload(ref)
// server verifies completion (size match, optional checksum) and marks the blob available
// orphan refs that never get confirmed are GC'd after a short timeout
```

### Fetch

```ts
const { url } = await doc.blobs.requestFetch(ref, { range })
const bytes   = await fetch(url).then(r => r.arrayBuffer())

// or for direct browser rendering:
const src = await doc.blobs.url(ref)        // <img src={src} />
```

SDK wraps the two-step calls into `doc.blobs.upload(file)` / `doc.blobs.fetch(ref)` for ergonomic single-call usage.

### Why Presigned URLs Universally

- **Uniform API.** Apps and SDKs write one code path regardless of backend.
- **Engine bandwidth saved.** Even on local FS, bytes flow through a separate HTTP route, not the websocket. Engine main loop stays light.
- **CDN-native.** Production deployments offload reads to a CDN by issuing CDN-signed URLs.
- **Backends pluggable cleanly.** Trait surface is "issue PUT URL" + "issue GET URL," not "stream bytes through me."

### Trade-offs Acknowledged

- Engine cannot easily middleware-process bytes (compression, virus scan) without explicit middleware mode.
- Direct-to-S3 means the server does not observe the upload happening — relies on S3 event hooks or `confirmUpload` + post-upload verification.
- Local FS backend needs a co-located HTTP route + signed-token verification (more setup than "just save bytes from the websocket").

Worth it for the uniform-API + CDN-native + bandwidth-savings wins.

## Backends

```text
trait BlobBackend {
  presign_upload(blob_id, size, mime)   -> URL
  presign_fetch(blob_id, range?)        -> URL
  exists(blob_id)                       -> bool
  delete(blob_id)                       -> Result
  size(blob_id)                         -> bytes
  verify_upload(blob_id, expected_size) -> Result   // post-upload integrity
}
```

Ship two backends:

| Backend | Use case | Notes |
|---------|----------|-------|
| **Local filesystem** | single-node dev, small deployments | engine serves bytes via co-located HTTP route with signed JWT tokens |
| **S3-compatible** (S3 / R2 / B2 / MinIO) | production | real S3 presigned URLs, direct-to-S3 from clients |

Deferred backends:
- CDN tier (signed CDN URLs backed by S3) — v0.7
- IPFS backend — future, contingent on E2E story
- Embedded SQLite blob column — rarely worth it

## Authorization

Two-layer check, both server-side:

### 1. Reference-site Element Auth

Can recipient read the Element containing the `BlobRef`? If no, the entire Element (and the embedded ref) is filtered before send. Wire-level guarantee — the `BlobRef` never leaves the server for that recipient.

### 2. Blob-Fetch Auth

When a client calls `requestFetch(ref)`, server checks ACL **in the context of the reference site that delivered the ref**. ACL is evaluated per reference site, not per blob.

```text
blob X referenced from:
  - element_a in zone "team"    → fetch authorized by zone "team" + element_a's ACL
  - element_b in zone "private" → fetch authorized by zone "private" + element_b's ACL

User can read element_a but not element_b:
  → sees the ref at element_a, can fetch (auth passes via that site)
  → does not see the ref at element_b at all, cannot even try
```

No global "Alice can read blob X" tuple. Auth flows through the containing element. Same blob in two contexts has two independent ACL evaluations.

### Default Blob ACL

A new blob inherits the ACL of the Element where it was first attached. App can override per-site with explicit grant/deny on `element_id:<id>` (the reference site, not the blob).

## Dedup (Server-Side, Invisible to Clients)

Same content → same internal sha256 → stored once. Reference counting tracks how many active reference sites point at each underlying hash across all docs / branches / snapshots.

Big space savings on:

- user avatars (uploaded by hundreds of users, stored once)
- template assets
- shared brand images, logos
- replicated PDFs across docs

Dedup is transparent to clients — they always see distinct random IDs.

## Garbage Collection

When all reference sites for a blob disappear (deleted, replaced via LWW, branch pruned), the blob becomes orphan. GC sweeps periodically.

```text
sweep:
  for each blob in blob_store:
    if reference_count(blob) == 0:
      if (now - last_referenced(blob)) > grace_period:
        delete from blob_store
```

**Grace period: 30 days (default, tunable per deployment).** Protects against:

- undo / redo restoring a ref to a "deleted" blob
- restore-as-branch re-referencing old blobs
- mistaken delete recovery

Reference counting respects branches: a blob referenced from any live branch (or retained snapshot / named version) is retained. Blob referenced only from pruned op log + GC'd snapshots = eligible for delete.

Conservative GC — trades some storage for safety against accidental data loss.

## Upload Protocol

S3-compatible multipart for all backends (clients always speak the same protocol regardless of backend):

- chunked upload (typical chunk: 5–10 MB)
- resume on connection loss
- per-chunk integrity check
- progress reporting per chunk
- for S3-compatible backends: clients upload directly to S3 with no engine proxy
- for local FS: engine implements the S3 multipart protocol locally

Universal tooling for free — every language already has S3 multipart support.

## Range Requests and Streaming

```text
GET /blobs/<random_id>?token=...
GET /blobs/<random_id>?token=...    Range: bytes=0-65535
```

Critical for:

- video / audio streaming
- PDF page-by-page fetch
- large image progressive load
- partial download with resume

S3 backends support natively. Local FS backend implements via HTTP range serving.

## Snapshots

Snapshots reference blobs by id, not include bytes. Snapshot size stays manageable independent of blob count or sizes.

```bash
crdtsync snapshot export <room>                            # refs only (small file)
crdtsync snapshot export <room> --with-blobs               # bundle blob bytes too (large)

crdtsync snapshot import backup.snap                       # errors if blobs not on target
crdtsync snapshot import backup.snap --pull-blobs-from <origin>   # lazy fetch from origin
```

For cross-server migration: `--with-blobs` for one-shot, or `crdtsync blob sync <origin> <target>` to transfer blob store separately.

## Versioning + Blobs

Blobs are immutable. "Editing" = upload new version → new `BlobRef` → assign new ref (LWW):

```ts
const newRef = await doc.blobs.upload(updatedFile)
xmlElement.setAttr("image", newRef)
```

Old blob retained as long as any branch / version / snapshot references the old ref. GC frees it after the grace period beyond the last reference. Undo restores the old ref naturally.

For explicit version-tracking of a file, model it in the app with normal CRDT primitives:

```ts
const attachments = doc.getMap("attachment_versions")
attachments.set("v1", ref1)
attachments.set("v2", ref2)   // user kept v1 around explicitly
```

No engine concept of "blob versioning" needed. App composes from existing primitives.

## Wire-Format Reservation

The op envelope's `Value` type reserves a slot for `BlobRef` from v0.1, even though full implementation lands later:

```text
Value =
  | Scalar (string, int, bool, null)
  | BlobRef { id, size, mime_type, filename?, inline?, ... }
  | ElementRef element_id
```

Cheap-now / painful-later decision. The slot exists in the format from day one so adding full blob support in v0.5 does not break the wire.

## Locked Decisions

| Decision | Choice |
|----------|--------|
| Public identity | random UUID per upload; server-internal sha256 for dedup |
| Privacy | random IDs are default — content hash never leaks to wire or apps |
| Upload / fetch interface | presigned URLs, uniform across all backends |
| Engine in the bytes path | never — bytes flow through dedicated HTTP route or directly to S3 |
| Default inline threshold | 4 KB (schema can override per attr) |
| Backend default | local filesystem for single-node; S3-compatible trait for production |
| Multipart protocol | S3-compatible (universal tooling) |
| Blob ACL evaluation | per reference site (not per blob) |
| Default blob ACL | inherits from the Element where attached at creation |
| Mutable blob semantics | none — replace ref to "edit"; immutable bytes |
| GC grace period | 30 days post-last-reference (tunable) |
| Hash-as-identity ultra-privacy mode (per-tenant HMAC keys) | deferred to v0.7 |

---

# Networking Layer

## Transport

WebSocket. Bidirectional, browser-native, low latency, mature tooling. WSS over TLS in production.

## Connection / Multiplexing Model

**One WebSocket per `(server, actor session)`. Logical channels multiplexed per `(room, branch, zone)` subscription.**

| Property | Choice |
|----------|--------|
| Connections per actor session | 1 |
| Channels per connection | many — one per active `(room, branch, zone)` subscription |
| Subscribe / unsubscribe | in-band control messages, runtime-mutable |
| Server scaling | rooms sharded across replica sets; clients routed to the right replica via front-door router or HTTP redirect |
| Heartbeat | per-connection keepalive (interval config) |
| Reconnect | resume all subscriptions + `last_seen_seq` per channel |

A client editing five docs in five tabs of one app opens five connections (per-tab `client_id`); a client editing five docs in one tab opens one connection with five channels. Server tracks subscriptions per channel; ops route only to subscribers of the channel they belong to.

## Handshake

Three phases. Wire structure is fixed; the credential carrier is deployment-pluggable.

```text
Phase 1: Hello (always)
  Client → Server : ClientHello { wire_version, supported_codecs, capability_flags }
  Server → Client : ServerHello { chosen_wire_version, chosen_codec, server_caps,
                                  auth_state: "established" | "required" }

Phase 2: Auth (only if auth_state == "required")
  Client → Server : Auth        { credentials: opaque }
  Server → Client : AuthResult  { actor_id, schema_versions_supported, ... }
                  | AuthFailure { code, message }

Phase 3: Subscribe (after auth established, repeatable)
  Client → Server : Subscribe    { room, branch }
  Server → Client : SubscribeAck { channel_id, snapshot_lamport, ops_since_lamport, ... }
```

### Wire-Version Header (Format-Stable)

The very first bytes of the connection carry a **fixed-format header**: a 4-byte magic + 4-byte protocol version. This header never changes shape across protocol versions — it is the only piece guaranteed to be parseable by every client/server forever.

After the Hello exchange negotiates `chosen_codec`, all subsequent messages use that codec. New binary formats can ship in later releases without breaking older clients because Hello itself stays in a format-stable shape.

### Fast Path: Credentials Present at Transport Upgrade

Browser cookie / WS subprotocol / `Authorization` header arrives with the connection upgrade. Server validates during accept. `ServerHello.auth_state = "established"`. Client skips Phase 2 and goes straight to Subscribe. Saves one round trip.

### Fallback Path: In-Band Auth Message

No upgrade-time credentials? Server replies `auth_state = "required"`. Client sends an `Auth` message with credentials in-band. Server validates and replies `AuthResult`. Then subscriptions allowed.

### Pluggable Auth Carriers

Deployment configures which carriers the server accepts:

| Carrier | Use case |
|---------|----------|
| Cookie (HttpOnly, SameSite) | browser app served from same origin; XSS-safer, app code never touches credentials |
| Bearer in `Sec-WebSocket-Protocol` subprotocol | browser app cross-origin |
| `Authorization` header on upgrade | native apps, server-to-server |
| In-band `Auth` message post-connect | transports without header support; embed contexts; custom transports |
| Query parameter (`wss://...?token=...`) | not ideal (logs leak); supported for restricted embed contexts |
| mTLS | server-to-server / high-security |
| API key | server-to-server / scripted clients |

`Auth.credentials` is opaque bytes on the wire. The server interprets them per its configured carrier(s) and verification (JWT signature, OIDC introspection, mTLS peer cert, cookie session lookup, etc.). Clients never assert `actor_id` — the server derives it from the verified credential.

### Operations Allowed Before Auth Established

Only `ClientHello`, `ServerHello`, and `Auth` messages. Any other op before auth is established = protocol violation, connection terminated.

### Anonymous Mode

If deployment policy permits anonymous access, server emits `actor_id = "anon:<random>"` either during the upgrade fast path (no creds present, anon allowed) or in response to an explicit `Auth { credentials: <anon_token> }`. Treated as any other authenticated actor by the authorization layer.

## Binary Codec

Format choice (CBOR / MessagePack / Cap'n Proto / custom) is an **implementation decision**, not a foundational one. Negotiated via Hello. New codecs ship in later releases without breaking older clients because the Hello header is format-stable.

v0.1 likely ships with one binary format. JSON may be supported as a debug-mode codec for human-readable wire dumps.

## Error Response Envelope

Standardized error response carries a **closed enum code** + human message + opaque details. Closed enum for the same reason `op.kind` is closed: keeps the wire compact and ensures cross-language error handling stays uniform. New error codes ship through engine releases.

```text
Error {
  code:    enum   // closed namespace: auth_failed, schema_mismatch, migration_gap,
                  //                   unauthorized_op, blob_unavailable, ...
  message: string
  details: opaque
}
```

## Format / Framing Details Not Locked

| Decision | Status |
|----------|--------|
| Binary codec choice | deferred to implementation |
| Compression algorithm (per-message, per-batch) | deferred (additive flag in framing) |
| TLS profile, cipher selection | infrastructure, not protocol |
| Field tag numbering (for tagged formats) | implementation detail of chosen codec |
| Heartbeat interval default | runtime config |
| Op size limits | server-side gate, can tighten/loosen without wire impact |
| Batching strategy | optimization, semantics unchanged |

---

# Realtime Synchronization

## Connection Flow

```text
client connects
→ authenticate
→ join room
→ send last_seen_seq
→ receive missing operations
→ subscribe to live ops
```

---

## Client Reconnect

Client stores:

```json
{
  "room": "doc-1",
  "last_seen_seq": 128
}
```

Reconnect:

```text
resume from seq 128
```

---

# Idempotency

Every operation must be idempotent.

Necessary because:

- reconnects
- retries
- failovers
- duplicate packets

Example:

```text
op_id = client_id + client_seq
```

The server ignores already-seen operations.

---

# Offline-First Support

## Local Editing

Clients should support:

- local optimistic editing
- offline operation queues
- reconnect synchronization
- local snapshots

This is enabled by embedding the CRDT core locally.

---

# Shared Portable Core

## Goal

Avoid implementing the CRDT in every language.

---

# Export Strategy

## 1. WASM Export

Used for:

- browser
- Node.js
- Electron

Advantages:

- single implementation
- deterministic behavior
- easy web distribution

---

## 2. Stable C ABI

Used for:

- Python bindings
- Go bindings
- Rust bindings
- JVM native wrappers

The C ABI becomes the canonical native interface.

---

# Example C ABI

```c
crdt_doc_new
crdt_doc_free
crdt_doc_apply_update
crdt_doc_encode_update
crdt_doc_text_insert
crdt_doc_text_delete
crdt_doc_map_set
crdt_doc_observe
```

SDKs should be thin wrappers over this ABI.

---

# SDK Philosophy

SDKs should contain:

- serialization
- networking
- reconnect logic
- API ergonomics

SDKs should NOT contain:

- merge logic
- causality logic
- CRDT internals

---

# Horizontal Scaling

## Main Constraint

No Redis/Postgres dependencies.

The cluster layer must be internal.

---

# Room-Based Sharding

Each room maps to a replica set.

Example:

```text
room abc -> node A, node B, node C
```

One node becomes leader.

Others become followers.

---

# Consistent Hashing

Replica sets selected via:

```text
hash(room_id)
```

This enables:

- deterministic placement
- horizontal scaling
- balanced room distribution

---

# Leader Model

For each room:

```text
leader handles writes
followers replicate operations
```

Clients can connect to any node.

If connected to wrong node:

```text
proxy to leader
```

or redirect.

---

# Replication Flow

```text
client -> leader
leader persists locally
leader replicates to followers
followers ACK
leader ACKs client
```

---

# Durability Guarantees

Recommended policy:

```text
ACK only after majority replication
```

Example:

```text
3 replicas
2 acknowledgements required
```

Advantages:

- avoids losing acknowledged edits
- better collaborative guarantees

---

# Failover

If leader dies:

```text
followers elect new leader
clients reconnect
resume from last_seen_seq
```

---

# Client Recovery

Clients reconnect with:

```text
last_seen_seq
```

Server sends:

```text
missing operations
```

This allows seamless recovery.

---

# Cluster Discovery

Potential approaches:

## Static Join

```bash
crdtsync serve --join node-a,node-b,node-c
```

---

## Gossip-Based Discovery

Nodes exchange:

- liveness
- room ownership
- replication state
- membership

---

# Awareness

Ephemeral per-client state surfaced to other connected clients. Cursors, selections, user identity, typing indicators, viewport, mouse position, app-defined transient state.

Other libraries call this **presence** (Liveblocks, Slack, Firebase). We use **awareness** — the Yjs term, grounded in the CSCW workspace-awareness literature, more accurate (cursor positions and viewport are not "presence" in the chat-system sense). Treat the names as synonyms when reading other ecosystems' docs.

## Properties

- not durably persisted (ephemeral by design)
- not in the op log, not in snapshots, not replicated for durability
- replicates on a separate lower-latency channel from document ops
- per-entry TTL (some entries session-lifetime, others auto-expire after silence)
- per-entry throttle (server caps high-frequency entries like mouse/cursor)
- LWW per-client (each client owns its own state; no CRDT merge across clients)
- auth-filtered per recipient
- carries `actor_id` from token so receivers know which human is publishing

## Schema-Declared

Awareness entries are declared in the same schema file as content, alongside `types` and `marks`:

```json
{
  "version": 5,
  "types":  { ... },
  "marks":  { ... },
  "awareness": {
    "cursor": {
      "type":     "anchor",                     // a RelativePosition into doc content
      "ttl":      "10s",
      "throttle": "30ms",
      "@auth":    { "publish": ["role:editor"], "see": ["role:viewer", "role:editor"] }
    },
    "selection": {
      "type":     "anchor-range",
      "ttl":      "10s",
      "throttle": "50ms"
    },
    "user": {
      "type":  "object",
      "shape": { "name": "string", "color": "string", "avatar_url": "string?" },
      "ttl":   "session",                        // cleared only on disconnect
      "@auth": { "publish": ["authenticated:*"], "see": ["authenticated:*"] }
    },
    "typing": {
      "type": "boolean",
      "ttl":  "2s"                               // auto-clears after 2s without refresh
    },
    "mouse": {
      "type":     "object",
      "shape":    { "x": "int", "y": "int" },
      "ttl":      "5s",
      "throttle": "50ms"
    },
    "viewport": {
      "type":     "object",
      "shape":    { "top_anchor": "anchor", "bottom_anchor": "anchor" },
      "ttl":      "session",
      "throttle": "200ms"
    }
  }
}
```

| Field | Behavior |
|-------|----------|
| `type` | data shape — primitives, `anchor`, `anchor-range`, `object` with `shape` |
| `ttl` | auto-clear after N units of silence (`session` = clear only on disconnect) |
| `throttle` | server drops same-entry updates faster than this; SDK debounces client-side too |
| `@auth.publish` | who can publish this entry |
| `@auth.see` | who can observe this entry from others |

Schema-validated on publish — bad shape rejected at the SDK before wire.

## State Model

```text
Awareness {
  states: Map<client_id, ClientAwareness>
}

ClientAwareness {
  client_id:    string
  actor_id:     string                  // from token
  branch:       string
  connected_at: lamport
  entries:      Map<string, Entry>
}

Entry {
  value:        validated by schema
  updated_at:   wall-clock + lamport
  expires_at:   wall-clock              // from TTL
}
```

## API

```ts
// publish own state
doc.awareness.set("cursor", anchor)
doc.awareness.set("user", { name: "Alice", color: "#f0a", avatar_url: "..." })
doc.awareness.set("typing", true)
doc.awareness.delete("selection")

// observe others
doc.awareness.observe((all) => {
  for (const [clientId, state] of all) {
    if (clientId === doc.clientId) continue
    renderCursor(state.entries.cursor, state.entries.user)
  }
})

// granular events
doc.awareness.on("cursor",       (clientId, cursor) => { ... })
doc.awareness.on("user-joined",  (clientId, state)  => { ... })
doc.awareness.on("user-left",    (clientId)         => { ... })

// query
doc.awareness.get(clientId)
doc.awareness.list({ branch, filter })
doc.awareness.count()
```

## TTL Handling

Server sweeps entries:

- `ttl: "session"` entries cleared only on disconnect
- timed-TTL entries cleared on expiry; removal broadcast to subscribers
- SDK auto-refreshes high-traffic entries (cursor) on activity; lets low-traffic entries (typing) expire naturally

## Throttling (Two-Layer)

- **Client-side** SDK debounces at `throttle` interval before sending
- **Server-side** caps inbound — faster updates coalesce, keep latest only

Reduces wire chatter and downstream broadcast cost. Critical for mouse/cursor in whiteboard-style apps with many participants.

## Reconnect Grace Window

On disconnect:

- server marks state stale but does not immediately clear
- grace window (default 5s)
- if client reconnects with same `client_id` within grace: state preserved, no `user-left` fires
- if grace expires: state cleared, `user-left` broadcast

Fixes the flash-of-user-left-then-user-joined pattern on every brief reconnect.

## Anchors

Cursor / selection / viewport entries use the same `RelativePosition` model as document anchors (see Anchors and Element IDs). Survive concurrent doc edits without drifting. Editor bindings translate between view positions and `RelativePosition` for transmission.

## Auth-Aware Filtering

Awareness is not pure broadcast — server filters per recipient. Two permissions per entry govern visibility:

- `awareness.publish` — actor can publish this entry on a given branch / zone
- `awareness.see` — recipient can observe this entry from others

Server walks each entry per recipient:

- check `@auth.see` for the entry
- check anchor reachability (cursor targets element in unauthorized zone → drop entry for this recipient)
- skip whole client state if recipient cannot see them at all

Possible policies the schema enables:

- viewers see editors' cursors, not vice versa
- team A members see only team A awareness
- anonymous users see no awareness at all
- cursor in a private zone never sent to clients without access to that zone
- user identity visible to authenticated; cursor visible to anyone with `awareness.see`

## Branch and Zone Scoping

Awareness scoped per `(room, branch)`. Cursor on `main` not visible on `published`. Anchors must target Elements in zones both the publisher AND the recipient can access; otherwise filtered.

Per-user branches: typically only the branch owner publishes awareness on their own branch.

## Wire Format

Separate message kind from doc ops. No lamport ordering required — LWW-per-client suffices.

```text
{
  kind:      "awareness.update",
  client_id: "...",
  actor_id:  "...",
  branch:    "...",
  zone_hint: "shared_content",        // optional, derived from anchor target
  updates:   { cursor: <anchor>, typing: true },
  deletes:   ["selection"]
}
```

Server fan-out:

1. validate against schema
2. check `@auth.publish` for each updated entry
3. apply throttle (drop if too fast)
4. broadcast filtered per recipient

## Storage and Cluster

In-memory only. Not persisted, not in op log, not in snapshots.

For cluster: leader holds awareness state in memory, forwards ephemerally to followers for the clients connected to those followers. On failover, awareness is lost — clients republish to the new leader on reconnect. Acceptable trade for an ephemeral subsystem.

## What's Not Awareness

Things that look like awareness but belong in document content:

| Use case | Where it goes |
|----------|--------------|
| "Show poll results everyone sees" | Counter / Register in doc content — shared, persistent |
| "Last edited by X at time Y" | Audit log or content metadata, not awareness |
| "User X commented" | Comment is a `RangedElement`, not awareness |
| "User X is currently in this section" | Awareness `viewport` entry |
| "Active users in this room right now" | Awareness — derived from connected client states |

Rule of thumb: if it must persist beyond disconnect, it is not awareness.

## What We Don't Ship

- awareness history (use audit log of connect/disconnect events)
- CRDT merge across clients (each client owns its own state; no merge needed)
- awareness migrations (awareness schema evolves independently; mismatched clients drop unknown entries)

---

# Admin UI

The system should include a lightweight dashboard.

Features:

```text
rooms
connected users
ops/sec
snapshot size
replication lag
cluster health
operation log viewer
```

---

# Debugging Features

CRDT systems are difficult to debug.

Need tooling:

- operation inspection
- replay
- timeline visualization
- causal graph visualization
- room export/import

---

# Authentication

Engine validates signed tokens at connection time. Engine does not ship an identity provider — apps bring tokens from their own auth backend (JWT, OIDC, custom).

## Token Shape

```json
{
  "iss": "app-auth-provider",
  "sub": "user_123",
  "aud": "crdtsync:room:doc-7",
  "exp": 1234567890,
  "roles":  ["editor"],
  "groups": ["team-acme", "beta-testers"],
  "scope":  { "session_id": "...", "device_id": "..." }
}
```

Standard JWT. Signed with a key the engine trusts (configured at server startup). Engine validates signature + expiration + claims. Engine never issues tokens; app's auth provider does.

For sharing / embed: app generates a restricted-scope token (limited role, scoped room/branch, near-term expiration). Engine validates and grants access scoped to that token.

## Identity on Ops

Every op carries:

```text
op_id:          (client_id, client_seq)
actor_id:       authenticated user id (from token sub)
zone:           zone id (derived from target Element; see Authorization)
schema_version: int
lamport:        int
kind:           ...
target:         ...
payload:        ...
```

`client_id` identifies device/session. `actor_id` identifies the human. Same user across two devices = same `actor_id`, different `client_id`. Critical for per-user undo (stacks are per-actor across devices), per-user branches, and audit.

`actor_id` is mandatory from v0.1. For dev mode without auth, app issues an anonymous token with `actor_id = "anon:<random>"` so the field is always populated.

---

# Authorization

Authorization in a collaborative sync engine has to be first-class. Bolting it on after the fact is the most common reason CRDT-based apps end up reinventing huge amounts of infrastructure badly.

The engine ships:

- token validation
- declarative policy enforcement
- two-tier auth model (schema-level defaults + doc-level dynamic ACLs)
- wire-level redaction (unauthorized bytes never leave the server)
- audit log

The engine does not ship:

- identity provider, login, password reset, MFA
- user / team / org management UI
- permission management UI (admins build their own)
- organization modeling beyond claims in the token

## Two-Tier Model

Two complementary systems, each with its own scope:

| Layer | Where | Purpose |
|-------|-------|---------|
| **Schema-level `@auth`** | declared in schema, version-controlled, ships with app code | static type-wide defaults: "all paragraphs writable by editor role" |
| **Doc-level ACL** | CRDT-merged state inside the document | dynamic per-instance grants: "this specific comment readable only by Alice" |

Apps need both. Schema covers "default policy for things of type X." Doc-level covers "specific instance Y has unique sharing."

This split matches Google Docs, Notion, Linear, AWS IAM. Industry standard.

## Subject Types

| Subject | Match rule | Source |
|---------|-----------|--------|
| `user:<id>` | `actor_id == "<id>"` | token `sub` claim |
| `role:<name>` | `"<name>" ∈ token.roles` | token `roles` claim |
| `group:<name>` | `"<name>" ∈ token.groups` | token `groups` claim |
| `authenticated:*` | actor has a valid token | implicit |
| `anonymous:*` | request has no token or anon token | implicit |
| `*` | anyone (including anonymous) | implicit |

User-level and role-level are first-class peers — both supported, composable in any ACL tuple. Engine reads claims from the token, never decides role membership itself; that is the app auth provider's responsibility.

## Action Set

```text
room:      read, admin
branch:    read, write, create, delete
element:   read, write              (per path or per element id)
mark:      create, read, update, delete   (per mark name or per mark instance id)
version:   create, read, restore, delete
snapshot:  export
migration: apply
awareness: publish, see                  (per awareness entry kind)
acl:       grant, revoke, read       (meta-auth on the ACL system itself)
```

## Resource References

| Form | Scope |
|------|-------|
| `room.<id>` | the whole room |
| `branch:<name>` | a specific branch within the room |
| `element:<path>` | a path into the tree (inherits downward) |
| `element_id:<id>` | a specific Element instance, survives moves |
| `mark:<name>` | all instances of a mark name |
| `mark_instance:<id>` | a specific mark instance |
| `version:<id>` | a specific named version |

Path-based resources inherit downward (ACL on `element:doc.body` covers all descendants unless overridden). Instance-based resources are precise.

## Schema-Level `@auth` Annotations

Co-declared in the schema alongside type and mark definitions. Auth requirements travel with type definitions, version with the schema, get validated at producer side.

```json
{
  "types": {
    "paragraph": {
      "kind": "xml",
      "tag": "p",
      "@auth": {
        "read":  ["role:viewer", "role:editor", "role:admin"],
        "write": ["role:editor", "role:admin"]
      },
      "children": ["inline*"]
    },
    "private_note": {
      "kind": "xml",
      "tag": "note-private",
      "@auth": {
        "read":  ["user:${author_id}"],
        "write": ["user:${author_id}"]
      }
    }
  },
  "marks": {
    "comment": {
      "kind": "object",
      "@auth": {
        "create": ["role:viewer", "role:editor"],
        "delete": ["role:author", "role:admin"]
      }
    }
  }
}
```

Templating: `${actor_id}`, `${author_id}`, `${room_id}`, `${branch_id}` resolve at check time from connection / op context. Allows expressing "user can do X to resources they own" cleanly without instance-by-instance ACL tuples.

## Doc-Level ACL Subsystem

A dedicated CRDT-merged subsystem alongside content:

```text
Document {
  schema_version: int
  content:        <main element tree>
  acl:            ACLSubsystem
}

ACLSubsystem = CRDT-Set<ACLTuple>

ACLTuple {
  id:         stable CRDT id
  subject:    "user:42" | "role:editor" | "group:team-x" | "authenticated:*" | ...
  action:     "read" | "write" | "create" | "delete" | ...
  resource:   "element:doc.body.section_3"
            | "element_id:abc-123"
            | "mark:comment"
            | "mark_instance:def-456"
            | "branch:main"
            | "version:v-789"
  effect:     "allow" | "deny"
  granted_by: actor_id
  granted_at: lamport
  expires_at: lamport | timestamp | null
}
```

Tuples are CRDT-merged: add-wins for grant set membership; per-tuple LWW for field updates.

### API

```ts
doc.acl.grant({  subject: "user:bob",     action: "read",   resource: "element:doc.body.section_5" })
doc.acl.grant({  subject: "role:editor",  action: "update", resource: commentElement.id })
doc.acl.deny ({  subject: "user:bob",     action: "read",   resource: privateNote.id })
doc.acl.revoke(tupleId)

doc.acl.list({ filterBy: { resource: commentElement.id } })
doc.acl.check({ actor: "user:alice", action: "read", resource: commentElement.id })  // local check
doc.acl.observe((change) => { ... })
```

ACL ops are first-class CRDT log entries. Replicated, audited, undoable.

## Decision Flow

For every check `can(actor, action, resource)`:

```text
1. Walk ACL tuples matching (actor, action, resource) and its ancestors
2. Any explicit DENY match (on user, role, or group) → DENY
3. Any explicit ALLOW match → ALLOW
4. Schema @auth grants actor's role for this resource type → ALLOW
5. Otherwise → DENY                          (default-deny)
```

Standard IAM semantics:

- explicit deny always wins
- user-specific tuples are not implicitly "stronger" than role-based for allow — match is match
- absence of declaration = denial

This evaluator is the single source of truth used at every enforcement point.

## Enforcement Points (Server-Side)

| When | Check |
|------|-------|
| Connect | actor has `room.read` on the room AND `branch.read` on the requested branch |
| Op submit | actor has `element.write` at op's target AND schema-level type/mark auth |
| Op outbound | recipient has `element.read` at op's target — filter per recipient before sending |
| Awareness publish | actor has `awareness.publish` for the entry kind on the branch/zone |
| Awareness outbound | recipient has `awareness.see` for the entry kind — filter before broadcasting |
| Version create / restore / delete | actor has corresponding `version.*` |
| Branch create / delete | actor has corresponding `branch.*` |
| Migration apply | actor has `migration.apply` |
| Snapshot export | actor has `snapshot.export` |
| ACL grant / revoke | actor has `acl.grant` or `acl.revoke` |

Server is the final authority. SDK exposes `canDo(action, resource)` for client-side UI hints, but client-side checks are advisory only.

## Wire-Level Redaction

If bytes hit the client, assume they leak. Browser devtools, MitM, malicious extensions — any sent byte is observable. Server must never send unauthorized data, ever.

### Per-Recipient Filtering

On every op send and every cold-start snapshot, server walks the tree and filters:

```text
for each Element / attr / mark / range:
  if not can(recipient, "read", thing):
    skip — never send
```

The check combines schema + ACL evaluation through the same evaluator used for write authorization.

### Zones (Coarse Partition)

For docs with large auth-uniform subtrees, declare them as zones — separately replicated streams:

```json
{
  "types": {
    "doc": {
      "kind": "xml",
      "children": ["public_meta", "shared_content", "private_section"]
    },
    "public_meta":     { "kind": "xml", "@zone": "public",  "@auth": { "read": ["*"] } },
    "shared_content":  { "kind": "xml", "@zone": "team",    "@auth": { "read": ["role:viewer", "role:editor", "role:admin"] } },
    "private_section": { "kind": "xml", "@zone": "private", "@auth": { "read": ["role:admin"] } }
  }
}
```

Zone properties:

- each zone is a separate sync stream
- per-zone lamport clocks (avoids cross-zone activity leakage)
- client subscribes only to zones it's authorized for
- unauthorized zone ops, snapshots, structure, even element counts never sent
- cross-zone tree moves forbidden at schema level
- cross-zone anchors forbidden by default; opt-in opaque references for cross-zone marks/comments

Zones are a perf and isolation optimization. For fine-grained per-instance auth (lots of per-element ACLs), zones less useful — ACL set carries the load. For coarse uniform-auth subtrees, zones are highly efficient (one rule, big coverage). Both work together.

### Snapshot Strategy

| Option | Trade-off |
|--------|-----------|
| Per-zone snapshots stored separately | Cleaner. Server combines authorized zone snapshots on cold-start. Storage = sum of zones. |
| Single snapshot with per-recipient redaction on demand | Single storage. CPU cost per cold-start, mitigated by caching per auth profile. |

Default: per-zone snapshots when few zones exist; redact-on-demand with profile cache for many fine-grained zones.

## ACL State is Itself Privacy-Sensitive

The existence of a tuple "Alice can read `element_id:secret`" leaks that `secret` exists and Alice has access to it.

ACL tuples are redacted per recipient:

- tuple sent to a recipient only if they are the subject, or they have `acl.read` on the resource
- admins (with `acl.read = *`) see all tuples
- regular users see only tuples involving them

Engine handles ACL-tuple filtering the same way it handles content redaction.

## Meta-Auth: Who Can Grant?

Schema declares meta-rules about who can mutate the ACL subsystem:

```json
{
  "@meta_auth": {
    "acl.grant":  ["role:admin", "role:owner"],
    "acl.share":  ["role:editor"],     // sharing-only role, can grant read but not write
    "acl.revoke": ["role:admin", "role:owner"],
    "acl.read":   ["role:admin"]       // full visibility into all ACL tuples
  }
}
```

App tunes per-app: some apps let any editor share a section; some restrict grants to owner only.

## Producer-Side Defense in Depth

SDK won't let a client construct an op targeting elements / paths / zones it can't write to. Producer-side schema enforcement already exists; auth checks layer in. Invalid op never leaves the client.

Server still re-validates — client-side enforcement is advisory, server is authoritative.

## Audit

Op log is the authoritative record. Every op has `actor_id` + lamport + timestamp. Audit = log query:

```bash
crdtsync audit --room=doc-7 --user=user_123 --since=2026-01-01
crdtsync audit --room=doc-7 --action=restore --since=2026-01-01
crdtsync audit --room=doc-7 --kind=acl.grant
```

Separate **access log** for read-only actions (connect, snapshot export, branch read) since those don't generate ops:

```text
access_log {
  timestamp, actor_id, action, resource, result (allow/deny), token_hash
}
```

## Hard Problems

### Offline Edits + Permission Revocation

User offline for a day, edits locally. Permissions revoked while offline. Reconnects → server rejects unauthorized ops.

Behavior:

- server returns `unauthorized` for each rejected op with details
- SDK surfaces to app: "these ops were rejected" + op contents
- app decides UX: discard / export / show user / etc.
- local state reverts to last server-acknowledged state

Not silent. Not data-loss without notice.

### Race: Op Submitted as Permission is Being Revoked

Permission state itself is versioned in lamport time. Server checks ops against permissions at the op's lamport position. Deterministic across replicas — if revocation lamport < op lamport, op rejected.

### Schema Migration + Auth Migration

Schema version N has auth declaration. Schema version N+1 has different auth declaration. Migration entry carries new auth. Ops tagged version N checked against version N auth; ops tagged version N+1 checked against version N+1 auth. Auth declarations migrate alongside schema in the same migration files.

### Migration as Admin Op

Migration entries themselves require `migration.apply` permission. Signed by an admin actor. Server rejects migration entries from non-admins.

### Public / Anonymous Access

App generates anonymous tokens (`actor_id = "anon:<rand>"`, role `anon`). Policy treats `role:anon` like any other role. Engine doesn't distinguish anonymous from authenticated — it's just a token with whatever claims the app put in.

### Cross-Zone References

Comments anchored across auth zones, mentions of users in unauthorized zones, suggestions that bridge zones — restricted by default. App can opt into opaque-reference behavior where the anchor is a token the client can pass back but cannot decode.

## Storage / Perf Notes

- ACL set indexed by resource (lookup by element_id) and by subject (lookup all grants for a user)
- ACL ops are normal log entries
- ACL snapshot included in document snapshot
- Per-recipient filtered view computed on op send + cached per auth profile
- Cold-start: build per-recipient redacted view from snapshot + ACL state, cache by profile
- Old tuples GC'd when subject deleted or resource removed

ACL state grows with use. High-cardinality per-element ACLs cost more — apps that need many specific grants should consider whether a path-based or role-based grant would cover the use case more efficiently.

## Roadmap

| Capability | Milestone |
|-----------|-----------|
| Token validation + actor_id on ops + basic room read/write enforcement | v0.1 |
| Declarative policy file, role-based + path-based rules, audit log | v0.2 |
| Schema-declared awareness entries (typed, per-entry TTL + throttle) | v0.2 |
| Per-recipient awareness filtering (`@auth.see`, anchor reachability) | v0.2 |
| Reconnect grace window | v0.2 |
| Cluster awareness forwarding (leader → followers) | v0.4 |
| Branch-level ACL + branch-scoped replication | v0.4 |
| Schema-level `@auth` annotations | v0.5 |
| Doc-level ACL CRDT subsystem | v0.5 |
| Zones + per-zone replication streams + wire-level redaction | v0.5 |
| Per-zone snapshots + per-profile redacted view caching | v0.5 |
| Schema-aware attr / mark filtering per recipient | v0.5 |
| Meta-auth (who can grant) | v0.5 |
| Opaque cross-zone anchors (opt-in) | v0.6 |
| ACL audit / query CLI | v0.6 |
| Sharing / embed token generation helpers | v0.3 |

---

# API Surface

The main editing API should be SDK-based.

HTTP APIs mainly for:

- observability
- snapshots
- exports
- admin
- cluster inspection

---

# Deployment Story

## Single Node

```bash
docker run crdtsync
```

Provides:

- websocket server
- persistence
- snapshots
- admin UI

---

## Cluster Mode

```bash
crdtsync serve \
  --node-id node-a \
  --join node-b,node-c
```

Cluster features:

- room sharding
- replication
- failover
- distributed ownership

---

# Use Cases

## Collaborative Text Editors

- notes
- docs
- markdown editors
- CMS

---

## Kanban / Productivity Apps

- tasks
- boards
- comments
- shared state

---

## Multiplayer Applications

- shared state
- collaborative tools
- whiteboards

---

## Embedded Sync Engine

Apps embed local core and sync automatically.

---

# Yjs Interoperability

A `fromYDoc` importer ships in v0.3 alongside the WASM/C ABI work.

## Scope

- snapshot import only: walk a Y.Doc's current state, reconstruct as native Document
- one-way migration tool, not a live bridge
- imported doc starts fresh history; merge with live Yjs peers after import is not supported

## Type Mapping

| Yjs | Native | Notes |
|-----|--------|-------|
| `Y.Map` | `Map` | direct |
| `Y.Array` | `List` | direct |
| `Y.Text` | `Text` (+ marks via `RangedElement`) | format attributes mapped to marks |
| `Y.XmlElement` / `Y.XmlFragment` / `Y.XmlText` | `XmlElement` / `XmlFragment` / `Text` | direct (v0.5+) |
| `Y.Doc` | `Document` | direct |

## Non-Goals

- YATA wire-compat or binary update format — that would amount to reimplementing Yjs core and defeat the portable-OCaml-core architecture.
- Y.UndoManager parity — undo is reimplemented natively, not imported.
- Y.Awareness import — awareness is ephemeral, not part of the snapshot.

The importer is framed explicitly as a **migration tool** to avoid setting expectations of drop-in replacement.

---

# Why OCaml?

OCaml is a strong fit because of:

- algebraic data types
- correctness guarantees
- immutable data structures
- excellent parsing/modeling
- strong concurrency story with OCaml 5
- systems-level performance
- good fit for protocol/state machine design

CRDTs and distributed systems benefit heavily from:

- explicit modeling
- type-safe invariants
- correctness-oriented architecture

---

# Suggested Tech Stack

## Core

```text
OCaml 5
```

---

## Concurrency

Potentially:

```text
Eio
```

---

## Networking

Possible options:

```text
Dream
Piaf
custom Eio server
```

---

## Storage

Start:

```text
SQLite
```

Later optional:

```text
RocksDB
```

---

## Serialization

Potential options:

```text
CBOR
MessagePack
Cap'n Proto
```

---

## CLI

```text
Cmdliner
```

---

# Foundational Decisions

Decisions that shape the wire format, op model, or schema language. These bind early — adding them after v0.1 ships requires breaking changes.

**Status: all foundational decisions are decided.** Implementation choices (wire codec, compression, framing details, TLS profile, keepalive intervals, op size limits) are deferred to implementation time and can be revisited without breaking the model.

| Status | Decision | Why foundational |
|--------|----------|------------------|
| **decided** | **Binary blob model** | Refs in ops, bytes in separate blob store, content-addressable internally (sha256), random UUIDs publicly. Universal presigned-URL interface across all backends. Inline only for blobs ≤ 4KB. ACL per reference site. See **Binary Blobs** section. |
| **decided** | **Atomic multi-op transactions** | Single `doc.transact()` API. Non-atomic batching is the default (streaming UX, CRDT-correct). `atomic: true` opt-in for privilege / reference / cross-element invariants. `tx_id` + `tx_role` reserved in op envelope from v0.1. See **Transactions** section. |
| **decided** | **Unicode / Text char-id strategy** | Codepoint as CRDT identity (stable across Unicode versions), UTF-8 on wire, grapheme-cluster API default with codepoint-level opt-in. Mismatched Unicode versions produce cosmetic differences only — no data corruption. See **Text and Unicode** section. |
| **decided** | **Op causality model** | Lamport timestamp + implicit dependency via payload refs. No explicit `deps` list field, no vector clocks. Receivers buffer out-of-order ops by looking up referenced `char_id` / `element_id`. See **Algorithms and Invariants → Causality → Dependency Model**. |
| **decided** | **Custom Element types / plugin extensibility** | Closed primitive set. Wire-format op `kind` is a fixed enum. Apps cannot define new CRDT types in app code; they compose from existing primitives (cookbook ships v0.2). Genuinely new primitives ship through engine releases via RFC. App-level customization (XML types, marks, attrs, schema constraints, awareness, ACL) is fully supported through schema. See **Extensibility** section. |
| **decided** | **Client ID strategy** | UUID v7, client-generated, per-Document-instance, persisted across same-instance restart (sessionStorage on web / app temp storage on native). Each tab is a distinct `client_id`; multi-device handled by shared `actor_id`. 16 bytes binary on wire. See **Client ID** section. |
| **decided** | **Connection / multiplexing model** | One WebSocket per `(server, actor session)`; logical channels multiplexed per `(room, branch, zone)` subscription; subscribe/unsubscribe in-band. See **Networking Layer → Connection / Multiplexing Model**. |
| **decided** | **Handshake structure** | Three phases (Hello / Auth / Subscribe); format-stable wire-version header in the first 8 bytes; pluggable auth carriers (cookie / WS subprotocol / Authorization header / in-band Auth / mTLS / API key); `Auth.credentials` is opaque bytes interpreted by deployment-configured verifier; clients never assert `actor_id`. See **Networking Layer → Handshake**. |
| deferred to impl | **Wire format codec** (CBOR / MessagePack / Cap'n Proto / custom) | Negotiated via Hello; new codecs ship in later releases without breaking older clients. |
| deferred to impl | Compression, framing details, TLS profile, keepalive, op size limits | Implementation/infrastructure, not foundational. |

These decisions interlock — the cargo determines the carrier. Wire protocol is intentionally last; designing it before locking the model is premature.

## Additive (no foundational pressure)

Topics that can land cleanly later without breaking the v0.1 model:

- editor adapter contract (pure SDK layer)
- storage layout refresh (server-internal refactor)
- search / indexing (wraps existing op log + schema)
- quotas / rate limits (server-side gates)
- debugging tools (additive on top of op log + replay)
- E2E encryption (op payload wrapping, no envelope change)
- branch merging (logical layer over existing branch primitives)
- webhooks / external integrations (event emitter)

---

# Suggested Roadmap

# v0.1

## Single Node MVP

Features:

- websocket sync
- room support
- operation log
- snapshots
- SQLite persistence
- TS SDK
- shared CRDT core
- primitives: Map, List, Text, Register, Counter
- anchors / RelativePosition
- Map slot safety (initOnce, live, replace, orphan event)
- op batching wire format (encoder can ship dumb single-op)
- token validation + `actor_id` on every op + basic room-level read/write enforcement
- `BlobRef` value type reserved in op envelope
- local filesystem blob backend with co-located HTTP route + signed tokens
- blob upload / fetch / `inline_under: 4KB` for small blobs
- `tx_id` + `tx_role` reserved in op envelope
- `doc.transact()` API: client-side observer batching, network batching, server-side log atomic write (non-atomic default)
- Text with codepoint identity + UTF-8 wire + grapheme-aware SDK helpers (per-language Unicode lib bundled)
- op `kind` as a fixed enum in the wire format (closed primitive set)
- UUID v7 `client_id`, client-generated, per-instance, persisted via sessionStorage / app temp storage; 16 bytes binary on wire
- single multiplexed WebSocket per `(server, actor session)` with logical channels per `(room, branch, zone)`
- three-phase handshake (Hello / Auth / Subscribe), format-stable wire-version header, pluggable auth carriers (cookie / WS subprotocol / `Authorization` header / in-band Auth message), opaque `Auth.credentials`
- standardized `Error` envelope (closed-enum code + message + opaque details)

---

# v0.2

## Developer Experience

Features:

- declarative policy file (role / user / group + path-based rules) with audit log
- awareness subsystem (schema-declared entries, per-entry TTL + throttle, per-recipient auth filtering, reconnect grace window)
- reconnect
- compaction with tombstone GC watermark
- admin dashboard
- replay tooling
- `UndoManager` (per-user, redo) for Map / List / Text / Register / Counter — reuses `doc.transact()` for intention grouping
- composition cookbook (build Set / MV-Register / Counter-with-bounds / position pair / etc. from existing primitives)
- named versions (`createVersion` / `listVersions` / `updateVersion` / `deleteVersion`)
- auto-version triggers (event-driven and schedule-driven)

---

# v0.3

## Portable Runtime + Interop

Features:

- WASM export
- stable C ABI
- Python bindings
- Go bindings
- Yjs snapshot importer (`fromYDoc`) — framed as one-way migration tool, not live bridge

---

# v0.4

## Distributed Cluster + Branches

Features:

- room sharding
- replication
- failover
- leader election
- cluster membership
- first-class branches (`doc.branch(...)`, fork points, per-branch HEAD)
- branch-scoped replication (replica sets shard by `(room, branch)`)
- branch-level ACL (per-branch read/write permissions)
- restore-as-branch (no state-vector reset, no clock hacks, audit version auto-created)
- publish / draft convention (`branch("published").syncFrom("main")`)
- per-user branches (`branch(`user-${id}`).forkFrom(...)`)

Branches piggyback on cluster work because branch-scoped replication and sharding are co-designed with the cluster routing layer.

---

# v0.5

## Rich Text, Document Trees, Schema

Features:

- XmlElement / XmlFragment primitives
- RangedElement primitive (generic ranged annotation)
- Marks (Peritext-style range CRDT, declarative registration via schema)
- Kleppmann 2021 tree-move algorithm
- declarative Schema (first-class, JSON-serializable, versioned, optional)
- producer-side op validation against schema
- Invariant Repair (opinionated, fixed rules, no config, `repaired` event)
- `sync-prosemirror` adapter (mirrors `y-prosemirror` API surface)
- `UndoManager` extensions: XmlElement (including move), RangedElement, marks
- schema-aware diff API (`doc.diff(versionA, versionB)`) with per-type renderers
- schema-level `@auth` annotations (type / attr / mark)
- doc-level ACL CRDT subsystem (per-instance grants and denies, CRDT-merged)
- zones + per-zone replication streams + per-zone lamport clocks
- wire-level redaction (per-recipient filtering of ops, snapshots, marks, attrs)
- per-zone snapshots + per-profile redacted view caching
- meta-auth declarations (who can grant / revoke / share)
- schema-declared blob fields (`max_size`, `allowed_mime`, `inline_under`)
- reference-site + blob-fetch auth checks (per-site ACL evaluation)
- S3-compatible backend (direct-to-S3 with no engine proxy)
- S3-compatible multipart resumable upload
- range requests + streaming
- server-side blob dedup (`random_id → sha256` mapping)
- reference counting + grace-period (30 day default) blob GC
- atomic transactions opt-in (`atomic: true`): cross-replica buffering, commit marker, partial-tx timeout + abort
- repair-in-commit-pipeline for atomic txs (atomic with tx)
- producer-side tx scope validation (single branch / zone / schema version)

The milestone that unlocks editor-grade collaboration (ProseMirror, Tiptap, BlockNote, Notion-style apps) and locks in schema as a first-class concern. Rich text + schema are the hardest parts of the project and get a dedicated release.

---

# v0.6

## Schema Migration

Features:

- migration entries as first-class log entries (history preserved)
- per-op `schema_version` tagging
- two-tier migration format: built-in step kinds + pattern-rewrite DSL
- `crdtsync migrate` CLI suite: `status`, `check`, `verify`, `test`, `generate`, `apply`
- schema-diff-based migration generation (Prisma-style)
- schema annotations (`@renamedFrom`, `@derivedFrom`) as diff hints
- four detection gates: drift, verification, server boot completeness/immutability, per-doc reachability
- mixed-version sync with bidirectional / forward-only translation policy derived from migration kind
- migration immutability via SHA-256 hash lock
- ACL audit / query CLI
- opaque cross-zone anchors (opt-in)

Depends on v0.5 schema landing first. Standalone milestone because migration tooling is substantial and orthogonal to runtime features.

---

# v0.7

## Production Features

Features:

- metrics
- tracing
- snapshots export/import
- replication tuning
- durability modes
- compaction policies (including optional log compaction at migration boundaries)
- WASM tier-3 migration escape hatch (if real demand surfaces)
- `crdtsync snapshot export --with-blobs` + `crdtsync blob sync` (federation)
- CDN tier for blob fetches (signed CDN URLs backed by S3)
- per-tenant HMAC-keyed blob hashing (ultra-privacy mode)

---

# Potential Future Features

## Binary Attachments

- blobs
- media synchronization

---

## End-to-End Encryption

Potentially:

```text
encrypted operation payloads
```

---

## Edge Deployment

Deploy small sync nodes geographically.

---

# Final Positioning

**crdtsync** should be positioned as:

> A self-hosted collaborative sync backend with a portable CRDT core.

Not merely:

> A CRDT library.

The differentiation is:

- batteries-included infrastructure
- operational simplicity
- no external infra dependencies
- portable shared runtime
- multi-language editing
- first-class versioning, branches, schema, auth, awareness
- official backend architecture
- self-hosted deployment
- horizontal scalability

---

# One-Sentence Pitch

> **crdtsync** — open-source collaborative sync infrastructure with a portable CRDT core, deployable as a single container with no Redis or Postgres required.

