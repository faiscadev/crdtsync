# crdtsync — worklist

**Derived from [ARCHITECTURE.md](ARCHITECTURE.md).** ARCHITECTURE is the end-state — the full scope + design, everything meant to be built *eventually*. This board is the **prioritized breakdown of what's not built yet**: a rolling queue `cs-next` cuts from ARCHITECTURE and **refills as it drains**. It is regenerable — if the board and the code disagree, the code wins. Status lives here + in the code, never in ARCHITECTURE; design changes that implementation forced are logged in [DECISIONS.md](DECISIONS.md).

`/cs-next` reads this + the dependency graph, replenishes the queue from ARCHITECTURE when it's thin, breaks work into units, and hands the next to `/cs-implement`. **Test suites are the spec** — a unit is "done" when its suite is green + Miri-clean, merged to `main`. Breakdown + prioritization is autonomous; the human only edits ARCHITECTURE.

## Dependency order

Bottom-up; never advance a unit whose dependencies are red.

```
host → stamp → clientid → elementid → scalar → counter → register → element → map
                                                    ↓
                          op envelope → doc (transact/apply, buffering, displacement)
                                                    ↓
                       list (Fugue) → text (codepoint) → binary codec
                                                    ↓
              wire framing → room hub → session driver → connection registry → websocket
                                                    ↓
                            persistence → state codec → compaction → client session
                                                    ↓
                                    SDKs (FFI / wasm / Python / Go)
```

Element + Map are one coupled unit (Map slots hold Elements; Element forwards lifecycle to composites).

---

## ✅ Done (on `main`)

_Derived from code + git; a convenience view, not the source of truth._

**Core primitives** — all green, Miri-clean:
scalar / counter / register / element / map (#22–#27), list Fugue (#24), text codepoint (#25), op envelope (#22), doc transact/apply (#30/#31), out-of-order buffering + persistent container identity (#32), binary op codec + log framing (#33).

**Server** — wire message framing (#40), room hub: op-log + idempotent ingest + catch-up (#41), session protocol driver (#42), connection registry fan-out (#43), tokio WebSocket transport (#44), durable op-log disk persistence (#47).

**SDKs** — C ABI / FFI (#34–#36), cbindgen header (#37), Python (#38), Go (#39), shared `core::path` navigation façade (#45), WebAssembly / JS (#46).

**Correctness** — counter identity across displacement fixed (#48); randomized convergence property harness (#49/#50); server durability property fuzz (#51).

**v0.2 state codec + compaction (arc complete)** — leaf-value (#52), sequence-CRDT (#53), whole-replica document (#54) state serialization, in-memory op-log compaction + `Message::Snapshot` (#55), durable disk-log compaction crash-safe (#56), SDK snapshot-decode (#57), automatic compaction policy (#58).

**v0.2 wire / client** — client session / reconnect driver `core::client::ClientSession` (#59).

**Forward-compat reservations** — blob-ref value slot `Scalar::BlobRef` reserved in the op envelope + codec (#60).

**Channel multiplexing** — one connection multiplexes many rooms via client-assigned `Channel`; server session holds channel→room, registry fans out per peer-channel (#61); SDK-side `ClientSession` holds N rooms, each with its own replica + last-seen seq, routing inbound frames by channel, reconnect via `resume(channel)` (#62). Arc complete.

**Handshake auth** — three-phase Hello → Auth → Subscribe. Wire `Auth`/`AuthOk` messages (#63); server pluggable `Verifier` + session actor gate, dev-mode `AllowAll` default (#64); client `ClientSession::auth`/`actor` (#65). Server derives actor; client never asserts it.

**Auth fast path + anonymous mode** — `Session::authenticated` + `Registry::connect_authenticated` open a connection already authenticated (#73); runtime verifies an `Authorization`-header credential during the WS upgrade and sends an unsolicited `AuthOk`, skipping the in-band Auth phase; `ServeConfig::anonymous` mints `actor = anon:<random>` from transport-layer entropy (#74). Header carrier done.

**Verifier injection** — `serve_with_verifier` plugs a real `Box<dyn Verifier + Send>` (JWT/OIDC/API key) into the runtime; `serve`/`serve_with` default to dev `AllowAll` (#75). Real end-to-end map + reject now exercisable.

**Auth carriers** — fast-path credential read from four carriers in precedence order: `Authorization` header → `crdtsync.auth.<v>` subprotocol (echoed so browser negotiation succeeds) → `crdtsync_credential` cookie → `?credential=` query param (#76). Browser-reachable carriers (subprotocol/query/cookie) covered. mTLS deferred — no TLS layer yet (see Queue).

**Awareness (core)** — ephemeral presence: wire `AwarenessSet`/`AwarenessUpdate` (#66); server fan-out per peer-channel, actor-tagged, never logged/snapshotted (#67); client `set_awareness` + per-channel `(actor,key)` LWW view (#68); server-side ephemeral store → late-joiner replay on Subscribe + clear-on-disconnect (#69). Publish + fan-out + client view + late-joiner replay done.

**Awareness reconnect grace** — `AwarenessClear` wire message (#70); server `Clock` seam (`SystemClock`/`ManualClock`) + grace window (default 5s) + `Registry::sweep` fanning `AwarenessClear` to room peers, reconnect within window cancels the clear (#71); periodic sweep wired into the tokio runtime via `serve_with`/`ServeConfig` so grace expiry fires in production (#72). Session-TTL (grace) complete end-to-end. Timed-TTL + throttle are schema-gated (see Queue); auth-filter still queued.

**Auth fast path + carriers** — connection opens pre-authenticated (`Session::authenticated`/`connect_authenticated`, #73); `Authorization`-header credential verified at the WS upgrade with an unsolicited `AuthOk` + anonymous mode (#74); real `Verifier` injectable via `serve_with_verifier` (#75); credential carriers extended to subprotocol/cookie/query with precedence (#76). mTLS deferred (no TLS layer).

**SDK wiring — wire client** — the full `CrdtClient` C ABI (`ClientSession`: lifecycle, receive, per-channel edits/reads, auth, awareness, last-seen; core `document_mut`) (#77/#78), wrapped in the Python (#79), Go (#80), and wasm (#81) SDKs. Every SDK can now drive the sync protocol, not just the local `Document`.

**Authorization seam** — pluggable `authz::Authorizer` (`Action` × `Resource::Room`), default-deny contract, dev `PermitAll`; enforced at Subscribe (read) / Ops (write) / AwarenessSet (publish); non-closing `ErrorCode::Forbidden`; injectable via `set_authorizer` (#82). Room-level enforcement points that exist today; two-tier policy + redaction + zones + audit remain (see Next/Queue).

**Per-recipient read redaction** — the registry re-checks `Read` on every fan-out (ops + awareness), so a peer whose read is revoked mid-session stops receiving the room without resubscribing; enforces the "filter every op send" invariant against a dynamic policy (#83). Room-level today; the per-send hook is where element/zone redaction slots in. Doc-level ACL-CRDT + finer-grain snapshot redaction + audit log remain (see Next/Queue).

**ACL decision flow** — `acl::Acl`, a concrete tuple-walking `Authorizer` (the first real policy on the seam): allow/deny rules over `Subject` (`Actor`/`Authenticated`/`Anonymous`/`Anyone`) × action (`Option<Action>`) × `ResourceMatch` (`AnyRoom`/`Room`), evaluated explicit-deny-wins → allow → default-deny; order-independent; plugs in via `set_authorizer` (#84). Role/group subjects (need a claims model) and schema `@auth` role grants (need the schema layer) deferred. Doc-level ACL-as-CRDT feeds this same evaluator later.

**Access log** — `audit::{AccessLog, AccessRecord, Audited, Decision}`: an `Audited` decorator wraps any inner `Authorizer`, forwards its verdict, and emits each decision (actor, action, resource, verdict) to a pluggable sink — logged at the seam every enforcement point consults, so read-only accesses (subscribe) are captured alongside the writes the op log already records (#85). Never logs the credential; an awareness publish logs the decision only, never the entry's key/value. A distinct `Connect`/snapshot/branch audit action + the query surface are follow-ons (need those actions / an admin CLI).

**Named versions (index)** — `Hub` versions layer over the snapshot primitive: `create_version` captures a room's whole-replica state + covered seq under a name; `version_seq`/`version_state` read back (export/diff); `version_names` lists sorted for pagination; `rename_version`/`delete_version` complete the index. Versions are immutable point-in-time; taken names refused (#86).

**Named versions (durable)** — the index persists to a per-room `<room>.versions` store file (framed `(name, seq, state)` records), rewritten crash-atomically on each change (temp→fsync→rename→dir-fsync) and reloaded on reopen; `create`/`rename`/`delete` are now `io::Result<bool>`, persisting before commit with in-memory rollback on IO failure (#87).

**Named versions (wire)** — seven wire messages (tags 11–17): client→server `VersionCreate`/`VersionRename`/`VersionDelete`/`VersionList`/`VersionFetch`; server→client `Versions{names}` (state-based ack + list reply) and `VersionState{name,seq,state}` (fetch reply). Codec + round-trip suite; total decode (#88).

**Named versions (server handling)** — `step` serves the five requests on the channel's room: mutations run the `Hub` op and reply the fresh `Versions` list; list replies the same; fetch replies `VersionState` (hit) or the list (miss). Gated on `Write` (mutations) / `Read` (list/fetch); denial → non-closing `Forbidden`, unbound channel → violation, persist failure → closing `Internal` (#89).

**Named versions (client view)** — `ClientSession` frames the five requests for a held channel (`create`/`rename`/`delete`/`list`/`fetch_version`) and folds replies into a per-room view: `Versions` replaces the known names, `VersionState` caches fetched bytes by name; exposed via `versions(channel)` / `version_state(channel,name)` (#90). Server-authoritative — no optimistic local version state.

**Named versions (FFI)** — the `CrdtClient` C ABI exposes the five issue methods (`crdtsync_client_{create,rename,delete,list,fetch}_version` → request frames) and the view (`version_count` + indexed `version_name`; `version_state` by name) (#91). Header symbols asserted; Miri leak-clean.

**Named versions (language SDKs)** — Python/Go (over the C ABI) and wasm (over `ClientSession`) wrap the five issue methods + the view (`versions` list, `version_state` by name) (#92). **Arc complete** — named versions run end to end (index → durable → wire → server → client → SDKs). Restore-as-branch + auto-triggers stay blocked (branch layer / engine-event hooks).

**Composition cookbook** — executable recipes (`crates/core/tests/cookbook.rs`) building Set, bounded counter, multi-value register, and a tagged document from the closed primitive set — no new engine support — with convergence assertions (#93). The "compose, don't add primitives" thesis, kept honest by tests.

**UndoManager (scalars)** — `core::undo::UndoManager`: per-user undo/redo over root Register + Counter slots. Records each edit made through it (register/inc/dec/delete), capturing the overwritten value; `undo`/`redo` replay ordinary forward ops (no server state, no wire change) and converge on peers like any edit; a fresh edit clears the redo stack (#94).

**UndoManager (grouping)** — an undo step is one *intention*: `group(doc, |b| …)` records several edits (via a `Batch`) as a single undo/redo step, reverting them together; the single-edit methods are one-edit groups (#95). Matches ARCHITECTURE's "intentions = op groups." Nested paths + list/text revival are the next slices.

**Counter `dec` end-to-end** — a Counter decrement surfaced through the whole binding stack: `path::dec` → FFI `crdtsync_doc_dec`/`crdtsync_client_dec` → Python/Go/wasm `dec`, mirroring `inc` (#96). Closes an SDK gap (the PN-counter's negative direction had no path/FFI/SDK entry point) and readies nested-counter undo.

**UndoManager (nested paths)** — undo is now path-addressed: every `UndoManager`/`Batch` method takes an encoded path, so a scalar slot inside a nested Map undoes exactly as a root one; a group can revert root + nested edits as one intention (#97). Backed by `path::register` (generic scalar) + `path::get_register`. List/text revival is the remaining undo slice.

---

## 🚧 In progress

- _(nothing in flight)_

---

## ⏭ Next

- **Authorization — remaining policy layers** — atop the seam (#82), read redaction (#83), and the ACL decision flow (#84): doc-level ACL as CRDT-merged state (tuples live in the document, merge, and feed the #84 evaluator — needs the ACL-CRDT design + per-recipient ACL-tuple redaction, since ACL state is itself privacy-sensitive), role/group subjects (need a claims model threaded from the verifier to the enforcement point), and finer-grain wire redaction (element/zone + cold-start snapshot — room-level per-send landed in #83). Schema-level `@auth` defaults and zones are gated on the unbuilt schema + zone layers. Large — slice per layer. → *Authorization*. (v0.2, needs design)
- **Audit log — query surface + distinct read-only actions** — the access-decision half landed in #85 (`Audited` emits every authz decision to a pluggable sink). Remaining: an audit *query* surface (admin/CLI over the trail — pairs with the file-log-vs-DB revisit note) and distinct audit actions for accesses that today reach no `authorize` call — `Connect`, snapshot export, branch/version reads — each gated on that action/resource existing (`Action::Connect`, the branch/version layers). → *Authorization / Audit*. (v0.2, partly blocked)
- **Named versions — DONE (#86–#92)** — index → durable persistence → wire → server handling → client view → FFI → Python/Go/wasm, all merged. Remaining versioning work is dependency-gated: **restore-as-branch** (needs the branch layer) and **auto-version triggers** (need engine-event hooks / schedules) — both tracked under *Auto-version triggers* in the queue.
- **mTLS credential carrier** — a client certificate as the fast-path credential. Blocked: the server terminates plain TCP with no TLS layer to expose the cert; land TLS termination first. → *Networking / Handshake*. (v0.2, blocked on TLS)

---

## 📋 Queue — current cut of ARCHITECTURE, prioritized

Not exhaustive — the full backlog **is** ARCHITECTURE. This is the prioritized slice `cs-next` has broken out so far; when it drains, `cs-next` cuts the next slice from ARCHITECTURE. Order = dependency → foundational/forward-compat → roadmap/value. Each item cites the ARCHITECTURE section it derives from.

- **Element-ref value slot** — the other unbuilt payload value type (line 177). Under-specified shape, no v0.1 reservation promise, so deferred until its design settles — not urgent like blob-ref was. → *Internal Data Model*. (foundational, deferred)
- **Channel multiplexing — remaining** — logical channels currently key on `room`; widen to `(room, branch, zone)` once branches/zones exist. The `Channel` handle already abstracts this (no wire change needed then). → *Networking / Connection*. (v0.2, blocked on branches/zones)
- **Awareness timed-TTL + throttle** — per-entry auto-expire-after-silence (timed TTL, distinct from the session TTL the grace sweep already handles) + removal broadcast (reuse `AwarenessClear`), and server-side throttle/coalesce of high-frequency entries (cursor/mouse). **Schema-gated:** ARCHITECTURE §Awareness declares an entry's TTL and throttle interval in the schema file (line 708), and the schema layer is unbuilt — so these trigger values have no home until it lands. The clock seam (#71) + periodic sweep (#72) are ready to enforce them. → *Awareness / Schema*. (v0.2, blocked on schema)
- **Tombstone GC / watermark** — `min(last_seen_seq)` watermark, retention window ("keep last N"), time/migration compaction triggers. **Design depth (needs a careful pass before building):** snapshots are anchor-based (a tombstone anchors surviving nodes), so GC must be leaf-only (drop a below-watermark tombstone only when no surviving node parents off it), not a flat "discard older than watermark"; and the watermark is a server-seq while tombstones are lamport-`Stamp`-keyed with no client-ack protocol today — the correlation + ack semantics are unspecified. Gate any build on the convergence property harness (invariant: GC preserves materialized state). → *Snapshots / Tombstone GC*. (v0.2, needs design)
- **Declarative policy + audit log** — authorization enforcement. → *Authorization*. (v0.2)
- **UndoManager — remaining** — scalar undo (#94), grouping (#95), nested paths (#97) landed. Next slices: list/text revival (undo of an insert = delete; undo of a delete must re-insert the value — element-revival semantics), then the SDK surface. → *Undo/Redo*. (v0.2)
- **Auto-version triggers** (engine-event/schedule-driven version creation — needs the event hooks), **admin dashboard**, **replay tooling**. → *Versioning*, *Admin UI*, *Debugging*. (v0.2) _(composition cookbook landed #93)_
- **Blob refs (full)** — refs in ops, bytes in a content-addressable store. → *Binary Blobs*. (v0.5)
- **XmlElement / marks / schema / invariant repair / zones**. → *CRDT Model*, *Marks*, *Schema*, *Invariant Repair*, *Authorization/zones*. (v0.5)
- **Mixed-version migrations** — migration log entries, per-op `schema_version`, four detection gates. → *Schema Migration*. (v0.6)
- **Distributed cluster** — sharding, replication, failover, leader election, branches. → *Horizontal Scaling*, *Versioning and Branches*. (v0.4)

---

## 🔍 Revisit / tech-debt (accepted decisions flagged for a later look)

- **File-log vs. embedded DB for the query/metadata side** — the append-only file log is right for the op hot-path, but admin UI / op-log viewer / audit-query / retention want queryability, and durability is hand-rolled (a dir-fsync crash bug already shipped + fixed). Reconsider SQLite/redb for the metadata/index side when those consumers land. Checkpoint, not a reversal.
- **Cold-start snapshot CPU** — a below-floor subscriber triggers a whole-replica `encode_state` regenerated live per cold-start (O(state) CPU/connection). Fine now; cache the encoded snapshot per compaction floor if snapshots grow or cold-starts get frequent.

---

## Loop

`cs-next` (derive worklist from ARCHITECTURE → break down → prioritize → refill when thin) → `cs-implement` (spec-first tests → implement to green → Miri gate → `make fmt` → PR → react to CI + review → squash-merge → update this board; log any forced ARCHITECTURE change to DECISIONS.md) → continue up the chain.
