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

**Correctness** — counter identity across displacement fixed (#48); randomized convergence property harness (#49/#50); server durability property fuzz (#51); schema-aware diff soundness/completeness property (#114 — over random edit sequences, `diff` is empty iff an observable read of the two replicas is equal, round-trips through its codec, and is deterministic; oracle is a materialized read, not `encode_state`, since a no-op edit shifts causal metadata the diff correctly ignores).

**v0.2 state codec + compaction (arc complete)** — leaf-value (#52), sequence-CRDT (#53), whole-replica document (#54) state serialization, in-memory op-log compaction + `Message::Snapshot` (#55), durable disk-log compaction crash-safe (#56), SDK snapshot-decode (#57), automatic compaction policy (#58).

**v0.2 wire / client** — client session / reconnect driver `core::client::ClientSession` (#59).

**Forward-compat reservations** — blob-ref value slot `Scalar::BlobRef` reserved in the op envelope + codec (#60); error-envelope `details` byte string reserved in `Message::Error` + codec (#108) — round-tripped, empty, no producer yet, so the SDK error surface stays code + message.

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

**UndoManager (list revival)** — undo of a list insert deletes the node it minted (addressed by stable id, not a drifting index); undo of a list delete revives the removed value as a fresh insert at its place — the op log has no un-tombstone (#98). Backed by `List::live_index`, `ListCursor::delete_id`, `path::list_delete_id`/`list_live_index`. Text revival is the last undo slice.

**UndoManager (text revival)** — undo of a text insert deletes the run's char_ids; undo of a text delete revives the captured substring as a fresh run at its place (#99). Backed by `Text::live_index`, `TextCursor::delete_ids`, `path::text_delete_ids`/`text_run_ids`/`text_live_index`. Core undo now spans every value type (scalar / counter / list / text, root or nested); only the SDK surface remains.

**UndoManager (FFI)** — a `CrdtUndo` handle over the C ABI: `crdtsync_undo_new`/`_free` plus register_int/inc/dec/delete/list_insert/list_delete/text_insert/text_delete (each returns the ops to broadcast) and undo/redo/can_undo/can_redo (#100). The manager is a handle distinct from the `CrdtDoc` it drives, named on every call. Bytes-in-Register has no existing reader, so it is not exposed. Python/Go/wasm wrappers next.

**UndoManager (SDK wrappers)** — Python `Undo`, Go `Undo`, and wasm `WasmUndo` over the FFI/core surface: register_int/inc/dec/delete/list_insert/list_delete/text_insert/text_delete + undo/redo/can_undo/can_redo, each naming the document it drives (#101). Undo/redo now works end-to-end in every SDK; the subsystem is complete.

**Atomic transactions (core)** — `Document::atomic_transact` tags a group's ops with `Tx { id, count }` (op envelope + codec extended from the reserved `tx` slot); a receiver buffers members until the whole group is present and its external deps resolve, then applies them together in seq order — an all-or-nothing *view* boundary that preserves convergence (verified against the convergence harness) (#102). Buffered partials ride the existing op-buffer, so they survive a snapshot. Design logged in DECISIONS. Wire/server/client drive path + SDK surface are the follow-on slices.

**Atomic transactions (drive path)** — `ClientSession::atomic_edit` mirrors `edit` but tags the group as one transaction, sent as an ordinary `Message::Ops` (membership rides on the ops, no new wire message) (#103). The server needed no change: `Hub::ingest` returns every fresh op regardless of buffering, so a whole atomic batch fans out intact and a fresh subscriber's catch-up carries the full group — verified end to end (client peer folds it in all-or-nothing; hub preserves the tags through ingest + catch-up). SDK `atomic_edit`/`atomic_transact` is the remaining slice.

**Atomic transactions (SDK surface, doc-level)** — a begin/commit atomic API on the `Document` seat, since the C ABI has no closures: core `Document::begin_atomic`/`commit_atomic`/`is_atomic` (edits between them accumulate into one group; `atomic_transact` is now a thin wrapper), exposed as `crdtsync_doc_begin_atomic`/`_commit_atomic` (FFI), `begin_atomic`/`commit_atomic` (Python, Go), and `beginAtomic`/`commitAtomic` (wasm) (#104). Client-seat (per-channel) atomic wrappers over `ClientSession::atomic_edit` are the last slice.

**Atomic transactions (SDK surface, client-seat)** — per-channel begin/commit on the wire client: core `ClientSession::begin_atomic(channel)`/`commit_atomic(channel)` (the latter returns the group as one `Message::Ops`), exposed as `crdtsync_client_begin_atomic`/`_commit_atomic` (FFI) and `begin_atomic`/`commit_atomic` (Python, Go) / `beginAtomic`/`commitAtomic` (wasm) (#105). Edits recorded between them travel as one atomic group over the wire — a peer folds them all-or-nothing. **Atomic transactions are now complete end-to-end** (core → drive path → doc + client SDK seats).

**Atomic-transaction undo** — `UndoManager::atomic_group` records a gesture as one atomic transaction and its undo/redo replay as atomic transactions too, so a peer never sees a partially-undone group (#106). Realizes ARCHITECTURE §Transactions "a transaction is naturally an undo intention": an `Intention` carries an `atomic` flag and `apply` wraps an atomic intention's inverse ops in a fresh atomic tx via the doc's `begin_atomic`/`commit_atomic`. Core-only, matching `group` (closure-shaped, not in the SDK surface).

**Schema-aware diff (core)** — `core::diff::diff(old, new) -> Vec<Change>` computes the structural changes between two replica snapshots by walking the Element trees in lockstep (#109): slots `Added`/`Removed` (with kind), scalar/register `Value` and `Counter` old→new value changes, nested maps recursed so a deep edit reports at its own path. Path-addressed (shared `path` encoding) and ordered by path, so diffing a pair is deterministic. Backed by `Map::keys()`. Pairs with named versions + export/import (compare any two snapshots).

**Schema-aware diff — change-list codec** — `core::diff::encode_changes`/`decode_changes` serialize a `Vec<Change>` to a tag-led byte buffer (`u32` count then each change) and back, so a diff crosses the language-SDK boundary as one buffer (#112). Round-trips every variant, decodes totally (`BadTag`/`TrailingBytes`/`UnexpectedEof`), reuses the op-codec primitives (`put_scalar`/`Cursor`). Not durable — a diff is transient. Backed by `ElementKind::from_tag`.

**Schema-aware diff — FFI entry point** — `crdtsync_diff(old_state, new_state) -> CrdtBuf` over the C ABI: decodes two snapshot buffers, diffs them, returns the encoded change list (#113). Stateless (no doc handle) — the inputs are any two state blobs (a `version_state`, an exported room, `encode_state`); empty on malformed input or a bad snapshot, never a panic. Header symbol asserted, Miri leak-clean.

**Schema-aware diff — wasm surface** — `WasmDocument.diff(oldState, newState)` returns an array of structural change objects, each with an `op` tag, a `path` (Uint8Array), and its variant's fields (#115). A scalar is a tagged `{ t, v }` object (`int`/`bool`/`null`/`bytes`/`blobref`) so binary values and full-range ints read unambiguously; list items are `{ scalar }` or `{ kind }`. Throws on a malformed snapshot. This establishes the cross-language change representation.

**Schema-aware diff — Python surface** — module-level `diff(old_state, new_state)` returns a list of change dicts over the C ABI `crdtsync_diff` (#116): a pure-Python `_Reader` decodes the change-list bytes into the same shape as wasm — `{"op", "path", ...}` with tagged `{"t","v"}` scalars and `{"scalar"}`/`{"kind"}` list items. Raises `ValueError` on a malformed snapshot.

**Schema-aware diff — Go surface** — `crdtsync.Diff(oldState, newState) ([]Change, error)` over the C ABI `crdtsync_diff` (#117): a `changeReader` decodes the change-list bytes into typed `Change` structs (`Op`/`Path` + per-variant fields), with a tagged `Scalar` and `Item` (scalar-or-kind), mirroring the wasm/Python shape. Errors on a malformed snapshot. **Diff runs end to end across every SDK** (core → codec → C ABI → wasm/Python/Go); default renderers + XmlElement/marks/attrs diffs remain follow-ons (see Queue).

**Schema-aware diff — sequence detail** — Text and List diff to runs by stable id: Text → `TextInsert{index,text}`/`TextDelete{index,text}` codepoint runs (#110), List → `ListInsert{index,items}`/`ListDelete{index,items}` item runs (`SeqItem::Scalar`/`Composite`) (#111). Stable-id identity makes both exact (no heuristic alignment); consecutive same-op elements coalesce into runs, deletes (old index) before inserts (new index). Text is codepoint-indexed. Realizes ARCHITECTURE §Schema-Aware-Diff "Text values produce char-level diffs" + structural list change lists. **Core diff now spans every built primitive** (map/register/counter/list/text). XmlElement/marks/attrs diffs (unbuilt primitives), renderers, and the SDK surface remain follow-ons (see Queue).

**Snapshot export / import** — a room's whole-replica state is portable across hubs: `Hub::export_room` hands back the snapshot bytes (`None` for an unknown room); `Hub::import_room` rebuilds a fresh room from them, persisting the snapshot durably before it commits so the import survives a restart (#107). Backup / cross-server-move / debug-repro: the merged state, element/client identities, and dedup set all ride the bytes, so a client resending its ops is deduped exactly as against the origin, and a fresh subscriber is caught up with the imported snapshot. Import is create-only (refuses an existing room rather than clobbering live state) and rejects malformed bytes as `InvalidData`, never a panic; sequences renumber from the imported op count (server-local, so no collision with the origin). Cloning under a *new* room id — clock bumps + id namespacing so two live copies can't collide — stays a follow-on (see Queue).

---

## 🚧 In progress

- _(nothing in flight)_

---

## ⏭ Next

- **Atomic transactions — DONE (#102–#106)** — core (`Tx{id,count}` envelope, buffer-until-whole, seq-order commit), drive path (`ClientSession::atomic_edit`; server unchanged), the SDK surface at both the doc seat (`begin_atomic`/`commit_atomic` over FFI + Python/Go/wasm) and the client seat (per-channel begin/commit), and atomic-tx undo (`UndoManager::atomic_group`, #106). All-or-nothing view boundary, convergence preserved, verified end to end. Remaining scope constraints (one branch/zone/schema version, member cap) need the branch/zone/schema layers. → *Transactions*. (v0.2)
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
- **Schema-aware diff — renderers + blocked detail** — the diff runs end to end (core #109/#110/#111 → codec #112 → C ABI #113 → wasm #115 / Python #116 / Go #117). Remaining: default **renderers** (ARCHITECTURE §Schema-Aware-Diff "engine ships sensible default renderers; apps can override" — a presentation layer over the change list; product-shaped, likely human-steered), and **XmlElement / marks / attrs diffs** (blocked — those primitives aren't built). → *Versioning / Schema-Aware Diff*. (v0.2, partly blocked)
- **Snapshot clone (id-namespaced)** — the other half of §Export/Import: cloning a room under a *new* id as a live template. Identity-preserving move landed in #107; a clone needs the "import bumps clocks past imported lamport; element / client IDs are namespaced" step so two live copies of the same origin can co-exist without their op ids / element ids colliding. **Needs design:** the namespacing scheme (prefix element/client ids? rewrite the state blob or tag at the room boundary?) and the clock-bump semantics are unspecified, and rewriting identities inside an encoded snapshot touches the state codec. → *Snapshots / Export-Import*. (v0.2, needs design)
- **Tombstone GC / watermark** — `min(last_seen_seq)` watermark, retention window ("keep last N"), time/migration compaction triggers. **Design depth (needs a careful pass before building):** snapshots are anchor-based (a tombstone anchors surviving nodes), so GC must be leaf-only (drop a below-watermark tombstone only when no surviving node parents off it), not a flat "discard older than watermark"; and the watermark is a server-seq while tombstones are lamport-`Stamp`-keyed with no client-ack protocol today — the correlation + ack semantics are unspecified. Gate any build on the convergence property harness (invariant: GC preserves materialized state). → *Snapshots / Tombstone GC*. (v0.2, needs design)
- **Declarative policy + audit log** — authorization enforcement. → *Authorization*. (v0.2)
- **UndoManager — DONE (#94–#101)** — core undo across every value type (scalar/counter/list/text, root or nested), grouping, the FFI `CrdtUndo` handle, and Python/Go/wasm wrappers all merged. The whole subsystem is complete end-to-end. Remaining refinements are optional (bytes-in-Register has no reader to surface; a fluent group builder in the SDKs).
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
