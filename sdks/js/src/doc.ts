// A `Doc` is a local CRDT replica with a single root map. Editing is done
// through live typed handles obtained from the root (`getMap`/`getList`/
// `getText`); the byte-path core underneath stays hidden. A `Doc` is a pure
// local replica until it is bound to a sync provider — two docs that exchange
// each other's update ops (or share a provider) converge.
//
// Reactivity is diff-derived: an edit (local or an applied remote update) is
// bracketed by a state snapshot, and the core `diff` between the before and
// after states is re-marshaled into ergonomic change events. The snapshot/diff
// only runs when something is listening, so an unobserved document pays nothing.

import { type ApplyOutcome, type Backend, localBackend } from "./backend.js";
import { type Change, remarshalChange } from "./changes.js";
import { CrdtList, CrdtMap, CrdtText, CrdtXml } from "./handles.js";
import type { ChangeEvent, ChangeListener, HandleContext } from "./internal.js";
import { type Key, type RepairStep, decodeRepairPath, pathStartsWith } from "./path.js";
import { WasmDocument } from "./wasm/crdtsync_wasm.js";

export type { ApplyOutcome } from "./backend.js";
export type { Change } from "./changes.js";
export type { ChangeEvent, ChangeListener } from "./internal.js";
export type { RepairStep } from "./path.js";

const EMPTY = new Uint8Array();

/** Thrown by an edit the replica had no id left for.
 *
 * A refused mint is the fail-closed answer, never a re-issued id that would
 * collide with one already published. Without this it is
 * indistinguishable from an inert edit and the application reports a write that
 * never happened.
 *
 * A refusal cuts the transaction at the edit that could not mint — the edits after
 * it would address what it failed to create — so the call may still have emitted
 * and delivered what came *before* the refusal. Those ops are applied here and
 * shipped; the throw says the intention was not completed, not that nothing
 * happened. Nor is the replica always spent outright: a run reserves one id per
 * codepoint, so a shorter edit can still fit where a longer one was refused
 * (`can_mint` on the core is the capacity reading). */
export class MintExhausted extends Error {
  constructor(cause?: unknown) {
    super("crdtsync: the edit was refused, the replica could not mint the ids it needed", {
      cause,
    });
    this.name = "MintExhausted";
  }
}

/** An applied change to the document, delivered to `Doc.on("update")`. */
export interface UpdateEvent {
  /** `"local"` for an edit made on this replica, `"remote"` for an applied peer update. */
  readonly origin: "local" | "remote";
  /** The wire-bound bytes the edit produced (raw ops locally; an Ops frame when networked). */
  readonly ops: Uint8Array;
  /** The structural changes the edit produced (empty when nothing is observing). */
  readonly changes: Change[];
}

export type UpdateListener = (event: UpdateEvent) => void;

/** The `onRepaired` signal: locations whose repaired reading changed against the
 * bound schema after an edit. A path names a *location*, not a value — read the
 * fresh repaired value at it rather than caching. A repair belongs to a location,
 * not to whoever's edit surfaced it (local or remote, possibly batched across
 * several), so the event carries no origin. Empty until a schema is bound. */
export interface RepairEvent {
  /** The repaired locations, each a step path of map keys and sequence indices. */
  readonly paths: RepairStep[][];
}

export type RepairListener = (event: RepairEvent) => void;

export interface DocOptions {
  /** A fixed 16-byte replica id; a random one is minted when omitted. */
  clientId?: Uint8Array;
}

interface Observer {
  readonly prefix: Uint8Array;
  readonly listener: ChangeListener;
}

export class Doc {
  private backend!: Backend;
  private wire?: (bytes: Uint8Array) => void;
  private updateListeners!: Set<UpdateListener>;
  private repairListeners!: Set<RepairListener>;
  private observers!: Set<Observer>;
  private ctx!: HandleContext;
  private transacting = false;

  constructor(options: DocOptions = {}) {
    const clientId = options.clientId ?? randomClientId();
    if (clientId.length !== 16) {
      throw new TypeError(`crdtsync: clientId must be 16 bytes, got ${clientId.length}`);
    }
    this.init(localBackend(new WasmDocument(clientId)));
  }

  /** @internal Build a document over a provider-supplied networked backend. */
  static networked(backend: Backend, wire: (bytes: Uint8Array) => void): Doc {
    const doc = Object.create(Doc.prototype) as Doc;
    doc.init(backend, wire);
    return doc;
  }

  private init(backend: Backend, wire?: (bytes: Uint8Array) => void): void {
    this.backend = backend;
    this.wire = wire;
    this.updateListeners = new Set();
    this.repairListeners = new Set();
    this.observers = new Set();
    this.ctx = {
      backend,
      mutate: (run) => this.mutate(run),
      mutateReturning: (run) => this.mutateReturning(run),
      observe: (prefix, listener) => this.addObserver(prefix, listener),
    };
  }

  /** A live root Map handle at `key`. */
  getMap(key: Key): CrdtMap {
    return new CrdtMap(this.ctx, [key]);
  }

  /** A live root List handle at `key`. */
  getList(key: Key): CrdtList {
    return new CrdtList(this.ctx, [key]);
  }

  /** A live root Text handle at `key`. */
  getText(key: Key): CrdtText {
    return new CrdtText(this.ctx, [key]);
  }

  /** A live root Xml handle at `key`. */
  getXml(key: Key): CrdtXml {
    return new CrdtXml(this.ctx, [key]);
  }

  /** Fold a peer's update ops into this replica. Local documents only — a
   * networked document syncs through its provider, and throws here rather than
   * answering an outcome.
   *
   * The outcome separates an op that did not apply *yet* from one that never
   * will. `applied` counts what the fold took as the ops arrived; one it did not
   * take may be a duplicate, or be waiting — buffered until a create makes its
   * target reachable or its transaction group completes, which a later update
   * does, including one later in this same batch (released that way, it is not
   * counted). `refused` counts what no replica will ever hold, which is a bug in
   * whoever wrote it: a peer reached offline, directly, or over a byte pipe the
   * app carries itself has no server between it and this fold to reject such an
   * op first, so a non-zero `refused` is the only signal the app gets that a
   * peer's edits are dropped for good. A refused op does not hold back the rest
   * of the batch. */
  applyUpdate(ops: Uint8Array): ApplyOutcome {
    const before = this.observing() ? this.backend.encodeState() : undefined;
    const outcome = this.backend.apply(ops);
    if (outcome.applied > 0) {
      this.dispatch("remote", ops, before);
      this.emitRepairs();
    }
    return outcome;
  }

  /** @internal Bracket a provider-driven inbound receive with reactivity. */
  applyRemote(receive: () => void): void {
    const before = this.observing() ? this.backend.encodeState() : undefined;
    receive();
    if (before !== undefined) this.dispatch("remote", EMPTY, before);
    this.emitRepairs();
  }

  /** Subscribe to applied changes to the whole document (`"update"`), or to the
   * schema-repair signal (`"repair"`, fires only once a schema is bound). */
  on(event: "update", listener: UpdateListener): void;
  on(event: "repair", listener: RepairListener): void;
  on(event: "update" | "repair", listener: UpdateListener | RepairListener): void {
    if (event === "update") this.updateListeners.add(listener as UpdateListener);
    else if (event === "repair") this.repairListeners.add(listener as RepairListener);
  }

  /** Unsubscribe a listener registered with `on`. */
  off(event: "update", listener: UpdateListener): void;
  off(event: "repair", listener: RepairListener): void;
  off(event: "update" | "repair", listener: UpdateListener | RepairListener): void {
    if (event === "update") this.updateListeners.delete(listener as UpdateListener);
    else if (event === "repair") this.repairListeners.delete(listener as RepairListener);
  }

  /** Bind a schema (its JSON, as UTF-8 bytes) to this replica, returning whether
   * it bound. A bound schema gives named marks their declared formatting flavor —
   * a boolean, value, or object mark instead of the default object-flavor range
   * annotation — and turns on the `"repair"` signal: after an edit, the locations
   * whose repaired reading changed against the schema are delivered to `on("repair")`.
   * Non-UTF-8 or invalid-schema bytes bind nothing and return `false`. */
  setSchema(schema: Uint8Array): boolean {
    return this.backend.setSchema(schema);
  }

  /** Serialize the whole replica to a canonical snapshot. */
  encodeState(): Uint8Array {
    return this.backend.encodeState();
  }

  /** Run `fn`'s edits as an atomic group — they apply together on every replica
   * served the zone they fall in, and ride the wire as a single batch, firing one
   * update. Edits spanning two zones form one group per zone, since a transaction
   * stays inside one zone. Nested calls flatten into the outermost transaction. */
  transact(fn: () => void): void {
    if (this.transacting) {
      fn();
      return;
    }
    const before = this.observing() ? this.backend.encodeState() : undefined;
    this.transacting = true;
    this.backend.beginAtomic();
    let body: unknown;
    let threw = false;
    try {
      fn();
    } catch (e) {
      body = e;
      threw = true;
    }
    this.transacting = false;
    // The group is committed and delivered whatever the body did, so a body that
    // threw never strands it open. A listener that throws during that delivery is
    // held rather than propagated: a refusal already on its way out of `fn` is the
    // answer to the edit the application made, and outranks it — carrying it as
    // the cause rather than dropping it.
    const delivery = this.deliver(this.backend.commitAtomic(), before);
    if (threw) {
      // The body's own failure is what the caller asked for — a refusal or anything
      // else it threw — and a delivery failure rides along as its cause rather than
      // replacing it. Attaching is best-effort: a frozen or read-only error still
      // reaches the caller as itself rather than as the `TypeError` the write would
      // raise.
      if (delivery !== undefined && body instanceof Error && body.cause === undefined) {
        try {
          (body as { cause?: unknown }).cause = delivery;
        } catch {
          // The caller's error outranks the annotation.
        }
      }
      throw body;
    }
    if (delivery !== undefined) throw delivery;
  }

  /** Raise the refusal an edit's empty byte string cannot express.
   *
   * `refused` is read straight after the edit, never before and never later: the
   * core clears the latch as each intention opens, so reading it there is what makes
   * it this edit's answer, and reading it after the dispatch block would lose it to
   * a listener that throws. It is *raised* after the ops have gone to the wire and
   * the listeners, though — one backend call is one core transaction, and a refusal
   * cuts it at the edit that could not mint, so a refused call can carry ops that
   * did. Those are applied to this replica already; withholding them would leave it
   * ahead of every peer. */
  private guardMint(refused: boolean, dispatchError?: unknown): void {
    // The refusal outranks a listener's own failure: it is the answer to the call
    // the application made, and losing it is the silence this whole seam removes.
    // The listener's error rides along as the cause rather than disappearing.
    if (refused) throw new MintExhausted(dispatchError);
    if (dispatchError !== undefined) throw dispatchError;
  }

  /** Wire and dispatch what the edit emitted, returning a listener's failure
   * rather than propagating it, so the refusal is still reported. */
  private deliver(outbound: Uint8Array, before: Uint8Array | undefined): unknown {
    if (outbound.length === 0) return undefined;
    try {
      this.wire?.(outbound);
      this.dispatch("local", outbound, before);
      this.emitRepairs();
      return undefined;
    } catch (e) {
      return e === undefined ? new Error("crdtsync: an update listener threw") : e;
    }
  }

  private mutate(run: (backend: Backend) => Uint8Array): void {
    // Inside a transaction the edit just accumulates; the commit sends + dispatches.
    if (this.transacting) {
      run(this.backend);
      this.guardMint(this.backend.mintRefused());
      return;
    }
    const before = this.observing() ? this.backend.encodeState() : undefined;
    const outbound = run(this.backend);
    const refused = this.backend.mintRefused();
    this.guardMint(refused, this.deliver(outbound, before));
  }

  private mutateReturning<T>(run: (backend: Backend) => [T, Uint8Array]): T {
    if (this.transacting) {
      const [value] = run(this.backend);
      this.guardMint(this.backend.mintRefused());
      return value;
    }
    const before = this.observing() ? this.backend.encodeState() : undefined;
    const [value, outbound] = run(this.backend);
    const refused = this.backend.mintRefused();
    this.guardMint(refused, this.deliver(outbound, before));
    return value;
  }

  private addObserver(prefix: Uint8Array, listener: ChangeListener): () => void {
    const observer: Observer = { prefix, listener };
    this.observers.add(observer);
    return () => this.observers.delete(observer);
  }

  private observing(): boolean {
    return this.updateListeners.size > 0 || this.observers.size > 0;
  }

  // Drain the schema-repair signal after a state change and deliver it. Only runs
  // when a `"repair"` listener is attached — the drain reseeds the baseline, so
  // draining unobserved would lose the signal; an unobserved doc pays nothing (and
  // `takeRepairs` is empty until a schema is bound).
  private emitRepairs(): void {
    if (this.repairListeners.size === 0) return;
    const raw = this.backend.takeRepairs();
    if (raw.length === 0) return;
    const paths = raw.map(decodeRepairPath);
    for (const listener of [...this.repairListeners]) listener({ paths });
  }

  private dispatch(origin: ChangeEvent["origin"], ops: Uint8Array, before?: Uint8Array): void {
    const raws = before === undefined ? [] : this.computeChanges(before);
    const changes = raws.map((r) => r.change);

    // Snapshot the sets: a listener that subscribes another during dispatch must
    // not receive this in-flight event. A remote frame that changed nothing (an
    // ack, awareness) fires nothing; a local edit always reports its ops.
    if (origin === "local" || changes.length > 0) {
      for (const listener of [...this.updateListeners]) listener({ origin, ops, changes });
    }
    for (const observer of [...this.observers]) {
      const matched = raws
        .filter((r) => pathStartsWith(r.pathBytes, observer.prefix))
        .map((r) => r.change);
      if (matched.length > 0) observer.listener({ origin, changes: matched });
    }
  }

  private computeChanges(before: Uint8Array) {
    const after = this.backend.encodeState();
    // A missing state (an unheld channel yields an empty buffer) is not a
    // decodable snapshot; treat it as no changes rather than letting `diff` throw.
    if (before.length === 0 || after.length === 0) return [];
    // biome-ignore lint/suspicious/noExplicitAny: the wasm diff returns tagged plain objects
    const diff = WasmDocument.diff(before, after) as any[];
    return diff.map(remarshalChange);
  }
}

function randomClientId(): Uint8Array {
  const id = new Uint8Array(16);
  globalThis.crypto.getRandomValues(id);
  return id;
}
