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

import { type Backend, localBackend } from "./backend.js";
import { type Change, remarshalChange } from "./changes.js";
import { CrdtList, CrdtMap, CrdtText, CrdtXml } from "./handles.js";
import type { ChangeEvent, ChangeListener, HandleContext } from "./internal.js";
import { type Key, pathStartsWith } from "./path.js";
import { WasmDocument } from "./wasm/crdtsync_wasm.js";

export type { Change } from "./changes.js";
export type { ChangeEvent, ChangeListener } from "./internal.js";

const EMPTY = new Uint8Array();

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

  /** Fold a peer's update ops into this replica; returns the count applied.
   * Local documents only — a networked document syncs through its provider. */
  applyUpdate(ops: Uint8Array): number {
    const before = this.observing() ? this.backend.encodeState() : undefined;
    const applied = this.backend.apply(ops);
    if (applied > 0) this.dispatch("remote", ops, before);
    return applied;
  }

  /** @internal Bracket a provider-driven inbound receive with reactivity. */
  applyRemote(receive: () => void): void {
    const before = this.observing() ? this.backend.encodeState() : undefined;
    receive();
    if (before !== undefined) this.dispatch("remote", EMPTY, before);
  }

  /** Subscribe to applied changes to the whole document. */
  on(event: "update", listener: UpdateListener): void {
    if (event === "update") this.updateListeners.add(listener);
  }

  /** Unsubscribe a listener registered with `on`. */
  off(event: "update", listener: UpdateListener): void {
    if (event === "update") this.updateListeners.delete(listener);
  }

  /** Serialize the whole replica to a canonical snapshot. */
  encodeState(): Uint8Array {
    return this.backend.encodeState();
  }

  /** Run `fn`'s edits as one atomic group — they apply together on every replica
   * and ride the wire as a single batch, firing one update. Nested calls flatten
   * into the outermost transaction. */
  transact(fn: () => void): void {
    if (this.transacting) {
      fn();
      return;
    }
    const before = this.observing() ? this.backend.encodeState() : undefined;
    this.transacting = true;
    this.backend.beginAtomic();
    try {
      fn();
    } finally {
      this.transacting = false;
      const outbound = this.backend.commitAtomic();
      if (outbound.length > 0) {
        this.wire?.(outbound);
        this.dispatch("local", outbound, before);
      }
    }
  }

  private mutate(run: (backend: Backend) => Uint8Array): void {
    // Inside a transaction the edit just accumulates; the commit sends + dispatches.
    if (this.transacting) {
      run(this.backend);
      return;
    }
    const before = this.observing() ? this.backend.encodeState() : undefined;
    const outbound = run(this.backend);
    if (outbound.length === 0) return;
    this.wire?.(outbound);
    this.dispatch("local", outbound, before);
  }

  private mutateReturning<T>(run: (backend: Backend) => [T, Uint8Array]): T {
    if (this.transacting) {
      const [value] = run(this.backend);
      return value;
    }
    const before = this.observing() ? this.backend.encodeState() : undefined;
    const [value, outbound] = run(this.backend);
    if (outbound.length > 0) {
      this.wire?.(outbound);
      this.dispatch("local", outbound, before);
    }
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
