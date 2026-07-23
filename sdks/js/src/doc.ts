// A `Doc` is a local CRDT replica with a single root map. Editing is done
// through live typed handles obtained from the root (`getMap`/`getList`/
// `getText`); the byte-path core underneath stays hidden. A `Doc` is a pure
// local replica until it is bound to a sync provider (a later slice) — two docs
// that exchange each other's update ops converge.
//
// Reactivity is diff-derived: an edit (local or an applied remote update) is
// bracketed by a state snapshot, and the core `diff` between the before and
// after states is re-marshaled into ergonomic change events. The snapshot/diff
// only runs when something is listening, so an unobserved document pays nothing.

import { type Change, remarshalChange } from "./changes.js";
import { CrdtList, CrdtMap, CrdtText } from "./handles.js";
import type { ChangeEvent, ChangeListener, HandleContext } from "./internal.js";
import { type Key, pathStartsWith } from "./path.js";
import { WasmDocument } from "./wasm/crdtsync_wasm.js";

export type { Change } from "./changes.js";
export type { ChangeEvent, ChangeListener } from "./internal.js";

/** An applied change to the document, delivered to `Doc.on("update")`. */
export interface UpdateEvent {
  /** `"local"` for an edit made on this replica, `"remote"` for an applied peer update. */
  readonly origin: "local" | "remote";
  /** The encoded ops the change produced — the bytes to broadcast, or that arrived. */
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
  private readonly wasm: WasmDocument;
  private readonly updateListeners = new Set<UpdateListener>();
  private readonly observers = new Set<Observer>();
  private readonly ctx: HandleContext;

  constructor(options: DocOptions = {}) {
    const clientId = options.clientId ?? randomClientId();
    if (clientId.length !== 16) {
      throw new TypeError(`crdtsync: clientId must be 16 bytes, got ${clientId.length}`);
    }
    this.wasm = new WasmDocument(clientId);
    this.ctx = {
      wasm: this.wasm,
      mutate: (run) => this.mutate(run),
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

  /** Fold a peer's update ops into this replica; returns the count applied. */
  applyUpdate(ops: Uint8Array): number {
    const before = this.observing() ? this.wasm.encodeState() : undefined;
    const applied = this.wasm.apply(ops);
    if (applied > 0) this.dispatch("remote", ops, before);
    return applied;
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
    return this.wasm.encodeState();
  }

  private mutate(run: (wasm: WasmDocument) => Uint8Array): void {
    const before = this.observing() ? this.wasm.encodeState() : undefined;
    const ops = run(this.wasm);
    if (ops.length === 0) return;
    this.dispatch("local", ops, before);
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

    // Snapshot the listener/observer sets: a listener that subscribes another
    // during dispatch must not receive this in-flight event.
    for (const listener of [...this.updateListeners]) listener({ origin, ops, changes });

    for (const observer of [...this.observers]) {
      const matched = raws
        .filter((r) => pathStartsWith(r.pathBytes, observer.prefix))
        .map((r) => r.change);
      if (matched.length > 0) observer.listener({ origin, changes: matched });
    }
  }

  private computeChanges(before: Uint8Array) {
    const after = this.wasm.encodeState();
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
