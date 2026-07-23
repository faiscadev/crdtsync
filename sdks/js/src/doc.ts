// A `Doc` is a local CRDT replica with a single root map. Editing is done
// through live typed handles obtained from the root (`getMap`/`getList`/
// `getText`); the byte-path core underneath stays hidden. A `Doc` is a pure
// local replica until it is bound to a sync provider (a later slice) — two docs
// that exchange each other's update ops converge.

import { CrdtList, CrdtMap, CrdtText } from "./handles.js";
import type { HandleContext } from "./internal.js";
import type { Key } from "./path.js";
import { WasmDocument } from "./wasm/crdtsync_wasm.js";

/** An applied change to the document, delivered to `Doc.on("update")`. */
export interface UpdateEvent {
  /** `"local"` for an edit made on this replica, `"remote"` for an applied peer update. */
  readonly origin: "local" | "remote";
  /** The encoded ops the change produced — the bytes to broadcast, or that arrived. */
  readonly ops: Uint8Array;
}

export type UpdateListener = (event: UpdateEvent) => void;

export interface DocOptions {
  /** A fixed 16-byte replica id; a random one is minted when omitted. */
  clientId?: Uint8Array;
}

export class Doc {
  private readonly wasm: WasmDocument;
  private readonly updateListeners = new Set<UpdateListener>();
  private readonly ctx: HandleContext;

  constructor(options: DocOptions = {}) {
    const clientId = options.clientId ?? randomClientId();
    if (clientId.length !== 16) {
      throw new TypeError(`crdtsync: clientId must be 16 bytes, got ${clientId.length}`);
    }
    this.wasm = new WasmDocument(clientId);
    this.ctx = {
      wasm: this.wasm,
      emit: (ops) => this.onLocalOps(ops),
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
    const applied = this.wasm.apply(ops);
    if (applied > 0) this.notify({ origin: "remote", ops });
    return applied;
  }

  /** Subscribe to applied changes (local edits and remote updates). */
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

  private onLocalOps(ops: Uint8Array): void {
    if (ops.length === 0) return;
    this.notify({ origin: "local", ops });
  }

  private notify(event: UpdateEvent): void {
    for (const listener of this.updateListeners) listener(event);
  }
}

function randomClientId(): Uint8Array {
  const id = new Uint8Array(16);
  globalThis.crypto.getRandomValues(id);
  return id;
}
