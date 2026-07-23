// The storage/wire seam a `Doc` edits and reads through. A local document is
// backed directly by a `WasmDocument` (which already exposes exactly this method
// set); a networked document is backed by a channel of a `WasmClient`, so edits
// are framed + outboxed for the wire and reads query the channel's replica — one
// replica per room, never two divergent copies. An edit returns the bytes it
// produced: raw ops for a local backend, a wire Ops frame for a client backend.

import type { WasmClient, WasmDocument } from "./wasm/crdtsync_wasm.js";

export interface Backend {
  getScalar(path: Uint8Array): Uint8Array | undefined;
  mapKeys(path: Uint8Array): Uint8Array[] | undefined;
  listLen(path: Uint8Array): number | undefined;
  listGet(path: Uint8Array, index: number): Uint8Array | undefined;
  textLen(path: Uint8Array): number | undefined;
  textGet(path: Uint8Array): string | undefined;
  relativePosition(path: Uint8Array, index: number, side: number): Uint8Array | undefined;
  resolvePosition(path: Uint8Array, pos: Uint8Array): number | undefined;
  encodeState(): Uint8Array;

  setScalar(path: Uint8Array, scalar: Uint8Array): Uint8Array;
  delete(path: Uint8Array): Uint8Array;
  listInsert(path: Uint8Array, index: number, value: Uint8Array): Uint8Array;
  listDelete(path: Uint8Array, index: number): Uint8Array;
  textInsert(path: Uint8Array, index: number, s: string): Uint8Array;
  textDelete(path: Uint8Array, index: number, count: number): Uint8Array;

  setBlob(path: Uint8Array, mime: string, bytes: Uint8Array): Uint8Array | undefined;
  setBlobRef(path: Uint8Array, id: Uint8Array, mime: string, size: bigint): Uint8Array;
  getBlob(path: Uint8Array): unknown;

  /** Author a mark; returns its handle id and the ops/frame to broadcast. */
  mark(
    path: Uint8Array,
    startIndex: number,
    startSide: number,
    endIndex: number,
    endSide: number,
    name: Uint8Array,
    value: Uint8Array,
  ): { id?: Uint8Array; ops: Uint8Array };
  markSetValue(markId: Uint8Array, value: Uint8Array): Uint8Array;
  markDelete(markId: Uint8Array): Uint8Array;
  marksAt(path: Uint8Array, index: number): unknown;

  xmlElement(path: Uint8Array, tag: Uint8Array): Uint8Array;
  xmlFragment(path: Uint8Array): Uint8Array;
  xmlTag(path: Uint8Array): Uint8Array | undefined;
  xmlChildrenLen(path: Uint8Array): number | undefined;
  xmlInsertElement(path: Uint8Array, index: number, tag: Uint8Array): Uint8Array;
  xmlInsertText(path: Uint8Array, index: number, s: string): Uint8Array;
  xmlChildDelete(path: Uint8Array, index: number): Uint8Array;
  xmlMove(
    parent: Uint8Array,
    childIndex: number,
    newParent: Uint8Array,
    destIndex: number,
  ): Uint8Array;

  /** Begin an atomic group; edits accumulate until `commitAtomic`. */
  beginAtomic(): void;
  /** Commit the atomic group, returning its combined ops/frame. */
  commitAtomic(): Uint8Array;

  /** Fold a peer's ops in (local only); a client backend syncs through its provider. */
  apply(ops: Uint8Array): number;
}

/** `WasmDocument` already implements the `Backend` shape 1:1. */
export function localBackend(wasm: WasmDocument): Backend {
  return wasm as unknown as Backend;
}

/** A `Backend` over one channel of a `WasmClient`: edits frame + outbox for the
 * wire, reads query the channel replica. */
export class ClientBackend implements Backend {
  constructor(
    private readonly client: WasmClient,
    private readonly channel: number,
  ) {}

  getScalar(path: Uint8Array): Uint8Array | undefined {
    return this.client.getScalar(this.channel, path);
  }
  mapKeys(path: Uint8Array): Uint8Array[] | undefined {
    return this.client.mapKeys(this.channel, path);
  }
  listLen(path: Uint8Array): number | undefined {
    return this.client.listLen(this.channel, path);
  }
  listGet(path: Uint8Array, index: number): Uint8Array | undefined {
    return this.client.listGet(this.channel, path, index);
  }
  textLen(path: Uint8Array): number | undefined {
    return this.client.textLen(this.channel, path);
  }
  textGet(path: Uint8Array): string | undefined {
    return this.client.textGet(this.channel, path);
  }
  relativePosition(path: Uint8Array, index: number, side: number): Uint8Array | undefined {
    return this.client.relativePosition(this.channel, path, index, side);
  }
  resolvePosition(path: Uint8Array, pos: Uint8Array): number | undefined {
    return this.client.resolvePosition(this.channel, path, pos);
  }
  encodeState(): Uint8Array {
    return this.client.channelState(this.channel) ?? new Uint8Array();
  }

  setScalar(path: Uint8Array, scalar: Uint8Array): Uint8Array {
    return this.client.setScalar(this.channel, path, scalar);
  }
  delete(path: Uint8Array): Uint8Array {
    return this.client.delete(this.channel, path);
  }
  listInsert(path: Uint8Array, index: number, value: Uint8Array): Uint8Array {
    return this.client.listInsert(this.channel, path, index, value);
  }
  listDelete(path: Uint8Array, index: number): Uint8Array {
    return this.client.listDelete(this.channel, path, index);
  }
  textInsert(path: Uint8Array, index: number, s: string): Uint8Array {
    return this.client.textInsert(this.channel, path, index, s);
  }
  textDelete(path: Uint8Array, index: number, count: number): Uint8Array {
    return this.client.textDelete(this.channel, path, index, count);
  }

  setBlob(path: Uint8Array, mime: string, bytes: Uint8Array): Uint8Array | undefined {
    return this.client.setBlob(this.channel, path, mime, bytes);
  }
  setBlobRef(path: Uint8Array, id: Uint8Array, mime: string, size: bigint): Uint8Array {
    return this.client.setBlobRef(this.channel, path, id, mime, size);
  }
  getBlob(path: Uint8Array): unknown {
    return this.client.getBlob(this.channel, path);
  }
  mark(
    path: Uint8Array,
    startIndex: number,
    startSide: number,
    endIndex: number,
    endSide: number,
    name: Uint8Array,
    value: Uint8Array,
  ): { id?: Uint8Array; ops: Uint8Array } {
    return this.client.mark(
      this.channel,
      path,
      startIndex,
      startSide,
      endIndex,
      endSide,
      name,
      value,
    );
  }
  markSetValue(markId: Uint8Array, value: Uint8Array): Uint8Array {
    return this.client.markSetValue(this.channel, markId, value);
  }
  markDelete(markId: Uint8Array): Uint8Array {
    return this.client.markDelete(this.channel, markId);
  }
  marksAt(path: Uint8Array, index: number): unknown {
    return this.client.marksAt(this.channel, path, index);
  }

  xmlElement(path: Uint8Array, tag: Uint8Array): Uint8Array {
    return this.client.xmlElement(this.channel, path, tag);
  }
  xmlFragment(path: Uint8Array): Uint8Array {
    return this.client.xmlFragment(this.channel, path);
  }
  xmlTag(path: Uint8Array): Uint8Array | undefined {
    return this.client.xmlTag(this.channel, path);
  }
  xmlChildrenLen(path: Uint8Array): number | undefined {
    return this.client.xmlChildrenLen(this.channel, path);
  }
  xmlInsertElement(path: Uint8Array, index: number, tag: Uint8Array): Uint8Array {
    return this.client.xmlInsertElement(this.channel, path, index, tag);
  }
  xmlInsertText(path: Uint8Array, index: number, s: string): Uint8Array {
    return this.client.xmlInsertText(this.channel, path, index, s);
  }
  xmlChildDelete(path: Uint8Array, index: number): Uint8Array {
    return this.client.xmlChildDelete(this.channel, path, index);
  }
  xmlMove(
    parent: Uint8Array,
    childIndex: number,
    newParent: Uint8Array,
    destIndex: number,
  ): Uint8Array {
    return this.client.xmlMove(this.channel, parent, childIndex, newParent, destIndex);
  }

  beginAtomic(): void {
    this.client.beginAtomic(this.channel);
  }
  commitAtomic(): Uint8Array {
    return this.client.commitAtomic(this.channel);
  }

  apply(_ops: Uint8Array): number {
    throw new Error("crdtsync: a networked document syncs through its provider, not applyUpdate");
  }
}
