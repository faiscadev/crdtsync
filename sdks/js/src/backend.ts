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
  encodeState(): Uint8Array;

  setScalar(path: Uint8Array, scalar: Uint8Array): Uint8Array;
  delete(path: Uint8Array): Uint8Array;
  listInsert(path: Uint8Array, index: number, value: Uint8Array): Uint8Array;
  listDelete(path: Uint8Array, index: number): Uint8Array;
  textInsert(path: Uint8Array, index: number, s: string): Uint8Array;
  textDelete(path: Uint8Array, index: number, count: number): Uint8Array;

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

  apply(_ops: Uint8Array): number {
    throw new Error("crdtsync: a networked document syncs through its provider, not applyUpdate");
  }
}
