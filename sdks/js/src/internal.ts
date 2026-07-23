// The seam between the handle layer and the document it edits. A handle holds a
// `HandleContext`, not a `Doc` directly, so `doc.ts` and `handles.ts` don't form
// an import cycle and a handle stays a thin view over the raw wasm replica.

import type { Change } from "./changes.js";
import type { WasmDocument } from "./wasm/crdtsync_wasm.js";

/** A change notification for an observed subtree or the whole document. */
export interface ChangeEvent {
  /** `"local"` for an edit on this replica, `"remote"` for an applied peer update. */
  readonly origin: "local" | "remote";
  /** The structural changes the edit produced. */
  readonly changes: Change[];
}

export type ChangeListener = (event: ChangeEvent) => void;

export interface HandleContext {
  /** The raw wasm replica the handle reads and edits through. */
  readonly wasm: WasmDocument;
  /** Run a local edit; route its ops + diff-derived changes to observers/sync. */
  mutate(run: (wasm: WasmDocument) => Uint8Array): void;
  /** Observe changes to the subtree at `pathBytes`; returns an unsubscribe. */
  observe(pathBytes: Uint8Array, listener: ChangeListener): () => void;
}
