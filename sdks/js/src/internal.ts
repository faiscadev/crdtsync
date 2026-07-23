// The seam between the handle layer and the document it edits. A handle holds a
// `HandleContext`, not a `Doc` directly, so `doc.ts` and `handles.ts` don't form
// an import cycle and a handle stays a thin view over the raw wasm replica.

import type { WasmDocument } from "./wasm/crdtsync_wasm.js";

export interface HandleContext {
  /** The raw wasm replica the handle reads and edits through. */
  readonly wasm: WasmDocument;
  /** Route a local edit's emitted ops back to the document (observers, sync). */
  emit(ops: Uint8Array): void;
}
