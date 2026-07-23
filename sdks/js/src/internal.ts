// The seam between the handle layer and the document it edits. A handle holds a
// `HandleContext`, not a `Doc` directly, so `doc.ts` and `handles.ts` don't form
// an import cycle and a handle stays a thin view over the raw wasm replica.

import type { Backend } from "./backend.js";
import type { Change } from "./changes.js";

/** A change notification for an observed subtree or the whole document. */
export interface ChangeEvent {
  /** `"local"` for an edit on this replica, `"remote"` for an applied peer update. */
  readonly origin: "local" | "remote";
  /** The structural changes the edit produced. */
  readonly changes: Change[];
}

export type ChangeListener = (event: ChangeEvent) => void;

export interface HandleContext {
  /** The backend the handle reads through (a local replica or a wire channel). */
  readonly backend: Backend;
  /** Run a local edit; route its ops/frame + diff-derived changes to observers/sync. */
  mutate(run: (backend: Backend) => Uint8Array): void;
  /** Run a local edit that also yields a value (e.g. a mark id); route the ops. */
  mutateReturning<T>(run: (backend: Backend) => [T, Uint8Array]): T;
  /** Observe changes to the subtree at `pathBytes`; returns an unsubscribe. */
  observe(pathBytes: Uint8Array, listener: ChangeListener): () => void;
}
