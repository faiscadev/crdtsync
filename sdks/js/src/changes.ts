// The ergonomic change events reactivity delivers, re-marshaled from the core
// diff the wasm `diff` seam reports. A raw diff object addresses a slot by an
// encoded byte-path and carries scalars as tagged `{ t, v }` objects; here they
// become an ergonomic key path and native values, so an observer reads the same
// value types it wrote.

import {
  type DiffScalar,
  type ScalarValue,
  decodeValue,
  intToNative,
  nativeFromDiffScalar,
} from "./marshal.js";
import { decodePath, keyString } from "./path.js";

/** One structural change to the document. `path` is the ergonomic key path to the
 * affected slot (a list/text change adds a live `index`). A mark change targets a
 * sequence by id rather than a path, so it carries no `path`. */
export type Change =
  | { readonly kind: "add" | "remove"; readonly path: string[]; readonly valueKind: string }
  | {
      readonly kind: "update";
      readonly path: string[];
      readonly old: ScalarValue;
      readonly new: ScalarValue;
    }
  | {
      readonly kind: "counter";
      readonly path: string[];
      readonly old: number | bigint;
      readonly new: number | bigint;
    }
  | {
      readonly kind: "listInsert" | "listDelete";
      readonly path: string[];
      readonly index: number;
      readonly values: (ScalarValue | { readonly container: string })[];
    }
  | {
      readonly kind: "textInsert" | "textDelete";
      readonly path: string[];
      readonly index: number;
      readonly text: string;
    }
  | {
      readonly kind: "mark";
      readonly op: "add" | "remove" | "change";
      readonly name: string;
      readonly old?: ScalarValue;
      readonly new?: ScalarValue;
    };

/** A change plus the raw byte-path it targets, kept for observer prefix-matching. */
export interface RawChange {
  readonly pathBytes: Uint8Array;
  readonly change: Change;
}

interface DiffItem {
  scalar?: DiffScalar;
  kind?: string;
}

// The wasm `diff` object: an `op` tag plus the variant's fields. Typed loosely at
// the boundary, then narrowed by `op`. A mark change carries `name`/`value` (and
// `id`/`seq`) but no `path`.
interface DiffObject {
  op: string;
  path?: Uint8Array;
  kind?: string;
  old?: DiffScalar | bigint;
  new?: DiffScalar | bigint;
  index?: number;
  items?: DiffItem[];
  text?: string;
  name?: Uint8Array;
  value?: DiffScalar;
}

const EMPTY = new Uint8Array();

// The mark diff ops carry a `value` (add/remove) or `old`/`new` (change), and no
// path — an observer never subtree-matches them (empty path), but Doc.on("update")
// still receives them.
function markChange(raw: DiffObject): Change {
  const name = keyString(raw.name ?? EMPTY);
  switch (raw.op) {
    case "markAdd":
      return { kind: "mark", op: "add", name, new: nativeFromDiffScalar(raw.value as DiffScalar) };
    case "markRemove":
      return {
        kind: "mark",
        op: "remove",
        name,
        old: nativeFromDiffScalar(raw.value as DiffScalar),
      };
    default: // markChange
      return {
        kind: "mark",
        op: "change",
        name,
        old: nativeFromDiffScalar(raw.old as DiffScalar),
        new: nativeFromDiffScalar(raw.new as DiffScalar),
      };
  }
}

function item(i: DiffItem): ScalarValue | { container: string } {
  // A list item's scalar rides as enveloped bytes (a `Scalar::Bytes` wrapping the
  // SDK-encoded scalar), so it decodes through `decodeValue`, not the native path.
  if (i.scalar) return decodeValue(i.scalar.v as Uint8Array);
  return { container: i.kind ?? "unknown" };
}

/** Re-marshal one wasm diff object into an ergonomic change (with its byte-path).
 * Mark ops carry no path, so the decode is guarded — a diff containing a mark must
 * not crash the whole update. */
export function remarshalChange(raw: DiffObject): RawChange {
  if (raw.op === "markAdd" || raw.op === "markRemove" || raw.op === "markChange") {
    return { pathBytes: EMPTY, change: markChange(raw) };
  }
  const pathBytes = raw.path ?? EMPTY;
  const path = decodePath(pathBytes);
  let change: Change;
  switch (raw.op) {
    case "remove":
      change = { kind: "remove", path, valueKind: raw.kind ?? "unknown" };
      break;
    case "value":
      change = {
        kind: "update",
        path,
        old: nativeFromDiffScalar(raw.old as DiffScalar),
        new: nativeFromDiffScalar(raw.new as DiffScalar),
      };
      break;
    case "counter":
      change = {
        kind: "counter",
        path,
        old: intToNative(raw.old as bigint),
        new: intToNative(raw.new as bigint),
      };
      break;
    case "listInsert":
    case "listDelete":
      change = {
        kind: raw.op,
        path,
        index: raw.index ?? 0,
        values: (raw.items ?? []).map(item),
      };
      break;
    case "textInsert":
    case "textDelete":
      change = { kind: raw.op, path, index: raw.index ?? 0, text: raw.text ?? "" };
      break;
    default:
      // "add", and any future path-bearing op: expose the target + the op name.
      change = { kind: "add", path, valueKind: raw.kind ?? raw.op };
      break;
  }
  return { pathBytes, change };
}
