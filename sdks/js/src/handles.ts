// Live typed handles into the document's value graph. A handle owns its logical
// path (a sequence of ergonomic keys) and re-resolves it on every operation, so
// it stays valid as the document mutates and converges — it is a view, never a
// cached pointer. Handles compose: a map yields nested map/list/text handles by
// key. Reads reflect the current converged state; writes apply immediately and
// their ops flow to observers and any bound provider through the context.

import type { ChangeListener, HandleContext } from "./internal.js";
import { type ScalarValue, decodeValue, encodeValue } from "./marshal.js";
import { type Key, encodePath, keyString } from "./path.js";

/** A value read from a slot: a marshaled scalar, or a nested container handle. */
export type Value = ScalarValue | CrdtMap | CrdtList | CrdtText;

/** An opaque, stable position in a sequence — a cursor captured with
 * `relativePosition` and resolved back to a live index with `resolve`. It tracks
 * its spot across concurrent inserts and deletes, the anchor collaborative
 * editors bind a selection to. */
export type RelativePosition = Uint8Array & { readonly __rel: unique symbol };

/** Which side of the index a captured position anchors to: `"before"` stays left
 * of the index (a left-gravity anchor), `"after"` stays right of it. */
export type CursorSide = "before" | "after";

function sideCode(side: CursorSide): number {
  return side === "after" ? 1 : 0;
}

/** A live handle to a Map slot, addressed by ergonomic keys. */
export class CrdtMap {
  /** @internal */
  constructor(
    private readonly ctx: HandleContext,
    private readonly path: readonly Key[],
  ) {}

  private slot(key: Key): Uint8Array {
    return encodePath([...this.path, key]);
  }

  /** Set a leaf to a native scalar value. Rejects a plain object/array — a
   * nested container is created with `getMap`/`getList`/`getText`. */
  set(key: Key, value: ScalarValue): this {
    const slot = this.slot(key);
    const bytes = encodeValue(value);
    this.ctx.mutate((w) => w.setScalar(slot, bytes));
    return this;
  }

  /** The container kind a slot holds, or `undefined` if it holds no container. */
  private containerKind(slot: Uint8Array): "map" | "list" | "text" | undefined {
    if (this.ctx.backend.mapKeys(slot) !== undefined) return "map";
    if (this.ctx.backend.listLen(slot) !== undefined) return "list";
    if (this.ctx.backend.textLen(slot) !== undefined) return "text";
    return undefined;
  }

  /** Read a slot: a scalar value, a nested container handle, or `undefined`. */
  get(key: Key): Value | undefined {
    const slot = this.slot(key);
    const scalar = this.ctx.backend.getScalar(slot);
    if (scalar !== undefined) return decodeValue(scalar);
    const child: readonly Key[] = [...this.path, key];
    switch (this.containerKind(slot)) {
      case "map":
        return new CrdtMap(this.ctx, child);
      case "list":
        return new CrdtList(this.ctx, child);
      case "text":
        return new CrdtText(this.ctx, child);
      default:
        return undefined;
    }
  }

  /** Whether the key names a live slot (scalar or container). */
  has(key: Key): boolean {
    const slot = this.slot(key);
    return this.ctx.backend.getScalar(slot) !== undefined || this.containerKind(slot) !== undefined;
  }

  /** Tombstone the slot at `key`. */
  delete(key: Key): this {
    const slot = this.slot(key);
    this.ctx.mutate((w) => w.delete(slot));
    return this;
  }

  /** A nested Map handle at `key` (materializes on first nested write). */
  getMap(key: Key): CrdtMap {
    return new CrdtMap(this.ctx, [...this.path, key]);
  }

  /** A nested List handle at `key`. */
  getList(key: Key): CrdtList {
    return new CrdtList(this.ctx, [...this.path, key]);
  }

  /** A nested Text handle at `key`. */
  getText(key: Key): CrdtText {
    return new CrdtText(this.ctx, [...this.path, key]);
  }

  /** The raw live slot keys, as the core stores them. */
  private rawKeys(): Uint8Array[] {
    return this.ctx.backend.mapKeys(encodePath(this.path)) ?? [];
  }

  /** The live slot keys, as strings (utf-8). */
  keys(): string[] {
    return this.rawKeys().map(keyString);
  }

  /** The live `[key, value]` pairs. Values are read by the raw key bytes, so a
   * non-utf-8 (binary) key's value is never lost even though the key renders as
   * a best-effort utf-8 string. */
  entries(): [string, Value | undefined][] {
    return this.rawKeys().map((k) => [keyString(k), this.get(k)]);
  }

  /** The number of live slots. */
  get size(): number {
    return this.rawKeys().length;
  }

  [Symbol.iterator](): IterableIterator<[string, Value | undefined]> {
    return this.entries()[Symbol.iterator]();
  }

  /** Observe changes to this map's subtree (local edits and remote updates).
   * Returns an unsubscribe function. */
  observe(listener: ChangeListener): () => void {
    return this.ctx.observe(encodePath(this.path), listener);
  }
}

/** A live handle to a List of scalar items, addressed by live index. */
export class CrdtList {
  /** @internal */
  constructor(
    private readonly ctx: HandleContext,
    private readonly path: readonly Key[],
  ) {}

  private get self(): Uint8Array {
    return encodePath(this.path);
  }

  /** Insert a scalar item at a live index. The marshaled scalar rides in the
   * item's bytes; a list holds opaque item bytes, so the value's type is carried
   * by the SDK encoding, not a native register. */
  insert(index: number, value: ScalarValue): this {
    const self = this.self;
    const bytes = encodeValue(value);
    this.ctx.mutate((w) => w.listInsert(self, index, bytes));
    return this;
  }

  /** Append a scalar item. */
  push(value: ScalarValue): this {
    return this.insert(this.length, value);
  }

  /** Tombstone the live item at `index`. */
  delete(index: number): this {
    const self = this.self;
    this.ctx.mutate((w) => w.listDelete(self, index));
    return this;
  }

  /** Read the scalar item at a live index, or `undefined`. */
  get(index: number): ScalarValue | undefined {
    const item = this.ctx.backend.listGet(this.self, index);
    return item === undefined ? undefined : decodeValue(item);
  }

  /** The number of live items. */
  get length(): number {
    return this.ctx.backend.listLen(this.self) ?? 0;
  }

  /** A plain array snapshot of the live items. */
  toArray(): (ScalarValue | undefined)[] {
    return [...this];
  }

  *[Symbol.iterator](): IterableIterator<ScalarValue | undefined> {
    const n = this.length;
    for (let i = 0; i < n; i++) yield this.get(i);
  }

  /** Capture a stable cursor at a live index, resolved later with `resolve`. */
  relativePosition(index: number, side: CursorSide = "before"): RelativePosition | undefined {
    return this.ctx.backend.relativePosition(this.self, index, sideCode(side)) as
      | RelativePosition
      | undefined;
  }

  /** Resolve a captured cursor back to a live index, or `undefined` if it can't. */
  resolve(pos: RelativePosition): number | undefined {
    return this.ctx.backend.resolvePosition(this.self, pos);
  }

  /** Observe changes to this list (local edits and remote updates). Returns an
   * unsubscribe function. */
  observe(listener: ChangeListener): () => void {
    return this.ctx.observe(this.self, listener);
  }
}

/** A live handle to a collaborative Text run, indexed by codepoint. */
export class CrdtText {
  /** @internal */
  constructor(
    private readonly ctx: HandleContext,
    private readonly path: readonly Key[],
  ) {}

  private get self(): Uint8Array {
    return encodePath(this.path);
  }

  /** Insert `str` at a codepoint index. */
  insert(index: number, str: string): this {
    const self = this.self;
    this.ctx.mutate((w) => w.textInsert(self, index, str));
    return this;
  }

  /** Tombstone `count` codepoints from `index`. */
  delete(index: number, count: number): this {
    const self = this.self;
    this.ctx.mutate((w) => w.textDelete(self, index, count));
    return this;
  }

  /** The current text. */
  toString(): string {
    return this.ctx.backend.textGet(this.self) ?? "";
  }

  /** The codepoint length. */
  get length(): number {
    return this.ctx.backend.textLen(this.self) ?? 0;
  }

  /** Capture a stable cursor at a codepoint index, resolved later with `resolve`.
   * The cursor tracks its spot as text is inserted and deleted around it. */
  relativePosition(index: number, side: CursorSide = "before"): RelativePosition | undefined {
    return this.ctx.backend.relativePosition(this.self, index, sideCode(side)) as
      | RelativePosition
      | undefined;
  }

  /** Resolve a captured cursor back to a live codepoint index, or `undefined`. */
  resolve(pos: RelativePosition): number | undefined {
    return this.ctx.backend.resolvePosition(this.self, pos);
  }

  /** Observe changes to this text (local edits and remote updates). Returns an
   * unsubscribe function. */
  observe(listener: ChangeListener): () => void {
    return this.ctx.observe(this.self, listener);
  }
}
