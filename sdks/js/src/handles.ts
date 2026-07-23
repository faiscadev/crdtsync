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
    if (this.ctx.wasm.mapKeys(slot) !== undefined) return "map";
    if (this.ctx.wasm.listLen(slot) !== undefined) return "list";
    if (this.ctx.wasm.textLen(slot) !== undefined) return "text";
    return undefined;
  }

  /** Read a slot: a scalar value, a nested container handle, or `undefined`. */
  get(key: Key): Value | undefined {
    const slot = this.slot(key);
    const scalar = this.ctx.wasm.getScalar(slot);
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
    return this.ctx.wasm.getScalar(slot) !== undefined || this.containerKind(slot) !== undefined;
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
    return this.ctx.wasm.mapKeys(encodePath(this.path)) ?? [];
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
    const item = this.ctx.wasm.listGet(this.self, index);
    return item === undefined ? undefined : decodeValue(item);
  }

  /** The number of live items. */
  get length(): number {
    return this.ctx.wasm.listLen(this.self) ?? 0;
  }

  /** A plain array snapshot of the live items. */
  toArray(): (ScalarValue | undefined)[] {
    return [...this];
  }

  *[Symbol.iterator](): IterableIterator<ScalarValue | undefined> {
    const n = this.length;
    for (let i = 0; i < n; i++) yield this.get(i);
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
    return this.ctx.wasm.textGet(this.self) ?? "";
  }

  /** The codepoint length. */
  get length(): number {
    return this.ctx.wasm.textLen(this.self) ?? 0;
  }

  /** Observe changes to this text (local edits and remote updates). Returns an
   * unsubscribe function. */
  observe(listener: ChangeListener): () => void {
    return this.ctx.observe(this.self, listener);
  }
}
