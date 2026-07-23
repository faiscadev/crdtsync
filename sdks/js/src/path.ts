// Path encoding: a slot is addressed inside the wasm core by a length-framed
// sequence of byte keys (each a u32 little-endian length then the key bytes) —
// the core's `path::encode_path`. Handles carry their path as ergonomic keys
// (strings, or raw `Uint8Array`) and encode it here on every operation, so the
// byte-path never surfaces to the caller.

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** An ergonomic map key: a string (utf-8) or raw bytes. */
export type Key = string | Uint8Array;

export function keyBytes(key: Key): Uint8Array {
  return typeof key === "string" ? encoder.encode(key) : key;
}

/** A string key read back from the core (utf-8). */
export function keyString(bytes: Uint8Array): string {
  return decoder.decode(bytes);
}

/** Decode a length-framed path buffer (as the diff machinery reports) into its
 * keys, rendered as best-effort utf-8 strings. */
export function decodePath(bytes: Uint8Array): string[] {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const keys: string[] = [];
  let i = 0;
  while (i < bytes.length) {
    const len = view.getUint32(i, true);
    i += 4;
    keys.push(keyString(bytes.subarray(i, i + len)));
    i += len;
  }
  return keys;
}

/** Whether `whole`'s framed bytes begin with `prefix` — a key-path prefix test,
 * sound because each key is self-delimiting (length + bytes). */
export function pathStartsWith(whole: Uint8Array, prefix: Uint8Array): boolean {
  if (prefix.length > whole.length) return false;
  for (let i = 0; i < prefix.length; i++) {
    if (whole[i] !== prefix[i]) return false;
  }
  return true;
}

/** Encode a key path to the length-framed buffer the wasm methods expect. */
export function encodePath(keys: readonly Key[]): Uint8Array {
  const parts = keys.map(keyBytes);
  const total = parts.reduce((n, p) => n + 4 + p.length, 0);
  const out = new Uint8Array(total);
  const view = new DataView(out.buffer);
  let i = 0;
  for (const p of parts) {
    view.setUint32(i, p.length, true);
    i += 4;
    out.set(p, i);
    i += p.length;
  }
  return out;
}
