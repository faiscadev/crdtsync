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
