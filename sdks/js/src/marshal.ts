// Native JS value <-> CRDT scalar marshaling. A leaf holds one scalar; the
// mapping is the pinned contract (ARCHITECTURE §SDK-Ergonomic-Surface):
//
//   string   <-> Scalar::Bytes (utf-8)
//   number   <-> Scalar::Int          (integer only; a float has no lossless scalar)
//   bigint   <-> Scalar::Int          (full 64-bit range)
//   boolean  <-> Scalar::Bool
//   null     <-> Scalar::Null
//   Uint8Array <-> Scalar::Bytes (raw)
//
// `string` and `Uint8Array` both land in `Scalar::Bytes`, which the core cannot
// itself tell apart, so the SDK prefixes the bytes with a one-byte discriminator
// (string vs binary) — an SDK framing detail, invisible to the value the caller
// gets back. A plain object/array is rejected: containers are created with the
// explicit `getMap`/`getList`/`getText` accessors, never seeded implicitly
// (the explicit leaf-vs-container boundary; deep-seed is a rejected non-goal).

import { decodeScalar, encodeScalar } from "./scalar.js";

const BINARY = 0x00;
const STRING = 0x01;

/** A value the SDK stores in a leaf slot. */
export type ScalarValue = string | number | bigint | boolean | null | Uint8Array;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Marshal a native value into the encoded-scalar bytes the wasm layer stores. */
export function encodeValue(value: ScalarValue): Uint8Array {
  if (value === null) return encodeScalar({ type: "null" });
  if (typeof value === "boolean") return encodeScalar({ type: "bool", value });
  if (typeof value === "bigint") return encodeScalar({ type: "int", value });
  if (typeof value === "number") {
    if (!Number.isInteger(value)) {
      throw new TypeError(
        `crdtsync: only integer numbers are storable (got ${value}); use a string or Uint8Array for other data`,
      );
    }
    return encodeScalar({ type: "int", value: BigInt(value) });
  }
  if (typeof value === "string") {
    return encodeScalar({ type: "bytes", value: prefix(STRING, encoder.encode(value)) });
  }
  if (value instanceof Uint8Array) {
    return encodeScalar({ type: "bytes", value: prefix(BINARY, value) });
  }
  // A plain object/array is not a leaf value — containers are created explicitly.
  throw new TypeError(
    "crdtsync: value must be a string, number, bigint, boolean, null, or Uint8Array; " +
      "create a nested container with getMap/getList/getText",
  );
}

/** Read the encoded-scalar bytes from the wasm layer back into a native value. */
export function decodeValue(bytes: Uint8Array): ScalarValue {
  const s = decodeScalar(bytes);
  switch (s.type) {
    case "null":
      return null;
    case "bool":
      return s.value;
    case "int":
      return intToNative(s.value);
    case "bytes":
      return bytesToNative(s.value);
  }
}

/** A scalar as the wasm `diff` machinery reports it: a `{ t, v }` tagged object. */
export interface DiffScalar {
  readonly t: string;
  readonly v?: unknown;
}

/** Convert a diff-reported map-leaf scalar (a native `Scalar`) to a native value.
 * Distinct from `decodeValue`, which reads a list item's enveloped scalar bytes. */
export function nativeFromDiffScalar(s: DiffScalar): ScalarValue {
  switch (s.t) {
    case "null":
      return null;
    case "bool":
      return s.v as boolean;
    case "int":
      return intToNative(s.v as bigint);
    case "bytes":
      return bytesToNative(s.v as Uint8Array);
    default:
      // blobref / elementref: no native leaf form yet — hand back the raw bytes.
      return s.v as Uint8Array;
  }
}

// A value inside the safe-integer range reads back as a `number` (the common
// case); a larger magnitude keeps full fidelity as a `bigint`.
export function intToNative(v: bigint): number | bigint {
  return withinSafeInteger(v) ? Number(v) : v;
}

// A `Scalar::Bytes` payload carries the SDK's one-byte string/binary tag; strip
// it back to a string or the raw bytes (foreign untagged bytes read as binary).
function bytesToNative(payload: Uint8Array): string | Uint8Array {
  const [tag, rest] = unprefix(payload);
  return tag === STRING ? decoder.decode(rest) : rest;
}

function prefix(tag: number, body: Uint8Array): Uint8Array {
  const out = new Uint8Array(1 + body.length);
  out[0] = tag;
  out.set(body, 1);
  return out;
}

function unprefix(bytes: Uint8Array): [number, Uint8Array] {
  if (bytes.length === 0) return [BINARY, bytes];
  const tag = bytes[0];
  if (tag === STRING || tag === BINARY) return [tag, bytes.slice(1)];
  return [BINARY, bytes];
}

function withinSafeInteger(v: bigint): boolean {
  return v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER);
}
