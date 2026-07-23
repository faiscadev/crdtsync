// The core `Scalar` wire codec, mirrored in TypeScript so the handle layer can
// build and read the encoded-scalar payloads the wasm `setScalar`/`getScalar`
// (and list scalar) methods carry. The byte format is the core's
// `Scalar::encode_state`: a one-byte tag then the variant's payload, all
// little-endian. Only the leaf-value variants the SDK marshals are implemented
// (null, bool, int, bytes); a blob/element ref rides its own dedicated path.

const TAG_NULL = 0x00;
const TAG_BOOL = 0x01;
const TAG_INT = 0x02;
const TAG_BYTES = 0x03;

const INT_MIN = -(2n ** 63n);
const INT_MAX = 2n ** 63n - 1n;

export type Scalar =
  | { readonly type: "null" }
  | { readonly type: "bool"; readonly value: boolean }
  | { readonly type: "int"; readonly value: bigint }
  | { readonly type: "bytes"; readonly value: Uint8Array };

export function encodeScalar(s: Scalar): Uint8Array {
  switch (s.type) {
    case "null":
      return Uint8Array.of(TAG_NULL);
    case "bool":
      return Uint8Array.of(TAG_BOOL, s.value ? 1 : 0);
    case "int": {
      // A value past the i64 range would wrap silently through setBigInt64;
      // reject it instead so an out-of-range int is a loud error, not corruption.
      if (s.value < INT_MIN || s.value > INT_MAX) {
        throw new RangeError(`crdtsync: integer ${s.value} is outside the 64-bit range`);
      }
      const out = new Uint8Array(9);
      out[0] = TAG_INT;
      new DataView(out.buffer).setBigInt64(1, s.value, true);
      return out;
    }
    case "bytes": {
      const len = s.value.length;
      const out = new Uint8Array(5 + len);
      out[0] = TAG_BYTES;
      new DataView(out.buffer).setUint32(1, len, true);
      out.set(s.value, 5);
      return out;
    }
  }
}

export function decodeScalar(bytes: Uint8Array): Scalar {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const tag = bytes[0];
  switch (tag) {
    case TAG_NULL:
      return { type: "null" };
    case TAG_BOOL:
      return { type: "bool", value: bytes[1] !== 0 };
    case TAG_INT:
      return { type: "int", value: view.getBigInt64(1, true) };
    case TAG_BYTES: {
      const len = view.getUint32(1, true);
      return { type: "bytes", value: bytes.slice(5, 5 + len) };
    }
    default:
      throw new Error(`crdtsync: unsupported scalar tag ${tag}`);
  }
}
