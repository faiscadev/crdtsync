#ifndef _CRDT_UUID_H
#define _CRDT_UUID_H

// UUID utilities — format, parse, v5 derivation. Shared by ClientId
// (UUID v7) and ElementId (UUID v5). Operates on raw 16-byte arrays
// rather than a typed Uuid wrapper, so ClientId and ElementId keep
// distinct typedefs for compile-time type-safety while still sharing
// these utilities.
//
// Strings use the canonical RFC 4122 8-4-4-4-12 format:
//   xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
// lowercase hex, 36 chars + NUL.

#include "sha1.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// --- format / parse ---

// Write the canonical RFC 4122 string form into `out`. Always lowercase.
// `out` must be at least 37 bytes (36 + NUL).
void uuid_format(const uint8_t bytes[16], char out[37]);

// Parse a canonical RFC 4122 string into 16 bytes. Accepts lowercase or
// uppercase hex; requires hyphens at positions 8, 13, 18, 23. Returns
// true on success, false on any format violation (length wrong, bad hex
// digit, missing hyphen). `out` is untouched on failure.
bool uuid_parse(const char *s, uint8_t out[16]);

// --- version 5 (deterministic, SHA-1 over namespace || name) ---

// Typed v5 output. 16 bytes, RFC 4122 layout, version=5 + RFC variant
// bits set. Pass-by-value is cheap (16 bytes).
typedef struct UuidV5 {
    uint8_t bytes[16];
} UuidV5;

// One-shot v5 derive. digest = SHA-1(namespace || name), take first 16
// bytes, then set version=5 in the high nibble of byte 6 and variant=10xx
// in the high two bits of byte 8 per RFC 4122 §4.1.
UuidV5 uuid_v5(const uint8_t namespace_bytes[16], const uint8_t *name,
               size_t name_len);

// Streaming v5 — for callers that build the `name` portion from multiple
// pieces without allocating a contiguous buffer.
typedef struct UuidV5Ctx {
    // Opaque to callers; declared here only for stack allocation.
    // Layout mirrors SHA1_CTX internally.
    SHA1_CTX sha1_ctx;
} UuidV5Ctx;

void uuid_v5_init(UuidV5Ctx *ctx, const uint8_t namespace_bytes[16]);
void uuid_v5_update(UuidV5Ctx *ctx, const uint8_t *data, size_t len);
UuidV5 uuid_v5_final(UuidV5Ctx *ctx);

// --- bit-fiddle helpers ---
//
// Used by both v5 and v7 generators. Exposed publicly so app or test
// fixtures can construct UUIDs manually with the correct format bits.

// Mask byte 6's version nibble to the low 4 bits of `version` (the high
// 4 bits are ignored; no range validation), and force the RFC 4122
// variant (10xx in high two bits of byte 8). Mutates in place. Callers
// are responsible for passing a UUID version value (1..7 per RFC 4122 +
// RFC 9562); the helper does not enforce that.
void uuid_set_version_and_variant(uint8_t bytes[16], uint8_t version);

#endif // _CRDT_UUID_H
