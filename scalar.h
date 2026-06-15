#ifndef _CRDT_SCALAR_H
#define _CRDT_SCALAR_H

// Tagged value type used as the payload of LWW Registers, Map slots,
// XmlElement attrs, and mark values.
//
// Variants: NULL, BOOL, INT (int64_t), STRING (raw bytes + length,
// binary-safe — embedded NULs are part of the value).
//
// Ownership: passed by value (~24 bytes). For SCALAR_STRING the struct
// carries a BORROWED (bytes, len) view; the caller owns the underlying
// memory at the API boundary. Anything that needs to store a Scalar past
// the call (Register, Map, ...) must clone the bytes into its own arena.
//
// scalar_eq is kind-strict: cross-kind comparison is always false, even
// for "obvious" coincidences (scalar_int(0) != scalar_bool(false)). For
// SCALAR_STRING, equality is bytes+len memcmp (binary-safe).

#include "arena.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum ScalarKind {
    SCALAR_STRING,
    SCALAR_INT,
    SCALAR_BOOL,
    SCALAR_NULL
} ScalarKind;

typedef struct Scalar {
    ScalarKind kind;

    union {
        struct {
            const uint8_t *bytes;
            size_t len;
        } s;       // for SCALAR_STRING
        int64_t i; // for SCALAR_INT
        bool b;    // for SCALAR_BOOL
    } as;
} Scalar;

Scalar scalar_null(void);

Scalar scalar_bool(bool b);

Scalar scalar_int(int64_t i);

Scalar scalar_string(const uint8_t *bytes, size_t len);

bool scalar_eq(Scalar a, Scalar b);

// Clone a Scalar into owned storage. Two modes:
//   1. `scalar_clone(arena, value)` — string bytes (if any) allocated in
//      the supplied Arena. Bulk lifetime: arena_destroy frees them. Do
//      NOT call scalar_free on the result.
//   2. `scalar_clone(NULL, value)` — string bytes (if any) allocated via
//      host_malloc. Caller MUST release with scalar_free when done.
//
// For non-string kinds (NULL / BOOL / INT) cloning is a value-copy
// regardless of mode — nothing to allocate.
Scalar scalar_clone(Arena *arena, Scalar value);

// Release a host_malloc-backed Scalar's string bytes. No-op for non-string
// kinds and for empty strings (no allocation to release). MUST only be
// called on Scalars produced by `scalar_clone(NULL, ...)`. Calling on an
// arena-backed Scalar is undefined.
void scalar_free(Scalar value);

#endif // _CRDT_SCALAR_H
