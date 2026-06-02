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
// the call (Register, Map, ...) must dup the bytes into its own arena.
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

Scalar scalar_dup(Arena *arena, Scalar value);

#endif // _CRDT_SCALAR_H
