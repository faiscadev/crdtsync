#include "scalar.h"
#include "host.h"
#include <stdbool.h>
#include <string.h>

Scalar scalar_null(void) {
    Scalar s;
    s.kind = SCALAR_NULL;
    return s;
}

Scalar scalar_bool(bool b) {
    Scalar s;
    s.kind = SCALAR_BOOL;
    s.as.b = b;
    return s;
}

Scalar scalar_int(int64_t i) {
    Scalar s;
    s.kind = SCALAR_INT;
    s.as.i = i;
    return s;
}

Scalar scalar_string(const uint8_t *bytes, size_t len) {
    Scalar s;
    s.kind = SCALAR_STRING;
    s.as.s.bytes = bytes;
    s.as.s.len = len;
    return s;
}

bool scalar_eq(Scalar a, Scalar b) {
    if (a.kind != b.kind) {
        return false;
    }

    switch (a.kind) {
    case SCALAR_NULL:
        return true; // all nulls are equal
    case SCALAR_BOOL:
        return a.as.b == b.as.b;
    case SCALAR_INT:
        return a.as.i == b.as.i;
    case SCALAR_STRING:
        if (a.as.s.len != b.as.s.len) {
            return false;
        }
        // Guard zero-length: memcmp(NULL, NULL, 0) is UB pre-C2x even though
        // libc impls typically tolerate it. Two empty strings are equal.
        if (a.as.s.len == 0) {
            return true;
        }
        return memcmp(a.as.s.bytes, b.as.s.bytes, a.as.s.len) == 0;
    }
}

Scalar scalar_clone(Scalar value) {
    switch (value.kind) {
    case SCALAR_STRING: {
        // Empty string: no bytes to copy. Pass the value through unchanged.
        // Avoids portability fragility around malloc(0) (implementation-defined
        // return) and the matching free-leak that scalar_free would otherwise
        // miss.
        if (value.as.s.len == 0) {
            return value;
        }
        uint8_t *bytes_copy = host_malloc(value.as.s.len);
        if (!bytes_copy) {
            host_abortf("scalar_clone: host_malloc OOM (requested %zu bytes "
                        "for string value)",
                        value.as.s.len);
        }
        memcpy(bytes_copy, value.as.s.bytes, value.as.s.len);
        Scalar copy = {0};
        copy.kind = SCALAR_STRING;
        copy.as.s.bytes = bytes_copy;
        copy.as.s.len = value.as.s.len;
        return copy;
    }
    case SCALAR_INT:
    case SCALAR_BOOL:
    case SCALAR_NULL:
        // No heap data to clone, just copy the struct.
        return value;
    }
}

void scalar_free(Scalar value) {
    if (value.kind == SCALAR_STRING && value.as.s.len > 0) {
        host_free((void *)value.as.s.bytes);
    }
}
