#include "scalar.h"
#include <stdbool.h>

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
        for (size_t i = 0; i < a.as.s.len; i++) {
            if (a.as.s.bytes[i] != b.as.s.bytes[i]) {
                return false;
            }
        }
        return true;
    }
}
