#include "register.h"
#include "arena.h"
#include "host.h"
#include "scalar.h"
#include <stdbool.h>
#include <string.h>

struct Register {
    ElementId id;
    Arena *arena;
    Scalar value;
    Stamp stamp;
};

Scalar accept_value(Arena *arena, Scalar value) {
    switch (value.kind) {
    case SCALAR_STRING: {
        // Copy string bytes into arena.
        uint8_t *dst = arena_alloc(arena, value.as.s.len);
        if (!dst && value.as.s.len > 0) {
            host_abortf("register: arena OOM copying %zu string bytes",
                        value.as.s.len);
        }
        memcpy(dst, value.as.s.bytes, value.as.s.len);
        return scalar_string(dst, value.as.s.len);
    }
    case SCALAR_NULL:
    case SCALAR_BOOL:
    case SCALAR_INT:
        return value;
    }
}

Register *register_create(Arena *arena, ElementId id, Scalar value,
                          Stamp stamp) {
    Register *reg = arena_alloc(arena, sizeof(Register));
    if (!reg) {
        host_abortf(
            "register_create: arena OOM (requested %zu bytes for Register)",
            sizeof(Register));
    }
    reg->id = id;
    reg->arena = arena;
    reg->value = accept_value(arena, value);
    reg->stamp = stamp;
    return reg;
}

ElementId register_id(const Register *reg) { return reg->id; }

Scalar register_read(const Register *reg) { return reg->value; }

void register_set(Register *reg, Scalar value, Stamp stamp) {
    if (stamp_gt(stamp, reg->stamp)) {
        reg->value = accept_value(reg->arena, value);
        reg->stamp = stamp;
    }
}

void register_merge(Register *dst, const Register *src) {
    if (stamp_gt(src->stamp, dst->stamp)) {
        dst->value = accept_value(dst->arena, src->value);
        dst->stamp = src->stamp;
    }
}
