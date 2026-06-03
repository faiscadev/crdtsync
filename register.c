#include "register.h"
#include "arena.h"
#include "host.h"
#include "scalar.h"
#include <stdbool.h>
#include <string.h>

struct Register {
    Arena *arena;
    Scalar value;
    Stamp stamp;
};

Register *register_create(Arena *arena, Scalar value, Stamp stamp) {
    Register *reg = arena_alloc(arena, sizeof(Register));
    if (!reg) {
        host_abortf(
            "register_create: arena OOM (requested %zu bytes for Register)",
            sizeof(Register));
    }
    reg->arena = arena;
    reg->value = scalar_clone(arena, value);
    reg->stamp = stamp;
    return reg;
}

Scalar register_read(const Register *reg) { return reg->value; }

void register_set(Register *reg, Scalar value, Stamp stamp) {
    if (stamp_gt(stamp, reg->stamp)) {
        reg->value = scalar_clone(reg->arena, value);
        reg->stamp = stamp;
    }
}

void register_merge(Register *dst, const Register *src) {
    if (stamp_gt(src->stamp, dst->stamp)) {
        dst->value = scalar_clone(dst->arena, src->value);
        dst->stamp = src->stamp;
    }
}

Register *register_clone(Arena *arena, const Register *reg) {
    Register *clone = arena_alloc(arena, sizeof(Register));
    if (!clone) {
        host_abortf(
            "register_clone: arena OOM (requested %zu bytes for Register)",
            sizeof(Register));
    }
    clone->arena = arena;
    clone->value = scalar_clone(arena, reg->value);
    clone->stamp = reg->stamp;
    return clone;
}
