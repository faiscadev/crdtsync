#include "register.h"
#include "arena.h"
#include "scalar.h"
#include <stdbool.h>
#include <string.h>

Scalar accept_value(Arena *arena, Scalar value) {
    switch (value.kind) {
    case SCALAR_STRING: {
        // Copy string bytes into arena.
        uint8_t *dst = arena_alloc(arena, value.as.s.len);
        memcpy(dst, value.as.s.bytes, value.as.s.len);
        return scalar_string(dst, value.as.s.len);
    }
    case SCALAR_NULL:
    case SCALAR_BOOL:
    case SCALAR_INT:
        return value;
    }
}

Register *register_create(Arena *arena, Scalar value, Stamp stamp) {
    Register *reg = arena_alloc(arena, sizeof(Register));
    reg->arena = arena;
    reg->value = accept_value(arena, value);
    reg->stamp = stamp;
    return reg;
}

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
