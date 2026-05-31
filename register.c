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

Register *register_create(Arena *arena, Scalar value, uint64_t lamport,
                          uint32_t client_id) {
    Register *reg = arena_alloc(arena, sizeof(Register));
    reg->arena = arena;
    reg->value = accept_value(arena, value);
    reg->lamport = lamport;
    reg->client_id = client_id;
    return reg;
}

Scalar register_read(const Register *reg) { return reg->value; }

bool is_newer(uint64_t lamport_a, uint32_t client_id_a, uint64_t lamport_b,
              uint32_t client_id_b) {
    return (lamport_a > lamport_b) ||
           (lamport_a == lamport_b && client_id_a > client_id_b);
}

void register_set(Register *reg, Scalar value, uint64_t lamport,
                  uint32_t client_id) {
    if (is_newer(lamport, client_id, reg->lamport, reg->client_id)) {
        reg->value = accept_value(reg->arena, value);
        reg->lamport = lamport;
        reg->client_id = client_id;
    }
}

void register_merge(Register *dst, const Register *src) {
    if (is_newer(src->lamport, src->client_id, dst->lamport, dst->client_id)) {
        dst->value = accept_value(dst->arena, src->value);
        dst->lamport = src->lamport;
        dst->client_id = src->client_id;
    }
}
