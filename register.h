#ifndef _CRDT_REGISTER_H
#define _CRDT_REGISTER_H

#include "arena.h"
#include "scalar.h"
#include <stdbool.h>
#include <stdint.h>

typedef struct Register {
    Arena *arena;
    Scalar value;
    uint64_t lamport;
    uint32_t client_id;
} Register;

Register *register_create(Arena *arena, Scalar value, uint64_t lamport,
                          uint32_t client_id);

Scalar register_read(const Register *reg);

void register_set(Register *reg, Scalar value, uint64_t lamport,
                  uint32_t client_id);

void register_merge(Register *dst, const Register *src);

#endif // _CRDT_REGISTER_H
