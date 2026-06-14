#ifndef _CRDT_REGISTER_H
#define _CRDT_REGISTER_H

// LWW (last-writer-wins) Register holding a Scalar value with a
// (lamport, client_id) stamp. Identity: each Register is stamped with an
// ElementId at create, exposed via register_id. Two replicas independently
// creating "the same Register at the same slot" derive identical ids via
// elementid_derive(parent.id, key, key_len, ELEMENT_REGISTER), which is what
// map_merge's recursive guard uses to know they refer to the same logical
// element.
//
// Semantics:
//   - Register always holds a value (seeded at create); there is no "unset"
//     state. Presence is a container concern, not the primitive's.
//   - A write or merge takes effect iff its stamp is strictly greater than
//     the current one, where "greater" means larger lamport, or equal lamport
//     and larger client_id. Otherwise it is ignored.
//   - register_set is itself LWW (idempotent, order-independent) — applying
//     older-stamped writes after newer ones is a no-op.
//   - register_merge folds src's stamp into dst, keeping the winner. One-way:
//     src is left unchanged.
//
// Ownership:
//   - Scalar values are passed by value (~24 bytes). For SCALAR_NULL / _BOOL
//     / _INT the payload is in the struct.
//   - For SCALAR_STRING, the Scalar carries a borrowed (bytes, len) view.
//     Register dups those bytes into its arena on every accepted write
//     (create, set, winning merge). Old bytes leak in the arena (bump
//     allocator can't free) — fine for arena lifetime.
//   - register_read returns a Scalar by value; for SCALAR_STRING the bytes
//     pointer is a borrowed view into the register's arena. Valid as long as
//     the arena lives. Caller must not free or mutate.
//
// Lifetime: Register must not outlive its arena.

#include "arena.h"
#include "elementid.h"
#include "scalar.h"
#include "stamp.h"
#include <stdbool.h>
#include <stdint.h>

typedef struct Register Register;

Register *register_create(Arena *arena, ElementId id, Scalar value,
                          Stamp stamp);

ElementId register_id(const Register *reg);

Scalar register_read(const Register *reg);

void register_set(Register *reg, Scalar value, Stamp stamp);

void register_merge(Register *dst, const Register *src);

Register *register_clone(Arena *arena, const Register *reg);

#endif // _CRDT_REGISTER_H
