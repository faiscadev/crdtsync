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
//     Register owns its value: on every accepted write (create, set, winning
//     merge) it deep-copies the bytes via scalar_clone(NULL, ...) into a
//     host_malloc allocation, and frees the previous value's bytes. No leak
//     across overwrites.
//   - register_read returns a Scalar by value; for SCALAR_STRING the bytes
//     pointer is a borrowed view into the Register's own storage. Valid until
//     the next accepted write or until the Register is freed (refcount 0).
//     Caller must not free or mutate.
//
// Lifetime — refcounted:
//   - register_create returns a Register with refcount = 1; the creator owns
//     that ref. register_acquire bumps it; register_release drops it.
//   - When refcount hits 0, the Register is freed: the value's string bytes
//     (scalar_free) then the struct. Standard C ownership semantics.
//   - The displacement signal is independent of refcount: a Register marked
//     via register_displace (called by the Map slot path on an LWW
//     displacement) stays alive as long as some holder still has a ref.
//     Mutations on a displaced Register still mutate local state, but the Doc
//     layer (when wired) skips op emission. Holders can check
//     register_is_displaced to know their handle is no longer installed in a
//     slot.

#include "elementid.h"
#include "scalar.h"
#include "stamp.h"
#include <stdbool.h>
#include <stdint.h>

typedef struct Register Register;

// Allocate a Register via host_malloc with refcount=1, seeded with `value`
// (deep-copied) and `stamp`. Caller owns one ref.
Register *register_create(ElementId id, Scalar value, Stamp stamp);

ElementId register_id(const Register *reg);

// Borrowed view of the current value — see Ownership notes on validity.
Scalar register_read(const Register *reg);

void register_set(Register *reg, Scalar value, Stamp stamp);

void register_merge(Register *dst, const Register *src);

// Deep-copy the source Register into a fresh allocation. Returned clone has
// refcount=1 and is NOT marked displaced (regardless of src's displaced flag
// — displacement is a per-instance signal, not part of the value).
Register *register_clone(const Register *reg);

// Reference counting. Acquire bumps the refcount; release drops it. On
// reaching zero, the Register is freed (value bytes via scalar_free, then the
// struct).
void register_acquire(Register *reg);
void register_release(Register *reg);

// Displacement signal — see lifetime notes above. register_displace marks the
// Register as no-longer-in-a-slot (Map slot path calls this when it
// LWW-displaces the slot's previous value). register_is_displaced reads it.
void register_displace(Register *reg);
bool register_is_displaced(const Register *reg);

#endif // _CRDT_REGISTER_H
