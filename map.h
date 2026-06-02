#ifndef _CRDT_MAP_H
#define _CRDT_MAP_H

// LWW Map with tombstones, keyed on raw bytes (binary-safe), Element-valued.
// Map itself carries an ElementId, set at create, exposed via map_id —
// that's how parent containers identify "same logical Map across replicas".
//
// Semantics:
//   - Each slot carries a Stamp. set / delete take effect iff the new stamp
//     is strictly greater than the slot's current stamp (per stamp_gt).
//   - Older-stamped writes are silently ignored — set is itself LWW, not a
//     blind overwrite.
//   - Delete installs a tombstone Entry. Tombstones block older sets, lose
//     to newer sets, and persist across merge so replicas converge on the
//     same delete decision.
//
// Merge (per src slot):
//   - Both alive, same composite kind (REGISTER / COUNTER / MAP) and same
//     ElementId → element_merge(dst, src) recurses in place. Slot stamps
//     are ignored on this path; the composite owns its own merge order.
//   - Otherwise → LWW on slot stamp. Scalar winners are dup'd into dst's
//     arena. A composite winner is a cross-arena dangling-pointer hazard;
//     map_merge host_aborts. Deterministic id derivation keeps this path
//     unreachable in normal use.
//
// Ownership:
//   - SCALAR_STRING values are dup'd into the Map's arena on every accepted
//     write (set, winning merge). map_get returns a Scalar whose string
//     bytes are a borrowed view into that arena; valid as long as the arena
//     lives. Caller must not free or mutate.
//   - Composite slots (REGISTER / COUNTER / MAP) are stored as pointers.
//     The pointed-to object must live in the same arena as the Map, or at
//     least outlive it. map_set does not clone composites.
//   - Map lives in its arena; arena_destroy cleans up everything (no
//     separate map_destroy needed).
//
// Lifetime: Map must not outlive its arena.

#include "arena.h"
#include "element.h"
#include "elementid.h"
#include "scalar.h"
#include "stamp.h"
#include <stdbool.h>
#include <stddef.h>

typedef struct Map Map;

Map *map_create(Arena *arena, ElementId id);

ElementId map_id(const Map *map);

// Returns true if the key has a live (non-tombstone) entry, in which case
// *out is set. Returns false otherwise; *out is untouched.
bool map_get(const Map *map, const void *key, size_t key_len, Element *out);

void map_set(Map *map, const void *key, size_t key_len, Element value,
             Stamp stamp);
void map_delete(Map *map, const void *key, size_t key_len, Stamp stamp);

// One-way merge: src's slots are folded into dst; src is left unchanged.
void map_merge(Map *dst, const Map *src);

// Count of live (non-tombstone) entries.
size_t map_size(const Map *map);

#endif // _CRDT_MAP_H
