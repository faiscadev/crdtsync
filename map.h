#ifndef _CRDT_MAP_H
#define _CRDT_MAP_H

// LWW Map with tombstones, keyed on raw bytes (binary-safe), Element-valued.
// Identity: the Map itself is stamped with an ElementId at create, exposed
// via map_id. Each composite slot value (Counter / Register / nested Map)
// carries its own ElementId; helpers derive child ids convergently via
// elementid_derive(parent.id, key, kind). map_merge's recursive guard
// uses (kind, id) to know two slots refer to the same logical element.
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
//   - Both alive AND same composite kind AND matching ids → element_merge
//     recurses in place. Slot stamp advances to max(dst, src) so future
//     slot-level ops stay LWW-deterministic.
//   - Both alive AND same composite kind BUT mismatched ids → distinct
//     logical elements that happen to share the slot. LWW on slot stamp;
//     if src wins, dst's composite is replaced with a deep clone of src's
//     into dst's arena. Loser is orphaned.
//   - Otherwise → LWW on slot stamp. Scalar winners are scalar_clone'd
//     into dst's arena. Composite winners are deep-cloned via element_clone
//     into dst's arena, so dst owns its slot fully and survives src arena
//     destroy.
//
// Ownership:
//   - SCALAR_STRING values are cloned into the Map's arena on every accepted
//     write (set, winning merge). When map_get fills *out with a SCALAR
//     Element, the string bytes are a borrowed view into that arena; valid
//     as long as the arena lives. Caller must not free or mutate.
//   - Composite slots (REGISTER / COUNTER / MAP) are stored as pointers.
//     map_set does NOT clone composites — the pointed-to object must live
//     in the same arena as the Map. map_merge's LWW path clones via
//     element_clone, so the cross-arena hazard does not surface there.
//   - Map lives in its arena; arena_destroy cleans up everything (no
//     separate map_destroy needed).
//
// Lifetime: Map must not outlive its arena.

#include "arena.h"
#include "element.h"
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

// Get-or-create helpers. Behaviour per call:
//   1. Live slot with matching kind at `key` → return the existing pointer
//      (stamp + seed value ignored).
//   2. Else (empty, tombstone, scalar, or different-kind composite) AND
//      `stamp` wins LWW against any existing entry → create a fresh
//      composite in the Map's arena, install in the slot, return it.
//   3. Else → return a DETACHED composite: created in the Map's arena but
//      not installed in the slot. Caller always gets a usable handle; the
//      slot is left untouched.
Counter *map_counter(Map *map, const void *key, size_t key_len, Stamp stamp);

Register *map_register(Map *map, const void *key, size_t key_len, Scalar seed,
                       Stamp stamp);

Map *map_map(Map *map, const void *key, size_t key_len, Stamp stamp);

Map *map_clone(Arena *arena, const Map *map);

#endif // _CRDT_MAP_H
