#ifndef _CRDT_MAP_H
#define _CRDT_MAP_H

// LWW Map with tombstones, keyed on raw bytes (binary-safe), Element-valued.
// Identity: the Map itself is stamped with an ElementId at create, exposed
// via map_id. Each composite slot value (Counter / Register / nested Map)
// carries its own ElementId; helpers derive child ids convergently via
// elementid_derive(parent.id, key, key_len, kind). map_merge's recursive guard
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
//     (element_clone, refcount=1). The loser is displaced + released.
//   - Otherwise → LWW on slot stamp. Scalar winners are deep-copied; composite
//     winners are deep-cloned via element_clone (refcount=1) so dst owns its
//     slot fully and is independent of src.
//
// Ownership (Share semantics, refcounted — no arena):
//   - SCALAR_STRING values are deep-copied into Map-owned storage on every
//     accepted write (set, winning merge). When map_get fills *out with a
//     SCALAR Element, the bytes are a borrowed view valid until the slot's
//     next accepted write or until the Map is freed. Caller must not free or
//     mutate.
//   - Composite slots (REGISTER / COUNTER / MAP) are stored as refcounted
//     pointers. An accepted map_set element_acquires the slot's own ref, so
//     the caller always retains and releases the handle it passed, regardless
//     of the LWW outcome. Eviction (winning set/delete, merge LWW-replace)
//     displaces then releases the slot's ref on the loser.
//   - map_get and the helper install path return BORROWS (the slot owns the
//     ref); acquire to keep one valid past the next eviction.
//
// Lifetime — refcounted: map_create returns refcount=1; map_acquire /
// map_release manage it. At refcount 0 the Map releases each live slot
// composite (recursive for nested maps), then frees itself. map_displace marks
// the Map as evicted from a parent slot (it is itself a composite kind).

#include "element.h"
#include "elementid.h"
#include "scalar.h"
#include "stamp.h"
#include <stdbool.h>
#include <stddef.h>

typedef struct Map Map;

Map *map_create(ElementId id);

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
//   1. Live slot with matching kind at `key` → return the existing pointer as
//      a borrow (stamp + seed value ignored).
//   2. Else (empty, tombstone, scalar, or different-kind composite) AND
//      `stamp` wins LWW against any existing entry → create and install a
//      fresh composite, returning a BORROW (the slot owns the ref).
//   3. Else → return a DETACHED composite: created but not installed, born
//      displaced and OWNED by the caller (refcount=1, must release). The slot
//      is left untouched.
// In all cases the caller may acquire a borrow to keep it past eviction.
Counter *map_counter(Map *map, const void *key, size_t key_len, Stamp stamp);

Register *map_register(Map *map, const void *key, size_t key_len, Scalar seed,
                       Stamp stamp);

Map *map_map(Map *map, const void *key, size_t key_len, Stamp stamp);

// Deep recursive copy into a fresh allocation with refcount=1 (composite slots
// cloned via element_clone). The clone is NOT displaced and is independent of
// the source.
Map *map_clone(const Map *map);

// Reference counting. Acquire bumps the refcount; release drops it. On reaching
// zero, the Map releases each live slot composite (recursive) then frees
// itself.
void map_acquire(Map *map);
void map_release(Map *map);

// Displacement signal — marks the Map as no-longer-in-a-slot (a parent Map's
// slot path calls this when it LWW-displaces this Map). Independent of
// refcount; see the lifetime notes above.
void map_displace(Map *map);
bool map_is_displaced(Map *map);

#endif // _CRDT_MAP_H
