#include "map.h"
#include "element.h"
#include "hashtable.h"
#include "host.h"
#include "string.h"

typedef struct MapEntry {
    Stamp stamp;
    Element value;
    bool is_tombstone;
} Entry;

struct Map {
    ElementId id;
    HashTable *entries;

    size_t refcount;
    bool displaced;
};

Map *map_create(ElementId id) {
    Map *map = host_malloc(sizeof(Map));
    if (!map) {
        host_abortf("map_create: host_malloc OOM (requested %zu bytes for Map)",
                    sizeof(Map));
    }
    map->id = id;
    map->entries = hashtable_create();
    map->refcount = 1;
    map->displaced = false;
    if (!map->entries) {
        host_abort("map_create: hashtable_create OOM (slot table)");
    }
    return map;
}

ElementId map_id(const Map *map) { return map->id; }

bool map_get(const Map *map, const void *key, size_t key_len, Element *out) {
    void *entry;
    bool present = hashtable_get(map->entries, key, key_len, &entry);
    if (!present) {
        return false;
    }

    Entry *map_entry = entry;
    if (map_entry->is_tombstone) {
        return false;
    }

    *out = map_entry->value;
    return true;
}

void map_set(Map *map, const void *key, size_t key_len, Element value,
             Stamp stamp) {
    Entry *entry;
    bool present = hashtable_get(map->entries, key, key_len, (void **)&entry);
    bool update = !present || stamp_gt(stamp, entry->stamp);
    if (update) {
        element_acquire(value);
        if (!present) {
            entry = host_malloc(sizeof(Entry));
            if (!entry) {
                host_abortf(
                    "map_set: host_malloc OOM (requested %zu bytes for Entry)",
                    sizeof(Entry));
            }
            HashTableInsertResult r =
                hashtable_insert(map->entries, key, key_len, entry);
            if (r != HASHTABLE_OK) {
                host_abortf("map_set: hashtable_insert -> %s",
                            hashtable_insert_result_name(r));
            }
        }

        switch (value.kind) {
        case ELEMENT_SCALAR: {
            Scalar copy = scalar_clone(value.as.scalar);
            value.as.scalar = copy;
            break;
        }
        case ELEMENT_REGISTER:
        case ELEMENT_COUNTER:
        case ELEMENT_MAP:
            // Composite values are pointers to separately-allocated heap
            // objects; no dup needed.
            break;
        }

        // Drop the displaced value only when overwriting an existing live
        // slot. A brand-new entry has no prior value, and a tombstone's value
        // field is not a live handle — neither holds a ref to release.
        if (present && !entry->is_tombstone) {
            element_displace(entry->value);
            element_release(entry->value);
        }
        entry->value = value;
        entry->stamp = stamp;
        entry->is_tombstone = false;
    }
}

void map_delete(Map *map, const void *key, size_t key_len, Stamp stamp) {
    Entry *entry;
    bool present = hashtable_get(map->entries, key, key_len, (void **)&entry);
    if (present) {
        if (stamp_gt(stamp, entry->stamp)) {
            entry->stamp = stamp;
            // Drop the live composite (if any) before tombstoning. A
            // re-delete over an existing tombstone has no live handle.
            if (!entry->is_tombstone) {
                element_displace(entry->value);
                element_release(entry->value);
            }
            entry->is_tombstone = true;
        }
    } else {
        // Install a tombstone for the absent key, so that future merges can
        // compare stamps and know that the delete wins over older sets.
        entry = host_malloc(sizeof(Entry));
        if (!entry) {
            host_abortf(
                "map_delete: host_malloc OOM (requested %zu bytes for Entry)",
                sizeof(Entry));
        }
        entry->stamp = stamp;
        entry->is_tombstone = true;
        // A tombstone holds no live composite; give value a safe sentinel so
        // nothing ever releases an uninitialized handle.
        entry->value = element_scalar(scalar_null());
        HashTableInsertResult r =
            hashtable_insert(map->entries, key, key_len, entry);
        if (r != HASHTABLE_OK) {
            host_abortf("map_delete: hashtable_insert -> %s",
                        hashtable_insert_result_name(r));
        }
    }
}

void map_merge(Map *dst, const Map *src) {
    HashTableIter it = hashtable_iter(src->entries);
    const void *k;
    size_t klen;
    void *v;
    while (hashtable_iter_next(&it, &k, &klen, &v)) {
        Entry *se = v;

        Entry *de;
        bool dst_has = hashtable_get(dst->entries, k, klen, (void **)&de);

        // Same key, both alive composites of same kind: either recurse in
        // place (matching ids → same logical element) or LWW-clone the
        // winner (mismatched ids → distinct logical elements that happen
        // to share the slot).
        if (dst_has && !de->is_tombstone && !se->is_tombstone &&
            de->value.kind == se->value.kind &&
            de->value.kind != ELEMENT_SCALAR) {
            if (!elementid_eq(element_id(de->value), element_id(se->value))) {
                // Distinct logical elements at the same slot. LWW the
                // slot; if src wins, replace dst's composite with a
                // clone of src's. Loser is orphaned.
                if (stamp_gt(se->stamp, de->stamp)) {
                    de->value = element_clone(se->value);
                    de->stamp = se->stamp;
                }
                continue;
            }
            element_merge(de->value, se->value);
            // Advance slot stamp to max(dst, src) so future slot-level
            // ops on this key are LWW-deterministic across replicas.
            if (stamp_gt(se->stamp, de->stamp)) {
                de->stamp = se->stamp;
            }
            continue;
        }

        // LWW fallthrough.
        if (se->is_tombstone) {
            map_delete(dst, k, klen, se->stamp);
            continue;
        }

        // Skip the clone+set if src would lose LWW — map_set would do the
        // same stamp check internally and discard the work, but element_clone
        // on a composite is deep recursive and would leak a never-installed
        // refcounted copy when the value never wins.
        if (dst_has && !stamp_gt(se->stamp, de->stamp)) {
            continue;
        }

        Element cloned = element_clone(se->value);
        map_set(dst, k, klen, cloned, se->stamp);
    }
}

size_t map_size(const Map *map) {
    HashTableIter it = hashtable_iter(map->entries);
    size_t count = 0;
    const void *k = NULL;
    size_t klen = 0;
    void *v = NULL;
    while (hashtable_iter_next(&it, &k, &klen, &v)) {
        Entry *entry = v;
        if (!entry->is_tombstone) {
            count++;
        }
    }

    return count;
}

Counter *map_counter(Map *map, const void *key, size_t key_len, Stamp stamp) {
    Entry *existing;
    bool present =
        hashtable_get(map->entries, key, key_len, (void **)&existing);
    if (present && !existing->is_tombstone &&
        existing->value.kind == ELEMENT_COUNTER) {
        return existing->value.as.counter;
    }

    ElementId id = elementid_derive(map->id, key, key_len, ELEMENT_COUNTER);
    Counter *fresh = counter_create(id); // create-ref: rc = 1
    if (!present || stamp_gt(stamp, existing->stamp)) {
        // Installed: map_set acquires the slot's ref (rc = 2). Drop our
        // create-ref so the slot is the sole owner and the returned handle is
        // a borrow (rc = 1).
        map_set(map, key, key_len, element_counter(fresh), stamp);
        counter_release(fresh);
    } else {
        // Detached when LWW lost: never installed, so born displaced. The
        // caller owns this rc = 1 handle and must release it.
        counter_displace(fresh);
    }
    return fresh;
}

Register *map_register(Map *map, const void *key, size_t key_len, Scalar seed,
                       Stamp stamp) {
    Entry *existing;
    bool present =
        hashtable_get(map->entries, key, key_len, (void **)&existing);
    if (present && !existing->is_tombstone &&
        existing->value.kind == ELEMENT_REGISTER) {
        return existing->value.as.reg;
    }

    ElementId id = elementid_derive(map->id, key, key_len, ELEMENT_REGISTER);
    Register *fresh = register_create(id, seed, stamp); // create-ref: rc = 1
    if (!present || stamp_gt(stamp, existing->stamp)) {
        // Installed: map_set acquires the slot's ref (rc = 2). Drop our
        // create-ref so the slot is the sole owner and the returned handle is
        // a borrow (rc = 1).
        map_set(map, key, key_len, element_register(fresh), stamp);
        register_release(fresh);
    } else {
        // Detached when LWW lost: never installed, so born displaced. The
        // caller owns this rc = 1 handle and must release it.
        register_displace(fresh);
    }
    return fresh;
}

Map *map_map(Map *map, const void *key, size_t key_len, Stamp stamp) {
    Entry *existing;
    bool present =
        hashtable_get(map->entries, key, key_len, (void **)&existing);
    if (present && !existing->is_tombstone &&
        existing->value.kind == ELEMENT_MAP) {
        return existing->value.as.map;
    }

    ElementId id = elementid_derive(map->id, key, key_len, ELEMENT_MAP);
    Map *fresh = map_create(id); // create-ref: rc = 1
    if (!present || stamp_gt(stamp, existing->stamp)) {
        // Installed: map_set acquires the slot's ref (rc = 2). Drop our
        // create-ref so the slot is the sole owner and the returned handle is
        // a borrow (rc = 1).
        map_set(map, key, key_len, element_map(fresh), stamp);
        map_release(fresh);
    } else {
        // Detached when LWW lost: never installed, so born displaced. The
        // caller owns this rc = 1 handle and must release it.
        map_displace(fresh);
    }
    return fresh;
}

Map *map_clone(const Map *map) {
    Map *clone = map_create(map->id);
    HashTableIter it = hashtable_iter(map->entries);
    const void *k;
    size_t klen;
    void *v;
    while (hashtable_iter_next(&it, &k, &klen, &v)) {
        Entry *entry = v;
        Entry *copy = host_malloc(sizeof(Entry));
        if (!copy) {
            host_abortf(
                "map_clone: host_malloc OOM (requested %zu bytes for Entry)",
                sizeof(Entry));
        }

        copy->stamp = entry->stamp;
        copy->is_tombstone = entry->is_tombstone;
        if (!entry->is_tombstone) {
            copy->value = element_clone(entry->value);
        } else {
            copy->value = element_scalar(scalar_null());
        }
        HashTableInsertResult r =
            hashtable_insert(clone->entries, k, klen, copy);
        if (r != HASHTABLE_OK) {
            host_abortf("map_clone: hashtable_insert -> %s",
                        hashtable_insert_result_name(r));
        }
    }
    return clone;
}

void map_acquire(Map *map) { map->refcount++; }
void map_release(Map *map) {
    if (--map->refcount == 0) {
        HashTableIter it = hashtable_iter(map->entries);
        const void *k;
        size_t klen;
        void *v;
        while (hashtable_iter_next(&it, &k, &klen, &v)) {
            Entry *entry = v;
            if (!entry->is_tombstone) {
                element_release(entry->value);
            }
            host_free(entry);
        }
        hashtable_destroy(map->entries);
        host_free(map);
    }
}

void map_displace(Map *map) { map->displaced = true; }
bool map_is_displaced(Map *map) { return map->displaced; }
