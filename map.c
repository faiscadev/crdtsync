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
    Arena *arena;
    HashTable *entries;
};

Map *map_create(Arena *arena) {
    Map *map = arena_alloc(arena, sizeof(Map));
    if (!map) {
        host_abortf("map_create: arena OOM (requested %zu bytes for Map)",
                    sizeof(Map));
    }
    map->arena = arena;
    map->entries = hashtable_create(arena);
    if (!map->entries) {
        host_abort("map_create: hashtable_create OOM (slot table)");
    }
    return map;
}

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
        if (!present) {
            entry = arena_alloc(map->arena, sizeof(Entry));
            if (!entry) {
                host_abortf(
                    "map_set: arena OOM (requested %zu bytes for Entry)",
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
            Scalar copy = scalar_clone(map->arena, value.as.scalar);
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
            entry->is_tombstone = true;
        }
    } else {
        // Install a tombstone for the absent key, so that future merges can
        // compare stamps and know that the delete wins over older sets.
        entry = arena_alloc(map->arena, sizeof(Entry));
        if (!entry) {
            host_abortf("map_delete: arena OOM (requested %zu bytes for Entry)",
                        sizeof(Entry));
        }
        entry->stamp = stamp;
        entry->is_tombstone = true;
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

        // Recursive: both alive, same composite kind and same id then
        // element_merge. This wins over slot LWW.
        if (dst_has && !de->is_tombstone && !se->is_tombstone &&
            de->value.kind == se->value.kind &&
            de->value.kind != ELEMENT_SCALAR) {
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
        // on a composite is deep recursive and leaks into dst's arena even
        // when the value is never installed.
        if (dst_has && !stamp_gt(se->stamp, de->stamp)) {
            continue;
        }

        Element cloned = element_clone(dst->arena, se->value);
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

    Counter *fresh = counter_create(map->arena);
    if (!present || stamp_gt(stamp, existing->stamp)) {
        map_set(map, key, key_len, element_counter(fresh), stamp);
    }
    // Detached when LWW lost: returned anyway so caller always gets a usable
    // handle.
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

    Register *fresh = register_create(map->arena, seed, stamp);
    if (!present || stamp_gt(stamp, existing->stamp)) {
        map_set(map, key, key_len, element_register(fresh), stamp);
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

    Map *fresh = map_create(map->arena);
    if (!present || stamp_gt(stamp, existing->stamp)) {
        map_set(map, key, key_len, element_map(fresh), stamp);
    }
    return fresh;
}

Map *map_clone(Arena *arena, const Map *map) {
    Map *clone = map_create(arena);
    HashTableIter it = hashtable_iter(map->entries);
    const void *k;
    size_t klen;
    void *v;
    while (hashtable_iter_next(&it, &k, &klen, &v)) {
        Entry *entry = v;
        Entry *copy = arena_alloc(clone->arena, sizeof(Entry));
        if (!copy) {
            host_abortf("map_clone: arena OOM (requested %zu bytes for Entry)",
                        sizeof(Entry));
        }

        copy->stamp = entry->stamp;
        copy->is_tombstone = entry->is_tombstone;
        if (!entry->is_tombstone) {
            copy->value = element_clone(clone->arena, entry->value);
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
