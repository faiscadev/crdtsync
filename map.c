#include "map.h"
#include "hashtable.h"
#include "host.h"
#include "string.h"

typedef struct MapEntry {
    Stamp stamp;
    Scalar value;
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
        host_abort("map_create: hashtable_create OOM");
    }
    return map;
}

bool map_get(const Map *map, const void *key, size_t key_len, Scalar *out) {
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

void map_set(Map *map, const void *key, size_t key_len, Scalar value,
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

        Scalar copy = scalar_dup(map->arena, value);

        entry->stamp = stamp;
        entry->is_tombstone = false;
        entry->value = copy;
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
    const void *k = NULL;
    size_t klen = 0;
    void *v = NULL;
    while (hashtable_iter_next(&it, &k, &klen, &v)) {
        Entry *src_entry = v;
        if (src_entry->is_tombstone) {
            map_delete(dst, k, klen, src_entry->stamp);
        } else {
            map_set(dst, k, klen, src_entry->value, src_entry->stamp);
        }
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
