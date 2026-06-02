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
    Arena *arena;
    HashTable *entries;
};

Map *map_create(Arena *arena, ElementId id) {
    Map *map = arena_alloc(arena, sizeof(Map));
    if (!map) {
        host_abortf("map_create: arena OOM (requested %zu bytes for Map)",
                    sizeof(Map));
    }
    map->id = id;
    map->arena = arena;
    map->entries = hashtable_create(arena);
    if (!map->entries) {
        host_abort("map_create: hashtable_create OOM");
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
            Scalar copy = scalar_dup(map->arena, value.as.scalar);
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
            ElementId did, sid;
            switch (de->value.kind) {
            case ELEMENT_SCALAR:
                did = sid = elementid_root(); // unused, won't compare equal,
                                              // assign to silence warning
                break;
            case ELEMENT_REGISTER:
                did = register_id(de->value.as.reg);
                sid = register_id(se->value.as.reg);
                break;
            case ELEMENT_COUNTER:
                did = counter_id(de->value.as.counter);
                sid = counter_id(se->value.as.counter);
                break;
            case ELEMENT_MAP:
                did = map_id(de->value.as.map);
                sid = map_id(se->value.as.map);
                break;
            }
            if (elementid_eq(did, sid)) {
                element_merge(de->value, se->value);
                // Advance slot stamp to max(dst, src) so future slot-level
                // ops on this key are LWW-deterministic across replicas.
                if (stamp_gt(se->stamp, de->stamp)) {
                    de->stamp = se->stamp;
                }
                continue;
            }
        }

        // LWW fallthrough.
        if (se->is_tombstone) {
            map_delete(dst, k, klen,
                       se->stamp); // map_delete is itself LWW-guarded
            continue;
        }

        // src has a live value. Only abort if src's value would actually
        // win LWW and is a composite — that's the cross-arena displacement
        // hazard. If src loses by stamp, dst keeps its slot and nothing
        // dangerous happens.
        bool src_wins = !dst_has || stamp_gt(se->stamp, de->stamp);
        if (!src_wins) {
            continue;
        }
        if (se->value.kind != ELEMENT_SCALAR) {
            host_abortf("map_merge: cross-replica composite displacement "
                        "at key (LWW path) — "
                        "src %s id != dst id (or dst slot "
                        "empty/tombstone). Use deterministic "
                        "id derivation for composite slots.",
                        element_kind_name(se->value.kind));
        }
        map_set(dst, k, klen, se->value, se->stamp);
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
