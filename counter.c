#include "counter.h"
#include "arena.h"
#include "hashtable.h"
#include "host.h"

struct Counter {
    ElementId id;
    Arena *arena;
    HashTable *entries; // client_id (uint32_t) -> CounterEntry
};

static inline uint32_t max_u32(uint32_t a, uint32_t b) {
    if (a > b) {
        return a;
    }

    return b;
}

Counter *counter_create(Arena *arena, ElementId id) {
    Counter *counter = arena_alloc(arena, sizeof(Counter));
    if (!counter) {
        host_abortf(
            "counter_create: arena OOM (requested %zu bytes for Counter)",
            sizeof(Counter));
    }
    counter->id = id;
    counter->arena = arena;
    counter->entries = hashtable_create(arena);
    if (!counter->entries) {
        host_abort("counter_create: hashtable_create OOM");
    }
    return counter;
}

ElementId counter_id(const Counter *counter) { return counter->id; }

int64_t counter_read(const Counter *counter) {
    int64_t total = 0;
    HashTableIter it = hashtable_iter(counter->entries);
    const void *key;
    size_t key_len;
    void *value;
    while (hashtable_iter_next(&it, &key, &key_len, &value)) {
        CounterEntry *entry = value;
        total += entry->inc;
        total -= entry->dec;
    }
    return total;
}

void counter_merge(Counter *dst, const Counter *src) {
    HashTableIter it = hashtable_iter(src->entries);
    const void *key;
    size_t key_len;
    void *value;
    while (hashtable_iter_next(&it, &key, &key_len, &value)) {
        CounterEntry *src_entry = value;
        void *dst_entry_ptr;
        if (hashtable_get(dst->entries, key, key_len, &dst_entry_ptr)) {
            CounterEntry *dst_entry = dst_entry_ptr;
            dst_entry->inc = max_u32(dst_entry->inc, src_entry->inc);
            dst_entry->dec = max_u32(dst_entry->dec, src_entry->dec);
        } else {
            CounterEntry *copy = arena_alloc(dst->arena, sizeof *copy);
            if (!copy) {
                host_abortf("counter_merge: arena OOM (requested %zu bytes for "
                            "CounterEntry)",
                            sizeof *copy);
            }
            *copy = *src_entry;
            HashTableInsertResult r =
                hashtable_insert(dst->entries, key, key_len, copy);
            if (r != HASHTABLE_OK) {
                host_abortf("counter_merge: hashtable_insert -> %s",
                            hashtable_insert_result_name(r));
            }
        }
    }
}

void counter_inc(Counter *counter, ClientId client_id, uint32_t amount) {
    void *entry_ptr;
    if (hashtable_get(counter->entries, &client_id, sizeof(client_id),
                      &entry_ptr)) {
        CounterEntry *entry = entry_ptr;
        entry->inc += amount;
    } else {
        CounterEntry *entry = arena_alloc(counter->arena, sizeof *entry);
        if (!entry) {
            host_abortf("counter_inc: arena OOM (requested %zu bytes for "
                        "CounterEntry)",
                        sizeof *entry);
        }
        entry->client_id = client_id;
        entry->inc = amount;
        entry->dec = 0;
        HashTableInsertResult r = hashtable_insert(counter->entries, &client_id,
                                                   sizeof(client_id), entry);
        if (r != HASHTABLE_OK) {
            host_abortf("counter_inc: hashtable_insert -> %s",
                        hashtable_insert_result_name(r));
        }
    }
}

void counter_dec(Counter *counter, ClientId client_id, uint32_t amount) {
    void *entry_ptr;
    if (hashtable_get(counter->entries, &client_id, sizeof(client_id),
                      &entry_ptr)) {
        CounterEntry *entry = entry_ptr;
        entry->dec += amount;
    } else {
        CounterEntry *entry = arena_alloc(counter->arena, sizeof *entry);
        if (!entry) {
            host_abortf("counter_dec: arena OOM (requested %zu bytes for "
                        "CounterEntry)",
                        sizeof *entry);
        }
        entry->client_id = client_id;
        entry->inc = 0;
        entry->dec = amount;
        HashTableInsertResult r = hashtable_insert(counter->entries, &client_id,
                                                   sizeof(client_id), entry);
        if (r != HASHTABLE_OK) {
            host_abortf("counter_dec: hashtable_insert -> %s",
                        hashtable_insert_result_name(r));
        }
    }
}
