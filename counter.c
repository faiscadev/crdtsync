#include "counter.h"
#include "arena.h"
#include "hashtable.h"
#include <stdio.h>

static inline uint32_t max_u32(uint32_t a, uint32_t b) {
    if (a > b) {
        return a;
    }

    return b;
}

Counter *counter_create(Arena *arena) {
    Counter *counter = arena_alloc(arena, sizeof(Counter));
    counter->arena = arena;
    counter->entries = hashtable_create(arena);
    return counter;
}

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
            *copy = *src_entry;
            hashtable_insert(dst->entries, key, key_len, copy);
        }
    }
}

void counter_inc(Counter *counter, uint32_t client_id, uint32_t amount) {
    void *entry_ptr;
    if (hashtable_get(counter->entries, &client_id, sizeof(client_id),
                      &entry_ptr)) {
        CounterEntry *entry = entry_ptr;
        entry->inc += amount;
    } else {
        CounterEntry *entry = arena_alloc(counter->arena, sizeof *entry);
        entry->client_id = client_id;
        entry->inc = amount;
        entry->dec = 0;
        hashtable_insert(counter->entries, &client_id, sizeof(client_id),
                         entry);
    }
}

void counter_dec(Counter *counter, uint32_t client_id, uint32_t amount) {
    void *entry_ptr;
    if (hashtable_get(counter->entries, &client_id, sizeof(client_id),
                      &entry_ptr)) {
        CounterEntry *entry = entry_ptr;
        entry->dec += amount;
    } else {
        CounterEntry *entry = arena_alloc(counter->arena, sizeof *entry);
        entry->client_id = client_id;
        entry->inc = 0;
        entry->dec = amount;
        hashtable_insert(counter->entries, &client_id, sizeof(client_id),
                         entry);
    }
}
