#include "counter.h"
#include "hashtable.h"
#include "host.h"

struct Counter {
    ElementId id;
    HashTable *entries; // ClientId -> CounterEntry

    size_t refcount;
    bool displaced;
};

static inline uint32_t max_u32(uint32_t a, uint32_t b) {
    if (a > b) {
        return a;
    }
    return b;
}

Counter *counter_create(ElementId id) {
    Counter *counter = host_malloc(sizeof(Counter));
    if (!counter) {
        host_abortf(
            "counter_create: host_malloc OOM (requested %zu bytes for Counter)",
            sizeof(Counter));
    }
    counter->id = id;
    counter->entries = hashtable_create();
    if (!counter->entries) {
        host_free(counter);
        host_abort("counter_create: hashtable_create OOM (per-client tallies "
                   "table)");
    }
    counter->refcount = 1;
    counter->displaced = false;
    return counter;
}

ElementId counter_id(const Counter *counter) { return counter->id; }

// Get-or-create the per-client CounterEntry for `client_id`. Initializes a
// fresh entry to {inc=0, dec=0} on first call for a given client.
static CounterEntry *counter_entry_for(Counter *counter, ClientId client_id) {
    void *entry_ptr;
    if (hashtable_get(counter->entries, &client_id, sizeof(client_id),
                      &entry_ptr)) {
        return entry_ptr;
    }
    CounterEntry *entry = host_malloc(sizeof *entry);
    if (!entry) {
        host_abortf(
            "counter: host_malloc OOM (requested %zu bytes for CounterEntry)",
            sizeof *entry);
    }
    entry->client_id = client_id;
    entry->inc = 0;
    entry->dec = 0;
    HashTableInsertResult r = hashtable_insert(counter->entries, &client_id,
                                               sizeof(client_id), entry);
    if (r != HASHTABLE_OK) {
        host_abortf("counter: hashtable_insert -> %s",
                    hashtable_insert_result_name(r));
    }
    return entry;
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
            CounterEntry *copy = host_malloc(sizeof *copy);
            if (!copy) {
                host_abortf("counter_merge: host_malloc OOM (requested %zu "
                            "bytes for CounterEntry)",
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
    counter_entry_for(counter, client_id)->inc += amount;
}

void counter_dec(Counter *counter, ClientId client_id, uint32_t amount) {
    counter_entry_for(counter, client_id)->dec += amount;
}

Counter *counter_clone(const Counter *counter) {
    Counter *clone = counter_create(counter->id);
    HashTableIter it = hashtable_iter(counter->entries);
    const void *key;
    size_t key_len;
    void *value;
    while (hashtable_iter_next(&it, &key, &key_len, &value)) {
        CounterEntry *entry = value;
        CounterEntry *entry_copy = host_malloc(sizeof *entry_copy);
        if (!entry_copy) {
            host_abortf("counter_clone: host_malloc OOM (requested %zu bytes "
                        "for CounterEntry)",
                        sizeof *entry_copy);
        }
        *entry_copy = *entry;
        HashTableInsertResult r =
            hashtable_insert(clone->entries, key, key_len, entry_copy);
        if (r != HASHTABLE_OK) {
            host_abortf("counter_clone: hashtable_insert -> %s",
                        hashtable_insert_result_name(r));
        }
    }
    return clone;
}

void counter_acquire(Counter *counter) { counter->refcount++; }

void counter_release(Counter *counter) {
    if (counter->refcount == 0) {
        host_abort("counter_release: refcount already zero");
    }
    counter->refcount--;
    if (counter->refcount == 0) {
        // Per-client entries were host_malloc'd individually; free each before
        // tearing down the table.
        HashTableIter it = hashtable_iter(counter->entries);
        const void *key;
        size_t key_len;
        void *value;
        while (hashtable_iter_next(&it, &key, &key_len, &value)) {
            host_free(value);
        }
        hashtable_destroy(counter->entries);
        host_free(counter);
    }
}

void counter_displace(Counter *counter) { counter->displaced = true; }

bool counter_is_displaced(const Counter *counter) { return counter->displaced; }
