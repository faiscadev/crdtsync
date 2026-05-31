#ifndef _CRDT_COUNTER_H
#define _CRDT_COUNTER_H

#include "arena.h"
#include "clientid.h"
#include "hashtable.h"
#include <stdint.h>

typedef struct CounterEntry {
    ClientId client_id;
    uint32_t inc;
    uint32_t dec;
} CounterEntry;

typedef struct Counter {
    Arena *arena;
    HashTable *entries; // client_id (uint32_t) -> CounterEntry
} Counter;

Counter *counter_create(Arena *arena);

int64_t counter_read(const Counter *counter);

void counter_merge(Counter *dst, const Counter *src);

void counter_inc(Counter *counter, ClientId client_id, uint32_t amount);

void counter_dec(Counter *counter, ClientId client_id, uint32_t amount);

#endif // _CRDT_COUNTER_H
