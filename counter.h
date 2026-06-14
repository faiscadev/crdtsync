#ifndef _CRDT_COUNTER_H
#define _CRDT_COUNTER_H

// PN-Counter: integer counter with concurrent increments and decrements.
// Identity: each Counter is stamped with an ElementId at create, exposed
// via counter_id. Two replicas independently creating "the same Counter
// at the same slot" derive identical ids via
// elementid_derive(parent.id, key, ELEMENT_COUNTER), which is what
// map_merge's recursive guard uses to know they refer to the same logical
// element.
//
// Semantics:
//   - Per-client (inc, dec) tallies, one CounterEntry per ClientId that
//     ever wrote to this Counter. counter_inc / counter_dec add into the
//     calling client's own tallies.
//   - counter_read returns sum over all clients of (inc - dec).
//   - counter_merge unions src into dst per-direction: dst's entry for
//     each ClientId becomes (max(dst.inc, src.inc), max(dst.dec, src.dec)).
//     Merge is NOT addition — replicas may have observed the same writes
//     concurrently, so max is what makes the merge idempotent / commutative
//     / associative.
//   - Increments and decrements use uint32_t to keep per-direction max
//     well-defined; counter_read widens to int64_t for the signed total.
//
// Ownership:
//   - Per-client entries live in the Counter's arena.
//
// Lifetime: Counter must not outlive its arena.

#include "arena.h"
#include "clientid.h"
#include "elementid.h"
#include "hashtable.h"
#include <stdint.h>

typedef struct CounterEntry {
    ClientId client_id;
    uint32_t inc;
    uint32_t dec;
} CounterEntry;

typedef struct Counter Counter;

Counter *counter_create(Arena *arena, ElementId id);

ElementId counter_id(const Counter *counter);

int64_t counter_read(const Counter *counter);

void counter_merge(Counter *dst, const Counter *src);

void counter_inc(Counter *counter, ClientId client_id, uint32_t amount);

void counter_dec(Counter *counter, ClientId client_id, uint32_t amount);

Counter *counter_clone(Arena *arena, const Counter *counter);

#endif // _CRDT_COUNTER_H
