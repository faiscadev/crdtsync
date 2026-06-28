#ifndef _CRDT_COUNTER_H
#define _CRDT_COUNTER_H

// PN-Counter: integer counter with concurrent increments and decrements.
// Identity: each Counter is stamped with an ElementId at create, exposed
// via counter_id. Two replicas independently creating "the same Counter
// at the same slot" derive identical ids via
// elementid_derive(parent.id, key, key_len, ELEMENT_COUNTER), which is what
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
// Lifetime — refcounted:
//   - counter_create returns a Counter with refcount = 1; the creator owns
//     that ref.
//   - counter_acquire bumps the refcount; counter_release drops it.
//   - When refcount hits 0, the Counter is freed (per-client entries,
//     hashtable, struct). Standard C ownership semantics.
//   - The displacement signal is independent of refcount: a Counter
//     marked via counter_displace (called by the Map slot path when an
//     LWW displacement happens) stays alive as long as some holder still
//     has a ref. Mutations on a displaced Counter still mutate local
//     state but the Doc layer (when wired) skips op emission. Holders
//     can check counter_is_displaced to know their handle is no longer
//     installed in any slot.

#include "clientid.h"
#include "elementid.h"
#include "hashtable.h"
#include <stdbool.h>
#include <stdint.h>

typedef struct CounterEntry {
    ClientId client_id;
    uint32_t inc;
    uint32_t dec;
} CounterEntry;

typedef struct Counter Counter;

// Allocate a Counter via host_malloc with refcount=1. Caller owns one ref.
Counter *counter_create(ElementId id);

ElementId counter_id(const Counter *counter);

int64_t counter_read(const Counter *counter);

void counter_merge(Counter *dst, const Counter *src);

void counter_inc(Counter *counter, ClientId client_id, uint32_t amount);

void counter_dec(Counter *counter, ClientId client_id, uint32_t amount);

// Deep-copy the source Counter into a fresh allocation. Returned clone has
// refcount=1 and is NOT marked displaced (regardless of src's displaced
// flag — displacement is a per-instance signal, not part of the value).
Counter *counter_clone(const Counter *counter);

// Reference counting. Acquire bumps the refcount; release drops it. On
// reaching zero, the Counter is freed (per-client entries, hashtable,
// struct).
void counter_acquire(Counter *counter);
void counter_release(Counter *counter);

// Displacement signal — see lifetime notes above. counter_displace marks
// the Counter as no-longer-in-a-slot (Map slot path calls this when it
// LWW-displaces the slot's previous value). counter_is_displaced reads
// the flag.
void counter_displace(Counter *counter);
bool counter_is_displaced(const Counter *counter);

#endif // _CRDT_COUNTER_H
