// Death test: cross-arena composite LWW must host_abort.
//
// src holds a composite (Counter) at "votes" but dst has no entry. map_merge's
// LWW fallthrough must abort rather than store a cross-arena pointer.
// Deterministic id derivation (PR 5) keeps this path unreachable in normal
// use; the abort guards against silent dangling-pointer corruption.
//
// The Makefile target inverts the exit status: success means the binary died.

#include "arena.h"
#include "clientid.h"
#include "counter.h"
#include "element.h"
#include "elementid.h"
#include "map.h"
#include "stamp.h"
#include <stdint.h>
#include <stdio.h>

static ClientId cid(uint8_t first_byte) {
    uint8_t b[16] = {0};
    b[0] = first_byte;
    return clientid_from_bytes(b);
}

static ElementId eid(uint8_t origin_byte, uint64_t seq) {
    return elementid_new(cid(origin_byte), seq);
}

int main(void) {
    Arena *ad = arena_create();
    Arena *as = arena_create();
    Map *dst = map_create(ad, eid(0xFF, 0));
    Map *src = map_create(as, eid(0xFF, 0));
    Counter *sc = counter_create(as, eid(7, 1));
    counter_inc(sc, cid(1), 3);
    map_set(src, (const void *)"votes", 5, element_counter(sc),
            (Stamp){.lamport = 5, .client_id = cid(1)});

    map_merge(dst, src); // expected: host_abort, process dies

    fprintf(stderr,
            "test_map_abort: map_merge returned without aborting (bug)\n");
    return 0; // 0 exit will be inverted by Makefile -> failure
}
