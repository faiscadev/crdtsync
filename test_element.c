#include "arena.h"
#include "clientid.h"
#include "counter.h"
#include "element.h"
#include "map.h"
#include "register.h"
#include "scalar.h"
#include "stamp.h"
#include "string.h"
#include "test_util.h"

// Helpers.

static ClientId cid(uint8_t first_byte) {
    uint8_t b[16] = {0};
    b[0] = first_byte;
    return clientid_from_bytes(b);
}

static Stamp stmp(uint64_t lamport, uint8_t client_first_byte) {
    return (Stamp){.lamport = lamport, .client_id = cid(client_first_byte)};
}

// --- constructors set kind + payload ---

TEST(scalar_constructor_sets_kind_and_value) {
    Element e = element_scalar(scalar_int(42));
    ASSERT_EQ(element_kind(e), ELEMENT_SCALAR);
    ASSERT(scalar_eq(e.as.scalar, scalar_int(42)));
}

TEST(register_constructor_sets_kind_and_pointer) {
    Arena *a = arena_create();
    Register *r = register_create(a, scalar_int(1), stmp(1, 1));
    Element e = element_register(r);
    ASSERT_EQ(element_kind(e), ELEMENT_REGISTER);
    ASSERT(e.as.reg == r);
    arena_destroy(a);
}

TEST(counter_constructor_sets_kind_and_pointer) {
    Arena *a = arena_create();
    Counter *c = counter_create(a);
    Element e = element_counter(c);
    ASSERT_EQ(element_kind(e), ELEMENT_COUNTER);
    ASSERT(e.as.counter == c);
    arena_destroy(a);
}

TEST(map_constructor_sets_kind_and_pointer) {
    Arena *a = arena_create();
    Map *m = map_create(a);
    Element e = element_map(m);
    ASSERT_EQ(element_kind(e), ELEMENT_MAP);
    ASSERT(e.as.map == m);
    arena_destroy(a);
}

// --- kind name (for diagnostics) ---

TEST(kind_name_scalar) {
    ASSERT(strcmp(element_kind_name(ELEMENT_SCALAR), "SCALAR") == 0);
}

TEST(kind_name_register) {
    ASSERT(strcmp(element_kind_name(ELEMENT_REGISTER), "REGISTER") == 0);
}

TEST(kind_name_counter) {
    ASSERT(strcmp(element_kind_name(ELEMENT_COUNTER), "COUNTER") == 0);
}

TEST(kind_name_map) {
    ASSERT(strcmp(element_kind_name(ELEMENT_MAP), "MAP") == 0);
}

// --- merge dispatches by kind to the underlying _merge ---

// REGISTER: element_merge must call register_merge — dst takes src's value when
// src has the higher Stamp.
TEST(merge_register_takes_newer_value) {
    Arena *ad = arena_create();
    Arena *as = arena_create();
    Register *dst = register_create(ad, scalar_int(10), stmp(1, 1));
    Register *src = register_create(as, scalar_int(20), stmp(5, 1)); // newer

    element_merge(element_register(dst), element_register(src));

    ASSERT(scalar_eq(register_read(dst), scalar_int(20)));
    arena_destroy(ad);
    arena_destroy(as);
}

// COUNTER: element_merge must call counter_merge — per-client max on inc/dec.
TEST(merge_counter_unions_clients) {
    Arena *ad = arena_create();
    Arena *as = arena_create();
    Counter *dst = counter_create(ad);
    Counter *src = counter_create(as);
    counter_inc(dst, cid(1), 5);
    counter_inc(src, cid(2), 3);

    element_merge(element_counter(dst), element_counter(src));

    ASSERT_EQ(counter_read(dst), 8); // 5 from client 1 + 3 from client 2
    arena_destroy(ad);
    arena_destroy(as);
}

// MAP: element_merge must call map_merge — dst takes src's slots when src has
// the higher Stamp.
TEST(merge_map_takes_newer_slot) {
    Arena *ad = arena_create();
    Arena *as = arena_create();
    Map *dst = map_create(ad);
    Map *src = map_create(as);

    const uint8_t *k = (const uint8_t *)"k";
    size_t klen = 1;
    map_set(dst, k, klen, scalar_int(10), stmp(1, 1));
    map_set(src, k, klen, scalar_int(20), stmp(5, 1)); // newer

    element_merge(element_map(dst), element_map(src));

    Scalar out;
    ASSERT(map_get(dst, k, klen, &out) == true);
    ASSERT(scalar_eq(out, scalar_int(20)));
    arena_destroy(ad);
    arena_destroy(as);
}

// REGISTER: merge does not mutate src.
TEST(merge_register_does_not_mutate_src) {
    Arena *ad = arena_create();
    Arena *as = arena_create();
    Register *dst = register_create(ad, scalar_int(99), stmp(10, 1)); // newer
    Register *src = register_create(as, scalar_int(7), stmp(1, 1));

    element_merge(element_register(dst), element_register(src));

    ASSERT(scalar_eq(register_read(src), scalar_int(7)));
    arena_destroy(ad);
    arena_destroy(as);
}

// COUNTER: merge does not mutate src.
TEST(merge_counter_does_not_mutate_src) {
    Arena *ad = arena_create();
    Arena *as = arena_create();
    Counter *dst = counter_create(ad);
    Counter *src = counter_create(as);
    counter_inc(src, cid(1), 3);

    element_merge(element_counter(dst), element_counter(src));

    ASSERT_EQ(counter_read(src), 3);
    arena_destroy(ad);
    arena_destroy(as);
}

// MAP: merge does not mutate src.
TEST(merge_map_does_not_mutate_src) {
    Arena *ad = arena_create();
    Arena *as = arena_create();
    Map *dst = map_create(ad);
    Map *src = map_create(as);
    const uint8_t *k = (const uint8_t *)"k";
    map_set(src, k, 1, scalar_int(7), stmp(1, 1));

    element_merge(element_map(dst), element_map(src));

    Scalar out;
    ASSERT(map_get(src, k, 1, &out) == true);
    ASSERT(scalar_eq(out, scalar_int(7)));
    arena_destroy(ad);
    arena_destroy(as);
}

// Mixed-kind helper: an Element of any composite kind round-trips through the
// constructor / accessor pair without losing the payload.
TEST(round_trip_via_kind_and_payload) {
    Arena *a = arena_create();
    Counter *c = counter_create(a);
    Element e = element_counter(c);
    ASSERT_EQ(element_kind(e), ELEMENT_COUNTER);
    ASSERT(e.as.counter == c);
    arena_destroy(a);
}

int main(void) {
    RUN(scalar_constructor_sets_kind_and_value);
    RUN(register_constructor_sets_kind_and_pointer);
    RUN(counter_constructor_sets_kind_and_pointer);
    RUN(map_constructor_sets_kind_and_pointer);

    RUN(kind_name_scalar);
    RUN(kind_name_register);
    RUN(kind_name_counter);
    RUN(kind_name_map);

    RUN(merge_register_takes_newer_value);
    RUN(merge_counter_unions_clients);
    RUN(merge_map_takes_newer_slot);

    RUN(merge_register_does_not_mutate_src);
    RUN(merge_counter_does_not_mutate_src);
    RUN(merge_map_does_not_mutate_src);

    RUN(round_trip_via_kind_and_payload);

    TEST_SUMMARY();
}
