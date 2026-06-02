#include "arena.h"
#include "clientid.h"
#include "counter.h"
#include "element.h"
#include "elementid.h"
#include "map.h"
#include "register.h"
#include "scalar.h"
#include "stamp.h"
#include "string.h"
#include "test_util.h"

// Helpers — keep tests compact.

static ClientId cid(uint8_t first_byte) {
    uint8_t b[16] = {0};
    b[0] = first_byte;
    return clientid_from_bytes(b);
}

static ElementId eid(uint8_t origin_byte, uint64_t seq) {
    return elementid_new(cid(origin_byte), seq);
}

// Default id for the Map under test when identity does not matter.
static ElementId default_id(void) { return eid(0xFF, 0); }

static Stamp stmp(uint64_t lamport, uint8_t client_first_byte) {
    return (Stamp){.lamport = lamport, .client_id = cid(client_first_byte)};
}

// String-key shorthand: expands to (bytes, length) without the NUL terminator.
#define SK(s) ((const void *)(s)), strlen(s)

// Element wrappers for readability at the call site.
#define EI(n) element_scalar(scalar_int(n))
#define ES(p, n) element_scalar(scalar_string((const uint8_t *)(p), (n)))

static Map *fresh(void) {
    Arena *arena = arena_create();
    return map_create(arena, default_id());
}

// Assert helper: out is a SCALAR element equal to expected Scalar.
#define ASSERT_SCALAR_EQ(out, expected)                                        \
    do {                                                                       \
        ASSERT_EQ(element_kind(out), ELEMENT_SCALAR);                          \
        ASSERT(scalar_eq((out).as.scalar, (expected)));                        \
    } while (0)

// --- identity ---

TEST(map_create_stores_id) {
    Arena *a = arena_create();
    ElementId id = eid(7, 42);
    Map *m = map_create(a, id);
    ASSERT(elementid_eq(map_id(m), id) == true);
    arena_destroy(a);
}

// --- local set / get (scalar slots) ---

TEST(empty_get_returns_false) {
    Map *m = fresh();
    Element out;
    ASSERT(map_get(m, SK("missing"), &out) == false);
}

TEST(set_then_get) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(42), stmp(1, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(42));
}

TEST(set_overwrites_with_newer_stamp) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(1, 1));
    map_set(m, SK("k"), EI(20), stmp(2, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
}

TEST(set_lower_stamp_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(20), stmp(5, 1));
    map_set(m, SK("k"), EI(10), stmp(3, 1)); // older — ignored
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
}

TEST(set_equal_lamport_higher_client_wins) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(5, 1));
    map_set(m, SK("k"), EI(20), stmp(5, 2)); // same lamport, > client
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
}

TEST(set_equal_lamport_lower_client_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(20), stmp(5, 2));
    map_set(m, SK("k"), EI(10), stmp(5, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
}

TEST(set_same_stamp_idempotent) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(42), stmp(5, 1));
    map_set(m, SK("k"), EI(42), stmp(5, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(42));
}

// A newer write can change the Scalar kind.
TEST(set_can_change_value_kind) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(42), stmp(1, 1));
    map_set(m, SK("k"), ES("hi", 2), stmp(2, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_string((const uint8_t *)"hi", 2));
}

// Distinct keys are independent — writing one must not affect the other.
TEST(distinct_keys_are_independent) {
    Map *m = fresh();
    map_set(m, SK("a"), EI(1), stmp(1, 1));
    map_set(m, SK("b"), EI(2), stmp(1, 1));
    Element a, b;
    ASSERT(map_get(m, SK("a"), &a) == true);
    ASSERT(map_get(m, SK("b"), &b) == true);
    ASSERT_SCALAR_EQ(a, scalar_int(1));
    ASSERT_SCALAR_EQ(b, scalar_int(2));
}

// Headline reason for byte keys: keys with embedded NUL bytes must be
// distinguished past the NUL.
TEST(keys_with_embedded_nul_are_distinct) {
    Map *m = fresh();
    uint8_t k1[3] = {0x01, 0x00, 0x02};
    uint8_t k2[3] = {0x01, 0x00, 0x03};
    map_set(m, k1, sizeof k1, EI(1), stmp(1, 1));
    map_set(m, k2, sizeof k2, EI(2), stmp(1, 1));
    Element v1, v2;
    ASSERT(map_get(m, k1, sizeof k1, &v1) == true);
    ASSERT(map_get(m, k2, sizeof k2, &v2) == true);
    ASSERT_SCALAR_EQ(v1, scalar_int(1));
    ASSERT_SCALAR_EQ(v2, scalar_int(2));
}

// --- delete / tombstones ---

TEST(delete_makes_get_return_false) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(42), stmp(1, 1));
    map_delete(m, SK("k"), stmp(2, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == false);
}

// A delete with a stamp older than the existing value must NOT clobber.
TEST(delete_with_lower_stamp_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(42), stmp(5, 1));
    map_delete(m, SK("k"), stmp(3, 1)); // older — ignored
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(42));
}

// After delete, a set with a higher stamp must resurrect the slot.
TEST(set_after_delete_with_higher_stamp_resurrects) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(2, 1));
    map_set(m, SK("k"), EI(20), stmp(3, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
}

// After delete, a set with a lower-or-equal stamp must NOT resurrect.
TEST(set_after_delete_with_lower_stamp_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(5, 1));
    map_set(m, SK("k"), EI(20), stmp(3, 1)); // older than delete
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == false);
}

// Concurrent set vs delete: stamp decides which wins. Here delete has the
// higher stamp, so the slot ends up tombstoned.
TEST(set_vs_delete_higher_stamp_wins_delete) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(5, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == false);
}

TEST(delete_idempotent_same_stamp) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(5, 1));
    map_delete(m, SK("k"), stmp(5, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == false);
}

// Deleting a key that was never set is a no-op but installs a tombstone with
// the given stamp — a later set with a lower stamp must still be rejected.
TEST(delete_absent_key_still_installs_tombstone) {
    Map *m = fresh();
    map_delete(m, SK("ghost"), stmp(10, 1));
    map_set(m, SK("ghost"), EI(1), stmp(5, 1)); // older than delete
    Element out;
    ASSERT(map_get(m, SK("ghost"), &out) == false);
}

// --- map_size ---

TEST(size_zero_initially) {
    Map *m = fresh();
    ASSERT_EQ(map_size(m), 0);
}

TEST(size_counts_live_entries) {
    Map *m = fresh();
    map_set(m, SK("a"), EI(1), stmp(1, 1));
    map_set(m, SK("b"), EI(2), stmp(1, 1));
    map_set(m, SK("c"), EI(3), stmp(1, 1));
    ASSERT_EQ(map_size(m), 3);
}

TEST(size_excludes_tombstones) {
    Map *m = fresh();
    map_set(m, SK("a"), EI(1), stmp(1, 1));
    map_set(m, SK("b"), EI(2), stmp(1, 1));
    map_delete(m, SK("b"), stmp(2, 1));
    ASSERT_EQ(map_size(m), 1);
}

TEST(size_recovers_on_resurrect) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(1), stmp(1, 1));
    map_delete(m, SK("k"), stmp(2, 1));
    ASSERT_EQ(map_size(m), 0);
    map_set(m, SK("k"), EI(2), stmp(3, 1));
    ASSERT_EQ(map_size(m), 1);
}

// --- composite slot reads ---

TEST(set_counter_then_get_returns_element_counter) {
    Arena *ar = arena_create();
    Map *m = map_create(ar, default_id());
    Counter *c = counter_create(ar, eid(1, 1));
    counter_inc(c, cid(1), 5);
    map_set(m, SK("votes"), element_counter(c), stmp(1, 1));

    Element out;
    ASSERT(map_get(m, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT_EQ(counter_read(out.as.counter), 5);
    arena_destroy(ar);
}

TEST(set_register_then_get_returns_element_register) {
    Arena *ar = arena_create();
    Map *m = map_create(ar, default_id());
    Register *r = register_create(ar, eid(1, 1), scalar_int(7), stmp(1, 1));
    map_set(m, SK("title"), element_register(r), stmp(1, 1));

    Element out;
    ASSERT(map_get(m, SK("title"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    ASSERT(scalar_eq(register_read(out.as.reg), scalar_int(7)));
    arena_destroy(ar);
}

TEST(set_nested_map_then_get_returns_element_map) {
    Arena *ar = arena_create();
    Map *outer = map_create(ar, default_id());
    Map *inner = map_create(ar, eid(1, 1));
    map_set(inner, SK("a"), EI(1), stmp(1, 1));
    map_set(outer, SK("child"), element_map(inner), stmp(1, 1));

    Element out;
    ASSERT(map_get(outer, SK("child"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_MAP);
    Element inner_out;
    ASSERT(map_get(out.as.map, SK("a"), &inner_out) == true);
    ASSERT_SCALAR_EQ(inner_out, scalar_int(1));
    arena_destroy(ar);
}

// --- merge (two replicas, scalar slots) ---

TEST(merge_disjoint_keys_unions) {
    Map *a = map_create(arena_create(), default_id());
    Map *b = map_create(arena_create(), default_id());
    map_set(a, SK("x"), EI(1), stmp(1, 1));
    map_set(b, SK("y"), EI(2), stmp(1, 2));

    map_merge(a, b);
    Element x, y;
    ASSERT(map_get(a, SK("x"), &x) == true);
    ASSERT(map_get(a, SK("y"), &y) == true);
    ASSERT_SCALAR_EQ(x, scalar_int(1));
    ASSERT_SCALAR_EQ(y, scalar_int(2));
    ASSERT_EQ(map_size(a), 2);
}

TEST(merge_same_key_newer_wins) {
    Map *a = map_create(arena_create(), default_id());
    Map *b = map_create(arena_create(), default_id());
    map_set(a, SK("k"), EI(10), stmp(1, 1));
    map_set(b, SK("k"), EI(20), stmp(2, 2)); // newer

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
}

TEST(merge_src_older_loses) {
    Map *a = map_create(arena_create(), default_id());
    Map *b = map_create(arena_create(), default_id());
    map_set(a, SK("k"), EI(20), stmp(5, 1)); // newer
    map_set(b, SK("k"), EI(10), stmp(2, 2));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
}

// Concurrent: dst has a value, src has a delete with a higher stamp.
TEST(merge_delete_beats_older_set) {
    Map *a = map_create(arena_create(), default_id());
    Map *b = map_create(arena_create(), default_id());
    map_set(a, SK("k"), EI(10), stmp(1, 1));
    map_delete(b, SK("k"), stmp(5, 1)); // newer

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == false);
}

// Concurrent: dst has a delete, src has a value with a higher stamp.
TEST(merge_set_beats_older_delete) {
    Map *a = map_create(arena_create(), default_id());
    Map *b = map_create(arena_create(), default_id());
    map_delete(a, SK("k"), stmp(1, 1));
    map_set(b, SK("k"), EI(42), stmp(5, 1)); // newer

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(42));
}

TEST(merge_commutative) {
    // path 1: a <- b
    Map *a1 = map_create(arena_create(), default_id());
    Map *b1 = map_create(arena_create(), default_id());
    map_set(a1, SK("k"), EI(10), stmp(5, 1));
    map_set(b1, SK("k"), EI(20), stmp(5, 2));
    map_merge(a1, b1);

    // path 2: b <- a
    Map *a2 = map_create(arena_create(), default_id());
    Map *b2 = map_create(arena_create(), default_id());
    map_set(a2, SK("k"), EI(10), stmp(5, 1));
    map_set(b2, SK("k"), EI(20), stmp(5, 2));
    map_merge(b2, a2);

    Element v1, v2;
    ASSERT(map_get(a1, SK("k"), &v1) == true);
    ASSERT(map_get(b2, SK("k"), &v2) == true);
    ASSERT_EQ(element_kind(v1), ELEMENT_SCALAR);
    ASSERT_EQ(element_kind(v2), ELEMENT_SCALAR);
    ASSERT(scalar_eq(v1.as.scalar, v2.as.scalar));
    ASSERT(scalar_eq(v1.as.scalar, scalar_int(20)));
}

TEST(merge_idempotent) {
    Map *a = map_create(arena_create(), default_id());
    Map *b = map_create(arena_create(), default_id());
    map_set(a, SK("k"), EI(10), stmp(1, 1));
    map_set(b, SK("k"), EI(20), stmp(2, 1));

    map_merge(a, b);
    Element once;
    ASSERT(map_get(a, SK("k"), &once) == true);
    map_merge(a, b);
    Element twice;
    ASSERT(map_get(a, SK("k"), &twice) == true);
    ASSERT_EQ(element_kind(once), ELEMENT_SCALAR);
    ASSERT_EQ(element_kind(twice), ELEMENT_SCALAR);
    ASSERT(scalar_eq(once.as.scalar, twice.as.scalar));
    ASSERT(scalar_eq(twice.as.scalar, scalar_int(20)));
}

TEST(merge_associative) {
    // (a <- b) <- c
    Map *a = map_create(arena_create(), default_id());
    Map *b = map_create(arena_create(), default_id());
    Map *c = map_create(arena_create(), default_id());
    map_set(a, SK("k"), EI(10), stmp(1, 1));
    map_set(b, SK("k"), EI(20), stmp(2, 1));
    map_set(c, SK("k"), EI(30), stmp(3, 1));
    map_merge(a, b);
    map_merge(a, c);

    // a <- (b <- c)
    Map *a2 = map_create(arena_create(), default_id());
    Map *b2 = map_create(arena_create(), default_id());
    Map *c2 = map_create(arena_create(), default_id());
    map_set(a2, SK("k"), EI(10), stmp(1, 1));
    map_set(b2, SK("k"), EI(20), stmp(2, 1));
    map_set(c2, SK("k"), EI(30), stmp(3, 1));
    map_merge(b2, c2);
    map_merge(a2, b2);

    Element v1, v2;
    ASSERT(map_get(a, SK("k"), &v1) == true);
    ASSERT(map_get(a2, SK("k"), &v2) == true);
    ASSERT_EQ(element_kind(v1), ELEMENT_SCALAR);
    ASSERT_EQ(element_kind(v2), ELEMENT_SCALAR);
    ASSERT(scalar_eq(v1.as.scalar, v2.as.scalar));
    ASSERT(scalar_eq(v1.as.scalar, scalar_int(30)));
}

TEST(merge_does_not_mutate_src) {
    Map *a = map_create(arena_create(), default_id());
    Map *b = map_create(arena_create(), default_id());
    map_set(a, SK("k"), EI(99), stmp(10, 1)); // newer
    map_set(b, SK("k"), EI(7), stmp(1, 1));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(b, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(7)); // b unchanged
}

// When merge accepts a winning string value from src, dst must own its own
// copy in dst's arena. Mutating the source bytes after merge must not affect
// dst's stored value.
TEST(merge_copies_string_into_dst_arena) {
    Map *a = map_create(arena_create(), default_id());
    Map *b = map_create(arena_create(), default_id());

    uint8_t src_bytes[8];
    memcpy(src_bytes, "hello", 5);

    map_set(a, SK("k"), EI(0), stmp(1, 1));
    map_set(b, SK("k"), ES(src_bytes, 5), stmp(5, 1));

    map_merge(a, b); // a takes b's string

    src_bytes[0] = 'X';
    src_bytes[1] = 'X';

    Element out;
    ASSERT(map_get(a, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_string((const uint8_t *)"hello", 5));
}

// Tombstones survive merge: dst with a tombstone merged with src that has an
// older value must keep the tombstone (the higher stamp wins).
TEST(merge_preserves_tombstone_against_older_set) {
    Map *a = map_create(arena_create(), default_id());
    Map *b = map_create(arena_create(), default_id());
    map_delete(a, SK("k"), stmp(5, 1));
    map_set(b, SK("k"), EI(10), stmp(2, 1));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == false);
}

// --- nested merge: same id same kind recurses into element_merge ---

// Two replicas hold the same Counter at "votes" (same id). Merge must combine
// their per-client tallies via counter_merge, NOT do LWW on the slot stamp.
// The dst slot has the OLDER stamp on purpose: if the implementation chose
// LWW, dst would inherit src's counter (=3) instead of the union (=8).
TEST(merge_same_id_counter_recurses) {
    Arena *ad = arena_create();
    Arena *as = arena_create();
    ElementId votes_id = eid(7, 1);

    Map *dst = map_create(ad, default_id());
    Counter *dc = counter_create(ad, votes_id);
    counter_inc(dc, cid(1), 5);
    map_set(dst, SK("votes"), element_counter(dc),
            stmp(1, 1)); // older slot stamp

    Map *src = map_create(as, default_id());
    Counter *sc = counter_create(as, votes_id);
    counter_inc(sc, cid(2), 3);
    map_set(src, SK("votes"), element_counter(sc),
            stmp(10, 1)); // newer slot stamp

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT_EQ(counter_read(out.as.counter), 8); // unioned, not replaced
    arena_destroy(ad);
    arena_destroy(as);
}

// Same shape with Register: same id → element_merge (register_merge by stamp).
// Pick stamps so register_merge picks src's value; that's distinct from "dst
// took src's whole Register" because dst's Register pointer must be preserved.
TEST(merge_same_id_register_recurses) {
    Arena *ad = arena_create();
    Arena *as = arena_create();
    ElementId reg_id = eid(7, 1);

    Map *dst = map_create(ad, default_id());
    Register *dr = register_create(ad, reg_id, scalar_int(10), stmp(1, 1));
    map_set(dst, SK("title"), element_register(dr), stmp(1, 1));

    Map *src = map_create(as, default_id());
    Register *sr = register_create(as, reg_id, scalar_int(20), stmp(5, 1));
    map_set(src, SK("title"), element_register(sr), stmp(1, 1));

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("title"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    // dst kept its OWN Register pointer; that Register absorbed src's value.
    ASSERT(out.as.reg == dr);
    ASSERT(scalar_eq(register_read(dr), scalar_int(20)));
    arena_destroy(ad);
    arena_destroy(as);
}

// Same shape with nested Map: same id → element_merge recurses into map_merge
// on the inner maps. Inner slot from src must show up in dst's inner map.
TEST(merge_same_id_nested_map_recurses) {
    Arena *ad = arena_create();
    Arena *as = arena_create();
    ElementId inner_id = eid(7, 1);

    Map *dst = map_create(ad, default_id());
    Map *di = map_create(ad, inner_id);
    map_set(di, SK("a"), EI(1), stmp(1, 1));
    map_set(dst, SK("child"), element_map(di), stmp(1, 1));

    Map *src = map_create(as, default_id());
    Map *si = map_create(as, inner_id);
    map_set(si, SK("b"), EI(2), stmp(1, 2));
    map_set(src, SK("child"), element_map(si), stmp(1, 1));

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("child"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_MAP);
    ASSERT(out.as.map == di); // dst kept its own inner Map pointer
    Element a_out, b_out;
    ASSERT(map_get(di, SK("a"), &a_out) == true);
    ASSERT(map_get(di, SK("b"), &b_out) == true);
    ASSERT_SCALAR_EQ(a_out, scalar_int(1));
    ASSERT_SCALAR_EQ(b_out, scalar_int(2));
    arena_destroy(ad);
    arena_destroy(as);
}

// Recursive merge does not touch src's composite — dst absorbs, src untouched.
TEST(merge_same_id_counter_does_not_mutate_src) {
    Arena *ad = arena_create();
    Arena *as = arena_create();
    ElementId votes_id = eid(7, 1);

    Map *dst = map_create(ad, default_id());
    Counter *dc = counter_create(ad, votes_id);
    counter_inc(dc, cid(1), 5);
    map_set(dst, SK("votes"), element_counter(dc), stmp(1, 1));

    Map *src = map_create(as, default_id());
    Counter *sc = counter_create(as, votes_id);
    counter_inc(sc, cid(2), 3);
    map_set(src, SK("votes"), element_counter(sc), stmp(1, 1));

    map_merge(dst, src);

    ASSERT_EQ(counter_read(sc), 3); // src counter unchanged
    arena_destroy(ad);
    arena_destroy(as);
}

int main(void) {
    RUN(map_create_stores_id);

    RUN(empty_get_returns_false);
    RUN(set_then_get);
    RUN(set_overwrites_with_newer_stamp);
    RUN(set_lower_stamp_ignored);
    RUN(set_equal_lamport_higher_client_wins);
    RUN(set_equal_lamport_lower_client_ignored);
    RUN(set_same_stamp_idempotent);
    RUN(set_can_change_value_kind);
    RUN(distinct_keys_are_independent);
    RUN(keys_with_embedded_nul_are_distinct);

    RUN(delete_makes_get_return_false);
    RUN(delete_with_lower_stamp_ignored);
    RUN(set_after_delete_with_higher_stamp_resurrects);
    RUN(set_after_delete_with_lower_stamp_ignored);
    RUN(set_vs_delete_higher_stamp_wins_delete);
    RUN(delete_idempotent_same_stamp);
    RUN(delete_absent_key_still_installs_tombstone);

    RUN(size_zero_initially);
    RUN(size_counts_live_entries);
    RUN(size_excludes_tombstones);
    RUN(size_recovers_on_resurrect);

    RUN(set_counter_then_get_returns_element_counter);
    RUN(set_register_then_get_returns_element_register);
    RUN(set_nested_map_then_get_returns_element_map);

    RUN(merge_disjoint_keys_unions);
    RUN(merge_same_key_newer_wins);
    RUN(merge_src_older_loses);
    RUN(merge_delete_beats_older_set);
    RUN(merge_set_beats_older_delete);
    RUN(merge_commutative);
    RUN(merge_idempotent);
    RUN(merge_associative);
    RUN(merge_does_not_mutate_src);
    RUN(merge_copies_string_into_dst_arena);
    RUN(merge_preserves_tombstone_against_older_set);

    RUN(merge_same_id_counter_recurses);
    RUN(merge_same_id_register_recurses);
    RUN(merge_same_id_nested_map_recurses);
    RUN(merge_same_id_counter_does_not_mutate_src);

    TEST_SUMMARY();
}
