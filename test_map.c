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
#include <stdio.h>

static ClientId cid(uint8_t first_byte) {
    uint8_t b[16] = {0};
    b[0] = first_byte;
    return clientid_from_bytes(b);
}

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
    return map_create(arena);
}

#define ASSERT_SCALAR_EQ(out, expected)                                        \
    do {                                                                       \
        ASSERT_EQ(element_kind(out), ELEMENT_SCALAR);                          \
        ASSERT(scalar_eq((out).as.scalar, (expected)));                        \
    } while (0)

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
    map_set(m, SK("k"), EI(10), stmp(3, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
}

TEST(set_equal_lamport_higher_client_wins) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(5, 1));
    map_set(m, SK("k"), EI(20), stmp(5, 2));
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

TEST(set_can_change_value_kind) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(42), stmp(1, 1));
    map_set(m, SK("k"), ES("hi", 2), stmp(2, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_string((const uint8_t *)"hi", 2));
}

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

TEST(delete_with_lower_stamp_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(42), stmp(5, 1));
    map_delete(m, SK("k"), stmp(3, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(42));
}

TEST(set_after_delete_with_higher_stamp_resurrects) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(2, 1));
    map_set(m, SK("k"), EI(20), stmp(3, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
}

TEST(set_after_delete_with_lower_stamp_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(5, 1));
    map_set(m, SK("k"), EI(20), stmp(3, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == false);
}

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

TEST(delete_absent_key_still_installs_tombstone) {
    Map *m = fresh();
    map_delete(m, SK("ghost"), stmp(10, 1));
    map_set(m, SK("ghost"), EI(1), stmp(5, 1));
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
    Map *m = map_create(ar);
    Counter *c = counter_create(ar);
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
    Map *m = map_create(ar);
    Register *r = register_create(ar, scalar_int(7), stmp(1, 1));
    map_set(m, SK("title"), element_register(r), stmp(1, 1));

    Element out;
    ASSERT(map_get(m, SK("title"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    ASSERT(scalar_eq(register_read(out.as.reg), scalar_int(7)));
    arena_destroy(ar);
}

TEST(set_nested_map_then_get_returns_element_map) {
    Arena *ar = arena_create();
    Map *outer = map_create(ar);
    Map *inner = map_create(ar);
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
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
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
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_set(a, SK("k"), EI(10), stmp(1, 1));
    map_set(b, SK("k"), EI(20), stmp(2, 2));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
}

TEST(merge_src_older_loses) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_set(a, SK("k"), EI(20), stmp(5, 1));
    map_set(b, SK("k"), EI(10), stmp(2, 2));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
}

TEST(merge_delete_beats_older_set) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_set(a, SK("k"), EI(10), stmp(1, 1));
    map_delete(b, SK("k"), stmp(5, 1));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == false);
}

TEST(merge_set_beats_older_delete) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_delete(a, SK("k"), stmp(1, 1));
    map_set(b, SK("k"), EI(42), stmp(5, 1));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(42));
}

TEST(merge_commutative) {
    Map *a1 = map_create(arena_create());
    Map *b1 = map_create(arena_create());
    map_set(a1, SK("k"), EI(10), stmp(5, 1));
    map_set(b1, SK("k"), EI(20), stmp(5, 2));
    map_merge(a1, b1);

    Map *a2 = map_create(arena_create());
    Map *b2 = map_create(arena_create());
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
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
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
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    Map *c = map_create(arena_create());
    map_set(a, SK("k"), EI(10), stmp(1, 1));
    map_set(b, SK("k"), EI(20), stmp(2, 1));
    map_set(c, SK("k"), EI(30), stmp(3, 1));
    map_merge(a, b);
    map_merge(a, c);

    Map *a2 = map_create(arena_create());
    Map *b2 = map_create(arena_create());
    Map *c2 = map_create(arena_create());
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
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_set(a, SK("k"), EI(99), stmp(10, 1));
    map_set(b, SK("k"), EI(7), stmp(1, 1));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(b, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(7));
}

TEST(merge_copies_string_into_dst_arena) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());

    uint8_t src_bytes[8];
    memcpy(src_bytes, "hello", 5);

    map_set(a, SK("k"), EI(0), stmp(1, 1));
    map_set(b, SK("k"), ES(src_bytes, 5), stmp(5, 1));

    map_merge(a, b);

    src_bytes[0] = 'X';
    src_bytes[1] = 'X';

    Element out;
    ASSERT(map_get(a, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_string((const uint8_t *)"hello", 5));
}

TEST(merge_preserves_tombstone_against_older_set) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_delete(a, SK("k"), stmp(5, 1));
    map_set(b, SK("k"), EI(10), stmp(2, 1));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == false);
}

// --- recursive merge: same kind at same key recurses regardless of stamp ---
//
// Position is identity. Two replicas with a composite of the same kind at
// the same key are by definition the same logical object — recurse into
// element_merge. Slot stamp advances to max(dst, src).

TEST(merge_same_kind_counter_recurses) {
    Arena *ad = arena_create();
    Arena *as = arena_create();

    Map *dst = map_create(ad);
    Counter *dc = counter_create(ad);
    counter_inc(dc, cid(1), 5);
    map_set(dst, SK("votes"), element_counter(dc), stmp(1, 1));

    Map *src = map_create(as);
    Counter *sc = counter_create(as);
    counter_inc(sc, cid(2), 3);
    map_set(src, SK("votes"), element_counter(sc), stmp(10, 1));

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT(out.as.counter == dc); // dst kept its own pointer
    ASSERT_EQ(counter_read(out.as.counter), 8);
    arena_destroy(ad);
    arena_destroy(as);
}

TEST(merge_same_kind_register_recurses) {
    Arena *ad = arena_create();
    Arena *as = arena_create();

    Map *dst = map_create(ad);
    Register *dr = register_create(ad, scalar_int(10), stmp(1, 1));
    map_set(dst, SK("title"), element_register(dr), stmp(1, 1));

    Map *src = map_create(as);
    Register *sr = register_create(as, scalar_int(20), stmp(5, 1));
    map_set(src, SK("title"), element_register(sr), stmp(1, 1));

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("title"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    ASSERT(out.as.reg == dr);
    ASSERT(scalar_eq(register_read(dr), scalar_int(20)));
    arena_destroy(ad);
    arena_destroy(as);
}

TEST(merge_same_kind_nested_map_recurses) {
    Arena *ad = arena_create();
    Arena *as = arena_create();

    Map *dst = map_create(ad);
    Map *di = map_create(ad);
    map_set(di, SK("a"), EI(1), stmp(1, 1));
    map_set(dst, SK("child"), element_map(di), stmp(1, 1));

    Map *src = map_create(as);
    Map *si = map_create(as);
    map_set(si, SK("b"), EI(2), stmp(1, 2));
    map_set(src, SK("child"), element_map(si), stmp(1, 1));

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("child"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_MAP);
    ASSERT(out.as.map == di);
    Element a_out, b_out;
    ASSERT(map_get(di, SK("a"), &a_out) == true);
    ASSERT(map_get(di, SK("b"), &b_out) == true);
    ASSERT_SCALAR_EQ(a_out, scalar_int(1));
    ASSERT_SCALAR_EQ(b_out, scalar_int(2));
    arena_destroy(ad);
    arena_destroy(as);
}

TEST(merge_same_kind_counter_does_not_mutate_src) {
    Arena *ad = arena_create();
    Arena *as = arena_create();

    Map *dst = map_create(ad);
    Counter *dc = counter_create(ad);
    counter_inc(dc, cid(1), 5);
    map_set(dst, SK("votes"), element_counter(dc), stmp(1, 1));

    Map *src = map_create(as);
    Counter *sc = counter_create(as);
    counter_inc(sc, cid(2), 3);
    map_set(src, SK("votes"), element_counter(sc), stmp(1, 1));

    map_merge(dst, src);

    ASSERT_EQ(counter_read(sc), 3);
    arena_destroy(ad);
    arena_destroy(as);
}

// Recursive merge must advance the slot stamp to max(dst, src). Otherwise
// future slot-level ops on this key can diverge between replicas. Probe:
// a subsequent set with a stamp above dst's old slot stamp but below src's
// must be rejected.
TEST(merge_same_kind_counter_advances_slot_stamp) {
    Arena *ad = arena_create();
    Arena *as = arena_create();

    Map *dst = map_create(ad);
    Counter *dc = counter_create(ad);
    counter_inc(dc, cid(1), 5);
    map_set(dst, SK("votes"), element_counter(dc), stmp(1, 1));

    Map *src = map_create(as);
    Counter *sc = counter_create(as);
    counter_inc(sc, cid(2), 3);
    map_set(src, SK("votes"), element_counter(sc), stmp(10, 1));

    map_merge(dst, src);

    // A set at stamp(5,1) is below src's slot stamp(10,1) but above dst's
    // old slot stamp(1,1). If dst's stamp wasn't advanced to 10, this set
    // would replace the Counter — and replicas diverge.
    map_set(dst, SK("votes"), EI(99), stmp(5, 1));

    Element out;
    ASSERT(map_get(dst, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT_EQ(counter_read(out.as.counter), 8);
    arena_destroy(ad);
    arena_destroy(as);
}

// --- type-flip via LWW ---
//
// Composites at a key can flip kind. The newer-stamped write wins, the
// old object is orphaned (still alive in the arena but unreachable from
// the slot).

TEST(set_composite_displaces_scalar_at_lww) {
    Arena *ar = arena_create();
    Map *m = map_create(ar);

    map_set(m, SK("score"), EI(42), stmp(1, 1)); // scalar first
    Counter *c = counter_create(ar);
    map_set(m, SK("score"), element_counter(c), stmp(5, 1)); // newer composite

    Element out;
    ASSERT(map_get(m, SK("score"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT(out.as.counter == c);
    arena_destroy(ar);
}

TEST(set_scalar_displaces_composite_at_lww) {
    Arena *ar = arena_create();
    Map *m = map_create(ar);

    Counter *c = counter_create(ar);
    map_set(m, SK("score"), element_counter(c), stmp(1, 1));
    map_set(m, SK("score"), EI(42), stmp(5, 1));

    Element out;
    ASSERT(map_get(m, SK("score"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(42));
    arena_destroy(ar);
}

TEST(set_different_kind_composite_displaces_at_lww) {
    Arena *ar = arena_create();
    Map *m = map_create(ar);

    Counter *c = counter_create(ar);
    map_set(m, SK("score"), element_counter(c), stmp(1, 1));
    Register *r = register_create(ar, scalar_int(42), stmp(5, 1));
    map_set(m, SK("score"), element_register(r), stmp(5, 1));

    Element out;
    ASSERT(map_get(m, SK("score"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    ASSERT(out.as.reg == r);
    arena_destroy(ar);
}

// --- cross-arena composite LWW: clone winner into dst's arena ---

TEST(merge_composite_src_wins_into_empty_slot_clones) {
    Arena *ad = arena_create();
    Arena *as = arena_create();
    Map *dst = map_create(ad);
    Map *src = map_create(as);
    Counter *sc = counter_create(as);
    counter_inc(sc, cid(1), 5);
    map_set(src, SK("votes"), element_counter(sc), stmp(5, 1));

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT(out.as.counter != sc); // dst owns a clone

    arena_destroy(as); // src dies; dst clone must survive
    Element out2;
    ASSERT(map_get(dst, SK("votes"), &out2) == true);
    ASSERT_EQ(counter_read(out2.as.counter), 5);
    arena_destroy(ad);
}

TEST(merge_kind_mismatch_clones_winner_into_dst) {
    Arena *ad = arena_create();
    Arena *as = arena_create();

    Map *dst = map_create(ad);
    Counter *dc = counter_create(ad);
    counter_inc(dc, cid(1), 5);
    map_set(dst, SK("x"), element_counter(dc), stmp(1, 1));

    Map *src = map_create(as);
    Register *sr = register_create(as, scalar_int(42), stmp(10, 1));
    map_set(src, SK("x"), element_register(sr), stmp(10, 1));

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("x"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    ASSERT(out.as.reg != sr); // clone, not src's pointer
    ASSERT(scalar_eq(register_read(out.as.reg), scalar_int(42)));
    arena_destroy(ad);
    arena_destroy(as);
}

// --- get-or-create helpers ---
//
// map_counter / map_register / map_map: install a composite at the given
// key if the slot is empty or has a different kind (and the stamp wins
// LWW). If the slot already has a matching kind, return the existing
// pointer (stamp + value seed ignored). If the stamp loses LWW, return
// NULL.

TEST(map_counter_creates_and_installs_at_key) {
    Arena *ar = arena_create();
    Map *m = map_create(ar);

    Counter *c = map_counter(m, SK("votes"), stmp(1, 1));
    ASSERT(c != NULL);

    Element out;
    ASSERT(map_get(m, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT(out.as.counter == c);
    arena_destroy(ar);
}

TEST(map_counter_returns_same_pointer_on_repeat) {
    Arena *ar = arena_create();
    Map *m = map_create(ar);

    Counter *first = map_counter(m, SK("votes"), stmp(1, 1));
    Counter *second = map_counter(m, SK("votes"), stmp(2, 1));
    ASSERT(first == second);
    arena_destroy(ar);
}

TEST(map_register_creates_and_installs_at_key) {
    Arena *ar = arena_create();
    Map *m = map_create(ar);

    Register *r = map_register(m, SK("title"), scalar_int(42), stmp(1, 1));
    ASSERT(r != NULL);
    ASSERT(scalar_eq(register_read(r), scalar_int(42)));

    Element out;
    ASSERT(map_get(m, SK("title"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    ASSERT(out.as.reg == r);
    arena_destroy(ar);
}

TEST(map_register_returns_same_pointer_on_repeat) {
    Arena *ar = arena_create();
    Map *m = map_create(ar);

    Register *first = map_register(m, SK("title"), scalar_int(1), stmp(1, 1));
    // Second call's seed value is ignored — slot already exists.
    Register *second =
        map_register(m, SK("title"), scalar_int(999), stmp(2, 1));
    ASSERT(first == second);
    ASSERT(scalar_eq(register_read(first), scalar_int(1)));
    arena_destroy(ar);
}

TEST(map_map_creates_and_installs_at_key) {
    Arena *ar = arena_create();
    Map *outer = map_create(ar);

    Map *child = map_map(outer, SK("child"), stmp(1, 1));
    ASSERT(child != NULL);

    Element out;
    ASSERT(map_get(outer, SK("child"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_MAP);
    ASSERT(out.as.map == child);
    arena_destroy(ar);
}

TEST(map_map_returns_same_pointer_on_repeat) {
    Arena *ar = arena_create();
    Map *outer = map_create(ar);

    Map *first = map_map(outer, SK("child"), stmp(1, 1));
    Map *second = map_map(outer, SK("child"), stmp(2, 1));
    ASSERT(first == second);
    arena_destroy(ar);
}

// Helper called over a different-kind slot with a winning stamp must flip
// the kind via LWW and return a fresh composite.
TEST(map_register_after_map_counter_flips_kind_via_lww) {
    Arena *ar = arena_create();
    Map *m = map_create(ar);

    Counter *c = map_counter(m, SK("score"), stmp(1, 1));
    ASSERT(c != NULL);

    Register *r = map_register(m, SK("score"), scalar_int(42), stmp(5, 1));
    ASSERT(r != NULL);

    Element out;
    ASSERT(map_get(m, SK("score"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    ASSERT(out.as.reg == r);

    // The displaced Counter is still alive for direct use, just unreachable
    // from the slot.
    ASSERT_EQ(counter_read(c), 0);
    arena_destroy(ar);
}

// Helper called with a stamp that LOSES LWW returns a DETACHED composite —
// the caller always gets a usable handle, but the slot keeps its existing
// content. Detached composite lives in the arena and supports direct use,
// just isn't reachable from the slot.
TEST(map_helper_losing_stamp_returns_detached_and_keeps_slot) {
    Arena *ar = arena_create();
    Map *m = map_create(ar);

    Counter *c = map_counter(m, SK("score"), stmp(10, 1));
    ASSERT(c != NULL);

    Register *r = map_register(m, SK("score"), scalar_int(7), stmp(5, 1));
    ASSERT(r != NULL); // detached, but still returned
    ASSERT(scalar_eq(register_read(r), scalar_int(7)));

    // Slot kept its Counter — detached Register did not displace.
    Element out;
    ASSERT(map_get(m, SK("score"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT(out.as.counter == c);
    arena_destroy(ar);
}

// Cross-replica: two replicas each call map_counter on the same key. They
// get separate Counter pointers (own arenas), but merge takes the
// recursive path because (key, kind) matches.
TEST(map_counter_cross_replica_merge_recurses) {
    Arena *ad = arena_create();
    Arena *as = arena_create();
    Map *dst = map_create(ad);
    Map *src = map_create(as);

    Counter *dc = map_counter(dst, SK("votes"), stmp(1, 1));
    Counter *sc = map_counter(src, SK("votes"), stmp(1, 2));
    counter_inc(dc, cid(1), 5);
    counter_inc(sc, cid(2), 3);

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT_EQ(counter_read(out.as.counter), 8);
    arena_destroy(ad);
    arena_destroy(as);
}

// --- map_clone: deep recursive copy into a target arena ---

TEST(clone_empty_map_is_empty) {
    Arena *as = arena_create();
    Arena *ad = arena_create();
    Map *src = map_create(as);
    Map *clone = map_clone(ad, src);
    ASSERT(clone != NULL);
    ASSERT(clone != src);
    ASSERT_EQ(map_size(clone), 0);
    arena_destroy(as);
    arena_destroy(ad);
}

TEST(clone_preserves_scalar_slots) {
    Arena *as = arena_create();
    Arena *ad = arena_create();
    Map *src = map_create(as);
    map_set(src, SK("a"), EI(1), stmp(1, 1));
    map_set(src, SK("b"), ES("hi", 2), stmp(1, 1));
    Map *clone = map_clone(ad, src);
    ASSERT_EQ(map_size(clone), 2);
    Element a_out, b_out;
    ASSERT(map_get(clone, SK("a"), &a_out) == true);
    ASSERT(map_get(clone, SK("b"), &b_out) == true);
    ASSERT_SCALAR_EQ(a_out, scalar_int(1));
    ASSERT_SCALAR_EQ(b_out, scalar_string((const uint8_t *)"hi", 2));
    arena_destroy(as);
    arena_destroy(ad);
}

// Clone owns all its data — destroying the source arena leaves the clone
// fully usable.
TEST(clone_survives_src_arena_destroy) {
    Arena *as = arena_create();
    Arena *ad = arena_create();
    Map *src = map_create(as);
    map_set(src, SK("k"), ES("hello", 5), stmp(1, 1));
    Map *clone = map_clone(ad, src);
    arena_destroy(as);
    Element out;
    ASSERT(map_get(clone, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_string((const uint8_t *)"hello", 5));
    arena_destroy(ad);
}

// Composite slots are recursively cloned — the clone's nested composites
// are independent objects in dst's arena.
TEST(clone_recurses_into_composite_slots) {
    Arena *as = arena_create();
    Arena *ad = arena_create();
    Map *src = map_create(as);
    Counter *sc = counter_create(as);
    counter_inc(sc, cid(1), 5);
    map_set(src, SK("votes"), element_counter(sc), stmp(1, 1));

    Map *clone = map_clone(ad, src);

    Element out;
    ASSERT(map_get(clone, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT(out.as.counter != sc); // recursive clone, independent object
    ASSERT_EQ(counter_read(out.as.counter), 5);

    arena_destroy(as);
    Element out2;
    ASSERT(map_get(clone, SK("votes"), &out2) == true);
    ASSERT_EQ(counter_read(out2.as.counter), 5);
    arena_destroy(ad);
}

// Tombstones must round-trip through clone so deletion semantics survive.
TEST(clone_preserves_tombstones) {
    Arena *as = arena_create();
    Arena *ad = arena_create();
    Map *src = map_create(as);
    map_set(src, SK("k"), EI(1), stmp(1, 1));
    map_delete(src, SK("k"), stmp(5, 1));

    Map *clone = map_clone(ad, src);

    // Tombstone present at stamp(5,1) — older set must lose LWW.
    map_set(clone, SK("k"), EI(99), stmp(3, 1));
    Element out;
    ASSERT(map_get(clone, SK("k"), &out) == false);
    arena_destroy(as);
    arena_destroy(ad);
}

// Mutating src after clone must not affect the clone.
TEST(clone_independent_of_src) {
    Arena *as = arena_create();
    Arena *ad = arena_create();
    Map *src = map_create(as);
    map_set(src, SK("k"), EI(1), stmp(1, 1));
    Map *clone = map_clone(ad, src);
    map_set(src, SK("k"), EI(99), stmp(5, 1));
    map_set(src, SK("new"), EI(7), stmp(1, 1));

    Element out;
    ASSERT(map_get(clone, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(1));
    ASSERT(map_get(clone, SK("new"), &out) == false);
    arena_destroy(as);
    arena_destroy(ad);
}

// Tombstone entries carry a stale (or uninit) value field. map_clone must
// NOT recursively clone that stale value into the destination arena —
// doing so wastes memory and reads possibly-undefined bytes.
//
// Probe: build a sizeable subtree under a key, delete the slot (the Entry
// keeps the composite pointer in its `value` field), clone the Map, and
// check that the clone's arena did not absorb the full subtree.
TEST(clone_tombstone_does_not_recurse_into_stale_value) {
    Arena *as = arena_create();
    Arena *ad = arena_create();
    Map *src = map_create(as);

    // Big inner map under the to-be-tombstoned key.
    Map *inner = map_create(as);
    for (int i = 0; i < 50; i++) {
        char k[16];
        int n = snprintf(k, sizeof k, "k%d", i);
        map_set(inner, k, (size_t)n, EI(i), stmp(1, 1));
    }
    map_set(src, SK("child"), element_map(inner), stmp(1, 1));
    // Delete — Entry stays in src's hashtable with is_tombstone=true but
    // the `value` field still points at `inner`.
    map_delete(src, SK("child"), stmp(5, 1));

    size_t before = arena_used(ad);
    Map *clone = map_clone(ad, src);
    size_t after = arena_used(ad);

    // Tombstone semantics survive.
    Element out;
    ASSERT(map_get(clone, SK("child"), &out) == false);

    // Bug surfaces as massive over-allocation in dst: the bogus
    // element_clone on the tombstone recursively clones the 50-entry
    // inner Map into ad. The honest clone only allocates the outer Map,
    // its hashtable, and one tombstone Entry — well under 1 KB.
    size_t cost = after - before;
    ASSERT(cost < 1024);

    arena_destroy(as);
    arena_destroy(ad);
}

int main(void) {
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

    RUN(merge_same_kind_counter_recurses);
    RUN(merge_same_kind_register_recurses);
    RUN(merge_same_kind_nested_map_recurses);
    RUN(merge_same_kind_counter_does_not_mutate_src);
    RUN(merge_same_kind_counter_advances_slot_stamp);

    RUN(set_composite_displaces_scalar_at_lww);
    RUN(set_scalar_displaces_composite_at_lww);
    RUN(set_different_kind_composite_displaces_at_lww);

    RUN(merge_composite_src_wins_into_empty_slot_clones);
    RUN(merge_kind_mismatch_clones_winner_into_dst);

    RUN(map_counter_creates_and_installs_at_key);
    RUN(map_counter_returns_same_pointer_on_repeat);
    RUN(map_register_creates_and_installs_at_key);
    RUN(map_register_returns_same_pointer_on_repeat);
    RUN(map_map_creates_and_installs_at_key);
    RUN(map_map_returns_same_pointer_on_repeat);
    RUN(map_register_after_map_counter_flips_kind_via_lww);
    RUN(map_helper_losing_stamp_returns_detached_and_keeps_slot);
    RUN(map_counter_cross_replica_merge_recurses);

    RUN(clone_empty_map_is_empty);
    RUN(clone_preserves_scalar_slots);
    RUN(clone_survives_src_arena_destroy);
    RUN(clone_recurses_into_composite_slots);
    RUN(clone_preserves_tombstones);
    RUN(clone_independent_of_src);
    RUN(clone_tombstone_does_not_recurse_into_stale_value);

    TEST_SUMMARY();
}
