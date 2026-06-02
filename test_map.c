#include "arena.h"
#include "clientid.h"
#include "map.h"
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

static Stamp stmp(uint64_t lamport, uint8_t client_first_byte) {
    return (Stamp){.lamport = lamport, .client_id = cid(client_first_byte)};
}

// String-key shorthand: expands to (bytes, length) without the NUL terminator.
#define SK(s) ((const void *)(s)), strlen(s)

static Map *fresh(void) {
    Arena *arena = arena_create();
    return map_create(arena);
}

// --- local set / get ---

TEST(empty_get_returns_false) {
    Map *m = fresh();
    Scalar out;
    ASSERT(map_get(m, SK("missing"), &out) == false);
}

TEST(set_then_get) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(42), stmp(1, 1));
    Scalar out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT(scalar_eq(out, scalar_int(42)));
}

TEST(set_overwrites_with_newer_stamp) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(10), stmp(1, 1));
    map_set(m, SK("k"), scalar_int(20), stmp(2, 1));
    Scalar out;
    map_get(m, SK("k"), &out);
    ASSERT(scalar_eq(out, scalar_int(20)));
}

TEST(set_lower_stamp_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(20), stmp(5, 1));
    map_set(m, SK("k"), scalar_int(10), stmp(3, 1)); // older — ignored
    Scalar out;
    map_get(m, SK("k"), &out);
    ASSERT(scalar_eq(out, scalar_int(20)));
}

TEST(set_equal_lamport_higher_client_wins) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(10), stmp(5, 1));
    map_set(m, SK("k"), scalar_int(20), stmp(5, 2)); // same lamport, > client
    Scalar out;
    map_get(m, SK("k"), &out);
    ASSERT(scalar_eq(out, scalar_int(20)));
}

TEST(set_equal_lamport_lower_client_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(20), stmp(5, 2));
    map_set(m, SK("k"), scalar_int(10), stmp(5, 1));
    Scalar out;
    map_get(m, SK("k"), &out);
    ASSERT(scalar_eq(out, scalar_int(20)));
}

TEST(set_same_stamp_idempotent) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(42), stmp(5, 1));
    map_set(m, SK("k"), scalar_int(42), stmp(5, 1));
    Scalar out;
    map_get(m, SK("k"), &out);
    ASSERT(scalar_eq(out, scalar_int(42)));
}

// A newer write can change the Scalar kind.
TEST(set_can_change_value_kind) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(42), stmp(1, 1));
    map_set(m, SK("k"), scalar_string((const uint8_t *)"hi", 2), stmp(2, 1));
    Scalar out;
    map_get(m, SK("k"), &out);
    ASSERT(scalar_eq(out, scalar_string((const uint8_t *)"hi", 2)));
}

// Distinct keys are independent — writing one must not affect the other.
TEST(distinct_keys_are_independent) {
    Map *m = fresh();
    map_set(m, SK("a"), scalar_int(1), stmp(1, 1));
    map_set(m, SK("b"), scalar_int(2), stmp(1, 1));
    Scalar a, b;
    map_get(m, SK("a"), &a);
    map_get(m, SK("b"), &b);
    ASSERT(scalar_eq(a, scalar_int(1)));
    ASSERT(scalar_eq(b, scalar_int(2)));
}

// Headline reason for byte keys: keys with embedded NUL bytes must be
// distinguished past the NUL.
TEST(keys_with_embedded_nul_are_distinct) {
    Map *m = fresh();
    uint8_t k1[3] = {0x01, 0x00, 0x02};
    uint8_t k2[3] = {0x01, 0x00, 0x03};
    map_set(m, k1, sizeof k1, scalar_int(1), stmp(1, 1));
    map_set(m, k2, sizeof k2, scalar_int(2), stmp(1, 1));
    Scalar v1, v2;
    ASSERT(map_get(m, k1, sizeof k1, &v1) == true);
    ASSERT(map_get(m, k2, sizeof k2, &v2) == true);
    ASSERT(scalar_eq(v1, scalar_int(1)));
    ASSERT(scalar_eq(v2, scalar_int(2)));
}

// --- delete / tombstones ---

TEST(delete_makes_get_return_false) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(42), stmp(1, 1));
    map_delete(m, SK("k"), stmp(2, 1));
    Scalar out;
    ASSERT(map_get(m, SK("k"), &out) == false);
}

// A delete with a stamp older than the existing value must NOT clobber.
TEST(delete_with_lower_stamp_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(42), stmp(5, 1));
    map_delete(m, SK("k"), stmp(3, 1)); // older — ignored
    Scalar out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT(scalar_eq(out, scalar_int(42)));
}

// After delete, a set with a higher stamp must resurrect the slot.
TEST(set_after_delete_with_higher_stamp_resurrects) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(2, 1));
    map_set(m, SK("k"), scalar_int(20), stmp(3, 1));
    Scalar out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT(scalar_eq(out, scalar_int(20)));
}

// After delete, a set with a lower-or-equal stamp must NOT resurrect.
TEST(set_after_delete_with_lower_stamp_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(5, 1));
    map_set(m, SK("k"), scalar_int(20), stmp(3, 1)); // older than delete
    Scalar out;
    ASSERT(map_get(m, SK("k"), &out) == false);
}

// Concurrent set vs delete: stamp decides which wins.
TEST(set_vs_delete_higher_stamp_wins_set) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(5, 1));
    Scalar out;
    ASSERT(map_get(m, SK("k"), &out) == false);
}

TEST(delete_idempotent_same_stamp) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(5, 1));
    map_delete(m, SK("k"), stmp(5, 1));
    Scalar out;
    ASSERT(map_get(m, SK("k"), &out) == false);
}

// Deleting a key that was never set is a no-op but installs a tombstone with
// the given stamp — a later set with a lower stamp must still be rejected.
TEST(delete_absent_key_still_installs_tombstone) {
    Map *m = fresh();
    map_delete(m, SK("ghost"), stmp(10, 1));
    map_set(m, SK("ghost"), scalar_int(1), stmp(5, 1)); // older than delete
    Scalar out;
    ASSERT(map_get(m, SK("ghost"), &out) == false);
}

// --- map_size ---

TEST(size_zero_initially) {
    Map *m = fresh();
    ASSERT_EQ(map_size(m), 0);
}

TEST(size_counts_live_entries) {
    Map *m = fresh();
    map_set(m, SK("a"), scalar_int(1), stmp(1, 1));
    map_set(m, SK("b"), scalar_int(2), stmp(1, 1));
    map_set(m, SK("c"), scalar_int(3), stmp(1, 1));
    ASSERT_EQ(map_size(m), 3);
}

TEST(size_excludes_tombstones) {
    Map *m = fresh();
    map_set(m, SK("a"), scalar_int(1), stmp(1, 1));
    map_set(m, SK("b"), scalar_int(2), stmp(1, 1));
    map_delete(m, SK("b"), stmp(2, 1));
    ASSERT_EQ(map_size(m), 1);
}

TEST(size_recovers_on_resurrect) {
    Map *m = fresh();
    map_set(m, SK("k"), scalar_int(1), stmp(1, 1));
    map_delete(m, SK("k"), stmp(2, 1));
    ASSERT_EQ(map_size(m), 0);
    map_set(m, SK("k"), scalar_int(2), stmp(3, 1));
    ASSERT_EQ(map_size(m), 1);
}

// --- merge (two replicas) ---

TEST(merge_disjoint_keys_unions) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_set(a, SK("x"), scalar_int(1), stmp(1, 1));
    map_set(b, SK("y"), scalar_int(2), stmp(1, 2));

    map_merge(a, b);
    Scalar x, y;
    ASSERT(map_get(a, SK("x"), &x) == true);
    ASSERT(map_get(a, SK("y"), &y) == true);
    ASSERT(scalar_eq(x, scalar_int(1)));
    ASSERT(scalar_eq(y, scalar_int(2)));
    ASSERT_EQ(map_size(a), 2);
}

TEST(merge_same_key_newer_wins) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_set(a, SK("k"), scalar_int(10), stmp(1, 1));
    map_set(b, SK("k"), scalar_int(20), stmp(2, 2)); // newer

    map_merge(a, b);
    Scalar out;
    map_get(a, SK("k"), &out);
    ASSERT(scalar_eq(out, scalar_int(20)));
}

TEST(merge_src_older_loses) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_set(a, SK("k"), scalar_int(20), stmp(5, 1)); // newer
    map_set(b, SK("k"), scalar_int(10), stmp(2, 2));

    map_merge(a, b);
    Scalar out;
    map_get(a, SK("k"), &out);
    ASSERT(scalar_eq(out, scalar_int(20)));
}

// Concurrent: dst has a value, src has a delete with a higher stamp.
TEST(merge_delete_beats_older_set) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_set(a, SK("k"), scalar_int(10), stmp(1, 1));
    map_delete(b, SK("k"), stmp(5, 1)); // newer

    map_merge(a, b);
    Scalar out;
    ASSERT(map_get(a, SK("k"), &out) == false);
}

// Concurrent: dst has a delete, src has a value with a higher stamp.
TEST(merge_set_beats_older_delete) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_delete(a, SK("k"), stmp(1, 1));
    map_set(b, SK("k"), scalar_int(42), stmp(5, 1)); // newer

    map_merge(a, b);
    Scalar out;
    ASSERT(map_get(a, SK("k"), &out) == true);
    ASSERT(scalar_eq(out, scalar_int(42)));
}

TEST(merge_commutative) {
    // path 1: a <- b
    Map *a1 = map_create(arena_create());
    Map *b1 = map_create(arena_create());
    map_set(a1, SK("k"), scalar_int(10), stmp(5, 1));
    map_set(b1, SK("k"), scalar_int(20), stmp(5, 2));
    map_merge(a1, b1);

    // path 2: b <- a
    Map *a2 = map_create(arena_create());
    Map *b2 = map_create(arena_create());
    map_set(a2, SK("k"), scalar_int(10), stmp(5, 1));
    map_set(b2, SK("k"), scalar_int(20), stmp(5, 2));
    map_merge(b2, a2);

    Scalar v1, v2;
    map_get(a1, SK("k"), &v1);
    map_get(b2, SK("k"), &v2);
    ASSERT(scalar_eq(v1, v2));
    ASSERT(scalar_eq(v1, scalar_int(20)));
}

TEST(merge_idempotent) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_set(a, SK("k"), scalar_int(10), stmp(1, 1));
    map_set(b, SK("k"), scalar_int(20), stmp(2, 1));

    map_merge(a, b);
    Scalar once;
    map_get(a, SK("k"), &once);
    map_merge(a, b);
    Scalar twice;
    map_get(a, SK("k"), &twice);
    ASSERT(scalar_eq(once, twice));
    ASSERT(scalar_eq(twice, scalar_int(20)));
}

TEST(merge_associative) {
    // (a <- b) <- c
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    Map *c = map_create(arena_create());
    map_set(a, SK("k"), scalar_int(10), stmp(1, 1));
    map_set(b, SK("k"), scalar_int(20), stmp(2, 1));
    map_set(c, SK("k"), scalar_int(30), stmp(3, 1));
    map_merge(a, b);
    map_merge(a, c);

    // a <- (b <- c)
    Map *a2 = map_create(arena_create());
    Map *b2 = map_create(arena_create());
    Map *c2 = map_create(arena_create());
    map_set(a2, SK("k"), scalar_int(10), stmp(1, 1));
    map_set(b2, SK("k"), scalar_int(20), stmp(2, 1));
    map_set(c2, SK("k"), scalar_int(30), stmp(3, 1));
    map_merge(b2, c2);
    map_merge(a2, b2);

    Scalar v1, v2;
    map_get(a, SK("k"), &v1);
    map_get(a2, SK("k"), &v2);
    ASSERT(scalar_eq(v1, v2));
    ASSERT(scalar_eq(v1, scalar_int(30)));
}

TEST(merge_does_not_mutate_src) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_set(a, SK("k"), scalar_int(99), stmp(10, 1)); // newer
    map_set(b, SK("k"), scalar_int(7), stmp(1, 1));

    map_merge(a, b);
    Scalar out;
    map_get(b, SK("k"), &out);
    ASSERT(scalar_eq(out, scalar_int(7))); // b unchanged
}

// When merge accepts a winning string value from src, dst must own its own
// copy in dst's arena. Mutating the source bytes after merge must not affect
// dst's stored value.
TEST(merge_copies_string_into_dst_arena) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());

    uint8_t src_bytes[8];
    memcpy(src_bytes, "hello", 5);

    map_set(a, SK("k"), scalar_int(0), stmp(1, 1));
    map_set(b, SK("k"), scalar_string(src_bytes, 5), stmp(5, 1));

    map_merge(a, b); // a takes b's string

    src_bytes[0] = 'X';
    src_bytes[1] = 'X';

    Scalar out;
    map_get(a, SK("k"), &out);
    ASSERT(scalar_eq(out, scalar_string((const uint8_t *)"hello", 5)));
}

// Tombstones survive merge: dst with a tombstone merged with src that has an
// older value must keep the tombstone (the higher stamp wins).
TEST(merge_preserves_tombstone_against_older_set) {
    Map *a = map_create(arena_create());
    Map *b = map_create(arena_create());
    map_delete(a, SK("k"), stmp(5, 1));
    map_set(b, SK("k"), scalar_int(10), stmp(2, 1));

    map_merge(a, b);
    Scalar out;
    ASSERT(map_get(a, SK("k"), &out) == false);
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
    RUN(set_vs_delete_higher_stamp_wins_set);
    RUN(delete_idempotent_same_stamp);
    RUN(delete_absent_key_still_installs_tombstone);

    RUN(size_zero_initially);
    RUN(size_counts_live_entries);
    RUN(size_excludes_tombstones);
    RUN(size_recovers_on_resurrect);

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

    TEST_SUMMARY();
}
