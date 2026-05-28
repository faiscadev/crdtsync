// Native unit tests for the PN-counter CRDT. Compile with system clang,
// link arena.c + string.c + hashtable.c + counter.c. Run the binary.
//
// Expected API (you implement in counter.h + counter.c):
//
//   Counter *counter_create(Arena *arena);
//   void     counter_inc(Counter *c, uint32_t client_id, uint32_t amount);
//   void     counter_dec(Counter *c, uint32_t client_id, uint32_t amount);
//   int64_t  counter_read(const Counter *c);
//   void     counter_merge(Counter *dst, const Counter *src);
//
// Model — state-based PN-counter, backed by hashtable:
//   client_id (uint32_t, keyed as raw bytes) -> CounterEntry { uint32_t inc; uint32_t dec; }
//
//   - read  = sum over all clients of (inc - dec). Signed (can go negative).
//   - LOCAL inc/dec ACCUMULATE into the caller's own client entry
//       (entry.inc += amount  /  entry.dec += amount).
//   - MERGE takes per-direction MAX per client
//       (dst.inc = max(dst.inc, src.inc); dst.dec = max(dst.dec, src.dec)).
//     NOT a sum — both replicas may have counted the same ops; max reconciles.
//
// counter_merge is one-directional: src's state is folded into dst; src is
// left unchanged.

#include "test_util.h"
#include "arena.h"
#include "counter.h"

#define ARENA_BYTES (64 * 1024)

static Counter *fresh(uint8_t *buf, size_t len) {
    Arena *arena = arena_create(buf, len);
    return counter_create(arena);
}

// --- local operations (single replica) ---

TEST(empty_reads_zero) {
    uint8_t buf[ARENA_BYTES];
    Counter *c = fresh(buf, sizeof(buf));
    ASSERT_EQ(counter_read(c), 0);
}

TEST(single_inc) {
    uint8_t buf[ARENA_BYTES];
    Counter *c = fresh(buf, sizeof(buf));
    counter_inc(c, 1, 5);
    ASSERT_EQ(counter_read(c), 5);
}

TEST(inc_then_dec_nets) {
    uint8_t buf[ARENA_BYTES];
    Counter *c = fresh(buf, sizeof(buf));
    counter_inc(c, 1, 5);
    counter_dec(c, 1, 2);
    ASSERT_EQ(counter_read(c), 3);
}

// Local repeated ops on the same client accumulate (this is NOT max).
TEST(local_inc_accumulates) {
    uint8_t buf[ARENA_BYTES];
    Counter *c = fresh(buf, sizeof(buf));
    counter_inc(c, 1, 5);
    counter_inc(c, 1, 2);
    ASSERT_EQ(counter_read(c), 7);
}

TEST(read_can_go_negative) {
    uint8_t buf[ARENA_BYTES];
    Counter *c = fresh(buf, sizeof(buf));
    counter_dec(c, 1, 3);
    ASSERT_EQ(counter_read(c), -3);
}

TEST(two_clients_sum_in_one_replica) {
    uint8_t buf[ARENA_BYTES];
    Counter *c = fresh(buf, sizeof(buf));
    counter_inc(c, 1, 5);
    counter_inc(c, 2, 3);
    counter_dec(c, 2, 1);
    ASSERT_EQ(counter_read(c), 7); // (5-0) + (3-1)
}

// --- merge (two replicas) ---

TEST(merge_disjoint_clients_unions) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));

    counter_inc(a, 1, 5);
    counter_inc(b, 2, 3);

    counter_merge(a, b);
    ASSERT_EQ(counter_read(a), 8);
}

// Classic CRDT result: concurrent increments on different clients converge,
// both replicas read the same value after exchanging state.
TEST(concurrent_inc_converges) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));

    counter_inc(a, 1, 5);
    counter_inc(b, 2, 3);

    counter_merge(a, b);
    counter_merge(b, a);

    ASSERT_EQ(counter_read(a), 8);
    ASSERT_EQ(counter_read(b), 8);
}

// Same client seen with different counts on two replicas: merge takes the MAX,
// not the sum (the lower replica was simply behind).
TEST(merge_same_client_takes_max_not_sum) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));

    counter_inc(a, 1, 5);
    counter_inc(b, 1, 3);

    counter_merge(a, b);
    ASSERT_EQ(counter_read(a), 5); // max(5,3), NOT 8
}

TEST(merge_same_client_max_on_both_directions) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));

    // a: inc 10, dec 2  -> {inc:10, dec:2}
    counter_inc(a, 1, 10);
    counter_dec(a, 1, 2);
    // b: inc 4, dec 6   -> {inc:4, dec:6}
    counter_inc(b, 1, 4);
    counter_dec(b, 1, 6);

    counter_merge(a, b);
    // max(inc)=10, max(dec)=6 -> 10 - 6 = 4
    ASSERT_EQ(counter_read(a), 4);
}

TEST(merge_idempotent) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));

    counter_inc(a, 1, 5);
    counter_inc(b, 2, 3);

    counter_merge(a, b);
    int64_t once = counter_read(a);
    counter_merge(a, b);
    int64_t twice = counter_read(a);

    ASSERT_EQ(once, twice);
    ASSERT_EQ(twice, 8);
}

TEST(merge_commutative) {
    // (a <- b)
    uint8_t bufA1[ARENA_BYTES], bufB1[ARENA_BYTES];
    Counter *a1 = fresh(bufA1, sizeof(bufA1));
    Counter *b1 = fresh(bufB1, sizeof(bufB1));
    counter_inc(a1, 1, 5);
    counter_dec(a1, 1, 1);
    counter_inc(b1, 2, 3);
    counter_merge(a1, b1);

    // (b <- a)
    uint8_t bufA2[ARENA_BYTES], bufB2[ARENA_BYTES];
    Counter *a2 = fresh(bufA2, sizeof(bufA2));
    Counter *b2 = fresh(bufB2, sizeof(bufB2));
    counter_inc(a2, 1, 5);
    counter_dec(a2, 1, 1);
    counter_inc(b2, 2, 3);
    counter_merge(b2, a2);

    ASSERT_EQ(counter_read(a1), counter_read(b2));
}

TEST(merge_associative) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES], bufC[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));
    Counter *c = fresh(bufC, sizeof(bufC));
    counter_inc(a, 1, 5);
    counter_inc(b, 2, 3);
    counter_inc(c, 3, 2);

    // (a <- b) <- c
    counter_merge(a, b);
    counter_merge(a, c);

    // a <- (b <- c)  (rebuild on a fresh accumulator)
    uint8_t bufA2[ARENA_BYTES], bufB2[ARENA_BYTES], bufC2[ARENA_BYTES];
    Counter *a2 = fresh(bufA2, sizeof(bufA2));
    Counter *b2 = fresh(bufB2, sizeof(bufB2));
    Counter *c2 = fresh(bufC2, sizeof(bufC2));
    counter_inc(a2, 1, 5);
    counter_inc(b2, 2, 3);
    counter_inc(c2, 3, 2);
    counter_merge(b2, c2);
    counter_merge(a2, b2);

    ASSERT_EQ(counter_read(a), counter_read(a2));
    ASSERT_EQ(counter_read(a), 10);
}

// Merge leaves the source untouched.
TEST(merge_does_not_mutate_src) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));

    counter_inc(a, 1, 5);
    counter_inc(b, 2, 3);

    counter_merge(a, b);
    ASSERT_EQ(counter_read(b), 3); // b unchanged
}

// After merge, a subsequent local inc on a newly-learned client accumulates
// from the merged-in value (not from zero).
TEST(local_inc_after_merge_accumulates) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));

    counter_inc(b, 2, 3);
    counter_merge(a, b); // a learns c2 = 3

    counter_inc(a, 2, 4); // a is now also acting as c2? accumulate to 7
    ASSERT_EQ(counter_read(a), 7);
}

int main(void) {
    RUN(empty_reads_zero);
    RUN(single_inc);
    RUN(inc_then_dec_nets);
    RUN(local_inc_accumulates);
    RUN(read_can_go_negative);
    RUN(two_clients_sum_in_one_replica);
    RUN(merge_disjoint_clients_unions);
    RUN(concurrent_inc_converges);
    RUN(merge_same_client_takes_max_not_sum);
    RUN(merge_same_client_max_on_both_directions);
    RUN(merge_idempotent);
    RUN(merge_commutative);
    RUN(merge_associative);
    RUN(merge_does_not_mutate_src);
    RUN(local_inc_after_merge_accumulates);
    TEST_SUMMARY();
}
