#include "arena.h"
#include "clientid.h"
#include "counter.h"
#include "test_util.h"

#define ARENA_BYTES (64 * 1024)

static Counter *fresh(uint8_t *buf, size_t len) {
    Arena *arena = arena_create(buf, len);
    return counter_create(arena);
}

// Build a ClientId fixture from a single byte (rest zero). Keeps tests
// compact; ClientId is otherwise 16 raw bytes.
static ClientId cid(uint8_t first_byte) {
    uint8_t b[16] = {0};
    b[0] = first_byte;
    return clientid_from_bytes(b);
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
    counter_inc(c, cid(1), 5);
    ASSERT_EQ(counter_read(c), 5);
}

TEST(inc_then_dec_nets) {
    uint8_t buf[ARENA_BYTES];
    Counter *c = fresh(buf, sizeof(buf));
    counter_inc(c, cid(1), 5);
    counter_dec(c, cid(1), 2);
    ASSERT_EQ(counter_read(c), 3);
}

// Local repeated ops on the same client accumulate (this is NOT max).
TEST(local_inc_accumulates) {
    uint8_t buf[ARENA_BYTES];
    Counter *c = fresh(buf, sizeof(buf));
    counter_inc(c, cid(1), 5);
    counter_inc(c, cid(1), 2);
    ASSERT_EQ(counter_read(c), 7);
}

TEST(read_can_go_negative) {
    uint8_t buf[ARENA_BYTES];
    Counter *c = fresh(buf, sizeof(buf));
    counter_dec(c, cid(1), 3);
    ASSERT_EQ(counter_read(c), -3);
}

TEST(two_clients_sum_in_one_replica) {
    uint8_t buf[ARENA_BYTES];
    Counter *c = fresh(buf, sizeof(buf));
    counter_inc(c, cid(1), 5);
    counter_inc(c, cid(2), 3);
    counter_dec(c, cid(2), 1);
    ASSERT_EQ(counter_read(c), 7); // (5-0) + (3-1)
}

// Two clients are distinguished by the FULL 16-byte ClientId, not the first
// byte (proves the hashtable key uses sizeof(ClientId) and not just a prefix).
TEST(client_ids_distinguished_by_full_bytes) {
    uint8_t buf[ARENA_BYTES];
    Counter *c = fresh(buf, sizeof(buf));

    uint8_t a_bytes[16] = {1, 2,  3,  4,  5,  6,  7,  8,
                           9, 10, 11, 12, 13, 14, 15, 16};
    uint8_t b_bytes[16] = {1, 2,  3,  4,  5,  6,  7,  8,
                           9, 10, 11, 12, 13, 14, 15, 99};
    ClientId a = clientid_from_bytes(a_bytes);
    ClientId b = clientid_from_bytes(b_bytes);

    counter_inc(c, a, 5);
    counter_inc(c, b, 3);
    ASSERT_EQ(counter_read(c), 8); // distinct clients -> two entries
}

// --- merge (two replicas) ---

TEST(merge_disjoint_clients_unions) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));

    counter_inc(a, cid(1), 5);
    counter_inc(b, cid(2), 3);

    counter_merge(a, b);
    ASSERT_EQ(counter_read(a), 8);
}

// Classic CRDT result: concurrent increments on different clients converge,
// both replicas read the same value after exchanging state.
TEST(concurrent_inc_converges) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));

    counter_inc(a, cid(1), 5);
    counter_inc(b, cid(2), 3);

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

    counter_inc(a, cid(1), 5);
    counter_inc(b, cid(1), 3);

    counter_merge(a, b);
    ASSERT_EQ(counter_read(a), 5); // max(5,3), NOT 8
}

TEST(merge_same_client_max_on_both_directions) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));

    // a: inc 10, dec 2  -> {inc:10, dec:2}
    counter_inc(a, cid(1), 10);
    counter_dec(a, cid(1), 2);
    // b: inc 4, dec 6   -> {inc:4, dec:6}
    counter_inc(b, cid(1), 4);
    counter_dec(b, cid(1), 6);

    counter_merge(a, b);
    // max(inc)=10, max(dec)=6 -> 10 - 6 = 4
    ASSERT_EQ(counter_read(a), 4);
}

TEST(merge_idempotent) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));

    counter_inc(a, cid(1), 5);
    counter_inc(b, cid(2), 3);

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
    counter_inc(a1, cid(1), 5);
    counter_dec(a1, cid(1), 1);
    counter_inc(b1, cid(2), 3);
    counter_merge(a1, b1);

    // (b <- a)
    uint8_t bufA2[ARENA_BYTES], bufB2[ARENA_BYTES];
    Counter *a2 = fresh(bufA2, sizeof(bufA2));
    Counter *b2 = fresh(bufB2, sizeof(bufB2));
    counter_inc(a2, cid(1), 5);
    counter_dec(a2, cid(1), 1);
    counter_inc(b2, cid(2), 3);
    counter_merge(b2, a2);

    ASSERT_EQ(counter_read(a1), counter_read(b2));
}

TEST(merge_associative) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES], bufC[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));
    Counter *c = fresh(bufC, sizeof(bufC));
    counter_inc(a, cid(1), 5);
    counter_inc(b, cid(2), 3);
    counter_inc(c, cid(3), 2);

    // (a <- b) <- c
    counter_merge(a, b);
    counter_merge(a, c);

    // a <- (b <- c)  (rebuild on a fresh accumulator)
    uint8_t bufA2[ARENA_BYTES], bufB2[ARENA_BYTES], bufC2[ARENA_BYTES];
    Counter *a2 = fresh(bufA2, sizeof(bufA2));
    Counter *b2 = fresh(bufB2, sizeof(bufB2));
    Counter *c2 = fresh(bufC2, sizeof(bufC2));
    counter_inc(a2, cid(1), 5);
    counter_inc(b2, cid(2), 3);
    counter_inc(c2, cid(3), 2);
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

    counter_inc(a, cid(1), 5);
    counter_inc(b, cid(2), 3);

    counter_merge(a, b);
    ASSERT_EQ(counter_read(b), 3); // b unchanged
}

// After merge, a subsequent local inc on a newly-learned client accumulates
// from the merged-in value (not from zero).
TEST(local_inc_after_merge_accumulates) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Counter *a = fresh(bufA, sizeof(bufA));
    Counter *b = fresh(bufB, sizeof(bufB));

    counter_inc(b, cid(2), 3);
    counter_merge(a, b); // a learns client 2 = 3

    counter_inc(a, cid(2), 4); // a now also acting as client 2: accumulate to 7
    ASSERT_EQ(counter_read(a), 7);
}

int main(void) {
    RUN(empty_reads_zero);
    RUN(single_inc);
    RUN(inc_then_dec_nets);
    RUN(local_inc_accumulates);
    RUN(read_can_go_negative);
    RUN(two_clients_sum_in_one_replica);
    RUN(client_ids_distinguished_by_full_bytes);
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
