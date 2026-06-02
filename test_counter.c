#include "arena.h"
#include "clientid.h"
#include "counter.h"
#include "elementid.h"
#include "test_util.h"

// Build a ClientId fixture from a single byte (rest zero). Keeps tests
// compact; ClientId is otherwise 16 raw bytes.
static ClientId cid(uint8_t first_byte) {
    uint8_t b[16] = {0};
    b[0] = first_byte;
    return clientid_from_bytes(b);
}

static ElementId eid(uint8_t origin_byte, uint64_t seq) {
    return elementid_new(cid(origin_byte), seq);
}

// Default id for tests that don't care about identity.
static ElementId default_id(void) { return eid(0xFF, 0); }

static Counter *fresh(void) {
    Arena *arena = arena_create();
    return counter_create(arena, default_id());
}

TEST(counter_create_stores_id) {
    Arena *a = arena_create();
    ElementId id = eid(7, 42);
    Counter *c = counter_create(a, id);
    ASSERT(elementid_eq(counter_id(c), id) == true);
    arena_destroy(a);
}

// --- local operations (single replica) ---

TEST(empty_reads_zero) {

    Counter *c = fresh();
    ASSERT_EQ(counter_read(c), 0);
}

TEST(single_inc) {

    Counter *c = fresh();
    counter_inc(c, cid(1), 5);
    ASSERT_EQ(counter_read(c), 5);
}

TEST(inc_then_dec_nets) {

    Counter *c = fresh();
    counter_inc(c, cid(1), 5);
    counter_dec(c, cid(1), 2);
    ASSERT_EQ(counter_read(c), 3);
}

// Local repeated ops on the same client accumulate (this is NOT max).
TEST(local_inc_accumulates) {

    Counter *c = fresh();
    counter_inc(c, cid(1), 5);
    counter_inc(c, cid(1), 2);
    ASSERT_EQ(counter_read(c), 7);
}

TEST(read_can_go_negative) {

    Counter *c = fresh();
    counter_dec(c, cid(1), 3);
    ASSERT_EQ(counter_read(c), -3);
}

TEST(two_clients_sum_in_one_replica) {

    Counter *c = fresh();
    counter_inc(c, cid(1), 5);
    counter_inc(c, cid(2), 3);
    counter_dec(c, cid(2), 1);
    ASSERT_EQ(counter_read(c), 7); // (5-0) + (3-1)
}

// Two clients are distinguished by the FULL 16-byte ClientId, not the first
// byte (proves the hashtable key uses sizeof(ClientId) and not just a prefix).
TEST(client_ids_distinguished_by_full_bytes) {

    Counter *c = fresh();

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

    Counter *a = fresh();
    Counter *b = fresh();

    counter_inc(a, cid(1), 5);
    counter_inc(b, cid(2), 3);

    counter_merge(a, b);
    ASSERT_EQ(counter_read(a), 8);
}

// Classic CRDT result: concurrent increments on different clients converge,
// both replicas read the same value after exchanging state.
TEST(concurrent_inc_converges) {

    Counter *a = fresh();
    Counter *b = fresh();

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

    Counter *a = fresh();
    Counter *b = fresh();

    counter_inc(a, cid(1), 5);
    counter_inc(b, cid(1), 3);

    counter_merge(a, b);
    ASSERT_EQ(counter_read(a), 5); // max(5,3), NOT 8
}

TEST(merge_same_client_max_on_both_directions) {

    Counter *a = fresh();
    Counter *b = fresh();

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

    Counter *a = fresh();
    Counter *b = fresh();

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

    Counter *a1 = fresh();
    Counter *b1 = fresh();
    counter_inc(a1, cid(1), 5);
    counter_dec(a1, cid(1), 1);
    counter_inc(b1, cid(2), 3);
    counter_merge(a1, b1);

    // (b <- a)

    Counter *a2 = fresh();
    Counter *b2 = fresh();
    counter_inc(a2, cid(1), 5);
    counter_dec(a2, cid(1), 1);
    counter_inc(b2, cid(2), 3);
    counter_merge(b2, a2);

    ASSERT_EQ(counter_read(a1), counter_read(b2));
}

TEST(merge_associative) {

    Counter *a = fresh();
    Counter *b = fresh();
    Counter *c = fresh();
    counter_inc(a, cid(1), 5);
    counter_inc(b, cid(2), 3);
    counter_inc(c, cid(3), 2);

    // (a <- b) <- c
    counter_merge(a, b);
    counter_merge(a, c);

    // a <- (b <- c)  (rebuild on a fresh accumulator)

    Counter *a2 = fresh();
    Counter *b2 = fresh();
    Counter *c2 = fresh();
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

    Counter *a = fresh();
    Counter *b = fresh();

    counter_inc(a, cid(1), 5);
    counter_inc(b, cid(2), 3);

    counter_merge(a, b);
    ASSERT_EQ(counter_read(b), 3); // b unchanged
}

// After merge, a subsequent local inc on a newly-learned client accumulates
// from the merged-in value (not from zero).
TEST(local_inc_after_merge_accumulates) {

    Counter *a = fresh();
    Counter *b = fresh();

    counter_inc(b, cid(2), 3);
    counter_merge(a, b); // a learns client 2 = 3

    counter_inc(a, cid(2), 4); // a now also acting as client 2: accumulate to 7
    ASSERT_EQ(counter_read(a), 7);
}

int main(void) {
    RUN(counter_create_stores_id);
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
