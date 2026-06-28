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

static ElementId eid(uint64_t hi, uint64_t lo) {
    uint8_t b[16];
    for (int i = 0; i < 8; i++) {
        b[i] = (uint8_t)((hi >> ((7 - i) * 8)) & 0xff);
        b[8 + i] = (uint8_t)((lo >> ((7 - i) * 8)) & 0xff);
    }
    return elementid_from_bytes(b);
}

// Default id for tests that don't care about identity.
static ElementId default_id(void) { return eid(0xFF, 0); }

static Counter *fresh(void) { return counter_create(default_id()); }

TEST(counter_create_stores_id) {
    ElementId id = eid(7, 42);
    Counter *c = counter_create(id);
    ASSERT(elementid_eq(counter_id(c), id) == true);
    counter_release(c);
}

// --- local operations (single replica) ---

TEST(empty_reads_zero) {
    Counter *c = fresh();
    ASSERT_EQ(counter_read(c), 0);
    counter_release(c);
}

TEST(single_inc) {
    Counter *c = fresh();
    counter_inc(c, cid(1), 5);
    ASSERT_EQ(counter_read(c), 5);
    counter_release(c);
}

TEST(inc_then_dec_nets) {
    Counter *c = fresh();
    counter_inc(c, cid(1), 5);
    counter_dec(c, cid(1), 2);
    ASSERT_EQ(counter_read(c), 3);
    counter_release(c);
}

// Local repeated ops on the same client accumulate (this is NOT max).
TEST(local_inc_accumulates) {
    Counter *c = fresh();
    counter_inc(c, cid(1), 5);
    counter_inc(c, cid(1), 2);
    ASSERT_EQ(counter_read(c), 7);
    counter_release(c);
}

TEST(read_can_go_negative) {
    Counter *c = fresh();
    counter_dec(c, cid(1), 3);
    ASSERT_EQ(counter_read(c), -3);
    counter_release(c);
}

TEST(two_clients_sum_in_one_replica) {
    Counter *c = fresh();
    counter_inc(c, cid(1), 5);
    counter_inc(c, cid(2), 3);
    counter_dec(c, cid(2), 1);
    ASSERT_EQ(counter_read(c), 7); // (5-0) + (3-1)
    counter_release(c);
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
    counter_release(c);
}

// --- merge (two replicas) ---

TEST(merge_disjoint_clients_unions) {
    Counter *a = fresh();
    Counter *b = fresh();

    counter_inc(a, cid(1), 5);
    counter_inc(b, cid(2), 3);

    counter_merge(a, b);
    ASSERT_EQ(counter_read(a), 8);
    counter_release(a);
    counter_release(b);
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
    counter_release(a);
    counter_release(b);
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
    counter_release(a);
    counter_release(b);
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
    counter_release(a);
    counter_release(b);
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
    counter_release(a);
    counter_release(b);
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
    counter_release(a1);
    counter_release(b1);
    counter_release(a2);
    counter_release(b2);
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
    counter_release(a);
    counter_release(b);
    counter_release(c);
    counter_release(a2);
    counter_release(b2);
    counter_release(c2);
}

// Merge leaves the source untouched.
TEST(merge_does_not_mutate_src) {
    Counter *a = fresh();
    Counter *b = fresh();

    counter_inc(a, cid(1), 5);
    counter_inc(b, cid(2), 3);

    counter_merge(a, b);
    ASSERT_EQ(counter_read(b), 3); // b unchanged
    counter_release(a);
    counter_release(b);
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
    counter_release(a);
    counter_release(b);
}

// --- counter_clone: deep copy into a fresh refcount=1 allocation ---

TEST(clone_empty_counter_reads_zero) {
    Counter *src = counter_create(default_id());
    Counter *clone = counter_clone(src);
    ASSERT(clone != NULL);
    ASSERT(clone != src);
    ASSERT_EQ(counter_read(clone), 0);
    counter_release(src);
    counter_release(clone);
}

// Clone preserves the source's id. Cloned element represents the same
// logical element, just an independent host_malloc'd copy.
TEST(clone_preserves_id) {
    ElementId id = eid(7, 42);
    Counter *src = counter_create(id);
    Counter *clone = counter_clone(src);
    ASSERT(elementid_eq(counter_id(clone), id) == true);
    counter_release(src);
    counter_release(clone);
}

TEST(clone_preserves_per_client_tallies) {
    Counter *src = counter_create(default_id());
    counter_inc(src, cid(1), 5);
    counter_inc(src, cid(2), 3);
    counter_dec(src, cid(1), 2);
    Counter *clone = counter_clone(src);
    ASSERT_EQ(counter_read(clone), counter_read(src));
    ASSERT_EQ(counter_read(clone), 6); // (5-2) + 3
    counter_release(src);
    counter_release(clone);
}

// Clone owns its own data — releasing the source must leave the clone
// intact.
TEST(clone_survives_src_release) {
    Counter *src = counter_create(default_id());
    counter_inc(src, cid(1), 5);
    counter_inc(src, cid(2), 3);
    Counter *clone = counter_clone(src);
    counter_release(src);
    ASSERT_EQ(counter_read(clone), 8);
    counter_release(clone);
}

// Mutating src after clone must not affect the clone, and vice versa.
TEST(clone_independent_of_src) {
    Counter *src = counter_create(default_id());
    counter_inc(src, cid(1), 5);
    Counter *clone = counter_clone(src);
    counter_inc(src, cid(1), 100); // src now 105
    counter_inc(clone, cid(2), 7); // clone now 12 (5 + 7)
    ASSERT_EQ(counter_read(src), 105);
    ASSERT_EQ(counter_read(clone), 12);
    counter_release(src);
    counter_release(clone);
}

// --- refcount + displacement ---
//
// counter_create returns refcount=1; release on a fresh handle frees.
// acquire/release accounting balances correctly across multiple holders.
// The displaced flag is independent of refcount: marking displaced does
// not free the Counter; only refcount reaching zero does.

TEST(create_starts_not_displaced) {
    Counter *c = counter_create(default_id());
    ASSERT(counter_is_displaced(c) == false);
    counter_release(c);
}

TEST(displace_sets_flag) {
    Counter *c = counter_create(default_id());
    counter_displace(c);
    ASSERT(counter_is_displaced(c) == true);
    counter_release(c);
}

TEST(displaced_counter_still_mutable_locally) {
    Counter *c = counter_create(default_id());
    counter_inc(c, cid(1), 5);
    counter_displace(c);
    // Zombie writes still mutate the local Counter — Doc layer is
    // responsible for skipping op emission. The primitive itself doesn't
    // refuse mutations.
    counter_inc(c, cid(1), 3);
    ASSERT_EQ(counter_read(c), 8);
    counter_release(c);
}

// acquire balances release: two acquires, three releases would free the
// Counter (refcount: 1 -> 2 -> 3 -> 2 -> 1 -> 0). Test the balanced case
// of one extra acquire + one extra release.
TEST(acquire_release_balanced_keeps_alive) {
    Counter *c = counter_create(default_id());
    counter_acquire(c); // refcount = 2
    counter_inc(c, cid(1), 5);
    counter_release(c); // refcount = 1
    // Still alive, still readable.
    ASSERT_EQ(counter_read(c), 5);
    counter_release(c); // refcount = 0 → freed
}

// Clone is created with refcount=1 (independent of source's refcount).
// Releasing source while the clone is held leaves clone alive.
TEST(clone_has_independent_refcount) {
    Counter *src = counter_create(default_id());
    counter_inc(src, cid(1), 5);
    Counter *clone = counter_clone(src);
    counter_release(src); // src frees; clone untouched
    ASSERT_EQ(counter_read(clone), 5);
    counter_release(clone);
}

// Clone of a displaced Counter is itself not displaced — displacement is
// per-instance state, not part of the value.
TEST(clone_of_displaced_counter_is_not_displaced) {
    Counter *src = counter_create(default_id());
    counter_displace(src);
    Counter *clone = counter_clone(src);
    ASSERT(counter_is_displaced(clone) == false);
    counter_release(src);
    counter_release(clone);
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

    RUN(clone_empty_counter_reads_zero);
    RUN(clone_preserves_id);
    RUN(clone_preserves_per_client_tallies);
    RUN(clone_survives_src_release);
    RUN(clone_independent_of_src);

    RUN(create_starts_not_displaced);
    RUN(displace_sets_flag);
    RUN(displaced_counter_still_mutable_locally);
    RUN(acquire_release_balanced_keeps_alive);
    RUN(clone_has_independent_refcount);
    RUN(clone_of_displaced_counter_is_not_displaced);

    TEST_SUMMARY();
}
