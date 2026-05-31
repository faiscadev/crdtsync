#include "arena.h"
#include "clientid.h"
#include "register.h"
#include "scalar.h"
#include "stamp.h"
#include "string.h"
#include "test_util.h"

#define ARENA_BYTES (16 * 1024)

// Build a ClientId fixture from a single byte (rest zero).
static ClientId cid(uint8_t first_byte) {
    uint8_t b[16] = {0};
    b[0] = first_byte;
    return clientid_from_bytes(b);
}

// Build a Stamp from lamport + a ClientId's first byte. Tests stay readable.
static Stamp stmp(uint64_t lamport, uint8_t client_first_byte) {
    return (Stamp){.lamport = lamport, .client_id = cid(client_first_byte)};
}

static Register *fresh(uint8_t *buf, size_t len, Scalar value, Stamp stamp) {
    Arena *arena = arena_create(buf, len);
    return register_create(arena, value, stamp);
}

// --- create / read ---

TEST(create_seeds_value) {
    uint8_t buf[ARENA_BYTES];
    Register *r = fresh(buf, sizeof(buf), scalar_int(42), stmp(1, 1));
    ASSERT(scalar_eq(register_read(r), scalar_int(42)));
}

TEST(create_with_string) {
    uint8_t buf[ARENA_BYTES];
    Register *r = fresh(buf, sizeof(buf),
                        scalar_string((const uint8_t *)"hello", 5), stmp(1, 1));
    ASSERT(scalar_eq(register_read(r),
                     scalar_string((const uint8_t *)"hello", 5)));
}

TEST(create_with_null) {
    uint8_t buf[ARENA_BYTES];
    Register *r = fresh(buf, sizeof(buf), scalar_null(), stmp(1, 1));
    ASSERT(scalar_eq(register_read(r), scalar_null()));
}

TEST(create_with_bool) {
    uint8_t buf[ARENA_BYTES];
    Register *r = fresh(buf, sizeof(buf), scalar_bool(true), stmp(1, 1));
    ASSERT(scalar_eq(register_read(r), scalar_bool(true)));
}

// --- LWW: local set ---

TEST(higher_lamport_wins) {
    uint8_t buf[ARENA_BYTES];
    Register *r = fresh(buf, sizeof(buf), scalar_int(10), stmp(1, 1));
    register_set(r, scalar_int(20), stmp(2, 1));
    ASSERT(scalar_eq(register_read(r), scalar_int(20)));
}

TEST(lower_lamport_ignored) {
    uint8_t buf[ARENA_BYTES];
    Register *r = fresh(buf, sizeof(buf), scalar_int(20), stmp(5, 1));
    register_set(r, scalar_int(10), stmp(3, 1)); // older lamport — ignored
    ASSERT(scalar_eq(register_read(r), scalar_int(20)));
}

TEST(equal_lamport_higher_client_wins) {
    uint8_t buf[ARENA_BYTES];
    Register *r = fresh(buf, sizeof(buf), scalar_int(10), stmp(5, 1));
    register_set(r, scalar_int(20), stmp(5, 2)); // same lamport, higher client
    ASSERT(scalar_eq(register_read(r), scalar_int(20)));
}

TEST(equal_lamport_lower_client_ignored) {
    uint8_t buf[ARENA_BYTES];
    Register *r = fresh(buf, sizeof(buf), scalar_int(20), stmp(5, 2));
    register_set(r, scalar_int(10), stmp(5, 1)); // same lamport, lower client
    ASSERT(scalar_eq(register_read(r), scalar_int(20)));
}

TEST(set_same_stamp_idempotent) {
    uint8_t buf[ARENA_BYTES];
    Register *r = fresh(buf, sizeof(buf), scalar_int(42), stmp(5, 1));
    register_set(r, scalar_int(42), stmp(5, 1));
    ASSERT(scalar_eq(register_read(r), scalar_int(42)));
}

// Order of application does not matter: newer-then-older converges to newer.
TEST(out_of_order_sets_converge) {
    uint8_t buf[ARENA_BYTES];
    Register *r = fresh(buf, sizeof(buf), scalar_int(1), stmp(1, 1));
    register_set(r, scalar_int(99), stmp(10, 1)); // newer
    register_set(r, scalar_int(2), stmp(2, 1));   // older — ignored
    ASSERT(scalar_eq(register_read(r), scalar_int(99)));
}

// A newer write can change the Scalar kind.
TEST(kind_changes_on_newer_write) {
    uint8_t buf[ARENA_BYTES];
    Register *r = fresh(buf, sizeof(buf), scalar_int(42), stmp(1, 1));
    register_set(r, scalar_string((const uint8_t *)"hi", 2), stmp(2, 1));
    ASSERT(
        scalar_eq(register_read(r), scalar_string((const uint8_t *)"hi", 2)));
}

// String bytes must be copied into the arena: mutating the caller's buffer
// after set/create must not affect what register_read returns.
TEST(string_bytes_are_copied) {
    uint8_t buf[ARENA_BYTES];
    uint8_t key[8];
    memcpy(key, "hello", 5);
    Register *r = fresh(buf, sizeof(buf), scalar_string(key, 5), stmp(1, 1));

    key[0] = 'X';
    key[1] = 'X';

    ASSERT(scalar_eq(register_read(r),
                     scalar_string((const uint8_t *)"hello", 5)));
}

// --- merge (two replicas) ---

TEST(merge_src_newer_wins) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Register *a = fresh(bufA, sizeof(bufA), scalar_int(10), stmp(1, 1));
    Register *b =
        fresh(bufB, sizeof(bufB), scalar_int(20), stmp(2, 2)); // newer

    register_merge(a, b);
    ASSERT(scalar_eq(register_read(a), scalar_int(20)));
}

TEST(merge_src_older_ignored) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Register *a =
        fresh(bufA, sizeof(bufA), scalar_int(20), stmp(5, 1)); // newer
    Register *b = fresh(bufB, sizeof(bufB), scalar_int(10), stmp(2, 2));

    register_merge(a, b);
    ASSERT(scalar_eq(register_read(a), scalar_int(20)));
}

TEST(merge_equal_lamport_client_tiebreak) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Register *a = fresh(bufA, sizeof(bufA), scalar_int(10), stmp(5, 1));
    Register *b = fresh(bufB, sizeof(bufB), scalar_int(20),
                        stmp(5, 2)); // same lamport, > cid

    register_merge(a, b);
    ASSERT(scalar_eq(register_read(a), scalar_int(20)));
}

// Concurrent writes converge to the same winner regardless of merge direction.
TEST(merge_commutative) {
    uint8_t bA1[ARENA_BYTES], bB1[ARENA_BYTES];
    Register *a1 = fresh(bA1, sizeof(bA1), scalar_int(10), stmp(5, 1));
    Register *b1 = fresh(bB1, sizeof(bB1), scalar_int(20), stmp(5, 2));
    register_merge(a1, b1);

    uint8_t bA2[ARENA_BYTES], bB2[ARENA_BYTES];
    Register *a2 = fresh(bA2, sizeof(bA2), scalar_int(10), stmp(5, 1));
    Register *b2 = fresh(bB2, sizeof(bB2), scalar_int(20), stmp(5, 2));
    register_merge(b2, a2);

    ASSERT(scalar_eq(register_read(a1), register_read(b2)));
    ASSERT(scalar_eq(register_read(a1), scalar_int(20)));
}

TEST(merge_idempotent) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Register *a = fresh(bufA, sizeof(bufA), scalar_int(10), stmp(1, 1));
    Register *b = fresh(bufB, sizeof(bufB), scalar_int(20), stmp(2, 1));

    register_merge(a, b);
    Scalar once = register_read(a);
    register_merge(a, b);
    Scalar twice = register_read(a);

    ASSERT(scalar_eq(once, twice));
    ASSERT(scalar_eq(twice, scalar_int(20)));
}

TEST(merge_associative) {
    // (a <- b) <- c
    uint8_t bA[ARENA_BYTES], bB[ARENA_BYTES], bC[ARENA_BYTES];
    Register *a = fresh(bA, sizeof(bA), scalar_int(10), stmp(1, 1));
    Register *b = fresh(bB, sizeof(bB), scalar_int(20), stmp(2, 1));
    Register *c = fresh(bC, sizeof(bC), scalar_int(30), stmp(3, 1));
    register_merge(a, b);
    register_merge(a, c);

    // a <- (b <- c)
    uint8_t bA2[ARENA_BYTES], bB2[ARENA_BYTES], bC2[ARENA_BYTES];
    Register *a2 = fresh(bA2, sizeof(bA2), scalar_int(10), stmp(1, 1));
    Register *b2 = fresh(bB2, sizeof(bB2), scalar_int(20), stmp(2, 1));
    Register *c2 = fresh(bC2, sizeof(bC2), scalar_int(30), stmp(3, 1));
    register_merge(b2, c2);
    register_merge(a2, b2);

    ASSERT(scalar_eq(register_read(a), register_read(a2)));
    ASSERT(scalar_eq(register_read(a), scalar_int(30)));
}

TEST(merge_does_not_mutate_src) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    Register *a =
        fresh(bufA, sizeof(bufA), scalar_int(99), stmp(10, 1)); // a newer
    Register *b = fresh(bufB, sizeof(bufB), scalar_int(7), stmp(1, 1));

    register_merge(a, b);
    ASSERT(scalar_eq(register_read(b), scalar_int(7))); // b unchanged
}

// When merge takes src's winning string value, dst must own its own copy.
// Mutating src's value bytes after merge must not affect dst's read.
TEST(merge_copies_string_into_dst_arena) {
    uint8_t bufA[ARENA_BYTES], bufB[ARENA_BYTES];
    uint8_t src_bytes[8];
    memcpy(src_bytes, "hello", 5);

    Register *a = fresh(bufA, sizeof(bufA), scalar_int(0), stmp(1, 1));
    Register *b =
        fresh(bufB, sizeof(bufB), scalar_string(src_bytes, 5), stmp(5, 1));

    register_merge(a, b); // a takes b's string

    // Scribble src's buffer.
    src_bytes[0] = 'X';
    src_bytes[1] = 'X';

    ASSERT(scalar_eq(register_read(a),
                     scalar_string((const uint8_t *)"hello", 5)));
}

int main(void) {
    RUN(create_seeds_value);
    RUN(create_with_string);
    RUN(create_with_null);
    RUN(create_with_bool);

    RUN(higher_lamport_wins);
    RUN(lower_lamport_ignored);
    RUN(equal_lamport_higher_client_wins);
    RUN(equal_lamport_lower_client_ignored);
    RUN(set_same_stamp_idempotent);
    RUN(out_of_order_sets_converge);
    RUN(kind_changes_on_newer_write);
    RUN(string_bytes_are_copied);

    RUN(merge_src_newer_wins);
    RUN(merge_src_older_ignored);
    RUN(merge_equal_lamport_client_tiebreak);
    RUN(merge_commutative);
    RUN(merge_idempotent);
    RUN(merge_associative);
    RUN(merge_does_not_mutate_src);
    RUN(merge_copies_string_into_dst_arena);

    TEST_SUMMARY();
}
