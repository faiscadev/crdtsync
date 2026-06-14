#include "arena.h"
#include "clientid.h"
#include "elementid.h"
#include "register.h"
#include "scalar.h"
#include "stamp.h"
#include "string.h"
#include "test_util.h"

// Build a ClientId fixture from a single byte (rest zero).
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

static ElementId default_id(void) { return eid(0xFF, 0); }

// Build a Stamp from lamport + a ClientId's first byte. Tests stay readable.
static Stamp stmp(uint64_t lamport, uint8_t client_first_byte) {
    return (Stamp){.lamport = lamport, .client_id = cid(client_first_byte)};
}

static Register *fresh(Scalar value, Stamp stamp) {
    Arena *arena = arena_create();
    return register_create(arena, default_id(), value, stamp);
}

TEST(register_create_stores_id) {
    Arena *a = arena_create();
    ElementId id = eid(7, 42);
    Register *r = register_create(a, id, scalar_int(0), stmp(1, 1));
    ASSERT(elementid_eq(register_id(r), id) == true);
    arena_destroy(a);
}

TEST(register_clone_preserves_id) {
    Arena *as = arena_create();
    Arena *ad = arena_create();
    ElementId id = eid(7, 42);
    Register *src = register_create(as, id, scalar_int(42), stmp(1, 1));
    Register *clone = register_clone(ad, src);
    ASSERT(elementid_eq(register_id(clone), id) == true);
    arena_destroy(as);
    arena_destroy(ad);
}

// --- create / read ---

TEST(create_seeds_value) {

    Register *r = fresh(scalar_int(42), stmp(1, 1));
    ASSERT(scalar_eq(register_read(r), scalar_int(42)));
}

TEST(create_with_string) {

    Register *r = fresh(scalar_string((const uint8_t *)"hello", 5), stmp(1, 1));
    ASSERT(scalar_eq(register_read(r),
                     scalar_string((const uint8_t *)"hello", 5)));
}

TEST(create_with_null) {

    Register *r = fresh(scalar_null(), stmp(1, 1));
    ASSERT(scalar_eq(register_read(r), scalar_null()));
}

TEST(create_with_bool) {

    Register *r = fresh(scalar_bool(true), stmp(1, 1));
    ASSERT(scalar_eq(register_read(r), scalar_bool(true)));
}

// --- LWW: local set ---

TEST(higher_lamport_wins) {

    Register *r = fresh(scalar_int(10), stmp(1, 1));
    register_set(r, scalar_int(20), stmp(2, 1));
    ASSERT(scalar_eq(register_read(r), scalar_int(20)));
}

TEST(lower_lamport_ignored) {

    Register *r = fresh(scalar_int(20), stmp(5, 1));
    register_set(r, scalar_int(10), stmp(3, 1)); // older lamport — ignored
    ASSERT(scalar_eq(register_read(r), scalar_int(20)));
}

TEST(equal_lamport_higher_client_wins) {

    Register *r = fresh(scalar_int(10), stmp(5, 1));
    register_set(r, scalar_int(20), stmp(5, 2)); // same lamport, higher client
    ASSERT(scalar_eq(register_read(r), scalar_int(20)));
}

TEST(equal_lamport_lower_client_ignored) {

    Register *r = fresh(scalar_int(20), stmp(5, 2));
    register_set(r, scalar_int(10), stmp(5, 1)); // same lamport, lower client
    ASSERT(scalar_eq(register_read(r), scalar_int(20)));
}

TEST(set_same_stamp_idempotent) {

    Register *r = fresh(scalar_int(42), stmp(5, 1));
    register_set(r, scalar_int(42), stmp(5, 1));
    ASSERT(scalar_eq(register_read(r), scalar_int(42)));
}

// Order of application does not matter: newer-then-older converges to newer.
TEST(out_of_order_sets_converge) {

    Register *r = fresh(scalar_int(1), stmp(1, 1));
    register_set(r, scalar_int(99), stmp(10, 1)); // newer
    register_set(r, scalar_int(2), stmp(2, 1));   // older — ignored
    ASSERT(scalar_eq(register_read(r), scalar_int(99)));
}

// A newer write can change the Scalar kind.
TEST(kind_changes_on_newer_write) {

    Register *r = fresh(scalar_int(42), stmp(1, 1));
    register_set(r, scalar_string((const uint8_t *)"hi", 2), stmp(2, 1));
    ASSERT(
        scalar_eq(register_read(r), scalar_string((const uint8_t *)"hi", 2)));
}

// String bytes must be copied into the arena: mutating the caller's buffer
// after set/create must not affect what register_read returns.
TEST(string_bytes_are_copied) {

    uint8_t key[8];
    memcpy(key, "hello", 5);
    Register *r = fresh(scalar_string(key, 5), stmp(1, 1));

    key[0] = 'X';
    key[1] = 'X';

    ASSERT(scalar_eq(register_read(r),
                     scalar_string((const uint8_t *)"hello", 5)));
}

// --- merge (two replicas) ---

TEST(merge_src_newer_wins) {

    Register *a = fresh(scalar_int(10), stmp(1, 1));
    Register *b = fresh(scalar_int(20), stmp(2, 2)); // newer

    register_merge(a, b);
    ASSERT(scalar_eq(register_read(a), scalar_int(20)));
}

TEST(merge_src_older_ignored) {

    Register *a = fresh(scalar_int(20), stmp(5, 1)); // newer
    Register *b = fresh(scalar_int(10), stmp(2, 2));

    register_merge(a, b);
    ASSERT(scalar_eq(register_read(a), scalar_int(20)));
}

TEST(merge_equal_lamport_client_tiebreak) {

    Register *a = fresh(scalar_int(10), stmp(5, 1));
    Register *b = fresh(scalar_int(20), stmp(5, 2)); // same lamport, > cid

    register_merge(a, b);
    ASSERT(scalar_eq(register_read(a), scalar_int(20)));
}

// Concurrent writes converge to the same winner regardless of merge direction.
TEST(merge_commutative) {
    Register *a1 = fresh(scalar_int(10), stmp(5, 1));
    Register *b1 = fresh(scalar_int(20), stmp(5, 2));
    register_merge(a1, b1);

    Register *a2 = fresh(scalar_int(10), stmp(5, 1));
    Register *b2 = fresh(scalar_int(20), stmp(5, 2));
    register_merge(b2, a2);

    ASSERT(scalar_eq(register_read(a1), register_read(b2)));
    ASSERT(scalar_eq(register_read(a1), scalar_int(20)));
}

TEST(merge_idempotent) {

    Register *a = fresh(scalar_int(10), stmp(1, 1));
    Register *b = fresh(scalar_int(20), stmp(2, 1));

    register_merge(a, b);
    Scalar once = register_read(a);
    register_merge(a, b);
    Scalar twice = register_read(a);

    ASSERT(scalar_eq(once, twice));
    ASSERT(scalar_eq(twice, scalar_int(20)));
}

TEST(merge_associative) {
    // (a <- b) <- c
    Register *a = fresh(scalar_int(10), stmp(1, 1));
    Register *b = fresh(scalar_int(20), stmp(2, 1));
    Register *c = fresh(scalar_int(30), stmp(3, 1));
    register_merge(a, b);
    register_merge(a, c);

    // a <- (b <- c)
    Register *a2 = fresh(scalar_int(10), stmp(1, 1));
    Register *b2 = fresh(scalar_int(20), stmp(2, 1));
    Register *c2 = fresh(scalar_int(30), stmp(3, 1));
    register_merge(b2, c2);
    register_merge(a2, b2);

    ASSERT(scalar_eq(register_read(a), register_read(a2)));
    ASSERT(scalar_eq(register_read(a), scalar_int(30)));
}

TEST(merge_does_not_mutate_src) {

    Register *a = fresh(scalar_int(99), stmp(10, 1)); // a newer
    Register *b = fresh(scalar_int(7), stmp(1, 1));

    register_merge(a, b);
    ASSERT(scalar_eq(register_read(b), scalar_int(7))); // b unchanged
}

// When merge takes src's winning string value, dst must own its own copy.
// Mutating src's value bytes after merge must not affect dst's read.
TEST(merge_copies_string_into_dst_arena) {

    uint8_t src_bytes[8];
    memcpy(src_bytes, "hello", 5);

    Register *a = fresh(scalar_int(0), stmp(1, 1));
    Register *b = fresh(scalar_string(src_bytes, 5), stmp(5, 1));

    register_merge(a, b); // a takes b's string

    // Scribble src's buffer.
    src_bytes[0] = 'X';
    src_bytes[1] = 'X';

    ASSERT(scalar_eq(register_read(a),
                     scalar_string((const uint8_t *)"hello", 5)));
}

// --- register_clone: deep copy into a target arena ---

TEST(clone_preserves_value) {
    Arena *as = arena_create();
    Arena *ad = arena_create();
    Register *src =
        register_create(as, default_id(), scalar_int(42), stmp(5, 1));
    Register *clone = register_clone(ad, src);
    ASSERT(clone != NULL);
    ASSERT(clone != src);
    ASSERT(scalar_eq(register_read(clone), scalar_int(42)));
    arena_destroy(as);
    arena_destroy(ad);
}

// Clone must own its string bytes in dst arena — destroying src arena
// must leave the clone intact.
TEST(clone_string_survives_src_arena_destroy) {
    Arena *as = arena_create();
    Arena *ad = arena_create();
    Register *src =
        register_create(as, default_id(),
                        scalar_string((const uint8_t *)"hello", 5), stmp(1, 1));
    Register *clone = register_clone(ad, src);
    arena_destroy(as);
    ASSERT(scalar_eq(register_read(clone),
                     scalar_string((const uint8_t *)"hello", 5)));
    arena_destroy(ad);
}

// Mutating src after clone must not affect the clone, and vice versa.
TEST(clone_independent_of_src) {
    Arena *as = arena_create();
    Arena *ad = arena_create();
    Register *src =
        register_create(as, default_id(), scalar_int(1), stmp(1, 1));
    Register *clone = register_clone(ad, src);
    register_set(src, scalar_int(99), stmp(10, 1));
    register_set(clone, scalar_int(7), stmp(10, 1));
    ASSERT(scalar_eq(register_read(src), scalar_int(99)));
    ASSERT(scalar_eq(register_read(clone), scalar_int(7)));
    arena_destroy(as);
    arena_destroy(ad);
}

// Clone preserves the stamp — subsequent set with a stamp ≤ the source's
// original stamp must lose LWW on the clone.
TEST(clone_preserves_stamp) {
    Arena *as = arena_create();
    Arena *ad = arena_create();
    Register *src =
        register_create(as, default_id(), scalar_int(10), stmp(5, 1));
    Register *clone = register_clone(ad, src);
    register_set(clone, scalar_int(99), stmp(3, 1)); // older, must lose
    ASSERT(scalar_eq(register_read(clone), scalar_int(10)));
    arena_destroy(as);
    arena_destroy(ad);
}

int main(void) {
    RUN(register_create_stores_id);
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

    RUN(register_clone_preserves_id);
    RUN(clone_preserves_value);
    RUN(clone_string_survives_src_arena_destroy);
    RUN(clone_independent_of_src);
    RUN(clone_preserves_stamp);

    TEST_SUMMARY();
}
