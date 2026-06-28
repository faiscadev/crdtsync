#include "clientid.h"
#include "elementid.h"
#include "register.h"
#include "scalar.h"
#include "stamp.h"
#include "string.h"
#include "test_util.h"

// NOTE: these tests target the refcounted lifecycle contract (host_malloc
// backing, no arena), mirroring test_counter.c. They will not link until
// register.h / register.c are converted:
//   Register *register_create(ElementId id, Scalar value, Stamp stamp); // rc=1
//   Register *register_clone(const Register *reg);                      // rc=1
//   void register_acquire(Register *);
//   void register_release(Register *);   // frees at rc 0; scalar_free's value
//   void register_displace(Register *);
//   bool register_is_displaced(const Register *);

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
    return register_create(default_id(), value, stamp);
}

TEST(register_create_stores_id) {
    ElementId id = eid(7, 42);
    Register *r = register_create(id, scalar_int(0), stmp(1, 1));
    ASSERT(elementid_eq(register_id(r), id) == true);
    register_release(r);
}

// --- create / read ---

TEST(create_seeds_value) {
    Register *r = fresh(scalar_int(42), stmp(1, 1));
    ASSERT(scalar_eq(register_read(r), scalar_int(42)));
    register_release(r);
}

TEST(create_with_string) {
    Register *r = fresh(scalar_string((const uint8_t *)"hello", 5), stmp(1, 1));
    ASSERT(scalar_eq(register_read(r),
                     scalar_string((const uint8_t *)"hello", 5)));
    register_release(r);
}

TEST(create_with_null) {
    Register *r = fresh(scalar_null(), stmp(1, 1));
    ASSERT(scalar_eq(register_read(r), scalar_null()));
    register_release(r);
}

TEST(create_with_bool) {
    Register *r = fresh(scalar_bool(true), stmp(1, 1));
    ASSERT(scalar_eq(register_read(r), scalar_bool(true)));
    register_release(r);
}

// --- LWW: local set ---

TEST(higher_lamport_wins) {
    Register *r = fresh(scalar_int(10), stmp(1, 1));
    register_set(r, scalar_int(20), stmp(2, 1));
    ASSERT(scalar_eq(register_read(r), scalar_int(20)));
    register_release(r);
}

TEST(lower_lamport_ignored) {
    Register *r = fresh(scalar_int(20), stmp(5, 1));
    register_set(r, scalar_int(10), stmp(3, 1)); // older lamport — ignored
    ASSERT(scalar_eq(register_read(r), scalar_int(20)));
    register_release(r);
}

TEST(equal_lamport_higher_client_wins) {
    Register *r = fresh(scalar_int(10), stmp(5, 1));
    register_set(r, scalar_int(20), stmp(5, 2)); // same lamport, higher client
    ASSERT(scalar_eq(register_read(r), scalar_int(20)));
    register_release(r);
}

TEST(equal_lamport_lower_client_ignored) {
    Register *r = fresh(scalar_int(20), stmp(5, 2));
    register_set(r, scalar_int(10), stmp(5, 1)); // same lamport, lower client
    ASSERT(scalar_eq(register_read(r), scalar_int(20)));
    register_release(r);
}

TEST(set_same_stamp_idempotent) {
    Register *r = fresh(scalar_int(42), stmp(5, 1));
    register_set(r, scalar_int(42), stmp(5, 1));
    ASSERT(scalar_eq(register_read(r), scalar_int(42)));
    register_release(r);
}

// Order of application does not matter: newer-then-older converges to newer.
TEST(out_of_order_sets_converge) {
    Register *r = fresh(scalar_int(1), stmp(1, 1));
    register_set(r, scalar_int(99), stmp(10, 1)); // newer
    register_set(r, scalar_int(2), stmp(2, 1));   // older — ignored
    ASSERT(scalar_eq(register_read(r), scalar_int(99)));
    register_release(r);
}

// A newer write can change the Scalar kind.
TEST(kind_changes_on_newer_write) {
    Register *r = fresh(scalar_int(42), stmp(1, 1));
    register_set(r, scalar_string((const uint8_t *)"hi", 2), stmp(2, 1));
    ASSERT(
        scalar_eq(register_read(r), scalar_string((const uint8_t *)"hi", 2)));
    register_release(r);
}

// String bytes must be copied into owned storage: mutating the caller's buffer
// after set/create must not affect what register_read returns.
TEST(string_bytes_are_copied) {
    uint8_t key[8];
    memcpy(key, "hello", 5);
    Register *r = fresh(scalar_string(key, 5), stmp(1, 1));

    key[0] = 'X';
    key[1] = 'X';

    ASSERT(scalar_eq(register_read(r),
                     scalar_string((const uint8_t *)"hello", 5)));
    register_release(r);
}

// A newer string write must free the previously-held string and own the new
// one. (Behaviorally observable only as "no leak"; the read assertion guards
// correctness, ASan/leak-checking guards the free.)
TEST(string_replaced_by_newer_string) {
    Register *r = fresh(scalar_string((const uint8_t *)"first", 5), stmp(1, 1));
    register_set(r, scalar_string((const uint8_t *)"second", 6), stmp(2, 1));
    ASSERT(scalar_eq(register_read(r),
                     scalar_string((const uint8_t *)"second", 6)));
    register_release(r);
}

// --- merge (two replicas) ---

TEST(merge_src_newer_wins) {
    Register *a = fresh(scalar_int(10), stmp(1, 1));
    Register *b = fresh(scalar_int(20), stmp(2, 2)); // newer

    register_merge(a, b);
    ASSERT(scalar_eq(register_read(a), scalar_int(20)));
    register_release(a);
    register_release(b);
}

TEST(merge_src_older_ignored) {
    Register *a = fresh(scalar_int(20), stmp(5, 1)); // newer
    Register *b = fresh(scalar_int(10), stmp(2, 2));

    register_merge(a, b);
    ASSERT(scalar_eq(register_read(a), scalar_int(20)));
    register_release(a);
    register_release(b);
}

TEST(merge_equal_lamport_client_tiebreak) {
    Register *a = fresh(scalar_int(10), stmp(5, 1));
    Register *b = fresh(scalar_int(20), stmp(5, 2)); // same lamport, > cid

    register_merge(a, b);
    ASSERT(scalar_eq(register_read(a), scalar_int(20)));
    register_release(a);
    register_release(b);
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
    register_release(a1);
    register_release(b1);
    register_release(a2);
    register_release(b2);
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
    register_release(a);
    register_release(b);
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
    register_release(a);
    register_release(b);
    register_release(c);
    register_release(a2);
    register_release(b2);
    register_release(c2);
}

TEST(merge_does_not_mutate_src) {
    Register *a = fresh(scalar_int(99), stmp(10, 1)); // a newer
    Register *b = fresh(scalar_int(7), stmp(1, 1));

    register_merge(a, b);
    ASSERT(scalar_eq(register_read(b), scalar_int(7))); // b unchanged
    register_release(a);
    register_release(b);
}

// When merge takes src's winning string value, dst must own its own copy.
// Releasing src after the merge must leave dst's string intact.
TEST(merge_string_survives_src_release) {
    Register *a = fresh(scalar_int(0), stmp(1, 1));
    Register *b = fresh(scalar_string((const uint8_t *)"hello", 5), stmp(5, 1));

    register_merge(a, b); // a takes b's string (deep copy)
    register_release(b);  // b frees; a must be unaffected

    ASSERT(scalar_eq(register_read(a),
                     scalar_string((const uint8_t *)"hello", 5)));
    register_release(a);
}

// --- register_clone: deep copy, fresh refcount ---

TEST(register_clone_preserves_id) {
    ElementId id = eid(7, 42);
    Register *src = register_create(id, scalar_int(42), stmp(1, 1));
    Register *clone = register_clone(src);
    ASSERT(elementid_eq(register_id(clone), id) == true);
    register_release(src);
    register_release(clone);
}

TEST(clone_preserves_value) {
    Register *src = register_create(default_id(), scalar_int(42), stmp(5, 1));
    Register *clone = register_clone(src);
    ASSERT(clone != NULL);
    ASSERT(clone != src);
    ASSERT(scalar_eq(register_read(clone), scalar_int(42)));
    register_release(src);
    register_release(clone);
}

// Clone must own its own string copy — releasing src must leave clone intact.
TEST(clone_string_survives_src_release) {
    Register *src = register_create(
        default_id(), scalar_string((const uint8_t *)"hello", 5), stmp(1, 1));
    Register *clone = register_clone(src);
    register_release(src);
    ASSERT(scalar_eq(register_read(clone),
                     scalar_string((const uint8_t *)"hello", 5)));
    register_release(clone);
}

// Mutating src after clone must not affect the clone, and vice versa.
TEST(clone_independent_of_src) {
    Register *src = register_create(default_id(), scalar_int(1), stmp(1, 1));
    Register *clone = register_clone(src);
    register_set(src, scalar_int(99), stmp(10, 1));
    register_set(clone, scalar_int(7), stmp(10, 1));
    ASSERT(scalar_eq(register_read(src), scalar_int(99)));
    ASSERT(scalar_eq(register_read(clone), scalar_int(7)));
    register_release(src);
    register_release(clone);
}

// Clone preserves the stamp — a subsequent set with a stamp ≤ the source's
// original stamp must lose LWW on the clone.
TEST(clone_preserves_stamp) {
    Register *src = register_create(default_id(), scalar_int(10), stmp(5, 1));
    Register *clone = register_clone(src);
    register_set(clone, scalar_int(99), stmp(3, 1)); // older, must lose
    ASSERT(scalar_eq(register_read(clone), scalar_int(10)));
    register_release(src);
    register_release(clone);
}

// --- refcount + displacement ---
//
// register_create returns refcount=1; release on a fresh handle frees.
// acquire/release accounting balances correctly across multiple holders.
// The displaced flag is independent of refcount: marking displaced does not
// free the Register; only refcount reaching zero does.

TEST(create_starts_not_displaced) {
    Register *r = fresh(scalar_int(0), stmp(1, 1));
    ASSERT(register_is_displaced(r) == false);
    register_release(r);
}

TEST(displace_sets_flag) {
    Register *r = fresh(scalar_int(0), stmp(1, 1));
    register_displace(r);
    ASSERT(register_is_displaced(r) == true);
    register_release(r);
}

TEST(displaced_register_still_mutable_locally) {
    Register *r = fresh(scalar_int(1), stmp(1, 1));
    register_displace(r);
    // Zombie writes still mutate the local Register — the Doc layer is
    // responsible for skipping op emission. The primitive does not refuse
    // mutations.
    register_set(r, scalar_int(2), stmp(2, 1));
    ASSERT(scalar_eq(register_read(r), scalar_int(2)));
    register_release(r);
}

// acquire balances release: one extra acquire + one extra release keeps the
// Register alive and readable (refcount: 1 -> 2 -> 1).
TEST(acquire_release_balanced_keeps_alive) {
    Register *r = fresh(scalar_int(5), stmp(1, 1));
    register_acquire(r); // refcount = 2
    register_set(r, scalar_int(9), stmp(2, 1));
    register_release(r); // refcount = 1
    ASSERT(scalar_eq(register_read(r), scalar_int(9)));
    register_release(r); // refcount = 0 → freed
}

// Clone is created with refcount=1 (independent of source's refcount).
// Releasing source while the clone is held leaves clone alive.
TEST(clone_has_independent_refcount) {
    Register *src = register_create(default_id(), scalar_int(5), stmp(1, 1));
    Register *clone = register_clone(src);
    register_release(src); // src frees; clone untouched
    ASSERT(scalar_eq(register_read(clone), scalar_int(5)));
    register_release(clone);
}

// Clone of a displaced Register is itself not displaced — displacement is
// per-instance state, not part of the value.
TEST(clone_of_displaced_register_is_not_displaced) {
    Register *src = register_create(default_id(), scalar_int(0), stmp(1, 1));
    register_displace(src);
    Register *clone = register_clone(src);
    ASSERT(register_is_displaced(clone) == false);
    register_release(src);
    register_release(clone);
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
    RUN(string_replaced_by_newer_string);

    RUN(merge_src_newer_wins);
    RUN(merge_src_older_ignored);
    RUN(merge_equal_lamport_client_tiebreak);
    RUN(merge_commutative);
    RUN(merge_idempotent);
    RUN(merge_associative);
    RUN(merge_does_not_mutate_src);
    RUN(merge_string_survives_src_release);

    RUN(register_clone_preserves_id);
    RUN(clone_preserves_value);
    RUN(clone_string_survives_src_release);
    RUN(clone_independent_of_src);
    RUN(clone_preserves_stamp);

    RUN(create_starts_not_displaced);
    RUN(displace_sets_flag);
    RUN(displaced_register_still_mutable_locally);
    RUN(acquire_release_balanced_keeps_alive);
    RUN(clone_has_independent_refcount);
    RUN(clone_of_displaced_register_is_not_displaced);

    TEST_SUMMARY();
}
