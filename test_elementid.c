#include "clientid.h"
#include "elementid.h"
#include "test_util.h"
#include <stdint.h>

// Helpers.

static ClientId cid(uint8_t first_byte) {
    uint8_t b[16] = {0};
    b[0] = first_byte;
    return clientid_from_bytes(b);
}

static ElementId eid(uint8_t origin_byte, uint64_t seq) {
    return elementid_new(cid(origin_byte), seq);
}

// --- construction ---

TEST(new_sets_origin_and_seq) {
    ClientId origin = cid(7);
    ElementId id = elementid_new(origin, 42);
    ASSERT(clientid_eq(id.origin, origin) == true);
    ASSERT_EQ(id.seq, 42);
}

TEST(seq_zero_is_valid) {
    ElementId id = eid(1, 0);
    ASSERT_EQ(id.seq, 0);
}

TEST(seq_max_is_valid) {
    ElementId id = eid(1, UINT64_MAX);
    ASSERT_EQ(id.seq, UINT64_MAX);
}

// --- root sentinel ---

// root() must be deterministic — every call returns the same value.
TEST(root_is_stable) {
    ElementId r1 = elementid_root();
    ElementId r2 = elementid_root();
    ASSERT(elementid_eq(r1, r2) == true);
}

// root must not collide with any "regular" id constructed from a non-zero
// client or seq. Two regular ids that happen to match root would break the
// recursive merge dispatcher.
TEST(root_distinct_from_regular_ids) {
    ElementId r = elementid_root();
    ASSERT(elementid_eq(r, eid(1, 0)) == false);
    ASSERT(elementid_eq(r, eid(0, 1)) == false);
}

// --- equality ---

TEST(eq_same) {
    ElementId a = eid(5, 100);
    ElementId b = eid(5, 100);
    ASSERT(elementid_eq(a, b) == true);
}

TEST(eq_different_origin) {
    ElementId a = eid(5, 100);
    ElementId b = eid(6, 100);
    ASSERT(elementid_eq(a, b) == false);
}

TEST(eq_different_seq) {
    ElementId a = eid(5, 100);
    ElementId b = eid(5, 101);
    ASSERT(elementid_eq(a, b) == false);
}

TEST(eq_completely_different) {
    ElementId a = eid(5, 100);
    ElementId b = eid(99, 9999);
    ASSERT(elementid_eq(a, b) == false);
}

// --- ordering (cmp) ---

// Equality returns 0.
TEST(cmp_equal_returns_zero) {
    ElementId a = eid(5, 100);
    ElementId b = eid(5, 100);
    ASSERT_EQ(elementid_cmp(a, b), 0);
}

// Same origin: seq decides.
TEST(cmp_same_origin_lower_seq_less) {
    ElementId a = eid(5, 100);
    ElementId b = eid(5, 200);
    ASSERT(elementid_cmp(a, b) < 0);
}

TEST(cmp_same_origin_higher_seq_greater) {
    ElementId a = eid(5, 200);
    ElementId b = eid(5, 100);
    ASSERT(elementid_cmp(a, b) > 0);
}

// Different origin: origin decides (via clientid_cmp), seq is irrelevant.
TEST(cmp_origin_dominates_seq) {
    // a has smaller origin but huge seq; b has bigger origin but seq 0.
    ElementId a = eid(1, UINT64_MAX);
    ElementId b = eid(2, 0);
    ASSERT(elementid_cmp(a, b) < 0);
}

// Anti-symmetric: cmp(a, b) and cmp(b, a) have opposite signs (or both 0).
TEST(cmp_anti_symmetric_origin) {
    ElementId a = eid(1, 100);
    ElementId b = eid(2, 100);
    int ab = elementid_cmp(a, b);
    int ba = elementid_cmp(b, a);
    ASSERT((ab < 0 && ba > 0) || (ab > 0 && ba < 0));
}

TEST(cmp_anti_symmetric_seq) {
    ElementId a = eid(5, 1);
    ElementId b = eid(5, 2);
    int ab = elementid_cmp(a, b);
    int ba = elementid_cmp(b, a);
    ASSERT((ab < 0 && ba > 0) || (ab > 0 && ba < 0));
}

// Transitive: a < b and b < c implies a < c.
TEST(cmp_transitive) {
    ElementId a = eid(5, 1);
    ElementId b = eid(5, 2);
    ElementId c = eid(5, 3);
    ASSERT(elementid_cmp(a, b) < 0);
    ASSERT(elementid_cmp(b, c) < 0);
    ASSERT(elementid_cmp(a, c) < 0);
}

// Trichotomy: for any two ids, exactly one of (a < b, b < a, equal) holds.
TEST(cmp_trichotomy_distinct) {
    ElementId a = eid(1, 0);
    ElementId b = eid(2, 0);
    int ab = elementid_cmp(a, b);
    int ba = elementid_cmp(b, a);
    ASSERT((ab < 0 && ba > 0) || (ab > 0 && ba < 0));
}

TEST(cmp_trichotomy_equal) {
    ElementId a = eid(5, 100);
    ElementId b = eid(5, 100);
    ASSERT_EQ(elementid_cmp(a, b), 0);
    ASSERT_EQ(elementid_cmp(b, a), 0);
}

int main(void) {
    RUN(new_sets_origin_and_seq);
    RUN(seq_zero_is_valid);
    RUN(seq_max_is_valid);

    RUN(root_is_stable);
    RUN(root_distinct_from_regular_ids);

    RUN(eq_same);
    RUN(eq_different_origin);
    RUN(eq_different_seq);
    RUN(eq_completely_different);

    RUN(cmp_equal_returns_zero);
    RUN(cmp_same_origin_lower_seq_less);
    RUN(cmp_same_origin_higher_seq_greater);
    RUN(cmp_origin_dominates_seq);
    RUN(cmp_anti_symmetric_origin);
    RUN(cmp_anti_symmetric_seq);
    RUN(cmp_transitive);
    RUN(cmp_trichotomy_distinct);
    RUN(cmp_trichotomy_equal);

    TEST_SUMMARY();
}
