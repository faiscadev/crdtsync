#include "elementid.h"
#include "test_util.h"
#include <stdint.h>

// Local kind tags — mirror element.h's enum values. Kept here so this
// test stays independent of element.h. Values must match.
#define K_SCALAR 0
#define K_REGISTER 1
#define K_COUNTER 2
#define K_MAP 3

// Test fixture: build an ElementId where bytes[0..7] = hi big-endian and
// bytes[8..15] = lo big-endian. Distinct (hi, lo) pairs produce distinct
// raw byte arrays — convenient for tests that don't care about UUID
// validity, only about distinct-vs-equal.
static ElementId eid(uint64_t hi, uint64_t lo) {
    uint8_t b[16];
    for (int i = 0; i < 8; i++) {
        b[i] = (uint8_t)((hi >> ((7 - i) * 8)) & 0xff);
        b[8 + i] = (uint8_t)((lo >> ((7 - i) * 8)) & 0xff);
    }
    return elementid_from_bytes(b);
}

// --- construction ---

TEST(from_bytes_round_trips) {
    uint8_t b[16] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                     0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10};
    ElementId id = elementid_from_bytes(b);
    for (int i = 0; i < 16; i++) {
        ASSERT_EQ(id.uuid.bytes[i], b[i]);
    }
}

// --- root sentinel ---

TEST(root_is_stable) {
    ElementId r1 = elementid_root();
    ElementId r2 = elementid_root();
    ASSERT(elementid_eq(r1, r2) == true);
}

TEST(root_is_all_zero) {
    ElementId r = elementid_root();
    for (int i = 0; i < 16; i++) {
        ASSERT_EQ(r.uuid.bytes[i], 0);
    }
}

TEST(root_distinct_from_regular_ids) {
    ElementId r = elementid_root();
    ASSERT(elementid_eq(r, eid(1, 0)) == false);
    ASSERT(elementid_eq(r, eid(0, 1)) == false);
}

// --- equality ---

TEST(eq_same) { ASSERT(elementid_eq(eid(5, 100), eid(5, 100)) == true); }

TEST(eq_different_hi) {
    ASSERT(elementid_eq(eid(5, 100), eid(6, 100)) == false);
}

TEST(eq_different_lo) {
    ASSERT(elementid_eq(eid(5, 100), eid(5, 101)) == false);
}

// --- ordering (lexicographic over the 16 bytes) ---

TEST(cmp_equal_returns_zero) {
    ASSERT_EQ(elementid_cmp(eid(5, 100), eid(5, 100)), 0);
}

TEST(cmp_lower_hi_less) { ASSERT(elementid_cmp(eid(5, 100), eid(6, 0)) < 0); }

TEST(cmp_higher_hi_greater) {
    ASSERT(elementid_cmp(eid(6, 0), eid(5, 100)) > 0);
}

TEST(cmp_hi_dominates_lo) {
    // a has smaller hi but huge lo; b has bigger hi but lo 0.
    ASSERT(elementid_cmp(eid(1, UINT64_MAX), eid(2, 0)) < 0);
}

TEST(cmp_anti_symmetric) {
    ElementId a = eid(1, 100);
    ElementId b = eid(2, 100);
    int ab = elementid_cmp(a, b);
    int ba = elementid_cmp(b, a);
    ASSERT((ab < 0 && ba > 0) || (ab > 0 && ba < 0));
}

TEST(cmp_transitive) {
    ElementId a = eid(5, 1);
    ElementId b = eid(5, 2);
    ElementId c = eid(5, 3);
    ASSERT(elementid_cmp(a, b) < 0);
    ASSERT(elementid_cmp(b, c) < 0);
    ASSERT(elementid_cmp(a, c) < 0);
}

// --- derive (UUID v5) ---
//
// elementid_derive is the convergent-creation path: two replicas calling
// it with matching (parent, key, kind) must land on the same ElementId.
// All four inputs are sensitive to differences.

TEST(derive_is_deterministic) {
    ElementId parent = eid(7, 42);
    ElementId a = elementid_derive(parent, "votes", 5, K_COUNTER);
    ElementId b = elementid_derive(parent, "votes", 5, K_COUNTER);
    ASSERT(elementid_eq(a, b) == true);
}

TEST(derive_different_keys_distinct) {
    ElementId parent = eid(7, 42);
    ASSERT(elementid_eq(elementid_derive(parent, "a", 1, K_COUNTER),
                        elementid_derive(parent, "b", 1, K_COUNTER)) == false);
}

TEST(derive_different_parents_distinct) {
    ElementId p1 = eid(7, 42);
    ElementId p2 = eid(7, 43);
    ASSERT(elementid_eq(elementid_derive(p1, "k", 1, K_COUNTER),
                        elementid_derive(p2, "k", 1, K_COUNTER)) == false);
}

// Kind-in-derive: same parent + same key + different kind must produce
// distinct ids. This is what lets map_register("x") and map_counter("x")
// coexist as distinct logical elements.
TEST(derive_different_kinds_distinct) {
    ElementId parent = eid(7, 42);
    ElementId c = elementid_derive(parent, "x", 1, K_COUNTER);
    ElementId r = elementid_derive(parent, "x", 1, K_REGISTER);
    ElementId m = elementid_derive(parent, "x", 1, K_MAP);
    ASSERT(elementid_eq(c, r) == false);
    ASSERT(elementid_eq(c, m) == false);
    ASSERT(elementid_eq(r, m) == false);
}

TEST(derive_distinct_from_root) {
    ElementId d = elementid_derive(elementid_root(), "k", 1, K_COUNTER);
    ASSERT(elementid_eq(d, elementid_root()) == false);
}

// Binary-safe: keys differing only past an embedded NUL must produce
// distinct ids.
TEST(derive_binary_safe_keys) {
    ElementId parent = eid(7, 42);
    uint8_t k1[3] = {0x01, 0x00, 0x02};
    uint8_t k2[3] = {0x01, 0x00, 0x03};
    ASSERT(elementid_eq(elementid_derive(parent, k1, sizeof k1, K_COUNTER),
                        elementid_derive(parent, k2, sizeof k2, K_COUNTER)) ==
           false);
}

TEST(derive_empty_key_deterministic_and_distinct) {
    ElementId parent = eid(7, 42);
    ElementId e1 = elementid_derive(parent, "", 0, K_COUNTER);
    ElementId e2 = elementid_derive(parent, "", 0, K_COUNTER);
    ElementId nonempty = elementid_derive(parent, "x", 1, K_COUNTER);
    ASSERT(elementid_eq(e1, e2) == true);
    ASSERT(elementid_eq(e1, nonempty) == false);
}

// --- UUID v5 format conformance ---
//
// Per RFC 4122 §4.3 the result of v5 derivation must carry version=5
// in the high nibble of byte 6 and variant=10xx in the high two bits of
// byte 8. Any client / debugger that parses the output as a UUID relies
// on these bits.

TEST(derive_sets_version_5) {
    ElementId d = elementid_derive(eid(7, 42), "k", 1, K_COUNTER);
    ASSERT_EQ((d.uuid.bytes[6] & 0xf0) >> 4, 5);
}

TEST(derive_sets_variant_rfc4122) {
    ElementId d = elementid_derive(eid(7, 42), "k", 1, K_COUNTER);
    // Variant is the high two bits of byte 8 == 10
    ASSERT_EQ((d.uuid.bytes[8] & 0xc0), 0x80);
}

int main(void) {
    RUN(from_bytes_round_trips);

    RUN(root_is_stable);
    RUN(root_is_all_zero);
    RUN(root_distinct_from_regular_ids);

    RUN(eq_same);
    RUN(eq_different_hi);
    RUN(eq_different_lo);

    RUN(cmp_equal_returns_zero);
    RUN(cmp_lower_hi_less);
    RUN(cmp_higher_hi_greater);
    RUN(cmp_hi_dominates_lo);
    RUN(cmp_anti_symmetric);
    RUN(cmp_transitive);

    RUN(derive_is_deterministic);
    RUN(derive_different_keys_distinct);
    RUN(derive_different_parents_distinct);
    RUN(derive_different_kinds_distinct);
    RUN(derive_distinct_from_root);
    RUN(derive_binary_safe_keys);
    RUN(derive_empty_key_deterministic_and_distinct);
    RUN(derive_sets_version_5);
    RUN(derive_sets_variant_rfc4122);

    TEST_SUMMARY();
}
