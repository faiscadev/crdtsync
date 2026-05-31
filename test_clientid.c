#include "clientid.h"
#include "test_util.h"
#include <stdint.h>

// --- construction / accessors ---

TEST(construct_copies_bytes) {
    uint8_t src[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    ClientId id = clientid_from_bytes(src);

    // Scribble the source after construct.
    src[0] = 0xFF;
    src[15] = 0xFF;

    ASSERT_EQ(id.bytes[0], 1);
    ASSERT_EQ(id.bytes[15], 16);
}

TEST(bytes_accessible) {
    uint8_t src[16] = {0xAA, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xBB};
    ClientId id = clientid_from_bytes(src);
    ASSERT_EQ(id.bytes[0], 0xAA);
    ASSERT_EQ(id.bytes[15], 0xBB);
    for (int i = 1; i < 15; i++) {
        ASSERT_EQ(id.bytes[i], 0);
    }
}

// --- equality ---

TEST(eq_same) {
    uint8_t buf[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    ClientId a = clientid_from_bytes(buf);
    ClientId b = clientid_from_bytes(buf);
    ASSERT(clientid_eq(a, b) == true);
}

TEST(eq_all_zero) {
    uint8_t z[16] = {0};
    ClientId a = clientid_from_bytes(z);
    ClientId b = clientid_from_bytes(z);
    ASSERT(clientid_eq(a, b) == true);
}

TEST(eq_differing_last_byte) {
    uint8_t a_bytes[16] = {1, 2,  3,  4,  5,  6,  7,  8,
                           9, 10, 11, 12, 13, 14, 15, 16};
    uint8_t b_bytes[16] = {1, 2,  3,  4,  5,  6,  7,  8,
                           9, 10, 11, 12, 13, 14, 15, 99};
    ClientId a = clientid_from_bytes(a_bytes);
    ClientId b = clientid_from_bytes(b_bytes);
    ASSERT(clientid_eq(a, b) == false);
}

TEST(eq_differing_first_byte) {
    uint8_t a_bytes[16] = {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    uint8_t b_bytes[16] = {2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    ClientId a = clientid_from_bytes(a_bytes);
    ClientId b = clientid_from_bytes(b_bytes);
    ASSERT(clientid_eq(a, b) == false);
}

// --- comparison (sign only; magnitudes are implementation-defined) ---

TEST(cmp_equal_returns_zero) {
    uint8_t buf[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    ClientId a = clientid_from_bytes(buf);
    ClientId b = clientid_from_bytes(buf);
    ASSERT_EQ(clientid_cmp(a, b), 0);
}

TEST(cmp_less_first_byte) {
    uint8_t a_bytes[16] = {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    uint8_t b_bytes[16] = {2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    ClientId a = clientid_from_bytes(a_bytes);
    ClientId b = clientid_from_bytes(b_bytes);
    ASSERT(clientid_cmp(a, b) < 0);
}

TEST(cmp_greater_first_byte) {
    uint8_t a_bytes[16] = {9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    uint8_t b_bytes[16] = {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    ClientId a = clientid_from_bytes(a_bytes);
    ClientId b = clientid_from_bytes(b_bytes);
    ASSERT(clientid_cmp(a, b) > 0);
}

// Earlier-byte differences dominate later ones (lexicographic).
TEST(cmp_earlier_byte_dominates) {
    uint8_t a_bytes[16] = {1,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                           0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    uint8_t b_bytes[16] = {2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    ClientId a = clientid_from_bytes(a_bytes);
    ClientId b = clientid_from_bytes(b_bytes);
    ASSERT(clientid_cmp(a, b) < 0); // byte 0: 1 < 2, rest irrelevant
}

TEST(cmp_last_byte_differs) {
    uint8_t a_bytes[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
    uint8_t b_bytes[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2};
    ClientId a = clientid_from_bytes(a_bytes);
    ClientId b = clientid_from_bytes(b_bytes);
    ASSERT(clientid_cmp(a, b) < 0);
}

// Bytes are compared as unsigned: 0x80 > 0x01, never negative.
TEST(cmp_byte_unsigned) {
    uint8_t a_bytes[16] = {0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    uint8_t b_bytes[16] = {0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    ClientId a = clientid_from_bytes(a_bytes);
    ClientId b = clientid_from_bytes(b_bytes);
    ASSERT(clientid_cmp(a, b) > 0);
}

// cmp is anti-symmetric: cmp(a,b) and cmp(b,a) have opposite signs.
TEST(cmp_antisymmetric) {
    uint8_t a_bytes[16] = {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    uint8_t b_bytes[16] = {2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    ClientId a = clientid_from_bytes(a_bytes);
    ClientId b = clientid_from_bytes(b_bytes);
    int ab = clientid_cmp(a, b);
    int ba = clientid_cmp(b, a);
    ASSERT((ab < 0 && ba > 0) || (ab > 0 && ba < 0) || (ab == 0 && ba == 0));
}

int main(void) {
    RUN(construct_copies_bytes);
    RUN(bytes_accessible);

    RUN(eq_same);
    RUN(eq_all_zero);
    RUN(eq_differing_last_byte);
    RUN(eq_differing_first_byte);

    RUN(cmp_equal_returns_zero);
    RUN(cmp_less_first_byte);
    RUN(cmp_greater_first_byte);
    RUN(cmp_earlier_byte_dominates);
    RUN(cmp_last_byte_differs);
    RUN(cmp_byte_unsigned);
    RUN(cmp_antisymmetric);

    TEST_SUMMARY();
}
