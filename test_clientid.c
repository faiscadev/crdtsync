#include "clientid.h"
#include "test_util.h"
#include <stdint.h>
#include <time.h>

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

// --- UUID v7 layout (RFC 9562) ---
//
// Bytes 0..5  : 48-bit unix-ms timestamp, big-endian
// Byte  6     : upper nibble = version (0x7); lower nibble = top 4 bits of
// rand_a Byte  7     : low 8 bits of rand_a   (rand_a is 12 bits total) Byte  8
// : upper 2 bits = variant (0b10); lower 6 bits = top 6 bits of rand_b
// Bytes 9..15 : low 56 bits of rand_b   (rand_b is 62 bits total)

static uint64_t decode_v7_ts_ms(ClientId id) {
    uint64_t ts = 0;
    for (int i = 0; i < 6; i++) {
        ts = (ts << 8) | id.bytes[i];
    }
    return ts;
}

static uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)(ts.tv_nsec / 1000000);
}

// Fixture randomness (deterministic): exactly 10 input bytes.
static const uint8_t RND10_A[10] = {0xA1, 0xB2, 0xC3, 0xD4, 0xE5,
                                    0xF6, 0x07, 0x18, 0x29, 0x3A};
static const uint8_t RND10_B[10] = {0x11, 0x22, 0x33, 0x44, 0x55,
                                    0x66, 0x77, 0x88, 0x99, 0xAA};

TEST(v7_version_nibble_is_7) {
    ClientId id = clientid_v7(0x0123456789AB, RND10_A);
    ASSERT_EQ((id.bytes[6] >> 4) & 0x0F, 0x7);
}

TEST(v7_variant_bits_are_10) {
    ClientId id = clientid_v7(0x0123456789AB, RND10_A);
    // Top 2 bits of byte 8 must be 0b10 (== 0x02 when shifted down).
    ASSERT_EQ((id.bytes[8] >> 6) & 0x03, 0x02);
}

// Timestamp lives in bytes 0..5, big-endian.
TEST(v7_timestamp_is_big_endian) {
    uint64_t ts = 0x0123456789ABULL; // 48-bit value
    ClientId id = clientid_v7(ts, RND10_A);
    ASSERT_EQ(id.bytes[0], 0x01);
    ASSERT_EQ(id.bytes[1], 0x23);
    ASSERT_EQ(id.bytes[2], 0x45);
    ASSERT_EQ(id.bytes[3], 0x67);
    ASSERT_EQ(id.bytes[4], 0x89);
    ASSERT_EQ(id.bytes[5], 0xAB);
}

TEST(v7_timestamp_zero_keeps_version_and_variant) {
    ClientId id = clientid_v7(0, RND10_A);
    for (int i = 0; i < 6; i++) {
        ASSERT_EQ(id.bytes[i], 0);
    }
    ASSERT_EQ((id.bytes[6] >> 4) & 0x0F, 0x7);
    ASSERT_EQ((id.bytes[8] >> 6) & 0x03, 0x02);
}

// Same timestamp, different random → different id (random bytes are actually
// used, not zeroed).
TEST(v7_different_random_yields_different_id) {
    ClientId a = clientid_v7(0x0123456789AB, RND10_A);
    ClientId b = clientid_v7(0x0123456789AB, RND10_B);
    ASSERT(clientid_eq(a, b) == false);
}

// Same random, different timestamp → different id.
TEST(v7_different_ts_yields_different_id) {
    ClientId a = clientid_v7(0x0000000000AB, RND10_A);
    ClientId b = clientid_v7(0x0000000001AB, RND10_A);
    ASSERT(clientid_eq(a, b) == false);
}

// Each byte of the random input must influence the output id — confirms the
// implementation isn't masking off any input bytes wholesale.
TEST(v7_every_random_byte_affects_output) {
    uint8_t base[10] = {0};
    ClientId base_id = clientid_v7(0x0123456789AB, base);
    for (int i = 0; i < 10; i++) {
        uint8_t mod[10] = {0};
        mod[i] = 0xFF;
        ClientId mod_id = clientid_v7(0x0123456789AB, mod);
        ASSERT(clientid_eq(base_id, mod_id) == false);
    }
}

// --- clientid_v7_now: wall-clock + OS entropy wrapper ---

TEST(v7_now_has_version_and_variant) {
    ClientId id = clientid_v7_now();
    ASSERT_EQ((id.bytes[6] >> 4) & 0x0F, 0x7);
    ASSERT_EQ((id.bytes[8] >> 6) & 0x03, 0x02);
}

// The encoded timestamp must be within a few seconds of the wall clock at
// test time.
TEST(v7_now_timestamp_is_current) {
    uint64_t before = now_ms();
    ClientId id = clientid_v7_now();
    uint64_t after = now_ms();

    uint64_t enc = decode_v7_ts_ms(id);
    // Allow a small window for scheduling slop and clock drift.
    ASSERT(enc + 5000 >= before);
    ASSERT(enc <= after + 5000);
}

// Two consecutive calls should produce distinct ids (62 bits of randomness
// makes accidental collision astronomically unlikely).
TEST(v7_now_two_calls_distinct) {
    ClientId a = clientid_v7_now();
    ClientId b = clientid_v7_now();
    ASSERT(clientid_eq(a, b) == false);
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

    RUN(v7_version_nibble_is_7);
    RUN(v7_variant_bits_are_10);
    RUN(v7_timestamp_is_big_endian);
    RUN(v7_timestamp_zero_keeps_version_and_variant);
    RUN(v7_different_random_yields_different_id);
    RUN(v7_different_ts_yields_different_id);
    RUN(v7_every_random_byte_affects_output);

    RUN(v7_now_has_version_and_variant);
    RUN(v7_now_timestamp_is_current);
    RUN(v7_now_two_calls_distinct);

    TEST_SUMMARY();
}
