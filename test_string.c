#include <stdint.h>

#include "string.h"
#include "test_util.h"

// --- strlen ---

TEST(strlen_empty) { ASSERT_EQ(strlen(""), 0); }

TEST(strlen_simple) { ASSERT_EQ(strlen("abc"), 3); }

TEST(strlen_with_spaces) { ASSERT_EQ(strlen("hello world"), 11); }

// --- strcmp (test sign, not exact magnitude) ---

TEST(strcmp_equal) { ASSERT_EQ(strcmp("abc", "abc"), 0); }

TEST(strcmp_both_empty) { ASSERT_EQ(strcmp("", ""), 0); }

TEST(strcmp_less) { ASSERT(strcmp("a", "b") < 0); }

TEST(strcmp_greater) { ASSERT(strcmp("b", "a") > 0); }

// Shorter string that is a prefix of the longer compares less.
TEST(strcmp_prefix_is_less) { ASSERT(strcmp("ab", "abc") < 0); }

TEST(strcmp_longer_is_greater) { ASSERT(strcmp("abc", "ab") > 0); }

TEST(strcmp_empty_vs_nonempty) { ASSERT(strcmp("", "a") < 0); }

// strcmp compares as unsigned char: high bytes are greater, not negative.
TEST(strcmp_high_byte_unsigned) {
    char a[] = {(char)0x80, 0};
    char b[] = {0x01, 0};
    ASSERT(strcmp(a, b) > 0);
}

// --- strcpy ---

TEST(strcpy_copies_content) {
    char dst[16];
    memset(dst, 0xAA, sizeof dst);
    strcpy(dst, "hello");
    ASSERT(memcmp(dst, "hello", 6) == 0); // includes the NUL terminator
}

TEST(strcpy_empty) {
    char dst[4];
    memset(dst, 0xAA, sizeof dst);
    strcpy(dst, "");
    ASSERT_EQ(dst[0], 0);
}

// Standard strcpy returns dst (the start), not the terminating NUL.
TEST(strcpy_returns_dst) {
    char dst[16];
    char *ret = strcpy(dst, "abcd");
    ASSERT(ret == dst);
    ASSERT(memcmp(dst, "abcd", 5) == 0);
}

// --- memset ---

TEST(memset_fills) {
    uint8_t buf[8];
    memset(buf, 0x5A, sizeof buf);
    for (size_t i = 0; i < sizeof buf; i++)
        ASSERT_EQ(buf[i], 0x5A);
}

TEST(memset_zero_fill) {
    uint8_t buf[8];
    memset(buf, 0xFF, sizeof buf);
    memset(buf, 0, sizeof buf);
    for (size_t i = 0; i < sizeof buf; i++)
        ASSERT_EQ(buf[i], 0);
}

TEST(memset_returns_ptr) {
    uint8_t buf[4];
    ASSERT(memset(buf, 1, sizeof buf) == buf);
}

// memset must touch exactly `num` bytes — neighbors stay intact.
TEST(memset_respects_length) {
    uint8_t buf[6] = {1, 1, 1, 1, 1, 1};
    memset(buf + 1, 9, 3); // bytes 1,2,3
    ASSERT_EQ(buf[0], 1);
    ASSERT_EQ(buf[1], 9);
    ASSERT_EQ(buf[2], 9);
    ASSERT_EQ(buf[3], 9);
    ASSERT_EQ(buf[4], 1);
    ASSERT_EQ(buf[5], 1);
}

TEST(memset_zero_length_noop) {
    uint8_t buf[2] = {7, 7};
    memset(buf, 0, 0);
    ASSERT_EQ(buf[0], 7);
    ASSERT_EQ(buf[1], 7);
}

// --- memcpy ---

TEST(memcpy_copies) {
    uint8_t src[5] = {1, 2, 3, 4, 5};
    uint8_t dst[5];
    memcpy(dst, src, sizeof src);
    ASSERT(memcmp(dst, src, sizeof src) == 0);
}

TEST(memcpy_returns_dst) {
    uint8_t src[3] = {1, 2, 3};
    uint8_t dst[3];
    ASSERT(memcpy(dst, src, sizeof src) == dst);
}

// Binary data with embedded zeros must copy fully (not stop at a NUL).
TEST(memcpy_handles_embedded_zeros) {
    uint8_t src[5] = {0x01, 0x00, 0x00, 0x02, 0x03};
    uint8_t dst[5];
    memset(dst, 0xFF, sizeof dst);
    memcpy(dst, src, sizeof src);
    ASSERT(memcmp(dst, src, sizeof src) == 0);
}

TEST(memcpy_zero_length_noop) {
    uint8_t dst[2] = {7, 7};
    uint8_t src[2] = {9, 9};
    memcpy(dst, src, 0);
    ASSERT_EQ(dst[0], 7);
    ASSERT_EQ(dst[1], 7);
}

// --- memcmp ---

TEST(memcmp_equal) {
    uint8_t a[4] = {1, 2, 3, 4};
    uint8_t b[4] = {1, 2, 3, 4};
    ASSERT_EQ(memcmp(a, b, 4), 0);
}

TEST(memcmp_less) {
    uint8_t a[3] = {1, 2, 3};
    uint8_t b[3] = {1, 2, 4};
    ASSERT(memcmp(a, b, 3) < 0);
}

TEST(memcmp_greater) {
    uint8_t a[3] = {1, 2, 4};
    uint8_t b[3] = {1, 2, 3};
    ASSERT(memcmp(a, b, 3) > 0);
}

// Bytes after the first difference are irrelevant.
TEST(memcmp_stops_at_first_diff) {
    uint8_t a[3] = {1, 9, 9};
    uint8_t b[3] = {2, 0, 0};
    ASSERT(memcmp(a, b, 3) < 0);
}

TEST(memcmp_zero_length_equal) {
    uint8_t a[1] = {1};
    uint8_t b[1] = {2};
    ASSERT_EQ(memcmp(a, b, 0), 0);
}

// Embedded zeros are compared as ordinary bytes.
TEST(memcmp_embedded_zeros) {
    uint8_t a[3] = {0x01, 0x00, 0x02};
    uint8_t b[3] = {0x01, 0x00, 0x03};
    ASSERT(memcmp(a, b, 3) < 0);
}

// memcmp is unsigned: 0x80 is greater than 0x01.
TEST(memcmp_unsigned) {
    uint8_t a[1] = {0x80};
    uint8_t b[1] = {0x01};
    ASSERT(memcmp(a, b, 1) > 0);
}

int main(void) {
    RUN(strlen_empty);
    RUN(strlen_simple);
    RUN(strlen_with_spaces);

    RUN(strcmp_equal);
    RUN(strcmp_both_empty);
    RUN(strcmp_less);
    RUN(strcmp_greater);
    RUN(strcmp_prefix_is_less);
    RUN(strcmp_longer_is_greater);
    RUN(strcmp_empty_vs_nonempty);
    RUN(strcmp_high_byte_unsigned);

    RUN(strcpy_copies_content);
    RUN(strcpy_empty);
    RUN(strcpy_returns_dst);

    RUN(memset_fills);
    RUN(memset_zero_fill);
    RUN(memset_returns_ptr);
    RUN(memset_respects_length);
    RUN(memset_zero_length_noop);

    RUN(memcpy_copies);
    RUN(memcpy_returns_dst);
    RUN(memcpy_handles_embedded_zeros);
    RUN(memcpy_zero_length_noop);

    RUN(memcmp_equal);
    RUN(memcmp_less);
    RUN(memcmp_greater);
    RUN(memcmp_stops_at_first_diff);
    RUN(memcmp_zero_length_equal);
    RUN(memcmp_embedded_zeros);
    RUN(memcmp_unsigned);

    TEST_SUMMARY();
}
