#include "test_util.h"
#include "uuid.h"
#include <stdint.h>
#include <string.h>

// --- format ---

TEST(format_all_zero) {
    uint8_t b[16] = {0};
    char s[37];
    uuid_format(b, s);
    ASSERT(strcmp(s, "00000000-0000-0000-0000-000000000000") == 0);
}

TEST(format_canonical_layout) {
    uint8_t b[16] = {0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
                     0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10};
    char s[37];
    uuid_format(b, s);
    ASSERT(strcmp(s, "01234567-89ab-cdef-fedc-ba9876543210") == 0);
}

TEST(format_lowercase_hex) {
    // High-nibble bytes must format with lowercase a-f, not uppercase.
    uint8_t b[16] = {0};
    b[0] = 0xAB;
    b[15] = 0xCD;
    char s[37];
    uuid_format(b, s);
    ASSERT(s[0] == 'a' && s[1] == 'b');
    ASSERT(s[34] == 'c' && s[35] == 'd');
}

// --- parse ---

TEST(parse_round_trips_with_format) {
    uint8_t in[16];
    for (int i = 0; i < 16; i++)
        in[i] = (uint8_t)(i * 17);
    char s[37];
    uuid_format(in, s);
    uint8_t out[16];
    ASSERT(uuid_parse(s, out) == true);
    ASSERT(memcmp(in, out, 16) == 0);
}

TEST(parse_accepts_uppercase) {
    uint8_t out[16];
    ASSERT(uuid_parse("AABBCCDD-EEFF-1122-3344-556677889900", out) == true);
    ASSERT_EQ(out[0], 0xaa);
    ASSERT_EQ(out[1], 0xbb);
    ASSERT_EQ(out[15], 0x00);
}

TEST(parse_rejects_short) {
    uint8_t out[16];
    ASSERT(uuid_parse("01234567-89ab-cdef-fedc-ba98765432", out) == false);
}

TEST(parse_rejects_missing_hyphen) {
    uint8_t out[16];
    ASSERT(uuid_parse("01234567x89ab-cdef-fedc-ba9876543210", out) == false);
}

TEST(parse_rejects_non_hex) {
    uint8_t out[16];
    ASSERT(uuid_parse("0123456g-89ab-cdef-fedc-ba9876543210", out) == false);
}

// --- v5 derivation ---

TEST(v5_deterministic) {
    uint8_t ns[16] = {0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
                      0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8};
    const uint8_t name[] = "hello";
    UuidV5 a = uuid_v5(ns, name, sizeof name - 1);
    UuidV5 b = uuid_v5(ns, name, sizeof name - 1);
    ASSERT(memcmp(a.bytes, b.bytes, 16) == 0);
}

TEST(v5_sets_version_5) {
    uint8_t ns[16] = {0};
    UuidV5 out = uuid_v5(ns, (const uint8_t *)"x", 1);
    ASSERT_EQ((out.bytes[6] & 0xf0) >> 4, 5);
}

TEST(v5_sets_rfc4122_variant) {
    uint8_t ns[16] = {0};
    UuidV5 out = uuid_v5(ns, (const uint8_t *)"x", 1);
    ASSERT_EQ((out.bytes[8] & 0xc0), 0x80);
}

TEST(v5_different_names_distinct) {
    uint8_t ns[16] = {0};
    UuidV5 a = uuid_v5(ns, (const uint8_t *)"a", 1);
    UuidV5 b = uuid_v5(ns, (const uint8_t *)"b", 1);
    ASSERT(memcmp(a.bytes, b.bytes, 16) != 0);
}

TEST(v5_different_namespaces_distinct) {
    uint8_t ns1[16] = {0};
    uint8_t ns2[16] = {0};
    ns2[0] = 1;
    UuidV5 a = uuid_v5(ns1, (const uint8_t *)"x", 1);
    UuidV5 b = uuid_v5(ns2, (const uint8_t *)"x", 1);
    ASSERT(memcmp(a.bytes, b.bytes, 16) != 0);
}

// RFC 4122 Appendix B test vector: namespace = DNS namespace, name =
// "www.widgets.com" → 21f7f8de-8051-5b89-8680-0195ef798b6a.
TEST(v5_rfc_dns_vector) {
    uint8_t dns_ns[16] = {0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
                          0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8};
    UuidV5 out = uuid_v5(dns_ns, (const uint8_t *)"www.widgets.com",
                         strlen("www.widgets.com"));
    char got[37];
    uuid_format(out.bytes, got);
    ASSERT(strcmp(got, "21f7f8de-8051-5b89-8680-0195ef798b6a") == 0);
}

// Streaming v5 must produce the same result as one-shot v5 over the
// concatenated name bytes.
TEST(v5_streaming_matches_one_shot) {
    uint8_t ns[16] = {0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
                      0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8};
    const uint8_t name[] = "the-quick-brown-fox";
    size_t len = sizeof name - 1;

    UuidV5 one_shot = uuid_v5(ns, name, len);

    UuidV5Ctx ctx;
    uuid_v5_init(&ctx, ns);
    uuid_v5_update(&ctx, name, 4);
    uuid_v5_update(&ctx, name + 4, 6);
    uuid_v5_update(&ctx, name + 10, len - 10);
    UuidV5 streamed = uuid_v5_final(&ctx);

    ASSERT(memcmp(one_shot.bytes, streamed.bytes, 16) == 0);
}

// --- version / variant bit helper ---

TEST(set_version_and_variant_clears_existing) {
    uint8_t b[16];
    memset(b, 0xff, 16);
    uuid_set_version_and_variant(b, 5);
    ASSERT_EQ((b[6] & 0xf0) >> 4, 5);
    ASSERT_EQ((b[8] & 0xc0), 0x80);
    // Other bytes untouched.
    ASSERT_EQ(b[0], 0xff);
    ASSERT_EQ(b[15], 0xff);
}

TEST(set_version_and_variant_preserves_low_nibble) {
    uint8_t b[16] = {0};
    b[6] = 0x0a;
    b[8] = 0x33;
    uuid_set_version_and_variant(b, 7);
    ASSERT_EQ(b[6], 0x7a); // version=7, low nibble preserved
    // byte 8: top bits set to 10, low 6 bits preserved (0x33 & 0x3f = 0x33)
    ASSERT_EQ(b[8], (0x33 & 0x3f) | 0x80);
}

int main(void) {
    RUN(format_all_zero);
    RUN(format_canonical_layout);
    RUN(format_lowercase_hex);

    RUN(parse_round_trips_with_format);
    RUN(parse_accepts_uppercase);
    RUN(parse_rejects_short);
    RUN(parse_rejects_missing_hyphen);
    RUN(parse_rejects_non_hex);

    RUN(v5_deterministic);
    RUN(v5_sets_version_5);
    RUN(v5_sets_rfc4122_variant);
    RUN(v5_different_names_distinct);
    RUN(v5_different_namespaces_distinct);
    RUN(v5_rfc_dns_vector);
    RUN(v5_streaming_matches_one_shot);

    RUN(set_version_and_variant_clears_existing);
    RUN(set_version_and_variant_preserves_low_nibble);

    TEST_SUMMARY();
}
