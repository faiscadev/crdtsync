#include "sha1.h"
#include "test_util.h"
#include <stdint.h>
#include <string.h>

// Convert a 20-byte digest to a lowercase hex string of length 40 (+ NUL).
static void hex(const unsigned char digest[20], char out[41]) {
    static const char *h = "0123456789abcdef";
    for (int i = 0; i < 20; i++) {
        out[i * 2] = h[(digest[i] >> 4) & 0x0f];
        out[i * 2 + 1] = h[digest[i] & 0x0f];
    }
    out[40] = '\0';
}

// One-shot helper that wraps the Init/Update/Final sequence — keeps the
// test bodies compact and avoids relying on the `SHA1` one-shot whose
// signature uses char* for the digest output.
static void sha1_oneshot(const unsigned char *data, uint32_t len,
                         unsigned char out[20]) {
    SHA1_CTX ctx;
    SHA1Init(&ctx);
    SHA1Update(&ctx, data, len);
    SHA1Final(out, &ctx);
}

// --- NIST FIPS 180-4 test vectors ---

TEST(empty_string) {
    unsigned char digest[20];
    sha1_oneshot((const unsigned char *)"", 0, digest);
    char got[41];
    hex(digest, got);
    ASSERT(strcmp(got, "da39a3ee5e6b4b0d3255bfef95601890afd80709") == 0);
}

TEST(abc) {
    unsigned char digest[20];
    sha1_oneshot((const unsigned char *)"abc", 3, digest);
    char got[41];
    hex(digest, got);
    ASSERT(strcmp(got, "a9993e364706816aba3e25717850c26c9cd0d89d") == 0);
}

TEST(fips_two_block) {
    const char *msg =
        "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    unsigned char digest[20];
    sha1_oneshot((const unsigned char *)msg, (uint32_t)strlen(msg), digest);
    char got[41];
    hex(digest, got);
    ASSERT(strcmp(got, "84983e441c3bd26ebaae4aa1f95129e5e54670f1") == 0);
}

TEST(million_a) {
    // Exactly 1,000,000 'a' chars, fed via streaming update.
    SHA1_CTX ctx;
    SHA1Init(&ctx);
    unsigned char chunk[1000];
    memset(chunk, 'a', sizeof chunk);
    for (int i = 0; i < 1000; i++) {
        SHA1Update(&ctx, chunk, sizeof chunk);
    }
    unsigned char digest[20];
    SHA1Final(digest, &ctx);
    char got[41];
    hex(digest, got);
    ASSERT(strcmp(got, "34aa973cd4c4daa4f61eeb2bdbad27316534016f") == 0);
}

// Streaming-update equivalence: feeding the same bytes in chunks must
// produce the same digest as the one-shot call.
TEST(streaming_matches_one_shot) {
    const char *msg = "the quick brown fox jumps over the lazy dog";
    uint32_t len = (uint32_t)strlen(msg);

    unsigned char one_shot[20];
    sha1_oneshot((const unsigned char *)msg, len, one_shot);

    SHA1_CTX ctx;
    SHA1Init(&ctx);
    SHA1Update(&ctx, (const unsigned char *)msg, 5);
    SHA1Update(&ctx, (const unsigned char *)msg + 5, 10);
    SHA1Update(&ctx, (const unsigned char *)msg + 15, len - 15);
    unsigned char streamed[20];
    SHA1Final(streamed, &ctx);

    ASSERT(memcmp(one_shot, streamed, 20) == 0);
}

// Empty update calls must be a no-op — chunking with zero-length segments
// can happen in normal use and must not affect the digest.
TEST(empty_updates_are_noop) {
    const char *msg = "abc";
    unsigned char expected[20];
    sha1_oneshot((const unsigned char *)msg, 3, expected);

    SHA1_CTX ctx;
    SHA1Init(&ctx);
    SHA1Update(&ctx, (const unsigned char *)"", 0);
    SHA1Update(&ctx, (const unsigned char *)msg, 3);
    SHA1Update(&ctx, (const unsigned char *)"", 0);
    unsigned char got[20];
    SHA1Final(got, &ctx);

    ASSERT(memcmp(expected, got, 20) == 0);
}

int main(void) {
    RUN(empty_string);
    RUN(abc);
    RUN(fips_two_block);
    RUN(million_a);
    RUN(streaming_matches_one_shot);
    RUN(empty_updates_are_noop);

    TEST_SUMMARY();
}
