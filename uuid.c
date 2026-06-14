#include "uuid.h"
#include "sha1.h"
#include <string.h>

void uuid_format(const uint8_t bytes[16], char out[37]) {
    static const char *hex = "0123456789abcdef";
    int j = 0;
    for (int i = 0; i < 16; i++) {
        // Hyphens go between bytes 3|4, 5|6, 7|8, 9|10 — the canonical
        // 8-4-4-4-12 grouping. Interleave during the write rather than
        // stamping them in after, which would clobber hex digits.
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            out[j++] = '-';
        }
        out[j++] = hex[(bytes[i] >> 4) & 0xF];
        out[j++] = hex[bytes[i] & 0xF];
    }
    out[36] = '\0';
}

static int hex_digit_to_int(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    } else if (c >= 'a' && c <= 'f') {
        return 10 + (c - 'a');
    } else if (c >= 'A' && c <= 'F') {
        return 10 + (c - 'A');
    } else {
        return -1; // Invalid hex digit
    }
}

bool uuid_parse(const char *s, uint8_t out[16]) {
    // Validate length and hyphens at positions 8, 13, 18, 23.
    if (strlen(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' ||
        s[23] != '-') {
        return false;
    }

    // Parse into a local buffer first so a partial-parse failure leaves
    // the caller's `out` untouched (header contract).
    uint8_t buf[16];
    for (int i = 0; i < 16; i++) {
        int hi = hex_digit_to_int(
            s[i * 2 + (i >= 4) + (i >= 6) + (i >= 8) + (i >= 10)]);
        int lo = hex_digit_to_int(
            s[i * 2 + 1 + (i >= 4) + (i >= 6) + (i >= 8) + (i >= 10)]);
        if (hi < 0 || lo < 0) {
            return false; // Invalid hex digit
        }
        buf[i] = (hi << 4) | lo;
    }
    memcpy(out, buf, 16);
    return true;
}

void uuid_v5_init(UuidV5Ctx *ctx, const uint8_t namespace_bytes[16]) {

    SHA1Init(&ctx->sha1_ctx);
    SHA1Update(&ctx->sha1_ctx, namespace_bytes, 16);
}

// SHA1Update takes a uint32_t length, so size_t inputs above UINT32_MAX
// must be chunked to avoid silent truncation on 64-bit platforms.
void uuid_v5_update(UuidV5Ctx *ctx, const uint8_t *data, size_t len) {
    while (len > 0) {
        size_t chunk = len > UINT32_MAX ? UINT32_MAX : len;
        SHA1Update(&ctx->sha1_ctx, data, (uint32_t)chunk);
        data += chunk;
        len -= chunk;
    }
}

UuidV5 uuid_v5_final(UuidV5Ctx *ctx) {
    uint8_t digest[20];
    SHA1Final(digest, &ctx->sha1_ctx);
    UuidV5 out;
    memcpy(out.bytes, digest, 16);
    // Version 5 = SHA-1 + version/variant bits per RFC 4122 §4.3.
    // Set them here so streaming callers get a valid v5 UUID without
    // having to set the bits themselves.
    uuid_set_version_and_variant(out.bytes, 5);
    return out;
}

void uuid_set_version_and_variant(uint8_t bytes[16], uint8_t version) {
    // Clear version bits (high nibble of byte 6) and set to `version`.
    bytes[6] = (bytes[6] & 0x0F) | ((version & 0x0F) << 4);
    // Clear variant bits (high two bits of byte 8) and set to RFC 4122 variant
    // (10xx).
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
}

UuidV5 uuid_v5(const uint8_t namespace_bytes[16], const uint8_t *name,
               size_t name_len) {
    UuidV5Ctx ctx;
    uuid_v5_init(&ctx, namespace_bytes);
    uuid_v5_update(&ctx, name, name_len);
    return uuid_v5_final(&ctx);
}
