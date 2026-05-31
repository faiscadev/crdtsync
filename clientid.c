#include "clientid.h"
#include "host.h"

ClientId clientid_from_bytes(const uint8_t bytes[16]) {
    ClientId id;
    for (int i = 0; i < 16; i++) {
        id.bytes[i] = bytes[i];
    }
    return id;
}

ClientId clientid_v7(uint64_t ts_ms, const uint8_t rand[10]) {
    ClientId id = {0};

    // Bytes 0..5: 48-bit unix-ms timestamp, big-endian.
    id.bytes[0] = (uint8_t)(ts_ms >> 40);
    id.bytes[1] = (uint8_t)(ts_ms >> 32);
    id.bytes[2] = (uint8_t)(ts_ms >> 24);
    id.bytes[3] = (uint8_t)(ts_ms >> 16);
    id.bytes[4] = (uint8_t)(ts_ms >> 8);
    id.bytes[5] = (uint8_t)(ts_ms);

    // Byte 6: version (0x7) in upper nibble, top 4 bits of rand_a in lower
    // nibble.
    id.bytes[6] = 0x70 | (rand[0] & 0x0F);

    // Byte 7: low 8 bits of rand_a.
    id.bytes[7] = rand[1];

    // Byte 8: variant (0b10) in upper 2 bits, top 6 bits of rand_b in lower 6.
    id.bytes[8] = 0x80 | (rand[2] & 0x3F);

    // Bytes 9..15: low 56 bits of rand_b.
    id.bytes[9] = rand[3];
    id.bytes[10] = rand[4];
    id.bytes[11] = rand[5];
    id.bytes[12] = rand[6];
    id.bytes[13] = rand[7];
    id.bytes[14] = rand[8];
    id.bytes[15] = rand[9];

    return id;
}

ClientId clientid_v7_now(void) {
    uint8_t rand[10];
    host_fill_entropy(rand, sizeof(rand));
    return clientid_v7(host_now_ms(), rand);
}

int clientid_cmp(ClientId a, ClientId b) {
    for (int i = 0; i < 16; i++) {
        if (a.bytes[i] < b.bytes[i]) {
            return -1;
        } else if (a.bytes[i] > b.bytes[i]) {
            return 1;
        }
    }
    return 0;
}

bool clientid_eq(ClientId a, ClientId b) { return clientid_cmp(a, b) == 0; }
