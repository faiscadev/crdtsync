#include "clientid.h"

ClientId clientid_from_bytes(const uint8_t bytes[16]) {
    ClientId id;
    for (int i = 0; i < 16; i++) {
        id.bytes[i] = bytes[i];
    }
    return id;
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
