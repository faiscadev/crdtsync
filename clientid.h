#ifndef _CRDT_CLIENTID_H
#define _CRDT_CLIENTID_H

#include <stdbool.h>
#include <stdint.h>

typedef struct ClientId {
    uint8_t bytes[16];
} ClientId;

ClientId clientid_from_bytes(const uint8_t bytes[16]);

int clientid_cmp(ClientId a, ClientId b);

bool clientid_eq(ClientId a, ClientId b);

#endif // _CRDT_CLIENTID_H
