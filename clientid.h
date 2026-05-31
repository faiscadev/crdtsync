#ifndef _CRDT_CLIENTID_H
#define _CRDT_CLIENTID_H

// Per-Document-instance identifier. 16 bytes, binary-opaque.
//
// Architecture says ClientId is UUID v7 (time-sortable, RFC 9562); we keep
// the bytes raw so v7 emission can plug in later without changing the type.
// Generation (timestamp + random) is deferred until real client wiring;
// callers construct test fixtures via clientid_from_bytes for now.
//
// Ownership: pass-by-value (16 bytes, cheap). clientid_from_bytes copies the
// caller's 16-byte array into the ClientId — caller's buffer may be
// transient.
//
// Comparison: clientid_eq is memcmp == 0; clientid_cmp is the memcmp sign,
// i.e. lexicographic unsigned-byte order (0x80 > 0x01). Used as the
// tiebreak inside Stamp and as a 16-byte binary key into the hashtable.

#include <stdbool.h>
#include <stdint.h>

typedef struct ClientId {
    uint8_t bytes[16];
} ClientId;

ClientId clientid_from_bytes(const uint8_t bytes[16]);

int clientid_cmp(ClientId a, ClientId b);

bool clientid_eq(ClientId a, ClientId b);

#endif // _CRDT_CLIENTID_H
