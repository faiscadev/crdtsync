#ifndef _CRDT_STAMP_H
#define _CRDT_STAMP_H

#include "clientid.h"

typedef struct Stamp {
    uint64_t lamport;
    ClientId client_id;
} Stamp;

bool stamp_gt(Stamp a, Stamp b);

#endif // _CRDT_STAMP_H
