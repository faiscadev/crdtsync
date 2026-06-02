#ifndef _CRDT_ELEMENTID_H
#define _CRDT_ELEMENTID_H

#include "clientid.h"

typedef struct ElementId {
    ClientId origin;
    uint64_t seq;
} ElementId;

ElementId elementid_new(ClientId origin, uint64_t seq);
ElementId elementid_root(void);
bool elementid_eq(ElementId a, ElementId b);
int elementid_cmp(ElementId a, ElementId b);

#endif // _CRDT_ELEMENTID_H
