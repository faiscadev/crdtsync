#ifndef _CRDT_ELEMENTID_H
#define _CRDT_ELEMENTID_H

// ElementId: identity of a composite element (Register / Counter / Map),
// shared across replicas. Two replicas creating "the same logical element"
// must give it the same ElementId; that's the hook map_merge uses to know
// "these two slots are the same object, recurse" vs "these are different
// objects, LWW the slot".
//
// Shape: { ClientId origin, uint64 seq }. Pass by value (~24 bytes), like
// Stamp / ClientId / Scalar. Fields are public.
//
// elementid_new builds one from (origin, seq). elementid_root is a fixed
// sentinel for the top-level Map of a document; it does not collide with
// any id derived from a real ClientId. elementid_eq is the equality used
// by map_merge's recursive path; elementid_cmp gives a total order
// (origin first via clientid_cmp, then seq).

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
