#include "elementid.h"

ElementId elementid_new(ClientId origin, uint64_t seq) {
    ElementId id;
    id.origin = origin;
    id.seq = seq;
    return id;
}

ElementId elementid_root(void) {
    return elementid_new(clientid_from_bytes((uint8_t[16]){0}), 0);
}

bool elementid_eq(ElementId a, ElementId b) {
    return clientid_eq(a.origin, b.origin) && a.seq == b.seq;
}

int elementid_cmp(ElementId a, ElementId b) {
    int client_cmp = clientid_cmp(a.origin, b.origin);
    if (client_cmp != 0) {
        return client_cmp;
    }

    if (a.seq < b.seq) {
        return -1;
    } else if (a.seq > b.seq) {
        return 1;
    } else {
        return 0;
    }
}
