#include "elementid.h"

ElementId elementid_from_bytes(const uint8_t bytes[16]) {
    ElementId id;
    for (int i = 0; i < 16; i++) {
        id.uuid.bytes[i] = bytes[i];
    }
    return id;
}

ElementId elementid_root(void) {
    ElementId id;
    for (int i = 0; i < 16; i++) {
        id.uuid.bytes[i] = 0;
    }
    return id;
}

bool elementid_eq(ElementId a, ElementId b) {
    for (int i = 0; i < 16; i++) {
        if (a.uuid.bytes[i] != b.uuid.bytes[i]) {
            return false;
        }
    }
    return true;
}

int elementid_cmp(ElementId a, ElementId b) {
    for (int i = 0; i < 16; i++) {
        if (a.uuid.bytes[i] < b.uuid.bytes[i]) {
            return -1;
        } else if (a.uuid.bytes[i] > b.uuid.bytes[i]) {
            return 1;
        }
    }
    return 0;
}

ElementId elementid_derive(ElementId parent, const void *key, size_t key_len,
                           uint8_t kind) {
    UuidV5Ctx ctx = {0};
    uuid_v5_init(&ctx, parent.uuid.bytes);
    uuid_v5_update(&ctx, key, key_len);
    uuid_v5_update(&ctx, &kind, sizeof(kind));

    ElementId derived = {0};
    derived.uuid = uuid_v5_final(&ctx);
    return derived;
}
