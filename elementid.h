#ifndef _CRDT_ELEMENTID_H
#define _CRDT_ELEMENTID_H

// ElementId: stable identity of a composite element (Register / Counter /
// Map), shared across replicas. Stamped on the composite at create, never
// mutated afterwards.
//
// Wire format: 16-byte UUID per RFC 4122 / RFC 9562. Convergent derivation
// uses UUID v5 (SHA-1 over namespace + name). The version/variant bits are
// set per spec so the result is a valid UUID — useful for cross-language
// interop, debugging, and standard tooling.
//
// Two replicas independently calling elementid_derive with matching
// inputs land on the same UUID by construction. That's how map_merge's
// recursive guard knows two slots refer to the same logical element.
//
// Manual construction (elementid_from_bytes) is supported for imports and
// for cases where the app provides its own convergence guarantee.

#include "uuid.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct ElementId {
    UuidV5 uuid;
} ElementId;

ElementId elementid_from_bytes(const uint8_t bytes[16]);

ElementId elementid_root(void);

bool elementid_eq(ElementId a, ElementId b);

int elementid_cmp(ElementId a, ElementId b);

ElementId elementid_derive(ElementId parent, const void *key, size_t key_len,
                           uint8_t kind);

#endif // _CRDT_ELEMENTID_H
