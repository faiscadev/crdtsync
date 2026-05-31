#ifndef _CRDT_STAMP_H
#define _CRDT_STAMP_H

// The LWW comparison primitive: (lamport, client_id) pair.
//
// Used wherever last-writer-wins resolution is needed — Register slots,
// Map slot writes, XmlElement attr values, mark values of `kind: value`.
// Lamport carries causality; client_id is the deterministic tiebreak when
// two replicas wrote at the same lamport time.
//
// stamp_gt is strictly greater: larger lamport, or equal lamport and
// clientid_cmp(a.client_id, b.client_id) > 0. Otherwise false.
//
// Algebraic properties (relied on by LWW correctness):
//   - irreflexive    — stamp_gt(a, a) is always false
//   - anti-symmetric — stamp_gt(a, b) implies !stamp_gt(b, a)
//   - transitive     — stamp_gt(a, b) and stamp_gt(b, c) implies stamp_gt(a, c)
//   - trichotomous   — for any a, b: exactly one of (a > b, b > a, equal)
//
// Ownership: pass-by-value. No allocation, no arena.

#include "clientid.h"
#include <stdbool.h>
#include <stdint.h>

typedef struct Stamp {
    uint64_t lamport;
    ClientId client_id;
} Stamp;

bool stamp_gt(Stamp a, Stamp b);

#endif // _CRDT_STAMP_H
