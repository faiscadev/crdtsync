#include "clientid.h"
#include "stamp.h"
#include "test_util.h"
#include <stdint.h>

// Helpers — keep tests readable.

static ClientId cid(uint8_t first_byte) {
    uint8_t b[16] = {0};
    b[0] = first_byte;
    return clientid_from_bytes(b);
}

static Stamp stamp(uint64_t lamport, uint8_t client_first_byte) {
    return (Stamp){.lamport = lamport, .client_id = cid(client_first_byte)};
}

// --- core LWW rule ---

TEST(gt_larger_lamport_wins) {
    Stamp a = stamp(2, 1);
    Stamp b = stamp(1, 1); // same client, smaller lamport
    ASSERT(stamp_gt(a, b) == true);
}

TEST(gt_smaller_lamport_loses) {
    Stamp a = stamp(1, 1);
    Stamp b = stamp(2, 1);
    ASSERT(stamp_gt(a, b) == false);
}

TEST(gt_equal_lamport_larger_client_id_wins) {
    Stamp a = stamp(5, 2); // larger client
    Stamp b = stamp(5, 1);
    ASSERT(stamp_gt(a, b) == true);
}

TEST(gt_equal_lamport_smaller_client_id_loses) {
    Stamp a = stamp(5, 1);
    Stamp b = stamp(5, 2);
    ASSERT(stamp_gt(a, b) == false);
}

// Lamport always dominates client_id: bigger lamport wins even if client is
// smaller.
TEST(gt_lamport_dominates_client_id) {
    Stamp a = stamp(10, 1); // big lamport, small client
    Stamp b = stamp(5, 99); // small lamport, big client
    ASSERT(stamp_gt(a, b) == true);
}

// --- order properties ---

TEST(gt_equal_stamps_returns_false) {
    Stamp a = stamp(5, 1);
    Stamp b = stamp(5, 1);
    ASSERT(stamp_gt(a, b) == false);
}

// Irreflexive: a stamp is never strictly greater than itself.
TEST(gt_irreflexive) {
    Stamp a = stamp(5, 1);
    ASSERT(stamp_gt(a, a) == false);
}

// Anti-symmetric: a > b implies !(b > a).
TEST(gt_antisymmetric_lamport) {
    Stamp a = stamp(2, 1);
    Stamp b = stamp(1, 1);
    ASSERT(stamp_gt(a, b) == true);
    ASSERT(stamp_gt(b, a) == false);
}

TEST(gt_antisymmetric_client_id) {
    Stamp a = stamp(5, 2);
    Stamp b = stamp(5, 1);
    ASSERT(stamp_gt(a, b) == true);
    ASSERT(stamp_gt(b, a) == false);
}

// Transitive: a > b and b > c implies a > c (via lamport).
TEST(gt_transitive_lamport) {
    Stamp a = stamp(10, 1);
    Stamp b = stamp(5, 1);
    Stamp c = stamp(1, 1);
    ASSERT(stamp_gt(a, b) == true);
    ASSERT(stamp_gt(b, c) == true);
    ASSERT(stamp_gt(a, c) == true);
}

// Transitive across the lamport/client boundary: a > b via lamport, b > c via
// client tiebreak, a > c must still hold.
TEST(gt_transitive_mixed) {
    Stamp a = stamp(10, 1); // beats b by lamport
    Stamp b = stamp(5, 2);  // beats c by client tiebreak
    Stamp c = stamp(5, 1);
    ASSERT(stamp_gt(a, b) == true);
    ASSERT(stamp_gt(b, c) == true);
    ASSERT(stamp_gt(a, c) == true);
}

// Trichotomy: for any two stamps, exactly one of (a > b, b > a, equal) holds.
TEST(gt_trichotomy_distinct) {
    Stamp a = stamp(5, 1);
    Stamp b = stamp(5, 2);
    bool a_gt_b = stamp_gt(a, b);
    bool b_gt_a = stamp_gt(b, a);
    // exactly one direction is true (they're not equal)
    ASSERT((a_gt_b && !b_gt_a) || (!a_gt_b && b_gt_a));
}

TEST(gt_trichotomy_equal) {
    Stamp a = stamp(5, 1);
    Stamp b = stamp(5, 1);
    // neither direction is true when equal
    ASSERT(stamp_gt(a, b) == false);
    ASSERT(stamp_gt(b, a) == false);
}

// --- client_id uses the full 16 bytes, not just the first ---

TEST(gt_client_id_tiebreak_uses_full_id) {
    uint8_t a_bytes[16] = {1, 2,  3,  4,  5,  6,  7,  8,
                           9, 10, 11, 12, 13, 14, 15, 99};
    uint8_t b_bytes[16] = {1, 2,  3,  4,  5,  6,  7,  8,
                           9, 10, 11, 12, 13, 14, 15, 1};
    Stamp a = {.lamport = 5, .client_id = clientid_from_bytes(a_bytes)};
    Stamp b = {.lamport = 5, .client_id = clientid_from_bytes(b_bytes)};
    ASSERT(stamp_gt(a, b) == true); // differs only at byte 15
}

int main(void) {
    RUN(gt_larger_lamport_wins);
    RUN(gt_smaller_lamport_loses);
    RUN(gt_equal_lamport_larger_client_id_wins);
    RUN(gt_equal_lamport_smaller_client_id_loses);
    RUN(gt_lamport_dominates_client_id);

    RUN(gt_equal_stamps_returns_false);
    RUN(gt_irreflexive);
    RUN(gt_antisymmetric_lamport);
    RUN(gt_antisymmetric_client_id);
    RUN(gt_transitive_lamport);
    RUN(gt_transitive_mixed);
    RUN(gt_trichotomy_distinct);
    RUN(gt_trichotomy_equal);

    RUN(gt_client_id_tiebreak_uses_full_id);

    TEST_SUMMARY();
}
