#include "stamp.h"

bool stamp_gt(Stamp a, Stamp b) {
    if (a.lamport > b.lamport) {
        return true;
    } else if (a.lamport < b.lamport) {
        return false;
    } else {
        // Lamports are equal, tiebreak by client_id.
        return clientid_cmp(a.client_id, b.client_id) > 0;
    }
}
