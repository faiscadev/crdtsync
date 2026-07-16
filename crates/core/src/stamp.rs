//! Stamp — a globally unique CRDT id with a strict-greater total order.
//!
//! An op is minted at `(lamport, client)` with `offset == 0`. A text run takes
//! one id per codepoint by counting up the lamport from its base; `offset`
//! disambiguates the codepoints a run must place once the lamport reaches its
//! ceiling, so `run_member` never has to collapse two codepoints onto one id.

use crate::clientid::ClientId;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Stamp {
    pub lamport: u64,
    pub client: ClientId,
    /// Sub-lamport index, `0` for every op stamp and almost every node — the
    /// tiebreak that [`Stamp::run_member`] uses to keep a text run's codepoints
    /// distinct once the lamport reaches its ceiling.
    pub offset: u64,
}

impl Stamp {
    /// The id of the `k`-th codepoint (0-based) of a text run based at this
    /// stamp. Codepoints take consecutive lamports from the base; when the
    /// lamport would pass `u64::MAX` the surplus carries into `offset`, so every
    /// codepoint in a run keeps a distinct, replica-independent id even at the
    /// lamport ceiling — no silent collapse onto a saturated lamport.
    ///
    /// A legitimately minted op always bases a run at `offset == 0`, where the
    /// carry is exact for any run length. The carry saturates only past a base
    /// `offset` within a run's length of `u64::MAX` — unreachable except from a
    /// crafted op that decoded a near-ceiling offset — so a hostile stamp stays
    /// total (never panics) and convergent, at the cost of a collision no real
    /// insert can reach.
    pub fn run_member(&self, k: u64) -> Stamp {
        match self.lamport.checked_add(k) {
            Some(lamport) => Stamp {
                lamport,
                client: self.client,
                offset: self.offset,
            },
            None => {
                // `room` lamports fit below the ceiling; the rest overflow into
                // the offset. `k > room` here, so `over >= 1`.
                let room = u64::MAX - self.lamport;
                let over = k - room;
                Stamp {
                    lamport: u64::MAX,
                    client: self.client,
                    offset: self.offset.saturating_add(over),
                }
            }
        }
    }
}

impl Ord for Stamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.lamport
            .cmp(&other.lamport)
            .then_with(|| self.client.cmp(&other.client))
            .then_with(|| self.offset.cmp(&other.offset))
    }
}

impl PartialOrd for Stamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
