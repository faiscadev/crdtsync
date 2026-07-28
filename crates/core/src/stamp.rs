//! Stamp — a globally unique CRDT id with a strict-greater total order.
//!
//! An op is minted at `(lamport, client)` with `offset == 0`. A text run takes
//! one id per codepoint by counting up the lamport from its base; `offset`
//! disambiguates the codepoints a run must place once the lamport reaches its
//! ceiling, so `run_member` never has to collapse two codepoints onto one id.

use crate::clientid::ClientId;

/// The highest lamport a *folded op* may raise a partition clock to.
///
/// An op's lamport is a bare `u64` off the wire, and a partition clock is a
/// running maximum over the lamports it folds — so without a bound one op parks
/// a replica's clock wherever it likes, including where the next local mint
/// leaves the space. The clamp is a constant rather than a function of local
/// state, so every replica clamps the same op to the same value and clocks stay
/// convergent.
///
/// The op itself is still applied: refusing it would trade a panic for a
/// divergence (a replica whose clock an admissible op parked at the gate mints
/// one above it, which every peer then refuses) and would buy nothing on
/// ordering, since a peer wanting to dominate LWW sits one below whatever the
/// gate is.
///
/// A clock this high is not reachable by honest means. A clock advances by one per
/// op and one per inserted codepoint, so reaching it takes 2^62 real edits — and
/// since the mint counts on from the replica's whole id-space position rather than
/// one partition's, that is 2^62 edits across the document, not within a zone.
pub const LAMPORT_WIRE_CEILING: u64 = u64::MAX >> 2;

/// The highest clock a *decoded snapshot* may declare, for the root partition
/// and for each zone. A snapshot declaring more is **refused**, never clamped.
///
/// A stored clock is its author's high-water over the ids it has published, so a
/// decode that lowered one would hand the replica node ids that are still live in
/// the state it just decoded, and a sequence drops a re-issued id as a replay —
/// the write is lost on the author and on every peer, silently. Refusing keeps
/// anything above the ceiling unreadable, so nothing above it is ever lowered,
/// and it leaves `encode_state`'s byte-stability contract intact.
///
/// It sits above [`LAMPORT_WIRE_CEILING`] on purpose, and the gap is the runway a
/// wire-clamped replica mints into: without it, one hostile op would leave a
/// replica unable to reload its own snapshot.
pub const LAMPORT_STATE_CEILING: u64 = u64::MAX >> 1;

// The runway between the two is what makes the arithmetic downstream of a clock
// total: a replica whose clock a fold parked at the wire ceiling still has 2^62
// positions to mint into before it is spent.
//
// Nothing ever enters the half **above** the state ceiling. A mint refuses past it,
// a fold clamps lower still, `apply` refuses a stamp reaching past it, and a decode
// refuses a clock or an id-space record above it — so that half is headroom the
// arithmetic never uses, not space a replica climbs into.
const _: () = assert!(LAMPORT_WIRE_CEILING < LAMPORT_STATE_CEILING);
const _: () = assert!(LAMPORT_STATE_CEILING - LAMPORT_WIRE_CEILING == 1 << 62);
const _: () = assert!(u64::MAX - LAMPORT_STATE_CEILING == 1 << 63);

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
