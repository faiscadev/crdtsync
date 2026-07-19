//! Anti-entropy gossip membership discovery and failure detection for the
//! horizontal-scaling cluster.
//!
//! A node need not know the whole cluster at boot: it seeds from one (or a few)
//! peer addresses and learns the rest by gossip. Each round a node picks a random
//! known peer and exchanges liveness with it — a push-pull [`Message::Gossip`]
//! carrying `(node_id, advertise_addr, incarnation, state)` tuples. The exchange is
//! a SWIM anti-entropy merge ([`Membership::merge_liveness`]): both sides come away
//! holding every member either knew and the more-authoritative liveness for each,
//! so a node that boots knowing only a seed converges on the full set within a few
//! rounds. Placement (rendezvous/HRW) is order-independent, so once two nodes have
//! learned the same members they place every room identically.
//!
//! Failure detection rides the same exchange (no separate detector socket): a
//! *successful* round with a peer is first-hand proof it is alive
//! ([`Membership::note_gossip_reachable`]); a *failed* round counts toward
//! suspicion ([`Membership::note_gossip_unreachable`]), escalating the peer
//! `Alive → Suspect → Dead` once enough rounds miss. The verdict rides every gossip
//! frame, so a `Dead` propagates cluster-wide; a node falsely suspected refutes by
//! bumping its incarnation, and the correction wins everywhere. A `Dead` member is
//! excluded from room leadership through [`Membership::is_live`]; it stays in the
//! member set (permanent eviction/reaping is a follow-on).
//!
//! SWIM indirect probing hardens the detector against a single bad link: when a
//! node's *direct* gossip probe to a member fails, it asks up to
//! [`INDIRECT_PROBE_COUNT`] other members for a second opinion on that member
//! ([`Message::PingReq`]/[`Message::PingAck`]) before counting the failure. Each
//! relay answers from its own liveness view (its gossip state + relay link for the
//! target) — an independent vantage the failing direct path does not share, kept
//! fresh by the relay's own ~1s gossip cadence. A target any relay still reaches
//! ([`probe_outcome`]) is credited alive and its suspicion clock reset, so a
//! transient or asymmetric blip on one node's path does not falsely escalate it;
//! only a member every relay *also* reports unreachable counts toward
//! `Suspect`/`Dead`. The relay answers synchronously from the registry actor (it
//! never dials on a requester's behalf), so a ping-req is neither a task-spawn nor
//! an outbound-dial amplification surface.
//!
//! Scope of this cut: detection (direct + indirect) + dissemination + refutation
//! over the gossip exchange. Not here: a separate failure-detector socket, and
//! member *removal* from the set — refinements. The per-follower replication
//! peer-connections are still dialed from the static boot set; wiring those to a
//! member learned *after* boot is a follow-on, but its liveness now converges
//! through gossip rather than staying optimistically live.

use std::time::Duration;

use crdtsync_core::protocol::PROTOCOL_VERSION;
use crdtsync_core::{
    decode_message, encode_header, encode_message, ClientId, MemberState, Message,
};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::membership::Membership;
use crate::placement::NodeId;

/// One member's advertisement as the membership layer holds it: its [`NodeId`],
/// dial address, incarnation, and liveness state.
pub type GossipMember = (NodeId, Vec<u8>, u64, MemberState);

/// One member's advertisement as it rides the wire — the [`NodeId`] is raw bytes,
/// matching [`Message::Gossip`]'s payload. The receiver reconstitutes the id.
pub type GossipWireMember = (Vec<u8>, Vec<u8>, u64, MemberState);

/// How often a node initiates a gossip round. Frequent enough that a fresh joiner
/// converges within a few seconds, sparse enough that a steady-state cluster's
/// gossip traffic is negligible.
pub const GOSSIP_INTERVAL: Duration = Duration::from_secs(1);

/// How long a gossip round waits for a peer's reply before abandoning it and
/// redialing next round — a slow or dead peer must not wedge the gossip loop.
pub const GOSSIP_TIMEOUT: Duration = Duration::from_secs(5);

/// How many other members a node asks to probe a target on its behalf when its own
/// direct probe fails — the SWIM ping-req fan-out. A handful is enough that a
/// target reachable through *any* live path is very likely confirmed, few enough
/// that the indirect round stays cheap.
pub const INDIRECT_PROBE_COUNT: usize = 3;

/// How long an indirect probe waits for a relay's [`Message::PingAck`] before
/// abandoning it — a relay that is itself slow or down must not wedge the round.
pub const PING_REQ_TIMEOUT: Duration = Duration::from_secs(5);

/// Build the [`Message::Gossip`] frame advertising `members` — the liveness tuples
/// a node knows, as raw bytes for the wire.
pub fn gossip_frame(members: &[GossipMember]) -> Message {
    Message::Gossip {
        members: members
            .iter()
            .map(|(node, addr, incarnation, state)| {
                (node.as_bytes().to_vec(), addr.clone(), *incarnation, *state)
            })
            .collect(),
    }
}

/// Merge a gossiped liveness payload (as it arrives off the wire) into
/// `membership` — the SWIM anti-entropy merge. Higher incarnation wins, equal
/// incarnation the more-suspicious state wins, and a stale suspicion of self is
/// refuted; see [`Membership::merge_liveness`].
pub fn merge_into(membership: &mut Membership, payload: Vec<(Vec<u8>, Vec<u8>, u64, MemberState)>) {
    membership.merge_liveness(
        payload
            .into_iter()
            .map(|(node, addr, incarnation, state)| (NodeId::from(node), addr, incarnation, state)),
    );
}

/// One in-process push-pull anti-entropy exchange between two memberships:
/// `initiator` sends its liveness to `peer`, `peer` merges it, then `peer` sends
/// its (now-merged) view back and `initiator` merges that. Both hold the merged
/// liveness afterward. **Only the `initiator` credits the peer reachable** — it is
/// the side that directly probed. This mirrors the wire path faithfully: the dialed
/// node ([`Registry::apply_gossip`]) merges the frame but cannot identify the dialer
/// to credit it, so a node learns a peer alive on its *own* next initiated round,
/// not from being dialed. (Crediting an inbound sender — and so tolerating a one-way
/// partition where A can reach B but not vice-versa — needs sender identification or
/// SWIM indirect probing, a deferred refinement.)
pub fn exchange(initiator: &mut Membership, peer: &mut Membership) {
    let peer_id = peer.self_id().clone();
    peer.merge_liveness(initiator.known_liveness());
    initiator.merge_liveness(peer.known_liveness());
    initiator.note_gossip_reachable(&peer_id);
}

/// Pick a random peer to gossip to from `members`, excluding `self_id`, returning
/// its `(node_id, advertise_addr)` — the id so the round's success or failure feeds
/// the failure detector, the address to dial. `None` when the node knows no peer
/// but itself. Randomized so, over rounds, every known peer is reached; the choice
/// draws from system entropy, kept out of the deterministic membership logic.
pub fn choose_peer(members: &[GossipMember], self_id: &NodeId) -> Option<(NodeId, Vec<u8>)> {
    let peers: Vec<(&NodeId, &Vec<u8>)> = members
        .iter()
        .filter(|(node, ..)| node != self_id)
        .map(|(node, addr, ..)| (node, addr))
        .collect();
    if peers.is_empty() {
        return None;
    }
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes).expect("system entropy is available");
    let idx = (u64::from_le_bytes(bytes) % peers.len() as u64) as usize;
    let (node, addr) = peers[idx];
    Some((node.clone(), addr.clone()))
}

/// Pick up to `k` members to ask to probe `target` on this node's behalf — the
/// SWIM ping-req relays — excluding `self_id`, the `target` itself, and any member
/// gossip already holds `Dead` (asking a dead relay wastes a probe). Returns each
/// chosen relay's `(node_id, advertise_addr)`. The choice is randomized (drawn from
/// system entropy, kept out of the deterministic membership logic) and without
/// replacement, so distinct relays are asked; fewer than `k` are returned when
/// fewer are eligible.
pub fn choose_relays(
    members: &[GossipMember],
    self_id: &NodeId,
    target: &NodeId,
    k: usize,
) -> Vec<(NodeId, Vec<u8>)> {
    let mut eligible: Vec<(NodeId, Vec<u8>)> = members
        .iter()
        .filter(|(node, _, _, state)| {
            node != self_id && node != target && *state != MemberState::Dead
        })
        .map(|(node, addr, ..)| (node.clone(), addr.clone()))
        .collect();
    let take = k.min(eligible.len());
    // Partial Fisher-Yates: draw `take` distinct relays by swapping a random
    // remaining candidate into each front slot, so the prefix is a uniform sample
    // without replacement.
    for i in 0..take {
        let span = (eligible.len() - i) as u64;
        let mut bytes = [0u8; 8];
        getrandom::getrandom(&mut bytes).expect("system entropy is available");
        let j = i + (u64::from_le_bytes(bytes) % span) as usize;
        eligible.swap(i, j);
    }
    eligible.truncate(take);
    eligible
}

/// Whether an indirect probe round confirmed the target reachable: any relay that
/// answered `Some(true)`. A relay this node could not reach (`None`) or that
/// reported the target unreachable (`Some(false)`) is no evidence of life.
pub fn indirect_reachable(results: &[Option<bool>]) -> bool {
    results.iter().any(|r| *r == Some(true))
}

/// The reachability verdict for one SWIM probe round, folding the direct probe and
/// the indirect (ping-req) fallback: reachable iff the direct probe succeeded, or
/// any relay confirmed the target. A node feeds this to the failure detector —
/// [`Membership::note_gossip_reachable`] when `true`, `note_gossip_unreachable`
/// when `false` — so a direct-probe failure escalates suspicion only after every
/// indirect probe *also* fails.
pub fn probe_outcome(direct_reachable: bool, indirect: &[Option<bool>]) -> bool {
    direct_reachable || indirect_reachable(indirect)
}

/// Ask the relay at `relay_addr` to report whether it can reach `target_addr` on
/// this node's behalf: dial it, send a [`Message::PingReq`], and read back the
/// [`Message::PingAck`]'s verdict. `Some(reachable)` is the relay's answer — its
/// own liveness view of the target; `None` means the relay itself was unreachable
/// (or replied nothing within [`PING_REQ_TIMEOUT`]), which is no evidence either
/// way and the round simply consults the next relay.
pub async fn ping_req_exchange(
    relay_addr: &str,
    server: ClientId,
    target_addr: &[u8],
) -> Option<bool> {
    let frame = Message::PingReq {
        target: target_addr.to_vec(),
    };
    relay_roundtrip(relay_addr, server, frame, PING_REQ_TIMEOUT, |m| match m {
        Message::PingAck { reachable } => Some(reachable),
        _ => None,
    })
    .await
    .flatten()
}

/// Dial `addr` as a relay peer, push this node's `frame`, and pull the peer's
/// [`Message::Gossip`] reply — the members it knows back. `None` on a dial,
/// handshake, or send failure, or if the peer sends no gossip within
/// [`GOSSIP_TIMEOUT`]; the round is simply retried next tick. The dialed node's
/// server-side handler merges what it receives and answers with its own set, so
/// one exchange syncs both directions.
pub async fn gossip_exchange(
    addr: &str,
    server: ClientId,
    frame: Message,
) -> Option<Vec<(Vec<u8>, Vec<u8>, u64, MemberState)>> {
    relay_roundtrip(addr, server, frame, GOSSIP_TIMEOUT, |m| match m {
        Message::Gossip { members } => Some(members),
        _ => None,
    })
    .await
    .flatten()
}

/// Open an ephemeral relay connection to `addr`, push one `frame`, and pull the
/// peer's reply, returning the first inbound message `extract` accepts. Shared by
/// the node-to-node exchanges (gossip anti-entropy and ping-req): the 8-byte
/// header, the empty-`app_id` Hello that resolves to a relay so the peer accepts
/// the node frame that follows, the send, then the read loop that skips control
/// frames until `extract` yields. `None` on any dial/handshake/send failure, on a
/// close before a match, or if nothing matches within `timeout`. The outer `Option`
/// (from the timeout) and the inner (from the read) both collapse to "no reply", so
/// callers `.flatten()`.
async fn relay_roundtrip<T>(
    addr: &str,
    server: ClientId,
    frame: Message,
    timeout: Duration,
    extract: impl Fn(Message) -> Option<T>,
) -> Option<Option<T>> {
    let fut = async {
        let url = format!("ws://{addr}/");
        let (ws, _) = connect_async(&url).await.ok()?;
        let (mut write, mut read) = ws.split();
        write
            .send(WsMessage::Binary(encode_header(PROTOCOL_VERSION).to_vec()))
            .await
            .ok()?;
        // An empty-`app_id` Hello resolves to a relay, so the peer accepts the
        // node-to-node frame that follows.
        let hello = Message::Hello {
            client: server,
            app_id: Vec::new(),
            schema_version: 0,
        };
        write
            .send(WsMessage::Binary(encode_message(&hello)))
            .await
            .ok()?;
        write
            .send(WsMessage::Binary(encode_message(&frame)))
            .await
            .ok()?;
        loop {
            match read.next().await?.ok()? {
                WsMessage::Binary(bytes) => {
                    if let Ok(msg) = decode_message(&bytes) {
                        if let Some(value) = extract(msg) {
                            return Some(value);
                        }
                    }
                }
                WsMessage::Close(_) => return None,
                _ => continue,
            }
        }
    };
    tokio::time::timeout(timeout, fut).await.ok()
}
