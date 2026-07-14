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
//! Scope of this cut: detection + dissemination + refutation over the gossip
//! exchange. Not here: SWIM indirect probing (ping-req via k relays), a separate
//! failure-detector socket, and member *removal* from the set — all refinements.
//! The per-follower replication peer-connections are still dialed from the static
//! boot set; wiring those to a member learned *after* boot is a follow-on, but its
//! liveness now converges through gossip rather than staying optimistically live.

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
        // Read past control frames and the relay handshake for the gossip reply.
        loop {
            match read.next().await?.ok()? {
                WsMessage::Binary(bytes) => {
                    if let Ok(Message::Gossip { members }) = decode_message(&bytes) {
                        return Some(members);
                    }
                }
                WsMessage::Close(_) => return None,
                _ => continue,
            }
        }
    };
    tokio::time::timeout(GOSSIP_TIMEOUT, fut)
        .await
        .ok()
        .flatten()
}
