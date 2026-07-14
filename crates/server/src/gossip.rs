//! Anti-entropy gossip membership discovery for the horizontal-scaling cluster.
//!
//! A node need not know the whole cluster at boot: it seeds from one (or a few)
//! peer addresses and learns the rest by gossip. Each round a node picks a random
//! known peer and exchanges member sets with it — a push-pull [`Message::Gossip`]
//! carrying `(node_id, advertise_addr)` pairs. The exchange is a set union
//! ([`Membership::add_member`]): both sides come away holding every member either
//! knew, so a node that boots knowing only a seed converges on the full set
//! within a few rounds. Placement (rendezvous/HRW) is order-independent, so once
//! two nodes have learned the same members they place every room identically.
//!
//! The union is purely additive here — no member ever leaves. Failure detection
//! (suspicion, then eviction) is a later slice.
//!
//! Scope of this cut: gossip converges the *member set* and the placement view
//! every node computes from it. The per-follower replication peer-connections
//! (and the relay-link liveness signal they carry, Unit 6a) are still dialed from
//! the static boot set — wiring those to a member learned *after* boot is a
//! follow-on. So a dynamically-joined node's routing/redirect view converges here,
//! while the existing nodes' replication to it (and their liveness reading of it)
//! lands with that follow-on; a member learned by gossip is optimistically live
//! until then.

use std::time::Duration;

use crdtsync_core::protocol::PROTOCOL_VERSION;
use crdtsync_core::{decode_message, encode_header, encode_message, ClientId, Message};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::membership::Membership;
use crate::placement::NodeId;

/// How often a node initiates a gossip round. Frequent enough that a fresh joiner
/// converges within a few seconds, sparse enough that a steady-state cluster's
/// gossip traffic is negligible.
pub const GOSSIP_INTERVAL: Duration = Duration::from_secs(1);

/// How long a gossip round waits for a peer's reply before abandoning it and
/// redialing next round — a slow or dead peer must not wedge the gossip loop.
pub const GOSSIP_TIMEOUT: Duration = Duration::from_secs(5);

/// Build the [`Message::Gossip`] frame advertising `members` — the `(node_id,
/// advertise_addr)` pairs a node knows, as raw bytes for the wire.
pub fn gossip_frame(members: &[(NodeId, Vec<u8>)]) -> Message {
    Message::Gossip {
        members: members
            .iter()
            .map(|(node, addr)| (node.as_bytes().to_vec(), addr.clone()))
            .collect(),
    }
}

/// Union a gossiped member payload into `membership` — the anti-entropy merge.
/// Each `(node_id, advertise_addr)` pair is added idempotently, so a re-gossip of
/// an already-known set changes nothing and rebuilds no placement.
pub fn merge_into(membership: &mut Membership, payload: Vec<(Vec<u8>, Vec<u8>)>) {
    membership.add_members(
        payload
            .into_iter()
            .map(|(node, addr)| (NodeId::from(node), addr)),
    );
}

/// One in-process push-pull anti-entropy exchange between two memberships:
/// `initiator` sends its known members to `peer`, `peer` unions them, then `peer`
/// sends its (now-merged) set back and `initiator` unions that. Both hold the
/// union of the two sets afterward. The wire path ([`gossip_exchange`] plus the
/// server-side handler) realizes exactly this over a socket; this is its
/// deterministic, socket-free form for driving convergence in tests.
pub fn exchange(initiator: &mut Membership, peer: &mut Membership) {
    peer.add_members(initiator.known_members());
    initiator.add_members(peer.known_members());
}

/// Pick a random peer address to gossip to from `members`, excluding `self_id`.
/// `None` when the node knows no peer but itself. Randomized so, over rounds,
/// every known peer is reached and the whole set propagates; the choice draws
/// from system entropy, kept out of the deterministic membership logic.
pub fn choose_peer(members: &[(NodeId, Vec<u8>)], self_id: &NodeId) -> Option<Vec<u8>> {
    let peers: Vec<&Vec<u8>> = members
        .iter()
        .filter(|(node, _)| node != self_id)
        .map(|(_, addr)| addr)
        .collect();
    if peers.is_empty() {
        return None;
    }
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes).expect("system entropy is available");
    let idx = (u64::from_le_bytes(bytes) % peers.len() as u64) as usize;
    Some(peers[idx].clone())
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
) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
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
