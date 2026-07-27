//! C12 — a node dials its own peers over the transport each member advertises.
//!
//! Both outbound node-to-node dials once hard-coded `ws://`, and the server had no
//! client-side TLS config at all, so a node that terminated TLS bound a listener
//! its own peers could not speak to: replication, gossip anti-entropy and the SWIM
//! ping-req all stopped, and the node simply never converged. The confidentiality
//! half is worse than the availability half — the cluster secret is a *bearer*
//! credential, so the peer link is exactly the one that must not be plaintext.
//!
//! The transport is per member: an advertise address carries its scheme
//! (`wss://host:port` terminates TLS, `ws://host:port` or a bare `host:port` does
//! not), so a cluster mid-rollout may hold both and every node still dials every
//! other correctly. What a deployment cannot do is configure a node whose
//! advertised transport disagrees with the one it terminates — that is the silent
//! non-convergence, and it is now a startup error.
//!
//! Server-authenticated TLS on the dial also closes the harvest C10 left open: the
//! acceptor proves possession of a key for a cert chaining to the cluster's trust
//! anchors *before* the dialer writes a byte of the secret, so a squatter answering
//! a member's advertise address gets a failed handshake instead of the credential.
//!
//! Excluded under Miri, which cannot run tokio's real I/O; the pure
//! endpoint/policy half is unit-tested in `dial.rs` and does run there.
#![cfg(not(miri))]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crdtsync_core::protocol::{Channel, PROTOCOL_VERSION};
use crdtsync_core::{
    decode_message, encode_header, encode_message, ClientId, Document, MemberState, Message, Scalar,
};
use crdtsync_server::dial::{PeerDialer, PEER_DIAL_TIMEOUT};
use crdtsync_server::gossip::{gossip_exchange, gossip_frame, ping_req_exchange};
use crdtsync_server::membership::Membership;
use crdtsync_server::placement::NodeId;
use crdtsync_server::runtime::{serve_with, ServeConfig};
use crdtsync_server::{
    client_config_from_pem, client_config_from_pem_with_identity, server_config_from_pem,
    server_config_from_pem_with_client_ca, TlsConfigError,
};

const CH: Channel = Channel(0);

/// How long a positive convergence assertion waits. Generous rather than tight:
/// these tests stand up real nodes that dial, complete a TLS handshake, and
/// replicate, and a loaded machine running the whole suite in parallel makes a
/// short bound measure the machine rather than the code. A *negative* assertion
/// never leans on this — each has its own bound inside the code under test.
const CONVERGE: Duration = Duration::from_secs(60);

/// The deployment's cluster secret — what every node in one cluster holds and
/// nobody else does.
const SECRET: &[u8] = b"cluster-secret-of-at-least-32-bytes";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

fn doc(first: u8) -> Document {
    Document::new(cid(first))
}

// --- a test certificate authority ---

/// A throwaway CA plus the leaf certs it issues, all on disk under one temp
/// directory that is removed when the guard drops. The cluster's nodes chain to
/// it, which is what lets a dialer distinguish a member from anything else
/// answering the member's advertise address.
struct Ca {
    dir: PathBuf,
    ca_path: PathBuf,
    cert: rcgen::Certificate,
    key: rcgen::KeyPair,
    issued: std::sync::atomic::AtomicU64,
}

impl Drop for Ca {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// An issued leaf: the PEM cert chain and private key on disk.
struct Leaf {
    cert_path: PathBuf,
    key_path: PathBuf,
}

fn temp_dir(tag: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("crdtsync-peertls-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

impl Ca {
    /// Generate a fresh CA and write its cert to a PEM trust bundle on disk.
    fn new(tag: &str) -> Self {
        let dir = temp_dir(tag);
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "crdtsync-test-ca");
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        let ca_path = dir.join("ca.pem");
        std::fs::write(&ca_path, cert.pem()).unwrap();
        Self {
            dir,
            ca_path,
            cert,
            key,
            issued: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Issue a leaf cert good for both ends of a peer handshake — `serverAuth` for
    /// the listener, `clientAuth` so the same identity can be presented on a dial —
    /// naming `127.0.0.1` so a loopback `wss://127.0.0.1:port` verifies.
    fn issue(&self, name: &str) -> Leaf {
        self.issue_for(name, "127.0.0.1")
    }

    /// Issue a leaf cert as [`issue`](Self::issue) does, naming `san` instead — so
    /// a test can present a cert this authority really signed for an address that
    /// is not the one being dialed.
    fn issue_for(&self, name: &str, san: &str) -> Leaf {
        let n = self
            .issued
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut params = rcgen::CertificateParams::new(vec![san.to_string()]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.use_authority_key_identifier_extension = true;
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.cert, &self.key).unwrap();
        let cert_path = self.dir.join(format!("{name}-{n}.pem"));
        let key_path = self.dir.join(format!("{name}-{n}.key"));
        // The chain the listener presents: leaf then issuer, so a dialer trusting
        // only the CA can build the path.
        std::fs::write(&cert_path, format!("{}{}", cert.pem(), self.cert.pem())).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        Leaf {
            cert_path,
            key_path,
        }
    }
}

// --- cluster fixtures ---

/// A two-member cluster, self chosen by `me`, at replication factor 2 — a room's
/// replica set is the primary plus one follower.
fn two_node_membership(me: &str, other: &str) -> Membership {
    Membership::from_static_config(Some(me), None, other, 2).unwrap()
}

fn clustered(me: &str, other: &str) -> ServeConfig {
    ServeConfig {
        membership: Some(two_node_membership(me, other)),
        cluster_secret: Some(SECRET.to_vec()),
        ..ServeConfig::default()
    }
}

/// The first room this two-member cluster places on `leader_id` — so a write there
/// is served locally and replicated to the other member, rather than redirected.
fn room_led_by(leader_id: &str, follower_id: &str) -> Vec<u8> {
    let m = two_node_membership(leader_id, follower_id);
    let leader = NodeId::from(leader_id);
    (0..1_000_000)
        .map(|i| format!("room-{i}").into_bytes())
        .find(|room| m.primary_for(room) == Some(leader.clone()))
        .expect("a room the leader leads")
}

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn send_frame(ws: &mut Ws, msg: &Message) {
    ws.send(WsMessage::Binary(encode_message(msg)))
        .await
        .unwrap();
}

/// Open a client connection to a *plaintext* listener as `client`, draining the
/// AuthOk. The peer link is what this suite is about; the client end stays plain
/// so a test's own traffic never depends on the transport under test.
async fn open_client(addr: &str, client: ClientId) -> Ws {
    let (mut ws, _) = connect_async(format!("ws://{addr}/")).await.unwrap();
    ws.send(WsMessage::Binary(encode_header(PROTOCOL_VERSION).to_vec()))
        .await
        .unwrap();
    send_frame(
        &mut ws,
        &Message::Hello {
            client,
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
        },
    )
    .await;
    send_frame(
        &mut ws,
        &Message::Auth {
            credential: b"cred".to_vec(),
        },
    )
    .await;
    loop {
        if let WsMessage::Binary(b) = ws.next().await.unwrap().unwrap() {
            if matches!(decode_message(&b), Ok(Message::AuthOk { .. })) {
                break;
            }
        }
    }
    ws
}

/// The next message on `ws` matching `want` within `within`, or `None` — the
/// bounded poll a negative assertion needs so it never hangs.
async fn next_matching(
    ws: &mut Ws,
    within: Duration,
    want: impl Fn(&Message) -> bool,
) -> Option<Message> {
    tokio::time::timeout(within, async {
        loop {
            match ws.next().await {
                Some(Ok(WsMessage::Binary(b))) => match decode_message(&b) {
                    Ok(msg) if want(&msg) => return Some(msg),
                    _ => continue,
                },
                Some(Ok(_)) => continue,
                _ => return None,
            }
        }
    })
    .await
    .ok()
    .flatten()
}

/// Retry `round` until it yields, or [`CONVERGE`] elapses — the gossip loop retries
/// a round every tick in exactly this way, so a positive assertion about an
/// exchange measures whether it can happen at all rather than whether it happened
/// inside one attempt's own timeout on a loaded machine. A *negative* assertion
/// never uses this: one attempt failing is the whole of what it asserts.
async fn retrying<T, F: std::future::Future<Output = Option<T>>>(
    round: impl Fn() -> F,
) -> Option<T> {
    let deadline = std::time::Instant::now() + CONVERGE;
    loop {
        if let Some(value) = round().await {
            return Some(value);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        // A round that fails fast (a dial refused before the node's listener is up)
        // must not spin: the gossip loop paces its rounds, and so does this.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Subscribe `ws` to `room` and return the reply that settled it.
async fn subscribe(ws: &mut Ws, room: &[u8]) -> Option<Message> {
    send_frame(
        ws,
        &Message::Subscribe {
            channel: CH,
            room: room.to_vec(),
            branch: Vec::new(),
            zone: Vec::new(),
            last_seen_seq: 0,
        },
    )
    .await;
    next_matching(ws, CONVERGE, |m| {
        matches!(
            m,
            Message::Ops { .. } | Message::Snapshot { .. } | Message::Redirect { .. }
        )
    })
    .await
}

/// Write one op through `writer` and report whether the leader released its
/// `Accepted` — which it withholds until a majority (here: the one follower) holds
/// the write, so the ack arriving is itself proof the peer link carried it.
async fn write_reaches_the_follower(writer: &mut Ws, room: &[u8]) -> bool {
    let served = subscribe(writer, room).await;
    assert!(
        matches!(served, Some(Message::Ops { .. } | Message::Snapshot { .. })),
        "the leader did not serve the room it leads, got {served:?}",
    );
    send_frame(
        writer,
        &Message::Ops {
            channel: CH,
            ops: doc(1).transact(|tx| tx.register(b"k", Scalar::Int(1))),
        },
    )
    .await;
    next_matching(writer, CONVERGE, |m| matches!(m, Message::Accepted { .. }))
        .await
        .is_some()
}

// --- the whole peer plane over TLS ---

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
async fn a_tls_terminated_cluster_replicates_to_its_own_peers() {
    // The unit's headline: two nodes that both terminate TLS, each advertising
    // `wss://`, dial each other and replicate end to end. Before the fix both dials
    // were hard-coded `ws://` against a TLS listener, so this never converged.
    let ca = Ca::new("replicate");
    let leader_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let follower_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let leader_addr = format!("wss://{}", leader_listener.local_addr().unwrap());
    let follower_addr = format!("wss://{}", follower_listener.local_addr().unwrap());
    let room = room_led_by(&leader_addr, &follower_addr);

    let leader_leaf = ca.issue("leader");
    let follower_leaf = ca.issue("follower");
    let peer_tls = Some(client_config_from_pem(&ca.ca_path).unwrap());

    let follower = tokio::spawn(serve_with(
        follower_listener,
        cid(0xF0),
        None,
        ServeConfig {
            tls: Some(
                server_config_from_pem(&follower_leaf.cert_path, &follower_leaf.key_path).unwrap(),
            ),
            peer_tls: peer_tls.clone(),
            ..clustered(&follower_addr, &leader_addr)
        },
    ));
    let leader = tokio::spawn(serve_with(
        leader_listener,
        cid(0xFF),
        None,
        ServeConfig {
            tls: Some(
                server_config_from_pem(&leader_leaf.cert_path, &leader_leaf.key_path).unwrap(),
            ),
            peer_tls,
            ..clustered(&leader_addr, &follower_addr)
        },
    ));

    // Both listeners speak TLS, so the client end dials TLS too — the point being
    // that the *peer* link converged, which the released Accepted proves.
    let mut writer = open_tls_client(&leader_addr, &ca, cid(1)).await;
    assert!(
        write_reaches_the_follower(&mut writer, &room).await,
        "the leader never released the write's Accepted — the peer link never came up",
    );

    leader.abort();
    follower.abort();
}

/// Open a client connection to a `wss://` listener, trusting `ca`.
async fn open_tls_client(addr: &str, ca: &Ca, client: ClientId) -> Ws {
    let config = client_config_from_pem(&ca.ca_path).unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async_tls_with_config(
        format!("{addr}/"),
        None,
        false,
        Some(tokio_tungstenite::Connector::Rustls(config)),
    )
    .await
    .unwrap();
    ws.send(WsMessage::Binary(encode_header(PROTOCOL_VERSION).to_vec()))
        .await
        .unwrap();
    send_frame(
        &mut ws,
        &Message::Hello {
            client,
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
        },
    )
    .await;
    send_frame(
        &mut ws,
        &Message::Auth {
            credential: b"cred".to_vec(),
        },
    )
    .await;
    loop {
        if let WsMessage::Binary(b) = ws.next().await.unwrap().unwrap() {
            if matches!(decode_message(&b), Ok(Message::AuthOk { .. })) {
                break;
            }
        }
    }
    ws
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
async fn anti_entropy_and_ping_req_round_trip_over_tls() {
    // The other two node-to-node exchanges share one dial helper: the gossip
    // anti-entropy push-pull and the SWIM indirect probe. Both are answered only on
    // an admitted peer link, so a reply arriving is proof the TLS dial completed
    // *and* carried the cluster secret.
    let ca = Ca::new("gossip");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("wss://{}", listener.local_addr().unwrap());
    let leaf = ca.issue("node");
    let node = tokio::spawn(serve_with(
        listener,
        cid(0xFF),
        None,
        ServeConfig {
            tls: Some(server_config_from_pem(&leaf.cert_path, &leaf.key_path).unwrap()),
            peer_tls: Some(client_config_from_pem(&ca.ca_path).unwrap()),
            ..clustered(&addr, "10.0.0.9:9000")
        },
    ));

    let dialer = PeerDialer::new(
        Arc::from(SECRET),
        Some(client_config_from_pem(&ca.ca_path).unwrap()),
        false,
    );
    let members = retrying(|| {
        gossip_exchange(
            &addr,
            cid(0),
            &dialer,
            gossip_frame(&[(
                NodeId::from("10.0.0.9:9000"),
                b"10.0.0.9:9000".to_vec(),
                1,
                MemberState::Alive,
            )]),
        )
    })
    .await;
    assert!(
        members.is_some_and(|m| !m.is_empty()),
        "the anti-entropy exchange did not complete over TLS",
    );

    let verdict = retrying(|| ping_req_exchange(&addr, cid(0), &dialer, b"10.0.0.9:9000")).await;
    assert!(
        verdict.is_some(),
        "the ping-req did not round-trip over TLS",
    );

    node.abort();
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
async fn a_plaintext_dial_to_a_tls_listener_reaches_nothing() {
    // The failure the unit exists to remove, pinned as the counterfactual: a dialer
    // with no client TLS config, against a member that terminates TLS, gets no
    // answer at all — which is exactly how the whole peer plane behaved before.
    let ca = Ca::new("plaintext-dial");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tls_addr = listener.local_addr().unwrap().to_string();
    let leaf = ca.issue("node");
    let node = tokio::spawn(serve_with(
        listener,
        cid(0xFF),
        None,
        ServeConfig {
            tls: Some(server_config_from_pem(&leaf.cert_path, &leaf.key_path).unwrap()),
            ..ServeConfig::default()
        },
    ));

    let plain = PeerDialer::new(Arc::from(SECRET), None, false);
    assert!(
        gossip_exchange(&tls_addr, cid(0), &plain, gossip_frame(&[]))
            .await
            .is_none(),
        "a plaintext dial got an answer out of a TLS listener",
    );

    node.abort();
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
async fn a_squatter_at_a_members_address_never_receives_the_cluster_secret() {
    // C10 left admission one-directional: the dialer proved itself and the acceptor
    // proved nothing, so a node wrote the bearer secret to whatever answered a
    // member's advertise address. Verifying the acceptor's cert first inverts that —
    // the handshake fails before any frame is written, so the impostor collects a
    // ClientHello and nothing else.
    let ca = Ca::new("squatter");
    let squatter = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("wss://{}", squatter.local_addr().unwrap());

    let captured = tokio::spawn(async move {
        let (mut sock, _) = squatter.accept().await.unwrap();
        let mut seen = Vec::new();
        // Read until the dialer gives up on the handshake; a cap keeps a
        // misbehaving future from growing this without bound.
        let _ = tokio::time::timeout(Duration::from_secs(3), async {
            let mut buf = [0u8; 4096];
            while seen.len() < 64 * 1024 {
                match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => seen.extend_from_slice(&buf[..n]),
                }
            }
        })
        .await;
        seen
    });

    let dialer = PeerDialer::new(
        Arc::from(SECRET),
        Some(client_config_from_pem(&ca.ca_path).unwrap()),
        false,
    );
    assert!(
        gossip_exchange(&addr, cid(0), &dialer, gossip_frame(&[]))
            .await
            .is_none(),
        "the dialer completed an exchange with an impostor",
    );

    let seen = captured.await.unwrap();
    assert!(
        !seen.windows(SECRET.len()).any(|w| w == SECRET),
        "the cluster secret was written to a squatter at a member's address",
    );
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
async fn a_certificate_from_another_authority_is_not_a_member() {
    // The squatter with a certificate of its own: a real TLS listener, a real
    // handshake, a cert that simply chains to an authority this cluster does not
    // trust. Nothing about presenting *a* certificate makes a far end a member, so
    // the dial fails and the secret is not written.
    let cluster_ca = Ca::new("trusted");
    let other_ca = Ca::new("untrusted");
    let leaf = other_ca.issue("impostor");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("wss://{}", listener.local_addr().unwrap());
    let impostor = tokio::spawn(serve_with(
        listener,
        cid(0xFF),
        None,
        ServeConfig {
            tls: Some(server_config_from_pem(&leaf.cert_path, &leaf.key_path).unwrap()),
            ..ServeConfig::default()
        },
    ));

    let dialer = PeerDialer::new(
        Arc::from(SECRET),
        Some(client_config_from_pem(&cluster_ca.ca_path).unwrap()),
        false,
    );
    assert!(
        gossip_exchange(&addr, cid(0), &dialer, gossip_frame(&[]))
            .await
            .is_none(),
        "a certificate from an untrusted authority was accepted as a member",
    );

    impostor.abort();
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
async fn a_cluster_certificate_for_another_address_is_not_this_member() {
    // Chaining to the cluster's own authority is not enough either: the certificate
    // must name the address being dialed. Otherwise any member's key — or any leaf
    // the cluster CA ever issued — impersonates every other member, which is the
    // whole of what the dial is meant to distinguish.
    let ca = Ca::new("wrong-name");
    let leaf = ca.issue_for("elsewhere", "10.9.9.9");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("wss://{}", listener.local_addr().unwrap());
    let node = tokio::spawn(serve_with(
        listener,
        cid(0xFF),
        None,
        ServeConfig {
            tls: Some(server_config_from_pem(&leaf.cert_path, &leaf.key_path).unwrap()),
            ..ServeConfig::default()
        },
    ));

    let dialer = PeerDialer::new(
        Arc::from(SECRET),
        Some(client_config_from_pem(&ca.ca_path).unwrap()),
        false,
    );
    assert!(
        gossip_exchange(&addr, cid(0), &dialer, gossip_frame(&[]))
            .await
            .is_none(),
        "a certificate naming another address was accepted for this member",
    );

    node.abort();
}

// --- mixed plaintext / TLS membership ---

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
async fn a_cluster_may_mix_a_plaintext_member_and_a_tls_member() {
    // The rollout stance: transport is declared per member, so a cluster part-way
    // through a TLS migration still converges. Each node dials the other over
    // whatever that member's advertise address declares — TLS one way, plaintext
    // the other, in the same cluster and the same round.
    let ca = Ca::new("mixed");
    let tls_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plain_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tls_addr = format!("wss://{}", tls_listener.local_addr().unwrap());
    let plain_addr = plain_listener.local_addr().unwrap().to_string();
    let leaf = ca.issue("tls-node");
    let peer_tls = client_config_from_pem(&ca.ca_path).unwrap();

    // The plaintext node leads the room, so its write travels to the TLS follower —
    // exercising the plaintext node's `wss://` dial.
    let room = room_led_by(&plain_addr, &tls_addr);

    let tls_node = tokio::spawn(serve_with(
        tls_listener,
        cid(0xF0),
        None,
        ServeConfig {
            tls: Some(server_config_from_pem(&leaf.cert_path, &leaf.key_path).unwrap()),
            peer_tls: Some(peer_tls.clone()),
            ..clustered(&tls_addr, &plain_addr)
        },
    ));
    let plain_node = tokio::spawn(serve_with(
        plain_listener,
        cid(0xFF),
        None,
        ServeConfig {
            peer_tls: Some(peer_tls),
            ..clustered(&plain_addr, &tls_addr)
        },
    ));

    let mut writer = open_client(&plain_addr, cid(1)).await;
    assert!(
        write_reaches_the_follower(&mut writer, &room).await,
        "a mixed-transport cluster did not replicate",
    );

    tls_node.abort();
    plain_node.abort();
}

// --- mutual TLS on the peer link ---

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
async fn a_peer_link_carries_mtls_both_ways() {
    // The same handshake proves both ends when the acceptor requires a client cert
    // and the dialer is given an identity: the acceptor verifies the dialer's cert
    // and the dialer verifies the acceptor's, before either speaks the wire
    // protocol.
    let ca = Ca::new("mtls");
    let leader_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let follower_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let leader_addr = format!("wss://{}", leader_listener.local_addr().unwrap());
    let follower_addr = format!("wss://{}", follower_listener.local_addr().unwrap());
    let room = room_led_by(&leader_addr, &follower_addr);
    let leader_leaf = ca.issue("leader");
    let follower_leaf = ca.issue("follower");

    let mutual = |leaf: &Leaf| {
        Some(
            server_config_from_pem_with_client_ca(&leaf.cert_path, &leaf.key_path, &ca.ca_path)
                .unwrap(),
        )
    };
    let identity = |leaf: &Leaf| {
        Some(
            client_config_from_pem_with_identity(&ca.ca_path, &leaf.cert_path, &leaf.key_path)
                .unwrap(),
        )
    };

    let follower = tokio::spawn(serve_with(
        follower_listener,
        cid(0xF0),
        None,
        ServeConfig {
            tls: mutual(&follower_leaf),
            peer_tls: identity(&follower_leaf),
            ..clustered(&follower_addr, &leader_addr)
        },
    ));
    let leader = tokio::spawn(serve_with(
        leader_listener,
        cid(0xFF),
        None,
        ServeConfig {
            tls: mutual(&leader_leaf),
            peer_tls: identity(&leader_leaf),
            ..clustered(&leader_addr, &follower_addr)
        },
    ));

    // The client end presents the same CA-issued identity, since this listener
    // requires one of every connection.
    let mut writer = open_mtls_client(&leader_addr, &ca, &ca.issue("client"), cid(1)).await;
    assert!(
        write_reaches_the_follower(&mut writer, &room).await,
        "a mutually-authenticated peer link did not replicate",
    );

    leader.abort();
    follower.abort();
}

/// Open a client connection presenting `leaf` as its identity, trusting `ca`.
async fn open_mtls_client(addr: &str, ca: &Ca, leaf: &Leaf, client: ClientId) -> Ws {
    let config =
        client_config_from_pem_with_identity(&ca.ca_path, &leaf.cert_path, &leaf.key_path).unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async_tls_with_config(
        format!("{addr}/"),
        None,
        false,
        Some(tokio_tungstenite::Connector::Rustls(config)),
    )
    .await
    .unwrap();
    ws.send(WsMessage::Binary(encode_header(PROTOCOL_VERSION).to_vec()))
        .await
        .unwrap();
    send_frame(
        &mut ws,
        &Message::Hello {
            client,
            app_id: Vec::new(),
            schema_version: 0,
            codecs: Vec::new(),
        },
    )
    .await;
    loop {
        if let WsMessage::Binary(b) = ws.next().await.unwrap().unwrap() {
            if matches!(decode_message(&b), Ok(Message::AuthOk { .. })) {
                break;
            }
        }
    }
    ws
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials loopback servers over real sockets
async fn a_dialer_with_no_identity_is_refused_by_a_mutual_peer_listener() {
    // The counterfactual: the acceptor requires a client cert, the dialer has none,
    // and the handshake fails — so the secret is never written and nothing
    // replicates. A one-directional link cannot be mistaken for a mutual one.
    let ca = Ca::new("mtls-refused");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("wss://{}", listener.local_addr().unwrap());
    let leaf = ca.issue("node");
    let node = tokio::spawn(serve_with(
        listener,
        cid(0xFF),
        None,
        ServeConfig {
            tls: Some(
                server_config_from_pem_with_client_ca(&leaf.cert_path, &leaf.key_path, &ca.ca_path)
                    .unwrap(),
            ),
            peer_tls: Some(client_config_from_pem(&ca.ca_path).unwrap()),
            ..clustered(&addr, "10.0.0.9:9000")
        },
    ));

    let certless = PeerDialer::new(
        Arc::from(SECRET),
        Some(client_config_from_pem(&ca.ca_path).unwrap()),
        false,
    );
    assert!(
        gossip_exchange(&addr, cid(0), &certless, gossip_frame(&[]))
            .await
            .is_none(),
        "a certless dialer was admitted to a listener requiring client certs",
    );

    node.abort();
}

// --- deployment: a transport disagreement does not start ---

/// Serve `config` and return the startup error it refuses with. A node that starts
/// serves forever, so the wait is bounded: an accepted misconfiguration reports as
/// a failed assertion rather than a hung test.
async fn startup_error(listener: TcpListener, config: ServeConfig) -> std::io::Error {
    tokio::time::timeout(
        Duration::from_secs(5),
        serve_with(listener, cid(0xFF), None, config),
    )
    .await
    .expect("the node came up instead of refusing the configuration")
    .expect_err("the node came up instead of refusing the configuration")
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback listener
async fn a_tls_node_that_advertises_plaintext_refuses_to_start() {
    // The exact silent failure C12 names, now loud: the node terminates TLS and
    // tells its peers to dial `ws://`, so every peer's dial would fail forever and
    // the cluster would never converge.
    let ca = Ca::new("advertise-plain");
    let leaf = ca.issue("node");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let err = startup_error(
        listener,
        ServeConfig {
            tls: Some(server_config_from_pem(&leaf.cert_path, &leaf.key_path).unwrap()),
            peer_tls: Some(client_config_from_pem(&ca.ca_path).unwrap()),
            ..clustered(&addr, "10.0.0.9:9000")
        },
    )
    .await;
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback listener
async fn a_plaintext_node_that_advertises_tls_refuses_to_start() {
    // The same disagreement read the other way: peers would dial `wss://` into a
    // listener that terminates nothing.
    let ca = Ca::new("advertise-tls");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("wss://{}", listener.local_addr().unwrap());
    let err = startup_error(
        listener,
        ServeConfig {
            peer_tls: Some(client_config_from_pem(&ca.ca_path).unwrap()),
            ..clustered(&addr, "10.0.0.9:9000")
        },
    )
    .await;
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback listener
async fn a_tls_peer_with_no_trust_anchors_refuses_to_start() {
    // Nothing would authenticate the peer this node is about to hand the cluster
    // secret to, so the dial is refused at configuration rather than at every round.
    let ca = Ca::new("no-anchors");
    let leaf = ca.issue("node");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("wss://{}", listener.local_addr().unwrap());
    let err = startup_error(
        listener,
        ServeConfig {
            tls: Some(server_config_from_pem(&leaf.cert_path, &leaf.key_path).unwrap()),
            ..clustered(&addr, "wss://10.0.0.9:9000")
        },
    )
    .await;
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback listener
async fn a_plaintext_member_refuses_to_start_when_tls_is_required() {
    // The end of a rollout: the operator declares the cluster all-TLS, and a member
    // still advertising plaintext is a configuration error rather than a link that
    // quietly keeps writing the secret in the clear.
    let ca = Ca::new("require-tls");
    let leaf = ca.issue("node");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("wss://{}", listener.local_addr().unwrap());
    let err = startup_error(
        listener,
        ServeConfig {
            tls: Some(server_config_from_pem(&leaf.cert_path, &leaf.key_path).unwrap()),
            peer_tls: Some(client_config_from_pem(&ca.ca_path).unwrap()),
            require_peer_tls: true,
            ..clustered(&addr, "10.0.0.9:9000")
        },
    )
    .await;
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback listener
async fn peer_tls_without_a_cluster_refuses_to_start() {
    // A single-node deployment has no peer plane, so peer-dial TLS material there is
    // a cluster the operator meant to configure — the same shape as a cluster secret
    // with no membership.
    let ca = Ca::new("no-cluster");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let err = startup_error(
        listener,
        ServeConfig {
            peer_tls: Some(client_config_from_pem(&ca.ca_path).unwrap()),
            ..ServeConfig::default()
        },
    )
    .await;
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback listener
async fn an_unparseable_member_address_refuses_to_start() {
    // A scheme the dial cannot honor is a typo, not a hostname: folding it into one
    // would produce `ws://http://host/` and a dial that fails forever.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let err = startup_error(
        listener,
        ServeConfig {
            ..clustered(&addr, "http://10.0.0.9:9000")
        },
    )
    .await;
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds a loopback listener
async fn a_node_requiring_peer_tls_that_serves_plaintext_refuses_to_start() {
    // Incoherent rather than merely strict: a node that demands TLS of every peer
    // while terminating none is refused by every peer running the same policy, so
    // the cluster it declared finished cannot form at all.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let err = startup_error(
        listener,
        ServeConfig {
            require_peer_tls: true,
            ..clustered(&addr, "10.0.0.9:9000")
        },
    )
    .await;
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

// --- a dial that opens nothing does not wedge the link that owns it ---

#[tokio::test]
#[cfg_attr(miri, ignore)] // binds and dials a loopback socket
async fn a_dial_to_a_far_end_that_never_speaks_gives_up() {
    // TLS puts a handshake before the wire protocol, and a far end that accepts the
    // socket and then says nothing would stall it indefinitely — leaving the task
    // that owns a follower's link blocked forever, never redialing. The dial is
    // bounded, the way the accept loop bounds the handshake it terminates.
    let ca = Ca::new("blackhole");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("wss://{}", listener.local_addr().unwrap());
    // Accept and hold the socket open, reading nothing and writing nothing.
    let mute = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(600)).await;
        drop(sock);
    });

    let dialer = PeerDialer::new(
        Arc::from(SECRET),
        Some(client_config_from_pem(&ca.ca_path).unwrap()),
        false,
    );
    let opened = tokio::time::timeout(PEER_DIAL_TIMEOUT * 2, dialer.connect(&addr)).await;
    assert!(
        matches!(opened, Ok(Err(_))),
        "the dial neither opened nor gave up within its own timeout",
    );

    mute.abort();
}

// --- the client TLS config itself ---

#[test]
fn a_trust_bundle_holding_no_certificate_is_a_loud_error() {
    // The mirror of the listener's rule: a deployment that asked for peer TLS never
    // silently ends up trusting nothing (which would fail at every dial instead) or
    // trusting everything.
    let dir = temp_dir("empty-ca");
    let path = dir.join("ca.pem");
    std::fs::write(&path, b"not a certificate").unwrap();
    assert!(matches!(
        client_config_from_pem(&path),
        Err(TlsConfigError::NoPeerCa(_))
    ));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_missing_trust_bundle_is_a_loud_error() {
    let dir = temp_dir("missing-ca");
    assert!(matches!(
        client_config_from_pem(dir.join("absent.pem")),
        Err(TlsConfigError::Io { .. })
    ));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_client_identity_whose_key_does_not_match_its_cert_is_a_loud_error() {
    let ca = Ca::new("mismatched");
    let one = ca.issue("one");
    let other = ca.issue("other");
    assert!(matches!(
        client_config_from_pem_with_identity(&ca.ca_path, &one.cert_path, &other.key_path),
        Err(TlsConfigError::Rustls(_))
    ));
}
