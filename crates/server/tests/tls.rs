//! TLS termination at the listener — the encrypted transport end to end.
//!
//! A server configured with a cert/key wraps every accepted socket in a rustls
//! session and speaks the same wire protocol over it; a client completes a TLS
//! handshake and round-trips a frame over the encrypted stream. With no cert
//! configured the listener stays plaintext (the regression the `ws` suite covers
//! in full). A configured-but-broken cert is a loud startup error, never a silent
//! downgrade to plaintext.
//!
//! Excluded under Miri, which cannot run tokio's real I/O.
#![cfg(not(miri))]

use std::path::PathBuf;
use std::sync::Arc;

use crdtsync_core::protocol::{Channel, PROTOCOL_VERSION};
use crdtsync_core::{decode_message, encode_header, encode_message, ClientId, Message};
use crdtsync_server::runtime::{serve_with, ServeConfig};
use crdtsync_server::{server_config_from_pem, TlsConfigError};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{client_async, WebSocketStream};

const CH: Channel = Channel(0);
const ROOM: &[u8] = b"room-1";

fn cid(first: u8) -> ClientId {
    let mut b = [0u8; 16];
    b[0] = first;
    ClientId::from_bytes(b)
}

/// A generated self-signed cert/key pair on disk plus the cert DER a client
/// trusts. The temp directory is removed when the guard drops.
struct TestCert {
    dir: PathBuf,
    cert_path: PathBuf,
    key_path: PathBuf,
    cert_der: CertificateDer<'static>,
}

impl Drop for TestCert {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Generate a self-signed cert for `localhost`, writing the PEM cert + key to a
/// fresh temp directory.
fn test_cert() -> TestCert {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("crdtsync-tls-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, key.cert.pem()).unwrap();
    std::fs::write(&key_path, key.key_pair.serialize_pem()).unwrap();
    let cert_der = key.cert.der().clone();

    TestCert {
        dir,
        cert_path,
        key_path,
        cert_der,
    }
}

/// A running TLS test server: its socket address, the trusted cert, and the
/// accept-loop task (aborted when the server is dropped).
struct Server {
    addr: std::net::SocketAddr,
    cert_der: CertificateDer<'static>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Start a server terminating TLS with a fresh self-signed cert.
async fn start_tls_server() -> Server {
    let cert = test_cert();
    let tls = server_config_from_pem(&cert.cert_path, &cert.key_path).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_with(
        listener,
        cid(0xFF),
        None,
        ServeConfig {
            tls: Some(tls),
            ..ServeConfig::default()
        },
    ));
    Server {
        addr,
        cert_der: cert.cert_der.clone(),
        task,
    }
}

type Tls = WebSocketStream<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>;

/// Complete a TLS handshake to the server and negotiate a WebSocket over the
/// encrypted stream, trusting the server's self-signed cert.
async fn open_tls(server: &Server) -> Tls {
    let mut roots = RootCertStore::empty();
    roots.add(server.cert_der.clone()).unwrap();
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));

    let tcp = tokio::net::TcpStream::connect(server.addr).await.unwrap();
    let domain = ServerName::try_from("localhost").unwrap();
    let tls = connector.connect(domain, tcp).await.unwrap();
    let (ws, _) = client_async("ws://localhost/", tls).await.unwrap();
    ws
}

async fn send(ws: &mut Tls, msg: &Message) {
    ws.send(WsMessage::Binary(encode_message(msg)))
        .await
        .unwrap();
}

async fn recv(ws: &mut Tls) -> Message {
    loop {
        match ws.next().await.unwrap().unwrap() {
            WsMessage::Binary(b) => return decode_message(&b).unwrap(),
            WsMessage::Close(_) => panic!("connection closed before a message"),
            _ => continue,
        }
    }
}

/// The end-to-end property: a client completes a real TLS handshake and drives
/// the whole wire handshake — header, Hello, Auth, Subscribe — over the encrypted
/// stream, receiving its catch-up batch back. The session logic is unchanged from
/// the plaintext path; only the transport is wrapped.
#[tokio::test]
async fn a_wire_frame_round_trips_over_the_encrypted_stream() {
    let server = start_tls_server().await;
    let mut ws = open_tls(&server).await;

    ws.send(WsMessage::Binary(encode_header(PROTOCOL_VERSION).to_vec()))
        .await
        .unwrap();
    send(
        &mut ws,
        &Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
        },
    )
    .await;
    send(
        &mut ws,
        &Message::Auth {
            credential: b"cred".to_vec(),
        },
    )
    .await;
    assert_eq!(
        recv(&mut ws).await,
        Message::AuthOk {
            actor: b"cred".to_vec(),
        }
    );
    send(
        &mut ws,
        &Message::Subscribe {
            channel: CH,
            room: ROOM.to_vec(),
            zone: Vec::new(),
            last_seen_seq: 0,
            branch: Vec::new(),
        },
    )
    .await;
    assert_eq!(
        recv(&mut ws).await,
        Message::Ops {
            channel: CH,
            ops: Vec::new(),
        },
        "a fresh room's catch-up is empty, delivered over TLS"
    );
}

/// A plaintext client speaking straight to the TLS listener never completes the
/// handshake — the server does not fall back to plaintext for it. Together with
/// the encrypted round-trip above, this proves TLS is genuinely terminated (not
/// an accidental no-op that also accepts cleartext).
#[tokio::test]
async fn a_plaintext_client_cannot_talk_to_the_tls_listener() {
    let server = start_tls_server().await;
    let (mut ws, _) = match tokio_tungstenite::connect_async(format!("ws://{}/", server.addr)).await
    {
        Ok(conn) => conn,
        // A refused/aborted upgrade is the expected outcome — no plaintext session.
        Err(_) => return,
    };
    // If the upgrade somehow completed, the header write/read must not yield a
    // live wire session.
    let sent = ws
        .send(WsMessage::Binary(encode_header(PROTOCOL_VERSION).to_vec()))
        .await;
    assert!(
        sent.is_err() || ws.next().await.transpose().ok().flatten().is_none(),
        "a plaintext client gets no wire session on the TLS listener"
    );
}

// --- ServerConfig construction + config gating (no socket) ---

#[test]
fn a_good_pem_pair_builds_a_server_config() {
    let cert = test_cert();
    assert!(
        server_config_from_pem(&cert.cert_path, &cert.key_path).is_ok(),
        "a matching cert/key pair loads"
    );
}

#[test]
fn a_missing_cert_path_is_a_loud_error_not_a_plaintext_fallback() {
    let cert = test_cert();
    let err = server_config_from_pem(cert.dir.join("nope.pem"), &cert.key_path)
        .expect_err("a missing cert file must fail, never downgrade to plaintext");
    assert!(matches!(err, TlsConfigError::Io { .. }), "got {err:?}");
}

#[test]
fn a_missing_key_path_is_a_loud_error() {
    let cert = test_cert();
    let err = server_config_from_pem(&cert.cert_path, cert.dir.join("nope.pem"))
        .expect_err("a missing key file must fail");
    assert!(matches!(err, TlsConfigError::Io { .. }), "got {err:?}");
}

#[test]
fn an_empty_cert_file_reports_no_certificate() {
    let cert = test_cert();
    let empty = cert.dir.join("empty.pem");
    std::fs::write(&empty, b"not a certificate\n").unwrap();
    let err = server_config_from_pem(&empty, &cert.key_path)
        .expect_err("a PEM file with no certificate must fail");
    assert!(
        matches!(err, TlsConfigError::NoCertificate(_)),
        "got {err:?}"
    );
}

#[test]
fn a_key_file_with_no_private_key_reports_no_private_key() {
    let cert = test_cert();
    let empty = cert.dir.join("nokey.pem");
    std::fs::write(&empty, b"garbage\n").unwrap();
    let err = server_config_from_pem(&cert.cert_path, &empty)
        .expect_err("a PEM file with no private key must fail");
    assert!(
        matches!(err, TlsConfigError::NoPrivateKey(_)),
        "got {err:?}"
    );
}

#[test]
fn a_mismatched_cert_and_key_is_rejected_by_rustls() {
    // Two independent self-signed pairs: the first cert with the second key.
    let a = test_cert();
    let b = test_cert();
    let err = server_config_from_pem(&a.cert_path, &b.key_path)
        .expect_err("a key that does not match the cert must fail");
    assert!(matches!(err, TlsConfigError::Rustls(_)), "got {err:?}");
}
