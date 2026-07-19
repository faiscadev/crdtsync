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
use crdtsync_server::runtime::{serve_with, serve_with_authorizer, ServeConfig};
use crdtsync_server::{
    server_config_from_pem, server_config_from_pem_with_client_ca,
    server_config_from_pem_with_client_ca_mode, Action, AllowAll, Authorizer, ClientAuthMode,
    Identity, PermitAll, Resource, TlsConfigError,
};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_rustls::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName,
};
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

// --- mTLS: client-cert authentication ---------------------------------------

/// A test certificate authority: its cert + signing key held in memory (to issue
/// client certs) plus the CA cert written as PEM on disk (the trust bundle a
/// server loads). The temp directory is removed when the guard drops.
struct TestCa {
    dir: PathBuf,
    ca_pem_path: PathBuf,
    ca_cert: rcgen::Certificate,
    ca_key: rcgen::KeyPair,
}

impl Drop for TestCa {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Mint a self-signed CA and write its cert PEM to a fresh temp directory.
fn test_ca() -> TestCa {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("crdtsync-mtls-ca-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
    ];
    let mut dn = rcgen::DistinguishedName::new();
    dn.push(rcgen::DnType::CommonName, "crdtsync-test-ca");
    params.distinguished_name = dn;
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = params.self_signed(&ca_key).unwrap();

    let ca_pem_path = dir.join("ca.pem");
    std::fs::write(&ca_pem_path, ca_cert.pem()).unwrap();

    TestCa {
        dir,
        ca_pem_path,
        ca_cert,
        ca_key,
    }
}

/// A client keypair the connector presents: the leaf cert DER and its private key.
type ClientCert = (CertificateDer<'static>, PrivateKeyDer<'static>);

impl TestCa {
    /// Issue a client cert signed by this CA, carrying the given SAN DNS names and
    /// an optional CN. A `clientAuth` EKU marks it usable for client auth.
    fn issue(&self, sans: &[&str], cn: Option<&str>) -> ClientCert {
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        for san in sans {
            params
                .subject_alt_names
                .push(rcgen::SanType::DnsName((*san).try_into().unwrap()));
        }
        // Set the DN explicitly so a SAN-only cert carries no CN at all — the
        // fallback tests then turn only on the presence of a SAN.
        let mut dn = rcgen::DistinguishedName::new();
        if let Some(cn) = cn {
            dn.push(rcgen::DnType::CommonName, cn);
        }
        params.distinguished_name = dn;

        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.ca_cert, &self.ca_key).unwrap();
        let cert_der = cert.der().clone();
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der()));
        (cert_der, key_der)
    }
}

/// Start an mTLS server: it terminates TLS with a fresh self-signed server cert
/// and *requires* every client to present a cert chaining to `ca`, gating each
/// authenticated actor through `authorizer`.
async fn start_mtls_server(ca: &TestCa, authorizer: Box<dyn Authorizer + Send + Sync>) -> Server {
    let cert = test_cert();
    let tls =
        server_config_from_pem_with_client_ca(&cert.cert_path, &cert.key_path, &ca.ca_pem_path)
            .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_with_authorizer(
        listener,
        cid(0xFF),
        None,
        ServeConfig {
            tls: Some(tls),
            ..ServeConfig::default()
        },
        Box::new(AllowAll),
        authorizer,
    ));
    Server {
        addr,
        cert_der: cert.cert_der.clone(),
        task,
    }
}

/// Start a server in mTLS **request** mode against `ca`: it presents a cert
/// request and validates any cert a client presents, but a client presenting *no*
/// cert is admitted (opportunistic mTLS) and falls through to the certless session
/// path. An in-band credential is verified through `AllowAll`.
async fn start_mtls_request_server(
    ca: &TestCa,
    authorizer: Box<dyn Authorizer + Send + Sync>,
) -> Server {
    let cert = test_cert();
    let tls = server_config_from_pem_with_client_ca_mode(
        &cert.cert_path,
        &cert.key_path,
        &ca.ca_pem_path,
        ClientAuthMode::Request,
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_with_authorizer(
        listener,
        cid(0xFF),
        None,
        ServeConfig {
            tls: Some(tls),
            ..ServeConfig::default()
        },
        Box::new(AllowAll),
        authorizer,
    ));
    Server {
        addr,
        cert_der: cert.cert_der.clone(),
        task,
    }
}

/// Complete a TLS handshake to `server`, optionally presenting `client` as the
/// client certificate. Returns the handshake result — `Err` is a rejected
/// handshake (the fail-closed path), `Ok` a live encrypted WebSocket. A rejected
/// client cert can surface either at the TLS handshake itself or, under TLS 1.3
/// (where the client finishes before the server's alert), at the first read the
/// WebSocket handshake drives — both are propagated as an error.
async fn open_client(
    server: &Server,
    client: Option<ClientCert>,
) -> Result<Tls, Box<dyn std::error::Error>> {
    let mut roots = RootCertStore::empty();
    roots.add(server.cert_der.clone()).unwrap();
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let config = match client {
        Some((cert, key)) => builder.with_client_auth_cert(vec![cert], key).unwrap(),
        None => builder.with_no_client_auth(),
    };
    let connector = TlsConnector::from(Arc::new(config));

    let tcp = tokio::net::TcpStream::connect(server.addr).await?;
    let domain = ServerName::try_from("localhost").unwrap();
    let tls = connector.connect(domain, tcp).await?;
    let (ws, _) = client_async("ws://localhost/", tls).await?;
    Ok(ws)
}

/// Drive the wire opening handshake — the 8-byte header then Hello — over an mTLS
/// stream whose actor the cert already established, and read back the server's
/// `AuthOk`. The connection skips the in-band Auth phase, so the actor the server
/// reports is the one it derived from the client certificate.
async fn cert_authok_actor(ws: &mut Tls) -> Vec<u8> {
    ws.send(WsMessage::Binary(encode_header(PROTOCOL_VERSION).to_vec()))
        .await
        .unwrap();
    send(
        ws,
        &Message::Hello {
            client: cid(1),
            app_id: Vec::new(),
            schema_version: 0,
        },
    )
    .await;
    match recv(ws).await {
        Message::AuthOk { actor } => actor,
        other => panic!("expected AuthOk from the cert-authed connection, got {other:?}"),
    }
}

/// An authorizer that records the `(action, actor)` of every check and permits
/// it — a probe that proves which identity the ACL evaluator was asked about.
struct RecordingAuthorizer {
    seen: Arc<std::sync::Mutex<Vec<(Action, Vec<u8>)>>>,
}

impl Authorizer for RecordingAuthorizer {
    fn authorize(&self, identity: &Identity, action: Action, _resource: &Resource) -> bool {
        self.seen
            .lock()
            .unwrap()
            .push((action, identity.actor().to_vec()));
        true
    }
}

/// A valid client cert with a SAN authenticates the connection as that SAN: the
/// server reports it as the session actor without any in-band Auth.
#[tokio::test]
async fn a_client_cert_san_binds_as_the_session_actor() {
    let ca = test_ca();
    let server = start_mtls_server(&ca, Box::new(PermitAll)).await;
    let mut ws = open_client(&server, Some(ca.issue(&["alice"], Some("ignored-cn"))))
        .await
        .expect("a cert signed by the configured CA completes the handshake");
    assert_eq!(
        cert_authok_actor(&mut ws).await,
        b"alice".to_vec(),
        "the SAN is bound as the actor, in preference to the CN"
    );
}

/// A cert carrying no SAN falls back to its Common Name for the actor.
#[tokio::test]
async fn a_client_cert_without_a_san_falls_back_to_its_cn() {
    let ca = test_ca();
    let server = start_mtls_server(&ca, Box::new(PermitAll)).await;
    let mut ws = open_client(&server, Some(ca.issue(&[], Some("carol"))))
        .await
        .expect("a CN-only cert signed by the CA still handshakes");
    assert_eq!(
        cert_authok_actor(&mut ws).await,
        b"carol".to_vec(),
        "with no SAN the CN is the actor"
    );
}

/// The cert-derived actor is the identity the ACL evaluator sees: a subscribe's
/// read check is asked about the SAN, not the ephemeral client id.
#[tokio::test]
async fn an_acl_decision_is_keyed_on_the_cert_actor() {
    let ca = test_ca();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let authorizer = RecordingAuthorizer { seen: seen.clone() };
    let server = start_mtls_server(&ca, Box::new(authorizer)).await;
    let mut ws = open_client(&server, Some(ca.issue(&["alice"], None)))
        .await
        .expect("alice's cert handshakes");

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
    assert_eq!(
        recv(&mut ws).await,
        Message::AuthOk {
            actor: b"alice".to_vec(),
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
        }
    );

    let seen = seen.lock().unwrap();
    assert!(
        seen.contains(&(Action::Read, b"alice".to_vec())),
        "the subscribe's read authorization was keyed on the cert's SAN actor; saw {seen:?}"
    );
}

/// mTLS is fail-closed: a client presenting no certificate is rejected at the
/// handshake and never reaches the wire protocol.
#[tokio::test]
async fn a_client_with_no_cert_is_rejected_when_mtls_is_required() {
    let ca = test_ca();
    let server = start_mtls_server(&ca, Box::new(PermitAll)).await;
    let result = open_client(&server, None).await;
    assert!(
        result.is_err(),
        "a client with no cert must be rejected at the mTLS handshake"
    );
}

/// A cert from an untrusted CA is rejected at the handshake: the verifier trusts
/// only the configured trust anchors, so a cert that does not chain to them fails.
#[tokio::test]
async fn a_client_cert_from_an_untrusted_ca_is_rejected() {
    let trusted = test_ca();
    let rogue = test_ca();
    let server = start_mtls_server(&trusted, Box::new(PermitAll)).await;
    let result = open_client(&server, Some(rogue.issue(&["mallory"], None))).await;
    assert!(
        result.is_err(),
        "a cert not chaining to the configured CA must be rejected at the handshake"
    );
}

/// Regression against #300: with no client CA configured the server stays
/// server-auth-only — a plain-TLS client presenting no certificate connects and
/// drives the wire protocol, exactly as before mTLS existed.
#[tokio::test]
async fn a_server_with_no_client_ca_still_accepts_a_certless_client() {
    let server = start_tls_server().await;
    let mut ws = open_client(&server, None)
        .await
        .expect("server-auth-only TLS accepts a client with no cert");
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
        },
        "with no client CA the connection authenticates in band, unchanged"
    );
}

// --- mTLS request mode: opportunistic (authenticate-if-presented) --------------

/// Request mode still binds a presented valid cert's identity exactly as require
/// mode: authenticate-if-presented. A client offering a CA-signed cert is
/// authenticated as its SAN without any in-band Auth.
#[tokio::test]
async fn a_request_mode_client_with_a_valid_cert_binds_its_identity() {
    let ca = test_ca();
    let server = start_mtls_request_server(&ca, Box::new(PermitAll)).await;
    let mut ws = open_client(&server, Some(ca.issue(&["alice"], Some("ignored-cn"))))
        .await
        .expect("a CA-signed cert handshakes in request mode");
    assert_eq!(
        cert_authok_actor(&mut ws).await,
        b"alice".to_vec(),
        "a presented valid cert authenticates as its SAN, as in require mode"
    );
}

/// The relaxation: a client presenting NO cert is admitted in request mode (not
/// rejected at the handshake) and falls through to the ordinary certless session
/// path — it authenticates in band exactly as a non-mTLS connection does.
#[tokio::test]
async fn a_request_mode_certless_client_connects_and_authenticates_in_band() {
    let ca = test_ca();
    let server = start_mtls_request_server(&ca, Box::new(PermitAll)).await;
    let mut ws = open_client(&server, None)
        .await
        .expect("request mode admits a client presenting no cert (not rejected)");
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
        },
        "a certless request-mode client falls through to the in-band credential path"
    );
}

/// The boundary that must hold: request mode relaxes cert *absence* only. A client
/// that *presents* a cert not chaining to the configured CA is STILL rejected at
/// the handshake — a bad presented cert is never treated as anonymous.
#[tokio::test]
async fn a_request_mode_untrusted_cert_is_still_rejected() {
    let trusted = test_ca();
    let rogue = test_ca();
    let server = start_mtls_request_server(&trusted, Box::new(PermitAll)).await;
    let result = open_client(&server, Some(rogue.issue(&["mallory"], None))).await;
    assert!(
        result.is_err(),
        "request mode validates a presented cert: an untrusted one is still rejected"
    );
}

// --- mTLS ServerConfig construction (no socket) ---

#[test]
fn a_valid_client_ca_builds_an_mtls_server_config() {
    let cert = test_cert();
    let ca = test_ca();
    assert!(
        server_config_from_pem_with_client_ca(&cert.cert_path, &cert.key_path, &ca.ca_pem_path)
            .is_ok(),
        "a server cert/key plus a client-CA bundle builds an mTLS config"
    );
}

#[test]
fn an_empty_client_ca_bundle_is_a_loud_error_not_a_downgrade() {
    let cert = test_cert();
    let empty = cert.dir.join("empty-ca.pem");
    std::fs::write(&empty, b"not a certificate\n").unwrap();
    let err = server_config_from_pem_with_client_ca(&cert.cert_path, &cert.key_path, &empty)
        .expect_err("a client-CA bundle with no cert must fail, never silently drop client auth");
    assert!(matches!(err, TlsConfigError::NoClientCa(_)), "got {err:?}");
}

#[test]
fn a_missing_client_ca_path_is_a_loud_error() {
    let cert = test_cert();
    let err = server_config_from_pem_with_client_ca(
        &cert.cert_path,
        &cert.key_path,
        cert.dir.join("nope.pem"),
    )
    .expect_err("a missing client-CA file must fail");
    assert!(matches!(err, TlsConfigError::Io { .. }), "got {err:?}");
}

#[test]
fn a_request_mode_client_ca_builds_an_mtls_server_config() {
    let cert = test_cert();
    let ca = test_ca();
    assert!(
        server_config_from_pem_with_client_ca_mode(
            &cert.cert_path,
            &cert.key_path,
            &ca.ca_pem_path,
            ClientAuthMode::Request,
        )
        .is_ok(),
        "request mode builds a valid mTLS config"
    );
}

// --- client-auth mode parsing: default is the secure `require` -----------------

#[test]
fn absent_client_auth_mode_defaults_to_require() {
    assert_eq!(
        ClientAuthMode::parse(None).unwrap(),
        ClientAuthMode::Require,
        "an unset CRDTSYNC_TLS_CLIENT_AUTH is the secure require default"
    );
    assert_eq!(ClientAuthMode::default(), ClientAuthMode::Require);
}

#[test]
fn client_auth_mode_parses_require_and_request_case_insensitively() {
    for v in ["require", "REQUIRE", " Require "] {
        assert_eq!(
            ClientAuthMode::parse(Some(v)).unwrap(),
            ClientAuthMode::Require,
            "{v:?} parses as require"
        );
    }
    for v in ["request", "REQUEST", " Request "] {
        assert_eq!(
            ClientAuthMode::parse(Some(v)).unwrap(),
            ClientAuthMode::Request,
            "{v:?} parses as request"
        );
    }
}

#[test]
fn an_unrecognized_client_auth_mode_is_a_loud_error_not_the_permissive_mode() {
    let err = ClientAuthMode::parse(Some("allow-any"))
        .expect_err("an unknown mode must fail, never resolve to the permissive request mode");
    assert!(
        matches!(err, TlsConfigError::BadClientAuthMode(ref v) if v == "allow-any"),
        "got {err:?}"
    );
}
