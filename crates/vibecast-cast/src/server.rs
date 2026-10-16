//! TLS listener that accepts Cast senders and spawns per-connection tasks.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use rustls::ServerConfig;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;

use crate::connection::{run_connection, AuthMaterial, ServerEvent};

/// Accepts TLS connections on a listener and drives each as a Cast connection.
///
/// Device-auth material is held behind an [`ArcSwap`] so it can be hot-reloaded
/// with [`CastServer::update_auth`]; connections snapshot it at accept time.
pub struct CastServer {
    acceptor: TlsAcceptor,
    auth: Arc<ArcSwap<AuthMaterial>>,
    events: mpsc::Sender<ServerEvent>,
    next_id: AtomicU64,
}

impl CastServer {
    /// Create a server from a rustls config, initial auth material, and an
    /// event sink that receives connection lifecycle and inbound messages.
    #[must_use]
    pub fn new(
        config: ServerConfig,
        auth: AuthMaterial,
        events: mpsc::Sender<ServerEvent>,
    ) -> Self {
        Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            auth: Arc::new(ArcSwap::from_pointee(auth)),
            events,
            next_id: AtomicU64::new(1),
        }
    }

    /// Atomically replace the device-auth material for future connections.
    pub fn update_auth(&self, auth: AuthMaterial) {
        self.auth.store(Arc::new(auth));
    }

    /// Accept connections until the listener errors (e.g. is closed).
    ///
    /// Each accepted connection is handled in its own task; a slow or failed
    /// TLS handshake never blocks the accept loop.
    pub async fn serve(&self, listener: TcpListener) -> std::io::Result<()> {
        self.serve_with_handshake_timeout(listener, Duration::from_secs(10))
            .await
    }

    async fn serve_with_handshake_timeout(
        &self,
        listener: TcpListener,
        handshake_timeout: Duration,
    ) -> std::io::Result<()> {
        // Dropping the listener future must also close already accepted sockets.
        let mut connections = JoinSet::new();
        loop {
            let (stream, peer) = tokio::select! {
                accepted = listener.accept() => accepted?,
                _ = connections.join_next(), if !connections.is_empty() => continue,
            };
            let acceptor = self.acceptor.clone();
            let auth = self.auth.load_full();
            let events = self.events.clone();
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let peer: Arc<str> = Arc::from(peer.to_string());

            connections.spawn(async move {
                match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await {
                    Ok(Ok(tls)) => run_connection(tls, id, peer, auth, events).await,
                    Ok(Err(err)) => {
                        tracing::warn!(peer = %peer, error = %err, "TLS handshake failed")
                    }
                    Err(_) => tracing::warn!(peer = %peer, "TLS handshake timed out"),
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use vibecast_security::{server_config, CertResolver, CertificateBundle};

    fn test_server(events: mpsc::Sender<ServerEvent>) -> CastServer {
        let key = rcgen::KeyPair::generate().unwrap();
        let certificate = rcgen::CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let bundle = CertificateBundle {
            peer_cert_pem: certificate.pem().into_bytes(),
            peer_key_pem: key.serialize_pem().into_bytes(),
            peer_cert_der: certificate.der().to_vec(),
            device_cert_der: vec![1],
            intermediate_certs_der: vec![],
            signature_sha1: vec![],
            signature_sha256: vec![],
            not_valid_before: 0,
            not_valid_after: i64::MAX,
            crl: None,
        };
        let config = server_config(CertResolver::new(&bundle).unwrap()).unwrap();
        CastServer::new(config, AuthMaterial { bundle, crl: None }, events)
    }

    async fn pending_handshake(cancel: bool) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (events, _received) = mpsc::channel(4);
        let server = test_server(events);
        let task = tokio::spawn(async move {
            server
                .serve_with_handshake_timeout(
                    listener,
                    if cancel {
                        Duration::from_secs(60)
                    } else {
                        Duration::from_millis(100)
                    },
                )
                .await
        });
        let mut socket = TcpStream::connect(addr).await.unwrap();
        // An incomplete TLS record keeps the accepted handshake pending.
        socket.write_all(&[0x16, 0x03, 0x03]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        if cancel {
            task.abort();
        }
        let mut byte = [0];
        let result = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
            .await
            .expect("pending TLS socket must close after timeout or server cancellation");
        assert!(matches!(result, Ok(0) | Err(_)));
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn stalled_tls_handshake_has_a_deadline() {
        pending_handshake(false).await;
    }

    #[tokio::test]
    async fn stopping_server_closes_accepted_handshakes() {
        pending_handshake(true).await;
    }

    #[tokio::test]
    async fn stopping_server_closes_established_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (events, mut received) = mpsc::channel(4);
        let server = test_server(events);
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls_pki_types::CertificateDer::from(
                server.auth.load().bundle.peer_cert_der.clone(),
            ))
            .unwrap();
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let task = tokio::spawn(async move { server.serve(listener).await });
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut client = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(
                rustls_pki_types::ServerName::try_from("localhost").unwrap(),
                stream,
            )
            .await
            .unwrap();
        assert!(matches!(
            received.recv().await,
            Some(ServerEvent::Connected(_))
        ));
        task.abort();
        let _ = task.await;
        let result = tokio::time::timeout(Duration::from_secs(1), client.read(&mut [0]))
            .await
            .expect("receiver stop must close established TLS sockets too");
        assert!(matches!(result, Ok(0) | Err(_)));
    }
}
