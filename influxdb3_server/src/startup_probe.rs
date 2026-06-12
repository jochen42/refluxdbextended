//! Minimal HTTP responder that holds the API listener while the rest of
//! startup (catalog load, WAL replay) is still running.
//!
//! Replaying a large WAL from an object store can take minutes. If the
//! API port stays closed for that long, orchestrator health checks (load
//! balancers, MIG autohealing, docker healthchecks) declare the node dead
//! and kill it mid-replay — and the replacement starts over with an even
//! larger backlog. Binding the listener first and answering health probes
//! with 200 keeps the supervisor away; every other request gets a 503
//! with `Retry-After` so clients know the node exists but is not ready.
//!
//! Only HTTP/1.1 is spoken here, which is what health probes use. When
//! TLS is configured the probe terminates it with the same certificate as
//! the real server and advertises `http/1.1` via ALPN.

use crate::all_paths;
use observability_deps::tracing::{debug, info, warn};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{ServerConfig, SupportedProtocolVersion};
use tokio_util::sync::CancellationToken;

/// Cap on how long a single probe connection may take from accept to
/// response, TLS handshake included.
const PROBE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

static OK_RESPONSE: LazyLock<String> = LazyLock::new(|| probe_response("200 OK", "OK"));

static UNAVAILABLE_RESPONSE: LazyLock<String> = LazyLock::new(|| {
    probe_response(
        "503 Service Unavailable",
        "server is starting: catalog load / WAL replay in progress\n",
    )
});

fn probe_response(status: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\n\
         content-type: text/plain; charset=utf-8\r\n\
         content-length: {}\r\n\
         retry-after: 5\r\n\
         connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    )
}

/// A running startup probe responder. Create with [`StartupProbe::spawn`],
/// then call [`StartupProbe::into_listener`] once real startup is complete
/// to take the listener back — same socket, so an OS-assigned `:0` port
/// never changes across the handover.
#[derive(Debug)]
pub struct StartupProbe {
    cancel: CancellationToken,
    handle: JoinHandle<TcpListener>,
}

impl StartupProbe {
    /// Start answering probe requests on `listener`. Pass the same cert
    /// and key the real server will use so HTTPS health checks succeed
    /// during startup too.
    pub fn spawn(
        listener: TcpListener,
        cert_file: Option<&PathBuf>,
        key_file: Option<&PathBuf>,
        tls_minimum_version: &[&'static SupportedProtocolVersion],
    ) -> Result<Self, crate::Error> {
        let tls_acceptor = match (cert_file, key_file) {
            (Some(cert_file), Some(key_file)) => {
                Some(build_tls_acceptor(cert_file, key_file, tls_minimum_version)?)
            }
            _ => None,
        };
        if let Ok(addr) = listener.local_addr() {
            info!(address = %addr, "answering health probes while startup completes");
        }
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(accept_loop(listener, tls_acceptor, cancel.clone()));
        Ok(Self { cancel, handle })
    }

    /// Stop the probe responder and hand the listener back. In-flight
    /// probe connections own their streams and finish independently.
    pub async fn into_listener(self) -> Result<TcpListener, crate::Error> {
        self.cancel.cancel();
        self.handle
            .await
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))
    }
}

fn build_tls_acceptor(
    cert_file: &PathBuf,
    key_file: &PathBuf,
    tls_minimum_version: &[&'static SupportedProtocolVersion],
) -> Result<TlsAcceptor, crate::Error> {
    let certs = CertificateDer::pem_file_iter(cert_file)
        .map_err(|e| crate::Error::TlsConfig(format!("Error reading certs: {e}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| crate::Error::TlsConfig(format!("Error reading certs: {e}")))?;
    let key = PrivateKeyDer::from_pem_file(key_file)
        .map_err(|e| crate::Error::TlsConfig(format!("Error reading private key: {e}")))?;
    let mut tls_config = ServerConfig::builder_with_protocol_versions(tls_minimum_version)
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    tls_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(tls_config)))
}

async fn accept_loop(
    listener: TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
    cancel: CancellationToken,
) -> TcpListener {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return listener,
            res = listener.accept() => match res {
                Ok((stream, remote_addr)) => {
                    let tls_acceptor = tls_acceptor.clone();
                    tokio::spawn(async move {
                        let answer = async {
                            match tls_acceptor {
                                Some(acceptor) => {
                                    answer_probe(acceptor.accept(stream).await?).await
                                }
                                None => answer_probe(stream).await,
                            }
                        };
                        match tokio::time::timeout(PROBE_CONNECTION_TIMEOUT, answer).await {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => debug!(%remote_addr, error = %e, "startup probe connection error"),
                            Err(_) => debug!(%remote_addr, "startup probe connection timed out"),
                        }
                    });
                }
                Err(e) => {
                    // Transient accept errors (e.g. EMFILE) — back off
                    // briefly instead of spinning.
                    warn!(error = %e, "startup probe accept error");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

async fn answer_probe<S>(mut stream: S) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = [0u8; 4096];
    let mut filled = 0;
    loop {
        let n = stream.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
        if buf[..filled].windows(4).any(|w| w == b"\r\n\r\n") || filled == buf.len() {
            break;
        }
    }

    let head = String::from_utf8_lossy(&buf[..filled]);
    let mut request_line = head.lines().next().unwrap_or("").split_whitespace();
    let method = request_line.next().unwrap_or("");
    let path = request_line
        .next()
        .unwrap_or("")
        .split('?')
        .next()
        .unwrap_or("");

    let response: &str = if method == "GET"
        && matches!(
            path,
            all_paths::API_V3_HEALTH | all_paths::API_V1_HEALTH | all_paths::API_PING
        ) {
        &OK_RESPONSE
    } else {
        &UNAVAILABLE_RESPONSE
    };
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn request(addr: std::net::SocketAddr, raw: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(raw.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn health_paths_return_200_everything_else_503() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let probe = StartupProbe::spawn(listener, None, None, &[]).unwrap();

        for path in ["/health", "/api/v1/health", "/ping"] {
            let response =
                request(addr, &format!("GET {path} HTTP/1.1\r\nhost: x\r\n\r\n")).await;
            assert!(
                response.starts_with("HTTP/1.1 200 OK"),
                "{path}: {response}"
            );
            assert!(response.ends_with("OK"), "{path}: {response}");
        }

        let response = request(addr, "GET /api/v3/query_sql?q=x HTTP/1.1\r\nhost: x\r\n\r\n").await;
        assert!(
            response.starts_with("HTTP/1.1 503 Service Unavailable"),
            "{response}"
        );

        let response = request(addr, "POST /health HTTP/1.1\r\nhost: x\r\n\r\n").await;
        assert!(
            response.starts_with("HTTP/1.1 503 Service Unavailable"),
            "{response}"
        );

        // Handover returns the same socket.
        let listener = probe.into_listener().await.unwrap();
        assert_eq!(listener.local_addr().unwrap(), addr);
    }
}
