//! A **quicmix node** — one binary, both roles, no flag.
//!
//! Every quicmix node is simultaneously:
//! - a **gateway** for other nodes — it terminates a peer's QUIC stream (arriving
//!   over the mix substrate) and egresses to the requested target, and
//! - a local **ingress** HTTP proxy for its own apps — it carries them over the
//!   mix to a *peer* node's gateway.
//!
//! There is **no separate `gateway` binary and no config switch**. A peer is just
//! another quicmix node. Crucially, both roles use the **same oracle-fed
//! transport** ([`crate::client::Congestion::Quicmix`]) — which is exactly what
//! lets congestion control work *across* the mix: the two endpoints of the QUIC
//! connection are both quicmix nodes running the identical transport.

use crate::client::Congestion;
use crate::OracleParams;
use anyhow::Result;
use quinn::{ClientConfig, Connection, Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

fn make_cert() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
    let c = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert = c.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(c.key_pair.serialize_der()));
    Ok((cert, key))
}

/// A live client link to a peer node's gateway (the connection plus the endpoint
/// that owns it, kept alive together).
pub struct Link {
    pub endpoint: Endpoint,
    pub conn: Arc<Connection>,
}

/// One quicmix node. Holds its identity (cert) and the single oracle-fed
/// transport used for *both* the gateway and ingress roles.
pub struct Node {
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
    transport: Arc<TransportConfig>,
}

impl Node {
    /// Build a node whose transport is tuned to the measured mix (`oracle`), using
    /// the full quicmix oracle-fed congestion control.
    pub fn new(oracle: OracleParams) -> Result<Self> {
        Self::with_congestion(oracle, Congestion::Quicmix)
    }

    /// Build a node with an explicit congestion plan (for A/B comparisons over a
    /// real substrate: `Stock` vs `Quicmix`). Both ends of a connection should use
    /// the same plan.
    pub fn with_congestion(oracle: OracleParams, cc: Congestion) -> Result<Self> {
        let (cert, key) = make_cert()?;
        Ok(Self {
            cert,
            key,
            transport: cc.transport(&oracle),
        })
    }

    /// This node's gateway identity, which peers trust to connect to it.
    pub fn cert(&self) -> CertificateDer<'static> {
        self.cert.clone()
    }

    /// **Gateway role.** Accept peers' QUIC connections; each bi stream begins
    /// with a `"host:port\n"` target, which this node dials and splices. Always
    /// on — this is what makes the node usable as someone else's exit.
    pub async fn serve_gateway(&self) -> Result<(Endpoint, SocketAddr)> {
        let mut sc = ServerConfig::with_single_cert(vec![self.cert.clone()], self.key.clone_key())?;
        sc.transport_config(self.transport.clone());
        let endpoint = Endpoint::server(sc, "127.0.0.1:0".parse()?)?;
        let addr = endpoint.local_addr()?;
        let ep = endpoint.clone();
        tokio::spawn(async move {
            while let Some(incoming) = ep.accept().await {
                tokio::spawn(async move {
                    let Ok(conn) = incoming.await else { return };
                    while let Ok((send, recv)) = conn.accept_bi().await {
                        tokio::spawn(async move {
                            let _ = gateway_stream(send, recv).await;
                        });
                    }
                });
            }
        });
        Ok((endpoint, addr))
    }

    /// **Client role.** Open a quicmix QUIC connection to a peer node's gateway,
    /// reachable at `front` through the mix substrate, trusting `peer_cert`. Same
    /// oracle-fed transport as the gateway side. (Pre-establishing this is the
    /// "session warming" — keep a `rotation::CircuitPool` of these ready.)
    pub async fn connect(&self, front: SocketAddr, peer_cert: CertificateDer<'static>) -> Result<Link> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(peer_cert)?;
        let mut cc = ClientConfig::with_root_certificates(Arc::new(roots))?;
        cc.transport_config(self.transport.clone());
        let mut endpoint = Endpoint::client("127.0.0.1:0".parse()?)?;
        endpoint.set_default_client_config(cc);
        let conn = Arc::new(endpoint.connect(front, "localhost")?.await?);
        Ok(Link { endpoint, conn })
    }

    /// **Ingress role.** Run a local HTTP proxy on `listen`, carrying traffic over
    /// `conn` (a connection to a peer's gateway from [`Node::connect`]).
    pub async fn serve_ingress(&self, listen: &str, conn: Arc<Connection>) -> Result<SocketAddr> {
        let l = TcpListener::bind(listen).await?;
        let addr = l.local_addr()?;
        tokio::spawn(async move {
            while let Ok((sock, _)) = l.accept().await {
                let conn = conn.clone();
                tokio::spawn(async move {
                    let _ = handle_client(sock, conn).await;
                });
            }
        });
        Ok(addr)
    }
}

/// Gateway side of one proxied connection: read the target, dial it, splice.
async fn gateway_stream(mut send: SendStream, recv: RecvStream) -> Result<()> {
    let mut br = BufReader::new(recv);
    let mut target = String::new();
    br.read_line(&mut target).await?;
    let target = target.trim().to_string();
    if target.is_empty() {
        return Ok(());
    }
    let tcp = TcpStream::connect(&target).await?;
    let (mut tr, mut tw) = tcp.into_split();
    let up = async {
        let _ = tokio::io::copy(&mut br, &mut tw).await;
        let _ = tw.shutdown().await;
    };
    let down = async {
        let _ = tokio::io::copy(&mut tr, &mut send).await;
        let _ = send.finish();
    };
    tokio::join!(up, down);
    Ok(())
}

async fn handle_client(sock: TcpStream, conn: Arc<Connection>) -> Result<()> {
    let mut br = BufReader::new(sock);
    let mut request_line = String::new();
    br.read_line(&mut request_line).await?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let uri = parts.next().unwrap_or("").to_string();

    if method.eq_ignore_ascii_case("CONNECT") {
        drain_headers(&mut br).await?;
        br.get_mut()
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await?;
        let (mut send, recv) = conn.open_bi().await?;
        send.write_all(format!("{uri}\n").as_bytes()).await?;
        splice_client(br, send, recv).await
    } else if let Some((host, port, path)) = parse_absolute(&uri) {
        let (mut send, recv) = conn.open_bi().await?;
        send.write_all(format!("{host}:{port}\n").as_bytes()).await?;
        send.write_all(format!("{method} {path} HTTP/1.0\r\nHost: {host}\r\n").as_bytes())
            .await?;
        forward_headers(&mut br, &mut send).await?;
        send.write_all(b"Connection: close\r\n\r\n").await?;
        splice_client(br, send, recv).await
    } else {
        let _ = br
            .get_mut()
            .write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            .await;
        Ok(())
    }
}

async fn splice_client(client: BufReader<TcpStream>, mut send: SendStream, mut recv: RecvStream) -> Result<()> {
    let (mut cr, mut cw) = tokio::io::split(client);
    let up = async {
        let _ = tokio::io::copy(&mut cr, &mut send).await;
        let _ = send.finish();
    };
    let down = async {
        let _ = tokio::io::copy(&mut recv, &mut cw).await;
        let _ = cw.shutdown().await;
    };
    tokio::join!(up, down);
    Ok(())
}

async fn drain_headers(br: &mut BufReader<TcpStream>) -> Result<()> {
    let mut line = String::new();
    loop {
        line.clear();
        let n = br.read_line(&mut line).await?;
        if n == 0 || line == "\r\n" || line == "\n" {
            break;
        }
    }
    Ok(())
}

async fn forward_headers(br: &mut BufReader<TcpStream>, send: &mut SendStream) -> Result<()> {
    let mut line = String::new();
    loop {
        line.clear();
        let n = br.read_line(&mut line).await?;
        if n == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("proxy-") || lower.starts_with("connection:") {
            continue;
        }
        send.write_all(line.as_bytes()).await?;
    }
    Ok(())
}

fn parse_absolute(uri: &str) -> Option<(String, u16, String)> {
    let rest = uri.strip_prefix("http://")?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(80)),
        None => (authority.to_string(), 80),
    };
    Some((host, port, path.to_string()))
}
