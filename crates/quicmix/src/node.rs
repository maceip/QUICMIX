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
use tokio::net::TcpStream;

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
        self.serve_gateway_at("127.0.0.1:0".parse()?).await
    }

    /// **Gateway role, bound to `bind`.** Same as [`Node::serve_gateway`] but binds
    /// the QUIC server to an explicit address (e.g. `0.0.0.0:4433` for a real
    /// cross-machine deployment), not just loopback.
    pub async fn serve_gateway_at(&self, bind: SocketAddr) -> Result<(Endpoint, SocketAddr)> {
        let mut sc = ServerConfig::with_single_cert(vec![self.cert.clone()], self.key.clone_key())?;
        sc.transport_config(self.transport.clone());
        let endpoint = Endpoint::server(sc, bind)?;
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
        self.connect_via("127.0.0.1:0".parse()?, front, peer_cert).await
    }

    /// **Client role, bound to `bind`.** Same as [`Node::connect`] but binds the
    /// local QUIC client socket to an explicit address — use `0.0.0.0:0` to reach a
    /// gateway on a public IP (the loopback bind in [`Node::connect`] can only talk
    /// to `127.0.0.1`).
    pub async fn connect_via(
        &self,
        bind: SocketAddr,
        front: SocketAddr,
        peer_cert: CertificateDer<'static>,
    ) -> Result<Link> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(peer_cert)?;
        let mut cc = ClientConfig::with_root_certificates(Arc::new(roots))?;
        cc.transport_config(self.transport.clone());
        let mut endpoint = Endpoint::client(bind)?;
        endpoint.set_default_client_config(cc);
        let conn = Arc::new(endpoint.connect(front, "localhost")?.await?);
        Ok(Link { endpoint, conn })
    }

    /// **Ingress role.** Run the local HTTP(S) proxy on `listen`, carrying traffic
    /// over `conn`. Delegates to the single hyper-based ingress
    /// ([`crate::ingress::serve`]) — one proxy code path everywhere.
    pub async fn serve_ingress(&self, listen: &str, conn: Arc<Connection>) -> Result<SocketAddr> {
        crate::ingress::serve(listen, conn).await
    }
}

/// Sentinel target for the self-sourced bandwidth probe (the live-metrics demo).
/// `"@measure <bytes>\n"` asks the gateway to stream `<bytes>` of throwaway data
/// back over this QUIC stream instead of splicing to a TCP target — so the demo
/// measures the **quicmix link itself**, not a third-party mirror's bandwidth.
const MEASURE_PREFIX: &str = "@measure ";
const MEASURE_MAX: u64 = 64 * 1024 * 1024;

/// Gateway side of one proxied connection: read the target, dial it, splice.
async fn gateway_stream(mut send: SendStream, recv: RecvStream) -> Result<()> {
    let mut br = BufReader::new(recv);
    let mut target = String::new();
    br.read_line(&mut target).await?;
    let target = target.trim().to_string();
    if target.is_empty() {
        return Ok(());
    }
    if let Some(rest) = target.strip_prefix(MEASURE_PREFIX) {
        let n = rest.trim().parse::<u64>().unwrap_or(0).min(MEASURE_MAX);
        let chunk = vec![0u8; 64 * 1024];
        let mut left = n;
        while left > 0 {
            let k = left.min(chunk.len() as u64) as usize;
            if send.write_all(&chunk[..k]).await.is_err() {
                break;
            }
            left -= k as u64;
        }
        let _ = send.finish();
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
