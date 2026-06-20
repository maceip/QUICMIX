//! Auto-promotion runtime: **one binary, role chosen by the environment.**
//!
//! There is no `gateway`/`ingress` subcommand and no role flag. On start a node:
//!
//! 1. discovers the IP it egresses on ([`directory::discover_egress_ip`]),
//! 2. if that IP is public/routable ([`directory::qualifies_as_gateway`]) it
//!    **auto-promotes to gateway**: serves QUIC, publishes its cert over a tiny
//!    HTTP endpoint on the same port (TCP), and announces itself into the
//!    [`GatewayDirectory`];
//! 3. seeds the directory from a bootstrap list (well-known IPs, overridable via
//!    `QUICMIX_BOOTSTRAP`) by fetching each peer's cert, and refreshes on a timer;
//! 4. runs the **ingress** role — the local HTTP proxy pointed at a
//!    directory-sampled gateway.
//!
//! Gossip propagation of announcements and active inbound-reachability probing are
//! the documented future work (see [`crate::directory`]); bootstrap here is the
//! baked/`QUICMIX_BOOTSTRAP` list with cert-over-HTTP fetch.

use crate::directory::{self, GatewayDirectory};
use crate::front::{self, SubstrateBuilder, SubstrateChoice};
use crate::node::Node;
use crate::OracleParams;
use anyhow::{anyhow, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Default bootstrap gateways (the verified-live demo nodes). Overridable via
/// `QUICMIX_BOOTSTRAP` (comma-separated `ip:port`).
pub const DEFAULT_BOOTSTRAP: &[&str] = &["64.226.93.43:4433", "68.183.148.148:4433"];

/// How long a directory entry stays live without a refresh.
const DIR_TTL: Duration = Duration::from_secs(120);

/// Runtime config, entirely from the environment (deployment config, not flags).
#[derive(Clone, Debug)]
pub struct Config {
    /// QUIC gateway bind (also the TCP port the cert endpoint serves on).
    pub gw_bind: SocketAddr,
    /// Local ingress HTTP proxy bind.
    pub ingress_bind: String,
    /// Bootstrap gateways to seed the directory from.
    pub bootstrap: Vec<SocketAddr>,
    /// Optional override of the discovered egress IP (e.g. behind cloud NAT where
    /// the public IP isn't bound to the NIC). Forces gateway promotion.
    pub public_ip: Option<std::net::IpAddr>,
    /// Optional path to also persist the gateway cert to (ops visibility).
    pub cert_out: Option<String>,
    /// Which substrate the ingress role carries its QUIC over (default `direct`).
    pub substrate: SubstrateChoice,
    /// Optional bind for the live-metrics websocket (`QUICMIX_METRICS_WS_BIND`,
    /// e.g. `127.0.0.1:8787`). Drives the public demo with *real* quinn stats.
    /// Only honoured when the binary is built with the `metrics-ws` feature.
    pub metrics_ws_bind: Option<SocketAddr>,
    /// Optional TCP bind for the gateway **stream bridge** (`QUICMIX_STREAM_BRIDGE_BIND`,
    /// e.g. `0.0.0.0:8443`). Lets stream substrates (native Tor) reach this gateway:
    /// a Tor exit makes a TCP connection here, datagrams are de-framed into the local
    /// QUIC listener. Only active when this node is a gateway. Pick a Tor-exit-friendly
    /// port (8443 is in Tor's default reduced exit policy).
    pub stream_bridge_bind: Option<SocketAddr>,
}

impl Config {
    /// Build from environment with sensible defaults.
    pub fn from_env() -> Result<Self> {
        let gw_bind = std::env::var("QUICMIX_GW_BIND")
            .unwrap_or_else(|_| "0.0.0.0:4433".into())
            .parse()
            .map_err(|e| anyhow!("QUICMIX_GW_BIND: {e}"))?;
        let ingress_bind = std::env::var("QUICMIX_INGRESS_BIND").unwrap_or_else(|_| "127.0.0.1:8888".into());
        let bootstrap = match std::env::var("QUICMIX_BOOTSTRAP") {
            Ok(s) => s
                .split(',')
                .map(|x| x.trim())
                .filter(|x| !x.is_empty())
                .map(|x| x.parse::<SocketAddr>().map_err(|e| anyhow!("QUICMIX_BOOTSTRAP {x:?}: {e}")))
                .collect::<Result<Vec<_>>>()?,
            Err(_) => DEFAULT_BOOTSTRAP.iter().map(|x| x.parse().unwrap()).collect(),
        };
        let public_ip = match std::env::var("QUICMIX_PUBLIC_IP") {
            Ok(s) if !s.is_empty() => Some(s.parse().map_err(|e| anyhow!("QUICMIX_PUBLIC_IP: {e}"))?),
            _ => None,
        };
        let cert_out = std::env::var("QUICMIX_CERT_OUT").ok().filter(|s| !s.is_empty());
        let substrate = SubstrateChoice::from_env()?;
        let metrics_ws_bind = match std::env::var("QUICMIX_METRICS_WS_BIND") {
            Ok(s) if !s.is_empty() => Some(s.parse().map_err(|e| anyhow!("QUICMIX_METRICS_WS_BIND: {e}"))?),
            _ => None,
        };
        let stream_bridge_bind = match std::env::var("QUICMIX_STREAM_BRIDGE_BIND") {
            Ok(s) if !s.is_empty() => Some(s.parse().map_err(|e| anyhow!("QUICMIX_STREAM_BRIDGE_BIND: {e}"))?),
            _ => None,
        };
        Ok(Self { gw_bind, ingress_bind, bootstrap, public_ip, cert_out, substrate, metrics_ws_bind, stream_bridge_bind })
    }
}

/// The transport timing model for the direct-internet path (one real hop to the
/// peer gateway). Identical params the old `gw_serve`/`ingress_serve` used.
pub fn oracle() -> OracleParams {
    OracleParams {
        hops: 1,
        mean_hop_delay: Duration::from_millis(30),
        drop_prob: 0.0,
        slot_interval: Duration::ZERO,
        mtu: 1200,
    }
}

/// Serve the node's cert over a minimal HTTP/1.0 endpoint (`GET /cert` → DER bytes)
/// on `bind` (TCP). Co-located with the QUIC gateway so a peer that knows the
/// gateway's `ip:port` can fetch the identity it must pin. TOFU for now; signed
/// announcements are future work.
async fn serve_cert_http(bind: SocketAddr, cert: Arc<Vec<u8>>) -> Result<()> {
    let listener = TcpListener::bind(bind).await?;
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else { continue };
            let cert = cert.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = s.read(&mut buf).await; // drain request line; we only serve /cert
                let head = format!(
                    "HTTP/1.0 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    cert.len()
                );
                let _ = s.write_all(head.as_bytes()).await;
                let _ = s.write_all(&cert).await;
                let _ = s.shutdown().await;
            });
        }
    });
    Ok(())
}

/// Fetch a peer gateway's pinned cert from its cert-over-HTTP endpoint (TCP, same
/// `ip:port` as its QUIC gateway).
pub async fn fetch_cert(addr: SocketAddr) -> Result<Vec<u8>> {
    let mut s = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr)).await??;
    s.write_all(format!("GET /cert HTTP/1.0\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes())
        .await?;
    let mut data = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut data)).await??;
    let sep = data
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| anyhow!("cert response had no header terminator"))?;
    let body = data[sep + 4..].to_vec();
    if body.is_empty() {
        return Err(anyhow!("empty cert from {addr}"));
    }
    Ok(body)
}

/// Periodically (re)seed the directory from the bootstrap peers so live gateways
/// stay fresh and crashed ones expire. Skips `self_addr` (already announced).
fn spawn_bootstrap_refresh(dir: Arc<GatewayDirectory>, peers: Vec<SocketAddr>, self_addr: Option<SocketAddr>) {
    tokio::spawn(async move {
        loop {
            for &addr in &peers {
                if Some(addr) == self_addr {
                    continue;
                }
                match fetch_cert(addr).await {
                    Ok(cert) => dir.announce(addr, cert),
                    Err(e) => eprintln!("bootstrap: {addr} unreachable: {e}"),
                }
            }
            tokio::time::sleep(DIR_TTL / 2).await;
        }
    });
}

/// Run the auto-promotion node to completion (until Ctrl-C).
///
/// `substrate_builder` is the optional injected constructor for **linked** (in-process)
/// substrates (e.g. Katzenpost, HOPR), supplied by the composition binary crate that
/// links them. `None` (the lib's default) means only `direct` and sidecar substrates
/// are available — selecting a linked substrate then errors with guidance.
pub async fn run(cfg: Config, substrate_builder: Option<Arc<dyn SubstrateBuilder>>) -> Result<()> {
    rustls::crypto::ring::default_provider().install_default().ok();

    let dir = Arc::new(GatewayDirectory::new(DIR_TTL));
    let node = Arc::new(Node::new(oracle())?);
    let cert = Arc::new(node.cert().as_ref().to_vec());

    // 1) discover egress IP and decide the role.
    let egress = cfg.public_ip.or_else(directory::discover_egress_ip);
    let self_public: Option<SocketAddr> =
        egress.map(|ip| SocketAddr::new(ip, cfg.gw_bind.port())).filter(|a| directory::qualifies_as_gateway(*a));

    // 2) auto-promote to gateway when the egress IP is public.
    let _gw_ep = if let Some(public_addr) = self_public {
        let (ep, bound) = node.serve_gateway_at(cfg.gw_bind).await?;
        serve_cert_http(cfg.gw_bind, cert.clone()).await?;
        dir.announce(public_addr, cert.as_ref().clone());
        if let Some(ref path) = cfg.cert_out {
            let _ = std::fs::write(path, cert.as_ref());
        }
        println!("auto-promoted to GATEWAY · quic+cert on {bound} · public {public_addr}");
        // Optional stream bridge so native-Tor clients can reach this gateway: a Tor
        // exit's TCP connection is de-framed into the local QUIC listener.
        if let Some(br) = cfg.stream_bridge_bind {
            let quic_local = SocketAddr::new(std::net::IpAddr::from([127, 0, 0, 1]), cfg.gw_bind.port());
            if let Err(e) = crate::stream_bridge::serve(br, quic_local).await {
                eprintln!("stream-bridge: failed to start on {br}: {e}");
            }
        }
        Some(ep)
    } else {
        println!("no public IP (egress {egress:?}) — running as INGRESS only");
        None
    };

    // 3) seed + refresh the directory from bootstrap peers.
    spawn_bootstrap_refresh(dir.clone(), cfg.bootstrap.clone(), self_public);

    // 3b) optional live-metrics websocket — the public demo's real data source.
    // It runs *this* node as a quicmix client on demand and streams the real quinn
    // path stats + verified egress. Only present with the `metrics-ws` feature.
    #[cfg(feature = "metrics-ws")]
    if let Some(bind) = cfg.metrics_ws_bind {
        if let Err(e) = crate::metrics_ws::serve(bind, node.clone()).await {
            eprintln!("metrics-ws: failed to start on {bind}: {e}");
        }
    }

    // 4) ingress role: wait for a gateway to appear in the directory, resolve the
    //    chosen substrate's local UDP front, connect over it, and serve.
    let node2 = node.clone();
    let dir2 = dir.clone();
    let ingress_bind = cfg.ingress_bind.clone();
    let substrate = cfg.substrate.clone();
    let builder = substrate_builder.clone();
    println!("ingress substrate: {}", substrate.label());
    tokio::spawn(async move {
        loop {
            if let Some(gw) = dir2.sample() {
                let peer_cert = rustls::pki_types::CertificateDer::from(gw.cert.clone());
                // Resolve the UDP front (gateway addr for `direct`; a sidecar's local
                // socket for a sidecar substrate; a bridged local socket for a linked
                // substrate). Held alongside the link so any spawned sidecar / bridge
                // stays alive for the connection's lifetime.
                let front = match front::resolve(&substrate, gw.addr, builder.as_deref()).await {
                    Ok(f) => f,
                    Err(e) => {
                        eprintln!("ingress: substrate front for {} failed: {e}", gw.addr);
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                };
                match node2
                    .connect_via("0.0.0.0:0".parse().unwrap(), front.addr, peer_cert)
                    .await
                {
                    Ok(link) => {
                        match node2.serve_ingress(&ingress_bind, link.conn.clone()).await {
                            Ok(proxy) => {
                                println!(
                                    "ingress: local proxy http://{proxy} -> gateway {} via {} (front {})",
                                    gw.addr,
                                    substrate.label(),
                                    front.addr
                                );
                                let _keep = link.endpoint; // hold the client endpoint open
                                let _keep_front = front; // keep any spawned sidecar alive
                                tokio::signal::ctrl_c().await.ok();
                                return;
                            }
                            Err(e) => eprintln!("ingress: serve failed: {e}"),
                        }
                    }
                    Err(e) => eprintln!("ingress: connect {} failed: {e}", gw.addr),
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    tokio::signal::ctrl_c().await.ok();
    Ok(())
}
