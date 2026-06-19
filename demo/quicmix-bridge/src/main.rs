//! quicmix live-demo bridge.
//!
//! A browser cannot route `fetch()` through a proxy or speak quicmix's QUIC, so the
//! github-pages globe can't make a real connection on its own. This bridge is the
//! server-side half that makes it real: a websocket service that, per request,
//!
//! 1. opens a **real** quicmix ingress link to the chosen live gateway (`connect_via`),
//! 2. runs `ingress::serve` over it (the same proxy the cli `ingress_serve` uses),
//! 3. drives the fetch with the box's own `curl` through that proxy, and
//! 4. samples the **real** quinn connection stats (rtt, cwnd, lost/sent, bytes) every
//!    150 ms and streams them — plus logs and the verified egress ip — to the browser.
//!
//! Nothing here is simulated: the bytes traverse a real QUIC connection to a real
//! gateway droplet that egresses at its own ip. Run behind Caddy (auto-TLS) so the
//! https page can reach it over `wss://`.

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use quicmix::node::Node;
use quicmix::OracleParams;
use rustls::pki_types::CertificateDer;
use serde::Deserialize;
use serde_json::json;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;

/// The live gateways the demo can route through (verified up). Cert path comes from
/// `QUICMIX_GW_<ID>_CERT`, defaulting to `./certs/<id>.cert`.
const GATEWAYS: &[(&str, &str)] = &[
    ("fra1", "64.226.93.43:4433"),
    ("nyc3", "68.183.148.148:4433"),
];

fn gateway(id: &str) -> Result<(SocketAddr, CertificateDer<'static>)> {
    let (_, default_addr) = GATEWAYS
        .iter()
        .find(|(g, _)| *g == id)
        .ok_or_else(|| anyhow!("unknown gateway {id:?}"))?;
    let up = id.to_uppercase();
    // Addr + cert path are overridable (e.g. on the droplet, route fra1 over loopback).
    let addr = std::env::var(format!("QUICMIX_GW_{up}_ADDR")).unwrap_or_else(|_| default_addr.to_string());
    let path = std::env::var(format!("QUICMIX_GW_{up}_CERT")).unwrap_or_else(|_| format!("certs/{id}.cert"));
    let bytes = std::fs::read(&path).map_err(|e| anyhow!("cert {path}: {e}"))?;
    Ok((addr.parse()?, CertificateDer::from(bytes)))
}

#[derive(Deserialize)]
struct ReqMsg {
    url: String,
    gateway: String,
}

/// Block the obvious SSRF targets — the fetch egresses at the gateway, so loopback /
/// private / cloud-metadata hosts must not be reachable through the bridge.
fn url_is_allowed(raw: &str) -> Result<url::Url> {
    let u = url::Url::parse(raw).map_err(|_| anyhow!("not a url"))?;
    if !matches!(u.scheme(), "http" | "https") {
        return Err(anyhow!("only http/https"));
    }
    let host = u.host_str().ok_or_else(|| anyhow!("no host"))?.to_lowercase();
    let blocked = host == "localhost"
        || host.ends_with(".local")
        || host == "metadata.google.internal"
        || host.starts_with("127.")
        || host.starts_with("10.")
        || host.starts_with("192.168.")
        || host.starts_with("169.254.")
        || host.starts_with("::1")
        || (host.starts_with("172.")
            && host
                .split('.')
                .nth(1)
                .and_then(|o| o.parse::<u8>().ok())
                .map(|o| (16..=31).contains(&o))
                .unwrap_or(false));
    if blocked {
        return Err(anyhow!("host not allowed"));
    }
    Ok(u)
}

fn oracle() -> OracleParams {
    OracleParams {
        hops: 1,
        mean_hop_delay: Duration::from_millis(30),
        drop_prob: 0.0,
        slot_interval: Duration::ZERO,
        mtu: 1200,
    }
}

type Ws = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

async fn emit(ws: &mut Ws, v: serde_json::Value) -> Result<()> {
    ws.send(Message::Text(v.to_string())).await?;
    Ok(())
}

/// curl through the local proxy; returns trimmed stdout.
async fn curl_through(proxy: &str, url: &str, extra: &[&str]) -> Result<String> {
    let out = Command::new("curl")
        .args(["-sS", "--max-time", "30", "--max-filesize", "26214400", "-x", proxy])
        .args(extra)
        .arg(url)
        .output()
        .await?;
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

async fn run_trace(ws: &mut Ws, req: ReqMsg) -> Result<()> {
    let url = url_is_allowed(&req.url)?;
    let url = url.to_string();
    let (addr, cert) = gateway(&req.gateway)?;
    emit(ws, json!({"type":"log","msg":format!("connecting to gateway {} ({addr}) over quic", req.gateway)})).await?;

    // 1) real quicmix link to the live gateway
    let node = Node::new(oracle())?;
    let t0 = Instant::now();
    let link = node.connect_via("0.0.0.0:0".parse()?, addr, cert).await?;
    let handshake_ms = t0.elapsed().as_secs_f64() * 1e3;
    let conn = link.conn.clone();
    emit(ws, json!({"type":"connected","gateway":req.gateway,"addr":addr.to_string(),"handshake_ms":handshake_ms})).await?;

    // 2) the proxy bound to that quic connection
    let proxy_addr = quicmix::ingress::serve("127.0.0.1:0", conn.clone()).await?;
    let proxy = format!("http://{proxy_addr}");

    // 3) verify the egress ip is the gateway's (proves it really tunneled)
    if let Ok(ip) = curl_through(&proxy, "http://checkip.amazonaws.com", &[]).await {
        if !ip.is_empty() {
            emit(ws, json!({"type":"egress","ip":ip})).await?;
        }
    }

    // 4) sample the real connection stats while the fetch runs
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<serde_json::Value>();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let conn2 = conn.clone();
    let sample_task = tokio::spawn(async move {
        let started = Instant::now();
        let mut last_rx = 0u64;
        let mut last_t = started;
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                _ = tokio::time::sleep(Duration::from_millis(150)) => {
                    let s = conn2.stats();
                    let now = Instant::now();
                    let rxb = s.udp_rx.bytes;
                    let dt = now.duration_since(last_t).as_secs_f64().max(1e-3);
                    let bps = ((rxb.saturating_sub(last_rx)) as f64 / dt) as u64;
                    last_rx = rxb; last_t = now;
                    let _ = tx.send(json!({
                        "type":"sample",
                        "t_ms": now.duration_since(started).as_secs_f64()*1e3,
                        "rtt_ms": s.path.rtt.as_secs_f64()*1e3,
                        "cwnd": s.path.cwnd,
                        "sent_packets": s.path.sent_packets,
                        "lost_packets": s.path.lost_packets,
                        "rx_bytes": rxb,
                        "throughput_bps": bps,
                    }));
                }
            }
        }
    });

    // 5) the real fetch (curl handles tls/http; -w gives the verdict line)
    emit(ws, json!({"type":"log","msg":format!("GET {url}")})).await?;
    let fetch = curl_through(
        &proxy,
        &url,
        &["-L", "-o", "/dev/null", "-w",
          "{\"code\":%{http_code},\"bytes\":%{size_download},\"speed\":%{speed_download},\"ttfb\":%{time_starttransfer},\"total\":%{time_total}}"],
    );
    tokio::pin!(fetch);
    let verdict = loop {
        tokio::select! {
            v = &mut fetch => break v,
            Some(sample) = rx.recv() => { emit(ws, sample).await.ok(); }
        }
    };
    let _ = stop_tx.send(());
    let _ = sample_task.await;
    while let Ok(sample) = rx.try_recv() { emit(ws, sample).await.ok(); }

    let s = conn.stats();
    let mut done = json!({
        "type":"done",
        "rtt_ms": s.path.rtt.as_secs_f64()*1e3,
        "sent_packets": s.path.sent_packets,
        "lost_packets": s.path.lost_packets,
        "rx_bytes": s.udp_rx.bytes,
        "tx_bytes": s.udp_tx.bytes,
    });
    if let Ok(v) = verdict {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&v) {
            done["fetch"] = parsed;
        } else {
            done["fetch_raw"] = json!(v);
        }
    }
    emit(ws, done).await?;
    drop(link); // close the circuit
    Ok(())
}

async fn handle(stream: tokio::net::TcpStream) {
    let allowed_origin = |o: &str| {
        o.ends_with("github.io")
            || o.starts_with("http://localhost")
            || o.starts_with("http://127.0.0.1")
            || o.starts_with("https://localhost")
    };
    let mut ok_origin = true;
    let cb = |req: &Request, resp: Response| {
        if let Some(o) = req.headers().get("origin").and_then(|v| v.to_str().ok()) {
            if !allowed_origin(o) {
                ok_origin = false;
            }
        }
        Ok(resp)
    };
    let mut ws = match tokio_tungstenite::accept_hdr_async(stream, cb).await {
        Ok(ws) => ws,
        Err(_) => return,
    };
    if !ok_origin {
        let _ = ws.close(None).await;
        return;
    }
    // first message: the request
    let first = match ws.next().await {
        Some(Ok(Message::Text(t))) => t,
        _ => return,
    };
    let req: ReqMsg = match serde_json::from_str(&first) {
        Ok(r) => r,
        Err(e) => {
            let _ = emit(&mut ws, json!({"type":"error","msg":format!("bad request: {e}")})).await;
            return;
        }
    };
    if let Err(e) = run_trace(&mut ws, req).await {
        let _ = emit(&mut ws, json!({"type":"error","msg":e.to_string()})).await;
    }
    let _ = ws.close(None).await;
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider().install_default().ok();
    let listen = std::env::var("BRIDGE_LISTEN").unwrap_or_else(|_| "127.0.0.1:9000".into());
    let listener = TcpListener::bind(&listen).await?;
    eprintln!("quicmix-bridge ws on {listen} (gateways: fra1, nyc3)");
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(handle(stream));
    }
}
