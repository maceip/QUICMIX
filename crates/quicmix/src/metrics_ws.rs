//! **Live-metrics websocket** — the demo's *real* data source (opt-in).
//!
//! This is **not** a separate "bridge" binary or a demo crate: it is an optional
//! capability of the node itself, compiled in behind the `metrics-ws` feature and
//! enabled only when [`crate::autopilot::Config::metrics_ws_bind`] is set
//! (`QUICMIX_METRICS_WS_BIND`). A browser cannot speak QUIC or read quinn's
//! congestion state, so the GitHub-Pages demo asks *this* node — which is a real
//! quicmix client — to actually run the request and report what happened.
//!
//! Per websocket connection it performs ONE real run against the chosen gateway:
//! 1. a genuine QUIC handshake (timed),
//! 2. a real **HTTP(S) fetch of the requested URL carried over the quicmix link**
//!    (the gateway TCP-splices to the origin; for `https` we run a TLS client
//!    *over the QUIC stream*, so the page bytes truly cross quicmix), and
//! 3. a sustained bandwidth probe over the same link for live throughput.
//!
//! Frames (server → client): `connected`, `egress`, `page` (the real fetched
//! HTML), `sample` (live quinn `PathStats` + goodput), `done`, `error`. Nothing is
//! synthesised: every number comes off the live `quinn::Connection` or the bytes
//! that actually crossed it.

use crate::node::Node;
use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use quinn::Connection;
use rustls::pki_types::CertificateDer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// Bound the bandwidth probe so a run stays short.
const PROBE_CAP_BYTES: u64 = 4 * 1024 * 1024;
const PROBE_CAP_TIME: Duration = Duration::from_secs(5);
/// Cap the fetched page we ship to the browser (the document only; sub-resources
/// load in the browser via an injected `<base>`).
const PAGE_CAP: usize = 768 * 1024;

/// Serve the live-metrics websocket on `bind` (plain ws; front it with caddy/TLS
/// for `wss://`). Each connection drives one real quicmix run via `node`.
pub async fn serve(bind: SocketAddr, node: Arc<Node>) -> Result<()> {
    let listener = TcpListener::bind(bind).await?;
    println!("metrics-ws: live stats websocket on ws://{bind}");
    tokio::spawn(async move {
        loop {
            let Ok((stream, peer)) = listener.accept().await else { continue };
            let node = node.clone();
            tokio::spawn(async move {
                if let Err(e) = handle(stream, node).await {
                    eprintln!("metrics-ws: run for {peer} ended: {e}");
                }
            });
        }
    });
    Ok(())
}

async fn handle(stream: tokio::net::TcpStream, node: Arc<Node>) -> Result<()> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let (mut tx, mut rx) = ws.split();

    let first = match rx.next().await {
        Some(Ok(Message::Text(t))) => t.to_string(),
        Some(Ok(Message::Close(_))) | None => return Ok(()),
        Some(Ok(_)) => return Ok(()),
        Some(Err(e)) => return Err(e.into()),
    };
    let req: serde_json::Value = serde_json::from_str(&first).map_err(|e| anyhow!("bad request json: {e}"))?;
    let gateway = req.get("gateway").and_then(|v| v.as_str()).unwrap_or("");
    let url = req.get("url").and_then(|v| v.as_str()).unwrap_or("https://example.com").to_string();
    let substrate = req.get("substrate").and_then(|v| v.as_str()).unwrap_or("direct").to_ascii_lowercase();
    let gw_addr = resolve_gateway(gateway)?;

    if let Err(e) = run_measure(&node, gw_addr, &url, &substrate, &mut tx).await {
        let _ = send_json(&mut tx, serde_json::json!({"type":"error","msg": e.to_string()})).await;
    }
    let _ = tx.send(Message::Close(None)).await;
    Ok(())
}

/// Accept a gateway as `ip`, `ip:port`, or the well-known demo ids; default port 4433.
fn resolve_gateway(s: &str) -> Result<SocketAddr> {
    let s = s.trim();
    let mapped = match s {
        "fra1" => "64.226.93.43:4433",
        "nyc3" => "68.183.148.148:4433",
        other => other,
    };
    if let Ok(a) = mapped.parse::<SocketAddr>() {
        return Ok(a);
    }
    if let Ok(ip) = mapped.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, 4433));
    }
    Err(anyhow!("unrecognised gateway {s:?}"))
}

type Sink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    Message,
>;

async fn send_json(tx: &mut Sink, v: serde_json::Value) -> Result<()> {
    tx.send(Message::text(v.to_string())).await?;
    Ok(())
}

/// Tor bridge port on the gateway (the [`crate::stream_bridge`] TCP listener).
/// Override via `QUICMIX_TOR_BRIDGE_PORT`; default 8443 (Tor default exit policy).
fn tor_bridge_port() -> u16 {
    std::env::var("QUICMIX_TOR_BRIDGE_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(8443)
}

/// Oracle timing model for the Tor leg (3 relays; uncapped ordered stream).
fn tor_oracle() -> crate::OracleParams {
    crate::OracleParams {
        hops: 3,
        mean_hop_delay: Duration::from_millis(100),
        drop_prob: 0.0,
        slot_interval: Duration::ZERO,
        mtu: 1200,
    }
}

/// Run one real measurement against `gw_addr` over a fresh quicmix link, carried
/// over the selected `substrate` (`direct` = one internet hop; `tor` = a real Tor
/// circuit via the native Tor SOCKS proxy to the gateway's stream bridge).
async fn run_measure(node: &Node, gw_addr: SocketAddr, url: &str, substrate: &str, tx: &mut Sink) -> Result<()> {
    // 1) real QUIC handshake to the chosen gateway (cert pinned via cert-over-HTTP).
    let t0 = Instant::now();
    let cert = crate::autopilot::fetch_cert(gw_addr).await.map_err(|e| anyhow!("fetch cert {gw_addr}: {e}"))?;
    let peer = CertificateDer::from(cert);

    // Resolve the UDP front the quinn client dials. For `direct` it's the gateway's
    // public addr; for `tor` we open a real Tor circuit to the gateway's stream
    // bridge and bridge it to a local UDP front (held alive via `_front_guard`).
    let (dial_addr, _front_guard, substrate_label) = match substrate {
        "tor" => {
            let socks = crate::tor::tor_socks_addr();
            if !crate::tor::tor_socks_reachable(socks).await {
                return Err(anyhow!("native Tor SOCKS not reachable at {socks} (is the tor service running?)"));
            }
            let bridge = format!("{}:{}", gw_addr.ip(), tor_bridge_port());
            let sub = crate::tor::tor_stream_to(socks, &bridge, tor_oracle())
                .await
                .map_err(|e| anyhow!("tor circuit to {bridge}: {e}"))?;
            let (front, boundary) = crate::front::spawn_substrate_front(std::sync::Arc::new(sub)).await?;
            (front, Some(boundary), format!("tor (3-hop) → quicmix gateway {}", gw_addr.ip()))
        }
        _ => (gw_addr, None, "direct quicmix QUIC · 1 hop".to_string()),
    };

    let link = node
        .connect_via("0.0.0.0:0".parse().unwrap(), dial_addr, peer)
        .await
        .map_err(|e| anyhow!("connect {dial_addr} ({substrate_label}): {e}"))?;
    let conn = link.conn.clone();
    let handshake_ms = t0.elapsed().as_secs_f64() * 1000.0;
    send_json(tx, serde_json::json!({"type":"connected","handshake_ms": handshake_ms, "substrate": substrate_label})).await?;

    // 2) egress = the verified exit: the gateway makes the TCP connection to the
    //    origin, so the origin sees the gateway's IP. A 1-hop path exits there, and
    //    the real fetch below confirms reachability through it.
    let egress_ip = gw_addr.ip().to_string();
    send_json(tx, serde_json::json!({"type":"egress","ip": egress_ip})).await?;

    // 3) REAL fetch of the requested URL, carried over the quicmix link.
    let mut page_code: u16 = 0;
    let mut page_bytes: u64 = 0;
    let mut page_ttfb = 0.0;
    match fetch_over_quicmix(&conn, url, 0).await {
        Ok(f) => {
            page_code = f.code;
            page_bytes = f.bytes;
            page_ttfb = f.ttfb_ms;
            let html = if f.content_type.contains("text/html") {
                Some(String::from_utf8_lossy(&f.body).chars().take(PAGE_CAP).collect::<String>())
            } else {
                None
            };
            send_json(
                tx,
                serde_json::json!({"type":"page","code": f.code,"bytes": f.bytes,"url": f.final_url,"html": html}),
            )
            .await?;
        }
        Err(e) => {
            send_json(tx, serde_json::json!({"type":"page","code": 0,"error": e.to_string(),"url": url})).await?;
        }
    }

    // 4) bandwidth probe over the SAME quicmix link (self-sourced from the gateway,
    //    no third-party mirror) → real, sustained throughput + live samples.
    let probe_bytes = probe_size();
    let (mut s, mut r) = conn.open_bi().await?;
    s.write_all(format!("@measure {probe_bytes}\n").as_bytes()).await?;
    let _ = s.finish();

    let mut buf = vec![0u8; 64 * 1024];
    let mut body_bytes: u64 = 0;
    let start = Instant::now();
    let mut first_byte: Option<Instant> = None;
    let mut last_sample = Instant::now();
    loop {
        let n = match tokio::time::timeout(Duration::from_secs(6), r.read(&mut buf)).await {
            Ok(Ok(Some(n))) => n,
            Ok(Ok(None)) => break,
            Ok(Err(_)) | Err(_) => break,
        };
        if first_byte.is_none() {
            first_byte = Some(Instant::now());
        }
        body_bytes += n as u64;
        let stable = first_byte.map(|f| f.elapsed() >= Duration::from_millis(120)).unwrap_or(false);
        if stable && last_sample.elapsed() >= Duration::from_millis(250) {
            last_sample = Instant::now();
            send_json(tx, sample_frame(&conn, body_bytes, first_byte)).await?;
        }
        if body_bytes >= PROBE_CAP_BYTES || start.elapsed() >= PROBE_CAP_TIME {
            break;
        }
    }

    let st = conn.stats();
    let secs = first_byte
        .map(|f| f.elapsed().as_secs_f64())
        .unwrap_or_else(|| start.elapsed().as_secs_f64())
        .max(1e-3);
    let speed = body_bytes as f64 / secs;
    let total_ms = start.elapsed().as_secs_f64() * 1000.0;

    send_json(
        tx,
        serde_json::json!({
            "type":"done",
            "rtt_ms": st.path.rtt.as_secs_f64() * 1000.0,
            "lost_packets": st.path.lost_packets,
            "sent_packets": st.path.sent_packets,
            "fetch": {
                "code": page_code,
                "bytes": page_bytes,
                "ttfb": page_ttfb,
                "total": total_ms,
                "speed": speed,
            }
        }),
    )
    .await?;
    Ok(())
}

fn sample_frame(conn: &Connection, body_bytes: u64, first_byte: Option<Instant>) -> serde_json::Value {
    let st = conn.stats();
    let secs = first_byte.map(|f| f.elapsed().as_secs_f64()).unwrap_or(0.0).max(1e-3);
    serde_json::json!({
        "type":"sample",
        "rtt_ms": st.path.rtt.as_secs_f64() * 1000.0,
        "throughput_bps": body_bytes as f64 / secs,
        "cwnd": st.path.cwnd,
        "lost_packets": st.path.lost_packets,
        "sent_packets": st.path.sent_packets,
    })
}

struct Fetched {
    code: u16,
    bytes: u64,
    ttfb_ms: f64,
    body: Vec<u8>,
    content_type: String,
    final_url: String,
}

/// Fetch `url` for real, **carried over the quicmix link** `conn`. The gateway
/// TCP-splices to the origin; for `https` we run a TLS client directly over the
/// QUIC stream so the page bytes genuinely traverse quicmix. Follows up to 3
/// redirects. This is the demo's proof that traffic really rides the tunnel.
async fn fetch_over_quicmix(conn: &Connection, url: &str, depth: u8) -> Result<Fetched> {
    if depth > 3 {
        return Err(anyhow!("too many redirects"));
    }
    let (scheme, host, port, path) = split_url(url)?;
    let (mut s, mut r) = conn.open_bi().await?;
    s.write_all(format!("{host}:{port}\n").as_bytes()).await?;
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: quicmix-demo/0.1\r\nAccept: text/html,*/*\r\nAccept-Encoding: identity\r\nConnection: close\r\n\r\n"
    );
    let start = Instant::now();
    let (raw, ttfb_ms) = if scheme == "https" {
        let stream = tokio::io::join(r, s);
        let sni = rustls::pki_types::ServerName::try_from(host.clone()).map_err(|_| anyhow!("bad sni {host}"))?;
        let mut tls = tls_connector().connect(sni, stream).await.map_err(|e| anyhow!("tls {host}: {e}"))?;
        tls.write_all(req.as_bytes()).await?;
        slurp(&mut tls, start, PAGE_CAP + 8192).await?
    } else {
        s.write_all(req.as_bytes()).await?;
        let _ = s.finish();
        slurp(&mut r, start, PAGE_CAP + 8192).await?
    };

    let (code, headers, body) = parse_response(&raw)?;
    // follow redirects
    if (300..400).contains(&code) {
        if let Some(loc) = header_value(&headers, "location") {
            let next = resolve_location(scheme, &host, &loc);
            return Box::pin(fetch_over_quicmix(conn, &next, depth + 1)).await;
        }
    }
    let content_type = header_value(&headers, "content-type").unwrap_or_default();
    let body = if header_value(&headers, "transfer-encoding").map(|v| v.contains("chunked")).unwrap_or(false) {
        dechunk(&body)
    } else {
        body
    };
    Ok(Fetched {
        code,
        bytes: body.len() as u64,
        ttfb_ms,
        body,
        content_type,
        final_url: url.to_string(),
    })
}

fn tls_connector() -> tokio_rustls::TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    tokio_rustls::TlsConnector::from(Arc::new(config))
}

async fn slurp<S: AsyncReadExt + Unpin>(s: &mut S, start: Instant, cap: usize) -> Result<(Vec<u8>, f64)> {
    let mut data = Vec::new();
    let mut buf = vec![0u8; 32 * 1024];
    let mut ttfb = 0.0;
    loop {
        match tokio::time::timeout(Duration::from_secs(10), s.read(&mut buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => {
                if data.is_empty() {
                    ttfb = start.elapsed().as_secs_f64() * 1000.0;
                }
                data.extend_from_slice(&buf[..n]);
                if data.len() >= cap {
                    break;
                }
            }
            Ok(Err(_)) | Err(_) => break,
        }
    }
    Ok((data, ttfb))
}

/// (scheme, host, port, path)
fn split_url(url: &str) -> Result<(&'static str, String, u16, String)> {
    let (scheme, rest) = if let Some(r) = url.strip_prefix("https://") {
        ("https", r)
    } else if let Some(r) = url.strip_prefix("http://") {
        ("http", r)
    } else {
        ("https", url)
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(if scheme == "https" { 443 } else { 80 })),
        None => (authority.to_string(), if scheme == "https" { 443 } else { 80 }),
    };
    if host.is_empty() {
        return Err(anyhow!("empty host in {url:?}"));
    }
    let path = if path.is_empty() { "/".to_string() } else { path.to_string() };
    Ok((scheme, host, port, path))
}

fn resolve_location(scheme: &str, host: &str, loc: &str) -> String {
    if loc.starts_with("http://") || loc.starts_with("https://") {
        loc.to_string()
    } else if let Some(stripped) = loc.strip_prefix('/') {
        format!("{scheme}://{host}/{stripped}")
    } else {
        format!("{scheme}://{host}/{loc}")
    }
}

/// (status_code, header_block_lowercased_lines, body_bytes)
fn parse_response(raw: &[u8]) -> Result<(u16, Vec<(String, String)>, Vec<u8>)> {
    let sep = find_crlfcrlf(raw).ok_or_else(|| anyhow!("no header terminator in response"))?;
    let head = &raw[..sep];
    let body = raw[sep + 4..].to_vec();
    let head_str = String::from_utf8_lossy(head);
    let mut lines = head_str.split("\r\n");
    let status = lines.next().unwrap_or("");
    let code = status.split_whitespace().nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
    let mut headers = Vec::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
    }
    Ok((code, headers, body))
}

fn header_value(headers: &[(String, String)], name: &str) -> Option<String> {
    headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.to_ascii_lowercase())
}

fn dechunk(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < body.len() {
        let line_end = match body[i..].windows(2).position(|w| w == b"\r\n") {
            Some(p) => i + p,
            None => break,
        };
        let size_str = String::from_utf8_lossy(&body[i..line_end]);
        let size = usize::from_str_radix(size_str.trim().split(';').next().unwrap_or("0").trim(), 16).unwrap_or(0);
        if size == 0 {
            break;
        }
        let data_start = line_end + 2;
        let data_end = (data_start + size).min(body.len());
        out.extend_from_slice(&body[data_start..data_end]);
        i = data_end + 2; // skip trailing CRLF
    }
    out
}

fn find_crlfcrlf(b: &[u8]) -> Option<usize> {
    b.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Size of the self-sourced bandwidth probe (bytes). Overridable via
/// `QUICMIX_METRICS_PROBE_BYTES`; capped by [`PROBE_CAP_BYTES`] on read.
fn probe_size() -> u64 {
    std::env::var("QUICMIX_METRICS_PROBE_BYTES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(3_000_000)
}
