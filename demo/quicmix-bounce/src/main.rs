//! quicmix bounce — iframe loader service.
//!
//! The github-pages globe demo finishes a "fetch over the mixnet" by *showing* the requested
//! page in an iframe. Most sites refuse to be framed (`X-Frame-Options: DENY/SAMEORIGIN` or
//! `Content-Security-Policy: frame-ancestors ...`), so the iframe renders blank. A browser
//! can't strip those response headers — only a server can. This service is that server-side hop:
//!
//!   GET /bounce?url=<encoded>[&gateway=<id>]
//!     1. open a REAL quicmix link to the chosen gateway (`connect_via`) and run `ingress::serve`
//!        over it — the same proxy the bridge/cli use — so the fetch egresses at the gateway over QUIC,
//!     2. fetch the page through that local proxy,
//!     3. re-serve it WITHOUT the frame blockers, injecting a `<base href>` so its assets resolve.
//!
//! If the gateway link can't be established (no cert / gateway down) it falls back to a direct
//! server-side fetch from the droplet, so the demo still renders. Either way it is NOT a
//! general-purpose frame-buster: every response pins `Content-Security-Policy: frame-ancestors` to
//! the demo origins only, and SSRF targets (loopback / private / metadata) are refused on the initial
//! url and on every redirect hop. Run behind Caddy (auto-TLS), beside quicmix-bridge, sharing certs.

use axum::{
    extract::Query,
    http::{header, HeaderValue, StatusCode},
    response::Response,
    routing::get,
    Router,
};
use once_cell::sync::Lazy;
use quicmix::node::Node;
use quicmix::OracleParams;
use regex::Regex;
use rustls::pki_types::CertificateDer;
use serde::Deserialize;
use std::net::SocketAddr;
use std::time::Duration;

/// Cap the bytes we'll buffer + re-serve (a generous page budget).
const MAX_BYTES: usize = 12 * 1024 * 1024;
/// Who is allowed to frame the bounced page — the demo only, never an arbitrary site.
const FRAME_ANCESTORS: &str =
    "frame-ancestors 'self' https://*.github.io http://localhost:* https://localhost:* http://127.0.0.1:*";

/// The live gateways the demo can route through (verified up); addr/cert overridable per-id via
/// `QUICMIX_GW_<ID>_ADDR` / `QUICMIX_GW_<ID>_CERT` (same convention as quicmix-bridge).
const GATEWAYS: &[(&str, &str)] = &[
    ("fra1", "64.226.93.43:4433"),
    ("nyc3", "68.183.148.148:4433"),
];

static DIRECT: Lazy<reqwest::Client> = Lazy::new(|| base_builder().build().expect("client"));
// strip the page's own framing directives + any <base> it ships (we inject our own)
static RE_META_BLOCK: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?is)<meta\b[^>]*?http-equiv\s*=\s*["']?(?:content-security-policy|x-frame-options)["']?[^>]*>"#).unwrap()
});
static RE_BASE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?is)<base\b[^>]*>").unwrap());
static RE_HEAD: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?is)<head\b[^>]*>").unwrap());

#[derive(Deserialize)]
struct Params {
    url: String,
    gateway: Option<String>,
}

/// Block the obvious SSRF targets — the fetch egresses from the droplet/gateway, so loopback /
/// private / link-local / cloud-metadata hosts must never be reachable through the bounce.
fn host_blocked(host: &str) -> bool {
    host == "localhost"
        || host == "0.0.0.0"
        || host == "::1"
        || host == "metadata.google.internal"
        || host.ends_with(".local")
        || host.ends_with(".internal")
        || host.starts_with("127.")
        || host.starts_with("10.")
        || host.starts_with("192.168.")
        || host.starts_with("169.254.")
        || (host.starts_with("172.")
            && host
                .split('.')
                .nth(1)
                .and_then(|o| o.parse::<u8>().ok())
                .map(|o| (16..=31).contains(&o))
                .unwrap_or(false))
}

fn check_url(raw: &str) -> Result<url::Url, String> {
    let u = url::Url::parse(raw).map_err(|_| "not a url".to_string())?;
    if !matches!(u.scheme(), "http" | "https") {
        return Err("only http/https urls".into());
    }
    let host = u.host_str().ok_or("no host")?.to_lowercase();
    if host_blocked(&host) {
        return Err("host not allowed".into());
    }
    Ok(u)
}

fn gateway(id: &str) -> Result<(SocketAddr, CertificateDer<'static>), String> {
    let (_, default_addr) = GATEWAYS
        .iter()
        .find(|(g, _)| *g == id)
        .ok_or_else(|| format!("unknown gateway {id:?}"))?;
    let up = id.to_uppercase();
    let addr = std::env::var(format!("QUICMIX_GW_{up}_ADDR")).unwrap_or_else(|_| default_addr.to_string());
    let path = std::env::var(format!("QUICMIX_GW_{up}_CERT")).unwrap_or_else(|_| format!("certs/{id}.cert"));
    let bytes = std::fs::read(&path).map_err(|e| format!("cert {path}: {e}"))?;
    let addr = addr.parse().map_err(|e| format!("addr {addr}: {e}"))?;
    Ok((addr, CertificateDer::from(bytes)))
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

/// Shared client config — browser-ish UA, a timeout, and a redirect policy that re-checks SSRF on
/// every hop. Built once for the direct client, and per-request for the gateway-proxied client.
fn base_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
        .timeout(Duration::from_secs(25))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() > 8 {
                return attempt.error("too many redirects");
            }
            match attempt.url().host_str() {
                Some(h) if !host_blocked(&h.to_lowercase()) => attempt.follow(),
                _ => attempt.stop(),
            }
        }))
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn page(status: StatusCode, route: &str, body: String) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CONTENT_SECURITY_POLICY, FRAME_ANCESTORS)
        .header(header::REFERRER_POLICY, "no-referrer")
        .header("x-bounce-route", route)
        .body(axum::body::Body::from(body))
        .unwrap()
}

fn err_page(msg: &str) -> Response {
    page(
        StatusCode::BAD_GATEWAY,
        "error",
        format!(
            "<!doctype html><html><head><meta charset=utf-8></head>\
             <body style=\"margin:0;font:14px ui-monospace,monospace;background:#02060a;color:#ffb53d;\
             display:flex;align-items:center;justify-content:center;height:100vh;text-align:center;padding:24px\">\
             <div>bounce loader: {}<br><br><span style=\"color:#52ff8f\">this page could not be loaded for embedding</span></div>\
             </body></html>",
            esc(msg)
        ),
    )
}

/// Strip the page's framing directives + base, inject a `<base href>` so relative assets resolve
/// against the real origin (loaded cross-origin directly, which is fine for img/script/css/font).
fn rewrite_html(body: &str, base: &str) -> String {
    let body = RE_META_BLOCK.replace_all(body, "");
    let body = RE_BASE.replace_all(&body, "");
    let base_tag = format!(
        "<base href=\"{}\"><meta name=\"referrer\" content=\"no-referrer\">",
        base.replace('"', "%22")
    );
    if let Some(m) = RE_HEAD.find(&body) {
        let idx = m.end();
        let mut out = String::with_capacity(body.len() + base_tag.len());
        out.push_str(&body[..idx]);
        out.push_str(&base_tag);
        out.push_str(&body[idx..]);
        out
    } else {
        format!("{base_tag}{body}")
    }
}

/// Fetch with the given client, rewrite HTML, and build the framable response.
async fn fetch_render(client: &reqwest::Client, url: &url::Url, route: &str) -> Response {
    let resp = match client.get(url.clone()).send().await {
        Ok(r) => r,
        Err(e) => return err_page(&format!("fetch failed: {e}")),
    };
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
    let final_url = resp.url().to_string();
    let ctype = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if let Some(len) = resp.content_length() {
        if len as usize > MAX_BYTES {
            return err_page("page too large to embed");
        }
    }
    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => return err_page(&format!("read failed: {e}")),
    };
    if bytes.len() > MAX_BYTES {
        return err_page("page too large to embed");
    }

    if ctype.is_empty() || ctype.contains("html") {
        let rewritten = rewrite_html(&String::from_utf8_lossy(&bytes), &final_url);
        return page(status, route, rewritten);
    }
    let ct = HeaderValue::from_str(&ctype).unwrap_or(HeaderValue::from_static("application/octet-stream"));
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, ct)
        .header(header::CONTENT_SECURITY_POLICY, FRAME_ANCESTORS)
        .header(header::REFERRER_POLICY, "no-referrer")
        .header("x-bounce-route", route)
        .body(axum::body::Body::from(bytes))
        .unwrap()
}

/// The on-theme path: a real quicmix link to the gateway, an `ingress::serve` proxy bound to that
/// QUIC connection, and the fetch driven through it — so the request really egresses at the gateway.
async fn via_gateway(gw: &str, url: &url::Url) -> Result<Response, String> {
    let (addr, cert) = gateway(gw)?;
    let node = Node::new(oracle()).map_err(|e| e.to_string())?;
    // bound the handshake so a dead gateway falls back quickly instead of hanging the request
    let link = tokio::time::timeout(Duration::from_secs(7), node.connect_via("0.0.0.0:0".parse().unwrap(), addr, cert))
        .await
        .map_err(|_| "gateway connect timed out".to_string())?
        .map_err(|e| e.to_string())?;
    let proxy_addr = quicmix::ingress::serve("127.0.0.1:0", link.conn.clone())
        .await
        .map_err(|e| e.to_string())?;
    let client = base_builder()
        .proxy(reqwest::Proxy::all(format!("http://{proxy_addr}")).map_err(|e| e.to_string())?)
        .build()
        .map_err(|e| e.to_string())?;
    let resp = fetch_render(&client, url, &format!("gateway:{gw}")).await;
    drop(link); // hold the circuit open until the fetch is done, then close it
    Ok(resp)
}

async fn bounce(Query(p): Query<Params>) -> Response {
    let u = match check_url(&p.url) {
        Ok(u) => u,
        Err(e) => return err_page(&e),
    };
    let gw = p.gateway.as_deref().unwrap_or("fra1");
    match via_gateway(gw, &u).await {
        Ok(resp) => resp,
        Err(e) => {
            eprintln!("bounce: gateway route via {gw} failed ({e}); serving direct");
            fetch_render(&DIRECT, &u, "direct").await
        }
    }
}

#[tokio::main]
async fn main() {
    // one process-wide crypto provider, shared by quinn (gateway link) and reqwest's rustls
    rustls::crypto::ring::default_provider().install_default().ok();
    Lazy::force(&DIRECT);
    let listen = std::env::var("BOUNCE_LISTEN").unwrap_or_else(|_| "127.0.0.1:9100".into());
    let app = Router::new()
        .route("/bounce", get(bounce))
        .route("/healthz", get(|| async { "ok" }))
        .route("/", get(|| async { "quicmix-bounce — GET /bounce?url=<encoded>&gateway=<id>" }));
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .unwrap_or_else(|e| panic!("bind {listen}: {e}"));
    eprintln!("quicmix-bounce on http://{listen}  (GET /bounce?url=...&gateway=fra1|nyc3)");
    axum::serve(listener, app).await.unwrap();
}
