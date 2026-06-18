//! quicmix bounce — iframe loader service.
//!
//! The github-pages globe demo finishes a "fetch over the mixnet" by *showing* the requested
//! page in an iframe. Most sites refuse to be framed (`X-Frame-Options: DENY/SAMEORIGIN` or
//! `Content-Security-Policy: frame-ancestors ...`), so the iframe would render blank. A browser
//! can't strip those response headers — only a server can. This service is that server-side hop:
//!
//!   GET /bounce?url=<encoded>  ->  fetch the page (egressing at the droplet's ip, the same exit
//!                                  as the quicmix gateway), then re-serve it WITHOUT the frame
//!                                  blockers, with a `<base href>` injected so its assets resolve.
//!
//! It is deliberately NOT a general-purpose frame-buster: every response pins
//! `Content-Security-Policy: frame-ancestors` to the demo origins only, so the bounced page can be
//! embedded by the quicmix demo and nothing else. SSRF targets (loopback / private / metadata) are
//! refused, on the initial url and on every redirect hop. Run behind Caddy (auto-TLS) so the https
//! page can load it over https — e.g. `https://<droplet>.nip.io/bounce?url=...`.

use axum::{
    extract::Query,
    http::{header, HeaderValue, StatusCode},
    response::Response,
    routing::get,
    Router,
};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Deserialize;
use std::time::Duration;

/// Cap the bytes we'll buffer + re-serve (a generous page budget).
const MAX_BYTES: usize = 12 * 1024 * 1024;
/// Who is allowed to frame the bounced page — the demo only, never an arbitrary site.
const FRAME_ANCESTORS: &str =
    "frame-ancestors 'self' https://*.github.io http://localhost:* https://localhost:* http://127.0.0.1:*";

static CLIENT: Lazy<reqwest::Client> = Lazy::new(build_client);
// strip the page's own framing directives + any <base> it ships (we inject our own)
static RE_META_BLOCK: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?is)<meta\b[^>]*?http-equiv\s*=\s*["']?(?:content-security-policy|x-frame-options)["']?[^>]*>"#).unwrap()
});
static RE_BASE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?is)<base\b[^>]*>").unwrap());
static RE_HEAD: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?is)<head\b[^>]*>").unwrap());

#[derive(Deserialize)]
struct Params {
    url: String,
    // accepted (the demo sends the chosen gateway) but unused for now — the droplet's own egress
    // ip already matches the gateway it hosts. left here so the front-end contract is stable.
    #[allow(dead_code)]
    gateway: Option<String>,
}

/// Block the obvious SSRF targets — the fetch egresses from the droplet, so loopback / private /
/// link-local / cloud-metadata hosts must never be reachable through the bounce.
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
        || host.starts_with("::1")
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

fn build_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() > 8 {
                return attempt.error("too many redirects");
            }
            match attempt.url().host_str() {
                Some(h) if !host_blocked(&h.to_lowercase()) => attempt.follow(),
                _ => attempt.stop(),
            }
        }))
        .build()
        .expect("reqwest client")
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

/// A framable little error/status page (so the iframe shows *something* useful, not a blank).
fn page(status: StatusCode, body: String) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CONTENT_SECURITY_POLICY, FRAME_ANCESTORS)
        .header(header::REFERRER_POLICY, "no-referrer")
        .header("x-bounce", "quicmix")
        .body(axum::body::Body::from(body))
        .unwrap()
}

fn err_page(msg: &str) -> Response {
    page(
        StatusCode::BAD_GATEWAY,
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

async fn bounce(Query(p): Query<Params>) -> Response {
    let u = match check_url(&p.url) {
        Ok(u) => u,
        Err(e) => return err_page(&e),
    };

    let resp = match CLIENT.get(u.clone()).send().await {
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

    // HTML (or unlabelled) → rewrite + re-serve framable; anything else → pass the bytes through.
    if ctype.is_empty() || ctype.contains("html") {
        let rewritten = rewrite_html(&String::from_utf8_lossy(&bytes), &final_url);
        return page(status, rewritten);
    }
    let ct = HeaderValue::from_str(&ctype).unwrap_or(HeaderValue::from_static("application/octet-stream"));
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, ct)
        .header(header::CONTENT_SECURITY_POLICY, FRAME_ANCESTORS)
        .header(header::REFERRER_POLICY, "no-referrer")
        .header("x-bounce", "quicmix")
        .body(axum::body::Body::from(bytes))
        .unwrap()
}

#[tokio::main]
async fn main() {
    Lazy::force(&CLIENT);
    let listen = std::env::var("BOUNCE_LISTEN").unwrap_or_else(|_| "127.0.0.1:9100".into());
    let app = Router::new()
        .route("/bounce", get(bounce))
        .route("/healthz", get(|| async { "ok" }))
        .route("/", get(|| async { "quicmix-bounce — GET /bounce?url=<encoded>" }));
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .unwrap_or_else(|e| panic!("bind {listen}: {e}"));
    eprintln!("quicmix-bounce on http://{listen}  (GET /bounce?url=...)");
    axum::serve(listener, app).await.unwrap();
}
