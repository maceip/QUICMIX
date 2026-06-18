//! Best-in-class HTTP(S) ingress proxy.
//!
//! This is the seam where a user's traffic enters quicmix: a local forward proxy
//! that carries requests over a quicmix QUIC connection to a gateway, which egresses.
//! HTTP parsing is **hyper** (the standard Rust HTTP stack), so the awkward corners
//! are handled for us — every method, chunked transfer, keep-alive, header-case and
//! edge cases, HTTP/1.1, and `CONNECT`/`Upgrade` (WebSocket).
//!
//! Two paths, one clean module:
//! - **`CONNECT`** (the common case — HTTPS, gRPC, WebSocket-over-TLS, h2-over-TLS):
//!   answer `200`, then splice the upgraded byte stream over a QUIC bidi stream. This
//!   tunnels *anything* carried inside TLS, opaquely.
//! - **absolute-form HTTP** (cleartext forward proxying): open a QUIC bidi stream and
//!   run a hyper *client* over it, so request serialization and response parsing are
//!   hyper's job on both ends.
//!
//! Wire to the gateway: each stream starts with a `"host:port\n"` line (the same
//! contract [`crate::node`]'s gateway already speaks), then raw bytes.
//!
//! Scope boundary (intentional, no creep): this proxies **HTTP/HTTPS over TCP**. Raw
//! UDP / WebRTC (ICE) needs `CONNECT-UDP`/MASQUE and is a separate effort — see
//! [`UNSUPPORTED_UDP`].

use anyhow::anyhow;
use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use quinn::Connection;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Marker for the deferred raw-UDP / WebRTC path (CONNECT-UDP / MASQUE). Not built.
pub const UNSUPPORTED_UDP: &str = "raw UDP / WebRTC (CONNECT-UDP / MASQUE) — out of scope for now";

type Body = BoxBody<Bytes, hyper::Error>;

fn full(s: &'static str) -> Body {
    Full::new(Bytes::from(s)).map_err(|e| match e {}).boxed()
}
fn empty() -> Body {
    Empty::<Bytes>::new().map_err(|e| match e {}).boxed()
}
fn status(code: StatusCode, msg: &'static str) -> Response<Body> {
    let mut r = Response::new(full(msg));
    *r.status_mut() = code;
    r
}

/// Serve the HTTP(S) forward proxy on `listen`, carrying everything over a single
/// fixed `conn`. Thin wrapper over [`serve_with`].
pub async fn serve(listen: &str, conn: Arc<Connection>) -> anyhow::Result<SocketAddr> {
    serve_with(listen, move || {
        let conn = conn.clone();
        async move { Some(conn) }
    })
    .await
}

/// Serve the HTTP(S) forward proxy on `listen`, asking `provider` for a QUIC
/// connection **per request** — so a pooled/rotating proxy can round-robin circuits
/// while reusing this one hyper code path. `provider` returning `None` yields 503.
pub async fn serve_with<P, Fut, C>(listen: &str, provider: P) -> anyhow::Result<SocketAddr>
where
    P: Fn() -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Option<C>> + Send,
    C: AsRef<Connection> + Send + 'static,
{
    let l = TcpListener::bind(listen).await?;
    let addr = l.local_addr()?;
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = l.accept().await else { continue };
            let provider = provider.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(tcp);
                let svc = service_fn(move |req| {
                    let provider = provider.clone();
                    async move {
                        match provider().await {
                            Some(conn) => proxy(req, conn).await,
                            None => Ok::<_, hyper::Error>(status(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "no warm circuit available",
                            )),
                        }
                    }
                });
                // with_upgrades() is required for CONNECT to hand us the raw stream.
                if let Err(e) = http1::Builder::new()
                    .preserve_header_case(true)
                    .title_case_headers(true)
                    .serve_connection(io, svc)
                    .with_upgrades()
                    .await
                {
                    let _ = e; // per-connection errors are not fatal to the proxy
                }
            });
        }
    });
    Ok(addr)
}

async fn proxy<C: AsRef<Connection> + Send + 'static>(
    req: Request<Incoming>,
    conn: C,
) -> Result<Response<Body>, hyper::Error> {
    if req.method() == Method::CONNECT {
        // CONNECT host:port — tunnel the upgraded stream over QUIC.
        let Some(authority) = req.uri().authority().map(|a| a.to_string()) else {
            return Ok(status(StatusCode::BAD_REQUEST, "CONNECT requires host:port"));
        };
        tokio::spawn(async move {
            if let Ok(upgraded) = hyper::upgrade::on(req).await {
                let _ = tunnel(upgraded, authority, conn).await;
            }
        });
        // 200 with empty body == "Connection Established".
        Ok(Response::new(empty()))
    } else {
        match forward(req, conn).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                eprintln!("[ingress] forward error: {e:#}");
                Ok(status(StatusCode::BAD_GATEWAY, "quicmix upstream error"))
            }
        }
    }
}

/// CONNECT: splice the client's upgraded byte stream ⇄ a QUIC bidi stream to the
/// gateway (which dials `authority` and pipes raw bytes). Opaque to whatever rides
/// inside — TLS, WebSocket, gRPC, h2.
async fn tunnel<C: AsRef<Connection>>(
    upgraded: hyper::upgrade::Upgraded,
    authority: String,
    conn: C,
) -> anyhow::Result<()> {
    let (mut send, recv) = conn.as_ref().open_bi().await?;
    send.write_all(format!("{authority}\n").as_bytes()).await?;
    let mut client = TokioIo::new(upgraded);
    let mut server = tokio::io::join(recv, send);
    tokio::io::copy_bidirectional(&mut client, &mut server).await?;
    Ok(())
}

/// Cleartext forward proxy: open a QUIC bidi stream, name the target, then run a
/// hyper client over the stream so request/response framing is hyper's job.
async fn forward<C: AsRef<Connection>>(
    req: Request<Incoming>,
    conn: C,
) -> anyhow::Result<Response<Body>> {
    let (parts, body) = req.into_parts();
    let host = parts.uri.host().ok_or_else(|| anyhow!("request URI has no host"))?.to_string();
    let port = parts.uri.port_u16().unwrap_or(80);

    let (mut send, recv) = conn.as_ref().open_bi().await?;
    send.write_all(format!("{host}:{port}\n").as_bytes()).await?;
    let io = TokioIo::new(tokio::io::join(recv, send));
    let (mut sender, hconn) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        hyper::client::conn::http1::handshake(io),
    )
    .await
    .map_err(|_| anyhow!("upstream handshake timed out"))??;
    tokio::spawn(async move {
        let _ = hconn.await;
    });

    // Re-target to origin-form (path?query); carry end-to-end headers + body through.
    let path = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/")
        .to_string();
    let mut req_headers = parts.headers;
    strip_hop_by_hop(&mut req_headers); // don't leak the client↔proxy hop upstream
    let mut builder = Request::builder().method(parts.method).uri(path);
    if let Some(h) = builder.headers_mut() {
        *h = req_headers;
    }
    let upstream = builder.body(body)?;
    let resp = sender.send_request(upstream).await?;
    let mut resp = resp.map(|b| b.boxed());
    // Don't leak the proxy↔upstream hop (e.g. its `Connection: close`) to the
    // client — hyper re-frames the body and manages the client connection.
    strip_hop_by_hop(resp.headers_mut());
    Ok(resp)
}

/// Remove hop-by-hop / framing headers so each TCP/QUIC hop manages its own
/// connection + body framing (RFC 9110 §7.6.1). Keeping them leaks one hop's
/// keep-alive/close decision onto the other.
fn strip_hop_by_hop(headers: &mut hyper::HeaderMap) {
    for h in [
        "connection",
        "keep-alive",
        "proxy-connection",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "content-length",
        "upgrade",
    ] {
        headers.remove(h);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Node;
    use crate::OracleParams;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    fn fast() -> OracleParams {
        OracleParams {
            hops: 1,
            mean_hop_delay: Duration::from_millis(1),
            drop_prob: 0.0,
            slot_interval: Duration::ZERO,
            mtu: 1200,
        }
    }

    /// A minimal HTTP/1.1 origin: replies 200 + "hello-origin" (Connection: close).
    async fn origin() -> SocketAddr {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let _ = s.read(&mut buf).await;
                    let body = b"hello-origin";
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = s.write_all(head.as_bytes()).await;
                    let _ = s.write_all(body).await;
                });
            }
        });
        addr
    }

    /// (proxy_addr, origin_addr, [keep-alive endpoints]) over an in-process gateway.
    async fn setup() -> (SocketAddr, SocketAddr, (quinn::Endpoint, quinn::Endpoint)) {
        rustls::crypto::ring::default_provider().install_default().ok();
        let origin_addr = origin().await;
        let b = Node::new(fast()).unwrap();
        let (gw_ep, gw) = b.serve_gateway().await.unwrap();
        let a = Node::new(fast()).unwrap();
        let link = a.connect(gw, b.cert()).await.unwrap();
        let proxy = serve("127.0.0.1:0", link.conn.clone()).await.unwrap();
        (proxy, origin_addr, (gw_ep, link.endpoint))
    }

    /// Accumulate the response until `marker` appears or the stream goes quiet/EOF
    /// (so a single read getting only the headers doesn't fool the assertion).
    async fn read_until(s: &mut TcpStream, marker: &str) -> String {
        let mut acc = Vec::new();
        let mut buf = vec![0u8; 2048];
        loop {
            match tokio::time::timeout(Duration::from_millis(800), s.read(&mut buf)).await {
                Ok(Ok(0)) | Err(_) => break,
                Ok(Ok(n)) => {
                    acc.extend_from_slice(&buf[..n]);
                    if String::from_utf8_lossy(&acc).contains(marker) {
                        break;
                    }
                }
                Ok(Err(_)) => break,
            }
        }
        String::from_utf8_lossy(&acc).into_owned()
    }

    #[tokio::test]
    async fn forward_absolute_form_and_keep_alive() {
        let (proxy, origin, _keep) = setup().await;
        let mut s = TcpStream::connect(proxy).await.unwrap();
        // two absolute-form GETs on ONE client connection (proxy-side keep-alive).
        for _ in 0..2 {
            s.write_all(format!("GET http://{origin}/ HTTP/1.1\r\nHost: {origin}\r\n\r\n").as_bytes())
                .await
                .unwrap();
            let resp = read_until(&mut s, "hello-origin").await;
            assert!(resp.contains("200") && resp.contains("hello-origin"), "got: {resp}");
        }
    }

    #[tokio::test]
    async fn connect_tunnel() {
        let (proxy, origin, _keep) = setup().await;
        let mut s = TcpStream::connect(proxy).await.unwrap();
        s.write_all(format!("CONNECT {origin} HTTP/1.1\r\nHost: {origin}\r\n\r\n").as_bytes())
            .await
            .unwrap();
        assert!(read_until(&mut s, "200").await.contains("200"), "CONNECT not established");
        // HTTP request inside the opaque tunnel.
        s.write_all(b"GET / HTTP/1.0\r\n\r\n").await.unwrap();
        let mut body = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(8), s.read_to_end(&mut body)).await;
        assert!(String::from_utf8_lossy(&body).contains("hello-origin"), "tunnel body");
    }

    #[tokio::test]
    async fn bad_upstream_is_502() {
        let (proxy, _origin, _keep) = setup().await;
        let mut s = TcpStream::connect(proxy).await.unwrap();
        // discard port 9 → upstream dial fails.
        s.write_all(b"GET http://127.0.0.1:9/ HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        assert!(read_until(&mut s, "502").await.contains("502"), "bad upstream must be 502");
    }
}
