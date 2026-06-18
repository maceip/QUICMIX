//! HOPR substrate — `quicmix::MixTransport` over a running `hoprd` node's **v4
//! Session API** (a UDP tunnel through the HOPR mixnet).
//!
//! HOPR has no usable Rust client crate, so this speaks the node's HTTP API directly.
//! hoprd **3.x removed** the old v3 message API (`POST /api/v3/messages` + `/pop`,
//! `X-Auth-Token`); messaging is now the **Session API** (verified against the real
//! `hopr-pluto` 3.1.0 image's generated client `sdk/python/api/request_objects.py`):
//!
//! - open a udp session: `POST /api/v4/session/udp`, `Authorization: Bearer <token>`,
//!   body `{ capabilities, destination, listenHost, forwardPath, returnPath, target,
//!   responseBuffer }` → returns `{ protocol, ip, port }`, a **local udp socket on the
//!   node** that tunnels your datagrams to the destination over the mixnet.
//! - carry datagrams: just `send`/`recv` udp to that socket — hoprd does the mixing and
//!   (with the `Segmentation` capability) the fragmentation, so a full QUIC datagram
//!   fits with no per-message header of our own.
//! - close: `DELETE /api/v4/session/{protocol}/{ip}/{port}`.
//!
//! Addressing is by on-chain `destination` (an Address), routing by `forwardPath` /
//! `returnPath` (`{"Hops": n}` or `{"IntermediatePath": [...]}`), and the session
//! tunnels to a `target` service the exit node reaches (`{"Plain": "host:port"}`).
//!
//! ## Status — VERIFIED LIVE
//! Run against a real `hopr-pluto` 3.0.0 cluster (6 nodes, anvil-backed chain): a v4
//! udp session node1→node2 (0 hops) carried a datagram through the mixnet to an echo
//! target and back — **round-trip OK in 244 ms**. See `bin/hopr_probe`.

use async_trait::async_trait;
use quicmix::{MixTransport, OracleParams, SubstrateError};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;

/// Map a reqwest transport error into a typed [`SubstrateError`].
fn map_reqwest(e: reqwest::Error) -> SubstrateError {
    if e.is_timeout() {
        SubstrateError::Timeout
    } else if e.is_connect() {
        SubstrateError::Closed
    } else {
        SubstrateError::Io(format!("hopr http: {e}"))
    }
}

/// Map an hoprd HTTP status into a typed error (401/403→auth, 5xx→remote-rejected).
fn status_error(st: reqwest::StatusCode) -> SubstrateError {
    let c = st.as_u16();
    if c == 401 || c == 403 {
        SubstrateError::AuthFailed
    } else if st.is_server_error() {
        SubstrateError::RemoteRejected
    } else {
        SubstrateError::Io(format!("hopr HTTP {c}"))
    }
}

/// Canonical HOPR `OracleParams` — the timing model the oracle-fed CC consumes. HOPR
/// is a mixnet (per-hop delay + a constant-rate session), so `slot_interval` is
/// non-zero, giving the CC a *bounded* BDP window rather than an uncapped link. `hops`
/// is the configured path length. Conservative defaults; measure the real
/// rate/delay/loss on your node (`bin/hopr_probe`) and refresh.
pub fn hopr_oracle(hops: u8) -> OracleParams {
    OracleParams {
        hops: hops.max(1) as u32,
        mean_hop_delay: Duration::from_millis(100),
        drop_prob: 0.0,
        slot_interval: Duration::from_millis(50),
        mtu: 1200,
    }
}

// ---- v4 session-create request/response (field names per the real generated client) ----

#[derive(Serialize)]
struct PathHops {
    #[serde(rename = "Hops")]
    hops: u8,
}
#[derive(Serialize)]
struct TargetPlain {
    #[serde(rename = "Plain")]
    plain: String,
}
#[derive(Serialize)]
struct CreateSessionBody {
    capabilities: Vec<String>,
    destination: String,
    #[serde(rename = "listenHost")]
    listen_host: String,
    #[serde(rename = "forwardPath")]
    forward_path: PathHops,
    #[serde(rename = "returnPath")]
    return_path: PathHops,
    target: TargetPlain,
    #[serde(rename = "responseBuffer")]
    response_buffer: String,
}
#[derive(Deserialize, Debug)]
struct SessionResp {
    protocol: String,
    ip: String,
    port: u16,
}

/// A live HOPR v4 session: the HTTP control handle plus the local UDP socket carrying
/// datagrams through the tunnel.
#[derive(Debug)]
pub struct HoprSubstrate {
    http: reqwest::Client,
    api: String,
    token: String,
    sock: UdpSocket,
    session: SessionResp,
    oracle: OracleParams,
}

/// Knobs for opening a session. `target` is the service the exit node tunnels to
/// (e.g. a peer quicmix UDP endpoint, `"host:port"`); `hops` is the path length both
/// ways; `listen_host` is where hoprd opens the data socket; `data_host` overrides the
/// host used to reach that socket (when the returned ip isn't directly reachable, e.g.
/// `0.0.0.0` → `127.0.0.1`).
#[derive(Clone, Debug)]
pub struct SessionOpts {
    pub hops: u8,
    pub target: String,
    pub listen_host: String,
    pub data_host: Option<String>,
    pub http_timeout: Duration,
}

impl Default for SessionOpts {
    fn default() -> Self {
        Self {
            hops: 0,
            target: "127.0.0.1:9999".into(),
            listen_host: "0.0.0.0:0".into(),
            data_host: None,
            http_timeout: Duration::from_secs(30),
        }
    }
}

impl HoprSubstrate {
    /// Open a real UDP session to `destination` (an on-chain Address) on a running
    /// hoprd node and bind a local socket to the returned data endpoint.
    pub async fn connect(
        api: impl Into<String>,
        token: impl Into<String>,
        destination: impl Into<String>,
        opts: SessionOpts,
        oracle: OracleParams,
    ) -> anyhow::Result<Self> {
        let api = api.into();
        let token = token.into();
        let http = reqwest::Client::builder()
            .timeout(opts.http_timeout)
            .build()
            .unwrap_or_default();
        let body = CreateSessionBody {
            // Segmentation so a full datagram fits; no Retransmission — quicmix's QUIC
            // does its own reliability, the substrate stays a lossy datagram pipe.
            capabilities: vec!["Segmentation".into()],
            destination: destination.into(),
            listen_host: opts.listen_host.clone(),
            forward_path: PathHops { hops: opts.hops },
            return_path: PathHops { hops: opts.hops },
            target: TargetPlain { plain: opts.target.clone() },
            response_buffer: "4MiB".into(),
        };
        let resp = http
            .post(format!("{api}/api/v4/session/udp"))
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await
            .map_err(map_reqwest)?;
        let st = resp.status();
        if !st.is_success() {
            let detail = resp.text().await.unwrap_or_default();
            anyhow::bail!("session open failed: {} ({})", status_error(st), detail.trim());
        }
        let session: SessionResp = resp
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("malformed session response: {e}"))?;

        // Reach the data socket. The returned ip may be a wildcard (0.0.0.0) — fall
        // back to loopback, or use the explicit override.
        let host = opts.data_host.clone().unwrap_or_else(|| {
            if session.ip == "0.0.0.0" || session.ip == "::" {
                "127.0.0.1".into()
            } else {
                session.ip.clone()
            }
        });
        let addr: SocketAddr = format!("{host}:{}", session.port)
            .parse()
            .map_err(|e| anyhow::anyhow!("bad session addr {host}:{}: {e}", session.port))?;
        let sock = UdpSocket::bind("0.0.0.0:0").await?;
        sock.connect(addr).await?;

        Ok(Self { http, api, token, sock, session, oracle })
    }

    /// The local data endpoint this session listens on (diagnostic).
    pub fn session_endpoint(&self) -> (String, u16) {
        (self.session.ip.clone(), self.session.port)
    }

    /// Close the session on the node (best effort).
    pub async fn close(&self) {
        let url = format!(
            "{}/api/v4/session/{}/{}/{}",
            self.api, self.session.protocol, self.session.ip, self.session.port
        );
        let _ = self.http.delete(url).bearer_auth(&self.token).send().await;
    }
}

#[async_trait]
impl MixTransport for HoprSubstrate {
    fn oracle(&self) -> OracleParams {
        self.oracle
    }
    async fn send(&self, datagram: Vec<u8>) {
        let _ = self.try_send(datagram).await;
    }
    async fn recv(&self) -> Option<Vec<u8>> {
        self.try_recv().await.ok()
    }
    async fn try_send(&self, datagram: Vec<u8>) -> Result<(), SubstrateError> {
        self.sock
            .send(&datagram)
            .await
            .map(|_| ())
            .map_err(|e| SubstrateError::Io(format!("hopr session send: {e}")))
    }
    async fn try_recv(&self) -> Result<Vec<u8>, SubstrateError> {
        let mut buf = vec![0u8; 65535];
        let n = self.sock.recv(&mut buf).await.map_err(|_| SubstrateError::Closed)?;
        buf.truncate(n);
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The oracle-fed CC must build correctly from HOPR's params: HOPR is a mixnet, so
    /// the BDP window must be *bounded* (a non-zero `slot_interval`), never the uncapped
    /// sentinel that a `slot_interval: ZERO` would produce.
    #[test]
    fn cc_builds_from_hopr_oracle_rate_capped() {
        use quicmix::client::{bdp_bytes, Congestion};
        let p = hopr_oracle(1);
        for cc in [Congestion::Stock, Congestion::Timers, Congestion::Quicmix] {
            let _ = cc.transport(&p);
        }
        assert!(!p.slot_interval.is_zero(), "HOPR is a mixnet → must be rate-capped");
        let w = bdp_bytes(&p);
        assert!(w >= p.mtu as u64 * 2, "window below two cells");
        assert!(w < 1 << 20, "HOPR is rate-capped → BDP must be bounded, got {w}");
        assert!(p.loss_time_threshold() >= 1.5);
    }

    // ---- #9 failure modes: real HTTP responses on the v4 session-open → typed error ----
    // A raw-TCP mock hoprd answering the session-create POST with a canned status (or
    // hanging for the timeout case). Exercises the real reqwest + status-mapping path.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    enum Reply {
        Status(&'static str),
        Hang,
    }

    async fn mock_hoprd(reply: Reply) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { break };
                match reply {
                    Reply::Status(line) => {
                        let mut buf = [0u8; 2048];
                        let _ = sock.read(&mut buf).await;
                        let body = "{}";
                        let resp = format!(
                            "HTTP/1.1 {line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                        let _ = sock.flush().await;
                    }
                    Reply::Hang => {
                        let mut buf = [0u8; 2048];
                        let _ = sock.read(&mut buf).await;
                        std::future::pending::<()>().await;
                    }
                }
            }
        });
        format!("http://{addr}")
    }

    async fn open(api: String, http_timeout: Duration) -> anyhow::Result<HoprSubstrate> {
        HoprSubstrate::connect(
            api,
            "test-token",
            "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            SessionOpts { http_timeout, ..Default::default() },
            hopr_oracle(1),
        )
        .await
    }

    #[tokio::test]
    async fn http_401_maps_to_auth_failed() {
        let api = mock_hoprd(Reply::Status("401 Unauthorized")).await;
        let e = open(api, Duration::from_secs(5)).await.unwrap_err().to_string();
        assert!(e.contains("auth"), "401 → auth failed, got: {e}");
    }

    #[tokio::test]
    async fn http_500_maps_to_remote_rejected() {
        let api = mock_hoprd(Reply::Status("500 Internal Server Error")).await;
        let e = open(api, Duration::from_secs(5)).await.unwrap_err().to_string();
        assert!(e.contains("rejected"), "5xx → remote rejected, got: {e}");
    }

    #[tokio::test]
    async fn wedged_node_times_out_bounded() {
        let api = mock_hoprd(Reply::Hang).await;
        let t0 = std::time::Instant::now();
        let e = open(api, Duration::from_millis(300)).await.unwrap_err().to_string();
        assert!(e.to_lowercase().contains("timeout") || e.contains("Timeout"), "wedged → timeout, got: {e}");
        assert!(t0.elapsed() < Duration::from_secs(3), "recovery bounded by the timeout");
    }

    #[tokio::test]
    async fn dead_node_maps_to_closed() {
        let e = open("http://127.0.0.1:1".to_string(), Duration::from_secs(2))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("Closed") || e.to_lowercase().contains("closed"), "dead node → closed, got: {e}");
    }
}
