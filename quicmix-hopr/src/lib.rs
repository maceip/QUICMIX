//! HOPR substrate — `quicmix::MixTransport` over a running `hoprd` node's REST API.
//!
//! HOPR has **no usable Rust client crate** (verified: `hopr-api` on crates.io is
//! *internal traits only*; the node crates `hopr-lib`/`transport/*` are git-only
//! full-node code, not a client). The real way an application talks to a hoprd
//! node is its **REST API**: send via `POST /api/v3/messages`, receive via
//! `POST /api/v3/messages/pop`, auth header `X-Auth-Token`. This crate speaks that
//! protocol directly — real HTTP, no mock.
//!
//! Because a HOPR message payload is small, a full QUIC datagram is **segmented**
//! across multiple HOPR messages (8-byte header `[id u32][idx u16][total u16]`,
//! base64 body) and **reassembled** on receive — so this carries arbitrary
//! datagrams, not just tiny payloads.
//!
//! ## Status — validated against the API spec; live-node run still needed
//! Compiles, and the wire protocol is now checked against hoprd's own generated
//! SDK (`hoprnet/hoprd-sdk-python`, REST API **v3.1.0** / hoprd 2.1.x):
//! - send `POST /api/v3/messages`, pop `POST /api/v3/messages/pop`, auth
//!   `X-Auth-Token` — all confirmed.
//! - recipient field is **`peerId`** (was wrongly `destination`) — fixed.
//! - pop response is `{ body, receivedAt, tag }` — we read `body`.
//!
//! Not yet exercised against a running node (needs a hoprd node — a funded one, or
//! a local `pluto` dev cluster). Run `bin/hopr_probe` there to confirm end-to-end.
//! Open items: (1) `CHUNK` vs the node's real max message payload (kept
//! conservative at 300 B); (2) on very recent hoprd the **Session API**
//! (UDP-over-HOPR, no size limit, `destination`/`Address` addressing) supersedes
//! `/messages` — that path's schema must be read off a live node.

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use quicmix::{MixTransport, OracleParams, SubstrateError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

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

/// Map an hoprd HTTP status into a typed result (401/403→auth, 5xx→remote-rejected).
fn status_to_result(st: reqwest::StatusCode) -> Result<(), SubstrateError> {
    let c = st.as_u16();
    if st.is_success() {
        Ok(())
    } else if c == 401 || c == 403 {
        Err(SubstrateError::AuthFailed)
    } else if st.is_server_error() {
        Err(SubstrateError::RemoteRejected)
    } else {
        Err(SubstrateError::Io(format!("hopr HTTP {c}")))
    }
}

/// Raw datagram bytes per HOPR message (before the 8-byte header + base64).
/// Conservative for HOPR's small payload; confirm against the live node.
const CHUNK: usize = 300;

/// Canonical HOPR `OracleParams` — the timing model the oracle-fed CC consumes for
/// this substrate. HOPR is **rate-limited** (small REST messages, segmented from each
/// QUIC datagram into ~`mtu/CHUNK` HOPR messages), so `slot_interval` is non-zero: a
/// 1200 B datagram is ~4 HOPR messages, and at a conservative ~20 msg/s that's
/// ~200 ms per datagram — giving the CC a *bounded* BDP window rather than an uncapped
/// link. `hops` is the configured HOPR path length. These are conservative defaults;
/// measure the real rate/delay/loss on your node (`bin/hopr_probe`) and refresh.
pub fn hopr_oracle(hops: u8) -> OracleParams {
    OracleParams {
        hops: hops.max(1) as u32,
        mean_hop_delay: Duration::from_millis(100),
        drop_prob: 0.0,
        slot_interval: Duration::from_millis(200),
        mtu: 1200,
    }
}

pub struct HoprSubstrate {
    http: reqwest::Client,
    api: String,   // e.g. "http://127.0.0.1:3001"
    token: String, // X-Auth-Token
    dest: String,  // recipient HOPR peer id
    hops: u8,
    tag: u16,
    oracle: OracleParams,
    next_id: AtomicU32,
    reasm: Mutex<HashMap<u32, Reasm>>,
}

struct Reasm {
    parts: Vec<Option<Vec<u8>>>,
}

#[derive(Serialize)]
struct SendBody<'a> {
    body: String,
    // hoprd REST API v3 (v3.1.0 / hoprd 2.1.x) names the recipient `peerId`
    // (confirmed against hoprnet/hoprd-sdk-python `SendMessageBodyRequest`). The
    // newer Session API uses an `Address`/`destination` instead — see the module
    // status note.
    #[serde(rename = "peerId")]
    peer_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    hops: Option<u8>,
    tag: u16,
}

#[derive(Serialize)]
struct TagQuery {
    tag: u16,
}

#[derive(Deserialize, Default)]
struct PopResponse {
    #[serde(default)]
    body: Option<String>,
}

impl HoprSubstrate {
    pub fn connect(
        api: impl Into<String>,
        token: impl Into<String>,
        dest: impl Into<String>,
        hops: u8,
        tag: u16,
        oracle: OracleParams,
    ) -> Self {
        // A wedged hoprd must surface as a typed `Timeout`, not hang the substrate
        // forever. 30 s is generous for a mix path (seconds-scale RTT) yet bounded.
        Self::connect_with_timeout(api, token, dest, hops, tag, oracle, Duration::from_secs(30))
    }

    /// As [`Self::connect`] but with an explicit per-request HTTP timeout. A request
    /// that exceeds it surfaces as [`SubstrateError::Timeout`] (via [`map_reqwest`]).
    #[allow(clippy::too_many_arguments)]
    pub fn connect_with_timeout(
        api: impl Into<String>,
        token: impl Into<String>,
        dest: impl Into<String>,
        hops: u8,
        tag: u16,
        oracle: OracleParams,
        http_timeout: Duration,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(http_timeout)
            .build()
            .unwrap_or_default();
        Self {
            http,
            api: api.into(),
            token: token.into(),
            dest: dest.into(),
            hops,
            tag,
            oracle,
            next_id: AtomicU32::new(0),
            reasm: Mutex::new(HashMap::new()),
        }
    }

    /// POST one HOPR message carrying the (already-encoded) `body` string. Maps
    /// hoprd's HTTP errors into typed [`SubstrateError`]s instead of dropping them.
    async fn post_body(&self, body: String) -> Result<(), SubstrateError> {
        let resp = self
            .http
            .post(format!("{}/api/v3/messages", self.api))
            .header("X-Auth-Token", &self.token)
            .json(&SendBody {
                body,
                peer_id: &self.dest,
                hops: Some(self.hops),
                tag: self.tag,
            })
            .send()
            .await
            .map_err(map_reqwest)?;
        status_to_result(resp.status())
    }

    /// Pop one HOPR message body string for our tag. `Ok(None)` = empty inbox
    /// (404/204); auth/server errors surface as typed errors (same mapping as send).
    async fn pop_typed(&self) -> Result<Option<String>, SubstrateError> {
        let resp = self
            .http
            .post(format!("{}/api/v3/messages/pop", self.api))
            .header("X-Auth-Token", &self.token)
            .json(&TagQuery { tag: self.tag })
            .send()
            .await
            .map_err(map_reqwest)?;
        let st = resp.status();
        if matches!(st.as_u16(), 404 | 204) {
            return Ok(None); // empty inbox is not an error
        }
        status_to_result(st)?;
        let pr = resp
            .json::<PopResponse>()
            .await
            .map_err(|_| SubstrateError::Malformed)?;
        Ok(pr.body)
    }

    /// Receive one full (reassembled) datagram, mapping substrate errors.
    async fn recv_typed(&self) -> Result<Vec<u8>, SubstrateError> {
        loop {
            let body = match self.pop_typed().await? {
                Some(b) => b,
                None => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let raw = match B64.decode(body) {
                Ok(r) if r.len() >= 8 => r,
                _ => continue, // not a quicmix frame; skip
            };
            let id = u32::from_be_bytes(raw[0..4].try_into().unwrap());
            let idx = u16::from_be_bytes(raw[4..6].try_into().unwrap());
            let total = u16::from_be_bytes(raw[6..8].try_into().unwrap());
            let chunk = raw[8..].to_vec();
            if total == 0 || idx >= total {
                continue;
            }
            let mut map = self.reasm.lock().await;
            const MAX_INFLIGHT: usize = 512;
            if map.len() >= MAX_INFLIGHT && !map.contains_key(&id) {
                if let Some(&oldest) = map.keys().min() {
                    map.remove(&oldest);
                }
            }
            let entry = map.entry(id).or_insert_with(|| Reasm {
                parts: vec![None; total as usize],
            });
            entry.parts[idx as usize] = Some(chunk);
            if entry.parts.iter().all(|p| p.is_some()) {
                let full: Vec<u8> = entry.parts.iter().flatten().flatten().copied().collect();
                map.remove(&id);
                return Ok(full);
            }
        }
    }

    /// Single-message send/recv helpers (no segmentation) — used by `hopr_probe`.
    pub async fn send_one(&self, data: &[u8]) -> Result<(), SubstrateError> {
        self.post_body(B64.encode(data)).await
    }
    pub async fn pop_one(&self) -> Result<Option<Vec<u8>>, SubstrateError> {
        match self.pop_typed().await? {
            Some(b) => B64.decode(b).map(Some).map_err(|_| SubstrateError::Malformed),
            None => Ok(None),
        }
    }
}

fn frame(id: u32, idx: u16, total: u16, chunk: &[u8]) -> String {
    let mut buf = Vec::with_capacity(8 + chunk.len());
    buf.extend_from_slice(&id.to_be_bytes());
    buf.extend_from_slice(&idx.to_be_bytes());
    buf.extend_from_slice(&total.to_be_bytes());
    buf.extend_from_slice(chunk);
    B64.encode(buf)
}

#[async_trait]
impl MixTransport for HoprSubstrate {
    fn oracle(&self) -> OracleParams {
        self.oracle
    }

    async fn send(&self, datagram: Vec<u8>) {
        // Legacy lossy path; production routes through Substrate -> try_send.
        let _ = self.try_send(datagram).await;
    }

    async fn recv(&self) -> Option<Vec<u8>> {
        self.recv_typed().await.ok()
    }

    async fn try_send(&self, datagram: Vec<u8>) -> Result<(), SubstrateError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let chunks: Vec<&[u8]> = if datagram.is_empty() {
            vec![&[]]
        } else {
            datagram.chunks(CHUNK).collect()
        };
        let total = chunks.len() as u16;
        for (idx, c) in chunks.iter().enumerate() {
            // First failing segment surfaces a typed error (401/5xx/timeout/io).
            self.post_body(frame(id, idx as u16, total, c)).await?;
        }
        Ok(())
    }

    async fn try_recv(&self) -> Result<Vec<u8>, SubstrateError> {
        self.recv_typed().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The oracle-fed CC must build correctly from HOPR's params: HOPR is
    /// rate-limited, so the BDP window must be *bounded* (a non-zero `slot_interval`),
    /// never the uncapped sentinel that a `slot_interval: ZERO` would produce.
    #[test]
    fn cc_builds_from_hopr_oracle_rate_capped() {
        use quicmix::client::{bdp_bytes, Congestion};
        let p = hopr_oracle(1);
        for cc in [Congestion::Stock, Congestion::Timers, Congestion::Quicmix] {
            let _ = cc.transport(&p);
        }
        assert!(!p.slot_interval.is_zero(), "HOPR is rate-limited → must be rate-capped");
        let w = bdp_bytes(&p);
        assert!(w >= p.mtu as u64 * 2, "window below two cells");
        assert!(
            w < 1 << 20,
            "HOPR is rate-capped → BDP must be bounded, got {w}"
        );
        assert!(p.loss_time_threshold() >= 1.5);
    }

    // ---- #9 failure modes: real HTTP responses → typed SubstrateError ----
    // A raw-TCP mock hoprd. It answers every request with one canned response (or,
    // for `Hang`, accepts and never replies → the client's timeout must fire). This
    // exercises the *real* reqwest send + status-mapping path, no stubbing.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    enum Reply {
        Status(&'static str), // full HTTP status line, e.g. "401 Unauthorized"
        Hang,                 // accept, read, never respond
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
                        let _ = sock.read(&mut buf).await; // consume the request
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
                        // hold the socket open, never respond
                        std::future::pending::<()>().await;
                    }
                }
            }
        });
        format!("http://{addr}")
    }

    fn sub(api: String, http_timeout: Duration) -> HoprSubstrate {
        HoprSubstrate::connect_with_timeout(
            api,
            "test-token",
            "peerDest",
            1,
            42,
            hopr_oracle(1),
            http_timeout,
        )
    }

    #[tokio::test]
    async fn http_401_maps_to_auth_failed() {
        let api = mock_hoprd(Reply::Status("401 Unauthorized")).await;
        let s = sub(api, Duration::from_secs(5));
        // send: typed AuthFailed surfaces (not a swallowed drop)
        assert_eq!(s.try_send(b"hello".to_vec()).await, Err(SubstrateError::AuthFailed));
        // recv path maps it too
        assert_eq!(s.pop_one().await, Err(SubstrateError::AuthFailed));
    }

    #[tokio::test]
    async fn http_500_maps_to_remote_rejected() {
        let api = mock_hoprd(Reply::Status("500 Internal Server Error")).await;
        let s = sub(api, Duration::from_secs(5));
        assert_eq!(s.try_send(b"hello".to_vec()).await, Err(SubstrateError::RemoteRejected));
        assert_eq!(s.pop_one().await, Err(SubstrateError::RemoteRejected));
    }

    #[tokio::test]
    async fn wedged_node_times_out_bounded() {
        // A hoprd that accepts but never replies must surface Timeout within the
        // configured bound, not hang the substrate forever.
        let api = mock_hoprd(Reply::Hang).await;
        let s = sub(api, Duration::from_millis(300));
        let t0 = std::time::Instant::now();
        let r = s.try_send(b"hello".to_vec()).await;
        assert_eq!(r, Err(SubstrateError::Timeout), "wedged node → typed Timeout");
        assert!(t0.elapsed() < Duration::from_secs(3), "recovery is bounded by the timeout");
    }

    #[tokio::test]
    async fn dead_node_maps_to_closed() {
        // Nothing listening on this port → connection refused → typed Closed.
        let s = sub("http://127.0.0.1:1".to_string(), Duration::from_secs(2));
        let r = s.try_send(b"hello".to_vec()).await;
        assert_eq!(r, Err(SubstrateError::Closed), "connect failure → typed Closed");
    }
}
