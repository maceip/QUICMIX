//! Katzenpost datagram substrate via the thin-client daemon — **real CBOR**.
//!
//! Katzenpost has no Rust client (upstream is Go). Apps integrate through its
//! **thin-client daemon** (`kpclientd`): a local TCP socket exchanging
//! **length-prefixed CBOR frames** (`[u32 big-endian len][CBOR Request/Response]`).
//! This crate speaks that real protocol, end to end:
//!
//! - [`connect_and_handshake`] performs the real handshake (`ConnectionStatusEvent`
//!   → `NewPKIDocumentEvent` → `SessionToken` → `SessionTokenReply`).
//! - [`resolve_service`] parses the daemon's CBOR **PKI document** to find a service
//!   node by capability (e.g. `echo`), deriving its `destination_id_hash`
//!   (`blake2b256(IdentityKey)`, == Katzenpost's `hash.Sum256`) and `recipient_queue_id`.
//! - [`request_reply`] sends a real `SendMessage` (with a reply SURB) and waits for
//!   the matching `MessageReplyEvent` — a full round-trip through the mixnet.
//! - [`KatzenpostSubstrate`] implements [`quicmix::MixTransport`] over a resolved
//!   service.
//!
//! Verified live against the docker testnet's `kpclientd` (`bin/kp_probe`,
//! `bin/kp_echo`). The CBOR schema mirrors Katzenpost's Go `client/thin` +
//! `core/pki` structs (field names = their Go field names / `cbor:"…"` tags).

use async_trait::async_trait;
use blake2::digest::consts::U32;
use blake2::{Blake2b, Digest};
use quicmix::{MixTransport, OracleParams, SubstrateError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

// ---- Thin-client CBOR schema (subset; field names mirror the Go `cbor:"…"` tags).

#[derive(Serialize, Default)]
pub struct Request {
    #[serde(rename = "session_token", skip_serializing_if = "Option::is_none")]
    pub session_token: Option<SessionToken>,
    #[serde(rename = "send_message", skip_serializing_if = "Option::is_none")]
    pub send_message: Option<SendMessage>,
}

#[derive(Serialize)]
pub struct SessionToken {
    /// Go: `ClientInstanceToken [16]byte` — fixed array → CBOR array of u8.
    #[serde(rename = "client_instance_token")]
    pub client_instance_token: [u8; 16],
}

#[derive(Serialize, Default)]
pub struct SendMessage {
    #[serde(rename = "with_surb")]
    pub with_surb: bool,
    #[serde(rename = "surbid", skip_serializing_if = "Option::is_none")]
    pub surbid: Option<[u8; 16]>,
    #[serde(rename = "destination_id_hash")]
    pub destination_id_hash: [u8; 32],
    #[serde(rename = "recipient_queue_id", with = "serde_bytes")]
    pub recipient_queue_id: Vec<u8>,
    #[serde(rename = "payload", with = "serde_bytes")]
    pub payload: Vec<u8>,
}

impl SendMessage {
    /// A `SendMessage` to `service` carrying `payload`, with a reply SURB (a fresh
    /// `surbid`) attached so the service can route its reply back to us.
    fn with_reply_surb(service: &Service, payload: Vec<u8>) -> Self {
        Self {
            with_surb: true,
            surbid: Some(instance_token()),
            destination_id_hash: service.destination_id_hash,
            recipient_queue_id: service.recipient_queue_id.clone(),
            payload,
        }
    }
}

#[derive(Deserialize, Default)]
pub struct Response {
    #[serde(rename = "connection_status_event", default)]
    pub connection_status_event: Option<ConnectionStatusEvent>,
    #[serde(rename = "new_pki_document_event", default)]
    pub new_pki_document_event: Option<NewPkiDocumentEvent>,
    #[serde(rename = "session_token_reply", default)]
    pub session_token_reply: Option<SessionTokenReply>,
    #[serde(rename = "message_reply_event", default)]
    pub message_reply_event: Option<MessageReplyEvent>,
}

#[derive(Deserialize, Default)]
pub struct ConnectionStatusEvent {
    #[serde(rename = "is_connected", default)]
    pub is_connected: bool,
}

#[derive(Deserialize, Default)]
pub struct NewPkiDocumentEvent {
    #[serde(rename = "payload", default, with = "serde_bytes")]
    pub payload: Vec<u8>,
}

#[derive(Deserialize, Default)]
pub struct SessionTokenReply {
    #[serde(rename = "resumed", default)]
    pub resumed: bool,
}

#[derive(Deserialize, Default)]
pub struct MessageReplyEvent {
    // `surbid` is intentionally not decoded: Go encodes `[16]byte` in a way we
    // don't need to interpret here (only one request is in flight), and decoding
    // it strictly would risk erroring the whole frame on an encoding mismatch.
    #[serde(rename = "payload", default, with = "serde_bytes")]
    pub payload: Vec<u8>,
    #[serde(rename = "error_code", default)]
    pub error_code: u8,
}

// ---- PKI document (subset of `core/pki.Document` / `.MixDescriptor`). Untagged Go
// fields → CBOR keys are the exact Go field names ("ServiceNodes", "IdentityKey", …).

#[derive(Deserialize, Default)]
struct Document {
    // Each *MixDescriptor is embedded via Go's encoding.BinaryMarshaler, i.e. as a
    // CBOR byte string wrapping the descriptor's own CBOR map — so decode element by
    // element.
    #[serde(rename = "ServiceNodes", default)]
    service_nodes: Vec<serde_bytes::ByteBuf>,
}

#[derive(Deserialize, Default)]
struct MixDescriptor {
    #[serde(rename = "IdentityKey", default, with = "serde_bytes")]
    identity_key: Vec<u8>,
    #[serde(rename = "Kaetzchen", default)]
    kaetzchen: BTreeMap<String, BTreeMap<String, ciborium::value::Value>>,
}

// ---- Wire framing: [u32 BE length][CBOR].

async fn write_frame<W: AsyncWriteExt + Unpin>(w: &mut W, req: &Request) -> anyhow::Result<()> {
    let mut buf = Vec::new();
    ciborium::into_writer(req, &mut buf)?;
    w.write_all(&(buf.len() as u32).to_be_bytes()).await?;
    w.write_all(&buf).await?;
    w.flush().await?;
    Ok(())
}

async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> anyhow::Result<Response> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    if n > (1 << 24) {
        anyhow::bail!("thin-client frame too large: {n} bytes");
    }
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(ciborium::from_reader(&buf[..])?)
}

/// Read frames until the next `MessageReplyEvent`, ignoring intervening
/// status/PKI/ack events. Only one request is ever in flight, so the first reply
/// is ours.
async fn read_until_reply<R: AsyncReadExt + Unpin>(r: &mut R) -> anyhow::Result<MessageReplyEvent> {
    loop {
        if let Some(ev) = read_frame(r).await?.message_reply_event {
            return Ok(ev);
        }
    }
}

/// Katzenpost's `hash.Sum256` — blake2b with a 32-byte digest.
pub fn blake2b256(data: &[u8]) -> [u8; 32] {
    let mut h = Blake2b::<U32>::new();
    h.update(data);
    h.finalize().into()
}

/// A resolved destination service: its 32-byte node identity hash + queue id.
#[derive(Debug, Clone)]
pub struct Service {
    pub destination_id_hash: [u8; 32],
    pub recipient_queue_id: Vec<u8>,
}

/// Decode the PKI document, peeling any CBOR byte-string / tag wrappers the daemon
/// applies (it delivers the doc as a CBOR-encoded byte string, not a bare map).
fn decode_document(bytes: &[u8]) -> anyhow::Result<Document> {
    let mut cur = bytes.to_vec();
    for _ in 0..4 {
        let val: ciborium::value::Value = ciborium::from_reader(&cur[..])
            .map_err(|e| anyhow::anyhow!("PKI doc is not valid CBOR: {e}"))?;
        match val {
            ciborium::value::Value::Map(_) => {
                return ciborium::from_reader(&cur[..])
                    .map_err(|e| anyhow::anyhow!("failed to decode PKI Document: {e}"));
            }
            ciborium::value::Value::Bytes(inner) => cur = inner,
            ciborium::value::Value::Tag(_, boxed) => match *boxed {
                ciborium::value::Value::Bytes(inner) => cur = inner,
                other => anyhow::bail!("unexpected tagged PKI doc: {other:?}"),
            },
            other => anyhow::bail!("unexpected PKI doc CBOR top-level: {other:?}"),
        }
    }
    anyhow::bail!("PKI doc CBOR nesting too deep")
}

/// Parse the daemon's CBOR PKI document and find a service node advertising
/// `capability` (e.g. `"echo"`), returning its destination hash + queue id.
pub fn resolve_service(pki_doc: &[u8], capability: &str) -> anyhow::Result<Service> {
    let doc = decode_document(pki_doc)?;
    let debug = std::env::var("QUICMIX_KP_DEBUG").is_ok();
    let mut caps_seen = Vec::new();
    for raw in &doc.service_nodes {
        let node: MixDescriptor = match ciborium::from_reader(&raw[..]) {
            Ok(n) => n,
            Err(e) => {
                if debug {
                    eprintln!("[kp] skipping undecodable service-node descriptor: {e}");
                }
                continue;
            }
        };
        if debug {
            caps_seen.extend(node.kaetzchen.keys().cloned());
        }
        if let Some(inner) = node.kaetzchen.get(capability) {
            if let Some(ciborium::value::Value::Text(endpoint)) = inner.get("endpoint") {
                if node.identity_key.is_empty() {
                    anyhow::bail!("service {capability:?} has no IdentityKey in PKI doc");
                }
                return Ok(Service {
                    destination_id_hash: blake2b256(&node.identity_key),
                    recipient_queue_id: endpoint.as_bytes().to_vec(),
                });
            }
        }
    }
    if debug {
        eprintln!(
            "[kp] PKI doc: {} service node(s); capabilities seen: {caps_seen:?}",
            doc.service_nodes.len()
        );
    }
    anyhow::bail!("service capability {capability:?} not found in PKI document")
}

/// Result of the thin-client connection handshake.
#[derive(Debug, Clone)]
pub struct Handshake {
    /// Whether the daemon is connected to the mixnet (false = offline mode).
    pub is_connected: bool,
    /// The CBOR PKI document the daemon delivered (empty if none yet).
    pub pki_doc: Vec<u8>,
    /// Whether the daemon resumed a prior session for our instance token.
    pub session_resumed: bool,
}

/// Connect to a running Katzenpost thin-client daemon and complete the real
/// handshake. Returns the live stream and the handshake facts (incl. the PKI doc).
pub async fn connect_and_handshake(daemon: &str) -> anyhow::Result<(TcpStream, Handshake)> {
    let mut stream = TcpStream::connect(daemon).await?;

    let m1 = read_frame(&mut stream).await?;
    let cse = m1
        .connection_status_event
        .ok_or_else(|| anyhow::anyhow!("protocol: expected ConnectionStatusEvent first"))?;

    let m2 = read_frame(&mut stream).await?;
    let pki = m2
        .new_pki_document_event
        .ok_or_else(|| anyhow::anyhow!("protocol: expected NewPKIDocumentEvent second"))?;

    write_frame(
        &mut stream,
        &Request {
            session_token: Some(SessionToken {
                client_instance_token: instance_token(),
            }),
            ..Default::default()
        },
    )
    .await?;

    let m3 = read_frame(&mut stream).await?;
    let reply = m3
        .session_token_reply
        .ok_or_else(|| anyhow::anyhow!("protocol: expected SessionTokenReply"))?;

    Ok((
        stream,
        Handshake {
            is_connected: cse.is_connected,
            pki_doc: pki.payload,
            session_resumed: reply.resumed,
        },
    ))
}

/// The reply to a [`request_reply`] round-trip.
#[derive(Debug, Clone)]
pub struct Reply {
    pub error_code: u8,
    pub payload: Vec<u8>,
}

/// Full data path: handshake, resolve `capability` from the PKI doc, send a real
/// `SendMessage` (with a reply SURB) carrying `payload`, and wait up to `timeout`
/// for the matching `MessageReplyEvent`. A complete round-trip through the mixnet.
pub async fn request_reply(
    daemon: &str,
    capability: &str,
    payload: Vec<u8>,
    timeout: Duration,
) -> anyhow::Result<(Service, Reply)> {
    let (mut stream, hs) = connect_and_handshake(daemon).await?;
    if !hs.is_connected {
        anyhow::bail!("daemon is in offline mode (not connected to the mixnet)");
    }
    let service = resolve_service(&hs.pki_doc, capability)?;

    write_frame(
        &mut stream,
        &Request {
            send_message: Some(SendMessage::with_reply_surb(&service, payload)),
            ..Default::default()
        },
    )
    .await?;

    // Wait for the reply (ignoring sent-acks / PKI updates), bounded by `timeout`.
    let ev = tokio::time::timeout(timeout, read_until_reply(&mut stream))
        .await
        .map_err(|_| anyhow::anyhow!("no MessageReplyEvent within {timeout:?}"))??;

    let reply = Reply {
        error_code: ev.error_code,
        payload: ev.payload,
    };
    Ok((service, reply))
}

/// A 16-byte token. Uniqueness across reconnects/requests is all the daemon needs;
/// a process-mixed counter avoids an RNG dependency.
fn instance_token() -> [u8; 16] {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CTR: AtomicU64 = AtomicU64::new(0xA5A5_5A5A_1234_9E37);
    let mut out = [0u8; 16];
    for chunk in out.chunks_mut(8) {
        let mut x = CTR.fetch_add(0x9E3779B97F4A7C15, Ordering::Relaxed);
        x ^= x >> 30;
        x = x.wrapping_mul(0xBF58476D1CE4E5B9);
        x ^= x >> 27;
        chunk.copy_from_slice(&x.to_le_bytes()[..chunk.len()]);
    }
    out
}

/// Canonical Katzenpost `OracleParams` — the timing model the oracle-fed CC consumes
/// for this substrate. Katzenpost is a Loopix/Sphinx mixnet: 3 mix hops with a
/// per-hop exponential delay and a constant (Poisson) send rate, so it is
/// **rate-capped** like Nym — never an uncapped link. The numbers here are
/// representative defaults; derive the exact per-hop delay / send rate from the
/// daemon's PKI document (its `Mu` / `LambdaP`) on your deployment, the way
/// `realprobe` measures Nym. A ~1200 B QUIC datagram fits in one Sphinx forward
/// payload.
pub fn katzenpost_oracle() -> OracleParams {
    OracleParams {
        hops: 3,
        mean_hop_delay: Duration::from_millis(50),
        drop_prob: 0.0,
        slot_interval: Duration::from_millis(30),
        mtu: 1200,
    }
}

/// A Katzenpost [`MixTransport`] over the thin-client daemon, bound to a resolved
/// service. `send` emits a real `SendMessage`; `recv` returns `MessageReplyEvent`
/// payloads.
pub struct KatzenpostSubstrate {
    read: Mutex<ReadHalf<TcpStream>>,
    write: Mutex<WriteHalf<TcpStream>>,
    service: Service,
    oracle: OracleParams,
}

impl KatzenpostSubstrate {
    /// Connect + handshake + resolve `capability` (e.g. `"echo"`) from the PKI doc.
    pub async fn connect_service(
        daemon: &str,
        capability: &str,
        oracle: OracleParams,
    ) -> anyhow::Result<(Self, Handshake)> {
        let (stream, hs) = connect_and_handshake(daemon).await?;
        let service = resolve_service(&hs.pki_doc, capability)?;
        let (read, write) = tokio::io::split(stream);
        Ok((
            Self {
                read: Mutex::new(read),
                write: Mutex::new(write),
                service,
                oracle,
            },
            hs,
        ))
    }

    pub fn service(&self) -> &Service {
        &self.service
    }
}

#[async_trait]
impl MixTransport for KatzenpostSubstrate {
    fn oracle(&self) -> OracleParams {
        self.oracle
    }

    async fn send(&self, datagram: Vec<u8>) {
        // Legacy lossy path; production routes through Substrate -> try_send.
        let _ = self.try_send(datagram).await;
    }

    async fn recv(&self) -> Option<Vec<u8>> {
        self.try_recv().await.ok()
    }

    async fn try_send(&self, datagram: Vec<u8>) -> Result<(), SubstrateError> {
        let req = Request {
            send_message: Some(SendMessage::with_reply_surb(&self.service, datagram)),
            ..Default::default()
        };
        let mut w = self.write.lock().await;
        // A write failure means the daemon socket is gone.
        write_frame(&mut *w, &req)
            .await
            .map_err(|e| SubstrateError::Io(format!("katzenpost write: {e}")))
    }

    async fn try_recv(&self) -> Result<Vec<u8>, SubstrateError> {
        let mut r = self.read.lock().await;
        // A read failure on the daemon socket = Closed (e.g. daemon shut down).
        read_until_reply(&mut *r)
            .await
            .map(|ev| ev.payload)
            .map_err(|_| SubstrateError::Closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_cbor_roundtrips_session_token() {
        let req = Request {
            session_token: Some(SessionToken {
                client_instance_token: [7u8; 16],
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        ciborium::into_writer(&req, &mut buf).unwrap();
        assert!(!buf.is_empty());
        let val: ciborium::value::Value = ciborium::from_reader(&buf[..]).unwrap();
        match val {
            ciborium::value::Value::Map(entries) => assert!(entries.iter().any(|(k, _)| {
                matches!(k, ciborium::value::Value::Text(t) if t == "session_token")
            })),
            other => panic!("expected CBOR map, got {other:?}"),
        }
    }

    #[test]
    fn blake2b256_matches_known_vector() {
        // blake2b-256("") — RFC/standard test vector for the unkeyed 256-bit digest.
        let got = blake2b256(b"");
        let want =
            "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8";
        let hex: String = got.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, want);
    }

    /// The oracle-fed CC must build correctly from Katzenpost's params: a bounded,
    /// rate-derived BDP window (Katzenpost is a constant-rate Loopix mixnet — never
    /// the uncapped sentinel), tolerant loss timers, and a 3-hop RTT.
    #[test]
    fn cc_builds_from_katzenpost_oracle() {
        use quicmix::client::{bdp_bytes, Congestion};
        let p = katzenpost_oracle();
        for cc in [Congestion::Stock, Congestion::Timers, Congestion::Quicmix] {
            let _ = cc.transport(&p);
        }
        let w = bdp_bytes(&p);
        assert!(w >= p.mtu as u64 * 2, "window below two cells");
        assert!(
            w < 1 << 20,
            "Katzenpost is rate-capped → BDP must be bounded, got {w}"
        );
        assert!(p.loss_time_threshold() >= 1.5);
        assert_eq!(p.hops, 3);
        assert!(!p.slot_interval.is_zero(), "must be rate-capped, not uncapped");
    }
}
