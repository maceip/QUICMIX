//! Unlinkable circuit rotation.
//!
//! The logical session lives **end-to-end inside the tunnel** (a session id sent
//! as the first bytes of a stream), so the underlying QUIC connection + mix
//! circuit are disposable. Each rotation uses a **fresh** client endpoint (no
//! session cache → no TLS resumption ticket), a **fresh** mix relay path, and
//! therefore fresh keys, Connection IDs, and apparent source address — nothing
//! links it to the previous circuit at the transport layer. A **pre-warmed pool**
//! hides the setup handshake so a rotation is ~instant instead of paying a mix
//! round-trip.

use crate::emulator::EmulatedMixnet;
use crate::relay::start_relay;
use crate::OracleParams;
use anyhow::Result;
use quinn::{ClientConfig, Connection, Endpoint, TransportConfig};
use rustls::pki_types::CertificateDer;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

/// A live circuit: the QUIC connection plus the endpoint that owns it (kept alive
/// so the connection isn't dropped) and its local source address.
pub struct Circuit {
    pub endpoint: Endpoint,
    pub conn: Connection,
    pub local: SocketAddr,
}

/// Provisions a **fresh** local UDP front for one new circuit and returns the
/// address a fresh quinn client should dial. The substrate lives behind it: the
/// emulator ([`emulated_front`]) spins up a new in-process mix relay; a real
/// substrate (e.g. `quicmix-nym`'s `nym_front`) bootstraps a fresh Nym client
/// (new identity / SURB context) behind a UDP bridge. Each call MUST yield an
/// independent circuit so consecutive ones share nothing observable on the wire.
///
/// This is the seam that decouples rotation from any one substrate: rotation logic
/// (fresh endpoint → no resumption → fresh keys/CIDs/source addr) is identical; only
/// the front factory differs.
pub type FrontFactory =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<SocketAddr>> + Send>> + Send + Sync>;

/// Emulator front factory: each call builds a fresh in-process mix relay to
/// `server_addr` (the original, hardcoded behaviour, now just one factory).
pub fn emulated_front(server_addr: SocketAddr, p: OracleParams) -> FrontFactory {
    Arc::new(move || {
        Box::pin(async move {
            start_relay(
                server_addr,
                Arc::new(EmulatedMixnet::new(p)),
                Arc::new(EmulatedMixnet::new(p)),
            )
            .await
            .map(|r| r.front)
            .map_err(anyhow::Error::from)
        })
    })
}

/// Build one fresh, unlinkable circuit over whatever substrate `make_front`
/// provisions: a new client endpoint (no shared session storage → no TLS
/// resumption ticket → cryptographically independent) dialing the fresh front.
///
/// `transport` is the quinn `TransportConfig` for the new connection. On a real
/// mixnet this **must** be the oracle-tuned config (seconds-scale `initial_rtt`,
/// long idle timeout) or the connection times out before the first multi-second
/// RTT completes — pass `Some(Congestion::Quicmix.transport(&oracle))`. `None`
/// uses quinn defaults (fine for the ms-scale emulator).
pub async fn connect_fresh_with(
    make_front: &FrontFactory,
    server_cert: CertificateDer<'static>,
    transport: Option<Arc<TransportConfig>>,
) -> Result<Circuit> {
    let front = make_front().await?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(server_cert)?;
    // Build the TLS config ourselves so we can **explicitly disable resumption**:
    // with no session store the client can never present a resumption ticket, so
    // every rotated circuit is a full, cryptographically-independent handshake —
    // proven by configuration, not just by "fresh endpoint" reasoning.
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.resumption = rustls::client::Resumption::disabled();
    let quic = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
        .map_err(|e| anyhow::anyhow!("quic client config: {e}"))?;
    let mut client_config = ClientConfig::new(Arc::new(quic));
    if let Some(t) = transport {
        client_config.transport_config(t);
    }
    let mut endpoint = Endpoint::client("127.0.0.1:0".parse()?)?;
    endpoint.set_default_client_config(client_config);
    let local = endpoint.local_addr()?;
    let conn = endpoint.connect(front, "localhost")?.await?;
    Ok(Circuit {
        endpoint,
        conn,
        local,
    })
}

/// Build one fresh, unlinkable circuit over the **emulator** (a new mix relay path
/// to `server_addr`). Thin wrapper over [`connect_fresh_with`] + [`emulated_front`]
/// (quinn-default transport — the emulator's RTT is milliseconds).
pub async fn connect_fresh(
    server_addr: SocketAddr,
    server_cert: CertificateDer<'static>,
    p: OracleParams,
) -> Result<Circuit> {
    connect_fresh_with(&emulated_front(server_addr, p), server_cert, None).await
}

/// A pool of pre-warmed, unlinkable circuits ready for instant rotation.
pub struct CircuitPool {
    warm: Vec<Circuit>,
}

impl CircuitPool {
    /// Establish `n` circuits ahead of time over any substrate `make_front`
    /// provisions (under cover, in a real deployment). On a real mixnet this
    /// pre-pays the *entire* per-circuit bootstrap (e.g. a fresh Nym client),
    /// which is exactly what makes a hot-path rotation cheap.
    pub async fn prewarm_with(
        n: usize,
        make_front: &FrontFactory,
        server_cert: CertificateDer<'static>,
        transport: Option<Arc<TransportConfig>>,
    ) -> Result<Self> {
        let mut warm = Vec::with_capacity(n);
        for _ in 0..n {
            warm.push(connect_fresh_with(make_front, server_cert.clone(), transport.clone()).await?);
        }
        Ok(Self { warm })
    }

    /// Establish `n` circuits ahead of time over the **emulator**. Thin wrapper
    /// over [`Self::prewarm_with`] + [`emulated_front`].
    pub async fn prewarm(
        n: usize,
        server_addr: SocketAddr,
        server_cert: CertificateDer<'static>,
        p: OracleParams,
    ) -> Result<Self> {
        Self::prewarm_with(n, &emulated_front(server_addr, p), server_cert, None).await
    }

    /// Take a ready circuit (the rotation itself — no handshake on the hot path).
    pub fn take(&mut self) -> Option<Circuit> {
        self.warm.pop()
    }

    pub fn len(&self) -> usize {
        self.warm.len()
    }
}

/// Send one message over a circuit, prefixed with the end-to-end session id, and
/// wait for the peer's ack (a request/response round-trip). The session id is
/// *inside* the encrypted stream, so it rebinds the logical session across
/// circuits without ever being visible to the network/relay.
pub async fn session_send(conn: &Connection, session_id: [u8; 8], msg: &[u8]) -> Result<()> {
    let (mut s, mut r) = conn.open_bi().await?;
    let mut buf = session_id.to_vec();
    buf.extend_from_slice(msg);
    s.write_all(&buf).await?;
    s.finish()?;
    let _ack = r.read_to_end(64).await?;
    Ok(())
}
