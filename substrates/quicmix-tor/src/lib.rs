//! Real Tor stream substrate for quicmix — over the **native C Tor daemon** (no arti).
//!
//! Frames quicmix datagrams over a SOCKS5 stream through Tor to a gateway's
//! stream-bridge. The heavy lifting lives in [`quicmix::tor`] (the SOCKS5 client,
//! the [`quicmix::tor::TorDaemon`] supervisor for the native `tor` process, and the
//! [`quicmix::tor::StreamDatagram`] framing); this crate is a thin [`MixTransport`]
//! wrapper that owns one circuit (and optionally the daemon).
//!
//! Because Tor is one ordered stream, the whole path head-of-line-blocks — the
//! documented "slow leg." quicmix's CC does not govern Tor's internal transport.

use async_trait::async_trait;
use quicmix::tor::{tor_socks_addr, tor_stream_to, StreamDatagram, TorDaemon};
use quicmix::{MixTransport, OracleParams, SubstrateError, SubstrateKind};

/// A Tor-carried datagram substrate. Holds the framed circuit; if it launched its
/// own native `tor`, the supervisor is held alive too (killed on drop).
pub struct TorSubstrate {
    inner: StreamDatagram,
    _daemon: Option<TorDaemon>,
}

impl TorSubstrate {
    /// Open a Tor circuit to `gateway` (`host:port` of the gateway stream-bridge)
    /// via an already-running native Tor SOCKS proxy (system service, or
    /// `QUICMIX_TOR_SOCKS`).
    pub async fn connect(gateway: &str, oracle: OracleParams) -> anyhow::Result<Self> {
        let inner = tor_stream_to(tor_socks_addr(), gateway, oracle).await?;
        Ok(Self { inner, _daemon: None })
    }

    /// Launch and supervise our **own** native `tor`, then open the circuit through
    /// it — fully self-contained, no system service required.
    pub async fn connect_managed(gateway: &str, oracle: OracleParams) -> anyhow::Result<Self> {
        let daemon = TorDaemon::launch().await?;
        let inner = tor_stream_to(daemon.socks, gateway, oracle).await?;
        Ok(Self { inner, _daemon: Some(daemon) })
    }
}

#[async_trait]
impl MixTransport for TorSubstrate {
    fn kind(&self) -> SubstrateKind {
        SubstrateKind::Stream
    }
    fn oracle(&self) -> OracleParams {
        self.inner.oracle()
    }
    async fn send(&self, datagram: Vec<u8>) {
        self.inner.send(datagram).await
    }
    async fn recv(&self) -> Option<Vec<u8>> {
        self.inner.recv().await
    }
    async fn try_send(&self, datagram: Vec<u8>) -> Result<(), SubstrateError> {
        self.inner.try_send(datagram).await
    }
    async fn try_recv(&self) -> Result<Vec<u8>, SubstrateError> {
        self.inner.try_recv().await
    }
}
