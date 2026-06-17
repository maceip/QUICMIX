//! Real Tor stream substrate — `quicmix::MixTransport` over arti.
//!
//! Bootstraps real Tor, opens a circuit/stream to the gateway, and frames
//! datagrams over it (`quicmix::tor::StreamDatagram`). Because Tor is one ordered
//! stream, the whole path head-of-line-blocks — this is the documented "slow leg"
//! in the round-robin. quicmix's CC does not govern Tor's internal transport.
//!
//! Tested to the extent the sandbox allows: **compiles** against arti-client 0.43;
//! the framing it relies on is unit-tested in `quicmix::tor`. The live bootstrap
//! needs open egress to the Tor network (blocked in CI) — run on a laptop.

use arti_client::{TorClient, TorClientConfig};
use async_trait::async_trait;
use quicmix::tor::StreamDatagram;
use quicmix::{MixTransport, OracleParams, SubstrateKind};
use std::sync::Arc;
use tor_rtcompat::PreferredRuntime;

/// A bootstrapped-Tor substrate. Holds the client alive alongside the framed
/// stream so the circuit isn't torn down.
pub struct TorSubstrate {
    _client: Arc<TorClient<PreferredRuntime>>,
    inner: StreamDatagram,
}

impl TorSubstrate {
    /// Bootstrap real Tor and open a stream to `gateway` (`"host:port"`).
    pub async fn connect(gateway: &str, oracle: OracleParams) -> anyhow::Result<Self> {
        let client = TorClient::create_bootstrapped(TorClientConfig::default()).await?;
        let stream = client.connect(gateway).await?;
        Ok(Self {
            inner: StreamDatagram::new(stream, oracle),
            _client: client,
        })
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
}
