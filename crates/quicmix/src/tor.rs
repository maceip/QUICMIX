//! Tor (and other stream substrates) — brought back, but adapted to the datagram
//! model so it can join the round-robin with the datagram mixnets.
//!
//! Tor is a *stream* substrate (a reliable byte stream via SOCKS5 / arti's
//! `DataStream`), not a datagram pipe. To let it participate in the eval `striped`
//! round-robin alongside Nym/Katzenpost, we frame datagrams over the stream
//! ([`StreamDatagram`], length-prefixed). This is the documented anti-pattern:
//! every datagram shares one ordered stream, so the **whole Tor path
//! head-of-line-blocks** — which is exactly why, in the multipath demo, the Tor
//! leg behaves like the "slow substrate." quicmix's CC does not apply to Tor's
//! internal transport (Tor owns that); this adapter only lets the bytes flow.
//!
//! The live arti binding (`StreamSubstrate::open` → arti `DataStream`) is a spec
//! here — it needs a live Tor network (egress-blocked in CI). The framing adapter
//! below is real and tested.

use crate::{MixTransport, OracleParams, SubstrateError, SubstrateKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

/// Map a stream I/O error into a typed [`SubstrateError`] (EOF = the Tor stream closed).
fn map_io(e: std::io::Error) -> SubstrateError {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        SubstrateError::Closed
    } else {
        SubstrateError::Io(format!("tor stream: {e}"))
    }
}

/// A bidirectional byte stream — `TcpStream`, arti `DataStream`, etc.
pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

/// A reliable, ordered byte-stream anonymity substrate (Tor-like). `open(target)`
/// returns a stream routed through the network. The live arti impl is spec'd in
/// the module docs / `realprobe`-style; not built in CI.
#[async_trait::async_trait]
pub trait StreamSubstrate: Send + Sync {
    fn kind(&self) -> SubstrateKind {
        SubstrateKind::Stream
    }
    async fn open(&self, target: &str) -> std::io::Result<Box<dyn Stream>>;
}

/// Adapts any byte stream into a [`MixTransport`] by length-prefix framing each
/// datagram. This is how a stream substrate (Tor) joins the datagram round-robin.
/// `oracle` is a configured/estimated model for the stream path (we don't control
/// the stream substrate's internals).
pub struct StreamDatagram {
    read: Mutex<Box<dyn AsyncRead + Unpin + Send>>,
    write: Mutex<Box<dyn AsyncWrite + Unpin + Send>>,
    oracle: OracleParams,
}

impl StreamDatagram {
    pub fn new<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(stream: S, oracle: OracleParams) -> Self {
        let (r, w) = tokio::io::split(stream);
        Self {
            read: Mutex::new(Box::new(r)),
            write: Mutex::new(Box::new(w)),
            oracle,
        }
    }
}

#[async_trait::async_trait]
impl MixTransport for StreamDatagram {
    fn kind(&self) -> SubstrateKind {
        SubstrateKind::Stream
    }
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
        let mut w = self.write.lock().await;
        let len = (datagram.len() as u32).to_be_bytes();
        w.write_all(&len).await.map_err(map_io)?;
        w.write_all(&datagram).await.map_err(map_io)?;
        w.flush().await.map_err(map_io)?;
        Ok(())
    }
    async fn try_recv(&self) -> Result<Vec<u8>, SubstrateError> {
        let mut r = self.read.lock().await;
        let mut len = [0u8; 4];
        r.read_exact(&mut len).await.map_err(map_io)?;
        let n = u32::from_be_bytes(len) as usize;
        if n > (1 << 20) {
            return Err(SubstrateError::Malformed);
        }
        let mut buf = vec![0u8; n];
        r.read_exact(&mut buf).await.map_err(map_io)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn oracle() -> OracleParams {
        OracleParams {
            hops: 3,
            mean_hop_delay: Duration::from_millis(20),
            drop_prob: 0.0,
            slot_interval: Duration::ZERO,
            mtu: 1200,
        }
    }

    #[tokio::test]
    async fn datagrams_frame_over_a_stream() {
        // Two ends of an in-memory duplex stand in for a Tor stream's endpoints.
        let (a, b) = tokio::io::duplex(64 * 1024);
        let sender = StreamDatagram::new(a, oracle());
        let receiver = StreamDatagram::new(b, oracle());
        sender.send(b"hello".to_vec()).await;
        sender.send(b"world".to_vec()).await;
        assert_eq!(receiver.recv().await.as_deref(), Some(&b"hello"[..]));
        assert_eq!(receiver.recv().await.as_deref(), Some(&b"world"[..]));
    }
}
