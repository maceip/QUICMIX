//! Tor (and other stream substrates) — adapted to the datagram model so it can
//! join the round-robin with the datagram mixnets.
//!
//! Tor is a *stream* substrate (a reliable byte stream), not a datagram pipe. To
//! let it carry quicmix's QUIC we frame datagrams over the stream
//! ([`StreamDatagram`], length-prefixed). Every datagram shares one ordered
//! stream, so the **whole Tor path head-of-line-blocks** — the documented "slow
//! leg." quicmix's CC does not apply to Tor's internal transport (Tor owns that);
//! this adapter only lets the bytes flow.
//!
//! ## Native Tor, not arti
//!
//! The live binding uses the **native C Tor daemon** (`tor`), reached over its
//! SOCKS5 proxy ([`socks5_connect`]). There is no arti / `libsqlite3-sys` in the
//! tree: SOCKS5 is plain TCP, so the Tor substrate links nothing heavy and has no
//! native-dep conflicts. The daemon can be the system service or one this process
//! launches and supervises ([`TorDaemon`]). [`tor_stream_to`] opens a real circuit
//! to a gateway's stream-bridge (`host:port`) and returns a framed datagram
//! transport ready for [`crate::front::spawn_substrate_front`].

use crate::{MixTransport, OracleParams, SubstrateError, SubstrateKind};
use anyhow::{anyhow, Context, Result};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
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

/// SOCKS address of the native Tor daemon: `QUICMIX_TOR_SOCKS` or `127.0.0.1:9050`.
pub fn tor_socks_addr() -> SocketAddr {
    std::env::var("QUICMIX_TOR_SOCKS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| "127.0.0.1:9050".parse().unwrap())
}

/// Is a Tor SOCKS proxy reachable at `socks` (i.e. is the native daemon up)?
pub async fn tor_socks_reachable(socks: SocketAddr) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(socks)).await,
        Ok(Ok(_))
    )
}

/// Open a TCP stream to `host:port` through a SOCKS5 proxy (the native Tor
/// `SocksPort`), with **remote DNS** (ATYP=domain) so the name is resolved inside
/// the Tor network and never leaks locally. No authentication (Tor's default).
pub async fn socks5_connect(socks: SocketAddr, host: &str, port: u16) -> Result<TcpStream> {
    let mut s = TcpStream::connect(socks)
        .await
        .with_context(|| format!("connect tor socks {socks}"))?;
    s.set_nodelay(true).ok();
    // greeting: VER=5, NMETHODS=1, METHOD=0 (no-auth)
    s.write_all(&[0x05, 0x01, 0x00]).await?;
    let mut sel = [0u8; 2];
    s.read_exact(&mut sel).await?;
    if sel != [0x05, 0x00] {
        return Err(anyhow!("socks5: no-auth method rejected (got {sel:?})"));
    }
    // CONNECT (CMD=1), ATYP=3 (domain) for remote resolution
    let host_b = host.as_bytes();
    if host_b.len() > 255 {
        return Err(anyhow!("socks5: host too long"));
    }
    let mut req = vec![0x05, 0x01, 0x00, 0x03, host_b.len() as u8];
    req.extend_from_slice(host_b);
    req.extend_from_slice(&port.to_be_bytes());
    s.write_all(&req).await?;
    // reply: VER REP RSV ATYP BND.ADDR BND.PORT
    let mut head = [0u8; 4];
    s.read_exact(&mut head).await?;
    if head[1] != 0x00 {
        return Err(anyhow!("socks5: CONNECT failed (rep={}) — exit policy may block {host}:{port}", head[1]));
    }
    let addr_len = match head[3] {
        0x01 => 4,
        0x04 => 16,
        0x03 => {
            let mut l = [0u8; 1];
            s.read_exact(&mut l).await?;
            l[0] as usize
        }
        other => return Err(anyhow!("socks5: bad bound ATYP {other}")),
    };
    let mut skip = vec![0u8; addr_len + 2];
    s.read_exact(&mut skip).await?;
    Ok(s)
}

/// Build a real Tor-carried datagram transport to a gateway's **stream-bridge**
/// (`gateway_bridge` = `host:port`, the [`crate::stream_bridge`] TCP listener) via
/// the native Tor SOCKS proxy `socks`. Returns a [`StreamDatagram`] ready to be
/// wrapped by [`crate::front::spawn_substrate_front`] into a local UDP front that
/// quinn dials — so quicmix's QUIC genuinely traverses a Tor circuit.
pub async fn tor_stream_to(
    socks: SocketAddr,
    gateway_bridge: &str,
    oracle: OracleParams,
) -> Result<StreamDatagram> {
    let (host, port) = split_host_port(gateway_bridge)?;
    let stream = socks5_connect(socks, &host, port)
        .await
        .with_context(|| format!("tor circuit to {gateway_bridge}"))?;
    Ok(StreamDatagram::new(stream, oracle))
}

fn split_host_port(s: &str) -> Result<(String, u16)> {
    let (h, p) = s.rsplit_once(':').ok_or_else(|| anyhow!("expected host:port, got {s:?}"))?;
    Ok((h.to_string(), p.parse().map_err(|e| anyhow!("bad port in {s:?}: {e}"))?))
}

/// A **native Tor daemon supervised by this process** (no arti). Launches `tor`
/// with an ephemeral data dir + SOCKS port, waits for `Bootstrapped 100%`, and
/// kills it on drop. Use when no system Tor service is available; otherwise prefer
/// the running service via [`tor_socks_addr`].
pub struct TorDaemon {
    child: tokio::process::Child,
    pub socks: SocketAddr,
    #[allow(dead_code)]
    data_dir: std::path::PathBuf,
}

impl TorDaemon {
    /// Launch and bootstrap a native `tor` on an ephemeral SOCKS port.
    pub async fn launch() -> Result<Self> {
        let port = {
            // grab a free localhost port, then hand it to tor
            let l = std::net::TcpListener::bind("127.0.0.1:0")?;
            l.local_addr()?.port()
        };
        let socks: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let data_dir = std::env::temp_dir().join(format!("quicmix-tor-{}", std::process::id()));
        std::fs::create_dir_all(&data_dir).ok();

        let mut child = tokio::process::Command::new("tor")
            .args([
                "--SocksPort",
                &port.to_string(),
                "--DataDirectory",
                &data_dir.to_string_lossy(),
                "--Log",
                "notice stdout",
                "--ClientOnly",
                "1",
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .context("spawning native `tor` (is the tor package installed?)")?;

        let stdout = child.stdout.take().ok_or_else(|| anyhow!("tor: no stdout"))?;
        let mut lines = BufReader::new(stdout).lines();
        let booted = tokio::time::timeout(Duration::from_secs(90), async {
            while let Some(line) = lines.next_line().await? {
                if line.contains("Bootstrapped 100%") {
                    return Ok::<(), anyhow::Error>(());
                }
            }
            Err(anyhow!("tor exited before bootstrap"))
        })
        .await
        .map_err(|_| anyhow!("tor did not bootstrap within 90s"))?;
        booted?;
        // keep draining logs so the pipe never blocks the daemon
        tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
        Ok(Self { child, socks, data_dir })
    }
}

impl Drop for TorDaemon {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
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
