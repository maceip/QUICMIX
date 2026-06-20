//! Gateway-side **stream bridge** — the inverse of the client's substrate front.
//!
//! Stream substrates (Tor) carry quicmix's QUIC datagrams length-prefix framed over
//! ONE ordered byte stream (see [`crate::tor::StreamDatagram`]). A Tor *exit*,
//! however, speaks TCP, while the quicmix gateway listens for QUIC on UDP. This
//! bridge closes that gap: it accepts the exit's TCP connection, de-frames each
//! datagram and injects it into the local quinn listener over UDP, and frames the
//! gateway's UDP replies back onto the same stream.
//!
//! Wire format is identical to [`crate::tor::StreamDatagram`]: a 4-byte big-endian
//! length followed by that many payload bytes. One fresh local UDP socket is bound
//! per accepted TCP connection so quinn sees each bridged client as a distinct peer
//! (its own source address), exactly as if it had arrived directly.
//!
//! Enabled on a gateway by setting `QUICMIX_STREAM_BRIDGE_BIND` (e.g. `0.0.0.0:443`,
//! a port Tor exit policies commonly allow). This is substrate-agnostic: anything
//! that delivers the framed stream over TCP (a Tor exit, an arti `DataStream` whose
//! sidecar forwards over TCP, etc.) reaches the gateway through it.

use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

/// Reject absurd frame lengths (matches [`crate::tor::StreamDatagram`]'s 1 MiB cap).
const MAX_DATAGRAM: usize = 1 << 20;

/// Run the stream bridge on `bind` (TCP), forwarding framed datagrams to/from the
/// local QUIC gateway at `quic_local` (UDP, e.g. `127.0.0.1:4433`). Spawns a task
/// and returns once the listener is bound.
pub async fn serve(bind: SocketAddr, quic_local: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(bind).await?;
    println!("stream-bridge: TCP {bind} <-> local QUIC udp {quic_local} (framed datagrams)");
    tokio::spawn(async move {
        loop {
            let Ok((tcp, peer)) = listener.accept().await else { continue };
            tokio::spawn(async move {
                if let Err(e) = handle(tcp, quic_local).await {
                    eprintln!("stream-bridge: conn {peer} ended: {e}");
                }
            });
        }
    });
    Ok(())
}

async fn handle(tcp: TcpStream, quic_local: SocketAddr) -> Result<()> {
    tcp.set_nodelay(true).ok();
    // A fresh UDP socket per TCP conn => a unique source addr, so quinn treats each
    // bridged client as its own peer/path.
    let udp = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    udp.connect(quic_local).await?;
    let (mut rd, mut wr) = tcp.into_split();

    // TCP (framed) -> UDP (into the local quinn gateway).
    let to_udp = {
        let udp = udp.clone();
        async move {
            let mut len = [0u8; 4];
            loop {
                if rd.read_exact(&mut len).await.is_err() {
                    break;
                }
                let n = u32::from_be_bytes(len) as usize;
                if n == 0 || n > MAX_DATAGRAM {
                    break;
                }
                let mut buf = vec![0u8; n];
                if rd.read_exact(&mut buf).await.is_err() {
                    break;
                }
                if udp.send(&buf).await.is_err() {
                    break;
                }
            }
        }
    };

    // UDP (gateway replies) -> TCP (framed back over the stream).
    let to_tcp = {
        let udp = udp.clone();
        async move {
            let mut buf = vec![0u8; 65535];
            loop {
                let n = match udp.recv(&mut buf).await {
                    Ok(n) => n,
                    Err(_) => break,
                };
                let len = (n as u32).to_be_bytes();
                if wr.write_all(&len).await.is_err() || wr.write_all(&buf[..n]).await.is_err() {
                    break;
                }
                if wr.flush().await.is_err() {
                    break;
                }
            }
        }
    };

    // When either direction ends (stream closed / peer gone), drop the other.
    tokio::select! {
        _ = to_udp => {},
        _ = to_tcp => {},
    }
    Ok(())
}
