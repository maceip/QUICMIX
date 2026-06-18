//! A UDP mix-relay: insert a substrate into a real UDP path so an unmodified QUIC
//! stack (quinn) runs *over* it without a custom socket.
//!
//! Two real UDP sockets sit between client and server. Each direction's datagrams
//! pass through a [`Substrate`] boundary — **paced** (one cell per `slot_interval`),
//! **backpressured** (a BDP-sized bounded queue), and **observable** (metrics) — so
//! the relay exercises the production substrate boundary, not a raw `send`. The
//! returned [`Relay`] exposes the per-direction handles for metrics.

use crate::client::bdp_bytes;
use crate::substrate::Substrate;
use crate::{MixTransport, OracleParams};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

/// A running relay plus the observable boundary for each direction.
pub struct Relay {
    /// Address the client connects to.
    pub front: SocketAddr,
    /// Client→server boundary (paced send, queue/error metrics).
    pub up: Arc<Substrate>,
    /// Server→client boundary.
    pub down: Arc<Substrate>,
}

/// Bound queue depth (datagrams) from the oracle's BDP — not an arbitrary constant.
fn bdp_depth(p: &OracleParams) -> usize {
    (bdp_bytes(p) / p.mtu.max(1) as u64).max(16) as usize
}

/// Start a relay forwarding between a (to-be-learned) client and `server_addr`,
/// applying `up` to client→server datagrams and `down` to server→client. Each
/// direction is wrapped in a paced/fallible [`Substrate`].
pub async fn start_relay(
    server_addr: SocketAddr,
    up: Arc<dyn MixTransport>,
    down: Arc<dyn MixTransport>,
) -> std::io::Result<Relay> {
    // Queue depth from each direction's BDP.
    let up = Substrate::new(up.clone(), bdp_depth(&up.oracle()));
    let down = Substrate::new(down.clone(), bdp_depth(&down.oracle()));

    let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let back = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let front_addr = front.local_addr()?;
    let client_addr: Arc<Mutex<Option<SocketAddr>>> = Arc::new(Mutex::new(None));

    // client -> up.send (backpressured enqueue; the paced pump forwards to the
    // substrate and counts any inner send failure in `up` metrics. The loop only
    // stops if the boundary itself is dropped — `send` Err == queue gone).
    {
        let (front, up, client_addr) = (front.clone(), up.clone(), client_addr.clone());
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            while let Ok((n, addr)) = front.recv_from(&mut buf).await {
                *client_addr.lock().await = Some(addr);
                if up.send(buf[..n].to_vec()).await.is_err() {
                    break;
                }
            }
        });
    }
    // up.recv -> server
    {
        let (back, up) = (back.clone(), up.clone());
        tokio::spawn(async move {
            while let Ok(dg) = up.recv().await {
                let _ = back.send_to(&dg, server_addr).await;
            }
        });
    }
    // server -> down.send
    {
        let (back, down) = (back.clone(), down.clone());
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            while let Ok((n, _)) = back.recv_from(&mut buf).await {
                if down.send(buf[..n].to_vec()).await.is_err() {
                    break;
                }
            }
        });
    }
    // down.recv -> client
    {
        let (front, down, client_addr) = (front.clone(), down.clone(), client_addr.clone());
        tokio::spawn(async move {
            while let Ok(dg) = down.recv().await {
                if let Some(addr) = *client_addr.lock().await {
                    let _ = front.send_to(&dg, addr).await;
                }
            }
        });
    }

    Ok(Relay {
        front: front_addr,
        up,
        down,
    })
}
