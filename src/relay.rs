//! A UDP mix-relay: insert the [`EmulatedMixnet`] into a real UDP path so an
//! unmodified QUIC stack (quinn) runs *over* it without a custom socket.
//!
//! Two real UDP sockets sit between client and server. Datagrams are shuttled
//! through one `EmulatedMixnet` per direction, which applies delay/drop/reorder.
//! This keeps quinn talking ordinary UDP while our emulator behaves like a real
//! middlebox — the same shape we'll later replace with a real Nym transport.

use crate::MixTransport;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

/// Start a relay forwarding between a (to-be-learned) client and `server_addr`,
/// applying `up` to client→server datagrams and `down` to server→client.
/// Returns the front address the client should connect to.
pub async fn start_relay(
    server_addr: SocketAddr,
    up: Arc<dyn MixTransport>,
    down: Arc<dyn MixTransport>,
) -> std::io::Result<SocketAddr> {
    let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let back = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let front_addr = front.local_addr()?;
    let client_addr: Arc<Mutex<Option<SocketAddr>>> = Arc::new(Mutex::new(None));

    // client -> up.send
    {
        let (front, up, client_addr) = (front.clone(), up.clone(), client_addr.clone());
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            while let Ok((n, addr)) = front.recv_from(&mut buf).await {
                *client_addr.lock().await = Some(addr);
                up.send(buf[..n].to_vec()).await;
            }
        });
    }
    // up.recv -> server
    {
        let (back, up) = (back.clone(), up.clone());
        tokio::spawn(async move {
            while let Some(dg) = up.recv().await {
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
                down.send(buf[..n].to_vec()).await;
            }
        });
    }
    // down.recv -> client
    {
        let (front, down, client_addr) = (front.clone(), down.clone(), client_addr.clone());
        tokio::spawn(async move {
            while let Some(dg) = down.recv().await {
                if let Some(addr) = *client_addr.lock().await {
                    let _ = front.send_to(&dg, addr).await;
                }
            }
        });
    }

    Ok(front_addr)
}
