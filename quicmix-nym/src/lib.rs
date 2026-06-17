//! Real Nym datagram substrate — `quicmix::MixTransport` over `nym-sdk`, **plus**
//! the live end-to-end wiring (T2): a client-side UDP↔Nym bridge and a gateway
//! loop, so real QUIC (quinn) — and quicmix's oracle-fed congestion control —
//! runs over the **real Nym mainnet**, not the emulator.
//!
//! Topology (both ends run on one laptop, addressed through the live mixnet):
//! ```text
//!  app → Node A ingress → quinn client ─UDP→ client bridge ─send(SURBs)→
//!        [ real Nym mixnet ] → gateway Nym client ─UDP→ Node B quinn server → origin
//!        ← send_reply(sender_tag) ← [ Nym ] ← client bridge ←UDP─ quinn client ← app
//! ```
//!
//! - [`NymSubstrate`] — the `MixTransport` client (datagram = Nym message + SURBs).
//! - [`spawn_client_bridge`] — local UDP front ⇄ `NymSubstrate` (lets unmodified
//!   quinn speak UDP while the bytes actually traverse Nym).
//! - [`NymGateway`] — a second Nym client that receives client datagrams, forwards
//!   each sender's packets to the local quinn server over a *dedicated* UDP socket
//!   (so the server sees one source per QUIC connection), and ships the server's
//!   replies back through the mix via the SURBs (`send_reply` keyed by sender tag).

use async_trait::async_trait;
use futures::StreamExt;
use nym_sdk::mixnet::{
    AnonymousSenderTag, IncludedSurbs, MixnetClient, MixnetClientSender, MixnetMessageSender,
    Recipient,
};
use quicmix::rotation::FrontFactory;
use quicmix::{MixTransport, OracleParams};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

/// Bootstrap an ephemeral Nym client over wss/443 (works behind 443-only egress,
/// and reliably on an open-egress laptop — the same path `realprobe` measured).
async fn build_client() -> anyhow::Result<MixnetClient> {
    Ok(nym_sdk::mixnet::MixnetClientBuilder::new_ephemeral()
        .force_tls(true)
        .build()?
        .connect_to_mixnet()
        .await?)
}

pub struct NymSubstrate {
    sender: MixnetClientSender,
    client: Mutex<MixnetClient>,
    recipient: Recipient,
    surbs: u32,
    oracle: OracleParams,
}

impl NymSubstrate {
    /// Bootstrap an ephemeral Nym client and target `gateway` (a base58 Nym
    /// address). `surbs` is the reply-SURB budget per datagram (the gateway needs
    /// these to send responses back through the mix; nym-client-core auto-requests
    /// more when the reply store runs low, so this is a per-datagram floor).
    pub async fn connect(gateway: &str, surbs: u32, oracle: OracleParams) -> anyhow::Result<Self> {
        let recipient = parse_recipient(gateway)?;
        let client = build_client().await?;
        let sender = client.split_sender();
        Ok(Self {
            sender,
            client: Mutex::new(client),
            recipient,
            surbs,
            oracle,
        })
    }

    /// This client's own Nym address (so a gateway/peer can address it).
    pub async fn our_address(&self) -> String {
        self.client.lock().await.nym_address().to_string()
    }
}

fn parse_recipient(s: &str) -> anyhow::Result<Recipient> {
    Recipient::try_from_base58_string(s)
        .map_err(|e| anyhow::anyhow!("invalid Nym recipient {s:?}: {e:?}"))
}

#[async_trait]
impl MixTransport for NymSubstrate {
    fn oracle(&self) -> OracleParams {
        self.oracle
    }

    async fn send(&self, datagram: Vec<u8>) {
        let _ = self
            .sender
            .send_message(self.recipient, datagram, IncludedSurbs::new(self.surbs))
            .await;
    }

    async fn recv(&self) -> Option<Vec<u8>> {
        let mut client = self.client.lock().await;
        client.next().await.map(|m| m.message)
    }
}

/// Client-side bridge: bind a local UDP socket and shuttle datagrams between it and
/// `substrate`. quinn connects to the returned address and speaks ordinary UDP; the
/// bytes traverse the real Nym mixnet. Returns the front address for `Node::connect`.
pub async fn spawn_client_bridge(substrate: Arc<NymSubstrate>) -> anyhow::Result<SocketAddr> {
    let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let front_addr = front.local_addr()?;
    // Learned address of the local quinn client (set on its first packet out).
    let peer: Arc<Mutex<Option<SocketAddr>>> = Arc::new(Mutex::new(None));

    // quinn → Nym (send with SURBs to the gateway).
    {
        let (front, sub, peer) = (front.clone(), substrate.clone(), peer.clone());
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            while let Ok((n, addr)) = front.recv_from(&mut buf).await {
                *peer.lock().await = Some(addr);
                sub.send(buf[..n].to_vec()).await;
            }
        });
    }
    // Nym → quinn (replies arriving back through the mix).
    {
        let (front, sub, peer) = (front.clone(), substrate.clone(), peer.clone());
        tokio::spawn(async move {
            while let Some(dg) = sub.recv().await {
                if let Some(addr) = *peer.lock().await {
                    let _ = front.send_to(&dg, addr).await;
                }
            }
        });
    }
    Ok(front_addr)
}

/// A [`quicmix::rotation::FrontFactory`] over Nym: **each call bootstraps a fresh
/// ephemeral Nym client** (new identity → new SURB context) behind a new UDP
/// bridge to `gateway`. That is what makes a rotated circuit unlinkable at the Nym
/// layer too — not just fresh QUIC keys, but a fresh mixnet pseudo-identity. Drop
/// it into [`quicmix::rotation::connect_fresh_with`] / `CircuitPool::prewarm_with`
/// to pre-warm and rotate **real** Nym circuits.
pub fn nym_front(gateway: String, surbs: u32, oracle: OracleParams) -> FrontFactory {
    Arc::new(move || {
        let gateway = gateway.clone();
        Box::pin(async move {
            let sub = Arc::new(NymSubstrate::connect(&gateway, surbs, oracle).await?);
            spawn_client_bridge(sub).await
        })
    })
}

/// Gateway-side Nym client: receives client datagrams over the mix and bridges them
/// to a local quinn server ([`quicmix::node::Node::serve_gateway`]), replying through
/// the SURBs each client attached.
pub struct NymGateway {
    client: MixnetClient,
    nym_address: String,
}

impl NymGateway {
    /// Bootstrap the gateway's own ephemeral Nym identity. Its [`Self::nym_address`]
    /// is what clients target.
    pub async fn connect() -> anyhow::Result<Self> {
        let client = build_client().await?;
        let nym_address = client.nym_address().to_string();
        Ok(Self {
            client,
            nym_address,
        })
    }

    /// The gateway's Nym address (give this to clients as the recipient).
    pub fn nym_address(&self) -> &str {
        &self.nym_address
    }

    /// Run the bridge loop forever: each distinct `sender_tag` gets a dedicated UDP
    /// socket to `server_udp` (so the quinn server distinguishes QUIC connections by
    /// source address), client→server packets go out that socket, and server→client
    /// packets are returned through the mix via `send_reply(sender_tag, …)`.
    pub async fn run(mut self, server_udp: SocketAddr) -> anyhow::Result<()> {
        let sender = self.client.split_sender();
        let socks: Arc<Mutex<HashMap<AnonymousSenderTag, Arc<UdpSocket>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        while let Some(msg) = self.client.next().await {
            let Some(tag) = msg.sender_tag else { continue };
            let sock = {
                let mut m = socks.lock().await;
                match m.get(&tag) {
                    Some(s) => s.clone(),
                    None => {
                        let s = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
                        s.connect(server_udp).await?;
                        m.insert(tag, s.clone());
                        // Pump this client's server replies back through the mix.
                        let (reader, replier) = (s.clone(), sender.clone());
                        tokio::spawn(async move {
                            let mut buf = vec![0u8; 65535];
                            while let Ok(n) = reader.recv(&mut buf).await {
                                let _ = replier.send_reply(tag, buf[..n].to_vec()).await;
                            }
                        });
                        s
                    }
                }
            };
            let _ = sock.send(&msg.message).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_bad_recipient() {
        assert!(parse_recipient("not-a-real-nym-address").is_err());
    }
}
