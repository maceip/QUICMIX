//! Gateway directory, auto-promotion, and (future) gossip.
//!
//! Architecture vision: a few **bootstrap** nodes on well-known IPs; any node
//! with a **public, reachable IP is auto-promoted to gateway** and announces
//! itself; the set of **live gateways is gossiped** so clients discover and
//! rotate across them. Since every node is the same binary (gateway is a role),
//! the gateway pool grows with adoption — Tor-style fungibility.
//!
//! This module is the concrete home for that: an in-memory directory that nodes
//! `announce` into (on self-promotion, or on receiving a gossip announcement) and
//! `sample` from (for exit/circuit selection + rotation). The **gossip transport**
//! (how announcements propagate, anti-entropy, signing) is future work; the
//! directory, the auto-promotion policy gate, expiry, and sampling are real here.

use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A live gateway peers can use as an exit.
#[derive(Clone, Debug)]
pub struct GatewayInfo {
    pub addr: SocketAddr,
    /// The gateway's quicmix identity (cert DER) that peers trust to connect.
    pub cert: Vec<u8>,
    pub last_seen: Instant,
}

/// In-memory directory of live gateways. Entries expire after `ttl` unless
/// refreshed by a fresh announcement (so a crashed gateway drops out).
pub struct GatewayDirectory {
    gateways: Mutex<HashMap<SocketAddr, GatewayInfo>>,
    ttl: Duration,
}

impl GatewayDirectory {
    pub fn new(ttl: Duration) -> Self {
        Self {
            gateways: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Record/refresh a gateway (from a gossip announcement or self-announce).
    pub fn announce(&self, addr: SocketAddr, cert: Vec<u8>) {
        self.gateways.lock().unwrap().insert(
            addr,
            GatewayInfo {
                addr,
                cert,
                last_seen: Instant::now(),
            },
        );
    }

    /// Currently-live gateways (non-expired).
    pub fn live(&self) -> Vec<GatewayInfo> {
        let now = Instant::now();
        let mut g = self.gateways.lock().unwrap();
        g.retain(|_, v| now.duration_since(v.last_seen) < self.ttl);
        g.values().cloned().collect()
    }

    /// Sample one live gateway for an exit/circuit. Clients rotate across these.
    pub fn sample(&self) -> Option<GatewayInfo> {
        self.live().choose(&mut rand::thread_rng()).cloned()
    }
}

/// Auto-promotion policy gate: a node qualifies as a gateway if it has a public,
/// routable address. (Active reachability probing is future work; this is the
/// address-level gate.)
pub fn qualifies_as_gateway(addr: SocketAddr) -> bool {
    let ip = addr.ip();
    !(ip.is_loopback() || ip.is_unspecified() || is_private(ip))
}

/// Discover the IP the host would use to reach the internet.
///
/// "Connects" a UDP socket to a public address: no packets are sent, but the OS
/// selects the egress interface and `local_addr()` then reveals the source IP it
/// would use. On a cloud host with a public IP bound to its NIC (e.g. a
/// DigitalOcean droplet) this is the **public** IP; behind NAT it's the private
/// RFC1918 address. Feeding the result to [`qualifies_as_gateway`] is the
/// auto-promotion decision. Returns `None` if no route/interface is available.
pub fn discover_egress_ip() -> Option<IpAddr> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    // 1.1.1.1:53 is just a routing target; UDP connect sends nothing on the wire.
    sock.connect("1.1.1.1:53").ok()?;
    sock.local_addr().ok().map(|a| a.ip())
}

fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_private() || v4.is_link_local(),
        IpAddr::V6(v6) => {
            let s0 = v6.segments()[0];
            (s0 & 0xfe00) == 0xfc00 /* unique-local fc00::/7 */ || (s0 & 0xffc0) == 0xfe80 /* link-local */
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_promotion_gate() {
        assert!(qualifies_as_gateway("8.8.8.8:443".parse().unwrap()));
        assert!(!qualifies_as_gateway("127.0.0.1:443".parse().unwrap()));
        assert!(!qualifies_as_gateway("192.168.1.5:443".parse().unwrap()));
        assert!(!qualifies_as_gateway("10.0.0.1:443".parse().unwrap()));
    }

    #[test]
    fn directory_announce_live_sample_and_expiry() {
        let dir = GatewayDirectory::new(Duration::from_millis(50));
        dir.announce("8.8.8.8:443".parse().unwrap(), vec![1, 2, 3]);
        dir.announce("1.1.1.1:443".parse().unwrap(), vec![4, 5, 6]);
        assert_eq!(dir.live().len(), 2);
        assert!(dir.sample().is_some());
        std::thread::sleep(Duration::from_millis(70));
        assert_eq!(dir.live().len(), 0, "stale gateways expire");
        assert!(dir.sample().is_none());
    }
}
