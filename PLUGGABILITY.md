# quicmix pluggability

Two independent axes are designed to be swappable: the **client side** (which QUIC
stack / congestion logic / oracle you use) and the **substrate** (which anonymity
network carries the traffic). This doc defines the plug points and is honest about
where a given substrate — notably **Tor** — does and doesn't fit.

---

## Axis 1 — client side

| plug point | trait / hook | shipped | alternatives |
|---|---|---|---|
| **QUIC stack** | `QuicStack` (design) | `quinn` | `s2n-quic` (verified pluggable CC), `quiche` |
| **Congestion / scheduler** | `quinn::congestion::ControllerFactory` | `sched::OracleCc` | any `Controller`; or s2n-quic's `congestion_controller::Provider` |
| **Oracle source** | `OracleParams` provider | exact (emulator) | `oracle::OracleEstimator` (measured, online) |
| **Rotation policy** | `rotation::CircuitPool` params | pre-warm N + take | trigger/budget/pool-size are parameters |

- The **congestion controller is a real, concrete plug today**: `OracleCc`
  implements quinn's `ControllerFactory` (constant window + no-op
  `on_congestion_event`). Swap it for stock CUBIC/BBR or any custom controller via
  `TransportConfig::congestion_controller_factory`.
- The **QUIC stack** is pluggable in principle because the two leading Rust stacks
  both expose custom congestion control (verified: quinn's `Controller`, s2n-quic's
  `Provider`). `QuicStack` would abstract "build client/server endpoint + apply a
  controller + open/accept streams"; today only the quinn path is implemented.
  Porting to s2n-quic is an isolated change behind that trait.
- The **oracle source** is already two implementations of the same `OracleParams`
  shape: exact (set by the emulator / a self-hosted topology) and measured
  (`OracleEstimator`, fed by observed RTT + ack/loss). The scheduler consumes
  `OracleParams` and does not care which produced them.

Adding a new client-side stack: implement the controller for that stack's CC hook
(or `QuicStack`), keep everything above it (`rotation`, oracle) unchanged.

---

## Axis 2 — substrate

There are **two kinds** of substrate (`SubstrateKind`), and they are not
interchangeable:

| | **Datagram** (mixnet) | **Stream** (Tor-like) |
|---|---|---|
| plug point | `MixTransport` | `tor::StreamSubstrate` |
| examples | `EmulatedMixnet`, Nym, Loopix/Sphinx nets | Tor (SOCKS5 / `arti`), I2P streams |
| transport | unreliable, reordering datagrams | reliable, ordered byte stream |
| QUIC fit | **native** (1 datagram = 1 cell) | **none** — no UDP; QUIC only by tunneling over TCP (HoL blocking, double CC) |
| threat model | global/coalition observer (with cover) | low-latency; **not** GPA-resistant |

### Datagram substrates (the main path)
`MixTransport` is the seam: `oracle()` + `send`/`recv`. The emulator and Nym
implement it; any other datagram mixnet (Loopix/Sphinx-style, HOPR, …) plugs in
the same way. **All of quicmix's contributions apply here**: oracle-fed congestion
control, fixed-size cells / cover, and unlinkable rotation.

Adding a new datagram substrate: implement `MixTransport` (map one QUIC datagram
to one cell; provide `oracle()` exact-or-measured). Nothing above it changes —
that's the whole point of the seam.

### Tor (and other stream substrates) — what plugs in, and what doesn't

Tor is reachable as a SOCKS5 proxy (or via `arti`). It is a **stream** substrate,
so `tor::StreamSubstrate::open(target) -> TcpStream` is the right shape, **not**
`MixTransport`. The honest consequences:

- ❌ **QUIC doesn't run natively over Tor** — Tor carries TCP only. Tunneling QUIC
  datagrams inside a Tor TCP stream reintroduces head-of-line blocking and stacks
  two congestion controllers; you'd just use the TCP stream directly.
- ❌ **quicmix's oracle-fed congestion control does not apply** — the path's CC is
  Tor's; there's no datagram pipe to pace to a slot budget.
- ❌ **No fixed-size cells / cover control** from the client; and Tor is
  **low-latency, not global-observer-resistant** — the exact adversary the
  datagram path targets. So Tor is a *fast, compatible fallback*, not a peer of
  the GPA-resistant substrates.
- ✅ **What does port:** the end-to-end session id (`rotation::session_send`)
  rebinds a logical session across circuits on any substrate, and Tor has its own
  unlinkable rotation (`NEWNYM`). So the *rotation* capability has a natural
  analogue over Tor, even though the pre-warmed-pool latency win is Tor's to give.

### Stream substrates (Tor) — re-added as a datagram adapter (the "slow leg")

Tor was first removed, then **brought back** — adapted to the datagram model so it
can join the round-robin alongside the datagram mixnets, rather than as a peer of
them. The honest verdict above still holds: over a reliable stream quicmix's CC is
a **no-op** (Tor runs its own congestion control and its own circuit rotation,
`NEWNYM`), and tunneling QUIC datagrams over one ordered TCP stream
head-of-line-blocks the whole path — which is exactly why, in the multipath demo,
the Tor leg behaves like the "slow substrate."

What exists today (`src/tor.rs`, `quicmix-tor/`): a `StreamSubstrate` trait and a
`StreamDatagram` length-prefix framing adapter that lets a Tor (arti) stream
present as a `MixTransport` and ride the same round-robin. A real arti circuit
reached `check.torproject.org` (HTTP 301). Use Tor as a **fallback/compatibility
leg**, not as a carrier of the datagram model or the global-observer threat model —
for those, the datagram mixnet path is the real substrate.

### Capability matrix (substrate × quicmix feature)

| feature | Datagram (emulator/Nym) | Stream (Tor) |
|---|---|---|
| QUIC native | ✅ | ❌ (TCP tunnel anti-pattern) |
| oracle-fed congestion control | ✅ | ❌ (N/A) |
| fixed-size cells / cover | ✅ | ❌ |
| unlinkable rotation + e2e session | ✅ | ⚠️ partial (Tor's own circuits + e2e session id) |
| global-observer resistance | ✅ (with cover) | ❌ (low-latency) |

---

## Bottom line

- **Client side** is genuinely pluggable: congestion controller is a live plug;
  QUIC stack (quinn↔s2n-quic) and oracle source (exact↔measured) are designed as
  swaps with the quinn + exact/estimator paths implemented.
- **Substrate** is pluggable *within a kind*: any datagram mixnet drops into
  `MixTransport` and gets all of quicmix; Tor plugs into `StreamSubstrate` but, by
  the nature of being a TCP/low-latency network, it can only act as a fallback
  transport — it does **not** carry the datagram model or the GPA threat model
  that quicmix is built for. Supporting it is useful for reach/compatibility, not
  as a substitute for the mixnet path.
