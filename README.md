<p align="center">
  <img src="assets/quicmix.png" alt="quicmix" width="260">
</p>

<p align="center">
  <a href="https://maceip.github.io/QUICMIX/"><img alt="live demo" src="https://img.shields.io/badge/live_demo-mixnet_trace-ffb53d?style=flat-square&labelColor=02060a"></a>
  <img alt="tests" src="https://img.shields.io/badge/tests-34_passing-52ff8f?style=flat-square&labelColor=02060a">
  <img alt="substrates" src="https://img.shields.io/badge/substrates-nym·tor·katzenpost·hopr-7df9ff?style=flat-square&labelColor=02060a">
  <img alt="transport" src="https://img.shields.io/badge/transport-quic_/_quinn-52ff8f?style=flat-square&labelColor=02060a">
  <img alt="nym mainnet" src="https://img.shields.io/badge/nym_mainnet-verified_live-ffb53d?style=flat-square&labelColor=02060a">
</p>

# quicmix

mixnet-native quic transport — it makes quic usable over a metadata-private datagram
mixnet via two mechanisms shipped as one transport

- **oracle-fed congestion control** — feed quic the mix's measured delay/drop/rate
  model so its reordering and anonymity drops aren't read as congestion
- **unlinkable rotation** — swap the quic connection + mix circuit for a fresh,
  unlinkable one mid-session, with setup hidden behind a pre-warmed pool

independent of any specific mixnet — it's transport plumbing, not a new anonymity
primitive

> ▶ **[watch the live 3d trace →](https://maceip.github.io/QUICMIX/)** a wargames-style
> walk of an http request crossing the mixnet, where you pick which real gateway droplet
> terminates the circuit and the camera flies the packet hop by hop around the globe

## data path

```
   ┌──────┐  http   ┌────────────────┐    quic over the mixnet     ┌────────────────┐  tcp   ┌──────────┐
   │ app  │ ──────▶ │  quicmix        │ ─ hopr / nym / tor / katz ─▶ │  quicmix        │ ─────▶ │  origin  │
   │      │ ◀────── │  ingress  (a)   │ ◀──── 3 sphinx hops ──────── │  gateway  (b)   │ ◀───── │ clearweb │
   └──────┘         └────────────────┘     (constant cell rate)      └────────────────┘        └──────────┘
                     oracle-fed cc + unlinkable rotation                terminates quic
                                                                        egresses, returns via surbs

   both ends are the same quicmix node → cc + rotation run end-to-end (it's a transport optimization)
```

## how it plugs in

one trait — the emulator and the real substrates are interchangeable

```rust
trait MixTransport {
    fn oracle(&self) -> OracleParams;          // exact on the emulator, measured on a real net
    async fn send(&self, datagram: Vec<u8>);
    async fn recv(&self) -> Option<Vec<u8>>;
}
```

`OracleParams { hops, mean_hop_delay, drop_prob, slot_interval, mtu }` is the
public/aggregate timing model the scheduler reads instead of inferring from rtt/loss —
measured live by the probes, exact on the emulator. the production trait adds the
fallible `try_send`/`try_recv` (typed `SubstrateError`), and a `Substrate` boundary wraps
any transport with pacing, bounded-queue backpressure, and metrics

## congestion control

stock cubic collapses over a mix — it reads reordering and anonymity drops as congestion.
quicmix instead

- holds a **fixed bdp window** (oracle rate × rtt), paced to the mix rate rather than probing past it
- treats **loss as not a congestion signal** — anonymity drops are arq-recovered, never backed off on
- derives its **loss-detection threshold from the measured per-hop jitter**, so a merely-delayed packet isn't mistaken for a drop

## rotation

the logical session lives end-to-end inside the tunnel (a session id in the first stream
bytes), so the quic connection + circuit are disposable. each rotation is a fresh client
endpoint (no resumption ticket → fresh keys, connection ids, source addr) over a fresh
circuit (new mix identity). a pre-warmed, self-healing pool hides the setup handshake and
**drains** retiring circuits so a rotation never kills an in-flight request

## performance

emulator — controlled cc comparison, 4 mb, 3 hops, 5 ms/hop, 2% drop, ~1.2 mb/s cap

| cc | goodput | loss | vs stock |
|---|---:|---:|---:|
| stock cubic | 0.09 mb/s | 20% | 1.0× |
| cubic + tolerant timers | 0.32 mb/s | 5% | 3.4× |
| **quicmix** | **0.99 mb/s** | 7% | **10.6×** |

emulator rotation — 3 hops, 10 ms/hop, median of 11

| | cost | |
|---|---:|---|
| cold (build fresh circuit) | 194 ms | handshake + data rtt |
| **warm (pre-warmed pool)** | **58 ms** | data rtt only → **3.3×** |

nym mainnet (measured probe) — 30/30 returned, 0% loss, rtt p50 ≈ 2.8 s, p90 ≈ 4.6 s,
~6 msg/s — quic + quicmix runs a real http fetch end-to-end over this. numbers are
stochastic and the emulator's are emulator-specific — the real-network honest read is below

## substrates

| crate | substrate | status |
|---|---|---|
| `src/` | emulated datagram mixnet | cc + rotation, unit-tested |
| `quicmix-nym/` | nym mainnet (datagram) | ✅ full end-to-end, cc + rotation verified live |
| `quicmix-tor/` | tor via arti (stream) | ✅ real circuit, cc inert on a reliable stream |
| `quicmix-katzenpost/` | katzenpost thin-client | ✅ real cbor, pki-resolved `sendmessage`→reply verified live |
| `quicmix-hopr/` | hopr via `hoprd` rest | ⚠️ http failure-mapping verified vs a mock, live data path pending a node |

quic runs natively over datagram substrates. tor is a reliable stream, so it's framed into
datagrams and head-of-line-blocks — a compatible slow leg, not a peer of the datagram nets,
and quicmix's cc does not govern it

## verified live

measured on a laptop with open egress — full record in [`docs/results.md`](docs/results.md)

- **nym mainnet** — http fetch end-to-end over real quic + quicmix cc, plus unlinkable rotation (one session over two distinct nym identities, two distinct apparent sources)
- **tor** — real arti circuit to check.torproject.org (http 301)
- **katzenpost** (docker testnet) — pki-resolved `sendmessage`→echo→reply round-trip through the mixnet

honest read — on real nym the cc gain is muted (near-zero real loss) and the rotation
*cost* win doesn't reproduce (per-request mix latency dominates), what holds live is the
unlinkability. the emulator's large numbers are the emulator flattering itself

## deployed (multi-cloud)

the http→quic proxy live across a real fleet — a quicmix gateway on aws ec2 + two
digitalocean droplets, with a laptop ingress to each. a `curl` through each proxy egresses
from that node's own ip, not the laptop's. **these are the gateways you can pick in the
[live demo](https://maceip.github.io/QUICMIX/)**

| gateway | region | egress ip | quic download |
|---|---|---|---:|
| aws ec2 | eu-central-1 · frankfurt | `3.79.19.58` | 3.36 mb/s |
| do · fra1 | frankfurt | `64.226.93.43` | 2.99 mb/s |
| do · nyc3 | new york | `68.183.148.148` | 3.36 mb/s |

## production hardening

- typed `SubstrateError` at the boundary, mapped from real lib/http errors in every binding
- a `Substrate` layer (pacing + bounded-queue backpressure + metrics) on every real path
- measured-oracle updates feeding each new circuit, draining rotation that never kills an in-flight request, no tls resumption by config
- a prometheus observability contract (`src/metrics.rs`) and a failure-mode test matrix — typed error + metric + bounded recovery for nym disconnect, katz close, hopr 401/500/timeout, gateway death, queue saturation, oracle churn, prewarm partial fail

## run

```sh
cargo test                                                       # core + integration, 34 tests
cargo run --bin quicmix                                          # ingress → mix-emulator → gateway → origin
cargo run --bin bench                                            # stock cubic vs quicmix cc
cargo run --bin rotate                                           # rotation cost + unlinkability
cargo run --bin proxy                                            # pooled multipath proxy + prometheus metrics

# real networks (open egress) — isolated crates, so heavy deps stay out of the core:
cargo run --release --manifest-path realprobe/Cargo.toml -- 30                # nym mainnet → OracleParams
cargo run --release --manifest-path quicmix-nym/Cargo.toml --bin nym_e2e      # quic over nym
cargo run --release --manifest-path quicmix-nym/Cargo.toml --bin nym_rotate   # rotation over nym
cargo run --manifest-path quicmix-katzenpost/Cargo.toml --bin kp_echo         # katzenpost round-trip
```

## docs

- **[live 3d demo](https://maceip.github.io/QUICMIX/)** — the wargames-style mixnet trace
- [`docs/architecture.md`](docs/architecture.md) — design, threat model, pluggability, invariants
- [`docs/results.md`](docs/results.md) — the verified live-network record + the multi-cloud deploy
- [`docs/testing.md`](docs/testing.md) — the end-to-end test matrix (commands, prereqs, captured output)
- [`docs/roadmap.md`](docs/roadmap.md) — forward work that preserves the design invariants

the binding crates are standalone (own `Cargo.lock`) — the core crate has no network deps
