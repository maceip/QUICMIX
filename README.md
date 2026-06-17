<p align="center">
  <img src="assets/quicmix.png" alt="quicmix" width="260">
</p>

# quicmix

mixnet-native quic transport. makes quic usable over a metadata-private datagram
mixnet via two mechanisms shipped as one transport:

- **oracle-fed congestion control** — feed quic the mix's measured delay/drop/rate
  model so its reordering and anonymity drops aren't read as congestion.
- **unlinkable rotation** — swap the quic connection + mix circuit for a fresh,
  unlinkable one mid-session; setup hidden behind a pre-warmed pool.

independent of any specific mixnet; it's transport plumbing, not a new anonymity
primitive.

## data path

```
   ┌──────┐  http   ┌───────────────┐   quic over the mixnet   ┌───────────────┐  tcp   ┌──────────┐
   │ app  │ ──────▶ │ quicmix       │ ── hopr / nym / tor / katz ───▶ │ quicmix       │ ─────▶ │  origin  │
   │      │ ◀────── │ ingress  (a)  │ ◀──── (3 sphinx hops) ── │ gateway  (b)  │ ◀───── │ clearweb │
   └──────┘         └───────────────┘                          └───────────────┘        └──────────┘

   both ends are the same quicmix node → oracle-fed cc + unlinkable rotation run end-to-end
```

## how it plugs in

one trait. emulator and real substrates are interchangeable:

```rust
trait MixTransport {
    fn oracle(&self) -> OracleParams;   // exact on the emulator, measured on a real net
    async fn send(&self, datagram: Vec<u8>);
    async fn recv(&self) -> Option<Vec<u8>>;
}
```

`OracleParams { hops, mean_hop_delay, drop_prob, slot_interval, mtu }` — the
public/aggregate timing model the scheduler reads instead of inferring from rtt/loss.
measured live by the probes; exact on the emulator.

## congestion control

stock cubic collapses over a mix (reads reordering + anonymity drops as congestion).
quicmix instead:

- fixed bdp window (oracle rate × rtt), paced to the mix rate rather than probing past it.
- loss not treated as a congestion signal — anonymity drops are arq-recovered, not backed off.
- loss-detection threshold derived from the measured per-hop jitter, so a merely-delayed
  packet isn't mistaken for a drop.

## rotation

the logical session lives end-to-end inside the tunnel (a session id in the first
stream bytes), so the quic connection + circuit are disposable. each rotation is a
fresh client endpoint (no resumption ticket → fresh keys, connection ids, source
addr) over a fresh circuit (new mix identity). a pre-warmed pool hides the setup
handshake so the swap is a data round-trip, not a full bootstrap.

## performance

emulator (simulation) — 4 mb, 3 hops, 5 ms/hop, 2% drop, ~1.2 mb/s cap:

| cc | goodput | loss | vs stock |
|---|---|---|---|
| stock cubic | 0.09 mb/s | 20% | 1.0× |
| cubic + tolerant timers | 0.32 mb/s | 5% | 3.4× |
| quicmix | 0.99 mb/s | 7% | **10.6×** |

emulator rotation — 3 hops, 10 ms/hop, median of 11:

| | cost | |
|---|---|---|
| cold (build fresh circuit) | 194 ms | handshake + data rtt |
| warm (pre-warmed pool) | 58 ms | data rtt only → **3.3×** |

nym mainnet (measured probe): 30/30 returned, 0% loss, 2.8 s rtt p50, 4.6 s p90,
~6 msg/s. quic + quicmix runs end-to-end over this (a real http fetch).

the emulator is the controlled cc comparison (known drop/rate, repeatable). a
head-to-head over real nym is dominated by per-transfer variance — a 16 kb upload
swings ~2–13 s run to run — so single-sample a/b numbers there aren't meaningful;
what real nym shows is that it works at all, and at what rtt/loss.

## substrates

| crate | substrate | status |
|---|---|---|
| `src/` | emulated datagram mixnet | cc + rotation, unit-tested |
| `quicmix-nym/` | nym mainnet (datagram) | full end-to-end; cc + rotation verified live |
| `quicmix-tor/` | tor via arti (stream) | real circuit; cc inert on a reliable stream |
| `quicmix-katzenpost/` | katzenpost thin-client daemon | real cbor; pki-resolved `sendmessage`→reply verified live |

quic runs natively over datagram substrates. tor is a reliable stream, so it's framed
into datagrams and head-of-line-blocks — a compatible slow leg, not a peer of the
datagram nets; quicmix's cc does not govern it.

## verified live

measured on a laptop with open egress (full record in `REAL_RESULTS.md`):

- **nym mainnet** — http fetch end-to-end over real quic + quicmix cc; unlinkable
  rotation (one session over two distinct nym identities, two distinct apparent sources).
- **tor** — real arti circuit to check.torproject.org (http 301).
- **katzenpost** (docker testnet) — pki-resolved `sendmessage`→echo→reply round-trip
  through the mixnet; `destination_id_hash = blake2b256(identity_key)`.

honest read: on real nym the cc gain is muted (near-zero real loss) and the rotation
*cost* win doesn't reproduce (per-request mix latency dominates) — what holds live is
the unlinkability. the emulator's large numbers are the emulator flattering itself.

## layout

- `src/` — core: `MixTransport`, oracle-fed cc (`client`, `sched`), rotation, emulator,
  node proxy (ingress + gateway), online oracle estimator.
- `quicmix-{nym,tor,katzenpost}/` — real substrate bindings; heavy deps isolated so the
  core build pulls none of them.
- `realprobe/`, `torprobe/` — live measurement probes → measured `OracleParams`.

## run

```sh
cargo test                                                       # core, 13 tests
cargo run --bin quicmix                                          # ingress → mix-emulator → gateway → origin
cargo run --bin bench                                            # stock cubic vs quicmix cc
cargo run --bin rotate                                           # rotation cost + unlinkability

# real networks (open egress):
cargo run --release --manifest-path realprobe/Cargo.toml -- 30   # nym mainnet → OracleParams
cargo run --release --manifest-path quicmix-nym/Cargo.toml --bin nym_e2e      # quic over nym
cargo run --release --manifest-path quicmix-nym/Cargo.toml --bin nym_rotate   # rotation over nym
cargo run --manifest-path quicmix-katzenpost/Cargo.toml --bin kp_echo         # katzenpost round-trip
```

the binding crates are standalone (own `Cargo.lock`); the core crate has no network deps.
