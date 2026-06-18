# quicmix — design

**quicmix is one product: a mixnet-native QUIC client.** It makes HTTP/3 usable
over a metadata-private mixnet by doing two things that ship together as a single
transport:

1. **Stay fast over the mix** — replace QUIC's probe-based congestion control with
   logic fed by the mixnet's *known/measured* delay/drop/rate model, so the mix's
   reordering and drops don't get mistaken for congestion.
2. **Rotate unlinkably** — swap the underlying QUIC connection + mix circuit for a
   fresh, unlinkable one while the HTTP/3 session continues, hiding the setup
   handshake behind a pre-warmed pool.

These are not two separate projects or "research areas" — they are one client.
The first keeps a session *performant*; the second keeps it *unlinkable over
time*; a usable mixnet client needs both.

This project is **independent** of the root `freevpn` mixnet, which is left
exactly as-is. quicmix is a *transport seam*, not a new anonymity primitive.

---

## 1. Why this exists (the reframe)

A long design exploration landed on one result:

> Against a **government / coalition** adversary — which for intra-region web
> traffic (EU↔EU, US↔US) is effectively a **global observer over the surface that
> matters** — there is **no cheap, fast, unlinkable bulk path**. Unlinkability
> *on the data itself* is the expensive thing, and it's the only thing a client
> wants. Either (1) pay full mixnet cost on the data path (unlinkable, slow), or
> (2) downgrade the adversary (OHTTP/Tor non-collusion; fast, but linkable to
> whoever sees both ends — which the coalition does). There is no third box.

We **choose option 1** — the protection clients actually want — and accept that
the anonymity core is **already solved by Nym**. Re-implementing a mixnet is not
novel. The under-built, demonstrable work is the **client/transport seam** that
makes option 1 *usable* for the web. That is quicmix.

### Non-goals
- A new anonymity primitive or mixnet (use Nym for "real", emulate for dev).
- A dialing / contact-discovery / "metadata plane" — that's a *messaging*-system
  construct; our use case is anonymous web access, where the site is public and
  the gateway is a known service. There is **one plane**: bulk data over the mix.
- Touching the existing `freevpn` code.

---

## 2. Threat model

- **Adversary:** coalition-scale. Passive observation of ~all links on the
  relevant surface; active (inject/drop/delay) within reach; can compel mixes it
  hosts.
- **Inherited dependency (not solved here):** anonymity needs **≥1 honest,
  non-compellable mix per path**, in practice hosted outside the coalition. An
  operational requirement of the substrate; we depend on it.
- **quicmix's own responsibility:** as a transport it must not *leak via transport
  behavior* and must not weaken the mix beneath it:
  - Congestion/scheduling uses only *public/aggregate* mix params, never "which
    flow is mine"; the return path is paced like the forward path (no fast lane).
  - Rotation is *unlinkable* — fresh keys/Connection IDs, **no resumption
    tickets** (tickets re-link by construction), and it does **not** rely on QUIC
    connection migration for unlinkability (migration is continuity, the exit
    still links it).
- quicmix provides no anonymity by itself; it inherits it from the substrate.

---

## 3. How the client works

```
 ┌─────────────┐    ┌──────────────────────────────────────┐    ┌──────────────────────┐
 │ HTTP/3 app  │───▶│  quicmix client (one transport)        │───▶│  MixTransport          │
 │ (unchanged) │    │   • oracle-fed congestion/scheduling   │    │  ┌──────────────────┐  │ dev
 │             │◀───│   • unlinkable rotation + warm pool    │◀───│  │ EmulatedMixnet   │  │
 └─────────────┘    │   • e2e session continuity             │    │  └──────────────────┘  │
                    └──────────────────────────────────────┘    │  ┌──────────────────┐  │ real
                              ▲ reads oracle (known or measured)  │  │ Nym (spec)       │  │
                              └────────────────────────────────── │  └──────────────────┘  │
                                                                   └──────────────────────┘
```

The **`MixTransport` seam** (`src/lib.rs`) is a datagram pipe plus an `oracle()`
timing model. Both the emulator and a future Nym binding implement it, so moving
to a real mixnet is a substitution, not a rewrite, and the same client runs over
both. The oracle is **exact** on the emulator and **measured** on a real mixnet
(`src/oracle.rs`), never per-packet truth (mixing *is* the secret) — only the
distribution/policy.

### 3a. Staying fast over the mix

Stock CC infers capacity from RTT and loss; over a mixnet every signal lies:

| Signal | Internet | Mixnet | Stock CC's wrong move |
|---|---|---|---|
| Rate | discovered by probing | **dictated** by the cover/slot policy | probes past budget; fights cover |
| Loss | ≈ congestion | cover displacement / policy drops | collapses cwnd needlessly |
| RTT | ≈ path length | huge + jittery (per-hop random delay) | timers fire → spurious retransmits |
| Reorder | mild | **extreme** (mixing *is* reordering) | declares loss on reordered packets |

So the client feeds the controller the oracle instead of inferring it: a **fixed
window** sized from the bandwidth-delay product (rate is exogenous), **loss
ignored as congestion** (mix drops are ARQ-recovered, not a congestion signal),
and **loss-detection timers seeded from the known delay distribution** (tolerate
reordering). The anonymity-critical constant rate is enforced at the
`MixTransport` boundary, so the QUIC controller only has to avoid over/under-
driving it.

Verified expressible (not a gamble): quinn's `congestion::Controller` allows a
constant `window()` + a no-op `on_congestion_event()` (`src/sched.rs`), with
`TransportConfig` (`initial_rtt`, `packet_threshold`) for reorder tolerance;
s2n-quic has a pluggable CC `Provider` as a second option.

### 3b. Rotating unlinkably

The logical session lives **end-to-end inside the tunnel** (a session id as the
first bytes of a stream), so the QUIC connection + mix circuit are disposable.
Each rotation (`src/rotation.rs`) uses a **fresh client endpoint** (no session
cache → no resumption ticket), a **fresh mix path**, hence fresh keys, Connection
IDs, and apparent source address — nothing links it to the prior circuit on the
wire. A **pre-warmed pool** hides the setup handshake so a rotation is ~instant
instead of paying a mix round-trip. Unlinkable rotation also makes per-circuit
exposure resettable (the metadata-budget argument), and re-randomizes the path so
an unlucky all-compromised path is time-bounded.

---

## 4. Results (emulator)

Run: `cargo run --manifest-path quicmix/Cargo.toml --bin bench` (and `--bin rotate`).

**Congestion** (`bin/bench`, 4 MB, 3 hops, 5 ms/hop, 2% drop, ~1.2 MB/s rate cap):

| arm | goodput | loss | vs stock |
|---|---|---|---|
| `cubic` (stock) | ~0.09 MB/s | ~20% | 1.0× |
| `cubic+timers` (reorder + jitter tolerant) | ~0.40 MB/s | ~3% | ~4.4× |
| **quicmix** (window + ignore-loss + tolerant timers) | **~0.99 MB/s** | **~3%** | **~10.8×** |

Stock CUBIC reads the mix's reordering+drops as congestion and collapses.
**The spurious-retransmit chase (resolved):** an earlier version capped quicmix at
~0.56 MB/s with ~58% detected loss because QUIC mistook the mix's *jitter* for
loss and spuriously retransmitted. The fix is loss-detection tolerance **derived
from the measured mix**, not a magic constant: high `packet_threshold` (reorder
count) plus `time_threshold = 1 + 3.1/√(2·hops)` — i.e. cover ≈ p99.9 of the
round-trip jitter (Erlang(2·hops, mean_hop_delay)), so it tightens with more hops
and widens with fewer. Going to "never" stalls on the real drop rate; there's a
jitter-derived sweet spot. That cut loss 58%→~7% and raised quicmix to ~0.99 MB/s
(**~82% of the 1.2 cap**, ~12×). `cubic+timers` also jumps to ~3.7×: tolerant
timers do most of the work over a mix; the custom window + ignore-loss gets the
rest to near-cap. A `MeasuredOracle` can derive the threshold model-free from
observed RTT percentiles (`OracleEstimator::jitter_ratio`). Numbers are stochastic.

**Rotation** (`bin/rotate`, 3 hops, 10 ms/hop, median of 11):

| rotation | cost | |
|---|---|---|
| cold (build fresh circuit) | ~217 ms | handshake + data RTT |
| **warm (take from pool)** | **~64 ms** | data RTT only → **~3.4×** |

Plus a verified continuity/unlinkability check: the same e2e session id carried
over two circuits with **distinct source addresses, fresh keys, no resumption
ticket**.

### Locked success thresholds
- **Speed:** quicmix ≥ **3× goodput** or ≥ **50% lower FCT** vs stock CUBIC at
  matched anonymity policy on the emulator. *(Met: ~10.8×, ~82% of rate cap.)*
- **Rotation:** warm switch < cold switch by a clear margin AND the link-check
  passes. *(Met: ~3.4×, link-check passes.)*
- **Real-substrate honesty:** report measured numbers whatever they are, with a
  root-cause for any emulator↔real gap (Nym pacing, online-estimation error, SURB
  latency, bandwidth cap). A negative result is a planned, presentable outcome.

---

## 5. Substrates
- **EmulatedMixnet** (`src/emulator.rs`, done): one-way datagram pipe imposing
  per-hop exponential delay, drops, reordering, and an egress pacer honoring the
  constant-rate `slot_interval`. Exposes the **exact** oracle. The UDP mix-relay
  (`src/relay.rs`) drops it into a real UDP path so unmodified quinn runs over it.
- **Nym** (`quicmix-nym/` crate, **real + verified live on mainnet**): binds
  `MixTransport` to `nym-sdk` (send/recv + SURB return path; **measured** oracle via
  `src/oracle.rs`), wired end-to-end (`spawn_client_bridge` + `NymGateway`). Real
  QUIC + quicmix CC carried an HTTP fetch over live Nym; CC A/B and unlinkable
  rotation verified (`results.md`). Tor (`quicmix-tor/`), Katzenpost
  (`quicmix-katzenpost/`, real CBOR), and HOPR (`quicmix-hopr/`, REST) are likewise
  separate, real bindings — the heavy network deps stay isolated from the core.

---

## 6. Risks & open questions
- **~~Spurious PTO/loss under jitter~~ — RESOLVED.** Was the cap on the speed gain
  (~58% loss). Fix: tolerant loss detection with `time_threshold` **derived from
  the measured jitter** (`1 + 3.1/√(2·hops)`), not a constant — so it adapts as the
  mix's hops/delay change. Loss 58%→~7%, goodput →~82% of cap. (Going to "never"
  stalls on real drops — it's a jitter-derived value, not on/off.)
- **QUIC CC expressiveness** — RESOLVED (verified quinn + s2n-quic hooks).
- **Real-substrate cost/feasibility** — Nym throughput ceiling, SURB return
  latency, sustained bulk transfer; live-Nym vs AWS-self-host trade. Emulator
  keeps the work unblocked.
- **Online oracle accuracy** — how well the estimator tracks realized delay/loss,
  and the client's sensitivity to that error (the known-vs-measured experiment).

---

## 7. Stack & layout
- **Rust.** `quinn` (QUIC + pluggable CC), `rustls`+`ring`, `rcgen`, `tokio`,
  `rand`. `nym-sdk` only for the real substrate (runbook). HPKE for unlinkable
  pre-builds is a follow-on.
- Layout:
  ```
  quicmix/
    `architecture.md`  README.md  `architecture.md`  `results.md`  `testing.md`
    src/
      lib.rs       ← MixTransport seam + OracleParams + SubstrateKind + SubstrateError [done]
      node.rs      ← Node: ONE type, both roles (gateway + ingress)    [done]
      client.rs    ← client-side plugs: OracleSource/Congestion/MeasuredCc[done]
      emulator.rs  ← EmulatedMixnet (delay/drop/reorder/finite buffer) [done]
      relay.rs     ← UDP mix-relay: quinn over a Substrate boundary    [done]
      substrate.rs ← Substrate boundary: pacing + backpressure + metrics[done]
      sched.rs     ← oracle controller (constant window, ignore loss)  [done]
      rotation.rs  ← unlinkable rotation + pre-warmed pool (no resumption)[done]
      proxy.rs     ← WarmPool: pre-warmed circuits + draining rotation [done]
      oracle.rs    ← online oracle estimator (bounded recent-RTT window)[done]
      striped.rs   ← round-robin multipath over N substrates           [done]
      directory.rs ← gateway directory + auto-promotion gate           [done]
      ingress.rs   ← hyper http→quic ingress proxy                     [done]
      metrics.rs   ← observability contract (prometheus exposition)    [done]
      tor.rs       ← Tor StreamSubstrate + StreamDatagram framing adapter[done]
    src/bin/  quicmix (node)  bench (congestion)  rotate (rotation)
              multipath (striping)  proxy (pooled ingress)  gw_serve  ingress_serve
    quicmix-nym/  quicmix-tor/  quicmix-katzenpost/  quicmix-hopr/  ← real substrate
                  bindings (isolated crates; heavy network deps stay out of the core)
  ```

  **Substrates & multipath.** Datagram substrates (`EmulatedMixnet`, Nym,
  Katzenpost) plug into `MixTransport` and get all of quicmix. Tor is a *stream*
  substrate; `tor::StreamDatagram` frames datagrams over its stream so it can join
  the round-robin as a (head-of-line-blocked) slow leg. `striped::Striped`
  round-robins one flow across any mix of these — see `bin/multipath` (fast 1.66 /
  slow 0.51 / round-robin 1.34 MB/s; naive RR is bounded by the slow path, so
  rate-weighted scheduling is the next step). Multipath is an opt-in
  throughput-for-anonymity trade (cross-network correlation) — see PLUGGABILITY.

  **Roadmap — auto-promotion + gossiped gateways.** A few bootstrap nodes on
  well-known IPs; any node with a public routable IP is auto-promoted to gateway
  (`directory::qualifies_as_gateway`) and announced; live gateways are gossiped
  (`directory::GatewayDirectory`) so clients discover and rotate across them.
  Since gateway is just a role of the one binary, the exit pool grows with
  adoption (Tor-style fungibility). The directory, promotion gate, expiry, and
  sampling are implemented; the gossip *transport* (propagation, signing,
  anti-entropy) is the next milestone.

  **One binary, two roles, no flag.** There is no separate `gateway` CLI. Every
  quicmix node (`node::Node`) is *both* a gateway-for-others and a local ingress
  proxy for itself; a peer is just another quicmix node. The CC win exists *only
  because both ends run quicmix* — a transport optimization is end-to-end, so the
  gateway must speak the same oracle-fed transport as the client. (Verified by
  reading the source: Nym/Tor/I2P exits forward raw IP, so you can't ride them for
  a transport win — this is the "willing gateway" model, like OHTTP/MASQUE.)

## 8. Plan

**Foundation (done):** the `MixTransport` seam + emulator + tests — the stable
base, not part of the active sprint.

**Sprint — "Client Proving"** (one bucket, not gated milestones):
- [x] QUIC over the emulator (UDP mix-relay) + `bin/bench`.
- [x] Oracle-fed controller; stock vs quicmix comparison (**~10.8×**, ~82% of cap).
- [x] Spurious-retransmit (PTO/loss) chase — tolerant loss detection; loss 58%→3%.
- [x] Unlinkable rotation + pre-warmed pool; cost + continuity demo (~3.4×).
- [x] Online oracle estimator (bridge to a measured substrate) + tests.
- [x] Client-side pluggability (`client.rs`) + substrate pluggability incl. Tor.
- [x] Real-substrate validation — **done, verified live on an open-egress laptop**
  (`results.md`): measured `OracleParams` from live Nym/Tor/Katzenpost; real
  QUIC + quicmix CC end-to-end over Nym mainnet (`nym_e2e`); CC A/B (`nym_bench`);
  unlinkable rotation (`nym_rotate`); Katzenpost CBOR `SendMessage`→echo→reply
  (`kp_echo`). The full QUIC-over-Nym `MixTransport` is wired, not a spec.
- [x] Production hardening — typed `SubstrateError`; `Substrate` boundary (pacing,
  backpressure, metrics) on every real path; measured-oracle updates feeding new
  circuits; graceful **draining** rotation; observability contract (`metrics.rs`);
  failure-mode test matrix. See `testing.md` and `tests/failure_modes.rs`.

The emulator gives a deterministic, CI-able arm; the real substrates are the
verified live arm.


---

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
