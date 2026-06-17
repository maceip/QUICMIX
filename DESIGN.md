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
- **Nym** (`src/nym.rs` spec + `REAL_MIXNET.md` runbook): bind `MixTransport` to
  `nym-sdk` (send/recv + SURB return path; **measured** oracle via
  `src/oracle.rs`). Two options: live Nym mainnet, or a self-hosted 10–20-node AWS
  topology that pins configured params for the airtight "known oracle" arm. Not
  wired in the offline build — it needs a live network.

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
    DESIGN.md  README.md  REAL_MIXNET.md  PLUGGABILITY.md
    src/
      lib.rs       ← MixTransport seam + OracleParams + SubstrateKind  [done]
      node.rs      ← Node: ONE type, both roles (gateway + ingress)    [done]
      client.rs    ← client-side plugs: OracleSource/Congestion/Profile[done]
      emulator.rs  ← EmulatedMixnet (delay/drop/reorder/finite buffer) [done]
      relay.rs     ← UDP mix-relay: quinn over the emulator           [done]
      sched.rs     ← oracle controller (constant window, ignore loss)  [done]
      rotation.rs  ← unlinkable rotation + pre-warmed pool             [done]
      oracle.rs    ← online oracle estimator (real substrate)          [done]
      striped.rs   ← round-robin multipath over N substrates           [done]
      directory.rs ← gateway directory + auto-promotion gate           [done]
      nym.rs       ← Nym datagram-substrate binding                    [spec]
      katzenpost.rs← Katzenpost datagram-substrate binding             [spec]
      tor.rs       ← Tor StreamSubstrate + StreamDatagram framing adapter[done adapter, spec arti]
    src/bin/  quicmix.rs (node)  bench.rs (congestion)  rotate.rs (rotation)  multipath.rs (striping)
    realprobe/  ← live Nym probe (nym-sdk)
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
- [~] Real-substrate validation: `realprobe` (nym-sdk) **builds and connects to
  live Nym mainnet** (topology fetch + gateway selection work). The live
  *measurement* is blocked by **this sandbox's egress policy** (gateway TCP:9000
  and UDP:53 DNS both filtered; only HTTPS/443 permitted) — see `REAL_MIXNET.md`.
  The probe is ready to run from any open-egress host (laptop/AWS); remaining:
  run it there, calibrate the emulator to the measured numbers, then wire the full
  QUIC-over-Nym `MixTransport`.

The emulator keeps the sprint unblocked while the real substrate is egress-gated.
