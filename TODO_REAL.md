# WHAT IS REAL vs FAKE, AND WHAT TO BUILD (read this first)

> **SUPERSEDED (2026-06-17).** This was the build plan. T1–T5 are now **done and
> verified live** on a real network — QUIC + quicmix CC end-to-end over Nym mainnet,
> unlinkable rotation over Nym, a real Tor circuit, and a PKI-resolved Katzenpost
> `SendMessage`→reply round-trip. The verdict below describes the *starting* state,
> not the current one. See **`REAL_RESULTS.md`** for the verified record.

## Verdict (no hedging)
The transport **mechanisms are real and unit-tested, but they have only ever run
against the in-process emulator (a simulation).** Nothing — not the congestion
control, not the session rotation — has run over a real Nym / Tor / Katzenpost
network. **The novelty (CC + session re-establishment *over real mixnets*) is NOT
implemented end-to-end.** The parts exist in isolation; the integration to real
substrates does not.

## Per-substrate reality
| | Nym | Tor | Katzenpost |
|---|---|---|---|
| `MixTransport` binding | real, compiles, **never run live** | real, compiles, **never run live** | **stub** (socket+frame only; CBOR schema is a TODO; won't talk to a real daemon) |
| Oracle-fed CC applied to it | **NO** | **NO** | **NO** |
| Session re-establishment / rotation | **NO** | **NO** | **NO** |
| End-to-end over the live network | **NO** (no gateway loop in repo) | **NO** | **NO** |

## Real, but emulator-only (do NOT call these "real-network")
- CC logic: `src/client.rs` (`Congestion`), `src/sched.rs` (`OracleController`),
  `bdp_bytes`, `loss_time_threshold`. ONE substrate-agnostic mechanism
  parameterized by `OracleParams`. It only ever consumed **emulator** params.
  There is no per-substrate CC — "per substrate" means "feed it each substrate's
  measured numbers," and those were **never measured** (probes never ran live).
- Rotation: `src/rotation.rs` + `bin/rotate`. Real, but `connect_fresh` is
  **hardcoded to `start_relay` + `EmulatedMixnet`** — it cannot re-establish a real
  Nym SURB context or a Tor circuit.
- `striped`, `oracle`, `tor::StreamDatagram`, `directory`, `node` proxy: real,
  tested — all over the emulator.

## Fake / unbuilt — must replace
1. No live run of anything (bindings compile; never carried a real packet).
2. CC not connected to real substrates (data path uses the emulator).
3. Rotation is emulator-coupled (doesn't know Nym/Tor re-establishment).
4. No gateway-side loop in the repo (Nym→QUIC-server→SURB-reply).
5. Katzenpost binding is a stub (CBOR thin-client schema missing; no Rust client
   upstream).

## BUILD-OUT TASKS (ordered, concrete)

**T1 — Measure real substrates (unblocks real CC values).**
Add throughput to `realprobe`/`torprobe` (send N msgs / fetch a known-size
object; record bytes/sec). Emit per substrate `{median_rtt, loss, throughput}` →
`OracleParams { hops, mean_hop_delay = (rtt/2)/hops, drop_prob = loss,
slot_interval = mtu/throughput, mtu }`.

**T2 — Wire substrates into the data path (end-to-end over Nym/Tor).**
- `spawn_client_bridge(substrate) -> SocketAddr`: local UDP socket ⟷
  `MixTransport` (quinn client connects to the returned addr; packets → substrate,
  replies → client).
- `run_nym_gateway(oracle)`: a `MixnetClient` whose inbound messages are bridged,
  **keyed per `sender_tag`**, to a local `node::Node::serve_gateway()` quinn server
  (which already terminates QUIC + egresses); the server's replies go back via
  `sender.send_reply(sender_tag, bytes)`. The gateway needs **no public IP** — it's
  addressed by its Nym address.
- A `gateway` bin (prints Nym address + QUIC cert) and a `client` path
  (`Node::connect(bridge_addr, gateway_cert)` + `serve_ingress`).
- Tor variant: gateway reachable via an onion service / addr; same bridge shape.
- nym-sdk API (verified): `ReconstructedMessage { message: Vec<u8>, sender_tag:
  Option<AnonymousSenderTag> }`; `sender.send_message(recipient, bytes,
  IncludedSurbs::new(n))`; `sender.send_reply(tag, bytes)`; `client.split_sender()`.

**T3 — Feed measured CC per substrate.**
Build each substrate with its T1 `OracleParams` so `Congestion::Quicmix
.transport(&oracle)` sizes window + loss timers to *that* network. Run T2 and
confirm CC beats stock over the real net (this is the headline result).

**T4 — Real session re-establishment (the rotation novelty).**
Refactor `rotation::connect_fresh` to take a **substrate factory**
(`Fn() -> Arc<dyn MixTransport>`) instead of hardcoded `EmulatedMixnet`. Then:
- Nym rotate = fresh ephemeral `NymSubstrate` (new client / SURB context) + fresh
  QUIC + e2e session-id rebind.
- Tor rotate = fresh circuit (new `TorClient`/stream or `NEWNYM`) + fresh QUIC.
- Pre-warm the pool with real circuits; measure warm-vs-cold over the real net.

**T5 — Katzenpost.** Implement the thin-client CBOR schema in
`quicmix-katzenpost` against a running daemon (`scripts/real-katzenpost.sh` brings
up their docker testnet), or drop it from the demo. Currently non-functional.

## Hard constraint
This was all developed in an **egress-blocked sandbox**, so none of T2–T5 can be
verified here — they must be run/debugged on a host with open egress. The probes
(`scripts/real-nym.sh`, `scripts/real-tor.sh`) are the only things that touch real
networks today, and they MEASURE; they don't carry traffic.
