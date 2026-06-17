# quicmix over REAL networks — verified-live results

This is the honest record of what now actually runs on a real network, produced on
an open-egress laptop (macOS, 2026-06-16). It supersedes the earlier framing in
which the substrate bindings *compiled* but had **never carried a packet on a real
network** and were **not wired** to the congestion-control / rotation machinery.

The headline novelty — **QUIC + quicmix's oracle-fed CC, and unlinkable rotation,
running over a real mixnet** — is now built and **verified live over Nym mainnet**.
Tor and Katzenpost are exercised live to the extent each substrate allows.

## What changed vs "compiles but never run"

| | Nym | Tor | Katzenpost |
|---|---|---|---|
| Substrate binding (`MixTransport`) | real | real | real (CBOR) |
| **Ran live on the real network** | ✅ mainnet | ✅ live Tor | ✅ docker testnet |
| Measured `OracleParams` (T1) | ✅ | ✅ (probe) | network params from PKI |
| **Oracle-fed CC over the real net (T2/T3)** | ✅ **end-to-end** | n/a (stream) | (CBOR data path) |
| **Unlinkable rotation over the real net (T4)** | ✅ **verified** | — | — |
| End-to-end data path wired | ✅ | probe (stream) | ✅ **SendMessage→echo→reply** |

"n/a (stream)" for Tor: Tor is a reliable ordered stream, so quicmix's datagram CC
is a documented no-op there (it is the "slow leg"); we still run a real circuit
through the binding and measure it.

## T1 — measured `OracleParams` from the live substrates

**Nym mainnet** (`realprobe`, 30 self-addressed pings, decoupled send/recv, matched
by id, throughput from reply-arrival span):

```
30/30 returned (0% loss)   RTT p50 2823 ms   p90 4598 ms   throughput 6.3 msg/s
→ OracleParams { hops: 3, mean_hop_delay: 470ms, drop_prob: 0.000, slot_interval: 158ms, mtu: 1200 }
```

> Note: an earlier probe reported "65% loss / 8.5 s". That was a **measurement bug**,
> not the network: a 10 s per-ping timeout (≈ the real median RTT) plus a sequential
> send-then-wait loop that discarded late replies. Decoupling send from receive and
> widening the collection window gives the real picture: **~0% loss**, multi-second
> RTT. (This is the "update variables/configuration as needed" fix.)

**Tor** (`torprobe`, arti, via `quicmix::tor::StreamSubstrate`, cold cache):

```
bootstrap 160 s   stream connect 13.4 s   first-byte RTT 677 ms
→ check.torproject.org returned HTTP/1.1 301  (confirms a real Tor exit)
```

## T2 — real QUIC + quicmix CC end-to-end over **Nym mainnet** (the headline)

`quicmix-nym/bin/nym_e2e`: two `quicmix::node::Node`s (ingress A → mix → gateway B →
origin), but the "mix" is the **live Nym mixnet**, not the emulator. Wiring built
this session:

- `spawn_client_bridge` — local UDP front ⇄ `NymSubstrate` (lets unmodified quinn
  speak UDP while bytes traverse Nym, with reply-SURBs).
- `NymGateway` — a second Nym client; per-`sender_tag` UDP socket to the local quinn
  server, replies back through the mix via `send_reply` (the SURBs).
- Core: a mixnet-aware `max_idle_timeout` (seconds-scale RTT) + `Node::with_congestion`.

```
oracle (measured Nym): hops=3 mean_hop_delay=470ms slot=158ms rtt≈2.82s
QUIC connection established over Nym in 2.5 s
end-to-end (A→Nym→B): OK (80 bytes, 6.6 s)
origin response: "hello from origin via real Nym + quicmix"
✅ real QUIC + quicmix CC carried an HTTP fetch over the live Nym mainnet.
```

## T3 — CC A/B over **Nym mainnet** (`nym_bench`, 16 KB upload)

| arm | FCT | goodput | retransmits | loss |
|---|---|---|---|---|
| `cubic` (stock) | 2.1 s | 7.64 KB/s | **1**/25 | 4.0% |
| `quicmix` | 1.8 s | 8.75 KB/s | **0**/24 | 0.0% |

Honest, muted result: at Nym's measured ~0% real loss, stock CUBIC's tighter loss
timers still produced **one spurious retransmit** (mix reordering/jitter read as
loss); quicmix's oracle-derived tolerant timers produced **zero**, with ~1.15×
goodput. The predicted direction; the magnitude is small because real Nym loss is
low and the transfer is small (a larger transfer pays more SURB-return cost).

## T4 — unlinkable rotation over **Nym mainnet** (`nym_rotate`)

Rotation is now substrate-agnostic: `rotation::connect_fresh_with` /
`CircuitPool::prewarm_with` take a `FrontFactory`; `quicmix_nym::nym_front`
bootstraps a **fresh ephemeral Nym client** (new mixnet identity + SURB context) per
circuit.

```
session continuity / unlinkability over real Nym:
  one e2e session id carried over 2 circuits; 2 distinct apparent source addrs
  => same logical session over two fresh Nym identities — no shared source/keys/ticket. OK

rotation cost (medians): cold 6.7 s (n=2)   warm 8.3 s (n=4)   → 0.8×
```

- **Unlinkability: verified live.** The novelty holds — one continuous session over
  two cryptographically- and mixnet-independent circuits.
- **Cost: honest negative.** The emulator's ~3× warm-pool speedup does **not**
  reproduce on real Nym. Per-request mix latency (multi-second, high-variance)
  dominates, and an ephemeral Nym client bootstrap is not the bottleneck the
  emulator assumes — so pre-warming saves little. The emulator *overstates* the
  rotation cost benefit; what survives contact with the real network is the
  unlinkability property, not the latency win.

## T5 — Katzenpost (docker voting testnet)

No public mainnet; brought up Katzenpost's docker voting testnet (17 containers:
3 dirauths, 3 mixes, 5 replicas, 3 service nodes, gateway, `kpclientd`).

- **Network validated:** PKI consensus signed by 3 dirauths; their Go `ping` client
  sent **5/5 Sphinx packets to `+echo@servicenode1`, 100% success**.
- **Binding upgraded stub → real CBOR:** `quicmix-katzenpost` implements the real
  thin-client schema (`Request`/`Response`/`SendMessage`/`SessionToken`/events) over
  length-prefixed CBOR. `connect_and_handshake` is **verified live against
  `kpclientd`** (`bin/kp_probe`): daemon connected, 27394-byte PKI doc delivered.
- **Full data path — verified live (`bin/kp_echo`):** parse the daemon's CBOR PKI
  document (peel its byte-string wrapper; `ServiceNodes` is an array of
  `BinaryMarshaler` byte-strings, each a `MixDescriptor` map), resolve the `echo`
  service, derive `destination_id_hash = blake2b256(IdentityKey)` (== Katzenpost's
  `hash.Sum256`), send a real `SendMessage` with a reply SURB, and receive the
  `MessageReplyEvent`:

  ```
  resolved 'echo': queue="+echo"  dest_id_hash=1a6964426a0211b1…  (blake2b256 of node IdentityKey)
  SendMessage → MessageReplyEvent: error_code=0  payload=2574 bytes
  ✅ the echo service returned our payload over the live Katzenpost mixnet.
  ```

  The derived `dest_id_hash` (`1a6964426a0211b1…`) matches `servicenode2`'s identity
  hash in the PKI consensus — independent confirmation the derivation is correct.
  Katzenpost now carries real application data through the binding, end to end.

## Emulator baselines (simulation — labeled, for contrast)

Still green and unchanged by the real-network work:

- 13/13 core unit tests pass.
- `bench` (4 MB, 3 hops, 2% drop, ~1.2 MB/s cap): cubic 0.09 MB/s (20% loss) →
  cubic+timers 0.36 (3.8×) → quicmix 1.00 MB/s (10.8×).
- `rotate`: cold ~178 ms / warm ~60 ms (~3×), unlinkability OK.

The emulator caps rate and injects drops it *knows*, so its CC win is large and its
rotation win is clean. The real-network runs above show which of those survive
contact with a live mixnet (the CC direction does; the rotation *cost* win does not;
the unlinkability property does).

## How to reproduce (open-egress host)

```sh
# T1 measure
cargo run --release -p realprobe -- 30                 # Nym mainnet OracleParams
cargo run --release -p torprobe -- check.torproject.org:80

# T2/T3 over real Nym
cargo run --release -p quicmix-nym --bin nym_e2e
QUICMIX_CC=stock   cargo run --release -p quicmix-nym --bin nym_bench
QUICMIX_CC=quicmix cargo run --release -p quicmix-nym --bin nym_bench

# T4 over real Nym
cargo run --release -p quicmix-nym --bin nym_rotate

# T5 Katzenpost (after: cd <katzenpost>/docker && make start && make wait)
cargo run -p quicmix-katzenpost --bin kp_probe -- 127.0.0.1:64331   # handshake
cargo run -p quicmix-katzenpost --bin kp_echo  -- 127.0.0.1:64331 echo  # full echo round-trip
```
