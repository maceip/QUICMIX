# Running quicmix over a real mixnet

The emulator (`EmulatedMixnet`) lets us develop and measure offline with an exact
oracle. To validate on a **real** mixnet, swap the substrate behind
`MixTransport` for a Nym binding (`src/nym.rs`). Two options; pick by how much
control you need.

> Status: `realprobe/` is a **working, compiled** Nym mainnet probe (uses
> `nym-sdk`). The online oracle estimator it feeds (`src/oracle.rs`) is
> implemented and tested. The full QUIC-over-Nym `MixTransport` binding
> (`src/nym.rs`) is specified, not yet wired.

## What we verified in-environment (and the egress block)

Run against **live Nym mainnet** from the dev sandbox:

- ✅ Mainnet HTTPS API reachable (`validator.nymtech.net/api/v1/network/details`
  returns live mainnet JSON) — so topology fetch + gateway selection work.
- ✅ `nym-sdk` (v1.21) builds in-environment; `realprobe` runs and selects a
  gateway.
- ❌ **The live mixnet connection is blocked by this sandbox's egress policy**,
  proven two ways:
  - default gateway `ws://<ip>:9000` → `Connection timed out (os error 110)`
    (arbitrary high TCP ports are filtered);
  - with `force_tls(true)` (pick a wss/443 gateway) → `hickory-dns resolver error:
    request timed out` (nym-sdk's bundled resolver needs **UDP:53**, also
    filtered).
- ✅ For contrast, TCP **443 to arbitrary IPs works** — the policy permits HTTPS
  egress (via proxy) but not Nym's data path (arbitrary host:port + its own DNS).

**Conclusion:** the real measurement can't complete *from this managed sandbox*
because of its network policy — not because the code or Nym is unavailable.
`realprobe` is ready to produce real numbers from any host with normal egress
(your laptop, or an AWS box):

```sh
cargo run --manifest-path quicmix/realprobe/Cargo.toml --release -- 30
# prints: returned/sent, median RTT (ms), loss% from real Nym mainnet
```

Feed those measured numbers into the emulator (`OracleParams`) to calibrate it to
reality, and into `OracleEstimator` as the measured oracle.

## Option A — live Nym mainnet (fastest to "real")

1. Add the SDK: `nym-sdk` (and `nym-sphinx`/`nym-topology` as needed).
2. Build a `MixnetClient` (ephemeral identity is fine for the demo):
   ```rust
   let mut client = nym_sdk::mixnet::MixnetClient::connect_new().await?;
   let our_address = client.nym_address();
   ```
3. Implement `MixTransport` for a wrapper:
   - `send`: `client.send_message(recipient, datagram, IncludedSurbs::new(n)).await` —
     the QUIC datagram is the payload; include a SURB budget for the return path.
   - `recv`: pull from `client.next().await` / the message stream; hand the
     reassembled bytes back as one QUIC datagram.
   - Return traffic: the exit/gateway uses the attached SURBs to send response
     datagrams back; **replenish** SURBs as they're consumed (a steady reply
     budget for a QUIC session).
4. Measure the oracle online with `OracleEstimator`: feed it observed RTTs and
   `Connection::stats()` `sent`/`lost`, call `.estimate()`, and pass the result to
   the controller/transport config.
5. Point `bin/bench` and `bin/rotate` at the Nym `MixTransport` instead of the
   emulator relay.

Caveats: mainnet throughput/latency vary; sustained bulk transfer may be limited;
SURB return latency is part of the measurement. Report whatever the numbers are.

## Option B — self-hosted 10–20-node AWS topology (airtight "known oracle")

Use this to *set* the policy so the "known params" arm of the oracle comparison is
exact and the run is reproducible.

1. Provision N mix nodes + ≥1 gateway across a few AWS regions (region spread
   approximates jurisdictional diversity for the demo narrative). t3.small is
   enough for a demo.
2. Run `nym-node` in mixnode mode on each and `nym-node` in gateway mode on the
   gateway; skip Nyx/token economics — use a **custom topology** file the client
   loads directly (no bonding).
3. Set the policy you'll report as "known": hop count (3), Poisson mix delay λ,
   Sphinx packet size, cover/loop rates.
4. Point the SDK client at your topology source instead of mainnet; otherwise the
   `MixTransport` binding is identical to Option A.

## The headline experiment (same on either option)

Run all three congestion arms (`bin/bench`: `cubic`, `cubic+timers`, `quicmix`)
and the rotation demo (`bin/rotate`) over the real substrate, then:

- **CC gains:** compare goodput / FCT / loss to the emulator numbers; root-cause
  any gap (Nym pacing, SURB latency, throughput ceiling).
- **Oracle: known vs measured:** feed the controller (i) the configured params
  (exact on Option B) and (ii) the `OracleEstimator` online estimate, and plot the
  gap — "how much does knowing the oracle exactly buy?"
- **Rotation:** warm-pool vs cold switch latency, and confirm the unlinkability
  evidence holds over real circuits.

A negative or muted result is a planned, presentable outcome — report measured
numbers with their root cause against the locked thresholds in `DESIGN.md`.
