# quicmix multi-cloud deployment + benchmark (live)

The quicmix client deployed across a real fleet (laptop + EC2 + DigitalOcean
droplets) and benchmarked laptop→each-node and laptop→each-substrate. Run live on
2026-06-17.

## what's deployed

- **laptop** (`109.205.194.69`) — quicmix `ingress_serve` (http→quic proxy), one per node.
- **EC2** `i-020e4e82afb0eac45` @ `3.79.19.58` — eu-central-1, t3.small, Ubuntu 24.04 — quicmix gateway.
- **DO fra1** @ `64.226.93.43` — Frankfurt, s-1vcpu-1gb — quicmix gateway.
- **DO nyc3** @ `68.183.148.148` — New York, s-1vcpu-1gb — quicmix gateway.

Each node runs `gw_serve` (gateway on `0.0.0.0:4433`, oracle-fed CC) plus a local
25 MB file server. The laptop runs one `ingress_serve` per node.

## proof of the proxy (egress IP)

A `curl` on the laptop, through each quicmix proxy, comes back with **the node's own
public IP** — not the laptop's. So the request really tunneled over QUIC to that node
and egressed there:

| path | egress IP (from `checkip.amazonaws.com`) | = node? |
|---|---|---|
| laptop, direct (no proxy) | `109.205.194.69` | — |
| laptop → quicmix proxy → **EC2** | `3.79.19.58` | ✅ |
| laptop → quicmix proxy → **DO fra1** | `64.226.93.43` | ✅ |
| laptop → quicmix proxy → **DO nyc3** | `68.183.148.148` | ✅ |

## benchmark — laptop ↔ each node over quic (oracle-fed CC)

25 MB pulled from each node *through* its quicmix proxy:

| node | region | download | time |
|---|---|---|---|
| EC2 | eu-central-1 | **3.36 MB/s** | 7.4 s |
| DO fra1 | Frankfurt | **2.99 MB/s** | 8.4 s |
| DO nyc3 | New York | **3.36 MB/s** | 7.4 s |

~27 Mbps per single QUIC flow; the common bottleneck is the laptop's single-flow
downlink / CC window, not the path — nyc3 (US) matches the EU nodes.

## substrates — laptop quicmix client → each substrate (measured live)

| substrate | status | measurement |
|---|---|---|
| nym mainnet | ✅ live | 20/20 returned, 0% loss, RTT p50 705 ms, 7.6 msg/s |
| tor (arti) | ✅ live | bootstrap 0.5 s, connect 447 ms, first-byte 95 ms (HTTP 301) |
| katzenpost (docker testnet) | ✅ live | `SendMessage`→echo→reply, error_code=0, 2574 B |
| hopr | ⚠️ n/a | binding spec-validated + compiles; no hoprd node available to run against |

## how

```
app → laptop ingress_serve (http proxy) → quic (quicmix oracle-fed CC)
    → node gw_serve (gateway) → egress to the internet
```

Cross-machine support added to the node: `Node::serve_gateway_at(0.0.0.0:port)` and
`Node::connect_via(0.0.0.0:0, gateway, cert)` (the prior code bound everything to
`127.0.0.1`). Each gateway's self-signed cert is pinned and transferred to the laptop
per node. Build: `gw_serve` compiled once on EC2 (x86-64 glibc), copied to the
identical-distro droplets.

## reproduce

```sh
# on each node:
gw_serve 0.0.0.0:4433 /tmp/gw.cert        # gateway + writes its pinned cert
python3 -m http.server 8000 --bind 127.0.0.1   # (benchmark file server)
# on the laptop, per node:
ingress_serve <node_ip>:4433 <node.cert> 127.0.0.1:<port> 0.0.0.0:0
curl -x http://127.0.0.1:<port> http://checkip.amazonaws.com      # egress = node IP
curl -x http://127.0.0.1:<port> http://127.0.0.1:8000/bigfile -o /dev/null -w '%{speed_download}'
```

## infra / teardown

```sh
aws ec2 terminate-instances --instance-ids i-020e4e82afb0eac45 --region eu-central-1
# DO droplets 578408727 (fra1), 578408733 (nyc3):
curl -X DELETE -H "Authorization: Bearer $DO_TOKEN" https://api.digitalocean.com/v2/droplets/578408727
curl -X DELETE -H "Authorization: Bearer $DO_TOKEN" https://api.digitalocean.com/v2/droplets/578408733
```

Cost while up: ~cents/hour (1× t3.small + 2× s-1vcpu-1gb).
