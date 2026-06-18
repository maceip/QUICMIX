# quicmix-bounce

A tiny **iframe loader** for the [live demo globe](https://maceip.github.io/QUICMIX/). When the demo
finishes "fetching a page over the mixnet" it shows that page in an iframe — but most sites send
`X-Frame-Options` / `Content-Security-Policy: frame-ancestors` headers that forbid embedding, so the
iframe renders blank. A browser can't strip response headers; only a server can.

This service runs on the gateway droplet, fetches the page **server-side** (so it egresses at the
droplet's own IP — the same exit as the quicmix gateway), and re-serves it **without** the
frame-blocking headers, injecting a `<base href>` so the page's assets still resolve.

```
GET /bounce?url=<url-encoded>[&gateway=<id>]   ->   the page, re-served framable
GET /healthz                                    ->   "ok"
```

It is **not** a general-purpose frame-buster:

- every response pins `Content-Security-Policy: frame-ancestors` to the demo origins only
  (`*.github.io` + localhost), so the bounced page can be embedded by the quicmix demo and nothing else;
- SSRF targets (loopback / private / link-local / cloud-metadata hosts) are refused on the initial
  URL **and** re-checked on every redirect hop;
- only `http`/`https` URLs, a 12 MB cap, and a 20 s timeout.

## run

```sh
cargo run --release --manifest-path quicmix-bounce/Cargo.toml
# listens on 127.0.0.1:9100 by default; override with BOUNCE_LISTEN
BOUNCE_LISTEN=127.0.0.1:9100 ./target/release/quicmix-bounce
```

### behind Caddy (auto-TLS, same droplet as quicmix-bridge)

The demo loads the iframe over `https://`, so the bounce must be served over TLS. Add a route to the
droplet's Caddyfile (here sharing the `nip.io` host the bridge already uses):

```caddy
64.226.93.43.nip.io {
    @bounce path /bounce /bounce/* /healthz
    handle @bounce {
        reverse_proxy 127.0.0.1:9100
    }
    # ... existing reverse_proxy for the quicmix-bridge websocket ...
}
```

### systemd

```ini
# /etc/systemd/system/quicmix-bounce.service
[Unit]
Description=quicmix bounce iframe loader
After=network.target

[Service]
Environment=BOUNCE_LISTEN=127.0.0.1:9100
ExecStart=/opt/quicmix/quicmix-bounce
Restart=always

[Install]
WantedBy=multi-user.target
```

```sh
sudo systemctl daemon-reload && sudo systemctl enable --now quicmix-bounce
```

## wiring the demo

`docs/index.html` points the result iframe at this service via the `BOUNCE` constant:

```js
const BOUNCE = "https://64.226.93.43.nip.io/bounce";   // set to "" to embed the url directly
```

The iframe src becomes `${BOUNCE}?url=<encoded>&gateway=<id>`. The result bar also links the
original URL (opens in a new tab) as an escape hatch.

## caveats

- Links/forms inside the bounced page point at the real origin (via the injected `<base>`), so
  clicking them navigates the iframe to the origin directly — which may then be frame-blocked again.
  This loads the *landing* page for the demo; it is not a full browsing proxy.
- If the origin is plain `http`, its sub-resources are mixed-content and the https demo page will
  block them. Most sites are https.
