# Public TLS test identities

These deterministic certificates and the server key are **public test fixtures**,
never installation credentials. `generate.py` uses cryptography 45.0.7 and fixed
test-only Ed25519 seeds, serials and validity dates. `manifest.json` records the
exact DER hashes and permitted names. Regeneration reproduces those bytes.

The two unrelated roots distinguish correct trust from wrong trust. The server
certificate names only `fixture.example` and `127.0.0.1`; `other.example` must
fail even when DNS supplies the same allowed address. These fixtures remain
valid from 2026-01-01 to 2036-01-01; an expired fixture is a setup failure, never
permission to disable certificate time checks. Product code uses the ordinary
rustls verifier and wall clock. No ambient client certificate is selected.

The actual Rust peers also record SNI, ALPN, selected sockets and literal request
bytes. DNS responses are independently encoded RFC1035 bytes. This is component
evidence, not installed Control/enrollment, HTTP redirects or WebSocket acceptance.
