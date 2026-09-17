# Gateway protocol research

These are isolated research checks, not BigBrotr implementation or acceptance
tests. They contact no relay, payment service, or deployed BigBrotr process.
Source checkouts and generated files stay under ignored `.research/`.

Pin the official repositories before running:

| Checkout under `.research/checkouts/` | Repository | Revision |
|---|---|---|
| `gateway-mcp-spec` | `https://github.com/modelcontextprotocol/modelcontextprotocol.git` | `f56f204f6290f6531b14d5734eb3e0a10f0eb201` |
| `gateway-contextvm-rust` | `https://github.com/ContextVM/rs-sdk.git` | `848144c0af627a2ffb1793e21ed346e269acddf4` |

From the repository root:

```sh
python3 -m venv .research/gateway-contract/venv
.research/gateway-contract/venv/bin/pip install jsonschema==4.25.1
.research/gateway-contract/venv/bin/python experiments/gateway-contract/schema_probe.py > .research/gateway-contract/schema-results.json
cp experiments/gateway-contract/upstream.Cargo.lock .research/checkouts/gateway-contextvm-rust/Cargo.lock
CARGO_TARGET_DIR="$PWD/.research/gateway-contract/target" cargo test --manifest-path .research/checkouts/gateway-contextvm-rust/Cargo.toml --locked --features test-utils --test conformance_wire_format --test rmcp_handshake_survival --test e2e_happy_path > .research/gateway-contract/upstream-tests.log 2>&1
```

The Rust command runs upstream source unchanged, using its paired in-memory
`MockRelayPool`. It verifies envelope fields, client identity extensions, encrypted
round trips, two-client correlation, and service survival after hostile early
messages. No WebSocket relay, BigBrotr policy store, real read engine, output
revocation, payment or production resource bound is exercised. Announcements in
the upstream fixture are only in-memory events.

Three additional negative probes use the same environment and manifest:

```sh
CARGO_TARGET_DIR="$PWD/.research/gateway-contract/target" cargo test --manifest-path .research/checkouts/gateway-contextvm-rust/Cargo.toml --locked --features test-utils --test transport_integration plaintext_forged_pubkey_is_rejected_by_signature_verify -- --exact
CARGO_TARGET_DIR="$PWD/.research/gateway-contract/target" cargo test --manifest-path .research/checkouts/gateway-contextvm-rust/Cargo.toml --locked --features test-utils --test transport_integration encryption_mode_required_drops_plaintext -- --exact
CARGO_TARGET_DIR="$PWD/.research/gateway-contract/target" cargo test --manifest-path .research/checkouts/gateway-contextvm-rust/Cargo.toml --locked --features test-utils --test transport_integration auth_allowlist_blocks_disallowed_pubkey -- --exact
```

The schema probe uses both official MCP schemas, including deliberate failures
for missing modern version/result/cache fields, batched requests and duplicate
JSON keys. A passing MCP envelope can still contain arbitrary tool arguments;
the probe records that limitation instead of claiming MCP supplies BigBrotr
admission. The page is a synthetic shape used to test preservation, not a claim
that BigBrotr's complete response schema already exists.

`upstream.Cargo.lock` freezes the dependency resolution used for the research
tests; it is not the future product lockfile. The upstream checkout did not ship a
root lockfile, so the first `--locked` attempt correctly failed. A lockfile was
generated in the ignored checkout, inspected, frozen here, then the stated
`--locked` test command passed. See the machine-readable
[evidence](../../docs/research/evidence/gateway-research.json) and selected
[gateway contract](../../docs/specification/gateway-contracts.md).
