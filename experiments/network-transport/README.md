# Guarded network component laboratory

P04-T02 component execution on native Linux amd64. This runs the actual Rust DNS
stub against independently encoded TCP peers, direct TCP/TLS with actual controlled
peers and fixed certificate identities, and destination policy against literal
expected sets. It does not qualify installed acquisition, recursive
resolver interoperability, WebSocket, current Control grants, overlays or
capacity. Those remain the owning task's practical acceptance.

The Mac is the editing authority. After local review and checks, make one private
candidate commit and transfer it with a Git bundle using the
[delivery workflow](../../docs/engineering/development-and-delivery.md).
Create a new `runs/network-<first-seven-commit-digits>/source` checkout under the
inventoried Tux implementation root; fetch the bundle and detach at that exact
commit. Never edit the remote source. Invoke from that checkout:

```sh
python3 -B experiments/network-transport/lab.py \
  "$(git rev-parse HEAD)" "$(git rev-parse 'HEAD^{tree}')"
```

The runner checks clean source, inventories containers/mounts/processes/ports and
requires 12 GiB free compiler headroom. This profile creates no archive, so it
does not consume the separate export completion reserve. It copies only the
verified stopped `network-1a2a8be` compiler/registry caches into its new
output; originals, databases and historical artifacts stay untouched. Locked
dependency fetching uses a credential-free container. Build and all tests run
offline with `--network none`; fixture traffic stays on container loopback.
Containers have the pinned laboratory image, two CPUs, 6 GiB memory, 512 PIDs,
an unprivileged user, read-only root/source and no Docker socket or host ports.

The shared `tools/ci.py` executor bounds and joins commands. Four exact test
executables are retained and hashed outside compiler caches; source hashes are
checked before/after. Qualification requires 9 policy, 9 DNS lifecycle, 14 DNS
semantics and 12 direct TCP/TLS tests, zero failed/ignored, and Clippy. Counts bind this fixed fixture
set; they are not coverage targets. Raw command outcomes, terminal container
metadata and preflight stay under the run; original failures are preserved.
`out/qualification.json` is the component result, not a product release manifest.

Both created containers finish stopped-retained; no pruning or volume deletion
occurs. Review source/dependency/artifact identities and original logs before
publishing a concise evidence report and updating the existing inventory.
