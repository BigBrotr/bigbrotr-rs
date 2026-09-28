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
does not consume the separate export completion reserve. It exclusively reuses the
verified stopped `network-1154757` compiler/registry caches through explicit nested
container mounts and a nonblocking host lock. Their original test executables
must be retained and hash-verified outside the mutable caches. This avoids another
complete compiler-cache copy for a fixture-only correction. Original source,
results, databases and historical artifacts stay untouched. Locked
dependency fetching uses a credential-free container. Build and all tests run
offline with `--network none`; fixture traffic stays on container loopback.
Containers have the pinned laboratory image, two CPUs, 6 GiB memory, 512 PIDs,
an unprivileged user, read-only root/source and no Docker socket or host ports.

The shared `tools/ci.py` executor bounds and joins commands. The exact test
executable is retained and hashed outside compiler caches; source hashes are
checked before/after. This correction profile requires 12 direct TCP/TLS tests,
zero failed/ignored, and Clippy. Revision1154757 already executed the unchanged
32 DNS/policy tests successfully; its direct suite retains the Linux split-response
BrokenPipe fixture failure. The corrected one-write response must still reject the
old missing-poll-check interpretation. Counts bind this fixed fixture
set; they are not coverage targets. Raw command outcomes, terminal container
metadata and preflight stay under the run; original failures are preserved.
`out/qualification.json` is the component result, not a product release manifest.

The `websocket` profile runs the supplied-stream WebSocket integration suite and
the two private input-buffer tests. `relay-wire` runs only the preserving decoder's
eight tests and the new signed WebSocket corpus, compiling the actual protocol and
application crates. It checks changed content, a forged same-ID signature, an
alternate valid signature, duplicated signed fields and retained EOSE hints. The
peer independently decodes outgoing masked REQ and Close; no AUTH response is sent.
Both profiles use their name as the run-directory prefix and final lab.py argument.
Neither qualifies installed Sync, source-bound admission or completed coverage.

The `selector` profile runs eight collection-identity/membership tests, four
strict selector-codec tests and both signed socket corpora. Frozen signed events
separate local ephemeral exclusion from a request-window violation; the peer
checks the generated masked REQ against an independent literal. Three independent
fingerprint vectors include multi-filter typed ordering. The profile qualifies
no persistent scope registration, installed Sync, coverage or capacity.

Both created containers finish stopped-retained; no pruning or volume deletion
occurs. Review source/dependency/artifact identities and original logs before
publishing a concise evidence report and updating the existing inventory.

`request-session` runs only the five actual-socket request-generation tests and
two bounded encoder tests plus scoped Clippy. It retains both exact release test
executables. Independent peers observe masked REQ/CLOSE, stale and concurrent
attribution, poisoned-session refusal and socket closure. No PostgreSQL, dataset
or archive is modified, and the profile claims no installed Sync or coverage.
