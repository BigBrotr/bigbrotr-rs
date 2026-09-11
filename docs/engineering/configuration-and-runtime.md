# Configuration and runtime contracts

## One source of defaults

The Rust configuration schema owns defaults, units, ranges, applicability and
change behavior. Generate the reference, JSON Schema and default examples from
that model. Explicit overrides use TOML; environment variables carry a documented
small deployment subset and secret references. CLI flags are highest precedence.
Resolution is defaults → recipe → explicit config → optional versioned managed
overrides → documented environment → CLI. Deployment-locked fields cannot be
changed by the management interface; applied revision is reported per service.
Reject duplicate/unknown keys, nonsensical bounds, conflicting settings and inert
enabled capabilities. A deliberately specified missing file is an error.

`config init` writes a minimal editable configuration. `config show` prints
redacted effective values and their sources; it is a diagnostic artifact, not
implicitly a round-trippable config. `config validate` performs static checks;
`doctor` performs bounded dependency, disk, port, permission and route probes.
Neither starts collection or signs events. `serve --roles ...` resolves required
data dependencies and names missing ones without silently enabling more services.

| Concern | Default design | Change behavior |
|---|---|---|
| Full signed event archive | Always | Invariant; no body-loss switch |
| Provenance | On in the full recipe | New evidence epoch; off stops new edges, does not claim old edges vanished |
| Acquisition filter | All eligible kinds/events, bounded safety policy | New filter revision and coverage work |
| Public network | Enabled | New work; host/endpoint fairness shared across services |
| Overlay/private networks | Explicit routes only | Drain/reconnect; private rules scoped to destination |
| Local API/dashboard | Loopback/private binding | Listener restart when bind changes |
| Public API/archive relay/DVM | Explicit enablement | Validate exposure, dependencies and any signer before binding |
| Signing/publishing | No implicit identity or publication | Key reference and identity-policy version; bounded drain |
| Rank computation | Enabled when usable input/viewpoint exists | Empty seeds yield an explicit unavailable score, not invented global trust |
| Import/export | On-demand jobs | Inputs and policy versions pinned per job |
| Resource limits | Finite host envelope plus role shares | Reductions stop new admission and drain current work |
| Backups | Local configuration required for off-host protection | Report unprotected durability class until configured and verified |
| NIP-09 history | Retain original and deletion-request records | Serving policy is distinct; physical removal is not automatic |

Every service's complete key table appears in its contract. Values there are
starting defaults with units, not promised production limits. A Tux tuning recipe
is separate from safe generic settings. A max_connections value of 128 in a generic
worker does not mean the product supports only 128 endpoints; the recipe, memory
envelope and measured workload determine active-session capacity.

## Shared budget allocator

Treat the host envelope as the upper bound. Component limits cannot all claim the
same free RAM. Account for application heaps, TLS/websocket buffers, pending
batches, Rust/C++ library caches, allocator overhead, kernel sockets/page cache,
CH queries/merges, PG connections, DuckDB jobs and graph workers. Track cgroup
memory as well as RSS; mmap resident pages and page cache are not free resources.

Each role acquires item and byte permits before allocating queued payloads.
Release permits after ownership transfer or completion. A bounded queue at every
layer is insufficient if batching, clones or per-connection queues multiply its
size by R. Derive the aggregate maximum and test it. Schedule admission, live
sessions, historic backfill, imports and maintenance with reserved minimum work
shares and borrowable unused capacity. Reserve control/monitoring responsiveness
under backfill load. Within a share, oldest-due work and per-host tokens prevent
starvation; relay clusters never silently remove endpoints from observation.

CPU-heavy signature verification, compression and ranking use explicit bounded
pools or isolated workers. Do not spawn one blocking task per event and rely on
Tokio to discover a sensible CPU limit. A started blocking task cannot generally
be aborted; long jobs need cooperative checkpoints or a terminable process.
See [Tokio spawn_blocking](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html).

## Lifecycle and health

Use `starting`, `ready`, `idle`, `degraded`, `draining`, `stopped` with attributable
reasons and last progress. Liveness means process responsiveness. Readiness is
role-specific: the read API can serve a stale complete generation while reporting
indexing lag; an ingest owner cannot acknowledge if durable storage fails. An
empty queue is healthy. A disabled service is not a failed service. A misconfigured
enabled listener cannot report ready merely because its HTTP process responds.

Shutdown stops new work, cancels network sessions, drains bounded admitted work,
commits durable checkpoints, and releases exclusive ownership only after writes
stop. A timeout must not release a lease and leave an old writer alive. Lost
ownership rejects further durable writes; leases coordinate scheduling while
commit-time fencing protects data. Retry policies distinguish permanent config
errors, unavailable dependencies, rate limits, malformed remote data and
transient I/O. A five-failure counter is not a universal permanent shutdown rule.

Structured logs are useful without raw hostile payloads or secrets. Histograms
use consistent units and bounded labels; do not label Prometheus series by event
ID, pubkey, arbitrary relay URL or error string. Per-relay detail belongs in
queryable state. Expose queue bytes/age, active permits, event/edge rates, signature
CPU, stale frontiers, replays, compaction debt, disk reserve, errors and coverage
age. Alerts have actionable runbooks and distinguish a remote relay failure from
local inability to measure it.
