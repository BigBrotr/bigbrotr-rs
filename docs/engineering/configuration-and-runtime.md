# Configuration and runtime contracts

## One source of defaults

The Rust configuration schema owns defaults, units, ranges, applicability and
change behavior. Generate the reference, JSON Schema and default examples from
that model. Choose one instance-wide durable product-policy authority:
`managed` (full-local default) or `declarative`. In managed mode, defaults/recipe/
explicit seed document initialize revision 1 once; subsequent UI/CLI edits use
the PG desired-state document and expected-revision CAS. Later seed-file changes
do not override it. In declarative mode, the operator document is the sole desired
authority; its validated hash becomes an accepted revision through the same typed
activation command. UI edits produce patches/previews, not managed overrides.
A file changed on disk is staged visibly until activation, including at restart.

Operator-owned deployment roots hold storage/engine endpoints, topology,
listener/security ceilings, secret providers and host/process budgets. They are
locked against management edits. Environment/CLI accept only the documented root
subset and restrictive emergency runtime locks, not arbitrary competing durable
policy values. Applied revision is reported per service. The
[authority transitions and exact validation](../specification/operations-contract.md#configuration-authority-and-exact-validation)
define bootstrap, file drift, mode switching, restore, locks and the new composition
keys. Reject duplicate/unknown keys, nonsensical bounds, conflicting settings and
inert enabled capabilities. A deliberately specified missing file is an error.

`config init` writes a minimal editable configuration. `config show` prints
redacted effective values and their sources; it is a diagnostic artifact, not
implicitly a round-trippable config. `config validate` performs static checks;
`doctor` performs bounded read-only dependency, disk, port, permission and route
checks. Active acquisition/network measurements use a separate explicit command.
Neither command starts collection, mutates store ownership or signs events.
`serve --roles ...` resolves required
data dependencies and names missing ones without silently enabling more services.
The [deployment contract](../specification/deployment-contract.md) defines concrete
native/Compose recipes and PG/CH connection roots, applicable secret references
and verified remote TLS. Role selection is the current process's assignment,
not permission to create another process or database. Unselected worker defaults
are staged and create no work; omitted role arguments use the explicit local root
assignment. Static validation checks declared dependencies; `doctor` checks their
bounded reachability/schema/grants from the calling namespace. Neither can turn a
reachable empty or wrong dataset into a verified serving generation.

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
| Content search | Enabled in complete online recipe; explicit scope/index lifecycle | Rebuild generation for analyzer/backend change; disabled is not empty |
| ArchiveRelay NIP-77 | Enabled with the explicitly selected complete relay role when its set projection is ready | Bound sessions; no implicit provenance requirement |
| Import/export | On-demand jobs | Inputs and policy versions pinned per job |
| Resource limits | Finite host envelope plus role shares | Reductions stop new admission and drain current work |
| Backups | Local configuration required for off-host protection | Report unprotected durability class until configured and verified |
| NIP-09 history | Retain original and deletion-request records | Serving policy is distinct; physical removal is not automatic |

Every service's complete key table appears in its contract. Values there are
starting defaults with units, not promised production limits. A Tux tuning recipe
is separate from safe generic settings. A max_connections value of 128 in a host
route domain does not mean the product supports only 128 endpoints; the recipe, memory
envelope and measured workload determine active-session capacity.

The initial closed policy seed uses `bigbrotr-product-policy/1`: a compact UTF-8
JSON object with `format` and `settings`. Settings contain every registered
non-deployment-locked key, including explicit null and staged disabled values,
in lexical key order. Values are the schema's normalized booleans, full-domain
unsigned numbers, finite fractions, text and structured values; bytes and durations
use bytes and microseconds. Negative zero fractions normalize to positive zero.
No root, secret reference or diagnostic attribution
enters this document. Encoding uses the pinned serde_json serializer, bounded to
10 MiB with a counting pass before allocating the output. The candidate digest is
SHA-256 over `bigbrotr/product-policy/1\0` followed by those exact document bytes.
Source-file digest remains separate: comments, equivalent units and deployment
root edits do not change the policy seed. The seed is candidate data, not accepted
configuration, an applied security revision or a principal/action grant. Existing
durable authority must win at restart. Adding settings requires an explicit
document-version migration before persisted seeds can be reused; this initial
encoding does not fill in the remaining unimplemented service policy families.

## Shared budget allocator

Treat the host envelope as the upper bound. Component limits cannot all claim the
same free RAM. Account for application heaps, TLS/websocket buffers, pending
batches, Rust/C++ library caches, allocator overhead, kernel sockets/page cache,
CH queries/merges, PG connections, DuckDB jobs and graph workers. Track cgroup
memory as well as RSS; mmap resident pages and page cache are not free resources.

Select coarse process/domain ceilings at startup or job admission, then local
item/byte credits before allocation. A whole native job gets its memory/CPU/I/O/
spill grant before launch. Optional capacity is redistributed at job boundaries;
no distributed semaphore is required on each delivery. Release local credits
after ownership transfer or completion. A bounded queue at every layer is
insufficient if batching, clones or per-connection queues multiply its size by R.
Derive the aggregate maximum and test it. Schedule admission, live sessions,
historic backfill, imports and maintenance with reserved minimum work shares.
Completion/control reserves cannot be borrowed by intake or optional jobs.
Within a share, oldest-due work and per-host tokens prevent starvation; relay
clusters never silently remove endpoints from observation.

The [maximum-unit and reservation contract](../specification/operations-contract.md#maximum-units-reservations-and-progress)
defines conservative encoding/parse/copy bounds, nonblocking transfer order,
archive/ACK recovery lanes, memory and disk equations, and resolved arithmetic
examples. Validate unsafe relationships statically. Engine cache/query/concurrency
settings and cgroup ceilings have distinct functions; do not shrink an engine's
global thread pool as though it were a CPU quota without testing startup and
background progress. Actual bound/overshoot qualification belongs to engine tests.

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

## Deterministic first launch

The full local recipe resolves these enabled responsibilities: bootstrap seeding,
Discoverer, Validator, Monitor, Synchronizer, durable ingest, archive publication/
maintenance, Indexer, Deriver, Ranker, local HTTP API, dashboard and private
administration. Ranker waits visibly for a usable configured viewpoint and
complete inputs. Import/export are callable finite jobs. Public ArchiveRelay,
DVM, NIP publishers and active signing probes remain explicitly disabled until
configured. External directory discovery and overlay routes are off initially.
Database processes and their credentials are supplied by the documented recipe.

`bootstrap.seed=true` imports the bundled versioned list once into a newly empty
registry, using a durable source-digest/job receipt. Validator establishes actual
reachability; seeding never fabricates it. A failed bootstrap is visible and
retryable. `bootstrap.seed=false` intentionally permits an empty idle deployment
and explains how to enroll/import endpoints. Restart does not re-enroll banned
relays or launch an independent recurring seed loop. No reachable public relay
is required by deterministic tests: the test recipe supplies controlled signed
relay fixtures through an explicit route, while dated real-network smoke tests
measure the bundled list's practical usefulness separately.

Clean-volume acceptance must launch the shipped executable and Compose recipe,
observe a real durable fixture admission and archive publication, query the local
API, open the dashboard/admin onboarding and inspect resolved role health. Assert
actual listener addresses and zero signing/publication calls, not just an enabled
service list. Networkless/empty/malformed bootstrap and dependency outage variants
must identify the cause and documented recovery.

The full-local placement uses one acquisition/socket owner co-located with ingest,
separate storage/Index ownership, isolated native workers and explicit read/Control
domains. Recipe/feature transitions and fixed split-role routes follow the
[operational matrix](../specification/operations-contract.md#process-placement-and-capabilities).
An enabled role needs its live dependency; stopping an algorithm does not silently
stop the storage owner or erase retained results. Archive-only allocates no online
Index Rocks/CH domains.

`sync-only` and `monitor-only` use explicit fixed endpoint lists plus one-shot
operator enrollment, with bootstrap/discovery/validation disabled; another known
relay cannot join their schedules. Their archival variants require no CH.
`serve-existing` requires matching PG/Control authority and CH frontier/incarnation
state; archive and Index routes apply only to advertised cold/maintenance/NEG
capabilities. Remote engine credentials are deployment roots, distinct from
public-relay routing rules. Compact placement may co-locate the explicitly selected
application owners, while preserving native-job process termination and the
documented shared failure scope.

## Shared lifecycle vocabulary and once semantics

Health states remain `starting`, `ready`, `idle`, `degraded`, `draining`, `stopped`.
A finite job separately reports `succeeded`, `partial`, `failed`, `cancelled` or
`skipped`, with a typed reason, progress and coverage. A partial derivation job is
not a COMPLETE published generation. Acquisition protocol phases are nested
progress fields. Do not use one enum for process health, protocol state and data
completeness. Every terminal outcome has a CLI exit/result and management view.

A finite command exits after its durable result. A scheduled worker's `once`
executes one declared bounded work unit and reports remaining work. Synchronizer
uses an explicit bounded acquisition job (scope/deadline), rather than calling an
infinite live loop once. Listeners run until stopped and reject meaningless
`once` mode. CLI help lists the semantics for each role.

Starting defaults: ownership lease TTL 60 s, heartbeat 20 s, graceful drain 30 s;
all are typed durations with heartbeat below TTL and tested failure behavior.
The 5 s authorization lease is a different, stricter policy mechanism. Expired
ownership never authorizes a still-running worker to write; lease release follows
actual write cessation. A timed-out process needs termination/positive exclusion
before takeover. Tests exercise stalls longer than TTL, drain failure and stale
owner resumption; a two-worker lease-row test alone cannot establish fencing.

## Shipped operational alerts

Ship Prometheus rules, Alertmanager routing/inhibition templates and linked
runbooks. Private detail remains private. At minimum distinguish: coverage/cursor
stall with pending eligible work; unexpected ingest halt; WAL/fsync failure or
low disk reserve; archive/index backlog age; a monitored route/network class going
dark; and replication lag when replicated storage is configured. Also expose
backup age/verification failure, scrub corruption, expired policy authority and
compaction debt. Disabled services and intentionally absent replicas are not alerts.

Use low-cardinality aggregate labels and expose individual relay details through
the read model. An upstream dependency alert may inhibit its consequential
backlog alerts; it must not hide independent disk corruption or policy failure.
Rules have explicit timing, severity, owning role and recovery instructions.

Release verification injects these states through controlled service/engine
failures and exercises evaluation → Alertmanager grouping/routing/inhibition →
a local test receiver → resolution. A rule-unit test is useful but cannot prove
that the receiver ever receives an alert. Actual operator notification endpoints
are configured explicitly; test suites never send unsolicited external messages.

### Ownership timing and run envelope

`runtime.lease_ttl=60s`, `heartbeat_interval=20s`,
`renewal_timeout=5s`, `renewal_safety_margin=20s` and
`graceful_shutdown_timeout=30s` are the initial ownership timing keys. Validate
heartbeat plus renewal timeout below TTL minus the safety margin. Losing a
renewal starts bounded retry; before the safety margin is consumed stop new work
and drain. Remaining authority can shorten the ordinary graceful timeout.
Commit-time fencing/exclusive ownership remains the safety boundary; clock math
or lease timing alone does not exclude a paused old owner. Test wall-clock steps
and monotonic elapsed-time accounting. Timing changes require controlled role
restart initially, rather than reinterpret an already granted lease in place.

Each execution envelope records run/job ID, role, owner/epoch, effective config
revision, input cut/scope, start/end times, monotonic duration, terminal outcome,
typed reason, bounded progress/counters and output artifact/generation references.
The owner writes its envelope; output COMPLETE publication still follows the
consumer's transactional contract. Sensitive details have private visibility.
Raw exception text and arbitrary unbounded JSON are not the schema.

## Ownership and probes at actual process boundaries

The [Go entry-point walkthrough](../research/go-read-runtime-code-walkthrough.md)
shows why an HTTP cockpit check cannot own a writer. Missing, disabled or
unreachable health endpoints never authorize concurrent access to an embedded
store or publication stream. Enforce storage/process exclusion and commit fencing
at their owning boundaries. A worker identity contains a fresh cryptographic
process-incarnation nonce; hostname/PID are diagnostic attributes, since both can
repeat. Fencing epochs remain separate from identity uniqueness.

Dependency health probes are coalesced and bounded per adapter/instance. A caller
timeout does not free its work permit while an underlying driver is still active.
Do not launch a fresh unbounded probe every interval while earlier calls ignore
cancellation. Close/reset a supported transport, or escalate to the isolated
process boundary when necessary; expose stale last-success and probe-in-flight
state honestly. Recovery of the dependency must release the old probe and restore
normal cadence without a reconnect storm.

Shutdown stops intake, cancels work and joins owners before closing their stores.
If a driver cannot be joined within the configured authority/drain envelope,
process termination and positive old-owner exclusion are the recovery boundary.
Never close a shared native handle underneath a still-running task or release
its lease as though the task had stopped. Test cockpit outage, repeated health
probes against a stuck driver, process-ID reuse and noncooperative shutdown.
