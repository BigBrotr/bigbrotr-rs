# Installation and independently selected capabilities

Status: selected planning contract, 2026-09-17. The commands, recipes and runtime
behavior below are implementation requirements, not an installed product. The
[operations contract](operations-contract.md) owns recovery and resource ordering;
this document owns the operator's choice of roles, hosts and dependency endpoints.

## Selected installation paths and alternatives

Ship one Linux binary and one application image with identical role semantics.
Support native processes supervised by systemd and Docker Compose on one host;
split installations run the same native units or a separate Compose project on
each host, using explicit authenticated routes. A compact installation may combine
Control, acquisition/ingest, storage coordination and read roles in one application
process when selected. Exclusive stores, individual work queues and whole-job
grants remain; expensive native archive/export/graph work stays terminable in
on-demand child processes. An abort/OOM of that compact process affects all its
roles. The isolated full recipe retains the process domains in operations.

Do not require a container or daemon for each logical service, embedded DuckDB,
RocksDB, a seed import, or an export. Selecting a Monitor does not start a
Synchronizer; selecting read does not start an Indexer. Dependencies can be supplied
by already running owners. Missing dependencies produce explicit preflight/readiness
results, not background provisioning or hidden role activation.

| Alternative | Decision and tradeoff |
|---|---|
| Native processes / systemd | Supported: few packaging layers and direct local paths; operator supplies matching engines, users, permissions and service supervision. |
| Per-host Docker Compose | Supported reference packaging: repeatable images, volumes and explicit binds; container DNS/loopback and host paths need separate configuration. It is not cross-host storage or ownership coordination. |
| Kubernetes / Swarm / automatic failover | No initial deployment requirement or shipped guarantee. They may later schedule the same roles, but scheduler restart or replica count does not fence local Rocks/catalog writers. Qualify their volume, identity and exclusion procedures before advertising support. |

The local filesystem is the default archive placement. Optional S3-compatible
storage and its URI, endpoint, region, addressing, credentials, protection and
relocation rules belong to the [archive storage contract](archive-storage-contract.md).
S3 denotes an access protocol; no particular object-server daemon is mandatory.
A split installation can retain local files when every archive publisher/reader
and archive job executes on one storage host. Remote collectors submit to its
Ingest owner; hot readers use CH/Control. A distributed archive-worker variant
uses a qualified shared object route at every host that reads archive bytes.
Shared PG alone cannot supply those bytes.

## Role and capability selection

`serve --roles ...` selects only the current process's assigned roles. The
deployment root declares the complete placement and its reachable dependencies;
an omitted role on this process may be supplied remotely. Omitted `--roles` uses
the root's explicit local assignment, never an implicit full installation.
Role enablement and worker gates are both required for dispatch. Defaults in an
unselected worker block do not activate it. Managed commands can control only
already assigned roles; expanding placement requires a deployment-root change.
One authoritative configuration schema generates native and image examples.

The packaged recipes below are initialization choices, with the same managed or
declarative authority behavior after initialization. Their names describe useful
outcomes, not a second storage format. `config init --recipe NAME` materializes
that choice; `config validate` resolves it without network or mutations, and
`doctor` performs bounded read-only checks from the actual process namespace.
Connectivity is distinct from required schema, grants, data identity and readiness.

| Recipe | Explicit application work | Required data/authority | Intentionally absent work |
|---|---|---|---|
| `full` | Existing full observatory roles | PG control/catalog, ingest Rocks, archive, Index Rocks, CH; selected read/compute owners | Public listeners, signing, overlays and directory polling until configured |
| `sync-only` | Synchronizer, ingest and archive plus local Control, or declared remote equivalents | PG registry/policy/checkpoints, guarded relay transport and durable archive pipeline; optional ordered-set reader | Discoverer, Validator, Monitor, Deriver, Ranker, public reads/signing; CH not required for linear/live acquisition |
| `monitor-only` | Monitor, ingest and archive plus local Control, or declared remote equivalents | PG registry/policy/attempt ledger, guarded transport, evidence/documents and archive | Synchronizer, Discoverer, Validator, Ranker and signing; CH optional for the separately selected online monitor views |
| `serve-existing` | Read adapters and Control, local or remote | Matching PG policy/frontiers/pins and CH dataset/serving incarnation; resource-specific maintenance when advertised | Acquisition, ingest and archive publication; no startup data import or index rebuild |
| `archive-only` | Selected acquisition/import/archive and private operations | PG, ingest Rocks and archive; offline export jobs as requested | CH, Index Rocks and online analytical resources |
| `split-role` | Explicit placement of the same selected roles | Authenticated role routes, matching dataset/topology IDs, reachable PG/CH where used; local archive jobs on one host or a qualified object route for distributed archive users | Automatic ownership transfer, extra shards and implied shared filesystem |

`relay-only` is the existing documented variant of `serve-existing` with
ArchiveRelay selected; it has the same authority and conditional index rules.
HTTP API, ArchiveRelay and optional gateways remain independent listener choices.
No-provenance and social-filter choices modify evidence/selection within a recipe
without changing full signed-event fidelity or activating unrelated workers.

### Fixed relay acquisition without discovery or validation

The acquisition contract owns `sync.endpoints.mode/urls` and
`monitor.endpoints.mode/urls`. Each mode is either `enrolled` (full-recipe default)
or `fixed`; fixed requires a nonempty bounded list of canonical websocket URLs.
The list is a scheduling intersection with enrollment, route/access authority and
bans, not a bypass of those checks. Validate and deduplicate with the existing URL
normalizer; config diagnostics redact private paths/queries. Managed edits to the
list follow policy revision activation, not live file watching.

For a fixed-list installation, initialize the appropriate recipe, set its list,
then run the finite `seed --file relays.txt --enroll` or existing `relay enroll URL`
commands with operator authority. The registry records explicit unverified
enrollment. A Synchronizer/Monitor may start its own due work on that permission
without requiring a successful Validator result or `active` reachability first.
Each worker keeps its own attributable results/backoff; neither invents Validator
evidence. Dormancy retains permission and sparse due retries; only an explicit ban,
scope restriction or withdrawal stops permitted work altogether.

These recipes set `bootstrap.seed=false`, candidate validation disabled and
automatic public enrollment false; no bundled seed or external directory expands
the list. Pre-existing enrolled endpoints, event mentions and later operator
enrollments outside the fixed list remain outside this worker's schedule. The
list is not a global ban: another explicitly selected worker may have another
scope. Removing an entry drains its work and preserves history; re-adding it
resumes retained scope progress under current policy. Empty enrolled mode is
healthy idle; empty fixed mode is an input error. A list containing blocked or
unenrolled entries shows per-entry inactive reasons; it never silently reports
successful collection of the whole list.

`sync.mode=linear` requires no ordered-set reader. `auto` uses reconciliation only
when a declared eligible inventory is available; otherwise it chooses bounded
linear/revisit work and reports that reason without changing the remote support
observation. Explicit `negentropy` requires the reader and reports unavailable
when it cannot be supplied. No path substitutes an unavailable inventory for an
empty set. The worker's protocol verification and bounded capability checks are
part of synchronization, not a secretly running Validator or Monitor.

### What monitor-only and serving-only mean

Passive Monitor emits no signed EVENT. Its probes may receive and verify fixture
events needed to interpret a measurement; that does not enroll synchronization or
launch live/backfill collection. Actual measurements/documents still need durable
ingest and archive receipts. Without CH, private operational status and offline
exports remain available; the rich public directory/history pages are unavailable.
Online monitor views require explicit Indexer/CH/read selection. Reusing PG for a
second public scientific projection is not selected: it would add another reducer,
rebuild and consistency contract merely to remove one engine. This is a concrete
capability limit, not a claim that all monitoring needs ClickHouse.

Serving existing data supports remote CH with separate read credentials. It also
requires the **matching** PG control state, directly through a local Control role
or through an authenticated remote Control adapter: principal grants, current
policy leases, published source/target frontiers, serving incarnation and cursor/
snapshot pins are not recoverable from arbitrary CH rows alone. Creating an empty
PG schema beside populated CH does not create an authorized ready dataset. An
unrelated CH database cannot be silently attached/imported on startup.

Already complete immutable resources do not require acquisition or an active
projector. Ordinary current reads must preserve deletion/replacement/expiry
semantics; NEG needs its ordered-set owner; derivation needs its state owner and
inputs. Either supply those explicitly or mark the affected resources unavailable
at their temporal/authority limit. Do not turn them into historical reads to keep
an endpoint green. CH-only files/SQL analysis remains an external analysis activity,
not the authenticated BigBrotr serving contract. Archive access is unnecessary for
hot CH-only resources; an advertised cold read, replay, rebuild or export requires
the actual catalog/object route and suitable credentials at its executing host.

## Remote endpoints, credentials and trust boundaries

P01-T04 owns these concrete root fields; defaults do not guess localhost for an
unsupplied remote dependency. They are ordinary engine settings, not a universal
connection-string or secret templating framework. Roots are deployment-locked.

| Root fields | Contract |
|---|---|
| `postgres.control.{endpoint,database,user,password_ref}` | Explicit PG host/port, database and least-privilege control user; password is a protected reference. The typed TLS settings below belong to this connection. |
| `postgres.catalog.{endpoint,database,user,password_ref}` | Separate DuckLake catalog credentials/schema authority; may share the PG server with control. Needed only at catalog consumers. |
| `clickhouse.{endpoint,database,read_user,read_password_ref,write_user,write_password_ref}` | Explicit HTTP(S) interface and database. Read processes receive only read references; writers receive only their required write references. Do not default to an empty-password `default` account. |
| Each PG/CH connection's `tls.{mode,ca_file,server_name,client_cert_ref,client_key_ref}` | `verify-full` on cross-host routes: chain and intended server name checked. Optional client certificate/key are paired protected references. Local socket/private bridge plaintext is an explicit local recipe exception with stated boundary, never opportunistic fallback after TLS failure. |
| Existing `topology` RPC endpoint/peer bindings | One declared owner/dataset/epoch and mTLS identity per remote authority. The connected owner must attest compatible schema/dataset and its role authority; certificate possession alone is insufficient. |

Endpoints contain no password/userinfo or credential query parameters. A read role
does not need catalog credentials when it uses no archive reads; a role supplied
through Control RPC does not inherit direct PG credentials. Migration, create/drop,
backup and archive deletion privileges are granted only to their explicit owners,
not every process. Only applicable secrets are opened; unknown or malformed disabled
settings still fail syntax validation, while staged unused secret references do
not require provisioning a disabled dependency.

Operator-root PG/CH/S3/private RPC/signer destinations are **trusted infrastructure
routes**, permitted to use declared private addresses. Relay/directory/LNURL inputs
are **untrusted acquisition destinations** and keep the guarded public/private
egress policy. Sharing transport primitives does not share authority: a private
database endpoint must neither be rejected by the public-relay CIDR policy nor
grant crawling access to its subnet. Discovered data, public request fields and
managed product-policy edits cannot create infrastructure routes. Preserve bounded
timeouts/pools, verified TLS, endpoint identity and credential redaction on both.
Do not forward database/object credentials through unapproved redirects.

The TLS choice has the certificate+hostname meaning described by
[PostgreSQL](https://www.postgresql.org/docs/current/libpq-ssl.html); the selected
Rust connector must qualify that behavior rather than assume a libpq default.
CH's [HTTP interface](https://clickhouse.com/docs/concepts/features/interfaces/http#authentication)
supports separate authentication and database selection; the application uses
protected credentials, never URL passwords. The PG/CH adapter tests own actual
wrong-CA/name/user/database and credential-rotation behavior.

## Native and Docker installation contract

Each released guide must give a complete path: obtain/verify/extract the supported
bundle; select a recipe; fill endpoints and protected secret files; provision the
explicit local engines or use existing remote ones; run the dedicated initialization/
migration job; validate/doctor from each role's namespace; enroll any fixed relays;
start the named roles; inspect capability/status and one actual result; stop/restart.
Initialization is an explicit finite operation with the P02/P08 migration authority,
not side effects of `doctor` or `serve`. The guide lists actual local paths/UIDs,
required credentials, expected ports, output and recovery from failure at each step.
No generated Compose/config/service-unit file is presumed shipped before P08.

In native mode, loopback refers to that host. In bridge Compose, loopback refers
to the container; use the local service name and container port for bundled
databases, or a routable operator endpoint for another host. A container listener
intended to receive published traffic binds its container interface; the host
mapping separately restricts exposure to `127.0.0.1`/`::1` or the selected private
address. Private database/RPC ports have no public publishing by default. Per-host
Compose service names are not cross-host DNS. This follows Docker's
[networking model](https://docs.docker.com/compose/how-tos/networking/).

Render each recipe with `docker compose -f RECIPE config` before launch and inspect
the resolved service/role set. Separate recipe files are preferred to a universal
profile switchboard. Compose can activate an explicitly targeted profiled service
and its dependencies even when the profile was not selected; this is why
[profiles](https://docs.docker.com/compose/how-tos/profiles/) cannot enforce application
role permission. `depends_on: service_healthy` helps initial engine ordering but
does not replace application schema/grant/readiness or runtime reconnect logic;
Docker distinguishes [running from ready](https://docs.docker.com/compose/how-tos/startup-order/).

File-backed [Compose secrets](https://docs.docker.com/compose/how-tos/use-secrets/)
mount only into the services that need them. Source-file permissions and the
application's descriptor checks remain necessary; `_FILE` is an image convention,
not automatic support for every application. Native units use equivalent protected
files. Never embed credentials in images, committed examples, rendered DSNs or
ordinary environment diagnostics. Rotation reconnects affected pools without
changing dataset identity or pretending a failed connection means empty data.

Initial recipes use Linux bridge networking. Host networking is a separately
qualified option: Docker [ignores published port mappings in host mode](https://docs.docker.com/engine/network/drivers/host/),
so application binds then determine host exposure. Release evidence records the
actual Engine/Compose versions, daemon routing mode, IPv4/IPv6 binds and external
reachability. Loopback publishing alone is not a universal isolation proof,
including Docker's documented [older-engine and direct-routing caveats](https://docs.docker.com/engine/network/port-publishing/).

## Placement, identity and failure behavior

A simple two-host starting placement has acquisition on A and Control/ingest/
storage/CH/read on B; archive publication, replay and export execute on B against
its local files. A reaches Ingest/Control through declared RPC, so this split
adds no object server. Native heavy jobs on B remain terminable child processes.
Further splitting archive-byte consumers across hosts requires the qualified
object route. Databases can be on a third host by changing their explicit
endpoints. Pure read on C needs only its selected CH/Control and conditional
Index/archive-job routes; it does not mount another host's Rocks directory.
Preflight reports every data path by executing host, engine action and credential.
Local paths on different machines never satisfy the same archive dependency.

Preserve dataset ID, logical producer slot, observer ID and durable owner epoch
across a controlled restart. Each process gets a new unpredictable incarnation.
A copied config is not a new observer or ownership grant; intentional independent
observation requires a separately registered observer. Never mount/copy a live
Rocks directory into a replacement as a shortcut. Manual handoff resolves unknown
receipts, checkpoints/copies/verifies state, positively excludes the old writer,
advances topology by CAS, then activates its successor. Restarts and health failures
do not authorize concurrent writers.

PG/Control loss expires policy authority and stops protected new actions; CH loss
leaves archive acquisition possible within its finite limits; archive loss grows
pending work to its high-water stop; ingest loss pauses producers. Fixed-list scope
and worker disablement survive every restart. Data-plane faults cannot silently
start another worker, widen a relay set or provision a fresh empty database. Report
blocked capability and cause separately from process responsiveness and remote
relay health. These recipes promise no unlimited buffering or zero bottlenecks.

## Evidence and implementation gates

The isolated [deployment probe](../../experiments/deployment-contract/README.md)
checks explicit capabilities, endpoint scope and forbidden topology interpretations.
It is not a product resolver, engine test, installed bundle or network-isolation
proof. Current primary-document inspection informs packaging choices only.

P01-T04/T07 implement the schema, resolver and adapters; P03-T01/T07/T12 implement
fixed acquisition; P04-T01/T07/T12 implement independent enrollment/monitoring;
P08-T01/T09/T10/T12 ship and exercise the actual recipes. Required real acceptance:
fresh sync-only and monitor-only with unrelated enrolled endpoints, zero hidden
workers/signing, no CH for archival variants; remote serve-existing with matching
and mismatching PG/CH identities; every dependency fault and recovery; wrong TLS
and credentials; native plus image installs outside the checkout; two-host data
reachability and old-owner resumption; actual private/public bind observation.
Unavailable architecture/platform/tools are NOT_RUN, never installation success.
