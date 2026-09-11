# Component boundaries and implementation vocabulary

Status: selected specification, not shipped APIs. Names here connect the
[phase tasks](../implementation/README.md); generated Rust/schema definitions
must agree. Change a shared contract here and update its producer, consumers,
fixtures and public documentation together. A task cannot invent a second
meaning for an existing receipt, event, policy or completion state.

## Dependency map

```mermaid
flowchart LR
  CLI[CLI / private management] --> C[Config, policy, ownership]
  C --> A[Acquisition and measurement workers]
  A --> I[Verified durable admission]
  I --> W[Synced pending source facts]
  W --> P[Archive publisher]
  P --> Q[DuckLake / Parquet committed snapshot]
  W --> X[Online indexer]
  Q --> X
  X --> R[Read core]
  X --> D[Independent derivation targets]
  D --> R
  D --> O[Signed outbox]
  A --> O
  R --> H[HTTP / ArchiveRelay / DVM]
  H --> O
  H --> U[Public dashboard]
  CLI --> V[Private administration]
```

Arrows are data/control dependencies, not synchronous request chains. Outages
have bounded queues, explicit stale/unavailable results and owned recovery.
The read core does not need the Synchronizer running to serve already committed
data. An outbox destination outage does not block archive admission. An archive
outage eventually fills its bounded pending allowance and stops new admission.

## Shared model and paths

| Proposed file | Definitions / ownership |
|---|---|
| `crates/bigbrotr-model/src/identity.rs` | DatasetId, ShardId, OwnerEpoch, Sequence, SourceRecordId, ProjectionRowId, EndpointId, Receipt, SourceCut, GenerationId |
| `crates/bigbrotr-model/src/event.rs` | SignedEvent, VerifiedEvent, EventId, Signature, Tags, first-receive metadata |
| `crates/bigbrotr-model/src/evidence.rs` | Measurement/claim/document kinds, observer/clock/access dimensions, scientific outcomes |
| `crates/bigbrotr-model/src/coverage.rs` | Interval bound, filter scope, traversal evidence, gap and terminal reason |
| `crates/bigbrotr-model/src/policy.rs` | VisibilityScope, Principal, Grants, PolicyEpoch; no embedded secrets/engine handles |
| `crates/bigbrotr-model/src/analytics.rs` | DefinitionId, TargetManifest, typed fact values and score basis |
| `crates/bigbrotr-model/src/read.rs` | QueryRequest, admitted context identity, QueryPage, resource/exactness states |
| `crates/bigbrotr-model/src/publication.rs` | SignedIntent, coordinate, destination obligation and outcomes |
| `apps/bigbrotr/src/runtime/` | Cancellation, BudgetGrant, lease authority, actual async ownership; private application modules |
| `apps/bigbrotr/src/interfaces/rpc/` | Versioned internal transport adapters, authenticated peer identity and limits |

Immutable verified constructors stay private to the audited verification
boundary. Serialized input cannot claim to be a VerifiedEvent. Pure model crates
have no Tokio/PG/CH dependencies; cancellation and secret references belong to
application orchestration. Introduce a library only when its task has a real
consumer and useful public contract.

## Typed boundaries

Signatures below describe ownership and results; they are specification notation,
not source code to copy without lifetime/error design.

| Contract | Input → output | Authority and failure |
|---|---|---|
| Verify | BoundedWireEvent + VerificationPolicy → VerifiedEvent or Invalid/Excluded | Recompute canonical ID; verify actual signature; record exact reason without archiving hostile body |
| Admit | ProducerRequestId + VerifiedEvent + optional LocalObservation + BudgetGrant → AdmissionResult | Owner independently validates remote untrusted input; atomic synced body/identity/facts/receipt |
| AdmitEvidence | ProducerRequestId + typed Measurement/Document/Claim batch → Receipt | Required dictionaries and source references validated; event validity alone cannot authenticate an evidence claim |
| ReadPending | Owner stream + after Receipt + byte/item bound → SourceBatch | Complete committed sequences only; immutable record IDs and content digest |
| PublishArchive | Deterministic ArchiveBatch + ownership → ArchiveCommit | Data/dictionaries/references/receipt commit once under fenced single publisher |
| Project | SourceBatch + TableGeneration + deterministic chunk identity → ProjectionAck | Ack records actual durability class/incarnation, never merely accepted into RAM |
| ActivateFrontier | Required family acknowledgements + expected previous frontier → SourceCut | Fenced PG compare-and-swap; missing required family cannot count as complete |
| ReadSource | Authorized scope + compatible SourceCut + bounded selector → SourceRows | Same cut/visibility across IDs, tags, bodies and correction reduction |
| Derive | TargetDefinition + immutable inputs/config + resource grant → TargetManifest | Complete/empty/suppressed/failed independently per target; activate only verified output |
| Query | QueryRequest + authenticated caller → QueryPage or typed refusal | Resolve policy and resource before plan; budget shared by HTTP/WS/DVM |
| Sign | ValidatedPurpose + canonical unsigned event + authorized key reference → SignedEvent | Kind/subject/key/body validated; no public generic signing endpoint |
| Enqueue | Immutable signed bytes + lineage + destination set → OutboxReceipt | Durable bytes before send; per-destination outcomes and exact retry |
| Manage | Actor + typed command + expected configuration revision → durable Job/Revision | Desired/applied state separate; action-level grants, no arbitrary SQL/shell |

`AdmissionResult` is one of `NewFacts(Receipt)`,
`AlreadyDurable(existing Receipt)`, `Excluded(reason)`,
`Invalid(reason)`, `Retryable(reason)`. An existing event with a new permitted
provenance edge is NewFacts, not a new event body. A later repeated edge is
AlreadyDurable. A first/earlier event-level local observation can create facts
with provenance off. Retryable never authorizes progress. Durable outcomes expose
which body/evidence effects they cover, rather than returning a generic boolean.

ProducerRequestId belongs to a producer incarnation plus monotonic local request
identity; retries with another payload are rejected. Receipt persistence has a
defined correctness horizon, separate from request log retention. SourceRecordId
is permanent archive identity; producer retry records are operational state.

AdmitEvidence enforces enabled data families as well as visibility. In particular,
provenance off excludes imported event→relay sidecars and intentional per-event
availability measurements; an alternate ingestion route cannot create hidden
provenance state. Full signed originals and non-provenance observations retain
their separate meanings.

## Internal transport and topology

The selected remote interface uses protobuf over HTTP/2 gRPC with authenticated
peer identities and mTLS in split-role deployments. Same-process callers use
the same application contract without serialize/deserialize copies. Protobuf
records use fixed byte fields with explicit length validation, uint64 protocol
times, repeated ordered tag strings and a schema version; no Rust memory layout
or lossy JSON-number conversion crosses the boundary.

Initial services are Ingest (Admit/AdmitEvidence/receipt resolution), Archive
(commit/status), Read (query/page/capabilities), and Control (config/jobs/leases).
Public HTTP/Nostr interfaces are different adapters with their own authentication.
Frame/message/stream byte limits, deadlines, cancellation and per-peer quotas
apply before decode/allocation. Application limits still apply after decompression.
No unauthenticated private RPC port is published by Compose.

Use the maintained [tonic HTTP/2 transport](https://docs.rs/tonic/0.14.6/tonic/)
behind this application contract. Configure both encoding and decoding limits;
the library's finite decode default is not a bound on outbound messages or on
aggregate simultaneous streams. Pin the actual tested dependency set in P01-T01.

Topology configuration maps stable event ownership to ingest shards; provenance
routes with the event. Measurement/document streams use stable subject ownership
and source identity, not an arbitrary currently available worker. Initial one-host
operation has one writer per embedded store. Shared files are not mounted writable
into independent owners. Split-role operation can route to that owner; it does
not imply automatic failover or replicated durability.

Request timeout after possible durable acceptance is ambiguous. Resolve/retry the
same request identity; do not allocate a new source identity merely because an
HTTP response was lost. New topology generation requires controlled handoff,
positive old-owner exclusion and preservation of dictionaries/receipts.

## State machines and temporal boundaries

* Admission: validate → reserve → atomic synced commit → durable response.
  Cancellation before commit has no receipt; after commit recovery can resolve it.
* Archive batch: prepared → committed or ambiguous → receipt resolution →
  pending-reclaim eligible. Object cleanup cannot race a published reference.
* Index generation: building → verified → active → retained → reclaimable.
  Unclean serving loss invalidates its incarnation before any stale PG receipt
  can restore readiness.
* Derived target: planned → running → complete/empty/suppressed/failed.
  A generation uses exact input manifests, not “latest” resolved independently
  inside each SQL statement.
* Outbox: prepared bytes → queued → attempted → accepted/refused/unknown per
  destination. Unknown is retryable exact bytes; acceptance is not remote retention.
* Managed change: validated desired revision → applying → applied/partial/failed.
  Security restrictions activate under authority leases, not eventual UI refresh.

Process health uses starting/ready/idle/degraded/draining/stopped. Finite jobs use
succeeded/partial/failed/cancelled/skipped. A zero-row valid result is different
from unsupported, expired, missing, budget-refused or not-executed work.

## Schema, migration and generated documentation ownership

PostgreSQL migration names start with `0001_control.sql`,
`0002_ingest.sql`, `0003_relay.sql`, `0004_acquisition.sql`,
`0005_import.sql`, `0006_relay_lifecycle.sql`; later tasks reserve distinct phase-prefixed migrations through
the central manifest. DuckLake schema versions live in `contracts/archive/v1/`
and their migration runner; ClickHouse begins with
`migrations/clickhouse/0001_events.sql` and `0002_evidence.sql`.
Role ownership and applied versions are explicit; do not auto-run destructive
migrations from every service startup.

The central migration manifest reserves the following namespaces; numeric gaps
are intentional task ownership, not permission to invent missing migrations:

| Engine | Reserved identifiers and owner |
|---|---|
| PG | 0001 control, 0002 ingest, 0003 minimal relay (P02); 0004 acquisition, 0005 import (P03); 0006 relay lifecycle (P04) |
| PG | 0501 analytics control; 0603 read pins; 0609 DVM jobs, 0610 subscriptions, 0611 payments |
| PG | 0703 publication outbox, 0704 delivery, 0705 NIP-85 progress, 0707 NIP-66 progress |
| PG | 0801 management identity, 0802 commands/revisions/jobs, 0803 saved views |
| CH | 0001 events, 0002 evidence; 0502 current state through 0512 relay quality as named by P05; 0606 search projection |

Tasks extend the named owner file rather than creating another migration with
the same number. Archive format migrations are separately versioned by their
manifest. Once a migration ships, later changes use a new version and compatibility
transition; editing its past checksum is not an upgrade strategy.

Config schema, OpenAPI, protobuf and archive manifests each have one generated/
validated source pipeline. Generated reference includes defaults, units, ranges,
secret status, applicability and reload semantics. A checkbox cannot mark a
feature complete while its CLI/help/UI/capabilities advertise another meaning.

## Common practical acceptance runner

P01-T08 creates `tests/acceptance/run.py` and
`contracts/acceptance/result.schema.json`. Phase cases live in
`tests/acceptance/cases/`; fixtures in `tests/fixtures/`.
The proposed invocation is:

```sh
python3 tests/acceptance/run.py p02-t04 --binary dist/bigbrotr --compose packaging/compose/acceptance.yaml --evidence .research/implementation/p02-t04
```

This command is a planned interface; none of those product artifacts exist yet.
The runner consumes an explicit built release binary/bundle and isolated real
engines/controlled relay clients. It validates fixture preconditions, starts the
actual documented entrypoint, observes independent external results, injects the
case's fault, recovers, and writes versions, inputs, expected/actual membership,
resource measurements and bounded diagnostic evidence. Workload setup happens
outside timed regions; correctness is checked before performance is compared.

Result states are PASS, FAIL, NOT_RUN and SKIPPED with declared requiredness.
Missing required scenarios/tools cannot yield PASS. Tests intentionally run a
named wrong interpretation against a fixture to prove the oracle distinguishes
it; this is an explicit negative control, not source-code mutation in production.
Each case lists its preconditions/oracle, so it cannot succeed by reading an
empty database or invoking an unconnected helper. Cleanup uses recorded process/
container identities and touches only the isolated run.

Exact shell commands do not establish realistic coverage alone. Every phase task
also states the user journey, actual dependency, failure trigger and evidence
that the scenario must inspect. Shared runner implementation is not permission
to replace actual clients/engine reads with prearranged mock answers.

The acceptance result additionally records the actual release-bundle manifest path
and digest, alongside binary and per-platform/static asset digests. Early vertical
slices name their limited capability set; only the whole-release cases may claim
complete product installation. An assembly helper is not an install result.
