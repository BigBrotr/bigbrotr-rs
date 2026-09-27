# Derived information, read services, exports, and signed publication

Status: proposed implementation contract, 2026-09-11. This document covers every
historical Deriver, Ranker, NIP-85 publisher, NIP-66 publisher, Exporter, API, DVM,
and ArchiveRelay responsibility, plus their shared read and signing boundaries.
It defines behavior independently of the storage engines and Rust libraries
selected by the overall architecture. No service is implemented by this document.

[Research findings](../research/derived-read-publication-findings.md) record the
full historical reading coverage, specification changes, and owner questions.
Defaults below are proposed starting policies, not measured capacity guarantees.
Instance-wide memory, disk, file-descriptor, and network budgets constrain their
sum; a configuration validator rejects allocations that exceed that envelope.

## Common contracts

### Ownership and durable progress

The durable event archive owns verified original events and acquisition evidence.
Every accepted original retains its complete signed event, including content,
signature and ordered tags. There is no `heavy_fields` deployment axis or lossy
storage profile. BigBrotr is one product/image; a possible LilBrotr recipe means
full signed-event storage without provenance, explicitly distinguished from the
historical lossy profile. Social acquisition filters are orthogonal to provenance.
Indexes and analytical exports may project fields, but the authoritative archive
remains immutable and complete at
the event-record level. Deletion requests are retained as events; they change
logical serving policy rather than erase archived bytes. Low trust, poor relay
quality, supersession or a ranking threshold never authorizes archive deletion.
This is a fidelity statement about accepted records, not global network coverage.
The index subsystem owns committed searchable projections. Deriver owns immutable
fact/current-view generations; Ranker owns score generations; each publisher owns
its desired assertions and delivery ledger. The shared read core owns no archive
writes. Exporter owns export manifests and resume state. DVM owns accepted jobs,
cursors, and its result outbox, but has no authority to mutate archived events.
Supervisor owns scheduling and leases, not these services' correctness records.

A source cut binds instance identity, a per-stream/epoch vector of committed
ingestion positions, applied index frontiers and serving-store incarnation. It
does not compare unrelated shard sequences as a single scalar. Immutable source
fact identity survives replay, correction, backup and restore; a correction has
a new fact identity referencing the same logical subject. Projection row identity
also includes its distinct projection key, because one event creates multiple tag
rows. Apply the pinned cut before reducing immutable correction history. Event
timestamps never substitute for ingestion progress. Imported events retain their
attributed source and local acquisition identity. A COMPLETE generation means that
its declared targets have
been built for its declared source cut and scope; it does not mean all events on
Nostr have been discovered.

A generation manifest contains the service and schema version, input generations,
source cut, filter/scope digest, target definition versions, configuration digest,
artifacts and checksums, observation window, capability/coverage statement, and
creation/commit timestamps. Target states are `complete`, `empty`, `suppressed`,
`failed`, or `not-built`. Empty is a successfully computed empty result;
suppressed names an intentional policy or missing capability. Neither a failed
dependency nor an absent manifest may be relabeled as empty.

Time-sensitive target requests/manifests additionally bind their explicit
evaluation/window basis, `visibility_as_of_us`, `temporal_generation` and relevant
`expiry_deadline_us`, compatible with the
[ReadViewStamp](../specification/read-view-contract.md). Source progress and
temporal progress are separate: an expiration can change current eligibility
without a new source record. Pin the target generations and their temporal basis
together; an old COMPLETE membership/count generation cannot acquire a fresh
current stamp merely because its Deriver is paused. After its deadline, ordinary
current dependencies refuse until a compatible target exists. Explicitly dated
stale-analysis resources may retain the old meaning; they cannot advertise it
as current. Verify this with Deriver stopped while expiry/read ownership stays
active: new list/count/current-member views agree or refuse, then recovery
matches a rebuild at equal source and temporal inputs.

Write immutable artifacts, verify their integrity, then atomically commit the
manifest and publish its pointer. Failed or abandoned staging artifacts are never
reader-visible. Readers pin one manifest or dependency vector for the duration of
a request/job/export. Retention respects active pins, input lineage required by
published assertions, and at least the last usable generation per target. Finite
pin lifetimes and storage quotas prevent an abandoned reader from retaining data
forever; expiry produces an explicit restart condition, not silent repinning.

Use fenced leases for singleton work and idempotent checkpoint transitions. A
lease alone does not prevent a paused former owner from writing; durable updates
must reject an obsolete fencing token. Cross-service handoffs refer to committed
artifacts, not mutable in-process pointers. Stopping a producer makes downstream
data older; stopping a consumer does not prevent unrelated services from running.

### Configuration semantics and resource envelope

Configuration is typed. Unknown fields, invalid enums, incompatible capabilities,
unsafe key use, negative durations, and ineffective switches are errors. An empty
required relay/key/destination/profile selection is a startup error for an enabled
service. `0`, absent, and disabled have explicit meanings; zero is not a universal
unlimited sentinel. Secrets are references and are redacted even in diagnostic
configuration output. Effective configuration explains inherited values.

Reload classes used below:

| Class | Effect |
|---|---|
| Live | Apply atomically to new admissions; explicitly tightened exposure policy also invalidates incompatible active work. |
| Cycle | The next job/run uses the new value; an active run retains its recorded configuration. |
| Generation | Rebuild the affected derived generation; do not relabel old results with the new semantics. |
| Restart | Drain and restart the affected listener/client/worker. |
| Identity | Explicit coordinated key/algorithm rotation with publication history preserved. |
| Job | Immutable within one submitted export or DVM job; a different value makes a different job. |

All compute stages acquire CPU/IO/memory admission from the instance allocator.
Byte-bounded queues and backpressure apply between stages. The static
configuration namespaces for these templates are `deriver.*`, `ranker.*` and
`export.*`. Runtime admission checks the concrete job against its declared
budget. Candidate worker budgets are 512 MiB resident/8 GiB temporary disk for
Deriver, 1 GiB/16 GiB for Ranker, and 256 MiB/16 GiB for a JSONL export. These are reservations/ceilings
within the instance budget, not minimum hardware recommendations. If a correct
job cannot fit, spill using the chosen engine, partition an exhaustive plan, or
return `resource-exhausted`; never score/export a silently truncated subset.
One active heavy job per service is the initial default, with the global allocator
deciding whether different heavy services may overlap. Selected local native
consumers require their declared process grant and scratch root even when Archive
is not local. Static root applicability, distinct-root comparison and doctor
metadata checks must use the same selection. Actual scratch alias/permission/space
checks and child termination remain runtime gates.

Worker drain defaults to 30 seconds. Finish the current safe checkpoint or cancel;
do not pretend an uncommitted run completed. Queued work has a durable identity,
but queue growth has a byte and age budget. Outbox exhaustion pauses the producer
that needs that outbox; it never discards accepted jobs or archive records.

## Deriver

Purpose: make trustworthy, reproducible current state and aggregate evidence from
the archive without changing original events. Inputs are committed event/index
cuts, tag positions, observation records, coverage/gap evidence, capability and
exposure metadata, and explicit repair/dirty-partition notices. Outputs are
complete target generations and their quality/manifests. Deriver cannot enroll
relays, initiate probes, edit source records, publish Nostr events, or assign
subjective trust scores.

| Target family | Retained outputs and calculation contract |
|---|---|
| Current events | `current_replaceable_events`, `current_addressable_events`: winner by greatest `created_at`, then lowest ID; preserve coordinate parsing status. Resolve against the entire admitted source history, then apply read filters. |
| Current relay lists | `current_relay_list_entries`: one winning list event per author, replaced as a whole. Preserve read/write markers and endpoint URLs; a removed entry remains removed. |
| Global facts | `global_facts`: distinct events/authors, kinds, activity periods, event sizes and archive coverage. State count unit and accepted-source scope; do not describe acquired totals as total Nostr. |
| Social facts | `social_facts`: current follow/mute edges and separately defined posts, replies, interactions, topics and activity statistics. Keep raw edge source IDs and parsing/coverage status. |
| Assertion inputs | `nip85_input_facts`: versioned subject statistics with immutable source lineage and exact producer/subject/field mappings in [assertion bindings](../specification/assertion-bindings.md). Each of the four families enables only qualified supported fields. |
| Relay facts and state | `relay_facts`, `relay_state_aggregates`: local observations and attributed imported monitoring claims; deduplicate samples before aggregation and preserve conflicting observations. |
| Coverage | `relay_coverage`: per-endpoint scope/time/access context, traversal progress and explicit gaps; distinguish observation coverage from knowledge of all stored events. |
| Content composition | Typed projection of `relay_facts`/`relay_coverage`: exact witnessed or locally visible event/kind/author/topic counts, content bytes, author concentration and optional curator bands under [the composition populations](analytics-detail.md#relay-content-composition-from-a-declared-population). Existing direct availability jobs can supply their separately named served-requested-set population. No remote inventory claim or additional service. |
| Overlap | `relay_dedup_candidates`, `relay_cluster_edges`, `relay_clusters`: candidate method, pairwise set statistics, qualifying edges and components. Advisory similarity, never collection identity. |
| Propagation | `event_propagation_stats`: observer-relative acquisition timing, stratified by live/backfill/import path and clock quality; no causal propagation assertion. |
| Availability | `relay_selective_availability`, `relay_availability_frontier`: direct scoped served/not-served/unknown evidence and labeled estimates of historical availability, with transport/auth uncertainty. |

The native event representation must retain ordered tags. Coordinate extraction
is a versioned protocol interpretation shared with the read core; multiple or
malformed `d` tags are handled by that parser and remain visible in diagnostics.
Do not preserve the historical lexicographic-`d` workaround from an index that lost
positions. Retain a winner's malformed-list status rather than borrowing fields
from an older valid list.

An instance created with provenance disabled allocates no historical edge state.
Disabling it after collection stops new evidence in a new coverage epoch while
retaining prior facts and dictionary identities. Historical queries and derivation
may use those earlier supported intervals with explicit gaps; the current disabled
epoch is not evidence that past relay presence was zero. Ordinary repeated sweeps
reuse exact edge-admission receipts; they do not create permanent delivery records.

Incremental processing consumes a monotonically committed ingestion range and a
set of dirty partitions/keys. Late historical arrivals dirty their actual event
periods; repair notices below the normal frontier do the same. A configured
genesis floor explicitly limits the dataset and is present in every affected
manifest. Recompute partition contributions by replacement, or use an equivalent
transactional/idempotent delta scheme. Replaying a checkpoint cannot inflate a
count. Completion advances only after artifact and manifest commit.

Use immutable target manifests referencing unchanged partitions to preserve a
complete logical snapshot without rewriting all unchanged rows. Fast current
views, periodic statistics, and expensive overlap analysis have separate
generations. A consumer that needs several targets pins an explicitly compatible
dependency vector; it must not read whichever generation is latest in each table
mid-query. The storage planner selects indexed, sorted, partitioned, or streaming
execution from measured costs rather than a mandatory SQL idiom.

Local observation identity is observer UUID plus a durable acquisition/probe
identity, independent of the Nostr signing key. Imported claims retain signed
event ID and author, source archive/relay, import verification status, measured
time if stated, and local receipt time. Exact duplicate records collapse before
statistics. Two different claims with the same timestamp are not automatically
duplicates. Record per-observer contributions so a prolific monitor cannot gain
weight solely by sampling more often. Report distinct keys separately from known
operator identities; keys are not proof of independence.

Overlap uses one common reference window and explicitly eligible coverage for
both endpoints. Initial Jaccard threshold 0.99, minimum 10,000 shared distinct
events, and a 24-hour analysis window preserve the historical analysis profile.
Store set sizes, intersection/union, method, sampling error if any, and evidence
window. Connected components retain edge evidence and do not assert all-pairs
similarity. Do not merge endpoint identities or suppress collection on this basis.

Selective availability requires a direct probe or advertised-ID/fetch attempt in
the same access context. Distinct event/probe identity defines both numerator and
denominator. A timeout or authorization failure stays uncertain, not evidence of
intentional censorship. Coverage/frontier calculations retain the exact observed
scope; never subtract an unrelated safe-tail lag to manufacture evidence. Keep
availability observation history independently of derivation-run retention.

| Setting | Proposed default | Unit | Reload |
|---|---|---|---|
| `enabled` | true in the complete online recipe; false in archive-only recipes unless a separately supported offline execution mode is selected | boolean | Live |
| `current.interval` / `facts.interval` / `overlap.interval` | 300 / 86400 / 86400 | seconds | Cycle |
| `targets` | explicit complete-online target set below; no wildcard activation of supported or newly registered definitions | set | Generation |
| `work.max_ingestion_bytes` | 256 MiB per scheduled chunk; drain all chunks over repeated runs | bytes | Cycle |
| `work.max_duration` | 3600; cancel only at safe partition boundaries where possible | seconds | Cycle |
| `genesis_floor` | none beyond source scope | event timestamp | Generation |
| `overlap.window` / `overlap.jaccard_min` / `overlap.shared_min` | 86400 / 0.99 / 10000 | seconds / ratio / events | Generation |
| `retention.completed_generations` | 3, subject to last-usable and active pin protection | generations | Cycle |
| `continue_independent_targets_on_error` | true | boolean | Cycle |
| `memory.max_bytes` / `spill.max_bytes` | 512 MiB / 8 GiB, allocator constrained | bytes | Cycle |

The complete-online recipe selects these existing target/relation bindings and
their registered baseline definitions. This is an execution default, not a
reduction in implementation quality or supported analytical features. A new
registry entry never silently joins the default schedule. Independent recipes
select only their declared capabilities; archive-only and acquisition-only do
not start these online computations.

| Selected work | Initial activation and consumer |
|---|---|
| `current_replaceable_events`, `current_addressable_events`, `current_relay_list_entries`, `serving.current_graph_edges` | Current headers/memberships and eligible public social relations for normal client reads, author pages and follower/following facts. Incrementally maintain these without requiring seeds, diffusion or a CSR snapshot for every tick. Current temporal deadlines remain independent of the 300-second batching cadence. |
| `global_facts`, `social_facts`, `nip85_input_facts` | The existing baseline corpus/kind/author/activity and supported interaction definitions for dashboard/statistics/subject pages and optional publication. Field-specific missing/unsupported states remain; no signer is activated. Optional historical issuer-authority/zap bindings require their declared input grant and qualification, and unavailable rank never suppresses otherwise qualified facts. |
| `relay_facts`, `relay_state_aggregates`, `relay_coverage` | Local/resolved monitoring, traversal/gaps and the selected base `relay.composition/1` projection. Composition selects only its public `visible-witness/1` scope and six precomputed components from [analytics detail](analytics-detail.md#materialize-only-the-selected-useful-views), when provenance inputs exist. No all-population/window/viewpoint cross-product. Missing provenance suppresses only its dependent components; local monitoring remains usable. |
| `relay_quality_scores` operational vector, independently owned by P05-T12 | Scoped availability/latency/coverage and tested-capability explanations for the default directory when qualifying monitor inputs exist. It runs independently of social seeds and of the optional `general-read-relay/1` scalar. Insufficient evidence preserves missing reasons and any qualified components. |

`nip85_input_facts` may remain sparse: do not allocate an all-event set of
zero-engagement rows or indexes used only by a disabled publisher. A bounded
subject lookup may produce an exact zero only after verifying subject eligibility,
definition applicability and a compatible complete contribution target that proves
no eligible contribution exists at that cut. Missing or temporally incompatible
targets, unresolved issuer/target evidence and withheld bindings follow their
registered missing/withheld rules; row absence alone never proves zero. Shared
fact/retraction indexes needed by active dashboard or client resources remain maintained.
Enabling publication enumerates eligible subjects under the existing complete
keyset/job contract and admits any additional support-index work; it does not
require pre-existing zero rows. Its potentially corpus-sized sweep remains charged.

`event_propagation_stats`, `relay_overlap_groups`, `relay_overlap_members`,
`relay_dedup_candidates`, `relay_cluster_edges`, `relay_clusters`,
`relay_selective_availability` and `relay_availability_frontier` are supported
explicit selections or admitted jobs, not default fleet-wide schedules. Their
configured cadence applies only after selection. Direct availability uses an
already authorized finite probe selection; enabling a reducer cannot start
network probes. Topic distributions, broad alternate composition windows and
curator bands follow the same explicitly selected read/job/schedule rules.
The dashboard presents available measured core views plus clear job/disabled
states; it never fills an uncomputed panel with zero or silently launches an
exhaustive job. Bounded event drilldown and authorized corpus export remain
available through their own read/job capabilities without building every global
distribution. Optional features still require their full correctness, recovery,
interop and capacity acceptance before advertised support.

On stop, finish a safe partition or abandon staging; committed manifests remain
usable. On restart, resume the ingestion/dirty ledger. If required journal history
was pruned before catch-up, report `rebuild-required` and rebuild from a retained
authoritative corpus; never advance over the gap. A failed target leaves its last
complete generation visible with age and failure metadata, while independent
targets proceed. Operator status names source lag, dirty backlog age/bytes, target
state, scan bytes, spill bytes, commit age, and why output was suppressed.

## Ranker

Purpose: produce attributable, versioned scores from facts. Inputs are pinned
current social graphs, interaction facts, relay measurement/coverage generations,
and an editorially owned algorithm profile. Outputs are immutable score sets and
explanation metadata. Ranker neither signs nor decides where to publish. It must
not fetch arbitrary external graphs or decrypt private lists as an implicit step.

Every aggregate/score generation binds an input visibility domain and policy
revision. Public graphs and aggregates use only inputs authorized for that public
domain; filtering the final subject rows cannot remove private edges' influence
on an otherwise public score. Restricted and public calculations remain distinct
generations, even when they share physical source storage. Policy tightening
invalidates affected generations and queued publication eligibility before their
next use. Public quality metadata uses opaque cut handles or visible-domain
counters rather than exposing private stream sizes or authorization identifiers.

Each score record contains subject type/identifier, algorithm family and semantic
version, configured publication identity, viewpoint/curator, parameter digest,
seed-set digest and source, input manifests, source window and coverage, raw score,
normalization/population, published-scale value, component contributions, and
missing/suppressed reasons. A run ID is an execution identifier, not an algorithm
version. Distinct algorithm and personalized viewpoints require distinct NIP-85
service keys; mapping is validated by the signing registry.

The default social algorithm is `seeded-trust/2`, with a stationary target and
explicit error budget. Normalize configured positive seed weights to teleport
vector t. Use the current verified public follow graph, remove self-follows and
duplicate edges, and distribute each author's outgoing mass evenly. Dangling mass
returns to t. With alpha=0.85, the target is the unique probability vector solving
`x = (1-alpha)*t + alpha*P^T*x`, including seed-directed dangling redistribution.
Initialize from t or a compatible prior probability vector; a warm start changes
execution, not the mathematical target or input identity.

The default L1 error objective is `1e-8`. For candidate x compute an upper bound r
on `||F(x)-x||_1`, including floating-point evaluation/reduction error; accept only
`r/(1-alpha) <= epsilon`. The residual costs a graph pass and is charged. Use
stable bounded reductions and a documented conservative rounding bound; the
small exact-rational fixture independently checks the claimed enclosure. The
initial work ceiling is 256 passes, plus separately accounted residual passes;
budget/ceiling exhaustion withholds a new generation, never returns an unfinished
vector as converged. Check nonnegativity, finite values and total-mass error.
Implementation may improve summation/solver strategy within this target and error
contract, recording solver/version and actual bound. Bit-identical vectors across
solvers/platforms are not promised.

`seeded-trust/1` remains an explicitly requested compatibility/reference profile:
start at t and run exactly 100 deterministic ordered float64 iterations with the
same graph/dangling rule. Record residual; do not label that loop proof of
convergence or silently change its meaning. No seeds means `withheld:no-seeds`.
An incomplete graph or exceeded resource budget yields no new score generation.

Public mutes remain a separate observed relation, not a negative probability. The
candidate version-1 policy removes a follow edge when that same author publicly
mutes the recipient, and retains the attributed mute relation as explanatory
evidence. A separate trust-weighted mute statistic requires its own definition
and compute budget before becoming a published fact.
A broader negative reputation penalty requires a distinct documented algorithm
version and adversarial validation; it is not silently introduced by a tunable
negative edge. Reports and zap weighting remain supported input/statistical
families, with any score influence separately versioned. This preserves their
product value without claiming that unvalidated economic or report volume is
truth. No algorithm promises immunity to a compromised curator or trusted seed.

For a proposed public rank projection, retain raw probability and map positive
scores to a documented percentile among eligible positive-score subjects. For
positive score `s`, use `max(1, round(100 * count(0 < score <= s) / positive_count))`,
with half-integer rounding upward; zero remains zero. Equal computed scores receive the same value and a sole positive subject maps
to 100. For version 2 these are percentiles of the computed approximation, not
certified ordering of exact stationary probabilities: expose the error bound and
near-tie uncertainty. Keep deterministic ID ordering for display; do not call it
trust differentiation. Solver/configuration changes are part of the manifest. Record the population and exact formula
version. This is a proposed relative-rank product choice, not a claim that
NIP-85 prescribes this normalization. A new population changes the interpretation;
the API must expose raw values and provenance alongside the wire value.

Relay selection defaults to the scoped operational vector, with an explicit
use-case filter/order; no universal scalar is the default directory order. The
optional attributed `general-read-relay/1` profile is defined completely in
[analytics detail](analytics-detail.md#relay-profile-general-read-relay1): exact
outcome classes, irregular observed-time anchors, capped carry, duration
denominator, latency sampling, comparison strata and the independent fixture.
Its scalar is `Q=.625*A+.375*L` only when both components exist and at least five
classified actual in-window attempts qualify. Missing L withholds Q; available
weights are not redistributed. Tested capabilities remain a vector and optional
declared eligibility filters, without a capability/coverage score component.
The API exposes outcomes, coverage and latency even when Q is withheld. Full
input processing, correction/expiry state and exact quantile costs belong to
the profile's admitted plan; the sample floor is not a source-row cap or
statistical confidence claim. Relay and pubkey scores are distinct subject and
algorithm families, not interchangeable values on a 0–100 scale.

| Setting | Proposed default | Unit | Reload |
|---|---|---|---|
| `enabled` / `interval` | true / 3600 | boolean / seconds | Live / Cycle |
| `profiles` | `seeded-trust/2`; optional `seeded-trust/1` reference and `general-read-relay/1` composite, distinct identities | registry entries | Generation + Identity |
| `seed_set` | empty; explicit curator required for emission | weighted pubkeys | Generation |
| `graph.damping` / `epsilon_l1` / `max_passes` | 0.85 / 1e-8 / 256 | ratio / error / passes | Generation; fixed-100 reference profile remains distinct |
| `graph.ignore_self_follows` | true | boolean | Generation |
| `graph.public_mute_policy` | suppress same-author follow edge, retain explanatory mute mass | enum | Generation |
| `relay.window` / `min_observations` | 86400 / 5 | seconds / classified actual attempts ending in the window | Generation |
| `relay.component_weights` | availability .625, RTT .375; both required; capabilities/coverage separate | fixed weights | Generation/profile identity |
| `retention.completed_generations` | 3, subject to active publication/input pins | generations | Cycle |
| `memory.max_bytes` / `spill.max_bytes` | 1 GiB / 16 GiB, allocator constrained | bytes | Cycle |

Stopping Ranker leaves archive reads, derivation, and monitoring operational.
An empty configured seed set produces `withheld:no-seeds` before allocating or
rebuilding a diffusion/CSR job solely for ranking. Nonempty configured seeds still
require the complete pinned graph before deciding which seeds are present; no
missing seed becomes an invented vertex. Each explicitly configured eligible
viewpoint has its own admitted job; no implicit fleet of curator vectors exists.
The independently selected relay operational vector above remains scheduled
without social seeds; only the scalar needs its explicit composite profile.
Publishers may drain already frozen output but must not relabel old scores as new.
Restart selects the next required compatible input generation; it may coalesce
obsolete unpublished generations when only latest assertions are requested, with
that coalescing recorded. Historical analysis jobs never silently coalesce.
Expose withheld reason, curator/profile identity, input age, graph vertices/edges,
iteration residual, memory/spill, score distribution, and drift between runs.

## Assertion registry and incremental computation

All factual measurements, imported claims and derived scores use one versioned
definition registry. A wire tag is a serialization binding, not the definition
itself. Local richer assertions may precede a protocol extension, but their
standard-compatible serialization and any experimental extension are separate
bindings. [Protocol evolution](protocol-evolution.md) owns the extension/standards
roadmap; this registry owns meaning, provenance and computation.

| Definition field | Required meaning |
|---|---|
| Identity | Stable definition ID, semantic version, subject type and scope; changing meaning produces a new version. |
| Claim class | Direct measurement, deterministic aggregate, imported attributed claim, editorial score, or statistical estimate. |
| Authorship | Authoring service/observer identity, selected publication key if published, algorithm/viewpoint/curator, and separately all source event authors and observation sources. |
| Inputs | Source record kinds, field/parser versions, source cuts, required capabilities, auth/visibility context, and dependency definitions. |
| Calculation | Explicit numerator/denominator, duplicate/conflict handling, units, time/subject attribution, normalization and missing-value rules. |
| Window | Full acquired history, current state, fixed interval, or rolling interval; cutoff/expiry and late-arrival behavior. |
| Quality | Observation count, coverage/gaps, missing evidence, age, uncertainty/estimator when relevant, and verification status. |
| Update plan | Invalidating inputs, keyed contribution/retraction or rebuild strategy, state ownership, estimated cost class, and schedule. |
| Serialization | API schema and optional NIP kind/tag or extension binding, including quantization, omitted values, size limits and compatibility status. |
| Explanation | Human-readable purpose, limitations, data/source lineage and references sufficient to reproduce the value. |

Reuse pure definition/reducer functions across local views, derived generations
and publication projections only when their population/method semantics match.
Acquisition owns execution and consent for new network checks; reducers cannot
probe, sign or select destinations. Publishers translate eligible committed
values and use the shared outbox. Adding a metric normally adds a registry entry
and a reducer/binding, not another independently deployed service. The optional
[definition manifest](protocol-evolution.md#portable-definition-manifests-and-categorical-labels)
exports this registry's meaning; it does not create another definition authority.

An assertion's signer is not automatically the author of its underlying source
events, and an imported claim's subject is not its author. Keep these identities
separate in schemas and UI. An unpublished local assertion can name its observer
or computation service without pretending it has a Nostr signature. Signed output
binds that artifact to the authorized publisher identity and frozen algorithm.
Unsigned database metadata alone is not cryptographic proof of asserted lineage;
provide manifest hashes and a defined signed binding when that stronger claim is
needed. Do not insert arbitrary nonstandard provenance tags and call them NIP-85.

Incremental means maintaining the declared result across changes, not requiring
`CREATE MATERIALIZED VIEW`. ClickHouse's insert-time views process new input
blocks; right-side join changes do not refresh previous results. Cascaded views
also consume inserted blocks rather than the later merged source state. These
documented semantics explain why a naive count over replayed physical rows or
a mutable rank join cannot establish the required logical result.
[Incremental views](https://clickhouse.com/docs/concepts/features/materialized-views/incremental-materialized-view),
[cascading views](https://clickhouse.com/docs/concepts/features/materialized-views/cascading-materialized-views)

Selected CH aggregate states/materialized views remain possible measured
accelerators for compatible additive work. Feed logical admissions or an
idempotent partition/generation protocol, prove replay/backfill behavior and use
the required final merge at reads; source `ReplacingMergeTree` merges and a
finite insert-deduplication window do not substitute for logical-event identity.
Clock passage, deletion/replacement, repaired input and mutable join dependencies
require explicit invalidation/retraction or a new complete reduction. An exact
distinct aggregate can merge membership, but subtracting a removed contributor
needs retained refcounts/sets or recomputation. No CH-only shortcut silently
changes a visible metric into an ever-seen metric.

| Computation | Update strategy and practical cost | State and served result |
|---|---|---|
| Additive counts/sums over immutable accepted events | Process admitted source chunks and repair notices once; replace committed partition reductions or use equivalent exact idempotence. | Derived checkpoint/contribution identity; complete CH counts/sums. A selected insert-time MV is an accelerator inside this contract. |
| Exact unique authors/actors | Maintain per-group memberships/refcounts or exact union of retained sets; one actor in two days is one all-history actor. Refcount zero retracts its membership. | `derived.distinct_memberships` or equivalent exact partition sets, with complete typed CH counts; no sum of distinct-count scalars. |
| Current social/interaction/NIP-85 facts | Replace an old winning list/event's whole contribution, including an empty or suppressed new winner. Dirty referenced subjects on late resolution or eligibility changes. | Derived current state, contributions and reverse dependencies; CH current relations, interaction facts and qualified assertion inputs. |
| Rolling statistics/expiry | Advance expiry without ingestion; retain inverse contributions or recompute affected buckets for late data and noninvertible summaries. | Derived window inputs/deadlines and temporal generation; CH dated complete histograms/counts/quantiles. |
| Definition, scope or policy change | Build a new versioned result from required history; a configuration reload alone is not recomputation. | Separate working generation and pinned old result; CH/artifacts selected by the new verified manifest only. |
| Social graph and global rank | Current adjacency changes incrementally. Exact diffusion remains O(iterations × (vertices + edges)); percentile changes may affect subjects with no new events. | Derived changed-edge state and immutable graph/score artifacts; complete CH scores pin graph, viewpoint and normalization population. |
| Personalized or trust-conditioned joins | Share immutable adjacency/parsing; charge each viewpoint's solver and changed-weight fanout. Mutable author weights invalidate prior joined contributions. | Optional admitted score/join state and dependency indexes; separate completed CH projection or artifact, disabled profiles allocate no extra join state. |
| Relay pair similarity | Naive pairs are quadratic in endpoints and per-event fanout; prune soundly or disclose approximate candidates. | Existing scoped membership/candidate state and immutable result artifacts; CH complete pair/component generations. |
| Publication | Compare eligible normalized meaning from committed inputs; sign/send changed values or missing-target deliveries. | PG publication manifests and exact-byte outbox; no fact recomputation in the send loop. |

Across these cases, Indexer-owned derived RocksDB commits working transitions,
checkpoint and pending output identity together. CH serves verified complete
result partitions; the fenced PG manifest/pointer selects them. A source cursor
may advance over an empty delta, while expiry/dirty work may advance a target
without any new source cut. Both directions are required, independently of MV use.

ReadCore translates authorized typed requests into bounded storage queries,
including CH SQL over these precomputed generations; it does not require a raw
archive rescan for each dashboard read. Public HTTP/Nostr/DVM readers retain its
resource, authority, budget and compatible-view boundary. Direct SQL remains an
operator/research capability against explicitly granted read-only scoped data or
offline exports, with its own database role and resource limits. It is not an
unrestricted public endpoint and cannot bypass those grants or silently promote
an incomplete/stale analytical result into current product facts.

Generation manifests preserve the actual dependency vector, so a frequently
updated event count can coexist with a less frequent global graph score without
pretending both have the same freshness. Dirty queues coalesce repeated work by
definition/partition/subject while retaining the greatest required source cut and
repair evidence. A bounded queue overflow writes a durable rebuild marker for
that scope; it cannot silently drop invalidation. Configuration that enables an
assertion also enables/checks its dependency plan, or reports why it cannot run.

Graph acceleration such as warm starts, local propagation, residual stopping and
approximation needs its own recorded algorithm contract and comparison with the
deterministic reference. It must not be presented as exact incremental TrustRank
without evidence. The scheduler can batch updates and serve an older explicit
generation while expensive global work finishes. Maintain publication quantization
and change detection to avoid unnecessary network work, while exposing raw scores
and precise input freshness through the read APIs. Never optimize a global graph
by silently dropping low-rank nodes from the authoritative graph or archive.

## Signing and publication delivery boundary

Signing is a narrow service interface over a typed unsigned event, intended
purpose, algorithm/viewpoint if applicable, and expected public key. It validates
kind, subject coordinate, content/tag size, caller authorization, and configured
purpose before invoking a signer. Key access is never passed into generic query,
template, plugin, or export code. The caller independently verifies the returned
public key, event hash and signature against the exact requested event. Recompute
the canonical event hash from the complete current body; a library's cached event
ID or cached verification result is not independent evidence of body integrity.

| Purpose | Permitted scope |
|---|---|
| NIP-85 assertion | Configured subject families and exactly one registered algorithm/viewpoint per service identity |
| NIP-66 measurement publication | Public eligible endpoint or non-dialable relay identity assertions and configured monitor metadata |
| DVM result/feedback | Accepted verified local job ledger entries and the documented request profile |
| NIP-42 AUTH | Explicitly configured relay/session challenge; never a general publication key fallback |
| Optional monitor write probe | Explicitly enabled acquisition probe policy with its own identity and limits |
| NIP-46 transport | Separate client transport identity and approved remote signer connection |

The registry distinguishes key material location from public signing identity.
Local restrictive files and operating-system key stores are initial adapter
candidates; remote NIP-46 is an independent adapter. KMS/HSM support is conditional
on demonstrated secp256k1 BIP-340 signing and correct event hashing; generic ECDSA
support does not qualify. A NIP-46 connection checks `get_public_key` and preserves
the distinction between remote signer key, client key, and user key. Remote
authorization challenges pause signing with an actionable operator state. No
automatic key replacement or plaintext private-key fallback is allowed.

NIP-85 algorithm identity rules and NIP-46 connection behavior follow the current
[NIP-85](https://raw.githubusercontent.com/nostr-protocol/nips/master/85.md) and
[NIP-46](https://raw.githubusercontent.com/nostr-protocol/nips/master/46.md)
contracts; pin exact tested revisions before implementation. A profile/purpose
allowlist is necessary but not sufficient: validate the whole event shape so an
allowed kind cannot be used to sign unrelated attacker-chosen content.

Publication state proceeds through `desired`, `awaiting-signature`, `signed`, and
per-target `pending`, `accepted`, `retryable`, or `rejected`. A withdrawal is its own
desired revision, not deletion of a delivery row. Persist the immutable desired
artifact and its provenance before signing, then persist exact signed bytes before
the first network send. Delivery records key on signed event ID and canonical
target endpoint. A configurable success policy describes observed delivery, while
unaccepted targets continue retrying within policy; one accepted target never
erases failures at other targets.

The semantic digest excludes execution run IDs and delivery timestamps unless
they are actually part of the published meaning. It includes all emitted content
and ordered tags after canonical construction, subject, identity, profile/version,
and withdrawal state. Unchanged payloads reuse the existing signed revision for
new or missing targets. Changed payloads allocate a monotonic revision and
`created_at` for their replaceable coordinate. This timestamp records publication,
not the input observation's event time. When multiple changes occur within one
second, coalesce before signing or wait for a valid later timestamp; do not race
same-time replacements or drift arbitrarily into the future. Guard clock rollback
using persisted coordinate state and surface a clock-related wait.

Before constructing a new context artifact, compare a normalized assertion
projection: values, omission/eligibility, definition/method/viewpoint/population
meaning and withdrawal. For unchanged NIP-85 assertions, a newer internal cut or
computation timestamp alone keeps the prior signed revision and its **original**
context/lineage. Do not rewrite context behind an existing digest, or add a new
context digest merely to defeat changed-content suppression. Meaningful scope,
method or eligibility changes do require reconsideration even when numbers agree.
NIP-66 may publish a genuinely new attempt at its configured cadence; an unchanged
old attempt is never renewed by a scheduler tick. This pre-projection comparison
does not replace the final digest over the actual emitted ordered bytes.

Each publisher's existing scheduler uses durable fair endpoint/subject keyset
progress, bounded pages (initially 500), encoded-byte/outbox grants and finite
cycle duration. Coalesce unsigned updates by coordinate; preserve exact signed
obligations. Budget refusal defers the remaining suffix, not just its first page,
and reports oldest pending age, full-sweep age and achieved versus intended
cadence. Separate cheap changed-value selection from costly global percentile
recomputation. A changed rank distribution can enqueue many subjects; scheduling
and queue overflow retain correctness through the existing rebuild/sweep marker.
New destinations still receive stored signed bytes without recomputation.

Retries send the exact stored signed event. Nostr event identity does not require
the signer to produce a deterministic signature, so reconstructing/re-signing an
otherwise identical event is not a substitute for storing its bytes. A crash
after relay acceptance but before receipt persistence may resend the same event;
the contract is at least once. Relay `OK` is an attributed acceptance response,
not proof of retention or global replication. Optional read-back verification is
separate evidence and cannot create a permanence guarantee.

An identity rotation retains old coordinate state, pending deliveries, and the
mapping from old to new profile identity. Restoring stale backup state requires
operator recovery of publication sequence/key policy before enabling signing.
Never silently resume an old signer ledger after an acknowledged rollback.

| Shared signer/delivery setting | Proposed default | Unit | Reload |
|---|---|---|---|
| `key_ref` / `expected_pubkey` | required for every enabled signing purpose | secret reference / pubkey | Identity |
| `signer.timeout` | 30; remote authorization waits are separately visible | seconds | Cycle |
| `signer.max_inflight` | 2 per identity | calls | Live |
| `delivery.relays` | empty; explicit targets required when enabled | URLs | Cycle |
| `delivery.acceptance_policy` | at least one accepted; continue retrying remaining targets | enum | Cycle |
| `delivery.max_inflight` | 4 total, 1 per target/coordinate | sends | Live |
| `delivery.retry_initial` / `retry_max` | 5 / 3600 with full jitter | seconds | Cycle |
| `delivery.outbox_max_bytes` | 256 MiB per publisher, allocator constrained | bytes | Live |
| `delivery.pending_max_age` | 7 days, then operator-visible parked state | seconds | Cycle |
| `delivery.readback_verify` | false | boolean | Cycle |
| `transport.allow_insecure` | false | boolean | Restart |

Outbox age expiry parks work rather than asserting success or deleting it. Requeue
is explicit and idempotent. Emit bounded metrics for pending age/bytes, key/purpose
status, signing latency, target acceptance/rejection, retry cause, and frozen input
age. Do not place subject IDs, query text, keys, or signatures in metric labels.

## NIP-85 publisher

Purpose: turn pinned subject statistics and score generations into interoperable,
attributable assertions. Inputs are complete Ranker/Deriver manifests and a
registered assertion profile. Outputs are signed assertion events, optional
provider metadata, and durable delivery/withdrawal state. No live join to an
independently latest facts table is permitted. A publication artifact freezes all
values and lineage needed for reproduction even if input generations are later
compacted.

All four original subject families are planned. Enabling a kind without its
registered fact definition is an error, not a promise that the subject is handled.

| Subject | Kind | Intended information |
|---|---|---|
| Public key | 30382 | Relative rank, followers, first observed post/activity basis, posts/replies/reactions, sent/received reports and zaps, daily zap averages, topics, UTC active hours |
| Regular event | 30383 | Rank, comments, quotes, reposts, reactions, zap count and amount |
| Addressable coordinate | 30384 | The event-family statistics attributed to a stable coordinate, with version versus coordinate aggregation made explicit |
| External identifier | 30385 | Rank, comments, reactions, identifier type and attribution under the selected NIP-73 interpretation |

The versioned statistics catalog defines each numerator, denominator, unit,
observation window, subject association, duplicate rule, and exclusions. Posts and
replies need explicit NIP-10/NIP-22 interpretation; reposts and quotes require
distinct rules; reaction and report counts count valid distinct source events, not
physical rows. Zap accounting must validate the selected receipt/invoice semantics
and distinguish claimed receipts from verified payment evidence; units cannot be
guessed from similarly named tags. Do not call these values audited revenue.
Topic extraction has a version and a count/sample basis. Active hours are UTC with
sample size and tie policy; first activity is first observed in the declared
corpus unless historical completeness is actually established. Missing inputs
omit an optional assertion field and add local explanation; they never become a
fabricated zero. Each catalog revision carries interoperability fixtures before
that assertion family is enabled for publication.

Provider kind-0 content explains organization, algorithm/version, viewpoint,
coverage and freshness policy, and the documentation URL. Kind 10040 is the
signing user's provider-preference list. An optional list for the provider's own
identity must say what that identity prefers; it is not advertising acceptance
by other users and cannot overwrite their preferences. Distinct social/relay or
personalized algorithms do not share a service key merely to simplify operations.

Eligibility is a versioned selection over the frozen artifact. When a previously
published subject becomes ineligible, keep a withdrawal record and stop refreshing
the old assertion. A configured owned-event deletion request can be attempted as
best effort, with delivery tracked separately; remote deletion is not guaranteed.
Do not publish rank zero to mean withdrawn. Public readers expose last measurement,
publication age and withdrawal status from local provenance; wire-only consumers
may retain stale assertions, which the provider documentation must explain.

| Setting | Proposed default | Unit | Reload |
|---|---|---|---|
| `enabled` / `interval` | false / 3600 | boolean / seconds | Live / Cycle |
| `profiles` | explicit registered algorithm/viewpoint to key mapping | registry entries | Identity |
| `selection.kinds` | all four implemented/validated families | kinds | Generation |
| `selection.batch_size` / `min_events` / `top_topics` | 500 / 1 / 5 | subjects / events / topics | Cycle / Generation / Generation |
| `max_input_age` | 172800; withhold refresh beyond this age | seconds | Cycle |
| `provider_profile.enabled` | false until operator content is supplied | boolean | Cycle |
| `own_provider_preferences.enabled` | false | boolean | Cycle |
| `withdrawal.publish_deletion_request` | false; retain local withdrawal ledger | boolean | Cycle |
| Signer, relays and delivery | shared table; no inherited public destination list | — | as above |

Stopping this service stops new signatures/sends, not Ranker or Deriver. Resume
drains signed pending events and recomputes latest desired state as policy allows.
Input failure withholds new assertions while existing delivery jobs remain
reproducible. The operator can preview exact unsigned events and their input
lineage without accessing a key, inspect per-target receipts, and explain why a
subject was omitted, stale, changed, or withdrawn.

## NIP-66 publisher

Purpose: publish an explicitly selected public view of stored monitor evidence.
Inputs are committed local monitor observations, endpoint/subject identities,
measurement configuration and publication privacy policy. Imported third-party
claims remain attributed claims; republishing them under the local key as if
locally measured is prohibited. Outputs are 30166 discovery events and optional
10166 announcements, kind-0 profile, and kind-10002 relay list, using the shared
publication outbox.

Endpoint identity follows acquisition's conservative full URL: scheme, path,
non-root trailing slash and query are not discarded. DNS/host aliases do not
collapse endpoints. Preserve NIP-66 pubkey-only subjects as non-dialable identities.
Normalize only with a documented rule that cannot silently change the relay's
resource. Publication eligibility additionally applies network/privacy policy;
having stored a private endpoint is not permission to expose it.

30166 construction uses the pinned protocol's required subject and only supported
optional tags, with explicit milliseconds for RTT. Repeated facts produce repeated
tags where required. NIP-11 JSON content is optional and bounded as untrusted
stored input. Shared-host measurements are reused only when the fact's scope
matches host, SNI, port, resolved address and observation validity; a TLS certificate
on one port is not automatically true of another endpoint. Precise geolocation,
DNS records, certificate detail and HTTP metadata have separate exposure controls.
No private address, loopback endpoint, local development URL, monitor secret,
or authentication token may enter a public payload.

Announcements describe checks actually configured and performed, plus intended
publication frequency; they do not imply every endpoint was checked successfully.
The current [NIP-66 specification](https://raw.githubusercontent.com/nostr-protocol/nips/master/66.md)
has conflicting prose and example timeout order. The proposed encoder follows
the prose, `["timeout", milliseconds, check]`, with a pinned fixture and consumer
interop evidence required before release. The parser may retain both raw forms
with a dialect status; it must not guess when ambiguous. NIP-32 extensions need
a published namespace, vocabulary and version; unmerged proposals are not core
NIP-66 support.

A stale or failed observation never becomes a newly dated successful measurement.
Freeze original observed-at times and omit unavailable optional metrics. An
endpoint's failure does not prove it disappeared or deleted events. Refreshed
discovery metadata may be republished only with an honest measurement-age policy.
Monitor location is independent of relay location, and omitted by default.

| Setting | Proposed default | Unit | Reload |
|---|---|---|---|
| `enabled` | false | boolean | Live |
| `discovery.enabled` / `interval` | true / 14400 | boolean / seconds | Cycle |
| `announcement.enabled` / `interval` | true / 86400 | boolean / seconds | Cycle |
| `measurement.max_age` | 28800; stale data withheld or explicitly omitted | seconds | Cycle |
| `include.rtt` / `capabilities` / `network_class` | true / true / true | boolean | Cycle |
| `include.nip11` | true after bounded sanitation/exposure policy | boolean | Cycle |
| `include.geo` / `dns` / `tls_detail` / `http_detail` | false / false / false / false | boolean | Cycle |
| `geo.max_precision` | 4 if explicitly enabled; never raise stored precision | geohash characters | Cycle |
| `announcement.monitor_geohash` | absent | geohash | Cycle |
| `profile.enabled` / `relay_list.enabled` | true only with operator-supplied public metadata/targets | boolean | Cycle |
| `publish_private_or_localhost` | forbidden in public publication profile | invariant | — |
| Signer, relays and delivery | shared table | — | as above |

The service stops/resumes independently of Monitor. A stopped Monitor eventually
makes publications stale; a stopped publisher does not stop measurement.
Catch-up prefers the latest eligible observation per subject and preserves any
already signed revision awaiting delivery. The operator can preview sanitized
output, trace each field to its original observation, and distinguish input age,
privacy omission, unsupported extension, and delivery failure.

## Shared read core

### Resource registry and query meaning

The registry is a typed product contract: resource name/version, subject and row
unit, supported filters/operators/sorts, projection, required capabilities,
current/history behavior, exposure policy, source-generation requirement, count
methods, search semantics, retention/cursor support, and limits. Resources include
events/current events, named history, event provenance, profiles/current lists,
global/social statistics, relay facts/coverage/overlap/availability, score profiles,
assertion provenance, and search. Unsupported capability combinations are explicit
resource states, not empty result sets. Dashboard and documentation consume this
registry; adapter authors cannot invent their own field meanings.

Admission validates shape and cheap metadata first. Authenticate and enforce
exposure before any content-dependent response. Resolve source capabilities and
an admissible access plan using indexes/statistics; pin the data cut required by
that plan; execute with runtime memory/scan/deadline budgets and bounded delivery.
Small control metadata reads during planning are allowed; an unbounded archive
scan disguised as an estimate is not. Engines must support cancellation or an
isolated, killable execution boundary. Result-count caps alone are insufficient.

Nostr-compatible event filters follow [NIP-01](https://raw.githubusercontent.com/nostr-protocol/nips/master/01.md):
AND between fields in one filter, OR among values and between filters, inclusive
time bounds, exact identifiers and case-sensitive tag matching under the pinned
specification. Deduplicate by event ID across matching filters before delivery
and count. Distinguish an omitted limit from zero. The current-event policy
authorizes the source population before replaceable/addressable winner and
deletion reduction, then applies ordinary expiry/query/exposure predicates.
Private inputs cannot suppress or alter the public result. A query restricted to an old
interval must not resurrect an older replaced value. Normal ID lookup follows
the resource's visibility semantics; full raw history requires a named historical
resource and authorization, not a hidden `ids` bypass in standard REQ.

The parser defines absent, empty and malformed coordinates without losing the raw
event. Follow NIP-09 ownership and timestamp constraints for effective deletions;
an `e` reference targets an exact owned event ID without a creation-time cutoff,
while an `a` reference targets owned coordinate versions at or before the deletion
request's timestamp. Unknown targets retain an unresolved owned-request candidate
and undergo author/coordinate verification if the target later arrives. A request
to delete a deletion request does not cancel its effect. In every case,
an unrelated author cannot delete another author's data. Public current views
exclude effective deletions; explicit historical API/export access has separately
configured exposure and authorization. All original bytes and deletion requests
remain archived. This uses the NIP-09 option of stopping publication while keeping
immutable evidence, rather than interpreting logical deletion as physical erasure.
See [NIP-09](https://raw.githubusercontent.com/nostr-protocol/nips/master/09.md)
and the policy explanation in [protocol evolution](protocol-evolution.md).
Do not claim local deletion policy erases previously exported or remote copies.

Snapshot queries pin one source cut and compatible index/eligibility generations.
Ordinary chronological traversal instead fixes its candidate acquisition cut and
rechecks a separately reported current eligibility cut per page, under the
[read-view contract](../specification/read-view-contract.md). It does not claim
immutable membership or equality to an earlier count. Other snapshots cannot
mix arbitrary live and derived state. Ordinary event ordering is descending
`created_at`, then ascending event ID. HTTP keyset cursors bind resource/version,
normalized filter digest, sort, source cut/generation, exposure-policy epoch,
position and expiry, protected for integrity and confidentiality as specified
under [cursor key ownership](#cursor-key-ownership-and-confidentiality). New
backfill appears in a new snapshot. An expired/pruned snapshot returns
restart-required, never a cursor
silently rebound to latest. Policy revocation can invalidate active cursors.

Count uses exactly the selected resource's visible predicate and distinct row
unit. Overlapping filters do not double-count. `exact` is honored or refused when
its plan exceeds budget. `auto` chooses exact when admissible, otherwise a named,
defensible approximate method and disclosed basis/error properties when known.
Physical replay duplicates with an approximate flag are not such a method.
NIP-45 wire mapping exposes its allowed approximation indicator; richer HTTP
metadata records the estimator. Its optional HLL wire mode is implemented only
against the exact specialized protocol construction and fixtures, never a generic
event-ID HLL mislabeled as compatible. The protocol HLL uses author pubkeys: its
distinct-actor estimate cannot stand in for a count of distinct reaction/event IDs.

Search is a resource, not an arbitrary query language endpoint. Define indexed
fields/kinds, tokenizer/language behavior, update frontier, ranking profile,
ordering ties and supported operators. Stable HTTP search pagination pins both
index generation and ranking version. Advertised NIP-50 search returns relevance
order, not ordinary time order, and follows its handling of unsupported textual
extensions; the structured HTTP API rejects unknown options. No SQL, filesystem,
script expression, or backend-specific search syntax is executed from a request.
Security exclusions cannot be undone by `include:spam` or another search modifier.

### Exposure, limits, and failures

Separate public current resources, public aggregate resources, and explicitly
authorized historical/provenance resources. Raw encrypted-message history requires
explicit exposure policy. Deleted and superseded events are available through
deliberately configured historical API/export resources; ordinary relay reads use
current/deletion-aware policy. Initial public defaults withhold restricted history
while authorized research access remains a first-class capability.
The same current policy revision applies to API, DVM, relay, public exports, and
publication eligibility. This does not alter the original signed event or
pretend that storage fidelity and public distribution are the same decision.

Load the policy before serving content. A missing policy store is not an empty
blocklist. Parsed policy may be cached, but authority to serve/send requires an
unexpired authorization lease for the effective restrictive-policy epoch, initially
at most 5 seconds. A saved configuration revision is desired state until affected
roles acknowledge application or their previous leases expire and bounded drains
finish. Notifications accelerate this transition but are not proof of enforcement.
On lease expiry/control failure reject affected queries and stop incompatible
deliveries, including already pinned reads and queued signed output. Revalidate
publication authorization before retrying; a durable signature is not permanent
permission to send. Authorization precedes details
that reveal existence. Restricted and nonexistent point lookups use the same
public response shape; this reduces disclosure but is not a proof of eliminating
all timing side channels. Do not emit query text or event content in ordinary logs.

Common read limits use the `read.*` configuration namespace. Adapter enablement
remains under its own prefix (`api.enabled`, `archive_relay.enabled`, and so on);
there is no additional `read.enabled` switch. `api.request_timeout` defaults to
the resolved `read.execution.timeout` plus 5 seconds, rather than a competing
fixed timeout. Explicit overrides retain at least that delivery allowance.

| Common read setting | Proposed default | Unit | Reload |
|---|---|---|---|
| `enabled` | false for each public adapter | boolean | Live |
| `default_page_size` / `max_page_size` | 100 / 1000 | rows | Live |
| `max_filters` / `max_values_per_filter_field` | 8 / 100 | count | Live |
| `request.max_bytes` | 64 KiB | bytes | Live |
| `execution.timeout` | 15 | seconds | Live |
| `execution.max_scan_bytes` / `max_memory_bytes` | 256 MiB / 64 MiB per query, global allocator constrained | bytes | Live |
| `execution.max_concurrent` / `admission.queue_max` | 16 / 64 across adapters in one deployment | requests | Live |
| `response.max_bytes` | 16 MiB per finite response | bytes | Live |
| `count.mode` | auto; exact when budget admits | enum | Live |
| `cursor.ttl` | 900 | seconds | Live |
| `cursor.clock_skew` | 30 maximum accepted inter-worker skew; independent of policy lease authority | seconds | Live |
| `public.rate` / `burst` | 5 / 10 per peer identity, plus global admission | requests/second / requests | Live |
| `policy.cache_max_age` | 60 for parsed metadata; never extends authorization | seconds | Live |
| `authorization.lease_ttl` | 5 maximum in the initial policy; revocation completes after lease expiry plus bounded drain | seconds | Live |
| `exposure` | current public events and allowed aggregates; explicit grants for history/provenance | policy | Live |

Page-size clamping is disclosed with continuation. Do not silently narrow event
time, authors, kinds, or a search expression to fit a budget. Metadata explains
unsupported shapes and acceptable scope; runtime exhaustion is a distinct error,
not a truncated successful page. Stream cancellation releases permits, pins and
engine work promptly; bound cancellation by measured time/work, not an impossible
universal promise that every backend stops within one row.

Every response has transport-appropriate quality information: applied cut,
generation, freshness, scope/coverage, approximation and cursor status. Nostr
event envelopes remain valid signed events; do not insert metadata into an event.
Nostr transports with no field for rich quality use protocol responses and
documented metadata resources, without claiming EOSE conveys completeness.

The [content-search contract](search-and-reconciliation.md) specifies the baseline
text corpus, matcher, relevance tuple, optional index lifecycle and engine
alternatives. It extends these shared read rules rather than defining a parallel
unrestricted search API.

## API

Purpose: provide stable read-only HTTP access for dashboard, external clients and
authorized researchers. It depends on the shared core and indexes; Deriver is a
dependency only for resources that require its generations. API has no ingestion,
publication, control-plane mutation or ad-hoc SQL route.

The proposed public route family is `/v1/events`, `/v1/events/{id}`,
`/v1/resources/{resource}`, `/v1/resources/{resource}/count`, and `/v1/meta`.
History and provenance have distinct registered resource names. Routes and schemas
must become a versioned OpenAPI artifact before implementation; these are planned
product paths, not claims that an existing server implements them. Event bytes and
decoded event objects retain their exact meaning; metadata is an outer envelope.
Support cursor/quality fields and cache validators tied to both data generation
and policy revision. Do not cache authenticated responses as shared public data.

Invalid/unsupported query returns 400; missing authentication 401; unauthorized
resource access 403, with point lookups using indistinguishable 404 where needed;
rate/queue limits 429 with Retry-After; absent required complete generation or
stale policy 503; execution deadline 504; expired cursor 410; incompatible cursor
409. A versioned problem code explains the condition without exposing raw backend
errors or sensitive query contents. An exact-count request never returns an
approximation under a success code without the caller selecting auto mode.

| Setting | Proposed default | Unit | Reload |
|---|---|---|---|
| `enabled` | false | boolean | Live |
| `listen.host` / `port` | 127.0.0.1 / 8080 | address / port | Restart |
| `route_prefix` | `/v1` | path | Restart |
| `request_timeout` | common execution timeout plus 5 seconds delivery allowance | seconds | Live |
| `cors_origins` | empty; explicit public origins when needed | origins | Live |
| `api_tokens` | empty; hashed/secret-referenced grants when enabled | references | Live |
| `trusted_proxies` | empty; only actual configured socket peers can supply forwarding identity | CIDRs | Live |
| `tls_termination` | explicit deployment ingress, or configured local TLS | mode | Restart |
| Other read limits | common table | — | as above |

An untrusted `X-Forwarded-For` header never changes rate-limit identity. CORS is
separate from authorization; wildcard origins cannot be combined with credentialed
access. Authenticated tiers may have different budgets but cannot bypass exposure.
Stopping API drains requests and releases pins; no durable query backlog exists.
Restart immediately serves available resources at their declared cuts. Operator
status distinguishes readiness by resource, policy age, rejection cause, query
plan class, engine scan bytes, cancellation latency, and latency distributions.

## ArchiveRelay

Purpose: expose ordinary read-only Nostr event retrieval and counting to relay
clients, using the shared read semantics. Inputs are REQ/CLOSE/COUNT messages and
a committed index change feed; outputs are EVENT/EOSE/CLOSED/COUNT and a truthful
NIP-11 document. It cannot ingest an inbound EVENT, apply inbound deletion events,
or run library background jobs that mutate the archive. Use read-only data-store
credentials in addition to interface separation.

Content search and NIP-77 server reconciliation are required supported
capabilities of the complete read product, with per-deployment enablement.
Their engine choices, matching/ranking, projection ownership, scoped sets,
resource bounds and acceptance cases are defined in
[search and reconciliation](search-and-reconciliation.md). Inputs also include
NEG-OPEN/NEG-MSG/NEG-CLOSE; output includes NEG-MSG/NEG-ERR. They traverse the same
authorization and cost boundary. Serving reconciliation does not require
provenance and is distinct from Synchronizer's upstream reconciliation.

Standard REQ serves deletion-aware current views and ordinary nonreplaceable
events according to registered event policy. Return each matching event once per
subscription even when filters overlap. A new REQ with the same connection-local
subscription ID replaces the previous one. Validate NIP-01 subscription-ID limits,
filter shape, and resource budgets before the expensive plan. Refuse inbound EVENT
with `OK` false and a stable supported reason. COUNT uses the shared counting
operation; advertise NIP-45 only when it is implemented and tested. NIP-11 limits
are generated from effective limits, not a separate contradictory document.

The proposed conforming behavior is historical catch-up followed by a bounded
live feed of newly committed matching archive events. Capture a feed frontier
atomically with the historical query cut; query through that cut, send EOSE, then
deliver changes after it without a gap. The feed includes new acquisitions of old
event timestamps, not only events with `created_at > last_seen_time`. Deduplicate
the boundary and apply current/deletion/exposure rules to newly eligible events.
No protocol claim can retract events a client already saw; deletion events and
current policy govern future sends. Slow consumers lose the subscription with an
explicit CLOSED and must resubscribe; no infinite per-client backlog is retained.

`limit=0` sends no stored events, sends EOSE, and keeps new-event matching active.
Do not implement the historical open-but-silent contract. If the implementation
cannot yet supply the feed, an explicitly documented archive-only mode may close
after the snapshot, but it is an interoperability limitation and cannot claim the
full ongoing subscription contract. A custom NIP-11 field does not override
standard behavior. EOSE is completion of this stored-result phase, not global
Nostr completeness. Preserve applicable relay protocol completion hints only
when the selected specification and actual evidence support them.

| Setting | Proposed default | Unit | Reload |
|---|---|---|---|
| `enabled` / `listen` | false / 127.0.0.1:8081 | boolean / socket | Live / Restart |
| `connections.max` | 128, within global socket budget | connections | Live |
| `subscriptions.max_per_connection` | 8 | subscriptions | Live |
| `filters.max_per_subscription` | common maximum 8 | filters | Live |
| `message.max_bytes` | 64 KiB | bytes | Live |
| `send_buffer.max_bytes` | 1 MiB per connection, global byte cap also enforced | bytes | Live |
| `slow_client_timeout` | 10 without write progress | seconds | Live |
| `live_feed.enabled` | true when ArchiveRelay enabled | boolean | Restart |
| `live_feed.retention` | 900; exceeding cursor retention closes lagging subscription | seconds | Cycle |
| `max_subscription_lifetime` | 3600; explicit CLOSED permits client renewal | seconds | Live |
| `nip11` | generated supported capabilities/limits plus operator public metadata | document | Live |
| Other read limits | common table | — | as above |

On stop, close/drain sockets with a stable reason and release reads; readers
reconnect and resubscribe, so no per-client state blocks acquisition. Index lag
delays live delivery and is observable. Missing feed history closes affected
subscriptions rather than skipping it. Policy tightening stops incompatible live
delivery immediately. Metrics report active sockets/subscriptions, actual buffer
bytes, feed lag, slow-consumer closures, rejected shapes, and read-only credential
violations. Never log whole relay frames by default.

## DVM / Nostr query service

The selected optional interface is specified completely in the
[BigBrotr query DVM profile](../specification/dvm-profile.md), with concrete
implementation/acceptance in [P06-T09–T11](../implementation/06-read-surfaces.md)
and the shared [signer/outbox](../implementation/07-publication.md). That profile
owns the wire schemas, numeric kinds, limits, encryption, payment and recovery
states; this summary does not define a competing set of defaults.

Use directed request 5999/result 6999/feedback 7000 plus mandatory exact
`bigbrotr.query/1` marker. This is an experimental application binding, not a
reserved kind or generic NIP-90 compatibility claim. HTTP and ArchiveRelay remain
the general interoperable read interfaces. The adapter is disabled by default;
explicit handler announcements name only its actual version and capabilities.

Requests contain bounded inline typed queries. Finite list/count operations share
the read core's authority, filters, exactness and pinned source. Event-only
subscriptions have a bounded initial snapshot, acquisition-feed handoff, numbered
at-least-once batches and same-requester signed renewal/cancellation. A generic
aggregate does not gain a fictitious change feed. No arbitrary URL fetching,
remote event dereferencing, job chaining or user-granted egress is supported.
Requested destinations must be a subset of the operator's allowlist.

Private requests select the explicit NIP-44 v2 variant; legacy NIP-04 is not a
fallback. Results and sensitive feedback are encrypted without copied cleartext
inputs. Requester/provider/timing/routing still expose metadata. Independent
canonical ID/signature validation precedes quotas and signing; replay resolves
the same durable job. Malformed/misdirected traffic causes no reply. A valid
rejection may create one bounded deduplicated feedback event.

Finite jobs have a 900-second absolute lifetime from acceptance, at most 60 seconds
queued, the shared 15-second query deadline, and delivery deadline no later than
300 seconds after durable result readiness or job expiry. Exact signed output is
durable before networking. Read pins can release after its required bytes are
retained; compact lineage and outstanding delivery obligations remain. At least
one accepted configured destination satisfies delivery; client processing is not
proven. Terminal expiry cancels undispatched sends rather than delivering after a
refund decision. The profile specifies late-ACK and crash behavior.

Payments default off. The selected optional `lnd-prepaid-v1` uses a fixed positive
msat quote per finite operation and an invoice-only LND adapter. Persist one
protected preimage/hash before invoice creation, resolve ambiguity by that same
hash, and trust independently checked node settlement rather than requester
claims. No spending RPC or automatic refund exists; late/overpayment or paid
undeliverable work retains an explicit refund-due ledger and operator-reported
external resolution. Paid recurring jobs are outside this baseline. Real isolated
LND regtest evidence is required before advertising the paid mode.

DVM failure/stop does not stop API, relay or acquisition. Jobs resume the same
source/result identity or fail explicitly if their cut is unavailable; replay
cannot revive an expired/cancelled request. Private DVM payload retention is
operational by default. Explicit public scientific outputs and independently
collected public events follow their own archival policy. Role status and private
administration expose redacted job/payment/destination state, never public raw
query logs or a misleading universal `sent` boolean.

## Exporter and integrity work

Purpose: produce portable corpora, native backups and migration artifacts with
honest fidelity and resumability. Inputs are a frozen authoritative source cut,
optional consistent control snapshot, explicit selection and destination policy.
Outputs are verified files/objects, a committed manifest, and per-job resume
state. Exporter never advances ingestion/index completeness, repairs corrupt source
data silently, or moves/deletes the authoritative archive as part of export.

| Mode | Contract |
|---|---|
| Native deployment backup | A consistent DuckLake catalog snapshot and all referenced data, plus the required control/pending checkpoint and identity dictionaries. Preserves canonical signed events, admitted facts and immutable corrections; ordinary repeated acquisitions are not a delivery log. |
| Portable Parquet corpus | Materialized selected rows from a pinned logical snapshot, with a standalone manifest usable without the live DuckLake catalog. Physical file enumeration is not the selection predicate. |
| JSONL event corpus | Complete valid signed events, one declared serialization per distinct event ID by default, with a versioned selection/dedup rule. Missing original fields indicate invalid input/archive damage; never emit a synthetic partial event as valid Nostr. |
| Analytical projection | Explicitly selected columns in a documented tabular/JSON schema may omit content or tag subsets. Label these as derived projections, not standalone verifiable Nostr events; retain event ID, source manifest and projection definition for linkage to the full archive. |
| Provenance companion | Explicit separately authorized records linked only to selected events, with local/imported attribution, verification status and auth/scope privacy treatment. |
| Filtered archive corpus | A separately identified Parquet/JSONL transformation with its own manifest; never claim whole source files meet a row filter without selecting/materializing their logical rows. |

Record the source instance, source cut, source generations, scope/filter semantics,
exposure-policy revision, schema/format versions, capabilities, expected files and
record types, observed/excluded counts by unit, coverage/gaps, original or
transformed status, checksum/byte counts, dedup method, and any distribution terms
supplied by the operator. A provenance companion cannot leak evidence for events
excluded from the public corpus. Unknown native record types fail the selected
compatibility contract explicitly; observation/control records excluded from event
JSONL are counted and explained. No invented license is attached to third-party
content merely because BigBrotr exported it.

Native backup incremental selection keys include source instance, catalog snapshot,
immutable file identity/digest and destination namespace, not a filename or legacy
segment number. Include any catalog-inlined data through the supported backup
contract; a live Parquet glob is not a backup. Portable corpus increments use
logical record selection and explicitly declare whether they are standalone or
delta manifests requiring earlier artifacts; compaction alone is not a new event.
Scope, format and policy form part of resume identity. Snapshot enumeration is
frozen; new source data belongs to another export generation. Write temporary objects or
files, verify bytes/checksums, establish backend-specific durable completion, then
commit the final manifest atomically or with an equivalent conditional object-store
commit. Local rename alone does not prove persisted file/directory metadata.
Remote copy success alone does not prove object checksum agreement.

A native deployment backup requires permission for the entire selected deployment
state, including protected provenance and control metadata. A caller authorized
only for a filtered/public corpus cannot request a partial artifact labeled as a
restorable deployment backup. That caller uses a portable corpus job instead.

A native deployment backup first establishes a storage/control cut
and pins the required immutable data and journal. Use the chosen engine's snapshot
and checkpoint protocol to prove all referenced artifacts exist and replay starts
from a valid boundary. A later independent control dump is not enough. Include
schema/config versions and recovery instructions, but never plaintext secrets.
Mark leases as non-restorable live ownership. Publication and external-effect
ledgers require separate rollback recovery before signers can resume.

JSONL dedup is by event ID across the full selected corpus. Emit either a declared
canonical rendering of the complete signed fields or original wire bytes when an
explicit wire-capture mode retained them. The default canonical archive does not
promise every encountered JSON serialization. A native backup preserves optional
wire-capture records only when present. Duplicate-delivery counters are operational
telemetry, not a permanent exported event-history family in the baseline.
Do not reconstruct a lossy event or claim the event hash is a hash of the literal
JSONL line. Use external sort/partitioned dedup with bounded memory and
temporary disk; when insufficient disk prevents a complete export, pause/fail
without committing its final manifest. Nostr filter sets use OR semantics, then
intersect that selection with the source scope and authorized exposure.

Archive-maintenance owns the single scheduled integrity scrub executor and durable
progress ledger. Exporter and administration may request, cancel or inspect that
same job through the domain interface; they do not schedule a second weekly walk.
Per-export artifact verification remains mandatory and is a different operation.
The shared scrub traverses immutable artifacts with checksums,
reconcile logical record identities and intentional quarantines/exclusions across
the archive and projections, and perform a deterministic rotating signature sample.
Persist traversal position so repeated restarts do not inspect only the newest
files. Report damage and affected references; repair is a separate authorized
process. Sampled signature verification detects some failures and is never labeled
full cryptographic verification. Offer an explicit exhaustive verification mode.

| Setting | Proposed default | Unit | Reload |
|---|---|---|---|
| `mode` / `format` | on-demand corpus export / Parquet; native deployment backup is a distinct job mode | enum | Job |
| `destination` | required; explicit filesystem/object-store adapter | URI | Job |
| `incremental` | true for compatible native backup destination; corpus delta mode explicit | boolean | Job |
| `scope` | corpus: all within source and caller-authorized exposure; deployment backup: complete authorized deployment cut | filter / full cut | Job |
| `include_control` | required for deployment backup, false for corpus export | boolean | Job |
| `include_provenance` | native fidelity; JSONL companion requires explicit authorization | policy | Job |
| `dedup` | logical event/fact identity for portable corpora; native backup preserves the selected catalog snapshot | mode | Job |
| `concurrency` | 1 export, with global IO admission | jobs | Live |
| `memory.max_bytes` / `spill.max_bytes` | 256 MiB / 16 GiB, allocator constrained | bytes | Job |
| `artifact.max_bytes` | 256 MiB target for newly constructed files; original native sizes preserved | bytes | Job |
| `verify_restore` | false for routine export; required acceptance/periodic recovery drill | boolean | Job |
| Scrub scheduling and budgets | one archive-maintenance policy/ledger; Exporter requests and observes it | — | archive-maintenance contract |

Stopping an export preserves verified completed artifacts and resume state; no
manifest claims completion until every selected artifact is verified. Restart
checks the same immutable source cut and destination identity before reuse.
Missing pinned source history produces `source-unavailable`, not a smaller
successful corpus. A failed export does not stop acquisition unless global disk
pressure requires ordinary backpressure. Scrub failure alerts independently.
Operators see bytes/read-write throughput, logical/physical/deduplicated counts,
remaining work, source pin age, destination verification and resume reason.

## Cross-service failure and recovery states

| Condition | User-visible effect | Required recovery |
|---|---|---|
| Indexer stopped or lagging | Reads/derivation show committed cut and age; no invented freshness | Resume replay; invalidate/rebuild projections only from authoritative data |
| Deriver target failure | Affected resource serves last complete with age or explicit unavailable; independent targets continue | Retry dirty work under same definition or build a new version |
| Ranker no seeds/insufficient evidence | Scores withheld with explanation; facts still readable | Supply editorial profile/evidence; build new generation |
| Signer unavailable/reauthorization required | Frozen desired output waits; no replacement key chosen | Restore approved signer session, verify expected identity, resume |
| Relay ACK lost or process crash after send | Target remains uncertain; duplicate delivery of same signed ID possible | Replay exact stored bytes; reconcile receipts without claiming exactly once |
| Publication target rejects an event permanently | Visible per-target rejection; other targets unaffected | Correct policy/profile in a new revision or explicitly requeue after target change |
| Policy store stale or policy revoked | Affected reads/public exports/publications pause or cancel | Restore current policy; rebuild/revalidate authorization before resuming |
| Slow HTTP/WebSocket client | Finite buffer; request/subscription closes, expensive producer is cancelled | Client retries/resubscribes within documented snapshot/feed retention |
| DVM outbox/queue full | New jobs not accepted; existing accepted work preserved | Drain delivery or provision approved capacity; no silent job loss |
| Temporary disk exhausted | Heavy job pauses/fails without complete manifest; committed readers unaffected | Free/provision budget and resume deterministic checkpoint |
| Restore from old backup | Data can be queried after integrity/replay verification; signing remains gated | Recover external-effect sequence and identity policy before enabling publication |
| Evidence/pinned generation pruned | Explicit restart/rebuild required; no continuity claim | New snapshot or reconstruction from retained authoritative source |

## Practical acceptance evidence

These are required future implementation scenarios, not tests already run. Drive
the packaged service entry points with real adapters wherever the boundary is the
subject of the check. Record fixture/data generation, exact versions, machine and
resource limits, command, observed outputs and artifacts so another engineer can
reproduce the result. Unit tests help parser/formula edge cases; they do not
replace these journeys.

1. **Historical catch-up and current views.** Import a corpus containing old/new
   replaceable versions, timestamp ties, multiple/malformed `d` tags, relay-list
   removals, late old events and a repair below the watermark. Run actual indexing
   and Deriver entry points. Compare current reads, facts and counts with an
   independently implemented small reference interpretation; show untouched
   partitions were reused and dirty partitions changed exactly once after replay.
2. **Evidence honesty.** Include duplicate and conflicting monitor claims, missing
   local provenance, partial relay traversal, live and imported acquisition times,
   timeouts/auth failures, and direct positive/negative probes. Inspect actual
   API outputs: absent capability, unknown evidence and service failure differ;
   averages are duplicate-safe; clusters retain edges; no peer absence becomes a
   censorship or retention claim.
3. **Score identity and attacks.** Run real Ranker jobs for empty seeds, two named
   curators, disconnected Sybil rings, a trusted seed linking to an attack cluster,
   conflicting public mutes and missing relay components. Verify repeatability,
   provenance and explicit limitations; show no-seed withholding and separate
   algorithm/viewpoint publication keys. Profile memory and spill on a graph large
   enough to exceed the resident budget, with correct failure if spill also fills.
4. **Independent publication validation.** Preview and sign all four NIP-85 subject
   families and NIP-66 outputs from stored fixtures. Verify event hashes/signatures
   with an independent implementation and read them through independent clients.
   Check tag types/units, output privacy, provider metadata, timeout dialect, and
   explanation-to-source linkage. A missing family definition must reject config.
5. **Publication crash matrix.** Use an actual local relay plus fault injection
   before signing, after signed persistence, after relay OK and before local receipt
   commit. Restart the service and compare exact bytes/IDs. Add a destination after
   an unchanged payload; it receives the existing event. Exercise clock rollback,
   same-second changes, old-data recomputation, stale withdrawal, a permanently
   rejecting relay and a disconnected remote signer without using a fallback key.
6. **Cross-adapter query parity.** Send equivalent filters through real HTTP,
   WebSocket REQ/COUNT, and signed archive-profile jobs. Check inclusive times,
   filter unions, duplicate suppression, current/deletion rules, visibility,
   exact/auto counts, source cuts, malformed requests and resource refusals. Search
   uses independently checked relevance ordering and declared index freshness.
7. **Real subscription lifecycle.** Hold a WebSocket past EOSE, including a
   `limit=0` request; commit matching new and backfilled events, then verify delivery
   without a cut/feed gap. Replace a subscription ID, send CLOSE, stop reading,
   exceed buffers, tighten policy and overrun feed retention. Observe bounded
   memory and backend cancellation through metrics/process measurements, not only
   a mocked channel assertion. Confirm no inbound EVENT or library task writes.
8. **DVM replay and privacy.** Submit signed duplicate, redirected, malformed,
   cancelled, expired and encrypted jobs through a relay. Restart during each job
   transition and compare durable result identities. A requester URL aimed at a
   private address causes no request. Inspect logs, feedback tags and output for
   decrypted query leakage. Exercise subscription cursor commit/outbox atomicity
   and late backfill. Paid mode, when implemented, needs real settlement adapter
   sandbox fixtures for underpayment, duplicate settlement, expiry and refunds.
9. **Portable export and blank restore.** Export during continuing acquisition,
   interrupt transfer, resume, verify the manifest and restore into an empty
   isolated instance through documented commands. Re-index and issue real API/
   relay queries; compare original signatures, distinct corpus counts, provenance
   and the declared source cut. Confirm leases are replaced and old publication
   state cannot enable signing without recovery. JSONL dedup must work when the
   corpus exceeds its resident memory budget.
10. **Independent stops and realistic pressure.** Stop each service while the others
    continue, allow measurable backlog, then restart and prove stated catch-up or
    explicit rebuild behavior. Combine multiple readers, one export, indexing and
    scoring under the deployment envelope; measure CPU, RSS, temporary disk, IO,
    scan bytes, network and p50/p95/p99 latency. Validate overload responses,
    fairness and preserved durable progress, rather than declaring success from
    isolated microbenchmarks alone.

Owner decisions preserve every accepted complete signed original and deletion
request, use logical deletion for ordinary relay serving, and provide explicit
historical API/export resources. Remaining product questions concern particular
public exposure grants, curator identity, Nostr query modes, live relay
expectations, and monitoring privacy.
Engineering work also needs an exact statistics catalog, pinned protocol/profile
revisions, independently demonstrated signer compatibility, engine cancellation
and snapshot guarantees, and measured defaults. These are explicit remaining
design/validation obligations; none justifies dropping a historical product area.

## Transport and policy regressions recovered from the complete phase reread

HTTP decoding is strict before query planning: malformed percent escapes, invalid
UTF-8, duplicate singleton fields, explicitly empty invalid values and unknown
fields receive the declared client error. Do not use a convenience decoder that
silently drops an undecodable predicate. Repeated fields are accepted only for
an explicitly documented list encoding. Apply one canonical parser shared by
request validation, filter hashing and cursor verification.

Cursor tests mutate every meaning-bearing predicate, including tag name and
value. Search whitespace is interpreted by the versioned parser: semantically
equivalent normalized expressions share a filter identity; a change in literal
meaning does not. Include quoted/escaped text. A client receiving a clamped page
must be able to continue using that response's own cursor and reported effective
page size; a clamp cannot manufacture an immediately incompatible cursor.

Policy intersection has a typed `MatchNone` result. If an access grant permits
none of the requested authors, do not remove the authors constraint and thereby
select all authors. Apply this invariant to events, counts, text search, live
subscriptions, NIP-77 sets and exports. Empty selection after policy evaluation
is distinct from a disabled/unavailable capability, and from malformed empty input.
Test restrictive changes on a read-only worker that misses notifications; the
short authorization lease still expires and incompatible sends cease.

One accepted DVM job has one resource envelope across all its filters, queries,
feedback and results. Reserve cumulative scan/CPU/work limits and concurrent
memory before admitting a fan-out plan; subqueries do not each receive a fresh
full quota. Count the Nostr outer fields, tags, signature, JSON string escaping
and framing when bounding a signed response. Use a conservative schema/row-byte
bound before execution, and actual incremental encoded-byte accounting while
building output. Refuse an unbounded result shape; if runtime data exceed the
reserved bound, send a bounded explicit error or use the profile's supported
artifact delivery, never a truncated successful signed result. Feedback itself
has a reserved finite allowance. Real transport tests verify cumulative work and
final wire bytes, including adversarial quote/backslash-heavy content.

## Detailed analytics and additional relay acceptance

The [analytical definitions](analytics-detail.md) complete the facts and Ranker
contracts: propagation buckets and quantiles, scoped availability, exact versus
candidate-based overlap, attributed claim resolution, seed availability, optional
publication eligibility, current adjacency and complete graph resource costs.
Their definitions and reference formulas govern those named outputs; they are
not extra independently deployed services or claims of executed benchmarks.

Exercise a real ArchiveRelay connection with multiple REQ filters: apply each
filter's historical limit, deduplicate the union by event ID, send EOSE, and then
observe newly committed matching events without applying historical limits to
the live stream. Include `limit=0` followed by a newly committed backdated event,
replacement of an existing subscription ID, and COUNT's response/completion
lifecycle under the pinned protocol. A COUNT request must not accidentally open
a REQ event feed. Repeat with overlapping filters and independent expected counts.
Server NIP-77 uses its separately specified complete-set filter restrictions.

## Visibility and real pagination continuation

The [read/runtime code walkthrough](../research/go-read-runtime-code-walkthrough.md)
adds a concrete pagination failure mode to the earlier semantic-cursor contract.
Apply authorized visibility before keyset ordering/limit where the engine can
implement it. Where a bounded scan and final revalidation are required, retain
an authenticated opaque internal scan position as well as delivered-page state;
the last delivered ID alone does not describe withheld candidates consumed after
it. Never disclose hidden IDs, counts or raw order keys through the cursor.

A mixed or entirely withheld candidate page is not proof of logical exhaustion.
Continue within the shared work budget, return a valid protected continuation
where the resource supports it, or report an explicit budget/policy-restart
outcome; do not mint a cursor its own next-page validator cannot accept. Test
following the actual returned cursor after allowed-then-withheld rows, all-
withheld candidates, clamped pages and policy changes against other eligible
rows. Assert progress, no repeated/skipped eligible results and no private data
leak. A pure fullness helper cannot prove that journey.

The typed read context carries the authenticated principal, grants, policy epoch
and source visibility domain through admission, execution and delivery. Selecting
a profile or receiving an API key is not a substitute for that context.

## Lossless HTTP filters and publication traversal

Define a versioned typed query envelope for collection/count resources. A
read-only POST to the existing collection/count route carries that envelope as
JSON; it invokes exactly the same read admission and has no mutation capability.
GET uses one bounded percent-encoded JSON `filter` parameter for the same typed
filter and documented scalar page/cursor options. Reject duplicated singleton
parameters, ambiguous mixed filter encodings and unsupported fields. Enforce
request-target/body/depth/term bounds before parsing and return the appropriate
size refusal; POST does not bypass read budgets or authorization. OpenAPI and the
generated SDK define these method/encoding pairs, while the dashboard hides the
encoding detail from ordinary users.

Filter arrays preserve literal values, including a comma within a tag value and
an explicitly empty first tag value. Do not comma-split decoded arbitrary strings
or silently drop their empty elements. An empty filter list is malformed; a
nonempty list containing a valid empty tag value is different. Round-trip
comma/percent/plus/Unicode/empty tag values through HTTP→typed AST→cursor and the
Nostr adapter, and compare actual result membership. Strict decoding is not
permission to reject a valid value that the chosen resource supports.

Current-state selection first determines eligible coordinate winners at the
source cut, then applies allowed predicates and visibility, then keyset limit.
Never take a limited prefix of winners and apply ID/time selection afterward
as though that prefix were the complete candidate set. Unsupported current-view
predicates may be rejected explicitly, not silently approximated. Deletion
horizons use an explicit absent value; absent is not timestamp zero. Test a
never-deleted timestamp-zero winner and an actual applicable zero horizon with
contrasting outcomes under the deletion contract.

Deletion checks use source-cut-bound, incrementally maintained event/coordinate
lookups for the relevant candidates. Do not construct a global all-history
deletion-horizon map for each small page. Charge candidate joins and any fallback
scan to the same read budget; an unavailable required deletion projection cannot
be interpreted as no applicable deletions.

NIP-85 publication traverses a pinned complete subject generation with durable
page progress; batch size is a bounded work unit, not a permanent maximum number
of subjects. Persist each page's exact signed intents and enumeration checkpoint
without losing a suffix after restart; per-destination delivery progresses
independently. Construct all enabled supported subject families through the
actual producer→stored artifact→publisher path. An unwired helper or configured
kind is not delivered feature support. Retain the shared monotonic coordinate
timestamp, same-byte retry and clock-reversal rules above.

Coalesce or serialize identical `(signed_event_id, destination, connection_epoch)`
sends with shared acknowledgement ownership. A second waiter must not replace
the first silently; stale completion/cleanup cannot consume another request's
response. Public capability declarations derive from activated, actually
populated resource bindings, not every helper in a static schema table.

External assertion subjects use registered per-namespace NIP-73 parsers, including
URL `web`, identifiers without a colon and multi-component namespaces where the
specification defines them. Splitting every identifier at its first colon is not
a protocol parser. Addressable subjects enforce the pinned kind/coordinate
semantics, not merely unsigned-integer syntax. Bind only measurements whose
direction, population and unit match the chosen assertion definition.

### Cursor key ownership and confidentiality

The inspected Go API can create a random cursor key per server instance. The
Rust deployment instead owns a persisted, protected cursor keyring shared by
its API workers. Bootstrap creates it once through the configured secret store;
control metadata records key IDs and lifecycle, not plaintext keys. Restart or
routing the next page to another worker must not invalidate an otherwise valid
cursor. Cursor keys are separate from Nostr signing and API credential keys.

The initial protected-files provider uses the optional deployment root
`read.cursor.keyring_ref`, a redacted `file:/absolute/filename` reference excluded
from accepted policy bytes. Existing administration does not require this root
until it selects a cursor consumer. `access cursor-keyring prepare --file CONFIG`
explicitly prepares local bytes before any Control registration;
`access cursor-keyring inspect --file CONFIG` only loads an existing file. Neither
command contacts Control, activates a key or grants cursor authority. Reports
contain dataset/keyring/key IDs and a file fingerprint, never secret bytes.
`prepared-local` reports successful preparation; `inspected-local` observes
existing bytes without certifying a previous sync or Control registration.

The parent is an operator-created dedicated directory owned by the effective UID
with mode0700. Preparation locks its retained descriptor exclusively, refuses
unexpected or partial siblings, writes a fresh exclusive mode0600 temporary file,
syncs its bytes, renames it and syncs the directory and its parent. An identical
complete retry writes the same verified bytes through a fresh synced inode, never
new key material. First publication uses no-replace rename, including against a
file created after the initial absence check. Complete retry replaces the verified
existing file under a cooperative lock; operators must exclude other tools that
write or rename this directory during preparation. The returned keyring is a
local byte snapshot, not continuous custody of its pathname. A failed write retains staging for explicit inspection; no
automatic cleanup or regeneration is safe. Read-only loading takes a shared lock,
requires a regular single-link mode0400/0600 file and no symlink in its path, and
can use the last complete final file despite unrelated staging. Loading does not
repair or establish durable publication after an incomplete preparation. Control
registration must follow successful preparation, and serve must never replace a
missing registered key. Locks are nonblocking; another preparer returns busy.

Private keyfile version1 has exact bytes: eight-byte magic `BBCKEY\0\x01`, dataset
ID16, keyring ID16, unsigned big-endian key count2, then count records of key ID16
and raw XChaCha key32, followed by SHA-25632 of every preceding byte. Count is
1..16, total122..842bytes; IDs/material are nonzero and key IDs unique. Unknown
versions, trailing bytes, wrong dataset, malformed counts or checksum mismatch
are refused without modification. The checksum detects corruption and binds
Control metadata; it is not authentication against the local credential owner.
Lifecycle and issuance decisions belong to Control, never flags in this file.
Preparation initially creates one key using OS entropy. No automatic format
upgrade is selected; future formats require an explicit compatibility path.
The loader retains a32KiB logical reservation through the key buffer's lifetime;
its owned buffer is zeroized on drop. This is not an RSS ceiling or a claim of
erasing kernel buffers, downstream copies or registers. Synchronous file I/O is
performed before runtime/Control acquisition, not inside list transactions.

Initial Control registration is an explicit deployment operation:
`access cursor-keyring register --file CONFIG`. It re-prepares only an existing
valid keyfile under the same exclusive/synchronization protocol before connecting
to Control; it never creates missing keys, adopts partial staging or interprets
inspection as proof of synchronization. The typed registration input owns that
prepared snapshot and its reservation through the actual connection-driver join.
Operators still exclude external provisioning changes; this is a snapshot, not
continuous custody of the path or proof of distribution to every worker.

Additive migration0601 appends after0814. One immutable registration per dataset
binds dataset/instance, stable keyring ID, format1, initial file fingerprint, ordered
1..16 key IDs/material fingerprints and its original installation audit. A key's
fingerprint is SHA-256 of `bigbrotr/cursor-key-material/1\0`, dataset ID16,
keyring ID16, key ID16 and raw key32. Control stores the fingerprint, never the
raw key. The file fingerprint is its validated terminal checksum: SHA-256 of
all preceding file bytes, not SHA-256 of the checksum-appended file. Individual
fingerprints remain meaningful when later rotation changes
the whole file. Existing SQL and records are not rewritten or adopted.

The protected application credential performs registration under the common
migration lock and dataset row lock, with matching installation-role attribution.
Registration, every declared key and the exact audit commit together. Identical
retry reads the original receipt without SQL writes; a changed ring, material,
order, file digest or count is a conflict. A lost COMMIT is unknown until this
same serialized retry on a fresh connection resolves it; neither regenerate
keys nor blindly report rollback. Registration history remains retained while
key compatibility/recovery can depend on it. No initial registration is an
issuing key, activation, worker readiness, authentication or decryption grant.
Current lifecycle, worker distribution, activation and rotation are separate
required consumers before exposing paged reads.

Local preparation for rotation uses `access cursor-keyring prepare-key --file
CONFIG --key-id ID`. The explicit ID is a nonzero32-character lowercase hexadecimal
identifier, not secret material. On an existing complete keyfile, a missing ID
appends one fresh OS-generated key; an existing ID preserves its original bytes.
Both cases use the same exclusive synchronized publication as preparation and
return `prepared-local`, without contacting Control. Dataset, ring ID, prior key
IDs/material and order remain unchanged. At16keys only an already present ID can
be retried; insufficient capacity, missing files, invalid IDs and partial staging
refuse without replacement or cleanup. A lost response is retried with the same
ID, never a new identifier. Prepare the key in one provisioning location and
distribute those exact protected bytes; running the same ID on separate workers
would generate different material. Readiness must compare each material binding,
not just the public ID. This prepares additional local bytes only; it cannot
prove distribution or register/activate them. Globally registered key IDs must
never be reused with new material, including after local removal; Control's
permanent material binding will independently enforce this at enrollment.
Initial `register` still resolves only its exact original keyfile; subsequent
key enrollment and lifecycle need their separate additive protocol.

Additional key enrollment is an explicit protected deployment operation:
`access cursor-keyring enroll-key --file CONFIG --key-id ID`. It synchronizes an
existing complete local keyfile and requires the selected key before connecting.
Migration0602 introduces a permanent material-binding registry; it copies initial
bindings and their original actor/audit/time under migration exclusion, without
rewriting0601 rows or inventing acceptance events. Future initial registrations
populate the same registry in their original transaction. The initial ordered
key set remains the immutable registration snapshot, while the material registry
owns the permanent namespace for initial and subsequently enrolled keys.

Enrollment serializes on the dataset row and requires its installation credential,
matching dataset/instance/ring and material fingerprint. A new binding and its
exact `cursor-key.enroll` audit commit together. The key ID identifies the retry:
an existing initial key resolves its original ring-registration audit with origin
`initial`; an additional key resolves its original per-key audit with origin
`additional`. Neither retry writes SQL or changes the original attribution/time.
Other legitimate local additions/removals/order changes do not change this one-key
identity. A conflicting material fingerprint is refused permanently, including
after key retirement or local removal. Control retains only the fingerprint.
A missing initial ring is a prerequisite failure, never implicit registration.

The finite command keeps the prepared snapshot and combined connection reservation
through the actual driver join. An unknown COMMIT requires identical retry on a
fresh connection. This operation establishes enrollment only; it cannot attest
worker distribution, readiness or activate issuance. The local file limit is16
simultaneously loaded keys; retained historical bindings are not evicted at that
limit. Point lookup by dataset/key ID bounds each retry without scanning history.
Raw SQL must preserve initial-origin provenance, exact additional-audit closure
and immutable rows; rollback leaves neither a binding nor an orphan accepted audit.

An explicitly configured keyring is also a prerequisite for `serve`, including
headless Control. Startup opens the existing file once before connecting, using
one32KiB item from the shared process budget. It never prepares or regenerates
keys. The loaded snapshot remains owned through the actual Control driver join,
including failed acquisition, cancellation and refused database validation.
No configured root preserves compatibility with runtimes predating0602; finite
Archive initialization/receipt commands do not select this cursor consumer.

The current Control incarnation checks the loaded dataset, ring and each of its
at most16individual material bindings against0602. Migration exclusion precedes
the Control fence and dataset lock. Fresh statements after locking observe prior
enrollment commits; a final server-clock check requires the actual owner's lease
still to be valid after all waits. The supervisor additionally applies its local
nonrenewable startup deadline and delivery check. Successful validation awaits
rollback before exposing an `enrolled-local-snapshot` observation. It writes no
cursor or audit history and requires no provisioning actor; acquiring Control
itself may already have committed a new owner epoch before a later refusal.

Initial registration order/count/file fingerprint do not constrain subsequent
local snapshots: any nonempty subset of enrolled material can pass this binding
check. This observation grants no activation, issuance or decryption authority
and does not prove distribution or possession of every still-required old key.
Those lifecycle checks remain required before paged reads; they extend this
actual runtime consumer rather than an unused persistent worker registry.

The first lifecycle consumer uses an explicit maintenance window. The protected
installation commands are `access cursor-keyring activate-key` and `revoke-key`,
each with `--file CONFIG --key-id ID --request-id ID --expected-revision N`.
IDs are nonzero lowercase32hex; the predecessor is canonical unsigned decimal,
including zero for no previous transition. Migration0604 appends after0815;
0603 remains reserved for scientific read pins. No old registration, enrollment,
job or audit is rewritten. Enrollment never implicitly activates a key.

An immutable lifecycle receipt records request, actor, ring, action, selected key,
expected/new revision, resulting active key and exact installation audit. The
latest receipt by dataset/revision is the head; there is no separate mutable head
table or second active flag. A per-key projection records first activation,
irreversible revocation and the maximum original issued expiry plus allowance.
The request digest is SHA-256 of `bigbrotr/cursor-key-lifecycle/1\0`, dataset16,
ring16, actor16, request16, action byte (1 activate,2 revoke), selected key16 and
expected revision as unsigned big-endian8. No whole-file fingerprint or mutable
global lifecycle revision is bound into an existing cursor's envelope.

The finite command synchronizes an existing complete local snapshot and retains
it and the connection reservation through driver join. It authenticates the
deployment/installation and resolves an identical historical request before new
quiescence, revision or local selected-key checks. A changed request conflicts;
lost COMMIT requires the same request on a fresh connection, even after Control
has restarted or another transition committed. Historical retry grants no new
authority and preserves all original rows and attribution.

New transitions take migration exclusion, the existing Control try-advisory
transaction fence, then the dataset row lock. They refuse any unreleased Control
owner, including expired owners: expiry is not process exclusion. After a crash,
a valid Control start and clean stop supplies the existing recovery path. Read
the predecessor in a fresh READ COMMITTED statement after locking. Receipt,
projection effects and exact audit commit atomically. Raw SQL must preserve this
closure, CAS, irreversible revocation and immutable receipts; multiple valid
transitions in one transaction must not compare an earlier receipt to the final
mutable projection. Activation requires enrolled matching local material, refuses
an already active or revoked target, and retains old decrypt obligations. Revoking
the active key leaves no issuer until a different key is explicitly activated;
revoking an already revoked key with a new request is a conflict.
An enrolled but never activated key can be revoked, including when absent from
the local snapshot; an unenrolled ID cannot. Activation after such revocation is
permanently refused. Rotating A→B→A preserves A's first activation and retention;
A→B→revoke A leaves B active.

The maintenance command is not a worker-distribution attestation. Each actual
cursor-selected Control startup validates every loaded material and requires the
current active key plus all unrevoked keys whose retained decrypt deadline has not
passed. Selection is bounded by the indexed live retention projection and a17th
sentinel; at most16 keys may be required/loaded, independent of permanent history
size. Existing602-only enrollment observations remain non-authorizing. A runtime
without a configured cursor root cannot issue/decrypt pages. Future independent
workers require their own actual readiness before they become eligible; online
multiworker rollout is not qualified by this maintenance protocol.

No issuer is enabled by this migration alone. Before token delivery the owning
page transaction must monotonically retain that key through the token's original
absolute expiry plus30seconds, with known COMMIT under current authority. This
uses the maximum supported allowance even when the current live setting is lower.
Until that consumer exists, direct retention writes are refused. Decreasing TTL
cannot shorten prior obligations; continued pages cannot extend original expiry.
Decryption must check authoritative revocation despite cached bytes. Tests of this
maintenance boundary do not qualify actual issuance, retirement or cross-worker
continuation; these remain required before the paged endpoint is advertised.

Use a versioned authenticated-encryption envelope for self-contained continuation,
so private scan positions are neither editable nor readable by clients. The
initial implementation candidate is full-round XChaCha20-Poly1305 through the
maintained RustCrypto library, using its nonce API and OS-backed secure randomness
for independent 192-bit nonces. Verify and pin the actual dependency at
implementation; do not implement the cipher or a reduced-round variant locally.
The [library documentation](https://docs.rs/chacha20poly1305/0.11.0/chacha20poly1305/)
describes this extended-nonce AEAD and its platform constraints; XChaCha is not
claimed to be an IETF-standardized algorithm. This is an internal token format,
not a Nostr wire extension or a claim that encryption eliminates timing leakage.

Authenticate the envelope version/key ID and deployment/resource domain. Encrypt
the semantic filter, principal/authorization scope, policy epoch, generation,
position, issue time and expiry bindings; bound encoded and decoded sizes before
allocation. A valid token still undergoes current authorization, snapshot
availability and request-binding checks. Possessing it grants no additional read
authority. Avoid secret-dependent compression and do not log complete tokens.

Envelope version 1 is canonical unpadded base64url of version byte 1, key ID16,
OS nonce24, ciphertext and detached tag16. Its encoded bound is 4096 ASCII bytes,
decoded bound 3072 bytes and plaintext bound 3015 bytes; empty plaintext is allowed
by the envelope, while each resource validates its own required fields. Unknown
versions, padding, noncanonical trailing bits, malformed lengths and failed
authentication are refused without plaintext output. No legacy format fallback
or compression is selected.

Associated data is the literal 16 bytes `BigBrotr cursor\0`, dataset ID16, keyring
ID16, version/key ID17, unsigned big-endian resource-name length2 and the resource
name. Resource names are 1..128 ASCII bytes from letters, digits, `.`, `_`, `-`, `/`;
their exact case and version are significant. The first operational consumer
uses `management.jobs/1`. Bind the stable keyring ID, not the whole file checksum:
adding a key during rotation must not invalidate tokens issued with retained keys.
This envelope supplies confidentiality/integrity only. Its caller authenticates
current access first, then validates Control's current key lifecycle and decrypted
resource-specific principal/query/policy/time/cut fields. Loading a key or opening
an envelope cannot grant authority or certify expiry/rotation by itself.

Each finite encode/decode operation retains a 16KiB logical reservation through
the returned token/plaintext lifetime, separately from the keyring's32KiB charge
and the caller's input/transport reservation. Allocate fallibly only after length
and reservation checks. The owned scratch/plaintext/token buffers are zeroized
on drop; downstream serialization and cipher-internal copies keep their own
documented bounds and erasure limitations. No unbounded input allocation or
per-token filesystem/network access is part of this codec.

Rotation switches issuance to a new key only after the serving workers can read
it. Retain an old decrypt-only key through the latest permitted expiry of tokens
it issued, including the configured clock-skew allowance. Use the common 900-second
cursor TTL by default; increasing TTL does not retroactively extend old tokens.
Emergency key revocation invalidates its outstanding cursors explicitly. Policy
revocation and expired snapshot retention still apply even while a key remains
available. An undecryptable, revoked or expired token never silently restarts at
the latest generation. Test cross-worker/restart continuation, normal and emergency
rotation, changed principal/filter, withheld scan positions and tampered envelopes.

Scientific cursor pages share the original absolute 900-second query pin; page requests
do not slide retention indefinitely. Initial admission permits at most 4096
active query pins plus the configured retained-artifact byte budget. Follow the
server-emitted cursor after byte-bound pages and scan-only progress; the token
must remain usable under its own documented continuation parameters. The exact
implementation and negative controls are in P06-T03.

Operational management lists reuse the encrypted envelope/keyring and current
authorization checks, with their own resource-specific continuation bindings.
The [job registry](../implementation/08-operations.md#p08-t03--managed-revisions-lifecycle-commands-and-durable-jobs)
uses an admission upper cut and rechecks mutable state at each page; it does not
create an Archive generation pin, `SourceCut` or scientific `ReadViewStamp` merely
to inspect redacted job metadata. Its original absolute cursor expiry never
slides on a later page. This distinction does not weaken generation/retention
requirements for scientific resources that consume actual retained inputs.
