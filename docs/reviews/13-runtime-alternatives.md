# Runtime, acquisition and analytics alternatives

Advisory design review, 2026-09-17. Baseline: `41cc381`. This report proposes
changes for consideration; it does not change selected architecture contracts.
Line references below refer to that baseline.

The most promising improvements are to reduce coordination on the delivery path,
spend acquisition bandwidth on explicitly scheduled evidence, and choose
analytical outputs whose cost matches their usefulness. Rust versus Go is a
secondary question here. None of these recommendations permits an unverified
event, invented witness, silently incomplete aggregate, or unacknowledged change
to an existing algorithm's meaning.

## Scope and evidence

Read completely: `AGENTS.md`, `PLAN.md`, acquisition-services.md,
analytics-detail.md, configuration-and-runtime.md, rust-and-repository.md,
capacity-and-benchmarks.md and planning/mandate.md. Also read the cited portions
of data-and-durability.md, storage-services.md, derived-read-publication.md,
component-contracts.md, interface-catalog.md and implementation/05-analytics.md.
Searches of other documents located related contracts; this is not a complete
repository audit. The neighboring legacy repository was neither inspected nor
modified in this review.

Ran only read-only repository inspection, official/primary-source web lookups,
arithmetic checks, and writing/checking this report. No product implementation, services,
benchmarks, stress tests, commits or pushes were run. Numerical examples below
are arithmetic or explicitly hypothetical workloads, not measurements. The
experiments described are proposals, not completed evidence.

## 1. Let the socket owner enforce network limits; use coarse host reservations

**Current choice.** Acquisition “Boundaries and deployment” and “Pooled transport
evidence and destination fairness,” lines 7–24 and 591–621, allow separately
placed capabilities but require host-local cross-process socket permits through
Control, recovery of surviving holdings, and positive exclusion before reissue.
Configuration/runtime “Shared budget allocator,” lines 46–66, also specifies
borrowable host resources and per-payload byte/item permits. These are sound
constraints; their most general implementation is unnecessarily expensive as the
default topology.

**Scenario and cost.** A monitor and synchronizer in separate processes need both
socket sharing and agreement about endpoint/auth/IP ownership. Losing the permit
owner while sockets remain open creates a second recovery protocol alongside
ingest, registry and publication ownership. It is avoidable coordination before
the product acquires another event. Meanwhile a global application semaphore
cannot precisely allocate native engine caches, kernel buffers and ClickHouse
merges. A single weighted FIFO semaphore also does not implement the required
priority policy: Tokio documents that a large `acquire_many` at the front can
block smaller requests despite sufficient permits for them. This is a risk of
that implementation, not a claim the plan already specifies it.
([Tokio Semaphore](https://docs.rs/tokio/latest/tokio/sync/struct.Semaphore.html))

**Preferred alternative.** Select one acquisition process per host/route domain
as the default socket owner. Validator, Monitor, Synchronizer, directory fetches
and publication transport remain independently enabled modules/work queues inside
it, with distinct scientific methods and lifecycle. That process already owns
socket close and can enforce endpoint/hostname/IP limits locally. Other process
roles submit bounded work to it only when split placement is useful. Do not add a
mandatory proxy daemon to the all-in-one recipe. Multi-host collectors use the
already honest host-local network quotas; a true fleet-wide limit is a separate
capability.

Within the process, use a bounded socket driver per endpoint/auth connection and
explicit subscription state machines, with CPU work handed to the shared bounded
pool. This needs ordinary Tokio tasks/channels, not an actor framework. Reserved
local control capacity cannot make a remote WebSocket skip EVENT bytes before an
EOSE; overload still needs bounded close/recovery and truthful measurement
deferral. A separately justified probe connection consumes its explicit quota.

For memory/CPU, allocate explicit process/domain ceilings at configuration or job
admission, then use bounded queues and owned buffer credits locally. Give native
analytical jobs terminable process boundaries and configure their engine limits.
Use Linux cgroups as containment/accounting, with `memory.high` as pressure and
`memory.max` as a last limit; neither is application admission or an exact
reservation. They account for substantially more than Rust's managed queues,
including page cache and TCP buffers. Redistribute optional heavy-job capacity at
job boundaries before building a continuously borrowing distributed allocator.
([Linux cgroup v2](https://cdn.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html))

Roles should describe product capabilities; processes should describe resource,
trust and failure ownership. Retain the current module/crate distinction in
rust-and-repository.md, lines 36–52. A stopped source projector need not stop its
derived-state storage adapter, as component-contracts.md, lines 136–150, already
recognizes. Co-locate cheap reducers with that adapter and send whole bounded
deltas or artifacts from heavy workers, avoiding RPC per contribution.

**Downside and counterargument.** The acquisition process becomes a larger failure
domain and cannot have each capability independently restarted without affecting
shared sessions. Genuine deployment isolation can justify the existing remote
permit contract. Coarse memory shares may leave temporary capacity unused. These
are preferable starting costs to universal host coordination, but not universally
better for every deployment.

**Confidence:** high on simpler ownership; medium on throughput benefit.
**Decisive narrow experiment:** compare the same bounded monitor+sync socket
fixture in one acquisition process and in two processes using Control permits.
Measure control calls, allocations and monitor tail latency; kill the permit
owner while sockets survive. If separate placement supplies no required isolation
benefit, keep it optional. This needs a small controlled fixture, not 100k sockets.

## 2. Make repeated verified deliveries a read path, not a new durable workflow

**Current choice.** Data/durability “Admission transaction,” lines 38–74, serializes
identity admission and maintains exact U/P inventories. Storage-services.md,
lines 30–35, already has 5 ms/4 MiB group commit and verification workers; batching
is not missing. Its receipt-reclamation contract, lines 104–151, adds per-request
digest/outcome persistence, producer incremental checkpointing, owner ACK
watermarks and causal backup closure even though ordinary repeated deliveries
ultimately return an older fact receipt.

**Scenario and cost.** In a hypothetical `D=100,000/s`, `U=1,000/s` workload with
mostly previously witnessed edges, exact membership and actual payload integrity
must still be checked. Creating a durable producer outcome for every delivery
makes operational writes follow D even when scientific changes follow much
smaller U and new/earlier P. Group commit reduces synchronization frequency; it
does not eliminate request ledger mutations, producer checkpoint work or GC.
The baseline explicitly acknowledges this cost at storage-services.md:150–151.

**Preferred alternative.** Add a bounded owner-side classification operation for
already verified deliveries. When current policy permits the exact retained body
and exact witness scope, and the observation changes neither event-level nor
edge-level first-receive facts, return the existing durable scientific receipt
without allocating a new mutating ProducerRequestId. A lost read reply is retried
and re-evaluated; unlike Admit, it need not reproduce an earlier no-effect policy
decision after policy changes. New bodies, new scopes, earlier observations and
all actual mutations continue through the existing durable Admit protocol.
Version this boundary explicitly; do not quietly remove idempotency from Admit.

The owner remains authoritative. This is not a collector claiming an ID was
verified, nor a cached ID authorizing a new relay witness. Share in-flight
verification and bounded exact-payload/signature cache entries, then perform
batched exact membership lookups. Cache state must not invent retained ownership
after suppression, retention or restore. Coverage checkpoints refer to the
existing retained receipts and the real successful delivery; EOSE still waits
for all preceding classification/admission results.

Inside one owner process, separate parallel parsing/verification/lookups from the
short final identity/policy/epoch commit boundary. If contention justifies it,
stripe preparation by event identity while retaining one sequenced commit
coordinator, rechecking conflicting keys against committed plus pending state.
“One process owns a RocksDB” does not mean all event work belongs in one serial
loop. RocksDB explicitly permits concurrent threads to share a DB, but that does
not make an application read-then-write dedupe operation atomic.
([RocksDB basic operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations))

**Downside and counterargument.** The read path adds an API branch and requires a
precise distinction between no-effect delivery classification and mutation retry.
It preserves scientific receipts but changes the operational guarantee that every
delivery has its own durable request outcome. A simple grouped Admit path may
already be fast enough; in that case keep it. Do not introduce sharded databases
merely to avoid clarifying the single-owner pipeline.

**Confidence:** medium; the likely gain depends on duplicate-edge rate and ledger
cost, not just body duplication. **Decisive narrow experiment:** fixed signed
payloads with new bodies, new witnesses, repeat witnesses, earlier observations
and invalid alternative signatures; compare current Admit against the no-effect
path. Count WAL/ledger writes per D, verification work, mutation correctness and
recovery after an ambiguous reply. Retain the simpler path unless ledger work is
a material cost.

## 3. Replace fixed overlapping linear resweeps with an explicit age-band schedule

**Current choice.** Acquisition lines 278–294 and 327–332 combine live collection,
one-hour hot sweeps every 15 minutes, 48-hour warm sweeps every hour and deeper
reverification. NIP-77 is preferred where available. The contract is already
careful about incomplete linear traversal, unseen late events and minimum
windows; the issue is the selected work policy, not its honesty.

**Scenario and cost.** Assume a steady in-scope event production rate `lambda`,
each event is retained and returned by complete linear traversals, windows use
the stated widths, all due sweeps actually run, and no shared-result elimination
occurs between layers. Warm requests return about `48h*lambda` each hour: `48`
historical deliveries per newly produced event. Hot requests return
`1h*lambda` four times an hour: another `4`. Total historical load is about
`52*lambda`; live adds `lambda`, giving about `53*lambda`. The windows overlap,
so these are repeated deliveries, not 52 distinct evidence discoveries. Inclusive
pagination overlaps, retry traffic and initial catch-up can add work.

For illustration, 5,000 such endpoints at one 2,000-byte event/s imply about
520 MB/s of historical payload and 10 MB/s live, versus a theoretical 125 MB/s
for 1 Gbit/s. This is not a measured Nostr rate or a claim all relays expose full
windows. Caps, refusals and scheduling deferrals can reduce traffic by leaving
work incomplete. Successful Negentropy radically changes this calculation:
common events need not be transferred again. NIP-77 explicitly separates set
difference discovery from subsequent body retrieval.
([NIP-77](https://github.com/nostr-protocol/nips/blob/master/77.md))

**Preferred alternative.** Retain explicit historical coverage and a configured
late-discovery policy, but schedule disjoint age bands with declared revisit
intervals. A recent band can be frequent; progressively older bands rotate under
a byte/work allocation. Coalesce overlapping requests only when the wider
attempt's completion time and recorded scope really satisfy both obligations.
Show achieved revisit age per band and known gaps. This is an explicit policy
change from “48 hours every hour,” not a claim of equivalent lateness detection.
Never learn a safe old-age cutoff solely from events already observed, and never
let high-novelty relays starve the rest of the fleet.

For NIP-77 relays, prefer pinned reusable ordered-set/range-summary access over
reconstructing a full vector on every sweep. Charge its maintenance and P-sized
dimensions; use projection plus bounded tail or a proven shared index as the
existing contract allows. The protocol permits tree-backed storage, and its
reference implementation lists BTree variants; a per-session vector is not a
protocol requirement. New witnesses still require actual verified deliveries
from that endpoint; a global body set cannot silently stand in for local witness
membership. ([Negentropy specification and implementations](https://github.com/hoytech/negentropy))

**Downside and counterargument.** Older backdated arrivals can take longer to find
under a cheaper linear schedule. If hourly 48-hour detection is the desired
product SLA, retain it and provision its measured cost. Reusable reconciliation
summaries cost storage and invalidation work. This alternative makes that choice
explicit instead of letting overload choose the effective cadence accidentally.

**Confidence:** high on amplification arithmetic; medium on the best cadence.
**Decisive narrow experiment:** one immutable signed fixture with controlled late
insertions into each age band, exposed both linearly and through NIP-77. Replay a
short virtual schedule; compare bytes, set construction cost and detection delay
by insertion age. Select a documented delay/cost frontier, not maximum throughput
alone.

## 4. Choose an exact similarity-join plan and compact evidence as the baseline

**Current choice.** Analytics “Relay overlap,” lines 271–343, selects exact size
pruning followed by pair verification, complete generation or refusal, and
persisted qualifying edges. The implementation supplement, lines 789–797,
already suggests sound joins and exact identical-set grouping. Promote those
alternatives to a concrete starting algorithm rather than treating repeated
capacity refusal as the acceptable ordinary outcome.

**Scenario and cost.** At 100,000 equal-size endpoint sets, the size test rejects
no pair: `100000*99999/2 = 4,999,950,000`. With identical eligible sets of at least
10,000 events, every pair also qualifies. Even an implausibly tiny 16-byte pair
record is about 80 GB before indexes, provenance and generation retention. No
faster intersection kernel removes that output-size problem.

**Preferred alternative.** First group identical scoped sets using a stable
fingerprint for candidates and an exact sorted-set equality check for proof.
Retain each endpoint and its distinct acquisition evidence. Then use an exact
Jaccard similarity join over representatives, with common token order, length,
prefix and positional bounds and exact final intersection. Prefix/positional
filtering is an established exact alternative to examining all size-compatible
pairs; it is not MinHash/LSH and does not introduce candidate false negatives
when its assumptions are met. The literature also warns that candidate counts
can remain poor on unfavorable distributions.
([Xiao et al., Efficient Similarity Joins for Near Duplicate Detection](https://cgi.cse.unsw.edu.au/~lxue/WWW08.pdf))

Persist the logical pair relation compactly: an exact equality class represents
its qualifying clique, and an exact qualified representative pair represents the
corresponding Cartesian endpoint pairs. A generation keeps immutable input-set
references, counts, scope and proofs. A public pair query can return the same
exact `a,b,i,u,J`; paginated enumeration lazily expands the ordered pair relation.
Worst-case export is still quadratic, and its budget must say so.

A spanning forest alone proves component connectivity and is enough for the
adjusted count, but it is **not** a replacement for the existing `relay_overlap`
pair resource (interface-catalog.md:48), exhaustive edge explanations, or the
minimum-incident-edge Jaccard (P05-T09:537–538). Preserve those through the compact
logical relation, or introduce a separately named component-only profile with
those pair capabilities explicitly absent. Never relabel witness edges as the
whole graph. Likewise, a separately useful sketch-candidate directory must keep
its recall qualification and cannot produce the exact profile's global count.

**Downside and counterargument.** Building token frequencies/order, indexes and
set generations has its own cost. Mostly distinct, dense, nearly identical sets
can still produce quadratic representative output. Expiry can split components.
Exact lazy representations complicate query planning. Small fleets may be best
served by the existing simple intersection baseline.

**Confidence:** high that the selected baseline is weak for equal-size fleets;
medium that a particular exact join wins on Nostr sets. **Decisive narrow
experiment:** compare 2,000 sets in four shapes—unrelated equal-size, identical
cliques, 0.99 boundary cases and transitive chains—with an exhaustive oracle.
Compare candidate counts, complete logical pair equality, compact bytes and
actual pair-page latency. Then scale only the discriminating shape; no broad
stress programme is required to choose the first algorithm.

## 5. Define production diffusion by an error contract, not 100 passes

**Current choice.** Analytics lines 505–511 and 600–606, plus derived/read lines
244–272, define `seeded-trust/1` by exactly 100 ordered iterations from the seed
vector and a global positive-score percentile. This is reproducible, but it
freezes one numerical procedure as the scientific output. Warm starts and early
termination necessarily become different versions, even when they improve the
approximation to the same intended stationary diffusion.

**Scenario and cost.** A pinned graph with 100 million current social edges costs
10 billion edge visits per 100-pass viewpoint, excluding graph loading and
normalization. Eight viewpoints cost 80 billion visits. These numbers are
arithmetic, not predicted seconds. Small graph changes do not remove global work,
and a percentile can change many wire values after a tiny population change.
The U/P/V/E/H/K distinction and persisted current adjacency in analytics
lines 552–605 are good choices and should remain.

**Preferred alternative.** Keep `/1` as a deterministic reference/historical
profile. Define a production profile by the stationary equation on the complete
pinned graph, with explicit numerical tolerance, reduction order/precision,
residual and failed-convergence outcome. No vertex or low-score edge is dropped.
Warm-start compatible graphs, and stop according to the stated error condition.
Begin with ordinary sparse power iteration; local residual propagation is a
later option only if its extra state earns its cost.

Here is the relevant derivation, not an observed performance result. For a fixed
normalized seed vector `t`, nonnegative normalized outgoing weights, dangling
columns replaced by `t`, and `0<alpha<1`, the complete transition matrix `M_t` is
column-stochastic. With `T(x)=(1-alpha)*t+alpha*M_t*x`, L1 contraction gives
`||x-x*||_1 <= ||T(x)-x||_1/(1-alpha)`. At alpha 0.85, residual delta gives an
absolute L1 bound of delta/0.15 in exact arithmetic. Numerical certification must
also bound floating-point residual evaluation error; a printed float residual
alone is not a rigorous bound. These assumptions match the proposed profile's
positive-mass/dangling construction, not every possible signed graph score.
The PageRank linear-system formulation and CSR/CSC iterative implementations are
described by [David Gleich's primary teaching material](https://www.cs.purdue.edu/homes/dgleich/cs515-2025/homeworks/homework-2.html).

Evaluating a fresh residual costs a full sparse multiply, O(V+E), plus the norm;
it is not free. The next power-iteration vector already supplies `T(x)` and can
be used for that stopping check. Warm-start mapping, seed changes, dangling mass
and generation loading are also charged. Convergence saves passes only when the
actual graph/residual permits it.

Keep raw mass with this error interpretation as the primary score. Make full
global percentile publication an explicit scheduled projection, not a premise of
every low-latency viewpoint request. A small L1 error does not guarantee the
ordering of near ties or every integer percentile: retain stated tie/uncertainty
handling, or compute the full declared projection. An on-demand top-k with
uncertain boundary cannot be advertised as exact global rank.

**Downside and counterargument.** Some graphs and tight tolerances need as many
or more than 100 passes; error certification and near ties add care. `/1` may
intentionally mean finite-horizon diffusion rather than stationary trust. In that
case retain it as such; do not overwrite its meaning. The recommendation is a
new profile and product default, not a claim of bit-identical acceleration.

**Confidence:** high on the clearer mathematical target; medium on performance.
**Decisive narrow experiment:** pinned directed rings, dangling-heavy graphs and
one realistic current-follow sample, with controlled edge/seed changes. Compare
100-pass `/1`, cold residual stopping and warm residual stopping against a
high-accuracy reference. Include residual evaluation, graph build/load, complete
normalization and changes in quantized outputs; keep the simpler method if total
work does not improve.

## 6. Make relay choice a declared utility query; lead with the evidence vector

**Current choice.** Analytics lines 404–459 defines
`Q=.625*A+.375*exp(-p50_seconds)`, five classified attempts, capped duration carry
and no minimum coverage gate. Its temporal evidence treatment is careful; the
weights and one-second latency constant inherit a preference without evidence
that it fits a general read client. The document itself names vector-only and
coverage-gated alternatives at lines 441–448.

**Scenario and cost.** Using the formula, a relay with A=0.8 and 20 ms latency gets
Q≈0.8676; A=0.999 and 500 ms gets Q≈0.8518. The faster relay ranks higher despite
the much worse observed availability. That may suit one task and be undesirable
for another. Five well-separated successes at the five-minute base cadence can
cover at most 50 minutes via the ten-minute carry limit—about 3.47% of a day—yet
meet the sample gate. Neither example proves the formula wrong; both show that
its “general” scalar requires a utility assumption.

**Preferred alternative.** Make the default directory a comparable vector of
observed success/failure/unknown duration, coverage, freshness, latency,
authorization and tested capabilities. Supply named selection policies that first
apply explicit requirements, then order by the relevant remaining dimension.
For example, a client can request sufficiently covered fresh evidence, a required
read method and an availability floor, then order by latency. A historical
retrieval client instead needs compatible acquisition scope and useful retained
history. A user can inspect tradeoffs or Pareto alternatives without treating an
opaque weighted sum as a universal quality fact.

Retain `general-read-relay/1` as an optional named opinion for reproducibility and
existing consumers. Simple vectors/policies still use the same incrementally
maintained attempt/boundary state; this does not require another rank service or
another copy of raw observations. If an API really needs a scalar, define its
utility, missing-data and coverage rules explicitly as a new profile. Do not
attach binomial confidence to temporally correlated probes without a sampling
model. Measured-attempt success, carried-time availability and predicted future
success remain different quantities.

**Downside and counterargument.** Vectors and predicates are less convenient than
one sortable score; badly chosen gates may leave few eligible relays. The
current score is understandable once its components are visible and could be a
useful convenience. Its main weakness is being the default general recommendation,
not the existence of the profile.

**Confidence:** high on utility mismatch; medium on which shipped policy users
will prefer. **Decisive narrow experiment:** frozen small observation histories
covering sparse success, frequent failure, stale capability and slow reliable
reads. Compare candidate selections for two explicit client tasks and the current
Q ordering; assess explained selection behavior and coverage before adding any
new statistical model. No live relay traffic is necessary.

## Choices to retain, and changes that are not justified

The plan correctly distinguishes capabilities from processes and crates; keeps
pure model/protocol/analytics boundaries; avoids a service mesh and generic
plugin framework; preserves actual verification, first witnesses and all signed
fields; distinguishes facts from claims; and keeps source-cut/generation identity
through incremental corrections. Independent target generations and whole-list
winner retractions are substantially better than rerunning or overwriting every
aggregate on each cycle. None of the alternatives needs Kafka, Flink, a graph
database or a second orchestration framework by default.

Keep Tokio, explicit bounded CPU pools and isolated native jobs. Tokio documents
that started blocking tasks generally cannot be aborted, supporting the current
process/cooperative-cancellation boundary rather than one blocking task per
event. ([Tokio spawn_blocking](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html))

Keep Rust as the intended core language unless maintainability or a real library
gap changes the decision. The repository's existing measurements show only about
20.29 MiB between tuned Rust and Go at 5,000 synthetic sessions, whereas untuned
buffer defaults differed much more (capacity-and-benchmarks.md:14–17). They do
not justify a rewrite on fleet capacity alone. A Go collector would need to earn
the added deployment/language boundary; Rust is no cure for 52-fold redelivery
or five billion output pairs.

Keep a narrow information-preserving protocol adapter and maintained event/crypto
primitives. The recorded pinned SDK finding justifies independent admission, not
a permanent conclusion that every future SDK release is unusable. Upstream
exposes separate protocol and SDK packages; evaluate any replacement against the
exact raw-envelope, guarded-dial, ordering and archival-semantics boundaries,
without reimplementing cryptography or the Negentropy protocol.
([Upstream Nostr project structure](https://github.com/nostrdevkit/nostr))

For the reference host, the coherent starting topology is one acquisition/socket
owner, one bounded ingest owner, one projection/derived-state owner, existing
database processes with explicit ceilings, and a small admitted set of isolated
heavy jobs. These can share a binary/image and collapse into one application
process where isolation adds no value. Later distribute durable ownership
partitions and whole jobs. Do not distribute individual buffer permits, reducer
contributions or graph edges merely because product services have different
names.
