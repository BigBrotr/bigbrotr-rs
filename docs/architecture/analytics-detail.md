# Analytical definitions, incremental state and ranking scale

Status: selected planning supplement, 2026-09-11; no implementation or capacity
certification. The [phase-5 reread](../reviews/07-phase-5-recheck.md) recovered
specific contracts that broad target-family names did not fully preserve.
This document fills those gaps within the existing
[Deriver, Ranker and assertion registry](derived-read-publication.md).
It introduces no additional service or mandatory storage engine.
[Protocol evolution](protocol-evolution.md) continues to own wire bindings.

## Scope, identity and exactness

Every definition below includes its semantic version, subject, source cut,
visibility domain, parser/configuration digest, time interpretation and quality
fields from the registry. Proposed names are local definition IDs, not new NIP
tags. An immutable archive total and a current-visible count are different
definitions even when their present numbers agree. Effective deletion, expiry,
replacement or public exposure changes can retract visible contributions without
removing any accepted signed event or acquisition fact from the archive.

Analytical intervals use `[start, end)` unless a named definition explicitly
states otherwise. Ordinary Nostr filter bounds remain inclusive. The historical
Go `(Start, End]` convention is not copied into an unnamed new metric. Migration
and comparison fixtures state both interval conventions and test their endpoints.
Source-cut coordinates remain ingestion positions; an analytical event-time
window never substitutes for those coordinates.

Checked integer arithmetic is required for counts and exact amount sums; overflow
fails a generation. API serialization uses decimal strings when values exceed
interoperable JSON integer precision. Ratios carry their integer numerator and
denominator. Missing evidence, an unsupported capability, an empty result and an
execution failure are different states. Scalar summaries never hide a failed or
truncated population read.

The replay unit is the immutable source record or a committed replacement
partition. A physical retry copy is not another contribution. Do not create a
second permanent per-event receipt table for every additive metric: reuse exact
admission identity, committed source chunks and partition contribution manifests
where their algebra permits it. Exact per-group distinct actors, membership
under expiry, and corrections still require their own necessary state.

## Subjects and the meaning of ranking

BigBrotr measures activity and publishes attributable opinions; it does not rank
human worth or certify the truth of a note. A pubkey is an identifier, not proof
of one person. Multiple people may share it and one person may control many keys.
Archive fidelity, query eligibility and score eligibility are separate decisions.
Low scores never remove accepted signed history or its required source evidence.

| Subject | Initial product | Additional scoring disposition |
|---|---|---|
| Pubkey | Acquired-corpus activity/interaction facts, current public relations and curator-specific seeded trust | Retain the seeded profile with explicit seeds, graph, limitations and normalization; no universal person reputation |
| Relay endpoint | Scoped operational vector: outcomes, latency, tested capabilities, coverage and freshness; a named use-case composite when sufficient evidence exists | Different use cases/vantages can define different composites; neither hosting location nor many supported NIPs is universal trust |
| Regular event, including notes | Explicit comments/replies/quotes/reposts/reaction/zap facts under versioned relation parsers | No universal event truth score in the baseline; optional trust-weighted recommendation is a separate profile |
| Addressable coordinate | The same interaction families with explicit version versus coordinate association, plus current-version identity | A recommendation must declare whether it ranks one signed version or the stable coordinate; do not combine them by accident |
| External identifier | Source-attributed comments/reactions and identifier normalization; relay URLs use the separately tested protocol interpretation | An optional relevance/recommendation profile names the source domain and threat model; rank cannot be inferred from an arbitrary identifier string |

NIP-85 subject-family support does not require inventing a rank for every subject.
The baseline publishes meaningful supported factual fields while omitting an
unsupported rank. All four subject families remain planned, but enabling a field
requires its actual registered parser and complete dependencies. A raw kind count
cannot stand in for a reaction or reply parser.

An optional event/addressable/external recommendation profile must specify which
verified interactions count, relation attribution, whether each actor contributes
once, actor trust generation, saturation/caps, time decay, missing evidence and
normalization population. It must distinguish recommendation from truth and test
self-promotion, interaction floods, brigading, compromised trusted actors and
claimed/unverified zap receipts. No default economic or report-volume weight is
introduced merely because the old implementation named one.

Use subject-keyed interaction contributions and exact actor/subject refcounts
where needed. New interactions normally dirty their referenced subjects; late
reference resolution or replaced/deleted visible interactions retract old
contributions. Maintain an actor-to-contributed-subject dependency index only if
that profile needs it and account for its size. Changing an actor-trust generation
can invalidate many previously scored subjects, and global percentile or decay
changes can broaden that work further. Pin the old actor-trust generation until
its replacement dependencies are processed; never advertise O(new events) for a
weighted score whose historical actor weights changed globally. Optional profiles
consume existing job admission and artifact mechanisms, not a new mandatory engine.

## Baseline event and social facts

| Local definition family | Value and unit | Invalidating inputs and update state |
|---|---|---|
| `archive.events.total/1` | Distinct accepted signed event IDs in the selected acquired corpus; events | New admissions add once. Corrections to observation time do not add another event. Rebuild a changed scope from its pinned archive selection. |
| `archive.events.by-kind/1` | The same count grouped by exact signed kind; events | Add the admitted event to one kind and total contribution in one committed source-chunk result. |
| `archive.author.events.by-kind/1` | Distinct accepted event IDs grouped by author and kind; events | Same admission transition; an author with no matching events is empty, not an invented zero history claim. |
| `archive.authors.distinct/1` | Distinct author pubkeys contributing selected accepted events; keys | Maintain exact set/refcount or merge exact sorted partition sets. A sum of daily distinct counts is not an all-history count. |
| `visible.events.total/1`, `visible.events.by-kind/1`, `visible.author.events.by-kind/1` | Corresponding counts under a named current/deletion/expiration/exposure policy | Maintain effective event visibility contributions. A winner change removes the prior whole contribution and adds the new eligible one; a suppressed winner does not resurrect an older event. Policy or parser changes invalidate affected partitions. |
| `social.current.follows/1`, `social.current.mutes/1` | Distinct current directed author→pubkey relations, with source list event ID | Replace the winning list as a whole; retain malformed/empty status. Keep self-edge diagnostics separately; Ranker's eligible graph removes self-follows and applies its declared mute rule. |
| `social.current.followers/1`, `social.current.following/1` | Incoming/outgoing eligible distinct graph neighbors; keys | Update only endpoints affected by changed eligible edges. Preserve the graph generation and exact eligibility rule. |

These preserve the old `global.events.total`, `global.events.kind.count` and
`social.events.kind.<kind>.count` behavior without making those legacy strings
the Rust schema. A compatibility export can map names and interval semantics
explicitly. They work with provenance disabled. Independent historical versions
are counted in archive facts; a distinct current-visible definition counts only
the selected serving population.

Posts, replies, quotes, reposts, reports, zaps, topics and activity histograms
remain separate registry definitions with the relation/parser and source
verification requirements in the
[NIP-85 contract](derived-read-publication.md#nip-85-publisher). These baseline
kind counts must not be relabeled as a specific interaction without that parser.
No publication family becomes enabled merely because a generic count exists.

## Propagation aggregates and historical evidence

The retained product is observer-relative acquisition evidence. It cannot infer
the network's causal transmission path or prove when a relay first possessed an
event. Select an event-created-time interval, source cut, allowed endpoint scope,
observer/clock context and acquisition class. Live, backfill and import evidence
remain separate strata. Imported observations use their attributed source
namespace and verification status; importing them now does not create historical
local network observations.

Within one eligible stratum, reduce immutable first-observation facts and earlier
corrections **after** applying the source cut. For each event `e`, let `S(e)` be
its observed distinct eligible endpoints, `k(e)=|S(e)|`, and `t(e,r)` the earliest
eligible observation at endpoint `r`. For `k(e)>0`, define:

`spread(e) = max(t(e,r)) - min(t(e,r))` over `r` in `S(e)`.

Store durations in integer milliseconds and preserve clock-quality metadata.
Clock uncertainty remains visible; even a nonnegative computed spread does not
establish causal order. One endpoint gives spread zero, meaning only one observed
endpoint. An event without eligible provenance does not enter an invented
zero-relay bucket. Expose the count of selected archive events lacking that
evidence separately, where the caller may see that population.

| Definition | Retained output |
|---|---|
| `propagation.relay-count-distribution/1` | For each exact `k>=1`, distinct event count; events per observed endpoint-count bucket |
| `propagation.spread-by-relay-count/1` | Per bucket: event count and spread p50, p95 and maximum in ms, exactness/estimator and observation scope |
| `propagation.summary/1` | Distinct eligible events, single-endpoint events, maximum observed spread, evidence-excluded event count when authorized |
| `propagation.event/1` | Bounded event-ID drilldown: endpoints, original observations, corrections effective at the selected cut, ordering ties and clock/access/path labels |
| `availability.event-history/1` | Bounded event-ID and endpoint/scope/time query returning direct served/not-served/unknown observations; optional named interval estimator with its assumptions |

Exact p50/p95 use nearest rank: sorted values `x[1..n]`, percentile `p` returns
`x[ceil(p*n)]`. Maximum is exact over the included values. Empty buckets have no
quantile. Approximate quantiles are a separately named optional KLL binding with
recorded parameters, serialization version and rank-error semantics; they cannot
silently replace exact output. No KLL-enabled capability is advertised until its
implementation and merge/rebuild behavior are validated. The initial exact job
spills/partitions or reports resource exhaustion when necessary.

Incremental ownership is explicit:

1. New endpoint membership or an earlier first-observation correction dirties
   that event/stratum. Read its prior committed contribution and recompute its
   endpoint count, minimum and maximum from the affected exact membership state.
2. Retract the old event contribution and add the replacement in one committed
   target generation. A bucket change affects both old and new buckets. A
   correction that preserves `k` can still change spread. Later duplicate
   observations with no new fact cause no durable analytical work.
3. Exact counts can update by signed deltas. Quantile buckets and a removed
   maximum use retained exact spread values or rebuild affected immutable bucket
   partitions; a median, maximum or ordinary sketch is not generally invertible.
4. Rolling-window expiry, source corrections, scope/visibility changes and a
   parser revision dirty their actual event/time partitions. Unchanged partition
   artifacts are shared by manifests. Retain the prior complete generation while
   an expensive bucket rebuild runs.

State for this surface includes necessary per-event summaries and selected
membership access, plus bucket contributions. It is not a permanent copy of all
delivery attempts. A request for global distributions reads committed aggregates;
per-event drilldown requires an event ID and the common query budget. An event
observed on very many endpoints can require pagination or an asynchronous job.

Historical acquisition observations prove existence of an observation at its
recorded time. They do not prove continuous availability between observations.
Neither a relay's oldest observed event nor retention declared today establishes
a historical start/stop-serving time. The API returns the evidence timeline and
unknown intervals; any carry-forward estimator names its finite horizon and
never manufactures direct negative evidence. Source observation history is
retained independently of disposable derived-run generations.

## Selective availability

The version-1 requested set is the distinct event IDs for which a direct bounded
fetch/probe has a finalized local attempt outcome in the same endpoint,
authorization, method and declared observation interval. Finalization includes
success, completed empty response, explicit rejection, deadline expiry or a
recorded transport failure; it does not claim the remote query completed.
A relay advertising an ID and then completing a matching empty fetch can support
scoped not-served evidence. An unrelated peer's copy alone cannot. Timeouts,
disconnected or unfinished queries, rate limits and unresolved authentication
remain unknown. Explicit policy rejection is a separately named outcome, not
proof of intent.

For a per-ID interval summary, use the most recent finalized local attempt in
that interval, with attempt identity and timing retained. Conflicting outcomes at
the same latest measurement time remain contested/unknown rather than choosing
the favorable one. An earlier success followed by an inconclusive later attempt
remains in history; it is not a fresh success. Raw attempt counts and distinct-ID
summary counts are separate quantities, so retries cannot change a numerator just
by being more numerous.

Let `S`, `N` and `U` be disjoint distinct-ID sets classified served, explicitly
not-served and unknown. Report all three cardinalities and
`not_served_fraction = |N| / (|S| + |N|)` only when the classified denominator is
at least 20. Also report `classified_fraction=(|S|+|N|)/(|S|+|N|+|U|)` when any
outcome exists. Below 20, the first ratio is undefined; 20 is an initial display
eligibility policy, not a confidence guarantee. Do not subtract unprobed peer
events into these sets or claim this fraction represents the relay's whole corpus.
Overlapping requested filters deduplicate before the per-ID classification.

Update the affected endpoint/context/event's prior contribution on new attempts,
corrections and interval expiry. A rolling summary requires exact expiry state
or affected-bucket rebuild. There is no censorship penalty in the initial relay
quality profile. A future penalty needs a new version and direct classified
evidence; unknown acquisition coverage cannot lower a relay's score.

## Relay overlap and the adjusted endpoint count

Preserve the default 24-hour analysis interval, Jaccard threshold 0.99 and at
least 10,000 common distinct event IDs. Both endpoint sets use the **same** event
interval, source visibility domain, access context and compatible coverage basis.
Record whether the interval describes event timestamps or observations; the
initial overlap profile uses event-created time with separately declared traversal
evidence. Do not compare pair-specific time slices and then reuse a size bound
proved for a single common universe.

For eligible endpoint sets `A` and `B`, retain `a=|A|`, `b=|B|`, `i=|A∩B|`,
`u=a+b-i`, and `J=i/u`. Qualification is `i>=10000` and `J>=0.99`, with exact
integer threshold comparison where possible. Candidate pruning by
`min(a,b)/max(a,b)<0.99` is sound for those fixed sets. Requiring full traversal
coverage is an explicitly narrower eligibility policy: it can omit identical
partial mirrors and is not a lossless mathematical pruning rule.

The initial exact profile uses the declared coverage eligibility and sound size
pruning, then exact verification of its candidates within the shared work budget.
It completes that eligible scope or leaves the generation unavailable; it never
keeps a prefix of pairs as an exhaustive result. A sampled/sketch candidate method
is an optional distinct profile, labeled
approximate with its configuration and evaluated recall; exact verification does
not make the fleet-wide candidate search exhaustive. A requested exhaustive job
either completes within its declared scope or refuses. Candidate discovery
failure or a work limit is never silently reported as an exhaustive empty graph.
Persist candidates, qualifying edges and components with the same dependency
manifest. A connected component means a path of qualifying edges, not all-pairs
similarity, common hardware, shared ownership or interchangeable future behavior.
Derive its stable component identifier from the definition, scope/window and
sorted member endpoint identities, independently of execution run ID. Membership
rows retain that identifier and evidence window. If exposing the historical
per-member score, label it as that member's minimum incident qualifying-edge
Jaccard; it is not a component-wide minimum pair similarity. The exact edge set
and generation remain the authoritative explanation.

For a selected covered endpoint set `C`, and a compatible committed partition
into qualifying-edge components `K`, define:

`deduction = sum(max(|K_j ∩ C| - 1, 0))`

`overlap_adjusted_endpoints = |C| - deduction`.

Use only members of `C`; a cluster member outside the coverage population cannot
reduce its count. Preserve `observed_endpoints=|C|`, deduction, adjusted count,
reference scope and exact component generation. This is an analysis-defined
count, **not** a true independent-relay census or a guaranteed upper/lower bound
on real servers. Threshold false positives and transitive components invalidate
that stronger historical wording.

| Basis state | Meaning |
|---|---|
| `exhaustive-complete` | All pairs required by the declared eligible scope were decided, including the valid no-edge case |
| `candidate-complete` | All pairs from the declared non-exhaustive candidate plan were decided; coverage/recall limitation remains |
| `suppressed` | Capability or policy intentionally disables analysis |
| `failed` / `not-built` | No usable generation for the requested dependency; expose the failure or missing work |

For the last two cases, report the observed count and omit the adjusted value;
do not borrow an older incompatible cluster set or collapse failure into an
apparently measured zero deduction. A caller may deliberately select an older
compatible complete generation and see its age. Candidate exclusions have
aggregate reason counts and bounded examples; retaining every rejected pair
would itself create an avoidable quadratic dataset.

Membership changes dirty affected candidates/pairs; high event fanout `f` can
make naive pair updates O(f²). Sound pruning or a disclosed candidate method is
required before scheduling that work. Edge insertion can merge components;
expiry/removal can split them and requires recomputation or a validated dynamic
method. The baseline recomputes components from the committed qualifying edges.
The adjusted summary then recomputes from the compatible coverage membership.
No overlap output changes endpoint enrollment or removes archived data. Mirrors
retain their own positive serving evidence and any separately enabled coverage
bonus; the count deduction is not a per-relay quality penalty.

## Third-party claims and local observation precedence

This target works without event-relay provenance because it consumes measurement
records and signed monitoring claims. A claim key includes relay subject,
attribute definition/version, method/access/vantage scope, author/observer,
original source identity and effective measurement time. Preserve raw source
events and conflicting immutable records. Neither `(relay,time,pubkey)` nor a
versionless replacing row is a sufficient identity for destructive collapse.

For each categorical attribute in a named interval, keep unique report count,
distinct contributing key count, each normalized value's contributing key set,
measurement times and source evidence. A key that never supplied this attribute
is not a dissenting vote. One key polling frequently increases reports without
increasing distinct-key support. Known shared operator membership is separate
metadata; several keys are not proof of independent witnesses.

Two complementary views avoid confusing a genuine change over time with dispute:

* Historical value support reports each value's distinct key support and time
  range. If all contributors observed the same transition, that alone is not
  disagreement. Keep the timelines rather than claiming simultaneous conflict.
* A current claim view takes each observer's latest eligible measurement for the
  same context. Different values among those contributions are contested. A
  same-observer/same-time contradictory source is itself contested and cannot
  supply an uncontested vote. Missing or stale observations are labeled, not
  converted into a contrary value.

The local viewpoint first uses the selected local observer's newest eligible
measurement for that attribute/context, independent of third-party support.
Forty-nine strangers agreeing with an older local value cannot make it the latest
local observation. Multiple local vantage points remain separate unless an
explicit aggregate viewpoint defines their reduction. At a same-time local tie,
return contested alternatives and no scalar resolved value.

Without an eligible local measurement, the optional third-party summary selects
the value with most distinct eligible reporting keys. It remains an attributed
claim, labeled single-key or corroborated-by-keys, with full competing support.
A top-support tie returns contested alternatives without arbitrary lexical
resolution. Even a unique top value is marked contested when other eligible keys
disagree. Age is relative to the pinned observation-window end, while current
read age is separate metadata. Freshness never silently changes votes or weights.
Use per-attribute validity policy; default eligible history is the last 24 hours,
matching the initial monitor summary interval, with original measurement times
required for a fresh-measurement claim.

RTT and geography measured from different paths are not categorical votes about
one common truth. Retain their scopes and use duplicate-safe per-observer
continuous summaries where defined. Declared capabilities and tested capabilities
are different attributes; a targeted auth requirement need not contradict a
general NIP-11 declaration. The initial relay Ranker consumes qualified local
measurements, not an uncorroborated or contested crowd value recast as measurement.

Keep exact contributing identities under authorized access and page them. Public
previews may include at most 64 permitted example keys plus the exact allowed
support count and a truncation marker. Resolution and local-membership checks
operate on the full state, never the preview. Arrival, correction, expiry and a
parser/scope revision dirty only the affected attribute/context and dependencies;
latest-observer state and value support must retract superseded contributions.

## Seed availability, optional eligibility and curation

Ranker's eligible public graph and raw seeded-diffusion formula remain those in
the [Ranker contract](derived-read-publication.md#ranker). Freeze graph membership
before checking seeds. Target-only and dangling pubkeys are vertices; a configured
seed absent from every graph vertex is missing, not an invented active vertex.
Seed configuration accepts unique pubkeys with finite positive weights; duplicate
entries or invalid weights are configuration errors rather than implicit extra
votes.

| Condition | Selected behavior |
|---|---|
| No configured seeds | Complete withheld outcome `no-seeds`; facts remain usable |
| Nonempty configured seeds, none in the graph | Complete withheld outcome `seeds-absent-from-graph`; do not run unseeded PageRank |
| Some configured seeds present | Renormalize the present seeds' configured weights; record configured/present/missing counts, digests and present/configured weight fraction |
| Seed input unavailable or invalid | Configuration/dependency failure; never relabel as a healthy empty seed set |
| No relay subjects satisfy the independent measurement profile | Complete relay-half empty/withheld outcome with its own reason; social success is unaffected |

The present-weight fraction is source-graph coverage, not seed trustworthiness.
With one usable anchor, that anchor receives all teleport mass. Surface that
fact explicitly. Distinct curator/viewpoint profiles retain separate generation
and publication identity; neither compromised seeds nor sybil-resistant editorial
selection is guaranteed by the diffusion formula.

For the deterministic reference, initialize `r0=t`, the usable normalized seed
vector. Each iteration computes `r_next=(1-alpha)*t + alpha*(incoming(r) +
dangling_mass(r)*t)`, where an eligible source distributes its current mass
equally over its distinct outgoing neighbors. Use alpha 0.85 and exactly 100
iterations, with a stable vertex/neighbor order and declared numeric precision.
Validate finite values and report mass drift and residual; no discarded low-score
vertices or convergence-dependent early stop is part of this reference version.

Retain an optional `publication.min_indegree` eligibility rule, default 0
(disabled). It counts distinct eligible incoming neighbors in the complete pinned
graph. Apply it after raw diffusion and before computing the proposed percentile
among eligible positive-score subjects. Record floor, admitted/withheld counts
and normalization population. Raw scores and the graph remain unchanged and
authorized explanation resources can show withheld reasons. Changing this rule
changes the assertion profile's recorded generation and, where it defines a
distinct algorithm/viewpoint, its NIP-85 signing identity. It never deletes or
stops acquisition of low-degree authors. Incoming sybil edges can satisfy a raw
indegree floor, so this rule is a publication policy, not another trust guarantee.

The historical inverse-PageRank-assisted seed discovery is retained as optional
curation assistance: an on-demand analysis can order candidates from a pinned
reversed-follow graph, alongside source/coverage and sensitivity diagnostics.
It does not automatically install seeds, certify people or run on every cycle.
Any such diagnostic gets its own definition/parameters and compute admission;
no separate mandatory engine or service is introduced. The initial shipped seed
set is empty. Operators supply and review their editorial anchors, source,
rationale and revision through the existing configuration workflow. The old
unapproved relay novel-rate proposal is not a ranker or acquisition default:
changing collection priorities from novelty would require a separately documented
coverage policy and starvation evidence.

| Additional setting | Initial default and unit | Activation |
|---|---|---|
| Analytical interval convention | `[start,end)` | Definition version; does not change Nostr filter bounds |
| Propagation quantiles | Exact nearest-rank; optional approximate binding disabled | Generation and serialization definition |
| Selective availability ratio minimum | 20 classified distinct IDs | Generation; display eligibility, not confidence |
| Claim current-history window | 86,400 seconds; attribute-specific override explicit | Generation |
| Public contributor examples | Maximum 64 permitted keys with truncation metadata | Read policy; full count and resolution independent |
| Overlap execution | Exact within declared eligible scope and common job budget; optional sketch profile disabled | Generation; no partial-complete outcome |
| `publication.min_indegree` | 0 incoming distinct graph neighbors; disabled | Generation/profile identity as applicable |
| Curation assistance / recommendation scores | On-demand optional definitions, disabled in default scheduled profiles | Definition and resource admission required |

Use the existing Deriver/Ranker memory, spill, duration and scheduling controls;
these settings do not allocate an independent resource pool. An operator changing
interval/parser/score meaning creates new recorded output, not an in-place edit
to historical assertions.

## Graph state is different from provenance state

Use separate symbols in capacity records:

* `U`: distinct retained signed events; `P`: admitted event/endpoint/observer/access
  provenance facts under the selected identity model; repeated deliveries are `D`.
* `V`: vertices and `E`: eligible **current social** edges in a pinned graph;
  `H`: historical follow/mute tag records read when reconstructing that graph.
* `K`: concurrently retained viewpoint vectors, `I`: diffusion iterations, and
  `ΔE`: changed current edges. None of these is interchangeable with `P`.

A straightforward CSR graph with 64-bit offsets costs roughly `8*(V+1)` bytes
for offsets plus `w*E` for neighbor ordinals, where `w` is 4 or 8 only when its
validated ordinal range permits. Add vertex-ID mapping, weights if needed,
reverse adjacency if retained, alignment, indexes and artifacts. Two float64
iteration vectors alone cost `16*V` bytes per active viewpoint; retaining one
final raw vector costs `8*V` before IDs, rank projections and explanations.
These are layout arithmetic, not measured resident or compressed file sizes.

At 100 million graph vertices, two float64 vectors alone occupy 1.6 GB (decimal),
already exceeding the proposed 1 GiB Ranker resident ceiling. At 100 billion
**social** edges, a 4-byte neighbor array alone would occupy 400 GB, before the
other structures. A workload with 100 billion provenance facts need not have
100 billion social edges; measure both. If those social edges did exist, 100 full
iterations would visit roughly 10 trillion edges and read at least 40 TB of a
400 GB neighbor array across uncached passes, excluding vector traffic and every
other cost. RAM caching can reduce physical I/O but cannot eliminate edge work.

The 1 GiB resident/16 GiB spill proposal is a starting admission envelope for
jobs that fit, not a promise that this graph fits. Larger deployments must assign
measured sufficient memory/spill/storage or use separately validated partitioned
execution. A graph that cannot fit the admitted correct plan fails without a new
complete score generation; facts and prior explicitly aged scores continue.
No extra graph engine is mandated by these calculations.

Maintain current adjacency incrementally from changed whole-list winners; avoid
rereading all `H` historical p-tags for every rank cycle. A first build, parser
change or lost adjacency artifact can still require that reconstruction. Perform
canonical neighbor sorting/duplicate removal during graph construction, then
traverse the persisted order during iterations; avoid sorting or allocating a
fresh neighbor list per vertex per iteration. Late old winners and a single
ancient still-current list are included in the source plan.

`ΔE` can be small while exact diffusion and percentile normalization remain
global. The reference costs O(I*(V+E)); initial I=100, damping 0.85, with residual
reported. Warm starts, local residual propagation, convergence stopping or sparse
personalization remain separately defined optimized variants, compared against
the reference. Admit K explicitly; sharing adjacency does not make K vectors or
their computation free. Measure building/loading, all iterations, normalization,
artifact writes and publication differences, not only a warm numerical kernel.

## Required discriminating validation

All checks below are future Rust implementation evidence through real service
entry points and selected real engines. The historical Go/ClickHouse measurements
in the reread justify these workload shapes, not future pass results.

| Workload or failure | Required observable result |
|---|---|
| Quiet incremental window, late losing list, newer whole-list replacement and malformed new winner | Whole current snapshot survives; only affected contributions change; deleted/malformed winners do not resurrect earlier lists |
| New edge followed by strictly earlier correction at a later source cut | Propagation can change bucket/spread exactly once; a pinned earlier answer remains unchanged after merges/replay |
| Asymmetric propagation buckets, unknown evidence, repeated probes and overlapping filters | Counts and p50/p95/max match an independent exact small reference; no retry inflation or fake zero-relay/censorship result |
| Local recent value versus many imported supporters of an older local value; unanimous time transition; same-time conflicts | Local precedence, per-attribute voting and timeline/current distinctions match the selected policy; no arbitrary conflict winner |
| Same monitor contribution replicated across more observers, reordered source delivery and duplicate physical rows | No popularity-derived quality gain; stable declared precision and labels; deterministic ordered accumulation where required |
| Identical partial mirrors, one component member outside C, no qualifying edge, candidate refusal and expired bridge edge | Eligibility and candidate coverage are disclosed; adjusted count uses only C; valid empty differs from failure; removal can split components |
| Exact distinct at small and much larger changed workloads, many groups, forced spill and full temporary disk | Record retained state and scan bytes; exact answer or explicit refusal, never row-limit truncation; periodic rebuild agrees with incremental state |
| 2,000 authors × 25 historical revisions, ordinary low churn and high-churn backfill | Compare winner-restricted body reads, scan bytes, materialization memory and output correctness; no assumption that fewer result rows mean less scan work |
| Current p-tag extraction with all winners recent, then one ancient current list, under few and many parts | Measure loss of time pruning, H versus E, graph build/load cost and the incremental adjacency alternative |
| Same logical facts across compact and fragmented part layouts | Measure in-order/grouped plan memory, rows, bytes and timing spread; reject a universal setting justified by one favorable layout |
| Empty, absent and partially present seeds; target-only vertices; distinct curators; indegree gate on/off | Exact withheld reason and weight fraction; full raw graph unchanged by publication eligibility; normalized population and identities explicit |
| Disconnected rings, trusted entrance to a directed ring, trusted one-hop target, increasing seed outdegree, compromised seed | Compare exact reference, total/maximum/mean attack mass and rank projection; no unproved universal seed-poisoning or per-node 1/N guarantee |
| Packaged Ranker → persisted manifest decode → actual NIP-85 publisher | Real emitted algorithm/subject/units accepted or explicitly unsupported; no hand-written retired algorithm fixture standing in for the producer |
| U in billions, P up to 100 billion, independently varied V/E/H/K, skewed fanout and low/high ΔE | Report complete disk/RAM/I/O budgets and time to recover/rebuild under mixed ingestion, read, export and maintenance load |

The directed-ring case needs its topology stated. For a single seed→entry
directed ring with damping 0.85, stationary total non-seed ring mass can remain
near 0.85 while the mean falls with ring size and the entry's maximum does not.
If an additional trusted hop precedes the ring, mass near 0.85² is a different
topology. Record the actual edges and inspect all three statistics; never turn
one fixture's arithmetic into a universal attack bound. Finite-iteration results
also depend on initialization and graph size, so compare the specified iteration
contract rather than claiming exact stationary values without a convergence basis.

For every workload preserve fixture manifest, engine/library versions, current
and historical graph sizes, changed-key count, part counts, source/target cuts,
correctness digest, CPU/wall time, read/write/temporary bytes, RSS and cancellation
behavior. Force physical retry duplicates when testing that boundary and verify
that the fixture really contains them. Test spill/refusal and cancellation on a
real engine; a fake that always returns fewer rows cannot prove those outcomes.
