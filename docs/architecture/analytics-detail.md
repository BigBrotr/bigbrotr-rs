# Analytical definitions, incremental state and ranking scale

Status: selected planning supplement, 2026-09-11; no implementation or capacity
certification. The [phase-5 reread](../reviews/07-phase-5-recheck.md) recovered
specific contracts that broad target-family names did not fully preserve.
This document fills those gaps within the existing
[Deriver, Ranker and assertion registry](derived-read-publication.md).
It introduces no additional service or mandatory storage engine.
[Protocol evolution](protocol-evolution.md) owns compatibility and publication
identity; [assertion bindings](../specification/assertion-bindings.md) owns exact
producer, relation, subject and field mappings.

## Scope, identity and exactness

Every definition below includes its semantic version, subject, source cut,
visibility domain, parser/configuration digest, time interpretation and quality
fields from the registry. Proposed names are local definition IDs, not new NIP
tags. An immutable archive total and a current-visible count are different
definitions even when their present numbers agree. Effective deletion, expiry,
replacement or public exposure changes can retract visible contributions without
removing any accepted signed event or acquisition fact from the archive.

For time-sensitive definitions, source cut plus completed generation is
insufficient: bind the evaluation/window basis, `visibility_as_of_us`,
`temporal_generation` and `expiry_deadline_us` from the
[read-view contract](../specification/read-view-contract.md). Ordinary current
reads require compatible target temporal identity and refuse after its deadline
until replacement facts exist. Pausing Deriver cannot relabel old memberships
or counts with a fresh current stamp. Only explicitly dated analysis resources
may serve the old interpretation as stale; archived originals remain retained.

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

The [storage catalog](../specification/storage-catalog.md#shared-derived-state-and-reconciliation-directory)
selects a separate Indexer-owned RocksDB directory for that mutable derived
state. Target/scope/generation-qualified keys prevent working checkpoints from
rewriting a pinned generation. An atomic synchronized batch binds state changes,
checkpoint and pending output identity; complete CH materialization is verified
before the fenced PG generation pointer changes. PG records target authority and
published manifests, not a second dirty-state commit authority. Deriver/Ranker
own algorithms and submit bounded typed mutations through the storage owner.
Recovery replays the same pending output or its pinned inputs without loss or
double contribution. Retain pins and account for WAL, compaction, checkpoints,
staged CH output and artifacts together.

Migration 0501 supplies shared `artifacts`/`artifact_files` inventory for the first
graph consumer; `graph_artifacts` is its typed graph binding. Later 0804 adds
publication metadata, without making P05 depend on P08 or owning the files again.

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
use the exact versioned definitions and producer mappings in
[assertion bindings](../specification/assertion-bindings.md). In particular,
received reactions differ from authored reactions, comment-root/parent unions
differ from direct replies, and payment identities differ from receipt IDs.
P05-T02 implements T04's shared pure-parser contract before T03 activity or T04 aggregation;
neither aggregate depends cyclically on the other's completed generation.
Materialize per-source contributions, exact actor refcounts, unresolved-target
reverse dependencies and family-specific publication support as specified there.
These baseline
kind counts must not be relabeled as a specific interaction without that parser.
No publication family becomes enabled merely because a generic count exists.

### Explicit active-hours and daily zap summaries

The source-to-wire definitions are now fixed in
[P05-T03/T05](../implementation/05-analytics.md) and
[P07-T05](../implementation/07-publication.md). Activity uses the previous 30
completed UTC days and an exact 24-bin eligible post/reply histogram. With at
least 20 samples, choose the shortest non-wrapping whole-hour interval containing
at least 80% of mass; ties select the earliest start. Start is 0–23 and exclusive
end 1–24. Publish the histogram, sample count/window/coverage and method beside
this heuristic; it is neither a timezone nor a network-wide activity claim.

Daily zap amount uses accepted exact msats over the same 30 completed calendar
days, denominator 30 regardless of active days. Standard integer satoshi totals
require divisibility by 1,000; integer sats/day requires divisibility by 30,000.
Otherwise omit that optional standard field while retaining exact local numerator
and denominator. Missing evidence remains missing, not a zero global claim.
The pinned [NIP-85](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/85.md)
defines the field forms; these aggregation heuristics are BigBrotr's explicit
selected interpretation. Extreme signed timestamps stay intact in A01; select
eligible window bounds numerically before any calendar conversion, and report
unsupported calendar projections instead of overflowing or altering the event.

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

## Relay content composition from a declared population

`relay.composition/1` answers what the selected evidence contains. It does not
answer how many events a remote relay currently stores. Reuse Deriver's coverage
target, typed `relay_facts`/`relay_coverage` rows and the shared derived-state
owner; no additional daemon, graph engine or permanent delivery log is selected.
P05-T07 owns witnessed composition and T08 owns the existing probe-set projection.

Every result fixes endpoint, local observer, access and visibility domain,
acquisition class, SourceCut, event-created interval `[start,end)` or explicit
all-history selection, definition/parser versions and the following population.
Authorization applies to both bodies and supporting evidence before membership.

| Population ID | Exact membership and allowed statement |
|---|---|
| `witness-history/1` | Distinct verified event IDs with a qualifying direct local endpoint witness admitted through the cut. Earliest-witness corrections change timing, not cardinality. "Acquired here from this endpoint within the declared corpus." Remote removal does not retract this history. |
| `visible-witness/1` | The preceding set intersected with BigBrotr's normal-visible event set at the compatible ReadViewStamp. "Locally visible subset of acquired endpoint evidence." Resolve global authorized winners before intersection; a newer version witnessed elsewhere may suppress an older one here. This is not the relay's current-visible set. |
| `probe-served-set/1` | Distinct requested IDs classified served by the existing availability job's latest finalized direct attempts, using its pinned selection, interval and context. "Served among these specifically requested IDs." S/N/U/unattempted counts and selection bias remain alongside composition; an older success followed by unknown does not remain a fresh positive. |

Ordinary public composition selects `visible-witness/1`. Historical witness
composition requires its explicit historical/provenance aggregate grant, and
probe-set composition requires authorization for that selection and its evidence.
An endpoint being public does not make every historical body or access context
public. Restrictive policy invalidates incompatible generations and publication
through the existing authority barrier; a small aggregate is not an exemption.

Imported signed monitor events and sidecar presence claims can describe a separate
attributed-claim population, never enter these direct-witness definitions, and
never cause local probe traffic. Provenance disabled from creation suppresses
these populations; existing permitted history after disablement stays historical
with its collection gaps. A first-witness timestamp inside a recent window means
newly discovered membership, not all IDs served during that window. Ordinary
repeat deliveries are intentionally not retained, so they cannot reconstruct
recent full membership or last-served time. A later traversal-sample definition
would require an explicitly retained, bounded exact membership artifact and
selection/coverage contract before activation; a completed traversal counter or
EOSE alone cannot supply it. Remote COUNT remains a separately attributed relay
response, including its filter/time/approximation status, never local exact truth.

Let `E_r` be the chosen event set, `N=|E_r|`, `A` its distinct signed author keys,
and `n_a` the number of events in `E_r` authored by `a`. These definitions are
exact within `E_r`; unknown remote coverage does not make this local count an
estimate of remote inventory.

| Registered component | Formula/unit and missing rule |
|---|---|
| `events`, `events-by-kind`, `authors` | N events, exact per-kind counts, and `|A|` keys. Kind buckets partition N; distinct-author counts across buckets do not add. |
| `content-bytes` | Sum of UTF-8 byte lengths of signed content strings, once per event; bytes of content, not relay disk, compressed archive or full wire size. |
| `signed-time-range` | Minimum/maximum signed event timestamps when N>0. This is observed content chronology, not relay retention age or first-ever publication. |
| `topics` | Distinct literal nonempty signed `t` strings per event under the existing topic parser. One event can contribute to multiple topic buckets; their sum need not equal N. Counts are author-supplied associations, not inferred language or adjudicated themes. |
| `author-concentration` | Retain checked integer `sum_author_squares=Σ n_a²` and denominator N²; concentration is their ratio for N>0. Effective author-key diversity is N²/Σ n_a². Neither counts independent people nor proves resistance to many keys controlled by one actor. |
| `top-10-author-share` | Sum of the largest min(10,|A|) author event counts divided by N, with deterministic pubkey tie ordering for explanations. Undefined for N=0. Public top-key identities require their own allowed projection; the aggregate does not authorize disclosure. |

Empty completed membership yields zero counts and undefined ratios/ranges;
missing capability, incomplete input and failed reduction remain unavailable.
Store numerator/denominator losslessly and check the wider square/sum arithmetic;
overflow fails that component/generation. Paging top kinds/topics is a projection
over the complete exact state, not permission to keep only early source rows.
Signed type/time/topic breadth is useful without automatic content classification,
private decryption or fetching referenced media. New semantic/NLP classifications
need a new definition with model/version, validation and resource cost.

### Materialize only the selected useful views

The complete online recipe selects one configured public `visible-witness/1`
all-acquired-history scope per collection stratum when provenance is enabled.
The existing target selection can disable it. Do not eagerly materialize every
population × observer × credential × kind × window × curator combination.

| Work placement | Initial selection and scheduling |
|---|---|
| Precomputed CH projection | Dashboard/directory cards and repeated client comparisons use `events`, exact per-kind counts, `authors`, `content-bytes`, `signed-time-range` and `author-concentration`. These reuse exact membership/author refcounts and small sufficient statistics rather than a new scan per display. The existing 86,400-second facts cadence drains changed endpoint partitions under shared budgets, with source/computation age visible. |
| Bounded on-demand reads | Client drill-down and scoped research requests use top-ten share/explanations and supported alternate windows/populations over indexed retained memberships/contributions, only when the common read budget admits the complete computation. Historical populations require their grants. A small returned top ten is not assumed to be a cheap scan. |
| Admitted analytical jobs/artifacts | Research comparisons and exports use full topic distributions, broad historical/comparison windows, probe-set composition and curator bands whose complete work exceeds a finite read. They use the existing Deriver/job/artifact/read path, not a public arbitrary-SQL endpoint. Operators may explicitly schedule a measured useful profile and reuse its immutable result. No new schedule is implicit in a request. |

Topic and curator-band materialization is disabled by default; no topic-state or
author→endpoint reverse index is allocated solely for a disabled optional profile.
Required concentration state is the same endpoint/author refcount already needed
for exact distinct authors, plus a square sum. Minimum/maximum retraction still
needs admitted ordered state or a partition rebuild. Snapshot artifacts and
authorized corpus exports let researchers evaluate new definitions offline before
promoting them into scheduled CH projections. Publication binds only an actually
complete compatible result; a research notebook does not auto-register a field.

The daily cadence does not extend temporal validity. Expiry/visibility deadlines
enqueue their affected current partitions independently; an overdue generation
is explicitly dated analysis or unavailable for a current-dependent read until
its reducer catches up. Track achieved refresh, dirty age and exact bytes/work.
If this default is too stale or costly for the measured user journey, tune the
existing target cadence or select on-demand scope explicitly; do not claim an
untested freshness SLA or silently lower exactness.

Optional `relay.composition.curator-bands/1` joins a **public**, pinned T11 score
generation to these author keys. Use bands `0`, `1–25`, `26–50`, `51–75`, `76–100`
of that generation's recorded integer rank; report both distinct authors and
event counts per band, plus explicit unknown-author/event buckets. Known zero
belongs to band zero; absent, stale or withheld score is unknown. Publish known
author fraction `|K|/|A|` and known-event fraction `Σ(a in K)n_a/N`, with undefined
empty denominators. Do not renormalize away unknown keys or recalculate percentiles
inside one relay. The original graph population, curator/seeds, numerical bound,
near-tie caveat and score age remain attached. This describes the selected
curator's distribution among observed content; "famous", "trusted relay" and
"true content" are not alternative names. It never changes operational Q,
acquisition priority or archive retention. Missing Ranker leaves base composition
available, with only this optional join unavailable.

Maintain one exact event-membership contribution and endpoint/author refcount
per enabled population/scope; reuse existing witness and visibility transitions
instead of duplicating a per-event receipt for each component. On an author count
change `n→n+δ`, update square sum by `2*n*δ+δ²` with checked signed arithmetic,
then validate nonnegative counts. Retractions also update kinds, topics, byte sum
and extrema using retained counts/ordered state or an admitted partition rebuild.
An event witnessed by f endpoints can invalidate f contributions after local
visibility changes. Source-cut-before-correction and old/new membership equality
make replay and earlier-first-witness repair cardinality-idempotent.

Advance event-time windows and probe-attempt eligibility without new arrivals;
archive witness history alone does not expire. Keep all previously pinned results
immutable. Use a coalesced endpoint/partition dirty queue and the existing
overflow-to-rebuild rule. No request creates a permanent arbitrary-filter index;
configured reusable scopes get materialization, ad-hoc scopes get bounded jobs
or refusal. Exact distinct/topic/extrema state, visibility fanout and staged
output count toward the existing Deriver grant. A small displayed top ten does
not bound that work. Score-only changes can touch every endpoint with a changed
author; an optional author→endpoint count index has its own measured cardinality.
Global percentile changes may dirty the entire curator-band projection even with
no new witnesses. Base counts remain reusable while the join is recomputed.

Acceptance uses signed events with duplicate witnesses, two events by one key,
duplicate topics, private-only evidence, an imported presence claim, a later
replacement witnessed elsewhere, expiry without ingestion, and a known-zero
versus missing score. Compare incremental state at every cut against an independent
set/grouping oracle. Two peers can produce identical first-witness history and
later have different remote inventories: the API must return identical historical
composition and refuse to infer current inventory. The isolated
[composition experiment](../../experiments/relay-composition/README.md) checks
these distinctions on a small fixture; it is not a live relay or scale result.

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

## Relay selection and optional scalar

The default directory presents comparable availability, latency, evidence
coverage, freshness and tested capability fields. `reliable-read/1` filters by
explicit requested tested capabilities, observed availability >=0.99 and
covered_fraction >=0.8 in the same stratum/window, then orders by availability
descending, p50 latency ascending with missing latency last, and endpoint ID
ascending. A carry-in availability estimate with no in-window latency sample
can remain listed; missing latency is never treated as zero. Unknown values do
not satisfy a required filter. Thresholds are named product defaults, not
statistical confidence estimates; users can request a separately identified
policy. The unfiltered directory still displays missing/insufficient evidence.

A scalar trades unlike properties: the old .625/.375 blend can rank an 80%-up
20ms endpoint above a 99.9%-up 500ms endpoint. Therefore the blend remains an
optional attributable profile, never the meaning of general relay quality.
Neither choice filters archive retention. The profile below remains unchanged
for explicit callers and compatible publication identities.

## Relay profile general-read-relay/1

This is the selected initial local measurement profile, not an implemented or
production-qualified algorithm. Its subject is `Subject::Relay{endpoint_id}`.
It estimates a bounded read's operational behavior from one path. Its component
vector, evidence and coverage remain useful when the scalar is withheld.

| Element | Version-1 rule |
|---|---|
| Input stratum | Endpoint, local observer, route/address-family class, access scope, exact bounded request/method version, timeout and clock policy. Apply SourceCut before reducing corrections. Do not pool imported claims or incompatible paths. |
| Window | Pinned observed-time `[window_end-86400s,window_end)`. The end is independent of commit time. Read required in-window anchors, effective policy boundaries and the necessary predecessor. Signed event times are not measurement times. |
| Sampling unit | One actual compound read attempt, deduplicated by immutable attempt ID after terminal correction reduction. Children, physical duplicate rows and redelivery add no samples. A new executed network retry has its own attempt ID and recorded retry/schedule context. |
| Success | Matching EOSE for the exact bounded request with required returned bodies verified. Empty EOSE succeeds. A preceding AUTH challenge alone is not failure if the permitted read completes. This does not establish exhaustive remote storage. |
| Failure | An executed method's explicit access/protocol refusal, recorded actual deadline expiry, or attributable remote/transport failure before completion. It means this bounded read failed from this path, not global relay downtime. |
| Unknown | Local policy/permit/storage failure, cancellation, unproven dispatch, interruption without terminal evidence, unusable clock or conflicting terminal results. An intent or skipped child never becomes an executed failure or successful sample. |
| Anchors | A terminal observation anchors at its actual end time. An interrupted attempt with retained actual start can break prior carry with an unknown anchor there; an intent without proven execution supplies no invented timestamp. Equal-time conflicting states become unknown; same-state ties retain all sources and use the shortest allowed hold. |
| Hold | Until the earliest next anchor, `anchor+2*base_cadence_at_attempt`, effective cadence/method-policy boundary, or window end. The cadence is the recorded configured base interval, not failure backoff or today's configuration. A boundary stops old carry; unknown anchors stop it too. Remaining time is unknown. |
| Availability | Integrate success S, failure F and unknown U in checked integer microseconds. `A=S/(S+F)` when the denominator is positive; otherwise unavailable. Report `covered_fraction=(S+F)/86400s`, S/F/U and carry assumptions separately. The duration sum equals the window. |
| Latency | Eligible successful attempts with end inside the window and measured request-write-complete→matching-EOSE duration in integer microseconds. Exact nearest-rank p50: sorted `x[ceil(n/2)]`, the lower middle for even n. `L=exp(-p50_us/1_000_000)` in float64. No latency samples means unavailable L; open/first-event RTT is a different fact. |
| Sample gate | At least five unambiguous finalized success/failure attempts ending inside the window. Carry-in predecessors, unknowns and child checks do not satisfy the gate. Keep the exact classified count and separate successful-latency count. |
| Composite | Only with both A and L available and the sample gate met: `Q=0.625*A+0.375*L`. Validate finite values in [0,1]. Missing L withholds Q even if failures establish A=0. Do not redistribute weights. Retain raw Q; the separately gated NIP-85 projection is half-up `round(100*Q)`, not a social percentile. |
| Comparison | Same profile/version, observer/route/access/method/clock stratum, timeout/cadence policy and compatible window/cut basis. Publish coverage, schedule/retry mix and sample differences. Identical scale is not sufficient comparability. |

Capabilities are separate tested facts and optional eligibility filters. The
default component vector includes the selected bounded read result plus declared
versus tested capability statuses; it has no capability scalar C or coverage
weight. For each explicitly requested requirement (for example COUNT or NEG),
use the latest unambiguous actual test in its declared method/access/observer
stratum and window, retaining exact test identity and outcome. Success passes;
an explicit tested refusal/unsupported response fails that scoped requirement;
timeout, local failure or conflicting result is unknown. A newer inconclusive
test cannot be replaced silently by an older successful one. A filter that
requires a fresh passed test additionally applies its registered age limit;
missing/unknown/stale cannot pass. NIP-11 declarations do not establish a passed
test. Every enabled requirement/age policy is recorded with the filtered result;
it does not alter Q or infer support from a count of advertised NIPs.

The two weights preserve the earlier relative A:L ratio while removing the
redundant read-capability contribution. This is a reasoned initial selection,
not evidence of optimal client utility. A vector-only directory, another
time-constant/weight choice, a minimum-coverage gate or a profile treating
timeouts as unknown can be evaluated separately. Such semantic changes need a
new profile/version. Five attempts can cover very little of a day; it is an
admission floor, not confidence. Missing latency does not conceal the failures:
the API still exposes A, sample counts and outcomes when Q is unavailable.

The query plan must pay for scoped window reads and predecessor/boundary lookup,
correction deduplication, input classification, exact latency selection and
capability lookups. Use indexed endpoint/stratum/time partitions and a changed
attempt/boundary dependency queue; schedule expiry without new arrivals. Store
enough boundary state to retract changed adjacent segments. Exact p50 charges
all eligible durations and sort/selection/spill state; a five-row source limit
is not its implementation. Late corrections can change both adjacent timeline
segments and latency order statistics. Bound/resume the actual affected work,
or refuse an over-budget generation; do not rescan retained measurement history
for every hourly score or claim O(changed rows) regardless of partition size.
Include target artifacts, visible comparison queries and rejected/unknown inputs
in the common job budget.

The independent fixture has window [0,1440) minutes, cadence 10, and anchors
S at -5, F at 5, U at 10, S at 25/30, F at 35 and S at 45/50. Carry-in gives
[0,5); final carry stops at 70. Expected S=40 minutes, F=15, U=1385,
`A=8/11`, coverage `11/288`; six classified in-window attempts meet the floor.
Their four successful latency values `[200000,100000,300000,900000]` us give
p50=200000, `L=exp(-0.2)` and `Q=.625*(8/11)+.375*exp(-.2)`.
A policy boundary at minute 60 instead gives S=30, F=15, U=1395 without changing
earlier intervals. Probe-count availability 4/6, averaging middle RTTs, carrying
through unknown to the next success, using backoff as cadence and adding child
checks must disagree with the oracle. All-failure inputs expose A=0 but withhold
Q for absent L. These are specified expectations, not executed product results.

## Seed availability, optional eligibility and curation

The `seeded-trust/1` reference and `seeded-trust/2` default share the eligible public graph and seeded-diffusion target described in
the [Ranker contract](derived-read-publication.md#ranker). For nonempty configured
seeds, freeze graph membership before checking which seeds are present. An empty
configured seed set withholds before allocating a graph job solely for ranking.
The initial graph includes authors of eligible current follow or public-mute
list headers, including valid empty lists, and endpoints of
eligible relations. Retain vertices whose edges are removed by self-edge or
same-source mute rules; they can become isolated/dangling. A malformed, deleted
or otherwise ineligible list does not alone establish membership, although a
pubkey can still occur through another eligible list or relation. A configured
seed absent from this declared vertex set is missing, not an invented active
vertex. Membership is a graph definition, not a census of people or all accounts.
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
validated ordinal range permits. An ordinal-to-pubkey array adds at least `32*V`
raw bytes; the reverse pubkey-to-ordinal lookup/sort-join state, hashes/indexes,
alignment and temporary build copies are additional. These dictionaries can
exceed the iteration vectors: 100 million 32-byte pubkeys alone are 3.2 GB
(decimal). Account for their persisted and resident placement separately;
compression or mapping does not eliminate construction/join cost. Add weights if needed,
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

## Execution refinements from the Go code walkthrough

The [analytical code walkthrough](../research/go-analytics-code-walkthrough.md)
examines actual planner, targets and score/publication paths. A selected-target
configuration must stop unselected computations at execution, not only remove
their names from a completion manifest. Independent target generations prevent
a costly optional overlap calculation from blocking unrelated global facts.

Dirty-only work is valid with no newly admitted source interval: corrections,
policy changes, expiry and repaired/reindexed input can require a new output at
the same data frontier. Publish its explicit dependency/revision change through
a real progress transaction, with empty source work distinct from missing work.
A fully inspected source interval with no eligible output still advances its
source progress through an explicit no-output commit. Repeated control-only or
below-floor intervals must not trap a capped reader before later useful data.
Store the actual affected partition/key set, not merely the broad earliest-to-
latest span. Apply the analytical half-open interval convention consistently at
midnight and other boundaries. Every target's actual source reads bind the
compatible source cut; a shared run identifier alone does not freeze rows against
concurrent ingestion.

Exact quantile state is charged by its complete input population and algorithm,
not by the small number of output buckets. Use an admitted spill/external-memory
plan or fail without a new complete generation when it cannot fit. Keep
approximate sketches separately named. Measure empty, sparse, widely separated
dirty partitions, whole historical reconstruction and concurrent arrivals,
checking source membership as well as aggregate values.

Current list state has a winner header independent of child entries: subject,
coordinate, winning event ID/time, parser version, interpretation/visibility
status and source lineage. Preserve a valid empty winner and an uninterpretable
winning event as distinct states. Child entries alone cannot prevent an older
backfill from resurrecting a superseded list. Maintain changed headers and their
whole-list contribution deltas; do not rewrite every unchanged author's state
for each small update. Test old list → newer empty list → still older backfill in
a different touched month, and compare with a full rebuild at the same cut.
The declared isolated-vertex policy applies consistently to graph construction,
seed availability, raw score vectors and publication eligibility.

## Exact overlap without unconditional pair materialization

First canonicalize scoped/windowed exact endpoint event sets and group equal
sets by digest plus bytewise equality verification. A digest alone is not proof
of equality. Evaluate sound size/prefix filters and exact intersection/Jaccard
verification across distinct groups; probabilistic candidate filters remain a
separately labelled approximate mode. Store qualifying group relations with
window/cut/scope identity and expand endpoint pairs lazily for authorized paging.
This represents the full exact logical relation without eagerly writing every
pair of equal sets. Changed membership invalidates the affected group and its
relations; pinned generations remain immutable.

An export requesting every pair still costs the output size: 100,000 mutually
qualifying endpoints imply 4,999,950,000 pairs. Budget, resumability and declared
refusal apply; a spanning forest or sampled candidates cannot substitute for the
requested relation. Qualify grouping, filtering and expansion against an
independent all-pairs oracle on small fixtures, including hash-collision negative
controls, empty sets, skew, churn and endpoint access changes.
