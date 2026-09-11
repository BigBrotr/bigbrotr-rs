# Analytics, graph, read and publication audit

Date: 2026-09-11. Fixed baseline:
`8c81472e75da118b04809636a73f3231157227b5`. Planning review only; no product
implementation, database experiment, historical test execution, publication,
commit or push occurred. The neighboring Go and historical repositories were
not accessed or changed. Reading the previous Go walkthrough does not repeat
its underlying source audit.

All twelve assigned files (6,642 logical lines; 490,765 bytes) were read
completely from the fixed Git objects.
The [read manifest](../research/evidence/11-analytics-read-manifest.json) records
complete ranges and before/after SHA-256, byte and logical-line counts, separately
from supplemental excerpts and primary protocol checks. The initial truncated
analytics output was replaced by complete bounded reads. Hash agreement identifies
the text reviewed; it is not evidence of runtime correctness.

## Findings and concrete dispositions

Severity describes the effect on a future implementation, not an observed runtime
failure. P1 means externally different answers or a misleading promised contract;
P2 means a material ambiguity, dependency error or qualification gap. Canonical
amendments are owned by the coordinating review; this report records its baseline.

### AA11-01 — P1: the relay composite has weights but no complete profile

[P05-T12](../implementation/05-analytics.md#p05-t12--independent-relay-operational-quality)
and derived-read-publication lines 260–278 specify five observations, time-weighted
availability, twice-cadence carry-forward, successful RTT p50, an exponential
transform and weights 0.5/0.3/0.2. They do not settle:

* which of WebSocket-open, first-valid-event and request-to-EOSE durations is the
  rank input; P04 deliberately retains all three separately;
* which executed outcomes establish a zero, which remain unknown, where their
  intervals begin, or how overlapping, same-time or corrected observations reduce;
* the availability denominator, carry-in observation, irregular cadence and
  scheduled backoff treatment;
* whether five observations means compound attempts, child checks, successes,
  distinct timestamps or monitor records;
* the default capability requirement set, its scalar reduction and validity.

Two conforming-looking implementations could therefore rank the same relays
differently under the same profile identity. NIP-66 specifies RTT units but does
not provide this composite, its measurement boundaries or an optimum formula.
[Pinned NIP-66](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/66.md)

The minimal proposed closure is the fully specified candidate below. This is a
deliberate BigBrotr policy for comparison, not an empirical endorsement. Its
registry/configuration version must change if another interpretation is selected.

### AA11-02 — P1: temporal visibility is missing from the snapshot contract

P06-T02 lines 121–124 requires expiry to change visibility without new ingestion;
P06-T03 lines 161–188 binds cursors to data/target generations and policy. The
common SourceCut represents ingestion positions, not the evaluation clock.
Neither the assigned read specification nor its cursor fields chooses the effect
of a signed event expiring between the first page, its count and its next page.
Reading at an old cut alone does not settle whether that event is still eligible.

Freeze a `visibility_as_of` and a temporal visibility revision in the admitted
context, distinct from source cut and restrictive-policy authority. For ordinary
reads, choose an explicit expiry barrier: when relevant expiry makes the pinned
ordinary population ineligible, invalidate continuation with a typed restart
condition before sending expired records. New admission pins the new temporal
revision. Apply the same basis to list/count/search/NEG and cache validators.
Authorized immutable history
can retain its separate frozen membership.

Proposed contract names are `ReadViewStamp { source_cut, target_generations,
visibility_as_of_us, temporal_generation, expiry_deadline_us, policy_epoch }`
and refusal `temporal_snapshot_expired` (409 for ordinary HTTP continuation).
They are new logical contract names, not existing tables. The Indexer maintains
an ordered expiry schedule and next-future-expiry summaries per source/view
partition, updated with source membership and the temporal frontier. Admission
looks up summaries for the bounded partition cover selected by its access plan;
an exact narrower candidate lookup is optional within the same budget. Use the
minimum relevant future expiry as the cursor's ordinary visibility deadline.
Preserve a separate absolute 900-second pin expiry. No query scans all historical
expiration tags or creates an enduring per-query expiry index.
Compare the original unsigned expiration timestamp before any calendar/microsecond
cast; an extreme future protocol timestamp must not overflow this schedule.

A conservative partition cover may expire a narrow cursor earlier than its own
result membership requires. Disclose that limitation, preserve entered filters,
and measure completion/restart rates for actual long traversals under high
expiry churn. Do not rotate every resource globally each second or claim that
perpetual early restarts deliver useful pagination. Charge expiry scheduling,
summary updates, pin lookup and cancellation fanout; the bounded active-pin
registry owns work cancellation. If broad current traversals cannot complete
usefully, offer a separately named live-as-of current resource with explicitly
shrinking membership or the already authorized immutable history/export route.
Neither alternative preserves both unchanged ordinary membership and immediate
expiry suppression; its interface must state which promise it provides.

An alternative is continuing a cursor with live expiry exclusions and recording
that membership may shrink; that is a different pagination promise and cannot be
called an unchanged snapshot. Merely including current wall time in each query
does not resolve the contract. Test one event expiring between list/count/next
page, with no new archive arrival, and another expiring between historical read
and live handoff. Preserve existing restrictive-policy revocation regardless of
the chosen temporal policy.

### AA11-03 — P2: NIP-66 still depends on the optional cross-observer resolver

P07-T07 line 345 lists P05-T06 as a mandatory dependency. P05 lines 718–722 and
P04 lines 500–503 explicitly separate `LocalMonitorView` (Indexer/P04-T11) from
`ResolvedRelayEvidence` (Deriver/P05-T06), and require local NIP-66 publication
to work with the latter disabled. Change P07-T07 to depend on P04-T11 and its
committed local view; add the resolver only for a separately declared aggregate
profile. The ordinary publisher cannot relabel imported success as local success.
Acceptance must disable/corrupt the resolver, advance local measurements, and
capture a new correct signed 30166 from the actual stored local producer.

### AA11-04 — P2: family names and proposed files do not supply row/read bindings

The fifteen retained derived names are useful target-family names, listed below.
P05's migration filenames and A08's generic fact fields do not define their exact
table keys, typed resource names or query schemas. Required newer state includes
empty current headers, unresolved relation dependencies, actor refcounts, current
adjacency, graph artifacts and independent social/relay score rows; these are not
all independently named tables. No production DDL exists in this assigned scope.

A central logical catalog should distinguish existing family identifiers from
new proposed identifiers and record, for each, owner, row unit/key, full generation
binding, input/output type, common reads, update/retraction and retention. Do not
infer that `0504_interaction_facts.sql` already specifies a table called
`interaction_facts`, or that a route's resource placeholder accepts a table name.
Physical partition/order/index layouts remain measured P02/P05 decisions.

Cross-subject keys must include a discriminator: equal 32 bytes used as a pubkey
and event ID remain different subjects. A relay endpoint additionally needs an
explicit local subject form and a mapping to conservative canonical URL for an
optional external-identifier assertion. It must not be mistaken for a pubkey-only
NIP-66 relay identity. Addressable identity includes kind, author and complete
identifier; version identity remains an event ID. Facts additionally bind their
definition, population/window and generation; raw `subject,value` is insufficient.

P05-T04 still instructs implementation to define relation kind/marker precedence,
self-interaction and polarity rules. P07-T05 still instructs it to define exact
bindings. These are real remaining semantic work, even though all four subject
families are retained. Freeze a producer→definition→subject→wire table before
claiming field completion. Include received versus authored `reactions_cnt`,
root versus immediate-parent `comment_cnt`, address-versus-version coalescing,
report direction, zap payment identity/conflicts, and per-family meaning of the
generic `selection.min_events=1`. A relay score must not require an invented
authored event to satisfy that gate. If relay NIP-85 mapping is enabled after its
interop gate, it needs P05-T12 as an actual producer dependency.

### AA11-05 — P2: mixed search matching is defined more precisely than relevance

P06-T06 lines 325–332 defines a mixed terms/phrases grammar and ignored unsupported
extensions. Lines 345–348 and search-and-reconciliation lines 74–80 rank by whole
content equality and contiguous query phrase before term count. They do not say
what the equality string and singular phrase are for `alpha "beta gamma"`, two
quoted phrases, or a query whose ignored extension changes its raw text.

Freeze relevance input in the parsed query contract. A minimal candidate is:
after removing syntax delimiters and ignored extensions, preserve ordered semantic
segments. Proposed exact fields are `ranking_text = join(normalize(literal_i),
U+0020)` and `ranking_tokens = concatenate(analyze(literal_i))` in source order,
where normalize/analyze are the existing Unicode profile. Decode permitted quote
escapes before this construction; preserve literal internal whitespace, and use
one ASCII space only between parsed segments. Compare normalized whole content
to ranking_text; compare its ordered tokens to contiguous ranking_tokens; use
distinct-token intersection cardinality as the third score component. Empty
analyzed queries retain the existing invalid-query rule. Bind these fields and
the matching AST/version into filter/cursor identity: literal whitespace that
changes an equality boost is a semantic change even if token membership agrees.
Unknown extensions contribute to neither field. Alternatively,
disable the first two boosts for mixed expressions and state that rule. Compare
the alternatives on real queries before asserting better relevance. Adding an
ignored extension or changing quote syntax without changing the selected semantic
query must not silently alter the ranking/cursor identity. The NIP leaves search
quality implementation-defined; it does not resolve this local ambiguity.
[Pinned NIP-50](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/50.md)

### AA11-06 — P2: graph accounting changes the meaning of H

Analytics-detail lines 452–457 reserves `U` for retained signed events and `H` for
historical follow/mute tag records. P05-T10 lines 533–535 instead says “archive
events H”; T13 then asks for scanned H/E/P. Use U/P/V/E/H/K/I consistently in the
catalog and evidence schema. Report whole-list bytes/tags, not only the number
of changed authors. Generation-specific dense ordinals also require a mapping
checksum: a new vertex can change many ordinals, so sharing unchanged adjacency
requires either a stable map or explicit remapping/build cost. This is not proof
that every local edge update needs global historical reconstruction.

## Precise candidate relay profile for AA11-01

Proposed local identifier `general-read-relay/1`; not a new wire field or an
already implemented profile. It measures suitability of one bounded read method
from a declared local path. Keep the component vector and raw evidence visible.

| Element | Proposed version-1 rule |
|---|---|
| Input identity | Endpoint, local observer, route/address-family class, access scope, exact bounded read request/method version, timeout and clock policy. No pooling imported claims or different paths. Bind source cut before correction reduction. |
| Window | Pinned `[window_end-86400s,window_end)` in observed time. Record window_end independently from commit time. Read one necessary preceding timeline state plus all in-window changes; do not use an event-created-time cut. |
| Sample unit | One actual compound liveness/read attempt, deduplicated by immutable attempt identity after resolving its terminal correction. DNS/TLS/catalog children and physical retries add no independent sample. A new actual network retry remains a new attempt, identified as such. |
| Success state | Matching EOSE for the exact bounded request, after verification/attribution of any returned required events. Empty EOSE succeeds. A challenge alone does not establish refusal when the read still succeeds. |
| Failure state | An actually executed bounded method that explicitly refuses the requested access, has a recorded deadline expiry, or ends with an attributable remote/transport failure before completion. This means the selected bounded read did not complete from this path; it does not prove relay-wide outage or missing event storage. |
| Unknown state | Local policy/permit/storage failure, cancellation, unproven dispatch, interruption without terminal evidence, unusable clock or conflicting same-attempt results. A recovered intent is never a measured failure. Retain detailed reasons. |
| Timeline | A completed observation anchors at its actual end time. A started-but-interrupted observation may anchor unknown at its retained actual start time; an intent with no actual time contributes coverage uncertainty without an invented anchor. Sort anchors deterministically; simultaneous conflicting state values produce unknown. Same-state ties retain their sources and use the shortest permitted carry-forward horizon. |
| Hold horizon | A state applies until the earliest next anchor, `anchor+2*base_cadence_at_attempt`, next effective cadence/method policy boundary, or window end. Use recorded configured base cadence, not exponential failure backoff or today's configuration. After the horizon, state is unknown. Do not retrospectively assign today's cadence to historical attempts. |
| Availability A | Integrate success and failure durations in checked integer microseconds as S and F, with unknown U. `A=S/(S+F)` if denominator is positive; otherwise missing. Report all durations and `covered_fraction=(S+F)/(S+F+U)` separately. Carry-forward is an estimator, not direct continuous uptime evidence. |
| Latency L | Only successful in-window attempts with measured request-write-complete→matching-EOSE duration, in integer microseconds. Exact nearest-rank p50 (even n uses the lower middle order statistic), then `L=exp(-p50_us/1_000_000)` float64. Open/first-event RTT remain separate facts. No successful samples means missing L. |
| Capability requirement | Minimal default one named requirement: that same bounded unauthenticated REQ→EOSE operation. The latest unambiguous in-window actual test yields C=1 for success, C=0 for explicit access/protocol refusal; timeout/transport/local/unknown outcomes yield missing C. No stale prior success replaces a newer inconclusive test. NIP-11 declarations are explanatory only. |
| Optional larger requirement set | A different explicitly identified profile can name several tested operations. Use fixed equal requirement weights and `passed/requirements` only when every required latest test is classified; otherwise omit C. Record requirement IDs, exact test versions and evidence validity. Arbitrary NIP-count scoring is not this rule. |
| Composite gate | At least five unambiguous actual finalized success/failure attempts with end time in the window and at least one available positive-weight component. Unknown attempts, carry-in predecessors and child checks cannot satisfy the five-attempt gate. Keep facts when withheld. |
| Composite Q | For available components B, `Q=sum(w_j*x_j)/sum(w_j)`, weights A=.5, L=.3, C=.2. Q is float64 in [0,1], validated finite. API retains raw Q and B. A possible separately validated NIP-85 binding uses half-up `round(100*Q)`, without social percentile normalization. |
| Comparison | Require identical profile/version, method/access/route/observer basis, window/cut compatibility, capability set, timeout/base-cadence policy and available-component set. Publish coverage and sample differences alongside the value. Same numerical scale alone is not comparability. |

This minimal capability component is correlated with availability. Preserving
the earlier three weights makes that redundancy explicit; it is not evidence
that the component adds independent information. Alternatives worth comparing
are a vector-only directory, a two-component profile, or separately named client
requirements such as read+COUNT+reconciliation. Timeouts could instead remain
unknown in a conservative direct-evidence-only estimate, but that changes the
population and interpretation and must have another profile version. No sample
threshold, cadence or weight establishes statistical confidence or universal
relay trust. The five-attempt gate can admit low temporal coverage; the UI must
show it, and any minimum-coverage gate is a separately recorded policy choice.

The input/query plan is part of the correction: indexed scoped window reads plus
the required predecessor; source-identity deduplication before aggregation;
incremental changed-attempt/timeline partitions and scheduled expiry. Exact RTT
p50 charges all successful durations, with external sort/spill or honest refusal.
Do not cap a source query at five rows, or read the full retained monitoring
history on every hourly score. Charge predecessor lookup, correction reduction,
capability lookups, artifacts and public comparison queries to the same budget.

An independent arithmetic fixture uses a 24-hour window starting at minute 0 and
base cadence ten minutes. Anchors are success at -5, failure at 5, unknown at 10,
success at 25 and 30, failure at 35, success at 45 and 50. No further observations
occur. The carry-in success contributes minutes [0,5); final success stops at 70.
Expected S=40 minutes, F=15, U=1385, A=8/11 and covered_fraction=11/288.
There are six eligible in-window attempts, four successful: using 4/6 as A is
the negative control. For successful durations [200000,100000,300000,900000] us,
p50=200000 and L=exp(-0.2). Latest capability succeeds, so
Q=.5*(8/11)+.3*exp(-.2)+.2. A cadence-policy boundary at minute 60 instead stops
the last hold there: S=30, F=15, U=1395. A new cadence cannot alter intervals
before that boundary. Duplicate physical rows change none of these quantities.
These are specified expected values, not results of an executed product test.

## Exhaustive assigned-scope derived family inventory

The names below occur together in derived-read-publication lines 118–129 and
derived-read-publication-findings lines 91–96. They are selected logical target
families, not a claim of existing ClickHouse DDL, one physical table per family,
or public resource IDs. All bind definition/schema, source cut, visibility/policy,
window and immutable target generation. Only their own committed owner publishes
new contents; PG manifests/pointers arbitrate visibility.

| Exact existing identifier | Logical row/read meaning | Owner and main task |
|---|---|---|
| `current_replaceable_events` | Coordinate winner header, including empty/invalid state; author/kind/current reads | Deriver P05-T02; indexing/read ownership must share one interpretation |
| `current_addressable_events` | Kind/author/identifier winner header, source version and status | Deriver P05-T02 |
| `current_relay_list_entries` | Child membership under one whole winning relay list, preserving read/write marker; needs independent header even with no children | Deriver P05-T02 |
| `global_facts` | Scoped exact event/author/kind/activity/size facts; archive versus visible definitions differ | Deriver P05-T03 |
| `social_facts` | Current follows/mutes and registered author/interaction/activity facts; event count versus actor count distinct | Deriver P05-T02–T05 |
| `nip85_input_facts` | Typed subject×definition inputs with lineage for each actually supported wire field | Deriver registry/producers P05-T01–T05; publisher consumes P07-T05 |
| `relay_facts` | Scoped operational observations and attributed claims, never universal relay truth | Deriver P05-T06; distinguish Indexer local input projection |
| `relay_state_aggregates` | Time/attribute/context aggregates and resolved claim view with competing support | Deriver P05-T06 / `ResolvedRelayEvidence` |
| `relay_coverage` | Endpoint/scope/time traversal and gaps, observed evidence population | Deriver P05-T07 |
| `relay_dedup_candidates` | Ordered endpoint pairs under one candidate method/common universe; not identity deduplication | Deriver P05-T09 |
| `relay_cluster_edges` | Qualifying pair, exact set sizes/intersection/union and Jaccard evidence | Deriver P05-T09 |
| `relay_clusters` | Stable component/member identities and compatible edge generation; connectivity, not all-pairs similarity | Deriver P05-T09 |
| `event_propagation_stats` | Event/stratum summaries and endpoint-count bucket distribution/p50/p95/max; ID-required drilldown | Deriver P05-T07 |
| `relay_selective_availability` | Scoped direct S/N/U distinct-target classification and fractions; provenance required for per-event evidence | Deriver P05-T08 consuming P04-T13 |
| `relay_availability_frontier` | Historical observed serving evidence and separately named interval estimators; never an exact universal retention boundary | Deriver P05-T08 and historical evidence resource |

Additional required representations have **no exact table names fixed by the
assigned baseline**: current follow/mute headers/members, exact contribution and
actor refcount state, unresolved relation reverse indexes, accepted/conflicting
zap identities, incremental social adjacency, vertex ordinal mapping/CSR shards,
raw social scores/percentiles, independent relay component/score rows, and
search normalized/ordered/distinct token projection. Their migrations/schema
paths are proposed in P05-T02–T12 and P06-T06. `CurrentHeader`, `TargetRequest`,
`TargetManifest`, `LocalMonitorView` and `ResolvedRelayEvidence` are contract names,
not proof of similarly named SQL tables. This audit invents no physical DDL.

Graph artifacts need not be CH rows. PG owns definition/dirty/target commit state,
read pins and publication intents/outbox/obligations; these are not extra archive
fact families. Reconciliation proposes a separate Indexer-owned Rocks projection
plus bounded CH comparisons. Treating every artifact as a mandatory new database
would misstate the selected architecture.

## Read and publication interfaces

| Exact interface identifier | Meaning and limits |
|---|---|
| `QueryRequest + Principal → QueryPage` | One admitted read core, typed authority, source/target pins, scan/memory/deadline and output budget. DVM resource names are registry IDs, never table names. |
| `/v1/events`, `/v1/events/{id}` | Collection and visible point event reads; complete signed fields with outer metadata. Named authorized history is separate. |
| `/v1/resources/{resource}` and `/v1/resources/{resource}/count` | Registry-backed rows/counts; GET's percent-encoded JSON filter and read-only POST envelope preserve literal array values. Concrete registry IDs remain to be fixed centrally. |
| `GET /v1/meta`, `GET /v1/capabilities` | Instance/version and actual activated resource/capability state; neither is proof of unavailable producer support. P06 should avoid wording that implies POST is required on metadata/point routes. |
| `REQ`, `CLOSE`, `COUNT`, `AUTH` | Read-only ArchiveRelay/NIP-42 boundary; historical per-filter limits, distinct union, EOSE then bounded live acquisition feed. COUNT is finite. Inbound EVENT receives refusal. |
| `NEG-OPEN`, `NEG-MSG`, `NEG-CLOSE`, `NEG-ERR` | Optional enabled NIP-77 server set: current, authorized, body- and wire-retrievable IDs with pinned set/limit revision. Later REQ is not an atomic snapshot export. |
| `bigbrotr.query/1`; kinds 5999/6999/7000 | Experimental directed DVM request/result/feedback. Finite `list`/`count`; opt-in event `subscribe`/`renew`; matching-owner kind-5 cancellation. Explicit NIP-44 v2 variant and separate bounded controls. |
| `lnd-prepaid-v1` | Optional finite DVM payment profile; LND `POST /v1/invoices`, `GET /v1/invoice/{payment_hash_hex}` only. No spend/refund RPC; independent regtest gate. |
| `Sign`, `Enqueue` | Purpose/key/body validation, independent returned-event validation, exact bytes before network, one durable obligation per `(signed_event_id,destination)`; connection epoch for ACK ownership. |
| Kinds 30382/30383/30384/30385 | NIP-85 pubkey/event/addressable/external assertions. Facts work without rank; distinct algorithm/viewpoint keys, immutable source artifacts, durable whole-population pagination. |
| Kinds 0/10040 | Provider explanation and explicitly authorized own-user provider preferences; preferences are not provider advertisements or endorsements by others. |
| Kinds 30166/10166, optional 0/10002 | Local NIP-66 discovery/announcement and operator metadata/relay list; original field times, privacy controls, prose timeout dialect and explicit microsecond→millisecond conversion. |
| `bigbrotr-context/1`, `bigbrotr:meta` | Opt-in immutable context bytes bound by a signed digest; production HTTPS retrieval route still needs a concrete route/schema binding. The illustration `/assertion-context/<digest>.json` is not existing deployment evidence. |
| Kind 31990 | Optional NIP-89 DVM handler announcement, persisted random d, k=5999 and exact experimental profile marker. |
| Exporter native/Parquet/JSONL/projection/provenance-companion modes | Pinned authorized selection and durable verified manifest; native deployment backup differs from portable corpus. Export does not mutate or advance archive completeness. |

The NIP-85 kinds, optional rank field interpretation, key separation and user
preferences were checked against the pinned primary document. Score 80 for a
pubkey percentile, relay composite and hypothetical note recommendation has
different meaning; never order those subjects in one purported trust population.
[Pinned NIP-85](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/85.md)

## Review-10 recheck and practical evidence

| Review-10 finding | What the current baseline actually contains | Remaining boundary |
|---|---|---|
| AR-01 relay wire eligibility | Search lines 283–302/P06-T07–T08 bind `RelayRetrievable`, subscription scalar/byte bounds and serving-limit revision | Actual NEG→REQ encoded-body and limit-change tests remain unrun |
| AR-02 optional paid gate | P06-T12/P07-T09 and DVM closing sections distinguish core/free/subscription/paid gates | No paid capability claim without real node evidence |
| AR-03 control admission | DVM lines 368–390 reserve verification/control capacity and separate subscription slots | Saturated real data queue plus owner control test remains required |
| AR-04 overlap scale | P05 lines 724–732 explicitly add equal-size sets, cliques, edge output bytes and exact alternatives | O(R²) output is still possible; useful measured scope is not supplied by this amendment |
| AR-05 search usefulness | Search lines 304–310/P06 closing additions require accepted/refused query mix and whole footprint | No evidence yet selects CH as universally optimal or makes common broad queries useful |
| AR-06 zap issuer producer | P05-T05 now names P04-T14; A05 names LNURL evidence. The full acquisition-domain reread is owned separately | This audit checked dependency excerpts, not all P04-T14 behavior; end-to-end historical issuer acceptance still needs evidence |

Seeded social trust survives the reread coherently: `r0=t`, alpha .85, 100
ordered float64 iterations, dangling mass returned to seeds, same-author mute
suppression, no unseeded fallback, optional post-diffusion indegree gate and
positive-population percentile. The raw graph/score is not author worth or a truth
classifier. Event/addressable/external facts do not require inventing a universal
note score. Optional recommendations must own verified relation definitions,
actor caps/decay, pinned actor trust and the potential global invalidation cost.

Current adjacency can be updated from changed whole lists, but global diffusion
and percentile projection remain broad. Measure U/P separately from V/E/H/K/I,
both graph construction and iteration, normalization, artifact writes, retained
generations, publication traversal, exact distinct/refcount/quantile state,
overlap candidate/edge output, and search tokens/postings/verification. The
documented 16V vector bytes and edge-pass arithmetic are layout calculations,
not achieved capacity. Sound pruning, partitioned execution, alternative embedded
search and disclosed approximate overlap remain alternatives with evidence gates.

The next discriminating evidence is the existing packaged source→archive→CH/
artifact→API/relay/publisher path with independent expected identities and values,
plus the new temporal and relay-profile cases above. Include physical duplicate
rows, late corrections at different cuts, one ancient current list, partial seed
presence, ring topology/mass diagnostics, no-ranker facts, resolver-off NIP-66,
changed subjects beyond page 500, same-byte retry, clock rollback, lost ACK,
policy revocation, actual spill/refusal and mixed-load cancellation. Report useful
accepted workloads as well as safe refusals. Nothing in this complete reread
establishes production fitness, statistical confidence or universal optimality.
