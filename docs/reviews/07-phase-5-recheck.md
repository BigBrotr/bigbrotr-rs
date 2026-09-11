# Phase 5 full reread and analytical completeness recheck

Date: 2026-09-11. Scope: planning only. This is a fresh full-text reread of the
surviving original phase-5 source, followed by a contract-level crosscheck of the
current design. Historical implementation choices are evidence and proposals,
not obligations to reproduce useless work. No product code, legacy files, Git or
SSH state was changed and no historical or prospective test suite was run here.

The current design already preserved every phase-5 service and target family.
The reread found several missing concrete analytical definitions and important
scale/test distinctions behind those family names. They are now addressed by the
new [analytics supplement](../architecture/analytics-detail.md). This is closure
of planning gaps, not proof that a future implementation is correct or fast.

## Exact source coverage and inventory

The full source is the frozen phase-5 README at
`.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-5-derivation/README.md`.
Its source Git revision is `757a8d899aefb277c0211077bc30ba0c8de68796`, as recorded
in the existing [source inventory](../research/source-inventory.json). This task
read that inventory rather than executing Git or claiming a fresh legacy HEAD
inspection.

| Evidence | Result |
|---|---|
| Frozen README size | 442,162 bytes; 4,551 logical lines, 4,550 newline characters; no final newline |
| SHA-256 | `db70bfddf2eb29732792f5c25cf8f8a7a4ee5a01e5024c2d4dcb8a77ae463221` |
| Fresh reading | Every logical line 1–4,551, sequential bounded full-text chunks, including historical contradictions, corrected measurements, traceability discussion and closing ledger |
| Output coverage | Truncated intervals in early chunks were reread; heading/text searches subsequently located citations, rather than substituting for reading |
| Frozen phase directory | Recursive Markdown enumeration found only `README.md` |
| Read-only legacy phase directory | Recursive Markdown enumeration found only `README.md`, byte-identical to the frozen copy |
| Extra capture required | None; no missing surviving phase-5 Markdown exists to copy and no snapshot was overwritten |
| Retired sources | Lines 97–103 explicitly retire all 22 indexed task Markdown files on 2026-09-06; they are absent in both directories |

The [machine-readable inventory](../research/evidence/phase-5-extra-sources.json)
records both paths, hashes, bytes, line conventions, full read intervals, the 22
retired task filenames and the empty extra-source set. The absent task files were
**not** recovered from historical Git and their complete bodies were **not** read.
Their surviving consolidated contracts and correction history were read in full.
This boundary prevents a claim of rereading unavailable files or auditing every
referenced Go implementation and old test.

Full current-document crosscheck:

* [Derived/read/publication](../architecture/derived-read-publication.md): complete
  service, generation, registry, score, publication, read, export and acceptance
  contracts, including independent lifecycle and public-policy boundaries.
* [Data and durability](../architecture/data-and-durability.md): complete event,
  source-fact identity, immutable correction, serving-frontier and backup contract.
* [Capacity and benchmarks](../engineering/capacity-and-benchmarks.md): complete
  measured-evidence, workload and billion-scale representation programme.
* [Protocol evolution](../architecture/protocol-evolution.md): complete pinned
  standards, measurement context, assertion families, incremental cost matrix,
  proposed extensions and practical release acceptance.

These are live design files and may receive the coordinating review's changes.
The frozen phase source is the fixed evidentiary baseline; links below identify
current owning sections. The new supplement is part of this review's resolution.
The wider vision, other phases and current protocol reread are separate parallel
assignments; their completeness is not claimed by this phase-5 artifact.

## Findings and resolutions

| Finding from the full reread | Resolution now persisted | Status |
|---|---|---|
| Broad global/social facts did not explicitly preserve total, per-kind and per-author/per-kind exact event counts | [Baseline facts](../architecture/analytics-detail.md#baseline-event-and-social-facts) separate acquired archive versus current-visible populations, kind-count compatibility, exact units and update ownership | Addressed in planning |
| Propagation existed as a name but lacked bucket outputs, precise spread quantiles and correction transitions | [Propagation](../architecture/analytics-detail.md#propagation-aggregates-and-historical-evidence) defines distinct endpoint-count distribution, per-bucket event counts/p50/p95/max, single-endpoint summary, bounded history/drilldown and old/new contribution replacement | Addressed in planning |
| Historical adjusted relay count disappeared behind advisory clustering | [Overlap count](../architecture/analytics-detail.md#relay-overlap-and-the-adjusted-endpoint-count) defines the component/coverage intersection deduction and basis states; rejects an unsupported true-relay census or hard real-server bound | Addressed with corrected meaning |
| Third-party conflict handling was broad but did not define per-attribute support/current resolution/local precedence | [Claim policy](../architecture/analytics-detail.md#third-party-claims-and-local-observation-precedence) preserves historical support and current latest-observer views, source keys and conflicts, recent local precedence, unknowns and bounded previews | Addressed with safer tie semantics |
| Nonempty configured seeds absent from the graph were not an explicit withholding state; publication indegree and inverse-PageRank assistance had no disposition | [Seed contract](../architecture/analytics-detail.md#seed-availability-optional-eligibility-and-curation) defines all absent/partial states, weight coverage and initialization; optional eligibility defaults off and optional curation never installs seeds | Addressed without mandatory extra service |
| P at 100 billion did not quantify V/E/H/K or score-vector state; kernel timing could be mistaken for complete ranking cost | [Graph capacity](../architecture/analytics-detail.md#graph-state-is-different-from-provenance-state) separates all dimensions, gives transparent byte/pass arithmetic and requires full build/load/iterate/normalize/write evidence | Addressed; actual capacity remains unmeasured |
| Original attack and real-engine fixtures were stronger than generic statements of future correctness | [Discriminating validation](../architecture/analytics-detail.md#required-discriminating-validation) preserves the useful cases and explains topology, part-count, churn, replay and producer→consumer evidence | Addressed as future evidence obligations |
| Ranking every named subject could encourage invented universal scores | [Subject policy](../architecture/analytics-detail.md#subjects-and-the-meaning-of-ranking) chooses pubkey viewpoint trust and relay suitability; other subject families initially expose valid facts, with optional recommendation profiles carrying a distinct threat model and invalidation cost | Addressed under latest owner steering |

## Consolidated requirement-to-design disposition

The source line spans below refer to the frozen README. A row can retain the
product requirement while changing its implementation or rejecting a false
inference. Repeated historical test reviews are consolidated into the contract
they establish; they do not create repeated obligations or line-count gates.

### Current state, facts and progress

| Source requirement and locations | Current owning contract | Disposition and rationale |
|---|---|---|
| D-E1 current replaceable/addressable state; lines 19–88, 180–274, 767–976 | [Deriver](../architecture/derived-read-publication.md#deriver), [ordinary serving](../architecture/protocol-evolution.md#archive-retention-and-ordinary-serving) | Retain whole current snapshots, greatest timestamp/lowest-ID tie, kind/coordinate semantics, malformed status and current-before-filter. Full ordered tags replace the old lexicographic-d workaround. |
| Whole relay-list replacement and quiet-window carry-forward; lines 806–976, 3799–3848 | [Baseline current edges](../architecture/analytics-detail.md#baseline-event-and-social-facts), [incremental registry](../architecture/derived-read-publication.md#assertion-registry-and-incremental-computation) | Retain list removal, markers and immutable winner source. Share unchanged partitions instead of copying every prior row or showing only authors active in the last interval. |
| D-E1 exact global/social per-kind fact keys; lines 977–1040 | [Baseline facts](../architecture/analytics-detail.md#baseline-event-and-social-facts) | Retain meaning and explicit migration mapping, change schema strings freely. Use exact event identities; archive and visible totals differ deliberately. |
| D-E5 provenance-conditional relay facts; lines 28–40, 1041–1200, 2231–2241 | [Deriver capability policy](../architecture/derived-read-publication.md#deriver), [claim target](../architecture/analytics-detail.md#third-party-claims-and-local-observation-precedence) | Retain missing-capability suppression for actual edge-dependent targets. Global/social and monitoring-claim facts still operate without provenance. Disabling future collection does not erase earlier evidence. |
| D-E1 NIP-85 subject×fact assembly; lines 115–127, 1201–1450 | [Registry](../architecture/derived-read-publication.md#assertion-registry-and-incremental-computation), [subject product](../architecture/analytics-detail.md#subjects-and-the-meaning-of-ranking) | Retain generic typed definitions and all supported subject families, source/algorithm identity and exact units. A new wire subject is not authority to manufacture a score or ambiguous parser. |
| Same logical fact key with conflicting values; lines 1009–1033, 1235–1245 | [Immutable generations](../architecture/derived-read-publication.md#ownership-and-durable-progress), [source identity](../architecture/data-and-durability.md#serving-layout-and-exact-answers) | Retain refusal/diagnosis. Neither equal-version argMax nor highest numeric value arbitrates conflicting facts. Byte-identical physical retries may collapse by immutable identity. |
| D-E3 COMPLETE-only source/run contract; lines 301–536, 2840–3126, 4103–4236 | [Manifest ownership](../architecture/derived-read-publication.md#ownership-and-durable-progress) | Retain atomic publication, explicit target outcomes and frozen dependencies. Separate target generations allow independent useful work; empty, intentionally suppressed, failed and absent remain different. |
| Watermark ordering and deterministic replay; lines 301–536, 3570–3716 | [Source cuts](../architecture/derived-read-publication.md#ownership-and-durable-progress), [serving incarnation](../architecture/data-and-durability.md#serving-persistence-and-incarnation) | Change event-time watermark/run-name mechanics to source stream/epoch vectors and dirty ranges. Older-source rebuild completion cannot roll back source progress. Event-time window and timestamp aliases are not durable ingestion identity. |
| D-E2/D-E8 columnar-native/no application LIMIT rule; lines 21, 120–121, 1230–1279, 3127–3329 | [Resource envelope](../architecture/derived-read-publication.md#configuration-semantics-and-resource-envelope), [incremental cost](../architecture/protocol-evolution.md#incremental-computation-and-honest-complexity) | Retain complete bounded processing, cost refusal and replay correctness. Reject a universal SQL-only/AST/FINAL prohibition as a product requirement. Partitioned/streaming/external-memory Rust execution is allowed when it fulfills the contract. |
| Source projection can lag the main body table; lines 1247–1279 | [Serving frontier](../architecture/data-and-durability.md#serving-layout-and-exact-answers) | Retain consistent-cut requirement. Change the old permanent base-table-only restriction: a proven compatible projection may be selected from measured cost; a fast lagging projection may not certify complete facts. |
| D-E4/D-B9 retention and progressive recomputation; lines 2840–3126, 4378–4387 | [Generation retention](../architecture/derived-read-publication.md#ownership-and-durable-progress), [archive reclamation](../architecture/data-and-durability.md#archive-publication-and-reclamation) | Retain finite derived artifacts, last-usable/input/reader pins, recovery and recompute ownership. Reject automatic low-rank/original-event pruning. Immutable evidence history and optional artifact retention are separate. |
| Orphan cleanup can misclassify a run absent from a bounded registry listing; lines 2840–3126 | [Manifest/pin ownership](../architecture/derived-read-publication.md#ownership-and-durable-progress) | Retain exact reference/registry checks before deletion and atomic visibility changes. A candidate-list limit is not proof of orphanhood; expired readers receive explicit restart. |
| Targets, intervals, max source work, duration, continue-on-error, retention and health; lines 120–121, 3570–3716 | [Deriver configuration/lifecycle](../architecture/derived-read-publication.md#deriver) | Retain effective independent execution and observable backlog; config selection must stop the actual computation. Replace old ineffective/max-window knobs with byte-bounded work and source-aware invalidation. |

### Relay evidence, propagation and historical queries

| Source requirement and locations | Current owning contract | Disposition and rationale |
|---|---|---|
| D-E6/D-E9 threshold/window candidates, exact set statistics, qualifying edges/components; lines 537–627, 1790–2001 | [Overlap detail](../architecture/analytics-detail.md#relay-overlap-and-the-adjusted-endpoint-count) | Retain common interval, 0.99/10k/24h initial analysis, candidate/edge/component evidence and bounded work. Reject identity merging and transitive all-pairs claims. Candidate approximation has explicit coverage; refusal is not successful empty output. |
| L4 full-window coverage exclusion is not mathematically lossless; lines 1790–2070 | [Overlap eligibility](../architecture/analytics-detail.md#relay-overlap-and-the-adjusted-endpoint-count) | Preserve the distinction between coverage eligibility and sound fixed-set size pruning. Identical partial mirrors may be excluded, so an exhaustive claim names its narrower eligible scope. |
| D-V4 per-relay acquired coverage and adjusted fleet count; lines 2002–2206 | [Adjusted endpoint count](../architecture/analytics-detail.md#relay-overlap-and-the-adjusted-endpoint-count) | Retain observed count and same-dependency component deduction over covered members only. Change historical true-distinct/upper-bound wording: approximate similarity is not a real-server identity census. Valid no-edge, cost refusal and disabled now differ. |
| D-D4/D-E7 third-party support/conflicts/freshness/source keys; lines 2207–2450 | [Claim detail](../architecture/analytics-detail.md#third-party-claims-and-local-observation-precedence) | Retain reports versus distinct supporters, per-attribute identity, local recency precedence, fresh/stale basis and full contributors. Split historical transition support from current disagreement; replace arbitrary lexical tie winner with contested alternatives. |
| Local and imported records do not collide; ReplacingMergeTree reversal; lines 600–675, 2207–2450, 4337–4350 | [Admission and immutable corrections](../architecture/data-and-durability.md#admission-transaction), [claim detail](../architecture/analytics-detail.md#third-party-claims-and-local-observation-precedence) | Retain complete conflicting evidence and duplicate-safe continuous aggregates. No versionless raw replacement silently discards disagreements. SourceRecordId survives replay while logical minima are reduced after the cut. |
| D-V1 global propagation buckets plus per-event views; lines 38, 675–706, 2520–2839 | [Propagation detail](../architecture/analytics-detail.md#propagation-aggregates-and-historical-evidence) | Retain relay-count distribution, p50/p95/max spread, single-endpoint/widest summary and bounded per-event lookup. Change seconds-only storage to explicit ms and precise quantile binding. Observer/path/import strata prevent false causal propagation claims. |
| D-V2 advertised-but-unserved evidence and ratio guard; lines 675–706, 2520–2839 | [Selective availability](../architecture/analytics-detail.md#selective-availability) | Retain direct scoped evidence, distinct numerator/denominator, unknown outcomes and initial 20-classified-ID display threshold. Replace old peer/global-coverage subtraction with explicit requested-set classification. A failure does not prove censorship intent. |
| Safe-tail/global-cursor subtraction created impossible or false coverage; lines 2520–2839 | [Selective availability](../architecture/analytics-detail.md#selective-availability), [coverage semantics](../architecture/protocol-evolution.md#coverage-is-not-availability) | Reject mixing event-created cursor and current observation time. Pin actual probe/access evidence; subtracting the safe tail cannot establish remote body availability. |
| D-V3 historical availability/frontier protected from run GC; lines 118, 2520–2669, 2840–2860 | [Historical evidence](../architecture/analytics-detail.md#propagation-aggregates-and-historical-evidence), [full archive](../architecture/derived-read-publication.md#ownership-and-durable-progress) | Retain event/endpoint evidence timelines and named historical reads. Reject exact start/stop-serving inference from first_seen plus current retention. Preserve direct evidence beyond disposable generations; estimators disclose unknown intervals. |
| Per-event versus global R7 ownership split; lines 88, 703–706, 2544–2554, 3549–3558 | [Read resources](../architecture/derived-read-publication.md#resource-registry-and-query-meaning), [propagation detail](../architecture/analytics-detail.md#propagation-aggregates-and-historical-evidence) | Retain separate precomputed global resources and ID-required drilldown. A public full-P scan is not a substitute for either. No old DDL filename is treated as an existing new implementation. |

### Ranking, publication and evidence quality

| Source requirement and locations | Current owning contract | Disposition and rationale |
|---|---|---|
| Current kind-3 graph, kind-10000 mute relations, source/target-only vertices, self/duplicate filtering and pinned join; lines 3717–3906 | [Ranker](../architecture/derived-read-publication.md#ranker), [graph detail](../architecture/analytics-detail.md#graph-state-is-different-from-provenance-state) | Retain exact current graph and canonical ordered adjacency. Change ownership restriction forbidding an incremental edge target: persisting current adjacency avoids rescanning all historical p-tags on each cycle. |
| Curated seed source, empty/absent seed behavior, duplicate diagnostics; lines 3907–3925, 4103–4236, 4359–4376 | [Seed detail](../architecture/analytics-detail.md#seed-availability-optional-eligibility-and-curation) | Retain explicit editorial ownership and no unseeded fallback; partial usable weights and missing coverage are now defined. Reject shipping an unexplained inherited single anchor; duplicate seed configuration errors instead of implicit votes. |
| Inverse-PageRank-assisted candidates and human curation; lines 3911–3919 | [Curation disposition](../architecture/analytics-detail.md#seed-availability-optional-eligibility-and-curation) | Change to optional on-demand diagnostic with declared algorithm and cost; no automatic trust anchors or mandatory background computation. Useful only when an operator elects to curate with it. |
| Deterministic seeded diffusion, damping, outdegree dilution, dangling mass, mute negatives; lines 78, 145–158, 3330–3569 | [Ranker](../architecture/derived-read-publication.md#ranker), [seed/reference definition](../architecture/analytics-detail.md#seed-availability-optional-eligibility-and-curation) | Retain deterministic reference and explicit initialization/residual. Change ambiguous negative edges to versioned same-author muted-follow suppression; broader penalties require separate threat-model evidence. |
| Sybil rings, one-hop trusted target and seed-poisoning promises; lines 145–158, 3330–3569, 4360–4372 | [Attack validation](../architecture/analytics-detail.md#required-discriminating-validation) | Retain discriminating topologies, zero disconnected trust and comparative measurements. Reject universal poisoned-seed bounds and a claim every ring member falls as 1/N; maximum, total and mean behave differently. |
| `min_indegree_gate`, default 0; lines 4103–4202 | [Eligibility detail](../architecture/analytics-detail.md#seed-availability-optional-eligibility-and-curation) | Retain as optional publication selection, applied after raw diffusion and before eligible percentile normalization. It never prunes the archive/graph and is not sybil resistance. |
| Relay-quality components, graph independence and missing basis; lines 3926–4102 | [Relay Ranker](../architecture/derived-read-publication.md#ranker), [subject meanings](../architecture/analytics-detail.md#subjects-and-the-meaning-of-ranking) | Retain explicit operational components, local path/method and missingness. Change to use-case capabilities, robust RTT and interval-aware availability; coverage weight starts at zero. Old universal NIP-count/RTT-ceiling weights are not required. |
| Coverage must be nonnegative capped bonus; mirror credit and scored-population normalization; lines 3968–4057 | [Relay Ranker](../architecture/derived-read-publication.md#ranker), [overlap detail](../architecture/analytics-detail.md#relay-overlap-and-the-adjusted-endpoint-count) | Retain no penalty from unknown acquired coverage and no mirror deduction from a per-relay score. Any future bonus must define its population/normalization and cannot be shrunk by irrelevant unscored rows. Coverage-off default avoids an unsupported comparison. |
| Monitor popularity neutrality, quantization and deterministic accumulation; lines 3995–4048 | [Claim policy](../architecture/analytics-detail.md#third-party-claims-and-local-observation-precedence), [validation](../architecture/analytics-detail.md#required-discriminating-validation) | Retain per-observer influence and precisely stated numerical equality. Do not inherit updateMean as an untouchable Go symbol; validate the chosen formula and emitted precision with discriminating/randomized fixtures. |
| O-F3 technical emission guards, per-half noemit reasons and stored algorithm pins; lines 125, 4103–4236 | [Ranker](../architecture/derived-read-publication.md#ranker), [publication boundary](../architecture/derived-read-publication.md#signing-and-publication-delivery-boundary) | Retain complete compatible input, seed/quality withholding, algorithm/version/viewpoint and immutable signed outbox. Reject an obsolete procedural owner-approval gate; signing authority is already a separate configured boundary. |
| Actual producer algorithm rejected by publisher while retired fixture passed; lines 4218–4235 | [NIP-85 publication](../architecture/derived-read-publication.md#nip-85-publisher), [validation](../architecture/analytics-detail.md#required-discriminating-validation) | Retain mandatory real Ranker→persisted manifest→publisher journey and exact subject encodings. A literal fixture for an obsolete algorithm is not integration coverage. |
| Ranker output outlives pruned Deriver lineage; lines 4073–4075, 4103–4236 | [Manifest pins](../architecture/derived-read-publication.md#ownership-and-durable-progress), [frozen assertion](../architecture/derived-read-publication.md#nip-85-publisher) | Retain self-contained publication artifact/source hashes and protected reproducibility dependencies; never silently join a different latest Deriver run to explain old scores. |
| Relay novel-rate collection proposal unapproved; lines 4352–4358 | [Curation/novelty disposition](../architecture/analytics-detail.md#seed-availability-optional-eligibility-and-curation) | Reject as an initial collection/ranking feature. A future proposal needs explicit coverage-policy value and starvation evidence, not preservation for its own sake. |

## Historical measurements: what is worth carrying forward

The following are **reports in the frozen historical README**, primarily for
ClickHouse 26.5.1.882 in August 2026. They were not rerun in this task and are not
benchmarks of Rust, the selected new durability path or a billion-event corpus.
Their value is the workload distinction and the counterexample to a cheap slogan.

| Frozen source and fixture | Reported observation | Required design implication |
|---|---|---|
| Lines 1247–1279: 2M events over 12 months, 13 base parts/17 time-index parts | One-hour base read 163,930 rows/5.94 MiB versus 8,190/131.68 KiB through the time index; with 60 interleaved parts the advantage fell to about 4× | Benchmark window width relative to partition width and part fragmentation, while proving compatible source frontiers |
| Lines 1265–1279: exact distinct IDs in the same fixture | 228 IDs used 1.22 MiB; 163,934 used 24.19 MiB; 1,999,999 used 192.44 MiB | Changed-workload/group cardinality determines state. The source's 100M→10–15 GiB estimate was extrapolation, not a measured capacity result or a rule for all engines |
| Lines 1458–1480: 3.6M fact rows/1.2M subjects | In-order aggregation changed 1.51 GiB→192.6 MiB on 3 parts; 946→876 MiB and about 3× slower on 57 parts | Compare layouts under the same aged workload. Reject the unsupported earlier 917→143 MiB figure and any always-on performance claim derived from it |
| Lines 2002–2070: 1M/500-relay/16-part and 3M/1,500-relay provenance fixtures | Reverse-order coverage reads had higher peak memory than forward order in these runs, despite a real in-order aggregation plan | An asymptotic aggregation-state argument is not measured total memory; include read buffers and prove each reverse-index benefit by query family |
| Lines 2290–2324: 1M observations, 500 relays, 7 monitors, 13 parts | Backfill in-order aggregation reduced about 945→488 MiB but increased about 698→4,252 ms; one-hour mode had 2.69 MiB either way | Genesis and normal incremental runs need separate envelopes; lower peak memory can purchase much more CPU/wall time |
| Whole-list and graph-input history, lines 806–976, 3717–3906 | Winner-restricted reads save materialization under high churn but can scan more; one old current list can defeat timestamp pruning across historical p-tags | Measure H historical tag volume versus E current edges, low/high churn, old winners and graph construction/loading; retain incremental adjacency |

The current [capacity programme](../engineering/capacity-and-benchmarks.md) already
requires billions of events and tens/hundreds of billions of provenance facts,
realistic skew, exact admission, both provenance orderings and recovery reserve.
The supplement adds the missing independent graph/vector dimensions. For example,
100 million vertices require 1.6 GB just for two float64 vectors; 100 billion
social edges would require 400 GB for 4-byte neighbors alone. These are declared
layout arithmetic, not achieved compression ratios. P=100 billion does not imply
E=100 billion. Neither number fits by invoking a generic 1 GiB worker reservation.

Cheap additive/current-state updates remain incremental. Exact distinct sets,
quantile correction/expiry, high-fanout pair updates, component splits, score
normalization and global diffusion have different state/cost classes. Optional
actor-trust-weighted recommendations add their own reverse dependency and broad
invalidation costs. The design now records those differences rather than promising
that every assertion costs only the latest event batch.

## Practical validation preserved and strengthened

The source's useful testing lessons are requirements on evidence, not an
instruction to reproduce its accumulated review rituals. Current owners are
[service acceptance](../architecture/derived-read-publication.md#practical-acceptance-evidence),
[protocol acceptance](../architecture/protocol-evolution.md#implementation-and-release-acceptance)
and the [new workload cases](../architecture/analytics-detail.md#required-discriminating-validation).

| Historical failure class | Concrete future witness |
|---|---|
| Deterministic garbage from window-only current graphs | Quiet window with unchanged old lists retains the complete graph; a later winner changes only its whole contribution |
| Boundary tests whose equal total masks different members | Put interval endpoints under different authors/subjects and assert exact membership, including event time versus observation time |
| Merges-stopped fixture accidentally contains no physical duplicates | Verify physical versus logical source cardinality first; replay through real admission/projection paths and inspect actual target values |
| Driver type, binary/hex or alias mistakes invisible to fakes | Use real Rust driver binds and independently inspect source→fact→score→wire identity, without a test normalizer accepting the very malformed output being tested |
| Score batch and manifest agree because both came from the same in-memory object | Round-trip the stored manifest/schema, validate the actual emitted batch and require actual publisher acceptance under that artifact |
| Old test retired while later prose claimed it discharged a boundary | Keep a concrete entry-point/fixture/output evidence map. Real-engine cost exhaustion/refusal and cancellation must actually execute; a fake short result is insufficient |
| Same-time conflicts arbitrarily lost; crowd selects old local reading | Preserve contradictory immutable records and inspect actual attribute/current/history API outputs under asymmetric source fixtures |
| Sparse graph or single seed creates a false security claim | Empty/absent/partial seed states, topology-labeled sybil tests and total/max/mean distributions, including seed compromise and increasing outdegree |
| Engine plan wins only on fresh tiny data | Few/many parts, cold/warm cache, high/low churn, genesis and late repairs; report scan bytes, buffers, temporary space and mixed-load tail latency |
| Config and metrics tested only by constructing unused functions | Packaged Deriver/Ranker entry points, actual target disablement, independent stops/backlog/catch-up and scrapeable metrics |
| TTL removes fixture inputs before assertions | Use controlled clocks or explicit time anchors according to the real retention contract, and assert source population; do not blindly copy fixed historical dates |
| A reference check accepts less scope after its parser changes | Inspect actual source inventory and concrete requirements, not only count matching labels or broadened text patterns |

The old closing ledger, lines 4237–4551, contains live-at-the-time findings,
later corrections and retired artifacts. In particular its three service/metrics
wiring gaps are explicitly marked closed by the September 6 correction. This
review does not restate them as current Go defects. Old Go coverage percentages,
mandatory mutation/review counts, schema script numbers, syntax-only house forms
and shell-gate mechanics are not new Rust requirements. Their durable lesson is
to validate the actual reachable behavior, supported migration and observed
failure boundary with evidence that can distinguish the error.

## Remaining implementation obligations and review boundary

The exact selected contracts recovered by this review are written, rather than
left as unnamed recommendations. Implementers still owe measured physical plans,
parser/profile schemas, real resource refusal/spill behavior, protocol fixtures,
end-to-end generation/publication evidence and billion-scale representation and
recovery measurements. These are validation and implementation work, not a claim
that historical repeated tests already certify the fresh design.

The coordinating review linked the analytics supplement into its owning service,
capacity and execution stages. No product behavior is implemented here. Broader
NIP-85 interaction parser details, read/search/reconciliation and other phase
inventories are owned by the coordinated plan and their respective reviews; this
artifact does not claim to have audited those unrelated source bodies afresh.
