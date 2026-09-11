# Phase 05 — Incremental facts, graphs and rankings

This is an implementation plan, not implemented software or recorded test success.
Execute its checkboxes only after implementation is authorized. The selected
contracts are [analytics detail](../architecture/analytics-detail.md),
[derived services](../architecture/derived-read-publication.md),
[protocol evolution](../architecture/protocol-evolution.md),
[data durability](../architecture/data-and-durability.md) and
[capacity](../engineering/capacity-and-benchmarks.md). The
[Go walkthrough](../research/go-analytics-code-walkthrough.md) supplies inspected
counterexamples, not a Rust implementation specification or runtime certification.

All source, migration, configuration, fixture and command paths below are proposed
outputs unless already present. Pure algorithms belong in `bigbrotr-analytics`;
engine access, scheduling and commit ownership remain application modules. A
target is independently runnable, stoppable and recoverable. No graph failure may
prevent a compatible relay-fact generation from completing.

## Shared execution and evidence contract

Dependencies use the central task IDs: P01-T02 domain types, T03 verification,
T04 configuration, T05 budgets/lifecycle, T06 authority, T07 internal RPC and T08
the actual-process fixture harness; P02-T01 control state, T03 dictionaries and
documents, T04 archive, T05 serving, T06 frontiers, T07 pins and T09 physical
benchmarks. These tasks must agree with the central data/component specifications
before their dependent implementation begins.

`SourceRecordId`, `ProjectionRowId` and `SourceCut` are distinct types. Every
derived input applies its cut before resolving immutable corrections or logical
current state. Every committed target records definition/parser/config digests,
visibility domain, policy epoch, exact input cuts and compatible target
generations, time window, coverage, counts, checksums and physical artifacts.
`complete`, `empty`, `suppressed`, `failed` and `not_built` are distinct. Source
events, actual measurements and useful first-observation evidence follow the
canonical archive retention policy. Provenance-off also excludes interpreted
imported event-to-relay claims and availability-probe edges; original signed
assertion events remain complete archive events. Existing permitted evidence is
not erased when collection is subsequently disabled. Job ticks, retries, duplicate-delivery
counters and last duplicate arrival are not permanent scientific facts.

Common initial Deriver limits are current-state cadence 300 s, ordinary fact cadence
86,400 s, overlap cadence 86,400 s, input chunk 256 MiB, task memory 512 MiB,
spill 8 GiB and duration 3,600 s, admitted through P01-T05's deployment budget.
Ranker starts with its separate 1 GiB memory/16 GiB spill grant from the canonical
service table; both consume the same host envelope. These are configured bounds,
not evidence that a billion-event deployment fits.
The scheduler must request a larger approved resource class or report an
actionable capacity refusal; it must not silently sample, truncate or advance a
frontier. Changes to input policy/definition force a new compatible generation;
interval changes affect subsequent scheduling, memory changes subsequent leases.

For each task, the proposed packaged verification command is:

```sh
python3 tests/acceptance/run.py p05-tNN --binary dist/bigbrotr --compose packaging/compose/acceptance.yaml --evidence .research/implementation/p05-tNN
```

Replace `NN` with the task's two-digit number. P01-T08 supplies the driver and
records the exact packaged CLI invocations, release/config/fixture digests,
real PG/CH/DuckDB versions, process exit status, query logs, memory/spill/IO and
the independent oracle output. A scheduled `--once` invocation performs one
bounded unit; catch-up tests repeat it to a declared frontier. The driver must
use real persisted inputs and process boundaries, not call production reducers
as its oracle. Missing engines, absent tests or skipped large fixtures produce
`NOT RUN`, not success. Small exact tests and scale experiments report separate
results; no extrapolation is presented as a completed 100-billion-edge run.

## P05-T01 — Definition registry and independently committed targets

**Dependencies:** P01-T02/T04–T08; P02-T01/T04/T06/T07.

**Proposed files:** `crates/bigbrotr-model/src/analytics.rs`;
`apps/bigbrotr/src/analytics/{registry,scheduler,dirty,manifest,commit}.rs`;
`migrations/postgres/0501_analytics_control.sql`;
`contracts/analytics/v1/{definition,target-manifest}.schema.json`;
`config/examples/analytics.toml`; `docs/user/analytics/definitions.md`;
`apps/bigbrotr/tests/analytics_generations.rs`;
`tests/fixtures/analytics/generations.json`;
`tests/acceptance/cases/analytics_generations.py`.

**Boundary:** `TargetRequest { definition_id, visibility, window, source_cut,
dependency_generations, config_digest }` and a fenced ownership lease produce
either a verified `TargetManifest` plus atomic current pointer, or a typed
capacity/source/compatibility/fencing failure. Dirty entries carry a logical key,
reason and maximum cut, not an unbounded list of duplicate deliveries. Durable
states are pending → running → staged → verified → committed, with failed and
cancelled branches; cancelled work cannot commit later.

- [ ] **Step 1:** define registry entries with subject type, row identity, units,
  parser, truth class, source requirements, update/retract rules, exactness,
  absent/zero distinction, API exposure and permitted protocol bindings. Reject
  duplicate IDs and bindings that claim facts no producer supplies.
- [ ] **Step 2:** implement actual selected-target execution and dependency
  planning. Share compatible unchanged partitions; do not run every family then
  discard unselected results. Validate each dependency's cut compatibility.
- [ ] **Step 3:** persist coalesced dirty keys and a bounded overflow-to-rebuild
  marker. Execute dirty-only work even when the latest archive cut is unchanged;
  advance source progress after a successful empty/no-output interval.
- [ ] **Step 4:** stage immutable artifacts and exact touched partitions. Verify
  counts/checksums before a fenced PG transaction publishes the manifest and
  pointer. A stale worker may leave an orphan, never publish a target.
- [ ] **Step 5:** implement retry, stop, resume and retention with live pins and
  last usable generation protection. Independent targets continue after one
  fails. Configured-disabled or naturally withheld targets report suppressed;
  missing required engines, source generations or corrupt input report failed/
  unavailable and preserve an older compatible generation where policy permits.
- [ ] **Step 6:** run the proposed generation tests and packaged case. Inject
  crashes before artifact close, after file durability and before/after pointer
  commit; restart a fresh process and inspect the durable manifest ledger.

**Oracle and negative control:** a separately authored transition table expects
one committed result per logical generation. A fixture adds only a dirty key at
the same source cut, then only irrelevant events at a newer cut. Both must finish
with the correct progress. Disable one selected target in the configuration and
assert its engine query never ran; merely checking absent output would miss the
historical failure. A poisoned incomplete artifact must not become current.

## P05-T02 — Current-state headers, membership and visibility effects

**Dependencies:** P05-T01; P01-T03; P02-T03/T05/T06.

**Proposed files:** `crates/bigbrotr-analytics/src/current_state.rs`;
`crates/bigbrotr-model/src/current_state.rs`;
`apps/bigbrotr/src/analytics/current_state.rs`;
`migrations/clickhouse/0502_current_state.sql`;
`contracts/analytics/v1/current-state.schema.json`;
`config/examples/analytics.toml`; `docs/user/analytics/current-state.md`;
`crates/bigbrotr-analytics/tests/current_state.rs`;
`tests/fixtures/analytics/current_state.json`;
`tests/acceptance/cases/current_state.py`.

**Boundary:** complete verified events at a cut produce independent
`CurrentHeader { coordinate, winner_event_id, parser_status, member_count,
visibility_reason }` and child memberships plus exact contribution removals and
additions. Headers exist for valid empty lists and invalid winning content;
children never serve as proof that a winner exists. Missing `d`, empty `d` and
malformed coordinates retain their defined distinction.

- [ ] **Step 1:** implement coordinate and winner selection over full relevant
  history: greatest `created_at`, then lowest event ID. Do not apply search,
  member, deletion or expiry predicates first and thereby resurrect old values.
- [ ] **Step 2:** parse profile metadata, follow/mute and relay lists as whole-list
  replacement. Preserve signed list ordering separately from set membership;
  deduplicate logical edges without changing the source event.
- [ ] **Step 3:** implement matching-author deletion effects, pending unknown
  targets and timestamp-driven expiry. An `e` deletion has no target-time cutoff;
  an `a` deletion applies only through the request's timestamp. Deleting a
  deletion request does not undo its effect. Originals remain archived.
- [ ] **Step 4:** persist changed-coordinate headers and old/new contribution
  sets together. Schedule expiry without new arrivals; replay late older events
  and immutable corrections under the same deterministic cut rules.
- [ ] **Step 5:** expose separate raw-current, normal-visible and authorized
  historical views. Policy restrictions invalidate the relevant public
  generation before any downstream graph computation or serving.
- [ ] **Step 6:** run unit and packaged tests with shuffled imports, restarts,
  empty and malformed replacements, equal-time ties and deletion before target.

**Oracle and negative control:** a simple fixture oracle sorts the entire small
coordinate history independently, applies the written policy and enumerates
members. New empty follows followed by an older nonempty backfill must remain
empty. Filtering an expired winner before sorting must deliberately fail the
fixture. Read archive bytes after logical deletion to prove retention, and
normal relay/API views to prove suppression. Timestamp zero is a real value,
not the representation of a missing horizon.

## P05-T03 — Exact event, author and activity statistics

**Dependencies:** P05-T01/T02; P02-T03/T04/T05.

**Proposed files:** `crates/bigbrotr-analytics/src/{counts,activity,topics}.rs`;
`apps/bigbrotr/src/analytics/event_facts.rs`;
`migrations/clickhouse/0503_event_facts.sql`;
`contracts/analytics/v1/event-facts.schema.json`;
`config/examples/analytics.toml`; `docs/user/analytics/event-facts.md`;
`crates/bigbrotr-analytics/tests/event_facts.rs`;
`tests/fixtures/analytics/event_facts.json`;
`tests/acceptance/cases/event_facts.py`.

**Boundary:** admitted unique event IDs and visibility deltas produce scoped
archive counts, visible counts, per-kind/author-kind counts, distinct authors,
first eligible signed post time, UTC-hour histograms and signed-topic counts.
Every integer has a checked range and unit; JSON transports decimal strings when
needed for losslessness. Observed corpus activity is not global Nostr activity.

- [ ] **Step 1:** register separate historical-acquired and normal-visible
  definitions. Record count/sum for mergeable means; retain exact distinct-key
  state or reference counts rather than summing daily distinct counts.
- [ ] **Step 2:** define baseline topic extraction as distinct literal nonempty
  signed `t` values per eligible event, with UTF-8 bytewise tie breaking and no
  hidden NLP/case-folding. Keep top-five projection separate from full topic
  facts. Any later normalization is a different definition.
- [ ] **Step 3:** persist the 24 UTC-hour event histogram and earliest eligible
  authored post timestamp. Late older posts update the minimum; a local first
  observation is separate evidence. Select the versioned active-hours summary
  over the previous 30 completed UTC days: with at least 20 eligible authored
  posts/replies, choose the shortest non-wrapping integer-hour interval containing
  at least 80% of their histogram mass, ties by earliest start. Emit start 0–23
  and exclusive end 1–24; otherwise withhold the summary. Expose histogram,
  sample count, window and corpus coverage so this heuristic is inspectable,
  not presented as the person's timezone or an observed network-wide schedule.
- [ ] **Step 4:** implement exact insertion/retraction contributions and rolling
  half-open `[begin,end)` buckets, including expiry with no ingestion. Update
  only touched author/kind/time partitions, not every month between two edits.
  Apply unsigned numeric window eligibility before any engine/calendar cast.
  Preserve timestamp zero and u64 maximum in originals and non-calendar facts;
  an unsupported calendar projection is explicitly excluded/labelled, never
  wrapped, narrowed or allowed to crash an unrelated target.
- [ ] **Step 5:** implement empty/unknown/overflow outcomes and source capability
  suppression. A missing predecessor generation triggers bounded reconstruction,
  not guessed subtraction or negative counts.
- [ ] **Step 6:** run the packaged case with duplicate deliveries, authors across
  many days, far-apart touched months, visibility changes and a timestamp-zero
  post; compare a full rebuild with incremental output at the same cut.

**Oracle and negative control:** independently enumerate fixture event IDs and
sets in Python and use hand-calculated hour/topic tables. The same author on two
days counts once in a two-day distinct query; the same event delivered to ten
relays still counts once. A fixture intentionally makes sum-of-daily-distincts
and broad min/max partition rewrites produce different answers or excess IO.
Active-hour fixtures include 19/20 samples, two equal shortest intervals,
midnight-spanning activity and a day expiring without new arrivals; a circular
interval or local-time conversion must disagree with the selected UTC oracle.
Include u64-maximum signed time outside the 30-day window and a supported epoch
boundary inside it; casting before numeric eligibility must fail the fixture,
while original full-event round-trip and archive counts remain exact.

## P05-T04 — Interaction targets, engagement facts and report claims

**Dependencies:** P05-T01/T02/T03; P01-T03.

**Proposed files:** `crates/bigbrotr-protocol/src/relations.rs`;
`crates/bigbrotr-analytics/src/{interactions,reports}.rs`;
`apps/bigbrotr/src/analytics/interactions.rs`;
`migrations/clickhouse/0504_interaction_facts.sql`;
`contracts/analytics/v1/interaction.schema.json`;
`config/examples/analytics.toml`; `docs/user/analytics/interactions.md`;
`crates/bigbrotr-analytics/tests/interactions.rs`;
`tests/fixtures/analytics/interactions.json`;
`tests/acceptance/cases/interactions.py`.

**Boundary:** `Subject = Pubkey | EventId | AddressableCoordinate |
ExternalIdentifier` stays tagged through storage, API and publication. Parsed
references produce facts with source event, relation role, target domain,
validation status and parser version. Event interactions and distinct actors
are different units. A report is an author's claim, never an adjudication.

- [ ] **Step 1:** freeze a supported parser matrix and protocol fixtures for
  NIP-10 marked/deprecated positional replies, NIP-22 root/parent comments,
  quotes, NIP-18 reposts, NIP-25 reactions and NIP-56 reports. Retain unknown
  input rather than invent a relation. Comment root and immediate parent are
  separately named; paired address/version references cannot double-count one
  logical interaction in a single definition.
- [ ] **Step 2:** specify each counter's eligible kinds, marker precedence,
  duplicate-tag rule, self-interaction rule and reaction polarity/content rule.
  Keep all reactions, positive/negative classes and distinct reaction authors as
  separate facts. Do not discard a known fact merely because no rank exists.
- [ ] **Step 3:** implement unresolved-reference state and a bounded reverse
  dependency index for facts requiring the target's author or coordinate.
  Target arrival or correction dirties only indexed dependents; no whole-event
  history join on every new target.
- [ ] **Step 4:** maintain per-source-event contributions and per-subject/actor
  reference counts so deleting one of an actor's two reactions leaves one
  distinct actor. Apply visibility before building public aggregate inputs.
- [ ] **Step 5:** keep unsupported/ambiguous references observable and separate
  from zero. Register reporting-author, category, target and current/historical
  counts without negative trust weights by default.
- [ ] **Step 6:** run packaged fixtures with direct root reply, nested reply,
  comment on an addressable version, quote+reply, repeated tags, late targets and
  authored deletion; compare source-event contribution records to expected facts.

**Oracle and negative control:** use static expected relations authored from the
protocol examples and an independent set aggregator. A fixture must fail if all
`e` tags are treated as replies, if an address+version pair is counted twice, or
if actor counts equal event counts. Verify API facts and eventual NIP-85 inputs
through real serving tables, not a helper that the publisher never consumes.

## P05-T05 — Zap attestations and exact amount accounting

**Dependencies:** P05-T01/T04; P01-T03; P02-T03.

**Proposed files:** `crates/bigbrotr-protocol/src/zaps.rs`;
`crates/bigbrotr-analytics/src/zaps.rs`;
`apps/bigbrotr/src/analytics/zaps.rs`;
`migrations/clickhouse/0505_zap_facts.sql`;
`contracts/analytics/v1/zap-facts.schema.json`;
`config/examples/analytics.toml`; `docs/user/analytics/zaps.md`;
`crates/bigbrotr-analytics/tests/zaps.rs`;
`tests/fixtures/analytics/zaps.json`; `tests/acceptance/cases/zaps.py`.

**Boundary:** a verified NIP-57 receipt plus parsed request/invoice and retained
issuer evidence produces a classified attestation with exact millisatoshis,
sender/recipient/subject bindings and issuer-validation status. It does not
prove that BigBrotr observed Lightning settlement. Missing historical issuer
evidence remains unresolved, not accepted by consulting today's mutable URL.

- [ ] **Step 1:** define receipt/request signature, invoice amount, description
  binding, recipient and target validation and a versioned issuer evidence
  policy. Separate syntactically valid, issuer-unresolved and accepted receipts.
- [ ] **Step 2:** implement exact checked msat sums and receipt/payment-identity
  deduplication with explicit conflicting-attestation handling. Preserve source
  receipt IDs so deduplication does not erase archive evidence.
- [ ] **Step 3:** implement contributions for sent/received counts and totals for
  the eligible subject domains. Classification changes retract the old accepted
  contribution before adding the new one.
- [ ] **Step 4:** bind current NIP-85 integer-satoshi fields only when the exact
  aggregate is divisible by 1,000; otherwise omit that optional field and expose
  precise msats in the API. Do not silently round each receipt or the total.
  For average amount/day use the previous 30 completed UTC days, storing exact
  accepted-msat numerator and denominator 30 calendar days; do not divide by only
  active days. Emit integer sats/day only when the numerator is divisible by
  30,000. Report acquisition coverage separately; absent days are not evidence
  of a globally inactive sender or recipient.
- [ ] **Step 5:** bound invoice parsing and nested request bytes and forbid
  arbitrary URL resolution in analytical jobs. Resolve issuer evidence through
  already admitted, attributed inputs or an explicit acquisition task.
- [ ] **Step 6:** run the packaged case with 999+1 msats, 999 alone, conflicting
  invoices, repeated receipts and unavailable historical issuer evidence.

**Oracle and negative control:** independent invoice decoding plus static
receipt-binding expectations distinguish a 1,000-msat accepted total from two
incorrect per-receipt rounded values. A valid receipt signature with a mismatched
description must not count. Altering the current issuer endpoint cannot change
a previously pinned generation without new explicit evidence and a new cut.
Include an active-day versus calendar-day denominator fixture and a 30,000-msat
window whose exact published daily mean is one satoshi.

## P05-T06 — Monitoring facts and source-aware claim resolution

**Dependencies:** P05-T01; P02-T03/T04/T06; acquisition monitoring and import tasks.

**Proposed files:** `crates/bigbrotr-analytics/src/{monitor_facts,claim_resolution}.rs`;
`apps/bigbrotr/src/analytics/monitor_facts.rs`;
`migrations/clickhouse/0506_monitor_facts.sql`;
`contracts/analytics/v1/monitor-facts.schema.json`;
`config/examples/analytics.toml`; `docs/user/analytics/monitor-facts.md`;
`crates/bigbrotr-analytics/tests/monitor_facts.rs`;
`tests/fixtures/analytics/monitor_facts.json`; `tests/acceptance/cases/monitor_facts.py`.

**Boundary:** immutable local measurements and imported claims carry endpoint or
non-dialable subject, observer identity, publisher identity, method/auth/network
scope, observed time and source ID. Derived per-field decisions retain the
winning basis and opposition. A local observer UUID is not a Nostr signing key;
distinct keys are not proof of independent people or hosts.

- [ ] **Step 1:** expose sanitized documents, DNS/TLS/HTTP/NIP-11 and successful
  RTT measurements with separate capture times, units and declared-versus-tested
  capability status. Null, failure, unsupported and zero remain distinct.
- [ ] **Step 2:** implement the 24-hour eligibility window and latest eligible
  claim per observer/context. Conflicting values at one observer's same time
  remain contested; do not settle them by event-ID ordering.
- [ ] **Step 3:** prefer the latest eligible local measurement for the same
  context. Optional imported resolution uses distinct eligible observer keys,
  preserves tied alternatives and labels single/corroborated/contested. Do not
  majority-vote across incompatible methods or continuous RTT samples.
- [ ] **Step 4:** distinguish historical transitions from simultaneous conflict;
  cap public examples at 64 after resolution, never before voting. Retain actual
  measurement facts under the canonical evidence policy, not transient job logs.
- [ ] **Step 5:** implement bounded late/corrected-observation invalidation and
  independent target execution when the social graph is absent or broken.
- [ ] **Step 6:** run a packaged observer fixture with one recent local result,
  many imported aliases, same-time contradictions, stale results and private
  auth scopes; inspect both private history and public projection.

**Oracle and negative control:** a handwritten decision matrix predicts each
field's outcome. Counting repeated deliveries or capping input claims before
resolution must change the test and fail. A failed RTT observation must never
appear as a successful 0-ms sample or be republished as a local witness.

## P05-T07 — Provenance-derived coverage and propagation statistics

**Dependencies:** P05-T01; P02-T03/T04/T06; acquisition coverage/evidence contracts.

**Proposed files:** `crates/bigbrotr-analytics/src/{coverage,propagation}.rs`;
`apps/bigbrotr/src/analytics/propagation.rs`;
`migrations/clickhouse/0507_propagation_facts.sql`;
`contracts/analytics/v1/propagation.schema.json`;
`config/examples/analytics.toml`; `docs/user/analytics/coverage-propagation.md`;
`crates/bigbrotr-analytics/tests/propagation.rs`;
`tests/fixtures/analytics/propagation.json`; `tests/acceptance/cases/propagation.py`.

**Boundary:** exact compact provenance facts, immutable corrections and coverage
intervals produce per-event endpoint-set evidence and per-stratum propagation
statistics. Preserve observer/auth/clock/acquisition class; backfill first
receipt is not live propagation delay. No permanent duplicate-arrival log is
needed for earliest useful observation evidence.

- [ ] **Step 1:** compute `t(event,endpoint)` from eligible first-observation
  evidence at the source cut, then `k = |endpoints|` and spread `max(t)-min(t)` in
  milliseconds. Count excluded/no-evidence events separately; singleton spread
  zero means one observed endpoint, not zero network delay.
- [ ] **Step 2:** expose acquired coverage intervals, traversal progress and gaps
  with endpoint/filter/context identity. EOSE, peer overlap and import completion
  are scoped evidence, never global completeness certificates.
- [ ] **Step 3:** maintain per-event spread contributions and exact frequency or
  ordered state needed for nearest-rank p50/p95/max. Corrected minima retract the
  previous value; do not average percentiles or use merge-only sketches for
  deletions. Optional sketches require separately named approximate definitions.
- [ ] **Step 4:** partition rolling windows with explicit half-open boundaries,
  bounded spill and deterministic recomputation when noninvertible state is
  unavailable. Account for the full quantile input state, not only its output.
- [ ] **Step 5:** make provenance-off-from-creation suppressed. If collection is
  disabled later, retain usable older evidence and expose new interval gaps;
  never fabricate continuation or erase existing scientific evidence.
- [ ] **Step 6:** run packaged incremental and full-rebuild jobs with backdated
  minima, two clock strata, many physical parts, missing coverage and corrected
  observations; compare exact bucket multisets before comparing percentiles.

**Oracle and negative control:** Python sorts an independently enumerated small
spread multiset and selects `ceil(p*n)` (one-based). A late earlier observation
must move the correct percentile; an average-of-batch-percentiles implementation
must fail. Provenance-disabled input still produces unrelated event counts while
propagation reports suppressed, not a fabricated zero-valued generation.

## P05-T08 — Selective availability as bounded observations

**Dependencies:** P05-T01/T06/T07; acquisition attempt/evidence contract.

**Proposed files:** `crates/bigbrotr-analytics/src/availability.rs`;
`apps/bigbrotr/src/analytics/availability.rs`;
`migrations/clickhouse/0508_availability_facts.sql`;
`contracts/analytics/v1/availability.schema.json`;
`config/examples/analytics.toml`; `docs/user/analytics/selective-availability.md`;
`crates/bigbrotr-analytics/tests/availability.rs`;
`tests/fixtures/analytics/availability.json`; `tests/acceptance/cases/availability.py`.

**Boundary:** latest finalized attempts per event/endpoint/auth/method/interval
produce served `S`, explicit completed-not-served `N`, and unclassified `U`.
Timeout, unfinished traversal and authentication refusal are unclassified, not
proof that a relay lacks an event. Historical attempts remain inspectable.

- [ ] **Step 1:** define attempt finality and result classification, including
  completed empty exact-ID requests, explicit rejections and contested outcomes.
  Keep target selection and observation coverage in the definition.
- [ ] **Step 2:** compute `N/(S+N)` only with at least 20 classified attempts and
  report `(S+N)/(S+N+U)` when its denominator is nonzero. Expose all counts and
  insufficient-sample status; a missing denominator is not zero.
- [ ] **Step 3:** maintain old/new finalized-attempt contributions with interval
  expiry and immutable corrections. Distinct target counts and attempt counts
  are separate; repeated delivery of one measurement cannot increase either.
- [ ] **Step 4:** restrict public aggregates to eligible public contexts before
  aggregation. Do not infer censorship from missing peer overlap or publicize
  private authentication subjects in examples.
- [ ] **Step 5:** run the packaged case with 19 versus 20 classified targets,
  late finalization, timeout→served correction, duplicated receipt and policy
  restriction. Inspect the input IDs supporting every numerator/denominator.

**Oracle and negative control:** static S/N/U sets determine the fractions. A
timeout-only relay must yield insufficient evidence, never 100% non-service.
Removing U from the classified-fraction denominator is a deliberately failing
negative control. API wording must describe observed selective availability,
not a verdict about relay intent.

## P05-T09 — Relay overlap and similarity components

**Dependencies:** P05-T01/T07; P02-T05/T09.

**Proposed files:** `crates/bigbrotr-analytics/src/{relay_overlap,components}.rs`;
`apps/bigbrotr/src/analytics/relay_overlap.rs`;
`migrations/clickhouse/0509_relay_overlap.sql`;
`contracts/analytics/v1/relay-overlap.schema.json`;
`config/examples/analytics.toml`; `docs/user/analytics/relay-overlap.md`;
`crates/bigbrotr-analytics/tests/relay_overlap.rs`;
`tests/fixtures/analytics/relay_overlap.json`; `tests/acceptance/cases/relay_overlap.py`.

**Boundary:** endpoint event-ID sets from the same 24-hour universe, visibility,
source cut and acquisition context produce exact intersection/union/Jaccard
facts and threshold components. Defaults: Jaccard at least 0.99 and at least
10,000 shared events. These are similarity hypotheses, not endpoint identity.

- [ ] **Step 1:** define eligible set membership and coverage metadata. Use
  `min(|A|,|B|)/max(|A|,|B|)` as a sound upper bound for threshold pruning.
  Empty-set handling and unknown acquisition coverage must be explicit.
- [ ] **Step 2:** implement partitioned exact intersection for the bounded
  eligible profile with admitted candidate/IO limits. Complete the declared
  profile or refuse it; never emit the first N pairs as a complete result.
- [ ] **Step 3:** persist changed endpoint memberships and affected candidate
  pairs; deletions/expiry can split components and require recomputation of the
  affected old component. Stable component IDs hash definition, scope, window
  and sorted member identities, not worker iteration order.
- [ ] **Step 4:** expose member minimum incident-edge Jaccard separately from
  any all-pairs component claim. Compute the documented coverage-adjusted
  observed count without claiming a population bound or census. Never merge
  endpoint identities or disable acquisition because a component formed.
- [ ] **Step 5:** keep approximate candidate discovery a separately enabled,
  separately named profile with candidate-recall limitations, even when each
  candidate is later verified exactly. No extra mandatory sketch engine.
- [ ] **Step 6:** run exact small and physical many-part fixtures, abort a job
  mid-pair enumeration, remove a bridge and rerun incrementally. Report eligible
  pairs, pruned pairs, verified pairs, spilled bytes and refusal conditions.

**Oracle and negative control:** independently calculate complete small set
intersections and graph components. A chain A–B–C whose A–C similarity fails the
threshold must not be labelled an all-pairs-equivalent group. A low candidate
budget must yield capacity refusal, not a plausible-looking partial component
list. Billion-scale acceptance reports measured pairs/IO and extrapolation
separately; quadratic work is not hidden by calling the result incremental.

## P05-T10 — Incremental social adjacency and bounded graph artifacts

**Dependencies:** P05-T01/T02; P02-T03/T07/T09.

**Proposed files:** `crates/bigbrotr-analytics/src/{graph,csr}.rs`;
`apps/bigbrotr/src/analytics/graph.rs`;
`contracts/analytics/v1/graph-manifest.schema.json`;
`migrations/clickhouse/0510_social_adjacency.sql`;
`config/examples/analytics.toml`; `docs/user/analytics/graph.md`;
`crates/bigbrotr-analytics/tests/graph.rs`;
`tests/fixtures/analytics/graph.json`; `tests/acceptance/cases/graph.py`.

**Boundary:** current follow/mute headers and members at a compatible cut produce
versioned current adjacency and immutable rank-input CSR shards. Pubkeys are
vertices, not people. Valid empty-list authors and relation endpoints establish
vertices; malformed/deleted headers alone do not. Retain isolated vertices after
self-edge or same-author mute suppression.

- [ ] **Step 1:** maintain exact per-author adjacency replacement from header
  contributions. Retain mute evidence separately; baseline rank edges remove an
  author's self-follow and followed pubkeys that the same author publicly mutes.
  Reports, money and popularity do not silently become edge weights.
- [ ] **Step 2:** build a deterministic dense ordinal map and sorted adjacency
  with checked 32/64-bit ordinals and 64-bit offsets. Supply bounded external
  sort/join rather than a whole-history scan or one random lookup per edge.
- [ ] **Step 3:** publish immutable graph artifacts with vertex/edge counts,
  byte sizes, source generations, mapping checksum and source-policy identity;
  pin them across rank computation and release only through manifest ownership.
- [ ] **Step 4:** estimate and admit vertex map, offsets, adjacency, buffers,
  rank vectors and spill together. Distinguish archive events H, social edges E,
  evidence/provenance P, vertices V and simultaneous viewpoints K. Two float64
  vectors alone cost 16V bytes; 100 million vertices require 1.6 GB for these.
- [ ] **Step 5:** implement stop/resume at shard boundaries and explicit corrupt,
  missing, incompatible and over-budget outcomes. A small adjacency delta does
  not imply local-only TrustRank recomputation.
- [ ] **Step 6:** run graph fixtures through packaged acquisition/import,
  current-state derivation and artifact generation; independently decode the
  resulting on-disk graph, then repeat with incremental author replacements.

**Oracle and negative control:** a separate fixture builder enumerates vertices
and edges, including isolated authors, duplicate follows, mute suppression and
empty winners. A fixture with millions of immutable input fragments must reveal
per-fragment/query overhead. A synthetic ordinal boundary must reject overflow
without allocating billions of entries. An estimator excluding rank vectors
must fail the budget test.

## P05-T11 — Seeded pubkey trust and explicit ranking viewpoints

**Dependencies:** P05-T01/T10; P01-T04/T05; P02-T07/T09.

**Proposed files:** `crates/bigbrotr-analytics/src/{trustrank,normalization,curation}.rs`;
`apps/bigbrotr/src/analytics/ranker.rs`;
`migrations/clickhouse/0511_rank_scores.sql`;
`contracts/analytics/v1/rank-generation.schema.json`;
`config/examples/ranker.toml`; `docs/user/analytics/rankings.md`;
`crates/bigbrotr-analytics/tests/trustrank.rs`;
`tests/fixtures/analytics/trustrank.json`; `tests/acceptance/cases/trustrank.py`.

**Boundary:** pinned graph, unique finite-positive seed weights and explicit
algorithm/viewpoint configuration produce raw float64 ranks plus a separately
versioned public integer projection. A score describes that pubkey under those
seeds and data, not a person's identity or universal truth. Different viewpoints
have distinct score and publisher identities.

- [ ] **Step 1:** implement configuration checks and missing-seed behavior:
  duplicate seeds fail; empty/all-missing seeds withhold scores; partial presence
  renormalizes present weight and reports retained weight fraction. A missing or
  unreadable seed file is an error, not an empty successful run.
- [ ] **Step 2:** implement `r0=t`, damping 0.85, 100 deterministic iterations,
  dangling mass returned through t and stable reductions. Record residual,
  total mass and numerical tolerance; reject NaN/infinity/negative mass.
- [ ] **Step 3:** implement minimum indegree gate default zero after raw ranking,
  then the specified positive-rank empirical percentile with half-up rounding,
  tied values equal, zero mapped to zero and a sole eligible positive to 100.
  Record empty gating as withheld; do not substitute max-score normalization.
- [ ] **Step 4:** keep optional reverse-graph seed-curation diagnostics on demand,
  bounded and labelled diagnostic. Never auto-replace configured seeds or turn
  a curation score into a trust score. Rank filtering affects serving only.
  Select `reverse-follow-curation/1`: transpose the same eligible adjacency,
  initialize uniform mass over its fixed vertex set, use uniform teleport and
  dangling redistribution with alpha 0.85 for exactly 100 float64 iterations.
  Output raw scores and deterministic score-descending/pubkey-ascending candidates
  with source/parameter diagnostics; no automatic NIP-85 trust publication. Empty
  graph produces empty output. Its full O(I(V+E)) cost is separately admitted.
- [ ] **Step 5:** bound K simultaneous viewpoints and account for
  O(I(V+E)) baseline recomputation even after a small edge change. Checkpoint
  only reproducible iteration boundaries with the same graph/seed/config pins.
- [ ] **Step 6:** expose other subjects deliberately: relay operational scores
  come from T12; event/addressable/external subjects have engagement facts. No
  initial universal event truth score. A later optional recommendation algorithm
  needs a separate threat model, actor caps/decay, update costs and definition;
  it is not smuggled into this baseline by reusing the `rank` field.
- [ ] **Step 7:** run an independent dense-matrix reference on small graphs and
  the packaged graph→rank path under shuffled shards, restarts, seed changes and
  resource refusal; compare raw vectors before comparing integer projections.
  Include seed→directed-ring, seed→trusted-hop→ring, a trusted one-hop target,
  growing seed outdegree and compromised-seed cases. Record total attack mass,
  maximum member and mean member scores separately; no claimed universal 1/N
  or seed-poisoning bound follows from a single favorable topology.

**Oracle and negative control:** analytically solved tiny graphs and an
independent Python matrix iteration establish mass, dangling behavior and ties.
The fixture must distinguish returning dangling mass to uniform vertices from
returning it to seeds, and percentile normalization from dividing by the maximum.
Append disconnected Sybil vertices and inspect the defined effect rather than
asserting unproved Sybil immunity. Report numerical tolerances and platform
reproducibility separately from exact event/fact identity.

## P05-T12 — Independent relay operational quality

**Dependencies:** P05-T01/T06; P02-T09. This task can complete with every social
graph and rank target disabled or unavailable.

**Proposed files:** `crates/bigbrotr-analytics/src/relay_quality.rs`;
`apps/bigbrotr/src/analytics/relay_quality.rs`;
`migrations/clickhouse/0512_relay_quality.sql`;
`contracts/analytics/v1/relay-quality.schema.json`;
`config/examples/ranker.toml`; `docs/user/analytics/relay-quality.md`;
`crates/bigbrotr-analytics/tests/relay_quality.rs`;
`tests/fixtures/analytics/relay_quality.json`;
`tests/acceptance/cases/relay_quality.py`.

**Boundary:** eligible local monitoring evidence for a named read-use case and
24-hour window produces an operational component vector and optional composite.
At least five eligible observations are required for the composite. Unknown
measurements stay unknown; social trust and relay quality are not one score.

- [ ] **Step 1:** implement time-weighted availability with carry-forward limited
  to twice the monitor cadence and explicit unknown seconds. Compute median
  successful RTT and its `exp(-milliseconds/1000)` transform. Bind capability
  satisfaction to the named read-use-case requirements and evidence method.
  Select nearest-rank p50 of successful integer-microsecond durations, then
  evaluate the transform as `exp(-duration_us/1_000_000)` in float64; the median
  input, unit conversion and numeric tolerance remain visible in the manifest.
- [ ] **Step 2:** apply initial availability/latency/capability weights 0.5/0.3/0.2,
  renormalizing only available components and recording the resulting basis.
  Compare composites only with the same basis; coverage is explanatory by
  default, not a hidden zero-valued component.
- [ ] **Step 3:** implement independent window expiry, late-measurement updates
  and target commits. Insufficient historical fields suppress only the affected
  component/composite; preserve available measurements and explanations.
- [ ] **Step 4:** run the packaged quality case with a hand-authored timeline,
  missing fields, stale samples, incompatible observer scopes, late correction
  and zero-arrival expiry; compare component values before composites.
- [ ] **Step 5:** stop Ranker social computation and corrupt its graph artifact,
  then advance monitoring. Require a new relay-quality manifest while the social
  target remains failed. Restore the graph without changing that quality result.

**Oracle and negative control:** a hand-integrated timeline distinguishes
time-weighted availability from fraction of successful probes. Unobserved hours
must reduce coverage without becoming failures. Different available-component
bases cannot be compared as one scalar ranking. A corrupt social graph must not
block relay-quality advancement.

## P05-T13 — Whole-phase catch-up, churn and physical scale qualification

**Dependencies:** P05-T01–T12; P02-T09.

**Proposed files:** `tests/acceptance/cases/analytics_system.py`;
`tests/acceptance/oracles/analytics.py`;
`tests/fixtures/analytics/system.json`;
`contracts/acceptance/analytics-evidence.schema.json`;
`config/recipes/analytics-lab.toml`; `docs/user/operations/analytics-recovery.md`;
`apps/bigbrotr/tests/analytics_lifecycle.rs`.

**Boundary:** real admitted archive/evidence and source cuts pass through each
independently owned target to exposed persisted facts and score artifacts.
Qualification reports fixture truth, recovery and measured work separately.

- [ ] **Step 1:** freeze an independently enumerated complete small fixture with
  expected facts, current headers, graph edges/raw ranks, relay values and scopes.
  Exercise actual archive→current state→facts→graph/rank→CH resources.
- [ ] **Step 2:** pause each service independently, append new input and compare
  catch-up to a clean rebuild at the same cut. Include dirty-only changes,
  no-output source progress and window expiry with no new events.
- [ ] **Step 3:** execute separate measured churn and many-part workloads with
  the P02 scale driver. Record scanned H/E/P, dirty keys, rewritten partitions,
  candidate pairs, rank iterations, memory, spill, wall time and foreground
  acquisition/read latency. Setup is outside the measurement window.
- [ ] **Step 4:** measure auxiliary-state cardinality: dirty keys, reverse
  references, exact distinct refcounts, quantile input state, CSR mappings,
  simultaneous viewpoint vectors and retained generations/pins. Report bytes
  per logical unit and compaction/write amplification, not just final table size.
- [ ] **Step 5:** inject disk-full, worker kill, missing graph shard, stale lease,
  CH outage and visibility restriction. Require old compatible generations or
  explicit unavailability, never mixed-cut success. Capture real CLI/process and
  engine evidence in the task evidence directory.
- [ ] **Step 6:** collect an evidence manifest mapping each target to nonempty or
  intentionally empty expected output, independent oracle, negative control and
  tested sizes. Missing required paths are NOT_RUN, never inferred from another
  target or from historical Go tests.

**Oracle and negative control:** compare complete small outputs to independently
calculated source-event contributions and graph matrices. A deliberately omitted
producer, silent source-cut advance, broad month rewrite and rank-vector-free
budget estimate must each fail their fixture. Scale success requires measured
bounded work and recovery for the exercised sizes; small tests cannot establish
billions-of-events or 100-billion-edge capacity.

## Design review record

These are five substantive planning reviews and corrections, not executed tests.
The central review ledger records cross-phase review and remaining release gates.

| Round | Concrete finding and disposition |
|---|---|
| 1 — Product/producer coverage | Preserved empty current headers, real interaction/zap producers and fact-only publication inputs; excluded a universal event truth score (T02–T05/T11). |
| 2 — Data semantics | Distinguished infrastructure failure from healthy suppression; applied permanent useful evidence, provenance-off imported-edge exclusions and retained-generation policy (shared contract/T01/T07). |
| 3 — Crash/authority | Required dirty-only and no-output commits, exact affected partitions and fenced pointer publication; split relay quality from social qualification into T12/T13. |
| 4 — Work/state cost | Made exact quantile input, inverse references, distinct refcounts, CSR/16V vectors/K viewpoints and many-part overhead measurable; specified reverse-curation cost and all-pairs refusal (T07/T09–T13). |
| 5 — Whole-product proof | Added explicit active-hour/day-average formulas and discriminating fixtures, ring topologies, and target-by-target actual archive→CH/artifact evidence with independent oracles and NOT_RUN outcomes (T03/T05/T11/T13). |
