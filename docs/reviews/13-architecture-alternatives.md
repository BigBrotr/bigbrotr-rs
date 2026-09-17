# Architecture challenge and recommended decisions

Date: 2026-09-17. Reviewed baseline: `41cc381`.
Status: **advisory recommendations for discussion, not adopted specifications**.
The selected architecture and open P00 closure tasks remain unchanged. This
document must not be implemented as an overriding second specification.

## Judgment and limits

The plan has a coherent foundation, but there are consequential choices I would
change or reopen before implementing it. Its strongest properties are preservation
of signed evidence, explicit ownership and recovery, scope-aware reads, bounded
work, and separation of scientific facts from operational state. Its greatest
remaining design risk is paying too much coordination, repeated acquisition,
materialization and client complexity for guarantees that not every operation
needs. A different framework alone would not remove these costs.

The previous [readiness assessment](../planning/specification-readiness.md) asked
whether an implementer could infer incompatible behavior from incomplete
contracts. For example, an event first acquired privately and later publicly needs
precise visibility and first-observation rules. This review asks a different
question: even if the contracts were complete, would another behavior or physical
architecture make a better product? Both questions matter. Resolving ambiguity
does not establish that the chosen solution is best.

Three independently tasked AI reviewers examined storage, runtime/analytics and
product/interfaces with separate task contexts. They are not independent human
consultants, and agreement is not external certification. Their reports contain
baseline locations, counterarguments, confidence and deciding experiments:

- [Storage alternatives](13-storage-alternatives.md): five findings.
- [Runtime and analytical alternatives](13-runtime-alternatives.md): six findings.
- [Product and interface alternatives](13-product-alternatives.md): six findings.

The coordinator read all three reports, challenged their proposed semantics and
reviewed relevant owning contracts and official documentation. This is a targeted
choice review, **not another complete reread of every repository file**, an audit
of engine source, or a production benchmark. No product was implemented or run.
The [arithmetic evidence](../research/evidence/design-alternatives-arithmetic.json)
records parameter-derived examples, not measured capacity. A percentage of
"perfection" would be invented: competing goals and unmeasured workloads prevent
such a score.

## Decisions I recommend changing on paper

### 1. Spend acquisition work on explicit discovery obligations

With steady arrivals and complete independent linear sweeps, the current hot
window contributes `1h / 15m = 4` historical deliveries per new event; the warm
window contributes `48h / 1h = 48`. Together that is approximately 52 historical
deliveries plus the live delivery. This assumes all due windows complete without
coalescing or Negentropy savings; it is not a measured public-network rate.
Refusing or deferring work lowers traffic but also changes achieved coverage.

I recommend capability-aware Negentropy and a linear fallback with explicit,
nonoverlapping age bands and different revisit cadences. Preserve fair service
for every endpoint and report actual discovery delay/gaps. Coalesce requests only
when their completed scope and timing satisfy both obligations. Do not infer a
safe historical cutoff from observed timestamps alone: unseen backdated events
are precisely the difficult case. Choose cadence from a late-arrival fixture and
a declared delay/cost tradeoff. This changes the service guarantee honestly; it
does not promise 52-fold savings at identical detection latency.

### 2. Distinguish an already-retained fact from a new mutation

Ordinary duplicate deliveries currently avoid permanent scientific duplication
but still create durable request outcomes and acknowledgement/reclamation work.
Group commit already exists; adding batching is not the missing idea.

I recommend specifying a separate read-only `CertifyExisting` operation, then
qualifying it before adoption. It checks the actual signed input and every
required effect against current durable ownership. It returns an existing durable
proof or requests normal admission. A new witness, scope, first-receive correction
or other effect must still mutate. This operation has no mutating request-ID
conflict promise; ordinary `Admit` keeps that promise unchanged. Producer progress
still needs durable checkpointing and a valid relationship to the retained proof.
Do not create a new proof ledger that merely recreates the work being removed.

This is promising because it reduces writes that follow all deliveries rather
than genuinely new facts. It cannot eliminate network traffic, parsing, required
signature validation or all exact inventory reads. Its state-machine/backup
proof comes before its throughput comparison.

### 3. Make ordinary traversal usable under changing eligibility

The current strict view can invalidate continuation when a relevant event expires,
or earlier under a conservative partition deadline. A broad result with frequent
expiry could force repeated restarts while a person reads the first page.

I recommend ordinary chronological browsing with a fixed acquisition upper cut,
immutable keyset ordering and current eligibility checked before delivery.
Suppression removes affected rows without restarting the whole traversal. A new
admission, including backdated content, appears only after refresh; a newly
admitted replacement can suppress the old event without entering the old view.
The cursor tracks scanned positions, including skipped rows. Traversal promises
no repeated event IDs, but not complete enumeration of changing eligibility.
Counts are dated evaluations, not a promise to equal all eventual pages.

Retain strict snapshots and authorized immutable research exports for clients
that require their semantics. Keep authorization revocation strict in every mode.
Do not automatically apply chronological traversal semantics to relevance sorting
or reconciliation: those need their own pinned inputs. NIP-40 permits retained
history while discouraging ordinary expired-event delivery; it does not require
restarting all otherwise eligible pages. See the official
[expiration specification](https://github.com/nostr-protocol/nips/blob/master/40.md).
The cost is another explicit client contract, so test the two modes with actual
expiry/replacement/slow-client sequences before selecting the ordinary default.

### 4. Rank for an explained purpose

For the selected relay formula, 80% measured availability and 20 ms latency yield
about `0.8676`, while 99.9% and 500 ms yield about `0.8518`. The formula deliberately
trades reliability for speed; calling its ordering general quality hides that
preference. I recommend leading with coverage-qualified availability, latency,
freshness, capabilities and evidence, then named use-case filters/orderings.
Retain a versioned scalar as an attributed opinion where useful, not a universal
fact or a reason to discard archive content.

For social trust diffusion, define a new production profile by its mathematical
target and a stated numerical error contract. Keep the existing fixed-100-pass
profile as a reference if useful. A residual-based stopping rule and compatible
warm starts can reduce repeated work, but residual evaluation costs a graph pass
and near-tie rankings need separate treatment. Global percentile publication is
an explicit scheduled computation. Do not call an approximate ordering exact or
overwrite an existing algorithm version with changed semantics.

For relay overlap, first group exactly equal scoped sets, then evaluate sound
similarity-join filters and exact verification. Preserve the public exact pair
relation through compact classes and lazy expansion where possible. A spanning
forest alone does not preserve that relation. At 100,000 endpoints there are
4,999,950,000 possible pairs; if all qualify, enumerating them is inherently
quadratic. Neither Rust nor a graph database removes the output-size lower bound.

### 5. Align process boundaries with actual ownership

Select one acquisition/socket owner per host and route domain as the normal
placement. Monitor, validation and synchronization retain separate policies and
queues while sharing locally owned sessions and limits. This avoids making
cross-process socket-permit recovery a prerequisite for a normal installation.
Use bounded local queues and process/job budgets; isolate native and heavy jobs
where termination or fault containment requires it. Do not promise per-module
crash isolation when modules share a process.

Remote collectors still need real durable ownership, source cuts and recovery.
Distribute whole partitions/jobs, not every buffer credit or reducer update.
One image with composable roles remains the right packaging direction; a product
service name does not require its own crate, process or container.

### 6. Make configuration and user tasks simpler before expanding surfaces

I prefer one durable policy authority per instance, with an explicit managed or
declarative operating mode. Deployment roots, secrets and security constraints
remain separate. Files seed managed state or remain authoritative in declarative
mode; UI and CLI must not create competing hidden overrides. Keep revisions,
preview, desired/applied status and rollback. Validate restart/restore and mode
transition examples before replacing the current precedence rules. Do not begin
with arbitrary per-field ownership modes.

Integrate the complete product around tasks: choose a relay, verify an event,
find useful content, explain a score, export a cited dataset, install and restore.
Keep advanced raw resources. The recommendation changes integration evidence and
main navigation, not the requirement for the full dashboard, administration,
website and optional interfaces. A rendered table is not proof that its user can
make the intended decision.

## Consequential alternatives that need small comparative prototypes

### Archive authority and duplication

I still prefer the independent archive for BigBrotr's research/retention purpose,
but would test one serious challenger before completing its storage implementation:

| Candidate | Permanent facts | Online use | Main gain and cost |
|---|---|---|---|
| A: selected archive-oriented design | DuckLake-managed Parquet after synchronized admission | CH bodies/indexes and derived generations; DuckDB archive jobs | Independent portable rebuild source and archive-only use; two body layouts and archive maintenance |
| B: retained CH source authority | Immutable CH source tables after synchronized admission, plus protected backups | Source/derived CH data and explicit Parquet exports | Removes continuous DuckLake publication; concentrates source/serving risk and makes independent exports additional work |

Both still need Rocks exact admission/pending state, PG control, stable identities,
cross-family manifests, read cuts, safe reclamation and causally closed backup.
Candidate B is not "one database" or a transaction shortcut. Compare equal loss
guarantees, useful reads during backfill, total steady/peak bytes, restore to useful
service, and independent exports. Count backup/export copies. Choose one main
architecture; supporting both forever would multiply product complexity.

Keep DuckDB/Parquet-only operation for declared archive/offline use; it has not
demonstrated the same concurrent online service. Keep LMDB/nostrdb-style serving
as a challenger only if a relay-first access pattern exposes a measured deficit.
Adding it now would create another index, migration and recovery boundary.

The selected full-online recipe explicitly duplicates some bodies. Long-term
tiering may be worthwhile, but cold retrieval needs measured locators/manifests,
compaction-safe identity and explicit latency. A request cannot silently scan all
Parquet files. Existing tiered recipes need that qualification before being sold
as the answer to billions of events on an 8-TB host.

### Exact provenance and physical read paths

At 100 billion edges, one extra byte per edge is 100 GB before replicas. Compare
flat compact keys with grouped exact values and, only if warranted, bounded
base/delta collections. Include times, scope, stable identities, corrections and
all dictionaries. A bitmap without metadata is not an equivalent archive.
RocksDB already has an LSM; do not build another compactor without a demonstrated
total-cost gain on a store larger than cache.

Prefer engine-owned CH projections as the first candidate for simple alternate
orders, including reverse provenance. They may remove application coordination
as well as copies, but pointer projections can increase scattered reads. Verify
the actual source-cut/correction/page queries, not just point lookups. Official
[projection documentation](https://clickhouse.com/docs/concepts/features/projections/projections)
describes covering and offset-based alternatives; it does not establish their
performance for BigBrotr.

If DuckLake stays, replace blanket inlining prohibition with a qualified
per-relation policy, starting with narrow technical commit receipts. Inlining
keeps small inserts in the catalog before flushing; catalog backup and byte
budgets must therefore cover those rows. Keep scientific body-heavy tables out
until separately justified. The official
[inlining contract](https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining)
supports the comparison, not a claim that the earlier disabled-inlining probe
tested it. Transactional identity and lossless exports matter more than requiring
every technical receipt to immediately produce Parquet bytes.

### Search usefulness before engine commitment

The current all-terms score often ties on term count and falls back to recency.
It can be semantically correct while finding poor answers. Define human-text
search and explicit raw-content search scopes; preserve all originals regardless
of index selection. Compare ranking policies using judged tasks, and compare CH
and Tantivy implementations on the same semantics separately. Retain CH if it
delivers useful results with lower total cost. Do not automatically add OpenSearch
or assume BM25 is the desired product behavior. Filters, visibility, rebuild and
mixed ingestion/query costs are part of this choice.

## Proposals I would constrain or decline

| Independent proposal | Coordinator disposition and reason |
|---|---|
| Make CH the retained authority | Reopen as an early competitor; retain archive-oriented preference until equal-protection evidence overturns it |
| Duplicate certification | Pursue a separate nonmutating contract and proof first; reject silent weakening of existing mutation retries |
| Compressed provenance base/delta | Promote representation comparison; do not adopt a custom second LSM by assumption |
| Native projections and selective inlining | Prefer bounded engine-owned candidates, subject to pinned-version semantics and actual recovery/query probes |
| One acquisition owner, fewer distributed budgets | Prefer for normal host placement; keep genuinely needed isolation and remote ownership explicit |
| Age bands, similarity joins, residual diffusion | Prefer these algorithmic directions with changed cadence/version/output semantics made explicit |
| Relay evidence vector and use-case selection | Prefer as the default explanation; a named scalar can remain available |
| Ordinary traversal separate from strict snapshots | Prefer for chronological browsing; no automatic relaxation for authorization, search, NEG or historical exports |
| Search policy/engine competition | Move before broad production search implementation; CH can still win |
| Small observatory backed by PG current facts | Do not add a second public read implementation now. Test only if actual minimum-deployment cost justifies it; protect PG control responsiveness |
| Narrower DVM capability gates | Preserve optional DVM, qualify finite queries, subscriptions and payments separately; do not remove promised capabilities by review fiat |
| Single configuration authority | Prefer one explicit instance mode; resist a new combinatorial field-ownership matrix |
| Task-centered product integration | Adopt as planning direction after discussion; keep expert data exploration and the complete website |

The small-observatory suggestion is attractive but not free: duplicating current
projections and semantics in PG can cost more than operating CH. Likewise, paid
DVM is substantial support work even when disabled by default. Keep a real client
journey before selecting its payment/delivery contract. The current official
[NIP-90](https://github.com/nostr-protocol/nips/blob/master/90.md) discourages its
general model; that supports a focused review, not removing Nostr-native queries.

## Choices I would retain from macro to micro

- **Product/data:** complete signed originals, optional attributable provenance,
  preserved archival history, no ranking-based deletion, distinct import claims
  and actual observations. Scope eligibility and signature provenance must close
  S01 without leaking private first-receive metadata through a shared body.
- **Durability/security:** synchronized admission, positive writer exclusion,
  separate archive/serving/coverage frontiers, immutable correction identity and
  cut-before-reduce queries. Local disk still does not survive host loss by itself.
- **Language/runtime:** Rust/Tokio, bounded CPU work and terminable native jobs.
  Previous tuned 5k synthetic TLS measurements were close to Go; they establish
  neither full-system superiority nor 100k-fleet capacity. No extra language or
  actor framework has earned its cost here.
- **Storage placement:** PG transactional control, one owner per Rocks store,
  catalog-aware archival operations and rebuildable online derivations. A broker,
  graph database or service mesh needs a concrete requirement before adoption.
- **Libraries/package design:** small reusable domain/protocol/analytics crates,
  application modules for internal roles, maintained crypto/protocol primitives,
  narrow adapters where upstream behavior would lose evidence. Recheck pinned
  SDK issues; do not turn one observed defect into a permanent upstream judgment.
- **Interfaces:** one typed read core, separate public/private authority, static
  SvelteKit interfaces and Astro/Starlight documentation, shared contracts/tokens.
  A framework consolidation needs an actual maintenance or user benefit.
- **Representation/naming:** binary natural IDs, qualified stable ordinals,
  versioned canonical encodings, explicit units and evidence subjects. Physical
  row positions must never become scientific identity. Choose user/wire/storage
  names carefully; inventing every future private function name is false precision.

These are reasoned retained choices, not proof of optimality. Source-documented
features are not workload results, and all important foreign/native bindings
still need their actual Rust integration and recovery qualification.

## How this changes the next planning work

Resolve meaning first: acquisition obligations, observation scopes, browse modes,
rank purposes, configuration authority and DVM capability scope. Then compare
only prototypes capable of changing a consequential selection: archive authority,
exact inventory representation, search and the small engine-specific paths above.
Use the same facts, guarantees and useful workloads for each comparison. Do not
postpone semantics to implementation or run an indiscriminate benchmark matrix.

If a recommendation is adopted after discussion, update its owning architecture,
field/component/client contracts, defaults, failure behavior and corresponding
implementation steps together. Reconcile the confidence register and this report's
disposition. P00-T06–T10 already contain the paper/experiment/reconciliation work;
none is completed by writing this review, and no new arbitrary review quota is
introduced. Until then the selected baseline remains authoritative and these
alternatives remain explicit challenges to it.

Mechanical artifact and arithmetic verification is recorded in
[the check record](../research/evidence/design-alternatives-checks.json).
It checks the handoff files, not scientific usefulness, production capacity or
the superiority of a proposed architecture.
