# Product alternatives: reads, search, optional services and operation

Status: **advisory design review; none of these alternatives is adopted**.
Baseline: `41cc381`, inspected on 2026-09-17. This report changes no existing
specification, implementation plan, owner decision or product code.

Evidence consists of static inspection of the cited repository documents and the
official sources linked below. No product, database, browser, payment node or
benchmark was run. Failure scenarios are constructed counterexamples or design
inferences, not observed production incidents. Proposed experiments remain undone.

The preserved requirements are full signed-event archival, optional provenance,
selectable roles, relay observation, useful public reads/search/ranking, private
administration, the website, and equal quality for every supported capability.
The [mandate](../planning/mandate.md), lines 58–61, explicitly distinguishes equal
quality from default enablement and implementation order. Technology choices are
not owner mandates, as its lines 34–37 also make clear.

The current plan already recognizes many of these tradeoffs. The alternatives
below change which behavior is primary, when a decision is made, or what counts
as product evidence; they are not allegations that the authors forgot every risk.

## 1. Make ordinary browsing survive unrelated expirations

**Current choice and evidence.** In [read views](../specification/read-view-contract.md),
lines 18–31 and 51–58, a view expires at the earliest relevant eligibility change,
possibly an earlier conservative partition deadline. A page under construction
is discarded and continuation restarts. The same constraint reaches DVM signing,
delivery and payment at lines 71–86. Lines 96–100 retain live traversal only as a
fallback to evaluate if the baseline proves unusable.

**Why another choice could be better.** Consider a broad ordinary event query
whose covered partition contains one event expiring every second. Even if none
appears on the first page, a covering partition deadline can repeatedly invalidate
the query while someone reads a result and requests the next page. If relevant
events themselves expire every second, an exact query-specific deadline has the
same problem. A system can be perfectly correct yet practically impossible to
browse. This is a conditional scenario, not a measurement of Nostr expiry rates.

**Recommended alternative.** Define distinct, explicitly named consumption modes
before implementation:

- **Ordinary chronological traversal:** fixed acquisition upper cut and immutable
  `(created_at DESC, event_id ASC)` keyset order, with current eligibility checked
  on each page and before delivery. The cursor also retains the private scanned
  position so skipped, now-ineligible candidates do not repeat or masquerade as
  exhaustion. No event is returned twice during a forward traversal. Expired,
  deleted and newly superseded events are suppressed; their removal does not
  invalidate every remaining row. An unavailable current suppression view still
  refuses the affected request.
- **Strict snapshot query:** retain the existing list/count equivalence and its
  explicit temporal restart behavior for clients that actually need that
  guarantee. Explain its effective lifetime before they choose it.
- **Authorized immutable research artifact:** retain the explicit historical
  resource/export semantics, source identity and manifest for reproducibility.
  Ordinary browsing never silently becomes historical access.

For the recommended ordinary traversal, an event admitted after the upper cut
does not appear anywhere in that traversal, including a backfilled event whose
authored timestamp falls behind the cursor. Refresh creates a new cut. A new
replacement admitted after the cut can suppress the previous winner without
inserting the new winner; refresh is needed to see it. A row that becomes eligible
again behind the already-scanned position may likewise require refresh. Therefore
the guarantee is stable forward ordering without repeated IDs, **not** complete
enumeration of a membership set that keeps changing. Show the acquisition cut,
eligibility evaluation time, mode and refresh affordance in metadata/UI.

Counts are separate dated evaluations of that cut under the eligibility state
used for the count. They retain exact/estimated labels, but cannot be promised to
equal the eventual number of rows delivered across changing pages. Page metadata
must not imply a shared immutable list/count result. A true continuously admitting
feed remains a separate feed contract; it is not smuggled into this traversal.

Keep restrictive authorization revocation and its current lease/barrier strict.
This alternative changes scheduled event eligibility and traversal membership,
not who may read data. It does not permit sending an expired event. [NIP-40](https://raw.githubusercontent.com/nostr-protocol/nips/master/40.md)
separates retention from ordinary delivery; its relay behavior discourages sending
expired events but does not prescribe invalidating all otherwise eligible pages.

Do not automatically generalize this mode to score order, search relevance or
NIP-77. A relevance order needs its own stable ranking/corpus contract. NEG needs
one pinned set and may still require restart when that set becomes ineligible.
The dashboard can combine independently labeled resources without pretending
that every panel shares one timeless snapshot.

**Strongest counterargument.** One strict read stamp makes cross-surface equivalence
and scientific explanation simpler. Two modes create additional client semantics,
and conservative expiry may rarely matter in realistic scopes. Keep the strict
mode; select the ordinary default only after demonstrating the benefit.

**Confidence and deciding experiment.** High confidence in the conditional
availability failure; medium confidence in changing the default. Exercise a
bounded chronological prototype with dense expiry, deletion, replacement,
backdated admission and eligibility restoration between pages. Compare completed
user traversals, restarts, duplicate IDs, suppressed outputs and scan cost against
the strict contract. Include long pauses and slow delivery. Test search and NEG
separately instead of declaring them solved by the browsing experiment.

## 2. Select search for useful answers before selecting its engine

**Current choice and evidence.** [Search semantics](../architecture/search-and-reconciliation.md),
lines 45–82, select all nonempty content in the configured scope, a specific
analyzer and a relevance tuple: whole-text equality, phrase containment, number
of distinct query terms, then recency. Lines 95–116 choose ClickHouse first and
Tantivy when the baseline is inadequate. Lines 309–315 already require measuring
usefulness and refusal rates; the alternative is to move that decision before
the baseline becomes a broad implementation commitment.

**Concrete product weakness.** For an all-terms query such as `relay reliability`,
two documents with neither whole-text equality nor the contiguous phrase have
the same distinct-term score. A recent passing mention can outrank an older,
substantive explanation entirely because of recency. That follows from the tuple;
it does not prove a particular relevance model would always do better. Indexing
opaque or machine-oriented nonempty content also incurs cost without establishing
that ordinary text-search users want it.

**Recommended alternative.** Separate the product decisions:

1. Define a versioned human-text search scope plus an explicit raw-content mode,
   preserving every original signed event in the archive. Publish the exact
   kind/content exclusions; do not silently narrow an already submitted query.
2. Choose a useful ranking policy from judged example tasks. Keep `recent` as an
   explicit sort, and evaluate a relevance model with phrase/field/length signals
   where the chosen corpus supports them. Neither BM25 nor the present tuple is
   automatically the desired product meaning.
3. Compare implementations on the **same** corpus, matching, filters, visibility
   and ranking semantics. Separately compare candidate ranking policies on user
   judgments. An engine advantage and a changed ranking policy are different
   experimental variables.

Make the competition two concrete paths initially: CH plus its admitted exact
ranking plan, and a bounded Tantivy projection with compatible metadata/body
hydration. Keep OpenSearch as a distributed-operations alternative, not a third
mandatory service. Define the selection gate before final production search DDL.
Preserve filter-before-top-K, no recent-prefix reranking, visibility-safe corpus
statistics, versioned reader publication and refusal of unbounded work.

Official [ClickHouse text-index documentation](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/textindexes)
describes inverted indexes and optional phrase-position storage; the inspected
documentation labels its position-storage option experimental. This does not
invalidate BigBrotr's existing application-token verification design or prove CH
cannot implement a better ranking policy. [Tantivy's own documentation](https://github.com/quickwit-oss/tantivy)
lists BM25, phrase queries and incremental indexing, and explicitly excludes
distributed search from its scope. These are capability statements, not BigBrotr
quality or performance measurements.

**Strongest counterargument.** CH may win decisively when structured restrictions
make candidate sets small: one existing online store, shared filter machinery,
fewer projections and recovery paths, no additional search process, and adequate
answers for the actual audience. Retain CH if it meets judged usefulness and
mixed-load cost goals. “Dedicated search engine” is not itself a benefit.

**Confidence and deciding experiment.** High confidence that the current tuple
often reduces all-terms relevance to recency; medium confidence that another
ranking/backend combination gives a worthwhile improvement. Build a small,
owner-reviewed task corpus covering known-item retrieval, exploratory questions,
common terms, older useful results, multilingual content and structured narrowing.
Record judged success/rank, refusal rate, query completion time, memory, index
bytes, rebuild/replay time and impact on concurrent ingestion. Predetermine the
decision criterion from those tasks rather than accepting whichever engine was
implemented first. No result of that experiment is claimed here.

## 3. Keep DVM optional, but split transport value from subscription and payment scope

**Current choice and evidence.** [The DVM profile](../specification/dvm-profile.md),
lines 27–43, selects a custom 5999/6999/7000 application binding with no generic
client-compatibility promise. Lines 97–132 add list/count, subscription, renewal
and cancellation; lines 193–273 add durable lifecycle and expiry; lines 275–358
add invoice creation/reconciliation and manual refund obligations. Each next page
is separately budgeted and, in paid mode, independently purchased. The existing
profile correctly makes free operation independent of LND at lines 402–404.

**Concrete product weakness.** A user pays for a search page whose view becomes
temporally ineligible before relay delivery. The correct current outcome may be
a failed query and a manual refund obligation. That is a substantial operator
workflow to support an interface requiring a custom client in the first place.
An optional flag prevents accidental activation but does not reduce development,
documentation, recovery or support obligations for a shipped capability.

**Recommended alternative.** Preserve a supported optional Nostr query interface,
but plan separately versioned capabilities with independent completion evidence:

- A small finite-query profile backed by a real client journey: the exact
  resource operations that a Nostr-only consumer needs, using the common read
  core, signer and outbox. Preserve encryption when that journey needs it.
- Subscription/renew/cancel as an optional extension only after demonstrating
  what durable signed batches provide beyond the existing ArchiveRelay feed.
- Paid delivery as an optional separately qualified capability. Preserve it in
  the product plan where required, but require a concrete buyer, charging unit,
  delivered-result definition and workable refund journey before freezing the
  invoice state machine as the only selected design.

For paid use, compare payment-per-page with a finite named report/artifact whose
declared historical semantics and delivery obligation are understandable. Such
an artifact still requires explicit history/exposure authorization; it cannot
keep ordinary expired content deliverable by calling it a report. Another valid
outcome is retaining prepaid ordinary queries after evidence shows expiry and
refund rates are acceptable. This is not permission to remove promised paid
support without an explicit product decision.

The inspected current [NIP-90](https://raw.githubusercontent.com/nostr-protocol/nips/master/90.md)
labels itself unrecommended and directs implementers toward use-case-specific
standards. That supports reconsidering generality; it does not establish that
every DVM use is unhelpful or that a particular microstandard already exists.

**Strongest counterargument.** A single general profile avoids proliferating
incompatible micro-protocols and exposes all BigBrotr resources consistently.
Nostr-native consumers may genuinely need private paid queries and durable
subscriptions. The existing plan is unusually careful about financial ambiguity.
Keep that rigor whenever payments ship, and do not replace the profile with an
equally bespoke but less specified protocol.

**Confidence and deciding experiment.** High confidence that this is a distinct
product/support investment; medium confidence that narrower capability gates are
better. Before implementation selection, run an independently written client
journey specification for one finite query, one continuation and one cancelled
subscription. For payments, map a late payment, lost delivery and refund through
both customer and operator screens. Then perform the existing isolated regtest
qualification only for the chosen paid contract. Require demonstrated utility
alongside transport/state-machine correctness.

## 4. Make a small observatory a first-class deployment outcome

**Current choice and evidence.** [The topology matrix](../architecture/system-design.md),
lines 177–191, places CH in the full observatory and removes online read/derivation
roles in archive-only mode. [First launch](../engineering/configuration-and-runtime.md),
lines 94–103, starts the full collection/index/derive/rank/read/admin set. The
[dashboard plan](../product/dashboard-and-website.md), lines 151–167, interprets
equal product importance as a coherent complete canonical launch while still
supporting custom roles. [PLAN](../../PLAN.md), lines 63–77, makes CH the complete
online serving representation.

**Concrete product weakness.** An operator who wants a useful relay directory,
recent health evidence and archived measurements must choose the large online
stack or an archive-only mode with no online data view. Turning Ranker or search
off does not remove the serving engine dependency. The recipe therefore makes
roles selectable, but the smallest useful observatory remains coupled to a
large fraction of the data platform. This is a document-derived dependency
observation, not a measured claim that CH is too expensive.

**Recommended alternative.** Define supported deployment outcomes and their exact
resource contracts before assigning every public resource to an engine:

- **Observe relays:** archival evidence, latest directory/health/coverage summaries,
  private management and the corresponding usable dashboard pages. Evaluate a
  small typed current-fact projection in the already required PostgreSQL, with
  bounded read credentials/pools separate from control commands. Historical
  analytical queries remain unavailable unless their actual engine is enabled.
- **Collect and export:** the existing full signed archive and optional provenance,
  with useful private inspection and export workflows.
- **Complete online product:** rich history, structured reads, full text, analytical
  generations and ranking, with CH or the serving choices that pass their gates.

These are transparent recipes in the same product/image; no event loses signed
fields and no capability falsely claims to work without its dependencies. Do not
create a second canonical measurement archive in PG: a current-fact projection
has an explicit source, rebuild procedure and publication/cut semantics.

Make default process placement concrete for each recipe. Preserve a responsive
administrative listener while archive/graph/native workers are terminated or hit
their memory limit; retain a compact combined mode with an honest shared-failure
description. The system document already distinguishes process isolation at
lines 113–125. The alternative is a shipped, tested topology guarantee, not
another paragraph claiming logical service independence.

**Strongest counterargument.** A second serving implementation adds migrations,
projection ownership and semantic parity work; reads can contend with critical
PG control transactions. CH may be cheap and simple enough on supported hosts
that this optional topology saves little. Avoid duplicating billion-event
serving or historical analytics in PG. The proposed scope is deliberately small.

**Confidence and deciding experiment.** Medium. Compare the existing complete
recipe with a bounded current-observatory projection at declared fleet sizes.
Measure clean installation steps, idle/active memory, time to first useful relay
answer, current-projection write/read contention and recovery effort. Stop CH,
kill a heavy worker and break PG separately; record exactly which UI/CLI actions
still work. Reject the extra projection if it fails to materially simplify the
operator journey or harms control-plane reliability.

## 5. Give each configurable field one durable authority

**Current choice and evidence.** [Configuration](../engineering/configuration-and-runtime.md),
lines 5–20, defines defaults → recipe → file → managed overrides → environment →
CLI. [Private administration](../product/private-administration.md), lines 40–59,
adds versioned PG overrides, locked deployment fields, expected revisions and
per-role application outcomes. These are coherent correctness mechanisms, but
the six-layer resolution model becomes part of routine operator reasoning.

**Concrete product weakness.** An operator edits a mounted TOML value, restarts,
and sees a PG managed value continue to win; another field is locked by an
environment variable. The UI can explain every outcome correctly while the
operator still needs a precedence investigation to make a simple lasting change.
A backup/restore that restores managed state against a different deployment file
can recreate the same confusion. This is possible under the documented rules,
not a claim that the preview currently hides the sources.

**Recommended alternative.** Retain a small deployment-root layer for storage,
listeners, secret providers and hard security constraints. For ordinary product
policies, choose one explicit durable mode per instance or field group:

- **Managed:** recipe/file values seed a versioned desired-state document once;
  UI and CLI subsequently edit that authority using the existing compare-and-set
  revision and desired/applied model. Export it as a safe declarative artifact.
- **Declarative:** the operator-owned document remains authoritative; UI shows
  status/previews and can produce a proposed patch or export, but does not create
  an invisible competing override. CLI changes use the declared workflow.

Treat emergency environment/CLI overrides as explicit runtime locks with a
visible nonpersistent status, rather than a normal permanent editing path for
every policy. Recipes are initialization inputs, not a second live authority.
Switching modes is a typed previewed import/migration with restart behavior,
conflict handling, audit and backup semantics; it must never silently discard
operator changes. Secret references and deployment security roots retain their
existing protections.

**Strongest counterargument.** Layered configuration is familiar to experienced
operators and supports immutable infrastructure well. Explicit management modes
can themselves become complexity, especially when different groups need different
owners. Keep the current layering if ordinary users can predict and repair it;
do not replace a documented model with unexplained magic.

**Confidence and deciding experiment.** Medium-high that one clear authority will
help routine operation; unmeasured implementation cost. Before coding, walk
fresh setup, UI edit, file edit, restart, rollback and restore with concrete
configuration examples in both models. Ask operators to predict the effective
value and make a lasting change without internal help. Record wrong predictions,
steps and recovery success. Choose the simplest model that supports the actual
deployment workflows, while preserving the current revision and security checks.

## 6. Organize the complete product around decisions, not the resource inventory

**Current choice and evidence.** [The interface catalog](../specification/interface-catalog.md),
lines 35–55, promises a broad typed resource registry. [Dashboard pages](../product/dashboard-and-website.md),
lines 305–325, mirror many resource families; its lines 334–387 already describe
good journeys. [PLAN](../../PLAN.md), lines 108–124, retains service-oriented
phases and completes browser administration and the website late; lines 179–185
record 121 tasks/746 steps. Those counts show breadth, not a defect by themselves.

**Concrete product weakness.** A relay-selection user may have to understand
directory filters, measurement windows, availability strata, overlap scope and
score comparability before copying an appropriate relay URL. A page can render
each definition correctly yet fail to help that user decide. Similarly, a large
capability registry can be technically rich while an API integrator needs several
queries to answer one ordinary question. The current journeys reduce this risk
but do not establish that users can complete them.

**Recommended alternative.** Preserve all required capabilities and technical
resource APIs, but plan complete, bounded user tasks as the primary integration
units: choose a relay; inspect and verify an event; find useful content; explain
a score; produce a cited research result; install/restore an instance. For each,
define the user's decision, minimum necessary evidence, expected useful output,
unavailable-state action and optional deeper evidence. Use existing resources
through a thin composition layer where necessary; do not invent another generic
query engine or a mandatory new service.

For relay selection, for example, expose explicit needs and constraints first,
show a small comparable set with the evidence needed to choose, then make
methodology, missing evidence and raw observations inspectable. Do not produce
an automatic optimizer or hide unknown values inside a universal score. For
ranking, ask users to explain what changed between viewpoints rather than merely
verify that two numerical values render.

Move a concrete CLI/browser/docs task specification and prototype into the work
that defines its resource contract. The existing plan already starts thin slices
early; make success at the decision the gate that can revise resource shape or
page composition before the broad endpoint/page inventory becomes expensive to
change. Equal quality still applies to every shipped area, including the complete
website and private administration. An initial task slice cannot declare the
whole product complete.

**Strongest counterargument.** The current taxonomy makes a powerful research
explorer and supports expert users with unanticipated questions. A task-led UI
can overfit a few examples or hide useful raw capabilities. Keep advanced tables,
typed resources and source downloads; simplify the main path, not the evidence.

**Confidence and deciding experiment.** High confidence that task completion is
better product evidence than surface count; medium confidence in any particular
navigation/composition proposal. Compare paper or isolated static prototypes of
the existing page flow with the proposed task flow. Use the same evidence and
states, including missing/stale data. Observe whether relay operators, application
integrators and researchers can complete their task, explain limitations, and
recover without developer intervention. No browser/user study was run here.

## Boundaries worth retaining

The separate public data, private management and static website artifacts are
well motivated. A public page's lack of a management menu is not authorization;
server-side action checks remain necessary. The current static artifacts also
avoid an operator Node runtime. [The repository design](../engineering/rust-and-repository.md),
lines 21–23 and 36–52, already keeps reusable Rust libraries small and application
modules private instead of forcing one crate per service.

Do not collapse SvelteKit and Astro/Starlight merely to reduce a framework count.
The [frontend rationale](../product/dashboard-and-website.md), lines 95–112,
assigns interactive instance applications and authored documentation different
jobs. [SvelteKit's static adapter](https://svelte.dev/docs/kit/adapter-static)
and [Starlight's documentation tooling](https://starlight.astro.build/getting-started/)
support that division. Shared tokens, schemas and a small client library are a
reasonable boundary; a universal frontend framework would need a demonstrated
maintenance or user benefit. The more consequential complexity is the lifecycle,
read semantics and operator authority exposed through those frontends.

Suggested decision order: settle read consumption modes and configuration
authority on paper; define the user/search task corpus; compare the small
observatory and search candidates; qualify DVM extensions against actual consumer
journeys. These are bounded decision gates, not an invitation to postpone every
choice until implementation or to add another round of speculative features.
