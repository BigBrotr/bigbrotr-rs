# Specification readiness and remaining closure work

Assessment date: 2026-09-17. Baseline: `78ea43b43523a90c9d9d215ef80b98456ba91242`.
This is a focused implementation-risk assessment, not another claim to have
reread every historical document or run the future product. The reviewed design
has substantial contracts and practical scenarios. It still needs the closure
work below before it can honestly be called a complete paper specification.
An implementation task saying “define” an interface does not establish that the
interface's meaning has already been decided.

The owner accepts practical decision-making where evidence can change the choice.
That does not justify postponing ordinary semantic decisions, nor does it require
inventing every private Rust function or final physical index before coding.
Product implementation remains outside this stage. See the
[confidence register](decision-confidence.md) for technology choices and costs.

## What is already decided

Preserve complete signed originals; provenance is optional; ordinary repeated
deliveries are not permanent scientific records. Acquisition, historical evidence,
current visibility, analysis and publishing have different meanings. Admission
receipts, archive commits and serving frontiers certify different boundaries.
The selected stack, roles, logical tables, API resources, named analytical
definitions and phase/task plan exist. Local disk does not imply replicated
host-loss protection. No new broker, database or service is proposed by this review.

The remaining work is to close ambiguous interactions, make public/persistent
contracts precise enough for independent implementations, and specify useful
acceptance criteria. It is not a reason to restart the architecture from scratch.

## Three kinds of decision

| Kind | Resolve when | Examples and limit |
|---|---|---|
| Semantic or persistent contract | On paper before its implementation | Visibility, exact identity/digest meaning, outcome and retry rules, input/output schemas, compatibility, retention ownership. A benchmark does not decide whether a private observation is public. |
| Empirical design selection | Define candidates, invariant, experiment and decision boundary now; choose with evidence before dependent implementation spreads | CH access paths, document layout, search adapter, row groups, native worker integration. An unavailable engine guarantee can reopen the architecture, not merely adjust a constant. |
| Local implementation detail | During the relevant code change and review | Private helper names, borrowing, module splits without new ownership, equivalent query construction. No second handwritten copy of the implementation is needed in the plan. |

An empirical deferral must identify the uncertain property, compared candidates,
fixed semantics, workload/host envelope, correctness oracle, measured costs,
decision owner/task and latest safe decision point. “Benchmark later” is not a
decision. A losing or inconclusive candidate cannot silently become the default.
If tradeoffs cross, retain an explicit workload-specific choice or the simpler
qualifying baseline; do not invent a single universal score.

## Remaining paper contracts

All eight entries are **open** at this assessment. “Open” distinguishes a remaining
decision or consolidation from missing runtime proof; it does not mean the linked
existing specification is absent. The owning P00 tasks are in
[phase 00](../implementation/00-specification.md). Those tasks update existing
normative documents rather than maintaining a competing architecture here.

### S01 — One event encountered through several visibility scopes

**Present:** A01 has one canonical body per event identity domain, an immutable
source envelope with `visibility_scope_id`, and sparse first-receive facts. A02
distinguishes source access contexts. Public derivations exclude private inputs.
**Remaining:** the body-to-visibility relationship and its acquisition transition
are not specified sufficiently to implement the same-ID cross-scope case.

Concrete counterexample: receive E through a private source, then receive the
same verified E on a public relay, with provenance off. The later observation is not an earlier-time
correction and creates no provenance edge. Decide which durable fact authorizes
public eligibility, which metadata that caller may see, and how replay reproduces
it. Returning only `AlreadyDurable` under the first private receipt is not an
explanation. Making the first private receive time public is not an acceptable
implicit consequence either. Repeat public→private and import→network orderings.

Also resolve a newer private replaceable event versus an older public event at
the same coordinate, and a private deletion request targeting a public event.
“Winner before visibility” must distinguish source population authorization from
ordinary post-winner deletion/expiry/query predicates. Otherwise an implementer
can accidentally let private input alter a public graph or suppress public data.

**Recommended direction for the closing design:** one deduplicated signed body
with separately attributable, scope-qualified eligibility facts; public reads
and reductions use their permitted input population. Evaluate against separately
scoped body records, which simplify isolation but duplicate bodies and change the
selected identity contract. This recommendation is not yet a newly selected fact
family: specify authority, fields, transitions and replay before changing tables.
Do not introduce tenants, per-delivery rows or hidden endpoint pairs to solve it.
Separate body eligibility from source metadata eligibility, including the retained
signature variant and first-admission/receive metadata.

**Closure:** update A01/envelope semantics, logical catalogs, Admit result effects,
public current-state selection and export closure together. Write a truth table
for both arrival orders, provenance on/off, policy restriction, replay and restore;
public results must disclose neither protected metadata nor private-input influence.
Decide this on paper, not through a performance benchmark. **Owner:** P00-T06;
affected implementation starts with P01-T02/P02-T02, then P05-T02/P06-T02/P08-T04.

### S02 — Persistent bytes, portable identity and compatibility

**Present:** scalar domains, qualified ordinals and versioned fact/batch digests
are described; JSON/protobuf/columnar adapters and version negotiation are planned.
**Remaining:** exact dictionary-domain allocation/encoding, digest preimages,
presence rules and compatibility outcomes need one unambiguous definition.

Specify identity allocation authority, collision checks, overflow, restore/import
mapping, immutable allocation domains and topology handoff. Select the digest
algorithm/domain/version and exact covered fields, list/map order, integer width,
byte order, string length encoding, absent/null/empty states and unknown-field
handling for facts, producer requests, archive batches and projection chunks.
Explain whether a batch digest binds semantic ordered records or prepared bytes;
physical Parquet rewriting cannot change source identity. Preserve Nostr's own
canonical ID rules separately. Define which fields bind request identity versus
retry-local transport/authority metadata. While detailed receipts exist, changed
bound payload is a conflict even if both inputs would deduplicate to the same
event. A retired request remains `RetiredRequest`; no reclaimed digest is invented.

Do not hash arbitrary protobuf serialization as the canonical fact definition.
The official [protobuf guidance](https://protobuf.dev/programming-guides/serialization-not-canonical/)
states that deterministic serialization is not canonical and recommends explicit
field-aware fingerprinting. Transport encoding and persistent identity are
separate contracts. This check does not select a new transport or custom general
serialization framework.

Close logical schema fields/keys/nullability and RPC/API required fields on paper.
Generated `.proto`, JSON Schema, OpenAPI, Rust types and engine DDL belong to their
existing implementation tasks; they must mechanically reflect those decisions.
Physical CH sort order, codecs and row-group sizes remain empirical choices.

**Closure:** small golden byte/digest examples for every digest domain; lossless
mapping examples for boundary integers, empty/absent fields and ordered tags;
version table covering old reader/new writer, unknown optional/required data and
mixed RPC peers. Refusal must precede incompatible writes. This is paper design
and bounded independent encoding verification, not a throughput gate.
**Owner:** P00-T06; consumers P01-T02/T07, P02-T02–T05 and import/export adapters.

### S03 — Cross-store effects and recovery schedules

**Present:** synced admission, receipt retirement, archive resolution, CH frontier
publication, derived checkpoints and signed outbox recovery are already described.
**Remaining:** consolidate each boundary into a transition/effect table so that
an implementer need not infer atomicity or recovery by combining distant paragraphs.

For each operation state: authority/epoch, accepted input identity, authoritative
store, atomic write set, durable acknowledgement point, external side effects,
possible timeout outcome, retry lookup, recovery owner and reclaim condition.
Distinguish “not committed” from “cannot currently determine whether committed.”
Write the transition sequences for:

1. Admission → producer checkpoint → acknowledgement → receipt GC, including a
   hole and a mixed backup cut.
2. Archive prepare → catalog commit → receipt resolution → pending reclamation.
3. Partial CH chunks → verified family set → frontier activation → restart.
4. Derived state/checkpoint → pending output → CH materialization → PG activation
   → old generation retirement, including dirty-only work without new ingestion.
5. Job acceptance/pins → cancellation or pause → restart; signed bytes → ambiguous
   send → destination resolution, including policy/temporal invalidation.

The fault schedule must include an in-flight request while its producer closes,
a paused old owner resuming, dependency recovery in a different order, and loss of
the response after each possible commit. Reference existing detailed retirement
and outbox rules rather than weakening them to generic “retry safely.”

**Closure:** each ambiguous state has one authority and a recovery action, with
forbidden observable results and an independent fixture. A small state-transition
model may expose missing interleavings; actual fsync, process exclusion and engine
crash proof still require the real adapters. **Owner:** P00-T07; P01-T06/T07,
P02-T02/T04–T08, P05-T01, P07-T03/T04 and P08-T03 implement/qualify the boundaries.

### S04 — Whole-pipeline admissibility and progress under pressure

**Present:** aggregate grants, bounded native work, destination fairness, pending
limits and byte accounting are required. **Remaining:** the reservation/transfer
order and recovery headroom need a concrete composition, not independent limits.

Trace one maximum eligible event/document through decode, verification, admission,
pending, archive, serving, export and each enabled response envelope. Account for
escaping, framing, decompression and temporarily duplicated buffers. A collected
event can intentionally exceed a particular relay response limit, but that route
must advertise/refuse it truthfully; NEG may not advertise an unretrievable body.
A supported internal pipeline must not accept a record it can never drain.

Write which resources an operation can hold while waiting for another owner;
define admission order, nonblocking transfer/retry or reserved completion capacity.
Show that archive/index progress, cancellation, producer ACK and recovery can
continue when intake is stopped. Include disk space for WAL, catalog updates,
checkpoint/receipt progress, partial files, compaction, restore and active pins.
Define configured high/low watermarks and hysteresis without claiming spare space
guarantees success on a damaged filesystem. No automatic deletion of accepted
scientific evidence is a pressure-relief mechanism.

**Closure:** one symbolic peak-memory/disk model and resolved example per supported
recipe, a wait/resource ownership diagram and fault schedule at each reserve
boundary. Numeric throughput tuning remains empirical; unsafe relationships
between configured limits must be rejected statically. **Owner:** P00-T07;
P01-T04/T05, P02-T06–T09 and P08-T08/T12 supply runtime proof.

### S05 — Role, feature and topology composition

**Present:** one image, composable roles, shared owners and several deployment
recipes. **Remaining:** one resolved matrix of configured capabilities, required
owners, transport/storage reachability and lifecycle effects across those recipes.

Cover full online, archive-only, no-provenance, filtered/social and split-role
setups. Separate a stopped algorithm from its still-required Index storage owner;
stopping acquisition cannot erase old read capability. For enabling/disabling
provenance, search or an analytical family, specify retained old data, whether a
backfill is possible/required, transitional capability state, catch-up frontier,
rebuild cost and actual readiness. Re-enabling provenance cannot invent missed
witnesses. Configuration fields need one owner, default, validation relationship,
reload class and deployment-lock policy, not repeated independent default tables.

State exactly which multi-host layout is supported: owner location, routes,
storage paths/object access, credential authority, network budget domain and
manual handoff. Shared PG and Docker do not establish shared embedded-store
ownership. Online resharding and automatic failover are not silently included.

**Closure:** expected effective configuration, capabilities and state transitions
for each recipe; one missing/stopped dependency and one feature-toggle sequence
per affected boundary. Validate a recipe graph, not every meaningless combination
of all booleans. **Owner:** P00-T08; P01-T04/T05/T07 and P08-T01/T09/T10.

### S06 — Client contracts and end-to-end user outcomes

**Present:** routes, resource IDs/row units, broad errors, typed reads, DVM profile
and management actions exist. **Remaining:** finish per-resource input/output
field tables, enums, default order/tie-break, filter operators, pagination/count
semantics and exact examples. A route catalog alone is not an API schema.

Include empty, unavailable, unsupported, budget-refused, stale, partial and
historical results; metadata integers must specify their actual lossless JSON
representation. Map domain errors to HTTP, internal RPC, CLI and Nostr lifecycle
without conflating a missing event with a disabled feature. Same names must mean
the same thing in capabilities, examples and browser labels. Define compatibility
for deployed static clients as well as API/RPC consumers.

Close cross-surface traces for first event, provenance lookup, filtered/current
read, search, explainable user/relay score, export/download, config change and
restore. Add only useful missing actions exposed by those journeys. Retain
distinct authority for public data, protected history, administration and signing.
Temporal snapshot invalidation is already selected; specify useful-client
acceptance for restart frequency before its practical comparison below.

**Closure:** complete field/operation tables and representative valid/invalid
request/response transcripts, an action/resource permission matrix, and a user
journey-to-handler/producer map. Actual OpenAPI/client generation, browser layout
and usability tests occur in implementation; no need to name private UI handlers
or fix every pixel here. **Owner:** P00-T08; P06, P08-T01–T04, P09 and P10 consumers.

### S07 — Useful workload acceptance and independent fixtures

**Present:** stress dimensions, per-task fault cases and negative controls are
substantial. **Remaining:** bind them to explicit mixed-workload manifests and
acceptance judgments so that “bounded” cannot be passed by refusing useful work.

Define workload tuples with R active/due endpoints, D deliveries, U unique events,
P new edges, M measurements, event/tag/text distributions, graph V/E, query mix,
arrival bursts, aged store size, maintenance and recovery work. Declare the host
and background-load envelope. Keep 5k/10k/50k/100k capacity experiments distinct
from a promise that all of them fit Tux. Synthetic signed data and permitted
real samples serve different purposes; an idle-socket count is not synchronization.

State provisional objectives before selecting a benchmark winner: acceptable
per-class queue age and observation interval, useful query completion/freshness,
recovery/catch-up, resource reserve and correctness. Label numerical objectives
as engineering targets, not achieved guarantees. Where no justified rate exists,
plan a calibration run followed by recorded target selection before comparison;
do not move a failed target afterwards without explanation. Include counts of
refused/cancelled/restarted work and starvation, not only latency of survivors.

Specify a compact cross-component fixture with expected event IDs, signed fields,
scope membership, cut-relative facts, current winners, graph edges/scores and
archive/export closure. Cover duplicates, late corrections, empty replacement,
unknown target, deletion, expiry, interrupted measurements and public/private
arrivals. Reuse facts across tests while keeping the oracle independent of the
production reducer. Not every test category belongs to every feature.

**Closure:** workload definitions, expected-result tables, discriminating wrong
interpretations, and an evidence map naming which real boundary each future test
must exercise. Large runs remain unperformed until executed and recorded.
**Owner:** P00-T09; P01-T08 and existing phase acceptance tasks implement the suite.

### S08 — Compatibility, protection and operator recovery

**Present:** backup closure, private management, platform targets, migration owners,
secret references and release validation are planned. **Remaining:** one supported
deployment/protection/version matrix with concrete upgrade/restore procedures.

Specify the initial receipt failure class and honest RPO/RTO objectives, what
stays operational during each dependency outage, catalog/object backup closure,
positive fencing evidence and command order for restore. Distinguish application
readiness from a restored data directory. Pin expiry, receipt retirement,
publication obligations and secret availability must survive their required
recovery horizon; purging an old job row cannot erase them.

Define supported mixed versions, drain/upgrade order, irreversible format boundary,
last safe rollback point, forward repair and minimum retained old-format fixtures.
Include cancellation/resume of a migration, disk-full recovery, corruption scope
and missing credentials. Finalize runtime artifact/platform/dependency ownership,
release signing/update authority and license/reuse disposition before distributing
affected material. Existing supported-platform choices need verification, not an
automatic expansion of the release matrix.

**Closure:** a reader can follow the proposed commands/state checks on paper and
identify every required file, authority and irreversible step. Actual clean-bundle
install, browser/tutorial execution, restore and upgrade are release evidence,
not achievable by documentation alone. **Owner:** P00-T08/T09; P02-T08 and P08–P10.

## Empirical decisions with a bounded decision point

These are not permission to defer S01–S08. Their semantic constraints, workload
and pass/refusal criteria are written first; actual integration can then decide.

| Decision | Useful experiment and compared alternatives | Latest safe decision point / response to failure |
|---|---|---|
| Actual Rust/native/engine set | Small real Rocks sync/crash, archive worker packaging/cancel, PG-backed multi-family archive/restore and CH source-cut replay tests on pinned versions | Early P01/P02 risk slices, before many consumers depend on adapters. Change binding/worker/engine if required guarantees cannot be met; no reduced durability hidden behind a pass. |
| CH serving access paths | ID-first versus chronological full body with equivalent narrow access paths; many parts, cold/warm reads, delayed chunks/merge/replay under mixed traffic | P02-T09 before release DDL activation; implement only enough candidate adapter code to decide, not two complete products. |
| Provenance inventory and document placement | Exact flat keys versus justified grouped representation; wide versus object/reference files under churn/locality/privacy/restore | P02-T09 before persistent release format freeze. Preserve times, scopes and exact identity; a smaller membership-only result is not equivalent. |
| Archive batching/compression and native resource settings | Quiet/small and busy/wide input; actual file/row groups, RSS, bytes, latency, small-file/catalog/compaction cost | P02-T04/T09 then tuning within compatible settings; unsafe limits reject, not merely run slowly. |
| Ordinary temporal cursor utility | Selected strict snapshots versus a separately specified changing-membership traversal; broad/narrow queries, dense expiry, stopped derivation, slow delivery | Early P06 read slice before DVM/browser dependence. Record completion/restart rates and policy/expiry correctness; a semantics change needs an explicit contract revision. |
| Search backend | CH baseline; compare Tantivy/OpenSearch only if a measured capability/cost deficit warrants them, with identical language/filter/ranking scope | Early P06-T06, before full search deployment/publishing its SLA. If semantics differ, label the changed capability rather than call it an equal-result speedup. |
| Graph/overlap implementation | Incremental versus independent full oracle, hub/churn/skew, complete graph construction/spill/rank and exact pair output | P05 bounded early slices before broad materialization. Budget refusal cannot masquerade as exhaustive completion; name approximate alternatives separately. |
| Sustainable fleet envelope | Combined acquisition, reads, monitoring, graph, export, compaction, backup and recovery at declared host limits | P03/P04/P05/P06/P08 acceptance before capacity claims. Reduce advertised envelope or change topology; do not delete accepted data or ignore overdue relays. |

Upstream documentation is evidence for a documented engine property, not for
BigBrotr's entire path. For example, [DuckLake transactions](https://ducklake.select/docs/stable/duckdb/advanced_features/transactions)
describe database snapshot transactions; they do not make our separate Rocks,
CH and control updates one transaction. Revalidate sources against the exact
chosen release. These current links do not retroactively upgrade earlier probes.

## Closing order and evidence required

1. P00-T06 closes S01/S02 first: identity, visibility and portable bytes affect
   almost every other contract.
2. P00-T07 closes S03/S04 against those data decisions: commit/recovery boundaries
   and actual resource transfer, with no distributed atomicity assumption.
3. P00-T08 closes S05/S06/S08: resolved recipes, client fields, lifecycle and
   supported upgrade/recovery operations. Reuse their existing owning documents.
4. P00-T09 closes S07 and the experiment specifications: independent examples,
   useful workload objectives and a latest-safe-decision point for each risk.
5. P00-T10 reconciles all affected documents and task edges, then records what is
   closed, empirically conditional, or still open. Counts of pages/tests/reviews
   do not change a decision's status.

Each closure records the former ambiguity, selected rule, rejected alternative,
affected producer/consumers, fixture/oracle and remaining runtime evidence. The
normative contract owns the rule; this register links its disposition. Historical
reviews and research observations never override a current contract silently.
Conflicting current contracts are defects to resolve, not instructions to pick
whichever file was edited last.

The paper stage is complete when these material ambiguities are closed, selected
features have coherent end-to-end contracts and discriminating acceptance plans,
and each empirical exception has the bounded decision record above. There should
be no remaining instruction to invent a core data meaning inside an implementation
task. Generated schemas and working adapters can then be built and checked against
that baseline; implementation still starts only on the owner's separate instruction.

This assessment does not certify global optimality, billion-row performance,
100k-relay concurrency, real recovery or complete API interoperability. It identifies
what evidence can establish each of those more specific claims.
