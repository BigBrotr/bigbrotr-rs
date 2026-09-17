# Independent agent review of adopted read, scheduling and analytics decisions

Reviewed 2026-09-17 by a separately tasked agent against the root agent's current
working tree. This is an independent agent counter-review, not external human
review, production validation or review of the reviewer's own gateway changes.
Scope: read modes, revisit bands, stationary seeded trust, relay vectors/default
selection, exact overlap representation and `experiments/read-rank-contract`.
Root owns fixes. Findings below record the reviewed failure and minimal repair;
the tree was changing while findings were delivered.

## Disposition after owner repairs

Rechecked the current owning text on 2026-09-17. These are paper-contract repairs;
the implementation tasks and their engine/process acceptance remain unexecuted.

| Finding | Current disposition and checked evidence |
|---|---|
| R14-01 | Resolved: P06-T02 and the shared-read architecture authorize population before reduction and distinguish candidate/eligibility cuts. P05-T02 Boundary/Step 1 now explicitly selects the authorized visibility domain before winner reduction; its acceptance includes cross-scope replacements. |
| R14-02 | Resolved: P03-T07 and its traceability table use `sync.revisit_bands`, scheduled-time intervals and conditional delay evidence. |
| R14-03 | Resolved: corrected tie ordering and an unreturned expired candidate are exercised. Reran the probe successfully; its committed evidence content matches the corrected output. |
| R14-04 | Resolved: P05-T09 and storage-catalog rows assign verified equal-set groups/membership/group relations, bounded lazy expansion, collisions, subthreshold groups and churn. |
| R14-05 | Resolved: selector places missing latency last; P05-T12 produces the vector independently and gates Q on explicit composite enablement, with both negative cases assigned. |

A subsequent bounded cross-check covered identity/encoding, CertifyExisting,
cross-store recovery, configuration authority, maximum units and dependency
recovery against their component/storage/foundation owners. It found no further
concrete contradiction requiring a new paper decision. In particular, proof-only
request identity is explicitly reserved and invalid for Admit; source dependencies
survive receipt GC; positive writer exclusion and unknown commit outcomes remain
requirements; accepted maximum units retain completion capacity. This is not an
exhaustive review or validation of native bindings, fsync, engine cancellation or
backup recovery. The reviewer's own gateway contract was excluded from this pass.

## Findings, in priority order

### R14-01 — P1: old reduction ordering/cut language contradicts traversal

The [new read contract](../specification/read-view-contract.md#chronological-traversal)
authorizes the source population before current-winner/deletion reduction and
uses a fixed candidate cut with a newer eligibility cut. At first inspection,
[P06-T02](../implementation/06-read-surfaces.md#p06-t02--exact-event-queries-historical-policy-and-full-bodies)
instead required one cut for IDs/tags/bodies/current headers and directed winner
selection before visibility. The [shared read architecture](../architecture/derived-read-publication.md#shared-read-core)
also retained winner-then-exposure and unconditional one-cut query wording.

A private newer replacement would suppress an otherwise eligible public event
under the old reduction order. Freezing suppression at the candidate cut would
retain an old public event despite a public replacement after that cut. These
are observable membership errors, not wording preferences.

Minimal repair: consistently distinguish authorized input population from later
ordinary expiry/query filtering, and candidate cut from eligibility cut. Keep
one stamp for strict snapshots; traversal must report its separate cuts/mode.
Root's correction to P06-T02 was observed during this review. The architecture
paragraphs and P05-T02's full-history winner wording must also preserve the same
scope qualification. Existing public/private replacement fixtures should be the
acceptance oracle across both owning tasks.

### R14-02 — P2: scheduler owner still specified the retired overlapping windows

[Acquisition architecture](../architecture/acquisition-services.md#layer-algorithms)
selects `[0,1h)/15m`, `[1h,6h)/1h`, `[6h,48h)/6h`. The initially inspected
[P03-T07](../implementation/03-acquisition.md#p03-t07--schedule-age-banded-revisits-deep-and-historical-work-fairly)
Step 3 still required hot 1h/15m plus warm 48h/1h, and its configuration mapping
still named `sync.hot/warm`. Following the implementation task would therefore
restore the old overlapping schedule and its modeled repeat cost.

Minimal repair: change T07 title/Step 3 and traceability rows to explicit band
obligations frozen at scheduled time, with overdue/cost/delay evidence. Root's
updated T07 title and Step 3 were observed during review. The model `4+5+7=16`
versus `4+48=52` is arithmetically consistent for its stated idealized conditions;
it is not measured fleet throughput. The documented boundary-late-arrival and
post-48h limitations correctly avoid claiming a universal discovery deadline.

### R14-03 — P2: the small traversal oracle selected the wrong equal-time winner

The initially inspected [probe](../../experiments/read-rank-contract/probe.py)
sorted `(time,id)` with `reverse=True`, choosing the greatest event ID at an
equal timestamp. P05-T02 requires greatest timestamp then **lowest** event ID.
An independent two-event counterexample (`a` and `z`, equal time/coordinate)
returned `z`, where the contract requires `a`. The main fixture had no tie.

The only expired event was also already returned as page 1, so removing expiry
handling from continuation did not alter the claimed expiry case. This let the
probe report expiry coverage without exercising an unreturned expired row.

Minimal repair: sort `(-time,id)`, add an explicit reversed-tie negative control,
and put another expired candidate after page 1. Root made those repairs during
review; the corrected probe was rerun and reported PASS. Regenerate its recorded
evidence from the corrected source before treating the old output as current.

### R14-04 — P2: exact grouped overlap needs ownership in task/storage contracts

[The new grouped-overlap section](../architecture/analytics-detail.md#exact-overlap-without-unconditional-pair-materialization)
selects equality-verified set groups and qualifying group relations, expanding
endpoint pairs lazily. At inspection [P05-T09](../implementation/05-analytics.md#p05-t09--relay-overlap-and-similarity-components)
still persisted affected candidate pairs/edges, and the
[storage catalog](../specification/storage-catalog.md) described only candidate
pairs, verified edges and components. Without assigning the compressed physical
representation, the implementer can still eagerly materialize the quadratic
pair relation while satisfying the older owning task.

Minimal repair: name canonical groups, memberships and verified group relations
as the physical state, with logical endpoint-pair expansion at read/export.
Extend T09 with hash-collision refusal, equal empty/sub-10k sets that must not
qualify, split/churn, lazy paging and all-pairs small-oracle equivalence. Preserve
the existing honest quadratic output cost when a caller requests every pair.
No new approximate algorithm or graph service is needed.

### R14-05 — P2: vector-first relay default leaves null ordering/activation ambiguous

[`reliable-read/1`](../architecture/analytics-detail.md#relay-selection-and-optional-scalar)
requires availability and coverage, then orders by p50 latency. A successful
carry-in anchor with a sufficiently long declared cadence can satisfy both
thresholds without any successful in-window latency observation. The profile
therefore needs a rule for null p50; unknown values failing required filters does
not settle a field used only for ordering. Engine defaults can disagree.

The Ranker profile roster also makes `general-read-relay/1` opt-in while
P05-T12 still binds both the baseline operational vector and Q to that profile,
and Step 2 emits Q whenever evidence qualifies. This does not state whether
disabling the scalar also disables the promised default vector.

Minimal repair: order absent p50 last (or explicitly require it), preserving the
endpoint tie-break; explicitly keep vector generation independent of composite
enablement and gate Q on that separate opt-in. Include absent latency and
composite-disabled fixtures. Preserve method/observer/route/access/window strata
and avoid a silent global ordering across incomparable measurements.

## What the review did not find wrong

The stationary-rank bound is valid: since the completed transition operator is
stochastic and alpha is below one, its L1 contraction gives
`||x-x*||_1 <= ||F(x)-x||_1/(1-alpha)`. The contract appropriately requires an
upper enclosure including numerical evaluation error and charges residual
passes. The exact-rational tiny probe verifies its candidates against exact
solutions; its README correctly does not claim to validate a future production
floating-point error estimator or large-graph speed. Zero passes on an unchanged
warm-start vector are consistent, but do not measure changed-graph warm-start
performance. Approximate-vector percentiles are explicitly distinguished from
certified stationary ordering.

Equal-set compression preserves an exact logical pair relation when equality and
thresholds are verified; it does not remove potentially quadratic distinct-group
comparison or exhaustive output costs. The adopted text acknowledges output
cost and refuses a spanning forest as a substitute for all requested pairs.

Verification performed here: source/task cross-reading, an executable equal-time
counterexample against the initial oracle, and execution of the corrected
standard-library probe. No BigBrotr product process, large graph, real fleet,
external relay, payment service or Tux workload was run for this review.

## Subsequent gateway/dependency self-review and handoff checks

This section is a final self-review of the same agent's gateway and dependency
work, separate from the independent counter-review above. It is not another
independent or external human approval.

Rechecked protocol/version selection, finite resource mappings, scope/principal
resolution, shared quotas, output-byte accounting, cancellation/replay and
default-off gates against the current interface/P06/operations contracts. The
gateway owner's surface table still said “supported optional” for ContextVM,
while P06 and protocol evolution explicitly called it experimental. Aligned that
table and status with the experimental P06-T14 gate; no unqualified client or
production maturity claim follows from the in-memory SDK tests. MCP's modern
HTTP binding and ContextVM's initialization-era binding remain distinct, and
neither adopts DVM's payment or durable delivery identity by implication.

Rechecked the exact dependency sources and the solver record. The selected
DuckDB/Arrow 58 relationship, best-effort PG cancellation, explicit tonic limits,
ClickHouse parameter/validation APIs and DuckDB materialization/iterator-panic
limitations are preserved in the owning handoff. No new family replacement was
justified. The Cargo solver result is not a compiled native adapter: P01/P02
must still establish the actual feature/native/platform tuple and failure behavior.
The earlier upstream ContextVM compilation/tests are separately scoped evidence.

[P01-T01](../implementation/01-foundation.md#p01-t01--reproducible-workspace-and-executable)
now consumes [dependency selections](../research/dependency-selections.md) and
their exact inventory. Source and laboratory inventories identify the additional
research separately from historical Go evidence.

[The handoff checker](../../experiments/verify_handoff.py) uses checksum-pinned
`github-slugger` 2.0.0 for actual GitHub heading fragments. It also checks explicit
task/range references, declared dependency IDs and contiguous task/step numbering.
The only broken fragment found was a historical review's retired S01 readiness
heading; the owner corrected its target to the current disposition section.
Three mechanically reported dependency-cycle candidates are downstream-consumer
mentions: P04-T02 supplies P03-T02, P04-T13 supplies P05-T08, and P04-T14 supplies
P05-T05. Their owning text states the producer direction; these are not circular
hard prerequisites. Phase-level prose and conditional foundation slices still
require judgment rather than an automatic whole-task DAG claim.

The current phase totals are 123 tasks and 756 numbered steps, including ten
planning tasks and 113 future implementation tasks. The
[recorded handoff check](../research/evidence/handoff-validation.json) identifies
the exact checked document hashes/counts and remaining limitations. It does not
replace implementation, native compilation, actual engine cancellation/recovery,
browser journeys or independent client acceptance.
