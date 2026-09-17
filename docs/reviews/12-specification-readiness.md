# Review 12 — Remaining paper contracts before implementation

Date: 2026-09-17. Reviewed Git baseline:
`78ea43b43523a90c9d9d215ef80b98456ba91242`.
The owner requested an assessment of what still needs specification to prevent
implementation mistakes, then clarified that practical decisions are acceptable
only where practical evidence can usefully decide them.

## Scope and evidence

This was targeted contract reading and cross-reference inspection, not another
complete-document audit. It covered identity/admission/source envelopes, scope
and current-state rules, storage/receipt/replay contracts, configuration/budgets,
read interfaces/time, capacity/validation, import, repository/dependency policy
and the owning phase tasks. PLAN, phase00/index, mandate, questions and confidence
were checked against their current readiness claims. The prior full-read and
experiment manifests retain their original scope; they are not new execution.

The most consequential traces were:

| Finding | Evidence inspected | Remaining closure |
|---|---|---|
| Same-ID event crosses private/public sources, especially without provenance | A01/A02 and immutable envelope in data-contracts; naming scopes; archive/serving catalog; first observation rules; current winner and public-generation rules in P05/P06/security | Specify durable scope eligibility and separately visible metadata; reconcile authorized input population versus post-winner filtering. S01 |
| “Versioned canonical encoding” leaves durable byte identity unspecified | Scalar/envelope/digest definitions; naming's dictionary-domain delegation; P01-T02/T07 and P02-T02/T03 | Exact preimages, allocation/qualified mapping, presence and compatibility fixtures. S02 |
| Detailed recovery exists across several owners/documents | Storage producer retirement, archive ambiguity, source cuts, derived Rocks state/CH/PG activation, component states and failure matrix | Consolidated transition/effect/fault schedules with one authoritative recovery action. S03 |
| Local caps do not yet provide a fully worked pipeline reservation model | Runtime budget rules, acquisition/event/transport caps, storage batches, read/relay limits, capacity accounting | Cross-stage size admissibility, resource order and completion/recovery reserve examples. S04 |
| Product paths are named but not uniformly closed at field/composition level | Interface catalog, configuration, internal RPC, read and management phase boundaries, deployment/upgrade plans | Recipe/capability and external field/error/version matrices, operator transitions. S05/S06/S08 |
| Stress programme describes dimensions more clearly than useful acceptance thresholds | Capacity programme, P02-T09, common read limits, temporal cursor acceptance and evidence discipline | Mixed workload/objective records, calibration rules and independent combined fixtures. S07 |

The protobuf primary documentation was opened and read for the specific canonical
serialization claim. It [explicitly distinguishes deterministic serialization from canonical bytes](https://protobuf.dev/programming-guides/serialization-not-canonical/).
The current [DuckLake transaction documentation](https://ducklake.select/docs/stable/duckdb/advanced_features/transactions)
was checked for its own database snapshot boundary. Neither source proves the
unimplemented cross-store application protocol; previous engine probe versions
are unchanged. No dependency upgrade, new benchmark or production runtime test
was performed in this review.

## Changes made by this review

Added [specification readiness](../planning/specification-readiness.md), with
eight remaining paper-contract groups, concrete counterexamples, recommended
closing directions, existing implementation owners and eight bounded empirical
decision points. Added P00-T06–T10 as **unchecked** closure work; the plan now has
121 tasks/746 numbered steps, including ten planning tasks and 111 future product
tasks. Counts describe the documents, not completeness or quality.

Updated mandate/questions/confidence and plan/index/coverage entry points so that
an old completed-review checkbox cannot be mistaken for current specification
readiness. The same-event scope alternative remains a recommendation awaiting
the specified paper closure; no new archive family or hidden product feature has
been adopted through this assessment. Existing normative rules remain selected
except where this register explicitly records a conflict requiring resolution.

## Validation and limits

The changed Markdown, task steps, local references and counts are checked before
commit; results are recorded in
[the review evidence](../research/evidence/specification-readiness-checks.json).
Mechanical checks cover links/structure, JSON/Python syntax and selected existing
evidence hashes; they cannot close a semantic question or prove runtime behavior.
The final reread checks that assessment is not mislabeled as completed closure.
The legacy repository and its external plan remain untouched.

The useful next paper work starts with P00-T06, because visibility/identity and
persistent bytes affect the rest. Engine/physical choices stay conditional only
with specified experiments and decision deadlines. Private helper names, every
future optimization and arbitrary test/review quotas are deliberately excluded.
