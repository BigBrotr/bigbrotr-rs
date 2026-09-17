# Large-storage evidence and final handoff countercheck

Date: 2026-09-17. This is a bounded coordinator review plus separately tasked AI
counter-review, not a new claim to have reread every historical document or an
external human audit. Product implementation has not started. The preceding
[closure review](14-independent-closure-review.md) and
[countercheck](14-handoff-countercheck.md) retain their respective scopes.

## Changes that matter before implementation

| Finding | Disposition |
|---|---|
| Older acquisition steps still required a durable mutation receipt for every duplicate | `EventEffectBasis` now accepts a receipt or an existing-effect proof through page joins, bounded checkpoints, imports and lifecycle integration. Proof-only pages allocate no request/ACK; retained dependencies still close recovery. The model compares 44 independently derived inventory states and detects six wrong alternatives. |
| An access scope alone did not distinguish private and public witness inventory | Target visibility is part of acquisition scope and local reconciliation membership. Private witnesses cannot suppress needed public evidence, including import-before-network cases. |
| Queue count could be mistaken for the durable-request concurrency limit | P03 names the 1024 queued-record ceiling and the separate 64 outstanding mutation requests under one byte grant. |
| A default archive layout appeared intrinsically much larger | Actual file inspection distinguishes Snappy/default row groups from explicit ZSTD/layout tuning. The 20M-row comparison is a combined layout experiment, not an isolated codec result. |
| The short `id_body` query name overstated its output | It queried content length. Reports now say so and separately report complete regenerated signed sample reads/population checks. |
| Parent CH part bytes could be double-counted with projection bytes | Exact-release source confirms parent totals include child projections. Reports label projection bytes as a component subset. |
| The summary could accept a weaker verifier mode under the same PASS string | It now requires the exact engine set, population/sample scope, per-engine status and baseline/restart digest equality. Mutated missing-engine and changed-result reports are rejected. |
| A concurrent CH reader query shadowed its stored ID with an output alias | Its failed run is retained. The corrected qualified-column query must pass a real-engine countercheck and a fresh separately named run; the original is never relabelled PASS. P02-T05 explicitly requires bound values, qualified stored columns and this real-engine/full-row regression case. |
| Restart identity checking treated mount enumeration order as meaningful | The first graceful restart's FAILED harness record remains visible; a fresh run compares exact sorted mount tuples and then verifies both original 20M-row tables. |

The additional storage reviewer read the full baseline/verification/backup/
restore/summary programs and checked retained initial evidence. That review found
the projection accounting and summary-mode weaknesses. It did not observe the
later restore/restart execution; the coordinator checks those actual reports.
Execution results and exact limitations belong to the
[Tux research report](../research/tux-storage-qualification-20260917.md).
The final collector also rejects a shortened CH mixed run, a missing worker,
an unfinished restart and a stale verification hash. These are checks against
mutated copies of actual reports, not extra engine qualification. Final evidence
contains the completed corrected runs and the stopped owned-resource inventory.

## What the next agent receives

Root `AGENTS.md` points to one
[execution pipeline](../implementation/agent-handoff.md). The phase index contains
123 tasks/756 steps: 10 planning tasks and 113 product implementation tasks.
All product steps remain unchecked. Each coherent slice must connect its owning
contracts, independent expected results, actual process/client journey, failure
boundary, documentation and review. Periodic audits occur at persistent format,
acknowledgement, scope, derived generation, public surface and distribution
boundaries; a count of clean reviews is not a substitute for evidence.

Service/configuration/identity decisions and public read contracts are selected.
Physical engine tuning and supported capacity retain bounded decision gates with
named owners and alternatives. These are purposeful experiments before dependent
implementation spreads, not unspecified product semantics left to guesswork.
Historical reviews remain evidence of the path taken; current owning contracts
are the specification. Research programs are not production code templates.

## Claims deliberately not made

The larger corpus and engine recovery drills improve confidence in particular
storage/read boundaries. They cannot establish a globally optimal architecture,
Rust-over-Go end-to-end superiority, native Rust adapter correctness, off-host
durability, billion-edge provenance cost, useful full-text/graph behavior or a
5k–100k busy relay operating envelope. The
[readiness register](../planning/specification-readiness.md) and
[workload contract](../specification/workload-contract.md) keep those obligations
explicit. A failing future gate changes the choice or advertised envelope; it
must not be converted into a weaker hidden requirement.
