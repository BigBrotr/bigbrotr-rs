# Review 16: complete planning handoff and practical boundary checks

Date: 2026-09-17. Baseline:
`3619443381498fc14487a10b2b2cfbbdc58ae3f7`. This is a coordinated planning and
research review, including independently assigned AI critiques. It is not an
external human certification, a complete Go-code audit or product acceptance.
The legacy Go repository remained read only; the Rust product is not implemented.

## What was actually read

All **106 baseline Markdown files, 2,278,809 bytes and 26,566 lines**, were fully
read in bounded outputs by their assigned reviewers. Each hash was matched to
the baseline Git object. The [portable ledger](../research/evidence/review-16-complete-read.json)
records every path and assignment. Searching, counting and hashing did not count
as reading, and the coordinator does not claim to have personally reread all
106 files. Truncated combined outputs were repeated in smaller pieces.

| Reviewer assignment | Files | Bytes | Lines |
|---|---:|---:|---:|
| Coordinator: durability, storage, identity, import and large-engine evidence | 11 | 221,524 | 2,992 |
| Deployment/acquisition/operations | 25 | 685,380 | 6,771 |
| Analytics/protocol/read/publication | 26 | 685,821 | 9,260 |
| Entrypoints/product/continuity/historical reviews | 44 | 686,084 | 7,543 |

New contracts and changed integration boundaries received targeted rereads and
counter-review. In particular, the deployment reviewer fully reread the four
current analytics/protocol files (269,658 bytes/3,492 lines) and inspected the
complete storage/interface diffs. The final acquisition review fully read three
current design/walkthrough documents plus ten selected Go files (264,188 bytes/
3,923 lines), including newly frozen complete re-verification and state storage
sources. Its manifest and observations are included in the
[protocol evidence](../research/evidence/protocol-composition-research.json).
Pinned native DuckLake source inspection is recorded separately in the
[object-storage evidence](../research/evidence/object-storage-20260917.json).
These activities do not repeat every historical experiment or read every source
file in the moving Go project.

## Selected corrections and reasons

| Boundary | Decision | Owning contract |
|---|---|---|
| Installation and process split | One product/binary/image with explicit roles; native and Compose recipes; role selection does not silently activate another worker. Fixed-list sync/monitor work with Validator/Discoverer disabled. Serving existing CH still needs coherent control/authority/frontier and temporal maintenance. | [Deployment](../specification/deployment-contract.md) |
| Archive location | Local files are the simple default. Qualified external/self-hosted S3 is optional; remote collectors/gateways alone do not force an object server. Shared catalog/path authority and single writers remain explicit. | [Archive placement](../specification/archive-storage-contract.md) |
| Publication and durability | Synced admission, catalog transaction, CH publication and PG activation are separate recoverable boundaries. Pending data is reclaimed only after required protected dependencies. Unknown outcomes are resolved by stable identity/receipt, not guessed from a timeout. | [Durability](../architecture/data-and-durability.md), [operations](../specification/operations-contract.md) |
| Native file integrity | Native INSERT does not yield an application SHA automatically. Explicit checksum capture and backup verification cannot substitute for qualified persistence. Mandatory full-file S3 GET after every commit was rejected as extra cost without that guarantee. | [Archive protection](../specification/archive-storage-contract.md#actual-publication-and-protection-boundaries) |
| Incremental analytics | Mutable working state/checkpoints and complete CH generations own replay, retraction, join changes and clock expiry. Native materialized views are optional accelerators under that contract. | [Derivation](../architecture/derived-read-publication.md#assertion-registry-and-incremental-computation) |
| Useful default work | Explicit core current/client/dashboard/fact targets and independent relay operational scores. No wildcard registration; optional overlap/propagation/availability studies require admitted selection. Empty seed sets avoid ranking-only graph allocation. Assertion zero rows stay sparse with explicit proof for derived zero. | [Derived services](../architecture/derived-read-publication.md), [P05](../implementation/05-analytics.md) |
| Relay composition | State precisely which locally observed/visible/probe-requested population supports event/kind/author/concentration statistics. Core facts are incremental; topics, alternate windows and curator joins have explicit costs. None claims today's complete remote inventory. | [Analytics](../architecture/analytics-detail.md#relay-content-composition-from-a-declared-population) |
| Publication interoperability | Preserve standard NIP-66/NIP-85 meanings, independently identified optional context/definition manifests and categorical labels. No arbitrary composition value disguised as a standard count/rank. Unchanged assertion meaning retains its original signed revision and lineage. | [Protocol evolution](../architecture/protocol-evolution.md), [bindings](../specification/assertion-bindings.md) |
| Research access | Typed bounded ReadCore resources serve public clients/dashboard; explicitly granted scoped SQL or standalone exports serve research. No unrestricted public SQL endpoint or browser database credentials. | [Interfaces](../specification/interface-catalog.md) |
| Implementation continuity | One verified current checkpoint, exact dependency/source checks, full affected-file reads and actual engine/client journeys. Product documentation includes optional MCP and experimental ContextVM without creating mandatory services. | [Agent guide](../implementation/agent-handoff.md), [handoff audit](16-handoff-audit.md) |

Independent review changed proposals rather than merely endorsing them. Besides
the file-readback correction, it found overbroad analytical defaults, unnecessary
empty-seed work, sparse-zero interpretation and experimental oracle weaknesses.
Those changes are integrated into the owning phase steps, not left as advisory
notes for a future agent to rediscover. The final independent entry-guide check
also corrected a broad stale-read promise: paused projections are readable only
while their authority and temporal dependencies remain valid.

## Synchronization and crash cases

The final Go comparison preserved useful fixed-range checkpoints and bounded
retry paging while rejecting completion labels broader than their evidence.
An initial history pass captures a finite horizon and preserves every child
range across subdivision, preemption and restart. A contiguous attempted prefix
is distinct from unresolved gaps and from the largest observed timestamp. New
time adds a new obligation instead of moving the pass's finish line.

Empty history is not monotone: binary subdivision cannot search away unqueried
islands. Short pages, all-known bodies, EOSE and saturated seconds cannot prove
full relay contents. A finite complete kind domain can optionally partition a
saturated second, but unknown authors cannot be exhaustively enumerated from
seen authors. Second zero retains its unresolved boundary without underflow.
Generic NEG `blocked` is not evidence of a size limit: age/auth/policy and unknown
refusals must not produce an endless split tree. Advertised IDs still need actual
verified retrieval to establish local witnesses.

Live, recent age bands, historical extension and deep re-verification retain
independent fair budgets. A disconnection longer than the recent bands leaves
its entire missed historical extension pending. A completed gap retry/sample
cycle cannot refresh a full-range-pass completion time. These rules and the
independent source/transcript/restart fixtures are explicit in
[P03](../implementation/03-acquisition.md).

There is deliberately no invented atomic transaction across RocksDB, DuckLake,
CH and PG. Durable admission precedes its acknowledgement; archive rows and
semantic batch receipt commit together; index families become served only at
verified complete publication boundaries. A crash can leave lag, unreferenced
files, pending outputs or an ambiguous response, each with an owning recovery
rule. It must not produce a forged completed cursor or silently empty read.
The implementation still has to exercise every boundary with its actual adapters;
planning cannot exempt the implementer from finding additional edge cases.

## New executed evidence

| Experiment | Observed result | Practical limit |
|---|---|---|
| Deployment model and Compose parser | 26 literal fixtures, nine wrong-rule controls and two daemon-free parser cases pass | Not actual packaged startup, sockets, credentials or fencing |
| Signed composition reducer | Nine signed events, 22 independent SQLite comparisons, 22 replay comparisons, two expiry comparisons and 13 wrong-rule controls pass | Small semantic experiment, not full producer/engine recovery |
| Native local/S3 archive | 1,048,576 unique rows per layout; exact population oracles and 132 full regenerated signed samples; reader without local data mount succeeds | Ordered shared-host runs, not WAN/cold-cache/performance ratio |
| Native faults/restore | Precommit writer kill leaves no added body/receipt; missing committed object causes failure; 20-file catalog/alternate-prefix restore matches hashes and signed samples | Same host, source accessible; no ambiguous COMMIT or power-loss claim |
| Stronger boundaries | Position-dependent multipart/range bytes, exact LIST/HEAD closure for both experimental families, read-role denied writes and PG denied DML/DDL pass | PG trust lab is not authenticated production isolation |
| CH insert-time view counterexamples | Replay count two versus one logical event; stale right-side join label; expired eligibility zero versus unchanged MV count one | Three real-engine semantic failures of naive designs, not throughput measurements |

The [experiment summary](../research/experiment-summary.md) links sources,
versions, source/report hashes and limits. The object experiment also found two
uncommitted orphan files in the source prefix: catalog membership selected the
20 committed files, while a glob would see 22. Earlier weak oracles and a
provisioning parser failure remain disclosed beside the corrected run.

All five newly owned Tux containers were stopped after completed work. Prior
qualification source data was mounted read only; unrelated workloads were
preserved. Data and inventories remain at the recorded owned lab roots. A worker
reached its 10 GiB cgroup ceiling without OOM kill, and Docker swap allowances
were not disabled; the report does not mislabel that as process RSS or spare RAM.
No additional wide benchmark is justified solely by a desire for a universal
"best" claim: further physical choices now require the actual product workload.

## Completion boundary

The integrated artifact and canonical-link/task checks are recorded in
[verification evidence](../research/evidence/review-16-verification.json).
Structural success does not validate semantics; the full reads, counterexamples
and independent critiques above provide separate bounded evidence.
The roadmap remains 123 tasks/756 steps, including 113 implementation tasks.
No future product step is checked as completed.

The repository is ready to guide the first implementation slice. It is not a
promise of zero future defects, global optimality, 100,000 simultaneously busy
relays or production release readiness. Exact formats, authority, logical state
and recovery obligations are selected; implementation measurements own physical
layout/index tuning and acceptance of the pinned native adapters. Any discovered
contradiction must be repaired in the owning contract and fixture before dependent
work proceeds. This ends the planning review, not the critical review discipline.
