# Agent continuity and product handoff audit

Date: 2026-09-17. Frozen baseline:
`3619443381498fc14487a10b2b2cfbbdc58ae3f7`.
Planning review only; no product implementation, remote operation, commit or push
was performed by this reviewer. This is an independently assigned AI review,
not an external human certification or a complete solo repository audit.

## Complete reading and limits

All **44 assigned frozen Markdown files, 686,084 bytes and 7,543 lines**, were
read completely in bounded tool output. Exact paths, SHA-256, sizes and read
method are in the local `.research/review-16/handoff-read-manifest.json`. Every
assigned hash was independently compared with its Git blob at the baseline above.
Two early combined outputs truncated portions of README/agent-handoff and the
implementation index/coverage; those four complete files were reread in smaller
outputs before their full reads were recorded. Searches and hashing were never
counted as reading.

| Assigned group | Complete files |
|---|---|
| Entrypoints (4) | `AGENTS.md`, `README.md`, `PLAN.md`, `docs/start-here.md` |
| Engineering (2) | Rust/repository; security/recovery/validation |
| Implementation (7) | P00, P02, P09, P10, phase index, coverage and agent guide |
| Planning (5) | Mandate, process, questions, confidence and specification readiness |
| Product (4) | Requirements/history, data/feature decisions, dashboard/website and private administration |
| Research (4) | Dependency selections, experiment summary, product-surface findings and Tux retirement |
| Specifications (2) | Interface catalog and shared naming |
| Historical reviews (11) | 03, 05, 06, both assigned 07 phase reports, 08 coordinator, all three assigned 10 reports, and 13 runtime/storage alternatives |
| Experiment explanations (5) | Document layout, workload, DuckLake/PG, read/rank and RocksDB admission |

The laboratory inventory JSON was additionally read in full from the current
worktree. Its recorded lab status was not remotely rechecked. Other reviewers own
the complete specialized deployment, identity, protocol, gateway and analytics
contracts outside this assignment. Reading historical reports does not repeat
their underlying Go/source/browser audits or re-execute their experiments.

## Findings and selected repairs

**H16-01 — Resumption had no durable current-work record.** The original agent
guide supplied a good task pipeline but no location for the active step, dirty
work, tested revision, interrupted processes or next bounded action. Conversation
memory could become an accidental authority after compaction. Root AGENTS now
points to one [checkpoint](../implementation/progress.md), and the
[guide](../implementation/agent-handoff.md#resume-from-one-verified-checkpoint)
requires checking Git, current contracts, available artifacts and actual owned
job/receipt state before continuing. The checkpoint records planning state only;
it does not manufacture P01–P10 progress or a second task hierarchy. An unknown
write outcome remains unknown until the owning recovery protocol resolves it.

**H16-02 — Optional gateways were absent from concrete user documentation.**
The interface catalog and phase index select MCP and experimental ContextVM, but
P09-T06 About links and P10-T04/T05 guide/example inventories enumerated HTTP,
ArchiveRelay and DVM only. Generic advertised-feature validation did not identify
who supplies gateway onboarding and real-client examples. The repaired tasks and
[product contract](../product/dashboard-and-website.md) include the appropriate
links, version/subset/authentication and encryption distinctions, bounded queries,
continuation, cancellation and denied/disabled cases. P06 remains the protocol
owner; P10 executes its displayed examples against the actual advertised adapter.
Neither gateway becomes a mandatory runtime dependency or an invented DVM variant.

**H16-03 — The product summary could restore a global signature representative.**
Its first retained-data row said one full signed body per event ID, while adopted
S01/P02 contracts distinguish shared payload from first verified signature per
eligible visibility scope. That shorthand could leak a private signature or
admission envelope when a later public copy arrives. The
[data decisions](../product/data-and-feature-decisions.md) now describe the scoped
representatives explicitly. This is a paper consistency repair; actual private/
public arrival, alternate-signature, replay and export proof still belongs to
the existing identity/storage/read acceptance tasks.

**H16-04 — Logical document deduplication was presented as physical placement.**
The same product table discouraged copying unchanged bytes into every observation
without the catalog's measured wide-layout qualification. The assigned controlled
experiment found both winners depending on churn and file locality, and P02-T09
already owns the comparison. The repair keeps stable scoped object/occurrence
identity while allowing either qualified physical representation. No compression
or latency result is newly claimed, and no new storage engine is introduced.

**H16-05 — Settled product defaults still looked like unanswered release questions.**
The dashboard/site ending called for resolving public-instance, exposure, score
presentation and citation expectations even though current contracts select their
defaults. It now states self-hosting/local verification, deliberate resource
exposure, explained named profiles and immutable export/explicit snapshot citation.
A hosted public service commitment remains a separate future operation. Historical
research question candidates stay historical; they create no new permission gate.

**H16-06 — Exact dependency and evidence scope needed a practical resumption rule.**
The original guide asked for version inspection but did not explain how to resolve
candidate-versus-lock or docs-versus-source disagreement after a long interruption.
It now connects the dependency record to actual features, native assets, matched
official documentation/source and narrow experiments. Frozen independent expected
bytes/sets/reducers/clients remain distinct from production assertions. Missing
artifacts and unknown terminal outcomes cannot be reused as PASS.

**H16-07 — Clarify relevant complete-file reading and product selection.** Later
owner steering explicitly requires directory navigation and complete relevant
files/contracts instead of grep excerpts, while treating ideas as context rather
than obligations. AGENTS and the guide now require full bounded owning-file and
affected caller/consumer reads, disclosure of uninspected boundaries, and scope
proportionate to the change. They do not demand a whole-codebase reread for a small
edit. Feature decisions start from the verifiable archive/observatory purpose and
useful outcomes; MCP has an optional agent read use, ContextVM remains experimental,
and no additional public RPC follows automatically.

## Independent assessment of the pipeline

Retain the existing staged journey and boundary audits. Full events, provenance,
monitor measurements, derived facts, read adapters, private administration and the
website have real purposes and independent costs. Their coverage must follow
producer → durable authority → user result, rather than count headings or
checkboxes. A service toggle, process/container choice and quality obligation are
different decisions. Optional paid/gateway evidence need not block free reads,
but an unqualified omitted capability is not completion of the selected product.

The historical reviews are valuable precisely because they contain counterexamples:
false success from unwired CLI composition, empty fixtures, skipped engine suites,
body-only recovery, and no producer for a polished consumer. They also preserve
superseded 365-day raw-measurement retention, overlapping resweeps and fixed-pass
ranking advice. Those historical values must not overwrite current contracts.
The new guide makes this precedence explicit without erasing the earlier evidence.

Documentation volume remains a maintenance risk. This change adds one current
checkpoint and this scoped audit, not another master plan or per-session log
hierarchy. Owning tasks and specifications retain their authority; changes to
them must update the corresponding consumers. Review frequency follows persistent
formats/identity, durable acknowledgements, public authority, derived activation
and distribution boundaries. Neither fixed review counts, comment limits nor
tests that repeat implementation expressions establish correctness.

## Validation and remaining evidence

Completed reading/hash checks establish the exact assigned baseline only. The
first `python3 experiments/verify_artifacts.py` run exited 1 on four targets still
being created by other domain work: relay-composition README, protocol-composition
evidence, archive-storage contract and deployment-contract README. `git diff
--check` exited 0. Local `.research/review-16/handoff-validation/` retains exact
logs and before/after worktree hashes; none changed during that check interval.
This is an integration finding, not a claimed final all-pass. Final coordinated
checks must assess the integrated state, including later edits to this report.
The separate canonical-fragment/task check exited 1 on concurrent P05-T07's
ambiguous cross-phase task shorthand, and reported no fragment errors. Its exact
output and unchanged check-interval hashes are in local
`.research/review-16/handoff-final-check/`. A direct checkbox inspection found no
completed future product steps. Cycle candidates emitted by that checker require
semantic interpretation; they are not automatically valid whole-task dependencies.
No product, browser, website, native adapter, real fleet, power-loss or distributed
failover test was run by this reviewer. Engine research remains evidence for its
recorded workloads and failure classes. The current readiness register retains
the practical implementation and release gates.


Coordinator disposition: the missing targets were subsequently supplied and the
ambiguous dependency shorthand was replaced by explicit existing task IDs. The
[integrated report](16-complete-handoff.md) owns final validation and lab state;
the intermediate failures above remain historical observations, not open tasks.
