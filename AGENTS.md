# Repository context

This repository contains BigBrotr's fresh Rust implementation and its design.
The owner explicitly started product implementation on 2026-09-17. Implement the
complete selected product through the existing task plan; earlier planning-only
statements describe the previous stage, not a current implementation restriction.
Isolated research experiments are authorized and must be clearly labeled. The
owner explicitly authorizes substantial, long-running, realistic experiments on
Tux when scale can change a critical decision; research is not limited to
small probes. Preserve the local legacy repository and unrelated workloads.
The owner explicitly retired and authorized deletion of all pre-existing
BigBrotr resources on Tux on 2026-09-17; do not reuse those environments. New
research must create separately inventoried resources from scratch.

The neighboring local Go repository and local historical projects are reference
material and must remain read-only. Their process rules and past technology
decisions do not constrain this new design. Preserve useful product intent and
explicitly explain changes to it.

BigBrotr's identity is a verifiable Nostr archive and observatory. Owner ideas and
historical features are context to evaluate, not automatic obligations: include
a capability when its useful question and total cost justify it.

Write repository content and commit messages in English. Keep the product name
BigBrotr; `bigbrotr-rs` is the repository identifier. Record user decisions and
unresolved assumptions in `docs/planning/questions.md`, and keep the current
mandate in `docs/planning/mandate.md`.

Owner decision Q40 defines delivery as the complete P00–P10/M1–M5 product,
including selected optional features, reproducible from a clean checkout with
declared dependencies and inputs. Required build, test and operating tools belong
in the maintained repository; private lab paths, caches or this session cannot be
prerequisites. Technical suggestions are intent, not mandated implementations.

This is before the first production release. Internal commits/formats are not
public compatibility obligations: consolidate obsolete migrations, adapters and
fixtures when their actual consumers and guarantees are understood. Preserve
current restart/replay/recovery, corruption refusals and data integrity; do not
rewrite Git history or replace working components without need. Q40 supersedes
blanket additive-migration and indefinite-retention rules. Retain essential
evidence, open-problem reproducers, unique data and unintegrated work; retire
verified owned, unused, unnecessary resources incrementally. Q37/Q38 remain.

Base claims on source inspection, documented guarantees, or recorded experiments.
Separate measured results from extrapolations and proposals. A passing test is
evidence for the behavior it exercises, not proof of overall production fitness.
Review code, contracts, operations, and actual user journeys together.

Map the relevant directories before working. Read complete owning files and the
linked contracts/callers that determine the affected behavior, using bounded
untruncated chunks. Searches locate material; snippets, headings and file counts
are not comprehension or complete-review evidence. Scope this reading to the
change and its real boundaries, rather than rereading the whole eventual codebase
for every small edit. Disclose any review boundary that remains uninspected.

Use coherent commits for reviewable work. Keep reproducible experiment sources
and concise evidence; downloaded checkouts, build caches, and raw generated data
belong under ignored `.research/` and must be inventoried. Never commit secrets,
live credentials, or unrelated host data. Do not add arbitrary line-count,
coverage-percentage, or repeated-clean-review requirements.

## Implementation handoff

Use [the agent execution guide](docs/implementation/agent-handoff.md) as the
single pipeline for task selection, dependency research, independent fixtures,
practical execution, review, documentation and coherent commits. Root AGENTS.md
is the agent entrypoint; do not maintain a competing agent.txt instruction set.
The current specification is authoritative over historical review suggestions.

Follow the [development and delivery contract](docs/engineering/development-and-delivery.md):
Tux as the sole editing/Git, development and test host under the owner's
2026-10-12 instruction, with isolated exact-revision Linux runs, GitHub Actions
for available isolated CI and Docker Hub for images. The former Mac authoring
rule is historical. Keep ordinary installation
on Docker Compose with explicit initialization/migrations. All initial Cargo
packages remain unpublished. Under Q36, `main` is the current integrated project
state; update it with verified coherent work and retire redundant branch refs
after reconciling their original commits. Preserve pending work and required
ignored/external evidence; dispose of obsolete worktrees/labs separately under
Q40's ownership and retention checks. Branches and parallel agents serve concrete needs. Keep
one integrating owner and separate writable worktrees when implementation is
parallel. Never run untrusted PR code on the shared Tux host or with release keys.
The same contract documents the existing planning checks and their prerequisites;
future product workflows are not yet implemented or passing.

On a new session or after compaction, read the single durable
[progress checkpoint](docs/implementation/progress.md) and follow the guide's
resumption procedure before continuing. Verify its revision, dirty files,
evidence paths and process/resource claims against reality; the checkpoint is
navigation, not proof or a second task plan. Preserve another contributor's work.
Update the same checkpoint at a meaningful task boundary or before interruption,
including unresolved outcomes and the next bounded action. Do not create a new
handoff file per session or copy secrets, raw logs or every completed step into it.
The latest explicit owner instruction governs the stage; an old checkpoint does
not revoke authorization already given in the session.

Before product work, consult the readiness register. During implementation,
audit every new persistent boundary and each end-to-end milestone; test actual
processes, engines and clients where those guarantees matter. Do not translate
unchecked planned commands or small research probes into claimed release evidence.
