# Implementing BigBrotr from this specification

This is the execution guide for a new implementation agent. This is a public
implementation/design repository in development (Q42). Its research programs are experiments,
not production adapters. The owner explicitly started product implementation on
2026-09-17; see the [current mandate](../planning/mandate.md). The neighboring Go
checkout remains read-only.

## Establish the baseline

Read root [AGENTS.md](../../AGENTS.md), [Start here](../start-here.md),
[PLAN.md](../../PLAN.md) and the [phase index](README.md). Then read the current
[readiness register](../planning/specification-readiness.md) and the task's
linked contracts. Historical reviews explain decisions; they do not override the
current owning specification. Resolve an actual conflict before propagating it
into code, tests, generated schemas or documentation.

Read the [progress checkpoint](progress.md) to locate current work, then verify
it using the procedure below. The latest explicit owner stage instruction takes
precedence over a stale checkpoint. Research authorization alone does not start
product implementation; already authorized work does not require a new approval
merely because the conversation was compacted.

Map the relevant folders and file ownership first; `rg --files` is useful for
navigation. Read each complete current owning document/source file and its linked
contracts, callers and consumers needed to understand the affected behavior.
Use bounded contiguous reads and repeat any truncated interval. Search hits and
headings locate a call chain or rule; they do not establish that the file or
behavior was understood. Follow actual dependency boundaries, including a caller
that can change authority, persistence or output meaning. Record uninspected
boundaries honestly. A focused edit needs its relevant files, not a ritual reread
of every file in the future codebase.

The key contracts have distinct ownership:

| Concern | Authoritative specification |
|---|---|
| Product intent and exclusions | [Feature decisions](../product/data-and-feature-decisions.md) |
| Record meaning, fields and lifecycle | [Data](../specification/data-contracts.md) and [storage catalog](../specification/storage-catalog.md) |
| Identity, eligibility and portable bytes | [Identity and encoding](../specification/identity-and-encoding.md) |
| Authority, recovery, resources and configuration transitions | [Operations contract](../specification/operations-contract.md) |
| Public resources and modes | [Interface catalog](../specification/interface-catalog.md) and [read views](../specification/read-view-contract.md) |
| DVM versus MCP/ContextVM | [DVM profile](../specification/dvm-profile.md) and [gateway contract](../specification/gateway-contracts.md) |
| Analytical definitions and costs | [Analytics](../architecture/analytics-detail.md), [derivation](../architecture/derived-read-publication.md), [assertion bindings](../specification/assertion-bindings.md) |
| Useful workload acceptance | [Capacity](../engineering/capacity-and-benchmarks.md) and the task's practical scenario |
| Code boundaries and dependency policy | [Rust and repository](../engineering/rust-and-repository.md) |
| Development hosts, CI/release, commits and collaboration | [Development and delivery](../engineering/development-and-delivery.md) |
| Browser journeys, private operation and published documentation | [Product surfaces](../product/dashboard-and-website.md), [private administration](../product/private-administration.md), [P09](09-product-interfaces.md) and [P10](10-website.md) |

Q40 defines completion as all selected P00–P10/M1–M5 responsibilities, including
optional features, delivered through a maintainable clean-checkout repository.
Keep required reusable build/test/operation tools and suitable inputs in stable
repository locations, or document verified acquisition/generation. Lab caches and
private artifacts may accelerate work but cannot be essential undeclared inputs.
Use P01-T08, P08-T11/T12 and P10-T08/T10 for acceptance and delivery; do not create
a second plan. User documentation describes portable current procedures, not
Tux, an agent session or experimental history.

Before first production release, development iterations are not compatibility
commitments. Remove superseded paths/migrations/fixtures after consumer analysis;
consolidate bootstrap/schema while preserving current system properties. Retain
necessary restart/replay/recovery and corrupt/incompatible-input tests. Reuse
working components, preserve Git history and update owning contracts/tests/docs
together. Q40 replaces blanket additive-migration and indefinite-retention rules.
Keep essential current evidence, open-problem reproducers, unique data and pending
work; verify ownership, actual use and regeneration before incremental cleanup.
No broad historical audit, cleanup project or reduced product scope is implied.

Q41 requires functioning hosted GitHub Actions, demonstrated by real runs, with
the same maintained commands/criteria used locally where appropriate. Q33 only
authorized a temporary fallback during billing failure. Keep check selection
conservative, dependency caches verified and optional, costly qualification
explicit, and pushes grouped around coherent results. Anticipate inexpensive
checks and evaluate safe compilation reuse without losing platform/artifact or
clean-install coverage; compare timings from already-required runs. Close known
failures before treating their gates as qualified. Record provider admission
failure, workflow failure, cancellation and product failure separately; none is
success. Q42 makes accessible source history, logs and artifacts public. Review
upload allowlists; never upload raw credentials, sessions or database captures.
Official releases, package publication and production deployment still require
separate authorization. Do not rewrite historical evidence to imply a past run
was public or succeeded.

Do not scaffold every proposed folder or crate before its first consumer exists.
Do not copy the research Python or a state model into a product implementation
merely because it passes its own fixture. Generated APIs, DDL and serializers
must express the selected contracts; their names are not a license to invent
new data meanings during implementation.

## Resume from one verified checkpoint

Maintain only [progress.md](progress.md) as the current execution checkpoint.
Task files own planned steps and completed checkboxes, specifications own meaning,
and experiment/acceptance reports own detailed evidence. Replace the checkpoint's
current record as work advances; Git history preserves earlier records. Do not
append a conversation transcript or create competing per-session plans.

Under Q38, keep the checkpoint to the current objective, revision/local changes,
open problems, active processes and next action, linking existing detailed reports.
After compaction, reconcile those facts and investigate actual discrepancies;
do not reread unchanged documents or copy the history of completed runs.
Reuse verified reads, reviews, tools and results while their relevant code,
dependencies, configuration, inputs and artifacts remain unchanged. Repeat affected
checks for changed assumptions, new anomalies or lost certainty. Verify actual
resource ownership and isolation before mutation/destruction; check space before
significant allocation, low headroom or changed conditions. Unrelated edits do not
require another complete laboratory inventory. At coherent checkpoints with low
headroom, retire verified obsolete owned caches/copies incrementally, preserving
unique data, pending work and essential evidence. Keep displaced unfinished work
assigned to its existing task and acceptance gate while Index/M1 advances.

Keep real-engine evidence, independent expectations and critical review. Mechanical
and documentation fixes need targeted checks; verifier changes affecting PASS/FAIL
meaning need particular scrutiny. Prefer parameterizing already verified tools to
creating a new reviewed script set for every attempt. Preserve original failures
and artifact identities in their owning reports, resource custody in the inventory,
and link between them instead of duplicating detail. Group recording, commits and
pushes around coherent outcomes; these rules preserve all product/quality gates
and do not create a separate process-optimization project.

Q38's October25 clarification makes each increment a demonstrable capability:
briefly state its expected result, real dependencies and necessary checks. Do not
turn every substep into a further refinement chain. Verifier or documentation
changes alone do not mandate product rebuilds; use retained captures and inexpensive
independent diagnostics where useful before required real runs. Classify a failure
as product, fixture/verifier, environment or unknown and fix its cause, preserving
the original outcome. Never fit expectations to observations, widen limits for
PASS or weaken durability, authority, cancellation or recovery. Repeat critical
review only for relevant changes, new evidence or unresolved questions. Reduce
duplication in touched owners without a general rewrite or new framework.

Select the next increment by dependency, risk and capability unlocked. Keep a short
M1 blocker list in the checkpoint, separating its signed-event admission/archive/
read/export/restart requirements from later work. Name the concrete risk requiring
any deeper prerequisite; do not gate M1 on all of P02. Preserve later obligations
in the existing plan. Updates should explain what now works, its evidence, the
remaining blocker and next useful result. Commit/test counts are not productivity
or quality measures; there is no imposed deadline or reduced product scope.

Record enough to resume safely:

| Field | Minimum useful content |
|---|---|
| Stage and active slice | Planning or owner-started implementation; exact existing task/step IDs, bounded purpose and owner. A planning review names its review ID without inventing completed product tasks. |
| Repository state | Repository/worktree, branch, base commit, last observed HEAD, relevant staged/unstaged/untracked paths and their owners; identify an in-progress merge/rebase if present. Never write a guessed future commit hash. |
| Contract changes | Links to changed owning specifications and affected consumers; unresolved semantic conflicts and decisions still conditional on evidence. |
| Work and evidence | Actual command/run, tested commit or dirty-diff identity, input/dependency identity, result and existing report/artifact paths. Separate PASS, FAIL, SKIP, NOT_RUN and unknown/interrupted outcome. |
| Findings and gates | Open findings, not-run required scenarios and the release/capability each prevents; link their owner instead of copying the whole backlog. |
| Processes and lab resources | Inventory reference, owner and last observed status of relevant jobs/processes/containers/data; unfinished writes, cleanup obligations and whether status was freshly checked. No credentials or raw private data. |
| Next action | One exact bounded action, its prerequisites, expected observable result and where to record it; name a concrete blocker when one exists. |

Before resuming, inspect `git status --short --branch`, `git rev-parse HEAD`,
the relevant staged/unstaged diff and the current task/contract text. A different
checkout, revision or unexpected dirty file requires reconciliation with its
owner; do not reset, overwrite or re-run it based on remembered context. A clean
worktree does not establish that remote state or a background job is unchanged.

Check that evidence paths still exist, that recorded inputs/tool versions and
hashes match the result being reused, and that its asserted outcome was actually
observed. Read the result, including failures and exclusions. A missing artifact
is unavailable evidence, even when an old report contains its digest. A process
handle or PID recorded before interruption is not proof of current ownership:
verify the job identity, receipt/log boundary and actual owned resource before
joining, stopping or resuming it. Use read-only inspection first; an ambiguous
durable write must follow its owning recovery contract before retry. Missing
terminal output means unknown, never PASS or assumed rollback.

The [laboratory inventory](../research/laboratory-inventory.json) owns research
resource records. Its dated stopped/retired observations are not live inspections.
Retired Tux environments stay retired; separately created retained environments
must be identified individually. Verify scope/ownership before reuse or cleanup,
preserve unrelated workloads, and keep generated data under the inventoried
ignored root. Do not start remote work just to refresh a handoff status.

Update the checkpoint after a coherent verified slice, a material change of
direction, or before a planned interruption/compaction. If interruption prevented
that update, reconcile from Git, existing artifacts and receipts on resumption.
This protocol is recovery from incomplete context, not an assumption that every
interruption leaves a complete note.

## Research the dependency actually being used

Start with the task's contracts and the
[dependency selection record](../research/dependency-selections.md). Inspect the
repository's actual lockfile/toolchain, enabled features, native engine/extension
assets and generated schema versions. The selected candidates are not a qualified
release tuple. Read official documentation for that exact version and inspect
matching source where guarantees, defaults or documentation disagree. A current
website, search snippet or similarly named crate cannot establish an older
binding's behavior. Record a narrow source path/commit and the consequence for
the task; avoid copying a dependency manual into the checkpoint.

Exercise the boundary when source inspection cannot decide it: actual commit
completion, cancellation, memory ownership, encoding, engine persistence or client
interoperability. A solver-only lock, Python binding, SDK fixture or in-memory
model supports only its executed claim. Keep expected results independent of
production code through frozen signed fixtures, literal expected sets/bytes,
slow exact reducers or separately implemented clients. Reuse suitable existing
oracles; do not derive expected output by calling the implementation under test.

## Execute a coherent task slice

1. Select a task whose concrete prerequisites exist. List its consumed/produced
   contracts, incompatible outcomes and practical evidence before changing code.
   Use the cross-phase dependency edges; avoid blanket phase barriers.
2. Inspect the relevant existing code and the exact upstream dependency version.
   Pin the tested dependency set, including native engines/extensions. Read the
   behavior that matters: a method name or successful ACK is not a guarantee.
3. For changed behavior, reuse or write an independent expected-result fixture
   and the discriminating failure cases the task requires. Keep the oracle independent of the production
   reducer/query. Cover complete signed bytes and real ownership boundaries where
   relevant; do not certify crypto using fake signatures.
4. Implement the smallest complete responsibility with idiomatic Rust, explicit
   errors, bounded work and cancellation. Keep operational state separate from
   retained scientific evidence. Update generated contracts, configuration help,
   user documentation and migrations with their actual behavior.
5. Run checks appropriate to the changed behavior and its real packaged practical
   scenario. Choose unit/property/fuzz/integration coverage for the risk; a small
   documentation correction needs its affected artifact/example checks, not an
   invented test or automatic rerun of every product scenario. Missing
   infrastructure, empty fixtures and unimplemented
   scenarios are `NOT_RUN`, never success. Include refused and cancelled work in
   load reports. Do not repeat broad suites without a new change or concern.
6. Review the complete diff for correctness, complexity, ownership, allocations,
   I/O, observability, exposure and maintenance. Re-read the written contract
   against the implementation and actual output. Fix findings and rerun affected
   checks; a passing test suite does not replace this review. At task closure,
   also reconcile obsolete compatibility and stable experimental tools under Q40;
   keep responsibilities and maintenance obligations explicit, without expanding
   the increment into a general rewrite.
7. Commit a coherent outcome with evidence and documentation. Record what passed,
   what was not exercised and any remaining release gate. Mark only genuinely
   completed steps and refresh the progress checkpoint. Never mark the whole
   phase complete from one happy path. A pending commit is not a pushed result.

The practical runner implements `p01-t01`, `p01-event-cli`, `p01-config-static`
and the fresh installed `p01-first-launch` / `p01-first-launch-restart` /
`p01-first-launch-restart-provenance-off` Compose journeys; use the actual
source/artifact arguments and declared scope in
[verification](../development/verification.md). Remaining P01-T08 scenarios are
open; a planned scenario is not executed evidence. Current planning checks are
`python3 experiments/verify_artifacts.py` and
`python3 experiments/verify_handoff.py --output .research/handoff-check.json`.
The latter needs Node and the checksum-pinned first-use `--prepare` step described
in [development and delivery](../engineering/development-and-delivery.md#checks-available-in-the-planning-repository).
These check structure, links and task references, not product correctness or
semantic completeness. Read prose cycle candidates in their actual task context.

## Deliver useful slices across the whole product

The product identity is a verifiable archive and observatory. Evaluate an owner's
idea or inherited feature by the useful question it answers, required evidence,
authority, resource/maintenance cost and practical outcome. Ideas provide context;
they do not automatically expand the selected product. Adopt, improve or exclude
them explicitly through the owning contract instead of implementing every named
technology. Native MCP serves a concrete optional agent read workflow; ContextVM
remains an experimental alternative, with no extra public RPC selected merely
because another transport exists.

Follow the [milestones and cross-phase edges](README.md#coherent-implementation-milestones):
first one real signed event through admission, archive, read, standalone export
and restart; then controlled acquisition/monitoring/import; useful derived results
and real clients; operable packaging/recovery; complete browser and website
journeys. Introduce thin UI, help and documentation consumers as their real
contracts exist. P09/P10 finish their full acceptance; they do not postpone all
user experience until the backend is considered complete.

Default enablement, process placement and feature quality are independent.
An optional capability still needs its complete producer-to-user path, refusal,
recovery and documentation before it is advertised. A release manifest may omit
an unqualified optional capability with its gate explicit; that does not finish
the entire selected product. MCP and ContextVM consume the same read core while
keeping their separate protocol versions and authority; ContextVM remains
experimental under its contract. Paid DVM evidence does not gate free HTTP reads.

Select roles through the current deployment/capability matrices, with existing
dependencies, state ownership and finite resources. A disabled Monitor, Ranker
or publisher does not remove already valid event reads; archive-only operation
does not invent online capabilities. Test supported independent compositions and
their missing/stale/disabled states through actual entrypoints. Independent
roles do not require one container or crate per service.

## Review at boundaries that can amplify mistakes

| Boundary | Audit before dependent work spreads |
|---|---|
| First event end to end | Actual signed event enters, survives restart, is read and exported with exact fields; invalid-ID/signature controls fail |
| New persistent format or identity | Golden bytes, restore/import mapping, compatibility refusal and old-format fixtures; backup/replay closure |
| New durable effect/acknowledgement | Kill at every ambiguity boundary, restart owners in another order, replay retries and positively fence stale owners |
| New source or scope | Untrusted input bounds, private/public arrival order, provenance off, same-ID variants, SSRF/route and scheduling rules |
| New derived generation | Incremental result versus full independent recomputation, retractions/expiry, dirty-only work, partial materialization and activation |
| New public surface | Real client, resource/auth parity, output limits, cancellation, slow consumer, revocation and disabled-capability behavior |
| Claimed performance improvement | Equal semantics/data/loss model, aged state and mixed load, peak resources, maintenance debt, errors and useful completion |
| Distribution milestone/release | Clean install and browser journeys, usable docs, upgrade/restore, offline extension bundle, supported architecture and protection class |

These are periodic audits tied to risk, not an arbitrary required number of clean
reviews. Use an independently tasked reviewer when available and useful; label
AI review as AI review. Ask it to find counterexamples and missed work, not to
approve a predetermined conclusion. Record findings and their disposition.

## Handle decisions and discoveries honestly

Semantic choices are made before affected code. Physical engine settings and
bindings use the bounded decision records and early risk slices; do not build
two complete alternative products. A failed engine guarantee may reopen an
architecture decision. A small performance difference need not justify another
database, process, language or compatibility surface.

When new evidence changes a choice, update its owning specification, affected
tasks, fixtures, migration/compatibility consequences and user explanation in one
reviewable change. Preserve historical evidence as historical evidence. Avoid a
new parallel master plan, hidden TODO authority or a second agent instruction file.

The final implementation report identifies the exact tested revision, supported
deployment/workload envelope and remaining gates. “All tests passed,” “100k
connections,” and “zero loss” are incomplete claims without the exercised
boundary, offered workload and failure class.

## Starting the continuing goal on Tux

Under owner decision Q37 (2026-10-20), only these configurations are authorized:

| Responsibility | Exact model | Reasoning |
|---|---|---|
| Main coordinator; always | `gpt-6-astra` | `xhigh` |
| Subagents for architecture, concurrency, persistence, fault recovery, security, complex diagnosis or critical review | `gpt-6-astra` | `xhigh` |
| Subagents for bounded implementations, local fixes and work with clear contracts and verification; preferred | `gpt-6.1-sol` | `xhigh` |

Judge the actual difficulty, including tests, verification tools and documentation.
Substantial ambiguity, high risk or inconclusive Sol attempts require escalation
to Astra, still at `xhigh`. No other model or reasoning level is authorized,
including the older `gpt-6-sol`. If a configuration is unavailable, use the other
only where this allocation permits; otherwise report the limit without silently
substituting. Apply this to upcoming assignments without unnecessarily disrupting
work already in progress.

Use actual session/subagent controls for both model and reasoning. With the
current collaboration tool, set `model` and `reasoning_effort` explicitly and use
`fork_turns="none"` for overrides, supplying the bounded task and relevant context.
Reuse an agent only when its effective configuration fits the assignment. Verify
the effective settings at creation, after a change or when continuity is uncertain,
through session status or recorded runtime metadata;
a prompt, available-model catalog or global default alone is not that evidence.
Compaction alone does not require repeating model/documentation/configuration research.
Report any configuration that cannot be set or verified. This allocation preserves
all existing plan, review and verification requirements and the priority of a
complete, correct, clean, minimal, maintainable and efficient product.

The CLI exposes `/model` and `/status` for selection and verification; see the
official [commands](https://learn.chatgpt.com/docs/developer-commands?surface=cli)
and [configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference).
Writing a model name inside the goal does not change the running model.
For a new coordinator session from a Tux terminal:

```sh
codex -C /home/tux/bigbrotr-rs -m gpt-6-astra -c 'model_reasoning_effort="xhigh"'
```

Then submit the following as a goal. This is an invocation of the existing
contracts, not a separate task plan or authorization for an official release.

```text
/goal Complete BigBrotr Rust across the entire selected P00–P10 plan and M1–M5
milestones, including selected optional capabilities. Resume from the current
checkpoint and follow AGENTS.md, the reconciled mandate and execution guide.

Deliver a professional repository ready for open-source maintenance: complete,
correct, secure, robust, modular, readable and efficient implementation; necessary
dependencies/tools/configuration; current documentation and licensed notices;
reproducible build, installation and verification from a clean checkout with
declared prerequisites and obtainable inputs. The source repository is public;
this does not authorize official releases, packages, rename or production deployment.

Before first production release, remove superseded internal compatibility, code,
migrations/fixtures, duplication and implicit personal-lab dependencies when their
current consumers are understood. Consolidate deliberately, reuse valid components,
preserve Git history and all current integrity, restart, replay, recovery and
failure-handling guarantees. Retain essential evidence, open-defect reproducers,
unique data and pending work; retire only verified owned, unused, unnecessary
resources incrementally. Caches must be optional accelerators.

Demonstrate requirements with maintained independent tests where needed, real
engines/integrations, distributed-artifact end-to-end runs and direct installed
interface observations. Qualify required errors, interruption, security, durability,
cancellation, recovery and capacity; measure relevant CPU/RAM/disk/concurrency and
maintenance under the plan's workloads. Declare command prerequisites and CI,
dedicated or release placement. Never weaken an oracle or hide a failure by
changing limits, bypassing the real path or calling missing execution PASS.

Advance autonomously by coherent dependency/risk-driven increments: finish M1
with necessary foundations, then all remaining milestones. Technical suggestions
are intent; choose justified implementations without needless frameworks or
rewrites. Update code, tests, contracts and portable user guidance together. Reuse
valid reads/builds/reviews/proofs and repeat affected checks when assumptions
change or uncertainty arises. Keep the existing essential checkpoint and plan,
Q37 model controls and Q38 efficiency; use bounded agents where useful with one
integrating owner. Preserve resource, credential and release-publication limits.

Finish only when every selected requirement is implemented, integrated and
verified, with reproducible delivery independent of undeclared private resources.
Do not reduce scope to claim success. Record remaining requirements and exact
external blockers honestly while completing independent available work. FAIL,
NOT_RUN, SKIP, missing prerequisites and interrupted outcomes never become PASS.

```

Goals provide persistent continuation, not a guarantee against host failure,
service limits, interruption or missing external prerequisites. See the official
[Goals guide](https://developers.openai.com/cookbook/examples/codex/using_goals_in_codex)
for lifecycle controls. The existing resumption protocol remains necessary.
The model identifier is documented in the official
[Astra reference](https://developers.openai.com/api/docs/models/gpt-6-astra).
