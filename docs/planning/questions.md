# Questions, design selections and owner context

Technical suggestions from the owner are hypotheses to evaluate, not orders to
adopt particular technologies. The designer compares alternatives, selects a
coherent design and explains departures. Unanswered questions do not block the
planning work; the selected defaults below remain revisable with new evidence.

Operational instructions remain explicit: private temporary repository, legacy
repository read-only, English repository content and coherent commits/pushes.
Q30 records the explicit transition to implementation; earlier stage statements
below are historical.

| ID | Topic | Input or answer | Selected treatment |
|---|---|---|---|
| Q01 | Repository identity | Private `bigbrotr-rs` locally/GitHub now; eventually `bigbrotr` | Preserve BigBrotr brand; plan final transition without renaming current legacy repository |
| Q02 | Deletion and history | Retain original events and deletion requests; consider serving policy separately | Full historical archive; ordinary relay/current reads honor logical deletion; explicit authorized history resources |
| Q03 | First-launch priority | Every service/product area matters equally | Full scope and equal quality; order by dependencies; coherent local experience with explicit external exposure/signing |
| Q04 | Lossy lightweight profiles | Signature retention matters; old content-loss combinations may be removed; slim exports may suffice | Retain all signed fields/signature, remove heavy_fields storage axis and old Lil/Index runtime semantics; analytical projections labeled non-verifiable |
| Q05 | Active write probes | No separate answer received | Optional bounded probes with dedicated configured identity; passive monitoring fully works without signing |
| Q06 | Imported provenance | No separate answer received | Retain when selected as attributable third-party claims; never relabel as direct local observations |
| Q07 | Friendly recipe names | LilBrotr might mean complete archive without provenance; SocialBrotr may just be filters | One product/image, transparent recipe examples; no silent reinterpretation of old profile names; provenance orthogonal to social filtering |
| Q08 | Multiple users/datasets | No separate answer received | Shared instance dataset with scoped visibility/quotas; tenant-isolated archives are a distinct future topology |
| Q09 | Private web management | Strong interest in accessible private management as well as public dashboard | Include CLI, public explorer and private administration with roles, config revisions, jobs, diagnostics and recovery |

Additional owner context incorporated into the design:

- Examine 5k, 10k, 50k and 100k endpoints, billions of unique events and tens or
  hundreds of billions of provenance relations, under simultaneous workload.
- Prefer incremental computations where valid, but account for global graph,
  exact set and normalization work rather than promise constant-cost updates.
- Improve NIP-66/NIP-85 concepts through compatible current behavior and explicit
  proposed extensions; no upstream submission is part of this stage.
- Low TrustRank should not discard historical evidence. Ranking is an attributed
  viewpoint and may change with inputs, algorithms or curator.
- Accessibility, installation, explanations, examples and maintenance matter to
  adoption alongside efficient, readable and carefully reviewed code.

The complete design records remaining empirical validation tasks in PLAN.md.
They are not unanswered product questions or permission requests. The future
implementation must supply evidence before making production-capacity claims.

## Additional owner context: search, reuse and final source reread

| ID | Context | Design disposition |
|---|---|---|
| Q10 | Content search matters; OpenSearch was only a suggestion | Required supported feature with optional deployment index; CH baseline, Tantivy/OpenSearch evaluated against explicit semantics and workload |
| Q11 | Clients should reconcile against the archive as a relay | Add server NIP-77 to ArchiveRelay, independent of acquisition/provenance, with authorized retrievable sets and bounded shared indexes |
| Q12 | Service/container/crate boundaries and reuse are unclear | One image, selectable roles and finite jobs; fewer initial libraries, deliberate reusable APIs; explain compilation versus execution |
| Q13 | Reread original vision and every phase-0–9 Markdown, persist everything needed by a future implementer | Expand source inventory beyond phase READMEs, record requirement dispositions and fix discovered design gaps; no claim that planning proves production capacity |

These decisions do not authorize product implementation yet. The owner's request
remains a complete, explicit implementation baseline followed by a separate start
of the build stage. Research conclusions and performance targets remain revisable
when real implementation evidence contradicts them.

The latest owner clarification reinforces that the original Go feature list is
not mandatory. The product/data/feature decision catalog evaluates useful
questions, retained evidence, indexes, public APIs and distinct score meanings;
features may be removed or improved when their value does not justify their cost.

The owner additionally requested substantive complete-file reading of important
Go implementation paths. The [code walkthrough review](../reviews/08-go-code-walkthrough.md)
persists what those paths actually do and what the Rust design retains or changes.
The source remains read-only; tests were inspected as source, not reported as
executed. Implementation remains a later explicitly started stage.

## Detailed implementation breakdown and archive-value reconsideration

| ID | New owner context | Selected disposition |
|---|---|---|
| Q14 | Go THE PLAN is incomplete and moving; surprises reveal Rust planning gaps | Qualify captured code findings, recheck external plan identity, replace reliance on overview with phase/task/step specifications |
| Q15 | Specify all work and reconsider it at least five times before implementation | Expanded phases00–10, field/component/import/DVM contracts, coverage ledger and five actual correction rounds; future tests remain unchecked |
| Q16 | Permanent archive should retain useful ecosystem evidence, not duplicate-ingestion logistics | Keep full signed events, first observations/corrections and actual compact monitoring evidence; remove passive delivery histories/counts; operational TTLs retain correctness obligations |
| Q17 | Challenge storage value and duplication rather than assume a layout is efficient | Actual tiny Parquet experiment found object/reference files larger than a compressed wide layout; measure physical alternatives in P02-T09 |

The [expansion review](../reviews/09-expanded-plan-rounds.md) and
[coverage ledger](../implementation/coverage.md) record the chosen consequences.
Private DVM payloads stay operational by default; public scientific signed outputs
retain their originals/compact lineage. Provenance-off excludes interpreted
imported event-to-relay sidecars and intentional availability pairs as well as
ordinary witness indexes. No unanswered product question prevents this planning
handoff; actual platform/engine/WAN limits remain explicit measurement work.

## Complete-document review and limits of validation

Q18: The owner asks whether all new-repository documents were actually reread
and every choice validated as best possible. The prior pass did not establish
that claim. The [fresh full sweep](../reviews/10-whole-repository-audit.md)
identifies 64 complete baseline Markdown reads, new defects/corrections and the
remaining empirical limits. [The confidence register](decision-confidence.md)
records reasoned choices and alternatives without claiming a global optimum.
No additional owner answer is required; product implementation has not begun.

Q19: The owner requests another complete reread, newcomer-readable organization,
exact table/interface/naming decisions, document-layout alternatives, user/relay
ranking and review plus real practical validation for each submitted change.
[Review 11](../reviews/11-whole-repository-audit.md) records the 70-file baseline
and subsequent counter-review. The new catalogs distinguish logical data from
physical DDL, permanent scientific evidence from rebuildable state, and public
resources from database tables. A controlled eight-case Parquet experiment finds
both layout winners; no unconditional document-normalization claim remains.
Temporal views and named rank/assertion definitions now have concrete oracles.
Implementation review is required by impact, without arbitrary clean-round quotas.
Selections are autonomous design judgments, not new owner technology mandates.

Q20: The owner asks what still needs specification before implementation and
accepts practical decisions only where doing so makes sense. The
[readiness assessment](specification-readiness.md) separates genuine semantic
gaps, contract consolidation, empirical design choices and private coding detail.
S01–S08 are explicit remaining paper work under P00-T06–T10; writing those tasks
does not close them. No new permission question or product implementation follows
from this assessment. Earlier completed reviews retain their historical scope.

Q21: The owner asks for a deeper challenge of the plan's actual choices, from
product architecture to algorithms/frameworks/representations, including
independent opinions, before discussing changes. The
[architecture challenge](../reviews/13-architecture-alternatives.md) reconciles
three separately tasked AI reviews, counterarguments and bounded experiments.
They are not external human certification. Recommendations remain advisory;
the selected specifications and P00 closure status are unchanged. No percentage
of optimality, new product benchmark or whole-repository reread is claimed.

Q22: The owner reinforces that critical choices may need elaborate, large and
realistic experiments and makes `ssh tux` available. Research is no longer
described as limited to small probes. The
[extended qualification protocol](../engineering/capacity-and-benchmarks.md#extended-preimplementation-qualification-on-tux)
defines when to escalate and what evidence can decide the alternatives. A fresh
[read-only preflight](../research/evidence/tux-preflight-20260917.json) confirms SSH
access, current resources, shared active workloads and RAM-backed `/tmp`.
No new large experiment or product implementation is claimed by that preflight.

Q23: The owner explicitly declares everything already on Tux relating to BigBrotr
deprecated and to be deleted. The remote retirement supersedes earlier protection
of those particular resources; local repositories and unrelated Tux applications
remain untouched. The [retirement record](../research/tux-retirement-20260917.md)
documents scope and verification. Prior labs are unavailable for reuse; new
research must recreate dependencies and fixtures deliberately.

Q24: The owner delegates resolution of remaining design choices and asks for a
clean repository another implementation agent can execute, with meaningful
periodic audits and practical validation. The selected S01–S08 contracts, agent
handoff and current readiness register replace the prior open disposition.
ContextVM and MCP have concrete optional gateway contracts; generic public RPC
is not selected. Product implementation has not started. New isolated Tux
qualification uses a fresh separately inventoried lab; actual results and remaining
failure/scale limits belong to the evidence dossier, not implied approval claims.

Q25: The owner clarifies independent service selections, ordinary-hardware setup,
optional external S3 capacity, split hosts, rich monitoring/assertions and reliable
agent continuation. Select explicit role/capability dependencies, fixed endpoint
sets without discovery, local archive by default and qualified S3 where placement
requires it. Native and Compose share semantics; neither Docker nor shared PG
provides automatic failover. Current contracts and review16 explain the choices.

Q26: The owner reiterates that ideas are hypotheses and requests complete relevant
file reads. BigBrotr's identity remains verifiable Nostr archive/observatory with
useful client/research access. Keep concrete optional MCP; ContextVM experimental;
no generic public RPC merely for completeness. Directory navigation and full
owning-file/caller/contract reads precede changes; searches locate material but
cannot stand for review coverage. One checkpoint preserves verified continuation.

Q27: The owner emphasizes CH analytics, rich dashboards, Nostr reads and research.
Select shared full online bodies plus narrow query paths and complete derived
generations. Maintain frequent facts incrementally with replay/retraction/expiry
ownership, not naive sum-on-insert views. Alternate populations and costly analysis
use bounded jobs/artifacts; direct SQL is a separate authorized research/admin
route, not an unrestricted public read-core endpoint. Actual CH counterexamples
and measured layout candidates inform the design without claiming universal
optimality or unlimited billion-row capacity.

Q28: The owner prefers ordinary deployment mechanisms over a bespoke multi-host
system. Keep single-host Compose with local archive as the primary path; standard
remote database/RPC/S3 endpoints retain their explicit capability, authority and
qualification requirements. Docker alone provides no cross-host orchestration.

Q29: The final handoff request adds a concrete developer/delivery workflow.
Select Mac authoring/Git plus exact-revision Tux Linux experiments, isolated
GitHub Actions CI and Docker Hub image distribution. The owner permits linear
coherent commits without mandatory branches/merges. All Cargo packages remain
unpublished initially; clean internal libraries do not force a public SDK.
The [delivery contract](../engineering/development-and-delivery.md) closes runner
trust, native target evidence, candidate promotion, signed release manifests and
ordinary Compose initialization/upgrade. Docker Hub namespace/credentials and
real release keys are provisioning inputs for implementation/release, not
resources created by this planning pass. No product workflow has run yet.

Q30 (2026-09-17): The owner explicitly starts autonomous implementation of the
complete selected product and authorizes edits, builds, isolated experiments,
coherent commits and verified private pushes. Begin with foundation/storage risk
slices leading to the first durable signed event, then continue through all
selected services, interfaces, operations and website work. Do not stop at initial
milestones or treat optional enablement as permission to omit selected work.
Keep current task IDs and the single progress checkpoint; no competing plan.
Mac remains the editing/Git authority; native Linux evidence uses exact Git
revisions and inventoried isolated Tux resources. Preserve unrelated workloads,
the local Go checkout and private distribution boundaries. Ask only for missing
information or authority that materially prevents a correct next step. Report
executed evidence, measured limits and remaining gates honestly, in Italian.

Q31 (implementation decision, 2026-09-17): Exact source inspection found that the
candidate Nostr SDK's ID/key deserializers accept alternate textual encodings and
would add a second crypto version. Use BigBrotr's strict lossless model with direct
maintained `secp256k1` 0.33.1 verification, retaining u64 time/u16 kind and the
pinned NIP baseline. This is an autonomous dependency-boundary decision under Q30,
not a new owner requirement or a change to admitted-event semantics. The
[selection record](../research/dependency-selections.md#implemented-event-verification-dependency-slice)
and [protocol reference](../reference/protocols.md) record evidence and remaining
native/runtime/storage qualification. Unknown wire extensions remain bounded
and excluded from signed content; their presence grants no authority.

Q32 (implementation decision, 2026-09-17): Use maintained `minicbor` 2.3.0 without
optional features for the selected deterministic-CBOR recipes. Exact source
inspection found its direct streaming encoder suitable; its permissive decoder
does not enforce the profile. BigBrotr owns closed typed mappings and, before
persistent decoding, bounded shape checks plus canonical-byte comparison.
This does not change profile-1 bytes or reopen storage selection. The
[selection record](../research/dependency-selections.md#implemented-fingerprint-codec-slice)
records the BlueOak license notice and implemented versus future boundaries.

Q33 (2026-09-18): Hosted GitHub Actions jobs cannot start because of the account's
billing/spending restriction. The owner explicitly directs continued work and CI
checks on Tux without depending on GitHub's runner engine. Use the same checked-in
commands for Actions and deliberately invoked isolated Tux runs of trusted exact
Git revisions. Record native Linux amd64 results independently; do not relabel
them as GitHub job success or native arm64/macOS evidence. This does not authorize
untrusted PR execution on the shared host, billing changes, added spending,
release credentials or publication. Docker Hub and private candidate/release
boundaries remain unchanged.

Q34 (2026-10-12): The owner asks to recover interrupted BigBrotr development and
continue entirely on Tux: editing, Git, testing and research. Reconcile the private
GitHub baseline with retained local source before resuming; preserve unpublished
work, original evidence and unrelated applications. Prepare an explicit cleanup
plan and a full-product Codex goal using Astra with xhigh reasoning and bounded
subagent collaboration under the existing guide. Replace the former Mac/Tux
authoring split in current instructions, without rewriting historical evidence.
Use `/home/tux/bigbrotr-rs` as the single integrating checkout, separate writable
worktrees for parallel implementation, and isolated immutable run inputs.
The [recovery audit](../research/tux-resumption-20261012.md) records the verified
baseline, space candidates, tooling and limitations. No new architecture, public
release, removal of retained datasets or arbitrary quality quota is implied.

Q35 (2026-10-12): After reviewing the recovery, the owner appoints this session
the sole integrating owner and explicitly requests activation of the goal to
complete the entire selected product. Autonomous implementation, verification,
coherent commits, verified private pushes and evidence-preserving resource
maintenance continue on Tux. The full-product goal is now active; the existing
task plan, execution guide and single checkpoint remain authoritative. Use bounded
subagents where useful under that guide, with one integrating source owner.
Ask only for missing information or authority that blocks a correct required
outcome. Preserve retained datasets, negative fixtures and unrelated applications;
the instruction does not authorize public release or production deployment.

Q36 (2026-10-15): The owner explicitly directs integrating current project work
into `main` and deleting unnecessary branches while continuing the full goal.
Keep main as the current integrated project state; use fast-forward integration
where ancestry permits and preserve all qualified evidence and explicit remaining
gates. Retire redundant local/remote branch refs only after checking exclusive
commits, active worktrees and integration; preserve worktree contents, ignored
`.research` artifacts and external labs. Temporary active implementation branches
remain available until their useful work is reconciled. This does not authorize
removing evidence, rewriting published history or expanding public release scope.

Q37 (2026-10-20): The owner authorizes only `gpt-6-astra` and `gpt-6.1-sol`,
both exclusively with `xhigh` reasoning. The coordinator always uses Astra;
architecture, concurrency, persistence, fault recovery, security, complex
diagnosis and critical review subagents also require Astra. Prefer Sol for bounded
implementation, local fixes and work with clear contracts and verification.
Assess actual difficulty even for tests, tools and documentation; escalate
substantial ambiguity, high risk or inconclusive attempts to Astra. No other
models/efforts, including `gpt-6-sol`, or silent unavailable-model substitutions.
Select through actual controls and verify effective configuration; report limits
honestly. Apply to upcoming assignments without unnecessary interruption of work
already underway. The [existing execution guide](../implementation/agent-handoff.md#starting-the-continuing-goal-on-tux)
owns the operational allocation; the same plan, checkpoint, quality requirements
and autonomous full-product goal remain in force. Earlier decisions remain
historical records.

Q38 (2026-10-20): The owner refines execution efficiency without reducing selected
scope, correctness, robustness or code quality. Reuse verified reads, reviews,
tools and results while relevant code, dependencies, configuration, inputs and
artifacts remain unchanged; repeat affected checks on change, anomaly or lost
certainty. Q37 remains unchanged; verify effective model/effort at creation,
after a change or uncertain continuity, not merely because of compaction.
Keep the current checkpoint concise and link detailed evidence; reconcile its
objective, revision/local changes, open problems, active processes and next step
on resumption. Preserve resource ownership/isolation checks before mutation and
space checks before significant allocation, low headroom or changed conditions;
avoid unrelated complete inventories. Retain real-engine evidence, independent
expectations and critical review, with targeted checks for mechanical/documentary
fixes and particular scrutiny for verifier changes affecting PASS/FAIL. Prefer
parameterized verified tools; preserve original outcomes and artifact identities
in their owning records without copying them across checkpoint/report/inventory.
Group recording, commits and pushes around coherent outcomes. Apply through the
existing execution guide and continue the same goal; no separate optimization
project or historical rewrite.

Q38 clarification (2026-10-25): The owner requests safe suspension and a durable
execution correction, retaining Q37, all selected product scope and correctness,
durability, authorization, cancellation, recovery and code-quality obligations.
No deadline or reduced functionality is imposed. The external October25 audit
reported real frequent increments still concentrated on P02 prerequisites,
relatively short builds/checks, avoidable verifier assumptions and fragmented
implementation/verification/recording/integration. These observations do not make
checks unnecessary; commit/test counts measure neither waste nor quality.

Choose coherent increments that demonstrate a useful capability. State the expected
result, actual prerequisites and necessary checks at the start; a substep does not
automatically open another refinement chain. Reuse valid source reads, reviews,
lab configurations, binaries and results. Verifier/documentation-only changes do
not automatically require product rebuilds. Prefer extending existing tools and
cheap independent checks against retained captures before required real runs;
avoid a new generic framework. Classify failures promptly as product, fixture/
verifier, environment or unknown, then correct that cause. Preserve the original
failure and independent expectations; never fit an oracle to observed output,
widen limits for PASS or weaken safety guarantees. Repeat critical reviews for
changed evidence or unresolved questions, not unchanged material. Reduce duplication
when touching its owners without a general rewrite. Group records/commits/pushes
around coherent results, keeping interruption checkpoints timely and compact.

At resumption select by dependency, risk and useful capability unlocked. The
checkpoint must distinguish actual M1 signed-event admission/archive/read/export/
restart blockers from later-milestone work. A deep prerequisite needs its concrete
requirement or risk; all of P02 is not an indivisible gate. Later work stays in the
existing plan, incomplete. Report what works, supporting evidence, remaining
blocker and next demonstrable result. No parallel plan, metrics system or repeated
historical audit is requested. After preserving work and pushing ready commits,
set the actual goal PAUSED and wait for the owner's explicit resume.

Q39 (implementation decision, 2026-10-23): Begin acknowledged-detail reclamation
with a completely retired nonempty initial producer. Reuse immutable PostgreSQL
outcomes, the existing source closure and typed checkpoint; do not introduce a
second per-request proof ledger. The [selected native floor](../reference/producer-request-inputs.md#closed-producer-reclamation-reader-under-qualification-deletion-unavailable)
binds those records in a versioned ProducerState value. Install the floor and
remove exact native receipt keys atomically, then unlink authenticated original
inputs only after observed synchronization success. Preserve all scientific and
pending data. First implement the non-destructive witness and shared validation;
do not enable deletion until recovery and existing successor consumers support
the compacted prefix. This is an autonomous implementation choice under Q35,
not an owner-directed scope change or a claim of qualified GC, unlimited traffic
or backup support. The existing recovery report retains the critical countercheck
and remaining practical gates.

Q40 (2026-10-26): The owner clarifies delivery and requests a safe pause of the
incomplete goal before replacement. Scope remains every selected P00–P10
requirement and M1–M5 milestone, including optional capabilities. Technical
suggestions express intent; choose justified solutions. No deadline, feature
reduction or weaker correctness, security, data or verification guarantee is implied.

Deliver a professional maintainable repository: complete code, necessary tools,
tests, configuration, dependencies, licenses and build/install/verification/user
procedures reproducible from a clean checkout with declared obtainable inputs.
Neither the session, .research, personal paths, old containers nor unrecreatable
private artifacts may be prerequisites. Promote useful stable exploratory
reproducers into maintained infrastructure. Declare commands and CI/dedicated/
release prerequisites; real installed-interface observations complement independent
automated checks. Retain required errors, interruptions, restart, recovery,
security and capacity checks; FAIL/NOT_RUN/unknown never become PASS.

Before the first production release, internal iterations are not supported
versions. Supersede indiscriminate additive-migration and internal-preservation
rules: remove genuinely superseded compatibility, code, adapters, migrations,
fixtures and dependencies after understanding current consumers and guarantees.
Consolidate schema/bootstrap deliberately; a private commit/push is not a support
promise. Preserve current operational transitions, corruption/incompatibility
checks, integrity and recovery. Reuse valid code; do not rewrite Git history or
working components for cosmetic novelty. Keep responsibilities, errors and resource
ownership clear; abstractions need real use. Measure relevant CPU/RAM/disk,
concurrency, backpressure, cancellation, maintenance and recovery under the plan's
workloads; partial benchmarks/source reading do not prove system performance.

Retention is proportional: essential current evidence, open-defect reproducers,
unique data, unintegrated work and safe continuation. Verify ownership, actual use,
dependencies and regeneration before removing obsolete owned resources; a missing
text-search hit is insufficient. Preserve other workloads. Neither indefinite
retention of every attempt nor a general cleanup campaign is required. Use existing
P01-T08, P08-T11/T12 and P10-T08/T10 for acceptance, delivery and final documentation.
User guidance describes current portable procedures, not the host/session/history.
Replace superseded operational rules in place; retain attributable historical
decisions and evidence without treating them as current mandates.

Continue coherent increments by dependency, risk and useful capability: finish M1
then every remaining milestone. Q37/Q38 remain; reuse still-valid reads, builds,
reviews and proofs, repeating affected checks on change, anomaly or uncertainty.
Preserve resource, credential and publication limits. For this transition, save
and privately push coherent work, preserve pending work, stop owned autonomous
activity and update the existing checkpoint. Set the actual goal PAUSED, never
complete merely to replace it, and never manipulate internal Codex data. If an
incomplete goal cannot be cleared here, the owner clears/starts the replacement
through supported controls. Full consolidation continues under the new goal; it
is not a prerequisite for this safe pause.
