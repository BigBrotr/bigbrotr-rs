# Current mandate

## Tux-only resumption — 2026-10-12

Q35 activates the continuing full-product goal and appoints this session the sole
integrating owner. Continue implementation, verification, private Git delivery
and evidence-preserving maintenance autonomously through the existing selected
scope. The goal is active, not merely prepared for another session.

The owner requests recovery of interrupted development, preserving current work,
reconciling GitHub with Tux, a concrete resource-retirement plan and preparation
of the continuing full-product Codex goal. Tux is now the sole authoring/Git,
development and test host; the Mac is no longer required. This overrides the
older host split below, without changing the selected product or its quality,
private distribution and evidence obligations.

The canonical working checkout is `/home/tux/bigbrotr-rs`. GitHub remains the
published baseline; recovered unpublished commits retain their own branch until
qualified and reconciled. Old lab snapshots are evidence, not writable development
copies. See [Q34](questions.md), the [delivery contract](../engineering/development-and-delivery.md)
and [resumption audit](../research/tux-resumption-20261012.md). Preserve retained
datasets, fault states and unrelated workloads while identifying regenerable
caches. The 2026-09-17 retirement authorization is not permission to erase labs
created afterwards. The existing checkpoint and task plan remain the only
execution authorities; this recovery session does not declare the product done.

## Owner-started implementation — 2026-09-17

The owner explicitly authorizes autonomous implementation of the complete selected
product: Rust backend, storage, acquisition, analytics/ranking, publication, read
surfaces, public dashboard, private administration, website/documentation/blog,
packaging, CI/CD and practical validation. Follow the existing task IDs and
cross-phase slices, starting with a real signed event through durable admission,
archive, read/export and restart. Scaffolding or passing unit tests do not complete
this objective. The authorization supersedes the historical planning-only stage
below and persists through resumption.

Use Tux as the editing/Git authority and inventoried exact-revision Tux labs
for native Linux qualification (host policy updated by Q34). Coherent commits and
verified pushes to this private repository are authorized. Keep all initial Cargo packages unpublished;
no public release, repository rename or change to the neighboring Go checkout is
authorized. Preserve unrelated workloads and retained current experiments.
Use the [execution guide](../implementation/agent-handoff.md),
[delivery contract](../engineering/development-and-delivery.md) and existing
[checkpoint](../implementation/progress.md); do not create a competing master plan.
Routine decisions are autonomous; material evidence-driven design changes update
their contracts, tasks, compatibility consequences and tests together.

On 2026-09-18 the owner explicitly directs continued implementation and CI checks
on isolated Tux resources while GitHub-hosted runners are blocked by billing.
[Q33](questions.md) preserves exact-source, runner-trust and native-target
boundaries. Share the checked-in CI commands; a Tux report is separate evidence,
not a fabricated successful GitHub job.

## Historical planning mandate

The objective is a complete, implementable design for BigBrotr, reconsidered
from its product purpose rather than inherited implementation choices.

The owner authorizes creation of this repository, research, isolated experiments,
coherent commits, and pushing the planning work. The current Go repository remains
read-only. Product implementation is explicitly outside this stage. The owner
confirmed a private `BigBrotr/bigbrotr-rs` repository during planning, with eventual
transition to the GitHub name `bigbrotr`; this does not authorize renaming the
existing repository during the present stage.

The design must account for every original service, the historical vision and
master plan, all nine planned development phases, the public dashboard, and a
distinct website/documentation/blog workstream. Existing behavior may be retained,
reorganized, replaced, or removed with an explicit explanation.

Each service needs a defined purpose, inputs, outputs, ownership, dependencies,
configuration and defaults, lifecycle, failure behavior, security boundary,
resource model, operator experience, and practical acceptance evidence.
Services must be independently selectable and deployable where useful. Their
data dependencies and finite buffering must remain explicit.

Configuration should expose useful policies and operational controls with good
defaults and small user overrides. Implementation accidents do not deserve
permanent configuration knobs. Unknown, contradictory, unsafe, or ineffective
configuration must be handled deliberately and documented.

The design must optimize computation, memory, disk, network, reads, writes,
imports, exports, and operational effort while preserving fidelity and honest
completeness claims. Tux is a useful single-host reference; multi-host growth and
failure recovery are part of the design, not assumed consequences of Docker.

Rust is the intended implementation language, subject to evidence about library
capabilities and boundaries. RocksDB, Parquet, DuckLake, PostgreSQL, ClickHouse,
DuckDB, and OpenSearch are candidates from the preceding investigation, not
owner-mandated components. Justify each adopted dependency and its alternatives.

The owner requests at least five or six substantive reconsiderations of the plan.
Use six distinct revisions grounded in further reading, practical experiments,
questions, and cross-component analysis. Do not substitute repeated stylistic
passes or a quota of clean reviews for learning.

Communicate progress in simple, detailed Italian and ask product questions
throughout. Record answers and label assumptions. Do not delegate technology
choices to the owner merely because they involve a tradeoff.

Verification must include real execution through user-facing entry points,
realistic workloads, failure/recovery, and tests appropriate to each risk.
Unit tests alone and code written only to satisfy its own tests are insufficient.
Do not impose arbitrary style or process limits such as maximum comment length,
fixed coverage percentages, or mandatory repeated defect-free reviews.

Deliver a clean English planning repository that another engineer can implement
in a justified order, with concrete contracts, validation scenarios, source
references, unresolved release risks, and a readable explanation for the owner.

The owner clarified that every product area is important. Do not seek a preferred
service to justify neglecting others. Default enablement, implementation order,
and equal quality obligations are different concerns; sequence by dependencies
and evidence while planning the entire product.

The owner requires retaining event signatures and invited removal of lossy
LilBrotr/IndexBrotr storage profiles. The adopted design recommendation is full
signed-event archival, optional provenance, selectable acquisition filters and
serving indexes, and explicitly projected analytical exports. Low TrustRank is
not grounds for automatic archival deletion. This replaces the old heavy_fields
configuration axis; legacy incomplete datasets require honest migration handling.

The capacity design must explicitly examine 5k/10k/50k/100k relay endpoints,
billions of unique events and tens/hundreds of billions of provenance relations,
under simultaneous acquisition, reads, ranking, export and maintenance. Avoid
avoidable bottlenecks, measure representation/amplification costs, and preserve
bounded overload behavior; no claim of unlimited physical capacity is justified.

The owner's latest refinement requires the entire selected product on paper as
phases, tasks and numbered steps, followed by at least five substantive expansion
rounds. The prior overview is insufficient alone. The external Go THE PLAN is
unfinished context; findings cannot be attributed to its future corrected state.
Retained fields must earn their scientific value: actual relay measurements and
first observations are useful; internal duplicate counts, retries and queues are
bounded operations. Record corrections and practical evidence, including results
that overturn an efficiency assumption. Do not begin product implementation.

The owner further accepts leaving decisions to practical evaluation only where
that can actually improve the choice. Close semantic, authority, persistent-format
and client-behavior ambiguities on paper. Bound empirical alternatives by an
explicit experiment and decision point; leave private implementation details to
code review instead of manufacturing false precision. The current remaining
paper work is recorded in [specification readiness](specification-readiness.md).

The owner also requests a challenge to the selected solution itself: alternative
approaches, algorithms, implementations and frameworks that could materially
improve the product, including independently reasoned reviews. Record precise
recommendations and counterarguments for discussion, beyond a completeness
checklist. Do not equate independent AI reviewers with external human experts,
or silently adopt conflicting alternatives as another normative specification.

The owner explicitly makes `ssh tux` available for substantial, realistic and
large-scale research when needed to decide critical alternatives. Do not constrain
qualification to tiny fixtures or defer decisive scale effects until the product
is built. Scale isolated comparative prototypes enough to expose cache limits,
maintenance debt, mixed-load contention and recovery costs. Preserve the legacy
repository and unrelated services. This expands research authorization, not the
authorization to begin product implementation or assume ownership of public relays.

The owner's subsequent explicit instruction retires all pre-existing BigBrotr
material on Tux and authorizes its deletion, including project containers, data,
checkouts, runners and old research environments. This supersedes protection of
those remote project resources. The neighboring local Go repository and unrelated
Tux workloads remain protected. New experiments start with a fresh inventoried
environment; historical measured evidence retained locally is still qualified
evidence, not an endorsement of the retired deployment.

The final handoff refinement selects the local Mac as primary authoring/Git
workspace, with exact-revision native Linux qualification on Tux and isolated CI.
Use standard Docker Compose installation and Docker Hub application images;
GitHub Actions may run CI without using GitHub Container Registry. Keep complex
multi-host operation optional and operator-managed. All initial Rust packages
remain unpublished. Linear coherent commits are acceptable; branches and parallel
agents are used only when useful, with explicit ownership. The
[development and delivery contract](../engineering/development-and-delivery.md)
records these reasoned decisions, practical requirements and remaining release
provisioning inputs. Product implementation is still the next separate stage.
