# Implementation reading after the complete planning-source reread

Date: 2026-09-11. The owner requested substantial selective reading of actual Go
implementation, in addition to the vision and complete surviving phase Markdown.
This review records what that code teaches the Rust design. It neither ports Go
nor claims to audit every implementation file. No product implementation begins.

The [previous review](07-vision-and-final-recheck.md) preserves the exact planning
corpus, its recovered files, deliberate design differences and exclusions. Those
full reads are not replaced by code searches, and this source inspection does
not retroactively turn historical test comments into newly executed results.

## Read the implementation findings

| Complete selected path | Written source reasoning and manifest |
|---|---|
| Scheduling, relay sessions, guarded transport, verification, admission and scoped acquisition evidence | [Acquisition walkthrough](../research/go-acquisition-code-walkthrough.md), [source identity](../research/evidence/go-acquisition-code-sources.json) |
| Exact membership, journal append/seal/recovery, index replay and checkpoints | [Storage walkthrough](../research/go-storage-code-walkthrough.md), [source identity](../research/evidence/go-storage-code-sources.json) |
| Deriver planner/targets, current graph, social/relay ranking and signed NIP-85 delivery | [Analytics walkthrough](../research/go-analytics-code-walkthrough.md), [source identity](../research/evidence/go-analytics-code-sources.json) |
| Actual CLI/composition, process lifecycle, shared read selection and HTTP continuation | [Read/runtime walkthrough](../research/go-read-runtime-code-walkthrough.md), [source identity](../research/evidence/go-read-runtime-code-sources.json) |

Each walkthrough identifies full files read, function/line references, exact
snapshot hashes, inspected tests and boundaries where only an injected interface
or an uninspected dependency is known. Sources were copied to new ignored paths
without writing the legacy checkout. Before/after comparisons detect changes
during capture and reading; no atomic whole-repository snapshot is implied.

The four inventories contain **146 distinct complete files: 125 production Go
files, 20 test files and `go.mod`**, totaling **37,946 logical lines** and
1,559,121 bytes. No source path is counted twice. Final mechanical comparison
verified every snapshot's SHA-256, byte/line count and equality to the current
original at the time of checking. These numbers describe the reading scope;
they do not establish complete codebase coverage or production quality.

## What survives because the implementation earns it

The Go implementation contains useful careful work. Preserve permit reservation
before dialing, idempotent release and owner joins; one ordered EVENT/EOSE stream;
lazy event serialization; an explicit pending/in-flight membership state; durable
prefix ordering that lets append proceed during an earlier sync; all-target retry
repair rather than testing just base-table presence; complete-generation registry
concepts; immutable input explanations; and actual CLI assembly with required
adapters. These are concrete behaviors, not reasons to inherit every interface,
database layout, scheduler policy or comment style.

Several selected tests are also informative: deliberately pausing an in-flight
store so another cache cannot satisfy the assertion; testing both orders of an
earlier timestamp correction; inspecting SOCKS targets; or pausing a real file
seal while another append runs. Preserve their discriminating intent. A helper
file, a mocked store result, a comments-only invariant and a passing historical
suite are each narrower evidence than a working shipped service under faults.

## Concrete design changes and their owning contracts

| Code-level lesson | Selected Rust behavior and where it is written |
|---|---|
| A rotating scheduler can still reserve seats indefinitely for endless backfill | Receipt-backed bounded preemption, separately reserved live/control work, permits before allocation and distinct new-start versus active-drain policy in [acquisition](../architecture/acquisition-services.md#refinements-from-the-go-implementation-walkthrough) |
| Global new-body count is not a relay page's progress; a generic sink success conflates several outcomes | Request-local IDs, full selector checking, typed obtained/excluded/invalid/retryable outcomes and independent coverage receipts in the same acquisition contract |
| Wire order can precede asynchronous validation; dropped frames may be followed by EOSE | Bounded ordered validation/admission join and per-request loss evidence; no clean outcome overtakes omitted required input |
| A known ID and well-shaped signature can bypass an explicit Schnorr check in the inspected sink | Exact verified-payload/signature caching, new-endpoint forgery fixture, and stable canonical storage of a first valid signature in [data identity](../architecture/data-and-durability.md#identity-and-replay-refinements-from-inspected-code) |
| Cache slabs, pending maps, replay rows, lookahead and driver staging have different lifetimes | One total accounting model and admitted streaming replay units in [storage](../architecture/storage-services.md#replay-memory-progress-and-repair-boundaries) |
| Displayed replay offsets are not used as restart positions; a base insert does not prove secondary projections | Explicit recovery granularity, immutable source identities, all-required-family completion and dependency metadata before the frontier |
| A conservative repair can still read an entire corrupt tail and repeatedly checksum candidate bodies | Bounded forensic jobs and documented engine recovery; preserve suspicious source data rather than silently truncate it |
| Dirty-only planner work and configured target lists may fail to survive actual execution wiring | Valid no-new-input progress, target selection that stops actual work, precise touched sets and cut-bound reads in [analytics](../architecture/analytics-detail.md#execution-refinements-from-the-go-code-walkthrough) |
| Few output buckets do not make exact quantiles small; warm rank kernels omit graph build and mapping | Budget full input state/spill and all graph phases; independent social/relay outputs, with intentional algorithm changes versioned and explained |
| Child-entry-only state forgets an empty winning list; edge-only graph loaders can forget isolated authors | Independent winner headers, declared vertex membership and older-backfill/full-rebuild comparison in the same analytics contract |
| PID/hostname can repeat, a cockpit probe is advisory, and response timeout may abandon a live driver call | Process-incarnation nonce, real writer exclusion, bounded coalesced probes and positive shutdown escalation in [runtime](../engineering/configuration-and-runtime.md#ownership-and-probes-at-actual-process-boundaries) |
| Delivered-row cursor position can disagree with withheld scanned candidates | Policy before limit where possible, protected scan continuation when needed, and actual mixed/all-withheld next-page journeys in [read continuation](../architecture/derived-read-publication.md#visibility-and-real-pagination-continuation) |
| Per-instance random cursor keys break restart or multi-worker continuation | Persisted shared keyring, rotation and authenticated encryption of private positions in [cursor key ownership](../architecture/derived-read-publication.md#cursor-key-ownership-and-confidentiality) |
| Decoded comma splitting and dropped empty values lose valid tag meaning; absent deletion state is not time zero | Lossless typed GET/POST query encoding, explicit absent horizons and current-before-filter-before-limit in [HTTP/current semantics](../architecture/derived-read-publication.md#lossless-http-filters-and-publication-traversal) |
| Subject builders can be unwired; a whole-population batch ceiling and one-ACK state lose useful publication work | Paged complete subject generation, activated binding metadata and durable per-destination exact-byte delivery in the same publication contract |
| A generic signer or ordinary blocking path open is broader than application signing authority | Purpose-bound validated requests, safe bounded opened-descriptor checks and explicit supported credential sources in [security validation](../engineering/security-recovery-and-validation.md) |

The [practical scenarios](../engineering/security-recovery-and-validation.md#scenarios-recovered-from-actual-go-call-chains)
connect these decisions to actual entry points and independent witnesses. They
are future implementation obligations; source inspection alone does not satisfy
them. Specific potential failures found by static reasoning retain their scope
qualifiers in the walkthroughs rather than being presented as reproduced incidents.

## Architectural judgment after reading the code

The selected architecture remains Rust application roles, a transactional control
plane, atomic local admission/pending storage, transactional Parquet archive and
rebuildable bounded online reads/analytics. Reading the Go path strengthens the
case for fewer ambiguous boundaries: one admission receipt, immutable source
identity, one policy-bearing read context and independent target generations.
It does not prove Rust automatically uses less RAM or that Go cannot serve the
requested fleet. Serialization, temporary copies, fairness, index state, recovery
work and protocol evidence matter in either language.

Do not reproduce the Go custom journal's repair machinery just because much work
went into it. Keep the durability invariants and independently validate the
chosen RocksDB/DuckLake/CH path. Conversely, do not discard strong ordering or
in-flight ownership patterns merely because the new language offers async tasks.
Changing seeded ranking/mute behavior and score normalization is a new algorithm,
not a faithful port; its definition/version and publication identity must say so.

Current invariants belong beside their code with enough explanation to maintain
them. Long incident histories and procedural mandates belong in linked design or
review records. The aim is clear idiomatic code, not arbitrary comment limits,
tests that merely copy implementation, or a crate per historical service name.

## Evidence and remaining boundary

This pass reads code and tests, preserves and verifies source bytes, documents
call-chain reasoning and amends the plan. It runs no Go/Rust product suite,
production process, relay workload or new capacity benchmark. It changes no legacy
source or live service. Artifact checks establish links, manifests, formatting
and traceability, not architectural correctness or production capacity.

Final artifact verification on the report date found no errors across 45
Markdown files, 33 JSON files and four JSON examples. A separate local-target
check resolved 573 links and heading anchors against reviewable repository
files. All 146 captured source hashes, sizes and original comparisons matched;
`git diff --check` reported no whitespace errors. No product test result is
implied by these document and evidence checks.

The handoff remains [PLAN.md](../../PLAN.md). All actionable findings above have
an explicit selected behavior in an owning contract. Future implementation must
prove those behaviors through the stated real scenarios, refine physical choices
when evidence requires it, and keep those decisions written rather than implicit
in an agent's memory.
