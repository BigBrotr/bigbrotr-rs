# Expanded implementation specification: five substantive review rounds

Date: 2026-09-11. Scope: **planning and isolated research only**. The new
[work breakdown](../implementation/README.md), [coverage ledger](../implementation/coverage.md)
and [field contracts](../specification/data-contracts.md) replace reliance on the
previous overview as an execution plan. No product crates/services/UI were built.
Unchecked implementation steps and proposed commands remain unexecuted.

## What was missing and what the Go context means

The previous phase table was too coarse for the owner's requested handoff. It
identified systems and broad acceptance but left field meanings, some concrete
producers, adapter profiles, backend ownership and crash boundaries implicit.
Those are planning gaps, even where a high-level architectural choice was sound.
The proper response is to expand and challenge the contracts, not declare future
quality from existing toy tests or from the number of pages written.

The external Go plan is `/Users/vincenzo/Documents/GitHub/BigBrotr/BIGBROTR-THE-PLAN.md`.
Its 825-line, 54,782-byte file still matches the previously read SHA-256
`e99abda1190ba48279fb9ee9d31b6e36d3f5d11f1b27ad3951831f802e200c09`.
This pass rechecked that identity and reread its opening context. It did not
repeat or claim a new complete read of all historical files. The prior complete
[vision/phase reread](07-vision-and-final-recheck.md) and selective complete
[Go code walkthrough](08-go-code-walkthrough.md) remain the source inventories.
The Go work is unfinished and its worktree is changing. Captured observations
describe their dated versions; they do not establish the final Go implementation's
faults, performance or production readiness. The legacy checkout stayed read-only.

The five rounds below are actual drafting and correction activities. Domain
passes overlapped; their common review lenses are grouped here for clarity.
They are neither five clean-review certificates nor five executions of the
future product tests. Detailed phase records retain local corrections.

## Round 1 — Decompose responsibilities into real producers and consumers

Expanded phases 00–10 into stable task IDs and numbered steps. Each task names
proposed files, input/output contract, prerequisites, useful configuration/docs,
failure/recovery and an actual entrypoint with an independent expected outcome.
Kept the website separate from public exploration and private administration.
Separated foundational code from later integration so a complete graph engine
does not become a prerequisite for a relay fact or ordinary event read.

Concrete repairs: added the missing separate admin artifact, backend command/job
authority before UI controls, shared practical runner with named requiredness,
and source/producer coverage for assertion fields. Canonical full events and
scientific observations no longer share a vague generic storage description.
The coverage ledger maps old intent, selected changes and every data family to
the task that writes it and the task that makes it useful.

## Round 2 — Challenge every retained field and interpretation

Defined A01–A09 with binary identity, exact signed strings/tags/signature,
u64 protocol time, attributable observed time/clock, source-record identities,
nullable unknowns, origins, authoritative location and retention/export meaning.
Removed permanent passive delivery series, repeat counts and last-arrival fields
from the baseline. First observations and strictly earlier corrections are useful
evidence; imports cannot backdate local network observations.

Replaced the old 365-day raw-monitor expiry with permanent compact actual
measurements by default. An explicit lossy policy must identify missing detail.
Closed provenance-off loopholes in imported event→relay sidecars and intentional
per-event availability facts. Full signed originals remain unchanged even when
their tags mention relays. Chose `enabled-families`, `require-all`, `none` import
claim modes with unambiguous effects.

Reduced default large derived-history retention to current plus two complete
generations, while protecting real active references. A completed publication
retains compact lineage and public scientific signed originals; it does not pin
every full graph forever. Private DVM payloads stay operational by default.
Review caught and removed an accidental fifth SourceRecordId component: the
canonical identity remains dataset/shard/owner-epoch/sequence, one sequence per
fact and complete atomic-batch cuts.

## Round 3 — Walk crash windows and ambiguous external actions

Separated pre-dispatch intent from proof of actual probe execution. A crash may
leave an unsent intent; recovery records unknown start/completion and excludes it
from measured success/failure denominators. Late attributable terminal evidence
resolves the same identity. Queue delay cannot become a measurement.

Clarified acceptance before/after commit, lost replies, receipt lookup and policy
changes: cancellation cannot undo a committed body, and a response may resolve
previous durable acceptance without performing a fresh now-forbidden mutation.
Managed command/job/pins commit before acknowledgement; browser disconnect does
not own an already accepted job. Restrictive application is separate from saving
configuration. Restore uses isolated roots/destinations and positive old-owner
exclusion, not elapsed TTL alone.

Publication keeps exact signed bytes and independent destination obligations.
The optional invoice adapter persists one encrypted preimage/payment hash before
requesting the invoice, resolving an ambiguous response by that same identity.
No second random invoice, user-supplied payment claim or HTTP 200 substitutes for
node settlement. Source pins, outbox, financial ambiguity and display TTLs have
different lifetimes. The selected DVM wire/payment contract is explicit and
experimental, not mislabeled as a standardized generic database-query protocol.

## Round 4 — Test representation and challenge storage/resource assumptions

Ran the isolated local [archive-model probe](../../experiments/archive-model/README.md)
against real DuckDB 1.4.5 and Parquet, using two threads and a 256 MB engine limit.
The retained [result](../research/evidence/archive-model.json) includes fixture,
command, program/file hashes and six discriminating negative controls. It checks
exact unsigned times and nested tags after reopening exported files, cut-before-
minimum semantics, imported/local clock separation, namespaced document joins,
and empty-current-header behavior. Final experiment review caught an omitted
`pubkey` column: an independent required-field check failed the first artifact.
The corrected second run includes and compares all seven event fields, and the
retained result/hashes identify that run. This illustrates why a passing
self-roundtrip was insufficient. Its duplicate-admission portion is a Python
reference model, **not** evidence that a Rust/Rocks admission implementation ran.
Synthetic event fields test representation and are not valid signature fixtures.

Unexpected result: 1,000 observations plus three namespaced objects occupy
6,563 bytes across two tiny Parquet files; a wide repeated-body layout occupies
6,101 bytes. Dictionary compression and per-file overhead defeat the simplistic
claim that splitting repeated documents always saves physical space. Keep logical
document identity/reference semantics for evidence and reuse, but P02-T09 must
measure physical layouts, compression, indexes, export and larger skewed corpora.
This experiment establishes neither durability under power loss nor production
compression ratios.

Added aggregate native/decompression/output/spill accounting, protected control/
live shares, maintenance overlap and oversized-first-record scenarios. Refined
overlap planning into bounded exact work with sound pruning, plus separately
labeled approximate candidates. Read/search top-K must cover the admitted scope
or refuse, not rerank a convenient recent prefix. Browser pages and graphs cannot
silently download the entire corpus. R/D/U/P/M/C/T/V/E/Q costs remain explicit;
no language removes the bytes or linear work inherent in retained facts.

## Round 5 — Reconcile actual onboarding, interfaces and source formats

Found the bootstrap permission gap: seeds create candidates, so the full recipe
needs guarded public-candidate validation and automatic enrollment only after
real public read success. Private routes still need explicit enrollment. Added
the exact active probe profile and signer purpose. Matched CLI root/modules,
config schema, migrations, packaging tree and acceptance case ownership. Saved
private views and optional-index rebuild/retire now have backend routes/schema/
jobs, rather than appearing only as frontend controls.

Completed selected search normalization/grammar, source-to-wire assertion
bindings, and the optional DVM request/result/encryption/payment profile. Split
independent relay quality from whole-analytics qualification. Broke apparent
foundation/read/signing/management phase cycles using actual task prerequisites;
the first useful bundle is an integration consumer, not a foundation prerequisite.
A final reread also separated authority/RPC core steps from their later real-owner
acceptance so foundation and storage do not require each other to be complete.

Read the complete pinned Pensieve schema and Rust decoder/test file, and its
license, without running or copying upstream code. The
[source record](../research/evidence/pensieve-format-source.json) matches pinned
Git blobs. The [import contract](../specification/import-formats.md) now explains
proto3 default-value ambiguity, the smaller signed timestamp domain, truncated
prefix versus clean EOF, allocation bounds and noncommercial upstream reuse.
The earlier promise to preserve absent-versus-empty protobuf content was wrong
and is removed. Actual independent producer interoperability remains a product
acceptance obligation.

Reread resulting core specs and phase files, repaired cross-document defaults and
route ownership, and linked the task coverage. Release evidence binds the whole
bundle, image/platform/static assets and docs, not just one executable. Website
examples execute the commands in the built guide outside the checkout; browser
journeys use actual APIs. Required missing environments stay NOT_RUN. Mechanical
checks validate artifact references and task structure; they cannot certify
semantic correctness by themselves.

## What this pass establishes, and what remains empirical

The expanded handoff states the selected product behavior and concrete
implementation/verification work. Important uncertainty is assigned to a task:
P02-T09 physical layout/amplification; P03-T12 and P04-T12 real fleet behavior;
P05-T13 large analytical churn; P06-T12 concurrent reads/search; P07-T09 delivery
failure; P08-T12 full operational workload; P09-T11/P10-T10 actual user journeys.
No full Rust system, million/billion-row workload, 100k-relay WAN run, power-loss
test, browser release or real paid service was executed by this planning pass.

Selection is not immunity from future evidence. If real binding behavior,
measured resource cost or protocol interoperability contradicts the design,
correct the owning contract and dependent tasks before extending implementation.
There is no promise that planning can eliminate all discoveries during coding.
The deliverable is an explicit, reviewable foundation with honest empirical
obligations, rather than unsupported certainty.

## Recorded planning-artifact checks

The final breakdown has 114 tasks and 702 numbered steps across phases00–10:
5 planning tasks and 109 future product tasks. The structural inspection checked
unique task IDs, sequential step numbers, referenced task existence and 272 local
heading links. `python3 experiments/verify_artifacts.py` checked 64 Markdown files,
35 JSON files, four JSON examples, Python syntax, file links/fences and its
selected historical evidence hashes, with no reported errors. Separate checks
matched the corrected probe program/Parquet hashes and the unchanged external
Go plan identity. `git diff --check` reported no whitespace errors.

These counts and checks are mechanical evidence only. Critical step/phase cycles
were reviewed semantically and corrected in the owning plans; a reference checker
does not prove an executable DAG. Product acceptance remains unexecuted. The
private GitHub destination and absence of product Cargo scaffolding were checked
before preparing the planning commit. Git history and the owner-facing handoff
record publication of this dossier.
