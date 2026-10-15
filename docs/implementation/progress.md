# Current execution checkpoint

Observed 2026-10-15T07:46:17Z on Tux. Execution is STOPPED at the owner's explicit
credit-exhaustion pause request. Only safe-stop documentation, commit/private push
and the final runtime Goal pause remain in this turn; no new implementation or
test campaign is authorized while paused. Resume only on explicit owner request.
The selected M1–M5 product remains INCOMPLETE. Follow the
[resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint).

## Authority and source

Q34 makes Tux the sole editing/Git/development/test host; Q35 appoints this session
sole integrating owner. Preserve legacy repositories, unrelated workloads and all
ignored/external artifacts. Public publication/deployment, repository rename and
unpublished Cargo promotion remain separately gated.

Canonical `/home/tux/bigbrotr-rs`, branch `recovery/tux-20261012`; observed HEAD and
verified private remote before this checkpoint:
`46bfcb72dc5e98ded3f39a680679c5daf06a3dae`. Main remains
`54ad09dc6659bfe67e3bf2871e43ce619c199a5c`. This checkpoint's containing commit saves
only four existing documents: progress, storage task, recovery evidence and lab
inventory. No product source change or merge/rebase pending. The final checkpoint
commit/push happens after authoring this record; obtain its exact hash from Git
and reconcile the remote at resume rather than guessing a future hash here.

Root integrated SQL fixtures at2b71490/27384e6 and dispatcher46bfcb7. Original
qualification branch `qualification/activated-retirement-guards` is clean and
verified pushed at4858c807c4fb9f502101f43f3a076501d880104b. Its actual engine,
independent raw replay and custody audit are now complete PASS.

Zero/rejected fixture is separately committed and verified pushed at
`c794d63e02633ee6d60b9ee1f16b97d19a692b5a`, clean branch
`qualification/activated-retirement-zero`, worktree
`.research/activated-recovery-native-20261013`. It is NOT integrated into root.
Only two new Python files; no Rust or shared-helper change. Original fault branch
18e4203 and first-wave fixture branch1f38222 remain preserved/pushed.

## Current task and precise next action

P02-T01 Step5 / P02-T02 Step4 enables P01-T09 Step4/M1. None is complete.
The [storage task](02-storage.md), [registration contract](../reference/ingest-registration.md),
[recovery evidence](evidence/p02-producer-recovery-native-20260920.json) and
[lab inventory](../research/laboratory-inventory.json) remain the durable authority.
Retirement evidence node:
`initial_successor_preparation_20261012.next_live_activation_20261013.activated_initial_retirement_20261013`;
inventory node `implementation_activated_initial_retirement_20261013`.
The current `pause` record supersedes dated `pause_20261013` history.

Only after explicit resume and reconciliation: review the final c794d63 delta and
its retained synthetic/capture evidence, then integrate its two files and add the
missing root dispatcher plus independent raw replay. Root previously read the
complete178-line driver/267-line oracle and shared36/37 migration/catalog/Control
helpers. The final one-line capture-failure preservation correction and22 added
oracle lines remain for root review. Dispatcher and independent actual replay
are NOT implemented; no zero/rejected engine is prepared or started.

The later bounded qualification should run two genuine c7b2b3c source-closed
clones/three installed CLI: zero first/fresh retry and unbound Rejected/Event
refusal, using exact388a01a binaries after unchanged compiled-input verification.
Expect12 then0 target writes for zero and exact Outcome(Conflict) for Rejected;
the latter exercises the existing-copy Event check, not final SQL Event-mask
dispatch. Record actual full-state/native/input/process evidence, independent
replay and custody audit in the existing nodes. Synthetic checks alone do not
qualify actual SQL/native/CLI behavior.

Zero author reports PASS:3 artificial contexts/6 states/74 perturbations and5
capture-custody mocks, AST/diff check. Manifest:
`.research/activated-recovery-native-20261013/.research/activated-retirement-zero-selfcheck-20261015/source-checks.json`,
SHA `bb7d176abd4ee097c5cb8f0935f05910a205ad905027aba8b36adc062f6b4885`.
Actual SQL/native/CLI/build execution is NOT_RUN.

Read-only workflow design is complete but not independently qualified or
implemented. The evidence records proposed genuine da839b7 zero-request
acquisition retirement, checkpoint/binding vetoes, missing/conflicting copy or
input and mixed terminal metadata. Authentic import proposal: real live Control8
callback creates/seals/queues import, stop/release8, explicit source close9,
acquisition abandonment10, retirement11 reaches the named import-claim veto.
Do not fabricate historical authority. Review the actual classifier/callers before
implementing; no follow-on work was started during pause.

Remaining gates include those zero/workflow/copy cases, blocked cancellation,
actual Control UTC expiry and session custody/poisoning. Ordinary OPEN source
compatibility, repeated lifetimes, import logical closure, ACK/GC, epoch3,
Archive restart and full M1–M5 remain separate. Retirement is existing-copy-only
under already CLOSING native proof:12 target rows, no source rewrite/native reclaim.

## Completed retirement evidence

All `/runs/` paths below are relative to
`/home/tux/bigbrotr-implementation-20260917/runs`.

Build `/runs/ingest-activated-retirement-388a01a`, source
`388a01abd6dadf2a894a88c16e585ace6b382ffa`:48 native and48 entrypoint/schema/migration
tests plus Clippy PASS; wrapper35099/audit47257 joined0.1834 source/export/archive
files and76 protected ELFs. Compiled inputs remain unchanged for the recorded
engines. No build or test was rerun to pause.

First/retry `/runs/ingest-activated-retirement-engine-9bdc86a`:
wrapper78186/replay68851/audit22397 joined0. Four CLI on two acquisition-abandoned
copies; first12/retry0 rows, exact terminal T/xmin, no native/input/source/workflow
rewrite. Ten full states/110 controls. Result48506a15/replaya92255be/audit33356948;
full hashes and exclusions in evidence.

Final COMMIT loss `/runs/ingest-activated-retirement-faults-engine-724b2ff`:
engine85398/replay12446/audit18438 joined0. Two actual final-C7 losses and two
fresh recoveries; failed CLI exit4 CommitUnknown, recoveries exit0. Before-forward
0 then12 target writes; after-complete12 then0 preserving committed T/xmin.
Controls14 positively fence failed13; native70/sequence85/six inputs unchanged.
Ten full states/106 controls;1838 engine files/1834 build files/76 ELFs preserved.
No same-session poison or internal driver telemetry claim.
Result `37d187cb29659a795ca70035a778c8120ac87175174664e0ee4f6905f733899b`;
replay `db939bef8fe6efcce8a97d8bbe75c886629c4ffda0e7c7d0be7b8483f8f6d4dd`;
audit `7c7738a6c98c48f398a142a4317dd93c584d554a2a24ee98f3b222630e97580c`.

Raw SQL `/runs/ingest-activated-retirement-guards-engine-46bfcb7`:
engine1545/replay38330/audit41257 joined0.18 actual attempts on three clones:
three rollback positives,14 named23514 guard refusals, one PostgreSQL FK0A000
TRUNCATE refusal.36 internal full72-table snapshots plus36 before/after states,
23 perturbations, six native inspections, two isolated operator deletions with
exact preimages; all temporary grants restored. Native70/sequence85/six inputs,
original1d64off and catalog preserved. No installed retirement CLI in this wave;
raw SQL proves no native authority. Root replay independently checks raw captures.
Result `a287916fa167cf71d7893f47e6ec3ba3350280386a7b26c63265582f2d46535c`;
replay `2088b04c36b6d5d79ffd48bd09189e400328d8db70594d4de1e80ce419b0f29e`;
audit `020fd1e345627a7924441da225c0e4414badbc13b4bea40708d934f33ab6c290`.
Audit verifies1840 engine files/1834 build files/76 ELFs/239 raw bindings and1290
unchanged nonborrowed container projections. Original24 paths/314449 bytes/six
inputs/four frozen artifacts intact. Auditor authored the reused first-wave
reducer, not the SQL fixture; root semantic replay remains separately recorded.

Under each run, retain `bindings/result.json`, `root-independent-replay.json` and
`audit-cleanup-review/audit.json`. Retain failed drafts too: SQL replay's initial
syntax failure and custody `audit-v1.py/log` (wrong ledger field name, corrected
reader only). Neither failure was reclassified as a product PASS.

## Processes, resources and preservation

Fresh pause observation:
`.research/activated-retirement-pause-20261015.json`,07:46:17Z. No matching
non-shell BigBrotr/build process and no running BigBrotr container observed.
All three child agents completed: cleanup_review, recovery_review and
successor_restart_design. No engine/build/replay/audit/pure process remains active.
Do not treat historical handles as live ownership proof on resume.

Borrowed PG container
7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec,
root `/runs/relay-source-recheck-e06f580/pg`, owner
5e69b0f060b1607c25230d1dbd3e5608: exited0/PID0/noOOM.
Latest SQL client724ea8491f159601e840395f6da9a4b2dc892fb14d9d7e80e653cb62148404f8
and fault client248a2f7719c14ae96617f5c16fe400c728c283071328b14473a8dc30159c20f7:
exited143/PID0/noOOM. PG16.15 limits remain1.5GiB/2CPU/64MiB shared memory.
Six unrelated services remain running and untouched: inspirehep-reindex
db/mq/cache/es, ollama, tgdataset-mongo.

Preserve every ignored `.research` artifact, external lab, source export, dataset,
worktree, stopped container, protected executable and original/failure fixture.
No cleanup performed during this pause. Never open frozen originals as writers.
Private Git backs up tracked source only; ignored labs remain on Tux, and
unavailable Mac captures remain unavailable.

Free bytes at pause:44,450,078,720; remeasure before allocating. Small engines
require2GiB, builds12GiB;64GiB+577MiB exports unavailable. Never lower reserves.
Cleanup executor RETIRED_VERIFIED must never be replayed or retired caches reused.
Current cache `network-1154757/out/build/{target,cargo-home}` requires exclusive
`.compiler-cache.lock`; preserve final/failed executables outside mutable cache.

Prior acquisition expiry53a126b remains FAIL:32MiB observer DSM allocation failed
inside64MiB shared memory; subsequent checkpointer SIGSEGV after checkpoint is
separately unexplained. Successful aad54 expiry does not prove crash recovery or
remediate SIGSEGV. Shared capture bounds parallelism locally. Prior HTTP schema32
apply-response remains missing. Earlier failures and dirty network-direct-no-poll
1154757 remain preserved. Five exports under256MiB and other M1 gates remain
NOT_RUN. GitHub billing-blocked run37017677570/Q33 trusted Tux CI, native arm64,
signing/registry and public promotion remain separate gates.
