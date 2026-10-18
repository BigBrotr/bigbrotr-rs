# Current execution checkpoint

Observed 2026-10-18T19:22Z on Tux. Goal **ACTIVE**, verified with the goal control
following the owner's explicit resume. M1–M5 remain INCOMPLETE. Existing task files
own the plan; follow the [resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint).

## Repository and responsibility

Canonical `/home/tux/bigbrotr-rs`, branch `main`, HEAD
`a99c041769a791d5c923be2b3f555881674a7553`, already pushed and verified remotely.
Root is the sole integrating owner; Tux handles development, Git and tests.
No merge/rebase pending. Root's current documentation edits preserve the failed
initial prefix of the current-join refusal run. Product source is unchanged.

Active task: P02-T04 Steps 3/4/5, enabling P01-T09 Step 4/M1 through
[finite retained Archive catch-up](../specification/archive-storage-contract.md#finite-retained-archive-catch-up-selected-under-implementation)
and its [native profile](../reference/archive-handoff.md#finite-retained-lineage-catch-up-selected-under-implementation).
No task checkbox closes from the bounded journeys below.

Detailed source, failures, checks and artifacts remain in the
[producer recovery report](evidence/p02-producer-recovery-native-20260920.json),
node `initial_successor_preparation_20261012.repeatable_succession_20261015`.
The [laboratory inventory](../research/laboratory-inventory.json), node
`implementation_archive_catch_up_20261018`, owns resources. Older checkpoint
revisions and original failed artifacts are preserved; their process statuses
are historical observations, not current authority.

## Qualified baseline

Installed build: source `9a7fd81f3d1d351a206a54415f27cf8c7ca055c0`,
tree `124db7fd291accb66f30eb0ceb9fceaf9f750107`, run
`/home/tux/bigbrotr-implementation-20260917/runs/ingest-succession-archive-installed-9a7fd81`.
Prepare11865/build61068/audit16469 joined0. Compilation, journal6/Serve27/entrypoint8
(41 tests), Clippy, worker build/build-info/verify-worker PASS. Build audit
`1164abade607ced8e3c93ed7fd48f13c73e8167c3a259e839376ba9728fb3494`
binds1974 source files and190 protected ELFs. Reuse this build for unchanged product
sources; the new attempt probe requires a later separate build.

Normal installed journey: `runs/ingest-succession-archive-catch-up-engine-410d804`,
source `410d8044754acd9857278f658c4972a05ef12aec`,
tree `d720a06a8cf4c644f1f3a16ea342bbaee7d48744`.
Prepare77846/engine51349/replay89789/audit54075 joined0. Fourteen actual processes,
three terminal lifetimes, signed later source, catch-up/exact retry and real
HTTP/history/export/restart PASS. Independent42states/75Control tables/22facts/
7receipts, family counts[13,2,2,2,3], snapshot8/frontier4→7 and91negative controls.
Strict raw replay binds612files. Custody audit SHA
`7814290c380f19dedb0d9aa4d0fa63ae07c2186eb6717d7b9348efdaf1979181`.

Publication and first-frontier COMMIT-reply loss: run
`runs/ingest-succession-archive-catch-up-faults-engine-f45b1b0`, source
`f45b1b035fb2577e874e2655afd1e4f00b996cbb`, tree
`944c4c4bea0c38514a5add02a495a1837c43d60b`.
Prepare8912/engine74147/replay15730/audit16792 joined0; engine18:31:04Z–18:33:45Z.
Fourteen actual processes were reaped unforced; both loss processes exited4 as
expected, resolution/exact retry exited0. Both proxies observed server COMMIT and
withheld the client reply, then joined0. Full36states/75tables/22facts/7receipts,
snapshot8/frontier4→7 and91negative controls PASS. Strict replay binds593files.
Custody SHA `e40efc94d5485b784379cd37ae922b7bbd5cd24c6cc01df769ff96f1a3a65ef0`.
Borrowed PG/client stopped0/143, PID0/noOOM; cleanup/missing/unrelated errors empty.
**Never replay a completed custody audit after intentional borrowed-PG reuse.**

Original a62/d910/7d failures remain FAIL/UNKNOWN at their recorded boundaries.
Separate corrected-oracle diagnoses are not successful original runs. The report
retains proxy-limit and name/Id corrections, original exits, all diagnostic
failures and custody evidence. No failed artifact was overwritten or promoted.

## Completed maintenance and immediate next action

Root cache retirement46202 **joined0 at19:18:24Z**, actual receipt RETIRED_VERIFIED.
Folder `.research/tux-maintenance-cargo-source-headroom-20261018`;
helper04e5ed91/frozen manifest15f265d0. Actual prepare84124/manifest review14945
joined0. Removed133209files+20547directories from nine proven duplicate old
Cargo source caches,2235510784allocated bytes. Complete retained counterpart
`private-panel-referrer-cc4aa3a` and22984retained entries,54input pins,
190ELFs/10assets and1403stable container projections verified unchanged.
Currentnetwork115, worktrees, datasets, logs and native roots were not selected.
Observed free space70860189696B, gain1460027392B on the shared host; no cause is
assigned to the difference from candidate bytes. Reserve69324505088B remains.
Receipt and complete153756-entry journal are bound in existing evidence/inventory.
This one-shot executor is complete; never replay it. No runtime remains active.

The first current-join attempt at a99c041 failed before the selected refusal.
Prepare56594 joined0; engine46140 joined1 at19:19:47Z. Initial CLI exited4,
unforced/reaped in1.900s: `initial Sync deadline; preserve original outcome`.
No Archive attempt/frontier was issued; full75-table diagnostics and raw artifacts
remain at `runs/ingest-succession-archive-catch-up-refusal-current-join-engine-a99c041`.
Limited read-only custody22494 joined0, SHA13cc824a, binds2084files/190ELFs,
1402unchanged nonborrowed containers and original SQLbasis. PG/client stopped0/143.
The exact timed-out Sync sub-await is unknown; no false refusal PASS.

Root and bounded source review found no demonstrated blocker to one unchanged
fresh trial; exact sub-await remains unknown. Checkpoint/push this failure, then
prepare **one fresh unchanged-product/current-join attempt** from
clean main with `.research/archive-renewal-dispatch-20261018/prepare-refusal-journey.py`
(SHA5b61ccf3), build9a/audit1164. Run actual engine, strict
`installed_archive_catch_up_refusal_capture.py` replay and latest custody template
SHA `c023b95dc0a72a98d1df17476bb3c46f40d3ed01571257b2e1285cc984375921`
before borrowed-PG reuse. Template lives under journeyWT's
`.research/archive-catch-up-client-name-audit-20261018`.

The six cases are current-join, orphan-history, family-counts, missing-input,
frontier-cancel and frontier-control-loss. All six actual engines/replays are
NOT_RUN at the selected refusal boundary; the a99 initial failure is separate.
Artificial/model/custody-helper checks do not qualify them. Each case
needs its own genuine initial→terminal3 setup, unique clean source revision/root
and coherent evidence checkpoint. Do not prepare multiple cases at one HEAD:
the client name includes source7. Preserve each original failure.

## Reviewed work not yet integrated or executed

Attempt-limit packet: worktree `.research/archive-catch-up-refusals-20261018`,
branch `qualification/archive-catch-up-attempt-limits`, based on d910233;
commits bfe1b14c0b04f4194e09c1fafc6447d9cdd514e6 and
acc9bea67d5380f35c2fbf8d69ceed4539925133. Seven exact source blobs and manifest
07fb1351 authenticated; full owning source and independent review closed.
It holds a real LocalAttempt through installed Occupied refusal, then genuinely
issues122additional attempts and verifies126issued+3planned refuses before127.
Only its new fixture profile gets10800s client lifetime; product limits unchanged.
Pure/static checks PASS; compilation and actual engine/full replay NOT_RUN.
Integrate only after the unchanged9a refusal sequence. Preserve original branches,
worktree and ignored evidence until reconciliation.

Future attempt engine custody template517c6b06/manifest76abaab4 is fully reviewed.
Future build helpers are under journeyWT's
`.research/archive-attempt-build-preparation-20261018`: preparation bbc8cd3a,
build auditor **v2 660da49b**, engine preparation b47fa121.
Original manifest a6b624b4; v2 manifest57c84692; independent reviewa9e0bf96 under
`.research/archive-attempt-build-review-20261018`. Root read all134+629+91lines
and the exact v2 delta. V2 includes interrupted component result.tmp in recursive
pins; original helper and checker-only failure remain. Explicit future clean
revision/tree and sevenacc9bea hashes are mandatory; unchanged41tests,
190prior→196ELFs and64GiB+577MiB engine reserve. Every future runtime step NOT_RUN.

Export memory fixture: recovery_review owns the new isolated worktree
`.research/export-memory-probe-20261018`, branch `qualification/export-memory-probe`,
base d716b999, frozen source a0f604a5b951dc95668bc130de4d8a789e16cfa0.
Source manifest153bb3d3 authenticated; all7files fully reviewed. Narrow correction
c68ac35c3ec87e764fbef85e7a2eddaaadec5b6c/manifest e5b485a2 closes both custody
findings: Held retains WrittenExport and checked/original destination through
actual Control driver join. Root review8c67dee4 and independent review46a46025
close the exact delta; original packet/review retained. Compilation/engine NOT_RUN,
not integrated/qualified.
Scope: accepted exact-ID job through actual Control/CatalogCapture/pin/start/query/
write/completion/driver joins; unchanged9a worker and existing signed94577-byte
sample under the full1MiB row envelope. Caller markers/whole-call process and
cgroup peaks must not invent worker-internal phases. Actual installed Control
reservation includes512MiB plus transport; distinguish host baseline, per-export
cap, ledger admission and measured memory. No smaller product profile selected.

The redundant `qualification/archive-catch-up-journey` branch was retired at
19:00:32Z after all7 original commits matched integrated main patch IDs. Its
worktree remains detached at c7b643d, with all227 ignored files byte-identical.
Original commits are preserved in the verified incremental bundle86a024ab under
`.research/archive-catch-up-ref-retirement-20261018`; prerequisite b3735e9 is on
main. Refusal, attempt-limit and export-memory branches still carry pending work.
No remote branch or external lab was changed.

## Resource custody and remaining scope

All fault/refusal-prefix/replay/audit/maintenance processes above have joined.
Only successor_restart_design is doing bounded read-only source diagnosis;
other agents are idle. No runtime currently active. A repeated initial Sync
timeout requires targeted diagnosis, not indefinite retry or relaxed deadlines.
Compiler cache: `runs/network-1154757/out/build/{target,cargo-home}`, owner
8b21a13162699a6038154874c2ac89da, exclusive`.compiler-cache.lock`.
Rust1.98.1 Linuxamd64 image7e946a0c0aa4a5d357c3ae517fe9830f675cfc98fd26a9cba79b2bc957928b9d.
Native assets remain in installed-publication-61271e2/out/archive-worker and
qualified9a/out/artifacts/archive-worker. Borrowed PG
7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec,
owner5e69b0f060b1607c25230d1dbd3e5608, retained relay-source-recheck-e06f580/pg.
Unrelated workloads remain untouched.

Historical cache retirements, including13081Cargo archives and interrupted76909
plus reconciled recovery46004, are recorded in existing evidence/inventory.
Never replay their one-shot executors. Preserve all datasets, logs, original
failures, .research/worktrees and external labs; seven retired refs have the
existing97b09ad6archive/bundle. Push protects committed source, not raw Tux data.

M1–M5, ACK/GC, import/restore, capacity/aged-state/RSS, five completed signed exports
under the256MiB export gate and broader release acceptance remain incomplete.
Q33/GitHub billing, nativearm64, signing/registry and public promotion are separate
gates. No public release is authorized. Continue the existing dependency-ready
plan; do not convert a scoped success into overall completion.
