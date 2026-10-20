# Current execution checkpoint

Observed 2026-10-20T09:46Z on Tux. Goal **ACTIVE**, verified after the owner
explicitly resumed the same goal. The previous pause was completed and preserved
in Git/evidence; its stop instruction no longer prevents authorized development.
M1–M5 remain INCOMPLETE. Existing task files
own the plan; follow the [resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint).

## Repository and responsibility

Canonical `/home/tux/bigbrotr-rs`, branch `main`, HEAD
`a12d45e16eccdafb40802fa4f997f5692d21bac0`, already pushed and verified remotely.
Root is the sole integrating owner; Tux handles development, Git and tests.
No merge/rebase pending. Current documentation/evidence edits record the qualified
missing-input refusal below. Product source is unchanged. The
attempt-limit and export-memory worktrees are clean on their preserved commits;
their reviewed source is not integrated or runtime-qualified.

Q37 is recorded in the existing guide and decision register. Root session
`01a12f2c-849d-7d20-9523-dcce1021b0c6` has actual turn-context metadata at
2026-10-20T09:40:10.642Z reporting `gpt-6-astra` / `xhigh`; the available-model
catalog separately lists both allowed configurations. Global defaults are not
proof of this session's settings. The existing review was allowed to finish;
its effective model/effort is unverified and must be checked before reuse.

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

Bounded diagnosis7cef1f9c under journeyWT's
`.research/archive-current-join-initial-diagnosis-20261018/source-note.json`
compares exact initial owners/configuration/accepted policy with normal410.
Four positive outcome copies and a completed47ms relay are observed; precise
pending await/latency cause remains unknown. No deadline/profile was widened.

The one new unchanged trial **current-join6735064 PASS**:
prepare2145/engine57754/replay12410/custody85430 all joined0.
Engine19:23:17Z–19:24:59Z,11installed processes; actual refusal exit4 unforced/reaped.
Independent31snapshots/75tables/10negative controls; no new Archive attempt or
frontier from the refused command. Strict replay binds509files; custody binds
1981source files/190ELFs, SHA
`c3d396e801f2a65521deb5084daf49b4e2497d9efad6094038151b76dcfde6a9`.
PG/client stopped0/143, PID0/noOOM; cleanup/missing/unrelated arrays empty.
Do not replay custody after PG reuse. This success does not erase the a99 failure.

The fresh **orphan-history e919097 PASS** has preparation85111/engine9231/
replay60311/custody10910 all joined0. Engine19:46:47Z–19:48:30Z; eleven actual
processes and refusal exit4 unforced/reaped. Independent31states/75tables and
10negative controls confirm no new Archive attempt/frontier. Strict replay binds
511files; custody e4c3e195 binds1981source files/190ELFs. PG/client stopped0/143,
PID0/noOOM; cleanup/missing/unrelated arrays empty. Do not rerun custody after PG reuse.

The fresh **family-counts be181ce PASS** has preparation55062/engine45417/
replay27307/custody35826 all joined0. Engine19:50:55Z–19:52:39Z; eleven actual
processes and refusal exit4 unforced/reaped. Independent31states/75tables and
10negative controls confirm no new Archive attempt/frontier. Strict replay binds
509files; custody23ab5c37 binds1981source files/190ELFs. PG/client stopped0/143,
PID0/noOOM; cleanup/missing/unrelated arrays empty. Do not rerun custody after PG reuse.

The **missing-input2426646 run was interrupted for this pause**. Preparation38651
joined0; engine11859 joined with child exit-2 (SIGINT), wrapper tool254, at19:55:38Z.
The verified controller pidfd received SIGINT during the already-running negative
controls. All nine prefix steps and the refusal exit4 unforced/reaped had been
captured; the captured state oracle says PASS, but negative controls did not finish.
Original engine/fixture status remains **FAIL / KeyboardInterrupt**, never a
qualified PASS. Strict independent replay and full custody audit are **NOT_RUN**.
Existing cleanup completed with no errors/missing/unrelated changes. The limited
shutdown snapshot at19:56:14Z confirms exact owned PG/client stopped0/143, PID0,
noOOM, no matching host runtime process. No additional test campaign was started.
Artifacts remain at `runs/ingest-succession-archive-catch-up-refusal-missing-input-engine-2426646`,
including `root-owner-pause-interruption.json`, `root-owner-pause-resources.json`,
raw logs, original before/after SQL captures and quarantined input bytes.
The existing report/inventory records `pause_checkpoint_20261018_1956` and the
interrupted attempt. The earlier a99 initial Sync timeout remains unresolved;
later successful trials do not establish its cause or fix.

Resumption custody68738 joined0 at2026-10-20T09:26:01Z. Read-only report
`audit-root/audit-interrupted-v1.json` SHA757e9a0a binds2524retained files,
190ELFs and1406unchanged nonborrowed containers; original SQLbasis captures and
quarantined input bytes/inode are preserved. Exact PG/client remain stopped0/143,
PID0/noOOM. Root read the complete115-line helper and delta; independent review
2cd68b4e found no concrete blocker. This is custody observation only: original
FAIL/-2, interrupted negative controls and strict replay NOT_RUN remain unchanged.
Never rerun this custody check after borrowed-PG reuse or replay the old engine.

Fresh **missing-input fd009b6 PASS**, preparation23219/engine2957/replay17097/
custody8521 all joined0; engine09:35:55Z–09:37:34Z. Run
`runs/ingest-succession-archive-catch-up-refusal-missing-input-engine-fd009b6`
uses sourcefd009b6/tree2ae2cdc918067a8e7e267fbad0095768281c1bd4 and unchanged9a
product. Independent replay verifies11installed processes,31states/75tables and
10negative controls, refusal exit4 unforced/reaped, no new Archive attempt/frontier.
Strict replay7421e94a binds511files; custody3832f226 at09:44:24Z binds1981source
files/190ELFs and1407unchanged nonborrowed containers. Terminal PG/client
stopped0/143, PID0/noOOM; cleanup/missing/unrelated arrays empty.
The original interrupted2426646 result remains FAIL; this does not resolve the
earlier initial Sync timeout. Never replay custody after borrowed-PG reuse.

**Next action:** prepare a fresh frontier-cancel run from clean main using unchanged
qualified9a product, then run its engine, independent strict replay and custody.
Frontier-cancel and frontier-control-loss remain NOT_RUN.
The headroom prerequisite is now satisfied. Package retirement19616 joined0 at
19:45:04Z, status **RETIRED_VERIFIED**. Root/independent source reviews authenticated
helper f37f584b; preparation72510 and manifest reviews70453/41961 joined0.
Frozen manifest9a627127 under
`.research/tux-maintenance-cargo-package-headroom-20261018` binds37old source roots,
four retained complete counterparts and9378packages. Actual cleanup removed
529697files/80400directories,9009500160allocated bytes. Complete90236retained
entries,180input pins,190ELFs/10assets and1405stable container projections passed
post-verification. The full journal exactly matches every frozen package/ordinal.
No failure artifact or stderr; never replay the completed one-shot executor.
Free293816459264B exceeds the unchanged69324505088B floor. Shared-host free space
also changed independently before/during cleanup; the11161845760B observed delta
is not solely attributed to the selected9009500160B. All source/data/log evidence,
worktrees and original failures remain preserved. Detailed pins and limitations
are in `archive_package_cache_headroom` in the existing report/inventory.

After this checkpoint commit/push, use existing preparation5b61ccf3 under
`.research/archive-renewal-dispatch-20261018`, case frontier-cancel. Run the actual
engine, strict `installed_archive_catch_up_refusal_capture.py` replay and latest
custody templatec023b95d under journeyWT's
`.research/archive-catch-up-client-name-audit-20261018` before borrowed-PG reuse.
Each case needs its own genuine initial→terminal3 setup, unique clean revision/root
and coherent checkpoint; client names include source7. Preserve every failure.
A repeated initial Sync timeout requires targeted diagnostics before more trials.

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
cleanup_review completed the interrupted-custody source review. recovery_review
froze the qualification-only export setup at370cf792 in its dedicated worktree;
root remains sole runtime/compiler/SQL/Docker/cache owner. Before the pause it
performed only static reads for the large signed export setup and made zero file changes. It found two setup gaps: the relay encoder accepts frames
below65536B, while the frozen event is94577B; ordinary Serve can reserve a job
immediately after HTTP acceptance, while Initial cannot accept Export. Minimal
bounded relay framing and a genuine typed acceptance-only fixture consumer were
implemented only as a source candidate, not integrated or compiled. Manifest
75b8f6a7 under exportWT `.research/export-accept-setup-source-20261020` is
authenticated; independent source-only review67a5ecef is complete with no concrete
blocker, while root's affected-owner review remains incomplete. Compilation and
runtime are NOT_RUN. The reviewed
c68ac35 baseline and all original artifacts remain intact.
No session runtime is active. Both old nine-root
and new37-root cache retirements are complete; neither executor may be replayed.
The673/e919/be181/fd009b6 refusal custody checks are complete and must not be rerun after
borrowed-PG reuse. The interrupted2426646 limited custody observation is complete; its scientific
qualification and strict independent replay remain NOT_RUN.
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
