# Current execution checkpoint

Observed 2026-10-18T17:30Z on Tux. Goal ACTIVE after the owner's explicit resume.
M1–M5 remain INCOMPLETE. Existing task files own the plan. Follow the
[resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint).

## Repository and active responsibility

Canonical `/home/tux/bigbrotr-rs`, main 2a548374d0e1027ff148961ffd09a7fda4b4d2b7,
remote exact2a54837 verified.
Root is the sole integrating owner; Tux alone handles development and tests.
No merge/rebase is pending. Commit2a54837 integrated the three corrected oracle files and their evidence.
Current root edits record cleanup recovery in this checkpoint, existing evidence
and inventory. No product source or qualified executable changed.

Current task: P02-T04 Steps 3/4/5, enabling P01-T09 Step 4/M1 after selected
P02-T01 Step 5 / P02-T02 Step 4 succession work. See the
[storage task](02-storage.md), [finite catch-up contract](../specification/archive-storage-contract.md#finite-retained-archive-catch-up-selected-under-implementation)
and [native profile](../reference/archive-handoff.md#finite-retained-lineage-catch-up-selected-under-implementation).
No task closes from component tests or AI review alone.

Durable details: [producer recovery report](evidence/p02-producer-recovery-native-20260920.json),
node `initial_successor_preparation_20261012.repeatable_succession_20261015`.
[Laboratory inventory](../research/laboratory-inventory.json), node
`implementation_archive_catch_up_20261018`, owns roots/resources. Historical
checkpoint a62f08a preserves earlier detailed chronology; it is not current status.

## Current result and correction

Latest engine root:
`/home/tux/bigbrotr-implementation-20260917/runs/ingest-succession-archive-catch-up-engine-a62f08a`.
Source a62f08a4fedd454ce9e593a57a6a45dd870e892d, tree
24c8eb0c14eaabf5f94e986305a3274790be72a2. Prepare74810 joined0;
engine94225 joined1, 16:56:14Z–16:58:51Z. All14 product processes exited0,
unforced/reaped; all13 continuations including catch-up, exact retry, ordinary
HTTP/history/export and restart completed. Both HTTP clients report PASS.
Final independent verification failed at catalog line85. Engine and aggregate
remain FAIL; their full science did not complete, raw success replay and negative
controls NOT_RUN. No original output was rewritten.

Read-only failed-run custody9843 joined0. `audit-root/audit-failure-v1.json`,
SHA c87e57ed63885fec2b9ef7f24dc32941e304e55544633259891d6926bdfbdb5d,
binds1974 source files,190 protected ELFs,14 process receipts and270 structural
raw files. PG/client are stopped/PID0/noOOM, exits0/143; cleanup/missing/unrelated
errors empty. Never replay that auditor after intentional borrowed-PG changes.

Root corrected verifier assumptions using pinned source and original raw evidence:

- DuckLake value_count means non-null count; require exact nonnegative integers,
  value_count+null_count=record_count and valid dimensions. All283 new statistics
  meet this;77 were falsely refused by null_count<=value_count.
- Pinned DuckLake BLOB global extrema use escaped-text order. Six metadata
  extrema differ from decoded-byte order. The full scientific C API reader stays
  independent. Inspected upstream optimizer/constant-pruning paths do not use
  those BLOB global extrema as byte bounds; no broad upstream query proof implied.
- PostgreSQL membership rows receive fresh unique OIDs. Historical rows and all
  role/member/grantor/privilege fields remain checked exactly.
- Commands/reads need not update last_seen. This sequential browser fixture's
  commands precede logout and expiry; export completion precedes logout.
- SHA256 bytes need explicit SQL bytea or JSON hex presentation; export jobs do
  not inherit acquisition_job_protocol from the old acquisition-row template.

Separate offline diagnostic43366 joined0 using the corrected oracle:
`.research/archive-catalog-counts-20261018/diagnosis-v8.json`, SHA
 4107b563f3e5d5e2b9d543f977d58b43245a1c984f7d81dc1871794bdf358735.
It checks42 observations,75 Control tables,22 facts,7 receipts, snapshot8,
frontier4→7, counts[13,2,2,2,3], exact export retry and91 negative controls.
This is changed-oracle diagnosis on preserved captures, not strict success replay
or a new engine PASS. The original status is changed only in separate memory;
original SHA11ed80db remains exact. Six earlier diagnostic failures and their
sources are retained. Existing evidence node `archive_journey_oracle_correction`
binds all sources, findings, scoped checks and limits.

## Qualified product and next action

Exact packaged build remains9a7fd81f3d1d351a206a54415f27cf8c7ca055c0,
tree124db7fd291accb66f30eb0ceb9fceaf9f750107, root
`/home/tux/bigbrotr-implementation-20260917/runs/ingest-succession-archive-installed-9a7fd81`.
Prepare11865/build61068/audit16469 joined0. Five checks PASS: compile, journal6,
Serve27, entrypoint8 and Clippy;41 actual tests. Worker build, build-info and
verify-worker PASS. Audit1164abade607ced8e3c93ed7fd48f13c73e8167c3a259e839376ba9728fb3494
binds1974 source files and190 preserved ELFs. Existing Copy-drop warning in a
library test is separate from passing Clippy. No rebuild for experiment-only fixes.

The renewal correction preserves the original due time and returns Ready inside
its100ms margin; retained Work keeps its grant and original Archive deadline.
It closes a source starvation counterexample compatible with the earlier live3
watchdog failure, without claiming its unique trigger or arbitrary scheduling progress.

Next bounded action: commit/push the recovered headroom record, then prepare one
fresh normal journey from clean main using unchanged qualified9a7fd81. Helpers:
`.research/archive-renewal-dispatch-20261018/prepare-journey.py` SHAe1d82ee6,
success auditor6c7efb8a. Bind build audit1164abade and190 ELFs; require
69,324,505,088B free. After enginePASS, run its actual strict raw reader, then
read-only custody. Never replay existing roots or old audits after PG reuse.

Cleanup recovery completed17:29:06Z under
`.research/tux-maintenance-archive-renewal-20261018`. Original76909joined143
remains interrupted/noresult: root incorrectly submitted execution after a failed
source hash precheck, then stopped its exact process.379 journaled removals plus
one observed missing file were reconciled as the first380 of1309 frozen Cargo
rlib/rmeta candidates. Separate recovery46004joined0 removed the remaining929;
full before/after cache hashes/stat,190 protectedELFs,10assets and1399 stable
container projections matched. Recovery source48323b79 received root and independent
source review; exact hash gating preceded execution. Total1309 reconciled absent,
2,153,873,408 allocated candidate bytes. Free70,666,690,560B at completion.
Original evidence is preserved; neither executor may be replayed. Detailed hashes
and limits live in existing report `archive_renewal_headroom`/inventory `renewal_headroom`.

## Pending independent work

Fault source is frozen in `.research/archive-catch-up-journey-20261018`, branch
`qualification/archive-catch-up-journey`, HEADc7b643d. Original29d43a0 remains
preserved; followups7c2628f/c7b643d correct counts/BLOB/membership using the shared
normal helper. Source-only33+24 controls PASS; full four-process fault journey,
scientific replay and89 whole-transcript negative controls NOT_RUN.
Root read all original files/hooks and both complete followup deltas/check scripts;
existing manifests bind source and artifacts. Integrate/run after normal success.

Fault design: real publication COMMIT delivery loss, fresh resolver reuses original
receipt, first frontier COMMIT delivery loss, fresh original-operation resolution,
then exact retry. Same physical roots; no synthetic positive receipts or SQL seals.
Local diagnostic custody and resolver credential sealing remain distinct.
Fault custody template under `.research/archive-deadline-build-20261018`,
SHAc169d78e; original manifest9b63a3f3 and provenance supplement05c1b5bb retained.
Source-only/NOT_RUN. Future profile/root `archive-catch-up-faults`.

Source-only refusal packet is frozen in separate worktree
`.research/archive-catch-up-refusals-20261018`, branch
`qualification/archive-catch-up-refusals`, basea62f08a plus original fault29d43a0
cherry-picked as31b250f and oracle2a54837 as e2806e7. New commit69a32b6 is the
sole refusal delta; independent source review is active. Six planned actual cases each require a fresh original
terminal3 root: missing current join, orphan history, changed family counts,
missing retained input, cancellation during observed SQL wait, exact Control
backend loss during wait. No engine/native/SQL/compiler delegated.
Genuine held-attempt and valid issued+planned cap remain separate NOT_RUN work.

## Resources, preservation and remaining gates

No root-owned runtime/compiler/native/SQL remains active. Only independent refusal source review is active. Unrelated services/work remain untouched.
Compiler cache stays root-exclusive under
`/home/tux/bigbrotr-implementation-20260917/runs/network-1154757/out/build/{target,cargo-home}`,
owner8b21a13162699a6038154874c2ac89da, exclusive `.compiler-cache.lock`.
Rust1.98.1 Linuxamd64 build image7e946a0c0aa4a5d357c3ae517fe9830f675cfc98fd26a9cba79b2bc957928b9d.
Native Archive assets remain under installed-publication-61271e2/out/archive-worker.
Borrowed PG7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec,
owner5e69b0f060b1607c25230d1dbd3e5608, retained relay-source-recheck-e06f580/pg.

All original failed builds/engines6474507/be57042/4aaf017/a62f08a, diagnostic
failures, native datasets, ignored `.research`, external logs and artifacts remain.
Existing reports retain exact statuses and separate custody audits. Never replay
one-shot preparation, old audits after PG reuse, or completed cleanup executors.
Prior cleanup30roots/57,469 intermediates,8roots/16,943 and13roots/9,229 is complete;
last receiptadaabd2e/4.48GB observed gain is under tux-maintenance-archive-read-20261018.
Earlier current-cache56rlib retirement is separate and complete.
Seven retired component refs remain in archived refs and bundle97b09ad6; worktrees
and ignored artifacts were preserved. Both current qualification branches are needed.

Broader M1–M5 journeys, ACK/GC, selected import/restore paths, capacity/aged-state/RSS,
five exports below256MiB and release acceptance remain incomplete. Existing task
contracts own these gates; the finite catch-up exclusion does not delete them.
GitHub billing/Q33, nativearm64, signing, registry and public promotion remain
separate gates. Source pushes protect committed work; external/ignored artifacts
remain on Tux. No public release is authorized.
