# Current execution checkpoint

Observed 2026-10-13T16:04Z on Tux. The owner explicitly requested PAUSE because
credits are exhausted. Safe stop is complete; only this checkpoint's commit/push
and the runtime goal transition to `paused` remain at authoring time. Do not resume
implementation or testing without the owner's explicit request. The selected
M1–M5 product remains INCOMPLETE. This checkpoint is navigation; follow the
[resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint).

## Authority and source

Q34 makes Tux the sole editing/Git/development/test host. Q35 appoints this session
sole integrating owner through the selected product, including private commits
and verified pushes. The latest explicit pause overrides the earlier resumption.
Preserve unpublished Cargo packages, read-only legacy repositories, unrelated
workloads and every ignored/external artifact.

Canonical `/home/tux/bigbrotr-rs`, branch `recovery/tux-20261012`, source HEAD before
this checkpoint `c12b22db16b6861a3d38521c2aae1ab75c070908`. Last verified private
source push is this same revision (wrapper88385 joined0, exact ls-remote match).
The three original qualification branches below were pushed and verified in that
same operation. Main remains `54ad09dc6659bfe67e3bf2871e43ce619c199a5c`.
The only root changes for this pause are this checkpoint, storage task, existing
recovery evidence and laboratory inventory. No merge/rebase or source edit pending.
The commit containing this record is the pause checkpoint; determine its actual
hash and remote state from Git on resume, rather than guessing a self-reference.
Root alone integrates, builds and executes; all three agents are stopped.

## Current task and next action

P02-T01 Step5 / P02-T02 Step4 enables P01-T09 Step4/M1. None is complete.
The [storage task](02-storage.md), [registration contract](../reference/ingest-registration.md),
[recovery evidence](evidence/p02-producer-recovery-native-20260920.json) and
[lab inventory](../research/laboratory-inventory.json) remain the durable authority.
Retirement evidence node:
`initial_successor_preparation_20261012.next_live_activation_20261013.activated_initial_retirement_20261013`;
inventory node `implementation_activated_initial_retirement_20261013`.
The adjacent `activated_acquisition_abandonment_20261013` evidence and matching
inventory node retain the completed bounded acquisition qualification.

Next bounded action, ONLY after explicit resume: reconcile Git, artifacts and
stopped resources; verify compiled inputs still match build388a01a, then execute
the already-reviewed installed final COMMIT7 loss fixture before forwarding and
after completion, each followed by fresh-process recovery. Use the existing
`experiments/acquisition-scope/bindings.py --ingest-activated-retirement-faults`
from a clean exact revision and a new inventoried lab. The388a01a artifacts and
root `.research/activated-retirement-faults-replay-20261013.py` are prepared;
actual fault execution is NOT_RUN. Expected evidence distinguishes absent versus
committed twelve-row retirement and exact retry preservation, with backend/native
custody, raw replay and source/original audit. Record actual outcomes in the same
evidence/inventory. Do not claim a passing pure fixture proves this boundary.

The separately committed raw SQL draft7730bfb is NOT integrated or fully reviewed.
Before its later integration/execution, evaluate the missing local parallelism
bound in new full72-table snapshots, update any affected SQL pins, finish oracle
review and capture-custody mocks. This draft does not block the separate reviewed
fault fixture. Other retirement gates remain in the owning contract/evidence:
zero/Event and workflow/import obligations, conflicting/missing copies and mixed
metadata, raw SQL immutability/earlier grants, blocked cancellation and actual
Control UTC expiry. Repeated lifetimes, logical import closure, ACK/GC, epoch3,
Archive restart and full M1–M5 remain separate and incomplete.

Retirement is an explicit existing-copy-only command under already CLOSING native
proof. Only producer2, nine domains, its owner and role transition together;
source2, historical recorder identities and all native/input bytes remain exact.
No implicit workflow repair, source close or native reclamation is qualified.

## Latest completed evidence and preserved drafts

Exact build388a01a at `/runs/ingest-activated-retirement-388a01a`:
wrapper35099 joined0;48 native tests,48 entrypoint/schema/migration tests and
Clippy PASS. Root audit47257 joined0:1834 exact Git/export/archive files,
76 retained ELFs,1287 prior projections unchanged; two build containers stopped0.
Native9931 integrated8fd74ec; PGf412 integrated001cae7; CLI/build388a01a.
Source reviews closed after correcting private Metadata access before compilation.

First installed retirement at `/runs/ingest-activated-retirement-engine-9bdc86a`:
source `9bdc86a2316e0aa0d1078ededb80a8d9c9502a2f`, same compiled388a01a inputs;
wrapper78186 joined0,15:53:31–15:55:15Z, PASS. Four actual CLI commands across
abandoned1d64 off/source5 and AlreadyDurable/source0 copies, all exit0. Each first
command changes exact12 target rows; fresh retries change0 and retain timestamp/
xmin. No native writes or source/job/session/gap/outcome rewrites. Schema37 applied
and reapplied with exact3 EXECUTE grants; whole72-table and native/input captures.
Root independent replay68851 joined0 covers2 seed+8 installed states/110 controls.
Author custody audit22397 joined0 (the audit already running at pause):1836 engine
files/1834 build files/76 ELFs,1288 unchanged nonborrowed container projections,
both originals intact (46 paths/625030 regular bytes/11 inputs/7 frozen artifacts).
Result SHA `48506a152b393854f2f50cbedfedb71b66e53ed9578753d7e6b4df3bd76c8e09`;
root replay SHA `a92255be2358b714930d3250bfff59a7501420fe60fe753acefda3ece6e9c140`;
author audit SHA `33356948159234c2ddaeb721b5e75efb63d0cd38b1580b8012e0f32077456507`.
Audit paths: `root-independent-replay.json`, `audit-cleanup-review/audit.json`.

All three worktrees below were freshly clean, have no active owned process and
are preserved under canonical `.research/`; original branches verified pushed:

- `activated-recovery-qualification-20261013`, branch
  `qualification/activated-retirement-fixtures`, original
  `1f382225538c4146c7683254bad68f340f78e565`; integrated through9bdc86a.
  Complete root fixture review, actual four commands and both audits above pass.
- `activated-recovery-native-20261013`, branch
  `qualification/activated-retirement-faults`, original
  `18e4203de7a036051d3b26bf2e52fd8cb59aab2c`; integratedf1ab517,
  dispatcher c12b22d, shared optional-action helper bae95c4. Full root review done.
  Four artificial contexts/eight states,106 perturbations and4 capture-custody
  mocks pass. Actual COMMIT7/recovery engine remains NOT_RUN.
- `activated-recovery-postgres-20261013`, branch
  `qualification/activated-retirement-guards`, WIP
  `7730bfb812a29167c1c4ea6aa861e009ecdb679e`; NOT integrated. Root read full
  driver and oracle1–150; remaining oracle and independent complete review pending.
  Already-running generator PID1478541 completed; waiter98973 joined0:
  artificial18 cases/36 snapshots/21 perturbations. Actual SQL and custody mocks
  NOT_RUN. New internal snapshot lacks `max_parallel_workers_per_gather=0`;
  evaluation/correction intentionally deferred at pause.

Each worktree retains its nested ignored retirement selfcheck directory with all
artificial states, source scripts, logs and initial failures. They are inventoried
in the existing retirement evidence; none is erased or recast as engine evidence.
No new implementation/test campaign was started to reach this pause.

## Earlier evidence and unresolved observations

Actual UTC expiry: `/runs/ingest-activated-acquisition-expiry-engine-aad54e8`,
sourceaad54e8, wrapper4291 joined0, PASS. One finite probe, zero installed CLI;
16 full72-table snapshots, native70/sequence85/six inputs, three external states.
Actual final CLAIMED_JOBS wait observed1,897,335us before expiry and182,439us after;
rollback clock267,814us after expiry. Dispatch-to-release-observation envelope
2,352,103us; these samples do not measure exact lock duration or Rust timer internals.
Exact Control(Unavailable), then actual refresh Unavailable; clean Control/observer
joins and exact backend absence. No new gap or terminal job writes, lease unchanged
through the wait, originals intact. Result SHA
`ef7e8129ad97af94798b5ebc2cef1607075b616326e338dc7b5ecf078ea4d7f2`.
Root replay97263 joined0 checks raw bindings and23 controls. Its first script/log
failed only because template.toml accompanied the database config; both retained,
then exact config binding corrected offline. Author audit48844 joined0 validates
1830 engine files/1828 build-export-archive files/70 ELFs and1285 unchanged
nonborrowed Docker projections, frozen15:15:37Z. PG0/client143/PID0/noOOM.

Previous expiry53a126b remains FAIL,69833 joined1. Database clone created, but zero
probe/CLI/upgrade/native-clone execution. Full-table observation failed allocating
32MiB DSM inside64MiB /dev/shm; host disk had over54GB free. During subsequent fast
shutdown, checkpoint completed then checkpointer SIGSEGV occurred; PG exit1.
These are distinct observations, with no established causal explanation. Audit60081
joined0 preserves original939, all70 ELFs and1284 nonborrowed projections. Root
changed only the capture connection's max_parallel_workers_per_gather to0.
Follow-up startup reports the database was shut down and ends cleanly; it does not
qualify WAL/crash recovery or explain/remediate the prior SIGSEGV. Preserve both
run logs and audits; retain this finding in future host qualification.

Older build `/runs/ingest-activated-refusals-bd4da0d` and its70 protected ELFs
remain historical acquisition evidence. Retirement uses the new exact388a01a build
and affected tests above; do not substitute the older executable.

Earlier completed acquisition gates remain in the same evidence node: first/retry
on off/source5 and AlreadyDurable/source0; both final C10 COMMIT reply-loss schedules
with fresh-process recovery; genuine zero-request claim and accepted-unclaimed
preservation;14 raw SQL guard cases; actual HTTP with recovery-only grants revoked;
64 success/retry,65 and retained1+selected64 refusal, unsupported-last batch refusal;
installed final-job SIGTERM with backend exclusion separately observed after unlock.
HTTP retains its missing first schema32 apply-response limitation; no reconstructed
receipt. Failed9e74,53a and all earlier failures remain FAIL, with NOT_RUN cases
preserved. Completion of this bounded operation does not complete M1.

## Processes, resources and maintenance

Safe-stop read-only observation at16:04:08Z:
`.research/pause-retirement-observation-20261013.json`. No owned build, engine,
audit or pure-check work remains active; all three agents stopped. The process
sample briefly saw only this pause's GitHub verification shell, subsequently
joined0. Build35099/audit47257, engine78186/replay68851/audit22397 and agent waiter98973
have all joined0. PG container717755 stopped0, clientfe56e3 stopped143, both build
containers stopped0: all PID0/noOOM. The143 is the retained client container stop;
all four installed product CLI invocations themselves exited0. Cache lock released.

External base `/home/tux/bigbrotr-implementation-20260917/runs`; `/runs/` above is
relative to it. Borrowed PG root `relay-source-recheck-e06f580/pg`, owner
5e69b0f060b1607c25230d1dbd3e5608, container
7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec, PG16.15,
1.5GiB memory/2CPU/64MiB shared memory. Retirement engine owner
1979cce97962b0239e79e3fa47b2e5ae, retained client
fe56e36ac2dd47fa41842cef7d6bbc8564e2032352c5097f68448047bb2c89c0.
Build owner e506f075f2dd2b847bc4a4a2191236fb. Detailed terminal identities/states
are in inventory and the safe-stop observation.

Frozen originals include retiredf12, preparedac644, activated-zero2c142, installed
live61aoff/on, closed939off/0e4917durable and acquisition-abandoned1d64 copies.
Never open originals as writers. Verify exact captures before cloning.
All76 protected ELFs, worktrees, stopped containers, volumes, datasets, source
exports and every ignored/external artifact are retained. No cleanup at pause.
Six unrelated services remain running and untouched: inspirehep-reindex db/mq/
cache/es, ollama, tgdataset-mongo. Private Git does not back up ignored labs;
those remain on Tux. Unavailable Mac captures remain unavailable.
Recovery details: [Tux audit](../research/tux-resumption-20261012.md).

Free bytes at safe stop:49,925,443,584 (after first engine:50,595,037,184).
Remeasure before allocations. Small engines require2GiB, native builds12GiB;
larger exports64GiB+577MiB remain unavailable. Never lower the reserve. Five exports
under256MiB and other M1 release gates remain NOT_RUN.

Cleanup executor is RETIRED_VERIFIED:48,725 regenerable intermediates removed from
18 historical targets,472,825 retained files preserved; never replay it or reuse
retired caches. Current cache `network-1154757/out/build/{target,cargo-home}` needs
exclusive `.compiler-cache.lock`. Keep final and failed binaries outside it.
Historical negative variants and dirty network-direct-no-poll1154757 stay unmerged.
GitHub run37017677570 was billing-blocked; Q33 authorizes isolated trusted Tux CI.
Native arm64, signing/registry and public promotion remain separate gates.
