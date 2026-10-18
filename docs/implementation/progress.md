# Current execution checkpoint

Observed 2026-10-18T16:22:14.789328+00:00 on Tux. Goal ACTIVE after the owner's explicit resume.
M1–M5 remain INCOMPLETE. Existing task files own the plan; this checkpoint locates
current work and evidence. Follow the [resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint).
Historical detail remains in the evidence/laboratory records below and Git history;
the longer checkpoint at `97b8dc9` is preserved, not a current process inventory.

## Repository and active responsibility

Q34/Q35: Tux alone, root sole integrating owner. Q36: `main` is integrated state.
Canonical `/home/tux/bigbrotr-rs`; main HEAD 3d0f09d, last verified remote be57042. Qualified
product 40bc295 is pushed. No merge/rebase is pending. Engine 8902 joined 1;
all owned runtime resources stopped. Current documentation records its real
partial outcome; the client-fixture correction 08d282c is integrated as 3d0f09d.

Current task: P02-T04 Steps 3/4/5, enabling P01-T09 Step 4/M1 after the selected
P02-T01 Step 5 / P02-T02 Step 4 succession work. See the
[storage task](02-storage.md),
[finite catch-up contract](../specification/archive-storage-contract.md#finite-retained-archive-catch-up-selected-under-implementation)
and [native handoff/profile](../reference/archive-handoff.md#finite-retained-lineage-catch-up-selected-under-implementation).
The installed `archive catch-up --file ROOT` source is wired and its components
pass. First qualification failed during initial publication; the second reached catch-up
and retry, then failed in the HTTP test client. Full connected qualification is open.
No task checkbox closes from component tests or AI review.

Durable evidence: [producer recovery report](evidence/p02-producer-recovery-native-20260920.json),
node `initial_successor_preparation_20261012.repeatable_succession_20261015`.
Current children: `archive_pending_bridge_source`, `archive_component_sources`,
`archive_component_build`, `archive_finite_runtime_sources` and
`archive_installed_build_attempts`.
[Laboratory inventory](../research/laboratory-inventory.json) owns resource records:
`implementation_archive_catch_up_20261018`, `tux_intermediate_retirement_20261018`
and `tux_branch_retirement_20261015`. Earlier qualification remains under the same
report's source/acquisition/retirement/installed-family/cycle/prepared children.

## Implemented boundary and review

The runtime owner is `apps/bigbrotr/src/interfaces/serve/initial_ingest/archive_catch_up.rs`
with authority, inventory, plan, capture and publication children. It opens the
existing native/coordinator custody before credentials, verifies the complete
terminal native/SQL lineage and all retained inputs/attempts, resolves original
frontier operation IDs, admits the entire missing-batch plan, publishes complete
original batches, then bridges each immediate topology with explicit zero bounds
before the final complete cut. Exact retry preserves immutable attribution and
scientific state. Actual native/worker/driver joins and Control teardown precede
success delivery. It creates no Ingest writer, adoption, ACK/GC or listener.

Limits remain explicit: 128 issued-plus-planned Archive attempts; 256 immutable
frontier intents of at most 64 KiB; a complete 1024-entry state-directory census;
64 KiB scan and 1 MiB decoded-intent grants; existing full-cut/wire/pending bounds.
The pending decoder's real allocation exceeds its encoded-byte allowance.
Filesystem/export admission retains the separate 64 GiB plus 577 MiB requirement.
No copied coordinator, boot rebinding, journal repair or history deletion is allowed.

Original source commits were reconciled with their exact integrations:
contract de794eb9→4a8f882, topology d51b32b→96965b5, pending bridge 5c70778→8cf12cb,
PG frontier b130630→b4873fc, native receipts 0bc8bcb→7e01db1,
journal 8388712→fa15a74 and runner 083c202→150f1da. Frozen author manifests remain
in the preserved worktrees and are linked by the existing evidence records.
Root CLI/composition 878d671, overlap/count corrections d7b8aec and subsequent
compiler/diagnostic/lint corrections end at the qualified product ced7693.

Independent AI composition review found and closed two source defects: changed
per-family counts at the retained prefix must refuse before publication, and the
zero-new-attempt path must hold the actual manager preparation across peak admission.
Record `.research/archive-catch-up-resume-20261018/composition-review.json`,
SHA d7fa596b81b6a24fd0ff1b09d2b6e989bf88079b3635676725cd5b8d3ce4b726.
Independent complete journal/census review includes exact locked rustix 1.1.5
allocation source; record `journal-review.json` in the same directory,
SHA f83941fbb706ffc9860157bd493829c7253f455e191f3dcf091f89976a88ee0e.
These are scoped source reviews, not power-loss or connected-engine evidence.

## Qualified artifacts and retained failures

Current exact packaged build: 40bc295f97adf00d5f2afe833eaef9e99f3b1f23,
tree 3e3076829b908bcdd819fa0a64541ff614940862. Root:
`/home/tux/bigbrotr-implementation-20260917/runs/ingest-succession-archive-installed-40bc295`.
All five checks PASS: CLI/probe compilation, journal 6, Serve 26, entrypoint 8 tests
and Clippy without diagnostics. Actual worker build, packaged build-info and
verify-worker PASS. The unchanged library-test Copy-drop warning at
lineage_replay_tests.rs:498 remains separate. No connected-engine claim yet.

Run 16:05:46Z–16:09:07Z; prepare 94443, build 14910 and audit 78427 joined 0.
Read-only `audit-root/audit-v1.json` SHA
dafd7715e55cf9ec89a9b448f7056df6725a99edd9dcb2e78d95c317c568989f
binds 1974 source files, 178 prior plus 6 new = 184 protected ELF paths and 1392
unchanged prior containers. Fetch/native exited 0 / PID 0 / no OOM. Free after build:
73,310,355,456 bytes. Use `out/artifacts` as installed build argument. Prior
ced7693 PASS remains retained with audit 1c266222 and its exact earlier scope.

All preceding failed build roots remain under the same lab prefix, with their
original scripts, logs, retained ELFs and separate read-only custody audits:

| Revision | Actual result | Audit SHA prefix |
| --- | --- | --- |
| d7b8aec | Worker PASS; probe compilation E0432; tests/Clippy NOT_RUN | 0a904870 |
| b3735e9 | Compilation, journal 6 and Serve 19 PASS; entrypoint 7 PASS/1 FAIL on diagnostic wording; Clippy NOT_RUN | 692c4dec |
| 1d42894 | All 33 tests and packaged checks PASS; Clippy manual_range_contains FAIL | b724078d |
| 48956e2 | All 33 tests and packaged checks PASS; Clippy collapsible_if FAIL | f64a7fdd |

Earlier e613a19/tree973205bb library-only build passes 35 tests: historical pending
5, retained PG frontier 6, receipts 3, membership 3 and custody 18, plus Clippy.
Audit a8263d7e binds that exact scope; it does not prove installed composition.
All one-shot preparation scripts for these runs are EXECUTED; never replay them.

## Work in progress and next action

Frozen same-root journey a2439a8 is integrated on main at 51fff8f. Author worktree
`.research/archive-catch-up-journey-20261018`, branch
`qualification/archive-catch-up-journey`, is clean and retained. Seven new
`installed_archive_catch_up*.py` modules plus four narrow existing hooks are
reviewed. Author AST/import/25 bounded controls and root manifest/diff checks PASS.
The first inert-import smoke FAIL is retained and corrected. At that source freeze, engines and full scientific replay were NOT_RUN;
the two later failed engine attempts are recorded below.

The journey keeps one freshly initialized physical dataset/catalog/coordinator:
initial signed Archive/Sync, terminal epoch 1, genuine empty epoch 2 then retirement,
genuine signed successor epoch 3 then terminalization, catch-up and fresh retry,
ordinary authorized history/export, joined stop/restart and repeated read/export.
Expected independent science: 22 facts, 7 original batch receipts, family counts
[13,2,2,2,3], snapshot 8, bounds 12/0/10, frontier R+3. All 75 Control tables checked.
Root and independent AI reviewer closed the actual frozen source; full report
`.research/archive-catch-up-resume-20261018/journey-source-review.json` is bound
by the evidence node `archive_connected_journey_source`. The author manifest
SHA 9a968649 and complete frozen source hashes remain in the author worktree.
Source review is not connected-engine evidence or exhaustive negative coverage.

Custody template is frozen at
`.research/archive-catch-up-custody-20261018/audit-engine-template.py`,
SHA d26aa071f6150aa46da84d5fb33f80d44283307e0a81cab2aa7968d7c41099d6.
AST-only PASS; execution NOT_RUN. Root preparation script
`.research/archive-catch-up-resume-20261018/prepare-journey.py` first attempt:
session 62229 joined 1 at the free-space guard, before creating any engine root.
That failure is preserved; no database, worker or runtime began in that attempt.
The later admitted run below is now EXECUTED and must never be overwritten.
Further fresh preparation requires actual reserve 69,324,505,088 B and reconciled
clean exact source; never replay a successfully created one-shot run.

The first preparation failure remains retained. The subsequent bounded cleanup
retired 16,943 verified compiler intermediates from eight historical targets,
retaining all 227,593 other entries, 178 qualified ELFs and unchanged container
projections. Prepare 3240 and execute 28698 joined 0. Frozen manifest bb2c965f,
receipt and deletion journal are under `.research/tux-maintenance-followup-20261018`;
the same lab inventory node binds full hashes and target names. Free space after
cleanup was 78,657,138,688 B; measured gain 16,878,981,120 B. These eight targets
are retired partial caches; preserve final artifacts and never reuse or replay
their executor. The earlier 56-file current-cache retirement remains separately
recorded. Unrelated nsealr work and the six unrelated services were untouched.

Actual engine root:
`/home/tux/bigbrotr-implementation-20260917/runs/ingest-succession-archive-catch-up-engine-6474507`.
Exact source 64745070b7d9601ad0b2ee6af5d353198e0d874a,
tree c77db8676e459c9fc9e80327561f144456c19edb. Preparation 69850 joined 0,
with 78,656,954,368 B free and unchanged 69,324,505,088 B admission requirement.
`root-preparation.json` and `root-engine-invocation.json` bind the inputs.
Root wrapper 40167 joined 1 at 15:29:04Z; `bindings/result.json` is FAIL.
Initial child 212 was unforced/reaped, exit 4 after 35.43s, reporting
`initial Ingest phase completed after its deadline; preserve retained state`.
The HTTP client then exited 1 with ConnectionRefused. Only initial startup was
reported; two actual attempt input files/Parquet groups exist, which do not alone
prove catalog COMMIT. No continuation journey JSON or read/export/catch-up ran.
PG/client exited 0/143, stopped/PID0/noOOM, with empty cleanup/missing/unrelated
errors. Preserve this root; never rerun its preparation or overwrite the failure.

Failed-run custody audit25350 joined 0: `audit-root/audit-failure-v1.json`,
SHA3df265f2, binds 1974 source files, 178 protected ELFs and232 retained engine files.
Custody PASS preserves engine FAIL; all13 continuations/scientific replay NOT_RUN.
Root then restarted only the owned PG for explicit read-only repeatable-read SQL.
`diagnostic-root/sql-forensics-v1` preserves complete75-table Control and32-table
catalog captures, unchanged original66/64-table datasets, redacted role metadata
and no other client backends. Both catalog snapshots2/3 and Control frontier
revisions1/2 are committed, with complete sequence bounds2/3 and dictionary counts2/3.
No native or Parquet scientific reader was opened. The exact expired phase remains unknown.

Diagnostic wrapper65011 joined1: its final assertion compared Docker Mounts in
list order. Only order changed; complete normalized mount objects match. The
original FAIL report0a533860 is preserved. Separate read-only reconciliation
`custody-reconciliation.json` SHA6f96ca89 passes and binds all raw captures,
232 unchanged engine files and unchanged other1391 containers. PG exited 0/PID0,
noOOM at15:41:43Z; no product/native process was restarted. Do not replay either
one-shot diagnostic or old auditor against the intentionally changed PG timestamps.

Independent AI source review proves a separate scheduling counterexample:
HTTP can arrive after a pre-select100ms safeguard, immediately before renewal,
and receive an arbitrarily short operation budget. Both main/external dispatch
need admission-time scrutiny. This does not identify the actual failed callsite.
`.research/archive-catch-up-resume-20261018/initial-deadline-review.json` and the
existing evidence node bind this distinction; no timeout widening is justified.

Diagnostic 90dfa92→2315fb6 and admission 68cc6dd→40bc295 are integrated and pass
fresh component qualification. Six new admission tests plus one diagnostic test
and the strengthened existing diagnostic test run within actual Serve 26. Python
preservation controls remain their separately recorded source-only scope.
Manifest f102622d in the retained author worktree binds the admission source.
Root and independent AI review closed exact hashes before build.

Admission is rechecked after selection; one pending Work retains its existing
grant. The central renewal guard refuses a second renewal for that unadmitted
item. An exact 100 ms renewal horizon refuses; original absolute Archive deadline
caps every nested phase/command. Cancellation and expiry are checked before the
first nested poll. No lease/config widening or progress claim under arbitrary descheduling.
The prior 6474507 engine's actual expired callsite remains unknown.

Second engine root ends `archive-catch-up-engine-be57042`, source
be5704224c71f55e7d128a880fd8601bc85ee8f9, tree 5e098299471035f212bfbc78e51974f3eda8f718.
Prepare 98751 joined 0; engine 8902 joined 1, 16:11:15Z–16:13:39Z. Initial plus
11 continuations through catch-up/retry completed. Catch-up reports snapshot 8,
frontier 7, counts [13,2,2,2,3], 3 new attempts and 3 frontier revisions; fresh retry
reports unchanged 7/8 and 0/0 new effects. These are actual retained outputs,
not completed independent full-journey scientific replay.

Ordinary Control then exited 0/unforced/reaped with exact/global backend absence,
but the HTTP fixture failed after CLI login, before its first direct HTTP request:
`def http` shadows the imported module, causing AttributeError at HTTPConnection.
All 13 installed terminal receipts are zero/unforced/reaped; cleanup errors empty.
The partial journey remains FAIL; ordinary verification, restart and final
oracle/negative controls/replay NOT_RUN. New evidence node
`archive_connected_journey_attempts` retains this separately from 6474507.

HTTP alias fix 08d282c→3d0f09d changes two lines. All six source-extracted
regression controls PASS, including exact original AttributeError reproduction.
Manifest 3bd70045 binds source and actual failure; this is no runtime pass.

Failed-run custody audit 43564 joined 0: `audit-root/audit-failure-v1.json`,
SHA f597c077, binds 1974 source files, 184 protected ELFs, all 13 process
receipts and 240 structurally bound raw captures. Engine FAIL stays unchanged;
no scientific replay or additional engine ran. Full manifest and limits are in
the same evidence node. Do not replay this audit after restarting borrowed PG.

Next bounded action: prepare one fresh same-root journey with corrected HTTP
fixture 3d0f09d and unchanged qualified 40bc295 artifacts.
No Rust rebuild is needed for this experiment-only correction. Preserve both
failed roots. Success auditor 8402a325 / manifest 15de291e is still NOT_RUN;
new preparation must use current stopped PG state and 184 protected ELF paths.

Required interruption/COMMIT-loss, cancellation/expiry and refusal qualification
still follows a successful normal journey. Independent source mapping identifies
publication after-COMMIT delivery loss, fresh-process receipt reuse plus first
bridge delivery loss, fresh original operation resolution and exact retry as the
next useful fault slice. Existing proxy selects actual queries; never infer a
fixed COMMIT ordinal or call a local timeout SQL UTC expiry. Other refusal and
capacity boundaries remain separate.

## Host resources and preservation

Engine 8902 is joined; no owned runtime/compiler/SQL/native remains active.
Borrowed PG stopped at 16:13:39Z, client exit 143; PID 0 / no OOM. All source-only
agent tasks completed. Unrelated workloads remain untouched. Verify ownership
before any next engine action.

Root-only compiler cache:
`/home/tux/bigbrotr-implementation-20260917/runs/network-1154757/out/build/{target,cargo-home}`,
owner 8b21a13162699a6038154874c2ac89da, exclusive `.compiler-cache.lock`.
Build image sha256:7e946a0c0aa4a5d357c3ae517fe9830f675cfc98fd26a9cba79b2bc957928b9d,
Rust 1.98.1 Linux amd64. Pinned read-only Archive assets are under
`/runs/installed-publication-61271e2/out/archive-worker` in the same lab.
Build admission is 12 GiB; Archive/export admission is 69,324,505,088 bytes.
Borrowed PG 7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec,
owner 5e69b0f060b1607c25230d1dbd3e5608, lives at
`/runs/relay-source-recheck-e06f580/pg` in that lab; stopped, retained, PG 16.15.

Headroom cleanup completed 13:44:18Z, prepare 59509/execute 50680 joined 0.
`.research/tux-maintenance-20261018` retains frozen manifest 3deba21a, deletion
journal 5bcb902f and receipt a59dbbda. Exactly 57,469 verified compiler intermediates
in 30 historical targets were removed, freeing 54,485,643,264 observed bytes.
772,682 retained entries and all then-qualified 152 ELFs were preserved. These 30
targets are retired partial caches; never reuse them. Current cache, native data,
Docker data, secrets and evidence were untouched. This one-shot cleanup is EXECUTED.

Seven Archive source refs retired only after verified push b8dfa61; session 51903
joined 0. Original and integrated commits were reconciled, then complete histories
archived under `refs/archive/retained-archive-components-20261018/` and bundled in
`.research/branch-retirement-20261018/archive-components.bundle` (15,385,967 bytes,
SHA 97b09ad6ce41d5c3c3f084c771ae0a4f8e76d72d7c594c8dd45dd78abe5f4c28).
Report `archive-components.json` SHA
8b60df247c8bbb2c9429ac5aead2a752354178aae3856a53eab50ce39eab5c9a.
Every tracked/ignored/link/stat census stayed identical. Retirement is COMPLETE;
never replay its executor. Current Git: 144 worktrees, 142 detached; main and the
active journey branch remain locally, remote main only. No worktree, original/fault
dataset, artifact or container was deleted. All older bundles and pause snapshots remain.

## Remaining gates

Selected catch-up excludes ACK/GC, unresolved Ingest tails, import logical closure,
cross-boot restore, S3 and continuous publisher scheduling. They do not disappear
from their owning tasks where selected. Broader M1–M5 journeys, backup/restore,
capacity/aged-state/RSS and the five exports below 256 MiB remain incomplete.
Earlier acquisition DSM failure, PG checkpointer SIGSEGV, missing schema32 response,
original build/fixture failures and dirty network-1154757 evidence stay retained;
none are relabelled PASS. GitHub billing/Q33, native arm64, signing, registry and
public promotion retain their separate gates. GitHub protects committed source;
ignored/external laboratory artifacts stay on Tux. No public release is authorized.
