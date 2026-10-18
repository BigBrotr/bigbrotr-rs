# Current execution checkpoint

Observed 2026-10-18T15:02:28.403270+00:00 on Tux. Goal ACTIVE after the owner's explicit resume.
M1–M5 remain INCOMPLETE. Existing task files own the plan; this checkpoint locates
current work and evidence. Follow the [resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint).
Historical detail remains in the evidence/laboratory records below and Git history;
the longer checkpoint at `97b8dc9` is preserved, not a current process inventory.

## Repository and active responsibility

Q34/Q35: Tux alone, root sole integrating owner. Q36: `main` is integrated state.
Canonical `/home/tux/bigbrotr-rs`; current HEAD and verified remote `97b8dc9`.
Product `ced7693` and packaged qualification `b8dfa61` are committed and pushed.
Ref retirement is committed and pushed at `97b8dc9`. No merge/rebase is pending.
This checkpoint simplification is not yet committed. No owned build/engine is active.

Current task: P02-T04 Steps 3/4/5, enabling P01-T09 Step 4/M1 after the selected
P02-T01 Step 5 / P02-T02 Step 4 succession work. See the
[storage task](02-storage.md),
[finite catch-up contract](../specification/archive-storage-contract.md#finite-retained-archive-catch-up-selected-under-implementation)
and [native handoff/profile](../reference/archive-handoff.md#finite-retained-lineage-catch-up-selected-under-implementation).
The installed `archive catch-up --file ROOT` source is wired and its components
pass. Connected catch-up/read/export/restart qualification remains NOT_RUN.
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

Current exact packaged build: ced7693e0ce78553f5a64b7a67dfb7d1c7cd31d1,
tree 89360589aca7e4368ce2df4752206a4a3dfd0921. Root:
`/home/tux/bigbrotr-implementation-20260917/runs/ingest-succession-archive-installed-ced7693`.
Profile `ingest-succession-archive-installed` passes all five component checks:
actual CLI/probe compilation, journal 6, Serve 19, entrypoint 8 tests and Clippy.
Exact worker build and packaged `build-info`/`archive verify-worker` pass.
Clippy has zero diagnostics; the unchanged library-unit `dropping_copy_types`
warning in `lineage_replay_tests.rs:498` remains separately recorded.

Run 14:52:07Z–14:55:26Z; prepare 24369, build 5625 and audit 49465 all joined 0.
Read-only `audit-root/audit-v1.json` SHA
1c266222096017e27c487ba076b49519ebb69502dd6d77b5c126e4743aed8fb9
binds 1967 source files, 172 prior plus six new = 178 protected ELF paths,
actual pinned worker/assets, raw test output and 1389 unchanged prior containers.
Fetch/native exited 0, PID 0, no OOM. Completion free space: 74,571,853,824 bytes.
Use `out/artifacts` as the installed build argument, not the run directory.
The independently built worker is embedded into the CLI by exact size/SHA identity.

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

One source author owns `.research/archive-catch-up-journey-20261018`, branch
`qualification/archive-catch-up-journey`, base b3735e9. New
`experiments/ingest-bootstrap/installed_archive_catch_up*.py` and narrow hooks in
`installed_archive.py`, `installed_archive_journey.py`, `installed_archive_read.py`
and `experiments/acquisition-scope/bindings.py` are still uncommitted author work.
Do not overwrite it. Root owns all compiler/cache/native/SQL/Docker operations.

The journey keeps one freshly initialized physical dataset/catalog/coordinator:
initial signed Archive/Sync, terminal epoch 1, genuine empty epoch 2 then retirement,
genuine signed successor epoch 3 then terminalization, catch-up and fresh retry,
ordinary authorized history/export, joined stop/restart and repeated read/export.
Expected scientific state is independently constructed: 22 facts, seven original
batch receipts, family counts [13,2,2,2,3], snapshot 8, source bounds 12/0/10.
Validated initial frontier revision R advances by three. All 75 Control tables
remain checked. No SQL/catalog clone supplies native/coordinator authority.

Root read the new driver/client/model/oracle/HTTP drafts and C-API reader, retaining
review snapshots under `.research/archive-catch-up-resume-20261018/root-journey-read-v1`.
Final source/hash deltas still need review. Independent reviewer recovery_review
is active; its transient model-capacity interruption was resumed, not called PASS.
Findings corrected by the author: coordinator incarnation equals current Control;
reader pin TTL is a remaining-runtime scalar bounded by five seconds, while the
durable export pin remains exactly 30 seconds. Ordinary wire formats, source-cut
shape, exact backend helper signature, system-role observations, raw Archive inputs
and failure-aware transport-client joins were also corrected during source work.
The top-level verifier and raw binder are still being completed. No engine result
or final review is claimed for this draft.

Custody author cleanup_review writes only the ignored
`.research/archive-catch-up-custody-20261018` directory. Its candidate auditor is
NOT_FROZEN/NOT_RUN pending the final binder contract. Old source worktrees remain
quiescent. Root's `.research/archive-catch-up-resume-20261018/prepare-journey.py`
is authored and NOT_RUN; it requires clean exact source, the qualified build audit,
178 preserved ELFs and unchanged completion reserve before creating a new root.

Next bounded action: finish/freeze the genuine journey source and raw binder;
review final deltas and bind source/protocol checks; integrate coherently; prepare
and run the actual installed journey with ced7693 artifacts. Join the wrapper,
then independently replay raw observations, then run the custody audit. Preserve
all failures and fix only their actual cause. Record results in the existing
report/lab nodes and this checkpoint. After the normal journey, qualify the new
owner's required interruption/COMMIT-loss, cancellation/expiry and refusal boundaries,
reusing unchanged valid component evidence at its exact scope.

## Host resources and preservation

No owned build/engine remains active at the latest completed audit. Three bounded
source tasks are active: journey author, independent reviewer and custody-auditor
author. No runtime resources belong to those tasks. Six unrelated services remain
outside project ownership; do not touch them. Reobserve actual ownership before reuse.

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
