# Current execution checkpoint

Observed 2026-10-23 on Tux at the owner's explicit safe-pause request. All
development is stopped pending explicit resume; M1–M5 remain incomplete. The
goal control is to be set to **PAUSED** after this checkpoint is committed and
pushed; consult the live control on resume. Root owns integration/runtime;
Q37 model allocation and Q38 evidence reuse remain in force.

## Objective and revision

P02-T02 Steps4/5/7: classified producer checkpoint → synchronized native ACK →
restart. Integrated source is main `25cf6cf00a930350d08c2ba9591cf2d2077691a9`
(tree `d1282b7042d5a841991432de384212c5fcd7c0ba`). It includes reviewed
ACK-loss fixture `70fd750`; product/build `fcad4c9` is unchanged. Before this
checkpoint, only the evidence report, reference and inventory had local edits;
there were no uncommitted source changes or merge/rebase in progress. This
documentation commit saves those records and this checkpoint. Its own revision
is the containing commit, not a guessed future hash. Last previously verified
remote main was `a3800ec`; push the coherent source/checkpoint ancestry together.

The [owning recovery report](evidence/p02-producer-recovery-native-20260920.json),
`initial_successor_preparation_20261012.repeatable_succession_20261015.
producer_checkpoint_ack_preparation_20261020.native_ack_source_preparation`,
owns exact receipts, invocation premises, failures and remaining gates. The
[inventory](../research/laboratory-inventory.json), ACK node's
`pause_resource_observation_20261023`, owns fresh process and resource custody.

## Completed evidence and remaining work

- **PASS:** reviewed product/build fcad4c9: 258 component tests in 19 groups,
  30 controls, build and both Clippy selections. Eight authenticated ELFs remain
  under `/home/tux/bigbrotr-implementation-20260917/runs/producer-ack-fcad4c9/out`.
- **PASS:** actual initial ACK941847c, activated epoch2 a3800ec and successor
  epoch3 a3800ec, each with registered window2, three new signed events and a
  separate proof-only resend. Complete science/raw replay passes with 48/41/31
  controls respectively. Reuse these results; originals remain in the three
  `installed-ack-*` run directories identified by the report.
- **PASS:** ACK-loss source/critical review at70fd750, merged main25cf6cf.
  Integrated pure verifier regression saved all ten cases and 176 controls.
  **FAIL wrapper:** session73439 exited1 on a final presentation `KeyError:
  controls` after saving that complete PASS receipt. Each saved result was read;
  the scientific checks were not rerun. This does not qualify new fault engines.
- **FAIL preserved:** original initial result assembly, activated quiet-wire
  selection, first successor standalone raw-reader import and earlier aggregate
  regression on historical activated965 lacking required ACL capture. Corrected
  invocations and their narrower claims are recorded separately in the report.
- **NOT_RUN:** new ACK mutation-COMMIT-loss engines, recovery/retry, 140 controls
  and strict raw replay. Source integration is not runtime qualification.

## Agent work and preserved resources

`producer_ack_loss_resume` and `producer_ack_loss_critical` completed; both were
informed of the pause. Their Astra/xhigh configuration observations and receipts
are recorded in the inventory/report. No active child work remains.

Four ACK worktrees under `.research/` were freshly observed clean: sync atfcad4c9,
storage atfff6901, positive fixture atf5cd4e9 and loss at70fd750. Their source is
integrated; the loss worktree retains its earlier `paused-partial-v1.json/.diff`
and completed author receipts. No partial snapshot was deleted or relabeled PASS.
Critical review and regression receipts remain under
`.research/producer-checkpoint-ack-review-20261020/`.

All runtime, build and offline drivers are joined. Fresh process inspection found
no project work or writer; exact latest client and borrowed PG are exited/PID0,
without OOM or cgroups, and no project Docker container is running. Unrelated
services remain untouched. All datasets, logs, original failures, `.research`,
worktrees and external artifacts are preserved. No cleanup or new run was begun
for the pause. Recheck ownership/state before reuse.

## Precise next step after explicit resume

Create one new detached exact-source lab after checking space and ownership.
Reuse `producer-ack-fcad4c9/out/artifacts` as BUILD. Execute existing
`python3 -B source/experiments/ingest-succession/installed_progress_commit_loss.py LAB BUILD --producer-ack`,
then `python3 -B -S source/experiments/ingest-succession/installed_progress_commit_replay.py LAB --producer-ack`.
Output directory is `installed-producer-ack-commit-loss`. Preserve sequential
predecessor SQL/native/input binding for both faults and four recovery/retry
processes; expected 140 controls remain unexecuted. Record actual PASS/FAIL in the
same report and inventory. No product rebuild or positive-case repeat is needed
unless its premises change.

GC, preparation-hole, mixed-restore/refusal/resource gates and original a99 Sync
timeout cause remain open. NoGC64/32MiB is temporary, not capacity qualification.
Q33 CI billing, arm64 and signing/registry remain separate gates. No public release
or overall-completion claim.
