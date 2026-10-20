# Current execution checkpoint

Observed 2026-10-20 on Tux. Goal **ACTIVE**; M1–M5 incomplete. Existing plan,
Q37 models and Q38 evidence reuse apply. Sole integration/runtime owner: root.

## Objective and revision

P02-T02 Steps4/5/7: durable classified producer checkpoint → synchronized native
ACK → restart. Current increment implements typed PG progress with native ACK0,
retaining every request/receipt/input (noGC); it grants no new credit.

Main integration `45cedcf3d5d4f0bbed650e7bcefdb0b12b5b43e3` was pushed and
verified equal to remote. It includes recovery738637f and proxy88a926; product
source remains exactly qualified d4e1b85. The two integrated local fixture refs
were retired after clean/ancestor/remote-ref checks. Their exact original
worktrees and ignored evidence remain; this checkpoint records custody only.

The [recovery report](evidence/p02-producer-recovery-native-20260920.json),
`initial_successor_preparation_20261012.repeatable_succession_20261015.
producer_checkpoint_ack_preparation_20261020`, owns all exact pins and attempts.
Product d4e1b85 passes 76 tests, 19 required controls, eight retained ELFs and both
Clippy selections. Fixture 75df760 passes three real successor Sync lifetimes,
77-table/full signed native/input preservation and 21 negative controls each.
Raw replay binds 1013 files and repeats science/controls. Earlier failures remain
FAIL, including migration syntax, historical xmin rewrite and the overbroad
historical-jobs checkpoint0 assertion; their corrections are recorded separately.
Fixture738637f additionally passes typed epoch3 recovery, exact retry and two
corrupted-copy refusals, with60 negative controls and raw replay binding856 files.
Full state/xmin/native/input preservation and exact refusal diagnostics pass.

## Processes and resources

Recovery driver61121 and raw replay35387 joined0. Exact owned client and borrowed
PG were freshly observed exited/PID0/noOOM/cgroups absent; no cleanup errors or
unrelated changes. Prior build/successor runs remain joined. No engine/build active.
`export_setup_review` owns the initial/activated fixture in its separate worktree;
`producer_recovery_review` reviews it. Before runtime, correct list-vs-table
comparison, strengthen initial lifecycle checks, move backend exclusion before
native reopen and finish raw-file binding. These source findings are not product
failures or passing runtime evidence.
Proxy88a926 is frozen in `.research/producer-progress-fault-20261020`, with root
and independent Astra source review,20 discriminator/wire tests and5 existing
proxy testsPASS. Its integration supplies a tool, not connected product evidence.
`export_setup_fixture` now owns source-only COMMIT-loss/recovery work in
`.research/producer-progress-commit-loss-20261020` on its matching work branch,
base45cedcf. The other source worktree is `.research/producer-progress-contexts-20261020`.
The [inventory](../research/laboratory-inventory.json) owns paths and custody.
No cleanup; original datasets, failed runs, logs and worktrees remain intact.

## Next action and open limits

Finish critical source review and raw binding of the initial/activated-epoch2
fixture, then run each actual entrypoint on qualified d4e1b85 binaries in separate
owned roots. The parallel COMMIT-loss source must model active old-Control
recovery explicitly before its own review/runtime. Reuse unchanged component builds.

The new job keeps jobs.checkpoint_revision=0; historical jobs retain their actual
values. Classified local positions are not remote cursor/coverage. Post-hole
ordinary admissions remain; progress refuses crossing unclassified mutations.
NativeACK, two-slot credit, COMMIT/reply-loss, all historical recovery contexts
and other recorded refusal/resource gates remain open. NoGC history64/32MiB is
temporary, not indefinite capacity or task completion. Original a99 Sync timeout
cause remains unresolved. TLS/other credentials, whole-service capacity,
ACK/GC/import/restore and M1–M5 remain open. Q33 CI billing, arm64 and
signing/registry remain separate gates. No public release.
