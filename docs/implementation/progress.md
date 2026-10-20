# Current execution checkpoint

Observed 2026-10-20 on Tux. Goal **ACTIVE**; M1–M5 incomplete. Existing plan,
Q37 models and Q38 evidence reuse apply. Sole integration/runtime owner: root.

## Objective and revision

P02-T02 Steps4/5/7: durable classified producer checkpoint → synchronized native
ACK → restart. Current increment implements typed PG progress with native ACK0,
retaining every request/receipt/input (noGC); it grants no new credit.

Main `c89b9e2052cd291640cdabfdf6905ed0be6853f9` was pushed and verified equal
to remote. It integrates candidate `75df76037cb5c60458165414f133d9deafcc4112`.
Five reconciled original source/integration branch refs were retired; all their
clean worktrees remain detached at their original commits, including
`.research/producer-progress-integration-20261020`. Ignored evidence is preserved.
The reviewed `738637f1c7154f2199eddecf86c83cdd5252bbdb` recovery fixture and its
qualification are now staged for one coherent integration commit.

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
`export_setup_review` owns the initial/activated fixture in its separate worktree.
Proxy88a926 is frozen in `.research/producer-progress-fault-20261020`, with root
and independent Astra source review,20 discriminator/wire tests and5 existing
proxy testsPASS. It is not integrated or connected product evidence yet.
The [inventory](../research/laboratory-inventory.json) owns paths and custody.
No cleanup; original datasets, failed runs, logs and worktrees remain intact.

## Next action and open limits

Finish this integration/push, then review/run the initial and activated-epoch2
Sync fixture on qualified d4e1b85 binaries. Integrate the reviewed narrow proxy
and add the agreed proof-only COMMIT-loss scenario using existing collectors and
oracles; active old-Control recovery requires its explicit lifecycle profile.
Reuse unchanged component builds.

The new job keeps jobs.checkpoint_revision=0; historical jobs retain their actual
values. Classified local positions are not remote cursor/coverage. Post-hole
ordinary admissions remain; progress refuses crossing unclassified mutations.
NativeACK, two-slot credit, COMMIT/reply-loss, all historical recovery contexts
and other recorded refusal/resource gates remain open. NoGC history64/32MiB is
temporary, not indefinite capacity or task completion. Original a99 Sync timeout
cause remains unresolved. TLS/other credentials, whole-service capacity,
ACK/GC/import/restore and M1–M5 remain open. Q33 CI billing, arm64 and
signing/registry remain separate gates. No public release.
