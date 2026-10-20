# Current execution checkpoint

Observed 2026-10-20 on Tux. Goal **ACTIVE**; M1–M5 incomplete. Existing plan,
Q37 models and Q38 evidence reuse apply. Sole integration/runtime owner: root.

## Objective and revision

P02-T02 Steps4/5/7: durable classified producer checkpoint → synchronized native
ACK → restart. Current increment implements typed PG progress with native ACK0,
retaining every request/receipt/input (noGC); it grants no new credit.

Main `d75306534d273866dbd278d4b283bb527de0c3bf` was verified equal to remote.
The merge carrying this checkpoint integrates clean candidate
`75df76037cb5c60458165414f133d9deafcc4112` from
`work/producer-progress-integration` in `.research/producer-progress-integration-20261020`.
Original author worktrees and ignored evidence remain preserved.

The [recovery report](evidence/p02-producer-recovery-native-20260920.json),
`initial_successor_preparation_20261012.repeatable_succession_20261015.
producer_checkpoint_ack_preparation_20261020`, owns all exact pins and attempts.
Product d4e1b85 passes 76 tests, 19 required controls, eight retained ELFs and both
Clippy selections. Fixture 75df760 passes three real successor Sync lifetimes,
77-table/full signed native/input preservation and 21 negative controls each.
Raw replay binds 1013 files and repeats science/controls. Earlier failures remain
FAIL, including migration syntax, historical xmin rewrite and the overbroad
historical-jobs checkpoint0 assertion; their corrections are recorded separately.

## Processes and resources

Build 87204, installed 67301 and raw replay 95690 joined with exit 0. Exact owned
build/client containers and borrowed PG were observed exited, PID 0, no OOM; final 75df760 cgroups
absent, no cleanup errors or unrelated changes. No build/runtime active.
Source-only agents `export_setup_review` and `producer_recovery_review` now own
separate initial/activated and historical-recovery fixture extensions based on
75df760. Their unpublished edits remain outside main until reviewed.
The [inventory](../research/laboratory-inventory.json) owns paths and custody.
No cleanup; original datasets, failed runs, logs and worktrees remain intact.

## Next action and open limits

Finish this coherent main commit/push, then review and run the bounded fixture
extensions on qualified d4e1b85 binaries: actual initial and activated-epoch2 Sync,
and fresh config-only typed successor recovery/retry with preserved origins and
specific corrupted-copy refusals. Reuse the existing runners and independent
oracles; do not repeat unchanged component builds for fixture-only changes.

The new job keeps jobs.checkpoint_revision=0; historical jobs retain their actual
values. Classified local positions are not remote cursor/coverage. Post-hole
ordinary admissions remain; progress refuses crossing unclassified mutations.
NativeACK, two-slot credit, COMMIT/reply-loss, all historical recovery contexts
and other recorded refusal/resource gates remain open. NoGC history64/32MiB is
temporary, not indefinite capacity or task completion. Original a99 Sync timeout
cause remains unresolved. TLS/other credentials, whole-service capacity,
ACK/GC/import/restore and M1–M5 remain open. Q33 CI billing, arm64 and
signing/registry remain separate gates. No public release.
