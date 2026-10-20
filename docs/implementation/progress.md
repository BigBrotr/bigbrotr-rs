# Current execution checkpoint

Observed 2026-10-20 on Tux. Goal **ACTIVE**; M1–M5 incomplete. Existing plan,
Q37 model allocation and Q38 evidence reuse apply. Root owns integration/runtime.

## Objective and revision

P02-T02 Steps4/5/7: classified producer checkpoint → synchronized native ACK →
restart. Current product d4e1b85 implements typed PG progress with ACK0, retaining
all requests/receipts/inputs and granting no new credit. Qualified build, three
successor Sync contexts and historical epoch3 recovery/refusals remain reusable.

Main fc97d43eec95c7b553417882f0e4395b2e650323 was pushed and verified equal to
remote. This checkpoint/report/inventory change records two failed fixture
attempts; product code is unchanged. Integrated original worktrees and ignored
artifacts remain after redundant branch-ref retirement.

The [owning recovery report](evidence/p02-producer-recovery-native-20260920.json),
`initial_successor_preparation_20261012.repeatable_succession_20261015.
producer_checkpoint_ack_preparation_20261020`, owns exact source/build/review pins,
original failures, PASS controls and raw replays. The [inventory](../research/laboratory-inventory.json)
owns laboratory paths, branch custody and resources; no cleanup was performed.

## Current work and processes

Initial/activated source is in `.research/producer-progress-contexts-20261020`,
branch `work/producer-progress-contexts`, author `export_setup_review`.
Initial9b driver80208 failed before Serve because its oracle confused reserved
migration families with core chronology. Correction9653725 passed setup, but
initial driver15335 failed registration: guard_initial_activation_target lacks
its narrow fixture SELECT on initial_ingest_activations. Both joined1; exact
clients/borrowed PG are exited/PID0/noOOM/cgroups absent. Original data/logs remain.
The author is correcting fixture ACLs; no initial/activated Sync PASS yet.

COMMIT-loss source ca77ca0 is frozen in `.research/producer-progress-commit-loss-20261020`,
branch `work/producer-progress-commit-loss`, base45cedcf. Its six loss/recover/retry
cases and82 controls are NOT_RUN. `producer_recovery_review` performs critical
review, briefly prioritizing the initial ACL correction. No engine/build active.

## Next action and open limits

Review the narrow ACL correction, then run actual initial and activated epoch2
entrypoints on unchanged qualified d4e1b85 binaries in separate owned roots;
perform each raw replay. Finish ca77ca0 critical review before its real fault run.

ACK/credit/GC, preparation-hole and further recovery/refusal/resource gates remain
open in the report. Job checkpoint0 and retained historical values do not imply
workflow completion, remote cursor or coverage. Original a99 Sync timeout cause
remains unresolved. NoGC64/32MiB is temporary, not capacity qualification. Q33 CI
billing, arm64 and signing/registry remain separate gates. No public release.
