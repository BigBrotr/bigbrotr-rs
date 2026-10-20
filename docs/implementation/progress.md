# Current execution checkpoint

Observed 2026-10-20 on Tux. Goal **ACTIVE**; M1–M5 incomplete. Existing plan,
Q37 models and Q38 evidence reuse apply. Sole integration/runtime owner: root.

## Objective and revision

P02-T02 Steps4/5/7: implement one durable producer checkpoint → synchronized owner
ACK → restart path, retaining all request/receipt detail initially (noGC).
This checkpoint follows main `61ffa80d9d0268e9429ab5a0b2c97bc94340d498`
(last exact verified remote; push66258 joined0). Main adds the reviewed
`producer-progress` profile to the existing runner; build/runtime NOT_RUN.
Product implementation is isolated on `work/producer-checkpoint-ack`
at `.research/producer-checkpoint-ack-20261020`, base `0e4ac3e`.
Current new files are postgres/producer_progress.rs and rocks/producer_checkpoint.rs;
they remain incomplete/unverified author work.

The [recovery report](evidence/p02-producer-recovery-native-20260920.json),
`initial_successor_preparation_20261012.repeatable_succession_20261015`, owns
`producer_checkpoint_ack_preparation_20261020` and the completed
`archive_export_memory_source.manager_observation_20261020.credential_boundary_20261020`.
Current export mapping is P08-T04 Step3; prior P08-T05 label was corrected without
rewriting historical records. P08-T05 remains the separate backup/restore task.

## Processes and resources

No runtime/build/cache/cleanup active. Fresh container observation: no BigBrotr
container running; the six unrelated containers remain unchanged. Prior runtime77164
joined0 and its stopped resources/artifacts remain in the [inventory](../research/laboratory-inventory.json).
Astra/xhigh export_setup_fixture owns source implementation in the isolated
worktree. export_setup_review completed fixture preparation and builder review;
their pinned receipts are in the report. Root alone owns builds/runtime/integration.
All prior artifacts retained.

## Next action and open limits

Review the first usable increment: typed cumulative PG progress, native context-bound
classification witness, actual Sync consumer and validated zero-ACK recovery.
Keep current window/deadlines and jobs.checkpoint_revision=0; classified local
positions imply neither remote cursor nor completed acquisition coverage.
The accepted design, independent countercheck and concrete scenarios are pinned in
the linked report. Once source freezes, reconcile named tests with the prepared
builder filters; a nonempty group alone is not complete scenario coverage.
Source implementation is in progress; build/real-engine qualification is NOT_RUN.
Next add synced nativeACK with exact frontier transition and compatible recovery;
actual ACK, not a newer PG head alone, grants credit. Qualify two-slot window,
proof-only progress and reply-loss/restart. NoGC history64/32MiB is a temporary
retention bound, not indefinite live capacity or completion of the ACK/GC gate.

Original a99 initial Sync timeout cause remains unresolved. TLS/other credentials,
worst-case/internal-owner/whole-service capacity, ACK/GC/import/restore and M1–M5
remain open. Q33 CI billing, arm64 and signing/registry are separate gates.
No public release.
