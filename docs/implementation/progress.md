# Current execution checkpoint

Observed 2026-10-20 on Tux. Goal **ACTIVE**; M1–M5 incomplete. Existing plan,
Q37 models and Q38 evidence reuse apply. Sole integration/runtime owner: root.

## Objective and revision

P02-T02 Steps4/5/7: design one durable producer checkpoint → synchronized owner
ACK → restart path, retaining all request/receipt detail initially (noGC).
This documentation commit follows main `855ea20c8091d2a9afefbfea510cded51f295469`
(last exact verified remote; push46245 joined0). No new product changes.
The max65536-byte plaintext manager case at frozen `b364611` is now qualified:
source/integration reviews,27 offline controls, actual runtime, root custody and
critical outcome review PASS within the stated retained f1e/214 sample scope.

The [recovery report](evidence/p02-producer-recovery-native-20260920.json),
`initial_successor_preparation_20261012.repeatable_succession_20261015`, owns
`producer_checkpoint_ack_preparation_20261020` and the completed
`archive_export_memory_source.manager_observation_20261020.credential_boundary_20261020`.
Current export mapping is P08-T04 Step3; prior P08-T05 label was corrected without
rewriting historical records. P08-T05 remains the separate backup/restore task.

## Processes and resources

Runtime77164 joined0. All8owned containers stopped/PID0/noOOM, cgroups absent,
no active root holder; unrelated resources unchanged. No runtime/build/cache/
cleanup active. Source worktree/lab paths and custody are in the [inventory](../research/laboratory-inventory.json).
The integrated credential ref is retired; worktree detached atb364611 and all
ignored/external artifacts retained. Its author and critical reviewer are done.
Astra/xhigh export_setup_fixture is reading the checkpoint/ACK architecture;
source-only, no writable implementation or runtime delegation.

## Next action and open limits

Complete and review the bounded checkpoint/ACK design against existing schema,
producer classified-progress authority and all native closing/recovery consumers.
Root has read complete PGproducer_outcomes, rocksproducer_closing/recovery and
ACK contracts; detailed read boundary lives in the linked report. Current code
rejects nonzeroACK state, so a lone ACK writer cannot be integrated safely.
PG outcome copies or archive progress are not producer checkpoint authority.
Then implement and qualify the two-slot window/reply-loss/restart path. Keep
reclamation disabled until its causal closure and recovery are implemented.

Original a99 initial Sync timeout cause remains unresolved. TLS/other credentials,
worst-case/internal-owner/whole-service capacity, ACK/GC/import/restore and M1–M5
remain open. Q33 CI billing, arm64 and signing/registry are separate gates.
No public release.
