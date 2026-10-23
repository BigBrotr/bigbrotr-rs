# Current execution checkpoint

Observed 2026-10-23 on Tux. Goal **ACTIVE** under explicit owner resume;
M1–M5 remain incomplete. Root owns integration/runtime. Q37/Q38 apply.

## Current objective and revision

P02-T02 Steps4/5/7: safe receipt/input reclamation for a completely closed producer.
Main source HEAD is `98f80b121b6e8b3501240284fcc5cf6f192a00d0`: corrected3a97312
product plus integrated fixture dc980a53 with mechanical helper rustfmt. Last
verified pushed/equal remote is2c2a1ff. This commit adds only the reviewed fixture ACL correction and evidence/docs;
root owns runtime. No floor writer or GC is exposed.

The [recovery report](evidence/p02-producer-recovery-native-20260920.json), under
`initial_successor_preparation_20261012.repeatable_succession_20261015.
producer_checkpoint_ack_preparation_20261020.
closed_producer_reclamation_preparation_20261023`, owns exact source, receipts,
checks and limits. The [inventory](../research/laboratory-inventory.json) owns
resource and original worktree custody. Earlier qualified ACK results remain
valid and linked in the same report; the qualified installed product remains9cef25f.

## Latest outcome and unresolved verification

**PASS:** exact product/helper98f80b1, external `runs/producer-floor-98f80b1`;
CLI/probe compilation, Clippy and106 tests (7 compacted native,1 exact PG head,
90 affected lineage,8 shared input), all eight literal controls. Driver34357
joined0, all three ELFs hashed/preserved. The corrected COMMIT path compiles;
actual installed compacted transaction remains NOT_RUN.

**FAIL preserved:** `runs/installed-compacted-reader-98f80b1` stopped at its first
positive-abandon, before synthetic floor creation. The ACK origin lacked existing
abandonment permissions: new Control4/PID134 reported permission denied for
acquisition_gaps. Source closure had committed; no gap was created. Driver43345
joined1; the original fixture retains OUTCOME_UNKNOWN/nativeNOT_RUN after its
expected-success assertion, and all partial SQL/files/logs remain preserved.

This commit corrects only isolated fixture setup and its exact catalog allowlist:
existing gap SELECT/INSERT and three function grants already used by qualified
initial-retirement fixtures, plus the three selected reader column reads.
Independent review approved the correction, explicitly recording the prior ACL
review miss. Product and98f80b1 build artifacts are unchanged and reusable.

## Active work and preserved resources

No engine/build remains. Both98f80b1 build containers exited0/PID0/noOOM; failed
installed client exited143 and borrowed PG exited0, bothPID0/noOOM freshly checked.
Outer cleanup/missing/unrelated/unexpected inventories are empty. All raw logs, ELFs, exports,
original failures, ignored `.research`, native/input data and worktrees remain.
No cache or dataset cleanup occurred.

Native author worktree `.research/producer-reclamation-native-20261023` is clean
at584bad3 (including d224a8d); PG author worktree
`.research/producer-reclamation-pg-20261023` is clean at4662b96. Both are integrated. Exact changed blobs match10a5430; redundant author branch
refs were retired and both original worktrees remain clean/detached.
`compacted_reader_review` completed independent3a97312 review with no remaining
product source blocker; receipt is linked in the recovery report.
`compacted_reader_fixture` completed dc980a53 in clean
`.research/compacted-installed-reader-20261023`, branch
`work/compacted-installed-reader`, base10a5430; integrated98f80b1. The independent
reviewer is finalizing this fixture review; no remaining source blocker reported.
Both new agents' actual Astra/xhigh turn_context was verified at creation.

## Precise next step and open boundaries

Run a fresh excluded dfbb8dc derivative with this corrected fixture source,
reusing exact98f80b1 product/helper build. Each clone first executes authentic
abandonment/retirement; only then use the synthetic floor for full-original and
quarantined-original retry plus incompatible-head/closure/pending refusals.
Join the driver and run exact-source raw replay before recording qualification.
Preserve failed98f80b1 and all originals; this demonstrates the reader, not GC.

Adapt remaining successor consumers before implementing deletion. NoGC64/32MiB,
connected ACK error delivery, physical loss, mixed restore, capacity and backup
remain open. Repeated residual-spool scanning is bounded but capacity is unmeasured.
The historical a99 initial Sync timeout cause remains unknown; preserve existing
diagnostics on any natural recurrence, without a separate retry campaign.
Q33 CI billing, arm64 and signing/registry remain separate gates. No whole-step,
release or goal completion is claimed.
