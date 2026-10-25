# Current execution checkpoint

Observed 2026-10-25 on Tux. Goal **ACTIVE** after explicit owner resume.
M1–M5 remain incomplete. Q37/Q38 apply; root owns runtime and integration.

## Objective and revision

P02-T02 Steps4/5/7: closed-producer reclamation prerequisites, including later
compacted consumers. Main HEAD `61fcd1a0329d007ac6d41c72af16b514d834384e`
was pushed and clean before this documentation update. No merge/rebase is pending.
Qualified ordinary fixture `c3bec54c`, compacted later-empty fixture `28cd644d`
and product `2bdf513c` are integrated. New live product and input-proof fixture
remain in separate worktrees until their required qualification succeeds.

The [recovery report](evidence/p02-producer-recovery-native-20260920.json), at
`initial_successor_preparation_20261012.repeatable_succession_20261015.
producer_checkpoint_ack_preparation_20261020.
closed_producer_reclamation_preparation_20261023.
compacted_successor_preparation.later_empty_consumer_work`, owns exact source,
review and outcome receipts. The [inventory](../research/laboratory-inventory.json)
owns worktrees, retained runs and resources. This checkpoint links their evidence.

## Verified work and open findings

**PASS, reused:** product2bdf build196 components/87 mandatory controls and Clippy;
28cd installed4copies/8stages,121controls,5935raw file bindings. Ordinaryc3bec54
retirement/retry:1copy/2commands,30controls,787raw bindings, full75-table state.
These results and their original inputs/ELFs remain preserved.

**Input/proof fixture736cbd52:** source review and synthetic diagnostics passed.
Actual driver64329 joined1: first6cases PASS, including its own installed held
COMMIT6 baseline, optional-input absence, required-input refusals and preparation
H drift. The seventh case changed canonicalD and hit the earlier SQL closed-source
commitment guard, returning `Control(Backend)` rather than the expected Rust
`StateMismatch`. Native sequence stayed134; process/backend custody was closed.
Final2cases and full raw replay NOT_RUN. Original failed run is immutable:
`/home/tux/bigbrotr-implementation-20260917/runs/installed-compacted-later-input-proof-736cbd5/failed-run.json`.
The author is binding the exact SQL refusal to backend/time/log evidence and
naming that boundary explicitly; the narrower post-SQL Rust D recheck stays open.

**Live source63a8fbf5:** critical review completed, but build95024 joined1 with
E0599 at `initial_writer.rs`: a historical caller still used a removed accessor.
This missed caller and original review/failure are retained. First build checkFAIL;
11checks NOT_RUN. Correction `2622fc82ba79b7e20c0ba039a68f84b9b999ea5b`
preserves same-owner historical identity with `matches_recovery`, rejects live
views, and has independent correction review. Its exact build is running below.
Static fixture reconciliation also found two new-mode input mismatches: epoch1:3
is dictionary input, while the Event loader needs the retained epoch1:5 Event;
the selected new Event already exists in the modern seed. Author and reviewer
are correcting these probe coordinates against literal original bytes before
connected runtime. Ordinary modes and product library behavior stay unchanged.

Earlier c843/887/f645 fixture failures, fb4/c1 build failures, preparer83 unknown
outcome and original a99 Sync timeout remain in the report. None is reclassified.

## Concurrent source and resources

- `/root/empty_malformed_floor`: input-proof correction in
  `.research/compacted-later-input-proof-20261025`, branch
  `work/compacted-later-input-proof`; frozen736 and failed captures preserved.
- `/root/compacted_reader_fixture`: original live worktree
  `.research/compacted-later-live-20261025` clean at2622fc82; now authors the finite
  and installed live fixture in `.research/compacted-later-live-fixture-20261025`,
  branch `work/compacted-later-live-fixture`, based on2622fc82.
  `/root/compacted_reader_review` independently reviews its expected outcomes.
  These agents run no builds, engines or SQL.
- Root build **2308 RUNNING**, exact2622fc82, under
  `/home/tux/bigbrotr-implementation-20260917/runs/producer-floor-2622fc8`.
  Join this existing handle; do not start a duplicate build. Component/Clippy
  outcomes remain NOT_RUN until observed. Existing exclusive-cache runner owns
  teardown. Free bytes before build:248828710912.
- Failed736 clientc287cb43 and borrowedPG71775507 were freshly observed exited,
  PID0/noOOM; both origins retain all5preservation facts. Failed63 build containers
  are also stopped with no unrelated changes. All logs, .research, worktrees,
  databases, datasets and original failures remain. No cleanup was performed.

## Next action

Join build2308 and inspect actual results; correct only observed failures. Review
and freeze the input-proof correction, then run its exact source with unchanged
qualified2bdf artifacts after the build releases laboratory custody. Finish the
independent live fixture before qualifying product2622 with real engines.
Record coherent outcomes in the existing report, integrate ready work on main
and push. Retire only verified redundant refs while preserving their worktrees
and evidence; ordinary fixture branch retirement remains pending.

Later live/Sync and fault qualification, fresh activated-later recovery, Archive,
physical64-file/32MiB limits under live load, mixed restore, backup/capacity and
M1–M5 remain incomplete. No product floor writer/unlink is qualified. Original
a99 Sync timeout cause remains unknown; Q33 CI billing, arm64 and signing/registry
remain separate gates.
