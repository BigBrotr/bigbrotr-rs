# Current execution checkpoint

2026-10-27, Tux. **Safe pause requested by the owner; full product incomplete.**
Preserve the current P00–P10/M1–M5 goal and Q37/Q38/Q40/Q41/Q42. Resume only on
explicit instruction. The goal control is paused after this checkpoint is saved;
its actual session status is authoritative. No release, package or deployment.

## Revision and saved work

Published `origin/main` remains `903dddb5f8fd585a86737db053cb1491a53d5584`.
The ordinary-readiness increment was pushed and its remote identity verified;
publication receipt: `.research/live-readiness-20261026/ordinary-publication.json`.
The incremental public source/history audit through903 found no new credential
or confidential material. Original captures remain protected and ignored.

Canonical checkout is now on `work/short-pin-checkpoint-20261027`, based on local
`f499cc87`; this checkpoint is committed on that branch. Local `main` still points
to f499, three commits ahead of published903. Those commits preserve the short-pin
fixture (`95b6c615`), Linux-only process-control declaration (`ffe4d64a`) and narrow
proxy correction (`f499cc87`, original `7b1c2346`). They are **not qualified by the
installed scenario** and are not being pushed to main. The checkpoint branch
preserves their source/control evidence and explicit failures without discarding
or rewriting local history. Its push is checked after saving this document.

Separate partial work, **not integrated or pushed**:

- `work/initial-index-recovery`, clean worktree
  `.research/initial-index-recovery-20261027`, WIP
  `0c34826fdbaf9a31d4b0d777320f44fece025c81`: finite `index catch-up`, initial
  source proof/read bridge and scoped controls. Final source review and contracts
  incomplete. Format check initially FAIL; formatting then applied successfully,
  without a post-format check. Compilation, Clippy, Rust/engine/installed checks
  NOT_RUN. No claim of a working recovery implementation.
- `work/index-restart-acceptance`, clean worktree
  `.research/index-restart-acceptance-20261027`, WIP
  `bdd582c4fcccd757f5552ae92c550ae653f3f7a6`: only the18-line source-client prefix
  barrier, runtime hook and closed `(revision,cut)` oracle parameterization are
  saved. Restart runtime, recovery attribution oracle and tests remain unwritten;
  all checks NOT_RUN.

All delegates stopped. Their original worktrees/receipts remain preserved; no
build, test, experiment or further implementation was started for this pause.

## Valid capability and latest failure

Qualified product `8256184cd30f49020c6c0a9884e9eaec958ba461` remains unchanged:
147 actual native controls, build and strict Clippy PASS; fresh installed build
PASS. Installed ordinary-readiness r7 with acceptancee644 passed all eight stages:
complete-current proof plus completed operation makes both capabilities Ready;
withholding the exact retained input makes them Unavailable; a narrow read cannot
revive them; exact restoration permits recovery. Original353B signed export and
manifest, all ten Index families/four batches/40 outcomes, initial physical
invalidation and prior Ingest restart/resend evidence remain valid at their scopes.
Detailed evidence stays in the [existing Index report](evidence/p02-initial-index-clickhouse-sql-20261026.json),
`live_readiness.ordinary`, not duplicated here.

The new `p01-first-launch-index-short-pin` fixture has scoped critical source
review and seven pin controls PASS. The corrected proxy has17 socket/admission
controls PASS. **Both actual installed attempts FAIL before the positive barrier
and before arming; actual pin-expiry cancellation is NOT_RUN.**

- r1: `.research/first-launch-index-short-pin-r1-20261027`. Ordinary setup passed;
  first history.read grant catalog work exhausted the inherited aggregate frame
  budget at20002 frames/1545189bytes. App reported native grant catalog barrier
  completion unresolved. Original failure retained.
- r2: `.research/first-launch-index-short-pin-r2-20261027`. The closed proxy now
  streams baseline traffic and frames only a new post-arm selected reader, but
  the17th pre-arm connection exhausted its16-connection lifetime admission:
  frames0/4450882bytes/armedfalse. No fault gate fired. Preserve this failure;
  no connection-limit change or third attempt has been started.

These are fixture admission failures, not successful product cancellation tests.
The source correction preserves original product action/lease/probe deadlines and
selected-reader/byte limits. Before another run, derive the needed baseline routing
or connection admission from actual caller lifetimes and retained traffic; do not
raise a limit merely to obtain PASS. Reuse the825 installed candidate and existing
proxy/oracle controls; a fixture-only correction needs no product rebuild.

## M1 remaining gates

- Actual short-pin expiry under continuing Control renewal; real reader-pin
  release COMMIT before-forward/after-complete uncertainty; old published snapshot,
  current-selection and selected-metadata installed negatives. Genuine unknown
  native termination remains distinct; SIGSTOP does not qualify it.
- Same-store Index recovery/rebinding/catch-up, including pending/new batches,
  unknown query and lost-COMMIT handling. Proposed installed sequence: closed
  client barrier at publishedrev2/cut3, verify positive readiness, pause only owned
  CH, admit through12, require real preparedplan3 plus unplanned4, orderly stop,
  unpause, explicit `ingest recover --close-source`, catch-up and exact retry.
  Preserve old origins and publish pending only after its PG commit. This sequence
  is a design, not executed evidence. Topology0→1/epoch2 remains a separate concrete
  contract/runtime blocker; completeP02 is not an indivisible M1 prerequisite.
- Independent Archive/CH outages, bounded pending/ACK and live pending bytes.
  Archive failure still exits its combined loop; independent Control survival and
  the needed isolated dependency topology remain unqualified.

Later work retains its existing plan owners: smaller-envelope overlap P01-T09
Step5; general valid-event family bounds P02-T05 Step3/T06; nonempty signed tags,
native census, second-event export, durability/GC/capacity and all remaining
selected milestones. Column-TTL negative is NOT_RUN. Originala99 initial Sync
timeout remains UNKNOWN. Terminal readiness still says unqualified; M1 and the
full goal remain incomplete. No FAIL/NOT_RUN/SKIP/interruption becomes PASS.

## CI and resources

[Hosted run40415243196](https://github.com/BigBrotr/bigbrotr-rs/actions/runs/40415243196)
on exact903 remained **in_progress** at2026-10-26T23:20:41Z: all three foundation
platforms running, scopePASS, optional Rocks/documentation SKIPPED. It was left
running remotely. Single local watcher24737 was stopped with SIGINT, reaped130
and its PID positively absent. Terminal source/context verification, keyring/full
selected outcomes and public-output audit remain INCOMPLETE/NOT_RUN. Protected
receipt: `.research/ci-publication-20261026/run-40415243196-exposure/pause-receipt.json`.

Prior hosted run40406281096 retains23PASS/1FAIL/6NOT_RUN: Linux amd64 keyring test
race, ARM64/macOS PASS. Linux FIFO fixture correction682 has all10 local keyring
controls/format/Clippy PASS;903 is its pending hosted confirmation. Dependency
policy and cancellation fixes remain qualified by earlier real runs; historical
billing refusal is not current. The [hosted report](evidence/p01-ci-hosted-foundation-20261026.json)
owns the exact outcomes, timings and previous public audits.

Both short-pin sessions are reaped: r1 session37259 exit1; r2 session54972 exit1.
All19 containers per run are stopped/PID0/noOOM. r1 ordinary app exited4, others0;
r2 all exited0. Each proxy exited3 and is positively absent. Their proxy cleanup
checks remainFAIL despite terminal resources being confirmed stopped. Borrowed
bridge1699209a remains empty and retained. No owned local development/build/test/
watcher process remains; delegates launched no additional engines. Remote CI above
may finish autonomously. Preserve all databases, logs, original failures, .research,
worktrees and825 artifacts. The released115 compiler cache remains optional.

The [existing inventory](../research/laboratory-inventory.json) owns identities
and terminal receipts. Earlier bounded retirement removed only2372 obsolete b533/
a295 compiler intermediates (2388500480 allocated bytes);228 ELF paths,50 outside
pins and6474 other entries were preserved. No additional cleanup during pause.

## First action on explicit resume

Reconcile actual Git/goal state and the remote903 CI outcome once, retaining any
FAIL or missing artifact. Resolve the r2 proxy connection-admission mismatch from
source and preserved evidence, critically check the affected bounds/target routing,
then run the same maintained short-pin case on825. Do not rebuild for fixture-only
changes. Afterwards resume the separate Index recovery/acceptance WIP review and
qualification. Merge/publish only coherent qualified work; do not mistake these
saved branches or source controls for completed installed recovery.
