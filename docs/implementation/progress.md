# Current execution checkpoint

2026-10-28, Tux. **Safe pause requested by the owner; full product incomplete.**
Preserve the current P00–P10/M1–M5 goal and Q37/Q38/Q40/Q41/Q42. Resume only on
explicit instruction. The goal control is paused after this checkpoint is saved;
its actual session status is authoritative. No release, package or deployment.

## Revision and saved work

Published `origin/main` remains `903dddb5f8fd585a86737db053cb1491a53d5584`.
The ordinary-readiness increment was pushed and its remote identity verified;
publication receipt: `.research/live-readiness-20261026/ordinary-publication.json`.
The incremental public source/history audit through903 found no new credential
or confidential material. Original captures remain protected and ignored.

Canonical checkout remains on `work/short-pin-checkpoint-20261027`, parent
`1d66779c4cc17e04a28a67829dca9ab6f66dc3f9`; this documentation checkpoint is
committed and pushed on that branch. Its resulting hash is the branch HEAD, not
a guessed self-reference. Local `main` remains `f499cc87`, three commits ahead of
published903: short-pin fixture95b6c615, Linux-only control declarationffe4d64a,
and opaque-baseline proxyf499cc87. Their installed qualification is incomplete;
no new product or fixture implementation is integrated into main during this pause.

Separate partial work, **saved locally, not integrated or pushed**:

| Branch / retained worktree | Saved revision | Valid evidence and remaining qualification |
|---|---|---|
| `work/short-pin-admission-wip-20261028` (Git branch; canonical checkout returned to checkpoint branch) | `bc2b1fbe7c5b9a227083712e71718bef7a6c5440` | Five-file active-connection admission correction;19 socket and7 pin controls PASS. Critical review found a cancellation cleanup race. Correction and installed retry NOT_RUN. |
| `work/initial-index-recovery` / `.research/initial-index-recovery-20261027` | `2d073ae0d785464f1ad98c1549c5b837121821b2` | Finite `index catch-up`, source proof/read bridge, native/cursor controls and contracts saved atop0c34826f. Format/diff PASS; compilation, Clippy, native/engine/installed tests NOT_RUN. Root final source review incomplete. |
| `work/index-restart-acceptance` / `.research/index-restart-acceptance-20261027` | `cb3d8490c5fa63a7f7823b8fe579d1bef1a9e712` | Restart runtime/oracle and controls saved atopbdd582c4.76 Python controls PASS before the final two deadline/scalar edits; post-edit checks NOT_RUN and final review incomplete. Builds/engine/installed qualification NOT_RUN. |
| `work/ci-export-fixture` / `.research/ci-export-fixture-20261028` | `33388863a1f4116f77d3f0c644eb36457a337f40` | Process-spawning and direct-custody fixtures separated into integration binaries; all13 original test bodies unchanged. Format/diff/automatic target discovery PASS; compilation, targeted Clippy, native tests and hosted confirmation NOT_RUN. |

All three retained delegate worktrees are clean and delegates stopped. No build,
engine campaign or implementation was started to obtain a perfect pause. The
inventory's `implementation_index_foundation_20261026.pause_20261028` owns resource
identities; protected receipts stay beside their original captures.

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
review and seven pin controls PASS. **Both installed attempts still FAIL before
the positive barrier and before arming; actual pin-expiry cancellation is NOT_RUN.**

- r1: `.research/first-launch-index-short-pin-r1-20261027`. Ordinary setup passed;
  grant catalog work exhausted the inherited aggregate frame budget at20002
  frames/1545189bytes. Original app error and cleanupFAIL preserved.
- r2: `.research/first-launch-index-short-pin-r2-20261027`. Opaque baseline routing
  avoided frame exhaustion, but the17th pre-arm connection exhausted the16 lifetime
  admissions: frames0/4450882bytes/armedfalse. Original failure preserved.

The saved bc2b1fbe correction instead bounds actual active connections at16 for
this closed fixture profile, retaining aggregate64MiB/180s and selected-reader
limits. It retires closed sockets/tasks;19 socket controls and7 pin controls PASS.
However, independent review reproduced cancellation during `wait_closed` skipping
the active-count decrement; source review also found possible masking of an
already-raised error. **This fixture defect is unresolved. No third installed
attempt ran.** Transports remain retained for outer cleanup; this is not evidence
of successful product cancellation or a product custody leak.

Keep `.research/short-pin-connection-admission-20261028`: the two control logs,
`cancellation-cleanup-reproducer.py`, `cancellation-cleanup-prior-observed.txt` and
`pause-state.json`. The observation was copied from the original completed tool
response, not a new execution or a raw shell transcript. The masking case is a
source finding, not an exercised result. Before retry, make transport closure and
accounting cancellation-resistant while preserving any earlier failure; critically
review and test that boundary. Reuse the qualified825 installed candidate: a
fixture-only change does not require a product rebuild.

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
on exact903 is **completed / FAIL**, terminal2026-10-26T23:44:01Z and freshly checked
at this pause: **23PASS/1FAIL/6NOT_RUN**. ARM64/macOS selected suites PASS; AMD64
fails during export destination setup with `Busy`. Source suggests subprocess
inheritance of a sibling fixture's directory lock; the hosted interleaving is
untraced and no product defect is established. All ten keyring controls pass on
all three platforms. No billing refusal, workflow admission error or cancellation
caused this run's failure. Dependency policy and earlier cancellation fixes retain
their scoped evidence; all-required hosted success after current changes is open.

The [hosted report](evidence/p01-ci-hosted-foundation-20261026.json) now records the
terminal outcomes, exact source/context verification, artifacts and scoped public
output audit (no new/unclassified or operational credential candidates). Protected
root: `.research/ci-publication-20261026/run-40415243196-exposure`; its
`fixture-review.json` records33388863 and the four integration targets still to
qualify. Original failures, reports and audit limits remain preserved. No queued
or in-progress Actions runs were present; no new workflow was dispatched.

Both short-pin sessions are reaped: r1 session37259 exit1; r2 session54972 exit1.
All19 containers per run are stopped/PID0/noOOM. r1 ordinary app exited4, others0;
r2 all exited0. Each proxy exited3 and is positively absent. Their proxy cleanup
checks remainFAIL despite terminal resources being confirmed stopped. Borrowed
bridge1699209a remains empty and retained. No owned local development/build/test/
watcher process remains; delegates launched no additional engines. Remote CI above
is terminal. Current local controls/reviews have also completed. Preserve all
databases, logs, original failures, .research, worktrees and825 artifacts. The released115 compiler cache remains optional.

The [existing inventory](../research/laboratory-inventory.json) owns identities
and terminal receipts. Earlier bounded retirement removed only2372 obsolete b533/
a295 compiler intermediates (2388500480 allocated bytes);228 ELF paths,50 outside
pins and6474 other entries were preserved. No additional cleanup during pause.

## First action on explicit resume

Reconcile this checkpoint and actual Git/goal state, then restore
`work/short-pin-admission-wip-20261028`. Fix the retained cancellation-during-close
reproducer without losing prior exceptions; review the affected cleanup/target
bounds and run only the impacted controls before the maintained short-pin case on
825. Candidate manifest:
`/home/tux/bigbrotr-implementation-20260917/runs/initial-index-installed-8256184/candidate/candidate-manifest.json`.
Do not rebuild the product for this fixture-only change.

Then qualify the four saved export integration targets and obtain hosted
confirmation on a coherent qualified integration. Resume final Index recovery and
restart-oracle review, post-edit Python controls, exact-source native and actual
engine/installed qualification. None of these saved branches establishes completed
recovery, M1 or the full goal. Resume only after the owner's explicit instruction.
