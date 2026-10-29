# Current execution checkpoint

2026-10-29, Tux. **Goal ACTIVE; M1 qualified; complete P00–P10/M1–M5 product incomplete.**
All selected optional requirements and Q37/Q38/Q40/Q41/Q42 remain. No official
release, package publication or production deployment is authorized.

## Git and current source

Public/origin main: **5cc9cbb188378093f97d64f3e51ff11444934b98**. Local main:
**ae9b168a**, including common-wait9ebd, hosted evidence4293 and subsequent
qualification checkpoints b28dc035/ae9b168a. Both documentation increments passed
the three shared documentation checks. Current lifecycle/report/inventory changes
are pending; no reclamation implementation or fixture is integrated on main.
Keep the next push around a coherent qualified capability.

Current frozen product **568a7c473dbda3bdfd0e02415aba32bc42683ab1**:
`.research/closed-reclamation-grant-qualified-20261029`,
`work/closed-reclamation-grant-qualification`.
Current frozen acceptance **18b01362a1af3448b6e17869e2ed249d957fc9e4**:
`.research/closed-reclamation-grant-acceptance-qualified-20261029`,
`work/closed-reclamation-grant-acceptance`.
Author `.research/closed-reclamation-20261029`/`work/closed-reclamation` is clean
at18b. Earlier DA/3641 and4a/42f immutable worktrees/branches remain preserved.
No pending product code is forced onto main.

## What is qualified and what failed

The original M1 and enabled-Index initial/live successor6cbe/1fab qualifications
retain their exact scopes in the [Index report](evidence/p02-initial-index-clickhouse-sql-20261026.json).
Common-wait9ebd passed208 portable controls, critical review and same-product
installed live16; broader P01-T08 coordination obligations remain open.

Current568a **native221/37 PASS** (133 library/88 binary), strict Clippy, CLI/library
build and test compilation PASS. Native receipt
`.research/initial-index-recovery-qualification-20261028/r23-568a7c47-native/receipt.json`,
SHA256 `bef45f0556e778cfff9bc6f5cb5427a45a9daf1c20f08c6a45584efd7b237fb0`.
This exercises actual native mechanics with synthetic SQL authority, not connected
PostgreSQL retirement/reclamation.

Two earlier real installed failures remain preserved under external run roots:

- `closed-reclamation-installed-da5e7dde/acceptance-closed-r1`: lexical SQL ordering
  refused numeric retirement64; seven PASS/one FAIL/three NOT_RUN/cleanup PASS.
  The same DA product's outside-checkout tutorial7 PASS retains its scope.
- `closed-reclamation-installed-4a24024f/acceptance-closed-r1`: numeric retirement64
  and corrupt-original refusal now succeed; actual reclaim child then exits4
  `Native(InsufficientReservation)` before COMMIT, masked by the later proxy event
  timeout. Again seven PASS/one FAIL/three NOT_RUN/cleanup PASS. Root terminal
  receipt `fc3135366fad470c117886ac43c3c3d83250905c3e05767aca37645509d18454`
  verifies28 owned containers stopped,11 PIDs absent and empty borrowed bridge.

Current source fixes the missing128KiB canonical outcome allowance using the
existing consumer helper, preserving process caps and all validation/authority
predicates. Critical source review54360094 closed. Acceptance18b saves unexpected
child diagnostics early, tolerates delayed real fault after expected CommitUnknown,
and preserves the strict110s positive deadline,15s join and full semantic oracles.
Critical diagnostic reviewc9b32b7c closed; owning module40 + profile1 controls PASS.
A stalled existing Docker inspect may delay failure reporting under its30s cap,
never permit late PASS. No new whole-suite total is invented from prior226 at3641.
Exact source, author, review and failure receipts are in the existing Index report;
reuse those scopes rather than repeat unchanged reviews.

## Current installed run and resources

Fresh568a parent build11690 **PASS** (130.30s), build receipt1860d2d9;
fresh observer37406 **PASS** (75.17s), observer receipt4fca9a26. Both receipts live
under `.research/initial-index-recovery-qualification-20261028/installed-568a7c47`
and sibling `observer-568a7c47`. Source2429 files, parent/probe/native toolchain,
worker assets, exact artifact identities and original cache ELF pins verified.
Both containers exited0/PID0/noOOM, launchers1857287/1860495 absent. Cache115
released/reacquired. Candidate assembled once from18b, manifest SHA256
`66a744dab2dee4aaf5ed04b750d646dde6d6288bf191d80913d4ed0c24a95e40`.

**Latest installed11 result: FAIL**, session72575 reaped1: eight PASS/one FAIL/
two NOT_RUN/cleanup PASS. The complete `closed_reclamation` stage is now PASS,
including both real COMMIT-loss conservation checks, floor/receipt/input effects,
restart/reclaim retry and incompatible partial-restore refusal. The next stage
`compacted_successor` fails because actual owner exits4 before activated startup:
`initial Index catch-up refused; preserve retained state: Unavailable`.
Finite/live consumer continuation and final history/export remain NOT_RUN.

Protected evidence:
`/home/tux/bigbrotr-implementation-20260917/runs/closed-reclamation-installed-568a7c47/acceptance-closed-r1`.
Exact successful GC verdict and actual fault receipts are in the owning report;
original failure/result and root-captured successor stderr remain protected.
Independent terminal receiptce4c68d4 verifies42 owned containers stopped/PID0/
noOOM,20 recorded PIDs and5273 command subprocesses absent,64 acceptance pins
unchanged and exact borrowed bridge empty; cleanup_errors[]. Captured startticks
exist only for runner/route, as disclosed in that receipt. No compiler or runtime
is active. Astra performs bounded source/capture Index startup diagnosis only.
All unique data/databases/inputs/logs/failures, .research and worktrees remain.
The [inventory](../research/laboratory-inventory.json) owns resource custody and
links detailed corrections to the report instead of duplicating them.

## Precise next action and remaining scope

**Verified source blocker:** `LiveInitialWriter::require_index_basis` requests a
30s local policy TTL while `PolicyLeaseTtl` accepts at most5s. Every reached
compacted branch fails; error mapping matches saved `Unavailable`, but the first
executed runtime branch remains unobserved. Activation can commit before catch-up;
missing activated startup JSON is not rollback evidence. Diagnosis receipt4fa830cc
is linked in the existing report. P02-T02/T06 Steps4/7 now explicitly own the
qualified GC scope, deadline fix and remaining successor/consumer closure criteria.

**Implementation in progress:** Sol owns authorworktree `work/closed-reclamation`
from18b. Thread the original command policy lease through live Index/Source/PG
operations; retain one compacted-only owner-intersected ActionWindow through all
pre/post/idempotent and COMMIT/ROLLBACK acknowledgement checks. Existing finalhandoff
locallease remains. No fresh deadline, ceiling change or relaxed authority; ordinary
original/finite semantics remain. Add focused actual-carrier nonrenewal/ownerlimit/
postackexpiry/cancel controls using the existing ManualClock where practical.
No compiler, engine or laboratory test is active. Astra clarified this contract and
will independently review the frozen change; do not treat the proposal as a run.

Next freeze source-only X3 plus its controls, obtain bounded critical review and
qualify its exact source before repeating actual64 continuation. Preserve all
current stores/logs/failures and do not reissue commands on the original failed
activation. No new request65 or compacted consumer result follows from GC PASS;
no Ready65, arbitrary-epoch or broad capacity claim.

After the complete installed cycle passes and cleanup is verified, run tutorial7
on the same candidate/source. Then integrate qualified source/fixtures on main,
reconcile existing owners/report, run appropriate shared gates, inspect public
publication inputs and push one coherent result with its automatic hosted run.
Keep pending implementation separate until qualification; never widen bounds/
oracles or rebuild product for verifier-only changes.

[Hosted run40814795307](https://github.com/BigBrotr/bigbrotr-rs/actions/runs/40814795307)
on exact public5cc passed all30 selected checks/all three platforms; exposure audit
found zero new/unclassified/provider alerts. Receipt352e039b and measured timings
are in the [hosted report](evidence/p01-ci-hosted-foundation-20261026.json).
No watcher/dispatch is active. This does not qualify newer private work on GitHub.
Duplicate equal-input C++ compilation remains unproven; checksum-only crate byte
reuse is an unimplemented opportunity, not measured savings. Preserve cold/default/
shipping and platform gates, without a parallel optimization project.

Code202/query-retirement, a99 Sync startup and old topology-r2 HTTP429 uncertainties
remain in the Index report. OPEN>64/sliding GC, arbitrary terminal epochs, scalable
30s readiness, payload GC/Archive all-family fallback/replay leases and full capacity/
recovery stay with P02-T02/T06/T07/T09. Admission cost, repeated decoding/ClickHouse
transport and coordinator maintenance retain P02-T02/T06/T09/P01-T05 owners. Broad
backup/restore/resource and every later selected phase remain open. Coordinator and
operational author are verified Sol/xhigh; bounded critical reviewer Astra/xhigh.
Do not recheck models solely for compaction; do not pause or complete the goal here.
