# Current execution checkpoint

Observed 2026-09-20 (Mac; terminal Tux audit 2026-09-19 UTC). Navigation only;
follow the [resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint).
Verify Git, dirty files, evidence and resource claims before continuing.

## Mandate and current responsibility

Complete owner-authorized implementation remains active. Mac is the primary
editing/Git workspace; root integrates. Trusted exact-source Tux execution
continues under the hosted-runner billing exception. Preserve private bigbrotr-rs,
unpublished Cargo packages and the read-only Go neighbor. No public release or
Linux arm64 claim. No P02/P03 task or M1 gate is complete. All selected later
product work remains required.

P02-T02 step 4 close-only native producer barrier is implemented and has scoped
native evidence. Root is finalizing the qualification commit and private push;
next bounded implementation responsibility is resolving original producer outcomes
through the exclusive historical recovery handle before any ACK/retirement or
fresh Ingest successor. No active lab, local build or reviewer after the terminal observations below.
Final read-only AI review found no factual discrepancy or overclaim; root verified
the final observer result and collected-file hashes again before committing. Do not infer complete drain from issued_through().

## Git and integration state

Mac main HEAD fd168a5 (full hash from Git), four unpublished candidate/harness
commits ahead of last verified private origin/main 442f3d878b3215e421a71160b3613b7ab0c93c7b.
Root owns only final observer/evidence/task/reference/checkpoint/inventory edits.
Product candidate 0f6e366fec4f84c0c3078142bea8c1cb91198f4f,
tree f148d133462d7b5f199aac9e3ad872bb02e2ae83; corrected fixture
cc9151b9bd81512237bbb07b8bde1c446199d95d, observer 01af703 and compatibility harness fd168a5.
No product/SQL/dependency changes after 0f6e366. Published migrations 1..14 unchanged.
No unexpected writer or merge/rebase.

Separate clean compatibility worktree .research/worktrees/producer-closing-old-writer,
branch research/producer-closing-old-writer, commit
f899896b1207f5e71c4e39d9ab7814b293814735. Every 840 baseline 442f3d8 file is identical
except an exact four-line cfg(test) hook; two fixture files added. This historical
fixture must remain separate from current-writer tests. fact_codec_review committed
only there; root reviewed every changed byte and qualified it on Tux. Both main
and this compatibility branch still require the authorized private push after
final checks. Local .research/targets/product is free; all agent Cargo processes
were joined. An initial native-environment build was interrupted and retained,
then normal Mac compile passed; missing-path invocation failed explicitly.

## Implemented boundary and executed evidence

The [closing report](evidence/p02-producer-closing-native-20260920.json) owns exact
source/artifact/workload identities, outcomes, retained failures and limits.
IngestRecovery has no conversion or admission API. One sync batch migrates only
one producer value from 26-byte v1 open to 27-byte v2 closing, preserving counters.
Zero requires no retained target request; nonzero requires the max request key to
match issued. Nonzero ACK/retired is unsupported. This is not an interior scrub.
Fresh staging/dispatch refuse closing, including already verified input.

Current Control validates its fence, full current policy and short local lease,
then the complete immutable historical target under Ingest/role SHARE and producer
UPDATE locks. Native sync precedes PG's scheduling veto. PG already closing never
substitutes for the native barrier. Old Ingest scheduling expiry does not block
monotone closure. Failures after sync preserve closing and require exact recovery;
no negative outcome, source closure, successor activation or ACK is inferred.

Executed Linux amd64 / Rust 1.98.1 / PostgreSQL 16.15 / actual Rocks 11.8.1:
- Ordered main/worker/fixture build and scoped Clippy PASS; all 35 native-store
  tests PASS, zero ignored, 86 filtered. Includes eight new closing tests and the
  actual packaged-worker consumer. Mac initial broad run retained 34 PASS / 1 FAIL
  because the packaged worker was absent and the skip name wrong; Linux ran it.
- Twelve connected cases PASS: normal/exact retry, empty, PG-first, expired old
  Ingest lease/draining, wrong root, wrong historical metadata, policy refusal,
  local expiry, SQL rollback, delayed SQL, and both lost-COMMIT orders. Full native
  values, exact original inputs, all 37 Control tables+xmin checked. Populated
  sequence 29→30, empty 1→2, refused 29→29; retries/recovery never add another
  native mutation. These starting sequences are measured, not a literal oracle.
- Both interrupted COMMIT cases use fixture cc9151b. The first before1 run remains
  INCOMPLETE: Config::port appended a second port during reconnect after real
  commit loss. Source tokio-postgres 0.7.18 confirmed the cause; only fixture fixed.
  The first corrected harness also refused provenance before SQL; exact artifact/
  product checks were then separated. Neither failure was reported PASS.
- Old writer-library fixture f899896 ran on byte-verified copies of actual new-client
  closed stores. All 11 snapshots preserve every native row and sequence; old event
  dispatch returns Limit, both bootstrap paths StateMismatch. Old staging retains
  original 3 (populated) / original 1 (empty), exactly compared with independent CBOR.
  Original qualified directories remain byte-identical. Test callbacks grant no
  PG authority; this is not an unchanged previously shipped binary.
- Independent AI review recalculated all 12 captures and found no product defect.
  Root strengthened two oracle gaps: committed-retry producer xmin and full Control
  recovery rows/exclusion digests. Rechecking captures plus 15 altered controls PASS
  offline; no new engine run. after-complete xmin 1026 / native sequence 30 unchanged.
  Source/fixture exports and all 28 captured originals independently match Mac Git/
  frozen CBOR. All 73 collected files have verified sizes/hashes.

Previous [certification](evidence/p02-certification-native-20260919.json) and
[import checkpoint](evidence/p03-import-checkpoints-native-20260919.json) prerequisites
remain qualified/published. Their wider task gates remain open.

## Evidence and resources

Terminal audit 2026-09-19T22:33:36.297395+00:00 PASS: 378 preexisting container
lifecycles unchanged, eight unrelated running, zero BigBrotr running, zero owned
PG clients, no OOM. New lab:
/home/tux/bigbrotr-implementation-20260917/runs/producer-closing-0f6e366,
owner 11748e8de0d0bb19d4178d5e0efecc47;
runner 692a92a610cf909e8c7901cd63b2b8d48e535290fae217f4963eea2263259aab;
PG 55b54500b996c4d089efc2e013a219d33189c220aef4036463fb30a3b477137f;
empty internal retained network 5aecbf2df459dbf06a740a79770051e8015f06a6770ad1192ff2ba8e12ae5a01.
Containers/data stopped and retained. Its out/build is the latest exact-lock
cache; compiled target artifacts are caches, never substituted for fresh source
identity checks. See producer_closing in the [inventory](../research/laboratory-inventory.json).
Runner 2 CPU / 6 GiB / 512 PIDs, PG 1 CPU / 1 GiB / 128 PIDs; no host ports. Not capacity evidence.
The prior certification network 3d454be2 was retired only after fresh ownership/
empty/stopped checks; its data/containers remain stopped. No global prune.

Mac .research/implementation/producer-closing/native/qualified holds 73 verified
files; evidence.tar SHA256
f4439e720fa4c7bb2d1db6e64fadf6c26fd2e92e5fee5dd8adcc6518a5f2c2b2.
Four complete exports match Git: product 846 files, corrected fixture 847, old
fixture 842 and old baseline 840. Main SHA256 f0bc9b88546e9571c42a688e6ca777ba0f9b48b6701fbf57cca9f1506b7b5abb;
corrected probe b4cb5e1823a28935f4c97af3cc952e814b69c5982a16fdd0ea6fb3e1ff3bc161;
old test c3838be980d792f6a4839a11735c590367a4de850efccf60f54ab0fdbf4ddb6a.
Ignored launch/transfer/build/audit/collect helpers need path/owner review before
reuse. Review captures and offline observer receipt live under the same root's
review directory. No active session/process obligation remains.

## Next action and remaining gates

Finish final report/diff/structural review, commit and push the authorized private
main and compatibility branch, verify remote hashes, then select the original-
request resolution consumer for P02-T02 step 4. Read retained-input/outcome-copy,
producer checkpoint/ACK, storage-services and recovery authority contracts before
adding methods to IngestRecovery. Closing cannot stand in for durable outcome
resolution or invent negative results for missing receipts/reservations. Fresh
processes cannot impersonate old incarnations/candidate nonces.

Current close-only evidence does not include fresh-process/held-old-LOCK recovery,
in-FFI or Control-backend kill, actual policy-head advancement, post-sync async
cancellation, ENOSPC/power loss, aged mixed workloads, post-GC/archive fallback or
causally closed restore. M1 still needs actual read/standalone export/restart.
Complete proof/claim checkpoints, drain/ACK/GC/abandonment, producer retirement,
source closure, successor/topology CAS and all selected acquisition/analytics/
read/UI/website/Compose/release work remain required. Tux amd64 does not qualify
Linux arm64 or hosted Actions.

External monitor findings remain scoped closed with practical evidence:
[MON-001/002/004/006](evidence/p01-monitor-corrections-native-20260918.json),
[MON-005](evidence/p01-jsonb-limit-native-20260918.json),
[MON-009](evidence/p02-job-bounds-native-20260918.json),
[MON-010](evidence/p01-tls-presence-20260918.json),
[MON-007](evidence/p01-archive-cache-retry-20260918.json).
No new contrary evidence in the requested external review note.
