# Current execution checkpoint

Observed 2026-09-20 (Mac; terminal Tux audit 2026-09-20T00:12:40Z).
Navigation only: follow the [resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint)
and verify Git, dirty files, evidence and actual resource state.

## Mandate and current responsibility

Complete owner-authorized implementation remains active. Root integrates on Mac;
Tux executes trusted exact-Git Linux work under the hosted-runner billing exception.
Preserve private bigbrotr-rs, unpublished Cargo packages and the read-only Go
neighbor. No public release or Linux arm64 claim. No P02/P03 task or M1 gate is
complete. All selected later product work remains required.

P02-T02 step 4 close-only producer barrier is qualified and published. Positive
original-request recovery is implemented and undergoing qualification: read the
exact retained input, validate the original native outcome and dependencies, then
copy it under current Control. Never admit, stage, reopen the producer, ACK or
infer absence from missing detail. Complete drain and reserved-but-uncommitted
requests remain unresolved. Reuse native replay validators and migration0012;
no SQL migration was needed for positive copying. See the owning
[producer input reference](../reference/producer-request-inputs.md).

The process fixture is now built and executed at 05fb02d: held1 PASS. It holds the
actual old native writer after signed sync/failed SQL copy, kills its exact PG
backend, checks a second process cannot open the still-held native LOCK, then
SIGKILLs/reaps the old child and recovers under a third process/current Control2.
The native directory is never copied. This is not mid-FFI or power-loss evidence.

## Git and integration state

Mac main HEAD efeb4e0fc5254b856746d12d12c28cb929371080 is unpublished. Product
candidate d2959da61a54a89b6b4e1b4806aa3a819bdff778, tree
45d8c742d032641b9579bfc3ae9bc0e2ef80ffa0, remains byte-identical in library,
dependencies and migrations1..14. Fixture3135497 added finite fault cases;
observer2580fb2 fixed checks; observer3497cf5 injected manifest corruption after
ordinary guards. Last read-only private origin/main verification:
8931a0f611b718cfac845cda39211e9e8da59d8c. No merge/rebase or unexpected writer.

The process fixture and connected Rejected/AlreadyDurable fixture are committed.
Root now owns finite missing-detail, unreserved-request3 and conflicting-copy
fixture/observer plus this checkpoint/inventory. Product/dependencies unchanged.
Mac scoped fixture Clippy PASS;16 earlier captured cases still pass the updated
observer. The three new refusal engine cases are NOT_RUN. No unexpected writer.
An initial outcome-class fixture compile refused a u32/u64 mismatch; explicit
widening corrected it before any native run.

The separate compatibility worktree .research/worktrees/producer-closing-old-writer
is clean at f899896b1207f5e71c4e39d9ab7814b293814735, branch
research/producer-closing-old-writer, published privately. Do not merge that old
fixture. Local .research/targets/product is the owned cache, not source authority.

## Product and native baseline evidence

Ignored Mac root .research/implementation/producer-recovery retains REDs and
actual checks. Native11 recovery tests PASS; full Mac store45 PASS with actual
packaged-worker case explicitly excluded. Linux ran all46 native-store tests,
including that worker, with zero failed/ignored. Tux ordered main/worker/examples
build and scoped Clippy PASS at d2959da. No broader gate follows from those counts.

Independent AI review found two actual native defects before the candidate:
- A valid AlreadyDurable receipt with unrelated dictionary dependencies passed
  structural checking. Shared receipt_effects now checks original event effects
  and complete bounded dictionary closure; the discriminating RED is retained.
- An independently signed 2MiB payload admitted successfully but a same-request
  retry with a smaller verification limit failed because its encoded request is
  2MiB+78. The fallback decoder now bounds the owned request (capped16MiB), retaining
  the fact limit and allocation grant. large-replay-red3.log is the real native
  RED; red1 was a fixture string limit and red2 a test compile error.
Historical rejection policy must remain original: a proposed equality to the
manifest policy broke the existing full-width-u64 counterexample and was removed.

Native recovery is positive-only, requires native closing, zero ACK/retired,
original owner/producer1, exact input digest/canonical bytes/encoded length and
supported receipt context. Missing detail is Unresolved, never noncommit. Shared
PG preflight retains current Control authority and Ingest/role SHARE + producer
UPDATE locks. New copy attributes current recorder; identical copies retain the
original complete row/xmin. Native state and all progress remain unchanged.

Baseline normal1 passed actual PG/native copy rollback → Control2 → close →
resolve[2,2,1]. Full26native rows,37Control tables,2originals; sequence29→30 for
closing then unchanged. Only outcome2 inserted. This was same-process reconnect.
Baseline15file collection in native/baseline is size/hash verified (independent
review too); tar SHA f80d7e39f71a79a1dfd6db275d35e53a9e3de67fd727feaf15fd0b35986d3cb8.
All859 source files match Mac Git d2959da. Main SHA256
8d3e6ab6a0f454423ac522cc07a9dab5204581e9a7f31201f524a673ec057c0b;
worker 4a8f8f37ec6dbf7b6e6def033966d21d116f027699cfd4c96ae6ff852f298f3c;
baseline probe 3ccc6f556a9a933402a097c8bfe13e98ffb6a4be669efbd82918b03fee0affc5.
Supplemental seed-fault-pg.log/identity correlate the original backend166 with the
actual AFTER INSERT RAISE; the old generic Backend assertion alone did not do so.

## Extended fault qualification and review

Fixture313549724b1fce2926b9cdbd4cf32acf824c784e built separately in
out/recovery-probe, asserting all product/dependency bytes equal d2959da. Build
and Clippy PASS; artifact SHA256
9efd7d0d5293858b7aab7bd2de69fc3a3c619b9feb0c69b2d64cadbddb203d71.
Its complete860file source manifest independently matches Mac Git/archive.

Thirteen actual cases PASS: normal2, pgonly1, size1, digest2, manif2, res1,
cancel1, policy1, expired1, roll1, late1, before1, after1. Each compares all26native
rows,37Control tables+xmin,2canonical inputs and zero remaining case backends.
PG-only refuses with native sequence29 unchanged; others close29→30 and never
mutate native state during resolution. Refusals distinguish ProducerOpen,
Conflict, DigestMismatch, InputLimit, authority, policy, expiry and SQL failures.
Cancellation is initial cancellation, not later in-flight cancellation. Resource
case is a reduced decoder bound, not exhausted ResourceBudget/RSS evidence.

COMMIT4 before-forward: server3/client3, original2 absent, fresh Control3 copies it
(xmin1110). After-complete: server4/client3, recorder2/xmin1130 already present
before reconnect and identical through fresh Control3 and all three retries.
Both actual connection drivers joined. Old bootstrap recorder1/xmin and Ingest
owner stay exact. No source/checkpoint/ACK/GC progress is inferred.

Preserved failed runs:
- Ignored initial fixture-transfer helper SyntaxError before execution; corrected
  helper/build completed. No engine work occurred in that failed transfer.
- resource1 invalid nine-character tag refused before setup; res1 is the valid run.
- digest1 INCOMPLETE observer: real refusal/client exit0/zero backends, but PG printed
  guard_producer_outcome() without schema. Observer corrected; digest2 ran afresh.
- manif1 INCOMPLETE intended recovery setup: migration0011 correctly refused a
  changed request2 manifest before native admission;16bootstrap rows/request1 remain,
  client exit101. manif2 intentionally corrupts request2 AFTER BEFORE guards via
  privileged lab trigger. All production guards remain enabled/unchanged; actual
  trigger order is recorded. This is defensive corrupt-data evidence, not a legal
  producer write. Actual outcome guard still refuses; resolver returns Conflict.

Independent AI review found/fixed two observer gaps: missing exact historical
Ingest owner comparison after a third Control claim, and snapshot JSONB/xmin order
versus numeric request ordering. The prior checker accepts an altered Ingest lease;
the corrected checker rejects it. All13 captured results now pass the corrected
observer, with131 altered controls refused and reordered-row positive controls.
This recheck is offline, not rerun engine evidence. res1 actually ran observer
3135497; do not relabel it as originally run with2580fb2. Independent review also
recomputed before1/after1/res1/manif2, actual originals and all55 captured hashes.
No further product defect found in that review. after1 uses259763 of the old
262144byte stdout limit; the new process fixture explicitly allows512KiB.

Mac native/faults-collected contains55 verified files; detached
native/faults-evidence-identity.json and tar SHA256
d8486368fef38e59d5acf6bd1cf27913b9737b96ad14ecfb2e49c7a19c0bb652.
Includes failed reports, exact originals, fixture build and idle audit. Offline
checks are in native/faults-review (receipt.json); prior four controls and the
Ingest regression are in native/faults. This is not the final evidence report.

## Owned laboratory and process obligations

Tux lab /home/tux/bigbrotr-implementation-20260917/runs/producer-recovery-d2959da,
owner f7d0467948f3bf4483742da12de9597e; runner
9a4699be5fa84f00bcd08ebec900b06d78cd6203c3d9726f64a40541a058ace5;
PG f49ef729184ce2a756173c42d1379df31b84e1fe3b1869968c82bb35c7b4afa9;
internal network803f48ecda42e8ec11f83afb9094d36e9355a356e6daeb59606ed649f6367be1.
Runner2CPU/6GiB/512PIDs, PG1CPU/1GiB/128PIDs; no host ports, socket or release
secrets; pinned images in native/resources.json and the laboratory inventory.
Actual Linuxamd64, Rust1.98.1, PG16.15, Rocks11.8.1. Noarm64 evidence.

Fresh preflight found380existing containers,8unrelated running,0BigBrotr and
97,201,893,376 free bytes. Launch started23:23:42Z. Terminal audit at
2026-09-20T00:12:40.081561+00:00 PASS:0ownedPGclients, runner only idle init/python
before stop; both owned containers stopped/noOOM, PostgreSQL exit0. All380prior
container identities/lifecycles/mounts/networks unchanged,8unrelated running,
0BigBrotr running. Empty internal network and all data/artifacts retained.
Local SSH/build/test sessions are joined; independent AI review completed.
No active case or cleanup obligation remains for this stopped lab. Never restart
its original entrypoint, which rebuilds into occupied output; a new required native
run needs separately inventoried exact-source resources/cache transfer.

held1 actually observed PID12183/start324515137/device259:2/inode25841912 holding
the same POSIX WRITE lock three times. Backend1374/start00:08:17.24955Z was killed
by exact PID/start and observed absent. PID12233 failed native open without state
changes. Old child SIGKILL/-9 was positively reaped; PID12332 then claimed Control2,
closed and recovered[2,2,1]. Bootstrap xmin1191/event1204 survive exact retries.
Native26rows,37Control tables and2inputs match. Independent AI review rechecked
all fields, SQL query hashes/results, kernel/process identities and old/new Control
transition. No mid-FFI, power loss or successor admission claim.

Mac native/collected contains80 size/hash-verified files; detached evidence-identity.json
and tar SHA f13285843fbf722c8111db59f0fb865dc2b600aac9a80fa51be8508c93e0ef3c.
It includes source/build manifests, original bytes, all results, failed audit.json,
and successful audit-terminal.json. The initial terminal helper glob also matched
a harness Git bundle as a directory and stopped before resource mutation; corrected
filter completed the audit and stop. audit.py/audit-corrected.py and both logs remain.
The863file process export was independently compared with Mac Git archive05fb02d.

Old closing containers/data remain stopped; only its exact empty owned network
5aecbf2d was retired by the recovery launch after fresh label/endpoint checks.
No global cleanup. See producer_recovery in the [inventory](../research/laboratory-inventory.json).

## Current connected class evidence and active lab

Fixture efeb4e0 is built and Clippy checked on actual Linuxamd64. Product bytes
remain d2959da; artifact8035de8de63d44e7fce602e9e0e7a5956550f78660999c82775bcce6469c5825.
Actual reject1,already1,normal1 PASS:17/27/26native rows,37tables each,2/3/2exact
original inputs and zero clients after each case. Rejected keeps Signature/code4
and no new scientific event facts; AlreadyDurable recovers[3,3,2,1], preserves
original copies/xmin779/781 and creates only copy3/xmin787 under Control2.
Rejected recovers[2,2,1], preserves bootstrapxmin759, inserts rejectionxmin765.
Native sequences19→20,31→32,29→30 advance only for closing; stable across recovery.
These are new Control in the same process; held1 remains separate fresh-process
NewFacts evidence. Independent AI read-only review checked both classes/all37tables,
SQL seed/backend identities, original bytes and all22 captured file hashes.

Mac ignored outcomes/collected beneath .research/implementation/producer-recovery
contains22verified files; detached outcomes-evidence-identity.json, tarSHA
 e989ae67b0d419cfc4beef9993addec128f52529f58cac3a3565c05def910e45.
All864source files independently match Git efeb4e0. Offline new class controls
refused14+13alterations, ordinary10; prior13cases/131controls still pass. These are
offline checks, not engine reruns. First local extraction helper refused unsupported
Python tar filter before extracting; explicit regular-file/path/allowlist checking
completed extraction. No product failure inferred.

New Tux lab /home/tux/bigbrotr-implementation-20260917/runs/producer-recovery-outcomes-efeb4e0
owner a67872467ee5c98ff518edf34bdb9182; runner
c4a8f9fe7466655632e16a67201efd9abedeae6d236a76505a4707a9c287ec9d;
PG4f091c5fbd6bc6bdfc56eebbeac157244400e0c043dd22333bda2dc078554d44;
network6d34130343922a501d5e9cfe74c52aeff9116601f69edfac49a5e41325fac4d8.
Same pinned images/limits as prior lab. Exact Git export/source readonly; baseline
out/build cache, native-tests and archive-worker copied into new owned output.
Previous data/containers retained; only verified empty prior network803f48ec retired.
Current audit-outcomes.json confirms idle runner,0PGclients; containers remain
running for the three refusal cases. No terminal audit yet. Root owns joining new
build/case sessions and stopping these exact resources after qualification.

## Next action and remaining gates

Commit/export/build the bounded refusal fixture in the current idle lab's separate
out/recovery-probe-refusals directory. Keep both earlier artifact identities intact.
Run missing native receipt2 after deliberate exclusive synchronized deletion,
unreserved request3 (no deleted PG metadata), and an existing SQL copy whose bytes
are deliberately corrupted after ordinary guards with a matching checksum.
Compare the injected change explicitly, require Unresolved/Conflict and exact
remaining state; never translate missing data into a noncommit or new request.
Then finish the scoped positive-recovery evidence report/review/private publication.
No P02/M1 checkbox advances yet. Full drain, ACK/retirement, source closure,
successor authority, archive fallback and causally closed restore remain required.

Published prior [closing](evidence/p02-producer-closing-native-20260920.json),
[certification](evidence/p02-certification-native-20260919.json) and
[import checkpoint](evidence/p03-import-checkpoints-native-20260919.json) reports
remain prerequisites, not completed task gates. Mid-FFI/backend kill, actual policy
advancement, asynchronous post-sync cancellation, ENOSPC/power loss, aged mixed
workloads, restore and native arm64 remain unqualified. M1 needs actual read,
standalone export and restart; proof checkpoints, drain/ACK/GC/abandonment,
retirement/source closure/topologyCAS and all selected acquisition/analytics/
read/UI/website/Compose/release work remain required.

External monitor findings remain scoped closed with practical evidence:
[MON001/002/004/006](evidence/p01-monitor-corrections-native-20260918.json),
[MON005](evidence/p01-jsonb-limit-native-20260918.json),
[MON009](evidence/p02-job-bounds-native-20260918.json),
[MON010](evidence/p01-tls-presence-20260918.json),
[MON007](evidence/p01-archive-cache-retry-20260918.json).
No new contrary monitor evidence.
