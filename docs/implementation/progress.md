# Current execution checkpoint

Observed 2026-09-19. Navigation only; follow the
[resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint).
Verify Git, dirty files, evidence and resource claims before continuing.

## Mandate and current responsibility

Complete owner-authorized implementation remains active. Mac is the sole editing/
Git workspace; root integrates. Trusted exact-source Tux execution continues under
the hosted-runner billing exception. Preserve private bigbrotr-rs, unpublished
Cargo packages and the read-only Go neighbor. No public release or Linux arm64
claim. No P02/P03 task or M1 gate is complete. All selected later product work
remains required.

Active P03-T10 step 4 / P02-T02 step 4: qualify and document finite event-only
logical record closure from the private native witness in the SAME PG transaction
as its complete outcome copy, plus a per-job/shard contiguous head. See the
[owning reference](../reference/import-input-bindings.md). Full installed import,
claim/proof variants, ACK/GC, recovery and capacity remain required.

## Git and integration state

Mac main HEAD ba53ae9733cff82582f8be9c485e7bcb7526922f, eight unpublished commits
ahead of private origin/main last verified ddfa74064d840541e58208195b8e1612733c2ca3.
No other writer, merge/rebase or unexpected files. Root owns dirty progress and
laboratory-inventory files. Final product/fixture source is cb7905fe6a98380b8875acdee8f11ddd3f8809cc,
tree b477bce343ee8e8e6a21419fe68243220d2ca349. HEAD changes only the concurrency
harness, tree 9faad4183c4ee6e5a8c5bb74b2ffc2d10eda142e; product inputs are identical.
Published SQL 1..13 is unchanged. Additive SQL 14 has not changed since 730e50e.

The private close_record accepts only NativeOutcomeCopy after recording the full
outcome. Rejected/unbound inputs cannot close a record. Replay compares immutable
membership without changing rows. Head advancement increments local/global job
revisions, preserving holes/zero. The 64-retained-bindings/job cap bounds scans;
there is no reclamation. Producer ACK/GC watermarks remain untouched. Migration 14
refuses unsupported prior nonzero import checkpoints atomically with an explicit
recovery diagnostic. Control, job, Ingest, producer lock order is preserved.
Cancellation/residual lease are rechecked after SQL awaits before COMMIT.

## Executed evidence

The [qualification report](evidence/p03-import-checkpoints-native-20260919.json)
records source/artifact identities, all outcomes, preserved failures and limits.
Final native qualification at cb7905f, Linux amd64/PostgreSQL 16.15/Rust+Rocks:
- Clean 832-file Git export, source hashes unchanged; 16 migration and 5 binding
  tests, scoped Clippy and actual release binaries/fixtures built successfully.
- Seven connected signed cases PASS: reverse logical order, identical delayed
  positive, cooperative cancellation during actual closure PgSleep, lease expiry
  after SQL, rollback, and COMMIT 12 reply loss before forwarding/after completion.
  All match 27 independent native rows, three full originals and exact PG copies.
  Reverse/delayed/after-complete retain two closures, head line 2/byte 706/revision 1,
  job revision 4/checkpoint 1. Others retain line 2 only, no head, job 3/checkpoint 0.
  Exact replay preserves complete rows including xmin. No producer checkpoint.
- 25 synthetic SQL constraint cases PASS, including holes, shard/global revisions,
  multirow AFTER visibility, no-op replay, immutable retention, rejected outcomes,
  wrong digest, rollback and pause. These synthetic outcomes are not native proof.
- Six real two-client lock/isolation schedules PASS using HEAD harness: both lock
  orders with COMMIT/ROLLBACK, stale READ COMMITTED and refused REPEATABLE READ.
  Actual blocking PIDs observed; closure/head/job state independently checked.
- Four actual 13-to-14 compatibility cases PASS (r4): empty/populated migration,
  old client refuses 14, new client refuses admission on 13, and unsupported old
  checkpoint refuses with explicit recovery diagnostic and unchanged complete
  rows/schema/native/spool. No migration backfill. New client on 13 may initialize
  its owner/native manifest before refusal; no input staging, reservation or fact.
- Every completed case and terminal audit observed zero owned PG client backends.

Preserved failures: native 730e50e RED observed admitted line 2 with no closure;
local E0716 fixed with explicit locals; rollback1 rejected before DB creation
(tag too long). Compatibility r1 failed private scratch permissions, r2 failed
oracle request ordering, r3 observed correct rollback but generic CLI diagnostic.
They remain INCOMPLETE; r4 is a separate corrected run. Independent AI review found
a coherent-PG-digest oracle gap: literal original CBOR/files and all binding/head
fields now checked, with offline altered-digest refusal retained. Final AI review
found no new blocking counterexample; it inspected captured evidence, not a new
engine run. Cooperative flag cancellation is not future/driver abort qualification.

## Evidence and resources

No active experiment or tool session. Read-only resumption inspection confirms
both final containers exited without OOM and no running BigBrotr container.
Terminal audit at 2026-09-19T20:50:11.082280+00:00 records 374 preexisting container
lifecycles unchanged, eight unrelated running and zero BigBrotr running.
Final lab /home/tux/bigbrotr-implementation-20260917/runs/import-checkpoints-cb7905f,
owner 50a453d1c2cd30239a63be431128312b. Runner
104b8cd0f60198575fd733708b2afc1c6947e5e6155e51b0143594061ca9328c,
PG 685eddb4df2a5d3bd55b0d80043ba8f9e055773fe4bf044812e3052fceefb26f,
retained empty internal network 43655e02c0afe3464e2ca75f7736be4d8e350d01e98f6f42f0b26e817a50f297.
Its out/build is the latest exact-lock cache. Verify ownership before reuse.
See import_checkpoints_final in the [inventory](../research/laboratory-inventory.json).

Mac .research/implementation/import-checkpoints/final/qualified retains 53
hash-verified files; tar SHA256 251eeaa5ac0d7d3e1d01993285c302aeee6f3b447aeab7f82e1d05535fe43cd0.
All 832 source hashes match Mac Git archive. Main SHA256
ed0776b68e4815686246d4df7c9e1527b15a54ddcbf5a0231b355ffb64a09575,
ingest probe e49fd9df41553b82288540ab36d43c8ec1ddba2ad2803d5fcf3aba6265793b7e.
Audit and evidence-identity.json retain complete source/artifact/report identities.
Earlier red/qualified (19 files) and green/qualified (50 files) captures preserve
failures. Their lab containers/data remain stopped. Prior owned empty networks
were retired after inspection to free subnet pools; no global prune or unrelated
cleanup. Helpers under the ignored import-checkpoints directory need path/owner
review before reuse and must not overwrite old evidence or include secret files.

## Next action and remaining gates

Checkpoint qualification report, owning references/task notes and integrated diff
review are complete. Artifact and handoff/link checks passed with no errors; their
scope is structural. Commit/push this verified slice, then begin P02-T02 step 5
CertifyExisting: inspect exact native admission/reducer and proof contracts before
adding its read-only existing-effect consumer. Independent fact_codec_review is
reading that boundary only, with no writes or Tux work. No task checkbox changed.

Initial adapters still bind original Control and producer/owner epoch 1. Recovery
needs a read handle incapable of fresh admission, synchronized native closing,
fresh-Control close/register/topology CAS and explicit successor activation/zero
counters. New processes cannot impersonate old incarnations/candidate nonces.
Resolve ambiguous original outcomes before choosing another path.

Logical proof/claim closure, ACK/GC, archive protection/replay, ENOSPC/power loss,
causally closed backup/restore, mixed aged workloads, M1 read/export/restart and all
selected acquisition/analytics/read/UI/website/Compose/release journeys remain.
Hosted Actions and native Linux arm64 are not qualified by Tux amd64 execution.

External monitor findings remain scoped closed with practical evidence:
[MON-001/002/004/006](evidence/p01-monitor-corrections-native-20260918.json),
[MON-005](evidence/p01-jsonb-limit-native-20260918.json),
[MON-009](evidence/p02-job-bounds-native-20260918.json),
[MON-010](evidence/p01-tls-presence-20260918.json),
[MON-007](evidence/p01-archive-cache-retry-20260918.json).
No new contrary evidence in the requested external review note.
