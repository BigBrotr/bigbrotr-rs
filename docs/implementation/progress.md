# Current execution checkpoint

Observed 2026-09-18. Navigation only; follow the
[resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint) and
verify Git, evidence and owned resources before continuing.

## Mandate and active boundary

Owner-authorized implementation remains active ([Q30](../planning/questions.md),
[mandate](../planning/mandate.md)). Complete the selected product, not only its
foundation tests. Continue trusted native qualification on Tux despite GitHub
Actions billing; hosted runners remain NOT_RUN. No public release, repository
rename or Linux arm64 qualification is authorized by these results.

The owner supplied independent monitor feedback at
`/Users/vincenzo/.codex/reviews/bigbrotr-rs-monitor/implementer-feedback.md`,
reviewed at `12f936d`. It was taken in charge after event qualification/cleanup
and the archive dependency preparation checkpoint. No competing plan was created.
P01-T04/T06 and P02-T02 now have scoped MON-001/002/004/006 corrections and
[Mac](evidence/p01-monitor-corrections-mac-20260918.json)/[native](evidence/p01-monitor-corrections-native-20260918.json)
evidence. No task checkbox advanced. MON-005 is confirmed; its correction is in
progress and native acceptance remains NOT_RUN.

## Git and local ownership

Main and private origin/main now match
`67f6c6f998fc8d5bd70a2286049402f7c3bf211f` after normal push of the reviewed
qualification. Its preceding implementation commits are:

- `77f3d98`: retained exact archive dependency inputs, no product dependency change.
- `2c0dfc116daf1a397f06bad71ce7013864bacd01`, tree
  `7015e92a3fed9fe9b99dc415019aa5cf8587aa90`: monitor product corrections and harness.

Those commits are published. Native source/artifact qualification below uses
exactly `2c0dfc1`. Root has started the MON-005 regression in
`apps/bigbrotr/src/config/policy.rs` and updated this checkpoint; those are the
initial owned dirty files. The size-only JSONB guard and all seven binary tests
now pass on Mac (five scoped policy tests); the original behavioral RED and
independent decimal/structural vectors are retained. Clippy, formatting and
artifact checks pass. The new `configuration-inspection/size_limits.py` is an
unexecuted native acceptance harness; owning configuration docs are updated.
Current additional root-owned dirty files are that harness, its frozen vectors,
its README, generated configuration reference/generator, P01-T04 task note, and
the configuration/runtime and initialization contracts. Raw local results and
fresh Tux preflight are in `.research/implementation/jsonb-limit/`; preflight
observed no active BigBrotr containers, eight unrelated workloads, unchanged
shared source, no consumers and about 342 GB free. No new remote resource yet.
No contributor conflict, force-push, history rewrite or Go-neighbor edit exists.
Check actual status/HEAD/remote before the next action; never guess a future
qualification commit hash.

## Verified monitor results and remaining defect

Fifty-four tests pass on Mac and Linux: 42 actual library tests (including native
Rocks and the fifteen signed semantic operations), 5 binary tests and 7 PG
connection tests. Scoped Mac Clippy, formatting and artifact checks pass. The
native shared CI executor ran actual release test ELFs; all three downloaded
ELF identities were independently checked. Native CLI SHA-256:
`360a71585e203f663731459e4711b3590e90d255db17bcb599bc9a644e5a0da8`.
All 541 Git/archive/source entries and the actual native tools match.

- MON-004: slow successful acquisition and a later policy poll each wait at least
  3.2 seconds on actual PostgreSQL, then continue healthy supervision. Scheduling
  uses half remaining request-start validity and the next heartbeat deadline.
  Request-start expiry and actual paused-process refusal remain unchanged.
- MON-001: stdout is genuinely full (65,536 unread bytes) while the database
  scheduling heartbeat renews. SIGTERM releases the owner, closes its backend and
  exits 1 because output cannot drain. The pipe remains unchanged and reaches EOF
  after the parent closes its own writer: no diagnostic thread survives process
  exit. Separately blocked stderr exits in 0.216 seconds on Linux. Diagnostics
  use a bounded thread/queue and direct descriptors, no Rust stdio locks; after
  backend cleanup the CLI drains for at most 200 ms then exits the whole process.
  This is not cancellation for a worker in a process that continues running.
- MON-002: twelve static-CLI/prepared-driver parity cases cover 443, ordinary and
  maximum ports, bracketed IPv6, absent, zero, overflow and invalid IPv6 syntax.
  No actual server listening on 443 is claimed.
- MON-006: retained eleven-dependency signed request retried with limit eight
  returns `Limit`, not `StateMismatch`; all KV rows stay unchanged and adequate
  replay returns the original receipt. Bootstrap's reduced 128-byte cap also
  returns `Limit` without writes. Malformed state still refuses as inconsistent.
- MON-005: actual static validation accepts 200,000 configured endpoint entries.
  Its independently matched compact policy is 10,407,457 bytes; actual PG16.15
  JSONB text is 10,607,924 bytes, above 10,485,760. Actual initialization exits 1
  and the entire database dump digest is unchanged. This is CONFIRMED_UNFIXED,
  not PASS. Inspection/activation of that candidate are NOT_RUN because initial
  persistence failed.

Four new process cases and all sixteen earlier Control cases pass. Independent
checks cover 330 table observations, exact policy digests, owner exclusion
receipts and lifecycle output. Tests include genuine expiry after pause, query
cancellation, DB crash/restart, concurrent ownership and both stop signals.
Raw scripts/logs/results: `.research/implementation/monitor-corrections/`;
`runtime-evidence/` contains complete observations and independent audit reports.
The original Mac behavioral failures remain recorded. A missing-API compile
failure for `remaining_validity` is not labeled a behavioral RED. Review used
owner-supplied independent static findings plus integrating-owner inspection;
no new independent agent review ran (previous agents have usage-limit errors).

## Owned native resources

All thirteen new containers are stopped-retained under
`/home/tux/bigbrotr-implementation-20260917/runs/monitor-2c0dfc1`; no deletion.
The runtime orchestration completed both matrices and its stop step. Fresh
terminal/cleanup audits confirm zero running BigBrotr containers and the same
eight unrelated running workloads unchanged, excluding rolling health-log entries
and normalizing mount order. Shared checkout remains clean detached
`6be851580ba415eed5ff0979ee7072480f9ee3ab`, with zero consumers. Do not reset it.

Build owner `2f29439d9430a0c9fea71a64d05bef04`; tests owner
`18bebe3a488ef24745292f465c09e737`; runtime owner
`cd35c516d7be99b8933712ff1e02cd9d`. Network/PG prefix:
`bigbrotr-implementation-pg-20260917-monitor_review`.
Exact IDs, labels, limits, mounts and terminal states are in the
[laboratory inventory](../research/laboratory-inventory.json) and local
`terminal-audit.json`, `final-preflight.json`, `cleanup-audit.json`. Older admission
and bootstrap laboratories remain separately retained and stopped.

## Next bounded action

Qualification documentation and verified work are normally pushed. Correct
MON-005 in P01-T04/P02-T01: preserve the
existing 10 MiB input/storage/read limits and reject a normalized policy before
acceptance when its JSONB rendering exceeds that cap. PostgreSQL 16.15 source
inspection confirms added separators and decimal expansion of exponent-form
numbers; counting compact JSON alone is insufficient. No applied migration has
been edited. Exact serde_json 1.0.151 Number::Display/Serialize and PostgreSQL
16.15 numeric
input/output inspection is complete; the size guard counts separators and scale
without creating a second rendered document or changing digest bytes.
Execute real rejected/near-limit accepted initialization, inspection and initial
activation, including tiny finite fractions and Unicode/escape controls. The
historical monitor harness still reproduces the old defect at its pinned
source. The new size harness must pass on a clean exact candidate before closing
MON-005. Avoid changing an expected failure into green without those cases.

After this boundary, resume M1's archive risk slice (P02-T03/T04): portable typed
records and isolated native DuckDB/DuckLake worker consuming actual pending
batches. The first signed-event durable admission is qualified at `12f936d` in
[its native report](evidence/p02-event-admission-native-20260918.json), but installed
source policy, ACK/retirement, archive-backed resolution, successor recovery,
read/export/restart and all later product milestones remain required.

Archive dependency inputs are retained under
`.research/implementation/archive-worker/` and inventoried: official DuckDB 1.5.5
Linux amd64/Mac universal ZIPs verified against upstream release digests; exact
libduckdb-sys 1.10505.0 crate/extracted sources match; six offline extension
candidates have hashes. Python urllib HTTP 403 and Python 3.9 tar-filter failures
were resolved with ordinary HTTPS curl and strict safe member validation. These
were preparation failures, not native tests. Extension signature/ABI/runtime and
packaged loading remain NOT_RUN. No archive Cargo dependency/product code or
archive Tux resource exists yet. Do not reclaim pending evidence before archive
receipt/protection and replay closure are established.
