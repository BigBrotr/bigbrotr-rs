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

Private origin/main remains `67f6c6f998fc8d5bd70a2286049402f7c3bf211f`.
Local main is candidate `de49f18972a9cbe3e014c3b4e830e20a91841558`, tree
`8708c62a0512a7f89b5c06a7afa83eb36bc7cdec`. It contains MON-005's size-only
JSONB guard, independent Decimal/structural vectors, size harness and owning docs.
No SQL migration, Cargo dependency or policy identity/version changed. Seven Mac
binary tests, Clippy, formatting and artifact checks pass; behavioral RED is in
`.research/implementation/monitor-corrections/jsonb-red.log`.

Native build and seven binary tests PASS at that exact candidate. All 544 source
entries and actual native tools were independently checked. CLI SHA-256:
`772530122087225cf87426418dde5927b73d4e08cca12d32f25dfda2ab4fc2b0`.
All 22 frozen numeric/structural vectors match actual PostgreSQL 16.15, but the
native maximum-document harness FAILED before its first large admission: the
fixture used `discoverer.api.sources`, explicitly unimplemented by this schema.
This is a harness error, not evidence that the new size guard failed or passed
its end-to-end gate. Later large cases are NOT_RUN. The failed report remains in
`.research/implementation/jsonb-limit/first-run/` and the stopped native root.

Root owns current uncommitted `size_limits.py` fixture correction plus checkpoint
and laboratory inventory. It now uses supported `seed.file` for Unicode/escapes.
An independently adjusted near-limit fixture passed the actual Mac CLI: compact
10,284,724 bytes, independent JSONB size 10,485,759, digest
`a88e5613bb746ad0b766a0a4a886bab06aff69b349444c2db1d25efca85e2fe6`.
This is static evidence only; corrected native persistence remains NOT_RUN.
No conflicting contributor files, force-push or Go-neighbor edit exists.

The first raw-SHA incremental bundle command refused an empty bundle locally;
a full HEAD bundle then succeeded. A formatted SSH inspection command had a
quoting error; unformatted inspection through Python succeeded. Neither failed
command changed product/database state. Raw source/tool/build/results are under
`.research/implementation/jsonb-limit/`. The pinned PG source tag resolves to
`7d3e000c5961a544302072058a1184e9a588837b`; scoped JSONB/escape/numeric functions
and exact serde_json 1.0.151 serialization were inspected. Source hashes are in
`upstream/identity.json` there.

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

The thirteen earlier monitor containers are stopped-retained under
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

The new first MON-005 lab also has all four containers stopped-retained:
`/home/tux/bigbrotr-implementation-20260917/runs/jsonb-de49f18`.
Build `bigbrotr-implementation-jsonb-de49f18-build`, owner
`a6f441775e1e4ad79e8c8dd83899bf43`; tests suffix `-tests`, owner
`93465c3b679e7d4c072435bebf631470`; PG/client/network prefix
`bigbrotr-implementation-pg-20260917-jsonb_limit`, runtime owner
`3f1b4f7b7a8a9d3c803ab714cf3dd5e5`. Fresh cleanup audit observes no BigBrotr
running, eight unrelated workloads unchanged, clean shared source `6be8515`
and no shared-source consumers. No resource was deleted. Exact IDs/artifacts and
terminal observations: inventory `implementation_jsonb_limit_20260918` and raw
`first-run/cleanup-audit.json`. No new candidate retry resource exists yet.

## Next bounded action

Commit the reviewed two-line fixture correction and its failed-run record, then
transfer the new exact Git candidate to a fresh Tux run and rebuild. Execute
`configuration-inspection/size_limits.py`: accepted JSONB sizes cap-1 and cap,
refusal at cap+1 and the original 200,000 x 49-byte endpoint counterexample.
Require initialization/retry, inspection, initial activation/retry, complete
stored-document equality, independent compact digests and actual PG byte counts.
Keep failed first-run evidence. Do not close MON-005 until this passes; do not
change constraints or a required result merely to get green. Record terminal
resource ownership/cleanup and normally push coherent verified commits.

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

During the native build wait, scoped archive boundary reading was resumed:
`Fact`, `DictionaryDescriptor`, pending feed, P02-T03/T04 and the archive placement
contract. Exact duckdb-rs 1.10505.0 source explicitly rejects List/Struct binding
through ToSql (`src/types/to_sql.rs` tests and `statement.rs`). Do not design
nested tags around those parameters. The existing selected Arrow path has an
`appender-arrow` feature; `append_record_batch` slices to native vector size and
copies through one DataChunk per slice. Its conversion/flush behavior and nested
fidelity still require inspection/actual tests before use; no feature/Cargo/code
change or executed native archive claim follows from this reading.
