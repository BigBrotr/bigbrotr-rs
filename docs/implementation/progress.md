# Current execution checkpoint

Observed 2026-09-18. Navigation only; follow the
[resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint) and
verify Git, evidence and owned resources before continuing.

## Mandate and active boundary

Owner-authorized implementation remains active ([Q30](../planning/questions.md),
[mandate](../planning/mandate.md)). Complete the selected product, not only its
foundation tests. Continue trusted native qualification on Tux despite GitHub
Actions billing; hosted runners remain NOT_RUN. No public release, repository
rename or new Linux arm64 qualification follows from these results.

The owner-supplied monitor review at
`/Users/vincenzo/.codex/reviews/bigbrotr-rs-monitor/implementer-feedback.md`
was handled after the signed-event qualification/cleanup boundary. All five
findings now have scoped corrections and practical evidence. No task checkbox,
P01/P02 completion, M1 completion or release gate was advanced by that result.
Resume the first signed-event archive slice, P02-T03/T04, after committing and
pushing the reviewed MON-005 qualification documentation below.

## Git and local ownership

Private origin/main was last observed at
`67f6c6f998fc8d5bd70a2286049402f7c3bf211f`. Local main contains these unpublished
candidates:

- `de49f18972a9cbe3e014c3b4e830e20a91841558`: MON-005 product guard, independent
  vectors, regression and native size harness. No SQL migration, Cargo dependency,
  canonical policy bytes/domain/version or reader cap changed.
- `c7e0c093a7666c9b66ed1c3950ed7a8886ee8fe3`, tree
  `64cd79637c7ae3551394843f7d30bb4d5b531a3c`: two-line harness fixture correction
  and first-run failure/resource record. Product code is unchanged from `de49f18`.

Root owns only current qualification edits: this checkpoint, P01-T04 task note,
new JSONB evidence, initialization/harness documentation, laboratory inventory
and the scoped archive binding note in `docs/research/dependency-selections.md`.
No contributor conflict, force-push, history rewrite or Go-neighbor edit exists.
Check actual status/HEAD/remote; never guess a future qualification commit hash.

## Verified monitor evidence

[Earlier Mac](evidence/p01-monitor-corrections-mac-20260918.json) and
[native evidence](evidence/p01-monitor-corrections-native-20260918.json) at
`2c0dfc1` cover MON-001/002/004/006: 54 scoped tests, four new process cases,
sixteen earlier Control process regressions, 330 relation observations and exact
source/tool/ELF audit. Those outcomes belong to that revision, not an unexecuted
new suite.

- MON-004: delayed successful acquisition/poll each waited at least 3.2 seconds,
  then continued healthy supervision. Scheduling uses remaining request-start
  authority and the next heartbeat; paused-process/genuine expiry still refuses.
- MON-001: a genuinely full unread 65,536-byte stdout pipe did not stop database
  renewal or SIGTERM release/backend cleanup. Exit 1 reports lost diagnostics;
  unchanged pipe/EOF proves no writer thread survived. Blocked stderr exited in
  0.216 seconds on Linux. Process exit after bounded diagnostic drain is not a
  reusable cancellation mechanism for a worker whose parent continues running.
- MON-002: twelve static/prepared-driver cases agree on explicit 443, ordinary/
  maximum ports, IPv6 and malformed/missing ports. No live server on 443 claimed.
- MON-006: identical retained replay under reduced byte/dependency limits reports
  Limit without changing native state; adequate replay returns original receipts.
  Malformed state still refuses as inconsistent.
- MON-005: [new native size evidence](evidence/p01-jsonb-limit-native-20260918.json)
  qualifies `c7e0c09`. JSONB at 10,485,759 and 10,485,760 bytes initializes,
  inspects, activates and retries successfully. Cap+1 and the original 200,000 x
  49-byte endpoint policy refuse before acceptance, with whole Control dumps
  unchanged. Both complete 215-setting stored documents and digests match an
  independent frozen-policy/Decimal/hashlib oracle. Tiny finite fractions,
  u64 max, Unicode/escapes and nested records are included; 22 numeric/structural
  vectors match actual PG16.15 rendering.

Native qualified CLI SHA-256:
`8087b78df1fd55451e5865588854230750839fea834cd1ab70d65513497bf67d`.
All 544 source entries and actual native tools match. Seven binary tests passed
on Mac and native Linux at `de49f18`; Rust code/fixtures are unchanged in `c7e0c09`.
Mac Clippy, formatting, artifact checks and six actual recipe validations pass.
Raw results and independent collection/audit scripts:
`.research/implementation/jsonb-limit/`, particularly `retry-c7e0c09/`.

The original `2c0dfc1` SQL refusal stays CONFIRMED_UNFIXED in its historical report.
The first `de49f18` size harness failed before large admission because it used the
explicitly unimplemented `discoverer.api.sources` field. Its 22 passing vectors,
failed case and later NOT_RUN cases remain in `first-run/`. The corrected fixture
uses supported `seed.file`; no required invariant was changed. A separately
adjusted Mac fixture also passed static validation, with its distinct digest
recorded. No peak RSS, mixed-load capacity, host power-loss, complete lifecycle,
arm64 or release qualification is claimed. No new subagent review was available;
integrating-owner review used the independent monitor finding and actual code,
engine source, CLI outputs and stored values.

## Owned native resources

Fresh cleanup observes zero running BigBrotr containers, the same eight unrelated
workloads unchanged, and clean detached shared source
`6be851580ba415eed5ff0979ee7072480f9ee3ab` with no consumers. Do not reset it.
No resource was deleted. Exact IDs/labels/mounts/terminal states are in the
[laboratory inventory](../research/laboratory-inventory.json).

- Earlier monitor lab: all thirteen containers stopped-retained under
  `/home/tux/bigbrotr-implementation-20260917/runs/monitor-2c0dfc1`; inventory
  `implementation_monitor_corrections_20260918`.
- First JSONB candidate: four stopped-retained containers under
  `/home/tux/bigbrotr-implementation-20260917/runs/jsonb-de49f18`. Build owner
  `a6f441775e1e4ad79e8c8dd83899bf43`, test owner
  `93465c3b679e7d4c072435bebf631470`, runtime owner
  `3f1b4f7b7a8a9d3c803ab714cf3dd5e5`; PG/client prefix `jsonb_limit`.
- Qualified JSONB candidate: three stopped-retained containers under
  `/home/tux/bigbrotr-implementation-20260917/runs/jsonb-c7e0c09`. Build owner
  `6496253d140c06a80c7e3329c85faeb8`, runtime owner
  `4c647482c8cc4e3fefa37fe038cc5edf`; PG/client prefix `jsonb_verified`.
  Inventory `implementation_jsonb_limit_20260918`; raw cleanup audit is in
  `retry-c7e0c09/cleanup-audit.json`. Build and scenario orchestration are terminal.

Older admission/bootstrap labs also remain separately stopped-retained. Retired
pre-existing Tux resources remain retired. No native archive lab exists yet.

## Next bounded action

Finish review/artifact checks of the qualification diff, commit and normally
push verified candidates to the private repository. Then implement P02-T03/T04's
portable typed records and isolated native DuckDB/DuckLake worker consuming the
actual pending feed. Keep pending data until real archive receipt/protection and
replay closure are qualified. Signed-event admission is already qualified at
`12f936d` in [its native report](evidence/p02-event-admission-native-20260918.json),
but installed source policy, ACK/retirement, archive-backed resolution, successor
recovery, read/export/restart and all later product milestones remain required.

Archive inputs under `.research/implementation/archive-worker/` are inventoried:
official DuckDB 1.5.5 amd64/Mac universal ZIPs match upstream digests;
libduckdb-sys 1.10505.0 sources match the exact crate; six offline extension
candidates have hashes. These native archives/extensions have not been loaded,
packaged or qualified. No archive Cargo dependency or product code exists yet.
The Arrow 58.4.0 list-array source was additionally downloaded and checksum-checked
against the pinned binding lock; its hash is recorded in the same inventory.

Scoped reading while builds ran covered P02-T03/T04, the complete archive placement
contract, archive rows in storage catalog, Fact/DictionaryDescriptor and the pending
feed. Binding observations are now in the dependency selection record:
ToSql rejects nested containers; appender-arrow uses native vector chunks; list
slices retain the entire child array and conversion can copy that full array;
explicit flush is required because Drop cannot report errors. Bound actual child
buffers and avoid repeated full-child conversion. Nested conversion/allocation,
offline signatures/ABI, real publisher fencing and native persistence still need
actual adapter tests. Continue from these concrete observations, not another
architecture-research restart or an opaque-blob substitute for typed archive rows.
