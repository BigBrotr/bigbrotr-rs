# Current execution checkpoint

Observed 2026-09-18. Navigation only; follow the
[resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint) and
verify Git, evidence and owned resources before continuing.

## Mandate and active boundary

Owner-authorized implementation remains active ([Q30](../planning/questions.md),
[mandate](../planning/mandate.md)). Implement the complete selected product.
Continue trusted native qualification on Tux despite unavailable hosted Actions.
No public release, repository rename or Linux arm64 qualification is implied.

Active responsibility: P02-T04 step 1's first native engine/bundle boundary, then
P02-T03/T04 typed archive publication consuming real pending batches. The initial
private worker now executes engine inspection; it does not yet publish scientific
rows or implement parent grants/supervision. No storage task checkbox or M1 gate
has been completed by this prerequisite.

## Git and local ownership

Local main starts from normally pushed
`a745f7f1406952f808cbe40d85e687b92cdfde84`. Root owns the current candidate changes:
Cargo manifests/lock, new `apps/bigbrotr-archive-worker/`, the native asset registry,
release dependency/license records and notices, bundle/build/CI helpers and their
controls, task/development documentation, laboratory inventory and this checkpoint.
No contributor conflict, history rewrite, public release or Go-neighbor edit.
Verify actual status and HEAD before committing; no future commit hash is assumed.

The new private Cargo package isolates native linkage from ordinary CLI/Control/
Ingest. `tools/build.py` prepares/copies the pinned bundle at build time, binds
explicit library/header paths, records the worker/artifact identities and checks
helper/source equality. Shared CI uses a fresh sibling `<output>-native-inputs`
root. [Bundle usage and limitations](../../packaging/archive-worker/README.md)
are the current interface consumer; parent exec verification and supervision are
still required before any publisher can acquire authority.

## Evidence and findings

Raw local root: `.research/implementation/archive-worker/`; inventory entry
`implementation_archive_worker_20260918` in the
[laboratory inventory](../research/laboratory-inventory.json).

- Actual DuckDB 1.5.5 / source `d8cdaa33fd` loads on Mac arm64 with the exact
  `duckdb`/`libduckdb-sys` 1.10505.0 binding and Arrow 58.4.0. The diagnostic loads
  all three pinned signed extensions: DuckLake `d8a1881e`, postgres scanner
  `41223e5`, httpfs `827222f`; see `inspect-mac.json`.
- The first RED test reproduced default automatic extension installation.
  The implementation disables autoload/autoinstall, unsigned/community extensions,
  persistent secret creation and progress output. Engine home/secret/temp paths
  point to the caller's empty canonical private directory. Native limits do not
  establish an RSS ceiling or a process reservation.
- Six native tests pass after the final private-home change: exact limits,
  invalid roots/zero limits, actual signed bundle load, altered native signature
  refusal and byte/hash/symlink controls. `final-mac-checks.log` also records
  successful worker Clippy. Ten build/CI process controls pass in
  `build-controls.log`; intentional negative CI subcases retain FAIL/NOT_RUN.
- Native negative-fixture setup initially failed because Rust copied readonly
  permissions. The retry changes only its owned copy to 0600 before corrupting
  the signature; original log `engine-tests.log` remains failed. An earlier
  `enable_progress_bar` startup option failed because it is connection-local;
  setting it on the connection is verified. No failure is relabeled PASS.
- All 207 preceding lock packages/checksums remain; the worker adds 87 packages.
  Dependency policy initially refused three new license instances. Exact source
  notices were read/copied and exact-version allowances added for foldhash 0.1.5,
  zlib-rs 0.6.8 and build-only webpki-roots 1.0.9. Advisories/licenses/sources now
  pass (`dependency-policy-reviewed.log`). This is not the full native release SBOM.
- Official Linux amd64/arm64 and Mac universal engine ZIPs and all four target
  extension sets are pinned in `contracts/release/archive-engine.json`. Additional
  target assets are downloaded inputs, not executed target evidence. A default
  Python user-agent got HTTP403; the explicit build client user-agent succeeds
  with ordinary verified HTTPS. Original failure remains `matrix-download.log`.
- Mac whole-workspace build was launched with explicit native/compiler inputs,
  log `mac-workspace-build.log`; its terminal outcome must be checked before any
  claim. Initial artifact checks pass. Exact-revision native worker build, actual
  no-network packaged execution, source/artifact audit and cleanup are next.

Typed scientific schemas, Arrow conversion/allocation, complete dictionary closure,
PG DuckLake catalog transactions, positive publisher fencing, ambiguous receipt
recovery, protection/reclamation, archive read/export/restart and later product
milestones remain required. Pending data must not be reclaimed. The previous
binding caveat remains: Arrow list slices retain complete child arrays, so row
count alone is not an allocation bound; explicitly check appender flush outcomes.

## Monitor review and prior completed qualification

The owner-supplied independent monitor review was handled at the preceding natural
boundary. All five findings have scoped corrections and practical evidence:
[MON-001/002/004/006](evidence/p01-monitor-corrections-native-20260918.json) at
`2c0dfc1`, and [MON-005](evidence/p01-jsonb-limit-native-20260918.json) at
`c7e0c09`. The qualification is normally pushed in `a745f7f`. JSONB cap-1 and exact
10 MiB initialize/inspect/activate/retry; cap+1 and the original compact-only
counterexample refuse without Control mutation. Original failures remain in their
historical reports. Signed admission remains qualified only for its recorded
[initial-owner scope](evidence/p02-event-admission-native-20260918.json) at `12f936d`.
No complete P01/P02, M1, peak capacity, power-loss or release qualification follows.

## Owned native resources

Fresh read-only Tux inventory is `native-preflight.json`: zero running BigBrotr
containers; eight unrelated workloads continue; 151 total retained containers;
340,682,608,640 free disk bytes. Shared checkout is clean/detached at
`6be851580ba415eed5ff0979ee7072480f9ee3ab`, with zero active consumers. Do not reset it.
No archive native lab exists yet. Older monitor, both JSONB, admission and bootstrap
labs remain separately stopped-retained with exact IDs/owners in the inventory.
No resource was deleted or reused. Retired environments stay retired.

## Next bounded action

Review the complete candidate diff and terminal Mac checks, commit the unpublished
native-worker candidate, transfer its exact source via Git bundle, and build/run it
in a fresh labeled native Linux amd64 lab. Record source, compiler, both executable
and native asset identities; test offline signed loading, tampered/missing assets,
loader independence of the ordinary CLI and refusal paths. Preserve all unrelated
resources. Qualify this prerequisite honestly, then implement the portable typed
record/publisher consumer; do not stop at an engine diagnostic or mark T04 complete.
