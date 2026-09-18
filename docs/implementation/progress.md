# Current execution checkpoint

Observed 2026-09-18. This is navigation, not a second plan. Follow the
[resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint) and
verify actual Git, evidence and owned resources before continuing.

## Mandate and current task

Owner-authorized implementation is active ([Q30](../planning/questions.md),
[mandate](../planning/mandate.md)). The objective remains the complete selected
product, not this milestone. The session goal was freshly read as `active`.
GitHub billing remains external NOT_RUN; use the shared trusted CI/build tooling
on Tux. No public release, repository rename or Linux arm64 qualification exists.

P02-T02 initial dictionary bootstrap now connects the actual Control session to
one synchronized native RocksDB batch, including facts, exact inventory,
allocation/source counters, producer state, request receipt and pending marker.
The existing receipt and pending readers consume it. Full retained replay checks
the entire bounded initial state. No P02-T02 checkbox was advanced: general signed
event admission, producer retirement, archive/read/export/restart and all later
product responsibilities remain required.

The latest local correction requires canonical endpoint bytes through the existing
protocol normalizer and explicitly compares the locked directory with the trusted
factory's configured root. The native probe now includes wrong-root refusal. This
is still a connected backend slice; installed Ingest startup, full action grants,
successor/recovery authority and event admission are not implemented.

## Git and local ownership

Starting main/origin/main were clean at
`4a927bacac89096f7c08446921c7d3ea06f4a0ea`. Candidate
`524e38cc7bd0c4f003d78e0cb40018bdcc3eb874` is committed locally, not pushed.
Root owns only the subsequent endpoint/root checks, corrected oracle/log retention,
probe and relevant documentation/evidence/inventory changes. Resolve the containing
correction commit and actual dirty/remote state on resumption; no other contributor
writes were found. Published history was not rewritten. The Go neighbor is untouched.

Mac checks: 48 model tests and four initial actual native tests passed. The added
endpoint test first reproduced an incorrectly accepted noncanonical URL; after
fixing the writer, all five bootstrap tests pass. Workspace Clippy and the final
probe check pass. See [Mac evidence](evidence/p02-ingest-bootstrap-mac-20260918.json)
for exact source/log hashes and the intermediate missing-module compile error.

## Native results and corrections

Candidate 524e38c built on Tux Linux amd64 from a new clean Git-bundle clone.
All 528 archived and actual source files, source manifest, native tools and both
downloaded ELF files were independently checked against Git:

- CLI SHA-256 `9894ba33eb4bc773a69c73334d4a789bbf7b9c575a6350d6a504f7d088ff75b3`.
- Probe SHA-256 `4aace7d0886d813abb305fdd794166f9824bb2e08144271778feb7c124a5aaae`.
- Native release tests: 48 model and four bootstrap cases PASS; eleven downloaded
  test executables independently hashed. These precede the endpoint/root correction.

The first native runtime matrix is **FAIL**, after the first actual write/replay.
Its independent Python oracle incorrectly used dictionary kind 1 for visibility;
the pre-existing identity and descriptor contracts assign kind 4. Correcting that
constant compares all twelve unchanged native rows, exact receipt and both fact
preimages successfully. This local re-audit does not turn unexecuted crash/negative
cases into PASS. The initial report and bytes are retained unchanged.

The first native test launcher also failed before any test command: it precreated
the output directory that shared CI `execute` requires to be absent. A corrected
launcher ran all 52 tests in a separate retained container. The initial attempt
remains NOT_RUN. An initial lab name was rejected before resource creation, and
SFTP did not expand a brace expression; explicit artifact paths then downloaded
and audited successfully. None of these errors was recorded as product PASS.

Raw records, observers, failed and successful attempts:
`.research/implementation/ingest-bootstrap/`. The exact build, original runtime
report/dump, corrected-oracle audit and test artifacts are in `native-524e38c/`.
Reproducible runtime/oracle sources are in `experiments/ingest-bootstrap/`.

## Owned Tux state

Latest complete inventory: `.research/implementation/ingest-bootstrap/correction-preflight.json`.
Zero running BigBrotr containers; the same eight unrelated workloads remain
running. Shared checkout remains clean detached `6be8515` with zero consumers.
No unrelated or retained experimental resource was removed.

Candidate root: `/home/tux/bigbrotr-implementation-20260917/runs/bootstrap-524e38c`.
Its `source` checkout is immutable input, with separate writable `out` and retained
`runtime` lab. Build container `bigbrotr-implementation-bootstrap-524e38c-build`
(`09ad7c9f...`, owner `5bdbb89a14feeebb1af26fa10a2042c7`) stopped, exit0/OOMfalse.
Initial test container stopped exit1 with no tests; retry stopped exit0/OOMfalse.
The PG16.15 laboratory and idle client were explicitly stopped after their ownership
labels were verified. Its first test database and native directory are retained.
Full IDs, labels, limits, launch receipts and terminal records are in the
[laboratory inventory](../research/laboratory-inventory.json). Build/test limits
were 2CPU/6GiB/512PIDs; no ports, Docker socket or release credentials were exposed.

## Reading and next bounded action

Root reviewed the complete new writer, Control bridge, tests, probe and oracle;
existing store/manifest/readers, model codecs and typed facts/effects; initial
registry and Control owner; affected identity, dictionary, operations, storage
and delivery contracts; and exact pinned RocksDB write/pin APIs. This slice has
no independent agent review. Later archive/read/UI boundaries remain incompletely
inspected and unimplemented. An actual engine test is evidence for its exercised
behavior, not a complete release qualification.

Commit the reviewed endpoint/root/oracle correction, transfer that exact revision
through Git and run the corrected eight-case matrix in a fresh separately owned
Tux lab. Preserve the failed initial lab. Independently audit all native bytes and
Control relations, including actual before/after SIGKILL, then document and normally
push verified work. Advance next to the existing signed-event oracle and complete
atomic event write set; do not substitute further read helpers for admission.

Earlier qualified prerequisites remain recorded in their individual evidence:
[Control runtime](evidence/p01-control-runtime-native-20260918.json),
[owned event verification](evidence/p02-owned-event-native-20260918.json),
[native receipts](evidence/p02-receipt-reader-native-20260918.json),
[initial registry](evidence/p02-ingest-registry-native-20260918.json),
[storage](evidence/p02-ingest-store-linux-20260918.json),
[configuration](evidence/p01-config-inspection-native-20260918.json),
[enrollment](evidence/p08-owner-enrollment-native-20260918.json), and
[trusted shared CI](evidence/p01-trusted-ci-tux-20260918.json).
Their scoped results do not qualify the current correction, complete product,
release, host/power loss, ENOSPC or maximum/aged-state load.
