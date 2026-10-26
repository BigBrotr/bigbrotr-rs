# Current execution checkpoint

2026-10-26, Tux. **Full-product goal ACTIVE:** all selected P00–P10/M1–M5,
under Q37/Q38/Q40. Q41 requires real hosted CI; Q42 records the public repository.
Official releases, packages and production remain unauthorized.

## Current capability and revision

Published `origin/main`: `94a52232`; qualified product `a295da60`, assembly
`55df797a`, acceptance `eff2faaf`. The initial readiness increment is
integrated and pushed; its redundant `work/live-readiness` branch was retired.
The next ordinary Archive readiness increment is in progress: root owns runtime
integration, `m1_runtime_grants` the bounded authoritative expectation bridge,
and `initial_index_rows` the observation cache, in separate writable worktrees.
Local main is `8256184c`: bridge `223a2e08`, cache `63285940`, runtime/input-reader
integration `814479ef`, and actual-file custody controls `450db223`. These changes
are not yet fully qualified or pushed. Native qualification PASS at8256184:
actual build/strict Clippy and147 selected controls (96 library/51 binary), with
33 evidence hashes and all3 retained executables checked; session15316 exited0
and both containers are stopped/PID0/noOOM. Fresh worker-before-parent installed
build is RUNNING as session19968 in `runs/initial-index-installed-8256184`.
Real PG statements and the installed ordinary readiness extension remain NOT_RUN.
Formatting/source review found and
fixed a pin-lifetime gap: Control renewal must not extend the probe's original
pin window. Remaining targeted work includes owned-input custody, actual short-pin
expiry, missing/corrupt input and stale-selection controls, followed by the same
maintained native/installed qualification paths. No new plan or harness is added.

**P01-T09 Step2 is qualified at its initial scope.** Installed product `b5333d53`,
assembly/acceptance `2d0bdc81`, passes all eight maintained
`p01-first-launch-index` stages: fresh migration/bootstrap, signed admission,
Archive and Index publication, ordinary private history, exact signed export and
controlled cleanup. All ten target families, four source batches and 40 outcomes
reach revision4/cut12 under the unchanged four-row per-target ceiling. Independent
signed/C API expectations check typed fields, digests and atomic publication.
Protected captures: `.research/first-launch-index-r4-20261026`; all 19 containers
were stopped/exit0/PID0/noOOM. This is initial publication, not complete M1.
The [existing Index report](evidence/p02-initial-index-clickhouse-sql-20261026.json)
owns the detailed installed result and preserved r1/r2/r3 failures.

**Live readiness source candidate now passes native qualification.** Product
`a295da606aa95093530532d8e576bae1c01992b9`, tree
`437ad02c3f3d8cbfebf81ab543f473416a4b028c`, passes actual product/test builds,
strict Clippy and all 122 selected controls (75 library, 47 binary; none ignored).
Root checked 30 evidence digests, three retained executable hashes and both
terminal containers. Results live under `runs/initial-index-a295da6` in the
existing external lab and the report's `live_readiness` extension.

This increment supplies authenticated observation-only role status through the
existing action/DeliveryGate, a bounded native source head, and coalesced complete
physical checks for the initial Archive/Index owners. Probes run on a 15s cadence
with a strict 30s oldest-read freshness limit; request handling consumes cached
observations. Archive verification in flight suppresses prior Ready through driver
join. Ordinary exact-ID read/export successes remain conservative observations;
they never certify complete current Archive readiness. Nested report timestamps
match authenticated PG time; freshness uses suspend-aware elapsed time.

**The installed readiness extension now passes all eight stages.** Product
`a295da60`, assembly `55df797a`, acceptance `eff2faaf` demonstrate authenticated
positive source/cut/capability observations, then independently introduce one
conflicting event row. Index becomes unavailable with correctly retained remote
query uncertainty; Control and other capabilities remain ready, publication and
session idle fields stay unchanged, and the original Archive history/exact signed
export journey still passes. Protected captures: `.research/first-launch-index-r6-20261026`.
Root replayed the complete typed/source oracle and both status oracles, checked
34 acceptance inputs/migrations/artifacts, and verified all19 terminal containers.

Original r5 remains FAIL: its oracle incorrectly required a known dependency
failure after the extra row rejects before cursor EOF. Critical source review
confirmed that local join cannot imply remote query cessation. The corrected
oracle requires exactly `remote_outcome_unknown`; no product, science, time limit
or uncertainty guarantee was changed. Its14 targeted controls and retained r5
capture replay PASS, followed by the complete real r6. Native122 and31 client
controls/TypeScript/contract drift remain valid. All delegates' changes are
integrated; detailed reviews and original failures are inventory-linked.

Original native failures remain intact: oversized completion enum (Clippy), wrong
Rust module prefixes in the verifier, and dictionary fixture reservations below
the operation's declared requirements. Corrections preserve production ceilings,
all 47 required binary controls and independent source expectations. Retained
executables were used to finish unaffected controls before the corrected full
native qualification. Inventory links retain each original FAIL and NOT_RUN.

## M1 remaining gates

The earlier installed Ingest restart/resend and ordinary read/export journey
passes all 12 stages with provenance on and off on product `c49cfe5c`/candidate
`47d53ce8`; Index was not included. Remaining M1 gates are:

- Complete ordinary full current Archive readiness and remaining clean-install
  negatives; the initial positive/physical-invalidation case is now qualified.
  Terminal lifecycle output remains `ready:false`/Index `not-qualified`.
- Index restart/rebinding and uncertain-query/lost-COMMIT recovery. Initial
  publication and live observation do not close these requirements.
- Independent Archive/CH outages, bounded pending/ACK behavior and live pending
  byte accounting. Archive failure currently exits the combined owning loop;
  independent Control must survive the required Archive outage qualification.
  Existing shared PG topology cannot demonstrate that independence.

Later obligations retain their existing owners: smaller-envelope concurrent
resource pressure in P01-T09 Step5; general valid-event family-bound handling in
P02-T05 Step3/T06; nonempty signed-tag engine coverage, native census, second-event
export and later durability/GC/capacity gates. The prepared column-TTL negative is
NOT_RUN. The original `a99` initial Sync timeout remains UNKNOWN with its original
evidence. There is no all-P02 phase barrier or scope reduction.

## CI and public exposure

[Run40376741278](https://github.com/BigBrotr/bigbrotr-rs/actions/runs/40376741278)
on `1cfd6eb5` passed all 30 selected hosted checks on Linux amd64/ARM64 and macOS.
Dependency-policy and cancellation corrections are qualified at that scope;
historical billing refusal is not a current blocker.

[Run40393162501](https://github.com/BigBrotr/bigbrotr-rs/actions/runs/40393162501)
on `00714653` is terminal FAIL: 17 PASS, 3 FAIL, 10 NOT_RUN. All three platforms
fail the same stale `control_runtime_entrypoint` expected role diagnostic after
Index support. The one-line correction is integrated at `a8b7a8ec`; all other
refusal assertions remain. Fresh builds, Clippy, rustdoc and early dependency
policy pass; subsequent workspace/acceptance/CLI gates remain unqualified.
The [existing hosted report](evidence/p01-ci-hosted-foundation-20261026.json)
records the separate results and timings. Shared frozen source/Cargo/tool/artifact
contexts are verified, but shorter failed runs are not evidence of a speedup.
[Run40406281096](https://github.com/BigBrotr/bigbrotr-rs/actions/runs/40406281096)
is running on the pushed `94a52232`; `ci_native_cancel_review` owns its sole
watcher (session49815). Its terminal result and public-output audit remain pending.

Watcher91567 ended/reaped. Scoped audit verified all four artifact digests,
30 artifact members and 38 log members, with zero credential-pattern candidates
or provider alerts; asynchronous provider scan completion is not claimed.
Incremental source/history audits through `94a52232` found no new or unclassified
credential candidates or confidential archive/store inputs. Receipts remain under
`.research/ci-publication-20261026`; raw captures stay protected and excluded from
public uploads. Provider scan completion is not inferred from an empty alert list.

## Resources and next action

Fresh worker-before-parent build `a295da60` is PASS: source/CLI/worker hashes and
all2365 source files verified. Sessions80251/10299 ended exit0/reaped. Installed
r5 session51261 ended exit1/reaped; r6 session74584 ended exit0/reaped. All owned
containers are stopped/PID0/noOOM (r5/r6 all exit0), with original databases and
captures preserved. Native qualification has released the exclusive115 compiler
cache; only the fresh installed build uses its separate recorded container and
output. `initial_index_rows` is preparing the
ordinary installed acceptance extension; the other source delegate is complete.
The hosted CI watcher remains active. Documentation checks passed all three
selected gates at94a; the new increment is unverified.

The [laboratory inventory](../research/laboratory-inventory.json), primarily
`implementation_index_foundation_20261026.live_readiness`, owns exact resource and
receipt identities. Bounded retirement also removed 1,748 obsolete receipt-cache
intermediates (2,082,344,960 allocated bytes), preserving all 61 ELFs, nine outside
pins and other target entries. Earlier cache and obsolete034 target retirements
remain journaled. Current native/build caches, unique data, original failures and
pending worktrees are retained. No broad cleanup is underway.

Next: continue P01-T09 Step1/6 with ordinary readiness: create a bounded authoritative
bridge from current Archive selection/sealed batches to complete input
expectations for the existing native verifier. Keep actual grants, reader/catalog
custody and driver join through final currentness checks; narrow successful
history/export alone cannot certify a complete cut. Source remains Unknown without
an Ingest owner. No unbounded census or full-P02 prerequisite is implied. Retain
all other M1 and later gates above; FAIL/NOT_RUN/interruption never become PASS.
