# Current execution checkpoint

2026-10-26, Tux. **Full-product goal ACTIVE:** all selected P00–P10/M1–M5,
under Q37/Q38/Q40. Q41 requires real hosted CI; Q42 records the public repository.
Official releases, packages and production remain unauthorized.

## Current capability and revision

Canonical checkout: `work/live-readiness`; source/acceptance candidate through
`1ae90220`. This checkpoint commit records native qualification, terminal hosted
results, bounded cache retirement and the acceptance instructions. `main`/`origin/main` remain at qualified
`00714653` in `.research/assembly-fixture-canonical-20261026`. The new readiness
increment is committed locally but has not been merged or pushed.

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

The maintained installed case now includes positive role observations and one
independent conflicting event row. It must detect Index unavailability while
Control and the original Archive journey remain usable. Acceptance source
`1ae90220` integrates author `04ce548c`; 14 targeted oracle/refusal controls and
critical review PASS. Actual positive/negative installed transition is NOT_RUN.
31 client controls, TypeScript and contract drift also pass at the recorded source.
Protected source reviews are linked from the inventory; both implementation and
negative-acceptance delegates have finished, with their changes integrated.

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

- Execute the current installed positive/negative live-observation case, then
  complete ordinary full current Archive readiness and clean-install negatives.
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
A new coherent publication must complete the changed CI path on GitHub.

Watcher91567 ended/reaped. Scoped audit verified all four artifact digests,
30 artifact members and 38 log members, with zero credential-pattern candidates
or provider alerts; asynchronous provider scan completion is not claimed.
Source publication audit through `00714653` remains valid; the new increment must
be checked before push. Keep raw captures protected and out of public uploads.

## Resources and next action

Fresh worker-before-parent build is RUNNING, session80251, root
`runs/initial-index-installed-a295da6`, container
`2636be56c57987784fcff5fb7f13cd9f3e36856fdd453c003c15c969357658bd`.
It uses a separate fresh Cargo home/target with the existing pinned native assets,
2 CPUs/6 GiB/512 PIDs, 65 GiB minimum free space and 40 GiB output ceiling.
Native session10299 ended exit0/reaped; its owned containers are stopped/PID0/
noOOM. No installed engine case is active. No delegate owns an active build,
engine or GitHub watcher.

The [laboratory inventory](../research/laboratory-inventory.json), primarily
`implementation_index_foundation_20261026.live_readiness`, owns exact resource and
receipt identities. Bounded retirement also removed 1,748 obsolete receipt-cache
intermediates (2,082,344,960 allocated bytes), preserving all 61 ELFs, nine outside
pins and other target entries. Earlier cache and obsolete034 target retirements
remain journaled. Current native/build caches, unique data, original failures and
pending worktrees are retained. No broad cleanup is underway.

Next: collect and verify the fresh `a295da60` worker-bound build; assemble from a
clean checkout containing `1ae90220` and execute `p01-first-launch-index`, including
the positive/physical-invalidation status checks. Reuse the native qualification;
an acceptance/documentation edit alone does not require another product build.
Record the real outcome, classify any failure, and publish the coherent verified
increment to main with the known CI fixture correction. Then collect the actual
new hosted run while continuing the remaining M1 gates above. Never turn FAIL,
NOT_RUN, interruption or missing prerequisites into PASS.
