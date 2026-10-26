# Current execution checkpoint

2026-10-26, Tux. **Full-product goal ACTIVE:** all selected P00–P10/M1–M5,
under Q37/Q38/Q40. Q41 requires real hosted CI; Q42 records the public repository.
Official releases, packages and production remain unauthorized.

## Current increment and revision

P01-T09 initial Index is on `work/initial-index` in the canonical checkout,
HEAD `f7d6befe` before this hosted-evidence checkpoint. Runtime wiring is complete
at its initial-only scope: explicit `--initialize-index`, accepted-policy checks,
actual PG driver custody, bounded physical operations, serialized renewal/timer
work and joined shutdown. The first real installed attempt exposed a write-adapter defect described below.

Exact source `4404f94c` passed actual library/CLI build, both test executable
builds, strict Clippy and all96 selected controls:65 library and31 binary service
controls. Root verified all22 evidence digests and the three executable hashes.
The qualification is retained under external
`runs/initial-index-4404f94/out/qualification.json`; the inventory owns full paths,
source/tree, digest and resource status. These results precede the current write-adapter correction; unaffected evidence is
reused, while the changed physical write path needs a new build and engine run. This diagnostic binary
has no bound Archive worker and cannot qualify an installed journey.

Critical runtime review found and corrected disabled-Indexer bypass, permanent
resource wait under a640MiB shared payload, and a metadata row cap incorrectly
tied to insert batch size. Native behavior now passes; the maintained installed
case now uses4-row batches to exercise the160-column metadata inspection
through the actual Rust adapter. Original failed builds remain preserved:
`fadc6bbd` import failure, and `479d649c` synthetic fixture compilation failure
(E0277), with their tests/Clippy NOT_RUN. Neither failure is rewritten as PASS.

The CI reuse candidate and its independent review correction are integrated as
`74337306`/`2c3e5071`: early dependency policy, fresh worker-before-parent release
build, then complete default/debug Clippy, rustdoc and workspace tests using the
same run-local source/Cargo home/target. Source, native tools, copied artifacts
and effective Cargo configuration are checked around commands. An undeclared
ancestor Cargo configuration could change test semantics; that finding is fixed.
Merged21 CI controls and3 noncompiler builder controls PASS. Native execution,
platform confirmation and timing benefit for these changed helpers remain open;
run40376741278 uses the earlier helpers and cannot qualify this change.

The maintained installed case is integrated as `36df8031`/`14929744`. All48
first-launch controls pass on the merged base. Critical review found an omitted
current consumer-head transaction check; its narrow correction passes12 affected
Index controls, retaining the reproduced original gap. The independent review
confirms the corrected boundary, full81-table snapshot and independent10-family
expectations. The broader ambient Python run has113 PASS, one missing-llvm-config
environment error and two declared strace SKIP; it is not a complete PASS.

The required fresh ordinary build at `03464fcf` is **PASS**: current worker built
before the parent, source/manifest and all2348 exported files verified, copied
binary/worker hashes bound. The separate actual candidate `config validate` also
passes the new Index recipe. Both containers are stopped/exit0/PID0/noOOM;
monitor27067 ended and was reaped. These are build/static checks, not installed
publication. No product rebuild is needed for the later acceptance-only changes.

Actual installed run `.research/first-launch-index-r1-20261026` failed before
engine startup because the generated Compose extension omitted its source file.
The corrected relocated file reference passed actual Compose validation, with
unchanged inherited resource limits and credential mounts. Run `r2` on product
03464fcf/acceptancecb900b4a passed migrations41/bootstrap and fresh CH installation.
Archive reached revision4/cut12, but Index prepared its first plan and parked:
`Client::insert` escaped the already-qualified table name as one identifier;
ClickHouse returned Code60 UNKNOWN_TABLE during DESCRIBE. The signed client
expired its existing gate; this is a product adapter defect, not grounds to extend
timeouts. Index publication and later read/export stages are not qualified.

The correction uses the driver's validated unescaped API with the existing closed,
quoted database/table identifiers. DESCRIBE receives its exact column-count limit,
then the insert restores the accepted data ceiling. Independent critical source
review found no new injection, limit, query-ID or custody issue. The corrected fresh worker-bound build is active at exactf7d6befe; engine
execution is pending. The same maintained case now tightens data to4 rows, the
independently derived maximum family count in a source batch, while keeping all
scientific expectations and timeouts unchanged. Its12 affected controls PASS.
Original failures, both databases and inputs are retained; all16 r2 containers
are stopped/exit0/PID0/noOOM, cleanupPASS, and the borrowed bridge is unchanged.

The [laboratory inventory](../research/laboratory-inventory.json) owns detailed
resource/evidence locations under `implementation_index_foundation_20261026`,
`implementation_first_launch_maintained_20261026`,
`implementation_ci_publication_20261026` and
`implementation_ci_assembly_fixture_20261026`. Preserve original failures,
retained inputs and pending worktrees; component/SQL results are not full product
qualification.

## M1 capability and remaining gates

Installed signed-event acquisition, Archive, private history/exact353-byte export,
Ingest restart/succession/resend, Archive catch-up/exact retry and successor
history passed all12 stages in both provenance profiles on product `c49cfe5c`,
candidate `47d53ce8`. Existing receipts are
`.research/first-launch-restart-r6-20261026/result.json` and
`.research/first-launch-restart-off-20261026/result.json`. Their26 owned containers
per profile are stopped/exit0/PID0. P01-T09Step4 is qualified at that scope.

The next M1 gates are:

- Actual signed-source Index publication through Rust, PostgreSQL and ClickHouse,
  with independent all-family rows, source cut, plan and engine receipt checks.
  SQL-only schema41 qualification passed31 cases on real PG16.15, but direct SQL
  migration application does not qualify the product CLI or Rust transactions.
- Connected truthful readiness and clean-install negative readiness. Index
  restart and remote uncertain-outcome recovery remain unimplemented/unqualified;
  an initial-only successful publication does not close these gates.
- Independent Archive/CH outages, bounded pending/ACK behavior and live pending
  byte accounting. Control must remain available during the Archive outage;
  the existing shared PG topology cannot demonstrate this independence.

The [ClickHouse SQL report](evidence/p02-initial-index-clickhouse-sql-20261026.json)
records successful selected26.8.35.10 schema/catalog/grants/row/concurrency checks,
original DDL and verifier failures, and the still-NOT_RUN column-TTL negative.
All three servers are stopped. The corrected TTL fixture is preserved; no actual
Rust HTTP/publication/recovery result is claimed from those SQL checks.

Later obligations stay in their existing tasks: smaller-envelope concurrent
resource pressure in P01-T09Step5, general valid-event family-bound handling in
P02-T05Step3/T06, and later durability/GC/capacity gates. The small M1 fixture does
not qualify the complete ingestion envelope. Nonempty signed-tag engine coverage,
second-event export and native census remain open. The original `a99` initial
Sync timeout remains UNKNOWN with its original evidence. Do not treat all P02 as
an indivisible prerequisite or erase these deferred obligations.

## Hosted CI and public exposure

Integrated foundation is published on `main` at `1cfd6eb5`, in
`.research/assembly-fixture-canonical-20261026`. Its documentation checks pass3/3.
Product-source review through `a8423d76` and final documentation delta through
`1cfd6eb5` found no confirmed exposure; protected receipts are inventory-linked.
Runtime and CI-helper commits after that publication remain local/unpublished.
Their source/history delta through03464fcf has no confirmed exposure in the bounded
audit; that revision also passes its three documentation checks.

[Run40376741278](https://github.com/BigBrotr/bigbrotr-rs/actions/runs/40376741278)
uses exactly `1cfd6eb5` and is terminal **SUCCESS**: all30 selected checks PASS
across the three platforms, including all65 initial Index library controls.
Separate documentation and optional native ARM experiment SKIP under existing
rules. Watcher69669 ended/exit0 and was reaped. The bounded4-artifact/513-member,
34-log audit found only the same five synthetic/example fingerprints and zero
observed provider alerts; the detailed existing hosted report is updated.
This run does not qualify later runtime wiring or changed CI reuse helpers.

[Run40363410510](https://github.com/BigBrotr/bigbrotr-rs/actions/runs/40363410510)
on `f1f6d166` completed all30 selected checks PASS across Linux amd64/ARM64 and
macOS. The [hosted report](evidence/p01-ci-hosted-foundation-20261026.json) owns
platform/test counts, skips, timings and audit details. Dependency policy and
cancellation corrections are confirmed at this scope. The earlier macOS linked
fixture failure is fixed and verified; its original
[fixture evidence](evidence/p01-ci-assembly-fixture-20261026.json) remains.
Billing refusals are historical, not a current finding.

Early dependency policy is measured at ordinal4 after119.312seconds of preceding
recorded checks, versus ordinal12 after3945.362seconds in the earlier run. This
is not a whole-run or isolated causal speedup. The latest completed public audit
covers4 artifacts/507 members and34 log members:54 matches are five known
synthetic/example fingerprints, with no operational credential confirmed and zero
provider alerts observed. It is a bounded audit, not proof of exhaustive absence
or asynchronous provider scan completion.

## Resources and next action

Native sessions62389 (FAIL) and5554 (PASS) ended and were reaped; both runs'
containers are stopped-retained, with unrelated inventory unchanged. No owned
engine or build is active. The fresh build and offline configuration containers
are stopped-retained in external `runs/initial-index-installed-03464fc`.
Root session77182 monitors the corrected candidate build in external
`runs/initial-index-installed-f7d6bef`; its single owned container is active under
the recorded CPU/RAM/PID/disk/deadline limits. No engine is active. All delegates
completed their scoped work; worktrees and receipts are retained.
Root owns integration and all native/lab execution. Q37 settings remain as verified.

The earlier bounded cache retirement removed4446 obsolete compiler intermediates
and observed3,823,820,800bytes of additional free space. All11120 nonselected
entries and35 outside pins were checked. Inventory
`archive_read_headroom.checkpoint_20261026` owns its manifest/journal/reconstruction
procedure. No dataset, executable, source, worktree or container was removed;
never replay that deletion or incrementally resume the partly retired caches.
Latest post-native free space is154,385,682,432bytes. Check headroom before the
next significant allocation; no broad cleanup is needed to unlock this increment.

Next: verify the active corrected current-source worker-bound build, assemble it with
the same maintained journey, then execute signed-event admission → Archive and Index publication →
independent read/export checks with the4-row accepted policy. Preserve failure
classification and original captures. Exercise the pending real TTL rejection
and independent outages at their relevant acceptance boundaries; do not claim
restart/readiness from initial installation. Observe hosted CI to its terminal
outcome and group the next publication around this coherent capability.
