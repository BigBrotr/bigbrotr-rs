# Initial native dictionary batch experiment

Run only from an exact clean trusted source in the inventoried native Linux lab.
Use the shared `tools/build.py` release build, then
`experiments/ingest-registry/build_probe.py --build BUILD --example ingest_bootstrap_probe`.
Create a fresh owned PostgreSQL lab using `experiments/control-migrations/lab.py`
and run `python3 experiments/ingest-bootstrap/run.py --root LAB`.

The actual client invokes the real Control session, initial registry and native
RocksDB writer. Its policy validator compares the entire frozen configuration
fixture. This is adapter qualification, not the installed root validator.
Eight finite cases cover a two-mapping write, close/reopen/exact replay, cancelled
authority, expired policy deadline, closing producer, mismatched manifest/root and
actual SIGKILL before native work / after commit before receipt delivery. Each
case has its own database and native directory. Owned crash containers and data
remain retained. The harness never removes an unrelated resource.

`oracle.py` independently assembles every expected key and value using explicit
CBOR and SHA-256, parameterized only by the dataset read directly from PostgreSQL.
A separate raw native process dumps the store after the writer exits. The
complete state, including receipt, fact preimages, inventory, counters, manifest
and pending digest, must match. All thirty Control tables are retained for
external inspection; only the initial registry/owner tables may change.

These are process crash experiments, not power-loss/host-failure evidence. They
do not inject errors inside FFI, exercise ENOSPC or prove a successor can resume
the old owner's authority. The report lists these and other unexecuted cases.
Missing infrastructure, an empty dump or a failed harness is not PASS.

## Connected signed import

`run.py --suite event --root LAB` exercises seven separately initialized native
cases through `ControlSession::admit_initial_event`. The source is the exact
independently signed `tests/fixtures/ingest/import-event.jsonl` artifact. The
registered initial producer first bootstraps visibility and lineage dictionaries,
then admits the import at request sequence two. The finite workload fits its
two-request window; it does not implement or qualify ACK/window progress.

`event_oracle.py` independently assembles all sixteen bootstrap rows or all
twenty-six committed rows, including exact event/payload/eligibility, request and
batch bytes. A normal case retries under the same owner; a separate read-only
native process reopens the directory for complete inspection. The before/after
process kills occur around event dispatch and external receipt delivery, after
bootstrap. Source-close, cancellation, expired lease and configured-root controls
must leave exactly the bootstrap state. The application validator checks the
complete frozen policy plus this exact source, scope, lineage and verification
limit; it is not an installed source-policy evaluator. The full signed semantic
fixture is separately exercised by the native Rust writer tests.

## Committed visibility reads

`experiments/network-transport/lab.py REVISION TREE ingest-visibility` reuses the
inventoried exclusive compiler cache, builds the actual CLI/ingest bootstrap probe,
runs five focused native tests and Clippy, then invokes `visibility.py LAB BUILD`.
The exact-Git owner inventories processes, containers, mounts, ports, networks and
space before allocation, and verifies all prior container projections afterward.
The fixture requires an unused isolated 172.16.240.32/28 subnet; it refuses overlaps.

Five fresh PG databases/native stores exercise the connected visibility reader:
public/private/absent descriptors with bounded-limit retry and reopen, expired
lease, cancellation, policy validator refusal and closing producer. Before/after
observations include all 64 Control tables with xmin, complete native rows/sequence
and original producer-input files. The existing independent oracle checks native
dictionary facts/indexes/counters/manifest/pending bytes; receipt identity is
preserved by the full-state comparison, not reclassified through an older codec.
Whole-process exit and zero remaining probe backends are required after driver
join. The report has 31 explicit checks; it does not qualify acquisition grants,
installed runtime, mid-FFI cancellation, policy transitions, load or arm64.

## Installed first-lifetime SQL interruption

`experiments/acquisition-scope/bindings.py LAB --installed-commits BUILD` runs
`commits.py` against fresh clones of the inventoried empty source template. It
reuses qualified CLI/probe artifacts only when every changed source path is
inside docs/experiments; Rust and all build inputs must remain identical.

Twelve finite scenarios use the actual explicit initial CLI: healthy stop,
response loss before/after Control claim, registry registration, original-input
reservation and native-outcome copy, cancellation at reservation/outcome COMMIT,
and a delayed successful outcome COMMIT. The existing finite loopback proxy
records exact forwarded/server-complete/client-complete counters. Each held
transaction has an independently observed backend/session advisory lock and an
actual competing CLI refused as Busy. Cancellation additionally observes that
the server transaction can outlive the terminated client until the proxy closes.
A closed client socket is never reported as server rollback or exclusion.

All 64 tables/xmin, original CBOR inputs and complete RocksDB rows are checked.
The literal codec oracle uses SQL identities; only the opaque random recovery ID
is extracted from the original manifest, whose remaining bytes are independently
reconstructed. Partial initialization is preserved and existing native targets
refuse fresh initialization before another Control claim. Registry-only cuts
retain their state without claiming a crash-resumable initializer. Renewal cuts,
worker panic, successor authority, full M1 and capacity remain separate work.
The [executed SQL wave](../../docs/implementation/evidence/p01-initial-ingest-commit-native-20260928.json)
records all twelve cases on2e2a4e1 with exact7ab155e artifacts.

## Installed initial private sessions

`experiments/network-transport/lab.py REVISION TREE initial-ingest` builds the
actual CLI/native probe and runs the changed initial lifetime, HTTP profile and
CLI entrypoint tests. `experiments/acquisition-scope/bindings.py LAB
--installed-sessions [BUILD]` then uses fresh isolated template clones. Optional
BUILD reuses exact artifacts only when every intervening change is in docs or
experiments; product/build input changes require a new build.

The three-case first wave exercises actual access login/logout and
incorrect credentials, twelve concurrent self-inspection clients for eight
seconds across the original scheduling expiry, shared-payload refusal before
listener binding, and SIGTERM during an observed running password worker. The
last case observes a newly created second Tokio blocking thread consuming CPU;
the first blocking slot is already held by the native owner. It briefly stops
that exact process, queues SIGTERM and resumes it, then requires empty HTTP
delivery, no session issuance, actual reaping and ordered graceful release.

The independent native oracle still requires exactly the original twelve rows,
bootstrap input and receipt; all Control tables/xmin are retained for comparison.
The disabled startup administration seed deliberately differs from the two
accepted enabled documents. The old `policy-admin` refusal fixture is superseded
by this selected behavior; the headless first-lifetime matrix now uses a selected
archive-reader policy to test unsupported consumers. Historical outcomes retain
their original source and meaning. The [executed first wave](../../docs/implementation/evidence/p01-initial-sessions-native-20261001.json)
records product23e1923 with successful cases on fixtures4a00f64 and0ac54d1, plus
all original fixture failures. Hash-only retries use `--installed-sessions-hash`
with the same build-reuse restriction; they do not rerun the already passed cases.

`--installed-sessions-faults BUILD` runs the separate
[executed fault wave](../../docs/implementation/evidence/p01-initial-session-faults-native-20261001.json).
An isolated clone's trigger delays the first Control renewal three seconds within
five seconds of request-start authority. A disconnected running hash must leave
no session; a subsequent real login/logout proves the slot can be reused. The
close case observes an accepted process-owned partial HTTP socket, then holds the
actual native destructor: HTTP must close while Control still excludes a competing
owner, followed by bounded process death. Complete native/input/SQL oracles remain
in force. No product build was repeated for these fixture-only changes. Worker
panic, renewal-specific ambiguous COMMIT, successor recovery and capacity remain
separate acceptance work.

## Installed initial relay enrollment

`--installed-enrollments [BUILD]` uses the real initial CLI/session dispatcher and
existing enrollment adapter. Isolated actual registry setup registers one endpoint
before startup; this is explicitly not authenticated URL registration evidence.
The three cases cover enable/retry/withdraw/historical retry with complete SQL
snapshots, public-policy intersection/private roots, principals.manage-only refusal,
and missing table INSERT before bind. Complete bootstrap native/input/receipt
oracles remain independent. Product438067f/fixture17c6ea0 passed this bounded wave.

`--installed-enrollment-functions BUILD` tests each required EXECUTE privilege
separately, followed by the permitted journey: route validation, enrollment digest
and its uint64 encoder. Each missing-privilege case must refuse before binding without command/session effects; actual native initialization
is preserved. On438067f, fixture9c7f75c detected the missing route-function probe:
the listener initialized despite revoked EXECUTE. Original failure and graceful
cleanup are retained; remaining two cases were not reached in that failed run.
Corrected917c76b passed all three refusals and the valid journey. The
[bounded report](../../docs/implementation/evidence/p02-initial-enrollment-http-native-20261001.json)
records both waves and the original failure. No whole-step, M1 or release claim
follows from these cases.

## Installed authenticated URL registration

`--installed-registrations BUILD` uses the actual initial CLI, session dispatcher
and additive0025. The frozen20 template and populated917c76b enrollment clone
are copied before any mutation. Explicit CLI upgrade/reapply must preserve every
old row/xmin/heap filenode, allowing only the declared new nullable columns; no
browser attribution is backfilled. Historical session/enrollment modes above
retain their recorded source/binaries and schema prerequisites.

The new client first registers a normalized URL, retries its original key, accepts
that identity under another key, and registers an8192-byte escaped-query URL whose
minimal JSON is16436bytes. It then enrolls and withdraws the new endpoint and
replays all original receipts with complete SQL snapshots. Additional cases cover
principals.manage-only refusal, missing detail INSERT, candidate/digest EXECUTE
and missing migration before bind. All native bootstrap bytes/original CBOR and
receipts remain independently checked; registration itself writes no native facts.
The host oracle uses literal expected URLs and independent command hash bytes.

Product7011acd/fixture45f7593 passed all seven cases; the [bounded report](../../docs/implementation/evidence/p02-initial-registration-http-native-20261001.json)
records exact artifacts, complete retained-state review and stopped resources.
The native build alone does not qualify SQL. The fault consumer below separately
qualifies deferred expiry and lost/cancelled COMMIT for an existing endpoint.
Raw relational negatives remain; current-authority races have separate evidence below. The
initial installed lifetime cannot survive a changed accepted policy; historical
retry across changed heads belongs to a fresh adapter owner until successor
recovery is implemented.


`--registration-faults BUILD` uses a separately compiled finite actual registration
adapter probe and the existing bounded PostgreSQL wire proxy. Seven proposed
cases select exactly the fourth COMMIT: pass, loss before/after completion,
cancelled hold closed/released, owner expiry and session expiry. The retained
installed registration journey is cloned with its existing endpoint; every command
must leave that original identity unchanged. All68 Control tables/xmin, independent
literal digest/receipt and real Busy contender are checked, followed by fresh-owner
recovery. The common host inventories/stops owned resources and preserves the
original populated source. Exact00600af/fixturec54c6bd passed these seven schedules;
see the [bounded fault report](../../docs/implementation/evidence/p02-relay-registration-faults-native-20261001.json).
The first cc1a4e8 probe had a source-reviewed v1/v2 fixture incompatibility; its
build-only result remains retained, with no engine executed on it. Concurrent
writers have the separate evidence below; raw relational and installed HTTP ambiguity remain.


`--registration-races BUILD` first repeats the seven COMMIT cases affected by the
shared probe helper extraction, then exercises five current-authority writers (policy heads,
dataset permission, session revocation, deactivation and deactivate/reactivate)
in both positively observed lock orders, plus writer rollback. It reuses the same
exact registration probe. Expected writer changes are literal and all68 relations/
xmin remain checked; a committed acceptance keeps its original actor revisions.
Policy/permission writers after acceptance also require exact historical recovery
under a fresh current owner, without new command or domain writes.
Exact00600af/fixture41d508a passed all18 schedules; the
[bounded race report](../../docs/implementation/evidence/p02-relay-registration-races-native-20261001.json)
records the11 new cases and7 affected regressions. Privileged
clone-only writers are fault injections, not installed administration workflows.
