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

Nineteen finite scenarios use the actual explicit initial CLI: healthy stop,
response loss before/after Control claim, registry registration, original-input
reservation and native-outcome copy, cancellation at reservation/outcome COMMIT,
and a delayed successful outcome COMMIT for each of bootstrap request1 and
self-scoped private append request2. The existing finite loopback proxy
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
records the original twelve cases on2e2a4e1 with exact7ab155e artifacts. The
[private append report](../../docs/implementation/evidence/p02-dictionary-append-native-20261002.json)
adds the seven request2 cases and nine installed lifetime/window cases onf8015bc.

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

The current independent native oracle requires17 rows and both original inputs/
receipts after private append. Historical session waves below retain their original
12-row bootstrap contract; all Control tables/xmin are retained for comparison.
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
Raw relational and current-authority races have separate evidence below. The
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
writers/raw relational checks have separate evidence below; installed HTTP ambiguity remains.


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


`--registration-relations BUILD` executes16 rollback subtransactions inside the
actual authenticated registration over an existing endpoint. A clone-only hook
first validates the real parent, then checks missing command/detail/audit, endpoint
digest mismatch, audit linkage, old owner, absent/revoked session, all three actor
revisions and immutable update/delete, with valid controls before/after.
A separate actual-Control SQL session checks TRUNCATE outside the active INSERT
so PostgreSQL's table-in-use refusal cannot mask the immutable history guard.
Expected command bytes are independently encoded in Python. All68 Control tables
including xmin must remain identical after each attempt. Actual login also prunes
the old revoked session while preserving its durable receipts, exercising the
intentional absence of a session FK. Only one real new command remains.
[Exact00600af/fixtureda87534 passed](../../docs/implementation/evidence/p02-relay-registration-relations-native-20261001.json)
the16 hook checks and separate TRUNCATE, with unchanged original populated source
and stopped owned resources. No migration or product code changed in this wave.


`--registration-http-faults BUILD` proposes five actual initial-CLI schedules:
nominal acceptance, lost ninth COMMIT before/after forwarding, admitted HTTP
client reset followed by timely COMMIT and exact HTTP retry, and SIGTERM during
held COMMIT followed by explicit proxy close. Eight preceding commits must be
observed after real CLI login; a clone-only server log marker at the actual detail
INSERT proves that periodic renewal did not silently change the fault cut. Logs
identify that boundary only; independent SQL/native state determines the outcome. The last schedule observes the retained upstream backend after CLI death,
then positively retires it before fresh-owner receipt recovery. Every case checks
all68 Control relations,12 original native rows and staged CBOR. The qualified7011acd
native inspector is reused separately from the00600af CLI/SQL probe, with hashes
and all affected build inputs checked. No installed successor is inferred from
fresh adapter recovery; actual reinitialization of the retained store must refuse.
[Native installed fault evidence](../../docs/implementation/evidence/p02-initial-registration-faults-native-20261001.json)
now qualifies four unchanged4db23a0 schedules plus the focusedb61fcee signal case.
Original626cbc3 config setup failure (fixture30s exceeded actual5s authorization)
and47ed56b preliminary output remain retained. Actual-parser negative controls
reproduce partial-header status loss and short fixed-length EOF before correction;
seven corrected regressions pass on Mac and Tux. No product code changed.

`--registration-http-signal BUILD` repeats only the affected cancellation case.
It observes HTTP closure within2s of launch on CLOCK_BOOTTIME, before the earliest
2.5s half-lease deadline, and then joins the CLI before closing the held PG proxy.
The observed closure was1.367s, and signal-to-reap2.141s. This distinguishes processed
signal cancellation from a watchdog timeout or merely transmitting a signal during
driver drain. Four unaffected schedules remain in4db23a0; the focused rerun does not
claim to execute them again. Complete SQL/native originals and source/artifact
identities were independently checked. Installed successor/acquisition/M1 and
capacity/release acceptance remain open.


## Installed immutable acquisition scopes

`experiments/acquisition-scope/bindings.py LAB --installed-scopes BUILD` proposes
seven cases using the exact initial-owner CLI and native inspector. It reuses the
existing isolated clone/lifecycle infrastructure. Populated25→26 and exact reapply
must preserve old rows/xmin/heap files. The actual password-login journey registers
one URL, enrolls private visibility, registers a normalized selector under three
keys (including after withdrawal), then replays each receipt. Every successful
HTTP receipt is compared with its exact SQL command/detail; complete SQL snapshots
require no retry mutation except lease renewal. The independent literal document,
command digests, original attribution and twelve native bootstrap rows/CBOR remain
checked. Policy-change-only authority refuses scope registration; missing INSERT,
candidate/digest EXECUTE and schema refuse before listener bind with no registry
writes. The maximum padded body tests the2098176-byte HTTP envelope, and65536
one-byte chunks test fragmented collection; neither is a maximum canonical-selector
acceptance or production capacity measurement. First062f82d execution passed the
populated upgrade and HTTP requests but failed its final command count: it omitted
the template's original policy activation. The correction preserves the exact old
row and counts only new keys. Original FAIL/captures remain retained; three mutated
receipt controls reject false scope IDs, timestamps and created flags.
Exact5e7dbab/unchangede4c2825 artifacts passed all seven cases; the
[bounded report](../../docs/implementation/evidence/p03-initial-scopes-http-native-20261002.json)
records all69 relations,12 native rows/original CBOR and stopped resources.

Read-only AI fixture review identified unbound HTTP receipts and an overly broad
startup-refusal write allowance; both were corrected before engine execution.
COMMIT faults/current-authority races/raw relational checks and the affected
internal scope adapter regression remain separate required qualification.


`--scope-command-faults BUILD` proposes seven actual authenticated scope-adapter
fourth-COMMIT schedules, borrowing the existing finite proxy/lifetime controller.
The probe's optional final `scope` argument selects the actual scope API; its old
endpoint path remains. Scope expected results are independently encoded in
scope_command_faults.py. Clone the retained5e7dbab scope journey, keep its original
selector/scope attribution unchanged, and require atomic command/detail/audit or
no changes across all69 tables. Same-key recovery under fresh owner3 must return
the original owner2 receipt when committed. Isolated owner/session expiry must
refuse even for an already existing scope. Native Ingest is not adopted/reopened.
The changed shared fixture also requires the seven old endpoint schedules against
the retained45f7593 source. Exact0733326 build/endpoint regression and7104b2d scope
fixture passed all fourteen cases; the [bounded report](../../docs/implementation/evidence/p03-scope-command-faults-native-20261002.json)
retains exact identities and terminal inventories. Current-authority races and raw
relational negatives remain separate work.


`--scope-command-races BUILD` proposes eleven scope acceptance schedules using
registration_races.py with an explicit scope profile. Policy heads, roles.control,
session revocation, account deactivation and deactivate/reactivate each run in
both observed lock orders, plus writer rollback. The permission writer locks the
account and updates grant_revision atomically; arbitrary bare grant DML is outside
this claim. Literal writer effects are compared independently from the complete
scope command/digest oracle. Fresh-owner recovery after changed heads or permission
must return the original receipt without durable changes. The existing endpoint
race profile also has its affected regression. Exact7a22f94/unchanged0733326 artifacts
passed eleven scope and eighteen endpoint cases; the [bounded report](../../docs/implementation/evidence/p03-scope-command-races-native-20261002.json)
records complete state and terminal ownership checks. No product/probe rebuild was needed.


`--scope-command-relations BUILD` proposes twenty rollback subtransactions inside
an actual authenticated scope command, followed by separate actual-Control TRUNCATE.
The retained scope journey supplies one immutable identity; wrong-scope testing
creates a valid alternate selector/scope inside the speculative subtransaction,
with a literal different document. Positive alternate-identity acceptance and
missing-detail/revoked-session negatives require complete69-table rollback.
Independent original/alternate command hashes distinguish semantic mismatch from
an absent FK. Other checks cover exact audit linkage, missing command/detail/audit,
old owner, current session and all actor revisions, missing roles.control and
immutable mutation/deletion, with positives before/after. Direct grant deletion
is clone-only fault injection in the same transaction, not a concurrent supported
writer protocol. Real login prunes historical sessions without removing receipts.
The common controller preserves the existing endpoint profile, whose affected
sixteen checks/TRUNCATE were also rerun. Exact5f956b3/unchanged0733326 artifacts
passed all checks; the [bounded relational report](../../docs/implementation/evidence/p03-scope-command-relations-native-20261002.json)
records original captures and stopped resources. Forced constraint checks qualify
relational closure, not COMMIT-time expiry; the separate fault wave owns that claim.


`--scope-http-faults BUILD` proposes five installed first-scope schedules: nominal
acceptance, lost COMMIT before/after forwarding, accepted client reset/exact retry,
and timely signal during a held COMMIT. The actual password client registers URL91
and enrollment92 before the scope command31, so the positively marked scope INSERT
must belong to COMMIT11 with exactly ten preceding completed transactions. Its
independent oracle requires either all five new selector/scope/command/detail/audit
rows or none, exact canonical bytes/attribution, complete69-table snapshots and
original twelve native rows/CBOR. A fresh SQL owner retries the same saved intent;
it does not reopen/adopt native Ingest. The five-second authority ceiling and
signal/HTTP-closure-before2s assertion stay unchanged.

The shared HTTP controller now explicitly upgrades25→26 and borrows the qualified
e4c2825 native inspector, matching the current0733326 CLI/probe inputs. The old
endpoint HTTP profile also needs five affected regressions at this exact fixture;
its original7011acd/00600af evidence remains historical. Two concrete client/oracle
helpers own scope-specific setup and expected rows; the finite lifetime/proxy stays
shared. Eight local actual-parser checks pass, including receipt-format separation.
Exact7363a12/0733326 CLI/probe and e4c2825 native inspector passed all five scope
and five endpoint cases. The [bounded installed report](../../docs/implementation/evidence/p03-initial-scopes-faults-native-20261002.json)
records all69 SQL tables,12 native rows/original CBOR and source/artifact identities.
Scope signal HTTP closure1.369s precedes the earliest2.5s command deadline;
signal-to-reap2.132s, with upstream custody observed after CLI death. No installed
successor, capacity or full acquisition claim follows.
