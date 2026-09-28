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
This recipe is a test definition, not evidence that these cases have executed.
