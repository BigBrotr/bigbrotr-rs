# Initial native dictionary batch experiment

Run only from an exact clean trusted source in the inventoried native Linux lab.
Use the shared `tools/build.py` release build, then
`experiments/ingest-registry/build_probe.py --build BUILD --example ingest_bootstrap_probe`.
Create a fresh owned PostgreSQL lab using `experiments/control-migrations/lab.py`
and run `python3 experiments/ingest-bootstrap/run.py --root LAB`.

The actual client invokes the real Control session, initial registry and native
RocksDB writer. Its policy validator compares the entire frozen configuration
fixture. This is adapter qualification, not the installed root validator.
Seven finite cases cover a two-mapping write, close/reopen/exact replay, cancelled
authority, expired policy deadline, closing producer, mismatched manifest and
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
