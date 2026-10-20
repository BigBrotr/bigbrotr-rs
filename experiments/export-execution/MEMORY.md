# Finite Export memory observation

Source-only fixture. Compilation and runtime are **NOT_RUN**. This is one real
accepted Export job, not the five-export M1 acceptance and not the installed
scheduler. It does not change production configuration, publisher limits, or the
packaged `9a7fd81` worker. A fresh `export-execution-probe` build is required.

The root executor first supplies an isolated, genuinely initialized catalog and
destination, published signed event, authorized accepted exact-ID Export job and
its existing retention pin. This packet does not create any of them. In
particular, the old `adapter_cases.py` seed exports the small `E_public` event;
it is **not** a ready accepted-job seed for the 94,577-byte fixture. Preparing
that genuine job with existing ingestion/publication/login/accept APIs is still
a root-owned prerequisite, not accomplished by this source packet.

The executable entry point is:

```text
export-execution-probe measure /lab/case.json
python3 experiments/export-execution/memory_run.py /lab/run.json /lab/capture
python3 experiments/export-execution/memory_oracle.py /lab/capture
```

The second command invokes the first. Run it as the sole initial process in a
new dedicated cgroup-v2 container with a finite memory maximum. PostgreSQL runs
outside that cgroup; the initial profile uses an explicitly permitted loopback
route, for example a separately owned shared network namespace. The fixture
does not start containers. Root must authenticate fresh cgroup/container identity,
source and ELF pins, installed migration/grants, actual before/after SQL, backend
absence and preserved inputs/catalog artifacts in its surrounding audit.

`case.json` is a closed `bigbrotr-export-measure/1` object. Its fields are:

| Fields | Meaning |
|---|---|
| `name`, `dataset`, `instance`, `job`, `event_id` | Actual independent fixture identities; fixed IDs are not authority. |
| `control`, `catalog` | Each has `endpoint`, `database`, `user`, `password_ref` (`file:/…`). The real transport pins the route. |
| `reader_user`, `reader_password_ref` | Actual permanently installed reader, inspected by the catalog adapter. |
| `config`, `policy` | Each has absolute `path`, exact file `sha256`, and independently pinned SQL `stored_digest`. Documents must be <=64KiB and exactly match actual SQL under Control's checks. |
| `data_root`, `state_root`, `destination` | Actual existing bindings. `data_root` is the exact catalog prefix, including its terminal `/`; the acceptance descriptor uses the same convention. Nothing is initialized or repaired. |
| `scratch`, `ledger` | Fresh private empty native scratch and nonexistent ledger filename. Files/failures remain retained. |
| `worker_root`, `worker_bytes`, `worker_sha256` | Exact immutable packaged9a worker. Normal bundle/library/extension verification still runs. |
| `limits` | Closed limits object described below. |

`limits` contains integer `memory_max_bytes`, `baseline_bytes`, `data_bytes`,
`native_bytes`, `control_bytes`, `engine_memory_bytes`, `native_reserve_bytes`,
`row_bytes`, `scan_bytes`, `temp_bytes`, `export_bytes`, `disk_reserve_bytes`,
`query_ms`. Whole envelope arithmetic and actual native floors are checked.
The first runner requires `row_bytes=1048576`, even for the 94,577-byte sample.
The native call uses one thread, at most two rows, scan<=256MiB and query<=4s.
`start_export` still supplies its real <=5s nonrenewable action window, begun
before SQL and checked by `write_with_window`; no deadline is widened.

`run.json` is a closed `bigbrotr-export-memory-run/1` object containing `probe`,
`probe_sha256`, `case`, `case_sha256`, `cgroup`, `science`, `science_sha256`.
These paths are absolute. No hashes, job identities, granted scopes, or candidate
limits are guessed by the fixture. Root supplies and authenticates them.

`science` names a separately frozen `bigbrotr-export-memory-science/1` document:
`event`, complete original SQL `query`, `catalog`, numeric `frontier_revision`,
and `accepted_revisions` (`configuration` and `policy` canonical decimal strings).
The event descriptor is `{kind,path,sha256}`. Initially use
`kind=frozen-archive-recipe` with
`tests/fixtures/protocol/archive-event-response.json`, SHA256
`815bf9e7e0fbcda73a77fbc3f91bebe0ec5c5f356e01bd665f695a8edb5af963`.
The oracle reconstructs its signed literal, independently hashes the Nostr ID,
and compares complete JSONL and manifest bytes. It does not generate signatures.
`frozen-signed-event` admits a future independently signed/pinned literal; this
packet creates no near1MiB or many-tag seed and makes no claim about them.

Resource accounting follows actual owners: Control uses credential copies plus
16MiB+512MiB (`postgres_command::prepare_control_shared`); catalog uses copies
plus16MiB+64MiB (`prepare_catalog_as`); reader and output each retain1MiB. A
disclosed16MiB fixture/controller allowance is separate (not a measured RSS
bound; runtime/thread baseline remains explicitly configured). The read itself acquires
`64*(row_bytes+2048)` parent bytes, engine+native reserve, and128KiB control.
Its native floor is16MiB plus the parent overlap. Admission is checked before
claim; actual grants remain with their owners. Driver/credential/output custody
survives failed futures and known joins. Unknown completion is never retried or
converted into a completed/pause record by this fixture.

Caller markers bracket the whole native query (including spawn, attach, query,
serialization, child reap and parent decode) and subsequent output/SQL completion.
They do **not** expose individual internal phases. The 5ms ledger sampler writes
bounded records without retaining a sample vector. The external10ms observer
records PID/start identity, RSS/HWM, cgroup current/lifetime peak/events/stat and
scratch logical/allocated bytes. Sampling may miss peaks and races spill removal;
cgroup lifetime includes the Python observer, file cache and kernel memory.
No baseline subtraction or peak reset is performed. Fresh-cgroup provenance
belongs to the outer root receipt. Oracle processing runs after measurement.

Configured whole-service reservation, per-export job cap and measured RSS/cgroup
are different quantities. The large Control reservation is disclosed as service
cost, not assigned to the export-job256MiB allowance. A low observed RSS does not
qualify smaller grants, a smaller event envelope, or five completed exports.
`PASS_SCIENCE_AND_CAPTURE` deliberately sets all those acceptance claims false.

On timeout, nonzero exit, forced reap, OOM, missing worker samples, failed driver
join, retained grants, or changed inputs, retain all artifacts. No successful
marker substitutes for the external SQL/backend/container custody audit.
