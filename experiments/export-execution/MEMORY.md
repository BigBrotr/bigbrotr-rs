# Finite Export memory observation

Finite fixture for one real accepted Export job, not the five-export M1
acceptance or the installed scheduler. The retained6fe setup now has a genuine
acceptance, one successful capture, independent SQL/physical custody and corrected
offline literal verification in the [recovery report](../../docs/implementation/evidence/p02-producer-recovery-native-20260920.json).
Original orchestration and oracle failures remain preserved. It does not change
production configuration, publisher limits or the qualified packaged worker.
Use the exact source/binary identities in that report; a different probe requires
its own build qualification.

The root executor first supplies an isolated, genuinely initialized catalog and
destination, published signed event, authorized accepted exact-ID Export job and
its existing retention pin. This packet does not create any of them. In
particular, the old `adapter_cases.py` seed exports the small `E_public` event;
it is **not** a ready accepted-job seed for the 94,577-byte fixture. Preparing
that genuine job with existing ingestion/publication/login/accept APIs is a
root-owned prerequisite supplied by the separate fresh-setup protocol.

The executable entry point is:

```text
export-execution-probe measure /lab/case.json
python3 experiments/export-execution/memory_run.py /lab/run.json /lab/capture
python3 experiments/export-execution/memory_oracle.py /lab/capture
python3 experiments/export-execution/test_memory_oracle.py /lab/capture
```

The last command checks the unchanged real capture and virtual negative controls;
it never modifies retained files or starts an engine. The second command invokes
the first. Run it as the sole initial process in a
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
`frozen-signed-event` accepts an independently signed/pinned literal. The
[fresh setup](FRESH_SETUP.md#supported-signed-input-boundary-cases) now has two
offline-verified 262,144-byte literals, including the 65,536-node tag-heavy input.
Their real publication and five installed exports each are recorded in the
recovery report's `supported_boundaries_20261020`, with separate source and outcome
reviews. Neither is a near-1-MiB native-row seed; the empirical 256 MiB cgroup
scope does not qualify whole-service sizing or internal-owner high-water.
The separate typed [native-row fixture](../archive-engine/README.md#explicit-native-row-boundary-fixture)
at `1250f0a` now qualifies an escape-heavy complete 1 MiB NativeFact query and
exact `ResourceLimit` refusal one byte beyond, under 64+96 MiB grants/five-second
deadline. Its real publication/custody evidence is in `native_row_boundary_20261020`;
it supplies no installed-job or empirical whole-process memory claim.

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


## Explicit reduced plaintext manager observation

`memory_measure.rs` is a standalone research entry, directly linked by the existing
build owner against the exact Cargo-selected libraries. It imports only `memory.rs`
and applies `forbid(unsafe_code)` to that reused module. Its System allocator
wrapper is the only unsafe source; no Cargo manifest, product lint or product code
changes. Direct dependencies are `bigbrotr`, `bigbrotr_model`, `bigbrotr_protocol`,
`getrandom`, `serde`, `serde_json`, `sha2`, `tokio`, and `tokio_postgres`. The same
source has deterministic `rustc --test` counter checks. Both executable/test
compilation and execution require root qualification and exact artifact pins.

The new executable retains `measure CASE`, so the existing closed run descriptor,
`memory_run.py`, full signed-file oracle and external custody audit remain the
consumers. Select it by its independently pinned executable path/hash. Ordinary
`export-execution-probe measure` and acceptance mode retain their previous output,
80 MiB plus credential catalog charge, and five-second fixture driver drain.
The explicit wrapper selects catalog extra=0 and a two-second driver drain,
matching `prepare_export` and `postgres_command::DRIVER_DRAIN`. It requires the
existing 1 MiB row and 64+96 MiB native profile. Control, query/output/reader grants,
query/action deadlines and failure/output custody remain unchanged.

The descriptor permits only local plaintext. The actual preparation API supplies
the manager charge: 16 MiB plus eight copies of its 65,536-byte password bound
and one-byte overbound sentinel, **17,301,512 bytes**. The independent oracle
checks that arithmetic. This is configured bounded-input accounting, not eight
copies of the observed secret length. TLS/mTLS are outside this profile.

The allocator counts successful `alloc`/`alloc_zeroed`, successful realloc size
deltas and matching deallocations from process startup. Failure preserves the
prior charge. Checked arithmetic sets a permanent invalid flag on underflow or
overflow. No reset, active window, allocation-owner tagging, locks or allocator
callback formatting occurs. Snapshots help publish observed live bytes into the
global monotonic peak, including a pending allocation's peak update. A snapshot
is not an atomic multi-field or quiescent process cut. Deallocation stays charged
until the real delegate returns. System-internal realloc overlap, usable size,
allocator overhead, FFI/native/TLS allocation, children, kernel and physical
RSS are not measured by this counter. The separate existing process/cgroup
observations remain necessary.

In this mode each caller marker and ledger sample adds the closed object
`rust_allocations={scope,live_bytes,peak_bytes,accounting_valid}` with
`scope=whole-probe-rust-requested-live/1`. Additional finite caller markers bracket
catalog preparation, inspection, snapshot capture, explicit rollback, driver join
and grant release. They describe actual outer calls and held options, not worker
interior phases or inferred allocations. The manager grant remains held through
actual driver join. Failed joins and unknown output keep their prior cleanup
semantics; added marker failures do not skip driver cleanup.

After the measurement future, sampler and signal task finish, the wrapper
naturally drops the Tokio runtime. It then takes the final counter snapshot before
formatting the `export-allocation-terminal` record. Runtime destruction may wait
for blocking threads; the existing finite outer timeout retains UNKNOWN if this
cannot complete. No terminal success is fabricated by timed runtime shutdown.
Allocations still live at this cut are counted; zero is not required. Terminal
formatting after the cut is explicitly excluded.

The full capture verifier requires original science/process checks, natural
catalog and Control joins, released grants, exact reduced marker grammar, valid
counters, monotonically increasing peaks within each serialized stream separately,
and a terminal peak covering both streams. Concurrent ledger and main-thread
snapshots are not sorted together by timestamps. Its nested
`manager_allocation_observation.status` is `OBSERVED_WITHIN_MANAGER_REQUEST_BUDGET`
only if the conservative **whole-parent** peak (including runtime, observer,
Control and query decode Rust allocations) is at most 17,301,512 bytes. That
supports a requested-allocation observation for this plaintext fixture; it is
not per-owner attribution, physical memory, universal catalog sizing, installed
scheduler or TLS qualification. A larger whole-parent peak yields
`INCONCLUSIVE_WHOLE_PARENT_EXCEEDS_MANAGER`, with the successful science capture
preserved. Invalid counters, missing terminal/joins, or operation failure cannot
pass. Existing qualification flags remain false.

Offline counter-verifier unit controls use
`python3 -B experiments/export-execution/test_memory_oracle.py --allocations`.
These constructed records do not manufacture a live capture. The seven Rust
counter checks cover preexisting/zeroed blocks, realloc growth/shrink and content,
deterministic allocation/realloc failure, cross-thread lifetime, known concurrent
sum, delayed peak publication, and permanent invalid arithmetic. They are separate
from the real signed-event capture and must pass before interpreting its meter.

The existing oversized function-identity layout fixture supplies a useful early
inspection refusal input. The wrapper already records cleanup joins and the
operation error on this path, then emits `UNKNOWN_OR_FAILED` after runtime drop;
this is not a successful export/capture predicate. A compatible fresh layout
case, independent refusal/drain/SQL/backend audit and real execution remain root
prerequisites. No timed fault injection, proxy adaptation or refusal PASS oracle
is introduced here. A positive run needs a genuinely new Accepted job and pin;
completed jobs cannot be replayed. Previously qualified application/worker assets
may be reused with their original source identity, while this new wrapper must be
built and pinned separately. This source packet claims no new runtime result.
