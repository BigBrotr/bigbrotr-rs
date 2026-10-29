# Guarded network component laboratory

P04-T02 component execution on native Linux amd64. This runs the actual Rust DNS
stub against independently encoded TCP peers, direct TCP/TLS with actual controlled
peers and fixed certificate identities, and destination policy against literal
expected sets. It does not qualify installed acquisition, recursive
resolver interoperability, WebSocket, current Control grants, overlays or
capacity. Those remain the owning task's practical acceptance.

Tux is the editing and execution host under the current
[delivery workflow](../../docs/engineering/development-and-delivery.md).
The repository and published Actions outputs are public. Review publication inputs
for credentials; retain private captures only in protected ignored storage. Use a
trusted, reviewed candidate commit, never untrusted pull-request code, with a new
`runs/network-<first-seven-commit-digits>/source` checkout under the inventoried
Tux implementation root. Detach at that exact commit and leave the run checkout
unchanged. Invoke from that checkout:

```sh
python3 -B experiments/network-transport/lab.py \
  "$(git rev-parse HEAD)" "$(git rev-parse 'HEAD^{tree}')"
```

The runner checks clean source, inventories containers/mounts/processes/ports and
requires 12 GiB free compiler headroom. This profile creates no archive, so it
does not consume the separate export completion reserve. It exclusively reuses the
verified stopped `network-1154757` compiler/registry caches through explicit nested
container mounts and a nonblocking host lock. Their original test executables
must be retained and hash-verified outside the mutable caches. This avoids another
complete compiler-cache copy for a fixture-only correction. Original source,
results, databases and historical artifacts stay untouched. Locked
dependency fetching uses a credential-free container. Build and all tests run
offline with `--network none`; fixture traffic stays on container loopback.
Containers have the pinned laboratory image, two CPUs, 6 GiB memory, 512 PIDs,
an unprivileged user, read-only root/source and no Docker socket or host ports.

The shared `tools/ci.py` executor bounds and joins commands. The exact test
executable is retained and hashed outside compiler caches; source hashes are
checked before/after. This correction profile requires 12 direct TCP/TLS tests,
zero failed/ignored, and Clippy. Revision1154757 already executed the unchanged
32 DNS/policy tests successfully; its direct suite retains the Linux split-response
BrokenPipe fixture failure. The corrected one-write response must still reject the
old missing-poll-check interpretation. Counts bind this fixed fixture
set; they are not coverage targets. Raw command outcomes, terminal container
metadata and preflight stay under the run; original failures are preserved.
`out/qualification.json` is the component result, not a product release manifest.

The `initial-index` profile uses the same owned cache, pinned image, source export,
offline container, resource limits and terminal inventory checks. Its run directory
is `runs/initial-index-<first-seven-commit-digits>`; pass `initial-index` as the final
`lab.py` argument. It builds the actual `bigbrotr` library and CLI with the selected
locked dependencies, compiles separate actual library and binary unit-test
executables and runs strict Clippy for the library, CLI and test targets. This
includes test-code lints that the earlier lib/bin-only invocation missed; the
hosted workflow still checks every workspace target. No source
substitution or signature stub is used.
The separate Archive worker is neither built nor bound by this diagnostic.

The retained library executable lists its compiled tests before selecting
`storage::clickhouse::`, `storage::indexer::`,
`storage::postgres::control_owner::index_publication::` and
`storage::postgres::transport::route_tests::`. Every expected owning module must
be present. That route group requires four exact controls, including a real
refused loopback connection versus a server authentication error, and refusal to
classify timeout/reset/other errors as the narrow connection-refused outcome.
Two existing Archive transport-custody controls also cover the shared
future-before-guard destruction boundary changed by late Index attachment.
The library selection also covers native source-head observation and its signed
admission/reopen boundary, bounded role DTOs and suspend-aware elapsed time. It
includes the original input codec, actual input-file custody (including an owned
thread handoff and independent lock competitor), and retained/current-selection
Archive reconstruction controls.
The closed native source-closure group requires complete original-request proof,
preservation of native bytes, signed historical batch reads after acknowledgement,
and refusal of incomplete, changed or foreign source state.
The five existing lineage-pending controls cover the successor source accessor:
complete head without a prepared gap, exact original batches, stale native/input
custody, out-of-cut identities and unchanged grant/cancellation bounds. The
Index publication selection also requires the typed handoff control module.
The separate binary executable must contain the closed service scope declared
in `initial_index.py`: Index actors/preparation, physical-probe scheduling,
explicit initial CLI selection, command admission and supervision, watchdogs,
authenticated role HTTP and ordinary observation-cache controls. It also includes
finite catch-up cursor, exact publication receipt, explicit topology handoff,
global request/bridge bounds and recovery-capacity controls. The closed Sync command/diagnostic scope requires
retention of the same prepared signed input and grant across safe-point renewal,
pre-REQ reauthorization, cancellation/revalidation refusal, refusal before an
expired operation's first poll, and preservation of a started admission's deadline
and unknown outcome. Ordinary initialized renewal controls collect the same
in-flight stage and grant on stop under one absolute chain deadline, refuse
later stages and dispatch, and retain startup/external/successor fail-stop
selection. An expired original accepted-policy window cannot be replaced by a
new tracker refresh. These controls require actual Rust execution; the Python
verifier controls alone do not qualify renewal or shutdown. Diagnostic progress
cannot certify a commit. The binary
scope contains 104 required names, including pre-effect finite-work admission,
fair deferral, funded ACK completion and Archive publication/capture, plus parked Archive status retaining its
last published cut or explicit absence. The library selection is combined
with its actual compiled inventory, never inferred from this source count. The
closed library reservation/DNS scope checks atomic parent funding, fixed limits,
exact retained-result credit, FIFO admission, positive drain and the unchanged
shared DNS connection gate. These controls do not qualify installed concurrent
Sync/Archive/Index pressure.
The capacity controls retain denial/admission identity through watch coalescing,
explicit overflow and record limits, and actual terminal/cancellation-grace join.
They do not turn local result consumption into remote query exclusion.
Idle nomination, retained selected work, fresh Archive polls and post-join
readiness-credit retry are separately covered; the readiness cache control reads
an actual empty Rocks source but supplies no ClickHouse verification authority.
Only the Sync, Archive and Index groups use `--show-output`: each emits one bounded
`bigbrotr-initial-budget-requirements/1` JSON record with compiled finite-profile
requirements. These source-bound test logs inform the separately declared
installed profile; they do not establish a minimum process envelope or capacity.
Ordinary cache
tests distinguish a complete current proof from a narrow operation, retain the
oldest dependency age, refuse historical selection promotion and preserve failures
through retry or incomplete cleanup. These observations do not qualify live dependency
readiness, PostgreSQL authorization transactions or installed failures. Names
and counts are declared from the owning source; missing, renamed or additional
tests within that scope require a reviewed inventory update. Both executables
are listed before execution, and every group records which executable ran it.
Executed names and counts must match their inventory with zero failed
or ignored tests; an empty filter cannot pass. The copied CLI's `build-info` must
match the source/tree/manifest and input hashes. Exact binary/test bytes, selected
test names, command logs and result hashes remain outside the mutable compiler
cache. `PASS_INITIAL_INDEX_APPLICATION_COMPONENTS` certifies only these checks:
actual ClickHouse/PG operations, installed recovery/outage behavior, the complete
test suite, release packaging and capacity remain separate gates. A running owned
engine lab must finish before this runner's existing exclusivity checks can pass.

The verifier controls can run without Cargo or Docker:

```sh
python3 -B -m unittest discover -s experiments/network-transport -p 'test_*.py'
```

The `websocket` profile runs the supplied-stream WebSocket integration suite and
the two private input-buffer tests. `relay-wire` runs only the preserving decoder's
eight tests and the new signed WebSocket corpus, compiling the actual protocol and
application crates. It checks changed content, a forged same-ID signature, an
alternate valid signature, duplicated signed fields and retained EOSE hints. The
peer independently decodes outgoing masked REQ and Close; no AUTH response is sent.
Both profiles use their name as the run-directory prefix and final lab.py argument.
Neither qualifies installed Sync, source-bound admission or completed coverage.

The `selector` profile runs eight collection-identity/membership tests, four
strict selector-codec tests and both signed socket corpora. Frozen signed events
separate local ephemeral exclusion from a request-window violation; the peer
checks the generated masked REQ against an independent literal. Three independent
fingerprint vectors include multi-filter typed ordering. The profile qualifies
no persistent scope registration, installed Sync, coverage or capacity.

Both created containers finish stopped-retained; no pruning or volume deletion
occurs. Review source/dependency/artifact identities and original logs before
publishing a concise evidence report and updating the existing inventory.

`request-session` runs only the five actual-socket request-generation tests and
two bounded encoder tests plus scoped Clippy. It retains both exact release test
executables. Independent peers observe masked REQ/CLOSE, stale and concurrent
attribution, poisoned-session refusal and socket closure. No PostgreSQL, dataset
or archive is modified, and the profile claims no installed Sync or coverage.

`observation` uses the existing registry builder to compile the CLI and actual
Control probe, run six RocksDB, three clock and five socket cases, and retain the
two exact test executables outside the shared compiler cache. The separate
`acquisition-scope/bindings.py <root> --observation` engine wave borrows the owned
stopped PG laboratory, creates six clones and compares complete native/input/
receipt bytes against independent literals. This is attribution/read qualification,
not socket admission, late rollback delivery, general dictionary allocation or M1.
