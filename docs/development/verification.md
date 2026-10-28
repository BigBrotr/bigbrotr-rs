# Practical implementation verification

Run the shared foundation CI commands on a clean committed checkout with
`python3 tools/ci.py --output .research/ci`. GitHub Actions invokes the same
entrypoint. A deliberately provisioned trusted Tux container can execute it
without hosted runners; see [the runner boundary](../engineering/development-and-delivery.md#trusted-tux-execution-when-hosted-runners-are-unavailable).
Each new run needs a fresh output directory. The JSON receipt distinguishes
failed and unexecuted checks, and logs retain the underlying test counts.
The offline executable cases below require Linux and strace. The first-launch
case has its separate declared engine prerequisites below.

The GitHub workflow selects the complete native matrix for product, workflow,
build, fixture, verifier and unknown changes. A narrow allowlist of planning,
implementation, research, review and engineering Markdown/JSON plus root README,
AGENTS and PLAN can instead select the shared documentation checks:

```sh
python3 tools/ci.py --documentation-only --output .research/ci-docs
```

Selection uses the complete Git diff (including deletions and both rename paths),
not a truncated provider file list. Missing comparisons, empty diffs and manual
foundation dispatch select full checks. The selection receipt states the actual
scope; documentation success does not qualify the product. Pushes run on main,
PRs on their merge inputs; work-branch pushes do not duplicate PR runs. Concurrent
obsolete runs on the same ref/track are cancelled and remain incomplete evidence.

Native downloaded assets can be reused with `--download-cache PATH`; missing
entries are downloaded and every use verifies the pinned size and SHA-256. Actions
keys them by runner, registry digest and trust namespace. The cache contains no
credentials, extracted source, compiled workspace output or test results, and
never bypasses checks. Its absence cannot prevent a fresh run. Real-engine,
installation, load/recovery and release qualifications retain their explicit
P01-T08/P08-T11/T12 gates; the foundation workflow does not claim those cases.

This repository and accessible Actions artifacts are public. The workflow uploads
only selected synthetic foundation reports/logs, never whole lab roots. In
particular, runtime owner tokens, passwords, sessions and full database captures
must stay protected locally. Review any new upload path before enabling it.
Billing admission failures are NOT_RUN checks; setup/workflow errors, product
assertion failures and cancellation/timeouts have distinct causes. A hosted PASS
requires completed required steps and inspected reports, not a local substitute.

The native archive worker additionally needs the exact
[engine bundle](../../packaging/archive-worker/README.md). Shared CI prepares it
in a fresh sibling `<output>-native-inputs` directory; retain/inventory that root
with the run. After toolchain, formatting and selected dependency-policy gates,
the shared run makes the fresh exact-source release build. Clippy, rustdoc and
the full default-profile workspace tests then use that build's exported `source`,
`cargo-home`, `target` and installed native bundle. They retain their existing
workspace/all-targets scopes and debug assertions; tests do not use `--release`.
Cargo may rebuild units for different profiles, features or check/test modes.
This reuse stays within one run; compiled outputs are not uploaded to a cache.

The dispatch commands are `python3 tools/ci.py --workspace-check clippy
--from-build <build-directory>`, with `rust-doc` or `rust-tests` for the other
checks. They require the same committed checkout and helper identities, reselect
the recorded native tools, sanitize and record the declared child environment,
and check source, receipts, tools, native assets and copied executable hashes
before and after each command. Cargo's mutable target remains separate from the
copied release parent/worker used by artifact CLI acceptance. The default-profile
build metadata states `dev`; the copied artifacts retain their release identity.

The dispatch binds the copied worker at compile time and supplies its path,
size and hash to the connected signed-pending test. The receipt is a trusted
local build input here, not independent release authentication. Manual Cargo
development commands must supply the documented explicit library environment;
manual scoped process tests need the same
`BIGBROTR_ARCHIVE_TEST_INSTALLATION`, `BIGBROTR_ARCHIVE_TEST_WORKER_BYTES` and
`BIGBROTR_ARCHIVE_TEST_WORKER_SHA256` identity. An unavailable asset is a missing prerequisite,
not a skipped passing engine test.

The shared runner implements P01-T01's executable boundary and the bounded
`p01-event-cli` signed-event diagnostic slice of P01-T02/T03, plus
`p01-config-static` for P01-T04's implemented offline configuration commands.
The `p01-first-launch` case additionally orchestrates the bounded fresh installed
signed-event, ordinary history and export journey described below. Additional
P01-T08 scenarios remain required.
No planned task ID silently maps to a successful empty scenario.

After an exact-source build, run on Linux as an unprivileged user with `strace`:

```sh
python3 tests/acceptance/run.py p01-t01 \
  --binary .research/build-candidate/bigbrotr \
  --source-root .research/build-candidate/source \
  --source-manifest .research/build-candidate/source-manifest.json \
  --build-result .research/build-candidate/build-result.json \
  --evidence .research/p01-t01
```

Replace `p01-t01` with `p01-event-cli` to additionally verify the 17 independently
signed fixture variants, compare every decoded field and exercise changed payload,
forged signature, signed empty tag, invalid unknown UTF-8 and over-limit wire input.
It runs the same exact supplied artifact; no fixture signing or network peer is
required. This does not mark P01-T03's controlled WebSocket scenario complete.

Use `p01-config-static` with the same build/source arguments and another evidence
path to exercise all six checked-in recipe examples, validate/show, generated
schema/reference equality, selected roles/dependencies, source attribution,
redaction, lossless owner epochs, missing input and malformed/over-limit input.
The recipe role/dependency expectations are literal independent fixtures, not
candidate-generated expected answers. Two explicit `config init` calls must create
only the requested files with mode 0600 and distinct nonzero candidate identities;
an existing output must remain byte-identical. Initialization is the intentional
write operation: its network calls are observed, while file membership, contents
and permissions are inspected separately. This case does not claim a syscall
proof against transient/outside-directory mutations during initialization.

Every read-only config invocation is traced for attempted networking, file
mutation and opening staged synthetic secret references. A real secret canary
exists so an accidental read cannot hide behind a missing file. `doctor` must
return exit 4, `ready=false`, and explicit NOT_RUN live adapters/authority; its
honest incomplete diagnostic is the expected behavior of this static slice.
It does not complete P01-T04's live PG/CH/TLS/dataset/grant, durable authority,
resource measurements or packaged installation acceptance. The reported static
capability never implies those runtime guarantees.

Use a fresh evidence directory. The case invokes the actual binary outside the
checkout in a read-only temporary working directory and a minimal environment.
It checks help/version, missing/invalid arguments, unknown commands, JSON build
identity against independent input hashes, the complete source manifest and the
supplied build-result digest, created files and actual network/file mutation
syscalls. The executing runner's hash must match the selected source; the config case
also compares its helper module and records every fixture/input digest. The event
case observes every positive and negative invocation too. Linux `strace` observes
attempts outside the working directory too; merely
disabling networking is insufficient. Missing tracing, writable root-user context
or absent source/build manifests is NOT_RUN and fails this practical gate.
Mac Rust process tests cover the portable CLI behavior; they do not substitute
for the syscall observation.

Exit codes are 0 PASS, 1 FAIL and 2 NOT_RUN. Reports contain nonempty checks,
requiredness, collected/executed/failed/skipped/not-run counts, input hashes and
environment. Raw process outputs and traces remain in the ignored evidence
directory. Cleanup terminates/joins the owned process group and removes only the
runner-owned temporary directory. Timeout or interruption cannot produce PASS.
Manifest comparison establishes equality, not independent release authenticity.
Retained concise evidence distinguishes this
foundation from an eventual full bundle or storage/protocol qualification.

The runner's negative controls remove the executable, substitute a shell script
printing a plausible version, supply an invalid manifest/malformed fixture, echo
unverified events, create/unlink a transient file only during event verification,
and leave a hanging descendant. Config controls reject hard-coded validation
success and an otherwise plausible report that enables signing, exposes a public
listener, starts extra roles or fabricates applied authority. A Linux process
control opens the synthetic secret, creates/unlinks an outside-cwd temporary file
and creates a socket; the observer must detect each actual effect. Literal trace
controls also reject failed descriptor-based permission/owner changes. The process
side-effect controls require Linux strace; missing execution must be NOT_RUN;
wrong output/manifest and timeout
must FAIL, and the descendant must no longer be running.
These controls are separate from the product and never alter the product to make
an oracle pass. The Rust process tests compare SHA-256 against the actual inputs.

## Fresh installed first-launch case

`p01-first-launch` uses the same runner and result/exit-code contract. It extracts
the supplied candidate, initializes a new PostgreSQL instance through the actual
provisioning/migration/activation/enrollment commands, then runs the existing
independent signed relay and authenticated clients. It checks the initial
Archive frontier and snapshot, orderly owner shutdown, ordinary Control restart,
explicit scope authorization, exact signed history and JSONL export. The existing
export oracle compares the full manifest, file bytes and all 77 Control tables;
catalog/Archive files must remain unchanged during ordinary read/export, and
application database backends must be absent after each lifetime.

This base case does **not** run the independent RocksDB census, typed producer
progress oracle or C API Archive census. These are recorded as NOT_RUN scope
limits, not implied by its PASS. Ingest successor/resend, Index readiness,
dependency outages, capacity and complete M1 also remain outside this case.
The earlier deeper retained result remains evidence for its recorded inputs.

Run on native Linux amd64 with Python 3.11+, a non-root account, local Docker
daemon access and Compose v2. The candidate executable must run on the host for
the existing CLI client. Have more than 65 GiB free completion space and keep
loopback ports 15432, 18081 and 7447 free. The fixed recipe bounds application
plus native work to 6 GiB and PostgreSQL to 1.5 GiB; these are configured limits,
not measured capacity. `strace`, a compiler and a native probe are not required
for this case.

Prepare the pinned PostgreSQL image named by
[compose.yaml](../../packaging/compose/compose.yaml) and the pinned Distroless
base in the [Dockerfile](../../packaging/image/Dockerfile) locally first. The
runner never pulls them. It builds only the small runtime wrapper from the
verified extracted Dockerfile and existing candidate bytes, with
`docker build --no-cache --network=none --pull=false`, a unique owner label/tag and a recorded
immutable image ID. Compose receives that ID, never an unrelated supplied image.
No Rust or product build runs during acceptance. PostgreSQL readiness uses
loopback TCP so its temporary socket-only initialization server cannot qualify. Wrapper COPY layers are recreated:
a retained BuildKit layer was observed to keep inaccessible directory modes after
source permissions changed. Pinned base layers and verified product bytes remain
reusable; installed worker verification still runs before any engine initialization.

From a clean checkout, an initial candidate can be prepared with the existing
[build prerequisites](build.md) and maintained commands:

```sh
python3 tools/build.py --output /absolute/new-build
python3 packaging/release/assemble.py \
  --artifacts /absolute/new-build \
  --source-manifest /absolute/new-build/source-manifest.json \
  --output /absolute/new-candidate --include-journey
python3 tests/acceptance/run.py p01-first-launch \
  --candidate-manifest /absolute/new-candidate/candidate-manifest.json \
  --source-root "$PWD" --evidence .research/first-launch
```

The assembler output must be outside its source and artifact inputs. The case
requires a fresh evidence directory and uses only its own state. The candidate's
journey files must match the selected acceptance source. Product build provenance
and current assembly/acceptance sources are recorded separately, so verified
unchanged binaries can be reused after Python/documentation changes; do not
rebuild them merely to run this case. The manifest's historical `private` field
describes that candidate format, not current repository visibility or release
authentication.

By default the case creates a fresh labeled bridge. If local Docker address pools
are exhausted, append `--compose-network EXISTING_BRIDGE` to borrow an explicitly
selected local bridge. Its ID and existing endpoints are recorded; the case
neither removes that bridge nor stops unrelated containers. Application containers
use the recipe's host networking; the bridge is for the dedicated database.

Every created container has a retained creation intent and is checked against its
exact ID, owner/project/service labels and bind mounts before cleanup. Finite
commands and clients have bounded waits and owned process groups. Timeout,
interruption, wrong output or uncertain cleanup records FAIL; unavailable required
inputs/tools record NOT_RUN. Unreached stages remain NOT_RUN. Cleanup stops owned
containers and joins process groups. It retains stopped containers, image,
network, data and logs for inspection; it never prunes resources, deletes volumes
or reuses an earlier instance. Review `resources.json` before any later manual
retirement.

The evidence root is mode 0700. Its generated secrets, owner-token response,
session/CSRF files and full SQL captures are protected local material. Client
credentials stay outside application data mounts. **Never upload the evidence
directory wholesale**, including from public Actions. `result.json` is the
bounded summary, with hashes, checks and scope limits; review any selected upload
separately. Raw command receipts and original failures remain local.

## Installed initial Index publication

`p01-first-launch-index` adds one fresh Index publisher to the same signed-event
and Archive journey. It runs the actual installed
`serve --initialize-ingest --initialize-index --roles control,ingest,sync,archive,index`,
then uses the ordinary Control lifetime for the unchanged history/export check.
The eight stages require actual execution; authoring the case or passing its
Python refusal controls does not qualify the product or engines.

Use the build and assembly commands above with `--include-journey`, then select:

```sh
python3 tests/acceptance/run.py p01-first-launch-index \
  --candidate-manifest /absolute/new-candidate/candidate-manifest.json \
  --source-root "$PWD" --evidence .research/first-launch-index
```

The ordinary builder compiles the exact worker before binding its identity into
the parent. A native `initial-index` diagnostic parent without that binding is
not an installed candidate. The assembler records product and acceptance source
identities separately, so acceptance-only changes do not require another product
build when the verified product inputs remain unchanged.

In addition to the base prerequisites, prepare the pinned Linux amd64
ClickHouse 26.8.35.10 image declared by
[initial_schema.py](../../experiments/clickhouse-frontier/initial_schema.py)
locally. Keep loopback ports 18123 and 19000 free and more than 67 GiB completion
space. The evidence path must meet the C API observer's SQL-safe path requirements
below. The owned CH container has 4 GiB memory, 2 CPUs and 256 PIDs; the runner checks
its data/log allocation remains below 2 GiB and free completion space above 65 GiB
between observations. These are configured/checkpoint bounds, not filesystem
quotas or capacity measurements. No engine image is pulled during the case.

The case provisions a new empty Atomic database with separate reader, writer and
initializer accounts. Only the explicit initialization lifetime mounts the
initializer secret. It can create tables and inspect their emptiness; the writer
has SELECT/INSERT and must refuse CREATE/ALTER and concurrency-limit overrides.
All credentials, complete SQL captures and raw outputs remain protected under
the original first-launch resource owner and retention rules.

Both accepted policy documents must enable Index with `batch_max_rows=4`, the
largest per-target family in one literal source batch. Readback is scoped to that
batch's source range; the cumulative dictionary census still contains eight rows.
The 160-column inventory and largest 35-column insertion description require
their independent metadata limits above this smaller data-row ceiling.
While the combined owner remains alive, the observer captures all 81 Control tables
in one repeatable-read transaction and all ten CH families. Independent signed
source and C API expectations verify all typed fields, four source batches,
40 committed target outcomes, metadata/row/engine digests and the exact
shard 0/epoch 1/cut 12 frontier. The independent schema census checks all 160 columns.
The fixture's empty tags require an explicit zero `event_tag_index` outcome;
nonempty signed-tag engine acceptance remains separate. Unfiltered captures are
retained; Index owner/session/plan/head validation precedes the existing source
oracle's scoped owner view.

Before stopping the initial owner, the case reads authenticated
`GET /management/v1/roles`. It requires the literal signed source cut, current
Control identity, Archive/Index revision and fresh complete physical observations;
the status request must not refresh session idle time. After preserving this
positive baseline, the independent observer appends one event row with conflicting
content. The next physical sweep must report Index unavailable while Control
continues serving authenticated status and the other capabilities remain ready.
The fault and all original captures are retained. The original signed client
deadline and subsequent Archive history/export assertions remain unchanged.

The ordinary Control lifetime also checks complete current Archive readiness:
both capabilities must become Ready, withholding one exact retained input must
make them unavailable, and a successful narrow history read cannot revive the
failed full proof. Exact input restoration permits full-probe recovery. The
installed r7 run passed this positive/missing/narrow/restored scope with unchanged
scientific data and export artifacts.

These observations qualify only the exercised initial and ordinary profiles.
They do not qualify a direct RocksDB/native census, Index restart, remote or pin
cleanup uncertainty, column-TTL refusal, an Index query interface, independent
service outages, capacity or complete M1. Terminal overall readiness remains
explicitly unqualified; earlier failures retain their original outcomes.

### Same-store installed Index restart with backlog

`p01-first-launch-index-restart` selects a separate fresh variant with the same
candidate, ClickHouse, source and protected-evidence prerequisites:

```sh
python3 tests/acceptance/run.py p01-first-launch-index-restart \
  --candidate-manifest /absolute/new-candidate/candidate-manifest.json \
  --source-root "$PWD" --evidence .research/first-launch-index-restart
```

Its eleven stages require actual execution. The first installed attempt failed
at the prefix gate: the default Archive dispatch timer consumed almost the whole
15s window before starting its first batch. That failure is retained; installed
qualification of the corrected fixture is **NOT_RUN**. The original Index
corruption and ordinary readiness case above keeps its own behavior.

This fresh variant declares `archive.max_publish_delay = "1s"` before configuration
initialization and policy activation. Before starting the application, it checks
that deployment value and the exact `1000000` microseconds in both accepted
revision1 documents. This selects the batch dispatch cadence; it does not change
product operation timeouts, existing policy or the client's gate.

The existing signed client waits immediately after enrollment, before scope/job
creation. Its two admission batches end at source3. Within that client's original
absolute15s deadline, the observer requires Archive revision2, Index revision2,
and authenticated complete physical readiness at cut3, without touching session
idle time. It verifies the exact owned ClickHouse ID, labels, image, mounts and
isolation before pausing only that container and releasing the source gate.
Neither delayed observation nor a new parent wait extends the client deadline.

Source and Archive must then reach12 while Index stays at revision2/cut3 with
the real retained plan3 for [4..7] prepared and no plan4 for [8..12]. All ten
prepared outcomes, the accepted prefix, complete SQL captures and the original
client shutdown are retained. The application and client stop orderly; cleanup
always attempts to unpause the verified ClickHouse before stopping containers,
including after an ambiguous pause or failed assertion. A local executor join
does not resolve an unknown remote query.

After unpause, explicit `ingest recover --close-source` verifies the original
requests and closes source12. That existing Ingest command performs its own
retained acknowledgement; it does not retire the producer or reclaim the source.
The installed `index catch-up --file /etc/bigbrotr/instance.toml` must recover
owner1→2, retain published revision2 until actual publication, publish pending
plan3, and prepare/publish exactly one new batch4. An exact retry rebinds2→3,
keeps revision4 and reports no pending or new batch. Both Index reports keep
`ready=false` and explicitly perform no acknowledgement, Ingest retirement or
topology transition.

The oracle independently reconstructs recovery/session commitments and verifies
creator epochs `(1,1,1,2)` and publisher epochs `(1,1,2,2)` without removing the
new owner history. All81 Control tables are compared for preservation or their
specific permitted changes. Generation, storage incarnation, custody directory,
lock/file identities and all ten table UUIDs remain unchanged. The existing
signed-source/C API and typed-family oracle checks40 outcomes and family counts
`(8,1,1,1,1,1,1,0,1,1)`. Archive/input bytes remain unchanged, and the ordinary
history/export journey still produces the exact353-byte signed JSONL under its
original40s/12s bounds.

This paused-engine scenario does not qualify unknown native termination, remote
uncertainty resolution, lost COMMIT, topology handoff, independent process outages,
capacity or complete M1. A complete-prefix-only recovery cannot satisfy its
required prepared and unplanned backlog.

### Reader-pin release COMMIT uncertainty

Select `p01-first-launch-index-pin-release-before` or
`p01-first-launch-index-pin-release-after` with the existing candidate, source
and protected evidence arguments. Each fresh case first completes the unchanged
signed Index journey and ordinary history/export, including its original
40s/12s deadlines, exact signed output and committed owner release. This original
journey shares the restart case's startup observation helper. Before the
initial403 checks, a client marker lets the parent independently observe a
completed full startup probe, released reader/claims and an empty child inventory.
This wait consumes the original40s journey; it adds no excluded phase time.
The common client retains actual HTTP status and allowlisted public error
classification before its initial403 assertions, without headers or arbitrary
response text. All original401/403/200 expectations remain exact. The added
ninth stage starts a distinct ordinary Control lifetime and authenticates a real
new status session. Login may prune exactly the prior revoked browser session;
its protected bytes and raw SQL history remain retained. The setup oracle checks
the actual new owner/role, issuance audit, two login bucket charges and completed
readiness pins. Every earlier owner, role, pin, claim and scientific/export row
remains unchanged. Existing login capacity is observed for at most65s before
starting that lifetime; no bucket, policy or data row is rewritten by the fixture.

The second lifetime must independently complete its startup full probe, with no
active reader-purpose pin or native child. Authenticated capabilities must be
exactly `building/not_observed`; `verification_in_flight` and unavailable results
refuse. Its history/export operation cache is empty, so claiming Ready there
would be incorrect. The completed startup pin/claim and unchanged current
publication supply separate durable observations. The first lifetime retains
the actual history/export proof. The retained export-purpose pin is preserved.
The next full probe is gated at its new authenticated catalog precheck; its exact lease/claims,
owner backend PID/start, reader identity and installed worker ELF/PID/PPID/start
are captured. Only then is the Control proxy armed for the exact reader-release
UPDATE's successful Execute completion. At that hold, the selected native worker
must be positively absent with an empty child inventory, the same parent must
remain alive, and the unchanged initialization lock inode must still be busy.
The observer performs only local checks before allowing that completion, within
the product's existing500ms release operation. The actual following COMMIT is
either withheld before forwarding or completed by PostgreSQL with its reply
withheld. No ordinal, synthesized result or signal substitutes for this boundary.

Independent SQL requires the before-forward lease and claim to remain active at
revision1 with unchanged raw `xmin`. After-complete requires a released revision2,
`reader_joined`, immutable pin/holder/acquisition/expiry identity, and matching
changed lease/claim `xmin` from the atomic release transaction. **Both** product
outcomes must remain `CommitUnknown`, unavailable and exit4, without a claimed
committed owner release. Actual process and database backend absence, refusal of
the management connection and independently available local custody are separate
terminal gates. The observer's SQL knowledge never becomes a product recovery
receipt. The before-forward obligation remains retained, even after expiry.

Setup and the focused phase each have30s. The original3s owner lease,500ms
heartbeat/renewal timeout/safety margin,5s authorization and30s full probe remain.
Both proxies retain64MiB/120s ceilings; the catalog route uses the existing
16-active-connection/20,000-selected-frame profile, and the Control route keeps
its legacy16-lifetime-connection/20,000-frame bound. Output capture and observation
counts are bounded. Slower or ambiguous execution fails; a timeout alone cannot
qualify either COMMIT boundary. Raw81-table captures, earlier owner history,
scientific catalog, retained inputs, scratch cleanup and original export files
are checked independently. Proxy cleanup preserves the original failure and
never claims remote exclusion from a local process signal.

Maintained controls: `cd tests/acceptance && python3 -B -m unittest
test_first_launch_index_pin_release`; proxy controls: `python3 -B
experiments/archive-engine/native_commit_proxy_tests.py`. These are bounded
oracle, actual flock and loopback framing controls. Installed PostgreSQL/native
qualification of both new fault boundaries remains **NOT_RUN** until separately
recorded. Before-variant r1 failed its original baseline403 assertion before the
fault stage. Its actual HTTP response was not captured and remains unknown;
the source-supported startup overlap is an inference, not a measured429.
Genuine native `TerminationUnknown`, automatic pin recovery and general service
outages remain separate unqualified gates. Short-pin r1–r5 remain failures; the
qualified r6 expiry case is unchanged.

### Ordinary full-probe short-pin expiry

Select `p01-first-launch-index-short-pin` with the same candidate/source/evidence
arguments for the closed pin-expiry variant. It retains the eight Index stages
and original signed history/export checks. In the same ordinary Control process,
both capabilities must first be Ready. The client then performs only authenticated
status reads while a one-shot numeric-loopback proxy gates the next complete
native reader and an owned PostgreSQL transaction locks `ducklake_data_file`.
Actual `pg_blocking_pids`, backend start times, the installed worker ELF and
PID/start identity bind the obstruction; no worker signal is injected.
The proxy's closed `ordinary-reader` profile streams baseline/manager traffic
under its aggregate64MiB/180s and16-active-connection bounds, then frames only the first new
explicit reader accepted after arming. Complete baseline catalog preparation
therefore cannot exhaust the fault reader's separate20,000-frame admission.
The selected connection ordinal and actual backend PID must match the held gate;
pre-arm readers are never retroactively selected. That first connection is a setup
barrier: the native worker checks immutable catalog bindings through PostgreSQL,
detaches that connection, then attaches DuckLake. The gate's backend identity and
live pin sample are retained separately. After releasing it, the observer must
discover exactly one actual relation-lock waiter with the expected reader role,
database, application, original pin admission lower bound and owned blocker.
The same worker ELF/PID/PPID/start and singleton child inventory remain held;
the discovered waiter PID/backend-start is then fixed for every expiry sample.
The owned relation lock must actually block that waiter near the original pin
deadline. A gate marker, absent precheck backend or merely live child cannot
substitute for this proof. Discovery consumes the same 96-observation and phase
budgets; it cannot restart or extend the pin window. Serial completed baseline sessions
release connection slots only after transport closure; cumulative traffic remains
bounded. The active connection ceiling and peak are reported explicitly.

The deployment uses a 3s owner lease, 500ms heartbeat/renewal timeout/safety margin,
and unchanged 5s authorization and 30s probe limits. No accepted policy changes.
The original stored reader pin must remain fixed while the same Control
backend/epoch renews beyond its expiry. The policy ticket starts before the
successful, at-most-500ms owner refresh that writes a 3s lease, independently
placing its original 5s deadline no earlier than pin expiry plus 1.5s. The native
cancellation monitor polls every 25ms, worker transport every 2ms, and kill/join has
a 300ms bound. This fixture requires observed joined release before expiry plus
1s, with clock-bracket checks; that is a discriminating fixture gate, not a
general filesystem/shutdown latency guarantee. Slower or ambiguous observations
fail the fixture rather than widen its limits.

Pin admission itself has the same 500ms cap. Including the separately checked
100ms clock tolerance, the original local window cannot expire before stored
expiry minus 600ms. The worker must remain actually blocked close to that lower
bound, and an earlier disappearance or release is refused. Final Control lease
expiry must exceed the original pin expiry plus 3s, proving a refresh sampled
after the original pin deadline, rather than merely a lease spanning it.

The phase requires no earlier active reader, a live pin during actual blocking,
positive native reap, exact reader/claim release, no post-expiry Ready response,
and unchanged scientific data, retained inputs, export files and prior pins.
Setup, positive and expiry each have 30s; the original 40s active journey and 12s
export bounds remain (130s total). Protected evidence retains all failures.
Pin-release COMMIT uncertainty, genuine `TerminationUnknown`, withheld-input
restoration and general outages are **NOT_RUN** in this variant. Python refusal
controls do not substitute for the required real installed execution.

## Same-store installed Ingest restart

`p01-first-launch-restart` extends the fresh base case above on the same physical
stores. Before initialization, its fresh deployment declares both exact loopback
fixture endpoints (`/installed-fixture` and `/installed-live-fixture`) with
`127.0.0.1/32`, port7447; accepted policy/grants are not rewritten.
It completes the first export, closes and retires the original Ingest
producer, activates epoch 2, and sends a distinct signed event, the original
event and the distinct event again. After closing and retiring that producer,
it catches Archive up, checks an exact no-op retry, and reads both full signed
events through a new ordinary Control lifetime.

Before each new post-export client lifetime, the fixture observes its actual
account/loopback-peer password buckets and PostgreSQL clock. Login and grant
reauthentication share the existing 5-account/20-peer attempts per 60 seconds;
the fixture waits for eligibility before starting the finite owner. The admission
decision has a 65-second deadline, including a check after each independently
bounded SQL observation. It neither resets buckets nor retries a refused login.
Live-client CLI failures/timeouts retain original bytes in exclusive mode0600
`*-failure.private` files under the owned mode0700 evidence directory; these are
sensitive local diagnostics and must not be uploaded.

```sh
python3 tests/acceptance/run.py p01-first-launch-restart \
  --candidate-manifest /absolute/new-candidate/candidate-manifest.json \
  --source-root "$PWD" --evidence .research/first-launch-restart
```

Use a fresh evidence directory and the same pinned candidate/image, resource and
custody prerequisites as the base case. The C API observer additionally requires
an absolute evidence path using letters, digits, slash, dot, underscore or hyphen,
with no `.` or `..` components. It uses the verified bundled library, header and
extensions directly: no private probe executable, compiler or additional native
build is required. A candidate can be reused when its bundled journey files still
match; acceptance-source identities are recorded separately.

The host observer explicitly maps the container's Archive mount to the same
host files using DuckLake's connection-only `OVERRIDE_DATA_PATH` with `READ_ONLY`.
It compares the complete catalog, including row transaction identities, before
and after each census. This does not relocate data or alter the stored root;
see [DuckLake connection options](https://ducklake.select/docs/stable/duckdb/usage/connecting).

Independent literal encoders reconstruct all original and successor scientific
rows, request bytes, receipts, typed progress and publication digests. Full C API
censuses must agree; original body, eligibility, visibility, first receive and
export artifacts remain unchanged. The final resend is proof-only. The selected
provenance-on fixture has 13 dictionary, 2 body, 2 eligibility, 2 receive and
3 provenance rows, 7 Archive commits and snapshot 8. Exact retry adds no attempts,
frontiers or scientific changes. Missing or unreached checks remain NOT_RUN.

This case does not run a native RocksDB census, provenance-off repeat, second-event
export, Index/readiness, dependency-outage or capacity qualification. Those
requirements and complete M1 remain open. The case has 12 declared stages;
qualification requires their actual execution against the recorded candidate.
All raw credentials, sessions and SQL captures remain protected as above.

`p01-first-launch-restart-provenance-off` runs those same 12 stages against a
separate fresh instance. It sets `sync.provenance = "off"` before the real
configuration initialization and initial policy activation, then checks that
both accepted revision-1 documents contain that setting. It never changes an
already accepted dataset's policy to select the variant. Use the same candidate
arguments with this case name and a new evidence directory:

```sh
python3 tests/acceptance/run.py p01-first-launch-restart-provenance-off \
  --candidate-manifest /absolute/new-candidate/candidate-manifest.json \
  --source-root "$PWD" --evidence .research/first-launch-restart-off
```

Its independent expectations are 8 initial scientific facts and source cut8,
then 5 successor facts and source cut5: 7 dictionaries, 2 bodies, 2 eligibility
and 2 receive rows, with no provenance rows or endpoint/access/verification
dictionaries. Operational relay enrollment still authorizes the controlled
socket. The original signed body, scope, first eligibility/receive and export
remain exact. There are four initial and two successor mutation requests;
both successor resends are proof-only. The final typed checkpoint has 11
cumulative dependencies, including five historical references first retained
by the predecessor proof transaction. That transaction must follow the two
mutation outcomes and precede the final proof head in this fresh, nonwrapping
fixture. The final Archive has 6 commits, snapshot7 and frontier6. Retry and
ordinary history preserve the same science and original export.

The off case has the same native-census, second-event-export, Index, outage,
capacity and complete-M1 exclusions. Implemented expectations and offline
negative controls do not claim a connected PASS; inspect its actual result.

The [Control runtime matrix](../../experiments/control-runtime/README.md) runs
the actual persistent executable against an isolated PostgreSQL engine, with
external row/lock observations and process signals. It is separate from the
foundation runner and does not qualify the remaining data path or release.
