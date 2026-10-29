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
the Python acceptance controls run before the fresh exact-source release build.
They use temporary fixtures and the declared native tools, including the native
C/C++ smoke check and Linux syscall/process observers; they do not require a
compiled product. A control failure leaves the build and later checks NOT_RUN.
This exposes verifier failures earlier without reducing the checks in a passing
run. Clippy, rustdoc and the full default-profile workspace tests then use that
build's exported `source`,
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

### Outside-checkout tutorial qualification

`p01-first-launch-tutorial` runs the same seven first-launch stages and data/export
oracle with the checkout declared as the source of maintained clients/fixtures.
Invoke the runner by absolute path from a directory outside every working
checkout, with canonical absolute source/candidate/evidence arguments. Bundle,
state and evidence remain external. See the [first-event tutorial](../tutorials/first-event.md)
for the exact command; the bundle is not advertised as a source-free runner.

The variant retains the original client and resource bounds, including ordinary
history 40s/export 12s, and reuses the startup completion observation inside that
40s window. It snapshots socket inodes of the actual owned application/client
PIDs through Linux `/proc`, requiring the declared loopback TCP listeners.
Missing identity/descriptor visibility fails rather than implying absence.
PostgreSQL proof is its actual Docker loopback 15432 binding plus TCP reachability;
its process descriptors are not claimed inspected.

Before engine bootstrap, the installed executable must reject the assigned,
enabled but unwired Monitor role and a copied recipe with only active-write
enabled, using their exact exit 3 diagnostics without Ready output or changed
state. These negatives do not substitute for the real signed acquisition and
353-byte export. The actual peer must validate every accepted connection through
EOF, with no error, incomplete shutdown or unmatched opening; an unexpected
masked outbound EVENT is a real peer-parser negative control.

Zero Nostr signing is an explicit source-exclusion claim: reviewed `f5aad2c1` code has
no reachable Nostr signer, and only session REQ/CLOSE reach the text sender.
The case binds the executed product's code/dependency/configuration Git objects
and relevant source files to that reviewed profile; documentation-only revisions
can reuse it, while changed product inputs require review. Zero outbound Nostr
publication additionally uses the complete real peer observation. No fabricated
signer-call counter or candidate `publication:false` field is used. Session HMAC,
TLS and required Archive SQL publication are outside this Nostr claim.

The existing base case remains available with its original scope. This variant
adds no Index, native census, general egress, live successor or capacity gate.
Definition and unit controls alone are not installed acceptance evidence.

## Initialized Ingest renewal during stop

The dedicated `p01-first-launch-initial-renewal-stop` case reuses the installed
candidate, declared source checkout and pinned Compose dependencies from the
first-launch case. It starts Control/Ingest without acquisition or Archive/Index
work. An independent PostgreSQL row lock must block the exact renewal query of
the configured product login and its original backend incarnation before SIGTERM.
The private listener must first be reachable; its closure alone never proves
that the owner or SQL has stopped.

Run both schedules with fresh evidence directories:

```sh
for outcome in known held; do
  python3 -B tests/acceptance/run.py p01-first-launch-initial-renewal-stop \
    --source-root "$PWD" \
    --candidate-manifest /absolute/path/to/candidate/candidate-manifest.json \
    --renewal-stop-outcome "$outcome" \
    --evidence "/tmp/bigbrotr-renewal-$outcome"
done
```

`known` verifies that private admission closes while the same parent/backend
remain alive and blocked, then releases that lock for collection.
It requires exit0, native close, committed Control release and independent row/
backend checks. `held` retains the lock through actual refusal and
requires exit4 without a positive stopped/released claim. Both compare the original
Ingest scheduling row (including xmin), producer and job/frontier observations
to reject subsequent renewal-chain work or dispatch. The fixture explicitly uses
the existing5s authority/renewal defaults; it does not extend product deadlines.
The held schedule measures terminal refusal, not the exact query-collection
cutoff: the deterministic Rust controls prove the original absolute deadline and
absence of budget reset. Container/native teardown retains its separate join bound.

The initialized baseline includes its existing two dictionary requests (public
bootstrap mappings and the private mapping), with no acquisition job. Both
schedules preserve those exact input/producer observations across stop; they do
not assume that initialization leaves an empty producer-request ledger.

These are dedicated real-engine qualifications. The maintained first-launch
Python suite covers their oracle/CLI negatives in ordinary CI; it does not stand
in for these executions. Neither schedule qualifies startup, mutating HTTP,
native/unknown-COMMIT recovery or the complete installed reclamation case.

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
15s window before starting its first batch. That failure is retained; the corrected topology0 case completed all eleven
stages on product `076750d5` with acceptance `42a611ac`. The existing
[Index evidence](../implementation/evidence/p02-initial-index-clickhouse-sql-20261026.json)
records exact identities and limits; changed product revisions require affected
qualification. The original Index
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
The existing prefix query also retains each raw Archive/Index frontier row,
consumer identity and observation time. Its success predicate remains exactly
two joined rows at current revision2/frontier2/cut3, including duplicates; a
missing frontier remains visible without matching. At most256 observations of
16KiB each are retained. JSON processing and capture writes consume that same
15s budget; they add no separate observer query or deadline extension.

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
The fresh closure first changes the original producer epoch from `open` to
`closing`, before the source-closure transaction. The oracle requires that exact
barrier and preserves every other producer field, including its reservation
bound, identity and unretired state, along with the remaining Control tables.
The installed `index catch-up --file /etc/bigbrotr/instance.toml` must recover
owner1→2, retain published revision2 until actual publication, publish pending
plan3, and prepare/publish exactly one new batch4. An exact retry rebinds2→3,
keeps revision4 and reports no pending or new batch. Both Index reports keep
`ready=false` and explicitly perform no acknowledgement, Ingest retirement or
topology transition.

The oracle independently reconstructs recovery/session commitments and verifies
creator epochs `(1,1,1,2)` and publisher epochs `(1,1,2,2)` without removing the
new owner history. All82 current Control tables are compared for preservation or their
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

### Installed Archive-only catalog refusal

`p01-first-launch-archive-outage` uses the same fresh candidate, real engines,
signed client and protected-evidence arguments as the Index restart case. Its
seven stages retain the original 15s prefix and 65s client bounds, including the
existing Index physical-readiness allowance of45s/256 samples at most once per
250ms. Publication does not reset the product's15s physical-verification cooldown;
SQL head advancement alone is not a ready observation. It routes only
the Archive catalog through the existing bounded pass-through proxy, positively
joins that proxy after ready source3/Archive2/Index2, and observes connection
refusal before releasing the source gate. Control's database route stays direct.

The same application must then reach source12/Index4 while Archive keeps its
original revision2/cut3 and reports dependency unavailability with an aging,
unrefreshed observation. The oracle checks all82 Control tables, original input
bytes and typed progress, ten-family Index rows, the issued third Archive attempt,
its exact retained input/intent/binding, and custody held until orderly shutdown.
A separately declared read-only C API diagnostic verifies the unchanged Archive
prefix after shutdown; the running product never bypasses its failed route.

This case requires an actual installed execution; the case definition and its
unit controls are not acceptance evidence. It does not establish automatic
Archive recovery, generic connection/authentication/TLS failure handling,
unknown remote outcomes, shared-budget/disk-pressure capacity or complete M1.

### Installed separate Index backend stop

`p01-first-launch-index-outage` uses the ordinary candidate/source/evidence
arguments plus `--native-observer-build /absolute/observer-build`. The declared
observer must match the product source, as in the finite64 case below. The eight
ordinary stages use the existing restart prefix gate: source3/Archive2/Index2
must be independently ready, its physical rows/schema captured, and only the
owned ClickHouse positively stopped within the original15s. Both loopback
endpoints must refuse before post-prefix work is released. Control and the
Archive catalog remain direct and available; no proxy or recovery stage is added.

The unchanged four-request signed client must reach source12/Archive4 while
Index reports `unavailable/dependency_unavailable`, preserving its original
verified cut3/age and one immutable plan3[4..7] with ten prepared chunks. Lag is
nine source records; plan4[8..12] must remain absent. All82 Control tables,
unchanged custody and the same renewed Control owner are checked. The two
pre-outage acknowledged inputs describe three dictionaries; the signed event is
admitted during the outage. This does not claim a pre-outage signed event.

After the writer and its database backends positively join, the existing
read-only observer checks all49 native rows against literal encodings, including
four original inputs/receipts and ACK4/retired0. Full Archive science and the
original40s history/export client with its12s export gate must still pass while
ClickHouse stays stopped. The original65s submitted-client bound and Index45s/
256-sample observation bound remain. A failure before source release or any
incomplete stop, foreign identity, changed input, false Ready or extra plan fails
the case; later stages remain NOT_RUN and owned cleanup still runs.

The definition and unit controls are not installed acceptance evidence. Backend
restoration, automatic recovery, general remote-query uncertainty resolution,
byte hysteresis, empirical capacity, continuous enabled-Index successor and
complete M1 remain separately scoped.

### Installed finite retained-request boundary

`p01-first-launch-retained-boundary` reuses the catalog-only outage and the
unchanged installed application. Supply the ordinary candidate/source/evidence
arguments and `--native-observer-build /absolute/observer-build`. That separate
directory must contain `source-manifest.json`, `build-result.json`,
`ingest-bootstrap-probe-build.json` and `ingest_bootstrap_probe`, produced by the
maintained `experiments/ingest-registry/build_probe.py --build BUILD --example
ingest_bootstrap_probe`. The observer receipt, binary, parent build and exact
source-manifest identity must match the candidate's product source. It is an
acceptance prerequisite, never part of the production installation. Build and
run it through the existing isolated build/resource owners; do not borrow a
different revision's observer.

Two bootstrap producer requests write source1..3 and the source-dictionary
request writes4..7. The frozen62-event corpus preserves the original first
signed event (public test key3); the others use public test key1. The first61
distinct events each write five facts, filling requests4..64 and source8..312.
Twenty bounded REQs offer three new events each. REQ21 offers event61, eligible
event62, then an existing proof. Before any later proof is released, a complete
Control snapshot must retain64 reservations/outcomes, checkpoint64/complete64,
and61 successful classifications ending at REQ21 frame1. This intermediate
capture prevents a later current-row update from hiding incorrect advancement
past the unadmitted event.

An atomic release then permits exactly one proof-only REQ for event1. It advances
classification61→62 and checkpoint64→65, preserving all64 requests/inputs and
source312. The peer may send at most100 empty EOSE rounds while gated or after
the proof, then quiet rounds; it switches to quiet receive after logout for the
existing orderly-stop check. If all128 rounds finish first, the retained peer
closes its listener and records finite completion without accepting a129th
connection. The shutdown observer independently checks all128 closed rounds,
the delivered proof and a refused loopback connection. This finite source end
does not qualify quiet cancellation; other cases retain their quiet-receive
check. The original128 connections,3s socket, three-event
quantum,15s prefix and65s submitted gates remain. No product deadline, credit or
memory ceiling changes. Missing the declared bounds is a real case failure.

The case requires published Index64/cut312, all640 chunk outcomes and556 literal
physical rows, while Archive retains revision2/cut3, attempt3, input and custody.
It records honest non-Ready Index status: current separate250ms dispatch/join
turns require at least31.75s for64 plans, beyond the unchanged30s whole-sweep
freshness window. This case does not qualify current readiness over64 plans.
This evaluated initial-profile limitation belongs to existing P02-T06/T09
readiness/scaling follow-through; it is not an additional M1 prerequisite for
the bounded retained-request proof.

After actual application exit0, native close and backend absence, the observer
opens the original native store through a read-only bind in a separately owned
networkless512MiB/1CPU container, capped at30s and2MiB output. The independent
oracle compares all829 native rows,64 original receipts and inputs, producer
issued=ACK64/retired0, and native sequence1081. The separately gated proof adds
no native write. Offered overflow leaves no65th reservation, input, fact or ACK;
the case does not invent an internal refusal token from absent rows. Original
Archive data/attempts and all82 Control tables retain their exact permitted
transitions. Dynamic pending bytes, hysteresis, GC, general capacity and recovery
remain separate requirements; existing ACK/commit-loss evidence is reused.

Portable fixture/refusal controls are in `test_first_launch_retained.py` and run
with the ordinary acceptance unittest discovery. Corpus authoring/checking uses
`tests/fixtures/acquisition/retained_requests.py` with isolated
`secp256k1==0.14.0`: maintained signing plus independent BIP340 verification and
changed-body/signature refusals. The frozen corpus and its pinned hash let the
installed fixture run without a signing dependency. Source/control PASS alone
does not establish an installed result.
Signed history/export remains covered by the separate installed journeys.

The retained case also enables the same bounded writer lifecycle logger as the
Index COMMIT cases. After independently verified prefix readiness and before the
catalog fault or source release, one provisioner query records the ClickHouse
server's microsecond clock under the original15s prefix deadline. Collection
waits for actual application/client joins, then applies that query-start cutoff
to both the independent count and row export within the existing8192-row/4MiB/
30s limits. Later64-workload queries are outside this diagnostic. A failure
before the cutoff retains the original early terminal capture. Lifecycle pairs
may remain explicitly `INCOMPLETE`; this diagnostic status is separate from the
functional retained-history verdict. Query coverage and remote termination or
exclusion remain `NOT_PROVEN`, and a later case PASS does not resolve the earlier
Code202 failure. The queue1024/flush512/7500ms profile, writer quota1 and engine
budgets are unchanged.

### Installed shared-budget admission behind Index

`p01-first-launch-shared-budget` uses the same candidate, owned engines and
protected evidence as the Index restart case:

```sh
python3 tests/acceptance/run.py p01-first-launch-shared-budget \
  --candidate-manifest /absolute/path/to/candidate.json \
  --source-root "$PWD" --evidence .research/first-launch-shared-budget
```

Its fixed application payload is864MiB (905,969,664 bytes), selected from the
finite-profile allocation requirements. The staged HTTP response adds16MiB to
the earlier848MiB test envelope, preserving the original admission margin and
contention gap before measurement; product/default process envelopes do not grow.
The maintained native verifier emits the compiled requirements for confirmation.
Sync requires132,813,480 data bytes; Archive96,999,880; full Index235,471,022,
including197,656,576 physical bytes and17MiB retained preparation/metadata.
The Control connection allowance is554,172,424 bytes (528MiB plus eight copies
of the65,537-byte credential bound); other retained owners hold24MiB input,
4MiB TLS roots,1MiB Archive reader and17MiB Index preparation/metadata. Together
they retain602,406,920 bytes before the1MiB per retained Archive input.

Complete Index work with four Archive inputs and one complete login allowance
(65MiB plus128KiB connection and64KiB request) totals892,600,502 bytes, below
the fixed envelope. A retained physical Index operation, two prefix inputs and
one full Sync round require at least934,974,128 bytes, above it. The respective
margin13,369,162 bytes and gap29,004,464 bytes are unchanged. After Index
joins and yields inactive metadata, Sync with four retained inputs needs
739,414,704 bytes. The fixture's four-row/one-worker Index limits preserve the
same fixed local allocation formula as the emitted profile; source64/1MiB,
wire16MiB and query256MiB bounds remain. This arithmetic selects the fixture;
only actual denial/admission observations certify the overlap.
The remaining process allowances, original prefix15s/client65s and Index
query5s/operation30s bounds stay unchanged.

After positive source3/Archive2/Index2 physical readiness, the fixture pauses only
its verified ClickHouse container and releases the original signed client gate.
It requires an original `bigbrotr-initial-capacity/1` denial with the exact live
Control identity and an actual retained Index operation. One repeatable-read
capture of all82 Control tables, pending work, original input hashes and the live
backend/custody observation precedes immediate unpause. Capture overhead can
exhaust the existing query deadline and fail the case; no extra pause window or
query retry is introduced.

Either Sync or Archive may be the oldest deferred role. A submitted Sync job may
still be accepted/revision1/unowned or already queued/revision2 under the same
Control; only its exact documented claim transition is allowed afterward. An
Archive denial must have a complete unpublished source batch and no new issued
attempt. The same wait must later be admitted after positive local join and
result consumption of its retained Index operation, with the positive admission
observation itself inside the declared30s capacity wait. Missing, replaced, duplicate
or incomplete lifecycle evidence refuses the proof. The labeled post-refusal
budget snapshot is not the atomic failing state, and local join does not establish
remote exclusion.

The journey then requires source12/Archive4/Index4, all40 Index outcomes and all ten
typed families, six classified events, exact original inputs, independent full
Archive C API science and unchanged historical rows. Same-owner orderly shutdown
precedes the original353-byte signed history/export with40s/12s limits. This is
one declared logical-credit overlap, not general capacity, RSS/disk pressure,
remote uncertainty resolution, automatic recovery or complete M1 qualification.

### Installed Index continuation across the initial topology handoff

`p01-first-launch-index-topology` uses the same candidate and protected-evidence
arguments. Its fifteen stages combine the existing signed initial journey,
ordinary history/export, successor/resend, Archive catch-up and exact retry with
finite Index catch-up and retry. The independent expected inputs retain the
original event and add the existing signed successor with `t=allowed`.

```sh
python3 tests/acceptance/run.py p01-first-launch-index-topology \
  --candidate-manifest /absolute/new-candidate/candidate-manifest.json \
  --source-root "$PWD" --evidence .research/first-launch-index-topology
```

The initial Index must publish all four batches and pass positive physical
readiness. This variant leaves the intentional physical-content conflict to its
existing separate case. Source succession and Archive recovery must preserve
the old Index generation, custody, table UUIDs and all historical receipts.
Finite Index recovery must carry the closed epoch1 cut12 into explicit handoff
revision5, with a zero epoch2 bound and no fabricated batch or engine receipt.
Exactly three successor batches then publish revisions6–8, giving70 chunks and
100 chunk bounds. The oracle independently reconstructs every typed row and
commitment; expected family counts are `(13,2,2,2,2,2,2,1,3,3)`.

Index owner1→2 and retry2→3 must bind the retained typed history, preserve prior
receipt/MVCC identities and leave Archive, inputs, original export and other
consumer histories unchanged. Exact retry adds no batch or physical row. The
current `bigbrotr-initial-index-catch-up/2` report distinguishes the source epoch,
complete cuts and persistent handoff revision from whether this invocation
created the handoff. It remains `ready=false`, without Index acknowledgement or
Ingest retirement. The final ordinary reader verifies both signed events.
Before either history request, the shared successor client observes the current
Control owner's completed startup probe: exact Archive revision6 and both source
bounds, released reader pin and claims, positively absent native child and empty
operation caches (`building/not_observed`). This setup consumes the original30s
client deadline and uses the newly authenticated session. The verified startup
rows must remain exact; the two subsequent history requests each run once and
must return200 with the original signed events. A429 is retained as failure,
never retried by this fixture. This gate addresses a missing positive setup
precondition; it does not prove the cause of a prior generic capacity response.

At the closed/unretired and terminal successor boundaries, the journey also
runs the maintained SQL handoff constraints in two fresh database copies inside
its owned PostgreSQL container. The original database's identity and all82
tables must remain exact. The three nonterminal and23 terminal cases cover
premature/stale handoffs, missing or inconsistent cuts and receipts, atomic
publication, immutable history and typed retry. These are SQL-only controls;
their synthetic Index owner transfer is not native/ClickHouse recovery evidence.
The installed Index commands supply that separate evidence. Copies and complete
or partial failure diagnostics are retained with the protected run. See the
[SQL fixture contract](../../experiments/control-migrations/README.md).

Maintained oracle/dispatch controls run with
`cd tests/acceptance && python3 -B -m unittest test_first_launch_index_topology`
and are also discovered by the shared CI controls. This installed variant is
qualified by an actual fifteen-stage PASS on product `ace1b52f`, assembly
`3d01cc10` and acceptance `cb3d2fec`. The same candidate passes the affected
eleven-stage epoch1 restart and both twelve-stage publication-COMMIT cases.
The [Index evidence](../implementation/evidence/p02-initial-index-clickhouse-sql-20261026.json)
records exact inputs, independent expectations, original failures and review.
This does not qualify continuous combined successor ownership, general topology recovery,
unknown query/native termination, independent service outages, capacity,
second-event export or complete M1.

### Live retained Index continuation

`p01-first-launch-index-live-topology` extends the signed topology journey to
sixteen stages with the same candidate arguments. Its successor starts with
`serve --activate-initial-successor --roles control,ingest,sync,index`, using the
existing retained Index identity and ordinary credentials. It does not initialize
a new generation. Before starting the successor relay client, the fixture checks
the actual committed revision5 bridge: closed epoch1/cut12 and an explicit zero
epoch2 bound, no new request, plan, chunk or ClickHouse receipt. Epoch2 remains
open and its Index session must share the actual active Control incarnation.

The unchanged signed peer then supplies the new tagged event, predecessor resend
and exact new-event duplicate. All three successor batches must publish live,
and authenticated role observations must become Ready against the complete
ordered vector `[epoch1=12, epoch2=10]` before the successor stops. An old or
single-epoch Ready is refused. The original submitted25s gate covers classified3;
the client then verifies the queued job once. Only this live case opts in to a
separate pre-logout Index-proof phase, reusing the existing45s/256-sample/250ms
acceptance envelope. This is a finite qualification bound, not a product latency
SLA. The15s sweep cooldown and30s whole-sweep freshness remain unchanged.

The child publishes its monotonic phase start/deadline. Parent setup, authenticated
HTTP, before/after captures and release all consume that exact interval; neither
late predicate success nor child death can pass it. The child independently checks
the deadline when accepting release. The same session and quiet relay stay live,
then the original logout/401 check, commands-finished25s gate and owner stop/join
run normally. Prepared/submitted/commands-finished25s, job120s, peer128connections/
3s and product query/retry/resource bounds are preserved. An earlier attempt's
valid physical Ready arrived after submitted25s expired; its full-case FAIL stays
retained and is never relabelled by a capture replay.
The same independent source/C API science and typed ten-family oracle verify
all70 chunks and100 bounds, including original receipt and raw row multiplicity.

After the owner stops, source closure/retirement and Archive catch-up must retain
that live Index publication. Two finite Index catch-up invocations must recover
the committed historical bridge without adding a revision, batch or physical row.
The ordinary reader still verifies both original signed event bodies, with the
original export preserved. The final stage then uses the original normal runtime
configuration and `serve --activate-successor --roles control,ingest` for a genuine
epoch3/topology2 activation. Its predecessor must have exactly three resolved
requests and cut10; the new producer/source remain open with zero new requests.
All scientific tables, Index rows/bridge/cuts/sessions, original inputs, Archive
and export stay exact while only the actual successor lifecycle metadata advances.
The owner must stop and join normally.

Through an actual TCP `bigbrotr_control` login using the mounted product
credential, a read-only transaction records current/session users and role flags,
then requires `require_index_handoff_metadata` to validate the retained bridge.
The shared `require_index_handoff_authority` guard must return SQLSTATE23514 and
the exact unsupported-topology diagnostic. This proves that shared guard after a
genuine transition; it does not attempt native bridge creation or exercise both
authority branches independently. Critical source review separately binds the
guard to INSERT and deferred COMMIT. Other product SQL/native controls retain
their refusal matrix.

```sh
python3 tests/acceptance/run.py p01-first-launch-index-live-topology \
  --candidate-manifest /absolute/new-candidate/candidate-manifest.json \
  --source-root "$PWD" --evidence .research/first-launch-index-live-topology
```

Portable controls are discovered by CI and can be run with
`cd tests/acceptance && python3 -B -m unittest test_first_launch_index_live`.
The installed live variant remains **NOT_RUN** pending its exact-source product
and fixture reviews and an actual engine execution. It does not qualify live
Archive, arbitrary successor recovery,64-history readiness, broad capacity,
unknown remote/native outcome resolution or second-event export.

### Index publication COMMIT uncertainty

The fresh `p01-first-launch-index-commit-before` and
`p01-first-launch-index-commit-after` cases reuse that same prefix, paused backlog,
explicit source closure and independent signed science. Their first finite
catch-up positively rebinds Index1→2, then loses plan3's actual publication COMMIT.
The existing PostgreSQL proxy matches the exact final consumer-head UPDATE and
the four Bind values: retained Index consumer, revision3, topology0 and previous2.
Only its successful UPDATE1 selects the following COMMIT. Preparation, renewal,
rebind transactions, another consumer and a cached Parse alone cannot select it.

An opt-in completion hold preserves the real transaction while the observer
captures all82 current tables and binds the current Control backend PID/start, live
Control/Index epochs, actual process and held local custody. This capture sees
the committed rebind but the old revision2/prepared-plan3 state. The observer
must release the gate inside the existing SQL operation; slower observation
fails. The fresh root explicitly declares the unchanged
`runtime.renewal_timeout=5s` schema default before initialization. The observer
checks that deployed setting before the focused launch; this restart-only
setting is intentionally absent from the managed policy document. The product
may further shorten an operation to its remaining authority, and only an actual
COMMIT fault can qualify the case. The proxy retains64MiB,20,000 total frames,
16 lifetime connections and120s.
It neither replaces a COMMIT result nor counts a local stop as a fault.

| Actual boundary | Independent postfault PG state | Fresh catch-up2→3 |
| --- | --- | --- |
| COMMIT not forwarded | revision2/cut3; all ten plan3 chunks prepared | recovers pending3, publishes it under epoch3, creates/publishes plan4 |
| Server COMMIT complete, reply withheld | revision3/cut7; all ten chunks/frontier committed atomically under publisher2 | verifies published3 without replay, creates/publishes plan4 |

Neither fault may create plan4. Both failed commands must remain
`CommitUnknown`/exit4 without a terminal success or claimed owner release.
The actual process and PostgreSQL backend must join, and custody must become
independently available. A fresh Control lifetime then supplies actual lock-based
exclusion of its failed predecessor; the oracle reconstructs the successor-bound
exclusion digest and permits only the corresponding owner/role changes.
Index session2 retains its original recovered tuple `(published2,pending3)` in
both branches, even when the independently observed current head is3.

Final origins are `(1,1,1,3)`. Publishers are `(1,1,3,3)` before-forward and
`(1,1,2,3)` after-complete. Every earlier committed receipt and MVCC version stays
unchanged. Raw ClickHouse captures retain retry multiplicity: synchronous
immutable insertion with deduplication disabled writes plan3's four dictionary
rows twice after before-forward loss, giving12 raw dictionaries; after-complete
has8. The oracle derives each copy from original signed source bytes and checks
the unchanged logical row commitments. It does not normalize observer captures
with DISTINCT or accept a divergent copy. Exact retry3→4 creates no batch and
adds no third copy. All40 zero/nonzero outcomes, table UUIDs, original inputs,
Archive files and the353-byte signed history/export remain independently checked.

These cases add no product interface and qualify only PG publication-COMMIT
resolution. Unknown ClickHouse query termination/exclusion and genuinely unknown
native termination remain separate NOT_RUN gates.

Only these two COMMIT variants enable the stable Index writer's server query log.
The writer's concurrency constraint remains `MIN 1 MAX 1`; initializer and
provisioner query logging stays disabled. Query text is limited to one byte;
settings, profile events and per-thread query logging are disabled. The queue
holds1024 rows, reserves64 and flushes at512 or7500ms. These are queue bounds,
not a disk quota: the existing4GiB container and observed2GiB data/log bound
remain. Logging, background flushes and the prefix captures are explicit timing
perturbations. An actual named writer query must have matching QueryStart and
QueryFinish records before application launch; this checks logging, not remote
query exclusion.

After all application containers and local clients have positively joined,
cleanup flushes and captures writer lifecycle records before stopping the owned
ClickHouse. A separate count must match at most8192 exported rows within4MiB;
the export command has a30s cap. Exported fields contain only query/initial IDs, users, event types, times,
duration and numeric exception codes, without SQL or exception text. Missing or
duplicate lifecycle records make the diagnostic INCOMPLETE; count overflow,
flush or export failures also record cleanup failure. Capture failures never
skip the owned stop or replace the original case failure. `CAPTURED` describes
only the returned lifecycle shapes: queue loss, unlogged attempts and remote
termination remain unproven. QueryFinish is not a ProcessList removal receipt,
and a passing retry does not explain an earlier Code202 admission refusal.

Maintained controls:
`cd tests/acceptance && python3 -B -m unittest test_first_launch_index_commit`.

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
`not_observed` alone is insufficient: before pin admission, or while an actual
new reader remains active, the bounded observer retains pending rows and polls
again. Inconsistent rows refuse; completed rows enter the unchanged strict
oracle. Neither a timer's first poll nor startup branch ordering supplies proof.
The common client retains actual HTTP status and allowlisted public error
classification before its initial403 assertions, without headers or arbitrary
response text. All original401/403/200 expectations remain exact. The added
ninth stage starts a distinct ordinary Control lifetime and authenticates a real
new status session. Login may prune exactly the prior revoked browser session;
its protected bytes and raw SQL history remain retained. The setup oracle checks
the actual new owner/role, issuance audit, two login bucket charges and exactly
one new active reader pin and claim. Every earlier owner, role, pin, claim and
scientific/export row remains unchanged. Existing login capacity is observed for at most65s before
starting that lifetime; no bucket, policy or data row is rewritten by the fixture.

The catalog gate is armed before the second lifetime launches, selecting its
first full probe at the new authenticated catalog precheck. This avoids a completed
probe followed by the15s coalescing interval under the Control proxy's unchanged
total frame budget. Authenticated capabilities must be exactly
`building/verification_in_flight`, with no prior operation observation;
`not_observed`, Ready and unavailable results refuse. The first lifetime retains
the completed history/export proof and its separate startup-quiescence requirement.
The retained export-purpose pin is preserved. The selected active pin remains in
the raw81-table baseline; its rows and MVCC versions must match the independent
live gate observation, while untouched rows are compared by explicit selected ID.
Login, captures and attribution must finish while the original pin is still live;
slow tooling fails without extending its lifetime. The exact lease/claims,
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
qualification on product `076750d5` now passes all nine stages separately for
before-forward r3 (acceptance `e674f9a1`) and after-complete r1 (`eaac23f8`). The
[Index report](../implementation/evidence/p02-initial-index-clickhouse-sql-20261026.json)
records the distinct active versus released raw SQL outcomes, observed native
join/custody and expected product fail-stop; neither becomes an automatic recovery
claim. Before-variant r2 remains a pre-fault proxy-frame/cleanup FAIL. Before-variant
r1 failed its original baseline403 assertion before the fault stage. Its actual
HTTP response was not captured and remains unknown;
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
requirements have their own cases and task owners; this result alone does not
qualify complete M1. The case has 12 declared stages;
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


`p01-first-launch-closed-reclamation` extends the maintained finite64 case using
the same candidate/source/evidence and exact-source `--native-observer-build`
arguments. It first executes the existing genuine64-request signed fixture and
independent829-row native/556-row Index oracle. It then requires explicit initial
closure/retirement, open-producer/corrupt-inventory refusals, actual final proof
COMMIT loss before forwarding and after server completion with no native/unlink
change, positive product reclamation before every successor intent, a fresh
zero-write retry and compacted restart, an incompatible older typed-checkpoint
restore refusal on an isolated copy, and actual retained-Index successor
activation. A later reclamation attempt under successor publication authority
must refuse. The successor's genuine request1 is cumulative request65 and uses
freed spool capacity. Complete native/typed science, finite Index/Archive catch-up
and retries, signed history on a fresh process and exact353-byte first-event
export complete the profile. The independent terminal expectations are67 Archive
batches with published ordinals1,2,4..68/snapshot68/frontier4,322 scientific rows
and572 raw Index rows at revision68. The final C API census automatically selects
the fixed closed-reclamation probe envelope:73/62/62/62/63 rows by family and67
commits, each queried with one additional overflow sentinel. The probe's default
16 rows per family/8 commits remains for the smaller cases; these are diagnostic
envelopes, not product limits. Row-byte, memory, temporary-space and30s command
bounds remain unchanged.
Complete-frontier Archive retry may classify an older positively excluded input
as absent through one exact later replacement already selected by the retained
frontier. Its unchanged strict receipt reader must observe the replacement
through full EOF; that replacement still requires its own ordinary resolution
and physical membership. The profile also loses the first replacement4 native
COMMIT reply before any frontier binding or later publication: independent C API
science must contain exactly receipts1,2,4 while issued1..4/currentfrontier2 and
original3 custody remain. Fresh recovery must reuse4 unchanged, seal its original
login, publish only the64 missing attempts5..68 and bind bridge3/final4. The
uninterrupted3-before/65-new schedule remains available to the maintained oracle;
neither schedule derives its expected counts from observed rows. Unique unbound
adoption requires full-EOF Present; known provisional absence continues the strict
original read under the same deadline/custody, while errors refuse. Multiple
candidate/one-Present repeated-failure recovery retains its P02-T04/P02-T06/P08
requirement. These new source paths need their own actual installed evidence.
The existing completed-startup-probe and native-worker-join gate precedes both
fresh clients inside their original40s export/30s history windows. The isolated
mixed restore pairs an older checkpoint head with current progress children;
it requires the independently observed SQL typed-head digest refusal and does
not claim to reach the later direct floor comparison.
The live Index gate requires current publication and the complete authenticated
source vector `[epoch1=312, epoch2=10]`. The original batch/grant/operation/client
and30s readiness bounds remain; Ready65 is not required or claimed. The native
kernel controls separately cover stale native custody/sequence/inventory, one
observed unlink followed by cancellation and fresh proof,
uncertain native state and corrupt residuals. These declared scenarios are NOT_RUN
until their actual root-owned native/build/installed receipts exist; portable
controls alone cannot qualify reclamation, fsync or physical power failure.

```sh
python3 -B tests/acceptance/run.py p01-first-launch-closed-reclamation \
  --source-root /absolute/clean-source \
  --candidate-manifest /absolute/candidate/candidate-manifest.json \
  --native-observer-build /absolute/exact-product-build \
  --evidence /absolute/fresh-evidence
```

The observer build must be supplied separately using the maintained
`experiments/ingest-registry/build_probe.py --build BUILD --example ingest_bootstrap_probe`
recipe over the exact candidate's controlled parent release build. Its source,
toolchain, parent build receipt and binary hashes must match the candidate; an
older observer or the product's decoded reclamation report cannot substitute
for the raw139-byte floor and exact829→765 native-row comparison.
