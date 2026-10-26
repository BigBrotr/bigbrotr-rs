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
with the run. Both Cargo checks and the second clean build use these pinned
inputs. Manual Cargo development commands must supply the documented explicit
library/test-bundle environment. The shared run builds the exact-source package
before workspace tests, then invokes
`python3 tools/ci.py --rust-tests-from-build <build-directory>`. This verifies the
packaged worker against the trusted build receipt and native bundle, supplies its
path/size/hash to the connected signed-pending test, and runs `cargo test --locked
--workspace`. The receipt is a trusted local build input here, not independent
release authentication. Manual scoped process tests need the same
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
No Rust or product build runs during acceptance. Wrapper COPY layers are recreated:
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

## Same-store installed Ingest restart

`p01-first-launch-restart` extends the fresh base case above on the same physical
stores. It completes the first export, closes and retires the original Ingest
producer, activates epoch 2, and sends a distinct signed event, the original
event and the distinct event again. After closing and retiring that producer,
it catches Archive up, checks an exact no-op retry, and reads both full signed
events through a new ordinary Control lifetime.

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

The [Control runtime matrix](../../experiments/control-runtime/README.md) runs
the actual persistent executable against an isolated PostgreSQL engine, with
external row/lock observations and process signals. It is separate from the
foundation runner and does not qualify the remaining data path or release.
