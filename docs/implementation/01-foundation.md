# Phase 01 — Foundation and working boundaries

**Outcome:** a small real executable with typed domain/configuration contracts,
bounded runtime, guarded protocol handling, private transport and an independent
acceptance harness. It can exercise one useful vertical fixture without claiming
the full product exists. Checkboxes and linked evidence record completed work;
unchecked work and future practical scenarios are not claims of execution.

**Depends on:** P00 specification/review dossier. Canonical
[data](../specification/data-contracts.md),
[components](../specification/component-contracts.md),
[configuration](../engineering/configuration-and-runtime.md) and
[Rust layout](../engineering/rust-and-repository.md).
No task writes the neighboring Go repository.

Run each named practical case through the common runner defined by P01-T08;
before that runner exists, the task's real process/socket experiment is captured
manually with the same evidence schema and then integrated. A test binary calling
only a helper does not replace the shipped CLI path.

## P01-T01 — Reproducible workspace and executable

**Files:** `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `LICENSE`,
`apps/bigbrotr/Cargo.toml`, `apps/bigbrotr/src/main.rs`,
`apps/bigbrotr/src/cli.rs`, `docs/development/build.md`,
`contracts/release/dependencies.json`, `apps/bigbrotr/tests/cli_contract.rs`,
`.github/workflows/ci.yml`. P08-T11 extends this same CI workflow.
Create candidate library packages only with their P01-T02/T03 or P05 consumer.
**Contract:** clean source + pinned toolchain/native dependencies → one executable
reporting its exact build/schema/protocol versions. Product name remains BigBrotr.

- [ ] **Step 1:** Record tested Rust/MSRV, native RocksDB toolchain, TLS crypto provider,
  DuckDB/extension and CH/PG release identities; check license/security/update
  constraints against primary sources. Record why each dependency is needed.
  Apply the selected MIT project license and preserve separately attributable
  third-party notices; dependency license validation remains release-specific.
  Start from the [selected Rust adapter families](../research/dependency-selections.md)
  and their [revision/hash inventory](../research/evidence/dependency-selections.json).
  Keep the selected families unless a concrete qualification failure justifies
  a documented replacement. Exact candidate releases and successful dependency
  resolution are not a tested release tuple: qualify native builds, feature
  unification, offline assets and the assigned P01/P02 adapter boundaries before
  enabling those capabilities.
- [x] **Step 2:** Build the minimal application package with narrow default features and
  `publish = false`; apply that flag to later candidate library packages too.
  Avoid a crate per process role or generic utility package.
- [x] **Step 3:** Wire `bigbrotr --version`, `--help` and structured diagnostic exit codes
  through the real CLI parser; stdout carries requested output, stderr diagnostics.
- [ ] **Step 4:** Establish Linux amd64/arm64 build targets and macOS development checks.
  Record native/FFI dependency build requirements and offline asset expectations.
  Follow [development and delivery](../engineering/development-and-delivery.md):
  Mac authoring, exact-revision isolated Tux runs, and native Linux arm64
  acceptance on an available isolated runner; emulation is labeled separately.
- [x] **Step 5:** Add formatting/lint/build/documentation jobs with warnings selected by
  relevance; no arbitrary comment length, coverage ratio or test-count rule.
  Start isolated least-privilege CI here, with pinned actions and no untrusted
  PR access to Tux, trusted caches or release credentials.
- [x] **Step 6:** Build from a clean extracted source tree without undeclared cached files;
  compare reported identity with the supplied lock/toolchain metadata.
- [x] **Step 7:** **Practical P01-T01:** run the built binary from outside the checkout with
  invalid flags and read-only working directory. Expect actionable nonzero exit
  on invalid input, useful help/version and no created files or network activity.
  Negative control: a wrapper printing hardcoded version fails metadata comparison.
- [x] **Step 8:** Document build commands, supported platforms, artifact identity and known
  native portability limits; commit this independently usable foundation.

Foundation evidence: [2026-09-17](evidence/p01-foundation-20260917.json) and
[native Linux amd64 practical case](evidence/p01-t01-linux-amd64-20260917.json).
The exact `a289df4` foundation also passed native Linux arm64/amd64 and Mac arm64
CI [run 35256322733](https://github.com/BigBrotr/bigbrotr-rs/actions/runs/35256322733).
The native storage/TLS/extension tuple remains an unchecked gate; this slice
enables lossless-model work without claiming those adapters are qualified.

## P01-T02 — Lossless domain identity and evidence types

**Depends on:** P01-T01. **Files:** model package and
`src/{identity,event,evidence,coverage,policy}.rs`;
`contracts/archive/v1/{event,evidence,dictionary,manifest}.json`;
`tests/fixtures/model/`, `apps/bigbrotr/tests/model_roundtrip.rs`,
`docs/reference/data-model.md`.
**Contract:** untrusted serialized fields → validated scalar/domain values or
typed errors; SourceRecordId and SourceCut have exactly one shared meaning.

- [x] **Step 1:** Define fixed-length binary IDs/signatures, checked ordinals/sequences,
  NostrTime u64, observed UTC microseconds, duration and clock uncertainty.
- [x] **Step 2:** Encode complete ordered tags/content without normalization; distinguish
  absent value, empty string, duplicate tag and missing optional measurement.
- [x] **Step 3:** Define four-part source identities, contiguous atomic-batch receipts and
  topology-aware cuts; reject incompatible namespaces and arithmetic overflow.
  Define ExistingProof and acquisition's `EventEffectBasis = Receipt | ExistingProof`
  with retained source dependency closure; a read proof is not a mutation receipt.
- [ ] **Step 4:** Separate signed originals, first observation/correction, imported claim,
  executed measurement and private operational state using closed typed variants.
- [ ] **Step 5:** Specify JSON/protobuf/columnar mapping and fidelity labels; public metadata
  uses lossless scalar encodings, full Nostr JSON retains its actual wire types.
- [ ] **Step 6:** Add fixed independently described fixtures: empty/comma/Unicode tags,
  arbitrary JSON-looking content, u64 maximum, timestamp zero, alternative valid
  signature, earlier correction and missing clock uncertainty.
- [ ] **Step 7:** **Practical P01-T02:** write/read fixture records through the chosen actual
  serialization adapters and standalone DuckDB Parquet reader; compare every
  byte/string/integer against the original fixture. Negative controls: Float64
  timestamp conversion, tag sorting and empty-value omission must differ.
- [ ] **Step 8:** Generate the data reference and examples from the same schemas; ensure a
  projected export cannot be parsed as an independently verifiable full event.

The [portable model reference](../reference/data-model.md) covers implemented
scalars, clock quality, qualified references, structural source cuts/batches and
body/admission and core fact/event-request fingerprint recipes. The four core
event fact families and network/import intents are structurally checked; see the
[core fact evidence](evidence/p01-core-facts-20260917.json). Structural receipts,
proofs and acquisition effect bases now bind exact retained dependencies; see
[effect evidence](evidence/p01-effect-basis-20260917.json);
actual admission/recovery authority remains P02 work. Remaining
dictionary/claim/measurement families, other recipes/decoders, generated schemas and actual engine
round-trips remain unchecked. Structural validation never certifies a commit.
The [bounded core fact codec](evidence/p01-fact-codec-20260917.json) preserves
the exact canonical preimages and checks byte integrity; it does not establish
storage authority or replace the required engine round-trips.

## P01-T03 — Protocol parsing and independent verification

**Depends on:** P01-T02. **Files:** protocol
`src/{event,filter,wire,endpoint}/`, application
`src/ingest/verification.rs`, `tests/fixtures/protocol/`,
`apps/bigbrotr/tests/protocol_boundary.rs`, `docs/reference/protocols.md`.
**Contract:** bounded wire bytes → verified exact event/filter/URL or attributed
invalid/excluded result. Protocol hints never become local measurement evidence.

- [ ] **Step 1:** Pin NIP/spec/SDK revisions and register supported parser/probe versions.
  Preserve raw envelope fields needed for explicit extensions and imports.
- [x] **Step 2:** Parse bounded JSON with duplicate-field/depth/string/tag limits before
  expanding attacker-controlled arrays; reject malformed singleton fields.
- [x] **Step 3:** Recompute NIP-01 canonical ID and verify Schnorr against the actual payload;
  make the verified constructor inaccessible to ordinary deserialization.
- [ ] **Step 4:** Bind verification-cache keys to exact verified payload/signature; count
  cache memory and invalid-input work in runtime grants.
- [ ] **Step 5:** Define semantic filter normalization without losing values; explicit empty
  impossible selections remain distinct from an unrestricted query.
- [ ] **Step 6:** Define versioned endpoint normalization, private/overlay routing and DNS
  rebinding protection; preserve Host/SNI and actual selected dial address.
- [ ] **Step 7:** **Practical P01-T03:** controlled WS peer sends a valid event, changed
  content with its claimed cached ID, a forged same-ID signature and an alternate
  valid signature. Inspect the actual verification boundary output; this early case
  does not yet claim durable admission. Repeat the same hostile corpus through
  P02-T02/P03-T02 once real admission and guarded collectors exist. A permissive fake verifier is a negative control, never the oracle.
- [ ] **Step 8:** Publish conformance fixtures and limitation notes; do not inherit
  client-oriented rejection of old/deleted events into historical archival policy.

The first P01-T02/T03 slice is the [lossless event model](../reference/data-model.md)
and real [`event verify` CLI](../reference/protocols.md). Mac tests exercise frozen
signed variants/extremes, literal canonical bytes and malformed/limit controls.
The complete P01-T02 storage mappings, P01-T03 cache/filter/endpoint work and
controlled WebSocket practical case remain unchecked. `p01-event-cli` is a
bounded diagnostic acceptance case, not a substitute for that transport scenario.

The preserving `wire.rs` decoder and actual guarded WebSocket signed corpus now
have [scoped Mac](evidence/p01-relay-wire-mac-20260928.json) and
[native Linux amd64 evidence](evidence/p01-relay-wire-native-20260928.json). Original
EVENT values, duplicate fields, strict bounds and NIP-67 hints survive. The public
enum cannot bypass bounded decoding through Serde. This contributes Steps1/7;
subsequent installed source-bound admission remains open.

## P01-T04 — Configuration, defaults and topology resolution

**Depends on:** P01-T01/T02. **Files:** application `src/config/`,
`config/recipes/{full,archive-only,sync-only,monitor-only,serve-existing,split-role}.toml`,
`contracts/config/schema.json`, `docs/reference/configuration.md`,
`apps/bigbrotr/tests/config_entrypoint.rs`.
**Contract:** one instance-wide managed/declarative product-policy authority plus
locked deployment roots and restrictive runtime locks → redacted desired/applied
configuration with source attribution and reload class. Follow the selected
[authority transitions](../specification/operations-contract.md#configuration-authority-and-exact-validation);
recipes/defaults seed managed state once, not on every restart.

**Execution split:** the static configuration core is steps 1–5 (schema, typed
normalization, recipe resolution and read-only command paths); it requires no
managed PG writes. P01-T05/T06 and P02-T01 consume this core. The durable
managed/declarative authority in step 6 and its database-backed practical cases
finish after P02-T01 supplies migration 0001 and transactional CAS. An early
offline config command may validate/stage or report authority unavailable; it
cannot stand in for completed persistent configuration application.

MON-010 offline TLS presence parity is covered by the
[Mac entrypoint regression](evidence/p01-tls-presence-20260918.json): selected
verify-full dependencies require CA and server name, without accessing files or
servers. Staged unselected dependencies retain syntax-only validation.

The private acquisition destination ceiling now has
[scoped Mac configuration evidence](evidence/p01-private-routes-mac-20260928.json).
It is a closed bounded deployment root with controlled restart, not managed
policy or enrollment. Existing accepted v1 bytes remain unchanged; operational
route/enrollment and authenticated acquisition are P02-T01 Step6/P04-T02 consumers
still to implement. No complete step is implied by static validation.

The [actual v1/v2 policy journey](evidence/p01-policy-versions-native-20260928.json)
qualifies exact ban data, unchanged legacy document bytes, fresh v2 initialization,
version-aware readers and original initialization/activation retries. Explicit
legacy ban candidates remain distinct from absent accepted authority. Populated
CAS upgrade, policy transition/drain and actual acquisition remain open.

- [ ] **Step 1:** Encode all current service key tables with units/ranges/secret references;
  include operational composition/maximum-unit constraints and reject unknown,
  duplicate, unsafe and ineffective options. Do not retain a six-layer policy
  override stack beside the selected authority mode.
- [ ] **Step 2:** Preserve full events as an invariant; provenance toggles new evidence
  epochs, text indexing toggles capabilities, roles select processes rather than
  hidden storage profiles.
- [ ] **Step 3:** Wire `config init`, `config validate`, `config show` and `doctor`.
  An explicitly missing file errors; init refuses unintended overwrite.
- [ ] **Step 4:** Resolve required data services without silently enabling public listeners,
  signing keys, paid adapters, active write probes or external directory discovery.
  Apply the [deployment contract](../specification/deployment-contract.md): select
  only assigned local roles, declare remote owners, and distinguish static missing
  adapters from runtime unavailability. Fixed sync/monitor endpoint lists intersect
  enrollment/authority without requiring Validator. Linear/auto fallback needs no
  CH; serving existing CH requires matching PG/Control authority and frontiers.
  Bind concrete PG/CH endpoints, role-specific secret references and verified TLS;
  infrastructure private addresses do not widen untrusted acquisition egress.
- [ ] **Step 5:** Generate schema/default/reference content; report desired versus applied
  versions and restart/cycle/generation effects for each changed setting.
- [ ] **Step 6:** After P02-T01, implement managed-update CAS, declarative digest staging/activation,
  atomic mode-switch/restore reconciliation and deployment-locked roots; expose
  plaintext secrets through neither CLI nor management responses. File drift and
  nonpersistent emergency locks remain explicit desired/applied status.
- [ ] **Step 7:** **Practical P01-T04:** launch configuration commands from the release
  bundle using a minimal override, conflicting limits, unavailable dependency,
  fake secret and absent optional service. Observe exit/output and socket calls.
  Negative control: defaults that look valid but enable an unconfigured signer.
  Static validation rejects an empty fixed list or an enabled forced NEG without
  its declared reader. Doctor rejects a serve-only wrong PG/CH dataset and reports
  a declared remote engine unreachable through container loopback; static parsing
  cannot prove live data identity or socket reachability. Unselected worker
  defaults and staged unused secrets create no work.
- [ ] **Step 8:** Document full local, archive-only, sync-only, monitor-only, serving-existing and split-role examples and resource
  implications; every copied example must validate through the actual binary.

The [initial static implementation](evidence/p01-static-configuration-20260917.json)
now supplies closed bounded core settings, six generated recipe candidates,
redacted offline commands and explicit incomplete doctor results. Remaining key
families and live/durable authority are not complete, so the step checkboxes above
remain open. Native CI covers the implemented Rust process tests and the dedicated
[static syscall acceptance case](evidence/p01-config-static-acceptance-20260917.json)
on Linux amd64/arm64; live dependency/authority checks remain NOT_RUN. Pure endpoint identity
normalization does not establish DNS/route authorization.
The closed initial policy candidate and read-only `config inspect` consumer now
validate stored configuration against current roots. Explicit `config initialize`
now has a deployment-only atomic writer and installation attribution; [25 native PG cases](evidence/p01-control-bootstrap-native-20260918.json)
passed on Linux amd64. Management CAS, application plans, mode switching and
authentication remain step 6 work; an
inspection result is neither an applied-policy receipt nor a writer grant.
The explicit first-security-revision slice adds `config activate-initial` and the
0802 command receipt under the [owning transition](../specification/operations-contract.md#explicit-first-security-revision).
Its zero-participant barrier is limited to a deployment with no runtime history;
actual grants, policy leases, populated-deployment changes and native-owner
integration remain P01-T06/P08-T03 acceptance work. The [native activation evidence](evidence/p01-initial-activation-native-20260918.json)
records 30 main scenarios, three FK schedules and actual crash/recovery at fe63862.

The owner-supplied monitor findings now have [exact-revision native evidence](evidence/p01-monitor-corrections-native-20260918.json):
MON-002 explicit PostgreSQL port parity, MON-004 residual lease scheduling and
MON-001 genuinely blocked lifecycle/error output have scoped regression results.
MON-005 was reproduced at `2c0dfc1`: compact validation accepted a policy exceeding
the JSONB storage cap. The corrected encoder counts both representations before
acceptance, preserving SQL/read caps and identities. Its [native size evidence](evidence/p01-jsonb-limit-native-20260918.json)
at `c7e0c09` includes initialization/inspection/activation/retries at cap-1 and cap,
pre-acceptance refusal at cap+1 and the original 200,000-entry counterexample,
tiny finite fractions and Unicode/escape controls. The first harness failure is
retained separately. These scoped Step 7 cases pass; peak memory, complete role
configuration, managed updates and the full task remain open.

## P01-T05 — Shared resource budget, scheduling and lifecycle

**Depends on:** P01-T04 static configuration core (steps 1–5), not its later
PG-backed authority completion. **Files:** application
`src/runtime/{budget,supervisor,cancellation,health,metrics}.rs`,
`apps/bigbrotr/tests/runtime_pressure.rs`,
`tests/fixtures/runtime/`, `docs/operations/resource-budgets.md`.
**Contract:** host/cgroup envelope → owned byte/item/CPU/I/O permits and finite
queues; process health differs from finite-job and protocol states.

- [ ] **Step 1:** Detect configured/available envelope with explicit cgroup and native-engine
  allowances; reject aggregate role grants exceeding it.
- [ ] **Step 2:** Reserve before allocation, transfer permit ownership with payloads, include
  lookahead/clones/driver staging and release only when work actually ends.
- [ ] **Step 3:** Allocate reserved control/live/monitor shares and borrowable backfill/
  import/maintenance capacity; oldest-due plus per-host fairness prevents starvation.
  Allocate coarse process grants and local byte credits; redistribute optional
  heavy-job grants only at job boundaries, retaining archive/ACK/recovery reserves.
  Select one acquisition/socket owner per host/route domain by default under the
  [destination fairness contract](../architecture/acquisition-services.md#pooled-transport-evidence-and-destination-fairness);
  optional cross-process socket ownership retains Control permit reconciliation.
  Lost contact cannot release a still-open socket's grant.
- [ ] **Step 4:** Bound CPU verification/compression/graph pools; terminate uncooperative
  native work at a process boundary, not abandoned blocking tasks.
- [ ] **Step 5:** Implement role startup/readiness/idle/degraded/drain semantics and finite
  job outcomes with typed reasons, monotonic durations and bounded counters.
- [ ] **Step 6:** Coalesce stuck dependency probes while retaining their permits. Stop
  intake and join owners before closing embedded/native handles.
- [ ] **Step 7:** **Practical P01-T05:** saturate data work while querying health and issuing
  cancellation; block one driver beyond its deadline, reduce a live budget, then
  recover. Observe cgroup/native/socket state, outstanding operations and queue
  age, not only Rust allocations. Negative control: one fresh probe per tick
  while old probes remain alive must show unbounded operation count.
  Disconnect the host budget owner while sockets remain open; capacity is reused
  only after reconciled ownership or actual close/positive process exclusion.
- [ ] **Step 8:** Document overload behavior and include actual resource accounting in
  diagnostics; do not label deferred work as a failed remote relay measurement.

## P01-T06 — Authorization and writer ownership

**Depends on:** P01-T02/T05 and P01-T04 static configuration core (steps 1–5),
not its later PG-backed authority completion. **Files:** application
`src/runtime/{ownership,policy_authority}.rs`,
`src/interfaces/auth/`, `apps/bigbrotr/tests/ownership_faults.rs`,
`docs/security/authority.md`, `contracts/control/authority.json`.
**Contract:** authenticated principal + policy epoch + live authority → permitted
operation; scheduling lease is separate from positive writer exclusion.

- [ ] **Step 1:** Define actor/action/resource scopes and fail-closed policy availability.
  Separate public current reads, protected history/provenance and administration.
- [ ] **Step 2:** Implement fresh process-incarnation nonce, persisted owner epoch and lease
  timing validation; PID/hostname are diagnostic labels only.
- [ ] **Step 3:** Enforce single writable embedded directory through actual engine/process
  ownership and fenced control commits; an HTTP health check grants no ownership.
- [ ] **Step 4:** Apply restrictive policy leases, initially at most five seconds, to queued
  responses and signed retries as well as newly admitted reads.
- [ ] **Step 5:** Specify manual failover steps: stop/positively exclude old owner, recover
  data, advance epoch, then enable successor; a TTL expiry alone is insufficient.
- [ ] **Step 6:** **Practical P01-T06:** pause a writer beyond lease TTL, attempt takeover,
  resume the old writer and revoke a read grant midstream. Actual storage commits
  and client-delivered bytes establish safety. Negative control: an owner table
  that changes epoch while the stale writer can still mutate authoritative data.
- [ ] **Step 7:** Test no-policy/no-credential startup and public/restricted existence
  responses; logs must not disclose protected query data or secret material.
- [ ] **Step 8:** Document durability/failover classes and the limits of local disk under
  host loss; no distributed availability claim from a successful lock test.

The P01-T06/P02-T02 Control backend slice implements the
[PostgreSQL exclusion contract](../specification/operations-contract.md#postgresql-control-commit-exclusion)
with explicit migration 0007, an opaque dedicated connection owner and the first
Ingest registration consumer. Its [acceptance cases](../../experiments/control-owner/cases.md)
remain required; [scoped native evidence](evidence/p01-control-owner-native-20260918.json)
now covers seven adapter scenarios, six SQL negatives and the transaction-lock
control. [Extended native evidence](evidence/p01-control-owner-faults-native-20260918.json)
adds acquisition COMMIT ambiguity/cancellation, actual pauses beyond scheduling TTL
and PostgreSQL crash/restart, with 390 final relations checked independently.
Release-COMMIT faults, public/remote runtime grants and native handoff remain
open. The [Control process candidate](../../experiments/control-runtime/README.md)
adds the actual startup-root validator, separate policy/ownership polling and
owned signal/driver shutdown. Its [native process evidence](evidence/p01-control-runtime-native-20260918.json)
covers 16 actual executable/PG cases, all seven process outputs and 240 relations
at corrected source `6be8515`. Later [monitor correction evidence](evidence/p01-monitor-corrections-native-20260918.json)
at `2c0dfc1` exercises actually full stdout/stderr pipes, continued Control
heartbeats, bounded termination and delayed successful policy responses. Those
Control cases are qualified within that report's scope. Maximum-load, complete
service recovery and broader authority acceptance remain open; these scoped runs
do not complete the task.

P01-T05 Steps5/6 and P01-T06 Step4 also own the idle policy-poll shutdown
correction. A [native negative](evidence/p01-control-poll-shutdown-red-20260924.json)
positively blocks the current Control poll, then signals before a short unlock:
the original process abandons the operation and cannot report graceful release.
The candidate drains only that same idle operation within its original deadline,
closes private admission immediately and enters shutdown directly. The additive
`--runtime-shutdown` profile also holds the database beyond deadline and checks
private admission while the parent remains blocked. [Native corrected evidence](evidence/p01-control-poll-shutdown-native-20260924.json)
at877f3d4 passes the two headless schedules, including explicit non-success beyond
deadline. Its private probe sampled a live parent and a closed connection, but
did not establish their order relative to unlock. The [strengthened host handshake](evidence/p01-control-poll-admission-native-20260924.json)
atb3cd3f1 observes the completed probe, live parent on both sides and the same
blocked PostgreSQL query before unlock; it passes with unchanged877product. Broader native-work/mutating-command cancellation remains separate.

## P01-T07 — Same-process and remote component adapters

**Depends on:** P01-T02/T05/T06. **Files:** `contracts/rpc/v1/*.proto`,
application `src/interfaces/rpc/`, `src/ingest/client.rs`,
`apps/bigbrotr/tests/rpc_contract.rs`, `docs/reference/internal-rpc.md`.
**Contract:** the component vocabulary maps losslessly to in-process calls and
authenticated bounded gRPC; transport does not change admission semantics.

- [ ] **Step 1:** Define Ingest, Archive, Index, Read and Control requests/results using the shared
  identities, explicit outcome variants and schema negotiation.
  Control includes host-scoped network permit acquisition/release with stable
  grant identity and idempotent lost-reply resolution; callers cannot choose a
  fresh budget domain to bypass the host total.
- [ ] **Step 2:** Map application errors to retryable, invalid, excluded, unauthorized,
  unavailable, incompatible and ambiguous-result transport responses.
- [ ] **Step 3:** Configure both encoding/decoding caps, deadlines, peer limits and
  post-decompression limits; account for stream concurrency and response buffers.
- [ ] **Step 4:** Configure mTLS peer roles for split deployment and private binds by default;
  keep API grants separate from service-to-service identity.
  Verify expected dataset/role/schema at the connected owner; a valid peer
  certificate to an unrelated dataset is not a usable dependency. Exercise wrong
  CA/name, endpoint rotation and loss of policy authority without opening a new
  empty store or replacing an owner implicitly.
- [ ] **Step 5:** Preserve ProducerRequestId across timeout retries; reject same identity
  with another payload and resolve a possibly committed request after disconnect.
  Bind it to the registered producer slot/epoch and implement durable contiguous
  acknowledgement/retirement; map RetiredRequest without silently resubmitting
  the expired operation under a new identity.
- [ ] **Step 6:** **Practical P01-T07:** drop a response after storage commits, retry through
  a different caller process, and inspect one logical admission/receipt. Test
  oversized outbound/inbound messages and unauthorized certificates. Negative
  control: generating a new request ID on every timeout.
  Retry a lost network-grant reply and restart its owner with callers still live;
  inspect actual sockets to detect double issue or premature grant reclamation.
- [ ] **Step 7:** Run equivalent in-process/remote fixtures and compare source identities
  and result meaning; report added copies/latency rather than assuming zero cost.
- [ ] **Step 8:** Publish version compatibility and transport failure/retry guidance.

## P01-T08 — Independent practical harness and evidence

**Depends on:** P01-T01/T04; integrates earlier practical cases.
**Files:** `tests/acceptance/run.py`, `tests/acceptance/cases/foundation.py`,
`tests/acceptance/support/`, `tests/fixtures/manifest.json`,
`contracts/acceptance/result.schema.json`,
`packaging/compose/acceptance.yaml`, `docs/development/verification.md`.
**Contract:** named scenario + explicit release artifact → observed evidence,
cleanup record and PASS/FAIL/NOT_RUN/SKIPPED, with requiredness.

- [ ] **Step 1:** Build controlled relay/HTTP/DNS fixtures with known signed events and
  independently computed logical sets; include negative/wrong-interpretation
  controls before trusting an oracle.
- [ ] **Step 2:** Reserve isolated ports/volumes/process groups and record identities before
  launch. Never discover-and-kill unrelated processes or reuse legacy volumes.
- [ ] **Step 3:** Start actual shipped CLI commands and real PG/CH/archive adapters; fixture
  preparation is separate from measured execution.
- [ ] **Step 4:** Capture external client responses, durable receipts, artifact membership,
  database results, resource measurements and fault/recovery boundaries.
- [ ] **Step 5:** Validate nonempty fixture preconditions and required scenario execution;
  a missing binary, skipped engine or empty workload cannot pass.
- [ ] **Step 6:** Add finite diagnostic waits and deterministic coordination signals; avoid
  interpreting a short quiet sleep as proof that all schedules are safe.
- [ ] **Step 7:** **Practical P01-T08:** intentionally remove one required executable, break
  a fixture expectation and interrupt the runner. Expect NOT_RUN/failing release
  status, FAIL on wrong output, and cleanup only of recorded owned resources.
- [ ] **Step 8:** Document evidence manifests and reproducible invocation; raw artifacts
  stay under ignored run directories while concise results are retained.
  Bind Mac-to-Linux runs to the exact Git source/tree, dependency and input
  manifests and owned lab resources; unknown/interrupted output cannot pass.

## P01-T09 — First useful vertical composition

**Depends on:** P01-T01–T08; P02-T01–T07; P03-T02/T03 for the first controlled socket event; P04-T02 guarded transport; P05-T01/T02 and P06-T01–T05 for the first real read. These are early functional task slices, not prerequisites that all later phase-acceptance tasks finish first.
**Files:** application `src/app.rs`, `src/runtime/roles.rs`,
`tests/acceptance/cases/first_launch.py`,
`config/recipes/acceptance.toml`, `packaging/release/{assemble.py,manifest.schema.json}`, `docs/tutorials/first-event.md`. P08-T09 extends this same initial artifact assembler to the complete release; the early manifest explicitly lists only implemented capabilities.
**Contract:** clean release recipe → one known signed fixture durably archived
and queryable with truthful role readiness, without implicit public publishing.

- [ ] **Step 1:** Wire actual required owners/adapters into the CLI, rejecting enabled roles
  with unavailable required ports; no success stub for an unimplemented service.
  The explicit [initial Ingest owning lifetime](../reference/ingest-store.md#explicit-initial-owning-lifetime)
  has scoped startup/native/SQL-interruption evidence with P02-T02 Steps2/3. It
  retains native/Control/driver custody and initial dictionary bootstrap, reports
  acquisition absent and is not
  complete role readiness. Typed finite job authorization and the guarded socket
  consumer now have bounded component/engine evidence. Same-owner installed
  session delivery has [bounded installed fault evidence](evidence/p01-initial-session-faults-native-20261001.json),
  and [initial enrollment HTTP](evidence/p02-initial-enrollment-http-native-20261001.json)
  now accepts existing endpoints with exact historical retry. Authenticated
  [URL registration](evidence/p02-initial-registration-http-native-20261001.json)
  adds explicit populated upgrade and URL→enroll→withdraw/retry. Registration
  fault schedules and installed scope/job command consumers,
  dictionary append and successor recovery remain real integration prerequisites.
  Then qualify actual timer publication, read and restart/resend below; no complete
  adjacent Discovery/Monitor subsystem is a prerequisite for this small journey.
- [ ] **Step 2:** Enroll the controlled fixture endpoint explicitly, collect one small event
  below all batch thresholds, and observe timer-based archive/index publication.
- [ ] **Step 3:** Query the full event through the actual local read entrypoint available in
  the slice; independently verify bytes/signature and first-acquisition metadata.
- [ ] **Step 4:** Restart with the same data, resend the event and inspect one canonical body
  with unchanged first admission. Repeat with provenance off.
- [ ] **Step 5:** Stop archive/index dependencies separately: report actual lag and bounded
  pending behavior while preserving already acknowledged data.
- [ ] **Step 6:** **Practical P01-T09:** extract the bundle outside the checkout, start from
  empty volumes, follow the tutorial, inspect real listeners and assert zero
  signing/publication calls. Negative control: an unwired role reporting ready
  without producing the event on its advertised interface.
- [ ] **Step 7:** Update tutorial/health/default documentation from observed behavior;
  document the slice's available features without marking the whole phase set done.

## Foundation core versus later engine acceptance

P01-T06 steps 1/2/4/5 and P01-T07 steps 1–4 establish executable authority and
transport contracts needed by P02-T01/T02. Their real-engine acceptance steps
follow that storage implementation; they are not completed by a mock storage
port. P01-T06 steps 3/6–8 and P01-T07 steps 5–8 close against actual P02 owners,
receipts and client observations. This explicit step dependency breaks the
otherwise circular demand that admission exist before its interfaces and that
its interfaces have passed admission tests before admission can be implemented.
P01-T08's harness similarly starts with executable/fixture failure cases and
adds real engine cases when their owners exist. Only those executed cases count.

## Phase evidence

The phase is ready for dependent work when the required scenarios above execute
through the built artifact and their invariants hold. Record actual compiler/
engine/library versions and limits. Fast unit/property/fuzz tests support these
journeys; they do not replace them. Later phase completion reruns affected cases
only when wiring or contracts change, rather than repeating every test blindly.
