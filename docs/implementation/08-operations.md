# Phase 08 — Operations and distribution implementation specification

Status: implementation authorized by the owner. Unchecked steps and scenario commands below remain proposed unless linked to executed evidence. Read [PLAN](../../PLAN.md), [configuration/runtime](../engineering/configuration-and-runtime.md), [security/recovery](../engineering/security-recovery-and-validation.md), [repository organization](../engineering/rust-and-repository.md) and [private administration](../product/private-administration.md) first. Current Go code and THE PLAN are reference evidence, not immutable promises or files to mutate.

**Goal:** install, operate, diagnose, upgrade and recover the complete product through the delivered binary/image, documented CLI and authenticated management API.

Follow [development and delivery](../engineering/development-and-delivery.md) for Mac/Tux source identity, GitHub Actions, Docker Hub, release authentication, build-once promotion and ordinary Compose upgrade. These are normative refinements of the tasks below, not a separate pipeline.

**Architecture:** one Rust application with explicit roles; typed management commands own transactions and durable job receipts. PostgreSQL coordinates control state, exclusive/fenced owners protect mutations, and backups bind engine-compatible artifacts at declared cuts. Public and private listeners remain distinct. `packaging/` is the canonical distribution directory; this supersedes the earlier product sketch's `deploy/`. `apps/admin/` is a separate static artifact using the same frontend stack as `apps/dashboard/`.

**Dependencies:** P01 domain/config/runtime/permission foundations; P02 engine schemas, receipts, snapshot pinning and ownership; P03 import/admission and acquisition; P04 registry/monitor; P05 completed derivations; P06 typed read/query/visibility; P07 signer/outbox. Early tasks can land against those contracts before every consumer is finished, but phase completion requires real integrated consumers. P09 consumes this phase's private APIs. P10 consumes generated references and release manifests.

**Global constraints:** full signed archival fidelity; no implicit public exposure or signing; no age/score pruning of event bodies; no new tenant abstraction, public SQL or browser shell. Commands preserve visibility and provenance origin. Runtime health states are `starting|ready|idle|degraded|draining|stopped`; durable finite outcomes are `succeeded|partial|failed|cancelled|skipped`. A saved revision is not an applied revision. A timed-out owner is not an excluded owner. Linux amd64/arm64 are release targets; macOS is a development target until a native release is explicitly validated.

## Contracts, files and evidence conventions

P01/P02 own core IDs, cuts, policies and storage receipts. This phase imports those types rather than defining substitute IDs; [component contracts](../specification/component-contracts.md) and [data contracts](../specification/data-contracts.md) own their exact meaning. Reserve migration IDs 0801/0802/0803 through the shared `migrations/manifest.json`, not an independent allocator. Its additive wire schemas live at `contracts/management/v1/openapi.yaml`, `contracts/management/v1/commands.schema.json`, `contracts/artifacts/v1/{export,backup,release}.schema.json`. In the braces expression each named schema is a distinct proposed file. Every API mutation requires an authenticated principal, action grant, `Idempotency-Key`, expected revision where applicable, and a typed body; the same principal/key with different content returns `idempotency_conflict`.

Proposed shared envelopes:

```text
CommandReceipt { command_id, request_digest, actor_id, accepted_revision?, job_id?, outcome }
ConfigRevision { revision, parent_revision, redacted_diff, actor_id, created_at,
                 affected_roles[], security_revision?, application_by_role[] }
JobReceipt { job_id, job_revision, kind, principal_scope, input_cut, config_revision, state,
             committed_progress, display_progress, resume_unit, artifacts[], reason? }
RoleApplication { role_instance_id, desired_revision, applied_revision,
                  state: pending|draining|applied|restart_required|failed, reason? }
```

`input_cut` and all identity/visibility fields reuse the core data dictionary. Job cancellation does not rewind committed data; `cancel_requested` is nonterminal until the worker reaches its safe boundary. No route accepts shell text or SQL. Error JSON includes stable code, safe message, retry class and correlation ID, never a DSN, secret, raw event or private path by default.

P01-T08 owns `tests/acceptance/run.py`, `contracts/acceptance/result.schema.json` and `packaging/compose/acceptance.yaml`. This phase adds `tests/acceptance/cases/operations.py` and registers `p08-t01` through `p08-t12`; descriptive scenario names below are subcases of their owning task ID. Retained evidence includes input-manifest digest, artifact/version/architecture digests, invoked entrypoint, monotonic start/end, collected/executed/skipped counts with named reasons, result, independent oracle and redacted logs/output hashes. Required missing/NOT RUN cases fail release acceptance. Engine and browser suites supply actual results. Never retain actual secret files. All commands in this document are proposed commands. The exact invocation pattern is shown concretely in T01 and T12; their evidence lives under ignored `.research/implementation/`.

## Management routes and finite state semantics

`apps/bigbrotr/src/cli.rs` remains P01's single Clap root and delegates to the proposed `interfaces/cli/` modules. P08 extends that dispatcher; it does not add a second root. Private route bodies use the versioned management schemas and central data contract. The initial routes are:

| Method and private route | Command/result and authority |
| --- | --- |
| `POST /management/v1/bootstrap/enroll` | One-time local bootstrap token + account enrollment; atomically consume token and create initial owner |
| `POST /management/v1/sessions` and `DELETE /management/v1/sessions/current` | Authenticate/logout; session cookie never returned in ordinary response JSON; logout requires CSRF/origin protection |
| `GET /management/v1/me` | Principal's permitted actions, expiry and active security revision |
| `GET /management/v1/configuration` and `GET /management/v1/configuration/revisions` | Redacted effective source/lock state and authorized revision history |
| `POST /management/v1/configuration/previews` | Desired patch + expected revision → digest, diff, affected roles, coverage/resource/security consequences |
| `POST /management/v1/configuration/revisions` | Approved preview digest + expected revision → accepted revision/per-role application; stale preview returns conflict |
| `POST /management/v1/roles/{role_instance_id}/commands` | Typed start/pause/drain/resume/stop of configured supervised roles; appropriate role grant and idempotency key |
| `POST /management/v1/jobs`, `GET /management/v1/jobs/{job_id}`, `POST /management/v1/jobs/{job_id}/cancel` | Typed import/export/backup/verify/restore-plan/scrub/repair/migrate/derive/rank job; input/output permissions checked independently of job-control grant |
| `GET /management/v1/jobs` | P08-T03 bounded authorized job discovery; kind/state/time filters and stable opaque cursor |
| `GET /management/v1/roles` and `GET /management/v1/relays` | P08-T03 bounded role/registry operational summaries; protected by action/resource grants |
| `GET /management/v1/artifacts`, `GET /management/v1/artifacts/{artifact_id}` | P08-T04 bounded authorized catalog discovery/detail independent of expired job logs; creation-time/ID cursor and state filters |
| `POST /management/v1/artifacts/{artifact_id}/publish`, `POST /management/v1/artifacts/{artifact_id}/withdraw` | P08-T04 immutable artifact digest, expected revision, idempotency and explicit artifact.publish/withdraw grant; no signer required |
| `GET /management/v1/storage` and `GET /management/v1/diagnostics/preview` | Authorized resource/protection summary or redacted diagnostic inventory; actual bundle generation is a typed job |
| `GET /management/v1/principals`, `PUT /management/v1/principals/{principal_id}/grants` | List/grant with administrator authority, expected revision and reauthentication; signer/private-export grants remain explicit |
| `POST /management/v1/tokens`, `DELETE /management/v1/tokens/{token_id}` | Scoped issue/revoke; issued token revealed once, hash/metadata thereafter; restrictive activation barrier on revoke |
| `GET /management/v1/publishing` and `GET /management/v1/audit` | Authorized exact outbox/destination and attributable audit views; no secret disclosure |

Bad schema/input returns 400, missing/expired identity 401, forbidden action 403, invisible resource 404 where existence is restricted, revision/idempotency conflict 409, capacity/rate refusal 429 with retry advice, unavailable authority/dependency 503. Error codes distinguish these states; no adapter converts them to a successful empty list.

The [interface catalog](../specification/interface-catalog.md#private-management-and-cli)
completes this route inventory with actual principal enrollment/deactivation/reset,
reauthentication, token listing, protected secret-reference handles and paused-job
resume. P08-T02 owns those access routes; T03 owns lifecycle/resume. Its hash-only
single-use credential lifetimes and lost-issuance-response rules are normative.
P09-T07/T10 must use working handlers, including last-owner protection, explicit
revoke/reissue after a lost token response and secret provisioning guidance.

A job moves `accepted → queued → running → succeeded|partial|failed|cancelled|skipped`; `paused` and `cancel_requested` are recoverable nonterminal states with a reason and last committed recovery unit. Each mutation advances monotonic `job_revision` by CAS. Worker restart recovers the same identity/progress; paused stays paused, cancel_requested drains only toward cancellation, and only eligible running/queued work proceeds under current authority. Data COMPLETE is separate from job `succeeded`: a job may successfully publish a deliberately suppressed target, while a partial target never becomes COMPLETE. Task receipts record exact outputs. Frontend progress cannot assign either outcome.

## P08-T01 — Effective configuration and complete CLI semantics

**Files:** extend `apps/bigbrotr/src/config/{schema,resolution,validation,reference}.rs` and `apps/bigbrotr/src/interfaces/cli/{mod,config,serve,run,status,doctor}.rs`; create `contracts/config/schema.json`, `config/recipes/{full,archive-only,sync-only,monitor-only,serve-existing,relay-only,social,no-provenance}.toml`, `docs/reference/{configuration,cli}.md`, `apps/bigbrotr/tests/config_cli.rs`, `tests/fixtures/config/{precedence,unknown-key,locked-field,missing-dependency}.toml`.

**Input/output:** typed P01 schema resolves one managed/declarative product-policy authority plus locked deployment roots and documented restrictive runtime locks into `ResolvedConfig` with mode, desired/applied revision, document digest, staged drift, value/source/lock/applicability/change behavior. Defaults/recipe/file seed managed revision 1 only; declarative file changes require validated activation, never a hidden PG override. Follow the [authority transition contract](../specification/operations-contract.md#configuration-authority-and-exact-validation). `config init` produces minimal editable TOML; `config show --format json` produces redacted diagnostics, explicitly not an importable config. `run <role>` accepts only a declared bounded unit; `serve --roles` never silently enables dependencies. Exit mapping: 0 complete/valid; 2 input/config; 3 unavailable/refused prerequisite; 4 terminal failed/partial job; 130 cancellation. JSON outcome remains more precise than exit status.

- [ ] **Step 1:** enumerate every supported key and CLI subcommand from phases 1–7 plus operational composition, attach default/unit/range/consumer/reload class, and reject inert enabled settings. Implement managed initialization/CAS, declarative staging/activation, atomic mode transition and root/runtime-lock checks; ordinary env/CLI policy overrides are rejected.
- [ ] **Step 2:** implement `init`, `validate`, `show`, `serve`, bounded `run`, `status` and `doctor` dispatch using one resolved configuration and role dependency graph. Listener `run` fails before binding. Synchronizer `run` requires scope plus deadline/work bound; it cannot start an infinite live tail.
- [ ] **Step 3:** generate JSON Schema, reference and examples from that model, preserving useful minimal overrides. Add the explicit LilBrotr legacy semantic warning to the full-body/no-provenance recipe and keep SocialBrotr filter/provenance independent.
- [ ] **Step 4:** propose `cargo test -p bigbrotr --test config_cli` with managed seed drift/restart, declarative edit-before-activation, concurrent mode switch/file change, restored root mismatch, expired runtime locks, repeated/unknown TOML keys, explicit missing file, overflow/unit/maximum-unit/aggregate-resource errors, heartbeat constraints, role-only missing dependencies and no-signing defaults. Negative controls: reapplying a changed managed seed or silently applying a changed declarative file must disagree with literal expected desired/applied revisions; an unauthorized env policy override must fail rather than win precedence.
- [ ] **Step 5:** propose `python3 tests/acceptance/run.py p08-t01 --binary dist/bigbrotr --compose packaging/compose/acceptance.yaml --evidence .research/implementation/p08-t01`; initialize in a fresh directory, validate, inspect redaction, launch the selected role, observe its actual bind and bounded unit, and retain stdout/stderr/exit code plus independent socket/process observations.

**Failure contract:** parse/validation errors perform no engine mutation, collection or signing. A disabled dependency is named; a role waiting for data reports idle/unavailable inputs, not invalid configuration. Redaction tests plant distinct canary credentials in every supported secret source and assert absence in help/config/log/error artifacts.

T01 extends the same generated recipe inventory with `sync-only`, `monitor-only`
and `serve-existing` from the [deployment contract](../specification/deployment-contract.md).
`config init --recipe NAME` emits the explicit local assignment and remote adapter
requirements. `serve --roles` cannot bootstrap missing storage or activate default
worker blocks; omitted roles use the root assignment. Static validate names
declared requirements; doctor checks read-only runtime facts without creating a
dataset, migrating or performing relay probes. Test applicable versus staged
unused secret references, fixed-list empty/widening controls and wrong serving
dataset/frontier separately from a mere unavailable socket.

## P08-T02 — Private enrollment, sessions, grants and audit

**Files:** create `apps/bigbrotr/src/management/{auth,bootstrap,sessions,grants,audit,secrets}.rs`, `apps/bigbrotr/src/interfaces/management/{router,auth_middleware,csrf}.rs`, `apps/bigbrotr/src/interfaces/cli/access.rs`, `migrations/postgres/0801_management_identity.sql`, `contracts/management/v1/auth.schema.json`, `docs/user/administration/{enrollment,access,secrets}.md`, `apps/bigbrotr/tests/management_auth.rs`.

**Dependencies/interfaces:** use P01 principals/policy authorization and P07 purpose-scoped signer references. Private `/management/v1/` has bootstrap, session, principal, grant and token endpoints, entirely absent from public routers. Viewer reads allowed resources; operator controls permitted roles/budgets; administrator changes policy. Import, export, private export and signer administration are explicit additional grants. Tokens are hashed at rest; opaque browser sessions are server-side. Secrets use protected references, never config-history values.

The next scope-grant consumer first needs the [exact allocated visibility
observation](../specification/read-view-contract.md#visibility-allocation-before-a-private-grant).
A caller-declared name cannot be preauthorized for a future allocation. Reuse
the retained cut/snapshot and actual native reader; keep class labels separate
from monotone per-principal/action/scope grant revisions. Then connect session/
command-bound reauthentication, grant CAS/audit/retry and the actual private read.
The initial proof can be a server-side ID requiring the same live session and
CSRF, without introducing another bearer secret. Restrictive application remains
pending until old delivery/work has actually drained under the authority clock;
UTC plus five seconds or a historical command retry cannot mark it applied.
The [qualified allocation lookup](evidence/p02-archive-visibility-native-20260920.json)
now has native/PG child and restart evidence. Live certified pin validation belongs
in the grant transaction; a historical pin receipt is not authority. These
prerequisites do not complete any step below.

The [equal-mapping correction](evidence/p02-archive-visibility-repetition-native-20260920.json)
preserves availability for the cut's valid repeated mappings without accepting
conflicts. Its quiescent native mutations certify lookup semantics, not a new
frontier or grant. Continue directly with the authenticated consumer: register
`history.read` as a separate scope-qualified action and use the existing
`/v1/resources/event_history` QueryRequest/QueryPage contract on the private
origin with a session adapter. This binding does not turn management cookies into
public bearer credentials or add a second query protocol. An unconfigured native
reader remains explicitly unavailable; do not silently enable it in serve-existing.
Preserve typed absent versus unavailable/corrupt native outcomes before returning
an empty successful collection. Scope grant changes extend the common command-key
ledger with atomic CAS/audit/proof consumption; restrictive application waits for
the actual affected workers and socket delivery to drain.

- [ ] **Step 1:** implement atomic one-time owner enrollment using a random token in a new 0600 local file or explicit enrollment command output, ten-minute default expiry and single-use consume. Two concurrent enrollments can create only the intended initial owner. An unclaimed instance remains authenticated/locked; no first-visitor ownership shortcut exists.
- [ ] **Step 2:** implement local account authentication, eight-hour maximum/thirty-minute idle defaults, revocation, logout, reauthentication before grants, CSRF tokens and exact Origin checks. Use Argon2id with a 16-byte random salt, 32-byte hash, 64 MiB memory, three iterations and one lane as the initial local-account baseline; version those parameters and rehash on successful login after a future policy update. Bound hashing concurrency through reserved memory/CPU grants and account/peer rate limits; a host unable to reserve one hash reports unavailable, never falls back to plaintext or a cheaper silent hash. Store only salt/hash/parameters; enrollment/session/CSRF tokens use 32 random bytes. Local loopback HTTP and remote HTTPS have explicit cookie modes; remote non-TLS refuses startup.
- [ ] **Step 3:** implement action/resource authorization at the command handler, protected secret-reference write, and attributable append-only audit entries for accepted/rejected requests. Securely open supported regular secret files and reject pipe/device/disallowed symlink substitutions; enforce owner/mode/size on the opened descriptor. Never return stored secrets or permit arbitrary path browsing.
- [ ] **Step 4:** propose `cargo test -p bigbrotr --test management_auth`: wrong/expired/replayed bootstrap, concurrent enrollment, session fixation, CSRF/origin failure, viewer POST, operator private export, privilege escalation through configuration, revoked key during active work and malicious secret file. Negative control: an administrator-approved ordinary status action succeeds while the same principal without a private-export grant still fails that export; authentication alone cannot satisfy the oracle.
- [ ] **Step 5:** propose acceptance scenario `management-auth` against both actual private listener and public reverse-proxy route. Verify private routes cannot be reached publicly even with guessed URLs, compare permission matrix to server responses, and retain safe audit IDs plus network traces with credentials removed.

The first 0801 slice supplies immutable installation principals and the accepted
initialization audit receipt consumed by `config initialize`. These are attribution,
not enrolled users or grants. Complete human accounts/challenges/sessions and
ordinary audit in subsequent immutable migrations, allocated through the central
manifest; do not modify published 0801 or backfill synthetic actors as owners.
No T02 step is complete from this foundation alone.

The subsequent 0805 consumer implements local first-owner issue/status/revoke and
enrollment with the fixed password baseline and explicit initial dataset grants.
0804 remains reserved for artifact publication. CLI/native evidence is recorded
separately; private HTTP, sessions, account lifecycle, grant activation and full
T02 acceptance remain required before completing these steps.

The next finite CLI consumer, `access me`, verifies the actual stored credential
and inspects only that account's stored grants from one bounded snapshot. It
does not create a session or reusable action authority; its read-only local
scope does not satisfy remote login/rate-limit/audit or permission-matrix acceptance.

The additive 0806 slice introduces hash-only browser sessions, bounded persistent
account/peer attempt counters and a separate account authentication generation.
The actual Control adapter brackets bounded password work with two transactions,
checks the full credential again after hashing, and distinguishes expected login/
expiry/capacity refusal from a poisoned backend. Existing policy documents and
the seven initial dataset grants are unchanged. The private HTTP supervisor,
Origin/cookie profiles, login audit, reauthentication, scoped grants and restrictive
delivery barrier remain subsequent parts of this same task, not completed by
storage-only tests. [Native session evidence](evidence/p08-browser-session-storage-native-20260920.json)
records 12 actual PG16.15 cases at `d37e81f` with unchanged product `7a0f4b6`,
including an observed delayed-touch trigger and subsequent usable Control.
Do not advertise the session API until the listener journey
and its resource/transport lifetimes are exercised.

The next bounded consumer wires three private routes (login, logout and self
observation) into the actual Control supervisor, with one shared process envelope,
owned HTTP1 connections, owned password work and socket-level delivery checks.
`serve` follows the accepted administration gate despite managed file drift;
migration0806 and each required SQL privilege are checked before binding. Every
browser transaction checks the current config/policy head against the last fully
validated Control snapshot. An observed change stops this runtime for explicit
restart; it is not yet an application barrier or action-grant implementation.
The native executable fixture is `browser_http_probe`, driven by
`experiments/browser-sessions/http_native.py`. Its presence or compilation is not
execution evidence. No T02 step is closed by these partial routes.
[Native HTTP evidence](evidence/p08-browser-http-native-20260920.json) records
ten actual executable/PG cases at5a4e0d1,15 clock and11 interface tests. Four TCP
clients completed4094 self requests in12.011s without postponing Control renewal.
The slow-client marker, session/xmin comparisons and independent terminal SQL
qualify their named schedules only. [Hash lifecycle evidence](evidence/p08-browser-http-faults-native-20260920.json)
at661b54b adds actual TERM/CONT and SIGKILL during observed hash-sized RSS growth,
no issuance and successor login, plus a distinct controlled worker join test.
It does not trace an exact Argon2 instruction or qualify a universal shutdown bound.
[COMMIT/recovery evidence](evidence/p08-browser-http-commits-native-20260920.json)
at e65116b adds nine actual login/logout pass/before/after-COMMIT, explicit retry
and PostgreSQL SIGQUIT/live-cookie cases. The initial fixture configuration failure
is retained separately. Specific revocation and other-row/xmin checks distinguish
these outcomes. Browser, general audit/grants and remote TLS remain required.

The additive0807 slice records only newly committed session issuance/explicit
logout, in the same fenced transaction as the mutation. A partial unique index
preserves logout retry identity without changing rejected bootstrap audit. There
is no historical backfill or FK to removable session credentials. Startup requires
807 and audit INSERT privilege. Independent PG regression first observed a real
session without its receipt on the preceding product.
[Native audit evidence](evidence/p08-session-mutation-audit-native-20260920.json)
at591c75f records17adapter/12HTTP/8COMMIT cases, independent terminal SQL and
artifact/source/resource checks. The two fixture failures are retained. Upgrade
preserves all logical legacy fields while PostgreSQL rewrites the heap; ordinary
rollback/retry checks retain full row/xmin equality. General rejected-attempt
audit, retention/capacity maintenance, reauth,
grant CAS and restrictive application remain required; no T02 step is complete.

The next integrated slice uses additive0808 for bounded same-session/command
reauthentication and its exact issuance audit, followed by810 for durable scope
grants and atomic proof consumption. Separating their lifecycles does not complete
T02 or authorize a grant from a proof alone. The actual listener shares the login
hash worker and persistent attempt limits. Qualification must exercise exact
retry, expiry/cleanup, reset/logout/grant changes during hash, orphan/mixed-policy
audit refusals, ambiguous COMMIT and actual grant consumption before history read.
Current local tests establish only their named schema/transport boundaries;
engine and integrated history evidence remain required.

The [scoped grant qualification](evidence/p08-scoped-grants-native-20260923.json)
at4b9511d now executes actual password/native allocation to atomic grant/revoke,
exact historical retry, delayed COMMIT expiry and six omitted-effect refusals.
Explicit811 corrects an executed deferred SQL variable-binding failure without
rewriting810. This is an adapter/persistence prerequisite: proceed with the private
grant route and its owned native reader, then history delivery under per-action
authority. COMMIT-loss/reacquisition, multiple actors/scopes, restrictive drain and
the full private/public route journey remain required; no T02 step closes here.

The next HTTP slice mounts the selected principal-grants PUT route with exact
path/body target agreement, session/CSRF/key/proof decoding and acceptance-only
receipts. Startup checks811 and command/proof-lock privileges. ArchiveUnavailable
is a known rolled-back native prerequisite refusal; Unavailable still fails the
runtime when Control is unusable. The new executable fixture checks the unselected
reader case and healthy restart, not successful HTTP grant mutation. Continue the
owned native allocation consumer and private history; no task step closes here.
The current candidate wires explicit archive.reader.enabled through immutable
selection, captured snapshot, certified pin, joined native visibility and a fresh
acceptance check. Its isolated private_grant_cases.py fixture now passes eight actual native HTTP
cases at0d95a6a, with independent final SQL and signed readback; see the
[native HTTP evidence](evidence/p08-native-grant-http-20260923.json).
The [native deadline run](evidence/p08-native-grant-deadline-incomplete-20260923.json)
passes the actual blocked-query expiry case; its signal prerequisite failed.
The corrected fixture [reproduces signal failure](evidence/p08-native-grant-signal-red-20260923.json)
at1b9f710 with actual SQL wait and idle Control: exit4 retains the pin. A scoped
supervisor correction distinguishes joined cancellation from unknown Control/native
outcomes. [Native GREEN](evidence/p08-native-grant-cancellation-20260923.json)
at932902f passes the unchanged deadline/signal oracles, with pin0/Control0 and
no new grant command. Full history/drain and further signal/COMMIT interleavings
remain open. [Catalog cleanup qualification](evidence/p08-native-grant-cleanup-20260923.json)
at8e8cfa0 now holds the actual rollback through first SIGTERM and three completed
Control polls beyond the original5s policy lifetime. HTTP closes before its own
deadline, graceful release follows actual rollback, and restart retry preserves
all command/audit/grant/proof/account/pin rows and xmin. This does not qualify
interrupted Control renewal or an ambiguous accepting COMMIT.

The current P08-T02 prerequisite for P08-T03/T04 extends the same route with
[explicit export permissions](../user/administration/scope-grants.md), additive0812
and independent action fingerprints/generations. It leaves default permissions,
old grant digests and0811's deferred checks unchanged. The [native grant evidence](evidence/p08-export-grants-native-20260923.json) records14 actual cases at923f186,
including proof/action separation, retained generations and restart retry. The
original laboratory terminal is INCOMPLETE because its launcher requested the
wrong result filename after the case fixture passed; the final separate
both-database backend check was not run. Export jobs and destination approval
remain separate required work.

**Failure states:** unavailable policy authority causes expiry/fail-closed, not anonymous fallback. Last-owner recovery is an explicit local, positively authorized CLI operation with an audit record; it cannot be triggered through an unauthenticated HTTP request. Enrollment output is never copied into CI evidence.

The selected Argon2id parameters are an application baseline to benchmark under
P08-T02, not a claim that they reproduce every RFC recommendation. Use
[RFC 9106](https://www.rfc-editor.org/rfc/rfc9106.html) for the primitive and
independent vectors; changing lane/memory/time settings is a versioned policy.

**Local owner recovery baseline:** proposed `bigbrotr access recover-owner --principal alice --config config.toml` runs only as the OS data owner or root, with the management controller stopped and its exclusive control lock/fencing authority acquired. Refuse remote invocation mode, an active/unexcluded controller or wrong OS identity before changing accounts. The command records the recovery actor/old principal/security revision, revokes existing sessions/tokens as selected in its concrete preview, and emits a new short-lived single-use local credential-reset token through protected output; it never prints the old password. It does not disable resource policy, enable publication or rewrite existing data. Test attempted recovery while the controller is alive and after positive exclusion, including resumption of the old controller.

## P08-T03 — Managed revisions, lifecycle commands and durable jobs

**Files:** create `apps/bigbrotr/src/management/{commands,revisions,activation,jobs,lifecycle,saved_views}.rs`, `apps/bigbrotr/src/interfaces/management/{config,jobs,roles,saved_views,storage,publishing}.rs`, `apps/bigbrotr/src/interfaces/cli/{jobs,roles}.rs`, `migrations/postgres/0802_management_commands.sql`, `migrations/postgres/0803_saved_views.sql`, `contracts/management/v1/{commands,jobs,configuration,saved_views}.schema.json`, `docs/user/administration/{configuration,jobs,services}.md`, `apps/bigbrotr/tests/management_transactions.rs`.

**Input/output:** preview consumes desired patch + expected revision and returns redacted diff, validation, affected roles, new coverage work and restart/exposure consequences. Apply consumes that preview digest and expected revision, records one revision by CAS, and returns per-role application. Lifecycle commands are `start|pause|drain|resume|stop` for a configured role controlled by this supervisor, never arbitrary system services or remote container deployment. Start revalidates prerequisites/budgets/authority; stop joins or positively excludes work before releasing ownership. Job creation binds principal/source/destination/algorithm/config; retry returns the existing job. Explicit resume accepts only paused nonterminal jobs under current grants and expected revision.

- [ ] **Step 1:** implement preview/apply/rollback-as-new-revision and idempotency receipts in one PG transaction. Revalidate at apply; a stale preview is not authority. Locked bootstrap/database/listener/security roots remain deployment-owned and are shown with their source.
- [ ] **Step 2:** implement role acknowledgements and restrictive activation barrier. Persist desired/applied states separately. Restrictions invalidate streams and authorization leases; dropped notifications cannot extend the initial five-second authorization lease. Do not label a restriction applied while a role retains valid old authority or undisclosed in-flight work.
- [ ] **Step 3:** implement persistent job registry, resume identity, bounded chunk progress and cancellation. Persist committed recovery position separately from display progress. Browser disconnect does nothing to job ownership. Key/destination changes do not rewrite an already signed P07 outbox payload.
- [ ] **Step 4:** propose `cargo test -p bigbrotr --test management_transactions` with two editors, duplicate command/different digest, lost reply, service restart mid-apply, failed role, rollback after provenance-off interval, lost revocation notice and PG outage. Negative control: kill an affected role before acknowledgment; the controller must show pending/failed until its authority expires, even though the config row committed successfully.
- [ ] **Step 5:** run proposed `management-transactions` acceptance via CLI and HTTP using the same command key. Pause Indexer during collection, observe archived progress and explicitly stale serving, resume, restart the UI/client, and verify the same durable job/revision receipts. Retain independent read-frontier and archive-membership evidence.

The first 0802 consumer is the protected deployment-only
[initial security activation](../specification/operations-contract.md#explicit-first-security-revision).
It persists the first real typed command receipt without creating empty job or
application tables. This does not close any general-edit, running-role barrier,
HTTP or durable-job step above.

**Failure contract:** no automatic destructive compensation for partial apply. Report the affected roles and recovery action; security restrictions fail closed. A cancelled import retains committed events; a rollback cannot invent omitted provenance, retract delivered data or recall publication.

**Commit and disconnect boundary:** persist command idempotency record, accepted job and its authorization/source pins in the same transaction before replying. A lost response is resolved using the same command key; it does not create another job. Before commit, request cancellation may abort acceptance; after commit, the worker is owned by the durable scheduler rather than the HTTP request context. Recovery tests kill the controller before commit, after commit/before reply and after worker admission/before progress update, then compare one logical job and stable output receipts. Operator cancellation is a separate authenticated command and is reauthorized when processed.

**Saved views and optional indexes:** `GET|POST /management/v1/saved-views`, `PUT|DELETE /management/v1/saved-views/{view_id}` persist an owner-scoped canonical `QueryRequest`, label and revision; update/delete require expected revision and never store credentials or reuse past authorization. Opening a view reauthorizes its current input scope. Index rebuild/retire are typed job kinds over a P06 index-generation manifest, not arbitrary DDL: preview affected capability and pinned readers, build/verify/activate replacement before reclaiming the old generation, or explicitly disable/retire only that optional index. P09 Storage/Queries must use these working routes rather than invent client-only management features. Reserve migration 0803 in the central manifest alongside 0801/0802.

Job discovery is `bigbrotr jobs list` / `GET /management/v1/jobs`, with allowlisted
kind/state/created-time filters, default 50/max 200 results, descending
(created_at, job_id), an opaque policy-bound cursor and a listing upper cut.
Apply caller visibility before pagination; redact inputs and distinguish expired
detail from missing/invisible jobs. Poll again for newly created jobs. Lost-command
resolution still uses the original command key. Reopen a clean browser with
multiple principals/pages to prove the registry, not client memory, supplies it.
The upper cut excludes newly created jobs; mutable state filters are evaluated
at page-read time, so this operational listing is not a frozen historical audit.
Role/relay/artifact summaries share the page-size/authorization rules. Roles and
relays order by immutable role-instance and endpoint ID respectively; artifacts
order by (created_at, artifact_id) with their own upper cut. Refresh starts a new
listing; mutable artifact state follows the same page-read-time rule.

## P08-T04 — Portable imports, exports and useful research artifacts

**Files:** extend `apps/bigbrotr/src/acquisition/import/{preflight,job,jsonl,native,pensieve,worker}.rs`, `apps/bigbrotr/src/interfaces/cli/import.rs`, `contracts/import/v1/manifest.json` and `docs/operations/import.md` from P03; create `apps/bigbrotr/src/export/{job,manifest,parquet,jsonl,documents}.rs`, `apps/bigbrotr/src/interfaces/cli/export.rs`, `contracts/artifacts/v1/export.schema.json`, `docs/operations/{export,duckdb-analysis}.md`, `tests/fixtures/corpora/{full-events,legacy-projection,claimed-provenance}/manifest.json`, `apps/bigbrotr/tests/portable_artifacts.rs`, `tests/acceptance/export_oracle.py`.

**Dependencies/interfaces:** reuse P03 importer and P02/P06 pinned reads, full signed-event type, immutable origin identities and policy lease. Inputs are allowed roots/object prefixes plus a typed source manifest, never arbitrary browser file paths. Export choices are `signed-events-jsonl`, full Parquet and explicitly named analytical projection. Manifest includes schema/version, source cut/filter, exactness, visibility, methods, original/import origin, file digests/row counts, retained documents/dictionaries and missing fields. Export is not backup.

- [ ] **Step 1:** implement inspection/validation before job creation: file sizes/digests/schema, allowed roots, no symlink escape, signature/canonical identity, unsupported version, compression expansion limits, and provenance-claim labeling. Full imports require reconstructable signed content/tags/signatures under their format contract (proto3 defaults are described in [import formats](../specification/import-formats.md)); projected artifacts are rejected by canonical import and opened with standalone DuckDB using the shipped analysis guide. No second analytical dataset ingestion/catalog is selected.
- [ ] **Step 2:** implement resumable source units keyed by input digest and stable unit ID; retry/move cannot change original admission or fabricate a local relay witness. Unsupported seek-resume explicitly rereads a bounded unit; progress output names that cost.
- [ ] **Step 3:** implement pinned, policy-rechecked export chunks and atomic final manifest publication. The first signed export creates the shared 0501 artifact/file inventory; later analytics 0502 and publication 0804 extend it. Include referenced deduplicated monitoring documents and dictionaries; cancelled/failed files remain incomplete and cannot be listed as public datasets. Public publishing of an export is a separate grant/command, not a side effect of creating it.
- [ ] **Step 4:** propose `cargo test -p bigbrotr --test portable_artifacts` with ordered/repeated tags, empty values, timestamp zero/u64 maximum, conflicting duplicate IDs, private claims, missing document and signature-invalid legacy rows. Negative control: edit one content byte without updating its signed ID/signature; the independent signature verifier must reject it even if a file checksum was recomputed.
- [ ] **Step 5:** proposed `portable-artifacts` scenario imports a fixture twice, interrupts/resumes a unit, exports full and projected views, stops BigBrotr and all catalogs, then opens the result in a separately invoked pinned DuckDB. Compare signed fields and exact IDs with `export_oracle.py`'s source manifest, verify standalone document joins, and prove the projection cannot pass the full-event importer.

**Failure contract:** partial source coverage is attributed; bad units are quarantined/reported under selected import policy. A policy revocation terminates or restricts new export chunks; already delivered files cannot be recalled. Job result distinguishes complete requested scope, partial scope and failed execution.

The current bounded destination prerequisite has explicit finite
[initialize/inspect commands](../operations/export.md), deployment-root pairing
and descriptor-bound private markers. Source/job/pin atomic acceptance has
[scoped native evidence](evidence/p08-export-acceptance-native-20260924.json)
at5705b07. The private job route has [native HTTP evidence](evidence/p08-export-job-http-native-20260924.json)
at53c6c9d: privilege/authority/destination/native refusals, atomic acceptance,
exact retry and Control restart without destination/reader, retaining the
original job/pin through revocation. The [blocked-checker signal scenario](evidence/p08-export-checker-timing-native-20260924.json)
also passes with actual Control renewal and bounded process termination.
The [COMMIT-loss/restart scenarios](evidence/p08-export-job-commits-native-20260924.json)
resolve identical keys across actual before/after server commit interruptions,
preserving old pins. The [held-driver custody discriminator](evidence/p08-export-destination-custody-native-20260924.json)
passes at92550c9 with unchanged4e ELF; source-isolated Arc removal fails the same
fixture. This is sampled exclusion during actual pending driver drain; original
failed fixture attempts remain INCOMPLETE. Actual export delivery remains open.
This does not complete Step 1 or 3.
[The initial JSONL encoder](evidence/p08-export-jsonl-codec-mac-20260924.json)
has seven Mac tests over independent literal signed bytes, output bounds,
short writes and cancellation, including a source-isolated negative control.
Its filesystem consumer now writes complete private units; durable job execution
and restart remain unfinished. This codec evidence alone
does not qualify native file export.
The [initial manifest encoder](evidence/p08-export-manifest-mac-20260924.json)
now matches an independent Python byte oracle and the closed artifact schema.
Five Mac tests exercise exact selection/cut metadata, output bounds, empty
digest and native snapshot range. The [filesystem prerequisite](evidence/p08-export-files-mac-20260924.json)
has seven actual Mac file tests and a discriminating flush-cancellation control;
the research executable writes the independent signed fixture and exact manifest.
It uses fixed bounded staging, retained output custody and fresh-inode identical
retry. [Native filesystem qualification](evidence/p08-export-files-native-20260924.json)
executes thirteen main functional/fault cases, one bind preparation and one actual
bind refusal at d40b937 using unchanged c685bb3 ELF. This includes fsync EIO, three
SIGKILLs, short writes, ENOSPC and cross-process locking. The original mistargeted
statx fixture remains INCOMPLETE. No power-loss or SQL/Control job qualification;
the automatic consumer is not implemented yet. No Step 3 completion.
[Native destination qualification](evidence/p08-export-destination-native-20260923.json)
at a75d0fa passes ten actual Linux/ext4 cases, including bind aliases, cross-process
custody, fsync EIO and process death around rename. The marker's independent
compact bytes/digest and retained failed writes are checked. No power-loss,
export-job, standalone artifact or installation completion is inferred.

**Skew and output accounting:** reserve input/decompression/decoded-row/output/driver bytes before loading a unit and include JSON escaping or Parquet row-group/native memory in the job grant. One oversized first record is split only by a format that preserves its complete semantics, otherwise refused with exact scope; item count cannot exempt it. External-sort/spill work uses a finite scratch allowance. Export manifest paths are relative, normalized and unique with no parent traversal, disallowed symlink or platform-colliding names; referenced files cannot escape the artifact root. Run skewed large-content/repeated-tag/document fixtures, a slow download, one source dominating all chunks and full scratch disk alongside monitoring/live ingestion; retain peak RSS/cgroup/native/temp usage and independent progress of the reserved control/live shares.

**Artifact publication authority:** extend this task with
`src/export/publication.rs`, `src/interfaces/management/artifacts.rs`,
`migrations/postgres/0804_artifact_publication.sql` and
`contracts/artifacts/v1/publication.schema.json`. Extend migration 0501's common
`artifacts`/`artifact_files` inventory, first consumed by export and shared with
P05-T01, through additive migration 0804 and `artifact_publications`; never
recreate graph-owned files under a second retention
identity. The small PG catalog binds
artifact ID, immutable manifest/file digests, source cut/visibility-policy basis,
storage reference, owner, revision and private/verified/published/withdrawn/expired
state. Completion of an export leaves it private; incomplete artifacts cannot
be published. Commands are `bigbrotr artifact publish|withdraw ARTIFACT_ID`
and the corresponding private routes above, with expected revision and stable
idempotency key. Publish and withdraw have explicit independent action grants;
ordinary export/private-export or signer authority does not imply either grant.

Before publication, verify manifest/reference closure and that all inputs are
currently public-eligible. Private export is not declassification. Commit public
catalog state and retention pin atomically before reply; retries after lost reply
resolve the same publication. Public listing/download uses that catalog through
the authorized HTTP artifact adapter consumed by P09-T06, never filesystem glob
or an unrestricted storage URL. Policy restrictions invalidate affected artifact
availability until explicit revalidation; existing authorization expires within
the shared five-second bound. A download in progress obeys the same bound.
Withdrawal/expiry stops subsequent discovery/access; independently delivered
copies cannot be recalled. Published artifacts have no automatic expiry unless
explicitly selected and their pinned bytes count in capacity forecasts. Private
retention and removal after withdrawal remain distinct maintenance decisions.

Extend this task's real acceptance with export-without-publish-grant, authorized
publication, lost reply/retry, clean-browser listing/download, withdrawal, policy
restriction and incomplete/altered manifest refusal. Inspect actual downloaded
hashes and catalog/pins; neither a signed Nostr event nor a created file proves
that a research dataset was correctly published.

Implement manifest and GET/HEAD file-byte routes from the interface catalog,
including exact ETag/length, single-range resume, authorized conditional requests,
stream revocation and hash verification. Hosted assertion contexts use the same
catalog/pin/visibility authority with a typed artifact kind; P07-T08 signs their
verified configured URL/digest only after eligible bytes are published. There is
no separate unaudited static file server or signer-implied publication grant.
Exercise every route with the actual packaged HTTP handler and independent client.

## P08-T05 — Consistent backup, isolated restore and positive failover

**Files:** create `apps/bigbrotr/src/recovery/{backup,restore,inventory,fencing,verification}.rs`, `apps/bigbrotr/src/interfaces/cli/{backup,restore,ownership}.rs`, `contracts/artifacts/v1/backup.schema.json`, `docs/operations/{backup,restore,failover}.md`, `packaging/compose/restore-lab.yaml`, `apps/bigbrotr/tests/backup_restore.rs`, `tests/acceptance/restore_oracle.py`.

**Inputs/outputs:** P02 snapshot/checkpoint APIs plus control-state transaction/version, archive catalog snapshot and all referenced objects, dictionaries, pending durable state, receipt/outbox/deletion/policy state and replay frontiers produce a backup manifest with consistency protocol, required dependencies, encryption references, hashes and protection class. Signing secrets are separately backed up only by explicit encrypted secret-backup policy; absent keys restore publishers disabled. A backup does not claim current uncommitted work or host-loss protection merely because local files exist.

- [ ] **Step 1:** implement a coordinated backup barrier/pin procedure from P02 contracts. Keep live ingestion safe during bounded capture; record the precise pending/archive/serving cuts and what can be replayed. Copy referenced immutable files before releasing pins. Failed uploads do not mark the backup verified or permit referenced-file GC.
- [ ] **Step 2:** implement verification of object inventory/digests/schema/receipts, then isolated restoration into new roots and fresh control endpoints. Validate path destinations; never overwrite a running deployment from the web. Rehydrate ownership with a new incarnation and require positive exclusion of old writers before any mutation resumes.
- [ ] **Step 3:** implement `restore inspect`, `restore plan` and `restore apply` with expected target identity, explicit destination and fenced activation. Keep restored publishers and public listeners disabled until operator activation; preserve pending exact signed bytes/receipts without automatically resending during the verification drill.
- [ ] **Step 4:** propose `cargo test -p bigbrotr --test backup_restore`: missing object, altered dictionary, stale cut, absent secrets, wrong engine version, lost archive ACK and backup cancellation. Negative control: a complete object listing with one corrupt retained event must fail the independent logical digest/signature oracle; uploaded bytes alone cannot satisfy verification.
- [ ] **Step 5:** proposed `backup-restore` scenario backs up during fixture acquisition, kills a publisher/indexer between commit and ACK, restores outside the original network/volumes and runs HTTP/relay reads, provenance checks, source resumption and outbox inspection. Pause/resume an old owner past TTL and partition control; takeover remains refused until positively fenced. Retain cut vectors, exact signed-ID/digest comparisons and per-target receipt equality.

**Failure contract:** backup `uploaded`, `verified` and `restore-tested` are different states with timestamps. Unrecoverable loss requires explicit scoped acknowledgement and a visible gap; it never advances a complete frontier silently. An ordinary portable export cannot be accepted as a resumable deployment backup.

**Isolation and secrets:** restore drills use different dataset-root/control endpoints, disabled outbound publishing and blocked production destinations. Inspect/restoration must not create a competing ingest/archive owner. Decryption references, protected database credentials and optional signer recovery are validated separately; missing key material leaves the drill unverified for the affected scope, not successful with empty data. Do not pack plaintext signer secrets into ordinary backup manifests. A successful drill preserves the original backup and creates new verification evidence; it does not consume the only copy or mutate the live deployment. Test deliberately misconfigured restored outbound destinations and require the independent socket observer to see no send.

**Pressure boundary:** copying, hashing, compression, engine checkpoints and upload buffers draw from the same host/role budget as other maintenance. Fill backup destination or temporary storage mid-copy; no final manifest/verified state appears, source pins remain until resumable recovery or an audited abandonment, and cleanup touches only that job's staged objects. Native workers that ignore cancellation retain their permits until joined or positively terminated. Repeating timed-out backup/verify commands cannot create an unbounded population of abandoned processes or pinned snapshots.

## P08-T06 — Schema upgrades and legacy migration

**Files:** create `apps/bigbrotr/src/recovery/{migration,legacy_import,compatibility}.rs`, `apps/bigbrotr/src/interfaces/cli/migrate.rs`, `migrations/manifest.json`, `contracts/artifacts/v1/compatibility.schema.json`, `docs/operations/{upgrade,rollback,legacy-migration}.md`, `tests/fixtures/migration/{supported-old-release,incomplete-go,full-go}/manifest.json`, `apps/bigbrotr/tests/migrations.rs`.

**Dependencies/interfaces:** P02 versioned engine adapters provide inspect/prepare/apply/checkpoint/verify for PG, Rocks, archive and CH; logical body/evidence identities survive migration. Release manifest defines reader/writer/archive/pending/API compatibility and which downgrade requires restore. Legacy migration reads an explicitly frozen/exported source, not the concurrently changing development repository or live database without a supported snapshot.

- [ ] **Step 1:** implement `migrate inspect|plan|apply|resume` with single-owner fencing, required free-space estimate, backup reference, ordered transactional/nontransactional steps and durable checkpoints. Block incompatible writers before changing format; no automatic startup DDL requiring broad admin grants.
- [ ] **Step 2:** implement legacy full-event import with original metadata/claimed provenance preserved. Inspect source journal/export for missing signed fields; direct incomplete rows to standalone analytical use or an explicit best-effort refetch job; no in-product projected-data importer is selected. Record unrepairable gaps without fabricated content/signatures.
- [ ] **Step 3:** implement compatibility refusal and forward-repair/restore choices for partial changes. Rebuild optional indexes from retained truth; changing an index is not a rewrite of original observation identity. New schema defaults do not invent prior facts.
- [ ] **Step 4:** propose `cargo test -p bigbrotr --test migrations`: crash before/after each commit/checkpoint, two migrators, stale writer, unsupported future schema, insufficient reserve and interrupted index rebuild. Negative control: an old binary pointed at a format with a higher writer floor must fail before its first write, even if it can parse some rows.
- [ ] **Step 5:** proposed `migration` scenario installs the prior supported release bundle on fresh volumes, loads exact signed fixtures, upgrades with the documented command, compares old/new logical corpus and reads, then attempts the documented rollback path. Separately migrate full/incomplete legacy fixtures and verify honest capability/gap reports. Preserve commands, compatibility manifests and before/after corpus digests.

**Failure contract:** no in-place mutation of historical repositories during this work. Package rollback and data rollback are explicitly different. Schema migration credentials are scoped and absent from the ordinary read process.

**Crash matrix:** nontransactional CH/file changes have prepare/verify/activate checkpoints with explicit old/new reader compatibility. At every boundary, kill the migration process, restart the previous/new binary as permitted and verify that exactly one supported recovery path exists. A prepared new table or copied object cannot become active merely because it exists. Source import corruption cannot downgrade validation or make partially staged controls visible to other jobs.

## P08-T07 — Scrub, repair, compaction and differentiated retention

**Files:** create `apps/bigbrotr/src/recovery/{scrub,repair,retention,takedown}.rs`, `apps/bigbrotr/src/interfaces/cli/{scrub,repair,retention}.rs`, `contracts/management/v1/maintenance.schema.json`, `docs/operations/{integrity,retention,takedown}.md`, `apps/bigbrotr/tests/maintenance.rs`, `tests/fixtures/repair/manifest.json`.

**Interfaces:** maintenance consumes immutable artifact inventory, live pins, backup/export references, policy revisions and budget grants. Scrub outputs checked scope, verified/corrupt/unreadable units and resumable position; metadata inspection is not a full-content scrub. Repair outputs a new verified replacement and lineage, preserving suspicious original bytes until explicit disposal. Retention planner returns reclaimable units with every protecting reference and reason.

- [ ] **Step 1:** implement bounded resumable checksum/structure/signature scrub using independent decoders where meaningful; quarantine by exact unit/scope and stop completeness advancement over unverified gaps. Large corrupt tails must not be loaded in one allocation or scanned implicitly at startup.
- [ ] **Step 2:** implement copy/verify/activate repair, archive compaction and index rebuild through owner contracts. Preserve stable record IDs, first-observation corrections, coverage and deleted/current policy. Cancel between units without removing the only verified copy; fail when temporary reserve is inadequate.
- [ ] **Step 3:** implement separate retention classes: signed archive without automatic age/score expiry; useful raw measurement attempts and immutable document evidence permanent by default under central A05/A06; lossy aggregation/expiry only under an explicit operator policy declaring missing temporal detail and protected references; pinned documents retained while referenced; derived runs retained while active/citable; request/outbox replay tombstones retained through retry horizon; operational retention from central A09: redacted logs 14 days, detailed terminal jobs and resolved delivery detail 30 days, private audit 365 days; Prometheus/raw operational metric samples initially 30 days and optional raw diagnostic captures at most 24 hours under explicit size/security policy. Duplicate deliveries, retry-loop counts and queue samples stay operational telemetry and are not appended as permanent archive facts. Historical measurement attempts and document changes retain evidence/method/source semantics.
- [ ] **Step 4:** implement policy-only takedown separately from exceptional physical removal. The latter requires an explicit administrator action, exact affected inventory, backup/export consequences and a new audited gap; it is never a rank or NIP-09 automatic action. Deleting a document still referenced by observations fails the retention plan.
- [ ] **Step 5:** propose `cargo test -p bigbrotr --test maintenance` and `maintenance` acceptance: corrupt one large artifact, interrupt repair, pin an old snapshot/export, exhaust compaction reserve, expire operational telemetry and run repeated duplicate ingestion. Independent oracle compares retained signed IDs/metadata before/after; negative control removes a live pin from a deliberately faulty candidate retention set and requires the separate inventory validator to reject reclamation. Event/provenance truth remains unchanged when telemetry expires.

Add a document-skew control: one unchanged document fetched repeatedly produces many compact actual attempt facts but one same-visibility document object, while queued/deferred work creates no fictional measurement attempts. A repeated event delivery changes only operational counters unless it contributes new canonical evidence. The independent A01/A02/A05/A06 family counts and byte growth distinguish these cases after compaction and telemetry expiry. A lossy measurement-retention policy must leave an explicit lost-resolution descriptor; default maintenance preserves the raw facts.

**Failure contract:** “nothing checked” cannot be scrub success. Low disk triggers safe throttling and an actionable maintenance result, never undocumented archival deletion. Rollup exactness only covers its declared sufficient statistics and time resolution; it cannot answer lost per-attempt questions.

## P08-T08 — Diagnostics, measured capacity and delivered alerts

**Files:** create `apps/bigbrotr/src/operations/{doctor,diagnostics,capacity,telemetry,probes}.rs`, `apps/bigbrotr/src/interfaces/cli/{diagnostics,capacity}.rs`, `packaging/observability/{prometheus.yaml,alerts.yaml,alertmanager.yaml}`, `docs/operations/{diagnostics,capacity,alerts}.md`, `docs/user/runbooks/{coverage-stall,ingest-halt,durable-write,backlog,network-dark,replication-lag,backup-failure,corruption,policy-expired,compaction-debt}.md`, `apps/bigbrotr/tests/diagnostics.rs`, `tests/acceptance/alert_receiver.py`.

**Input/output:** doctor executes bounded read-only probes of declared engines/routes/ports/permissions; diagnostics exports redacted config sources, role/revision/progress, resource usage and correlated errors. Capacity reports measured bytes/rates/time windows, observed amplification and finite outage headroom, with unknowns rather than universal relay-count claims. Health probe permits remain held until underlying work actually stops.

- [ ] **Step 1:** implement role-aware liveness/readiness and coalesced driver probes; distinguish no data/idle, stale usable views, remote relay failure and local inability to measure. Preserve old last-success age without calling it fresh. Exercise uncooperative probes via a terminable fixture process.
- [ ] **Step 2:** implement low-cardinality metrics with units and accounted native/cgroup memory, queue bytes/age, work permits, fsync, replay, frontiers and compaction reserve. Keep per-relay/private detail in authorized query state. Capacity calculations use measured rates and explicit observation interval; duplicate ingest rates are expiring telemetry.
- [ ] **Step 3:** implement diagnostic bundle preview/redaction/size cap and receiver-local alert configuration. Ship all named runbooks and rules: stall requires pending eligible work; disabled roles/absent replicas do not alert; upstream outages inhibit consequential backlog alerts but never independent corruption/policy failure.
- [ ] **Step 4:** propose `cargo test -p bigbrotr --test diagnostics`: secret canaries, private URLs, malicious error labels, DNS/proxy failure, expired policy, idle registry and repeated stuck engine probes. Negative control holds one engine call active across several probe deadlines; independent process instrumentation must observe one bounded outstanding call, not only timely HTTP responses.
- [ ] **Step 5:** proposed `alerts` acceptance injects each rule condition into running fixtures and follows evaluation → grouping/routing → local receiver → inhibition → resolution. Retain receiver payloads and timestamps, rule/engine versions, runbook links and expected inhibition graph. No actual external Slack/email/webhook receiver is used. A rule test passing while the local receiver is disconnected must fail end-to-end acceptance.

## P08-T09 — Native/image packaging and self-contained release bundle

**Files:** create `packaging/image/Dockerfile`, `packaging/image/Dockerfile.dockerignore`, `packaging/compose/{full-local,archive-only,sync-only,monitor-only,serve-existing,relay-only,split-roles,fixture}.yaml`, `packaging/native/{bigbrotr.service,bigbrotr.env.example}`, `packaging/release/{assemble.py,verify.py,manifest.schema.json}`, `packaging/release/assets.list`, `docs/user/getting-started/{compose,native}.md`, `tests/acceptance/bundle_inventory.py`.

**Artifacts:** Linux amd64/arm64 binary packages; one multi-platform application image index; separate dashboard/admin static bundles; pinned engine/extension compatibility set; minimal recipes, versioned seeds, service units, migrations, checksums, SBOM and manifest. Node is a build dependency only. Every mount/reference resolves inside the extracted bundle or is an explicit operator-created data/secret root. Runtime DuckDB extensions are bundled and verified; no hidden network download is required to start archive work.

- [ ] **Step 1:** implement multi-stage release builds from pinned Cargo/toolchain/native dependencies and frontend artifacts. Exclude `.env`, caches, `.research`, worktree metadata and secrets by allowlisted asset assembly; use the clean repository root as build context with the Dockerfile-specific ignore file and allowlisted inputs. Inspect the transferred context and image layers as well as the final filesystem.
- [ ] **Step 2:** implement non-root image permissions, read-only executable/assets, declared writable data/temp/secret paths, health command and signal forwarding. Package service units with explicit user/directories/limits and environment reference; no shell pipelines in role startup.
  Provide compact and isolated application placement without a daemon per logical
  role. Compact Control/acquisition/ingest/storage/read retains exclusive handles,
  aggregate memory accounting and terminable on-demand native jobs. Native and
  Compose guides show the same role set, explicit finite initialization/migration,
  protected credential files and local or remote engines. The local archive needs
  no object server; optional S3 follows its separate qualified storage contract.
- [ ] **Step 3:** implement bundle inventory/resolution checks and compatibility manifest. Container engines use tested versions/digests. Record image-index digest separately from per-architecture manifest digests; a successful amd64 image cannot stand in for arm64 support. Select Docker Hub under the provisioned project namespace, retain private candidate visibility, and test the final index by digest with its SBOM/provenance before promotion.
  The installed worker prerequisite embeds its exact length/hash in the main
  executable after an ordered build and exposes `archive verify-worker` against
  that anchor. [Native prerequisite qualification](evidence/p02-installed-worker-binding-native-20260919.json)
  exercises the relocated actual CLI and same-cache binding transitions. The full
  fresh builder was not executed in that run. Bundle-byte inspection does not
  qualify catalog startup, image permissions, outer release authentication or the
  whole installation journey.
- [ ] **Step 4:** proposed `python3 packaging/release/assemble.py --output artifacts/release-candidate` followed by `python3 packaging/release/verify.py artifacts/release-candidate`; verify clean build and reused-cache build contain exactly allowed assets and no canary development credential. Negative control remove a required seed/config mount from an assembled test copy; inventory validation must fail before launch.
- [ ] **Step 5:** proposed `bundle-install` acceptance extracts the archive outside the checkout with a clean image cache, resolves mounts, initializes secrets/config, runs Compose and native documented installs on their actual supported architectures, obtains durable fixture admission/archive/read, opens local interfaces and shuts down. Retain installed file digests, process UID/binds, extension availability and output receipts. No checkout path or locally cached undeclared asset may rescue installation.
  Include fixed sync-only and monitor-only without CH, and serve-existing with
  remote CH plus matching Control/PG. Each guide must identify all commands,
  required credentials, expected role/status output and a real result; helper
  assembly or Compose config validation alone is not an install result.
  Include explicit engine provisioning, finite initialization/migration and runtime
  doctor before serving; offline validation never claims schema readiness. Render
  and inspect recipe services/roles before launch; an intentionally targeted
  Compose profile/dependency cannot bypass application role permission.

**Failure contract:** missing architecture runner, extension or required executable produces NOT RUN/failed release evidence. A discovered but unexecuted required scenario cannot be summarized as green.

## P08-T10 — Independent roles, networking and rolling replacement

**Files:** create `config/recipes/split-role.toml`, `packaging/compose/{split-roles,private-proxy,host-network}.yaml`, `docs/operations/{role-topologies,networking,rolling-replacement}.md`, `apps/bigbrotr/tests/topology_contract.rs`, `tests/acceptance/topology_oracle.py`; extend `apps/bigbrotr/src/runtime/{ownership,shutdown}.rs` and `apps/bigbrotr/src/interfaces/rpc/topology.rs` only through P01/P02 contracts.

**Input/output:** selected roles plus explicit engine/owner endpoints determine required durable links and finite queues. One image may run multiple roles or one role; an embedded store has one owner. Remote producers use authenticated durable receipt RPC, not a shared writable Rocks directory. Public proxy routes static dashboard and `/v1/` only; management/metrics/database ports remain private.

- [ ] **Step 1:** provide full local, fixed sync-only, monitor-only, archive-only, serve-existing (including relay-only), no-provenance, social-filter and split-role recipes. Preflight reports missing sources, ready capabilities, write owners, resources and expected binds. No recipe name changes archival fidelity. Use the deployment contract's concrete endpoint/credential/TLS roots and conditional archive/Index routes; PG/Control policy/frontiers/pins remain required for serving CH.
- [ ] **Step 2:** implement bridge/container-loopback and explicitly supported host-network instructions with observable host reachability. Doctor reports the effective contract rather than assuming container loopback means host loopback. Apply trusted-proxy/CORS policies only to configured hops/origins.
  The default is per-host bridge Compose or native units with routable remote
  endpoints; no cross-host Compose DNS assumption. Private infrastructure endpoints
  are operator roots, independent from public-relay SSRF policy. Test valid private
  PG/CH/RPC alongside a denied discovered relay at the same address, wrong CA/name,
  credential rotation and missing per-role grants. Read roles receive no migration
  or unnecessary archive/write secrets. Inspect both actual application bind and
  host publish address; host networking ignores port publishing restrictions.
- [ ] **Step 3:** implement documented shutdown/replacement order: stop new intake, drain bounded admitted work, persist recovery, positively exclude old owners, then activate replacements. Follow the delivery contract's ordinary Compose upgrade: verify/fetch exact digests, compatibility/backup preflight, finite migration, recreate selected services, doctor and real data checks. Declare bounded downtime and the actual binary-rollback boundary; never remove volumes to update. Lease defaults 60s/20s heartbeat/5s renewal/20s margin and 30s drain remain distinct from five-second policy authority; running out of authority shortens drain.
- [ ] **Step 4:** proposed `topology` acceptance starts/stops each role against real retained data, interrupts PG/CH/archive/downstream RPC independently, and proves finite buffering and visible capabilities. Negative control pauses an old owner beyond TTL then resumes it after takeover; any accepted stale durable mutation fails the external receipt/commit oracle, regardless of lease-row state.
  Exercise actual two-host collector/data-owner placement with local archive jobs
  confined to the data host, distributed archive workers with a qualified object
  route, and standalone remote serving. A same-named local archive path on another
  host must fail reachability; verify the real route at each consumer. Populated CH with empty
  or mismatched PG cannot become ready. Stop Index maintenance and let an ordinary
  view deadline expire: its capability stops while eligible immutable reads remain
  qualified. No outage may expand a fixed relay list or start an unselected worker.
- [ ] **Step 5:** run rolling API/static-asset replacement and cursor-key rotation with active pages and subscriptions, private policy revocation and old client assets. Preserve compatible assets or require an intelligible refresh; retain network ACL/bind inspection, key-ID rotation receipts and no-leak probes from a public-side container.

## P08-T11 — Trusted release workflow and compatibility promotion

**Files:** extend P01's `.github/workflows/ci.yml`; create `.github/workflows/{release-candidate,release}.yml`, `packaging/release/{compatibility.json,licenses.py,provenance.py}`, `SECURITY.md`, `SUPPORT.md`, `docs/user/releases/{policy,compatibility}.md`, `tests/acceptance/release_manifest.py`; modify `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml` and frontend lockfile only as deliberate tested version updates.

**Interfaces:** an immutable detached candidate artifact manifest binds backend, engine/native versions, API/management schemas, static assets, docs line, migrations, platform digests and required acceptance cases. Execute evidence against its digest, then create a separate detached release manifest binding candidate/artifact/evidence digests and sign it. Internal bundle inventories never hash themselves or their enclosing archive; the final manifest stays outside the bundles it hashes. Follow the acyclic construction in the delivery contract. Promotion copies verified artifacts unchanged from the private candidate repository to the separate release repository. Post-promotion receipts reference the signed manifest without rewriting it. Planning/private preview does not authorize public publication or repository rename.

- [ ] **Step 1:** split untrusted PR validation from credential-bearing promotion, with least-privilege tokens and no execution of PR code using publication credentials. Include dependency/native license inventories and selected project license before distribution; keep every initial Cargo package `publish = false`. Use isolated disposable PR runners, no privileged execution of PR code, separate trusted caches, and no Tux/registry/signing credentials in PR jobs. Pin actions and tool/base-image identities; Tux executes explicitly selected trusted revisions only.
- [ ] **Step 2:** implement MSRV/current-toolchain checks, Linux architecture builds, pure/protocol/engine/packaging/frontend/docs jobs and required-scenario accounting. Mark actual supported engine ranges in compatibility metadata; optional experiments do not expand those ranges.
- [ ] **Step 3:** implement checksums, SBOM, build provenance, immutable release manifest and operator-visible migration/rollback limitations. Apply the selected Minisign exact-manifest authentication, protected release authority, private test-key rehearsal, version/digest checks and key rotation/revocation policy. Keep signature verification keys and update channel rooted in documented release authority; never trust a checksum fetched from an unrelated user-supplied location as release authenticity.
- [ ] **Step 4:** proposed `release-manifest` acceptance validates cross-artifact references, architecture identity, schema ranges, documentation links and retained evidence completeness. Negative control swaps only the admin bundle or one image-platform digest from a different candidate; promotion must reject the internally inconsistent manifest. Also reject wrong signature key/version, stale evidence, absent required attestation and unexecuted target architecture. Verify the final candidate index and child digests, then assert destination equality after promotion.
- [ ] **Step 5:** rehearse promotion to a private test destination, then rollback of presentation metadata to the previous immutable release. Product data rollback remains P08-T06/P08-T05. Retain provenance verification and artifact hash equality; do not send public announcements or rename repositories as part of this task's tests. Use the provisioned private Docker Hub destination, unchanged verified artifacts and non-reassignable version tags; no application rebuild during promotion, GHCR dependency or automatic production deployment on push.

## P08-T12 — Whole-release operator acceptance and evidence review

**Files:** create `tests/acceptance/cases/{operations.py,operations_scenarios.json}`, `tests/fixtures/release/manifest.json`, `docs/operations/release-validation.md`, `docs/user/runbooks/recovery-without-browser.md`.

**Dependency/output:** all P08 tasks plus delivered P01–P07 product; P09/P10 supply their browser/tutorial results. Output is a reviewable result set, measured operating envelope and release blockers, not a coverage percentage or automatic certification.

- [ ] **Step 1:** implement the exact scenario registry named above with setup/action/monotonic condition/timeout/oracle/evidence/cleanup. Reuse the independently signed corpus manifest and controlled relay/engine processes; never depend on public relay reachability for deterministic success.
- [ ] **Step 2:** run proposed `python3 tests/acceptance/run.py p08-t12 --binary dist/bigbrotr --compose packaging/compose/acceptance.yaml --evidence .research/implementation/p08-t12` through extracted shipped artifacts. Execute empty-volume bootstrap, one sub-threshold event with both flush deadlines, provenance-off replay, each isolated role and public-disabled listener/signing checks.
  Include the installed native and Docker fixed-list sync-only, monitor-only and
  serve-existing journeys plus split-host data/authority reachability. Observe
  actual processes, packets and engine rows for absent workers; an enabled-list
  printout does not prove independence or zero unintended publication.
- [ ] **Step 3:** perform concurrent live/backfill/import/read/rank/export/compaction, introduce engine outage and low disk, inspect fair progress/byte budgets, then backup/restore/upgrade via CLI. This is a workload with explicit fixture size/rates/duration and hardware manifest, not a claim of 100k-relay capacity; larger capacity tracks retain their own measured results.
- [ ] **Step 4:** compare signed corpus/edge origins, published cuts, counts, docs/example outputs and job/outbox receipts with independent manifests and slow/alternate-engine oracles. Negative control injects a stale-success result file from a different candidate and requires rejection by manifest digest, even when every individual status says pass.
- [ ] **Step 5:** review actual failure recovery and NOT RUN cases, update installed-version runbooks with observed limitations, and retain the final artifact/evidence mapping. No phase is complete while a required advertised role or recovery journey is unexecuted. Product publication remains a separate authorized release action.

## Revision 1 — Initial completeness and reconciliation

The first drafting pass found and repaired these cross-contract omissions: a distinct `apps/admin` artifact; canonical `packaging/` rather than duplicate deployment trees; management backend ownership before browser work; cursor-key persistence/rotation in actual rollout; receiver-level alerts; archive-document dependencies in standalone exports; old-owner positive exclusion during restore; and operational telemetry expiry separated from useful archive measurements. Password/session mechanics and local recovery are made concrete here without introducing an external identity provider dependency. The subsequent numbered records below describe the concrete semantic, recovery, resource and release corrections actually made to this specification; none records execution of the proposed product tests.

## Revision 2 — Semantics, privacy and file ownership

Replaced the inherited 365-day raw-monitor expiry with permanent useful A05 measurement facts and referenced A06 documents; optional lossy expiry is explicit. Adopted central A09 operational TTLs and protected replay obligations. Unified the P01 runner/result schema, config schema/recipes, CLI root/submodules, P03 importer and canonical docs/operations paths. Added exact private route/HTTP error mappings and durable job transitions so UI progress, desired config and COMPLETE data cannot be conflated. Chose an explicit bounded Argon2id baseline instead of leaving password behavior unspecified.

## Revision 3 — Crash, cancellation and recovery authority

Added the atomic accepted-command/job/pin boundary and lost-reply recovery, explicitly detaching durable jobs from browser request contexts only after commit. Restore drills now isolate roots/control/destinations and distinguish unavailable decryption or signer recovery from a verified empty restore; outbound-socket observation proves no unintended publication. Migration checkpoints explicitly prevent prepared files/tables from becoming active after a crash. Positive exclusion remains a prerequisite for write-owner replacement, independent of TTL or cockpit health.

## Revision 4 — Skew, disk reserve, native memory and fairness

Added aggregate decoded/output/native byte accounting and oversized-first-record refusal to portable jobs, safe artifact paths, bounded spill and slow-client tests under concurrent live/control work. Backup pressure now retains correct pins without minting verified manifests and prevents abandoned native workers from freeing permits early. Added an independent family-count/byte-growth oracle distinguishing repeated event traffic, actual measurement attempts, unchanged deduplicated documents and mere queue deferrals. Telemetry expiry and compaction cannot alter archival truth.

## Revision 5 — Actual artifacts, documentation and advertised command coverage

Checked all task IDs, numbered actions and local document links against the common harness and current phase owners. Added missing backend ownership for private saved views and optional-index rebuild/retire, with exact endpoints/schema/migration and real P06 generation contracts. Release acceptance binds the binary, image/index, frontend bundles, engines/extensions, schemas, docs and evidence to one immutable manifest; install tests remove a required mount and reject wrong-architecture or cross-release artifacts. Generated config/CLI/reference and importer/tutorial prose have one owner, and missing required suites remain NOT RUN/fail rather than an empty success.

### Final task-to-entrypoint and evidence map

| Case | Actual proposed entrypoint(s) | Required independent observation |
| --- | --- | --- |
| `p08-t01` | Released `config init/validate/show`, `serve`, bounded `run`, `doctor` | Single-authority desired/applied revisions, staged drift/locks, actual binds, role effects and no unauthorized signing |
| `p08-t02` | Private enrollment/session/grant/token routes and local recovery CLI | Permission matrix, audit, public-network refusal and old-controller exclusion |
| `p08-t03` | Configuration preview/apply, role command, durable job and saved-view routes | PG receipts/revisions, actual role application, resumed job and reauthorization |
| `p08-t04` | Import/export CLI and typed jobs; independent DuckDB reader | Full signed fields/IDs, claims, referenced objects and standalone manifest hashes |
| `p08-t05` | Backup/verify/restore/ownership CLI plus isolated engines | Coherent cuts, logical corpus, pending/outbox receipts, no unintended output |
| `p08-t06` | Migrate inspect/plan/apply/resume and supported old/new binaries | Writer compatibility, crash checkpoint and exact logical identity preservation |
| `p08-t07` | Scrub/repair/retention CLI and maintenance jobs | Protected pin inventory, preserved facts, deduplicated objects, corruption scope |
| `p08-t08` | Doctor/status/diagnostics, Prometheus, Alertmanager, local receiver | Bounded probes, delivered/inhibited/resolved alerts and redaction |
| `p08-t09` | Extracted native/image bundle and documented installation | All mount/assets, actual architecture/UID/binds, first archived/read event |
| `p08-t10` | Same image in individual/split roles and public/private proxy recipes | Remote durable receipts, finite outage behavior, fenced old owner, ACLs |
| `p08-t11` | Private candidate/promotion workflow and independent artifact verifier | Unchanged promoted digests, complete compatibility/provenance, trusted execution |
| `p08-t12` | Whole delivered operator journey plus P09/P10 evidence references | Corpus/receipts/config oracles and complete named required-scenario accounting |

Every row runs through the shared runner with its exact case ID, candidate binary and Compose file, writing its matching `.research/implementation/` directory. The orchestrator records the detached candidate artifact manifest path/digest and extracts its bound release bundle to an isolated directory; a binary path alone is insufficient evidence that its associated deployment bundle was installed. Acceptance results then feed the separate final release manifest, never mutate the candidate identity. Assembly-only helper results never replace that journey. Later tasks extend previously created files, such as split-role Compose and canonical guides, rather than create competing copies.
