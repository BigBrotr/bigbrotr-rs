# Go read and runtime implementation walkthrough

Date: 2026-09-11. This is a bounded, static reading of actual Go implementation and selected complete test files. It supplements the original-phase review with evidence about what the current worktree really wires and executes. It is neither a Go implementation audit nor validation of the planned Rust implementation.

The valuable inheritance is a set of contracts: actual CLI composition, owned query inputs, admission at every executable entry point, parameterized statements, truthful refusals, consumer-paced delivery, and explicit lifecycle ownership. Several historical abstractions need replacement: a shared factory is not a shared resource budget; a row limit is not a scan limit; a scheduler cadence is not measured freshness; and a signed cursor is not necessarily a usable continuation.

## Evidence and scope

The [source manifest](evidence/go-read-runtime-code-sources.json) records every fully read file, complete SHA-256, byte and line counts, capture time, source HEAD metadata, and original hashes immediately before and after copying and at the final check. The source root was `/Users/vincenzo/Documents/GitHub/BigBrotr/bigbrotr`. Original files were copied into new files below `.research/code-snapshots/read-runtime/`; they were never moved or modified. Ignored snapshots are named as code paths rather than published Markdown links.

All 43 selected files were read completely: 11,514 lines and 494,788 bytes. These totals describe the completed selection; they were not a quota. The selection follows CLI startup, shared composition, supervisor ownership, HTTP admission and pagination, the full-event SQL execution path, current-version resolution and hydration, and the ArchiveRelay registration boundary. Four complete test files were read for discriminating contracts. DVM was followed through production construction only. Its intake, ledger, execution and response-publication internals remain outside this bounded code pass; the separate analytics/publication walkthrough covers Deriver, Ranker and NIP-85, not those DVM internals. Earlier complete planning-document reads are not counted as implementation coverage.

All captures reported source HEAD `757a8d899aefb277c0211077bc30ba0c8de68796`, obtained by reading Git metadata directly, without a Git command. This identifies repository metadata, not proof that worktree files equal that commit. No selected original changed during copying or between capture and the final source comparison at `2026-09-11T07:03:24.733253+00:00`. Concurrent work elsewhere in the legacy repository is outside that statement.

No Go tests, builds, formatting, services, probes or dependency code were executed. Historical measurements and third-party behavior described in Go comments remain attributed historical claims. The analysis below distinguishes **observed implementation**, **static inference**, and **Rust recommendation**. File and line references always refer to the frozen bytes named by the manifest.

Supporting design destinations are [runtime configuration](../engineering/configuration-and-runtime.md), [read/publication architecture](../architecture/derived-read-publication.md), [protocol evolution](../architecture/protocol-evolution.md), [search and reconciliation](../architecture/search-and-reconciliation.md), and [security/recovery validation](../engineering/security-recovery-and-validation.md). This walkthrough does not edit those contracts or claim their acceptance tests have run.

## 1. CLI to real composition and process ownership

The observed startup chain is:

```text
cmd/bigbrotr/main.go:run
  → cli.NewRootCommand → command.Execute
  → serve RunE → loadConfig → ValidateStartup → signal context → app.Run
  → app.Run: PostgreSQL open/ping → schemas → optional ClickHouse open/ping
  → journal/dedupe resources → shared blocklist → BuildSupervisor
  → bind enabled listeners → run supervisor/listeners → drain/close/report
```

`cmd/bigbrotr/main.go:25`, `internal/cli/root.go:18`, and `internal/cli/serve_cmd.go:24` show a real command dispatch. `serve` reaches `app.Run` at `serve_cmd.go:45`; it is not the old documented phase gate. Dry-run stops before engine construction. SIGINT/SIGTERM flow into the process context. `internal/app/run.go:83` validates startup before opening engines; PostgreSQL is opened and pinged at 102–107, while ClickHouse construction is conditional at 138–145.

`internal/cli/run.go:90` selects the requested service and calls the same `app.Run`. `runOnceConfig` at 136 narrows `EnabledServices`, switches to one-shot mode, removes the metrics bind, and revalidates the resulting configuration. `reportRunOnce` at 205 distinguishes complete, halted and failed outcomes rather than printing success merely because construction returned. This earns reuse: use one typed composition path and validate the effective configuration after applying a command's override.

`BuildSupervisor` in `internal/app/services.go:273` constructs actual services from a builder catalog, supplies shared dependencies, and attaches a lease releaser. Listener roles are process resources (`internal/app/listeners.go:149`), rather than fake scheduled workers. The production builders include separate `buildReadCore` calls for API, ArchiveRelay and DVM (`listeners.go:181`, 256, 810; `services.go:952`). Their semantics share a constructor; their Core instances and adapter semaphores are separate. A constructor named “shared” does not establish process-wide fairness or admission accounting.

The DVM construction path is concrete: `internal/app/services.go:933` validates its dependencies, constructs a file signer at 948, builds the read core at 952, then creates the relay publisher, monitored-relay source, cursor store, registry and intake service at 968–984. That is useful composition evidence. It does not prove signed response byte limits, replay prevention, aggregate DVM filter budgeting or error behavior on the wire; those implementation files were not included here.

Two ownership differences matter:

- **Observed:** `internal/cli/run.go:168` treats a successful cockpit `/health` response as evidence that `serve` is active. Missing metrics configuration only emits a warning; a probe error or non-200 response permits continuation. **Inference:** the cockpit probe is advisory and cannot be the authoritative writer-exclusion mechanism. The older comment about monitor not holding dedupe resources is stale against `internal/app/run.go:366–490`, which opens dedupe resources through the current composition. This walkthrough does not claim the lower-level dedupe lock is absent; that belongs to the storage walkthrough. **Rust:** prove journal/storage ownership and fencing independently of health reachability.
- **Observed:** `WorkerID` in `internal/app/leases.go:23–31` combines hostname and PID. **Inference:** that distinguishes simultaneous ordinary processes on a host, but cannot guarantee identity across PID reuse, container restarts or duplicate hostnames. **Rust:** include a process-start nonce, retain host/PID as human-readable metadata, and use storage-enforced fencing for mutations. Identity and fencing solve different problems.

## 2. Cycles, resident work, listeners and shutdown

`internal/supervisor/cadence.go:36` starts one cycle loop per runner. Each runner waits for the preceding runner's initial-start signal, then follows its configured cadence; this is not a guarantee that independent services finish in catalog order. Each loop owns a child context and cancels its resident work when the loop ends (`cadence.go:51–83`). A halted service does not automatically cancel the whole process. `watchCycles` at 86 detects the all-halted case and maintains heartbeat observation.

The cycle budget in `internal/supervisor/supervisor.go:209` requests cancellation through the clock/context machinery. The caller and storage adapter still have to honor it. Passing a context to a driver is necessary, but does not establish a wall-clock upper bound for a driver that never returns. `internal/readcore/core.go:480` checks cancellation before each sink delivery; that check cannot interrupt an already blocked engine `Query` or `Rows.Next`.

`internal/app/listeners.go:443` constructs listeners and closes already-bound listeners if later construction fails. The API uses a dedicated mux (`listeners.go:251–318`); metrics and pprof use a separate explicitly constructed listener/mux (`listeners.go:380`). `serveListeners` at 735 records listener failures and performs shutdown under a fresh drain deadline. If `http.Server.Shutdown` times out, the implementation closes that server at 763–770. `app.runUntilStopped` at `internal/app/run.go:245` joins listener and supervisor outcomes, and `errorWithout` in `internal/app/services.go:455` removes cancellation while retaining sibling failures. These are useful patterns for truthful shutdown reporting.

The limit of that evidence is significant: the listener wait group waits for `Serve` goroutines. This selection did not read khatru's connection registry or prove all upgraded WebSocket work joins that group. Rust shutdown acceptance must include active subscriptions, blocked writers, in-flight storage reads and background refresh tasks; an HTTP listener stopping is only one step.

Three concrete lifecycle refinements follow:

| Observed implementation | Static implication | Rust contract and discriminating acceptance |
| --- | --- | --- |
| `listeners.go:627–653` launches a goroutine for each engine probe and returns a timeout verdict when the caller's deadline expires. | If the engine's `Ping` ignores cancellation, the goroutine remains. Repeated readiness requests can accumulate outstanding work even while each HTTP response is bounded. This is conditional on driver behavior, not an observed leak from an executed test. | Bound and coalesce probes per engine; expire cached verdicts; give the underlying transport an interruptible lifetime. Repeated timed-out probes must not create an increasing population of engine operations. |
| `supervisor/shutdown.go:85–100` first joins under the drain context, then deliberately joins without a deadline if the grace period expires. `background.go:18` similarly cancels and waits for its group. | Waiting avoids closing shared state while a worker can still mutate it, but can make termination unbounded. | Preserve positive worker exclusion before releasing leases or replacing owners. Define escalation to process/transport termination when cooperative cancellation fails; do not report “drained” merely because a timer fired. Exercise a worker that ignores cancellation. |
| Blocklist refresh in `services.go:1212–1255` fails startup for enabled readers when initial database policy cannot load, then retains the last good set on refresh failures. | Initial fail-closed behavior is useful. A last-good set alone carries no bounded lease proving that a later revocation has been incorporated. | Put policy generation and authorization validity into read admission and continuation. After the configured validity expires, refuse protected reads until policy can be established. Test a lost revocation notification as well as a received one. |

## 3. HTTP request to immutable plan to full signed event

The fully traced full-event path is:

```text
API Handler
  → transport/method/query gates and inflight acquisition
  → query.go strict parse → Resolve(registry, request, capabilities)
  → optional cursor decode/replan → Core.Plan
  → prune blocklist → access-path/term admission → run pin → owned filter copy
  → Core.Query → plan validation → cancellation/blocklist sink wrappers
  → ClickHouse ReadStore.Scan → ScanStatement → ByIDStatement
  → parameterized engine Query → row decode → synchronous sink delivery
  → JSON envelope and continuation metadata
```

`internal/api/server.go:234` composes the handler chain; `internal/api/query.go:60` rejects repeated scalar parameters and its allowlist at 232 refuses unknown query parameters. The malformed URL-query gate at `server.go:340` calls `url.ParseQuery` rather than silently using the partially decoded result of `URL.Query`. This is worth preserving across every adapter: invalid framing is not an empty or broader query.

`readcore.Resolve` (`internal/readcore/resolve.go:156`) checks resource registration, instance capability, deployment exposure, supported filter fields and the resource ceiling. `Core.Plan` (`core.go:326`) does different work: blocklist pruning, path and term admission, completed-run resolution, plan bounding and owned filter capture. `Core.Query` at 444 revalidates plan structure before touching storage. Calling `Plan` alone does not establish the exposure checks performed by `Resolve`; `core.Request` at 118 also carries no authenticated principal. The current resolver explicitly refuses authenticated exposure when it cannot enforce it. Preserve that refusal; replace the split public seam with an opaque admitted query carrying principal, dataset/policy generations and execution reservations.

`cloneFilter` at `core.go:388` copies slices, tag maps and their value slices, and pointed-to time values. The private filter and run pin prevent a caller from mutating the accepted query through shared Go references. `core.admit` at 516 rechecks plan shape and pin integrity. Rust should express this through owned immutable domain values and an unforgeable admission result, rather than translating exported mutable Go structs field-for-field.

`internal/readcore/plan.go:81` picks an access path from filter shape; `planner.go:134` checks a bound physical-key prefix, and `planner.go:175` assigns minimum prefix lengths. A bounded author/time shape requires ordered endpoints, not a maximum interval or measured selectivity (`plan.go:100`). The chosen tag is deterministic (`planner.go:54`), not demonstrated to be the most selective. **Inference:** these checks can reject obvious broad scans but cannot prove affordable scans on a dense author, popular tag or very large completed run. The ArchiveRelay's own `adapter.go:215–221` correctly calls its `Bound` a returned-row limit, not touched rows.

`internal/readcore/navigate.go:42` chooses a statement and explicit row shape. `ByIDStatement` (`byid.go:141`) uses placeholders for caller values, groups duplicate physical rows by ID, resolves their stored version, and returns deterministic `(created_at DESC, id ASC)` order. `byIDPredicate` at 186 binds the other requested author, kind and time terms too; unsupported terms are refused rather than dropped. The full-event projection at `byid.go:48` includes tags, content and signature. `internal/storage/clickhouse/read_store.go:54` executes that statement, decodes rows, and invokes the sink inline before moving to the next row.

The navigation path is explicitly different: `NavigationStatement` at `navigate.go:111` projects distinct candidate IDs and delegates predicate construction to `countPredicate`. The definition of `countPredicate` and the COUNT execution family were outside this selection. The row decoder currently represents an ID-only result as a `nostr.Event` containing only its ID (`read_store.go:134`). Rust should use separate `EventRef`, complete signed-event and analytical-row types, while retaining the useful distinction between candidate selection and authoritative event hydration. The owner has selected full signed-event archival; the old `HeavyFields` deployment axis is historical behavior, not a Rust requirement.

The inspected execution loop provides consumer backpressure, not a complete memory/compute bound. Driver buffers, query intermediates, current-winner materialization and serialized event size remain separate. The Rust planner needs shared process and per-principal reservations, bounded queue waiting, engine-side work limits, cancellation with positive completion, and response-byte accounting. Those limits must cover the aggregate work of API, ArchiveRelay and DVM, not reset when traffic switches adapters.

## 4. Cursor integrity versus actual continuation

The API cursor has useful integrity checks. `internal/api/cursor.go:163` computes a canonical, length-delimited question digest with sorted set values; the token uses an HMAC. Cursor decoding ties the request to its resource, filter question and run. These checks prevent accidental or forged continuation into a different question.

The continuation algorithm is nevertheless a prefix replay. `internal/api/server.go:621–709` expands the next query's row bound to cover already consumed rows plus the next page, replans against the currently resolved run, and then walks to a remembered boundary. It is not an engine keyset seek. Resuming deeper requires rereading an increasingly large prefix, and the resource ceiling eventually prevents further paging. Resolving “latest run” again can intentionally force a restart after a new run completes; raw events have no analogous immutable source-generation pin in this cursor.

Production construction also matters: `internal/api/from_config.go:22` does not pass `Options.CursorKey`, while `internal/api/server.go:207` generates a key when it is absent. **Observed:** this production path uses an instance-local generated key. **Inference:** restart or routing to another independently initialized instance invalidates those cursors. A durable, rotatable cursor-key policy and its configuration wiring must be tested; a constructor option by itself is not operator support.

### Static counterexample: trailing withheld candidates

This is a deduction from the frozen code, not an executed failing test. Consider a paginated, admitted ID-filter query whose storage order is `A, B, C`, where A and C are visible and B is withheld by an author blocklist at delivery. The IDs themselves remain eligible for the initial ID-list pruning. Let the first plan return two candidates, and let the registry permit a subsequent page.

1. `responseSink.Deliver` (`server.go:496–539`) writes A and records A as `lastID`.
2. B is withheld by the blocking sink. `responseSink.Blocked` at 547 increments `blocked`, without changing `lastID`.
3. The page counts one written plus one blocked as full (`server.go:759`); the cursor records `AfterID=A` but `Consumed=2` at 765–768.
4. On resume, A is reached after one skipped candidate. The boundary is found, but `skipped != Consumed` at 509, so the emitted cursor is refused.

If both candidates are withheld, the page fullness logic can attempt to mint a cursor with an empty visible boundary ID. The essential problem is the mismatch between candidate progress and last-visible-row position; counting blocked candidates alone does not repair it.

`internal/api/blockedpage_test.go` is valuable evidence of the intended behavior: it tests trailing-withheld and entirely-withheld pages as full, the counter's resume phase, and implementation of the actual `readcore.BlockObserver` interface. It never follows the returned cursor from such a page through the real response/decoder path. That is the acceptance journey required in Rust: consume actual returned continuations through mixed and fully withheld candidate ranges; deliver each eligible event exactly once; make progress without exposing withheld identifiers. Apply visibility before keyset LIMIT where possible, and use opaque non-disclosing scan progress for any necessary post-scan withholding.

## 5. Current winners, deletions, freshness and hydration

The current-event path is separate from `Store.Scan`:

```text
admitted current-resource Plan
  → CurrentServer.Serve → classify finite-coordinate live or pinned snapshot
  → build statements before execution
  → live candidate IDs + authoritative event reduction / snapshot reduction
  → coordinate reconciliation → select matching winners → bound result
  → Resolve/Plan/Query full-event IDs in batches → deliver winner bodies
```

`internal/readcore/serving.go:91` validates the current-serving seam and constructs both live and snapshot statements before executing either when both are needed. `route.go:165` chooses live only when every coordinate dimension is enumerated. `route.go:237` and its SQL counterpart at 226 encode one deterministic winner rule. `LiveStatement` at `live.go:41` navigates through author candidate IDs, rereads deciding values from the event table, and reduces by coordinate. This separation between navigation and evidence is useful, although a lagging candidate index can still omit evidence; the reconciliation path is compensating for that possibility, not eliminating source coverage questions.

`CurrentServer` explicitly refuses relay-list entry-shaped results that its `CoordVersion` seam cannot carry (`serving.go:186`). Registration of a resource is therefore not proof that every execution seam can serve its row shape. Rust capability discovery must be derived from working producers, schema and adapters, with a real journey for each advertised resource.

`ServeCurrentEvents` (`internal/readcore/current_events.go:30`) first obtains winners, then loads their full signed bodies through normal resource resolution and planning. It preserves winner order. Known-withheld IDs are skipped; a missing body that was not accounted for as withheld is reported as corruption, without substituting an older event. This is a good insistence on distinguishing a missing winner from another version. Rust should additionally represent cold-body availability and policy withdrawal explicitly rather than treating every missing hydration result as engine corruption. The current implementation accumulates winner bodies in a map before returning them (`current_events.go:121`); bound bytes as well as ID counts.

Four details must change or receive a stronger contract:

| Evidence | Meaning and proposed Rust behavior |
| --- | --- |
| `snapshot.go:78` applies SQL LIMIT before `route.go:366` applies the winner's `ids`/`until` selector in Go; `serving.go:253` then clamps selected rows. | **Static builder-seam inference:** an early limited set of nonmatching winners can hide later matching coordinates. Some normal adapter requests are refused or redirected before this seam, so this is not claimed as a reproduced public endpoint failure. Resolve winners, apply requested winner predicates and serving policy, then apply the response limit; if execution is interrupted earlier, return an explicit budget/continuation outcome. Include an early excluded coordinate followed by a later eligible coordinate. |
| `serving.go:259–263` reports configured cadence as `LagBoundNS` for a nonempty snapshot result; `Reconcile` at 331 also derives the label from route participation. | Cadence describes intended scheduling, not the age of a completed snapshot after delays or outages. Report source frontier, completion/publication times, observed age, coverage and freshness policy; never promise a measured upper bound merely because a timer was configured. |
| `deleted.go:168` requires `created_at > deletionHorizons(...)[key]`; the absent-map default is treated as timestamp zero at 271–275. | **Static inference:** a valid timestamp-zero winner with no deletion request fails this test. An absent horizon must be a distinct state from a horizon at zero. Exercise never-deleted zero, deleted-at-zero and later republished versions. Preserve the original timestamp independently of engine date/time conversion. |
| `deleted.go:276–281` constructs a map of all addressable-deletion horizons in the rendered query. | This is a global deletion subquery inside an otherwise bounded read. The statement text does not establish bounded work proportional to the requested coordinates. Rust should maintain incremental, signer-scoped deletion facts and seek only relevant targets/coordinates under the selected source/policy generation. Engine optimization of the old statement was not measured here. |

The historical deletion policy itself differs from the chosen product policy. `deleted.go:50` suppresses only current resources; `archiverelay/adapter.go:461` routes direct-ID requests, no-author filters and unsupported current-resource fields to raw events. The fallback explicitly favors returning raw historical versions. That must not be copied into the selected standard deletion-aware relay endpoint. Keep the full historical archive, but separate authorized historical inspection from ordinary relay serving, including direct-ID requests, mixed filters and windowed current-kind requests. The [protocol policy](../architecture/protocol-evolution.md) is the normative planning reference; old implementation comments are not a substitute for current protocol analysis.

## 6. Real adapter boundaries and budgets

The HTTP limiter in `internal/api/inflight.go:9` acquires a semaphore with the request context and releases it after the handler. The ArchiveRelay independently acquires its own semaphore around `serveOnePlan` (`adapter.go:284`). These bound executing requests locally. They do not bound the number of requests waiting to acquire, total rows scanned, output bytes or all adapters' combined engine work. Nor does one slot price an inexpensive ID lookup and a current-snapshot reconciliation equally well.

`internal/api/ratelimit.go:69` keys anonymous limits by normalized peer address, not arbitrary forwarded headers. Its map is bounded (`ratelimit.go:111,175`), retry hints round upward, and API key material is wrapped for redacted formatting. These are useful implementation details. The key set at 197 is explicitly a per-IP limiter bypass, not a resource authorization system; a known key must not be mistaken for a principal with access to an authenticated resource. The Rust proxy policy must declare trusted hops before interpreting forwarded client addresses, and global work limits must still apply to authenticated clients.

The filter codec has a separate completeness limitation: `internal/api/query.go:435–452` preserves tag whitespace, then comma-splits already URL-decoded values and drops empty elements. That encoding cannot round-trip a single tag value containing a comma or an empty-string tag value. Rust HTTP/OpenAPI/SDK tests need lossless array encoding for these cases, plus spaces, Unicode and literal separators. Rejecting malformed queries is not sufficient if valid domain values cannot be expressed.

The actual ArchiveRelay production registration is `KhatruRelayWithLimits` (`internal/archiverelay/adapter.go:549`), called by the listener composition. It replaces the dependency's relay-information document, advertises its selected NIPs, rejects event writes, and registers separate REQ and COUNT rejection/execution slots. Admission is repeated on the execution slot, so a dependency path cannot rely solely on the order of a separate reject hook. `queryEvents` at 703 also rejects dependency-marked internal calls. This boundary deserves reuse in Rust: every entry that can reach a read must require admission, even background or library-internal callbacks.

Within the locally implemented in-process REQ path, `admitREQFrame` at 75 charges the whole filter list, `servePlanned` at 248 executes per-filter reads and deduplicates across their union, and `ServeREQ` in `eose.go:43` sends EOSE only after success while the same subscription still owns its ID. Subscription replacement uses identity rather than simply the reused string ID. These are useful semantics and test targets.

Do not equate those local paths with demonstrated socket behavior. The registered dependency slots are per-filter, with a separate envelope accumulator, channel buffering and third-party framing. `adapter.go:611–653` records historical khatru behavior for refused COUNT, missing approximate metadata and an unregistered HLL branch. `adapter.go:720–725` records a `limit:0` path that bypasses callbacks. This walkthrough observed the registration choices and those comments; it did not reread the dependency source or re-execute those socket claims. The Rust acceptance matrix must directly test refused COUNT without a false numeric success, complete-union counting, supported estimator metadata and explicit-zero admission on the actual wire.

The old in-process `ServeREQ` leaves subscriptions open and silent after EOSE (`eose.go:41`). The registered relay rejects incoming event writes; this selection establishes no committed-archive changefeed into open subscriptions. The selected Rust design requires the committed-index history/live handoff and NIP-77 serving described in [search and reconciliation](../architecture/search-and-reconciliation.md). Preserve the cancellation/identity discipline, but do not preserve a silent post-EOSE subscription as if it fulfilled that live contract.

## 7. What the complete test files actually contribute

| Fully read file | Observable contract in its tests | Limit of the evidence |
| --- | --- | --- |
| `internal/readcore/filter_ownership_test.go` | Mutates every reference-bearing filter component after planning, then checks that the planned filter and execution still represent the admitted question. Covers nested tag-map ownership, slices and pointed-to times. | Strong input-ownership intent. It does not establish principal/policy admission, scan cost or driver cancellation. No test was executed here. |
| `internal/api/blockedpage_test.go` | Counts withheld candidates in page fullness, distinguishes skipped/blocked resume phases, and asserts the real `BlockObserver` interface. | The helper contract stops short of consuming the actual returned continuation; the counterexample above crosses those helper boundaries. |
| `internal/supervisor/resident_halt_test.go` | A service reaching its failure budget cancels its resident work without canceling the parent process. | It uses a cooperative resident; it does not demonstrate hard-stop behavior for a worker or driver ignoring cancellation. |
| `internal/app/serve_drain_test.go` | Distinguishes measured lease drain from an absent releaser, inspects actual releaser wiring through the AST, and checks stable host/PID identity. | The drain-report assertions are useful but not an end-to-end lease release. The identity test intentionally pins the old host/PID format and would need replacement for the selected nonce/fencing design. |

The lesson is not to translate every existing assertion. Preserve behavior that matters to callers and operators, then run it through the Rust composition and real transport. Existing comments describing mutation testing, integration results and historical failures are supporting history only, not a claim of Rust coverage or a new passing Go run.

## 8. Resulting Rust acceptance journeys

The following refinements were sent to the main planning agent while reading. They supplement the existing acceptance contracts; this report itself implements none of them.

1. Start the same storage role through `serve` and `run` while cockpit metrics are disabled, unreachable and unhealthy. Storage ownership remains authoritative; diagnostic identity includes a nonce; a stale process cannot release or mutate its replacement's leases.
2. Hold a driver probe beyond cancellation and issue repeated readiness requests. Outstanding probe work remains bounded, readiness ages out truthfully, and shutdown escalates without pretending a still-running writer drained.
3. Submit one logical question through HTTP, ArchiveRelay and DVM concurrently. Shared reservations bound the sum of waiting and executing work; user/API-key identity does not bypass engine or response-byte budgets. A blocked output releases storage work after cancellation, not merely its adapter slot.
4. Mutate the caller's original filter after admission and change policy while a stream is active. The query remains the admitted question, while a revoked authorization or expired policy lease stops further exposure, including when a refresh notification is lost.
5. Follow actual returned cursors through visible, trailing-withheld and wholly withheld candidate pages, including policy changes, data-generation changes, restart and cursor-key rotation. Progress is either correct and non-disclosing or explicitly invalidated; no emitted cursor points to an impossible boundary.
6. Encode and decode filters with comma-bearing tags, empty-string tags, meaningful whitespace and Unicode through the HTTP API and generated SDK. The accepted domain query is identical at execution.
7. Resolve current events with equal timestamps, a newer excluded winner, timestamp-zero values, deletion horizons and a later eligible coordinate beyond an initial excluded prefix. Filter winners before the response limit; distinguish source age, coverage, hot/cold body availability and policy withdrawal.
8. Drive the actual WebSocket boundary with multiple filters, overlapping results, refused/approximate/HLL-shaped COUNT, explicit `limit:0`, replaced subscriptions, slow consumers, engine failure after partial delivery, and history/live handoff. Only success produces the appropriate completion claim; separate in-process adapter success is not the acceptance result.

These are dependent correctness and operability requirements, not a prioritization that reduces the quality of less-visible services. They apply to the single BigBrotr product and its explicitly enabled roles. Historical body-loss profiles, cadence-based freshness and raw-history fallbacks are not retained merely because the Go implementation has tests or comments for them.

## Fully read source inventory

Every row below names an original-relative path. Its immutable copy is `.research/code-snapshots/read-runtime/<path>`. SHA-256 values and complete before/after capture evidence are also available in the [machine-readable manifest](evidence/go-read-runtime-code-sources.json).

| Source | Lines | Bytes | SHA-256 |
| --- | ---: | ---: | --- |
| `cmd/bigbrotr/main.go` | 36 | 1168 | `353ab92a5f9a44f0195f410cdc960123415a7b3c3511e580cb2c53e167cb9731` |
| `internal/cli/root.go` | 167 | 4616 | `a8cce44d84a84324b39d3defd6203dc7c51ab0295d001fba98fd052115a521eb` |
| `internal/cli/run.go` | 245 | 10853 | `5dbf3e5d187a57ae1bb5a4dc27144da0963df445bd7a7975b3a12cbb7dcb0abc` |
| `internal/cli/serve_cmd.go` | 48 | 1497 | `d1b4a8facc74cac42892a6ad208ac1432881b7f17e0623c9ad54a6b282ca0962` |
| `internal/app/run.go` | 490 | 18602 | `831f4fd75de42e04d7c371dd2afdcd35bf8649242a45d62f3b8ef6fbc6e8d38f` |
| `internal/app/services.go` | 1324 | 55892 | `9f1a4cc4bb3c7ac29531a9ae2a880b7690502a54cbf3488e88dc987283b44122` |
| `internal/app/listeners.go` | 860 | 37670 | `8d16aa10e9edf382fdcdaa4a3fe07a14fe371a6856acfdd527b14aa121857bca` |
| `internal/app/background.go` | 28 | 481 | `5894e1e9a1a72d6120541147097b4e222863c067c988f91734b19ad887126671` |
| `internal/app/pools.go` | 22 | 942 | `61486492c6d34c8b5689e02e4127cbe6022c2098d69542cf2ef9ea200aaabb76` |
| `internal/app/leases.go` | 62 | 2438 | `efbc8bff96269a19e61d34e757be030657fcecaab9bd5f97608b9a14ac7e770d` |
| `internal/supervisor/supervisor.go` | 265 | 9191 | `f0eb7e06346a327cc1f980d69d5db756ec7433984cb9992c7f1c9c5aeb059b70` |
| `internal/supervisor/runner.go` | 90 | 2793 | `ed1066600cd4de0e250db75ae7eb67e11c8bc536e8c0a7b84aab6f04179083b9` |
| `internal/supervisor/shutdown.go` | 154 | 4942 | `f2a7e9ed63c60764fd27040f991ec1b3223274fae0dbf05a440b7af79f2339c7` |
| `internal/supervisor/service.go` | 32 | 1328 | `e47240b4166d8ccd283eda930c0dc945aea10d18724de9b535fc68416e493470` |
| `internal/supervisor/health.go` | 15 | 527 | `ab005a64a272050b7e9ff87ed900c73441dfdaf5e72f9f8e07485919e93391ed` |
| `internal/supervisor/resident_halt_test.go` | 40 | 988 | `1cb2b8bde9ba618cca947ad77d37063e7e6d15587f9bc13e702d12d1725c3d4f` |
| `internal/app/serve_drain_test.go` | 191 | 7983 | `2bebdd5348431b192e1b6d5911b73518927978a826e3898d87fc36d7897c2248` |
| `internal/supervisor/cadence.go` | 121 | 3082 | `272035ff4482c3fa75e4d090aac07ba32a804fc8c09eb997240baf626d71864e` |
| `internal/readcore/core.go` | 607 | 27075 | `43350526e202957d5595947b45fbfab0c38aca175021ff1fa9bbb7920dfd4002` |
| `internal/readcore/resolve.go` | 200 | 9822 | `8945df1bf15ccd389d9be8ff55e2b5931eb335f6657c45d9a85f70d086dc5414` |
| `internal/readcore/planner.go` | 269 | 12201 | `ceea0846150ee9fba8a5d2a02a9badffd77d87846f3fd2fd569107e063113219` |
| `internal/readcore/plan.go` | 131 | 6040 | `478ed96d1d3f4bcd0c45dbebfe7ce886fe4c6e0bb70d0f33f70829291496cefe` |
| `internal/readcore/blocklist.go` | 132 | 5305 | `84acb7015817c2e05d2b970d63751e4e8758e7978d3b7bc5d4f0771a0d6e398b` |
| `internal/readcore/current_events.go` | 234 | 9371 | `d94d73a9cd3aa5e84a331843a2a2d23e161986c8deae98064a66788721632366` |
| `internal/readcore/serving.go` | 436 | 20825 | `5b9b635b19b1876084d7fe315317c0d7214ca782f58e71e14df76e8546241697` |
| `internal/readcore/live.go` | 232 | 10571 | `5dfc610a67d4ebb36669f5799d281567eb9235f0d18d084ea0ce76a9f8412264` |
| `internal/readcore/snapshot.go` | 200 | 9200 | `3fe556fb9040f64c533b25be1d0fee4828154070b3d751180f81b769273e28bb` |
| `internal/readcore/byid.go` | 337 | 14590 | `075f118dd3bbbb5dda9c6aa395d03f77fdfbf0466918a26e21646b0d62418e31` |
| `internal/readcore/deleted.go` | 317 | 15389 | `36009b1a18ea68b926adbf078340801bca0e76f43f89c9eeb58619a3de3b5dc5` |
| `internal/readcore/route.go` | 473 | 20672 | `661f9c921ad9404074ef815841c77bd37a5b0430ee5879f79979fbaaaa03c7b5` |
| `internal/api/server.go` | 888 | 40134 | `3afc802e1585f426bf7dcb75479a1710a25ac3c6bd24eff43d0bd7331e181b68` |
| `internal/api/query.go` | 523 | 21543 | `a58d8d0c70e071837b399b8574f41462631715aa2adc9c3cf55e7551b75764a0` |
| `internal/api/cursor.go` | 291 | 13216 | `564fa2798287a2a8d7a01dee656d5dbb95b570a58030053dbf50fa423907d1f8` |
| `internal/api/from_config.go` | 39 | 1341 | `d328fc1919b100af12f379d02353a9f8cff8b3a04e1f68cdd6c3e29f533581b1` |
| `internal/api/blockedpage_test.go` | 108 | 4765 | `4e0d91d9f5cf2c7f06a7d4764319ff3c7372acde3e2cb09f360c74e9fd23b8cb` |
| `internal/readcore/filter_ownership_test.go` | 249 | 12318 | `7dc8f61094455c91e800c200b66d1776350da7a4ddc9128f2a23324f6b1453f9` |
| `internal/storage/clickhouse/read_store.go` | 155 | 6258 | `3f0d2103d1a7f9be8dbddf17baab3c8cb497e3a64780e94b020e44bf9dd3986c` |
| `internal/storage/clickhouse/current_runner.go` | 132 | 5488 | `d9c726ad1f6bc958cfef80e27effbdbf7360efcc6722ce8c4d0ea1e4c34060ca` |
| `internal/archiverelay/adapter.go` | 797 | 40250 | `c528d53f5deb86b645b5b76631a51db9144b2ca3aa9bfeece55cbb9fa4081f29` |
| `internal/archiverelay/eose.go` | 85 | 4034 | `b9ff545272c4d9fa1998cd9408d5e1eed34f6ee321ecc2c2d62a6097a02a844b` |
| `internal/readcore/navigate.go` | 167 | 7518 | `c13fb8243d8417e4b478fd5bcf4ed831a112b2db54cf7a0606c8b0b562cfe086` |
| `internal/api/inflight.go` | 18 | 467 | `aa19d10fcf61b0677ee3680de232330eac9f9b2c10c68ba48c3a7702aebe432b` |
| `internal/api/ratelimit.go` | 304 | 11402 | `ffc5e77cdd28622ab91c8d7c535e5f54866e12a713031fe7b0a374541392a1a9` |
