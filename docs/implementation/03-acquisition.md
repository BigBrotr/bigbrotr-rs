# Phase 03 — Acquisition implementation plan

Status: planning only. The owner requested at least five substantive review rounds
before implementation; the coordinator records those rounds across the plan.
No checkbox is complete, no command below has run, and no product scaffold is
created by this document. Execute only after the owner starts implementation.

Goal: collect complete signed events and optional first local provenance through
bounded live, linear, NIP-77 and import paths, with truthful scoped coverage and
dependency-backed recovery. Preserve the [acquisition contract](../architecture/acquisition-services.md),
[durability contract](../architecture/data-and-durability.md) and concrete
[Go code lessons](../research/go-acquisition-code-walkthrough.md).

The application owns orchestration under `apps/bigbrotr`; storage-free values and
protocol primitives remain in the existing `bigbrotr-model` and
`bigbrotr-protocol` crates. Extend useful modules when their task needs them; do
not create a crate per layer. PG control, Rocks admission, archive and CH remain
owned by phase 02. Archive-only operation may lack CH; linear/live/import still
work, and an unavailable ordered-set reader cannot be reported as an empty set.

## Shared prerequisites and execution conventions

P01-T02 supplies event/endpoint/source-cut/receipt types; P01-T03 supplies strict
verification, filters and preserving wire primitives; P01-T04 typed configuration;
P01-T05 budgets/lifecycle; P01-T06 authorization/ownership; P01-T07 internal RPC;
P01-T08 the real fixture harness. P02-T01 supplies PG control and minimal endpoint
identity/enrollment in `migrations/postgres/0003_relay.sql`. P02-T02 supplies atomic
admission; P02-T03 dictionaries/documents; P02-T04 archive transactions; P02-T05/06
serving projection/replay; P02-T07 source pins; P02-T08 recovery. Phase 04 enriches
relay discovery and scheduling; it is not needed to enroll controlled fixtures
through the minimal registry. Phase 06 public reads and phase 08 full operations
remain explicit later integrations.

Shared schemas are governed by the coordinator's planned
`docs/specification/data-contracts.md` and `component-contracts.md`. This phase
owns `migrations/postgres/0004_acquisition.sql` and `0005_import.sql` only. Reuse
`SourceCut`, `SourceRecordId`, `Receipt`, `ExistingProof`, `EndpointId`, observer and policy
identity types from their canonical model modules; do not invent parallel scalar
cursors. SourceRecordId is `(dataset_id, shard_id, owner_epoch, sequence)`, one sequence per fact;
receipts bind contiguous complete atomic-group ranges and their digest. Here,
`AcquisitionScope` means endpoint + selector + access scope + target visibility
scope + policy epoch; `PageOutcome` means qualified evidence + validated
`EventEffectBasis` set (`Receipt | ExistingProof`) + exclusions + gaps + next work,
never a boolean complete result. Proof-only work creates no mutation request or
ingest ACK. A bounded classified prefix/page durably checkpoints deduplicated
source dependencies; complete retained source-cut manifests may compact them.
Only mutation outcomes participate in contiguous request ACK/retirement. Each declaration below is a
proposed code path, not a claim that it already exists.

Every future practical journey uses the P01-T08 runner, for example:

```sh
python3 tests/acceptance/run.py p03-t01 --binary dist/bigbrotr --compose packaging/compose/acceptance.yaml --evidence .research/implementation/p03-t01
```

Substitute the task's lowercase ID. Its case is implemented in
`tests/acceptance/cases/acquisition.py`; shared fixtures live under
`tests/fixtures/acquisition/`. Each case declares its required engines, preloaded signed input digests, policy/route
profile, expected feature state and fault synchronization points before launch.
Preloading uses the real import/admission adapter or a separately documented
minimal P02 fixture loader; the latter cannot stand in for an import journey.
The runner starts the **built CLI** and real local
sockets/required pinned engines, creates isolated namespaces, and reports
selected/executed/failed/skipped/NOT RUN. Missing required engines fails that
required job. Evidence includes binary/dependency/config digests, fixture manifest,
commands, independent expected/actual logical sets, protocol transcript, receipts,
source cuts, outcomes and resource bounds. Pure unit tests supplement these
journeys; a fake service list cannot replace them. No public relay or external
notification recipient is required for deterministic acceptance. A harness-only
negative-control run deliberately corrupts one expected output or drops an
observed fixture frame to show that the comparison fails; its expected failure
is reported separately, never counted as a passing product case.

Permanent records contain complete signed events, first local observation and
strictly earlier corrections, imported source claims, scientifically useful
executed measurements, and compact proof/gap facts. Ordinary duplicate delivery
counts, last duplicate arrival and throughput are bounded operational telemetry,
not canonical per-delivery archive writes. Provenance off creates no hidden
P-sized edge inventory. No archival delivery-time-series feature is selected in this baseline.

## P03-T01 — Define acquisition scopes, finite jobs and coverage state

Dependencies: P01-T02/T03/T04/T06; P02-T01/T02. Contract: acquisition “Shared
records”, “Scope and scheduling”, “Progress and evidence”. Inputs are registered
collection intent, minimal enrolled endpoints and authorized work leases. Outputs
are durable scope/job identities, interval evidence, gaps and resumable work.

Paths: `crates/bigbrotr-model/src/acquisition.rs`, `crates/bigbrotr-model/src/coverage.rs`,
`apps/bigbrotr/src/acquisition/sync/scope.rs`, `apps/bigbrotr/src/acquisition/sync/progress.rs`,
`apps/bigbrotr/src/storage/postgres/acquisition.rs`, `apps/bigbrotr/src/config/acquisition.rs`,
`apps/bigbrotr/src/interfaces/cli/sync.rs`, `migrations/postgres/0004_acquisition.sql`,
`contracts/acquisition/v1/coverage.json`, `apps/bigbrotr/tests/acquisition_scope.rs`,
`tests/fixtures/acquisition/scopes.json`, `docs/operations/acquisition.md`.

- [ ] **1. Freeze scope values and examples.** Define `AcquisitionScope`, finite `AcquisitionJob`, `TraversalPosition`, `PageOutcome` and `AcquisitionGap` using shared unsigned event time, `Finite(u64)/AfterMax`, access and target visibility identities, policy epoch and SourceCut. Preserve explicit collection since/until; exclude generated page limits/tail windows from selector identity.
- [ ] **2. Implement normalized filter-union identity and validation.** Sort/deduplicate equivalent field values, reject empty lists and unsupported historical search, and record ephemeral retain/drop policy. Do not infer narrowing containment from string inclusion; initially create fresh evidence for every changed semantic scope.
- [ ] **3. Add typed PG operations.** Register scope, claim finite work and atomically append gap/evidence effects with Receipt/ExistingProof-backed traversal movement under the ownership epoch. A missing/invalid required basis, expired policy grant, failed gap write or stale owner rejects the whole progress update. Persist bounded classified prefixes with deduplicated dependency closure before acknowledging only mutation requests; proof-only pages require no synthetic receipt. Preserve prior evidence when a scope retires.
- [ ] **4. Expose finite CLI jobs.** Define `bigbrotr sync run --scope S --deadline 2m`, `sync status JOB` and `sync gaps --scope S`; `--deadline` bounds the job, not all future coverage. `serve --roles synchronizer` runs scheduling independently. Results include method, attempted interval, last verification, unresolved gaps and remaining work.
- [ ] **5. Implement configuration epochs.** `sync.selectors=[{}]`, history_since=0, ephemeral=retain and provenance=local_first_witness change semantic epochs; forced empty/no-filter settings fail without an explicit valid job scope. Render source and reload class without raw credentials.
  Implement `sync.endpoints.mode/urls` from the acquisition/deployment contracts.
  Fixed scope is the intersection with operator enrollment, bans and route/access
  permission; neither existing registry rows nor later discovery expand it.
  Unverified explicit enrollment permits the worker's own due work with Validator
  disabled. List removals drain and retain progress; list additions do not invent
  past coverage. Empty fixed lists fail; enrolled mode may be healthy idle.
- [ ] **6. Exercise the discriminating fixture.** Two reordered equivalent unions share identity; a wider union, changed access/visibility scope and policy change do not. Inject a durable gap-write failure after body admission: traversal stays unchanged. This catches scalar-cursor advancement and hash-only containment shortcuts.
- [ ] **7. Run the future p03-t01 journey.** Use the CLI against real PG/Rocks, stop/restart the job and inspect PG plus admission receipts with a separate reader. Preserve before/after scope rows and proof references; a reported “caught up” without method/gaps fails. Document finite-run exits and scope-retirement recovery.

## P03-T02 — Connect multiplexed sessions to ordered durable admission

Dependencies: P03-T01; P04-T02 guarded network adapter; P01-T03/T05/T06/T07; P02-T02. Contract: preserving wire
adapter, guarded transport and synchronized admission. Input is a scoped request
and bounded inbound frames; output is classified per-record Receipt/ExistingProof event-effect bases
and an ordered `PageOutcome`. Network transport success is not durable success.
Practical step 7 additionally requires P02-T04's actual archive reader; this
acceptance dependency does not block the earlier session/admission implementation.

Paths: `apps/bigbrotr/src/acquisition/sync/session.rs`, `apps/bigbrotr/src/acquisition/sync/admission.rs`,
`apps/bigbrotr/src/acquisition/sync/page.rs`, `crates/bigbrotr-protocol/src/wire/relay.rs`,
`apps/bigbrotr/tests/acquisition_ordering.rs`, `tests/fixtures/acquisition/ordered_frames.jsonl`,
`tests/acceptance/cases/acquisition.py`, `docs/operations/acquisition.md`.

- [ ] **1. Add per-connection request generations.** Multiplex one socket per endpoint/auth scope where allowed, allocate unique request IDs, and associate every frame with a request generation and receive sequence. An old handle cannot send CLOSE/NEG-CLOSE for its replacement.
- [ ] **2. Reserve bytes/items before ownership transfer.** Apply shared 2MiB message and 256KiB admitted-event limits, the producer's 32MiB byte grant and 1024 queued-record ceiling, and bounded verifier pools. Separately cap outstanding mutation requests at 64 under the storage contract; the queue ceiling is not permission for 1024 concurrent durable requests. Include decoded objects, queued frames and join buffers in the same grant. Keep compression disabled unless the tested incremental inflated-byte/ratio path is enabled.
- [ ] **3. Route each candidate through authoritative admission.** Verify actual payload/signature or its precisely bound verification cache and full collection/request membership. CertifyExisting may prove already retained effects under the exact access/visibility context without a ProducerRequestId; NeedsAdmission creates a fresh persisted strict Admit request. Carry validated Receipt/ExistingProof bases plus policy exclusion, invalid and retryable outcomes; resolve any previously ambiguous mutation by its original request first. No unverified claimed ID becomes permanent suppression.
- [ ] **4. Join pre-EOSE work.** Keep EVENT and EOSE in one wire order; finalize the page only after all prior receive positions have classified results and required Receipt/ExistingProof bases. Checkpoint their deduplicated retained dependencies with page progress; this does not allocate a request/receipt or durable row per duplicate. A slow signature/admission result cannot be overtaken by an EOSE or later faster event.
- [ ] **5. Preserve loss and termination evidence.** Oversized/invalid/dropped frames update the affected request outcome; unattributable malformed input conservatively interrupts active coverage. Differentiate network idle, local consumer stall, request deadline, scheduled lifetime rotation and owner loss. On cancellation or owner loss, stop new requests, close the owned generation and join bounded verifier/admission work; resolve any already-issued ambiguous admission before releasing its recovery state. A timeout after possible commit is not a terminal invalid result. Close and retry bounded work without inventing completed coverage.
- [ ] **6. Exercise exact failure sequences.** Retain E from A; send E with changed invalid signature from B, then valid E. B gains only the valid witness. Also send slow event→EOSE, oversized event→EOSE, cancellation after commit before reply and stale-handle close after replacement. The cancelled run resolves the existing receipt and cannot advance unrelated coverage or turn a durable event into a failed import. Add an all-known page longer than the mutation outstanding window: one bounded proof checkpoint advances its page without new ingest request/fact/WAL writes. Lose the proof reply, then remove one restored dependency or revoke authority before checkpoint; neither missing closure nor stale authority may advance. These catch receipt-only deadlock/fallback writes as well as seen-ID authentication, early barriers, silent cap holes and map-only cancellation checks.
- [ ] **7. Run future p03-t02 over the actual CLI/RPC adapters.** A controlled relay records outgoing request/close frames, while an independent archive reader verifies full signed fields, source receipt order and absence of invalid B provenance. Repeat in-process and authenticated split-role ingest using the same logical oracle; retain peak queued bytes and cancellation outcome. Document interrupted-page recovery.

## P03-T03 — Separate capability observations, AUTH and chosen fallback

Dependencies: P03-T01/T02; P01-T06 signer/authorization contract; phase 07 real
signer adapter for explicitly enabled authenticated access. Input is a specific
endpoint/auth/filter probe; outputs are scoped capability evidence and a separate
algorithm choice. A challenge or timeout cannot establish global unreadability.

Paths: `apps/bigbrotr/src/acquisition/sync/probe.rs`, `apps/bigbrotr/src/acquisition/sync/auth.rs`,
`crates/bigbrotr-protocol/src/probe.rs`, `apps/bigbrotr/src/storage/postgres/acquisition.rs`,
`apps/bigbrotr/tests/acquisition_capabilities.rs`, `tests/fixtures/acquisition/probe_scripts.json`,
`docs/operations/authenticated-acquisition.md`, `config/examples/acquisition-auth.toml`.

- [ ] **1. Define probe result and selection separately.** Persist not-sent, transport-indeterminate, sent-timeout, applicable rejection and demonstrated support with request/method/access scope and expiry. Select auto/linear/negentropy independently; a forced unsupported mode reports blocked instead of silently changing modes.
  In auto, unavailable declared local inventory chooses linear with a local
  dependency reason, preserving the independent remote capability observation.
  Linear mode needs no reader; forced NEG refuses missing reader dependencies.
  No mode starts an Indexer/CH as a side effect or models unavailable as empty.
- [ ] **2. Implement the bounded ladder.** Use 10s public/30s overlay probe deadline and second-attempt ×2, current contract cache horizons, and earlier retry on behavior change. Scope generic NOTICE as uncorrelated; preserve NIP-67 hints and applicable machine reasons. Arithmetic and total job deadline remain bounded.
- [ ] **3. Integrate operation-scoped AUTH.** Continue permitted public reads after a challenge; sign only with an explicit binding, correlate AUTH OK by signed event ID, and reissue only the refused request after accepted AUTH. Binding changes create new evidence scope and drain the prior socket.
- [ ] **4. Treat NIP-11 as a hint.** Declared subscription/size limits may lower a request but never raise local budgets. Preserve payment/write restrictions separately; do not purchase access or treat a payment advertisement as read failure.
- [ ] **5. Persist failures honestly.** A successful fallback must not upsert `unsupported` for a timed-out probe. Storage failure prevents new capability publication; cached older evidence retains its true timestamp, auth scope and expiry. Unsupported signer/proxy capabilities fail startup when enabled.
- [ ] **6. Exercise the discriminating fixtures.** Send two probe timeouts followed by a successful linear page: algorithm=linear, observed support=inconclusive. Send AUTH then public EOSE with no signer; public read works. Send unrelated OK and operation auth-required CLOSED; only its exact authorization result changes. These catch the Go fallback/evidence conversion and challenge-only gating.
- [ ] **7. Run future p03-t03 with real sockets and PG.** Independent transcript matching verifies request IDs, absent signing in passive cases, no invoice action, and separate stored observation/selection fields after restart. The phase 07 signer integration later repeats accepted/refused AUTH using a real scoped key. Document missing-credential and forced-mode diagnostics.

## P03-T04 — Acquire live events without inventing historical coverage

Dependencies: P03-T01/T02/T03; P02-T02/T04. Inputs are seated authorized endpoints
and a durable collection selector; outputs are complete signed bodies, optional
first witnesses and live interruption intervals. This worker cannot advance
historical traversal.

Paths: `apps/bigbrotr/src/acquisition/sync/live.rs`, `apps/bigbrotr/src/acquisition/sync/session.rs`,
`apps/bigbrotr/tests/acquisition_live.rs`, `tests/fixtures/acquisition/live_backdated.jsonl`,
`tests/acceptance/cases/acquisition.py`, `docs/operations/acquisition.md`.

- [ ] **1. Encode actual live intent.** Serialize `limit:0` with the collection selector; add no `since=now`. Preserve an operator's explicit time policy. EOSE is a barrier, never a zero/unsigned event to admit.
- [ ] **2. Consume through ordered admission.** Store events delivered after EOSE with full signature and provenance scope; tolerate bounded stored delivery from a relay ignoring limit while recording that protocol behavior. A new witness is independent of whether the event body already exists.
- [ ] **3. Implement reconnect/rotation.** Use 5m idle with keepalive diagnosis and 24h planned lifetime, cancellation-safe subscription close, bounded backoff and reserved scheduler capacity. Persist the interval during which the endpoint was unobserved; reconnect never retroactively fills that interval.
- [ ] **4. Apply changing policy.** New bans/auth scope/byte limits stop or drain the relevant session. The admission authority checks current policy at the atomic commit boundary. Later cancellation/revocation does not retroactively invalidate an issued durable receipt; recovery resolves the actual committed outcome while new reads/work obey current policy. A stale startup filter or key cannot authorize later work.
- [ ] **5. Keep duplicate telemetry bounded.** Repeated E updates runtime throughput/duplicate counters only; the archive gains no per-delivery facts or last-duplicate timestamp by default. An earlier eligible local receive report can emit its defined immutable correction. Provenance off preserves scope-qualified eligibility/signature and first_received_at/earlier corrections with observer/clock quality, but no hidden endpoint/auth edge. Global first_admitted_at remains restricted; public metadata uses first_eligible_at. Use CertifyExisting only under its complete retained-effects/progress-closure contract. Signed created_at remains author-controlled and is never rewritten from receive time.
- [ ] **6. Exercise the discriminating fixture.** Publish a correctly signed old-created event after live EOSE and a future-created valid event within explicit policy, replay duplicates, disconnect, reconnect and deliver again. Bodies survive with distinct receive/signed times; traversal does not change. This catches added-since filtering, duplicate archive growth and false live completeness.
- [ ] **7. Run future p03-t04 through `serve --roles synchronizer,ingest,archive`.** The fixture independently observes REQ JSON and disconnect intervals; standalone archive inspection checks logical body/edge/correction counts before and after restart, including provenance off. Retain receipt and socket timing evidence; document that a rotating fleet has visible live gaps.

## P03-T05 — Traverse capped linear ranges with explicit boundary uncertainty

Dependencies: P03-T01/T02/T03. Input is a finite unsigned half-open interval;
output is durable `linear_observed` or relay-reported exhaustion, gap records and
next work. The relay's mutable/capped responses cannot establish absolute history.

Paths: `apps/bigbrotr/src/acquisition/sync/linear.rs`, `apps/bigbrotr/src/acquisition/sync/partition.rs`,
`crates/bigbrotr-protocol/src/filter/interval.rs`, `apps/bigbrotr/tests/acquisition_linear.rs`,
`tests/fixtures/acquisition/linear_caps.json`, `docs/operations/coverage.md`.

- [ ] **1. Convert interval bounds exactly.** Finite upper maps to inclusive upper−1; AfterMax maps to u64 maximum; empty/inverted/zero cases never underflow. Start from the configured 24h window, request a bounded 500-event page with 30s timeout (90s overlay), and retain the full selector. Window growth/shrinkage never changes the collection identity.
  Follow the finite-history planner: freeze the pass horizon, retain its next
  interval/pending children/learned width, and advance only the contiguous
  attempted prefix with its gaps. Time subdivision cannot binary-search away
  unqueried sparse history; new wall time creates a separate extension.
- [ ] **2. Track request-local progress.** Verify/order/scope-check responses, retain local walk ID/boundary state and overlap the oldest returned second. A global body dedupe hit or short page cannot end traversal; each verified record needs its Receipt/ExistingProof basis before the page contributes evidence.
- [ ] **3. Interpret EOSE hints.** Preserve finish/more/auth/unknown hints; finish is a scoped relay exhaustion assertion, auth adds restriction, contradictory finish+more records an anomaly. Missing known eligible IDs remain gaps regardless of finish.
- [ ] **4. Handle repeated boundaries finitely.** Detect repeated IDs/no older time, record the saturated-second uncertainty and visit the older interval. Subdivide only complete enumerable kind/author domains within the 128-query slice budget; persist remaining partitions instead of claiming exhaustion on budget end.
  Optional full 0–65535 kind-domain subdivision retains its cursor and counts
  every request against that same grant. At second zero, retain the gap without
  issuing a negative older bound; a single saturated leaf stays unresolved.
- [ ] **5. Handle sparse and malformed responses.** Grow empty queried intervals up to 365d then reset width after data, recording each queried range. Ordering/filter violations interrupt its scope. Limit boundary IDs, partition state, wire bytes and total elapsed work; a dense second cannot allocate an unlimited set.
- [ ] **6. Exercise the discriminating corpus.** Prepopulate E bodies from A; B clamps 500 to 100, returns a smaller all-known page and older events, then saturates one second. B obtains appropriate witnesses and reaches older intervals while the saturated second stays unresolved. Repeat at zero/maximum and with dishonest finish. This catches Go body-novelty/count shortcuts and timestamp wraparound.
  Add two populated time islands separated by empty responses and restart after
  only one child partition finishes. The query transcript must account for both
  islands and every outstanding range, independent of the reported cursor.
- [ ] **7. Run future p03-t05 against scripted and pinned real relay sockets.** The independent source manifest identifies every fixture-exposable event; compare observed set and query transcript rather than the implementation's cursor formula. Restart at a boundary and fail a proof write; no unsupported advancement occurs. Document gap interpretation, finite partition cost and why empty/EOSE is qualified evidence.

## P03-T06 — Reconcile bounded witnessed sets and retrieve every required ID

Dependencies: P03-T01/T02/T03; P02-T02/T05/T06/T07. An archive-only or lagging
installation may use a bounded conservative reader; unavailable is not empty.
Inputs are scoped local-set snapshot plus declared SourceCut/policy epoch and a
finite interval. Outputs are verified bodies/witnesses or explicit exclusions,
qualified reconciled evidence, and unresolved advertised IDs.

Paths: `apps/bigbrotr/src/acquisition/sync/negentropy.rs`, `apps/bigbrotr/src/acquisition/sync/local_set.rs`,
`apps/bigbrotr/src/acquisition/sync/need_list.rs`, `crates/bigbrotr-protocol/src/negentropy.rs`,
`apps/bigbrotr/src/storage/clickhouse/local_set.rs`, `apps/bigbrotr/tests/acquisition_negentropy.rs`,
`tests/fixtures/acquisition/reconcile_sets.json`, `docs/operations/reconciliation.md`.

- [ ] **1. Implement the ordered local-set port.** Load witnessed `(created_at,id)` for endpoint/observer/access/target-visibility and active collection policy. Select authorized archive eligibility and exact-scope witnesses before forming membership, declare actual source cut, and optionally overlay a bounded committed tail. Private-only witnesses cannot suppress first public retrieval. Imported claims/global bodies do not become local witnessed membership; provenance-off body acquisition is a separately labeled objective requiring target-scope eligibility, not merely global payload existence.
- [ ] **2. Resolve stale/unavailable inputs safely.** Use a conservative older subset with extra downloads or defer when tail/set bounds exceed the grant. Never infer coverage from unavailable storage. Keep unrelated live acquisition independent and expose the reader watermark.
- [ ] **3. Drive the pinned NIP-77 algorithm.** Use typed bounds, 64KiB frame, 64-round/1000-message and 64MiB set starting limits, counting both directions and all representations. Join result collectors on success/error/cancel; local vector, hex/decoded buffers, sorting workspace, library-owned indexes and retry maps share the envelope. Reserve for simultaneous peak representations and allocator/native overhead measured by the fixture; a serialized set size alone is not the grant.
- [ ] **4. Bound refusal subdivision.** Preserve successful reduced widths within a walk; distinguish size, age/auth policy and unsupported errors. A still-blocked single second becomes an explicit gap. Exclude NIP-77's reserved maximum timestamp only from that protocol; retrieve/store it through another supported path.
  A generic `blocked` prefix is not sufficient size evidence. Repeat an auth/age
  refusal to prove it does not create an unbounded binary subdivision tree.
- [ ] **5. Resolve the need set explicitly.** Fetch up to 200 exact IDs bounded also by encoded bytes; retry each unresolved ID up to three total attempts. Validate full selector/request membership and actual signature; generic existing-body or suppressed outcomes do not prove a new witness. Persist advertised-unserved only after attributable requests, not after an auth/transport refusal. If exact need/retry state exceeds memory/scratch quota, stop and retain a bounded interval-level resource gap with count/sample/digest where known; discard neither missing state into a false zero nor an unbounded list into permanent proof. Reconciliation must rerun before resolving that interval.
- [ ] **6. Publish qualified evidence.** Algorithm convergence precedes client NEG-CLOSE; join all needed Receipt/ExistingProof, exclusion and gap results before progress publication. Remote omission never deletes local history. Persist scope/source cut and proof summary without canonical per-delivery counters.
- [ ] **7. Exercise and run future p03-t06.** Independent manifests define A/B sets, an imported B claim, one withheld body, one unsolicited response, alternate access/visibility scopes and a dense second. Hold E privately at the same endpoint/observer/access context before a public run: E must be fetched and gain public eligibility/first witness. Repeat with provenance off, both arrival orders, restricted scopes and a pinned earlier cut. Use actual NIP-77 relay implementations plus deliberate malformed scripts; observe NEG frames and standalone retained sets. Verify three bounded ID attempts, honest refusal classification, no global-membership shortcut and no growth beyond the memory grant. Document fallback/deferred outcomes.

## P03-T07 — Schedule age-banded revisits, deep and historical work fairly

Dependencies: P03-T01–T06; P01-T05/T06; P02-T01/T07. Inputs are enrolled scopes,
due times, ownership and host/spool budgets; outputs are finite work grants,
Receipt/ExistingProof-backed continuation and bounded operational schedule state. Acquisition
progress is not an independent service that polls a universal key/value cursor.

Paths: `apps/bigbrotr/src/acquisition/sync/scheduler.rs`, `apps/bigbrotr/src/acquisition/sync/reverify.rs`,
`apps/bigbrotr/src/acquisition/sync/budgets.rs`, `apps/bigbrotr/src/runtime/budget.rs`,
`apps/bigbrotr/tests/acquisition_fairness.rs`, `tests/fixtures/acquisition/fleet.json`,
`docs/operations/acquisition-capacity.md`.

- [ ] **1. Build fair due queues.** Scope by endpoint/selector/access/visibility/policy, prefer oldest unserved work with stable tie-break and separate live/control minimum shares. Reserve permit before connection/buffer allocation; waiting jobs hold no sockets. Keep host/backoff metadata bounded and reclaim inactive entries safely.
- [ ] **2. Implement bounded preemption.** Default active scopes=64, subscriptions/endpoint=3, work quantum=2m and live rotation=10m are constrained by the host envelope. Yield historical work at dependency-checkpointed request/segment boundaries even if it has never caught up; join cancelled work before a replacement owns its seat.
- [ ] **3. Implement explicit revisit obligations.** Use sync.revisit_bands: [0,1h)/15m, [1h,6h)/1h, [6h,48h)/6h. Freeze each interval at scheduled time; delayed work cannot silently shift to now. Prefer supported NEG, retain linear evidence rules, preserve per-band due/completion age and byte-budget deferrals. Compare old/new schedules with late boundary arrivals and observed cost/delay. Safety lag=48h is a scheduling label, not a lateness guarantee.
  Resume a disconnection longer than 48h with its full historical extension still
  pending; the recent bands cannot jump that older unattempted range.
- [ ] **4. Implement deep re-verification.** Spread the 7d target over capable scopes with persisted range/gap retry positions; linear old-range sampling stays explicitly sampling. An operator's full linear retraversal is a bounded finite job. Resolve only the exact prior gap supported by new durable evidence.
  Persist each full pass's fixed horizon across restart and temporary fallback;
  completing a gap queue/sample cannot refresh its full-range completion time.
- [ ] **5. Apply pressure and policy transitions.** Distinguish throttling new starts from draining active work; hard spool/disk failure stops acknowledgements. A newly banned/re-scoped worker loses authority, while another relay/overlay failure only backs off its own work. Optional per-endpoint daily byte ceilings count control and duplicate traffic without silently hiding missed work.
- [ ] **6. Exercise the discriminating fleet.** One relay grows history forever, many small relays wait, one overlay is down and Indexer pauses. Small scopes still get bounded work and monitor/control retain permits; no infinite-backfill seat monopoly, global five-failure shutdown or four-worker budget multiplication is allowed.
- [ ] **7. Run future p03-t07 with actual CLI workers/engines.** An independent scheduler observer records request starts, per-scope service intervals, held permits and dependency-checkpointed yields under a fixed host grant. Restart/lose a lease mid-request and verify stale progress rejection. Retain maximum memory/FD/spool use and honest unmet cadence; document tuning without a 5k/100k unmeasured capacity promise.

## P03-T08 — Preflight immutable research imports and destination policy

Dependencies: P03-T01; P01-T04/T06; P02-T01/T02/T03/T07; P01 source-format
freeze before compatibility certification. Inputs are explicit local source paths,
manifest/shape, chosen import scope and current policy. Output is a verified
immutable job identity, validated source manifest/destination scope or actionable preflight refusal.

**Execution split:** implement the job/identity/policy/budget/preflight core first.
It supplies P03-T09 without requiring successful full decoding or import. T09
then supplies actual decoding/verification dry-run; P03-T10 supplies durable import
and closes T08's signed-deletion-pair and full CLI acceptance in steps 6/7.
Do not mark those deferred cases passed from a header-only preflight or mock.

Paths: `apps/bigbrotr/src/acquisition/import/preflight.rs`, `apps/bigbrotr/src/acquisition/import/job.rs`,
`apps/bigbrotr/src/interfaces/cli/import.rs`, `apps/bigbrotr/src/config/import.rs`,
`migrations/postgres/0005_import.sql`, `contracts/import/v1/manifest.json`,
`contracts/import/v1/source-shape.json`, `apps/bigbrotr/tests/acquisition_import_preflight.rs`,
`tests/fixtures/acquisition/import/`, `docs/operations/import.md`.

- [ ] **1. Define explicit commands and identity.** `bigbrotr import --from PATH`, `--dry-run`, `--format`, `import status JOB` and `import resume JOB` create or resume only requested jobs. Bind source size/digest/shards, parser/format revision, source namespace, collection scope and destination policy revision to a job; allow independent concurrent jobs.
- [ ] **2. Detect shape without guessing fidelity.** Auto must select exactly one supported format; manifestless sources require declared shape/origin. Preserve all signed fields and signature; unsigned/projected/incomplete artifacts fail canonical compatibility rather than gain a trust-disabled path.
- [ ] **3. Resolve import policy scope.** Intersect meaningful durable selector bounds, not current moving sync windows. Empty active registry requires an explicit unfiltered job scope. Preflight claim-family compatibility under import.claims before any admission; full signed relay-list/NIP-66 originals remain valid inputs even when their interpreted provenance family is excluded. Persist source/destination policy conflict results; imported records cannot update network traversal/freshness.
- [ ] **4. Separate research import from deployment restore.** Selected research formats have no administrative source-tombstone family. Preserve signed kind-5 requests and their targets regardless of file order; apply current destination admission policy. Refuse unknown mandatory control-bearing formats and direct deployment backups to P08 restore, which activates controls before writers.
- [ ] **5. Preflight real budgets and dry-run semantics.** Validate 16MiB record, 256MiB job RAM, 20GiB scratch, 4-worker defaults under shared grants and target event cap. Dry-run verifies without archive mutation and records its source digest; changed source invalidates later reuse. No daemon automatically scans arbitrary files.
- [ ] **6. Exercise the discriminating fixture.** Supply ambiguous format, newer manifest version, slim signed-field omission, a source changed at the same path, a signed deletion request after its target, and an unsupported administrative-control format. Invalid/incompatible jobs refuse as specified; the signed deletion pair imports both originals. This catches offset-only resume, inferred fidelity and confusion between archival history and administrative restoration.
- [ ] **7. Run future p03-t08 using CLI and real PG.** Independently hash input files and inspect admission/archive before/after dry-run and rejected jobs. Store preflight decisions, scratch estimate and source-manifest/policy identities; no source path cleanup is permitted. Document compatibility limits and research-versus-restore semantics.

## P03-T09 — Decode full JSONL, native and pinned protobuf artifacts

The [captured JSONL prerequisite](../reference/import-source.md) supplies bounded
source identity and line/byte positions to the existing finite signed-admission
client. Its ephemeral snapshot is not durable job staging. T08's authorized
job/current-policy adapter, installed dry-run, additional formats and T10's
obligation/checkpoint acceptance remain open; no complete step is inferred.
[Native prerequisite evidence](evidence/p03-import-source-native-20260919.json)
records the actual signed-source consumer and its scoped limits.

Dependencies: P03-T08 preflight core (not its deferred import acceptance);
P01-T03 format/verification primitives and frozen producer
schema; P02-T03/T04 archive contracts. Inputs are preflight-approved immutable
shards; output is bounded logical event/claim/scientific-evidence records with
exact source positions. Selected research formats carry no administrative control
family; deployment controls use P08 restore. This adapter boundary does not attach
foreign files to a live owner.

Paths: `apps/bigbrotr/src/acquisition/import/jsonl.rs`, `apps/bigbrotr/src/acquisition/import/native.rs`,
`apps/bigbrotr/src/acquisition/import/pensieve.rs`, `contracts/import/v1/pensieve/source.json`,
`contracts/import/v1/pensieve/wire-contract.json`, `contracts/import/v1/provenance.json`,
`apps/bigbrotr/tests/acquisition_import_formats.rs`, `tests/fixtures/acquisition/import/formats/`,
`docs/reference/import-formats.md`.

- [ ] **1. Freeze compatibility evidence.** Use the pinned a89cec2 producer, original wire contract and source hashes in [import formats](../specification/import-formats.md), plus independently signed golden artifacts. Do not vendor the upstream noncommercial schema/code; include only independently authored compatible adapter material and verify dependency permissions before distribution. Unsupported producer variants fail preflight.
- [ ] **2. Implement bounded JSONL records.** Stream lines, drain/reject oversized complete lines, reject duplicate critical JSON keys, preserve full ordered tags and signed timestamp range, and emit stable shard/line/byte positions. Malformed records may continue with counted bounded reject samples.
- [ ] **3. Implement native logical decoding.** Verify format/version/checksum/manifest membership and decode events, dictionary references, provenance claims and declared scientific evidence. Unknown versions or corruption stop the affected shard where safe framing is no longer assured; never copy its segments into a live writer directory. Import into another deployment creates new local admissions with attributed source lineage; a same-deployment restore preserving original source identities is the separate P08 recovery operation.
- [ ] **4. Implement pinned protobuf decoding.** Enforce declared field/wire types, length/recursion/output bounds and pinned producer semantics. Unknown compatible fields follow the descriptor policy; incompatible versions do not silently reinterpret signed content or provenance. Honor proto3 implicit scalar defaults: omitted content and encoded empty content both decode to the same valid empty value if signature/ID verify; do not promise unavailable presence information. Reject negative or unrepresentable int64 time without wrapping; test clean EOF separately from a truncated length prefix and refuse oversized allocation before decode.
- [ ] **5. Normalize records through shared verification.** Recompute IDs and verify every event regardless of format or source reputation; analytical projections remain separate. Missing claims become dataset origin, not a fictional relay. Source event/observer times remain attributed imported metadata.
- [ ] **6. Exercise the discriminating corpus.** The same independently signed events encoded in all approved formats yield identical canonical signed fields; include empty content, duplicate/nested tags, extreme valid time, invalid signature, malformed varint/length and truncated native frame. This catches lossy reconstruction and protobuf-schema guessing.
- [ ] **7. Run future p03-t09 via CLI dry-run, then import after P03-T10.** Compare decoded canonical event bytes and declared fidelity with a standalone independent decoder/manifest oracle; retain rejected positions and format revision. Wrong native/protobuf input cannot advance the shard checkpoint. Document safe continuation versus shard halt and compatibility certification boundaries.

## P03-T10 — Join claims, admit and resume imports with bounded spill

The [initial input-binding adapter](../reference/import-input-bindings.md) is an
in-progress step3 prerequisite. Its event-only batch binds positions and original
requests before dispatch; it does not complete job authorization, claim/proof
obligations, checkpoints or this task's large-import acceptance.

Dependencies: P03-T08/T09; P02-T02/T03/T04/T07/T08. Inputs are decoded records and
preflight source manifest and destination policy; outputs are durable complete events, imported claims,
job checkpoints and bounded rejection summary. Local network witnesses and
network coverage are never imported implicitly.

Paths: `apps/bigbrotr/src/acquisition/import/staging.rs`, `apps/bigbrotr/src/acquisition/import/join.rs`,
`apps/bigbrotr/src/acquisition/import/worker.rs`, `apps/bigbrotr/src/storage/postgres/import.rs`,
`apps/bigbrotr/tests/acquisition_import_resume.rs`, `tests/fixtures/acquisition/import/out_of_order/`,
`docs/operations/import.md`.

- [ ] **1. Build a bounded staging plan.** Partition/external-sort body and sidecar IDs with a declared scratch quota; do not keep a corpus-sized map or a fixed ten-million-edge correctness ceiling. Persist phase and partition receipts so interrupted sort/join work can resume. Stream a single extremely popular event's claim group in bounded chunks; partition skew cannot recreate an unbounded in-memory join. Account for input, intermediate sort runs, merge output and final staging coexisting within scratch quota.
- [ ] **2. Join source claims independently of input order.** Resolve existing/unknown endpoint identities without enrollment, preserve origin/observer/source timestamp and classify missing/orphan references. `import.claims=enabled-families` preserves only enabled interpreted claim families; provenance-off excludes imported event→relay claims without creating a hidden P inventory. `require-all` fails preflight when a disabled family would be excluded; `none` skips interpreted claims while complete signed bodies remain unchanged. Record every excluded family in the import report/export lineage manifest; dataset-only input never invents a witness.
- [ ] **3. Admit batches through the same authority.** Default 1000 records is additionally byte-bounded. Certify already retained event effects without a request identity where the exact import intent/lineage qualifies; persist its ExistingProof dependencies with progress. New effects/NeedsAdmission and separate sidecar evidence use strict Admit/AdmitEvidence: persist a pending `ProducerRequestId=(producer_slot_id, producer_epoch, request_sequence)` before mutation submission, alongside its immutable job/shard/logical range and digest. Resolve ambiguous acknowledgement using that exact request; logical source-record identity is separate and remains stable after a new worker incarnation; current moderation is checked at each atomic commit boundary. Policy update or expired authority pauses/refuses uncommitted affected records without restoring takedowns. A post-commit revocation may restrict subsequent delivery/read visibility, but cannot rewrite the already committed admission result.
- [ ] **4. Checkpoint after durable effects.** Five-second target checkpoints name whole logical record/shard boundaries, event Receipt/ExistingProof dependency closure, separate claim receipts and source-manifest identity. Changed inputs fail resume; crash after admission/certification before checkpoint replays harmlessly, resolving existing mutations first. Covered logical source positions with validated effects, not attempted counters or proof count, determine logical imported totals. Deduplicate/compact proof dependencies under the shared contract; proof-only batches allocate no mutation request or ACK. Retain unresolved request/cursor state for its correctness horizon independently of the display-log TTL; cancel after commit preserves that recoverable checkpoint rather than deleting staging blindly.
- [ ] **5. Handle pressure and cleanup.** Spill on RAM pressure; scratch exhaustion pauses with required/free bytes. Bound reject artifacts to 64MiB while counters continue. Clean only owned staging objects after final job/retention rules; preserve source artifacts and other jobs. Terminal rejections have warning/non-success exit class, not a silent successful zero import.
- [ ] **6. Exercise the discriminating job.** Make sidecars precede and follow bodies across shards, include imported B claims for an event already seen on A, run two jobs, stop during staging/admission/checkpoint, and change local moderation mid-run. With claims enabled, B appears only as an imported claim; with provenance off, B produces no interpreted event→relay claim, inventory or local witness and the report names that excluded family. require-all refuses before admission. Network progress stays unchanged and repeat logical counts remain stable in every case.
- [ ] **7. Run future p03-t10 through import/status/resume on real stores.** Independent input and archive readers compare event/claim sets and job receipts with a corpus exceeding the configured RAM grant. Enforce skewed one-event/many-claim input, scratch failure and restart; preserve source file hashes, spill bytes, logical counts and current policy revision. Complete the deferred P03-T08/T09 successful-import acceptance with these real stores, including both signed deletion originals and dry-run nonmutation. Phase 08 later adds full export/restore operator integration over these same contracts.

P03-T10 additionally owns `contracts/evidence/v1/issuer-authority-binding.schema.json`
and the optional A03 `IssuerAuthorityBindingV1` parser/admission gate from
[assertion bindings](../specification/assertion-bindings.md#zap-qualification-and-conflict-handling).
Use the existing native scientific-claim format and `import.claims=enabled-families`
with a registered enabled family and explicit trusted-source grant, not a new
configuration string. Validate recipient/profile/URL, interval, authority policy,
evidence closure, superseding corrections and public-use scope. Exercise actual
import→receipt→P05 classification with valid, forged, conflicting and private-only
bindings; disabling the family never stops full signed-event import.

## P03-T11 — Expose truthful acquisition operations and cost diagnostics

Dependencies: P03-T01–T10; P01-T04/T05/T06; P02-T01; phase 06 shared read
resources and phase 09 browser use these records later. Inputs are bounded worker
counters, persisted evidence/gaps and job receipts; outputs are CLI/management
views, low-cardinality metrics and attributable run outcomes.

Paths: `apps/bigbrotr/src/acquisition/diagnostics.rs`, `apps/bigbrotr/src/interfaces/cli/sync.rs`,
`apps/bigbrotr/src/interfaces/cli/import.rs`, `contracts/acquisition/v1/status.json`,
`apps/bigbrotr/tests/acquisition_status.rs`, `tests/fixtures/acquisition/status_cases.json`,
`docs/reference/acquisition-status.md`, `docs/operations/acquisition-capacity.md`.

- [ ] **1. Define status resources with scope.** Return job/endpoint/filter/access/visibility/policy identity, data/coverage freshness, method, traversal position, gaps, deferred work and authorized retained-basis status. Keep ExistingProof tokens and hidden source dependencies private; public status/evidence/export never copies internal checkpoint closure. Missing/disabled/provenance-off capability returns unavailable, never a fabricated zero.
- [ ] **2. Separate operational counters from retained science.** Runtime deliveries/duplicates/last arrival/throughput reset or expire under the operations policy. Permanent first body/edge/correction and executed measurement records retain their own identities. Do not append a canonical record for every repeated delivery merely to calculate an ingest dashboard number.
- [ ] **3. Measure actual cost.** Count wire bytes by endpoint/route/layer including control/duplicates and reconcile CPU; aggregate metrics use bounded labels. Detailed endpoint counters use bounded private operational storage, with reset/retention annotation and no claimed historical total after expiry.
- [ ] **4. Map lifecycle outcomes consistently.** Clean rotation, cancelled job, another owner, partial coverage, config refusal, infrastructure outage and remote failure keep distinct CLI/management/error codes. An unavailable recorder produces an actionable telemetry failure without changing the original relay outcome.
- [ ] **5. Apply authorization and redaction.** Use current grants for read/write status operations, protect query-bearing endpoints and auth scopes, and omit payloads/secrets from rejection samples. A pinned evidence source cut is not a permission to bypass a newer restriction.
- [ ] **6. Exercise misleading-success controls.** Cancel a leased run, disable provenance, pause CH, saturate a queue and let a peer refuse. Every status is distinguishable after restart, duplicate counters do not create new archive facts, and recorder failure does not reclassify a peer. This catches healthy-idle camouflage and terminal-error flattening.
- [ ] **7. Run future p03-t11 via real CLI/status and private management adapters.** Compare printed/JSON results with independent PG/receipt/packet observations, collect metric cardinality and redact a planted fixture credential. Document reset semantics, retry actions, durability class and source-cut meaning for later dashboard implementation.

## P03-T12 — Qualify packaged acquisition, faults and finite capacity

Dependencies: P03-T01–T11; P02-T08/T09; P01-T08/T09. Phase 04/06/07/08 add
real discovered endpoints, public query/signing and complete operations later.
This task integrates already-tested acquisition responsibilities; it does not
stand in for each task's discriminating fixture.

Paths: `tests/acceptance/cases/acquisition.py`, `tests/fixtures/acquisition/end_to_end.json`,
`packaging/compose/acceptance.yaml`, `apps/bigbrotr/tests/acquisition_faults.rs`,
`docs/operations/acquisition-recovery.md`, `docs/validation/acquisition.md`.

- [ ] **1. Assemble supported recipe cases.** Launch built roles from clean isolated volumes for full local, provenance-off, archive-only and split-role ingest. Enroll controlled fixtures through P02 minimal registry so the completed phase 04 lifecycle is not an accidental prerequisite; the concrete P04-T02 transport remains required. Declare a no-CH archive-only variant: local-set unavailable is explicit and linear/live/import work without starting a hidden Indexer.
  Add the shipped sync-only fixed-list recipe, with Monitor/Discoverer/Validator
  disabled and no CH. Pre-enroll both selected A and unrelated B; mention C from
  A's event; independently observe traffic only for A. Restart, remove/re-add A,
  ban it and retry with auto/linear/forced NEG to prove actual scope, drain and
  conditional dependency behavior, including remote ingest.
- [ ] **2. Run one coherent data journey.** Two relays supply overlapping signed events, late/backdated records, a capped interval and a withheld advertised ID while an out-of-order import runs. Independently derive expected bodies, first witnesses, imported claims, explicit exclusions and unresolved gaps from the fixture manifest.
- [ ] **3. Exercise receipt and proof failures.** Kill at admission-before/after-sync, lost reply, producer proof-checkpoint write, archive publication and worker ownership loss. Recover with P02 tooling and compare stable identities/full signatures; no cursor cites unavailable acknowledged data under the tested durability class.
- [ ] **4. Exercise sustained bounded pressure.** Pause Indexer/archive separately, reduce RAM/connections, fill scratch/spool and inject slow verification/consumer. Acquisition stops or yields explicitly at the finite grant; live/control fairness remains visible, and no unsupported dropped interval is reported covered. Measure RSS/native allocation, file descriptors, every staging generation, spool/WAL and physical CH merge/temporary headroom through independent OS/engine observers; include one huge same-second need set and a heavily skewed import while queries are active.
- [ ] **5. Exercise hostile protocol and authorization.** Include malformed EOSE, huge frames/sets, stale CLOSE, proxy refusal, redirect/rebinding, auth change and forged known-ID signature. Observe actual sockets, policy barriers and persisted evidence rather than library return booleans.
- [ ] **6. Compare recovery through independent reads.** Use standalone archive/PG readers and phase 06 read interfaces when available; verify event/edge/claim logical sets and SourceCuts before/after replay. Provenance-off has no edge state/work, ordinary repeated delivery has no canonical series, and raw signed fields remain intact.
- [ ] **7. Publish the future evidence report.** The p03-t12 runner records exact executed cases, engine/binary/config versions, receipts/cuts, sets, failure points, max resources and limitations. Run explicit sync.enabled=false, live_enabled=false, provenance-off profiles; independent sockets/engine inventories must show no disabled subscriptions, provenance indexes or arrival-series work. Verify copied CLI examples through --help and the actual parser; an unwired advertised command fails qualification. NOT RUN cannot be PASS. Document supported operating envelope from those measurements only and recovery commands; leave public-relay observations as separate dated measurements.

## Coverage and cross-phase handoff

| Contract family | Tasks |
|---|---|
| Filter identity, unsigned intervals, policy/ownership, coverage and gap proof | T01, T02, T05, T06, T11 |
| Preserving wire, full verification, multiplexing, late live events and cancellation | T02–T04 |
| Linear caps, finite partitioning, NIP-67, NEG local set/retrieval and refusals | T03, T05, T06 |
| Age-banded/deep revisits, fair seats, shared byte/disk/CPU pressure and split roles | T07, T12 |
| JSONL/native/pinned pensieve, scientific evidence/claims, spill, immutable resume and destination moderation; deployment controls remain P08 restore | T08–T10 |
| Actual CLI/engine evidence, metrics/retention/redaction and operator recovery | T11, T12 and each task's journey |

No first-launch/discovery prerequisite is hidden in this phase: P04 implements
candidate enrollment and monitoring over the P02 minimal registry. No public
serving completeness is claimed before P06. AUTH/active signing adapters remain
explicit P07 dependencies; passive and refusal fixtures run earlier. Native
export production is P08; P03 uses independently frozen native fixtures and the
same archive format, never a second importer-only body encoding. P04-T02 is an
early transport prerequisite for socket work; P03-T08–T10 import work remains
independent of that network adapter.

## Configuration implementation coverage

Use the defaults/units/reload classes in the acquisition contract through P01-T04;
add schema/render/validation and changed-setting fixtures at the owning task.
No value in this table introduces a second allocator or scalar coverage cursor.

| Family | Task owner | Discriminating changed-setting case |
|---|---|---|
| sync.enabled/endpoints.mode/endpoints.urls/selectors/history_since/ephemeral/provenance | T01/T04 | Disabled work creates no hidden queue; fixed selection cannot expand from registry/discovery; semantic change creates an epoch |
| sync.mode/capability_probe_timeout/capability_reprobe | T03 | Forced unavailable mode is blocked; inconclusive cache retries separately |
| sync.active_endpoints/subscriptions_per_endpoint/work_quantum | T07 | Lower grant drains; unfinished backfill still yields |
| sync.live_enabled/live_rotation/connection_lifetime/idle_timeout | T04/T07 | Disabled live has no subscription; clean rotation preserves historical state |
| sync.request_timeout/linear_limit/window_width/partition_budget | T05 | Small relay cap, changed timeout and saturated second retain explicit gaps |
| sync.negentropy max_set/frame/rounds/messages/need_batch/fetch_attempts | T06 | Simultaneous codec/set/list copies and need-list overflow stop boundedly |
| sync.revisit_bands, safety_lag, deep_reverify_interval | T07 | Fixed scheduled intervals, overdue evidence and conditional discovery delay; no universal lateness guarantee |
| sync.relay_byte_budget and bounded operational delivery counters | T07/T11 | Control/duplicate bytes count; no selected archival delivery series |
| import.format/scope/source_shape/claims | T08/T09/T10 | Disabled claim family reported; require-all refuses before admission |
| import.batch_records/max_record_bytes/max_event_bytes/workers/memory_bytes/scratch_bytes | T08/T10 | One-key join skew and concurrent jobs cannot exceed global grants |
| import.reject_policy/reject_artifact_bytes/checkpoint_interval | T09/T10 | Corrupt framing stops shard; log cap never erases recovery state |
| Network/frame/auth/ownership/shared producer queues | P04-T02; T02 | Queue pressure, signature cache and stale request generation remain scoped |

## Recorded planning revisions

These are document reviews and edits, not executed product checks. The coordinator
reconciles them with the requested whole-plan review rounds.

1. **Code-lesson and dependency reread.** Compared task boundaries with the full
   Go walkthrough and canonical source-cut interfaces. Added the concrete early
   P04-T02 guarded-transport dependency for socket admission; made network-free
   import sequencing explicit. Retained discriminating cases for request-local
   linear progress, actual-signature verification, pre-EOSE joins, bounded
   preemption, observed probe versus selected fallback and stale-close ownership.
2. **Field, privacy and path review.** Aligned all operator paths to canonical
   `docs/operations/`; made four-part fact identities and complete-group receipt
   cuts explicit. Refined provenance-off event times/earlier corrections without
   endpoint/auth edges, and distinguished cross-deployment native import lineage
   from same-deployment restore. The three retained time meanings remain separate. Applied the coordinator's
   explicit import.claims decision: enabled-families/require-all/none, with no
   interpreted event→relay claim inventory when provenance is off and mandatory
   exclusion disclosure.
3. **Crash/cancellation/authority review.** Separated ProducerRequestId from
   stable import source identity and bound pending payload/range/digest before
   submission. Added cancellation after durable commit but before reply, exact
   receipt resolution, joining in-flight work, and correctness-horizon retention
   independent of display-log expiry. These refine recovery obligations without
   claiming network side effects can be undone.
4. **Memory/disk/skew/cost review.** Added simultaneous codec/library/sort
   representations to NEG accounting, bounded interval-level resource gaps when
   exact need state cannot fit, and one-key claim-join skew with coexisting sort
   runs/output. Added OS/engine observation of RSS/native/FD/WAL/merge headroom
   during imports and queries. Mapped every sync/import config family to a task
   and changed-setting fixture; restored explicit initial window/request defaults.
5. **Packaged journey and capability review.** Declared required engine,
   source, route, feature and fault preconditions plus independent negative-control
   oracle failures. Removed an unnecessary live-task prerequisite for linear
   traversal; clarified archive-only operation still needs concrete transport but
   no hidden Indexer. Expanded import fixtures for enabled/excluded/require-all
   claims and packaged disabled-feature checks for sockets and physical inventory.
   Copied CLI examples must parse and run through the shipped entry point.

Coordinator follow-up after whole-phase read: removed the optional archival
arrival-series proposal entirely, reused `Receipt` and `runtime/budget.rs`, and
moved policy authority language to the atomic commit boundary. A committed
receipt remains the actual durable outcome after cancellation/revocation; current
policy governs later work/visibility and cannot retroactively falsify that fact.
