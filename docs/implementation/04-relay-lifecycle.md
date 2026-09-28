# Phase 04 — Relay lifecycle implementation plan

Status: implementation active under the owner's 2026-09-17 authorization. P04-T02
direct address policy and bounded DNS have [scoped Mac component evidence](evidence/p04-network-components-mac-20260928.json);
no complete adapter step or phase is qualified by it. Unchecked steps and example
acceptance commands below remain selected future work. Historical Go behavior
informs fixtures and design choices, not a compatibility or correctness claim.

Goal: turn attributable mentions into governed endpoint work, establish actual
protocol evidence, and retain useful measurements with reproducible method,
route, time and access context. Keep endpoint identity, operator enrollment,
observed reachability, advertised claims and publication permission distinct.
Follow the [acquisition contract](../architecture/acquisition-services.md),
[data catalog](../specification/data-contracts.md), [component contracts](../specification/component-contracts.md)
and [Go walkthrough](../research/go-acquisition-code-walkthrough.md).

## Shared prerequisites and execution conventions

P01-T02 supplies identity/evidence/policy types; P01-T03 bounded protocol primitives;
P01-T04 configuration; P01-T05 budgets and lifecycle; P01-T06 egress authority,
ownership and signer contracts; P01-T07 internal RPC; P01-T08 the actual fixture
harness. P02-T01 provides the minimal registry in `0003_relay.sql`; P02-T02/T03/T04
provide durable evidence admission, source documents and archive publication.
P02-T05/T06/T07 provide source-cut readers, replay and pins. The phase 04 lifecycle
extension owns the reserved `migrations/postgres/0006_relay_lifecycle.sql`; P02
owns the preceding minimal registry migration.

P03 may use minimal explicitly enrolled fixture endpoints before discovery and
monitoring exist. Its real network journeys require the shared guarded adapter
qualified by P04-T02, which can execute immediately after its P01 prerequisites.
Phase numbers group responsibilities; task dependencies determine execution order.
No implicit dependency on a running Monitor, public API or external directory is
required to acquire events. Fixture preloading never fabricates a successful
Validator/Monitor result for a journey whose purpose is to exercise that result. Actual signing adapters arrive through phase 07;
passive, refusal and no-signer journeys run earlier.

Use application modules under `apps/bigbrotr`; reuse pure `bigbrotr-model` and
`bigbrotr-protocol` values rather than new service crates. `SourceRecordId` uses
`(dataset_id, shard_id, owner_epoch, sequence)`, one sequence per immutable fact,
from the shared naming contract. The shared durable type is `Receipt`.
An admission group has a contiguous sequence range and digest; publication cuts
end only at complete group boundaries. New measurement and claim facts use the
same receipt, dictionary, authorization and recovery contracts as other producers.
An ordinary verified event delivered to a shared acquisition session instead uses
P03's `EventEffectBasis = Receipt | ExistingProof`; its scope includes distinct
access and target visibility identities. Do not require a new measurement,
mutation request or receipt merely to checkpoint a certified duplicate. Actual
executed Monitor/availability results and imported claims remain new evidence
with their own AdmitEvidence receipts, even when an included event is certified.

Every task has a future practical case under
`tests/acceptance/cases/relay_lifecycle.py`, invoked through P01-T08, for example:

```sh
python3 tests/acceptance/run.py p04-t01 --binary dist/bigbrotr --compose packaging/compose/acceptance.yaml --evidence .research/implementation/p04-t01
```

Substitute the task ID. The runner launches the built CLI and required pinned
engines, local DNS/TLS/HTTP/WS/proxy fixtures, and isolated namespaces. Each case
first declares fixture input digests, trust roots, exact permitted private hosts/
ports, deterministic schedule/clock controls, required features and fault points.
An instrumented local relay is not silently substituted for a required named
NIP-77 implementation or real signer; those dependencies remain explicit. Independent
observers read fixture transcripts, signed input manifests, PG rows and archive
records without invoking the worker's decision function as their expected-value
oracle. Evidence includes exact binary/config/fixture versions, commands, actual
executed/not-run cases, scope/cuts/receipts, expected and actual results, bounded
resource observations and relevant wire traces. Missing a required engine or
adapter is NOT RUN and fails that required suite, never a simulated pass. Run
separate harness negative controls with an intentionally wrong expected digest
or forbidden extra socket; the independent comparator must reject them.

Executed measurements, including actual failed attempts, are retained by default
(`monitor.raw_retention=forever`). Scheduling/deferred decisions use bounded
operational state. A durable pre-dispatch intent alone does not prove execution:
recovery may know neither whether a socket action began nor its outcome. Preserve
that uncertainty with nullable actual start and exclude it from measured outcome
denominators. Ordinary event delivery/duplicate counters do not become permanent
measurement facts. No archival event-delivery series is selected. Optional lossy measurement expiry requires a manifest covering
archive, projections, documents, replicas and recovery sources.

## P04-T01 — Extend the registry with evidence, enrollment and lifecycle state

Dependencies: P01-T02/T04/T06; P02-T01/T02/T03. Inputs are normalized endpoint or
non-dialable subject identities and attributable source references. Outputs are
idempotent discovery facts, governed enrollment and fenced due-work state.
Identity existence and a mention never authorize a connection or publication.

Paths: `crates/bigbrotr-model/src/relay.rs`, `apps/bigbrotr/src/acquisition/registry/service.rs`,
`apps/bigbrotr/src/acquisition/registry/evidence.rs`, `apps/bigbrotr/src/storage/postgres/relay.rs`,
`apps/bigbrotr/src/interfaces/cli/relay.rs`, `migrations/postgres/0006_relay_lifecycle.sql`,
`contracts/relay/v1/registry.json`, `apps/bigbrotr/tests/relay_registry.rs`,
`tests/fixtures/relay-lifecycle/identities.json`, `docs/operations/relay-registry.md`.

- [ ] **1. Reuse the single versioned normalizer.** Store normalized full URL, original spelling/source reference, normalization version and route/sensitivity; preserve scheme, path, reserved escapes, query order and explicit empty query. Non-URL NIP-66 subjects remain typed assertions. Invalid syntax returns a typed exclusion, not a guessed endpoint.
- [ ] **2. Define transitions and independent authority.** Define relay_policy.candidate_validation (full recipe: public; alternatives enrolled_only/disabled) and relay_policy.auto_enroll_validated_public (full recipe: true), applied to new work under current grants. Public candidate validation covers only eligible public routes; private/overlay endpoints need their separately explicit enrollment and route authority. Preserve enrollment origin, pending/active/dormant state, attributable protocol status, failure/backoff, evidence revision and current operator blocks. Blocking cancels eligible new work and fences stale updates; repeated discovery does not clear it. Enabled imported references may create identity without enrollment. Provenance-off excludes interpreted imported event→relay claims; it does not alter signed originals or disable non-provenance discovery/monitor evidence.
- [ ] **3. Add evidence transactions.** Key event mentions by source record/tag offset/extraction version and directory mentions by source/digest/entry/polarity. Commit effects before advancing their producer cursor. Current first/last distinct mention and bounded recent references are derived from unique evidence, not repeated polling arrivals.
- [ ] **4. Expose governed CLI commands.** Define `bigbrotr relay enroll URL`, `relay show ENDPOINT`, `relay block ENDPOINT --reason TEXT` and `relay unblock ENDPOINT`; require current action grants and expected revision where stale writes matter. Unblock permits renewed evaluation, not fabricated reachability. Status explains enrollment, policy, source evidence, due time and stale measurement ages.
- [ ] **5. Implement due claims and revision-safe revival.** Reuse common leases/fencing and stable endpoint IDs. A genuinely new evidence revision can request one acceleration, subject to Validator's minimum interval; replay and unchanged directory responses do not. Never delete a referenced identity merely because its latest attempt failed.
- [ ] **6. Exercise the fixture.** Supply query/path/scheme-distinct URLs, equivalent normalizations, pubkey subjects, negative user-list references and a banned relay under a new seed filename. Expect separate identities where specified, no jobs for non-dialable subjects and no ban override. This catches host-only grouping, reference-as-permission and replay-driven resurrection.
- [ ] **7. Run future p04-t01 using CLI and real PG/admission.** Independently compare identity/evidence cardinality and revision changes before/after replay and competing stale-owner updates. Retain command outputs, rejected revision evidence and dictionary resolution; document normalization migrations and the difference between known, enrolled, readable and publishable. In a disabled/enrolled-only policy fixture, an otherwise valid un-enrolled mention produces no outbound probe.

## P04-T02 — Implement and qualify the shared guarded network adapter

Dependencies: P01-T03/T04/T05/T06; P04-T01 is not required for isolated transport
fixtures. Inputs are parsed endpoint, route policy, current grant and allocated
connection/byte budget. Outputs are bounded connections with actual selected
address, Host/SNI, route and typed termination. P03-T02 consumes this adapter;
Seeder must not consume it. This task owns the concrete adapter; P01-T03 owns
pure parsing/verification and P01-T05/T06 supply budget/authority contracts.

Paths: `apps/bigbrotr/src/network/egress.rs`, `apps/bigbrotr/src/network/dns.rs`,
`apps/bigbrotr/src/network/tls.rs`, `apps/bigbrotr/src/network/proxy.rs`,
`apps/bigbrotr/src/network/http.rs`, `apps/bigbrotr/src/network/websocket.rs`,
`apps/bigbrotr/src/config/network.rs`, `apps/bigbrotr/tests/guarded_transport.rs`,
`tests/fixtures/relay-lifecycle/transport/`, `docs/operations/network-routes.md`.

- [ ] **1. Resolve and authorize once per connection.** Canonicalize mapped addresses, filter every DNS result under exact public/private route rules, reserve permits before resolution/dial allocation, and pin an allowed address. Preserve original Host/SNI and endpoint path/query. No second default-client resolution or unguarded fallback may bypass policy.
- [ ] **2. Implement independent route adapters.** Public and explicit private routes enforce host/endpoint, address and port scope. Tor/I2P/Lokinet use separately configured adapters; overlay hostnames never enter public DNS. Test proxy address type: clearnet pinning sends the selected IP, while an authorized remote-resolved overlay sends its permitted hostname. Missing enabled adapters fail validation. Host/route token-bucket maps, DNS answer caches, proxy pending handshakes and wait queues have explicit cardinality/byte bounds and idle eviction; a hostile stream of distinct names cannot bypass the connection cap by filling metadata.
- [ ] **3. Apply TLS and redirect policy at the actual socket.** Validate chain and hostname with configured extra roots; reject a global verification bypass. Guard every HTTP redirect, cap at three, reject HTTPS downgrade and cross-origin credential forwarding. Distinguish selected IP, original name, port and handshake policy in reusable facts.
- [ ] **4. Bound the complete transport lifecycle.** Apply aggregate 128/host route domain, endpoint 1/scope, hostname 4 connections and 2 starts/s burst 4 through the shared socket owner or reconciled Control permits; split hosts have explicit independent quotas, not an implied fleet allocator. Enforce actual process FD reserve, byte/frame/inflation limits and deadlines. Lowered budgets stop new admissions and drain existing work. Compression remains disabled unless bounded incremental decoding is actually implemented and exercised.
- [ ] **5. Preserve protocol ownership and cancellation.** Expose a connection generation and request-local frame ordering to P03/P04 consumers. Bound all writer/read queues and cancellation joins; stale handles cannot close replacement subscriptions. A stalled application consumer differs from peer silence, and unknown frame attribution interrupts affected evidence conservatively.
- [ ] **6. Exercise actual adversarial routes.** Mixed public/denied answers use only an allowed pinned IP; a second-answer rebinding, redirect to metadata IP, wrong SNI certificate, private port mismatch and proxy hostname escape all fail before unauthorized traffic. Capture proxy ATYP, DNS queries and socket destinations; mock dial-function success is insufficient. Include the bounded DNS codec's independent wire cases: unrelated SRV/additional records, negative RCODE with addresses, conflicting/cyclic CNAMEs across packets, exact eight-edge/64-address/frame limits, local-name refusal, family outcome attribution and peer-observed closure on cancellation.
- [ ] **7. Run future p04-t02 with controlled DNS, CA, proxy and WS/HTTP servers.** Independent packet/server logs prove which endpoints were contacted and which were never contacted. Repeat budget lowering and cancellation while recording peak connections/FDs/bytes. Document fixture-only loopback rules, proxy enforcement limitations and operator diagnostics without publishing credentials.

## P04-T03 — Seed deterministic bootstrap and operator files

Dependencies: P04-T01; P01-T04/T05; P02-T01. Inputs are the bundled versioned
snapshot or explicit local file and job options. Outputs are discovery evidence,
optional unverified enrollment and a durable finite job receipt. There is no DNS,
probe, automatic remote-directory fetch or long-running Seeder loop.

Paths: `apps/bigbrotr/src/acquisition/seeder/reader.rs`, `apps/bigbrotr/src/acquisition/seeder/job.rs`,
`apps/bigbrotr/src/interfaces/cli/seed.rs`, `apps/bigbrotr/src/runtime/bootstrap.rs`,
`resources/relay-seeds.txt`, `resources/relay-seeds.manifest.json`,
`apps/bigbrotr/tests/seeder.rs`, `tests/fixtures/relay-lifecycle/seeds/`,
`docs/operations/bootstrap.md`, `config/examples/bootstrap.toml`.

- [ ] **1. Freeze the shipped input.** Record version, upstream provenance/license, digest and reproducible update procedure for the bundled list. Expose it in build metadata. An explicit unreadable/missing file fails before any writes; preflight the input descriptor and immutable job identity.
- [ ] **2. Parse bounded lines.** Apply 8192-byte maximum, drain oversized lines safely, preserve line/source and count blank/comment/malformed records. Only operator-supplied scheme-less input may assume configured `wss`; network-derived input never uses this convenience.
- [ ] **3. Commit idempotent candidate batches.** Default 500 candidates is additionally byte-bounded. Store canonical identity and source line evidence through registry operations; retries resolve the same batch receipt. Zero admissible candidates produces an explicit non-success/no-admissible-input result instead of an apparently successful bootstrap.
- [ ] **4. Separate enrollment from verification.** `bigbrotr seed [--file PATH] [--enroll]` defaults to enrollment false; enrollment sets unverified and remains subject to route/bans. Out-of-policy IP literals may remain excluded evidence; unresolvable hostnames remain candidates. No seed path writes reachable=true.
- [ ] **5. Wire fresh deployment behavior.** The full local recipe seeds an empty registry once unless `bootstrap.seed=false`; the durable bootstrap receipt prevents restart repetition. Render selected source and per-result counts. A database failure leaves a resumable unchanged batch and no whole-job completion receipt.
- [ ] **6. Exercise the mixed-source fixture.** Use comments, equivalent duplicates, invalid URLs, an oversized line, blocked endpoint and valid unknown hostname; rerun under another filename and kill between batch acknowledgement and progress. Expect no duplicate source facts for the same declared input identity, no unblock and no outbound packets. Also test empty explicit and bundled inputs as distinct failures.
- [ ] **7. Run future p04-t03 from a clean packaged data directory.** Execute config init/validate, seed, inspect registry, restart and repeat with `--enroll`. Independent file digest, PG batch receipts and a packet observer prove exactly what changed. Document startup failure recovery and the visible distinction between seeding, enrollment and successful probing.

## P04-T04 — Discover relay references from immutable archive inputs

Dependencies: P04-T01; P01-T03/T05; P02-T04/T06/T07; P03 import or fixture
admission can supply early events. Inputs are accepted complete signed events in
durable ingestion order. Outputs are attributable positive/negative/neutral
references, resumable tag offsets and discovery coverage gaps.

Paths: `apps/bigbrotr/src/acquisition/discoverer/archive.rs`,
`apps/bigbrotr/src/acquisition/discoverer/extract.rs`,
`apps/bigbrotr/src/acquisition/discoverer/progress.rs`,
`crates/bigbrotr-protocol/src/discovery.rs`, `apps/bigbrotr/tests/discovery_archive.rs`,
`tests/fixtures/relay-lifecycle/discovery_events.jsonl`, `docs/reference/discovery-sources.md`.

- [ ] **1. Define the ordered source contract.** Use committed stream/source-record identities with a pinned compatible cut; late historical events inserted after newer signed timestamps remain visible. Pages default to 500 inputs. Never substitute `created_at`, CH row arrival or live subscriptions for the durable checkpoint.
- [ ] **2. Implement the explicit extraction table.** Preserve NIP-65 kind-10002 `r` tags/read/write markers, NIP-66 kind-30166 subjects and kind-10166 publisher declarations, NIP-51 relay-list references, direct websocket `r`/`relay` tags and eligible `e`/`p`/`a` relay hints. Use full ordered tag arrays with source event/kind/author/tag position/parser version; list exact pinned kinds and tag columns in the source-reference document.
- [ ] **3. Preserve interpretation limits.** A blocked-relay list is that author's negative preference, not a local ban. Pubkey-only NIP-66 subjects remain non-dialable; malformed multiple `d` tags retain the first subject and a diagnostic. Encrypted list contents remain opaque. Unsupported sources can retain raw signed references without arbitrary expression evaluation or false capability observations.
- [ ] **4. Bound work and effects.** Use four source-partition workers, 1000 tags/input quantum, 30s slice and 500-reference byte-bounded registry batches. Persist sub-record offsets for a huge event, including offsets producing exclusions. Commit effects and checkpoint atomically or idempotently before movement; cancellation cannot skip the unexamined suffix.
- [ ] **5. Protect unread source history.** Hold a source pin or durable discovery outbox until effects commit. If a required source was explicitly lost/pruned, persist a discovery gap and require an explicit replay scope; `discoverer.events.replay_from` starts a separate finite job. Replayed identical references do not increment fresh-evidence revision.
- [ ] **6. Exercise the discriminating corpus.** Ingest a backdated relay-list event after a recent one, a huge but admitted-size-valid event whose only valid reference is past the 1000-tag quantum, negative markers, multiple subjects and repeated import/replay. Expect every eligible offset once, retained polarity, no pubkey dial and no lost late event. This catches timestamp cursors, first-value tag indexes and whole-event checkpoint shortcuts.
- [ ] **7. Run future p04-t04 through real import/admission→archive→Discoverer.** Stop between reference commit and cursor movement and at a tag quantum; an independent extractor over the signed fixture manifest compares exact source identities and endpoint sets. Retain pins/cuts/checkpoints, then test explicit source-loss handling. Document parser-version replay and why mentions are neither local witnesses nor server capabilities.

## P04-T05 — Poll optional external directories independently and safely

Dependencies: P04-T01/T02; P01-T04/T05; P02-T03. Inputs are explicitly enabled
stable source definitions with HTTPS URL and restricted selector. Outputs are
attributable digest-keyed discovery evidence, poll metadata and independent due
state. Defaults require no third-party service for startup.

Paths: `apps/bigbrotr/src/acquisition/discoverer/directory.rs`,
`apps/bigbrotr/src/acquisition/discoverer/selector.rs`,
`apps/bigbrotr/src/config/discoverer.rs`, `apps/bigbrotr/tests/discovery_directory.rs`,
`tests/fixtures/relay-lifecycle/directories/`, `config/examples/discovery-directories.toml`,
`docs/operations/discovery-directories.md`.

- [ ] **1. Validate source contracts.** Default enabled=false and sources=[]; enabled with no sources fails. Each source has stable ID, URL, secret references and a compiled field/index/array-wildcard selector only. Reject expressions, recursion and unbounded expansion before polling; pin the documented nostr.watch preset schema independently of live availability.
- [ ] **2. Fetch within shared egress bounds.** Apply HTTPS, 15s whole-request timeout, three guarded redirects, 5MiB decompressed response bound and 1s minimum source-host spacing. Reserve decode/selector output memory and bound headers, nesting and emitted candidates, not only response bytes.
- [ ] **3. Preserve conditional semantics.** Use ETag/Last-Modified where supported, store source/digest/document/parser identity and distinguish checked-at from body-fetched-at. A 304 or unchanged digest reuses the same evidence and does not grant new capability freshness or trigger repeated revival.
- [ ] **4. Schedule independent sources.** Default success interval 24h; failure starts 15m with factor-two backoff capped at 24h and 20% jitter. Honor bounded Retry-After within operator/job limits. One failing/rate-limited source does not suppress another source or archive discovery.
- [ ] **5. Persist effect and poll state safely.** Commit idempotent extracted references before successful checkpoint advancement. Retain last successful facts on failure with honest age; removing a source retires scheduling without rewriting prior evidence. Credential rotation changes connection state but does not leak secrets into document manifests.
- [ ] **6. Exercise hostile and unchanged inputs.** Return 200→304→same bytes under new ETag, a 429, redirect to a denied address, compressed oversized JSON and unsupported selector syntax. Expect one evidence revision for unchanged assertions, no forbidden dial and bounded failure isolated from a simultaneously progressing archive leg.
- [ ] **7. Run future p04-t05 against the actual HTTPS fixture and PG.** Independently compare request headers/timing, reference digests, revival counts and source-specific checkpoints after restart. Capture source credential redaction and peak decode output. Document enabling a preset, schema changes, freshness and source removal without implying directory authority.

## P04-T06 — Validate candidates with attributable protocol evidence

Dependencies: P04-T01/T02; P01-T05/T06; P02-T02/T03. P04-T03 supplies
the seeded operator journey but is not a runtime service prerequisite. Inputs are due governed
candidates and current grants. Outputs are typed transport/protocol/access facts,
transactional enrollment scheduling and separate best-effort NIP-11 evidence.
An HTTP upgrade alone cannot establish a usable Nostr relay.

Paths: `apps/bigbrotr/src/acquisition/validator/scheduler.rs`,
`apps/bigbrotr/src/acquisition/validator/probe.rs`,
`apps/bigbrotr/src/acquisition/validator/result.rs`,
`apps/bigbrotr/src/interfaces/cli/run.rs`, `apps/bigbrotr/src/config/validator.rs`,
`apps/bigbrotr/tests/validator.rs`, `tests/fixtures/relay-lifecycle/validator_scripts.json`,
`docs/operations/validator.md`.

- [ ] **1. Implement fair due claims.** Claim at most 100 candidates, execute at most 20 probes within global grants, order by due/least-recent/stable ID and alternate new/retry work. Recheck bans/lease immediately before each action. Batch claim count never preallocates that many sockets.
- [ ] **2. Perform the bounded handshake.** Use 10s clearnet/30s overlay transport+Nostr deadline and a high-entropy nonexistent exact-ID REQ. Correlate EOSE, independently verified EVENT, applicable CLOSED or AUTH with the request/session. A bare 101, unrelated frames or invalid signed payload cannot promote public usability.
- [ ] **3. Classify protocol and access separately.** An attributable AUTH exchange proves protocol behavior; a challenge alone does not prove all reads denied. Promote active only with usable public reads or explicit enrolled authenticated access. Keep auth-gated known relays inspectable and do not auto-pay advertised fees.
- [ ] **4. Fetch independent initial catalog evidence.** Use HTTP-equivalent scheme with identical path/query, separate 10s NIP-11 timeout and 1MiB cap. Preserve original bounded document/digest/parse result through the shared evidence path. Optional catalog failure does not consume the original handshake budget or invalidate a demonstrated read.
- [ ] **5. Commit outcome and next schedule with authority.** Use 1h initial retry, factor 2, maximum 7d, 0.2 jitter, dormancy after ten consecutive relay failures and one fresh-evidence revival per minimum hour. Record the executed evidence receipt before promoting readiness/advancing its PG schedule; a lost PG write reuses that evidence instead of relabeling a new probe as the old result. Infrastructure/storage failure does not increment relay-failure counts. Success resets backoff; bans, enrollment and referenced identities survive dormancy.
- [ ] **6. Exercise discriminating peers and failures.** Compare bare 101, prompt empty EOSE, AUTH followed by public EOSE, scoped refusal, invalid EVENT and a successful handshake followed by PG failure. Expect transport-only/active/auth-gated/invalid/infrastructure outcomes respectively; repeated old discovery cannot force immediate retry. This catches handshake-only admission and recorder-as-peer-failure behavior.
- [ ] **7. Run future p04-t06 using `bigbrotr run validator --once`.** The runner observes actual probe transcripts, independent PG transitions and durable evidence receipts across restart and a stale-owner conflict. Assert selected/attempted/admitted/excluded/infrastructure-failed counts and terminal status. Document DNS/TLS/no-protocol/missing-credentials/database diagnostics and sparse retry inspection.

## P04-T07 — Schedule and durably record actual monitor attempts

Dependencies: P04-T01/T02; P01-T05/T06/T07; P02-T02/T03/T04. Validator P04-T06
is not a prerequisite for explicitly operator-enrolled endpoints. Inputs are due
endpoint/method profiles and permits. Outputs are compact executed measurement
facts, compound-check children, independently aged latest fields and bounded
operational deferral state. Monitor does not change admission or sign announcements.

Paths: `apps/bigbrotr/src/acquisition/monitor/scheduler.rs`,
`apps/bigbrotr/src/acquisition/monitor/attempt.rs`, `apps/bigbrotr/src/acquisition/monitor/liveness.rs`,
`apps/bigbrotr/src/storage/postgres/monitor.rs`, `apps/bigbrotr/src/config/monitor.rs`,
`contracts/archive/v1/measurement.json`, `apps/bigbrotr/tests/monitor_attempts.rs`,
`tests/fixtures/relay-lifecycle/monitor_crashes.json`, `docs/operations/monitor.md`.

- [ ] **1. Separate schedule and measurement identity.** Maintain due/claim/deferral records in bounded operations state; acquire budget before external work. A stable attempt ID and method/profile bind actual endpoint/route/observer/access context. Persist a bounded dispatch intent before action to make ambiguity recoverable, without treating that intent as proof the action began.
- [ ] **2. Implement honest crash recovery.** Record actual start/end and monotonic duration when observed, plus terminal evidence receipt. After a crash with only intent, retain execution-unknown and nullable actual start; with durably recoverable proof of start but missing completion, retain completion-unknown. Neither becomes a fabricated timeout or success/failure vote. A local start timestamp observed only in lost RAM is not recovered proof of start. Retry is a new attempt; retrying an evidence write resolves the original receipt. Preserve unresolved dispatch intent through the correctness horizon until terminal/unknown recovery is durably linked; operational display TTL cannot delete that obligation.
- [ ] **3. Schedule liveness and catalog independently.** Defaults are 5m and 6h, claim 100, concurrency 32, host concurrency 1, host start budget 32/min and 0.1 jitter. Use 15s check timeout/45s overlay; retries=1 total attempt. Failure due decay factor 2 caps at 7d; achieved coverage/backlog is reported rather than promised cadence. A retry_initial of 1s is inactive until attempts exceed one.
  Apply `monitor.endpoints.mode/urls` independently of Synchronizer. Fixed-list
  explicit enrollment permits unverified work with discovery/validation disabled;
  each worker owns its attributable outcomes/backoff. Preserve dormant sparse
  retry without requiring Validator to make the endpoint active again. Received
  probe events do not start live/backfill synchronization.
- [ ] **4. Measure the real read phases.** Record websocket open, request-to-EOSE and request-to-first-valid-event durations separately; empty prompt EOSE is readable and has no event RTT. Preserve actual request profile, address family, phase failure and source clock quality. Reuse ordered frame ownership and verification barriers; never admit a fake zero event for EOSE.
- [ ] **5. Persist useful facts through common admission.** Validate store_checks as a subset of computed checks; explicitly disabled or discarded children remain unmeasured/unavailable in views. Group typed child checks under the executed compound attempt; use `AdmitEvidence`, stable fact identities and complete batch receipt boundaries. Include failed executed work and preserve unexecuted child status as context, not an independent attempt. Default permanent retention does not entail copying unchanged documents or storing queue deferrals in the archive.
- [ ] **6. Exercise crash boundaries and partial catalogs.** Stop before intent, after intent before socket, after observed start, after terminal admit before PG acknowledgement and during a liveness-only pass over older TLS data. For the observed-start stop, separately cover start evidence synced versus RAM-only: expect completion-unknown versus execution-unknown. Other stops yield no attempt/fabrication, execution-unknown, idempotent terminal facts and unchanged TLS age. A stale owner returning after a new claim cannot promote status or issue another probe. This catches intent-as-execution and latest-row freshness laundering.
- [ ] **7. Run future p04-t07 with real CLI, relay and stores.** Use `bigbrotr run monitor --once` and scheduled `serve --roles monitor,ingest,archive`; an independent socket observer and archive reader compare measured/unknown attempt sets and denominators. Retain source IDs/ranges/receipts and backlog/resource evidence. Document crash uncertainty, permanent default costs and distinction between configured and achieved cadence.

## P04-T08 — Capture scoped DNS, TLS, HTTP and NIP-11 catalog evidence

Dependencies: P04-T02/T07; P02-T03/T04. Inputs are an executed catalog profile and
actual endpoint/address/route context. Outputs are typed per-check facts and
bounded content-addressed documents. Shared-host facts require matching method
scope; endpoint HTTP/WS/NIP-11 always preserves the full URL.

Paths: `apps/bigbrotr/src/acquisition/monitor/dns.rs`, `apps/bigbrotr/src/acquisition/monitor/tls.rs`,
`apps/bigbrotr/src/acquisition/monitor/http.rs`, `apps/bigbrotr/src/acquisition/monitor/nip11.rs`,
`crates/bigbrotr-protocol/src/nip11.rs`, `apps/bigbrotr/tests/monitor_catalog.rs`,
`tests/fixtures/relay-lifecycle/catalog/`, `docs/reference/relay-measurements.md`.

- [ ] **1. Specify typed DNS measurements.** Preserve query name/type, route/resolver, bounded A/AAAA/CNAME/PTR/NS answer sets and TTLs, selected dial address, result and duration. Retain separate IPv4/IPv6 outcomes. Bound recursive chains, answer counts, certificate/DNS parser scratch and encoded sizes under the catalog grant; do not leak overlay names into public forward/reverse DNS.
- [ ] **2. Specify TLS facts from an actual handshake.** Capture validation result, bounded certificate chain references, issuer/subject/SAN, validity, fingerprints, negotiated version/cipher and phase duration. Reuse only when hostname+port+selected IP+handshake policy match; a sibling path may share that exact TLS context but never inherit HTTP readability by association.
- [ ] **3. Measure endpoint HTTP independently.** Preserve original scheme-equivalent endpoint/path/query, status/media type, capped permitted headers, redirect chain metadata and request duration. Persist only allowlisted nonsensitive header metadata. Credential-bearing responses must not become raw archive documents; an explicitly redacted derivative has its own digest, transformation/version and incomplete-original status. Never label altered bytes as the complete original document. Failed TLS validation remains an observed failure; no silent permissive second fetch obtains a more convenient document.
- [ ] **4. Parse NIP-11 as declared claims.** Retain complete capped source bytes/digest and parser/method versions; parse useful software/contact/name/description/pubkey, supported NIPs, limitations, payment/auth/retention declarations with presence/unknown distinctions. Invalid fields remain parse diagnostics and raw evidence; declared support, limits and fees are not observed capabilities or global read refusal.
- [ ] **5. Preserve document and freshness semantics.** Deduplicate unchanged content logically in its visibility namespace while keeping meaningful executed fetch results/context. This is an identity/reference guarantee, not an assumed physical-byte saving: compression can already exploit repeated bodies. A 304 records a conditional validation and original body-fetch age. A partial pass updates only measured fields; contradictory endpoints or observers remain separately inspectable.
- [ ] **6. Exercise scoped reuse and partial failure.** One hostname serves two paths with different NIP-11/public-read behavior and two SNI/IP backends; A succeeds while AAAA fails, an empty field differs from absent, and a malformed bounded document follows a good one. Expected projections preserve separate field times, route scopes and parse states; this catches host-wide capability copying and whole-row replacement.
- [ ] **7. Run future p04-t08 with controlled DNS/CA/HTTP/WS sockets.** Independent fixture records establish expected query types, Host/SNI and document hashes; archive inspection verifies typed values and bounded document references after replay. Document units, source age, missing/failed meanings and private endpoint redaction, with no physical-location or compliance claim inferred from metadata.

## P04-T09 — Resolve and refresh independently versioned Geo/ASN datasets

Dependencies: P04-T02/T07/T08; P01-T04/T05; P02-T03. Inputs are configured ASN/City
datasets and actual non-overlay dial addresses. Outputs are scoped approximate
lookup measurements with edition/version/date, plus safe optional dataset refresh
state. Missing datasets do not make a relay unreachable.

Paths: `apps/bigbrotr/src/acquisition/monitor/geo.rs`,
`apps/bigbrotr/src/acquisition/monitor/geo_refresh.rs`, `apps/bigbrotr/src/config/geo.rs`,
`apps/bigbrotr/tests/monitor_geo.rs`, `tests/fixtures/relay-lifecycle/geo/`,
`contracts/relay/v1/geo-source.json`, `docs/operations/geolocation.md`.

- [ ] **1. Freeze supported data contracts.** Use independently pinned and legally distributable MMDB fixture data and documented ASN/City products; validate product metadata and decoder compatibility. ASN and City are independently optional. Explicit unreadable paths fail startup; absent paths produce unavailable_dependency for that lookup only.
- [ ] **2. Implement bounded lookups and context.** Resolve both IPv4 and IPv6, preserve address/family/database edition/build date and typed ASN/location/accuracy fields. Default stale threshold is 30d and geohash precision 5 (valid 1–12). Approximate database location is not physical server certainty; overlay routes do not gain the proxy's location.
- [ ] **3. Validate optional refresh configuration.** Default refresh=false; enabling requires supported format/product and explicit source URLs/credential references. Apply independent source identities and 24h scheduled checks with shared HTTPS guards. Do not fetch a vendor database or accept a license on the operator's behalf without its configured authority.
- [ ] **4. Install new datasets atomically.** Cap both compressed download and extracted bytes at 100MiB; bound archive entry counts/names and reject traversal/symlink escapes. Download to an owned temporary object, verify declared digest/product/decoder, sync and atomically activate a version. Existing readers pin the old version until done. Preflight disk for compressed input, extracted candidate and pinned old versions at the same time; pause safely if the combined grant is unavailable. Do not count only the final MMDB file.
- [ ] **5. Preserve last-good and loss information.** Interrupted/corrupt/wrong-product updates do not replace usable files. Expose failure, active version, source age and next due time without credentials. Retain method/source descriptors needed to interpret historical lookup facts even if obsolete local binaries are reclaimed.
- [ ] **6. Exercise real dataset failures.** Run independent golden A/AAAA lookups, absent ASN with available City, stale source, truncated download, corrupt MMDB, wrong product and interruption before activation. Expect correct per-family availability and unchanged last-good hash; a new reader sees only a complete version. This catches filename-only validation and partial-file replacement.
- [ ] **7. Run future p04-t09 via actual CLI and controlled refresh HTTP server.** An independent MMDB reader and filesystem/hash observer verify values and atomic version switch while monitoring continues. Retain source/fixture license metadata, download/extraction bounds and lookup references. Document source setup, optional disablement and separately configured coarse geolocation publication permission.

## P04-T10 — Probe protocol capabilities and explicitly authorized writes

Dependencies: P04-T02/T07/T08; P03-T02/T03/T06 reusable wire/probe/NEG helpers;
P01-T06 signer purpose contract. Real AUTH/write signing requires phase 07 adapter
tasks; enabling an unimplemented signer fails validation. Inputs are explicit
bounded probe profiles, not an instruction to certify every NIP implementation.
Outputs are scoped observed results, inconclusive/refusal reasons and optional
signed probe references with actual destination outcomes.

Paths: `apps/bigbrotr/src/acquisition/monitor/capabilities.rs`,
`apps/bigbrotr/src/acquisition/monitor/active_write.rs`,
`crates/bigbrotr-protocol/src/probe.rs`, `apps/bigbrotr/tests/monitor_capabilities.rs`,
`tests/fixtures/relay-lifecycle/capabilities/`, `config/examples/active-monitor.toml`,
`docs/operations/active-monitoring.md`, `docs/reference/relay-measurements.md`.

- [ ] **1. Share request attribution without duplicating algorithms.** Use unique session/request generations, preserving EOSE hints, bounded decoder and actual verification from P03. Record each probe's exact input/method version/access context; support applies to that demonstrated behavior only. A timed-out probe remains inconclusive even if the next algorithm works.
- [ ] **2. Implement passive feature profiles.** NIP-42 challenge and actual refusal are distinct; NIP-45 demonstrates a bounded COUNT response, not count accuracy. NIP-50 preserves observed matching/refusal/inconclusive with a controlled sample; an empty unknown corpus cannot establish universal search correctness. NIP-77 uses the bounded reconciliation helper and tested request profile.
- [ ] **3. Combine observations with declarations transparently.** Store NIP-11 advertised NIPs/limits/payment rules separately from observed responses and expiry. A paid declaration plus successful public EOSE stays publicly readable; an actual payment refusal is scoped to its request. No invoice payment, automatic key creation or unrelated publication occurs.
- [ ] **4. Define optional active write intent.** Default enabled=false; explicit endpoint scope, dedicated signer reference, purpose, frequency 24h and current grants are mandatory. Implement versioned marker profile monitor-write-v1: kind 1, content "BigBrotr monitor probe v1.", one ["bigbrotr-probe", attempt_uuid] tag, the dedicated pubkey and a frozen created_at. Optional bounded NIP-13 work adds only its defined nonce tag. Keep the complete event below 1KiB and declare that this ordinary public event may persist; profile changes require purpose-policy review and new method version. Persist immutable signed bytes/event ID before send so retries reuse exactly one intent; recheck current purpose/endpoint authority immediately before send and cancel pending work after lease/key/policy revocation. An already-sent packet cannot be recalled, so a lost ACK remains unknown and any authorized retry uses the same signed bytes; the profile is not a permanent generic signing endpoint.
- [ ] **5. Bound active execution and interpret results.** Apply maximum PoW 20 bits and 1s CPU/wall pool budget; unmet relay requirements produce policy-unavailable, not proof of unwritability. Correlate OK by exact event ID, then retrieve that exact ID and verify complete signed fields before write+read success. Acceptance, retrieval, timeout and optional cleanup attempt stay distinct; cleanup never promises deletion.
- [ ] **6. Exercise the discriminating peers.** Return unrelated OK, correct OK without retrieval, forged known-ID event, AUTH while permitting public reads, inaccurate COUNT and over-budget PoW demand. Expect scoped/inconclusive results and no false write+read success. Revoke the active-write grant after durable intent and after actual send: the former emits no packet, the latter reports accepted/refused/unknown only from attributable evidence and never claims recall. Passive fixtures capture zero signed outbound EVENT; write fixtures use only the configured fixture key and endpoint after real phase 07 wiring.
- [ ] **7. Run future p04-t10 over actual sockets and durable evidence.** Independent event verification and relay transcripts compare probe IDs, exact retries, grants and measurements. Retain source cuts, signing intent references and actual resource usage without secret bytes. Document opt-in effects, unavailable signer behavior and why advertisements or acceptance alone cannot prove retention or standards compliance.

## P04-T11 — Project retained measurements and hand evidence to NIP-66 publication

Dependencies: P04-T07–T10; P02-T03/T04/T05/T06/T07; phase 06 read-resource and
phase 07 NIP-66 publisher integration later. Inputs are receipt-backed measurement,
claim/document facts and a compatible SourceCut. Outputs are independently aged
current fields, defined aggregates, retained source links and unsigned publication
input. Publisher execution must not trigger fresh monitor probes.

Paths: `apps/bigbrotr/src/acquisition/monitor/projection.rs`,
`apps/bigbrotr/src/acquisition/monitor/aggregate.rs`,
`apps/bigbrotr/src/acquisition/monitor/publication_view.rs`,
`contracts/relay/v1/monitor-view.json`, `apps/bigbrotr/tests/monitor_projection.rs`,
`tests/fixtures/relay-lifecycle/measurement_history.json`,
`docs/reference/monitor-history.md`, `docs/operations/measurement-retention.md`.

- [ ] **1. Define idempotent field projections.** Deduplicate immutable fact IDs at the selected cut, choose values by actual method/field time and scope, and retain source record links. A later failure is visible alongside the age of the last successful value; it cannot silently refresh that value. Distinguish zero, absent, unsupported and execution-unknown.
- [ ] **2. Define aggregate math before SQL.** Version UTC bucket boundaries, eligible attempted/completed denominators, method/scope grouping and latency units/reducers. Unknown execution, unexecuted children and deferred work are excluded from measured success/failure denominators but counted separately for observability. Publish count/sum/extrema or a named bounded approximate quantile sketch with its limits, never unlabeled exact percentiles.
- [ ] **3. Apply explicit retention contracts.** Default monitor.raw_retention=forever and monitor.aggregate_retention=none means no automatic expiry. Optional lossy raw expiry produces a versioned source/resolution manifest and coordinates archive, replay, documents, backups and projections; it cannot resurrect expired bytes during replay. Referenced deduplicated documents remain available for retained evidence; failed temporary documents have bounded cleanup.
- [ ] **4. Build the publisher input view.** Expose endpoint identity, local observer/signing identity separately, latest eligible measurements, raw-document digest/parsed NIP-11, access/freshness and publication permission. NIP-66 kind-30166 subject remains the full endpoint; incoming pubkey-only subjects stay imported assertions. Kind-10166 configured check/cadence declarations are separate from actually achieved measurement freshness.
- [ ] **5. Enforce independent visibility and source cuts.** Public claims cannot inherit private query-bearing URLs/auth context, precise geolocation or secrets. Coarse geolocation publication requires its own permission. Raw observations, aggregates and readiness share compatible committed cuts; a current policy revocation still applies to a previously pinned cut. Phase 07 chooses/specifies actual wire representation and signs durable inputs without re-probing.
- [ ] **6. Exercise field, denominator and replay traps.** Apply a full catalog, liveness-only success, executed timeout, unknown-dispatch recovery and duplicate replay; then expire a raw subset under explicit policy. Expect stable logical counts, old TLS age, timeout in the proper attempted denominator, unknown separately, and declared reduced evidence after restore. An independent reference reducer catches merge-order dependence and manufactured uptime.
- [ ] **7. Run future p04-t11 on real archive/CH/PG and later publisher adapter.** Compare independent raw reduction and current/aggregate rows at one cut, inspect document reachability after replay, and confirm no monitor traffic is triggered by publication consumption. Retain retention manifests and redacted view artifacts. Phase 07 adds actual signed NIP-66 wire verification; phase 06 adds public query journeys without declaring those integrations already exercised.

## P04-T12 — Qualify the fresh-deployment lifecycle and operating envelope

Dependencies: P04-T01–T11, P03-T01/T02/T04, P01-T08/T09, P02-T08/T09. Later
phase 06/07/08 integrations add public reads, real signing/publication and full
backup operations. Inputs are packaged roles, controlled datasets and explicit
route permissions; output is an evidence-backed supported operating envelope.

Paths: `tests/acceptance/cases/relay_lifecycle.py`,
`tests/fixtures/relay-lifecycle/end_to_end.json`, `packaging/compose/acceptance.yaml`,
`apps/bigbrotr/tests/relay_lifecycle_faults.rs`, `docs/validation/relay-lifecycle.md`,
`docs/operations/relay-lifecycle-recovery.md`, `docs/operations/monitor-capacity.md`.

- [ ] **1. Exercise first useful startup.** From clean volumes run config init/show/validate and doctor, then start intended roles. A versioned controlled seed overrides external inputs. The private-loopback journey explicitly operator-enrolls only its named fixture endpoints as unverified under the exact private route; seeding/discovery supplies independent source evidence and Validator must still demonstrate actual protocol behavior. A separately isolated controlled-public namespace case exercises default candidate auto-enrollment; no implicit private-address bypass is permitted; the full recipe's explicit public-candidate validation/automatic enrollment policy authorizes Validator work, not the source mention itself. Successful attributable public validation supplies active enrollment for acquisition; zero configured directories still permits seed→candidate→Validator→Monitor→Synchronizer. Explicit operator enrollment remains an independent permission with unverified probe state until measured. Prove role readiness from storage/policy capabilities, not named-service startup order.
- [ ] **2. Follow one coherent source-to-evidence journey.** Seed A, ingest a signed list mentioning B and negative preference C, validate/read A/B, capture DNS/TLS/NIP-11/liveness/Geo where configured, then acquire an event from both. P04-T13 event-availability jobs, when explicitly enabled, require provenance permission and may not retain event/endpoint pairs when provenance is off; an ordinary duplicate delivery is never such an experiment. Independent fixture manifests determine endpoint/evidence/measurement/body/first-witness sets; negative C is not an operator ban and mentions are not witnesses.
- [ ] **3. Exercise retained facts versus operational counters.** Flood valid duplicate event deliveries, queue deferred monitor work, perform actual failures and interrupt one dispatch intent. Permanent history gains only defined first/earlier provenance and executed/uncertain evidence under its schema; ordinary duplicate counters and unexecuted queue decisions remain bounded operations. Compare actual archive bytes/rows and ambiguity denominators. A certified duplicate page advances with retained proof closure and zero new ingest requests/WAL outcomes; an actual repeated measurement still has its separate attempt/receipt. A private witness at the same access context cannot satisfy the public acquisition inventory or suppress a required public witness.
- [ ] **4. Apply pressure and isolation.** Pause PG, ingest/archive and projection separately; exhaust host permits/FD/RAM/document spool; run unavailable overlay routes alongside public fixtures. Acquisition/measurement honor distinct authority and reserved budgets, emit typed outcomes and resume from receipts without fabricating peer failure or complete coverage. Lower limits while work is active and inspect drain behavior.
- [ ] **5. Measure finite fleet work.** Use a declared synthetic fleet size, seeded schedule and fixed duration with actual sockets for a representative subset and explicitly labeled scheduling-only expansion. Record achieved cadence, backlog age, CPU/RAM/connections/wire bytes, M growth and deduplicated document bytes. Include durable dispatch-intent, terminal and correction writes in M cost, plus current projections, dictionaries, object references and retention/pin overhead. Compare wide compressed observation rows against object-plus-reference encoding on identical low/high document-change corpora, measuring total archive/index bytes, queries, native memory and write amplification; object normalization does not automatically win. A simulation of 100000 schedules is not a measured 100000-socket capacity claim.
- [ ] **6. Verify split-role recovery and later integrations.** Run Monitor through authenticated remote AdmitEvidence, lose an acknowledgement and restart; independent readers compare immutable fact IDs and complete source-cut ranges. Restore through P02/P08 contracts. Later phase 07 verifies signed announcements use retained fields without re-probing; later phase 06 checks authorization and field freshness through actual public sockets.
  Run the shipped monitor-only recipe without Synchronizer/Discoverer/Validator
  or CH, with selected A and unrelated pre-enrolled B. Observe only A's bounded
  probes, zero signed EVENT and zero collection subscriptions; inspect durable
  A05/A06 plus private status and offline export. Online monitor pages remain
  unavailable until explicitly selecting Indexer/CH/read. Restart and change the
  fixed list while checking preserved observer identity and attempt recovery.
- [ ] **7. Publish the future qualification record and operator journey.** The p04-t12 report includes exact executed cases and NOT RUN dependencies, commands, scope/versions, logical oracle results, cuts/receipts, faults, resource envelope and residual uncertainty. Run disabled directories/Geo-refresh/active-write/overlay cases and show zero corresponding outbound work; Monitor disabled must not start a hidden measurement queue. With --once, distinguish healthy zero-due success, another-owner skipped, explicit cancellation and infrastructure failure. Validate documentation command examples through the built CLI and help surface. Document bootstrap, revive/block, missing credentials/datasets, last-good refresh, monitor backlog, measurement cost and explicit lossy retention recovery. No source inspection or unexecuted fixture is reported as a passing product test.

## P04-T13 — Explicit finite event-availability observations

**Depends on:** P04-T02/T07, P03-T02/T03 and P02 evidence admission. P05-T08 and
P09 availability pages consume this producer; synthetic reducer rows do not close it.
**Files:** `apps/bigbrotr/src/acquisition/monitor/availability.rs`,
`contracts/relay/v1/availability-job.json`, `tests/acceptance/cases/availability_probe.py`,
`docs/operations/availability-observations.md`; extend the existing monitor CLI,
configuration and P04 registry/job migration, plus A05, without a second daemon
or permanent retry log. The [architecture](../architecture/acquisition-services.md#selected-availability-and-issuer-evidence-producers)
owns gates/profile defaults; the [interface catalog](../specification/interface-catalog.md)
owns shared persisted-job submission/status/resume behavior.

- [ ] **Step 1:** define `event-availability/1` as an explicitly submitted finite
  Monitor job through `bigbrotr monitor availability run --selection FILE` and
  typed `AvailabilityJobRequest` on the shared job API. Validate submission
  idempotency/digest, explicit pairs or pinned bounded local query, source cut,
  access/visibility and profile version. Deduplicate overlapping selected pairs
  before applying limits. Persist the scientific `selection_id`, exact selection
  or reproducible descriptor/method/seed, eligible scope and selection-completion
  state separately from mutable job progress. The H gate
  `monitor.availability.enabled=true` permits these jobs only under provenance and
  current event/endpoint grants; it creates no periodic or continuous active
  probing. False or provenance off returns capability-unavailable before dispatch,
  without breaking unrelated startup. No implicit all-relay cross product or
  unsupported representative/random-sample claim is allowed.
- [ ] **Step 2:** capture the versioned profile's J values: 100 IDs/endpoint/job,
  20 IDs/REQ, 1,000 pairs/job, four concurrent requests, 10s request timeout,
  `max_network_requests=2000` and `max_duration=30m`. Reserve its consumed request
  count durably with dispatch intent before each availability REQ, including
  singleton follow-ups and retries after reconnect; transport/
  AUTH traffic also obeys common frame/rate/byte grants. The job deadline includes
  waiting and follow-up work. Persist consumed requests and deadline; resume
  cannot reset either. Shared host/endpoint/admission grants can lower execution
  capacity. Refuse an oversized selection or explicitly partition it into newly
  admitted jobs before dispatch. Budget exhaustion stops new requests and exposes
  unknown attempted pairs and separate unattempted pairs; it cannot manufacture
  completion. Outstanding receipt resolution remains recoverable after the
  measurement deadline. A fully exhausted job needs a new explicit submission
  for more measurement work, not a reset hidden behind resume.
- [ ] **Step 3:** use durable dispatch intent and the actual guarded WS stream.
  Independently verify complete returned bodies and correlate each requested ID.
  A valid delivered body can establish served evidence; omission from a multi-ID
  batch remains unknown even after EOSE. Follow omitted IDs with bounded singleton
  exact-ID REQs before claiming completed-not-served. A truly empty singleton
  plus ordered EOSE may establish that result only after all preceding validation/
  admission work is settled, with no invalid/dropped/unattributable input or
  `more`, AUTH, rate-limit or other restriction. Disconnect/deadline exhaustion
  stays unknown. Preserve final request profile and batch/singleton context in
  the evidence. Scoped not-served is neither physical/historical absence nor a
  deletion instruction.
- [ ] **Step 4:** use `MeasurementSubject::RelayEndpoint{endpoint_id}` and admit
  A05 child facts per actually attempted pair, plus the defined execution-unknown
  recovery record where intent cannot prove dispatch. Preserve shared source
  identity, receipt, access/observer/clock/method and `selection_id`. Each external request
  has one `attempt_id`; batched pair children share it, while singleton confirmation
  is a new external attempt. A logical pair is not an attempt or job identity.
  Retrying persistence resolves the same `ProducerRequestId`/fact/receipt rather
  than adding a measurement. Move persisted job progress only after durable
  terminal/unknown evidence. Disablement/revocation stops later dispatch and
  resolves actual outstanding commits; it neither erases an issued receipt nor
  invents outcomes for selected-but-unattempted pairs.
- [ ] **Step 5:** connect the actual stored facts to P05-T08 and its public/private
  history resources. Expose disabled/no-observations/partial and selected,
  attempted, served, not-served, unknown and unattempted counts distinctly.
  Shared persisted-job status/resume APIs retain the same input/profile/counter
  identities. Provenance off creates no hidden pair index or background job.
- [ ] **Step 6:** run the shipped job against a peer holding all 20 selected IDs
  but returning one per batch plus EOSE; singleton follow-ups serve each ID.
  Expect all served after permitted confirmation, or served plus unknown when
  budget ends, never 19 fabricated negative votes. Include a truly missing
  singleton, `more`, forged requested body, dropped pre-EOSE frame, AUTH/rate
  refusal, interruption and timeout. Compare independent socket logs, archived
  facts, distinct-ID reducer output and later P09 page data; 19 IDs with repeated
  attempts must not cross the 20-distinct-ID ratio threshold.
- [ ] **Step 7:** document sampling bias, all limits and pair-sized cost. Exercise
  the actual CLI/Manage parser, invalid selection and idempotent submission,
  cancellation/receipt loss, restart after consumed request budget, deadline
  expiry while queued during backfill, and both gate/provenance-off paths.
  Compare selected/executed/unattempted sets independently. Ordinary passive
  duplicates must produce no availability-experiment facts; restarting or cache
  reuse must not add network requests beyond the saved profile limits.

## P04-T14 — Acquire attributed LNURL issuer metadata

**Depends on:** P04-T02/T07/T08 and A06 document admission; P05-T05 consumes the
result without networking. **Files:** `apps/bigbrotr/src/acquisition/issuer.rs`,
`contracts/evidence/v1/issuer-observation.json`,
`tests/acceptance/cases/issuer_evidence.py`, `docs/operations/issuer-evidence.md`.
Extend the existing CLI/configuration and registry job/cooldown operations.
Use the existing finite acquisition/job/evidence owners and no separate service;
the architecture profile and interface catalog above govern defaults and commands.

- [ ] **Step 1:** define a bounded demand queue from selected unresolved zap/profile
  subjects from admitted public inputs and explicit operator jobs. The H gate
  `issuer.enabled=true` permits eligible demand from enabled zap analytics and
  `bigbrotr issuer lookup --recipient PUBKEY --profile EVENT_ID`, which submits
  typed `IssuerLookupRequest` through the shared persisted job API. Capture
  submission identity/digest, source cut, `profile_event_id`, the
  `issuer-metadata/1` revision, access and visibility; status/
  resume reuse the same job and resolve ambiguous receipts. False yields
  unavailable issuer inputs and no later dispatch, without making unrelated
  startup fail. The default alone does not schedule arbitrary HTTP work.
  Private-input-driven external lookups require explicit disclosure/egress authority, not just read
  permission. Bind recipient, source cut and recipient-signed profile defining the
  LNURL endpoint before submission. A payer-signed request's lnurl alone cannot
  authorize that recipient's issuer; uncorroborated bindings stay claims. For split
  recipients, require the selected recipient's own profile binding too. Preserve
  request/target references and method version without promoting their authority.
- [ ] **Step 2:** capture `issuer-metadata/1` J values: four concurrent lookups,
  10s whole-lookup deadline including redirects, 64KiB decoded response, two
  redirects, 24h cache freshness, 4,096 working entries/64MiB, one network start/s
  with burst four, and one-hour unsuccessful retry spacing. Coalesce eligible
  recipient/resource/access/visibility demand; preserve each signed profile
  binding even when it references an existing response. Queue, active inputs/
  decoding, cache and cooldown state share that single working grant across
  workers. Reserve a durable registry cooldown obligation before dispatch;
  unexpired obligations continue to consume entry capacity after cache eviction.
  Failed or ambiguous completion extends `not_before` to at least completion/
  recovery time plus one hour. Check it before each later start for the same
  recipient/resource/access/visibility key; profile replacement alone cannot
  bypass it. Retain unresolved dispatch until safe receipt/recovery retirement.
  Expire only resolved elapsed cooldowns; under pressure evict cache values or
  defer new identities instead of discarding cooldown obligations. Unavailable
  registry/cooldown capacity means no dial. Host/address/current memory grants
  still apply. Overflow reports deferred coverage; incremental feed/checkpoint
  work avoids full repeated unresolved-population scans. A cache hit references
  original A05/A06 evidence and time, creates no new fetch, and cannot establish
  continuous key authority throughout the 24-hour freshness window.
- [ ] **Step 3:** fetch only metadata through the shared guarded transport. Validate
  expected fields including `allowsNostr`/`nostrPubkey` under the pinned NIP-57
  contract. Never follow an invoice callback, pay, sign or resolve from analytics.
  Persist actual retrieval time, HTTP/TLS basis, original bounded document/digest,
  recipient/URL binding, observed key and parser outcome as A05/A06 evidence.
  Use the canonical tagged
  `MeasurementSubject::RecipientMetadataHttp{recipient_pubkey, profile_event_id, http_resource_id}`;
  the guarded HTTP descriptor and actual request/redirect/dial context remain
  distinct from a Nostr relay endpoint. A06 references resolve through that
  subject/observation contract. No fabricated endpoint or relay enrollment is
  required. A persistence retry resolves the original attempt and receipt;
  a later actual fetch is a distinct `attempt_id` subject to the durable cooldown.
- [ ] **Step 4:** distinguish observed-at-fetch key match, contradictory observation,
  and an attributed historical issuer assertion with its own authority/time scope.
  Today's metadata never retroactively authenticates old receipts. Observed-only
  status is available to researchers but cannot satisfy a stronger historical
  acceptance policy; P05 keeps that receipt unresolved without eligible evidence.
- [ ] **Step 5:** import historical issuer evidence only under an explicit source/
  trust class and the normal A03 validation/visibility contract. Bind every zap
  classification to the evidence IDs and issuer-policy version. Unknown trust or
  time scope does not become historical proof through import or a cache hit.
- [ ] **Step 6:** run controlled provider key changes, stale cache, malformed/oversize
  metadata, payer-selected impostor issuer, private-input disclosure denial,
  private-address rebinding and old receipts with no eligible evidence.
  Inspect real guarded requests, documents, classified facts and NIP-85 inputs.
  Export an observation for a recipient with no relay identity and independently
  resolve profile, HTTP resource and original document while the registry gains
  zero relay enrollments. Reject mixed subject-variant fields and cross-visibility
  document references. A current-fetch-as-historical-proof variant must fail the
  independent oracle.
- [ ] **Step 7:** measure lookup/cache/queue cost and the fraction of the declared zap
  corpus remaining issuer-unresolved. Document that source acquisition cannot
  recover unobserved historical authority; lack of evidence withholds affected
  fields, not original receipts. Include actual CLI/Manage submission/status/
  resume, lost receipt, outage and gate-disable journeys. Drive distinct recipients
  through a deliberately small shared grant; evict a cached response, restart the
  worker and prove from actual start times that its durable cooldown still holds.
  Exhaust cooldown capacity and verify deferred new demand, bounded state and
  incremental recovery. Disabled or private-disclosure-denied jobs emit no request;
  current lookup or cache results cannot repair missing historical authority.

## Coverage and configuration ownership

For T14's historical issuer branch, P03-T10 owns the optional A03
`IssuerAuthorityBindingV1` import/admission producer and its exact schema in
[assertion bindings](../specification/assertion-bindings.md). T14 supplies current
HTTP observations and must test that they cannot mint the claimed historical
interval. Qualified operator-attested inputs and their public-use authority are
separate; absent or disabled input withholds strict zap wire fields. No new daemon
or automatic external trust source is introduced.

T07/T08 must actually produce P05-T12's selected latency/cadence inputs: preserve
request-write-complete to matching EOSE duration separately from handshake and
first-event RTT, the effective base cadence/profile at the attempt and its
method/cadence boundaries. Pooled connections reuse prior handshake evidence;
they do not fabricate fresh handshake samples. Unavailable clock/phase fields
withhold that latency component. The end-to-end quality fixture must consume
these stored actual monitor fields, not injected precomputed RTTs alone.

Defaults, units and reload classes are authoritative in the acquisition contract;
these tasks must add corresponding typed fields, rendering, validation and the
behavioral change cases rather than invent parallel per-service allocators.

| Capability/configuration family | Implementation owner | Specific acceptance boundary |
|---|---|---|
| Full URL identity, observer, policy/self endpoints, candidate_validation, auto_enroll_validated_public, enrollment and bans | T01; P01 identity/authority | Distinct queries/paths, non-dialable subject, stale ban override |
| network/public/private/overlay/TLS/redirect/compression/global budgets | T02; P01 grants | Actual pinned address, SNI, proxy ATYP, no second DNS resolution |
| seed.file/enroll/assumed_scheme/max_line_bytes/batch_size; bootstrap.seed | T03 | No network, zero-admissible failure, restart idempotency |
| discoverer.events enable/pages/batches/workers/quantum/slice/replay | T04 | Late signed time and post-quantum reference survive restart |
| discoverer.api source/interval/retry/timeout/delay/response cap; evidence_recent_limit | T05/T01 | Repeated same digest does not revive; independent archive progress |
| validator claim/concurrency/deadlines/NIP-11/retry/dormancy/revival | T06 | 101 differs from readable Nostr; infrastructure failure is not peer failure |
| monitor endpoints.mode/endpoints.urls/cadence/claim/concurrency/host budget/jitter/timeouts/retries/backoff | T07 | Fixed selected endpoints without Validator; actual start versus durable intent; bounded due backlog and no stale catalog refresh |
| monitor.checks/store_checks and per-family values/doc limits | T07/T08/T10 | Stored set is a subset of computed checks; every unexecuted child remains unmeasured |
| monitor.geo paths/source/product/refresh/schedule/age/precision/download cap | T09 | Independent A/AAAA/ASN/City, corrupt update leaves last-good |
| monitor.active_write enable/signer/scope/interval/PoW limits; AUTH | T10; P07 signer | Default zero signed EVENT; OK without exact verified retrieval is partial |
| raw_retention, aggregate_retention, document references and NIP-66 handoff | T11 | Permanent default, explicit lossy manifest, no fabricated denominator or publisher probe |
| Runtime counters, health, finite once runs, split roles and capacity reporting | T12 and each task | Actual CLI/engine evidence and bounded transient counters |
| monitor.availability.enabled; event-availability/1 captured profile and persisted jobs | T13; existing CLI/Manage owner | False/provenance-off is unavailable without unrelated startup failure; capped batches need singleton confirmation; counters/deadline survive resume |
| issuer.enabled; issuer-metadata/1 captured profile, shared working grant and durable cooldowns | T14; existing CLI/Manage/registry owners | Tagged non-relay subject, zero enrollment, cache eviction/restart cannot bypass cooldown, full working-set pressure defers |

## Recorded planning revisions

These are document reviews and edits, not executed product checks. The coordinator
reconciles them with the requested whole-plan review rounds.

1. **Code-lesson and dependency reread.** Compared the draft with the guarded
   transport/session lessons and current data/component contracts. Resolved
   concrete transport ownership to P04-T02 and the reserved 0006 migration;
   P01 retains pure primitives and budget/authority contracts. The plan now
   explicitly permits this transport slice before P03 sockets without requiring
   the whole relay lifecycle. Retained actual socket/SNI/proxy observations as
   evidence, with no promotion from a bare upgrade or borrowed host metadata.
2. **Field, privacy and retention review.** Aligned `docs/operations/` paths;
   required stored-check subsets and explicit unmeasured children. Separated
   original document digests from redacted derivatives and prevented raw credential
   retention. Made provenance-off availability-experiment prohibition explicit;
   permanent executed measurements and optional lossy manifests remain distinct
   from bounded scheduler/delivery state. Applied provenance-off exclusion of
   interpreted imported event→relay claims without suppressing signed originals
   or non-provenance monitoring/discovery facts.
3. **Crash/cancellation/external-action review.** Refined durable dispatch
   ambiguity: a RAM-only start timestamp does not prove recovered execution;
   unresolved intent survives display TTL until its recovery link is durable.
   Added terminal-evidence-before-registry promotion, stale-owner suppression,
   grant revocation before versus after a signed send, and exact-byte retry after
   unknown acknowledgement. Fixture expectations distinguish both sides of each
   boundary; no transaction claims exactly-once remote execution.
4. **Memory/disk/skew/physical-cost review.** Bounded host/DNS/proxy metadata
   independently of socket count, included parser scratch and simultaneous Geo
   compressed/extracted/pinned generations, and distinguished logical document
   deduplication from physical savings. The monitor journey now measures durable
   intent/terminal/correction cost and compares wide compressed rows with
   object-plus-reference storage on identical corpora; no presumed byte win.
5. **Packaged journey and capability review.** Added explicit fixture trust,
   route, feature, source and fault preconditions and failing comparator controls.
   Removed Seeder as a Validator runtime prerequisite, made the full recipe's
   candidate-validation authority explicit, and reused the canonical archive
   measurement schema. Fixed a concrete versioned active-write marker profile;
   fixture evidence must come from its real authorized signer. Added disabled
   feature zero-work observations, once-outcome distinctions and CLI documentation
   parsing to packaged qualification. Private-loopback fixtures explicitly enroll
   their named unverified endpoints; a separate controlled-public case tests
   automatic candidate admission, avoiding an implicit private-route bypass.

Coordinator follow-up after whole-phase read: specified full-recipe candidate
validation/automatic-public-enrollment settings, still constrained by bans and
actual egress grants. Confirmed no archival event-delivery series and reused the
shared Receipt type. Measurement/registry recovery resolves committed outcomes;
post-commit revocation restricts later work rather than falsifying existing facts.

## Cross-contract repairs from the whole-repository audit

P04-T02 and P03-T07 implement the hostname-plus-selected-IP grants and alias
fixture in [destination fairness](../architecture/acquisition-services.md#pooled-transport-evidence-and-destination-fairness).
P04-T07/T08 retain pooled handshake identity/time and use the same endpoint slot
for a coordinated fresh connection; two liveness ticks cannot refresh old TLS.
P04-T11 writes LocalMonitorView through the Indexer. P05-T06 consumes/reuses its
pure local eligibility rules while separately owning ResolvedRelayEvidence;
local NIP-66 output has no cross-observer resolver dependency. Test local failure
plus imported success, and resolver disablement, at those actual consumers.
P04-T12 adds T13/T14 journeys when their capabilities are enabled; the original
passive monitoring gate requires neither an availability job nor a zap lookup.
