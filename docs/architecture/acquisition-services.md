# Acquisition services and relay identity

Status: concrete design proposal for implementation planning. Defaults are engineering starting points, not measured production capacity. Product assumptions awaiting owner answers are recorded through the coordinating planning workstream. No product implementation or test execution is claimed here. Source evidence and historical differences are in [Acquisition findings](../research/acquisition-findings.md).

## Boundaries and deployment

BigBrotr retains six independently selectable acquisition capabilities: Seeder, Discoverer, Validator, Monitor, Synchronizer and Importer. Shared libraries supply identities, typed protocol handling, policy checks, durable ingestion and bounded scheduling. A logical service is not required to be a separate process, nor to run on the same host as its producers.

| Capability | Purpose and input | Durable output and write ownership | Actual dependency |
|---|---|---|---|
| Seeder | Operator/bundled relay list | Discovery evidence and enrollment requests | Registry write API |
| Discoverer | Ordered accepted-event feed and optional external directories | Discovery evidence, candidate queue entries, extraction checkpoints | Registry; event feed only for its archive leg |
| Validator | Due enrollment/candidate work | Admission transitions and validation observations | Registry; guarded transport |
| Monitor | Eligible known endpoints and check schedule | Typed measurement records; latest-state projection; run envelopes | Registry; guarded transport; durable ingest |
| Synchronizer | Enrolled endpoints, acquisition selectors, scoped traversal work | Verified bodies, local provenance, traversal/evidence/gap records | Registry; durable ingest; ordered identity-set reader; guarded transport |
| Importer | Explicit immutable corpus and manifest | Verified bodies, imported claims, job progress and rejection summaries | Durable ingest; registry identity API; policy snapshot |

Seeder and Importer are finite jobs by default. The other services run independent asynchronous loops. No slow validation cycle blocks monitoring, discovery or synchronization. Disabling a producer leaves existing data usable and exposes its age. Missing required storage is a dependency failure, while an empty valid queue is healthy idle. A service-start graph must not invent a requirement to co-run Seeder, Discoverer or Validator when an existing registry already supplies relays.

Prefer a single binary with selectable services and in-process contract implementations for a single host. On multiple hosts use authenticated bounded ingest RPC to an append authority, or independently owned ingest partitions with globally unique record identifiers. Do not share an embedded database directory or writable journal across processes. PostgreSQL-style transactional registry operations are a suitable control-plane implementation; the storage decision document owns the selected engine. Analytics availability is not required to admit relay events or scan discovery evidence.

Three alternatives were considered: retaining the sequential Go supervisor would reproduce unrelated-service stalls; one process per named service from day one would add operational dependencies; composable service tasks with the same durable contracts preserve small installations and allow later process separation. The third is the recommendation. The no-RPC rule and the single-root-journal layout are not product invariants.

Each selected worker validates its effective configuration and actual required adapters, opens stores, restores durable progress, then acquires work. It reports process readiness separately from data freshness and empty-queue state. Leases use registry-issued fencing generations and registry time; expiration alone is insufficient if an old owner can still write. On renewal uncertainty the worker stops claims and closes its active request work before its grant can expire. Shutdown stops new claims, closes subscriptions, waits for bounded outstanding receipts and writes only receipt-backed progress. A drain timeout exits with resumable work outstanding; it does not manufacture a completed run. CLI status uses the shared health states `starting`, `ready`, `idle`, `degraded`, `draining` and `stopped`, with separate activity, reason and last successful durable action. Running work is an activity; backpressure and unavailable dependencies explain degraded health.

## Shared records and invariants

Names here are semantic contract names, not a requirement for a specific database schema. Authoritative scientific facts use the exact immutable source envelope in [data contracts](../specification/data-contracts.md#immutable-source-envelope); measurement methods/scopes carry their relevant revisions. Producer request IDs and process/config execution records remain operational and are not copied into every archival event. Preserve nonnegative signed-event timestamp integers losslessly in a 64-bit unsigned field; local UTC observations use integer microseconds and measured durations use a monotonic clock. Negative/noninteger/overflowing event timestamps receive explicit schema-rejection results. A verified event timestamp unsupported by a downstream calendar type remains an integer with a conversion-status flag. NIP-77 reserves the maximum uint64 timestamp: an event at that value cannot enter its set and receives a protocol-range gap, not silent data loss. Do not cast hostile timestamps into a smaller database type before range handling.

| Record | Required fields and semantics |
|---|---|
| `EndpointIdentity` | Opaque stable id, identity-normalization version, normalized websocket URL or non-dialable subject, network/route class, sensitivity, creation time. Identity creation grants no permission to dial. Unique on normalization version and normalized value. |
| `DiscoveryEvidence` | Idempotency key, endpoint/subject id, source type, source artifact/event id, original URL reference or protected bytes, author and kind when present, tag offset/marker, observed_at, evidence polarity, extraction version. A reference is evidence of mention, not an endorsement or local observation. |
| `AdmissionState` | Endpoint id, enrollment origin, `pending/active/dormant/blocked`, latest attributable protocol result, attempt/failure counts, next_due, policy revision, source evidence revision. Operator bans are independent from transport failures. |
| `CapabilityObservation` | Endpoint, observer, authorization scope, protocol feature, `advertised/observed/unsupported/inconclusive`, outcome reason, method/version, measured_at, expires_at, optional bounded raw-document digest. Unsupported requires attributable evidence; timeout is inconclusive. |
| `LocalWitness` | Verified event id and signed created_at, endpoint id, local observer id, authorization-scope id, received_at, acquisition layer, visibility class and durable source record. Body deduplication does not suppress a new witness. |
| `ImportedClaim` | Verified event id when available, claimed endpoint/subject, claimed first_seen_at, original observer if supplied, source artifact id and record position, imported_at, claim trust class. Its timestamp never replaces the local first-witness timestamp. |
| `Measurement` | Observation id, endpoint, stable observer UUID, check family/method version, per-check timestamps/durations/statuses, endpoint/address/route context, document references and visibility. Optional signer pubkey is separate from observer UUID. |
| `CoverageAttempt` | Attempt id, endpoint/filter/auth scope, half-open signed-time interval, algorithm and version, start/end wall times, result class, EOSE hints/cap evidence, requested/received/missing counts, policy exclusions, receipt references, next traversal position and optional parent partition. |
| `AcquisitionGap` | Scope/interval or exact event id, reason, first/last observed, retry due, attempts, latest evidence, unresolved/resolved status and resolution receipt. Resolving adds history; it does not erase the earlier gap. |
| `ImportJob` | Job id, source manifest/digests, parser and policy versions, requested scope, current phase, shard/offset, last durable receipt, bytes/records and outcomes, reject-artifact reference, terminal status. Multiple independent jobs may coexist. |

`received_at - created_at` measures signed-age at receipt. It is not proof of network latency or clock skew: signed time is author-controlled. First-local-seen, first-imported-claim and first-durable-admission are three distinct answers. Compact local witness records by minimum receipt time per `(event, endpoint, observer, auth scope)`; ordinary repeated arrivals remain bounded operational diagnostics and never a permanent archival series. The product may display a cross-observer minimum as a derived value with its evidence class.

### Durable ingest contract

`Admit(batch, producer, policy_revision, idempotency_key)` returns per-record outcomes and a `DurableReceipt`, or a retryable/terminal error. The receipt identifies stream/partition, committed sequence range, durability class and batch digest. A producer must not checkpoint network traversal, observation publication readiness or import offsets beyond receipts the ingest authority guarantees survive its documented crash model. RAM acceptance, SDK callbacks, successful RPC transport and analytics visibility are not receipts.

Admission applies finite framing/decompression bounds, strict event decoding, id recomputation and Schnorr verification, semantic acquisition scope, current moderation and event/edge deduplication. Cheap rejection may precede signature work, but an unverified claimed id must never enter a permanent seen/suppression set. Verified blocked ids produce durable suppression metadata, not event bytes. Already-held bodies with new provenance still append the provenance.

The coordinated fidelity decision requires the complete signed Nostr event: id, pubkey, created_at, kind, original tag arrays/order, content and signature. Signatures are always retained. There is no heavy-fields switch or lossy LilBrotr/IndexBrotr deployment. Auxiliary wire JSON layout, unknown event-object extensions and websocket envelopes are separately attributed source evidence; they are not authenticated merely because the canonical event signature verifies. Exact wire preservation, when enabled by storage policy, is distinct from preserving all canonical signed fields. Thin analytical exports remain labeled projections and are not standalone verifiable events. Reject ambiguous duplicate critical JSON keys rather than let different parsers authenticate different interpretations.

The append authority atomically establishes dedupe ownership and recoverable records, or rebuilds the dedupe index from committed records after a crash before serving new traffic. An optimistic in-memory claim cannot survive independently of its record. Progress updates are fenced by a lease generation; a stale worker may cause harmless duplicate records but cannot move traversal state. An append succeeds but progress update fails: replay/re-fetch from the old progress position. An append fails: no associated progress change.

Moderation has a monotonically increasing revision. A durable acknowledgment is checked against the active revision at admission, so a long import cannot use its startup-only blocklist. Unknown revision, unavailable required policy state, disk failure, or exhausted ingest quota stops new durable acknowledgments. NIP-09 events remain signed input records; product deletion/takedown policy is a separate durable control record applied by shared storage and serving policy.

Per-producer queues have both byte and item bounds. When full, pause readers where transport supports it; close/retry subscriptions when bounded buffering cannot continue. Record the interruption interval and recover through recent/historical sweeps. Offline import pauses without losing its source position. Independent monitor capacity is reserved so bulk imports cannot consume all admission capacity. Queue and retention limits must make disabled-consumer behavior explicit.

## Endpoint identity and transport

### Identity v1

Normalize through one versioned authority. Accepted network inputs are absolute `ws` or `wss` URLs. Operator seed input may omit a scheme only when the parser explicitly adds `wss` and records that inference. Network-extracted `http`/`https` references are web links, not automatic relay candidates. Pubkey-only NIP-66 subjects are retained as non-dialable identities.

Normalization lowercases scheme and IDNA-mapped ASCII hostname, removes one terminal DNS root dot, normalizes strict IP spelling, normalizes percent-escape case and unreserved characters, removes path dot segments, and supplies `/` for an empty path. Remove only the default port for the actual scheme: 80 for `ws`, 443 for `wss`. Preserve non-root trailing slash, path case, duplicate slashes, encoded reserved separators, query bytes/order and an explicitly empty query. Reject userinfo, fragments, malformed percent encoding, invalid ports, zone identifiers, ambiguous legacy IPv4 spellings, invalid UTF-8 and controls. Preserve raw input evidence; normalization never silently strips credentials or rewrites an HTTPS link into a websocket target.

`ws://example/`, `wss://example/`, `wss://example/path`, `wss://example/path/`, and query-distinct endpoints are distinct identities. Aliases and identical datasets can be derived later without rewriting provenance. The connection path, the ban comparison, AUTH relay tag and publication subject use the same parsed identity. Query-bearing/private endpoints are restricted by default; public surfaces use endpoint ids and redacted display URLs unless their publication visibility is explicitly configured. Identity normalization changes require a migration mapping and re-evaluation of bans/self-exclusions, not a hot reload.

This is deliberately more conservative than the Go scheme-less identity. The rationale is in [Acquisition findings](../research/acquisition-findings.md#protocol-findings-that-change-the-plan); normalization is grounded in [RFC 3986](https://www.rfc-editor.org/rfc/rfc3986.html#section-6).

### Guarded dialing and budgets

All websocket, HTTP, TLS, directory, Geo refresh and redirect requests go through the shared egress adapter. Direct public dialing resolves once, canonicalizes IPv4-mapped addresses for policy checks, filters all results, and dials only a pinned allowed address while retaining original Host/SNI. Re-check each fresh connection and redirect target. A hostname with both public and denied addresses may use only its public addresses; report policy exclusion if none remain. Never fall back to the OS client's default resolver or a second unguarded resolution. TLS verifies both chain and hostname; configured custom trust roots are supported, global verification bypass is not.

Use explicit `public`, `private` and overlay route policies rather than inherited global localhost XOR. Public route excludes loopback, private, link-local, carrier NAT, unspecified, multicast, broadcast and reserved address space. Private routes require operator-declared hostname/endpoint plus CIDR/port scope and are ineligible for public publication. Metadata/link-local addresses remain denied unless an exact private policy explicitly names them. The default has no private route. A fixture profile can allow only its loopback relay without exposing public crawling to private addresses.

Tor/I2P/Lokinet suffixes select distinct configured routes. Missing adapters are configuration failures when enabled, not relay downtime. Never resolve overlay names through public DNS. Proxy-resolved overlay routes disclose that the worker cannot inspect the final remote IP; restrict permitted suffixes and delegate isolation to the route. A clearnet host sent through a proxy retains local resolved-IP checks and pinned address, unless an explicitly declared remote-resolving route supplies equivalent enforceable policy. An unavailable overlay does not consume the public route's entire worker budget.

One websocket per `(endpoint, auth scope)` multiplexes live, backfill and probe subscriptions where possible. Sharing across different authorization identities is forbidden. Pooling separate processes is optional; a central scheduler still enforces endpoint/host connection and request tokens. Exact hostname, selected IP and registered-domain fairness are different budget keys: no shared-host TLS fact is inferred from a common apex. Public clients must not issue reverse DNS or geography queries for hidden-service endpoints.

Capability probe evidence uses a specific request/subscription and parser result. Generic NOTICE text is not enough to demote a feature. Cache inconclusive results briefly, explicit unsupported results longer, and positive results with bounded expiry. Preserve NIP-67 hint arrays even if the selected Rust SDK drops unknown trailing fields. Close/reissue requests on authentication changes; keys are read-only signer references scoped to exact endpoint/route and permitted operation, with no implicit signing identity. [NIP-42](https://github.com/nostr-protocol/nips/blob/master/42.md), [NIP-67](https://github.com/nostr-protocol/nips/blob/master/67.md).

### Shared configuration

Reload classes: **H** applies atomically to new work on hot reload; lowering limits drains existing work without admitting additional work. **C** applies to new connections after bounded drain. **J** is immutable for the current finite job, overridable for a new job. **R** requires a controlled restart. **E** creates a new semantic epoch and preserves prior evidence. Durations require units; bytes accept IEC suffixes. Unknown keys, duplicate keys, nonfinite numbers, negative/zero nonsensical bounds and unsupported enabled features fail validation, including in disabled blocks. Secrets are references and are redacted in effective configuration.

| Key | Default | Unit | Reload | Meaning/validation |
|---|---|---|---|---|
| `network.public.enabled` | `true` | bool | H | Public route |
| `network.private.rules` | `[]` | scoped rules | C | Explicit endpoint/hostname, CIDR and port; no broad implicit localhost mode |
| `network.{tor,i2p,lokinet}.enabled` | `false` | bool | H | Enabled route requires a tested adapter and proxy/router settings |
| `network.{tor,i2p,lokinet}.proxy` | absent | URL/reference | C | No invented reachable proxy default |
| `network.tls.extra_roots` | `[]` | file paths | C | Strict TLS plus optional custom CAs |
| `network.max_connections` | `128` | connections/worker | H | Validated against process FD budget with reserve |
| `network.endpoint_connections` | `1` | connections/scope | H | Can increase explicitly for demonstrated relay needs |
| `network.host_connections` | `4` | connections/hostname | H | Applies across services; lower global limit still wins |
| `network.host_request_rate` | `2` | starts/s | H | Token bucket, burst 4; not a byte budget |
| `network.connect_timeout` | `10s` | duration | C | Per attempt; overlay default 30s |
| `network.http_redirects` | `3` | redirects | C | Guard each target; no HTTPS→HTTP downgrade |
| `network.websocket_compression` | `disabled` | enum | C | Enable only with tested incremental inflation limits |
| `ingest.max_event_bytes` | `256KiB` | bytes | E | Policy exclusion at larger size; counted and visible in coverage |
| `network.max_message_bytes` | `2MiB` | bytes | C | Includes framing, checked before full allocation |
| `network.max_inflated_bytes` | `2MiB` | bytes | C | Absolute websocket decompression output bound if compression enabled; HTTP artifact sources have their own finite output bounds |
| `network.max_inflate_ratio` | `10` | ratio | C | Additional limit; absolute limit remains mandatory |
| `ingest.producer_queue_bytes` | `32MiB` | bytes/producer | H | Global memory budget also applies |
| `ingest.producer_queue_items` | `1024` | records/producer | H | Both limits apply; batches do not bypass either |
| `relay_policy.banned` | `[]` | endpoint/host rules | H | Versioned; in-flight work cancelled when newly banned |
| `relay_policy.self_endpoints` | generated plus `[]` | endpoint ids | H | Synchronizer excludes; Monitor may measure them |
| `auth.bindings` | `[]` | endpoint/signer/scope | C/E | Changed scope creates fresh evidence identity |
| `observer.id` | generated/persisted UUID | identity | R | Never an all-zero substitute pubkey |

These defaults are per worker. Distributed lease ownership and budget allocation must cap aggregate connections; starting four workers must not silently quadruple an operator's declared global budget. Total queue memory is bounded by a process-wide admission allocator, not the sum of independently preallocated maxima. Journal group-commit, durable partition quotas and retention are storage controls shared by all producers; do not duplicate them under Synchronizer.

## Seeder contract

User entry points: `bigbrotr seed` imports the bundled snapshot once; `bigbrotr seed --file PATH` imports an operator file; `bigbrotr seed --enroll` also records operator enrollment. The full local recipe performs bundled seeding once when initializing an empty registry, unless `bootstrap.seed=false` is explicitly selected. A durable bootstrap job receipt makes restart idempotent. File path and content digest appear in the run report, not in a permanent periodic polling loop.

Read bounded lines, preserve source/line number, ignore blanks and comments, normalize identities, and idempotently add evidence. Do not do DNS or capability probes in Seeder. IP literals that are out of policy can be recorded as excluded without scheduling a dial. Unresolvable hostnames remain candidates for Validator. `--enroll` grants scheduling permission subject to egress and bans; it creates `probe_state=unverified`, never `reachable=true`. A later successful attributable protocol result establishes reachability.

Missing explicit file or unreadable bytes fail the job before applying its input; an empty/comment-only explicit file fails with an actionable message. In a mixed file, malformed entries are counted and skipped; a file with zero admissible candidates finishes with an explicit no-admissible-input result rather than an apparently successful bootstrap. Re-running the same file adds no duplicate evidence. A newly named file does not re-enroll endpoints the operator blocked.

| Key | Default | Unit | Reload | Contract |
|---|---|---|---|---|
| `seed.file` | bundled versioned list | path/resource | J | User `--file` overrides |
| `seed.enroll` | `false` | bool | J | Replaces misleading direct-valid-relay insertion |
| `seed.assumed_scheme` | `wss` | enum | J | Only operator-supplied scheme-less lines; recorded |
| `seed.max_line_bytes` | `8192` | bytes | J | Drain/reject oversized line |
| `seed.batch_size` | `500` | candidates | J | Atomic batch evidence writes |
| `relay_policy.candidate_validation` | `public` | enum | H | `enrolled_only` or `disabled` restricts candidate probing; all egress/bans still apply |
| `relay_policy.auto_enroll_validated_public` | `true` in full recipe | bool | H | Enroll only after actual attributable public protocol/read success; private routes require explicit enrollment |

The full recipe grants Validator permission to test public candidates under the guarded transport, then automatically enrolls a candidate only after actual public read success. Seeder alone never proves reachability. Explicit bans and egress policy dominate this transition. Private/overlay credentials and private fixture routes need their own explicit scoped enrollment; the deterministic local acceptance recipe does not bypass public-route policy.

Security/resource boundary: bounded local file reader plus registry writer, no network credentials. Report read/blank/comment/malformed/duplicate/known/blocked/pending/enrolled counts and durable batch progress. A database failure retries the unchanged batch and does not claim the whole file finished.

## Discoverer contract

The archive leg consumes `DiscoveryInput(stream_id, sequence, event_id, signed_event_reference)` in durable ingestion order. The stream carries accepted immutable source events, including historical events inserted after newer timestamps. Scanning the archive by `created_at` is prohibited as a progress cursor. Replay/restoration uses stream sequence or object/shard manifest ordering and may safely repeat records.

Parse event kinds and full tags from the retained event, not a first-value-only tag index. Extract the useful source set described in research. A multi-`d` malformed NIP-66 event yields its first protocol subject plus a malformed-subject diagnostic; non-URL subjects stay evidence without dial jobs. Every emitted reference has `(stream, sequence, tag_offset, extraction_version)` identity. Distinct markers/polarities are retained even when the normalized endpoint is the same. Unsupported future discovery sources remain representable as raw evidence without running arbitrary format logic.

Each input record has bounded work quanta. If a huge event exceeds the candidate/tag quantum, persist a sub-record offset and resume; do not advance beyond undispatched valid references. Commit candidate/evidence effects and progress atomically in the registry, or make effects idempotent before advancing the checkpoint. Projection retention must retain an unread source or an independently durable discovery outbox. If data has already been pruned, record a discovery coverage gap and require explicit replay scope; do not skip silently.

Directory polling has its own loop and checkpoints per source. Use HTTPS, conditional GET where available, response/redirect/decode bounds, a restricted JSONPath subset (`field`, index, array wildcard only), and no expression evaluation. URLs carry source id and response digest. On 429 honor bounded Retry-After; on other errors back off independently, retaining last successful evidence. Poll failures do not suppress archive discovery. Directory credentials remain secret references. Default external sources are off because bundled seeds provide independent bootstrap; a documented nostr.watch preset preserves the prior capability without making a third party a required default.

Only fresh evidence increments the candidate's evidence revision. Validator can use that revision for one bounded revival; replayed events and repeated unchanged directory responses cannot force endless immediate retries. Retain first/last mention, count and a bounded recent evidence window, with durable source references for detailed history.

| Key | Default | Unit | Reload | Contract |
|---|---|---|---|---|
| `discoverer.events.enabled` | `true` | bool | H | Archive leg independent from directories |
| `discoverer.events.page_size` | `500` | inputs | H | Bounded source fetch |
| `discoverer.events.batch_size` | `500` | references | H | Registry commit grouping |
| `discoverer.events.workers` | `4` | workers | H | Different input partitions; ordered checkpoints per partition |
| `discoverer.events.quantum` | `1000` | tags/input slice | H | Resume remaining offsets |
| `discoverer.events.slice_time` | `30s` | duration | H | Yield fairly; no loss at deadline |
| `discoverer.events.replay_from` | current retained start | sequence scope | J | Explicit replay job, not a live cursor reset |
| `discoverer.api.enabled` | `false` | bool | H | Enabled requires nonempty sources |
| `discoverer.api.sources` | `[]` | typed source definitions | H | Each has stable id, URL and validated selector |
| `discoverer.api.interval` | `24h` | duration | H | Per-source successful poll cadence |
| `discoverer.api.failure_retry` | `15m` | duration | H | Exponential to 24h, 20% jitter |
| `discoverer.api.request_timeout` | `15s` | duration | C | Entire bounded HTTP request |
| `discoverer.api.request_delay` | `1s` | duration | H | Minimum spacing per source host |
| `discoverer.api.max_response_bytes` | `5MiB` | bytes | H | Includes decompressed output cap |
| `discoverer.evidence_recent_limit` | `100` | references/endpoint | H | Historical evidence remains in replayable source records |

Metrics include per-source successful/failed polls, freshness, input lag, oldest pending input, records/tags examined, references emitted, each exclusion reason, deferred offsets and revival requests. Raw endpoint/event ids belong in bounded diagnostic queries, not unbounded metric labels. The operator can inspect why an endpoint was discovered and why it has not yet been probed.

## Validator contract

Claim due candidates with a lease generation. Order by next_due, then least recent attempt, then stable endpoint id; alternate new and retry work so neither starves. Recheck policy before each dial. Establish WebSocket transport, issue a bounded Nostr request using an unguessable nonexistent exact event id, and accept attributable EOSE, a valid verified EVENT, or an applicable Nostr CLOSED/AUTH exchange as protocol evidence. A bare HTTP 101 proves transport only. Auth challenge support is recorded separately from an actual read refusal.

Fetch NIP-11 independently under its own deadline at the same path/query with the HTTP-equivalent scheme. Capture its bounded original body, digest and parse status. Optional capability probes never consume the admission handshake deadline. Promote into enrolled `active` only when there is usable public protocol/read evidence, or explicit enrolled authenticated access; auth-restricted candidates may still be known relays with acquisition blocked on credentials. Persist the admission result, initial capability facts and next schedule transactionally. A control-plane write error is infrastructure failure and does not increment relay failure count.

On unreachable, TLS or protocol failure, store a typed cause and retry. Never turn an operational failure into an operator ban. After the configured dormancy threshold, stop frequent retries but retain identity, compact evidence and a sparse retry. Fresh evidence can accelerate one retry subject to a minimum interval; a successful probe restores base cadence. Operator enrollment and imported identity references survive dormancy. No automatic deletion is required for correctness; retention may prune unreferenced dormant evidence with a manifest of what was pruned.

| Key | Default | Unit | Reload | Contract |
|---|---|---|---|---|
| `validator.enabled` | `true` | bool | H | Data prerequisites rather than service-name dependencies |
| `validator.claim_batch` | `100` | candidates | H | Claimed work does not imply simultaneous dialing |
| `validator.concurrency` | `20` | probes | H | Shared network budget also limits |
| `validator.handshake_timeout` | `10s` | duration | C | Clearnet transport+protocol attempt; overlay default 30s |
| `validator.nip11_timeout` | `10s` | duration | C | Separate best-effort request |
| `validator.nip11_max_bytes` | `1MiB` | bytes | H | No unbounded parse allocation |
| `validator.retry_initial` | `1h` | duration | H | First failure retry |
| `validator.retry_factor` | `2` | multiplier | H | Exponential, capped below |
| `validator.retry_max` | `7d` | duration | H | Sparse retries retained |
| `validator.retry_jitter` | `0.2` | fraction | H | In [0,1] |
| `validator.dormant_after` | `10` | consecutive relay failures | H | No deletion; infrastructure errors excluded |
| `validator.revival_min_interval` | `1h` | duration | H | Fresh evidence cannot cause a probe storm |

Acceptance is visible through `bigbrotr run validator --once`, candidate detail and structured run summary: selected, attempted, transport-only, admitted, auth-gated, dormant, excluded and infrastructure-failed. A new operator can diagnose the difference between no DNS, certificate failure, no Nostr reply, missing credentials and a database outage.

## Monitor contract

Monitor owns measurement, not relay admission or signed announcement publication. Two independent schedules share a budget: frequent lightweight liveness and slower catalog refresh. Due-time decay applies after repeated transport failures, capped at a weekly interval, with successful local evidence restoring normal cadence. New discovery mentions are hints for bounded rechecks, not proof of uptime. Fair due ordering and completed-probe counts expose whether configured cadence is actually achievable.

Liveness checks websocket open and a bounded read request. A prompt EOSE on an empty relay is readable; event-based read RTT is absent when no event arrives. Separate `open_ms`, `request_to_eose_ms` and `request_to_first_event_ms` so an empty relay is not down and an EOSE duration is not mislabeled as event latency. A pass updates only fields it measured; it cannot refresh old TLS/NIP-11 values or mark an unexecuted full catalog as complete.

Catalog captures DNS/IP facts, ASN/Geo when datasets exist, TLS chain validation and metadata, endpoint HTTP/NIP-11, liveness and optional feature probes. Reuse DNS/Geo by exact hostname+route+resolution epoch, and TLS facts by hostname+port+selected IP+handshake policy. Endpoint WS/HTTP/NIP-11 checks always use the full endpoint. Persist the context needed to tell IPv4 and IPv6 results apart. Never infer all members of an apex have the same certificate/backend. Failed/unavailable individual checks still produce a useful partial observation.

Check status is `ok`, `failed`, `unsupported`, `blocked_policy`, `auth_required`, `unavailable_dependency`, `not_attempted` or `budget_deferred`. Store measured duration only for executed work. Cap free-form error text and headers; preserve full NIP-11 under its document bound with versioned extraction. NIP-45 demonstrates a count response, not its exactness; NIP-50 records observed behavior/inconclusive rather than universal compliant-search certification. Observer data is append-only; current-state projection chooses each field by its measurement time and source, not by the latest entire row.

Geo uses operator-provided ASN/City files independently. Explicit unreadable configured paths fail startup; absent files mean unavailable dependency. The optional dataset refresher atomically validates and installs new files, maintains provenance/version and preserves last-good files on download failure. It never publishes download credentials. Approximate Geo has accuracy and source age; stale facts carry degraded status. Overlay checks omit public DNS/TLS/Geo when not applicable.

Default monitoring emits no signed Nostr EVENT. Optional active write probes are a separate explicitly enabled probe capability with a dedicated signer reference, endpoint scope and frequency/PoW budget. The selected `monitor-write-v1` probe is kind 1, content `BigBrotr monitor probe v1.`, with one `["bigbrotr-probe", attempt_uuid]` tag and a fresh attempt timestamp; optional NIP-13 nonce work is separately bounded. The complete signed event must stay below 1 KiB and the same persisted bytes are reused after ambiguous sends. It requires an OK acceptance and verifies retrieval by exact id before reporting write+read success. Persist its event id and distinguish acceptance from retrieval. Do not promise deletion of a probe already published to another relay. Auth signing is separately scoped. Announcements remain the NIP-66 publisher's responsibility; that publisher reads durable observation data and reports actual achieved freshness.

| Key | Default | Unit | Reload | Contract |
|---|---|---|---|---|
| `monitor.enabled` | `true` | bool | H | Independent capability |
| `monitor.liveness_interval` | `5m` | duration | H | Due target, not claimed achieved SLA |
| `monitor.catalog_interval` | `6h` | duration | H | Full catalog target |
| `monitor.concurrency` | `32` | endpoint sequences | H | Shared across both schedules |
| `monitor.claim_batch` | `100` | endpoints | H | Fair scheduler page |
| `monitor.per_host_concurrency` | `1` | sequences/hostname | H | Shared global budgets also apply |
| `monitor.host_budget` | `32` | request starts/minute | H | Token bucket; partial checks can resume |
| `monitor.schedule_jitter` | `0.1` | interval fraction | H | Avoid synchronized scans |
| `monitor.check_timeout` | `15s` | duration | C | Overlay default 45s |
| `monitor.retries` | `1` | total attempts | H | Attempts includes initial try |
| `monitor.retry_initial` | `1s` | duration | H | Factor 2, maximum 10s, jitter 0.2 |
| `monitor.failure_backoff_factor` | `2` | multiplier | H | Maximum interval 7d |
| `monitor.failure_max_interval` | `7d` | duration | H | Dormant fleet remains inspectable |
| `monitor.checks` | open/read/dns/tls/http/nip11/nip42/nip45/nip50/nip77/geo | set | H | Geo reports unavailable without files; capability limits above apply |
| `monitor.store_checks` | enabled check set | set | H | Must be subset of computed checks |
| `monitor.nip11_max_bytes` | `1MiB` | bytes | H | Full capped document |
| `monitor.geo.asn_file` | absent | path | C | Independent from City |
| `monitor.geo.city_file` | absent | path | C | Independent from ASN |
| `monitor.geo.max_age` | `30d` | duration | H | Stale retained with annotation |
| `monitor.geo.geohash_precision` | `5` | characters | H | Coarse default; valid 1–12 |
| `monitor.geo.refresh` | `false` | bool | H | Requires explicit supported source/credential config |
| `monitor.geo.max_download_bytes` | `100MiB` | bytes | H | Cap compressed and extracted artifact separately |
| `monitor.active_write.enabled` | `false` | bool | H | Enabling requires signer and endpoint scope |
| `monitor.active_write.signer` | absent | secret reference | C | Dedicated identity |
| `monitor.active_write.interval` | `24h` | duration/endpoint | H | Separate from frequent read checks |
| `monitor.active_write.max_pow` | `20` | difficulty bits | H | Exceeding relay requirement is unavailable policy, not unwritable |
| `monitor.active_write.pow_timeout` | `1s` | CPU/wall budget | H | Dedicated bounded pool; cancel on exhaustion |
| `monitor.raw_retention` | `forever` | duration or forever | H | Executed measurement evidence retained; explicit lossy expiry requires a retention manifest |

Compact aggregate history may remain indefinitely, but a raw-retention policy must also cover its replay log, object documents and backups. Keeping all journal observations forever while deleting a query table does not honor raw retention. Minimal coverage/run audit metadata remains sufficient to explain aggregate intervals. Per-check counters, duration histograms, due backlog, oldest unfinished check, last complete catalog, last liveness, deferred hosts, geo age and protocol result counts are required. Runtime errors in one host group do not end another host's measurement.

Monitor record/document separation, content deduplication, export references and
retention consequences follow the [product data decisions](../product/data-and-feature-decisions.md).
Compact attempt history must retain actual outcomes without copying an unchanged
full NIP-11 document into every liveness record.

## Synchronizer contract

### Scope and scheduling

An acquisition selector is a versioned union of structured Nostr filters, with explicit event-time inclusion policy and ephemeral policy. Selector fields express durable collection intent; request pagination `limit` and moving tail boundaries are not selector identity. Persistent author/id/tag lists use exact supported values. Reject unsupported search-based historical selectors because relevance ordering and server-defined matching cannot support this traversal algorithm. Normalize field/list order and duplicates before hashing; preserve the semantic distinction between absent and empty lists by rejecting empty lists. `since`/`until` explicitly supplied as collection policy remain part of the selector, while generated request windows do not.

Work is scoped by `(endpoint_id, selector_id, auth_scope_id, policy_epoch)`. A scope change starts new evidence; automatic narrowing reuse is an optional optimization requiring a containment proof for unions of conjunctions, not a prerequisite. Ephemeral exclusion is local admission policy and identity. Nostr filters lack a general negative-kind operator; an all-kinds request cannot simply express “all except 20000–29999” without an explicit bounded positive expansion. The implementation must either compile a bounded supported positive filter set or receive-and-drop with an honest wire-policy label. Do not claim that excluded kinds were never requested when the wire uses `{}`.

Schedule bounded work quanta across every eligible endpoint with a fair queue, favoring oldest unserved scopes without starving live/recent work. Maintain optional live subscriptions only for seated endpoints; show disconnected/unserved intervals when the fleet exceeds capacity. A ten-minute rotation is not proof of continuous all-relay live coverage. Historical traversal yields at request/segment boundaries and is preemptible on a work deadline; it cannot occupy a scarce slot forever because a relay has a large history.

### Progress and evidence

There is no scalar global complete flag. Preserve three separate concepts:

- `traversed_until`: exclusive timestamp through which the scheduler has attempted its planned intervals, possibly with recorded gaps.
- Interval evidence: `not_attempted`, `interrupted`, `linear_observed`, `relay_reported_exhaustion`, or `reconciled_served_set`, with query scope, observed times and durable receipt references.
- Unresolved gaps and policy exclusions: saturated boundary, advertised-unserved id, auth limitation, size exclusion, rate/retention restriction, budget deferral, data loss or transport interruption.

`reconciled_served_set` means a completed reconciliation exchange's advertised need set has been accounted for by durable valid bodies/witnesses or explicit policy exclusions. It does not mean immutable relay state or all events ever held by that relay. Existing local-only records are not removed because a remote set omits them. Every evidence class retains last_verified_at and authorization context. “Caught up” may be displayed only as “scheduled traversal reached [time], [method], [known gaps], last verified [time]”. No progress update is possible before corresponding receipts and gap records are durable.

### Layer algorithms

**Live.** Open a subscription with serialized `limit:0` and the collection selector, without an invented `since=now` unless collection policy requires it. An author may submit a backdated event live. Admission verifies every body and records local observation context. Unexpected stored events from a relay ignoring limit remain bounded and are admitted if within policy; flag the behavior. Live data never advances historical traversal.

**Historical linear.** For a finite internal interval `[lo, hi)`, translate to inclusive wire `since=lo, until=hi-1`, checking underflow/overflow. Request the configured limit and wait for attributable EOSE under a request deadline. Verify, scope-check and durably admit returned records. A short page does not finish traversal. Continue with `until=oldest_received_second`, inclusively overlapping the boundary. Track ids within the current request walk, not the global archive seen-set: an event already held elsewhere must not terminate pagination or suppress this relay's witness. Consecutive empty intervals may grow exponentially up to 365 days to avoid a daily request for decades before Nostr existed; return to the configured initial width after finding data. Each actual queried interval still has its own evidence and no inference skips an unqueried range.

If the boundary repeats without exposing new ids or older seconds, record a boundary stall/uncertainty and query the older interval ending at `second-1`; this makes finite progress without claiming the boundary is exhausted. Detect ordering/filter violations and interrupt the affected scope. For explicit finite kinds/authors, subdivide a saturated second into disjoint combinations within a partition budget. An unrestricted author set cannot be exhaustively enumerated from authors already observed. An empty page yields `linear_observed`, not absolute completeness. NIP-67 `finish` can terminate the current query as relay-reported exhaustion; `more` continues; `auth` retains a restricted-scope qualifier even alongside finish. Contradictory finish+more is an anomaly, and missing known ids stay gaps despite finish.

**Historical negentropy.** Use a durable snapshot of locally witnessed `(created_at, event_id)` for this endpoint/auth scope, plus same-scope verified policy suppressions. The snapshot reader reports an ingest watermark and moderation epoch; stale projections cannot be passed off as current. This is a read contract, not a requirement to maintain a permanent second provenance-sized RocksDB index. A sorted analytical projection plus a bounded committed tail, a dedicated index, or bounded reconstruction from durable shards are implementation alternatives to compare by measured reads, writes and memory.

Missing entries in a conservative stale local set cause extra downloads and are safe; fabricated or stale retained-ownership entries are not. Apply current suppression/retention state, and do not infer successful retrieval from a missing/unavailable reader. A bounded committed tail may supplement a lagging reader; if it exceeds its bound, either use a conservative older subset with extra verified fetches or defer reconciliation, recording the actual reader watermark. Unrelated live acquisition continues. With provenance disabled, global body membership is allowed only for a declared body-acquisition objective; it cannot generate local relay-witness claims.

Reconcile bounded time intervals and cap local set memory, frame bytes, rounds and duration. `blocked` starts bounded subdivision; an age restriction or a one-second interval still exceeding limits becomes a gap. The client algorithm's successful processing determines protocol termination, then the client sends NEG-CLOSE to release resources. Download missing ids in bounded exact-id REQs, verify each id/body, and require a real served event to create a new local witness even if that body exists from another relay. Retry absent ids; persist `advertised_unserved` after exhaustion. A remote advertisement without retrieval remains an advertised claim, not a local witness. No upload of local archive events is part of synchronization.

**Recent sweeps.** Revisit hot and warm intervals independently of historical progress. Use the same linear/negentropy algorithms and evidence rules. Later sweeps may supersede earlier gaps only with specific successful evidence. A scope reaching the moving safety lag is eligible for steady scheduling, not permanently complete. A byte budget can postpone a sweep, and the dashboard must show that deferral.

**Deep re-verification.** Stagger full-range reconciliation over time for capable relays; resume at durable interval boundaries after rotation/restart. For linear relays, retry known gaps and perform rotating older interval samples. Sampling is labeled sampling and supplies no full-history verification horizon. A scheduled full linear re-traversal is supported when explicitly budgeted, with the same protocol limitations. Do not automatically shrink the tail from observed signed-age percentiles: backdated/unseen events bias that statistic. A future adaptive policy requires a bounded comparison against fixed windows and must retain a configured minimum plus visible missed/late-event metrics.

NIP-11 limits are optional request-size/subscription-count hints. A false claim never increases local resource limits or establishes coverage. COUNT is never a completion test. Probe failure leaves capability inconclusive; after a longer transport-aware retry, linear fallback may be used temporarily without permanently recording “unsupported”. Signed timestamps outside ordinary calendar ranges retain the complete verified event under the integer representation above, with bounded conversion/quarantine metadata; they cannot wedge indexing or advance arbitrary frontiers. Future-created events are not evidence that historical traversal reached the future.

### Synchronizer configuration

| Key | Default | Unit | Reload | Contract |
|---|---|---|---|---|
| `sync.enabled` | `true` | bool | H | Requires enrolled endpoints or healthy idle |
| `sync.selectors` | `[{}]` | filter union | E | All structured event kinds under retention/admission policy |
| `sync.history_since` | `0` | Unix seconds | E | Collection lower bound; checked range |
| `sync.ephemeral` | `retain` | enum | E | `retain` or `drop`; no fictitious negative wire filter |
| `sync.provenance` | `local_first_witness` | enum | E | Off is explicit loss of the relay-evidence objective |
| `sync.mode` | `auto` | enum | H | `auto`, `linear`, `negentropy`; forced unsupported mode reports blocked |
| `sync.active_endpoints` | `64` | endpoint scopes | H | Global route/connection budget also applies |
| `sync.subscriptions_per_endpoint` | `3` | subscriptions | H | Live + finite acquisition/probe work; hints may reduce |
| `sync.work_quantum` | `2m` | duration | H | Stop at bounded request boundary; persist remaining work |
| `sync.live_enabled` | `true` | bool | H | Disablement recorded in scope scheduling history |
| `sync.live_rotation` | `10m` | duration | H | When eligible scopes wait; no claim of continuous coverage |
| `sync.connection_lifetime` | `24h` | duration | C | Clean rotation, not relay failure |
| `sync.request_timeout` | `30s` | duration | C | Overlay default 90s |
| `sync.idle_timeout` | `5m` | duration | C | Keepalive checks precede reconnect for quiet live relays |
| `sync.linear_limit` | `500` | events/request | H | Never completion denominator |
| `sync.window_width` | `24h` | duration | H | Initial historical interval; adapt by subdivision |
| `sync.partition_budget` | `128` | subqueries/interval slice | H | Persist remaining work, never claim exhausted on limit |
| `sync.negentropy.max_set_bytes` | `64MiB` | bytes/session | H | Exact implementation requires SDK/index experiment |
| `sync.negentropy.max_frame_bytes` | `64KiB` | bytes | C | Wire-encoded bytes; global message cap still applies |
| `sync.negentropy.max_rounds` | `64` | rounds/session | H | Bound full protocol loop |
| `sync.negentropy.max_messages` | `1000` | messages/session | H | Counts both directions |
| `sync.negentropy.need_batch` | `200` | ids/REQ | H | Also bounded by request serialization bytes |
| `sync.negentropy.fetch_attempts` | `3` | total attempts/id | H | Then durable advertised-unserved gap |
| `sync.capability_probe_timeout` | `10s` | duration | C | Overlay 30s; second attempt deadline ×2 |
| `sync.capability_reprobe` | `7d` | duration | H | Earlier on behavior change; inconclusive retries after 1h |
| `sync.hot_window` | `1h` | duration | H | Recent sweep scope |
| `sync.hot_interval` | `15m` | duration | H | Due target, budget deferral visible |
| `sync.warm_window` | `48h` | duration | H | Must cover hot window |
| `sync.warm_interval` | `1h` | duration | H | Potentially expensive linear re-download |
| `sync.safety_lag` | `48h` | duration | H | Scheduling/display qualification, not lateness guarantee |
| `sync.deep_reverify_interval` | `7d` | duration | H | Target spread across available capacity |
| `sync.relay_byte_budget` | absent | bytes/day/endpoint | H | Optional explicit cost ceiling; no silent default exclusion |

Wire bytes must actually be counted by route/endpoint/layer, including duplicates, reconciliation and control traffic. Aggregated metrics use bounded network/layer/reason labels; endpoint drilldowns are stored/queryable. Required diagnostics: oldest unserved endpoint, service interval distribution, traversal evidence by method, unresolved gaps by age/reason, admission/rejection/duplicate counts, new bodies versus new witnesses, bytes/day, protocol CPU, durable-ack latency, queue pressure, clean rotations, retries, active scopes and auth restrictions. Infrastructure errors must not decay relay reputation.

## Importer contract

User entry point: `bigbrotr import --from PATH` creates a finite resumable job. `--dry-run` parses and verifies without archive mutation, reports compatibility and estimated scratch use, and does not claim that a later changed source has been validated. `bigbrotr import status JOB` and `bigbrotr import resume JOB` expose actual progress. A scheduler may later submit explicit jobs; enabling the daemon never discovers arbitrary local files and imports them automatically.

Sources are immutable manifest-defined artifacts. Auto-detection must find exactly one supported format, otherwise require `--format`. Accept canonical JSONL with optional provenance sidecar, versioned native archive segments/manifests, and the explicitly pinned Pensieve adapter specified in [import formats](../specification/import-formats.md). A newer native format is refused before writes. Protobuf parsing honors the pinned schema and field-wire rules; incompatible producer revisions require another adapter rather than silently reinterpreting bytes. Third-party manifestless sources must declare their shape and origin. Exact source digest/size/shard identities and policy/parser versions bind resume; changed input fails resume instead of trusting byte offsets.

Preflight verifies manifest checksums, source shape, selected scope, required retention/fidelity capabilities and available scratch quota. Verify signatures and ids for every event regardless of source reputation. Unsigned or irreconstructable slim records are rejected from the canonical importer; there is no verification-disable knob or untrusted-mark workaround. Analytical projections may be opened by separate analysis tooling, but cannot be restored as original Nostr events. Source shape is data about available fields, never inferred from empty content or absent edges in a sample. The historical heavy-to-slim compatibility matrix is retired for canonical imports: every canonical destination retains the complete signed event.

Collection selector intersection is explicit and independent of Synchronizer's current moving request windows. The importer applies the durable selector's meaningful bounds; a separate job can intentionally choose another registered import scope. An empty selected filter registry fails unless the operator explicitly supplies an unfiltered scope. Imported data does not change any network traversal, freshness or coverage record.

With provenance enabled, preserve supplied provenance as imported claims. Unknown endpoint identities are created in a non-enrolled state; unreachable relays do not make a valid historical claim unrepresentable. Missing provenance is represented as a dataset source, not a fictional relay. The default `import.claims=enabled-families` preserves only enabled evidence families and reports excluded ones. `require-all` refuses a supplied family disabled at the destination; it does not require every event to have a relay claim. `none` imports complete signed originals without interpreted sidecars. Provenance off excludes event-to-relay claims and intentional per-event availability measurements, including alternate import paths. Signed events do not cryptographically prove the sidecar's relay provenance or timestamps.

Large joins use a bounded disk-backed staging index or external sort by event id, not a corpus-sized RAM map. Build/verify sidecar partitions, detect missing and orphan references, join in stable order, and checkpoint each completed phase behind durability. Input order does not decide whether an edge is lost. On RAM pressure spill; on scratch quota exhaustion pause with required/free bytes. Admission uses the same batch/idempotency/receipt contract as live acquisition. Native input segments are decoded into logical records unless an explicitly compatible attach operation validates formats, policies and ownership; copying arbitrary journal files into a live writer directory is not import.

The destination admission authority always enforces its current moderation policy.
Research imports do not restore another deployment's administrative control state.
NIP-09 kind-5 requests and their targets remain full signed archival events,
independent of file order; serving deletion semantics are derived later. Selected
research formats contain no administrative source-tombstone control family and
have no source-tombstone-ignore option. An unknown mandatory control-bearing
format fails preflight; a deployment backup must use P08 restore with its complete
control snapshot before writers are activated. Exported visibility/loss manifests
qualify their selected data; they do not impose a global destination ban.

Default malformed/oversized JSONL records are rejected with counts and a bounded reject artifact; corrupt native framing/checksums halt the affected shard because safe record boundaries are not assured. Forged ids cannot poison dedupe. A job can finish `completed_with_rejections` with a non-success warning exit class and machine-readable counters; neither zero imported records nor completed byte reading alone counts as a successful compatible import. Replaying after a crash may increase attempted counters but not logical imported counts.

| Key | Default | Unit | Reload | Contract |
|---|---|---|---|---|
| `import.format` | `auto` | enum | J | Ambiguity refused |
| `import.scope` | active durable collection selector | selector id | J | Explicit unfiltered override supported |
| `import.source_shape` | manifest | descriptor | J | Required declaration when no manifest; conflicts refused |
| `import.batch_records` | `1000` | records | J | Also capped at producer queue bytes |
| `import.max_record_bytes` | `16MiB` | bytes | J | Pre-decode limit; record formats define safe rejection behavior |
| `import.max_event_bytes` | shared ingest cap | bytes | J | Cannot increase beyond target policy |
| `import.workers` | `4` | parser/verifier tasks | J | Global CPU/memory and ingest fairness apply |
| `import.memory_bytes` | `256MiB` | bytes/job | J | Includes join/sort and bounded verification queues |
| `import.scratch_bytes` | `20GiB` | bytes/job | J | Finite staging quota; pause with diagnostics |
| `import.claims` | `enabled-families` | enum | J | `require-all` refuses disabled supplied families; `none` omits interpreted sidecars; provenance off excludes event→relay claims |
| `import.reject_policy` | `continue_records` | enum | J | Native framing corruption still stops shard |
| `import.reject_artifact_bytes` | `64MiB` | bytes/job | J | Counts continue after sample cap; payload redacted by default |
| `import.checkpoint_interval` | `5s` | duration | J | Checkpoint only durable whole-record/shard boundaries |

Progress includes phase, verified source bytes, processed logical records, newly durable bodies, existing bodies, claims, rejected/filtered/blocked/unsigned counts, orphan edges, scratch use, current throughput and durable checkpoint age. Log samples identify source position and claimed id without dumping hostile content. Completed jobs retain provenance and summary; staging cleanup uses job ownership and must never delete source artifacts.

## Practical acceptance scenarios

These are implementation acceptance scenarios to execute through the packaged binary, operator commands, dashboard/API views and realistic fault fixtures. They are not tests reported as passing today. Each records the config, source/relay fixture version, command, expected user-visible behavior and resource evidence.

1. **Fresh bootstrap with no internet directory.** Boot an empty fixture deployment, run bundled/file seeding, then validation and sync. Valid seed endpoints become probeable and acquired; directory outage does not block bootstrap. Re-run seeding: no duplicate evidence/enrollment. Missing and all-malformed explicit files give distinct actionable outcomes.
2. **Identity conservation.** Seed scheme-, port-, query- and path-distinct fixtures serving different events. Each retains separate provenance; true syntax equivalents deduplicate. A ban or self-exclusion is applied through the same normalization authority, including mapped IPv4 and IDNA. Query credentials do not appear in public output/logs.
3. **Egress enforcement at the socket.** Exercise direct HTTP/WS/TLS, redirects, DNS rebinding, mixed public/private answers and IPv6. Record accepted socket destinations; no denied destination is connected. A specifically allowed loopback fixture works while arbitrary private inputs remain blocked. Verify custom CA trust without disabling hostname checks.
4. **Overlay isolation.** Route Tor and I2P through distinct controlled adapters, stop one, and inspect status. No public DNS lookup occurs for overlay names; missing route is a dependency failure, not relay downtime. Public acquisition retains service capacity. The test requires actual adapter execution before overlay support is advertised.
5. **Delayed discovery and large tags.** Admit an old-signed event after a new one and an event with more references than one quantum. Observe every in-policy reference eventually appear, with provenance offsets and bounded memory. Restart mid-event and replay input; no references disappear or multiply. Prune a required unread source deliberately and verify an explicit discovery gap.
6. **Directory failure and evidence storms.** Exercise invalid selectors, bounded huge bodies, 429, redirects and repeated identical lists. Invalid config fails before polling; source failures back off independently. Identical evidence cannot continually revive a dead host. Negative relay-list evidence does not become a local ban.
7. **Admission semantics.** A generic WebSocket server is transport-only; a Nostr relay with no stored events and prompt EOSE is usable; a relay without NIP-11 is admissible; an auth challenge without read refusal is not globally private. Kill the control store during promotion and verify no false relay failure or half-admission claim.
8. **Dormancy and revival.** A fixture fails through its threshold, becomes dormant without deleting identity/provenance, and later succeeds or receives fresh evidence. Sparse retries and minimum revival spacing hold; successful local evidence restores cadence. Replayed evidence causes no extra revival.
9. **Truthful monitoring.** Three paths and two ports on one host expose different behavior/certificates. Shared facts are reused only within their valid context; per-endpoint checks stay distinct. An empty relay has readable=true and absent event RTT. Disable Geo or make its dataset stale: unavailable/degraded values appear without fabricated location. Fast liveness preserves prior catalog measurement age.
10. **Bounded monitoring and publication separation.** Exercise a large dead/shared-host fleet. Both schedules remain within resource limits with visible deferrals. Passive mode emits zero signed EVENT frames. Enabled active probes use only the declared signer/scope and distinguish accepted/retrievable/PoW-budget-exceeded; NIP-66 publication cadence uses real durable measurement timestamps.
11. **Capped linear relay.** Request 500 from a relay clamping to 100 over multiple timestamps; all fixture events that pagination can expose are durably admitted. A page of entirely globally known events does not terminate the walk and still creates this relay's witness. Inspect result: linear evidence, no absolute complete label.
12. **Same-second saturation and dishonest finish.** Put more than the cap in one second and return the same subset repeatedly. The worker records a stall, visits older ranges, stays finite and never marks the saturated second verified. Repeat with finite author/kind subdivision and with NIP-67 more/finish/auth combinations. A finish contradicting known missing ids remains a gap.
13. **Negentropy with changing/unserved sets.** Compare real pinned relay implementations against a fixture with a known set; exercise result-size caps and one-second saturation. If a relay advertises an id but withholds its body, bounded retries yield an advertised-unserved gap. NEG-CLOSE alone yields no success. A newly served event below previous traversal is found by a later re-verification and retains evidence timestamps.
14. **Local witness versus imported claim.** Hold E from X, import a claim for Y, then sync Y. Reconciliation still obtains a verifiable Y delivery before creating a local witness. Authenticated and public Y sessions retain separate scope. An unknown Y import creates identity without automatic enrollment.
15. **Durability and fencing.** Kill producers/append authority before and after append, fsync/group commit, dedupe publication, receipt and progress update. On restart, progress never cites unavailable acknowledged records under the configured durability class; replay creates no false ownership. Lose a lease mid-request: stale progress writes fail while newer work proceeds.
16. **Indexer down and disk pressure.** Stop analytics/indexing, then sustain network acquisition and import until finite spool thresholds. Show independent service operation and explicit backpressure; no unbounded memory/disk growth. ENOSPC and fsync failures stop acknowledgments and freeze affected progress. Restoring capacity resumes from receipts without claiming lost live intervals were observed.
17. **Fairness at realistic fleet size.** Use more eligible scopes than available connections, including one huge historical relay. Every eligible scope gets bounded work; backlog/service interval measurements explain unmet cadence. Record actual wire bytes, CPU, memory, FD count, disk writes and replay lag. No ≥5k events/s or fleet-size promise follows until those measurements justify it.
18. **Import/export round trip.** Export and import native and JSONL artifacts with full signed bytes, unknown relay claims, timestamps, signed deletion requests/targets and repeated bodies. Compare logical event/claim sets and specified byte fidelity, with network traversal unchanged. A slim artifact into a fidelity-requiring canonical target fails preflight; unsigned input cannot enter verified serving.
19. **Import crash and scale.** Use a corpus larger than RAM with provenance out of order. Stop during sidecar staging, verification and admission; resume from stable source identity with bounded spill. Modify a source shard and verify resume refusal. Scratch exhaustion pauses the job with exact diagnostics; source files survive cleanup.
20. **Moderation during import.** Apply a local takedown while a long import is running. Subsequent admission honors the new policy revision; blocked ids are not restored from the source. Kind-5 originals do not become administrative import exclusions; deployment controls use restore. A forged id cannot suppress a later authentic event.
21. **Hostile inputs and retention.** Exercise oversized lines, malformed JSON/protobuf/native frames, giant negentropy lists, malformed EOSE hints, decompression bombs, far-future/negative/out-of-range signed times and slow sockets. The binary remains bounded and records precise outcomes. After configured retention and replay, expired observation payloads do not reappear from an older journal while permitted aggregates remain explainable.

The SDK must be evaluated against these entry-point scenarios, especially custom guarded dialing, `limit:0` serialization, NIP-67 hints, per-id retrieval evidence, timestamp range, cancellation, exact event verification and bounded negentropy storage. Coordinating source inspection already found that `nostr` 0.45.4 at `5c669a498e5f8dd1ec5713b11fdb92bcd858ad73` drops EOSE hint arrays in its relay-message enum; a bounded envelope must preserve them before SDK conversion. A convenience method that hides required evidence is replaced by a lower-level adapter; the protocol itself is not rewritten merely to preserve an SDK abstraction.

## Clarifications from the complete historical reread

Shared lifecycle states and finite-job outcomes are defined in
[configuration/runtime](../engineering/configuration-and-runtime.md). Service
protocol phases such as probing or backfill are nested progress, not alternative
health enums. The full local recipe has a deterministic bootstrap; an empty
registry cannot silently satisfy its first-launch acceptance through healthy idle.

Time intervals use a typed exclusive upper bound, `Finite(u64)` or `AfterMax`,
rather than overflowing `u64::MAX + 1`. Convert a nonempty half-open range to
NIP-01's inclusive upper timestamp with checked arithmetic: finite upper minus
one, or u64::MAX for AfterMax. Empty/inverted ranges and lower-zero boundaries
have explicit cases. NIP-77's reserved maximum timestamp is a protocol limitation,
not grounds to discard a valid archived event; select another retrieval path and
record the reconciliation coverage limit.

The old zero-blocked-fetch claim is achievable for exact-ID requests under our
control: suppress a known denied ID from the need list. Ordinary NIP-01 range
queries cannot exclude arbitrary event IDs, so linear/live remote delivery of
blocked bodies cannot be promised to be zero. Verify bounded rejection work, no
retained prohibited body, no false local witness and explicit policy exclusions.
Do not mistake unavoidable wire redelivery for a failure of exact-ID suppression.

### Paid relays and measurement retention

A NIP-11 payment declaration is a claim about policy, not proof that every read
is denied. Continue permitted public reads under normal budgets; distinguish an
actual payment-required refusal from AUTH, protocol failure and transport failure.
No acquisition worker purchases access, pays an invoice or creates a signing
identity automatically. An operator-provisioned authorization binding can enable
access under its explicit scope; the resulting evidence remains scoped.

`monitor.aggregate_retention=none` retains compact completed historical aggregates
indefinitely by default, with their bucket resolution, denominator, method and
source-retention coverage. `monitor.raw_retention=forever` is separate. Reconfiguration
cannot promise recomputation at a resolution whose source detail has expired.
Reference/pin rules cover original documents, snapshots and backups.

When `monitor.geo.refresh=true`, an explicit `monitor.geo.sources` entry per
ASN/City dataset supplies supported HTTPS source, protected credential reference
if needed, archive format and expected database product/version metadata. The
initial database reader supports MaxMind DB (`mmdb`); accepted compressed
containers are an explicit bounded allowlist, not arbitrary extraction commands.
`monitor.geo.refresh_interval=24h` checks source freshness; download/install occurs
only when required by configured version/age/change evidence. Disabled refresh
needs no URL or credential. Format/path changes drain readers and atomically
replace a validated file, preserving last-good content and its version.

Required practical fixtures: a hostname with both A and AAAA where one family
fails and the other succeeds; same hostname with different selected IP/TLS
results; corrupt, truncated, oversized and wrong-product Geo archives; interruption
between temporary download/validation/atomic install; expired data with an
unavailable refresh source. Verify actual per-family evidence, guarded dials,
last-good reads and explicit stale/unavailable status rather than only filenames.

## Refinements from the Go implementation walkthrough

The [code walkthrough](../research/go-acquisition-code-walkthrough.md) traces
the earlier scheduling, transport and admission path; these are selected Rust
contracts, not instructions to patch that checkout.

Reserve connection, buffer and handshake permits before allocating or dialing.
Historical work has a finite scheduling quantum and can be preempted after
receipt-backed progress even when backfill has never completed; an endless
remote history cannot monopolize a rotating seat. Preserve separate live/control
reservations. Index-lag throttling of new starts and draining already active work
are distinct policies; spool exhaustion invokes the documented stronger stop.
Cancellation, another owner holding a lease and failed remote attempts map to
different typed outcomes and retry/backoff accounting through the actual caller.

Linear-page progress is based on request-local returned IDs and boundary evidence,
not whether the global archive gained a new body. A relay returning globally
known events may still hold unseen older data. Exact-ID fetches validate the
complete requested selector and membership; unsolicited IDs cannot resolve any
need-list entry. Typed admission results distinguish a verified received/already
held body, explicit policy exclusion, invalid input and retryable failure. A
boolean new-body result cannot substitute for that decision or a durable receipt.

Keep EVENT/EOSE wire order, then join all preceding verification/admission work
before committing the page outcome. Track dropped/invalid/over-cap frames in
the affected request's evidence; EOSE cannot turn omitted required data into a
clean result. If a malformed frame cannot be attributed safely, conservatively
invalidate affected active coverage attempts. Record rejection, exclusion and
inconclusive coverage separately rather than store the hostile body.

A known event ID never authenticates a new endpoint's delivery: verify its actual
signed payload/signature or use the precisely bound verification cache. Test a
known valid event followed by the same payload with an invalid, well-shaped
signature from another endpoint; it must not produce a witness. Also retain the
changed-content/claimed-ID sequence and an alternative valid signature case.

Probe evidence records timeout, attributable rejection or demonstrated support
independently of the next chosen acquisition algorithm. Falling back to linear
queries does not persist a timeout as unsupported NIP-77. A NIP-42 challenge alone
also does not prove that public reads fail. All layers share one authoritative
route decision for mixed DNS answers and use the actual guarded transport.

Use unique per-connection request generations, and test stale close against a
replacement at the actual wire boundary. An old local handle cannot emit CLOSE
or NEG-CLOSE that closes its replacement. Inspect wire frames, pinned-IP/SNI and
SOCKS address types; local registry state alone is insufficient. Include an
oversized EVENT followed by EOSE, out-of-scope responses, backdated live events
and starvation under endless historical load in the real socket acceptance.

Account for lifetime host/backoff metadata, local reconciliation vectors, encoded
ID forms, SDK result queues and retry maps as well as sockets. Sparse time
windows can still contain dense same-second sets. Spill/subdivide within the
common byte budget or record deferred/gap evidence; do not materialize several
full set representations merely because each layer has its own item limit.
Explicit proxy credentials must be consumed through a supported protected
adapter or rejected during validation, never accepted and ignored.

## Selected availability and issuer-evidence producers

P04-T13 owns finite, explicitly requested `event-availability/1` Monitor jobs.
It creates A05 pair observations from actual exact-ID requests with a declared
selection, bounded work and proven response ordering. Periodic submission is off
by default; provenance is required. P05-T08 and the availability pages consume
this producer. First witnesses or operational retry logs cannot replace it.
The [task contract](../implementation/04-relay-lifecycle.md#p04-t13--explicit-finite-event-availability-observations)
owns its starting limits and real socket-to-browser evidence.

P04-T14 owns guarded LNURL issuer-metadata acquisition for selected zap/profile
subjects. It stores actual observed time, recipient/source binding and A06 bytes;
analytics never performs HTTP resolution. A present key match is not historical
issuer authority. The [task contract](../implementation/04-relay-lifecycle.md#p04-t14--acquire-attributed-lnurl-issuer-metadata)
owns demand coalescing, bounds, source trust and unresolved-history behavior.

## Pooled transport evidence and destination fairness

A liveness request on a pooled socket measures its actual request/read behavior.
Its TCP/TLS/WebSocket-open fields reference the original connection-generation
observation and original timestamps; reuse never creates a new handshake sample.
An explicitly due fresh-connect check waits for the same endpoint permit or
coordinates a bounded live rotation with an exposed coverage gap. No compulsory
reconnect happens on every liveness tick. P04-T02/T07/T08 test two ticks on one
socket while the peer changes the certificate offered on subsequent connections.

After guarded resolution, all names and roles on a host share a selected-IP
bucket: `network.address_connections=8`,
`network.address_start_rate=4/s`, burst 8 as initial hot controls. Reserve before
connecting, retain the concurrency permit for the actual socket lifetime and
apply the lower endpoint/hostname/global grants too. Lowering the limit stops
new starts and drains without dropping acknowledged work. Scoped operator
address overrides support legitimate large shared hosts. Distinct endpoint
identities, Host/SNI and evidence remain separate. Remote-resolved overlays use
their route/endpoint limits when the final destination IP is unknown; the proxy
address is not falsely treated as every relay's destination. Split hosts initially
have host-local quotas, not a claimed globally coordinated IP bucket.
In-process roles use one network-budget owner. Multiple processes on the same
host acquire/release these permits through the existing private Control adapter;
a separate per-process bucket cannot be advertised as the shared host limit.
Retain a grant until actual socket close or positive process exclusion. After
budget-owner loss, reconcile live holdings or drain/exclude workers before
reissuing capacity; a timeout alone cannot forget a still-open socket. P01-T05/T07
and P04-T02 own this host-local contract, with disconnected-owner recovery tested.
P04-T02/P03-T07 test hundreds of allowed hostname aliases to one address plus an
unrelated peer; actual connections must obey the shared address bound and fair
rotation while the unrelated peer progresses. Token maps stay bounded.

P04-T11 owns `LocalMonitorView`, derived by the shared Indexer from local admitted
measurements. P05-T06 owns `ResolvedRelayEvidence`, a separately versioned
cross-observer/imported-claim result. Reuse pure field/eligibility reducers when
meanings match, without a universal aggregation framework. Local NIP-66 publication
reads LocalMonitorView; public analytical APIs name the local/resolved origin.
Disabling claim resolution cannot block local measurement publication. A local
timeout plus imported success claims must keep those two outputs distinguishable.
