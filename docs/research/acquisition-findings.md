# Acquisition and relay lifecycle findings

Research date: 2026-09-11. This is source inspection and design analysis, not a report of executed product tests. The proposed replacement contracts are in [Acquisition services](../architecture/acquisition-services.md). Historical decisions below are evidence of intent, not constraints on the new implementation.

## Read coverage and evidence boundaries

The historical documentation was read from the frozen `.research/source-snapshots/go/` tree. Its captured Go revision is `757a8d899aefb277c0211077bc30ba0c8de68796`; per-file digests and original paths are recorded in [source-inventory.json](source-inventory.json). Paths in the following table are relative to that snapshot. All service pages and both phase READMEs were read in full, including their amendments and historical contracts.

| Source | Coverage | Main evidence |
|---|---|---|
| `docs/services/seeder.md` | All 72 lines | Bootstrap, normalization, direct-admission option, counters |
| `docs/services/discoverer.md` | All 85 lines | Event/API legs, candidate gate, fan-out loss, future-event handling |
| `docs/services/validator.md` | All 90 lines | Promotion, capability hints, retries, unsupported TLS switch |
| `docs/services/monitor.md` | All 109 lines | Separate liveness/catalog work, durable observations, missing signer/geo refresh |
| `docs/services/synchronizer.md` | All 156 lines | Layers, re-verification, filter identities, backpressure, seal cadence, unmeasured bandwidth |
| `docs/services/importer.md` | All 237 lines | Real CLI, three formats, compatibility, two-pass edge join, resume, moderation limitations |
| `docs/superpowers/plans/bigbrotr/phase-3-ingest-path/README.md` | Full file, lines 1–461 | Acquisition intent, durability ordering, historical package/DDL contracts and contradictions |
| `docs/superpowers/plans/bigbrotr/phase-4-relay-lifecycle/README.md` | Full file | Bootstrap chain, monitor catalog, decay, identity and transport rules, amendments |
| `docs/spec/BIGBROTR_VISION_DECISIONS_2026-06-29.md` | Lines 270–501 | Area C acquisition, Area D catalog and dependencies |
| Same vision document | Lines 613–643 | Area R identity/networking |
| Same vision document | Lines 2238–2510 | Full II.9.1–II.9.6 service specifications, check catalog and acceptance scenarios |

Additional read-only inspection used the neighboring live Go tree, so these observations must not be mistaken for the frozen revision. The tree has `internal/seeder`, `internal/discoverer`, and `internal/validator`; older pages still refer to `internal/discovery` and `admission`. Fully read: `internal/config/{seeder,discoverer,validator,monitor,synchronizer,importer,store}.go`, `internal/discoverer/{gate,chscan}.go`, `internal/validator/lifecycle.go`, and `pkg/transport/dialer.go`. Targeted inspection: `internal/discoverer/candidates.go` source builders and candidate fan-out logic; `pkg/relayurl/relayurl.go` canonicalization, host/path normalization, recomposition and network classification; `internal/validator/validator.go:377–510` admission sequence. Symbol searches in `internal/seeder/service.go`, `internal/discoverer/scan.go`, and `pkg/transport/guard.go` establish entry-point locations only, not full behavioral review. No Go file was modified.

Primary protocol material was consulted live on 2026-09-11. NIPs remain drafts and are not immutable pins. Before implementation, pin the chosen SDK and protocol corpus and rerun conformance scenarios against them.

## Useful product behavior to preserve

### Seeder

The bootstrap source is a bundled curated list, supplemented by an operator file. It remains usable when all third-party directories are unavailable. Parse blanks and comments, normalize identities once, deduplicate, preserve source evidence, count malformed/policy-excluded inputs, and make repeated runs idempotent. The historical `seed.to_validate=false` bypass is useful as operator enrollment, but it must not manufacture a successful probe. Distinguish enrollment from observed reachability. A specifically configured missing file is an error; an archive already populated by imports or operator enrollment does not require a bundled seed cycle to remain healthy.

The historical spec asks Seeder to resolve addresses while also claiming it performs no network I/O. The live Seeder has DNS resolution concurrency. Candidate creation need not prove reachability: admission and every actual connection must independently enforce egress policy.

### Discoverer

Preserve independent archive and external-directory sources. Relay references include NIP-65 `r` tags with read/write markers; NIP-66 `d` subjects and advertised capability evidence; relay-list tags; direct websocket `r`/`relay` tags; and optional relay hints in `e`/`p`/`a` tags. Preserve event id, author, kind, tag position, marker, source directory, raw spelling and observation time. A user's blocked-relay list supplies negative evidence about that author's preference; it is not an instruction to ban the relay locally. Private encrypted list contents remain opaque.

Current production archive queries read only direct `r` tags plus selected list kinds; the richer generic builder understands extra relay hints that those queries cannot supply. The new plan should intentionally carry the full useful source set. NIP-65 markers describe user relay preferences, not proven capabilities of a server. [NIP-65](https://github.com/nostr-protocol/nips/blob/master/65.md), [NIP-51](https://github.com/nostr-protocol/nips/blob/master/51.md).

Directory data stays untrusted. Poll failures should affect the failed source only; retain source-specific cooldown, timeout, response-size, conditional-request and extraction information. Extraction expressions must be a bounded data selector, not executable code. Known endpoints accumulate evidence without resetting validation failures on every replay.

### Validator

Preserve a due-candidate queue, bounded concurrent guarded probes, best-effort NIP-11 capture, explicit promotion, retry timing and durable failure reasons. Reachability, supported protocol behavior, authorization requirements, operator enrollment and network eligibility are separate facts. A WebSocket upgrade alone can be a generic web server; require an attributable Nostr protocol response before classifying it as a usable relay. Empty-query EOSE is a successful readable endpoint; no content is not proof of failure. NIP-11 absence must not block admission. Its document is optional and permits unknown fields, so retain capped raw content and treat declarations as hints. [NIP-11](https://github.com/nostr-protocol/nips/blob/master/11.md).

The live lifecycle applies a fixed one-hour retry, not the exponential policy suggested by older prose. Exhaustion can delete candidates and lose future rediscovery context. Promotion and measured capability persistence are separate live writes; the source contains recovery for partial promotion. A transactional admission result with a retained dormant identity is simpler in the fresh design.

### Monitor

Keep fast connection/read liveness separate from a slower rich catalog. Store each check's result and timestamp; missing, unsupported, blocked, failed and not attempted are distinct. Preserve the previous catalog result when a fast pass updates only liveness. Preserve durable observation history, latest-state mirrors, full capped NIP-11 documents, probe attribution, complete/partial pass envelopes, staleness, host budgets, retry limits, and independent scheduling.

The useful measurement catalog includes DNS A/AAAA/CNAME/PTR/NS/TTL, IP/ASN/organization, approximate geographic facts and database age, TLS certificate/expiry/issuer/version/cipher/SAN/fingerprint, WS open/read timing, NIP-11 and HTTP headers, authentication/payment declarations, COUNT/search/negentropy behavior and reachability. Optional active writes require a signer and re-fetch verification. TLS observations are specific to hostname, port, selected address and handshake, not reusable across every subdomain on an apex. DNS and Geo cannot honestly be measured by substituting the overlay proxy's location.

The source's original “publishes nothing” rule conflicts with its write-test catalog: a test EVENT is a publication. The live page resolves this by reporting `probe_unavailable` because no signer is wired. The new plan must state exactly which component may emit probe events and keep announcement publishers separate. Another historical conflict is “refresh stale GeoLite files” versus the live no-refresher path; the replacement must define the actual refresh owner.

### Synchronizer

Preserve live subscriptions, historical backfill, repeated recent sweeps, historical re-verification, per-endpoint/filter progress, first-observation provenance, blocked-content suppression, signature/id validation, fair scheduling, finite resources, retries and fail-closed durability. Imported records and live events never themselves prove that a historical range was traversed. New filters need new evidence; filter narrowing may reuse evidence only when semantic containment is proven.

First body deduplication must not discard a new relay observation. Reconciliation uses `(created_at, event_id)`, not first-seen time, and local relay membership must be separable from global body ownership. A relay saying it has an event without serving its verifiable body is a persisted acquisition gap. Backdated events, deletions, replacement policies, authentication scope and relay mutation qualify every coverage statement.

The source correctly documents that its wire-bytes metric is always zero. It therefore provides no measured basis for an adaptive bandwidth policy. The proposal retains explicit windows and measured budgeting first; automatic reduction is a separately evaluated policy because observed lateness is biased by what was successfully acquired. The inherited ≥5,000 events/s number is a historical target, not evidence or a new release threshold.

### Importer

Preserve an operator-invoked resumable job, machine-readable and human progress, format auto-detection with ambiguity refusal, manifests, event and record bounds, immutable source identity, shared admission, filter intersection, full signed-event verification, source provenance timestamps and summaries. Native segments, JSONL events/provenance and the pinned pensieve protobuf shape are useful compatibility routes. Source and destination shapes must be explicit: absent original payload cannot be reconstructed.

The live importer exposes useful lessons: unsigned mode is refused because trust cannot be stored; unknown relays cause provenance/body rejection; the entire provenance index is held in RAM behind a ten-million-edge ceiling; one unfinished corpus blocks another; local moderation is loaded once, and source tombstones are counted but not enforced. None of those implementation limits is a product requirement. Prefer durable spill joins, independent jobs, continuously fenced local moderation, imported claims with source attribution, and explicit source-tombstone semantics. Imported data must never update network traversal evidence.

## Protocol findings that change the plan

1. **No finite proof of all historical Nostr events.** NIP-01 has inclusive time filters, capped initial results, replaceable-event behavior, and a live/stored boundary. It supplies neither a snapshot transaction nor an intra-second pagination cursor. An empty or repeated page is evidence of that query's outcome, not proof that an adversarial or changing relay has exposed every event. This is an inference from the protocol, and corrects historical wording that one extra query defeats any cap. Keep traversal and coverage confidence separate. [NIP-01](https://github.com/nostr-protocol/nips/blob/master/01.md).
2. **EOSE now has useful optional hints.** NIP-67 defines `finish`, `more`, and `auth`; unknown hints are tolerated. Preserve them in the wire adapter and coverage record. `finish` is a relay-reported exhaustion claim scoped to filters, time and current authorization; it is not independent verification. Contradictory hints or unsolicited framing require a protocol anomaly result. [NIP-67](https://github.com/nostr-protocol/nips/blob/master/67.md).
3. **NEG-CLOSE does not prove convergence.** It is a client message releasing resources. Protocol completion comes from successful reconciliation processing; downloads of every required id still need verification and durability. `blocked` can mean query size or age restriction, so unlimited shrinking is wrong. A `NEG-MSG` demonstrates a protocol response, not unrestricted historical access. NIP-77 uses 64-bit timestamps and reserves its maximum value. [NIP-77](https://github.com/nostr-protocol/nips/blob/master/77.md).
4. **Authorization is part of the query scope.** An AUTH challenge alone does not mean every read requires authorization. NIP-42 can restrict particular operations; its authentication event is connection-bound and not intended for broadcast. Reissue a refused subscription only after an accepted AUTH response. Persist public and authenticated observations separately. [NIP-42](https://github.com/nostr-protocol/nips/blob/master/42.md).
5. **COUNT and search responses have limited meaning.** Counts may be approximate. Search result ordering is relevance-based, and implementations may ignore a search field. Neither an empty search response nor an EOSE proves compliant search behavior; neither COUNT nor search establishes archive coverage. [NIP-45](https://github.com/nostr-protocol/nips/blob/master/45.md), [NIP-50](https://github.com/nostr-protocol/nips/blob/master/50.md).
6. **Endpoint identity should conserve distinctions.** RFC 3986 normalizes scheme/host case and permitted syntax, but does not authorize arbitrary removal of queries or merging `/path` and `/path/`. NIP-66 identifies a normalized full URL, and now permits a pubkey for non-URL relays. Preserve opaque non-dialable subjects in discovery rather than manufacture URLs. Proposed scheme-sensitive endpoint identity avoids merging `ws://host:443` with `wss://host:443` or discarding query-specific backends. [RFC 3986 §6](https://www.rfc-editor.org/rfc/rfc3986.html#section-6), [NIP-66](https://github.com/nostr-protocol/nips/blob/master/66.md).

## Changes justified by the findings

| Inherited rule or implementation | Replacement | Useful intent preserved |
|---|---|---|
| Sequential `RunOnce` chain and service-name startup dependencies | Independent workers over explicit data contracts | One service's slow relay cannot hold up unrelated work |
| Every service reads only its own state; no RPC | Ownership of writes; versioned shared read models and optional authenticated ingest RPC | Durable recovery and independent deployment |
| One process root writer necessarily shared by all acquisition | One append authority per ingest partition; durable receipts across process boundaries | Deduplication and progress behind durable data |
| ClickHouse required for network acquisition and archive discovery | Watermarked local-set read contract and ordered discovery feed; storage implementation chosen by measured cost | Queryable/rebuildable evidence without coupling all admission to analytics availability |
| Scheme-less identity, implicit plaintext fallback | Explicit transport URL; derived alias evidence | Endpoint fidelity and explicit transport policy |
| Empty/all-seen page completes a range | Evidence class plus independent traversal position and unresolved intervals | Honest systematic acquisition with progress |
| Unverified imported provenance must resolve through active relays | Imported claims refer to identities without granting enrollment | Preserve external evidence without fabricating local observations |
| Delete repeatedly failing candidates | Dormant state, bounded evidence retention, new-evidence revival | Avoid dead-host resource waste without destroying identity history |
| Monitor pubkey defaults to all zero bytes | Stable random observer id; optional actual signer binding | Attribution across independently deployed monitors |
| Permanent per-event discovery truncation | Resumable tag offset with bounded work quanta | Bounded resources without silently losing valid tail references |

## Historical defaults and mutability evidence

The current Go defaults below are inspected values, not endorsed Rust defaults. Live code changes after the frozen docs explain extra fields. The read source does not establish a reliable hot-reload contract for each field; the new contract defines reload classes independently.

| Service | Inspected Go defaults and significant bounds |
|---|---|
| Seeder | `interval=20s`, `resolve_concurrency=32`, `file_path=static/seed_relays.txt`, `to_validate=true` |
| Discoverer | `interval=20s`; events enabled, scan/batch 500, parallel upserts 60, per-relay 15m, cycle 2h; API enabled, 24h cooldown, 1s delay, 5 MiB response, nostr.watch online/offline URLs, selector `[*]`, request/connect 10s |
| Validator | `interval=20s`, chunk 100, concurrency 20, no total candidate limit, fixed retry 1h, cleanup enabled after 720 failures, NIP-11 1 MiB, handshake 10s, insecure TLS refused |
| Monitor | interval 5m, chunk 100, max relays 0=all, host budget 32, jitter 500ms, host concurrency 30, inflight probes 32, check deadline 45s, NIP-11 1 MiB, retry count 0, retry delays 1s/10s and jitter 0.5; Geo max 100,000,000 bytes, age 30d, geohash 9, paths empty; all implemented check families computed/stored, NET refused |
| Synchronizer | interval 20s, filters `[{}]`, since 0, safe lag/warm 48h, hot 1h, reverify 7d, segment width 1d, neg messages 1000/rounds 64, need list batch 200, linear limit 500, hints/live enabled, host concurrency 3/global 128, lag throttle 1h→32 workers, rotation 10m, connect 10s, idle 5m, connection 24h, phase 4h, journal segment 256 MiB/15m, seal 8 MiB/10s, event 256 KiB, message 2 MiB, inflate ratio 10/bytes 10 MiB, probe 10s×2 attempts/scale2, reprobe 7d, ephemeral_drop=false, insecure TLS refused |
| Importer | detect format, batch 1000, record 16 MiB, event 256 KiB, signature verification and moderation enforced, edge ceiling 10,000,000; unfiltered false, selected filter empty, source shape undeclared |

## Remaining research and product decisions

No real relay fleet, import corpus, Rust SDK, disk crash recovery or network proxy was exercised in this subtask. The coordinating agent independently inspected `nostrdevkit/nostr` at `5c669a498e5f8dd1ec5713b11fdb92bcd858ad73` (`nostr` 0.45.4): `nostr/src/message/relay.rs` models EOSE with only the subscription id, and its test intentionally discards the third-element hints. This communicated source finding supports a bounded wire-envelope adapter; it is not an executed experiment result in this document. Real-relay capability distribution, same-second interoperability, SDK memory bounds, proxy routing and reconciliation-set reader behavior remain implementation research requirements. Live source observations have not been frozen as source snapshots in this document; historical documentation has.

Five product questions were sent to the coordinating agent: breadth versus explicit budgeted exclusions; retaining imported claims from unknown relays; optional active write probes and their identity; conservative transport-sensitive endpoint identity; and whether lightweight means thin serving indexes or irreversible loss of signed payload. The owner subsequently required retained signatures; the coordinated plan preserves complete signed event bodies, removes the heavy-fields axis and lossy LilBrotr/IndexBrotr deployment presets, and permits slim analytical projections only as explicitly non-verifiable datasets. Unsigned/slim artifacts cannot enter the canonical event archive. Proposed defaults for the other questions appear in the architecture contract; decisions belong in `docs/planning/questions.md`. The old repository's scheduling, languages, process requirements and unimplemented switches do not settle these questions.
