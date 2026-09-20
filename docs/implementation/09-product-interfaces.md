# Phase 09 — Public dashboard and private administration specification

Status: **implementation in progress; no P09 task is complete**. Unchecked acceptance remains planned. The owner opened implementation on 2026-09-17. This completes historical dashboard phase 9 and the selected private administration scope; it does not include the independent project website in [phase 10](10-website.md).

**Goal:** make the archive's actual evidence useful to readers and its configuration, work and recovery understandable to operators through separate accessible browser applications.

**Architecture:** SvelteKit/TypeScript static builds at `apps/dashboard/` and `apps/admin/`, served by their respective Rust public-read and authenticated private listeners. Reuse generated schema types and small shared accessible components. No Node server, database credential, browser SQL or privileged embedded API key is required in a deployed instance. Public routes cannot proxy the management API.

**Contracts:** [dashboard/product](../product/dashboard-and-website.md), [private administration](../product/private-administration.md), [read/publication](../architecture/derived-read-publication.md), [search/reconciliation](../architecture/search-and-reconciliation.md), [data decisions](../product/data-and-feature-decisions.md), [P08 operations](08-operations.md). Core field meanings come from the [central data contracts](../specification/data-contracts.md); no UI-local replacement for visibility, snapshot, timestamp or count semantics is permitted. A backend capability requires an executable authorized answer path, ready inputs and declared scope, not merely a configured service name.

## Shared acceptance and contract rules

P01-T08 owns `tests/acceptance/run.py`, shared result schema and artifact identity checks. This phase adds `tests/acceptance/cases/interfaces.py`, plus actual browser suites below. The proposed concrete first invocation is `python3 tests/acceptance/run.py p09-t01 --binary dist/bigbrotr --compose packaging/compose/acceptance.yaml --evidence .research/implementation/p09-t01`. Register the task cases `p09-t01` through `p09-t11` in that runner; each writes to its matching named directory under `.research/implementation/`. browser engines are run against production static bundles and actual packaged Rust APIs. Private deterministic fixtures may prepare setup; intercepted mock HTTP alone cannot satisfy release acceptance.

Retain input corpus/visibility manifest, backend/asset/schema digests, browser/OS/viewport, task steps, executed and NOT RUN states, traces, accessibility findings, safe screenshots, console/network errors, API correlation IDs and independent oracle results. Exclude credentials, private event bodies and bootstrap tokens. Playwright Chromium/Firefox/WebKit is the automated matrix; real Safari/Apple, Android and screen-reader observations are separately named evidence, never inferred from emulation.

The common answer-state model is:

```text
ViewState = unavailable | incompatible | loading | preparing | ready-empty |
            ready-data | partial | stale | failed | rate-limited | forbidden
AnswerBasis = { effective_query, source_cut, policy_revision, coverage,
                observation_time?, completion_time?, exactness, continuation? }
```

These are UI presentation states over canonical API envelopes, not substitute backend enums. An HTTP failure is never `ready-empty`; unknown is never zero; stale is never current. Restricted resource existence or size is not disclosed in a public explanation. Back/forward/reload preserve nonsecret filters, while caches are partitioned by origin/principal/policy/capability revision and remain in memory by default.

## P09-T01 — Separate static apps, shared client and accessible primitives

**Files:** create `package.json`, `pnpm-workspace.yaml`, `pnpm-lock.yaml`, `apps/dashboard/{package.json,svelte.config.js,vite.config.ts}`, `apps/admin/{package.json,svelte.config.js,vite.config.ts}`, `packages/api-client/src/{read,management,transport,errors,query-codec}.ts`, `packages/ui/src/{Field,DataTable,StatusNotice,Dialog,ChartTable,AsyncButton}.svelte`, `packages/brand/{tokens.css,logo.svg}`, `apps/dashboard/src/lib/{config,i18n,query-state}.ts`, `apps/admin/src/lib/config.ts`, `apps/dashboard/tests/unit/client-contract.test.ts`.

**Dependencies/input/output:** consume P06 versioned HTTP schema and P08 management schema; generate wire types rather than manual duplicates. Produce small typed clients that preserve answer metadata, `AbortSignal`, retry advice, cursor strings and typed errors. Nonsecret `app-config.json` provides API origin/base path/instance/docs labels. Admin config names only its private origin; it is not appended to public configuration.

- [ ] **Step 1:** configure separately named static packages `@bigbrotr/dashboard` and `@bigbrotr/admin`, strict TS, pinned lockfile and Svelte static adapters. Import only required shared components; Rust-only development does not trigger frontend builds.
- [ ] **Step 2:** implement schema generation/runtime compatibility checks and fetch transport with bounded response consumption, explicit request cancellation and no implicit infinite retry. Preserve exact string IDs, u64-safe timestamp representations, empty tag values, commas and meaningful whitespace through the canonical JSON query codec.
- [ ] **Step 3:** implement native semantic forms/tables/dialogs with keyboard focus restoration, errors linked to inputs, live regions for submitted operations, reduced-motion support and chart/table alternatives. English messages use stable keys/plurals and locale-aware formatting; timestamp displays name timezone with UTC available. No unmaintained language option appears.
- [ ] **Step 4:** propose `pnpm --filter @bigbrotr/dashboard test:unit -- client-contract.test.ts` using fixture envelopes for unsupported major, missing field, partial/exact/estimated count, cancellation, 429 and private-policy revision. Negative control delays query A until after query B; the rendered accepted state must still be B and the server must observe A's cancellation.
- [ ] **Step 5:** run proposed common case `p09-t01`; serve each production artifact from Rust at root and `/explorer/` or `/control/`, reload deep links, change runtime origin without rebuilding, and inspect public bundle/config/network for management paths and secret canaries. A missing lazy asset after rollout must produce a recoverable refresh message rather than a blank screen.

**Failure contract:** no third-party request is made merely by parsing instance content. Unknown API major stops data queries with a version explanation. Authentication failures clear unauthorized caches; changing instances cannot reuse prior results.

**Reconnect and identity rule:** every active request records API origin, principal, policy revision and query generation. A completion may update the view only if all still match; aborting fetch is supplemented by this comparison because a response can already be queued. Login/logout and cross-tab revocation broadcast invalidation without broadcasting credentials or bodies. Evidence includes a late authorized response arriving after logout and must show it is neither rendered nor restored from cache.

**Browser resource rule:** respect advertised result/byte limits while streaming the response, including when Content-Length is absent or misleading. Keep only the current page plus a bounded permitted cache; cancel/discard superseded pages and revoke object URLs. Browser-safe uint64 fields remain decimal strings/BigInt as defined by the API; conversion to JavaScript Number cannot silently round timestamps/counters. Prefer server-streamed permitted artifacts for large downloads rather than constructing a whole-corpus Blob. Content/signature verification uses the proposed `apps/dashboard/src/lib/events/verify.worker.ts` worker with bounded transferred input so hostile content cannot stall navigation or multiply copies unaccounted.

Cache/request identity also includes ReadViewStamp and its temporal deadline.
Back/forward/reconnect cannot redisplay an expired ordinary answer as current;
invalidate/mark it and re-admit the query. Explicitly authorized historical or
dated stale analysis has its separately declared presentation semantics. P09-T01
tests expiry without new ingestion, delayed responses and history navigation.

### Initial private session consumer (2026-09-20, in progress)

The first concrete consumer is `@bigbrotr/admin` and its generated session-only
client, backed by the three existing P08-T02 HTTP routes. The optional Rust static
loader verifies closed manifests and retains bounded immutable bytes for root
and `/control/` delivery. [Local evidence](evidence/p09-private-panel-local-20260920.json)
records Mac10 client/8 model/25 config/6 policy/17 management tests, strict type
checks, build/Clippy and both bundles. Actual TCP cases cover headers, exact mounts,
refusals and zero-byte delivery after revocation; these are not browser evidence.
Independent AI review corrections have discriminating runtime regressions for
cross-tab lifecycle. The [actual native browser matrix](evidence/p09-private-panel-bootstrap-real-native-20260920.json)
executes eight session journeys on Chromium/Firefox at both mounts; WebKit passes
seven and fails recovery from an absent bootstrap module. The selected correction
omits speculative JS preloads while retaining ordinary imports and CSS. Qualify
the same real 404/restoration/reload journey before closing this guarantee.
Public application separation, large blocked response, full accessible primitives
and shared frontend CI/release assembly remain required. No task step is complete.

## P09-T02 — Public navigation, capabilities and first useful data

**Files:** create `apps/dashboard/src/routes/{+layout,+page}.svelte`, `apps/dashboard/src/routes/status/+page.svelte`, `apps/dashboard/src/routes/about/+page.svelte`, `apps/dashboard/src/lib/{capabilities,answer-state,cache}.ts`, `apps/dashboard/src/lib/components/{InstanceHeader,ScopeSummary,CapabilityNotice}.svelte`, `apps/dashboard/tests/e2e/{onboarding,capabilities,status}.spec.ts`, `docs/user/dashboard/overview.md`.

**Interfaces:** `/v1/capabilities` and sanitized instance/status descriptors drive navigation for Overview, Relays, Events, Analytics and About. Menu presence uses caller-visible supported resources, ready inputs and scope; recipe names are never feature gates. Public About includes operator/contact/method/API/relay/DVM links only when explicitly exposed. Status never reveals private hosts, paths, keys or raw metrics.

- [ ] **Step 1:** implement bootstrap capability fetch, API-version check, instance identity and initial resource navigation. Display setup progress for supported-but-not-ready resources; direct links to hidden resources use a generic unavailable response without probing their private existence.
- [ ] **Step 2:** implement loading/empty/partial/stale/down transitions, preserving filters and dated old data only while permitted. Summary cards require defined numerator/denominator/coverage; no published rank generation means an unavailable score, not zero.
- [ ] **Step 3:** implement policy/capability refresh, cancellation and invalidation. Poll only at advertised cadence, pause hidden/offline tabs, obey `Retry-After`, and recheck on return without a request burst. An expired policy lease clears protected data rather than serving it indefinitely from cache.
- [ ] **Step 4:** run `p09-t02` over full, archive-only, no-provenance and custom social scopes against real backend fixtures; disable the indexer and revoke one resource while open. Negative control supplies a registered capability with no ready executable source; a page that displays a zero chart instead of preparing/unavailable fails.
- [ ] **Step 5:** retain the clean bundle journey: launch with empty volumes, inspect honest initialization, admit one signed event, see corresponding ready data, stop the read dependency and distinguish failure from valid empty. Check status from public network side while management remains accessible only privately.

## P09-T03 — Relay directory, detail, comparison and selection

**Files:** create `apps/dashboard/src/routes/relays/+page.svelte`, `apps/dashboard/src/routes/relays/[relay_id]/+page.svelte`, `apps/dashboard/src/routes/relays/compare/+page.svelte`, `apps/dashboard/src/lib/relay/{filters,comparison,measurements}.ts`, `apps/dashboard/src/lib/components/{RelayTable,MeasurementTimeline,DocumentVersion}.svelte`, `apps/dashboard/tests/e2e/relay-selection.spec.ts`, `docs/user/dashboard/relays.md`.

**Interfaces:** canonical endpoint IDs/URLs, typed measurement attempts and document versions, coverage/route/method metadata, optional scores and comparable windows from P04/P05/P06. Auth-restricted endpoints stay hidden. Hosting country/ASN describes the measured endpoint at stated precision, not an author's location. “Unknown”, “unmeasured”, “stale”, “remote failed” and “local probe unavailable” remain distinct.

- [ ] **Step 1:** implement directory filters/sort for only supported dimensions: network, NIPs, latest observed reachability, software, location/ASN and measurement age; show effective query and finite pagination. Copy preserves the canonical relay URL, including case-sensitive path/query semantics.
- [ ] **Step 2:** implement detail sections for original declared NIP-11 versions, TLS/DNS/catalog changes, liveness/catalog attempt history, eligibility/backoff and coverage. Show method/observer/route/time and document age; an unchanged cached document is not a fresh independent measurement.
- [ ] **Step 3:** implement server-bounded comparison with a common requested time window, explicit incomparability and no client all-pairs graph. Explain scores and advisory overlap through components; no missing score sorts as an invented zero or makes endpoints disappear from observation.
- [ ] **Step 4:** run `p09-t03`: select required network/NIPs, compare two comparable endpoints and one stale/different-scope endpoint, inspect evidence, then copy URL. Independent fixture records determine labels and ordering. Negative control swaps declared NIP support with measured protocol support; the interface must keep the two evidence categories distinct.
- [ ] **Step 5:** test malicious relay names/document strings/URLs, 429, unavailable geography and very long identifiers through actual browser actions. Require escaped content, no automatic external media or private fetch, keyboard sorting/filtering and table-equivalent chart data. Retain copied values and external-request deny log.

The large-directory fixture contains many more endpoints than one page and a heavily skewed software/network class. The network observer must see bounded server queries rather than automatic fetch-all pagination. Comparison caps come from the backend resource, and removing a selected relay cancels its obsolete detail work. Rendered result count, retained client memory and request rate are measured separately from backend corpus size.

## P09-T04 — Event queries, search, details and authorized history

**Files:** create `apps/dashboard/src/routes/events/+page.svelte`, `apps/dashboard/src/routes/events/[event_id]/{+page,observations/+page,availability/+page,history/+page}.svelte`, `apps/dashboard/src/routes/authors/[pubkey]/+page.svelte`, `apps/dashboard/src/routes/search/+page.svelte`, `apps/dashboard/src/lib/events/{identifiers,query,download,signature,highlight,verify.worker}.ts`, `apps/dashboard/tests/e2e/{event-inspection,query-pagination,search-policy}.spec.ts`, `docs/user/dashboard/{events,search,history}.md`.

**Interfaces:** P06 bounded query/current/full-event/history/provenance/search resources and canonical signed-event codec. Ordinary direct-ID reads retain deletion/current/expiration policy. Full history requires its explicit grant; observation history requires actual provenance/recheck data. Content search is distinct from ID/table/tag filtering and exposes analyzer/scope/relevance/coverage.

- [ ] **Step 1:** implement lossless supported identifier parsing and submitted query forms for IDs/authors/kinds/time/tags/text. Invalid identifiers fail locally without changing the user's text. Shareable URLs contain public filters only; repeated tags/commas/empty strings round-trip through the defined JSON query representation.
- [ ] **Step 2:** implement cursor navigation and count/exactness/coverage display, preserving snapshot/policy basis. Follow actual opaque returned cursors through mixed and wholly withheld candidate pages; cursor expiration/revocation yields a restart explanation, not a silent jump to page one.
- [ ] **Step 3:** implement metadata/full signed raw display, bounded text preview, explicit single-event download and local verification. Verification checks cryptographic event authorship only. Show signed `created_at`, first local acquisition and relay observation times separately; backfill timestamps never become propagation latency.
- [ ] **Step 4:** implement observations, dated positive/negative availability and authorized version/deletion history. Missing hot body, cold retrieval unavailable, restricted body and absent metadata use backend distinctions; never substitute an older body for a current winner. Safe search highlighting renders text nodes, not event-supplied HTML.

Author links open the public key's permitted current profile/list and bounded event resources, showing the originating signed event and method. Missing/invalid kind-specific metadata stays distinct from a missing account; a public key is not proof of a real-world identity. Empty current lists do not resurrect older entries. Profile images/links follow the same explicit external-media action as event content.
- [ ] **Step 5:** run `p09-t04` with ties, repeated tags, zero/max timestamps, deleted targets, private provenance, cold bodies, HTML/script text and cancellation during search. Independent signed corpus/policy manifest decides IDs and complete downloaded fields. Negative control edits a downloaded content byte while preserving the signature; the local verification indicator must change to invalid, while merely changing display timezone must not.

## P09-T05 — Analysis, graph viewpoints and explainable scores

**Files:** create `apps/dashboard/src/routes/analytics/{network,coverage,availability,events,social,scores}/+page.svelte`, `apps/dashboard/src/routes/analytics/scores/[subject_type]/[subject_id]/+page.svelte`, `apps/dashboard/src/lib/analytics/{basis,comparability,citations,charts}.ts`, `apps/dashboard/src/lib/components/{ScoreBreakdown,MethodPanel,BoundedGraph}.svelte`, `apps/dashboard/tests/e2e/analytics.spec.ts`, `docs/user/dashboard/analysis.md`.

**Interfaces:** P05 published facts/assertions, graph/rank generations, named viewpoints, exactness/units/evidence metadata and P06 bounded resources. Each panel names its required kinds/fields/coverage and public-eligible input generation. Graphs fetch bounded server subgraphs; the browser never downloads the full network adjacency to compute global statistics.

- [ ] **Step 1:** implement network/software/NIP/geography trends and event-kind/activity timelines with separate creation/acquisition axes; repeated ingestion counts are operational traffic, not unique archival growth. Display correct denominators and unknown scope.
- [ ] **Step 2:** implement coverage/overlap/contribution and selective-availability comparisons with source windows/methods/sampling. Reject or explicitly segregate incomparable sets. Observed serving differences do not automatically become a claim of censorship intent or independent operator identity.
- [ ] **Step 3:** implement social follow/mute/activity views and scores with raw value, normalization, model/version, seeds/viewpoint, input cut, missing components and last completed calculation. Zero-valued valid scores remain distinct from missing/withheld/undefined. A newly empty replaceable list removes old edges; no list observed is unknown.

Score routes include canonical subject type as well as ID; equal bytes for an event and public key cannot select the wrong assertion family. URL state includes selected algorithm/viewpoint/generation, and the method panel preserves those values in citations and downloads. Non-user relay/event/addressable assertions use their registry definitions rather than silently reusing a user-score label.
- [ ] **Step 4:** implement chart summaries/tables, explicit bounded graph expansion and reusable citation metadata. Compare only compatible generations; a new partial rank job leaves the prior permitted complete view dated, not silently replaced.
- [ ] **Step 5:** run `p09-t05` with no seeds, provenance off, partial social kinds, private follows, equal-score ties, stale complete run and failed new run. Independent slow reducers/source fixtures provide expected totals/components. Negative control hides private rows only after aggregating them in a deliberately faulty fixture response; the independently public-only expected score/count must detect the leak.

Exercise a hub-dominated graph and very long time series: only a server-bounded subgraph or declared aggregate enters the browser, not the complete adjacency or an undocumented client sample. Chart downsampling/aggregation preserves method and resolution labels, and table/download reflects the same basis. Load charts lazily; switching to Events releases obsolete graph work. A score-page timeout cannot starve configuration or enrollment requests sharing the instance.

## P09-T06 — Published datasets, saved queries and developer discovery

**Files:** create `apps/dashboard/src/routes/datasets/+page.svelte`, `apps/dashboard/src/routes/datasets/[dataset_id]/+page.svelte`, `apps/dashboard/src/lib/research/{saved-query,citation,artifact}.ts`, `apps/dashboard/src/lib/components/{DatasetManifest,QueryCitation}.svelte`, `apps/dashboard/tests/e2e/research-output.spec.ts`, `docs/user/dashboard/research.md`.

**Input/output:** only operator-published export manifests are public datasets. A query citation contains instance/resource/effective filter, retained snapshot or artifact digest when one exists, method version and retrieval time. A live-query URL explicitly may change. Saved public queries can stay in URLs; authenticated private saved views use a versioned management preference record, not a new query engine.

- [ ] **Step 1:** implement dataset listing/detail with format/schema, full-versus-projected label, scope/exactness, origin/visibility, file hashes, document references and reproduction commands. Public browsing cannot create arbitrary export or backup jobs.
- [ ] **Step 2:** implement citation copy/download and retained snapshot expiry. Preserve exactness/coverage in CSV or JSON result downloads; do not label a transformed row a full signed event. Artifact URLs follow current download authorization; no credential enters a share URL.
- [ ] **Step 3:** implement About links to exact-version HTTP schemas, ArchiveRelay REQ/COUNT/NIP-50/NIP-77, optional DVM, native MCP and experimental ContextVM documentation according to exposed protocols. Link the gateway's actual negotiated version, supported tools/read modes, authentication, limits and cancellation behavior; ContextVM is a separate encrypted transport, not DVM. Label HTTP-only answer metadata; an unavailable interface is not a broken clickable endpoint.
- [ ] **Step 4:** run `p09-t06`: export a fixture through P08, explicitly publish its manifest, open/download/cite it, stop the instance and read the files using standalone DuckDB. Independent hashes and source manifest must match. Negative control change only one referenced document file; verification must fail although the main Parquet remains intact.
- [ ] **Step 5:** exercise expired/private/unpublished artifacts, browser reconnection, native mobile download and live-versus-immutable query citations. Enable/disable each selected gateway and follow its About link to the matching version/subset example; absent or unqualified adapters cannot acquire a working-endpoint claim. Retain downloaded manifests and safe screenshots showing the distinctions; no private artifact bytes enter public evidence.

## P09-T07 — Private enrollment shell and permission-aware navigation

**Files:** create `apps/admin/src/routes/{+layout,+page,login/+page,enroll/+page}.svelte`, `apps/admin/src/lib/{session,permissions,security-revision}.ts`, `apps/admin/src/lib/components/{PrivateHeader,SessionNotice,Reauthenticate}.svelte`, `apps/admin/tests/e2e/{enrollment,roles,session}.spec.ts`, `docs/user/administration/panel.md`.

**Dependencies:** P08-T02 real local enrollment/account/session/grant APIs and P08-T03 revisions/jobs. The private shell displays explicitly that it manages the named instance. Its navigation is Overview, Services, Collection, Relays, Jobs, Queries, Storage, Backups, Publishing, Access, Configuration and Audit/help, filtered by allowed actions without assuming menu visibility enforces them.

The app keeps anti-CSRF/session handling at the private origin and never offers a control to send an existing administrator cookie/token to an arbitrary API origin. Switching private instances performs fresh authentication. Owner-recovery instructions invoke the local P08 CLI recovery flow; they do not expose a browser reset endpoint that bypasses enrollment. Account removal/revocation cannot accidentally remove the last recoverable owner without the explicit local recovery procedure.

- [ ] **Step 1:** implement one-time owner enrollment and login via the private origin with server CSRF/session controls. Token entry is not persisted to URL/storage/analytics. Handle expired/reused token and show local recovery instructions; remote setup cannot create an unauthenticated interval.
- [ ] **Step 2:** implement session expiry warning, sign-out, reauthentication and permission refresh. Cancelling reauthentication preserves the unsubmitted change but performs no action. Revocation clears sensitive in-memory data and invalidates affected open tabs.
- [ ] **Step 3:** implement a private overview with actual role health, alert causes, progress, desired/applied revisions and next safe actions. Started/ready/caught-up remain distinct. Present equivalent CLI/runbook links for recovery when the browser fails.
- [ ] **Step 4:** run `p09-t07` using viewer/operator/administrator plus explicit import/export/private-export/signing grants. Attempt requests directly outside the menu. Negative control grants operator job visibility but not private export; the server must deny export even if a crafted client renders the button.
- [ ] **Step 5:** test two concurrent initial enrollments, session max/idle expiry, CSRF, wrong Origin, public-proxy route guessing and keyboard-only enrollment. Retain safe role/action result matrix and audit references; omit tokens, cookies and passwords from traces.

## P09-T08 — Services, collection, relays and transactional configuration

**Files:** create `apps/admin/src/routes/{services,collection,relays,configuration}/+page.svelte`, `apps/admin/src/routes/relays/[relay_id]/+page.svelte`, `apps/admin/src/lib/config/{form,diff,application}.ts`, `apps/admin/src/lib/components/{RevisionDiff,RoleProgress,DependencyImpact,LockedSetting}.svelte`, `apps/admin/tests/e2e/configuration.spec.ts`, `docs/user/administration/configuration-workflow.md`.

**Input/output:** P08-T01 effective config/source/units, P08-T03 preview/CAS/application, P04 registry/coverage, P01 budgets. Forms express useful settings and policies, never private Rust implementation knobs. New provenance/filter/route policy creates visible evidence/coverage consequences; it cannot recreate missing history.

- [ ] **Step 1:** implement schema-driven common/advanced forms showing default/effective source/lock and useful help. Allow minimal changes; changing a source-locked value explains its deployment source and does not write mounted TOML.
- [ ] **Step 2:** implement preview with dependency/budget/listener/signing/coverage impact, apply with expected revision, per-role saved/draining/applied/restart-required/failed state and rollback as a new revision. Concurrent editor conflicts present a diff and reload/reapply path; never silently overwrite.
- [ ] **Step 3:** implement start/pause/drain/resume/stop for configured supervised roles with actual dependency/backlog/capability effects and desired/applied outcomes. Relay actions expose enrollment origin, exclusions, backoff and original measurement evidence; route/enrollment changes require the proper grant. No arbitrary hostname/file/script field bypasses guarded source adapters.
- [ ] **Step 4:** run `p09-t08`: two editors change different fields from the same base, apply a locked field, lower a budget below live use, disable provenance, pause Indexer and restart one affected role. Independent effective-config endpoint and observed permits/frontiers must agree with UI application status.
- [ ] **Step 5:** drop restrictive-change invalidation notifications and disconnect PG while the page is open. Negative control persist desired security revision without role acknowledgements; the UI must not display globally applied until old authority expires and affected work stops. Retain redacted revision/action timelines and actual denied reads.

## P09-T09 — Jobs, queries, storage and verified recovery

**Files:** create `apps/admin/src/routes/{jobs,queries,storage,backups}/+page.svelte`, `apps/admin/src/routes/jobs/[job_id]/+page.svelte`, `apps/admin/src/lib/jobs/{forms,progress,artifacts}.ts`, `apps/admin/src/lib/components/{JobProgress,CapacityBreakdown,BackupVerification}.svelte`, `apps/admin/tests/e2e/jobs-recovery.spec.ts`, `docs/user/administration/jobs-recovery.md`.

**Interfaces:** P08 durable import/export/backup/scrub/repair/migration jobs and pinned query builder; permission applies to both action and input/output data. Storage breakdown separates pending, archival truth, optional online bodies/indexes, caches and compaction reserve. Useful archived measurements remain historical evidence; logs/duplicate retry counters have separate operational retention.

- [ ] **Step 1:** implement job creation from approved sources/destinations with pinned scope, full/projected export choice, coverage/visibility preview and safe defaults. Jobs use persisted idempotency keys; double-click/network retry does not start duplicate work.
- [ ] **Step 2:** implement progress with committed versus displayed position, remaining work and resume unit. Closing/reopening a tab or restarting a worker retains receipt and progress. Cancel requests stop new work at a declared boundary and preserve committed data; cancelled does not mean rolled back.
- [ ] **Step 3:** implement typed queries/saved private views plus explicit export, storage/headroom and maintenance pages. Show measured rates/windows and unknown forecast, no unmeasured capacity promise. Explain current/historical policy and full/projection differences before download.

Saved-view persistence uses P08-T03's owner-scoped CAS routes. Storage exposes optional content/provenance/index readiness, cost, rebuild/retire jobs and affected capability; index removal is not removal of the full archival evidence. Exercise rebuild failure with the old complete generation still readable, then successful replacement and eventual retirement after pins release.
- [ ] **Step 4:** implement backups as uploaded/verified/restore-tested stages with last drill, destination and protection class. Restore first produces an isolated target plan and links to fenced CLI activation; the browser never overwrites a running instance or assumes a local backup protects against host loss.
- [ ] **Step 5:** run `p09-t09`: start export, close/reopen browser, restart worker, cancel another import, run backup and isolated restore, inspect scrub corruption, and compare final artifacts through independent source/standalone DuckDB oracle. Negative control upload a checksum-invalid backup: UI must never display recoverable/verified merely because upload finished. Repeat with viewer and private-export-denied operator.

**Lost acknowledgment journey:** drop the `POST /jobs` response after durable acceptance, reload the page and retry with the same idempotency key; discover one existing job, not two. Cancel after one committed chunk and verify that UI says which data already committed. Downloads reauthorize at request and bounded streaming chunks; expired/revoked authorization ends delivery without advertising a complete manifest. Already downloaded files remain explicitly outside recall guarantees.

## P09-T10 — Publishing, access, audit and safe diagnostics

**Files:** create `apps/admin/src/routes/{publishing,access,audit}/+page.svelte`, `apps/admin/src/lib/security/{grants,secret-reference,publication-preview}.ts`, `apps/admin/src/lib/components/{OutboxTargets,GrantDiff,AuditTable,DiagnosticPreview}.svelte`, `apps/admin/tests/e2e/security-operations.spec.ts`, `docs/user/administration/security-operations.md`.

**Input/output:** consume P07 purpose/algorithm/viewpoint signer policies and immutable signed outbox bytes/per-destination receipts; P08 scoped grants/audit/diagnostics. UI can submit a protected reference, not retrieve a current secret. Public resource exposure, management authority and signing administration remain separate grants.

- [ ] **Step 1:** implement publisher preview naming kind/profile, algorithm/viewpoint, source-generation visibility and destinations; explicit enablement requires signing grant. Show queued/sent/accepted/rejected/unknown per destination and distinguish acceptance from enduring retention/global dissemination.
- [ ] **Step 2:** implement user/token grant/revoke with reauthentication and activation status. Show newly issued token once through a dedicated safe view; never place it in normal config/audit history or browser storage. Non-signing administrators do not automatically obtain signer authority.
- [ ] **Step 3:** implement filterable audit and bounded redacted diagnostic download preview with installed-version runbook links. Include rejected commands and partial applications; no event-body/secret/private-URL dump by default. Browser shell/SQL execution is absent.
- [ ] **Step 4:** run `p09-t10`: one destination accepts and another rejects, worker crashes after send, signer goes unavailable, private generation is selected for public publication, token is revoked mid-read/export. Independent signed-byte digest and P07 receipts must remain stable across retries. Negative control changes a retry timestamp to create new signed bytes; UI/backend acceptance must flag identity mismatch rather than silently presenting it as the same delivery.
- [ ] **Step 5:** plant secret/file/private-endpoint canaries, inspect all diagnostic downloads/error states/console traces and crafted unauthorized commands. Match accepted/rejected actions to audit IDs and security-barrier state. Display “already delivered cannot be recalled” for irreversible external effects without promising rollback.

## P09-T11 — Browser, accessibility, security and packaged journey completion

**Files:** create `apps/dashboard/playwright.config.ts`, `apps/admin/playwright.config.ts`, `apps/dashboard/tests/e2e/{accessibility,hostile-content,deployment}.spec.ts`, `apps/admin/tests/e2e/{accessibility,deployment}.spec.ts`, `tests/acceptance/cases/interfaces.py`, `docs/user/validation/browser-matrix.md`, `packages/ui/tests/accessibility.test.ts`.

**Dependencies/output:** every prior P09 task, P08 real bundles and real API resources. Output is named journey evidence and resolved release blockers, not an axe score or an arbitrary bundle/latency threshold. WCAG 2.2 AA is the target; automated scanning alone does not establish conformance.

- [ ] **Step 1:** run proposed `pnpm --filter @bigbrotr/dashboard test:e2e` and `pnpm --filter @bigbrotr/admin test:e2e` against release static output in Chromium, Firefox and WebKit. Cover direct deep links, root/subpath, same-origin/allowed CORS, runtime config, stale asset rollout and browser back/forward; incompatible API must fail intelligibly.
- [ ] **Step 2:** perform keyboard-only relay selection/event research/enrollment/config/apply/job recovery, 200%/400% zoom, narrow layout, dark/light/high contrast and reduced motion. Verify chart-equivalent tables, labels, error focus and dialog return focus. Inspect VoiceOver/Safari and NVDA/Firefox in maintained environments plus real Safari/Apple and Android downloads/touch behavior; missing environment records NOT RUN.
- [ ] **Step 3:** drive hostile Unicode/bidi/HTML, long content/IDs, malformed links, external media, delayed responses, 429/offline, revoked sessions/policy and forbidden deep links. Network oracle rejects unsolicited third-party/private destinations; large content keeps bounded preview and explicit download. A visual disabled button cannot satisfy the authorization test.
- [ ] **Step 4:** run `p09-t11` on the extracted complete bundle: install/enroll, choose relay, inspect/verify event, explain score, cite/download corpus, pause/resume Indexer, apply conflicting revisions, export across tab closure, verify backup/restore, recover without public management exposure. Compare shown results and effects to independent API/corpus/config/receipt oracles.
- [ ] **Step 5:** measure cold/warm transfer, interaction latency, long tasks and memory on recorded ordinary hardware/network under realistic page sizes. Investigate unbounded work and material regressions; set a release-specific measured envelope rather than inventing a universal benchmark. Negative control runs the previous known-bad fixture with an injected focus trap and stale response overwrite: the acceptance suite must report both failures, proving it exercised the user journey rather than merely loaded pages.

## Revision 1 — Initial completeness review

The first pass explicitly repaired common omissions: private UI is a separate artifact/listener; configuration has actual per-role application; jobs survive tab closure; permanent useful archive measurements are separated from expiring operational telemetry; timestamps and tags stay lossless; full/projection downloads have different meanings; raw-history access does not bypass ordinary serving; graph/score inputs respect visibility before derivation; and every page has a real resource/permission gate. The subsequent numbered records document the central-field, scenario, recovery and artifact reconciliations actually made after this first completeness review. They are specification revisions, not claims that the future product passed acceptance.

## Revision 2 — Semantics and privacy

Aligned canonical core field meanings and P08 routes rather than duplicating backend state. UI states distinguish unknown/empty/stale/forbidden, direct-ID ordinary policy from authorized history, source visibility before aggregate/score generation and artifact fidelity. Canonical operator links now use docs/operations. Proposed acceptance uses the shared P01 runner and separately identifies production bundles versus setup fixtures; no frontend menu or mock response counts as authorization evidence.

## Revision 3 — Disconnect, revoked authority and recovery journeys

Added a response identity/generation check in addition to fetch cancellation, including late private responses after logout. Switching management instances requires fresh authentication. The jobs journey now drops the acknowledgment after commit and resumes the same idempotent job, distinguishes committed data from cancellation and reauthorizes downloads. Owner recovery remains a local authenticated-authority workflow rather than a web reset shortcut; isolated restore verifies data and blocks production publication.

## Revision 4 — Browser size, skew and cancellation

Added streamed-response limits, bounded page/cache/object-URL ownership, lossless uint64 handling and worker-isolated signature verification. Large directory acceptance now proves the browser never fetches every page automatically; skewed graph/time-series tests require bounded server data and truthful aggregation labels. Live/admin responsiveness and actual retained memory/request rate are observed while queries/downloads are slow or oversized, rather than inferred from component-local array limits.

## Revision 5 — Complete routes and real backend ownership

Added the author/profile/current-list destination so author links have an actual permitted resource journey, and disambiguated score routes by subject type plus algorithm/viewpoint/generation. Filled the backend handoff for persisted saved views and index rebuild/retire instead of treating them as frontend-only features. All eleven task IDs have five concrete numbered actions and an independent negative control; all local planning links resolve. Final browser acceptance uses the exact released public/private bundles, native download/assistive tests and runtime API effects; a static asset build or intercepted fixture alone cannot finish the phase.
