# Dashboard, operations and website product design

Status: proposed design for the fresh Rust product, 2026-09-11. This document is
implementation planning, not an assertion that these pages or APIs already exist.
Read the [source findings](../research/product-surfaces-findings.md) for inspected
evidence, historical gaps, stack alternatives and owner-question candidates.

## Product promise and boundaries

BigBrotr helps people observe Nostr relays, acquire a durable event corpus, examine
what that corpus actually covers, and consume useful facts through supported
interfaces. It must make the next action clear without making the user understand
its storage engines first. Every view describes the instance's observations and
retained scope; it does not claim to see the entire network.

Three surfaces serve different audiences:

| Surface | Audience and purpose | Data and access | Deployment |
|---|---|---|---|
| Public/research dashboard | Relay selection, event inspection, evidence and corpus analysis | Only versioned, authorized read resources; optional anonymous access | Optional static application assets, normally served on the same origin as the Rust read API |
| Private administration | Deployment, configuration, jobs, access, failures, capacity and recovery | Authenticated management UI and CLI using the same typed commands; optional Grafana detail | Separate private management API/listener and authentication boundary; never included in public reverse-proxy routes |
| Project website, docs and blog | Explain value, install, integrate, learn and follow releases/research | Authored static content and explicitly published release/corpus metadata | Independent static artifact on `bigbrotr.com` or another static host; no database access |

The dashboard is useful privately too. “Public dashboard” describes its end-user
scope, not permission to expose a listener. Operators explicitly select external
exposure. The website's “Explore an instance” link names its operator and destination
so that the project website is not mistaken for the host of every archive.

The owner selected a real private administration interface as part of the product.
It shares the dashboard's frontend stack and visual components while using a
separate authenticated management API. The public dashboard retains read-only
resource access and sanitized service status. Grafana provides optional detailed
metrics; routine configuration and durable jobs are accessible through the panel
and equivalent CLI commands. The canonical authorization, configuration revision,
secret handling and job contracts are in
[Private administration](private-administration.md).

The private panel has concrete pages with matching CLI/runbook actions. Routes
below belong to the private application origin, not the public dashboard:

| Private page | Decision it supports | Required evidence/action |
|---|---|---|
| Overview `/` | Is work progressing, paused or failing? | Enabled services, dependencies, alerts and last success/failure; started, ready and caught-up states remain distinct |
| Services `/services` | Can this role be paused, drained or resumed safely? | Actual dependencies, durable/visible frontiers, backlog and effects on capabilities; permission-checked lifecycle commands |
| Collection `/collection` | What will changing acquisition or provenance collect next? | Effective filters/routes, new coverage epoch, historical gaps and predicted work; validated config revision |
| Relays `/relays` | Is acquisition stalled or waiting for bounded work? | Enrollment, original measurements, backoff/exclusions, scoped traversal and local-versus-remote failure; attributable operator changes |
| Jobs `/jobs` | Did an import, export, derivation or rank run finish, and can it resume? | Persistent job identity, pinned source/algorithm, progress, cancellation boundary and outputs; closing a tab does not cancel work |
| Queries `/queries` | Can an operator investigate and save a useful result within budget? | Typed query builder, exactness, generation, safe saved views and authorized export; no browser SQL credentials |
| Storage `/storage` | What will exhaust resources first? | Pending/archive/index/hot-cold bytes, compaction reserve, measured growth and finite outage headroom |
| Backups `/backups` | Can the archive and control state be recovered? | Destination references, backup jobs, verified artifacts and separate restore drills; uploaded is not restored |
| Publishing `/publishing` | Which identity publishes what, and are destinations accepting it? | Algorithm/viewpoint and signer references, preview, outbox state and destination failures; never display raw secret keys |
| Access `/access` | Who may read or administer each resource? | Viewer/operator/administrator permissions, scoped token issue/revoke and public exposure separate from management authority |
| Configuration `/configuration` | Will a change persist and apply correctly? | Effective source/units, locked values, candidate diff, dependency/resource checks and per-service applied/restart-required/failed revision state |
| Audit and help `/audit` | What changed, and how can the operator recover? | Attributable/redacted actions and rejected attempts, correlated errors, safe diagnostic bundle and installed-version runbooks |

The management API validates permissions and commands independently of the visible
menu. It persists configuration revisions and long-running job receipts, exposes
partial application honestly, and records attributable audit events. Bootstrap
security/network roots remain deployment-owned; the panel does not silently rewrite
mounted files or bypass explicit locks. External publication and exports cannot
be undone by rolling back a local setting. Any exceptional destructive operation
must describe the concrete consequences and its own authorized scope.

## Repository and deployment decision

Use a monorepo during the Rust rebuild. Keep build/deploy boundaries independent:

```text
crates/                      Focused reusable Rust model, protocol and analytics libraries
apps/dashboard/              SvelteKit + TypeScript, static build
apps/admin/                  same frontend stack, separate private static bundle
apps/site/                   Astro + Starlight, static site and blog
packages/api-client/         generated HTTP types and small typed client
packages/ui/                 shared accessible components and form conventions
packages/brand/              shared assets and design tokens
contracts/http/              versioned OpenAPI/schema artifacts
contracts/management/        private typed command, revision and job schemas
docs/                        canonical English engineering and operator content
packaging/compose/           local/private/public deployment compositions
```

This is a recommended future layout, not a scaffold created by this planning
stage. A contract, backend change, dashboard state and documentation example can
be reviewed together. Separate release artifacts retain independent deployment.
The historical separate-dashboard-repository decision is intentionally replaced:
it provided organizational isolation but would add cross-repository coordination
before there are independent teams. A later split can preserve the HTTP contract.

The repository is temporarily `bigbrotr-rs` and private. The brand, executable and
product remain **BigBrotr**. The final repository name becomes `bigbrotr` after the
transition. Do not publish links to a private planning repository as working
installation instructions. At cutover, update source, issue, edit, package and
release URLs through a single site configuration and check old links explicitly.

Choose SvelteKit/TypeScript for the dashboard: typed routes, shared layouts and a
static adapter give the application structure without an operator Node server.
Use the browser's fetch/cancellation mechanisms and a small typed read client;
add a query-cache dependency only if the implemented interaction requires it.
Use native HTML forms and tables, and lazy-load Apache ECharts for chart pages
with text/table alternatives. Neither library choice is a measured performance
claim. [SvelteKit documents the static adapter](https://svelte.dev/docs/kit/adapter-static)
and explicitly warns about [SPA startup and SEO costs](https://svelte.dev/docs/kit/single-page-apps).
Prerender fixed home/help/method pages; dynamic instance results remain client
queries. Public search-engine landing content belongs on the fully rendered site.

Keep Astro/Starlight for the website. Local schema-validated Markdown and static
output fit documentation and editorial content. Existing content is migrated with
corrections and historical labels. Prefer Starlight's native accessible navigation
and preferences; customize only to meet an actual product need, avoiding a second
hand-maintained copy of theme behavior. [Astro content collections](https://docs.astro.build/en/guides/content-collections/)
and [Starlight search](https://starlight.astro.build/guides/site-search/) support
these requirements without a live application backend.

The deployment default is:

1. The published release includes the Rust image/binary, static dashboard and
   administration archives, and a compatible deployment bundle. Operators download artifacts;
   they do not install a JavaScript toolchain to run an instance.
2. The standard local deployment serves the dashboard assets through the Rust
   HTTP process when the local dashboard/read surface is enabled. It binds to
   loopback by default. Public exposure remains a separate explicit setting.
3. A public reverse proxy routes dashboard assets and `/v1/` to that read process.
   It routes neither `/metrics` nor private health/debug/administration endpoints.
   All databases and operations listeners remain private.
4. Advanced operators may put static assets on another host and configure a
   specific API origin with an allowlisted CORS policy. This is an alternative
   deployment of the same API client, not a browser-to-database path.
5. Non-secret runtime `app-config.json` supplies the API base URL, instance label,
   documentation URL and deployment base path. Changes do not require rebuilding
   the dashboard. Secrets and privileged API keys never enter this file.
6. The local management listener serves the private panel and management API with
   mandatory authentication. A one-time local owner-enrollment flow cannot create
   an unauthenticated exposure window. Remote management uses a separately secured
   private origin/network and TLS. Its static bundle can be omitted independently
   of the public dashboard; neither bundle contains privileged credentials.

Serving bundled dashboard files is optional. Rust library/backend development can
run without Node or a website build; release CI is responsible for assembling the
compatible frontend artifact. A website build has no live archive credentials.
Private previews require hosting access control: `noindex` is not authentication.

Direct SQL is an operator/research notebook interface, if the chosen storage
architecture supports it, documented separately with read-only credentials.
Remove the old “direct database dashboard mode” from first-release scope. A
browser cannot safely hold a database credential, and an extra server adapter
would duplicate authorization, limits and semantic contracts. A later server-side
SQL connector must return through the same read policy rather than bypass it.

## First deployment and useful configuration

The owner has established that every service and the whole product matter equally.
Implementation order follows dependencies; an earlier slice does not lower the
quality required of later work or remove it from acceptance. The first launch
therefore presents a coherent complete product rather than asking the owner to
choose which service deserves to work first.

The recommended canonical local deployment starts the required storage, relay
lifecycle, acquisition, indexing, derivation and ranking components with bounded
defaults. It makes the local dashboard/read API and authenticated administration
panel available on loopback for first verification. Import/export/backup are ready
operator commands and panel jobs rather than jobs
that copy arbitrary data on startup. Integrity maintenance has its own explicit
schedule. Publishers, DVM, MCP, ContextVM and publicly exposed read listeners
require deliberate enablement and any relevant signing identity. MCP and ContextVM
are separate optional read adapters; ContextVM remains experimental under its
[gateway contract](../specification/gateway-contracts.md). A disabled public
endpoint is a deployment choice, not incomplete implementation. Relay-only, archive-only and
custom examples demonstrate independent service selection without defining lower
quality products. No configuration questionnaire is required before useful data.

All canonical archives retain full signed events, including signature and native
tags/content. Remove the `heavy_fields`/body-loss configuration axis and former
lossy LilBrotr/IndexBrotr presets. BigBrotr remains one product, executable and
image. LilBrotr may name a documented recipe for full signed archival storage with
provenance disabled; this intentionally differs from the historical lossy meaning.
SocialBrotr may name a social-event-filter recipe with provenance independently
selectable. Recipes expand into ordinary visible settings, never hidden runtime
semantics or permanent capability identities. Storage savings come from filters,
compression, hot/cold placement and optional indexes/provenance, with their
actual consequences visible. Slim analytical exports remain useful, but they are
typed projections and cannot be advertised or imported as complete verifiable
Nostr events. Low TrustRank never automatically prunes the archive.

The launch guide follows one continuous journey:

1. Explain prerequisites using measured supported host sizes and selected storage
   components. State that startup does not mean full historical synchronization.
2. Download a versioned release/deployment bundle. Initialize secrets locally and
   use bundled defaults plus a small override file for storage paths and selected
   services. No database-console edits or image rebuild is required.
3. Run configuration validation and a preflight that names required dependencies,
   effective service set, public binds, expected storage policy and finite work
   budgets. An explicit missing file, unknown key, contradictory axis or disabled
   dependency produces an actionable error before work begins.
4. Start through the shipped command/Compose entry point. The terminal prints the
   local dashboard URL, secure owner-enrollment entry point and a status command.
   Enroll the owner locally, then the private overview identifies the instance,
   shows acquisition/derivation progress, and links to useful data and management
   actions. The public/research dashboard remains independently accessible under
   the configured read policy.
5. Verify one measured relay or one admitted event, depending on the selected
   starter. A system with no reachable external relay reports that fact without
   fabricating data or blocking local diagnostics.
6. Link to service selection, import, sizing, backup and publishing instructions.
   Public API/dashboard/DVM/relay/MCP/ContextVM/publisher enablement is explicit,
   with signing keys required only for the services that sign.

The settings documentation separates policies from tuning. Policies include
retained event scope, provenance retention, public resource exposure,
serving of current versus historical versions, outbound networks and backup
destinations. Tuning includes finite concurrency, query limits, memory grants and
cadence. Implementation details without useful operator intent remain internal.

All config keys need a consumer, a documented default, validation, applicability,
units and change behavior. `config show` is redacted, records where effective
values came from, and can emit a machine-readable report. A separate initializer
produces a usable minimal config; a diagnostic report is not misrepresented as a
round-trippable configuration file. The reference is generated from the same
schema/default definitions and checked against release entry points.

Presets are recipes, not permanent feature identities. The UI describes effective
capabilities and covered intervals. A later provenance policy change cannot create
missing historical observations; report new coverage boundaries and any backfill
state. Body availability is a serving/hot-cold placement property, not permission
to discard the signed archival event. Adding a service can require dependencies
and resources, which preflight must explain rather than enabling extra work silently.

Legacy migration distinguishes retained source truth from old index layout. An
older lightweight instance may still have complete original events in its journal;
verify and import those, then rebuild selected indexes. If it holds only metadata,
report the incompatible corpus and provide a standalone DuckDB analytical guide
or explicit best-effort refetch path. There is no in-product projected-data importer.
Do not fabricate content/signatures, label the result
complete, or assume old historical provenance can be reconstructed. Old preset
names link to this explanation and their explicit replacement configuration.

## Capability and answer-quality contract

`GET /v1/capabilities` is the proposed discovery route. The canonical HTTP contract
must adopt or consistently rename it before implementation. Discovery and resource
authorization share the backend registry; a frontend menu is never an access gate.

The response advertises the supported API major version, capability schema version,
instance identity, capability revision and only resources visible to that caller.
Each visible resource describes its query schema, supported fields/dimensions,
limits, state, observation/snapshot basis and coverage summary. Do not expose
secret configuration, hidden resources, key identities, topology or internal host
names to explain why another resource is unavailable.

| Dimension | Required meaning |
|---|---|
| Implemented | The selected build has a working handler and answer shape, not only a registered name |
| Exposed and authorized | This operator permits this caller to read it under the current policy revision |
| Semantically supported | Required fields/facts exist for this type of query; supported kinds/tags/time dimensions are stated |
| Ready | A usable source view or fully published derivation exists; an enabled producer is insufficient |
| Coverage | Retained kinds, time intervals, source relays/imports and field completeness; gaps and unknown bounds remain explicit |
| Freshness | When observations were made, when the result was computed and how current the underlying read view is |
| Limits | Page/result limits, supported sort/filter forms, query-cost bounds and any export restrictions |
| Provenance | Data/algorithm version, public completed snapshot identifier where supported, and an observation-method reference |

A resource can be operationally ready but cover only some requested periods. A
historical result can be old and still correct for its stated snapshot. Readiness,
coverage and freshness are separate concepts; none becomes a single green badge.

Every query answer carries the effective query, covered scope, warnings or refused
terms, a stable paging/snapshot basis, and exact/approximate count metadata. If the
server narrows a request, the UI says what changed before the user treats it as the
answer. A failed dependency, no completed derivation, query timeout or budget limit
never becomes a successful empty result.

| Resource/data state | Navigation and page behavior |
|---|---|
| Unsupported or not exposed | No menu item; direct links show a generic unavailable page and supported alternatives; no protected request is attempted |
| Supported, no usable data yet | A preparation status may appear on instance overview; the direct page explains collection/derivation progress, not a zero chart |
| Valid empty answer within known covered scope | Render “No matching records” and the selected scope; distinguish this from unknown coverage |
| Partial field/time/source coverage | Render supported data with coverage explanation; suppress metrics whose denominator is unknown |
| Stale last usable data | Keep a dated view if permitted, label it stale and state the last successful observation/run; never silently substitute it for current health |
| Temporary outage | Preserve the user's filters; show retry/error state; any retained result keeps its timestamp and a visible stale label |
| Capability/auth revision changes | Revalidate, cancel now-ineligible requests, discard unauthorized cached results and recompute navigation |
| Unknown API/capability major version | Show a compatibility error with supported versions and operator docs; fail closed for data views |

The browser uses in-memory result caching partitioned by API origin, caller and
policy/capability revision. Do not persist event bodies in a service-worker cache
or local storage by default. Responses enforce current exposure policy even when
a completed snapshot remains addressable. HTTP caching for permitted public
resources must include invalidation behavior for policy/takedown changes.

Do not conflate quota and authentication. Initial public reads may be anonymous
and bounded. A private deployment uses real authentication through the chosen
gateway/API policy; unauthenticated requests cannot acquire higher access merely
by receiving the standard rate limit. No privileged key ships inside a public
JavaScript bundle. Honor `429` and `Retry-After`, stop automatic retries when
offline, and propagate cancellation to the backend's query budget.

## Dashboard information architecture and pages

Use an instance header with name, scope and last data update, followed by navigation
for Overview, Relays, Events, Analytics and About this instance. Only supported
subsections appear. Links to Documentation and API reference are always available.
Status is a compact route rather than a replica of Grafana. Search controls must
state whether they search event identifiers, a resource table or full event text.

The resource names below are semantic contract requirements; they do not imply
database table names. Every page has a list/detail/table alternative before an
interactive map or graph is added.

| Page / proposed route | User task and content | Capability and coverage conditions |
|---|---|---|
| Overview `/` | Understand this instance; choose relay search, event lookup or analysis; show only meaningful summary cards | Instance descriptor plus whichever summary resources are ready; show initialization if none is ready |
| Relay directory `/relays` | Filter/sort by reachability, network, NIPs and measured characteristics; copy a relay URL | Relay directory resource; filters appear only for stored facts; old/unknown health is distinct from offline |
| Relay detail `/relays/{relay-id}` | See canonical URL/aliases, observations, supported capabilities, uptime basis and latest measurements | Directory lookup; each health, geographic, availability or score panel independently requires its resource and timestamps |
| Relay comparison `/relays/compare` | Compare a bounded selection on a common measurement window | Comparable relay facts; reject mismatched scopes or label them explicitly; no unlimited pairwise browser computation |
| Network overview `/analytics/network` | Trends by network, software, supported NIPs, country/ASN and observed relay counts | Completed relay aggregates; geography/ASN only when those facts exist; estimated endpoint clusters are not “true distinct operators” |
| Events `/events` | Query IDs, authors, kinds, time and supported tags; copy/share the query; inspect count exactness | Bounded event-index resource; show metadata even if full bodies are unavailable; cursor pagination preserves stable order |
| Event detail `/events/{event-id}` | Inspect identifier, author, kind and timestamps; read raw signed event and single-event download where allowed | Metadata lookup; body/tags/signature panels require those retained fields for this event and public serving permission |
| Observations `/events/{event-id}/observations` | See which relays supplied this event, first observations and subsequent checks | Observation/provenance resource for this event; label observer order, clock basis and acquisition method; never infer network propagation latency from backfill |
| Availability history `/events/{event-id}/availability` | See positive/negative checks at dated instants or intervals | Repeated availability observations and their query context; first-seen alone supports only a first-observed fact |
| Coverage and overlap `/analytics/coverage` | Compare observed corpora, overlap, unique contributions and acquisition gaps | Published provenance/coverage aggregates, denominators and comparable scopes; report uncertainty and sampling limits |
| Selective availability `/analytics/availability` | Investigate observed serving differences by relay, event scope or author | Comparable repeated checks/coverage facts and published method; evidence does not automatically label an operator a censor |
| Archive trends `/analytics/events` | Volumes by kind, author and creation/acquisition time, with count methodology | Completed event aggregates; event creation and ingestion timelines are separate; exact totals require declared exactness |
| Social analysis `/analytics/social` | Follow/mute graph summaries, activity/cohorts and social-kind trends | Each analysis names its required kinds, fields and completed facts; all-kind/custom archives qualify when they contain those inputs |
| Scores `/analytics/scores` and subject detail | Explain a score, its model, seed/input basis, update and missing terms; compare like with like | Completed rank results plus method/version/coverage metadata; no score displayed from merely enabling Ranker; missing score is not zero |
| Content search `/search` | Search supported event text with bounded filters and highlighted matches | Exposed full-text resource and text coverage; snippets obey serving policy; plain lookup remains usable when full text is absent |
| Version/deletion history `/events/{event-id}/history` | Examine superseded versions or deletion evidence in a research context | Explicitly exposed historical/current-state resources, retained bodies/versions where necessary, and operator serving policy; unavailable under normal current-only serving |
| Published datasets `/datasets` and detail | Find a released corpus; inspect schema, provenance, scope, hashes and reproduction recipe | Operator-published export manifest/resource; public browsing cannot start an unbounded export or backup job |
| API and methodology `/about` | Learn instance operator/contact, limits, data sources, semantics and exposed HTTP/relay/DVM/MCP/ContextVM links | Exact-version supported adapters and subsets; ContextVM experimental; sanitized descriptor, never private operations credentials/endpoints |
| Public status `/status` | Know if exposed read products answer and how old data is | Sanitized status resource; measured uptime window/target only if actually operated and recorded; no storage host names, logs or raw metric labels |

Optional visualization does not create evidence. A timeline of sightings is more
honest than an animated global propagation map when acquisition occurred by
historical backfill. Geographic maps describe measured hosting endpoints at
documented precision, not an author's location. A social account with no observed
follow list is unknown, not socially disconnected. A signature verifies the event
under a key; it does not verify a person's identity or the truth of its content.

## Core journeys and interface behavior

**Choose a relay.** Open the directory, narrow to needed network/NIPs, compare a
small selection over the same window, open a detail panel, inspect timestamp and
measurement method, then copy the relay URL. If a fact is missing, leave that
column unknown and make the missing condition legible. A score never hides the
underlying measurements or silently ranks an unmeasured relay last.

**Investigate an event.** Paste a supported hex/Nostr identifier, validate locally,
look up metadata, inspect the permitted raw event, and open observations. Show
`created_at`, acquisition time and per-relay observation time separately. Single
event download preserves the signed event when available; a transformed CSV row
is not described as a byte-faithful original. Identifier parsing is the same
contract used in SDK examples. No private signing key is requested.

**Produce a research result.** Select scope and time, inspect exactness and coverage,
compare a stable completed result, then download allowed result rows with metadata
or follow a published corpus manifest. Copy a citation containing instance ID,
resource/query, snapshot or export digest, method version and retrieval time.
Saved query links encode public filters, not credentials. Unless a retained
snapshot/export exists, label a link as a live query whose future answer can change.
Retained snapshot tokens may address only published completed results and expire
explicitly; they cannot select partial or unauthorized runs. This deliberately
reconsiders the Go API's latest-run-only limitation for citable research.

**Diagnose and operate an instance.** The public reader sees an honest availability
message and data timestamp. The operator opens the private panel, identifies the
stalled role and backlog, pauses or resumes eligible work, and follows the matching
runbook or optional Grafana detail. Equivalent CLI commands work when the browser
is unavailable. User-facing errors include a safe request identifier for
correlation, not a SQL query or connection string.

**Change configuration and finish a job.** The operator edits a useful setting,
previews its diff and resource/coverage effects, then applies a versioned revision.
The panel distinguishes saved from applied and reports conflicting edits. The
operator starts an export or backup, closes the browser, reopens its persistent
job receipt and sees committed progress. Cancellation preserves already committed
work. Secret fields accept protected references without returning stored values;
restricted actions require the appropriate role, as defined by the
[private administration contract](private-administration.md).

**Integrate an application.** Begin with a capability request, make a bounded query,
walk a continuation token, inspect completeness/freshness, handle `429`, and
cancel. The same guide links to ArchiveRelay, DVM, native MCP and experimental
ContextVM mappings where their protocols can represent the operation. The
gateway contract fixes each adapter's negotiated version, transport, encryption,
authentication, supported tools and cancellation behavior. ContextVM does not use
the DVM profile. HTTP-only answer-quality metadata is documented rather than
silently dropped in protocol examples; disabled adapters are labeled unavailable.

Queries are shareable through stable URL parameters. Back/forward and page reload
restore filters and selected time ranges. Expensive searches run on explicit
submission, not every keystroke. Navigation aborts old requests and late responses
cannot overwrite the new query. Tables are paginated before adding virtualization;
full-corpus aggregation belongs on the server. Graphs request bounded subgraphs,
not every edge. Polling follows advertised cadence/cache policy, pauses in hidden
tabs, and does not restart a rate-limited request in a tight loop.

Render untrusted relay names, profile text and event content as escaped text.
Safe highlighting does not turn event text into HTML. Raw HTML, scripts, executable
URLs and automatic external media/embed loads are disabled by default. External
media is an explicit action; leaving the instance is visible. Large content has a
bounded preview and separate permitted download, keeping an event from freezing
the page. Charts never assemble HTML tooltips from unescaped event text.

## Website, documentation and blog architecture

The website's primary navigation is Product, Get started, Documentation, Developers,
Blog and Releases. An “Explore an instance” action appears only with a real
maintained destination. Use case pages explain outcomes and supported evidence,
with links to the relevant dashboard view or deployment guide. Do not make visitors
read implementation package diagrams before understanding what BigBrotr does.

| Destination | Required content and next action |
|---|---|
| `/` | What BigBrotr observes/archives, who benefits, honest example outputs, release state; install or explore |
| `/product/` | Discovery, monitoring, acquisition, research, scores and access, each tied to an actual supported capability |
| `/use-cases/relay-operators/`, `/researchers/`, `/developers/` | Concrete jobs, required data and limitations; direct links to corresponding guides |
| `/get-started/` | Supported installation paths and minimal prerequisites; one recommended local path plus advanced alternatives |
| `/docs/` | Task-oriented guide index and visible documentation version |
| `/docs/<version>/getting-started/` | Install, enroll the local owner, first data, validate health, choose services, customize scope, expose a read surface |
| `/docs/<version>/concepts/` | Durability, observations versus claims, coverage, freshness, current/archive policy, service boundaries and scaling |
| `/docs/<version>/services/` | All fifteen responsibilities, dependencies, input/output, config/defaults, lifecycle/failure/resource model and examples |
| `/docs/<version>/operations/` | Sizing, imports/exports, backup/restore verification, repair, retention/takedown, upgrades/rollback constraints, alerts and transport deployment |
| `/docs/<version>/administration/` | Panel/CLI parity, roles, configuration sources/locks/revisions, persistent jobs, token revocation, audit and recovery without a browser |
| `/docs/<version>/reference/` | Generated CLI/config, HTTP schemas and errors, NIP compatibility, artifact/manifest formats and metrics definitions |
| `/developers/` | Choose HTTP, Nostr relay/DVM, native MCP, experimental ContextVM or a Rust library; supported versions/subsets, bounded examples and compatibility policy |
| `/developers/api/` | Versioned OpenAPI download, typed resources, auth/quotas, pagination, errors, timestamps, exactness and capability examples |
| `/developers/sdk/` | Supported Rust primitives and TypeScript client, examples, release compatibility and links to generated Rust API docs |
| `/blog/` | Dated releases, tutorials, research and design notes with filters; RSS/Atom feed |
| `/releases/` and release detail | Exact artifacts, checksums/signatures if shipped, supported platforms, compatibility, changes, migration and known limitations |
| `/roadmap/` | Shipped, experimental and planned capabilities separated; no roadmap feature presented as available |
| `/about/` | Project/community links, contact, license/attribution and contribution path |
| `/legacy/` | Historical documentation with original implementation/version, read-only instructions and migration links |

`<version>` means a supported released documentation line, not an implementation
language marketing brand. `/docs/latest/` resolves to the latest stable line;
`/docs/next/` is clearly unreleased and must not replace the default installation
guide. Before a first stable release, prerelease state is visible. Release pages
pin exact artifacts and reproducible documentation snapshots even when corrections
are later applied to a supported documentation line.

The default search searches the chosen supported documentation line. Blog and
legacy results are labeled and optionally included, not mixed into installation
answers without context. Test against a production build because Starlight search
is built at publication time. Native docs search and event full-text search are
different products with different labels, backends and data policies.

Each service guide uses the same practical template: what the service accomplishes;
when to enable it; input/output and prerequisites; minimal working configuration;
defaults and useful overrides; lifecycle and recovery; limits; monitoring; and a
verified example. The reference can contain deep technical detail, but the user's
first action stays near the beginning.

## SDK and contract publication

The HTTP OpenAPI document is checked into/generated into the versioned contract
artifact and validated against actual Rust responses in release tests. Generate
TypeScript wire types and the reference from it. The small client preserves
answer-quality metadata, cancellation, continuation tokens and typed errors rather
than converting every answer into an unqualified list. A schema file by itself
does not prove runtime compatibility. [OpenAPI defines this language-independent
interface role](https://www.openapis.org/what-is-openapi).

Publish Rust libraries only for useful, storage-independent primitives after their
contracts are stable: event verification, relay URL/transport policies, probing,
filters, and acquisition mechanisms where appropriate. Keep runtime internals
private. Library examples must state network/durability assumptions and show
cancellation and bounded work; do not repeat historical unconditional completeness
claims. Rustdoc and runnable examples are linked from the developer site.

Provide curl, TypeScript and Rust HTTP examples for discovery, event lookup,
pagination, counts, freshness, and an unavailable resource. Python researchers
receive a plain HTTP/notebook example and export reading guide; a separately
maintained Python SDK is not required for the first release. Nostr examples cover
the explicitly supported ArchiveRelay/DVM contracts and the separately encrypted
experimental ContextVM transport. Native MCP and ContextVM examples discover tools,
use authorized bounded queries and cancellation, and preserve their supported
read metadata. Pin each client/protocol version, label unsupported subsets, and
execute every advertised example against its packaged adapter. These examples
share read semantics; they do not imply interchangeable transports or full HTTP
metadata on every protocol.

## Content maintenance and historical migration

Canonical prose lives once in repository Markdown. Site ingestion/adapters render
that content without a manually synchronized second copy. Generated CLI/config/API
reference is rebuilt from its source definitions. Brand assets/tokens can be
shared; application components and editorial layout need not be a common UI
framework or large design-system package.

| Content type | Required ownership/metadata | Maintenance trigger |
|---|---|---|
| Guide | Owning service/workstream, supported versions, verification scenario/date | A command, config, deployment or behavior change in the same PR |
| Generated reference | Owning schema/CLI/API contract and generator version | Contract change; generation diff and runtime example validation |
| Release note | Release owner, version/date/artifact IDs, upgrade and compatibility notes | Artifact promotion; known issue/correction after release |
| Research article | Author, date, corpus/query provenance, method version, limitations and revision log | New evidence or corrected analysis; do not silently rewrite original results |
| Tutorial | Author, tested product version and runnable example | Supported version changes or broken example |
| Historical page | Original date/implementation, archived label, successor link | Broken links, factual correction or security-sensitive outdated advice |

Preserve existing blog slugs and authorship where possible. Add a historical
Python implementation notice to relevant tutorials and architecture articles,
link to the Rust replacement, and record corrections. Comparative articles require
fresh source/version review before being reused as current product positioning.
The compromised-key study is evidence that citable corpus work matters; it does
not authorize automatic republication of sensitive raw datasets.

Create a route migration manifest covering every current authored page and generated
blog route. Move old docs to a clearly labeled legacy tree and redirect former
docs URLs to their historical equivalents or a precise new guide. Keep blog URLs
stable. Repair the self-hosting article's missing guide/incorrect blog links.
Inspect canonical URLs, feeds, sitemaps, images, Nostr identity metadata and old
external documentation domains at cutover. An unavailable old reference gets a
useful explanation; it must not quietly land on an unrelated home page.

English is the source language for repository content, UI and first publication.
Use message keys/plurals and locale-aware date/number formatting in the dashboard;
provide an explicit timezone display with UTC available. Keep identifiers and
query values lossless. Starlight's root English locale can later add translated
routes with [fallback notices and RTL support](https://starlight.astro.build/guides/i18n/).
Do not show a language choice until someone maintains that translation. Automated
translation is a draft, not evidence that commands or operational warnings were
reviewed in that language.

## Accessibility and browser acceptance

Target WCAG 2.2 AA for shipped journeys. Native landmarks, headings, links,
form labels, focus visibility, keyboard use and text alternatives are baseline
design requirements. Charts need readable summaries and equivalent data tables;
color alone cannot encode health or categories. Support reduced motion, narrow
viewports, text zoom and high contrast. A graph must not be the only way to select
a relay or understand an event's observations. [WCAG 2.2](https://www.w3.org/TR/WCAG22/)
provides the criteria; a numerical score or axe scan is not conformance proof.

The matrix below is release work, not tests already executed. Prefer tests of
observable user behavior and actual deployed entry points. Unit tests support
complex query/capability logic; a coverage percentage is not an acceptance gate.

| Scenario | Execution and evidence | Required result |
|---|---|---|
| Clean local adoption | Release artifacts on clean supported Linux amd64 and arm64 environments; execute the documented commands | Start selected services, preserve volumes, expose only declared loopback endpoints, reach first meaningful data or an honest source-unavailable state |
| Independent service selection | Relay-only, archive without Monitor, custom social subset, full input set, and public services disabled | Service dependencies are explained; UI uses working resources; no imaginary always-on relay/social/ranking panels |
| Storage/capability combinations | Full signed archive with provenance on/off, optional indexes, hot/cold bodies, selective public exposure, historical backfill and provenance policy changes | Field/history panels honor actual serving/data coverage; body-less legacy corpora are refused as full-event imports and remain explicitly typed projections |
| Empty/warming/partial/stale/down | Real API fixtures plus real backend derivation interruption/recovery | Distinct states; zero means a valid empty result; old complete results retain timestamps; partial runs never serve as complete |
| Exposure/auth change | Reload capabilities and revoke resource permission during an open page/session | Server denies forbidden reads; UI cancels/discards affected results; no privileged key in assets/logs; private endpoint paths cannot route publicly |
| Private owner enrollment and roles | Fresh local setup, expired bootstrap/session, viewer/operator/admin actions and revoked token | No unauthenticated management interval; server enforces permissions; secrets stay redacted; rejected and accepted actions are auditable |
| Configuration transactions | Two editors update the same revision; modify locked setting; apply a drain/restart-required change | Conflict is explicit; locked source is explained; saved/applied/failed states match reality and survive process restart |
| Durable management jobs | Start import/export/backup in panel, close/reopen tab, cancel and restart a worker | Persistent receipt and committed progress remain accessible; cancellation boundary and restore verification are truthful; matching CLI command uses the same contract |
| Browser engines | Playwright Chromium, Firefox and WebKit on production static output | Relay/event/research and private configuration/job journeys, navigation, downloads and deep links pass across engines |
| Actual device/assistive use | Safari on a supported Apple device, one current Android browser, VoiceOver/Safari and NVDA/Firefox or equivalent maintained test environments | No touch, native download, focus or screen-reader failure hidden by desktop engine emulation |
| Responsive and accessibility | Keyboard-only traversal, 200% and 400% zoom, narrow mobile layout, dark/light/high contrast, reduced motion; axe in representative states | All actions reachable and labeled; content remains usable; chart alternatives complete; automated findings and manual observations resolved or documented as release blockers |
| Query navigation | Open/share/reload a filtered deep link; Back/Forward; switch filters while a response is delayed | Restored query/scope; stale response cannot replace the latest selection; invalid identifiers produce a clear local error |
| Large and hostile inputs | Long/malformed identifiers, oversized bodies, HTML/script-like relay/profile content, unusual Unicode, bidirectional text, external URLs | Bounded display, escaped text, no script execution or automatic third-party requests; full identifiers remain copyable |
| Pagination and exactness | Real dataset with ties, concurrent ingestion, paging expiry and approximate/exact counts | No unexplained duplicate/skip from changing order; stable snapshot or explicit expiry; approximate/clamped metadata is visible and retained in downloads |
| Limits and cancellations | Expensive query, user navigation, disconnected tab, timeout, 429 and Retry-After | Backend work respects cancellation/budgets; UI offers a narrower request/retry with no retry storm |
| Independent frontend deployment | Same-origin and allowlisted cross-origin, root and configured subpath, direct deep-link reload, rolling asset/API replacement | Correct assets/CORS; old assets remain loadable for active clients or prompt an intelligible refresh; incompatible API versions fail clearly |
| Research reproducibility | Export one fixture corpus and compare download hashes/metadata; reopen retained result or expire it | Citation identifies exact export/snapshot where promised; live query never masquerades as an immutable dataset |
| Website offline/static behavior | Production build with no archive/API available and JavaScript disabled | Core docs/blog navigation and reading work; search degradation is explained; no backend dependency for publication |
| Docs and blog publication | Build/crawl all routes and redirects; run search queries; validate feed/sitemap/canonical metadata, code examples and release downloads | Current commands point to matching artifacts; legacy results are labeled; old links have intentional destinations |
| Operations separation and recovery | Disable public read process while private diagnostics run; restart after backend/storage failure | Public status reflects outage; operator can diagnose through private channels; recovery does not require exposing them |
| Resource behavior | Record cold/warm transfer sizes, route/render time, memory and long tasks on defined ordinary hardware/network plus realistic API result sizes | Evidence supports responsive core journeys; unresolved regressions or unbounded client work block release; do not invent a universal latency/bundle threshold before measurement |

Playwright's [browser support](https://playwright.dev/docs/browsers) is useful for
engine coverage but is not equivalent to testing every shipping device/browser.
Its [accessibility guidance](https://playwright.dev/docs/accessibility-testing)
also requires manual assessment beyond automation. ECharts' [ARIA/decal support](https://echarts.apache.org/handbook/en/best-practices/aria/)
must be explicitly configured and does not replace the equivalent data table.

## Delivery phases and release plan

Preserve original phases 1–9, with phase 0 as specification work, and add a distinct
website/documentation/blog phase. The site work can progress alongside backend
phases; a release waits for its applicable user journeys and documentation.

| Phase/work package | Surface deliverables | Exit evidence |
|---|---|---|
| Phase 1: runtime foundation | Typed config/defaults, service selection, status/doctor model, private/public binding distinction | Real CLI/preflight shows effective behavior and actionable refusal |
| Phase 2: storage spine | Full signed archival fidelity, provenance/retention and hot/cold vocabulary, durability/restore contracts and legacy slim migration | Fields and metadata represent historical provenance and serving coverage honestly; projections cannot masquerade as signed events |
| Phase 3: ingest | Acquisition/import-origin progress, coverage/reconciliation semantics and cancellation | Operator can distinguish observed events, indexed visibility and verified scope |
| Phase 4: relay lifecycle | Stable relay IDs/URLs, field-specific observations, monitoring coverage | Directory/detail resources have real data and timestamps |
| Phase 5: derivation | Complete result publication, analysis/score methodology and provenance | Partial/stale/unknown cases represented; algorithm and dataset bases reportable |
| Phase 6: read side | Versioned HTTP/capability schemas, bounded semantic resources, authorization, pagination, protocol mappings and client types | Actual responses match contract; no registry-only capabilities or ignored exposure settings |
| Phase 7: publication | NIP-66/NIP-85/DVM publication docs and identity/destination behavior | Documented opt-in and signing/recovery behavior exercised through public protocols |
| Phase 8: operations and packaging | Local deployment bundle, authenticated management commands/roles/revisions/jobs, diagnostics/alerts, backups/restores, import/export and upgrade/exposure guides | Clean-host enrollment, failure/restore drill, multi-platform artifact checks, durable command behavior and private/public separation |
| Phase 9: dashboard foundation | Static artifact, client/capability handling, navigation, overview, directory, event lookup, public status | First relay-selection and event-investigation browser journeys across capability states |
| Phase 9: dashboard analysis | Observations/coverage/availability, trends, social analysis, scores, optional full text and policy-gated historical views | Each page's concrete resource/coverage requirements met and browser matrix exercised; backend-unready pages remain unavailable |
| Phase 9: private administration | Separate panel bundle/listener, all management pages, shared accessible components, config preview/application and durable job navigation | Owner-enrollment, role/revocation, concurrent edits, disconnected browser and recovery journeys pass through the real management API |
| Phase 9: dashboard release | Deployment/base-path modes, accessibility, error/cancellation behavior, compatibility and research citations | No advertised page depends on a missing answer path; measured browser and operator evidence captured |
| Additional website/docs/blog phase | Fresh product/guide/developer IA, all service docs, versioning, historical migration, blog/feed, releases, redirects, ownership and English/i18n structure | Production build/crawl/search, tested examples, release links, accessible navigation and complete historical route inventory |

The added website phase is a publication deliverable separate from dashboard
phase 9. The old Go “Phase 10 consolidation” document is historical material;
the fresh plan must name this new phase unambiguously rather than accidentally
inheriting that old scope or dropping publication work.

Deliver useful dashboard slices without pretending later resource groups already
ship. Full-text engine work gates content search only. Absence of repeat availability
measurements gates availability history only. A release note explicitly lists
available groups and remaining requirements; an initial slice does not mark the
complete planned product finished.

Promotion sequence: private preview with deterministic fixtures; integrated
preview against real Rust resources and isolated relays; release candidate with
clean-host/upgrade/browser/docs evidence; operator-selected public deployment;
website stable release and legacy redirects. Preview content and artifacts remain
private until their publication is authorized. Public demo hosting and its service
commitment remain an owner product decision, not an implicit result of deploying
the static site.

Version artifacts together in the main release manifest: backend, read and management
API schema revisions, dashboard/admin asset builds, documentation line and deployment
bundle. State the supported backend/frontend ranges explicitly; preserve assets used by active
clients during frontend rollout. Website/editorial corrections can deploy without
restarting an archive. Binary downgrade is not promised across storage migrations:
the release runbook must describe actual compatibility and recovery requirements.

Use the selected defaults throughout acceptance: independent self-hosting with
local verification, explicit per-resource public exposure, explained named score
profiles, and checksummed published exports or explicitly retained/expiring query
snapshots for immutable citations. A live query URL is not an immutable result.
No official hosted-instance uptime commitment is selected; creating one remains
a separate future publication/operation decision. The
[planning register](../planning/questions.md) and current contracts own these
choices, rather than the earlier research question candidates. Full history with
deletion-aware ordinary reads, equal product quality, full signed archival events,
private administration and private planning remain established requirements.

## Content search and reconciliation detail

The [search and reconciliation contract](../architecture/search-and-reconciliation.md)
defines event-text syntax, ranking, index coverage and disable/rebuild behavior.
Search is part of the complete product; an optional deployment index is not an
unspecified future feature. The private UI manages that derived index and shows
its cost/progress. Document NIP-77 archive downloads alongside REQ, COUNT and
NIP-50, including provenance independence and the difference between negotiation
and a frozen corpus export. Website documentation search remains a separate
static-site feature.
