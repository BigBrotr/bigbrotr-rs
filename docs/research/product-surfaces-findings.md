# Product surfaces: source findings and reconsiderations

Date: 2026-09-11. Status: design research, not implementation or a running-site audit.

The proposed design is in [Dashboard and website](../product/dashboard-and-website.md).
This document distinguishes inspected source, conclusions drawn from that source,
and proposed changes. The neighboring repositories remained read-only; no website
build, deployment, browser session, Git operation, or product prototype was run.

Subsequent owner decisions govern the recommendations: all services and product
areas matter equally, so implementation order follows dependencies rather than a
reduced feature priority; the planning repository stays private; and the new
archive retains full signed events and signatures. The former payload-loss axis
and former lossy LilBrotr/IndexBrotr presets are retired. LilBrotr can now name a
full-archive/no-provenance recipe; SocialBrotr can name a social-filter recipe
with provenance independently selectable. Neither adds hidden runtime semantics.
Provenance, filters, index selection, services and public exposure remain meaningful
choices. The owner also selected full history with deletion-aware ordinary reads
and a real private administration interface; its current contract is in
[Private administration](../product/private-administration.md). Historical slim
formats and earlier operational limitations below are source evidence, not new
product constraints.

## Evidence scope and read coverage

The Go reference is the frozen copy under `.research/source-snapshots/go/`, with
provenance recorded in [source-inventory.json](source-inventory.json). References
below use paths relative to that copy. Old process instructions and old technology
choices are historical evidence, not rules for the new repository.

| Source | Coverage | Findings used |
|---|---|---|
| `docs/spec/BIGBROTR_VISION_DECISIONS_2026-06-29.md` | Lines 44–83, 644–801, 932–972, including the complete dashboard matrix | Storage axes, presets as shortcuts, independent services, private operations, public opt-in, dashboard, differentiated research value, day-2 operations, naming |
| `docs/superpowers/plans/bigbrotr/phase-8-ops-packaging/README.md` | Full | Packaging, CLI, restore evidence, import/export, alerting, public status and API-key administration gaps |
| `docs/superpowers/plans/bigbrotr/phase-9-dashboard/README.md` | Full | Dashboard scope, versioned API, capability discovery, intended pages and acceptance; source explicitly says phase not started |
| `README.md` | Full | Current Go product framing, fifteen services, deployment and storage summary |
| `internal/config/profiles.go` | Full | Five presets, full/lightweight payload, provenance, custom kinds, default social kinds and optional list kinds |
| `docs/reference/configuration.md` | Full | Actual documented configuration contract, explicit profile requirement, service selection, defaults, some contradictory/stale descriptions |
| `docs/reference/cli.md` | Full | Run/status/health/doctor, config/schema, import/export/backup, repair, takedown/pruning, sizing |
| `docs/reference/http-api.md`, `docs/services/api.md` | Full | Query envelope, freshness, completed runs, exactness, limits, API-key semantics, unimplemented availability routing and ignored exposure configuration acknowledged by source |
| `docs/services/monitor.md`, `deriver.md`, `ranker.md` | Full | Different observation times, optional facts, completed derivations, provenance suppression, graph and relay score bases |
| `docs/services/archive-relay.md`, `dvm.md`, `exporter.md` | Full, with the exporter opening re-read after output truncation | Different read adapters, signing boundary, bounded export, corpus manifests, backup versus export distinction |
| `docs/reference/data-model.md` | Targeted search for provenance, availability and observation fields | Locating relevant concepts; not a full schema audit |
| Other service files and phases 0–7 | Inventoried by the parent workstream, not independently audited here | Their integration requirements are mapped below; this document does not certify their implementation |

The website reference is `/Users/vincenzo/Documents/GitHub/BigBrotr/website`.
Its text/config inventory contains 49 files: Markdown/MDX/Astro/TypeScript/CSS
under `src/`, plus README, package manifest/lock, Astro, TypeScript and Vercel
configuration. SHA-256 of the sorted manifest of `relative path<TAB>file SHA-256`
entries is `3d17274a09160bbf4ffbeb89689ddded8418b24b31d0caca36ba775c15313571`.
This fingerprints the inspected source inventory, not deployed content. No secrets
or host data were collected. The lockfile was inventoried, not dependency-audited.

## Existing website inventory

The source contains **37 authored content pages**: one home page, 28 documentation
pages including the docs landing page, and eight blog posts. Starlight and its
blog plugin can generate additional routes; these counts concern authored files.

`F` means the full text was read. `P` means selected body passages plus all titles
and headings were read. `I` means path, title, description and headings were
inventoried; the body was not reviewed in full. This distinction matters for long
historical articles and avoids presenting enumeration as a complete content audit.

Paths in this table are relative to `src/content/docs/`.

| Area | Files and read coverage | Current role |
|---|---|---|
| Home | `index.mdx` F | Five pillars, eight service cards, Python quick start, architecture and external links |
| Docs landing | `docs/index.mdx` F | Cards for getting started, architecture, services, database, configuration, NIPs, development and FAQ |
| Getting started | `docs/getting-started/introduction.md` F; `installation.md` F; `quick-start.md` F | Python/PostgreSQL introduction, source installation, Docker plus manual Python service launch |
| Architecture | `docs/architecture/overview.md` I; `packages.md` I; `data-flow.md` I | Historical diamond DAG, Python packages, shared database and cascade operations |
| Configuration | `docs/configuration/overview.md` F; `deployments.md` F; `core.md` I; `services.md` I | Pydantic/YAML, core/service split, BigBrotr and LilBrotr deployments |
| Services | `docs/services/overview.md` F; `api.md` F; `dvm.md` F; `seeder.md` I; `finder.md` I; `validator.md` I; `monitor.md` I; `refresher.md` I; `synchronizer.md` I | Eight Python services, runtime schema exposure, monitoring and publication combined |
| Database | `docs/database/schema.md` I; `procedures.md` I; `views.md` I | Six tables, stored procedures and eleven PostgreSQL materialized views |
| NIPs | `docs/nips/nip-11.md` I; `nip-66.md` I | Historical library usage and relay monitoring/publication shapes |
| Development | `docs/development/contributing.md` I; `testing.md` I | Python tooling, branching and test conventions |
| FAQ | `docs/resources/faq.md` F | Historical product, deployment and architecture answers |
| Blog: introduction | `blog/welcome.md` F, 2026-01-15 | Python/PostgreSQL announcement |
| Blog: architecture | `blog/inside-bigbrotr.md` I, 2026-02-28 | Long historical implementation tour; body not audited |
| Blog: monitor | `blog/building-a-nostr-relay-monitor.md` I, 2026-03-13 | Python monitor tutorial; code not independently checked |
| Blog: streaming | `blog/streaming-events-from-nostr-relays.md` P, 2026-03-13 | Python stream helper; completeness and resume claims examined |
| Blog: comparison | `blog/bigbrotr-vs-pensieve.md` P, 2026-03-15 | Historical technical comparison; no current competitor claim adopted without fresh verification |
| Blog: corpus study | `blog/exposed-nsec-analysis.md` P, 2026-03-17 | Research example with corpus size, methodology and explicit sampling limitations |
| Blog: operations | `blog/self-hosting-bigbrotr.md` P, 2026-03-18 | Proxmox/ZFS/PostgreSQL walkthrough and deployment links |
| Blog: protocol proposal | `blog/dead-keys.md` P, 2026-03-20 | Compromised-identity proposal; not evidence of a supported Rust feature |

Other website sources:

| Source | Coverage | What is present |
|---|---|---|
| `README.md` | Full | Astro, Starlight, starlight-blog; claims Vercel deployment on `main` |
| `package.json` | Full | `astro ^5.6.0`, `@astrojs/starlight ^0.37.0`, `starlight-blog ^0.25.3`, `sharp ^0.34.0`; dev/build/preview commands |
| `astro.config.mjs` | Full | Canonical `bigbrotr.com`, explicit docs sidebar, blog authors, custom components, GitHub link |
| `src/content.config.ts` | Full | Starlight content loader/schema extended by the blog schema |
| `tsconfig.json`, `vercel.json` | Full | Strict Astro TypeScript configuration; `dist` output; CORS header for Nostr identity file |
| `src/components/Header.astro` | Full | Docs/Blog navigation, Starlight search/language integration, custom mobile navigation |
| `src/components/MobileMenuFooter.astro`, `Head.astro`, `ThemeSelect.astro` | Full | Mobile links/preferences, shared social image, persisted auto/dark/light theme |
| `src/styles/custom.css` | Inventoried only | Custom styling exists; no computed-style or contrast audit performed |
| `src/assets/`, `public/` | File inventory | Brand/social images, blog illustrations, favicon and Nostr identity static assets |
| `package-lock.json`, `LICENSE` | Presence inventory | Existing dependency lock and license file; migration needs actual license/asset review |
| `dist/`, `.astro/` | Presence only | Generated artifacts exist, but are not evidence of the current source build |
| `src/pages/`, `.github/` | Absent in inspected tree | Routing comes through content/integrations; no local workflow directory observed |

The package manifest has no explicit lint, typecheck, browser, accessibility or
link-check scripts. That is an observation about this manifest, not proof that
no checks run on its hosting account or elsewhere.

## What should survive

The five-pillar explanation remains useful: discover relays, measure them, acquire
events, derive facts, and make the result usable. The new product adds an explicit
durable archive, differentiated research views, ranking and multiple read adapters.
Independent services and small configuration overrides remain central to adoption.

The existing website already has useful technical depth, a recognizable brand,
Markdown content, documentation search integration, dark/light themes, mobile
navigation and research posts. Its underlying static publication model fits the
new documentation and blog; the product change does not justify replacing it for
novelty. Historical articles should remain citable with dates and correction notes.

The Go API's answer-quality metadata is a strong product idea. A chart needs to
say which scope and observation time its data represents, whether a count is
approximate, and whether a query was narrowed. A status dot cannot replace these
facts. The archive/export distinction, CLI previews, health diagnostics and restore
drills also belong in the new operator journey.

## Current gaps and their implications

| Finding from inspected source | Consequence for the fresh product |
|---|---|
| Website home, docs introduction and quick start teach Python, eight services, PostgreSQL views and `python -m bigbrotr` | A Rust launch requires new versioned instructions and a historical documentation boundary. A new logo or framework alone would leave adoption broken. |
| Home/quick start says a Docker launch gets services running, then instructs users to launch Python services separately | The first-run guide must state exactly which processes start, and verify through the documented entry point on an empty host. |
| Website API pages expose discovered database tables and offset pagination; the Go plan uses a bounded semantic API | Publish a stable resource contract rather than database internals; give migration examples for old table-based clients. |
| Historical FAQ promises complete network visibility and says a shared database eliminates distributed failures | Replace absolute claims with scope, reachable relays, retained intervals and evidence. Sharing a database does not eliminate outages, ordering or backpressure obligations. |
| Streaming article says no missing events while its single-second fallback yields the available batch and malformed events can be skipped | Explain the limits of relay claims, bounded queries and acquisition evidence. A successful end-of-stream is not proof that a relay disclosed everything. This is a source-claim inconsistency, not a fresh execution result. |
| Phase 9 treats Monitor output as universally present because Monitor defaults on | Optional services and disabled individual probes require resource-level availability and per-field timestamps. A monitoring-disabled archive remains a valid instance. |
| Phase 9 gates social pages on a social preset/filter | An all-kind archive may have social inputs; a custom scope may have only some. Gate each analysis on actual required facts and declared coverage. |
| Phase 9 gates on provenance/full payload booleans | The new archive always retains full signed events. The dashboard instead distinguishes archival retention, hot/cold serving availability, public exposure and historical provenance coverage; legacy body-less imports cannot masquerade as complete events. |
| Vision calls first-sighting order a propagation map and first-seen retention a start/stop availability history | First sightings establish the observer's sequence, not causal propagation or exact deletion time. Availability history needs repeat observations and explicit negative-check evidence. |
| Vision describes censorship detection as a differentiated feature | Prefer selective-availability evidence with denominator, probe context and uncertainty. Omission alone cannot establish a relay operator's intent. |
| Frozen Go HTTP docs explicitly say `event_availability` is registered but not routed, and exposure configuration is validated but ignored | A capability endpoint must derive from executable, authorized resources. Frontend hiding cannot repair server exposure or a missing answer adapter. These are documented findings, not independently re-executed defects. |
| Frozen Go API keys bypass rate limits but do not authenticate | The new docs and UI must distinguish anonymous reads, authenticated reads and quotas. Never advertise rate-limit bypass as access control. |
| Frozen configuration page gives conflicting disk-watermark defaults and stale importer/engine descriptions | Generate machine-readable defaults and verify published examples against the release binary. Keep descriptions close to their owning schema/service. |
| Self-hosting blog has two links to `/docs/guides/self-hosting/` and one to `/docs/blog/inside-bigbrotr/`; no matching authored docs route exists and Vercel config has no redirect | Add an explicit redirect/link migration manifest and inspect built routes. This was a static source check, not an HTTP test of production. |
| No authored release, upgrade, Rust CLI, OpenAPI, Rust SDK or dashboard guidance exists in the website inventory | Add a separate website/docs/blog phase with release ownership, not a small dashboard footer task. |
| No configured docs versions or historical labels were found in the inspected content/configuration | Preserve old URLs deliberately and label historical instructions before routing visitors to the new implementation. |

The source corpus contains useful historical measurements, but their workloads,
software versions and dates differ. None becomes a Rust resource requirement,
compression promise, latency target or current competitor comparison by copying it.

## Surface ownership across the whole product

All fifteen original service responsibilities have a user-facing home. This is a
coverage map, not a demand to expose each service directly in navigation.

| Service responsibility | Public/research surface when exposed | Private operator/developer surface |
|---|---|---|
| Seeder | Source and acquisition-method description | Bootstrap sources, source errors, selected seed scope |
| Discoverer | Discovery provenance on relay detail when allowed | Candidate intake/backlog, egress and source failures |
| Validator | Validation status and observation time | Retry/backoff and candidate lifecycle diagnostics |
| Monitor | Relay health, capability, geography with field freshness | Probe scheduling, budgets, missing credentials/datasets, fleet gaps |
| Synchronizer | Acquisition coverage and evidence caveats | Progress, lag, stalled relays, method, resume and backpressure |
| Importer | Corpus origin and imported coverage labels | Preflight compatibility, progress, rejects, resume and source manifests |
| Indexer | Read-view freshness | Backlog, visibility boundary, replay/repair and failures |
| Deriver | Dated complete facts, cohorts, overlap and availability evidence | Target readiness, run history, invalidation and recomputation |
| Ranker | Explained scores with method, inputs and scope | Seed/configuration provenance, convergence, missing inputs |
| NIP-85 publisher | Publication metadata and documented protocol endpoint | Signing key, destination and acknowledgement state |
| NIP-66 publisher | Published monitor identity and observation link | Publication cadence, output age and destination failures |
| Exporter | Explicitly published dataset manifests and downloads | Export/backup/scrub progress, checksums, restore proof |
| API | Versioned bounded resources and capability discovery | Bind/exposure, budgets, auth and request diagnostics |
| DVM | Supported job contract and examples | Signing, recovery/idempotency, quotas and publication failures |
| Archive Relay | Relay URL, NIPs, bounded read semantics | Listener health, limits, cancellation and backpressure |

Phases 1–7 own these working contracts and production data paths. Phase 8 owns
first deployment, maintenance, private operations and release packaging. Phase 9
owns the public dashboard. The new, distinct website/documentation/blog phase
owns information architecture, content, docs versions and publication. Foundational
documentation is written alongside the earlier phases; its publication phase
consolidates and validates it rather than delaying all writing until the end.

## Primary-source stack research

These are documented capabilities and selection rationale, not a benchmark of
BigBrotr or a claim that one framework is universally faster.

| Option/source, checked 2026-09-11 | Documented fact | Design implication |
|---|---|---|
| [Astro content collections](https://docs.astro.build/en/guides/content-collections/) | Supports schema-validated local content and build-time generation | Keep prose, release metadata and authored research static; no live archive dependency at site build time |
| [Starlight site search](https://starlight.astro.build/guides/site-search/) | Pagefind search is included and requires built output to exercise | Verify search against the production build, including version/legacy handling |
| [Starlight internationalization](https://starlight.astro.build/guides/i18n/) | Locale routes, root locale, fallback notices and RTL support | English is the initial repository/site language; add maintained translations later without rewriting routes |
| [SvelteKit static adapter](https://svelte.dev/docs/kit/adapter-static) | Emits static assets; fallback and prerendering are distinct; host path behavior matters | A dashboard can deploy beside Rust without a Node server, but deep links/base paths must be tested |
| [SvelteKit SPA guidance](https://svelte.dev/docs/kit/single-page-apps) | Warns about startup round trips, SEO and JavaScript dependence | Use static output deliberately for self-hosted application views, prerender fixed explanatory pages and keep the public marketing/docs site fully rendered |
| [React application guidance](https://react.dev/learn/creating-a-react-app) | Frameworks can produce static/SPA output; starting with a bare build tool leaves routing/data choices to the project | React is viable, and does not inherently require a Node runtime. No existing React UI or measured advantage requires choosing it here. |
| [Askama](https://askama.rs/en/stable/) and [htmx](https://htmx.org/docs/) | Rust templates compile into Rust; htmx commonly exchanges HTML fragments | Strong alternative for a small server-rendered admin/table UI. Rich cross-filtered analysis would still need JavaScript, and a separately hosted dashboard would need an HTML service. |
| [Apache ECharts accessibility](https://echarts.apache.org/handbook/en/best-practices/aria/) | Supports ARIA descriptions and decal patterns, with explicit component/configuration requirements | Candidate for lazy-loaded analytics charts; provide a real table and text interpretation because a chart label alone is insufficient |
| [Playwright browsers](https://playwright.dev/docs/browsers) and [accessibility testing](https://playwright.dev/docs/accessibility-testing) | Browser projects cover Chromium/Firefox/WebKit; automated accessibility checks find only some problems | Use real browser journeys plus manual keyboard/screen-reader review |
| [WCAG 2.2](https://www.w3.org/TR/WCAG22/) | Defines testable accessibility success criteria | Target AA for release journeys and record manual checks, not a fabricated accessibility percentage |
| [OpenAPI overview](https://www.openapis.org/what-is-openapi) | A language-independent machine-readable HTTP interface description | Publish one contract for server, generated TypeScript types, reference docs and compatibility tests |

Recommendation: independently built SvelteKit/TypeScript dashboard and
Astro/Starlight website within the Rust monorepo. Reuse the existing blog plugin
only after its selected release is checked for the required routes, feed and
content metadata; Astro content collections provide a fallback if it obstructs
versioned docs. The plugin documentation URL attempted during this research did
not load, so no unverified plugin guarantee is asserted. Exact versions must be
pinned and tested when implementation begins.

## Five product-question candidates

These are candidates for the parent's ongoing owner conversation, not additional
approval gates and not questions already answered. They concern product intent;
framework and database choices remain engineering decisions.

| Candidate | Question | Why the answer matters | Working recommendation |
|---|---|---|---|
| PS-Q1 | Should a dashboard compare observations from independently operated BigBrotr instances, or describe only its connected instance? | Cross-instance comparison needs source identity, compatible scopes and methods, authorization and clock interpretation | Start with clearly identified single-instance views and links to other instances; add comparisons only over explicit compatible published datasets |
| PS-Q2 | Should the official public instance offer raw event content and historical versions by default, or begin with relay facts and aggregates? | Storage policy alone cannot choose public exposure; deleted/superseded versions need a deliberate serving policy | Publish metadata/aggregates first; make raw content, historical forensics and downloadable corpora explicit resource choices |
| PS-Q3 | Should BigBrotr operate a maintained public demo with an uptime commitment, or ship a dashboard principally for independent operators? | Changes hosting ownership, incident communication, costs and the website's main call to action | Ship self-hosting as the supported product; label any hosted instance with its operator, coverage and service commitment |
| PS-Q4 | Should public reputation pages emphasize finding useful peers, or publish broad ranked leaderboards of identities? | A leaderboard amplifies context-dependent scores and needs an explanation of seeds, missing data and intended meaning | Start with contextual lookups and explainable relay recommendations; publish identity rankings only with their model and dataset scope visible |
| PS-Q5 | Is a research citation expected to reopen an unchanged dataset/result, or is a saved query plus dated metadata sufficient? | Durable snapshot retention has storage and access-policy consequences; latest-run-only reads cannot promise immutable reproducibility | Guarantee checksummed published export manifests; make retained query snapshots explicit with retention and expiry, never imply that a live URL is an immutable corpus |

## Work still required before a release claim

This research did not execute either product, verify production HTTP routes,
benchmark frontend bundles, inspect browser rendering, establish accessibility
conformance, audit article claims against current protocols, or validate all
historical articles line by line. The implementation acceptance matrix in the
product design names those checks where they affect a release. Existing assets
and articles require attribution/license review before migration. No marketing
claim of completion, scale, latency or universal network visibility is supported
by this source inventory alone.
