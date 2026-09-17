# Product, data and feature decisions

This document answers why each major capability belongs in BigBrotr, what data
it needs, what is indexed and what is exposed. Historical Go plans provide
context. A historical feature name is not a requirement to preserve its design.
The baseline is a Nostr observatory and verifiable archive with useful read and
analysis interfaces; it is not a universal search engine, a proof of network
completeness, or an authority on whether people or statements are trustworthy.

## What to retain and why

| Data class | Selected retained form | Why it earns its cost | Exclusions or limits |
|---|---|---|---|
| Verified signed event | One immutable shared payload per canonical event ID, with the first verified signature representative independently retained for each eligible visibility scope; binary IDs/key/signature, integer time, full ordered tags and original content string | Can reconstruct, verify, serve and export a complete signed event from the caller's authorized scope | No private signature or admission envelope is reused as public evidence; policy excludes invalid/oversized input explicitly; no automatic deletion by trust score |
| Event acquisition metadata | Original admission receipt/time and scoped first local receive metadata, with immutable earlier corrections | Explains when this instance acquired an event, even without provenance | Remote signed time and import claims are separate; no invented network witness |
| Event→relay provenance | Optional first attributable observation per event/endpoint/observer/auth scope; exact admission, compact dictionaries, immutable corrections | Answers where an event was actually observed; supports scoped reconciliation/overlap | Not every repeat delivery; not proof it remains available |
| Imported provenance | Source-attributed claims with validation status and source times | Useful outside evidence can be compared without pretending this instance measured it | An event signature authenticates the event, not the claimed relay observation |
| Measurement attempt | Compact typed outcome, method/version, observer/route, actual measurement time/duration and document references | Distinguishes failed, interrupted, unknown and successful executed checks; supplies valid denominators | Deferred/unexecuted scheduler work stays operational; it cannot refresh measurements |
| NIP-11, TLS/DNS and other bounded documents | Immutable logical document identities, original bytes, content digest, parse/method version and security scope | Reparse improved schemas, inspect declared behavior, diagnose changes | Repeated observations reference the same logical object; physical wide versus separate-object storage follows the measured catalog decision; do not index every arbitrary JSON field |
| Registry and coverage | Typed transactional identities, eligibility, attempts, gaps, source cuts and configuration epochs | Drives fair scheduling and tells users what was actually investigated | A maximum created_at or EOSE is not a completeness certificate |
| Derived facts and scores | Versioned outputs with source cuts, definitions, parameters, viewpoint and basis | Efficient reads, explainable comparisons and reproducible results | Retain usable/pinned generations; do not accumulate a full copy per tick forever |
| Signed outputs and job state | Durable intent, exact signed bytes, per-destination receipts, policy identity, bounded execution ledger | Safe retries, attributable publication and resumable operations | ACK is not indefinite remote retention; terminal payload retention is separate from replay tombstones |
| Operator policy and audit | Access/configuration changes, desired/applied versions, reason and actor, protected references | Makes management and recovery accountable | No secrets in public exports, ordinary logs or a browser-readable config dump |

Full signed-event archival and actual compact monitor evidence have no automatic
age limit in the baseline. This replaces the earlier proposed 365-day raw-monitor
default: executed measurements are reusable research evidence. An operator may
select a declared lossy retention policy; retained aggregate history names its
buckets, sufficient statistics and information lost. It cannot pretend that raw
detail remains available after expiry. Imported artifacts,
publication payloads and job details have their own retention references. Backups
and pinned exports are included when evaluating whether bytes can be reclaimed.

### Avoid repeated monitoring payloads

At 100,000 endpoints and one liveness attempt every five minutes, the schedule
would produce 28.8 million attempts/day before retries or additional checks. This
is arithmetic, not an achieved sampling rate. At 200 bytes of retained data per
attempt it would be 5.76 GB/day before indexes; neither figure estimates actual
compression. Monitoring deserves the same storage scrutiny as event ingestion.

Separate compact liveness attempts from the slower catalog schedule. Record a
bounded immutable document once per content digest and visibility namespace;
observations reference it with their own fetched/checked time. A byte digest is
verified on creation, and conflicting bytes cannot overwrite an existing object.
Changing content yields a new version. Reusing cached data records its age; it
cannot masquerade as a fresh independent probe. Host-level references bind the
exact hostname/address/port/route and method context needed for that check.

Publish new documents and their referring observations in one consistent archive
snapshot. Dictionaries and documents needed by retained observations, exports or
backups remain pinned. Rebuild parsed views from original documents. In CH, store
hot fields and references rather than duplicating full NIP-11 in every liveness
row. A standalone export includes its referenced documents so it needs no live
registry. This is a projection/data-model choice, not a new document database.

Logical identity does not prescribe Parquet placement. A measured physical layout
may place unchanged bytes beside observations when compression and lookup costs
justify it, while reconstructing the same scoped object and occurrence identities.
The [storage catalog](../specification/storage-catalog.md#document-layout-and-archive-immutability)
and P02-T09 own that comparison, including all files, references and recovery costs.

Do not retain every hostile frame merely for diagnostics. Use bounded sampled
reject metadata; optional raw capture requires explicit size/retention/security
settings and must not enter the valid event corpus. Do not compress away actual
measurement attempts into a misleading continuous uptime claim. Lossy historical
rollups must declare their temporal resolution and inability to answer finer
queries; raw expiry cannot silently turn approximate time accounting into exact.

## Which indexes are justified

| Query or computation | Selected access path | Cost boundary |
|---|---|---|
| Fetch an event or a bounded ID batch | Event-ID lookup returning full signed data at a pinned cut | No body scan through every Parquet file for the normal online path |
| Events by author/kind/time | Narrow ordered keys/projections, body lookup only for admitted result rows | Compare physical sort layouts under many parts; avoid copying content into every index |
| Events matching tags | Exact supported tag key/value postings; distinct IDs before limit/join | Native tags remain complete; repeated tag occurrences cannot multiply results |
| Event→relays and relay→events | Two selected provenance orderings when provenance is retained/exposed | P-sized cost is visible; no empty hidden edge indexes in a newly provenance-off deployment |
| Content search | Optional CH text index with declared analyzer/coverage | Token/phrase/score semantics are explicit; rich search engines require evidence |
| Reconcile a client's archive | Shared ordered retrievable-event set and range summaries | No billion-ID in-memory copy per client; filter-specific construction is bounded |
| Current social/replaceable state | Winner coordinates and affected old/new contributions | Resolve against the relevant source history before query filters |
| Counts/statistics and repeated dashboard queries | Complete incremental targets, bounded exact paths or declared estimators | Never substitute raw retry count for a valid approximation |
| Graph ranking | Dense node mapping, compressed adjacency and bounded rank vectors | Graph E and requested viewpoints drive work; do not build one permanent vector per pubkey |
| Offline corpus analysis/export | Published Parquet snapshots or standalone manifests | Async/isolated resources, no implied online query SLA |

Not every retained field receives an index. Index admission requires a named
query/computation, selectivity/order requirement, expected frequency and measured
read/write/space cost. Rebuildable optional indexes have visible ownership,
progress and removal operations. Arbitrary SQL belongs to authorized offline
analysis, not the public request path.

## Public resources and exposure

Public deployment is an explicit operation; the default full local recipe first
serves the local operator. Once a public surface is enabled, its capability
metadata explains what it can serve. Data-derived capability details obey the
same authorization boundary; capabilities must not reveal a private dataset's size.

| Resource family | Useful answer | Public treatment |
|---|---|---|
| Instance metadata/status | Version, supported resources, freshness, limits and broad health | Safe public descriptor; private topology, paths and secrets excluded |
| Current events and full-event lookup | Ordinary Nostr retrieval and bounded ID batches | Shared current/deletion/expiration/access policy; exact-ID lookup does not bypass it |
| Author/profile/current-list views | Interpretable public state and references | Public eligible inputs; explain unsupported/invalid kind-specific parsing |
| Relay directory/health/capabilities | Eligible endpoints, actual measurements, history and method/route basis | Redact restricted endpoints and operational credentials; probe errors are not global downtime |
| Event provenance and overlap | Attributable observation edges and comparable scoped sets | Separate opt-in resource grants; private/auth-scoped observations never leak through totals |
| Content search | Matching signed events and safe snippets | Explicit indexed scope, relevance and syntax; authorization before ranking/limit |
| Facts/counts | Defined quantities with cut, window and exactness | Share only authorized complete generations; small-cohort policy where needed |
| Trust and relay scores | Named viewpoint, raw score, normalization and components | Publishable input domain required; missing/withheld differs from zero |
| Full history/forensics | Superseded, deleted or expired evidence where allowed | Explicit authorized history resources, distinct from ordinary relay behavior |
| Immutable corpora/export jobs | Reproducible dataset with checksum/schema/filter manifest | Public downloads only for explicitly published artifacts; private jobs remain private |
| Management | Config, keys, grants, storage, recovery, jobs and audit | Private authenticated surface with action-level grants; never a public SQL/shell console |

HTTP routes remain the shared versioned families in the read contract; resource
IDs enumerate these semantics rather than create a second endpoint-specific query
engine. ArchiveRelay maps REQ/COUNT/NIP-50/NIP-77 to the same policies. The Nostr
query adapter uses its own explicit protocol profile and signed job delivery.

## Feature choices and user journeys

| Capability | Selected product behavior | Why and practical proof |
|---|---|---|
| Bootstrap and discovery | Bundled seed snapshot, continuous event reference extraction, optional external directories; inspect why an endpoint was found | A new instance acquires real signed fixtures through shipped commands without needing an external directory |
| Honest observation coverage | Fleet heatmap of seated/live, due, deferred, auth-gated and historically verified scopes | Users can distinguish 100k known endpoints from 100k continuously observed endpoints |
| Relay diagnosis | Per-check history, document changes, route basis, stale dependencies and explicit active-probe status | Empty relays, missing Geo files and local failures produce useful distinct outcomes |
| Acquisition | Concurrent live/history/reverification, configurable scope, gaps, adaptive fair scheduling and resumable imports | Controlled capped/malicious relays and overload do not create false progress or unlimited queues |
| Archive inspection | Full event, local signature verification, source metadata and authorized history/provenance | A user can verify and export the actual retained event rather than trust a rendered summary |
| Search and structured reads | Tags and text are distinct query modes, stable pagination and truthful counts | Client fixtures verify mixed filters, duplicates, ranking and disabled/stale index states |
| Client synchronization | Read-only archive relay with server NIP-77 and subsequent body retrieval | Reference clients reconcile with provenance off; restricted/missing bodies are not advertised as retrievable |
| Fact/rank exploration | Definitions, basis, viewpoint, components and input-cut comparison | A number can be explained and reproduced; the UI never labels popularity as truth |
| Corpus research | Named snapshots, full/projected exports, manifests and standalone DuckDB examples | Export opens without the running system; incomplete projections cannot pose as verifiable events |
| Private operations | CLI/UI parity for config, roles, grants, jobs, storage growth, recovery and diagnostics | Changes have desired/applied revisions, finite effects and permissions; recovery works without the browser |
| Reusable code and interfaces | Focused Rust libraries plus versioned HTTP/Nostr contracts | An external example uses a pure helper without pulling in all database engines |
| Documentation and adoption | Complete website, versioned guides/reference, tutorials, blog/releases and accessible dashboard | Clean-machine tutorial and browser journeys exercise shipped artifacts |

An explainable relay-selection view is useful: combine requested capability,
authorization/route, measured quality, provenance coverage and advisory overlap
so an operator can choose endpoints. It is a bounded read/rank resource, not a
new mandatory daemon or an excuse to stop observing mirrors. Initially expose the
components and user-selected constraints; an automatic multi-relay optimizer is
an extension only after its objective and benefit are measured.

Saved filters and comparisons belong in dashboard state or private user settings
when persistence is needed, not a new data engine. Operational alerts are shipped;
public user webhooks, arbitrary automation scripts and a new event notification
product are not automatically added. Each would need a separate delivery,
security and resource contract.

## Deliberately removed or deferred assumptions

Remove lossy storage profiles, per-profile images, one-container-per-service
requirements, public direct SQL, silent open-but-nonstreaming subscriptions,
automatic archive deletion from rank/NIP-09 and claims that observations establish
censorship intent. Replace them with explicit fidelity, roles, views and evidence.

Do not add media crawling, decryption, vector embeddings, automatic payments,
universal person identity, global truth scoring, consensus among monitors,
dynamic plugins or a search cluster simply because they are technically possible.
They change purpose, trust, cost or operational burden. Some may become useful
extensions after a concrete user need and measured benefit justify a contract.

The complete plan must preserve the named user outcomes, not every old knob or
service name. Configuration exposes meaningful safe choices; correctness rules
such as verified admission, coherent cuts and policy enforcement are invariants,
not speed toggles. Any later feature proposal states its useful question, source
evidence, persisted bytes, index/computation cost, public meaning, failure behavior
and practical acceptance scenario before being called part of the product.

The [detailed analytics contract](../architecture/analytics-detail.md) specifies
which facts and scores exist for each subject, their formulas, input scope,
retraction/expiry behavior, undefined outcomes and evidence requirements. It also
explains why a small graph update can still require a global score computation.
