# Product requirements and historical crosswalk

BigBrotr is an independently operated Nostr acquisition, observation, archival,
analysis, and publication system. It collects eligible events across relays,
measures relay behavior, derives reproducible facts and trust viewpoints, and
serves those results through optional interfaces. It is also a distributable
product with installation, recovery, dashboard, documentation, and contribution
experiences. Every area carries the same quality obligation.

## Evidence and authority

The captured June 29 vision contains 2,819 lines and a full historical service
catalog. The Go master plan contains phases 0–9; its phase 9 is the public
dashboard. The adjacent consolidation plan calls itself phase 10 but concerns Go
cleanup, not the website. The new website workstream is additional scope and
must not be mistaken for that consolidation phase. Source hashes and capture
paths are in [the inventory](../research/source-inventory.json).

These are historical sources, not immutable design constraints. In particular,
Go, a custom permanent journal, Badger, a single binary, no RPC, separate frontend
repositories, and identical required infrastructure for every profile are
engineering choices being reconsidered. The owner's latest instructions are
recorded in [the mandate](../planning/mandate.md).

## Required outcomes

| ID | Outcome | Important qualification |
|---|---|---|
| R01 | Discover and fairly observe all eligible relays | Eligibility, backlog, reachability and cost are visible; no universal relay count guarantee |
| R02 | Acquire live and historical events concurrently | Transport completion is not proof that a relay disclosed all historical events |
| R03 | Preserve retained signed events and optional provenance | Retention policy and evidence origin are separate; no fabricated relay edges |
| R04 | Recover without advancing progress beyond durable data | State which failures an acknowledgement survives |
| R05 | Serve event, author, kind, tag, time, provenance and search reads | Cost budgets, pagination, current-state and archival semantics are explicit |
| R06 | Derive facts and compute reproducible trust viewpoints | Publish complete generations with input provenance, parameters and algorithm version |
| R07 | Support NIP-66 and NIP-85 publication and optional DVM compatibility | Signing identities and protocol-specific contracts are required |
| R08 | Configure and deploy each service independently | Durable data dependencies remain; finite buffers cannot hide indefinite downstream outages |
| R09 | Import, export, back up, restore, migrate and scrub | A portable data export is not a full resumable deployment backup |
| R10 | Provide dashboard, private operations and full website | Working capabilities, historical coverage and freshness drive the UI |
| R11 | Operate efficiently on Tux and grow across hosts | CPU, RAM, network, disk I/O and duplication are measured separately |
| R12 | Remain understandable, secure and maintainable | Idiomatic language boundaries, generated contracts, useful diagnostics and practical tests |

## Historical phases mapped to the new design

| Historical phase | Preserved responsibility | New treatment |
|---|---|---|
| 0: specification | Complete requirements and protocol decisions | This repository; resolve contradictions before product code |
| 1: runtime | Configuration, profiles, lifecycle, metrics | Continuous workers, scheduled jobs and listeners have distinct lifecycle contracts |
| 2: storage | Durability, replay, deduplication, indexes | Separate admission, archive and serving frontiers; challenge physical layouts |
| 3: acquisition | Live tail, backfill, reconciliation | Fair multiplexed sessions, explicit gaps and scoped evidence |
| 4: relay lifecycle | Seed, discover, validate, monitor | Shared egress policy; conservative endpoint identity; observations independent of publishing |
| 5: derivation | Current state, facts, clusters, rankings | Incremental state and pinned graph generations; advisory clusters never erase evidence |
| 6: reads | API, archive relay, DVM | One semantic read core, distinct streaming and protocol adapters |
| 7: publication | Signers, NIP-66, NIP-85 | Durable signed outbox, algorithm/viewpoint identity and relay receipts |
| 8: operations | Import/export, packaging, recovery | Designed from the first storage slice, completed with end-to-end release journeys |
| 9: dashboard | Public instance-aware frontend | Same monorepo, independent build/deployment, versioned HTTP contract |
| Additional website | Product pages, docs, blog, migration | Explicit versioned replacement of the historical Python site |

The original fifteen services remain accounted for: seeder, discoverer,
validator, monitor, synchronizer, importer, indexer, deriver, ranker,
nip85-publisher, nip66-publisher, exporter, api, dvm, archive-relay. Additional
internal responsibilities need names and owners: durable ingest, archive
publication/maintenance, policy enforcement, capability reporting and recovery.
They need not each become a separate mandatory container.

## Profiles and policy history

Replace the five historical storage presets with one product and explicit policy
axes. The owner requires signatures and invited removal of the lossy profiles.
The design removes LilBrotr and IndexBrotr as deployment profiles and the
`heavy_fields` axis. All retained events include full signed fields: id, pubkey,
created_at, kind, tags, content and signature. Provenance, acquisition filters,
serving indexes/retention, derived targets and external interfaces are independent
choices. BigBrotr, ArchiveBrotr and SocialBrotr may remain documented recipes,
not different binaries or mandatory names in the storage model.

Every policy change records an effective configuration generation and historical
consequences. Enabling provenance later cannot reconstruct past observations.
Widening filters introduces uncovered intervals. A projected export can omit
content or non-indexed tags, but is labeled an analytical dataset: retaining a
signature does not make a partial event independently verifiable. Legacy archives
already missing fields cannot be silently upgraded; mark gaps and attempt
reacquisition without promising success. Low trust scores never automatically
remove archival events; ranking is a derived viewpoint, not an admission oracle.

The owner's answer to Q03 requires full product quality, not a favored startup
service. Defaults therefore follow operational reasoning: a working local
experience without implicit signing or public exposure; all optional functions
receive complete setup, diagnostics and validation paths. Q02 now supports retaining originals and deletion requests; ordinary serving
applies a separate logical-deletion policy.

## Complexity that cannot be removed

Let R be monitored endpoints, D received deliveries, U unique retained events,
P retained event/relay relationships, T indexed tag values and E graph edges.
Connection state and a complete monitoring sweep cost at least O(R). Receiving
and checking deliveries costs at least O(D); retaining distinct facts requires
space proportional to U, P or T. A full graph pass touches E. The design avoids
unnecessary repeated full scans, not these information costs. Incremental
processing, compact representations, batching, narrow indexes and bounded
scheduling reduce constants and repeated work.

## Friendly names without hidden behavior

One downloadable product and application image serves every recipe. LilBrotr may
name a new full-archive/no-provenance example in documentation. It must not
silently reuse the incompatible old runtime profile meaning. SocialBrotr names a
social-filter example, with provenance chosen explicitly. Recipe initialization
writes the actual settings; stored capabilities do not infer behavior from names.
A deployment may split roles into several containers using the same image.

## Feature and source decisions

[Product/data/feature decisions](data-and-feature-decisions.md) states why each
retained data class, index and public resource earns its cost, which useful
journeys are included, and which tempting additions do not fit the baseline.
The [complete vision reread](../reviews/07-vision-and-final-recheck.md) maps all
original decision definitions to retained intent or deliberate changes. Legacy
plans are context, not feature obligations or performance certificates.

The [implementation walkthrough](../reviews/08-go-code-walkthrough.md) additionally
checks selected complete production call chains and test files. Its source-backed
lessons refine the current contracts; actual Go behavior does not automatically
become a required Rust feature or implementation technique.
