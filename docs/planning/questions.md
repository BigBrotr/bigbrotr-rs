# Questions, design selections and owner context

Technical suggestions from the owner are hypotheses to evaluate, not orders to
adopt particular technologies. The designer compares alternatives, selects a
coherent design and explains departures. Unanswered questions do not block the
planning work; the selected defaults below remain revisable with new evidence.

Operational instructions remain explicit: private temporary repository, legacy
repository read-only, English repository content, coherent commits/pushes and
planning/research only before product implementation.

| ID | Topic | Input or answer | Selected treatment |
|---|---|---|---|
| Q01 | Repository identity | Private `bigbrotr-rs` locally/GitHub now; eventually `bigbrotr` | Preserve BigBrotr brand; plan final transition without renaming current legacy repository |
| Q02 | Deletion and history | Retain original events and deletion requests; consider serving policy separately | Full historical archive; ordinary relay/current reads honor logical deletion; explicit authorized history resources |
| Q03 | First-launch priority | Every service/product area matters equally | Full scope and equal quality; order by dependencies; coherent local experience with explicit external exposure/signing |
| Q04 | Lossy lightweight profiles | Signature retention matters; old content-loss combinations may be removed; slim exports may suffice | Retain all signed fields/signature, remove heavy_fields storage axis and old Lil/Index runtime semantics; analytical projections labeled non-verifiable |
| Q05 | Active write probes | No separate answer received | Optional bounded probes with dedicated configured identity; passive monitoring fully works without signing |
| Q06 | Imported provenance | No separate answer received | Retain when selected as attributable third-party claims; never relabel as direct local observations |
| Q07 | Friendly recipe names | LilBrotr might mean complete archive without provenance; SocialBrotr may just be filters | One product/image, transparent recipe examples; no silent reinterpretation of old profile names; provenance orthogonal to social filtering |
| Q08 | Multiple users/datasets | No separate answer received | Shared instance dataset with scoped visibility/quotas; tenant-isolated archives are a distinct future topology |
| Q09 | Private web management | Strong interest in accessible private management as well as public dashboard | Include CLI, public explorer and private administration with roles, config revisions, jobs, diagnostics and recovery |

Additional owner context incorporated into the design:

- Examine 5k, 10k, 50k and 100k endpoints, billions of unique events and tens or
  hundreds of billions of provenance relations, under simultaneous workload.
- Prefer incremental computations where valid, but account for global graph,
  exact set and normalization work rather than promise constant-cost updates.
- Improve NIP-66/NIP-85 concepts through compatible current behavior and explicit
  proposed extensions; no upstream submission is part of this stage.
- Low TrustRank should not discard historical evidence. Ranking is an attributed
  viewpoint and may change with inputs, algorithms or curator.
- Accessibility, installation, explanations, examples and maintenance matter to
  adoption alongside efficient, readable and carefully reviewed code.

The complete design records remaining empirical validation tasks in PLAN.md.
They are not unanswered product questions or permission requests. The future
implementation must supply evidence before making production-capacity claims.

## Additional owner context: search, reuse and final source reread

| ID | Context | Design disposition |
|---|---|---|
| Q10 | Content search matters; OpenSearch was only a suggestion | Required supported feature with optional deployment index; CH baseline, Tantivy/OpenSearch evaluated against explicit semantics and workload |
| Q11 | Clients should reconcile against the archive as a relay | Add server NIP-77 to ArchiveRelay, independent of acquisition/provenance, with authorized retrievable sets and bounded shared indexes |
| Q12 | Service/container/crate boundaries and reuse are unclear | One image, selectable roles and finite jobs; fewer initial libraries, deliberate reusable APIs; explain compilation versus execution |
| Q13 | Reread original vision and every phase-0–9 Markdown, persist everything needed by a future implementer | Expand source inventory beyond phase READMEs, record requirement dispositions and fix discovered design gaps; no claim that planning proves production capacity |

These decisions do not authorize product implementation yet. The owner's request
remains a complete, explicit implementation baseline followed by a separate start
of the build stage. Research conclusions and performance targets remain revisable
when real implementation evidence contradicts them.

The latest owner clarification reinforces that the original Go feature list is
not mandatory. The product/data/feature decision catalog evaluates useful
questions, retained evidence, indexes, public APIs and distinct score meanings;
features may be removed or improved when their value does not justify their cost.

The owner additionally requested substantive complete-file reading of important
Go implementation paths. The [code walkthrough review](../reviews/08-go-code-walkthrough.md)
persists what those paths actually do and what the Rust design retains or changes.
The source remains read-only; tests were inspected as source, not reported as
executed. Implementation remains a later explicitly started stage.

## Detailed implementation breakdown and archive-value reconsideration

| ID | New owner context | Selected disposition |
|---|---|---|
| Q14 | Go THE PLAN is incomplete and moving; surprises reveal Rust planning gaps | Qualify captured code findings, recheck external plan identity, replace reliance on overview with phase/task/step specifications |
| Q15 | Specify all work and reconsider it at least five times before implementation | Expanded phases00–10, field/component/import/DVM contracts, coverage ledger and five actual correction rounds; future tests remain unchecked |
| Q16 | Permanent archive should retain useful ecosystem evidence, not duplicate-ingestion logistics | Keep full signed events, first observations/corrections and actual compact monitoring evidence; remove passive delivery histories/counts; operational TTLs retain correctness obligations |
| Q17 | Challenge storage value and duplication rather than assume a layout is efficient | Actual tiny Parquet experiment found object/reference files larger than a compressed wide layout; measure physical alternatives in P02-T09 |

The [expansion review](../reviews/09-expanded-plan-rounds.md) and
[coverage ledger](../implementation/coverage.md) record the chosen consequences.
Private DVM payloads stay operational by default; public scientific signed outputs
retain their originals/compact lineage. Provenance-off excludes interpreted
imported event-to-relay sidecars and intentional availability pairs as well as
ordinary witness indexes. No unanswered product question prevents this planning
handoff; actual platform/engine/WAN limits remain explicit measurement work.
