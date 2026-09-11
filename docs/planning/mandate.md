# Planning mandate

The objective is a complete, implementable design for BigBrotr, reconsidered
from its product purpose rather than inherited implementation choices.

The owner authorizes creation of this repository, research, isolated experiments,
coherent commits, and pushing the planning work. The current Go repository remains
read-only. Product implementation is explicitly outside this stage. The owner
confirmed a private `BigBrotr/bigbrotr-rs` repository during planning, with eventual
transition to the GitHub name `bigbrotr`; this does not authorize renaming the
existing repository during the present stage.

The design must account for every original service, the historical vision and
master plan, all nine planned development phases, the public dashboard, and a
distinct website/documentation/blog workstream. Existing behavior may be retained,
reorganized, replaced, or removed with an explicit explanation.

Each service needs a defined purpose, inputs, outputs, ownership, dependencies,
configuration and defaults, lifecycle, failure behavior, security boundary,
resource model, operator experience, and practical acceptance evidence.
Services must be independently selectable and deployable where useful. Their
data dependencies and finite buffering must remain explicit.

Configuration should expose useful policies and operational controls with good
defaults and small user overrides. Implementation accidents do not deserve
permanent configuration knobs. Unknown, contradictory, unsafe, or ineffective
configuration must be handled deliberately and documented.

The design must optimize computation, memory, disk, network, reads, writes,
imports, exports, and operational effort while preserving fidelity and honest
completeness claims. Tux is a useful single-host reference; multi-host growth and
failure recovery are part of the design, not assumed consequences of Docker.

Rust is the intended implementation language, subject to evidence about library
capabilities and boundaries. RocksDB, Parquet, DuckLake, PostgreSQL, ClickHouse,
DuckDB, and OpenSearch are candidates from the preceding investigation, not
owner-mandated components. Justify each adopted dependency and its alternatives.

The owner requests at least five or six substantive reconsiderations of the plan.
Use six distinct revisions grounded in further reading, practical experiments,
questions, and cross-component analysis. Do not substitute repeated stylistic
passes or a quota of clean reviews for learning.

Communicate progress in simple, detailed Italian and ask product questions
throughout. Record answers and label assumptions. Do not delegate technology
choices to the owner merely because they involve a tradeoff.

Verification must include real execution through user-facing entry points,
realistic workloads, failure/recovery, and tests appropriate to each risk.
Unit tests alone and code written only to satisfy its own tests are insufficient.
Do not impose arbitrary style or process limits such as maximum comment length,
fixed coverage percentages, or mandatory repeated defect-free reviews.

Deliver a clean English planning repository that another engineer can implement
in a justified order, with concrete contracts, validation scenarios, source
references, unresolved release risks, and a readable explanation for the owner.

The owner clarified that every product area is important. Do not seek a preferred
service to justify neglecting others. Default enablement, implementation order,
and equal quality obligations are different concerns; sequence by dependencies
and evidence while planning the entire product.

The owner requires retaining event signatures and invited removal of lossy
LilBrotr/IndexBrotr storage profiles. The adopted design recommendation is full
signed-event archival, optional provenance, selectable acquisition filters and
serving indexes, and explicitly projected analytical exports. Low TrustRank is
not grounds for automatic archival deletion. This replaces the old heavy_fields
configuration axis; legacy incomplete datasets require honest migration handling.

The capacity design must explicitly examine 5k/10k/50k/100k relay endpoints,
billions of unique events and tens/hundreds of billions of provenance relations,
under simultaneous acquisition, reads, ranking, export and maintenance. Avoid
avoidable bottlenecks, measure representation/amplification costs, and preserve
bounded overload behavior; no claim of unlimited physical capacity is justified.
