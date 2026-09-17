# Review 17 — Development and delivery handoff

Date: 2026-09-17. Baseline: `ca7bc727396ae770ebce084b49efb70d1338a802`.
Stage: planning only. This review completes the owner's final handoff request;
it does not start product implementation, publish a container, or requalify a
future Rust adapter through documentation checks.

## Scope and method

[Review 16](16-complete-handoff.md) owns the full reading of its 106-document
baseline, subsequent changed boundaries and its actual Tux experiments. This pass
adds three independent AI domain reviews of the current handoff, delivery and
acquisition/storage semantics. These are AI counterreviews, not external human
certification. Each reviewer read complete assigned files in bounded chunks and
recorded hashes; root integrated findings and reread changed contracts/diffs.
The [read ledger](../research/evidence/review-17-read-evidence.json) preserves
individual scope and hashes, including overlap, rather than inflating coverage
by adding duplicate reads or calling a mechanical scan a full reread.

The final checks cover all current planning links, canonical fragments, task IDs,
step numbering, JSON evidence and research Python syntax. A clean exported source
snapshot exercises the documented first-use preparation independently of the
working checkout's ignored cache. This validates the current handoff tooling,
not planned Rust/engine/browser/CI acceptance. Exact commands, results, inputs
and exclusions belong to the
[verification record](../research/evidence/review-17-verification.json).

## Findings and disposition

| Finding | Correction and owning contract |
|---|---|
| Developer host choice and source transfer were implicit | [Development and delivery](../engineering/development-and-delivery.md) selects Mac authoring plus exact-revision isolated Tux qualification, with clean source/input identity and actual architecture evidence. Remote SSH editing remains a declared single-owner alternative. |
| Registry destination was unspecified | Docker Hub is selected; GitHub Actions runs CI without GHCR. Namespace/credentials and private destination are provisioned during implementation, not invented here. P08/P10 consume the same release contract. |
| CI trust was described only in general terms | Disposable untrusted jobs, pinned actions, bounded permissions, separate caches/artifact trust and no PR execution on shared Tux or with release secrets. P01 creates CI; P08 extends it. |
| Release identity/authentication needed a concrete path | Build once, qualify the exact final multi-platform index and promote unchanged digests. BuildKit supplies explicit SBOM/provenance; Minisign authenticates the exact release manifest binding all artifacts/evidence. No public transparency infrastructure is required for private previews. |
| Nested `.dockerignore` might not govern the root build context | P08 uses root source context and `packaging/image/Dockerfile.dockerignore`; inspect transferred context, intermediate layers and final image with a secret canary. |
| First-install narratives skipped finite initialization | Product journey and P10 now match deployment/P08: offline validation, engines, explicit initialization/migration, runtime doctor, then selected roles and a real result. |
| Ordinary Compose update was less explicit than advanced placement | Delivery defines bounded downtime, fetch/verify, compatibility/backup, writer drain/exclusion, finite migration, recreate/doctor/data checks and the actual safe rollback boundary. No volume deletion or automatic production rollout on push. |
| Crate reuse language could imply a required public library | All initial packages use `publish = false`; external crates need a concrete independent consumer. Newly authored project code uses the already selected MIT license, while upstream rights/notices remain separate. |
| Agent entrypoint omitted the stronger existing checker | AGENTS/README/agent guide link the canonical fragment/task checker, Node prerequisite and checksum-pinned first-use preparation. No competing agent.txt or master plan is added. |
| Checkpoint omitted concrete checkout state | Refresh the single checkpoint with observed worktree, branch, baseline and changed-file ownership; distinguish precommit observation from a later push. |
| P03-T02 practical archive read lacked its explicit prerequisite | Its step 7 requires P02-T04; earlier session/admission construction is not blocked on that acceptance. |
| Import preflight/decoder acceptance depended on their later importer | P03-T08 preflight core unblocks T09 decoding/dry-run; T10 adds durable imports and completes their deferred acceptance. No mock can close those cases. |
| One documentation rule demanded “the same PR” | Use “the same coherent change”; moderate linear commits are supported without obligatory branch/merge ceremony. |

Counterreview additionally required clean staged/unstaged/untracked source checks
for native lab builds: matching Git HEAD alone does not prove which bytes a
compiler consumed. The final delivery contract fixes that distinction and keeps
ignored developer material out of declared source inputs.
Counterreview also separated Docker Hub's private candidate repository from the
release repository (visibility is repository-wide), and removed a possible
manifest/evidence hash cycle. Candidate artifacts freeze first, acceptance binds
that identity, then a detached signed release manifest binds both. Publication
and site receipts follow without rewriting accepted artifacts.

## Decisions and remaining practical gates

The selected architecture remains Rust, PG control/catalog, RocksDB admission and
rebuildable state, DuckLake/Parquet archive, ClickHouse serving/analysis and bounded
DuckDB jobs. This pass found no reason to add another database, container per
logical service, mandatory S3, custom distributed scheduler or public Rust SDK.
Single-host Compose is the primary installation; useful remote roles/storage keep
their existing explicit contracts and qualify before being advertised.

The selected release authentication is intentionally file-oriented: one signed
manifest binds both native bundles and Docker digests. It avoids making a separate
OCI signing system or public identity/log service a prerequisite. P08 must still
pin and exercise the actual signer/verifier, wrong-key/version cases, key recovery,
registry credentials, attestations and digest-preserving promotion. No release key
or Docker Hub namespace has been created by this review.

[Readiness](../planning/specification-readiness.md) remains authoritative for
native bindings, empirical layout tuning, real engine durability, browser/site
journeys, architecture qualification and supported workload envelopes. These are
implementation gates with defined tests, not unanswered semantic choices. Large
component experiments already performed do not establish 100k busy relays,
host-loss protection, power-loss behavior or the future full product's capacity.
No remote experiment was started for this documentation/delivery pass, and dated
lab stop/retention observations were not silently presented as live inspections.

## Handoff

The implementer starts at root AGENTS, verifies the single progress checkpoint,
reads the agent guide and selected contracts, then builds the first real durable
signed-event → archive → read/export → restart slice through existing task IDs.
The work breakdown remains 123 tasks / 756 numbered steps; this refinement adds
no product completion marks. Coherent commits carry relevant review and practical
evidence, and unexpected measurements reopen the affected decision explicitly.

The planning handoff is ready once its recorded final checks pass. This means the
implementation has a concrete, consistent starting contract and validation path;
it does not assert that every private coding detail is predetermined or that the
unimplemented product is perfect.
