# Development, verification and delivery

This contract fixes the development and release workflow. It complements the
[agent execution guide](../implementation/agent-handoff.md), which owns task
selection and review, and the [deployment contract](../specification/deployment-contract.md),
which owns installed roles, dependencies and authority. Product implementation
started on 2026-09-17. The initial CLI/build/CI slice is described in
[build instructions](../development/build.md); the checkpoint and task evidence
identify executed work. Remaining workflows and packaging paths below are still
implementation deliverables, not claims of passing acceptance.

## One source revision, different execution environments

Q40 requires another developer to build, verify and use the complete selected
product from a clean checkout with declared dependencies and obtainable inputs.
The host conventions below govern this development session, not product
prerequisites. Maintain reusable tools and stable regression scenarios in the
repository, or a verified procedure for obtaining/generating their inputs;
caches may accelerate execution but their absence must not prevent it. Keep
commands, prerequisites and fast/real-engine/dedicated/release tracks explicit
through P01-T08, P08-T11/T12 and P10-T08/T10, without a parallel plan.

Under the owner's 2026-10-12 instruction (Q34), Tux is the sole editing, review,
Git, development and test host. The canonical integrating checkout is
`/home/tux/bigbrotr-rs`; use separate writable worktrees for actual parallel
implementation. Native Linux builds, engines, resource measurements and
crash/recovery scenarios still use isolated, inventoried exact-revision runs.
The recorded Tux host is Linux amd64; verify identity and resources before a run.
Neither the working checkout nor a lab run is a production release authority.
The prior Mac workspace and SSH transport are no longer prerequisites.

```text
Tux: edit, inspect, fast checks, coherent candidate commit
  ├── exact Git revision → isolated Tux checkout → Linux practical evidence
  └── verified commit → public development repository → GitHub Actions
                                                 ├── isolated CI checks
                                                 └── trusted release candidate
                                                      → Docker Hub by digest
                                                      → operator's Compose host
```

Local checks cover documentation, pure Rust, configuration and frontend work
where the actual dependencies support macOS. Linux storage/native/container
evidence must come from Linux. Docker Desktop or QEMU may help development; their
results are labeled virtualized/emulated and do not establish native target
performance. A native Linux arm64 runner supplies arm64 installation and binding
acceptance. Neither an Apple Silicon Mac build nor Tux's amd64 build qualifies it.
Select available isolated CI runners without assuming an account's paid features.
Missing target execution blocks advertising that target until it is qualified.

For a practical Linux run:

1. Form one coherent local candidate commit after fast checks and diff review.
   It can remain unpublished while Linux qualification runs. Transfer/fetch its
   exact commit using ordinary local Git cloning/archive extraction or a bundle.
   Keep authenticated Git access in the authoring context; do not mount credentials
   into test containers. Record and compare source commit and tree in the
   authoring checkout and isolated run.
   Verify staged, unstaged and untracked source state before building; candidate
   qualification refuses undeclared modifications. Matching HEAD alone is not
   enough. Source inputs come from the selected Git tree (a clean checkout or
   `git archive` extraction); generated inputs have explicit build manifests.
   Ignored developer files and ambient caches cannot supply undeclared sources.
2. Create an inventoried checkout/run directory under an owned BigBrotr lab root,
   with explicit ports, volumes, datasets, quotas and cleanup scope. Build from
   that revision; keep generated assets outside source. Do not synchronize an
   arbitrary mutable directory over another checkout or share writable engine
   directories. Preserve unrelated workloads and separately retained research.
3. Record command, source/tree identity, locks, compiler/native/extension/image
   identities, architecture, workload manifest, resource envelope, results and
   owned resources. Return concise evidence to the canonical checkout; retain raw
   output under the inventoried ignored run root. A disconnect or missing result means
   unknown, not PASS. Use the owning receipt/recovery protocol before retrying.
4. If the candidate changes, qualify the affected behavior again. Amending an
   unpublished candidate is allowed; never rewrite published history. Link each
   result to the revision it actually executed. A later documentation-only
   evidence commit may reuse a tested product tree when that equivalence is
   explicit. Release artifacts always identify the exact release source revision.
5. Stop only owned finished processes and record retained data and unfinished
   obligations. Large run directories are not release assets or source files.

An editor or agent may connect to Tux remotely, but authoring remains on Tux.
Do not edit two unsynchronized copies or modify historical run snapshots.
The workspace path is a development convention, not an end-user installation
requirement. Historical Mac-only raw captures are unavailable unless actually
recovered; retained Tux artifacts must be inspected before reusing their claims.

### Resource retention on the shared development host

Keep new generated work under inventoried ignored `.research/` roots where the
owning runner supports them. Existing run roots retain their paths until callers,
manifests and container mounts are reconciled; do not move them for cosmetic
organization. The laboratory inventory owns resource identities and lifecycle.
Record source, inputs, outputs, receipts and cleanup scope before a substantial run.

Use one bounded mutable compiler cache per qualified toolchain/profile when the
existing runner supports it, protected by its exclusive lock. Source inputs and
qualified executables/manifests remain outside that cache. A cache hit never
replaces an input hash check. Separate trusted and untrusted build contexts.

After a useful slice, retain evidence essential to current conclusions, open
problem reproducers, unique data, unintegrated work and safe resumption. Under Q40,
not every attempt, dataset copy or internal compatibility fixture needs permanent
retention. Document purpose, dependencies and regeneration for what remains;
promote useful reusable tools/inputs into maintained repository locations.
Retire enumerated obsolete resources only after verifying ownership, actual use
and whether any required information exists solely there. Text-search absence or
a stopped container does not prove disuse. Record material space reclamation and
preservation checks. Preserve unrelated workloads; avoid global pruning and a
general cleanup campaign that delays the product.

Check free disk before allocation against the selected scenario's actual reserve
and setup peak. The existing export executor's 64 GiB reserve must not be lowered
to fit the host. Reuse valid evidence when behavior and input identity are
unchanged; do not multiply build trees or broad reruns without a concrete reason.

## Commit and collaboration policy

Under the owner's Q36 instruction, `main` holds the current integrated project
state. The sole integrating owner works linearly there with coherent commits and
no mandatory PR or merge ceremony. Commit one understandable outcome with its
contracts, documentation and relevant evidence. Avoid both a commit per small
edit and a phase-sized dump that cannot be reviewed. The future contribution
guide follows the same discipline. Candidate commits used for testing may be
consolidated before first publication; final evidence must still identify the
tested input and any change after it. Push verified outcomes, not an unqualified
intermediate main merely to transfer it to a lab.

Branches/worktrees are useful for actual concurrent or risky work, not an
obligation. With parallel agents, assign bounded ownership and explicit contracts;
keep separate writable worktrees and choose one integrating owner. Do not let
agents race on a shared Git index, lockfile, migration allocator or checkpoint.
Re-read and test the integrated result. Independent review can be useful without
parallel implementation. No fixed agent/reviewer count is required.

Retire inactive local and remote branch refs after reconciling their changes and
current consumers. Keep original commits needed by pending work or retained
evidence reachable; use a verified bundle/protected ref when needed. A cherry-picked
patch alone does not preserve original identity, but obsolete internal iterations
do not all need permanent archival. Detach
inactive clean worktrees at the same commit when removing their branch refs,
preserving unintegrated work and required evidence. Dispose of obsolete worktree
contents and labs separately under the retention checks above; removing a branch
ref is not permission to remove its data. Keep active implementation branches
until their work is reconciled.

Before pushing, fetch and inspect remote changes. Use ordinary fast-forward
updates; reconcile unexpected work without resetting another contributor or
force-pushing published history. Keep commit messages and repository content in
English. [progress.md](../implementation/progress.md) remains the one current
resumption checkpoint; task files and contracts remain the source of meaning.

## Checks available in the planning repository

Requirements: Git, Python 3 and Node.js. From this repository root:

```sh
python3 experiments/verify_artifacts.py
python3 experiments/verify_handoff.py --prepare
python3 experiments/verify_handoff.py --output .research/handoff-check.json
git diff --check
```

`--prepare` is a first-use preparation step: it downloads checksum-pinned
`github-slugger` 2.0.0 from npm into ignored `.research/checkouts/handoff-validation/`.
It does not run npm installation scripts. Subsequent checks reuse that inspected
asset; an offline clean checkout needs the matching asset provided beforehand.
The current checkers validate document links/fragments, task references, numbered
steps and artifact structure. Dependency cycles in prose are review candidates:
read the task's foundation-versus-acceptance split before deciding they are real.
These programs do not execute the future product or prove semantic correctness.

P01 establishes the actual Rust/frontend/engine commands and common acceptance
runner. P08 extends the same CI and artifact assembler; it must not replace them
with a second successful-looking pipeline. Every required suite reports collected,
executed, failed, skipped and NOT_RUN cases with exact input identities. Empty or
missing required execution fails its gate. Documentation examples are executed
as printed where they are commands, not verified only by a parallel helper.

## CI boundaries

Use GitHub Actions for automation and **Docker Hub for application images**.
Q41 requires actual hosted results, not merely a configured workflow or a local
PASS. Q42 makes this source repository public; treat accessible Actions logs and
artifacts as public too. Review upload allowlists and use only synthetic nonsecret
inputs there. Do not upload whole laboratory/evidence roots containing owner
tokens, passwords, sessions, private inputs or database captures. Keep those in
protected ignored storage; publish only reviewed nonsensitive summaries.
GitHub Actions is a job runner; it does not require GitHub Container Registry.
GHCR is not the selected release destination. The normal installed runtime is
Docker Engine with Compose; native Linux remains a supported advanced path.
No custom deployment controller, mandatory Kubernetes or Tux-specific installer
is introduced.

| Track | Execution and evidence |
|---|---|
| Push/PR checks | Reproducible locks/toolchain; formatting, relevant Clippy, pure/property/protocol tests, generated-contract drift, document checks, frontend/static builds and focused real-engine integration |
| Trusted candidate | Clean builds; actual native Linux targets; complete advertised engine/client/browser/install scenarios, compatibility and artifact checks |
| Scheduled or explicitly selected qualification | Bounded fuzz/soak, aged state, mixed load, recovery and larger capacity tracks; report actual workload/host, not a universal capacity claim |
| Release promotion | Consume the verified candidate manifest and existing artifacts, verify identities/evidence, sign and publish unchanged bytes |

PR jobs run on disposable isolated runners with read-only repository permissions,
no persisted checkout credentials, no Tux SSH, registry, deployment or signing
secrets and no shared writable trusted release caches. Pin actions to reviewed
full commit SHAs, base images by digest and tool versions deliberately. Do not
execute PR code under privileged `pull_request_target`, `workflow_run` or another
credential-bearing trigger. Trusted jobs never promote arbitrary PR artifacts or
trust their names alone. Use separate trusted cache namespaces or rebuild clean.
GitHub's [secure-use guidance](https://docs.github.com/en/actions/reference/security/secure-use)
explains privileged-trigger and persistent-runner risks.

Do not register the shared, persistent Tux host as an unrestricted public-PR
self-hosted runner. Invoke its inventoried lab deliberately for a selected trusted
revision. A future automated large-test runner needs its own disposable isolation
and narrowly scoped credentials; Docker access on a shared host is not that
isolation. CI availability does not authorize deleting other host resources.

### Trusted Tux execution when hosted runners are unavailable

The owner's [Q33/Q41](../planning/questions.md) authorize native Tux checks as a
temporary fallback when hosted runners cannot start, not a local-only CI policy.
Inspect current run attempts, jobs, steps and annotations: provider billing refusal
means the checks were NOT_RUN; a workflow setup error, timeout/cancellation and a
product assertion failure are distinct outcomes. Hosted completion remains a
delivery requirement. Both Actions and the trusted
lab invoke `python3 tools/ci.py --output .research/ci` from a clean Git checkout.
The shared entrypoint selects the installed native compiler, runs the existing
foundation checks and records exact source, tool hashes, commands, per-step logs,
failures and unexecuted steps. It refuses an existing result directory. A timeout
or failed prerequisite cannot produce PASS; the command group is killed/joined.
The 120-minute command deadline fits the existing bounded CI track.

For an explicitly allowlisted portable-control-only diff, Actions runs the same
acceptance/export-profile Python controls and documentation checks on all three
foundation platforms through `tools/ci.py --controls-only`. The native compiler
smoke checks and platform-specific syscall controls remain; product/worker
compilation and native-engine downloads are unnecessary for that scope. Full
native CI remains required for product/build/workflow/selector inputs and unknown
changes, and dedicated installed qualifications remain with their existing task.
The complete Git diff includes deleted and renamed inputs; no empty or missing
comparison can bypass the native track. Control success is not product evidence.
The existing documentation-only track remains narrower. Both reduced scopes use
the same honest execution/termination receipts and reviewed upload allowlists.

Foundation uploads currently retain receipts and logs. Installed qualification
needs executable parent, worker, observer and native payloads from the maintained
candidate build/assembly path. Verify actual payload availability, mandatory
bindings and runtime ABI before reuse. A target triple or an uploaded build
receipt alone cannot establish those properties; record missing payloads and
unknown ABI honestly. The current hosted evidence lives in the existing
[P01 report](../implementation/evidence/p01-ci-hosted-foundation-20261026.json).

On Linux amd64, the pinned cargo-deny installation and dependency policy checks
run after toolchain/format checks and before the fresh exact-source release
build starts workspace/native compilation. Policy runs with `--locked` so it
cannot repair a stale lockfile before the first compiler check. It reads manifests, the lockfile, crate/license
sources and advisory data; they do not depend on a compiled product or worker receipt. Failure leaves
later checks NOT_RUN. Native tool discovery and hashed archive-input preparation
remain prerequisites. Other platform coverage and the requirement to build the
exact packaged worker before process tests are unchanged.

After that clean build, Clippy, rustdoc and the full default/debug workspace tests
reuse its frozen exported source, recorded native inputs and run-local Cargo
home/target. The runner sanitizes the environment and rechecks source, tool and
copied-artifact identities around each command. It preserves all debug test
coverage and artifact CLI gates; it does not convert tests to release mode or
reuse compiled output across runs. Different Cargo profiles/features can still
rebuild dependencies. Any timing benefit must be measured on a required run.

On Tux, use a fresh clone of the reviewed exact Git revision inside an inventoried
run directory, not a second edited source tree. Run as an unprivileged user in a
container pinned by image digest, with bounded CPU/RAM/PIDs, a read-only root,
no host ports, no Docker socket and no signing/publishing credentials. The
[development laboratory image](../../tests/acceptance/linux/Dockerfile) supplies
native build tools, Node and strace. Git/registry network access is only for
locked dependencies and the existing checksum-pinned link checker. Inventory
before launch; retain and inspect the receipt, actual artifacts and resource
termination. Do not register a persistent Actions runner to bypass this boundary.

These are native Linux amd64 command results, not GitHub job status. Current
hosted results are linked in the checkpoint; do not infer a continuing billing
block from the historical September report. Linux arm64 and
macOS retain their own native evidence requirements; no emulation or AMD64
result satisfies them. Candidate/release authentication, build-once promotion
and Docker Hub publication rules below remain unchanged.

## Build once and deliver verifiable artifacts

P08 owns three workflows: CI (initially created by P01), trusted candidate build,
and release promotion. A trusted manual candidate request selects one accepted
source SHA and intended version. It is not a deploy-on-every-push mechanism.
Concurrent releases of the same version are excluded. The final version tag is
created/checked against that source identity; an existing different tag is refused.

Use standard BuildKit/Buildx multi-stage builds, one application image with role
selection, and the already specified static frontend assets/offline extensions.
The build context is the clean repository root, with
`packaging/image/Dockerfile.dockerignore` beside its Dockerfile and an explicit
allowlist of needed source/build inputs. A bare `.dockerignore` beside a nested
Dockerfile is insufficient for a root context. Qualify the transferred context,
intermediate layers and final image using a planted development-secret canary.
See Docker's [context rules](https://docs.docker.com/build/concepts/context/).

Use two repositories: private candidates at
`docker.io/<provisioned-namespace>/bigbrotr-candidates` and released images at
`docker.io/<provisioned-namespace>/bigbrotr`. Keep the latter private until public
release is authorized. Docker Hub visibility applies to a whole repository, not
individual tags; never make all candidates public as a promotion shortcut.
Namespace ownership and restricted publishing credentials are operator
provisioning inputs. Private preview
artifacts stay private, including logs, provenance, SBOM and site previews. See
[Docker Hub access](https://docs.docker.com/docker-hub/repos/manage/access/).
The current development authorization permits the public source repository;
official releases, package publication, public site publication and production
deployment require separate authorization.

Build candidates once, push under a unique candidate reference, and test pulled
digests. Assemble the index from qualified platform manifests and record **both
the index digest and each platform digest**. Qualify that final index and its
attestations before promotion. Promotion retains the existing index bytes and
child manifests and their referenced attestations; copy only the selected candidate
closure to the release repository and verify destination digests rather than
assuming a tag-copy command preserved them. Never rebuild after acceptance under
the same version.
For the private native worker, compile the worker once before its main executable;
the trusted builder embeds the worker length/hash in the parent. Runtime selection
uses that anchor and the embedded native asset pins, never a mutable adjacent
receipt or runtime environment hash. Unbound development builds refuse installed
worker selection. This acyclic executable binding complements the outer signed
release manifest; it cannot authenticate the main executable itself.
Exact release tags are never reassigned; enable registry immutability where
available, but installation relies on digests regardless. Optional moving aliases
are convenience pointers and never the lock in shipped recipes. Docker documents
[multi-platform builds](https://docs.docker.com/build/ci/github-actions/multi-platform/)
and [index-copy behavior](https://docs.docker.com/reference/cli/docker/buildx/imagetools/create/).

Enable BuildKit SBOM and provenance explicitly, verify their availability after
push/pull, and bind their identities in release evidence. A local Docker exporter
or `load: true` must not be assumed to preserve them. Pass secrets as secret
mounts, never build arguments; review metadata before any public promotion.
[Docker's attestation guidance](https://docs.docker.com/build/ci/github-actions/attestations/)
documents these distinctions. An SBOM is inventory, not a correctness certificate.

Construct artifact and evidence identities in one direction:

1. Build each bundle with an internal inventory of its members, excluding the
   inventory itself; it never hashes its enclosing archive. Freeze a detached
   **candidate artifact manifest** binding the resulting bundle hashes,
   source/version, final image/index/platform digests, SBOM/provenance, schemas,
   docs line and required acceptance cases. It contains no future result hashes.
2. Execute acceptance against those exact artifacts and candidate-manifest digest.
   Each evidence report binds its inputs and actual outcome; reports stay detached
   from the candidate bundles and do not cause a rebuild.
3. Create the detached **release manifest** binding the candidate-manifest digest,
   qualified artifact identities and acceptance-report digests. Neither manifest
   contains its own digest/signature, and the release manifest is outside all
   bundles it hashes. Sign this final byte sequence.
4. Promote that verified closure unchanged. Post-promotion verification, release
   publication and site-deployment receipts may reference the signed manifest;
   they do not rewrite it or create a hash cycle back into its accepted evidence.

Select a small offline release-authenticity path: **Minisign signs the exact
release-manifest bytes**. Use the current hashed signature
format and the upstream tool, not new project cryptography. The protected release
job receives a dedicated project release key, separate from all Nostr keys;
ordinary builds/PRs/Tux do not. Pin the tool in the release inventory. A published
verification key/fingerprint is obtained through the documented project trust
root, not accepted merely because it accompanies an untrusted download. Version
and expected release identity are checked after signature verification, so an
authentic old manifest cannot silently substitute for the requested release.

Private rehearsals use test keys; public transparency-log infrastructure is not
required for this path. Before real signing, document encrypted/offline key backup,
rotation with authenticated old-to-new transition, and compromise revocation.
An offline operator-held production key may perform the same signing step if CI
custody is not configured. [Minisign's specification](https://jedisct1.github.io/minisign/)
provides the file/signature format and verification mechanism. Sigstore/Cosign
keyless attestations can be a later interoperable addition; they are not needed to
introduce a second required authenticity protocol in the initial release.

Release evidence must reject altered bundles, substituted platform/admin assets,
wrong signing key, wrong version, missing native acceptance, stale test artifacts
and absent attestations. Signing/promoting metadata never turns missing tests into
PASS. GitHub Releases may host signed bundles and notes; this does not make GHCR
the image registry. P10 renders the same manifest instead of hand-maintaining
download versions. CI/CD produces releasable artifacts; installation into an
operator's data host remains an explicit operation.

## Ordinary installation and upgrade

Before the first production release, internal iterations do not create supported
release compatibility. Q40 permits deliberate consolidation of schema/bootstrap,
superseded code and historical-only migration fixtures after consumer analysis,
while preserving every property of the current product and its required failure
tests. A development commit/push, including to the public repository, does not
freeze a migration forever. Supported release upgrades below remain deliverables; current restart, replay, recovery and state
succession are operational invariants, not disposable legacy compatibility.

The main path is a single Linux machine using Compose, local archive storage and
declared database volumes. S3 and standard remote endpoints remain supported
options under their qualification contracts; they do not complicate first setup.
Each logical service need not have a container. Use the selected compact or
isolated role placement with exclusive embedded-store ownership.

The first installation follows verified bundle → protected config/secrets →
offline validation → provision/start required engines → finite initialization/
migration → runtime doctor → start selected roles → owner enrollment and one
actual useful result. `doctor` and `serve` never initialize or migrate implicitly.
Interrupted setup preserves secrets, data and completed initialization receipts.

The initial Ingest writer has an explicit same-Control startup path under
qualification: [serve --initialize-ingest](../reference/ingest-store.md#explicit-initial-owning-lifetime).
It combines fresh native initialization and that first retained owning lifetime;
it is not an implicit Serve side effect or a finite initializer whose old identity
may be reused on restart. Its dictionary-only report is not role/M1 readiness.
Partial setup remains preserved and refused until explicit recovery is available.

The ordinary upgrade has explicit bounded downtime:

1. Verify the new signed manifest, render the selected Compose/config recipe and
   fetch exact application/engine digests. Check schema/native compatibility and
   disk headroom. Back up and verify the recovery point required by the migration.
2. Stop new intake, drain to durable boundaries and stop/positively exclude the
   old exclusive writers. Preserve named volumes and protected secrets. Do not
   use a remove-all-volumes command as an update or recovery shortcut.
3. Run the explicit migration operation with its own authority; resolve an
   interrupted migration by its persisted checkpoint. Recreate selected services
   using the qualified version set, then inspect doctor/capabilities and an actual
   permitted read plus a controlled admission where that deployment writes.
4. Record the active version and last safe rollback boundary. A previous image
   is usable only while persisted formats remain compatible. Beyond a writer
   floor, use forward repair or the specified fenced restore; retagging an image
   does not roll back databases. Keep production signing/egress controlled during
   recovery as required by the owning operations contract.

Compose alone does not promise rolling availability or cross-host failover.
Advanced stateless replacement uses P08's existing contracts. Do not add an
automatic updater or automatic SSH production deployment to main-branch CI.

## Package and license policy

All initial Cargo packages, including the three candidate pure libraries, use
`publish = false`. Internal modularity and clean APIs are useful without a
crates.io release or a promised externally stable application framework. HTTP,
Nostr and portable artifacts are the initial external integration contracts.
Publish a library only after a concrete independent consumer justifies its API,
compatibility and maintenance obligations in a separate decision.

MIT is selected for newly authored project code when P01 creates the license.
It does not relicense imported code, schemas, assets or dependencies. Check their
actual permissions and preserve required notices before copying/distribution;
follow the import contract's restriction on noncommercial upstream material.
The public repository's MIT license covers project code only; dependency and
imported-material notices retain their own terms. Visibility does not relicense them.

P01-T01/T08 own reproducible development, early CI and practical evidence;
P08-T09/T11/T12 own packaging, promotion and real release acceptance; P10-T03/T07
own executed installation and download/contribution guidance. These are steps of
the existing plan, not new parallel milestones or completed implementation work.
