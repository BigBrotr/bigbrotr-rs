# Tux resumption audit — 2026-10-12

This dated audit prepares the owner's Tux-only development request (Q34). It is
not a new product roadmap or checkpoint. The [existing checkpoint](../implementation/progress.md)
owns the next action; the [agent guide](../implementation/agent-handoff.md)
owns execution and contains the continuing goal. Repository text remains English;
the owner-facing explanation is Italian.

## Source reconciliation and preservation

The private remote is `https://github.com/BigBrotr/bigbrotr-rs`. GitHub access as
`VincenzoImp` was initially invalid and was restored by the owner during this
audit. Fresh fetch confirms `origin/main` at
`54ad09dc6659bfe67e3bf2871e43ce619c199a5c`, with no open issues or pull requests
observed. The other remote branch is `research/producer-closing-old-writer`;
it is retained as a remote ref, not adopted as product work.

The former `/home/tux/bigbrotr-implementation-20260917/checkout` is a detached
September 18 laboratory snapshot at `6be85158`, with a bundle as its remote.
It is not the current development repository. The newest located product source
is `runs/ingest-history-f12dbf0/source`, a clean detached snapshot at
`f12dbf04677f8afa8c62aa1c8638e199dad0abc1`, tree
`7dd4e343f51b50a6055096ef14a0aaec03a0a8e5`.

Fresh ancestry comparison finds zero remote-only commits and four local-only
commits after `origin/main`:

| Commit | Recovered work |
|---|---|
| `f36a09e` | Explicit initial retirement specification and recovery boundary |
| `64f60b0` | Initial producer retirement implementation |
| `6bc2005` | Installed retirement fixture and historical refusals |
| `f12dbf0` | Refuse a regressed retirement timestamp before terminal writes |

Created `/home/tux/bigbrotr-rs` with `git clone --no-local`, independent Git objects,
and branch `recovery/tux-20261012` at the recovered source. Local `main` tracks
the actual remote baseline. `recovery-source` records the old source location;
`origin` is GitHub. No original checkout was reset, moved or edited. A complete
pre-migration Git bundle and successful `git fsck --full` preserve/verify the
selected reachable history. This same-disk bundle is not a host-loss backup.
The recovery branch contains unpublished work, not a newly qualified main.

A bounded census of the implementation lab found 504 Git repositories, 474 unique
HEADs and no dirty tracked/untracked source in those repositories. It pruned engine,
dependency and build-cache directories and searched to depth five; it is not a
claim about every repository on the host or unsaved work on the unavailable Mac.
Original snapshots, bundles and any divergent/negative branches remain intact.

## Product state and actual interruption boundary

BigBrotr is a verifiable Nostr archive and observatory: full signed events,
optional provenance, controlled acquisition, useful analysis/ranking and public/
private interfaces. The existing ten-phase implementation plan has 123 tasks and
756 numbered steps including historical planning. Counts describe scope, not a
completion percentage. Selected architecture remains Rust/Tokio, PostgreSQL
control and separate DuckLake catalog, RocksDB admission, DuckLake/Parquet archive,
ClickHouse online serving and a bounded DuckDB worker; TypeScript static clients
and the website are part of the same product.

There is substantial implemented and boundedly tested behavior, including signed
acquisition, admission, archive publication, private history and signed JSONL
export. Full M1 is still open, especially Ingest successor/restart/resend and
provenance-off recovery. M2–M5 and full analytics, client, operational, browser,
website and release journeys remain selected work. Neither planning completeness
nor passing component checks establishes a finished installable product.

Independent read-only AI review verified the interruption discrepancy:

* The old checkpoint called the clock fix uncommitted and awaiting build. In fact
  `f12dbf0` was committed and its native build finished at 2026-10-02 14:54:53 UTC.
* `ingest-history-f12dbf0/out/qualification.json` reports
  `PASS_INGEST_HISTORY_BUILD_ONLY`: release CLI/probe, four entrypoint tests,
  35 migration catalog tests, 3 source-closure + 9 producer-closing + 11 recovery
  native tests, and Clippy. The six recorded command groups succeeded.
* The actual retained CLI/probe/test executable and all 1,718 source-manifest
  files match their recorded hashes. This verifies those build inputs/outputs,
  not the entire product or a fresh engine execution.
* No corrected connected-engine result was found among standard run receipts.
  `ingest-recovery-engine-6bc2005/bindings/result.json` remains FAIL against
  product `64f60b0`: first nine cases passed; the tenth exposed retirement
  committing earlier than the retained source-closure timestamp.

| Retained f12 artifact | Verified SHA-256 |
|---|---|
| CLI | `b0aa597b383ee44dc7407edfe683f0c10572f5206a635163133a3714d83db2cd` |
| Probe | `59e307ab7df0001cc52c9a46759ec38109ee565e3216cc4ce8797340e4aa1bbb` |
| Source manifest | `06847c50496281cb824a9d4e55501c2f9f56132f8272deb363b60a19cfc589b9` |

The next product action is the existing ten-case retirement fixture on fresh
inventoried clones using the verified retained f12 artifacts. The clock case
must return exit 4 / InvalidMetadata without terminal changes. Then qualify
rollback/ambiguous COMMIT, workflow obligations, authority/lock schedules and
explicit successor before claiming P01-T09 Step 4. This continues P02-T01 Step 5 /
P02-T02 Step 4; it does not require completing all adjacent services first.

The tracked launcher exists on Tux; no missing Mac orchestration blocks it.
After current ownership and run preflight, its invocation is:

```sh
python3 "$BB_RETIRE_RUN/source/experiments/acquisition-scope/bindings.py" \
  "$BB_RETIRE_RUN" --ingest-retirement \
  /home/tux/bigbrotr-implementation-20260917/runs/ingest-history-f12dbf0/out/artifacts
```

`BB_RETIRE_RUN` must be a fresh inventoried sibling under the existing `runs/`
parent, for example `ingest-recovery-engine-f12dbf0`, and `source/` a clean exact
f12dbf0 checkout. The harness resolves retained PG, basis and seed by sibling paths;
moving this run under the new checkout's `.research/` would break those references.
Use exact f12 source rather than the new governance commit: artifact reuse accepts
differences only within `docs/` and `experiments/`, whereas governance also changes
AGENTS.md and PLAN.md. Keep integration documentation in the canonical checkout.

Read-only inspection confirmed retained PG resources/secret-file existence and
owner labels, the PASS acquisition-scope-54d1a18 basis, the b7b60f9 signed seed
and required captures, and the pinned client image. No secret values were read.
The original seed result SHA is
`08ec9000df8e5cf9facfd20d67cbf379f14a4c492fe36dedebb1b35e44ca63d0`.
Fresh database collision/table checks remain the launcher's live preflight.
It starts the borrowed PG, creates new clones/client, and stops owned resources
afterward. It requires more than 2 GiB free, so the observed disk does not by
itself block this small rerun. The 64 GiB export reserve still blocks the larger
export wave. Success is PASS_INSTALLED_INITIAL_RETIREMENT_FIRST_WAVE with ten
cases and original Control/catalog/files preserved; broader recovery remains open.

Mac-side ignored captures referenced by the old checkpoint were not recovered.
The Tux raw outputs above exist; missing Mac capture bundles remain unavailable
evidence. Original negative fixtures and applied migration checksums must remain.

## Space and live-state findings

The host has 32 logical CPUs and approximately 61 GiB RAM. Initial root filesystem
was 98% used, with approximately 38 GiB free. Free space is a moving observation,
not a reservation. `/tmp` is tmpfs and is unsuitable for large durable build/data
allocations. The inspected roots consume the following allocated bytes:

| Root | Observed bytes | Approx. GiB | Meaning |
|---|---:|---:|---|
| `bigbrotr-qualification-20260917` | 677384060928 | 630.9 | 20M signed synthetic events, alternate layouts, databases and same-disk restore backups |
| `bigbrotr-implementation-20260917` | 360376532992 | 335.6 | 493 run directories, Git snapshots, native builds, fixtures and retained engine state |
| `bigbrotr-object-storage-20260917` | 8903888896 | 8.3 | 1,048,576-event local/S3 experiments and restore state |

The latter two measurements are lower bounds: unprivileged `du` reported 225 and
one inaccessible entries respectively. All three are post-retirement retained
labs, not the pre-existing resources retired on September 17. No historical
retirement script should be replayed against them.

Docker census: 1,182 total containers, 1,163 with BigBrotr names, zero BigBrotr
containers running. Six unrelated containers are running: four InspireHEP
services, Ollama and tgdataset MongoDB. Docker reports 106.2 GB images and 18.4 GB
container writable layers, but shared layers and volumes prevent treating those
figures as additional guaranteed reclaimable space. Stopped containers can retain
required fault state and mounts. No containers were stopped or removed here.

Unprivileged `/proc` inspection found only this audit's BigBrotr path matches;
448 inaccessible/exited process entries limit that observation. It is not proof
that no privileged process has an open resource. Fresh process/open-file/mount
and compiler-lock checks are required before removal.

## Concrete cleanup sequence

1. Preserve the reconciled source branch, compact evidence, original failures,
   artifact hashes, database identities and required seed/input closure. Keep the
   existing laboratory inventory as owner. Source recovery is completed locally;
   retained data has not been backed up to another device by this audit.
2. Retire an explicitly enumerated subset of regenerable compiler intermediates,
   after fresh ownership/lock checks and preservation manifests. The independent
   cache review identifies about 41.46 GiB of single-link non-ELF candidate files
   across 18 old target trees while retaining every executable/shared ELF.
   This is a candidate ceiling, not a deletion-ready allowlist or reclaimed space:
   non-ELF alone is insufficient to prove an artifact disposable. The three old
   CI incremental directories offer a simpler subset of about 1.61 GiB.
   Exact paths, sizes, artifact checks and caveats are recorded in this audit's
   ignored cache-review artifacts and concise evidence JSON.
3. Re-measure available disk and the selected scenario's peak setup requirement.
   The export runner's existing 64 GiB completion reserve remains binding.
   Approximately 41 GiB reclaimed would provide useful working headroom from the
   observed baseline, but disk availability must be checked again before launch.
4. Only if that is insufficient, assess whole obsolete run/container closures
   and old qualification datasets individually. The roughly 201 GiB archive
   backup is not automatically redundant: it carries restore evidence and is on
   the same disk as its source. Retiring retained scientific/fault data requires
   explicit disposition or a verified external copy, with unavailable evidence
   recorded honestly. Do not delete unrelated projects or run global prune.
5. Prevent recurrence using the delivery contract's bounded shared compiler cache,
   preserved artifacts outside caches, isolated source inputs, per-run inventory,
   space preflight and cleanup at meaningful task boundaries. Keep old paths in
   place until referenced mounts/callers are deliberately migrated.

This audit performs no cache, dataset, image, container or volume deletion.
No bytes are claimed reclaimed. It prepares a reviewable cleanup scope.

## Local development and session readiness

The canonical checkout, GitHub access and recovered source are ready. Rust
1.98.1 with rustfmt/Clippy was materialized through the pinned rustup toolchain;
Node is 26.4.0 and Python 3.13.5. Installed only pnpm 9.15.9 into ignored
`.research/tooling` with install scripts disabled and npm integrity lock retained.
No global pnpm, shell profile, unrelated repository or system packages changed.

For local commands needing frontend/native discovery, use:

```sh
cd /home/tux/bigbrotr-rs
export PATH="$PWD/.research/tooling/node_modules/.bin:/usr/lib/llvm-19/bin:$PATH"
```

The existing `tools/build.py` tool-discovery routine successfully resolved clang,
clang++, llvm-ar and libclang 19 with that PATH. This is tool discovery only;
the retained qualified native build uses clang14 in its pinned lab image. Changing
compiler context requires fresh affected build evidence. Do not point historical
experiments at the host tools and claim identical qualification. Heavy product
builds and the connected engine rerun have not run during this audit.

The latest observed GitHub Actions run
[37017677570](https://github.com/BigBrotr/bigbrotr-rs/actions/runs/37017677570)
has billing/spending-limit annotations preventing job starts. Native Tux checks
remain authorized under Q33; no billing settings changed or new workflow started.
Native arm64 qualification, real signing/registry provisioning and public release
remain separate external/delivery gates. Tux amd64 cannot silently satisfy them.

Codex CLI is 0.162.1; the existing local configuration already selects
`gpt-6-astra` with `xhigh`, corroborated by the local model catalog. No extra
project-specific SKILL.md was found in the recovered repository; `AGENTS.md` and
the linked execution guide own project discipline. The continuing goal is in the
[existing guide](../implementation/agent-handoff.md#starting-the-continuing-goal-on-tux).
It is prepared, not activated by this recovery session. Persistent goals support
continuation but cannot promise a literally uninterrupted path through resource,
credential, platform or service constraints.

## Evidence and review boundary

The [concise audit evidence](evidence/tux-resumption-20261012.json) and
[laboratory inventory](laboratory-inventory.json) link local raw census/build/cache
records under `.research/tux-resumption-20261012`. Required documentation checks
are recorded there. The original product result remains unchanged.

Inspected governing entrypoint, mandate, questions, roadmap, phase index, execution
guide, readiness, Rust/delivery/build contracts and relevant active task sections.
Independent AI recovery review read the complete retirement adapter, migration,
fixture and owning reference, verified retained outputs and looked for newer
receipts. Independent AI cache review examined scoped target trees and artifact
dependencies. This is recovery/governance/resource review, not a complete security,
performance or correctness audit of all product source and dependencies. No task
checkbox was closed merely by recovering source or running structural checks.
