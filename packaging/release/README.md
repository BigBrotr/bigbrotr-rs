# Native candidate assembly

Run this committed helper from a clean checkout. Supply the artifact directory and
source manifest explicitly; the helper never builds, executes a product binary,
connects to an engine, downloads an asset, or searches neighboring directories.

```sh
python3 packaging/release/assemble.py \
  --artifacts /absolute/build/artifacts \
  --source-manifest /absolute/build/source-manifest.json \
  --output /absolute/new-candidate --include-journey
```

Use [the exact-source builder](../../docs/development/build.md#exact-source-builds)
to obtain the explicit artifact directory and matching source manifest from a
clean checkout. Existing trusted builds can be reused when the required source,
build-info, CLI, embedded worker and native registry identities still agree.
Product provenance is recorded separately from the current assembly/acceptance
source; a Python or documentation edit alone does not require a product rebuild.
This initial assembler accepts Linux x86_64 release artifacts only. It verifies
supplied provenance and bytes; unsigned
local build receipts do not authenticate their producer. The caller must obtain
the inputs through its trusted build custody.

Choose a fresh output directory outside the assembly checkout and artifact inputs;
existing outputs and symlink/traversal paths are refused. The new local directory
contains `bigbrotr-x86_64-unknown-linux-gnu.tar.gz` and
detached `candidate-manifest.json`, described by [manifest.schema.json](manifest.schema.json). The
archive retains `bigbrotr` beside its `archive-worker` directory so the existing
executable-relative worker/library lookup survives relocation. Only the selected
CLI, worker, pinned native assets/header, public notices/registry and generated
build-info are included. Lab test executables, logs, source trees, caches and
credentials are never recursively copied. Unexpected native members, links,
special files, traversal and existing output paths are refused. Input trees must
remain immutable during assembly; output belongs to the invoking user.

`members.json` inventories regular members with hashes, sizes and normalized
modes, excluding itself. The detached candidate manifest binds that inventory and
the enclosing archive; neither embeds its own hash, signatures or future test
results. Tar entries use sorted names and fixed ownership/time; gzip has no source
filename/time. Repeated assembly with the same source and bytes is deterministic
under the same Python/zlib implementation. The manifest records the artifact's
original source separately from the committed assembly source. Raw build receipts
and their host paths are represented by hashes, not copied into the archive.

`--include-journey` adds only the fixed tracked recipe, tutorial, Compose definition,
provisioning SQL and image Dockerfile named in `JOURNEY_FILES`. All must exist in
the assembly commit; missing inputs fail. It is an inclusion hook, not a claim
that Compose or the first-launch journey has passed. No arbitrary extra-file
argument is accepted. Omit the flag only for a core native bundle; the maintained
`p01-first-launch` case requires the complete journey set. If any journey file
changes, commit it, assemble a fresh candidate and qualify that candidate. An
older bundle's result remains bound to its original recipe/tutorial bytes.

The repository is public. The existing manifest's `visibility: "private"` and
`status: "candidate-not-release"` fields describe this historical local candidate
format, not repository visibility, release authentication or a credential
protection mechanism. Only obtain artifacts/manifests through trusted build
custody; the unsigned records establish byte identity, not producer authenticity.

The only advertised entrypoint capabilities remain the binary's
`event-verification` and `config-static`. This local candidate is not a release,
complete M1 installation, authenticated download or arm64 qualification. Complete
distribution notices and release gates remain open.

Use the maintained case from the matching clean checkout:

```sh
python3 tests/acceptance/run.py p01-first-launch \
  --candidate-manifest /absolute/new-candidate/candidate-manifest.json \
  --source-root "$PWD" --evidence .research/first-launch
```

The case verifies and extracts the exact archive into its fresh evidence directory
before using its installed bytes. Evidence can be outside the checkout or under
its Git-ignored `.research` directory; the extraction destination must be fresh,
exclusive, free of symlink components and separate from source/input files. This
intake rule does not permit assembler output inside its checkout/artifact inputs.
The candidate's bundled journey files must match the selected acceptance source.

The case is implemented; real-engine qualification of this maintained integration
is currently **NOT_RUN**. It selects fresh signed acquisition, Archive publication,
ordinary Control restart, exact private history and standalone JSONL export.
Its PASS does not cover the independent native/C API census, typed progress,
Ingest successor/resend, Index readiness or complete M1. See
[practical verification](../../docs/development/verification.md#fresh-installed-first-launch-case)
for pinned image inputs, prerequisites, result meanings, protected evidence
and cleanup custody. The [first-event tutorial](../../docs/tutorials/first-event.md)
provides the current operator procedure. Keep generated credentials and raw
captures protected locally; never upload the complete evidence directory to
public logs or artifacts.

Focused offline tests: `python3 -B -m unittest discover -s tests/acceptance -p test_assemble.py`.
