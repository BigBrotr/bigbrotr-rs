# Private native candidate assembly

Run this committed helper from a clean checkout. Supply the artifact directory and
source manifest explicitly; the helper never builds, executes a product binary,
connects to an engine, downloads an asset, or searches neighboring directories.

```sh
python3 packaging/release/assemble.py \
  --artifacts /absolute/build/artifacts \
  --source-manifest /absolute/build/source-manifest.json \
  --output /absolute/new-candidate
```

Both `tools/build.py` output and the retained Archive installed laboratory build
receipt are supported. The shared source/build-info, CLI, embedded worker and
native registry identities must agree. This initial assembler accepts Linux
x86_64 release artifacts only. It verifies supplied provenance and bytes; unsigned
local build receipts do not authenticate their producer. The caller must obtain
the inputs through its trusted build custody.

The new private directory contains `bigbrotr-x86_64-unknown-linux-gnu.tar.gz` and
detached `candidate-manifest.json`, described by `manifest.schema.json`. The
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
argument is accepted.

The only advertised entrypoint capabilities remain the binary's
`event-verification` and `config-static`. This private candidate is not a release,
complete M1 installation, authenticated download or arm64 qualification. Complete
distribution notices and release gates remain open. Root acceptance must extract
the exact archive outside the checkout and execute the installed journey and
worker verification against the detached candidate identity before claiming it.

Focused offline tests: `python3 -B -m unittest discover -s tests/acceptance -p test_assemble.py`.
