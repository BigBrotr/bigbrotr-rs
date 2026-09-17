#!/usr/bin/env python3
"""Build one committed source tree; generated files stay outside that tree."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile


def git(root, *arguments):
    return subprocess.check_output(["git", "-C", str(root), *arguments], text=True).strip()


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build(args):
    root = Path(__file__).resolve().parents[1]
    if git(root, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("source checkout has staged, unstaged or untracked changes; commit the candidate first")
    revision = git(root, "rev-parse", "--verify", "--end-of-options", args.revision + "^{commit}")
    tree = git(root, "rev-parse", revision + "^{tree}")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    source = output / "source"
    source.mkdir()
    archive = output / "source.tar"
    subprocess.run(["git", "-C", str(root), "archive", "--format=tar", "--output", str(archive), revision], check=True)
    # Reject source links and special files instead of allowing extraction to
    # escape the fresh root. No current build input requires either.
    with tarfile.open(archive) as bundle:
        for entry in bundle.getmembers():
            path = Path(entry.name)
            if path.is_absolute() or ".." in path.parts or not (entry.isdir() or entry.isfile()):
                raise ValueError("source archive contains an unsupported path or file type")
        bundle.extractall(source)
    helper_hash = file_hash(Path(__file__).resolve())
    if file_hash(source / "tools/build.py") != helper_hash:
        raise ValueError("selected revision has a different build helper; execute that revision in its own checkout")
    members = {str(path.relative_to(source)): file_hash(path)
               for path in sorted(source.rglob("*")) if path.is_file()}
    manifest = {"format": "bigbrotr-source/1", "revision": revision, "tree": tree,
                "archive_sha256": file_hash(archive), "files_sha256": members}
    (output / "source-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    manifest_hash = file_hash(output / "source-manifest.json")
    # Keep only the platform/tool discovery environment. Ambient RUSTFLAGS,
    # compiler wrappers, Cargo feature settings and source-identity overrides
    # cannot become unrecorded build inputs. Cargo caches remain dependencies,
    # verified by the lock; they never provide workspace sources.
    env = {key: os.environ[key] for key in ("PATH", "HOME", "USER", "TMPDIR", "SystemRoot", "RUSTUP_HOME") if key in os.environ}
    env.update({"CARGO_TARGET_DIR": str(output / "target"), "CARGO_HOME": str(output / "cargo-home"),
                "BIGBROTR_SOURCE_REVISION": revision, "BIGBROTR_SOURCE_TREE": tree,
                "BIGBROTR_SOURCE_MANIFEST_SHA256": manifest_hash, "BIGBROTR_BUILD_PROFILE": "release"})
    subprocess.run(["cargo", "build", "--locked", "--release", "--workspace"], cwd=source, env=env, check=True)
    after = {str(path.relative_to(source)): file_hash(path)
             for path in sorted(source.rglob("*")) if path.is_file()}
    if members != after:
        raise ValueError("build changed its declared source inputs")
    binary = output / "bigbrotr"
    shutil.copy2(output / "target/release/bigbrotr", binary)
    info = json.loads(subprocess.check_output([str(binary), "build-info"], cwd=output, env=env))
    if info["source"] != {"state": "builder-supplied", "revision": revision, "tree": tree,
                          "dirty": False, "manifest_sha256": manifest_hash}:
        raise ValueError("binary reports a different source identity")
    for name in ("Cargo.lock", "rust-toolchain.toml", "contracts/release/dependencies.json", "contracts/protocol/versions.json", "contracts/protocol/nip01-event.json"):
        if info["inputs_sha256"][name] != members[name]:
            raise ValueError("binary reports different build inputs: " + name)
    (output / "build-result.json").write_text(json.dumps({
        "format": "bigbrotr-build-result/1", "binary_sha256": file_hash(binary),
        "source_manifest_sha256": manifest_hash, "helper_sha256": helper_hash, "build_info": info,
    }, indent=2) + "\n")
    print(binary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", default="HEAD", help="committed source revision (default: HEAD)")
    parser.add_argument("--output", required=True, type=Path, help="new build directory; never overwritten")
    args = parser.parse_args()
    try:
        build(args)
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print("build failed: " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
