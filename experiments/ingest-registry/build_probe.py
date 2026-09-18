#!/usr/bin/env python3
"""Build a selected actual PostgreSQL acceptance example from one verified source archive."""
import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys

# Loading the selected helper must not add bytecode to the immutable archive.
sys.dont_write_bytecode = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--example", choices=["ingest_registry_probe", "control_owner_probe"], default="ingest_registry_probe")
    args = parser.parse_args()
    kind = "registry" if args.example == "ingest_registry_probe" else "control-owner"
    build = args.build.resolve(strict=True)
    source = build / "source"
    spec = importlib.util.spec_from_file_location("exact_build", source / "tools/build.py")
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    manifest = json.loads((build / "source-manifest.json").read_text())
    receipt = json.loads((build / "build-result.json").read_text())
    own_path = "experiments/ingest-registry/build_probe.py"
    if helper.file_hash(Path(__file__)) != manifest["files_sha256"][own_path]:
        raise ValueError("probe helper does not match selected source")
    if helper.file_hash(build / "source-manifest.json") != receipt["source_manifest_sha256"]:
        raise ValueError("source manifest mismatch")
    def check_source():
        actual = {str(p.relative_to(source)): helper.file_hash(p)
                  for p in sorted(source.rglob("*")) if p.is_file()}
        if actual != manifest["files_sha256"]:
            raise ValueError("source archive changed")
    check_source()
    output = build / args.example
    output_receipt = build / f"{kind}-probe-build.json"
    if output.exists() or output_receipt.exists():
        raise ValueError("probe artifacts already exist")
    env = helper.clean_environment()
    native_env, native = helper.native_toolchain(env, 2)
    if native != receipt["native_toolchain"]:
        raise ValueError("native toolchain differs from the source build")
    env.update(native_env)
    env.update({"CARGO_TARGET_DIR": str(build / "target"), "CARGO_HOME": str(build / "cargo-home"),
                "BIGBROTR_SOURCE_REVISION": manifest["revision"], "BIGBROTR_SOURCE_TREE": manifest["tree"],
                "BIGBROTR_SOURCE_MANIFEST_SHA256": receipt["source_manifest_sha256"],
                "BIGBROTR_BUILD_PROFILE": "release"})
    command = ["cargo", "build", "--locked", "--release", "-p", "bigbrotr", "--example", args.example]
    subprocess.run(command, cwd=source, env=env, check=True)
    check_source()
    for tool in native["tools"].values():
        if helper.file_hash(Path(tool["path"])) != tool["sha256"]:
            raise ValueError("native tool changed")
    shutil.copy2(build / "target/release/examples" / args.example, output)
    output_receipt.write_text(json.dumps({"format": f"bigbrotr-{kind}-probe-build/1",
        "source_revision": manifest["revision"], "source_tree": manifest["tree"],
        "source_manifest_sha256": receipt["source_manifest_sha256"],
        "parent_build_receipt_sha256": helper.file_hash(build / "build-result.json"),
        "binary_sha256": helper.file_hash(output), "native_toolchain": native,
        "command": command, "profile": "release"}, indent=2) + "\n")
    print(output)


if __name__ == "__main__":
    main()
