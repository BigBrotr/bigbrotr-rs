#!/usr/bin/env python3
"""Build one committed source tree; generated files stay outside that tree."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import stat
import subprocess
import sys
import tarfile

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.archive_bundle import (build_environment, install_bundle, native_target,
                                  prepare_bundle, verify_bundle)


def git(root, *arguments):
    return subprocess.check_output(["git", "-C", str(root), *arguments], text=True).strip()


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def regular_file_hash(path):
    if not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError('expected a regular file without a symbolic link: ' + str(path))
    return file_hash(path)


def source_inventory(source):
    """Inventory the exported tree without following substituted source links."""
    if source.is_symlink() or not source.is_dir():
        raise ValueError('invalid exported source directory')
    members = {}
    for path in sorted(source.rglob('*')):
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            continue
        members[str(path.relative_to(source))] = regular_file_hash(path)
    return members


def verify_native_toolchain(native):
    for tool in native['tools'].values():
        path = Path(tool['path'])
        resolved = path.resolve(strict=True)
        if str(resolved) != tool['realpath'] or regular_file_hash(resolved) != tool['sha256']:
            raise ValueError('native tool changed: ' + str(path))
    sdk = native['sdk']
    if sdk and regular_file_hash((Path(sdk['path']) / 'SDKSettings.json').resolve(strict=True)) != sdk['settings_sha256']:
        raise ValueError('SDK metadata changed')


def cargo_environment(output, manifest, manifest_hash, profile):
    """Declared run-local Cargo paths and source assertions, shared by checks."""
    return {'CARGO_TARGET_DIR': str(output / 'target'), 'CARGO_HOME': str(output / 'cargo-home'),
            'BIGBROTR_SOURCE_REVISION': manifest['revision'], 'BIGBROTR_SOURCE_TREE': manifest['tree'],
            'BIGBROTR_SOURCE_MANIFEST_SHA256': manifest_hash, 'BIGBROTR_BUILD_PROFILE': profile}


def clean_environment():
    # Native overrides (including target-qualified cc variables), wrappers,
    # prebuilt libraries and user Cargo configuration are not build inputs.
    return {key: os.environ[key] for key in
            ("PATH", "HOME", "USER", "TMPDIR", "SystemRoot", "RUSTUP_HOME") if key in os.environ}


def positive_jobs(value):
    jobs = int(value)
    if jobs < 1:
        raise ValueError("build jobs must be positive")
    return jobs


def command_output(command, env):
    return subprocess.check_output(command, env=env, text=True,
                                   stderr=subprocess.STDOUT, timeout=30).strip()


def native_toolchain(env, jobs):
    """Select one installed native toolchain, then bind its actual Cargo inputs."""
    jobs = positive_jobs(jobs)
    system = platform.system()
    discovery_name = {"Darwin": "xcrun", "Linux": "llvm-config"}.get(system)
    discovery = shutil.which(discovery_name, path=env.get("PATH", "")) if discovery_name else None
    if discovery is None:
        raise ValueError("native build requires xcrun on macOS or llvm-config on Linux")
    discovery = str(Path(discovery).resolve(strict=True))
    sdk = None
    if system == "Darwin":
        paths = {name: Path(command_output([discovery, "--find", tool], env))
                 for name, tool in (("CC", "clang"), ("CXX", "clang++"), ("AR", "ar"))}
        libclang = paths["CC"].parent.parent / "lib/libclang.dylib"
        sdk_path = Path(command_output([discovery, "--sdk", "macosx", "--show-sdk-path"], env)).resolve(strict=True)
        sdk = {"path": str(sdk_path),
               "version": command_output([discovery, "--sdk", "macosx", "--show-sdk-version"], env),
               "settings_sha256": file_hash(sdk_path / "SDKSettings.json")}
    else:
        bindir = Path(command_output([discovery, "--bindir"], env))
        paths = {name: bindir / tool for name, tool in
                 (("CC", "clang"), ("CXX", "clang++"), ("AR", "llvm-ar"))}
        libclang = Path(command_output([discovery, "--libdir"], env)) / "libclang.so"
    # Preserve the clang++ invocation name: resolving its final symlink to clang
    # changes driver mode. Record both the invocation and the resolved file.
    paths = {name: path.parent.resolve(strict=True) / path.name for name, path in paths.items()}
    for path in paths.values():
        if not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError("selected native tool is not executable: " + str(path))
    paths["LIBCLANG_PATH"] = libclang.resolve(strict=True)
    selected = {name: str(path) for name, path in paths.items()}
    selected.update({"CLANG_PATH": selected["CC"], "CARGO_BUILD_JOBS": str(jobs)})
    if sdk:
        selected["SDKROOT"] = sdk["path"]
    else:
        selected["LLVM_CONFIG_PATH"] = discovery
    native_env = dict(env, **selected)
    # Query the selected library in a fresh process with the same sanitized
    # environment as Cargo, rather than loading through ambient loader settings.
    clang_version = command_output([sys.executable, "-c", """
import ctypes, sys
class CXString(ctypes.Structure):
    _fields_ = [("data", ctypes.c_void_p), ("private_flags", ctypes.c_uint)]
lib = ctypes.CDLL(sys.argv[1])
lib.clang_getClangVersion.argtypes = []
lib.clang_getClangVersion.restype = CXString
lib.clang_getCString.argtypes = [CXString]
lib.clang_getCString.restype = ctypes.c_char_p
lib.clang_disposeString.argtypes = [CXString]
lib.clang_disposeString.restype = None
value = lib.clang_getClangVersion()
try:
    print(lib.clang_getCString(value).decode("utf-8"))
finally:
    lib.clang_disposeString(value)
""", selected["LIBCLANG_PATH"]], native_env)
    tools = {}
    for name, path in dict(paths, discovery=Path(discovery)).items():
        version = clang_version if name == "LIBCLANG_PATH" else (
            None if name == "AR" and system == "Darwin" else
            command_output([str(path), "--version"], native_env))
        tools[name] = {"path": str(path), "realpath": str(path.resolve(strict=True)),
                       "sha256": file_hash(path), "version": version}
    return selected, {"os": system, "architecture": platform.machine(),
                      "discovery": discovery_name, "environment": selected,
                      "tools": tools, "sdk": sdk}


def build_binaries(source, env):
    """Build once in dependency order; the parent binds the resulting worker bytes."""
    subprocess.run(['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr-archive-worker',
                    '--bin', 'bigbrotr-archive-worker'], cwd=source, env=env, check=True)
    worker = Path(env['CARGO_TARGET_DIR']) / 'release/bigbrotr-archive-worker'
    identity = {'bytes': worker.stat().st_size, 'sha256': file_hash(worker)}
    parent_env = dict(env, BIGBROTR_ARCHIVE_WORKER_BYTES=str(identity['bytes']),
                      BIGBROTR_ARCHIVE_WORKER_SHA256=identity['sha256'])
    subprocess.run(['cargo', 'build', '--locked', '--release', '-p', 'bigbrotr',
                    '--bin', 'bigbrotr'], cwd=source, env=parent_env, check=True)
    if worker.stat().st_size != identity['bytes'] or file_hash(worker) != identity['sha256']:
        raise ValueError('archive worker changed while building the parent')
    return identity


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
    for helper in ['tools/archive_bundle.py', 'contracts/release/archive-engine.json']:
        if file_hash(root/helper) != file_hash(source/helper):
            raise ValueError('selected revision has different archive build inputs; execute that revision in its own checkout')
    members = source_inventory(source)
    manifest = {"format": "bigbrotr-source/1", "revision": revision, "tree": tree,
                "archive_sha256": file_hash(archive), "files_sha256": members}
    (output / "source-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    manifest_hash = file_hash(output / "source-manifest.json")
    # Keep only the platform/tool discovery environment. Ambient RUSTFLAGS,
    # compiler wrappers, Cargo feature settings and source-identity overrides
    # cannot become unrecorded build inputs. Cargo caches remain dependencies,
    # verified by the lock; they never provide workspace sources.
    env = clean_environment()
    native_env, native = native_toolchain(env, args.jobs)
    env.update(native_env)
    archive_root = output / 'archive-worker'
    target = native_target()
    if args.archive_bundle:
        archive_root.mkdir()
        install_bundle(args.archive_bundle.resolve(strict=True), archive_root, target)
    else:
        prepare_bundle(archive_root, target, output / 'native-downloads', download=True)
    archive_assets = verify_bundle(archive_root, target)
    archive_environment = build_environment(archive_root)
    env.update(archive_environment)
    env.update(cargo_environment(output, manifest, manifest_hash, 'release'))
    worker_identity = build_binaries(source, env)
    verify_bundle(archive_root, target)
    verify_native_toolchain(native)
    after = source_inventory(source)
    if members != after:
        raise ValueError("build changed its declared source inputs")
    binary = output / "bigbrotr"
    shutil.copy2(output / "target/release/bigbrotr", binary)
    worker = archive_root / 'bigbrotr-archive-worker'
    shutil.copy2(output / 'target/release/bigbrotr-archive-worker', worker)
    info = json.loads(subprocess.check_output([str(binary), "build-info"], cwd=output, env=env))
    if (info.get('archive_worker') != worker_identity
            or worker.stat().st_size != worker_identity['bytes']
            or file_hash(worker) != worker_identity['sha256']):
        raise ValueError('installed worker does not match the identity embedded in its parent')
    verification = json.loads(subprocess.check_output(
        [str(binary), 'archive', 'verify-worker'], cwd=output, env=env))
    if (verification.get('result') != 'verified'
            or verification.get('executable') != worker_identity):
        raise ValueError('installed worker verification returned a different identity')
    if info["source"] != {"state": "builder-supplied", "revision": revision, "tree": tree,
                          "dirty": False, "manifest_sha256": manifest_hash}:
        raise ValueError("binary reports a different source identity")
    for name in ("Cargo.lock", "rust-toolchain.toml", "contracts/release/dependencies.json", "contracts/protocol/versions.json", "contracts/protocol/nip01-event.json"):
        if info["inputs_sha256"][name] != members[name]:
            raise ValueError("binary reports different build inputs: " + name)
    (output / "build-result.json").write_text(json.dumps({
        "format": "bigbrotr-build-result/1", "binary_sha256": file_hash(binary),
        "source_manifest_sha256": manifest_hash, "helper_sha256": helper_hash, "build_info": info,
        "native_toolchain": native,
        "archive_worker": {"path": str(worker), **worker_identity,
                           "registry_sha256": members['contracts/release/archive-engine.json'],
                           "assets": archive_assets, "build_environment": archive_environment},
    }, indent=2) + "\n")
    print(binary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", default="HEAD", help="committed source revision (default: HEAD)")
    parser.add_argument("--output", required=True, type=Path, help="new build directory; never overwritten")
    parser.add_argument("--jobs", type=positive_jobs, default=2,
                        help="positive Cargo/native compilation job limit (default: 2)")
    parser.add_argument('--archive-bundle', type=Path,
                        help='explicit existing pinned archive bundle; otherwise prepare it over HTTPS at build time')
    args = parser.parse_args()
    try:
        build(args)
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print("build failed: " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
