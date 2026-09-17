"""Native build input controls, including real compiler/library smoke checks."""
import hashlib
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("clean_builder", ROOT / "tools/build.py")
builder = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(builder)


class NativeBuildControls(unittest.TestCase):
    def test_ambient_native_overrides_are_not_build_inputs(self):
        overrides = {
            "CC": "/untrusted/cc", "CXX": "/untrusted/c++", "AR": "/untrusted/ar",
            "CC_aarch64_apple_darwin": "/untrusted/target-cc",
            "TARGET_CXX": "/untrusted/target-c++", "HOST_AR": "/untrusted/host-ar",
            "CFLAGS": "-DUNTRUSTED", "CXXFLAGS": "-DUNTRUSTED", "ARFLAGS": "evil",
            "LIBCLANG_PATH": "/untrusted/libclang", "CLANG_PATH": "/untrusted/clang",
            "LLVM_CONFIG_PATH": "/untrusted/llvm-config", "BINDGEN_EXTRA_CLANG_ARGS": "evil",
            "ROCKSDB_LIB_DIR": "/untrusted/rocksdb", "ROCKSDB_INCLUDE_DIR": "/untrusted/headers",
            "ROCKSDB_CXX_STD": "c++11", "ROCKSDB_STATIC": "0", "ZSTD_SYS_USE_PKG_CONFIG": "1",
            "SDKROOT": "/untrusted/sdk", "DYLD_LIBRARY_PATH": "/untrusted/dylibs",
            "LD_PRELOAD": "/untrusted/preload", "LIBRARY_PATH": "/untrusted/libraries",
            "RUSTFLAGS": "--cfg secp256k1_fuzz", "RUSTC_WRAPPER": "/untrusted/wrapper",
            "CARGO_BUILD_JOBS": "999", "CARGO_ENCODED_RUSTFLAGS": "evil",
            "CARGO_HOME": "/untrusted/cargo", "CARGO_MAKEFLAGS": "evil", "NUM_JOBS": "999",
        }
        with patch.dict(os.environ, overrides):
            env = builder.clean_environment()
        self.assertEqual(set(env) & set(overrides), set())
        self.assertEqual(env["PATH"], os.environ["PATH"])

    def test_jobs_must_be_an_explicit_positive_integer(self):
        for invalid in ("0", "-1", "two", "1.5"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                builder.positive_jobs(invalid)
        self.assertEqual(builder.positive_jobs("2"), 2)
        self.assertEqual(builder.positive_jobs("32"), 32)

    def test_missing_discovery_tool_fails_without_ambient_fallback(self):
        # No compiler fallback is selected from a user-provided CC/LIBCLANG_PATH.
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, {"CC": "/untrusted/cc", "LIBCLANG_PATH": "/untrusted/libclang"}):
                env = builder.clean_environment()
                env["PATH"] = directory
                with self.assertRaises((OSError, ValueError, subprocess.SubprocessError)):
                    builder.native_toolchain(env, 2)

    def test_real_native_tools_compile_archive_link_and_load_clang(self):
        env = builder.clean_environment()
        selected, receipt = builder.native_toolchain(env, 2)
        env.update(selected)
        self.assertEqual(selected["CARGO_BUILD_JOBS"], "2")
        self.assertEqual(receipt["environment"], selected)
        self.assertTrue(receipt["tools"]["LIBCLANG_PATH"]["version"])
        for name in ("CC", "CXX", "AR", "LIBCLANG_PATH"):
            self.assertEqual(selected[name], receipt["tools"][name]["path"])
            path = Path(selected[name])
            self.assertTrue(path.is_absolute())
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), receipt["tools"][name]["sha256"])
        self.assertTrue(Path(selected["CXX"]).name.startswith("clang++"))
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            (work / "probe.cpp").write_text(
                '#include <vector>\n#include <cstdint>\n'
                'static_assert(__cplusplus >= 202002L);\n'
                'extern "C" int value() { std::vector<uint64_t> v{41, 1}; return v[0]+v[1]; }\n')
            (work / "main.c").write_text('int value(void); int main(void) { return value() == 42 ? 0 : 1; }\n')
            for command in (
                [selected["CXX"], "-std=c++20", "-c", "probe.cpp", "-o", "probe.o"],
                [selected["AR"], "crs", "libprobe.a", "probe.o"],
                [selected["CC"], "-c", "main.c", "-o", "main.o"],
                [selected["CXX"], "main.o", "libprobe.a", "-o", "probe"],
                [str(work / "probe")],
            ):
                result = subprocess.run(command, cwd=work, env=env, capture_output=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr.decode())
            # Apple's BSD archive exposes its symbol index as a member; GNU/
            # LLVM ar's listing omits the corresponding index.
            expected = ["__.SYMDEF SORTED", "probe.o"] if receipt["os"] == "Darwin" else ["probe.o"]
            self.assertEqual(subprocess.check_output([selected["AR"], "t", "libprobe.a"],
                                                     cwd=work, env=env, text=True).splitlines(), expected)


if __name__ == "__main__":
    unittest.main()
