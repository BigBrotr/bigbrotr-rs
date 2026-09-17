"""Negative controls for the real-process foundation runner."""
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests/acceptance/run.py"


class RunnerControls(unittest.TestCase):
    def invoke(self, binary, evidence, manifest=None, case="p01-t01"):
        extra = ["--source-manifest", str(manifest)] if manifest else []
        result = subprocess.run(
            [sys.executable, str(RUNNER), case, "--binary", str(binary),
             "--source-root", str(ROOT), "--evidence", str(evidence), *extra],
            capture_output=True, text=True, timeout=30,
        )
        self.assertTrue((evidence / "result.json").is_file(), "runner must record a terminal result")
        return result, json.loads((evidence / "result.json").read_text())

    def test_missing_executable_is_not_run_and_fails_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, evidence = self.invoke(root / "absent", root / "evidence")
            self.assertEqual(result.returncode, 2)
            self.assertEqual(evidence["result"], "NOT_RUN")
            self.assertEqual(evidence["counts"]["executed"], 0)

    def test_hardcoded_version_wrapper_fails_the_oracle(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "fake-bigbrotr"
            binary.write_text("#!/bin/sh\nprintf 'BigBrotr 0.1.0-dev\\n'\n")
            binary.chmod(0o755)
            result, evidence = self.invoke(binary, root / "evidence")
            self.assertEqual(result.returncode, 1)
            self.assertEqual(evidence["result"], "FAIL")
            self.assertGreater(evidence["counts"]["failed"], 0)

    def test_unvalidated_source_manifest_never_receives_a_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "fake-bigbrotr"
            binary.write_text("#!/bin/sh\nprintf 'BigBrotr 0.1.0-dev\\n'\n")
            binary.chmod(0o755)
            manifest = root / "source.json"
            manifest.write_text("{}")
            result, evidence = self.invoke(binary, root / "evidence", manifest)
            self.assertEqual(result.returncode, 1)
            source = next(c for c in evidence["checks"] if c["name"] == "clean_source_identity")
            self.assertEqual(source["result"], "FAIL")

    def test_permissive_event_echo_cannot_pass_the_crypto_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "permissive-candidate"
            binary.write_text("#!/bin/sh\nexec /bin/cat\n")
            binary.chmod(0o755)
            result, evidence = self.invoke(binary, root / "evidence", case="p01-event-cli")
            self.assertEqual(result.returncode, 1)
            checks = {check["name"]: check["result"] for check in evidence["checks"]}
            self.assertEqual(checks["signed_event E_public"], "PASS")
            self.assertEqual(checks["changed_payload"], "FAIL")
            self.assertEqual(checks["forged_signature"], "FAIL")
            self.assertEqual(checks["signed_empty_tag"], "FAIL")

    def test_timeout_cleans_up_descendants(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pid_file = root / "owned-child.pid"
            binary = root / "hanging-candidate"
            binary.write_text("#!/bin/sh\nsleep 120 >/dev/null 2>&1 &\nprintf '%s' \"$!\" > "
                              + shlex.quote(str(pid_file)) + "\nwait\n")
            binary.chmod(0o755)
            child = None
            try:
                result, evidence = self.invoke(binary, root / "evidence")
                self.assertEqual(result.returncode, 1)
                self.assertEqual(evidence["result"], "FAIL")
                child = int(pid_file.read_text())
                state = subprocess.run(["ps", "-o", "stat=", "-p", str(child)], capture_output=True, text=True)
                self.assertTrue(not state.stdout.strip() or state.stdout.strip().startswith("Z"),
                                "owned descendant remains live after timeout: " + state.stdout)
            finally:
                # The deliberate wrong implementation is also exercised against
                # this fixture; clean only the child PID this fixture just created.
                if child is not None:
                    try:
                        os.kill(child, signal.SIGKILL)
                    except ProcessLookupError:
                        pass


if __name__ == "__main__":
    unittest.main()
