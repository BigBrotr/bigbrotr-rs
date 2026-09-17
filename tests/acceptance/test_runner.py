"""Negative controls for the real-process foundation runner."""
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests/acceptance/run.py"


class RunnerControls(unittest.TestCase):
    def invoke(self, binary, evidence, manifest=None, case="p01-t01", source_root=ROOT):
        extra = ["--source-manifest", str(manifest)] if manifest else []
        result = subprocess.run(
            [sys.executable, str(RUNNER), case, "--binary", str(binary),
             "--source-root", str(source_root), "--evidence", str(evidence), *extra],
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

    def test_malformed_fixture_still_records_terminal_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("experiments/identity-contract/signed-fixture.json",
                         "tests/fixtures/protocol/signed-extremes.json"):
                target = root / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("{invalid")
            result, evidence = self.invoke(Path("/bin/cat"), root / "evidence",
                                           case="p01-event-cli", source_root=root)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(evidence["result"], "FAIL")
            self.assertTrue(any(c["name"] == "execution" for c in evidence["checks"]))

    @unittest.skipUnless(sys.platform.startswith("linux") and shutil.which("strace"),
                         "event syscall control requires native Linux strace")
    def test_event_only_transient_file_mutation_is_observed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "side-effect-candidate"
            path = shlex.quote(str(root / "transient"))
            binary.write_text('#!/bin/sh\nif [ "$1" = event ]; then\n'
                              + 'touch ' + path + '\nrm -f ' + path + '\nfi\nexec /bin/cat\n')
            binary.chmod(0o755)
            result, evidence = self.invoke(binary, root / "evidence", case="p01-event-cli")
            checks = {c["name"]: c["result"] for c in evidence["checks"]}
            self.assertEqual(result.returncode, 1)
            self.assertEqual(checks["signed_event E_public"], "PASS")
            self.assertEqual(checks["event_no_file_mutation_syscalls"], "FAIL")

    def test_static_config_case_never_accepts_hardcoded_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "permissive-config"
            binary.write_text('#!/bin/sh\nprintf \'{"status":"valid-static"}\\n\'\n')
            binary.chmod(0o755)
            result, evidence = self.invoke(binary, root / "evidence", case="p01-config-static")
            self.assertEqual(result.returncode, 1)
            checks = {c["name"]: c["result"] for c in evidence["checks"]}
            self.assertEqual(checks["config_invalid_unknown_key"], "FAIL")
            self.assertEqual(checks["config_invalid_disabled_proxy"], "FAIL")
            self.assertEqual(checks["config_doctor_incomplete"], "FAIL")

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
