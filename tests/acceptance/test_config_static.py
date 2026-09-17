"""Discriminating oracle and real-process controls for the static config case."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import config_static
import run as acceptance_runner
import test_runner


class StaticOracleControls(unittest.TestCase):
    def test_sensible_looking_report_with_implicit_signing_or_authority_fails(self):
        document = b"independent document identity"
        expected = {"roles": ["control", "read"], "dependencies": ["clickhouse", "postgres.control"],
                    "fixed": {"sync": 0, "monitor": 0}}
        report = {"format": "bigbrotr-config-static/1", "status": "valid-static",
                  "document_sha256": config_static.hashlib.sha256(document).hexdigest(),
                  "authority": {"mode": "managed", "status": "unavailable-offline", "desired_revision": None,
                                "applied_revision": None, "activation": "not-performed"},
                  "local_roles": expected["roles"], "required_dependencies": [
                      {"name": name, "runtime_identity": "not-checked"} for name in expected["dependencies"]],
                  "fixed_endpoint_counts": expected["fixed"], "full_signed_events": True,
                  "listeners": {"public": False}, "signing": False, "directory_discovery": False}

        def accepted(value):
            result = subprocess.CompletedProcess([], 0, json.dumps(value).encode(), b"")
            return config_static.report_matches(result, expected, document)

        self.assertTrue(accepted(report))
        for field, wrong in [("signing", True), ("directory_discovery", True), ("full_signed_events", False),
                             ("listeners", {"public": True}), ("listeners", {"public": 0}),
                             ("required_dependencies", []), ("local_roles", ["control", "read", "monitor"])]:
            self.assertFalse(accepted(dict(report, **{field: wrong})), field)
        authority = dict(report["authority"], status="applied", applied_revision="1")
        self.assertFalse(accepted(dict(report, authority=authority)))

    def test_absent_or_empty_syscall_observation_cannot_prove_no_secret_open(self):
        for observer, traces in [(None, []), ("strace", []), ("strace", [""])]:
            checks = []
            config_static.secret_open_check(checks, observer, traces, 1, Path("/owned/secret"))
            self.assertEqual(checks[0]["result"], "NOT_RUN")
        checks = []
        config_static.secret_open_check(checks, "strace", ['123 openat(AT_FDCWD, "/owned/secret", O_RDONLY) = -1 EACCES'],
                                        1, Path("/owned/secret"))
        self.assertEqual(checks[0]["result"], "FAIL")

    def test_descriptor_metadata_mutation_is_observed_even_when_it_fails(self):
        for call in ('fchmod(3, 0600) = -1 EPERM', 'fchownat(3, "secret", 1000, 1000, 0) = -1 EPERM'):
            checks = []
            acceptance_runner.syscall_checks(checks, "strace", ["123 " + call], 1)
            self.assertEqual(checks[1]["result"], "FAIL", call)


class StaticProcessControls(unittest.TestCase):
    invoke = test_runner.RunnerControls.invoke

    def test_missing_static_config_executable_is_not_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result, report = self.invoke(root / "absent", root / "evidence", case="p01-config-static")
            self.assertEqual(result.returncode, 2)
            self.assertEqual(report["counts"]["executed"], 0)
            self.assertEqual(report["result"], "NOT_RUN")

    @unittest.skipUnless(sys.platform.startswith("linux") and shutil.which("strace"),
                         "configuration side-effect control requires native Linux strace")
    def test_static_commands_cannot_hide_secret_open_transient_mutation_or_socket(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "side-effect-config"
            binary.write_text("#!" + sys.executable + " -B\n" +
                              "import pathlib,re,socket,sys\n" +
                              "if sys.argv[1:3] == ['config','show']:\n" +
                              "    socket.socket().close()\n" +
                              "    transient=pathlib.Path(" + repr(str(root / "transient")) + ")\n" +
                              "    transient.write_text('temporary'); transient.unlink()\n" +
                              "    data=pathlib.Path(sys.argv[-1]).read_text()\n" +
                              "    for ref in re.findall(r'file:([^\\\"\\n]+)',data):\n" +
                              "        if 'BB_CONFIG_SECRET_CANARY' in ref: pathlib.Path(ref).read_bytes()\n" +
                              "print('{\"status\":\"valid-static\"}')\n")
            binary.chmod(0o755)
            result, report = self.invoke(binary, root / "evidence", case="p01-config-static")
            checks = {c["name"]: c["result"] for c in report["checks"]}
            self.assertEqual(result.returncode, 1)
            self.assertEqual(checks["config_no_secret_open_syscalls"], "FAIL")
            self.assertEqual(checks["config_no_network_syscalls"], "FAIL")
            self.assertEqual(checks["config_no_file_mutation_syscalls"], "FAIL")
            self.assertFalse((root / "transient").exists())


if __name__ == "__main__":
    unittest.main()
