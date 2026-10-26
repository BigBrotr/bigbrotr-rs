"""Local process and command-boundary controls; never calls Docker or an engine."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import initial_schema


class LifecycleControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.lab = object.__new__(initial_schema.Lab)
        self.lab.root = Path(self.temp.name).resolve(strict=True)
        self.lab.name = "name-now-points-elsewhere"
        self.lab.started = True
        self.lab.phase = ""
        self.lab.checks = []
        self.lab.observations = []
        self.lab.bound = lambda: None
        self.lab.query = lambda *args, **kwargs: "1"

    def child_control(self, sleep):
        child = subprocess.Popen(
            [sys.executable, "-u", "-c", "import sys,time; "
             "print('READY', file=sys.stderr, flush=True); "
             f"print('captured', flush=True); time.sleep({sleep})"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        try:
            self.assertEqual(child.stderr.readline(), b"READY\n")
            communicate = child.communicate
            first = True
            original_timeout = []

            def short_wait(timeout):
                nonlocal first
                limit = 0.02 if first and sleep else timeout
                first = False
                try:
                    return communicate(timeout=limit)
                except subprocess.TimeoutExpired as error:
                    original_timeout.append(error)
                    raise

            with mock.patch.object(initial_schema.subprocess, "Popen", return_value=child), \
                    mock.patch.object(child, "communicate", side_effect=short_wait):
                if sleep:
                    with self.assertRaises(subprocess.TimeoutExpired) as caught:
                        self.lab.concurrency()
                    self.assertIs(caught.exception, original_timeout[0])
                    self.assertEqual(child.returncode, -9)
                else:
                    self.lab.concurrency()
                    self.assertEqual(child.returncode, 0)
            with self.assertRaises(ChildProcessError):
                os.waitpid(child.pid, os.WNOHANG)
            self.assertEqual((self.lab.root/"concurrent.stdout").read_bytes(), b"captured\n")
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
            child.stdout.close()
            child.stderr.close()

    def test_timeout_kills_reaps_and_preserves_failure_and_output(self):
        self.child_control(60)

    def test_completed_child_is_reaped_and_output_saved(self):
        self.child_control(0)

    def container(self):
        ident = "a" * 64
        self.lab.save("container-id.json", ident)
        return dict(Id=ident, Config=dict(Image=initial_schema.IMAGE, Labels={
            "org.bigbrotr.research":"initial-index-schema-20261026"}), Mounts=[
                dict(Destination=target, Source=str(self.lab.root/source))
                for target, source in [("/var/lib/clickhouse","data"),
                                       ("/var/log/clickhouse-server","logs"),
                                       ("/lab-private","private")]
            ], State=dict(Running=False, Pid=0, ExitCode=0, OOMKilled=False))

    def command_control(self, container):
        calls = []
        def command(argv, **kwargs):
            calls.append(argv)
            output = json.dumps([container]).encode() if argv[1] == "inspect" else b""
            return subprocess.CompletedProcess(argv, 0, output, b"")
        self.lab.command = command
        return calls

    def test_stop_and_readback_address_recorded_id_after_fresh_inspection(self):
        calls = self.command_control(self.container())
        self.lab.stop()
        self.assertEqual([c[1] for c in calls], ["inspect", "stop", "logs", "inspect"])
        self.assertTrue(all(c[-1] == "a" * 64 for c in calls))

    def test_foreign_identity_label_image_or_mount_refuses_before_stop(self):
        original = self.container()
        for change in ["id", "label", "image", "mount"]:
            with self.subTest(change=change):
                altered = copy.deepcopy(original)
                if change == "id": altered["Id"] = "b" * 64
                if change == "label": altered["Config"]["Labels"]["org.bigbrotr.research"] = "foreign"
                if change == "image": altered["Config"]["Image"] = "foreign"
                if change == "mount": altered["Mounts"][0]["Source"] = "/foreign"
                calls = self.command_control(altered)
                with self.assertRaises(AssertionError):
                    self.lab.stop()
                self.assertEqual(calls, [["docker", "inspect", "a" * 64]])


if __name__ == "__main__":
    unittest.main()
