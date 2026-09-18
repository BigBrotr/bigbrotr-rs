"""Independent process controls for shared CI execution and honest receipts."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class CiControls(unittest.TestCase):
    def load(self):
        path = ROOT / 'tools/ci.py'
        self.assertTrue(path.is_file(), 'shared CI entrypoint missing')
        spec = importlib.util.spec_from_file_location('shared_ci', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_failure_keeps_later_checks_not_run(self):
        ci = self.load()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'receipt'
            checks = [('positive', [sys.executable, '-c', 'print(42)']),
                      ('negative', [sys.executable, '-c', 'raise SystemExit(7)']),
                      ('unreached', [sys.executable, '-c', 'raise SystemExit(0)'])]
            report = ci.execute(checks, output, ROOT, os.environ.copy(), {}, 20)
            self.assertEqual(report['status'], 'FAIL')
            self.assertEqual([r['status'] for r in report['checks']], ['PASS', 'FAIL', 'NOT_RUN'])
            self.assertEqual(report['checks'][1]['exit_code'], 7)
            self.assertEqual(report['counts'], {'collected': 3, 'executed': 2, 'failed': 1, 'not_run': 1})
            self.assertEqual(json.loads((output/'result.json').read_text()), report)
            self.assertEqual((output/'positive.log').read_text().strip(), '42')
            with self.assertRaises(FileExistsError):
                ci.execute(checks, output, ROOT, os.environ.copy(), {}, 20)

    def test_timeout_kills_owned_descendant(self):
        ci = self.load()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'receipt'
            command = [sys.executable, '-u', '-c',
                       "import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); print(p.pid); time.sleep(60)"]
            report = ci.execute([('hang', command)], output, ROOT, os.environ.copy(), {}, .5)
            self.assertEqual(report['status'], 'FAIL')
            self.assertEqual(report['checks'][0]['failure'], 'deadline')
            pid = int((output/'hang.log').read_text())
            state = subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)], capture_output=True, text=True).stdout.strip()
            self.assertTrue(not state or state.startswith('Z'), state)

    def test_empty_execution_cannot_pass(self):
        ci = self.load()
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                ci.execute([], Path(directory)/'receipt', ROOT, os.environ.copy(), {}, 20)


if __name__ == '__main__':
    unittest.main()
