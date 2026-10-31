"""Current policy-runner artifact admission before any engine setup."""
import hashlib
import json
from pathlib import Path
import runpy
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / 'experiments/policy-versions/run.py'


class EngineBoundaryReached(Exception):
    pass


class PolicyArtifactControls(unittest.TestCase):
    def run_preflight(self, tampered):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            run = home / 'new-owned-run'
            build = home / 'build'
            build.mkdir()
            original = b'independent-current-probe-fixture'
            (build / 'ingest_bootstrap_probe').write_bytes(original)
            (build / 'build-result.json').write_text(json.dumps(dict(
                source='a' * 40, probe_sha256=hashlib.sha256(original).hexdigest())))
            if tampered:
                (build / 'ingest_bootstrap_probe').write_bytes(original + b' changed after receipt')
            lab = SimpleNamespace(
                command=Mock(side_effect=lambda args: SimpleNamespace(
                    stdout='a' * 40 + '\n' if args[-1] == 'HEAD' else '')),
                setup=Mock(side_effect=EngineBoundaryReached),
                docker=Mock(side_effect=AssertionError('engine call before preflight')),
            )
            modules = dict(lab=lab, oracle=SimpleNamespace(expected=Mock()))
            expected = AssertionError if tampered else EngineBoundaryReached
            with patch.object(sys, 'argv', [str(RUNNER), str(run), str(build)]), \
                    patch.object(sys, 'path', list(sys.path)), patch.dict(sys.modules, modules):
                if tampered:
                    with self.assertRaisesRegex(expected, 'current ingest probe differs'):
                        runpy.run_path(str(RUNNER), run_name='__main__')
                else:
                    with self.assertRaises(expected):
                        runpy.run_path(str(RUNNER), run_name='__main__')
            self.assertEqual(lab.setup.call_count, 0 if tampered else 1)
            lab.docker.assert_not_called()
            self.assertFalse(run.exists(), 'preflight must not create a run or engine state')

    def test_changed_probe_is_refused_before_engine_setup(self):
        self.run_preflight(True)

    def test_matching_current_probe_reaches_the_setup_boundary(self):
        self.run_preflight(False)
