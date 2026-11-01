"""Run maintained native fault-verifier controls in the shared cheap CI suite."""
import importlib.util
from pathlib import Path


def load_tests(loader, _tests, _pattern):
    path = Path(__file__).resolve().parents[2] / 'experiments/native-rocks-binding/test_posix_sst_gate.py'
    spec = importlib.util.spec_from_file_location('native_posix_fixture_controls', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return loader.loadTestsFromModule(module)
