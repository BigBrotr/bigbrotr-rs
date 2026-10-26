"""Verifier fault controls only; these never qualify the product or an engine."""
import json
from pathlib import Path
import tempfile
import unittest

from initial_index import (GROUPS, REQUIRED_MODULES, completed_tests,
                           listed_tests, retain_executable, selected_tests)


class IndexResultControls(unittest.TestCase):
    def test_inventory_requires_each_real_owning_module(self):
        names = sorted({prefix + 'control' for prefix in (*GROUPS.values(), *REQUIRED_MODULES)})
        text = '\n'.join(name + ': test' for name in names) + f'\n\n{len(names)} tests, 0 benchmarks\n'
        self.assertEqual(listed_tests(text), names)
        self.assertEqual(set(selected_tests(names)), set(GROUPS))
        for prefix in (*GROUPS.values(), *REQUIRED_MODULES):
            with self.assertRaises(ValueError):
                selected_tests([name for name in names if not name.startswith(prefix)])
        for invalid in ('0 tests, 0 benchmarks\n', text + text,
                        text.replace(f'{len(names)} tests', '999 tests')):
            with self.assertRaises(ValueError):
                listed_tests(invalid)

    def test_zero_ignored_missing_extra_and_duplicate_results_cannot_pass(self):
        expected = ['storage::clickhouse::fixture']
        valid = ('test storage::clickhouse::fixture ... ok\n'
                 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 20 filtered out; finished in 0.01s\n')
        completed_tests(valid, expected)
        for invalid in (
            valid.replace('1 passed', '0 passed'),
            valid.replace('0 ignored', '1 ignored'),
            valid.replace('0 failed', '1 failed'),
            valid.replace('fixture ... ok', 'other ... ok'),
            valid.splitlines()[1] + '\n',
            'test storage::clickhouse::extra ... ok\n' + valid,
            valid.splitlines()[0] + '\n' + valid,
            valid + valid.splitlines()[1] + '\n',
        ):
            with self.assertRaises(ValueError):
                completed_tests(invalid, expected)
        with self.assertRaises(ValueError):
            completed_tests('test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.00s\n', [])

    def test_only_one_matching_compiled_regular_executable_can_be_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / 'target'
            target.mkdir()
            executable = target / 'test-executable'
            executable.write_bytes(b'synthetic executable identity only')
            log = root / 'compile.log'
            item = dict(reason='compiler-artifact', executable=str(executable),
                        target=dict(name='bigbrotr', kind=['lib']), profile=dict(test=True))
            log.write_text(json.dumps(item) + '\n')
            destination = root / 'retained'
            retain_executable(log, 'lib', True, destination, target)
            self.assertEqual(destination.read_bytes(), executable.read_bytes())
            for text in ('', json.dumps(item) + '\n' + json.dumps(item),
                         json.dumps(dict(item, profile=dict(test=False))),
                         json.dumps(dict(item, executable=str(target / '..' / 'retained')))):
                log.write_text(text)
                with self.assertRaises(ValueError):
                    retain_executable(log, 'lib', True, root / 'never-created', target)
            link = target / 'link'
            link.symlink_to(executable)
            log.write_text(json.dumps(dict(item, executable=str(link))))
            with self.assertRaises(ValueError):
                retain_executable(log, 'lib', True, root / 'never-created', target)


if __name__ == '__main__':
    unittest.main()
