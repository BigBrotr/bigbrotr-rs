"""Verifier fault controls only; these never qualify the product or an engine."""
import json
from pathlib import Path
import tempfile
import unittest

from initial_index import (BINARY_EXACT, BINARY_GROUPS, BINARY_REQUIRED, GROUPS, SOURCE_REQUIRED,
                           REQUIRED_MODULES, build_commands, completed_tests,
                           listed_tests, retain_executable, selected_tests,
                           test_commands)


class IndexResultControls(unittest.TestCase):
    def test_binary_scope_requires_all_named_controls(self):
        names = sorted(test for tests in BINARY_REQUIRED.values() for test in tests)
        self.assertEqual(len(names), 108)
        self.assertEqual(len(set(names)), 108)
        self.assertEqual({name: len(tests) for name, tests in BINARY_REQUIRED.items()}, {
            'initial-admission': 3, 'initial-capacity': 4, 'initial-sync': 4, 'initial-archive': 19,
            'initial-index': 30, 'initial-commands': 13, 'initial-sync-diagnostic': 8, 'initial-supervision': 6,
            'initial-watchdog': 2, 'initial-cli': 1, 'index-cli': 1,
            'initial-readiness': 3, 'role-status-http': 3, 'ordinary-observations': 9, 'initial-role-route': 1, 'ordinary-reader-budget': 1,
        })
        self.assertEqual(sum('::index::tests::' in name for name in names), 7)
        self.assertEqual(sum('::index::prepare::tests::' in name for name in names), 7)
        self.assertEqual(sum('::index::catch_up::tests::' in name for name in names), 9)

        def select(inventory):
            return selected_tests(inventory, groups=BINARY_GROUPS, required_modules=(),
                                  required_tests=BINARY_REQUIRED)

        text = '\n'.join(name + ': test' for name in names) + '\n\n108 tests, 0 benchmarks\n'
        selected = select(listed_tests(text))
        self.assertEqual({key: sorted(value) for key, value in BINARY_REQUIRED.items()}, selected)
        # Unrelated compiled CLI tests are not part of this focused profile.
        self.assertEqual(select(names + ['interfaces::unrelated::fixture']), selected)
        for name in names:
            without = [test for test in names if test != name]
            for invalid in (without, without + [name + '_different'], names + [name]):
                with self.subTest(name=name), self.assertRaises(ValueError):
                    select(invalid)
        for prefix in BINARY_GROUPS.values():
            with self.assertRaises(ValueError):
                select([name for name in names if not name.startswith(prefix)])
            with self.assertRaises(ValueError):
                select(names + [prefix + 'unexpected'])
        # The old library-only inventory cannot satisfy a binary module.
        with self.assertRaises(ValueError):
            select([prefix + 'control' for prefix in GROUPS.values()])
        for invalid in (text.replace('108 tests', '107 tests'),
                        text.replace(names[0] + ': test', names[1] + ': test'),
                        '0 tests, 0 benchmarks\n'):
            with self.assertRaises(ValueError):
                listed_tests(invalid)

    def test_source_and_connection_scope_requires_all_thirty_named_controls(self):
        names = sorted(test for tests in SOURCE_REQUIRED.values() for test in tests)
        self.assertEqual({key: len(tests) for key, tests in SOURCE_REQUIRED.items()},
                         {'funded-reservation': 4, 'funded-dns': 1,
                          'postgres-route': 4, 'source-closure': 4, 'lineage-pending': 5, 'live-pending': 1, 'live-census': 1,
                          'closed-reclamation':7,'compacted-progress':2,'compacted-live':1})
        self.assertEqual(len(names), 30)
        groups = {name: GROUPS[name] for name in SOURCE_REQUIRED}

        def select(inventory):
            return selected_tests(inventory, groups=groups, required_modules=(),
                                  required_tests=SOURCE_REQUIRED)

        expected = {name: sorted(tests) for name, tests in SOURCE_REQUIRED.items()}
        self.assertEqual(select(names), expected)
        self.assertEqual(select(names + ['unrelated::control']), expected)
        for name in names:
            without = [test for test in names if test != name]
            for invalid in (without, without + [name + '_different'], names + [name]):
                with self.subTest(name=name), self.assertRaises(ValueError):
                    select(invalid)
        for prefix in groups.values():
            with self.assertRaises(ValueError):
                select(names + [prefix + 'unexpected'])

    def test_actual_binary_target_is_compiled_and_each_group_runs_its_retained_elf(self):
        commands = dict(build_commands())
        self.assertEqual(list(commands), ['clippy', 'compile', 'compile-tests'])
        for command in commands.values():
            for flag in ('--locked', '--offline', '--release', '--lib', '--bin'):
                self.assertIn(flag, command)
            self.assertEqual(command[command.index('--bin') + 1], 'bigbrotr')
        self.assertEqual(commands['compile-tests'][:2], ['cargo', 'test'])
        self.assertIn('--no-run', commands['compile-tests'])
        self.assertEqual(commands['clippy'][-3:], ['--', '-D', 'warnings'])
        self.assertIn('--tests', commands['clippy'])
        artifact_root = Path('/synthetic/retained-artifacts')
        selected = dict(test_commands(artifact_root))
        self.assertEqual(set(selected), set(GROUPS) | set(BINARY_GROUPS))
        for groups, executable in ((GROUPS, 'bigbrotr-lib-tests'),
                                   (BINARY_GROUPS, 'bigbrotr-bin-tests')):
            for name, prefix in groups.items():
                command = selected[name]
                self.assertEqual(command[:2], [str(artifact_root / executable), prefix])
                self.assertEqual(command[-1], '--test-threads=1')
                self.assertEqual('--exact' in command, name in BINARY_EXACT)
                self.assertEqual('--show-output' in command,
                                 name in ('initial-sync', 'initial-archive', 'initial-index'))
                self.assertNotIn('--nocapture', command)

    def test_binary_results_cannot_omit_replace_ignore_or_only_report_a_count(self):
        for group, expected in BINARY_REQUIRED.items():
            lines = ['test ' + name + ' ... ok' for name in expected]
            summary = (f'test result: ok. {len(expected)} passed; 0 failed; 0 ignored; '
                       '0 measured; 999 filtered out; finished in 0.01s')
            valid = '\n'.join([*lines, summary]) + '\n'
            completed_tests(valid, expected)
            for invalid in (
                summary + '\n',
                '\n'.join([*lines[1:], summary]) + '\n',
                valid.replace(expected[0] + ' ... ok', expected[0] + '_different ... ok'),
                valid.replace(f'{len(expected)} passed', f'{len(expected) - 1} passed'),
                valid.replace('0 ignored', '1 ignored'),
                valid.replace(expected[0] + ' ... ok', expected[0] + ' ... ignored'),
                lines[0] + '\n' + valid,
            ):
                with self.subTest(group=group), self.assertRaises(ValueError):
                    completed_tests(invalid, expected)

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

    def test_binary_test_artifact_is_distinct_from_library_and_ordinary_cli(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / 'target'
            target.mkdir()
            entries = []
            for name, kind, is_test in [('cli', 'bin', False), ('library', 'lib', True),
                                       ('binary-tests', 'bin', True)]:
                path = target / name
                path.write_bytes(('synthetic compiler identity ' + name).encode())
                entries.append(dict(reason='compiler-artifact', executable=str(path),
                                    target=dict(name='bigbrotr', kind=[kind]),
                                    profile=dict(test=is_test)))
            log = root / 'compile.log'
            log.write_text('\n'.join(json.dumps(item) for item in entries) + '\n')
            retained = root / 'retained-bin-tests'
            retain_executable(log, 'bin', True, retained, target)
            self.assertEqual(retained.read_bytes(), (target / 'binary-tests').read_bytes())
            for invalid in (entries[:2], entries + [entries[-1]],
                            [dict(entries[-1], target=dict(name='other', kind=['bin']))]):
                log.write_text('\n'.join(json.dumps(item) for item in invalid) + '\n')
                with self.assertRaises(ValueError):
                    retain_executable(log, 'bin', True, root / 'not-created', target)


if __name__ == '__main__':
    unittest.main()
