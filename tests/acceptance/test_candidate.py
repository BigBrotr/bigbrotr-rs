"""Focused offline intake controls using the assembler's synthetic native fixture."""
import copy
import gzip
import io
import json
import stat
import tarfile
import unittest

import candidate
import test_assemble as fixture


class CandidateIntake(unittest.TestCase):
    def setUp(self):
        # Compose only the existing setup/cleanup, without inheriting or running
        # the unrelated assembler test methods.
        self.fixture = fixture.CandidateAssembly('test_deterministic_closed_bundle_and_detached_acyclic_identity')
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.home, self.source = self.fixture.home, self.fixture.source
        for name in fixture.assembler.JOURNEY_FILES:
            path = self.source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((fixture.ROOT / name).read_bytes())
        (self.source / '.gitignore').write_text('.research/\n')
        self.fixture.commit()
        self.value = self.fixture.run_assembly(include_journey=True)
        self.manifest = self.home / 'candidate/candidate-manifest.json'
        self.archive = self.manifest.parent / self.value['bundle']['name']

    def save(self, value):
        self.manifest.chmod(0o600)
        fixture.write(self.manifest, value)

    def extract(self, destination=None):
        return candidate.extract_candidate(self.manifest, destination or self.home / 'bundle',
            source_root=self.source)

    def rewrite(self, transform, *, tar_format=tarfile.USTAR_FORMAT):
        with tarfile.open(self.archive) as archive:
            members = [(copy.copy(entry), archive.extractfile(entry).read() if entry.isfile() else None)
                       for entry in archive]
        transform(members)
        self.archive.chmod(0o600)
        with tarfile.open(self.archive, 'w:gz', format=tar_format) as archive:
            for entry, data in members:
                archive.addfile(entry, None if data is None else io.BytesIO(data))
        self.value['bundle'].update(fixture.assembler.identity(self.archive))
        self.save(self.value)

    def inventory(self, members, transform):
        for index, (entry, raw) in enumerate(members):
            if entry.name == 'members.json':
                value = json.loads(raw)
                transform(value)
                data = fixture.assembler.json_bytes(value)
                members[index] = (entry, data)
                entry.size = len(data)
                self.value['inventory'].update(bytes=len(data), sha256=fixture.sha(data))
                return
        self.fail('synthetic inventory missing')

    def rebind_file(self, members, name, data):
        for index, (entry, _) in enumerate(members):
            if entry.name == name:
                entry.size = len(data); members[index] = (entry, data)
                self.inventory(members, lambda value: value['files'][name].update(
                    bytes=len(data), sha256=fixture.sha(data)))
                return
        self.fail('synthetic member missing')

    def test_closed_bundle_extracts_outside_and_inside_ignored_research(self):
        for destination in (self.home / 'bundle', self.source / '.research/evidence/bundle',
                self.manifest.parent / 'bundle'):
            destination.parent.mkdir(parents=True, exist_ok=True)
            result = self.extract(destination)
            self.assertEqual(set(result), {'manifest', 'manifest_sha256', 'bundle_root', 'bundle_sha256'})
            self.assertEqual(result['manifest'], self.value)
            self.assertEqual(result['manifest_sha256'], fixture.sha(self.manifest.read_bytes()))
            self.assertEqual(result['bundle_sha256'], self.value['bundle']['sha256'])
            self.assertEqual(result['bundle_root'], str(destination))
            self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o700)
            inventory = json.loads((destination / 'members.json').read_text())['files']
            self.assertEqual({p.relative_to(destination).as_posix() for p in destination.rglob('*') if p.is_file()},
                set(inventory) | {'members.json'})
            for name, record in inventory.items():
                self.assertEqual(fixture.assembler.identity(destination / name),
                    {key: record[key] for key in ('bytes', 'sha256')})
                self.assertEqual(stat.S_IMODE((destination / name).stat().st_mode), record['mode'])

    def test_destination_refusals_preserve_inputs_and_existing_owners(self):
        existing = self.home / 'existing'; existing.mkdir(); (existing / 'keep').write_bytes(b'owned')
        for destination in (existing, self.source, self.source.parent, self.source / 'bundle',
                self.source / '.research', self.manifest, self.archive,
                self.home / 'missing/../escape'):
            with self.subTest(path=destination), self.assertRaises((ValueError, OSError)):
                self.extract(destination)
        self.assertEqual((existing / 'keep').read_bytes(), b'owned')
        self.assertEqual(fixture.sha(self.archive.read_bytes()), self.value['bundle']['sha256'])
        (self.source / '.research').mkdir()
        (self.source / '.research/linked').symlink_to(existing, target_is_directory=True)
        with self.assertRaises(ValueError): self.extract(self.source / '.research/linked/bundle')
        (self.home / 'link').symlink_to(self.home / 'absent')
        with self.assertRaises(ValueError): self.extract(self.home / 'link')
        (self.source / '.gitignore').write_text('')
        with self.assertRaisesRegex(ValueError, 'ignored by Git'):
            self.extract(self.source / '.research/unignored')

    def test_linked_manifest_archive_and_source_are_refused(self):
        alias = self.home / 'manifest-link'; alias.symlink_to(self.manifest)
        with self.assertRaises(ValueError):
            candidate.extract_candidate(alias, self.home / 'bundle', source_root=self.source)
        alias.unlink(); alias.symlink_to(self.source, target_is_directory=True)
        with self.assertRaises(ValueError):
            candidate.extract_candidate(self.manifest, self.home / 'bundle', source_root=alias)
        original = self.home / 'original.tar.gz'; self.archive.rename(original)
        self.archive.symlink_to(original)
        with self.assertRaises(ValueError): self.extract()
        self.assertFalse((self.home / 'bundle').exists())

    def test_manifest_is_closed_at_every_declared_boundary(self):
        cases = [([], 'extra', True), ([], 'open_gates', []), ([], 'journey_files', []),
            (['assembly'], 'extra', True), (['assembly', 'packing'], 'gzip_level', 6.0),
            (['assembly', 'packing'], 'extra', True), (['assembly', 'files_sha256'], 'extra', 'a' * 64),
            (['artifact'], 'extra', True), (['artifact'], 'build_result_sha256', 'invalid'),
            (['artifact', 'source'], 'extra', True), (['artifact', 'source'], 'dirty', 0),
            (['artifact', 'native_toolchain'], 'extra', True),
            (['artifact', 'native_toolchain', 'tools_sha256'], 'extra', 'a' * 64),
            (['artifact', 'build_info'], 'extra', True),
            (['artifact', 'build_info'], 'protocols', {'one': False}),
            (['artifact', 'build_info', 'inputs_sha256'], 'extra', 'a' * 64),
            (['artifact', 'binary'], 'bytes', True), (['bundle'], 'extra', True),
            (['bundle'], 'name', '../escape')]
        for path, key, value in cases:
            changed = copy.deepcopy(self.value); target = changed
            for part in path: target = target[part]
            target[key] = value; self.save(changed)
            with self.subTest(path=path, key=key), self.assertRaises(ValueError): self.extract()
            self.assertFalse((self.home / 'bundle').exists())
        for path in ([], ['assembly'], ['assembly', 'packing'], ['artifact'], ['artifact', 'source'],
                ['artifact', 'native_toolchain'], ['artifact', 'build_info']):
            changed = copy.deepcopy(self.value); target = changed
            for part in path: target = target[part]
            del target[next(iter(target))]; self.save(changed)
            with self.subTest(missing=path), self.assertRaises(ValueError): self.extract()
        self.manifest.write_text('{"format":1,"format":2}')
        with self.assertRaisesRegex(ValueError, 'duplicate JSON'): self.extract()

    def test_bad_tar_members_types_modes_and_hidden_metadata_are_refused(self):
        original = self.archive.read_bytes(); original_value = copy.deepcopy(self.value)
        def extra(members):
            entry = tarfile.TarInfo('unselected'); entry.size = 1; members.append((entry, b'x'))
        def hidden_longname(members):
            entry = tarfile.TarInfo('././@LongLink'); entry.type = tarfile.GNUTYPE_LONGNAME
            data = (members[0][0].name + '\0').encode(); entry.size = len(data)
            members.insert(0, (entry, data))
        changes = [extra, lambda members: members.pop(), lambda members: members.append(members[0]),
            lambda members: setattr(members[0][0], 'name', '../escape'),
            lambda members: setattr(members[0][0], 'mode', 0o777),
            lambda members: setattr(members[0][0], 'uid', 1),
            lambda members: setattr(members[0][0], 'linkname', 'unexpected'), hidden_longname]
        for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE):
            def special(members, kind=kind):
                index = next(i for i, (entry, _) in enumerate(members) if entry.name == 'bigbrotr')
                entry, _ = members[index]; entry.type = kind; entry.size = 0
                members[index] = (entry, None)
            changes.append(special)
        for number, change in enumerate(changes):
            self.archive.chmod(0o600); self.archive.write_bytes(original)
            self.value = copy.deepcopy(original_value)
            self.rewrite(change)
            with self.subTest(number=number), self.assertRaises(ValueError):
                self.extract(self.home / ('bad-' + str(number)))
        self.archive.write_bytes(original); self.value = copy.deepcopy(original_value)
        self.rewrite(lambda members: None, tar_format=tarfile.GNU_FORMAT)
        with self.assertRaisesRegex(ValueError, 'USTAR'): self.extract()

    def test_tar_ending_cannot_hide_extra_or_truncated_content(self):
        original = self.archive.read_bytes()
        raw = gzip.decompress(original)
        for number, data in enumerate((raw + b'hidden', raw[:-512])):
            self.archive.chmod(0o600); self.archive.write_bytes(gzip.compress(data, mtime=0))
            self.value['bundle'].update(fixture.assembler.identity(self.archive)); self.save(self.value)
            with self.subTest(number=number), self.assertRaisesRegex(ValueError, 'tar ending'):
                self.extract(self.home / ('bad-' + str(number)))

    def test_inventory_missing_extra_mistyped_and_rebound_native_hashes_fail(self):
        original = self.archive.read_bytes(); original_value = copy.deepcopy(self.value)
        changes = [lambda value: value['files'].pop('bigbrotr'),
            lambda value: value['files'].update(secret={'bytes': 1, 'sha256': 'a' * 64, 'mode': 0o444}),
            lambda value: value['files']['bigbrotr'].update(bytes=True),
            lambda value: value['files']['bigbrotr'].update(mode=True),
            lambda value: value['files']['bigbrotr'].update(extra=1),
            lambda value: value['files']['archive-worker/lib/libduckdb.so'].update(sha256='a' * 64)]
        for number, change in enumerate(changes):
            self.archive.chmod(0o600); self.archive.write_bytes(original)
            self.value = copy.deepcopy(original_value)
            self.rewrite(lambda members: self.inventory(members, change))
            with self.subTest(number=number), self.assertRaises(ValueError):
                self.extract(self.home / ('bad-' + str(number)))

    def test_archive_and_member_tamper_fail_without_executing_native_bytes(self):
        original = self.archive.read_bytes()
        self.archive.chmod(0o600); self.archive.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
        with self.assertRaisesRegex(ValueError, 'archive identity'): self.extract()
        self.archive.write_bytes(original)
        def tamper(members):
            index = next(i for i, (entry, _) in enumerate(members) if entry.name == 'bigbrotr')
            entry, data = members[index]; members[index] = (entry, data[:-1] + b'X')
        self.rewrite(tamper)
        destination = self.home / 'bundle'
        with self.assertRaisesRegex(ValueError, 'hash mismatch'): self.extract(destination)
        self.assertTrue(destination.is_dir())  # Partial intake is retained, not relabeled success.
        self.assertFalse((destination / 'bigbrotr').exists())
        self.assertFalse((self.home / 'SHOULD_NOT_EXIST').exists())

    def test_native_registry_and_source_members_bind_independently_of_tar_inventory(self):
        changed = copy.deepcopy(self.value); changed['artifact']['registry_sha256'] = 'a' * 64
        self.save(changed)
        with self.assertRaisesRegex(ValueError, 'native registry'): self.extract()
        self.save(self.value)
        path = self.source / fixture.assembler.JOURNEY_FILES[0]
        original = path.read_bytes(); path.write_bytes(original + b'changed')
        with self.assertRaisesRegex(ValueError, 'assembly input differs'): self.extract()
        path.write_bytes(original)
        def tamper(members):
            name = 'archive-worker/lib/libduckdb.so'
            self.rebind_file(members, name, b'X' + self.fixture.library[1:])
        self.rewrite(tamper)
        with self.assertRaisesRegex(ValueError, 'member binding differs'): self.extract()
        self.assertFalse((self.home / 'bundle').exists())

    def test_rebound_non_elf_and_aliased_build_info_are_refused(self):
        original = self.archive.read_bytes(); original_value = copy.deepcopy(self.value)
        for section, key, scalar in (('source', 'dirty', 0), ('archive_worker', 'bytes',
                float(self.value['artifact']['build_info']['archive_worker']['bytes']))):
            self.archive.chmod(0o600); self.archive.write_bytes(original)
            self.value = copy.deepcopy(original_value)
            def wrong_info(members):
                info = copy.deepcopy(self.value['artifact']['build_info'])
                info[section][key] = scalar
                self.rebind_file(members, 'build-info.json', fixture.assembler.json_bytes(info))
            self.rewrite(wrong_info)
            with self.assertRaisesRegex(ValueError, 'build-info differs'): self.extract()
        self.archive.chmod(0o600); self.archive.write_bytes(original)
        self.value = copy.deepcopy(original_value)
        def non_elf(members):
            data = b'#!/bin/sh\ntouch SHOULD_NOT_EXIST\n'
            self.rebind_file(members, 'bigbrotr', data)
            self.value['artifact']['binary'] = {'bytes': len(data), 'sha256': fixture.sha(data)}
        self.rewrite(non_elf)
        with self.assertRaisesRegex(ValueError, 'ELF'): self.extract()
        self.assertFalse((self.home / 'SHOULD_NOT_EXIST').exists())


if __name__ == '__main__':
    unittest.main()
