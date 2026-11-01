"""Synthetic-file controls only; never read the actual cache or launch the lab."""
import ast
import errno
import hashlib
import os
from pathlib import Path
import stat
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


LAB = Path(__file__).resolve().parents[2] / 'experiments/network-transport/lab.py'
# The runner has deliberate top-level launch guards. Load only this exact helper
# so these controls cannot inspect Docker or enter the laboratory lifecycle.
function, = [node for node in ast.parse(LAB.read_text()).body
             if isinstance(node, ast.FunctionDef) and node.name == 'original_anchor_sha']
namespace = dict(errno=errno, hashlib=hashlib, os=os, stat=stat, sys=sys)
exec(compile(ast.Module(body=[function], type_ignores=[]), str(LAB), 'exec'), namespace)
original_anchor_sha = namespace['original_anchor_sha']
SUPPORTED = sys.platform == 'linux' and all(hasattr(os, flag)
    for flag in ('O_RDONLY', 'O_NOFOLLOW', 'O_NOATIME'))


def metadata(value):
    return {name: getattr(value, name) for name in dir(value) if name.startswith('st_')}


class UnsupportedAnchorControls(unittest.TestCase):
    def test_unsupported_platform_refuses_before_any_file_access(self):
        path = SimpleNamespace(lstat=lambda: self.fail('unsupported platform accessed the file'))
        with patch.object(sys, 'platform', 'darwin'), patch.object(os, 'open') as opened:
            with self.assertRaises(OSError) as error:
                original_anchor_sha(path)
            self.assertEqual(error.exception.errno, errno.ENOTSUP)
            opened.assert_not_called()

    def test_missing_noatime_flag_has_no_ordinary_read_fallback(self):
        path = SimpleNamespace(lstat=lambda: self.fail('missing O_NOATIME accessed the file'))
        unavailable = SimpleNamespace(O_RDONLY=0, O_NOFOLLOW=1)
        with patch.object(sys, 'platform', 'linux'), patch.dict(namespace, os=unavailable):
            with self.assertRaises(OSError) as error:
                original_anchor_sha(path)
            self.assertEqual(error.exception.errno, errno.ENOTSUP)


@unittest.skipUnless(SUPPORTED, 'actual no-atime reads require Linux and both open flags')
class OriginalAnchorControls(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'retained'
        self.payload = b'bounded synthetic original bytes'
        self.path.write_bytes(self.payload)
        # An ordinary relatime read would advance this old access timestamp.
        os.utime(self.path, ns=(1, 2))

    def test_real_hash_preserves_every_stat_field_including_atime(self):
        before = metadata(self.path.lstat())
        self.assertEqual(original_anchor_sha(self.path), hashlib.sha256(self.payload).hexdigest())
        self.assertEqual(metadata(self.path.lstat()), before)

    def test_permission_and_filesystem_refusals_never_retry_without_flags(self):
        before = metadata(self.path.lstat())
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NOATIME
        for code in (errno.EPERM, errno.EOPNOTSUPP):
            with self.subTest(code=code), patch.object(os, 'open', side_effect=OSError(code, 'fixture refusal')) as opened:
                with self.assertRaises(OSError) as error:
                    original_anchor_sha(self.path)
                self.assertEqual(error.exception.errno, code)
                opened.assert_called_once_with(self.path, flags)
            self.assertEqual(metadata(self.path.lstat()), before)

    def test_symlink_directory_and_multiple_links_refuse_before_open(self):
        alias = self.path.with_name('alias')
        alias.symlink_to(self.path)
        with patch.object(os, 'open') as opened:
            for path in (alias, self.path.parent):
                with self.subTest(path=path), self.assertRaises(ValueError):
                    original_anchor_sha(path)
            opened.assert_not_called()
        alias.unlink()
        os.link(self.path, alias)
        with patch.object(os, 'open') as opened, self.assertRaises(ValueError):
            original_anchor_sha(self.path)
        opened.assert_not_called()

    def test_substituted_open_inode_is_refused_and_descriptor_is_closed(self):
        other = self.path.with_name('other')
        other.write_bytes(self.payload)
        opened = []
        real_open = os.open

        def substitute(path, flags):
            descriptor = real_open(other, flags)
            opened.append(descriptor)
            return descriptor

        with patch.object(os, 'open', side_effect=substitute), self.assertRaisesRegex(ValueError, 'between lstat and open'):
            original_anchor_sha(self.path)
        with self.assertRaises(OSError) as error:
            os.fstat(opened[0])
        self.assertEqual(error.exception.errno, errno.EBADF)

    def test_every_descriptor_stat_field_is_checked_after_hashing(self):
        before = self.path.lstat()
        fields = metadata(before)
        for field, value in fields.items():
            changed = SimpleNamespace(**dict(fields, **{field: value + 1}))
            with self.subTest(field=field), patch.object(os, 'fstat', side_effect=[before, changed]):
                with self.assertRaisesRegex(ValueError, 'metadata changed during hashing'):
                    original_anchor_sha(self.path)

    def test_path_replacement_after_hashing_is_refused(self):
        before = self.path.lstat()
        changed = SimpleNamespace(**dict(metadata(before), st_ino=before.st_ino + 1))
        with patch.object(Path, 'lstat', side_effect=[before, changed]):
            with self.assertRaisesRegex(ValueError, 'metadata changed during hashing'):
                original_anchor_sha(self.path)


if __name__ == '__main__':
    unittest.main()
