"""Asset tampering and extraction controls; these do not qualify engine behavior."""
import io
from pathlib import Path
import stat
import tempfile
import unittest
import zipfile

from tools.archive_bundle import copy_verified, extract_member, verify_file

ABC = {'bytes': 3, 'sha256': 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'}


class ArchiveBundleControls(unittest.TestCase):
    def test_same_length_tamper_truncation_and_excess_are_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            copy_verified(io.BytesIO(b'abc'), root/'good', ABC)
            verify_file(root/'good', ABC)
            self.assertEqual((root/'good').read_bytes(), b'abc')
            for number, data in enumerate([b'abd', b'ab', b'abcd']):
                with self.assertRaises(ValueError):
                    copy_verified(io.BytesIO(data), root/str(number), ABC)
            (root/'link').symlink_to(root/'good')
            with self.assertRaises(ValueError):
                verify_file(root/'link', ABC)

    def test_only_named_zip_member_is_extracted_and_links_are_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = io.BytesIO()
            with zipfile.ZipFile(payload, 'w') as archive:
                archive.writestr('library', b'abc')
                archive.writestr('../outside', b'unselected path must never be extracted')
                link = zipfile.ZipInfo('link')
                link.create_system = 3
                link.external_attr = (stat.S_IFLNK | 0o777) << 16
                archive.writestr(link, b'abc')
            payload.seek(0)
            with zipfile.ZipFile(payload) as archive:
                extract_member(archive, 'library', root/'library', ABC)
                with self.assertRaises(ValueError):
                    extract_member(archive, 'link', root/'link', ABC)
            self.assertEqual(sorted(p.name for p in root.iterdir()), ['library'])


if __name__ == '__main__':
    unittest.main()
