"""Asset tampering and extraction controls; these do not qualify engine behavior."""
import io
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ssl
import subprocess
import threading
import urllib.request
from unittest.mock import patch
from pathlib import Path
import stat
import tempfile
import unittest
import zipfile

from tools.archive_bundle import copy_verified, extract_member, verify_file, obtain

ABC = {'bytes': 3, 'sha256': 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'}


@contextmanager
def https_asset(root, *, interrupt_first=False, concurrent=False):
    """Real loopback HTTPS; trust only this test's ephemeral public certificate."""
    cert, key = root / 'cert.pem', root / 'key.pem'
    config = root / 'openssl.cnf'
    config.write_text('[req]\ndistinguished_name=dn\nx509_extensions=ext\nprompt=no\n[dn]\nCN=localhost\n[ext]\nsubjectAltName=IP:127.0.0.1\nbasicConstraints=critical,CA:TRUE\n')
    subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                    '-days', '1', '-keyout', str(key), '-out', str(cert),
                    '-config', str(config)], capture_output=True, check=True, timeout=20)
    barrier = threading.Barrier(2, timeout=10) if concurrent else None
    requested = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            with lock:
                requested.append(self.path)
                number = len(requested)
            self.send_response(200)
            self.send_header('Content-Length', '3')
            self.end_headers()
            if barrier:
                barrier.wait()
            self.wfile.write(b'a' if interrupt_first and number == 1 else b'abc')
            self.wfile.flush()
            self.close_connection = True

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    client = ssl.create_default_context(cafile=str(cert))
    real_open = urllib.request.urlopen
    try:
        # This seam injects only the ephemeral test CA; all reads use real TLS,
        # HTTP framing, sockets and the production bounded download implementation.
        with patch('tools.archive_bundle.urllib.request.urlopen',
                   side_effect=lambda request, timeout: real_open(request, timeout=timeout, context=client)):
            yield dict(ABC, url=f'https://127.0.0.1:{server.server_port}/asset'), requested
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        if thread.is_alive():
            raise RuntimeError('HTTPS fixture did not terminate')


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

    def test_failed_copy_cleans_only_its_own_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            destination = root / 'asset'
            for data in (b'abd', b'ab', b'abcd'):
                with self.assertRaises(ValueError):
                    copy_verified(io.BytesIO(data), destination, ABC)
                self.assertFalse(destination.exists())
            class Interrupted(io.BytesIO):
                def read(self, size):
                    if self.tell():
                        raise OSError('synthetic source interruption')
                    return super().read(1)
            with self.assertRaises(OSError):
                copy_verified(Interrupted(b'abc'), destination, ABC)
            self.assertFalse(destination.exists())
            destination.write_bytes(b'previous owner')
            with self.assertRaises(FileExistsError):
                copy_verified(io.BytesIO(b'abc'), destination, ABC)
            self.assertEqual(destination.read_bytes(), b'previous owner')

    def test_interrupted_https_download_can_retry_the_same_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cache = root / 'cache'; cache.mkdir()
            with https_asset(root, interrupt_first=True) as (asset, requests):
                with self.assertRaises((ValueError, OSError)):
                    obtain(asset, cache, True)
                self.assertFalse((cache / ABC['sha256']).exists())
                result = obtain(asset, cache, True)
                self.assertEqual(result.read_bytes(), b'abc')
                self.assertEqual(requests, ['/asset', '/asset'])
                self.assertEqual(obtain(asset, cache, False), result)
                self.assertEqual(list(cache.iterdir()), [result])

    def test_concurrent_https_downloads_publish_one_verified_asset(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cache = root / 'cache'; cache.mkdir()
            # An interrupted older process's deterministic name must not poison
            # retries. It is not ours to delete while another invocation may run.
            stale = cache / (ABC['sha256'] + '.partial')
            stale.write_bytes(b'old partial')
            with https_asset(root, concurrent=True) as (asset, requests):
                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = [executor.submit(obtain, asset, cache, True) for _ in range(2)]
                    results = [future.result(timeout=15) for future in futures]
                self.assertEqual(results[0], results[1])
                self.assertEqual(results[0].read_bytes(), b'abc')
                self.assertEqual(stat.S_IMODE(results[0].stat().st_mode), 0o444)
                self.assertEqual(requests, ['/asset', '/asset'])
                self.assertEqual(stale.read_bytes(), b'old partial')
                self.assertEqual(set(cache.iterdir()), {stale, results[0]})


if __name__ == '__main__':
    unittest.main()
