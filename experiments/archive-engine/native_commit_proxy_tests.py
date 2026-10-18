"""Traffic admission checks without sockets, SQL or the product runtime."""
import asyncio
from contextlib import redirect_stdout
import io
import json
import struct
import unittest

from native_commit_proxy import MAX_WIRE, Proxy


class TrafficAdmission(unittest.IsolatedAsyncioTestCase):
    async def test_frame_boundary_and_original_default(self):
        for profile, limit in [('default', 20000), ('retained-catch-up', 131072)]:
            with self.subTest(profile=profile):
                proxy = Proxy('pass', profile=profile)
                stream = asyncio.StreamReader()
                stream.feed_data((b'Z' + struct.pack('!I', 5) + b'I') * 2)
                stream.feed_eof()
                proxy.report['frames'] = limit - 1
                self.assertEqual(await proxy.frame(stream), (b'Z', struct.pack('!I', 5), b'I'))
                self.assertEqual(proxy.report['frames'], limit)
                with self.assertRaisesRegex(ValueError, '^budget$'):
                    await proxy.frame(stream)

    async def test_retained_scan_can_cross_original_limit(self):
        proxy = Proxy('after-complete', profile='retained-catch-up')
        proxy.report['frames'] = 20000
        stream = asyncio.StreamReader()
        stream.feed_data(b'Z' + struct.pack('!I', 5) + b'I')
        self.assertEqual((await proxy.frame(stream))[2], b'I')
        self.assertEqual(proxy.report['frames'], 20001)

    async def test_wire_ceiling_is_unchanged(self):
        self.assertEqual(MAX_WIRE, 64 * 1024 * 1024)
        for profile in ('default', 'retained-catch-up'):
            with self.subTest(profile=profile):
                proxy = Proxy('pass', profile=profile)
                proxy.report['bytes'] = MAX_WIRE - 6
                stream = asyncio.StreamReader()
                stream.feed_data((b'Z' + struct.pack('!I', 5) + b'I') * 2)
                await proxy.frame(stream)
                self.assertEqual(proxy.report['bytes'], MAX_WIRE)
                with self.assertRaisesRegex(ValueError, '^budget$'):
                    await proxy.frame(stream)

    async def test_connection_admission_precedes_startup(self):
        class Reader:
            async def readexactly(self, length):
                raise EOFError('admitted startup')
        class Writer:
            closed = False
            def close(self):
                self.closed = True
        for profile, limit in [('default', 16), ('retained-catch-up', 64)]:
            with self.subTest(profile=profile):
                proxy = Proxy('pass', profile=profile)
                proxy.report['connections'] = limit - 1
                last, extra = Writer(), Writer()
                with self.assertRaisesRegex(EOFError, 'admitted startup'):
                    await proxy.session(Reader(), last)
                self.assertFalse(last.closed)
                self.assertEqual(proxy.report['connections'], limit)
                with self.assertRaisesRegex(ValueError, '^connections$'):
                    await proxy.session(Reader(), extra)
                self.assertTrue(extra.closed)

    def test_explicit_profile_reporting_and_legacy_call(self):
        for proxy in (Proxy('pass', 'publication', 120),
                      Proxy('pass', profile='retained-catch-up')):
            stream = io.StringIO()
            with redirect_stdout(stream):
                proxy.emit('ready', port=12345)
            event = json.loads(stream.getvalue())
            if proxy.profile == 'default':
                self.assertFalse({'profile', 'frame_limit', 'connection_limit'} & event.keys())
            else:
                self.assertEqual({k: event[k] for k in ('profile', 'frame_limit', 'connection_limit')},
                                 dict(profile='retained-catch-up', frame_limit=131072, connection_limit=64))
            self.assertEqual(proxy.lifetime_seconds, 120)
        with self.assertRaisesRegex(ValueError, '^profile$'):
            Proxy('pass', profile='unbounded')


if __name__ == '__main__':
    unittest.main()
