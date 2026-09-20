"""Synthetic protocol discriminator for the actual proxy, not engine evidence."""
import asyncio
import importlib.util
from pathlib import Path
import struct
import unittest
from unittest.mock import patch

path = Path(__file__).resolve().parents[1] / 'archive-engine/native_commit_proxy.py'
spec = importlib.util.spec_from_file_location('commit_wire_proxy', path)
proxy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proxy)


def frame(tag, body):
    return tag + struct.pack('!I', len(body) + 4) + body


class Wire(unittest.IsolatedAsyncioTestCase):
    async def test_actual_proxy_counts_session_but_not_following_audit(self):
        observer = proxy.Proxy('pass', 'control-session-issue')
        queries = [b'INSERT INTO control.browser_sessions(session_id) VALUES($1)',
                   b'INSERT INTO control.management_audit(audit_id) VALUES($1)']
        upstream_tasks, proxy_tasks = [], []

        async def backend(reader, writer):
            try:
                length = struct.unpack('!I', await reader.readexactly(4))[0]
                await reader.readexactly(length - 4)
                writer.write(frame(b'Z', b'I'))
                await writer.drain()
                for query in queries:
                    message = await observer.frame(reader)
                    self.assertEqual(message[0], b'P')
                    self.assertIn(query, message[2])
                    self.assertEqual((await observer.frame(reader))[0], b'S')
                    writer.write(frame(b'1', b'') + frame(b'Z', b'T'))
                    await writer.drain()
                    for tag in [b'B', b'E', b'S']:
                        self.assertEqual((await observer.frame(reader))[0], tag)
                    writer.write(frame(b'2', b'') + frame(b'C', b'INSERT 0 1\0') + frame(b'Z', b'T'))
                    await writer.drain()
                self.assertEqual((await observer.frame(reader))[0], b'Q')
                writer.write(frame(b'C', b'COMMIT\0') + frame(b'Z', b'I'))
                await writer.drain()
                await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()

        upstream = await asyncio.start_server(
            lambda r, w: upstream_tasks.append(asyncio.create_task(backend(r, w))), '127.0.0.1', 0)
        listener = await asyncio.start_server(
            lambda r, w: proxy_tasks.append(asyncio.create_task(observer.session(r, w))), '127.0.0.1', 0)
        connect = asyncio.open_connection

        async def redirect(host, port, *args, **kwargs):
            if port == 5432:
                port = upstream.sockets[0].getsockname()[1]
            return await connect(host, port, *args, **kwargs)

        writer = None
        try:
            with patch.object(proxy.asyncio, 'open_connection', redirect):
                reader, writer = await connect('127.0.0.1', listener.sockets[0].getsockname()[1])
                startup = b'\0\3\0\0user\0fixture\0\0'
                writer.write(struct.pack('!I', len(startup) + 4) + startup)
                await writer.drain()
                self.assertEqual((await observer.frame(reader))[0], b'Z')
                for number, query in enumerate(queries):
                    name = str(number).encode()
                    writer.write(frame(b'P', name + b'\0' + query + b'\0\0\0') + frame(b'S', b''))
                    await writer.drain()
                    for tag in [b'1', b'Z']:
                        self.assertEqual((await observer.frame(reader))[0], tag)
                    writer.write(frame(b'B', b'\0' + name + b'\0' + b'\0' * 6) +
                                 frame(b'E', b'\0' * 5) + frame(b'S', b''))
                    await writer.drain()
                    for tag in [b'2', b'C', b'Z']:
                        self.assertEqual((await observer.frame(reader))[0], tag)
                writer.write(frame(b'Q', b'COMMIT\0'))
                await writer.drain()
                for tag in [b'C', b'Z']:
                    self.assertEqual((await observer.frame(reader))[0], tag)
                writer.close()
                await writer.wait_closed()
                await asyncio.wait_for(asyncio.gather(*proxy_tasks, *upstream_tasks), 3)
            for key in ['data_queries', 'data_commits', 'session_mutations_complete',
                        'server_complete', 'client_complete']:
                self.assertEqual(observer.report[key], 1, key)
        finally:
            if writer is not None:
                writer.close()
                await writer.wait_closed()
            listener.close()
            upstream.close()
            await listener.wait_closed()
            await upstream.wait_closed()
            for task in proxy_tasks + upstream_tasks:
                task.cancel()
            await asyncio.gather(*proxy_tasks, *upstream_tasks, return_exceptions=True)


if __name__ == '__main__':
    unittest.main()
