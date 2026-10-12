"""Real loopback framing check for the fixture, not PostgreSQL evidence."""
import asyncio
import json
from pathlib import Path
import struct
import sys
import unittest

SCRIPT = Path(__file__).with_name('rollback_proxy.py')
frame = lambda tag, body: tag + struct.pack('!I', len(body) + 4) + body


class RollbackProxyTest(unittest.IsolatedAsyncioTestCase):
    async def test_holds_only_selected_rollback_reply_after_server_completion(self):
        await self.check_selected_rollback(False)

    async def test_holds_selected_rollback_before_forwarding(self):
        await self.check_selected_rollback(True)

    async def check_selected_rollback(self, before):
        self.assertTrue(SCRIPT.is_file(), 'rollback proxy is absent')
        sockets, tasks = [], []
        async def backend(reader, writer):
            sockets.append(writer)
            size = struct.unpack('!I', await reader.readexactly(4))[0]
            await reader.readexactly(size - 4)
            for _ in range(2):
                self.assertEqual(await reader.readexactly(14), frame(b'Q', b'ROLLBACK\0'))
                writer.write(frame(b'C', b'ROLLBACK\0') + frame(b'Z', b'I'))
                await writer.drain()
            self.assertEqual(await reader.read(), b'')
        def accept(reader, writer):
            tasks.append(asyncio.create_task(backend(reader, writer)))
        server = await asyncio.start_server(accept, '127.0.0.1', 0)
        process = await asyncio.create_subprocess_exec(sys.executable, str(SCRIPT),
            '--upstream-port', str(server.sockets[0].getsockname()[1]),
            '--rollback-number', '2', '--listen-port', '0',
            *(['--before-forward'] if before else []),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        async def event(wanted):
            while True:
                value = json.loads(await asyncio.wait_for(process.stdout.readline(), 3))
                if value['event'] == wanted:
                    return value
        try:
            ready = await event('ready')
            reader, writer = await asyncio.open_connection('127.0.0.1', ready['listen_port'])
            sockets.append(writer)
            writer.write(struct.pack('!II', 8, 196608))
            writer.write(frame(b'Q', b'ROLLBACK\0'))
            await writer.drain()
            expected = frame(b'C', b'ROLLBACK\0') + frame(b'Z', b'I')
            self.assertEqual(await reader.readexactly(len(expected)), expected)
            writer.write(frame(b'Q', b'ROLLBACK\0')); await writer.drain()
            held = await event('held')
            self.assertEqual((held['rollback_requests'], held['rollback_completions'], held['rollback_released'], held['rollback_ready']), (2, 1 if before else 2, 1, 1))
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(reader.read(1), .05)
            process.stdin.write(b'release\n'); await process.stdin.drain()
            self.assertEqual(await reader.readexactly(len(expected)), expected)
            writer.close(); await writer.wait_closed()
            receipt = await event('receipt')
            self.assertEqual((receipt['rollback_requests'], receipt['rollback_completions'], receipt['rollback_released'], receipt['rollback_ready']), (2, 2, 2, 2))
            self.assertEqual(await process.wait(), 0)
            self.assertEqual(await process.stderr.read(), b'')
            await asyncio.gather(*tasks)
        finally:
            if process.returncode is None:
                process.terminate(); await process.wait()
            server.close()
            for writer in sockets: writer.close()
            for task in tasks: task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.wait_for(server.wait_closed(), 3)


if __name__ == '__main__':
    unittest.main()
