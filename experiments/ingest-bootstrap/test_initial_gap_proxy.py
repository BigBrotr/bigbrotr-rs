"""Real loopback framing controls for the new selector; not PG evidence."""
import asyncio
import json
from pathlib import Path
import struct
import sys
import unittest

SCRIPT = Path(__file__).with_name('initial_gap_proxy.py')
START = b'START TRANSACTION ISOLATION LEVEL READ COMMITTED\0'


def frame(tag, body):
    return tag + struct.pack('!I', len(body) + 4) + body


class GapProxyTest(unittest.IsolatedAsyncioTestCase):
    async def test_twelfth_transaction_is_held_before_forward_and_explicitly_closed(self):
        await self.check_boundary('before')

    async def test_second_rollback_is_complete_but_not_delivered(self):
        await self.check_boundary('after')

    async def check_boundary(self, boundary):
        sockets, tasks, starts = [], [], []
        rollbacks = []

        async def backend(reader, writer):
            sockets.append(writer)
            size = struct.unpack('!I', await reader.readexactly(4))[0]
            await reader.readexactly(size - 4)
            while await reader.read(1):
                size = struct.unpack('!I', await reader.readexactly(4))[0]
                body = await reader.readexactly(size - 4)
                self.assertIn(body, (START, b'ROLLBACK\0'))
                if body == START:
                    starts.append(body)
                else:
                    rollbacks.append(body)
                writer.write(frame(b'C', b'BEGIN\0' if body == START else b'ROLLBACK\0')
                    + frame(b'Z', b'T' if body == START else b'I'))
                await writer.drain()

        def accept(reader, writer):
            tasks.append(asyncio.create_task(backend(reader, writer)))

        server = await asyncio.start_server(accept, '127.0.0.1', 0)
        process = await asyncio.create_subprocess_exec(sys.executable, str(SCRIPT),
            '--upstream-port', str(server.sockets[0].getsockname()[1]), '--boundary', boundary,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        async def event(wanted):
            value = json.loads(await asyncio.wait_for(process.stdout.readline(), 3))
            self.assertEqual(value['event'], wanted)
            return value

        try:
            ready = await event('ready')
            reader, writer = await asyncio.open_connection('127.0.0.1', ready['listen_port'])
            sockets.append(writer)
            writer.write(struct.pack('!II', 8, 196608))
            for _ in range(11):
                writer.write(frame(b'Q', START))
                await writer.drain()
                reply = frame(b'C', b'BEGIN\0') + frame(b'Z', b'T')
                self.assertEqual(await asyncio.wait_for(reader.readexactly(len(reply)), 3), reply)
            writer.write(frame(b'Q', b'ROLLBACK\0'))
            await writer.drain()
            rollback_reply = frame(b'C', b'ROLLBACK\0') + frame(b'Z', b'I')
            self.assertEqual(await asyncio.wait_for(reader.readexactly(len(rollback_reply)), 3), rollback_reply)
            writer.write(frame(b'Q', START))
            await writer.drain()
            if boundary == 'after':
                self.assertEqual(await asyncio.wait_for(reader.readexactly(len(reply)), 3), reply)
                writer.write(frame(b'Q', b'ROLLBACK\0'))
                await writer.drain()
            held = await event('held')
            self.assertEqual(held['transaction_starts'], 12)
            self.assertEqual(held['faults'], 1)
            self.assertEqual(len(starts), 11 if boundary == 'before' else 12)
            self.assertEqual(len(rollbacks), 1 if boundary == 'before' else 2)
            self.assertEqual([held[k] for k in ('rollback_requests', 'rollback_completions',
                'rollback_released', 'rollback_ready')], [1, 1, 1, 1] if boundary == 'before' else [2, 2, 1, 1])
            process.stdin.write(b'close\n')
            await process.stdin.drain()
            receipt = await event('receipt')
            self.assertEqual(receipt['reason'], 'held-close')
            self.assertEqual(receipt['transaction_starts'], 12)
            self.assertEqual(await asyncio.wait_for(process.wait(), 3), 0)
            self.assertEqual(await process.stderr.read(), b'')
            self.assertEqual(await asyncio.wait_for(reader.read(), 3), b'')
            await asyncio.wait_for(asyncio.gather(*tasks), 3)
            self.assertEqual(len(starts), 11 if boundary == 'before' else 12)
        finally:
            if process.returncode is None:
                process.kill()
                await asyncio.wait_for(process.wait(), 3)
            server.close()
            for writer in sockets:
                writer.close()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.wait_for(server.wait_closed(), 3)


if __name__ == '__main__':
    unittest.main()
