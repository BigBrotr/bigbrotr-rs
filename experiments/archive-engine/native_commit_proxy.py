#!/usr/bin/env python3
"""Owned plaintext loopback lab only: lose the reply to one native data COMMIT.

DuckLake uses several native connections and commits during attach. Target only
the connection that submitted an INSERT containing ducklake_data_file; other
transactions pass. Log allowlisted counters, never SQL/authentication payloads.
"""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import struct
import sys

MAX_FRAME = 8 * 1024 * 1024
MAX_WIRE = 64 * 1024 * 1024


class Proxy:
    def __init__(self, mode):
        self.mode = mode
        self.writers = []
        self.tasks = []
        self.report = dict(connections=0, bytes=0, frames=0, data_queries=0,
                           data_commits=0, server_complete=0, client_complete=0, faults=0)

    def emit(self, event, **extra):
        print(json.dumps(dict(event=event, mode=self.mode, **self.report, **extra)), flush=True)

    async def frame(self, reader):
        tag = await reader.read(1)
        if not tag:
            return None
        header = await reader.readexactly(4)
        length = struct.unpack('!I', header)[0]
        if not 4 <= length <= MAX_FRAME:
            raise ValueError('frame')
        self.report['bytes'] += length + 1
        self.report['frames'] += 1
        if self.report['bytes'] > MAX_WIRE or self.report['frames'] > 20000:
            raise ValueError('budget')
        return tag, header, await reader.readexactly(length - 4)

    async def session(self, reader, writer):
        self.report['connections'] += 1
        if self.report['connections'] > 16:
            writer.close()
            raise ValueError('connections')
        self.writers.append(writer)
        header = await reader.readexactly(4)
        length = struct.unpack('!I', header)[0]
        if not 8 <= length <= 65536:
            raise ValueError('startup')
        startup = await reader.readexactly(length - 4)
        if startup[:4] != b'\0\3\0\0':
            raise ValueError('plaintext-required')
        upstream, upstream_writer = await asyncio.open_connection('127.0.0.1', 5432)
        self.writers.append(upstream_writer)
        upstream_writer.write(header + startup)
        await upstream_writer.drain()
        state = dict(data=False, commit=False)

        async def pump(source, destination, frontend):
            while True:
                frame = await self.frame(source)
                if frame is None:
                    return
                tag, header, body = frame
                if frontend and tag == b'Q':
                    query = body.upper()
                    if b'INSERT INTO' in query and b'DUCKLAKE_DATA_FILE' in query:
                        state['data'] = True
                        self.report['data_queries'] += 1
                    if query.rstrip(b'\0; \r\n') == b'COMMIT' and state['data']:
                        state['commit'] = True
                        self.report['data_commits'] += 1
                        if self.mode == 'before-forward' and not self.report['faults']:
                            self.report['faults'] += 1
                            self.emit('fault')
                            return
                if not frontend and tag == b'C' and body == b'COMMIT\0' and state['commit']:
                    self.report['server_complete'] += 1
                    state['commit'] = False
                    state['data'] = False
                    if self.mode == 'after-complete' and not self.report['faults']:
                        self.report['faults'] += 1
                        self.emit('fault')
                        return
                    self.report['client_complete'] += 1
                destination.write(tag + header + body)
                await destination.drain()

        pumps = [asyncio.create_task(pump(reader, upstream_writer, True)),
                 asyncio.create_task(pump(upstream, writer, False))]
        try:
            done, _ = await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            for task in pumps:
                task.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            writer.close()
            upstream_writer.close()
            await asyncio.gather(writer.wait_closed(), upstream_writer.wait_closed(), return_exceptions=True)

    async def run(self):
        def accept(reader, writer):
            self.tasks.append(asyncio.create_task(self.session(reader, writer)))
        server = await asyncio.start_server(accept, '127.0.0.1', 0, backlog=8)
        self.emit('ready', port=server.sockets[0].getsockname()[1],
                  source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        control = asyncio.StreamReader(limit=32)
        transport, _ = await asyncio.get_running_loop().connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(control), sys.stdin.buffer)
        try:
            assert await asyncio.wait_for(control.readline(), 120) == b'stop\n'
        finally:
            server.close()
            await server.wait_closed()
            transport.close()
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            outcomes = await asyncio.gather(*self.tasks, return_exceptions=True)
            for writer in self.writers:
                writer.close()
            await asyncio.gather(*(w.wait_closed() for w in self.writers), return_exceptions=True)
        # Normal peer close/reset following deliberate truncation is expected.
        unexpected = sum(isinstance(r, BaseException) and not isinstance(r,
            (ConnectionError, asyncio.IncompleteReadError, asyncio.CancelledError)) for r in outcomes)
        self.emit('terminal', unexpected=unexpected)
        return 0 if not unexpected else 3


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['pass', 'before-forward', 'after-complete'], required=True)
    try:
        sys.exit(asyncio.run(Proxy(parser.parse_args().mode).run()))
    except Exception:
        print('native commit proxy failed', file=sys.stderr)
        sys.exit(3)
