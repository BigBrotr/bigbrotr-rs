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
        self.held = False
        self.action = None
        self.release = asyncio.Event()
        self.report = dict(connections=0, bytes=0, frames=0, data_queries=0,
                           data_commits=0, server_complete=0, client_complete=0, faults=0,
                           owner_requests=0, owner_forwarded=0, owner_complete=0,
                           guard_rejections=0, serialization_rejections=0, rejection_sequence=[])

    def emit(self, event, **extra):
        print(json.dumps(dict(event=event, mode=self.mode, **self.report, **extra)), flush=True)

    async def hold(self, stage, backend_pid):
        if self.mode != 'hold-' + stage or self.held:
            return True
        assert backend_pid is not None
        self.held = True
        self.emit('held', stage=stage, backend_pid=backend_pid)
        await self.release.wait()
        if self.action == 'close':
            self.report['faults'] += 1
            return False
        assert self.action == 'release'
        return True

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
        state = dict(data=False, commit=False, backend_pid=None,
                     owner_statement=None, owner_waiting=False, owner_forwarded=False)
        manager_committed = asyncio.Event()

        async def pump(source, destination, frontend):
            while True:
                try:
                    frame = await self.frame(source)
                except ConnectionError:
                    if not (frontend and self.mode == 'hold-manager-owner' and state['owner_forwarded']):
                        raise
                    frame = None
                if frame is None:
                    if frontend and self.mode == 'hold-manager-owner' and state['owner_forwarded']:
                        # Deliberately keep the lab's server endpoint alive until
                        # explicit stop, even after the real driver is destroyed.
                        await asyncio.Future()
                    return
                tag, header, body = frame
                if frontend and self.mode == 'hold-manager-owner':
                    if tag == b'P':
                        statement, query, _ = body.split(b'\0', 2)
                        if query == b'SELECT ordinal::text,enabled FROM bigbrotr_archive.owner WHERE singleton FOR UPDATE':
                            assert state['owner_statement'] is None and len(statement) <= 64
                            state['owner_statement'] = statement
                    if tag == b'B' and body.split(b'\0', 2)[1] == state['owner_statement']:
                        execute, sync = await self.frame(source), await self.frame(source)
                        assert execute and execute[0] == b'E' and execute[2] == b'\0'*5
                        assert sync and sync[0] == b'S' and sync[2] == b''
                        self.report['owner_requests'] += 1
                        if not await self.hold('manager-owner', state['backend_pid']):
                            return
                        state['owner_waiting'] = state['owner_forwarded'] = True
                        self.report['owner_forwarded'] += 1
                        for parts in (frame, execute, sync):
                            destination.write(b''.join(parts))
                        await destination.drain()
                        continue
                if frontend and tag == b'Q':
                    query = body.upper()
                    # Closed manager fixture: only the generated archive login
                    # marks provisioning, never log its verifier or SQL bytes.
                    if self.mode in ('hold-manager-commit', 'hold-manager-owner') and query.startswith(b'CREATE ROLE "BB_ARCHIVE_'):
                        state['data'] = True
                        self.report['data_queries'] += 1
                    if b'INSERT INTO' in query and b'DUCKLAKE_DATA_FILE' in query:
                        state['data'] = True
                        self.report['data_queries'] += 1
                        if not await self.hold('data', state['backend_pid']):
                            return
                    if query.rstrip(b'\0; \r\n') == b'COMMIT' and state['data']:
                        state['commit'] = True
                        self.report['data_commits'] += 1
                        if not await self.hold('manager-commit', state['backend_pid']):
                            return
                        if not await self.hold('commit', state['backend_pid']):
                            return
                        if self.mode == 'before-forward' and not self.report['faults']:
                            self.report['faults'] += 1
                            self.emit('fault')
                            return
                if not frontend and tag == b'K':
                    assert len(body) == 8
                    # Retain the public backend PID only, never the cancel key.
                    state['backend_pid'] = struct.unpack('!I', body[:4])[0]
                if not frontend and tag == b'E' and state['data']:
                    fields = body.split(b'\0')
                    if b'C55000' in fields and b'Marchive catalog writer excluded' in fields:
                        self.report['guard_rejections'] += 1
                        self.report['rejection_sequence'].append('guard')
                    elif b'C40001' in fields and b'Mcould not serialize access due to concurrent update' in fields:
                        self.report['serialization_rejections'] += 1
                        self.report['rejection_sequence'].append('serialization')
                    else:
                        self.report['rejection_sequence'].append('other')
                    if len(self.report['rejection_sequence']) > 16:
                        raise ValueError('rejection-budget')
                if not frontend and tag == b'C' and body == b'COMMIT\0' and state['commit']:
                    self.report['server_complete'] += 1
                    manager_committed.set()
                    state['commit'] = False
                    state['data'] = False
                    if self.mode == 'after-complete' and not self.report['faults']:
                        self.report['faults'] += 1
                        self.emit('fault')
                        return
                    self.report['client_complete'] += 1
                if not frontend and tag == b'Z' and body == b'I':
                    # A failed native attempt can roll back and reuse this
                    # connection. Its later transactions need fresh classification.
                    state['commit'] = False
                    state['data'] = False
                if not frontend and tag == b'Z' and state['owner_waiting']:
                    assert body == b'T'
                    state['owner_waiting'] = False
                    self.report['owner_complete'] += 1
                    self.emit('owner-complete', backend_pid=state['backend_pid'])
                try:
                    destination.write(tag + header + body)
                    await destination.drain()
                except ConnectionError:
                    if not (not frontend and self.mode == 'hold-manager-owner' and state['owner_forwarded']):
                        raise
                if frontend and self.mode == 'hold-manager-commit' and tag == b'Q' and body.rstrip(b'\0; \r\n').upper() == b'COMMIT':
                    # Once explicitly released, observe the real server COMMIT
                    # before handling the dead manager's EOF. This fixture owns
                    # both ends; production never receives this proxy behavior.
                    await manager_committed.wait()

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
            async with asyncio.timeout(120):
                for _ in range(3):
                    action = await control.readline()
                    if action == b'stop\n':
                        break
                    assert action in (b'release\n', b'close\n') and self.held and self.action is None
                    self.action = action.decode().strip()
                    self.release.set()
                    self.emit('released', action=self.action)
                else:
                    raise ValueError('control-budget')
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
    parser.add_argument('--mode', choices=['pass', 'before-forward', 'after-complete', 'hold-data', 'hold-commit', 'hold-manager-commit', 'hold-manager-owner'], required=True)
    try:
        sys.exit(asyncio.run(Proxy(parser.parse_args().mode).run()))
    except Exception:
        print('native commit proxy failed', file=sys.stderr)
        sys.exit(3)
