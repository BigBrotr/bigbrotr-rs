"""Bounded proxy controls, including real sockets with synthetic PG framing.

No PostgreSQL engine, product runtime, authentication or cleanup qualification.
"""
import asyncio
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import struct
import subprocess
import sys
import unittest

from native_commit_proxy import (MAX_WIRE, OPAQUE_CHUNK, PIN_RELEASE_QUERY, INDEX_PUBLICATION_QUERY,
                                IndexPublicationObserver, Proxy, control_target_query)


def index_bind(values=(bytes.fromhex('aa'*16),b'3',b'0',b'2')):
    return b'\0publish\0'+struct.pack('!HHH',1,1,4)+b''.join(struct.pack('!i',len(v))+v for v in values)+struct.pack('!HH',1,1)


def ordinary_proxy(**changes):
    options = dict(profile='ordinary-reader', defer_arm=True, catalog_manager='manager',
                   catalog_reader='reader', catalog_database='catalog')
    options.update(changes)
    return Proxy(options.pop('mode', 'hold-reader-query'), options.pop('target', 'publication'), **options)


class TrafficAdmission(unittest.IsolatedAsyncioTestCase):
    def test_index_target_requires_exact_current_statement_and_bound_plan3(self):
        source = Path(__file__).resolve().parents[2]/'apps/bigbrotr/src/storage/postgres/index_publication/adapter.rs'
        self.assertEqual(source.read_text().count(INDEX_PUBLICATION_QUERY.decode()),1)
        self.assertTrue(control_target_query('control-index-publication',INDEX_PUBLICATION_QUERY))
        self.assertFalse(control_target_query('control-index-publication',INDEX_PUBLICATION_QUERY+b'; SELECT 1'))
        for values,expected in ((None,True),((b'b'*16,b'3',b'0',b'2'),False),
                ((bytes.fromhex('aa'*16),b'4',b'0',b'2'),False),
                ((bytes.fromhex('aa'*16),b'3',b'1',b'2'),False),
                ((bytes.fromhex('aa'*16),b'3',b'0',b'1'),False)):
            observer=IndexPublicationObserver('aa'*16)
            observer.frontend(b'P',b'publish\0'+INDEX_PUBLICATION_QUERY+b'\0\0\0')
            observer.frontend(b'B',index_bind() if values is None else index_bind(values))
            observer.frontend(b'E',b'\0'*5)
            self.assertEqual(observer.backend(b'C',b'UPDATE 1\0'),expected)
        for malformed in (index_bind()[:-1],index_bind()+b'x',b'\0publish\0'+b'\0'*6):
            observer=IndexPublicationObserver('aa'*16)
            observer.frontend(b'P',b'publish\0'+INDEX_PUBLICATION_QUERY+b'\0\0\0')
            with self.assertRaises((ValueError,AssertionError)):
                observer.frontend(b'B',malformed)
        with self.assertRaises(ValueError):Proxy('before-forward',target='control-index-publication')
        proxy=Proxy('before-forward',target='control-index-publication',defer_arm=True,
                    index_consumer='aa'*16,hold_index_publication=True)
        self.assertEqual((proxy.frame_limit,proxy.connection_limit,proxy.lifetime_seconds),(20000,16,120))

    def test_release_hold_is_explicit_and_only_valid_for_armed_release_target(self):
        for options in (dict(),dict(target='control-pin-release'),dict(defer_arm=True),
                        dict(target='control-pin-release',defer_arm=True,hold_pin_release=1)):
            with self.subTest(options=options), self.assertRaises(ValueError):
                Proxy('before-forward',**dict(dict(hold_pin_release=True),**options))
        for mode in ('before-forward','after-complete'):
            value = Proxy(mode,target='control-pin-release',defer_arm=True,hold_pin_release=True)
            self.assertEqual((value.frame_limit,value.connection_limit,value.lifetime_seconds),(20000,16,120))
            self.assertFalse(value.armed)

    async def test_ordinary_streaming_keeps_aggregate_byte_and_target_frame_limits(self):
        proxy = ordinary_proxy()
        self.assertEqual((proxy.frame_limit, proxy.connection_limit, OPAQUE_CHUNK), (20000, 16, 65536))
        proxy.charge_unframed(MAX_WIRE-6)
        stream = asyncio.StreamReader()
        stream.feed_data((b'Z' + struct.pack('!I', 5) + b'I') * 2)
        await proxy.frame(stream)
        self.assertEqual(proxy.report['bytes'], MAX_WIRE)
        with self.assertRaisesRegex(ValueError, '^budget$'):
            await proxy.frame(stream)
        class Writer:
            closed = False
            def close(self):
                self.closed = True
        proxy = ordinary_proxy()
        proxy.report['active_connections'] = 16
        writer = Writer()
        with self.assertRaisesRegex(ValueError, '^connections$'):
            await proxy.session(None, writer)
        self.assertTrue(writer.closed)
        proxy = ordinary_proxy()
        proxy.report['bytes'] = MAX_WIRE-1
        proxy.charge_unframed(1)
        with self.assertRaisesRegex(ValueError, '^budget$'):
            proxy.charge_unframed(1)
        proxy = ordinary_proxy()
        proxy.report['frames'] = 20000
        stream = asyncio.StreamReader()
        stream.feed_data(b'Z' + struct.pack('!I', 5) + b'I')
        with self.assertRaisesRegex(ValueError, '^budget$'):
            await proxy.frame(stream)

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
                self.assertFalse({'upstream_host', 'upstream_port', 'catalog_reader', 'armed'} & event.keys())
            else:
                self.assertEqual({k: event[k] for k in ('profile', 'frame_limit', 'connection_limit')},
                                 dict(profile='retained-catch-up', frame_limit=131072, connection_limit=64))
            self.assertEqual(proxy.lifetime_seconds, 120)
        with self.assertRaisesRegex(ValueError, '^profile$'):
            Proxy('pass', profile='unbounded')


class CancellationCleanup(unittest.IsolatedAsyncioTestCase):
    async def test_stop_or_peer_close_cannot_hide_an_already_failed_pump(self):
        for stop in (False, True):
            with self.subTest(stop=stop):
                proxy = ordinary_proxy()
                failed = asyncio.Event()

                class Writer:
                    def close(self):
                        pass

                    async def wait_closed(self):
                        pass

                async def fail(error):
                    raise error

                async def failed_pumps(_reader, _writer, _armed, _number, _owned, pumps):
                    # The expected reset precedes an unexpected frame failure.
                    # Both results exist before the enclosing session stops.
                    pumps.extend(asyncio.create_task(fail(error)) for error in
                                 (ConnectionResetError('peer'), ValueError('frame')))
                    await asyncio.gather(*pumps, return_exceptions=True)
                    failed.set()
                    if stop:
                        await asyncio.Future()
                    raise ConnectionResetError('peer')

                proxy.connected_session = failed_pumps
                async with asyncio.timeout(3):
                    task = asyncio.create_task(proxy.session(None, Writer()))
                    await failed.wait()
                    if stop:
                        task.cancel()
                    result, = await asyncio.gather(task, return_exceptions=True)
                self.assertIsInstance(result, ValueError)
                self.assertEqual(str(result), 'frame')
                self.assertFalse(task.cancelled())
                self.assertEqual(proxy.report['active_connections'], 0)
                self.assertEqual(proxy.writers, [])

    async def test_stop_during_close_joins_transport_and_preserves_prior_failure(self):
        for malformed in (False, True):
            with self.subTest(malformed=malformed):
                proxy = ordinary_proxy()
                closing, release = asyncio.Event(), asyncio.Event()
                cancelled_closure = []

                class Writer:
                    def close(self):
                        pass

                    async def wait_closed(self):
                        closing.set()
                        try:
                            await release.wait()
                        except asyncio.CancelledError:
                            cancelled_closure.append(True)
                            raise

                reader = asyncio.StreamReader()
                reader.feed_data(struct.pack('!I', 7))
                if not malformed:
                    async def completed_peer(*_args):
                        pass
                    proxy.connected_session = completed_peer
                writer = Writer()
                task = asyncio.create_task(proxy.session(reader, writer))
                try:
                    async with asyncio.timeout(3):
                        await closing.wait()
                        for _ in range(2):
                            task.cancel()
                            await asyncio.sleep(0)
                            self.assertFalse(task.done())
                            self.assertEqual(proxy.report['active_connections'], 1)
                            self.assertEqual(proxy.writers, [writer])
                        release.set()
                        result, = await asyncio.gather(task, return_exceptions=True)
                    if malformed:
                        self.assertIsInstance(result, ValueError)
                        self.assertEqual(str(result), 'startup')
                        self.assertFalse(task.cancelled())
                    else:
                        self.assertIsInstance(result, asyncio.CancelledError)
                    self.assertEqual(cancelled_closure, [])
                    self.assertEqual(proxy.report['active_connections'], 0)
                    self.assertEqual(proxy.writers, [])
                finally:
                    release.set()
                    await asyncio.gather(task, return_exceptions=True)


class ClosedProfile(unittest.TestCase):
    def test_ordinary_profile_requires_explicit_fresh_reader_gate(self):
        for change in ({'mode':'pass'}, {'mode':'hold-capture-verify'}, {'target':'native-schema'},
                       {'defer_arm':False}, {'catalog_manager':None,'catalog_reader':None,'catalog_database':None}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                ordinary_proxy(**change)

    def test_cli_rejects_noncanonical_port_without_connecting_or_echoing_values(self):
        for port in ('0', '65536', '05432', '+5432', '1e3', '127.0.0.1:5432'):
            result = subprocess.run([sys.executable, '-B', str(Path(__file__).with_name('native_commit_proxy.py')),
                '--mode', 'hold-reader-query', '--upstream-port', port],
                capture_output=True, timeout=3, check=False)
            self.assertEqual(result.returncode, 3)
            self.assertEqual(result.stdout, b'')
            self.assertEqual(result.stderr, b'native commit proxy failed\n')

    def test_route_identifiers_and_deferred_modes_refuse_before_network(self):
        for value in (0, 65536, True, 1.0, '5432'):
            with self.subTest(port=value), self.assertRaisesRegex(ValueError, '^upstream-port$'):
                Proxy('pass', upstream_port=value)
        for changes in ({'catalog_reader': 'reader'},
                        {'catalog_database': 'db;other'}, {'catalog_manager': 'm' * 64},
                        {'catalog_reader': 'manager'}, {'catalog_database': 'déb'},
                        {'catalog_reader': 'reader\x00'}):
            profile = dict(catalog_manager='manager', catalog_reader='reader', catalog_database='catalog')
            if changes == {'catalog_reader': 'reader'}:
                profile = {}
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, '^catalog-profile$'):
                Proxy('pass', **(profile | changes))
        for mode in ('pass', 'hold-data', 'omit-auth-final'):
            with self.assertRaisesRegex(ValueError, '^defer-arm-mode$'):
                Proxy(mode, defer_arm=True)
        for mode in ('pass', 'hold-commit', 'omit-pin-claim'):
            with self.assertRaisesRegex(ValueError, '^pin-release-mode$'):
                Proxy(mode, 'control-pin-release')

    def test_arming_is_explicit_once_and_never_releases_a_gate(self):
        output = io.StringIO()
        with redirect_stdout(output):
            proxy = Proxy('hold-reader-query', defer_arm=True)
            self.assertFalse(proxy.armed)
            self.assertTrue(proxy.command(b'arm\n'))
            self.assertTrue(proxy.armed)
            self.assertFalse(proxy.release.is_set())
            for command in (b'arm\n', b'release\n', b'close\n', b'unknown\n'):
                with self.assertRaises(ValueError):
                    proxy.command(command)
            self.assertFalse(proxy.command(b'stop\n'))
            self.assertFalse(proxy.release.is_set())
            with self.assertRaisesRegex(ValueError, '^arm-state$'):
                Proxy('hold-reader-query').command(b'arm\n')
        self.assertEqual([json.loads(line)['event'] for line in output.getvalue().splitlines()], ['armed'])

    def test_release_target_is_the_exact_current_owner_statement(self):
        source = Path(__file__).resolve().parents[2] / 'apps/bigbrotr/src/storage/postgres/snapshot_leases.rs'
        self.assertEqual(source.read_text().count(PIN_RELEASE_QUERY.decode()), 1)
        self.assertTrue(control_target_query('control-pin-release', PIN_RELEASE_QUERY))
        for query in (PIN_RELEASE_QUERY + b'; SELECT 1', PIN_RELEASE_QUERY.replace(b'active', b'other'),
                      PIN_RELEASE_QUERY.replace(b'lease_revision+1', b'lease_revision+2'),
                      b'UPDATE control.snapshot_leases SET state=\'released\'',
                      b'UPDATE control.retention_claims SET state=\'released\''):
            self.assertFalse(control_target_query('control-pin-release', query))


def message(tag, body):
    return tag + struct.pack('!I', len(body) + 4) + body


async def read_message(reader):
    tag = await reader.readexactly(1)
    length, = struct.unpack('!I', await reader.readexactly(4))
    if not 4 <= length <= 65536:
        raise ValueError('synthetic-frame')
    return tag, await reader.readexactly(length - 4)


class SocketProxy:
    """Actual proxy subprocess and loopback streams; the upstream is framing only."""
    def __init__(self, case, mode, *, target='publication', deferred=True, profile='default', unexpected=0,
                 hold_pin_release=False,index_consumer=None):
        self.case, self.mode, self.target, self.deferred = case, mode, target, deferred
        self.profile, self.unexpected = profile, unexpected
        self.hold_pin_release = hold_pin_release
        self.index_consumer = index_consumer
        self.connections = []
        self.events = []
        self.process = None
        self.server = None

    async def __aenter__(self):
        self.accepted = asyncio.Queue()
        self.server = await asyncio.start_server(
            lambda reader, writer: self.accepted.put_nowait((reader, writer)), '127.0.0.1', 0)
        port = self.server.sockets[0].getsockname()[1]
        command = [sys.executable, '-B', str(Path(__file__).with_name('native_commit_proxy.py')),
                   '--mode', self.mode, '--target', self.target, '--upstream-port', str(port),
                   '--catalog-manager', 'manager', '--catalog-reader', 'reader',
                   '--catalog-database', 'catalog']
        if self.profile != 'default':
            command += ['--profile', self.profile]
        if self.deferred:
            command.append('--defer-arm')
        if self.hold_pin_release:
            command.append('--hold-pin-release')
        if self.index_consumer is not None:
            command += ['--index-consumer',self.index_consumer,'--hold-index-publication']
        self.process = await asyncio.create_subprocess_exec(*command, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            ready = await self.event('ready')
        except BaseException:
            if self.process.returncode is None:
                self.process.kill()
            await self.process.communicate()
            self.server.close()
            await self.server.wait_closed()
            raise
        self.case.assertEqual((ready['upstream_host'], ready['upstream_port'], ready['armed']),
                              ('127.0.0.1', port, not self.deferred))
        self.port = ready['port']
        return self

    async def event(self, expected):
        raw = await asyncio.wait_for(self.process.stdout.readline(), 3)
        self.case.assertTrue(raw, 'proxy ended before its expected marker')
        value = json.loads(raw)
        self.events.append(value)
        self.case.assertEqual(value['event'], expected)
        return value

    async def command(self, action, marker):
        self.process.stdin.write(action.encode() + b'\n')
        await self.process.stdin.drain()
        return await self.event(marker)

    async def connect(self, user='reader', database='catalog'):
        client, sender = await asyncio.open_connection('127.0.0.1', self.port)
        self.connections.append(sender)
        startup = b'\x00\x03\x00\x00user\x00' + user.encode() + b'\x00database\x00' + database.encode() + b'\x00\x00'
        sender.write(struct.pack('!I', len(startup) + 4) + startup)
        await sender.drain()
        upstream, responder = await asyncio.wait_for(self.accepted.get(), 3)
        self.connections.append(responder)
        size, = struct.unpack('!I', await asyncio.wait_for(upstream.readexactly(4), 3))
        self.case.assertEqual(await asyncio.wait_for(upstream.readexactly(size - 4), 3), startup)
        # Synthetic frames only. The cancel key must never appear in a marker.
        for tag, body in ((b'R', struct.pack('!I', 0)),
                          (b'K', struct.pack('!II', 7331, 0x1234abcd)), (b'Z', b'I')):
            responder.write(message(tag, body))
            await responder.drain()
            self.case.assertEqual(await asyncio.wait_for(read_message(client), 3), (tag, body))
        return client, sender, upstream, responder

    async def stop(self):
        if self.process.returncode is None:
            self.process.stdin.write(b'stop\n')
            await self.process.stdin.drain()
        await asyncio.wait_for(self.process.wait(), 5)
        rest = await self.process.stdout.read()
        self.events.extend(json.loads(line) for line in rest.splitlines())
        self.case.assertEqual(await self.process.stderr.read(), b'')
        self.case.assertEqual(self.process.returncode, 3 if self.unexpected else 0)
        self.case.assertEqual(self.events[-1]['event'], 'terminal')
        self.case.assertEqual(self.events[-1]['unexpected'], self.unexpected)
        encoded = json.dumps(self.events)
        self.case.assertNotIn(str(0x1234abcd), encoded)
        self.case.assertNotIn(PIN_RELEASE_QUERY.decode(), encoded)
        self.case.assertNotIn(INDEX_PUBLICATION_QUERY.decode(), encoded)

    async def __aexit__(self, *_):
        try:
            if self.process is not None:
                await self.stop()
        finally:
            if self.process is not None and self.process.returncode is None:
                self.process.kill()
                await self.process.wait()
            for writer in self.connections:
                writer.close()
            await asyncio.gather(*(writer.wait_closed() for writer in self.connections), return_exceptions=True)
            if self.server is not None:
                self.server.close()
                await self.server.wait_closed()


class ProtocolSockets(unittest.IsolatedAsyncioTestCase):
    async def test_index_publication_hold_is_bound_and_both_commit_results_stay_distinct(self):
        for mode in ('before-forward','after-complete'):
            async with SocketProxy(self,mode,target='control-index-publication',index_consumer='aa'*16) as proxy:
                connection=await proxy.connect('bigbrotr_control','bigbrotr_control')
                begin=[(b'C',b'BEGIN\0'),(b'Z',b'T')]
                commit=[(b'C',b'COMMIT\0'),(b'Z',b'I')]
                await self.exchange(connection,b'Q',b'BEGIN\0',begin)
                await self.exchange(connection,b'P',b'publish\0'+INDEX_PUBLICATION_QUERY+b'\0\0\0',[(b'1',b'')])
                await self.exchange(connection,b'Q',b'COMMIT\0',commit)
                await proxy.command('arm','armed')
                # A cached prepared statement and wrong consumer cannot select
                # an unrelated renewal/preparation transaction's COMMIT.
                await self.exchange(connection,b'Q',b'BEGIN\0',begin)
                await self.exchange(connection,b'B',index_bind((b'b'*16,b'3',b'0',b'2')),[(b'2',b'')])
                await self.exchange(connection,b'E',b'\0'*5,[(b'C',b'UPDATE 1\0')])
                await self.exchange(connection,b'S',b'',[(b'Z',b'T')])
                await self.exchange(connection,b'Q',b'COMMIT\0',commit)
                await self.exchange(connection,b'Q',b'BEGIN\0',begin)
                await self.exchange(connection,b'B',index_bind(),[(b'2',b'')])
                await self.exchange(connection,b'E',b'\0'*5)
                connection[3].write(message(b'C',b'UPDATE 1\0'));await connection[3].drain()
                selected=await proxy.event('index-publication-update-complete')
                held=await proxy.event('held')
                self.assertEqual((selected['data_queries'],selected['data_commits'],held['stage']),(1,0,'index-publication-update'))
                await proxy.command('release','released')
                self.assertEqual(await read_message(connection[0]),(b'C',b'UPDATE 1\0'))
                await self.exchange(connection,b'S',b'',[(b'Z',b'T')])
                connection[1].write(message(b'Q',b'COMMIT\0'));await connection[1].drain()
                if mode=='after-complete':
                    self.assertEqual(await read_message(connection[2]),(b'Q',b'COMMIT\0'))
                    connection[3].write(message(b'C',b'COMMIT\0')+message(b'Z',b'I'));await connection[3].drain()
                fault=await proxy.event('fault')
                self.assertEqual([fault[k] for k in ('data_queries','data_commits','server_complete','client_complete','faults')],
                                 [1,1,int(mode=='after-complete'),0,1])
                self.assertEqual(await asyncio.wait_for(connection[0].read(),3),b'')
                self.assertEqual(await asyncio.wait_for(connection[2].read(),3),b'')

    async def test_release_update_hold_precedes_commit_and_stop_does_not_invent_fault(self):
        for action in ('release', 'stop'):
            for mode in ('before-forward', 'after-complete'):
                with self.subTest(action=action, mode=mode):
                    async with SocketProxy(self, mode, target='control-pin-release', hold_pin_release=True) as proxy:
                        connection = await proxy.connect('bigbrotr_control', 'control')
                        await self.exchange(connection, b'Q', b'BEGIN\0', [(b'C', b'BEGIN\0'), (b'Z', b'T')])
                        await proxy.command('arm', 'armed')
                        await self.exchange(connection, b'P', b'release\0' + PIN_RELEASE_QUERY + b'\0\0\0', [(b'1', b'')])
                        await self.exchange(connection, b'B', b'\0release\0\0\0\0\0\0\0', [(b'2', b'')])
                        await self.exchange(connection, b'E', b'\0' * 5)
                        connection[3].write(message(b'C', b'UPDATE 1\0'))
                        await connection[3].drain()
                        selected = await proxy.event('pin-release-update-complete')
                        held = await proxy.event('held')
                        self.assertEqual((held['stage'], held['backend_pid'], held['data_commits']),
                                         ('pin-release-update', selected['backend_pid'], 0))
                        with self.assertRaises(asyncio.TimeoutError):
                            await asyncio.wait_for(read_message(connection[0]), .05)
                        if action == 'stop':
                            await proxy.stop()
                            self.assertEqual(proxy.events[-1]['faults'], 0)
                            self.assertEqual(proxy.events[-1]['data_commits'], 0)
                            continue
                        await proxy.command('release', 'released')
                        self.assertEqual(await asyncio.wait_for(read_message(connection[0]), 3), (b'C', b'UPDATE 1\0'))
                        await self.exchange(connection, b'S', b'', [(b'Z', b'T')])
                        connection[1].write(message(b'Q', b'COMMIT\0'))
                        await connection[1].drain()
                        if mode == 'after-complete':
                            self.assertEqual(await asyncio.wait_for(read_message(connection[2]), 3), (b'Q', b'COMMIT\0'))
                            connection[3].write(message(b'C', b'COMMIT\0') + message(b'Z', b'I'))
                            await connection[3].drain()
                        fault = await proxy.event('fault')
                        self.assertEqual([fault[k] for k in ('faults','data_queries','data_commits','server_complete','client_complete')],
                                         [1,1,1,int(mode == 'after-complete'),0])

    async def exchange(self, connection, tag, body, replies=()):
        client, sender, upstream, responder = connection
        sender.write(message(tag, body))
        await sender.drain()
        self.assertEqual(await asyncio.wait_for(read_message(upstream), 3), (tag, body))
        for reply_tag, reply_body in replies:
            responder.write(message(reply_tag, reply_body))
            await responder.drain()
            self.assertEqual(await asyncio.wait_for(read_message(client), 3), (reply_tag, reply_body))

    async def test_ordinary_serial_connections_release_slots_before_reuse(self):
        async with SocketProxy(self, 'hold-reader-query', profile='ordinary-reader') as proxy:
            for _ in range(20):
                connection = await proxy.connect('manager')
                await self.exchange(connection, b'Q', b'SELECT serial_baseline\0')
                connection[1].close()
                await connection[1].wait_closed()
                self.assertEqual(await asyncio.wait_for(connection[2].read(), 3), b'')
            armed = await proxy.command('arm', 'armed')
            self.assertEqual(armed['connections'], 20)
            self.assertEqual(armed['connection_limit_scope'], 'active')
            self.assertLessEqual(armed['active_connections'], 1)
            self.assertLessEqual(armed['peak_connections'], 16)
            self.assertEqual(armed['frames'], 0)
            await proxy.stop()
            self.assertEqual(proxy.events[-1]['active_connections'], 0)

    async def test_ordinary_seventeenth_live_connection_is_refused_before_startup(self):
        async with SocketProxy(self, 'hold-reader-query', profile='ordinary-reader', unexpected=1) as proxy:
            for _ in range(16):
                await proxy.connect('manager')
            armed = await proxy.command('arm', 'armed')
            self.assertEqual((armed['active_connections'], armed['peak_connections']), (16, 16))
            client, sender = await asyncio.open_connection('127.0.0.1', proxy.port)
            proxy.connections.append(sender)
            self.assertEqual(await asyncio.wait_for(client.read(), 3), b'')
            self.assertTrue(proxy.accepted.empty())
            await proxy.stop()
            terminal = proxy.events[-1]
            self.assertEqual((terminal['connections'], terminal['active_connections'], terminal['peak_connections']),
                             (17, 0, 16))

    async def test_ordinary_baseline_is_bounded_byte_stream_and_only_fresh_reader_is_framed(self):
        async with SocketProxy(self, 'hold-reader-query', profile='ordinary-reader') as proxy:
            old_reader = await proxy.connect()
            manager = await proxy.connect('manager')
            # More than the original aggregate20k frames, but only140KiB. The
            # manager's complete metadata response is forwarded byte-for-byte.
            data = message(b'D', b'\0\0') * 20001
            manager[3].write(data)
            await manager[3].drain()
            self.assertEqual(await asyncio.wait_for(manager[0].readexactly(len(data)), 3), data)
            armed = await proxy.command('arm', 'armed')
            self.assertEqual(armed['frames'], 0)
            self.assertGreaterEqual(armed['unframed_bytes'], len(data))
            await self.exchange(old_reader, b'Q', b'SELECT existing_reader\0')
            other_database = await proxy.connect(database='other')
            await self.exchange(other_database, b'Q', b'SELECT other_database\0')
            new_manager = await proxy.connect('manager')
            await self.exchange(new_manager, b'Q', b'SELECT new_manager\0')
            selected = await proxy.connect()
            admitted = await proxy.event('reader-selected')
            self.assertTrue(admitted['admitted_after_arm'])
            self.assertEqual((admitted['framed_connections'], admitted['backend_pid']), (1, 7331))
            selected[1].write(message(b'Q', b'SELECT withheld\0'))
            await selected[1].drain()
            held = await proxy.event('held')
            self.assertEqual((held['stage'], held['backend_pid'], held['target_connection']),
                ('reader-query', 7331, admitted['connection_number']))
            pending = asyncio.create_task(read_message(selected[2]))
            try:
                self.assertFalse((await asyncio.wait([pending], timeout=.05))[0])
                await proxy.command('release', 'released')
                self.assertEqual(await asyncio.wait_for(pending, 3), (b'Q', b'SELECT withheld\0'))
            finally:
                if not pending.done():
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
            later_reader = await proxy.connect()
            await self.exchange(later_reader, b'Q', b'SELECT later_reader\0')
            await proxy.stop()
            self.assertEqual(proxy.events[-1]['framed_connections'], 1)
            self.assertLess(proxy.events[-1]['frames'], 20000)
            self.assertLess(proxy.events[-1]['bytes'], MAX_WIRE)

    async def test_ordinary_malformed_startup_is_refused_before_upstream(self):
        malformed = (
            b'\0\3\0\0user\0reader\0user\0manager\0database\0catalog\0\0',
            b'\0\3\0\0user\0reader\0\0',
            b'\0\3\0\0user\0reader\0database\0bad database\0\0',
            b'\0\2\0\0user\0reader\0database\0catalog\0\0',
        )
        packets = [struct.pack('!I',len(startup)+4)+startup for startup in malformed]
        packets += [struct.pack('!I',7), struct.pack('!I',65537)]
        for packet in packets:
            async with SocketProxy(self, 'hold-reader-query', profile='ordinary-reader', unexpected=1) as proxy:
                client, sender = await asyncio.open_connection('127.0.0.1', proxy.port)
                proxy.connections.append(sender)
                sender.write(packet)
                await sender.drain()
                self.assertEqual(await asyncio.wait_for(client.read(), 3), b'')
                self.assertTrue(proxy.accepted.empty())

    async def test_deferred_reader_forwards_baseline_and_foreign_database_then_holds_once(self):
        async with SocketProxy(self, 'hold-reader-query') as proxy:
            selected = await proxy.connect()
            await self.exchange(selected, b'Q', b'SELECT baseline\0', [(b'C', b'SELECT 1\0'), (b'Z', b'I')])
            await proxy.command('arm', 'armed')
            foreign = await proxy.connect(database='other')
            await self.exchange(foreign, b'Q', b'SELECT foreign\0')
            manager = await proxy.connect(user='manager')
            await self.exchange(manager, b'Q', b'SELECT manager\0')
            selected[1].write(message(b'Q', b'SELECT withheld\0'))
            await selected[1].drain()
            held = await proxy.event('held')
            self.assertEqual((held['stage'], held['backend_pid']), ('reader-query', 7331))
            pending = asyncio.create_task(read_message(selected[2]))
            try:
                self.assertFalse((await asyncio.wait([pending], timeout=.05))[0])
                await proxy.command('release', 'released')
                self.assertEqual(await asyncio.wait_for(pending, 3), (b'Q', b'SELECT withheld\0'))
            finally:
                if not pending.done():
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
            await self.exchange(selected, b'Q', b'SELECT after\0')

    async def test_stop_joins_a_held_gate_without_forwarding_its_query(self):
        for profile,deferred in (('default',False), ('default',True), ('ordinary-reader',True)):
            async with SocketProxy(self, 'hold-reader-query', deferred=deferred, profile=profile) as proxy:
                if profile == 'ordinary-reader':
                    await proxy.command('arm', 'armed')
                connection = await proxy.connect()
                if profile == 'ordinary-reader':
                    await proxy.event('reader-selected')
                elif deferred:
                    await proxy.command('arm', 'armed')
                connection[1].write(message(b'Q', b'SELECT never_forward\0'))
                await connection[1].drain()
                await proxy.event('held')
                await proxy.stop()
                self.assertEqual(await asyncio.wait_for(connection[0].read(), 3), b'')
                self.assertEqual(await asyncio.wait_for(connection[2].read(), 3), b'')

    async def test_capture_gates_use_only_selected_reader_traffic_after_arming(self):
        for mode in ('hold-capture-rollback', 'hold-capture-verify'):
            async with SocketProxy(self, mode) as proxy:
                manager = await proxy.connect('manager')
                reader = await proxy.connect()
                await self.exchange(reader, b'Q', b'SELECT baseline\0')
                verify = b'v\0SELECT NOT enabled AND (SELECT max(snapshot_id) FROM public.ducklake_snapshot)=$1\0\0\0'
                if mode == 'hold-capture-verify':
                    await self.exchange(manager, b'P', verify)
                    request = (b'B', b'\0v\0\0\0\0\0\0\0')
                else:
                    request = (b'Q', b'ROLLBACK\0')
                await proxy.command('arm', 'armed')
                # Arming must not relabel the baseline reader as this capture.
                await self.exchange(manager, *request)
                await self.exchange(reader, b'Q', b'SELECT current\0')
                manager[1].write(message(*request))
                await manager[1].drain()
                held = await proxy.event('held')
                self.assertEqual(held['stage'], mode.removeprefix('hold-'))
                self.assertEqual(held['reader_queries'], 1)
                await proxy.command('close', 'released')
                self.assertEqual(await asyncio.wait_for(manager[0].read(), 3), b'')
                self.assertEqual(await asyncio.wait_for(manager[2].read(), 3), b'')

    async def release_update(self, connection, *, parse=False, affected=1):
        if parse:
            await self.exchange(connection, b'P', b'release\0' + PIN_RELEASE_QUERY + b'\0\0\0', [(b'1', b'')])
        await self.exchange(connection, b'B', b'\0release\0\0\0\0\0\0\0', [(b'2', b'')])
        await self.exchange(connection, b'E', b'\0' * 5, [(b'C', f'UPDATE {affected}\0'.encode())])
        await self.exchange(connection, b'S', b'', [(b'Z', b'T')])

    async def test_release_fault_is_execute_bound_and_distinguishes_both_commit_sides(self):
        for mode in ('before-forward', 'after-complete'):
            with self.subTest(mode=mode):
                async with SocketProxy(self, mode, target='control-pin-release') as proxy:
                    connection = await proxy.connect('bigbrotr_control', 'control')
                    begin = [(b'C', b'BEGIN\0'), (b'Z', b'T')]
                    commit = [(b'C', b'COMMIT\0'), (b'Z', b'I')]
                    await self.exchange(connection, b'Q', b'BEGIN\0', begin)
                    await self.release_update(connection, parse=True)
                    await self.exchange(connection, b'Q', b'COMMIT\0', commit)
                    # Arming between Execute and its reply cannot retroactively
                    # select an already issued baseline UPDATE.
                    await self.exchange(connection, b'Q', b'BEGIN\0', begin)
                    await self.exchange(connection, b'B', b'\0release\0\0\0\0\0\0\0', [(b'2', b'')])
                    await self.exchange(connection, b'E', b'\0' * 5)
                    await proxy.command('arm', 'armed')
                    connection[3].write(message(b'C', b'UPDATE 1\0'))
                    await connection[3].drain()
                    self.assertEqual(await asyncio.wait_for(read_message(connection[0]), 3), (b'C', b'UPDATE 1\0'))
                    await self.exchange(connection, b'S', b'', [(b'Z', b'T')])
                    await self.exchange(connection, b'Q', b'COMMIT\0', commit)
                    # Cached Parse exists but unrelated COMMIT and UPDATE0 cannot select it.
                    await self.exchange(connection, b'Q', b'BEGIN\0', begin)
                    await self.exchange(connection, b'Q', b'COMMIT\0', commit)
                    await self.exchange(connection, b'Q', b'BEGIN\0', begin)
                    await self.release_update(connection, affected=0)
                    await self.exchange(connection, b'Q', b'COMMIT\0', commit)
                    await self.exchange(connection, b'Q', b'BEGIN\0', begin)
                    await self.release_update(connection)
                    selected = await proxy.event('pin-release-update-complete')
                    self.assertEqual((selected['data_queries'], selected['data_commits']), (1, 0))
                    connection[1].write(message(b'Q', b'COMMIT\0'))
                    await connection[1].drain()
                    if mode == 'after-complete':
                        self.assertEqual(await asyncio.wait_for(read_message(connection[2]), 3), (b'Q', b'COMMIT\0'))
                        connection[3].write(message(b'C', b'COMMIT\0') + message(b'Z', b'I'))
                        await connection[3].drain()
                    fault = await proxy.event('fault')
                    self.assertEqual((fault['faults'], fault['data_queries'], fault['data_commits'],
                                      fault['server_complete'], fault['client_complete']),
                                     (1, 1, 1, int(mode == 'after-complete'), 0))
                    self.assertEqual(await asyncio.wait_for(connection[0].read(), 3), b'')
                    self.assertEqual(await asyncio.wait_for(connection[2].read(), 3), b'')



class ClosedReclamationTarget(unittest.TestCase):
    def test_only_the_exact_final_typed_reclamation_head_selects_its_commit(self):
        query=(b"SELECT left(checkpoint_revision::text,21),substring(checkpoint_digest FROM 1 FOR 33)\n"
            b" FROM control.producer_checkpoints WHERE producer_slot_id=$1::bytea AND producer_epoch=1 AND progress_format=1\n"
            b" AND complete_through_request=$2::text::numeric")
        self.assertTrue(control_target_query('control-closed-reclamation',query))
        for changed in (query+b'; SELECT 1',query.replace(b'progress_format=1',b'progress_format=2'),
            query.replace(b'producer_epoch=1',b'producer_epoch=2'),query.replace(b'complete_through_request=$2::text::numeric',b'TRUE')):
            self.assertFalse(control_target_query('control-closed-reclamation',changed))
        for mode in ('pass','before-forward','after-complete'):
            selected=Proxy(mode,'control-closed-reclamation',profile='retained-catch-up')
            self.assertEqual(selected.target,'control-closed-reclamation')
        with self.assertRaises(ValueError):Proxy('hold-data','control-closed-reclamation')

if __name__ == '__main__':
    unittest.main()
