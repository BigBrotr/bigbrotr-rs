"""Synthetic protocol discriminator for the actual proxy, not engine evidence."""
import asyncio
from contextlib import redirect_stdout
import importlib.util
import io
import json
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
    def progress_prepare(self, completed=0, status=b'T'):
        query = b'UPDATE control.acquisition_progress SET scope_id=$5::bytea, progress_revision=$6'
        return ([(b'P', b'progress\0' + query + b'\0\0\0'), (b'S', b'')],
                [(b'1', b''), (b'Z', status)], completed)

    def progress_execute(self, name=b'progress', command=b'UPDATE 1\0', completed=0, error=False):
        return ([(b'B', b'\0' + name + b'\0' + b'\0' * 6), (b'E', b'\0' * 5), (b'S', b'')],
                [(b'2', b''), (b'E' if error else b'C', command), (b'Z', b'E' if error else b'T')], completed)

    def progress_steps(self):
        """Literal closed fixture: setup, NEW INSERT, OLD UPDATE1, repeat UPDATE2."""
        steps = []

        def simple(query, command, status, completed):
            steps.append(([(b'Q', query + b'\0')],
                          [(b'C', command + b'\0'), (b'Z', status)], completed))

        def prepare(name, query, completed):
            steps.append(([(b'P', name + b'\0' + query + b'\0\0\0'), (b'S', b'')],
                          [(b'1', b''), (b'Z', b'T')], completed))

        def execute(name, command, completed, error=False):
            steps.append(self.progress_execute(name, command, completed, error))

        simple(b'BEGIN', b'BEGIN', b'T', 0)
        prepare(b'dictionary', b'INSERT INTO control.producer_checkpoints(dataset_id) VALUES($1)', 0)
        execute(b'dictionary', b'INSERT 0 1\0', 0)
        simple(b'COMMIT', b'COMMIT', b'I', 0)
        simple(b'BEGIN', b'BEGIN', b'T', 0)
        prepare(b'new', b'INSERT INTO control.acquisition_progress(dataset_id) VALUES($1)', 0)
        execute(b'new', b'INSERT 0 1\0', 0)
        steps.append(self.progress_prepare())
        # Preparing the future UPDATE must not select this INSERT transaction.
        simple(b'COMMIT', b'COMMIT', b'I', 0)
        simple(b'BEGIN', b'BEGIN', b'T', 0)
        execute(b'progress', b'UPDATE 0\0', 0)
        simple(b'COMMIT', b'COMMIT', b'I', 0)
        simple(b'BEGIN', b'BEGIN', b'T', 0)
        execute(b'progress', b'C23514\0Msynthetic-error-payload\0\0', 0, error=True)
        simple(b'ROLLBACK', b'ROLLBACK', b'I', 0)
        simple(b'BEGIN', b'BEGIN', b'T', 0)
        prepare(b'other', b'UPDATE control.jobs SET state=$1', 0)
        execute(b'other', b'UPDATE 1\0', 0)
        prepare(b'select', b'SELECT scope_id FROM control.acquisition_progress', 0)
        execute(b'select', b'SELECT 1\0', 0)
        execute(b'progress', b'UPDATE 1\0', 1)
        prepare(b'audit', b'INSERT INTO control.management_audit(audit_id) VALUES($1)', 1)
        execute(b'audit', b'INSERT 0 1\0', 1)
        simple(b'COMMIT', b'COMMIT', b'I', 1)
        simple(b'BEGIN', b'BEGIN', b'T', 1)
        execute(b'other', b'UPDATE 1\0', 1)
        execute(b'progress', b'UPDATE 1\0', 2)
        execute(b'audit', b'INSERT 0 1\0', 2)
        simple(b'COMMIT', b'COMMIT', b'I', 2)
        return steps

    async def progress_wire(self, mode, steps, observer=None, backend_pid=41001,
                            user=b'bigbrotr_control', expected_error=None):
        """Run this target's transcript through Proxy.session and owned peers."""
        observer = observer or proxy.Proxy(mode, 'control-producer-progress')
        upstream_tasks, proxy_tasks = [], []
        stream = io.StringIO()

        async def receive(reader):
            tag = await reader.readexactly(1)
            length = struct.unpack('!I', await reader.readexactly(4))[0]
            self.assertTrue(4 <= length <= proxy.MAX_FRAME)
            return tag, await reader.readexactly(length - 4)

        async def backend(reader, writer):
            try:
                length = struct.unpack('!I', await reader.readexactly(4))[0]
                await reader.readexactly(length - 4)
                writer.write(frame(b'K', struct.pack('!II', backend_pid, 0x5A17C0DE)) + frame(b'Z', b'I'))
                await writer.drain()
                for index, (frontend, replies, _) in enumerate(steps):
                    selected = index == len(steps) - 1 and mode == 'before-forward' and expected_error is None
                    if selected:
                        self.assertEqual(await reader.read(), b'')
                        return
                    for message in frontend:
                        self.assertEqual(await receive(reader), message)
                    # COMMIT and ReadyForQuery deliberately share one write.
                    writer.write(b''.join(frame(*reply) for reply in replies))
                    await writer.drain()
                self.assertEqual(await reader.read(), b'')
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
                self.assertEqual(host, '127.0.0.1')
                port = upstream.sockets[0].getsockname()[1]
            return await connect(host, port, *args, **kwargs)

        writer = None
        try:
            async with asyncio.timeout(5):
                with redirect_stdout(stream), patch.object(proxy.asyncio, 'open_connection', redirect):
                    reader, writer = await connect('127.0.0.1', listener.sockets[0].getsockname()[1])
                    startup = b'\0\3\0\0user\0' + user + b'\0database\0fixture_control\0\0'
                    writer.write(struct.pack('!I', len(startup) + 4) + startup)
                    await writer.drain()
                    self.assertEqual(await receive(reader), (b'K', struct.pack('!II', backend_pid, 0x5A17C0DE)))
                    self.assertEqual(await receive(reader), (b'Z', b'I'))
                    for index, (frontend, replies, completed) in enumerate(steps):
                        writer.write(b''.join(frame(*message) for message in frontend))
                        await writer.drain()
                        final = index == len(steps) - 1
                        if final and (expected_error is not None or mode != 'pass'):
                            # Neither the selected COMMIT completion nor its
                            # coalesced ReadyForQuery may escape truncation.
                            if expected_error is not None:
                                self.assertEqual(await receive(reader), (b'2', b''))
                            self.assertEqual(await reader.read(), b'')
                            break
                        for reply in replies:
                            self.assertEqual(await receive(reader), reply)
                        self.assertEqual(observer.report['progress_updates_complete'], completed)
                        self.assertEqual(observer.report['data_queries'], int(completed == 2))
                        if completed < 2:
                            self.assertEqual(observer.report['data_commits'], 0)
                    writer.close()
                    await writer.wait_closed()
                    outcomes = await asyncio.gather(*proxy_tasks, *upstream_tasks, return_exceptions=True)
                    if expected_error is None:
                        self.assertEqual(outcomes, [None, None])
                    else:
                        self.assertIsInstance(outcomes[0], ValueError)
                        self.assertEqual(str(outcomes[0]), expected_error)
                        self.assertIsNone(outcomes[1])
                    observer.emit('terminal')
        finally:
            if writer is not None:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), 1)
            listener.close()
            upstream.close()
            await listener.wait_closed()
            await upstream.wait_closed()
            for task in proxy_tasks + upstream_tasks:
                task.cancel()
            await asyncio.wait_for(asyncio.gather(*proxy_tasks, *upstream_tasks, return_exceptions=True), 1)
        output = stream.getvalue()
        for private in ['synthetic-error-payload', 'acquisition_progress', 'scope_id',
                        'bigbrotr_control', str(0x5A17C0DE), '5a17c0de']:
            self.assertNotIn(private, output)
        events = [json.loads(line) for line in output.splitlines()]
        self.assertTrue(all(event['target_occurrence'] == 2 for event in events))
        return observer, events

    async def test_progress_cached_updates_select_only_second_transaction(self):
        for mode, server_complete, client_complete, faults in [
                ('pass', 1, 1, 0), ('before-forward', 0, 0, 1), ('after-complete', 1, 0, 1)]:
            with self.subTest(mode=mode):
                observer, events = await self.progress_wire(mode, self.progress_steps())
                self.assertEqual({key: observer.report[key] for key in [
                    'data_queries', 'data_commits', 'progress_updates_complete',
                    'server_complete', 'client_complete', 'faults', 'session_mutations_complete']},
                    dict(data_queries=1, data_commits=1, progress_updates_complete=2,
                         server_complete=server_complete, client_complete=client_complete,
                         faults=faults, session_mutations_complete=0))
                self.assertEqual([event['progress_updates_complete'] for event in events
                                  if event['event'] == 'progress-update-complete'], [1, 2])
                self.assertTrue(all(event['backend_pid'] == 41001 for event in events))

    async def test_progress_matches_cannot_move_to_another_backend_session(self):
        all_steps = self.progress_steps()
        first_commit = next(index for index, (frontend, _, completed) in enumerate(all_steps)
                            if frontend == [(b'Q', b'COMMIT\0')] and completed == 1)
        first = all_steps[:first_commit + 1]
        second = [([(b'Q', b'BEGIN\0')], [(b'C', b'BEGIN\0'), (b'Z', b'T')], 1),
                  self.progress_prepare(completed=1), self.progress_execute(completed=2)]
        for backend_pid in [41001, 41002]:
            with self.subTest(backend_pid=backend_pid):
                observer, _ = await self.progress_wire('pass', first)
                observer, _ = await self.progress_wire('pass', second, observer=observer,
                    backend_pid=backend_pid, expected_error='producer-progress-backend')
                self.assertEqual(observer.report['progress_updates_complete'], 1)
                self.assertEqual(observer.report['data_commits'], 0)

    async def test_progress_requires_control_user_and_explicit_transaction(self):
        for user, status, reason in [(b'fixture', b'T', 'producer-progress-backend'),
                                     (b'bigbrotr_control', b'I', 'producer-progress-transaction')]:
            with self.subTest(reason=reason):
                steps = [self.progress_prepare(status=status), self.progress_execute(completed=1)]
                if status == b'T':
                    steps.insert(0, ([(b'Q', b'BEGIN\0')], [(b'C', b'BEGIN\0'), (b'Z', b'T')], 0))
                observer, _ = await self.progress_wire('pass', steps, user=user, expected_error=reason)
                self.assertEqual(observer.report['progress_updates_complete'], 0)
                self.assertEqual(observer.report['data_commits'], 0)

    async def test_progress_success_budget_is_two_on_same_backend(self):
        all_steps = self.progress_steps()
        steps = all_steps + [
            ([(b'Q', b'BEGIN\0')], [(b'C', b'BEGIN\0'), (b'Z', b'T')], 2),
            self.progress_execute(completed=2)]
        observer, _ = await self.progress_wire('pass', steps, expected_error='producer-progress-target-budget')
        self.assertEqual(observer.report['progress_updates_complete'], 2)
        self.assertEqual(observer.report['data_commits'], 1)

    async def test_progress_error_after_second_update_cannot_select_later_commit(self):
        all_steps = self.progress_steps()
        steps = all_steps[:-1] + [
            self.progress_execute(command=b'C23514\0Msynthetic-error-payload\0\0', completed=2, error=True),
            ([(b'Q', b'ROLLBACK\0')], [(b'C', b'ROLLBACK\0'), (b'Z', b'I')], 2),
            ([(b'Q', b'BEGIN\0')], [(b'C', b'BEGIN\0'), (b'Z', b'T')], 2),
            self.progress_execute(name=b'other', completed=2),
            ([(b'Q', b'COMMIT\0')], [(b'C', b'COMMIT\0'), (b'Z', b'I')], 2)]
        observer, _ = await self.progress_wire('pass', steps)
        self.assertEqual(observer.report['progress_updates_complete'], 2)
        self.assertEqual(observer.report['data_queries'], 1)
        self.assertEqual(observer.report['data_commits'], 0)
        self.assertEqual(observer.report['faults'], 0)

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
