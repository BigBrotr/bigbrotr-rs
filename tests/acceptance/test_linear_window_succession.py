"""Literal window-succession verifier controls; no product or engine execution."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

from test_first_launch import ROOT
from test_first_launch_restart import fixture as signed_fixture, current42_head
from test_linear_quantum import fixture as quantum_fixture
from cases import first_launch_window_succession as check
from cases import first_launch_quantum_oracle as quantum
from cases import first_launch_restart as source
from installed_sync_client import exact, frozen_horizon, quantum_status
from installed_sync_oracle import expected, decode
from bootstrap_oracle import cbor
import linear_window_transport as peer


# Independent of both the transport recipe and retained SQL observations.
SELECTOR = dict(version=1, filters=[dict(kinds=[1], until=259199)], history_since=0, ephemeral='retain')
ENTRIES = (('request', 0, 86400, 86400), ('request', 0, 2, 86400),
    ('request', 0, 1, 86400), ('request', 86400, 172800, 172800),
    ('request', 172800, 259200, 259200), ('request', 172800, 172802, 259200),
    ('request', 172800, 172802, 259200), ('request', 172800, 172801, 259200))
PAGES = ((0,), (0,), (), (), (1,), (2,), (2,), ())
IDS = ('8a0096a3a84f1460c11a432355666cb9fd377cca00a5a1e08ef0071c9c0b9849',
    'fac8ef0b0353caa848edc035806ccdb65f1ff1d17e78535126a86796bcc09e2e',
    'a6d21c3e069f3f776259ef542ca4022054924a3f6d89ac5a9254101e2044f003')
SUMMARY = dict(queries_spent=8, attempted_prefix=259200, gap_count=2, pending_work='settled', pending_interval=None)
ACCEPTED = 300_000_999_999
COMPLETED = 300_002_000_000


def corpus(root):
    for name, target in (('tests/fixtures/ingest/import-event.jsonl', 'event.jsonl'),
            ('tests/fixtures/acquisition/linear-window-events.json', 'linear-window-events.json')):
        (root / target).write_bytes((ROOT / name).read_bytes())
    return peer.corpus(root)


def fixture(root):
    events = corpus(root)
    encoded = signed_fixture()[3]
    state, report = copy.deepcopy(encoded.initial_state), encoded.initial_report
    skeleton, _ = quantum_fixture()
    dataset = state['datasets'][0]['dataset_id']
    owner = dict(skeleton['owner_epochs'][0], dataset_id=dataset,
        acquired_at_us=300_000_000_000, lease_expires_at_us=300_060_000_000)
    for historical in state['owner_epochs']:
        historical.setdefault('dataset_id', dataset)
        historical.setdefault('epoch', 1)
    state['owner_epochs'].append(owner)
    job = dict(skeleton['jobs'][0], dataset_id=dataset, request_key='\\x' + 'b1' * 16, created_at_us=ACCEPTED)
    detail = dict(skeleton['acquisition_jobs'][0], dataset_id=dataset, job_id=job['job_id'])
    state.update(jobs=[job], acquisition_jobs=[detail], acquisition_quanta=[], acquisition_gaps=[],
        producer_requests=[], producer_checkpoints=[], producer_epochs=[dict(dataset_id=dataset,
            producer_slot_id='\\x' + report['producer_slot_id'], producer_epoch=1)])
    wires = []
    for number, (entry, page) in enumerate(zip(ENTRIES, PAGES), 1):
        _, lower, upper, _ = entry
        at = 300_001_000_000 + number * 1000
        ident = f'{number:032x}' + '0' * 16
        values = [events[index] for index in page]
        wires.append(dict(number=number, request=['REQ', ident, dict(kinds=[1], since=lower, until=upper - 1, limit=3)],
            connected_at_us=at, request_at_us=at + 10, sent_at_us=at + 100, closed_at_us=at + 400,
            clock_resolution_us=1, frames=['signed'] * len(values) + ['EOSE'],
            event_ids=[event['id'] for event in values], event_json_hex=[quantum.wire_json(event).hex() for event in values],
            completion='request-and-websocket-close'))
    parameters = []
    for index, wire_number in ((0, 0), (1, 4), (2, 5)):
        event, wire = events[index], wires[wire_number]
        body = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']), event['created_at'],
            1, event['tags'], event['content']]
        parameters.append((event, wire['sent_at_us'] + 10, wire['sent_at_us'] + 50, 1,
            source.sha(b'bigbrotr/body/1\0' + cbor(body))))
    first = parameters[0]
    native, inputs, receipts = expected(state, report, events[0], 300_001_000_000, first[1], first[2], 1,
        expected_body_digest=first[4], additional_events=parameters[1:], acknowledge=True)
    dump = dict(event='dump-state', sequence=103,
        rows=[dict(cf=cf, key=key.hex(), value=value.hex()) for cf, key, value in native])
    recipe = (((0, 2, 4),), ((0, 5, 0),), (), (), ((1, 2, 5),), ((2, 2, 6),), ((2, 5, 0),), ())
    dispositions = ('overlap', 'saturated_second', 'window_closed', 'window_closed',
        'overlap', 'overlap', 'saturated_second', 'window_closed')
    continuations = (2, 1, 86400, 172800, 172802, 172802, 172801, 259200)
    checkpoint, classified = 3, 0
    for number, (wire, entry, effects) in enumerate(zip(wires, ENTRIES, recipe), 1):
        records, references = b'', []
        for ordinal, (index, kind, request) in enumerate(effects, 1):
            event = events[index]
            references += quantum.receipt_refs(receipts[3 + index])
            records += (ordinal.to_bytes(2, 'big') + ordinal.to_bytes(8, 'big')
                + hashlib.sha256(quantum.wire_json(event)).digest() + kind.to_bytes(2, 'big')
                + request.to_bytes(8, 'big') + event['created_at'].to_bytes(8, 'big') + b'\x01' + bytes.fromhex(event['id']))
        refs = quantum.reference_bytes(references)
        predicate = quantum.wire_json(['REQ', '0' * 48, wire['request'][2]])
        terminal = quantum.wire_json(['EOSE', wire['request'][1]])
        gap = 1 if number == 2 else 172801 if number == 7 else None
        row = dict(skeleton['acquisition_quanta'][0], dataset_id=dataset, job_id=job['job_id'],
            scope_id=detail['scope_id'], producer_slot_id='\\x' + report['producer_slot_id'],
            quantum_id='\\x' + f'{number:032x}', quantum_number=number, query_number=number,
            interval_lower=entry[1], interval_upper=entry[2], window_end=entry[3], horizon=259200,
            classified_before=classified, disposition=dispositions[number - 1], continuation_upper=continuations[number - 1],
            gap_reason='saturated_second' if gap is not None else None,
            gap_lower=gap, gap_upper=gap + 1 if gap is not None else None,
            request_predicate='\\x' + predicate.hex(), predicate_sha256='\\x' + source.sha(predicate),
            terminal_frame='\\x' + terminal.hex(), request_id='\\x' + wire['request'][1].encode().hex(),
            terminal_sequence=len(effects) + 1, terminal_received_at_us=wire['sent_at_us'] + 100,
            completed_at_us=wire['closed_at_us'] + 10, retained_events=len(effects), classified_prefix=len(effects),
            effect_records='\\x' + records.hex(), dependencies='\\x' + refs.hex(),
            checkpoint_before=checkpoint, checkpoint_after=checkpoint + len(effects),
            charged_bytes=1024 + len(predicate) + len(terminal) + len(records) + 256 * (len(refs) // 72))
        checkpoint += len(effects)
        classified += len(effects)
        state['acquisition_quanta'].append(row)
    return state, report, events, wires, dump, inputs, receipts


def science(state, report, events, wires, dump, inputs):
    return check.initial_science(state, report, events, wires, dump, inputs, COMPLETED,
        mutations=((0, 4, 0, 8), (1, 5, 4, 13), (2, 6, 5, 18)),
        first_accepted_at_us=ACCEPTED, native_sequence=103)


def final_status(scope):
    return dict(format='bigbrotr-linear-quantum-status/2', quantum_number='8', scope_id=scope,
        lower='172800', upper='172801', horizon='259200', result='linear_observed', terminal='eose',
        gap=None, continuation_upper='259200', retained_events=0, classified_prefix=0, coverage='pending',
        entry_kind='request', disposition='window_closed', window_end='259200', window_width='86400',
        query_budget=128, queries_spent=8, attempted_prefix='259200', gap_count=2,
        pending_work='settled', pending_interval=None)


class WindowSuccessionControls(unittest.TestCase):
    def test_literals_and_real_horizon_match_the_three_window_obligation(self):
        with tempfile.TemporaryDirectory() as temporary:
            state, _, events, wires, _, _, receipts = fixture(Path(temporary))
            self.assertEqual(check.ENTRIES, ENTRIES)
            self.assertEqual(check.PAGES, PAGES)
            self.assertEqual(check.SELECTOR, SELECTOR)
            self.assertEqual(tuple(event['id'] for event in events), IDS)
            self.assertEqual(frozen_horizon(SELECTOR, ACCEPTED), 259200)
            self.assertGreaterEqual(ACCEPTED, 259200 * 1_000_000)
            check.check_wires(wires, events)
            check.check_history(state, wires, events, receipts)
            changed = copy.deepcopy(state)
            changed['jobs'][0]['created_at_us'] = 200_000_999_999
            with self.assertRaises(AssertionError):
                check.check_history(changed, wires, events, receipts)

    def test_omitted_empty_queries_or_stopping_at_new_same_second_id_refuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            state, _, events, wires, _, _, receipts = fixture(Path(temporary))
            for index in (2, 3, 7):
                changed, transcript = copy.deepcopy(state), copy.deepcopy(wires)
                changed['acquisition_quanta'].pop(index)
                transcript.pop(index)
                with self.subTest(missing=index + 1), self.assertRaises(AssertionError):
                    check.check_history(changed, transcript, events, receipts)
                with self.assertRaises(AssertionError):
                    check.check_wires(transcript, events)
            with self.assertRaises(AssertionError):
                check.check_history(dict(state, acquisition_quanta=state['acquisition_quanta'][:6]), wires[:6], events, receipts)
            changed = copy.deepcopy(state)
            changed['acquisition_quanta'][5].update(disposition='saturated_second', gap_reason='saturated_second',
                gap_lower=172801, gap_upper=172802, continuation_upper=172801)
            quantum.shape(changed)
            with self.assertRaises(AssertionError):
                check.check_history(changed, wires, events, receipts)

    def test_lost_stall_gap_forged_window_end_and_extra_query_after_h_refuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            state, _, events, wires, _, _, receipts = fixture(Path(temporary))
            for index in (1, 6):
                changed = copy.deepcopy(state)
                row = changed['acquisition_quanta'][index]
                row.update(disposition='overlap', gap_reason=None, gap_lower=None, gap_upper=None,
                    continuation_upper=row['interval_upper'])
                quantum.shape(changed)
                with self.subTest(gap=index), self.assertRaises(AssertionError):
                    check.check_history(changed, wires, events, receipts)
            changed = copy.deepcopy(state)
            changed['acquisition_quanta'][2].update(window_end=1, continuation_upper=1)
            quantum.shape(changed)
            with self.assertRaises(AssertionError):
                check.check_history(changed, wires, events, receipts)
            changed = copy.deepcopy(state)
            changed['acquisition_quanta'].append(dict(changed['acquisition_quanta'][-1], quantum_number=9,
                quantum_id='\\x' + '99' * 16, query_number=9))
            with self.assertRaises(AssertionError):
                check.check_history(changed, wires, events, receipts)

    def test_complete_independent_native_rows_receipts_and_inputs_refuse_corruption(self):
        with tempfile.TemporaryDirectory() as temporary:
            state, report, events, wires, dump, inputs, receipts = fixture(Path(temporary))
            value = science(state, report, events, wires, dump, inputs)
            self.assertEqual((len(value['native']), len(value['inputs']), len(value['receipts'])), (76, 6, 6))
            self.assertEqual(value['receipts'], receipts)
            self.assertEqual(sum(row['cf'] == 'facts' and row['key'].startswith('0120') for row in dump['rows']), 22)
            for change in ('sequence', 'payload', 'receipt', 'missing-row', 'input'):
                altered, originals = copy.deepcopy(dump), list(inputs)
                if change == 'sequence':
                    altered['sequence'] = 121
                elif change == 'missing-row':
                    altered['rows'].pop()
                elif change == 'input':
                    body = decode(originals[-1].split(b'\0', 1)[1])
                    body[3][2][-1] = bytes(64)
                    originals[-1] = b'bigbrotr/producer-request/1\0' + cbor(body)
                else:
                    row = next(row for row in altered['rows'] if row['cf'] == ('facts' if change == 'payload' else 'receipts')
                        and row['key'].startswith('0120' if change == 'payload' else '0130')
                        and (change != 'payload' or row['key'].endswith(f'{14:016x}')))
                    raw = bytearray.fromhex(row['value'])
                    raw[-1] ^= 1
                    row['value'] = raw.hex()
                with self.subTest(change=change), self.assertRaises((AssertionError, ValueError)):
                    science(state, report, events, wires, altered, originals)

    def test_status_requires_final_prefix_two_gaps_and_settled_work(self):
        scope = '66' * 16
        expected_status = final_status(scope)
        def verify(value):
            actual = quantum_status(dict(linear_quantum=value), scope, str(ACCEPTED), known_plan=True,
                selector=SELECTOR, expected_entries=dict(enumerate(ENTRIES, 1)))
            self.assertEqual(actual, expected_status)
        verify(copy.deepcopy(expected_status))
        for fields in (dict(attempted_prefix='172800'), dict(gap_count=1), dict(gap_count=0),
                dict(queries_spent=7), dict(window_end='172801'), dict(coverage='complete'),
                dict(pending_work='query_ready', pending_interval=['172800', '172801']), dict(quantum_number='9')):
            with self.subTest(fields=fields), self.assertRaises((AssertionError, KeyError)):
                verify(dict(expected_status, **fields))
        premature = dict(expected_status, quantum_number='1', lower='0', upper='86400', window_end='86400',
            result='linear_observed', disposition='overlap', continuation_upper='2', retained_events=1,
            classified_prefix=1, queries_spent=1, gap_count=0)
        with self.assertRaises(AssertionError):
            verify(premature)

    def test_typed_progress_keeps_all_twenty_two_dependencies_and_five_classifications(self):
        import installed_progress_replay as typed
        from installed_progress_context_oracle import current_head_core
        with tempfile.TemporaryDirectory() as temporary:
            state, report, events, wires, dump, inputs, _ = fixture(Path(temporary))
            full = science(state, report, events, wires, dump, inputs)
            job, detail = state['jobs'][0], state['acquisition_jobs'][0]
            ds = source.raw(job['dataset_id'])
            domains = {row['dictionary_kind']: source.raw(row['allocation_domain']) for row in state['allocation_domains']}
            ref = lambda kind, ordinal=1: [ds, kind, domains[kind], ordinal]
            state['acquisition_scopes'] = [dict(scope_id=detail['scope_id'], endpoint_id='\\x' + '71' * 16, access_scope_id='\\x' + '72' * 16)]
            state['relay_enrollments'] = [dict(endpoint_id='\\x' + '71' * 16, access_scope_id='\\x' + '72' * 16, route_id='\\x' + '73' * 16)]
            state['producer_outcomes'] = [dict(request_sequence=n, xmin=str(100 + n)) for n in range(1, 7)]
            common = dict(dataset_id=job['dataset_id'], producer_slot_id='\\x' + report['producer_slot_id'], producer_epoch=1)
            event, wire = events[2], wires[6]
            progress = dict(common, job_id=job['job_id'], scope_id=detail['scope_id'], progress_revision=5,
                request_id='\\x' + wire['request'][1].encode().hex(), event_ordinal=1, wire_sequence=1,
                input_sha256='\\x' + source.sha(quantum.wire_json(event)), classification=5, mutation_request=None,
                classified_events=5, successful_events=5, rejected_events=0, observed_at_us=wire['sent_at_us'] + 20,
                clock_resolution_us=1, clock_anomaly_flags=0, xmin='108')
            progress['intent_digest'] = '\\x' + typed.certification_digest(event, ref(4, 3), ref(2), ref(2),
                [ref(1), ref(3), ref(5)], progress, wire).hex()
            values = {(cf, key.hex()): value.hex() for cf, key, value in full['native']}
            dependencies = []
            for number in range(1, 23):
                first = 1 if number <= 2 else 2 if number == 3 else 3 if number <= 7 else 4 + (number - 8) // 5
                key = b'\x01\x20' + ds + bytes(4) + source.be(1) + source.be(number)
                value = bytes.fromhex(values['facts', key.hex()])
                dependencies.append(dict(common, source_shard=0, source_epoch=1, source_sequence=number,
                    payload_digest='\\x' + value[2:34].hex(), xmin=str(100 + first)))
            owner = source.only(state['owner_epochs'], role='control')
            head = dict(common, progress_format=1, manifest_sha256='\\x' + source.sha(full['manifest']),
                checkpoint_revision=8, complete_through_request=6, progress_jobs=1, dependency_count=22,
                issuer_resource=owner['resource_id'], issuer_epoch=owner['epoch'], issuer_incarnation=owner['process_incarnation'],
                committed_at_us=wire['closed_at_us'] + 10, xmin='108', checkpoint_digest='\\x' + '00' * 32)
            head['checkpoint_digest'] = '\\x' + typed.head_digest(head, [progress], dependencies, lambda row: typed.native_reference(row, values)).hex()
            state.update(producer_checkpoints=[current42_head(head, initial=True)], acquisition_progress=[progress],
                producer_checkpoint_dependencies=dependencies)
            check.check_progress(state, report, wires, events, full, COMPLETED)
            for change in ('dependency', 'classified', 'request-cut', 'checkpoint'):
                altered = copy.deepcopy(state)
                if change == 'dependency':
                    altered['producer_checkpoint_dependencies'].pop()
                    current = altered['producer_checkpoints'][0]
                    current['dependency_count'] = 21
                    current['checkpoint_digest'] = '\\x' + typed.head_digest(current_head_core(current, prefix_profile=True),
                        altered['acquisition_progress'], altered['producer_checkpoint_dependencies'], lambda row: typed.native_reference(row, values)).hex()
                elif change == 'classified':
                    altered['acquisition_progress'][0]['classified_events'] = 4
                elif change == 'request-cut':
                    altered['producer_checkpoints'][0]['complete_through_request'] = 5
                else:
                    altered['producer_checkpoints'][0]['checkpoint_revision'] = 7
                with self.subTest(change=change), self.assertRaises(AssertionError):
                    check.check_progress(altered, report, wires, events, full, COMPLETED)

    def test_corpus_and_wire_checks_reject_altered_source_body_or_bounds(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _, _, events, wires, _, _, _ = fixture(root)
            for index, field, value in ((3, 'since', 0), (5, 'until', 172800), (7, 'limit', 2)):
                changed = copy.deepcopy(wires)
                changed[index]['request'][2][field] = value
                with self.subTest(index=index, field=field), self.assertRaises(AssertionError):
                    check.check_wires(changed, events)
            changed = copy.deepcopy(wires)
            changed[5]['event_ids'] = [events[1]['id']]
            with self.assertRaises(AssertionError):
                check.check_wires(changed, events)
            for name in ('event.jsonl', 'linear-window-events.json'):
                path = root / name
                original = path.read_bytes()
                path.write_bytes(original + b'\n')
                with self.subTest(name=name), self.assertRaises(AssertionError):
                    peer.corpus(root)
                path.write_bytes(original)

    def test_real_eight_page_socket_peer_keeps_then_joins_its_owned_listener(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            events = corpus(root)
            (root / 'linear-window-profile.json').write_text(json.dumps(dict(format='bigbrotr-linear-window-fixture/1', requests=8, scopes=1)))
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.bind(('127.0.0.1', 0))
            listener.listen(1)
            listener.settimeout(.05)
            address = listener.getsockname()
            finished, errors = threading.Event(), []
            worker = threading.Thread(target=peer.relay, args=(root, listener, finished, errors))
            worker.start()
            def send(connection, opcode, value):
                body = quantum.wire_json(value) if opcode == 1 else b''
                mask = b'\x01\x02\x03\x04'
                header = bytes([0x80 | opcode, 0x80 | len(body)]) if len(body) < 126 else bytes([0x80 | opcode, 0xfe]) + len(body).to_bytes(2, 'big')
                connection.sendall(header + mask + bytes(byte ^ mask[n % 4] for n, byte in enumerate(body)))
            def receive(connection):
                first, second = exact(connection, 2)
                self.assertFalse(second & 128)
                length = second & 127
                if length == 126:
                    length = int.from_bytes(exact(connection, 2), 'big')
                self.assertLess(length, 65536)
                body = exact(connection, length)
                return first & 15, json.loads(body) if first & 15 == 1 else body
            try:
                for number, (entry, page) in enumerate(zip(ENTRIES, PAGES), 1):
                    with socket.create_connection(address, timeout=3) as connection:
                        connection.settimeout(3)
                        connection.sendall(('GET /installed-fixture HTTP/1.1\r\nHost: 127.0.0.1:7447\r\n'
                            'Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n'
                            'Sec-WebSocket-Key: ' + base64.b64encode(bytes(16)).decode() + '\r\n\r\n').encode())
                        header = bytearray()
                        while not header.endswith(b'\r\n\r\n'):
                            self.assertLess(len(header), 4096)
                            header.extend(exact(connection, 1))
                        self.assertTrue(header.startswith(b'HTTP/1.1 101 Switching Protocols\r\n'))
                        ident = f'{number:032x}' + '0' * 16
                        send(connection, 1, ['REQ', ident, dict(kinds=[1], since=entry[1], until=entry[2] - 1, limit=3)])
                        for index in page:
                            self.assertEqual(receive(connection), (1, ['EVENT', ident, events[index]]))
                        self.assertEqual(receive(connection), (1, ['EOSE', ident]))
                        send(connection, 1, ['CLOSE', ident])
                        send(connection, 8, None)
                        self.assertEqual(receive(connection), (8, b''))
                        self.assertEqual(connection.recv(1), b'')
                complete = root / 'linear-window-peer-complete.json'
                deadline = time.monotonic() + 1
                while not complete.exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue(complete.exists())
                self.assertTrue(json.loads(complete.read_text())['listening'])
                self.assertEqual(listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN), 1)
            finally:
                finished.set()
                worker.join(4)
                self.assertFalse(worker.is_alive())
                self.assertEqual(listener.fileno(), -1)
            self.assertEqual(errors, [])
            wires = [json.loads(path.read_text()) for path in sorted(root.glob('relay-closed-*.json'))]
            check.check_wires(wires, events)
            joined = json.loads((root / 'linear-window-peer-joined.json').read_text())
            self.assertEqual((joined['requests'], joined['errors'], joined['listener_closed']), (8, 0, True))

    def test_peer_rejects_ninth_query_and_still_closes_owned_listener(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            events = corpus(root)
            (root / 'linear-window-profile.json').write_text(json.dumps(dict(format='bigbrotr-linear-window-fixture/1', requests=8, scopes=1)))
            class Connection:
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    return None
            listener = Mock()
            listener.accept.side_effect = [(Connection(), ('127.0.0.1', 12345)) for _ in range(9)]
            listener.getsockopt.return_value = 1
            errors = []
            with patch.object(peer, 'exchange_page') as exchange:
                peer.relay(root, listener, threading.Event(), errors)
            self.assertEqual(errors, ['AssertionError: settled pass opened a ninth socket'])
            self.assertEqual(exchange.call_count, 8)
            for call, entry, page in zip(exchange.call_args_list, ENTRIES, PAGES):
                self.assertEqual(call.args[3], dict(kinds=[1], since=entry[1], until=entry[2] - 1, limit=3))
                self.assertEqual(call.args[4], [events[index] for index in page])
            listener.close.assert_called_once()
            joined = json.loads((root / 'linear-window-peer-joined.json').read_text())
            self.assertEqual((joined['requests'], joined['errors'], joined['listener_closed']), (9, 1, True))


if __name__ == '__main__':
    unittest.main()
